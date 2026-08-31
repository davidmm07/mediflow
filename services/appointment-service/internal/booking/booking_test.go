package booking_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/davidmm07/mediflow/services/appointment-service/internal/booking"
	"github.com/davidmm07/mediflow/services/appointment-service/internal/doctorclient"
	"github.com/davidmm07/mediflow/services/appointment-service/internal/domain"
	"github.com/davidmm07/mediflow/services/appointment-service/internal/store"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

var (
	testNow   = time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	testStart = time.Date(2026, 9, 14, 9, 0, 0, 0, time.UTC)
	testEnd   = time.Date(2026, 9, 14, 9, 30, 0, 0, time.UTC)
)

type fakeDoctorAPI struct {
	doctor         doctorclient.Doctor
	slot           doctorclient.Slot
	getErr         error
	reserveErr     error
	releaseErr     error
	releasedSlots  []string
	reserveCalls   int
}

func (f *fakeDoctorAPI) GetDoctor(_ context.Context, _, _ string) (doctorclient.Doctor, error) {
	return f.doctor, f.getErr
}

func (f *fakeDoctorAPI) ReserveSlot(_ context.Context, _, _, _, appointmentID string) (doctorclient.Slot, error) {
	f.reserveCalls++
	if f.reserveErr != nil {
		return doctorclient.Slot{}, f.reserveErr
	}
	slot := f.slot
	slot.Reserved = true
	slot.AppointmentID = appointmentID
	return slot, nil
}

func (f *fakeDoctorAPI) ReleaseSlot(_ context.Context, _, _, slotID string) error {
	f.releasedSlots = append(f.releasedSlots, slotID)
	return f.releaseErr
}

type recordingPublisher struct {
	topics    []string
	payloads  []interface{}
	returnErr error
}

func (r *recordingPublisher) Publish(_ context.Context, topic, _ string, _ int, _ string, data interface{}) error {
	r.topics = append(r.topics, topic)
	r.payloads = append(r.payloads, data)
	return r.returnErr
}

// failingStore lets tests drive the compensation path without a real DB.
type failingStore struct {
	store.Store
	createErr error
}

func (f failingStore) Create(ctx context.Context, a domain.Appointment) error {
	if f.createErr != nil {
		return f.createErr
	}
	return f.Store.Create(ctx, a)
}

func newDoctorAPI() *fakeDoctorAPI {
	return &fakeDoctorAPI{
		doctor: doctorclient.Doctor{
			ID:        "doc-1",
			FullName:  "Gregory House",
			Specialty: "cardiology",
			Active:    true,
		},
		slot: doctorclient.Slot{
			ID:       "slot-1",
			DoctorID: "doc-1",
			StartsAt: testStart,
			EndsAt:   testEnd,
		},
	}
}

func newService(st store.Store, doctors booking.DoctorAPI, pub booking.Publisher) *booking.Service {
	return &booking.Service{
		Store:     st,
		Doctors:   doctors,
		Publisher: pub,
		Log:       zerolog.Nop(),
		Now:       func() time.Time { return testNow },
	}
}

func bookRequest() booking.BookRequest {
	return booking.BookRequest{
		BearerToken:   "token",
		PatientUserID: "patient-1",
		PatientName:   "ana.paciente",
		DoctorID:      "doc-1",
		SlotID:        "slot-1",
		Reason:        "chest pain follow-up",
	}
}

func TestBookReservesSlotAndPublishesEvent(t *testing.T) {
	doctors := newDoctorAPI()
	pub := &recordingPublisher{}
	svc := newService(store.NewMemory(), doctors, pub)

	appointment, err := svc.Book(context.Background(), bookRequest())
	require.NoError(t, err)

	require.Equal(t, domain.StatusConfirmed, appointment.Status)
	require.Equal(t, "Gregory House", appointment.DoctorName)
	require.Equal(t, "cardiology", appointment.Specialty)
	require.Equal(t, testStart, appointment.StartsAt)
	require.Equal(t, []string{booking.EventAppointmentCreated}, pub.topics)

	event, ok := pub.payloads[0].(booking.AppointmentCreated)
	require.True(t, ok)
	require.Equal(t, appointment.ID, event.AppointmentID)
	require.Equal(t, "patient-1", event.PatientUserID)
}

func TestBookReturnsSlotUnavailableWhenRaceLost(t *testing.T) {
	doctors := newDoctorAPI()
	doctors.reserveErr = doctorclient.ErrSlotTaken
	pub := &recordingPublisher{}
	svc := newService(store.NewMemory(), doctors, pub)

	_, err := svc.Book(context.Background(), bookRequest())

	require.ErrorIs(t, err, domain.ErrSlotUnavailable)
	require.Empty(t, pub.topics, "no event should be published for a failed booking")
}

func TestBookReturnsDoctorNotFound(t *testing.T) {
	doctors := newDoctorAPI()
	doctors.getErr = doctorclient.ErrDoctorNotFound
	svc := newService(store.NewMemory(), doctors, &recordingPublisher{})

	_, err := svc.Book(context.Background(), bookRequest())

	require.ErrorIs(t, err, domain.ErrDoctorNotFound)
	require.Zero(t, doctors.reserveCalls, "a missing doctor must not reserve anything")
}

// The compensating release is the part of the saga most likely to rot
// unnoticed, since nothing user-visible depends on it succeeding.
func TestBookReleasesSlotWhenPersistenceFails(t *testing.T) {
	doctors := newDoctorAPI()
	pub := &recordingPublisher{}
	svc := newService(
		failingStore{Store: store.NewMemory(), createErr: errors.New("mongo unavailable")},
		doctors, pub,
	)

	_, err := svc.Book(context.Background(), bookRequest())

	require.Error(t, err)
	require.Equal(t, []string{"slot-1"}, doctors.releasedSlots)
	require.Empty(t, pub.topics)
}

// The booking is already durable when publishing happens, so a broker outage
// must not turn a successful booking into an error for the patient.
func TestBookSucceedsWhenPublishFails(t *testing.T) {
	doctors := newDoctorAPI()
	pub := &recordingPublisher{returnErr: errors.New("broker down")}
	svc := newService(store.NewMemory(), doctors, pub)

	appointment, err := svc.Book(context.Background(), bookRequest())

	require.NoError(t, err)
	require.Equal(t, domain.StatusConfirmed, appointment.Status)
}

func TestCancelReleasesSlotAndPublishes(t *testing.T) {
	doctors := newDoctorAPI()
	pub := &recordingPublisher{}
	svc := newService(store.NewMemory(), doctors, pub)

	appointment, err := svc.Book(context.Background(), bookRequest())
	require.NoError(t, err)

	cancelled, err := svc.Cancel(context.Background(), booking.CancelRequest{
		BearerToken:   "token",
		AppointmentID: appointment.ID,
		ActorUserID:   "patient-1",
		Reason:        "scheduling conflict",
	})
	require.NoError(t, err)

	require.Equal(t, domain.StatusCancelled, cancelled.Status)
	require.Equal(t, "scheduling conflict", cancelled.CancelledReason)
	require.Equal(t, []string{"slot-1"}, doctors.releasedSlots)
	require.Equal(t,
		[]string{booking.EventAppointmentCreated, booking.EventAppointmentCancelled},
		pub.topics,
	)

	event, ok := pub.payloads[1].(booking.AppointmentCancelled)
	require.True(t, ok)
	require.Equal(t, "patient", event.CancelledBy)
}

func TestCancelRejectsAnotherPatientsAppointment(t *testing.T) {
	svc := newService(store.NewMemory(), newDoctorAPI(), &recordingPublisher{})

	appointment, err := svc.Book(context.Background(), bookRequest())
	require.NoError(t, err)

	_, err = svc.Cancel(context.Background(), booking.CancelRequest{
		AppointmentID: appointment.ID,
		ActorUserID:   "a-different-patient",
	})

	require.ErrorIs(t, err, domain.ErrForbidden)
}

func TestCancelRejectsAppointmentAlreadyStarted(t *testing.T) {
	svc := newService(store.NewMemory(), newDoctorAPI(), &recordingPublisher{})

	appointment, err := svc.Book(context.Background(), bookRequest())
	require.NoError(t, err)

	// Jump past the consultation start time.
	svc.Now = func() time.Time { return testStart.Add(time.Hour) }

	_, err = svc.Cancel(context.Background(), booking.CancelRequest{
		AppointmentID: appointment.ID,
		ActorUserID:   "patient-1",
	})

	require.ErrorIs(t, err, domain.ErrNotCancellable)
}

func TestCancelAllowsStaffToCancelOnBehalfOfPatient(t *testing.T) {
	pub := &recordingPublisher{}
	svc := newService(store.NewMemory(), newDoctorAPI(), pub)

	appointment, err := svc.Book(context.Background(), bookRequest())
	require.NoError(t, err)

	_, err = svc.Cancel(context.Background(), booking.CancelRequest{
		AppointmentID: appointment.ID,
		ActorUserID:   "doctor-user",
		ActorIsStaff:  true,
		Reason:        "doctor unavailable",
	})
	require.NoError(t, err)

	event, ok := pub.payloads[1].(booking.AppointmentCancelled)
	require.True(t, ok)
	require.Equal(t, "staff", event.CancelledBy)
}
