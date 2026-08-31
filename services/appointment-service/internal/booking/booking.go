// Package booking orchestrates MediFlow's central use case. It is the only
// place in the system that touches three collaborators in one operation —
// doctor-service over HTTP, MongoDB, and Kafka — so it is also where the
// failure ordering matters most and where the compensating action lives.
package booking

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/davidmm07/mediflow/services/appointment-service/internal/doctorclient"
	"github.com/davidmm07/mediflow/services/appointment-service/internal/domain"
	"github.com/davidmm07/mediflow/services/appointment-service/internal/store"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

// Kafka topics and event names this service produces. The notification
// service's message pact is written against these exact names.
const (
	EventAppointmentCreated   = "appointments.created"
	EventAppointmentCancelled = "appointments.cancelled"
	EventVersion              = 1
)

// DoctorAPI is the slice of doctor-service appointment-service depends on.
type DoctorAPI interface {
	GetDoctor(ctx context.Context, bearerToken, doctorID string) (doctorclient.Doctor, error)
	ReserveSlot(ctx context.Context, bearerToken, doctorID, slotID, appointmentID string) (doctorclient.Slot, error)
	ReleaseSlot(ctx context.Context, bearerToken, doctorID, slotID string) error
}

// Publisher emits domain events to a topic chosen by the caller.
type Publisher interface {
	Publish(ctx context.Context, topic, event string, version int, key string, data interface{}) error
}

// AppointmentCreated is the payload of the appointments.created event.
// notification-service depends on this shape, and the message pact makes
// that dependency explicit and testable.
type AppointmentCreated struct {
	AppointmentID string    `json:"appointment_id"`
	PatientUserID string    `json:"patient_user_id"`
	PatientName   string    `json:"patient_name"`
	DoctorID      string    `json:"doctor_id"`
	DoctorName    string    `json:"doctor_name"`
	Specialty     string    `json:"specialty"`
	StartsAt      time.Time `json:"starts_at"`
	EndsAt        time.Time `json:"ends_at"`
	Reason        string    `json:"reason"`
}

// AppointmentCancelled is the payload of the appointments.cancelled event.
type AppointmentCancelled struct {
	AppointmentID string    `json:"appointment_id"`
	PatientUserID string    `json:"patient_user_id"`
	DoctorID      string    `json:"doctor_id"`
	DoctorName    string    `json:"doctor_name"`
	StartsAt      time.Time `json:"starts_at"`
	CancelledBy   string    `json:"cancelled_by"`
	Reason        string    `json:"reason"`
}

// Service implements the booking use cases.
type Service struct {
	Store     store.Store
	Doctors   DoctorAPI
	Publisher Publisher
	Log       zerolog.Logger
	Now       func() time.Time
}

// BookRequest is the input to Book.
type BookRequest struct {
	BearerToken   string
	PatientUserID string
	PatientName   string
	DoctorID      string
	SlotID        string
	Reason        string
}

func (s *Service) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now().UTC()
}

// Book reserves a doctor's slot and records the appointment.
//
// Ordering is deliberate: the slot is reserved in doctor-service *before*
// the appointment row is written, because the reservation is the contended
// resource — reserving first means a losing racer never creates an orphan
// appointment. If the local write then fails, the reservation is released so
// the slot doesn't leak.
func (s *Service) Book(ctx context.Context, req BookRequest) (domain.Appointment, error) {
	doctor, err := s.Doctors.GetDoctor(ctx, req.BearerToken, req.DoctorID)
	if errors.Is(err, doctorclient.ErrDoctorNotFound) {
		return domain.Appointment{}, domain.ErrDoctorNotFound
	}
	if err != nil {
		return domain.Appointment{}, fmt.Errorf("booking: fetch doctor: %w", err)
	}

	appointmentID := uuid.NewString()

	slot, err := s.Doctors.ReserveSlot(ctx, req.BearerToken, req.DoctorID, req.SlotID, appointmentID)
	switch {
	case errors.Is(err, doctorclient.ErrSlotTaken), errors.Is(err, doctorclient.ErrSlotNotFound):
		return domain.Appointment{}, domain.ErrSlotUnavailable
	case err != nil:
		return domain.Appointment{}, fmt.Errorf("booking: reserve slot: %w", err)
	}

	now := s.now()
	appointment := domain.Appointment{
		ID:            appointmentID,
		PatientUserID: req.PatientUserID,
		PatientName:   req.PatientName,
		DoctorID:      doctor.ID,
		DoctorName:    doctor.FullName,
		Specialty:     doctor.Specialty,
		SlotID:        slot.ID,
		StartsAt:      slot.StartsAt,
		EndsAt:        slot.EndsAt,
		Status:        domain.StatusConfirmed,
		Reason:        req.Reason,
		CreatedAt:     now,
		UpdatedAt:     now,
	}

	if err := s.Store.Create(ctx, appointment); err != nil {
		// Compensate: the slot is reserved in another service's database and
		// would stay blocked forever if we simply returned the error.
		if relErr := s.Doctors.ReleaseSlot(ctx, req.BearerToken, req.DoctorID, req.SlotID); relErr != nil {
			s.Log.Error().Err(relErr).
				Str("slot_id", req.SlotID).
				Msg("compensating release failed; slot may be stuck reserved")
		}
		return domain.Appointment{}, fmt.Errorf("booking: persist appointment: %w", err)
	}

	// The booking is durable at this point, so a publish failure is logged
	// and swallowed: the patient has their appointment even if the reminder
	// email is delayed.
	if err := s.Publisher.Publish(ctx, EventAppointmentCreated, EventAppointmentCreated, EventVersion,
		appointment.ID, toCreatedEvent(appointment)); err != nil {
		s.Log.Error().Err(err).Str("appointment_id", appointment.ID).
			Msg("publish appointments.created failed")
	}

	return appointment, nil
}

// CancelRequest is the input to Cancel.
type CancelRequest struct {
	BearerToken   string
	AppointmentID string
	ActorUserID   string
	ActorIsStaff  bool
	Reason        string
}

// Cancel releases the doctor's slot and marks the appointment cancelled.
// Unlike Book, the local write happens first: it is the record patients see,
// and a failed release only costs an unusable-but-recoverable slot rather
// than an appointment the patient believes is cancelled but isn't.
func (s *Service) Cancel(ctx context.Context, req CancelRequest) (domain.Appointment, error) {
	appointment, err := s.Store.Get(ctx, req.AppointmentID)
	if err != nil {
		return domain.Appointment{}, err
	}

	if !req.ActorIsStaff && appointment.PatientUserID != req.ActorUserID {
		return domain.Appointment{}, domain.ErrForbidden
	}
	if !appointment.CanCancel(s.now()) {
		return domain.Appointment{}, domain.ErrNotCancellable
	}

	appointment.Status = domain.StatusCancelled
	appointment.CancelledReason = req.Reason
	appointment.UpdatedAt = s.now()

	if err := s.Store.Update(ctx, appointment); err != nil {
		return domain.Appointment{}, fmt.Errorf("booking: update appointment: %w", err)
	}

	if err := s.Doctors.ReleaseSlot(ctx, req.BearerToken, appointment.DoctorID, appointment.SlotID); err != nil {
		s.Log.Error().Err(err).
			Str("appointment_id", appointment.ID).
			Str("slot_id", appointment.SlotID).
			Msg("slot release failed; slot stays reserved until reconciled")
	}

	actor := "patient"
	if req.ActorIsStaff {
		actor = "staff"
	}

	if err := s.Publisher.Publish(ctx, EventAppointmentCancelled, EventAppointmentCancelled, EventVersion,
		appointment.ID, AppointmentCancelled{
			AppointmentID: appointment.ID,
			PatientUserID: appointment.PatientUserID,
			DoctorID:      appointment.DoctorID,
			DoctorName:    appointment.DoctorName,
			StartsAt:      appointment.StartsAt,
			CancelledBy:   actor,
			Reason:        req.Reason,
		}); err != nil {
		s.Log.Error().Err(err).Str("appointment_id", appointment.ID).
			Msg("publish appointments.cancelled failed")
	}

	return appointment, nil
}

func toCreatedEvent(a domain.Appointment) AppointmentCreated {
	return AppointmentCreated{
		AppointmentID: a.ID,
		PatientUserID: a.PatientUserID,
		PatientName:   a.PatientName,
		DoctorID:      a.DoctorID,
		DoctorName:    a.DoctorName,
		Specialty:     a.Specialty,
		StartsAt:      a.StartsAt,
		EndsAt:        a.EndsAt,
		Reason:        a.Reason,
	}
}
