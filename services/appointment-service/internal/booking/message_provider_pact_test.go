//go:build pact

// Message provider verification: appointment-service proves it really emits
// the events notification-service says it depends on.
//
// The message handlers below do not hand-write JSON. They drive the actual
// booking.Service, with a stubbed doctor API and an in-memory store, and
// return whatever payload the service genuinely published to Kafka. That is
// what makes the verification meaningful: if Book() stops setting
// DoctorName, no fixture hides it, and the pact fails here.
package booking_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/davidmm07/mediflow/services/appointment-service/internal/booking"
	"github.com/davidmm07/mediflow/services/appointment-service/internal/doctorclient"
	"github.com/davidmm07/mediflow/services/appointment-service/internal/store"
	pactmessage "github.com/pact-foundation/pact-go/v2/message"
	"github.com/pact-foundation/pact-go/v2/models"
	"github.com/pact-foundation/pact-go/v2/provider"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// capturingPublisher records the last event a booking operation emitted so
// the pact handler can return the real payload.
type capturingPublisher struct {
	topic string
	event string
	data  interface{}
}

func (c *capturingPublisher) Publish(_ context.Context, topic, event string, _ int, _ string, data interface{}) error {
	c.topic = topic
	c.event = event
	c.data = data
	return nil
}

// stubDoctorAPI stands in for doctor-service. Its shape is safe to fake here
// precisely because the HTTP pact in internal/doctorclient already pins the
// real interaction.
type stubDoctorAPI struct {
	doctor doctorclient.Doctor
	slot   doctorclient.Slot
}

func (s stubDoctorAPI) GetDoctor(_ context.Context, _, _ string) (doctorclient.Doctor, error) {
	return s.doctor, nil
}

func (s stubDoctorAPI) ReserveSlot(_ context.Context, _, _, _, appointmentID string) (doctorclient.Slot, error) {
	slot := s.slot
	slot.Reserved = true
	slot.AppointmentID = appointmentID
	return slot, nil
}

func (s stubDoctorAPI) ReleaseSlot(_ context.Context, _, _, _ string) error { return nil }

var (
	fixedNow  = time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	slotStart = time.Date(2026, 9, 14, 9, 0, 0, 0, time.UTC)
	slotEnd   = time.Date(2026, 9, 14, 9, 30, 0, 0, time.UTC)
)

func newBookingService() (*booking.Service, *capturingPublisher) {
	publisher := &capturingPublisher{}

	svc := &booking.Service{
		Store: store.NewMemory(),
		Doctors: stubDoctorAPI{
			doctor: doctorclient.Doctor{
				ID:              "3f1a9b7c-2d54-4a1e-9f3b-8c0d5e6a7b21",
				FullName:        "Gregory House",
				Specialty:       "cardiology",
				ConsultationFee: 75.0,
				Active:          true,
			},
			slot: doctorclient.Slot{
				ID:       "b2c4d6e8-1357-4900-a1b2-c3d4e5f60718",
				DoctorID: "3f1a9b7c-2d54-4a1e-9f3b-8c0d5e6a7b21",
				StartsAt: slotStart,
				EndsAt:   slotEnd,
			},
		},
		Publisher: publisher,
		Log:       zerolog.Nop(),
		Now:       func() time.Time { return fixedNow },
	}

	return svc, publisher
}

// bookOne runs a real booking and returns the event payload it published.
func bookOne(svc *booking.Service, publisher *capturingPublisher) (pactmessage.Body, error) {
	appointment, err := svc.Book(context.Background(), booking.BookRequest{
		BearerToken:   "an-access-token-issued-by-keycloak",
		PatientUserID: "11111111-2222-4333-8444-555555555555",
		PatientName:   "ana.paciente",
		DoctorID:      "3f1a9b7c-2d54-4a1e-9f3b-8c0d5e6a7b21",
		SlotID:        "b2c4d6e8-1357-4900-a1b2-c3d4e5f60718",
		Reason:        "chest pain follow-up",
	})
	if err != nil {
		return nil, err
	}
	if publisher.data == nil {
		return nil, errors.New("booking did not publish an event")
	}
	_ = appointment
	return toBody(publisher.data)
}

// toBody round-trips through JSON so the verifier compares the same bytes
// that would go on the wire, struct tags and all.
func toBody(v interface{}) (pactmessage.Body, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var body map[string]interface{}
	if err := json.Unmarshal(raw, &body); err != nil {
		return nil, err
	}
	return body, nil
}

func TestPactMessageProviderVerification(t *testing.T) {
	pactDir, err := filepath.Abs(filepath.Join("..", "..", "..", "..", "pacts"))
	require.NoError(t, err)

	request := provider.VerifyRequest{
		Provider:                   "appointment-service",
		ProviderVersion:            providerVersion(),
		ProviderBranch:             os.Getenv("GIT_BRANCH"),
		FailIfNoPactsFound:         true,
		DisableColoredOutput:       true,
		PublishVerificationResults: os.Getenv("PACT_PUBLISH_RESULTS") == "true",

		MessageHandlers: pactmessage.Handlers{
			"an appointments.created event": func(_ []models.ProviderState) (pactmessage.Body, pactmessage.Metadata, error) {
				svc, publisher := newBookingService()

				body, err := bookOne(svc, publisher)
				if err != nil {
					return nil, nil, err
				}
				return body, pactmessage.Metadata{
					"kafka_topic": booking.EventAppointmentCreated,
					"contentType": "application/json",
				}, nil
			},

			"an appointments.cancelled event": func(_ []models.ProviderState) (pactmessage.Body, pactmessage.Metadata, error) {
				svc, publisher := newBookingService()

				appointment, err := svc.Book(context.Background(), booking.BookRequest{
					BearerToken:   "an-access-token-issued-by-keycloak",
					PatientUserID: "11111111-2222-4333-8444-555555555555",
					PatientName:   "ana.paciente",
					DoctorID:      "3f1a9b7c-2d54-4a1e-9f3b-8c0d5e6a7b21",
					SlotID:        "b2c4d6e8-1357-4900-a1b2-c3d4e5f60718",
					Reason:        "chest pain follow-up",
				})
				if err != nil {
					return nil, nil, err
				}

				if _, err := svc.Cancel(context.Background(), booking.CancelRequest{
					BearerToken:   "an-access-token-issued-by-keycloak",
					AppointmentID: appointment.ID,
					ActorUserID:   appointment.PatientUserID,
					Reason:        "scheduling conflict",
				}); err != nil {
					return nil, nil, err
				}

				body, err := toBody(publisher.data)
				if err != nil {
					return nil, nil, err
				}
				return body, pactmessage.Metadata{
					"kafka_topic": booking.EventAppointmentCancelled,
					"contentType": "application/json",
				}, nil
			},
		},
	}

	if brokerURL := os.Getenv("PACT_BROKER_BASE_URL"); brokerURL != "" {
		request.BrokerURL = brokerURL
		request.BrokerUsername = os.Getenv("PACT_BROKER_USERNAME")
		request.BrokerPassword = os.Getenv("PACT_BROKER_PASSWORD")
		request.BrokerToken = os.Getenv("PACT_BROKER_TOKEN")
		request.ConsumerVersionSelectors = []provider.Selector{
			&provider.ConsumerVersionSelector{MainBranch: true},
			&provider.ConsumerVersionSelector{Deployed: true},
		}
	} else {
		request.PactFiles = []string{
			filepath.ToSlash(filepath.Join(pactDir, "notification-service-appointment-service.json")),
		}
	}

	require.NoError(t, provider.NewVerifier().VerifyProvider(t, request))
}

func providerVersion() string {
	if sha := os.Getenv("GIT_COMMIT"); sha != "" {
		return sha
	}
	return "dev"
}
