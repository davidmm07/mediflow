//go:build pact

// Message-based contract test: notification-service (consumer) against
// appointment-service and auth-service (producers).
//
// HTTP pacts are the well-known case; async ones are the interesting half of
// an event-driven system, because a broker will happily deliver a payload
// whose shape nobody agreed on. Kafka has no request/response to assert
// against, so the contract is the *message*: Pact reifies an example event
// from the matchers below, hands it to the real Notifier.Handle, and the
// assertions check that a correct, non-empty notification came out.
//
// The consequence is concrete: if appointment-service renames `starts_at` or
// drops `doctor_name`, this test still passes locally (the producer isn't
// here), but the pact published from it fails when appointment-service
// verifies it, in the producer's own pipeline, before the change ships.
package events_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/davidmm07/mediflow/common/kafkautil"
	"github.com/davidmm07/mediflow/services/notification-service/internal/domain"
	"github.com/davidmm07/mediflow/services/notification-service/internal/events"
	"github.com/davidmm07/mediflow/services/notification-service/internal/store"
	"github.com/pact-foundation/pact-go/v2/matchers"
	message "github.com/pact-foundation/pact-go/v2/message/v4"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

const patientUserID = "11111111-2222-4333-8444-555555555555"

func newMessagePact(t *testing.T, provider string) *message.AsynchronousPact {
	t.Helper()

	pactDir, err := filepath.Abs(filepath.Join("..", "..", "..", "..", "pacts"))
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(pactDir, 0o755))

	pact, err := message.NewAsynchronousPact(message.Config{
		Consumer: "notification-service",
		Provider: provider,
		PactDir:  pactDir,
	})
	require.NoError(t, err)
	return pact
}

// consume feeds a Pact-reified message through the production handler as if
// it had arrived from Kafka, and returns the notifications it produced.
func consume(t *testing.T, eventName string, raw []byte) []domain.Notification {
	t.Helper()

	memory := store.NewMemory()
	notifier := &events.Notifier{
		Store: memory,
		Log:   zerolog.Nop(),
		Now:   func() time.Time { return time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC) },
	}

	err := notifier.Handle(context.Background(), kafkautil.Envelope{
		Event:      eventName,
		Version:    1,
		OccurredAt: time.Now().UTC(),
		Data:       json.RawMessage(raw),
	})
	require.NoError(t, err)

	return memory.All()
}

func TestPactConsumesAppointmentCreated(t *testing.T) {
	pact := newMessagePact(t, "appointment-service")

	err := pact.AddAsynchronousMessage().
		Given("a patient has just booked a cardiology consultation").
		ExpectsToReceive("an appointments.created event").
		WithJSONContent(matchers.Map{
			"appointment_id":  matchers.Like("9a8b7c6d-5e4f-4a3b-8c9d-0e1f2a3b4c5d"),
			"patient_user_id": matchers.Like(patientUserID),
			"patient_name":    matchers.Like("ana.paciente"),
			"doctor_id":       matchers.Like("3f1a9b7c-2d54-4a1e-9f3b-8c0d5e6a7b21"),
			"doctor_name":     matchers.Like("Gregory House"),
			"specialty":       matchers.Like("cardiology"),
			"starts_at":       matchers.Regex("2026-09-14T09:00:00Z", `^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(Z|[+-]\d{2}:\d{2})$`),
			"ends_at":         matchers.Regex("2026-09-14T09:30:00Z", `^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(Z|[+-]\d{2}:\d{2})$`),
			"reason":          matchers.Like("chest pain follow-up"),
		}).
		AsType(&events.AppointmentCreated{}).
		ConsumedBy(func(msg message.AsynchronousMessage) error {
			notifications := consume(t, events.EventAppointmentCreated, msg.Contents)

			require.Len(t, notifications, 1)
			n := notifications[0]
			require.Equal(t, patientUserID, n.UserID)
			require.Equal(t, domain.KindAppointmentConfirmed, n.Kind)
			// The body is rendered from event fields, so an empty-looking
			// body is exactly the symptom a renamed field would produce.
			require.Contains(t, n.Body, "Gregory House")
			require.Contains(t, n.Body, "cardiology")
			require.Contains(t, n.Body, "14 Sep 2026")
			return nil
		}).
		Verify(t)

	require.NoError(t, err)
}

func TestPactConsumesAppointmentCancelled(t *testing.T) {
	pact := newMessagePact(t, "appointment-service")

	err := pact.AddAsynchronousMessage().
		Given("a confirmed appointment has just been cancelled by the patient").
		ExpectsToReceive("an appointments.cancelled event").
		WithJSONContent(matchers.Map{
			"appointment_id":  matchers.Like("9a8b7c6d-5e4f-4a3b-8c9d-0e1f2a3b4c5d"),
			"patient_user_id": matchers.Like(patientUserID),
			"doctor_id":       matchers.Like("3f1a9b7c-2d54-4a1e-9f3b-8c0d5e6a7b21"),
			"doctor_name":     matchers.Like("Gregory House"),
			"starts_at":       matchers.Regex("2026-09-14T09:00:00Z", `^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(Z|[+-]\d{2}:\d{2})$`),
			"cancelled_by":    matchers.Regex("patient", "^(patient|staff)$"),
			"reason":          matchers.Like("scheduling conflict"),
		}).
		AsType(&events.AppointmentCancelled{}).
		ConsumedBy(func(msg message.AsynchronousMessage) error {
			notifications := consume(t, events.EventAppointmentCancelled, msg.Contents)

			require.Len(t, notifications, 1)
			n := notifications[0]
			require.Equal(t, patientUserID, n.UserID)
			require.Equal(t, domain.KindAppointmentCancelled, n.Kind)
			require.Contains(t, n.Body, "Gregory House")
			require.Contains(t, n.Body, "scheduling conflict")
			return nil
		}).
		Verify(t)

	require.NoError(t, err)
}

func TestPactConsumesUserRegistered(t *testing.T) {
	pact := newMessagePact(t, "auth-service")

	err := pact.AddAsynchronousMessage().
		Given("a new patient has just completed self-registration").
		ExpectsToReceive("an auth.user.registered event").
		WithJSONContent(matchers.Map{
			"user_id":    matchers.Like(patientUserID),
			"username":   matchers.Like("ana.paciente"),
			"email":      matchers.Regex("ana@mediflow.dev", `^[^@\s]+@[^@\s]+\.[a-z]{2,}$`),
			"first_name": matchers.Like("Ana"),
			"last_name":  matchers.Like("Ramirez"),
			"role":       matchers.Regex("patient", "^(patient|doctor)$"),
		}).
		AsType(&events.UserRegistered{}).
		ConsumedBy(func(msg message.AsynchronousMessage) error {
			notifications := consume(t, events.EventUserRegistered, msg.Contents)

			require.Len(t, notifications, 1)
			n := notifications[0]
			require.Equal(t, patientUserID, n.UserID)
			require.Equal(t, domain.KindWelcome, n.Kind)
			require.Equal(t, domain.ChannelEmail, n.Channel)
			require.Contains(t, n.Body, "Ana")
			return nil
		}).
		Verify(t)

	require.NoError(t, err)
}
