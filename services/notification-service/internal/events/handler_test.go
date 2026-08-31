package events_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/davidmm07/mediflow/common/kafkautil"
	"github.com/davidmm07/mediflow/services/notification-service/internal/domain"
	"github.com/davidmm07/mediflow/services/notification-service/internal/events"
	"github.com/davidmm07/mediflow/services/notification-service/internal/store"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

var fixedTime = time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)

func newNotifier(t *testing.T) (*events.Notifier, *store.Memory) {
	t.Helper()
	memory := store.NewMemory()
	return &events.Notifier{
		Store: memory,
		Log:   zerolog.Nop(),
		Now:   func() time.Time { return fixedTime },
	}, memory
}

func envelope(t *testing.T, event string, payload interface{}) kafkautil.Envelope {
	t.Helper()
	raw, err := json.Marshal(payload)
	require.NoError(t, err)
	return kafkautil.Envelope{
		Event:      event,
		Version:    1,
		OccurredAt: fixedTime,
		Data:       raw,
	}
}

func TestHandleAppointmentCreated(t *testing.T) {
	notifier, memory := newNotifier(t)

	err := notifier.Handle(context.Background(), envelope(t, events.EventAppointmentCreated,
		events.AppointmentCreated{
			AppointmentID: "appt-1",
			PatientUserID: "patient-1",
			PatientName:   "ana.paciente",
			DoctorID:      "doc-1",
			DoctorName:    "Gregory House",
			Specialty:     "cardiology",
			StartsAt:      time.Date(2026, 9, 14, 9, 0, 0, 0, time.UTC),
			EndsAt:        time.Date(2026, 9, 14, 9, 30, 0, 0, time.UTC),
		}))
	require.NoError(t, err)

	stored := memory.All()
	require.Len(t, stored, 1)
	require.Equal(t, "patient-1", stored[0].UserID)
	require.Equal(t, domain.KindAppointmentConfirmed, stored[0].Kind)
	require.Contains(t, stored[0].Body, "Gregory House")
	require.Contains(t, stored[0].Body, "14 Sep 2026")
}

func TestHandleAppointmentCancelledIncludesReason(t *testing.T) {
	notifier, memory := newNotifier(t)

	err := notifier.Handle(context.Background(), envelope(t, events.EventAppointmentCancelled,
		events.AppointmentCancelled{
			AppointmentID: "appt-1",
			PatientUserID: "patient-1",
			DoctorName:    "Gregory House",
			StartsAt:      time.Date(2026, 9, 14, 9, 0, 0, 0, time.UTC),
			CancelledBy:   "staff",
			Reason:        "doctor unavailable",
		}))
	require.NoError(t, err)

	stored := memory.All()
	require.Len(t, stored, 1)
	require.Equal(t, domain.KindAppointmentCancelled, stored[0].Kind)
	require.Contains(t, stored[0].Body, "doctor unavailable")
}

func TestHandleUserRegisteredSendsWelcome(t *testing.T) {
	notifier, memory := newNotifier(t)

	err := notifier.Handle(context.Background(), envelope(t, events.EventUserRegistered,
		events.UserRegistered{
			UserID:    "patient-1",
			Username:  "ana.paciente",
			Email:     "ana@mediflow.dev",
			FirstName: "Ana",
			Role:      "patient",
		}))
	require.NoError(t, err)

	stored := memory.All()
	require.Len(t, stored, 1)
	require.Equal(t, domain.KindWelcome, stored[0].Kind)
	require.Equal(t, domain.ChannelEmail, stored[0].Channel)
	require.Contains(t, stored[0].Body, "Ana")
}

// A topic this service doesn't care about must advance the offset rather
// than jam the consumer in a retry loop.
func TestHandleIgnoresUnknownEvent(t *testing.T) {
	notifier, memory := newNotifier(t)

	err := notifier.Handle(context.Background(), envelope(t, "billing.invoice.issued",
		map[string]string{"invoice_id": "inv-1"}))

	require.NoError(t, err)
	require.Empty(t, memory.All())
}

// A malformed payload can never become valid on redelivery, so it is dropped
// rather than retried forever.
func TestHandleDropsMalformedPayload(t *testing.T) {
	notifier, memory := newNotifier(t)

	err := notifier.Handle(context.Background(), kafkautil.Envelope{
		Event: events.EventAppointmentCreated,
		Data:  json.RawMessage(`{"starts_at": "not-a-timestamp"}`),
	})

	require.NoError(t, err)
	require.Empty(t, memory.All())
}

func TestHandleDropsEventMissingRequiredIDs(t *testing.T) {
	notifier, memory := newNotifier(t)

	err := notifier.Handle(context.Background(), envelope(t, events.EventAppointmentCreated,
		events.AppointmentCreated{DoctorName: "Gregory House"}))

	require.NoError(t, err)
	require.Empty(t, memory.All())
}
