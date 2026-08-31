package events_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/davidmm07/mediflow/common/kafkautil"
	"github.com/davidmm07/mediflow/services/patient-service/internal/events"
	"github.com/davidmm07/mediflow/services/patient-service/internal/store"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func newProvisioner() (*events.Provisioner, *store.Memory) {
	memory := store.NewMemory()
	return &events.Provisioner{Store: memory, Log: zerolog.Nop()}, memory
}

func envelope(t *testing.T, event string, payload interface{}) kafkautil.Envelope {
	t.Helper()
	raw, err := json.Marshal(payload)
	require.NoError(t, err)
	return kafkautil.Envelope{Event: event, Version: 1, Data: raw}
}

func TestProvisionsPatientOnRegistration(t *testing.T) {
	provisioner, memory := newProvisioner()

	err := provisioner.Handle(context.Background(), envelope(t, "auth.user.registered",
		events.UserRegistered{
			UserID:    "user-1",
			Username:  "ana.paciente",
			Email:     "ana@mediflow.dev",
			FirstName: "Ana",
			LastName:  "Ramirez",
			Role:      "patient",
		}))
	require.NoError(t, err)

	patient, err := memory.GetByUserID(context.Background(), "user-1")
	require.NoError(t, err)
	require.Equal(t, "Ana Ramirez", patient.FullName)
	require.Equal(t, "ana@mediflow.dev", patient.Email)
	require.False(t, patient.Onboarded)
}

func TestIgnoresDoctorRegistrations(t *testing.T) {
	provisioner, memory := newProvisioner()

	err := provisioner.Handle(context.Background(), envelope(t, "auth.user.registered",
		events.UserRegistered{UserID: "user-2", Username: "dr.house", Role: "doctor"}))
	require.NoError(t, err)

	_, err = memory.GetByUserID(context.Background(), "user-2")
	require.Error(t, err)
}

// Kafka guarantees at-least-once delivery, so the same registration event
// will eventually arrive twice. It must not produce two patient records.
func TestRedeliveredEventIsIdempotent(t *testing.T) {
	provisioner, memory := newProvisioner()

	event := envelope(t, "auth.user.registered", events.UserRegistered{
		UserID: "user-1", Username: "ana.paciente", Role: "patient",
	})

	require.NoError(t, provisioner.Handle(context.Background(), event))
	require.NoError(t, provisioner.Handle(context.Background(), event))

	patient, err := memory.GetByUserID(context.Background(), "user-1")
	require.NoError(t, err)
	require.Equal(t, "ana.paciente", patient.FullName)
}

func TestFallsBackToUsernameWhenNameMissing(t *testing.T) {
	provisioner, memory := newProvisioner()

	require.NoError(t, provisioner.Handle(context.Background(), envelope(t, "auth.user.registered",
		events.UserRegistered{UserID: "user-3", Username: "ana.paciente", Role: "patient"})))

	patient, err := memory.GetByUserID(context.Background(), "user-3")
	require.NoError(t, err)
	require.Equal(t, "ana.paciente", patient.FullName)
}

func TestIgnoresUnrelatedEvents(t *testing.T) {
	provisioner, memory := newProvisioner()

	require.NoError(t, provisioner.Handle(context.Background(), envelope(t, "appointments.created",
		map[string]string{"appointment_id": "appt-1"})))

	require.Empty(t, memory.All())
}

func TestDropsMalformedPayload(t *testing.T) {
	provisioner, memory := newProvisioner()

	err := provisioner.Handle(context.Background(), kafkautil.Envelope{
		Event: "auth.user.registered",
		Data:  json.RawMessage(`{"user_id": 12345}`),
	})

	require.NoError(t, err)
	require.Empty(t, memory.All())
}
