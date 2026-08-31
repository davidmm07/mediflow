// Package events turns MediFlow domain events into user-facing notifications.
//
// The exported *payload structs* here are notification-service's half of the
// message contracts: they declare exactly which fields this service reads
// from each event. The message pact tests in events_pact_test.go feed
// Pact-generated messages through Handle, so if appointment-service ever
// renames or drops one of these fields the contract fails in CI rather than
// silently producing empty notification bodies in production.
package events

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/davidmm07/mediflow/common/kafkautil"
	"github.com/davidmm07/mediflow/services/notification-service/internal/domain"
	"github.com/davidmm07/mediflow/services/notification-service/internal/store"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

// Event names this service reacts to.
const (
	EventUserRegistered       = "auth.user.registered"
	EventAppointmentCreated   = "appointments.created"
	EventAppointmentCancelled = "appointments.cancelled"
)

// AppointmentCreated is the subset of the appointments.created payload
// notification-service depends on.
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

// AppointmentCancelled is the subset of the appointments.cancelled payload
// notification-service depends on.
type AppointmentCancelled struct {
	AppointmentID string    `json:"appointment_id"`
	PatientUserID string    `json:"patient_user_id"`
	DoctorID      string    `json:"doctor_id"`
	DoctorName    string    `json:"doctor_name"`
	StartsAt      time.Time `json:"starts_at"`
	CancelledBy   string    `json:"cancelled_by"`
	Reason        string    `json:"reason"`
}

// UserRegistered is the subset of the auth.user.registered payload
// notification-service depends on.
type UserRegistered struct {
	UserID    string `json:"user_id"`
	Username  string `json:"username"`
	Email     string `json:"email"`
	FirstName string `json:"first_name"`
	Role      string `json:"role"`
}

// Notifier renders events into notifications and stores them.
type Notifier struct {
	Store store.Store
	Log   zerolog.Logger
	Now   func() time.Time
}

func (n *Notifier) now() time.Time {
	if n.Now != nil {
		return n.Now()
	}
	return time.Now().UTC()
}

// Handle dispatches one envelope to the matching renderer. Unknown events
// are ignored so a new event type elsewhere in the system doesn't stall this
// consumer.
func (n *Notifier) Handle(ctx context.Context, env kafkautil.Envelope) error {
	switch env.Event {
	case EventAppointmentCreated:
		return n.handleAppointmentCreated(ctx, env.Data)
	case EventAppointmentCancelled:
		return n.handleAppointmentCancelled(ctx, env.Data)
	case EventUserRegistered:
		return n.handleUserRegistered(ctx, env.Data)
	default:
		n.Log.Debug().Str("event", env.Event).Msg("ignoring unrecognised event")
		return nil
	}
}

func (n *Notifier) handleAppointmentCreated(ctx context.Context, raw json.RawMessage) error {
	var payload AppointmentCreated
	if err := json.Unmarshal(raw, &payload); err != nil {
		n.Log.Error().Err(err).Msg("discarding unparseable appointments.created event")
		return nil
	}
	if payload.PatientUserID == "" || payload.AppointmentID == "" {
		n.Log.Warn().Msg("discarding appointments.created event missing required ids")
		return nil
	}

	notification := domain.Notification{
		ID:       uuid.NewString(),
		UserID:   payload.PatientUserID,
		Kind:     domain.KindAppointmentConfirmed,
		Channel:  domain.ChannelInApp,
		Title:    "Your appointment is confirmed",
		Body: fmt.Sprintf("Your %s consultation with %s is confirmed for %s.",
			payload.Specialty, payload.DoctorName, domain.AppointmentTime(payload.StartsAt)),
		SourceID:  payload.AppointmentID,
		CreatedAt: n.now(),
	}

	if err := n.Store.Save(ctx, notification); err != nil {
		return fmt.Errorf("save appointment confirmation: %w", err)
	}
	return nil
}

func (n *Notifier) handleAppointmentCancelled(ctx context.Context, raw json.RawMessage) error {
	var payload AppointmentCancelled
	if err := json.Unmarshal(raw, &payload); err != nil {
		n.Log.Error().Err(err).Msg("discarding unparseable appointments.cancelled event")
		return nil
	}
	if payload.PatientUserID == "" || payload.AppointmentID == "" {
		n.Log.Warn().Msg("discarding appointments.cancelled event missing required ids")
		return nil
	}

	body := fmt.Sprintf("Your appointment with %s on %s has been cancelled.",
		payload.DoctorName, domain.AppointmentTime(payload.StartsAt))
	if payload.Reason != "" {
		body += " Reason: " + payload.Reason
	}

	notification := domain.Notification{
		ID:        uuid.NewString(),
		UserID:    payload.PatientUserID,
		Kind:      domain.KindAppointmentCancelled,
		Channel:   domain.ChannelInApp,
		Title:     "Your appointment was cancelled",
		Body:      body,
		SourceID:  payload.AppointmentID,
		CreatedAt: n.now(),
	}

	if err := n.Store.Save(ctx, notification); err != nil {
		return fmt.Errorf("save cancellation notice: %w", err)
	}
	return nil
}

func (n *Notifier) handleUserRegistered(ctx context.Context, raw json.RawMessage) error {
	var payload UserRegistered
	if err := json.Unmarshal(raw, &payload); err != nil {
		n.Log.Error().Err(err).Msg("discarding unparseable auth.user.registered event")
		return nil
	}
	if payload.UserID == "" {
		return nil
	}

	name := payload.FirstName
	if name == "" {
		name = payload.Username
	}

	notification := domain.Notification{
		ID:        uuid.NewString(),
		UserID:    payload.UserID,
		Kind:      domain.KindWelcome,
		Channel:   domain.ChannelEmail,
		Title:     "Welcome to MediFlow",
		Body:      fmt.Sprintf("Hi %s, your MediFlow account is ready. You can now book consultations.", name),
		SourceID:  payload.UserID,
		CreatedAt: n.now(),
	}

	if err := n.Store.Save(ctx, notification); err != nil {
		return fmt.Errorf("save welcome notice: %w", err)
	}
	return nil
}
