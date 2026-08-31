// Package events reacts to identity changes broadcast by auth-service.
// Provisioning a patient record here, rather than having auth-service write
// into another service's database, is what keeps the two services
// independently deployable.
package events

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/davidmm07/mediflow/common/kafkautil"
	"github.com/davidmm07/mediflow/services/patient-service/internal/domain"
	"github.com/davidmm07/mediflow/services/patient-service/internal/store"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

// UserRegistered is the payload patient-service expects inside the
// auth.user.registered envelope. Only the fields consumed here are declared,
// so auth-service can add fields without breaking this consumer.
type UserRegistered struct {
	UserID    string `json:"user_id"`
	Username  string `json:"username"`
	Email     string `json:"email"`
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
	Role      string `json:"role"`
}

// Provisioner creates a patient profile from an identity event.
type Provisioner struct {
	Store store.Store
	Log   zerolog.Logger
}

// Handle processes one envelope. Non-patient registrations and already
// provisioned users are treated as success so the offset advances instead of
// the message being retried forever.
func (p *Provisioner) Handle(ctx context.Context, env kafkautil.Envelope) error {
	if env.Event != "auth.user.registered" {
		return nil
	}

	var payload UserRegistered
	if err := json.Unmarshal(env.Data, &payload); err != nil {
		// A malformed payload will never become valid on retry.
		p.Log.Error().Err(err).Msg("discarding unparseable auth.user.registered event")
		return nil
	}

	if payload.Role != "patient" {
		return nil
	}
	if payload.UserID == "" {
		p.Log.Warn().Msg("discarding auth.user.registered event without user_id")
		return nil
	}

	now := time.Now().UTC()
	patient := domain.Patient{
		ID:             uuid.NewString(),
		KeycloakUserID: payload.UserID,
		FullName:       fullName(payload),
		Email:          payload.Email,
		Allergies:      []string{},
		Onboarded:      false,
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	err := p.Store.Create(ctx, patient)
	if errors.Is(err, domain.ErrDuplicate) {
		p.Log.Debug().Str("user_id", payload.UserID).Msg("patient already provisioned, skipping")
		return nil
	}
	if err != nil {
		return fmt.Errorf("provision patient for user %s: %w", payload.UserID, err)
	}

	p.Log.Info().
		Str("user_id", payload.UserID).
		Str("patient_id", patient.ID).
		Msg("patient profile provisioned from identity event")
	return nil
}

func fullName(p UserRegistered) string {
	switch {
	case p.FirstName != "" && p.LastName != "":
		return p.FirstName + " " + p.LastName
	case p.FirstName != "":
		return p.FirstName
	default:
		return p.Username
	}
}
