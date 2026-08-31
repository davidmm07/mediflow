// Package domain holds patient-service's core types. A patient record is
// created reactively from the auth.user.registered event, then enriched by
// the patient themselves through the API.
package domain

import (
	"errors"
	"time"
)

// Errors surfaced by the store.
var (
	ErrNotFound  = errors.New("patient not found")
	ErrDuplicate = errors.New("patient profile already exists for this user")
)

// Patient is the clinical-administrative profile MediFlow keeps for a person.
// Deliberately light on medical data: the point of the portfolio project is
// the distributed architecture, not a real EHR.
type Patient struct {
	ID             string    `json:"id" bson:"_id"`
	KeycloakUserID string    `json:"keycloak_user_id" bson:"keycloak_user_id"`
	FullName       string    `json:"full_name" bson:"full_name"`
	Email          string    `json:"email" bson:"email"`
	Phone          string    `json:"phone" bson:"phone"`
	BirthDate      string    `json:"birth_date" bson:"birth_date"`
	BloodType      string    `json:"blood_type" bson:"blood_type"`
	Allergies      []string  `json:"allergies" bson:"allergies"`
	Onboarded      bool      `json:"onboarded" bson:"onboarded"`
	CreatedAt      time.Time `json:"created_at" bson:"created_at"`
	UpdatedAt      time.Time `json:"updated_at" bson:"updated_at"`
}

var validBloodTypes = map[string]bool{
	"A+": true, "A-": true, "B+": true, "B-": true,
	"AB+": true, "AB-": true, "O+": true, "O-": true, "": true,
}

// Validate enforces the profile invariants applied on user-driven updates.
// It does not run on event-driven provisioning, where only a name and email
// are known.
func (p Patient) Validate() error {
	if p.FullName == "" {
		return errors.New("full_name is required")
	}
	if !validBloodTypes[p.BloodType] {
		return errors.New("blood_type must be one of A+, A-, B+, B-, AB+, AB-, O+, O-")
	}
	if p.BirthDate != "" {
		if _, err := time.Parse("2006-01-02", p.BirthDate); err != nil {
			return errors.New("birth_date must be formatted as YYYY-MM-DD")
		}
	}
	return nil
}
