// Package domain holds doctor-service's core types and business rules,
// deliberately free of HTTP and MongoDB concerns so they can be unit-tested
// and reused by the Pact provider states.
package domain

import (
	"errors"
	"time"
)

// Errors surfaced by the store and mapped to HTTP status codes by the API
// layer.
var (
	ErrNotFound     = errors.New("doctor not found")
	ErrSlotNotFound = errors.New("availability slot not found")
	ErrSlotTaken    = errors.New("availability slot is already reserved")
	ErrDuplicate    = errors.New("doctor profile already exists for this user")
)

// Doctor is a practitioner's public profile as shown in the patient-facing
// directory.
type Doctor struct {
	ID              string    `json:"id" bson:"_id"`
	KeycloakUserID  string    `json:"keycloak_user_id" bson:"keycloak_user_id"`
	FullName        string    `json:"full_name" bson:"full_name"`
	Specialty       string    `json:"specialty" bson:"specialty"`
	LicenseNumber   string    `json:"license_number" bson:"license_number"`
	Bio             string    `json:"bio" bson:"bio"`
	ConsultationFee float64   `json:"consultation_fee" bson:"consultation_fee"`
	Languages       []string  `json:"languages" bson:"languages"`
	Active          bool      `json:"active" bson:"active"`
	CreatedAt       time.Time `json:"created_at" bson:"created_at"`
	UpdatedAt       time.Time `json:"updated_at" bson:"updated_at"`
}

// Slot is a bookable window in a doctor's calendar. Reserved flips to true
// when appointment-service claims it; the appointment id is kept so a
// cancellation can release exactly the right slot.
type Slot struct {
	ID            string    `json:"id" bson:"_id"`
	DoctorID      string    `json:"doctor_id" bson:"doctor_id"`
	StartsAt      time.Time `json:"starts_at" bson:"starts_at"`
	EndsAt        time.Time `json:"ends_at" bson:"ends_at"`
	Reserved      bool      `json:"reserved" bson:"reserved"`
	AppointmentID string    `json:"appointment_id,omitempty" bson:"appointment_id,omitempty"`
}

// Duration returns how long the slot lasts.
func (s Slot) Duration() time.Duration {
	return s.EndsAt.Sub(s.StartsAt)
}

// Validate enforces the scheduling invariants a slot must satisfy before it
// can be persisted.
func (s Slot) Validate() error {
	if s.StartsAt.IsZero() || s.EndsAt.IsZero() {
		return errors.New("slot requires starts_at and ends_at")
	}
	if !s.EndsAt.After(s.StartsAt) {
		return errors.New("slot ends_at must be after starts_at")
	}
	if s.Duration() < 10*time.Minute {
		return errors.New("slot must be at least 10 minutes long")
	}
	if s.Duration() > 3*time.Hour {
		return errors.New("slot must not exceed 3 hours")
	}
	return nil
}

// Overlaps reports whether two slots intersect in time. Used to reject
// double-booked calendars at write time.
func (s Slot) Overlaps(other Slot) bool {
	return s.StartsAt.Before(other.EndsAt) && other.StartsAt.Before(s.EndsAt)
}

// Validate enforces the profile invariants.
func (d Doctor) Validate() error {
	if d.FullName == "" {
		return errors.New("full_name is required")
	}
	if d.Specialty == "" {
		return errors.New("specialty is required")
	}
	if d.LicenseNumber == "" {
		return errors.New("license_number is required")
	}
	if d.ConsultationFee < 0 {
		return errors.New("consultation_fee cannot be negative")
	}
	return nil
}
