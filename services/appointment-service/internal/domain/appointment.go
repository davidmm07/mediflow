// Package domain holds appointment-service's core types and the booking
// state machine.
package domain

import (
	"errors"
	"time"
)

// Errors surfaced by the store and service layers.
var (
	ErrNotFound        = errors.New("appointment not found")
	ErrSlotUnavailable = errors.New("the requested slot is no longer available")
	ErrDoctorNotFound  = errors.New("doctor not found")
	ErrNotCancellable  = errors.New("appointment cannot be cancelled in its current state")
	ErrForbidden       = errors.New("appointment does not belong to this user")
)

// Status is the lifecycle state of an appointment.
type Status string

// The booking lifecycle. Completion is out of scope for the portfolio demo;
// what matters is that a cancellation releases the doctor's slot.
const (
	StatusConfirmed Status = "confirmed"
	StatusCancelled Status = "cancelled"
	StatusCompleted Status = "completed"
)

// Appointment is a confirmed booking between a patient and a doctor for a
// specific availability slot.
type Appointment struct {
	ID              string    `json:"id" bson:"_id"`
	PatientUserID   string    `json:"patient_user_id" bson:"patient_user_id"`
	PatientName     string    `json:"patient_name" bson:"patient_name"`
	DoctorID        string    `json:"doctor_id" bson:"doctor_id"`
	DoctorName      string    `json:"doctor_name" bson:"doctor_name"`
	Specialty       string    `json:"specialty" bson:"specialty"`
	SlotID          string    `json:"slot_id" bson:"slot_id"`
	StartsAt        time.Time `json:"starts_at" bson:"starts_at"`
	EndsAt          time.Time `json:"ends_at" bson:"ends_at"`
	Status          Status    `json:"status" bson:"status"`
	Reason          string    `json:"reason" bson:"reason"`
	CancelledReason string    `json:"cancelled_reason,omitempty" bson:"cancelled_reason,omitempty"`
	CreatedAt       time.Time `json:"created_at" bson:"created_at"`
	UpdatedAt       time.Time `json:"updated_at" bson:"updated_at"`
}

// CanCancel reports whether the appointment may still be cancelled: only
// confirmed bookings, and only while the consultation hasn't started.
func (a Appointment) CanCancel(now time.Time) bool {
	return a.Status == StatusConfirmed && a.StartsAt.After(now)
}
