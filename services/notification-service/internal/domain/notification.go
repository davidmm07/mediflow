// Package domain holds notification-service's types and the rendering rules
// that turn a domain event into something a human reads.
package domain

import "time"

// Channel is the delivery medium. Only in-app is actually delivered in this
// portfolio build; email/SMS are recorded as intent so the fan-out shape is
// visible without wiring a real provider.
type Channel string

const (
	ChannelInApp Channel = "in_app"
	ChannelEmail Channel = "email"
)

// Kind categorises a notification so clients can filter and style them.
type Kind string

const (
	KindWelcome              Kind = "welcome"
	KindAppointmentConfirmed Kind = "appointment_confirmed"
	KindAppointmentCancelled Kind = "appointment_cancelled"
)

// Notification is one message addressed to one user.
type Notification struct {
	ID        string    `json:"id" bson:"_id"`
	UserID    string    `json:"user_id" bson:"user_id"`
	Kind      Kind      `json:"kind" bson:"kind"`
	Channel   Channel   `json:"channel" bson:"channel"`
	Title     string    `json:"title" bson:"title"`
	Body      string    `json:"body" bson:"body"`
	Read      bool      `json:"read" bson:"read"`
	SourceID  string    `json:"source_id" bson:"source_id"`
	CreatedAt time.Time `json:"created_at" bson:"created_at"`
}

// AppointmentTime renders a start time in the human format used in the
// notification body.
func AppointmentTime(t time.Time) string {
	return t.UTC().Format("Mon 02 Jan 2006 at 15:04 UTC")
}
