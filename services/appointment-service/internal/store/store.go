// Package store persists appointments in MongoDB.
package store

import (
	"context"
	"errors"
	"sync"

	"github.com/davidmm07/mediflow/services/appointment-service/internal/domain"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Store is the persistence contract for appointment-service.
type Store interface {
	Create(ctx context.Context, a domain.Appointment) error
	Get(ctx context.Context, id string) (domain.Appointment, error)
	ListByPatient(ctx context.Context, patientUserID string) ([]domain.Appointment, error)
	ListByDoctor(ctx context.Context, doctorID string) ([]domain.Appointment, error)
	Update(ctx context.Context, a domain.Appointment) error
}

// Mongo is the MongoDB-backed Store.
type Mongo struct {
	appointments *mongo.Collection
}

// NewMongo wires the collection and creates the agenda indexes.
func NewMongo(ctx context.Context, db *mongo.Database) (*Mongo, error) {
	m := &Mongo{appointments: db.Collection("appointments")}

	if _, err := m.appointments.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "patient_user_id", Value: 1}, {Key: "starts_at", Value: -1}}},
		{Keys: bson.D{{Key: "doctor_id", Value: 1}, {Key: "starts_at", Value: -1}}},
		{Keys: bson.D{{Key: "slot_id", Value: 1}}, Options: options.Index().SetUnique(true)},
	}); err != nil {
		return nil, err
	}
	return m, nil
}

// Create inserts a booking. The unique slot_id index is the last line of
// defence against two appointments claiming one slot.
func (m *Mongo) Create(ctx context.Context, a domain.Appointment) error {
	_, err := m.appointments.InsertOne(ctx, a)
	if mongo.IsDuplicateKeyError(err) {
		return domain.ErrSlotUnavailable
	}
	return err
}

// Get fetches one appointment by id.
func (m *Mongo) Get(ctx context.Context, id string) (domain.Appointment, error) {
	var a domain.Appointment
	err := m.appointments.FindOne(ctx, bson.M{"_id": id}).Decode(&a)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return domain.Appointment{}, domain.ErrNotFound
	}
	return a, err
}

// ListByPatient returns a patient's agenda, newest first.
func (m *Mongo) ListByPatient(ctx context.Context, patientUserID string) ([]domain.Appointment, error) {
	return m.list(ctx, bson.M{"patient_user_id": patientUserID})
}

// ListByDoctor returns a doctor's agenda, newest first.
func (m *Mongo) ListByDoctor(ctx context.Context, doctorID string) ([]domain.Appointment, error) {
	return m.list(ctx, bson.M{"doctor_id": doctorID})
}

func (m *Mongo) list(ctx context.Context, filter bson.M) ([]domain.Appointment, error) {
	cur, err := m.appointments.Find(ctx, filter,
		options.Find().SetSort(bson.D{{Key: "starts_at", Value: -1}}))
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	out := []domain.Appointment{}
	if err := cur.All(ctx, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// Update persists the mutable fields of an appointment.
func (m *Mongo) Update(ctx context.Context, a domain.Appointment) error {
	res, err := m.appointments.UpdateOne(ctx,
		bson.M{"_id": a.ID},
		bson.M{"$set": bson.M{
			"status":           a.Status,
			"cancelled_reason": a.CancelledReason,
			"updated_at":       a.UpdatedAt,
		}},
	)
	if err != nil {
		return err
	}
	if res.MatchedCount == 0 {
		return domain.ErrNotFound
	}
	return nil
}

// Memory is an in-memory Store used by the unit and contract test suites.
type Memory struct {
	mu           sync.RWMutex
	appointments map[string]domain.Appointment
}

// NewMemory returns an empty in-memory store.
func NewMemory() *Memory {
	return &Memory{appointments: make(map[string]domain.Appointment)}
}

func (m *Memory) Create(_ context.Context, a domain.Appointment) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, existing := range m.appointments {
		if existing.SlotID == a.SlotID && existing.Status != domain.StatusCancelled {
			return domain.ErrSlotUnavailable
		}
	}
	m.appointments[a.ID] = a
	return nil
}

func (m *Memory) Get(_ context.Context, id string) (domain.Appointment, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	a, ok := m.appointments[id]
	if !ok {
		return domain.Appointment{}, domain.ErrNotFound
	}
	return a, nil
}

func (m *Memory) ListByPatient(_ context.Context, patientUserID string) ([]domain.Appointment, error) {
	return m.filter(func(a domain.Appointment) bool { return a.PatientUserID == patientUserID }), nil
}

func (m *Memory) ListByDoctor(_ context.Context, doctorID string) ([]domain.Appointment, error) {
	return m.filter(func(a domain.Appointment) bool { return a.DoctorID == doctorID }), nil
}

func (m *Memory) filter(keep func(domain.Appointment) bool) []domain.Appointment {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := []domain.Appointment{}
	for _, a := range m.appointments {
		if keep(a) {
			out = append(out, a)
		}
	}
	return out
}

func (m *Memory) Update(_ context.Context, a domain.Appointment) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.appointments[a.ID]; !ok {
		return domain.ErrNotFound
	}
	m.appointments[a.ID] = a
	return nil
}
