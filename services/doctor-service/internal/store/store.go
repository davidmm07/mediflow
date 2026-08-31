// Package store persists doctors and their availability slots in MongoDB.
// The Store interface is what the API layer depends on, which lets Pact
// provider verification swap in an in-memory implementation to set up
// provider states without a database.
package store

import (
	"context"
	"errors"
	"time"

	"github.com/davidmm07/mediflow/services/doctor-service/internal/domain"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Store is the persistence contract for doctor-service.
type Store interface {
	CreateDoctor(ctx context.Context, d domain.Doctor) error
	GetDoctor(ctx context.Context, id string) (domain.Doctor, error)
	ListDoctors(ctx context.Context, specialty string) ([]domain.Doctor, error)
	CreateSlot(ctx context.Context, s domain.Slot) error
	ListSlots(ctx context.Context, doctorID string, from, to time.Time, onlyFree bool) ([]domain.Slot, error)
	GetSlot(ctx context.Context, slotID string) (domain.Slot, error)
	ReserveSlot(ctx context.Context, slotID, appointmentID string) (domain.Slot, error)
	ReleaseSlot(ctx context.Context, slotID string) error
}

// Mongo is the MongoDB-backed Store.
type Mongo struct {
	doctors *mongo.Collection
	slots   *mongo.Collection
}

// NewMongo wires the collections and creates the indexes the queries rely on.
func NewMongo(ctx context.Context, db *mongo.Database) (*Mongo, error) {
	m := &Mongo{
		doctors: db.Collection("doctors"),
		slots:   db.Collection("slots"),
	}

	if _, err := m.doctors.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "keycloak_user_id", Value: 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "specialty", Value: 1}, {Key: "active", Value: 1}}},
	}); err != nil {
		return nil, err
	}

	if _, err := m.slots.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "doctor_id", Value: 1}, {Key: "starts_at", Value: 1}},
	}); err != nil {
		return nil, err
	}

	return m, nil
}

// CreateDoctor inserts a profile, mapping a unique-index violation to
// domain.ErrDuplicate.
func (m *Mongo) CreateDoctor(ctx context.Context, d domain.Doctor) error {
	_, err := m.doctors.InsertOne(ctx, d)
	if mongo.IsDuplicateKeyError(err) {
		return domain.ErrDuplicate
	}
	return err
}

// GetDoctor fetches one profile by id.
func (m *Mongo) GetDoctor(ctx context.Context, id string) (domain.Doctor, error) {
	var d domain.Doctor
	err := m.doctors.FindOne(ctx, bson.M{"_id": id}).Decode(&d)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return domain.Doctor{}, domain.ErrNotFound
	}
	return d, err
}

// ListDoctors returns active profiles, optionally narrowed to one specialty.
func (m *Mongo) ListDoctors(ctx context.Context, specialty string) ([]domain.Doctor, error) {
	filter := bson.M{"active": true}
	if specialty != "" {
		filter["specialty"] = specialty
	}

	cur, err := m.doctors.Find(ctx, filter, options.Find().SetSort(bson.D{{Key: "full_name", Value: 1}}))
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	doctors := []domain.Doctor{}
	if err := cur.All(ctx, &doctors); err != nil {
		return nil, err
	}
	return doctors, nil
}

// CreateSlot inserts an availability window, rejecting it when it overlaps
// an existing one for the same doctor.
func (m *Mongo) CreateSlot(ctx context.Context, s domain.Slot) error {
	overlapping := bson.M{
		"doctor_id": s.DoctorID,
		"starts_at": bson.M{"$lt": s.EndsAt},
		"ends_at":   bson.M{"$gt": s.StartsAt},
	}

	count, err := m.slots.CountDocuments(ctx, overlapping)
	if err != nil {
		return err
	}
	if count > 0 {
		return errors.New("slot overlaps an existing availability window")
	}

	_, err = m.slots.InsertOne(ctx, s)
	return err
}

// ListSlots returns a doctor's slots in a time range, optionally only the
// unreserved ones.
func (m *Mongo) ListSlots(ctx context.Context, doctorID string, from, to time.Time, onlyFree bool) ([]domain.Slot, error) {
	filter := bson.M{"doctor_id": doctorID}
	if !from.IsZero() || !to.IsZero() {
		window := bson.M{}
		if !from.IsZero() {
			window["$gte"] = from
		}
		if !to.IsZero() {
			window["$lte"] = to
		}
		filter["starts_at"] = window
	}
	if onlyFree {
		filter["reserved"] = false
	}

	cur, err := m.slots.Find(ctx, filter, options.Find().SetSort(bson.D{{Key: "starts_at", Value: 1}}))
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	slots := []domain.Slot{}
	if err := cur.All(ctx, &slots); err != nil {
		return nil, err
	}
	return slots, nil
}

// GetSlot fetches a single slot by id.
func (m *Mongo) GetSlot(ctx context.Context, slotID string) (domain.Slot, error) {
	var s domain.Slot
	err := m.slots.FindOne(ctx, bson.M{"_id": slotID}).Decode(&s)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return domain.Slot{}, domain.ErrSlotNotFound
	}
	return s, err
}

// ReserveSlot atomically flips a free slot to reserved. The filter includes
// reserved:false so two concurrent bookings can't both win. The loser gets
// ErrSlotTaken instead of silently overwriting the winner.
func (m *Mongo) ReserveSlot(ctx context.Context, slotID, appointmentID string) (domain.Slot, error) {
	filter := bson.M{"_id": slotID, "reserved": false}
	update := bson.M{"$set": bson.M{"reserved": true, "appointment_id": appointmentID}}

	var updated domain.Slot
	err := m.slots.FindOneAndUpdate(ctx, filter, update,
		options.FindOneAndUpdate().SetReturnDocument(options.After)).Decode(&updated)

	if errors.Is(err, mongo.ErrNoDocuments) {
		// Either the slot doesn't exist at all, or it exists but is taken.
		if _, getErr := m.GetSlot(ctx, slotID); errors.Is(getErr, domain.ErrSlotNotFound) {
			return domain.Slot{}, domain.ErrSlotNotFound
		}
		return domain.Slot{}, domain.ErrSlotTaken
	}
	return updated, err
}

// ReleaseSlot returns a slot to the pool after a cancellation.
func (m *Mongo) ReleaseSlot(ctx context.Context, slotID string) error {
	res, err := m.slots.UpdateOne(ctx,
		bson.M{"_id": slotID},
		bson.M{"$set": bson.M{"reserved": false}, "$unset": bson.M{"appointment_id": ""}},
	)
	if err != nil {
		return err
	}
	if res.MatchedCount == 0 {
		return domain.ErrSlotNotFound
	}
	return nil
}
