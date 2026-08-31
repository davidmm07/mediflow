// Package store persists patient profiles in MongoDB.
package store

import (
	"context"
	"errors"

	"github.com/davidmm07/mediflow/services/patient-service/internal/domain"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Store is the persistence contract for patient-service.
type Store interface {
	Create(ctx context.Context, p domain.Patient) error
	GetByUserID(ctx context.Context, keycloakUserID string) (domain.Patient, error)
	Update(ctx context.Context, p domain.Patient) error
}

// Mongo is the MongoDB-backed Store.
type Mongo struct {
	patients *mongo.Collection
}

// NewMongo wires the collection and enforces one profile per identity.
func NewMongo(ctx context.Context, db *mongo.Database) (*Mongo, error) {
	m := &Mongo{patients: db.Collection("patients")}

	_, err := m.patients.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "keycloak_user_id", Value: 1}},
		Options: options.Index().SetUnique(true),
	})
	if err != nil {
		return nil, err
	}
	return m, nil
}

// Create inserts a profile, mapping a unique-index violation to
// domain.ErrDuplicate. The event consumer relies on this so a redelivered
// auth.user.registered event is a no-op rather than a duplicate patient.
func (m *Mongo) Create(ctx context.Context, p domain.Patient) error {
	_, err := m.patients.InsertOne(ctx, p)
	if mongo.IsDuplicateKeyError(err) {
		return domain.ErrDuplicate
	}
	return err
}

// GetByUserID looks a profile up by its Keycloak subject.
func (m *Mongo) GetByUserID(ctx context.Context, keycloakUserID string) (domain.Patient, error) {
	var p domain.Patient
	err := m.patients.FindOne(ctx, bson.M{"keycloak_user_id": keycloakUserID}).Decode(&p)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return domain.Patient{}, domain.ErrNotFound
	}
	return p, err
}

// Update replaces the mutable fields of an existing profile.
func (m *Mongo) Update(ctx context.Context, p domain.Patient) error {
	res, err := m.patients.UpdateOne(ctx,
		bson.M{"_id": p.ID},
		bson.M{"$set": bson.M{
			"full_name":  p.FullName,
			"phone":      p.Phone,
			"birth_date": p.BirthDate,
			"blood_type": p.BloodType,
			"allergies":  p.Allergies,
			"onboarded":  p.Onboarded,
			"updated_at": p.UpdatedAt,
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
