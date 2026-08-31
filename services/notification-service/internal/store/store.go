// Package store persists notifications in MongoDB.
package store

import (
	"context"
	"sort"
	"sync"

	"github.com/davidmm07/mediflow/services/notification-service/internal/domain"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Store is the persistence contract for notification-service.
type Store interface {
	Save(ctx context.Context, n domain.Notification) error
	ListByUser(ctx context.Context, userID string, limit int64) ([]domain.Notification, error)
	MarkRead(ctx context.Context, userID, notificationID string) error
}

// Mongo is the MongoDB-backed Store.
type Mongo struct {
	notifications *mongo.Collection
}

// NewMongo wires the collection and its inbox index.
func NewMongo(ctx context.Context, db *mongo.Database) (*Mongo, error) {
	m := &Mongo{notifications: db.Collection("notifications")}

	_, err := m.notifications.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "user_id", Value: 1}, {Key: "created_at", Value: -1}},
	})
	if err != nil {
		return nil, err
	}
	return m, nil
}

// Save inserts a notification.
func (m *Mongo) Save(ctx context.Context, n domain.Notification) error {
	_, err := m.notifications.InsertOne(ctx, n)
	return err
}

// ListByUser returns a user's inbox, newest first.
func (m *Mongo) ListByUser(ctx context.Context, userID string, limit int64) ([]domain.Notification, error) {
	cur, err := m.notifications.Find(ctx,
		bson.M{"user_id": userID},
		options.Find().SetSort(bson.D{{Key: "created_at", Value: -1}}).SetLimit(limit),
	)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	out := []domain.Notification{}
	if err := cur.All(ctx, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// MarkRead flips the read flag, scoped to the owning user so one account
// can't mutate another's inbox.
func (m *Mongo) MarkRead(ctx context.Context, userID, notificationID string) error {
	_, err := m.notifications.UpdateOne(ctx,
		bson.M{"_id": notificationID, "user_id": userID},
		bson.M{"$set": bson.M{"read": true}},
	)
	return err
}

// Memory is an in-memory Store used by the unit and message-pact suites.
type Memory struct {
	mu            sync.RWMutex
	notifications map[string]domain.Notification
}

// NewMemory returns an empty in-memory store.
func NewMemory() *Memory {
	return &Memory{notifications: make(map[string]domain.Notification)}
}

func (m *Memory) Save(_ context.Context, n domain.Notification) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.notifications[n.ID] = n
	return nil
}

func (m *Memory) ListByUser(_ context.Context, userID string, limit int64) ([]domain.Notification, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := []domain.Notification{}
	for _, n := range m.notifications {
		if n.UserID == userID {
			out = append(out, n)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })

	if limit > 0 && int64(len(out)) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (m *Memory) MarkRead(_ context.Context, userID, notificationID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	n, ok := m.notifications[notificationID]
	if !ok || n.UserID != userID {
		return nil
	}
	n.Read = true
	m.notifications[notificationID] = n
	return nil
}

// All returns every stored notification, for assertions in tests.
func (m *Memory) All() []domain.Notification {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make([]domain.Notification, 0, len(m.notifications))
	for _, n := range m.notifications {
		out = append(out, n)
	}
	return out
}
