// Package mongoutil provides a thin, opinionated connection helper so every
// MediFlow service opens Mongo the same way (timeouts, ping-on-connect)
// instead of hand-rolling client setup five times.
package mongoutil

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Connect dials uri, pings the server and returns the handle to database
// dbName. Each MediFlow service owns its own database (database-per-service),
// so dbName is typically just the service name.
func Connect(uri, dbName string) (*mongo.Client, *mongo.Database, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, nil, fmt.Errorf("mongoutil: connect: %w", err)
	}

	if err := client.Ping(ctx, nil); err != nil {
		return nil, nil, fmt.Errorf("mongoutil: ping: %w", err)
	}

	return client, client.Database(dbName), nil
}
