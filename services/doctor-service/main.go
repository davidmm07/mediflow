// Command doctor-service owns the practitioner directory and the source of
// truth for availability. It is MediFlow's only synchronous dependency in the
// booking flow, which is why its HTTP contract with appointment-service is
// pinned by Pact.
package main

import (
	"context"

	"github.com/davidmm07/mediflow/common/authmw"
	"github.com/davidmm07/mediflow/common/config"
	"github.com/davidmm07/mediflow/common/logger"
	"github.com/davidmm07/mediflow/common/mongoutil"
	"github.com/davidmm07/mediflow/common/server"
	"github.com/davidmm07/mediflow/services/doctor-service/internal/api"
	"github.com/davidmm07/mediflow/services/doctor-service/internal/store"
)

func main() {
	log := logger.New("doctor-service")
	ctx := context.Background()

	verifier, err := authmw.NewVerifier(ctx, config.MustGet("KEYCLOAK_ISSUER"))
	if err != nil {
		log.Fatal().Err(err).Msg("cannot reach Keycloak JWKS")
	}

	client, db, err := mongoutil.Connect(config.MustGet("MONGO_URI"), config.Get("MONGO_DB", "doctors"))
	if err != nil {
		log.Fatal().Err(err).Msg("cannot connect to MongoDB")
	}

	mongoStore, err := store.NewMongo(ctx, db)
	if err != nil {
		log.Fatal().Err(err).Msg("cannot prepare MongoDB indexes")
	}

	handler := &api.Handler{Store: mongoStore, Verifier: verifier, Log: log}

	if err := server.Run(config.Addr(), handler.Routes(), log, func() {
		_ = client.Disconnect(context.Background())
	}); err != nil {
		log.Fatal().Err(err).Msg("server stopped unexpectedly")
	}
}
