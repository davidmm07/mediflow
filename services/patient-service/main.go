// Command patient-service owns patient profiles. It is provisioned reactively
// from auth-service's identity events, which is MediFlow's demonstration of
// eventual consistency across service boundaries: no synchronous call, no
// shared database, no distributed transaction.
package main

import (
	"context"

	"github.com/davidmm07/mediflow/common/authmw"
	"github.com/davidmm07/mediflow/common/config"
	"github.com/davidmm07/mediflow/common/kafkautil"
	"github.com/davidmm07/mediflow/common/logger"
	"github.com/davidmm07/mediflow/common/mongoutil"
	"github.com/davidmm07/mediflow/common/server"
	"github.com/davidmm07/mediflow/services/patient-service/internal/api"
	"github.com/davidmm07/mediflow/services/patient-service/internal/events"
	"github.com/davidmm07/mediflow/services/patient-service/internal/store"
)

func main() {
	log := logger.New("patient-service")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	verifier, err := authmw.NewVerifier(ctx, config.MustGet("KEYCLOAK_ISSUER"))
	if err != nil {
		log.Fatal().Err(err).Msg("cannot reach Keycloak JWKS")
	}

	client, db, err := mongoutil.Connect(config.MustGet("MONGO_URI"), config.Get("MONGO_DB", "patients"))
	if err != nil {
		log.Fatal().Err(err).Msg("cannot connect to MongoDB")
	}

	mongoStore, err := store.NewMongo(ctx, db)
	if err != nil {
		log.Fatal().Err(err).Msg("cannot prepare MongoDB indexes")
	}

	consumer := kafkautil.NewConsumer(
		config.GetList("KAFKA_BROKERS", []string{"localhost:9092"}),
		"auth.user.registered",
		"patient-service",
	)
	provisioner := &events.Provisioner{Store: mongoStore, Log: log}

	go func() {
		if err := consumer.Run(ctx, provisioner.Handle, func(err error) {
			log.Error().Err(err).Msg("event processing error")
		}); err != nil {
			log.Error().Err(err).Msg("consumer stopped")
		}
	}()

	handler := &api.Handler{Store: mongoStore, Verifier: verifier, Log: log}

	if err := server.Run(config.Addr(), handler.Routes(), log, func() {
		cancel()
		_ = consumer.Close()
		_ = client.Disconnect(context.Background())
	}); err != nil {
		log.Fatal().Err(err).Msg("server stopped unexpectedly")
	}
}
