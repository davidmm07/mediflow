// Command appointment-service is MediFlow's booking engine. It is the only
// service that talks to another one synchronously (doctor-service, for slot
// reservation) and the main producer of domain events, which makes it the
// natural place to demonstrate both consumer-driven HTTP contracts and
// message contracts.
package main

import (
	"context"

	"github.com/davidmm07/mediflow/common/authmw"
	"github.com/davidmm07/mediflow/common/config"
	"github.com/davidmm07/mediflow/common/kafkautil"
	"github.com/davidmm07/mediflow/common/logger"
	"github.com/davidmm07/mediflow/common/mongoutil"
	"github.com/davidmm07/mediflow/common/server"
	"github.com/davidmm07/mediflow/services/appointment-service/internal/api"
	"github.com/davidmm07/mediflow/services/appointment-service/internal/booking"
	"github.com/davidmm07/mediflow/services/appointment-service/internal/doctorclient"
	"github.com/davidmm07/mediflow/services/appointment-service/internal/store"
)

func main() {
	log := logger.New("appointment-service")
	ctx := context.Background()

	verifier, err := authmw.NewVerifier(ctx, config.MustGet("KEYCLOAK_ISSUER"))
	if err != nil {
		log.Fatal().Err(err).Msg("cannot reach Keycloak JWKS")
	}

	client, db, err := mongoutil.Connect(config.MustGet("MONGO_URI"), config.Get("MONGO_DB", "appointments"))
	if err != nil {
		log.Fatal().Err(err).Msg("cannot connect to MongoDB")
	}

	mongoStore, err := store.NewMongo(ctx, db)
	if err != nil {
		log.Fatal().Err(err).Msg("cannot prepare MongoDB indexes")
	}

	producer := kafkautil.NewMultiProducer(config.GetList("KAFKA_BROKERS", []string{"localhost:9092"}))

	bookingSvc := &booking.Service{
		Store:     mongoStore,
		Doctors:   doctorclient.New(config.MustGet("DOCTOR_SERVICE_URL")),
		Publisher: producer,
		Log:       log,
	}

	handler := &api.Handler{
		Booking:  bookingSvc,
		Store:    mongoStore,
		Verifier: verifier,
		Log:      log,
	}

	if err := server.Run(config.Addr(), handler.Routes(), log, func() {
		_ = producer.Close()
		_ = client.Disconnect(context.Background())
	}); err != nil {
		log.Fatal().Err(err).Msg("server stopped unexpectedly")
	}
}
