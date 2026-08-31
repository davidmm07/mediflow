// Command notification-service is MediFlow's pure event consumer: it holds no
// synchronous dependency on any other service and reacts only to what lands
// on Kafka. That isolation is what makes message-based contract testing the
// right tool for it: there is no HTTP call to pin down, only the shape of
// the events it consumes.
package main

import (
	"context"

	"github.com/davidmm07/mediflow/common/authmw"
	"github.com/davidmm07/mediflow/common/config"
	"github.com/davidmm07/mediflow/common/kafkautil"
	"github.com/davidmm07/mediflow/common/logger"
	"github.com/davidmm07/mediflow/common/mongoutil"
	"github.com/davidmm07/mediflow/common/server"
	"github.com/davidmm07/mediflow/services/notification-service/internal/api"
	"github.com/davidmm07/mediflow/services/notification-service/internal/events"
	"github.com/davidmm07/mediflow/services/notification-service/internal/store"
)

// subscribedTopics are the streams this service reacts to; each gets its own
// consumer goroutine sharing one group id per topic.
var subscribedTopics = []string{
	events.EventUserRegistered,
	events.EventAppointmentCreated,
	events.EventAppointmentCancelled,
}

func main() {
	log := logger.New("notification-service")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	verifier, err := authmw.NewVerifier(ctx, config.MustGet("KEYCLOAK_ISSUER"))
	if err != nil {
		log.Fatal().Err(err).Msg("cannot reach Keycloak JWKS")
	}

	client, db, err := mongoutil.Connect(config.MustGet("MONGO_URI"), config.Get("MONGO_DB", "notifications"))
	if err != nil {
		log.Fatal().Err(err).Msg("cannot connect to MongoDB")
	}

	mongoStore, err := store.NewMongo(ctx, db)
	if err != nil {
		log.Fatal().Err(err).Msg("cannot prepare MongoDB indexes")
	}

	notifier := &events.Notifier{Store: mongoStore, Log: log}
	brokers := config.GetList("KAFKA_BROKERS", []string{"localhost:9092"})

	consumers := make([]*kafkautil.Consumer, 0, len(subscribedTopics))
	for _, topic := range subscribedTopics {
		consumer := kafkautil.NewConsumer(brokers, topic, "notification-service")
		consumers = append(consumers, consumer)

		go func(topic string, c *kafkautil.Consumer) {
			if err := c.Run(ctx, notifier.Handle, func(err error) {
				log.Error().Err(err).Str("topic", topic).Msg("event processing error")
			}); err != nil {
				log.Error().Err(err).Str("topic", topic).Msg("consumer stopped")
			}
		}(topic, consumer)
	}

	handler := &api.Handler{Store: mongoStore, Verifier: verifier, Log: log}

	if err := server.Run(config.Addr(), handler.Routes(), log, func() {
		cancel()
		for _, c := range consumers {
			_ = c.Close()
		}
		_ = client.Disconnect(context.Background())
	}); err != nil {
		log.Fatal().Err(err).Msg("server stopped unexpectedly")
	}
}
