// Command auth-service is MediFlow's front door to the identity provider: it
// turns a public self-registration request into a Keycloak user with the
// right realm role and announces the new account on Kafka so the clinical
// services can provision their own view of the person.
package main

import (
	"context"

	"github.com/davidmm07/mediflow/common/authmw"
	"github.com/davidmm07/mediflow/common/config"
	"github.com/davidmm07/mediflow/common/kafkautil"
	"github.com/davidmm07/mediflow/common/logger"
	"github.com/davidmm07/mediflow/common/server"
	"github.com/davidmm07/mediflow/services/auth-service/internal/api"
	"github.com/davidmm07/mediflow/services/auth-service/internal/keycloak"
)

func main() {
	log := logger.New("auth-service")
	ctx := context.Background()

	verifier, err := authmw.NewVerifier(ctx, config.MustGet("KEYCLOAK_ISSUER"))
	if err != nil {
		log.Fatal().Err(err).Msg("cannot reach Keycloak JWKS")
	}

	kc := keycloak.New(keycloak.Config{
		BaseURL:      config.MustGet("KEYCLOAK_BASE_URL"),
		Realm:        config.Get("KEYCLOAK_REALM", "mediflow"),
		ClientID:     config.MustGet("KEYCLOAK_ADMIN_CLIENT_ID"),
		ClientSecret: config.MustGet("KEYCLOAK_ADMIN_CLIENT_SECRET"),
	})

	producer := kafkautil.NewProducer(
		config.GetList("KAFKA_BROKERS", []string{"localhost:9092"}),
		"auth.user.registered",
	)

	handler := &api.Handler{
		Registrar: kc,
		Publisher: producer,
		Verifier:  verifier,
		Log:       log,
	}

	if err := server.Run(config.Addr(), handler.Routes(), log, func() {
		_ = producer.Close()
	}); err != nil {
		log.Fatal().Err(err).Msg("server stopped unexpectedly")
	}
}
