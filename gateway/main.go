// Command gateway is MediFlow's single public entry point. It terminates
// client traffic, validates the Keycloak access token once, and fans requests
// out to the service that owns each resource.
package main

import (
	"context"

	"github.com/davidmm07/mediflow/common/authmw"
	"github.com/davidmm07/mediflow/common/config"
	"github.com/davidmm07/mediflow/common/logger"
	"github.com/davidmm07/mediflow/common/server"
	"github.com/davidmm07/mediflow/gateway/internal/proxy"
)

func main() {
	log := logger.New("gateway")
	ctx := context.Background()

	verifier, err := authmw.NewVerifier(ctx, config.MustGet("KEYCLOAK_ISSUER"), config.Get("KEYCLOAK_INTERNAL_URL", ""))
	if err != nil {
		log.Fatal().Err(err).Msg("cannot reach Keycloak JWKS")
	}

	routes := []proxy.Route{
		// Registration must be reachable without a token, since it is how a
		// user obtains one in the first place.
		{Prefix: "/auth", Upstream: config.MustGet("AUTH_SERVICE_URL"), Public: true},
		{Prefix: "/doctors", Upstream: config.MustGet("DOCTOR_SERVICE_URL")},
		{Prefix: "/patients", Upstream: config.MustGet("PATIENT_SERVICE_URL")},
		{
			Prefix:   "/appointments",
			Upstream: config.MustGet("APPOINTMENT_SERVICE_URL"),
			// Reads as a sub-resource of the practitioner but is owned here,
			// so it must not fall through to doctor-service.
			Patterns: []string{"/doctors/{doctorID}/appointments"},
		},
		{Prefix: "/notifications", Upstream: config.MustGet("NOTIFICATION_SERVICE_URL")},
	}

	gw, err := proxy.New(verifier, log, routes)
	if err != nil {
		log.Fatal().Err(err).Msg("invalid upstream configuration")
	}

	// Empty by default: a browser-facing origin has to be named explicitly.
	// Locally that is the Swagger UI container, which is served from its own
	// port and so is cross-origin to every call it makes.
	if origins := config.GetList("CORS_ALLOWED_ORIGINS", nil); len(origins) > 0 {
		gw.AllowOrigins(origins)
		log.Info().Strs("origins", origins).Msg("CORS enabled")
	}

	if err := server.Run(config.Addr(), gw.Handler(), log, nil); err != nil {
		log.Fatal().Err(err).Msg("server stopped unexpectedly")
	}
}
