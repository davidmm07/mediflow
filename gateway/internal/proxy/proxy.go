// Package proxy implements MediFlow's edge: one public port that validates
// the caller's token once and forwards the request to the owning service.
//
// The token is *not* stripped before forwarding. Backend services verify it
// again against the same JWKS, so a service reached directly (from another
// pod, or by a misconfigured ingress) is still protected. The gateway is a
// convenience and a first filter, never the only gate.
package proxy

import (
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"

	"github.com/davidmm07/mediflow/common/authmw"
	"github.com/davidmm07/mediflow/common/httpx"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/cors"
	"github.com/rs/zerolog"
)

// Route binds a public path prefix to an upstream service.
type Route struct {
	Prefix   string
	Upstream string
	Public   bool

	// Patterns are extra chi patterns that belong to this upstream even
	// though they sit under another service's prefix. A practitioner's
	// agenda reads as /doctors/{doctorID}/appointments but is owned by
	// appointment-service, and chi matches a concrete pattern ahead of the
	// /doctors/* wildcard, so the nested path reaches the right service.
	Patterns []string
}

// Gateway routes public traffic to MediFlow's services.
type Gateway struct {
	verifier       *authmw.Verifier
	log            zerolog.Logger
	proxies        map[string]*httputil.ReverseProxy
	routes         []Route
	allowedOrigins []string
}

// AllowOrigins enables CORS for the given browser origins. It is off unless
// configured, because a gateway that echoes any origin is a gateway that lets
// any page spend a user's token.
//
// The order matters more than it looks: CORS has to run before the token check
// so a preflight, which carries no Authorization header by design, is answered
// instead of being rejected with a 401 the browser then reports as an opaque
// network failure.
func (g *Gateway) AllowOrigins(origins []string) {
	g.allowedOrigins = origins
}

// New builds a Gateway for the given routes, failing if any upstream URL is
// unparseable. A typo in configuration should stop the process at boot rather
// than produce 502s later.
func New(verifier *authmw.Verifier, log zerolog.Logger, routes []Route) (*Gateway, error) {
	g := &Gateway{
		verifier: verifier,
		log:      log,
		proxies:  make(map[string]*httputil.ReverseProxy, len(routes)),
		routes:   routes,
	}

	for _, route := range routes {
		target, err := url.Parse(route.Upstream)
		if err != nil {
			return nil, err
		}

		rp := httputil.NewSingleHostReverseProxy(target)
		rp.Transport = &http.Transport{
			MaxIdleConnsPerHost: 32,
			IdleConnTimeout:     90 * time.Second,
		}
		rp.ErrorHandler = g.upstreamError(route.Prefix)
		g.proxies[route.Prefix] = rp
	}

	return g, nil
}

// Handler builds the public router.
func (g *Gateway) Handler() http.Handler {
	r := chi.NewRouter()
	r.Use(httpx.RequestIDMiddleware)

	// Mounted as router middleware so it sits ahead of every route's token
	// check and can answer a preflight on its own.
	if len(g.allowedOrigins) > 0 {
		r.Use(cors.Handler(cors.Options{
			AllowedOrigins: g.allowedOrigins,
			AllowedMethods: []string{
				http.MethodGet, http.MethodPost, http.MethodPut,
				http.MethodDelete, http.MethodOptions,
			},
			AllowedHeaders:   []string{"Authorization", "Content-Type", "X-Request-Id"},
			ExposedHeaders:   []string{"X-Request-Id"},
			AllowCredentials: false,
			MaxAge:           300,
		}))
	}

	r.Get("/health", httpx.HealthHandler)

	for _, route := range g.routes {
		handler := g.forward(route.Prefix)
		if !route.Public {
			handler = g.verifier.Middleware(handler)
		}
		// Handle both the collection root and everything beneath it.
		r.Handle(route.Prefix, handler)
		r.Handle(route.Prefix+"/*", handler)

		for _, pattern := range route.Patterns {
			r.Handle(pattern, handler)
		}
	}

	return r
}

func (g *Gateway) forward(prefix string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxy, ok := g.proxies[prefix]
		if !ok {
			httpx.WriteError(w, http.StatusNotFound, "unknown route")
			return
		}

		// Pass the correlation id downstream so one booking is traceable
		// end to end across the gateway, appointment-service and
		// doctor-service logs.
		if id := httpx.RequestID(r); id != "" {
			r.Header.Set("X-Request-Id", id)
		}

		if claims, ok := authmw.FromContext(r.Context()); ok {
			r.Header.Set("X-MediFlow-User", claims.Subject)
			r.Header.Set("X-MediFlow-Roles", strings.Join(claims.RealmRoles, ","))
		}

		proxy.ServeHTTP(w, r)
	})
}

func (g *Gateway) upstreamError(prefix string) func(http.ResponseWriter, *http.Request, error) {
	return func(w http.ResponseWriter, r *http.Request, err error) {
		g.log.Error().Err(err).
			Str("prefix", prefix).
			Str("path", r.URL.Path).
			Msg("upstream request failed")
		httpx.WriteError(w, http.StatusBadGateway, "upstream service unavailable")
	}
}
