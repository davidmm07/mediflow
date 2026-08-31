// Package api exposes auth-service's HTTP surface: self-service registration
// (delegated to Keycloak) and token introspection for clients that want to
// know who they are without decoding the JWT themselves.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/mail"
	"strings"

	"github.com/davidmm07/mediflow/common/authmw"
	"github.com/davidmm07/mediflow/common/httpx"
	"github.com/davidmm07/mediflow/services/auth-service/internal/keycloak"
	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"
)

// UserRegistrar creates identities in the identity provider. The interface
// exists so handler tests (and Pact provider verification) can substitute a
// stub instead of standing up Keycloak.
type UserRegistrar interface {
	CreateUser(ctx context.Context, u keycloak.NewUser) (string, error)
}

// EventPublisher emits a domain event after a successful registration.
// patient-service consumes it to auto-provision the clinical profile.
type EventPublisher interface {
	Publish(ctx context.Context, event string, version int, key string, data interface{}) error
}

// TokenVerifier guards the authenticated routes. In production this is a
// *authmw.Verifier hitting Keycloak's JWKS; tests inject a stub so the suite
// doesn't need a live identity provider.
type TokenVerifier interface {
	Middleware(next http.Handler) http.Handler
}

// Handler wires the dependencies of auth-service's endpoints.
type Handler struct {
	Registrar UserRegistrar
	Publisher EventPublisher
	Verifier  TokenVerifier
	Log       zerolog.Logger
}

// RegisterRequest is the public self-registration payload.
type RegisterRequest struct {
	Username  string `json:"username"`
	Email     string `json:"email"`
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
	Password  string `json:"password"`
	Role      string `json:"role"`
}

// RegisterResponse is returned on a successful registration.
type RegisterResponse struct {
	UserID   string `json:"user_id"`
	Username string `json:"username"`
	Role     string `json:"role"`
}

// MeResponse describes the caller as derived from their access token.
type MeResponse struct {
	UserID   string   `json:"user_id"`
	Username string   `json:"username"`
	Email    string   `json:"email"`
	Roles    []string `json:"roles"`
}

// UserRegisteredEvent is the payload of the auth.user.registered event.
type UserRegisteredEvent struct {
	UserID    string `json:"user_id"`
	Username  string `json:"username"`
	Email     string `json:"email"`
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
	Role      string `json:"role"`
}

// Routes builds the service's router. Registration is deliberately public
// (it is how a patient gets their first credential); everything else sits
// behind bearer-token verification.
func (h *Handler) Routes() http.Handler {
	r := chi.NewRouter()
	r.Use(httpx.RequestIDMiddleware)

	r.Get("/health", httpx.HealthHandler)
	r.Post("/auth/register", h.register)

	r.Group(func(private chi.Router) {
		private.Use(h.Verifier.Middleware)
		private.Get("/auth/me", h.me)
	})

	return r
}

func (h *Handler) register(w http.ResponseWriter, r *http.Request) {
	var req RegisterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, "malformed JSON body")
		return
	}

	if msg, ok := validateRegister(req); !ok {
		httpx.WriteError(w, http.StatusBadRequest, msg)
		return
	}

	userID, err := h.Registrar.CreateUser(r.Context(), keycloak.NewUser{
		Username:  req.Username,
		Email:     req.Email,
		FirstName: req.FirstName,
		LastName:  req.LastName,
		Password:  req.Password,
		Role:      req.Role,
	})
	if errors.Is(err, keycloak.ErrUserExists) {
		httpx.WriteError(w, http.StatusConflict, "username or email already registered")
		return
	}
	if err != nil {
		h.Log.Error().Err(err).Str("username", req.Username).Msg("registration failed")
		httpx.WriteError(w, http.StatusBadGateway, "identity provider unavailable")
		return
	}

	// Registration already succeeded in Keycloak, so a broker hiccup must not
	// fail the request. Downstream provisioning is eventually consistent.
	if err := h.Publisher.Publish(r.Context(), "auth.user.registered", 1, userID, UserRegisteredEvent{
		UserID:    userID,
		Username:  req.Username,
		Email:     req.Email,
		FirstName: req.FirstName,
		LastName:  req.LastName,
		Role:      req.Role,
	}); err != nil {
		h.Log.Error().Err(err).Str("user_id", userID).Msg("publish auth.user.registered failed")
	}

	httpx.WriteJSON(w, http.StatusCreated, RegisterResponse{
		UserID:   userID,
		Username: req.Username,
		Role:     req.Role,
	})
}

func (h *Handler) me(w http.ResponseWriter, r *http.Request) {
	claims, ok := authmw.FromContext(r.Context())
	if !ok {
		httpx.WriteError(w, http.StatusUnauthorized, "unauthenticated")
		return
	}

	roles := claims.RealmRoles
	if roles == nil {
		roles = []string{}
	}

	httpx.WriteJSON(w, http.StatusOK, MeResponse{
		UserID:   claims.Subject,
		Username: claims.PreferredUsername,
		Email:    claims.Email,
		Roles:    roles,
	})
}

// validateRegister enforces the input rules at the system boundary; it
// returns a human-readable message when the payload is unusable.
func validateRegister(req RegisterRequest) (string, bool) {
	if strings.TrimSpace(req.Username) == "" {
		return "username is required", false
	}
	if _, err := mail.ParseAddress(req.Email); err != nil {
		return "a valid email is required", false
	}
	if len(req.Password) < 8 {
		return "password must be at least 8 characters", false
	}
	if req.Role != "patient" && req.Role != "doctor" {
		return "role must be one of: patient, doctor", false
	}
	return "", true
}
