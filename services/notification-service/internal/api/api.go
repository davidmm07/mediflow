// Package api exposes the notification inbox. Like patient-service, the
// identity always comes from the token: there is no route that lets a caller
// name someone else's inbox.
package api

import (
	"net/http"
	"strconv"

	"github.com/davidmm07/mediflow/common/authmw"
	"github.com/davidmm07/mediflow/common/httpx"
	"github.com/davidmm07/mediflow/services/notification-service/internal/store"
	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"
)

// TokenVerifier guards authenticated routes.
type TokenVerifier interface {
	Middleware(next http.Handler) http.Handler
}

// Handler holds notification-service's dependencies.
type Handler struct {
	Store    store.Store
	Verifier TokenVerifier
	Log      zerolog.Logger
}

const (
	defaultInboxLimit = 20
	maxInboxLimit     = 100
)

// Routes builds the router.
func (h *Handler) Routes() http.Handler {
	r := chi.NewRouter()
	r.Use(httpx.RequestIDMiddleware)
	r.Get("/health", httpx.HealthHandler)

	r.Group(func(private chi.Router) {
		private.Use(h.Verifier.Middleware)
		private.Get("/notifications/me", h.listMine)
		private.Post("/notifications/{notificationID}/read", h.markRead)
	})

	return r
}

func (h *Handler) listMine(w http.ResponseWriter, r *http.Request) {
	claims, _ := authmw.FromContext(r.Context())

	limit := int64(defaultInboxLimit)
	if raw := r.URL.Query().Get("limit"); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || parsed < 1 || parsed > maxInboxLimit {
			httpx.WriteError(w, http.StatusBadRequest, "limit must be an integer between 1 and 100")
			return
		}
		limit = parsed
	}

	notifications, err := h.Store.ListByUser(r.Context(), claims.Subject, limit)
	if err != nil {
		h.Log.Error().Err(err).Msg("list notifications failed")
		httpx.WriteError(w, http.StatusInternalServerError, "could not list notifications")
		return
	}

	httpx.WriteJSON(w, http.StatusOK, map[string]interface{}{"notifications": notifications})
}

func (h *Handler) markRead(w http.ResponseWriter, r *http.Request) {
	claims, _ := authmw.FromContext(r.Context())

	if err := h.Store.MarkRead(r.Context(), claims.Subject, chi.URLParam(r, "notificationID")); err != nil {
		h.Log.Error().Err(err).Msg("mark notification read failed")
		httpx.WriteError(w, http.StatusInternalServerError, "could not update notification")
		return
	}
	httpx.WriteJSON(w, http.StatusNoContent, nil)
}
