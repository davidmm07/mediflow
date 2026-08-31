// Package httpx holds tiny HTTP helpers (JSON responses, request id
// middleware, health checks) shared across MediFlow's services so each one
// doesn't reinvent them slightly differently.
package httpx

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/google/uuid"
)

// WriteJSON writes v as JSON with the given status code.
func WriteJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if v != nil {
		_ = json.NewEncoder(w).Encode(v)
	}
}

// ErrorResponse is the uniform error body every MediFlow endpoint returns.
type ErrorResponse struct {
	Error string `json:"error"`
}

// WriteError writes a JSON error body with the given status code.
func WriteError(w http.ResponseWriter, status int, msg string) {
	WriteJSON(w, status, ErrorResponse{Error: msg})
}

type requestIDKey struct{}

// RequestID returns the correlation id attached by RequestIDMiddleware, or
// "" if the middleware hasn't run.
func RequestID(r *http.Request) string {
	id, _ := r.Context().Value(requestIDKey{}).(string)
	return id
}

// RequestIDMiddleware stamps every request with a correlation id (reusing an
// inbound X-Request-Id when the gateway already set one) so a single booking
// flow can be traced across services in the logs.
func RequestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-Id")
		if id == "" {
			id = uuid.NewString()
		}
		w.Header().Set("X-Request-Id", id)
		ctx := context.WithValue(r.Context(), requestIDKey{}, id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// HealthHandler responds 200 with a static body; used for container health
// checks and the gateway's upstream probing.
func HealthHandler(w http.ResponseWriter, r *http.Request) {
	WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}
