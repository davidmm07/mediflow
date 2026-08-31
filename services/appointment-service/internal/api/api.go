// Package api exposes appointment-service's HTTP endpoints: booking,
// cancelling, and reading one's own agenda.
package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/davidmm07/mediflow/common/authmw"
	"github.com/davidmm07/mediflow/common/httpx"
	"github.com/davidmm07/mediflow/services/appointment-service/internal/booking"
	"github.com/davidmm07/mediflow/services/appointment-service/internal/domain"
	"github.com/davidmm07/mediflow/services/appointment-service/internal/store"
	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"
)

// TokenVerifier guards authenticated routes.
type TokenVerifier interface {
	Middleware(next http.Handler) http.Handler
}

// Handler holds appointment-service's dependencies.
type Handler struct {
	Booking  *booking.Service
	Store    store.Store
	Verifier TokenVerifier
	Log      zerolog.Logger
}

// BookRequest is the booking payload.
type BookRequest struct {
	DoctorID string `json:"doctor_id"`
	SlotID   string `json:"slot_id"`
	Reason   string `json:"reason"`
}

// CancelRequest carries the optional cancellation reason.
type CancelRequest struct {
	Reason string `json:"reason"`
}

// Routes builds the router.
func (h *Handler) Routes() http.Handler {
	r := chi.NewRouter()
	r.Use(httpx.RequestIDMiddleware)
	r.Get("/health", httpx.HealthHandler)

	r.Group(func(private chi.Router) {
		private.Use(h.Verifier.Middleware)

		private.With(authmw.RequireRole("patient")).
			Post("/appointments", h.book)
		private.Get("/appointments/me", h.listMine)
		private.Get("/appointments/{appointmentID}", h.get)
		private.Post("/appointments/{appointmentID}/cancel", h.cancel)

		private.With(authmw.RequireRole("doctor", "admin")).
			Get("/appointments/doctor/{doctorID}", h.listByDoctor)
	})

	return r
}

func (h *Handler) book(w http.ResponseWriter, r *http.Request) {
	claims, _ := authmw.FromContext(r.Context())

	var req BookRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, "malformed JSON body")
		return
	}
	if req.DoctorID == "" || req.SlotID == "" {
		httpx.WriteError(w, http.StatusBadRequest, "doctor_id and slot_id are required")
		return
	}

	appointment, err := h.Booking.Book(r.Context(), booking.BookRequest{
		BearerToken:   bearerToken(r),
		PatientUserID: claims.Subject,
		PatientName:   displayName(claims),
		DoctorID:      req.DoctorID,
		SlotID:        req.SlotID,
		Reason:        req.Reason,
	})

	switch {
	case errors.Is(err, domain.ErrDoctorNotFound):
		httpx.WriteError(w, http.StatusNotFound, "doctor not found")
	case errors.Is(err, domain.ErrSlotUnavailable):
		httpx.WriteError(w, http.StatusConflict, "that time slot is no longer available")
	case err != nil:
		h.Log.Error().Err(err).Msg("booking failed")
		httpx.WriteError(w, http.StatusBadGateway, "could not complete the booking")
	default:
		httpx.WriteJSON(w, http.StatusCreated, appointment)
	}
}

func (h *Handler) cancel(w http.ResponseWriter, r *http.Request) {
	claims, _ := authmw.FromContext(r.Context())

	var req CancelRequest
	if r.ContentLength > 0 {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			httpx.WriteError(w, http.StatusBadRequest, "malformed JSON body")
			return
		}
	}

	appointment, err := h.Booking.Cancel(r.Context(), booking.CancelRequest{
		BearerToken:   bearerToken(r),
		AppointmentID: chi.URLParam(r, "appointmentID"),
		ActorUserID:   claims.Subject,
		ActorIsStaff:  claims.HasRole("doctor") || claims.HasRole("admin"),
		Reason:        req.Reason,
	})

	switch {
	case errors.Is(err, domain.ErrNotFound):
		httpx.WriteError(w, http.StatusNotFound, "appointment not found")
	case errors.Is(err, domain.ErrForbidden):
		httpx.WriteError(w, http.StatusForbidden, "this appointment belongs to another patient")
	case errors.Is(err, domain.ErrNotCancellable):
		httpx.WriteError(w, http.StatusConflict, "only upcoming confirmed appointments can be cancelled")
	case err != nil:
		h.Log.Error().Err(err).Msg("cancellation failed")
		httpx.WriteError(w, http.StatusInternalServerError, "could not cancel the appointment")
	default:
		httpx.WriteJSON(w, http.StatusOK, appointment)
	}
}

func (h *Handler) listMine(w http.ResponseWriter, r *http.Request) {
	claims, _ := authmw.FromContext(r.Context())

	appointments, err := h.Store.ListByPatient(r.Context(), claims.Subject)
	if err != nil {
		h.Log.Error().Err(err).Msg("list appointments failed")
		httpx.WriteError(w, http.StatusInternalServerError, "could not list appointments")
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]interface{}{"appointments": appointments})
}

func (h *Handler) listByDoctor(w http.ResponseWriter, r *http.Request) {
	appointments, err := h.Store.ListByDoctor(r.Context(), chi.URLParam(r, "doctorID"))
	if err != nil {
		h.Log.Error().Err(err).Msg("list doctor agenda failed")
		httpx.WriteError(w, http.StatusInternalServerError, "could not list appointments")
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]interface{}{"appointments": appointments})
}

func (h *Handler) get(w http.ResponseWriter, r *http.Request) {
	claims, _ := authmw.FromContext(r.Context())

	appointment, err := h.Store.Get(r.Context(), chi.URLParam(r, "appointmentID"))
	if errors.Is(err, domain.ErrNotFound) {
		httpx.WriteError(w, http.StatusNotFound, "appointment not found")
		return
	}
	if err != nil {
		h.Log.Error().Err(err).Msg("get appointment failed")
		httpx.WriteError(w, http.StatusInternalServerError, "could not fetch appointment")
		return
	}

	isStaff := claims.HasRole("doctor") || claims.HasRole("admin")
	if !isStaff && appointment.PatientUserID != claims.Subject {
		// 404 rather than 403: confirming an id exists would leak that
		// another patient has an appointment.
		httpx.WriteError(w, http.StatusNotFound, "appointment not found")
		return
	}

	httpx.WriteJSON(w, http.StatusOK, appointment)
}

func bearerToken(r *http.Request) string {
	return strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
}

func displayName(claims authmw.Claims) string {
	if claims.PreferredUsername != "" {
		return claims.PreferredUsername
	}
	return claims.Email
}
