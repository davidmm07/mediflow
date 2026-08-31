// Package api is doctor-service's HTTP surface. Two audiences use it: humans
// browsing the practitioner directory through the gateway, and
// appointment-service calling the slot reservation endpoints synchronously,
// which is exactly the interaction covered by the Pact contract.
package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/davidmm07/mediflow/common/authmw"
	"github.com/davidmm07/mediflow/common/httpx"
	"github.com/davidmm07/mediflow/services/doctor-service/internal/domain"
	"github.com/davidmm07/mediflow/services/doctor-service/internal/store"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

// TokenVerifier guards authenticated routes; see auth-service for the same
// pattern. Provider verification injects a permissive stub.
type TokenVerifier interface {
	Middleware(next http.Handler) http.Handler
}

// Handler holds doctor-service's dependencies.
type Handler struct {
	Store    store.Store
	Verifier TokenVerifier
	Log      zerolog.Logger
}

// CreateDoctorRequest is the payload to publish a practitioner profile.
type CreateDoctorRequest struct {
	FullName        string   `json:"full_name"`
	Specialty       string   `json:"specialty"`
	LicenseNumber   string   `json:"license_number"`
	Bio             string   `json:"bio"`
	ConsultationFee float64  `json:"consultation_fee"`
	Languages       []string `json:"languages"`
}

// CreateSlotRequest is the payload to open an availability window.
type CreateSlotRequest struct {
	StartsAt time.Time `json:"starts_at"`
	EndsAt   time.Time `json:"ends_at"`
}

// ReserveSlotRequest carries the appointment claiming the slot.
type ReserveSlotRequest struct {
	AppointmentID string `json:"appointment_id"`
}

// SlotListResponse wraps the availability query result. It is an object
// rather than a bare array so the payload can grow (paging, totals) without
// breaking the consumer contract.
type SlotListResponse struct {
	DoctorID string        `json:"doctor_id"`
	Slots    []domain.Slot `json:"slots"`
}

// Routes builds the router. Everything except health requires a valid token;
// mutations additionally require the matching realm role.
func (h *Handler) Routes() http.Handler {
	r := chi.NewRouter()
	r.Use(httpx.RequestIDMiddleware)
	r.Get("/health", httpx.HealthHandler)

	r.Group(func(private chi.Router) {
		private.Use(h.Verifier.Middleware)

		private.Get("/doctors", h.listDoctors)
		private.Get("/doctors/{doctorID}", h.getDoctor)
		private.Get("/doctors/{doctorID}/slots", h.listSlots)

		private.With(authmw.RequireRole("doctor", "admin")).
			Post("/doctors", h.createDoctor)
		private.With(authmw.RequireRole("doctor", "admin")).
			Post("/doctors/{doctorID}/slots", h.createSlot)

		// Reservation endpoints are service-to-service: appointment-service
		// forwards the end user's token, so a patient booking their own
		// appointment is what authorises the reservation.
		private.Post("/doctors/{doctorID}/slots/{slotID}/reserve", h.reserveSlot)
		private.Post("/doctors/{doctorID}/slots/{slotID}/release", h.releaseSlot)
	})

	return r
}

func (h *Handler) listDoctors(w http.ResponseWriter, r *http.Request) {
	doctors, err := h.Store.ListDoctors(r.Context(), r.URL.Query().Get("specialty"))
	if err != nil {
		h.Log.Error().Err(err).Msg("list doctors failed")
		httpx.WriteError(w, http.StatusInternalServerError, "could not list doctors")
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]interface{}{"doctors": doctors})
}

func (h *Handler) getDoctor(w http.ResponseWriter, r *http.Request) {
	doctor, err := h.Store.GetDoctor(r.Context(), chi.URLParam(r, "doctorID"))
	if errors.Is(err, domain.ErrNotFound) {
		httpx.WriteError(w, http.StatusNotFound, "doctor not found")
		return
	}
	if err != nil {
		h.Log.Error().Err(err).Msg("get doctor failed")
		httpx.WriteError(w, http.StatusInternalServerError, "could not fetch doctor")
		return
	}
	httpx.WriteJSON(w, http.StatusOK, doctor)
}

func (h *Handler) createDoctor(w http.ResponseWriter, r *http.Request) {
	claims, _ := authmw.FromContext(r.Context())

	var req CreateDoctorRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, "malformed JSON body")
		return
	}

	now := time.Now().UTC()
	doctor := domain.Doctor{
		ID:              uuid.NewString(),
		KeycloakUserID:  claims.Subject,
		FullName:        req.FullName,
		Specialty:       req.Specialty,
		LicenseNumber:   req.LicenseNumber,
		Bio:             req.Bio,
		ConsultationFee: req.ConsultationFee,
		Languages:       req.Languages,
		Active:          true,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	if doctor.Languages == nil {
		doctor.Languages = []string{}
	}

	if err := doctor.Validate(); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	err := h.Store.CreateDoctor(r.Context(), doctor)
	if errors.Is(err, domain.ErrDuplicate) {
		httpx.WriteError(w, http.StatusConflict, "a profile already exists for this user")
		return
	}
	if err != nil {
		h.Log.Error().Err(err).Msg("create doctor failed")
		httpx.WriteError(w, http.StatusInternalServerError, "could not create doctor")
		return
	}

	httpx.WriteJSON(w, http.StatusCreated, doctor)
}

func (h *Handler) listSlots(w http.ResponseWriter, r *http.Request) {
	doctorID := chi.URLParam(r, "doctorID")

	if _, err := h.Store.GetDoctor(r.Context(), doctorID); errors.Is(err, domain.ErrNotFound) {
		httpx.WriteError(w, http.StatusNotFound, "doctor not found")
		return
	}

	from, err := parseTimeQuery(r, "from")
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, "from must be an RFC3339 timestamp")
		return
	}
	to, err := parseTimeQuery(r, "to")
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, "to must be an RFC3339 timestamp")
		return
	}

	onlyFree := r.URL.Query().Get("available") == "true"

	slots, err := h.Store.ListSlots(r.Context(), doctorID, from, to, onlyFree)
	if err != nil {
		h.Log.Error().Err(err).Msg("list slots failed")
		httpx.WriteError(w, http.StatusInternalServerError, "could not list slots")
		return
	}

	httpx.WriteJSON(w, http.StatusOK, SlotListResponse{DoctorID: doctorID, Slots: slots})
}

func (h *Handler) createSlot(w http.ResponseWriter, r *http.Request) {
	doctorID := chi.URLParam(r, "doctorID")

	var req CreateSlotRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, "malformed JSON body")
		return
	}

	slot := domain.Slot{
		ID:       uuid.NewString(),
		DoctorID: doctorID,
		StartsAt: req.StartsAt.UTC(),
		EndsAt:   req.EndsAt.UTC(),
	}
	if err := slot.Validate(); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	if err := h.Store.CreateSlot(r.Context(), slot); err != nil {
		httpx.WriteError(w, http.StatusConflict, err.Error())
		return
	}

	httpx.WriteJSON(w, http.StatusCreated, slot)
}

func (h *Handler) reserveSlot(w http.ResponseWriter, r *http.Request) {
	var req ReserveSlotRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, "malformed JSON body")
		return
	}
	if req.AppointmentID == "" {
		httpx.WriteError(w, http.StatusBadRequest, "appointment_id is required")
		return
	}

	slot, err := h.Store.ReserveSlot(r.Context(), chi.URLParam(r, "slotID"), req.AppointmentID)
	switch {
	case errors.Is(err, domain.ErrSlotNotFound):
		httpx.WriteError(w, http.StatusNotFound, "availability slot not found")
	case errors.Is(err, domain.ErrSlotTaken):
		httpx.WriteError(w, http.StatusConflict, "availability slot is already reserved")
	case err != nil:
		h.Log.Error().Err(err).Msg("reserve slot failed")
		httpx.WriteError(w, http.StatusInternalServerError, "could not reserve slot")
	default:
		httpx.WriteJSON(w, http.StatusOK, slot)
	}
}

func (h *Handler) releaseSlot(w http.ResponseWriter, r *http.Request) {
	err := h.Store.ReleaseSlot(r.Context(), chi.URLParam(r, "slotID"))
	if errors.Is(err, domain.ErrSlotNotFound) {
		httpx.WriteError(w, http.StatusNotFound, "availability slot not found")
		return
	}
	if err != nil {
		h.Log.Error().Err(err).Msg("release slot failed")
		httpx.WriteError(w, http.StatusInternalServerError, "could not release slot")
		return
	}
	httpx.WriteJSON(w, http.StatusNoContent, nil)
}

func parseTimeQuery(r *http.Request, key string) (time.Time, error) {
	raw := r.URL.Query().Get(key)
	if raw == "" {
		return time.Time{}, nil
	}
	return time.Parse(time.RFC3339, raw)
}
