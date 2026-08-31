// Package api exposes patient-service's HTTP endpoints. Patients only ever
// address their own record ("/patients/me"). The identity comes from the
// token, never from a path parameter, so one patient can't read another's
// profile by guessing an id.
package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/davidmm07/mediflow/common/authmw"
	"github.com/davidmm07/mediflow/common/httpx"
	"github.com/davidmm07/mediflow/services/patient-service/internal/domain"
	"github.com/davidmm07/mediflow/services/patient-service/internal/store"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

// TokenVerifier guards authenticated routes.
type TokenVerifier interface {
	Middleware(next http.Handler) http.Handler
}

// Handler holds patient-service's dependencies.
type Handler struct {
	Store    store.Store
	Verifier TokenVerifier
	Log      zerolog.Logger
}

// UpdateProfileRequest is the self-service profile update payload.
type UpdateProfileRequest struct {
	FullName  string   `json:"full_name"`
	Phone     string   `json:"phone"`
	BirthDate string   `json:"birth_date"`
	BloodType string   `json:"blood_type"`
	Allergies []string `json:"allergies"`
}

// Routes builds the router.
func (h *Handler) Routes() http.Handler {
	r := chi.NewRouter()
	r.Use(httpx.RequestIDMiddleware)
	r.Get("/health", httpx.HealthHandler)

	r.Group(func(private chi.Router) {
		private.Use(h.Verifier.Middleware)

		private.Get("/patients/me", h.getMe)
		private.Put("/patients/me", h.updateMe)

		// Clinicians need to read a patient's record; patients do not get
		// this route at all.
		private.With(authmw.RequireRole("doctor", "admin")).
			Get("/patients/{userID}", h.getByUserID)
	})

	return r
}

func (h *Handler) getMe(w http.ResponseWriter, r *http.Request) {
	claims, _ := authmw.FromContext(r.Context())

	patient, err := h.Store.GetByUserID(r.Context(), claims.Subject)
	if errors.Is(err, domain.ErrNotFound) {
		httpx.WriteError(w, http.StatusNotFound, "no patient profile for this account")
		return
	}
	if err != nil {
		h.Log.Error().Err(err).Msg("get patient failed")
		httpx.WriteError(w, http.StatusInternalServerError, "could not fetch profile")
		return
	}
	httpx.WriteJSON(w, http.StatusOK, patient)
}

func (h *Handler) getByUserID(w http.ResponseWriter, r *http.Request) {
	patient, err := h.Store.GetByUserID(r.Context(), chi.URLParam(r, "userID"))
	if errors.Is(err, domain.ErrNotFound) {
		httpx.WriteError(w, http.StatusNotFound, "patient not found")
		return
	}
	if err != nil {
		h.Log.Error().Err(err).Msg("get patient failed")
		httpx.WriteError(w, http.StatusInternalServerError, "could not fetch patient")
		return
	}
	httpx.WriteJSON(w, http.StatusOK, patient)
}

// updateMe upserts the caller's own profile. It creates the record when the
// identity event hasn't landed yet, so a patient who registers and
// immediately opens the app isn't blocked on Kafka consumer lag.
func (h *Handler) updateMe(w http.ResponseWriter, r *http.Request) {
	claims, _ := authmw.FromContext(r.Context())

	var req UpdateProfileRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, "malformed JSON body")
		return
	}

	now := time.Now().UTC()
	existing, err := h.Store.GetByUserID(r.Context(), claims.Subject)

	switch {
	case errors.Is(err, domain.ErrNotFound):
		patient := domain.Patient{
			ID:             uuid.NewString(),
			KeycloakUserID: claims.Subject,
			Email:          claims.Email,
			CreatedAt:      now,
		}
		applyUpdate(&patient, req, now)

		if vErr := patient.Validate(); vErr != nil {
			httpx.WriteError(w, http.StatusBadRequest, vErr.Error())
			return
		}
		if cErr := h.Store.Create(r.Context(), patient); cErr != nil {
			h.Log.Error().Err(cErr).Msg("create patient failed")
			httpx.WriteError(w, http.StatusInternalServerError, "could not save profile")
			return
		}
		httpx.WriteJSON(w, http.StatusCreated, patient)

	case err != nil:
		h.Log.Error().Err(err).Msg("lookup patient failed")
		httpx.WriteError(w, http.StatusInternalServerError, "could not save profile")

	default:
		applyUpdate(&existing, req, now)

		if vErr := existing.Validate(); vErr != nil {
			httpx.WriteError(w, http.StatusBadRequest, vErr.Error())
			return
		}
		if uErr := h.Store.Update(r.Context(), existing); uErr != nil {
			h.Log.Error().Err(uErr).Msg("update patient failed")
			httpx.WriteError(w, http.StatusInternalServerError, "could not save profile")
			return
		}
		httpx.WriteJSON(w, http.StatusOK, existing)
	}
}

func applyUpdate(p *domain.Patient, req UpdateProfileRequest, now time.Time) {
	if req.FullName != "" {
		p.FullName = req.FullName
	}
	p.Phone = req.Phone
	p.BirthDate = req.BirthDate
	p.BloodType = req.BloodType

	p.Allergies = req.Allergies
	if p.Allergies == nil {
		p.Allergies = []string{}
	}

	p.Onboarded = true
	p.UpdatedAt = now
}
