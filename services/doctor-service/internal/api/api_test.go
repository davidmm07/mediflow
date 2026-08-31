package api_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/davidmm07/mediflow/common/authmw"
	"github.com/davidmm07/mediflow/services/doctor-service/internal/api"
	"github.com/davidmm07/mediflow/services/doctor-service/internal/domain"
	"github.com/davidmm07/mediflow/services/doctor-service/internal/store"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// roleVerifier injects a caller with the given realm roles, standing in for
// a validated Keycloak token.
type roleVerifier struct {
	roles []string
}

func (v roleVerifier) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		claims := authmw.Claims{
			Subject:           "user-subject",
			PreferredUsername: "test.user",
			RealmRoles:        v.roles,
		}
		next.ServeHTTP(w, r.WithContext(authmw.WithClaims(r.Context(), claims)))
	})
}

func newHandler(roles ...string) (*api.Handler, *store.Memory) {
	memory := store.NewMemory()
	return &api.Handler{
		Store:    memory,
		Verifier: roleVerifier{roles: roles},
		Log:      zerolog.Nop(),
	}, memory
}

func do(t *testing.T, h http.Handler, method, path string, body interface{}) *httptest.ResponseRecorder {
	t.Helper()

	var reader *bytes.Reader
	if body != nil {
		payload, err := json.Marshal(body)
		require.NoError(t, err)
		reader = bytes.NewReader(payload)
	} else {
		reader = bytes.NewReader(nil)
	}

	req := httptest.NewRequest(method, path, reader)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func seededSlot(memory *store.Memory, reserved bool) domain.Slot {
	slot := domain.Slot{
		ID:       "slot-1",
		DoctorID: "doc-1",
		StartsAt: time.Date(2026, 9, 14, 9, 0, 0, 0, time.UTC),
		EndsAt:   time.Date(2026, 9, 14, 9, 30, 0, 0, time.UTC),
		Reserved: reserved,
	}
	memory.SeedDoctor(domain.Doctor{ID: "doc-1", FullName: "Gregory House", Specialty: "cardiology", Active: true})
	memory.SeedSlot(slot)
	return slot
}

func TestCreateDoctorRequiresDoctorRole(t *testing.T) {
	h, _ := newHandler("patient")

	rec := do(t, h.Routes(), http.MethodPost, "/doctors", api.CreateDoctorRequest{
		FullName:      "Gregory House",
		Specialty:     "cardiology",
		LicenseNumber: "ES-1",
	})

	require.Equal(t, http.StatusForbidden, rec.Code)
}

func TestCreateDoctorValidatesPayload(t *testing.T) {
	h, _ := newHandler("doctor")

	rec := do(t, h.Routes(), http.MethodPost, "/doctors", api.CreateDoctorRequest{
		Specialty: "cardiology",
	})

	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestReserveSlotSucceedsThenConflicts(t *testing.T) {
	h, memory := newHandler("patient")
	seededSlot(memory, false)

	first := do(t, h.Routes(), http.MethodPost, "/doctors/doc-1/slots/slot-1/reserve",
		api.ReserveSlotRequest{AppointmentID: "appt-1"})
	require.Equal(t, http.StatusOK, first.Code)

	var slot domain.Slot
	require.NoError(t, json.Unmarshal(first.Body.Bytes(), &slot))
	require.True(t, slot.Reserved)
	require.Equal(t, "appt-1", slot.AppointmentID)

	// The second patient to arrive must lose cleanly with a 409, which is
	// what appointment-service's booking flow branches on.
	second := do(t, h.Routes(), http.MethodPost, "/doctors/doc-1/slots/slot-1/reserve",
		api.ReserveSlotRequest{AppointmentID: "appt-2"})
	require.Equal(t, http.StatusConflict, second.Code)
}

func TestReserveSlotRequiresAppointmentID(t *testing.T) {
	h, memory := newHandler("patient")
	seededSlot(memory, false)

	rec := do(t, h.Routes(), http.MethodPost, "/doctors/doc-1/slots/slot-1/reserve",
		api.ReserveSlotRequest{})

	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestReserveUnknownSlotReturns404(t *testing.T) {
	h, _ := newHandler("patient")

	rec := do(t, h.Routes(), http.MethodPost, "/doctors/doc-1/slots/nope/reserve",
		api.ReserveSlotRequest{AppointmentID: "appt-1"})

	require.Equal(t, http.StatusNotFound, rec.Code)
}

func TestReleaseSlotReturnsItToThePool(t *testing.T) {
	h, memory := newHandler("patient")
	seededSlot(memory, true)

	rec := do(t, h.Routes(), http.MethodPost, "/doctors/doc-1/slots/slot-1/release", nil)
	require.Equal(t, http.StatusNoContent, rec.Code)

	reserveAgain := do(t, h.Routes(), http.MethodPost, "/doctors/doc-1/slots/slot-1/reserve",
		api.ReserveSlotRequest{AppointmentID: "appt-2"})
	require.Equal(t, http.StatusOK, reserveAgain.Code)
}

func TestListSlotsFiltersToAvailableOnly(t *testing.T) {
	h, memory := newHandler("patient")
	seededSlot(memory, false)
	memory.SeedSlot(domain.Slot{
		ID:       "slot-2",
		DoctorID: "doc-1",
		StartsAt: time.Date(2026, 9, 15, 9, 0, 0, 0, time.UTC),
		EndsAt:   time.Date(2026, 9, 15, 9, 30, 0, 0, time.UTC),
		Reserved: true,
	})

	rec := do(t, h.Routes(), http.MethodGet, "/doctors/doc-1/slots?available=true", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp api.SlotListResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Slots, 1)
	require.Equal(t, "slot-1", resp.Slots[0].ID)
}

func TestListSlotsForUnknownDoctorReturns404(t *testing.T) {
	h, _ := newHandler("patient")

	rec := do(t, h.Routes(), http.MethodGet, "/doctors/nope/slots", nil)

	require.Equal(t, http.StatusNotFound, rec.Code)
}

func TestListSlotsRejectsBadTimeFilter(t *testing.T) {
	h, memory := newHandler("patient")
	seededSlot(memory, false)

	rec := do(t, h.Routes(), http.MethodGet, "/doctors/doc-1/slots?from=yesterday", nil)

	require.Equal(t, http.StatusBadRequest, rec.Code)
}
