//go:build pact

// Provider verification: doctor-service replays every interaction that
// appointment-service recorded in its pact, against the real chi router and
// the real handler code.
//
// Two shortcuts are taken deliberately, and only two:
//   - the store is the in-memory implementation, so verification needs no
//     MongoDB container and each provider state can seed exact fixtures;
//   - the token verifier is a stub, because Keycloak's availability is not
//     what this contract is about (the pact still asserts that an
//     Authorization header is *sent*).
//
// Everything else is the production code path: routing, status codes, JSON
// field names, and the 409 on a contended slot.
package api_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/davidmm07/mediflow/common/authmw"
	"github.com/davidmm07/mediflow/services/doctor-service/internal/api"
	"github.com/davidmm07/mediflow/services/doctor-service/internal/domain"
	"github.com/davidmm07/mediflow/services/doctor-service/internal/store"
	"github.com/pact-foundation/pact-go/v2/models"
	"github.com/pact-foundation/pact-go/v2/provider"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// These ids mirror the ones baked into appointment-service's pact.
const (
	doctorID    = "3f1a9b7c-2d54-4a1e-9f3b-8c0d5e6a7b21"
	missingID   = "00000000-0000-4000-8000-000000000000"
	freeSlotID  = "b2c4d6e8-1357-4900-a1b2-c3d4e5f60718"
	takenSlotID = "d4e6f801-2468-4a13-b5c6-d7e8f9001122"
)

// pactClaims is the identity every replayed interaction runs as: a patient
// booking on their own behalf, which is what appointment-service forwards.
var pactClaims = authmw.Claims{
	Subject:           "11111111-2222-4333-8444-555555555555",
	PreferredUsername: "ana.paciente",
	Email:             "ana@mediflow.dev",
	RealmRoles:        []string{"patient"},
}

// stubVerifier injects a fixed identity instead of validating a real token.
type stubVerifier struct{}

func (stubVerifier) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			http.Error(w, "missing bearer token", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r.WithContext(authmw.WithClaims(r.Context(), pactClaims)))
	})
}

func seedDoctor(memory *store.Memory) {
	memory.SeedDoctor(domain.Doctor{
		ID:              doctorID,
		KeycloakUserID:  "doctor-keycloak-subject",
		FullName:        "Gregory House",
		Specialty:       "cardiology",
		LicenseNumber:   "ES-CARD-99120",
		Bio:             "Diagnostic medicine, 20 years of practice.",
		ConsultationFee: 75.0,
		Languages:       []string{"es", "en"},
		Active:          true,
		CreatedAt:       time.Date(2026, 1, 10, 8, 0, 0, 0, time.UTC),
		UpdatedAt:       time.Date(2026, 1, 10, 8, 0, 0, 0, time.UTC),
	})
}

func seedFreeSlot(memory *store.Memory) {
	memory.SeedSlot(domain.Slot{
		ID:       freeSlotID,
		DoctorID: doctorID,
		StartsAt: time.Date(2026, 9, 14, 9, 0, 0, 0, time.UTC),
		EndsAt:   time.Date(2026, 9, 14, 9, 30, 0, 0, time.UTC),
		Reserved: false,
	})
}

func seedTakenSlot(memory *store.Memory) {
	memory.SeedSlot(domain.Slot{
		ID:            takenSlotID,
		DoctorID:      doctorID,
		StartsAt:      time.Date(2026, 9, 15, 11, 0, 0, 0, time.UTC),
		EndsAt:        time.Date(2026, 9, 15, 11, 30, 0, 0, time.UTC),
		Reserved:      true,
		AppointmentID: "an-earlier-appointment",
	})
}

func TestPactProviderVerification(t *testing.T) {
	memory := store.NewMemory()

	handler := &api.Handler{
		Store:    memory,
		Verifier: stubVerifier{},
		Log:      zerolog.Nop(),
	}

	server := httptest.NewServer(handler.Routes())
	defer server.Close()

	pactDir, err := filepath.Abs(filepath.Join("..", "..", "..", "..", "pacts"))
	require.NoError(t, err)

	request := provider.VerifyRequest{
		Provider:                   "doctor-service",
		ProviderBaseURL:            server.URL,
		ProviderVersion:            providerVersion(),
		ProviderBranch:             os.Getenv("GIT_BRANCH"),
		FailIfNoPactsFound:         true,
		DisableColoredOutput:       true,
		PublishVerificationResults: os.Getenv("PACT_PUBLISH_RESULTS") == "true",

		// Each state resets the store and seeds exactly what the interaction
		// needs, so interactions can't leak fixtures into one another.
		StateHandlers: models.StateHandlers{
			fmt.Sprintf("a doctor with ID %s exists", doctorID): func(setup bool, _ models.ProviderState) (models.ProviderStateResponse, error) {
				memory.Reset()
				if setup {
					seedDoctor(memory)
				}
				return nil, nil
			},
			fmt.Sprintf("no doctor with ID %s exists", missingID): func(setup bool, _ models.ProviderState) (models.ProviderStateResponse, error) {
				memory.Reset()
				return nil, nil
			},
			fmt.Sprintf("doctor %s has at least one free slot in September 2026", doctorID): func(setup bool, _ models.ProviderState) (models.ProviderStateResponse, error) {
				memory.Reset()
				if setup {
					seedDoctor(memory)
					seedFreeSlot(memory)
				}
				return nil, nil
			},
			fmt.Sprintf("slot %s of doctor %s is free", freeSlotID, doctorID): func(setup bool, _ models.ProviderState) (models.ProviderStateResponse, error) {
				memory.Reset()
				if setup {
					seedDoctor(memory)
					seedFreeSlot(memory)
				}
				return nil, nil
			},
			fmt.Sprintf("slot %s of doctor %s is already reserved", takenSlotID, doctorID): func(setup bool, _ models.ProviderState) (models.ProviderStateResponse, error) {
				memory.Reset()
				if setup {
					seedDoctor(memory)
					seedTakenSlot(memory)
				}
				return nil, nil
			},
		},
	}

	// Prefer the broker when one is configured (that is what CI does); fall
	// back to the pact files on disk for a purely local run.
	if brokerURL := os.Getenv("PACT_BROKER_BASE_URL"); brokerURL != "" {
		request.BrokerURL = brokerURL
		request.BrokerUsername = os.Getenv("PACT_BROKER_USERNAME")
		request.BrokerPassword = os.Getenv("PACT_BROKER_PASSWORD")
		request.BrokerToken = os.Getenv("PACT_BROKER_TOKEN")
		request.ConsumerVersionSelectors = []provider.Selector{
			&provider.ConsumerVersionSelector{MainBranch: true},
			&provider.ConsumerVersionSelector{Deployed: true},
		}
	} else {
		request.PactFiles = []string{
			filepath.ToSlash(filepath.Join(pactDir, "appointment-service-doctor-service.json")),
		}
	}

	require.NoError(t, provider.NewVerifier().VerifyProvider(t, request))
}

func providerVersion() string {
	if sha := os.Getenv("GIT_COMMIT"); sha != "" {
		return sha
	}
	return "dev"
}
