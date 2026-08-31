//go:build pact

// Message provider verification: auth-service proves it really emits the
// auth.user.registered event that patient-service and notification-service
// build on.
//
// As with the other provider verifications, the payload is not hand-written:
// the handler runs for real against a stub identity provider, and whatever
// it published to Kafka is what gets verified against the consumer's pact.
package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/davidmm07/mediflow/services/auth-service/internal/api"
	"github.com/davidmm07/mediflow/services/auth-service/internal/keycloak"
	pactmessage "github.com/pact-foundation/pact-go/v2/message"
	"github.com/pact-foundation/pact-go/v2/models"
	"github.com/pact-foundation/pact-go/v2/provider"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// capturingPublisher records the event a registration emitted.
type capturingPublisher struct {
	event string
	data  interface{}
}

func (c *capturingPublisher) Publish(_ context.Context, event string, _ int, _ string, data interface{}) error {
	c.event = event
	c.data = data
	return nil
}

// fixedRegistrar stands in for Keycloak, returning a deterministic user id.
type fixedRegistrar struct{ userID string }

func (f fixedRegistrar) CreateUser(_ context.Context, _ keycloak.NewUser) (string, error) {
	return f.userID, nil
}

// registerOnce drives the real HTTP handler and returns the event it emitted.
func registerOnce() (pactmessage.Body, error) {
	publisher := &capturingPublisher{}

	handler := &api.Handler{
		Registrar: fixedRegistrar{userID: "11111111-2222-4333-8444-555555555555"},
		Publisher: publisher,
		Verifier:  passthroughVerifier{},
		Log:       zerolog.Nop(),
	}

	payload, err := json.Marshal(api.RegisterRequest{
		Username:  "ana.paciente",
		Email:     "ana@mediflow.dev",
		FirstName: "Ana",
		LastName:  "Ramirez",
		Password:  "a-strong-password",
		Role:      "patient",
	})
	if err != nil {
		return nil, err
	}

	req := httptest.NewRequest(http.MethodPost, "/auth/register", bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		return nil, errUnexpectedStatus(rec.Code)
	}

	raw, err := json.Marshal(publisher.data)
	if err != nil {
		return nil, err
	}
	var body map[string]interface{}
	if err := json.Unmarshal(raw, &body); err != nil {
		return nil, err
	}
	return body, nil
}

type passthroughVerifier struct{}

func (passthroughVerifier) Middleware(next http.Handler) http.Handler { return next }

type statusError int

func (e statusError) Error() string {
	return "registration handler returned unexpected status " + http.StatusText(int(e))
}

func errUnexpectedStatus(code int) error { return statusError(code) }

func TestPactMessageProviderVerification(t *testing.T) {
	pactDir, err := filepath.Abs(filepath.Join("..", "..", "..", "..", "pacts"))
	require.NoError(t, err)

	request := provider.VerifyRequest{
		Provider:                   "auth-service",
		ProviderVersion:            providerVersion(),
		ProviderBranch:             os.Getenv("GIT_BRANCH"),
		FailIfNoPactsFound:         true,
		DisableColoredOutput:       true,
		PublishVerificationResults: os.Getenv("PACT_PUBLISH_RESULTS") == "true",

		MessageHandlers: pactmessage.Handlers{
			"an auth.user.registered event": func(_ []models.ProviderState) (pactmessage.Body, pactmessage.Metadata, error) {
				body, err := registerOnce()
				if err != nil {
					return nil, nil, err
				}
				return body, pactmessage.Metadata{
					"kafka_topic": "auth.user.registered",
					"contentType": "application/json",
				}, nil
			},
		},
	}

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
			filepath.ToSlash(filepath.Join(pactDir, "notification-service-auth-service.json")),
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
