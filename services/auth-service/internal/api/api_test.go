package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/davidmm07/mediflow/services/auth-service/internal/api"
	"github.com/davidmm07/mediflow/services/auth-service/internal/keycloak"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

type stubRegistrar struct {
	returnID  string
	returnErr error
	got       keycloak.NewUser
}

func (s *stubRegistrar) CreateUser(_ context.Context, u keycloak.NewUser) (string, error) {
	s.got = u
	return s.returnID, s.returnErr
}

type stubPublisher struct {
	events    []string
	returnErr error
}

func (s *stubPublisher) Publish(_ context.Context, event string, _ int, _ string, _ interface{}) error {
	s.events = append(s.events, event)
	return s.returnErr
}

type allowAllVerifier struct{}

func (allowAllVerifier) Middleware(next http.Handler) http.Handler { return next }

func newHandler(reg api.UserRegistrar, pub api.EventPublisher) *api.Handler {
	return &api.Handler{
		Registrar: reg,
		Publisher: pub,
		Verifier:  allowAllVerifier{},
		Log:       zerolog.Nop(),
	}
}

func post(t *testing.T, h http.Handler, path string, body interface{}) *httptest.ResponseRecorder {
	t.Helper()
	payload, err := json.Marshal(body)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestRegisterCreatesUserAndPublishesEvent(t *testing.T) {
	reg := &stubRegistrar{returnID: "kc-user-123"}
	pub := &stubPublisher{}
	h := newHandler(reg, pub)

	rec := post(t, h.Routes(), "/auth/register", api.RegisterRequest{
		Username:  "ana.paciente",
		Email:     "ana@mediflow.dev",
		FirstName: "Ana",
		LastName:  "Ramirez",
		Password:  "supersecret",
		Role:      "patient",
	})

	require.Equal(t, http.StatusCreated, rec.Code)

	var resp api.RegisterResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Equal(t, "kc-user-123", resp.UserID)
	require.Equal(t, "ana.paciente", resp.Username)
	require.Equal(t, "patient", reg.got.Role)
	require.Equal(t, []string{"auth.user.registered"}, pub.events)
}

func TestRegisterRejectsInvalidPayloads(t *testing.T) {
	cases := map[string]api.RegisterRequest{
		"missing username": {Email: "a@b.dev", Password: "supersecret", Role: "patient"},
		"bad email":        {Username: "u", Email: "not-an-email", Password: "supersecret", Role: "patient"},
		"short password":   {Username: "u", Email: "a@b.dev", Password: "short", Role: "patient"},
		"unknown role":     {Username: "u", Email: "a@b.dev", Password: "supersecret", Role: "wizard"},
	}

	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			h := newHandler(&stubRegistrar{returnID: "x"}, &stubPublisher{})
			rec := post(t, h.Routes(), "/auth/register", body)
			require.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

func TestRegisterMapsDuplicateUserToConflict(t *testing.T) {
	h := newHandler(&stubRegistrar{returnErr: keycloak.ErrUserExists}, &stubPublisher{})

	rec := post(t, h.Routes(), "/auth/register", api.RegisterRequest{
		Username: "ana.paciente", Email: "ana@mediflow.dev", Password: "supersecret", Role: "patient",
	})

	require.Equal(t, http.StatusConflict, rec.Code)
}

// A broker outage must not roll back an identity that Keycloak already
// created; the registration stays successful and provisioning catches up.
func TestRegisterSucceedsWhenEventPublishFails(t *testing.T) {
	pub := &stubPublisher{returnErr: errors.New("broker down")}
	h := newHandler(&stubRegistrar{returnID: "kc-user-9"}, pub)

	rec := post(t, h.Routes(), "/auth/register", api.RegisterRequest{
		Username: "dr.strange", Email: "strange@mediflow.dev", Password: "supersecret", Role: "doctor",
	})

	require.Equal(t, http.StatusCreated, rec.Code)
}
