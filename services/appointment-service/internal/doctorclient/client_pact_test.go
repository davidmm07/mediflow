//go:build pact

// Consumer-driven contract test: appointment-service (consumer) against
// doctor-service (provider).
//
// The point of this file is that the expectations below are exercised by the
// *real* doctorclient code — the Pact mock provider answers, the client
// parses, and the assertions run on the parsed result. A pact generated this
// way cannot drift from the client: if someone changes a URL, a header or a
// field name in client.go, this test fails before the contract is published.
//
// Build-tagged `pact` because it needs the Pact FFI native library
// (`make pact-install`); plain `go test ./...` stays fast and dependency-free.
package doctorclient_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/davidmm07/mediflow/services/appointment-service/internal/doctorclient"
	"github.com/pact-foundation/pact-go/v2/consumer"
	"github.com/pact-foundation/pact-go/v2/matchers"
	"github.com/stretchr/testify/require"
)

// Fixed identifiers shared between the expectations and the provider states
// doctor-service implements. Keeping them in constants makes the coupling
// between the two sides explicit and greppable.
const (
	doctorID      = "3f1a9b7c-2d54-4a1e-9f3b-8c0d5e6a7b21"
	freeSlotID    = "b2c4d6e8-1357-4900-a1b2-c3d4e5f60718"
	takenSlotID   = "d4e6f801-2468-4a13-b5c6-d7e8f9001122"
	appointmentID = "9a8b7c6d-5e4f-4a3b-8c9d-0e1f2a3b4c5d"
	bearerToken   = "an-access-token-issued-by-keycloak"
)

func newPact(t *testing.T) *consumer.V2HTTPMockProvider {
	t.Helper()

	pactDir, err := filepath.Abs(filepath.Join("..", "..", "..", "..", "pacts"))
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(pactDir, 0o755))

	mockProvider, err := consumer.NewV2Pact(consumer.MockHTTPProviderConfig{
		Consumer: "appointment-service",
		Provider: "doctor-service",
		PactDir:  pactDir,
	})
	require.NoError(t, err)
	return mockProvider
}

func clientFor(config consumer.MockServerConfig) *doctorclient.Client {
	return doctorclient.New(fmt.Sprintf("http://%s:%d", config.Host, config.Port))
}

func TestPactGetDoctor(t *testing.T) {
	mockProvider := newPact(t)

	err := mockProvider.
		AddInteraction().
		Given("a doctor with ID " + doctorID + " exists").
		UponReceiving("a request for that doctor's profile").
		WithRequest("GET", "/doctors/"+doctorID, func(b *consumer.V2RequestBuilder) {
			b.Header("Authorization", matchers.Regex("Bearer "+bearerToken, "^Bearer .+$"))
			b.Header("Accept", matchers.S("application/json"))
		}).
		WillRespondWith(200, func(b *consumer.V2ResponseBuilder) {
			b.Header("Content-Type", matchers.Regex("application/json", "application/json.*"))
			b.JSONBody(matchers.Map{
				"id":               matchers.S(doctorID),
				"full_name":        matchers.Like("Gregory House"),
				"specialty":        matchers.Like("cardiology"),
				"consultation_fee": matchers.Like(75.0),
				"active":           matchers.Like(true),
			})
		}).
		ExecuteTest(t, func(config consumer.MockServerConfig) error {
			doctor, err := clientFor(config).GetDoctor(context.Background(), bearerToken, doctorID)
			require.NoError(t, err)
			require.Equal(t, doctorID, doctor.ID)
			require.NotEmpty(t, doctor.FullName)
			require.True(t, doctor.Active)
			return nil
		})

	require.NoError(t, err)
}

func TestPactGetDoctorNotFound(t *testing.T) {
	mockProvider := newPact(t)
	missingID := "00000000-0000-4000-8000-000000000000"

	err := mockProvider.
		AddInteraction().
		Given("no doctor with ID " + missingID + " exists").
		UponReceiving("a request for a doctor that does not exist").
		WithRequest("GET", "/doctors/"+missingID, func(b *consumer.V2RequestBuilder) {
			b.Header("Authorization", matchers.Regex("Bearer "+bearerToken, "^Bearer .+$"))
			b.Header("Accept", matchers.S("application/json"))
		}).
		WillRespondWith(404, func(b *consumer.V2ResponseBuilder) {
			b.JSONBody(matchers.Map{"error": matchers.Like("doctor not found")})
		}).
		ExecuteTest(t, func(config consumer.MockServerConfig) error {
			_, err := clientFor(config).GetDoctor(context.Background(), bearerToken, missingID)
			require.ErrorIs(t, err, doctorclient.ErrDoctorNotFound)
			return nil
		})

	require.NoError(t, err)
}

func TestPactListAvailableSlots(t *testing.T) {
	mockProvider := newPact(t)

	from := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC)

	err := mockProvider.
		AddInteraction().
		Given("doctor " + doctorID + " has at least one free slot in September 2026").
		UponReceiving("a request for that doctor's available slots").
		WithRequest("GET", "/doctors/"+doctorID+"/slots", func(b *consumer.V2RequestBuilder) {
			b.Query("available", matchers.S("true"))
			b.Query("from", matchers.S(from.Format(time.RFC3339)))
			b.Query("to", matchers.S(to.Format(time.RFC3339)))
			b.Header("Authorization", matchers.Regex("Bearer "+bearerToken, "^Bearer .+$"))
		}).
		WillRespondWith(200, func(b *consumer.V2ResponseBuilder) {
			b.JSONBody(matchers.Map{
				"doctor_id": matchers.S(doctorID),
				// EachLike pins the element shape while letting the provider
				// return any number of slots — the consumer genuinely does
				// not care how many, only what each one looks like.
				"slots": matchers.EachLike(matchers.Map{
					"id":        matchers.Like(freeSlotID),
					"doctor_id": matchers.S(doctorID),
					"starts_at": matchers.Regex("2026-09-14T09:00:00Z", `^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(Z|[+-]\d{2}:\d{2})$`),
					"ends_at":   matchers.Regex("2026-09-14T09:30:00Z", `^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(Z|[+-]\d{2}:\d{2})$`),
					"reserved":  matchers.Like(false),
				}, 1),
			})
		}).
		ExecuteTest(t, func(config consumer.MockServerConfig) error {
			slots, err := clientFor(config).ListAvailableSlots(context.Background(), bearerToken, doctorID, from, to)
			require.NoError(t, err)
			require.NotEmpty(t, slots)
			require.Equal(t, doctorID, slots[0].DoctorID)
			require.False(t, slots[0].Reserved)
			require.True(t, slots[0].EndsAt.After(slots[0].StartsAt))
			return nil
		})

	require.NoError(t, err)
}

func TestPactReserveSlot(t *testing.T) {
	mockProvider := newPact(t)

	err := mockProvider.
		AddInteraction().
		Given("slot " + freeSlotID + " of doctor " + doctorID + " is free").
		UponReceiving("a request to reserve that slot").
		WithRequest("POST", fmt.Sprintf("/doctors/%s/slots/%s/reserve", doctorID, freeSlotID),
			func(b *consumer.V2RequestBuilder) {
				b.Header("Authorization", matchers.Regex("Bearer "+bearerToken, "^Bearer .+$"))
				b.Header("Content-Type", matchers.S("application/json"))
				b.JSONBody(matchers.Map{
					"appointment_id": matchers.Like(appointmentID),
				})
			}).
		WillRespondWith(200, func(b *consumer.V2ResponseBuilder) {
			b.JSONBody(matchers.Map{
				"id":             matchers.S(freeSlotID),
				"doctor_id":      matchers.S(doctorID),
				"starts_at":      matchers.Regex("2026-09-14T09:00:00Z", `^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(Z|[+-]\d{2}:\d{2})$`),
				"ends_at":        matchers.Regex("2026-09-14T09:30:00Z", `^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(Z|[+-]\d{2}:\d{2})$`),
				"reserved":       matchers.Like(true),
				"appointment_id": matchers.Like(appointmentID),
			})
		}).
		ExecuteTest(t, func(config consumer.MockServerConfig) error {
			slot, err := clientFor(config).ReserveSlot(context.Background(), bearerToken, doctorID, freeSlotID, appointmentID)
			require.NoError(t, err)
			require.Equal(t, freeSlotID, slot.ID)
			require.True(t, slot.Reserved)
			return nil
		})

	require.NoError(t, err)
}

// The 409 case is the one that makes this contract worth having: the booking
// flow only degrades gracefully into "pick another time" if doctor-service
// really does answer 409 when losing a race. Pinning it stops a provider-side
// refactor from turning a race into a 500.
func TestPactReserveSlotAlreadyTaken(t *testing.T) {
	mockProvider := newPact(t)

	err := mockProvider.
		AddInteraction().
		Given("slot " + takenSlotID + " of doctor " + doctorID + " is already reserved").
		UponReceiving("a request to reserve a slot that another patient already took").
		WithRequest("POST", fmt.Sprintf("/doctors/%s/slots/%s/reserve", doctorID, takenSlotID),
			func(b *consumer.V2RequestBuilder) {
				b.Header("Authorization", matchers.Regex("Bearer "+bearerToken, "^Bearer .+$"))
				b.JSONBody(matchers.Map{"appointment_id": matchers.Like(appointmentID)})
			}).
		WillRespondWith(409, func(b *consumer.V2ResponseBuilder) {
			b.JSONBody(matchers.Map{
				"error": matchers.Like("availability slot is already reserved"),
			})
		}).
		ExecuteTest(t, func(config consumer.MockServerConfig) error {
			_, err := clientFor(config).ReserveSlot(context.Background(), bearerToken, doctorID, takenSlotID, appointmentID)
			require.ErrorIs(t, err, doctorclient.ErrSlotTaken)
			return nil
		})

	require.NoError(t, err)
}

// Release is the compensating action of the booking saga; if it silently
// stopped working, slots would leak. The contract keeps it honest.
func TestPactReleaseSlot(t *testing.T) {
	mockProvider := newPact(t)

	err := mockProvider.
		AddInteraction().
		Given("slot " + takenSlotID + " of doctor " + doctorID + " is already reserved").
		UponReceiving("a request to release that slot after a cancellation").
		WithRequest("POST", fmt.Sprintf("/doctors/%s/slots/%s/release", doctorID, takenSlotID),
			func(b *consumer.V2RequestBuilder) {
				b.Header("Authorization", matchers.Regex("Bearer "+bearerToken, "^Bearer .+$"))
			}).
		WillRespondWith(204).
		ExecuteTest(t, func(config consumer.MockServerConfig) error {
			err := clientFor(config).ReleaseSlot(context.Background(), bearerToken, doctorID, takenSlotID)
			require.NoError(t, err)
			return nil
		})

	require.NoError(t, err)
}
