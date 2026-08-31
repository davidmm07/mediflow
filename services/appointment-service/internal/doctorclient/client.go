// Package doctorclient is appointment-service's view of doctor-service. This
// client is the Pact *consumer*: the contract test in client_pact_test.go
// runs this exact code against a Pact mock provider, so the pact file
// published to the broker describes real requests this client makes, not a
// hand-written approximation.
package doctorclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// Errors mapped from doctor-service's status codes so callers can branch on
// meaning rather than on HTTP numbers.
var (
	ErrDoctorNotFound = errors.New("doctorclient: doctor not found")
	ErrSlotNotFound   = errors.New("doctorclient: slot not found")
	ErrSlotTaken      = errors.New("doctorclient: slot already reserved")
	ErrUnauthorized   = errors.New("doctorclient: rejected by doctor-service")
)

// Doctor mirrors the fields appointment-service needs from a profile.
type Doctor struct {
	ID              string  `json:"id"`
	FullName        string  `json:"full_name"`
	Specialty       string  `json:"specialty"`
	ConsultationFee float64 `json:"consultation_fee"`
	Active          bool    `json:"active"`
}

// Slot mirrors an availability window.
type Slot struct {
	ID            string    `json:"id"`
	DoctorID      string    `json:"doctor_id"`
	StartsAt      time.Time `json:"starts_at"`
	EndsAt        time.Time `json:"ends_at"`
	Reserved      bool      `json:"reserved"`
	AppointmentID string    `json:"appointment_id,omitempty"`
}

// slotListResponse is the envelope doctor-service returns for availability.
type slotListResponse struct {
	DoctorID string `json:"doctor_id"`
	Slots    []Slot `json:"slots"`
}

// Client calls doctor-service over HTTP.
type Client struct {
	baseURL string
	http    *http.Client
}

// New builds a Client pointed at doctor-service's base URL.
func New(baseURL string) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		http:    &http.Client{Timeout: 5 * time.Second},
	}
}

// GetDoctor fetches a practitioner profile. The caller's bearer token is
// forwarded so doctor-service applies the same authorization it would for a
// direct call. MediFlow never uses an all-powerful internal service token for
// user-initiated actions.
func (c *Client) GetDoctor(ctx context.Context, bearerToken, doctorID string) (Doctor, error) {
	req, err := c.newRequest(ctx, http.MethodGet,
		fmt.Sprintf("/doctors/%s", doctorID), bearerToken, nil)
	if err != nil {
		return Doctor{}, err
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return Doctor{}, fmt.Errorf("doctorclient: get doctor: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		var d Doctor
		if err := json.NewDecoder(resp.Body).Decode(&d); err != nil {
			return Doctor{}, fmt.Errorf("doctorclient: decode doctor: %w", err)
		}
		return d, nil
	case http.StatusNotFound:
		return Doctor{}, ErrDoctorNotFound
	case http.StatusUnauthorized, http.StatusForbidden:
		return Doctor{}, ErrUnauthorized
	default:
		return Doctor{}, fmt.Errorf("doctorclient: get doctor returned %d", resp.StatusCode)
	}
}

// ListAvailableSlots returns the doctor's unreserved slots starting within
// [from, to].
func (c *Client) ListAvailableSlots(ctx context.Context, bearerToken, doctorID string, from, to time.Time) ([]Slot, error) {
	path := fmt.Sprintf("/doctors/%s/slots?available=true&from=%s&to=%s",
		doctorID,
		from.UTC().Format(time.RFC3339),
		to.UTC().Format(time.RFC3339),
	)

	req, err := c.newRequest(ctx, http.MethodGet, path, bearerToken, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("doctorclient: list slots: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		var payload slotListResponse
		if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
			return nil, fmt.Errorf("doctorclient: decode slots: %w", err)
		}
		return payload.Slots, nil
	case http.StatusNotFound:
		return nil, ErrDoctorNotFound
	case http.StatusUnauthorized, http.StatusForbidden:
		return nil, ErrUnauthorized
	default:
		return nil, fmt.Errorf("doctorclient: list slots returned %d", resp.StatusCode)
	}
}

// ReserveSlot claims a slot for an appointment. A 409 means another patient
// won the race, which the booking flow surfaces to the user as "pick another
// time" rather than as an error.
func (c *Client) ReserveSlot(ctx context.Context, bearerToken, doctorID, slotID, appointmentID string) (Slot, error) {
	body, err := json.Marshal(map[string]string{"appointment_id": appointmentID})
	if err != nil {
		return Slot{}, err
	}

	req, err := c.newRequest(ctx, http.MethodPost,
		fmt.Sprintf("/doctors/%s/slots/%s/reserve", doctorID, slotID), bearerToken, body)
	if err != nil {
		return Slot{}, err
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return Slot{}, fmt.Errorf("doctorclient: reserve slot: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		var s Slot
		if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
			return Slot{}, fmt.Errorf("doctorclient: decode slot: %w", err)
		}
		return s, nil
	case http.StatusConflict:
		return Slot{}, ErrSlotTaken
	case http.StatusNotFound:
		return Slot{}, ErrSlotNotFound
	case http.StatusUnauthorized, http.StatusForbidden:
		return Slot{}, ErrUnauthorized
	default:
		return Slot{}, fmt.Errorf("doctorclient: reserve slot returned %d", resp.StatusCode)
	}
}

// ReleaseSlot returns a slot to the pool when an appointment is cancelled.
func (c *Client) ReleaseSlot(ctx context.Context, bearerToken, doctorID, slotID string) error {
	req, err := c.newRequest(ctx, http.MethodPost,
		fmt.Sprintf("/doctors/%s/slots/%s/release", doctorID, slotID), bearerToken, nil)
	if err != nil {
		return err
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("doctorclient: release slot: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK, http.StatusNoContent:
		return nil
	case http.StatusNotFound:
		return ErrSlotNotFound
	default:
		return fmt.Errorf("doctorclient: release slot returned %d", resp.StatusCode)
	}
}

func (c *Client) newRequest(ctx context.Context, method, path, bearerToken string, body []byte) (*http.Request, error) {
	var reader *bytes.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	} else {
		reader = bytes.NewReader(nil)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
	if err != nil {
		return nil, fmt.Errorf("doctorclient: build request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+bearerToken)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return req, nil
}
