// Command smoke walks MediFlow's whole distributed flow against a running
// stack: register through auth-service, obtain a real Keycloak token, publish
// a doctor profile and a slot, book it through appointment-service (which
// calls doctor-service synchronously), then poll until the Kafka-driven
// notification shows up, and finally cancel and check the slot came back.
//
// The notification step is the one that matters. Nothing in the HTTP
// responses tells you the event was actually delivered, so without polling
// for it the asynchronous half of the system is untested end to end.
//
// It is written to be run repeatedly against the same stack. Anything it
// creates is either unique per run or reused when it already exists, because
// a smoke test that only passes against a pristine database stops being run.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

type config struct {
	gateway  string
	keycloak string
	realm    string
	clientID string
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func loadConfig() config {
	return config{
		gateway:  strings.TrimRight(envOr("GATEWAY", "http://localhost:8080"), "/"),
		keycloak: strings.TrimRight(envOr("KEYCLOAK", "http://localhost:8081"), "/"),
		realm:    envOr("REALM", "mediflow"),
		clientID: envOr("CLIENT_ID", "mediflow-gateway"),
	}
}

// ---------------------------------------------------------------- output

var useColor = os.Getenv("NO_COLOR") == ""

func paint(code, s string) string {
	if !useColor {
		return s
	}
	return "\033[" + code + "m" + s + "\033[0m"
}

func step(format string, args ...any) {
	fmt.Printf("\n%s\n", paint("1;36", "==> "+fmt.Sprintf(format, args...)))
}

func info(format string, args ...any) {
	fmt.Printf("    %s\n", fmt.Sprintf(format, args...))
}

// fail ends the run with a non-zero status. Every call site passes the server's
// own response, because "booking failed" without the body is the kind of
// message that sends you reading logs for ten minutes.
func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "\n%s\n", paint("1;31", "FAIL: "+fmt.Sprintf(format, args...)))
	os.Exit(1)
}

// ---------------------------------------------------------------- client

type client struct {
	http *http.Client
	cfg  config
}

// response carries enough of the raw reply to build a useful error message.
type response struct {
	status int
	body   []byte
}

// apiError returns the server's `error` field when the body is MediFlow's
// standard error envelope, or the raw body when it is not.
func (r response) apiError() string {
	var env struct {
		Error string `json:"error"`
	}
	if json.Unmarshal(r.body, &env) == nil && env.Error != "" {
		return env.Error
	}
	return strings.TrimSpace(string(r.body))
}

func (c *client) do(ctx context.Context, method, url, token string, in, out any) (response, error) {
	var body io.Reader
	if in != nil {
		payload, err := json.Marshal(in)
		if err != nil {
			return response{}, fmt.Errorf("encode request: %w", err)
		}
		body = bytes.NewReader(payload)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return response{}, err
	}
	req.Header.Set("Accept", "application/json")
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return response{}, err
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return response{}, err
	}

	r := response{status: resp.StatusCode, body: raw}
	if out != nil && resp.StatusCode < 300 && len(raw) > 0 {
		if err := json.Unmarshal(raw, out); err != nil {
			return r, fmt.Errorf("decode %s %s: %w", method, url, err)
		}
	}
	return r, nil
}

// token exchanges a username and password for an access token. The password
// grant exists in this realm only so this test can run unattended; a browser
// client uses Authorization Code + PKCE.
func (c *client) token(ctx context.Context, username, password string) (string, error) {
	form := url.Values{
		"client_id":  {c.cfg.clientID},
		"grant_type": {"password"},
		"username":   {username},
		"password":   {password},
	}
	endpoint := fmt.Sprintf("%s/realms/%s/protocol/openid-connect/token", c.cfg.keycloak, c.cfg.realm)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := c.http.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("keycloak returned %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}

	var out struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return "", fmt.Errorf("decode token response: %w", err)
	}
	if out.AccessToken == "" {
		return "", fmt.Errorf("keycloak returned no access_token")
	}
	return out.AccessToken, nil
}

// ---------------------------------------------------------------- payloads

type doctor struct {
	ID        string `json:"id"`
	FullName  string `json:"full_name"`
	Specialty string `json:"specialty"`
}

type doctorList struct {
	Doctors []doctor `json:"doctors"`
}

type slot struct {
	ID       string    `json:"id"`
	StartsAt time.Time `json:"starts_at"`
	EndsAt   time.Time `json:"ends_at"`
}

type slotList struct {
	Slots []slot `json:"slots"`
}

type appointment struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

type notificationList struct {
	Notifications []struct {
		SourceID string `json:"source_id"`
		Kind     string `json:"kind"`
		Title    string `json:"title"`
	} `json:"notifications"`
}

// ---------------------------------------------------------------- steps

func (c *client) waitForGateway(ctx context.Context) {
	step("Waiting for the gateway to become healthy")

	deadline := time.Now().Add(2 * time.Minute)
	for {
		resp, err := c.do(ctx, http.MethodGet, c.cfg.gateway+"/health", "", nil, nil)
		if err == nil && resp.status == http.StatusOK {
			info("gateway is up")
			return
		}
		if time.Now().After(deadline) {
			fail("gateway never became healthy at %s", c.cfg.gateway)
		}
		time.Sleep(2 * time.Second)
	}
}

func (c *client) registerPatient(ctx context.Context) (username, password string) {
	step("Registering a new patient through auth-service")

	username = fmt.Sprintf("smoke.patient.%d", time.Now().UnixNano())
	password = "smoke-password-123"

	var out struct {
		UserID string `json:"user_id"`
	}
	resp, err := c.do(ctx, http.MethodPost, c.cfg.gateway+"/auth/register", "", map[string]any{
		"username":   username,
		"email":      username + "@mediflow.dev",
		"first_name": "Smoke",
		"last_name":  "Tester",
		"password":   password,
		"role":       "patient",
	}, &out)
	if err != nil {
		fail("registration request failed: %v", err)
	}
	if resp.status != http.StatusCreated && resp.status != http.StatusOK {
		fail("registration returned %d: %s", resp.status, resp.apiError())
	}

	info("registered %s (%s)", username, out.UserID)
	return username, password
}

// ensureDoctor publishes a profile for the seeded doctor, reusing the existing
// one when a previous run already created it. doctor-service allows one
// profile per Keycloak user and answers 409, which is what makes the reuse
// branch reachable on every run after the first.
func (c *client) ensureDoctor(ctx context.Context, token string) doctor {
	step("Publishing a doctor profile")

	var created doctor
	resp, err := c.do(ctx, http.MethodPost, c.cfg.gateway+"/doctors", token, map[string]any{
		"full_name":        "Gregory House",
		"specialty":        "cardiology",
		"license_number":   "ES-CARD-99120",
		"bio":              "Diagnostic medicine",
		"consultation_fee": 75,
		"languages":        []string{"es", "en"},
	}, &created)
	if err != nil {
		fail("create doctor request failed: %v", err)
	}

	switch resp.status {
	case http.StatusCreated, http.StatusOK:
		info("created doctor %s", created.ID)
		return created

	case http.StatusConflict:
		var list doctorList
		listResp, err := c.do(ctx, http.MethodGet,
			c.cfg.gateway+"/doctors?specialty=cardiology", token, nil, &list)
		if err != nil {
			fail("listing doctors failed: %v", err)
		}
		if listResp.status != http.StatusOK || len(list.Doctors) == 0 {
			fail("doctor already exists but none could be listed (%d): %s",
				listResp.status, listResp.apiError())
		}
		info("reusing existing doctor %s", list.Doctors[0].ID)
		return list.Doctors[0]

	default:
		fail("creating the doctor returned %d: %s", resp.status, resp.apiError())
		return doctor{}
	}
}

// openSlot creates an availability window, walking forward in 30-minute steps
// until it finds one that does not overlap. Overlap is a 409 from
// doctor-service, and hitting it is expected: every previous run of this test
// left a slot behind, and cancelling an appointment releases a slot rather
// than deleting it.
func (c *client) openSlot(ctx context.Context, token, doctorID string) slot {
	step("Opening an availability slot")

	const (
		duration = 30 * time.Minute
		attempts = 96 // two days' worth of 30-minute windows
	)
	base := time.Now().UTC().AddDate(0, 0, 7).Truncate(time.Hour)

	for i := 0; i < attempts; i++ {
		start := base.Add(time.Duration(i) * duration)
		end := start.Add(duration)

		var created slot
		resp, err := c.do(ctx, http.MethodPost,
			fmt.Sprintf("%s/doctors/%s/slots", c.cfg.gateway, doctorID), token, map[string]any{
				"starts_at": start.Format(time.RFC3339),
				"ends_at":   end.Format(time.RFC3339),
			}, &created)
		if err != nil {
			fail("create slot request failed: %v", err)
		}

		switch resp.status {
		case http.StatusCreated, http.StatusOK:
			info("slot %s at %s", created.ID, start.Format(time.RFC3339))
			return created
		case http.StatusConflict:
			continue // taken by an earlier run; try the next window
		default:
			fail("creating a slot returned %d: %s", resp.status, resp.apiError())
		}
	}

	fail("no free slot found in %d attempts starting at %s", attempts, base.Format(time.RFC3339))
	return slot{}
}

func (c *client) book(ctx context.Context, token, doctorID, slotID string) appointment {
	step("Booking the appointment (appointment-service -> doctor-service)")

	var booked appointment
	resp, err := c.do(ctx, http.MethodPost, c.cfg.gateway+"/appointments", token, map[string]any{
		"doctor_id": doctorID,
		"slot_id":   slotID,
		"reason":    "smoke test consultation",
	}, &booked)
	if err != nil {
		fail("booking request failed: %v", err)
	}
	if resp.status != http.StatusCreated && resp.status != http.StatusOK {
		fail("booking returned %d: %s", resp.status, resp.apiError())
	}

	info("appointment %s is %s", booked.ID, booked.Status)
	return booked
}

// slotOffered reports whether the slot is still advertised as available.
func (c *client) slotOffered(ctx context.Context, token, doctorID, slotID string) bool {
	var list slotList
	resp, err := c.do(ctx, http.MethodGet,
		fmt.Sprintf("%s/doctors/%s/slots?available=true", c.cfg.gateway, doctorID), token, nil, &list)
	if err != nil {
		fail("listing slots failed: %v", err)
	}
	if resp.status != http.StatusOK {
		fail("listing slots returned %d: %s", resp.status, resp.apiError())
	}

	for _, s := range list.Slots {
		if s.ID == slotID {
			return true
		}
	}
	return false
}

// awaitNotification polls until notification-service has reacted to the
// booking. This is the only assertion that proves the Kafka path works: the
// booking response says nothing about whether the event was delivered.
func (c *client) awaitNotification(ctx context.Context, token, appointmentID string) {
	step("Waiting for the Kafka-driven notification")

	deadline := time.Now().Add(60 * time.Second)
	for {
		var list notificationList
		resp, err := c.do(ctx, http.MethodGet, c.cfg.gateway+"/notifications/me", token, nil, &list)
		if err != nil {
			fail("listing notifications failed: %v", err)
		}
		if resp.status != http.StatusOK {
			fail("listing notifications returned %d: %s", resp.status, resp.apiError())
		}

		for _, n := range list.Notifications {
			if n.SourceID == appointmentID {
				info("delivered: %q (%s)", n.Title, n.Kind)
				return
			}
		}

		if time.Now().After(deadline) {
			fail("no notification arrived for appointment %s within 60s", appointmentID)
		}
		time.Sleep(2 * time.Second)
	}
}

func (c *client) cancel(ctx context.Context, token, appointmentID string) {
	step("Cancelling the appointment")

	var cancelled appointment
	resp, err := c.do(ctx, http.MethodPost,
		fmt.Sprintf("%s/appointments/%s/cancel", c.cfg.gateway, appointmentID), token, map[string]any{
			"reason": "smoke test cleanup",
		}, &cancelled)
	if err != nil {
		fail("cancellation request failed: %v", err)
	}
	if resp.status != http.StatusOK {
		fail("cancellation returned %d: %s", resp.status, resp.apiError())
	}
	if cancelled.Status != "cancelled" {
		fail("cancellation left the appointment in state %q", cancelled.Status)
	}

	info("appointment %s is cancelled", appointmentID)
}

// ---------------------------------------------------------------- main

func main() {
	cfg := loadConfig()
	c := &client{
		http: &http.Client{Timeout: 15 * time.Second},
		cfg:  cfg,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	c.waitForGateway(ctx)

	patientUser, patientPass := c.registerPatient(ctx)

	step("Obtaining tokens from Keycloak")
	doctorToken, err := c.token(ctx, "dr.house", "doctor123")
	if err != nil {
		fail("could not authenticate the seeded doctor: %v", err)
	}
	patientToken, err := c.token(ctx, patientUser, patientPass)
	if err != nil {
		fail("could not authenticate the new patient: %v", err)
	}
	info("got tokens for dr.house and %s", patientUser)

	doc := c.ensureDoctor(ctx, doctorToken)
	free := c.openSlot(ctx, doctorToken, doc.ID)
	appt := c.book(ctx, patientToken, doc.ID, free.ID)

	step("Confirming the slot is no longer offered")
	if c.slotOffered(ctx, patientToken, doc.ID, free.ID) {
		fail("slot %s is still advertised as available after booking", free.ID)
	}
	info("slot %s withdrawn", free.ID)

	c.awaitNotification(ctx, patientToken, appt.ID)
	c.cancel(ctx, patientToken, appt.ID)

	step("Confirming the slot is released")
	if !c.slotOffered(ctx, patientToken, doc.ID, free.ID) {
		fail("slot %s was not released after cancellation", free.ID)
	}
	info("slot %s is back on offer", free.ID)

	fmt.Printf("\n%s\n", paint("1;32",
		"Smoke test passed: registration, booking, events and compensation all work."))
}
