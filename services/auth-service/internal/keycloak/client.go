// Package keycloak is a minimal Admin API client covering only what
// auth-service needs: obtaining a service-account token and creating realm
// users with a password and a realm role.
package keycloak

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// ErrUserExists is returned when Keycloak rejects a registration because the
// username or email is already taken.
var ErrUserExists = errors.New("keycloak: user already exists")

// Client talks to the Keycloak Admin REST API using the client-credentials
// grant of a confidential client.
type Client struct {
	baseURL      string
	realm        string
	clientID     string
	clientSecret string
	http         *http.Client

	mu          sync.Mutex
	token       string
	tokenExpiry time.Time
}

// Config holds the connection settings for a Client.
type Config struct {
	BaseURL      string
	Realm        string
	ClientID     string
	ClientSecret string
}

// New builds a Client from cfg.
func New(cfg Config) *Client {
	return &Client{
		baseURL:      strings.TrimRight(cfg.BaseURL, "/"),
		realm:        cfg.Realm,
		clientID:     cfg.ClientID,
		clientSecret: cfg.ClientSecret,
		http:         &http.Client{Timeout: 10 * time.Second},
	}
}

// NewUser describes a realm user to create.
type NewUser struct {
	Username  string
	Email     string
	FirstName string
	LastName  string
	Password  string
	Role      string
}

// serviceToken returns a cached service-account access token, refreshing it
// shortly before expiry.
func (c *Client) serviceToken(ctx context.Context) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.token != "" && time.Now().Before(c.tokenExpiry) {
		return c.token, nil
	}

	form := url.Values{
		"grant_type":    {"client_credentials"},
		"client_id":     {c.clientID},
		"client_secret": {c.clientSecret},
	}
	endpoint := fmt.Sprintf("%s/realms/%s/protocol/openid-connect/token", c.baseURL, c.realm)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := c.http.Do(req)
	if err != nil {
		return "", fmt.Errorf("keycloak: token request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("keycloak: token request returned %d", resp.StatusCode)
	}

	var payload struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return "", fmt.Errorf("keycloak: decode token: %w", err)
	}

	c.token = payload.AccessToken
	c.tokenExpiry = time.Now().Add(time.Duration(payload.ExpiresIn-30) * time.Second)
	return c.token, nil
}

// CreateUser registers the user, sets their password and assigns the realm
// role, returning the new Keycloak user id.
func (c *Client) CreateUser(ctx context.Context, u NewUser) (string, error) {
	token, err := c.serviceToken(ctx)
	if err != nil {
		return "", err
	}

	body := map[string]interface{}{
		"username":      u.Username,
		"email":         u.Email,
		"firstName":     u.FirstName,
		"lastName":      u.LastName,
		"enabled":       true,
		"emailVerified": true,
		"credentials": []map[string]interface{}{
			{"type": "password", "value": u.Password, "temporary": false},
		},
	}

	userID, err := c.createUserRequest(ctx, token, body)
	if err != nil {
		return "", err
	}

	if err := c.assignRealmRole(ctx, token, userID, u.Role); err != nil {
		return "", err
	}

	return userID, nil
}

func (c *Client) createUserRequest(ctx context.Context, token string, body map[string]interface{}) (string, error) {
	payload, err := json.Marshal(body)
	if err != nil {
		return "", err
	}

	endpoint := fmt.Sprintf("%s/admin/realms/%s/users", c.baseURL, c.realm)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return "", fmt.Errorf("keycloak: create user: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusCreated:
		// Keycloak returns the new id only in the Location header.
		location := resp.Header.Get("Location")
		idx := strings.LastIndex(location, "/")
		if idx == -1 {
			return "", errors.New("keycloak: missing user id in Location header")
		}
		return location[idx+1:], nil
	case http.StatusConflict:
		return "", ErrUserExists
	default:
		return "", fmt.Errorf("keycloak: create user returned %d", resp.StatusCode)
	}
}

func (c *Client) assignRealmRole(ctx context.Context, token, userID, role string) error {
	roleRep, err := c.realmRole(ctx, token, role)
	if err != nil {
		return err
	}

	payload, err := json.Marshal([]map[string]interface{}{roleRep})
	if err != nil {
		return err
	}

	endpoint := fmt.Sprintf("%s/admin/realms/%s/users/%s/role-mappings/realm", c.baseURL, c.realm, userID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("keycloak: assign role: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("keycloak: assign role returned %d", resp.StatusCode)
	}
	return nil
}

func (c *Client) realmRole(ctx context.Context, token, role string) (map[string]interface{}, error) {
	endpoint := fmt.Sprintf("%s/admin/realms/%s/roles/%s", c.baseURL, c.realm, url.PathEscape(role))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("keycloak: fetch role: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("keycloak: fetch role %q returned %d", role, resp.StatusCode)
	}

	var rep map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&rep); err != nil {
		return nil, fmt.Errorf("keycloak: decode role: %w", err)
	}
	return rep, nil
}
