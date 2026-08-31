// Package config reads the small set of environment variables MediFlow
// services agree on, failing fast when a required one is missing rather than
// booting into a half-configured state.
package config

import (
	"fmt"
	"os"
	"strings"
)

// MustGet returns the value of key or terminates with a clear message. Used
// for settings a service genuinely cannot run without (Mongo URI, issuer).
func MustGet(key string) string {
	v := os.Getenv(key)
	if v == "" {
		panic(fmt.Sprintf("config: required environment variable %s is not set", key))
	}
	return v
}

// Get returns the value of key or fallback when unset.
func Get(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// GetList splits a comma-separated variable (e.g. KAFKA_BROKERS) into a
// trimmed slice.
func GetList(key string, fallback []string) []string {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

// Addr returns the ":port" listen address from PORT, defaulting to 8080.
func Addr() string {
	return ":" + Get("PORT", "8080")
}
