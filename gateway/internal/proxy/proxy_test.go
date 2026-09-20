package proxy_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/davidmm07/mediflow/gateway/internal/proxy"
	"github.com/rs/zerolog"
)

// upstream is a stand-in service that reports which one was reached.
func upstream(name string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Upstream", name)
		w.WriteHeader(http.StatusOK)
	}))
}

// TestRoutingPrecedence pins the one piece of routing that is not obvious:
// a practitioner's agenda reads as a sub-resource of /doctors but is owned by
// appointment-service. It only works because chi matches the concrete pattern
// ahead of the /doctors/* wildcard, so this asserts that rather than assuming
// it. Both routes are public so the test needs no Keycloak.
func TestRoutingPrecedence(t *testing.T) {
	doctors := upstream("doctor-service")
	defer doctors.Close()
	appointments := upstream("appointment-service")
	defer appointments.Close()

	gw, err := proxy.New(nil, zerolog.Nop(), []proxy.Route{
		{Prefix: "/doctors", Upstream: doctors.URL, Public: true},
		{
			Prefix:   "/appointments",
			Upstream: appointments.URL,
			Public:   true,
			Patterns: []string{"/doctors/{doctorID}/appointments"},
		},
	})
	if err != nil {
		t.Fatalf("building the gateway: %v", err)
	}

	edge := httptest.NewServer(gw.Handler())
	defer edge.Close()

	cases := []struct {
		path string
		want string
	}{
		{"/doctors", "doctor-service"},
		{"/doctors/doc-1", "doctor-service"},
		{"/doctors/doc-1/slots", "doctor-service"},
		{"/doctors/doc-1/slots/slot-1/reserve", "doctor-service"},
		{"/appointments", "appointment-service"},
		{"/appointments/appt-1/cancel", "appointment-service"},

		// The whole point of the Patterns field.
		{"/doctors/doc-1/appointments", "appointment-service"},
	}

	for _, tc := range cases {
		t.Run(tc.path, func(t *testing.T) {
			resp, err := http.Get(edge.URL + tc.path)
			if err != nil {
				t.Fatalf("requesting %s: %v", tc.path, err)
			}
			defer resp.Body.Close()

			if got := resp.Header.Get("X-Upstream"); got != tc.want {
				t.Errorf("%s reached %q, want %q", tc.path, got, tc.want)
			}
		})
	}
}
