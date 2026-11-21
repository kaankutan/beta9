package common

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestCheckCustomDomainAccess(t *testing.T) {
	ctx := context.Background()

	// Without a gate every workspace may add domains.
	require.Empty(t, CheckCustomDomainAccess(ctx, types.HTTPConfig{}, "ws-1"))

	gate := func(t *testing.T, status int, body string) types.HTTPConfig {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, http.MethodPost, r.Method)
			require.Equal(t, "Bearer secret", r.Header.Get("Authorization"))
			var req map[string]string
			require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			require.Equal(t, map[string]string{"workspace_id": "ws-1", "feature": "custom_domains"}, req)
			w.WriteHeader(status)
			_, _ = w.Write([]byte(body))
		}))
		t.Cleanup(server.Close)
		return types.HTTPConfig{DomainGateURL: server.URL, DomainGateToken: "secret"}
	}

	for _, tc := range []struct {
		name   string
		status int
		body   string
		want   string
	}{
		{"allowed", http.StatusOK, `{"ok":true}`, ""},
		{"denied with the gate's message", http.StatusOK, `{"ok":false,"message":"Custom domains are available on the Team plan."}`, "Custom domains are available on the Team plan."},
		{"denied without a message", http.StatusOK, `{"ok":false}`, customDomainGateDenied},
		{"gate error", http.StatusInternalServerError, `{"ok":true}`, customDomainGateUnavailable},
		{"malformed answer", http.StatusOK, `not json`, customDomainGateUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, CheckCustomDomainAccess(ctx, gate(t, tc.status, tc.body), "ws-1"))
		})
	}

	t.Run("unreachable gate refuses", func(t *testing.T) {
		server := httptest.NewServer(http.NotFoundHandler())
		url := server.URL
		server.Close()
		require.Equal(t, customDomainGateUnavailable, CheckCustomDomainAccess(ctx, types.HTTPConfig{DomainGateURL: url}, "ws-1"))
	})
}
