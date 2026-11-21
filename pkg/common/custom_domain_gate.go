package common

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/beam-cloud/beta9/pkg/types"
)

const (
	customDomainGateFeature     = "custom_domains"
	customDomainGateTimeout     = 5 * time.Second
	customDomainGateDenied      = "Custom domains are not enabled for this workspace."
	customDomainGateUnavailable = "Unable to check whether custom domains are enabled for this workspace; try again shortly."
)

var customDomainGateClient = &http.Client{Timeout: customDomainGateTimeout}

// CheckCustomDomainAccess asks the gate at config.DomainGateURL whether a
// workspace may add custom domains. It returns "" when the workspace may, and
// otherwise the message to show the user. Without a gate URL every workspace
// may. A gate that cannot be reached refuses: only new domains are checked, so
// a refusal stops nothing that already runs, and the user can try again.
func CheckCustomDomainAccess(ctx context.Context, config types.HTTPConfig, workspaceExternalId string) string {
	url := strings.TrimSpace(config.DomainGateURL)
	if url == "" {
		return ""
	}

	body, err := json.Marshal(map[string]string{"workspace_id": workspaceExternalId, "feature": customDomainGateFeature})
	if err != nil {
		return customDomainGateUnavailable
	}

	ctx, cancel := context.WithTimeout(ctx, customDomainGateTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		log.Error().Err(err).Msg("custom domain gate: invalid request")
		return customDomainGateUnavailable
	}
	req.Header.Set("Content-Type", "application/json")
	if token := strings.TrimSpace(config.DomainGateToken); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := customDomainGateClient.Do(req)
	if err != nil {
		log.Warn().Err(err).Str("workspace_id", workspaceExternalId).Msg("custom domain gate: unreachable")
		return customDomainGateUnavailable
	}
	defer resp.Body.Close()

	var decision struct {
		OK      bool   `json:"ok"`
		Message string `json:"message"`
	}
	if resp.StatusCode >= http.StatusBadRequest {
		log.Warn().Int("status", resp.StatusCode).Str("workspace_id", workspaceExternalId).Msg("custom domain gate: request failed")
		return customDomainGateUnavailable
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&decision); err != nil {
		log.Warn().Err(err).Str("workspace_id", workspaceExternalId).Msg("custom domain gate: malformed response")
		return customDomainGateUnavailable
	}
	if !decision.OK {
		return cmp.Or(strings.TrimSpace(decision.Message), customDomainGateDenied)
	}
	return ""
}
