package gatewayservices

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/rs/zerolog/log"
)

// ActiveDeploymentByName is the newest active deployment with exactly this name.
func (gws *GatewayService) ActiveDeploymentByName(ctx context.Context, workspace *types.Workspace, name string) (*types.DeploymentWithRelated, error) {
	deployments, err := gws.deploymentsByName(ctx, workspace, name)
	if err != nil {
		return nil, err
	}
	active := make([]types.DeploymentWithRelated, 0, len(deployments))
	for _, d := range deployments {
		if d.Active {
			active = append(active, d)
		}
	}
	if len(active) == 0 {
		return nil, fmt.Errorf("no active deployment named %q", name)
	}
	return newestDeployment(active), nil
}

// RedeployWithConfig deploys a new version of an app's newest active
// deployment from its config after `mutate` edits it. This is how settings
// change without a code push; the previous version stays for rollback.
func (gws *GatewayService) RedeployWithConfig(ctx context.Context, authInfo *auth.AuthInfo, appName string, mutate func(*types.StubConfigV1) error) (*pb.DeployStubResponse, error) {
	deployment, err := gws.ActiveDeploymentByName(ctx, authInfo.Workspace, appName)
	if err != nil {
		return nil, err
	}
	return gws.RedeployStub(ctx, authInfo, deployment, mutate)
}

// RedeployStub deploys a new version of the app from one deployment's config
// after `mutate` edits it. The app's platform env is applied on top, as on
// every deploy, before the stub is stored, so the deploy reuses that stub.
func (gws *GatewayService) RedeployStub(ctx context.Context, authInfo *auth.AuthInfo, deployment *types.DeploymentWithRelated, mutate func(*types.StubConfigV1) error) (*pb.DeployStubResponse, error) {
	stub, err := gws.backendRepo.GetStubByExternalId(ctx, deployment.Stub.ExternalId)
	if err != nil {
		return nil, fmt.Errorf("load stub: %w", err)
	}
	config, err := stub.UnmarshalConfig()
	if err != nil {
		return nil, fmt.Errorf("decode stub config: %w", err)
	}
	if err := mutate(config); err != nil {
		return nil, err
	}
	refreshManagedPostgres(config)
	if valid, msg := types.ValidateCpuAndMemory(config.Runtime.Cpu, config.Runtime.Memory, gws.appConfig.GatewayService.StubLimits); !valid {
		return nil, errors.New(msg)
	}
	if err := gws.resolveStubCPUTier(ctx, authInfo.Workspace, stub.Type, config, true); err != nil {
		return nil, err
	}
	withEnv, err := gws.applyPlatformEnv(ctx, authInfo, stub, config, deployment.Name)
	if err != nil {
		return nil, err
	}
	next, err := gws.backendRepo.GetOrCreateStub(ctx, stub.Name, string(stub.Type), withEnv.config, stub.ObjectId, authInfo.Workspace.Id, true, stub.AppId)
	if err != nil {
		return nil, fmt.Errorf("create stub: %w", err)
	}
	return gws.DeployStub(ctx, &pb.DeployStubRequest{StubId: next.ExternalId, Name: deployment.Name})
}

// SetDeploymentEnv sets or removes platform env on a deployment and deploys a
// new version with it. Values may hold `${{...}}` references and resolve the
// way a fresh deploy would; a variable set here replaces any binding it had.
//
// What is set here is stored apart from the stub (see types.DeploymentEnv), so
// later deploys of the name keep it, code pushes included. Removing a
// variable takes it out of the platform env and of the new version; one the
// code defines comes back with the next code deploy. When the deploy fails,
// the platform env is put back as it was, so it matches what runs.
func (gws *GatewayService) SetDeploymentEnv(ctx context.Context, authInfo *auth.AuthInfo, appName string, set map[string]string, unset []string) (*pb.DeployStubResponse, error) {
	workspace := authInfo.Workspace
	deployment, err := gws.ActiveDeploymentByName(ctx, workspace, appName)
	if err != nil {
		return nil, err
	}

	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	entries := make([]string, 0, len(keys))
	for _, key := range keys {
		entries = append(entries, key+"="+set[key])
	}
	expanded, bindings, err := gws.expandReferences(ctx, authInfo, appName, entries)
	if err != nil {
		return nil, err
	}
	// A secret that does not exist fails here, before anything is stored.
	if _, err := gws.resolveSecretBindings(ctx, workspace, bindings); err != nil {
		return nil, err
	}

	rows := make([]types.DeploymentEnv, 0, len(expanded)+len(bindings))
	for _, entry := range expanded {
		key, value, _ := strings.Cut(entry, "=")
		rows = append(rows, types.DeploymentEnv{Key: key, Value: value})
	}
	for _, binding := range bindings {
		rows = append(rows, types.DeploymentEnv{Key: binding.envName(), SecretName: binding.Name})
	}
	touched := make(map[string]bool, len(keys)+len(unset))
	for _, key := range keys {
		touched[key] = true
	}
	var removed []string
	for _, key := range unset {
		if !touched[key] {
			removed = append(removed, key)
		}
		touched[key] = true
	}

	previous, err := gws.backendRepo.ListDeploymentEnv(ctx, workspace.Id, appName)
	if err != nil {
		return nil, fmt.Errorf("load env: %w", err)
	}
	if err := gws.backendRepo.UpdateDeploymentEnv(ctx, workspace.Id, appName, rows, removed); err != nil {
		return nil, fmt.Errorf("store env: %w", err)
	}

	// The new version drops what it had for these variables; the platform env
	// applied while it deploys sets the ones that were set.
	res, err := gws.RedeployStub(ctx, authInfo, deployment, func(config *types.StubConfigV1) error {
		config.Env = slices.DeleteFunc(config.Env, func(entry string) bool {
			key, _, _ := strings.Cut(entry, "=")
			return touched[key]
		})
		config.Secrets = slices.DeleteFunc(config.Secrets, func(secret types.Secret) bool {
			return touched[secret.EnvVarName()]
		})
		return nil
	})
	if err == nil && res.GetOk() {
		return res, nil
	}
	if restoreErr := gws.restoreDeploymentEnv(ctx, workspace.Id, appName, previous, touched); restoreErr != nil {
		log.Error().Err(restoreErr).Str("deployment", appName).Msg("failed to restore platform env after a failed deploy")
		if err != nil {
			err = errors.Join(err, fmt.Errorf("restore env: %w", restoreErr))
		}
	}
	return res, err
}

// restoreDeploymentEnv puts the touched keys of a deployment's platform env
// back to previous.
func (gws *GatewayService) restoreDeploymentEnv(ctx context.Context, workspaceId uint, name string, previous []types.DeploymentEnv, touched map[string]bool) error {
	before := make(map[string]types.DeploymentEnv, len(previous))
	for _, row := range previous {
		before[row.Key] = row
	}
	var set []types.DeploymentEnv
	var unset []string
	for key := range touched {
		if row, ok := before[key]; ok {
			set = append(set, row)
		} else {
			unset = append(unset, key)
		}
	}
	return gws.backendRepo.UpdateDeploymentEnv(ctx, workspaceId, name, set, unset)
}

// platformEnvResult is a stub config with the platform env of its deployment
// applied.
type platformEnvResult struct {
	config types.StubConfigV1
	// changed is whether config differs from the one it started from, in
	// which case the deploy stores it as a new stub.
	changed bool
	// overridden are the variables whose value in code the platform env
	// replaced.
	overridden []string
}

// warning says which variables in code the platform env overrode.
func (r platformEnvResult) warning() string {
	if len(r.overridden) == 0 {
		return ""
	}
	return fmt.Sprintf("Env set with set_env overrides the value in code for: %s. Remove it with set_env to use the value in code.", strings.Join(r.overridden, ", "))
}

// applyPlatformEnv applies the platform env of the deployment `name` (see
// types.DeploymentEnv) to config, and leaves config itself untouched. Platform
// env wins over env the config sets.
//
// Bound secrets are resolved from the workspace on every deploy, as a fresh
// stub's are, so a rotated value or changed hosts take effect; sandbox stubs
// get the same egress check. A bound secret that no longer exists fails the
// deploy, with every missing one listed.
//
// Platform env left from a deleted deployment is not applied to a new one
// with the same name: when no deployment of the name is left, it is removed.
func (gws *GatewayService) applyPlatformEnv(ctx context.Context, authInfo *auth.AuthInfo, stub *types.StubWithRelated, config *types.StubConfigV1, name string) (platformEnvResult, error) {
	unchanged := platformEnvResult{config: *config}
	if stub.Type.IsManagedEndpoint() {
		return unchanged, nil
	}
	workspace := authInfo.Workspace
	rows, err := gws.backendRepo.ListDeploymentEnv(ctx, workspace.Id, name)
	if err != nil {
		return unchanged, fmt.Errorf("load platform env: %w", err)
	}
	if len(rows) == 0 {
		return unchanged, nil
	}
	live, err := gws.deploymentsByName(ctx, workspace, name)
	if err != nil {
		return unchanged, fmt.Errorf("check deployments for platform env: %w", err)
	}
	if len(live) == 0 {
		if err := gws.backendRepo.DeleteDeploymentEnv(ctx, workspace.Id, name); err != nil {
			log.Warn().Err(err).Str("deployment", name).Msg("failed to remove platform env of a deleted deployment")
		}
		return unchanged, nil
	}

	secrets, err := gws.boundSecrets(ctx, workspace, rows)
	if err != nil {
		return unchanged, err
	}

	candidate := *config
	candidate.Env = append([]string(nil), config.Env...)
	candidate.Secrets = append([]types.Secret(nil), config.Secrets...)
	var overridden []string
	for _, row := range rows {
		if row.SecretName == "" {
			if value, ok := envValue(config.Env, row.Key); (ok && value != row.Value) || hasSecretBinding(config.Secrets, row.Key, "") {
				overridden = append(overridden, row.Key)
			}
			candidate.Env = setEnvValue(candidate.Env, row.Key, row.Value)
			candidate.Secrets = withoutSecretBinding(candidate.Secrets, row.Key)
			continue
		}
		if _, ok := envValue(config.Env, row.Key); ok || hasSecretBinding(config.Secrets, row.Key, row.SecretName) {
			overridden = append(overridden, row.Key)
		}
		secret := secrets[row.SecretName]
		secret.EnvName = row.Key
		candidate.Env = withoutEnvValue(candidate.Env, row.Key)
		candidate.Secrets = append(withoutSecretBinding(candidate.Secrets, row.Key), secret)
	}
	if stub.Type.IsSandbox() {
		if err := common.ValidateSandboxSecrets(candidate.Secrets, candidate.AllowList); err != nil {
			return unchanged, err
		}
	}

	changed, err := envDiffers(config, &candidate)
	if err != nil {
		return unchanged, err
	}
	return platformEnvResult{config: candidate, changed: changed, overridden: overridden}, nil
}

// boundSecrets loads, by name, the workspace secrets platform env binds, and
// fails listing every one that no longer exists.
func (gws *GatewayService) boundSecrets(ctx context.Context, workspace *types.Workspace, rows []types.DeploymentEnv) (map[string]types.Secret, error) {
	var names []string
	for _, row := range rows {
		if row.SecretName != "" {
			names = append(names, row.SecretName)
		}
	}
	if len(names) == 0 {
		return nil, nil
	}
	existing, err := gws.backendRepo.GetSecretsByName(ctx, workspace, names)
	if err != nil {
		return nil, fmt.Errorf("load secrets bound with set_env: %w", err)
	}
	byName := make(map[string]types.Secret, len(existing))
	for _, secret := range existing {
		byName[secret.Name] = secret
	}
	var missing []string
	for _, row := range rows {
		if _, ok := byName[row.SecretName]; row.SecretName != "" && !ok {
			missing = append(missing, fmt.Sprintf("%s (secret %q)", row.Key, row.SecretName))
		}
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("secrets bound with set_env no longer exist; create them or remove the variables with set_env: %s", strings.Join(missing, ", "))
	}
	return byName, nil
}

// createPlatformEnvStub stores config, which carries platform env, as the
// stub to deploy in place of stub. Stubs are deduplicated by config
// (forceCreate=false), so a retried deploy reuses the row.
func (gws *GatewayService) createPlatformEnvStub(ctx context.Context, authInfo *auth.AuthInfo, stub *types.StubWithRelated, config types.StubConfigV1) (*types.StubWithRelated, error) {
	created, err := gws.backendRepo.GetOrCreateStub(ctx, stub.Name, string(stub.Type), config, stub.ObjectId, authInfo.Workspace.Id, false, stub.AppId)
	if err != nil {
		return nil, fmt.Errorf("create stub with platform env: %w", err)
	}
	// Only the stub row changed; workspace and object are the same.
	next := *stub
	next.Stub = created
	return &next, nil
}

func envValue(env []string, key string) (string, bool) {
	for _, entry := range env {
		if k, value, _ := strings.Cut(entry, "="); k == key {
			return value, true
		}
	}
	return "", false
}

// setEnvValue sets key in place of its first entry, drops any other entry for
// it, and appends it when there is none.
func setEnvValue(env []string, key, value string) []string {
	out := make([]string, 0, len(env)+1)
	placed := false
	for _, entry := range env {
		if k, _, _ := strings.Cut(entry, "="); k == key {
			if !placed {
				out = append(out, key+"="+value)
				placed = true
			}
			continue
		}
		out = append(out, entry)
	}
	if !placed {
		out = append(out, key+"="+value)
	}
	return out
}

func withoutEnvValue(env []string, key string) []string {
	return slices.DeleteFunc(env, func(entry string) bool {
		k, _, _ := strings.Cut(entry, "=")
		return k == key
	})
}

// hasSecretBinding reports whether a secret is bound as key, other than the
// secret named except.
func hasSecretBinding(secrets []types.Secret, key, except string) bool {
	return slices.ContainsFunc(secrets, func(secret types.Secret) bool {
		return secret.EnvVarName() == key && secret.Name != except
	})
}

func withoutSecretBinding(secrets []types.Secret, key string) []types.Secret {
	return slices.DeleteFunc(secrets, func(secret types.Secret) bool {
		return secret.EnvVarName() == key
	})
}

// envDiffers compares the env and secret bindings of two configs as they are
// stored.
func envDiffers(a, b *types.StubConfigV1) (bool, error) {
	encode := func(c *types.StubConfigV1) ([]byte, error) {
		return json.Marshal(struct {
			Env     []string       `json:"env"`
			Secrets []types.Secret `json:"secrets"`
		}{c.Env, c.Secrets})
	}
	ea, err := encode(a)
	if err != nil {
		return false, err
	}
	eb, err := encode(b)
	if err != nil {
		return false, err
	}
	return string(ea) != string(eb), nil
}
