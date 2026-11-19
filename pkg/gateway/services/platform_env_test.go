package gatewayservices

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
)

// envRepo keeps stubs, deployments, secrets and platform env in memory and
// records what a deploy writes.
type envRepo struct {
	repository.BackendRepository
	stubs       map[string]*types.StubWithRelated
	deployments []types.DeploymentWithRelated
	secrets     map[string]types.Secret
	env         map[string][]types.DeploymentEnv

	createStubErr       error
	createDeploymentErr error

	nextStubId         uint
	createdStubs       []types.StubConfigV1
	createdDeployments []types.Deployment
	updatedDeployments []types.Deployment
	deletedEnv         []string
}

var testWorkspace = types.Workspace{Id: 1, ExternalId: "ws-1", Name: "default"}

func (r *envRepo) GetStubByExternalId(_ context.Context, externalId string, _ ...types.QueryFilter) (*types.StubWithRelated, error) {
	if stub, ok := r.stubs[externalId]; ok {
		return stub, nil
	}
	return nil, sql.ErrNoRows
}

func (r *envRepo) GetLatestDeploymentByName(_ context.Context, _ uint, name string, stubType string, filterDeleted bool) (*types.DeploymentWithRelated, error) {
	var latest *types.DeploymentWithRelated
	for i := range r.deployments {
		d := &r.deployments[i]
		if d.Name != name || d.StubType != stubType || (filterDeleted && d.DeletedAt.Valid) {
			continue
		}
		if latest == nil || d.Version > latest.Version {
			latest = d
		}
	}
	return latest, nil
}

func (r *envRepo) ListDeploymentsWithRelated(_ context.Context, filter types.DeploymentFilter) ([]types.DeploymentWithRelated, error) {
	var out []types.DeploymentWithRelated
	for _, d := range r.deployments {
		if (filter.Name != "" && d.Name != filter.Name) || (filter.Active != nil && d.Active != *filter.Active) || (d.DeletedAt.Valid && !filter.ShowDeleted) {
			continue
		}
		out = append(out, d)
	}
	return out, nil
}

func (r *envRepo) GetSecretByName(_ context.Context, _ *types.Workspace, name string) (*types.Secret, error) {
	if secret, ok := r.secrets[name]; ok {
		return &secret, nil
	}
	return nil, sql.ErrNoRows
}

func (r *envRepo) GetSecretsByName(_ context.Context, _ *types.Workspace, names []string) ([]types.Secret, error) {
	var out []types.Secret
	for _, name := range names {
		if secret, ok := r.secrets[name]; ok {
			out = append(out, secret)
		}
	}
	return out, nil
}

func (r *envRepo) GetOrCreateStub(_ context.Context, name, stubType string, config types.StubConfigV1, _, _ uint, _ bool, appId uint) (types.Stub, error) {
	if r.createStubErr != nil {
		return types.Stub{}, r.createStubErr
	}
	encoded, err := json.Marshal(config)
	if err != nil {
		return types.Stub{}, err
	}
	r.nextStubId++
	stub := types.Stub{Id: 100 + r.nextStubId, ExternalId: fmt.Sprintf("stub-created-%d", r.nextStubId), Name: name, Type: types.StubType(stubType), Config: string(encoded), AppId: appId}
	r.stubs[stub.ExternalId] = &types.StubWithRelated{Stub: stub, Workspace: testWorkspace}
	r.createdStubs = append(r.createdStubs, config)
	return stub, nil
}

func (r *envRepo) CreateDeployment(_ context.Context, workspaceId uint, name string, version uint, stubId uint, stubType string, appId uint) (*types.Deployment, error) {
	if r.createDeploymentErr != nil {
		return nil, r.createDeploymentErr
	}
	var stub types.Stub
	for _, s := range r.stubs {
		if s.Id == stubId {
			stub = s.Stub
		}
	}
	dep := types.Deployment{Id: 1000 + version, ExternalId: fmt.Sprintf("dep-v%d", version), Name: name, Active: true, Subdomain: name + "-abc1234", WorkspaceId: workspaceId, StubId: stubId, StubType: stubType, Version: version, AppId: appId}
	r.deployments = append(r.deployments, types.DeploymentWithRelated{Deployment: dep, Stub: stub, Workspace: testWorkspace})
	r.createdDeployments = append(r.createdDeployments, dep)
	return &dep, nil
}

func (r *envRepo) UpdateDeployment(_ context.Context, dep types.Deployment) (*types.Deployment, error) {
	r.updatedDeployments = append(r.updatedDeployments, dep)
	return &dep, nil
}

func (r *envRepo) ListDeploymentEnv(_ context.Context, _ uint, name string) ([]types.DeploymentEnv, error) {
	return slices.Clone(r.env[name]), nil
}

func (r *envRepo) UpdateDeploymentEnv(_ context.Context, _ uint, name string, set []types.DeploymentEnv, unset []string) error {
	rows := slices.DeleteFunc(slices.Clone(r.env[name]), func(row types.DeploymentEnv) bool {
		return slices.Contains(unset, row.Key)
	})
	for _, entry := range set {
		rows = slices.DeleteFunc(rows, func(row types.DeploymentEnv) bool { return row.Key == entry.Key })
		rows = append(rows, types.DeploymentEnv{Key: entry.Key, Value: entry.Value, SecretName: entry.SecretName})
	}
	slices.SortFunc(rows, func(a, b types.DeploymentEnv) int {
		if a.Key < b.Key {
			return -1
		}
		if a.Key > b.Key {
			return 1
		}
		return 0
	})
	r.env[name] = rows
	return nil
}

func (r *envRepo) DeleteDeploymentEnv(_ context.Context, _ uint, name string) error {
	delete(r.env, name)
	r.deletedEnv = append(r.deletedEnv, name)
	return nil
}

func (r *envRepo) addStub(t *testing.T, externalId string, stubType types.StubType, config types.StubConfigV1) *types.StubWithRelated {
	t.Helper()
	encoded, err := json.Marshal(config)
	require.NoError(t, err)
	r.nextStubId++
	stub := &types.StubWithRelated{
		Stub:      types.Stub{Id: r.nextStubId, ExternalId: externalId, Name: "app:handler", Type: stubType, Config: string(encoded), AppId: 7},
		Workspace: testWorkspace,
	}
	r.stubs[externalId] = stub
	return stub
}

func (r *envRepo) envKeys(name string) []string {
	keys := []string{}
	for _, row := range r.env[name] {
		keys = append(keys, row.Key)
	}
	return keys
}

// deployedConfig is the stub config of the newest deployment.
func (r *envRepo) deployedConfig(t *testing.T) *types.StubConfigV1 {
	t.Helper()
	require.NotEmpty(t, r.deployments)
	stub := r.deployments[len(r.deployments)-1].Stub
	config, err := stub.UnmarshalConfig()
	require.NoError(t, err)
	return config
}

var endpointDeployment = types.StubType(types.StubTypeEndpointDeployment)

func codeConfig(env ...string) types.StubConfigV1 {
	return types.StubConfigV1{Runtime: types.Runtime{Cpu: 1000, Memory: 1024}, Env: env}
}

// newEnvFixture has the app "my-api" running version 1 from stub-v1, whose
// code sets CODE=1.
func newEnvFixture(t *testing.T) (context.Context, *GatewayService, *envRepo, *auth.AuthInfo) {
	t.Helper()
	info := &auth.AuthInfo{Workspace: &types.Workspace{Id: testWorkspace.Id, ExternalId: testWorkspace.ExternalId, Name: testWorkspace.Name}}
	ctx := auth.ContextWithAuthInfo(context.Background(), info)
	rdb, err := repository.NewRedisClientForTest()
	require.NoError(t, err)
	t.Cleanup(func() { _ = rdb.Close() })

	repo := &envRepo{
		stubs: map[string]*types.StubWithRelated{},
		secrets: map[string]types.Secret{
			"db-pass": {Name: "db-pass", Value: "current", Hosts: []string{"db.internal"}, Placeholder: "p-db"},
			"token":   {Name: "token", Value: "tok", Hosts: []string{"api.internal"}, Placeholder: "p-tok"},
		},
		env: map[string][]types.DeploymentEnv{},
	}
	v1 := repo.addStub(t, "stub-v1", endpointDeployment, codeConfig("CODE=1"))
	repo.deployments = []types.DeploymentWithRelated{{
		Deployment: types.Deployment{Id: 1001, ExternalId: "dep-v1", Name: "my-api", Active: true, WorkspaceId: 1, StubId: v1.Id, StubType: types.StubTypeEndpointDeployment, Version: 1, AppId: 7},
		Stub:       v1.Stub,
		Workspace:  testWorkspace,
	}}

	svc := &GatewayService{
		backendRepo:   repo,
		containerRepo: &deploymentLifecycleContainerRepo{},
		redisClient:   rdb,
		scheduler:     scheduler.NewSchedulerForCapacityChecks(nil, nil, scheduler.NewWorkerPoolManager()),
	}
	svc.appConfig.GatewayService.StubLimits = types.StubLimits{Cpu: 64000, Memory: 65536}
	return ctx, svc, repo, info
}

func setPlatformEnv(repo *envRepo, rows ...types.DeploymentEnv) {
	repo.env["my-api"] = rows
}

func TestApplyPlatformEnvWithoutPlatformEnvChangesNothing(t *testing.T) {
	ctx, svc, repo, info := newEnvFixture(t)
	stub := repo.addStub(t, "stub-code", endpointDeployment, codeConfig("CODE=2"))
	config := codeConfig("CODE=2")

	got, err := svc.applyPlatformEnv(ctx, info, stub, &config, "my-api")

	require.NoError(t, err)
	require.False(t, got.changed)
	require.Equal(t, config, got.config)
	require.Empty(t, got.warning())
}

func TestApplyPlatformEnvSetsVariablesInPlaceAndOverridesCode(t *testing.T) {
	ctx, svc, repo, info := newEnvFixture(t)
	setPlatformEnv(repo, types.DeploymentEnv{Key: "FOO", Value: "platform"}, types.DeploymentEnv{Key: "NEW", Value: "x"})
	stub := repo.addStub(t, "stub-code", endpointDeployment, types.StubConfigV1{})
	config := codeConfig("A=1", "FOO=code", "B=2")

	got, err := svc.applyPlatformEnv(ctx, info, stub, &config, "my-api")

	require.NoError(t, err)
	require.True(t, got.changed)
	require.Equal(t, []string{"A=1", "FOO=platform", "B=2", "NEW=x"}, got.config.Env)
	require.Equal(t, []string{"FOO"}, got.overridden)
	require.Contains(t, got.warning(), "FOO")
	// The config it was given is not touched.
	require.Equal(t, []string{"A=1", "FOO=code", "B=2"}, config.Env)
}

func TestApplyPlatformEnvDoesNotReportAVariableWithTheSameValue(t *testing.T) {
	ctx, svc, repo, info := newEnvFixture(t)
	setPlatformEnv(repo, types.DeploymentEnv{Key: "FOO", Value: "same"})
	stub := repo.addStub(t, "stub-code", endpointDeployment, types.StubConfigV1{})
	config := codeConfig("FOO=same")

	got, err := svc.applyPlatformEnv(ctx, info, stub, &config, "my-api")

	require.NoError(t, err)
	require.False(t, got.changed)
	require.Empty(t, got.overridden)
}

// A bound secret takes the workspace's current row on every deploy, so a
// rotated value applies, and it replaces a plain variable of the same name.
func TestApplyPlatformEnvBindsTheCurrentSecret(t *testing.T) {
	ctx, svc, repo, info := newEnvFixture(t)
	setPlatformEnv(repo, types.DeploymentEnv{Key: "DB_PASSWORD", SecretName: "db-pass"})
	stub := repo.addStub(t, "stub-code", endpointDeployment, types.StubConfigV1{})
	config := codeConfig("DB_PASSWORD=plain", "CODE=1")

	got, err := svc.applyPlatformEnv(ctx, info, stub, &config, "my-api")

	require.NoError(t, err)
	require.True(t, got.changed)
	require.Equal(t, []string{"CODE=1"}, got.config.Env)
	require.Len(t, got.config.Secrets, 1)
	require.Equal(t, "db-pass", got.config.Secrets[0].Name)
	require.Equal(t, "DB_PASSWORD", got.config.Secrets[0].EnvName)
	require.Equal(t, "current", got.config.Secrets[0].Value)
	require.Equal(t, []string{"DB_PASSWORD"}, got.overridden)
}

func TestApplyPlatformEnvReplacesACodeSecretBinding(t *testing.T) {
	ctx, svc, repo, info := newEnvFixture(t)
	setPlatformEnv(repo, types.DeploymentEnv{Key: "TOKEN", SecretName: "token"})
	stub := repo.addStub(t, "stub-code", endpointDeployment, types.StubConfigV1{})
	config := codeConfig()
	config.Secrets = []types.Secret{{Name: "db-pass", EnvName: "TOKEN"}, {Name: "db-pass", EnvName: "KEEP"}}

	got, err := svc.applyPlatformEnv(ctx, info, stub, &config, "my-api")

	require.NoError(t, err)
	require.Equal(t, []string{"TOKEN"}, got.overridden)
	require.Len(t, got.config.Secrets, 2)
	require.Equal(t, "KEEP", got.config.Secrets[0].EnvName)
	require.Equal(t, "token", got.config.Secrets[1].Name)
	require.Equal(t, "TOKEN", got.config.Secrets[1].EnvName)
}

func TestApplyPlatformEnvPlainValueReplacesASecretBinding(t *testing.T) {
	ctx, svc, repo, info := newEnvFixture(t)
	setPlatformEnv(repo, types.DeploymentEnv{Key: "TOKEN", Value: "literal"})
	stub := repo.addStub(t, "stub-code", endpointDeployment, types.StubConfigV1{})
	config := codeConfig()
	config.Secrets = []types.Secret{{Name: "token", EnvName: "TOKEN"}}

	got, err := svc.applyPlatformEnv(ctx, info, stub, &config, "my-api")

	require.NoError(t, err)
	require.Equal(t, []string{"TOKEN=literal"}, got.config.Env)
	require.Empty(t, got.config.Secrets)
	require.Equal(t, []string{"TOKEN"}, got.overridden)
}

// Applying platform env to a config that already carries it changes nothing,
// so a redeploy that applied it before storing its stub deploys that stub.
func TestApplyPlatformEnvIsStable(t *testing.T) {
	ctx, svc, repo, info := newEnvFixture(t)
	setPlatformEnv(repo,
		types.DeploymentEnv{Key: "A_SECRET", SecretName: "db-pass"},
		types.DeploymentEnv{Key: "B_SECRET", SecretName: "token"},
		types.DeploymentEnv{Key: "FOO", Value: "bar"},
	)
	stub := repo.addStub(t, "stub-code", endpointDeployment, types.StubConfigV1{})
	config := codeConfig("CODE=1")
	config.Secrets = []types.Secret{{Name: "token", EnvName: "CODE_SECRET"}}

	first, err := svc.applyPlatformEnv(ctx, info, stub, &config, "my-api")
	require.NoError(t, err)
	require.True(t, first.changed)

	second, err := svc.applyPlatformEnv(ctx, info, stub, &first.config, "my-api")
	require.NoError(t, err)
	require.False(t, second.changed)
	require.Empty(t, second.overridden)
	require.Equal(t, first.config, second.config)
}

func TestApplyPlatformEnvFailsListingEveryMissingSecret(t *testing.T) {
	ctx, svc, repo, info := newEnvFixture(t)
	setPlatformEnv(repo,
		types.DeploymentEnv{Key: "A", SecretName: "gone-a"},
		types.DeploymentEnv{Key: "B", SecretName: "gone-b"},
		types.DeploymentEnv{Key: "FOO", Value: "bar"},
	)
	stub := repo.addStub(t, "stub-code", endpointDeployment, types.StubConfigV1{})
	config := codeConfig()

	_, err := svc.applyPlatformEnv(ctx, info, stub, &config, "my-api")

	require.Error(t, err)
	require.Contains(t, err.Error(), "no longer exist")
	require.Contains(t, err.Error(), `A (secret "gone-a")`)
	require.Contains(t, err.Error(), `B (secret "gone-b")`)
	require.Empty(t, config.Env)
}

func TestApplyPlatformEnvChecksSandboxSecretsAgainstEgress(t *testing.T) {
	ctx, svc, repo, info := newEnvFixture(t)
	repo.secrets["open"] = types.Secret{Name: "open"} // no approved hosts
	setPlatformEnv(repo, types.DeploymentEnv{Key: "OPEN", SecretName: "open"})
	sandbox := repo.addStub(t, "stub-sandbox", types.StubType(types.StubTypeSandbox), types.StubConfigV1{})
	config := codeConfig()

	_, err := svc.applyPlatformEnv(ctx, info, sandbox, &config, "my-api")

	require.Error(t, err)
	require.Contains(t, err.Error(), "requires allowed hosts")
}

func TestApplyPlatformEnvSkipsManagedEndpoints(t *testing.T) {
	ctx, svc, repo, info := newEnvFixture(t)
	setPlatformEnv(repo, types.DeploymentEnv{Key: "FOO", Value: "bar"})
	managed := repo.addStub(t, "stub-managed", types.StubType(types.StubTypeManagedEndpointDeployment), types.StubConfigV1{})
	config := codeConfig()

	got, err := svc.applyPlatformEnv(ctx, info, managed, &config, "my-api")

	require.NoError(t, err)
	require.False(t, got.changed)
}

// An app deleted and created again under the same name starts without the
// old app's platform env, secret bindings included.
func TestApplyPlatformEnvDropsEnvLeftByADeletedApp(t *testing.T) {
	ctx, svc, repo, info := newEnvFixture(t)
	setPlatformEnv(repo, types.DeploymentEnv{Key: "DB_PASSWORD", SecretName: "db-pass"})
	repo.deployments[0].DeletedAt = types.NullTime{Valid: true}
	stub := repo.addStub(t, "stub-code", endpointDeployment, types.StubConfigV1{})
	config := codeConfig("CODE=2")

	got, err := svc.applyPlatformEnv(ctx, info, stub, &config, "my-api")

	require.NoError(t, err)
	require.False(t, got.changed)
	require.Equal(t, []string{"CODE=2"}, got.config.Env)
	require.Empty(t, repo.env["my-api"])
	require.Equal(t, []string{"my-api"}, repo.deletedEnv)
}

func TestDeployStubKeepsPlatformEnvOnACodePush(t *testing.T) {
	ctx, svc, repo, _ := newEnvFixture(t)
	setPlatformEnv(repo, types.DeploymentEnv{Key: "DB_PASSWORD", SecretName: "db-pass"}, types.DeploymentEnv{Key: "FOO", Value: "bar"})
	repo.addStub(t, "stub-code", endpointDeployment, codeConfig("CODE=2"))

	resp, err := svc.DeployStub(ctx, &pb.DeployStubRequest{StubId: "stub-code", Name: "my-api"})

	require.NoError(t, err)
	require.True(t, resp.Ok, resp.ErrMsg)
	require.Empty(t, resp.WarnMsg)
	require.Len(t, repo.createdStubs, 1)
	deployed := repo.deployedConfig(t)
	require.Equal(t, []string{"CODE=2", "FOO=bar"}, deployed.Env)
	require.Len(t, deployed.Secrets, 1)
	require.Equal(t, "DB_PASSWORD", deployed.Secrets[0].EnvName)
	require.NotEqual(t, repo.stubs["stub-code"].Id, repo.createdDeployments[0].StubId)
}

func TestDeployStubWarnsWhenPlatformEnvOverridesCode(t *testing.T) {
	ctx, svc, repo, _ := newEnvFixture(t)
	setPlatformEnv(repo, types.DeploymentEnv{Key: "CODE", Value: "platform"})
	repo.addStub(t, "stub-code", endpointDeployment, codeConfig("CODE=2"))

	resp, err := svc.DeployStub(ctx, &pb.DeployStubRequest{StubId: "stub-code", Name: "my-api"})

	require.NoError(t, err)
	require.True(t, resp.Ok, resp.ErrMsg)
	require.Contains(t, resp.WarnMsg, "overrides the value in code for: CODE")
	require.Equal(t, []string{"CODE=platform"}, repo.deployedConfig(t).Env)
}

func TestDeployStubWithoutPlatformEnvDeploysTheStubAsIs(t *testing.T) {
	ctx, svc, repo, _ := newEnvFixture(t)
	code := repo.addStub(t, "stub-code", endpointDeployment, codeConfig("CODE=2"))

	resp, err := svc.DeployStub(ctx, &pb.DeployStubRequest{StubId: "stub-code", Name: "my-api"})

	require.NoError(t, err)
	require.True(t, resp.Ok, resp.ErrMsg)
	require.Empty(t, repo.createdStubs)
	require.Equal(t, code.Id, repo.createdDeployments[0].StubId)
}

func TestDeployStubFailsWhenABoundSecretIsGone(t *testing.T) {
	ctx, svc, repo, _ := newEnvFixture(t)
	setPlatformEnv(repo, types.DeploymentEnv{Key: "DB_PASSWORD", SecretName: "db-pass"})
	delete(repo.secrets, "db-pass")
	repo.addStub(t, "stub-code", endpointDeployment, codeConfig())

	resp, err := svc.DeployStub(ctx, &pb.DeployStubRequest{StubId: "stub-code", Name: "my-api"})

	require.NoError(t, err)
	require.False(t, resp.Ok)
	require.Contains(t, resp.ErrMsg, "DB_PASSWORD")
	require.Empty(t, repo.createdStubs)
	require.Empty(t, repo.createdDeployments)
}

// The stub that carries platform env is stored only after the rollout plan
// passes, so a rejected rollout leaves no stub row behind.
func TestDeployStubStoresNoStubWhenTheRolloutIsRejected(t *testing.T) {
	ctx, svc, repo, _ := newEnvFixture(t)
	setPlatformEnv(repo, types.DeploymentEnv{Key: "FOO", Value: "bar"})
	repo.addStub(t, "stub-code", endpointDeployment, codeConfig())

	resp, err := svc.DeployStub(ctx, &pb.DeployStubRequest{StubId: "stub-code", Name: "my-api", Rollout: "sideways"})

	require.NoError(t, err)
	require.False(t, resp.Ok)
	require.Contains(t, resp.ErrMsg, "unsupported rollout mode")
	require.Empty(t, repo.createdStubs)
	require.Empty(t, repo.createdDeployments)
}

// A writable disk makes the rollout stop the running version first; when the
// stub cannot be stored, that version must still be running.
func TestDeployStubStopsNothingWhenThePlatformEnvStubCannotBeStored(t *testing.T) {
	ctx, svc, repo, _ := newEnvFixture(t)
	setPlatformEnv(repo, types.DeploymentEnv{Key: "FOO", Value: "bar"})
	withDisk := codeConfig()
	withDisk.Disks = []*pb.DurableDisk{{}}
	repo.addStub(t, "stub-code", endpointDeployment, withDisk)
	repo.createStubErr = errors.New("db down")

	resp, err := svc.DeployStub(ctx, &pb.DeployStubRequest{StubId: "stub-code", Name: "my-api"})

	require.NoError(t, err)
	require.False(t, resp.Ok)
	require.Contains(t, resp.ErrMsg, "create stub with platform env")
	require.Empty(t, repo.updatedDeployments)
	require.Empty(t, repo.createdDeployments)
}

// The bug this design fixes: env set with set_env used to be lost on the next
// code push, because the push deployed a stub built from code alone.
func TestSetDeploymentEnvSurvivesACodePush(t *testing.T) {
	ctx, svc, repo, info := newEnvFixture(t)

	resp, err := svc.SetDeploymentEnv(ctx, info, "my-api", map[string]string{"FOO": "bar", "DB_PASSWORD": "${{secret.db-pass}}"}, nil)
	require.NoError(t, err)
	require.True(t, resp.Ok, resp.ErrMsg)
	require.Equal(t, []types.DeploymentEnv{{Key: "DB_PASSWORD", SecretName: "db-pass"}, {Key: "FOO", Value: "bar"}}, repo.env["my-api"])
	afterSet := repo.deployedConfig(t)
	require.Equal(t, []string{"CODE=1", "FOO=bar"}, afterSet.Env)
	require.Len(t, afterSet.Secrets, 1)
	// The redeploy stored one stub, already carrying the platform env.
	require.Len(t, repo.createdStubs, 1)

	repo.addStub(t, "stub-code", endpointDeployment, codeConfig("CODE=2"))
	resp, err = svc.DeployStub(ctx, &pb.DeployStubRequest{StubId: "stub-code", Name: "my-api"})
	require.NoError(t, err)
	require.True(t, resp.Ok, resp.ErrMsg)
	afterPush := repo.deployedConfig(t)
	require.Equal(t, []string{"CODE=2", "FOO=bar"}, afterPush.Env)
	require.Len(t, afterPush.Secrets, 1)
	require.Equal(t, "DB_PASSWORD", afterPush.Secrets[0].EnvName)
}

// Unset removes a variable from the platform env and from the new version; a
// variable the code defines comes back with the next code deploy.
func TestSetDeploymentEnvUnsetRemovesTheVariable(t *testing.T) {
	ctx, svc, repo, info := newEnvFixture(t)
	_, err := svc.SetDeploymentEnv(ctx, info, "my-api", map[string]string{"FOO": "bar"}, nil)
	require.NoError(t, err)

	resp, err := svc.SetDeploymentEnv(ctx, info, "my-api", nil, []string{"FOO", "CODE"})
	require.NoError(t, err)
	require.True(t, resp.Ok, resp.ErrMsg)
	require.Empty(t, repo.env["my-api"])
	require.Empty(t, repo.deployedConfig(t).Env)

	repo.addStub(t, "stub-code", endpointDeployment, codeConfig("CODE=2"))
	resp, err = svc.DeployStub(ctx, &pb.DeployStubRequest{StubId: "stub-code", Name: "my-api"})
	require.NoError(t, err)
	require.True(t, resp.Ok, resp.ErrMsg)
	require.Equal(t, []string{"CODE=2"}, repo.deployedConfig(t).Env)
}

func TestSetDeploymentEnvReplacesASecretBindingWithAValue(t *testing.T) {
	ctx, svc, repo, info := newEnvFixture(t)
	_, err := svc.SetDeploymentEnv(ctx, info, "my-api", map[string]string{"TOKEN": "${{secret.token}}"}, nil)
	require.NoError(t, err)

	resp, err := svc.SetDeploymentEnv(ctx, info, "my-api", map[string]string{"TOKEN": "literal"}, nil)

	require.NoError(t, err)
	require.True(t, resp.Ok, resp.ErrMsg)
	require.Equal(t, []types.DeploymentEnv{{Key: "TOKEN", Value: "literal"}}, repo.env["my-api"])
	deployed := repo.deployedConfig(t)
	require.Equal(t, []string{"CODE=1", "TOKEN=literal"}, deployed.Env)
	require.Empty(t, deployed.Secrets)
}

func TestSetDeploymentEnvStoresNothingWhenASecretIsMissing(t *testing.T) {
	ctx, svc, repo, info := newEnvFixture(t)

	_, err := svc.SetDeploymentEnv(ctx, info, "my-api", map[string]string{"FOO": "bar", "X": "${{secret.missing}}"}, nil)

	require.Error(t, err)
	require.Contains(t, err.Error(), "missing")
	require.Empty(t, repo.env["my-api"])
	require.Empty(t, repo.createdDeployments)
}

func TestSetDeploymentEnvNeedsAnActiveDeployment(t *testing.T) {
	ctx, svc, repo, info := newEnvFixture(t)
	repo.deployments[0].Active = false

	_, err := svc.SetDeploymentEnv(ctx, info, "my-api", map[string]string{"FOO": "bar"}, nil)

	require.Error(t, err)
	require.Empty(t, repo.env["my-api"])
}

// A failed deploy puts the platform env back, so it matches what runs.
func TestSetDeploymentEnvRestoresPlatformEnvWhenTheDeployFails(t *testing.T) {
	ctx, svc, repo, info := newEnvFixture(t)
	before := []types.DeploymentEnv{{Key: "BAZ", Value: "keep"}, {Key: "FOO", Value: "old"}, {Key: "OTHER", Value: "1"}}
	setPlatformEnv(repo, slices.Clone(before)...)
	repo.createDeploymentErr = errors.New("db down")

	resp, err := svc.SetDeploymentEnv(ctx, info, "my-api", map[string]string{"FOO": "new", "BAR": "1"}, []string{"BAZ"})

	require.NoError(t, err)
	require.False(t, resp.Ok)
	require.Equal(t, before, repo.env["my-api"])
}
