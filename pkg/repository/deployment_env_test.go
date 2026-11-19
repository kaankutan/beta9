package repository

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

func TestListDeploymentEnv(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	now := time.Now()
	mock.ExpectQuery(`SELECT workspace_id, deployment_name, key, value, secret_name, created_at, updated_at\s+FROM deployment_env WHERE workspace_id = \$1 AND deployment_name = \$2 ORDER BY key`).
		WithArgs(uint(1), "my-api").
		WillReturnRows(sqlmock.NewRows([]string{"workspace_id", "deployment_name", "key", "value", "secret_name", "created_at", "updated_at"}).
			AddRow(1, "my-api", "DB_PASSWORD", "", "db-pass", now, now).
			AddRow(1, "my-api", "FOO", "bar", "", now, now))

	env, err := repo.ListDeploymentEnv(context.Background(), 1, "my-api")

	require.NoError(t, err)
	require.Len(t, env, 2)
	require.Equal(t, "DB_PASSWORD", env[0].Key)
	require.Equal(t, "db-pass", env[0].SecretName)
	require.Equal(t, "bar", env[1].Value)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestListDeploymentEnvIsEmptyNotNil(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	mock.ExpectQuery(`FROM deployment_env`).
		WillReturnRows(sqlmock.NewRows([]string{"workspace_id", "deployment_name", "key", "value", "secret_name", "created_at", "updated_at"}))

	env, err := repo.ListDeploymentEnv(context.Background(), 1, "my-api")

	require.NoError(t, err)
	require.NotNil(t, env)
	require.Empty(t, env)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestUpdateDeploymentEnvRemovesThenUpsertsInOneTransaction(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	mock.ExpectBegin()
	mock.ExpectExec(`DELETE FROM deployment_env WHERE workspace_id = \$1 AND deployment_name = \$2 AND key = ANY\(\$3\)`).
		WithArgs(uint(1), "my-api", pq.Array([]string{"OLD"})).
		WillReturnResult(sqlmock.NewResult(0, 1))
	upsert := `INSERT INTO deployment_env \(workspace_id, deployment_name, key, value, secret_name\)\s+VALUES \(\$1, \$2, \$3, \$4, \$5\)\s+ON CONFLICT \(workspace_id, deployment_name, key\)\s+DO UPDATE SET value = EXCLUDED.value, secret_name = EXCLUDED.secret_name, updated_at = NOW\(\)`
	mock.ExpectExec(upsert).WithArgs(uint(1), "my-api", "FOO", "bar", "").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(upsert).WithArgs(uint(1), "my-api", "DB_PASSWORD", "", "db-pass").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	err := repo.UpdateDeploymentEnv(context.Background(), 1, "my-api", []types.DeploymentEnv{
		{Key: "FOO", Value: "bar"},
		{Key: "DB_PASSWORD", SecretName: "db-pass"},
	}, []string{"OLD"})

	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestUpdateDeploymentEnvRollsBackWhenAWriteFails(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO deployment_env`).WithArgs(uint(1), "my-api", "FOO", "bar", "").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO deployment_env`).WithArgs(uint(1), "my-api", "BAR", "1", "").WillReturnError(errors.New("db down"))
	mock.ExpectRollback()

	err := repo.UpdateDeploymentEnv(context.Background(), 1, "my-api", []types.DeploymentEnv{
		{Key: "FOO", Value: "bar"},
		{Key: "BAR", Value: "1"},
	}, nil)

	require.ErrorContains(t, err, "db down")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestDeleteDeploymentEnv(t *testing.T) {
	repo, mock := NewBackendPostgresRepositoryForTest()
	mock.ExpectExec(`DELETE FROM deployment_env WHERE workspace_id = \$1 AND deployment_name = \$2`).
		WithArgs(uint(1), "my-api").
		WillReturnResult(sqlmock.NewResult(0, 2))

	require.NoError(t, repo.DeleteDeploymentEnv(context.Background(), 1, "my-api"))
	require.NoError(t, mock.ExpectationsWereMet())
}
