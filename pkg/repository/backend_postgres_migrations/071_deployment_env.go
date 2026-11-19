package backend_postgres_migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() { goose.AddMigrationContext(upDeploymentEnv, downDeploymentEnv) }

// Env set on a deployment through the platform (set_env, connect_services)
// rather than in its code, by deployment name, so every version deployed
// under the name gets it, code pushes included. A row binds a workspace
// secret when secret_name is set, and sets value otherwise.
func upDeploymentEnv(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `
CREATE TABLE deployment_env (
 workspace_id BIGINT NOT NULL REFERENCES workspace(id),
 deployment_name TEXT NOT NULL,
 key TEXT NOT NULL,
 value TEXT NOT NULL DEFAULT '',
 secret_name TEXT NOT NULL DEFAULT '',
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 PRIMARY KEY (workspace_id, deployment_name, key)
);`)
	return err
}

func downDeploymentEnv(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `DROP TABLE deployment_env;`)
	return err
}
