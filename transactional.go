package outbox

import (
	"context"
	"database/sql"
	"fmt"
)

// transactional is a helper function that executes a function within a database transaction.
func transactional(ctx context.Context, db *sql.DB, opts *sql.TxOptions, fn func(ctx context.Context, tx *sql.Tx) error) error {
	tx, err := db.BeginTx(ctx, opts)
	if err != nil {
		return err
	}

	if err := fn(ctx, tx); err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			return fmt.Errorf("rollback failed: %w, original error: %w", rollbackErr, err)
		}
		return err
	}

	return tx.Commit()
}
