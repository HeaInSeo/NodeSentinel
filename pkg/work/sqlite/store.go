package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	// Register the sqlite3 database driver used by sql.Open.
	_ "github.com/mattn/go-sqlite3"

	"github.com/HeaInSeo/NodeSentinel/pkg/work"
)

type Store struct {
	db *sql.DB
}

func New(path string) (*Store, error) {
	// _txlock=immediate makes every transaction opened on this DB (via
	// Begin/BeginTx) issue "BEGIN IMMEDIATE" instead of SQLite's default
	// deferred BEGIN. Deferred transactions take no lock until their first
	// write, so two connections can both pass a read-then-decide check (see
	// migrateValidationRequestID) before either one blocks — immediate
	// transactions take the write lock up front, serializing exactly that
	// race instead of leaving it to be resolved mid-transaction.
	dsn := fmt.Sprintf("file:%s?_busy_timeout=5000&_foreign_keys=on&_txlock=immediate", path)
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}

	store := &Store{db: db}
	if err := store.initSchema(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) initSchema(ctx context.Context) error {
	const schema = `
CREATE TABLE IF NOT EXISTS jobs (
  job_id TEXT PRIMARY KEY,
  artifact_kind TEXT NOT NULL,
  image_repository TEXT NOT NULL,
  image_digest TEXT NOT NULL,
  stable_ref TEXT NOT NULL,
  tool_name TEXT NOT NULL,
  version TEXT NOT NULL,
  cas_hash TEXT NOT NULL,
  requested_actions TEXT NOT NULL,
  requested_fixture_set TEXT NOT NULL,
  validation_request_id TEXT,
  request_fingerprint TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL,
  attempt INTEGER NOT NULL DEFAULT 0,
  lease_owner TEXT NOT NULL DEFAULT '',
  lease_until TEXT,
  last_error TEXT NOT NULL DEFAULT '',
  result_summary TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_jobs_status_updated_at ON jobs(status, updated_at);
CREATE INDEX IF NOT EXISTS idx_jobs_lease_until ON jobs(lease_until);
`
	if _, err := s.db.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("init schema: %w", err)
	}
	if err := s.migrateValidationRequestID(ctx); err != nil {
		return fmt.Errorf("migrate validation_request_id: %w", err)
	}
	return nil
}

// migrateValidationRequestID adds the validation_request_id/request_fingerprint
// columns and their enforcing index to a jobs table created before this
// migration existed. CREATE TABLE IF NOT EXISTS above already includes these
// columns for a fresh database, so on a fresh DB every step here is a no-op;
// this only does real work against a pre-existing jobs table.
//
// The column is nullable and the index is partial (WHERE ... <> ”) rather
// than "NOT NULL DEFAULT ” + UNIQUE": every pre-existing row would migrate
// to the same empty-string value, and a plain UNIQUE index would then fail
// to build the moment there is more than one existing job.
// migrateValidationRequestID runs its hasColumn checks and ALTER TABLE
// statements inside one transaction, rather than as separate autocommit
// statements against s.db. With New's _txlock=immediate DSN option, BeginTx
// takes SQLite's write lock up front: a second process/connection racing to
// migrate the same pre-existing database blocks on BeginTx until the first
// migration commits, so by the time it proceeds its hasColumn checks
// correctly observe the already-added columns — closing the check-then-act
// race a plain "SELECT, then maybe ALTER" sequence would otherwise have
// across two separate connections (each individual statement is atomic, but
// the pair isn't, without a shared lock spanning both).
func (s *Store) migrateValidationRequestID(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin migration tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }() // no-op after a successful Commit

	hasIDCol, err := hasColumn(ctx, tx, "jobs", "validation_request_id")
	if err != nil {
		return err
	}
	if !hasIDCol {
		if _, err := tx.ExecContext(ctx, `ALTER TABLE jobs ADD COLUMN validation_request_id TEXT`); err != nil {
			return fmt.Errorf("add validation_request_id column: %w", err)
		}
	}

	hasFPCol, err := hasColumn(ctx, tx, "jobs", "request_fingerprint")
	if err != nil {
		return err
	}
	if !hasFPCol {
		if _, err := tx.ExecContext(
			ctx, `ALTER TABLE jobs ADD COLUMN request_fingerprint TEXT NOT NULL DEFAULT ''`,
		); err != nil {
			return fmt.Errorf("add request_fingerprint column: %w", err)
		}
	}

	const idx = `
CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_validation_request_id
ON jobs(validation_request_id)
WHERE validation_request_id IS NOT NULL AND validation_request_id <> ''
`
	if _, err := tx.ExecContext(ctx, idx); err != nil {
		return fmt.Errorf("create validation_request_id index: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit migration tx: %w", err)
	}
	return nil
}

// queryer is the subset of *sql.DB / *sql.Tx that hasColumn needs — letting
// migrateValidationRequestID run it inside a transaction instead of as a
// separate autocommit statement against the DB handle.
type queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// hasColumn reports whether table already has the given column, so
// migrations can skip an ALTER TABLE ADD COLUMN that would otherwise fail
// with "duplicate column name" on a database that already has it (SQLite
// has no ADD COLUMN IF NOT EXISTS). table is always an internal literal —
// never external input — since PRAGMA does not accept bound parameters.
func hasColumn(ctx context.Context, q queryer, table, column string) (bool, error) {
	rows, err := q.QueryContext(ctx, fmt.Sprintf("PRAGMA table_info(%s)", table))
	if err != nil {
		return false, fmt.Errorf("pragma table_info(%s): %w", table, err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var (
			cid       int
			name      string
			colType   string
			notNull   int
			dfltValue sql.NullString
			pk        int
		)
		if err := rows.Scan(&cid, &name, &colType, &notNull, &dfltValue, &pk); err != nil {
			return false, fmt.Errorf("scan table_info row: %w", err)
		}
		if name == column {
			return true, nil
		}
	}
	return false, rows.Err()
}

// requestFingerprint canonicalizes the identity-relevant fields of a
// JobRequest so CreateJob can tell "same logical request, retried" (fields
// match) apart from "validation_request_id reused for a different request"
// (fields differ) — see ErrValidationRequestConflict. RequestedActions is
// sorted first since it's semantically a set, not an ordered list, and each
// action is hashed as its own field (not joined into one comma-separated
// string) so an action value that happens to contain a comma can't make two
// different action sets collide on the same fingerprint.
func requestFingerprint(req work.JobRequest) string {
	actions := make([]string, len(req.RequestedActions))
	for i, a := range req.RequestedActions {
		actions[i] = string(a)
	}
	sort.Strings(actions)

	h := sha256.New()
	fields := make([]string, 0, 7+len(actions))
	fields = append(fields,
		req.ArtifactKind,
		req.ImageRepository,
		req.ImageDigest,
		req.StableRef,
		req.ToolName,
		req.Version,
		req.CasHash,
	)
	fields = append(fields, actions...)
	fields = append(fields, req.RequestedFixtureSet)
	for _, field := range fields {
		h.Write([]byte(field))
		h.Write([]byte{0}) // separator — prevents field-concatenation collisions
	}
	return hex.EncodeToString(h.Sum(nil))
}

// CreateJob is idempotent on req.ValidationRequestID when set: a fresh
// (job_id, validation_request_id) pair is inserted normally, but a
// validation_request_id that already owns a job is NOT re-inserted —
// instead CreateJob returns the existing job when the request fingerprint
// matches (safe retry), or ErrValidationRequestConflict when it doesn't
// (validation_request_id reused for a different logical request).
//
// The pre-INSERT existence check below is only a fast path for the common
// case; concurrent callers racing on the same validation_request_id are
// resolved by the partial UNIQUE index via ON CONFLICT ... DO NOTHING, not
// by this check — see migrateValidationRequestID.
func (s *Store) CreateJob(ctx context.Context, req work.JobRequest) (*work.Job, error) {
	actions, err := json.Marshal(req.RequestedActions)
	if err != nil {
		return nil, fmt.Errorf("marshal actions: %w", err)
	}
	fingerprint := requestFingerprint(req)

	var validationRequestID any
	if req.ValidationRequestID != "" {
		validationRequestID = req.ValidationRequestID
	}

	now := time.Now().UTC()
	const insert = `
INSERT INTO jobs (
  job_id, artifact_kind, image_repository, image_digest, stable_ref, tool_name,
  version, cas_hash, requested_actions, requested_fixture_set,
  validation_request_id, request_fingerprint, status, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(validation_request_id) WHERE validation_request_id IS NOT NULL AND validation_request_id <> '' DO NOTHING
`
	res, err := s.db.ExecContext(
		ctx,
		insert,
		req.JobID,
		req.ArtifactKind,
		req.ImageRepository,
		req.ImageDigest,
		req.StableRef,
		req.ToolName,
		req.Version,
		req.CasHash,
		string(actions),
		req.RequestedFixtureSet,
		validationRequestID,
		fingerprint,
		work.StatusQueued,
		now.Format(time.RFC3339Nano),
		now.Format(time.RFC3339Nano),
	)
	if err != nil {
		return nil, fmt.Errorf("create job: %w", err)
	}

	if validationRequestID == nil {
		// No idempotency key supplied — the partial index never covers this
		// row, so the INSERT above always succeeds; nothing to reconcile.
		return s.GetJob(ctx, req.JobID)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("rows affected: %w", err)
	}
	if rows == 1 {
		return s.GetJob(ctx, req.JobID) // this call's INSERT won the race
	}

	// Conflict: validation_request_id already belongs to another job.
	existingJobID, existingFingerprint, err := s.jobByValidationRequestID(ctx, req.ValidationRequestID)
	if err != nil {
		return nil, fmt.Errorf("lookup existing job for validation_request_id %q: %w", req.ValidationRequestID, err)
	}
	if existingFingerprint != fingerprint {
		return nil, work.ErrValidationRequestConflict
	}
	return s.GetJob(ctx, existingJobID)
}

// jobByValidationRequestID returns the job_id and request_fingerprint of the
// job already owning validationRequestID.
func (s *Store) jobByValidationRequestID(ctx context.Context, validationRequestID string) (jobID, fingerprint string, err error) {
	const query = `SELECT job_id, request_fingerprint FROM jobs WHERE validation_request_id = ?`
	if scanErr := s.db.QueryRowContext(ctx, query, validationRequestID).Scan(&jobID, &fingerprint); scanErr != nil {
		return "", "", fmt.Errorf("select job by validation_request_id: %w", scanErr)
	}
	return jobID, fingerprint, nil
}

func (s *Store) LeaseJob(ctx context.Context, worker string, ttl time.Duration) (*work.Job, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin lease tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	now := time.Now().UTC()
	const selectSQL = `
SELECT job_id FROM jobs
WHERE status = ? OR (status = ? AND (lease_until IS NULL OR lease_until < ?))
ORDER BY created_at ASC
LIMIT 1
`
	var jobID string
	err = tx.QueryRowContext(
		ctx,
		selectSQL,
		work.StatusQueued,
		work.StatusLeased,
		now.Format(time.RFC3339Nano),
	).Scan(&jobID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, work.ErrNoAvailableJob
		}
		return nil, fmt.Errorf("select leaseable job: %w", err)
	}

	leaseUntil := now.Add(ttl).UTC()
	const updateSQL = `
UPDATE jobs
SET status = ?, attempt = attempt + 1, lease_owner = ?, lease_until = ?, updated_at = ?
WHERE job_id = ?
`
	_, err = tx.ExecContext(
		ctx,
		updateSQL,
		work.StatusLeased,
		worker,
		leaseUntil.Format(time.RFC3339Nano),
		now.Format(time.RFC3339Nano),
		jobID,
	)
	if err != nil {
		return nil, fmt.Errorf("update leased job: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit lease tx: %w", err)
	}
	return s.GetJob(ctx, jobID)
}

func (s *Store) Heartbeat(ctx context.Context, jobID, worker string, ttl time.Duration) error {
	now := time.Now().UTC()
	leaseUntil := now.Add(ttl).UTC().Format(time.RFC3339Nano)
	const updateSQL = `
UPDATE jobs
SET status = ?, lease_until = ?, updated_at = ?
WHERE job_id = ? AND lease_owner = ? AND status IN (?, ?)
`
	res, err := s.db.ExecContext(
		ctx,
		updateSQL,
		work.StatusRunning,
		leaseUntil,
		now.Format(time.RFC3339Nano),
		jobID,
		worker,
		work.StatusLeased,
		work.StatusRunning,
	)
	if err != nil {
		return fmt.Errorf("heartbeat: %w", err)
	}
	return ensureAffected(res)
}

func (s *Store) CompleteJob(ctx context.Context, jobID, worker, resultSummary string) error {
	return s.finishJob(ctx, jobID, worker, work.StatusSucceeded, "", resultSummary)
}

func (s *Store) FailJob(ctx context.Context, jobID, worker, lastError string, retryable bool) error {
	if retryable {
		now := time.Now().UTC()
		const retrySQL = `
UPDATE jobs
SET status = ?, lease_owner = '', lease_until = NULL, last_error = ?, updated_at = ?
WHERE job_id = ? AND lease_owner = ?
`
		res, err := s.db.ExecContext(
			ctx,
			retrySQL,
			work.StatusQueued,
			lastError,
			now.Format(time.RFC3339Nano),
			jobID,
			worker,
		)
		if err != nil {
			return fmt.Errorf("retryable fail: %w", err)
		}
		return ensureAffected(res)
	}
	return s.finishJob(ctx, jobID, worker, work.StatusFailed, lastError, "")
}

func (s *Store) finishJob(
	ctx context.Context, jobID, worker string, status work.Status, lastError, resultSummary string,
) error {
	now := time.Now().UTC()
	const updateSQL = `
UPDATE jobs
SET status = ?, lease_owner = '', lease_until = NULL, last_error = ?, result_summary = ?, updated_at = ?
WHERE job_id = ? AND lease_owner = ?
`
	res, err := s.db.ExecContext(
		ctx,
		updateSQL,
		status,
		lastError,
		resultSummary,
		now.Format(time.RFC3339Nano),
		jobID,
		worker,
	)
	if err != nil {
		return fmt.Errorf("finish job: %w", err)
	}
	return ensureAffected(res)
}

func (s *Store) GetJob(ctx context.Context, jobID string) (*work.Job, error) {
	const query = `
SELECT job_id, artifact_kind, image_repository, image_digest, stable_ref, tool_name,
       version, cas_hash, requested_actions, requested_fixture_set, validation_request_id,
       status, attempt, lease_owner, lease_until, last_error, result_summary, created_at, updated_at
FROM jobs
WHERE job_id = ?
`
	row := s.db.QueryRowContext(ctx, query, jobID)
	job, err := scanJob(row.Scan)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, work.ErrNotFound
		}
		return nil, fmt.Errorf("get job: %w", err)
	}
	return job, nil
}

func (s *Store) ListJobs(ctx context.Context, status work.Status) ([]*work.Job, error) {
	query := `
SELECT job_id, artifact_kind, image_repository, image_digest, stable_ref, tool_name,
       version, cas_hash, requested_actions, requested_fixture_set, validation_request_id,
       status, attempt, lease_owner, lease_until, last_error, result_summary, created_at, updated_at
FROM jobs
`
	args := []any{}
	if status != "" {
		query += " WHERE status = ?"
		args = append(args, status)
	}
	query += " ORDER BY created_at ASC"

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []*work.Job
	for rows.Next() {
		job, scanErr := scanJob(rows.Scan)
		if scanErr != nil {
			return nil, fmt.Errorf("scan listed job: %w", scanErr)
		}
		out = append(out, job)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate jobs: %w", err)
	}
	return out, nil
}

type scanner func(dest ...any) error

func scanJob(scan scanner) (*work.Job, error) {
	var (
		actionsJSON         string
		validationRequestID sql.NullString
		status              string
		leaseOwner          string
		leaseUntil          sql.NullString
		lastError           string
		result              string
		createdAt           string
		updatedAt           string
		job                 work.Job
	)

	err := scan(
		&job.JobID,
		&job.ArtifactKind,
		&job.ImageRepository,
		&job.ImageDigest,
		&job.StableRef,
		&job.ToolName,
		&job.Version,
		&job.CasHash,
		&actionsJSON,
		&job.RequestedFixtureSet,
		&validationRequestID,
		&status,
		&job.Attempt,
		&leaseOwner,
		&leaseUntil,
		&lastError,
		&result,
		&createdAt,
		&updatedAt,
	)
	if err != nil {
		return nil, err
	}
	job.ValidationRequestID = validationRequestID.String

	var actions []work.Action
	if unmarshalErr := json.Unmarshal([]byte(actionsJSON), &actions); unmarshalErr != nil {
		return nil, fmt.Errorf("unmarshal actions: %w", unmarshalErr)
	}

	job.RequestedActions = actions
	job.Status = work.Status(status)
	job.LeaseOwner = leaseOwner
	job.LastError = lastError
	job.ResultSummary = result

	created, err := time.Parse(time.RFC3339Nano, createdAt)
	if err != nil {
		return nil, fmt.Errorf("parse created_at: %w", err)
	}
	updated, err := time.Parse(time.RFC3339Nano, updatedAt)
	if err != nil {
		return nil, fmt.Errorf("parse updated_at: %w", err)
	}
	job.CreatedAt = created
	job.UpdatedAt = updated

	if leaseUntil.Valid && leaseUntil.String != "" {
		ts, parseErr := time.Parse(time.RFC3339Nano, leaseUntil.String)
		if parseErr != nil {
			return nil, fmt.Errorf("parse lease_until: %w", parseErr)
		}
		job.LeaseUntil = &ts
	}

	return &job, nil
}

func ensureAffected(res sql.Result) error {
	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("rows affected: %w", err)
	}
	if rows == 0 {
		return work.ErrNotFound
	}
	return nil
}
