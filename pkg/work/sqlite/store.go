package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"github.com/HeaInSeo/NodeSentinel/pkg/work"
)

type Store struct {
	db *sql.DB
}

func New(path string) (*Store, error) {
	dsn := fmt.Sprintf("file:%s?_busy_timeout=5000&_foreign_keys=on", path)
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}

	store := &Store{db: db}
	if err := store.initSchema(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) initSchema() error {
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
	_, err := s.db.Exec(schema)
	if err != nil {
		return fmt.Errorf("init schema: %w", err)
	}
	return nil
}

func (s *Store) CreateJob(ctx context.Context, req work.JobRequest) (*work.Job, error) {
	actions, err := json.Marshal(req.RequestedActions)
	if err != nil {
		return nil, fmt.Errorf("marshal actions: %w", err)
	}

	now := time.Now().UTC()
	const insert = `
INSERT INTO jobs (
  job_id, artifact_kind, image_repository, image_digest, stable_ref, tool_name,
  version, cas_hash, requested_actions, requested_fixture_set, status,
  created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`
	_, err = s.db.ExecContext(
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
		work.StatusQueued,
		now.Format(time.RFC3339Nano),
		now.Format(time.RFC3339Nano),
	)
	if err != nil {
		return nil, fmt.Errorf("create job: %w", err)
	}

	return s.GetJob(ctx, req.JobID)
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
       version, cas_hash, requested_actions, requested_fixture_set, status, attempt,
       lease_owner, lease_until, last_error, result_summary, created_at, updated_at
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
       version, cas_hash, requested_actions, requested_fixture_set, status, attempt,
       lease_owner, lease_until, last_error, result_summary, created_at, updated_at
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
		actionsJSON string
		status      string
		leaseOwner  string
		leaseUntil  sql.NullString
		lastError   string
		result      string
		createdAt   string
		updatedAt   string
		job         work.Job
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
