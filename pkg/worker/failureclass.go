package worker

import (
	"strings"

	"github.com/HeaInSeo/NodeSentinel/pkg/vaultclient"
	"github.com/HeaInSeo/NodeSentinel/pkg/work"
)

// FailureClass is NodeSentinel's internal retry-policy classification for a
// pipeline-stage failure (L3 dry-run, L4 smoke-run, L5-a functional
// validation). It exists purely to drive this process's own retry decision
// (see decideRetry) and its wire mapping onto the existing NodeVault
// CheckRecord schema (see wireStatus) — the class name/value itself is never
// sent over the wire. NodeVault only ever sees the fields
// vaultclient.SubmitCheckRecordRequest already defines (ValidationStatus,
// FailureKind, Retryable, FailureReason); FailureClass is not a new field on
// that struct and must not become one — see wireStatus's doc comment for why.
type FailureClass string

const (
	// FailureClassDeterministic means retrying under unchanged inputs cannot
	// succeed: the tool ran and reported a real application-level failure
	// (non-zero exit code), or the request itself was invalid (unrecognized
	// requested_actions). Never retried.
	FailureClassDeterministic FailureClass = "DETERMINISTIC"
	// FailureClassTransientInfra means the failure looks environment-caused
	// and plausibly won't recur on a fresh attempt: pod scheduling failure,
	// image pull failure, a K8s API call itself erroring, a smoke-run
	// timeout, or a NodeVault submission failure classified retryable by
	// vaultclient.Retryable (network error / 5xx). Retried, bounded by
	// maxAttempts.
	FailureClassTransientInfra FailureClass = "TRANSIENT_INFRA"
	// FailureClassResourceObservation means the tool actually ran (a valid
	// observation was possible) but failed a resource contract under the
	// current resource conditions — today this is exactly OOMKilled. Per
	// this package's design note (see wireStatus), this is NOT retried
	// under the same conditions: nothing about a bare retry changes the
	// memory ceiling that caused the OOM.
	FailureClassResourceObservation FailureClass = "RESOURCE_OBSERVATION"
	// FailureClassUnknown means the observed signal doesn't match any
	// pattern this classifier recognizes (e.g. no pods found to inspect, a
	// Job ended without ever reaching a Complete/Failed condition this code
	// understands). Retried at most once — see decideRetry — specifically
	// so a genuinely novel failure mode gets one automatic retry (transient
	// blips look like this too) without letting an unrecognized-forever
	// signal loop up to the full maxAttempts budget.
	FailureClassUnknown FailureClass = "UNKNOWN"
)

// wireStatus maps class onto the ValidationStatus/FailureKind fields
// already defined on vaultclient.SubmitCheckRecordRequest — the caller
// combines this with the actual RetryDecision.Retry (not repeated here) for
// the Retryable field. See the design note below for why this mapping table
// exists instead of a new wire schema.
//
// Design note — FailureClass -> wire schema (why no new field/status):
//
// docs/OBSERVED_PROFILE_SPEC.md (NodeVault) defines a richer
// "application/vnd.nodevault.toolprofile.v1+json" referrer payload with a
// profileStatus of "observed" (valid execution, contractCheck may still
// fail) vs "inconclusive" (no valid observation was possible) — see its §2.
// That is exactly the distinction FailureClassResourceObservation vs
// FailureClassTransientInfra/Unknown is drawing. But per
// OBSERVED_PROFILE_SPEC.md §6, NodeSentinel does not build or push that
// payload — pkg/oras/referrer.go's PushToolProfileReferrer and the
// observedResourceProfile/contractCheck fields live in NodeVault
// (pkg/validation, Sprint 2+), not here. NodeSentinel only ever sends
// vaultclient.SubmitCheckRecordRequest (POST /v1/validation/check-records).
// So FailureClassResourceObservation maps onto the closest existing
// CheckRecord fields instead of inventing a new "profileStatus"/
// "contractCheck" concept on this wire: ValidationStatus="failed" (a real
// observation happened, matching §2.1's "profile" being present at all) with
// FailureKind=Application and Retryable=false — the same three fields
// already used for a deterministic exit-code failure — rather than
// ValidationStatus="infra_failed" (§2.2's inconclusive case), which would
// wrongly tell NodeVault no valid observation was possible. The
// OOM-specific detail (which §2.1's observedResourceProfile would carry)
// is folded into FailureReason's free text instead — see classify.go's
// OOMKilled case and l5a.go's l5aFailureSubmission — since no numeric peak
// memory is observable today (buildSmokeJobSpec/buildL5aJobSpec set no
// Resources limits — see jobspec.go), only the fact that OOM occurred.
func (c FailureClass) wireStatus() (validationStatus, failureKind string) {
	switch c {
	case FailureClassDeterministic, FailureClassResourceObservation:
		return "failed", vaultclient.FailureKindApplication
	default: // FailureClassTransientInfra, FailureClassUnknown
		// UNKNOWN maps onto the same wire fields as TRANSIENT_INFRA
		// (infra_failed/Infrastructure): an unrecognized signal is treated
		// conservatively as "inconclusive" rather than asserting a
		// definitive application-level judgement NodeSentinel cannot
		// actually back up.
		return "infra_failed", vaultclient.FailureKindInfrastructure
	}
}

// maxAttempts caps the total number of times a job may be leased (see
// work.Job.Attempt, incremented on every LeaseJob call —
// pkg/work/sqlite/store.go) before NodeSentinel stops requesting a retry and
// reports the job Terminal-failed instead, preserving the last failure
// reason (see retryExhaustedReason). A package-level var, not a const, so
// tests can lower it — mirrors the pollFrequency/heartbeatFrequency override
// pattern already used by process_test.go's useFastWorkerTicks.
//
// Before this existed, FailJob (pkg/work/sqlite/store.go) had no retry cap
// at all: a permanently-failing retryable job would requeue itself forever
// and, because LeaseJob selects strictly by "ORDER BY created_at ASC", it
// would always be re-selected ahead of any newer job for as long as it kept
// failing — head-of-line blocking every other queued job behind it. Once
// this job's attempts are exhausted it is instead marked work.StatusFailed
// (a terminal status LeaseJob's WHERE clause never selects), which is what
// actually unblocks the jobs behind it — see
// TestHeadOfLine_PermanentlyFailingJob_DoesNotBlockLaterJobs.
var maxAttempts = 5

// retryExhaustedReason is the LastError/FailureReason text NodeSentinel
// reports — both internally via FailJob and externally via the CheckRecord
// this job's Terminal record carries — when a job has consumed maxAttempts
// without succeeding. Distinct from unknownRetryLimitReason (see
// decideRetry), which fires before maxAttempts is necessarily reached,
// specifically for a second consecutive FailureClassUnknown.
const retryExhaustedReason = "RETRY_EXHAUSTED"

// unknownRetryLimitReason is the LastError/FailureReason text used when an
// UNKNOWN-classified failure recurs immediately after the one retry
// decideRetry already granted it.
const unknownRetryLimitReason = "UNKNOWN_RETRY_LIMIT"

// unknownRetryMarker is stamped onto the LastError text FailJob persists
// when a FailureClassUnknown failure is retried, so the *next* attempt can
// tell "this is a fresh UNKNOWN" apart from "UNKNOWN happened again right
// after its one permitted retry" — without adding a dedicated counter
// column to the jobs table. job.Attempt (already tracked) still governs the
// overall maxAttempts budget; this marker only governs UNKNOWN's own
// tighter one-retry allowance within that budget.
//
// Trade-off, stated explicitly: this marker is overwritten by whatever
// LastError the *next* failure (of any class) writes, so it only detects
// two UNKNOWN classifications in immediate succession, not "this job has
// ever had an UNKNOWN failure at any point in its history." A job that goes
// UNKNOWN -> (retried) -> TRANSIENT_INFRA -> (retried) -> UNKNOWN again gets
// a second one-retry allowance for that second, later UNKNOWN. This is
// intentional: each UNKNOWN classification still only ever grants exactly
// one retry *for that occurrence*, and the total is still hard-capped by
// maxAttempts regardless — this codebase's stated retry budget uses
// job.Attempt as the sole counter, so consecutive-UNKNOWN detection is
// itself derived from existing state (LastError text) rather than new
// storage.
const unknownRetryMarker = "[unknown-retry]"

// RetryDecision is decideRetry's output: whether NodeSentinel's WorkStore
// should requeue the job (Retry) and the LastError/FailureReason text to
// persist either way.
type RetryDecision struct {
	Class  FailureClass
	Retry  bool
	Reason string // persisted via FailJob's lastError and the CheckRecord's FailureReason
}

// decideRetry applies NodeSentinel's bounded retry policy for a job whose
// current attempt (job.Attempt, as leased — see work.Store.LeaseJob) just
// failed with the given class and human-readable reason. job.LastError
// reflects the *previous* attempt's outcome (FailJob's lastError from the
// prior FailJob call, unchanged by leasing) — see unknownRetryMarker.
//
// Policy (see this file's package-level doc comment / the sprint design
// note for the four classes):
//   - DETERMINISTIC, RESOURCE_OBSERVATION: never retried.
//   - UNKNOWN: retried at most once (see unknownRetryMarker), still subject
//     to maxAttempts.
//   - TRANSIENT_INFRA: retried while job.Attempt < maxAttempts.
func decideRetry(class FailureClass, job *work.Job, reason string) RetryDecision {
	switch class {
	case FailureClassDeterministic, FailureClassResourceObservation:
		return RetryDecision{Class: class, Retry: false, Reason: reason}

	case FailureClassUnknown:
		if strings.Contains(job.LastError, unknownRetryMarker) {
			return RetryDecision{
				Class: class, Retry: false,
				Reason: unknownRetryLimitReason + ": UNKNOWN failure recurred immediately after its one permitted retry: " + reason,
			}
		}
		if job.Attempt >= maxAttempts {
			return RetryDecision{Class: class, Retry: false, Reason: retryExhaustedReason + ": " + reason}
		}
		return RetryDecision{Class: class, Retry: true, Reason: reason + " " + unknownRetryMarker}

	default: // FailureClassTransientInfra
		if job.Attempt >= maxAttempts {
			return RetryDecision{Class: class, Retry: false, Reason: retryExhaustedReason + ": " + reason}
		}
		return RetryDecision{Class: class, Retry: true, Reason: reason}
	}
}
