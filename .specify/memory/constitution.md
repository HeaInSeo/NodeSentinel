# NodeSentinel Constitution

<!--
  ②-form, authority revision AR-2026-08-17.1: this file does NOT own cross-repo
  invariants. It consumes the task Authority Snapshot and indexes only THIS
  repo's own constraints. SoT for those is the rules themselves (Makefile gates /
  CI), not this prose.

  SPECIAL CASE (verified 2026-08-03): NodeSentinel has NO branch ruleset on
  `main`. Its CI checks RUN on PR/push but do NOT block merge. Therefore every
  repo-local gate below is recorded as PROPOSED (advisory), NOT IMPLEMENTED —
  recording an unenforced gate as enforced would itself violate §1.10.
  Platform tracking: NodeVault#83 (branch-protection / ruleset rollout).
-->

## Cross-repo authority — revision-pinned repository mirror

Cross-repo platform meaning is selected by the external Authority Router. For
`AR-2026-08-17.1` the scoped authority chain is:

- platform invariants: `Platform Spec Wiki — CURRENT / 1. constitution`
- platform structure / responsibility / call direction:
  `Platform Spec Wiki — CURRENT / 2. architecture`
- repository-portable mirror: `HeaInSeo/NodeVault` —
  `docs/PLATFORM_MASTER_DESIGN.md` at the same authority revision

NodeSentinel does **not** treat NodeVault §4 as an independent platform
canonical. A task may consume that repository mirror only when its `Authority
Snapshot` declares `AR-2026-08-17.1`. Missing/mismatched/conflicting snapshots
must stop with `AUTHORITY_CONFLICT`; do not choose a source by timestamp,
filename, or search rank.

## Process discipline (repo-operational — owned by this repo)

- **Deterministic gates are intended to be the guarantee.** The intent is that
  merge is decided by deterministic checks (tests, govulncheck, coverage,
  golangci-lint) and that LLM/agent review stays **advisory**: a passing review
  never merges alone, a failing gate is never overridden.
- **Stated intent, not enforced.** NodeSentinel currently has **NO branch
  ruleset**, so the gates below RUN in CI but do **not** block merge. Until a
  ruleset lands (see NodeVault#83), this discipline is advisory — a convention
  contributors follow, not a mechanism that stops a bad merge.
- **Spec-anchored change**; **test-first** (behavioral changes ship with tests
  that fail before / pass after; CI runs the `-race` variant); **Builder/Critic
  separation** (read-only Critic pass before merge).
- **Local verify (before a PR):** `make verify` (fmt-check, mod-tidy-check,
  lint-config, lint, build, vet, test-unit, test-k8s, coverage-check).
- **Branch protection**: **NONE yet** — `main` has no required-checks ruleset
  and accepts direct pushes. This is a known governance gap tracked in
  NodeVault#83; closing it is what would promote the gates below to IMPLEMENTED.

## Repo-local constraints (derived index — NOT canonical)

> Derived index of THIS repo's own gates. Not canonical — SoT is the gate
> itself. All are **PROPOSED**: they run via `make …` / CI but are **not
> merge-enforced** because this repo has no ruleset.

- **golangci-lint** (PROPOSED — `make lint` / `make lint-config`, not
  merge-enforced, no ruleset): lint gate.
- **govulncheck** (PROPOSED — `make vuln`, not merge-enforced, no ruleset):
  vulnerability scan. (NodeSentinel has **no `gosec` gate**; govulncheck is the
  security scanner present in this repo.)
- **unit tests** (PROPOSED — `make test-unit`, `-race -shuffle`, not
  merge-enforced, no ruleset): concurrency-safe unit tests.
- **k8s contract tests** (PROPOSED — `make test-k8s`, not merge-enforced, no
  ruleset): bori-facing data-plane manifest contract tests.
- **coverage** (PROPOSED — `make coverage-check`, threshold 70%, not
  merge-enforced, no ruleset): coverage-threshold gate.
- **build / vet / mod-tidy** (PROPOSED — `make build`, `make vet`,
  `make mod-tidy-check`, not merge-enforced, no ruleset): build integrity.

## §1.10 — "do not record what you did not observe"

**Authority: CURRENT platform invariant under `AR-2026-08-17.1`. Enforcement in
this repo: PROPOSED.** NodeSentinel has **no deterministic rule** enforcing this
invariant today, and — because this repo has no ruleset — could not
merge-enforce such a rule yet. The platform invariant's authority status and
this repo's local enforcement status are separate axes.

**Version**: 2.0.0 | **Ratified**: 2026-08-03 | **Last Amended**: 2026-08-17
