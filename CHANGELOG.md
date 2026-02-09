# Changelog

## 2026-02-09

### Added

- Added `fold2.sh` as a non-destructive copy-based enhancement of `fold.sh` (original production script left untouched).
- Added replica-level log-state persistence with new `replica_status` table (created idempotently by `fold2.sh`).
- Added YAML stage log resolution for `Procedure -> <Stage> -> Log`.
- Added replica scanner logic that classifies each replica as:
  - `success`
  - `error`
  - `active`
  - `unknown`

### Changed

- Updated status update behavior in `fold2.sh` (default no-arg mode):
  - Forces stage completion to `100%` if any replica log contains:
    `INFO: END OF THE SIMULATION, everything went OK!`
  - Continues workflow with partial replica failures.
  - Marks a stage as failed only when all replicas for that stage have `ERROR`.
  - Prints explicit summary listing replica IDs and log files requiring manual intervention.
- Added/expanded status summary counters in `fold2.sh`:
  - log-forced completions
  - all-replica failures
  - total replica `ERROR` matches

### Documentation

- Updated `README.md` with current script responsibilities, quick start, and `fold2.sh` behavior.
- Added `INFO.md` with detailed developer documentation (architecture, schema, workflows, and operational details).
