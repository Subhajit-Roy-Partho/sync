# Sync

Linux job-management helpers for large simulation batches (SLURM + SQLite + YAML-driven workflows).

## Main Scripts

- `sync.sh`
  Legacy/general job manager (GPU-aware resubmission logic).
- `fold.sh`
  Production workflow manager for staged replica simulations from `main.yaml`.
- `fold2.sh`
  Enhanced workflow manager based on `fold.sh` with replica-level log checks and failure tracking.
- `foldp.sh`
  Parallelized variant of fold workflow submission/status checks.

## Quick Start (fold workflow)

```bash
# initialize DB if needed
./fold2.sh init

# create simulation directory structure and scripts from main.yaml
./fold2.sh start "$PWD"

# submit first stage jobs
./fold2.sh run "$PWD"

# periodic status check (default mode)
./fold2.sh
```

## Commands (`fold.sh` / `fold2.sh`)

- `init`
- `start <path>`
- `run <path>`
- `status <path>`
- `update-progress <path>`
- `view-progress`
- `viewG`
- `view-folders`
- `view-jobs`
- `view-runs`

## Job Status Codes

- `0` not started
- `1` completed successfully
- `2` running or pending
- `3` failed or manual intervention required

## `fold2.sh` Replica-Aware Log Behavior

- Reads per-stage log filename from `main.yaml` using `Procedure -> <Stage> -> Log`.
- On default no-arg execution (`./fold2.sh`), checks each replica log for:
  - success line: `INFO: END OF THE SIMULATION, everything went OK!`
  - error line containing: `ERROR`
- Continues workflow while at least one replica is still viable.
- Marks stage failed only when all replicas for that stage fail.
- Lists all errored replica IDs and log files in summary output.

## Database Files

- `fold.db` is used by `fold.sh`, `fold2.sh`, and `foldp.sh`.
- `test.db` is used by `sync.sh`.

## Additional Documentation

- Developer detail: `INFO.md`
- Change catalog: `CHANGELOG.md`
