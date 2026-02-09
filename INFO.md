# INFO.md

Developer reference for the `sync` project.

## 1. Project Scope

This repository contains shell-based job orchestration for simulation workloads, focused on:

- generating staged simulation folder layouts from YAML,
- submitting and monitoring SLURM jobs,
- resubmitting interrupted jobs,
- tracking progress and status in SQLite databases,
- visualizing progress in terminal output.

There are two families of tooling:

- `sync.sh` uses `test.db` and provides generic job/GPU queue management.
- `fold*.sh` scripts use `fold.db` and provide staged replica workflows from `main.yaml`.

## 2. Repository Layout

- `sync.sh`
  Generic job manager with GPU-node preference logic.
- `fold.sh`
  Production staged workflow manager.
- `fold2.sh`
  Enhanced `fold.sh` variant with replica log-aware progression/failure control.
- `foldp.sh`
  Parallelized fold workflow status/submission implementation.
- `fold/main.yaml`
  Example workflow configuration.
- `Scripts/`
  Helper scripts and templates.

## 3. Runtime Dependencies

- `bash`
- `sqlite3`
- `awk`, `sed`, `grep`, `wc`, `bc`
- SLURM CLI tools:
  - `sbatch`
  - `squeue`
  - `scancel`

If SLURM tools are unavailable, state checks/submissions will fail.

## 4. Databases and Schemas

## 4.1 `fold.db` (fold scripts)

Core tables:

- `folders`
  Tracks generated replica folders.
- `jobs`
  Tracks per-stage workflow jobs (`location`, `stage`, `jobid`, `status`, `progress`, `max_steps`).
- `setup_runs`
  Tracks setup invocations for reproducibility.

`fold2.sh` adds:

- `replica_status`
  Per-replica, per-stage log-derived state.

Columns:

- `location`
- `stage`
- `replica_id`
- `log_file`
- `replica_state` (`success`, `error`, `active`, `unknown`)
- `last_match` (`SUCCESS_SENTINEL`, `ERROR`, `ACTIVE`, `MISSING_LOG`)
- `last_checked`

Unique key:

- `(location, stage, replica_id)`

Creation is idempotent via `ensureReplicaStatusTable`.

## 4.2 `test.db` (sync.sh)

Used by `sync.sh` for general job tracking and GPU availability bookkeeping.

## 5. YAML Contract (`fold/main.yaml`)

`fold*.sh` expects a specific structure:

- `Input`
  - `Replicas`
  - `Folder`
  - `Files`
  - `OutputName`
  - `CopyAllFiles`
  - `Executable`
- `Procedure`
  - ordered list of stages (`MC`, `MDrelax`, `MDProd`, etc.)
  - stage properties include:
    - `GPU`
    - `JobsPerGPU`
    - `CPUs`
    - `Memory`
    - `ExecutableInput`
    - `MaxSteps`
    - `Log`
- `Output`
  - `Folder`

`fold2.sh` depends on `Log` for replica-level status checks.

## 6. fold Workflow Lifecycle

Typical command flow:

1. `./fold2.sh init`
2. `./fold2.sh start <path>`
3. `./fold2.sh run <path>`
4. periodic: `./fold2.sh` (no arguments)
5. inspect: `./fold2.sh view-progress` or `./fold2.sh viewG`

### 6.1 `start`

- parses YAML,
- creates output simulation directories,
- creates replica subdirectories (`0`, `1`, `2`, ...),
- copies required files,
- generates `start_<stage>.sh` scripts,
- inserts corresponding stage rows into `jobs`.

### 6.2 `run`

- updates statuses,
- submits first stage for pending simulations.

### 6.3 default no-arg invocation

- runs status update cycle for active jobs in current directory (`main.yaml` required).

## 7. `fold2.sh` Behavior in Detail

## 7.1 Scope of log-aware checks

Log-aware checks are enabled for default no-arg invocation:

- `./fold2.sh`

They are currently disabled for explicit `run` and `status` command paths:

- `./fold2.sh run <path>`
- `./fold2.sh status <path>`

## 7.2 Replica classification rules

For each active job, `fold2.sh`:

1. reads stage log filename from YAML (`Log`),
2. scans each numeric replica directory under `location`,
3. classifies replica state:
   - if log contains `ERROR` -> `error`
   - else if log contains `INFO: END OF THE SIMULATION, everything went OK!` -> `success`
   - else if log exists -> `active`
   - else -> `unknown`

Each replica state is persisted to `replica_status`.

## 7.3 Stage decision policy

- If all replicas are `error`:
  - stage is marked failed (`jobs.status = 3`),
  - no next-stage submission,
  - workflow for that stage stops (manual intervention).
- Else if any replica is `success`:
  - stage progress forced to `100`,
  - current stage is completed and advanced.
- Else:
  - stage progress is computed from non-error replicas only,
  - existing resubmit/continue behavior applies.

This policy allows partial replica failure and continues until all replicas fail.

## 7.4 Progress calculation in `fold2.sh`

When log-aware mode is active:

- `success` replicas contribute full `MaxSteps`.
- `active`/`unknown` replicas contribute their `energy.dat` line count.
- `error` replicas are excluded from denominator and progress contribution.

This avoids stalled progress caused only by already-failed replicas.

## 7.5 Summary output extensions

`fold2.sh` status summary includes:

- total jobs checked,
- running/pending/resubmitted counts,
- stage advances/completions,
- log-forced completions,
- all-replica-failure count,
- total replica `ERROR` matches,
- explicit error list with stage, replica id, location, and log file.

## 8. Status Codes

`jobs.status` semantics in fold workflow:

- `0` not started
- `1` completed
- `2` running or pending
- `3` failed or requires manual intervention

## 9. Script Relationship and Safety

- `fold.sh` is the production baseline and remains unchanged.
- `fold2.sh` is the experimental/extended variant.
- `foldp.sh` introduces parallel processing for status and submission operations.

Any production rollout should validate behavior under real SLURM load before replacing `fold.sh`.

## 10. Known Constraints and Caveats

- YAML parsing is line-pattern based (`grep`/`awk`), not full YAML parsing.
- `Log` field must be present and accurate per stage for log-aware logic.
- Log matching is simple substring search:
  - success sentinel exact substring,
  - `ERROR` substring.
- Failed resubmissions still depend on presence of generated stage scripts (for example `start_<stage>.sh`).

## 11. Suggested Developer Workflow

1. Validate shell syntax:
   - `bash -n fold2.sh`
2. Run in isolated temp directories with synthetic logs before cluster execution.
3. Confirm DB schema and values:
   - `.schema` for `fold.db`
   - queries on `jobs` and `replica_status`
4. Test all critical branches:
   - all replicas fail,
   - mixed success/error,
   - partial active+error with no success,
   - missing log files.
