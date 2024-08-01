# Sync

Job management system for Linux to auto resume and data manipulation for a large set of jobs.

#### Defaults

- `start.sh` - default slurm start script.
- `resume.sh` - default slurm resume script

#### Helpful Tricks
- `sync.sh submit "$(pwd)"` to submit a job if currently in the start location.

#### Status
- 0 Active
- 1 Stopped
- 2 In Progress
- 3 Other
