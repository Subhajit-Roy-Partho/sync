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


#### GPU Jobs

To automatically select the best gpu please write in the following format `#SBATCH -G` nothing after G.

The gpus will be updated for a job only if the following format is present
```bash
#SBATCH -G 1
#SBATCH -w scg025
```
-w directly below -G. If you don't want the script to mistakenly consider a static configuration for dynamic gpu allocation, please don't write the two one after another.