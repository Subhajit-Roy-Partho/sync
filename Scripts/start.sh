#!/bin/sh
#SBATCH -q private
#SBATCH -p general
#SBATCH -t 7-00:00
#SBATCH -c numCPUs
#SBATCH -G 
#SBATCH -o empty.out
#SBATCH -e empty.err
#SBATCH -J jobName

module load cuda-12.1.1-gcc-12.1.0 gcc-12.1.0-gcc-11.2.0 cmake eigen-3.4.0-gcc-11.2.0 amber/22v3

export CUDA_MPS_PIPE_DIRECTORY=/tmp/mps-pipe_$SLURM_TASK_PID
export CUDA_MPS_LOG_DIRECTORY=/tmp/mps-log_$SLURM_TASK_PID
mkdir -p $CUDA_MPS_PIPE_DIRECTORY
mkdir -p $CUDA_MPS_LOG_DIRECTORY
nvidia-cuda-mps-control -d

echo "Starting jobs"
bash script.sh
echo "Done"
exec screen -Dm -S slurm$SLURM_JOB_ID
