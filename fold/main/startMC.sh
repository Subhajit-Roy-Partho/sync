#!/bin/sh
#SBATCH -q private
#SBATCH -p general
#SBATCH -t 7-00:00
#SBATCH -c 1
#SBATCH -o empty.out
#SBATCH -e empty.err
#SBATCH -J jobName

module load cuda-12.1.1-gcc-12.1.0 gcc-12.1.0-gcc-11.2.0 cmake eigen-3.4.0-gcc-11.2.0

/scratch/sroy85/Github/oxOriginal/build/bin/oxDNA inputMC
