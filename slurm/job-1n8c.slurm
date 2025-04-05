#!/bin/bash

#SBATCH --nodes=1            # Number of nodes
#SBATCH --ntasks=8           # Number of tasks
#SBATCH --cpus-per-task=1
#SBATCH --time=1-00:00:00    # 1 day
#SBATCH --output=job_1n8c_%j.out
#SBATCH --error=job_1n8c_%j.err 

# Load modules
module load mpi4py/3.1.4
module load Python/3.10.4

echo "Starting job for 1 node and 8 cores"

# Run the script
srun --nodes=1 --ntasks=8 --cpus-per-task=1 python3 scatter-gather.py $1

# Collect job stats
my-job-stats -a -n -s
