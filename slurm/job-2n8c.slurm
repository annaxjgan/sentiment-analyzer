#!/bin/bash

#SBATCH --nodes=2            # Number of nodes
#SBATCH --ntasks=8           # Number of tasks
#SBATCH --ntasks-per-node=4  # Distribute the 8 tasks across 2 nodes
#SBATCH --cpus-per-task=1

# Load modules
module load mpi4py/3.1.4
module load Python/3.10.4

echo "Starting job for 2 nodes and 8 cores"

# Run the script
srun -n 8 python3 ../app/scatter-gather.py $1

# Collect job stats
my-job-stats -a -n -s
