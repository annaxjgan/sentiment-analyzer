#!/bin/bash

#SBATCH --nodes=1            # Number of nodes
#SBATCH --ntasks=8           # Number of tasks
#SBATCH --cpus-per-task=1

# Load modules
module load OpenMPI/4.1.4
module load GCCcore/11.3.0
module load mpi4py/3.1.4
module load Python/3.10.4

echo "Starting job for 1 node and 8 cores"

# Run the script
srun -n 8 python3 ../app/scatter-gather.py $1

# Collect job stats
my-job-stats -a -n -s
