#!/bin/bash

# Check if the input file is passed as an argument
if [ -z "$1" ]; then
    echo "Error: No input file provided."
    echo "Usage: $0 <input_file>"
    exit 1
fi

# The input file argument passed to the script
input_file="$1"

# List of SLURM files to submit
slurm_files=(
    "scatter-gather-1-1.slurm"
    "scatter-gather-1-8.slurm"
    "scatter-gather-2-8.slurm"
)

# Loop over each SLURM file and submit it with sbatch
for slurm_file in "${slurm_files[@]}"; do
    echo "Submitting job: $slurm_file with input file: $input_file"
    
    # Submit the SLURM job with the necessary arguments (e.g., input_file)
    sbatch "$slurm_file" "$input_file"
    
    echo "Job submitted: $slurm_file with input file: $input_file"
done

echo "All SLURM jobs have been submitted."
