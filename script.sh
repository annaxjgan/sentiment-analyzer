#!/bin/bash

# Get the input file name as argument
input_file=$1

# Folder where your .slurm files are stored
slurm_folder="slurm"

# Loop through each .slurm file and submit it with the input file as argument
for script in "$slurm_folder"/*.slurm; do
    echo "Submitting $script with input file: $input_file"
    sbatch "$script" "$input_file"

echo "All SLURM jobs submitted"

done
