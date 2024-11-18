#!/bin/bash

# Array of file sizes
file_sizes=("1MB" "10MB" "100MB" "1GB" "10GB")

# Loop through each file size
for size in "${file_sizes[@]}"; do
    echo "Processing file of size $size"
    # Add your processing commands here
    python join.py $size 10
done