#!/bin/bash
#SBATCH --job-name=process_images    # Job name
#SBATCH --array=1-1000              # Array of 1000 tasks
#SBATCH --output=logs/job_%A_%a.out # Standard output (one file per task)
#SBATCH --error=logs/job_%A_%a.err  # Standard error (one file per task)
#SBATCH --cpus-per-task=4           # Number of CPU cores per task
#SBATCH --mem=8G                    # Memory per task
#SBATCH --time=02:00:00             # Maximum runtime
#SBATCH --partition=standard        # Partition name (adjust as necessary)

# Ensure a manifest file argument is provided
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <manifest_file>"
    exit 1
fi

# Get the manifest file from arguments
MANIFEST_FILE="$1"

# Batch size per task
BATCH_SIZE=100

# Calculate the start and end batch for this task
BATCH_FROM=$(( (SLURM_ARRAY_TASK_ID - 1) * BATCH_SIZE + 1 ))
BATCH_TO=$(( BATCH_FROM + BATCH_SIZE - 1 ))

echo "Processing batches from $BATCH_FROM to $BATCH_TO"

# Run the command with the calculated batch range
python3 ../impc_etl/jobs/transform/images_jpg_generation.py \
    --manifest "$MANIFEST_FILE" \
    --batch-from $BATCH_FROM \
    --batch-to $BATCH_TO \
    --full-suffix "_full" \
    --thumbnail-suffix "_thumbnail" \
    --thumbnail-width 200 \
    --thumbnail-quality 80