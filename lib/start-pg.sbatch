#!/bin/bash
#SBATCH --job-name=postgres_job        # Job name
#SBATCH --output=postgres_job.log      # Log file
#SBATCH --time=21-0                    # Time limit (e.g., 1 hour)
#SBATCH --signal=B:USR1@300            # Send SIGUSR1 300 seconds (5 minutes) before termination
#SBATCH --ntasks=1                     # Number of tasks
#SBATCH --cpus-per-task=8              # Number of CPUs
#SBATCH --mem=16G                      # Memory


# Default PostgreSQL port
PG_PORT=5432

# Check if PG_DATA_DIR and OUTPUT_FILE are provided as arguments
if [ -z "$1" ] || [ -z "$2" ]; then
    echo "Error: PG_DATA_DIR or OUTPUT_FILE not specified."
    echo "Usage: sbatch $0 /path/to/custom/directory /path/to/output_file"
    exit 1
fi

PG_DATA_DIR="$1"
OUTPUT_FILE="$2"

# Function to handle the shutdown signal
shutdown_postgres() {
    echo "Shutting down PostgreSQL..."
    pg_ctl stop -D "$PG_DATA_DIR" -m fast
    echo "PostgreSQL stopped."
    exit 0
}

# Function to handle the pre-termination warning
handle_warning() {
    echo "WARNING: Job will terminate in 5 minutes. Preparing for shutdown..."
}

# Trap signals
trap shutdown_postgres SIGTERM
trap handle_warning SIGUSR1

# Load postgresql module
module load postgresql

# Start PostgreSQL
echo "Starting PostgreSQL..."
pg_ctl start -D "$PG_DATA_DIR" -o "-p $PG_PORT" -w
if [ $? -ne 0 ]; then
    echo "Failed to start PostgreSQL. Exiting."
    exit 1
fi
echo "PostgreSQL started on port $PG_PORT."

# Write the hostname and port to the output file
HOSTNAME=$(hostname)
echo "Writing hostname and port to $OUTPUT_FILE..."
echo "PostgreSQL is running on host: $HOSTNAME, port: $PG_PORT" > "$OUTPUT_FILE"
if [ $? -eq 0 ]; then
    echo "Hostname and port written to $OUTPUT_FILE successfully."
else
    echo "Failed to write hostname and port to $OUTPUT_FILE."
    shutdown_postgres
fi

# Keep the script running to prevent job termination
echo "Waiting for termination signal..."
while true; do
    sleep 10
done
