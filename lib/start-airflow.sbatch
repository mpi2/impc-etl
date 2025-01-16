#!/bin/bash
#SBATCH --job-name=airflow_job          # Job name
#SBATCH --output=airflow_job.log        # Log file
#SBATCH --time=01:00:00                 # Time limit (e.g., 1 hour)
#SBATCH --ntasks=1                      # Number of tasks
#SBATCH --cpus-per-task=4               # Number of CPUs
#SBATCH --mem=4G                        # Memory

# Arguments
if [ -z "$1" ] || [ -z "$2" ]; then
    echo "Error: Required arguments not provided."
    echo "Usage: sbatch $0 /path/to/hostname_file /path/to/airflow_home"
    exit 1
fi

HOSTNAME_FILE="$1"        # File with hostname and port information from PostgreSQL job
AIRFLOW_HOME="$2"         # Path to Airflow home directory

# Ensure the hostname file exists
if [ ! -f "$HOSTNAME_FILE" ]; then
    echo "Error: Hostname file not found at $HOSTNAME_FILE."
    exit 1
fi

# Read hostname and port from the file
HOST_INFO=$(cat "$HOSTNAME_FILE")
DB_HOST=$(echo "$HOST_INFO" | grep -oP '(?<=host: ).*?(?=,)')
DB_PORT=$(echo "$HOST_INFO" | grep -oP '(?<=port: ).*')

if [ -z "$DB_HOST" ] || [ -z "$DB_PORT" ]; then
    echo "Error: Failed to parse hostname or port from $HOSTNAME_FILE."
    exit 1
fi

# Build the SQLAlchemy URL
DB_USER="airflow_user"    # Replace with your database username
DB_PASSWORD="secure_pass" # Replace with your database password
DB_NAME="airflow"         # Replace with your database name
SQLALCHEMY_URL="postgresql+psycopg2://${DB_USER}:${DB_PASSWORD}@${DB_HOST}:${DB_PORT}/${DB_NAME}"

# Set environment variables for Airflow
export AIRFLOW_HOME="$AIRFLOW_HOME"
export AIRFLOW__CORE__SQL_ALCHEMY_CONN="$SQLALCHEMY_URL"

echo "Using SQLAlchemy URL: $SQLALCHEMY_URL"

# Initialize the Airflow database
echo "Initializing Airflow database..."
airflow db init
if [ $? -ne 0 ]; then
    echo "Error: Failed to initialize Airflow database."
    exit 1
fi

# Start the Airflow webserver
echo "Starting Airflow webserver..."
airflow webserver -p 8080 &
WEBSERVER_PID=$!

# Start the Airflow scheduler
echo "Starting Airflow scheduler..."
airflow scheduler &
SCHEDULER_PID=$!

# Wait for termination signals
trap "kill $WEBSERVER_PID $SCHEDULER_PID; exit 0" SIGTERM SIGINT

# Keep the script running
wait
