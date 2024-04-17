#!/bin/bash

# Check for correct number of arguments
if [ "$#" -ne 5 ]; then
    echo "Usage: $0 <solr_scripts_path> <solr_post_path> <solr_port> <input_directory> <solr_core_name>"
    exit 1
fi

# Extract arguments
SOLR_SCRIPTS_PATH="$1"
SOLR_POST_PATH="$2"
SOLR_PORT="$3"
DIRECTORY="$4"
CORE_NAME="$5"

# Start Solr
echo "Starting Solr in port $SOLR_PORT"
bash "$SOLR_SCRIPTS_PATH/start.sh" $SOLR_PORT $CORE_NAME
wait
sleep 60

# Delete all the documents on the Solr Core
echo "Deleting all documents Solr in port under http://localhost:$SOLR_PORT/solr/$CORE_NAME/"
eval "curl -X POST -H 'Content-Type: application/json' 'http://localhost:$SOLR_PORT/solr/$CORE_NAME/update' --data-binary '{\"delete\": {\"query\":\"*:*\"}, \"commit\": {}}'"
wait


# Define the command to execute for each file
COMMAND="$SOLR_POST_PATH -p $SOLR_PORT -commit false -c $CORE_NAME"

# Function to process each file
process_file() {
    local file="$1"
    echo "$COMMAND" "$file"
    # Execute the command for the file
    cat "$file" | xargs -I{} sh -c "$COMMAND {}"
}

# Export the function to make it available to parallel
export -f process_file

# Change to the directory
cd "$DIRECTORY" || exit 1

pwd
# Split the list of files in the directory into groups of 10 files each
find . -maxdepth 1 -type f -name 'part*.json' | split -l 10 - file_list_part_
wait

# Process each group of files in parallel
for file_group in file_list_part_*; do
    process_file "$file_group" &
done
wait

eval "curl http://localhost:$SOLR_PORT/solr/$CORE_NAME/update?optimize=true"
wait


# Clean up temporary files (optional)
rm file_list_part_*

# Stop Solr
bash "$SOLR_SCRIPTS_PATH/stop.sh" $SOLR_PORT
touch "$DIRECTORY/_INDEX_SUCCESS"
exit  0
