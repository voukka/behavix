#!/bin/bash
set -e
#set -e -x

# Check for correct usage
if [ "$#" -ne 2 ]; then
    echo "Usage: calculate_web_sessions <input_file> <output_file>"
    exit 1
fi

# Input and output file arguments
INPUT_FILE="$1"
OUTPUT_FILE="$2"
SQL_FILE="calculate_web_sessions.sql"

# Check if the SQL file exists
if [ ! -f "$SQL_FILE" ]; then
    echo "SQL file $SQL_FILE not found. Please ensure it exists in the current directory."
    exit 1
fi

# Create a temporary SQL file with the input file replaced
TEMP_SQL_FILE=$(mktemp)
sed "s|{input_file}|$INPUT_FILE|g" "$SQL_FILE" > "$TEMP_SQL_FILE"


# Run the DuckDB CLI command with the provided SQL
duckdb -noheader <<EOF
PRAGMA memory_limit='2GB';
PRAGMA threads=4;

.read $TEMP_SQL_FILE
COPY (SELECT * FROM sessions_with_domains) to $OUTPUT_FILE (FORMAT PARQUET);
EOF

# Clean up the temporary SQL file
rm "$TEMP_SQL_FILE"

echo "Sessions calculated and saved to $OUTPUT_FILE"
