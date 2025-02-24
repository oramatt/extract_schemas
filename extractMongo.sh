#!/bin/bash

# Script Purpose - Planning
#
# Author: Matt DeMarco (matthew.demarco@oracle.com)
#
# This script will gather metadata and a sample document from the listed MongoDB databases
# The data will be used for analysis for 23ai support of migrating a MongoDB workload
# Please see README.md for detailed explanation
#


# Configuration: MongoDB connection details
MONGO_HOST="localhost"
MONGO_PORT="23456"
DATABASES=("test" "yelp")  # Space delimited list of databases to scan
OUTPUT_DIR="mongodb_metadata"  # Base directory for output
PARALLEL_LIMIT=4  # Maximum number of parallel jobs

# Optional MongoDB authentication
USERNAME=""  # MongoDB username (if needed)
PASSWORD=""  # MongoDB password (if needed)
AUTH_DB=""   # Authentication database (e.g., "admin", if needed)

# Construct MongoDB authentication parameters
AUTH_ARGS=()
if [[ -n "$USERNAME" && -n "$PASSWORD" ]]; then
    AUTH_ARGS+=("--username" "$USERNAME" "--password" "$PASSWORD")
    if [[ -n "$AUTH_DB" ]]; then
        AUTH_ARGS+=("--authenticationDatabase" "$AUTH_DB")
    fi
fi

# Ensure required directories exist
mkdir -p "$OUTPUT_DIR"

# Ensure mongosh is installed
if ! command -v mongosh &> /dev/null; then
    echo "Error: mongosh is not installed. Please install MongoDB shell."
    exit 1
fi

echo "Starting metadata extraction for MongoDB on $MONGO_HOST:$MONGO_PORT"
echo "Output will be saved in: $OUTPUT_DIR"


# Extract functions
# Function to extract document count
extract_document_count() {
    local db_name=$1
    local coll_name=$2
    local coll_folder=$3

    mongosh --quiet --host "$MONGO_HOST" --port "$MONGO_PORT" "$db_name" "${AUTH_ARGS[@]}" --eval "
        JSON.stringify({ document_count: db[\"$coll_name\"].estimatedDocumentCount() }, null, 2);
    " > "$coll_folder/document_count.json"
}

# Function to extract storage statistics per collection
extract_storage_stats() {
    local db_name=$1
    local coll_name=$2
    local coll_folder=$3

    mongosh --quiet --host "$MONGO_HOST" --port "$MONGO_PORT" "$db_name" "${AUTH_ARGS[@]}" --eval "
        JSON.stringify(db.runCommand({ collStats: \"$coll_name\" }), null, 2);
    " > "$coll_folder/storage_stats.json"
}

# Extract MongoDB server status (MongoDB version, CPU, Memory, IOPS, and network)
extract_system_info() {
    local db_name=$1
    local coll_name=$2
    local coll_folder=$3 

    mongosh --quiet --host "$MONGO_HOST" --port "$MONGO_PORT" "${AUTH_ARGS[@]}" --eval "
        var status = db.serverStatus();
        var buildInfo = db.adminCommand({ buildInfo: 1 });

        var metrics = {
            mongodb_version: buildInfo.version || 'Unknown',
            uptime_seconds: status.uptime || 0,
            cpu: {
                user: (status.hostInfo && status.hostInfo.system && status.hostInfo.system.cpu) ? status.hostInfo.system.cpu.user : 'N/A',
                system: (status.hostInfo && status.hostInfo.system && status.hostInfo.system.cpu) ? status.hostInfo.system.cpu.system : 'N/A'
            },
            memory: {
                resident_MB: (status.mem && status.mem.resident) ? status.mem.resident : 'N/A',
                virtual_MB: (status.mem && status.mem.virtual) ? status.mem.virtual : 'N/A'
            },
            disk: {
                read_per_sec: (status.metrics && status.metrics.disk && status.metrics.disk.reads) ? status.metrics.disk.reads : 'N/A',
                write_per_sec: (status.metrics && status.metrics.disk && status.metrics.disk.writes) ? status.metrics.disk.writes : 'N/A'
            },
            network: {
                bytes_in: (status.network && status.network.bytesIn) ? status.network.bytesIn.low : 'N/A',
                bytes_out: (status.network && status.network.bytesOut) ? status.network.bytesOut.low : 'N/A'
            }
        };

        JSON.stringify(metrics, null, 2);
    " > "$coll_folder/system_info.json"
}



# Function to extract a sample document
extract_sample_document() {
    local db_name=$1
    local coll_name=$2
    local coll_folder=$3

    mongosh --quiet --host "$MONGO_HOST" --port "$MONGO_PORT" "$db_name" "${AUTH_ARGS[@]}" --eval "
        JSON.stringify({ sample_document: db[\"$coll_name\"].findOne() || {} }, null, 2);
    " > "$coll_folder/example_document.json"
}

# Function to extract schema and indexes
extract_schema_and_indexes() {
    local db_name=$1
    local coll_name=$2
    local coll_folder=$3

    mongosh --quiet --host "$MONGO_HOST" --port "$MONGO_PORT" "$db_name" "${AUTH_ARGS[@]}" --eval "
        var coll = \"$coll_name\";
        var cursor = db[coll].find().limit(100);
        var schema = {};
        cursor.forEach(doc => {
            Object.keys(doc).forEach(key => {
                let val = doc[key];
                let detectedType;
                if (val === null) detectedType = 'null';
                else if (Array.isArray(val)) detectedType = 'array';
                else if (val instanceof ObjectId) detectedType = 'ObjectId';
                else if (val instanceof Date) detectedType = 'date';
                else if (val instanceof Timestamp) detectedType = 'Timestamp';
                else if (val instanceof NumberLong) detectedType = 'long';
                else if (val instanceof NumberInt) detectedType = 'int';
                else if (val instanceof NumberDecimal) detectedType = 'Decimal128';
                else if (val instanceof BinData) detectedType = 'Binary';
                else if (val instanceof RegExp) detectedType = 'Regex';
                else if (typeof val === 'object' && val.hasOwnProperty('$minKey')) detectedType = 'MinKey';
                else if (typeof val === 'object' && val.hasOwnProperty('$maxKey')) detectedType = 'MaxKey';
                else if (typeof val === 'object' && val.hasOwnProperty('$code')) detectedType = 'JavaScript';
                else if (typeof val === 'object' && val.hasOwnProperty('$scope')) detectedType = 'JavaScriptWithScope';
                else if (typeof val === 'object' && val.type === 'Point') detectedType = 'GeoJSON Point';
                else if (typeof val === 'object' && val.type === 'LineString') detectedType = 'GeoJSON LineString';
                else if (typeof val === 'object' && val.type === 'Polygon') detectedType = 'GeoJSON Polygon';
                else if (typeof val === 'number') {
                    if (Number.isInteger(val) && val >= -2147483648 && val <= 2147483647) detectedType = 'int';
                    else detectedType = 'double';
                } 
                else if (typeof val === 'boolean') detectedType = 'boolean';
                else if (typeof val === 'string') detectedType = 'string';
                else if (typeof val === 'object') detectedType = 'object';
                else detectedType = 'unknown';
                if (!schema[key]) schema[key] = [detectedType];
                else if (!schema[key].includes(detectedType)) schema[key].push(detectedType);
            });
        });
        JSON.stringify({ schema: schema, indexes: db[coll].getIndexes() }, null, 2);
    " > "$coll_folder/schema_and_indexes.json"
}

# Extract from database profiler if enabled, if not extract from in-memory MongoDB logs using getLog: "global" to review workload operations & operators
extract_workload_profile() {
    local db_name=$1
    local coll_name=$2
    local coll_folder=$3

    # Get profiling level for the database using `db.getProfilingStatus().was`
    profiling_level=$(mongosh --quiet --host "$MONGO_HOST" --port "$MONGO_PORT" "$db_name" "${AUTH_ARGS[@]}" --eval "
        print(db.getProfilingStatus().was);
    " | tail -n 1)

    # echo $profiling_level

    if [[ "$profiling_level" =~ ^[1-2]$ ]]; then
        # Profiling is enabled, extract data from `system.profile`
        mongosh --quiet --host "$MONGO_HOST" --port "$MONGO_PORT" "$db_name" "${AUTH_ARGS[@]}" --eval "
            var profilerData = db.system.profile.find({ ns: '$db_name.$coll_name' }).toArray();
            printjson({
                source: 'database_profiler',
                database: '$db_name',
                collection: '$coll_name',
                profilerEntries: profilerData.length > 0 ? profilerData : 'No profiler data available'
            });
        " > "$coll_folder/workload.json"

    else
        # Profiling is not enabled, use in-memory logs (`getLog: 'global'`)
        mongosh --quiet --host "$MONGO_HOST" --port "$MONGO_PORT" "admin" "${AUTH_ARGS[@]}" --eval "
            var logs = db.adminCommand({ getLog: 'global' });
            var commandLogs = logs.log.filter(entry => entry.includes('COMMAND') && entry.includes('$db_name.$coll_name'));
            printjson({
                source: 'in-memory_logs',
                totalLines: logs.totalLinesWritten,
                commandEntries: commandLogs.length > 0 ? commandLogs : 'No matching COMMAND logs found'
            });
        " > "$coll_folder/workload.json"
    fi
}


# Retrieve available databases
# databases_json=$(mongosh --quiet --host "$MONGO_HOST" --port "$MONGO_PORT" "${AUTH_ARGS[@]}" --eval "JSON.stringify(db.adminCommand({listDatabases: 1}).databases.map(d => d.name))" | tail -n 1)

# Loop through each database
for DB_NAME in "${DATABASES[@]}"; do
    echo "--------------------------------------------"
    echo "Extracting metadata for database: $DB_NAME"

    DB_FOLDER="$OUTPUT_DIR/${DB_NAME//[^a-zA-Z0-9_]}"  
    mkdir -p "$DB_FOLDER"

    collections_json=$(mongosh --quiet --host "$MONGO_HOST" --port "$MONGO_PORT" "$DB_NAME" "${AUTH_ARGS[@]}" --eval "JSON.stringify(db.getCollectionNames())" | tail -n 1)
    collections=($(echo "$collections_json" | sed 's/\[//;s/\]//g' | tr ',' ' ' | tr -d '"'))

    # Parallel job control
    job_count=0

    for coll_name in "${collections[@]}"; do
        if [[ -z "$coll_name" ]] || [[ "$coll_name" == system.* ]]; then
            echo "Skipping system collection: $coll_name"
            continue
        fi

        COLL_FOLDER="$DB_FOLDER/${coll_name//[^a-zA-Z0-9_]}"
        mkdir -p "$COLL_FOLDER"

        # Run extract functions
        # Add new functions here
        # Run metadata extraction in parallel
        (
            extract_document_count "$DB_NAME" "$coll_name" "$COLL_FOLDER" &
            extract_storage_stats "$DB_NAME" "$coll_name" "$COLL_FOLDER" &
            extract_system_info "$DB_NAME" "$coll_name" "$COLL_FOLDER" & 
            extract_sample_document "$DB_NAME" "$coll_name" "$COLL_FOLDER" &
            extract_schema_and_indexes "$DB_NAME" "$coll_name" "$COLL_FOLDER" &
            extract_workload_profile "$DB_NAME" "$coll_name" "$COLL_FOLDER" &
            echo "Saved metadata for collection: $DB_NAME.$coll_name" &
        ) &

        ((job_count++))

        # Limit parallel jobs
        if [[ "$job_count" -ge $PARALLEL_LIMIT ]]; then
            wait  
            job_count=0
        fi
    done

    wait  

    echo "Metadata extraction complete for database: $DB_NAME"
    echo "Saved in folder: $DB_FOLDER"
done

echo "All metadata extraction completed!"
