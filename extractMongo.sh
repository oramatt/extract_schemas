#!/bin/bash

# Script Purpose - Planning
#
# Author: Matt DeMarco (matthew.demarco@oracle.com)
#
# This script will gather metadata and a sample document from the listed MongoDB databases
# The data will be used for analysis for 23ai support of migrating a MongoDB workload
# Please see README.md for detailed explanation
#
#

# ==========================================
# Configuration parameters
# ==========================================

# MongoDB connection details
MONGO_HOST="localhost"
MONGO_PORT="23456"
DATABASES=("emptyonpurpose" "test" "tweetme")  # Space delimited list of databases to scan
OUTPUT_DIR="mongodb_metadata"  # Base directory for output
PARALLEL_LIMIT=32  # Maximum number of parallel jobs

# MongoDB authentication
USERNAME=""  # MongoDB username (if needed)
PASSWORD=""  # MongoDB password (if needed)
AUTH_DB=""   # Authentication database (e.g., "admin", if needed)
USE_TLS="false"  # Set to "true" to enable TLS/SSL, needs work to completely enable

# ==========================================
# Setup area
# ==========================================

# Create timestamp
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="${OUTPUT_DIR}/extract_${TIMESTAMP}.log"
mkdir -p "$OUTPUT_DIR"
touch "$LOG_FILE"

# Logging functions
log_info() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [INFO] $*" | tee -a "$LOG_FILE"
}

log_error() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [ERROR] $*" | tee -a "$LOG_FILE" >&2
}

# Build MongoDB connection parameters
AUTH_ARGS=()
if [[ -n "$USERNAME" && -n "$PASSWORD" ]]; then
    AUTH_ARGS+=("--username" "$USERNAME" "--password" "$PASSWORD")
    if [[ -n "$AUTH_DB" ]]; then
        AUTH_ARGS+=("--authenticationDatabase" "$AUTH_DB")
    fi
fi

if [[ "$USE_TLS" == "true" ]]; then
    AUTH_ARGS+=("--tls")
fi

# ==========================================
# Check prerequisites
# ==========================================

# Check for mongosh
if ! command -v mongosh &>/dev/null; then
    log_error "mongosh is not installed. Please install MongoDB shell."
    exit 1
fi

# Test MongoDB connection
log_info "Testing connection to MongoDB at $MONGO_HOST:$MONGO_PORT..."
mongo_version=$(mongosh --quiet --host "$MONGO_HOST" --port "$MONGO_PORT" "${AUTH_ARGS[@]}" --eval "db.version()" 2>/dev/null)
if [[ $? -ne 0 ]]; then
    log_error "Cannot connect to MongoDB at $MONGO_HOST:$MONGO_PORT. Please check connection details."
    exit 1
fi

log_info "Connected to MongoDB version $mongo_version at $MONGO_HOST:$MONGO_PORT"
log_info "Starting metadata extraction"
log_info "Output directory: $OUTPUT_DIR"
log_info "Log file: $LOG_FILE"

# ==========================================
# Extraction Functions
# ==========================================

# Function to extract document count
extract_document_count() {
    local db_name=$1
    local coll_name=$2
    local coll_folder=$3
    local output_file="$coll_folder/document_count.json"
    
    mongosh --quiet --host "$MONGO_HOST" --port "$MONGO_PORT" "$db_name" "${AUTH_ARGS[@]}" --eval "
        JSON.stringify({ document_count: db[\"$coll_name\"].estimatedDocumentCount() }, null, 2);
    " > "$output_file" 2>/dev/null
    
    # Handle empty output
    if [[ ! -s "$output_file" ]]; then
        echo "{\"document_count\": 0, \"error\": \"Failed to extract count\"}" > "$output_file"
        log_error "Failed to extract document count for $db_name.$coll_name"
        return 1
    else
        log_info "Successfully extracted document count"
    fi
    
    return 0
}

# Function to extract storage statistics
extract_storage_stats() {
    local db_name=$1
    local coll_name=$2
    local coll_folder=$3
    local output_file="$coll_folder/storage_stats.json"
    
    mongosh --quiet --host "$MONGO_HOST" --port "$MONGO_PORT" "$db_name" "${AUTH_ARGS[@]}" --eval "
        JSON.stringify(db.runCommand({ collStats: \"$coll_name\" }), null, 2);
    " > "$output_file" 2>/dev/null
    
    # Handle empty output
    if [[ ! -s "$output_file" ]]; then
        echo "{\"error\": \"Failed to extract storage statistics\"}" > "$output_file"
        log_error "Failed to extract storage statistics"
        return 1
    else
        log_info "Successfully extracted storage statistics"
    fi
    
    return 0
}

# Extract system info
extract_system_info() {
    local db_name=$1
    local coll_name=$2
    local coll_folder=$3
    local output_file="$coll_folder/system_info.json"
    
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
    " > "$output_file" 2>/dev/null
    
    # Handle empty output
    if [[ ! -s "$output_file" ]]; then
        echo "{\"error\": \"Failed to extract system information\"}" > "$output_file"
        log_error "Failed to extract system information"
        return 1
    else
        log_info "Successfully extracted system information"
    fi
    
    return 0
}

# Function to extract a sample document
extract_sample_document() {
    local db_name=$1
    local coll_name=$2
    local coll_folder=$3
    local output_file="$coll_folder/example_document.json"
    
    mongosh --quiet --host "$MONGO_HOST" --port "$MONGO_PORT" "$db_name" "${AUTH_ARGS[@]}" --eval "
        JSON.stringify({ sample_document: db[\"$coll_name\"].findOne() || {} }, null, 2);
    " > "$output_file" 2>/dev/null
    
    # Handle empty output
    if [[ ! -s "$output_file" ]]; then
        echo "{\"sample_document\": {}, \"error\": \"Failed to extract sample document\"}" > "$output_file"
        log_error "Failed to extract sample document for $db_name.$coll_name"
        return 1
    else
        log_info "Successfully extracted sample document for $db_name.$coll_name"        
    fi
    
    return 0
}

# Function to extract schema and indexes
extract_schema_and_indexes() {
    local db_name=$1
    local coll_name=$2
    local coll_folder=$3
    local output_file="$coll_folder/schema_and_indexes.json"

    log_info "Extracting schema and indexes for $db_name.$coll_name..."

    mongosh --quiet --host "$MONGO_HOST" --port "$MONGO_PORT" "$db_name" "${AUTH_ARGS[@]}" --eval "
        var coll = \"$coll_name\";
        var cursor = db[coll].find().limit(300);
        var schema = {};
        
        function detectType(val) {
            if (val === null) return 'null';
            if (val === undefined) return 'undefined';
            
            if (val instanceof ObjectId) return 'ObjectId';
            if (val instanceof Date) return 'date';
            if (val instanceof Timestamp) return 'Timestamp';
            if (val instanceof NumberLong) return 'long';
            if (val instanceof NumberInt) return 'int';
            if (val instanceof NumberDecimal) return 'Decimal128';
            if (val instanceof BinData) return 'Binary';
            if (val instanceof RegExp) return 'Regex';
            
            if (Array.isArray(val)) return 'array';
            if (typeof val === 'number') {
                if (Number.isInteger(val) && val >= -2147483648 && val <= 2147483647) return 'int';
                return 'double';
            }
            if (typeof val === 'boolean') return 'boolean';
            if (typeof val === 'string') return 'string';
            if (typeof val === 'object') {
                if (val.type && val.type === 'Point' && val.coordinates) return 'GeoJSON Point';
                return 'object';
            }
            
            return 'unknown';
        }
        
        var docCount = 0;
        cursor.forEach(function(doc) {
            docCount++;
            processObject(doc, '');
        });
        
        function processObject(obj, prefix) {
            for (var key in obj) {
                var fullPath = prefix ? prefix + '.' + key : key;
                var val = obj[key];
                var detectedType = detectType(val);
                
                if (!schema[fullPath]) {
                    schema[fullPath] = [detectedType];
                } else if (schema[fullPath].indexOf(detectedType) === -1) {
                    schema[fullPath].push(detectedType);
                }
                
                if (typeof val === 'object' && val !== null && !Array.isArray(val) 
                    && !(val instanceof ObjectId) && !(val instanceof Date) 
                    && !(val instanceof RegExp) && !(val instanceof BinData)) {
                    processObject(val, fullPath);
                }
            }
        }
        
        var indexInfo = [];
        try {
            indexInfo = db[coll].getIndexes();
        } catch(e) {
            print('Error getting indexes: ' + e.message);
        }
        
        var totalDocs = 0;
        try {
            totalDocs = db[coll].estimatedDocumentCount();
        } catch(e) {
            print('Error getting document count: ' + e.message);
        }
        
        JSON.stringify({ 
            schema: schema, 
            indexes: indexInfo,
            metadata: {
                sampleSize: docCount,
                totalDocuments: totalDocs
            }
        }, null, 2);
    " > "$output_file" 2>/dev/null
    
    # Handle empty output
    if [[ ! -s "$output_file" ]]; then
        echo "{\"error\": \"Failed to extract schema and indexes\"}" > "$output_file"
        log_error "Failed to extract schema for $db_name.$coll_name"
        return 1
    else
        log_info "Successfully extracted schema for $db_name.$coll_name" 

    fi
    
    return 0
}

# Extract workload profile
extract_workload_profile() {
    local db_name=$1
    local coll_name=$2
    local coll_folder=$3
    local output_file="$coll_folder/workload.json"
    
    # Get profiling level
    profiling_level=$(mongosh --quiet --host "$MONGO_HOST" --port "$MONGO_PORT" "$db_name" "${AUTH_ARGS[@]}" --eval "
        print(db.getProfilingStatus().was);
    " 2>/dev/null | tail -n 1)

    if [[ "$profiling_level" =~ ^[1-2]$ ]]; then
        # Profiling is enabled
        mongosh --quiet --host "$MONGO_HOST" --port "$MONGO_PORT" "$db_name" "${AUTH_ARGS[@]}" --eval "
            var profilerData = db.system.profile.find({ ns: '$db_name.$coll_name' }).toArray();
            printjson({
                source: 'database_profiler',
                database: '$db_name',
                collection: '$coll_name',
                profilerEntries: profilerData.length > 0 ? profilerData : 'No profiler data available'
            });
        " > "$output_file" 2>/dev/null
    else
        # Profiling is not enabled
        mongosh --quiet --host "$MONGO_HOST" --port "$MONGO_PORT" "admin" "${AUTH_ARGS[@]}" --eval "
            var logs = db.adminCommand({ getLog: 'global' });
            var commandLogs = logs.log.filter(function(entry) { 
                return entry.includes('COMMAND') && entry.includes('$db_name.$coll_name');
            });
            printjson({
                source: 'in-memory_logs',
                totalLines: logs.totalLinesWritten,
                commandEntries: commandLogs.length > 0 ? commandLogs : 'No matching COMMAND logs found'
            });
        " > "$output_file" 2>/dev/null
    fi
    
    # Handle empty output
    if [[ ! -s "$output_file" ]]; then
        echo "{\"error\": \"Failed to extract workload profile\"}" > "$output_file"
        log_error "Failed to extract workload profile for $db_name.$coll_name"
        return 1
    else
        log_info "Successfully extracted workload profile for $db_name.$coll_name"
    fi
    
    # Create simple workload summary, might be overkill 👷
    # echo "Workload extracted on $(date)" > "$coll_folder/workload_summary.txt"
    
    return 0
}

# Function to extract users and roles
extract_users_and_roles() {
    local db_name=$1
    local coll_name=$2
    local coll_folder=$3
    local output_file="$coll_folder/users_and_roles.json"

    mongosh --quiet --host "$MONGO_HOST" --port "$MONGO_PORT" "${AUTH_ARGS[@]}" --eval "
        JSON.stringify({
            users: db.getUsers(),
            roles: db.getRoles({ showPrivileges: true })
        }, null, 2);
    " > "$output_file" 2>/dev/null

    # Handle empty output
    if [[ ! -s "$output_file" ]]; then
        echo "{\"error\": \"Failed to extract users and roles\"}" > "$output_file"
        log_error "Failed to extract users and roles"
        return 1
    else
        log_info "Successfully extracted users and roles"
    fi

    return 0
}


# Create collection summary
create_collection_summary() {
    local db_name=$1
    local coll_name=$2
    local coll_folder=$3
    
    echo "{
  \"database\": \"$db_name\",
  \"collection\": \"$coll_name\",
  \"extractedAt\": \"$(date -u +"%Y-%m-%dT%H:%M:%SZ")\",
  \"extractionStatus\": \"complete\"
}" > "$coll_folder/summary.json"
}

# ==========================================
# Main Extraction Process
# ==========================================

start_time=$(date +%s)
total_dbs=0
total_collections=0

# Process each database
for DB_NAME in "${DATABASES[@]}"; do
    log_info "--------------------------------------------"
    log_info "Extracting metadata for database: $DB_NAME"

    DB_FOLDER="$OUTPUT_DIR/${DB_NAME//[^a-zA-Z0-9_]}"  
    mkdir -p "$DB_FOLDER"

    # Get collections
    collections_json=$(mongosh --quiet --host "$MONGO_HOST" --port "$MONGO_PORT" "$DB_NAME" "${AUTH_ARGS[@]}" --eval "
        JSON.stringify(db.getCollectionNames());
    " 2>/dev/null)

    # If the database does not exist or is empty, log it
    if [[ -z "$collections_json" ]]; then
        log_info "No collections found in database: $DB_NAME (database may not exist)"
        continue  # Skip this database and move to the next
    fi
    
    # Parse collection names
    collections=($(echo "$collections_json" | sed 's/\[//;s/\]//g' | tr ',' ' ' | tr -d '"'))
    
    log_info "Found ${#collections[@]} collections in database $DB_NAME"
    ((total_dbs++))

    # Track jobs
    job_count=0

    for coll_name in "${collections[@]}"; do
            # Skip system collections
        if [[ -z "$coll_name" ]] || [[ "$coll_name" == system.* ]]; then
            log_info "Skipping system collection: $coll_name"
            continue
        fi

        COLL_FOLDER="$DB_FOLDER/${coll_name//[^a-zA-Z0-9_]}"
        mkdir -p "$COLL_FOLDER"
        
        ((total_collections++))
        
        # Extract metadata (in parallel)
        (
            collection_status="complete"
            
            # Run extraction with basic error handling
            # echo "Running extract in parallel"
            extract_document_count "$DB_NAME" "$coll_name" "$COLL_FOLDER" || collection_status="partial" 
            extract_storage_stats "$DB_NAME" "$coll_name" "$COLL_FOLDER" || collection_status="partial" 
            extract_system_info "$DB_NAME" "$coll_name" "$COLL_FOLDER" || collection_status="partial" 
            extract_sample_document "$DB_NAME" "$coll_name" "$COLL_FOLDER" || collection_status="partial" 
            extract_schema_and_indexes "$DB_NAME" "$coll_name" "$COLL_FOLDER" || collection_status="partial" 
            extract_workload_profile "$DB_NAME" "$coll_name" "$COLL_FOLDER" || collection_status="partial" 
            extract_users_and_roles "$DB_NAME" "$coll_name" "$COLL_FOLDER" || collection_status="partial" 
            
            # Create summary
            echo "{
                  \"database\": \"$DB_NAME\",
                  \"collection\": \"$coll_name\",
                  \"extractedAt\": \"$(date -u +"%Y-%m-%dT%H:%M:%SZ")\",
                  \"extractionStatus\": \"$collection_status\"
}" > "$COLL_FOLDER/summary.json"
            
            log_info "Completed metadata extraction for $DB_NAME.$coll_name ($collection_status)"
        ) &

        ((job_count++))

        # Limit parallel jobs
        if [[ "$job_count" -ge $PARALLEL_LIMIT ]]; then
            wait  
            job_count=0
        fi
    done

    # Wait for all jobs to finish
    wait  

    log_info "Metadata extraction complete for database: $DB_NAME"
    log_info "Saved in folder: $DB_FOLDER"
done

# Calculate execution time
end_time=$(date +%s)
duration=$((end_time - start_time))
hours=$((duration / 3600))
minutes=$(( (duration % 3600) / 60 ))
seconds=$((duration % 60))

log_info "======================================================"
log_info "Metadata extraction completed!"
log_info "Processed $total_collections collections across $total_dbs databases"
log_info "Total duration: ${hours}h ${minutes}m ${seconds}s"
log_info "Output directory: $OUTPUT_DIR"
log_info "Log file: $LOG_FILE"
log_info "======================================================"


# done 🚀
