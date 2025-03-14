# MongoDB Metadata Extraction and Restoration

## Overview
This repository provides two scripts for working with MongoDB metadata extraction and data restoration:

1. **`extractMongo.sh`**: Extracts metadata, schema details, sample documents, storage statistics, system performance data, and workload profiling information from a MongoDB instance.
2. **`restoreMongo.py`**: Recreates MongoDB collections using extracted metadata and sample documents, falling back to schema definitions if necessary.

## Features
### extractMongo.sh
- **Document Count Extraction**: Retrieves an estimated count of documents per collection using `estimatedDocumentCount()`.
- **Storage Statistics**: Captures collection size, index size, and detailed storage metrics using `collStats`.
- **System Information**: Fetches MongoDB version, CPU usage, memory consumption, disk I/O, and network activity statistics.
- **Sample Document Retrieval**: Extracts a representative document from each collection using `findOne()`.
- **Schema and Index Detection**: 
  - Identifies field types in collections by examining up to 300 sample documents (increased from 100)
  - Detects MongoDB-specific types (ObjectId, Date, NumberLong, etc.)
  - Recognizes GeoJSON structures (Point, LineString, Polygon)
  - Extracts all collection indexes using `getIndexes()`
- **Workload Profile Extraction**:
  - Uses the MongoDB profiler data when enabled (`profiling level 1-2`)
  - Falls back to MongoDB in-memory logs (`getLog: 'global'`) when profiling is disabled
- **User and Role Information**: Captures user details and associated role permissions using `getUsers()` and `getRoles()`.
- **Parallel Processing**: Extracts data from multiple collections concurrently with a configurable job limit.
- **Authentication Support**: Supports username/password authentication with custom authentication database.
- **TLS/SSL Support**: Optional TLS/SSL connection capabilities for secure data extraction.
- **Robust Error Handling**: Each extraction function has error checking with appropriate logging.
- **Detailed Logging**: Comprehensive timestamped logs for tracking extraction progress.

### restoreMongo.py
- **Collection Recreation**: Provides a pathway to recreate collections from extracted metadata.
- **Flexible Data Source**: 
  - Uses sample documents from `example_document.json` when available
  - Falls back to schema information from `schema_and_indexes.json` when needed
- **Synthetic Data Generation**: Uses the Python `faker` library to generate placeholder data when working with schema-only information.
- **MongoDB Type Support**: Handles various MongoDB data types including:
  - Standard types (string, int, long, double, boolean, date)
  - MongoDB-specific types (ObjectId, Decimal128)
  - GeoJSON structures (Point, LineString, Polygon)
- **Basic Error Handling**: Provides error messages for missing or invalid metadata files.

## Prerequisites
- **MongoDB shell (`mongosh`)** must be installed and accessible from the command line.
- **Python 3.x** is required to run `restoreMongo.py`.
- **Python packages**: 
  - `faker`: For generating synthetic data
  - `pymongo`: For MongoDB connectivity
- The user must have **read access** to the targeted databases for extraction.
- The user must have **write access** to MongoDB for restoration.
- Optional: If authentication is required, provide a valid username, password, and authentication database.

## Configuration
### extractMongo.sh
Edit the script variables to define the MongoDB connection details and databases to scan:

```bash
# MongoDB connection details
MONGO_HOST="localhost"
MONGO_PORT="23456"
DATABASES=("test" "tweetme" "emptyonpurpose")  # Space delimited list of databases to scan
OUTPUT_DIR="mongodb_metadata"  # Base directory for output
PARALLEL_LIMIT=4  # Maximum number of parallel jobs

# MongoDB authentication
USERNAME=""  # MongoDB username (if needed)
PASSWORD=""  # MongoDB password (if needed)
AUTH_DB=""   # Authentication database (e.g., "admin", if needed)
USE_TLS="false"  # Set to "true" to enable TLS/SSL
```

### restoreMongo.py
Edit the script variables to define MongoDB connection details:

```python
# Configuration: MongoDB connection details
MONGO_HOST = "localhost"
MONGO_PORT = 23456
DATABASES_DIR = "mongodb_metadata"  # Directory containing metadata
USERNAME = ""  # MongoDB username (if needed)
PASSWORD = ""  # MongoDB password (if needed)
AUTH_DB = ""   # Authentication database (if needed)
```

## Running the Scripts
### Extract Metadata
Run the extraction script:
```bash
bash extractMongo.sh
```
This creates a structured directory containing JSON files with MongoDB metadata.

### Restore Collections
To recreate collections from extracted metadata:
```bash
python3 restoreMongo.py
```
This script reads the extracted metadata and restores collections using sample documents or schema information.

## Output Structure
The extracted metadata is stored under the `mongodb_metadata/` directory in the following structure:
```
mongodb_metadata/
   ├── <database>/
   │   ├── <collection>/
   │   │   ├── document_count.json
   │   │   ├── storage_stats.json
   │   │   ├── system_info.json
   │   │   ├── example_document.json
   │   │   ├── schema_and_indexes.json
   │   │   ├── workload.json
   │   │   ├── users_and_roles.json
   │   │   └── summary.json
   ├── extract_YYYYMMDD_HHMMSS.log  # Extraction log file
```

## Limitations
### extractMongo.sh
- Examines only up to 300 documents per collection for schema detection (increased from 100)
- Detects only top-level fields in documents (limited support for nested objects)
- All data extraction occurs at the collection level

### restoreMongo.py
- Creates only one synthetic document when using schema-based generation
- Does not specifically recreate indexes from the extracted metadata
- Includes the original `_id` field which may cause conflicts during restoration
- No built-in TLS/SSL support

## Conclusion
These scripts aid in analyzing MongoDB workloads and restoring collections for testing or migration. The extracted metadata helps in understanding database structure, typical documents, and workload patterns.

---
## FAQ
1. What is the purpose of this toolset?
  - In order to help customers of course 🦄 (TBD)
2. Why is the metadata extract written in shell script?
  - Providing a shell script that invokes native MongoDB tools such as `mongosh` reduces additional dependencies and supply chain concerns. By providing a simple shell script, a customer's security team can review the functions and data collection without the need to reverse engineer or decompile the solution.
3. What can I do with the `restoreMongo.py` script?
  - The `restoreMongo.py` script will attempt to recreate MongoDB databases and the associated collections. Customers can navigate the directory structure and rename folders that represent either databases or collections based on their needs. To create new databases or collections, simply copy the folder structure to the appropriate base directory. Additionally, customers can edit the `example_document.json` and/or `schema_and_indexes.json` to remove any data they feel is sensitive or proprietary.
4. Does the extraction process modify my MongoDB data?
  - No, the extraction process is entirely read-only and does not modify your MongoDB data in any way.
5. How are the extraction logs organized?
  - The script creates a timestamped log file in the output directory that captures all operations during the extraction process, including errors and successful extractions.
6. What happens if the script encounters an error during extraction?
  - The script implements robust error handling. If an error occurs during extraction of a specific piece of metadata, that operation will be marked as failed, but the script will continue with other extraction tasks. A detailed error message will be logged, and the collection's summary.json will indicate a "partial" extraction status.