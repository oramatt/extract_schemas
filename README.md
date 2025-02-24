# MongoDB Metadata Extraction and Restoration

## Overview
This repository provides two scripts for working with MongoDB metadata extraction and data restoration:

1. **`extractMongo.sh`**: Extracts metadata, schema details, sample documents, storage statistics, system performance data, and workload profiling information from a MongoDB instance.
2. **`restoreMongo.py`**: Recreates MongoDB collections using extracted metadata and sample documents, falling back to schema definitions if necessary.

## Features
### extractMongo.sh
- **Extract Document Count**: Retrieves an estimated count of documents per collection.
- **Extract Storage Statistics**: Captures collection size, index size, and storage statistics.
- **Extract System Information**: Fetches MongoDB version, CPU usage, memory, disk IOPS, and network metrics.
- **Extract Sample Document**: Retrieves a single sample document from each collection and saves it as `example_document.json`.
- **Extract Schema and Indexes**: Identifies field types and index structures in collections.
- **Extract Workload Profile**:
  - If the **MongoDB profiler** is enabled (`system.profile`), fetches profiling data.
  - If the profiler is **disabled**, retrieves workload data from **MongoDB in-memory logs** (`getLog: 'global'`).

### restoreMongo.py
- **Recreates Collections**: Restores collections from extracted metadata.
- **Uses Sample Documents**: If available, uses `example_document.json` to structure collections.
- **Falls Back to Schema**: If no sample document exists, it generates synthetic data using MongoDB schema metadata from `schema_and_indexes.json`.
- **Supports Index Recreation**: Ensures indexes are rebuilt during restoration.
- **Handles Geospatial Data**: Correctly processes GeoJSON types such as `Point`, `LineString`, and `Polygon`.
- **Synthetic Data Generation**: Uses the `faker` library to generate missing sample documents based on schema definitions.
- **Handles Data Encoding Issues**: Fixes BSON encoding issues such as `Decimal128` conversions.
- **Parallel Processing**: Can restore multiple collections concurrently.
- **Verbose Logging**: Provides detailed logs for errors and actions performed.

## Prerequisites
- **MongoDB shell (`mongosh`)** must be installed and accessible.
- **Python 3.x** is required to run `restoreMongo.py`.
- The user must have **read access** to the targeted databases for extraction.
- The user must have **write access** to MongoDB for restoration.
- Optional: If authentication is required, provide a valid username, password, and authentication database.

## Configuration
### extractMongo.sh
Edit the script variables to define the MongoDB connection details and databases to scan:

```bash
MONGO_HOST="localhost"
MONGO_PORT="23456"
DATABASES=("test" "yelp")  # List of databases to scan
OUTPUT_DIR="mongodb_metadata"  # Output directory for extracted metadata
PARALLEL_LIMIT=4  # Maximum parallel jobs

# Authentication (if required)
USERNAME=""  # MongoDB username
PASSWORD=""  # MongoDB password
AUTH_DB=""   # Authentication database (e.g., "admin")
```

### restoreMongo.py
Edit the script variables to define MongoDB connection details:

```bash
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
This will create structured JSON files containing metadata and workload analysis.

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
```

### Restore Collections
To recreate collections from extracted metadata, use:
```bash
python3 restoreMongo.py
```
This script scans the extracted metadata and restores collections using either sample documents or schema information.


## Conclusion
These scripts assist in analyzing MongoDB workloads and restoring collections efficiently. The extracted metadata aids in migration planning and workload analysis, ensuring seamless transitions to other environments such as Oracle 23ai.

---
## FAQ
1. What is the purpose of this toolset?
  - In order to help customers of course 🦄 (TBD)
2.  Why is the metadata extract written in shell script?
  - Providing a shell script that invokes native MongoDB tools such as `mongosh` reduces additional dependancies and supply chain concerns. By providing a simple shell script, a customer's security can review the functions and data collection without the need to reverse engineer or decompile the solution.
3.  What can I do with the `restoreMongo.py` script?
  - The `restoreMongo.py` script will attempt to recreate MongoDB databases and the associated collections. Customers can navigate the directory structure and rename folders that represent either databases or collections based on their needs. To create new databases or collections, simply copy the folder structure to the approipate base directory. Additionally, customers can edit the `example_document.json` and/or `schema_and_indexes.json` to remove any data they feel is sensitive or proprietary.


