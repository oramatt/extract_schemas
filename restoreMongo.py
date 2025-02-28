#!/usr/bin/env python3

# Script Purpose - MongoDB Metadata Restoration
#
# Author: Matt DeMarco (matthew.demarco@oracle.com)
#
# This script will restore MongoDB collections from metadata previously extracted
# using extractMongo.sh. It can restore using example documents or generate
# synthetic data based on schema information.
# In order to use this script please run extractMongo.sh first and run from the 
# same directory where mongodb_metadata is located
#

import os
import sys
import json
import random
import logging
from datetime import datetime
from faker import Faker
from pymongo import MongoClient, IndexModel, GEOSPHERE, ASCENDING, DESCENDING
from pymongo.errors import ConnectionFailure, OperationFailure
from bson.decimal128 import Decimal128
from bson.objectid import ObjectId

# Configure logging
current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
log_filename = f"restore_mongo_{current_time}.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("restoreMongo")

# Configuration: MongoDB connection details
MONGO_HOST = "localhost"
MONGO_PORT = 23456
DATABASES_DIR = "mongodb_metadata"  # Directory containing metadata
USERNAME = ""  # MongoDB username (if needed)
PASSWORD = ""  # MongoDB password (if needed)
AUTH_DB = ""   # Authentication database (if needed)
MONGO_TLS = False  # Set to True to enable TLS/SSL
SYNTHETIC_DOCS_COUNT = 10  # Number of synthetic documents to generate when using schema

# MongoDB Connection
try:
    if USERNAME and PASSWORD:
        mongo_uri = f"mongodb://{USERNAME}:{PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/{AUTH_DB}?authSource={AUTH_DB}"
        if MONGO_TLS:
            mongo_uri += "&tls=true"
    else:
        mongo_uri = f"mongodb://{MONGO_HOST}:{MONGO_PORT}"
        if MONGO_TLS:
            mongo_uri += "?tls=true"

    client = MongoClient(mongo_uri)
    # Verify connection
    client.admin.command('ping')
    logger.info(f"Connected to MongoDB at {MONGO_HOST}:{MONGO_PORT}")
    
    # Get server info for compatibility check
    server_info = client.server_info()
    mongo_version = server_info.get('version', '0.0.0')
    logger.info(f"MongoDB server version: {mongo_version}")
    
    # Check MongoDB version compatibility (requires 4.0+)
    major_version = int(mongo_version.split('.')[0])
    if major_version < 4:
        logger.warning(f"MongoDB version {mongo_version} may have compatibility issues. Version 4.0+ recommended.")
        
except ConnectionFailure as e:
    logger.error(f"Failed to connect to MongoDB: {e}")
    sys.exit(1)

# Initialize Faker for synthetic data
faker = Faker()


def load_json(file_path):
    """Load JSON file safely, handling empty or invalid files."""
    if not os.path.exists(file_path):
        logger.warning(f"File not found: {file_path}")
        return None

    if os.path.getsize(file_path) == 0:
        logger.warning(f"Empty file: {file_path}")
        return None

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse {file_path}: {e}")
        return None


def generate_synthetic_document(schema, include_id=False):
    """Generate a synthetic document using Faker based on schema types.
    
    Args:
        schema: Dictionary containing field names and their types
        include_id: Whether to include _id field (defaults is False to let MongoDB generate it)
    """
    fake_doc = {}

    for field, types in schema.items():
        try:
            # Skip processing nested fields at the top level since they'll be handled by their parent
            if "." in field:
                continue
                
            # Skip the _id field unless explicitly requested
            if field == "_id" and not include_id:
                continue
                
            # Handle different data types
            if "string" in types:
                if "email" in field.lower():
                    fake_doc[field] = faker.email()
                elif "name" in field.lower():
                    fake_doc[field] = faker.name()
                elif "city" in field.lower():
                    fake_doc[field] = faker.city()
                elif "address" in field.lower():
                    fake_doc[field] = faker.address()
                elif "phone" in field.lower():
                    fake_doc[field] = faker.phone_number()
                elif "url" in field.lower() or "website" in field.lower():
                    fake_doc[field] = faker.url()
                elif "date" in field.lower() and not any(t in types for t in ["date", "timestamp"]):
                    fake_doc[field] = faker.date().isoformat()
                elif "description" in field.lower() or "notes" in field.lower() or "comment" in field.lower():
                    fake_doc[field] = faker.paragraph()
                else:
                    fake_doc[field] = faker.word()
            elif "int" in types:
                # Use context clues to generate appropriate integer values
                if "age" in field.lower():
                    fake_doc[field] = random.randint(18, 80)
                elif "year" in field.lower():
                    fake_doc[field] = random.randint(1950, 2025)
                elif "count" in field.lower() or "quantity" in field.lower():
                    fake_doc[field] = random.randint(1, 1000)
                else:
                    fake_doc[field] = random.randint(1, 100)
            elif "long" in types:
                fake_doc[field] = random.randint(1000000000, 9999999999)
            elif "double" in types:
                if "price" in field.lower() or "cost" in field.lower() or "fee" in field.lower():
                    fake_doc[field] = round(random.uniform(1.0, 1000.0), 2)
                elif "rating" in field.lower() or "score" in field.lower():
                    fake_doc[field] = round(random.uniform(0.0, 5.0), 1)
                else:
                    fake_doc[field] = round(random.uniform(1.0, 1000.0), 2)
            elif "Decimal128" in types:
                fake_doc[field] = Decimal128(str(round(random.uniform(-1000.0, 1000.0), 6)))
            elif "boolean" in types:
                fake_doc[field] = random.choice([True, False])
            elif "date" in types:
                fake_doc[field] = faker.date_time().isoformat()
            elif "array" in types:
                if "tags" in field.lower() or "categories" in field.lower():
                    fake_doc[field] = [faker.word() for _ in range(random.randint(1, 5))]
                else:
                    fake_doc[field] = [faker.word() for _ in range(3)]
            elif "object" in types:
                # Handle specific object types
                if field == "location" and schema.get("location.type") and "Point" in schema.get("location.type", []):
                    fake_doc[field] = {
                        "type": "Point",
                        "coordinates": [
                            float(faker.longitude()),
                            float(faker.latitude())
                        ]
                    }
                else:
                    # Generic nested object
                    fake_doc[field] = {"nested_key": faker.word()}
                    
                    # Process nested fields if they exist
                    nested_prefix = f"{field}."
                    nested_fields = {k[len(nested_prefix):]: v for k, v in schema.items() 
                                    if k.startswith(nested_prefix)}
                    
                    if nested_fields:
                        nested_doc = generate_synthetic_document(nested_fields)
                        fake_doc[field].update(nested_doc)
            elif "ObjectId" in types:
                fake_doc[field] = str(ObjectId())
            elif "null" in types:
                fake_doc[field] = None
            # Handle Geospatial types
            elif any("GeoJSON" in t for t in types):
                if "Point" in str(types):
                    fake_doc[field] = {
                        "type": "Point",
                        "coordinates": [float(faker.longitude()), float(faker.latitude())]
                    }
                elif "LineString" in str(types):
                    fake_doc[field] = {
                        "type": "LineString",
                        "coordinates": [
                            [float(faker.longitude()), float(faker.latitude())],
                            [float(faker.longitude()), float(faker.latitude())],
                            [float(faker.longitude()), float(faker.latitude())]
                        ]
                    }
                elif "Polygon" in str(types):
                    # Create a simple polygon (points must form a closed loop)
                    lng, lat = float(faker.longitude()), float(faker.latitude())
                    fake_doc[field] = {
                        "type": "Polygon",
                        "coordinates": [[
                            [lng, lat],
                            [lng + 0.1, lat],
                            [lng + 0.1, lat + 0.1],
                            [lng, lat + 0.1],
                            [lng, lat]  # Close the loop
                        ]]
                    }
            else:
                fake_doc[field] = "unknown"
        except Exception as e:
            logger.error(f"Error generating field '{field}': {e}")
            fake_doc[field] = None

    return fake_doc


def restore_indexes(db, coll_name, schema_metadata):
    """Recreate indexes from metadata."""
    if not schema_metadata or "indexes" not in schema_metadata:
        logger.warning(f"No index metadata found for {db.name}.{coll_name}")
        return

    logger.info(f"Restoring indexes for {db.name}.{coll_name}...")
    
    for index in schema_metadata["indexes"]:
        # Skip _id index as it's created automatically
        if index["name"] == "_id_":
            continue
            
        try:
            # Handle different index types
            key_items = []
            for key, value in index["key"].items():
                # Handle special index types
                if value == "2dsphere":
                    key_items.append((key, GEOSPHERE))
                elif value == 1:
                    key_items.append((key, ASCENDING))
                elif value == -1:
                    key_items.append((key, DESCENDING))
                else:
                    key_items.append((key, value))
            
            # Extract index options (exclude MongoDB internal fields)
            options = {k: v for k, v in index.items() 
                      if k not in ["v", "key", "ns"]}
            
            # Create the index
            db[coll_name].create_index(key_items, **options)
            logger.info(f"  - Created index: {index['name']}")
        except Exception as e:
            logger.error(f"  - Error creating index {index['name']}: {e}")


def recreate_collection(db_name, coll_name, coll_folder):
    """Recreate a MongoDB collection using sample document or schema metadata."""
    db = client[db_name]

    # Drop collection if it already exists
    if coll_name in db.list_collection_names():
        logger.info(f"Collection {db_name}.{coll_name} already exists. Dropping it...")
        db[coll_name].drop()

    # Load metadata files
    sample_doc_path = os.path.join(coll_folder, "example_document.json")
    schema_path = os.path.join(coll_folder, "schema_and_indexes.json")

    # Check for sample document first
    sample_data = load_json(sample_doc_path)
    sample_document = sample_data.get("sample_document", None) if sample_data else None
    
    # If no sample document, try to use schema
    schema_metadata = None
    if not sample_document:
        schema_metadata = load_json(schema_path)
        if not schema_metadata or "schema" not in schema_metadata:
            logger.error(f"Skipping {db_name}.{coll_name} (No valid metadata found)")
            return False

    # Insert document(s) into the collection
    try:
        if sample_document:
            # Use the sample document but remove the _id field to let MongoDB generate a new one
            document_to_insert = sample_document.copy()
            if '_id' in document_to_insert:
                logger.info(f"Removing existing _id field from sample document")
                del document_to_insert['_id']
                
            # Insert the document
            logger.info(f"Inserting sample document into {db_name}.{coll_name}...")
            result = db[coll_name].insert_one(document_to_insert)
            logger.info(f"Inserted sample document with new ID: {result.inserted_id}")
            
            # Also restore indexes if we have schema metadata
            if not schema_metadata:
                schema_metadata = load_json(schema_path)
            
        elif schema_metadata and "schema" in schema_metadata:
            # Generate synthetic documents based on schema
            logger.info(f"Generating {SYNTHETIC_DOCS_COUNT} synthetic documents for {db_name}.{coll_name} using Faker...")
            
            # Generate multiple synthetic documents (without _id fields)
            synthetic_docs = []
            for i in range(SYNTHETIC_DOCS_COUNT):
                # Pass include_id=False to let MongoDB generate _id values
                fake_document = generate_synthetic_document(schema_metadata["schema"], include_id=False)
                synthetic_docs.append(fake_document)
            
            # Insert the synthetic documents
            result = db[coll_name].insert_many(synthetic_docs)
            logger.info(f"Inserted {len(result.inserted_ids)} synthetic documents")
        else:
            logger.error(f"Skipping {db_name}.{coll_name} (No valid metadata found)")
            return False
            
        # Restore indexes
        if schema_metadata:
            restore_indexes(db, coll_name, schema_metadata)
        
        # Validate the restoration
        doc_count = db[coll_name].count_documents({})
        logger.info(f"Collection {db_name}.{coll_name} recreated with {doc_count} documents")
        return True
        
    except Exception as e:
        logger.error(f"Error recreating {db_name}.{coll_name}: {e}")
        if 'fake_document' in locals():
            logger.error(f"Problematic data: {fake_document}")
        elif 'sample_document' in locals():
            logger.error(f"Problematic data: {sample_document}")
        return False


def check_compatibility():
    """Check MongoDB version compatibility."""
    try:
        server_info = client.server_info()
        version = server_info.get('version', '0.0.0')
        logger.info(f"Target MongoDB version: {version}")
        
        # Extract major version
        major_version = int(version.split('.')[0])
        
        if major_version < 4:
            logger.warning("Target MongoDB version is below 4.0, which may cause compatibility issues")
        
        return True
    except Exception as e:
        logger.error(f"Error connecting to MongoDB: {e}")
        return False


def main():
    """Main function to recreate MongoDB collections from metadata."""
    logger.info(f"=== MongoDB Metadata Restoration Started at {datetime.now().isoformat()} ===")
    
    # Check compatibility
    if not check_compatibility():
        logger.error("Compatibility check failed. Exiting.")
        return

    if not os.path.exists(DATABASES_DIR):
        logger.error(f"Metadata directory '{DATABASES_DIR}' not found!")
        return

    # Track statistics
    stats = {
        "databases_processed": 0,
        "collections_processed": 0,
        "collections_restored": 0,
        "errors": 0
    }

    for db_name in os.listdir(DATABASES_DIR):
        db_folder = os.path.join(DATABASES_DIR, db_name)

        if not os.path.isdir(db_folder):
            continue  # Skip if not a directory

        logger.info(f"Processing database: {db_name}")
        stats["databases_processed"] += 1

        for coll_name in os.listdir(db_folder):
            coll_folder = os.path.join(db_folder, coll_name)

            if not os.path.isdir(coll_folder):
                continue  # Skip if not a directory

            logger.info(f"Processing collection: {db_name}.{coll_name}")
            stats["collections_processed"] += 1

            if recreate_collection(db_name, coll_name, coll_folder):
                stats["collections_restored"] += 1
            else:
                stats["errors"] += 1

        logger.info(f"Database restoration complete: {db_name}")

    logger.info("=== Restoration Summary ===")
    logger.info(f"Databases processed: {stats['databases_processed']}")
    logger.info(f"Collections processed: {stats['collections_processed']}")
    logger.info(f"Collections successfully restored: {stats['collections_restored']}")
    logger.info(f"Errors encountered: {stats['errors']}")
    logger.info(f"=== MongoDB Metadata Restoration Completed at {datetime.now().isoformat()} ===")


if __name__ == "__main__":
    main()