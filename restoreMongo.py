# Script Purpose - Planning
#
# Author: Matt DeMarco (matthew.demarco@oracle.com)
#
# This script will generate data from a sample document or schema information gathered from a MongoDB database
# In order to use this script please run extractMongo.sh first and run from the same directory where mongodb_metadata is located
# The data will be used for analysis for 23ai support of migrating a MongoDB workload
# Please see README.md for detailed explanation
#




import os
import json
import random
from faker import Faker
from pymongo import MongoClient, IndexModel
from bson.decimal128 import Decimal128

# Configuration: MongoDB connection details
MONGO_HOST = "localhost"
MONGO_PORT = 23456
DATABASES_DIR = "mongodb_metadata"  # Directory containing metadata
USERNAME = ""  # MongoDB username (if needed)
PASSWORD = ""  # MongoDB password (if needed)
AUTH_DB = ""   # Authentication database (if needed)

# MongoDB Connection
if USERNAME and PASSWORD:
    mongo_uri = f"mongodb://{USERNAME}:{PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/{AUTH_DB}?authSource={AUTH_DB}"
else:
    mongo_uri = f"mongodb://{MONGO_HOST}:{MONGO_PORT}"

client = MongoClient(mongo_uri)

# Initialize Faker for synthetic data
faker = Faker()


def load_json(file_path):
    """Load JSON file safely, handling empty or invalid files."""
    if not os.path.exists(file_path):
        print(f"Warning: {file_path} not found. Skipping...")
        return None

    if os.path.getsize(file_path) == 0:
        print(f"Warning: {file_path} is empty. Skipping...")
        return None

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        print(f"Error: Failed to parse {file_path} - {e}")
        return None


def generate_synthetic_document(schema):
    """Generate a synthetic document using Faker based on schema types, including Geospatial data."""
    fake_doc = {}

    for field, types in schema.items():
        try:
            if "string" in types:
                fake_doc[field] = faker.word()
            elif "int" in types:
                fake_doc[field] = random.randint(1, 100)
            elif "long" in types:
                fake_doc[field] = random.randint(1000000000, 9999999999)
            elif "double" in types:
                fake_doc[field] = round(random.uniform(1.0, 1000.0), 2)
            elif "Decimal128" in types:
                fake_doc[field] = Decimal128(str(round(random.uniform(-1000.0, 1000.0), 6)))  # Convert Decimal to BSON Decimal128
            elif "boolean" in types:
                fake_doc[field] = random.choice([True, False])
            elif "date" in types:
                fake_doc[field] = faker.date_time().isoformat()
            elif "array" in types:
                fake_doc[field] = [faker.word() for _ in range(3)]
            elif "object" in types:
                fake_doc[field] = {"nested_key": faker.word()}
            elif "ObjectId" in types:
                fake_doc[field] = faker.uuid4()[:24]  # Simulating a MongoDB ObjectId
            elif "null" in types:
                fake_doc[field] = None

            # Handle Geospatial types
            elif "GeoJSON Point" in types:
                fake_doc[field] = {"type": "Point", "coordinates": [faker.longitude(), faker.latitude()]}
            elif "GeoJSON LineString" in types:
                fake_doc[field] = {
                    "type": "LineString",
                    "coordinates": [
                        [faker.longitude(), faker.latitude()],
                        [faker.longitude(), faker.latitude()],
                        [faker.longitude(), faker.latitude()]
                    ]
                }
            elif "GeoJSON Polygon" in types:
                fake_doc[field] = {
                    "type": "Polygon",
                    "coordinates": [[
                        [faker.longitude(), faker.latitude()],
                        [faker.longitude(), faker.latitude()],
                        [faker.longitude(), faker.latitude()],
                        [faker.longitude(), faker.latitude()]  # Closing point same as first
                    ]]
                }
            else:
                fake_doc[field] = "unknown"
        except Exception as e:
            print(f"Error generating field '{field}': {e}")

    return fake_doc


def recreate_collection(db_name, coll_name, coll_folder):
    """Recreate a MongoDB collection using sample document or schema metadata."""
    db = client[db_name]

    # Drop collection if it already exists
    if coll_name in db.list_collection_names():
        print(f"Collection {db_name}.{coll_name} already exists. Dropping it first...")
        db[coll_name].drop()

    # Load metadata files
    sample_doc_path = os.path.join(coll_folder, "example_document.json")
    schema_path = os.path.join(coll_folder, "schema_and_indexes.json")

    sample_data = load_json(sample_doc_path)
    sample_document = sample_data.get("sample_document", None) if sample_data else None
    schema_metadata = load_json(schema_path) if not sample_document else None

    # Insert sample document or generate synthetic data
    try:
        if sample_document:
            print(f"Inserting sample document into {db_name}.{coll_name}...")
            db[coll_name].insert_one(sample_document)
        elif schema_metadata and "schema" in schema_metadata:
            print(f"Generating synthetic document for {db_name}.{coll_name} using Faker...")
            fake_document = generate_synthetic_document(schema_metadata["schema"])
            db[coll_name].insert_one(fake_document)
        else:
            print(f"Skipping {db_name}.{coll_name} (No valid metadata found)")
            return
    except Exception as e:
        print(f"Error inserting document into {db_name}.{coll_name} (from {schema_path if schema_metadata else sample_doc_path}): {e}")
        print(f"Problematic data: {fake_document if 'fake_document' in locals() else sample_document}")

    print(f"Collection {db_name}.{coll_name} recreated successfully!")


def main():
    """Main function to recreate MongoDB collections from metadata."""
    if not os.path.exists(DATABASES_DIR):
        print(f"Error: Metadata directory '{DATABASES_DIR}' not found!")
        return

    for db_name in os.listdir(DATABASES_DIR):
        db_folder = os.path.join(DATABASES_DIR, db_name)

        #print(db_folder)
        print("--------------------------------------------")
        print(f"Restoring data from: {db_folder}")

        if not os.path.isdir(db_folder):
            continue  # Skip if not a directory

        print(f"Processing database: {db_name}")

        for coll_name in os.listdir(db_folder):
            coll_folder = os.path.join(db_folder, coll_name)

            if not os.path.isdir(coll_folder):
                continue  # Skip if not a directory

            recreate_collection(db_name, coll_name, coll_folder)

        print(f"Database restoration complete: {db_name}")

    print("All collections have been recreated!")


if __name__ == "__main__":
    main()
