import sys
import json
from pymongo import MongoClient
import os

def import_multiple_json_to_mongodb(json_files, db_name):
    try:
        # Connect to MongoDB
        client = MongoClient('mongodb://localhost:27017/')
        db = client[db_name]

        for json_file in json_files:
            # Use the filename as the collection name
            collection_name = os.path.splitext(os.path.basename(json_file))[0]
            collection = db[collection_name]

            with open(json_file, 'r') as file:
                data = json.load(file)

            # Ensure data format is correct
            if isinstance(data, list):
                collection.insert_many(data)  # Insert a list of documents
            elif isinstance(data, dict):
                collection.insert_one(data)  # Insert a single document
            else:
                print(f"Unsupported JSON format in file {json_file}: {type(data)}")
                continue

            print(f"Data from {json_file} successfully imported into {db_name}.{collection_name}")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python import_multiple_json_to_mongodb.py <db_name> <json_file1> [<json_file2> ... <json_fileN>]")
        sys.exit(1)

    db_name = sys.argv[1]
    json_files = sys.argv[2:]

    import_multiple_json_to_mongodb(json_files, db_name)
