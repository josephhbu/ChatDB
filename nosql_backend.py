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

            # Open and read the JSON file line by line
            with open(json_file, 'r') as file:
                data = []
                for line in file:
                    line = line.strip() 
                    if line:  
                        try:
                            json_object = json.loads(line)
                            data.append(json_object)
                        except json.JSONDecodeError as e:
                            print(f"Error decoding JSON in {json_file}: {e}")
                            continue

                if data:
                    collection.insert_many(data) 

            print(f"Data from {json_file} successfully imported into {db_name}.{collection_name}")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python nosql_backend.py <db_name> <json_file1> [<json_file2> ... <json_fileN>]")
        sys.exit(1)

    db_name = sys.argv[1]
    json_files = sys.argv[2:]

    import_multiple_json_to_mongodb(json_files, db_name)
