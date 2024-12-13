import sys
import json
import pandas as pd
from pymongo import MongoClient
import os
import re

def csv_to_json(csv_file_path, json_file_path=None):
    try:
        df = pd.read_csv(csv_file_path, encoding="utf-8")
        df = df.dropna(how="all")  
        df = df.reset_index(drop=True)  

        json_data = json.loads(df.to_json(orient="records"))
        if json_file_path:
            with open(json_file_path, "w") as outfile:
                json.dump(json_data, outfile, indent=4)
            print(f"JSON file saved at: {json_file_path}")

        return json_data
    except pd.errors.ParserError as e:
        print(f"Parsing error during CSV to JSON conversion: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"JSON encoding error: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None

def import_multiple_json_to_mongodb(json_files, db_name):
    try:
        # Connect to MongoDB
        client = MongoClient('mongodb://localhost:27017/')
        db = client[db_name]

        for json_file in json_files:
            # Use the filename as the collection name (without extension)
            collection_name = os.path.splitext(os.path.basename(json_file))[0].lower()
            collection = db[collection_name]

            with open(json_file, 'r') as file:
                try:
                    data = json.load(file)
                    if isinstance(data, dict): 
                        data = [data]
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON in {json_file}: {e}")
                    continue

            # Insert data into the collection
            if data:
                if isinstance(data, list):
                    collection.insert_many(data) 
                else:
                    collection.insert_one(data) 

            print(f"Data from {json_file} successfully imported into {db_name}.{collection_name}")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    csv_file_paths = ["data/INCIDENT.csv", "data/SHOOTER.csv", "data/VICTIM.csv"]
    json_file_paths = []
    for file in csv_file_paths:
        match = re.search(r'/([A-Z]+)\.csv$', file)
        if match:
            result = match.group(1)
        json_file_path = f"data/{result}.json"
        json_file = csv_to_json(file, json_file_path)
        if json_file is not None:
            json_file_paths.append(json_file_path)

    db_name='chatDB'
    json_files = json_file_paths
    import_multiple_json_to_mongodb(json_files, db_name)

    # Old code for running on command line
    # if len(sys.argv) < 3:
    #     print("Usage: python nosql_backend.py <db_name> <json_file1> [<json_file2> ... <json_fileN>]")
    #     sys.exit(1)
    #db_name = sys.argv[1]
    #json_files = sys.argv[2:]
