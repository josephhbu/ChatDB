import re
from pymongo import MongoClient
import ast
import json

# MongoDB setup
client = MongoClient("mongodb://localhost:27017/")
db = client["chatDB"]

# Utility functions
def parse_query(query):
    query = query.strip()

    # Check if the query is a valid MongoDB query
    if is_mongo_query(query):
        return {"operation": "raw_mongo", "query": query}

    # Natural language processing for specific queries
    query = query.lower()

    if re.match(r"show collections", query):
        return {"operation": "show_collections"}

    # Count shooters by gender
    match = re.match(r"count how many shooters are (\w+)", query)
    if match:
        gender = match.group(1)
        return {
        "operation": "count",
        "collection": "shooter",  # Match the actual collection name
        "filter": {"gender": {"$regex": f"^{gender}$", "$options": "i"}}  # Case-insensitive match
        }
    
    # Count victims with specific injury
    match = re.match(r"how many victims were (\w+)", query)
    if match:
        injury = match.group(1)
        return {
        "operation": "count",
        "collection": "victim",  # Match the exact name of the collection
        "filter": {"injury": {"$regex": f"^{injury}$", "$options": "i"}}  # Case-insensitive match
        }

    # Query: Count incidents by location
    match = re.match(r"how many incidents occurred in (\w+)", query)
    if match:
        location = match.group(1).upper()  # Standardize location to uppercase
        return {
        "operation": "count",
        "collection": "incident",  # Lowercase collection name
        "filter": {"State": location}
        }

    # Query: Join query for incidents with male shooter and female victim
    match = re.match(r"how many incidents had a (\w+) shooter and a (\w+) victim", query)
    if match:
        shooter_gender, victim_gender = match.groups()
        return {
        "operation": "join_count",
        "lookup": {
            "from": "victim",  # Lowercase collection name for lookup
            "localField": "incidentid",  # Match field for joining
            "foreignField": "incidentid",
            "as": "victim_data"
        },
        "collection": "shooter",  # Lowercase main collection name
        "filter": {
            "gender": {"$regex": f"^{shooter_gender}$", "$options": "i"},  # Case-insensitive shooter gender
            "victim_data.gender": {"$regex": f"^{victim_gender}$", "$options": "i"}  # Case-insensitive victim gender
        }
        }

    # Fallback for unmatched query
    return "Query not recognized or improperly formatted."

def is_mongo_query(query):
    """
    Heuristic to determine if the query is in MongoDB NoSQL syntax.
    """
    try:
        # Try parsing the query to see if it's valid MongoDB syntax
        ast.literal_eval(query)
        return True
    except Exception:
        return False

def is_mongo_query(query):
    """
    Check if the query is a valid MongoDB query.
    
    Returns:
        bool: True if it's a valid MongoDB query, False otherwise.
    """
    query = query.strip()

    # MongoDB queries typically start with "db.<collection>.<operation>"
    if query.startswith("db."):
        return True

    try:
        # Check if it's valid JSON
        json.loads(query)
        return True
    except json.JSONDecodeError:
        pass

    # If none of the above checks pass, it's not a MongoDB query
    return False

def execute_query(parsed_query):
    if "error" in parsed_query:
        return {"query": None, "result": parsed_query["error"]}

    if parsed_query["operation"] == "show_collections":
        return {"query": "db.list_collection_names()", "result": db.list_collection_names()}

    if parsed_query["operation"] == "count":
        collection = db[parsed_query["collection"]]
        mongo_query = f'db.{parsed_query["collection"]}.count_documents({parsed_query["filter"]})'
        result = collection.count_documents(parsed_query["filter"])
        return {"query": mongo_query, "result": result}

    if parsed_query["operation"] == "join_count":
        collection = db[parsed_query["collection"]]
        pipeline = [
            {"$lookup": parsed_query["lookup"]},
            {"$unwind": "$victim_data"},
            {"$match": parsed_query["filter"]},
            {"$count": "total"}
        ]
        mongo_query = f'db.{parsed_query["collection"]}.aggregate({pipeline})'
        result = list(collection.aggregate(pipeline))
        count_result = result[0]["total"] if result else 0
        return {"query": mongo_query, "result": count_result}

    if parsed_query["operation"] == "raw_mongo":
        try:
            # Extract the collection and operation from the raw query
            raw_query = parsed_query["query"].strip()

            # Extract collection and operation dynamically
            if raw_query.startswith("db."):
                collection_name = raw_query.split(".")[1]
                command = raw_query.split(".", 2)[2]  # Operation and its parameters
                collection = db[collection_name]

                # Execute the operation dynamically
                exec_result = eval(f"collection.{command}")
                return {"query": raw_query, "result": list(exec_result) if hasattr(exec_result, "__iter__") else exec_result}

            return {"query": None, "result": "Invalid MongoDB raw query format"}
        except Exception as e:
            return {"query": None, "result": f"Error executing raw MongoDB query: {str(e)}"}

    return {"query": None, "result": "Unsupported operation"}

# Main loop
def main():
    print("Enter your query (or type 'exit' to quit):")
    while True:
        query = input("> ")
        if query.lower() == "exit":
            break
        parsed_query = parse_query(query)
        result = execute_query(parsed_query)
        print(result)

if __name__ == "__main__":
    main()
