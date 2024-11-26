import re
from pymongo import MongoClient
import ast

# MongoDB setup
client = MongoClient("mongodb://localhost:27017/")
db = client["Shootings"]

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

    # match = re.match(r"how many victims were (over|under|between) (\d+)(?: and (\d+))?", query)
    # if match:
    #     condition, age1, age2 = match.groups()
    #     age1 = int(age1)
    #     age2 = int(age2) if age2 else None

    #     # Handle "over" condition
    #     if condition == "over":
    #         return {
    #         "operation": "count",
    #         "collection": "victim",  # Lowercase collection name
    #         "filter": {
    #             "$and": [
    #                 {"age": {"$exists": True, "$ne": ""}},  # Ensure `age` is non-empty
    #                 {"$expr": {"$gt": [{"$toInt": "$age"}, age1]}}  # Dynamically convert `age` to int and compare
    #                 ]
    #             }
    #         }

    #     # Handle "under" condition
    #     elif condition == "under":
    #         return {
    #         "operation": "count",
    #         "collection": "victim",  # Lowercase collection name
    #         "filter": {
    #             "$and": [
    #                 {"age": {"$exists": True, "$ne": ""}},  # Ensure `age` is non-empty
    #                 {"$expr": {"$lt": [{"$toInt": "$age"}, age1]}}  # Dynamically convert `age` to int and compare
    #                 ]
    #             }
    #         }

    #     # Handle "between" condition
    #     elif condition == "between":
    #         if age2 is None:
    #             raise ValueError("The 'between' condition requires two age values.")
    #         return {
    #         "operation": "count",
    #         "collection": "victim",  # Lowercase collection name
    #         "filter": {
    #             "$and": [
    #                 {"age": {"$exists": True, "$ne": ""}},  # Ensure `age` is non-empty
    #                 {"$expr": {
    #                     "$and": [
    #                         {"$gte": [{"$toInt": "$age"}, age1]},  # Convert `age` to int and check >= age1
    #                         {"$lte": [{"$toInt": "$age"}, age2]}   # Convert `age` to int and check <= age2
    #                         ]
    #                     }}
    #                 ]
    #             }
    #         }

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

def execute_query(parsed_query):
    if "error" in parsed_query:
        return parsed_query["error"]

    if parsed_query["operation"] == "show_collections":
        return db.list_collection_names()

    if parsed_query["operation"] == "count":
        collection = db[parsed_query["collection"]]
        return collection.count_documents(parsed_query["filter"])

    if parsed_query["operation"] == "join_count":
        collection = db[parsed_query["collection"]]
        pipeline = [
            {"$lookup": parsed_query["lookup"]},
            {"$unwind": "$victim_data"},
            {"$match": parsed_query["filter"]},
            {"$count": "total"}
        ]
        result = list(collection.aggregate(pipeline))
        return result[0]["total"] if result else 0

    if parsed_query["operation"] == "raw_mongo":
        try:
            # Evaluate the query safely using Python's literal_eval
            raw_query = ast.literal_eval(parsed_query["query"])
            collection_name = raw_query.pop("collection")
            collection = db[collection_name]
            return list(collection.find(raw_query))
        except Exception as e:
            return f"Error executing raw MongoDB query: {str(e)}"

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
