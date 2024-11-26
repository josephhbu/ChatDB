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
            "collection": "Shooter",
            "filter": {"gender": gender}
        }

    # Count victims with specific injury
    match = re.match(r"how many victims were (\w+)", query)
    if match:
        injury = match.group(1)
        return {
            "operation": "count",
            "collection": "Victim",
            "filter": {"injury": {"$regex": injury, "$options": "i"}}
        }

    # Count victims by age condition
    match = re.match(r"how many victims were (over|under|between) (\d+)(?: and (\d+))?", query)
    if match:
        condition, age1, age2 = match.groups()
        if condition == "over":
            return {
                "operation": "count",
                "collection": "Victim",
                "filter": {"age": {"$gt": int(age1)}}
            }
        elif condition == "under":
            return {
                "operation": "count",
                "collection": "Victim",
                "filter": {"age": {"$lt": int(age1)}}
            }
        elif condition == "between":
            return {
                "operation": "count",
                "collection": "Victim",
                "filter": {"age": {"$gte": int(age1), "$lte": int(age2)}}
            }

    # Count incidents by location
    match = re.match(r"how many incidents occurred in (\w+)", query)
    if match:
        location = match.group(1).upper()
        return {
            "operation": "count",
            "collection": "Incident",
            "filter": {"location": location}
        }

    # Join query for incidents with male shooter and female victim
    match = re.match(r"how many incidents had a (\w+) shooter and a (\w+) victim", query)
    if match:
        shooter_gender, victim_gender = match.groups()
        return {
            "operation": "join_count",
            "lookup": {
                "from": "Victim",
                "localField": "incident_id",
                "foreignField": "incident_id",
                "as": "victim_data"
            },
            "collection": "Shooter",
            "filter": {
                "gender": shooter_gender,
                "victim_data.gender": victim_gender
            }
        }

    return {"error": "Query not recognized"}

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
