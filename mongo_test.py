from pymongo import MongoClient
import re

class QueryGenerator:
    def generate_query(self, query_type, **kwargs):
        if query_type == "mongo_lookup":
            return [
                {
                    "$lookup": {
                        "from": kwargs["table2"],
                        "localField": kwargs["local_field"],
                        "foreignField": kwargs["foreign_field"],
                        "as": kwargs.get("join_as", "joined_data"),  # Default alias if not provided
                    }
                },
                {"$match": kwargs["condition"]}
            ]
        elif query_type == "mongo_group_sum":
            return [
                {"$group": {"_id": f"${kwargs['category']}", "total": {"$sum": f"${kwargs['measure']}"}}}
            ]
        elif query_type == "mongo_filter_and_sort":
            return [
                {"$match": kwargs["condition"]},
                {"$sort": {kwargs["sort_column"]: 1 if kwargs["sort_order"].lower() == "asc" else -1}}
            ]
        elif query_type == "mongo_count_by_category":
            return [
                {"$group": {"_id": f"${kwargs['category']}", "count": {"$sum": 1}}}
            ]
        elif query_type == "mongo_list_collections":
            return None  # Special case for listing collections
        else:
            raise ValueError(f"Unknown query type: {query_type}")

generator = QueryGenerator()

def run_mongo_query(query, db_name, collection_name=None):
    """
    Executes the MongoDB query on the specified database and collection.
    """
    client = MongoClient("mongodb://localhost:27017/")
    db = client[db_name]
    
    if isinstance(query, list):  # Aggregate query
        collection = db[collection_name]
        return list(collection.aggregate(query))
    elif isinstance(query, dict):  # Simple find query
        collection = db[collection_name]
        return list(collection.find(query))
    elif query is None:  # List collections
        return db.list_collection_names()
    else:
        raise ValueError("Unsupported query format.")

def parse_condition(condition_str):
    """
    Parses a condition string and converts it into MongoDB query syntax.
    """
    condition_str = condition_str.lower()
    if "is at least" in condition_str:
        field, value = condition_str.split("is at least")
        return {field.strip(): {"$gte": int(value.strip())}}
    elif "is at most" in condition_str:
        field, value = condition_str.split("is at most")
        return {field.strip(): {"$lte": int(value.strip())}}
    elif "is greater than" in condition_str:
        field, value = condition_str.split("is greater than")
        return {field.strip(): {"$gt": int(value.strip())}}
    elif "is less than" in condition_str:
        field, value = condition_str.split("is less than")
        return {field.strip(): {"$lt": int(value.strip())}}
    elif "is" in condition_str:
        field, value = condition_str.split("is")
        return {field.strip(): value.strip()}
    else:
        raise ValueError(f"Unsupported condition format: {condition_str}")

def extract_params(user_input, pattern):
    """
    Extract parameters dynamically from a natural language query based on the provided pattern.
    """
    regex_pattern = pattern.replace("{", "(?P<").replace("}", ">.+?)")
    regex = re.compile(regex_pattern, re.IGNORECASE)  # Case-insensitive
    match = regex.match(user_input)
    if not match:
        raise ValueError(f"Failed to extract parameters from: {user_input}")
    return match.groupdict()

def extract_collections(user_input, db=None):
    """
    Extract collections dynamically from the user input.
    If the query is 'list collections', return all collections in the database.
    """
    # Handle the special case for "list collections"
    if "list collections" in user_input.lower():
        if db is None:  # Explicitly check if db is None
            raise ValueError("Database connection is required to list collections.")
        return db.list_collection_names()  # Return all collections in the database

    # General case for extracting collections
    match = re.search(r"from ([\w\s]+(?:and [\w\s]+)*)", user_input, re.IGNORECASE)
    if not match:
        raise ValueError("No collections specified in the query.")
    collections = match.group(1).split(" and ")
    return [collection.strip() for collection in collections]


def detect_intent(user_input):
    """
    Detects the intent of the user query based on predefined patterns.
    """
    user_input = user_input.lower().strip()
    intent_patterns = [
        ("join_query", r"from (?P<table1>\w+) and (?P<table2>\w+) join (?P<local_field>\w+\.\w+) with (?P<foreign_field>\w+\.\w+) where (?P<condition>.+)"),
        ("total_group_by", r"from (?P<table>\w+) total (?P<measure>\w+) by (?P<category>\w+)"),
        ("filter_sort", r"from (?P<table>\w+) find (?P<columns>.+?) where (?P<condition>.+) order by (?P<sort_column>\w+) (?P<sort_order>\w+)"),
        ("count_by_category", r"from (?P<table>\w+) count (?P<category>\w+)"),
        ("list_collections", r"list collections"),
    ]
    for intent, pattern in intent_patterns:
        if re.search(pattern, user_input):
            return intent
    return "unknown"

def process_user_input_mongodb(user_input, db_name):
    """
    Processes the user input and executes a MongoDB query based on the detected intent.
    """
    client = MongoClient("mongodb://localhost:27017/")
    db = client[db_name]

    intent = detect_intent(user_input)
    if intent == "unknown":
        raise ValueError(f"Unable to detect intent for query: {user_input}")

    # Handle the special case for "list collections"
    if intent == "list_collections":
        return extract_collections(user_input, db=db)

    # Extract collections dynamically from the user input
    collections = extract_collections(user_input, db=db)
    if not collections:
        raise ValueError("No collections found in the query.")
    
    intent_config = {
        "join_query": {
            "pattern": r"from (?P<table1>\w+) and (?P<table2>\w+) join (?P<local_field>\w+\.\w+) with (?P<foreign_field>\w+\.\w+) where (?P<condition>.+)",
            "query_type": "mongo_lookup",
            "params": ["table1", "table2", "local_field", "foreign_field", "condition"],
        },
        "total_group_by": {
            "pattern": r"from (?P<table>\w+) total (?P<measure>\w+) by (?P<category>\w+)",
            "query_type": "mongo_group_sum",
            "params": ["category", "measure"],
        },
        "filter_sort": {
            "pattern": r"from (?P<table>\w+) find (?P<columns>.+?) where (?P<condition>.+) order by (?P<sort_column>\w+) (?P<sort_order>\w+)",
            "query_type": "mongo_filter_and_sort",
            "params": ["condition", "sort_column", "sort_order"],
        },
        "count_by_category": {
            "pattern": r"from (?P<table>\w+) count (?P<category>\w+)",
            "query_type": "mongo_count_by_category",
            "params": ["category"],
        },
        "list_collections": {
            "pattern": None,
            "query_type": "mongo_list_collections",
            "params": [],
        },
    }

    config = intent_config[intent]
    pattern = config["pattern"]
    query_type = config["query_type"]

    params = extract_params(user_input, pattern) if pattern else {}

    # Parse conditions if applicable
    if "condition" in params:
        params["condition"] = parse_condition(params["condition"])

    if len(collections) == 1:
        collection_name = collections[0]
        query = generator.generate_query(query_type, **params)
        results = run_mongo_query(query, db_name, collection_name)
    else:
        # Handle joins if multiple collections are specified
        params["table1"] = collections[0]
        params["table2"] = collections[1]
        query = generator.generate_query(query_type, **params)
        results = run_mongo_query(query, db_name, collections[0])  # Using the first collection as the base

    return results


user_input = "list collections"
results = process_user_input_mongodb(user_input, "shooter")
print(results)
