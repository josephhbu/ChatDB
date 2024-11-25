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

def run_mongo_query(query, db_name, collection_name):
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
    Example:
        "age is at least 25" -> {'age': {'$gte': 25}}
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
    print(f"Matching input: '{user_input}' with pattern: '{regex.pattern}'")
    match = regex.match(user_input)
    if not match:
        print(f"Failed to extract parameters from: {user_input}")
        return {}
    return match.groupdict()

def process_user_input_mongodb(user_input, intent, db_name="testdb", collection_name="orders"):
    """
    Processes the user input and executes a MongoDB query based on the detected intent.

    Args:
        user_input (str): User input string.
        intent (str): Intent of the query.
        db_name (str): Name of the database to query.
        collection_name (str): Name of the collection to query.

    Returns:
        list: Results of the MongoDB query.
    """
    intent_config = {
        "join_query": {
            # Updated pattern to handle "is at least" conditions
            "pattern": r"join_query (?P<columns>.+?) from (?P<table1>.+?) join (?P<table2>.+?) on (?P<local_field>.+?)=(?P<foreign_field>.+?) where (?P<condition>.+)",
            "query_type": "mongo_lookup",
            "params": ["table1", "table2", "local_field", "foreign_field", "columns", "condition"],
        },
        "total_group_by": {
            "pattern": r"total (?P<measure>.+?) by (?P<category>.+?) from (?P<table>.+)",
            "query_type": "mongo_group_sum",
            "params": ["category", "measure"],
        },
        "filter_sort": {
            "pattern": r"find (?P<columns>.+?) from (?P<table>.+?) where (?P<condition>.+?) order by (?P<sort_column>.+?) (?P<sort_order>.+)",
            "query_type": "mongo_filter_and_sort",
            "params": ["condition", "sort_column", "sort_order"],
        },
        "count_by_category": {
            "pattern": r"count (?P<measure>.+?) by (?P<category>.+)",
            "query_type": "mongo_count_by_category",
            "params": ["category"],
        },
        "list_collections": {
            "pattern": None,  # No parameters needed
            "query_type": "mongo_list_collections",
            "params": [],
        },
    }

    if intent not in intent_config:
        print(f"Unsupported intent: {intent}")
        return []

    config = intent_config[intent]
    pattern = config["pattern"]
    query_type = config["query_type"]

    # Extract parameters
    params = extract_params(user_input, pattern) if pattern else {}

    # Validate extracted parameters
    missing_params = [param for param in config["params"] if param not in params]
    if missing_params:
        print(f"Missing parameters for {intent}: {missing_params}")
        return []

    # Parse the condition
    params["condition"] = parse_condition(params["condition"])

    # Add a default alias for the joined data
    params["join_as"] = "joined_data"

    # Generate the MongoDB query
    query = generator.generate_query(query_type, **params)
    print(f"Generated Query ({intent}): {query}")

    # Execute the query
    result = run_mongo_query(query, db_name=db_name, collection_name=params["table1"])
    print(f"Query Result ({intent}): {result}")

    return result

# Example User Input for Join Query
user_input = "join_query name, age, amount from users join orders on user_id=user_id where age is at least 25"
intent = "join_query"
db_name = "shooter"
collection_name = "users"  # The primary collection for the query

process_user_input_mongodb(user_input, intent, db_name=db_name, collection_name=collection_name)
