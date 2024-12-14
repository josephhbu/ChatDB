import random
from pymongo import MongoClient
from sqlalchemy.sql import text
from sqlalchemy import create_engine
from nlp_logic.query_patterns import sql_query_patterns, mongo_query_patterns
import mysql.connector

def fetch_sql_metadata(engine):
    """
    Fetch table and column metadata from the SQL database.

    Args:
        engine: SQLAlchemy engine object.

    Returns:
        dict: A dictionary with table names as keys and column lists with types as values.
    """
    metadata = {}
    query = "SHOW TABLES;"
            
    cursor = engine.cursor()
    cursor.execute(query)
    res = cursor.fetchall()
    tables = [row[0] for row in res]
    
    for table in tables:
        attr_query = f"SHOW COLUMNS FROM {table}"
        cursor.execute(attr_query)
        attr_result = cursor.fetchall()
        metadata[table] = [{"name": col[0], "type": col[1]} for col in attr_result]
 
    cursor.close()
    return metadata


def fetch_mongo_metadata(db_name):
    """
    Fetch collection and field metadata from the MongoDB database.

    Args:
        db_name (str): Name of the MongoDB database.

    Returns:
        dict: A dictionary with collection names as keys and field lists as values.
    """
    metadata = {}
    client = MongoClient('mongodb://localhost:27017/')
    db = client[db_name]
    collections = db.list_collection_names()
    for collection_name in collections:
        collection = db[collection_name]
        sample_doc = collection.find_one()
        if sample_doc:
            metadata[collection_name] = list(sample_doc.keys())
    return metadata



def generate_sample_queries(db_type, metadata, construct=None, limit=3, engine=None):
    """
    Generate a set of sample queries dynamically using actual database metadata.

    Args:
        db_type (str): Database type ('SQL' or 'MongoDB').
        metadata (dict): Metadata of the database (tables/columns for SQL, collections/fields for MongoDB).
        construct (str, optional): Specific construct to filter (e.g., 'group by', 'join').
        limit (int): Number of sample queries to generate.

    Returns:
        list: A list of sample queries with their natural language representation.
    """
    patterns = sql_query_patterns 

    # Filter patterns by construct if specified
    if construct:
        patterns = [pattern for pattern in patterns if construct.lower() in pattern["sql"].lower()]

    sample_queries = []
    used_patterns = set()
    table_names = list(metadata.keys())

    for _ in range(limit):
        if construct:
            available_patterns = patterns
        else:
            available_patterns = [pattern for pattern in patterns if pattern["description"] not in used_patterns]
        if not available_patterns:
            break  # No more unique patterns available
        pattern = random.choice(available_patterns)
        used_patterns.add(pattern["description"])

        # Randomly select a table and its columns
        table = random.choice(table_names)
        columns = metadata[table] if table in metadata else []
        if not columns:
            continue

        # Classify columns       
        numeric_columns = [col for col in columns if col['type'].strip() in ['float', 'int', 'number']]
        categorical_columns = [col for col in columns if col not in numeric_columns]

        # Generate date_column safely
        date_columns = [col for col in columns if "date" in col['name'].lower()]
        list_date_col_names = [col['name'] for col in date_columns]
        date_column = random.choice(list_date_col_names) if list_date_col_names else None
        list_col_names = [col['name'] for col in columns]
        list_cate_col_names = [col['name'] for col in categorical_columns]
        list_numeric_col_names = [col['name'] for col in numeric_columns]
        
        condition_col = random.choice(list_cate_col_names)
        query = f"select {condition_col} from {table} group by {condition_col}"
        cursor = engine.cursor()
        cursor.execute(query)
        res = cursor.fetchall()
        res = [item[0] for item in res if item[0] and len(item[0].split()) == 1]
        
        random_condition_choice = random.choice(res)
        
        # Fill placeholders with actual values or defaults
        placeholders = {
            "table": table,
            "category": random.choice(list_cate_col_names) if list_cate_col_names else random.choice(list_col_names),
            "measure": random.choice(list_numeric_col_names) if list_numeric_col_names else random.choice(list_col_names),
            "columns": ", ".join(random.sample(list_col_names, min(2, len(list_col_names)))),
            "condition": f"{condition_col} is {random_condition_choice}",
            "sort_column": random.choice(list_col_names),
            "sort_order": random.choice(["ASC", "DESC"]),
            "date_column": date_column,
            "start_date": "2022-01-01",
            "end_date": "2022-12-31",
            "n": random.randint(4,10),
            "column": random.choice(list_col_names),
        }

        # Skip queries that require a date column but none exist
        if "{date_column}" in pattern.get("sql", "") and not placeholders["date_column"]:
            continue
        
        if not all(placeholders.get(key) for key in ["table", "columns", "measure", "sort_column", "sort_order"]):
            sample_queries.append({
                "description": f"Generate a query for {pattern['description']} (Error: Missing placeholder details)",
                "query": "[Incomplete Query - Unable to generate valid SQL structure]"
            })
            continue

        description_templates = {
            "basic_select": "get me {columns} of {table} where {condition}",
            "count_by_category": "count {table} by {category}",
            "filter_sort": "find {columns} from {table} where {condition} order by {sort_column} {sort_order}",
            "total_by_category": "total {measure} by {category} from {table}",
            "average_by_category": "average {measure} by {category} from {table}",
            "top_n_by_measure_table": "get me {n} {table} with highest {measure}",
            "filter_by_date_range": "show {table} where {date_column} is between '{start_date}' and '{end_date}'",
        }

        try:
            query = pattern["sql"] if db_type.lower() == "sql" else pattern["query"]
            query = query.format(**placeholders)

            # Update the description with actual values
            description = description_templates.get(pattern["name"], pattern["description"]).format(
                table=table.lower(),
                category=placeholders.get("category"),
                measure=placeholders.get("measure"),
                columns=placeholders.get("columns"),
                condition=placeholders.get("condition"),
                sort_column=placeholders.get("sort_column"),
                sort_order=placeholders.get("sort_order"),
                date_column=placeholders.get("date_column"),
                start_date=placeholders.get("start_date"),
                end_date=placeholders.get("end_date"),
                n=placeholders.get("n"),
            )


            # Append query and description to results
            sample_queries.append({
                "description": description,
                "query": query
            })
        except KeyError as e:
            print(f"Missing placeholder: {e}")
            

    return sample_queries

def generate_mongo_sample_queries(metadata, construct=None, limit=3):
    """
    Generate a set of sample queries dynamically using actual MongoDB metadata.

    Args:
        metadata (dict): Metadata of the MongoDB database (collections/fields).
        construct (str, optional): Specific construct to filter (e.g., 'aggregate', 'find').
        limit (int): Number of sample queries to generate.

    Returns:
        list: A list of sample queries with their natural language representation.
    """
    sample_queries = []
    used_patterns = set()
    collection_names = list(metadata.keys())
    
    # Filter patterns if a construct is specified
    patterns = mongo_query_patterns
    if construct:
        patterns = [pattern for pattern in patterns if construct.lower() in pattern["query"].lower()]
    for _ in range(limit):
        if not patterns:
            break  # No more unique patterns available
        pattern = random.choice(patterns)
        patterns.remove(pattern)  # Ensure unique queries for the current run
        used_patterns.add(pattern["description"])

        # Randomly select a collection and its fields
        collection = random.choice(collection_names)
        fields = metadata[collection] if collection in metadata else []
        if not fields:
            continue

        # Classify fields
        numeric_fields = [field for field in fields if "int" in field or "double" in field or "float" in field]
        string_fields = [field for field in fields if field not in numeric_fields]
        date_fields = [field for field in fields if "date" in field]

        placeholders_mongo = {
            "collection": collection,
            "field": random.choice(fields),
            "numeric_field": random.choice(numeric_fields) if numeric_fields else random.choice(fields),
            "string_field": random.choice(string_fields) if string_fields else random.choice(fields),
            "date_field": random.choice(date_fields) if date_fields else None,
            "value": "example_value",
            "start_date": "2022-01-01",
            "end_date": "2022-12-31",
        }

        # Skip queries that require a date field but none exist
        if "{date_field}" in pattern.get("query", "") and not placeholders_mongo["date_field"]:
            continue

        # Fill placeholders in the query
        try:
            query = pattern["query"].format(**placeholders_mongo)

            # Create a human-readable description
            description_templates = {
                "basic_find": "Retrieve all documents from {collection} where {field} is {value}.",
                "aggregate_sum": "Calculate the total of {numeric_field} grouped by {field} in {collection}.",
                "filter_date_range": "Find documents in {collection} where {date_field} is between {start_date} and {end_date}.",
                "top_n": "Find top 5 documents in {collection} sorted by {numeric_field} in descending order.",
            }
            description = description_templates.get(pattern["name"], pattern["description"]).format(
                **placeholders_mongo
            )

            # Append the query and description to the results
            sample_queries.append({
                "description": description,
                "query": query
            })
        except KeyError as e:
            print(f"Missing placeholder: {e}")

    return sample_queries


# Process user input for generating sample queries
def process_sample_queries(user_input, db_type, engine=None, metadata=None, db_name=None):
    """
    Process user input for generating sample queries.

    Args:
        user_input (str): The user input specifying the query construct.
        db_type (str): Database type ('SQL' or 'MongoDB').
        engine: Database engine for SQL or MongoDB client for MongoDB.
        metadata (dict, optional): Metadata of the database.

    Returns:
        list: A list of generated sample queries.
    """
    construct = None

    if not metadata:
        if db_type == "SQL":
            metadata = fetch_sql_metadata(engine)
        elif db_type == "MongoDB":
            metadata = fetch_mongo_metadata(db_name)

    if "with" in user_input:
        construct = user_input.split("with")[-1].strip()

    if db_type == "SQL":
        queries = generate_sample_queries(db_type, metadata, construct=construct, limit=3, engine=engine)
    elif db_type == "MongoDB":
        queries = generate_mongo_sample_queries(metadata, construct=construct, limit=3)

    for query in queries:
        print(f"Description: {query['description']}\nQuery: {query['query']}\n")
    return queries
