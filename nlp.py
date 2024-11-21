import re
from query_patterns import generator
# Assume we have an existing 'extract_params' function to extract parameters

# MongoDB Query Execution
def run_mongo_query(query, db_name):
    client = MongoClient('mongodb://localhost:27017/')
    db = client[db_name]
    result = db.command(query)
    return result

# Function to run a SQL query
def run_sql_query(query, engine):
    with engine.connect() as connection:
        result = connection.execute(text(query))
        return result.fetchall()

# Steps:
# 1. Extract Parameters Dynamically: Use regex to extract parameters from the natural language input.
# 2. Map Intent to the Query Pattern: Use the detected intent to choose the appropriate query template.
# 3. Generate Queries Dynamically: Once parameters are extracted, use them to generate the query using the pattern's template depending on the database chosen (MongoDB or SQL).

# Function to detect the intent of the user input
def detect_intent(user_input):
    if re.search(r'total.*by.*', user_input.lower()):
        return "group_by"
    elif re.search(r'find.*where.*', user_input.lower()):
        return "filter_sort"
    elif re.search(r'count.*by.*', user_input.lower()):
        return "count_by_category"
    elif re.search(r'average.*of.*', user_input.lower()):
        return "average_by_category"
    elif re.search(r'show.*from.*to.*', user_input.lower()):
        return "filter_by_date_range"
    elif re.search(r'top.*where.*', user_input.lower()):
        return "top_n_by_measure"
    elif re.search(r'join.*on.*', user_input.lower()):
        return "join_query"
    elif re.search(r'get.*from.*', user_input.lower()):
        return "basic_select"
    elif re.search(r'insert.*into.*', user_input.lower()):
        return "insert_query"
    elif re.search(r'update.*set.*', user_input.lower()):
        return "update_query"
    elif re.search(r'delete.*from.*', user_input.lower()):
        return "delete_query"
    elif re.search(r'list.*tables', user_input.lower()):
        return "list_tables"
    elif re.search(r'list.*collections', user_input.lower()):
        return "list_collections"
    else:
        return "unknown"

# Extract parameters dynamically from the natural language query
def extract_params(nl_query, pattern):
    # Convert the query pattern to a regex with named groups
    regex = pattern.replace("{", "(?P<").replace("}", ">.*?)")
    match = re.match(regex, nl_query)
    return match.groupdict() if match else None

# Main function for processing the user input
def process_user_input(user_input, selected_db):
    # Step 1: Detect intent
    intent = detect_intent(user_input)

    if intent == "join_query":
        # Extract parameters for a join query
        pattern = "join_query {columns} from {table1} join {table2} on {join_column} where {condition}"
        params = extract_params(user_input, pattern)

        if params:
            # Generate query based on the selected database (SQL or MongoDB)
            if selected_db == 'SQL':
                query = generator.generate_query(
                    "join_query",
                    table1=params["table1"],
                    table2=params["table2"],
                    join_column=params["join_column"],
                    condition=params["condition"],
                    columns=params["columns"]
                )
            elif selected_db == 'MongoDB':
                query = generator.generate_query(
                    "mongo_lookup",
                    table1=params["table1"],
                    table2=params["table2"],
                    local_field=params["join_column"],
                    foreign_field=params["join_column"],  # Same field in this example
                    join_as=params["columns"],
                    condition=params["condition"]
                )
            print("Generated Join Query:", query)
        else:
            print(f"Unable to extract parameters from: {user_input}")

    elif intent == "group_by":
        # Extract parameters for a 'total_by_category' query
        pattern = "total {measure} by {category} from {table}"
        params = extract_params(user_input, pattern)

        if params:
            # Generate the query for "group_by"
            if selected_db == 'SQL':
                query = generator.generate_query(
                    "total_by_category",
                    category=params["category"],
                    measure=params["measure"],
                    table=params["table"]
                )
                result = run_sql_query(query)  # Run SQL query
                print("Generated and Executed SQL Group By Query:", query)
                print("SQL Query Result:", result)
            elif selected_db == 'MongoDB':
                query = generator.generate_query(
                    "mongo_group_sum",
                    category=params["category"],
                    measure=params["measure"]
                )
                result = run_mongo_query(query)  # Run MongoDB query
                print("Generated and Executed MongoDB Group By Query:", query)
                print("MongoDB Query Result:", result)

    elif intent == "filter_sort":
        # Extract parameters for a filter and sort query
        pattern = "find {columns} from {table} where {condition} order by {sort_column} {sort_order}"
        params = extract_params(user_input, pattern)

        if params:
            # Generate the filter and sort query
            if selected_db == 'SQL':
                query = generator.generate_query(
                    "filter_and_sort",
                    table=params["table"],
                    condition=params["condition"],
                    sort_column=params["sort_column"],
                    sort_order=params["sort_order"]
                )
                result = run_sql_query(query)  # Run SQL query
                print("Generated and Executed SQL Filter Sort By Query:", query)
                print("SQL Query Result:", result)
            elif selected_db == 'MongoDB':
                query = generator.generate_query(
                    "mongo_filter_and_sort",
                    condition=params["condition"],
                    sort_column=params["sort_column"],
                    sort_order=params["sort_order"]
                )
                result = run_mongo_query(query)  # Run MongoDB query
                print("Generated and Executed MongoDB Filter Sort By Query:", query)
                print("MongoDB Query Result:", result)

    elif intent == "count_by_category":
        # Extract parameters for count by category query
        pattern = "count {measure} by {category} from {table}"
        params = extract_params(user_input, pattern)

        if params:
            # Generate count query based on selected database
            if selected_db == 'SQL':
                query = generator.generate_query(
                    "count_by_category",
                    category=params["category"],
                    measure=params["measure"],
                    table=params["table"]
                )
                result = run_sql_query(query)  # Run SQL query
                print("Generated and Executed SQL Count By Query:", query)
                print("SQL Query Result:", result)
            elif selected_db == 'MongoDB':
                query = generator.generate_query(
                    "mongo_count_by_category",
                    category=params["category"]
                )
                result = run_mongo_query(query)  # Run MongoDB query
                print("Generated and Executed MongoDB Count By Query:", query)
                print("MongoDB Query Result:", result)

    elif intent == "insert_query":
        # Extract parameters for insert query
        pattern = "insert into {table} ({columns}) values ({values})"
        params = extract_params(user_input, pattern)

        if params:
            # Generate insert query
            if selected_db == 'SQL':
                query = generator.generate_query(
                    "insert_query",
                    table=params["table"],
                    columns=params["columns"],
                    values=params["values"]
                )
                result = run_sql_query(query)  # Run SQL query
                print("Generated and Executed SQL Insert Query:", query)
                print("SQL Query Result:", result)
            elif selected_db == 'MongoDB':
                query = generator.generate_query(
                    "mongo_insert_query",
                    table=params["table"],
                    values=params["values"]
                )
                result = run_mongo_query(query)  # Run MongoDB query
                print("Generated and Executed MongoDB Insert Query:", query)
                print("MongoDB Query Result:", result)

    elif intent == "update_query":
        # Extract parameters for update query
        pattern = "update {table} set {columns} where {condition}"
        params = extract_params(user_input, pattern)

        if params:
            # Generate update query
            if selected_db == 'SQL':
                query = generator.generate_query(
                    "update_query",
                    table=params["table"],
                    columns=params["columns"],
                    condition=params["condition"]
                )
                result = run_sql_query(query)  # Run SQL query
                print("Generated and Executed SQL Update Query:", query)
                print("SQL Query Result:", result)
            elif selected_db == 'MongoDB':
                query = generator.generate_query(
                    "mongo_update_query",
                    table=params["table"],
                    columns=params["columns"],
                    condition=params["condition"]
                )
                result = run_mongo_query(query)  # Run MongoDB query
                print("Generated and Executed MongoDB Update Query:", query)
                print("MongoDB Query Result:", result)

    elif intent == "delete_query":
        # Extract parameters for delete query
        pattern = "delete from {table} where {condition}"
        params = extract_params(user_input, pattern)

        if params:
            # Generate delete query
            if selected_db == 'SQL':
                query = generator.generate_query(
                    "delete_query",
                    table=params["table"],
                    condition=params["condition"]
                )
                result = run_sql_query(query)  # Run SQL query
                print("Generated and Executed SQL Delete Query:", query)
                print("SQL Query Result:", result)
            elif selected_db == 'MongoDB':
                query = generator.generate_query(
                    "mongo_delete_query",
                    table=params["table"],
                    condition=params["condition"]
                )
                result = run_mongo_query(query)  # Run MongoDB query
                print("Generated and Executed MongoDB Delete Query:", query)
                print("MongoDB Query Result:", result)

    elif intent == "list_tables":
        # Generate query for listing tables in SQL
        if selected_db == 'SQL':
            query = generator.generate_query("list_tables")
            result = run_sql_query(query)
            print("Generated List Tables Query:", query)
            print("SQL Query Result:", result)

    elif intent == "list_collections":
        # Generate query for listing collections in MongoDB
        if selected_db == 'MongoDB':
            query = generator.generate_query("mongo_list_collections")
            result = run_mongo_query(query)  # Run MongoDB query
            print("Generated List Collections Query:", query)
            print("MongoDB Query Result:", result)

    else:
        print(f"No pattern detected for input: {user_input}")

