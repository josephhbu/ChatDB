sql_query_patterns = [
    {
        "name": "total_by_category",
        "sql": "SELECT {category}, SUM({measure}) AS total FROM {table} GROUP BY {category}",
        "description": "Total {measure} by {category}"
    },
    {
        "name": "filter_and_sort",
        "sql": "SELECT * FROM {table} WHERE {condition} ORDER BY {sort_column} {sort_order}",
        "description": "Filter {table} by {condition} and sort by {sort_column}"
    },
    {
        "name": "count_by_category",
        "sql": "SELECT {category}, COUNT(*) AS count FROM {table} GROUP BY {category}",
        "description": "Count entries by {category}"
    },
    {
        "name": "average_by_category",
        "sql": "SELECT {category}, AVG({measure}) AS average FROM {table} GROUP BY {category}",
        "description": "Average {measure} by {category}"
    },
    {
        "name": "filter_by_date_range",
        "sql": "SELECT * FROM {table} WHERE {date_column} BETWEEN '{start_date}' AND '{end_date}'",
        "description": "Filter {table} by date range between {start_date} and {end_date}"
    },
    {
        "name": "top_n_by_measure",
        "sql": "SELECT * FROM {table} ORDER BY {measure} DESC LIMIT {n}",
        "description": "Get top {n} results by {measure}"
    },
    {
        "name": "join_query",
        "sql": "SELECT {table1}.* FROM {table1} INNER JOIN {table2} ON {table1}.{join_column1} = {table2}.{join_column2} WHERE {table2}.{columns} = '{condition}'",
        "description": "Join {table1} with {table2} on {join_column} with condition {condition}"
    },
    {
        "name": "basic_select",
        "sql": "SELECT * FROM {table} WHERE {columns} = '{condition}'",
        "description": "Basic select query from {table} with condition {condition}"
    },
    {
        "name": "list_tables",
        "sql": "SHOW TABLES",
        "description": "List all tables in the database"
    }
]


# MongoDB Patterns
# Define all MongoDB query patterns
mongo_query_patterns = [
    {
        "name": "mongo_group_sum",
        "query": '[{{"$group": {{"_id": "${category}", "total": {{"$sum": "${measure}"}}}}}}]',
        "description": "Total {measure} by {category} in MongoDB"
    },
    {
        "name": "mongo_filter_and_sort",
        "query": '[{{"$match": {{"{condition}"}}}}, {{"$sort": {{"{sort_column}": {{"$direction": "{sort_order}"}}}}}}]',
        "description": "Filter and sort MongoDB collection by {condition} and {sort_column}"
    },
    {
        "name": "mongo_count_by_category",
        "query": '[{{"$group": {{"_id": "${category}", "count": {{"$sum": 1}}}}}}]',
        "description": "Count documents by {category} in MongoDB"
    },
    {
        "name": "mongo_average_by_category",
        "query": '[{{"$group": {{"_id": "${category}", "average": {{"$avg": "${measure}"}}}}}}]',
        "description": "Average {measure} by {category} in MongoDB"
    },
    {
        "name": "mongo_filter_by_date_range",
        "query": '[{{"$match": {{"{date_column}": {{"$gte": "{start_date}", "$lte": "{end_date}"}}}}}}]',
        "description": "Filter MongoDB collection by date range between {start_date} and {end_date}"
    },
    {
        "name": "mongo_top_n_by_measure",
        "query": '[{{"$sort": {{"{measure}": -1}}}}, {{"$limit": {n}}}]',
        "description": "Get top {n} results by {measure} in MongoDB"
    },
    {
        "name": "mongo_lookup",
        "query": '[{{"$lookup": {{"from": "{table2}", "localField": "{local_field}", "foreignField": "{foreign_field}", "as": "{join_as}"}}}}]',
        "description": "Join {table1} with {table2} on {local_field} and {foreign_field} in MongoDB"
    },
    {
        "name": "mongo_basic_select",
        "query": '[{{"$match": {{"{condition}"}}}}]',
        "description": "Basic MongoDB select query with condition {condition}"
    },
    {
        "name": "mongo_insert_query",
        "query": '[{{"$insert": {{"into": "{table}", "documents": {values}}}}}]',
        "description": "Insert documents into {table} in MongoDB"
    },
    {
        "name": "mongo_update_query",
        "query": '[{{"$update": {{"table": "{table}", "set": "{columns}", "where": "{condition}"}}}}]',
        "description": "Update {table} collection in MongoDB"
    },
    {
        "name": "mongo_delete_query",
        "query": '[{{"$delete": {{"from": "{table}", "where": "{condition}"}}}}]',
        "description": "Delete from {table} where {condition} in MongoDB"
    },
    {
        "name": "mongo_list_collections",
        "query": '[{{"$listCollections": {}}}]',
        "description": "List all collections in MongoDB"
    }
]

# Query pattern building
# Class for a query pattern QueryPattern Class:
# Each pattern will represent a specific query type (e.g., "total by category").
# It includes a name (to identify the pattern), a query template (with placeholders like {category} and {measure}), and a description to explain what the pattern does.
class QueryPattern:
    def __init__(self, name, template, description):
        self.name = name           # Name of the pattern (e.g., "total_by_category")
        self.template = template   # SQL or MongoDB query template with placeholders
        self.description = description  # A brief description of what the query does
        # self.conditionclase = 
# Class to manage multiple patterns and generate queries
# This class manages a collection of query patterns.
# You can add new query patterns to it.
# You can generate SQL or MongoDB queries by selecting the pattern and filling in the required placeholders (like the table name, column names, etc.).
class QueryGenerator:
    def __init__(self):
        self.patterns = []  # List to store all query patterns

    # Function to add new patterns to the generator
    def add_pattern(self, pattern):
        self.patterns.append(pattern)

    # Function to generate a query based on the pattern name
    def generate_query(self, pattern_name, **kwargs):
        for pattern in self.patterns:
            if pattern.name == pattern_name:
                print(pattern.template.format(**kwargs))

                # Fill in the placeholders in the template with actual values (kwargs)
                try:
                    return pattern.template.format(**kwargs)
                except KeyError as e:
                    raise ValueError(f"Missing parameter: {e}")
        # If the pattern is not found, raise an error
        raise ValueError(f"Pattern {pattern_name} not found")

# Initialize the query generator
generator = QueryGenerator()

# Step 2: Add SQL patterns (can be used for different tables and columns)


# Add MongoDB patterns to the generator
for pattern in mongo_query_patterns:
    generator.add_pattern(QueryPattern(
        pattern["name"],
        pattern["query"],
        pattern["description"]
    ))


for pattern in sql_query_patterns:
     generator.add_pattern(QueryPattern(
        pattern["name"],
        pattern["sql"],
        pattern["description"]
    ))