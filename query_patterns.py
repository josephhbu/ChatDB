# Query pattern building
# Class for a query pattern QueryPattern Class:
# Each pattern will represent a specific query type (e.g., "total by category").
# It includes a name (to identify the pattern), a query template (with placeholders like {category} and {measure}), and a description to explain what the pattern does.
class QueryPattern:
    def __init__(self, name, template, description):
        self.name = name           # Name of the pattern (e.g., "total_by_category")
        self.template = template   # SQL or MongoDB query template with placeholders
        self.description = description  # A brief description of what the query does

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

# SQL Pattern 1: Total (A) by (B) for SQL
generator.add_pattern(QueryPattern(
    "total_by_category",
    "SELECT {category}, SUM({measure}) AS total FROM {table} GROUP BY {category}",
    "Total {measure} by {category}"
))

# SQL Pattern 2: Filter and sort
generator.add_pattern(QueryPattern(
    "filter_and_sort",
    "SELECT * FROM {table} WHERE {condition} ORDER BY {sort_column} {sort_order}",
    "Filter {table} by {condition} and sort by {sort_column}"
))

# SQL Pattern 3: Count by category
generator.add_pattern(QueryPattern(
    "count_by_category",
    "SELECT {category}, COUNT(*) AS count FROM {table} GROUP BY {category}",
    "Count entries by {category}"
))

# SQL Pattern 4: Average by category
generator.add_pattern(QueryPattern(
    "average_by_category",
    "SELECT {category}, AVG({measure}) AS average FROM {table} GROUP BY {category}",
    "Average {measure} by {category}"
))

# SQL Pattern 5: Filter by date range
generator.add_pattern(QueryPattern(
    "filter_by_date_range",
    "SELECT * FROM {table} WHERE {date_column} BETWEEN '{start_date}' AND '{end_date}'",
    "Filter {table} by date range between {start_date} and {end_date}"
))

# SQL Pattern 6: Top n by measure
generator.add_pattern(QueryPattern(
    "top_n_by_measure",
    "SELECT * FROM {table} ORDER BY {measure} DESC LIMIT {n}",
    "Get top {n} results by {measure}"
))

# SQL Pattern 7: Joins TABLES
generator.add_pattern(QueryPattern(
    "join_query",
    "SELECT {columns} FROM {table1} INNER JOIN {table2} ON {table1}.{join_column} = {table2}.{join_column} WHERE {condition}",
    "Join {table1} with {table2} on {join_column} with condition {condition}"
))

# SQL Pattern 8: Basic select
generator.add_pattern(QueryPattern(
    "basic_select",
    "SELECT {columns} FROM {table} WHERE {condition}",
    "Basic select query from {table} with condition {condition}"
))

# SQL Pattern 9: Insert into table
generator.add_pattern(QueryPattern(
    "insert_query",
    "INSERT INTO {table} ({columns}) VALUES ({values})",
    "Insert values into {table}"
))

# SQL Pattern 10: Update table
generator.add_pattern(QueryPattern(
    "update_query",
    "UPDATE {table} SET {columns} WHERE {condition}",
    "Update {table} with values {columns} where {condition}"
))

# SQL Pattern 11: Delete from table
generator.add_pattern(QueryPattern(
    "delete_query",
    "DELETE FROM {table} WHERE {condition}",
    "Delete from {table} where {condition}"
))

# SQL Pattern 12: List tables
generator.add_pattern(QueryPattern(
    "list_tables",
    "SHOW TABLES",
    "List all tables in the database"
))

# MongoDB Patterns

# MongoDB Pattern 1: Aggregation Pattern: This pattern uses MongoDB's aggregate pipeline for grouping and summing data, similar to the SQL GROUP BY with SUM.
generator.add_pattern(QueryPattern(
    "mongo_group_sum",
    '[{{"$group": {{"_id": "${category}", "total": {{"$sum": "${measure}"}}}}}}]',
    "Total {measure} by {category} in MongoDB"
))

# MongoDB Pattern 2: Filter and Sort Pattern: This pattern allows filtering with match and sorting with sort in MongoDB.
generator.add_pattern(QueryPattern(
    "mongo_filter_and_sort",
    '[{{"$match": {{"{condition}"}}}}, {{"$sort": {{"{sort_column}": {{"$direction": "{sort_order}"}}}}}}]',
    "Filter and sort MongoDB collection by {condition} and {sort_column}"
))

# MongoDB Pattern 3: Count Pattern: Count the number of documents grouped by a category.
generator.add_pattern(QueryPattern(
    "mongo_count_by_category",
    '[{{"$group": {{"_id": "${category}", "count": {{"$sum": 1}}}}}}]',
    "Count documents by {category} in MongoDB"
))

# MongoDB Pattern 4: Average Pattern: Calculate the average of a field for each category.
generator.add_pattern(QueryPattern(
    "mongo_average_by_category",
    '[{{"$group": {{"_id": "${category}", "average": {{"$avg": "${measure}"}}}}}}]',
    "Average {measure} by {category} in MongoDB"
))

# MongoDB Pattern 5: Top N Pattern: Retrieve the top N documents based on a specific field.
generator.add_pattern(QueryPattern(
    "mongo_filter_by_date_range",
    '[{{"$match": {{"{date_column}": {{"$gte": "{start_date}", "$lte": "{end_date}"}}}}}}]',
    "Filter MongoDB collection by date range between {start_date} and {end_date}"
))

# MongoDB Pattern 6: Date Range Filter Pattern: Filter documents based on a date range.
generator.add_pattern(QueryPattern(
    "mongo_top_n_by_measure",
    '[{{"$sort": {{"{measure}": -1}}}}, {{"$limit": {n}}}]',
    "Get top {n} results by {measure} in MongoDB"
))

# MongoDB Pattern 7 Join Equivalent (lookup): MongoDB doesn't have traditional SQL JOIN, but we can use the $lookup stage to join collections.
generator.add_pattern(QueryPattern(
    "mongo_lookup",
    '[{{"$lookup": {{"from": "{table2}", "localField": "{local_field}", "foreignField": "{foreign_field}", "as": "{join_as}"}}}}]',
    "Join {table1} with {table2} on {local_field} and {foreign_field} in MongoDB"
))

# MongoDB Pattern 8: Basic Select
generator.add_pattern(QueryPattern(
    "mongo_basic_select",
    '[{{"$match": {{"{condition}"}}}}]',
    "Basic MongoDB select query with condition {condition}"
))

# MongoDB Pattern 9: Insert into collection
generator.add_pattern(QueryPattern(
    "mongo_insert_query",
    '[{{"$insert": {{"into": "{table}", "documents": {values}}}}}]',
    "Insert documents into {table} in MongoDB"
))

# MongoDB Pattern 10: Update collection
generator.add_pattern(QueryPattern(
    "mongo_update_query",
    '[{{"$update": {{"table": "{table}", "set": "{columns}", "where": "{condition}"}}}}]',
    "Update {table} collection in MongoDB"
))

# MongoDB Pattern 11: Delete from collection
generator.add_pattern(QueryPattern(
    "mongo_delete_query",
    '[{{"$delete": {{"from": "{table}", "where": "{condition}"}}}}]',
    "Delete from {table} where {condition} in MongoDB"
))

# MongoDB Pattern 12: List collections
generator.add_pattern(QueryPattern(
    "mongo_list_collections",
    '[{{"$listCollections": {}}}]',
    "List all collections in MongoDB"
))


# #Example: Generate SQL query for "total_by_category"
# sql_query = generator.generate_query(
#     "total_by_category",
#     category="product_category",   # Replace {category}
#     measure="sales_amount",        # Replace {measure}
#     table="sales"                  # Replace {table}
# )
# print("Generated SQL Query:", sql_query)

# # Example: Generate SQL query for "filter_and_sort"
# filter_sort_query = generator.generate_query(
#     "filter_and_sort",
#     table="sales",
#     condition="product_category = 'Coffee'",
#     sort_column="sales_amount",
#     sort_order="DESC"
# )
# print("Generated Filter and Sort Query:", filter_sort_query)

# # Example: Generate MongoDB query for "mongo_group_sum"
# mongo_query = generator.generate_query(
#     "mongo_group_sum",
#     category="product_category",
#     measure="sales_amount"
# )
# print("Generated MongoDB Group Sum Query:", mongo_query)

# # Example: Generate MongoDB query for "mongo_filter_and_sort"
# mongo_filter_query = generator.generate_query(
#     "mongo_filter_and_sort",
#     condition='category = "Coffee"',
#     sort_column="sales_amount",
#     sort_order="DESC"
# )
# print("Generated MongoDB Filter and Sort Query:", mongo_filter_query)

# # Example: Generate MongoDB query for "mongo_count_by_category"
# mongo_count_query = generator.generate_query(
#     "mongo_count_by_category",
#     category="product_category"
# )
# print("Generated MongoDB Count Query:", mongo_count_query)

# # Example: Generate MongoDB query for "mongo_lookup"
# mongo_lookup_query = generator.generate_query(
#     "mongo_lookup",
#     table2="products",
#     local_field="product_id",
#     foreign_field="product_id",
#     join_as="product_details"
# )
# print("Generated MongoDB Join Query:", mongo_lookup_query)