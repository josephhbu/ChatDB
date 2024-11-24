from backend_functions import implement 
from nlp import process_user_input
import mysql.connector
import sys

# implement() done to upload all datasets from US shooting

MYSQL_USER = ''  # Default MySQL user created by Homebrew
MYSQL_PASSWORD = ''  # No password set
MYSQL_HOST = 'localhost'  # Default host
DATABASE_NAME = 'db'
mydb = mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database='us_shooter_test'
)

query = sys.argv[1]
end_result = process_user_input(query, 'SQL', mydb)
print(list(end_result))

# CK test query: in terminal: python main.py "get me SHOOTER where race is Hispanic"

# example query tested:

# "get me SHOOTER where race is Hispanic"
# "get me incident where state is LA"
# "count INCIDENT by state"
# "show tables"


# Currently working on JOIN
#"show incident which has weapon that the weapontype is Rifle"