# ChatDB
### an interactive ChatGPT-like application that assists users in learning how to query data in database systems, including SQL and NoSQL databases. For example, it can suggest sample queries, including ones that include specific language constructs, and understand queries in natural language. But unlike ChatGPT, it can also execute the queries in the database systems and display the query results to the users.
### Collaborators: Joseph Bu, Ayush Agarwal, Cheryl Khau

---

## Features

- **Natural Language Querying**:
  - Supports SQL and MongoDB query generation from natural language input.
- **Database Support**:
  - SQL: MySQL
  - NoSQL: MongoDB
- **Dataset Upload**:
  - Upload datasets in CSV format for MySQL.
  - Upload datasets in JSON format for MongoDB.
- **Dynamic Query Execution**:
  - Executes queries and displays results in real time.
- **Example Queries**:
  - Provides example queries for both SQL and MongoDB to guide users.

---

## Installation

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/yourusername/ChatDB.git
   ```
2. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```
   
3. **Set Up MySQL**:
   - Start the MySQL server.
   - Update MySQL credentials in `backend_functions.py` _if needed_:
     ```python
     MYSQL_USER = 'your_mysql_username'
     MYSQL_PASSWORD = 'your_mysql_password'
     ```

4. **Set Up MongoDB**:
   - Start the MongoDB server.

5. **Run Backend Scripts to Upload Datasets to MySQL and MongoDB**
   - ```bash
     python backend_functions.py
     python nosql_backend.py
     ```
  - This will create the databases and upload three datasets (INCIDENT.csv, SHOOTER.csv, VICTIM.csv) to both MySQL and MongoDB databases.

6. **Run the Application**:
   ```bash
   streamlit run chatDB_main.py
   ```

---

## Usage

1. **Choose Database Type**:
   - SQL
   - MongoDB

2. **Upload Weapon Dataset**:
   - Choose data/WEAPON.csv
   - Enter table/collection name
   - Will upload dataset into either database

3. **Ask for Example Queries**:
   - SQL
     - Ask for example queries in the input box
       - This will output example queries for you to use. If you would like to run any of these, please copy and paste any of the **natural language queries** into the text box
       - Example Input: "Can I get example queries"
     - Ask for example queries with any of the following specific language constructs **(group by, order by, avg, sum, date, from, count)**
       - Example Input: "Can I get example queries with group by" (replace "group by" with any of the specific language constructs above)
   - MongoDB
     - Ask for example queries in the input box. If you would like to run any of these, please copy and paste the **actual MongoDB query** into the text box.
       - Example Input: "Can I get example queries"

4. **Natural Language Queries**:
   - SQL
     - list tables: "show tables"
     - descibe attributes: "show table <shooter> attributes
     - Basic Select
       - "get me gender of SHOOTER where race is Hispanic"
       - "get me incident where state is LA"
       - "get me victim where injury is Fatal "
     - 
   
