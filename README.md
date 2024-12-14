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
       - This will output example queries for you to use. If you would like to run any of these, please copy and paste any of the <ins>**natural language queries**</ins> into the text box
       - Example Input: "Can I get example queries"
     - Ask for example queries with any of the following specific language constructs <ins>**(group by, order by, avg, sum, date, from, count)**</ins>
       - Example Input: "Can I get example queries with group by" (replace "group by" with any of the specific language constructs above)
   - MongoDB
     - Ask for example queries in the input box. If you would like to run any of these, please copy and paste the <ins>**actual MongoDB query**</ins> into the text box.
       - Example Input: "Can I get example queries"

4. **Natural Language Queries**:
   - Please input any of these natural language queries in quotation marks into the text box.
   - **SQL**
     - <ins>List Tables:</ins> "show tables"
     - <ins>Describe Attributes:</ins> "show table shooter attributes"
     - <ins>Basic Select</ins>
       - "get me gender of shooter where race is Hispanic"
       - "get me incident where state is LA"
       - "get me victim where injury is Fatal"
     - <ins>Total Group By</ins>
       - "total Shots_Fired by state from incident"
     - <ins>Count Group By</ins>
       - "Count victim by injury"
       - "Count shooter by age"
     - <ins>Joins</ins>
       - "show incident which has shooter that the shooteroutcome is Surrendered"
       - "show victim which has incident that the city is Los Angeles"
       - After uploading weapon dataset: "show shooter which has weapon that the weapontype is Rifle"
     - <ins>Filter Sort</ins>
       - "find gender, age from shooter where race is Hispanic order by age desc"
     - <ins>Average By Category</ins>
       - "average age by schoolaffiliation from shooter"
       - "average Shots_Fired by city from incident"
     - <ins>Top N By Measure</ins>
       - "5 city with highest shots_fired"
       - "10 shooter with highest age"
       - "3 state with the highest number of incident"
     - <ins>Filter By Date</ins>
       - "show incident where date is between '2022-05-30' and '2022-06-01"
   - **MongoDB**
     - MongoDB supports less natural language queries, but supports all valid NoSQL queries.
     - Natural Language
       - <ins>List Collections:</ins> "show collections"
       - "Count how many shooters are male" (male/female)
       - "How many victims were wounded" (wounded/fatal
       - "How many incidents occurred in CA" (any other abbreviated state)
       - "How many incidents had a male shooter and a female victim" (male/female)
     - NoSQL Queries
       - db.shooter.aggregate([ {"$group": {"_id": "$gender", "total_age": {"$sum": "$age"}}} ])
       - db.shooter.aggregate([ {"$match": {"age": {"$gte": 30}}}, {"$group": {"_id": "$gender", "count": {"$sum": 1}}} ])
       - db.victim.find().sort({"age": -1}).limit(5)
       - db.victim.aggregate([    {"$group": {"_id": "$gender", "average_age": {"$avg": "$age"}}}])
       - After uploading WEAPON.csv dataset with table name weapon:
         - db.weapon.find({"weapontype": "Handgun"})
         - db.weapon.find({}, {"incidentid": 1, "weapontype": 1, "_id": 0})








