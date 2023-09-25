import psycopg2
import csv
from psycopg2 import sql

try:
    connection = psycopg2.connect(
        dbname="testdb",
        user="consultants",
        password="WelcomeItc@2022",
        host="ec2-3-9-191-104.eu-west-2.compute.amazonaws.com",
        port="5432"
    )

    cursor = connection.cursor()
    print("Connected to the database")
except Exception as e:
    print(f"Error: {e}")

# Define the table name and CSV file path
table_name = "FraudTable"
csv_file_path = "C:/Users/Consultant/Desktop/Project_Data.csv"

# Read the first row of the CSV file to get column names
with open(csv_file_path, 'r') as csv_file:
    csv_reader = csv.reader(csv_file)
    header = next(csv_reader)

# Create the table with the same column names
create_table_query = sql.SQL("CREATE TABLE {} ({});").format(
    sql.Identifier(table_name),
    sql.SQL(', ').join(sql.Identifier(column) for column in header)
)

try:
    cursor.execute(create_table_query)
    connection.commit()
    print(f"Table {table_name} created successfully with columns: {', '.join(header)}")
except Exception as e:
    connection.rollback()
    print(f"Error: {e}")

# Now you can proceed to insert data from the CSV file as shown in the previous response

# Close the database connection
connection.close()
print("Database connection closed")
