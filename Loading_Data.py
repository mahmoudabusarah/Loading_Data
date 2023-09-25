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

csv_file_path = "C:/Users/Consultant/Desktop/Project_Data.csv"
table_name = "fraudtable"
with open(csv_file_path, 'r') as csv_file:
    csv_reader = csv.reader(csv_file)
    next(csv_reader)  # Skip the header row if it exists

    data = [tuple(row) for row in csv_reader]  # Convert each row to a tuple

# Construct the insert query with placeholders based on the number of columns
columns = ["step", "type", "amount", "nameorig", "oldbalanceorg", "newbalanceorig", "namedest", "oldbalancedest", "newbalancedest", "isfraud", "isflaggedfraud"]
insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
    sql.Identifier(table_name),
    sql.SQL(', ').join(map(sql.Identifier, columns)),
    sql.SQL(', ').join(sql.Placeholder() * len(columns))
)

try:
    cursor.executemany(insert_query, data)
    connection.commit()
    print("Data from CSV file inserted successfully")
except Exception as e:
    connection.rollback()
    print(f"Error: {e}")

connection.close()
print("Database connection closed")
