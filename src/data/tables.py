import psycopg2
from config import config

def create_table():
    """Function to create 'worktime' and 'cumulative' tables and print their data from the Azure PostgreSQL database."""
    try:
        # Get the PostgreSQL configuration details (use 'postgresql' or 'azure' section)
        db_config = config(section='azure')  # Use 'azure' if you want Azure-specific settings

        # Connect to the PostgreSQL database
        connection = psycopg2.connect(**db_config)
        print("Connected to the database successfully.")

        # Create a cursor object to execute SQL commands
        cursor = connection.cursor()

        # Define SQL command to create the 'worktime' table
        create_worktime_table = """
        CREATE TABLE IF NOT EXISTS worktime (
            ID SERIAL PRIMARY KEY,
            startTime TIMESTAMP NOT NULL,
            endTime TIMESTAMP NOT NULL,
            lunchBreak BOOLEAN DEFAULT FALSE,
            consultantName VARCHAR(255),
            customerName VARCHAR(255),
            dailyHours INT
        );
        """

        # Define SQL command to create the 'cumulative' table with consultantName as foreign key
        create_cumulative_table = """
        CREATE TABLE IF NOT EXISTS cumulative (
            ID SERIAL PRIMARY KEY,
            consultantName VARCHAR(255),
            totalHours INT
        );
        """

        # Execute the SQL command to create the 'worktime' table
        cursor.execute(create_worktime_table)
        print("Table 'worktime' created successfully.")

        # Execute the SQL command to create the 'cumulative' table
        cursor.execute(create_cumulative_table)
        print("Table 'cumulative' created successfully.")

        # Commit changes to the database
        connection.commit()

    except Exception as error:
        print(f"An error occurred: {error}")
    finally:
        # Close the cursor and connection
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals():
            connection.close()
            print("Database connection closed.")

def query_tables():
    try:
        # Get the PostgreSQL configuration details (use 'postgresql' or 'azure' section)
        db_config = config(section='azure')  # Use 'azure' if you want Azure-specific settings

        # Connect to the PostgreSQL database
        connection = psycopg2.connect(**db_config)
        print("Connected to the database successfully.")

        # Create a cursor object to execute SQL commands

        cursor = connection.cursor()
        # Query the 'worktime' and 'cumulative' tables to retrieve and print their columns
        query_worktime = "SELECT * FROM worktime;"
        query_cumulative = "SELECT * FROM cumulative;"

        # Execute the queries
        cursor.execute(query_worktime)
        worktime_rows = cursor.fetchall()
        
        cursor.execute(query_cumulative)
        cumulative_rows = cursor.fetchall()

        # Print out the columns and data from the 'worktime' table
        print("\nData from 'worktime' table:")
        for row in worktime_rows:
            print(row)

        # Print out the columns and data from the 'cumulative' table
        print("\nData from 'cumulative' table:")
        for row in cumulative_rows:
            print(row)

    except Exception as error:
        print(f"An error occurred: {error}")
    finally:
        # Close the cursor and connection
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals():
            connection.close()
            print("Database connection closed.")

if __name__ == "__main__":
    create_table()
    query_tables()