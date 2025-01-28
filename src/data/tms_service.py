import psycopg2
from psycopg2.extras import RealDictCursor
from config import config
import json
from datetime import datetime
from decimal import Decimal

def db_add_worktime(startTime, endTime, lunchBreak, consultantName, customerName):
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor(cursor_factory=RealDictCursor)

        # Convert string times to datetime objects
        startTime = datetime.strptime(startTime, '%Y-%m-%d %H:%M')
        endTime = datetime.strptime(endTime, '%Y-%m-%d %H:%M')

        # Calculate duration
        duration = endTime - startTime

        # Calculate daily hours, subtracting lunch break if True
        dailyHours = round(duration.total_seconds() / 3600, 1) - (0.5 if lunchBreak else 0)

        # Convert dailyHours to Decimal for consistency
        dailyHours = Decimal(str(dailyHours))

        # Insert into worktime table
        insert_worktime_sql = '''
        INSERT INTO worktime (startTime, endTime, lunchBreak, consultantName, customerName, dailyHours)
        VALUES (%s, %s, %s, %s, %s, %s);
        '''
        cursor.execute(insert_worktime_sql, (startTime, endTime, lunchBreak, consultantName, customerName, dailyHours))
        
        # Check if consultant already exists in cumulative table
        check_consultant_sql = '''
        SELECT totalhours FROM cumulative WHERE consultantName = %s;
        '''
        cursor.execute(check_consultant_sql, (consultantName,))
        consultant_record = cursor.fetchone()

        if consultant_record:
            # Debugging output
            print("Consultant record:", consultant_record)

            # Access 'totalhours' in lowercase and ensure type is Decimal
            total_hours = consultant_record.get('totalhours', Decimal('0.0'))
            new_totalHours = total_hours + dailyHours

            # Update cumulative total hours
            update_cumulative_sql = '''
            UPDATE cumulative
            SET totalHours = %s
            WHERE consultantName = %s;
            '''
            cursor.execute(update_cumulative_sql, (new_totalHours, consultantName))
            feedback_message = f"Updated cumulative total hours for {consultantName}. New total: {new_totalHours} hours."
        else:
            # Insert new cumulative record
            insert_cumulative_sql = '''
            INSERT INTO cumulative (consultantName, totalHours)
            VALUES (%s, %s);
            '''
            cursor.execute(insert_cumulative_sql, (consultantName, dailyHours))
            feedback_message = f"Inserted new cumulative record for {consultantName} with total hours: {dailyHours}."

        # Commit the transaction
        con.commit()

        result = {"success": f"Created worktime booking for {consultantName} with {dailyHours} daily hours. {feedback_message}"}
        
        cursor.close()
        return json.dumps(result)

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        return json.dumps({"error": "Error creating worktime booking"})
    
    finally:
        if con is not None:
            con.close()


def db_get_all_worktimes():
    con = None
    try:
        # Connect to the database
        con = psycopg2.connect(**config())
        cursor = con.cursor(cursor_factory=RealDictCursor)

        # Execute query to get all worktime records
        select_all_sql = '''
        SELECT * FROM worktime ORDER BY id ASC;
        '''
        cursor.execute(select_all_sql)
        worktimes = cursor.fetchall()

        cursor.close()
        return worktimes

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        raise Exception("Error fetching worktimes from database")
    
    finally:
        if con is not None:
            con.close()

def db_get_all_cumulative():
    con = None
    try:
        # Connect to the database
        con = psycopg2.connect(**config())
        cursor = con.cursor(cursor_factory=RealDictCursor)

        # Query to fetch all records from the cumulative table
        select_all_sql = '''
        SELECT * FROM cumulative ORDER BY consultantName ASC;
        '''
        cursor.execute(select_all_sql)
        cumulative_records = cursor.fetchall()

        cursor.close()
        return cumulative_records

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        raise Exception("Error fetching cumulative records from database")
    
    finally:
        if con is not None:
            con.close()

def db_get_worktime_by_id(worktime_id):
    con = None
    try:
        # Connect to the database
        con = psycopg2.connect(**config())
        cursor = con.cursor(cursor_factory=RealDictCursor)

        # Execute query to get a specific worktime record
        select_by_id_sql = '''
        SELECT * FROM worktime WHERE id = %s;
        '''
        cursor.execute(select_by_id_sql, (worktime_id,))
        worktime = cursor.fetchone()

        cursor.close()
        return worktime

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        raise Exception(f"Error fetching worktime with ID {worktime_id}")
    
    finally:
        if con is not None:
            con.close()

def db_delete_worktime(worktime_id):
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()

        # SQL query to delete a worktime record by ID
        delete_worktime_sql = '''
        DELETE FROM worktime WHERE id = %s;
        '''
        cursor.execute(delete_worktime_sql, (worktime_id,))

        # Commit the transaction
        con.commit()

        # Check if any rows were deleted
        if cursor.rowcount > 0:
            result = {"success": f"Successfully deleted worktime record with id {worktime_id}."}
        else:
            result = {"error": f"Worktime record with id {worktime_id} not found."}

        cursor.close()
        return json.dumps(result)

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        return json.dumps({"error": "Error deleting worktime record"})

    finally:
        if con is not None:
            con.close()

            
def db_get_attributes_by_id(id):
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor(cursor_factory=RealDictCursor)
        SQL = 'SELECT * FROM attributes where id = %s;'
        cursor.execute(SQL, (id,))
        row = cursor.fetchone()
        cursor.close()
        return json.dumps(row)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()

def db_get_person_by_id(id):
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor(cursor_factory=RealDictCursor)
        SQL = 'SELECT * FROM person where id = %s;'
        cursor.execute(SQL, (id,))
        row = cursor.fetchone()
        cursor.close()
        return json.dumps(row)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()
            
def db_create_person(username, age, student):
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor(cursor_factory=RealDictCursor)
        SQL = 'INSERT INTO person (name, age, student) VALUES (%s, %s, %s);'
        cursor.execute(SQL, (username, age, student))
        con.commit()
        result = {"success": "created person name: %s " % username}
        cursor.close()
        return json.dumps(result)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()

def db_create_attribute(attribute_name, attribute_description, attribute_value, person_id):
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor(cursor_factory=RealDictCursor)
        SQL = '''INSERT INTO attributes (attribute_name, attribute_description, attribute_value, person_id)
        VALUES (%s, %s, %s, %s);'''
        cursor.execute(SQL, (attribute_name, attribute_description, attribute_value, person_id))
        con.commit()
        result = {"success": "created attribute name: %s " % attribute_name}
        cursor.close()
        return json.dumps(result)

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()

def db_update_person(id, username, age, student):
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor(cursor_factory=RealDictCursor)
        SQL = 'UPDATE person SET name = %s, age = %s, student = %s WHERE id = %s;'
        cursor.execute(SQL, (username, age, student, id))
        con.commit()
        cursor.close()
        result = {"success": "updated person id: %s " % id}
        return json.dumps(result)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()

def db_update_attribute(id, attribute_name, attribute_description, attribute_value, person_id):
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor(cursor_factory=RealDictCursor)
        SQL = '''
        UPDATE attributes 
        SET attribute_name = %s, attribute_description = %s, attribute_value = %s, person_id = %s 
        WHERE id = %s;
        '''
        cursor.execute(SQL, (attribute_name, attribute_description, attribute_value, person_id, id))
        con.commit()
        cursor.close()
        result = {"success": f"Updated attribute id: {id}"}
        return result
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Database error: {error}")
        raise
    finally:
        if con is not None:
            con.close()

def db_delete_attribute(id):
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor(cursor_factory=RealDictCursor)
        SQL = 'DELETE FROM attributes WHERE id = %s;'
        cursor.execute(SQL, (id,))
        
        # Check if a row was actually deleted
        if cursor.rowcount == 0:
            return json.dumps({"error": f"No attribute found with id: {id}"})
        
        con.commit()
        cursor.close()
        result = {"success": f"Deleted attribute id: {id}"}
        return json.dumps(result)
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Database error: {error}")
        raise
    finally:
        if con is not None:
            con.close()

def db_delete_person(id):
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor(cursor_factory=RealDictCursor)
        SQL = 'DELETE FROM person WHERE id = %s;'
        cursor.execute(SQL, (id,))
        con.commit()
        cursor.close()
        result = {"success": "deleted person id: %s " % id}
        return json.dumps(result)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()

if __name__ == '__main__':
    ...