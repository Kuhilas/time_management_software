import psycopg2
from psycopg2.extras import RealDictCursor
from config import config
import json

def db_add_worktime():
    ...

def db_get_persons():
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor(cursor_factory=RealDictCursor)
        SQL = 'SELECT * FROM worktime;'
        cursor.execute(SQL)
        data = cursor.fetchall()
        cursor.close()
        return json.dumps({"person_list": data})
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()

def get_attributes():
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor(cursor_factory=RealDictCursor)
        SQL = 'SELECT * FROM attributes;'
        cursor.execute(SQL)
        data = cursor.fetchall()
        cursor.close()
        return json.dumps({"attributes_list": data})
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
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

#if __name__ == '__main__':
    #print(db_create_person("John"))
    #print(db_update_person(5, "John"))  
    #print(db_delete_person(5))      
    # print(db_get_persons())