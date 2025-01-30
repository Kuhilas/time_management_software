import psycopg2
from config_2 import config

def all_rows():
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        SQL = 'SELECT * FROM worktime;'
        cursor.execute(SQL)
        row = cursor.fetchall()
        print("Querying all rows from worktime:")
        print(row)
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()

if __name__ == '__main__':
    all_rows()