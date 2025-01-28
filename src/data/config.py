from configparser import ConfigParser
import os

def config(filename='src\\data\\database.ini', section='azure'):
    parser = ConfigParser()
    parser.read(filename)
    db = {}

    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))
    return db

#def config(filename='src\\data\\database.ini', section='azure'):
    # Create a parser object to read the configuration
    #parser = ConfigParser()
    
    # Check if the .ini file exists
    #if not os.path.exists(filename):
        #raise Exception(f"Configuration file not found at {filename}")

    # Read the configuration file
    #parser.read(filename)

    # Check if the section exists in the .ini file
    #if parser.has_section(section):
        # Get the parameters of the specified section
        #params = parser[section]
        #db_config = {
            #'host': params['host'],
            #'database': params['database'],
            #'user': params['user'],
            #'password': params['password'],
            #'port': params.get('port', 5432),  # Default to 5432 if not specified
            #'sslmode': params.get('sslmode', 'require')  # Use 'require' if not specified
         #}
    #return db
    #else:
        #raise Exception(f"Section {section} not found in the {filename} file")