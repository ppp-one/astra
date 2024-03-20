import sqlite3

## ADD YOUR VALUES HERE
path_to_db = '../log/SaintEx.db'

field = 'target1'
filter_name = 'I+z'


# Connect to the SQLite database
conn = sqlite3.connect(path_to_db)
cursor = conn.cursor()

# Execute the DELETE statement
qry = f"""
        DELETE
        FROM autoguider_ref
        WHERE field is '{field}'
        AND filter is '{filter_name}'
        """
cursor.execute(qry)

# Commit the changes
conn.commit()

# Close the connection
conn.close()