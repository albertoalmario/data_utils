import pyodbc
import db_connections

server = db_connections.server
database = db_connections.database
username = db_connections.username
password = db_connections.password
driver = '{ODBC Driver 17 for SQL Server}'
port = '1433'
connection_string = f"DRIVER={driver};SERVER={server};PORT={port};DATABASE={database};UID={username};PWD={password}"


def find_value_in_all_columns_of_tables(p_value_to_search):
    connection = pyodbc.connect(connection_string)

    try:
        cursor = connection.cursor()
        cursor.execute(f"""SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME
                            FROM INFORMATION_SCHEMA.COLUMNS
                            WHERE 1=1
                            AND TABLE_SCHEMA IN ('edw_stage','edw_core')
                            AND TABLE_NAME IN ('tcustomer')
                            AND DATA_TYPE IN ('char','nchar','varchar','nvarchar')
                            ORDER BY ORDINAL_POSITION ASC
                        """)
        columns_to_check = cursor.fetchall()
        
        for row in columns_to_check:

            table_schema = row[1]
            table_name = row[2]
            column_name = row[3]
            qry = F"SELECT COUNT(1) AS row_count FROM [{table_schema}].[{table_name}] WHERE lower([{column_name}]) in ({p_value_to_search})"
            # print(qry)

            cursor.execute(qry)
            record_count = cursor.fetchone()
            if record_count[0] >= 3:
                print(table_name, column_name ,record_count[0])
            

    except Exception as e:
        print(e)

    finally:
        if connection:
            connection.close()

find_value_in_all_columns_of_tables("'renewal', 'cancellation', 'endorsement'")

