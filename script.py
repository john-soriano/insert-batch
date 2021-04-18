
from configparser import ConfigParser
import psycopg2
import psycopg2.extras as psql_extras
import pandas as pd
from typing import Dict


def load_connection_info(
    ini_filename: str
) -> Dict[str, str]:
    parser = ConfigParser()
    parser.read(ini_filename)
    # Create a dictionary of the variables stored under the "postgresql" section of the .ini
    conn_info = {param[0]: param[1] for param in parser.items("postgresql")}
    return conn_info


def create_db(
    conn_info: Dict[str, str],
) -> None:
    # Connect just to PostgreSQL with the user loaded from the .ini file
    psql_connection_string = f"user={conn_info['user']} password={conn_info['password']}"
    conn = psycopg2.connect(psql_connection_string)
    cur = conn.cursor()

    # "CREATE DATABASE" requires automatic commits
    conn.autocommit = True
    sql_query = f"CREATE DATABASE {conn_info['database']}"

    try:
        cur.execute(sql_query)
    except Exception as e:
        print(f"{type(e).__name__}: {e}")
        print(f"Query: {cur.query}")
        cur.close()
    else:
        # Revert autocommit settings
        conn.autocommit = False


def create_table(
    sql_query: str,
    conn: psycopg2.extensions.connection,
    cur: psycopg2.extensions.cursor
) -> None:
    try:
        # Execute the table creation query
        cur.execute(sql_query)
    except Exception as e:
        print(f"{type(e).__name__}: {e}")
        print(f"Query: {cur.query}")
        conn.rollback()
        cur.close()
    else:
        # To take effect, changes need be committed to the database
        conn.commit()


def list_tables(
    schema: str,  # public
    conn: psycopg2.extensions.connection,
    cur: psycopg2.extensions.cursor
) -> list:
    try:
        query = 'SELECT table_name FROM information_schema.tables WHERE (table_schema = \'%s\') ORDER BY table_schema, table_name;' % (
            schema)
        cur.execute(query)
        tables = []
        for table in cur.fetchall():
            tables.append(table[0])
        return tables
    except Exception as error:
        print(f"{type(error).__name__}: {error}")
        print("Query:", cur.query)
        conn.rollback()


def insert_data(
    query: str,
    conn: psycopg2.extensions.connection,
    cur: psycopg2.extensions.cursor,
    df: pd.DataFrame,
    page_size: int
) -> None:
    data_tuples = [tuple(row.to_numpy()) for index, row in df.iterrows()]

    try:
        psql_extras.execute_values(
            cur, query, data_tuples, page_size=page_size)

    except Exception as error:
        print(f"{type(error).__name__}: {error}")
        print("Query:", cur.query)
        conn.rollback()
        cur.close()

    else:
        conn.commit()


if __name__ == "__main__":
   # host, database, user, password
    conn_info = load_connection_info("db.ini")

    # Connect to the database created
    connection = psycopg2.connect(**conn_info)
    cursor = connection.cursor()

    attributes = []

    data_path = input("Select CSV path: ")

    data = pd.read_csv(data_path)

    chunk_size = 100  # chunk row size because psycopg2 don't have solution yet for masive insert
    data_chunks = [data[i:i+chunk_size]
                   for i in range(0, data.shape[0], chunk_size)]

    # csv columns
    headers = data.columns

    table = None

    print("Assign types:\n\n")
    for header in headers:
        value = input(header + " :")
        attributes.append({"name": header, "value": value})

    is_create_table = input(
        "Create table? (Y/n): ")

    if is_create_table == "Y":
        table = input("Table name: ")
        create_table_params = map(lambda x: '"%s" %s' %
                                  (x["name"], x["value"]), attributes)
        create_table_query = 'CREATE TABLE %s (%s)' % (
            table, ', '.join(create_table_params))
        create_table(create_table_query, connection, cursor)

        insert_params = ', '.join(
            map(lambda x: '"%s"' % (x["name"]), attributes))
        insert_query = 'INSERT INTO %s(%s)' % (
            table, insert_params) + " VALUES %s"

        for chunk in data_chunks:
            insert_data(insert_query, connection, cursor, chunk, chunk_size)
    else:
        tables = ', '.join(list_tables('public', connection, cursor))
        table = input('Select table from ' + tables + '\n\nOption: ')
        insert_params = ', '.join(
            map(lambda x: '"%s"' % (x["name"]), attributes))
        insert_query = 'INSERT INTO %s(%s)' % (
            table, insert_params) + " VALUES %s"
        for chunk in data_chunks:
            insert_data(insert_query, connection, cursor, chunk, chunk_size)
    # # select data

    # Close all connections to the database
    connection.close()
    cursor.close()
