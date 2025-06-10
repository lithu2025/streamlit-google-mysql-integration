import mysql.connector
from mysql.connector import Error
import os
import getpass

import mysql.connector
from mysql.connector import Error
import os
import getpass

def connect_to_gcloud_database(host, port, user, password, database, ssl_ca, ssl_cert, ssl_key):
    """
    Establish a connection to a Google Cloud SQL MySQL database using SSL certificates.
    Returns a MySQL connection object, or None if connection fails.
    """
    # Verify SSL certificate files exist
    for cert_file, cert_name in [(ssl_ca, "CA"), (ssl_cert, "Client Certificate"), (ssl_key, "Client Key")]:
        if not os.path.exists(cert_file):
            print(f"Error: {cert_name} file not found at {cert_file}")
            return None

    try:
        if not password:
            password = getpass.getpass(f"Enter MySQL password for user '{user}': ")

        ssl_config = {
            'ssl_ca': ssl_ca,
            'ssl_cert': ssl_cert,
            'ssl_key': ssl_key,
            'ssl_verify_cert': True
        }

        connection = mysql.connector.connect(
            host=host,
            port=int(port),
            user=user,
            password=password,
            database=database,
            **ssl_config
        )

        if connection.is_connected():
            db_info = connection.get_server_info()
            print(f"Connected to MySQL Server version {db_info}")
            return connection

    except Error as e:
        print(f"Error while connecting to MySQL: {str(e)}")
        return None

def execute_query(connection, query, params=None, fetch=False, many=False):
    """
    Execute a SQL query (any type). Optionally fetch results.
    """
    cursor = None
    try:
        cursor = connection.cursor(dictionary=True)
        if many:
            cursor.executemany(query, params)
        else:
            cursor.execute(query, params)
        if fetch:
            result = cursor.fetchall()
            return result
        else:
            connection.commit()
            return cursor.rowcount
    except Error as e:
        print(f"Error executing query: {str(e)}")
        return None
    finally:
        if cursor:
            cursor.close()

def create_table(connection, create_table_sql):
    """
    Create a table in the database.
    """
    result = execute_query(connection, create_table_sql)
    if result is not None:
        print("Table created or already exists.")
        return True
    else:
        print("Failed to create table.")
        return False

def insert_data(connection, table_name, data):
    """
    Insert data (list of dicts) into a table.
    """
    if not data:
        print("No data to insert.")
        return False
    columns = list(data[0].keys())
    columns_str = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
    values = [tuple(item[col] for col in columns) for item in data]
    rowcount = execute_query(connection, query, values, many=True)
    if rowcount is not None:
        print(f"Inserted {rowcount} rows into {table_name}.")
        return True
    return False

def update_data(connection, table_name, update_dict, where_clause="", where_params=None):
    """
    Update rows in a table.
    """
    set_clause = ", ".join([f"{k}=%s" for k in update_dict.keys()])
    query = f"UPDATE {table_name} SET {set_clause}"
    params = list(update_dict.values())
    if where_clause:
        query += f" WHERE {where_clause}"
        if where_params:
            params.extend(where_params)
    rowcount = execute_query(connection, query, params)
    if rowcount is not None:
        print(f"Updated {rowcount} rows in {table_name}.")
        return True
    return False

def delete_data(connection, table_name, where_clause="", where_params=None):
    """
    Delete rows from a table.
    """
    query = f"DELETE FROM {table_name}"
    params = []
    if where_clause:
        query += f" WHERE {where_clause}"
        if where_params:
            params = where_params
    rowcount = execute_query(connection, query, params)
    if rowcount is not None:
        print(f"Deleted {rowcount} rows from {table_name}.")
        return True
    return False

def select_data(connection, query, params=None):
    """
    Execute a SELECT query and fetch results.
    """
    result = execute_query(connection, query, params, fetch=True)
    return result

if __name__ == "__main__":
    # Database connection parameters
    HOST ="34.118.200.124"
    PORT = "3306"
    USER = "digitwebai"
    PASS = "digitweb@2025" # Replace with the actual root password or leave empty to prompt
    DB= "amazon"
    SSL_CA = r"C:\Users\digit\Desktop\Amazon\server-ca.pem"
    SSL_CERT = r"C:\Users\digit\Desktop\Amazon\client-cert.pem"
    SSL_KEY = r"C:\Users\digit\Desktop\Amazon\client-key.pem"

    conn = None
    try:
        conn = connect_to_gcloud_database(HOST, PORT, USER, PASS, DB, SSL_CA, SSL_CERT, SSL_KEY)
        if conn:
            # Create table
            create_table(conn, "CREATE TABLE IF NOT EXISTS test (id INT PRIMARY KEY, name VARCHAR(100));")
            # Insert data
            insert_data(conn, "test", [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}])
            # Update data
            update_data(conn, "test", {"name": "Eve"}, "id=%s", [2])
            # Delete data
            delete_data(conn, "test", "id=%s", [1])
            # Select data
            rows = select_data(conn, "SELECT * FROM test")
            print(rows)
    finally:
        if conn and conn.is_connected():
            conn.close()
            print("Database connection closed.")
            
