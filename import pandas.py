import pandas as pd
import urllib.parse
import mysql.connector
from mysql.connector import Error
import os

# 🔹 Function to clean column names
def clean_column_name(col_name):
    col_name = col_name.replace('[', '').replace(']', '')
    col_name = col_name.replace('(', '').replace(')', '')
    col_name = col_name.replace('=', '')
    col_name = col_name.replace('"', '')
    col_name = col_name.replace('.', '')
    col_name = col_name.replace('/', '')
    col_name = col_name.replace('-', '_')
    col_name = col_name.replace(' ', '_')
    col_name = ''.join(char for char in col_name if char.isalnum() or char == '_')
    return col_name.lower()

# 🔹 Connect to MySQL with SSL
def connect_to_gcloud_database(host, port, user, password, database, ssl_ca, ssl_cert, ssl_key):
    for cert_file, cert_name in [(ssl_ca, "CA"), (ssl_cert, "Client Certificate"), (ssl_key, "Client Key")]:
        if not os.path.exists(cert_file):
            print(f"Error: {cert_name} file not found at {cert_file}")
            return None

    try:
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
            print("✅ Connected to MySQL!")
            return connection

    except Error as e:
        print(f"❌ Error connecting to MySQL: {str(e)}")
        return None

# 🔹 Create table based on dataframe (VARCHAR → TEXT!)
def create_table_auto(cursor, table_name, df):
    col_defs = []
    for col, dtype in zip(df.columns, df.dtypes):
        if pd.api.types.is_integer_dtype(dtype):
            sql_type = "BIGINT"
        elif pd.api.types.is_float_dtype(dtype):
            sql_type = "DOUBLE"
        elif pd.api.types.is_bool_dtype(dtype):
            sql_type = "BOOLEAN"
        else:
            sql_type = "TEXT"
        col_defs.append(f"`{col}` {sql_type}")

    columns_sql = ", ".join(col_defs)
    create_sql = f"CREATE TABLE `{table_name}` ({columns_sql});"
    cursor.execute(create_sql)
    print(f"✅ Table `{table_name}` created with auto-detected columns.")

# 🔹 Drop, recreate, and batch insert data
def insert_data_bulk(connection, table_name, df):
    if df.empty:
        print(f"⚠️ No data to insert for table `{table_name}`.")
        return

    cleaned_columns = [clean_column_name(col) for col in df.columns]
    df.columns = cleaned_columns

    cursor = connection.cursor()
    try:
        # Drop table if exists and create new
        cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`;")
        create_table_auto(cursor, table_name, df)
        print(f"✅ Table `{table_name}` dropped & recreated with new columns.")

        # Insert data in batches
        columns_str = ", ".join(f"`{col}`" for col in cleaned_columns)
        placeholders = ", ".join(["%s"] * len(cleaned_columns))
        query = f"INSERT INTO `{table_name}` ({columns_str}) VALUES ({placeholders})"
        data = [tuple(row) for row in df.values]

        BATCH_SIZE = 10000  # Adjust based on system capability
        total_inserted = 0

        for i in range(0, len(data), BATCH_SIZE):
            batch_data = data[i:i+BATCH_SIZE]
            cursor.executemany(query, batch_data)
            connection.commit()
            total_inserted += len(batch_data)
            print(f"✅ Inserted batch {i//BATCH_SIZE + 1}: {len(batch_data)} rows")

        print(f"🎉 Inserted total {total_inserted} rows into `{table_name}`.")
    except Error as e:
        print(f"❌ Error in `{table_name}`: {str(e)}")
    finally:
        cursor.close()

# 🔹 Main script
if __name__ == "__main__":
    SHEET_ID = '1IGfBYoeRpZmENUsGx34orHfmUjuw8gLsETWXP4RVJxA'
    SHEET_NAMES = [
        'BRAND ANALYTIC RP',
        'BUSINESS RP',
        'Traffic Report',
        'Listing ID'
    ]

    # Database details
    HOST = "34.118.200.124"
    PORT = "3306"
    USER = "digitwebai"
    PASS = "digitweb@2025"
    DB = "amazon"
    SSL_CA = r"C:\Users\digit\Desktop\Amazon\server-ca.pem"
    SSL_CERT = r"C:\Users\digit\Desktop\Amazon\client-cert.pem"
    SSL_KEY = r"C:\Users\digit\Desktop\Amazon\client-key.pem"

    conn = connect_to_gcloud_database(HOST, PORT, USER, PASS, DB, SSL_CA, SSL_CERT, SSL_KEY)

    if conn:
        for sheet_name in SHEET_NAMES:
            encoded_sheet_name = urllib.parse.quote(sheet_name)
            url = f'https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet={encoded_sheet_name}'
            print(f"\n🔍 Loading data from sheet: {sheet_name} (URL: {url})")
            df = pd.read_csv(url)
            print(f"🔢 Rows loaded: {len(df)}")
            table_name = clean_column_name(sheet_name)
            insert_data_bulk(conn, table_name, df)

        conn.close()
        print("\n🎉 All sheets uploaded to MySQL (`amazon` DB) successfully!")
