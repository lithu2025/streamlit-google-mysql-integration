import streamlit as st
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
def insert_data_bulk(connection, table_name, df, update=False):
    if df.empty:
        print(f"⚠️ No data to insert for table `{table_name}`.")
        return

    cleaned_columns = [clean_column_name(col) for col in df.columns]
    df.columns = cleaned_columns

    cursor = connection.cursor()
    try:
        if update:
            print(f"🔄 Updating table `{table_name}` with new data.")
            columns_str = ", ".join(f"`{col}` = %s" for col in cleaned_columns)
            query = f"UPDATE `{table_name}` SET {columns_str} WHERE <primary_column> = %s"
            # Assuming there is a primary key column, update where primary key matches
            data = [tuple(row) for row in df.values]
            cursor.executemany(query, data)
            connection.commit()
            print(f"✅ Updated rows in `{table_name}`.")
        else:
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
                batch_data = data[i:i + BATCH_SIZE]
                cursor.executemany(query, batch_data)
                connection.commit()
                total_inserted += len(batch_data)
                print(f"✅ Inserted batch {i // BATCH_SIZE + 1}: {len(batch_data)} rows")

            print(f"🎉 Inserted total {total_inserted} rows into `{table_name}`.")
    except Error as e:
        print(f"❌ Error in `{table_name}`: {str(e)}")
    finally:
        cursor.close()

# 🔹 Function to get the list of available tables from MySQL
def get_table_names(connection):
    cursor = connection.cursor()
    cursor.execute("SHOW TABLES")
    tables = [table[0] for table in cursor.fetchall()]
    cursor.close()
    return tables

# 🔹 Streamlit UI with Dark Theme and Digital Icons
st.set_page_config(page_title="📊 Google Sheets to MySQL", layout="wide")

# Add CSS for dark theme
st.markdown("""
    <style>
        body {
            background-color: #2b2b2b;
            color: #ffffff;
        }
        .stButton>button {
            background-color: #FF4B4B;
            color: white;
            border-radius: 10px;
            font-size: 14px;
        }
        .stButton>button:hover {
            background-color: #FF2F2F;
        }
        .stTextInput>div>input {
            background-color: #2b2b2b;
            color: white;
            border: 1px solid #FF4B4B;
        }
        .stSelectbox>div>input {
            background-color: #2b2b2b;
            color: white;
            border: 1px solid #FF4B4B;
        }
    </style>
""", unsafe_allow_html=True)

# Title
st.title("📥 **Data Import & Management**")
st.header("⚡ Google Sheets, CSV, and XLSX Integration")

# MySQL Connection Details
HOST = "34.118.200.124"
PORT = "3306"
USER = "digitwebai"
PASS = "digitweb@2025"
DB = "amazon"
SSL_CA = r"C:\Users\digit\Desktop\Amazon\server-ca.pem"
SSL_CERT = r"C:\Users\digit\Desktop\Amazon\client-cert.pem"
SSL_KEY = r"C:\Users\digit\Desktop\Amazon\client-key.pem"

conn = connect_to_gcloud_database(HOST, PORT, USER, PASS, DB, SSL_CA, SSL_CERT, SSL_KEY)

# Allow user to upload multiple CSV or XLSX files
uploaded_files = st.file_uploader("Choose CSV or XLSX files", accept_multiple_files=True, type=["csv", "xlsx"])

# Allow Google Sheet Upload by providing the Sheet ID and Sheet Name
sheet_id = st.text_input("Enter Google Sheet ID (Optional)", "")
sheet_name = st.text_input("Enter Sheet Name (Optional)", "")

update_existing_data = st.checkbox("Update existing data with new data?")

# Process Multiple Files
if uploaded_files:
    for uploaded_file in uploaded_files:
        file_extension = uploaded_file.name.split('.')[-1].lower()
        st.write(f"📂 Processing file: {uploaded_file.name}")
        
        # Process CSV files
        if file_extension == "csv":
            df = pd.read_csv(uploaded_file)
        elif file_extension == "xlsx":
            df = pd.read_excel(uploaded_file)
        
        # Clean column names and insert data
        table_name = clean_column_name(uploaded_file.name)
        insert_data_bulk(conn, table_name, df, update_existing_data)

# Process Google Sheets data
if sheet_id and sheet_name:
    try:
        url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={urllib.parse.quote(sheet_name)}"
        df = pd.read_csv(url)
        st.write(f"✅ Successfully loaded {len(df)} rows from Google Sheets!")
        table_name = clean_column_name(sheet_name)
        insert_data_bulk(conn, table_name, df, update_existing_data)
    except Exception as e:
        st.error(f"❌ Error loading data from Google Sheets: {e}")

# Dynamic Table List from MySQL
tables_in_db = get_table_names(conn)

# Choose Table to View
table_choice = st.selectbox("Choose a table to view:", tables_in_db)

# Show Data from the selected table
if st.button("🔍 Show Data"):
    if conn:
        df = pd.read_sql(f"SELECT * FROM `{table_choice}` LIMIT 1000", conn)
        st.dataframe(df)

        # Allow downloading data
        csv = df.to_csv(index=False).encode("utf-8")
        st.download_button("📥 Download Data as CSV", csv, f"{table_choice}.csv", "text/csv")

# Delete Table Option
if st.button("🔴 Delete Table from MySQL"):
    if table_choice:
        cursor = conn.cursor()
        try:
            cursor.execute(f"DROP TABLE IF EXISTS `{table_choice}`;")
            conn.commit()
            st.success(f"✅ Table `{table_choice}` deleted successfully.")
        except Error as e:
            st.error(f"❌ Error deleting table: {e}")
        finally:
            cursor.close()

        # Refresh the dropdown after table deletion
        tables_in_db = get_table_names(conn)  # Update table list
        table_choice = st.selectbox("Choose a table to view:", tables_in_db)
