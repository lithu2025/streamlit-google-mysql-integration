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

# 🔹 Connect to MySQL with SSL - FIXED VERSION
def connect_to_gcloud_database(host, port, user, password, database, ssl_ca, ssl_cert, ssl_key):
    # Check if SSL certificates exist (only if provided)
    if ssl_ca and ssl_cert and ssl_key:
        for cert_file, cert_name in [(ssl_ca, "CA"), (ssl_cert, "Client Certificate"), (ssl_key, "Client Key")]:
            if not os.path.exists(cert_file):
                st.error(f"Error: {cert_name} file not found at {cert_file}")
                return None
    
    try:
        # Try SSL connection first
        if ssl_ca and ssl_cert and ssl_key:
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
        else:
            # Try connection without SSL
            connection = mysql.connector.connect(
                host=host,
                port=int(port),
                user=user,
                password=password,
                database=database,
                ssl_disabled=True  # Explicitly disable SSL
            )

        if connection.is_connected():
            st.success("✅ Connected to MySQL!")
            return connection

    except Error as e:
        st.error(f"❌ Error connecting to MySQL: {str(e)}")
        # Try alternative connection methods
        try:
            st.info("🔄 Trying connection without SSL...")
            connection = mysql.connector.connect(
                host=host,
                port=int(port),
                user=user,
                password=password,
                database=database,
                ssl_disabled=True,
                autocommit=True
            )
            if connection.is_connected():
                st.success("✅ Connected to MySQL (without SSL)!")
                return connection
        except Error as e2:
            st.error(f"❌ Alternative connection also failed: {str(e2)}")
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
    st.success(f"✅ Table `{table_name}` created with auto-detected columns.")

# 🔹 Drop, recreate, and batch insert data
def insert_data_bulk(connection, table_name, df, update=False):
    if df.empty:
        st.warning(f"⚠️ No data to insert for table `{table_name}`.")
        return

    cleaned_columns = [clean_column_name(col) for col in df.columns]
    df.columns = cleaned_columns

    cursor = connection.cursor()
    try:
        if update:
            st.info(f"🔄 Updating table `{table_name}` with new data.")
            columns_str = ", ".join(f"`{col}` = %s" for col in cleaned_columns)
            query = f"UPDATE `{table_name}` SET {columns_str} WHERE <primary_column> = %s"
            # Assuming there is a primary key column, update where primary key matches
            data = [tuple(row) for row in df.values]
            cursor.executemany(query, data)
            connection.commit()
            st.success(f"✅ Updated rows in `{table_name}`.")
        else:
            # Drop table if exists and create new
            cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`;")
            create_table_auto(cursor, table_name, df)

            # Insert data in batches
            columns_str = ", ".join(f"`{col}`" for col in cleaned_columns)
            placeholders = ", ".join(["%s"] * len(cleaned_columns))
            query = f"INSERT INTO `{table_name}` ({columns_str}) VALUES ({placeholders})"
            data = [tuple(row) for row in df.values]

            BATCH_SIZE = 1000  # Reduced batch size for stability
            total_inserted = 0

            for i in range(0, len(data), BATCH_SIZE):
                batch_data = data[i:i + BATCH_SIZE]
                cursor.executemany(query, batch_data)
                connection.commit()
                total_inserted += len(batch_data)
                st.info(f"✅ Inserted batch {i // BATCH_SIZE + 1}: {len(batch_data)} rows")

            st.success(f"🎉 Inserted total {total_inserted} rows into `{table_name}`.")
    except Error as e:
        st.error(f"❌ Error in `{table_name}`: {str(e)}")
    finally:
        cursor.close()

# 🔹 Function to get the list of available tables from MySQL - FIXED
def get_table_names(connection):
    if connection is None:
        st.error("❌ No database connection available")
        return []
    
    try:
        cursor = connection.cursor()
        cursor.execute("SHOW TABLES")
        tables = [table[0] for table in cursor.fetchall()]
        cursor.close()
        return tables
    except Error as e:
        st.error(f"❌ Error fetching table names: {str(e)}")
        return []

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

# Connection status indicator
st.markdown("---")
st.subheader("🔌 Database Connection")

# MySQL Connection Details - UPDATED
HOST = "34.118.200.124"
PORT = "3306"
USER = "digitwebai"
PASS = "digitweb@2025"
DB = "amazon"

# SSL Certificate paths - Make these optional
SSL_CA = r"C:\Users\digit\Desktop\Amazon\server-ca.pem" if os.path.exists(r"C:\Users\digit\Desktop\Amazon\server-ca.pem") else None
SSL_CERT = r"C:\Users\digit\Desktop\Amazon\client-cert.pem" if os.path.exists(r"C:\Users\digit\Desktop\Amazon\client-cert.pem") else None
SSL_KEY = r"C:\Users\digit\Desktop\Amazon\client-key.pem" if os.path.exists(r"C:\Users\digit\Desktop\Amazon\client-key.pem") else None

# Establish connection
conn = connect_to_gcloud_database(HOST, PORT, USER, PASS, DB, SSL_CA, SSL_CERT, SSL_KEY)

# Show connection status
if conn:
    st.success("🟢 Database Connected Successfully")
else:
    st.error("🔴 Database Connection Failed")
    st.info("Please check your connection details and SSL certificates")

st.markdown("---")

# Only show UI if connected
if conn:
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
            table_name = clean_column_name(uploaded_file.name.replace('.csv', '').replace('.xlsx', ''))
            insert_data_bulk(conn, table_name, df, update_existing_data)

    # Process Google Sheets data
    if sheet_id and sheet_name:
        try:
            url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={urllib.parse.quote(sheet_name)}"
            df = pd.read_csv(url)
            st.success(f"✅ Successfully loaded {len(df)} rows from Google Sheets!")
            table_name = clean_column_name(sheet_name)
            insert_data_bulk(conn, table_name, df, update_existing_data)
        except Exception as e:
            st.error(f"❌ Error loading data from Google Sheets: {e}")

    # Dynamic Table List from MySQL
    tables_in_db = get_table_names(conn)

    if tables_in_db:
        # Choose Table to View
        table_choice = st.selectbox("Choose a table to view:", tables_in_db)

        # Show Data from the selected table
        if st.button("🔍 Show Data"):
            try:
                df = pd.read_sql(f"SELECT * FROM `{table_choice}` LIMIT 1000", conn)
                st.dataframe(df)

                # Allow downloading data
                csv = df.to_csv(index=False).encode("utf-8")
                st.download_button("📥 Download Data as CSV", csv, f"{table_choice}.csv", "text/csv")
            except Exception as e:
                st.error(f"❌ Error loading table data: {e}")

        # Delete Table Option
        if st.button("🔴 Delete Table from MySQL"):
            if table_choice:
                cursor = conn.cursor()
                try:
                    cursor.execute(f"DROP TABLE IF EXISTS `{table_choice}`;")
                    conn.commit()
                    st.success(f"✅ Table `{table_choice}` deleted successfully.")
                    st.experimental_rerun()  # Refresh the page
                except Error as e:
                    st.error(f"❌ Error deleting table: {e}")
                finally:
                    cursor.close()
    else:
        st.info("📋 No tables found in the database. Upload some data to get started!")

else:
    st.warning("⚠️ Please fix the database connection to use this application.")
    
    # Connection troubleshooting tips
    with st.expander("🔧 Connection Troubleshooting"):
        st.markdown("""
        **Common Issues:**
        
        1. **SSL Certificate Issues:**
           - Ensure SSL certificate files exist at the specified paths
           - Check file permissions
           
        2. **Network Issues:**
           - Verify your IP is in Cloud SQL authorized networks
           - Check if Cloud SQL instance is running
           
        3. **Authentication Issues:**
           - Verify username and password
           - Ensure user has proper database permissions
           
        4. **Alternative Solutions:**
           - Try using Cloud SQL Proxy
           - Use private IP if running on Google Cloud
           - Temporarily disable SSL for testing
        """)
