import pandas as pd
from google.oauth2.service_account import Credentials
import urllib.parse
from sqlalchemy import create_engine

# 1️⃣ Google Sheets credentials
SERVICE_ACCOUNT_FILE = 'C:/Users/digit/Desktop/Amazon/credentials.json'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)

# 2️⃣ Google Sheets ID and sheet names
SHEET_ID = '1IGfBYoeRpZmENUsGx34orHfmUjuw8gLsETWXP4RVJxA'
SHEET_NAMES = ['BRAND ANALYTIC RP', 'BUSINESS RP']

# 3️⃣ Read data from Google Sheets
dfs = {}
for sheet_name in SHEET_NAMES:
    encoded_sheet_name = urllib.parse.quote(sheet_name)
    url = f'https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet={encoded_sheet_name}'
    print(f"Loading sheet: {sheet_name} from URL: {url}")
    df = pd.read_csv(url)
    dfs[sheet_name] = df

print("\n✅ Sample data from BRAND ANALYTIC RP:")
print(dfs['BRAND ANALYTIC RP'].head())

# 4️⃣ Cloud SQL (MySQL) connection details
db_user = 'digitwebai'
db_password_raw = 'digitweb@2025'
db_password = urllib.parse.quote_plus(db_password_raw)  # encode special characters
db_host = '34.118.200.124'  # Cloud SQL MySQL public IP
db_port = '3306'
db_name = 'amazon'

# 5️⃣ Create SQLAlchemy engine for MySQL
db_url = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
engine = create_engine(db_url)

# 6️⃣ Upload the sheets as tables
print("\n🔄 Uploading data to Cloud SQL (MySQL)...")
dfs['BRAND ANALYTIC RP'].to_sql('brand_analytic_rp', con=engine, if_exists='replace', index=False)
dfs['BUSINESS RP'].to_sql('business_rp', con=engine, if_exists='replace', index=False)

print("\n🎉 Data upload to MySQL complete!")
