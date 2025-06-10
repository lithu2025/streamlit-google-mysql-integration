from sqlalchemy import create_engine

db_user = 'digitwebai'
db_password = 'digitweb@2025'
db_name = 'amazon (Default)'
cloud_sql_connection_name = 'uplifted-stream-461211-h8:us-central1:digitwebai'

# Create PostgreSQL SQLAlchemy engine
db_url = (
    f"postgresql+psycopg2://{db_user}:{db_password}@/"
    f"{db_name}?host=/cloudsql/{cloud_sql_connection_name}"
)

engine = create_engine(db_url)

# Upload BRAND ANALYTIC RP data
dfs['BRAND ANALYTIC RP'].to_sql('brand_analytic_rp', con=engine, if_exists='replace', index=False)

# Upload BUSINESS RP data
dfs['BUSINESS RP'].to_sql('business_rp', con=engine, if_exists='replace', index=False)
