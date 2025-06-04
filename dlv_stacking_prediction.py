import pandas as pd
import pyodbc
import datetime

# Connect to Hoag's SQL Server
conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=hoag_server;"  # Replace with Hoag's server name
    "DATABASE=HoagDonorDB;"
    "Trusted_Connection=yes;"
)
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# Query donor data
query = """
SELECT DonorID, TotalDonationAmount, NumberOfGifts, FirstDonationDate, LastDonationDate
FROM DonorData
WHERE DonationYear = 2025
"""
df = pd.read_sql(query, conn)

# Calculate CLV components
df['AverageDonationValue'] = df['TotalDonationAmount'] / df['NumberOfGifts']
df['DonationFrequency'] = df['NumberOfGifts'] / (
    (pd.to_datetime(df['LastDonationDate']) - pd.to_datetime(df['FirstDonationDate'])).dt.days / 365.25
)
df['DonorValue'] = df['AverageDonationValue'] * df['DonationFrequency']

# Estimate donor lifespan (assume 10% churn rate; adjust with actual data)
churn_rate = 0.10
df['DonorLifespan'] = 1 / churn_rate

# Calculate CLV
df['CLV'] = df['DonorValue'] * df['DonorLifespan']

# Adjust for fundraising costs (assume 20% of donations)
fundraising_cost_ratio = 0.20
df['AdjustedCLV'] = df['CLV'] * (1 - fundraising_cost_ratio)

# Calculate CPA (assume $10M spend, 500 new donors)
total_fundraising_spend = 10000000
new_donors = 500
cpa = total_fundraising_spend / new_donors
df['CPA'] = cpa

# Segment donors by CLV
df['CLVSegment'] = pd.cut(
    df['AdjustedCLV'],
    bins=[0, 10000, 50000, float('inf')],
    labels=['Low Value', 'Medium Value', 'High Value']
)

# Flag acquisition efficiency
df['AcquisitionEfficiency'] = df['AdjustedCLV'].apply(lambda x: 'Profitable' if x > cpa else 'Unprofitable')

# Export results to SQL Server
cursor.execute("IF OBJECT_ID('DonorCLVSegmentation') IS NOT NULL DROP TABLE DonorCLVSegmentation")
cursor.execute("""
CREATE TABLE DonorCLVSegmentation (
    DonorID INT,
    CLV FLOAT,
    AdjustedCLV FLOAT,
    CPA FLOAT,
    CLVSegment VARCHAR(50),
    AcquisitionEfficiency VARCHAR(50),
    CalculationDate DATE
)
""")
for index, row in df.iterrows():
    cursor.execute(
        """
        INSERT INTO DonorCLVSegmentation (
            DonorID, CLV, AdjustedCLV, CPA, CLVSegment, AcquisitionEfficiency, CalculationDate
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        row['DonorID'],
        row['CLV'],
        row['AdjustedCLV'],
        row['CPA'],
        row['CLVSegment'],
        row['AcquisitionEfficiency'],
        datetime.datetime.now().date()
    )
conn.commit()

# Generate summary for Power BI
clv_summary = df.groupby('CLVSegment').agg({
    'DonorID': 'count',
    'AdjustedCLV': 'mean',
    'TotalDonationAmount': 'sum',
    'CPA': 'mean'
}).reset_index()
clv_summary.to_csv('clv_cpa_summary.csv', index=False)  # For Power BI import

# Close connection
conn.close()