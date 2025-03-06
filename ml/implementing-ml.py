import sqlite3
import pandas as pd
from sklearn.preprocessing import LabelEncoder, StandardScaler

# Connect to the database
conn = sqlite3.connect("../kairos.db")
query = "SELECT * FROM improved_trades"  # or 'trades' if that's the main table

# Load data into a pandas DataFrame
df = pd.read_sql(query, conn)

# Close the connection
conn.close()

# Copy the DataFrame to avoid modifying the original
df_processed = df.copy()

# Handle missing values by filling with 0 or using a strategy that fits your data
df_processed = df_processed.fillna(0)

print(df_processed.columns)


'''

# Encode categorical columns (for simplicity, we use LabelEncoder here)
categorical_columns = ['strategy', 'symbol', 'direction']  # Add more categorical columns if necessary
encoder = LabelEncoder()

for col in categorical_columns:
    df_processed[col] = encoder.fit_transform(df_processed[col])

# Normalize or scale numerical features
numerical_columns = ['entry_price', 'stop_price', 'exit_price', 'capital_required', 'trade_duration', 'risk_reward', 'perc_return', 'risk_size']
scaler = StandardScaler()

df_processed[numerical_columns] = scaler.fit_transform(df_processed[numerical_columns])

# Display the processed data
print(df_processed.head())
'''