# filename: load_parquet_duckdb.py

import duckdb
import pandas as pd

# -------------------------------
# 1️⃣ Connect to DuckDB
# -------------------------------
# In-memory database
con = duckdb.connect(database=':memory:')

# Optional: persistent database
# con = duckdb.connect('nyc_buildings.duckdb')

# -------------------------------
# 2️⃣ Load Parquet file
# -------------------------------
parquet_file = 'nyc_energy_clean.parquet'  # replace with your actual file path

# Create a DuckDB table from Parquet
con.execute(f"""
CREATE TABLE IF NOT EXISTS buildings AS
SELECT *
FROM '{parquet_file}'
""")

print("Parquet file loaded into DuckDB as table 'buildings'.")

# -------------------------------
# 3️⃣ Inspect the table
# -------------------------------
# Preview first 5 rows
df_preview = con.execute("SELECT * FROM buildings LIMIT 5").fetchdf()
print("Preview of data:")
print(df_preview)

# Column info
columns_info = con.execute("PRAGMA table_info(buildings)").fetchdf()
print("\nTable columns:")
print(columns_info)

# -------------------------------
# 4️⃣ Example queries
# -------------------------------

# Total number of rows
total_rows = con.execute("SELECT COUNT(*) FROM buildings").fetchone()[0]
print(f"\nTotal rows in 'buildings': {total_rows}")

# Sample buildings in Manhattan
try:
    manhattan_buildings = con.execute("""
    SELECT *
    FROM buildings
    WHERE "Borough" = 'MANHATTAN'
    LIMIT 5
    """).fetchdf()
    print("\nSample buildings in Manhattan:")
    print(manhattan_buildings)
except Exception as e:
    print("Error querying by borough:", e)

# Select specific columns (corrected)
try:
    cols_df = con.execute("""
    SELECT 
        "Property ID",
        "Property Name",
        "Latitude",
        "Longitude"
    FROM buildings
    LIMIT 5
    """).fetchdf()
    print("\nSelected columns:")
    print(cols_df)
except Exception as e:
    print("Error selecting specific columns:", e)

# -------------------------------
# 5️⃣ Optional: save or export table
# -------------------------------
# con.execute("EXPORT DATABASE 'nyc_buildings.duckdb' (FORMAT PARQUET);")
# print("Database exported to disk.")

print("\nDuckDB setup complete. Ready for queries or visualization!")
