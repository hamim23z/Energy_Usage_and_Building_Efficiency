from flask import Flask, jsonify, render_template, request
import duckdb
import pandas as pd
import traceback
from threading import Lock

app = Flask(__name__)

# Create a lock for database access
db_lock = Lock()

# Connect to DuckDB
con = duckdb.connect(database=':memory:')

try:
    print("Successfully connected to DuckDB")
except Exception as e:
    print(f"Error connecting: {e}")
    traceback.print_exc()


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/test')
def test():
    try:
        with db_lock:
            df = con.execute("SELECT * FROM 'nyc_energy_clean.parquet' LIMIT 5").fetchdf()
        return jsonify({
            "columns": df.columns.tolist(),
            "sample_data": df.head(5).to_dict(orient="records")
        })
    except Exception as e:
        return jsonify({"error": str(e)})


@app.route('/api/buildings')
def get_buildings():
    try:
        borough    = request.args.get('borough', "")
        min_energy = float(request.args.get('min_energy', "0"))
        max_energy = float(request.args.get('max_energy', "100"))
        max_ghg    = float(request.args.get('max_ghg', "500000"))

        print(f"\n{'='*60}")
        print(f"GET /api/buildings - Filters: borough={borough}, energy={min_energy}-{max_energy}, ghg<={max_ghg}")
        
        # Get data with lock from parquet
        with db_lock:
            df = con.execute("SELECT * FROM 'nyc_energy_clean.parquet' LIMIT 2000").fetchdf()
        
        print(f" Fetched {len(df)} buildings from parquet")
        
        df['Latitude'] = pd.to_numeric(df['Latitude'], errors='coerce')
        df['Longitude'] = pd.to_numeric(df['Longitude'], errors='coerce')
        df = df.dropna(subset=['Latitude', 'Longitude'])
        
        df['energy'] = df['ENERGY STAR Score'].replace('Not Available', None)
        df['energy'] = pd.to_numeric(df['energy'], errors='coerce').fillna(50)

        df['ghg'] = pd.to_numeric(df['Total (Location-Based) GHG Emissions (Metric Tons CO2e)'], errors='coerce').fillna(0)
        
        df['postal'] = df['Postal Code'].astype(str).str.strip()
        
        print(f"After cleaning: {len(df)} buildings")
        
        if borough:
            df = df[df['Borough'].str.upper() == borough.upper()]
            print(f" After borough filter: {len(df)}")
        
        df = df[(df['energy'] >= min_energy) & (df['energy'] <= max_energy)]
        df = df[df['ghg'] <= max_ghg]
        
        print(f"After all filters: {len(df)} buildings")
        print(f"{'='*60}\n")

        result = []
        for _, row in df.head(1500).iterrows():
            result.append({
                'Property Name': str(row['Property Name']),
                'Latitude': float(row['Latitude']),
                'Longitude': float(row['Longitude']),
                'energy': float(row['energy']),
                'ghg': float(row['ghg']),
                'postal': row['postal']
            })
        
        return jsonify(result)
        
    except Exception as e:
        print("ERROR in get_buildings:")
        print(traceback.format_exc())
        return jsonify([])


@app.route("/api/ghg_by_postal")
def ghg_by_postal():
    try:
        print(f"\n{'='*60}")
        print("GET /api/ghg_by_postal")
        
        # Query with aggregation directly in DuckDB to avoid loading all data
        query = """
        SELECT 
            CAST("Postal Code" AS VARCHAR) AS postal,
            SUM(TRY_CAST("Total (Location-Based) GHG Emissions (Metric Tons CO2e)" AS DOUBLE)) AS total_ghg
        FROM 'nyc_energy_clean.parquet'
        WHERE "Postal Code" IS NOT NULL
        GROUP BY postal
        HAVING total_ghg > 0
        ORDER BY total_ghg DESC
        """
        
        print("Executing aggregation query...")
        
        with db_lock:
            result = con.execute(query).fetchdf()
        
        print(f"Query complete: {len(result)} postal codes")
        
        if len(result) > 0:
            print(f"  Top 5 postal codes:")
            for idx, row in result.head(5).iterrows():
                print(f"    {row['postal']}: {row['total_ghg']:,.1f} MT CO2e")
        
        print(f"{'='*60}\n")
        
        return jsonify(result.to_dict(orient='records'))

    except Exception as e:
        print("ERROR in ghg_by_postal:")
        print(traceback.format_exc())
        return jsonify([])


if __name__ == '__main__':
    app.run(debug=True, threaded=True)