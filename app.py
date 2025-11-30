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
    
    # DEBUG: Print unique boroughs found in the data to help troubleshoot
    try:
        print("Checking available boroughs in dataset...")
        boroughs = con.execute("SELECT DISTINCT Borough FROM 'nyc_energy_clean.parquet'").fetchall()
        clean_boroughs = [str(b[0]).strip() for b in boroughs if b[0] is not None]
        print(f"Found Boroughs: {clean_boroughs}")
    except Exception as e:
        print(f"Could not list boroughs (File might be missing or empty): {e}")

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
            # Simple health check query
            df = con.execute("SELECT * FROM 'nyc_energy_clean.parquet' LIMIT 5").fetchdf()
        return jsonify({
            "columns": df.columns.tolist(),
            "sample_data": df.head(5).to_dict(orient="records")
        })
    except Exception as e:
        return jsonify({"error": str(e)})


@app.route('/api/property_types')
def get_property_types():
    """Helper endpoint to get list of property types for a dropdown"""
    try:
        query = """
        SELECT DISTINCT "Primary Property Type - Portfolio Manager-Calculated" as ptype
        FROM 'nyc_energy_clean.parquet'
        ORDER BY ptype
        """
        with db_lock:
            df = con.execute(query).fetchdf()
        return jsonify(df['ptype'].dropna().tolist())
    except Exception as e:
        return jsonify({"error": str(e)})


@app.route('/api/buildings')
def get_buildings():
    try:
        # Parse Query Parameters 
        borough    = request.args.get('borough', "")
        min_energy = float(request.args.get('min_energy', "0"))
        max_energy = float(request.args.get('max_energy', "100"))
        max_ghg    = float(request.args.get('max_ghg', "50000000")) # Default high to include all
        min_year   = int(request.args.get('min_year', "0"))
        max_year   = int(request.args.get('max_year', "2030"))
        min_gfa    = float(request.args.get('min_gfa', "0"))
        max_gfa    = float(request.args.get('max_gfa', "100000000"))
        min_eui    = float(request.args.get('min_eui', "0"))
        max_eui    = float(request.args.get('max_eui', "100000"))
        prop_type  = request.args.get('property_type', "")

        print(f"\n{'='*60}")
        print(f"GET /api/buildings")
        print(f"Filters: Boro='{borough}', Energy={min_energy}-{max_energy}")
        print(f"         Year={min_year}-{max_year}, GFA={min_gfa}-{max_gfa}, Type='{prop_type}'")
        
        query = """
            SELECT 
                "Property Name", 
                TRY_CAST(Latitude AS DOUBLE) as lat,
                TRY_CAST(Longitude AS DOUBLE) as lng,
                COALESCE(TRY_CAST("ENERGY STAR Score" AS INTEGER), 50) as energy_clean,
                COALESCE(TRY_CAST("Total (Location-Based) GHG Emissions (Metric Tons CO2e)" AS DOUBLE), 0) as ghg_clean,
                "Postal Code",
                
                -- New Columns for Response & Filtering
                TRY_CAST("Year Built" AS INTEGER) as year_built,
                TRY_CAST("Property GFA - Calculated (Buildings) (ft²)" AS DOUBLE) as gfa,
                TRY_CAST("Site EUI (kBtu/ft²)" AS DOUBLE) as site_eui,
                "Primary Property Type - Portfolio Manager-Calculated" as prop_type
                
            FROM 'nyc_energy_clean.parquet'
            WHERE 
                lat IS NOT NULL 
                AND lng IS NOT NULL
                AND energy_clean BETWEEN ? AND ?
                AND ghg_clean <= ?
                AND (year_built IS NULL OR year_built BETWEEN ? AND ?)
                AND (gfa IS NULL OR gfa BETWEEN ? AND ?)
                AND (site_eui IS NULL OR site_eui BETWEEN ? AND ?)
        """
        
        # Params must match the order of ? in query
        params = [
            min_energy, max_energy, 
            max_ghg,
            min_year, max_year,
            min_gfa, max_gfa,
            min_eui, max_eui
        ]
        
        if borough:
            clean_borough = borough.upper().strip()
            if clean_borough == "STATEN ISLAND":
                query += " AND (UPPER(TRIM(Borough)) = ? OR UPPER(TRIM(Borough)) = 'RICHMOND' OR UPPER(TRIM(Borough)) = 'STATEN IS')"
                params.append("STATEN ISLAND")
            else:
                query += " AND UPPER(TRIM(Borough)) = ?"
                params.append(clean_borough)

        if prop_type:
            query += " AND UPPER(prop_type) = ?"
            params.append(prop_type.upper())
            
        # Increase to see more results, but will slow down page
        query += " LIMIT 1500"
        
        with db_lock:
            df = con.execute(query, params).fetchdf()
            
        print(f"Fetched {len(df)} matching buildings from DuckDB")
        
        result = []
        for _, row in df.iterrows():
            result.append({
                'Property Name': str(row['Property Name']),
                'Latitude': float(row['lat']),
                'Longitude': float(row['lng']),
                'energy': float(row['energy_clean']),
                'ghg': float(row['ghg_clean']),
                'postal': str(row['Postal Code']).strip(),
                
                'year': int(row['year_built']) if pd.notna(row['year_built']) else None,
                'gfa': float(row['gfa']) if pd.notna(row['gfa']) else None,
                'eui': float(row['site_eui']) if pd.notna(row['site_eui']) else None,
                'type': str(row['prop_type']) if row['prop_type'] else "Unknown"
            })
        
        print(f"Returning {len(result)} buildings")
        print(f"{'='*60}\n")
        
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
        
        # Robust Aggregation Query
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
        
        # Clean postal codes
        result['postal'] = result['postal'].apply(lambda x: x.split('.')[0] if x else x)
        
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