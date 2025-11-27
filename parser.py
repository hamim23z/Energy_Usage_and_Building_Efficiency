import pandas as pd

INPUT_FILE = "NYC_Building_Energy_and_Water_Data_Disclosure_for_Local_Law_84_2023_to_Present_(Data_for_Calendar_Year_2022-Present)_20251125.csv"
OUTPUT_CSV = "nyc_energy_clean.csv"
OUTPUT_PARQUET = "nyc_energy_clean.parquet"

# Columns to keep
KEEP_COLS = [
    "Property ID",
    "Property Name",
    "Address 1",
    "Postal Code",
    "Year Built",
    "Primary Property Type - Portfolio Manager-Calculated",
    "Latitude",
    "Longitude",
    "Borough",
    "Census Tract (2020)",
    "Neighborhood Tabulation Area (NTA) (2020)",
    "Property GFA - Calculated (Buildings) (ft²)",
    "Site Energy Use (kBtu)",
    "Weather Normalized Site Energy Use (kBtu)",
    "Electricity Use - Grid Purchase (kWh)",
    "Natural Gas Use (therms)",
    "Site EUI (kBtu/ft²)",
    "Weather Normalized Site EUI (kBtu/ft²)",
    "ENERGY STAR Score",
    "Total (Location-Based) GHG Emissions (Metric Tons CO2e)",
    "Total (Location-Based) GHG Emissions Intensity (kgCO2e/ft²)"
]


print("Loading CSV (single chunk)...")
df = pd.read_csv(INPUT_FILE, low_memory=False)

print("Filtering columns...")
df = df[KEEP_COLS]

print("Dropping rows without coordinates...")
df = df.dropna(subset=["Latitude", "Longitude"])

print("Filtering to NYC bounding box...")
df = df[
    (df["Latitude"] > 40.0) & (df["Latitude"] < 41.0) &
    (df["Longitude"] > -75.0) & (df["Longitude"] < -73.0)
]

print(f"Rows remaining: {len(df):,}")

print("Saving cleaned CSV...")
df.to_csv(OUTPUT_CSV, index=False)

print("Saving Parquet (recommended)...")
df.to_parquet(OUTPUT_PARQUET, index=False)

print("Done.")
print(f"→ Clean CSV: {OUTPUT_CSV}")
print(f"→ Parquet:   {OUTPUT_PARQUET}")

df.to_json("data/cleaned_buildings.json", orient="records")
print("Saving JSON...")
print("→ JSON:      data/cleaned_buildings.json")
