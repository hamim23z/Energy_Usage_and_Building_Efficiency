# Now this file takes the output parquet that David got from the parser.py. Again, it's still huge, but not as big as before. In order to actually 
# run a test sample on the site and the pipeline, we use only 2000 from each borough, so 10,000 in total. And then to avoid any bias, its shuffled
# and then this sample is then saved as "nyc_energy_sample.parquet". THIS IS JUST A SAMPLE, NOT THE ENTIRE PARQUET. 

import pandas as pd

df = pd.read_parquet('nyc_energy_clean.parquet')  # replace with your actual file path
print(df.shape[0])

# Take only 2000 from each borough for a smaller sample
boroughs = df['Borough'].unique()
sampled_dfs = []
for borough in boroughs:
    borough_df = df[df['Borough'] == borough].head(2000)
    sampled_dfs.append(borough_df)
sampled_df = pd.concat(sampled_dfs)
sampled_df = sampled_df.sample(frac=1, random_state=42)

# randomize the data to avoid query bias/patterns
sampled_df.to_parquet('nyc_energy_sample.parquet', index=False)
print(sampled_df.shape[0])
print("Sampled data saved to 'nyc_energy_sample.parquet'.")