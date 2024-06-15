import pandas as pd
import matplotlib.pyplot as plt

# Step 1: Load the necessary datasets
g_patent = pd.read_stata("C:\\Users\\meman\\Downloads\\data\\g_patent.dta")
g_assignee = pd.read_stata("C:\\Users\\meman\\Downloads\\data\\g_assignee_disambiguated.dta")
g_location = pd.read_stata("C:\\Users\\meman\\Downloads\\data\\g_location_cleaned.dta")
pa = pd.read_csv("C:\\Users\\meman\\Downloads\\data\\pa.csv")

# Convert dates
pa['exec_dt'] = pd.to_datetime(pa['exec_dt'])

# Step 2: Merging the datasets
g_patent_assignee = pd.merge(g_patent, g_assignee, on='patent_id', how='left')
g_patent_assignee_location = pd.merge(g_patent_assignee, g_location, on='location_id', how='left')

# Step 3: Filtering for US records only
us_patent_assignee_location = g_patent_assignee_location[g_patent_assignee_location['disambig_country'] == 'US']

# Step 4: Identifying the first patent date for each assignee
us_patent_assignee_location['patent_date'] = pd.to_datetime(us_patent_assignee_location['patent_date'])
us_patent_assignee_location['first_patent_date'] = us_patent_assignee_location.groupby('assignee_id')['patent_date'].transform('min')
us_patent_assignee_location['year_first_patent'] = us_patent_assignee_location['first_patent_date'].dt.year

# Step 5: Filtering the data for startups founded between 1985 and 2010
filtered_data = us_patent_assignee_location[(us_patent_assignee_location['year_first_patent'] >= 1985) & (us_patent_assignee_location['year_first_patent'] <= 2010)]

# Step 6: Identifying acquisitions
pa_acquisitions = pa[pa['convey_type'].str.contains('assignment|merger', case=False, na=False)]

# Convert patent_id to string type for merging
filtered_data['patent_id'] = filtered_data['patent_id'].astype(str)
pa_acquisitions['patent_id'] = pa_acquisitions['patent_id'].astype(str)

# Merge with the startup data to identify acquired startups
merged_acquisitions = pd.merge(filtered_data, pa_acquisitions, on='patent_id', how='left')
merged_acquisitions['acquired'] = merged_acquisitions['exec_dt'].notna()
merged_acquisitions['within_10_years'] = (merged_acquisitions['exec_dt'] - merged_acquisitions['first_patent_date']).dt.days <= 10*365

# Check for duplicates
duplicates_count = merged_acquisitions.duplicated(subset=['patent_id', 'assignee_id'], keep=False).sum()

# Remove duplicates and recalculate shares
merged_acquisitions_no_duplicates = merged_acquisitions.drop_duplicates(subset=['patent_id', 'assignee_id'], keep='first')
acquired_startups = merged_acquisitions_no_duplicates[merged_acquisitions_no_duplicates['within_10_years']].groupby('year_first_patent')['assignee_id'].nunique()
total_startups = merged_acquisitions_no_duplicates.groupby('year_first_patent')['assignee_id'].nunique()
share_acquired = (acquired_startups / total_startups * 100).reset_index()
share_acquired.columns = ['year_first_patent', 'share_acquired']

# Step 8: Visualizing the share of acquired startups by founding year
plt.figure(figsize=(10, 6))
plt.plot(share_acquired['year_first_patent'], share_acquired['share_acquired'], marker='o')
plt.xlabel('Founding Year')
plt.ylabel('Share of Acquired Startups (%)')
plt.title('Share of Acquired Startups by Founding Year')
plt.grid(True)
plt.savefig("C:\\Users\\meman\\Downloads\\Acquired_Startups_Share.pdf")
plt.show()

# Save the results to a CSV file
share_acquired.to_csv("C:\\Users\\meman\\Downloads\\Acquired_Startups_Share.csv", index=False)
print(f"Results saved to Acquired_Startups_Share.csv with {duplicates_count} duplicate entries removed.")
