import pandas as pd
import numpy as np
from scipy import stats
import matplotlib.pyplot as plt

# Step 1: Load the necessary datasets in chunks
print("Step 1: Loading datasets in chunks")
chunksize = 10**6
g_patent = pd.read_stata("C:\\Users\\meman\\Downloads\\data\\g_patent.dta")
g_assignee = pd.read_stata("C:\\Users\\meman\\Downloads\\data\\g_assignee_disambiguated.dta")
g_location = pd.read_stata("C:\\Users\\meman\\Downloads\\data\\g_location_cleaned.dta")

# Step 2: Load and append citation files in chunks
print("Step 2: Loading and appending citation files")
citation_chunks = []
for chunk in pd.read_csv("C:\\Users\\meman\\Downloads\\data\\g_us_patent_citation_1.csv", chunksize=chunksize):
    citation_chunks.append(chunk)
for chunk in pd.read_csv("C:\\Users\\meman\\Downloads\\data\\g_us_patent_citation_2.csv", chunksize=chunksize):
    citation_chunks.append(chunk)
g_citation = pd.concat(citation_chunks)
print(f"Citation data columns: {g_citation.columns}")

# Step 3: Merging the datasets
print("Step 3: Merging the datasets")
g_patent_assignee = pd.merge(g_patent, g_assignee, on='patent_id', how='left')
g_patent_assignee_location = pd.merge(g_patent_assignee, g_location, on='location_id', how='left')

# Step 4: Filtering for US records only
print("Step 4: Filtering for US records only")
us_patent_assignee_location = g_patent_assignee_location[g_patent_assignee_location['disambig_country'] == 'US']

# Step 5: Identifying the first patent date for each assignee
print("Step 5: Identifying the first patent date for each assignee")
us_patent_assignee_location['patent_date'] = pd.to_datetime(us_patent_assignee_location['patent_date'])
us_patent_assignee_location['first_patent_date'] = us_patent_assignee_location.groupby('assignee_id')['patent_date'].transform('min')
us_patent_assignee_location['year_first_patent'] = us_patent_assignee_location['first_patent_date'].dt.year

# Step 6: Filtering the data for startups founded between 1985 and 2015
print("Step 6: Filtering the data for startups founded between 1985 and 2015")
filtered_data = us_patent_assignee_location[(us_patent_assignee_location['year_first_patent'] >= 1985) & (us_patent_assignee_location['year_first_patent'] <= 2015)]

# Step 7: Calculating the quantity of innovation (number of patents within five years)
print("Step 7: Calculating the quantity of innovation (number of patents within five years)")
filtered_data['within_5_years'] = filtered_data.groupby('assignee_id')['patent_date'].transform(lambda x: (x - x.min()).dt.days <= 5*365)
filtered_data['quantity_of_innovation'] = filtered_data.groupby('assignee_id')['within_5_years'].transform('sum')

# Step 8: Preparing citation data
print("Step 8: Preparing citation data")
g_patent['patent_id'] = g_patent['patent_id'].astype(str)
g_citation['citation_patent_id'] = g_citation['citation_patent_id'].astype(str)
g_citation = g_citation.merge(g_patent, left_on='citation_patent_id', right_on='patent_id', how='left')
g_citation = g_citation.rename(columns={'patent_date': 'citation_patent_date'})

# Ensure both datasets have the same type for patent_id and citation_patent_id
filtered_data['patent_id'] = filtered_data['patent_id'].astype(str)
g_citation['patent_id_x'] = g_citation['patent_id_x'].astype(str)

# Step 9: Converting patent_id to the same type in both datasets
print("Step 9: Converting patent_id to the same type in both datasets")
filtered_data['patent_id'] = filtered_data['patent_id'].astype(str)
g_citation['patent_id_x'] = g_citation['patent_id_x'].astype(str)

# Step 10: Merging the citation data with the filtered patent data
print("Step 10: Merging the citation data with the filtered patent data")
chunk_size = 10**6
merged_chunks = []

for chunk in np.array_split(g_citation, len(g_citation) // chunk_size + 1):
    merged_chunk = pd.merge(chunk, filtered_data[['patent_id', 'patent_date']], left_on='patent_id_x', right_on='patent_id', how='left')
    merged_chunks.append(merged_chunk)

merged_citations = pd.concat(merged_chunks)

# Step 11: Creating the citation date variable
print("Step 11: Creating the citation date variable")
merged_citations['citation_patent_date'] = pd.to_datetime(merged_citations['citation_patent_date'])
merged_citations['within_5_years'] = (merged_citations['citation_patent_date'] - merged_citations['patent_date']).dt.days <= 5*365
citation_counts = merged_citations.groupby('patent_id')['within_5_years'].sum().reset_index()
citation_counts = citation_counts.rename(columns={'within_5_years': 'quality_of_innovation'})

# Step 12: Merging citation counts with the filtered data
print("Step 12: Merging citation counts with the filtered data")
filtered_data = pd.merge(filtered_data, citation_counts, on='patent_id', how='left')

# Step 13: Filtering the data for startups founded between 1995-1999 and 2003-2007
print("Step 13: Filtering the data for startups founded between 1995-1999 and 2003-2007")
filtered_95_99 = filtered_data[(filtered_data['year_first_patent'] >= 1995) & (filtered_data['year_first_patent'] <= 1999)]
filtered_03_07 = filtered_data[(filtered_data['year_first_patent'] >= 2003) & (filtered_data['year_first_patent'] <= 2007)]

# Step 14: Calculating the average quantity and quality of innovation for each group
print("Step 14: Calculating the average quantity and quality of innovation for each group")
avg_quantity_95_99 = filtered_95_99['quantity_of_innovation'].mean()
avg_quantity_03_07 = filtered_03_07['quantity_of_innovation'].mean()

avg_quality_95_99 = filtered_95_99['quality_of_innovation'].mean()
avg_quality_03_07 = filtered_03_07['quality_of_innovation'].mean()

print(f"Avg Quantity of Innovation (1995-1999): {avg_quantity_95_99}")
print(f"Avg Quantity of Innovation (2003-2007): {avg_quantity_03_07}")
print(f"Avg Quality of Innovation (1995-1999): {avg_quality_95_99}")
print(f"Avg Quality of Innovation (2003-2007): {avg_quality_03_07}")

# Step 15: Perform statistical tests to check for significant differences
print("Step 15: Performing statistical tests to check for significant differences")
t_stat_quantity, p_value_quantity = stats.ttest_ind(filtered_95_99['quantity_of_innovation'].dropna(), filtered_03_07['quantity_of_innovation'].dropna())
t_stat_quality, p_value_quality = stats.ttest_ind(filtered_95_99['quality_of_innovation'].dropna(), filtered_03_07['quality_of_innovation'].dropna())

print(f"T-test for Quantity of Innovation: t-statistic = {t_stat_quantity}, p-value = {p_value_quantity}")
print(f"T-test for Quality of Innovation: t-statistic = {t_stat_quality}, p-value = {p_value_quality}")

# Step 16: Visualize the results
print("Step 16: Visualizing the results")
labels = ['1995-1999', '2003-2007']
quantity_means = [avg_quantity_95_99, avg_quantity_03_07]
quality_means = [avg_quality_95_99, avg_quality_03_07]

x = np.arange(len(labels))  # the label locations
width = 0.35  # the width of the bars

fig, ax1 = plt.subplots()

rects1 = ax1.bar(x - width/2, quantity_means, width, label='Quantity of Innovation')
rects2 = ax1.bar(x + width/2, quality_means, width, label='Quality of Innovation')

# Add some text for labels, title and axes ticks
ax1.set_xlabel('Founding Year Group')
ax1.set_ylabel('Average Innovation')
ax1.set_title('Average Quantity and Quality of Innovation by Founding Year Group')
ax1.set_xticks(x)
ax1.set_xticklabels(labels)
ax1.legend()

fig.tight_layout()

plt.show()

# Step 17: Save the results to a CSV file
print("Step 17: Saving the results to a CSV file")
results = pd.DataFrame({
    'Year_Group': ['1995-1999', '2003-2007'],
    'Avg_Quantity_of_Innovation': [avg_quantity_95_99, avg_quantity_03_07],
    'Avg_Quality_of_Innovation': [avg_quality_95_99, avg_quality_03_07],
    'T-statistic Quantity': [t_stat_quantity, t_stat_quantity],
    'P-value Quantity': [p_value_quantity, p_value_quantity],
    'T-statistic Quality': [t_stat_quality, t_stat_quality],
    'P-value Quality': [p_value_quality, p_value_quality]
})

results.to_csv("C:\\Users\\meman\\Downloads\\Innovation_Comparison_Results.csv", index=False)
print("Results saved to Innovation_Comparison_Results.csv")
