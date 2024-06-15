import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

print("Step 1: Load the necessary datasets in chunks")
# Step 1: Load the necessary datasets in chunks
chunk_size = 10**6
assignee_chunks = []

for chunk in pd.read_stata("C:\\Users\\meman\\Downloads\\data\\g_assignee_disambiguated.dta", chunksize=chunk_size):
    assignee_chunks.append(chunk)

g_assignee = pd.concat(assignee_chunks, ignore_index=True)

g_patent = pd.read_stata("C:\\Users\\meman\\Downloads\\data\\g_patent.dta")
g_location = pd.read_stata("C:\\Users\\meman\\Downloads\\data\\g_location_cleaned.dta")

# Step 2: Load and append citation files in chunks
citation_chunks = []

print("Step 2: Loading citation files in chunks")
for chunk in pd.read_csv("C:\\Users\\meman\\Downloads\\data\\g_us_patent_citation_1.csv", chunksize=chunk_size):
    citation_chunks.append(chunk)

for chunk in pd.read_csv("C:\\Users\\meman\\Downloads\\data\\g_us_patent_citation_2.csv", chunksize=chunk_size):
    citation_chunks.append(chunk)

g_citation = pd.concat(citation_chunks, ignore_index=True)

print("Citation data columns:", g_citation.columns)

print("Step 2: Saving combined citation data")
# Save the combined citation data for reuse
g_citation.to_csv("C:\\Users\\meman\\Downloads\\data\\g_us_patent_citation_combined.csv", index=False)

print("Step 3: Merging the datasets")
# Step 3: Merge the datasets
patent_assignee = pd.merge(g_patent, g_assignee, on="patent_id", how="left")
patent_assignee_location = pd.merge(patent_assignee, g_location, on="location_id", how="left")

print("Step 4: Filtering for US records only")
# Step 4: Filter for US records only
us_patent_assignee_location = patent_assignee_location[patent_assignee_location['disambig_country'] == "US"].copy()

print("Step 5: Identifying the first patent date for each assignee")
# Step 5: Identify the first patent date for each assignee
us_patent_assignee_location['patent_date'] = pd.to_datetime(us_patent_assignee_location['patent_date'])
us_patent_assignee_location = us_patent_assignee_location.sort_values(by=['assignee_id', 'patent_date'])
us_patent_assignee_location['first_patent_date'] = us_patent_assignee_location.groupby('assignee_id')['patent_date'].transform('first')
us_patent_assignee_location['year_first_patent'] = us_patent_assignee_location['first_patent_date'].dt.year

print("Step 6: Filtering the data for startups founded between 1985 and 2015")
# Step 6: Filter the data for startups founded between 1985 and 2015
filtered_data = us_patent_assignee_location[(us_patent_assignee_location['year_first_patent'] >= 1985) & (us_patent_assignee_location['year_first_patent'] <= 2015)].copy()

print("Step 7: Calculating the quantity of innovation (number of patents within five years)")
# Step 7: Calculate the quantity of innovation (number of patents within five years)
filtered_data['within_5_years'] = filtered_data.groupby('assignee_id')['patent_date'].transform(lambda x: (x - x.min()).dt.days <= 5*365)
filtered_data['quantity_of_innovation'] = filtered_data.groupby('assignee_id')['within_5_years'].transform('sum')

print("Step 8: Preparing citation data")
# Step 8: Prepare citation data by merging with patent data to get citation dates
g_patent['patent_id'] = g_patent['patent_id'].astype(str)
g_citation['citation_patent_id'] = g_citation['citation_patent_id'].astype(str)
g_citation = g_citation.merge(g_patent, left_on='citation_patent_id', right_on='patent_id', how='left')
g_citation = g_citation.rename(columns={'patent_date': 'citation_patent_date'})

print("Step 9: Converting patent_id to the same type in both datasets")
# Check the columns of g_citation
print("g_citation columns:", g_citation.columns)
print("filtered_data columns:", filtered_data.columns)

# Step 9: Convert patent_id to the same type in both datasets
filtered_data['patent_id'] = filtered_data['patent_id'].astype(str)
g_citation['patent_id_x'] = g_citation['patent_id_x'].astype(str)

print("Step 10: Merging the citation data with the filtered patent data")
# Step 10: Merge the citation data with the filtered patent data in chunks
merged_chunks = []

for chunk in np.array_split(g_citation, len(g_citation) // chunk_size + 1):
    merged_chunk = pd.merge(chunk, filtered_data[['patent_id', 'patent_date']], left_on='patent_id_x', right_on='patent_id', how='left')
    merged_chunks.append(merged_chunk)

merged_citations = pd.concat(merged_chunks, ignore_index=True)

print("Step 11: Creating the citation date variable")
# Step 11: Create the citation date variable
merged_citations['citation_patent_date'] = pd.to_datetime(merged_citations['citation_patent_date'])

print("Step 12: Calculating citations within five years")
# Step 12: Calculate citations within five years
merged_citations['within_5_years'] = (merged_citations['citation_patent_date'] - merged_citations['patent_date']).dt.days <= 5*365
citation_counts = merged_citations.groupby('patent_id')['within_5_years'].sum().reset_index()
citation_counts.columns = ['patent_id', 'quality_of_innovation']

print("Step 13: Merging the citation counts back to the filtered data")
# Step 13: Merge the citation counts back into the filtered dataset to include the quality of innovation
final_data = pd.merge(filtered_data, citation_counts, on='patent_id', how='left')

print("Step 14: Calculating the average quantity and quality by founding year")
# Step 14: Calculate the average quantity and quality by founding year
average_innovation = final_data.groupby('year_first_patent')[['quantity_of_innovation', 'quality_of_innovation']].mean().reset_index()

print("Step 15: Visualizing the average quantity and quality of startups' innovative output by founding year")
# Step 15: Visualize the average quantity and quality of startups' innovative output by founding year
fig, ax1 = plt.subplots()

ax2 = ax1.twinx()
ax1.plot(average_innovation['year_first_patent'], average_innovation['quantity_of_innovation'], 'g-')
ax2.plot(average_innovation['year_first_patent'], average_innovation['quality_of_innovation'], 'b-')

ax1.set_xlabel('Founding Year')
ax1.set_ylabel('Average Quantity of Innovation', color='g')
ax2.set_ylabel('Average Quality of Innovation', color='b')

plt.title("Average Quantity and Quality of Startups' Innovative Output by Founding Year")
plt.savefig("C:\\Users\\meman\\Downloads\\Question_5.pdf")

print("Step 16: Generating a table showing the average quantity and quality of startups' innovative output by founding year")
# Step 16: Generate a table showing the average quantity and quality of startups' innovative output by founding year
average_innovation.to_csv("C:\\Users\\meman\\Downloads\\Average_Innovation_Quantity_Quality.csv", index=False)
