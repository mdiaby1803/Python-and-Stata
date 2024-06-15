import os
import pandas as pd
import dask.dataframe as dd
import statsmodels.api as sm
from sklearn.preprocessing import LabelEncoder

# Function to read Stata files in chunks using Dask
def read_stata_in_chunks(filepath, chunksize=10000):
    return dd.read_stata(filepath, chunksize=chunksize)

# Paths to the datasets
paths = {
    "g_patent": "C:\\Users\\meman\\Downloads\\data\\g_patent.dta",
    "g_assignee": "C:\\Users\\meman\\Downloads\\data\\g_assignee_disambiguated.dta",
    "g_location": "C:\\Users\\meman\\Downloads\\data\\g_location_cleaned.dta",
    "pa": "C:\\Users\\meman\\Downloads\\data\\pa.csv",
    "g_cpc_current": "C:\\Users\\meman\\Downloads\\data\\g_cpc_current.dta",
    "g_us_patent_citation_1": "C:\\Users\\meman\\Downloads\\data\\g_us_patent_citation_1.dta",
    "g_us_patent_citation_2": "C:\\Users\\meman\\Downloads\\data\\g_us_patent_citation_2.dta"
}

# Load the datasets
print("Loading datasets...")
g_patent = read_stata_in_chunks(paths["g_patent"])
g_assignee = read_stata_in_chunks(paths["g_assignee"])
g_location = read_stata_in_chunks(paths["g_location"])
pa = pd.read_csv(paths["pa"])
g_cpc_current = read_stata_in_chunks(paths["g_cpc_current"])
g_us_patent_citation_1 = read_stata_in_chunks(paths["g_us_patent_citation_1"])
g_us_patent_citation_2 = read_stata_in_chunks(paths["g_us_patent_citation_2"])

# Print the columns of each dataframe for debugging purposes
print(f"g_patent columns: {g_patent.columns}")
print(f"g_assignee columns: {g_assignee.columns}")
print(f"g_location columns: {g_location.columns}")
print(f"pa columns: {pa.columns}")
print(f"g_cpc_current columns: {g_cpc_current.columns}")
print(f"g_us_patent_citation_1 columns: {g_us_patent_citation_1.columns}")
print(f"g_us_patent_citation_2 columns: {g_us_patent_citation_2.columns}")

# Convert IDs to strings
print("Converting IDs to strings...")
for df_name, df in zip(["g_patent", "g_assignee", "g_location", "pa", "g_cpc_current", "g_us_patent_citation_1", "g_us_patent_citation_2"],
                       [g_patent, g_assignee, g_location, pa, g_cpc_current, g_us_patent_citation_1, g_us_patent_citation_2]):
    try:
        df['patent_id'] = df['patent_id'].astype(str)
        if 'assignee_id' in df.columns:
            df['assignee_id'] = df['assignee_id'].astype(str)
        if 'location_id' in df.columns:
            df['location_id'] = df['location_id'].astype(str)
        if 'citation_patent_id' in df.columns:
            df['citation_patent_id'] = df['citation_patent_id'].astype(str)
    except KeyError as e:
        print(f"KeyError: {e} in dataset {df_name}")

print("Merging citation datasets...")
# Merge citation datasets
g_us_patent_citation = pd.concat([g_us_patent_citation_1.compute(), g_us_patent_citation_2.compute()])

print("Merging datasets...")
# Merge the assignee and location datasets with the patent dataset
g_patent_assignee = pd.merge(g_patent.compute(), g_assignee.compute(), on='patent_id', how='left')
g_patent_assignee_location = pd.merge(g_patent_assignee, g_location.compute(), on='location_id', how='left')

print("g_patent_assignee merged")
print("g_patent_assignee_location merged")

# Filter for US records only
print("Filtering for US records only...")
us_patent_assignee_location = g_patent_assignee_location[g_patent_assignee_location['disambig_country'] == 'US']
print(f"Number of US records: {len(us_patent_assignee_location)}")

print("Identifying first patent date for each assignee...")
# Identify the first patent date for each assignee
us_patent_assignee_location['patent_date'] = pd.to_datetime(us_patent_assignee_location['patent_date'])
us_patent_assignee_location['first_patent_date'] = us_patent_assignee_location.groupby('assignee_id')['patent_date'].transform('min')
us_patent_assignee_location['year_first_patent'] = us_patent_assignee_location['first_patent_date'].dt.year

print("Filtering data for startups founded between 1985 and 2010...")
# Filter the data for startups founded between 1985 and 2010
filtered_data = us_patent_assignee_location[(us_patent_assignee_location['year_first_patent'] >= 1985) & (us_patent_assignee_location['year_first_patent'] <= 2010)]
print(f"Number of startups founded between 1985 and 2010: {len(filtered_data)}")

print("Calculating quantity of innovation...")
# Calculate the quantity of innovation (number of patents within five years)
filtered_data['within_5_years'] = filtered_data.groupby('assignee_id')['patent_date'].transform(lambda x: (x - x.min()).dt.days <= 5*365)
filtered_data['quantity_of_innovation'] = filtered_data.groupby('assignee_id')['within_5_years'].transform('sum')

print("Identifying acquisitions...")
# Identify acquisitions
pa['patent_id'] = pa['patent_id'].astype(str)
filtered_data['patent_id'] = filtered_data['patent_id'].astype(str)
pa_acquisitions = pa[pa['convey_type'].isin(['assignment', 'merger'])]
merged_acquisitions = pd.merge(filtered_data, pa_acquisitions, on='patent_id', how='left')
merged_acquisitions['acquired'] = pd.notnull(merged_acquisitions['exec_dt'])
merged_acquisitions['exec_dt'] = pd.to_datetime(merged_acquisitions['exec_dt'])
merged_acquisitions['within_10_years'] = (merged_acquisitions['exec_dt'] - merged_acquisitions['first_patent_date']).dt.days <= 10*365
merged_acquisitions['acquired'] = merged_acquisitions['acquired'] & merged_acquisitions['within_10_years']

print("Calculating quality of innovation...")
# Calculate the quality of innovation (citations within five years)
g_patent['patent_date'] = pd.to_datetime(g_patent['patent_date'])
merged_citations = pd.merge(g_us_patent_citation, g_patent[['patent_id', 'patent_date']], left_on='citation_patent_id', right_on='patent_id', suffixes=('', '_citation'))
merged_citations['patent_date_citation'] = pd.to_datetime(merged_citations['patent_date'])
merged_citations['within_5_years'] = (merged_citations['patent_date_citation'] - merged_citations['patent_date']).dt.days <= 5*365
citation_counts = merged_citations.groupby('patent_id')['within_5_years'].sum().reset_index()
citation_counts.columns = ['patent_id', 'quality_of_innovation']

# Merge citation counts with the filtered data
filtered_data = pd.merge(filtered_data, citation_counts, on='patent_id', how='left')

# Merge with CPC data
filtered_data = pd.merge(filtered_data, g_cpc_current.compute(), on='patent_id', how='left')

# Prepare data for regression analysis
regression_data = merged_acquisitions[['assignee_id', 'quantity_of_innovation', 'quality_of_innovation', 'cpc_class', 'disambig_state', 'year_first_patent', 'acquired']].dropna()
regression_data['quantity_of_innovation'] = regression_data['quantity_of_innovation'].astype(float)
regression_data['quality_of_innovation'] = regression_data['quality_of_innovation'].astype(float)
regression_data['acquired'] = regression_data['acquired'].astype(int)

# Encode categorical variables
label_encoder = LabelEncoder()
regression_data['cpc_class'] = label_encoder.fit_transform(regression_data['cpc_class'])
regression_data['disambig_state'] = label_encoder.fit_transform(regression_data['disambig_state'])

# Define the dependent and independent variables
y = regression_data['acquired']
X = regression_data[['quantity_of_innovation', 'quality_of_innovation', 'cpc_class', 'disambig_state', 'year_first_patent']]

# Add a constant to the independent variables
X = sm.add_constant(X)

# Fit the logistic regression model
print("Fitting the logistic regression model...")
logit_model = sm.Logit(y, X).fit()

# Print the summary of the model
print(logit_model.summary())

# Save the output
output_dir = "C:\\Users\\meman\\Downloads\\Meman_Diaby_Answers"
os.makedirs(output_dir, exist_ok=True)
regression_data.to_csv(os.path.join(output_dir, "regression_data.csv"), index=False)
logit_model.save(os.path.join(output_dir, "logit_model.pickle"))

print(f"Output saved to {output_dir}")

