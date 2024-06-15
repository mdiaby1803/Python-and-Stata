* Question 4

* Import the CPC classification data
import delimited "C:\Users\meman\Downloads\data\g_cpc_current.csv", clear

* Convert patent_id to string
tostring patent_id, replace

* Save the converted dataset
save "C:\Users\meman\Downloads\data\g_cpc_current.dta", replace

* Load the necessary datasets
use "C:\Users\meman\Downloads\data\g_patent.dta", clear

* Merge with the assignee data
merge 1:m patent_id using "C:\Users\meman\Downloads\data\g_assignee_disambiguated.dta"
drop _merge

* Merge with the cleaned location data
merge m:1 location_id using "C:\Users\meman\Downloads\data\g_location_cleaned.dta"
drop _merge

* Filter for US records only
keep if disambig_country == "US"

* Identify the first patent date for each assignee
bysort assignee_id (patent_date_stata): gen first_patent_date = patent_date_stata[1]

* Extract the year from the first patent date
gen year_first_patent = year(first_patent_date)

* Filter the data for startups founded between 1985 and 2015
keep if inrange(year_first_patent, 1985, 2015)

* Load CPC classification data and merge with the filtered dataset
merge m:1 patent_id using "C:\Users\meman\Downloads\data\g_cpc_current.dta"
drop _merge

* Count the number of startups per CPC class
bysort cpc_class: egen startups_per_cpc = total(patent_date_stata == first_patent_date)

* Keep only one observation per CPC class for visualization purposes
bysort cpc_class: keep if _n == 1

* Sort the data by the number of startups per CPC class
sort startups_per_cpc

* Generate a table to show the technology fields with the largest number of startups
list cpc_class startups_per_cpc in -20/l

* Save the table as a CSV file
export delimited cpc_class startups_per_cpc using "C:\Users\meman\Downloads\startups_per_cpc.csv", replace

* Visualize the number of startups per CPC class
graph hbar startups_per_cpc, over(cpc_class) asyvars legend(size(vsmall)) title("Number of Startups per CPC Class")

* Export the graph
graph export "C:\Users\meman\Downloads\Question_4.pdf", as(pdf)