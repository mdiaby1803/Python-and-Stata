* Question 3

*g_location_disambiguated.csv Inspection and save of a cleaned version

* Import the location data
import delimited "C:\Users\meman\Downloads\data\g_location_disambiguated.csv", clear
save "C:\Users\meman\Downloads\data\g_location_disambiguated.dta", replace

* Identify duplicates in the location data
duplicates report location_id

* Tag duplicates in the location data
duplicates tag location_id, generate(dup_tag)

* Browse the duplicates to understand their structure
browse if dup_tag > 0

* If necessary, handle duplicates. For example, keep the first occurrence of each duplicate
bysort location_id: keep if _n == 1

* Save the cleaned location data
save "C:\Users\meman\Downloads\data\g_location_cleaned.dta", replace


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

* Count the number of startups per state
bysort disambig_state: egen startups_per_state = total(patent_date_stata == first_patent_date)

* Keep only one observation per state for visualization purposes
bysort disambig_state: keep if _n == 1

* Sort the data by the number of startups per state in descending order
gsort -startups_per_state

* Create a table of startups per state
list disambig_state startups_per_state, noobs

* Save the table as a CSV file
export delimited disambig_state startups_per_state using "C:\Users\meman\Downloads\startups_per_state.csv", replace

* Visualize the number of startups per state
graph hbar startups_per_state, over(disambig_state) asyvars legend(size(vsmall))

* Export the graph
graph export "C:\Users\meman\Downloads\Question_3_US.pdf", as(pdf)
