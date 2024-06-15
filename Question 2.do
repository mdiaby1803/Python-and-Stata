* Question 2

* Load the patent data
use "C:\Users\meman\Downloads\data\g_patent.dta", clear

* Convert patent_id to string
tostring patent_id, replace force

* Save the patent data
save "C:\Users\meman\Downloads\data\g_patent.dta", replace

* Load the assignee data
use "C:\Users\meman\Downloads\data\g_assignee_disambiguated.dta", clear

* Convert patent_id to string
tostring patent_id, replace force

* Save the assignee data
save "C:\Users\meman\Downloads\data\g_assignee_disambiguated.dta", replace

* Load the patent data again
use "C:\Users\meman\Downloads\data\g_patent.dta", clear

* Merge with the assignee data
merge 1:m patent_id using "C:\Users\meman\Downloads\data\g_assignee_disambiguated.dta"

* Keep only the matched observations
drop _merge

* Merge with the cleaned location data
merge m:1 location_id using "C:\Users\meman\Downloads\data\g_location_cleaned.dta"
drop _merge

* Filter for US records only
keep if disambig_country == "US"

* Convert patent_date to Stata date format if not already defined
capture drop patent_date_stata
gen patent_date_stata = date(patent_date, "YMD")
format patent_date_stata %td

* Generate the first patent date for each assignee
bysort assignee_id (patent_date_stata): gen first_patent_date = patent_date_stata[1]

* Extract the year from the first patent date
gen year_first_patent = year(first_patent_date)

* Create a variable to identify startups (first patent date)
gen startup = (patent_date_stata == first_patent_date)

* Count the total number of patents per year
gen year_patent = year(patent_date_stata)
bysort year_patent: egen total_patents_per_year = count(patent_id)

* Count the number of startups' first patents per year
bysort year_first_patent: egen startups_first_patents_per_year = total(startup)

* Compute the share of startups' first patents among all patents
gen share_startups_first_patents = startups_first_patents_per_year / total_patents_per_year

* Filter data for the years 1985-2015
keep if year_first_patent >= 1985 & year_first_patent <= 2015

* Visualize the share of startups' first patents per year
twoway (line share_startups_first_patents year_first_patent)

* Export the graph in a pdf file
graph save "Graph" "C:\Users\meman\Downloads\Meman_Diaby_Answers\Question 2 (Graph).gph"
