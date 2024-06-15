* Question 1 

* Import the patent data
import delimited "C:\Users\meman\Downloads\data\g_patent.csv", clear

* Convert the patent_date to a Stata date format
gen patent_date_stata = date(patent_date, "YMD")
format patent_date_stata %td

* Save the data
save "C:\Users\meman\Downloads\data\g_patent.dta", replace

* Import the assignee data
import delimited "C:\Users\meman\Downloads\data\g_assignee_disambiguated.csv", clear

* Convert patent_id to numeric if needed
destring patent_id, replace force

* Save the assignee data
save "C:\Users\meman\Downloads\data\g_assignee_disambiguated.dta", replace

* Load the patent data again
use "C:\Users\meman\Downloads\data\g_patent.dta", clear

* Convert patent_id to numeric if needed
destring patent_id, replace force

* Merge with the assignee data
merge 1:m patent_id using "C:\Users\meman\Downloads\data\g_assignee_disambiguated.dta"
drop _merge

* Merge with the cleaned location data
merge m:1 location_id using "C:\Users\meman\Downloads\data\g_location_cleaned.dta"
drop _merge

* Filter for US records only
keep if disambig_country == "US"

* Generate the first patent date for each assignee
bysort assignee_id (patent_date_stata): gen first_patent_date = patent_date_stata[1]

* Extract the year from the first patent date
gen year_first_patent = year(first_patent_date)

* Create a variable to identify startups (first patent date)
gen startup = (patent_date_stata == first_patent_date)

* Count the number of startups founded per year
bysort year_first_patent: egen startups_per_year = total(startup)

* Keep only one observation per year for visualization purposes
bysort year_first_patent: keep if _n == 1

* Filter data for the years 1985-2015
keep if year_first_patent >= 1985 & year_first_patent <= 2015

* Visualize the number of startups founded per year
twoway (line startups_per_year year_first_patent)

* Export the graph in a pdf file
graph export "C:\Users\meman\Downloads\Question 1.pdf", as(pdf) name("Graph")




