///////////////////////////////////////////////////////////////////////////////////////////////////
*********1969-2000 employment
clear
//import delimited "E:\lsu\bea data\caemp25s\CAEMP25S_AK_1969_2000.csv", numericcols(9,10) 
import delimited "E:\lsu\bea data\caemp25s\CAEMP25S_AK_1969_2000.csv"

drop if region==.  //remove notes at the bottom from csv file


local j=1968
forvalues i=9/40{   //注意更改
local j=`j'+1
if "`:type v`i''" == "long" {           // no space between ' and "
rename v`i' year_`j'
}
else if "`:type v`i''" == "double" {
rename v`i' year_`j'
}
else{
destring v`i', generate(year_`j') force
drop v`i'
}
}


keep geofips geoname description year_*

encode description, gen(descrip)
reshape long year_, i(geofips geoname description) j(year)  //i(varlist) 
rename year_ emp

label save descrip using description.do, replace
drop description

reshape wide emp, i(geofips geoname year) j(descrip)
do description
local j=1
foreach v of var emp* {
label var `v' "`: label descrip `j' ' "   //can know which variable this column is from the label
local ++j
}

save "E:\lsu\bea data\caemp25s_AK_1969_2000.dta", replace


*here lacks AK
foreach state in AL AR AZ CA CO CT DC DE FL GA HI IA ID IL IN KS KY LA MA MD ME MI MN MO MS MT NC ND NE NH NJ NM NV NY OH OK OR PA RI SC SD TN TX UT VA VT WA WI WV WY{
import delimited "E:\lsu\bea data\caemp25s\CAEMP25S_`state'_1969_2000.csv", clear

drop if region==.  //remove notes at the bottom from csv file

local j=1968
forvalues i=9/40{   //注意更改
local j=`j'+1
if "`:type v`i''" == "long" {           // no space between ' and "
rename v`i' year_`j'
}
else if "`:type v`i''" == "double" {
rename v`i' year_`j'
}
else{
destring v`i', generate(year_`j') force
drop v`i'
}
}



keep geofips geoname description year_*

encode description, gen(descrip)
reshape long year_, i(geofips geoname description) j(year)  //i(varlist) 
rename year_ emp

label save descrip using description.do, replace
drop description

reshape wide emp, i(geofips geoname year) j(descrip)
do description
local j=1
foreach v of var emp* {
label var `v' "`: label descrip `j' ' "   //can know which variable this column is from the label
local ++j
}

save "E:\lsu\bea data\caemp25s_`state'_1969_2000.dta", replace
}


////////////////////////////////////////////////////////////////////////////////////////////////////////
*********2001-2020 employment
clear

foreach state in AK AL AR AZ CA CO CT DC DE FL GA HI IA ID IL IN KS KY LA MA MD ME MI MN MO MS MT NC ND NE NH NJ NM NV NY OH OK OR PA RI SC SD TN TX UT VA VT WA WI WV WY{
import delimited "E:\lsu\bea data\caemp25n\CAEMP25N_`state'_2001_2020.csv", clear

drop if region==.  //remove notes at the bottom from csv file


/*
foreach var of varlist _all {
display " `var' "  _col(20) "`: type `var''"  //col(20)是把结果的两列用空格隔开
}
*/

*测试时用DC Wahington举例
local j=2000
forvalues i=9/28{   //注意更改
local j=`j'+1
if "`:type v`i''" == "long" {           // no space between ' and "
rename v`i' year_`j'
}
else if "`:type v`i''" == "double" {
rename v`i' year_`j'
}
else{
destring v`i', generate(year_`j') force
drop v`i'
}
}


keep geofips geoname description year_*

encode description, gen(descrip)
reshape long year_, i(geofips geoname description) j(year)  //i(varlist) 
rename year_ emp

label save descrip using description.do, replace
drop description

reshape wide emp, i(geofips geoname year) j(descrip)
do description
local j=1
foreach v of var emp* {
label var `v' "`: label descrip `j' ' "   //can know which variable this column is from the label
local ++j
}

save "E:\lsu\bea data\caemp25n_`state'_2001_2020.dta", replace
}




//////////////////////////////////////////////////////////////////////////////////////////////////////////////
********personal income and employment
//only Alaska has N/A cells???

clear

foreach state in AK AL AR AZ CA CO CT DC DE FL GA HI IA ID IL IN KS KY LA MA MD ME MI MN MO MS MT NC ND NE NH NJ NM NV NY OH OK OR PA RI SC SD TN TX UT VA VT WA WI WV WY{
import delimited "E:\lsu\bea data\cainc4\CAINC4_`state'_1969_2020.csv", clear

drop if region==.  //remove notes at the bottom from csv file

local j=1968
forvalues i=9/60{   //注意更改
local j=`j'+1
if "`:type v`i''" == "long" {           // no space between ' and "
rename v`i' year_`j'
}
else if "`:type v`i''" == "double" {
rename v`i' year_`j'
}
else{
destring v`i', generate(year_`j') force
drop v`i'
}
}



keep geofips geoname description year_*

encode description, gen(descrip)
//duplicates list geofips geoname description   //replication in "Employer contributions for government social insurance"
duplicates drop geofips geoname description, force

reshape long year_, i(geofips geoname description) j(year)  //i(varlist) 
rename year_ inc

label save descrip using description.do, replace
drop description

reshape wide inc, i(geofips geoname year) j(descrip)
do description
local j=1
foreach v of var inc* {
label var `v' "`: label descrip `j' ' "   //can know which variable this column is from the label
local ++j
}

save "E:\lsu\bea data\cainc4_`state'_1969_2020.dta", replace
}


//////////////////////////////////////////////////////////////////////////////////
***pick the variables I need


foreach state in AK AL AR AZ CA CO CT DC DE FL GA HI IA ID IL IN KS KY LA MA MD ME MI MN MO MS MT NC ND NE NH NJ NM NV NY OH OK OR PA RI SC SD TN TX UT VA VT WA WI WV WY{
use "E:\lsu\bea data\caemp25s_`state'_1969_2000.dta", clear
keep geofips geoname year emp20 emp16 emp4 emp7 emp14 emp10 emp15 emp8 emp23 emp17
rename emp20 nonfarm_proprietors_emp
rename emp16 private_nonfarm_emp
rename emp4 construction_emp
rename emp7 manufacturing_emp
rename emp14 wholesale_trade_emp
rename emp10 retail_trade_emp
rename emp15 government_enterprises_emp
rename emp8 military_emp
rename emp23 total_emp
rename emp17 farm_emp
save "E:\lsu\bea data\emp_`state'_1969_2000.dta", replace
}





foreach state in AK AL AR AZ CA CO CT DC DE FL GA HI IA ID IL IN KS KY LA MA MD ME MI MN MO MS MT NC ND NE NH NJ NM NV NY OH OK OR PA RI SC SD TN TX UT VA VT WA WI WV WY{
use "E:\lsu\bea data\caemp25n_`state'_2001_2020.dta", clear
keep geofips geoname year emp27 emp28 emp6 emp14 emp24 emp20 emp26 emp15 emp33 emp29
rename emp27 nonfarm_proprietors_emp
rename emp28 private_nonfarm_emp
rename emp6 construction_emp
rename emp14 manufacturing_emp
rename emp24 wholesale_trade_emp
rename emp20 retail_trade_emp
rename emp26 government_enterprises_emp
rename emp15 military_emp
rename emp33 total_emp
rename emp29 farm_emp
save "E:\lsu\bea data\emp_`state'_2001_2020.dta", replace
}




foreach state in AK AL AR AZ CA CO CT DC DE FL GA HI IA ID IL IN KS KY LA MA MD ME MI MN MO MS MT NC ND NE NH NJ NM NV NY OH OK OR PA RI SC SD TN TX UT VA VT WA WI WV WY{
use "E:\lsu\bea data\cainc4_`state'_1969_2020.dta", clear
keep geofips geoname year inc18 inc7 inc6
rename inc18  population
rename inc7 nonfarm_proprietors_income
rename inc6 nonfarm_personal_income
save "E:\lsu\bea data\inc_`state'_1969_2020.dta", replace
}


///////////////////////////////////////////////////////////////////////////////////////////////
**append and merge
use "E:\lsu\bea data\emp_AK_1969_2000.dta", clear
foreach state in AL AR AZ CA CO CT DC DE FL GA HI IA ID IL IN KS KY LA MA MD ME MI MN MO MS MT NC ND NE NH NJ NM NV NY OH OK OR PA RI SC SD TN TX UT VA VT WA WI WV WY{
append using "E:\lsu\bea data\emp_`state'_1969_2000.dta"
}
foreach state in AK AL AR AZ CA CO CT DC DE FL GA HI IA ID IL IN KS KY LA MA MD ME MI MN MO MS MT NC ND NE NH NJ NM NV NY OH OK OR PA RI SC SD TN TX UT VA VT WA WI WV WY{
append using "E:\lsu\bea data\emp_`state'_2001_2020.dta"
}
save "E:\lsu\bea data\emp.dta", replace
//fips有变动的


use "E:\lsu\bea data\inc_AK_1969_2020.dta", clear
foreach state in AL AR AZ CA CO CT DC DE FL GA HI IA ID IL IN KS KY LA MA MD ME MI MN MO MS MT NC ND NE NH NJ NM NV NY OH OK OR PA RI SC SD TN TX UT VA VT WA WI WV WY{
append using "E:\lsu\bea data\inc_`state'_1969_2020.dta"
}
save "E:\lsu\bea data\inc.dta", replace

merge 1:1 geofips year using "E:\lsu\bea data\emp.dta"

gen geofips2=substr(geofips, 3, 5)
drop geofips
rename geofips2 geofips
order geofips
destring geofips, generate(fips) force
save "E:\lsu\bea data\emp_inc.dta", replace


//bys geofips: egen aa=count(_n)

///////////////////////////////////////////////////////////////////////////////////////////////
**delete files
foreach state in AK AL AR AZ CA CO CT DC DE FL GA HI IA ID IL IN KS KY LA MA MD ME MI MN MO MS MT NC ND NE NH NJ NM NV NY OH OK OR PA RI SC SD TN TX UT VA VT WA WI WV WY{
erase "E:\lsu\bea data\emp_`state'_1969_2000.dta"
erase "E:\lsu\bea data\emp_`state'_2001_2020.dta"
erase "E:\lsu\bea data\inc_`state'_1969_2020.dta"
}
erase "E:\lsu\bea data\emp.dta"
erase "E:\lsu\bea data\inc.dta"




**check
gen mytotal_emp=farm_emp+private_nonfarm_emp+government_enterprises_emp
gen gap=total_emp-mytotal_emp







