--Daily Fleet Sales

-- Fleet OOS
with day_rtl_sales_thru as (
select l.sales_thru_dt,
l.curr_mth_start_dt,
month(l.curr_mth_start_dt) as rpt_mth_nbr,
date_format(l.curr_mth_start_dt, 'MMM') as rpt_mth_abbr_txt,
l.curr_yr_start_dt,
year(l.curr_yr_start_dt) as curr_sales_year,
l.prior_yr_start_dt,
year(l.prior_yr_start_dt) as prior_yr
from marketing360_public.fs_gbl_sales_cal_day_rpt_lkp l
where sales_thru_dt = date_sub(to_date(current_date), 1)
and iso_ctry_cd = 'US'), --This query provides sales thru dates


pre_day as (
Select
fd.rpt_dt,
day_rtl_sales_thru.sales_thru_dt,
day_rtl_sales_thru.rpt_mth_nbr,
day_rtl_sales_thru.rpt_mth_abbr_txt,
Sum(Case
when fd.sales_through_dt = date_sub(to_date(day_rtl_sales_thru.curr_yr_start_dt), 1) then 0 
when fd.sales_through_dt = date_sub(to_date(day_rtl_sales_thru.prior_yr_start_dt), 1) then 0
Else fd.jan
END) as Jan_MTD_Sales,
sum(fd.feb) as Feb_MTD_Sales,
sum(fd.mar) as Mar_MTD_Sales,
sum(fd.apr) as Apr_MTD_Sales,
sum(fd.may) as May_MTD_Sales,
sum(fd.jun) as Jun_MTD_Sales,
sum(fd.jul) as Jul_MTD_Sales,
sum(fd.aug) as Aug_MTD_Sales,
sum(fd.sep) as Sep_MTD_Sales,
sum(fd.oct) as Oct_MTD_Sales,
sum(fd.nov) as Nov_MTD_Sales,
sum(fd.`dec`) as Dec_MTD_Sales
from dl_edge_base_dcfpa_171749_base_dcfpa_avista.fleet_oos_stg fd, day_rtl_sales_thru
Where fd.sales_through_dt = date_sub(to_date(day_rtl_sales_thru.sales_thru_dt), 1)
group by fd.rpt_dt, day_rtl_sales_thru.sales_thru_dt, day_rtl_sales_thru.rpt_mth_nbr,
day_rtl_sales_thru.rpt_mth_abbr_txt

),

curr_day as (
Select
fd.rpt_dt, 
day_rtl_sales_thru.sales_thru_dt,
day_rtl_sales_thru.rpt_mth_nbr,
day_rtl_sales_thru.rpt_mth_abbr_txt,
fd.rpt_dt as daily_sales_rpt_dt,
sum(fd.jan) as Jan_MTD_Sales,
sum(fd.feb) as Feb_MTD_Sales,
sum(fd.mar) as Mar_MTD_Sales,
sum(fd.apr) as Apr_MTD_Sales,
sum(fd.may) as May_MTD_Sales,
sum(fd.jun) as Jun_MTD_Sales,
sum(fd.jul) as Jul_MTD_Sales,
sum(fd.aug) as Aug_MTD_Sales,
sum(fd.sep) as Sep_MTD_Sales,
sum(fd.oct) as Oct_MTD_Sales,
sum(fd.nov) as Nov_MTD_Sales,
sum(fd.`dec`) as Dec_MTD_Sales
from dl_edge_base_dcfpa_171749_base_dcfpa_avista.fleet_oos_stg fd, day_rtl_sales_thru
Where fd.sales_through_dt = to_date(day_rtl_sales_thru.sales_thru_dt)
group by fd.rpt_dt, day_rtl_sales_thru.sales_thru_dt, day_rtl_sales_thru.rpt_mth_nbr,
day_rtl_sales_thru.rpt_mth_abbr_txt
)

--January Month
Select
'Jan' as sales_mth_cat,
1 as sales_mth_cat_nbr,
f.rpt_dt,
f.sales_thru_dt,
f.rpt_mth_nbr,
f.rpt_mth_abbr_txt,
sum(f.Jan_MTD_Sales -pd.Jan_MTD_Sales) as Daily_Sales
from curr_day f 
left join pre_day pd
ON (
(date_format(to_date(f.rpt_dt), 'EEE') = 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 2))
OR (date_format(to_date(f.rpt_dt), 'EEE') <> 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 1))
)
Where f.rpt_mth_nbr = 1
GROUP BY
f.rpt_dt,
f.sales_thru_dt,
f.rpt_mth_nbr,
f.rpt_mth_abbr_txt

UNION

--February Month
Select
'Feb' as sales_mth_cat,
2 as sales_mth_cat_nbr,
f.rpt_dt,
f.sales_thru_dt,
f.rpt_mth_nbr,
f.rpt_mth_abbr_txt,
sum(f.Feb_MTD_Sales -pd.Feb_MTD_Sales) as Daily_Sales
from curr_day f 
left join pre_day pd
ON (
(date_format(to_date(f.rpt_dt), 'EEE') = 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 2))
OR (date_format(to_date(f.rpt_dt), 'EEE') <> 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 1))
)
Where f.rpt_mth_nbr = 2
GROUP BY
f.rpt_dt,
f.sales_thru_dt,
f.rpt_mth_nbr,
f.rpt_mth_abbr_txt

UNION

--March Month
Select
'Mar' as sales_mth_cat,
3 as sales_mth_cat_nbr,
f.rpt_dt,
f.sales_thru_dt,
f.rpt_mth_nbr,
f.rpt_mth_abbr_txt,
sum(f.Mar_MTD_Sales -pd.Mar_MTD_Sales) as Daily_Sales
from curr_day f 
left join pre_day pd
ON (
(date_format(to_date(f.rpt_dt), 'EEE') = 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 2))
OR (date_format(to_date(f.rpt_dt), 'EEE') <> 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 1))
)
Where f.rpt_mth_nbr = 3
GROUP BY
f.rpt_dt,
f.sales_thru_dt,
f.rpt_mth_nbr,
f.rpt_mth_abbr_txt

UNION

--Aoril Month
Select
'Apr' as sales_mth_cat,
4 as sales_mth_cat_nbr,
f.rpt_dt,
f.sales_thru_dt,
f.rpt_mth_nbr,
f.rpt_mth_abbr_txt,
sum(f.Apr_MTD_Sales -pd.Apr_MTD_Sales) as Daily_Sales
from curr_day f 
left join pre_day pd
ON (
(date_format(to_date(f.rpt_dt), 'EEE') = 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 2))
OR (date_format(to_date(f.rpt_dt), 'EEE') <> 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 1))
)
Where f.rpt_mth_nbr = 4
GROUP BY
f.rpt_dt,
f.sales_thru_dt,
f.rpt_mth_nbr,
f.rpt_mth_abbr_txt

UNION


--May Month
Select
'May' as sales_mth_cat,
5 as sales_mth_cat_nbr,
f.rpt_dt,
f.sales_thru_dt,
f.rpt_mth_nbr,
f.rpt_mth_abbr_txt,
sum(f.May_MTD_Sales -pd.May_MTD_Sales) as Daily_Sales
from curr_day f 
left join pre_day pd
ON (
(date_format(to_date(f.rpt_dt), 'EEE') = 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 2))
OR (date_format(to_date(f.rpt_dt), 'EEE') <> 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 1))
)
Where f.rpt_mth_nbr = 5
GROUP BY
f.rpt_dt,
f.sales_thru_dt,
f.rpt_mth_nbr,
f.rpt_mth_abbr_txt

UNION


--June Month
Select
'Jun' as sales_mth_cat,
6 as sales_mth_cat_nbr,
f.rpt_dt,
f.sales_thru_dt,
f.rpt_mth_nbr,
f.rpt_mth_abbr_txt,
sum(f.Jun_MTD_Sales -pd.Jun_MTD_Sales) as Daily_Sales
from curr_day f 
left join pre_day pd
ON (
(date_format(to_date(f.rpt_dt), 'EEE') = 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 2))
OR (date_format(to_date(f.rpt_dt), 'EEE') <> 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 1))
)
Where f.rpt_mth_nbr = 6
GROUP BY
f.rpt_dt,
f.sales_thru_dt,
f.rpt_mth_nbr,
f.rpt_mth_abbr_txt

UNION


--July Month
Select
'Jul' as sales_mth_cat,
7 as sales_mth_cat_nbr,
f.rpt_dt,
f.sales_thru_dt,
f.rpt_mth_nbr,
f.rpt_mth_abbr_txt,
sum(f.Jul_MTD_Sales -pd.Jul_MTD_Sales) as Daily_Sales
from curr_day f 
left join pre_day pd
ON (
(date_format(to_date(f.rpt_dt), 'EEE') = 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 2))
OR (date_format(to_date(f.rpt_dt), 'EEE') <> 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 1))
)
Where f.rpt_mth_nbr = 7
GROUP BY
f.rpt_dt,
f.sales_thru_dt,
f.rpt_mth_nbr,
f.rpt_mth_abbr_txt

UNION


--August Month
Select
'Aug' as sales_mth_cat,
8 as sales_mth_cat_nbr,
f.rpt_dt,
f.sales_thru_dt,
f.rpt_mth_nbr,
f.rpt_mth_abbr_txt,
sum(f.Aug_MTD_Sales -pd.Aug_MTD_Sales) as Daily_Sales
from curr_day f 
left join pre_day pd
ON (
(date_format(to_date(f.rpt_dt), 'EEE') = 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 2))
OR (date_format(to_date(f.rpt_dt), 'EEE') <> 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 1))
)
Where f.rpt_mth_nbr = 8
GROUP BY
f.rpt_dt,
f.sales_thru_dt,
f.rpt_mth_nbr,
f.rpt_mth_abbr_txt

UNION


--September Month
Select
'Sep' as sales_mth_cat,
9 as sales_mth_cat_nbr,
f.rpt_dt,
f.sales_thru_dt,
f.rpt_mth_nbr,
f.rpt_mth_abbr_txt,
sum(f.Sep_MTD_Sales -pd.Sep_MTD_Sales) as Daily_Sales
from curr_day f 
left join pre_day pd
ON (
(date_format(to_date(f.rpt_dt), 'EEE') = 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 2))
OR (date_format(to_date(f.rpt_dt), 'EEE') <> 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 1))
)
Where f.rpt_mth_nbr = 9
GROUP BY
f.rpt_dt,
f.sales_thru_dt,
f.rpt_mth_nbr,
f.rpt_mth_abbr_txt

UNION


--October Month
Select
'Oct' as sales_mth_cat,
10 as sales_mth_cat_nbr,
f.rpt_dt,
f.sales_thru_dt,
f.rpt_mth_nbr,
f.rpt_mth_abbr_txt,
sum(f.Oct_MTD_Sales -pd.Oct_MTD_Sales) as Daily_Sales
from curr_day f 
left join pre_day pd
ON (
(date_format(to_date(f.rpt_dt), 'EEE') = 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 2))
OR (date_format(to_date(f.rpt_dt), 'EEE') <> 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 1))
)
Where f.rpt_mth_nbr = 10
GROUP BY
f.rpt_dt,
f.sales_thru_dt,
f.rpt_mth_nbr,
f.rpt_mth_abbr_txt

UNION


--November Month
Select
'Nov' as sales_mth_cat,
11 as sales_mth_cat_nbr,
f.rpt_dt,
f.sales_thru_dt,
f.rpt_mth_nbr,
f.rpt_mth_abbr_txt,
sum(f.Nov_MTD_Sales -pd.Nov_MTD_Sales) as Daily_Sales
from curr_day f 
left join pre_day pd
ON (
(date_format(to_date(f.rpt_dt), 'EEE') = 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 2))
OR (date_format(to_date(f.rpt_dt), 'EEE') <> 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 1))
)
Where f.rpt_mth_nbr = 11
GROUP BY
f.rpt_dt,
f.sales_thru_dt,
f.rpt_mth_nbr,
f.rpt_mth_abbr_txt

UNION


--December Month
Select
'Dec' as sales_mth_cat,
12 as sales_mth_cat_nbr,
f.rpt_dt,
f.sales_thru_dt,
f.rpt_mth_nbr,
f.rpt_mth_abbr_txt,
sum(f.Dec_MTD_Sales -pd.Dec_MTD_Sales) as Daily_Sales
from curr_day f 
left join pre_day pd
ON (
(date_format(to_date(f.rpt_dt), 'EEE') = 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 2))
OR (date_format(to_date(f.rpt_dt), 'EEE') <> 'Mon' and pd.rpt_dt = date_sub(to_date(f.rpt_dt), 1))
)
Where f.rpt_mth_nbr = 12
GROUP BY
f.rpt_dt,
f.sales_thru_dt,
f.rpt_mth_nbr,
f.rpt_mth_abbr_txt
