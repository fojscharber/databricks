select
  cohort_name as cohort,
  year(date_) as year,
  quarter(date_) as qtr,
  sum(Mortality_Kg) as sumkg,
  sum(Mortality_No) as numfish,
  sum(Mortality_per) as percent,
  max(LTD_Mortality_Kg) as ltd_kg,
  max(LTD_Mortality_No) as ltd_numfish,
  max(LTD_Mortality_per) as ltd_percent
from
  view_daily_unit_report
group by
  cohort,
  year,
  qtr
order by
  cohort,
  year,
  qtr

