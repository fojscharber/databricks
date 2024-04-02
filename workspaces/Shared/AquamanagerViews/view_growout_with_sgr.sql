create
or replace view view_monthly_growth_with_sgr as
select
  year(date_) as year,
  month(date_) as month,
  to_number(date_format(last_day(date_), "dd"), "99") as days,
  concat(year, month) as yymm,
  batch_name as batch,
  cage_name as cage,
  min(Av_wt_) as starting,
  max(Av_wt_) as ending,
  ending - starting as increase_grams,
  ((ln(ending) - ln(starting)) * 100) / days as sgr
from
  view_daily_unit_report
group by
  batch_name,
  cage_name,
  year(date_),
  month(date_),
  days
order by
  year(date_),
  month(date_)
