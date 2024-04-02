select
  year(date_) as year,
  month(date_) as month,
  day(date_) as day,
  to_number(date_format(last_day(date_), "dd"), "99") as days,
  concat(year, month) as yymm,
  batch_name as batch,
  cage_name as cage,
  min(Av_wt_) as starting,
  max(Av_wt_) as ending,
  ending - starting as increase_grams,
  ((ln(ending) - ln(starting)) * 100) / days as sgr,
  increase_grams / days as day_g_growth
from
  view_daily_unit_report
group by
  batch,
  cage,
  month
order by
  batch,
  cage,
  month
