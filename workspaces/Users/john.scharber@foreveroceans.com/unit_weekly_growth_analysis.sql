select
  year(date_) as year,
  weekofyear(date_) as week,
  batch_name as batch,
  cage_name as cage,
  min(Av_wt_) as starting,
  max(Av_wt_) as ending,
  ending - starting as increase,
  ((ln(ending) - ln(starting)) * 100) / 7 as sgr
from
  view_daily_unit_report
group by
  batch_name,
  cage_name,
  year(date_),
  weekofyear(date_)
order by
  year(date_),
  weekofyear(date_)
