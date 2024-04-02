select
  year(to_date(date, 'M-dd-yy')) as year,
  quarter(to_date(date, 'M-dd-yy')) as quarter,
  batch,
  sum(fishintransaction),
  sum(totaltransactioncost)
from
  view_fish_population_transaction
where
  type = "Mortality"
group by 
  batch, year, quarter
  order by
  batch, year, quarter

