select
  cast(
    year(to_timestamp(date, 'mm-dd-yy')) as varchar(4)
  ) as yy,
  cast(
    month(to_timestamp(date, 'mm-dd-yy')) as varchar(4)
  ) as mm,
  to_timestamp(date, "mm-dd-yy") as date_,
  *
from
  view_feeding_transactions
order by
  date desc
