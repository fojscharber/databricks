select
  cohort_id,
  cage_id,
  cause,
  amount * -1,
  amount * -1 * {{ lostrevenue }} as value,
  date_format(date, "yyMM") as date
from
  latest_cohort_mortalities
where
  cause != "Stocking"
ORDER BY
  cohort_id,
  cage_id,
  date,
  cause
