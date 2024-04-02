select
  *
from
  (
    select
      date_,
      cage_age_in_days_population_date,
      batch_name as batch,
      cage_name as cage,
      Av_Wt_ as starting,
      lead(Av_Wt_, 1, Av_Wt_) over(
        order by
          batch_name,
          cage_name,
          cage_age_in_days_population_date
      ) as next_weight,
      if(next_Weight < Av_Wt_, Av_Wt_, next_weight) as ending,
      ending - starting as increase,
      ((ln(ending) - ln(starting)) * 100) / 7 as sgr
    from
      view_daily_unit_report
    where
      cage_age_in_days_population_date < 300
    order by
      batch_name,
      cage_name,
      cage_age_in_days_population_date
  )
where
  increase <= 15
