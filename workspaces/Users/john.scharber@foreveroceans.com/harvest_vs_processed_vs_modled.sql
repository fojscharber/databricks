SELECT DISTINCT
  *,
  (harvested_fish * (Av_wt_ / 1000)) as modeled_biomass,
  (modeled_biomass - harvested_avg_weight) as model_vs_harvest
FROM
  view_daily_unit_report,
  view_harvest_vs_processed
where
  date_ = harvest_date
  order by cohort_name, date_
