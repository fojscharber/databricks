select
  view_harvesting_details.`date` as harvest_date,
  view_summary_of_processed_fish.date as processed_date,
  total_weight as harvest_weight,
  format_number(total_harvested, '#') as harvested_fish,
  view_summary_of_processed_fish.totalFish as processed_fish,
  format_number(average_weight, '#.##') as harvested_avg_weight,
  cage_id as cage,
  cohort_id as batch,
  cohort as cohort,
  weightkg as processed_weight,
  yieldWeightkg as processed_yield,
  yieldPercentage as processed_yield_percentage,
  (harvest_weight - processed_weight) as weight_variance,
  (
    view_harvesting_details.total_harvested - view_summary_of_processed_fish.totalFish
  ) as count_variance
from
  hive_metastore.aquamanager_growout_dbo.view_harvesting_details
  left join hive_metastore.default.view_summary_of_processed_fish on hive_metastore.aquamanager_growout_dbo.view_harvesting_details.`date` = hive_metastore.default.view_summary_of_processed_fish.`date`
order by
  harvest_date
