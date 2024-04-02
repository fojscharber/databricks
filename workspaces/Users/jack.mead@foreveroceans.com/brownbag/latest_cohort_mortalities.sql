CREATE OR REPLACE VIEW latest_cohort_mortalities AS
SELECT
   batch as cohort_id,
   cage as cage_id,
   designation as cause,
   fishintransaction as amount,
   to_date(date, 'MM-d-yy') as date
FROM hive_metastore.aquamanager_growout_dbo.view_fish_population_transaction
WHERE type = 'Mortality' AND to_date(date, 'MM-d-yy') >= date_sub(current_date(), 90)
