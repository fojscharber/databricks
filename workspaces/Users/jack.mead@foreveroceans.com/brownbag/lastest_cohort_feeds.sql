CREATE OR REPLACE VIEW latest_cohort_feeds AS
SELECT
batch as cohort_id,
cage as cage_id,
to_date(date, 'MM-d-yy') as date,
foodquantity as kilograms,
foodType as feed_type
FROM hive_metastore.aquamanager_growout_dbo.view_feeding_transactions
WHERE type = 'Feeding' AND to_date(date, 'MM-d-yy') >= date_sub(current_date(), 90)
