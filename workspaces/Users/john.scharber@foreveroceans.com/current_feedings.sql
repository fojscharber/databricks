SELECT
  batch as cohort_id,
  cage as cage_id,
  to_date(date, 'MM-d-yy') as date,
  foodquantity as kilograms,
  foodType as feed_type
FROM
  hive_metastore.aquamanager_growout_dbo.view_feeding_transactions
WHERE
  type = 'Feeding'
  AND to_date(date, 'MM-d-yy') >= "{{daterange.start}}"
  and to_date(date, 'MM-d-yy') <= "{{daterange.end}}"
  
