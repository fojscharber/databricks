SELECT
  metrics.avg as avg,
  reading,
  site_id,
  sensor,
  window.start as timestamp
FROM
  water_quality_aggregations
WHERE CAST(`window`.`start` as date) BETWEEN '{{ date_range.start }}' AND '{{ date_range.end }}'
