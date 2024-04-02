SELECT
  ccm.cage_id,
  ccm.cohort_id,
  ccm.site_id,
  wq.timestamp,
  wq.date,
  wq.value,
  wq.type
FROM
  site_water_quality wq
  JOIN site_cage_cohort_mapping ccm ON ccm.site_id = wq.site_id
  WHERE wq.date > '{{ date_range.start }}' AND wq.date < '{{ date_range.end }}'
