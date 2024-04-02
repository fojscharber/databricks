SELECT
  ccm.cage_id,
  ccm.cohort_id,
  ccm.site_id,
  cm.date,
  cm.amount,
  cm.cause
FROM
  latest_cohort_mortalities cm
  JOIN site_cage_cohort_mapping ccm ON ccm.cohort_id = cm.cohort_id AND ccm.cage_id = cm.cage_id
  WHERE cm.date > '{{ date_range.start }}' AND cm.date < '{{ date_range.end }}'
