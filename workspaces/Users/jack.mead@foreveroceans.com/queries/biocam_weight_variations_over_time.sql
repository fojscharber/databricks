SELECT
  camera_name,
  date,
  std_offset_weight
FROM
  biomass_aggregations
WHERE
  `date` BETWEEN '{{ date_range.start }}' AND '{{ date_range.end }}'
ORDER BY
  `date`
