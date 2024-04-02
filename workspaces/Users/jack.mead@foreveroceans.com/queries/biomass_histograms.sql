SELECT
  camera_name,
  CAST(`timestamp` as DATE) as date,
  offset_weight
FROM
  biomass_filtered
WHERE
  `timestamp` BETWEEN '{{ date_range.start }}' AND '{{ date_range.end }}'
ORDER BY
  `timestamp`
