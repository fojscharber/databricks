SELECT
  date_format(CAST(timestamp as DATE), 'yyyy-MM-dd') as date,
  concat(unique_key, '/', instance_index) as instance,
  element_at(split(unique_key, '/'), 2) as site_id
FROM
  biomass_filtered
WHERE
  CAST(timestamp as DATE) > '{{ date_range.start }}'
  AND CAST(timestamp as DATE) < '{{ date_range.end }}'
