WITH data AS (
	SELECT
	  date_format(CAST(from_unixtime(timestamp / 1000, 'yyyy-MM-dd') as DATE), 'yyyy-MM-dd') as date,
	  uniqueKey as frame_number,
		element_at(split(uniqueKey, '/'), 2) as site_id
	FROM
	  biomass_results
) SELECT * FROM data
WHERE
  date > '{{ date_range.start }}'
  AND date < '{{ date_range.end }}'
