WITH data AS (
	SELECT
		camera_name,
	  CAST(substring_index(substring_index(unique_key, '/', 2), '/', -1) as INT) as site_id,
	  CAST(timestamp as DATE) as date,
		offset_weight,
		offset_length
	FROM
	  biomass_filtered
) SELECT * FROM data
WHERE
  date > '{{ date_range.start }}'
  AND date < '{{ date_range.end }}'
  AND site_id == {{ site_id }}
