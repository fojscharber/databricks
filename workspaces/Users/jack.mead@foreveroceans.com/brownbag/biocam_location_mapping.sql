CREATE
OR REPLACE VIEW biocam_location_map AS 
WITH camera_to_site AS (
  SELECT
    DISTINCT camera_name,
    CAST(
      substring_index(substring_index(unique_key, '/', 2), '/', -1) as INT
    ) as site_id
  FROM
    hive_metastore.default.biomass_filtered
) SELECT
  c.camera_name,
  c.site_id,
  m.cage_id,
  m.cohort_id
FROM
  camera_to_site c
  JOIN site_cage_cohort_mapping m ON c.site_id = m.site_id
