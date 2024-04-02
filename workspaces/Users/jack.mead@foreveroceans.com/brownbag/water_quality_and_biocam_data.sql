WITH vac AS (
  SELECT
    vac.camera_name as camera_name,
    vac.instances_detected as instances_detected,
    vac.weight as weight,
    vac.length as length,
    vac.date as date,
    vlm.site_id as site_id,
    vlm.cage_id as cage_id,
    vlm.cohort_id as cohort_id
  FROM
    view_ai_biomass_weight_per_camera vac
    JOIN view_biocam_location_mapping vlm ON vac.camera_name = vlm.camera_name
)
SELECT
  vac.date as date,
  vac.site_id as site_id,
  vac.cage_id as cage_id,
  vac.cohort_id as cohort_id,
  twq.value as do,
  vac.weight as weight,
  vac.length as length,
  twq.timestamp as timestamp
FROM
  site_water_quality twq
  JOIN vac ON twq.date = vac.date
WHERE
  type = 'dissolved_oxygen'
  AND twq.site_id = {{ site_id }}
