CREATE
OR REPLACE VIEW daily_biocam_weight_per_camera AS
SELECT
  camera_name,
  instances_detected,
  median_offset_weight as weight,
  median_offset_length as length,
  `date`
FROM
  hive_metastore.default.biomass_aggregations
