CREATE
or replace view view_biomass_daily_histograms as
SELECT
  camera,
  timestamp,
  weightBuckets,
  weightHistogram,
  biomass_daily_histograms.offsetWeightBuckets,
  biomass_daily_histograms.offsetWeightHistogram,
  lengthBuckets,
  lengthHistogram
FROM
  biomass_daily_histograms
