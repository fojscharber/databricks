CREATE OR REPLACE VIEW manual_biomass_samplings AS
SELECT
	batch as cohort_id,
	cage as cage_id,
	to_date(date, 'yyyy-dd-mm') as date,
	fishSampled as sampled,
	avgWeight as average_weight
FROM hive_metastore.aquamanager_growout_dbo.view_sample_summary_by_date
