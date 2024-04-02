CREATE OR REPLACE VIEW view_cohort_details AS
SELECT
  lot.designation as cohort_name,
  lot.lotid as cohort_id,
  cage.designation as cage_name,
  cage.cageid as cage_id,
  to_date(cagelot.populationdate, "yyyy-MM-dd") as cohort_start,
  to_date(cagelot.enddate, "yyyy-MM-dd") as cohort_end,
  lot.lotfish as initial_stocking,
  cagelot.initialbiomassafter as initial_biomass,
  to_date(cagelot.enddate, "yyyy-MM-dd") = '3000-01-01' as active
FROM
  cage,
  cagelot,
  lot,
  hatchery,
  species
WHERE
  cagelot.cageid = cage.cageid
  and cagelot.lotid = lot.lotid
  and lot.hatcheryid = hatchery.hatcheryid
  and lot.speciesid = species.speciesid
  and cage.designation != 'Virtual Cage'
  and cage.`_fivetran_deleted` = false
  and cagelot.`_fivetran_deleted` = false
  and lot.`_fivetran_deleted` = false
  and hatchery.`_fivetran_deleted` = false
  and species.`_fivetran_deleted` = false
