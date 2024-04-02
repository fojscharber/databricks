CREATE OR REPLACE VIEW view_parasite_details AS
SELECT 
  transdate as date,
  totalparasiteno as total_parasites,
  totalfishno as total_fish_sampled,
  cagelot.cageid as cage_id,
  view_cohort_details.cohort_id as cohort_id,
  view_cohort_details.cohort_name as cohort_name,
  view_cohort_details.cage_name as cage_name
FROM 
  clparasitetrans
JOIN cagelot ON cagelot.cagelotid = clparasitetrans.cagelotid
JOIN view_cohort_details ON view_cohort_details.cage_id = cagelot.cageid

