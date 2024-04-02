CREATE OR REPLACE VIEW view_treatment_details AS
SELECT 
  startdate as date,
  tt.designation as treatment,
  d.designation as disease,
  vcd.cohort_id,
  vcd.cage_id,
  vcd.cohort_name,
  vcd.cage_name
FROM 
  treatment t
JOIN treatmenttype tt ON tt.treatmenttypeid = t.treatmenttypeid
JOIN disease d ON d.diseaseid = t.diseaseid
JOIN treatcages tc ON tc.treatmentid = t.treatmentid
JOIN view_cohort_details vcd ON vcd.cage_id = tc.cageid
