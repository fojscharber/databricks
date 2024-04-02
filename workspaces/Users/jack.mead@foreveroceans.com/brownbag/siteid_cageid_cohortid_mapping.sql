CREATE
OR REPLACE VIEW site_cage_cohort_mapping AS
SELECT
  site_id,
  cage_id,
  cohort_id
FROM
  (
    VALUES
      (1, 'PTY C01', 'SR-C0421Mx'),
      (2, 'PTY C02', 'SR-C0522MxEc'),
      (3, 'PTY C03', ''),
      (4, 'PTY C04', '')
  ) as t(site_id, cage_id, cohort_id)
