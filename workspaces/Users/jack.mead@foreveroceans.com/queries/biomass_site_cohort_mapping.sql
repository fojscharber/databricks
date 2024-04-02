CREATE
OR REPLACE VIEW biocam_cohort_mapping AS
SELECT
  camera_name,
  site_id,
  cohort_id
FROM
  (
    VALUES
      ('biocamsvid92', 1, 'SR-C0421Mx'),
      ('biocamsvid91', 2, 'SR-C0522MxEc'),
      ('biocamsvid96', 4, 'SR-C0421Mx'),
      ('biocamsvid90', 2, 'SR-C0522MxEc'),
      ('biocamsvid97', 2, 'SR-C0522MxEc')
  ) as t(camera_name, site_id, cohort_id)
