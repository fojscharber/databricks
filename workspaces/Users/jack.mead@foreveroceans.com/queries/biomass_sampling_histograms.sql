SELECT
  sampleweight as sample_weight,
  to_date(date, 'MM-dd-yy') as date,
  batch
FROM
  view_sampling_details
WHERE
  to_date(date, 'MM-dd-yy') BETWEEN '{{ date_range.start }}' AND '{{ date_range.end }}'
