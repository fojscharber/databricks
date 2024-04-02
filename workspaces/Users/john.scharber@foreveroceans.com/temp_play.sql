select to_date(timestamp,'k') as hour, month, year, site_id, value from c2_readings
where asset_short_name like 'ysi%'
and reading_short_name like "exo%Depth" or reading_short_name like 'exo%Temperature'
and year = 2023 and month = 7

