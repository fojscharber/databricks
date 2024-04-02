SELECT * FROM view_daily_unit_report
where date_ >= {{ date_range.start }} and date_ <= {{ date_range.end }}
