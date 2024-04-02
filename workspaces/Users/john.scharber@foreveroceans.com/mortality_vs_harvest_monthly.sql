SELECT concat(substring(date,7,2), ":", substring(date,1,2)) as month, * FROM view_fish_population_transaction
where batch in ("{{ batch }}")
order by month, batch limit all
