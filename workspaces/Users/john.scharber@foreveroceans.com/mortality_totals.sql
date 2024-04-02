select designation, sum(fishintransaction) as total from view_fish_population_transaction
where type = "Mortality"
group by designation
order by total
