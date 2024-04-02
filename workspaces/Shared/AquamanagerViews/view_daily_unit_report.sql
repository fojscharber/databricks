CREATE OR REPLACE VIEW aquamanager_growout_dbo.view_daily_unit_report AS
       SELECT 
       to_date(costmain.costdate, "yyyy-MM-dd") as date_,
       to_date(cagelot.populationdate, "yyyy-MM-dd") AS populationdate,
       lot.designation as batch_name,
       cage.designation as cage_name, 
       cagelot.cagelotid,
       costmain.fishno as Fish_NO,
       costmain.mab as Av_wt_,
       CAST((costmain.fishno * costmain.mab)/1000 AS decimal(10,2)) as Biomass_kg,
       CAST((costmain.dayfoodkg/((costmain.fishno * costmain.mab)/1000))*100 AS decimal(10,2)) as SFR_Real,
       CAST((costmain.daysuggestedfoodqty/((costmain.fishno * costmain.mab)/1000))*100 AS decimal(10,2)) as SFR_Pred,
       costmain.dayharvestno as Harvest_No,
       costmain.dayharvestkg as Harvest_Kg,
       abs(costmain.daymortalno) as Mortality_No,
       costmain.daymortalkg as Mortality_Kg,
       CAST(((costmain.daymortalno / costmain.fishno ) * 100) AS decimal(10,2)) AS Mortality_per,
       round(costmain.accmortalno,0) as LTD_Mortality_No,
       costmain.accmortalkg as LTD_Mortality_Kg,
       CAST(((costmain.accmortalno / costmain.startfishno) * 100) AS decimal(10,2)) as LTD_Mortality_per,
       costmain.accharvestno as LTD_Harvest_No,
       costmain.accharvestkg as LTD_Harvest_Kg,
       costmain.accfoodkg as LTD_Feed_Kg,
       --costmain.startbiomass,
       CAST((costmain.accfoodkg/(((costmain.fishno * costmain.mab)/1000) - (costmain.startbiomass))) AS decimal(10,2)) as LTD_Econ_FCR,
       CAST((costmain.accfoodkg/(((costmain.fishno * costmain.mab)/1000) - (costmain.startbiomass) + (costmain.accmortalkg)+( costmain.accharvestkg))) AS decimal(10,2)) as LTD_Biol_FCR,
       
       DATEDIFF(day, MIN(costmain.costdate) OVER (PARTITION BY cage.designation), costmain.costdate) + 1 AS cage_age_in_days,
       DATEDIFF(day, MIN(cagelot.populationdate) OVER (PARTITION BY cage.designation), costmain.costdate) + 1 AS cage_age_in_days_population_date,
       
       DATEDIFF(month, MIN(costmain.costdate) OVER (PARTITION BY cage.designation), costmain.costdate) 
    + CASE WHEN DAY(costmain.costdate) >= DAY(MIN(costmain.costdate) OVER (PARTITION BY cage.designation)) THEN 1 ELSE 2 END AS cage_age_in_months,
       DATEDIFF(month, MIN(cagelot.populationdate) OVER (PARTITION BY cage.designation), costmain.costdate) 
    + CASE WHEN DAY(costmain.costdate) >= DAY(MIN(cagelot.populationdate) OVER (PARTITION BY cage.designation)) THEN 1 ELSE 2 END AS cage_age_in_months_population_date,
    costmain.`_fivetran_synced`

FROM aquamanager_growout_dbo.costmain 
LEFT JOIN aquamanager_growout_dbo.cagelot on(cagelot.cagelotid = costmain.cagelotid)
LEFT JOIN aquamanager_growout_dbo.lot on(cagelot.lotid = lot.lotid) 
LEFT JOIN aquamanager_growout_dbo.cage on(cagelot.cageid = cage.cageid)

WHERE  cage.designation != 'Virtual Cage' AND  
       cage.designation IN (SELECT DISTINCT(designation) FROM aquamanager_growout_dbo.cage) AND  
       --to_date(costmain.costdate, "yyyy-MM-dd") like '2023-07-%%' and
       costmain._fivetran_deleted = 'false'
ORDER BY date_ ASC
