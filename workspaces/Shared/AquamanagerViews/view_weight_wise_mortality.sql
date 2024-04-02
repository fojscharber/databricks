create or replace view view_weight_wise_mortality as
SELECT 
    to_date(costmain.costdate, "yyyy-MM-dd") AS costdate,
    to_date(cagelot.populationdate, "yyyy-MM-dd") AS populationdate,
    lot.designation AS batch_name,
    cage.designation AS cage_name,
    (costmain.mab * 0.0022046) AS av_weight_lbs,
    costmain.mab AS av_weight_gram,
    CASE
        WHEN costmain.mab > 0 AND costmain.mab < 1000 THEN '<2 lb'
        WHEN costmain.mab >= 1000 AND costmain.mab < 1800 THEN '2-4 lb'
        WHEN costmain.mab >= 1800 AND costmain.mab < 2700 THEN '4-6 lb'
        WHEN costmain.mab >= 2700 AND costmain.mab < 5000 THEN '6+ lb'
        WHEN costmain.mab >= 5000 THEN 'Size unknown'
        ELSE 'Size unspecified'
    END AS weight_category,
    ABS(costmain.daymortalno) AS mortality_no,
    
    DATEDIFF(day, MIN(costmain.costdate) OVER (PARTITION BY cage.designation), costmain.costdate) + 1 AS cage_age_in_days,
    DATEDIFF(day, MIN(cagelot.populationdate) OVER (PARTITION BY cage.designation), costmain.costdate) + 1 AS cage_age_in_days_population_date,
    DATEDIFF(month, MIN(costmain.costdate) OVER (PARTITION BY cage.designation), costmain.costdate) 
    + CASE WHEN DAY(costmain.costdate) >= DAY(MIN(costmain.costdate) OVER (PARTITION BY cage.designation)) THEN 1 ELSE 2 END AS cage_age_in_months,
    DATEDIFF(month, MIN(cagelot.populationdate) OVER (PARTITION BY cage.designation), costmain.costdate) 
    + CASE WHEN DAY(costmain.costdate) >= DAY(MIN(cagelot.populationdate) OVER (PARTITION BY cage.designation)) THEN 1 ELSE 2 END AS cage_age_in_months_population_date,
    costmain._fivetran_synced
FROM 
    costmain
LEFT JOIN cagelot ON (cagelot.cagelotid = costmain.cagelotid)
LEFT JOIN cage ON (cagelot.cageid = cage.cageid)
LEFT JOIN lot ON (cagelot.lotid = lot.lotid)
WHERE costmain._fivetran_deleted = 'false' AND 
      cage.designation != 'Virtual Cage'
ORDER BY costdate;

-- SELECT 
--     to_date(costmain.costdate, "yyyy-MM-dd") AS costdate,
--     cage.designation AS cage_name,
--     (costmain.mab * 0.0022046) AS av_weight_lbs,
--     costmain.mab AS av_weight_gram,
--     CASE
--         WHEN costmain.mab > 0 AND costmain.mab < 1000 THEN '<2 lb'
--         WHEN costmain.mab >= 1000 AND costmain.mab < 1800 THEN '2-4 lb'
--         WHEN costmain.mab >= 1800 AND costmain.mab < 2700 THEN '4-6 lb'
--         WHEN costmain.mab >= 2700 AND costmain.mab < 5000 THEN '6+ lb'
--         WHEN costmain.mab >= 5000 THEN 'Size unknown'
--         ELSE 'Size unspecified'
--     END AS weight_category,
--     ABS(costmain.daymortalno) AS mortality_no
-- FROM 
--     costmain
-- LEFT JOIN cagelot ON (cagelot.cagelotid = costmain.cagelotid)
-- -- LEFT JOIN lot ON (cagelot.lotid = lot.lotid)
-- LEFT JOIN cage ON (cagelot.cageid = cage.cageid)
-- ORDER BY costdate;




