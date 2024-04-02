create or replace view view_month_wise_mortality as

WITH CageStartDate AS (
    SELECT 
        cagelot.cageid,
        MIN(costmain.costdate) AS start_date,
        MIN(cagelot.populationdate) AS start_population_date
    FROM 
        costmain 
    LEFT JOIN cagelot ON (cagelot.cagelotid = costmain.cagelotid)
    GROUP BY 
        cagelot.cageid
)

SELECT 
    lot.designation AS batch_name,
    cage.designation AS cage_name,
    YEAR(costmain.costdate) AS year,
    MONTH(costmain.costdate) AS month,
    -- YEAR(cagelot.populationdate) AS year_population_date,
    -- MONTH(cagelot.populationdate) AS month_population_date,
    cagelot.populationdate,
    SUM(costmain.daymortalno) AS monthly_mortal_no,
    CASE 
        WHEN YEAR(csd.start_date) = YEAR(costmain.costdate) AND MONTH(csd.start_date) = MONTH(costmain.costdate) THEN 1
        ELSE TIMESTAMPDIFF(MONTH, csd.start_date, CONCAT(YEAR(costmain.costdate), '-', MONTH(costmain.costdate), '-01')) + 2
    END AS cage_age_in_months,
    CASE 
        WHEN YEAR(csd.start_population_date) = YEAR(costmain.costdate) AND MONTH(csd.start_population_date) = MONTH(costmain.costdate) THEN 1
        ELSE TIMESTAMPDIFF(MONTH, csd.start_population_date, CONCAT(YEAR(costmain.costdate), '-', MONTH(costmain.costdate), '-01')) + 2
    END AS cage_age_in_months_population_date,
    
    TIMESTAMPDIFF(DAY, csd.start_date, LAST_DAY(CONCAT(YEAR(costmain.costdate), '-', MONTH(costmain.costdate), '-01'))) + 1 AS cage_age_in_days,
    TIMESTAMPDIFF(DAY, csd.start_population_date, LAST_DAY(CONCAT(YEAR(costmain.costdate), '-', MONTH(costmain.costdate), '-01'))) + 1 AS cage_age_in_days_population_date,
    MAX(costmain._fivetran_synced) AS _fivetran_synced
FROM 
    costmain 
LEFT JOIN cagelot ON (cagelot.cagelotid = costmain.cagelotid)
LEFT JOIN lot ON (cagelot.lotid = lot.lotid)
LEFT JOIN cage ON (cagelot.cageid = cage.cageid)
LEFT JOIN CageStartDate csd ON cagelot.cageid = csd.cageid
WHERE costmain._fivetran_deleted = 'false' AND   
      cage.designation != 'Virtual Cage' 
GROUP BY 
    YEAR(costmain.costdate),
    MONTH(costmain.costdate),
    -- YEAR(cagelot.populationdate) ,
    -- MONTH(cagelot.populationdate) ,
    cagelot.populationdate,
    lot.designation,
    cage.designation,
    csd.start_date,
    csd.start_population_date
    -- costmain._fivetran_synced
    -- costmain.costdate
ORDER BY 
    year, month, batch_name, cage_name;


-- WITH CageStartDate AS (
--     SELECT 
--         cagelot.cageid,
--         MIN(costmain.costdate) AS start_date
--     FROM 
--         costmain 
--     LEFT JOIN cagelot ON (cagelot.cagelotid = costmain.cagelotid)
--     GROUP BY 
--         cagelot.cageid
-- )

-- SELECT 
--     lot.designation AS cohort_name,
--     cage.designation AS cage_name,
--     YEAR(costmain.costdate) AS year,
--     MONTH(costmain.costdate) AS month,
--     SUM(costmain.daymortalno) AS monthly_mortal_no,
--     CASE 
--         WHEN YEAR(csd.start_date) = YEAR(costmain.costdate) AND MONTH(csd.start_date) = MONTH(costmain.costdate) THEN 1
--         ELSE TIMESTAMPDIFF(MONTH, csd.start_date, CONCAT(YEAR(costmain.costdate), '-', MONTH(costmain.costdate), '-01')) + 2
--     END AS cage_age_in_months
-- FROM 
--     costmain 
-- LEFT JOIN cagelot ON (cagelot.cagelotid = costmain.cagelotid)
-- LEFT JOIN lot ON (cagelot.lotid = lot.lotid)
-- LEFT JOIN cage ON (cagelot.cageid = cage.cageid)
-- LEFT JOIN CageStartDate csd ON cagelot.cageid = csd.cageid
-- WHERE
--     cage.designation != 'Virtual Cage'
-- GROUP BY 
--     YEAR(costmain.costdate),
--     MONTH(costmain.costdate),
--     lot.designation,
--     cage.designation,
--     csd.start_date
-- ORDER BY 
--     year, month, cohort_name, cage_name;







