# Databricks notebook source
# MAGIC %md **This Script will create a dynamic view - FisheryInsights based on the dynamic nature**
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases;

# COMMAND ----------


broodstockorigin = spark.sql(f""" SELECT DISTINCT UPPER(broodstockorigin) as broodstockorigin FROM aquamanager_hatchery_dbo.tltransdetails
where broodstockorigin IS NOT NULL AND TRIM(broodstockorigin) != ''
""").select('broodstockorigin').rdd.flatMap(lambda x: x).collect()


# COMMAND ----------

dbutils.widgets.dropdown(
    name="broodstockorigin",
    label="broodstockorigin",
    choices= broodstockorigin,
    defaultValue="ECUADOR"
)

dbutils.widgets.multiselect(
    name="Reception",
    label="Reception",
    choices=["7B", "8A", "Batch 6","Batch 6B", "Batch 7", "Batch 7A", "Batch 7B", "Cohort 8A", "Cohorte 8A"],
    defaultValue="Batch 7A"
)

# COMMAND ----------

#Dynamic Variable assignment 
broodstockorigin = dbutils.widgets.get("broodstockorigin").strip()
Reception = dbutils.widgets.get("Reception").strip()
# Reception


elements = Reception.split(',')

# Surround each element with double quotes and join them back together
Reception = ','.join(["'{}'".format(element) for element in elements])


# COMMAND ----------

#Loggers:
print(f"The View creation is running for broodstockorigin = {broodstockorigin} and Reception = {Reception} ")

# COMMAND ----------

# MAGIC %md
# MAGIC The `extractAndConcatUDF` is a User-Defined Function (UDF) written in Scala. It's designed to process input strings, typically containing specific patterns. In this case, it identifies uppercase alphanumeric sequences followed  by "Ecuador /" and joins them together using hyphens ('-'). This UDF is valuable for parsing and extracting structured information from text data, enhancing data processing capabilities in SQL queries.
# MAGIC
# MAGIC **Usage:** This Function is used to extract broodstock_origen from broodstocktanks.
# MAGIC
# MAGIC **Examples:**
# MAGIC
# MAGIC Example 1:  
# MAGIC **Input:** M5 Ecuador / RH6 CENAIM  
# MAGIC **Output:** RH6
# MAGIC
# MAGIC Example 2:  
# MAGIC **Input:** M5 Ecuador / RH6 CENAIM|M6 Ecuador / RH7 CENAIM|RAS 6 Ecuador / RF3|M3 Ecuador / RF1  
# MAGIC **Output:** RF1-RF3-RH6-RH7
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.functions.udf
# MAGIC
# MAGIC // Define a User-Defined Function (UDF) named "extractAndConcat"
# MAGIC val extractAndConcatUDF = udf((input: String) => {
# MAGIC   if (input != null) {
# MAGIC     // Define a regular expression pattern to match data (case-insensitive)
# MAGIC     val pattern = """(?i)[A-Z0-9]+ Ecuador / ([A-Z0-9]+)""".r
# MAGIC
# MAGIC     // Find the first match of the pattern in the input string
# MAGIC     val matched = pattern.findFirstMatchIn(input)
# MAGIC
# MAGIC     matched match {
# MAGIC       case Some(m) => m.group(1).toUpperCase  // Return the matched group in uppercase
# MAGIC       case None if input.matches("[mM]6") => "M6"  // Return "M6" for "m6" or "M6"
# MAGIC       case _ => null.asInstanceOf[String]  // Return null for other cases
# MAGIC     }
# MAGIC   } else {
# MAGIC     // Return null if the input is null
# MAGIC     null.asInstanceOf[String]
# MAGIC   }
# MAGIC })
# MAGIC
# MAGIC // Register the UDF
# MAGIC spark.udf.register("extractAndConcat", extractAndConcatUDF)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC The `removeDuplicatesUDF` is a User-Defined Function (UDF) written in Scala for Apache Spark. It's designed to process input strings by splitting them using hyphens ('-') to separate values. The UDF then finds and retains distinct (unique) values from the split values and rejoins them together using hyphens. This UDF is particularly useful for deduplicating values in a string. It enhances data processing capabilities within Spark SQL queries.
# MAGIC
# MAGIC **Usage:** This Function is used in creation of broodstock_origen column.
# MAGIC
# MAGIC **Examples:**
# MAGIC
# MAGIC Example 1:  
# MAGIC **Input:** offshore-offshore  
# MAGIC **Output:** offshore
# MAGIC
# MAGIC Example 2:  
# MAGIC **Input:** offshore-nursery-nursery-larvae  
# MAGIC **Output:** offshore-nursery-larvae
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC // Register the UDF to remove duplicate values
# MAGIC
# MAGIC // Define a User-Defined Function (UDF) named "removeDuplicates"
# MAGIC val removeDuplicatesUDF = udf((origen: String) => {
# MAGIC   // Split the input string by hyphen "-" to separate values
# MAGIC   val splitValues = origen.split("-")
# MAGIC   
# MAGIC   // Find distinct (unique) values from the splitValues array
# MAGIC   val uniqueValues = splitValues.distinct.sorted
# MAGIC   
# MAGIC   // Join the unique values back together with hyphens "-"
# MAGIC   uniqueValues.mkString("-")
# MAGIC })
# MAGIC
# MAGIC // Register the UDF with a name "removeDuplicates" for use in Spark SQL queries
# MAGIC spark.udf.register("removeDuplicates", removeDuplicatesUDF)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC The `countIndividualValuesUDF` is a User-Defined Function (UDF) written in Scala for Apache Spark. It's designed to process input strings by splitting them using hyphens ('-') to separate values. The UDF then counts the occurrences of each value and presents the counts as a comma-separated string. This UDF enhances data processing capabilities within Spark SQL queries for calculating the occurrence counts of individual values.
# MAGIC
# MAGIC **Usage:** This Function is used in creation of Tank column.
# MAGIC
# MAGIC **Examples:**
# MAGIC
# MAGIC Example 1:  
# MAGIC **Input:** RF1-RF1-RH3  
# MAGIC **Output:** 2,1
# MAGIC
# MAGIC Example 2:  
# MAGIC **Input:** RF1-RF1-RH2-RH2-RF1  
# MAGIC **Output:** 3,2
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC // Define the UDF
# MAGIC val countIndividualValuesUDF = udf((origen: String) => {
# MAGIC   // Split the input string by "-"
# MAGIC   val splitValues = origen.split("-")
# MAGIC   
# MAGIC   // Initialize a map to store counts
# MAGIC   var countsMap = scala.collection.mutable.Map[String, Int]().withDefaultValue(0)
# MAGIC
# MAGIC   // Iterate through the split values and count occurrences
# MAGIC   for (value <- splitValues) {
# MAGIC     countsMap(value) += 1
# MAGIC   }
# MAGIC
# MAGIC   // Convert the counts map to a comma-separated string
# MAGIC   val countsString = countsMap.map { case (key, value) => s"$value" }.mkString(",")
# MAGIC
# MAGIC   countsString
# MAGIC })
# MAGIC
# MAGIC // Register the UDF with a name
# MAGIC spark.udf.register("countIndividualValues", countIndividualValuesUDF)
# MAGIC

# COMMAND ----------


# This cell demonstrates the creation of a view named FisheryInsights using Spark SQL.
# The following SQL code defines a Common Table Expression (CTE) named BASE that joins multiple tables 
# and calculates various statistics related to fishery data. It creates calculated columns, applies conditions,
# and generates observations based on different status values. Finally, it groups and orders the results. 
# The view FisheryInsights is created based on the result of the SQL query and can be used for further analysis.
FisheryInsightsDf = spark.sql(f"""
-- Common Table Expression (CTE) named BASE
WITH BASE AS (
-- Select relevant columns and apply transformations
select
    l.designation as Reception,
    tl.spawningdate as Date,
    -- l.stockdate as Date,
    extractAndConcat(ttd.broodstocktanks) as broodstock_origen,
    ttd.eggno,
    ttd.initialeggno,
    ttd.broodstocktanks,
    ttd.broodstockorigin,
    tl.survivalrateafterhatching,
    tk.designation as activity,
    tk.designation as Broodstock_origen1,
    tk.tankvolume * 5800 as tankvolume_in_litres,
    CASE
            WHEN lps.designation IN ('Nursery', 'Incubacion') THEN 'nursery'
            WHEN lps.designation IN ('Coalimentacion rotifero / artemia', 'Alimentacion pellet', 'Alimentacion artemia', 'Eclosion', 'Primera alimentacion Rotiferos', 'Destete') THEN 'larvae'
            ELSE 'offshore'
        END AS status,
    th.age as DPH,
    COALESCE(th.netqty,0 )as Larvae_harvest,
    th.comments

from aquamanager_hatchery_dbo.tanklot tl
left join aquamanager_hatchery_dbo.lot l
on tl.lotid = l.lotid

left join aquamanager_hatchery_dbo.tank tk
on tl.tankid = tk.tankid

left join aquamanager_hatchery_dbo.tltransdetails ttd
on tl.tanklotid = ttd.tanklotid

left join aquamanager_hatchery_dbo.broodstockcostmain bscm
on tk.tankid = bscm.tankid

left join aquamanager_hatchery_dbo.tlharvest th
on tl.tanklotid = th.tanklotid

left join aquamanager_hatchery_dbo.larvaeproductionstage lps   
on tl.larvaeproductionstageid = lps.larvaeproductionstageid

where UPPER(ttd.broodstockorigin) = '{broodstockorigin}'
and  l.designation in ({Reception})
-- and tk.designation like 'Incubador%'
)
SELECT 
    -- Batch Number
    Reception,
    -- Spawning Date
    Date,
    removeDuplicates(concat_ws('-', collect_list(broodstock_origen))) AS broodstock_origen, 
    cast(SUM(initialeggno) as INT) AS Eggs_from,
    SUM(CASE WHEN activity LIKE 'Incubador%' THEN eggno ELSE 0 END) AS Incubation_stocking_egg_no,
    concat(bround((SUM(CASE WHEN activity LIKE 'Incubador%' THEN eggno ELSE 0 END) / SUM(initialeggno)  * 100 ),2) ,'%' ) AS percentage_Hatching_eggs,
    (SUM(CASE WHEN activity LIKE 'Incubador%' THEN eggno ELSE 0 END) - SUM(CASE WHEN NOT activity LIKE 'Incubador%' THEN eggno ELSE 0 END))  as Incubation_Mortality,
    SUM(CASE WHEN NOT activity LIKE 'Incubador%' THEN eggno ELSE 0 END) AS Survival_Incubation_larvae,
    concat(bround(((SUM(CASE WHEN activity LIKE 'Incubador%' THEN eggno ELSE 0 END) - SUM(CASE WHEN NOT activity LIKE 'Incubador%' THEN eggno ELSE 0 END)) / SUM(CASE WHEN activity LIKE 'Incubador%' THEN eggno ELSE 0 END) * 100) ,2) ,'%') as percentageAccumulatedMortality,
    concat(bround((SUM(CASE WHEN NOT activity LIKE 'Incubador%' THEN eggno ELSE 0 END) / SUM(CASE WHEN activity LIKE 'Incubador%' THEN eggno ELSE 0 END) * 100 ),2),'%' ) as percentage_Survival_egg_DPH2,
    SUM(CASE WHEN NOT activity LIKE 'Incubador%' THEN eggno ELSE 0 END) AS Stoking_tank_larvae,
    INT(SUM(CASE WHEN NOT activity LIKE 'Incubador%' THEN eggno ELSE 0 END)/sum(tankvolume_in_litres)) as `Larvae/lt`,
    SUM(Larvae_harvest) AS Larvae_harvest,
    countIndividualValues (concat_ws('-', collect_list(broodstock_origen))) AS Tank,
    
    --Survival_rate% = (Incubation_stocking_eggs - incubation_larvae - larvae_harvest) / eggs_from * 100
    -- concat(bround((SUM(CASE WHEN activity LIKE 'Incubador%' THEN eggno ELSE 0 END) 
    -- - SUM(CASE WHEN NOT activity LIKE 'Incubador%' THEN eggno ELSE 0 END)
    -- - SUM(Larvae_harvest) ) 
    -- / cast(SUM(initialeggno) as INT) * 100 ,2) , '%') as Survival_rate_egg_1,

   --Survival_rate% = Larvae_harvest  / Incubation_stocking_egg_no * 100   
    concat(bround(((SUM(Larvae_harvest))/(SUM(CASE WHEN activity LIKE 'Incubador%' THEN eggno ELSE 0 END))) * 100 ,2), '%') AS Survival_rate_egg,
    concat(COALESCE((SUM(Larvae_harvest)/SUM(CASE WHEN NOT activity LIKE 'Incubador%' THEN eggno ELSE 0 END)) * 100 ,0),'%')
    AS Survival_rate_larvae,
    max(status) as Status,
    --removeDuplicates(concat_ws('-', collect_list(status))) AS Status, 
    COALESCE(AVG(DPH),0) AS DPH , --days prior hatching
    CASE
    WHEN max(status) = 'nursery' THEN 'The fish are in the nursery area'
    WHEN max(status) = 'larvae' THEN 'The fish are in the larvae feeding'
    WHEN max(status) = 'offshore-nursery' THEN 'The fish are in the offshore-nursery area'
    WHEN max(status) = 'nursery-offshore' THEN 'The fish are in the nursery-offshore area'
    ELSE 'The fish are in the offshore' END AS Observations
    FROM BASE
    GROUP BY Reception,Date
    ORDER BY Date,broodstock_origen
""")
#.createOrReplaceTempView("FisheryInsights")

# COMMAND ----------

FisheryInsightsDf.write.format("delta").partitionBy("Reception").mode("append").saveAsTable("aquamanager_hatchery_dbo.view_hatchery_larvae")

# COMMAND ----------

# MAGIC %md
# MAGIC Below, we have created a cell to showcase the view **FisheryInsights** created in the cell above.
# MAGIC

# COMMAND ----------

#spark.sql("CREATE OR REPLACE TABLE aquamanager_hatchery_dbo.FisheryInsights AS SELECT * FROM FisheryInsights")

