# Databricks notebook source
distinct_causes_df = spark.sql("""
    SELECT DISTINCT(designation)
    FROM aquamanager_hatchery_dbo.cause
""")

# Collecting the results and converting them into a tuple
causes_tuple = tuple(row.designation for row in distinct_causes_df.collect())

# Show the created tuple
print(causes_tuple)

# COMMAND ----------

spark.sql(f"""
        CREATE OR REPLACE VIEW aquamanager_hatchery_dbo.view_nursery_report AS(
SELECT *
FROM (
    SELECT 
       costmain.costdate as Fetcha, tank.designation as Tanque_Destino,costmain.startfishnoatnursery as Peces_Inicial,
       costmain.startavgweightatnursery as Peso_gr,costmain.age as DPH,costmain.daymortalno as Mortalidad_N,costmain.accmortalno as Mortalidad_acc_N,
       CAST((costmain.daymortalno/costmain.startfishnoatnursery) * 100 as decimal(10,4)) as Mortalidad_per,
       CAST((costmain.accmortalno/costmain.startfishnoatnursery) * 100 as decimal(10,4)) as Mortalidad_acc_per,
       costmain.startfishnoatnursery - costmain.accmortalno as Peces_Final,
       --tltransdetails.tanklotid,
       cause.designation as cause_of_death,
       count(cause.designation) as number_of_death_fish
    FROM aquamanager_hatchery_dbo.costmain 
    LEFT JOIN aquamanager_hatchery_dbo.tanklot on (costmain.tanklotid = tanklot.tanklotid)
    LEFT JOIN aquamanager_hatchery_dbo.tank on (tanklot.tankid = tank.tankid)
    LEFT JOIN aquamanager_hatchery_dbo.tltransdetails on (costmain.tanklotid = tltransdetails.tanklotid)
    LEFT JOIN aquamanager_hatchery_dbo.cause on (cause.causeid = tltransdetails.causeid)
    WHERE tank.designation IN (select distinct(designation) from aquamanager_hatchery_dbo.tank where designation like "N%")
        AND cause.designation IS NOT NULL 
        AND costdate like '2023-07-%%'
    GROUP BY ALL
) as SourceTable
PIVOT (
    count(number_of_death_fish)
    FOR cause_of_death IN {causes_tuple}
)
ORDER BY Fetcha);""")#.createOrReplaceTempView("temp_view_nursery_report")


# COMMAND ----------



