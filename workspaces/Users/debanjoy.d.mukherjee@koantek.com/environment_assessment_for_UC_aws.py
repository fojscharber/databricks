# Databricks notebook source
# MAGIC %md
# MAGIC ##Environment Assessment
# MAGIC - Assessing current Databricks environment on AWS
# MAGIC - Please set Cluster Configuration as such:-  
# MAGIC   - Policy: Unrestricted
# MAGIC   - Access mode: Shared
# MAGIC - Please set the dapiToken from an Admin user Account, also set the Container and Storag Account Names.
# MAGIC - Please make sure storage is mounted and is accessible using cluster. 
# MAGIC - Also if you are using Cred Pass Through make sure Table Access Controls are enables on workspace level

# COMMAND ----------

import requests

# COMMAND ----------

databricks_host = dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()

# COMMAND ----------

dbutils.widgets.text("access_token", "Enter dapi token here", "Access Token")
access_token = dbutils.widgets.get("access_token")
# dbutils.widgets.text("bucket_name", "Enter bucket name here", "Bucket Name")
# bucket_name = dbutils.widgets.get("bucket_name")

# COMMAND ----------

if not access_token or access_token.lower() == 'enter dapi token here':
    dbutils.notebook.exit("Access token Empty")

# COMMAND ----------

headers = {"Authorization": f"Bearer {access_token}"}

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.sql.functions import explode_outer, explode

# COMMAND ----------

storage_path = f"dbfs:/FileStore/Environment_Assessment"

# COMMAND ----------

# MAGIC %md
# MAGIC ###Users

# COMMAND ----------

def get_users():
    response = requests.get(
        f"https://{databricks_host}/api/2.0/preview/scim/v2/Users", headers=headers)
    # print('Response status : ', response.status_code)
    user_lst = []
    if response.status_code == 200:
        json_response = response.json()
        if len(json_response) > 0:
            if "Resources" in json_response.keys():
                user_lst  = json_response["Resources"]
            else:
                user_lst = [{"message": "an error occured while getting users"}]
        else:
            user_lst = [{"message": "user not found"}]
    else:
        user_lst = [{"message": "an error occured while getting users"}]
    return user_lst

# COMMAND ----------

try:
    users_lst = get_users()
    print("Users:")
    users_df = spark.createDataFrame(users_lst)
    display(users_df)
    users_path = f"{storage_path}/users.parquet"
    users_df.write.parquet(users_path, mode="overwrite")
except Exception as e:
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Groups

# COMMAND ----------

def get_groups():
    response = requests.get(
        f"https://{databricks_host}/api/2.0/preview/scim/v2/Groups", headers=headers)
    # print('Response status : ', response.status_code)
    group_lst = []
    if response.status_code == 200:
        json_response = response.json()
        if len(json_response) > 0:
            if "Resources" in json_response.keys():
                group_lst  = json_response["Resources"]
            else:
                group_lst = [{"message": "an error occured while getting groups"}]
        else:
            group_lst = [{"message": "group not found"}]
    else:
        group_lst = [{"message": "an error occured while getting groups"}]
    return group_lst

# COMMAND ----------

try:
    groups_lst = get_groups()
    for group in groups_lst:
        for key, val in group.items():
            group[key] = str(val)
    print("Groups:")
    groups_df = spark.createDataFrame(groups_lst)
    display(groups_df)
    groups_path = f"{storage_path}/groups.parquet"
    groups_df.write.parquet(groups_path, mode="overwrite")
except Exception as e:
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ###ServicePrincipals

# COMMAND ----------

def get_service_principals():
    response = requests.get(
        f"https://{databricks_host}/api/2.0/preview/scim/v2/ServicePrincipals", headers=headers)
    # print('Response status : ', response.status_code)
    service_principal_lst = []
    if response.status_code == 200:
        json_response = response.json()
        if len(json_response) > 0:
            if "Resources" in json_response.keys():
                service_principal_lst  = json_response["Resources"]
            else:
                service_principal_lst = [{"message": "an error occured while getting ServicePrincipals"}]
        else:
            service_principal_lst = [{"message": "servicePrincipal not found"}]
    else:
        service_principal_lst = [{"message": "an error occured while getting ServicePrincipals"}]
    return service_principal_lst

# COMMAND ----------

try:
    service_principals_lst = get_service_principals()
    for element in service_principals_lst:
        for key, val in element.items():
            element[key] = str(val)
    print("ServicePrincipals:")
    service_principals_df = spark.createDataFrame(service_principals_lst)
    display(service_principals_df)
    service_principals_path = f"{storage_path}/service_principals.parquet"
    service_principals_df.write.parquet(service_principals_path, mode="overwrite")
except Exception as e:
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Clusters

# COMMAND ----------

def get_clusters():
    response = requests.get(
        f"https://{databricks_host}/api/2.0/clusters/list", headers=headers)
    # print('Response status : ', response.status_code)
    cluster_lst = []
    if response.status_code == 200:
        json_response = response.json()
        if len(json_response) > 0:
            if "clusters" in json_response.keys():
                cluster_lst  = json_response["clusters"]
            else:
                cluster_lst = [{"message": "an error occured while getting clusters"}]
        else:
            cluster_lst = [{"message": "cluster not found"}]
    else:
        cluster_lst = [{"message": "an error occured while getting clusters"}]
    return cluster_lst

# COMMAND ----------

try:
    clusters_lst = get_clusters()
    for cluster in clusters_lst:
        for key, val in cluster.items():
            cluster[key] = str(val)
    print("Clusters:")
    clusters_df = spark.createDataFrame(clusters_lst)
    display(clusters_df)
    clusters_path = f"{storage_path}/clusters.parquet"
    clusters_df.write.parquet(clusters_path, mode="overwrite")
except Exception as e:
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Cluster Policies

# COMMAND ----------

def get_cluster_policies():
    response = requests.get(
        f"https://{databricks_host}/api/2.0/policies/clusters/list", headers=headers)
    # print('Response status : ', response.status_code)
    cluster_policies_lst = []
    if response.status_code == 200:
        json_response = response.json()
        if len(json_response) > 0:
            if "policies" in json_response.keys():
                cluster_policies_lst  = json_response["policies"]
            else:
                cluster_policies_lst = [{"message": "an error occured while getting cluster policies"}]
        else:
            cluster_policies_lst = [{"message": "cluster policy not found"}]
    else:
        cluster_policies_lst = [{"message": "an error occured while getting cluster policies"}]
    return cluster_policies_lst

# COMMAND ----------

try:
    cluster_policies_lst = get_cluster_policies()
    cluster_policies_df = spark.createDataFrame(cluster_policies_lst)
    print("Cluster Policies:")
    display(cluster_policies_df)
    cluster_policies_path = f"{storage_path}/cluster_policies.parquet"
    cluster_policies_df.write.parquet(cluster_policies_path, mode="overwrite")
except Exception as e:
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Cluster Permissions

# COMMAND ----------

def get_cluster_permissions(cluster_id):
    response = requests.get(
        f"https://{databricks_host}/api/2.0/permissions/clusters/{cluster_id}", headers=headers)
    # print('Response status : ', response.status_code)
    if response.status_code == 200:
        json_response = response.json()
        if len(json_response) > 0:
            cluster_permissions = json_response
        else:
            cluster_permissions = {"message": "cluster permissions not found"}
    else:
        cluster_permissions = {"message": f"an error occured while getting cluster permissions for {cluster_id}"}
    return cluster_permissions

# COMMAND ----------

try:
    cluster_permissions_lst = []
    if list(clusters_lst[0].keys())[0] != 'message':
        for cluster in clusters_lst:
            cluster_id = cluster["cluster_id"]
            cluster_permissions_dct = get_cluster_permissions(cluster_id)
            cluster_permissions_dct["cluster_id"] = cluster_id
            cluster_permissions_lst.append(cluster_permissions_dct)
        cluster_permissions_df = spark.createDataFrame(cluster_permissions_lst)
        print("Cluster permissions:")
        # display(cluster_permissions_df)

        cluster_permissions_df = (
        cluster_permissions_df.withColumn("access_control", explode_outer("access_control_list"))
        .select("cluster_id", "access_control")
        # .drop("access_control_list", "object_type", "object_id")
        )
        # print("Relevant Cluster permissions:")
        display(cluster_permissions_df)
        cluster_permissions_path = f"{storage_path}/cluster_permissions.parquet"
        cluster_permissions_df.write.parquet(cluster_permissions_path, mode="overwrite")
    else:
        print("Cluster not found, can't retrieve cluster permissions")
except Exception as e:
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ###SQL Warehouses

# COMMAND ----------

def get_warehouses():
    response = requests.get(
        f"https://{databricks_host}/api/2.0/sql/warehouses", headers=headers)
    # print('Response status : ', response.status_code)
    warehouse_lst = []
    if response.status_code == 200:
        json_response = response.json()
        if len(json_response) > 0:
            if "warehouses" in json_response.keys():
                warehouse_lst  = json_response["warehouses"]
            else:
                warehouse_lst = [{"message": "an error occured while getting warehouses"}]
        else:
            warehouse_lst = [{"message": "SQL warehouse not found"}]
    else:
        warehouse_lst = [{"message": "an error occured while getting warehouses"}]
    return warehouse_lst

# COMMAND ----------

try:
    warehouses_lst = get_warehouses()
    for warehouse in warehouses_lst:
        for key, val in warehouse.items():
            warehouse[key] = str(val)
    warehouses_df = spark.createDataFrame(warehouses_lst)
    print("SQL Warehouses:")
    display(warehouses_df)
    warehouses_path = f"{storage_path}/warehouses.parquet"
    warehouses_df.write.parquet(warehouses_path, mode="overwrite")
except Exception as e:
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ###SQL Warehouse Permissions

# COMMAND ----------

def get_warehouse_permissions(warehouse_id):
    response = requests.get(
        f"https://{databricks_host}/api/2.0/permissions/warehouses/{warehouse_id}", headers=headers)
    # print('Response status : ', response.status_code)
    if response.status_code == 200:
        json_response = response.json()
        if len(json_response) > 0:
            warehouse_permissions = json_response
        else:
            warehouse_permissions = {"message": "warehouse permission not found"}
    else:
        warehouse_permissions = {"message": f"an error occured while getting warehouse permissions for {warehouse_id}"}
    return warehouse_permissions

# COMMAND ----------

try:
    warehouse_permissions_lst = []
    if list(warehouses_lst[0].keys())[0] != 'message':
        for warehouse in warehouses_lst:
            warehouse_id = warehouse["id"]
            warehouse_permissions_dct = get_warehouse_permissions(warehouse_id)
            warehouse_permissions_dct["warehouse_id"] = warehouse_id
            warehouse_permissions_lst.append(warehouse_permissions_dct)
        warehouse_permissions_df = spark.createDataFrame(warehouse_permissions_lst)
        print("SQL Warehouse permissions:")
        # display(warehouse_permissions_df)

        warehouse_permissions_df = (
        warehouse_permissions_df.withColumn("access_control", explode_outer("access_control_list"))
        .select("warehouse_id", "access_control")
        # .drop("access_control_list", "object_id", "object_type")
        )
        # print("Relevant SQL Warehouse permissions:")
        display(warehouse_permissions_df)
        warehouse_permissions_path = f"{storage_path}/warehouse_permissions.parquet"
        warehouse_permissions_df.write.parquet(warehouse_permissions_path, mode="overwrite")
    else:
        print("SQL Warehouse not found, can't retrieve warehouse permissions")
except Exception as e:
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Jobs

# COMMAND ----------

def get_jobs():
    response = requests.get(
        f"https://{databricks_host}/api/2.1/jobs/list", headers=headers)
    # print('Response status : ', response.status_code)
    job_lst = []
    if response.status_code == 200:
        json_response = response.json()
        if len(json_response) > 0:
            if "jobs" in json_response.keys():
                job_lst  = json_response["jobs"]
            else:
                job_lst = [{"message": "an error occured while getting jobs"}]
        else:
            job_lst = [{"message": "job not found"}]
    else:
        job_lst = [{"message": "an error occured while getting jobs"}]
    return job_lst

# COMMAND ----------

try:
    jobs_lst = get_jobs()
    jobs_df = spark.createDataFrame(jobs_lst)
    print("Jobs:")
    display(jobs_df)
except Exception as e:
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ###More Details on a Job (like source etc)

# COMMAND ----------

def get_job_details(job_id):
    data = {"job_id": job_id}
    response = requests.get(
        f"https://{databricks_host}/api/2.1/jobs/get", headers=headers, json=data)
    # print('Response status : ', response.status_code)
    if response.status_code == 200:
        json_response = response.json()
        if len(json_response) > 0:
            job_details = json_response
        else:
            job_details = {"message": "job details not found"}
    else:
        job_details = {"message": f"an error occured while getting jobs details for {job_id}"}
    return job_details

# COMMAND ----------

try:
    job_details_lst = []
    if list(jobs_lst[0].keys())[0] != 'message':
        for job in jobs_lst:
            job_id = job["job_id"]
            job_details_dct = get_job_details(job_id)
            job_details_lst.append(job_details_dct)
        job_details_df = spark.createDataFrame(job_details_lst)
        print("Job Details:")
        display(job_details_df)
        job_details_path = f"{storage_path}/jobs.parquet"
        job_details_df.write.parquet(job_details_path, mode="overwrite")
    else:
        print("Job not found, can't get more details")
except Exception as e:
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Job Permissions

# COMMAND ----------

def get_job_permissions(job_id):
    response = requests.get(
        f"https://{databricks_host}/api/2.0/permissions/jobs/{job_id}", headers=headers)
    # print('Response status : ', response.status_code)
    if response.status_code == 200:
        json_response = response.json()
        if len(json_response) > 0:
            job_permissions = json_response
        else:
            job_permissions = {"message": "job permission not found"}
    else:
        job_permissions = {"message": f"an error occured while getting job permissions for {job_id}"}
    return job_permissions

# COMMAND ----------

try:
    job_permissions_lst = []
    if list(jobs_lst[0].keys())[0] != 'message':
        for job in jobs_lst:
            job_id = job["job_id"]
            job_permissions_dct = get_job_permissions(job_id)
            job_permissions_dct["job_id"] = job_id
            job_permissions_lst.append(job_permissions_dct)
        job_permissions_df = spark.createDataFrame(job_permissions_lst)
        print("Job Permissions:")
        # display(job_permissions_df)

        job_permissions_df = (
        job_permissions_df.withColumn("access_control", explode_outer("access_control_list"))
        .select("job_id", "access_control")
        # .drop("access_control_list", "object_id", "object_type")
        )
        # print("Relevant Job Permissions:")
        display(job_permissions_df)
        job_permissions_path = f"{storage_path}/job_permissions.parquet"
        job_permissions_df.write.parquet(job_permissions_path, mode="overwrite")
    else:
        print("Job not found, can't retrieve job permissions")
except Exception as e:
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ###DLT Pipelines

# COMMAND ----------

def get_dlt_pipelines(dlt_pipeline_lst=[], page_token=""):
    if page_token == "":
        response = requests.get(
            f"https://{databricks_host}/api/2.0/pipelines", headers=headers
        )
    else:
        data = {"page_token": page_token}
        response = requests.get(
        f"https://{databricks_host}/api/2.0/pipelines", headers=headers, json=data
        )
    # print('Response status : ', response.status_code)
    # dlt_pipeline_lst = []
    if response.status_code == 200:
        json_response = response.json()
        if len(json_response) > 0:
            if "statuses" in json_response.keys():
                dlt_pipeline_lst.extend(json_response["statuses"])
                if "next_page_token" in json_response.keys():
                    page_token = json_response["next_page_token"]
                    dlt_pipeline_lst = get_dlt_pipelines(dlt_pipeline_lst, page_token)
            else:
                dlt_pipeline_lst = [{"message": "an error occured while getting dlt pipelines"}]
        else:
            dlt_pipeline_lst = [{"message": "dlt pipeline not found"}]
    else:
        dlt_pipeline_lst = [{"message": "an error occured while getting dlt pipelines"}]
    return dlt_pipeline_lst

# COMMAND ----------

try:
    dlt_pipelines_lst = get_dlt_pipelines()
    dlt_pipelines_df = spark.createDataFrame(dlt_pipelines_lst)
    print("DLT Pipelines:")
    display(dlt_pipelines_df)
    dlt_pipelines_path = f"{storage_path}/dlt_pipelines.parquet"
    dlt_pipelines_df.write.parquet(dlt_pipelines_path, mode="overwrite")
except Exception as e:
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ###DLT Pipeline Permissions

# COMMAND ----------

def get_dlt_pipeline_permissions(pipeline_id):
    response = requests.get(
        f"https://{databricks_host}/api/2.0/permissions/pipelines/{pipeline_id}", headers=headers)
    # print('Response status : ', response.status_code)
    if response.status_code == 200:
        json_response = response.json()
        if len(json_response) > 0:
            dlt_pipeline_permissions = json_response
        else:
            dlt_pipeline_permissions = {"message": "dlt permission not found"}
    else:
        dlt_pipeline_permissions = {"message": f"an error occured while getting dlt permissions for {pipeline_id}"}
    return dlt_pipeline_permissions

# COMMAND ----------

try:
    dlt_pipeline_permissions_lst = []
    if list(dlt_pipelines_lst[0].keys())[0] != 'message':
        for dlt_pipeline in dlt_pipelines_lst:
            pipeline_id = dlt_pipeline["pipeline_id"]
            dlt_pipeline_permissions_dct = get_dlt_pipeline_permissions(pipeline_id)
            dlt_pipeline_permissions_dct["pipeline_id"] = pipeline_id
            dlt_pipeline_permissions_lst.append(dlt_pipeline_permissions_dct)
        dlt_pipeline_permissions_df = spark.createDataFrame(dlt_pipeline_permissions_lst)
        print("DLT Pipeline Permissions:")
        # display(dlt_pipeline_permissions_df)

        dlt_pipeline_permissions_df = (
        dlt_pipeline_permissions_df.withColumn("access_control", explode_outer("access_control_list"))
        .select("pipeline_id", "access_control")
        # .drop("access_control_list", "object_id", "object_type")
        )
        # print("Relevant DLT Pipeline Permissions:")
        display(dlt_pipeline_permissions_df)
        dlt_pipeline_permissions_path = f"{storage_path}/dlt_pipeline_permissions.parquet"
        dlt_pipeline_permissions_df.write.parquet(dlt_pipeline_permissions_path, mode="overwrite")
    else:
        print("DLT Pipeline not found, can't retrieve dlt permissions")
except Exception as e:
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Catalogs

# COMMAND ----------

try:
    catalogs_df = spark.sql(f"SHOW CATALOGS")
    display(catalogs_df)
    catalogs_path = f"{storage_path}/catalogs.parquet"
    catalogs_df.write.parquet(catalogs_path, mode="overwrite")
except Exception as e:
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Databases in hive metastore

# COMMAND ----------

try:
    spark.sql(f"USE CATALOG hive_metastore")
    databases_df = spark.sql('SHOW DATABASES')
    databases_lst = [db.databaseName for db in databases_df.collect()]
    display(databases_df)
    databases_path = f"{storage_path}/databases.parquet"
    databases_df.write.parquet(databases_path, mode="overwrite")
except Exception as e:
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Tables

# COMMAND ----------

from pyspark.sql import Row
db_tables_dct = {}
for database in databases_lst:
    spark.sql(f"USE DATABASE {database}")
    tables = [table["tableName"] for table in spark.sql("SHOW TABLES").collect()]
    db_tables_dct[database] = tables
# db_tables_dct

# COMMAND ----------

try:
    tables_schema = StructType([
        StructField("Database", StringType()),
        StructField("Tables", ArrayType(StringType()))
      ])
    tables_df = spark.createDataFrame(db_tables_dct.items(), schema=tables_schema)
    tables_df = (
        tables_df.withColumn("Table", explode("Tables"))
        .drop("Tables")
    )
    # display(tables_df)
    # tables_path = f"{storage_path}/tables.parquet"
    # tables_df.write.parquet(tables_path, mode="overwrite")

    detailed_tbl_lst = []
    for table in tables_df.collect():
        db_name = table["Database"]
        tbl_name = table["Table"]
        location, provider, owner, tbl_type = "", "", "", ""
        try:
            ext_tbl_df = spark.sql(f"DESCRIBE TABLE EXTENDED hive_metastore.{db_name}.{tbl_name}")
            for tbl_row in ext_tbl_df.collect():
                if tbl_row["col_name"]=='Location':
                    location = tbl_row["data_type"]
                if tbl_row["col_name"]=='Provider':
                    provider = tbl_row["data_type"]
                if tbl_row["col_name"]=='Owner':
                    owner = tbl_row["data_type"]
                if tbl_row["col_name"]=='Type':
                    tbl_type = tbl_row["data_type"]
        except Exception as e:
            pass
            # print("An error occured while getting table details")

        detailed_tbl_lst.append({"Database": db_name, "Table": tbl_name, "Location": location, "Provider": provider, "Owner": owner, "Table_Type": tbl_type})

    # print(detailed_tbl_lst)
    detailed_tbl_df = spark.createDataFrame(detailed_tbl_lst)
    display(detailed_tbl_df)
    tables_path = f"{storage_path}/tables.parquet"
    tables_df.write.parquet(tables_path, mode="overwrite")
except Exception as e:
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Table Grants

# COMMAND ----------

def get_table_grants(db_tables_dct):
    grants_lst = []
    for db, table_lst in db_tables_dct.items():
        for table in table_lst:
            grants_df = spark.sql(f"SHOW GRANTS ON TABLE hive_metastore.{db}.{table}")
            grants_dct = {}
            for grants in grants_df.collect():
                if grants["Principal"] in grants_dct.keys():
                    grants_dct[grants["Principal"]].append({"ActionType":grants["ActionType"], "ObjectType":grants["ObjectType"]})
                else:
                    grants_dct[grants["Principal"]] = [{"ActionType":grants["ActionType"], "ObjectType":grants["ObjectType"]}]

            grants_lst.append({"Database": db, "Table": table, "Grants": [grants_dct]})
    return grants_lst

# COMMAND ----------

try:
    if len(db_tables_dct) > 0:
        table_grants_lst = get_table_grants(db_tables_dct)
        # print(table_grants_lst)
        table_grants_df = spark.createDataFrame(table_grants_lst)
        table_grants_df = table_grants_df.select("*", explode_outer("Grants")).drop("Grants")
        table_grants_df = (
            table_grants_df.select("*", explode_outer("col")).drop("col")
            .withColumnRenamed("key", "Principal")
            .withColumnRenamed("value", "Grants")
        )
        display(table_grants_df)
        table_grants_path = f"{storage_path}/table_grants.parquet"
        table_grants_df.write.parquet(table_grants_path, mode="overwrite")
    else:
        print("No tables found, can't retrieve permissions")
except Exception as e:
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Mount Points

# COMMAND ----------

# try:
#     mount_points = dbutils.fs.mounts()
#     # print(mount_points)
#     mount_points_df = spark.createDataFrame(mount_points)
#     display(mount_points_df)
#     mount_points_path = f"{storage_path}/mount_points.parquet"
#     mount_points_df.write.parquet(mount_points_path, mode="overwrite")
# except Exception as e:
#     print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ###SQL Queries

# COMMAND ----------

def get_sql_queries():
    response = requests.get(
        f"https://{databricks_host}/api/2.0/preview/sql/queries", headers=headers)
    # print('Response status : ', response.status_code)
    sql_queries_lst = []
    if response.status_code == 200:
        json_response = response.json()
        if json_response["count"] > 0:
            if "results" in json_response.keys():
                sql_queries_lst  = json_response["results"]
            else:
                sql_queries_lst = [{"message": "an error occured while getting sql queries"}]
        else:
            sql_queries_lst = [{"message": "sql query not found"}]
    else:
        sql_queries_lst = [{"message": "an error occured while getting sql queries"}]
    return sql_queries_lst

# COMMAND ----------

try:
    sql_queries_lst = get_sql_queries()
    for query in sql_queries_lst:
        for key, val in query.items():
            query[key] = str(val)
    sql_queries_df = spark.createDataFrame(sql_queries_lst)
    print("SQL Queries:")
    display(sql_queries_df)
    sql_queries_path = f"{storage_path}/sql_queries.parquet"
    sql_queries_df.write.parquet(sql_queries_path, mode="overwrite")
except Exception as e:
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ###SQL Dashboards

# COMMAND ----------

def get_sql_dashboards():
    response = requests.get(
        f"https://{databricks_host}/api/2.0/preview/sql/dashboards", headers=headers)
    # print('Response status : ', response.status_code)
    sql_dashboards_lst = []
    if response.status_code == 200:
        json_response = response.json()
        if json_response["count"] > 0:
            if "results" in json_response.keys():
                sql_dashboards_lst  = json_response["results"]
            else:
                sql_dashboards_lst = [{"message": "an error occured while getting sql dashboards"}]
        else:
            sql_dashboards_lst = [{"message": "sql dashboard not found"}]
    else:
        sql_dashboards_lst = [{"message": "an error occured while getting sql dashboards"}]
    return sql_dashboards_lst

# COMMAND ----------

try:
    sql_dashboards_lst = get_sql_dashboards()
    for dashboard in sql_dashboards_lst:
        for key, val in dashboard.items():
            dashboard[key] = str(val)
    sql_dashboards_df = spark.createDataFrame(sql_dashboards_lst)
    print("SQL Dashboards:")
    display(sql_dashboards_df)
    sql_dashboards_path = f"{storage_path}/sql_dashboards.parquet"
    sql_dashboards_df.write.parquet(sql_dashboards_path, mode="overwrite")
except Exception as e:
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ###SQL Alerts

# COMMAND ----------

def get_sql_alerts():
    response = requests.get(
        f"https://{databricks_host}/api/2.0/preview/sql/alerts", headers=headers)
    # print('Response status : ', response.status_code)
    sql_alerts_lst = []
    if response.status_code == 200:
        json_response = response.json()
        if len(json_response) > 0:
            sql_alerts_lst  = json_response
        else:
            sql_alerts_lst = [{"message": "sql alert not found"}]
    else:
        sql_alerts_lst = [{"message": "an error occured while getting sql alerts"}]
    return sql_alerts_lst

# COMMAND ----------

try:
    sql_alerts_lst = get_sql_alerts()
    for alert in sql_alerts_lst:
        for key, val in alert.items():
            alert[key] = str(val)
    sql_alerts_df = spark.createDataFrame(sql_alerts_lst)
    print("SQL Alerts:")
    display(sql_alerts_df)
    sql_alerts_path = f"{storage_path}/sql_alerts.parquet"
    sql_alerts_df.write.parquet(sql_alerts_path, mode="overwrite")
except Exception as e:
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Permissions related to SQL Queries, Dashboards, Alerts

# COMMAND ----------

def get_sql_permissions(object_type, object_id):
    response = requests.get(
        f"https://{databricks_host}/api/2.0/preview/sql/permissions/{object_type}/{object_id}", headers=headers)
    # print('Response status : ', response.status_code)
    sql_permissions_lst = []
    if response.status_code == 200:
        json_response = response.json()
        if len(json_response) > 0:
            sql_permissions_lst  = json_response
        else:
            sql_permissions_lst = {"message": "sql permission not found"}
    else:
        sql_permissions_lst = {"message": "an error occured while getting sql permissions"}
    return sql_permissions_lst

# COMMAND ----------

try:
    sql_permissions_lst = []
    if list(sql_queries_lst[0].keys())[0] != 'message':
        for query in sql_queries_lst:
            query_id = query["id"]
            query_dct = get_sql_permissions("queries", query_id)
            query_dct["id"] = query_id
            sql_permissions_lst.append(query_dct)
    else:
        print("SQL query not found, can't retrieve permissions")
    
    if list(sql_dashboards_lst[0].keys())[0] != 'message':
        for dashboard in sql_dashboards_lst:
            dashboard_id = dashboard["id"]
            dashboard_dct = get_sql_permissions("dashboards", dashboard_id)
            dashboard_dct["id"] = dashboard_id
            sql_permissions_lst.append(dashboard_dct)
    else:
        print("SQL Dashboards not found, can't retrieve permissions")
    
    if list(sql_alerts_lst[0].keys())[0] != 'message':
        for alert in sql_alerts_lst:
            alert_id = alert["id"]
            alert_dct = get_sql_permissions("alerts", alert_id)
            alert_dct["id"] = alert_id
            sql_permissions_lst.append(alert_dct)
    else:
        print("SQL Alerts not found, can't retrieve permissions")

    if len(sql_permissions_lst) > 0:
        sql_permissions_df = spark.createDataFrame(sql_permissions_lst)
        sql_permissions_df = sql_permissions_df.drop("object_id")
        print("SQL Permissions:")
        display(sql_permissions_df)
        sql_permissions_path = f"{storage_path}/sql_permissions.parquet"
        sql_permissions_df.write.parquet(sql_permissions_path, mode="overwrite")
    else:
        print("SQL Permission not found")
except Exception as e:
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Instance Profile

# COMMAND ----------

def get_instance_profiles():
    response = requests.get(
        f"https://{databricks_host}/api/2.0/instance-profiles/list", headers=headers)
    # print('Response status : ', response.status_code)
    instance_profiles_lst = []
    if response.status_code == 200:
        json_response = response.json()
        if len(json_response) > 0:
            if "instance_profiles" in json_response.keys():
                instance_profiles_lst  = json_response["instance_profiles"]
            else:
                instance_profiles_lst = [{"message": "an error occured while getting instance-profiles"}]
        else:
            instance_profiles_lst = [{"message": "instance-profile not found"}]
    else:
        instance_profiles_lst = [{"message": "an error occured while getting instance-profiles"}]
    return instance_profiles_lst

# COMMAND ----------

try:
    instance_profiles_lst = get_instance_profiles()
    instance_profiles_df = spark.createDataFrame(instance_profiles_lst)
    print("Instance-Profiles:")
    display(instance_profiles_df)
    instance_profiles_path = f"{storage_path}/instance_profiles.parquet"
    instance_profiles_df.write.parquet(instance_profiles_path, mode="overwrite")
except Exception as e:
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Experiments

# COMMAND ----------

def get_experiments(experiments_lst=[], page_token=""):
    if page_token == "":
        response = requests.get(
            f"https://{databricks_host}/api/2.0/mlflow/experiments/list", headers=headers
        )
    else:
        data = {"page_token": page_token}
        response = requests.get(
        f"https://{databricks_host}/api/2.0/mlflow/experiments/list", headers=headers, json=data
        )
    # print('Response status : ', response.status_code)
    # experiments_lst = []
    if response.status_code == 200:
        json_response = response.json()
        if len(json_response) > 0:
            if "experiments" in json_response.keys():
                experiments_lst.extend(json_response["experiments"])
                if "next_page_token" in json_response.keys():
                    page_token = json_response["next_page_token"]
                    experiments_lst = get_experiments(experiments_lst, page_token)
            else:
                experiments_lst = [{"message": "an error occured while getting experiments"}]
        else:
            experiments_lst = [{"message": "experiment not found"}]
    else:
        experiments_lst = [{"message": "an error occured while getting experiments"}]
    return experiments_lst

# COMMAND ----------

try:
    experiments_lst = get_experiments()
    experiments_df = spark.createDataFrame(experiments_lst)
    print("Experiments:")
    display(experiments_df)
    experiments_path = f"{storage_path}/experiments.parquet"
    experiments_df.write.parquet(experiments_path, mode="overwrite")
except Exception as e:
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Experiment Permissions

# COMMAND ----------

def get_experiment_permissions(experiment_id):
    response = requests.get(
        f"https://{databricks_host}/api/2.0/permissions/experiments/{experiment_id}", headers=headers)
    # print('Response status : ', response.status_code)
    if response.status_code == 200:
        json_response = response.json()
        if len(json_response) > 0:
            experiment_permissions = json_response
        else:
            experiment_permissions = {"message": "experiment permissions not found"}
    else:
        experiment_permissions = {"message": f"an error occured while getting experiment permissions for {experiment_id}"}
    return experiment_permissions

# COMMAND ----------

try:
    experiment_permissions_lst = []
    if list(experiments_lst[0].keys())[0] != 'message':
        for experiment in experiments_lst:
            experiment_id = experiment["experiment_id"]
            experiment_permissions_dct = get_experiment_permissions(experiment_id)
            experiment_permissions_dct["experiment_id"] = experiment_id
            experiment_permissions_lst.append(experiment_permissions_dct)
        experiment_permissions_df = spark.createDataFrame(experiment_permissions_lst)
        experiment_permissions_df = experiment_permissions_df.select("experiment_id", "access_control_list")
        print("Experiment Permissions:")
        display(experiment_permissions_df)
    else:
        print("Experiment not found, can't retrieve experiment permissions")
except Exception as e:
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Registered Models

# COMMAND ----------

def get_registered_models(registered_models_lst=[], page_token=""):
    if page_token == "":
        response = requests.get(
            f"https://{databricks_host}/api/2.0/mlflow/registered-models/list", headers=headers
        )
    else:
        data = {"page_token": page_token}
        response = requests.get(
        f"https://{databricks_host}/api/2.0/mlflow/registered-models/list", headers=headers, json=data
        )
    # print('Response status : ', response.status_code)
    # registered_models_lst = []
    if response.status_code == 200:
        json_response = response.json()
        if len(json_response) > 0:
            if "registered_models" in json_response.keys():
                registered_models_lst.extend(json_response["registered_models"])
                if "next_page_token" in json_response.keys():
                    page_token = json_response["next_page_token"]
                    registered_models_lst = get_registered_models(registered_models_lst, page_token)
            else:
                registered_models_lst = [{"message": "an error occured while getting registered models"}]
        else:
            registered_models_lst = [{"message": "no registered model found"}]
    else:
        registered_models_lst = [{"message": "an error occured while getting registered models"}]
    return registered_models_lst

# COMMAND ----------

def get_registered_model_details(model_name):
    data = {"name" : model_name}
    response = requests.get(
        f"https://{databricks_host}/api/2.0/mlflow/databricks/registered-models/get", headers=headers, json=data)
    # print('Response status : ', response.status_code)
    if response.status_code == 200:
        json_response = response.json()
        if len(json_response) > 0:
            if "registered_model_databricks" in json_response:
                model_details = json_response["registered_model_databricks"]
            else:
                model_details = {"message": f"an error occured while getting models details for {model_name}"}
        else:
            model_details = {"message": "model details not found"}
    else:
        model_details = {"message": f"an error occured while getting models details for {model_name}"}
    return model_details

# COMMAND ----------

try:
    registered_model_details_lst = []
    registered_models_lst = get_registered_models()
    # registered_models_df = spark.createDataFrame(registered_models_lst)
    if list(registered_models_lst[0].keys())[0] != "message":
        for registered_model in registered_models_lst:
            model_name = registered_model["name"]
            registered_model_details_dct = get_registered_model_details(model_name)
            registered_model_details_lst.append(registered_model_details_dct)
        registered_models_df = spark.createDataFrame(registered_model_details_lst)
        print("Registered Models:")
        display(registered_models_df)
        registered_models_path = f"{storage_path}/registered_models.parquet"
        registered_models_df.write.parquet(registered_models_path, mode="overwrite")
    else:
        print("Registered model not found, can't retrieve more details")
except Exception as e:
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Registered Model Permissions

# COMMAND ----------

def get_registered_model_permissions(registered_model_id):
    response = requests.get(
        f"https://{databricks_host}/api/2.0/permissions/registered-models/{registered_model_id}", headers=headers)
    # print('Response status : ', response.status_code)
    if response.status_code == 200:
        json_response = response.json()
        if len(json_response) > 0:
            registered_model_permissions = json_response
        else:
            registered_model_permissions = {"message": "registered model permissions not found"}
    else:
        registered_model_permissions = {"message": f"an error occured while getting registered model permissions for {registered_model_id}"}
    return registered_model_permissions

# COMMAND ----------

try:
    registered_model_permissions_lst = []
    if len(registered_model_details_lst) > 0:
        for registered_model in registered_model_details_lst:
            registered_model_id = registered_model["id"]
            registered_model_permissions_dct = get_registered_model_permissions(registered_model_id)
            registered_model_permissions_dct["registered_model_id"] = registered_model_id
            registered_model_permissions_lst.append(registered_model_permissions_dct)
        registered_model_permissions_df = spark.createDataFrame(registered_model_permissions_lst)
        registered_model_permissions_df = registered_model_permissions_df.select("registered_model_id","access_control_list")
        print("Registered Model Permissions:")
        display(registered_model_permissions_df)
    else:
        print("Registered models not found, can't retrieve model permissions")
except Exception as e:
    print(e)

# COMMAND ----------



