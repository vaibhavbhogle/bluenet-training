# Databricks notebook source
# DBTITLE 1,Read whole table using JDBC connection
#driver to be installed on cluster - org.postgresql:postgresql:42.2.20.jre7

df = spark.read.format("jdbc")\
                .option("driver", "org.postgresql.Driver")\
                .option("url", "jdbc:postgresql://<host-name>/<database>")\
                .option("dbtable", "<schema>.<table>")\
                .option("user", "<username>")\
                .option("password", "<password>")\
                .load()

display(df)

# COMMAND ----------

# DBTITLE 1,Create Dataframe using query in JDBC connection
url = "jdbc:postgresql://<host-name>/<database>"

properties = {
  "user": "<username>",
  "password": "<password>",
  "driver": "org.postgresql.Driver"
}

# wrok around to fire a query instead of picking whole table
sql = "(<your valid subquery>) sqltable"

df1 = spark.read.jdbc(url=url, table=sql, properties=properties)

# COMMAND ----------

# DBTITLE 1,Write Dataframe to JDBC connected database
df2 = spark.read.format("csv").option("header", True).option("inferschema", True).load("/FileStore/tables/Employee_Data.csv")

df2.write.jdbc(url=url, table="public.employees_databricks", mode="overwrite", properties=properties)