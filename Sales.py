# Databricks notebook source
dbutils.fs.mount(
  source = "wasbs://<container-name>@<storage-account-name>.blob.core.windows.net",
  mount_point = "/mnt/iotdata",
  extra_configs = {"fs.azure.account.key.<storage-account-name>.blob.core.windows.net":dbutils.secrets.get(scope = "<scope-name>", key = "<key-name>")})


# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://raw@storageaccountfinals.blob.core.windows.net",
  mount_point = "/mnt/raw3",
   extra_configs = {"fs.azure.account.key.storageaccountfinals.blob.core.windows.net":"mStNnXSUs34WlYC+YjbJUoQTbQljPYrKKDKmxms+JtE7QfGw3b8ZpE5YTnISsK+vjpCnlhzS8reu+AStvqMU+A=="})


# COMMAND ----------

dbutils.fs.ls("/mnt/raw3/")

# COMMAND ----------

df=spark.read.format("csv").options(header='True',inferSchema='True').load('dbfs:/mnt/raw3/dbo.pizza_sales.txt')

# COMMAND ----------

display(df)

# COMMAND ----------

df.createOrReplaceTempView("pizza_sales_analysis")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from pizza_sales_analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC select order_id,
# MAGIC quantity,
# MAGIC date_format(order_date,"MMM" ) month_name,
# MAGIC date_format(order_date,"EEEE" ) day_name,
# MAGIC hour(order_time) order_time,
# MAGIC unit_price,
# MAGIC total_price,
# MAGIC pizza_size,
# MAGIC pizza_category,
# MAGIC pizza_name
# MAGIC from pizza_sales_analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC count(distinct order_id) order_id,
# MAGIC sum(quantity) quantity,
# MAGIC date_format(order_date,"MMM" ) month_name,
# MAGIC date_format(order_date,"EEEE" ) day_name,
# MAGIC hour(order_time) order_time,
# MAGIC sum(unit_price) unit_price,
# MAGIC sum(total_price) total_sales,
# MAGIC pizza_size,
# MAGIC pizza_category,
# MAGIC pizza_name
# MAGIC from pizza_sales_analysis
# MAGIC group by 3,4,5,8,9,10

# COMMAND ----------

# MAGIC % create table
