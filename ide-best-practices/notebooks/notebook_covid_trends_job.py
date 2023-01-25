# Databricks notebook source
dbutils.widgets.text("conf_path", "/dbfs/tmp/users/metecanakar/proj-conf/conf.json")

# COMMAND ----------

pip install /dbfs/dbx/dbx-getting-started/976dbe045b21419b8dc91501b39a50f1/artifacts/dist/covid_analysis-0.0.2-py3-none-any.whl

# COMMAND ----------

'''
Python Spark job that imports the latest COVID-19 hospitalization data
'''
import sys
import pandas as pd
from pyspark.sql import SparkSession

from covid_analysis.transforms import *
from covid_analysis.common.project_config import create_config


conf_path = dbutils.widgets.get("conf_path")
# get the project configuration files
config_dict = create_config(conf_path=conf_path)
print(config_dict["variables"]["mete_name"])
print(config_dict["variables"]["mete_age"])

spark = SparkSession.builder.getOrCreate()

# check if job is running in production mode
is_prod = len(sys.argv) >= 2 and sys.argv[1] == "--prod"

# read from /tmp, subset for USA, pivot and fill missing values
df = pd.read_csv(
    "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/hospitalizations/covid-hospitalizations.csv")

df = filter_country(df)
df = pivot_and_clean(df, fillna=0)
df = clean_spark_cols(df)
df = index_to_col(df, colname='date')

# Convert from Pandas to a pyspark sql DataFrame.
df = spark.createDataFrame(df)

# only write table in production mode
if is_prod:
    # Write to Delta Lake
    df.write.mode('overwrite').saveAsTable('covid_stats')
    print("Covid data successfully imported.")

# display sample data
if is_prod:
    spark.sql('select * from covid_stats').show(10)
else:
    df.show(12)
