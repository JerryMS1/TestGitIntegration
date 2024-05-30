# Fabric notebook source

# METADATA ********************

# META {
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "546aff91-4476-4df7-adc7-640ad059f0cb",
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": "",
# META       "known_lakehouses": [
# META         {
# META           "id": "546aff91-4476-4df7-adc7-640ad059f0cb"
# META         },
# META         {
# META           "id": "e39b793d-3b80-4417-91be-39820bc57aa3"
# META         },
# META         {
# META           "id": "83a72ddb-6437-48bd-b44c-89907df3b078"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
 

# CELL ********************

import pandas as pd
# Load data into pandas DataFrame from f"{mssparkutils.nbResPath}/builtin/1.5million_records_small.csv"
df = pd.read_csv(f"{mssparkutils.nbResPath}/builtin/1.5million_records_small.csv")
display(df.head(10))


# CELL ********************

df = spark.read.format("csv").option("header","true").load("abfss://JerryTest@dxt-onelake.dfs.fabric.microsoft.com/LH1.Lakehouse/Files/1.5million_records_small_SPO.csv")
# df now is a Spark DataFrame containing CSV data from "abfss://JerryTest@dxt-onelake.dfs.fabric.microsoft.com/LH1.Lakehouse/Files/1.5million_records_small_SPO.csv".
display(df.head(5))
