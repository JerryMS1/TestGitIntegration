# Fabric notebook source

# METADATA ********************

# META {
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "83a72ddb-6437-48bd-b44c-89907df3b078",
# META       "default_lakehouse_name": "LH1",
# META       "default_lakehouse_workspace_id": "cd0ebbc8-297f-4c4d-a4bd-b267e2a8403a",
# META       "known_lakehouses": [
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
