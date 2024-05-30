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
# META         },
# META         {
# META           "id": "950a1cb8-05a1-4061-8808-53327ff7e6b8"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
 

# CELL ********************

df = spark.read.format("csv").option("header","true").load("abfss://JerryTest@dxt-onelake.dfs.fabric.microsoft.com/LH1.Lakehouse/Files/1.5million_records_small_SPO.csv")
# df now is a Spark DataFrame containing CSV data from "abfss://JerryTest@dxt-onelake.dfs.fabric.microsoft.com/LH1.Lakehouse/Files/1.5million_records_small_SPO.csv".
display(df.head(5))

# CELL ********************

df = spark.sql("SELECT * FROM LH3.dbo.publicholidays LIMIT 10")
display(df.head(2))
