# Fabric notebook source


# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
 

# CELL ********************

import pandas as pd
# Load data into pandas DataFrame from f"{mssparkutils.nbResPath}/builtin/1.5million_records_small.csv"
df = pd.read_csv(f"{mssparkutils.nbResPath}/builtin/1.5million_records_small.csv")
display(df.head(10))

