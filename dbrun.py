import pandas as pd
import sqlite3
con = sqlite3.connect('Station_43057.db')
with con:
    df = pd.read_sql('SELECT * FROM Data', con, parse_dates="DateTime", coerce_float=True, index_col="DateTime")
df