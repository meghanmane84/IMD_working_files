import sqlite3
import pandas as pd
import datetime

#Reading Data given by IMD
con = sqlite3.connect('Station_43057.db')
with con:
    df = pd.read_sql('SELECT * FROM Data', con, parse_dates="DateTime", coerce_float=True, index_col="DateTime")
df

#Interpolating all missing data and discarding data older than 1995
df = df.loc['1989-01-01 01:00':'2018-01-01 00:00'].resample('H').mean().interpolate(method='linear', limit_direction='forward')
df = df['1995-01-01 01:00':'2018-01-01 00:00']
df

dataframe = df.copy()
datetime_df = df.index.copy()
datetime_df
for i in range(1, 336):
    dataframe.loc[:, f'Temp_{i}'] = dataframe.Temp.shift(-i)
    datetime_df = datetime_df.shift(1)[:-1]
dataframe.dropna(inplace=True)
dataframe
datetime_df
shift = 5 * 24
dataframe['Temp_Pred_True'] = dataframe.Temp_335.shift(-shift)
datetime_df = datetime_df.shift(shift)[:-shift]
dataframe.dropna(inplace=True)
dataframe
dataframe.index = datetime_df
dataframe
dataframe.loc[:'2016-12-31 23:00:00']['Temp_Pred_True']
df.loc['1995-01-20 00:00:00':'2016-12-31 23:00:00']['Temp']
dataframe.to_csv('processed_data_shift_5.csv')
datetime_df[0].to_pydatetime()