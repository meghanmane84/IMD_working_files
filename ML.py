%matplotlib inline
import pandas as pd
import numpy as np
from lightgbm import LGBMRegressor
import matplotlib.pyplot as plt

df = pd.read_csv('processed_data_shift_5.csv', index_col='DateTime')
df

train_df = df.loc[:'2016-12-31 23:00:00']
test_df = df.loc['2017-01-01 00:00:00':]
train_df.shape[0]+test_df.shape[0]

def eval_and_save(train_df, test_df, month):
    
    # Model creation
    X_train = train_df.values[:, :-1]
    y_train = train_df.values[:, -1]
    
    gbm = LGBMRegressor(n_estimators=1000, random_state=1, n_jobs=100)
    gbm.fit(X_train, y_train)
    
    # Output from model
    X = test_df.values[:, :-1]
    y = test_df.values[:, -1]
    
    pred = gbm.predict(X[:120])
    
    # Saving graphs and dataset
    df2 = test_df[:120].copy()
    dates = df2.index
    df2.insert(337, 'Predicted', pred)
    df2.to_csv(f'{os.getcwd()}\\pilot outputs\\{month}\\{dates[0][:10]}.csv')
    
    for i in range(5):
        x = dates[24*i:24*(i+1)]
        Y_1 = y[24*i:24*(i+1)]
        Y_2 = pred[24*i:24*(i+1)]
        plt.plot(x,Y_1, color='b', label='True')
        plt.plot(x,Y_2, color='y', label='Predicted')
        plt.legend()
        plt.savefig(f'{os.getcwd()}\\pilot outputs\\{month}\\{dates[i*24][:10]}.png')
        plt.clf()

    # Plot 5 day graph
    plot_series(
        pd.DataFrame(y[:120])[0],
        pd.DataFrame(pred)[0],
        labels=['true', 'predicted'], 
        markers=['.', '_']
    )

    eval_and_save(df.loc[:'2016-12-31 23:00:00'], test_df.loc['2017-01-01 00:00:00' : '2017-01-14 23:00:00'], 'January')