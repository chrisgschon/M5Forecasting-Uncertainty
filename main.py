import os
import warnings
warnings.filterwarnings('ignore')
import pandas as pd
import numpy as np
import argparse
from fbprophet import Prophet
import sqlite3
from modules import DataLoader, EQModel, Series, wpl, quantiles_list

datadir = './data/raw'


## define how you want to fit the model
def fit(s, **params):
    
    prophparams = {'weekly_seasonality':True, 'add_country_holidays': 'US'}
    model = s.fit_Prophet(ds = 'ds', y = 'sales_count', **prophparams)
    future = model.make_future_dataframe(periods=56)
    forecast = model.predict(future)
    pred_samples = s.predsamples_Prophet(model, forecast)
    pred_quantiles = s.predsamples_quantiles(pred_samples)

    return(model, forecast, pred_quantiles)

## define evaluation
def evaluate(pred_quantiles, y):   
    qtidy = pred_quantiles.reset_index().melt(id_vars = ['index'],
                                        value_vars = [0.005, 0.025, 0.165, 0.25, 0.5, 0.75, 0.835, 0.975, 0.995],
                                        var_name='quantile', value_name = 'prediction')\
                                .merge(y, left_on = 'index', right_index = True)
    qtidy['prediction'] = qtidy['prediction'].map(lambda x: 0 if x < 0 else x)
    qtidy['wpl'] = qtidy.apply(lambda x: wpl(x['xt'], x['prediction'], x['quantile']), axis = 1)
    return(qtidy)

## main pipe
def main(args):
    #load data + lookups
    d = DataLoader()
    d.load_lookups()
    item = args.item
    store = args.store
    sales_df = d.load_sales_df({'filters':{'item_id':item, 'store_id':store}})
    s = Series(sales_df, 'sales_count')

    #empirical quantile model
    eqmodel, eqmodel_fit,  = s.model_eq()
    eq_preds = eqmodel.predict_wide(s.tseries)

    #Prophet model
    model, forecast, pred_quantiles = fit(s)
    qtidy = evaluate(pred_quantiles, s.tseries)
    score = qtidy['wpl'].mean()
    print(f'Model WPL: {score}')

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--item', type = int)
    parser.add_argument('--store', type = int)
    args = parser.parse_args()
    main(args)

