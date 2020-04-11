import os
import warnings
warnings.filterwarnings('ignore')
import pandas as pd
import numpy as np
import argparse
from fbprophet import Prophet
import sqlite3
from modules import EQModel, Series, wpl, quantiles_list

datadir = './data/raw'

##dev work: loader class from JP work
def load(item_id, store_id):
    conn = sqlite3.connect('bosh.db')
    c = conn.cursor()
    sales = pd.read_sql(f'select * from sales where item_id = {item_id} and store_id = {store_id}', con = conn)
    dates = pd.read_sql('select * from dates', con = conn).set_index('date_id')
    holidays = pd.read_sql('select * from holidays', con = conn)

    sales_df = sales.merge(dates['date_name'], left_on = 'date_id', right_index = True).reset_index()
    sales_df['date_name'] = sales_df['date_name'].map(lambda x: pd.to_datetime(x))
    sales_df['ds'] = sales_df['date_name']
    sales_df['y'] = sales_df['sales_count']
    sales_df['log1plusy'] = sales_df['y'].apply(lambda x: np.log(1+x))
    return(sales_df)

def fit(s):
    sales_df = s.df
    prophparams = {'weekly_seasonality':True, 'add_country_holidays': 'US'}
    model = s.fit_Prophet(ds = 'ds', y = 'sales_count', **prophparams)

    future = model.make_future_dataframe(periods=56)
    forecast = model.predict(future)
    pred_samples = s.predsamples_Prophet(model, forecast)
    pred_quantiles = s.predsamples_quantiles(pred_samples)

    eqmodel, eqmodel_fit,  = s.model_eq()
    eq_preds = eqmodel.predict_wide(s.tseries)

    return(model, forecast, pred_quantiles)

def evaluate(pred_quantiles, y):   
    qtidy = pred_quantiles.reset_index().melt(id_vars = ['index'],
                                        value_vars = [0.005, 0.025, 0.165, 0.25, 0.5, 0.75, 0.835, 0.975, 0.995],
                                        var_name='quantile', value_name = 'prediction')\
                                .merge(y, left_on = 'index', right_index = True)
    qtidy['prediction'] = qtidy['prediction'].map(lambda x: 0 if x < 0 else x)
    qtidy['wpl'] = qtidy.apply(lambda x: wpl(x['xt'], x['prediction'], x['quantile']), axis = 1)
    return(qtidy)

def main(args):
    item_id = args.item
    store_id = args.store
    sales_df = load(item_id, store_id)
    s = Series(sales_df, 'sales_count')
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

