import numpy as np
import pandas as pd
from tqdm import tqdm
import sqlite3
from statsmodels.tsa.arima_model import ARIMA
import matplotlib.pyplot as plt
import seaborn as sns
from fbprophet import Prophet


quantiles_list = [0.005, 0.025, 0.165, 0.25, 0.5, 0.75, 0.835, 0.975, 0.995]

sales_cols = ['sale_id', 'sale_name', 'item_id', 'department_id',
              'category_id', 'store_id', 'state_id', 'date_id']

def wpl(y_true, y_pred, tau):
    err = y_true - y_pred
    return np.mean(np.max((tau * err, (tau-1) * err), axis = 0))

class DataLoader:
    def __init__(self):
        self.con = sqlite3.connect('bosh.db')
        self.c = self.con.cursor()
        
    def load_lookups(self):
        global dates
        dates = pd.read_sql('select * from dates', con = self.con).set_index('date_id')
        global holidays
        holidays = pd.read_sql('select * from holidays', con = self.con)
        
    def load_sales_df(self,conditions):
        cols = conditions.get('filters') #dict
        groups = conditions.get('groups') #list
        extra_cols = ['ds', 'date_name']
        filter_me = "where " + " and ".join([f"{k} == {c}" for k,c in cols.items()]) if cols else ""
        sql = f"select * from sales {filter_me}"
        sales_df = pd.read_sql(sql, con = self.con)
        prices_sql = f"select * from sale_prices {filter_me}"
        prices = pd.read_sql(prices_sql, con = self.con)
        sales_df = sales_df.merge(dates[['date_name', 'week_id']], left_on = 'date_id', right_index = True).reset_index()
        sales_df = sales_df.merge(prices[['item_id', 'store_id', 'week_id','sell_price']], left_on = ['item_id', 'store_id', 'week_id'], right_on = ['item_id', 'store_id', 'week_id']).reset_index()
        sales_df['date_name'] = sales_df['date_name'].map(lambda x: pd.to_datetime(x))
        sales_df['ds'] = sales_df['date_name']
        sales_df['log1plusy'] = sales_df['sales_count'].apply(lambda x: np.log(1+x))
        if groups:
            out_cols = extra_cols + ['sales_count', 'log1plusy']
            sales_df = sales_df.groupby(extra_cols + groups).sum().reset_index()[out_cols]
        return(sales_df)
                
    def do_grouping(self, *groups):
        non_group_cols = [c for c in sales_cols if c not in groups]
        group_me = ",".join(groups)
        select_me = group_me + ", sum(sales_count) as sales_count"
        filter_me = "where" + " and ".join([f"{k} == {c}" for k,c in t.items()])
        sql = f"select {select_me} from sales group by {group_me}"
        grouped_df = pd.read_sql(sql, con = self.con)
        return(grouped_df)

class EQModel:
    def __init__(self, x):
        self.x = x
    def fit(self, quantiles_list):
        self.qfit = self.x.quantile(quantiles_list)
        return self.qfit
    def predict(self, q):
        return self.qfit[q]
    def predict_wide(self, q):
        tbl = pd.DataFrame(self.qfit)
        for i in self.x.index:
            tbl[i] = tbl[self.x.name]
        tbl.drop(self.x.name, axis = 1, inplace = True)
        return(tbl.T)
        

class Series:
    def __init__(self, df, valcol):
        self.df = df
        self.columns = list(df.columns)
        self.valcol = valcol
        self.tseries = pd.Series(df[valcol]).rename('xt')
        self.tseries_normed = (df[valcol] - df[valcol].mean())/df[valcol].std()
        self.logtseries = np.log(1+df[valcol])
        self.length = len(df[valcol])
        self.description = df[valcol].describe()
        
    def plot_series(self, idx_range, **subplkwargs):
        plot_range = idx_range
        fig, ax = plt.subplots(**subplkwargs)
        ax.plot(tseries[plot_range])
                
    def plot_forecast(self, history, forecast, forecast_conf_ints, back = 28, forward = 28, **subplkwargs):
        maxidx = history.index[-1]
        forecast.index = pd.RangeIndex(maxidx + 1, maxidx + 1 + forward)
        plot_df = pd.DataFrame(history[-back:].append(forecast)).rename({0:'Sales'}, axis = 1)
        plot_df.loc[forecast.index, 'Type'] = 'Forecast'
        plot_df['Type'].fillna('Historic', inplace = True)
        plot_df['forecast_conf_int_lower'], plot_df['forecast_conf_int_upper'] = plot_df['Sales'], plot_df['Sales']
        plot_df.loc[forecast.index, 'forecast_conf_int_lower'] = forecast_conf_int['conf_int_lower'].values
        plot_df.loc[forecast.index, 'forecast_conf_int_upper'] = forecast_conf_int['conf_int_upper'].values
        fig, ax = plt.subplots(**subplkwargs)
        ax.plot(plot_df[plot_df['Type'] == 'Historic']['Sales'], color = 'black')
        ax.plot(plot_df[plot_df['Type'] == 'Forecast']['Sales'], color = 'green')
        ax.fill_between(plot_df.index, plot_df['forecast_conf_int_lower'], plot_df['forecast_conf_int_upper'], alpha=0.5, edgecolor='#CC4F1B', facecolor='#FF9848')
        
    def plot_acf(self, lags = [0,365], **subplkwargs):
        fig, ax = plt.subplots(**subplkwargs)
        pd.plotting.autocorrelation_plot(self.tseries, ax = ax)
        ax.set_xlim(lags)
        
    def model_eq(self):
        model = EQModel(self.tseries)
        model_fit = model.fit(quantiles_list)
        return(model, model_fit)
    
    def model_arima(self, data, p=7, d=1, q=0):
        model = ARIMA(data, order=(p,d,q))
        model_fit = model.fit(disp=0)
        return(model, model_fit)
    
    def model_forecast(self, model_fit, forward = 28):
        forecast, forecast_stderr, forecast_conf_int = model_fit.forecast(forward)
        forecast = pd.Series(forecast)
        forecast_stderr = pd.Series(forecast_stderr)
        forecast_conf_int = pd.DataFrame(forecast_conf_int).rename({0:'conf_int_lower', 1:'conf_int_upper'}, axis = 1)
        maxidx = self.tseries.index[-1]
        forecast.index = pd.RangeIndex(maxidx + 1, maxidx + 1 + forward)
        self.forecast = forecast
        self.forecast_stderr = forecast_stderr
        self.forecast_conf_int = forecast_conf_int
        return(forecast, forecast_stderr, forecast_conf_int)
    
    def fit_Prophet(self, ds, y, **params):
        country_hols = params.pop('add_country_holidays')
        regressors = params.pop('regressors') if 'regressors' in params else None
        model = Prophet(**params)
        self.df['y'] = self.df[y]
        self.df['ds'] = self.df[ds]
        if country_hols:
            model.add_country_holidays(country_name=country_hols)
        if regressors:
            for r in regressors:
                model.add_regressor(r)
        model.fit(self.df)
        return(model)
    
    def forecast_Prophet(self, model, periods):
        future = model.make_future_dataframe(periods=periods)
        forecast = model.predict(future)
        return(forecast)
    
    def predsamples_Prophet(self, model, forecast):
        ps = model.predictive_samples(forecast)
        return(ps)

    def predsamples_quantiles(self, samples, quantiles = [0.005, 0.025, 0.165, 0.25, 0.5, 0.75, 0.835, 0.975, 0.995]):
        #expects samples of form given by predsamples_Prophet method
        yhat_samples = samples['yhat']
        yhat_samples_df = pd.DataFrame(yhat_samples, index = np.arange(yhat_samples.shape[0]))
        yhat_samples_qs = yhat_samples_df.quantile(quantiles, axis = 1).T
        return(yhat_samples_qs)
    