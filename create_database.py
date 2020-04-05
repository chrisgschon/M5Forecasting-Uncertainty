## Must have data available within a /data folder in this directory
## Also must have sqlite3 installed I guess
import sqlite3
conn = sqlite3.connect('bosh.db')
c = conn.cursor()

#Dirs
import os
cwd = os.getcwd()
datadir = cwd + '/data/raw/'

## Create tables
c.execute('''CREATE TABLE IF NOT EXISTS stores
            (store_id INTEGER PRIMARY KEY,
            store_name TEXT NOT NULL UNIQUE)''')
c.execute('''CREATE TABLE IF NOT EXISTS items
            (item_id INTEGER PRIMARY KEY,
            item_name TEXT NOT NULL UNIQUE)''')
c.execute('''CREATE TABLE IF NOT EXISTS weeks
            (week_id INTEGER PRIMARY KEY,
            week_name TEXT NOT NULL UNIQUE)''')
c.execute('''CREATE TABLE IF NOT EXISTS dates
            (date_id INTEGER PRIMARY KEY,
            date_name TEXT NOT NULL UNIQUE,
            weekday INTEGER NOT NULL,
            month INTEGER NOT NULL,
            year INTEGER NOT NULL,
            day_name TEXT NOT NULL UNIQUE,
            week_id INTEGER NOT NULL,
            snap_ca INTEGER NOT NULL DEFAULT 0,
            snap_tx INTEGER NOT NULL DEFAULT 0,
            snap_wi INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY (week_id) REFERENCES weeks(week_id)
            )''')
c.execute('''CREATE TABLE IF NOT EXISTS holidays
            (holiday_id INTEGER PRIMARY KEY,
            holiday_name TEXT NOT NULL UNIQUE,
            holiday_type TEXT)''')
c.execute('''CREATE TABLE IF NOT EXISTS holiday_dates
            (holiday_date_id INTEGER PRIMARY KEY,
            holiday_id INTEGER,
            date_id INTEGER,
            FOREIGN KEY (holiday_id)
            REFERENCES holidays (holiday_id),
            FOREIGN KEY (date_id)
            REFERENCES dates (date_id) )''')
c.execute('''CREATE TABLE IF NOT EXISTS sale_prices
            (sale_price_id INTEGER PRIMARY KEY,
            store_id INTEGER,
            item_id INTEGER,
            week_id INTEGER,
            sell_price REAL,
            FOREIGN KEY(store_id) REFERENCES stores(store_id),
            FOREIGN KEY(item_id) REFERENCES items(item_id),
            FOREIGN KEY(week_id) REFERENCES weeks(week_id)
            )''')
c.execute('''CREATE TABLE IF NOT EXISTS departments
            (department_id INTEGER PRIMARY KEY,
            department_name TEXT
            )''')
c.execute(''' CREATE TABLE IF NOT EXISTS categories
            (category_id INTEGER PRIMARY KEY,
            category_name TEXT
            )''')
c.execute('''CREATE TABLE IF NOT EXISTS states
            (state_id INTEGER PRIMARY KEY,
            state_name TEXT)''')
c.execute('''CREATE TABLE IF NOT EXISTS sales
            (sale_id INTEGER PRIMARY KEY,
            sale_name TEXT,
            item_id INTEGER,
            department_id INTEGER,
            category_id INTEGER,
            store_id INTEGER,
            state_id INTEGER,
            date_id INTEGER,
            sales_count INTEGER,
            FOREIGN KEY (department_id) REFERENCES departments(department_id),
            FOREIGN KEY (category_id) REFERENCES categories(category_id),
            FOREIGN KEY (store_id) REFERENCES stores(store_id),
            FOREIGN KEY (state_id) REFERENCES states(state_id),
            FOREIGN KEY (date_id) REFERENCES dates(date_id)
            )''')

def is_empty_table(cursor_object, table_name) :
    #cursor_object = conn.cursor()
    sql = ''' SELECT count(*) FROM ''' + table_name
    cursor_object.execute(sql)
    if cursor_object.fetchall()[0][0] == 0 :
        return True
    else :
        return False

## Check if we've already entered the sales
if(not is_empty_table(c, "sales")) :
    conn.close()
    print("Database Already Created")
    raise SystemExit(0)

import pandas as pd
## Fill Tables
sales_data = pd.read_csv(f"{datadir}/sell_prices.csv")
## Fill stores
stores = pd.Series(data = sales_data.store_id.unique(), name = 'store_name')
if(is_empty_table(c, "stores")) :
    stores.to_sql('stores', conn, if_exists = 'append', index_label = 'store_id')

## Fill items
items = pd.Series(data = sales_data.item_id.unique(), name = 'item_name')
if(is_empty_table(c, "items")) :
    items.to_sql('items', conn, if_exists = 'append', index_label = 'item_id')

## Fill weeks
calendar_data = pd.read_csv(f"{datadir}/calendar.csv")
weeks = pd.Series(data = calendar_data.wm_yr_wk.unique(), name = 'week_name')
if(is_empty_table(c, "weeks")) :
    weeks.to_sql('weeks', conn, if_exists = 'append', index_label = 'week_id')

## Fill dates
weeks = pd.DataFrame(data = {'week_id' : weeks.index, 'week_name' : weeks.values})
dates = pd.DataFrame(data = {
'date_name' : calendar_data.date,
'weekday' : calendar_data.wday,
'month' : calendar_data.month,
'year' : calendar_data.year,
'day_name' : calendar_data.d,
'week_name' : calendar_data.wm_yr_wk,
'snap_ca' : calendar_data.snap_CA,
'snap_tx' : calendar_data.snap_TX,
'snap_wi' : calendar_data.snap_WI
}).merge(weeks, how = 'left', on = 'week_name').drop(columns = ['week_name'])
if(is_empty_table(c, "dates")) :
    dates.to_sql('dates', conn, if_exists = 'append', index_label = 'date_id')

## Fill holidays
holidays = pd.DataFrame(data =
{'holiday_name' : calendar_data.event_name_1.append(calendar_data.event_name_2),
'holiday_type' : calendar_data.event_type_1.append(calendar_data.event_type_2)
}).dropna(axis=0, how='all').drop_duplicates().reset_index(drop = True)
if(is_empty_table(c, "holidays")) :
    holidays.to_sql('holidays', conn, if_exists = 'append', index_label = 'holiday_id')

## Fill holiday_dates
dates['date_id'] = dates.index
holidays['holiday_id'] = holidays.index
holiday_dates = pd.DataFrame(data =
{'holiday_name' : calendar_data.event_name_1.append(calendar_data.event_name_2),
'holiday_type' : calendar_data.event_type_1.append(calendar_data.event_type_2),
'day_name' : calendar_data.d.append(calendar_data.d)
}).dropna(axis=0, how='any').drop_duplicates().reset_index(drop = True)
holiday_dates = holiday_dates.merge(dates[['date_id','day_name']], how = 'inner', on='day_name').drop(columns=['day_name'])
holiday_dates = holiday_dates.merge(holidays, how='inner', on = ['holiday_name', 'holiday_type']).drop(columns=['holiday_name','holiday_type'])

if(is_empty_table(c, "holiday_dates")) :
    holiday_dates.to_sql('holiday_dates', conn, if_exists = 'append', index_label = 'holiday_date_id')

## Fill Sale Prices
items = pd.DataFrame(data = {'item_id' : items.index, 'item_name' : items.values})
stores = pd.DataFrame(data = {'store_id' : stores.index, 'store_name' : stores.values})
sales_prices = sales_data.merge(weeks, left_on = 'wm_yr_wk', right_on = 'week_name').drop(columns = ['week_name',"wm_yr_wk"])
sales_prices = sales_prices.merge(items, left_on = 'item_id', right_on = 'item_name').drop(columns = ['item_id_x', 'item_name'])
sales_prices = sales_prices.merge(stores, left_on = 'store_id', right_on = 'store_name').drop(columns = ['store_id_x', 'store_name'])
sales_prices = sales_prices.rename(columns = {'item_id_y' : 'item_id', 'store_id_y' : 'store_id'})

if(is_empty_table(c, "sale_prices")) :
    sales_prices.to_sql('sale_prices', conn, if_exists = 'append', index_label = 'sale_price_id')

## Fill Departments
sales_train_validation = pd.read_csv(f"{datadir}/sales_train_validation.csv")
departments = pd.Series(data = sales_train_validation.dept_id.unique(), name = 'department_name')

if(is_empty_table(c, "departments")) :
    departments.to_sql('departments', conn, if_exists = 'append', index_label = 'department_id')

## Fill categories
categories = pd.Series(data = sales_train_validation.cat_id.unique(), name = 'category_name')

if(is_empty_table(c, "categories")) :
    categories.to_sql('categories', conn, if_exists = 'append', index_label = 'category_id')

## Fill states
states = pd.Series(data = sales_train_validation.state_id.unique(), name = 'state_name')

if(is_empty_table(c, "states")) :
    states.to_sql('states', conn, if_exists = 'append', index_label = 'state_id')

## Fill sales
# sale_id, sale_name, item_id, deparment_id, category_id,
# store_id, state_id,date_id,sales_count
#items = pd.DataFrame(data = {'item_id' : items.index, 'item_name' : items.values})
departments = pd.DataFrame(data = {'department_id' : departments.index, 'department_name' : departments.values})
categories = pd.DataFrame(data = {'category_id' : categories.index, 'category_name' : categories.values})
date_lookup = pd.Series(data = dates.date_id.values, index = dates.day_name.values, name = 'date_id')
states = pd.DataFrame(data = {'state_id' : states.index, 'state_name' : states.values})

sales = (sales_train_validation[['id','item_id','dept_id','cat_id','store_id','state_id']].
rename(columns = {'id' : 'sale_name', 'item_id' : 'item_name',
'dept_id' : 'department_name', 'cat_id' : 'category_name',
'store_id' : 'store_name', 'state_id' : 'state_name'}).
merge(items, on = 'item_name', how = 'left').
merge(departments, on = 'department_name', how='left').
merge(categories, on = 'category_name', how='left').
merge(stores, on = 'store_name', how='left').
merge(states, on = 'state_name', how='left').
drop(columns = ['item_name','department_name','category_name','store_name', 'state_name']))
print(sales.head())
day_columns = sales_train_validation.columns.tolist()
#print(sales_train_validation.columns.tolist()[7])
for i in range(len(day_columns)) :
    if(i % 100 == 1) :
        print(i / len(day_columns) * 100)
    if(i < 6) :
        continue
    current_day = day_columns[i]
    sales_tmp = sales
    sales_tmp['date_id'] = [date_lookup.loc[current_day]]*sales.shape[0]
    sales_tmp['sales_count'] = sales_train_validation[current_day].tolist()
    sales_tmp.to_sql('sales', conn, if_exists = 'append', index = False)

c.execute("""
create index sales_item_id_store_id_date_id_index
	on sales (item_id, store_id, date_id);

create unique index sales_sale_id_uindex
	on sales (sale_id);"""
    )
print("HELLO BOSH")
conn.close()
