## Need database producted by create_database.py
require(DBI, quietly = T, warn.conflicts = F)
require(RSQLite, quietly = T, warn.conflicts = F)
require(prophet, quietly = T, warn.conflicts = F)

drv <- dbDriver("SQLite")
con <- dbConnect(drv, dbname = "bosh.db", host = "localhost", user = "jpcryne")
all_sales <- dbGetQuery(con, "SELECT date_name as ds, sum(sales_count) as y FROM sales INNER JOIN dates ON dates.date_id = sales.date_id GROUP BY dates.date_id")
holidays_df <- dbGetQuery(con, "SELECT date_name as ds, holiday_name as holiday, -2 as lower_window, 2 as upper_window FROM holiday_dates INNER JOIN holidays ON holiday_dates.holiday_id = holidays.holiday_id INNER JOIN dates ON dates.date_id = holiday_dates.date_id ORDER BY date_name")
dbDisconnect(con)

m = prophet(all_sales, holidays = holidays_df)
future = make_future_dataframe(m, periods = 28)
forecast <- predict(m, future)
plot(m, forecast)
