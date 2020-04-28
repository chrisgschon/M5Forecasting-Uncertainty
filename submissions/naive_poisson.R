rm(list=ls())
library(DBI)
library(RSQLite)
library(dplyr)
## 2nd Attempt
setwd("/Users/jpcryne/Documents/BoshCapital/Kaggle/M5Forecasting-Uncertainty")

## Get all ids of item/store level time series
drv <- dbDriver("SQLite")
con <- dbConnect(drv, dbname = "bosh.db", host = "localhost", user = "jpcryne")

all_ts_id <- dbGetQuery(con, "SELECT DISTINCT time_series_id FROM time_series")

dbDisconnect(con)


## I just need sale price joined to the time series, that way I can see when not on sale!
get_ts <- function(ts_id) {
  setwd("/Users/jpcryne/Documents/BoshCapital/Kaggle/M5Forecasting-Uncertainty")
  drv <- dbDriver("SQLite")
  con <- dbConnect(drv, dbname = "bosh.db", host = "localhost", user = "jpcryne")
  ts <- dbGetQuery(con, paste0("SELECT time_series_id, time_series.date_id, Y, dates.date_name, week_id FROM time_series
                               INNER JOIN dates ON dates.date_id = time_series.date_id
                               WHERE time_series.time_series_id =", ts_id))
  dbDisconnect(con)
  return(ts)
}

library(parallel)
cl <- makeCluster(3)
clusterExport(cl, c("get_ts"))
clusterEvalQ(cl, library("DBI"))
clusterEvalQ(cl, library("RSQLite"))
l <- parLapply(cl, all_ts_id$time_series_id, function(I) get_ts(I))
stopCluster(cl)

library(moments)
get_ts_data <- function(ts) {
  t = ts$Y 
  tnz = t[t!=0]
  normalised_t = (tnz - mean(tnz)) / sd(tnz)
  negbinmodel = glm.nb(Y ~ 1, data = ts)
  
  df <- data.frame(
    time_series_id = ts$time_series_id[1],
    mean_val = mean(t[t!=0]),
    median_val = median(t[t!=0]),
    max_val = max(t),
    prop_zero = length(which(t == 0)) / length(t),
    var = var(tnz),
    kurtosis = kurtosis(normalised_t),
    dispersion =  var(tnz) / mean(tnz),
    nb_theta = negbinmodel$theta,
    nb_mu = as.numeric(negbinmodel$coefficients[1]),
    still_on_sale = as.numeric(sum(t[(length(t)-28):length(t)]) != 0)
  )
  return(df)
}

# 7 mins
start <- Sys.time()
cl <- makeCluster(2)
clusterExport(cl, c("get_ts_data"))
clusterEvalQ(cl, library("moments"))
clusterEvalQ(cl, library("MASS"))
l_data = parLapply(cl, l, function(I) get_ts_data(I))
stopCluster(cl)
print(Sys.time() - start)

all_ts_data = do.call("rbind", l_data)

drv <- dbDriver("SQLite")
con <- dbConnect(drv, dbname = "bosh.db", host = "localhost", user = "jpcryne")

submission_info <- dbGetQuery(con, "SELECT time_series_id, row_id, interval, validation FROM submission_data LEFT JOIN ts_submission_map ON row_id = submission_row_id")
submission_sample <- read.csv("data/sample_submission.csv")

dbDisconnect(con)

# submission_sample[1,2:29] <- 1

names(l) <- sapply(l, function(I) I$time_series_id[1])

for(rowid in submission_info$row_id) {
  if(rowid %% 1000 == 0) print(rowid)
  if(submission_info$validation[rowid] == 0) next
  if(all_ts_data$still_on_sale[submission_info$time_series_id[rowid]] == 0) next
  
  lambda <- all_ts_data$mean_val[submission_info$time_series_id[rowid]]
  interval <- submission_info$interval[rowid]
  submission_sample$F1[rowid] <- qpois(interval, lambda)
}
tmp <- submission_sample

for(i in 2:28) {
  submission_sample[paste0("F",i)] <- submission_sample$F1
}
write.csv(submission_sample, "submissions/naive_poission.csv", row.names = F)

