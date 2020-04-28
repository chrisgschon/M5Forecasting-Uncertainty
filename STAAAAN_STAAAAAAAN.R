# Please run create_database.py AND THEN submission_style_sales_data.R
library(DBI)
library(RSQLite)
library(lubridate)
library(rstan)
## Change your working directory in here please
setwd("/Users/jpcryne/Documents/BoshCapital/Kaggle/M5Forecasting-Uncertainty")

get_ts <- function(ts_id) {
  drv <- dbDriver("SQLite")
  con <- dbConnect(drv, dbname = "bosh.db", host = "localhost", user = "jpcryne")
  ts <- dbGetQuery(con, paste0("SELECT time_series_id, time_series.date_id, Y, dates.date_name, week_id FROM time_series
                               INNER JOIN dates ON dates.date_id = time_series.date_id
                               WHERE time_series.time_series_id =", ts_id))
  dbDisconnect(con)
  return(ts)
}

runQuery <- function(query) {
  drv <- dbDriver("SQLite")
  con <- dbConnect(drv, dbname = "bosh.db", host = "localhost", user = "jpcryne")
  result <- dbGetQuery(con, query)
  dbDisconnect(con)
  return(result)
}

## Generates out fourier terms, no colnames, that okay??
fourier_series <- function(dates, period, series.order) {
  # t <- time_diff(dates, set_date('1970-01-01 00:00:00'))
  t <- as.numeric(difftime(dates, as.POSIXct('1970-01-01 00:00:00', format = "%Y-%m-%d %H:%M:%S", tz = "GMT"), units = "days"))
  features <- matrix(0, length(t), 2 * series.order)
  for (i in 1:series.order) {
    x <- as.numeric(2 * i * pi * t / period)
    features[, i * 2 - 1] <- sin(x)
    features[, i * 2] <- cos(x)
  }
  return(features)
}
## Retrieve all dates in data in order
get_all_dates <- function() {
  drv <- dbDriver("SQLite")
  con <- dbConnect(drv, dbname = "bosh.db", host = "localhost", user = "jpcryne")
  all_dates <- dbGetQuery(con, "SELECT date_id, date_name FROM dates ORDER BY date_id")
  dbDisconnect(con)
  return(all_dates)
}

## Get ids for time series
get_all_ts_id <- function() {
  drv <- dbDriver("SQLite")
  con <- dbConnect(drv, dbname = "bosh.db", host = "localhost", user = "jpcryne")
  all_ts_id <- dbGetQuery(con, "SELECT DISTINCT time_series_id FROM time_series ORDER BY time_series_id")
  dbDisconnect(con)
  return(all_ts_id)
}

## Get holiday indicators for all holidays in range
initialise_holidays <- function() {
  # setwd("/Users/jpcryne/Documents/BoshCapital/Kaggle/M5Forecasting-Uncertainty")
  drv <- dbDriver("SQLite")
  con <- dbConnect(drv, dbname = "bosh.db", host = "localhost", user = "jpcryne")
  holidays <- dbGetQuery(con, paste0("SELECT holiday_dates.date_id, date_name, holiday_name FROM holiday_dates 
                                INNER JOIN dates ON holiday_dates.date_id = dates.date_id
                                INNER JOIN holidays ON holiday_dates.holiday_id = holidays.holiday_id"))
  all_dates <- dbGetQuery(con, "SELECT date_name FROM dates ORDER BY date_id")
  dbDisconnect(con)
  
  ## Any edits to holiday will go here, prior days, delete hols etc
  {
    
  }
  
  ## Return indicator for when the specified holiday is
  holiday_dates <- function(holiday_name) {
    h_dates = holidays$date_name[holidays$holiday_name == holiday_name]
    data    = as.numeric(all_dates$date_name %in% h_dates)
    m       = matrix(data, nrow = length(data), ncol = 1)
    
    colnames(m) <- holiday_name
    return(m)
  }
  l = lapply(unique(holidays$holiday_name), function(I) holiday_dates(I))
  holiday_regressors <- do.call("cbind", l)
  
  return(list(holiday_regressors = holiday_regressors,
              date_range = all_dates$date_name))
}

# holidays <- initialise_holidays()

## Generate holidays regressors, only use dates in time series
holiday_regressors <- function(dates) {
  return(holidays$holiday_regressors[holidays$date_range %in% dates,])
}

## Setup dataframe for prediction, if not validation then evaluation
make_future_df <- function(validation = T) {
  start_date <- 1913
  if(!validation) start_date_id <- 1941
  
  drv <- dbDriver("SQLite")
  con <- dbConnect(drv, dbname = "bosh.db", host = "localhost", user = "jpcryne")
  dates <- dbGetQuery(con, paste0("SELECT date_id, date_name FROM dates WHERE date_id >= ",start_date, " AND date_id < ",start_date+28," ORDER BY date_id"))
  dbDisconnect(con)
  return(dates)
}

setup_variables <- function(t_s) {
  nt       <- length(t_s$date_name)
  X        <- all_X[all_dates$date_id %in% t_s$date_id ,]
  K        <- ncol(X)
  y        <- t_s$y_scaled
  S        <- 2
  t_change <- c(0, floor(nt/2))
  t        <- 0:(length(t_s$date_name)-1)
  sigmas   <- rep(10, K)
  tau      <- 0.05
  s_a      <- rep(1, K)
  s_m      <- rep(0, K)
  
  data <- list(nt = nt, 
               X = X,
               K=K,
               y=y,
               S=S,
               t_change = t_change,
               t = t,
               sigmas = sigmas,
               tau=tau,
               s_a = s_a,
               s_m = s_m)
  return(data)
}

linear_growth_init <- function(df) {
  i0 <- which.min(ymd(df$date_name))
  i1 <- which.max(ymd(df$date_name))
  nt <- df$date_id[i1] - df$date_id[i0]
  # Initialize the rate
  k <- (df$y_scaled[i1] - df$y_scaled[i0]) / nt
  # And the offset
  m <- df$y_scaled[i0] - k * df$date_id[i0]
  return(c(k, m))
}

# kinit <- linear_growth_init(t_s)
stan_init <- function() {
  list(k = kinit[1],
       m = kinit[2],
       delta = array(rep(0, length(t_change))),
       beta = array(rep(0,ncol(X))),
       sigma_obs = 1
  )
}
stan_init_alternate <- function(kinit, data) {
  list(k = kinit[1],
       m = kinit[2],
       delta = array(rep(0, length(data$t_change))),
       beta = array(rep(0, data$K)),#ncol(X))),
       sigma_obs = 1
  )
}

## Prediction functions
{
  ## Taken from stan file
  linear_trend_r <- function() {
    return(((k + A * delta) %*% t) + (m + A * (-t_change %*% delta)))
  }
  ## Taken from prophet
  piecewise_linear <- function(t) {
    # Intercept changes
    gammas <- -t_change * delta
    # Get cumulative slope and intercept at each t
    k_t <- rep(k, length(t))
    m_t <- rep(m, length(t))
    for (s in 1:length(t_change)) {
      indx <- t >= t_change[s]
      k_t[indx] <- k_t[indx] + delta[s]
      m_t[indx] <- m_t[indx] + gammas[s]
    }
    y <- k_t * t + m_t
    return(y)
  }
  
  piecewise_linear_alternate <- function(t, params) {
    # Intercept changes
    gammas <- -params$t_change * params$delta
    # Get cumulative slope and intercept at each t
    k_t <- rep(params$k, length(t))
    m_t <- rep(params$m, length(t))
    for (s in 1:length(params$t_change)) {
      indx <- t >= params$t_change[s]
      k_t[indx] <- k_t[indx] + params$delta[s]
      m_t[indx] <- m_t[indx] + gammas[s]
    }
    y <- k_t * t + m_t
    return(y)
  }
  
  ## Taken from Prophet
  predict_trend <- function(df) {
    #k       <- k
    param.m <- m
    deltas  <- delta
    
    t       <- df$date_id
    trend   <- piecewise_linear(t)
    
    # return(trend * model$y.scale + df$floor)
    return(trend)
  }
  
  predict_trend_alternate <- function(df, params) {
    #k       <- k
    param.m <- params$m
    deltas  <- params$delta
    
    t       <- df$date_id
    trend   <- piecewise_linear_alternate(t, params)
    
    ## Does line below neew to return?? I think not because scale back at end
    # return(trend * model$y.scale + df$floor)
    return(trend)
  }
  
  ## Adapted from prophet
  ## Each row consists of posterior samples of that day
  posterior <-   function(future_df, n.obs = 100) {
    
    new_X <- all_X[all_dates$date_id %in% future_df$date_id, ]
    means <- predict_trend(future_df) + new_X %*% beta
    
    samples <- rnorm(n.obs*length(means),
                     mean = means,
                     sigma_obs)
    return(matrix(data = y_scale*samples, ncol = n.obs))
  }
  
  ## Predict the dates in future_df
  future_predictions <- function(future_df) {
    new_X <- all_X[all_dates$date_id %in% future_df$date_id, ]
    predictions <- predict_trend(future_df) + new_X %*% beta
    return(y_scale*predictions)
  }
  
  ## Get the quantiles for submission
  future_quantiles <- function(future_df) {
    quantiles <- c(0.005, 0.025, 0.165, 0.25, 0.5, 0.75, 0.835, 0.975, 0.995)
    new_X <- all_X[all_dates$date_id %in% future_df$date_id, ]
    means <- predict_trend(future_df) + new_X %*% beta
    
    output <- qnorm(p = quantiles, mean = rep(means, each = length(quantiles)), sd = sigma_obs)
    return(y_scale*matrix(data = output, nrow = length(quantiles),dimnames = list(quantiles, NULL)))
  }
  
  future_quantiles_alternate <- function(future_df, params) {
    quantiles <- c(0.005, 0.025, 0.165, 0.25, 0.5, 0.75, 0.835, 0.975, 0.995)
    new_X <- params$all_X[params$all_dates$date_id %in% future_df$date_id, ]
    means <- predict_trend_alternate(future_df, params) + new_X %*% params$beta
    
    output <- qnorm(p = quantiles, mean = rep(means, each = length(quantiles)), sd = params$sigma_obs)
    return(params$y_scale*matrix(data = output, nrow = length(quantiles),dimnames = list(quantiles, NULL)))
  }
}

###### Setup all regressors ######
print("Setting up regressors")
ts_ids <- get_all_ts_id()                         # Get ids
future_template <- make_future_df(validation = T) # Setup template for future
holidays <- initialise_holidays()                 # Holiday regressors

all_dates <- get_all_dates()                      # Fourier regressors
all_X <- cbind(fourier_series(all_dates$date_name, period = 365.25, 10),
               fourier_series(all_dates$date_name, period = 7, 4),
               holiday_regressors(all_dates$date_name))

print("Compiling Stan model")
model <- stan_model(file="first_model.stan")      # Compile the Stan model

## Pass ts_id, model, regressors and future df
run_all <- function(ts_id, model, all_X, future_template, all_dates) {
  ## Get time series and scale it, sometimes doesn't work if no scale!
  t_s <- get_ts(ts_id)
  y_scale = max(t_s$Y)
  t_s$y_scaled <- t_s$Y / y_scale
  
  data <- setup_variables(t_s)
  kinit <- linear_growth_init(t_s)
  ## Run Stan model
  point_estimates <- tryCatch(optimizing(model,
                                         data = data,
                                         init=stan_init_alternate(kinit, data),
                                         iter = 1e4,
                                         as_vector = FALSE,
                                         verbose = F),
                              error = function(err) {
                                return(NULL)
                              })
  if(is.null(point_estimates)) {
    return(NULL)
  }
  ## Extract estimates
  {
    k         <- point_estimates$par$k
    m         <- point_estimates$par$m
    delta     <- point_estimates$par$delta
    sigma_obs <- point_estimates$par$sigma_obs
    beta      <- point_estimates$par$beta
    A         <- NULL
  }
  
  params <- list(
    k         = point_estimates$par$k,
    m         = point_estimates$par$m,
    delta     = point_estimates$par$delta,
    sigma_obs = point_estimates$par$sigma_obs,
    beta      = point_estimates$par$beta,
    A         = NULL,
    all_dates = all_dates,
    all_X     = all_X,
    t_change  = data$t_change,
    y_scale   = y_scale
  )
  
  future <- future_template
  quants <- future_quantiles_alternate(future, params)
  return(quants)
}

## Run Stan model for all time series
print("Running Stan model, takes just under 2 hours with 2 cores")
library(parallel)
start <- Sys.time()
cl <- makeCluster(detectCores()-2)
clusterEvalQ(cl, {library(DBI); library(RSQLite); library(rstan); library(lubridate);})
clusterExport(cl, varlist = ls())
l <- parLapply(cl, ts_ids$time_series_id, function(I) run_all(I, all_X = all_X, model = model, future_template = future_template, all_dates = all_dates))
stopCluster(cl)
print(Sys.time()-start)

saveRDS(l, "all_quantiles.rds")

print("Dealing with any time series that failed")
## DEAL WITH ANYTHING THAT FAILED, FIT NAIVE NEG BETA
failedIndices <- which(sapply(l, is.null))
quantiles <- c(0.005, 0.025, 0.165, 0.25, 0.5, 0.75, 0.835, 0.975, 0.995)
library("MASS")
# Plot the ones that failed
for(t_id in failedIndices) {
  t_s <- get_ts(t_id)
  plot(t_s$Y / max(t_s$Y), type = 'l')
  # fit a neg beta model
  negbinmodel = glm.nb(Y ~ 1, data = tail(t_s,28))
  nb_theta = negbinmodel$theta
  nb_mu = as.numeric(negbinmodel$coefficients[1])
  # output <- qnbinom(interval, mu = mu, size = theta)
  output <- qnbinom(p = quantiles, mu = rep(exp(nb_mu), each = length(quantiles)), size = rep(nb_theta, each = length(quantiles)))
  output <- matrix(data = output, nrow = length(quantiles),dimnames = list(quantiles, NULL))
  output <- matrix(data = output , length(output) , 28 , dimnames = list(quantiles, NULL))
  l[[t_id]] <- output
}

## Organise the submission, need to map (ts_id, interval) -> (row_id)
submission_template <- read.csv("data/sample_submission.csv")
row_map <- runQuery("SELECT row_id, time_series_id, interval FROM submission_data INNER JOIN ts_submission_map ON ts_submission_map.submission_row_id = submission_data.row_id")
all_quantiles <- do.call("rbind", l)
all_quantiles <- cbind(
  rep(ts_ids$time_series_id, each = length(quantiles)),
  as.numeric(row.names(all_quantiles)),
  all_quantiles)
# aq_tmp <- all_quantiles
all_quantiles <- as.data.frame(all_quantiles, stringsAsFactors = F)
colnames(all_quantiles) <- c("time_series_id", "interval", paste0("F", c(1:28)))
h <- head(all_quantiles)

all_quantiles <- row_map %>% 
  dplyr::left_join(all_quantiles, by = c("time_series_id", "interval")) %>%
  dplyr::arrange(row_id)
all_quantiles$id <- submission_template$id
all_quantiles <- all_quantiles[c("id", colnames(all_quantiles)[4:31])]

write.csv(all_quantiles, "submissions/stan_normal_model.csv", row.names = F)

## Need to have validation and evaluation rows created somehow
