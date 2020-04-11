rm(list=ls())
library(DBI)
library(RSQLite)
library(dplyr)
## 2nd Attempt
setwd("~/git/M5Forecasting-Uncertainty")
drv <- dbDriver("SQLite")
con <- dbConnect(drv, dbname = "bosh.db", host = "localhost", user = "jpcryne")

items <- dbGetQuery(con, "SELECT * FROM items")
categories <- dbGetQuery(con, "SELECT * FROM categories")
departments <- dbGetQuery(con, "SELECT * FROM departments")
states <- dbGetQuery(con, "SELECT * FROM states")
stores <- dbGetQuery(con, "SELECT * FROM stores")

dbDisconnect(con)

submission_sample <- read.csv("data/raw/sample_submission.csv")
row_types <- data.frame(full_name = as.character(submission_sample$id), stringsAsFactors = F) %>%
  mutate(row_id = row_number())
row_types$partial_name <- sapply(row_types$full_name, function(I) strsplit(I, "_[0].[0-9]+_(evaluation|validation)")[[1]][1])
split_names <- sapply(row_types$full_name, function(I) strsplit(I, "_"))
row_types$validation <- sapply(split_names, function(I) as.numeric(I[[length(I)]] == "validation"))
row_types$interval <- sapply(split_names, function(I) as.numeric(I[[length(I)-1]]))
row_types$first_id <- as.character(sapply(row_types$full_name, function(I) strsplit(I, "(_[A-Z]+)")[[1]][1]))
second_id_lists <- strsplit(row_types$partial_name, paste0(row_types$first_id, "_"))
row_types$second_id <- sapply(second_id_lists, function(I) I[length(I)])

row_types_final <- row_types %>%
  left_join(items, by = c("first_id"="item_name")) %>%
  left_join(items, by = c("second_id"="item_name")) %>%
  mutate(item_id = coalesce(item_id.x, item_id.y)) %>%
  select(-item_id.x, -item_id.y) %>%
  left_join(categories, by = c("first_id"="category_name")) %>%
  left_join(categories, by = c("second_id"="category_name")) %>%
  mutate(category_id = coalesce(category_id.x, category_id.y)) %>%
  select(-category_id.x, -category_id.y) %>%
  left_join(departments, by = c("first_id"="department_name")) %>%
  left_join(departments, by = c("second_id"="department_name")) %>%
  mutate(department_id = coalesce(department_id.x, department_id.y)) %>%
  select(-department_id.x, -department_id.y) %>%
  left_join(states, by = c("first_id"="state_name")) %>%
  left_join(states, by = c("second_id"="state_name")) %>%
  mutate(state_id = coalesce(state_id.x, state_id.y)) %>%
  select(-state_id.x, -state_id.y) %>%
  left_join(stores, by = c("first_id"="store_name")) %>%
  left_join(stores, by = c("second_id"="store_name")) %>%
  mutate(store_id = coalesce(store_id.x, store_id.y)) %>%
  select(-store_id.x, -store_id.y) %>%
  select(full_name, row_id, item_id, category_id, department_id, state_id, store_id, validation, interval)


drv <- dbDriver("SQLite")
con <- dbConnect(drv, dbname = "bosh.db", host = "localhost", user = "jpcryne")

num_dates <- as.numeric(dbGetQuery(con, "SELECT COUNT(DISTINCT date_id) FROM sales"))
all_sales <- dbGetQuery(con, "SELECT * FROM sales")

dbDisconnect(con)

library(Matrix)
#submission_format_data = Matrix(data = 0, ncol = num_dates, nrow = nrow(submission_sample), sparse = T)
#submission_format_data = Matrix(data = 0, nrow = num_dates, ncol = nrow(submission_sample), sparse = T)
duplicated_rows <- duplicated(row_types_final %>% select(-full_name, -row_id, -interval,-validation))

library(data.table)
DT <- as.data.table(all_sales)

## 10x speedup if we set key just for this
# setkey(DT, item_id)
# start <- Sys.time()
# tmp <- DT[item_id == 0]#[.(date_id, sales_count)]
# #tmp <- tmp[,.(date_id, sales_count)]
# print(Sys.time() - start)

row_types_for_calculation <- row_types_final[!duplicated_rows, ]
# nothing (so all)
# item_id
# item_id, state_id
# item_id, store_id
# category_id
# category_id, state_id
# category_id, store_id
# department_id
# department_id, state_id
# department_id, store_id
# state_id
# store_id
#tmp <- row_types_for_calculation[1,]
#all(is.na(tmp[c("category_id","department_id","state_id","store_id")]))
#as.numeric(which(apply(row_types_for_calculation, MARGIN = 1, function(I) all(is.na(I[c("category_id","department_id","state_id","store_id")])))))

## These lists will help with the grouping
index_group_names <- c("none_not_na","i_not_na","i_sta_not_na",
                       "i_sto_not_na_1","i_sto_not_na_2",
                       "cat_not_na","cat_sta_not_na","cat_sto_not_na","dep_not_na","dep_sta_not_na","dep_sto_not_na",
                       "sta_not_na","sto_not_na" )
## will use eval(parse(text="setkey...")) to set the key and improve speed
index_key_set <- list(
  "none_not_na"= "NULL", "i_not_na" = "setkey(DT, item_id)",
  "i_sta_not_na" = "setkey(DT, item_id, state_id)",
  "i_sto_not_na_1"="setkey(DT,item_id,store_id)","i_sto_not_na_2"="setkey(DT,item_id,store_id)",
  "cat_not_na"="setkey(DT,category_id)","cat_sta_not_na"="setkey(DT,category_id,state_id)",
  "cat_sto_not_na"="setkey(DT, category_id, store_id)","dep_not_na"="setkey(DT, department_id)",
  "dep_sta_not_na"="setkey(DT,department_id,state_id)","dep_sto_not_na"="setkey(DT,department_id, store_id)",
  "sta_not_na"="setkey(DT,state_id)","sto_not_na"="setkey(DT,store_id)"
)
i_sto_not_na_1 = as.numeric(which(apply(row_types_for_calculation, MARGIN = 1,
                                        function(I) all(!is.na(I[c("item_id","store_id")])) & all(is.na(I[c("category_id","department_id","state_id")])))))
i_sto_not_na_2 = i_sto_not_na_1[1:floor(length(i_sto_not_na_1) / 2)]
i_sto_not_na_1 = i_sto_not_na_1[(1+floor(length(i_sto_not_na_1) / 2)):length(i_sto_not_na_1)]
index_groups <- list(
  "none_not_na" = as.numeric(which(apply(row_types_for_calculation, MARGIN = 1,
                                         function(I) all(is.na(I[c("item_id","category_id","department_id","state_id","store_id")]))))),
  "i_not_na" = as.numeric(which(apply(row_types_for_calculation, MARGIN = 1,
                                      function(I) all(!is.na(I["item_id"])) & all(is.na(I[c("category_id","department_id","state_id","store_id")]))))),
  "i_sta_not_na" = as.numeric(which(apply(row_types_for_calculation, MARGIN = 1,
                                          function(I) all(!is.na(I[c("item_id","state_id")])) & all(is.na(I[c("category_id","department_id","store_id")]))))),
  # "i_sto_not_na" = as.numeric(which(apply(row_types_for_calculation, MARGIN = 1,
  #                                        function(I) all(!is.na(I[c("item_id","store_id")])) & all(is.na(I[c("category_id","department_id","state_id")]))))),
  "i_sto_not_na_1" = i_sto_not_na_1,
  "i_sto_not_na_2" = i_sto_not_na_2,
  "cat_not_na" = as.numeric(which(apply(row_types_for_calculation, MARGIN = 1,
                                        function(I) all(!is.na(I["category_id"])) & all(is.na(I[c("item_id","department_id","state_id","store_id")]))))),
  "cat_sta_not_na" = as.numeric(which(apply(row_types_for_calculation, MARGIN = 1,
                                            function(I) all(!is.na(I[c("category_id","state_id")])) & all(is.na(I[c("item_id","department_id","store_id")]))))),
  "cat_sto_not_na" = as.numeric(which(apply(row_types_for_calculation, MARGIN = 1,
                                            function(I) all(!is.na(I[c("category_id","store_id")])) & all(is.na(I[c("item_id","department_id","state_id")]))))),
  "dep_not_na" = as.numeric(which(apply(row_types_for_calculation, MARGIN = 1,
                                        function(I) all(!is.na(I["department_id"])) & all(is.na(I[c("item_id","category_id","state_id","store_id")]))))),
  "dep_sta_not_na" = as.numeric(which(apply(row_types_for_calculation, MARGIN = 1,
                                            function(I) all(!is.na(I[c("department_id","state_id")])) & all(is.na(I[c("item_id","category_id","store_id")]))))),
  "dep_sto_not_na" = as.numeric(which(apply(row_types_for_calculation, MARGIN = 1,
                                            function(I) all(!is.na(I[c("department_id","store_id")])) & all(is.na(I[c("item_id","category_id","state_id")]))))),
  "sta_not_na" = as.numeric(which(apply(row_types_for_calculation, MARGIN = 1,
                                        function(I) all(!is.na(I["state_id"])) & all(is.na(I[c("item_id","category_id","department_id","store_id")]))))),
  "sto_not_na" = as.numeric(which(apply(row_types_for_calculation, MARGIN = 1,
                                        function(I) all(!is.na(I["store_id"])) & all(is.na(I[c("item_id","category_id","department_id","state_id")])))))
)
## Returns all rows that are same as the provided row, including the provided row
find_matching_row2 <- function(row, df) {
  l <- unlist(apply(df, 2, list), recursive=F)
  logic <- mapply(function(x,y)x==y, l, row)
  return(which(.rowSums(logic, m=nrow(logic), n=ncol(logic)) == ncol(logic)))
}

all_row_match_data = data.matrix(row_types_final[,c("item_id","category_id","department_id","state_id","store_id")])
all_row_match_data[is.na(all_row_match_data)] <- -1

#### SET UP THE PARALLEL
library(doParallel)
library(foreach)
#setup parallel backend to use many processors
cores=detectCores()
cl <- makeCluster(2)#cores[1]-1) #not to overload your computer
registerDoParallel(cl)

functionThatDoesSomething <- function(iteration, ig, all_row_match_data, row_types_for_calculation, DT, rows_for_loop) {
  i = rows_for_loop[iteration]
  row_data = row_types_for_calculation[i,]
  sales_vec = switch(ig,
                     "none_not_na" = DT[,.(sum(sales_count)), by = date_id][order(date_id)][,V1],
                     "i_not_na" = DT[item_id == row_data$item_id, .(sum(sales_count)), by = date_id][order(date_id)][,V1],
                     "i_sta_not_na" = DT[item_id == row_data$item_id & state_id == row_data$state_id, .(sum(sales_count)), by = date_id][order(date_id)][,V1],
                     # "i_sto_not_na" = DT[item_id == row_data$item_id & store_id == row_data$store_id, .(sum(sales_count)), by = date_id][order(date_id)][,V1],
                     "i_sto_not_na_1" = DT[item_id == row_data$item_id & store_id == row_data$store_id, .(sum(sales_count)), by = date_id][order(date_id)][,V1],
                     "i_sto_not_na_2" = DT[item_id == row_data$item_id & store_id == row_data$store_id, .(sum(sales_count)), by = date_id][order(date_id)][,V1],
                     "cat_not_na" = DT[category_id == row_data$category_id, .(sum(sales_count)), by = date_id][order(date_id)][,V1],
                     "cat_sta_not_na" = DT[category_id == row_data$category_id & state_id == row_data$state_id, .(sum(sales_count)), by = date_id][order(date_id)][,V1],
                     "cat_sto_not_na" = DT[category_id == row_data$category_id & store_id == row_data$store_id, .(sum(sales_count)), by = date_id][order(date_id)][,V1],
                     "dep_not_na" = DT[department_id == row_data$department_id, .(sum(sales_count)), by = date_id][order(date_id)][,V1],
                     "dep_sta_not_na" = DT[department_id == row_data$department_id & state_id == row_data$state_id, .(sum(sales_count)), by = date_id][order(date_id)][,V1],
                     "dep_sto_not_na" = DT[department_id == row_data$department_id & store_id == row_data$store_id, .(sum(sales_count)), by = date_id][order(date_id)][,V1],
                     "sta_not_na" = DT[state_id == row_data$state_id, .(sum(sales_count)), by = date_id][order(date_id)][,V1],
                     "sto_not_na" = DT[store_id == row_data$store_id, .(sum(sales_count)), by = date_id][order(date_id)][,V1]
  )

  row_match_data = row_data[,c("item_id","category_id","department_id","state_id","store_id")]
  row_match_data[is.na(row_match_data)] <- -1
  matched_row_indices <- find_matching_row2(row_match_data, all_row_match_data)
  # rnum = length(matched_row_indices)
  cnum = 1914 #length(sales_vec) + 1
  # mat = Matrix(data = c(c(matched_row_indices), rep(sales_vec, each = rnum)), nrow = rnum, ncol = cnum)
  mat = Matrix(data = c(matched_row_indices[1], sales_vec), nrow = 1, ncol = cnum)
  return(mat)
}
#############

for(ig in index_group_names) {
  print(ig)
  ## Set the key for the datatable
  eval(parse(text=index_key_set[[ig]]))
  rows_for_loop = index_groups[[ig]]
  counter = 0
  #length(rows_for_loop)
  start_time <- Sys.time()
  combinedMatrix <- foreach(i=1:length(rows_for_loop), .combine=rbind, .inorder = F, .packages = c("data.table","Matrix"), .export = c("find_matching_row2", "functionThatDoesSomething", "all_row_match_data", "row_types_for_calculation", "DT", "rows_for_loop", "ig"), .noexport = ls()) %dopar% {
    tempMatrix = functionThatDoesSomething(i, ig, all_row_match_data, row_types_for_calculation, DT, rows_for_loop) #calling a function
    #do other things if you want
    # if(i %% 50 == 1) {
    #   print(Sys.time() - start_time)
    #   cat(100*round(i / length(rows_for_loop), 2), "%\n")
    #   start_time <- Sys.time()
    # }
    tempMatrix #Equivalent to finalMatrix = rbind(finalMatrix, tempMatrix)
  }
  print(Sys.time() - start_time)

  if(ig == "none_not_na") {
    finalMatrix <- combinedMatrix
  } else {
    finalMatrix <- rbind(finalMatrix, combinedMatrix)
  }

  # start_time = Sys.time()
  # for(i in 1:1000){#length(rows_for_loop)) {
  #   tempMatrix = functionThatDoesSomething(i, ig, all_row_match_data, row_types_for_calculation, DT, rows_for_loop) #calling a function
  #   #do other things if you want
  #   # counter = counter + 1
  #   # if(counter %% 50 == 0) {
  #   #   print(Sys.time() - start_time)
  #   #   cat(100*round(counter / length(rows_for_loop), 2), "%\n")
  #   #   start_time <- Sys.time()
  #   # }
  #   if(i == 1) {
  #     finalMatrix = tempMatrix
  #   } else {
  #     finalMatrix = rbind(finalMatrix, tempMatrix)
  #   }
  # }
  # print(Sys.time() - start_time)
  ## This could be parallelised I suppose
  # for(i in rows_for_loop) {
  #   #print(counter)
  #   counter = counter + 1
  #   if(counter %% 50 == 0) {
  #     print(Sys.time() - start_time)
  #     cat(100*round(counter / length(rows_for_loop), 2), "%\n")
  #     start_time <- Sys.time()
  #   }
  #   row_data = row_types_for_calculation[i,]
  #   sales_vec = switch(ig,
  #                      "none_not_na" = DT[,.(sum(sales_count)), by = date_id][order(date_id)][,V1],
  #                      "i_not_na" = DT[item_id == row_data$item_id, .(sum(sales_count)), by = date_id][order(date_id)][,V1],
  #                      "i_sta_not_na" = DT[item_id == row_data$item_id & state_id == row_data$state_id, .(sum(sales_count)), by = date_id][order(date_id)][,V1],
  #                      "i_sto_not_na" = DT[item_id == row_data$item_id & store_id == row_data$store_id, .(sum(sales_count)), by = date_id][order(date_id)][,V1],
  #                      "cat_not_na" = DT[category_id == row_data$category_id, .(sum(sales_count)), by = date_id][order(date_id)][,V1],
  #                      "cat_sta_not_na" = DT[category_id == row_data$category_id & state_id == row_data$state_id, .(sum(sales_count)), by = date_id][order(date_id)][,V1],
  #                      "cat_sto_not_na" = DT[category_id == row_data$category_id & store_id == row_data$store_id, .(sum(sales_count)), by = date_id][order(date_id)][,V1],
  #                      "dep_not_na" = DT[department_id == row_data$department_id, .(sum(sales_count)), by = date_id][order(date_id)][,V1],
  #                      "dep_sta_not_na" = DT[department_id == row_data$department_id & state_id == row_data$state_id, .(sum(sales_count)), by = date_id][order(date_id)][,V1],
  #                      "dep_sto_not_na" = DT[department_id == row_data$department_id & store_id == row_data$store_id, .(sum(sales_count)), by = date_id][order(date_id)][,V1],
  #                      "sta_not_na" = DT[state_id == row_data$state_id, .(sum(sales_count)), by = date_id][order(date_id)][,V1],
  #                      "sto_not_na" = DT[store_id == row_data$store_id, .(sum(sales_count)), by = date_id][order(date_id)][,V1]
  #   )
  #
  #   #start<- Sys.time()
  #   row_match_data = row_data[,c("item_id","category_id","department_id","state_id","store_id")]
  #   row_match_data[is.na(row_match_data)] <- -1
  #   matched_row_indices <- find_matching_row2(row_match_data, all_row_match_data)
  #   #submission_format_data[matched_row_indices,] <- rep(sales_vec, each = length(matched_row_indices))
  #   #submission_format_data[,matched_row_indices] <- sales_vec
  #   rnum = length(matched_row_indices)
  #   cnum = 1914 #length(sales_vec) + 1
  #   mat = Matrix(data = c(c(matched_row_indices), rep(sales_vec, each = rnum)), nrow = rnum, ncol = cnum)
  #   ## sales_vec length 1913, ncol(submission_format_data) = 1969, correct with database, so why?
  # }
}
#stop cluster
stopCluster(cl)

saveRDS(finalMatrix, "submission_formatted_data.rds")
#colnames(finalMatrix) <- c("row_id", sapply(1:(dim(finalMatrix)[2]-1), function(I) paste0("d_",I)))
combineMatchingRows <- function(i, row_types_for_calculation) {
  row_data = row_types_for_calculation[i,]
  row_match_data = row_data[,c("item_id","category_id","department_id","state_id","store_id")]
  row_match_data[is.na(row_match_data)] <- -1
  matched_row_indices <- find_matching_row2(row_match_data, all_row_match_data)
  return(data.frame(submission_row_id = matched_row_indices,
                    all_ts_row_id = rep(matched_row_indices[1], length(matched_row_indices))))
}

cl <- makeCluster(2)#cores[1]-1) #not to overload your computer
registerDoParallel(cl)

start <- Sys.time()
map_table <- foreach(i=finalMatrix[,1], .combine=rbind, .inorder = F, .export = c("find_matching_row2", "row_types_for_calculation", "combineMatchingRows", "all_row_match_data"), .noexport = ls()) %dopar% {
  combineMatchingRows(i, row_types_for_calculation)
}
print(Sys.time()-start)

stopCluster(cl)

# ## Now map each row in the submission to a row of finalMatrix
# map_table <- NULL
# for(i in finalMatrix[,1]) {
#   if(i %% 100 == 0) {
#     cat(round(i / dim(finalMatrix)[1],2)*100, "%\n")
#   }
#   row_data = row_types_for_calculation[i,]
#   row_match_data = row_data[,c("item_id","category_id","department_id","state_id","store_id")]
#   row_match_data[is.na(row_match_data)] <- -1
#   matched_row_indices <- find_matching_row2(row_match_data, all_row_match_data)
#   if(is.null(map_table)) {
#     map_table <- data.frame(submission_row_number = matched_row_indices,
#                             submission_formatted_data_row = rep(matched_row_indices[1], length(matched_row_indices)))
#   } else {
#     map_table <- rbind(map_table, data.frame(submission_row_number = matched_row_indices,
#                                              submission_formatted_data_row = rep(matched_row_indices[1], length(matched_row_indices))))
#   }
# }
# ##map_table <- map_table %>% rename(submission_row_id = submission_row_number, all_ts_row_id = submission_formatted_data_row)
saveRDS(map_table, "map_submission_data.rds")

write.csv(as.matrix(finalMatrix), "all_time_series.csv", row.names = F)
write.csv(map_table, "submission_ts_map.csv", row.names = F)
## loop through the index groups, loop through their indices, set the key in first run,
## use data.table to get array out, find rows that have the same array as that row further down
## set Matrix rows to that array

## Try put it in database

drv <- dbDriver("SQLite")
con <- dbConnect(drv, dbname = "bosh.db", host = "localhost", user = "jpcryne")

create_ts_table <- dbExecute(con, "CREATE TABLE IF NOT EXISTS time_series(
                             time_series_id INTEGER,
                              Y REAL,
                              date_id INTEGER NOT NULL,
                              FOREIGN KEY(date_id) REFERENCES dates(date_id)
                              )")
create_submission_tables <- dbExecute(con, "CREATE TABLE IF NOT EXISTS submission_data(
                                      submission_data_id INTEGER PRIMARY KEY,
                                      row_id INTEGER NOT NULL,
                                      item_id INTEGER,
                                      category_id INTEGER,
                                      department_id INTEGER,
                                      state_id INTEGER,
                                      store_id INTEGER,
                                      validation INTEGER NOT NULL,
                                      interval REAL NOT NULL,
                                      FOREIGN KEY(item_id) REFERENCES items(item_id),
                                      FOREIGN KEY(category_id) REFERENCES categories(category_id),
                                      FOREIGN KEY(department_id) REFERENCES departments(department_id),
                                      FOREIGN KEY(state_id) REFERENCES states(state_id)
                                      FOREIGN KEY(store_id) REFERENCES store(store_id))")

create_map_table <- dbExecute(con, "CREATE TABLE IF NOT EXISTS ts_submission_map(
                              time_series_id INTEGER NOT NULL,
                              submission_row_id INTEGER NOT NULL,
                              FOREIGN KEY(time_series_id) REFERENCES time_series(time_series_id),
                              FOREIGN KEY(submission_row_id) REFERENCES submission_data(row_id))")

date_table <- dbGetQuery(con, "SELECT day_name, date_id FROM dates")

library(pracma)
tmp <- finalMatrix[,-1]
ts_table <- data.frame(time_series_id = rep(unique(finalMatrix[,1]), each = ncol(finalMatrix)-1),
           Y = as.matrix(Reshape(t(tmp), length(tmp), 1)),
           day_name = rep(c(sapply(1:(dim(finalMatrix)[2]-1), function(I) paste0("d_",I))), nrow(finalMatrix)))
ts_table <- ts_table %>% inner_join(date_table, by = c("day_name"="day_name"))
ts_table <- ts_table %>% dplyr::select(-day_name)

dbWriteTable(con, "time_series", ts_table, append = T)
dbWriteTable(con, "submission_data", row_types_final %>% dplyr::select(-full_name), append = T)
dbWriteTable(con, "ts_submission_map", map_table %>% rename(time_series_id = all_ts_row_id), append = T)

dbExecute(con, "CREATE INDEX time_series_index ON time_series(time_series_id)")

dbDisconnect(con)


## Create tables in database that hold every time series required by submission data,
## with map between the submission data and the time series
## also holds information about the submission data rows
