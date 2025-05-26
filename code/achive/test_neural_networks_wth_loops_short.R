helperfunctions35South::load_custom_functions()
one_drive_path <- helperfunctions35South::create_one_drive_path(
  path_extension = "raw data")
library(neuralnet)
raw_macro_data <- get_macro_event_data()
eur_data <- get_EUR_exports()
AUD_exports_total <- get_AUS_exports()  %>%
  pivot_longer(-TIME_PERIOD, names_to = "category", values_to = "Aus_Export") %>%
  rename(date = TIME_PERIOD) %>%
  group_by(date) %>%
  summarise(Aus_Export = sum(Aus_Export, na.rm = T))
USD_exports_total <- get_US_exports()  %>%
  pivot_longer(-date, names_to = "category", values_to = "US_Export") %>%
  group_by(date) %>%
  summarise(US_Export = sum(US_Export, na.rm = T)) %>%
  left_join(AUD_exports_total) %>%
  ungroup()
USD_exports_total <- USD_exports_total %>%
  mutate(
    month_date = lubridate::floor_date(date, "month")
  )
AUD_exports_total <- AUD_exports_total %>%
  mutate(
    month_date = lubridate::floor_date(date, "month")
  )
all_aud_symbols <- get_oanda_symbols() %>%
  keep(~ str_detect(.x, "AUD")|str_detect(.x, "USD_SEK|USD_NOK|USD_HUF|USD_ZAR|USD_CNY|USD_MXN"))
asset_infor <- get_instrument_info()
aud_assets <- read_all_asset_data_intra_day(
  asset_list_oanda = all_aud_symbols,
  save_path_oanda_assets = "C:/Users/Nikhil Chandra/Documents/Asset Data/oanda_data/",
  read_csv_or_API = "API",
  time_frame = "D",
  bid_or_ask = "bid",
  how_far_back = 10,
  start_date = (today() - days(2)) %>% as.character()
)
aud_assets <- aud_assets %>% map_dfr(bind_rows)
aud_usd_today <- get_aud_conversion(asset_data_daily_raw = aud_assets)

currency_conversion <-
  aud_usd_today %>%
  mutate(
    not_aud_asset = ending_value
  ) %>%
  dplyr::select(not_aud_asset, adjusted_conversion) %>%
  bind_rows(
    tibble(not_aud_asset = "AUD", adjusted_conversion = 1)
  )

#----------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------
#--------------------------------------------Short Neural Network
db_location = "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data.db"
start_date_day = "2011-01-01"
end_date_day = today() %>% as.character()

starting_asset_data_ask_daily <-
  get_db_price(
    db_location = db_location,
    start_date = start_date_day,
    end_date = end_date_day,
    bid_or_ask = "ask",
    time_frame = "D"
  )

starting_asset_data_ask_H1 <-
  get_db_price(
    db_location = db_location,
    start_date = start_date_day,
    end_date = end_date_day,
    bid_or_ask = "ask",
    time_frame = "H1"
  )

mean_values_by_asset_for_loop_D_ask =
  wrangle_asset_data(
    asset_data_daily_raw = starting_asset_data_ask_daily,
    summarise_means = TRUE
  )

mean_values_by_asset_for_loop_H1_ask =
  wrangle_asset_data(
    asset_data_daily_raw = starting_asset_data_ask_H1,
    summarise_means = TRUE
  )

starting_asset_data_bid_daily <-
  get_db_price(
    db_location = db_location,
    start_date = start_date_day,
    end_date = end_date_day,
    bid_or_ask = "bid",
    time_frame = "D"
  )

starting_asset_data_bid_H1 <-
  get_db_price(
    db_location = db_location,
    start_date = start_date_day,
    end_date = end_date_day,
    bid_or_ask = "bid",
    time_frame = "H1"
  )

mean_values_by_asset_for_loop_D_bid =
  wrangle_asset_data(
    asset_data_daily_raw = starting_asset_data_bid_daily,
    summarise_means = TRUE
  )

mean_values_by_asset_for_loop_H1_bid =
  wrangle_asset_data(
    asset_data_daily_raw = starting_asset_data_bid_H1,
    summarise_means = TRUE
  )

Hour_data_with_LM <-
  run_LM_join_to_H1(
    daily_data_internal = starting_asset_data_bid_daily,
    H1_data_internal = starting_asset_data_bid_H1,
    raw_macro_data = raw_macro_data,
    AUD_exports_total = AUD_exports_total,
    USD_exports_total = USD_exports_total,
    eur_data = eur_data
  )

Hour_data_with_LM_markov <-
  extract_required_markov_data(
    Hour_data_with_LM = Hour_data_with_LM,
    new_daily_data_ask = starting_asset_data_bid_daily,
    currency_conversion = currency_conversion,
    mean_values_by_asset_for_loop = mean_values_by_asset_for_loop_D_bid,
    profit_factor  = 5,
    stop_factor  = 3,
    risk_dollar_value = 5,
    trade_sd_fact = 2
  )

H1_Model_data_train <-
  Hour_data_with_LM_markov %>%
  group_by(Asset) %>%
  slice_head(prop = 0.4) %>%
  filter(!is.na(Price_to_Low_lag3), !is.na(Price_to_Low_lead), !is.na(Price_to_High_lag2))

H1_Model_data_test <-
  Hour_data_with_LM_markov %>%
  group_by(Asset) %>%
  slice_tail(prop = 0.55)

#------------------------Create Neural Networks

H1_model_High <- neuralnet::neuralnet(formula = Price_to_Price_lead ~
                                        Pred_trade + mean_value + mean_value +
                                        sd_value + Total_Avg_Prob_Diff_Low +
                                        Total_Avg_Prob_Diff_High + Total_Avg_Prob_Diff_SD_Low +
                                        Total_Avg_Prob_Diff_SD_High +
                                        Price_to_High_lag + Price_to_Low_lag +
                                        Price_to_High_lag2 + Price_to_Low_lag2 +
                                        Price_to_High_lag3 + Price_to_Low_lag3,
                                      hidden = c(5, 21, 21, 5),
                                      data = H1_Model_data_train,
                                      err.fct = "sse",
                                      linear.output = TRUE,
                                      lifesign = 'full',
                                      rep = 1,
                                      algorithm = "rprop+",
                                      stepmax = 20000,
                                      threshold = 0.03)

saveRDS(
  H1_model_High,
  glue::glue("C:/Users/Nikhil Chandra/Documents/trade_data/LM_ML_Lead_5_21_21_5_layer_{today()}.rds")
)

#-------------------------------------------------------------------------------------
#--------------------------------------------------------------------------5 21 21 5 NN

NN_results_DB <-
  "C:/Users/Nikhil Chandra/Documents/trade_data/NN_simulation_results.db"
db_con <- connect_db(path = NN_results_DB)
Network_Name <- "LM_ML_Lead_5_21_21_5_layer_"
H1_model_High <-
  readRDS(glue::glue("C:/Users/Nikhil Chandra/Documents/trade_data/LM_ML_Lead_5_21_21_5_layer_{today()}.rds"))

prediction_nn <- predict(object = H1_model_High, newdata = H1_Model_data_test)
prediction_nn_train <- predict(object = H1_model_High, newdata = H1_Model_data_train)

average_train_predictions <-
  H1_Model_data_train %>%
  ungroup() %>%
  mutate(
    train_predictions = prediction_nn_train %>% as.numeric()
  ) %>%
  group_by(Asset) %>%
  summarise(
    Average_NN_Pred = mean(train_predictions, na.rm = T),
    SD_NN_Pred = sd(train_predictions, na.rm = T)
  ) %>%
  ungroup()

risk_dollar_value <- 10

conditions_Params <-
  tibble(
    sd_fac = c(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
  )
conditions_Params <-
  seq(5, 18, 1) %>%
  map_dfr(
    ~ conditions_Params %>%
      mutate(
        stop_factor = .x
      ) %>%
      mutate(
        profit_factor = .x
      )
  )

trade_summaries <-
  list()

trade_summaries_asset <-
  list()

for (i in 91:dim(conditions_Params)[1] ) {

  stop_factor <-
    conditions_Params$stop_factor[i] %>% as.numeric()
  profit_factor <-
    conditions_Params$profit_factor[i] %>% as.numeric()
  sd_fac <-
    conditions_Params$sd_fac[i] %>% as.numeric()

  tagged_trades_neg <-
    H1_Model_data_test %>%
    ungroup() %>%
    mutate(
      predicted = prediction_nn %>% as.numeric()
    ) %>%
    left_join(average_train_predictions) %>%
    mutate(
      trade_col =
        case_when(
          # predicted >=  Average_NN_Pred + SD_NN_Pred*3 &
          #   predicted <=  Average_NN_Pred + SD_NN_Pred*500~ "Long"
          predicted >=  Average_NN_Pred - SD_NN_Pred*90000 &
            predicted <=  Average_NN_Pred - SD_NN_Pred*sd_fac~ "Short"
        )
    ) %>%
    filter(!is.na(trade_col))

  tagged_trades_pos <-
    H1_Model_data_test %>%
    ungroup() %>%
    mutate(
      predicted = prediction_nn %>% as.numeric()
    ) %>%
    left_join(average_train_predictions) %>%
    mutate(
      trade_col =
        case_when(
          predicted >=  Average_NN_Pred + SD_NN_Pred*sd_fac &
            predicted <=  Average_NN_Pred + SD_NN_Pred*9000~ "Short"
          # predicted >=  Average_NN_Pred - SD_NN_Pred*90000 &
          #   predicted <=  Average_NN_Pred - SD_NN_Pred*sd_fac~ "Long"
        )
    ) %>%
    filter(!is.na(trade_col))

  #-------------------------------------------------------------Negative

  long_bayes_loop_analysis_neg <-
    generic_trade_finder_loop(
      tagged_trades = tagged_trades_neg ,
      asset_data_daily_raw = H1_Model_data_test,
      stop_factor = stop_factor,
      profit_factor =profit_factor,
      trade_col = "trade_col",
      date_col = "Date",
      start_price_col = "Price",
      mean_values_by_asset = mean_values_by_asset_for_loop_H1_bid
    )

  trade_timings_neg <-
    long_bayes_loop_analysis_neg %>%
    mutate(
      ending_date_trade = as_datetime(ending_date_trade),
      dates = as_datetime(dates)
    ) %>%
    mutate(Time_Required = (ending_date_trade - dates)/dhours(1) )

  trade_timings_by_asset_neg <- trade_timings_neg %>%
    mutate(win_loss = ifelse(trade_returns < 0, "loss", "wins") ) %>%
    group_by(win_loss) %>%
    summarise(
      Time_Required = median(Time_Required, na.rm = T)
    ) %>%
    pivot_wider(names_from = win_loss, values_from = Time_Required) %>%
    rename(loss_time_hours = loss,
           win_time_hours = wins)

  analysis_data_neg <-
    generic_anlyser(
      trade_data = long_bayes_loop_analysis_neg %>% rename(Asset = asset),
      profit_factor = profit_factor,
      stop_factor = stop_factor,
      asset_infor = asset_infor,
      currency_conversion = currency_conversion,
      asset_col = "Asset",
      stop_col = "starting_stop_value",
      profit_col = "starting_profit_value",
      price_col = "trade_start_prices",
      trade_return_col = "trade_returns",
      risk_dollar_value = risk_dollar_value,
      grouping_vars = "trade_col"
    ) %>%
    mutate(
      sd_fac = sd_fac,
      Network_Name = Network_Name,
      direction_sd = "negative"
    ) %>%
    bind_cols(trade_timings_by_asset_neg)

  analysis_data_asset_neg <-
    generic_anlyser(
      trade_data = long_bayes_loop_analysis_neg %>% rename(Asset = asset),
      profit_factor = profit_factor,
      stop_factor = stop_factor,
      asset_infor = asset_infor,
      currency_conversion = currency_conversion,
      asset_col = "Asset",
      stop_col = "starting_stop_value",
      profit_col = "starting_profit_value",
      price_col = "trade_start_prices",
      trade_return_col = "trade_returns",
      risk_dollar_value = risk_dollar_value,
      grouping_vars = "Asset"
    ) %>%
    mutate(
      sd_fac = sd_fac,
      Network_Name = Network_Name,
      direction_sd = "negative"
    ) %>%
    bind_cols(trade_timings_by_asset_neg)

  #-----------------------------------------------------------Positive

  long_bayes_loop_analysis_pos <-
    generic_trade_finder_loop(
      tagged_trades = tagged_trades_pos ,
      asset_data_daily_raw = H1_Model_data_test,
      stop_factor = stop_factor,
      profit_factor =profit_factor,
      trade_col = "trade_col",
      date_col = "Date",
      start_price_col = "Price",
      mean_values_by_asset = mean_values_by_asset_for_loop_H1_bid
    )

  trade_timings_pos <-
    long_bayes_loop_analysis_pos %>%
    mutate(
      ending_date_trade = as_datetime(ending_date_trade),
      dates = as_datetime(dates)
    ) %>%
    mutate(Time_Required = (ending_date_trade - dates)/dhours(1) )

  trade_timings_by_asset_pos <- trade_timings_pos %>%
    mutate(win_loss = ifelse(trade_returns < 0, "loss", "wins") ) %>%
    group_by(win_loss) %>%
    summarise(
      Time_Required = median(Time_Required, na.rm = T)
    ) %>%
    pivot_wider(names_from = win_loss, values_from = Time_Required) %>%
    rename(loss_time_hours = loss,
           win_time_hours = wins)

  analysis_data_pos <-
    generic_anlyser(
      trade_data = long_bayes_loop_analysis_pos %>% rename(Asset = asset),
      profit_factor = profit_factor,
      stop_factor = stop_factor,
      asset_infor = asset_infor,
      currency_conversion = currency_conversion,
      asset_col = "Asset",
      stop_col = "starting_stop_value",
      profit_col = "starting_profit_value",
      price_col = "trade_start_prices",
      trade_return_col = "trade_returns",
      risk_dollar_value = risk_dollar_value,
      grouping_vars = "trade_col"
    ) %>%
    mutate(
      sd_fac = sd_fac,
      Network_Name = Network_Name,
      direction_sd = "positive"
    ) %>%
    bind_cols(trade_timings_by_asset_pos)

  analysis_data_asset_pos <-
    generic_anlyser(
      trade_data = long_bayes_loop_analysis_pos %>% rename(Asset = asset),
      profit_factor = profit_factor,
      stop_factor = stop_factor,
      asset_infor = asset_infor,
      currency_conversion = currency_conversion,
      asset_col = "Asset",
      stop_col = "starting_stop_value",
      profit_col = "starting_profit_value",
      price_col = "trade_start_prices",
      trade_return_col = "trade_returns",
      risk_dollar_value = risk_dollar_value,
      grouping_vars = "Asset"
    ) %>%
    mutate(
      sd_fac = sd_fac,
      Network_Name = Network_Name,
      direction_sd = "positive"
    ) %>%
    bind_cols(trade_timings_by_asset_pos)

  trade_summaries[[i]] <- analysis_data_pos %>%
    bind_rows(analysis_data_neg)
  trade_summaries_asset[[i]] <- analysis_data_asset_pos %>%
    bind_rows(analysis_data_asset_neg)

  trade_summaries_upload <- trade_summaries[[i]]
  trade_summaries_asset_upload <- trade_summaries_asset[[i]]

  names(trade_summaries_upload) <- c("trade_direction", "Trades", "wins", "Final_Dollars", "Lowest_Dollars",
                                     "Dollars_quantile_25", "Dollars_quantile_75", "max_Dollars", "minimal_loss",
                                     "maximum_win", "Perc", "stop_factor", "profit_factor", "risk_weighted_return",
                                     "sd_fac", "Network_Name", "direction_sd_1", "loss_time_hours", "win_time_hours")

  names(trade_summaries_asset_upload) <-
    c("Asset", "trade_direction", "Trades", "wins", "Final_Dollars", "Lowest_Dollars",
      "Dollars_quantile_25", "Dollars_quantile_75", "max_Dollars", "minimal_loss",
      "maximum_win", "Perc", "stop_factor", "profit_factor", "risk_weighted_return",
      "sd_fac", "Network_Name", "direction_sd_1", "loss_time_hours", "win_time_hours")

  if(i == 1) {
    append_table_sql_lite(conn = db_con,
                          .data = trade_summaries_upload,
                          table_name = "Simulation_Results" )

    append_table_sql_lite(conn = db_con,
                          .data = trade_summaries_asset_upload,
                          table_name = "Simulation_Results_Asset" )
  } else {

    append_table_sql_lite(conn = db_con,
                          .data = trade_summaries_upload,
                          table_name = "Simulation_Results" )

    append_table_sql_lite(conn = db_con,
                          .data = trade_summaries_asset_upload,
                          table_name = "Simulation_Results_Asset" )

  }

}

trade_summaries_5_21_21_5_layer_asset <- trade_summaries_asset %>%
  map_dfr(bind_rows) %>%
  mutate(Assets_Above_50 = ifelse(Trades >= 50, 50, 1)) %>%
  group_by(Assets_Above_50, trade_direction, sd_fac, Network_Name, stop_factor, profit_factor, direction_sd) %>%
  summarise(
    Assets = n_distinct(Asset)
  ) %>%
  group_by(trade_direction, sd_fac, Network_Name, stop_factor, profit_factor, direction_sd) %>%
  slice_max(Assets_Above_50) %>%
  ungroup() %>%
  mutate(
    Assets_Above_50 = ifelse(Assets_Above_50 ==50 , TRUE, FALSE)
  )

trade_summaries_5_21_21_5_layer <- trade_summaries %>%
  map_dfr(bind_rows) %>%
  left_join(trade_summaries_5_21_21_5_layer_asset)

test <- DBI::dbGetQuery(db_con,
                        "SELECT * FROM Simulation_Results") %>%
  filter(Network_Name == "LM_ML_Lead_21_21_21_layer")
test$Network_Name %>% unique()

#--------------------------------------------------------------------------------
#-----------------------------------------------21 21 21
NN_results_DB <-
  "C:/Users/Nikhil Chandra/Documents/trade_data/NN_simulation_results.db"
db_con <- connect_db(path = NN_results_DB)
Network_Name <- "LM_ML_Lead_21_21_21_layer_"
H1_model_High <-
  readRDS(glue::glue("C:/Users/Nikhil Chandra/Documents/trade_data/LM_ML_Lead_21_21_21_layer_2025-05-19.rds"))

prediction_nn <- predict(object = H1_model_High, newdata = H1_Model_data_test)
prediction_nn_train <- predict(object = H1_model_High, newdata = H1_Model_data_train)

average_train_predictions <-
  H1_Model_data_train %>%
  ungroup() %>%
  mutate(
    train_predictions = prediction_nn_train %>% as.numeric()
  ) %>%
  group_by(Asset) %>%
  summarise(
    Average_NN_Pred = mean(train_predictions, na.rm = T),
    SD_NN_Pred = sd(train_predictions, na.rm = T)
  ) %>%
  ungroup()

risk_dollar_value <- 10

conditions_Params <-
  tibble(
    sd_fac = c(7,8,9,10,11,12,13)
  )
conditions_Params <-
  seq(5, 18, 1) %>%
  map_dfr(
    ~ conditions_Params %>%
      mutate(
        stop_factor = .x
      ) %>%
      mutate(
        profit_factor = .x
      )
  )

trade_summaries <-
  list()

trade_summaries_asset <-
  list()

for (i in 1:dim(conditions_Params)[1] ) {

  stop_factor <-
    conditions_Params$stop_factor[i] %>% as.numeric()
  profit_factor <-
    conditions_Params$profit_factor[i] %>% as.numeric()
  sd_fac <-
    conditions_Params$sd_fac[i] %>% as.numeric()

  tagged_trades_neg <-
    H1_Model_data_test %>%
    ungroup() %>%
    mutate(
      predicted = prediction_nn %>% as.numeric()
    ) %>%
    left_join(average_train_predictions) %>%
    mutate(
      trade_col =
        case_when(
          # predicted >=  Average_NN_Pred + SD_NN_Pred*3 &
          #   predicted <=  Average_NN_Pred + SD_NN_Pred*500~ "Long"
          predicted >=  Average_NN_Pred - SD_NN_Pred*90000 &
            predicted <=  Average_NN_Pred - SD_NN_Pred*sd_fac~ "Short"
        )
    ) %>%
    filter(!is.na(trade_col))

  tagged_trades_pos <-
    H1_Model_data_test %>%
    ungroup() %>%
    mutate(
      predicted = prediction_nn %>% as.numeric()
    ) %>%
    left_join(average_train_predictions) %>%
    mutate(
      trade_col =
        case_when(
          predicted >=  Average_NN_Pred + SD_NN_Pred*sd_fac &
            predicted <=  Average_NN_Pred + SD_NN_Pred*9000~ "Short"
          # predicted >=  Average_NN_Pred - SD_NN_Pred*90000 &
          #   predicted <=  Average_NN_Pred - SD_NN_Pred*sd_fac~ "Long"
        )
    ) %>%
    filter(!is.na(trade_col))

  #-------------------------------------------------------------Negative

  long_bayes_loop_analysis_neg <-
    generic_trade_finder_loop(
      tagged_trades = tagged_trades_neg ,
      asset_data_daily_raw = H1_Model_data_test,
      stop_factor = stop_factor,
      profit_factor =profit_factor,
      trade_col = "trade_col",
      date_col = "Date",
      start_price_col = "Price",
      mean_values_by_asset = mean_values_by_asset_for_loop_H1
    )

  trade_timings_neg <-
    long_bayes_loop_analysis_neg %>%
    mutate(
      ending_date_trade = as_datetime(ending_date_trade),
      dates = as_datetime(dates)
    ) %>%
    mutate(Time_Required = (ending_date_trade - dates)/dhours(1) )

  trade_timings_by_asset_neg <- trade_timings_neg %>%
    mutate(win_loss = ifelse(trade_returns < 0, "loss", "wins") ) %>%
    group_by(win_loss) %>%
    summarise(
      Time_Required = median(Time_Required, na.rm = T)
    ) %>%
    pivot_wider(names_from = win_loss, values_from = Time_Required) %>%
    rename(loss_time_hours = loss,
           win_time_hours = wins)

  analysis_data_neg <-
    generic_anlyser(
      trade_data = long_bayes_loop_analysis_neg %>% rename(Asset = asset),
      profit_factor = profit_factor,
      stop_factor = stop_factor,
      asset_infor = asset_infor,
      currency_conversion = currency_conversion,
      asset_col = "Asset",
      stop_col = "starting_stop_value",
      profit_col = "starting_profit_value",
      price_col = "trade_start_prices",
      trade_return_col = "trade_returns",
      risk_dollar_value = risk_dollar_value,
      grouping_vars = "trade_col"
    ) %>%
    mutate(
      sd_fac = sd_fac,
      Network_Name = Network_Name,
      direction_sd = "negative"
    ) %>%
    bind_cols(trade_timings_by_asset_neg)

  analysis_data_asset_neg <-
    generic_anlyser(
      trade_data = long_bayes_loop_analysis_neg %>% rename(Asset = asset),
      profit_factor = profit_factor,
      stop_factor = stop_factor,
      asset_infor = asset_infor,
      currency_conversion = currency_conversion,
      asset_col = "Asset",
      stop_col = "starting_stop_value",
      profit_col = "starting_profit_value",
      price_col = "trade_start_prices",
      trade_return_col = "trade_returns",
      risk_dollar_value = risk_dollar_value,
      grouping_vars = "Asset"
    ) %>%
    mutate(
      sd_fac = sd_fac,
      Network_Name = Network_Name,
      direction_sd = "negative"
    ) %>%
    bind_cols(trade_timings_by_asset_neg)

  #-----------------------------------------------------------Positive

  long_bayes_loop_analysis_pos <-
    generic_trade_finder_loop(
      tagged_trades = tagged_trades_pos ,
      asset_data_daily_raw = H1_Model_data_test,
      stop_factor = stop_factor,
      profit_factor =profit_factor,
      trade_col = "trade_col",
      date_col = "Date",
      start_price_col = "Price",
      mean_values_by_asset = mean_values_by_asset_for_loop_H1
    )

  trade_timings_pos <-
    long_bayes_loop_analysis_pos %>%
    mutate(
      ending_date_trade = as_datetime(ending_date_trade),
      dates = as_datetime(dates)
    ) %>%
    mutate(Time_Required = (ending_date_trade - dates)/dhours(1) )

  trade_timings_by_asset_pos <- trade_timings_pos %>%
    mutate(win_loss = ifelse(trade_returns < 0, "loss", "wins") ) %>%
    group_by(win_loss) %>%
    summarise(
      Time_Required = median(Time_Required, na.rm = T)
    ) %>%
    pivot_wider(names_from = win_loss, values_from = Time_Required) %>%
    rename(loss_time_hours = loss,
           win_time_hours = wins)

  analysis_data_pos <-
    generic_anlyser(
      trade_data = long_bayes_loop_analysis_pos %>% rename(Asset = asset),
      profit_factor = profit_factor,
      stop_factor = stop_factor,
      asset_infor = asset_infor,
      currency_conversion = currency_conversion,
      asset_col = "Asset",
      stop_col = "starting_stop_value",
      profit_col = "starting_profit_value",
      price_col = "trade_start_prices",
      trade_return_col = "trade_returns",
      risk_dollar_value = risk_dollar_value,
      grouping_vars = "trade_col"
    ) %>%
    mutate(
      sd_fac = sd_fac,
      Network_Name = Network_Name,
      direction_sd = "positive"
    ) %>%
    bind_cols(trade_timings_by_asset_pos)

  analysis_data_asset_pos <-
    generic_anlyser(
      trade_data = long_bayes_loop_analysis_pos %>% rename(Asset = asset),
      profit_factor = profit_factor,
      stop_factor = stop_factor,
      asset_infor = asset_infor,
      currency_conversion = currency_conversion,
      asset_col = "Asset",
      stop_col = "starting_stop_value",
      profit_col = "starting_profit_value",
      price_col = "trade_start_prices",
      trade_return_col = "trade_returns",
      risk_dollar_value = risk_dollar_value,
      grouping_vars = "Asset"
    ) %>%
    mutate(
      sd_fac = sd_fac,
      Network_Name = Network_Name,
      direction_sd = "positive"
    ) %>%
    bind_cols(trade_timings_by_asset_pos)

  trade_summaries[[i]] <- analysis_data_pos %>%
    bind_rows(analysis_data_neg)
  trade_summaries_asset[[i]] <- analysis_data_asset_pos %>%
    bind_rows(analysis_data_asset_neg)

  trade_summaries_upload <- trade_summaries[[i]]
  trade_summaries_asset_upload <- trade_summaries_asset[[i]]

  names(trade_summaries_upload) <- c("trade_direction", "Trades", "wins", "Final_Dollars", "Lowest_Dollars",
                                     "Dollars_quantile_25", "Dollars_quantile_75", "max_Dollars", "minimal_loss",
                                     "maximum_win", "Perc", "stop_factor", "profit_factor", "risk_weighted_return",
                                     "sd_fac", "Network_Name", "direction_sd_1", "loss_time_hours", "win_time_hours")

  names(trade_summaries_asset_upload) <-
    c("Asset", "trade_direction", "Trades", "wins", "Final_Dollars", "Lowest_Dollars",
      "Dollars_quantile_25", "Dollars_quantile_75", "max_Dollars", "minimal_loss",
      "maximum_win", "Perc", "stop_factor", "profit_factor", "risk_weighted_return",
      "sd_fac", "Network_Name", "direction_sd_1", "loss_time_hours", "win_time_hours")

  if(i == 1) {
    append_table_sql_lite(conn = db_con,
                          .data = trade_summaries_upload,
                          table_name = "Simulation_Results" )

    append_table_sql_lite(conn = db_con,
                          .data = trade_summaries_asset_upload,
                          table_name = "Simulation_Results_Asset" )
  } else {

    append_table_sql_lite(conn = db_con,
                          .data = trade_summaries_upload,
                          table_name = "Simulation_Results" )

    append_table_sql_lite(conn = db_con,
                          .data = trade_summaries_asset_upload,
                          table_name = "Simulation_Results_Asset" )

  }

}

trade_summaries_21_21_layer_asset <- trade_summaries_asset %>%
  map_dfr(bind_rows) %>%
  mutate(Assets_Above_50 = ifelse(Trades >= 50, 50, 1)) %>%
  group_by(Assets_Above_50, trade_direction, sd_fac, Network_Name, stop_factor, profit_factor, direction_sd) %>%
  summarise(
    Assets = n_distinct(Asset)
  ) %>%
  group_by(trade_direction, sd_fac, Network_Name, stop_factor, profit_factor, direction_sd) %>%
  slice_max(Assets_Above_50) %>%
  ungroup() %>%
  mutate(
    Assets_Above_50 = ifelse(Assets_Above_50 ==50 , TRUE, FALSE)
  )

trade_summaries_21_21_layer <- trade_summaries %>%
  map_dfr(bind_rows) %>%
  left_join(trade_summaries_21_21_layer_asset)
  # filter(Trades > 1000)
  # filter(Perc >= 0.55)
