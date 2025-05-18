
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

mean_values_by_asset_for_loop_D =
  wrangle_asset_data(
    asset_data_daily_raw = starting_asset_data_ask_daily,
    summarise_means = TRUE
  )

mean_values_by_asset_for_loop_H1 =
  wrangle_asset_data(
    asset_data_daily_raw = starting_asset_data_ask_H1,
    summarise_means = TRUE
  )

Hour_data_with_LM <-
  run_LM_join_to_H1(
    daily_data_internal = starting_asset_data_ask_daily,
    H1_data_internal = starting_asset_data_ask_H1,
    raw_macro_data = raw_macro_data,
    AUD_exports_total = AUD_exports_total,
    USD_exports_total = USD_exports_total,
    eur_data = eur_data
  )

Hour_data_with_LM_markov <-
  extract_required_markov_data(
    Hour_data_with_LM = Hour_data_with_LM,
    new_daily_data_ask = starting_asset_data_ask_daily,
    currency_conversion = currency_conversion,
    mean_values_by_asset_for_loop = mean_values_by_asset_for_loop_D,
    profit_factor  = 5,
    stop_factor  = 3,
    risk_dollar_value = 5,
    trade_sd_fact = 2
  )

H1_Model_data_train <-
  Hour_data_with_LM_markov %>%
  group_by(Asset) %>%
  slice_head(prop = 0.4)

H1_Model_data_test <-
  Hour_data_with_LM_markov %>%
  group_by(Asset) %>%
  slice_tail(prop = 0.55)

mean_values_by_asset_for_loop_D =
  wrangle_asset_data(
    asset_data_daily_raw = starting_asset_data_ask_daily,
    summarise_means = TRUE
  )

mean_values_by_asset_for_loop_H1 =
  wrangle_asset_data(
    asset_data_daily_raw = starting_asset_data_ask_H1,
    summarise_means = TRUE
  )

NN_results_DB <-
  "C:/Users/Nikhil Chandra/Documents/trade_data/NN_simulation_results.db"
db_con <- connect_db(path = NN_results_DB)
#------------------------------------Starting Base Model
Network_Name <- "H1_LM_Markov_NN_25_SD_71Perc_2025-05-13"
H1_model_High <-
  readRDS(glue::glue("C:/Users/Nikhil Chandra/Documents/trade_data/H1_LM_Markov_NN_25_SD_71Perc_2025-05-13.rds"))

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
    sd_fac = c(0, 1, 2, 3, 4, 5, 6)
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
            predicted <=  Average_NN_Pred - SD_NN_Pred*sd_fac~ "Long"
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
            predicted <=  Average_NN_Pred + SD_NN_Pred*9000~ "Long"
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

  if(i == 1) {
    write_table_sql_lite(conn = db_con,
                         .data = trade_summaries_upload,
                         table_name = "Simulation_Results" )

    write_table_sql_lite(conn = db_con,
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

clean_names_func_temp <-
  function(.data = trade_summaries_asset[[1]]) {

    names(.data) <- c("trade_direction", "Trades", "wins", "Final_Dollars", "Lowest_Dollars",
                      "Dollars_quantile_25", "Dollars_quantile_75", "max_Dollars", "minimal_loss",
                      "maximum_win", "Perc", "stop_factor", "profit_factor", "risk_weighted_return",
                      "sd_fac", "Network_Name", "direction_sd_1", "loss_time_hours", "win_time_hours",
                      "direction_sd_2")

    return(.data)

  }

clean_names_func_temp_asset <-
  function(.data = trade_summaries_asset[[1]]) {

    names(.data) <- c("Asset", "trade_direction", "Trades", "wins", "Final_Dollars", "Lowest_Dollars",
                      "Dollars_quantile_25", "Dollars_quantile_75", "max_Dollars", "minimal_loss",
                      "maximum_win", "Perc", "stop_factor", "profit_factor", "risk_weighted_return",
                      "sd_fac", "Network_Name", "direction_sd_1", "loss_time_hours", "win_time_hours",
                      "direction_sd_2")

    return(.data)

  }

clean_trade_summaries <-
  trade_summaries %>%
  map_dfr(
    ~ clean_names_func_temp(.x)
  )

clean_trade_summaries_asset <-
  trade_summaries_asset %>%
  map_dfr(
    ~ clean_names_func_temp_asset(.x)
  )


testing <- clean_trade_summaries %>%
  filter(win_time_hours <= 94)

#----------------------------------------------------------------------------------
#------------------------------------Starting Base Model
Network_Name <- "H1_LM_Markov_NN_Long_56_prof_10_4sd2025_05_17"
H1_model_High <-
  readRDS(glue::glue("C:/Users/Nikhil Chandra/Documents/trade_data/H1_LM_Markov_NN_Long_56_prof_10_4sd2025-05-17.rds"))

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
    sd_fac = c(0, 1, 2, 3, 4, 5, 6)
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
            predicted <=  Average_NN_Pred - SD_NN_Pred*sd_fac~ "Long"
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
            predicted <=  Average_NN_Pred + SD_NN_Pred*9000~ "Long"
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


trade_summaries_sd2025_05_17_asset <- trade_summaries_asset %>%
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

trade_summaries_sd2025_05_17 <- trade_summaries %>%
  map_dfr(bind_rows) %>%
  left_join(trade_summaries_sd2025_05_17_asset) %>%
  filter(Perc >= 0.6)

Network_Name <- "H1_LM_Markov_NN_Long_56_prof_10_4sd2025_05_17"
H1_model_High <-
  readRDS(glue::glue("C:/Users/Nikhil Chandra/Documents/trade_data/H1_LM_Markov_NN_Long_56_prof_10_4sd2025-05-17.rds"))
db_location = "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data.db"

get_NN_best_trades_from_mult_anaysis <-
  function(
    db_path = "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data.db",
    network_name = "H1_LM_Markov_NN_Long_56_prof_10_4sd2025_05_17",
    NN_model = H1_model_High,
    Hour_data_with_LM_markov = Hour_data_with_LM_markov,
    mean_values_by_asset_for_loop_H1 = mean_values_by_asset_for_loop_H1,
    currency_conversion = currency_conversion,
    asset_infor = asset_infor,
    risk_dollar_value = 5,
    win_threshold = 0.6
    ) {

    db_con <- connect_db(db_path)
    analysis_data_db <-
      DBI::dbGetQuery(conn = db_con,
                      statement =  "SELECT * FROM Simulation_Results")

    analysis_data_Asset_db <-
      DBI::dbGetQuery(conn = db_con,
                      statement =  "SELECT * FROM Simulation_Results_Asset")%>%
      mutate(Assets_Above_50 = ifelse(Trades >= 50, 50, 1)) %>%
      group_by(Assets_Above_50, trade_direction, sd_fac, Network_Name, stop_factor, profit_factor, direction_sd_1) %>%
      summarise(
        Assets = n_distinct(Asset)
      ) %>%
      group_by(trade_direction, sd_fac, Network_Name, stop_factor, profit_factor, direction_sd_1) %>%
      slice_max(Assets_Above_50) %>%
      ungroup() %>%
      mutate(
        Assets_Above_50 = ifelse(Assets_Above_50 ==50 , TRUE, FALSE)
      )

    analysis_data_db <-
      DBI::dbGetQuery(conn = db_con,
                      statement =  "SELECT * FROM Simulation_Results") %>%
      left_join(analysis_data_Asset_db) %>%
      filter(Perc > win_threshold) %>%
      group_by(sd_fac, direction_sd_1, Network_Name) %>%
      slice_min(stop_factor) %>%
      ungroup() %>%
      distinct(sd_fac, direction_sd_1, stop_factor, profit_factor, Network_Name, Trades, Assets) %>%
      filter(Network_Name == network_name)

    trades_1 <- list()

    for (i in 1:dim(analysis_data_db)[1] ) {

      sd_value_xx <- analysis_data_db$sd_fac[i] %>% as.numeric()
      stop_factor <- analysis_data_db$stop_factor[i] %>% as.numeric()
      profit_factor <- analysis_data_db$profit_factor[i] %>% as.numeric()
      direction <- analysis_data_db$direction_sd_1[i] %>% as.character()

      trades_1[[i]] <-
        get_NN_trade_from_params(
          Hour_data_with_LM_markov = Hour_data_with_LM_markov,
          sd_value_xx = sd_value_xx,
          direction = direction,
          train_prop = 0.4,
          test_prop = 0.55,
          nn_model_for_trades = NN_model,
          get_risk_analysis = FALSE,
          mean_values_by_asset_for_loop_H1 = mean_values_by_asset_for_loop_H1,
          profit_factor  = profit_factor,
          stop_factor  = stop_factor,
          risk_dollar_value = risk_dollar_value,
          currency_conversion = currency_conversion,
          asset_infor = asset_infor
        )

    }

    all_trades <- trades_1 %>%
      map_dfr(bind_rows) %>%
      group_by(Asset) %>%
      slice_min(stop_factor) %>%
      ungroup()

    return(all_trades)

  }



