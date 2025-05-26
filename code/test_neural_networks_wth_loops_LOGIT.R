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

combine_NN_predictions <- function(
    training_data = H1_Model_data_train,
    testing_data = H1_Model_data_test,
    stop_factor = 6,
    profit_factor = 6,
    mean_values_by_asset_for_loop_H1_bid = mean_values_by_asset_for_loop_H1_bid,
    mean_values_by_asset_for_loop_H1_ask = mean_values_by_asset_for_loop_H1_ask,
    sd_fac = 3,
    currency_conversion = currency_conversion,
    risk_dollar_value = risk_dollar_value,
    asset_list_temp =
      c("USD_JPY", "GBP_JPY", "USD_SGD", "EUR_SEK", "USD_CHF", "USD_SEK", "XCU_USD",
        "USD_MXN", "GBP_USD", "WTICO_USD", "SUGAR_USD", "USD_NOK", "USD_CZK", "WHEAT_USD",
        "EUR_USD", "XAG_USD", "EUR_GBP", "USD_CNH", "USD_CAD", "SOYBN_USD", "AUD_USD", "NZD_USD",
        "NZD_CHF", "WHEAT_USD", "USD_NOK"),
    trade_direction = "Short"

) {

  Network_Name1 <- "H1_LM_Markov_NN_25_SD_71Perc_2025-05-13"
  NN1 <-
    readRDS(glue::glue("C:/Users/Nikhil Chandra/Documents/trade_data/H1_LM_Markov_NN_25_SD_71Perc_2025-05-13.rds"))
  prediction_nn_train_NN1 <- predict(object = NN1, newdata = training_data)
  prediction_nn_test_NN1 <- predict(object = NN1, newdata = testing_data)
  rm(NN1)

  Network_Name2 <- "H1_LM_Markov_NN_Long_56_prof_10_4sd2025_05_17"
  NN2 <-
    readRDS(glue::glue("C:/Users/Nikhil Chandra/Documents/trade_data/H1_LM_Markov_NN_Long_56_prof_10_4sd2025-05-17.rds"))
  prediction_nn_train_NN2 <- predict(object = NN2, newdata = training_data)
  prediction_nn_test_NN2 <- predict(object = NN2, newdata = testing_data)
  rm(NN2)

  Network_Name3 <- "LM_ML_Lead_5_21_21_5_layer"
  NN3 <-
    readRDS(glue::glue("C:/Users/Nikhil Chandra/Documents/trade_data/LM_ML_Lead_5_21_21_5_layer_2025-05-20.rds"))
  prediction_nn_train_NN3 <- predict(object = NN3, newdata = training_data)
  prediction_nn_test_NN3 <- predict(object = NN3, newdata = testing_data)
  rm(NN3)

  Network_Name4 <- "H1_LM_Markov_NN_Hidden35_withLag"
  NN4 <-
    readRDS(glue::glue("C:/Users/Nikhil Chandra/Documents/trade_data/H1_LM_Markov_NN_Hidden35_withLag_2025-05-17.rds"))
  prediction_nn_train_NN4 <- predict(object = NN4, newdata = training_data)
  prediction_nn_test_NN4 <- predict(object = NN4, newdata = testing_data)
  rm(NN4)

  Network_Name5 <- "LM_ML_HighLead_14_14_layer_"
  NN5 <-
    readRDS(glue::glue("C:/Users/Nikhil Chandra/Documents/trade_data/LM_ML_HighLead_14_14_layer_2025-05-19.rds"))
  prediction_nn_train_NN5 <- predict(object = NN5, newdata = training_data)
  prediction_nn_test_NN5 <- predict(object = NN5, newdata = testing_data)
  rm(NN5)

  Network_Name6 <- "H1_LM_Markov_NN_Hidden35"
  NN6 <-
    readRDS(glue::glue("C:/Users/Nikhil Chandra/Documents/trade_data/H1_LM_Markov_NN_Hidden35_withLag_2025-05-17.rds"))
  prediction_nn_train_NN6 <- predict(object = NN6, newdata = training_data)
  prediction_nn_test_NN6 <- predict(object = NN6, newdata = testing_data)
  rm(NN6)


  training_data_with_preds <-
    training_data %>%
    ungroup() %>%
    mutate(
      NN1 = prediction_nn_train_NN1 %>% as.numeric(),
      NN2 = prediction_nn_train_NN2%>% as.numeric(),
      NN3 = prediction_nn_train_NN3%>% as.numeric(),
      NN4 = prediction_nn_train_NN4%>% as.numeric(),
      NN5 = prediction_nn_train_NN5%>% as.numeric(),
      NN6 = prediction_nn_train_NN6%>% as.numeric()
    ) %>%
    # left_join(all_trades_for_logit %>% ungroup()) %>%
    group_by(Asset) %>%
    arrange(Date, .by_group = TRUE) %>%
    ungroup() %>%
    mutate(
      win_loss = case_when(
        abs(Price_to_Low_lead) > abs(Price_to_High_lead)  ~ 1, TRUE ~ 0
      )
    ) %>%
    filter(!is.na(NN1), !is.na(NN2), !is.na(NN3), !is.na(NN4), !is.na(win_loss))

  glm_1 <-
    glm(formula = win_loss ~ NN1 + NN2 + NN3 + NN4 +  NN5 + NN6 + Asset +
          NN1*Asset + NN2*Asset +
          Total_Avg_Prob_Diff_Low + Total_Avg_Prob_Diff_Median_Low +
          Total_Avg_Prob_Diff_SD_High + Total_Avg_Prob_Diff_Median_High,
        data = training_data_with_preds)

  testing_data_with_preds <-
    testing_data %>%
    ungroup() %>%
    mutate(
      NN1 = prediction_nn_test_NN1 %>% as.numeric(),
      NN2 = prediction_nn_test_NN2%>% as.numeric(),
      NN3 = prediction_nn_test_NN3%>% as.numeric(),
      NN4 = prediction_nn_test_NN4%>% as.numeric(),
      NN5 = prediction_nn_test_NN5%>% as.numeric(),
      NN6 = prediction_nn_test_NN6%>% as.numeric()
    )

  glm_preds <-
    predict.glm(glm_1,
                testing_data_with_preds,
                type = "response")  %>% as.numeric()
  glm_preds_train <-
    predict.glm(glm_1,
                training_data_with_preds,
                type = "response")  %>% as.numeric()

  mean_values_predictions_per_asset <-
    training_data_with_preds %>%
    dplyr::select(Asset) %>%
    mutate(glm_preds_train = glm_preds_train) %>%
    group_by(Asset) %>%
    summarise(
      mean_pred = mean(glm_preds_train, na.rm = T),
      sd_pred = sd(glm_preds_train, na.rm = T)
    ) %>%
    ungroup()


  testing_data_with_preds_GLM <-
    testing_data_with_preds %>%
    ungroup() %>%
    mutate(
      pred = glm_preds
    ) %>%
    left_join(mean_values_predictions_per_asset)


  tagged_trades_test_neg <- testing_data_with_preds_GLM %>%
    mutate(
      trade_col =
        case_when(
          pred <= mean_pred  - sd_pred*sd_fac ~ trade_direction
          # pred >= 0.47  ~ "Short"
          # pred <= 0.075 ~ "Short"
        )
    ) %>%
    filter(!is.na(trade_col)) %>%
    mutate(direction_sd_1= "negative")

  tagged_trades_test_pos <- testing_data_with_preds_GLM %>%
    mutate(
      trade_col =
        case_when(
          pred >= mean_pred  + sd_pred*sd_fac ~ trade_direction
          # pred >= 0.47  ~ "Short"
          # pred <= 0.075 ~ "Short"
        )
    ) %>%
    filter(!is.na(trade_col)) %>%
    mutate(direction_sd_1= "positive")

  dim(tagged_trades_test_neg)
  dim(tagged_trades_test_pos)

  returned_data  <-
    tagged_trades_test_neg %>%
    bind_rows(tagged_trades_test_pos) %>%
    filter(Asset %in% asset_list_temp)

  all_trades_in_testing_data_neg <-
    generic_trade_finder_loop(
      tagged_trades = tagged_trades_test_neg %>% filter(Asset %in% asset_list_temp) ,
      asset_data_daily_raw = testing_data_with_preds_GLM %>% filter(Asset %in% asset_list_temp),
      stop_factor = stop_factor,
      profit_factor =profit_factor,
      trade_col = "trade_col",
      date_col = "Date",
      start_price_col = "Price",
      mean_values_by_asset = mean_values_by_asset_for_loop_H1_bid %>% filter(Asset %in% asset_list_temp)
    )

  analysis_data_logit_neg <-
    generic_anlyser(
      trade_data = all_trades_in_testing_data_neg %>% rename(Asset = asset),
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
    )

  all_trades_in_testing_data_pos <-
    generic_trade_finder_loop(
      tagged_trades = tagged_trades_test_pos %>% filter(Asset %in% asset_list_temp) ,
      asset_data_daily_raw = testing_data_with_preds_GLM %>% filter(Asset %in% asset_list_temp),
      stop_factor = stop_factor,
      profit_factor =profit_factor,
      trade_col = "trade_col",
      date_col = "Date",
      start_price_col = "Price",
      mean_values_by_asset = mean_values_by_asset_for_loop_H1_bid %>% filter(Asset %in% asset_list_temp)
    )

  analysis_data_logit_pos <-
    generic_anlyser(
      trade_data = all_trades_in_testing_data_pos %>% rename(Asset = asset),
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
    )

  return(returned_data)


}


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

Hour_data_with_LM_markov_ask <-
  extract_required_markov_data(
    Hour_data_with_LM = Hour_data_with_LM_ask,
    new_daily_data_ask = new_daily_data_ask,
    currency_conversion = currency_conversion,
    mean_values_by_asset_for_loop = mean_values_by_asset_for_loop_D_ask,
    profit_factor  = 5,
    stop_factor  = 3,
    risk_dollar_value = 5,
    trade_sd_fact = 2
  )

Hour_data_with_LM_bid <-
  run_LM_join_to_H1(
    daily_data_internal = starting_asset_data_bid_daily,
    H1_data_internal = starting_asset_data_bid_H1,
    raw_macro_data = raw_macro_data,
    AUD_exports_total = AUD_exports_total,
    USD_exports_total = USD_exports_total,
    eur_data = eur_data
  )

Hour_data_with_LM_markov_bid <-
  extract_required_markov_data(
    Hour_data_with_LM = Hour_data_with_LM_bid,
    new_daily_data_ask = starting_asset_data_bid_daily,
    currency_conversion = currency_conversion,
    mean_values_by_asset_for_loop = mean_values_by_asset_for_loop_D_bid,
    profit_factor  = 5,
    stop_factor  = 3,
    risk_dollar_value = 5,
    trade_sd_fact = 2
  )

H1_Model_data_train <-
  Hour_data_with_LM_markov_bid %>%
  group_by(Asset) %>%
  slice_head(prop = 0.4)

H1_Model_data_test <-
  Hour_data_with_LM_markov_bid %>%
  group_by(Asset) %>%
  slice_tail(prop = 0.55)

Network_Name <- "All_NN_into_Logit"
NN_results_DB <-
  "C:/Users/Nikhil Chandra/Documents/trade_data/NN_simulation_results.db"
db_con <- connect_db(path = NN_results_DB)
test <- DBI::dbGetQuery(db_con, statement = "SELECT * FROM Simulation_Results")

risk_dollar_value <- 10

conditions_Params <-
  tibble(
    sd_fac = c( 2, 3, 4, 5, 6, 7,8,9)
  )
conditions_Params <-
  seq(3,11,1) %>%
  map_dfr(
    ~ conditions_Params %>%
      mutate(
        stop_factor = .x
      ) %>%
      mutate(
        profit_factor = .x*1.5
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


  tagged_trades <-
    combine_NN_predictions(
    training_data = H1_Model_data_train,
    testing_data = H1_Model_data_test,
    stop_factor = stop_factor,
    profit_factor = profit_factor,
    mean_values_by_asset_for_loop_H1_bid = mean_values_by_asset_for_loop_H1_bid,
    mean_values_by_asset_for_loop_H1_ask = mean_values_by_asset_for_loop_H1_ask,
    sd_fac = sd_fac,
    currency_conversion = currency_conversion,
    risk_dollar_value = risk_dollar_value,
    asset_list_temp =
      c("USD_JPY", "GBP_JPY", "USD_SGD", "EUR_SEK", "USD_CHF", "USD_SEK", "XCU_USD",
        "USD_MXN", "GBP_USD", "WTICO_USD", "SUGAR_USD", "USD_NOK", "USD_CZK", "WHEAT_USD",
        "EUR_USD", "XAG_USD", "EUR_GBP", "USD_CNH", "USD_CAD", "SOYBN_USD", "AUD_USD", "NZD_USD",
        "NZD_CHF", "WHEAT_USD", "USD_NOK")

    )


  tagged_trades_neg <- tagged_trades %>%
    filter(direction_sd_1 == "negative")

  tagged_trades_pos <- tagged_trades %>%
    filter(direction_sd_1 == "positive")

  #-------------------------------------------------------------Negative

  if(dim(tagged_trades_neg)[1] > 1) {
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
  }



  #-----------------------------------------------------------Positive

  if(dim(tagged_trades_pos)[1] > 1) {
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
  }

  if(dim(tagged_trades_pos)[1] >= 1 &  dim(tagged_trades_neg)[1] >= 1) {
    trade_summaries[[i]] <- analysis_data_pos %>%
      bind_rows(analysis_data_neg)
    trade_summaries_asset[[i]] <- analysis_data_asset_pos %>%
      bind_rows(analysis_data_asset_neg)
  }

  if(dim(tagged_trades_pos)[1] >= 1 &  dim(tagged_trades_neg)[1] <= 1) {
    trade_summaries[[i]] <- analysis_data_pos
    trade_summaries_asset[[i]] <- analysis_data_asset_pos
  }

  if(dim(tagged_trades_pos)[1] <= 1 &  dim(tagged_trades_neg)[1] >= 1) {
    trade_summaries[[i]] <- analysis_data_neg
    trade_summaries_asset[[i]] <- analysis_data_asset_neg
  }

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

  rm(tagged_trades_neg)
  rm(tagged_trades_pos)
  rm(analysis_data_asset_neg)
  rm(analysis_data_asset_pos)

}

test <- DBI::dbGetQuery(conn = db_con, "SELECT * FROM Simulation_Results")
test2 <- DBI::dbGetQuery(conn = db_con, "SELECT * FROM Simulation_Results_Asset")

trade_summaries_NN_GLM_multi_asset <- trade_summaries_asset %>%
  map_dfr(bind_rows) %>%
  filter(Network_Name == "All_NN_into_Logit") %>%
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

trade_summaries_NN_GLM_multi <- trade_summaries %>%
  map_dfr(bind_rows) %>%
  filter(Network_Name == "All_NN_into_Logit") %>%
  left_join(trade_summaries_NN_GLM_multi_asset) %>%
  filter(risk_weighted_return > 0.05)
