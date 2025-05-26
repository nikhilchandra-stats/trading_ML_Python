#' extract_required_markov_data
#'
#' @param Hour_data_with_LM
#' @param new_daily_data_ask
#' @param currency_conversion
#' @param mean_values_by_asset_for_loop
#' @param profit_factor
#' @param stop_factor
#' @param risk_dollar_value
#' @param trade_sd_fact
#'
#' @return
#' @export
#'
#' @examples
extract_required_markov_data <-
  function(
    Hour_data_with_LM = Hour_data_with_LM,
    new_daily_data_ask = new_daily_data_ask,
    currency_conversion = currency_conversion,
    mean_values_by_asset_for_loop = mean_values_by_asset_for_loop,
    profit_factor  = 5,
    stop_factor  = 3,
    risk_dollar_value = 5,
    trade_sd_fact = 2
  ) {

    markov_trades_raw <-
      get_markov_tag_pos_neg_diff(
        asset_data_combined = new_daily_data_ask,
        training_perc = 1,
        sd_divides = seq(0.25,2,0.25),
        quantile_divides = seq(0.1,0.9, 0.1),
        rolling_period = 400,
        markov_col_on_interest_pos = "Markov_Point_Pos_roll_sum_1.5",
        markov_col_on_interest_neg = "Markov_Point_Neg_roll_sum_-1.5",
        sum_sd_cut_off = "",
        profit_factor  = profit_factor,
        stop_factor  = stop_factor,
        asset_data_daily_raw = new_daily_data_ask,
        mean_values_by_asset_for_loop = mean_values_by_asset_for_loop,
        trade_sd_fact = trade_sd_fact,
        currency_conversion = currency_conversion,
        risk_dollar_value = risk_dollar_value
      )

    markov_data_daily_Lows <-
      markov_trades_raw$Trades %>% pluck(1)

    markov_data_daily_Lows <- markov_data_daily_Lows %>%
      dplyr::select(Asset, Date,
                    Total_Avg_Prob_Diff_Low = Total_Avg_Prob_Diff ,
                    Total_Avg_Prob_Diff_Median_Low = Total_Avg_Prob_Diff_Median,
                    Total_Avg_Prob_Diff_SD_Low = Total_Avg_Prob_Diff_SD) %>%
      mutate(Date = lubridate::as_datetime(Date, tz = "Australia/Sydney"))

    markov_data_daily_Highs <-
      markov_trades_raw$Trades %>% pluck(2)

    markov_data_daily_Highs <- markov_data_daily_Highs %>%
      dplyr::select(Asset, Date,
                    Total_Avg_Prob_Diff_High = Total_Avg_Prob_Diff ,
                    Total_Avg_Prob_Diff_Median_High = Total_Avg_Prob_Diff_Median,
                    Total_Avg_Prob_Diff_SD_High = Total_Avg_Prob_Diff_SD) %>%
      mutate(Date = lubridate::as_datetime(Date, tz = "Australia/Sydney"))

    makov_data_combined <-
      markov_data_daily_Lows %>%
      left_join(markov_data_daily_Highs)

    Hour_data_with_LM_markov <-
      Hour_data_with_LM %>%
      left_join(makov_data_combined) %>%
      group_by(Asset, Date) %>%
      arrange(Date, .by_group = TRUE) %>%
      group_by(Asset) %>%
      fill(
        c(Total_Avg_Prob_Diff_Low, Total_Avg_Prob_Diff_Median_Low, Total_Avg_Prob_Diff_SD_Low,
          Total_Avg_Prob_Diff_High, Total_Avg_Prob_Diff_Median_High, Total_Avg_Prob_Diff_SD_High),
        .direction = "down"
      ) %>%
      filter(!is.na(Pred_Filled)) %>%
      group_by(Asset) %>%
      arrange(Date, .by_group = TRUE) %>%
      group_by(Asset) %>%
      mutate(
        Price_to_High_lead = log(lead(High)/Price),
        Price_to_Low_lead =  log(Price/lead(Low)),

        Price_to_High_lag = log(lag(High)/lag(Open)),
        Price_to_Low_lag =  log(lag(Open)/lag(Low)),

        Price_to_High_lag2 = log(lag(High, 2)/lag(Open, 2)),
        Price_to_Low_lag2 =  log(lag(Open, 2)/lag(Low, 2)),

        Price_to_High_lag3 = log(lag(High, 3)/lag(Open, 3)),
        Price_to_Low_lag3 =  log(lag(Open, 3)/lag(Low, 3)),

        Price_to_Price_lead =lead(log(Price/Open))
      ) %>%
      ungroup()

    return(Hour_data_with_LM_markov)

  }


#' extract_LM_model_Daily
#'
#' @param daily_data_internal
#' @param raw_macro_data
#' @param AUD_exports_total
#' @param USD_exports_total
#' @param eur_data
#'
#'for the best NN model saved we saved got the LM model using
#'daily_data_internal = starting_asset_data_ask_daily
#'
#' @return
#' @export
#'
#' @examples
extract_LM_model_Daily <- function(
    daily_data_internal = starting_asset_data_ask_daily,
    raw_macro_data = raw_macro_data,
    AUD_exports_total = AUD_exports_total,
    USD_exports_total = USD_exports_total,
    eur_data = eur_data
  ) {

  mean_values_by_asset_for_loop =
    wrangle_asset_data(
      asset_data_daily_raw = daily_data_internal,
      summarise_means = TRUE
    )

  reg_data_list <- run_reg_daily_variant(
    raw_macro_data = raw_macro_data,
    eur_data = eur_data,
    AUD_exports_total = AUD_exports_total,
    USD_exports_total = USD_exports_total,
    asset_data_daily_raw = daily_data_internal,
    train_percent = 0.57
  )

  reg_model <- reg_data_list[[1]]

  return(reg_model)

}

#' run_LM_join_to_H1
#'
#' @param daily_data_internal
#' @param H1_data_internal
#' @param raw_macro_data
#' @param AUD_exports_total
#' @param USD_exports_total
#' @param eur_data
#'
#' @return
#' @export
#'
#' @examples
run_LM_join_to_H1 <- function(
    daily_data_internal = new_daily_data_ask,
    H1_data_internal = new_H1_data_ask,
    raw_macro_data = raw_macro_data,
    AUD_exports_total = AUD_exports_total,
    USD_exports_total = USD_exports_total,
    eur_data = eur_data
) {

  mean_values_by_asset_for_loop =
    wrangle_asset_data(
      asset_data_daily_raw = daily_data_internal,
      summarise_means = TRUE
    )

  reg_data_list <- run_reg_daily_variant(
    raw_macro_data = raw_macro_data,
    eur_data = eur_data,
    AUD_exports_total = AUD_exports_total,
    USD_exports_total = USD_exports_total,
    asset_data_daily_raw = daily_data_internal,
    train_percent = 0.57
  )

  regression_prediction <- reg_data_list[[2]]

  raw_LM_trade_df <- reg_data_list[[2]]

  LM_preped <- prep_LM_daily_trade_data(
    asset_data_daily_raw = daily_data_internal,
    raw_LM_trade_df = reg_data_list[[2]],
    raw_LM_trade_df_training = reg_data_list[[3]]
  )

  trade_with_daily_data <- LM_preped %>% pluck("LM Merged to Daily")
  start_date_reg_data <- trade_with_daily_data %>% pull(Date) %>% min()
  max_date_reg_data <- trade_with_daily_data %>% pull(Date) %>% max()

  start_date_H1 = start_date_reg_data %>% as.character()
  end_date_h1 = max_date_reg_data %>% as.character()
  Hour_data <- H1_data_internal

  Hour_data_with_LM <-
    Hour_data %>%
    left_join(
      trade_with_daily_data %>%
        dplyr::rename(
          Price_Daily = Price ,
          Open_Daily = Open,
          High_daily = High,
          Low_daily = Low
        ) %>%
        dplyr::select(-`Vol.`) %>%
        mutate(
          Date = lubridate::as_datetime(Date, tz = "Australia/Sydney")
        )
    ) %>%
    group_by(Asset) %>%
    arrange(Date, .by_group = TRUE) %>%
    group_by(Asset) %>%
    fill(c(Price_Daily, Open_Daily, High_daily, Low_daily, LM_pred, Pred_Filled, Pred_trade, mean_value, sd_value),
         .direction = "down")

  return(Hour_data_with_LM)

}

#' updated_data_internal
#'
#' @param starting_asset_data
#' @param end_date_day
#' @param time_frame
#' @param bid_or_ask
#'
#' @return
#' @export
#'
#' @examples
updated_data_internal <-
  function(
    starting_asset_data = starting_asset_data_ask_daily,
    end_date_day = end_date_day,
    time_frame = "D",
    bid_or_ask = "ask"
  ) {

    latest_data_in_start_asset <-
      starting_asset_data %>%
      group_by(Asset) %>%
      slice_max(Date)

    latest_date_in_start <-
      latest_data_in_start_asset %>%
      ungroup() %>%
      pull(Date) %>%
      min(na.rm = T)

    new_data <-
      get_db_price(
        db_location = db_location,
        start_date = latest_date_in_start ,
        end_date = end_date_day,
        bid_or_ask = bid_or_ask,
        time_frame = time_frame
      )

    returned <-
      starting_asset_data %>%
      bind_rows(new_data) %>%
      distinct()

    return(returned)

  }

#' get_NN_trade_from_params
#'
#' @param Hour_data_with_LM_markov
#' @param sd_value
#' @param direction
#' @param train_prop
#' @param test_prop
#' @param nn_model_for_trades
#'
#' @return
#' @export
#'
#' @examples
get_NN_trade_from_params <-
  function(
    Hour_data_with_LM_markov = Hour_data_with_LM_markov,
    sd_value_xx = 2,
    direction = "negative",
    train_prop = 0.4,
    test_prop = 0.55,
    nn_model_for_trades = H1_model_High_SD_2_65_neg,
    get_risk_analysis = FALSE,
    mean_values_by_asset_for_loop_H1 = mean_values_by_asset_for_loop_H1,
    profit_factor  = 14,
    stop_factor  = 14,
    risk_dollar_value = 3.5,
    currency_conversion = currency_conversion,
    asset_infor = asset_infor,
    trade_direction = "Long",
    slice_max = TRUE
    ) {

    H1_Model_data_train <-
      Hour_data_with_LM_markov %>%
      group_by(Asset) %>%
      slice_head(prop = train_prop)

    H1_Model_data_test <-
      Hour_data_with_LM_markov %>%
      group_by(Asset) %>%
      slice_tail(prop = test_prop)

    prediction_nn <- predict(object = nn_model_for_trades, newdata = H1_Model_data_test)
    prediction_nn_train <- predict(object = nn_model_for_trades, newdata = H1_Model_data_train)

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

    if(direction == "negative") {
      tagged_trades <-
        H1_Model_data_test %>%
        ungroup() %>%
        mutate(
          predicted = prediction_nn %>% as.numeric()
        ) %>%
        left_join(average_train_predictions) %>%
        mutate(
          trade_col =
            case_when(
              predicted <= (Average_NN_Pred - (SD_NN_Pred*sd_value_xx) ) ~ trade_direction
            )
        )
    }

    if(direction == "positive") {
      tagged_trades <-
        H1_Model_data_test %>%
        ungroup() %>%
        mutate(
          predicted = prediction_nn %>% as.numeric()
        ) %>%
        left_join(average_train_predictions) %>%
        mutate(
          trade_col =
            case_when(
              predicted >= (Average_NN_Pred + (SD_NN_Pred*sd_value_xx) ) ~ trade_direction
            )
        )
    }

    if(get_risk_analysis == TRUE) {

      long_bayes_loop_analysis <-
        generic_trade_finder_loop(
          tagged_trades = tagged_trades %>%
            filter(!is.na(trade_col)) ,
          asset_data_daily_raw = H1_Model_data_test,
          stop_factor = stop_factor,
          profit_factor =profit_factor,
          trade_col = "trade_col",
          date_col = "Date",
          start_price_col = "Price",
          mean_values_by_asset = mean_values_by_asset_for_loop_H1
        )

      analysis_data <-
        generic_anlyser(
          trade_data = long_bayes_loop_analysis %>% rename(Asset = asset),
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

      return(analysis_data)
    }

    if(get_risk_analysis == FALSE) {

      if(slice_max == TRUE) {
        tagged_trades <-
          tagged_trades %>%
          ungroup() %>%
          filter(Date == max(Date)) %>%
          mutate(Date = as_datetime(Date, tz = "Australia/Canberra")) %>%
          filter(!is.na(trade_col))

        if(dim(tagged_trades)[1] > 0) {
          tagged_trades2 <-
            get_stops_profs_volume_trades(mean_values_by_asset = mean_values_by_asset_for_loop_H1,
                                          tagged_trades = tagged_trades,
                                          trade_col = "trade_col",
                                          currency_conversion = currency_conversion,
                                          risk_dollar_value = risk_dollar_value,
                                          stop_factor = stop_factor,
                                          profit_factor = profit_factor)
        } else {
          tagged_trades2 <- NULL
        }

      }

      if(slice_max == FALSE) {
        tagged_trades <-
          tagged_trades %>%
          ungroup() %>%
          # filter(Date == max(Date)) %>%
          # mutate(Date = as_datetime(Date, tz = "Australia/Canberra")) %>%
          filter(!is.na(trade_col))

        tagged_trades2 <- tagged_trades
      }

      return(tagged_trades2)

    }

  }


#' get_NN_best_trades_from_mult_anaysis
#'
#' @param db_path
#' @param network_name
#' @param NN_model
#' @param Hour_data_with_LM_markov
#' @param mean_values_by_asset_for_loop_H1
#' @param currency_conversion
#' @param asset_infor
#' @param risk_dollar_value
#' @param win_threshold
#'
#' @return
#' @export
#'
#' @examples
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
    win_threshold = 0.55,
    slice_max = TRUE
  ) {

    db_con <- connect_db(db_path)

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
      filter(Perc > win_threshold)  %>%
      filter(Network_Name == network_name) %>%
      group_by(sd_fac, direction_sd_1, Network_Name, trade_direction) %>%
      slice_min(stop_factor) %>%
      ungroup() %>%
      distinct(sd_fac, direction_sd_1, stop_factor, profit_factor, Network_Name, Trades, Assets, trade_direction)

    trades_1 <- list()

    DBI::dbDisconnect(db_con)

    for (i in 1:dim(analysis_data_db)[1] ) {

      sd_value_xx <- analysis_data_db$sd_fac[i] %>% as.numeric()
      stop_factor <- analysis_data_db$stop_factor[i] %>% as.numeric()
      profit_factor <- analysis_data_db$profit_factor[i] %>% as.numeric()
      direction <- analysis_data_db$direction_sd_1[i] %>% as.character()
      trade_direction <- analysis_data_db$trade_direction[i] %>% as.character()

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
          asset_infor = asset_infor,
          trade_direction = trade_direction,
          slice_max = slice_max
        )

    }

    trades_1 <- trades_1 %>%
      keep(~ !is.null(.x))

    if(length(trades_1) > 0) {
      all_trades <- trades_1 %>%
        map_dfr(bind_rows)
        # group_by(Asset) %>%
        # slice_min(stop_factor) %>%
        # ungroup()
    } else {
      all_trades <- NULL
    }

    return(all_trades)

  }

#' combine_NN_predictions
#'
#' @param training_data
#' @param testing_data
#' @param stop_factor
#' @param profit_factor
#' @param mean_values_by_asset_for_loop_H1_bid
#' @param mean_values_by_asset_for_loop_H1_ask
#' @param sd_fac
#' @param currency_conversion
#' @param risk_dollar_value
#' @param asset_list_temp
#' @param trade_direction
#'
#' @return
#' @export
#'
#' @examples
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
    trade_direction = "Short",
    test_results = FALSE

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
        )
    ) %>%
    filter(!is.na(trade_col)) %>%
    mutate(direction_sd_1= "negative")

  tagged_trades_test_pos <- testing_data_with_preds_GLM %>%
    mutate(
      trade_col =
        case_when(
          pred >= mean_pred  + sd_pred*sd_fac ~ trade_direction
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

  if(test_results == TRUE) {
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

    return(list(returned_data,
                analysis_data_logit_neg,
                analysis_data_logit_pos)
           )


  }

  if(test_results == FALSE) {
    return(returned_data)
  }

}


#' simulate_trade_results_multi_param
#'
#' @param H1_Model_data_train
#' @param H1_Model_data_test
#' @param NN_results_DB
#' @param Network_Name
#' @param risk_dollar_value
#' @param conditions_Params
#'
#' @return
#' @export
#'
#' @examples
simulate_trade_results_multi_param <-
  function(
    H1_Model_data_train = H1_Model_data_train_ask,
    H1_Model_data_test = H1_Model_data_test_ask,
    NN_results_DB = "C:/Users/Nikhil Chandra/Documents/trade_data/NN_simulation_results.db",
    Network_Name = "H1_LM_Markov_NN_25_SD_71Perc_2025-05-13",
    NN_Model_path = "C:/Users/Nikhil Chandra/Documents/trade_data/H1_LM_Markov_NN_25_SD_71Perc_2025-05-13.rds",
    risk_dollar_value = 10,
    mean_values_by_asset_for_loop_H1 = mean_values_by_asset_for_loop_H1_bid,
    conditions_Params = NULL,
    trade_direction = "Long",
    write_required = FALSE,
    profit_factor_mult = 1
  ) {

    H1_model_High <- readRDS(NN_Model_path)

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


    db_con <- connect_db(path = NN_results_DB)

    if(is.null(conditions_Params)) {

      conditions_Params <-
        tibble(
          sd_fac = c(0, 1, 2, 3, 4, 5, 6, 7,8,9)
        )
      conditions_Params <-
        seq(3, 15, 1) %>%
        map_dfr(
          ~ conditions_Params %>%
            mutate(
              stop_factor = .x
            ) %>%
            mutate(
              profit_factor = profit_factor_mult*.x
            )
        )

    }

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
              predicted <=  Average_NN_Pred - SD_NN_Pred*sd_fac~ trade_direction
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
              predicted >=  Average_NN_Pred + SD_NN_Pred*sd_fac ~ trade_direction
            )
        ) %>%
        filter(!is.na(trade_col))

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

      if(write_required == FALSE) {
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

      if(write_required == TRUE) {
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

      rm(tagged_trades_neg)
      rm(tagged_trades_pos)
      rm(analysis_data_asset_neg)
      rm(analysis_data_asset_pos)

    }

    DBI::dbDisconnect(db_con)

    return(
      list(trade_summaries, trade_summaries_asset)
    )

}


