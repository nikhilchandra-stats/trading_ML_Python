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
      filter(!is.na(Price_to_Low_lag3), !is.na(Price_to_Low_lead), !is.na(Price_to_High_lag2))

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
    asset_infor = asset_infor
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
              predicted <= (Average_NN_Pred - (SD_NN_Pred*sd_value_xx) ) ~ "Long"
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
              predicted >= (Average_NN_Pred + (SD_NN_Pred*sd_value_xx) ) ~ "Long"
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

      tagged_trades <-
        tagged_trades %>%
        ungroup() %>%
        filter(Date == max(Date)) %>%
        mutate(Date = as_datetime(Date, tz = "Australia/Canberra")) %>%
        filter(!is.na(trade_col))

      if(dim(tagged_trades)[1] > 1) {
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
