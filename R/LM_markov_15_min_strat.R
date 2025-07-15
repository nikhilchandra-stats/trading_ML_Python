#' get_15_min_markov_data_macro
#'
#' @param new_15_data_ask
#' @param trade_sd_fact
#' @param rolling_period
#' @param mean_values_by_asset_for_loop
#' @param currency_conversion
#' @param MA_lag1
#' @param MA_lag2
#' @param sd_divides
#' @param quantile_divides
#'
#' @returns
#' @export
#'
#' @examples
get_15_min_markov_data_macro <-
  function(
    new_15_data_ask = starting_asset_data_ask_daily,
    raw_macro_data = raw_macro_data,
    trade_sd_fact = 0,
    rolling_period = 400,
    mean_values_by_asset_for_loop = mean_values_by_asset_for_loop_15_ask,
    currency_conversion = currency_conversion,
    LM_period_1 = 2,
    LM_period_2 = 10,
    LM_period_3 = 15,
    LM_period_4 = 35,
    MA_lag1 = 15,
    MA_lag2 = 30,
    sd_divides = seq(0.25,2,0.25),
    quantile_divides = seq(0.1,0.9, 0.1)
    ) {

    check_date_in_data <- new_15_data_ask %>% pull(Date) %>% max(na.rm = TRUE) %>% as.character()
    message(glue::glue("Max Date in Data provided to Data Prep Stage 1: {check_date_in_data}"))

    markov_trades_raw <-
      get_markov_tag_pos_neg_diff(
        asset_data_combined = new_15_data_ask,
        training_perc = 1,
        sd_divides = sd_divides,
        quantile_divides = quantile_divides,
        rolling_period = rolling_period,
        markov_col_on_interest_pos = "Markov_Point_Pos_roll_sum_1.5",
        markov_col_on_interest_neg = "Markov_Point_Neg_roll_sum_-1.5",
        sum_sd_cut_off = "",
        profit_factor  = 18,
        stop_factor  = 10,
        asset_data_daily_raw = new_15_data_ask,
        mean_values_by_asset_for_loop = mean_values_by_asset_for_loop,
        trade_sd_fact = trade_sd_fact,
        currency_conversion = currency_conversion,
        risk_dollar_value = 10,
        skip_trade_analysis = TRUE
      )

    # tictoc::toc()
    markov_data_Lows <-
      markov_trades_raw$Trades %>% pluck(1)

    markov_data_Lows <-
      markov_data_Lows  %>%
      mutate(Date = lubridate::as_datetime(Date, tz = "Australia/Sydney")) %>%
      dplyr::select(
        Date, Asset, Price, Open, High, Low,
        running_mid_low = running_mid,
        running_sd_low = running_sd,
        `Markov_Point_Neg_-0.25 Low` = `Markov_Point_Neg_-0.25`,
        `Markov_Point_Neg_-0.5 Low` = `Markov_Point_Neg_-0.5`,
        `Markov_Point_Neg_-1 Low` = `Markov_Point_Neg_-1`,
        `Markov_Point_Neg_-1.25 Low` = `Markov_Point_Neg_-1.25`,
        `Markov_Point_Neg_-1.5 Low` = `Markov_Point_Neg_-1.5`,
        Total_Avg_Prob_Diff_Low = Total_Avg_Prob_Diff ,
        Total_Avg_Prob_Diff_Median_Low = Total_Avg_Prob_Diff_Median,
        Total_Avg_Prob_Diff_SD_Low = Total_Avg_Prob_Diff_SD
      )

    markov_data_Highs <-
      markov_trades_raw$Trades %>% pluck(2)

    markov_data_Highs <- markov_data_Highs %>%
      mutate(Date = lubridate::as_datetime(Date, tz = "Australia/Sydney"))%>%
      dplyr::select(
        Date, Asset,
        running_mid_high = running_mid,
        running_sd_high = running_sd,
        `Markov_Point_Neg_-0.25 High` = `Markov_Point_Neg_-0.25`,
        `Markov_Point_Neg_-0.5 High` = `Markov_Point_Neg_-0.5`,
        `Markov_Point_Neg_-1 High` = `Markov_Point_Neg_-1`,
        `Markov_Point_Neg_-1.25 High` = `Markov_Point_Neg_-1.25`,
        `Markov_Point_Neg_-1.5 High` = `Markov_Point_Neg_-1.5`,
        Total_Avg_Prob_Diff_High = Total_Avg_Prob_Diff ,
        Total_Avg_Prob_Diff_Median_High = Total_Avg_Prob_Diff_Median,
        Total_Avg_Prob_Diff_SD_High = Total_Avg_Prob_Diff_SD
      )

    gc()

    message("LM Calcs")
    # tictoc::tic()
    US_Macro_Data <- get_USD_Indicators(raw_macro_data = raw_macro_data,
                                        lag_days = 4) %>%
      mutate(date = as_datetime(date)) %>%
      rename(Date = date)
    EUR_Macro_Data <- get_EUR_Indicators(raw_macro_data = raw_macro_data,
                                         lag_days = 4)%>%
      mutate(date = as_datetime(date)) %>%
      rename(Date = date)
    AUD_Macro_Data <- get_AUS_Indicators(raw_macro_data = raw_macro_data,
                                         lag_days = 4)%>%
      mutate(date = as_datetime(date)) %>%
      rename(Date = date)
    JPY_Macro_Data <- get_JPY_Indicators(raw_macro_data = raw_macro_data,
                                         lag_days = 4)%>%
      mutate(date = as_datetime(date)) %>%
      rename(Date = date)
    GBP_Macro_Data <- get_GBP_Indicators(raw_macro_data = raw_macro_data,
                                         lag_days = 4)%>%
      mutate(date = as_datetime(date)) %>%
      rename(Date = date)
    CAD_Macro_Data <- get_CAD_Indicators(raw_macro_data = raw_macro_data,
                                         lag_days = 4)%>%
      mutate(date = as_datetime(date)) %>%
      rename(Date = date)
    CNY_Macro_Data <- get_CNY_Indicators(raw_macro_data = raw_macro_data,
                                         lag_days = 4)%>%
      mutate(date = as_datetime(date)) %>%
      rename(Date = date)

    NZD_Macro_Data <- get_NZD_Indicators(raw_macro_data = raw_macro_data,
                                         lag_days = 4)%>%
      mutate(date = as_datetime(date)) %>%
      rename(Date = date)

    makov_data_combined <-
      markov_data_Lows %>%
      left_join(markov_data_Highs) %>%
      group_by(Asset) %>%
      mutate(
        Price_Change = lag(Price) - lag(Open, MA_lag1),
        MA_fast_Price = slider::slide_dbl(.x = Price_Change,.f = ~ mean(.x, na.rm = T), .before = MA_lag1),
        Price_Change = lag(Price) - lag(Open, MA_lag2),
        MA_slow_Price = slider::slide_dbl(.x = Price_Change,.f = ~ mean(.x, na.rm = T), .before = MA_lag2),

        High_Open = lag(High) - lag(Open, MA_lag1),
        MA_fast_High = slider::slide_dbl(.x = High_Open,.f = ~ mean(.x, na.rm = T), .before = MA_lag1),
        High_Open = lag(High) - lag(Open, MA_lag2),
        MA_slow_High = slider::slide_dbl(.x = High_Open,.f = ~ mean(.x, na.rm = T), .before = MA_lag2),

        lead_Open_to_Price = log(lead(Price, LM_period_1)/lead(Open, 1)),
        lead_Open_to_Price2 = log(lead(Price, LM_period_2)/lead(Open, 1)),
        lead_Open_to_Price3 = log(lead(Price, LM_period_3)/lead(Open, 1)),
        lead_Open_to_Price4 = log(lead(Price, LM_period_4)/lead(Open, 1))

      ) %>%
      mutate(
        Macro_Date_Col = as_date(Date)
      ) %>%
      ungroup() %>%
      left_join(US_Macro_Data, by = c("Macro_Date_Col" = "Date")) %>%
      left_join(EUR_Macro_Data, by = c("Macro_Date_Col" = "Date") ) %>%
      left_join(AUD_Macro_Data, by = c("Macro_Date_Col" = "Date") ) %>%
      left_join(JPY_Macro_Data, by = c("Macro_Date_Col" = "Date") ) %>%
      left_join(GBP_Macro_Data, by = c("Macro_Date_Col" = "Date") ) %>%
      left_join(CAD_Macro_Data, by = c("Macro_Date_Col" = "Date") ) %>%
      left_join(CNY_Macro_Data, by = c("Macro_Date_Col" = "Date") ) %>%
      left_join(NZD_Macro_Data, by = c("Macro_Date_Col" = "Date") ) %>%
      group_by(Asset) %>%
      fill(where(is.numeric), .direction = "down") %>%
      ungroup() %>%
      mutate(
        EUR_check = ifelse(str_detect(Asset, "EUR"), 1, 0),
        AUD_check = ifelse(str_detect(Asset, "AUD"), 1, 0),
        USD_check = ifelse(str_detect(Asset, "USD"), 1, 0),
        GBP_check = ifelse(str_detect(Asset, "GBP"), 1, 0),
        JPY_check = ifelse(str_detect(Asset, "JPY"), 1, 0),
        CNY_check = ifelse(str_detect(Asset, "CNY"), 1, 0),
        CAD_check = ifelse(str_detect(Asset, "CAD"), 1, 0),
        SEK_check = ifelse(str_detect(Asset, "SEK"), 1, 0),
        NOK_check = ifelse(str_detect(Asset, "NOK"), 1, 0),
        CHF_check = ifelse(str_detect(Asset, "CHF"), 1, 0),
        CHF_check = ifelse(str_detect(Asset, "NZD"), 1, 0),
        COMMOD_check = ifelse(str_detect(Asset, "XAG|XAU|WHEAT|SOY|WTICO|XCU|BCO|SUGAR|NATGAS"), 1, 0),
        INDEX_check = ifelse(str_detect(Asset, "SPX|SG30|SPX500|US2000|DE|AU200"), 1, 0)
      )

    gc()


    check_date_in_data <- makov_data_combined %>% pull(Date) %>% max(na.rm = TRUE) %>% as.character()
    message(glue::glue("Max Date in Data provided to Data Prep Stage 2: {check_date_in_data}"))

    return(makov_data_combined)

  }

#' get_15_min_markov_trades_markov_LM
#'
#'This function will create and save the LM models and the mean pred values
#'that you can use for predicting trades
#'
#' @param new_15_data_ask
#' @param profit_factor
#' @param stop_factor
#' @param risk_dollar_value
#' @param trade_sd_fact
#' @param rolling_period
#' @param mean_values_by_asset_for_loop
#' @param trade_sd_fact
#' @param currency_conversion
#' @param risk_dollar_value
#'
#' @return
#' @export
#'
#' @examples
get_15_min_markov_LM_models <-
  function(
    new_15_data_ask = starting_asset_data_ask_daily,
    trade_sd_fact = 0,
    rolling_period = 400,
    mean_values_by_asset_for_loop = mean_values_by_asset_for_loop_15_ask,
    currency_conversion = currency_conversion,
    LM_period_1 = 2,
    LM_period_2 = 10,
    LM_period_3 = 15,
    LM_period_4 = 35,
    MA_lag1 = 15,
    MA_lag2 = 30,
    sd_divides = seq(0.25,2,0.25),
    quantile_divides = seq(0.1,0.9, 0.1),
    LM_save_path = "C:/Users/nikhi/Documents/trade_data/"
  ) {

    makov_data_combined <-
      get_15_min_markov_data_macro(
        new_15_data_ask = new_15_data_ask,
        trade_sd_fact = trade_sd_fact,
        rolling_period = rolling_period,
        mean_values_by_asset_for_loop = mean_values_by_asset_for_loop,
        currency_conversion = currency_conversion,
        MA_lag1 = MA_lag1,
        MA_lag2 = MA_lag2,
        sd_divides = sd_divides,
        quantile_divides = quantile_divides
      )

    message("markov calcs")
    # tictoc::tic()
    markov_col_names <- names(makov_data_combined) %>% keep(~str_detect(.x, "Markov_Point")) %>% unlist()
    running_mid_sd_col_names <- names(makov_data_combined) %>% keep(~str_detect(.x, "running_")) %>% unlist()
    ma_col_names <- c("MA_fast_Price", "MA_slow_Price", "MA_fast_High", "MA_slow_High")
    macro_indicators <- names(makov_data_combined) %>%
      keep(~str_detect(.x, "USD |CAD |JPY |AUD |EUR |GBP |CNY |NZD ")) %>%
      unlist()
    check_names <- names(makov_data_combined) %>%
      keep(~str_detect(.x, "_check")) %>%
      unlist()

    regressors <-
      c(markov_col_names, running_mid_sd_col_names, macro_indicators, ma_col_names, check_names)
    lm_formula <- create_lm_formula(dependant = "lead_Open_to_Price", independant = regressors)
    lm_formula2 <- create_lm_formula(dependant = "lead_Open_to_Price2", independant = regressors)
    lm_formula3 <- create_lm_formula(dependant = "lead_Open_to_Price3", independant = regressors)
    lm_formula4 <- create_lm_formula(dependant = "lead_Open_to_Price4", independant = regressors)

    training_data <-makov_data_combined %>%
      group_by(Asset) %>%
      slice_head(prop = 0.55) %>%
      ungroup()

    testing_data <- makov_data_combined %>%
      group_by(Asset) %>%
      slice_tail(prop = 0.40) %>%
      ungroup()

    gc()

    lm_model <- lm(data = training_data, formula = lm_formula)
    mean_sd_pred1 <- training_data %>%
      distinct(Date, Asset) %>%
      mutate(
        Pred = predict.lm(lm_model, training_data) %>% as.numeric()
      ) %>%
      group_by(Asset) %>%
      summarise(mean_pred = mean(Pred, na.rm = T),
                sd_pred = sd(Pred, na.rm = T))

    saveRDS(object = lm_model,
            file = glue::glue("{LM_save_path}/LM_Model_1.rds"))
    saveRDS(object = mean_sd_pred1,
            file = glue::glue("{LM_save_path}/LM_Model_1_mean_values.rds"))

    # testing_pred1 <- predict.lm(lm_model, newdata = testing_data) %>% as.numeric()
    rm(lm_model)
    gc()

    lm_model <- lm(data = training_data, formula = lm_formula2)
    mean_sd_pred2 <- training_data %>%
      distinct(Date, Asset) %>%
      mutate(
        Pred = predict.lm(lm_model, training_data) %>% as.numeric()
      ) %>%
      group_by(Asset) %>%
      summarise(mean_pred = mean(Pred, na.rm = T),
                sd_pred = sd(Pred, na.rm = T))
    saveRDS(object = lm_model,
            file = glue::glue("{LM_save_path}/LM_Model_2.rds"))
    saveRDS(object = mean_sd_pred2,
            file = glue::glue("{LM_save_path}/LM_Model_2_mean_values.rds"))

    # testing_pred2 <- predict.lm(lm_model, newdata = testing_data) %>% as.numeric()
    rm(lm_model)
    gc()

    lm_model <- lm(data = training_data, formula = lm_formula3)
    mean_sd_pred3 <- training_data %>%
      distinct(Date, Asset) %>%
      mutate(
        Pred = predict.lm(lm_model, training_data) %>% as.numeric()
      ) %>%
      group_by(Asset) %>%
      summarise(mean_pred = mean(Pred, na.rm = T),
                sd_pred = sd(Pred, na.rm = T))
    saveRDS(object = lm_model,
            file = glue::glue("{LM_save_path}/LM_Model_3.rds"))
    saveRDS(object = mean_sd_pred3,
            file = glue::glue("{LM_save_path}/LM_Model_3_mean_values.rds"))

    # testing_pred3 <- predict.lm(lm_model, newdata = testing_data) %>% as.numeric()
    rm(lm_model)
    gc()

    lm_model <- lm(data = training_data, formula = lm_formula4)
    mean_sd_pred4 <- training_data %>%
      distinct(Date, Asset) %>%
      mutate(
        Pred = predict.lm(lm_model, training_data) %>% as.numeric()
      ) %>%
      group_by(Asset) %>%
      summarise(mean_pred = mean(Pred, na.rm = T),
                sd_pred = sd(Pred, na.rm = T))

    saveRDS(object = lm_model,
            file = glue::glue("{LM_save_path}/LM_Model_4.rds"))
    saveRDS(object = mean_sd_pred4,
            file = glue::glue("{LM_save_path}/LM_Model_4_mean_values.rds"))
    # testing_pred4 <- predict.lm(lm_model, newdata = testing_data) %>% as.numeric()
    rm(lm_model)
    gc()

  }

get_15_min_markov_LM_pred <- function(
    transformed_data = starting_asset_data_ask_daily,
    mean_values_by_asset_for_loop = mean_values_by_asset_for_loop_15_ask,
    LM_save_path = "C:/Users/nikhi/Documents/trade_data/"
  ) {

  testing_data <- transformed_data

  gc()

  check_date_in_data <- testing_data %>% pull(Date) %>% max(na.rm = TRUE) %>% as.character()
  message(glue::glue("Max Date in Data provided to LM Calculation: {check_date_in_data}"))

  lm_model <- readRDS(glue::glue("{LM_save_path}/LM_Model_1.rds"))
  mean_sd_pred1 <- readRDS(glue::glue("{LM_save_path}/LM_Model_1_mean_values.rds"))
  testing_pred1 <- predict.lm(lm_model, newdata = testing_data) %>% as.numeric()
  rm(lm_model)
  gc()

  lm_model <- readRDS(glue::glue("{LM_save_path}/LM_Model_2.rds"))
  mean_sd_pred2 <- readRDS(glue::glue("{LM_save_path}/LM_Model_2_mean_values.rds"))
  testing_pred2 <- predict.lm(lm_model, newdata = testing_data) %>% as.numeric()
  rm(lm_model)
  gc()

  lm_model <- readRDS(glue::glue("{LM_save_path}/LM_Model_3.rds"))
  mean_sd_pred3 <- readRDS(glue::glue("{LM_save_path}/LM_Model_3_mean_values.rds"))
  testing_pred3 <- predict.lm(lm_model, newdata = testing_data) %>% as.numeric()
  rm(lm_model)
  gc()

  lm_model <- readRDS(glue::glue("{LM_save_path}/LM_Model_4.rds"))
  mean_sd_pred4 <- readRDS(glue::glue("{LM_save_path}/LM_Model_4_mean_values.rds"))
  testing_pred4 <- predict.lm(lm_model, newdata = testing_data) %>% as.numeric()
  rm(lm_model)
  gc()

  returned_data <-
    testing_data %>%
    ungroup() %>%
    left_join(mean_sd_pred1 %>%
                rename(mean_pred1 = mean_pred,
                       sd_pred1 = sd_pred)
    ) %>%
    left_join(mean_sd_pred2 %>%
                rename(mean_pred2 = mean_pred,
                       sd_pred2 = sd_pred)
    ) %>%
    left_join(mean_sd_pred3 %>%
                rename(mean_pred3 = mean_pred,
                       sd_pred3 = sd_pred)
    ) %>%
    left_join(mean_sd_pred4 %>%
                rename(mean_pred4 = mean_pred,
                       sd_pred4 = sd_pred)
    ) %>%
    mutate(
      Pred1 = testing_pred1,
      Pred2 = testing_pred2,
      Pred3 = testing_pred3,
      Pred4 = testing_pred4
    )

  gc()

  return(returned_data)

}

#' identify_best_LM_15_Preds
#'
#' @param result_db_location
#' @param minimum_risk_avg
#' @param minimum_risk_25
#' @param minimum_Final_Dollars_avg
#' @param trade_direction
#'
#' @returns
#' @export
#'
#' @examples
identify_best_LM_15_Preds <- function(
    result_db_location = "C:/Users/nikhi/Documents/trade_data/LM_15min_markov_sampled.db",
    minimum_risk_avg = 0.05,
    minimum_risk_25 = 0,
    minimum_Final_Dollars_avg = 1000,
    trade_direction = "Long"
  ) {

  db_con <- connect_db(result_db_location)

  trade_data_raw <-
    DBI::dbGetQuery(db_con, statement = "SELECT * FROM LM_15min_markov")

  DBI::dbDisconnect(db_con)
  rm(db_con)

  trades_best <-
    trade_data_raw %>%
    group_by(profit_factor, stop_factor, sd_AVG_Prob, trade_sd_fact1, trade_sd_fact2, trade_sd_fact3, trade_sd_fact4,
             trade_direction, Network_Name) %>%
    summarise(
      Final_Dollars_avg = mean(Final_Dollars, na.rm = T),
      Final_Dollars_25 = quantile(Final_Dollars, 0.25, na.rm = T),

      risk_weighted_avg = mean(risk_weighted_return, na.rm = T),
      risk_weighted_25 = quantile(risk_weighted_return, 0.25, na.rm = T),

      win_time_hours = mean(win_time_hours, na.rm = T),
      loss_time_hours = mean(loss_time_hours, na.rm = T),
      Trades = mean(Trades, na.rm = T)

    ) %>%
    filter(
      risk_weighted_avg >= minimum_risk_avg,
      risk_weighted_25 >= minimum_risk_25,
      Final_Dollars_avg >= minimum_Final_Dollars_avg
    )

  return(trades_best)

}

get_15_min_markov_LM_pred_trades <-
  function(
    data_with_LM_pred = data_with_LM_pred,
    result_db_location = "C:/Users/nikhi/Documents/trade_data/LM_15min_markov_sampled.db",
    minimum_risk_avg = 0.05,
    minimum_risk_25 = 0,
    minimum_Final_Dollars_avg = 1000,
    trade_direction = "Long",
    mean_values_by_asset_for_loop = mean_values_by_asset_for_loop_15_ask,
    risk_dollar_value = 10,
    currency_conversion = currency_conversion,
    asset_infor = asset_infor,
    date_minute_threshold = 20,
    table_name = "LM_15min_markov_full_test"
  ) {

    check_date_in_data <- data_with_LM_pred %>% pull(Date) %>% max(na.rm = TRUE) %>% as.character()

    message(glue::glue("Max Date in Data provided to Trade Find: {check_date_in_data}"))

    db_con <- connect_db(result_db_location)

    asset_data_for_trade_estimation <-
      data_with_LM_pred %>%
      dplyr::select(Date, Asset, Price, Open, High, Low)

    trade_data_raw <-
      DBI::dbGetQuery(db_con, statement = glue::glue("SELECT * FROM {table_name}"))


    DBI::dbDisconnect(db_con)

    trade_data <-
      trade_data_raw %>%
      group_by(profit_factor, stop_factor, sd_AVG_Prob, trade_sd_fact1, trade_sd_fact2, trade_sd_fact3, trade_sd_fact4,
               trade_direction, Network_Name) %>%
      summarise(
        Final_Dollars_avg = mean(Final_Dollars, na.rm = T),
        Final_Dollars_25 = quantile(Final_Dollars, 0.25, na.rm = T),

        risk_weighted_avg = mean(risk_weighted_return, na.rm = T),
        risk_weighted_25 = quantile(risk_weighted_return, 0.25, na.rm = T),

        win_time_hours = mean(win_time_hours, na.rm = T),
        loss_time_hours = mean(loss_time_hours, na.rm = T),
        Trades = mean(Trades, na.rm = T)

      ) %>%
      filter(
        risk_weighted_avg >= minimum_risk_avg,
        risk_weighted_25 >= minimum_risk_25,
        Final_Dollars_avg >= minimum_Final_Dollars_avg
      )

    distinct_params <- trade_data %>%
      ungroup() %>%
      distinct(stop_factor, profit_factor, sd_AVG_Prob, trade_sd_fact1,
               trade_sd_fact2, trade_sd_fact3, trade_sd_fact4, trade_direction, Network_Name,
               win_time_hours) %>%
      filter(trade_direction == trade_direction) %>%
      filter(profit_factor > stop_factor) %>%
      group_by(sd_AVG_Prob, trade_sd_fact1,
               trade_sd_fact2, trade_sd_fact3, trade_sd_fact4, trade_direction, Network_Name) %>%
      slice_min(profit_factor) %>%
      group_by(sd_AVG_Prob, trade_sd_fact1,
               trade_sd_fact2, trade_sd_fact3, trade_sd_fact4, trade_direction, Network_Name) %>%
      slice_min(win_time_hours) %>%
      ungroup()

    collected_trades <- list()
    c = 0

    for (i in 1:dim(distinct_params)[1]) {

      trade_sd_fact1 <- distinct_params$trade_sd_fact1[i]
      trade_sd_fact2 <- distinct_params$trade_sd_fact2[i]
      trade_sd_fact3 <- distinct_params$trade_sd_fact3[i]
      trade_sd_fact4 <- distinct_params$trade_sd_fact4[i]
      sd_AVG_Prob <- distinct_params$sd_AVG_Prob[i]
      stop_factor <- distinct_params$stop_factor[i]
      profit_factor <- distinct_params$profit_factor[i]

      tagged_trades =
        data_with_LM_pred %>%
        mutate(
          trade_col =
            case_when(

            Pred3 >= mean_pred3 + sd_pred3*trade_sd_fact3 &
              Total_Avg_Prob_Diff_Low > Total_Avg_Prob_Diff_Median_Low + sd_AVG_Prob*Total_Avg_Prob_Diff_SD_Low
            ~ "Long",

            Pred2 >= mean_pred2 + sd_pred2*trade_sd_fact2 &
              Total_Avg_Prob_Diff_Low > Total_Avg_Prob_Diff_Median_Low + sd_AVG_Prob*Total_Avg_Prob_Diff_SD_Low
            ~ "Long",

            Pred4 >= mean_pred4 + sd_pred4*trade_sd_fact4 &
              Total_Avg_Prob_Diff_Low > Total_Avg_Prob_Diff_Median_Low + sd_AVG_Prob*Total_Avg_Prob_Diff_SD_Low
            ~ "Long",

            Pred1 >= mean_pred1 + sd_pred1*trade_sd_fact1 &
              Total_Avg_Prob_Diff_Low > Total_Avg_Prob_Diff_Median_Low + sd_AVG_Prob*Total_Avg_Prob_Diff_SD_Low
            ~ "Long"
          )
        ) %>%
        ungroup() %>%
        filter(!is.na(trade_col)) %>%
        slice_max(Date)

      if(dim(tagged_trades)[1] > 0) {

        c = c + 1

        trades_transformed <-
          generic_trade_finder_loop(
            tagged_trades = tagged_trades,
            asset_data_daily_raw = asset_data_for_trade_estimation,
            stop_factor = stop_factor,
            profit_factor =profit_factor,
            trade_col = "trade_col",
            date_col = "Date",
            start_price_col = "Price",
            mean_values_by_asset = mean_values_by_asset_for_loop
          ) %>%
          mutate(
            stop_factor = stop_factor,
            profit_factor = profit_factor
          ) %>%
          rename(
            Asset = asset,
            Date = dates
          ) %>%
          left_join(asset_data_for_trade_estimation)

        trades_with_stops_prof <-
          get_stops_profs_volume_trades(
            tagged_trades = trades_transformed,
            mean_values_by_asset = mean_values_by_asset_for_loop,
            trade_col = "trade_col",
            currency_conversion = currency_conversion,
            risk_dollar_value = 10,
            stop_factor = stop_factor,
            profit_factor = profit_factor,
            asset_col = "Asset",
            stop_col = "stop_value",
            profit_col = "profit_value",
            price_col = "Price",
            trade_return_col = "trade_returns"
          )

        collected_trades[[c]] <-
          trades_with_stops_prof %>%
          mutate(
            stop_factor = stop_factor,
            profit_factor = profit_factor,
            trade_sd_fact3 = distinct_params$trade_sd_fact3[i],
            trade_sd_fact2 = distinct_params$trade_sd_fact2[i],
            trade_sd_fact3 = distinct_params$trade_sd_fact3[i],
            trade_sd_fact4 = distinct_params$trade_sd_fact4[i],
            sd_AVG_Prob = distinct_params$sd_AVG_Prob[i]
          )

      }


    }


    trades_to_take <-
      collected_trades %>%
      map_dfr(bind_rows)

    max_date_in_trades <-
      trades_to_take %>% pull(Date) %>% max(na.rm = T) %>% as.character()
    message(glue::glue("Max Date in Tagged Trades After Loop: {max_date_in_trades}"))

     if (!is.null(trades_to_take) ) {

       check_dims <- ifelse(dim(trades_to_take)[1] > 0, TRUE, FALSE)

     } else {
       check_dims <- FALSE
       }

    if(check_dims == TRUE) {

      trades_to_take <-
        trades_to_take %>%
        left_join(trade_data) %>%
        group_by(Asset) %>%
        slice_max(risk_weighted_avg) %>%
        ungroup() %>%
        dplyr::select(-c(trade_sd_fact3, trade_sd_fact2, trade_sd_fact4, sd_AVG_Prob, Final_Dollars_25,
                         risk_weighted_avg, risk_weighted_25, win_time_hours,Final_Dollars_avg, win_time_hours, loss_time_hours,
                         Trades)) %>%
        filter(Date >= ((now() %>% as_datetime()) - minutes(date_minute_threshold)) )

      trades_found <- dim(trades_to_take)[1]
      message(glue::glue("Trades Found: {trades_found}"))

      if(dim(trades_to_take)[1] < 1) {
        trades_to_take <- NULL
        trades_found<- 0
        message(glue::glue("Trades Found: {trades_found}"))
      }

    } else {

      trades_to_take <- NULL
      trades_found<- 0
      message(glue::glue("Trades Found: {trades_found}"))

    }

    return(trades_to_take)

}

#' get_analysis_15min_LM
#'
#' @param modelling_data_for_trade_tag
#' @param profit_factor
#' @param stop_factor
#' @param risk_dollar_value
#' @param trade_sd_fact1
#' @param trade_sd_fact2
#' @param trade_sd_fact3
#' @param trade_sd_fact4
#' @param sd_AVG_Prob
#' @param rolling_period
#' @param asset_data_daily_raw
#' @param mean_values_by_asset_for_loop
#' @param trade_sd_fact
#' @param currency_conversion
#' @param risk_dollar_value
#' @param Network_Name
#'
#' @return
#' @export
#'
#' @examples
get_analysis_15min_LM <- function(
    modelling_data_for_trade_tag = testing_data,
    profit_factor  = 18,
    stop_factor  = 13,
    risk_dollar_value = 10,
    trade_sd_fact1 = 5,
    trade_sd_fact2 = 3,
    trade_sd_fact3 = 3,
    trade_sd_fact4 = 3,
    sd_AVG_Prob = 0.5,
    rolling_period = 400,
    asset_data_daily_raw = starting_asset_data_ask_daily,
    mean_values_by_asset_for_loop = mean_values_by_asset_for_loop_15_ask,
    trade_sd_fact = trade_sd_fact,
    currency_conversion = currency_conversion,
    Network_Name = "15_min_macro",
    trade_samples = 5000,
    trade_select_samples = 5000
) {

  if(!is.null(trade_samples)) {
    distinct_dates <-
      modelling_data_for_trade_tag %>%
      distinct(Date) %>%
      filter(Date <= (max(Date) + minutes(15*trade_samples)) ) %>%
      pull(Date)

    starting_date <- distinct_dates %>% sample(1)
    ending_date <- starting_date  + minutes(15*trade_samples)

    asset_data_daily_raw <- asset_data_daily_raw %>%
      filter(Date <= (ending_date + minutes(15*1000) ) &
               Date >= (starting_date - minutes(15*1000) ) )

    modelling_data_for_trade_tag <-
      modelling_data_for_trade_tag %>%
      filter(Date <= (ending_date + minutes(15*10) ) &
               Date >= (starting_date - minutes(15*10) ) )

  } else {
    modelling_data_for_trade_tag <-
      modelling_data_for_trade_tag
  }

  tagged_trades =
    modelling_data_for_trade_tag %>%
    # group_by(Asset) %>%
    mutate(
      trade_col =  case_when(

        Pred3 >= mean_pred3 + sd_pred3*trade_sd_fact3 &
          Total_Avg_Prob_Diff_Low > Total_Avg_Prob_Diff_Median_Low + sd_AVG_Prob*Total_Avg_Prob_Diff_SD_Low
        ~ "Long",

        Pred2 >= mean_pred2 + sd_pred2*trade_sd_fact2 &
          Total_Avg_Prob_Diff_Low > Total_Avg_Prob_Diff_Median_Low + sd_AVG_Prob*Total_Avg_Prob_Diff_SD_Low
        ~ "Long",

        Pred4 >= mean_pred4 + sd_pred4*trade_sd_fact4 &
          Total_Avg_Prob_Diff_Low > Total_Avg_Prob_Diff_Median_Low + sd_AVG_Prob*Total_Avg_Prob_Diff_SD_Low
        ~ "Long",

        Pred1 >= mean_pred1 + sd_pred1*trade_sd_fact1 &
          Total_Avg_Prob_Diff_Low > Total_Avg_Prob_Diff_Median_Low + sd_AVG_Prob*Total_Avg_Prob_Diff_SD_Low
        ~ "Long"
      )
    ) %>%
    ungroup() %>%
    filter(!is.na(trade_col))

  if(!is.null(trade_select_samples)) {
    tagged_trades <- tagged_trades %>%
      slice_sample(n = trade_select_samples)
  }

  # tictoc::tic()
  long_bayes_loop_analysis_pos <-
    generic_trade_finder_loop(
      tagged_trades = tagged_trades,
      asset_data_daily_raw = asset_data_daily_raw,
      stop_factor = stop_factor,
      profit_factor =profit_factor,
      trade_col = "trade_col",
      date_col = "Date",
      start_price_col = "Price",
      mean_values_by_asset = mean_values_by_asset_for_loop
    )
  # tictoc::toc()

  trade_timings_pos <-
    long_bayes_loop_analysis_pos %>%
    mutate(
      ending_date_trade = as_datetime(ending_date_trade, tz = "Australia/Canberra"),
      dates = as_datetime(dates, tz = "Australia/Canberra"),
      days_check =
        (as_datetime(ending_date_trade, tz = "Australia/Canberra" ) -
           as_datetime(dates , tz = "Australia/Canberra") )/ddays(1),
      days_check2 = wday(dates) + days_check
    ) %>%
    mutate(Time_Required = (as_datetime(ending_date_trade, tz = "Australia/Canberra" ) -
                              as_datetime(dates , tz = "Australia/Canberra") )/dhours(1),
           Time_Required =
             case_when(
               days_check2 >= 7 ~ Time_Required - 48,
               days_check2 >= 14 ~ Time_Required - 48*2,
               TRUE ~ Time_Required
             )
    )

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
      trade_sd_fact1 = trade_sd_fact1,
      trade_sd_fact2 = trade_sd_fact2,
      trade_sd_fact3 = trade_sd_fact3,
      trade_sd_fact4 = trade_sd_fact4,
      sd_AVG_Prob = sd_AVG_Prob,
      Network_Name = Network_Name

    ) %>%
    bind_cols(trade_timings_by_asset_pos)

  analysis_data_pos_asset <-
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
      trade_sd_fact1 = trade_sd_fact1,
      trade_sd_fact2 = trade_sd_fact2,
      trade_sd_fact3 = trade_sd_fact3,
      trade_sd_fact4 = trade_sd_fact4,
      sd_AVG_Prob = sd_AVG_Prob,
      Network_Name = Network_Name
    ) %>%
    bind_cols(trade_timings_by_asset_pos) %>%
    mutate(across(contains("Dollars"), ~ round(.)))


  if(!is.null(trade_samples)) {
    analysis_data_pos <-
      analysis_data_pos %>%
      mutate(
        starting_date = starting_date,
        ending_date = ending_date
        )

    analysis_data_pos_asset <-
      analysis_data_pos_asset %>%
      mutate(
        starting_date = starting_date,
        ending_date = ending_date
      )
  }

  return(
    list(analysis_data_pos, analysis_data_pos_asset)
  )


}

