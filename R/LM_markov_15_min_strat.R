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
    trade_sd_fact = 0,
    rolling_period = 400,
    mean_values_by_asset_for_loop = mean_values_by_asset_for_loop_15_ask,
    currency_conversion = currency_conversion,
    MA_lag1 = 15,
    MA_lag2 = 30,
    sd_divides = seq(0.25,2,0.25),
    quantile_divides = seq(0.1,0.9, 0.1)
    ) {

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
