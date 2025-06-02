helpeR::load_custom_functions()
library(neuralnet)
raw_macro_data <- get_macro_event_data()

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

asset_list_oanda <- get_oanda_symbols() %>%
  keep( ~ .x %in% c("HK33_HKD", "USD_JPY","SPX500_USD",
                    "AUD_NZD", "GBP_CHF",
                    "EUR_HUF", "EUR_ZAR", "NZD_JPY", "EUR_NZD",
                    "XAU_CAD", "GBP_JPY", "EUR_NOK", "USD_SGD", "EUR_SEK",
                    "DE30_EUR",
                    "AUD_CAD",
                    "XPD_USD",
                    "UK100_GBP",
                    "USD_CHF", "GBP_NZD",
                    "GBP_SGD", "USD_SEK", "EUR_SGD", "XCU_USD", "SUGAR_USD", "CHF_ZAR",
                    "AUD_CHF", "EUR_CHF", "USD_MXN", "GBP_USD", "WTICO_USD", "EUR_JPY", "USD_NOK",
                    "XAU_USD",
                    "USD_CZK", "AUD_SGD", "USD_HUF", "WHEAT_USD",
                    "EUR_USD", "SG30_SGD", "GBP_AUD", "NZD_CAD", "AU200_AUD", "XAG_USD",
                    "XAU_EUR", "EUR_GBP", "USD_CNH", "USD_CAD", "NAS100_USD",
                    "EU50_EUR", "NATGAS_USD", "CAD_JPY", "FR40_EUR", "USD_ZAR", "XAU_GBP",
                    "EUR_AUD", "SOYBN_USD",
                    "US2000_USD",
                    "BCO_USD", "AUD_USD", "NZD_USD", "NZD_CHF", "WHEAT_USD", "AUD_JPY", "AUD_SEK")
  )

asset_infor <- get_instrument_info()

db_location <- "C:/Users/Nikhil/Documents/Asset Data/Oanda_Asset_Data.db"
start_date_day = "2022-09-01"
end_date_day = today() %>% as.character()

starting_asset_data_ask_daily <-
  get_db_price(
    db_location = db_location,
    start_date = start_date_day,
    end_date = end_date_day,
    bid_or_ask = "ask",
    time_frame = "M15"
  )

mean_values_by_asset_for_loop_15_ask =
  wrangle_asset_data(
    asset_data_daily_raw = starting_asset_data_ask_daily,
    summarise_means = TRUE
  )

#' get_15_min_markov_trades_markov_LM
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
get_15_min_markov_trades_markov_LM <-
  function(
    new_15_data_ask = starting_asset_data_ask_daily,
    profit_factor  = 18,
    stop_factor  = 13,
    risk_dollar_value = 10,
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

    message("markov calcs")
    # tictoc::tic()
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
        profit_factor  = profit_factor,
        stop_factor  = stop_factor,
        asset_data_daily_raw = new_15_data_ask,
        mean_values_by_asset_for_loop = mean_values_by_asset_for_loop,
        trade_sd_fact = trade_sd_fact,
        currency_conversion = currency_conversion,
        risk_dollar_value = risk_dollar_value,
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
        MA_slow_Price = slider::slide_dbl(.x = Price_Change,.f = ~ mean(.x, na.rm = T), .before = 30),

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
    testing_pred1 <- predict.lm(lm_model, newdata = testing_data) %>% as.numeric()
    mean_sd_pred1 <- training_data %>%
      distinct(Date, Asset) %>%
      mutate(
        Pred = predict.lm(lm_model, training_data) %>% as.numeric()
      ) %>%
      group_by(Asset) %>%
      summarise(mean_pred = mean(Pred, na.rm = T),
                sd_pred = sd(Pred, na.rm = T))
    rm(lm_model)
    gc()

    lm_model <- lm(data = training_data, formula = lm_formula2)
    testing_pred2 <- predict.lm(lm_model, newdata = testing_data) %>% as.numeric()
    mean_sd_pred2 <- training_data %>%
      distinct(Date, Asset) %>%
      mutate(
        Pred = predict.lm(lm_model, training_data) %>% as.numeric()
      ) %>%
      group_by(Asset) %>%
      summarise(mean_pred = mean(Pred, na.rm = T),
                sd_pred = sd(Pred, na.rm = T))
    rm(lm_model)
    gc()

    lm_model <- lm(data = training_data, formula = lm_formula3)
    testing_pred3 <- predict.lm(lm_model, newdata = testing_data) %>% as.numeric()
    mean_sd_pred3 <- training_data %>%
      distinct(Date, Asset) %>%
      mutate(
        Pred = predict.lm(lm_model, training_data) %>% as.numeric()
      ) %>%
      group_by(Asset) %>%
      summarise(mean_pred = mean(Pred, na.rm = T),
                sd_pred = sd(Pred, na.rm = T))

    rm(lm_model)
    gc()

    lm_model <- lm(data = training_data, formula = lm_formula4)
    testing_pred4 <- predict.lm(lm_model, newdata = testing_data) %>% as.numeric()
    mean_sd_pred4 <- training_data %>%
      distinct(Date, Asset) %>%
      mutate(
        Pred = predict.lm(lm_model, training_data) %>% as.numeric()
      ) %>%
      group_by(Asset) %>%
      summarise(mean_pred = mean(Pred, na.rm = T),
                sd_pred = sd(Pred, na.rm = T))

    rm(lm_model)
    gc()

    rm(US_Macro_Data, EUR_Macro_Data, AUD_Macro_Data, JPY_Macro_Data, GBP_Macro_Data, CAD_Macro_Data, CNY_Macro_Data)
    rm(markov_data_Highs, markov_data_Lows, markov_trades_raw)
    gc()

    rm(training_data)
    gc()

    # tictoc::toc()

    testing_data <-
      makov_data_combined %>%
      group_by(Asset) %>%
      slice_tail(prop = 0.40) %>%
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

    return(testing_data)

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
    Network_Name = "15_min_macro"
  ) {

  # tictoc::tic()
  long_bayes_loop_analysis_pos <-
    generic_trade_finder_loop(
      tagged_trades = modelling_data_for_trade_tag %>%
        group_by(Asset) %>%
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
        ungroup(),
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

  return(
    list(analysis_data_pos, analysis_data_pos_asset)
  )


}

tictoc::tic()

LM_period_1 = 2
LM_period_2 = 10
LM_period_3 = 15
LM_period_4 = 35
MA_lag1 = 15
MA_lag2 = 30
rolling_period = 400

testing_data <-
  get_15_min_markov_trades_markov_LM(
    new_15_data_ask = starting_asset_data_ask_daily,
    profit_factor  = 18,
    stop_factor  = 13,
    risk_dollar_value = 10,
    trade_sd_fact = 0,
    rolling_period = 400,
    mean_values_by_asset_for_loop = mean_values_by_asset_for_loop_15_ask,
    currency_conversion = currency_conversion,
    LM_period_1 = LM_period_1,
    LM_period_2 = LM_period_2,
    LM_period_3 = LM_period_3,
    LM_period_4 = LM_period_4,
    MA_lag1 = MA_lag1,
    MA_lag2 = MA_lag2,
    sd_divides = seq(0.25,2,0.25),
    quantile_divides = seq(0.1,0.9, 0.1)
  )

tictoc::toc()

db_path <- "C:/Users/Nikhil/Documents/trade_data/LM_15min_markov.db"
db_con <- connect_db(db_path)
write_table_now <- FALSE

trade_params <-
  tibble(
    trade_sd_fact1 = c(6,7,8,9)
  )

trade_params <-
  c(3,4,5,6) %>%
  map_dfr(
    ~
      trade_params %>%
      mutate(
        trade_sd_fact2 = .x
      )
  )

trade_params <-
  c(3,4,5,6) %>%
  map_dfr(
    ~
      trade_params %>%
      mutate(
        trade_sd_fact3 = .x
      )
  )

trade_params <-
  c(3,4,5,6) %>%
  map_dfr(
    ~
      trade_params %>%
      mutate(
        trade_sd_fact4 = .x
      )
  )

trade_params <-
  c(8,9,10,11,12,13,14,15) %>%
  map_dfr(
    ~ trade_params %>%
      mutate(
        stop_factor = .x
      ) %>%
      mutate(
        profit_factor = stop_factor*1.5
      )
  )

trade_params <-
  c(0,0.25,0.5,0.75,1,1.25) %>%
  map_dfr(
    ~ trade_params %>%
      mutate(
        sd_AVG_Prob = .x
      )
  )

for (i in 1:dim(trade_params)[1]) {

  trade_sd_fact1 <- trade_params$trade_sd_fact1[1] %>% as.numeric()
  trade_sd_fact2 <- trade_params$trade_sd_fact2[1] %>% as.numeric()
  trade_sd_fact3 <- trade_params$trade_sd_fact3[1] %>% as.numeric()
  trade_sd_fact4 <- trade_params$trade_sd_fact4[1] %>% as.numeric()
  sd_AVG_Prob <- trade_params$sd_AVG_Prob[1] %>% as.numeric()
  stop_factor <- trade_params$stop_factor[1] %>% as.numeric()
  profit_factor <- trade_params$profit_factor[1] %>% as.numeric()


  analysis_info <- get_analysis_15min_LM(
    modelling_data_for_trade_tag = testing_data,
    profit_factor  = profit_factor,
    stop_factor  = stop_factor,
    risk_dollar_value = 10,
    trade_sd_fact1 = trade_sd_fact1,
    trade_sd_fact2 = trade_sd_fact2,
    trade_sd_fact3 = trade_sd_fact3,
    trade_sd_fact4 = trade_sd_fact4,
    sd_AVG_Prob = sd_AVG_Prob,
    rolling_period = 400,
    asset_data_daily_raw = starting_asset_data_ask_daily,
    mean_values_by_asset_for_loop = mean_values_by_asset_for_loop_15_ask,
    trade_sd_fact = trade_sd_fact,
    currency_conversion = currency_conversion,
    Network_Name = "15_min_macro"
  )

  if(i == 1 & write_table_now == TRUE) {
    write_table_sql_lite(.data = analysis_info[[1]],
                         table_name = "LM_15min_markov",
                         conn = db_con )
    write_table_sql_lite(.data = analysis_info[[2]],
                         table_name = "LM_15min_markov_asset",
                         conn = db_con )
  } else {
    append_table_sql_lite(.data = analysis_info[[1]],
                          table_name = "LM_15min_markov",
                          conn = db_con)
    append_table_sql_lite(.data = analysis_info[[2]],
                          table_name = "LM_15min_markov_asset",
                          conn = db_con)
  }

}


