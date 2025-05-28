helperfunctions35South::load_custom_functions()
one_drive_path <- helperfunctions35South::create_one_drive_path(
  path_extension = "raw data")
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

db_location <- "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data.db"
start_date_day = "2022-06-01"
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

# new_15_data_ask <-
#   updated_data_internal(starting_asset_data = starting_asset_data_ask_M15,
#                         end_date_day = current_date,
#                         time_frame = "M15", bid_or_ask = "ask")%>%
#   distinct()

# new_15_data_ask = starting_asset_data_ask_daily
profit_factor  = 5
stop_factor  = 3
risk_dollar_value = 5
trade_sd_fact = 2
rolling_period = 400
asset_data_daily_raw = new_15_data_ask
mean_values_by_asset_for_loop = mean_values_by_asset_for_loop_15_ask
trade_sd_fact = trade_sd_fact
currency_conversion = currency_conversion
risk_dollar_value = risk_dollar_value

markov_trades_raw <-
  get_markov_tag_pos_neg_diff(
    asset_data_combined = starting_asset_data_ask_daily,
    training_perc = 1,
    sd_divides = seq(0.25,2,0.25),
    quantile_divides = seq(0.1,0.9, 0.1),
    rolling_period = rolling_period,
    markov_col_on_interest_pos = "Markov_Point_Pos_roll_sum_1.5",
    markov_col_on_interest_neg = "Markov_Point_Neg_roll_sum_-1.5",
    sum_sd_cut_off = "",
    profit_factor  = profit_factor,
    stop_factor  = stop_factor,
    asset_data_daily_raw = starting_asset_data_ask_daily,
    mean_values_by_asset_for_loop = mean_values_by_asset_for_loop_15_ask,
    trade_sd_fact = trade_sd_fact,
    currency_conversion = currency_conversion,
    risk_dollar_value = risk_dollar_value
  )

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

macro_indicators <- names(makov_data_combined) %>%
  keep(~str_detect(.x, "USD |CAD |JPY |AUD |EUR |GBP |CNY ")) %>%
  unlist()

makov_data_combined <-
  markov_data_Lows %>%
  left_join(markov_data_Highs) %>%
  group_by(Asset) %>%
  mutate(
    Price_Change = lag(Price) - lag(Open, 15),
    MA_fast_Price = slider::slide_dbl(.x = Price_Change,.f = ~ mean(.x, na.rm = T), .before = 15),
    Price_Change = lag(Price) - lag(Open, 30),
    MA_slow_Price = slider::slide_dbl(.x = Price_Change,.f = ~ mean(.x, na.rm = T), .before = 30),

    High_Open = lag(High) - lag(Open, 15),
    MA_fast_High = slider::slide_dbl(.x = High_Open,.f = ~ mean(.x, na.rm = T), .before = 15),
    High_Open = lag(High) - lag(Open, 30),
    MA_slow_High = slider::slide_dbl(.x = High_Open,.f = ~ mean(.x, na.rm = T), .before = 30),

    lead_Open_to_Price = log(lead(Price, 10)/lead(Open, 1))

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
  group_by(Asset) %>%
  fill(where(is.numeric), .direction = "down") %>%
  ungroup()

gc()

markov_col_names <- names(makov_data_combined) %>% keep(~str_detect(.x, "Markov_Point")) %>% unlist()
running_mid_sd_col_names <- names(makov_data_combined) %>% keep(~str_detect(.x, "running_")) %>% unlist()
macro_indicators <- names(makov_data_combined) %>%
  keep(~str_detect(.x, "USD |CAD |JPY |AUD |EUR |GBP |CNY ")) %>%
  unlist()

regressors <-
  c(markov_col_names, running_mid_sd_col_names, macro_indicators)
lm_formula <- create_lm_formula(dependant = "lead_Open_to_Price", independant = regressors)

training_data <-makov_data_combined %>%
  group_by(Asset) %>%
  slice_head(prop = 0.55) %>%
  ungroup()

gc()

lm_model <- lm(data = training_data, formula = lm_formula)

testing_data <- makov_data_combined %>%
  group_by(Asset) %>%
  slice_tail(prop = 0.40) %>%
  ungroup()

mean_sd_pred <- training_data %>%
  distinct(Date, Asset) %>%
  mutate(
    Pred = predict.lm(lm_model, training_data) %>% as.numeric()
  ) %>%
  group_by(Asset) %>%
  summarise(mean_pred = mean(Pred, na.rm = T),
            sd_pred = sd(Pred, na.rm = T))

testing_data <- testing_data%>%
  mutate(
    Pred = predict.lm(lm_model,
                      newdata = testing_data
                      ) %>% as.numeric()
  ) %>%
  mutate(
    trade_col = Pred
  )
