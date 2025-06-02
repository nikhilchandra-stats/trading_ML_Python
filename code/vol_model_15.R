helpeR::load_custom_functions()
library(neuralnet)
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
start_date_day = "2024-03-01"
end_date_day = today() %>% as.character()

starting_asset_data_ask_daily <-
  get_db_price(
    db_location = db_location,
    start_date = start_date_day,
    end_date = end_date_day,
    bid_or_ask = "ask",
    time_frame = "M15"
  )

starting_asset_data_bid_daily <-
  get_db_price(
    db_location = db_location,
    start_date = start_date_day,
    end_date = end_date_day,
    bid_or_ask = "bid",
    time_frame = "M15"
  )

mean_values_by_asset_for_loop_15_ask =
  wrangle_asset_data(
    asset_data_daily_raw = starting_asset_data_ask_daily,
    summarise_means = TRUE
  )

mean_values_by_asset_for_loop_15_bid =
  wrangle_asset_data(
    asset_data_daily_raw = starting_asset_data_bid_daily,
    summarise_means = TRUE
  )


trade_sd_fac = 3
stop_factor = 5
profit_factor =4
asset_infor = asset_infor
asset_col = "Asset"
stop_col = "starting_stop_value"
profit_col = "starting_profit_value"
price_col = "trade_start_prices"
trade_return_col = "trade_returns"
risk_dollar_value = 10
returns_present = TRUE
trade_return_col = "trade_returns"
currency_conversion = currency_conversion

starting_asset_data_ask_daily <-
  starting_asset_data_ask_daily %>%
  group_by(Asset) %>%
  mutate(High_to_Open = abs(lag(High) - lag(Open)),
         High_to_Open_5 = abs(lag(High) - lag(Open, 5)),
         High_to_Open_10 = abs(lag(High) - lag(Open, 10)),
         High_to_Open_15 = abs(lag(High) - lag(Open, 15)),
         High_to_Open_20 = abs(lag(High) - lag(Open, 20)),
         High_to_Open_ma = slider::slide_dbl(.x = High_to_Open, .f = ~ mean(.x, na.rm = T) ,.before = 50),
         High_to_Open_5_ma = slider::slide_dbl(.x = High_to_Open_5, .f = ~ mean(.x, na.rm = T) ,.before = 50),
         High_to_Open_10_ma = slider::slide_dbl(.x = High_to_Open_10, .f = ~ mean(.x, na.rm = T) ,.before = 50),
         High_to_Open_15_ma = slider::slide_dbl(.x = High_to_Open_15, .f = ~ mean(.x, na.rm = T) ,.before = 50),
         High_to_Open_20_ma = slider::slide_dbl(.x = High_to_Open_20, .f = ~ mean(.x, na.rm = T) ,.before = 50),
         High_to_Open_lead_1 = abs(lead(High) - lead(Open)),
         High_to_Open_lead_10 = abs(lead(High, 10) - lead(Open)),
         High_to_Open_lead_20 = abs(lead(High, 20) - lead(Open)),

         High_to_Open_5_tan = atan((lag(High) - lag(Open, 5))/5),
         High_to_Open_10_tan = atan((lag(High) - lag(Open, 10))/10),
         High_to_Open_15_tan = atan((lag(High) - lag(Open, 15))/15)
         )

starting_asset_data_bid_daily <-
  starting_asset_data_bid_daily %>%
  group_by(Asset) %>%
  mutate(Low_to_Open = abs(lag(Low) - lag(Open)),
         Low_to_Open_5 = abs(lag(Low) - lag(Open, 5)),
         Low_to_Open_10 = abs(lag(Low) - lag(Open, 10)),
         Low_to_Open_15 = abs(lag(Low) - lag(Open, 15)),
         Low_to_Open_20 = abs(lag(Low) - lag(Open, 20)),
         Low_to_Open_ma = slider::slide_dbl(.x = Low_to_Open, .f = ~ mean(.x, na.rm = T) ,.before = 50),
         Low_to_Open_5_ma = slider::slide_dbl(.x = Low_to_Open_5, .f = ~ mean(.x, na.rm = T) ,.before = 50),
         Low_to_Open_10_ma = slider::slide_dbl(.x = Low_to_Open_10, .f = ~ mean(.x, na.rm = T) ,.before = 50),
         Low_to_Open_15_ma = slider::slide_dbl(.x = Low_to_Open_15, .f = ~ mean(.x, na.rm = T) ,.before = 50),
         Low_to_Open_20_ma = slider::slide_dbl(.x = Low_to_Open_20, .f = ~ mean(.x, na.rm = T) ,.before = 50),
         Low_to_Open_lead_1 = abs(lead(Low) - lead(Open)),
         Low_to_Open_lead_10 = abs(lead(Low, 10) - lead(Open)),
         Low_to_Open_lead_20 = abs(lead(Low, 20) - lead(Open)),

         Low_to_Open_5_tan = atan((lag(Low) - lag(Open, 5))/5),
         Low_to_Open_10_tan = atan((lag(Low) - lag(Open, 10))/10),
         Low_to_Open_15_tan = atan((lag(Low) - lag(Open, 15))/15)
  )

training_data_ask <-
  starting_asset_data_ask_daily %>%
  group_by(Asset) %>%
  slice_head(prop = 0.4) %>%
  ungroup()

training_data_bid <-
  starting_asset_data_bid_daily %>%
  group_by(Asset) %>%
  slice_head(prop = 0.4) %>%
  ungroup()

ask_independants <-
  names(starting_asset_data_ask_daily) %>%
  keep(~ str_detect(.x, "High_to_") & !str_detect(.x, "lead"))
bid_independants <-
  names(starting_asset_data_bid_daily) %>%
  keep(~ str_detect(.x, "Low_to_") & !str_detect(.x, "lead"))

lm_form_high <- create_lm_formula(dependant = "High_to_Open_lead_20",
                                  independant = ask_independants
                                  )

lm_form_low <- create_lm_formula(dependant = "Low_to_Open_lead_20",
                                  independant = bid_independants
)

vol_LM_High <- lm(data = training_data_ask,
             formula = lm_form_high)
summary(vol_LM_High)
vol_LM_Low <- lm(data = training_data_bid,
                  formula = lm_form_low)
summary(vol_LM_Low)

pred_high_Lm_ask_train <- predict.lm(vol_LM_High, newdata = training_data_ask) %>% as.numeric()
pred_Low_Lm_bid_train <- predict.lm(vol_LM_Low, newdata = training_data_bid) %>% as.numeric()

training_data_ask <- training_data_ask %>%
  mutate(Pred_High = pred_high_Lm_ask_train)

training_data_bid <- training_data_bid %>%
  mutate(Pred_Low = pred_Low_Lm_bid_train)

training_data_ask_with_bid_pred <- training_data_ask %>%
  left_join(
    training_data_bid %>%
      dplyr::select(Date, Asset,
                    Pred_Low)
  ) %>%
  mutate(
    Pred_Difference = Pred_High - Pred_Low
  ) %>%
  group_by(Asset) %>%
  summarise(
    Pred_Difference_mean = mean(Pred_Difference, na.rm = T),
    Pred_Difference_sd = sd(Pred_Difference, na.rm = T),

    Pred_High_mean = mean(Pred_High, na.rm = T),
    Pred_High_sd = sd(Pred_High, na.rm = T),

    Pred_Low_mean = mean(Pred_Low, na.rm = T),
    Pred_Low_sd = sd(Pred_Low, na.rm = T)
  )

testing_data_ask <-
  starting_asset_data_ask_daily %>%
  group_by(Asset) %>%
  slice_tail(prop = 0.55) %>%
  ungroup()

testing_data_bid <-
  starting_asset_data_bid_daily %>%
  group_by(Asset) %>%
  slice_tail(prop = 0.55) %>%
  ungroup()

pred_high_Lm_ask <- predict.lm(vol_LM_High, newdata = testing_data_ask) %>% as.numeric()
pred_Low_Lm_bid <- predict.lm(vol_LM_Low, newdata = testing_data_bid) %>% as.numeric()

testing_data_ask <- testing_data_ask %>%
  mutate(Pred_High = pred_high_Lm_ask)

testing_data_bid <- testing_data_bid %>%
  mutate(Pred_Low = pred_Low_Lm_bid)

testing_data_ask_with_bid_pred <- testing_data_ask %>%
  mutate(Pred_High = pred_high_Lm_ask) %>%
  left_join(
    testing_data_bid %>%
      dplyr::select(Date, Asset,
                    Pred_Low,
                    Low_to_open_lead_20,
                    Low_Short = Low,
                    High_Short = High)
  ) %>%
  rename(High_Long = High,
         Low_Long = Low) %>%
  mutate(
    Pred_Difference = Pred_High - Pred_Low
  ) %>%
  left_join(training_data_ask_with_bid_pred) %>%
  mutate(
    trade_col =
      case_when(
        # Pred_Difference <= Pred_Difference_mean - Pred_Difference_sd*trade_sd_fac ~ "Trade"
        # Pred_Difference >= Pred_Difference_mean + Pred_Difference_sd*trade_sd_fac ~ "Trade"
        # Pred_High <= Pred_High_mean - Pred_High_sd*trade_sd_fac  ~ "Trade",
        Pred_Low >= Pred_Low_mean + Pred_Low_sd*trade_sd_fac  ~ "Trade"
      )
  )


long_trades <-
  testing_data_ask_with_bid_pred %>%
  filter(!is.na(trade_col)) %>%
  mutate(
    trade_col = "Long"
  )

short_trades <-
  testing_data_ask_with_bid_pred %>%
  filter(!is.na(trade_col)) %>%
  mutate(
    trade_col = "Short"
  )

stop_factor = 12
profit_factor = 24


longs_results_ts <- generic_trade_finder_loop(
  tagged_trades = long_trades %>% rename(High = High_Long, Low=Low_Long),
  asset_data_daily_raw = testing_data_ask,
  stop_factor = stop_factor,
  profit_factor =profit_factor,
  trade_col = "trade_col",
  date_col = "Date",
  start_price_col = "Price",
  mean_values_by_asset =  wrangle_asset_data(
    asset_data_daily_raw = starting_asset_data_ask_daily,
    summarise_means = TRUE
  )
) %>%
  mutate(
    stop_factor = stop_factor,
    profit_factor =profit_factor
  ) %>%
  rename(Asset = asset,
         Date = dates) %>%
  left_join(
    testing_data_ask %>%
      dplyr::select(Asset, Date, Price, Low, High, Open)
  )

longs_results_ts_with_aud <-
  convert_stop_profit_AUD(
  trade_data = longs_results_ts,
  asset_infor = asset_infor,
  asset_col = asset_col,
  stop_col = stop_col,
  profit_col = profit_col,
  price_col = price_col,
  risk_dollar_value = risk_dollar_value,
  returns_present = TRUE,
  trade_return_col = trade_return_col,
  currency_conversion = currency_conversion
)

shorts_results_ts <- generic_trade_finder_loop(
  tagged_trades = short_trades,
  asset_data_daily_raw = testing_data_bid,
  stop_factor = stop_factor,
  profit_factor =profit_factor,
  trade_col = "trade_col",
  date_col = "Date",
  start_price_col = "Price",
  mean_values_by_asset = mean_values_by_asset_for_loop_15_bid
) %>%
  mutate(
    stop_factor = stop_factor,
    profit_factor =profit_factor
  ) %>%
  rename(Asset = asset,
         Date = dates) %>%
  left_join(
    testing_data_bid %>%
      dplyr::select(Asset, Date, Price, Low, High, Open)
  )

short_results_ts_with_aud <-
  convert_stop_profit_AUD(
    trade_data = shorts_results_ts,
    asset_infor = asset_infor,
    asset_col = asset_col,
    stop_col = stop_col,
    profit_col = profit_col,
    price_col = price_col,
    risk_dollar_value = risk_dollar_value,
    returns_present = TRUE,
    trade_return_col = trade_return_col,
    currency_conversion = currency_conversion
  )

combined_results <-
  longs_results_ts_with_aud %>%
  dplyr::select(Asset,
                trade_returns_long = trade_return_dollars_AUD,
                trade_start_prices_long = trade_start_prices,
                trade_end_prices_long = trade_end_prices,
                Date,
                ending_date_trade_long = ending_date_trade) %>%
  left_join(
    short_results_ts_with_aud %>%
      dplyr::select(Asset,
                    trade_returns_short = trade_return_dollars_AUD,
                    trade_start_prices_short = trade_start_prices,
                    trade_end_prices_short = trade_end_prices,
                    Date,
                    ending_date_trade_short = ending_date_trade)
  ) %>%
  mutate(
    trade_returns_aud = trade_returns_long + trade_returns_short
  )

final_Asset <- combined_results %>%
  # group_by(Asset) %>%
  summarise(trade_returns_aud = sum(trade_returns_aud, na.rm= T),
            trade_returns_long = sum(trade_returns_long, na.rm = T),
            trade_returns_short = sum(trade_returns_short, na.rm = T))

loss_analysis <-
  combined_results %>%
  filter(trade_returns_aud < 0)
i = 2

analyse_loss_trade <- testing_data_ask_with_bid_pred %>%
  filter(
    Asset == (loss_analysis$Asset[i] %>% as.character()),
    Date >= (loss_analysis$Date[i] %>% as_datetime())
  ) %>%
  left_join(
    combined_results %>%
      filter(Asset == (loss_analysis$Asset[i] %>% as.character()),
             Date == (loss_analysis$Date[i] %>% as_datetime()))
  ) %>%
  fill(
    matches(names(combined_results)),
    .direction = "down"
  ) %>%
 mutate(
   ending_date_trade_long = as_datetime(ending_date_trade_long, tz = "Australia/Canberra"),
   ending_date_trade_short = as_datetime(ending_date_trade_short, tz = "Australia/Canberra"),
   final_date =
     case_when(
       ending_date_trade_long < ending_date_trade_short ~ ending_date_trade_short,
       ending_date_trade_long > ending_date_trade_short ~ ending_date_trade_long
     )
 ) %>%
  filter(Date <= final_date)

man_colors <-
  c(
    "High_Short" = "red",
    "Low_Short" = "darkred",
    "High_Long" = "green",
    "Low_Long" = "darkgreen"
  )

analyse_loss_trade %>%
  ggplot(aes(x = Date)) +
  geom_line(aes(y = High_Short, color = "High_Short")) +
  geom_line(aes(y = Low_Short, color = "Low_Short")) +
  geom_line(aes(y = High_Long, color = "High_Long")) +
  geom_line(aes(y = Low_Long, color = "Low_Long")) +
  geom_line(aes(y = trade_end_prices_short), color = "black", linetype = "dashed") +
  geom_line(aes(y = trade_end_prices_long), color = "black", linetype = "dashed") +
  scale_color_manual(values = man_colors) +
  theme_minimal() +
  theme(legend.position = "bottom")
