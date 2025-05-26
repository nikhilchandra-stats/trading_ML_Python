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

#---------------------------------------------Daily Regression Join
db_location = "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data.db"
start_date_day = "2011-01-01"
end_date_day = today() %>% as.character()

starting_asset_data_ask_daily <-
  get_db_price(
    db_location = db_location,
    start_date = start_date_day,
    end_date = end_date_day,
    bid_or_ask = "bid",
    time_frame = "D"
  )

starting_asset_data_ask_H1 <-
  get_db_price(
    db_location = db_location,
    start_date = start_date_day,
    end_date = end_date_day,
    bid_or_ask = "bid",
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

H1_short_model <-
  readRDS(
    "C:/Users/Nikhil Chandra/Documents/trade_data/H1_LM_Markov_NN_Hidden35_2025-05-17.rds"
  )

prediction_nn <- predict(object = H1_short_model, newdata = H1_Model_data_test)
prediction_nn_train <- predict(object = H1_short_model, newdata = H1_Model_data_train)

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
        # predicted >=  Average_NN_Pred + SD_NN_Pred*5 &
        #   predicted <=  Average_NN_Pred + SD_NN_Pred*30~ "Short",

        predicted >=  Average_NN_Pred - SD_NN_Pred*50 &
          predicted <=  Average_NN_Pred - SD_NN_Pred*4 ~ "Short"
      )
  ) %>%
  filter(!is.na(trade_col))

profit_factor  = 10
stop_factor  = 10
risk_dollar_value <- 5

tagged_trades$Asset %>% unique()

long_bayes_loop_analysis <-
  generic_trade_finder_loop(
    tagged_trades = tagged_trades ,
    asset_data_daily_raw = H1_Model_data_test,
    stop_factor = stop_factor,
    profit_factor =profit_factor,
    trade_col = "trade_col",
    date_col = "Date",
    start_price_col = "Price",
    mean_values_by_asset = mean_values_by_asset_for_loop_H1
  )

trade_timings <-
  long_bayes_loop_analysis %>%
  mutate(
    ending_date_trade = as_datetime(ending_date_trade),
    dates = as_datetime(dates)
  ) %>%
  mutate(Time_Required = (ending_date_trade - dates)/ddays(1) )

trade_timings_by_asset <- trade_timings %>%
  mutate(win_loss = ifelse(trade_returns < 0, "loss", "wins") ) %>%
  group_by(asset, win_loss) %>%
  summarise(
    Time_Required = median(Time_Required, na.rm = T)
  ) %>%
  pivot_wider(names_from = win_loss, values_from = Time_Required)

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

analysis_data_asset <-
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
    grouping_vars = "Asset"
  )

long_bayes_loop_analysis_dollar <- long_bayes_loop_analysis %>%
  rename(Asset = asset) %>%
  convert_stop_profit_AUD(
    asset_infor = asset_infor,
    asset_col = "Asset",
    stop_col = "starting_stop_value",
    profit_col = "starting_profit_value",
    price_col = "trade_start_prices",
    risk_dollar_value = risk_dollar_value,
    returns_present = TRUE,
    trade_return_col = "trade_returns",
    currency_conversion = currency_conversion
  ) %>%
  mutate(ending_date_trade = as_datetime(ending_date_trade)) %>%
  group_by(ending_date_trade) %>%
  summarise(trade_return_dollars_AUD  = sum(trade_return_dollars_AUD , na.rm = T),
            total_margain = sum(estimated_margin)) %>%
  arrange(ending_date_trade) %>%
  mutate(cumulative_dollars = cumsum(trade_return_dollars_AUD))

long_bayes_loop_analysis_dollar %>%
  ggplot(aes(x = ending_date_trade, y = cumulative_dollars)) +
  geom_line() +
  theme_minimal()

long_bayes_loop_analysis_dollar %>%
  ggplot(aes(x = ending_date_trade, y = total_margain)) +
  geom_line() +
  theme_minimal()
