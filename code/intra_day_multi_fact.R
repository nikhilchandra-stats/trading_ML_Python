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

asset_data_ask_daily <-
  get_db_price(
    db_location = db_location,
    start_date = start_date_day,
    end_date = end_date_day,
    bid_or_ask = "ask",
    time_frame = "D"
  )

mean_values_by_asset_for_loop =
  wrangle_asset_data(
    asset_data_daily_raw = asset_data_ask_daily,
    summarise_means = TRUE
  )


reg_data_list <- run_reg_daily_variant(
  raw_macro_data = raw_macro_data,
  eur_data = eur_data,
  AUD_exports_total = AUD_exports_total,
  USD_exports_total = USD_exports_total,
  asset_data_daily_raw = asset_data_ask_daily,
  train_percent = 0.57
)

regression_prediction <- reg_data_list[[2]]

raw_LM_trade_df <- reg_data_list[[2]]

LM_preped <- prep_LM_daily_trade_data(
  asset_data_daily_raw = asset_data_ask_daily,
  raw_LM_trade_df = reg_data_list[[2]],
  raw_LM_trade_df_training = reg_data_list[[3]]
)

trade_with_daily_data <- LM_preped %>% pluck("LM Merged to Daily")
start_date_reg_data <- trade_with_daily_data %>% pull(Date) %>% min()
max_date_reg_data <- trade_with_daily_data %>% pull(Date) %>% max()

start_date_H1 = start_date_reg_data %>% as.character()
end_date_h1 = max_date_reg_data %>% as.character()
Hour_data <-
  get_db_price(
    db_location = db_location,
    start_date = start_date_H1,
    end_date = end_date_h1,
    bid_or_ask = "ask",
    time_frame = "H1"
  )

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

#-----------------------Daily Markov Join

profit_factor  = 5
stop_factor  = 3
risk_dollar_value <- 5
markov_trades_raw <-
  get_markov_tag_pos_neg_diff(
    asset_data_combined = asset_data_ask_daily,
    training_perc = 1,
    sd_divides = seq(0.25,2,0.25),
    quantile_divides = seq(0.1,0.9, 0.1),
    rolling_period = 400,
    markov_col_on_interest_pos = "Markov_Point_Pos_roll_sum_1.5",
    markov_col_on_interest_neg = "Markov_Point_Neg_roll_sum_-1.5",
    sum_sd_cut_off = "",
    profit_factor  = profit_factor,
    stop_factor  = stop_factor,
    asset_data_daily_raw = asset_data_ask_daily,
    mean_values_by_asset_for_loop = mean_values_by_asset_for_loop,
    trade_sd_fact = 2,
    currency_conversion = currency_conversion,
    risk_dollar_value = 5
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
    Price_to_Low_lead =  log(Price/lead(Low))
  )

H1_Model_data_train <-
  Hour_data_with_LM_markov %>%
  group_by(Asset) %>%
  slice_head(prop = 0.1)

H1_Model_data_test <-
  Hour_data_with_LM_markov %>%
  group_by(Asset) %>%
  slice_tail(prop = 0.85)

H1_Model_data_train <-
  Hour_data_with_LM_markov %>%
  group_by(Asset) %>%
  slice_head(prop = 0.4)

H1_Model_data_test <-
  Hour_data_with_LM_markov %>%
  group_by(Asset) %>%
  slice_tail(prop = 0.55)

H1_model_High <- neuralnet::neuralnet(formula = Price_to_High_lead ~
                                        Pred_trade + mean_value + mean_value +
                                        sd_value + Total_Avg_Prob_Diff_Low +
                                        Total_Avg_Prob_Diff_High + Total_Avg_Prob_Diff_SD_Low +
                                        Total_Avg_Prob_Diff_SD_High,
                                      hidden = 20,
                                      data = H1_Model_data_train,
                                      err.fct = "sse",
                                      linear.output = TRUE,
                                      lifesign = 'full',
                                      rep = 1,
                                      algorithm = "rprop+",
                                      stepmax = 20000,
                                      threshold = 0.04)


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
        predicted < Average_NN_Pred - SD_NN_Pred*1 ~ "Long"
      )
  ) %>%
  filter(!is.na(trade_col))

profit_factor  = 10
stop_factor  = 10
risk_dollar_value <- 3.5

long_bayes_loop_analysis <-
  generic_trade_finder_loop(
    tagged_trades = tagged_trades ,
    asset_data_daily_raw = H1_Model_data_test,
    stop_factor = stop_factor,
    profit_factor =profit_factor,
    trade_col = "trade_col",
    date_col = "Date",
    start_price_col = "Price",
    mean_values_by_asset = mean_values_by_asset_for_loop
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

#---------------------------------Winning Condiditions:
#----------Results 56% Win Rate, 13% Risk Weighted Return, final = 16404.63
#----------Lowest: -7735.936
#----------Trades = 16k

# H1_Model_data_train <-
#   Hour_data_with_LM_markov %>%
#   group_by(Asset) %>%
#   slice_head(prop = 0.4)
#
# H1_Model_data_test <-
#   Hour_data_with_LM_markov %>%
#   group_by(Asset) %>%
#   slice_tail(prop = 0.55)

# formula = Price_to_High_lead ~
#   Pred_trade + mean_value + mean_value +
#   sd_value + Total_Avg_Prob_Diff_Low +
#   Total_Avg_Prob_Diff_High + Total_Avg_Prob_Diff_SD_Low +
#   Total_Avg_Prob_Diff_SD_High,
# hidden = 20,
# data = H1_Model_data_train,
# err.fct = "sse",
# linear.output = TRUE,
# lifesign = 'full',
# rep = 1,
# algorithm = "rprop+",
# stepmax = 4000,
# threshold = 0.1

# tagged_trades <-
#   H1_Model_data_test %>%
#   ungroup() %>%
#   mutate(
#     predicted = prediction_nn %>% as.numeric()
#   ) %>%
#   left_join(average_train_predictions) %>%
#   mutate(
#     trade_col =
#       case_when(
#         predicted > Average_NN_Pred + SD_NN_Pred*7 ~ "Long"
#       )
#   ) %>%
#   filter(!is.na(trade_col))
#
# profit_factor  = 5
# stop_factor  = 5
# risk_dollar_value <- 5


#---------------------------------Winning Condiditions:
#----------Results 56% Win Rate, 13% Risk Weighted Return, final = 10500.82
#----------Lowest: -3605.944
#----------Trades = 24k
#This Trading route does the opposite of the model and succeeds. It has more
#trades and taken in conjunction with the first model we can get a continuous
#trading model.

# H1_Model_data_train <-
#   Hour_data_with_LM_markov %>%
#   group_by(Asset) %>%
#   slice_head(prop = 0.4)
#
# H1_Model_data_test <-
#   Hour_data_with_LM_markov %>%
#   group_by(Asset) %>%
#   slice_tail(prop = 0.55)
#
# H1_model_High <- neuralnet::neuralnet(formula = Price_to_High_lead ~
#                                         Pred_trade + mean_value + mean_value +
#                                         sd_value + Total_Avg_Prob_Diff_Low +
#                                         Total_Avg_Prob_Diff_High + Total_Avg_Prob_Diff_SD_Low +
#                                         Total_Avg_Prob_Diff_SD_High,
#                                       hidden = 20,
#                                       data = H1_Model_data_train,
#                                       err.fct = "sse",
#                                       linear.output = TRUE,
#                                       lifesign = 'full',
#                                       rep = 1,
#                                       algorithm = "rprop+",
#                                       stepmax = 20000,
#                                       threshold = 0.04)
#
#
# prediction_nn <- predict(object = H1_model_High, newdata = H1_Model_data_test)
# prediction_nn_train <- predict(object = H1_model_High, newdata = H1_Model_data_train)
# average_train_predictions <-
#   H1_Model_data_train %>%
#   ungroup() %>%
#   mutate(
#     train_predictions = prediction_nn_train %>% as.numeric()
#   ) %>%
#   group_by(Asset) %>%
#   summarise(
#     Average_NN_Pred = mean(train_predictions, na.rm = T),
#     SD_NN_Pred = sd(train_predictions, na.rm = T)
#   ) %>%
#   ungroup()
#
# tagged_trades <-
#   H1_Model_data_test %>%
#   ungroup() %>%
#   mutate(
#     predicted = prediction_nn %>% as.numeric()
#   ) %>%
#   left_join(average_train_predictions) %>%
#   mutate(
#     trade_col =
#       case_when(
#         predicted < Average_NN_Pred - SD_NN_Pred*1 ~ "Long"
#       )
#   ) %>%
#   filter(!is.na(trade_col))
#
# profit_factor  = 3
# stop_factor  = 3
# risk_dollar_value <- 3.5
#
# long_bayes_loop_analysis <-
#   generic_trade_finder_loop(
#     tagged_trades = tagged_trades ,
#     asset_data_daily_raw = H1_Model_data_test,
#     stop_factor = stop_factor,
#     profit_factor =profit_factor,
#     trade_col = "trade_col",
#     date_col = "Date",
#     start_price_col = "Price",
#     mean_values_by_asset = mean_values_by_asset_for_loop
#   )
#
# analysis_data <-
#   generic_anlyser(
#     trade_data = long_bayes_loop_analysis %>% rename(Asset = asset),
#     profit_factor = profit_factor,
#     stop_factor = stop_factor,
#     asset_infor = asset_infor,
#     currency_conversion = currency_conversion,
#     asset_col = "Asset",
#     stop_col = "starting_stop_value",
#     profit_col = "starting_profit_value",
#     price_col = "trade_start_prices",
#     trade_return_col = "trade_returns",
#     risk_dollar_value = risk_dollar_value,
#     grouping_vars = "trade_col"
#   )

#---------------------------------Winning Condiditions:
#----------Results 55% Win Rate, 10% Risk Weighted Return, final = 23354.17
#----------Lowest: -622
#----------Trades = 51,530
#This Trading route does the opposite of the model and succeeds. It has more
#trades and taken in conjunction with the first model we can get a continuous
#trading model.

# H1_Model_data_train <-
#   Hour_data_with_LM_markov %>%
#   group_by(Asset) %>%
#   slice_head(prop = 0.4)
#
# H1_Model_data_test <-
#   Hour_data_with_LM_markov %>%
#   group_by(Asset) %>%
#   slice_tail(prop = 0.55)
#
# H1_model_High <- neuralnet::neuralnet(formula = Price_to_High_lead ~
#                                         Pred_trade + mean_value + mean_value +
#                                         sd_value + Total_Avg_Prob_Diff_Low +
#                                         Total_Avg_Prob_Diff_High + Total_Avg_Prob_Diff_SD_Low +
#                                         Total_Avg_Prob_Diff_SD_High,
#                                       hidden = 20,
#                                       data = H1_Model_data_train,
#                                       err.fct = "sse",
#                                       linear.output = TRUE,
#                                       lifesign = 'full',
#                                       rep = 1,
#                                       algorithm = "rprop+",
#                                       stepmax = 20000,
#                                       threshold = 0.04)
#
#
# prediction_nn <- predict(object = H1_model_High, newdata = H1_Model_data_test)
# prediction_nn_train <- predict(object = H1_model_High, newdata = H1_Model_data_train)
# average_train_predictions <-
#   H1_Model_data_train %>%
#   ungroup() %>%
#   mutate(
#     train_predictions = prediction_nn_train %>% as.numeric()
#   ) %>%
#   group_by(Asset) %>%
#   summarise(
#     Average_NN_Pred = mean(train_predictions, na.rm = T),
#     SD_NN_Pred = sd(train_predictions, na.rm = T)
#   ) %>%
#   ungroup()
#
# tagged_trades <-
#   H1_Model_data_test %>%
#   ungroup() %>%
#   mutate(
#     predicted = prediction_nn %>% as.numeric()
#   ) %>%
#   left_join(average_train_predictions) %>%
#   mutate(
#     trade_col =
#       case_when(
#         predicted < Average_NN_Pred - SD_NN_Pred*1 ~ "Long"
#       )
#   ) %>%
#   filter(!is.na(trade_col))
#
# profit_factor  = 10
# stop_factor  = 10
# risk_dollar_value <- 3.5
#
# long_bayes_loop_analysis <-
#   generic_trade_finder_loop(
#     tagged_trades = tagged_trades ,
#     asset_data_daily_raw = H1_Model_data_test,
#     stop_factor = stop_factor,
#     profit_factor =profit_factor,
#     trade_col = "trade_col",
#     date_col = "Date",
#     start_price_col = "Price",
#     mean_values_by_asset = mean_values_by_asset_for_loop
#   )
#
# analysis_data <-
#   generic_anlyser(
#     trade_data = long_bayes_loop_analysis %>% rename(Asset = asset),
#     profit_factor = profit_factor,
#     stop_factor = stop_factor,
#     asset_infor = asset_infor,
#     currency_conversion = currency_conversion,
#     asset_col = "Asset",
#     stop_col = "starting_stop_value",
#     profit_col = "starting_profit_value",
#     price_col = "trade_start_prices",
#     trade_return_col = "trade_returns",
#     risk_dollar_value = risk_dollar_value,
#     grouping_vars = "trade_col"
#   )
