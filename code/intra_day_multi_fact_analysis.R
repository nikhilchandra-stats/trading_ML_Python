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
    new_daily_data_ask = new_daily_data_ask,
    currency_conversion = currency_conversion,
    mean_values_by_asset_for_loop = mean_values_by_asset_for_loop_D,
    profit_factor  = 5,
    stop_factor  = 3,
    risk_dollar_value = 5,
    trade_sd_fact = 2
  )

profit_factor  = 5
stop_factor  = 3
risk_dollar_value <- 5

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

#-------------------------------Trade Loop
asset_list_oanda =
  c("USD_JPY", "GBP_JPY", "USD_SGD", "EUR_SEK",
    "DE30_EUR",
    "USD_CHF", "USD_SEK", "XCU_USD", "SUGAR_USD",
    "USD_MXN", "GBP_USD", "WTICO_USD", "EUR_JPY", "USD_NOK",
    "XAU_USD",
    "USD_CZK",  "WHEAT_USD",
    "EUR_USD", "SG30_SGD", "AU200_AUD", "XAG_USD",
    "EUR_GBP", "USD_CNH", "USD_CAD", "NAS100_USD",
    "EU50_EUR", "NATGAS_USD", "SOYBN_USD",
    "US2000_USD",
    "BCO_USD", "AUD_USD", "NZD_USD", "NZD_CHF", "WHEAT_USD",
    "JP225_USD", "SPX500_USD")
end_time <- glue::glue("{floor_date(now(), 'week')} 11:59:00 AEST") %>% as_datetime(tz = "Australia/Canberra") + days(5)
current_time <- now()
trade_taken_this_hour <- 0
data_updated <- 0

margain_threshold <- 0.10
long_account_num <- 1
account_number_long <- "001-011-1615559-001"
account_name_long <- "primary"

db_location = "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data.db"
start_date_day = "2011-01-01"
end_date_day = today() %>% as.character()

H1_model_High_SD_2_65_neg <-
  readRDS(
    "C:/Users/Nikhil Chandra/Documents/trade_data/Holy_GRAIL_MODEL_H1_LM_Markov_NN_2_SD_65Perc.rds"
  )

initial_results <-
  get_NN_trade_from_params(
    Hour_data_with_LM_markov = Hour_data_with_LM_markov,
    sd_value = 2,
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
  )

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

#-----Upload new Data to DB
update_local_db_file(
  db_location = db_location,
  time_frame = "D",
  bid_or_ask = "ask"
)
update_local_db_file(
  db_location = db_location,
  time_frame = "H1",
  bid_or_ask = "ask"
)

while (current_time < end_time) {

  current_time <- now() %>% as_datetime()
  current_minute <- lubridate::minute(current_time)
  current_hour <- lubridate::hour(current_time)
  current_date <- now() %>% as_date(tz = "Australia/Canberra")

  #----------------------Refresh Data Stores and LM model
  if(current_minute > 0 & current_minute < 3 & data_updated == 0) {

    update_local_db_file(
      db_location = db_location,
      time_frame = "D",
      bid_or_ask = "ask",
      asset_list_oanda = asset_list_oanda
    )
    update_local_db_file(
      db_location = db_location,
      time_frame = "H1",
      bid_or_ask = "ask",
      asset_list_oanda = asset_list_oanda
    )

    data_updated <- 1

    raw_macro_data <- get_macro_event_data()

    new_daily_data_ask <-
      updated_data_internal(starting_asset_data = starting_asset_data_ask_daily,
                            end_date_day = current_date,
                            time_frame = "D", bid_or_ask = "ask") %>%
      distinct()
    new_H1_data_ask <-
        updated_data_internal(starting_asset_data = starting_asset_data_ask_H1,
                              end_date_day = current_date,
                              time_frame = "H1", bid_or_ask = "ask")%>%
      distinct()

    Hour_data_with_LM <-
        run_LM_join_to_H1(
          daily_data_internal = new_daily_data_ask,
          H1_data_internal = new_H1_data_ask,
          raw_macro_data = raw_macro_data,
          AUD_exports_total = AUD_exports_total,
          USD_exports_total = USD_exports_total,
          eur_data = eur_data
        )

     Hour_data_with_LM_markov <-
        extract_required_markov_data(
          Hour_data_with_LM = Hour_data_with_LM,
          new_daily_data_ask = new_daily_data_ask,
          currency_conversion = currency_conversion,
          mean_values_by_asset_for_loop = mean_values_by_asset_for_loop_D,
          profit_factor  = 5,
          stop_factor  = 3,
          risk_dollar_value = 5,
          trade_sd_fact = 2
        )

     trades_1 <-
       get_NN_trade_from_params(
       Hour_data_with_LM_markov = Hour_data_with_LM_markov,
       sd_value = 2,
       direction = "negative",
       train_prop = 0.4,
       test_prop = 0.55,
       nn_model_for_trades = H1_model_High_SD_2_65_neg,
       profit_factor  = 14,
       stop_factor  = 14,
       risk_dollar_value = 3.5
      )

    }


  if(current_minute > 30 &  current_minute < 40 & data_updated == 1) {data_updated <- 0}

}

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

#----------------------------------------------------------------BEST CONDITION
#----------------------------------------------------------------BEST CONDITION
#----------------------------------------------------------------BEST CONDITION
#---------------------------------Winning Condiditions:
#----------Results 67% Win Rate, 13% Risk Weighted Return, final = 22405.79
#----------Lowest: -129.093
#----------Trades = 17k
#This Trading route does the opposite of the model and succeeds. It has more
#trades and taken in conjunction with the first model we can get a continuous
#trading model.
#Params: train = 0.4, test = 0.55, hidden = 20, threshold = 0.04, stepmax = 20000,
# SD = 2,  predicted < Average_NN_Pred - SD_NN_Pred*2 ~ "Long"
# profit_factor  = 14
# stop_factor  = 14

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
    new_daily_data_ask = new_daily_data_ask,
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

# saveRDS(
#   H1_model_High,
#   "C:/Users/Nikhil Chandra/Documents/trade_data/H1_LM_Markov_NN_2_SD_65Perc.rds"
# )

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
        predicted < Average_NN_Pred - SD_NN_Pred*2 ~ "Long"
      )
  ) %>%
  filter(!is.na(trade_col))

profit_factor  = 14
stop_factor  = 14
# profit_factor  = 9
# stop_factor  = 9
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
