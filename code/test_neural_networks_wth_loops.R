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

#----------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------
#--------------------------------------------Short Neural Network
db_location = "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data.db"
start_date_day = "2011-01-01"
end_date_day = today() %>% as.character()
current_date <- now() %>% as_date(tz = "Australia/Canberra")

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

mean_values_by_asset_for_loop_D_ask =
  wrangle_asset_data(
    asset_data_daily_raw = starting_asset_data_ask_daily,
    summarise_means = TRUE
  )

mean_values_by_asset_for_loop_H1_ask =
  wrangle_asset_data(
    asset_data_daily_raw = starting_asset_data_ask_H1,
    summarise_means = TRUE
  )

starting_asset_data_bid_daily <-
  get_db_price(
    db_location = db_location,
    start_date = start_date_day,
    end_date = end_date_day,
    bid_or_ask = "bid",
    time_frame = "D"
  )

starting_asset_data_bid_H1 <-
  get_db_price(
    db_location = db_location,
    start_date = start_date_day,
    end_date = end_date_day,
    bid_or_ask = "bid",
    time_frame = "H1"
  )

mean_values_by_asset_for_loop_D_bid =
  wrangle_asset_data(
    asset_data_daily_raw = starting_asset_data_bid_daily,
    summarise_means = TRUE
  )

mean_values_by_asset_for_loop_H1_bid =
  wrangle_asset_data(
    asset_data_daily_raw = starting_asset_data_bid_H1,
    summarise_means = TRUE
  )

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

new_daily_data_bid <-
  updated_data_internal(starting_asset_data = starting_asset_data_bid_daily,
                        end_date_day = current_date,
                        time_frame = "D", bid_or_ask = "bid") %>%
  distinct()
new_H1_data_bid <-
  updated_data_internal(starting_asset_data = starting_asset_data_bid_H1,
                        end_date_day = current_date,
                        time_frame = "H1", bid_or_ask = "bid")%>%
  distinct()

Hour_data_with_LM_ask <-
  run_LM_join_to_H1(
    daily_data_internal = new_daily_data_ask,
    H1_data_internal = new_H1_data_ask,
    raw_macro_data = raw_macro_data,
    AUD_exports_total = AUD_exports_total,
    USD_exports_total = USD_exports_total,
    eur_data = eur_data
  )

Hour_data_with_LM_markov_ask <-
  extract_required_markov_data(
    Hour_data_with_LM = Hour_data_with_LM_ask,
    new_daily_data_ask = new_daily_data_ask,
    currency_conversion = currency_conversion,
    mean_values_by_asset_for_loop = mean_values_by_asset_for_loop_D_ask,
    profit_factor  = 5,
    stop_factor  = 3,
    risk_dollar_value = 5,
    trade_sd_fact = 2
  )

Hour_data_with_LM_bid <-
  run_LM_join_to_H1(
    daily_data_internal = new_daily_data_bid,
    H1_data_internal = new_H1_data_bid,
    raw_macro_data = raw_macro_data,
    AUD_exports_total = AUD_exports_total,
    USD_exports_total = USD_exports_total,
    eur_data = eur_data
  )

Hour_data_with_LM_markov_bid <-
  extract_required_markov_data(
    Hour_data_with_LM = Hour_data_with_LM_bid,
    new_daily_data_ask = new_daily_data_bid,
    currency_conversion = currency_conversion,
    mean_values_by_asset_for_loop = mean_values_by_asset_for_loop_D_bid,
    profit_factor  = 5,
    stop_factor  = 3,
    risk_dollar_value = 5,
    trade_sd_fact = 2
  )

H1_Model_data_train_bid <-
  Hour_data_with_LM_markov_bid %>%
  group_by(Asset) %>%
  slice_head(prop = 0.4)

H1_Model_data_test_bid <-
  Hour_data_with_LM_markov_bid %>%
  group_by(Asset) %>%
  slice_tail(prop = 0.55)

H1_Model_data_train_ask <-
  Hour_data_with_LM_markov_ask %>%
  group_by(Asset) %>%
  slice_head(prop = 0.4)

H1_Model_data_test_ask <-
  Hour_data_with_LM_markov_ask %>%
  group_by(Asset) %>%
  slice_tail(prop = 0.55)

#------------------------------------Starting Base Model (Long) (Profit Mult x 1.5)

H1_LM_Markov_NN_25_SD_71_results <-
  simulate_trade_results_multi_param(
    H1_Model_data_train = H1_Model_data_train_ask,
    H1_Model_data_test = H1_Model_data_test_ask,
    NN_results_DB = "C:/Users/Nikhil Chandra/Documents/trade_data/NN_simulation_results 2025-05-24 50% highProf.db",
    Network_Name = "H1_LM_Markov_NN_25_SD_71Perc_2025-05-13",
    NN_Model_path = "C:/Users/Nikhil Chandra/Documents/trade_data/H1_LM_Markov_NN_25_SD_71Perc_2025-05-13.rds",
    risk_dollar_value = 10,
    mean_values_by_asset_for_loop_H1 = mean_values_by_asset_for_loop_H1_ask,
    conditions_Params = NULL,
    trade_direction = "Long",
    write_required = FALSE,
    profit_factor_mult = 1.5
  )


H1_LM_Markov_NN_Long_56_prof_10_4sd2025_05_17_results <-
  simulate_trade_results_multi_param(
    H1_Model_data_train = H1_Model_data_train_ask,
    H1_Model_data_test = H1_Model_data_test_ask,
    NN_results_DB = "C:/Users/Nikhil Chandra/Documents/trade_data/NN_simulation_results 2025-05-24 50% highProf.db",
    Network_Name = "H1_LM_Markov_NN_Long_56_prof_10_4sd2025_05_17",
    NN_Model_path = "C:/Users/Nikhil Chandra/Documents/trade_data/H1_LM_Markov_NN_Long_56_prof_10_4sd2025-05-17.rds",
    risk_dollar_value = 10,
    mean_values_by_asset_for_loop_H1 = mean_values_by_asset_for_loop_H1_ask,
    conditions_Params = NULL,
    trade_direction = "Long",
    write_required = FALSE,
    profit_factor_mult = 1.5
  )

test<- H1_LM_Markov_NN_25_SD_71_results %>%
  pluck(1) %>%
  map_dfr(bind_rows)

test2<- H1_LM_Markov_NN_Long_56_prof_10_4sd2025_05_17_results %>%
  pluck(1) %>%
  map_dfr(bind_rows)

LM_ML_Lead_5_21_21_5_layer_results <-
  simulate_trade_results_multi_param(
    H1_Model_data_train = H1_Model_data_train_ask,
    H1_Model_data_test = H1_Model_data_test_ask,
    NN_results_DB = "C:/Users/Nikhil Chandra/Documents/trade_data/NN_simulation_results 2025-05-24 50% highProf.db",
    Network_Name = "LM_ML_Lead_5_21_21_5_layer",
    NN_Model_path = "C:/Users/Nikhil Chandra/Documents/trade_data/LM_ML_Lead_5_21_21_5_layer_2025-05-20.rds",
    risk_dollar_value = 10,
    mean_values_by_asset_for_loop_H1 = mean_values_by_asset_for_loop_H1_ask,
    conditions_Params = NULL,
    trade_direction = "Long",
    write_required = FALSE,
    profit_factor_mult = 1.5
  )

LM_ML_HighLead_14_14_14_layer_results <-
  simulate_trade_results_multi_param(
    H1_Model_data_train = H1_Model_data_train_ask,
    H1_Model_data_test = H1_Model_data_test_ask,
    NN_results_DB = "C:/Users/Nikhil Chandra/Documents/trade_data/NN_simulation_results 2025-05-24 50% highProf.db",
    Network_Name = "LM_ML_HighLead_14_14_14_layer",
    NN_Model_path = "C:/Users/Nikhil Chandra/Documents/trade_data/LM_ML_HighLead_14_14_14_layer_2025-05-19.rds",
    risk_dollar_value = 10,
    mean_values_by_asset_for_loop_H1 = mean_values_by_asset_for_loop_H1_ask,
    conditions_Params = NULL,
    trade_direction = "Long",
    write_required = FALSE,
    profit_factor_mult = 1.5
  )

H1_LM_Markov_NN_Hidden35_results <-
  simulate_trade_results_multi_param(
    H1_Model_data_train = H1_Model_data_train_bid,
    H1_Model_data_test = H1_Model_data_test_bid,
    NN_results_DB = "C:/Users/Nikhil Chandra/Documents/trade_data/NN_simulation_results 2025-05-24 50% highProf.db",
    Network_Name = "H1_LM_Markov_NN_Hidden35",
    NN_Model_path = "C:/Users/Nikhil Chandra/Documents/trade_data/H1_LM_Markov_NN_Hidden35_withLag_2025-05-17.rds",
    risk_dollar_value = 10,
    mean_values_by_asset_for_loop_H1 = mean_values_by_asset_for_loop_H1_bid,
    conditions_Params = NULL,
    trade_direction = "Long",
    write_required = FALSE,
    profit_factor_mult = 1.5
  )

LM_ML_HighLead_14_14_layer_results <-
  simulate_trade_results_multi_param(
    H1_Model_data_train = H1_Model_data_train_bid,
    H1_Model_data_test = H1_Model_data_test_bid,
    NN_results_DB = "C:/Users/Nikhil Chandra/Documents/trade_data/NN_simulation_results 2025-05-24 50% highProf.db",
    Network_Name = "LM_ML_HighLead_14_14_layer",
    NN_Model_path = "C:/Users/Nikhil Chandra/Documents/trade_data/LM_ML_HighLead_14_14_layer_2025-05-19.rds",
    risk_dollar_value = 10,
    mean_values_by_asset_for_loop_H1 = mean_values_by_asset_for_loop_H1_bid,
    conditions_Params = NULL,
    trade_direction = "Long",
    write_required = FALSE,
    profit_factor_mult = 1.5
  )

test_LM_ML_Lead_5_21_21_5<- LM_ML_Lead_5_21_21_5_layer_results %>%
  pluck(1) %>%
  map_dfr(bind_rows)

test_LM_ML_HighLead_14_14_14__<- LM_ML_HighLead_14_14_14_layer_results %>%
  pluck(1) %>%
  map_dfr(bind_rows)

LM_ML_HighLead_14_14_layer <- LM_ML_HighLead_14_14_layer_results%>%
  pluck(1) %>%
  map_dfr(bind_rows)

#------------------------------------Starting Base Model (Long)

H1_LM_Markov_NN_25_SD_71_results <-
  simulate_trade_results_multi_param(
  H1_Model_data_train = H1_Model_data_train_ask,
  H1_Model_data_test = H1_Model_data_test_ask,
  NN_results_DB = "C:/Users/Nikhil Chandra/Documents/trade_data/NN_simulation_results 2025-05-24 50% highProf.db",
  Network_Name = "H1_LM_Markov_NN_25_SD_71Perc_2025-05-13",
  NN_Model_path = "C:/Users/Nikhil Chandra/Documents/trade_data/H1_LM_Markov_NN_25_SD_71Perc_2025-05-13.rds",
  risk_dollar_value = 10,
  mean_values_by_asset_for_loop_H1 = mean_values_by_asset_for_loop_H1_ask,
  conditions_Params = NULL,
  trade_direction = "Long",
  write_required = FALSE
)


H1_LM_Markov_NN_Long_56_prof_10_4sd2025_05_17_results <-
  simulate_trade_results_multi_param(
    H1_Model_data_train = H1_Model_data_train_ask,
    H1_Model_data_test = H1_Model_data_test_ask,
    NN_results_DB = "C:/Users/Nikhil Chandra/Documents/trade_data/NN_simulation_results 2025-05-24 50% highProf.db",
    Network_Name = "H1_LM_Markov_NN_Long_56_prof_10_4sd2025_05_17",
    NN_Model_path = "C:/Users/Nikhil Chandra/Documents/trade_data/H1_LM_Markov_NN_Long_56_prof_10_4sd2025-05-17.rds",
    risk_dollar_value = 10,
    mean_values_by_asset_for_loop_H1 = mean_values_by_asset_for_loop_H1_ask,
    conditions_Params = NULL,
    trade_direction = "Long",
    write_required = FALSE
  )

test<- H1_LM_Markov_NN_25_SD_71_results %>%
  pluck(1) %>%
  map_dfr(bind_rows)

test2<- H1_LM_Markov_NN_Long_56_prof_10_4sd2025_05_17_results %>%
  pluck(1) %>%
  map_dfr(bind_rows)

LM_ML_Lead_5_21_21_5_layer_results <-
  simulate_trade_results_multi_param(
    H1_Model_data_train = H1_Model_data_train_ask,
    H1_Model_data_test = H1_Model_data_test_ask,
    NN_results_DB = "C:/Users/Nikhil Chandra/Documents/trade_data/NN_simulation_results 2025-05-24 50% highProf.db",
    Network_Name = "LM_ML_Lead_5_21_21_5_layer",
    NN_Model_path = "C:/Users/Nikhil Chandra/Documents/trade_data/LM_ML_Lead_5_21_21_5_layer_2025-05-20.rds",
    risk_dollar_value = 10,
    mean_values_by_asset_for_loop_H1 = mean_values_by_asset_for_loop_H1_ask,
    conditions_Params = NULL,
    trade_direction = "Long",
    write_required = FALSE
  )

LM_ML_HighLead_14_14_14_layer_results <-
  simulate_trade_results_multi_param(
    H1_Model_data_train = H1_Model_data_train_ask,
    H1_Model_data_test = H1_Model_data_test_ask,
    NN_results_DB = "C:/Users/Nikhil Chandra/Documents/trade_data/NN_simulation_results 2025-05-24 50% highProf.db",
    Network_Name = "LM_ML_HighLead_14_14_14_layer",
    NN_Model_path = "C:/Users/Nikhil Chandra/Documents/trade_data/LM_ML_HighLead_14_14_14_layer_2025-05-19.rds",
    risk_dollar_value = 10,
    mean_values_by_asset_for_loop_H1 = mean_values_by_asset_for_loop_H1_ask,
    conditions_Params = NULL,
    trade_direction = "Long",
    write_required = FALSE
  )

test_LM_ML_Lead_5_21_21_5<- LM_ML_Lead_5_21_21_5_layer_results %>%
  pluck(1) %>%
  map_dfr(bind_rows)

test_LM_ML_HighLead_14_14_14__<- LM_ML_HighLead_14_14_14_layer_results %>%
  pluck(1) %>%
  map_dfr(bind_rows)

#------------------------------------Starting Base Model (Short)

H1_LM_Markov_NN_25_SD_71_results_short <-
  simulate_trade_results_multi_param(
    H1_Model_data_train = H1_Model_data_train_bid,
    H1_Model_data_test = H1_Model_data_test_bid,
    NN_results_DB = "C:/Users/Nikhil Chandra/Documents/trade_data/NN_simulation_results 2025-05-24.db",
    Network_Name = "H1_LM_Markov_NN_25_SD_71Perc_2025-05-13",
    NN_Model_path = "C:/Users/Nikhil Chandra/Documents/trade_data/H1_LM_Markov_NN_25_SD_71Perc_2025-05-13.rds",
    risk_dollar_value = 10,
    mean_values_by_asset_for_loop_H1 = mean_values_by_asset_for_loop_H1_bid,
    conditions_Params = NULL,
    trade_direction = "Short",
    write_required = FALSE
  )


H1_LM_Markov_NN_Long_56_prof_10_4sd2025_05_17_results_short <-
  simulate_trade_results_multi_param(
    H1_Model_data_train = H1_Model_data_train_bid,
    H1_Model_data_test = H1_Model_data_test_bid,
    NN_results_DB = "C:/Users/Nikhil Chandra/Documents/trade_data/NN_simulation_results 2025-05-24.db",
    Network_Name = "H1_LM_Markov_NN_Long_56_prof_10_4sd2025_05_17",
    NN_Model_path = "C:/Users/Nikhil Chandra/Documents/trade_data/H1_LM_Markov_NN_Long_56_prof_10_4sd2025-05-17.rds",
    risk_dollar_value = 10,
    mean_values_by_asset_for_loop_H1 = mean_values_by_asset_for_loop_H1_bid,
    conditions_Params = NULL,
    trade_direction = "Short",
    write_required = FALSE
  )

test<- H1_LM_Markov_NN_25_SD_71_results_short %>%
  pluck(1) %>%
  map_dfr(bind_rows)

test2<- H1_LM_Markov_NN_Long_56_prof_10_4sd2025_05_17_results_short %>%
  pluck(1) %>%
  map_dfr(bind_rows)


LM_ML_Lead_5_21_21_5_layer_results_short <-
  simulate_trade_results_multi_param(
    H1_Model_data_train = H1_Model_data_train_bid,
    H1_Model_data_test = H1_Model_data_test_bid,
    NN_results_DB = "C:/Users/Nikhil Chandra/Documents/trade_data/NN_simulation_results 2025-05-24.db",
    Network_Name = "LM_ML_Lead_5_21_21_5_layer",
    NN_Model_path = "C:/Users/Nikhil Chandra/Documents/trade_data/LM_ML_Lead_5_21_21_5_layer_2025-05-20.rds",
    risk_dollar_value = 10,
    mean_values_by_asset_for_loop_H1 = mean_values_by_asset_for_loop_H1_bid,
    conditions_Params = NULL,
    trade_direction = "Short",
    write_required = FALSE
  )

LM_ML_HighLead_14_14_14_layer_results_short <-
  simulate_trade_results_multi_param(
    H1_Model_data_train = H1_Model_data_train_bid,
    H1_Model_data_test = H1_Model_data_test_bid,
    NN_results_DB = "C:/Users/Nikhil Chandra/Documents/trade_data/NN_simulation_results 2025-05-24.db",
    Network_Name = "LM_ML_HighLead_14_14_14_layer",
    NN_Model_path = "C:/Users/Nikhil Chandra/Documents/trade_data/LM_ML_HighLead_14_14_14_layer_2025-05-19.rds",
    risk_dollar_value = 10,
    mean_values_by_asset_for_loop_H1 = mean_values_by_asset_for_loop_H1_bid,
    conditions_Params = NULL,
    trade_direction = "Short",
    write_required = FALSE
  )

test_LM_ML_Lead_5_21_21_5_short<- LM_ML_Lead_5_21_21_5_layer_results_short %>%
  pluck(1) %>%
  map_dfr(bind_rows)

test_LM_ML_HighLead_14_14_14_short<- LM_ML_HighLead_14_14_14_layer_results_short %>%
  pluck(1) %>%
  map_dfr(bind_rows)


H1_LM_Markov_NN_Hidden35_results_short <-
  simulate_trade_results_multi_param(
    H1_Model_data_train = H1_Model_data_train_bid,
    H1_Model_data_test = H1_Model_data_test_bid,
    NN_results_DB = "C:/Users/Nikhil Chandra/Documents/trade_data/NN_simulation_results 2025-05-24.db",
    Network_Name = "H1_LM_Markov_NN_Hidden35",
    NN_Model_path = "C:/Users/Nikhil Chandra/Documents/trade_data/H1_LM_Markov_NN_Hidden35_withLag_2025-05-17.rds",
    risk_dollar_value = 10,
    mean_values_by_asset_for_loop_H1 = mean_values_by_asset_for_loop_H1_bid,
    conditions_Params = NULL,
    trade_direction = "Short",
    write_required = FALSE
  )

LM_ML_HighLead_14_14_layer_results_short <-
  simulate_trade_results_multi_param(
    H1_Model_data_train = H1_Model_data_train_bid,
    H1_Model_data_test = H1_Model_data_test_bid,
    NN_results_DB = "C:/Users/Nikhil Chandra/Documents/trade_data/NN_simulation_results 2025-05-24.db",
    Network_Name = "LM_ML_HighLead_14_14_layer",
    NN_Model_path = "C:/Users/Nikhil Chandra/Documents/trade_data/LM_ML_HighLead_14_14_layer_2025-05-19.rds",
    risk_dollar_value = 10,
    mean_values_by_asset_for_loop_H1 = mean_values_by_asset_for_loop_H1_bid,
    conditions_Params = NULL,
    trade_direction = "Short",
    write_required = FALSE
  )

H1_LM_Markov_NN_Hidden35_short<- H1_LM_Markov_NN_Hidden35_results_short %>%
  pluck(1) %>%
  map_dfr(bind_rows)

LM_ML_HighLead_14_14_layer_short<- LM_ML_HighLead_14_14_layer_results_short %>%
  pluck(1) %>%
  map_dfr(bind_rows)

