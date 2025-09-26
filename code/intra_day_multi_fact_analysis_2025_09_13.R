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
  keep(~ str_detect(.x, "AUD")|str_detect(.x, "USD_SEK|USD_NOK|USD_HUF|USD_ZAR|USD_CNY|USD_MXN|USD_CNH"))
asset_infor <- get_instrument_info()
aud_assets <- read_all_asset_data_intra_day(
  asset_list_oanda = all_aud_symbols,
  save_path_oanda_assets = "C:/Users/Nikhil Chandra/Documents/Asset Data/oanda_data/",
  read_csv_or_API = "API",
  time_frame = "D",
  bid_or_ask = "bid",
  how_far_back = 10,
  start_date = (today() - days(5)) %>% as.character()
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

#-------------------------------Trade Loop
asset_list_oanda =
  c("XAG_USD", "XAG_EUR", "XAG_CAD", "XAG_AUD", "XAG_GBP", "XAG_JPY", "XAG_SGD", "XAG_CHF",
    "XAG_NZD",
    "XAU_USD", "XAU_EUR", "XAU_CAD", "XAU_AUD", "XAU_GBP", "XAU_JPY", "XAU_SGD", "XAU_CHF",
    "XAU_NZD",
    "BTC_USD", "LTC_USD", "BCH_USD",
    "US30_USD", "FR40_EUR", "US2000_USD", "CH20_CHF", "SPX500_USD", "AU200_AUD",
    "JP225_USD", "JP225Y_JPY", "SG30_SGD", "EU50_EUR", "HK33_HKD",
    "USB02Y_USD", "USB05Y_USD", "USB30Y_USD", "USB10Y_USD", "UK100_GBP",
    "AUD_USD", "EUR_USD", "GBP_USD", "USD_CHF", "USD_JPY", "USD_MXN", "USD_SEK", "USD_NOK",
    "NZD_USD", "USD_CAD", "USD_SGD", "ETH_USD", "XPT_USD", "XPD_USD",
    "USD_JPY", "GBP_JPY", "USD_SGD", "EUR_SEK",
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
    "JP225_USD", "SPX500_USD") %>%
  unique()

end_time <- glue::glue("{floor_date(now(), 'week')} 23:59:00 AEST") %>% as_datetime(tz = "Australia/Canberra") + days(5)
current_time <- now()
trade_taken_this_hour <- 0
data_updated <- 0

margain_threshold <- 0.01
long_account_num <- 1
account_number_long <- "001-011-1615559-001"
account_name_long <- "primary"

short_account_num <- 3
account_number_short <- "001-011-1615559-004"
account_name_short <- "corr_no_macro"

long_account_num_equity <- 4
account_number_long_equity <- "001-011-1615559-002"
account_name_long_equity <- "equity_long"

short_account_num_equity <- 5
account_number_short_equity <- "001-011-1615559-005"
account_name_short_equity <- "equity_short"

db_location = "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data_Most_Assets_2025-09-13.db"
start_date_day = "2011-01-01"
end_date_day = today() %>% as.character()

#-----Upload new Data to DB
update_local_db_file(
  db_location = db_location,
  time_frame = "D",
  bid_or_ask = "ask",
  how_far_back = 15
)
update_local_db_file(
  db_location = db_location,
  time_frame = "H1",
  bid_or_ask = "ask",
  how_far_back = 15
)

update_local_db_file(
  db_location = db_location,
  time_frame = "D",
  bid_or_ask = "bid",
  asset_list_oanda = asset_list_oanda,
  how_far_back = 15
)
update_local_db_file(
  db_location = db_location,
  time_frame = "H1",
  bid_or_ask = "bid",
  asset_list_oanda = asset_list_oanda,
  how_far_back = 15
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
    start_date = "2016-01-01",
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
    start_date = "2016-01-01",
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

# all_trade_ts_actuals <-
#   get_Major_Indices_trade_ts_actuals(
#     full_ts_trade_db_location = "C:/Users/Nikhil Chandra/Documents/trade_data/full_ts_trades_mapped.db",
#     asset_infor = asset_infor,
#     currency_conversion = currency_conversion,
#     stop_factor_var = 4,
#     profit_factor_var = 8
#   )

all_trade_ts_actuals_Logit <-
  get_ts_trade_actuals_Logit_NN(
    full_ts_trade_db_location = "C:/Users/Nikhil Chandra/Documents/trade_data/full_ts_trades_mapped_AUD_USD.db"
  ) %>%
  bind_rows(
    get_ts_trade_actuals_Logit_NN(
      full_ts_trade_db_location = "C:/Users/Nikhil Chandra/Documents/trade_data/full_ts_trades_mapped_ALL_EUR_USD_JPY_GBP.db"
    )
  )%>%
  bind_rows(
    get_ts_trade_actuals_Logit_NN(
      full_ts_trade_db_location = "C:/Users/Nikhil Chandra/Documents/trade_data/full_ts_trades_mapped.db"
    )
  )

all_trade_ts_actuals_Logit1 <-
  all_trade_ts_actuals_Logit %>%
  filter(profit_factor == 8, stop_factor == 4) %>%
  filter(
    asset %in%
      c("EUR_USD", "EUR_GBP", "GBP_USD", "GBP_JPY", "EUR_JPY", "USD_JPY",
        "SPX500_USD", "US2000_USD", "EU50_EUR", "AU200_AUD", "SG30_SGD", "XAG_USD", "XAU_USD",
        "AUD_USD", "NZD_USD", "XCU_USD", "NZD_CHF", "XAG_USD", "XAU_USD",
        "EUR_AUD", "EUR_NZD", "XAG_EUR", "XAU_EUR", "USD_CHF", "XAU_CHF", "XAG_CHF"
        )
  )

all_trade_ts_actuals_Logit2 <-
  all_trade_ts_actuals_Logit %>%
  filter(profit_factor == 4, stop_factor == 2) %>%
  filter(
    asset %in%
      c("EUR_USD", "GBP_USD",
        "SPX500_USD", "US2000_USD", "EU50_EUR", "AU200_AUD", "SG30_SGD", "XAG_USD", "XAU_USD",
        "UK100_GBP", "JP225Y_JPY", "FR40_EUR", "CH20_CHF", "USB10Y_USD", "USB02Y_USD", "UK10YB_GBP",
        "HK33_HKD"
      )
  )

all_trade_ts_actuals_Logit <-
  all_trade_ts_actuals_Logit1 %>%
  bind_rows(all_trade_ts_actuals_Logit2)

rm(all_trade_ts_actuals_Logit1, all_trade_ts_actuals_Logit2)

all_trade_ts_actuals_Logit <- all_trade_ts_actuals_Logit %>%
  filter(dates >= "2016-01-01")

gc()


#------------------------------------------------------------Loop
#------------------------------------------------------------
while (current_time < end_time) {

  current_time <- now() %>% as_datetime()
  current_minute <- lubridate::minute(current_time)
  current_hour <- lubridate::hour(current_time)
  current_date <- now() %>% as_date(tz = "Australia/Canberra")

  #----------------------Refresh Data Stores and LM model
  if(current_minute > 0 & current_minute < 7 & data_updated == 0) {

    update_local_db_file(
      db_location = db_location,
      time_frame = "D",
      bid_or_ask = "ask",
      asset_list_oanda = asset_list_oanda,
      how_far_back = 3
    )
    update_local_db_file(
      db_location = db_location,
      time_frame = "H1",
      bid_or_ask = "ask",
      asset_list_oanda = asset_list_oanda,
      how_far_back = 3
    )

    update_local_db_file(
      db_location = db_location,
      time_frame = "D",
      bid_or_ask = "bid",
      asset_list_oanda = asset_list_oanda,
      how_far_back = 3
    )
    update_local_db_file(
      db_location = db_location,
      time_frame = "H1",
      bid_or_ask = "bid",
      asset_list_oanda = asset_list_oanda,
      how_far_back = 3
    )

    data_updated <- 1

    raw_macro_data <- get_macro_event_data()

    new_daily_data_ask <-
      updated_data_internal(starting_asset_data = starting_asset_data_ask_daily,
                            end_date_day = now() + days(1),
                            time_frame = "D",
                            bid_or_ask = "ask",
                            db_location = "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data_Most_Assets_2025-09-13.db") %>%
      distinct()

    new_H1_data_ask <-
      updated_data_internal(starting_asset_data = starting_asset_data_ask_H1,
                            end_date_day = now() + days(1),
                            time_frame = "H1",
                            bid_or_ask = "ask",
                            db_location = "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data_Most_Assets_2025-09-13.db")%>%
      distinct()


    new_daily_data_bid <-
      updated_data_internal(starting_asset_data = starting_asset_data_bid_daily,
                            end_date_day = now() + days(1),
                            time_frame = "D",
                            bid_or_ask = "bid",
                            db_location = "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data_Most_Assets_2025-09-13.db") %>%
      distinct()
    new_H1_data_bid <-
      updated_data_internal(starting_asset_data = starting_asset_data_bid_H1,
                            end_date_day = now() + days(1),
                            time_frame = "H1",
                            bid_or_ask = "bid",
                            db_location = "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data_Most_Assets_2025-09-13.db")%>%
      distinct()

    Hour_data_with_LM_ask <-
      run_LM_join_to_H1(
        daily_data_internal = new_daily_data_ask %>%
          filter(Asset %in%   c("USD_JPY", "GBP_JPY", "USD_SGD", "EUR_SEK",
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
                                "JP225_USD", "SPX500_USD")),
        H1_data_internal = new_H1_data_ask %>%
          filter(Asset %in%   c("USD_JPY", "GBP_JPY", "USD_SGD", "EUR_SEK",
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
                                "JP225_USD", "SPX500_USD")),
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

    # Hour_data_with_LM_bid <-
    #   run_LM_join_to_H1(
    #     daily_data_internal = new_daily_data_bid,
    #     H1_data_internal = new_H1_data_bid,
    #     raw_macro_data = raw_macro_data,
    #     AUD_exports_total = AUD_exports_total,
    #     USD_exports_total = USD_exports_total,
    #     eur_data = eur_data
    #   )

    # Hour_data_with_LM_markov_bid <-
    #   extract_required_markov_data(
    #     Hour_data_with_LM = Hour_data_with_LM_bid,
    #     new_daily_data_ask = new_daily_data_bid,
    #     currency_conversion = currency_conversion,
    #     mean_values_by_asset_for_loop = mean_values_by_asset_for_loop_D_bid,
    #     profit_factor  = 5,
    #     stop_factor  = 3,
    #     risk_dollar_value = 5,
    #     trade_sd_fact = 2
    #   )

    temp_db_con <- connect_db("C:/Users/Nikhil Chandra/Documents/trade_data/NN_simulation_results 2025-05-24 Trading Copy.db")
    assets_in_db <-
      DBI::dbGetQuery(conn = temp_db_con,
                      statement =  "SELECT * FROM Simulation_Results_Asset")  %>%
      distinct(Asset)
    DBI::dbDisconnect(temp_db_con)
    rm(temp_db_con)

    temp_db_con <- connect_db("C:/Users/Nikhil Chandra/Documents/trade_data/NN_simulation_results 2025-05-24 50% highProf.db")
    assets_in_db2 <-
      DBI::dbGetQuery(conn = temp_db_con,
                      statement =  "SELECT * FROM Simulation_Results_Asset")  %>%
      distinct(Asset)
    DBI::dbDisconnect(temp_db_con)
    rm(temp_db_con)

    all_assets_present <- assets_in_db2 %>% bind_rows(assets_in_db) %>% distinct(Asset) %>% pull(Asset)

    H1_model_High_SD_25_71_neg <- readRDS(
      glue::glue("C:/Users/Nikhil Chandra/Documents/trade_data/H1_LM_Markov_NN_25_SD_71Perc_2025-05-13.rds")
    )

    H1_LM_Markov_NN_Long_56_prof_10_4sd2025_05_17 <- readRDS(
      glue::glue("C:/Users/Nikhil Chandra/Documents/trade_data/H1_LM_Markov_NN_Long_56_prof_10_4sd2025-05-17.rds")
    )

    H1_LM_Markov_NN_Hidden35 <-
      readRDS(glue::glue("C:/Users/Nikhil Chandra/Documents/trade_data/H1_LM_Markov_NN_Hidden35_withLag_2025-05-17.rds"))

    LM_ML_Lead_5_21_21_5_layer =
      readRDS("C:/Users/Nikhil Chandra/Documents/trade_data/LM_ML_Lead_5_21_21_5_layer_2025-05-20.rds")

    trades_1 <-
      get_NN_best_trades_from_mult_anaysis(
        db_path = "C:/Users/Nikhil Chandra/Documents/trade_data/NN_simulation_results 2025-05-24 Trading Copy.db",
        network_name = "H1_LM_Markov_NN_Long_56_prof_10_4sd2025_05_17",
        NN_model = H1_LM_Markov_NN_Long_56_prof_10_4sd2025_05_17,
        Hour_data_with_LM_markov = Hour_data_with_LM_markov_ask,
        mean_values_by_asset_for_loop_H1 = mean_values_by_asset_for_loop_H1_ask,
        currency_conversion = currency_conversion,
        asset_infor = asset_infor,
        risk_dollar_value = 5,
        win_threshold = 0.2,
        risk_weighted_thresh = 0.3,
        slice_max = TRUE,
        filter_profitbale_assets = TRUE
      )

    trades_1_50 <-
      get_NN_best_trades_from_mult_anaysis(
        db_path = "C:/Users/Nikhil Chandra/Documents/trade_data/NN_simulation_results 2025-05-24 50% highProf.db",
        network_name = "H1_LM_Markov_NN_Long_56_prof_10_4sd2025_05_17",
        NN_model = H1_LM_Markov_NN_Long_56_prof_10_4sd2025_05_17,
        Hour_data_with_LM_markov = Hour_data_with_LM_markov_ask,
        mean_values_by_asset_for_loop_H1 = mean_values_by_asset_for_loop_H1_ask,
        currency_conversion = currency_conversion,
        asset_infor = asset_infor,
        risk_dollar_value = 5,
        win_threshold = 0.2,
        risk_weighted_thresh = 0.3,
        slice_max = TRUE,
        filter_profitbale_assets = TRUE
      )

    if(!is.null(trades_1)) {trades_1 <- trades_1 %>% filter(trade_col == "Long")}
    if(!is.null(trades_1_50)) {trades_1_50 <- trades_1_50 %>% filter(trade_col == "Long")}

    trades_2 <-
      get_NN_best_trades_from_mult_anaysis(
        db_path = "C:/Users/Nikhil Chandra/Documents/trade_data/NN_simulation_results 2025-05-24 Trading Copy.db",
        network_name = "H1_LM_Markov_NN_25_SD_71Perc_2025-05-13",
        NN_model = H1_model_High_SD_25_71_neg,
        Hour_data_with_LM_markov = Hour_data_with_LM_markov_ask,
        mean_values_by_asset_for_loop_H1 = mean_values_by_asset_for_loop_H1_ask,
        currency_conversion = currency_conversion,
        asset_infor = asset_infor,
        risk_dollar_value = 5,
        win_threshold = 0.2,
        risk_weighted_thresh = 0.3,
        slice_max = TRUE,
        filter_profitbale_assets = TRUE
      )

    trades_2_50 <-
      get_NN_best_trades_from_mult_anaysis(
        db_path = "C:/Users/Nikhil Chandra/Documents/trade_data/NN_simulation_results 2025-05-24 50% highProf.db",
        network_name = "H1_LM_Markov_NN_25_SD_71Perc_2025-05-13",
        NN_model = H1_model_High_SD_25_71_neg,
        Hour_data_with_LM_markov = Hour_data_with_LM_markov_ask,
        mean_values_by_asset_for_loop_H1 = mean_values_by_asset_for_loop_H1_ask,
        currency_conversion = currency_conversion,
        asset_infor = asset_infor,
        risk_dollar_value = 5,
        win_threshold = 0.2,
        risk_weighted_thresh = 0.3,
        slice_max = TRUE,
        filter_profitbale_assets = TRUE
      )

    if(!is.null(trades_2)) {trades_2 <- trades_2 %>% filter(trade_col == "Long")}
    if(!is.null(trades_2_50)) {trades_2_50 <- trades_2_50 %>% filter(trade_col == "Long")}

    # trades_3 <-
    #   get_NN_best_trades_from_mult_anaysis(
    #     db_path = "C:/Users/Nikhil Chandra/Documents/trade_data/NN_simulation_results 2025-05-24 Trading Copy.db",
    #     network_name = "H1_LM_Markov_NN_Hidden35",
    #     NN_model = H1_LM_Markov_NN_Hidden35,
    #     Hour_data_with_LM_markov = Hour_data_with_LM_markov_bid,
    #     mean_values_by_asset_for_loop_H1 = mean_values_by_asset_for_loop_H1_bid,
    #     currency_conversion = currency_conversion,
    #     asset_infor = asset_infor,
    #     risk_dollar_value = 5,
    #     win_threshold = 0.54,
    #     risk_weighted_thresh = 0.02,
    #     slice_max = TRUE,
    #     filter_profitbale_assets = TRUE
    #   )
    #
    # if(!is.null(trades_3)) {trades_3 <- trades_3 %>% filter(trade_col == "Short")}

    trades_4 <-
      get_NN_best_trades_from_mult_anaysis(
        db_path = "C:/Users/Nikhil Chandra/Documents/trade_data/NN_simulation_results 2025-05-24 50% highProf.db",
        network_name = "LM_ML_Lead_5_21_21_5_layer",
        NN_model = LM_ML_Lead_5_21_21_5_layer,
        Hour_data_with_LM_markov = Hour_data_with_LM_markov_ask,
        mean_values_by_asset_for_loop_H1 = mean_values_by_asset_for_loop_H1_ask,
        currency_conversion = currency_conversion,
        asset_infor = asset_infor,
        risk_dollar_value = 5,
        win_threshold = 0.2,
        risk_weighted_thresh = 0.2,
        slice_max = TRUE,
        filter_profitbale_assets = TRUE
      )

    if(!is.null(trades_4)) {trades_4 <- trades_4 %>% filter(trade_col == "Long")}

    rm(Hour_data_with_LM_ask, Hour_data_with_LM_markov_ask)
    rm(H1_model_High_SD_25_71_neg, H1_LM_Markov_NN_Long_56_prof_10_4sd2025_05_17,
       H1_LM_Markov_NN_Hidden35,  LM_ML_Lead_5_21_21_5_layer)
    gc()
    Sys.sleep(2)

    # commod_log_cumulative <-
    #   c("BCO_USD", "WTICO_USD" ,"NATGAS_USD", "SOYBN_USD", "SUGAR_USD", "WHEAT_USD", "XAG_USD", "XAU_USD", "XCU_USD") %>%
    #   map_dfr(
    #     ~
    #       create_log_cumulative_returns(
    #         asset_data_to_use =
    #           new_H1_data_ask %>%
    #           filter(Asset %in% c("BCO_USD", "WTICO_USD" ,"NATGAS_USD", "SOYBN_USD", "SUGAR_USD", "WHEAT_USD", "XAG_USD", "XAU_USD", "XCU_USD")),
    #         asset_to_use = c(.x[1]),
    #         price_col = "Open",
    #         return_long_format = TRUE
    #       )
    #   ) %>%
    #   left_join(
    #     new_H1_data_ask %>%
    #       filter(Asset %in% c("BCO_USD", "WTICO_USD" ,"NATGAS_USD", "SOYBN_USD", "SUGAR_USD", "WHEAT_USD", "XAG_USD", "XAU_USD", "XCU_USD")) %>%
    #       distinct(Date, Asset, Price, Open, High, Low)
    #   )
    #
    # commod_trades <- commod_model_trades_diff_vers(
    #   commod_data = commod_log_cumulative ,
    #   PCA_Data = NULL,
    #   assets_to_use = c("BCO_USD", "WTICO_USD" ,"NATGAS_USD", "SOYBN_USD", "SUGAR_USD", "WHEAT_USD", "XAG_USD", "XAU_USD", "XCU_USD"),
    #   samples_for_MLE = 0.5,
    #   test_samples = 0.45,
    #   rolling_period = 100,
    #   date_filter_min = "2011-01-01",
    #   stop_factor = 12,
    #   profit_factor = 15
    # )
    #
    # commod_trades_dfr <- commod_trades %>%
    #   map_dfr(bind_rows) %>%
    #   group_by(Asset) %>%
    #   slice_max(Date) %>%
    #   ungroup() %>%
    #   dplyr::select(Date, Asset, Price, Open, High, Low, trade_col) %>%
    #   mutate(
    #     stop_factor = 12,
    #     profit_factor = 15
    #   ) %>%
    #   get_stops_profs_asset_specific(
    #     raw_asset_data =
    #       new_H1_data_bid %>%
    #       filter(Asset %in% c("BCO_USD", "WTICO_USD" ,"NATGAS_USD", "SOYBN_USD", "SUGAR_USD", "WHEAT_USD", "XAG_USD", "XAU_USD", "XCU_USD")),
    #     currency_conversion = currency_conversion,
    #     risk_dollar_value = 5
    #   ) %>%
    #   filter(Date >= ((now() %>% as_datetime()) - minutes(60)))
    #
    # rm(commod_log_cumulative)
    # gc()

    trades1_dim <- ifelse(!is.null(trades_1), dim(trades_1)[1], 0)
    trades2_dim <- ifelse(!is.null(trades_2), dim(trades_2)[1], 0)
    # trades3_dim <- ifelse(!is.null(trades_3), dim(trades_3)[1], 0)
    trades4_dim <- ifelse(!is.null(trades_4), dim(trades_4)[1], 0)
    # commod_dim <- ifelse(!is.null(commod_trades_dfr), dim(commod_trades_dfr)[1], 0)
    trades1_50_dim <- ifelse(!is.null(trades_1_50), dim(trades_1_50)[1], 0)

    all_trades_string <-
      glue::glue("Trades 1: {trades1_dim}, Trades 2: {trades2_dim}, Trades 4: {trades4_dim}, Trades 1_50: {trades1_50_dim}") %>%
      as.character()

    message(all_trades_string)

    # all_trades_string <-
    #   glue::glue("Trades COMMOD Long and Short Trades: {commod_dim}") %>%
    #   as.character()

    message(all_trades_string)

    total_trades <- trades_1 %>%
      bind_rows(trades_2) %>%
      # bind_rows(trades_3) %>%
      bind_rows(trades_2_50) %>%
      bind_rows(trades_1_50) %>%
      bind_rows(trades_4)

    if(dim(total_trades)[1] > 0 & !is.null(total_trades)) {

      total_trades <-
        total_trades %>%
        filter(Asset %in% all_assets_present) %>%
        filter(stop_factor < profit_factor) %>%
        distinct()

      if(dim(total_trades)[1] > 0 & !is.null(total_trades)) {
        total_trades <-
          total_trades %>%
          group_by(Asset) %>%
          slice_max(risk_weighted_returns) %>%
          group_by(Asset) %>%
          mutate(xx = row_number()) %>%
          group_by(Asset) %>%
          slice_min(xx) %>%
          ungroup() %>%
          dplyr::select(-xx)
      }

    }

    total_trades <-
      list(total_trades
           # commod_trades_dfr
      ) %>%
      map_dfr(bind_rows)

    if(dim(total_trades)[1] > 0) {

      get_oanda_account_number(account_name = account_name_long)
      current_trades_long <- get_list_of_positions(account_var = long_account_num)
      current_trades_long <- current_trades_long %>%
        mutate(direction = stringr::str_to_title(direction)) %>%
        rename(Asset = instrument )

      current_trades_short <- get_list_of_positions(account_var = short_account_num)
      current_trades_short <- current_trades_short %>%
        mutate(direction = stringr::str_to_title(direction)) %>%
        rename(Asset = instrument )

      total_trades <- total_trades %>%
        filter( (abs(volume_required) >= 0.1 &
                   Asset %in% c("SPX500_USD", "JP225_USD", "EU50_EUR", "US2000_USD", "SG30_SGD", "AU200_AUD",
                                "NAS100_USD", "DE30_EUR", "HK33_HKD")) |
                  (abs(volume_required) >= 1 &
                     !(Asset %in% c("SPX500_USD", "JP225_USD", "EU50_EUR", "US2000_USD", "SG30_SGD", "AU200_AUD",
                                    "NAS100_USD", "DE30_EUR", "HK33_HKD"))
                  )
        ) %>%
        slice_max(Date)

      current_trades <- current_trades_long %>%
        bind_rows(current_trades_short) %>%
        mutate(trade_col = direction)

      if(dim(total_trades)[1] > 0) {

        total_trades <-
          total_trades %>%
          left_join(current_trades) %>%
          mutate(
            std_units = case_when(
              # type == "CURRENCY" ~ units/1000,
              Asset %in% c("WHEAT_USD", "SOYBN_USD", "XCU_USD", "XAG_USD") ~ units*100,
              Asset %in% c("SG30_SGD", "SPX500_USD", "AU200_AUD",
                           "JP225_USD", "DE30_EUR", "US2000_USD", "XAU_USD") ~ units*1000,
              TRUE ~ units
            )
          ) %>%
          mutate(
            too_many_positions =
              case_when(
                !(Asset %in% c(
                  "SPX500_USD", "JP225_USD", "EU50_EUR", "US2000_USD", "SG30_SGD", "AU200_AUD",
                  "NAS100_USD", "DE30_EUR", "HK33_HKD", "XAG_USD", "XCU_USD", "XAU_USD", "BCO_USD",
                  "SUGAR_USD", "WHEAT_USD", "FR40_EUR","CN50_USD", "USB10Y_USD", "NAS100_USD", "CORN_USD",
                  "US30_USD", "WTICO_USD"
                )) & abs(units) >= 11000 ~ TRUE,
                Asset %in% c("SPX500_USD", "JP225_USD", "EU50_EUR", "US2000_USD", "SG30_SGD", "AU200_AUD",
                             "NAS100_USD", "DE30_EUR", "NAS100_USD", "HK33_HKD", "US30_USD") & abs(units) >= 4 ~ TRUE,
                Asset == "XCU_USD" & abs(units)>=700 ~ TRUE,
                Asset == "WHEAT_USD" & abs(units)>=200 ~ TRUE,
                Asset == "CORN_USD" & abs(units)>=200 ~ TRUE,
                Asset == "NATGAS_USD" & abs(units)>=200 ~ TRUE,
                Asset == "XAG_USD" & abs(units)>=110 ~ TRUE,
                Asset %in% c("BCO_USD", "WTICO_USD") & abs(units)>=45 ~ TRUE,
                TRUE ~ FALSE
              )
          ) %>%
          arrange(std_units) %>%
          filter(too_many_positions == FALSE) %>%
          dplyr::select(-std_units, -direction, -units, -too_many_positions)

      }

      if(dim(total_trades)[1] > 0) {

        for (i in 1:dim(total_trades)[1]) {

          account_details_long <- get_account_summary(account_var = long_account_num)
          margain_available_long <- account_details_long$marginAvailable %>% as.numeric()
          margain_used_long <- account_details_long$marginUsed%>% as.numeric()
          total_margain_long <- margain_available_long + margain_used_long
          percentage_margain_available_long <- margain_available_long/total_margain_long

          account_details_short <- get_account_summary(account_var = short_account_num)
          margain_available_short <- account_details_short$marginAvailable %>% as.numeric()
          margain_used_short <- account_details_short$marginUsed%>% as.numeric()
          total_margain_short <- margain_available_short + margain_used_short
          percentage_margain_available_short <- margain_available_short/total_margain_short

          Sys.sleep(1)

          trade_direction <- total_trades$trade_col[i] %>% as.character()
          asset <- total_trades$Asset[i] %>% as.character()
          volume_trade <- total_trades$volume_required[i] %>% as.numeric()
          volume_trade <- ifelse(trade_direction == "Short" & volume_trade > 0, -1*volume_trade, volume_trade)
          volume_trade <- ifelse(trade_direction == "Long" & volume_trade < 0, -1*volume_trade, volume_trade)

          loss_var <- total_trades$stop_value[i] %>% as.numeric()
          profit_var <- total_trades$profit_value[i] %>% as.numeric()

          if(loss_var > 9) { loss_var <- round(loss_var)}
          if(profit_var > 9) { profit_var <- round(profit_var)}

          if(percentage_margain_available_long[1] > margain_threshold & trade_direction == "Long") {

            volume_trade <- ifelse(volume_trade < 0, -1*volume_trade, volume_trade)

            # This is misleading because it is price distance and not pip distance
            http_return <- oanda_place_order_pip_stop(
              asset = asset,
              volume = volume_trade,
              stopLoss = loss_var,
              takeProfit = profit_var,
              type = "MARKET",
              timeinForce = "FOK",
              acc_name = account_name_long,
              position_fill = "OPEN_ONLY" ,
              price
            )

          }

          if(percentage_margain_available_short[1] > margain_threshold & trade_direction == "Short") {

            volume_trade <- ifelse(volume_trade > 0, -1*volume_trade, volume_trade)

            # This is misleading because it is price distance and not pip distance
            http_return <- oanda_place_order_pip_stop(
              asset = asset,
              volume = volume_trade,
              stopLoss = loss_var,
              takeProfit = profit_var,
              type = "MARKET",
              timeinForce = "FOK",
              acc_name = account_name_short,
              position_fill = "OPEN_ONLY" ,
              price
            )

          }

        }

      }

    }

    Sys.sleep(5)

    major_indices_log_cumulative <-
      c("SPX500_USD", "US2000_USD", "NAS100_USD", "SG30_SGD", "AU200_AUD", "EU50_EUR", "DE30_EUR") %>%
      map_dfr(
        ~
          create_log_cumulative_returns(
            asset_data_to_use =
              new_H1_data_ask %>%
              filter(Asset %in% c("SPX500_USD", "US2000_USD", "NAS100_USD", "SG30_SGD", "AU200_AUD", "EU50_EUR", "DE30_EUR")),
            asset_to_use = c(.x[1]),
            price_col = "Open",
            return_long_format = TRUE
          )
      ) %>%
      left_join(
        new_H1_data_ask %>%
          filter(Asset %in% c("SPX500_USD", "US2000_USD", "NAS100_USD", "SG30_SGD", "AU200_AUD", "EU50_EUR", "DE30_EUR")) %>%
          distinct(Date, Asset, Price, Open, High, Low)
      )

    all_tagged_trades_equity <-
      equity_index_asset_model_trades(
        major_indices_log_cumulative = major_indices_log_cumulative ,
        PCA_Data = NULL,
        assets_to_use = c("SPX500_USD", "US2000_USD", "NAS100_USD", "SG30_SGD", "AU200_AUD", "EU50_EUR", "DE30_EUR"),
        samples_for_MLE = 0.5,
        test_samples = 0.45,
        rolling_period = 100,
        date_filter_min = "2018-01-01",
        stop_factor = 4,
        profit_factor = 8,
        stop_factor_long = 10,
        profit_factor_long = 15
      )

    all_tagged_trades_equity2 <-
      equity_index_asset_model_trades_diff_vers(
        major_indices_log_cumulative = major_indices_log_cumulative ,
        PCA_Data = NULL,
        assets_to_use = c("SPX500_USD", "US2000_USD", "NAS100_USD", "SG30_SGD", "AU200_AUD", "EU50_EUR", "DE30_EUR"),
        samples_for_MLE = 0.5,
        test_samples = 0.45,
        rolling_period = 100,
        date_filter_min = "2018-01-01",
        stop_factor = 4,
        profit_factor = 8,
        stop_factor_long = 4,
        profit_factor_long = 8
      )

    all_tagged_trades_equity_dfr1 <-
      all_tagged_trades_equity %>%
      map_dfr(bind_rows) %>%
      slice_max(Date) %>%
      ungroup() %>%
      dplyr::select(Date, Asset ,Price, Open, High, Low, trade_col) %>%
      mutate(
        stop_factor =
          case_when(
            (Asset == "US2000_USD" & trade_col == "Long") ~ 10,
            TRUE ~ 4
          ),
        profit_factor =
          case_when(
            (Asset == "US2000_USD" & trade_col == "Long") ~ 15,
            TRUE ~ 8
          )
      ) %>%
      filter(Date >= (now() - minutes(75)) )

    all_tagged_trades_equity_dfr2 <-
      all_tagged_trades_equity2 %>%
      map_dfr(bind_rows) %>%
      slice_max(Date) %>%
      ungroup() %>%
      dplyr::select(Date, Asset ,Price, Open, High, Low, trade_col) %>%
      mutate(
        stop_factor =4,
        profit_factor =8
      ) %>%
      filter(Date >= (now() - minutes(75)) )

    rm(major_indices_log_cumulative)
    gc()
    Sys.sleep(2)

    copula_data_Indices_Silver <-
      create_NN_Idices_Silver_H1Vers_data(
        SPX_US2000_XAG =
          new_H1_data_ask %>%
          filter(Date >= "2019-01-01") %>%
          filter(Asset %in% c("EUR_USD", "GBP_USD",
                              "SPX500_USD", "US2000_USD", "EU50_EUR", "AU200_AUD", "SG30_SGD", "XAG_USD", "XAU_USD",
                              "UK100_GBP", "JP225Y_JPY", "FR40_EUR", "CH20_CHF", "USB10Y_USD", "USB02Y_USD", "UK10YB_GBP",
                              "HK33_HKD"
          )),
        raw_macro_data = raw_macro_data,
        actual_wins_losses = all_trade_ts_actuals_Logit %>%
          filter(asset %in%
                   c("EUR_USD", "GBP_USD",
                     "SPX500_USD", "US2000_USD", "EU50_EUR", "AU200_AUD", "SG30_SGD", "XAG_USD", "XAU_USD",
                     "UK100_GBP", "JP225Y_JPY", "FR40_EUR", "CH20_CHF", "USB10Y_USD", "USB02Y_USD", "UK10YB_GBP",
                     "HK33_HKD"
                   )
                 ) %>%
          filter(profit_factor == 4, stop_factor == 2) %>%
          filter(dates >= "2019-01-01"),
        lag_days = 1,
        stop_value_var = 2,
        profit_value_var = 4,
        use_PCA_vars = FALSE
      )

    gc()

    Indices_Silver_LOGIT<- get_Logit_trades(
      logit_path_save_path = "C:/Users/Nikhil Chandra/Documents/trade_data/asset_specific_NNs/",
      Logit_sims_db = "C:/Users/Nikhil Chandra/Documents/trade_data/Indices_Silver_Logit_sims.db",
      copula_data = copula_data_Indices_Silver,
      sim_min = 30,
      edge_min = 0,
      stop_var = 2,
      profit_var = 4,
      outperformance_count_min = 0.5,
      risk_weighted_return_mid_min =  0.05,
      sim_table = "Indices_Silver_Logit_sims",
      combined_filter_n = 3
    )

    Indices_Silver_LOGIT_short <- get_Logit_trades(
      logit_path_save_path = "C:/Users/Nikhil Chandra/Documents/trade_data/asset_specific_NNs/",
      Logit_sims_db = "C:/Users/Nikhil Chandra/Documents/trade_data/Indices_Silver_Logit_sims_short.db",
      copula_data = copula_data_Indices_Silver,
      sim_min = 30,
      edge_min = 0,
      stop_var = 2,
      profit_var = 4,
      outperformance_count_min = 0.5,
      risk_weighted_return_mid_min =  0.08,
      sim_table = "Indices_Silver_Logit_sims",
      combined_filter_n = 5
    )

    rm(copula_data_Indices_Silver)
    gc()
    Sys.sleep(5)


    copula_data_AUD_USD_NZD <-
      create_NN_AUD_USD_XCU_NZD_data(
        AUD_USD_NZD_USD =
          new_H1_data_ask %>%
          filter(Date >= "2019-01-01") %>%
          filter(Asset %in% c("AUD_USD", "NZD_USD", "XCU_USD", "NZD_CHF", "XAG_USD", "XAU_USD", "XAU_AUD","XAG_AUD",
                              "EUR_AUD", "EUR_NZD", "XAG_EUR", "XAU_EUR", "USD_CHF", "XAU_CHF", "XAG_CHF")),
        raw_macro_data = raw_macro_data,
        actual_wins_losses =
          all_trade_ts_actuals_Logit %>%
          filter(asset %in% c("AUD_USD", "NZD_USD", "XCU_USD", "NZD_CHF", "XAG_USD", "XAU_USD",
                              "EUR_AUD", "EUR_NZD", "XAG_EUR", "XAU_EUR", "USD_CHF", "XAU_CHF", "XAG_CHF")) %>%
          filter(profit_factor == 8, stop_factor == 4) %>%
          filter(dates >= "2019-01-01"),
        lag_days = 1,
        stop_value_var = 4,
        profit_value_var = 8,
        use_PCA_vars = FALSE
      )

    gc()

    AUD_USD_NZD_LOGIT_trades<- get_Logit_trades(
      logit_path_save_path = "C:/Users/Nikhil Chandra/Documents/trade_data/asset_specific_NNs/",
      Logit_sims_db = "C:/Users/Nikhil Chandra/Documents/trade_data/AUD_USD_NZD_XCU_Logit_sims.db",
      copula_data = copula_data_AUD_USD_NZD,
      sim_min = 80,
      edge_min = 0.1,
      stop_var = 4,
      profit_var = 8,
      outperformance_count_min = 0.55,
      risk_weighted_return_mid_min =  0.12,
      sim_table = "AUD_USD_NZD_XCU_NN_sims",
      combined_filter_n = 4
    )

    AUD_USD_NZD_LOGIT_trades_short<-
      get_Logit_trades(
        logit_path_save_path = "C:/Users/Nikhil Chandra/Documents/trade_data/asset_specific_NN_short/",
        Logit_sims_db =  "C:/Users/Nikhil Chandra/Documents/trade_data/AUD_USD_NZD_XCU_Logit_sims_short.db",
        copula_data = copula_data_AUD_USD_NZD,
        sim_min = 30,
        edge_min = 0,
        stop_var = 4,
        profit_var = 8,
        outperformance_count_min = 0.51,
        risk_weighted_return_mid_min =  0.09,
        sim_table = "AUD_USD_NZD_XCU_NN_sims",
        combined_filter_n = 4
      )

    rm(copula_data_AUD_USD_NZD)
    gc()
    Sys.sleep(4)

    copula_data_EUR_GBP_JPY_USD <-
      create_NN_EUR_GBP_JPY_USD_data(
        EUR_USD_JPY_GBP =
          new_H1_data_ask %>%
          filter(Date >= "2016-01-01") %>%
          filter(Asset %in%  c("EUR_USD", "EUR_GBP", "GBP_USD", "GBP_JPY", "EUR_JPY", "USD_JPY")),
        raw_macro_data = raw_macro_data,
        actual_wins_losses =
          all_trade_ts_actuals_Logit %>%
          filter(asset %in%  c("EUR_USD", "EUR_GBP", "GBP_USD", "GBP_JPY", "EUR_JPY", "USD_JPY")) %>%
          filter(profit_factor == 8, stop_factor == 4) %>%
          filter(dates >= "2016-01-01"),
        lag_days = 1,
        stop_value_var = 4,
        profit_value_var = 8,
        use_PCA_vars = FALSE
      )


    EUR_GBP_JPY_LOGIT<-
      get_Logit_trades(
        logit_path_save_path = "C:/Users/Nikhil Chandra/Documents/trade_data/asset_specific_NNs/",
        Logit_sims_db =  "C:/Users/Nikhil Chandra/Documents/trade_data/EUR_USD_JPY_GBP_Logit_sims.db",
        copula_data = copula_data_EUR_GBP_JPY_USD,
        sim_min = 20,
        edge_min = 0,
        stop_var = 4,
        profit_var = 8,
        outperformance_count_min = 0.51,
        risk_weighted_return_mid_min =  0.1,
        sim_table = "EUR_GBP_JPY_USD_XCU_NN_sims",
        combined_filter_n = 4
      )

    EUR_GBP_JPY_LOGIT_short<-
      get_Logit_trades(
        logit_path_save_path = "C:/Users/Nikhil Chandra/Documents/trade_data/asset_specific_NN_short/",
        Logit_sims_db =  "C:/Users/Nikhil Chandra/Documents/trade_data/EUR_USD_JPY_GBP_Logit_sims_short.db",
        copula_data = copula_data_EUR_GBP_JPY_USD,
        sim_min = 20,
        edge_min = 0,
        stop_var = 4,
        profit_var = 8,
        outperformance_count_min = 0.51,
        risk_weighted_return_mid_min =  0.1,
        sim_table = "EUR_GBP_JPY_USD_XCU_NN_sims",
        combined_filter_n = 4
      )

    rm(copula_data_EUR_GBP_JPY_USD)
    gc()

    EUR_GBP_JPY_LOGIT_short_trades <-
      EUR_GBP_JPY_LOGIT_short %>%
      filter(pred >= pred_min) %>%
      dplyr::select(-pred, -pred_min) %>%
      group_by(Asset) %>%
      slice_max(Date) %>%
      ungroup()
    # filter(Date >= (now() - minutes(75)) )

    EUR_GBP_JPY_LOGIT_trades <-
      EUR_GBP_JPY_LOGIT %>%
      filter(pred >= pred_min) %>%
      dplyr::select(-pred, -pred_min)%>%
      group_by(Asset) %>%
      slice_max(Date) %>%
      ungroup()
    # filter(Date >= (now() - minutes(75)) )

    AUD_USD_NZD_LOGIT_trades <-
      AUD_USD_NZD_LOGIT_trades %>%
      filter(pred >= pred_min) %>%
      dplyr::select(-pred, -pred_min)%>%
      group_by(Asset) %>%
      slice_max(Date) %>%
      ungroup()
    # filter(Date >= (now() - minutes(75)) )

    AUD_USD_NZD_LOGIT_short_trades <-
      AUD_USD_NZD_LOGIT_trades_short %>%
      filter(pred >= pred_min) %>%
      dplyr::select(-pred, -pred_min)%>%
      group_by(Asset) %>%
      slice_max(Date) %>%
      ungroup()
    # filter(Date >= (now() - minutes(75)) )

    Indices_Silver_LOGIT_trades <- Indices_Silver_LOGIT %>%
      filter(pred >= pred_min) %>%
      dplyr::select(-pred, -pred_min)%>%
      group_by(Asset) %>%
      slice_max(Date) %>%
      ungroup()
    # filter(Date >= (now() - minutes(75)) )

    Indices_Silver_LOGIT_short_trades <-
      Indices_Silver_LOGIT_short %>%
      filter(pred >= pred_min) %>%
      dplyr::select(-pred, -pred_min)%>%
      group_by(Asset) %>%
      slice_max(Date) %>%
      ungroup()
    # filter(Date >= (now() - minutes(75)) )

    log_cumulative <-
      c("EU50_EUR", "AU200_AUD" ,"WTICO_USD",
        "SPX500_USD", "US2000_USD", "EUR_GBP",
        "EUR_USD", "EUR_JPY", "GBP_JPY", "USD_CNH",
        "GBP_USD", "USD_CHF", "USD_CAD", "USD_MXN", "USD_SEK",
        "USD_NOK", "EUR_SEK", "AUD_USD", "NZD_USD", "NZD_CHF",
        "SG30_SGD", "XAG_USD",
        # "XCU_USD",
        "USD_SGD", "USD_CZK",
        "NATGAS_USD") %>%
      map_dfr(
        ~
          create_log_cumulative_returns(
            asset_data_to_use = new_H1_data_ask,
            asset_to_use = c(.x[1]),
            price_col = "Open",
            return_long_format = TRUE
          )
      ) %>%
      left_join(
        new_H1_data_ask %>% distinct(Date, Asset, Price, Open, High, Low)
      )

    fib_trades_1 <-
      get_best_pivots_fib_trades(
        .data = log_cumulative %>% filter(Date >= (current_date - days(1500)) ),
        sims_db=
          "C:/Users/Nikhil Chandra/Documents/trade_data/SUP_RES_PERC_MODEL_TRADES.db",
        risk_weighted_return_min = 0.2,
        Trades_min =2000,
        Trades_max = 20000,
        currency_conversion = currency_conversion,
        trade_type = NULL,
        how_far_back_var = NULL
      )  %>%
      map_dfr(bind_rows) %>%
      group_by(Asset) %>%
      slice_min(profit_factor) %>%
      ungroup() %>%
      dplyr::select(Date, Asset, Price, Open, High, Low, trade_col, profit_factor, stop_factor)

    # fib_trades_2 <-
    #   get_best_pivots_fib_trades(
    #     .data = log_cumulative %>% filter(Date >= (current_date - days(1500)) ),
    #     sims_db=
    #       "C:/Users/Nikhil Chandra/Documents/trade_data/SUP_RES_PERC_MODEL_TRADES.db",
    #     risk_weighted_return_min = 0.2,
    #     Trades_min =2000,
    #     Trades_max = 20000,
    #     currency_conversion = currency_conversion,
    #     trade_type = "Line10_Neg",
    #     how_far_back_var = c(1000, 250)
    #   )  %>%
    #   map_dfr(bind_rows) %>%
    #   group_by(Asset) %>%
    #   slice_min(profit_factor) %>%
    #   ungroup() %>%
    #   dplyr::select(Date, Asset, Price, Open, High, Low, trade_col, profit_factor, stop_factor)

    fib_trades <-
      list(fib_trades_1
           # fib_trades_2
      ) %>%
      map_dfr(bind_rows) %>%
      distinct() %>%
      ungroup() %>%
      filter(!is.na(trade_col)) %>%
      group_by(Asset) %>%
      slice_max(Date) %>%
      ungroup()
    # filter(Date >= (now() - minutes(150)) )

    if(dim(fib_trades)[1] > 0) {
      fib_trades <- fib_trades %>%
        group_by(Asset) %>%
        slice_min(profit_factor) %>%
        ungroup()
    }

    rm(log_cumulative)
    gc()

    all_tagged_trades_equity_dfr <-
      list(
        # all_tagged_trades_equity_dfr1,
        all_tagged_trades_equity_dfr2,
        fib_trades,
        Indices_Silver_LOGIT_trades,
        Indices_Silver_LOGIT_short_trades,
        AUD_USD_NZD_LOGIT_trades,
        AUD_USD_NZD_LOGIT_short_trades,
        EUR_GBP_JPY_LOGIT_short_trades,
        EUR_GBP_JPY_LOGIT_trades) %>%
      map_dfr(bind_rows) %>%
      filter(
        # !(Asset %in% c("EUR_JPY", "GBP_JPY") & trade_col == "Long")
      )

    message(glue::glue("Equity Trades: {dim(all_tagged_trades_equity_dfr)[1]}"))

    current_trades_long <- get_list_of_positions(account_var = long_account_num)
    current_trades_long <- current_trades_long %>%
      mutate(direction = stringr::str_to_title(direction)) %>%
      rename(Asset = instrument )

    current_trades_short <- get_list_of_positions(account_var = short_account_num)
    current_trades_short <- current_trades_short %>%
      mutate(direction = stringr::str_to_title(direction)) %>%
      rename(Asset = instrument )

    current_trades_long2 <- get_list_of_positions(account_var = long_account_num_equity)
    current_trades_long2 <- current_trades_long2 %>%
      mutate(direction = stringr::str_to_title(direction)) %>%
      rename(Asset = instrument )

    current_trades_short2 <- get_list_of_positions(account_var = short_account_num_equity)
    current_trades_short2 <- current_trades_short2 %>%
      mutate(direction = stringr::str_to_title(direction)) %>%
      rename(Asset = instrument )

    symbols_to_filter_out <-
      current_trades_long2 %>%
      bind_rows(current_trades_short2) %>%
      # bind_rows(current_trades_long2) %>%
      # bind_rows(current_trades_short2) %>%
      group_by(Asset, direction) %>%
      summarise(
        units = sum(units, na.rm = T)
      ) %>%
      ungroup() %>%
      mutate(
        Keep_Or_Remove =
          case_when(
            !(Asset %in% c(
              "SPX500_USD", "JP225_USD", "EU50_EUR", "US2000_USD", "SG30_SGD", "AU200_AUD",
              "NAS100_USD", "DE30_EUR", "HK33_HKD", "XAG_USD", "XCU_USD", "XAU_USD", "BCO_USD",
              "SUGAR_USD", "WHEAT_USD", "FR40_EUR","CN50_USD", "USB10Y_USD", "NAS100_USD", "CORN_USD",
              "US30_USD", "WTICO_USD"
            )) & abs(units) >= 10000 ~ "Remove",
            Asset %in% c("SPX500_USD", "JP225_USD", "EU50_EUR", "US2000_USD", "AU200_AUD",
                         "NAS100_USD", "DE30_EUR", "NAS100_USD", "HK33_HKD", "US30_USD") & abs(units) >= 3 ~ "Remove",
            Asset == "SG30_SGD" & abs(units) >= 28 ~ "Remove",
            Asset == "XCU_USD" & abs(units)>=450 ~ "Remove",
            Asset == "WHEAT_USD" & abs(units)>=450 ~ "Remove",
            Asset == "CORN_USD" & abs(units)>=450 ~ "Remove",
            Asset == "NATGAS_USD" & abs(units)>=450 ~ "Remove",
            Asset == "XAG_USD" & abs(units)>=100 ~ "Remove",
            Asset %in% c("BCO_USD", "WTICO_USD") & abs(units)>=32 ~ "Remove",
            str_detect(Asset, "XAU_") & abs(units)>=1 ~ "Remove",
            TRUE ~ "Keep"
          )
      )

    if(dim(all_tagged_trades_equity_dfr)[1] > 0 &
       !is.null(all_tagged_trades_equity_dfr) ) {
      all_tagged_trades_equity_dfr_stops_profs <-
        all_tagged_trades_equity_dfr %>%
        ungroup() %>%
        mutate(kk = row_number()) %>%
        split(.$kk) %>%
        map_dfr(
          ~
            get_stops_profs_volume_trades(
              tagged_trades = .x,
              mean_values_by_asset = mean_values_by_asset_for_loop_H1_ask,
              trade_col = "trade_col",
              currency_conversion = currency_conversion,
              risk_dollar_value = 5,
              stop_factor = .x$stop_factor[1] %>% as.numeric(),
              profit_factor = .x$profit_factor[1] %>% as.numeric(),
              asset_col = "Asset",
              stop_col = "stop_value",
              profit_col = "profit_value",
              price_col = "Price",
              trade_return_col = "trade_returns"
            )
        )

      all_tagged_trades_equity_dfr_stops_profs <-
        all_tagged_trades_equity_dfr_stops_profs %>%
        left_join(
          symbols_to_filter_out %>%
            dplyr::select(-units) %>%
            rename(trade_col = direction)
        ) %>%
        mutate(
          Keep_Or_Remove =
            case_when(
              is.na(Keep_Or_Remove) ~ "Keep",
              TRUE ~ Keep_Or_Remove
            )
        ) %>%
        filter(Keep_Or_Remove == "Keep") %>%
        dplyr::select(-Keep_Or_Remove)

    } else {
      all_tagged_trades_equity_dfr_stops_profs <- all_tagged_trades_equity_dfr
    }


    if(dim(all_tagged_trades_equity_dfr_stops_profs)[1] > 0 &
       !is.null(all_tagged_trades_equity_dfr_stops_profs)) {

      for (i in 1:dim(all_tagged_trades_equity_dfr_stops_profs)[1]) {

        account_details_long <- get_account_summary(account_var = long_account_num_equity)
        margain_available_long <- account_details_long$marginAvailable %>% as.numeric()
        margain_used_long <- account_details_long$marginUsed%>% as.numeric()
        total_margain_long <- margain_available_long + margain_used_long
        percentage_margain_available_long <- margain_available_long/total_margain_long

        account_details_short <- get_account_summary(account_var = short_account_num_equity)
        margain_available_short <- account_details_short$marginAvailable %>% as.numeric()
        margain_used_short <- account_details_short$marginUsed%>% as.numeric()
        total_margain_short <- margain_available_short + margain_used_short
        percentage_margain_available_short <- margain_available_short/total_margain_short

        Sys.sleep(1)

        trade_direction <- all_tagged_trades_equity_dfr_stops_profs$trade_col[i] %>% as.character()
        asset <- all_tagged_trades_equity_dfr_stops_profs$Asset[i] %>% as.character()
        volume_trade <- all_tagged_trades_equity_dfr_stops_profs$volume_required[i] %>% as.numeric()
        volume_trade <- ifelse(trade_direction == "Short" & volume_trade > 0, -1*volume_trade, volume_trade)
        volume_trade <- ifelse(trade_direction == "Long" & volume_trade < 0, -1*volume_trade, volume_trade)

        loss_var <- all_tagged_trades_equity_dfr_stops_profs$stop_value[i] %>% as.numeric()
        profit_var <- all_tagged_trades_equity_dfr_stops_profs$profit_value[i] %>% as.numeric()

        if(loss_var > 9) { loss_var <- round(loss_var)}
        if(profit_var > 9) { profit_var <- round(profit_var)}

        if(percentage_margain_available_long[1] > margain_threshold & trade_direction == "Long") {

          volume_trade <- ifelse(volume_trade < 0, -1*volume_trade, volume_trade)

          # This is misleading because it is price distance and not pip distance
          http_return <- oanda_place_order_pip_stop(
            asset = asset,
            volume = volume_trade,
            stopLoss = loss_var,
            takeProfit = profit_var,
            type = "MARKET",
            timeinForce = "FOK",
            acc_name = account_name_long_equity,
            position_fill = "OPEN_ONLY" ,
            price
          )

        }

        if(percentage_margain_available_short[1] > margain_threshold & trade_direction == "Short") {

          volume_trade <- ifelse(volume_trade > 0, -1*volume_trade, volume_trade)

          # This is misleading because it is price distance and not pip distance
          http_return <- oanda_place_order_pip_stop(
            asset = asset,
            volume = volume_trade,
            stopLoss = loss_var,
            takeProfit = profit_var,
            type = "MARKET",
            timeinForce = "FOK",
            acc_name = account_name_short_equity,
            position_fill = "OPEN_ONLY" ,
            price
          )

        }

      }

    }

    gc()

    Sys.sleep(10)

    gc()

  }


  gc()

  if(current_minute > 30 &  current_minute < 55 & data_updated == 1) {data_updated <- 0}

}



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
    new_daily_data_ask = starting_asset_data_ask_daily,
    currency_conversion = currency_conversion,
    mean_values_by_asset_for_loop = mean_values_by_asset_for_loop_D,
    profit_factor  = 5,
    stop_factor  = 3,
    risk_dollar_value = 5,
    trade_sd_fact = 2
  ) %>%
  filter(!is.na(Price_to_Low_lag3), !is.na(Price_to_Low_lead), !is.na(Price_to_High_lag2))

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

H1_model_High <- neuralnet::neuralnet(formula = Price_to_Price_lead ~
                                        Pred_trade + mean_value + mean_value +
                                        sd_value + Total_Avg_Prob_Diff_Low +
                                        Total_Avg_Prob_Diff_High + Total_Avg_Prob_Diff_SD_Low +
                                        Total_Avg_Prob_Diff_SD_High +
                                        Price_to_High_lag + Price_to_Low_lag +
                                        Price_to_High_lag2 + Price_to_Low_lag2 +
                                        Price_to_High_lag3 + Price_to_Low_lag3,
                                      hidden = c(21, 21, 21),
                                      data = H1_Model_data_train,
                                      err.fct = "sse",
                                      linear.output = TRUE,
                                      lifesign = 'full',
                                      rep = 1,
                                      algorithm = "rprop+",
                                      stepmax = 20000,
                                      threshold = 0.03)

saveRDS(
  H1_model_High,
  glue::glue("C:/Users/Nikhil Chandra/Documents/trade_data/LM_ML_Lead_21_21_21_layer_{today()}.rds")
)

# H1_model_High_SD_2_65_neg <-
#   readRDS(
#     "C:/Users/Nikhil Chandra/Documents/trade_data/Holy_GRAIL_MODEL_H1_LM_Markov_NN_2_SD_65Perc.rds"
#   )

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
        # predicted >=  Average_NN_Pred + SD_NN_Pred*3 &
        #   predicted <=  Average_NN_Pred + SD_NN_Pred*500~ "Long"

        predicted >=  Average_NN_Pred - SD_NN_Pred*500 &
          predicted <=  Average_NN_Pred - SD_NN_Pred*4~ "Long"
      )
  ) %>%
  filter(!is.na(trade_col))

profit_factor  = 10
stop_factor  = 10
risk_dollar_value <- 10

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
    Time_Required = median(Time_Required, na.rm = T),
    Totals = n()
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
