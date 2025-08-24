helpeR::load_custom_functions()

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
raw_macro_data <- get_macro_event_data()
#---------------------Data
db_location = "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data For EDA.db"
start_date = "2011-01-01"
end_date = today() %>% as.character()

update_local_db_file(
  db_location = db_location,
  time_frame = "H1",
  bid_or_ask = "ask",
  how_far_back = 26
)

update_local_db_file(
  db_location = db_location,
  time_frame = "H1",
  bid_or_ask = "bid",
  how_far_back = 26
)

AUD_USD_NZD_USD_list <-
  get_all_AUD_USD_specific_data(
    db_location = db_location,
    start_date = start_date,
    end_date = today() %>% as.character(),
    time_frame = "H1"
  )

AUD_NZD_Trades_long <-
  AUD_USD_NZD_USD_list[[1]] %>%
  filter(Date >= "2010-01-01") %>%
  mutate(
    trade_col = "Long"
  )

AUD_NZD_Long_Data_15_20 <-
  run_pairs_analysis(
    tagged_trades = AUD_NZD_Trades_long,
    stop_factor = 15,
    profit_factor = 20,
    raw_asset_data = AUD_USD_NZD_USD_list[[1]],
    risk_dollar_value = 10,
    return_trade_ts = TRUE
  )

AUD_NZD_Long_Data_10_15 <-
  run_pairs_analysis(
    tagged_trades = AUD_NZD_Trades_long,
    stop_factor = 10,
    profit_factor = 15,
    raw_asset_data = AUD_USD_NZD_USD_list[[1]],
    risk_dollar_value = 10,
    return_trade_ts = TRUE
  )

AUD_NZD_Long_Data_8_12 <-
  run_pairs_analysis(
    tagged_trades = AUD_NZD_Trades_long,
    stop_factor = 8,
    profit_factor = 12,
    raw_asset_data = AUD_USD_NZD_USD_list[[1]],
    risk_dollar_value = 10,
    return_trade_ts = TRUE
  )

AUD_NZD_Trades_Short <-
  AUD_USD_NZD_USD_list[[1]] %>%
  filter(Date >= "2010-01-01") %>%
  mutate(
    trade_col = "Short"
  )

AUD_NZD_Short_Data_15_20 <-
  run_pairs_analysis(
    tagged_trades = AUD_NZD_Trades_Short,
    stop_factor = 15,
    profit_factor = 20,
    raw_asset_data = AUD_USD_NZD_USD_list[[1]],
    risk_dollar_value = 10,
    return_trade_ts = TRUE
  )

AUD_NZD_Short_Data_10_15 <-
  run_pairs_analysis(
    tagged_trades = AUD_NZD_Trades_Short,
    stop_factor = 10,
    profit_factor = 15,
    raw_asset_data = AUD_USD_NZD_USD_list[[1]],
    risk_dollar_value = 10,
    return_trade_ts = TRUE
  )

AUD_NZD_Short_Data_8_12 <-
  run_pairs_analysis(
    tagged_trades = AUD_NZD_Trades_Short,
    stop_factor = 8,
    profit_factor = 12,
    raw_asset_data = AUD_USD_NZD_USD_list[[1]],
    risk_dollar_value = 10,
    return_trade_ts = TRUE
  )

full_ts_trade_db_location = "C:/Users/Nikhil Chandra/Documents/trade_data/full_ts_trades_mapped_AUD_USD.db"

AUD_NZD_Trades_Short <-
  AUD_USD_NZD_USD_list[[1]] %>%
  filter(Date >= "2010-01-01") %>%
  mutate(
    trade_col = "Short"
  )

AUD_NZD_Trades_long <-
  AUD_USD_NZD_USD_list[[1]] %>%
  filter(Date >= "2010-01-01") %>%
  mutate(
    trade_col = "Long"
  )

AUD_NZD_Long_Data_5_6 <-
  run_pairs_analysis(
    tagged_trades = AUD_NZD_Trades_long,
    stop_factor = 5,
    profit_factor = 6,
    raw_asset_data = AUD_USD_NZD_USD_list[[1]],
    risk_dollar_value = 10,
    return_trade_ts = TRUE
  )

AUD_NZD_Short_Data_5_6 <-
  run_pairs_analysis(
    tagged_trades = AUD_NZD_Trades_Short,
    stop_factor = 5,
    profit_factor = 6,
    raw_asset_data = AUD_USD_NZD_USD_list[[1]],
    risk_dollar_value = 10,
    return_trade_ts = TRUE
  )

full_data_for_upload <-
  AUD_NZD_Long_Data_5_6 %>%
  mutate(
    stop_factor = 5,
    profit_factor = 6
  ) %>%
  bind_rows(
    AUD_NZD_Short_Data_5_6 %>%
      mutate(
        stop_factor = 5,
        profit_factor = 6
      )
  )

full_ts_trade_db_con <- connect_db(path = full_ts_trade_db_location)
append_table_sql_lite(.data = full_data_for_upload,
                      table_name = "full_ts_trades_mapped",
                      conn = full_ts_trade_db_con)
DBI::dbDisconnect(full_ts_trade_db_con)
rm(full_ts_trade_db_con)
gc()

rm(full_data_for_upload)
gc()

#------------------------------------------------------------------------------
full_ts_trade_db_location = "C:/Users/Nikhil Chandra/Documents/trade_data/full_ts_trades_mapped_AUD_USD.db"

AUD_NZD_Trades_Short <-
  AUD_USD_NZD_USD_list[[1]] %>%
  filter(Date >= "2010-01-01") %>%
  mutate(
    trade_col = "Short"
  )

AUD_NZD_Trades_long <-
  AUD_USD_NZD_USD_list[[1]] %>%
  filter(Date >= "2010-01-01") %>%
  mutate(
    trade_col = "Long"
  )

AUD_NZD_Long_Data_4_8 <-
  run_pairs_analysis(
    tagged_trades = AUD_NZD_Trades_long,
    stop_factor = 4,
    profit_factor = 8,
    raw_asset_data = AUD_USD_NZD_USD_list[[1]],
    risk_dollar_value = 10,
    return_trade_ts = TRUE
  )

AUD_NZD_Short_Data_4_8 <-
  run_pairs_analysis(
    tagged_trades = AUD_NZD_Trades_Short,
    stop_factor = 4,
    profit_factor = 8,
    raw_asset_data = AUD_USD_NZD_USD_list[[1]],
    risk_dollar_value = 10,
    return_trade_ts = TRUE
  )

full_data_for_upload <-
  AUD_NZD_Long_Data_4_8 %>%
  mutate(
    stop_factor = 4,
    profit_factor = 8
  ) %>%
  bind_rows(
    AUD_NZD_Short_Data_4_8 %>%
      mutate(
        stop_factor = 4,
        profit_factor = 8
      )
  )

full_ts_trade_db_con <- connect_db(path = full_ts_trade_db_location)
append_table_sql_lite(.data = full_data_for_upload,
                      table_name = "full_ts_trades_mapped",
                      conn = full_ts_trade_db_con)
DBI::dbDisconnect(full_ts_trade_db_con)
rm(full_ts_trade_db_con)
gc()

rm(full_data_for_upload)
gc()


#------------------------------------------------------------------------------
full_ts_trade_db_location = "C:/Users/Nikhil Chandra/Documents/trade_data/full_ts_trades_mapped_AUD_USD.db"

AUD_NZD_Trades_Short <-
  AUD_USD_NZD_USD_list[[1]] %>%
  filter(Date >= "2010-01-01") %>%
  mutate(
    trade_col = "Short"
  )

AUD_NZD_Trades_long <-
  AUD_USD_NZD_USD_list[[1]] %>%
  filter(Date >= "2010-01-01") %>%
  mutate(
    trade_col = "Long"
  )

AUD_NZD_Long_Data_4_8 <-
  run_pairs_analysis(
    tagged_trades = AUD_NZD_Trades_long,
    stop_factor = 5,
    profit_factor = 8,
    raw_asset_data = AUD_USD_NZD_USD_list[[1]],
    risk_dollar_value = 10,
    return_trade_ts = TRUE
  )

AUD_NZD_Short_Data_4_8 <-
  run_pairs_analysis(
    tagged_trades = AUD_NZD_Trades_Short,
    stop_factor = 5,
    profit_factor = 8,
    raw_asset_data = AUD_USD_NZD_USD_list[[1]],
    risk_dollar_value = 10,
    return_trade_ts = TRUE
  )

full_data_for_upload <-
  AUD_NZD_Long_Data_4_8 %>%
  mutate(
    stop_factor = 5,
    profit_factor = 8
  ) %>%
  bind_rows(
    AUD_NZD_Short_Data_4_8 %>%
      mutate(
        stop_factor = 5,
        profit_factor = 8
      )
  )

full_ts_trade_db_con <- connect_db(path = full_ts_trade_db_location)
append_table_sql_lite(.data = full_data_for_upload,
                      table_name = "full_ts_trades_mapped",
                      conn = full_ts_trade_db_con)
DBI::dbDisconnect(full_ts_trade_db_con)
rm(full_ts_trade_db_con)
gc()

rm(full_data_for_upload)
gc()

#------------------------------------------------------------------------------
full_ts_trade_db_location = "C:/Users/Nikhil Chandra/Documents/trade_data/full_ts_trades_mapped_ALL_EUR_USD_JPY_GBP.db"
EUR_USD_JPY_GBP_list <-
  get_EUR_GBP_USD_pairs_data(
  db_location = db_location,
  start_date = start_date,
  end_date = today() %>% as.character(),
  time_frame = "H1"
)

EUR_USD_JPY_GBP_Trades_Short <-
  EUR_USD_JPY_GBP_list[[1]] %>%
  filter(Date >= "2010-01-01") %>%
  mutate(
    trade_col = "Short"
  )

EUR_USD_JPY_GBP_Trades_long <-
  EUR_USD_JPY_GBP_list[[1]] %>%
  filter(Date >= "2010-01-01") %>%
  mutate(
    trade_col = "Long"
  )

EUR_USD_JPY_GBP_Long_Data_4_8 <-
  run_pairs_analysis(
    tagged_trades = EUR_USD_JPY_GBP_Trades_long,
    stop_factor = 4,
    profit_factor = 8,
    raw_asset_data = EUR_USD_JPY_GBP_list[[1]],
    risk_dollar_value = 10,
    return_trade_ts = TRUE
  )

EUR_USD_JPY_GBP_Short_Data_4_8 <-
  run_pairs_analysis(
    tagged_trades = EUR_USD_JPY_GBP_Trades_Short,
    stop_factor = 4,
    profit_factor = 8,
    raw_asset_data = EUR_USD_JPY_GBP_list[[1]],
    risk_dollar_value = 10,
    return_trade_ts = TRUE
  )

full_data_for_upload <-
  EUR_USD_JPY_GBP_Long_Data_4_8 %>%
  mutate(
    stop_factor = 4,
    profit_factor = 8
  ) %>%
  bind_rows(
    EUR_USD_JPY_GBP_Short_Data_4_8 %>%
      mutate(
        stop_factor = 4,
        profit_factor = 8
      )
  )

full_ts_trade_db_con <- connect_db(path = full_ts_trade_db_location)
write_table_sql_lite(.data = full_data_for_upload,
                      table_name = "full_ts_trades_mapped",
                      conn = full_ts_trade_db_con)
DBI::dbDisconnect(full_ts_trade_db_con)
rm(full_ts_trade_db_con)
gc()

rm(full_data_for_upload)
gc()
