helpeR::load_custom_functions()

all_aud_symbols <- get_oanda_symbols() %>%
  keep(~ str_detect(.x, "AUD")|str_detect(.x, "USD_SEK|USD_NOK|USD_HUF|USD_ZAR|USD_CNY|USD_MXN"))
asset_infor <- get_instrument_info()
aud_assets <- read_all_asset_data_intra_day(
  asset_list_oanda = all_aud_symbols,
  save_path_oanda_assets = "C:/Users/nikhi/Documents/Asset Data/oanda_data/",
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
    "JP225_USD", "SPX500_USD",
    "EUR_AUD", "EUR_NZD", "EUR_CHF", "ESPIX_EUR" ,"EUR_NZD" ,
    "GBP_AUD", "GBP_NZD", "UK100_GBP", "UK10YB_GBP", "GBP_CHF", "GBP_CAD",
    "NL25_EUR") %>%
  unique()

asset_infor <- get_instrument_info()
raw_macro_data <- get_macro_event_data()


load_custom_functions()
db_location = "C:/Users/nikhi/Documents/Asset Data/Oanda_Asset_Data_Most_Assets_2025-09-13 Third Algo.db"
start_date = "2015-01-01"
end_date = today() %>% as.character()

bin_factor = NULL
stop_value_var = 1
profit_value_var = 15
period_var = 5
full_ts_trade_db_location = "C:/Users/nikhi/Documents/trade_data/full_ts_trades_mapped_period_version_very_fast.db"
full_ts_trade_db_con <- connect_db(path = full_ts_trade_db_location)
actual_wins_losses <-
  DBI::dbGetQuery(full_ts_trade_db_con,
                  glue::glue("SELECT * FROM full_ts_trades_mapped
                  WHERE stop_factor = {stop_value_var} AND
                        periods_ahead = {period_var} AND Date >= {start_date}")
  ) %>%
  mutate(
    Date = as_datetime(Date)
  )

actual_wins_losses <-
  actual_wins_losses %>%
  filter(stop_factor == stop_value_var,
         profit_factor == profit_value_var,
         periods_ahead == period_var) %>%
  group_by(Asset) %>%
  slice_max(Date)

distinct_last_dates <-
  actual_wins_losses %>%
  distinct(Asset, Date) %>%
  rename(
    Last_Date = Date
  )

DBI::dbDisconnect(full_ts_trade_db_con)
rm(full_ts_trade_db_con)
gc()


db_location = "C:/Users/nikhi/Documents/Asset Data/Oanda_Asset_Data_Most_Assets_2025-09-13 Second Algo.db"
full_ts_trade_db_location = "C:/Users/nikhi/Documents/trade_data/full_ts_trades_mapped_period_version_very_fast.db"
start_date = "2025-10-01"
end_date = today() %>% as.character()

Indices_Metals_Bonds <-
  get_Port_Buy_Data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    time_frame = "H1"
  )

Indices_Metals_Bonds <-
  Indices_Metals_Bonds %>%
      map(
        ~ .x %>%
          left_join(distinct_last_dates) %>%
          filter(Date > Last_Date) %>%
          dplyr::select(-Last_Date)
      )

upload_trade_actuals_period_version_to_db(
  asset_data_raw_list = Indices_Metals_Bonds,
  date_filter = start_date,
  stop_factor = 1,
  profit_factor = 15,
  risk_dollar_value = 10,
  periods_ahead = 5,
  append_or_write = "append",
  full_ts_trade_db_location = full_ts_trade_db_location,
  currency_conversion = currency_conversion,
  asset_infor = asset_infor
)
