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

db_location = "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data_Most_Assets_2025-09-13.db"
start_date = "2024-01-01"
end_date = "2025-11-26"

Indices_Metals_Bonds <-
  get_Port_Buy_Data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    time_frame = "H1"
  )

model_data_store_path <-
  "C:/Users/Nikhil Chandra/Documents/trade_data/single_asset_improved_indcator_trades_ts_more_cop.db"
model_data_store_db <-
  connect_db(model_data_store_path)
gc()

indicator_data <-
  DBI::dbGetQuery(conn = model_data_store_db,
                  statement = "SELECT * FROM single_asset_improved") %>%
  distinct() %>%
  group_by(sim_index, Asset) %>%
  mutate(Date = as_datetime(Date),
         test_date_start = as_date(test_date_start),
         test_end_date = as_date(test_end_date),
         Date_filt = as_date(Date)) %>%
  filter(start_date >= test_date_start)


DBI::dbDisconnect(model_data_store_db)
rm(model_data_store_db)
gc()

pred_thresh <- 0

indicator_data <-
  indicator_data %>%
  group_by(Asset, Date, trade_col) %>%
  slice_max(sim_index) %>%
  ungroup() %>%
  filter(Date >= test_date_start) %>%
  filter(
    (logit_combined_pred >= mean_logit_combined_pred + pred_thresh*sd_logit_combined_pred &
       averaged_pred >=  mean_averaged_pred + sd_averaged_pred*pred_thresh & pred_thresh >= 0)
  ) %>%
  ungroup()

trades_to_tag_with_returns_long <-
  indicator_data %>%
  ungroup() %>%
  distinct(Asset, Date, trade_col) %>%
  filter(Date >= start_date) %>%
  filter(trade_col == "Long")

distinct_assets <-
  indicator_data %>%
  pull(Asset) %>%
  unique()

port_return_list <- list()

for (i in 1:length(distinct_assets)) {

  port_return_list[[i]] <-
    get_portfolio_model(
    asset_data = Indices_Metals_Bonds,
    asset_of_interest = distinct_assets[i],
    stop_factor_long = 2,
    profit_factor_long = 20,
    risk_dollar_value_long = 5,
    end_period = 20,
    time_frame = "H1"
  )

}

port_return_dfr <-
  port_return_list %>%
  map_dfr( ~
             .x %>%
             dplyr::select(adjusted_Date, Asset, Return)
           ) %>%
  ungroup() %>%
  filter(Asset != "BTC_USD") %>%
  group_by(adjusted_Date) %>%
  summarise(Return = sum(Return))


port_return_dfr %>%
  summarise(
    min_portfolio = min(Return, na.rm = T),
    Portfolio_05 = quantile(Return, 0.05, na.rm = T),
    Portfolio_10 = quantile(Return, 0.1, na.rm = T),
    Portfolio_25 = quantile(Return, 0.25, na.rm = T),
    Portfolio_50 = quantile(Return, 0.50, na.rm = T),
    Portfolio_75 = quantile(Return, 0.75, na.rm = T)
  )
