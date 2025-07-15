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
raw_macro_data <- get_macro_event_data()
#---------------------Data
db_location = "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data For EDA.db"
start_date = "2016-01-01"
end_date = today() %>% as.character()

AUD_USD_NZD_USD_all_data <-
  get_all_AUD_USD_specific_data(
    db_location = "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data For EDA.db",
    start_date = "2016-01-01",
    end_date = today() %>% as.character()
  )

#------------------------------------------------------Test with big LM Prop

AUD_USD_Trades_long <-
  get_AUD_USD_NZD_Specific_Trades(
    AUD_USD_NZD_USD = AUD_USD_NZD_USD_all_data[[1]],
    start_date = "2016-01-01",
    raw_macro_data = raw_macro_data,
    lag_days = 1,
    lm_period = 700,
    lm_train_prop = 0.74,
    lm_test_prop = 0.24,
    sd_fac_lm_trade = 1.5,
    sd_fac_lm_trade2 = 1,
    sd_fac_lm_trade3 = 1,
    trade_direction = "Long",
    stop_factor = 20,
    profit_factor = 30, assets_to_return = c("AUD_USD")
  )

AUD_USD_Trades_long <-
  AUD_USD_Trades_long %>%
  map_dfr(bind_rows)

AUD_USD_Long_Data <-
  run_pairs_analysis(
    tagged_trades = AUD_USD_Trades_long,
    stop_factor = 15,
    profit_factor = 20,
    raw_asset_data = AUD_USD_NZD_USD_all_data[[1]],
    risk_dollar_value = 10
  )

results_long <- AUD_USD_Long_Data[[1]]

NZD_USD_Trades_long <-
  get_AUD_USD_NZD_Specific_Trades(
    AUD_USD_NZD_USD = AUD_USD_NZD_USD_all_data[[1]],
    start_date = "2016-01-01",
    raw_macro_data = raw_macro_data,
    lag_days = 1,
    lm_period = 50,
    lm_train_prop = 0.74,
    lm_test_prop = 0.24,
    sd_fac_lm_trade = 0.5,
    sd_fac_lm_trade2 = 0.5,
    sd_fac_lm_trade3 = 0.5,
    trade_direction = "Long",
    stop_factor = 20,
    profit_factor = 30, assets_to_return = c("NZD_USD")
  )

NZD_USD_Trades_long <-
  NZD_USD_Trades_long %>%
  map_dfr(bind_rows)

NZD_USD_Long_Data <-
  run_pairs_analysis(
    tagged_trades = NZD_USD_Trades_long,
    stop_factor = 15,
    profit_factor = 20,
    raw_asset_data = AUD_USD_NZD_USD_all_data[[1]],
    risk_dollar_value = 10
  )

results_long_NZD_USD <- NZD_USD_Long_Data[[1]]

#-------------------------------------------------------

AUD_NZD_Trades_short <-
  get_AUD_USD_NZD_Specific_Trades(
    AUD_USD_NZD_USD = AUD_USD_NZD_USD_all_data[[2]],
    start_date = "2016-01-01",
    raw_macro_data = raw_macro_data,
    lag_days = 1,
    lm_period = 700,
    lm_train_prop = 0.74,
    lm_test_prop = 0.24,
    sd_fac_lm_trade = 0.75,
    sd_fac_lm_trade2 = 1.25,
    sd_fac_lm_trade3 = 1.25,
    trade_direction = "Short",
    stop_factor = 17,
    profit_factor = 27
  )

AUD_NZD_Trades_short <- AUD_NZD_Trades_short %>%
  map_dfr(bind_rows)

AUD_NZD_Short_Data <-
  run_pairs_analysis(
    tagged_trades = AUD_NZD_Trades_short,
    stop_factor = 15,
    profit_factor = 20,
    raw_asset_data =  AUD_USD_NZD_USD_all_data[[2]],
    risk_dollar_value = 10
  )

results_short <- AUD_NZD_Short_Data[[1]]
results_short2 <- AUD_NZD_Short_Data[[2]]
