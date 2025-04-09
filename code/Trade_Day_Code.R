helpeR::load_custom_functions()
one_drive_path <- helperfunctions35South::create_one_drive_path(
  path_extension = "raw data")
#--------------------------------------Trade Day Volatility
asset_infor <- get_instrument_info()
extracted_asset_data <- list()
asset_list_oanda <- get_oanda_symbols() %>%
  keep( ~ .x %in% c("HK33_HKD", "USD_JPY", "BTC_USD", "AUD_NZD", "GBP_CHF",
                    "EUR_HUF", "EUR_ZAR", "NZD_JPY", "EUR_NZD", "USB02Y_USD",
                    "XAU_CAD", "GBP_JPY", "EUR_NOK", "USD_SGD", "EUR_SEK", "DE30_EUR",
                    "AUD_CAD", "UK10YB_GBP", "XPD_USD", "UK100_GBP", "USD_CHF", "GBP_NZD",
                    "GBP_SGD", "USD_SEK", "EUR_SGD", "XCU_USD", "SUGAR_USD", "CHF_ZAR",
                    "AUD_CHF", "EUR_CHF", "USD_MXN", "GBP_USD", "WTICO_USD", "EUR_JPY", "USD_NOK",
                    "XAU_USD", "DE10YB_EUR", "USD_CZK", "AUD_SGD", "USD_HUF", "WHEAT_USD",
                    "EUR_USD", "SG30_SGD", "GBP_AUD", "NZD_CAD", "AU200_AUD", "XAG_USD",
                    "XAU_EUR", "EUR_GBP", "USD_CNH", "USD_CAD", "NAS100_USD", "USB10Y_USD",
                    "EU50_EUR", "NATGAS_USD", "CAD_JPY", "FR40_EUR", "USD_ZAR", "XAU_GBP",
                    "CH20_CHF", "ESPIX_EUR", "XPT_USD", "EUR_AUD", "SOYBN_USD", "US2000_USD",
                    "BCO_USD")
  )
for (i in 1:length(asset_list_oanda)) {

  extracted_asset_data[[i]] <-
    get_oanda_data_candles_normalised(
      assets = c(asset_list_oanda[i]),
      granularity = "D",
      date_var = "2011-01-01",
      date_var_start = NULL,
      time = "T15:00:00.000000000Z",
      how_far_back = 5000,
      bid_or_ask = "bid",
      sleep_time = 0
    ) %>%
    mutate(
      Asset = asset_list_oanda[i]
    )
}

asset_data_daily_raw <- extracted_asset_data  %>%
  map_dfr(bind_rows)

asset_data_daily_raw_week <- extracted_asset_data  %>%
  map_dfr(~ .x) %>%
  mutate(
    week_date = Date,
    week_start_price = Price,
    weekly_high = High,
    weekly_low = Low
  ) %>%
  dplyr::select(-Date, Price, -Low, -High, -Open)

mean_values_by_asset_for_loop =
  wrangle_asset_data(
    asset_data_daily_raw = asset_data_daily_raw,
    summarise_means = TRUE
  )

trade_params <- read_csv("data/volatility_trade_params.csv")

trades_today <-
  get_volatility_trades(
  asset_data_daily_raw_week = asset_data_daily_raw_week,
  asset_data_daily_raw = asset_data_daily_raw,
  mean_values_by_asset_for_loop = mean_values_by_asset_for_loop,
  sd_facs = 0,
  stop_fac = 0.32,
  prof_fac =0.8,
  risk_dollar_value = 50
)
