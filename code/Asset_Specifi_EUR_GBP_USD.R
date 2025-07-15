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
db_location <- "C:/Users/nikhi/Documents/Asset Data/Oanda_Asset_Data.db"
start_date = "2016-01-01"
end_date = today() %>% as.character()

EUR_USD_GBP_USD_ALL <- get_EUR_GBP_USD_pairs_data(
  db_location = db_location,
  start_date = "2016-01-01",
  end_date = today() %>% as.character()
)
EUR_USD_GBP_USD <-EUR_USD_GBP_USD_ALL[[1]]
EUR_USD_GBP_USD_short <- EUR_USD_GBP_USD_ALL[[2]]

#---------------------------------------------------------Large LM Sample
load_custom_functions()
EUR_GBP_USD_Trades_long <-
  get_EUR_GBP_Specific_Trades(
    EUR_USD_GBP_USD = EUR_USD_GBP_USD,
    start_date = "2016-01-01",
    raw_macro_data = raw_macro_data,
    lag_days = 4,
    lm_period = 80,
    lm_train_prop = 0.75 ,
    lm_test_prop = 0.25,
    sd_fac_lm_trade_eur_usd = 1,
    sd_fac_lm_trade_gbp_usd = 0.25,
    sd_fac_lm_trade_eur_gbp = 0.25,
    trade_direction = "Long",
    stop_factor = 10,
    profit_factor = 15
  )
EUR_GBP_USD_Trades_long <- EUR_GBP_USD_Trades_long %>%
  map_dfr(bind_rows)
EUR_GBP_USD_Long_Data <-
  run_pairs_analysis(
    tagged_trades = EUR_GBP_USD_Trades_long,
    stop_factor = 10,
    profit_factor = 15,
    raw_asset_data = EUR_USD_GBP_USD,
    risk_dollar_value = 10
  )

results_long <- EUR_GBP_USD_Long_Data[[1]]
results_long_asset <- EUR_GBP_USD_Long_Data[[2]]

EUR_GBP_USD_Trades_short <-
  get_EUR_GBP_Specific_Trades(
    EUR_USD_GBP_USD = EUR_USD_GBP_USD_short,
    start_date = "2016-01-01",
    raw_macro_data = raw_macro_data,
    lag_days = 4,
    lm_period = 80,
    lm_train_prop = 0.75 ,
    lm_test_prop = 0.25,
    sd_fac_lm_trade_eur_usd = 0.25,
    sd_fac_lm_trade_gbp_usd = 3.5,
    sd_fac_lm_trade_eur_gbp = 0.25,
    trade_direction = "Short",
    stop_factor = 12,
    profit_factor = 18
  )
EUR_GBP_USD_Trades_short <- EUR_GBP_USD_Trades_short %>%
  map_dfr(bind_rows)
EUR_GBP_USD_Short_Data <-
  run_pairs_analysis(
    tagged_trades = EUR_GBP_USD_Trades_short,
    stop_factor = 12,
    profit_factor = 18,
    raw_asset_data = EUR_USD_GBP_USD_short,
    risk_dollar_value = 10
  )

results_short <- EUR_GBP_USD_Short_Data[[1]]
results_short_asset <- EUR_GBP_USD_Short_Data[[2]]

#------------------------------------Convert Trades to Stops Profs
get_stops_profs_asset_specific <-
  function(
    trades_to_convert = EUR_GBP_USD_Trades_long,
    raw_asset_data = EUR_USD_GBP_USD,
    currency_conversion = currency_conversion,
    risk_dollar_value = 5

    ) {

    mean_values_by_asset_for_loop =
      wrangle_asset_data(raw_asset_data, summarise_means = TRUE)

    trades_with_stops_profs <-
      EUR_GBP_USD_Trades_long %>%
      left_join(raw_asset_data %>% dplyr::select(Date, Asset, Price, Open, High, Low)) %>%
      slice_max(Date) %>%
      mutate(kk = row_number()) %>%
      split(.$kk) %>%
      map_dfr(
        ~
          get_stops_profs_volume_trades(
            tagged_trades = .x,
            mean_values_by_asset = mean_values_by_asset_for_loop,
            trade_col = "trade_col",
            currency_conversion = currency_conversion,
            risk_dollar_value = risk_dollar_value,
            stop_factor = .x$stop_factor[1] %>% as.numeric(),
            profit_factor = .x$profit_factor[1] %>% as.numeric(),
            asset_col = "Asset",
            stop_col = "stop_value",
            profit_col = "profit_value",
            price_col = "Price",
            trade_return_col = "trade_returns"
          )
      )

    return(trades_with_stops_profs)

}

