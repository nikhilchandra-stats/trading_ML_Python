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

load_custom_functions()
db_location <- "C:/Users/nikhi/Documents/Asset Data/Oanda_Asset_Data EDA.db"
start_date = "2016-01-01"
end_date = today() %>% as.character()

SPX_US2000_XAG_ALL <- get_SPX_US2000_XAG_XAU(
  db_location = db_location,
  start_date = "2011-01-01",
  end_date = today() %>% as.character()
)
SPX_US2000_XAG <-SPX_US2000_XAG_ALL[[1]]
SPX_US2000_XAG_short <- SPX_US2000_XAG_ALL[[2]]

#Beta Binomial - beta(x + a, n - x + b),  x = number of sucesses, a,b hyper priors beta
#
mean(rbeta(n = 900000, shape1 = 5000, shape2 = 5000))
samples <- 1000
random_results_db_location <- "C:/Users/Nikhil Chandra/Documents/trade_data/random_results.db"
db_con <- connect_db(random_results_db_location)
stop_factor = 8
profit_factor = 16
analysis_syms = c("XCU_USD", "NZD_CHF")
trade_samples = 5000
new_table = FALSE
time_frame = "H1"

for (i in 1:samples) {

  temp_results <-
    get_random_results_trades(
      raw_asset_data_ask = AUD_USD_NZD_USD_list[[1]],
      raw_asset_data_bid = AUD_USD_NZD_USD_list[[2]],
      stop_factor = stop_factor,
      profit_factor = profit_factor,
      risk_dollar_value = 10,
      analysis_syms = analysis_syms,
      trade_samples = trade_samples
    )

  complete_results <-
    temp_results[[1]] %>%
    bind_rows(temp_results[[2]]) %>%
    mutate(trade_samples = trade_samples,
           time_frame = time_frame)

  if(new_table == TRUE) {
    write_table_sql_lite(.data = complete_results,
                         table_name = "random_results",
                         conn = db_con,
                         overwrite_true = TRUE)
  }

  if(new_table == FALSE) {
    append_table_sql_lite(
      .data = complete_results,
      table_name = "random_results",
      conn = db_con
    )
  }

}

DBI::dbDisconnect(db_con)

control_random_samples <-
  get_random_samples_MLE_beta(
    random_results_db_location = "C:/Users/Nikhil Chandra/Documents/trade_data/random_results.db",
    stop_factor = 8,
    profit_factor = 16,
    analysis_syms = c("AUD_USD", "NZD_USD", "XCU_USD", "NZD_CHF"),
    time_frame = "H1",
    return_summary = TRUE
  )
#-----------------------------------------------------------

#--------------------------------------------------Analysis
SPX_XAG_US2000_Long_trades <-
  get_SPX_US2000_XAG_Specific_Trades(
  SPX_US2000_XAG = SPX_US2000_XAG,
  raw_macro_data = raw_macro_data,
  lag_days = 1,
  lm_period = 5,
  lm_train_prop = 0.9,
  lm_test_prop = 0.1,
  # lm_train_prop = 0.9,
  # lm_test_prop = 0.08,
  sd_fac_lm_trade_SPX_USD = 1,
  sd_fac_lm_trade_US2000_USD = 1,
  sd_fac_lm_trade_XAG_USD = 1,
  sd_fac_lm_trade_XAU_USD = 1,
  trade_direction = "Long",
  stop_factor = 10,
  profit_factor = 15
  # stop_factor = 10,
  # profit_factor = 15
  )
SPX_XAG_US2000_Long_trades <- SPX_XAG_US2000_Long_trades %>% map_dfr(bind_rows)

SPX_XAG_US2000_Long_Data <-
  run_pairs_analysis(
    tagged_trades = SPX_XAG_US2000_Long_trades,
    # stop_factor = 15,
    # profit_factor = 25,
    stop_factor = 10,
    profit_factor = 15,
    raw_asset_data = SPX_US2000_XAG,
    risk_dollar_value = 10
  )

results_long <- SPX_XAG_US2000_Long_Data[[1]]
results_long_asset <- SPX_XAG_US2000_Long_Data[[2]]

#-------------------------------------------------------
SPX_XAG_US2000_Short_trades <-
  get_SPX_US2000_XAG_Specific_Trades(
    SPX_US2000_XAG = SPX_US2000_XAG_short,
    start_date = "2016-01-01",
    raw_macro_data = raw_macro_data,
    lag_days = 1,
    lm_period = 5,
    lm_train_prop = 0.9,
    lm_test_prop = 0.1,
    # lm_train_prop = 0.9,
    # lm_test_prop = 0.08,
    sd_fac_lm_trade_SPX_USD = 1,
    sd_fac_lm_trade_US2000_USD = 1,
    sd_fac_lm_trade_XAG_USD = 1,
    sd_fac_lm_trade_XAU_USD = 1,
    trade_direction = "Short",
    stop_factor = 10,
    profit_factor = 15
    # stop_factor = 10,
    # profit_factor = 15
  )
SPX_XAG_US2000_Short_trades <- SPX_XAG_US2000_Short_trades %>% map_dfr(bind_rows)


SPX_XAG_US2000_Short_Data <-
  run_pairs_analysis(
    tagged_trades = SPX_XAG_US2000_Short_trades,
    # stop_factor = 15,
    # profit_factor = 25,
    stop_factor = 10,
    profit_factor = 15,
    raw_asset_data = SPX_US2000_XAG_short,
    risk_dollar_value = 5
  )
results_short <- SPX_XAG_US2000_Short_Data[[1]]
results_short_asset <- SPX_XAG_US2000_Short_Data[[2]]
