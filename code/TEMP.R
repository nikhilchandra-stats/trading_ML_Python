helperfunctions35South::load_custom_functions()
one_drive_path <- helperfunctions35South::create_one_drive_path(
  path_extension = "raw data")
library(neuralnet)
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
  start_date = (today() - days(2)) %>% as.character()
)
aud_assets <- aud_assets %>% map_dfr(bind_rows)
aud_usd_today <- get_aud_conversion(asset_data_daily_raw = aud_assets)
raw_macro_data <- niksmacrohelpers::get_macro_event_data()

currency_conversion <-
  aud_usd_today %>%
  mutate(
    not_aud_asset = ending_value
  ) %>%
  dplyr::select(not_aud_asset, adjusted_conversion) %>%
  bind_rows(
    tibble(not_aud_asset = "AUD", adjusted_conversion = 1)
  )


db_location = "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data For EDA.db"
start_date = "2011-01-01"
end_date = today() %>% as.character()
time_frame = "H1"

asset_data_raw_ask <- get_db_price(
  db_location = db_location,
  start_date = start_date,
  end_date = end_date,
  bid_or_ask = "ask",
  time_frame = "H1"
)

asset_data_raw_bid <- get_db_price(
  db_location = db_location,
  start_date = start_date,
  end_date = end_date,
  bid_or_ask = "bid",
  time_frame = "H1"
)

asset_data_raw_ask$Asset %>% unique()

samples <- 1200
random_results_db_location <- "C:/Users/Nikhil Chandra/Documents/trade_data/random_results.db"
db_con <- connect_db(random_results_db_location)
stop_factor = 12
profit_factor = 40
analysis_syms = c("XAG_USD", "AUD_USD", "EUR_USD", "AU200_AUD", "WHEAT_USD",
                             "USD_CNH", "WTICO_USD", "SOYBN_USD", "SUGAR_USD", "NATGAS_USD",
                             "EUR_SEK", "USD_SGD", "USD_MXN")
trade_samples = 1000
new_table = FALSE
time_frame = "H1"

for (i in 1:samples) {

  temp_results <-
    get_random_results_trades(
      raw_asset_data_ask = asset_data_raw_ask,
      raw_asset_data_bid = asset_data_raw_bid,
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
    stop_factor = 12,
    profit_factor = 40,
    analysis_syms = analysis_syms,
    time_frame = "H1",
    return_summary = TRUE
  )

SPX_bench_mark <-
  get_bench_mark_results(
    asset_data = asset_data_raw_ask %>% filter(Date >= "2018-01-01"),
    Asset_Var = "SPX500_USD",
    min_date = "2018-01-01",
    trade_direction = "Long",
    risk_dollar_value = 5,
    stop_factor = 4,
    profit_factor = 8,
    mean_values_by_asset_for_loop = wrangle_asset_data(asset_data_raw_ask, summarise_means = TRUE),
    currency_conversion = currency_conversion
  )


