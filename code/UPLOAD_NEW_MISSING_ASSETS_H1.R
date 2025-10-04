helpeR::load_custom_functions()
all_aud_symbols <-
  get_oanda_symbols() %>%
  keep(~ str_detect(.x, "AUD")|str_detect(.x, "USD_SEK|USD_NOK|USD_HUF|USD_ZAR|USD_CNY|USD_MXN|USD_CNH"))
asset_infor <- get_instrument_info()
aud_assets <- read_all_asset_data_intra_day(
  asset_list_oanda = all_aud_symbols,
  save_path_oanda_assets = "C:/Users/Nikhil Chandra/Documents/Asset Data/oanda_data/",
  read_csv_or_API = "API",
  time_frame = "D",
  bid_or_ask = "bid",
  how_far_back = 10,
  start_date = (today() - days(30)) %>% as.character()
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

db_location = "C:/Users/nikhi/Documents/Asset Data/Oanda_Asset_Data_missing_Assets.db"

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
    "NZD_USD", "USD_CAD", "USD_SGD", "ETH_USD", "XPT_USD", "XPD_USD")

time_frame = "H1"
bid_or_ask = "bid"
how_far_back = 10
ending_date = "2025-08-28"
starting_date = "2011-01-01"

db_con <- connect_db(path = db_location)
table_name <-
  case_when(
    time_frame == "D" & bid_or_ask == "ask" ~ "Oanda_Asset_Data_ask_D",
    time_frame == "D" & bid_or_ask == "bid" ~ "Oanda_Asset_Data_bid_D",

    time_frame == "H1" & bid_or_ask == "ask" ~ "Oanda_Asset_Data_ask",
    time_frame == "H1" & bid_or_ask == "bid" ~ "Oanda_Asset_Data_bid",

    time_frame == "M15" & bid_or_ask == "ask" ~ "Oanda_Asset_Data_ask_M15",
    time_frame == "M15" & bid_or_ask == "bid" ~ "Oanda_Asset_Data_bid_M15",
  )


current_latest_date <- starting_date
data_to_Update <-
  read_all_asset_data_intra_day(
    asset_list_oanda = asset_list_oanda,
    save_path_oanda_assets = "C:/Users/Nikhil Chandra/Documents/Asset Data/oanda_data/",
    read_csv_or_API = "API",
    time_frame = time_frame,
    bid_or_ask = bid_or_ask,
    how_far_back = 5000,
    start_date = current_latest_date
  )

data_to_Update_dfr <-
  data_to_Update %>%
  map_dfr(bind_rows)

write_table_sql_lite(.data = data_to_Update_dfr,
                     table_name = table_name,
                     conn = db_con)

rm(data_to_Update_dfr, data_to_Update)
gc()

for (j in 1:length(asset_list_oanda)) {

  current_latest_data <-
    DBI::dbGetQuery(conn = db_con,
                  statement = glue::glue("SELECT * FROM {table_name} WHERE Asset = '{asset_list_oanda[j]}'"))

  current_latest_date <-
    current_latest_data %>%
    mutate(Date = as_datetime(Date, tz = "Australia/Canberra")) %>%
    slice_max(Date) %>%
    pull(Date) %>% unique() %>% max() %>% as_date() %>% unique() %>% as.character()

  while ( as_date(current_latest_date) < as_date(ending_date) ) {

    current_latest_data <-
      DBI::dbGetQuery(conn = db_con,
                      statement = glue::glue("SELECT * FROM {table_name} WHERE Asset = '{asset_list_oanda[j]}'"))

    current_latest_date <-
      current_latest_data %>%
      mutate(Date = as_datetime(Date, tz = "Australia/Canberra")) %>%
      slice_max(Date) %>%
      pull(Date) %>% unique() %>% max() %>% as_date() %>% unique() %>% as.character()

    current_latest_date_time <-
      current_latest_data %>%
      mutate(Date = as_datetime(Date, tz = "Australia/Canberra")) %>%
      slice_max(Date) %>%
      pull(Date) %>% pluck(1)

    data_to_Update <-
      read_all_asset_data_intra_day(
        asset_list_oanda = asset_list_oanda[j],
        save_path_oanda_assets = "C:/Users/Nikhil Chandra/Documents/Asset Data/oanda_data/",
        read_csv_or_API = "API",
        time_frame = time_frame,
        bid_or_ask = bid_or_ask,
        how_far_back = 5000,
        start_date = current_latest_date
      )

    data_to_Update_dfr <-
      data_to_Update %>%
      map_dfr(bind_rows) %>%
      filter(Date > current_latest_date_time)

    append_table_sql_lite(.data = data_to_Update_dfr,
                         table_name = table_name,
                         conn = db_con)

    Sys.sleep(1.5)

    current_latest_data <-
      DBI::dbGetQuery(conn = db_con,
                      statement = glue::glue("SELECT * FROM {table_name} WHERE Asset = '{asset_list_oanda[j]}'"))

    current_latest_date <-
      current_latest_data %>%
      mutate(Date = as_datetime(Date, tz = "Australia/Canberra")) %>%
      slice_max(Date) %>%
      pull(Date) %>% unique() %>% max() %>% as_date() %>% unique() %>% as.character()

  }

}

#' mean_values_by_asset_for_loop
#'
#' @param tagged_trades
#' @param stop_factor
#' @param profit_factor
#' @param raw_asset_data
#'
#' @return
#' @export
#'
#' @examples
get_trade_results_ts_aud <- function(
    tagged_trades = SPX_XAG_US2000_Long_trades %>% slice_sample(n = 1000),
    stop_factor = 10,
    profit_factor = 15,
    raw_asset_data = SPX_US2000_XAG,
    risk_dollar_value = 10,
    currency_conversion = currency_conversion
) {

  mean_values_by_asset_for_loop <-
    wrangle_asset_data(
      asset_data_daily_raw = raw_asset_data,
      summarise_means = TRUE
    )

  long_bayes_loop_analysis<-
    generic_trade_finder_loop(
      tagged_trades = tagged_trades ,
      asset_data_daily_raw = raw_asset_data,
      stop_factor = stop_factor,
      profit_factor =profit_factor,
      trade_col = "trade_col",
      date_col = "Date",
      start_price_col = "Price",
      mean_values_by_asset = mean_values_by_asset_for_loop
    )

  trades_with_aud_ts <-
    long_bayes_loop_analysis %>%
    rename(Asset = asset,
           Date = dates) %>%
    mutate(
      stop_factor = stop_factor,
      profit_factor = profit_factor
    ) %>%
    left_join(
      raw_asset_data %>% dplyr::select(Date, Asset, Price , Low, High, Open)
    ) %>%
    get_stops_profs_volume_trades(
      mean_values_by_asset = mean_values_by_asset_for_loop,
      trade_col = "trade_col",
      currency_conversion = currency_conversion,
      risk_dollar_value = risk_dollar_value,
      stop_factor = stop_factor,
      profit_factor = profit_factor,
      asset_col = "Asset",
      stop_col = "stop_value",
      profit_col = "profit_value",
      price_col = "Price",
      trade_return_col = "trade_returns"
    ) %>%
    mutate(
      trade_returns =
        case_when(
          trade_col == "Long" & trade_start_prices > trade_end_prices ~ maximum_win,
          trade_col == "Long" & trade_start_prices <= trade_end_prices ~ minimal_loss,
          trade_col == "Short" & trade_start_prices < trade_end_prices ~ maximum_win,
          trade_col == "Short" & trade_start_prices >= trade_end_prices ~ minimal_loss
        )
    ) %>%
    dplyr::select(Date, Asset, trade_returns, stop_factor, trade_col,
                  profit_factor, volume_required, estimated_margin,
                  trade_start_prices, trade_end_prices, minimal_loss, maximum_win,
                  starting_stop_value, starting_profit_value)

  return(trades_with_aud_ts)

}



helpeR::load_custom_functions()
all_aud_symbols <-
  get_oanda_symbols() %>%
  keep(~ str_detect(.x, "AUD")|str_detect(.x, "USD_SEK|USD_NOK|USD_HUF|USD_ZAR|USD_CNY|USD_MXN|USD_CNH"))
asset_infor <- get_instrument_info()
aud_assets <- read_all_asset_data_intra_day(
  asset_list_oanda = all_aud_symbols,
  save_path_oanda_assets = "C:/Users/Nikhil Chandra/Documents/Asset Data/oanda_data/",
  read_csv_or_API = "API",
  time_frame = "D",
  bid_or_ask = "bid",
  how_far_back = 10,
  start_date = (today() - days(30)) %>% as.character()
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

db_location = "C:/Users/nikhi/Documents/Asset Data/Oanda_Asset_Data_missing_Assets.db"
trade_actual_db <- "C:/Users/nikhi/Documents/trade_data/trade_actuals_addtional_assets.db"

METALS_CRYPTO <-
  get_All_Metals_USD_Currency(
    db_location = "C:/Users/nikhi/Documents/Asset Data/Oanda_Asset_Data_missing_Assets.db",
    start_date = "2015-01-01",
    end_date = today() %>% as.character(),
    time_frame = "H1"
  )

METALS_CRYPTO_long <-METALS_CRYPTO[[1]]
METALS_CRYPTO_short <- METALS_CRYPTO[[2]]

METALS_CRYPTO_long_trades <-
  METALS_CRYPTO_long %>%
  mutate(
    trade_col = "Long"
  )

METALS_CRYPTO_long_trades_Data <-
  get_trade_results_ts_aud(
    tagged_trades = METALS_CRYPTO_long_trades,
    stop_factor = 3,
    profit_factor = 6,
    raw_asset_data = METALS_CRYPTO_long,
    risk_dollar_value = 10,
    currency_conversion = currency_conversion
  )

trade_actual_db_con <- connect_db(trade_actual_db)
append_table_sql_lite(
  METALS_CRYPTO_long_trades_Data,
  table_name =  "trade_actual",
  conn = trade_actual_db_con)
DBI::dbDisconnect(trade_actual_db_con)
rm(trade_actual_db_con)


METALS_CRYPTO_short_trades <-
  METALS_CRYPTO_short %>%
  mutate(
    trade_col = "Short"
  )

METALS_CRYPTO_short_trades_Data <-
  get_trade_results_ts_aud(
    tagged_trades = METALS_CRYPTO_short_trades,
    stop_factor = 3,
    profit_factor = 6,
    raw_asset_data = METALS_CRYPTO_short,
    risk_dollar_value = 10,
    currency_conversion = currency_conversion
  )

trade_actual_db_con <- connect_db(trade_actual_db)
append_table_sql_lite(
  METALS_CRYPTO_short_trades_Data,
  table_name =  "trade_actual",
  conn = trade_actual_db_con)
DBI::dbDisconnect(trade_actual_db_con)
rm(trade_actual_db_con)

