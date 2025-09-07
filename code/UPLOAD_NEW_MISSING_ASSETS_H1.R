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

#' get_All_Metals_USD_Currency
#'
#' @param db_location
#' @param start_date
#' @param end_date
#' @param time_frame
#'
#' @returns
#' @export
#'
#' @examples
get_All_Metals_USD_Currency <- function(
    db_location = db_location,
    start_date = "2016-01-01",
    end_date = today() %>% as.character(),
    time_frame = "H1"
  ) {

  XAG_USD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "XAG_USD",
    keep_bid_to_ask = TRUE
  )

  XAG_EUR <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "XAG_EUR",
    keep_bid_to_ask = TRUE
  )

  XAG_GBP <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "XAG_GBP",
    keep_bid_to_ask = TRUE
  )

  XAG_AUD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "XAG_AUD",
    keep_bid_to_ask = TRUE
  )

  XAG_JPY <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "XAG_JPY",
    keep_bid_to_ask = TRUE
  )


  XAU_USD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "XAU_USD",
    keep_bid_to_ask = TRUE
  )

  XAU_EUR <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "XAU_EUR",
    keep_bid_to_ask = TRUE
  )

  XAU_GBP <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "XAU_GBP",
    keep_bid_to_ask = TRUE
  )

  XAU_AUD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "XAU_AUD",
    keep_bid_to_ask = TRUE
  )

  XAU_JPY <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "XAU_JPY",
    keep_bid_to_ask = TRUE
  )

  BTC_USD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "BTC_USD",
    keep_bid_to_ask = TRUE
  )

  ETH_USD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "ETH_USD",
    keep_bid_to_ask = TRUE
  )

  SPX500_USD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "SPX500_USD",
    keep_bid_to_ask = TRUE
  )

  EU50_EUR <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "EU50_EUR",
    keep_bid_to_ask = TRUE
  )

  FR40_EUR <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "FR40_EUR",
    keep_bid_to_ask = TRUE
  )

  ALL_ASK <-
    XAG_USD %>%
    bind_rows(XAG_EUR)%>%
    bind_rows(XAG_GBP)%>%
    bind_rows(XAG_AUD)%>%
    bind_rows(XAG_JPY) %>%
    bind_rows(XAU_USD)%>%
    bind_rows(XAU_EUR)%>%
    bind_rows(XAU_GBP)%>%
    bind_rows(XAU_AUD)%>%
    bind_rows(XAU_JPY)%>%
    bind_rows(BTC_USD)%>%
    bind_rows(ETH_USD)%>%
    bind_rows(SPX500_USD)%>%
    bind_rows(EU50_EUR)%>%
    bind_rows(FR40_EUR)

  rm(XAG_USD, XAG_EUR, XAG_GBP, XAG_AUD, XAG_JPY,
     XAU_USD, XAU_EUR, XAU_GBP, XAU_AUD, XAU_JPY, BTC_USD, ETH_USD, SPX500_USD)
  gc()

  #--------------------------------------------------------------------------------------

  XAG_USD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "XAG_USD",
    keep_bid_to_ask = TRUE
  )

  XAG_EUR <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "XAG_EUR",
    keep_bid_to_ask = TRUE
  )

  XAG_GBP <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "XAG_GBP",
    keep_bid_to_ask = TRUE
  )

  XAG_AUD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "XAG_AUD",
    keep_bid_to_ask = TRUE
  )

  XAG_JPY <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "XAG_JPY",
    keep_bid_to_ask = TRUE
  )


  XAU_USD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "XAU_USD",
    keep_bid_to_ask = TRUE
  )

  XAU_EUR <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "XAU_EUR",
    keep_bid_to_ask = TRUE
  )

  XAU_GBP <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "XAU_GBP",
    keep_bid_to_ask = TRUE
  )

  XAU_AUD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "XAU_AUD",
    keep_bid_to_ask = TRUE
  )

  XAU_JPY <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "XAU_JPY",
    keep_bid_to_ask = TRUE
  )

  BTC_USD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "BTC_USD",
    keep_bid_to_ask = TRUE
  )

  ETH_USD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "ETH_USD",
    keep_bid_to_ask = TRUE
  )

  SPX500_USD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "SPX500_USD",
    keep_bid_to_ask = TRUE
  )

  EU50_EUR <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "EU50_EUR",
    keep_bid_to_ask = TRUE
  )

  FR40_EUR <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "FR40_EUR",
    keep_bid_to_ask = TRUE
  )

  ALL_BID <-
    XAG_USD %>%
    bind_rows(XAG_EUR)%>%
    bind_rows(XAG_GBP)%>%
    bind_rows(XAG_AUD)%>%
    bind_rows(XAG_JPY) %>%
    bind_rows(XAU_USD)%>%
    bind_rows(XAU_EUR)%>%
    bind_rows(XAU_GBP)%>%
    bind_rows(XAU_AUD)%>%
    bind_rows(XAU_JPY) %>%
    bind_rows(BTC_USD) %>%
    bind_rows(ETH_USD)%>%
    bind_rows(SPX500_USD)%>%
    bind_rows(EU50_EUR)%>%
    bind_rows(FR40_EUR)

  rm(XAG_USD, XAG_EUR, XAG_GBP, XAG_AUD, XAG_JPY,
     XAU_USD, XAU_EUR, XAU_GBP, XAU_AUD, XAU_JPY,
     BTC_USD, ETH_USD, SPX500_USD, EU50_EUR)
  gc()

  return(
    list(
      ALL_ASK,
      ALL_BID
    )
  )

}


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
    stop_factor = 8,
    profit_factor = 13,
    raw_asset_data = METALS_CRYPTO_long,
    risk_dollar_value = 5,
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
    stop_factor = 8,
    profit_factor = 13,
    raw_asset_data = METALS_CRYPTO_short,
    risk_dollar_value = 5,
    currency_conversion = currency_conversion
  )

trade_actual_db_con <- connect_db(trade_actual_db)
append_table_sql_lite(
  METALS_CRYPTO_short_trades_Data,
  table_name =  "trade_actual",
  conn = trade_actual_db_con)
DBI::dbDisconnect(trade_actual_db_con)
rm(trade_actual_db_con)

#' create_NN_EUR_GBP_JPY_USD_data
#'
#' @return
#' @export
#'
#' @examples
create_NN_METALS_CRYPTO_data <-
  function(METALS_CRYPTO = METALS_CRYPTO[[1]],
           raw_macro_data,
           actual_wins_losses = actual_wins_losses,
           lag_days = 1,
           stop_value_var = 15,
           profit_value_var = 20,
           use_PCA_vars = FALSE) {

    copula_data_XAG_USD_EUR <-
      estimating_dual_copula(
        asset_data_to_use = METALS_CRYPTO,
        asset_to_use = c("XAG_USD", "XAG_EUR"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      )

    copula_data_XAG_USD_JPY <-
      estimating_dual_copula(
        asset_data_to_use = METALS_CRYPTO,
        asset_to_use = c("XAG_USD", "XAG_JPY"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAG_USD, -XAG_USD_log1_price, -XAG_USD_quantiles_1, -XAG_USD_tangent_angle1)

    copula_data_XAG_EUR_JPY <-
      estimating_dual_copula(
        asset_data_to_use = METALS_CRYPTO,
        asset_to_use = c("XAG_EUR", "XAG_JPY"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAG_EUR, -XAG_EUR_log1_price, -XAG_EUR_quantiles_1, -XAG_EUR_tangent_angle1) %>%
      dplyr::select(-XAG_JPY, -XAG_JPY_log2_price, -XAG_JPY_quantiles_2, -XAG_JPY_tangent_angle2)

    copula_data_XAG_USD_GBP <-
      estimating_dual_copula(
        asset_data_to_use = METALS_CRYPTO,
        asset_to_use = c("XAG_USD", "XAG_GBP"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAG_USD, -XAG_USD_log1_price, -XAG_USD_quantiles_1, -XAG_USD_tangent_angle1)

    copula_data_XAG_EUR_GBP <-
      estimating_dual_copula(
        asset_data_to_use = METALS_CRYPTO,
        asset_to_use = c("XAG_EUR", "XAG_GBP"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAG_EUR, -XAG_EUR_log1_price, -XAG_EUR_quantiles_1, -XAG_EUR_tangent_angle1) %>%
      dplyr::select(-XAG_GBP, -XAG_GBP_log2_price, -XAG_GBP_quantiles_2, -XAG_GBP_tangent_angle2)


    binary_data_for_post_model <-
      actual_wins_losses %>%
      rename(Date = dates, Asset = asset) %>%
      filter(profit_factor == profit_value_var)%>%
      filter(stop_factor == stop_value_var) %>%
      mutate(
        bin_var =
          case_when(
            trade_start_prices > trade_end_prices & trade_col == "Short" ~ "win",
            trade_start_prices <= trade_end_prices & trade_col == "Short" ~ "loss",

            trade_start_prices < trade_end_prices & trade_col == "Long" ~ "win",
            trade_start_prices >= trade_end_prices & trade_col == "Long" ~ "loss"

          )
      ) %>%
      dplyr::select(Date, bin_var, Asset, trade_col,
                    profit_factor, stop_factor,
                    trade_start_prices, trade_end_prices,
                    starting_stop_value, starting_profit_value)

    copula_data_macro <-
      METALS_CRYPTO %>%
      dplyr::select(Date,Asset, Price, High, Low, Open ) %>%
      group_by(Asset) %>%
      mutate(
        Resistance_50 = slider::slide_dbl(.x = High, .f = ~max(.x, na.rm = T), .before = 50),
        Resistance_100 = slider::slide_dbl(.x = High, .f = ~max(.x, na.rm = T), .before = 100),
        Resistance_200 = slider::slide_dbl(.x = High, .f = ~max(.x, na.rm = T), .before = 200),

        Support_50 = slider::slide_dbl(.x = High, .f = ~min(.x, na.rm = T), .before = 50),
        Support_100 = slider::slide_dbl(.x = High, .f = ~min(.x, na.rm = T), .before = 100),
        Support_200 = slider::slide_dbl(.x = High, .f = ~min(.x, na.rm = T), .before = 200),

        Close_50 = Price - lag(Price, 50),
        Close_100 = Price - lag(Price, 100),
        Close_200 = Price - lag(Price, 200),
      ) %>%
      group_by(Asset) %>%
      mutate(
        Fib_Point_Close_50_50 = Close_50/(Resistance_50 - Support_50),
        Fib_Point_Close_100_100 = Close_100/(Resistance_100 - Support_100),
        Fib_Point_Close_200_200 = Close_200/(Resistance_200 - Support_200)
      ) %>%
      left_join(copula_data_XAG_USD_EUR) %>%
      left_join(copula_data_XAG_USD_JPY) %>%
      left_join(copula_data_XAG_EUR_JPY) %>%
      left_join(copula_data_XAG_USD_GBP) %>%
      left_join(binary_data_for_post_model) %>%
      group_by(Asset) %>%
      arrange(Date, .by_group = TRUE) %>%
      group_by(Asset) %>%
      fill(matches(all_macro_vars, ignore.case = FALSE), .direction = "down") %>%
      mutate(hour_of_day = lubridate::hour(Date) %>% as.numeric(),
             day_of_week = lubridate::wday(Date) %>% as.numeric())

    max_date_in_testing_data <- copula_data_macro %>% pull(Date) %>% max(na.rm = T)
    message(glue::glue("Max date in Complete data: {max_date_in_testing_data}"))

    lm_quant_vars <- names(copula_data_macro) %>% keep(~ str_detect(.x,"quantiles|tangent|cor"))

    if(use_PCA_vars == TRUE) {
      lm_vars1 <- c(PC_macro_vars, lm_quant_vars,
                    "fib_1", "fib_2",
                    "fib_3", "fib_4",
                    "fib_5", "fib_6",
                    "lagged_var_1", "lagged_var_2",
                    "lagged_var_3", "lagged_var_5",
                    "lagged_var_8",
                    "lagged_var_13", "lagged_var_21",
                    "lagged_var_3_ma", "lagged_var_5_ma",
                    "lagged_var_8_ma", "lagged_var_13_ma",
                    "lagged_var_21_ma"
                    # "hour_of_day", "day_of_week"
      )
    } else {
      lm_vars1 <- c(all_macro_vars, lm_quant_vars,
                    "fib_1", "fib_2",
                    "fib_3", "fib_4",
                    "fib_5", "fib_6",
                    "lagged_var_1", "lagged_var_2",
                    "lagged_var_3", "lagged_var_5",
                    "lagged_var_8",
                    "lagged_var_13", "lagged_var_21",
                    "lagged_var_3_ma", "lagged_var_5_ma",
                    "lagged_var_8_ma", "lagged_var_13_ma",
                    "lagged_var_21_ma"
                    # "hour_of_day", "day_of_week"
      )
    }

    return(
      list(
        "copula_data_macro" = copula_data_macro,
        "lm_vars1" =
          lm_vars1 %>%
          keep( ~ !str_detect(.x, "sd") & !str_detect(.x, "tangent") )
      )
    )

  }

