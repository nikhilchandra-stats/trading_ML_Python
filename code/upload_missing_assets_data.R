helpeR::load_custom_functions()
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
  keep( ~ .x %in% c("UK100_GBP", "XAU_XAG", "XAG_SGD", "XAU_GBP", "XAU_CHF", "XAU_EUR",
                    "XAG_GBP", "BTC_USD", "XAG_NZD", "XAU_NZD", "XAG_JPY", "ETH_USD", "XAU_JPY",
                    "BCH_USD", "XAG_AUD", "XAU_CAD", "XAG_CHF", "XAG_CAD", "XAU_AUD", "XAG_EUR",
                    "XAU_SGD", "FR40_EUR", "CN50_USD", "DE30_EUR", "USB10Y_USD", "USB05Y_USD", "USB02Y_USD",
                    "DE10YB_EUR", "USB30Y_USD", "ESPIX_EUR")
  ) %>%
  unique()

asset_infor <- get_instrument_info()


upload_initial_assets_DB <- function(
    db_location <- "C:/Users/Nikhil/Documents/Asset Data/Oanda_Asset_Data_2.db",
    start_date_day = "2016-01-01",
    end_date_day = "2025-08-29",
    asset_list_oanda = asset_list_oanda,
    bid_or_ask = "ask",
    time_frame = "M15"
  ) {

  new_assets_to_upload <-
    read_all_asset_data_intra_day(
      asset_list_oanda = asset_list_oanda,
      save_path_oanda_assets = "C:/Users/Nikhil Chandra/Documents/Asset Data/oanda_data/",
      read_csv_or_API = "API",
      time_frame = time_frame,
      bid_or_ask = bid_or_ask,
      how_far_back = 5000,
      start_date = start_date_day
    )

  new_assets_to_upload_dfr <-
    new_assets_to_upload %>%
    map_dfr(bind_rows)

  db_table <- glue::glue("Oanda_Asset_Data_{bid_or_ask}_{time_frame}")
  db_table <- case_when(
    time_frame == "H1" ~ glue::glue("Oanda_Asset_Data_{bid_or_ask}"),
    time_frame == "D" ~  glue::glue("Oanda_Asset_Data_{bid_or_ask}_{time_frame}"),
    time_frame == "M15" ~  glue::glue("Oanda_Asset_Data_{bid_or_ask}_{time_frame}")
  )

  db_con <- connect_db(db_location)

  write_table_sql_lite(.data = new_assets_to_upload_dfr,
                       table_name = as.character(db_table),
                       conn = db_con,
                       overwrite_true = TRUE)

  DBI::dbDisconnect(db_con)

  db_con <- connect_db(db_location)
  db_query <- glue::glue("SELECT Date, Asset FROM {db_table}")

  current_max_date <-
    DBI::dbGetQuery(conn = db_con, statement = db_query) %>%
    mutate(Date = as_datetime(Date, tz = "Australia/Sydney")) %>%
    group_by(Asset) %>%
    slice_max(Date) %>%
    pull(Date) %>%
    min(na.rm = T) %>%
    as_date() %>%
    as.character()

  while(as_date(current_max_date) < as_date(end_date_day)){

    new_assets_to_upload <-
      read_all_asset_data_intra_day(
        asset_list_oanda = asset_list_oanda,
        save_path_oanda_assets = "C:/Users/Nikhil Chandra/Documents/Asset Data/oanda_data/",
        read_csv_or_API = "API",
        time_frame = time_frame,
        bid_or_ask = bid_or_ask,
        how_far_back = 5000,
        start_date = current_max_date
      )

    new_assets_to_upload_dfr <-
      new_assets_to_upload %>%
      map_dfr(bind_rows)

    db_con <- connect_db(db_location)
    db_query <- glue::glue("SELECT Date, MAX(Asset) FROM {db_table}
                           GROUP BY Asset")

    current_data <-
      DBI::dbGetQuery(conn = db_con, statement = db_query) %>%
      mutate(Date = as_datetime(Date, tz = "Australia/Sydney")) %>%
      group_by(Asset) %>%
      slice_max(Date) %>%
      ungroup() %>%
      dplyr::select(
        Current_Max_Date = Date,
        Asset
      )

    new_assets_to_upload_dfr <-
      new_assets_to_upload_dfr %>%
      left_join(current_data) %>%
      filter(Date >= Current_Max_Date) %>%
      dplyr::select(-Current_Max_Date)

    append_table_sql_lite(.data = new_assets_to_upload_dfr,
                          table_name = as.character(db_table),
                          conn = db_con)
    DBI::dbDisconnect(db_con)

    db_con <- connect_db(db_location)
    db_query <- glue::glue("SELECT Date, MAX(Asset) FROM {db_table}
                           GROUP BY Asset")
    current_max_date <-
      DBI::dbGetQuery(conn = db_con, statement = db_query) %>%
      mutate(Date = as_datetime(Date, tz = "Australia/Sydney")) %>%
      group_by(Asset) %>%
      slice_max(Date) %>%
      pull(Date) %>%
      min(na.rm = T) %>%
      as_date() %>%
      as.character()

    DBI::dbDisconnect(db_con)

  }

}

