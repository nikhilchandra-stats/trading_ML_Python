helpeR::load_custom_functions()

db_location = "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data_Most_Assets_2025-09-13.db"
# asset_list_oanda =
#   c("XAG_USD", "XAG_EUR", "XAG_CAD", "XAG_AUD", "XAG_GBP", "XAG_JPY", "XAG_SGD", "XAG_CHF",
#     "XAG_NZD",
#     "XAU_USD", "XAU_EUR", "XAU_CAD", "XAU_AUD", "XAU_GBP", "XAU_JPY", "XAU_SGD", "XAU_CHF",
#     "XAU_NZD",
#     "BTC_USD", "LTC_USD", "BCH_USD",
#     "US30_USD", "FR40_EUR", "US2000_USD", "CH20_CHF", "SPX500_USD", "AU200_AUD",
#     "JP225_USD", "JP225Y_JPY", "SG30_SGD", "EU50_EUR", "HK33_HKD",
#     "USB02Y_USD", "USB05Y_USD", "USB30Y_USD", "USB10Y_USD", "UK100_GBP",
#     "AUD_USD", "EUR_USD", "GBP_USD", "USD_CHF", "USD_JPY", "USD_MXN", "USD_SEK", "USD_NOK",
#     "NZD_USD", "USD_CAD", "USD_SGD", "ETH_USD", "XPT_USD", "XPD_USD",
#     "USD_JPY", "GBP_JPY", "USD_SGD", "EUR_SEK",
#     "DE30_EUR",
#     "USD_CHF", "USD_SEK", "XCU_USD", "SUGAR_USD",
#     "USD_MXN", "GBP_USD", "WTICO_USD", "EUR_JPY", "USD_NOK",
#     "XAU_USD",
#     "USD_CZK",  "WHEAT_USD",
#     "EUR_USD", "SG30_SGD", "AU200_AUD", "XAG_USD",
#     "EUR_GBP", "USD_CNH", "USD_CAD", "NAS100_USD",
#     "EU50_EUR", "NATGAS_USD", "SOYBN_USD",
#     "US2000_USD",
#     "BCO_USD", "AUD_USD", "NZD_USD", "NZD_CHF", "WHEAT_USD",
#     "JP225_USD", "SPX500_USD",
#     "EUR_AUD", "EUR_NZD", "EUR_CHF", "ESPIX_EUR" ,"EUR_NZD" ,
#     "GBP_AUD", "GBP_NZD", "UK100_GBP", "UK10YB_GBP", "GBP_CHF", "GBP_CAD",
#     "NL25_EUR") %>%
#   unique()


asset_list_oanda <-
  get_oanda_symbols()

time_frame = "H1"
bid_or_ask = "bid"
how_far_back = 10
ending_date = "2025-09-12"
starting_date = "2012-01-01"

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

#---------------------------------------------------------Day

helpeR::load_custom_functions()

db_location = "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data_Most_Assets_2025-09-13.db"
all_aud_symbols <- get_oanda_symbols()
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

time_frame = "D"
bid_or_ask = "bid"
how_far_back = 10
ending_date = "2025-09-12"
starting_date = "2008-01-01"

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

