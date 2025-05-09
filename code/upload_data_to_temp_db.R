start_date <- "2011-01-01"
while_loop_check <- as_date("2011-01-01")
end_date <- (today() - days(1)) %>% as.character()
asset_list_oanda <-
  c("USD_JPY", "GBP_JPY", "USD_SGD", "EUR_SEK",
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
    "JP225_USD", "SPX500_USD")
db_location <- "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data.db"
c = 0
db_con <- connect_db(db_location)

for (i in 1:length(asset_list_oanda) ) {

  while(while_loop_check < (today() - days(2)) ) {

    c = c + 1

    extracted_asset_data1 <-
      read_all_asset_data_intra_day(
        asset_list_oanda = asset_list_oanda[i],
        save_path_oanda_assets = "C:/Users/Nikhil Chandra/Documents/Asset Data/oanda_data/",
        read_csv_or_API = "API",
        time_frame = "H1",
        bid_or_ask = "ask",
        how_far_back = 5000,
        start_date = start_date
      )

    extracted_asset_data1 <- extracted_asset_data1 %>% map_dfr(bind_rows)
    max_date_in_1 <- extracted_asset_data1 %>%
      group_by(Asset) %>%
      slice_max(Date) %>%
      ungroup() %>%
      slice_min(Date) %>%
      pull(Date) %>% pluck(1) %>% as.character()
    min_date_in_1 <- extracted_asset_data1$Date %>% min(na.rm = T) %>% as_date() %>% as.character()

    while_loop_check <- as_date(max_date_in_1)

    start_date <- max_date_in_1

    if(c == 1) {

      write_table_sql_lite(.data = extracted_asset_data1,
                           conn = db_con,
                           table_name = "Oanda_Asset_Data_ask",
                           overwrite_true = TRUE)

      db_con <- connect_db(db_location)

    } else {

      append_table_sql_lite(.data = extracted_asset_data1,
                           conn = db_con,
                           table_name = "Oanda_Asset_Data_ask")

    }

  }

  start_date <- "2011-01-01"
  while_loop_check <- as_date("2011-01-01")

}


test_query <-
  DBI::dbGetQuery(conn = db_con,
                  statement = "SELECT * FROM Oanda_Asset_Data_ask
                               WHERE Asset = 'AUD_USD'") %>%
  mutate(Date = as_datetime(Date))

DBI::dbDisconnect(db_con)

gc()
#-------------------------------------------------------------------

start_date <- "2011-01-01"
while_loop_check <- as_date("2011-01-01")
end_date <- (today() - days(1)) %>% as.character()
db_location <- "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data.db"
c = 0
db_con <- connect_db(db_location)

for (i in 1:length(asset_list_oanda) ) {

  while(while_loop_check < (today() - days(2)) ) {

    c = c + 1

    extracted_asset_data1 <-
      read_all_asset_data_intra_day(
        asset_list_oanda = asset_list_oanda[i],
        save_path_oanda_assets = "C:/Users/Nikhil Chandra/Documents/Asset Data/oanda_data/",
        read_csv_or_API = "API",
        time_frame = "H1",
        bid_or_ask = "bid",
        how_far_back = 5000,
        start_date = start_date
      )

    extracted_asset_data1 <- extracted_asset_data1 %>% map_dfr(bind_rows)
    max_date_in_1 <- extracted_asset_data1 %>%
      group_by(Asset) %>%
      slice_max(Date) %>%
      ungroup() %>%
      slice_min(Date) %>%
      pull(Date) %>% pluck(1) %>% as.character()
    min_date_in_1 <- extracted_asset_data1$Date %>% min(na.rm = T) %>% as_date() %>% as.character()

    while_loop_check <- as_date(max_date_in_1)

    start_date <- max_date_in_1

    if(c == 1) {

      write_table_sql_lite(.data = extracted_asset_data1,
                           conn = db_con,
                           table_name = "Oanda_Asset_Data_bid",
                           overwrite_true = TRUE)

      db_con <- connect_db(db_location)

    } else {

      append_table_sql_lite(.data = extracted_asset_data1,
                            conn = db_con,
                            table_name = "Oanda_Asset_Data_bid")

    }

  }

  start_date <- "2011-01-01"
  while_loop_check <- as_date("2011-01-01")

}

test_query <-
  DBI::dbGetQuery(conn = db_con,
                  statement = "SELECT * FROM Oanda_Asset_Data_bid
                               WHERE Asset = 'AUD_USD'") %>%
  mutate(Date = as_datetime(Date))

DBI::dbDisconnect(db_con)

