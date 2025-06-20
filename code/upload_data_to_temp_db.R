helperfunctions35South::load_custom_functions()
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
safely_get_data <- safely(read_all_asset_data_intra_day, otherwise = NULL)

#--------------------------------------------------------------------------15M
for (i in 1:length(asset_list_oanda) ) {

  while(while_loop_check < (today() - days(2)) ) {

    c = c + 1

    extracted_asset_data1 <-
      safely_get_data(
        asset_list_oanda = asset_list_oanda[i],
        save_path_oanda_assets = "C:/Users/Nikhil Chandra/Documents/Asset Data/oanda_data/",
        read_csv_or_API = "API",
        time_frame = "M15",
        bid_or_ask = "ask",
        how_far_back = 5000,
        start_date = start_date
      ) %>% pluck('result')

    if(!is.null(extracted_asset_data1)) {
      message("made it to upload")

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
                             table_name = "Oanda_Asset_Data_ask_M15",
                             overwrite_true = TRUE)

        db_con <- connect_db(db_location)

      } else {

        append_table_sql_lite(.data = extracted_asset_data1,
                              conn = db_con,
                              table_name = "Oanda_Asset_Data_ask_M15")

      }

    } else {

      while_loop_check <- today()

    }

  }

  start_date <- "2011-01-01"
  while_loop_check <- as_date("2011-01-01")

}


test_query <-
  DBI::dbGetQuery(conn = db_con,
                  statement = "SELECT * FROM Oanda_Asset_Data_ask_M15") %>%
  mutate(Date = as_datetime(Date))

distinct_assets <- test_query %>% distinct(Asset)

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
      safely_get_data(
        asset_list_oanda = asset_list_oanda[i],
        save_path_oanda_assets = "C:/Users/Nikhil Chandra/Documents/Asset Data/oanda_data/",
        read_csv_or_API = "API",
        time_frame = "M15",
        bid_or_ask = "bid",
        how_far_back = 5000,
        start_date = start_date
      ) %>% pluck('result')

    if(!is.null(extracted_asset_data1)) {
      message("made it to upload")

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
                             table_name = "Oanda_Asset_Data_bid_M15",
                             overwrite_true = TRUE)

        db_con <- connect_db(db_location)

      } else {

        append_table_sql_lite(.data = extracted_asset_data1,
                              conn = db_con,
                              table_name = "Oanda_Asset_Data_bid_M15")

      }

    } else {

      while_loop_check <- today()

    }

  }

  start_date <- "2011-01-01"
  while_loop_check <- as_date("2011-01-01")

}

test_query <-
  DBI::dbGetQuery(conn = db_con,
                  statement = "SELECT * FROM Oanda_Asset_Data_bid_M15
                               WHERE Asset = 'AUD_USD'") %>%
  mutate(Date = as_datetime(Date))

DBI::dbDisconnect(db_con)

#---------------------------------------------------------------------1 Hour

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

#---------------------------------------------------------------------------
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
        time_frame = "D",
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
                           table_name = "Oanda_Asset_Data_ask_D",
                           overwrite_true = TRUE)

      db_con <- connect_db(db_location)

    } else {

      append_table_sql_lite(.data = extracted_asset_data1,
                            conn = db_con,
                            table_name = "Oanda_Asset_Data_ask_D")

    }

  }

  start_date <- "2011-01-01"
  while_loop_check <- as_date("2011-01-01")

}


test_query <-
  DBI::dbGetQuery(conn = db_con,
                  statement = "SELECT * FROM Oanda_Asset_Data_ask_D
                               WHERE Asset = 'AUD_USD'") %>%
  mutate(Date = as_datetime(Date))

DBI::dbDisconnect(db_con)

gc()

#---------------------------------------------------------------------------
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
        time_frame = "D",
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
                           table_name = "Oanda_Asset_Data_bid_D",
                           overwrite_true = TRUE)

      db_con <- connect_db(db_location)

    } else {

      append_table_sql_lite(.data = extracted_asset_data1,
                            conn = db_con,
                            table_name = "Oanda_Asset_Data_bid_D")

    }

  }

  start_date <- "2011-01-01"
  while_loop_check <- as_date("2011-01-01")

}


test_query <-
  DBI::dbGetQuery(conn = db_con,
                  statement = "SELECT * FROM Oanda_Asset_Data_ask_D
                               WHERE Date > '2020-01-01'") %>%
  mutate(Date = as_datetime(Date))

DBI::dbDisconnect(db_con)

gc()

#------------------------------------------------------------------Functions

#' get_db_price
#'
#' @param db_location
#' @param start_date
#' @param end_date
#' @param time_frame
#' @param bid_or_ask
#'
#' @return
#' @export
#'
#' @examples
get_db_price <- function(db_location = "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data.db",
                         start_date = "2012-01-01",
                         end_date = "2020-01-01",
                         time_frame = "D",
                         bid_or_ask = "ask") {

  start_date_integer <- start_date %>% as_datetime() %>% as.integer()
  end_date_integer <- (as_datetime(end_date) + days(1)) %>% as.integer()

  db_table <- glue::glue("Oanda_Asset_Data_{bid_or_ask}_{time_frame}")
  db_table <- case_when(
    time_frame == "H1" ~ glue::glue("Oanda_Asset_Data_{bid_or_ask}"),
    time_frame == "D" ~  glue::glue("Oanda_Asset_Data_{bid_or_ask}_{time_frame}")
     )

  db_query <- glue::glue("SELECT * FROM {db_table}
                         WHERE Date >= {start_date_integer} AND Date <= {end_date_integer}")

  db_con <- connect_db(db_location)

  query_data <-
    DBI::dbGetQuery(conn = db_con, statement = db_query) %>%
    mutate(Date = as_datetime(Date, tz = "Australia/Sydney")) %>%
    group_by(Asset, Date) %>%
    mutate(
      Price = mean(Price, na.rm = T),
      Open = mean(Open, na.rm = T),
      High = mean(High, na.rm = T),
      Low = mean(Low, na.rm = T)
    ) %>%
    ungroup() %>%
    group_by(Asset, Date) %>%
    mutate(kk = row_number()) %>%
    group_by(Asset, Date) %>%
    slice_max(kk) %>%
    ungroup() %>%
    dplyr::select(-kk)

  DBI::dbDisconnect(db_con)

  return(query_data)

}

#' get_joined_D_H1_Price_db
#'
#' @param db_location
#' @param start_date
#' @param end_date
#' @param bid_or_ask
#'
#' @return
#' @export
#'
#' @examples
get_joined_D_H1_Price_db <- function(
    db_location = "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data.db",
    start_date = "2012-01-01",
    end_date = "2020-01-01",
    bid_or_ask = "ask"
  ) {

  Hour_data <-
    get_db_price(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask,
      time_frame = "H1"
    )  %>%
    rename(
      Price_H1 = Price,
      Open_H1 = Open,
      High_H1 = High,
      Low_H1 = Low
    ) %>%
    dplyr::select(-Vol.)

  Day_data <-
    get_db_price(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask,
      time_frame = "D"
    )

  combined_data <-
    Hour_data %>%
    left_join(Day_data)

  return(combined_data)

}

#' get_joined_D_H1_Price_bespoke
#'
#' @param Day_Data
#' @param H_data
#'
#' @return
#' @export
#'
#' @examples
get_joined_D_H1_Price_bespoke <- function(
    Day_Data,
    H_data
) {

  H_data <-
    H_data %>% rename(
      Price_H1 = Price,
      Open_H1 = Open,
      High_H1 = High,
      Low_H1 = Low
    ) %>%
    dplyr::select(-Vol.) %>%
    mutate(
      Date = as_datetime(Date, "Australia/Sydney")
    )


  combined_data <-
    Hour_data %>%
    left_join(Day_data %>% mutate(Date = as_datetime(Date, "Australia/Sydney")) )

  return(combined_data)

}


#' update_local_db_file
#'
#' @param db_location
#' @param asset_list_oanda
#' @param time_frame
#' @param bid_or_ask
#'
#' @return
#' @export
#'
#' @examples
update_local_db_file <- function(
    db_location = "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data.db",
    asset_list_oanda =
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
        "JP225_USD", "SPX500_USD"),
    time_frame = "D",
    bid_or_ask = "ask"
  ) {

  dates_by_asset <-
    get_db_price(
      db_location = db_location,
      start_date = today() - days(30),
      end_date = today(),
      time_frame = time_frame,
      bid_or_ask = bid_or_ask) %>%
    group_by(Asset) %>%
    slice_max(Date) %>%
    mutate(Date = as_datetime(Date, "Australia/Canberra"))

  current_latest_date <-
    dates_by_asset %>%
    pull(Date) %>%
    min(na.rm = T)

  data_to_Update <-
    read_all_asset_data_intra_day(
      asset_list_oanda = asset_list_oanda,
      save_path_oanda_assets = "C:/Users/Nikhil Chandra/Documents/Asset Data/oanda_data/",
      read_csv_or_API = "API",
      time_frame = time_frame,
      bid_or_ask = bid_or_ask,
      how_far_back = 5000,
      start_date = (current_latest_date %>% as_date() - days(3)) %>% as.character()
    )

  data_to_Update <-
    data_to_Update %>%
    map_dfr(bind_rows)

  for_upload <-
    data_to_Update %>%
    mutate(Date = as_datetime(Date, tz = "Australia/Canberra")) %>%
    left_join(
      dates_by_asset %>% ungroup() %>%  dplyr::select(Asset,  DB_Date = Date)
    ) %>%
    filter(Date > DB_Date) %>%
    dplyr::select(-DB_Date)

  table_name <-
    case_when(
      time_frame == "D" & bid_or_ask == "ask" ~ "Oanda_Asset_Data_ask_D",
      time_frame == "D" & bid_or_ask == "bid" ~ "Oanda_Asset_Data_bid_D",

      time_frame == "H1" & bid_or_ask == "ask" ~ "Oanda_Asset_Data_ask",
      time_frame == "H1" & bid_or_ask == "bid" ~ "Oanda_Asset_Data_bid",
    )

  if(dim(for_upload)[1] > 0) {

    message(glue::glue("Latest Date in DB {current_latest_date}"))
    print(for_upload)
    db_con <- connect_db(db_location)
    append_table_sql_lite(.data = for_upload,
                          conn = db_con,
                          table_name = table_name)
    DBI::dbDisconnect(db_con)

  }

}
