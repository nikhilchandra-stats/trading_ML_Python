helpeR::load_custom_functions()

db_location = "C:/Users/nikhi/Documents/Asset Data/Oanda_Asset_Data_Most_Assets_2025-09-13.db"
all_aud_symbols <- get_oanda_symbols()
asset_list_oanda =
  get_oanda_symbols() %>%
  unique()

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

safely_get_data <-
  safely(read_all_asset_data_intra_day, otherwise = NULL)

write_table_sql_lite(.data = data_to_Update_dfr,
                      table_name = table_name,
                      conn = db_con,
                      overwrite_true = TRUE)

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

db_location = "C:/Users/nikhi/Documents/Asset Data/Oanda_Asset_Data_Most_Assets_2025-09-13.db"
all_aud_symbols <- get_oanda_symbols()
asset_list_oanda =
  get_oanda_symbols() %>%
  unique()

time_frame = "D"
bid_or_ask = "ask"
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

