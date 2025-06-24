helpeR::load_custom_functions()

all_aud_symbols <- get_oanda_symbols() %>%
  keep(~ str_detect(.x, "AUD")|str_detect(.x, "USD_SEK|USD_NOK|USD_HUF|USD_ZAR|USD_CNY|USD_MXN|USD_CNH"))
asset_infor <- get_instrument_info()
aud_assets <- read_all_asset_data_intra_day(
  asset_list_oanda = all_aud_symbols,
  save_path_oanda_assets = "C:/Users/Nikhil Chandra/Documents/Asset Data/oanda_data/",
  read_csv_or_API = "API",
  time_frame = "D",
  bid_or_ask = "ask",
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

db_location <- "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data.db"
start_date_day = "2020-01-01"
start_date_day_H1 = "2019-01-01"
end_date_day = today() %>% as.character()

starting_asset_data_ask_15M <-
  get_db_price(
    db_location = db_location,
    start_date = start_date_day,
    end_date = end_date_day,
    bid_or_ask = "ask",
    time_frame = "M15"
  )

starting_asset_data_ask_H1 <-
  get_db_price(
    db_location = db_location,
    start_date = start_date_day_H1,
    end_date = end_date_day,
    bid_or_ask = "ask",
    time_frame = "H1"
  )

mean_values_by_asset_for_loop_15_ask =
  wrangle_asset_data(
    asset_data_daily_raw = starting_asset_data_ask_15M,
    summarise_means = TRUE
  )


starting_asset_data_ask_H1 = starting_asset_data_ask_H1
starting_asset_data_ask_15M = starting_asset_data_ask_15M

sup_res_trade_db <-
  glue::glue("C:/Users/Nikhil Chandra/Documents/trade_data/sup_res_2025-06-11.db")
db_con <- connect_db(sup_res_trade_db)
create_new_table = TRUE

trade_params <-
  c(3,4,5) %>%
  map_dfr(
    ~ tibble(
      sd_fac_2 = c(3,4,5)
    ) %>%
      mutate(
        sd_fac_1 = .x
      )
  )

trade_params <- c(3,4,5) %>%
  map_dfr(
    ~ trade_params %>%
      mutate(
        sd_fac_3 = .x
      )
  )

trade_params_p1 <-
  seq(12,20, 1) %>%
  map_dfr(
    ~ trade_params %>%
      mutate(
        stop_factor = .x
      ) %>%
      mutate(
        profit_factor = .x
      )
  )

trade_params_p2 <-
  seq(12,20, 1) %>%
  map_dfr(
    ~ trade_params %>%
      mutate(
        stop_factor = .x
      ) %>%
      mutate(
        profit_factor = 1.5*.x
      )
  )

trade_params_p3 <-
  seq(12,20, 1) %>%
  map_dfr(
    ~ trade_params %>%
      mutate(
        stop_factor = .x
      ) %>%
      mutate(
        profit_factor = 2*.x
      )
  )

trade_params <-
  list(
    trade_params_p1,trade_params_p2, trade_params_p3
  ) %>%
  reduce(bind_rows)

# XX = 150
# rolling_slide = 300
# pois_period = 10

XX = 25
rolling_slide = 200
pois_period = 10


#----------------------------------------- Creating Data for Algo
tictoc::tic()

current_time <- now() %>% as_datetime()
current_minute <- lubridate::minute(current_time)
current_hour <- lubridate::hour(current_time)
current_date <- now() %>% as_date(tz = "Australia/Canberra")

starting_asset_data_ask_H1 = starting_asset_data_ask_H1
starting_asset_data_ask_15M = starting_asset_data_ask_15M

squeeze_detection <-
  get_res_sup_slow_fast_fractal_data(
    starting_asset_data_ask_H1 = starting_asset_data_ask_H1,
    starting_asset_data_ask_15M = starting_asset_data_ask_15M,
    XX = XX,
    rolling_slide = rolling_slide,
    pois_period = pois_period
  )
tictoc::toc()


for (j in 1:dim(trade_params)[1]) {

  stop_factor = trade_params$stop_factor[j]
  profit_factor = trade_params$profit_factor[j]
  sd_fac_1 = trade_params$sd_fac_1[j]
  sd_fac_2 = trade_params$sd_fac_2[j]
  sd_fac_3 = trade_params$sd_fac_3[j]

  stop_factor = 17
  profit_factor = 25.5
  sd_fac_1 = 4
  sd_fac_2 = 3
  sd_fac_3 = 3


  analysis_data <-
    get_res_sup_trade_analysis(
      squeeze_detection = squeeze_detection,
      raw_asset_data = starting_asset_data_ask_15M,
      mean_values_by_asset_for_loop = mean_values_by_asset_for_loop_15_ask,
      stop_factor = stop_factor,
      profit_factor =profit_factor,
      risk_dollar_value = 10,
      sd_fac_1 = sd_fac_1,
      sd_fac_2 = sd_fac_2,
      sd_fac_3 = sd_fac_3,
      trade_direction = "Long",
      currency_conversion = currency_conversion,
      asset_infor = asset_infor
    )

  analysis_data_total <- analysis_data[[2]] %>%
    mutate(
      XX = XX,
      rolling_slide = rolling_slide,
      pois_period = pois_period
    )
  analysis_data_asset <- analysis_data[[1]] %>%
    mutate(
      XX = XX,
      rolling_slide = rolling_slide,
      pois_period = pois_period
    )

  if(create_new_table == TRUE & j == 1) {
    write_table_sql_lite(conn = db_con, .data = analysis_data_total, table_name = "sup_res")
    write_table_sql_lite(conn = db_con, .data = analysis_data_asset, table_name = "sup_res_asset")
    create_new_table <- FALSE
  }

  if(create_new_table == FALSE) {
    append_table_sql_lite(conn = db_con, .data = analysis_data_total, table_name = "sup_res")
    append_table_sql_lite(conn = db_con, .data = analysis_data_asset, table_name = "sup_res_asset")
  }

}

short_analysis <-
  DBI::dbGetQuery(conn = db_con,
                  statement = "SELECT * FROM sup_res") %>%
  filter(trade_direction == "Long")

DBI::dbDisconnect(db_con)
rm(db_con)
gc()

#----------------------------------------- Creating Data for Algo
tictoc::tic()

current_time <- now() %>% as_datetime()
current_minute <- lubridate::minute(current_time)
current_hour <- lubridate::hour(current_time)
current_date <- now() %>% as_date(tz = "Australia/Canberra")

starting_asset_data_ask_H1 = starting_asset_data_ask_H1
starting_asset_data_ask_15M = starting_asset_data_ask_15M

update_local_db_file(
  db_location = db_location,
  time_frame = "H1",
  bid_or_ask = "ask",
  asset_list_oanda = asset_list_oanda,
  how_far_back = 5
)

update_local_db_file(
  db_location = db_location,
  time_frame = "M15",
  bid_or_ask = "ask",
  asset_list_oanda = asset_list_oanda,
  how_far_back = 5
)

new_H1_data_ask <-
  updated_data_internal(starting_asset_data = starting_asset_data_ask_H1,
                        end_date_day = current_date,
                        time_frame = "H1", bid_or_ask = "ask")%>%
  distinct()
new_15_data_ask <-
  updated_data_internal(starting_asset_data = starting_asset_data_ask_15M,
                        end_date_day = current_date,
                        time_frame = "M15", bid_or_ask = "ask")%>%
  distinct()

total_trades <-
  get_sup_res_trades_to_take(
    db_path = glue::glue("C:/Users/Nikhil Chandra/Documents/trade_data/sup_res_2025-06-11.db"),
    min_risk_win = 0.12,
    min_risk_perc = 0.1,
    max_win_time = 150,
    starting_asset_data_ask_H1 = new_H1_data_ask,
    starting_asset_data_ask_15M = new_15_data_ask,
    trade_direction = "Long",
    samples_to_use = 2000
  )


trades_today <-
  total_trades %>%
  split(.$Asset, drop = FALSE) %>%
  map_dfr(
    ~ .x %>%
      get_stops_profs_volume_trades(
        mean_values_by_asset = mean_values_by_asset_for_loop_15_ask,
        trade_col = "trade_col",
        currency_conversion = currency_conversion,
        risk_dollar_value = 10,
        stop_factor = .x$stop_factor[1] %>% as.numeric(),
        profit_factor = .x$profit_factor[1] %>% as.numeric(),
        asset_col = "Asset",
        stop_col = "stop_value",
        profit_col = "profit_value",
        price_col = "Price",
        trade_return_col = "trade_returns"
      )
  )

