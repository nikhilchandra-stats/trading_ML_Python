helpeR::load_custom_functions()
library(neuralnet)
raw_macro_data <- get_macro_event_data()

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

db_location <- "C:/Users/Nikhil/Documents/Asset Data/Oanda_Asset_Data.db"
start_date_day = "2018-03-01"
start_date_day_H1 = "2017-09-01"
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
  glue::glue("C:/Users/Nikhil/Documents/trade_data/sup_res_2025-07-01.db")
db_con <- connect_db(sup_res_trade_db)
create_new_table = FALSE

trade_params <-
  c(2,3,4,5,6) %>%
  map_dfr(
    ~ tibble(
      sd_fac_2 = c(2,3,4,5,6)
    ) %>%
      mutate(
        sd_fac_1 = .x
      )
  )

trade_params <- c(2,3,4,5,6) %>%
  map_dfr(
    ~ trade_params %>%
      mutate(
        sd_fac_3 = .x
      )
  )

trade_params_p1 <-
  seq(12,15, 1) %>%
  map_dfr(
    ~ trade_params %>%
      mutate(
        stop_factor = .x
      ) %>%
      mutate(
        profit_factor = 1.25*.x
      )
  )

trade_params_p2 <-
  seq(12,15, 1) %>%
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
    trade_params_p1,
    trade_params_p2,
    trade_params_p3
  ) %>%
  reduce(bind_rows)

XX = 400
rolling_slide = 250
pois_period = 10


# XX = 250
# rolling_slide = 100
# pois_period = 10

# XX = 150
# rolling_slide = 300
# pois_period = 10
gc()

# XX = 25
# rolling_slide = 200
# pois_period = 10
create_new_table <- FALSE
#----------------------------------------- Creating Data for Algo
tictoc::tic()

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

  tictoc::tic()

  stop_factor = trade_params$stop_factor[j]
  profit_factor = trade_params$profit_factor[j]
  sd_fac_1 = trade_params$sd_fac_1[j]
  sd_fac_2 = trade_params$sd_fac_2[j]
  sd_fac_3 = trade_params$sd_fac_3[j]

  analysis_data <-
    get_res_sup_trade_analysis(
      sup_res_data = squeeze_detection,
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
      asset_infor = asset_infor,
      trade_samples = 10000
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

  gc()

  tictoc::toc()

  if(create_new_table == TRUE & j == 1) {
    # write_table_sql_lite(conn = db_con, .data = analysis_data_total, table_name = "sup_res")
    # write_table_sql_lite(conn = db_con, .data = analysis_data_asset, table_name = "sup_res_asset")
    create_new_table <- FALSE
  }

  if(create_new_table == FALSE) {
    append_table_sql_lite(conn = db_con, .data = analysis_data_total, table_name = "sup_res")
    append_table_sql_lite(conn = db_con, .data = analysis_data_asset, table_name = "sup_res_asset")
  }

}

test <- DBI::dbGetQuery(conn = db_con, statement = "SELECT * FROM sup_res")
previous_results_con <-
  glue::glue("C:/Users/Nikhil/Documents/trade_data/sup_res_2025-06-11.db")

previous_results_con <- connect_db(previous_results_con)
previous_results <- DBI::dbGetQuery(conn = previous_results_con,
                                    statement = "SELECT * FROM sup_res" )

current_results <- DBI::dbGetQuery(conn = db_con, statement = "SELECT * FROM sup_res")
current_results <- current_results %>%
  group_by(XX, sd_fac_1, sd_fac_2, sd_fac_3, rolling_slide,
           pois_period, stop_factor, profit_factor, trade_direction) %>%
  summarise(
    Trades = round(mean(Trades, na.rm = T)),
    wins = round(mean(wins, na.rm = T)),
    Perc = mean(Perc, na.rm = T),
    minimal_loss = mean(minimal_loss, na.rm = T),
    maximum_win = mean(maximum_win, na.rm = T),
    win_time_hours = round(mean(win_time_hours, na.rm = T)),
    loss_time_hours = round(mean(loss_time_hours, na.rm = T)),

    Final_Dollars = round(mean(Final_Dollars, na.rm = T)),
    Lowest_Dollars = round(mean(Lowest_Dollars, na.rm = T)),
    Dollars_quantile_25 = round(mean(Dollars_quantile_25, na.rm = T)),
    Dollars_quantile_75 = round(mean(Dollars_quantile_75, na.rm = T)),
    max_Dollars = round(mean(max_Dollars, na.rm = T))
  ) %>%
  # filter(Trades > 8000) %>%
  mutate(
    risk_weighted_return = (maximum_win/minimal_loss)*(Perc) - (1 - Perc)
  ) %>%
  filter(risk_weighted_return > 0.05, profit_factor > stop_factor)

setdiff(names(previous_results), names(current_results))

write_table_sql_lite(.data = current_results,
                     table_name = "sup_res",
                     conn = previous_results_con,
                     overwrite_true = TRUE)

DBI::dbDisconnect(db_con)
DBI::dbDisconnect(previous_results_con)
rm(db_con)
rm(previous_results_con)
gc()


#--------------------------------------------Conplete Restest
sup_res_trade_db <-
  glue::glue("C:/Users/Nikhil/Documents/trade_data/sup_res_2025-07-01.db")
db_con <- connect_db(sup_res_trade_db)
current_results <- DBI::dbGetQuery(conn = db_con, statement = "SELECT * FROM sup_res")
current_results <- current_results %>%
  group_by(XX, sd_fac_1, sd_fac_2, sd_fac_3, rolling_slide,
           pois_period, stop_factor, profit_factor, trade_direction) %>%
  summarise(
    Trades = round(mean(Trades, na.rm = T)),
    wins = round(mean(wins, na.rm = T)),
    Perc = mean(Perc, na.rm = T),
    minimal_loss = mean(minimal_loss, na.rm = T),
    maximum_win = mean(maximum_win, na.rm = T),
    win_time_hours = round(mean(win_time_hours, na.rm = T)),
    loss_time_hours = round(mean(loss_time_hours, na.rm = T)),

    Final_Dollars = round(mean(Final_Dollars, na.rm = T)),
    Lowest_Dollars = round(mean(Lowest_Dollars, na.rm = T)),
    Dollars_quantile_25 = round(mean(Dollars_quantile_25, na.rm = T)),
    Dollars_quantile_75 = round(mean(Dollars_quantile_75, na.rm = T)),
    max_Dollars = round(mean(max_Dollars, na.rm = T))
  ) %>%
  # filter(Trades > 8000) %>%
  mutate(
    risk_weighted_return = (maximum_win/minimal_loss)*(Perc) - (1 - Perc)
  ) %>%
  filter(risk_weighted_return > 0.05, profit_factor > stop_factor) %>%
  filter((Trades + wins) > 9000) %>%
  arrange(desc(risk_weighted_return))

trade_params_new <-
  current_results %>%
  ungroup() %>%
  distinct(sd_fac_1, sd_fac_2, sd_fac_3,
           pois_period, stop_factor, profit_factor, trade_direction) %>%
  filter(trade_direction == "Long")

trade_params_new_XX_ROLL <-
  current_results %>%
  ungroup() %>%
  distinct(XX, rolling_slide)

create_new_table <- TRUE
pois_period <- 10

for (i in 3:dim(trade_params_new_XX_ROLL)[1] ) {

  XX = trade_params_new_XX_ROLL$XX[i] %>% as.numeric()
  rolling_slide = trade_params_new_XX_ROLL$rolling_slide[i] %>% as.numeric()

  tictoc::tic()

  squeeze_detection <-
    get_res_sup_slow_fast_fractal_data(
      starting_asset_data_ask_H1 = starting_asset_data_ask_H1,
      starting_asset_data_ask_15M = starting_asset_data_ask_15M,
      XX = XX,
      rolling_slide = rolling_slide,
      pois_period = pois_period
    )
  tictoc::toc()


for (j in 1:dim(trade_params_new)[1]) {
  tictoc::tic()

  stop_factor = trade_params_new$stop_factor[j]
  profit_factor = trade_params_new$profit_factor[j]
  sd_fac_1 = trade_params_new$sd_fac_1[j]
  sd_fac_2 = trade_params_new$sd_fac_2[j]
  sd_fac_3 = trade_params_new$sd_fac_3[j]

  analysis_data <-
    get_res_sup_trade_analysis(
      sup_res_data = squeeze_detection,
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
      asset_infor = asset_infor,
      trade_samples = 100000
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

  gc()

  tictoc::toc()

  if(create_new_table == TRUE & j == 1) {
    write_table_sql_lite(conn = db_con, .data = analysis_data_total, table_name = "sup_res_FULL_RETEST")
    write_table_sql_lite(conn = db_con, .data = analysis_data_asset, table_name = "sup_res_asset_FULL_RETEST")
    create_new_table <- FALSE
  }

  if(create_new_table == FALSE) {
    append_table_sql_lite(conn = db_con, .data = analysis_data_total, table_name = "sup_res_FULL_RETEST")
    append_table_sql_lite(conn = db_con, .data = analysis_data_asset, table_name = "sup_res_asset_FULL_RETEST")
  }

}

}

full_retest_results <-
  DBI::dbGetQuery(db_con, "SELECT * FROM sup_res_FULL_RETEST") %>%
  group_by(XX, sd_fac_1, sd_fac_2, sd_fac_3, rolling_slide,
           pois_period, stop_factor, profit_factor, trade_direction) %>%
  summarise(
    Trades = round(mean(Trades, na.rm = T)),
    wins = round(mean(wins, na.rm = T)),
    Perc = mean(Perc, na.rm = T),
    minimal_loss = mean(minimal_loss, na.rm = T),
    maximum_win = mean(maximum_win, na.rm = T),
    win_time_hours = round(mean(win_time_hours, na.rm = T)),
    loss_time_hours = round(mean(loss_time_hours, na.rm = T)),

    Final_Dollars = round(mean(Final_Dollars, na.rm = T)),
    Lowest_Dollars = round(mean(Lowest_Dollars, na.rm = T)),
    Dollars_quantile_25 = round(mean(Dollars_quantile_25, na.rm = T)),
    Dollars_quantile_75 = round(mean(Dollars_quantile_75, na.rm = T)),
    max_Dollars = round(mean(max_Dollars, na.rm = T))
  ) %>%
  # filter(Trades > 8000) %>%
  mutate(
    risk_weighted_return = (maximum_win/minimal_loss)*(Perc) - (1 - Perc)
  ) %>%
  filter(risk_weighted_return > 0.05, profit_factor > stop_factor) %>%
  filter((Trades + wins) > 9000) %>%
  arrange(desc(risk_weighted_return))

compare_results <-
  current_results %>%
  left_join(full_retest_results %>%
              dplyr::select(XX, sd_fac_1, sd_fac_2, sd_fac_3, rolling_slide,
                            pois_period, stop_factor, profit_factor, trade_direction,
                            risk_weighted_return_FULL = risk_weighted_return)) %>%
  filter(risk_weighted_return >= 0.09)
