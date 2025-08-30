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

db_location <- "C:/Users/nikhi/Documents/Asset Data/Oanda_Asset_Data.db"
start_date_day_15M = "2022-06-01"
end_date_day = today() %>% as.character()

starting_asset_data_ask_15M <-
  get_db_price(
    db_location = db_location,
    start_date = start_date_day_15M,
    end_date = end_date_day,
    bid_or_ask = "ask",
    time_frame = "M15"
  )

mean_values_by_asset_for_loop_15_ask =
  wrangle_asset_data(
    asset_data_daily_raw = starting_asset_data_ask_15M,
    summarise_means = TRUE
  )

gc()

#----------------------------------------- Creating Data for Algo

raw_macro_data <- get_macro_event_data()

# tictoc::tic()

markov_macro_data <-
  get_15_min_markov_data_macro(
    new_15_data_ask = starting_asset_data_ask_15M,
    raw_macro_data = raw_macro_data,
    trade_sd_fact = 0,
    rolling_period = 400,
    mean_values_by_asset_for_loop = mean_values_by_asset_for_loop_15_ask,
    currency_conversion = currency_conversion,
    LM_period_1 = 2,
    LM_period_2 = 10,
    LM_period_3 = 15,
    LM_period_4 = 35,
    MA_lag1 = 15,
    MA_lag2 = 30,
    sd_divides = seq(0.25,2,0.25),
    quantile_divides = seq(0.1,0.9, 0.1)
  )

# tictoc::toc()

# tictoc::tic()

data_with_LM_pred <-
  get_15_min_markov_LM_pred(
    transformed_data = markov_macro_data,
    mean_values_by_asset_for_loop = mean_values_by_asset_for_loop_15_ask,
    LM_save_path = "C:/Users/nikhi/Documents/trade_data/"
  )

best_trades <-
  identify_best_LM_15_Preds(
    result_db_location = "C:/Users/nikhi/Documents/trade_data/LM_15min_markov_sampled.db",
    minimum_risk_avg = 0.05,
    minimum_risk_25 = 0,
    minimum_Final_Dollars_avg = 1000,
    trade_direction = "Long"
  )

db_path <- "C:/Users/nikhi/Documents/trade_data/LM_15min_markov_sampled.db"
db_con <- connect_db(db_path)

best_trades <- best_trades %>%
  arrange(desc(risk_weighted_avg))

trade_params <-
  tibble(
    trade_sd_fact1 = c(2,3,4,5)
  )

trade_params <-
  c(2,3) %>%
  map_dfr(
    ~
      trade_params %>%
      mutate(
        trade_sd_fact2 = .x
      )
  )

trade_params <-
  c(2,3) %>%
  map_dfr(
    ~
      trade_params %>%
      mutate(
        trade_sd_fact3 = .x
      )
  )

trade_params <-
  c(2,3) %>%
  map_dfr(
    ~
      trade_params %>%
      mutate(
        trade_sd_fact4 = .x
      )
  )

trade_params <-
  c(17) %>%
  map_dfr(
    ~ trade_params %>%
      mutate(
        stop_factor = .x
      ) %>%
      mutate(
        profit_factor = stop_factor*1.5
      )
  )

trade_params <-
  c(0.25) %>%
  map_dfr(
    ~ trade_params %>%
      mutate(
        sd_AVG_Prob = .x
      )
  )

new_table = FALSE
best_trades <- trade_params %>%
  arrange(desc(trade_sd_fact1))
names(best_trades)

for (i in 42:dim(best_trades)[1]) {

  trade_sd_fact1 <- best_trades$trade_sd_fact1[i] %>% as.numeric()
  trade_sd_fact2 <- best_trades$trade_sd_fact2[i] %>% as.numeric()
  trade_sd_fact3 <- best_trades$trade_sd_fact3[i] %>% as.numeric()
  trade_sd_fact4 <- best_trades$trade_sd_fact4[i] %>% as.numeric()
  sd_AVG_Prob <- best_trades$sd_AVG_Prob[i] %>% as.numeric()
  stop_factor <- best_trades$stop_factor[i] %>% as.numeric()
  profit_factor <- best_trades$profit_factor[i] %>% as.numeric()

  trade_results <-
    get_analysis_15min_LM(
      modelling_data_for_trade_tag = data_with_LM_pred,
      profit_factor  = profit_factor,
      stop_factor  = stop_factor,
      risk_dollar_value = 10,
      trade_sd_fact1 = trade_sd_fact1,
      trade_sd_fact2 = trade_sd_fact2,
      trade_sd_fact3 = trade_sd_fact3,
      trade_sd_fact4 = trade_sd_fact4,
      sd_AVG_Prob = sd_AVG_Prob,
      rolling_period = 400,
      asset_data_daily_raw = starting_asset_data_ask_15M,
      mean_values_by_asset_for_loop = mean_values_by_asset_for_loop_15_ask,
      trade_sd_fact = trade_sd_fact,
      currency_conversion = currency_conversion,
      Network_Name = "15_min_macro",
      trade_samples = NULL,
      trade_select_samples = NULL
    )

  analysis_data <- trade_results[[1]]
  analysis_data_asset <- trade_results[[2]]

  if(i == 1 & new_table == TRUE) {
    # write_table_sql_lite(.data = analysis_data,
    #                      table_name = "LM_15min_markov_full_test",
    #                      conn = db_con )
    # write_table_sql_lite(.data = analysis_data_asset,
    #                      table_name = "LM_15min_markov_asset_full_test",
    #                      conn = db_con )
  } else {
    append_table_sql_lite(.data = analysis_data,
                          table_name = "LM_15min_markov_full_test",
                          conn = db_con)
    append_table_sql_lite(.data = analysis_data_asset,
                          table_name = "LM_15min_markov_asset_full_test",
                          conn = db_con)
  }
}

test <-
  DBI::dbGetQuery(conn = db_con,
                  "SELECT * FROM LM_15min_markov_full_test") %>%
  filter(Trades >= 500)


