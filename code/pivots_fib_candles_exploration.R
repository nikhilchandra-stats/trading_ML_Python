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

db_location = "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data For EDA.db"
start_date = "2011-01-01"
end_date = today() %>% as.character()

starting_asset_data_ask <-
  get_db_price(
    db_location = db_location,
    start_date = start_date_day_15M,
    end_date = end_date_day,
    bid_or_ask = "ask",
    time_frame = "H1"
  )

mean_values_by_asset_for_loop =
  wrangle_asset_data(
    asset_data_daily_raw = starting_asset_data_ask,
    summarise_means = TRUE
  )

gc()

#' find_pivots_fib_max_min
#'
#' @param starting_asset_data
#' @param how_far_back
#'
#' @return
#' @export
#'
#' @examples
find_pivots_fib_max_min <-
  function(starting_asset_data = starting_asset_data_ask,
           how_far_back = 500) {

    returned <- starting_asset_data %>%
      group_by(Asset) %>%
      mutate(
        line_1 = slider::slide_dbl(.x = Low, .f = ~ min(.x, na.rm = T) ,.before = how_far_back),
        # line_2 = slider::slide_dbl(.x = line_1, .f = ~ min(.x, na.rm = T) ,.before = how_far_back),
        # line_3 = slider::slide_dbl(.x = line_2, .f = ~ min(.x, na.rm = T) ,.before = how_far_back),
        # line_5 = slider::slide_dbl(.x = line_3, .f = ~ min(.x, na.rm = T) ,.before = how_far_back),
        # line_6 = slider::slide_dbl(.x = line_5, .f = ~ min(.x, na.rm = T) ,.before = how_far_back),
        # line_7 = slider::slide_dbl(.x = line_6, .f = ~ min(.x, na.rm = T) ,.before = how_far_back),
        # line_8 = slider::slide_dbl(.x = line_7, .f = ~ min(.x, na.rm = T) ,.before = how_far_back),
        # line_9 = slider::slide_dbl(.x = line_8, .f = ~ min(.x, na.rm = T) ,.before = how_far_back),
        line_10 = slider::slide_dbl(.x = line_1, .f = ~ min(.x, na.rm = T) ,.before = how_far_back*9),

        line_1_max = slider::slide_dbl(.x = High, .f = ~ max(.x, na.rm = T) ,.before = how_far_back),
        # line_2_max = slider::slide_dbl(.x = line_1_max, .f = ~ max(.x, na.rm = T) ,.before = how_far_back),
        # line_3_max = slider::slide_dbl(.x = line_2_max, .f = ~ max(.x, na.rm = T) ,.before = how_far_back),
        # line_5_max = slider::slide_dbl(.x = line_3_max, .f = ~ max(.x, na.rm = T) ,.before = how_far_back),
        # line_6_max = slider::slide_dbl(.x = line_5_max, .f = ~ max(.x, na.rm = T) ,.before = how_far_back),
        # line_7_max = slider::slide_dbl(.x = line_6_max, .f = ~ max(.x, na.rm = T) ,.before = how_far_back),
        # line_8_max = slider::slide_dbl(.x = line_7_max, .f = ~ max(.x, na.rm = T) ,.before = how_far_back),
        # line_9_max = slider::slide_dbl(.x = line_8_max, .f = ~ max(.x, na.rm = T) ,.before = how_far_back),
        line_10_max = slider::slide_dbl(.x = line_1_max, .f = ~ max(.x, na.rm = T) ,.before = how_far_back*9)
      ) %>%
      mutate(across(.cols = contains("line_") & !contains("_max"),
             .f = ~ slider::slide_dbl(.x = ., .f = ~ min(., na.rm = T), .before = how_far_back) )) %>%
      mutate(across(.cols = contains("line_") & contains("_max"),
                    .f = ~ slider::slide_dbl(.x = ., .f = ~ max(., na.rm = T), .before = how_far_back) )) %>%
      mutate(
        perc_line_1 = (Price - line_1)/(line_1_max - line_1),
        perc_line_10 = (Price - line_10)/(line_10_max - line_10),
        perc_line_1_to_10 = (Price - line_10)/(line_1_max - line_10),

        perc_line_1_mean = slider::slide_dbl(.x = perc_line_1, .f = ~ mean(., na.rm = T), .before = how_far_back),
        perc_line_1_sd = slider::slide_dbl(.x = perc_line_1, .f = ~ sd(., na.rm = T), .before = how_far_back),

        perc_line_10_mean = slider::slide_dbl(.x = perc_line_10, .f = ~ mean(., na.rm = T), .before = how_far_back),
        perc_line_10_sd = slider::slide_dbl(.x = perc_line_10, .f = ~ sd(., na.rm = T), .before = how_far_back),

        perc_line_1_to_10_mean = slider::slide_dbl(.x = perc_line_1_to_10, .f = ~ mean(., na.rm = T), .before = how_far_back),
        perc_line_1_to_10_sd = slider::slide_dbl(.x = perc_line_1_to_10, .f = ~ sd(., na.rm = T), .before = how_far_back)

      ) %>%
      ungroup()

      return(returned)

  }


sims_db <- "C:/Users/Nikhil Chandra/Documents/trade_data/SUP_RES_PERC_MODEL_TRADES.db"
sims_db_con <- connect_db(path = sims_db)

analysis_syms = c("EU50_EUR", "AU200_AUD" ,"WTICO_USD",
                  "SPX500_USD", "US2000_USD", "EUR_GBP",
                  "EUR_USD", "EUR_JPY", "GBP_JPY", "USD_CNH",
                  "GBP_USD", "USD_CHF", "USD_CAD", "USD_MXN", "USD_SEK",
                  "USD_NOK", "EUR_SEK", "AUD_USD", "NZD_USD", "NZD_CHF",
                  "SG30_SGD", "XAG_USD", "XCU_USD", "USD_SGD", "USD_CZK",
                  "NATGAS_USD")
how_far_back = 1000

log_cumulative <-
  analysis_syms %>%
  map_dfr(
    ~
      create_log_cumulative_returns(
        asset_data_to_use = starting_asset_data_ask,
        asset_to_use = c(.x[1]),
        price_col = "Open",
        return_long_format = TRUE
      )
  ) %>%
  left_join(
    starting_asset_data_ask %>% distinct(Date, Asset, Price, Open, High, Low)
  ) %>%
  find_pivots_fib_max_min(how_far_back = how_far_back)

log_cumulative %>%
  filter(Asset == "WTICO_USD") %>%
  ggplot(aes(x = Date)) +
  geom_line(aes(y = Price), color = "black") +
  geom_line(aes(y = line_1), color = "red", linetype = "dashed")+
  geom_line(aes(y = line_10), color = "navy", linetype = "dashed") +
  geom_line(aes(y = line_1_max), color = "red", linetype = "dashed")+
  geom_line(aes(y = line_10_max), color = "navy", linetype = "dashed") +
  theme_minimal()


trade_params1 <-
  tibble(
    line_1_sd_perc = c(2.5, 2.5, 2.5),
    line_10_sd_perc = c(2.5, 2.5, 2.5),
    stop_factor = c(4, 5, 6),
    profit_factor = c(8, 10, 12),
    fib_value_var = c(0.613, 0.613, 0.613)
  )

trade_params2 <-
  tibble(
    line_1_sd_perc = c(2, 2, 2),
    line_10_sd_perc = c(2, 2, 2),
    stop_factor = c(4, 5, 6),
    profit_factor = c(8, 10, 12),
    fib_value_var = c(0.713, 0.713, 0.713)
  )

trade_params3 <-
  tibble(
    line_1_sd_perc = c(3, 3, 3),
    line_10_sd_perc = c(3, 3, 3),
    stop_factor = c(4, 5, 6),
    profit_factor = c(8, 10, 12),
    fib_value_var = c(0.713, 0.713, 0.713)
  )

trade_params4 <-
  tibble(
    line_1_sd_perc = c(1.5, 1.5, 1.5),
    line_10_sd_perc = c(1.5, 1.5, 1.5),
    stop_factor = c(4, 5, 6),
    profit_factor = c(8, 10, 12),
    fib_value_var = c(0.382, 0.382, 0.382)
  )

trade_params5 <-
  tibble(
    line_1_sd_perc = c(3.25, 3.25, 3.25),
    line_10_sd_perc = c(3.25, 3.25, 3.25),
    stop_factor = c(4, 5, 6),
    profit_factor = c(8, 10, 12),
    fib_value_var = c(0.81, 0.81, 0.81)
  )

trade_params6 <-
  tibble(
    line_1_sd_perc = c(3.5, 3.5, 3.5),
    line_10_sd_perc = c(3.5, 3.5, 3.5),
    stop_factor = c(4, 5, 6),
    profit_factor = c(8, 10, 12),
    fib_value_var = c(0.11, 0.11, 0.11)
  )

trade_params <-
  # list(trade_params1, trade_params2, trade_params3, trade_params4) %>%
  list(trade_params1, trade_params2, trade_params3, trade_params4,
       trade_params5, trade_params6) %>%
  map_dfr(bind_rows)
lag_period_vec <- c(250,500,750,1000, 1500)

redo_db <- TRUE

for (i in 1:length(lag_period_vec)) {

  how_far_back = lag_period_vec[i] %>% as.numeric()

  log_cumulative <-
    analysis_syms %>%
    map_dfr(
      ~
        create_log_cumulative_returns(
          asset_data_to_use = starting_asset_data_ask,
          asset_to_use = c(.x[1]),
          price_col = "Open",
          return_long_format = TRUE
        )
    ) %>%
    left_join(
      starting_asset_data_ask %>% distinct(Date, Asset, Price, Open, High, Low)
    ) %>%
    find_pivots_min(how_far_back = how_far_back)

  gc()

  for (j in 1:dim(trade_params)[1]) {

    line_1_sd_perc <- trade_params$line_1_sd_perc[j]
    line_10_sd_perc <- trade_params$line_10_sd_perc[j]
    stop_factor <- trade_params$stop_factor[j]
    profit_factor <- trade_params$profit_factor[j]
    fib_value_var <-trade_params$fib_value_var[j]

    tagged_trades_line_1 <-
      log_cumulative %>%
      mutate(
        trade_col =
          case_when(
            perc_line_1 <= perc_line_1_mean - perc_line_1_sd*line_1_sd_perc ~ "Long"
          )
      ) %>%
      filter(!is.na(trade_col))

    tagged_trades_line_10 <-
      log_cumulative %>%
      mutate(
        trade_col =
          case_when(
            perc_line_10 <= perc_line_10_mean - perc_line_10_sd*line_10_sd_perc ~ "Long"
          )
      ) %>%
      filter(!is.na(trade_col))

    tagged_trades_line_1_pos <-
      log_cumulative %>%
      mutate(
        trade_col =
          case_when(
            perc_line_1 >= perc_line_1_mean + perc_line_1_sd*line_1_sd_perc ~ "Long"
          )
      ) %>%
      filter(!is.na(trade_col))

    tagged_trades_line_10_pos <-
      log_cumulative %>%
      mutate(
        trade_col =
          case_when(
            perc_line_10 >= perc_line_10_mean + perc_line_10_sd*line_10_sd_perc ~ "Long"
          )
      ) %>%
      filter(!is.na(trade_col))

    tagged_trades_line_1_Fib <-
      log_cumulative %>%
      mutate(
        trade_col =
          case_when(
            perc_line_1 <= fib_value_var + 0.02 & perc_line_1 >= fib_value_var - 0.02 ~ "Long"
          )
      ) %>%
      filter(!is.na(trade_col))

    trades_to_take_analysis_line_1 <-
      run_pairs_analysis(
        tagged_trades = tagged_trades_line_1,
        stop_factor = stop_factor,
        profit_factor = profit_factor,
        raw_asset_data = log_cumulative,
        risk_dollar_value = 10,
        return_trade_ts = FALSE
      )

    trades_to_take_analysis_line_1_asset <-
      trades_to_take_analysis_line_1[[2]] %>%
      mutate(
        line_1_sd_perc = trade_params$line_1_sd_perc[j],
        line_10_sd_perc = trade_params$line_10_sd_perc[j],
        stop_factor = trade_params$stop_factor[j],
        profit_factor = trade_params$profit_factor[j],
        fib_value_var = trade_params$fib_value_var[j],
        type_trade = "Line1_Neg"
      )

    trades_to_take_analysis_line_10 <-
      run_pairs_analysis(
        tagged_trades = tagged_trades_line_10,
        stop_factor = stop_factor,
        profit_factor = profit_factor,
        raw_asset_data = log_cumulative,
        risk_dollar_value = 10,
        return_trade_ts = FALSE
      )

    trades_to_take_analysis_line_10_asset <-
      trades_to_take_analysis_line_10[[2]] %>%
      mutate(
        line_1_sd_perc = trade_params$line_1_sd_perc[j],
        line_10_sd_perc = trade_params$line_10_sd_perc[j],
        stop_factor = trade_params$stop_factor[j],
        profit_factor = trade_params$profit_factor[j],
        fib_value_var = trade_params$fib_value_var[j],
        type_trade = "Line10_Neg"
      )

    #-------------------------------------------------
    trades_to_take_analysis_line_1_pos <-
      run_pairs_analysis(
        tagged_trades = tagged_trades_line_1_pos,
        stop_factor = stop_factor,
        profit_factor = profit_factor,
        raw_asset_data = log_cumulative,
        risk_dollar_value = 10,
        return_trade_ts = FALSE
      )

    trades_to_take_analysis_line_1_asset_pos <-
      trades_to_take_analysis_line_1_pos[[2]] %>%
      mutate(
        line_1_sd_perc = trade_params$line_1_sd_perc[j],
        line_10_sd_perc = trade_params$line_10_sd_perc[j],
        stop_factor = trade_params$stop_factor[j],
        profit_factor = trade_params$profit_factor[j],
        fib_value_var = trade_params$fib_value_var[j],
        type_trade = "Line1_Pos"
      )

    trades_to_take_analysis_line_10_pos <-
      run_pairs_analysis(
        tagged_trades = tagged_trades_line_10_pos,
        stop_factor = stop_factor,
        profit_factor = profit_factor,
        raw_asset_data = log_cumulative,
        risk_dollar_value = 10,
        return_trade_ts = FALSE
      )

    trades_to_take_analysis_line_10_asset_pos <-
      trades_to_take_analysis_line_10_pos[[2]] %>%
      mutate(
        line_1_sd_perc = trade_params$line_1_sd_perc[j],
        line_10_sd_perc = trade_params$line_10_sd_perc[j],
        stop_factor = trade_params$stop_factor[j],
        profit_factor = trade_params$profit_factor[j],
        fib_value_var = trade_params$fib_value_var[j],
        type_trade = "Line10_Pos"
      )
    #-------------------------------------------------

    trades_to_take_analysis_line_fib <-
      run_pairs_analysis(
        tagged_trades = tagged_trades_line_1_Fib,
        stop_factor = stop_factor,
        profit_factor = profit_factor,
        raw_asset_data = log_cumulative,
        risk_dollar_value = 10,
        return_trade_ts = FALSE
      )

    trades_to_take_analysis_line_fib_asset <-
      trades_to_take_analysis_line_fib[[2]] %>%
      mutate(
        line_1_sd_perc = trade_params$line_1_sd_perc[j],
        line_10_sd_perc = trade_params$line_10_sd_perc[j],
        stop_factor = trade_params$stop_factor[j],
        profit_factor = trade_params$profit_factor[j],
        fib_value_var = trade_params$fib_value_var[j],
        type_trade = "Fib"
      )

    all_trades <-
      trades_to_take_analysis_line_1_asset %>%
      bind_rows(trades_to_take_analysis_line_10_asset) %>%
      bind_rows(trades_to_take_analysis_line_fib_asset)%>%
      bind_rows(trades_to_take_analysis_line_1_asset_pos)%>%
      bind_rows(trades_to_take_analysis_line_10_asset_pos) %>%
      mutate(how_far_back = how_far_back)

    gc()

    if(redo_db == TRUE) {
      write_table_sql_lite(conn = sims_db_con,
                           .data = all_trades,
                           table_name = "SUP_RES_PERC_MODEL_TRADES",
                           overwrite_true = TRUE)
      redo_db = FALSE
    } else {
      append_table_sql_lite(conn = sims_db_con,
                            .data = all_trades,
                            table_name = "SUP_RES_PERC_MODEL_TRADES")
    }

    redo_db = FALSE

  }


}

#' get_best_pivots_fib_trades
#'
#' @param .data
#' @param sims_db
#'
#' @return
#' @export
#'
#' @examples
get_best_pivots_fib_trades <- function(.data = log_cumulative,
                                       sims_db=
                                         "C:/Users/Nikhil Chandra/Documents/trade_data/SUP_RES_PERC_MODEL_TRADES.db",
                                       risk_weighted_return_min = 0.1,
                                       Trades_min =3000,
                                       Trades_max = 6000,
                                       currency_conversion = currency_conversion,
                                       trade_type = NULL,
                                       how_far_back_var = NULL
                                       ){

  sims_db_con <- connect_db(path = sims_db)
  all_sim_results <-
    DBI::dbGetQuery(conn = sims_db_con, statement = "SELECT * FROM SUP_RES_PERC_MODEL_TRADES")
  DBI::dbDisconnect(sims_db_con)

  mean_values_by_asset_for_loop =
    wrangle_asset_data(
      asset_data_daily_raw = .data,
      summarise_means = TRUE
    )

  if(!is.null(trade_type)) {
    all_sim_results <- all_sim_results %>%
      filter(type_trade == trade_type)
  }

  if(!is.null(how_far_back_var)) {
    all_sim_results <- all_sim_results %>%
      filter(how_far_back == how_far_back_var)
  }

  all_sim_results_profit_by_sym <-
    all_sim_results %>%
    filter(risk_weighted_return > risk_weighted_return_min,
           Trades >= Trades_min, Trades <= Trades_max
           ) %>%
    group_by(
      trade_direction, Asset) %>%
    slice_max(Trades) %>%
    group_by(trade_direction, Asset) %>%
    slice_min(profit_factor)

  distinct_how_far_back <-
    all_sim_results_profit_by_sym %>%
    ungroup() %>%
    distinct(how_far_back)

  accumulated_trades <- list()
  c = 0

  for (k in 1:dim(distinct_how_far_back)[1] ) {

    how_far_back_var <- distinct_how_far_back$how_far_back[k] %>% as.numeric()

    internal_trade_params <-
      all_sim_results_profit_by_sym %>%
      ungroup() %>%
      filter(how_far_back == how_far_back_var)

    assets_inernal <-
      internal_trade_params %>%
      distinct(Asset) %>%
      pull(Asset) %>%
      unique()

    data_for_this_run <-
      find_pivots_fib_max_min(starting_asset_data = .data %>% filter(Asset %in% assets_inernal),
                              how_far_back = how_far_back_var) %>%
      slice_max(Date)
    max_date_in_data <- data_for_this_run %>% slice_max(Date) %>% pull(Date) %>% unique()
    message(glue::glue("Latest Date in Fib, Sup, Res Data: {max_date_in_data}"))

    for (o in 1:dim(internal_trade_params)[1] ) {

      trade_type_loop <- internal_trade_params$type_trade[o] %>% as.character()
      asset_loop_temp <- internal_trade_params$Asset[o] %>% as.character()
      line_1_sd_perc_loop <- internal_trade_params$line_1_sd_perc[o] %>% as.numeric()
      line_10_sd_perc_loop <- internal_trade_params$line_10_sd_perc[o] %>% as.numeric()
      fib_value_var_loop <- internal_trade_params$fib_value_var[o] %>% as.numeric()
      profit_factor_loop <- internal_trade_params$profit_factor[o] %>% as.numeric()
      stop_factor_loop <- internal_trade_params$stop_factor[o] %>% as.numeric()
      trade_direction_loop <- internal_trade_params$trade_direction[o] %>% as.character()

      data_for_this_run_loop <- data_for_this_run %>%
        filter(Asset == asset_loop_temp)

      if(trade_type_loop == "Line10_Pos") {
        new_trades_temp <-
          data_for_this_run_loop %>%
          mutate(
            trade_col =
              case_when(
              perc_line_10 >= perc_line_10_mean + perc_line_10_sd*line_10_sd_perc ~ trade_direction_loop
            ),
            profit_factor = profit_factor_loop,
            stop_factor = stop_factor_loop
          )
      }

      if(trade_type_loop == "Line10_Neg") {
        new_trades_temp <-
          data_for_this_run_loop %>%
          mutate(
            trade_col =
              case_when(
                perc_line_10 <= perc_line_10_mean - perc_line_10_sd*line_10_sd_perc ~ trade_direction_loop
              ),
            profit_factor = profit_factor_loop,
            stop_factor = stop_factor_loop
          )
      }

      if(trade_type_loop == "Fib") {
        new_trades_temp <-
          data_for_this_run_loop %>%
          mutate(
            trade_col =
              case_when(
                (perc_line_1 <= fib_value_var_loop + 0.02) &
                  (perc_line_1 >= fib_value_var_loop - 0.02) ~ trade_direction_loop
              ),
            profit_factor = profit_factor_loop,
            stop_factor = stop_factor_loop
          )
      }

      if(trade_type_loop == "Line1_Pos") {
        new_trades_temp <-
          data_for_this_run_loop %>%
          mutate(
            trade_col =
              case_when(
                perc_line_1 >= perc_line_1_mean + perc_line_1_sd*line_1_sd_perc_loop  ~ trade_direction_loop
              ),
            profit_factor = profit_factor_loop,
            stop_factor = stop_factor_loop
          )
      }

      if(trade_type_loop == "Line1_Neg") {
        new_trades_temp <-
          data_for_this_run_loop %>%
          mutate(
            trade_col =
              case_when(
                perc_line_1 <= perc_line_1_mean - perc_line_1_sd*line_1_sd_perc_loop  ~ trade_direction_loop
              ),
            profit_factor = profit_factor_loop,
            stop_factor = stop_factor_loop
          )
      }

      c = c + 1
      accumulated_trades[[c]] <- new_trades_temp

    }

  }

  return(accumulated_trades)

}


tictoc::tic()
log_cumulative <-
  analysis_syms %>%
  map_dfr(
    ~
      create_log_cumulative_returns(
        asset_data_to_use = starting_asset_data_ask,
        asset_to_use = c(.x[1]),
        price_col = "Open",
        return_long_format = TRUE
      )
  ) %>%
  left_join(
    starting_asset_data_ask %>% distinct(Date, Asset, Price, Open, High, Low)
  )

trades_to_take_now <-
  get_best_pivots_fib_trades(
    .data = log_cumulative,
    sims_db=
      "C:/Users/Nikhil Chandra/Documents/trade_data/SUP_RES_PERC_MODEL_TRADES.db",
    risk_weighted_return_min = 0.15,
    Trades_min =2000,
    Trades_max = 20000,
    currency_conversion = currency_conversion,
    trade_type = "Fib",
    how_far_back_var = 500
  )

trades_to_take_now <-
  trades_to_take_now %>%
  map_dfr(bind_rows)

trades_to_take_now_2 <-
  trades_to_take_now %>%
  group_by(Asset) %>%
  slice_min(profit_factor)
tictoc::toc()

trades_to_take_now_stops_profs <- trades_to_take_now %>%
  dplyr::select(Date, Asset, Price, Open, High, Low, profit_factor, stop_factor, trade_col) %>%
  split(.$Asset, drop = FALSE) %>%
  map_dfr(
    ~
      get_stops_profs_volume_trades(
        tagged_trades = .x,
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


all_sim_results <-
  DBI::dbGetQuery(conn = sims_db_con, statement = "SELECT * FROM SUP_RES_PERC_MODEL_TRADES")

sim_summaries <-
  all_sim_results %>%
  group_by(line_1_sd_perc, line_10_sd_perc, stop_factor,
           profit_factor, fib_value_var, type_trade, how_far_back,
           trade_direction) %>%
  summarise(
    Final_Dollars = sum(Final_Dollars, na.rm = T),
    Lowest_Dollars = sum(Lowest_Dollars, na.rm = T),
    Dollars_quantile_25 = sum(Dollars_quantile_25, na.rm = T),
    Trades = sum(Trades, na.rm = T),
    wins = sum(wins, na.rm = T),
    maximum_win = median(maximum_win),
    minimal_loss = median(minimal_loss)
  ) %>%
  mutate(
    Perc = wins/Trades,
    risk_weighted_return = Perc*(maximum_win/minimal_loss) - (1 - Perc)
  )


all_sim_results_syms <- all_sim_results %>%
  filter(risk_weighted_return > 0.1)  %>%
  group_by(line_1_sd_perc, line_10_sd_perc, stop_factor,
           profit_factor, fib_value_var, type_trade, how_far_back,
           trade_direction) %>%
  summarise(Assets = n_distinct(Asset),
            all_syms = paste(Asset, collapse = ", "),
            risk_weighted_return = median(risk_weighted_return),
            Final_Dollars = sum(Final_Dollars, na.rm = T))

all_sim_results_profit_by_sym <- all_sim_results %>%
  filter(risk_weighted_return > 0.12, Trades >= 2000
         # Trades >= 6000, Trades <= 10000
         ) %>%
  group_by(type_trade, fib_value_var, how_far_back) %>%
  summarise(Assets = n_distinct(Asset))
  # group_by(
  #          trade_direction, Asset) %>%
  # slice_max(risk_weighted_return, n = 5) %>%
  # group_by(
  #   trade_direction, Asset) %>%
  # slice_max(Trades) %>%
  # group_by(
  #   trade_direction, Asset) %>%
  # slice_min(profit_factor)

all_sim_results %>%
  filter(type_trade == "Fib") %>%
  ggplot(aes(x = fib_value_var, y = risk_weighted_return)) +
  geom_point() +
  facet_wrap(.~how_far_back, scales = "free")
