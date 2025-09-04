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
      filter(type_trade %in% trade_type)
  }

  if(!is.null(how_far_back_var)) {
    all_sim_results <- all_sim_results %>%
      filter(how_far_back %in% how_far_back_var)
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
                perc_line_10 >= perc_line_10_mean + perc_line_10_sd*line_10_sd_perc_loop ~ trade_direction_loop
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
                perc_line_10 <= perc_line_10_mean - perc_line_10_sd*line_10_sd_perc_loop ~ trade_direction_loop
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
