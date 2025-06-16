#' get_res_sup_slow_fast_fractal_data
#'
#' @param starting_asset_data_ask_H1
#' @param starting_asset_data_ask_15M
#' @param XX
#' @param rolling_slide
#' @param pois_period
#'
#' @return
#' @export
#'
#' @examples
get_res_sup_slow_fast_fractal_data <-
  function(
    starting_asset_data_ask_H1 = starting_asset_data_ask_H1,
    starting_asset_data_ask_15M = starting_asset_data_ask_15M,
    XX = 100,
    rolling_slide = 200,
    pois_period = 10
  ) {

    starting_asset_data_ask_H1_Tag <-
      starting_asset_data_ask_H1 %>%
      group_by(Asset) %>%
      mutate(
        High_Max_XX_H = slider::slide_dbl(High,
                                          .f = ~ max(.x, na.rm = T),
                                          .before = XX),
        Low_Max_XX_H = slider::slide_dbl(Low,
                                         .f =  ~ min(.x, na.rm = T),
                                         .before = XX),

        High_Max_XX_slow_H = slider::slide_dbl(High,
                                               .f = ~ max(.x, na.rm = T),
                                               .before = XX*2),
        Low_Max_XX_slow_H = slider::slide_dbl(Low,
                                              .f =  ~ min(.x, na.rm = T),
                                              .before = XX*2),

        High_Max_XX_very_slow_H = slider::slide_dbl(High,
                                                    .f = ~ max(.x, na.rm = T),
                                                    .before = XX*4),
        Low_Max_XX_very_slow_H = slider::slide_dbl(Low,
                                                   .f =  ~ min(.x, na.rm = T),
                                                   .before = XX*4)
      ) %>%
      ungroup()

    squeeze_detection <-
      starting_asset_data_ask_15M %>%
      group_by(Asset) %>%
      mutate(

        High_Max_XX = slider::slide_dbl(High,
                                        .f = ~ max(.x, na.rm = T),
                                        .before = XX),
        Low_Max_XX = slider::slide_dbl(Low,
                                       .f =  ~ min(.x, na.rm = T),
                                       .before = XX),

        High_Max_XX_slow = slider::slide_dbl(High,
                                             .f = ~ max(.x, na.rm = T),
                                             .before = XX*2),
        Low_Max_XX_slow = slider::slide_dbl(Low,
                                            .f =  ~ min(.x, na.rm = T),
                                            .before = XX*2),

        High_Max_XX_very_slow = slider::slide_dbl(High,
                                                  .f = ~ max(.x, na.rm = T),
                                                  .before = XX*4),
        Low_Max_XX_very_slow = slider::slide_dbl(Low,
                                                 .f =  ~ min(.x, na.rm = T),
                                                 .before = XX*4)

      ) %>%
      ungroup() %>%
      left_join(
        starting_asset_data_ask_H1_Tag %>%
          dplyr::select(Date, Asset, High_Max_XX_H, Low_Max_XX_H,
                        High_Max_XX_slow_H, Low_Max_XX_slow_H,High_Max_XX_very_slow_H, Low_Max_XX_very_slow_H )
      ) %>%
      dplyr::group_by(Asset) %>%
      arrange(Date, .by_group = TRUE) %>%
      dplyr::group_by(Asset) %>%
      fill(
        c(High_Max_XX_H, Low_Max_XX_H,
          High_Max_XX_slow_H, Low_Max_XX_slow_H,High_Max_XX_very_slow_H, Low_Max_XX_very_slow_H),
        .direction = "down"
      ) %>%
      ungroup()

    squeeze_detection <- squeeze_detection %>%
      mutate(
        Res_Diff_H1_XX = High_Max_XX_H - High,
        Res_Diff_H1_XX_slow = High_Max_XX_slow_H - High,
        Res_Diff_H1_XX_very_slow = High_Max_XX_very_slow_H - High,

        Sup_Diff_H1_XX = Low - Low_Max_XX_H,
        Sup_Diff_H1_XX_slow = Low - Low_Max_XX_slow_H,
        Sup_Diff_H1_XX_very_slow = Low - Low_Max_XX_very_slow_H
      ) %>%
      group_by(Asset) %>%
      mutate(
        Res_Diff_H1_XX_run_mean = slider::slide_dbl(Res_Diff_H1_XX, .f = ~ mean(.x, na.rm = T),
                                                    .before = rolling_slide, .complete = FALSE),
        Res_Diff_H1_XX_slow_run_mean =
          slider::slide_dbl(Res_Diff_H1_XX_slow, .f = ~ mean(.x, na.rm = T), .before = rolling_slide, .complete = FALSE),
        Res_Diff_H1_XX_very_slow_run_mean =
          slider::slide_dbl(Res_Diff_H1_XX_very_slow, .f = ~ mean(.x, na.rm = T), .before = rolling_slide, .complete = FALSE),

        Res_Diff_H1_XX_run_sd = slider::slide_dbl(Res_Diff_H1_XX, .f = ~ sd(.x, na.rm = T), .before = rolling_slide, .complete = FALSE),
        Res_Diff_H1_XX_slow_run_sd =
          slider::slide_dbl(Res_Diff_H1_XX_slow, .f = ~ sd(.x, na.rm = T), .before = rolling_slide, .complete = FALSE),
        Res_Diff_H1_XX_very_slow_run_sd =
          slider::slide_dbl(Res_Diff_H1_XX_very_slow, .f = ~ sd(.x, na.rm = T), .before = rolling_slide, .complete = FALSE),


        Sup_Diff_H1_XX_run_mean = slider::slide_dbl(Sup_Diff_H1_XX, .f = ~ mean(.x, na.rm = T), .before = rolling_slide, .complete = FALSE),
        Sup_Diff_H1_XX_slow_run_mean =
          slider::slide_dbl(Sup_Diff_H1_XX_slow, .f = ~ mean(.x, na.rm = T), .before = rolling_slide, .complete = FALSE),
        Sup_Diff_H1_XX_very_slow_run_mean =
          slider::slide_dbl(Sup_Diff_H1_XX_very_slow, .f = ~ mean(.x, na.rm = T), .before = rolling_slide, .complete = FALSE),

        Sup_Diff_H1_XX_run_sd = slider::slide_dbl(Sup_Diff_H1_XX, .f = ~ sd(.x, na.rm = T), .before = rolling_slide, .complete = FALSE),
        Sup_Diff_H1_XX_slow_run_sd =
          slider::slide_dbl(Sup_Diff_H1_XX_slow, .f = ~ sd(.x, na.rm = T), .before = rolling_slide, .complete = FALSE),
        Sup_Diff_H1_XX_very_slow_run_sd =
          slider::slide_dbl(Sup_Diff_H1_XX_very_slow, .f = ~ sd(.x, na.rm = T), .before = rolling_slide, .complete = FALSE)
      ) %>%
      ungroup()

    return(squeeze_detection)

  }

#' get_sup_res_tagged_trades
#'
#' @param squeeze_detection
#' @param raw_asset_data
#' @param mean_values_by_asset_for_loop
#' @param sd_fac_1
#' @param sd_fac_2
#' @param sd_fac_3
#' @param trade_direction
#'
#' @return
#' @export
#'
#' @examples
get_sup_res_tagged_trades <- function(sup_res_data = squeeze_detection,
                                      sd_fac_1 = 3.5,
                                      sd_fac_2 = 3.5,
                                      sd_fac_3 = 3.5,
                                      trade_direction = "Long") {

  tagged_trades <-
    sup_res_data %>%
    filter(!is.na(Sup_Diff_H1_XX_slow), !is.na(Res_Diff_H1_XX_run_mean)) %>%
    group_by(Asset) %>%
    mutate(
      trade_col =
        case_when(
          Sup_Diff_H1_XX <= Sup_Diff_H1_XX_run_mean - sd_fac_1*Sup_Diff_H1_XX_run_sd ~ trade_direction,
          Sup_Diff_H1_XX_slow <= Sup_Diff_H1_XX_slow_run_mean - sd_fac_2*Sup_Diff_H1_XX_slow_run_sd ~ trade_direction,
          Sup_Diff_H1_XX_very_slow <= Sup_Diff_H1_XX_very_slow_run_mean - sd_fac_3*Sup_Diff_H1_XX_very_slow_run_sd ~ trade_direction
        )
    ) %>%
    filter(trade_col == trade_direction)

  return(tagged_trades)

}

#' get_res_sup_trade_analysis
#'
#' @param squeeze_detection
#' @param raw_asset_data
#' @param mean_values_by_asset_for_loop
#' @param stop_factor
#' @param profit_factor
#' @param risk_dollar_value
#' @param sd_fac_1
#' @param sd_fac_2
#' @param sd_fac_3
#' @param trade_direction
#' @param currency_conversion
#' @param asset_infor
#'
#' @return
#' @export
#'
#' @examples
get_res_sup_trade_analysis <- function(
    squeeze_detection = squeeze_detection,
    raw_asset_data = starting_asset_data_ask_15M,
    mean_values_by_asset_for_loop = mean_values_by_asset_for_loop_15_ask,
    stop_factor = 16,
    profit_factor =16,
    risk_dollar_value = 10,
    sd_fac_1 = 3.5,
    sd_fac_2 = 3.5,
    sd_fac_3 = 3.5,
    trade_direction = "Long",
    currency_conversion = currency_conversion,
    asset_infor = asset_infor
) {


  tagged_trades <-
    get_sup_res_tagged_trades(
      squeeze_detection = squeeze_detection,
      sd_fac_1 = sd_fac_1,
      sd_fac_2 = sd_fac_2,
      sd_fac_3 = sd_fac_3,
      trade_direction = trade_direction
    )

  tagged_trades <-
    squeeze_detection %>%
    filter(!is.na(Sup_Diff_H1_XX_slow), !is.na(Res_Diff_H1_XX_run_mean)) %>%
    group_by(Asset) %>%
    mutate(
      trade_col =
        case_when(
          Sup_Diff_H1_XX <= Sup_Diff_H1_XX_run_mean - sd_fac_1*Sup_Diff_H1_XX_run_sd ~ trade_direction,
          Sup_Diff_H1_XX_slow <= Sup_Diff_H1_XX_slow_run_mean - sd_fac_2*Sup_Diff_H1_XX_slow_run_sd ~ trade_direction,
          Sup_Diff_H1_XX_very_slow <= Sup_Diff_H1_XX_very_slow_run_mean - sd_fac_3*Sup_Diff_H1_XX_very_slow_run_sd ~ trade_direction
        )
    ) %>%
    filter(trade_col == trade_direction)

  long_bayes_loop_analysis_neg <-
    generic_trade_finder_loop(
      tagged_trades = tagged_trades ,
      asset_data_daily_raw = raw_asset_data,
      stop_factor = stop_factor,
      profit_factor =profit_factor,
      trade_col = "trade_col",
      date_col = "Date",
      start_price_col = "Price",
      mean_values_by_asset = mean_values_by_asset_for_loop
    )

  trade_timings_neg <-
    long_bayes_loop_analysis_neg %>%
    mutate(
      ending_date_trade = as_datetime(ending_date_trade),
      dates = as_datetime(dates)
    ) %>%
    mutate(Time_Required = (ending_date_trade - dates)/dhours(1) )

  trade_timings_by_asset_neg <- trade_timings_neg %>%
    mutate(win_loss = ifelse(trade_returns < 0, "loss", "wins") ) %>%
    group_by(win_loss) %>%
    summarise(
      Time_Required = median(Time_Required, na.rm = T)
    ) %>%
    pivot_wider(names_from = win_loss, values_from = Time_Required) %>%
    rename(loss_time_hours = loss,
           win_time_hours = wins)

  analysis_data_neg <-
    generic_anlyser(
      trade_data = long_bayes_loop_analysis_neg %>% rename(Asset = asset),
      profit_factor = profit_factor,
      stop_factor = stop_factor,
      asset_infor = asset_infor,
      currency_conversion = currency_conversion,
      asset_col = "Asset",
      stop_col = "starting_stop_value",
      profit_col = "starting_profit_value",
      price_col = "trade_start_prices",
      trade_return_col = "trade_returns",
      risk_dollar_value = risk_dollar_value,
      grouping_vars = "trade_col"
    ) %>%
    mutate(
      sd_fac_1 = sd_fac_1,
      sd_fac_2 = sd_fac_2,
      sd_fac_3 = sd_fac_3
    ) %>%
    bind_cols(trade_timings_by_asset_neg)

  analysis_data_asset_neg <-
    generic_anlyser(
      trade_data = long_bayes_loop_analysis_neg %>% rename(Asset = asset),
      profit_factor = profit_factor,
      stop_factor = stop_factor,
      asset_infor = asset_infor,
      currency_conversion = currency_conversion,
      asset_col = "Asset",
      stop_col = "starting_stop_value",
      profit_col = "starting_profit_value",
      price_col = "trade_start_prices",
      trade_return_col = "trade_returns",
      risk_dollar_value = risk_dollar_value,
      grouping_vars = "Asset"
    ) %>%
    mutate(
      sd_fac_1 = sd_fac_1,
      sd_fac_2 = sd_fac_2,
      sd_fac_3 = sd_fac_3
    ) %>%
    bind_cols(trade_timings_by_asset_neg)

  return(
    list(
      analysis_data_asset_neg = analysis_data_asset_neg,
      analysis_data_neg = analysis_data_neg
    )
  )

}


#' get_sup_res_trades_to_take
#'
#' @param db_path
#' @param min_risk_win
#' @param min_risk_perc
#' @param max_win_time
#' @param starting_asset_data_ask_H1
#' @param starting_asset_data_ask_15M
#' @param trade_direction
#' @param samples_to_use
#'
#' @return
#' @export
#'
#' @examples
get_sup_res_trades_to_take <- function(db_path = glue::glue("C:/Users/Nikhil Chandra/Documents/trade_data/sup_res_2025-06-11.db"),
                                       min_risk_win = 0.12,
                                       min_risk_perc = 0.1,
                                       max_win_time = 150,
                                       starting_asset_data_ask_H1 = new_H1_data_ask,
                                       starting_asset_data_ask_15M = new_15_data_ask,
                                       mean_values_by_asset = mean_values_by_asset_for_loop_15_ask,
                                       trade_direction = "Long",
                                       samples_to_use = 5000) {


  db_con <- connect_db(db_path)

  starting_asset_data_ask_H1_smple <- starting_asset_data_ask_H1 %>%
    group_by(Asset) %>%
    slice_tail(n = samples_to_use) %>%
    ungroup()

  starting_asset_data_ask_15M_smple <- starting_asset_data_ask_15M %>%
    group_by(Asset) %>%
    slice_tail(n = samples_to_use)%>%
    ungroup()

  current_analysis <-
    DBI::dbGetQuery(conn = db_con, statement = "SELECT * FROM sup_res") %>%
    filter(win_time_hours < max_win_time) %>%
    filter(risk_weighted_return >= min_risk_win,
           Final_Dollars > 0,
           Perc >= min_risk_perc) %>%
    distinct(sd_fac_1, sd_fac_2, sd_fac_3, win_time_hours, XX,
             rolling_slide, pois_period, trade_direction, stop_factor, profit_factor) %>%
    group_by(sd_fac_1, sd_fac_2, sd_fac_3, XX, rolling_slide, pois_period, trade_direction) %>%
    slice_min(win_time_hours)

  all_trades_for_today <- list()

  tictoc::tic()

  for (k in 1:dim(current_analysis)[1] ) {

    XX <- current_analysis$XX[k] %>% as.numeric()
    rolling_slide <- current_analysis$rolling_slide[k] %>% as.numeric()
    pois_period <- current_analysis$pois_period[k] %>% as.numeric()
    stop_factor <- current_analysis$stop_factor[k]
    profit_factor <- current_analysis$profit_factor[k]
    win_time_hours <- current_analysis$win_time_hours[k]

    squeeze_detection <-
      get_res_sup_slow_fast_fractal_data(
        starting_asset_data_ask_H1 = starting_asset_data_ask_H1_smple,
        starting_asset_data_ask_15M = starting_asset_data_ask_15M_smple,
        XX = XX,
        rolling_slide = rolling_slide,
        pois_period = pois_period
      )

    tagged_trades <-
      get_sup_res_tagged_trades(
        sup_res_data = sup_res_data,
        sd_fac_1 = sd_fac_1,
        sd_fac_2 = sd_fac_2,
        sd_fac_3 = sd_fac_3,
        trade_direction = trade_direction
      )

    all_trades_for_today[[k]] <- tagged_trades %>%
      ungroup() %>%
      slice_max(Date) %>%
      filter(!is.na(trade_col), trade_col == trade_direction) %>%
      mutate(
        XX = current_analysis$XX[k] %>% as.numeric(),
        rolling_slide = current_analysis$rolling_slide[k] %>% as.numeric(),
        pois_period = current_analysis$pois_period[k] %>% as.numeric(),
        win_time_hours = current_analysis$win_time_hours[k] %>% as.numeric(),
        sd_fac_1 = sd_fac_1,
        sd_fac_2 = sd_fac_2,
        sd_fac_3 = sd_fac_3,
        trade_direction = trade_direction,
        stop_factor = stop_factor,
        profit_factor = profit_factor
      )

  }

  tictoc::toc()

  all_trades_for_today_dfr <- all_trades_for_today %>%
    keep(~ !is.null(.x)) %>%
    map_dfr(bind_rows)

  if(dim(all_trades_for_today_dfr)[1] > 0) {
    returned_data <-
      all_trades_for_today_dfr %>%
      ungroup() %>%
      left_join(current_analysis) %>%
      group_by(Asset) %>%
      slice_min(win_time_hours) %>%
      ungroup() %>%
      dplyr::group_by(Date, Asset,  Price, Low, High, Open, trade_col) %>%
      slice_max(profit_factor) %>%
      ungroup()

    stops_profs <- returned_data %>%
      distinct(Date, Asset, stop_factor, profit_factor, Price, Low, High, Open)

    returned_data2 <- generic_trade_finder_loop(
      tagged_trades = returned_data ,
      asset_data_daily_raw = starting_asset_data_ask_15M,
      stop_factor = stop_factor,
      profit_factor =profit_factor,
      trade_col = "trade_col",
      date_col = "Date",
      start_price_col = "Price",
      mean_values_by_asset = mean_values_by_asset
    ) %>%
      rename(Date = dates, Asset = asset) %>%
      left_join(stops_profs)


  } else {
    returned_data2 <- NULL
  }

  DBI::dbDisconnect(db_con)
  rm(db_con)

  return(returned_data2)

}

