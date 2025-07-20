#' get_cauchy_params_by_asset
#'
#' @param data_to_prob
#'
#' @return
#' @export
#'
#' @examples
get_cauchy_params_by_asset <-
  function(data_to_prob = starting_asset_data_ask_15M_sample,
           col_to_prob = "angle_XX"){

    distinct_assets <- data_to_prob %>% pull(Asset) %>% unique()

    prob_tibble <-
      tibble(
        Asset = distinct_assets
      ) %>%
      mutate(
        cauchy_location = 0,
        cauchy_scale = 0
      )

    for (j in 1:length(distinct_assets)) {

      prob_data_XX <- data_to_prob %>%
        filter(Asset == distinct_assets[j]) %>%
        pull(!!as.name(col_to_prob)) %>% as.numeric()

      cauchy_params_XX <- fitdistrplus::fitdist(prob_data_XX, distr = "cauchy")
      cauchy_loc <- cauchy_params_XX$estimate[1] %>% as.numeric()
      cauchy_scale <- cauchy_params_XX$estimate[2] %>% as.numeric()

      prob_tibble$cauchy_location[j] <- cauchy_loc
      prob_tibble$cauchy_scale[j] <- cauchy_scale

    }

    return(prob_tibble)

  }

#' get_gauss_params_by_asset
#'
#' @param data_to_prob
#'
#' @return
#' @export
#'
#' @examples
get_gauss_params_by_asset <-
  function(data_to_prob = starting_asset_data_ask_15M_sample,
           col_to_prob = "angle_XX"){

    distinct_assets <- data_to_prob %>% pull(Asset) %>% unique()

    prob_tibble <-
      tibble(
        Asset = distinct_assets
      ) %>%
      mutate(
        gauss_loc = 0,
        gauss_scale = 0
      )

    for (j in 1:length(distinct_assets)) {

      prob_data_XX <- data_to_prob %>%
        filter(Asset == distinct_assets[j]) %>%
        pull(!!as.name(col_to_prob)) %>% as.numeric()

      gauss_params_XX <- fitdistrplus::fitdist(prob_data_XX, distr = "norm")
      gauss_loc <- gauss_params_XX$estimate[1] %>% as.numeric()
      gauss_scale <- gauss_params_XX$estimate[2] %>% as.numeric()

      prob_tibble$gauss_loc[j] <- gauss_loc
      prob_tibble$gauss_scale[j] <- gauss_scale

    }

    return(prob_tibble)

  }

#' get_angles_data_cols
#'
#' @param data_to_angle
#' @param XX
#'
#' @return
#' @export
#'
#' @examples
get_angles_data_cols <- function(
    data_to_angle = starting_asset_data_ask_15M,
    XX = 100,
    new_col_name = "angle_XX"
) {

  tagged_data <- data_to_angle %>%
    group_by(Asset) %>%
    mutate(
      !!as.name(new_col_name) := atan(100*( lag(High) - lag(Low, round(XX)) )/lag(Low, round(XX)))*(180/pi),
      !!as.name( glue::glue("{new_col_name}_slow") ) :=
        atan(100*( lag(High) - lag(Low, round(1.5*XX)) )/lag(Low, round(1.5*XX)))*(180/pi),
      !!as.name( glue::glue("{new_col_name}_very_slow") ) :=
        atan(100*( lag(High) - lag(Low, round(2*XX)) )/lag(Low, round(2*XX)))*(180/pi)
    ) %>%
    ungroup() %>%
    filter(!is.na(!!as.name( glue::glue("{new_col_name}_very_slow") )))

  return(tagged_data)

}

#' Title
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
get_angle_fractal_data <-
  function(
    starting_asset_data_ask_H1 = starting_asset_data_ask_H1,
    starting_asset_data_ask_15M = starting_asset_data_ask_15M,
    XX = 200,
    XX_H1 = 50,
    rolling_slide = 200,
    pois_period = 10,
    period_ahead = 20,
    asset_infor,
    currency_conversion
  ) {


    tagged_data_15 <- get_angles_data_cols(
      data_to_angle = starting_asset_data_ask_15M,
      XX = XX,
      new_col_name = "angle_XX"
    ) %>%
      group_by(Asset) %>%
      arrange(Date, .by_group = TRUE) %>%
      group_by(Asset) %>%
      mutate(
        angle_XX_dff = angle_XX - lag(angle_XX),
        angle_XX_dff_ma = slider::slide_dbl(angle_XX_dff, .f = ~ mean(.x, na.rm = T), .before = rolling_slide),
        angle_XX_dff_sd = slider::slide_dbl(angle_XX_dff, .f = ~ sd(.x, na.rm = T), .before = rolling_slide)
      ) %>%
      ungroup()

    tagged_data_H1 <- get_angles_data_cols(
      data_to_angle = starting_asset_data_ask_H1,
      XX = XX_H1,
      new_col_name = "angle_XX_H1"
    ) %>%
      mutate(
        angle_XX_H1_dff = angle_XX_H1 - lag(angle_XX_H1),
        angle_XX_H1_dff_ma = slider::slide_dbl(angle_XX_H1_dff, .f = ~ mean(.x, na.rm = T), .before = rolling_slide),
        angle_XX_H1_dff_sd = slider::slide_dbl(angle_XX_H1_dff, .f = ~ sd(.x, na.rm = T), .before = rolling_slide)
      ) %>%
      ungroup()

    tagged_data_15 <- tagged_data_15 %>%
      left_join(
        tagged_data_H1 %>% dplyr::select(Date, Asset, contains("angle_XX_H1"))
      )

    tagged_data_15 <- tagged_data_15 %>%
      group_by(Asset) %>%
      arrange(Date, .by_group = TRUE) %>%
      group_by(Asset) %>%
      fill(contains("angle_XX_H1"), .direction = "down") %>%
      ungroup()

    sample_data <-
      tagged_data_15 %>%
      filter(!is.na(angle_XX_H1)) %>%
      group_by(Asset) %>%
      arrange(Date, .by_group = TRUE) %>%
      group_by(Asset) %>%
      slice_head(prop = 0.4) %>%
      ungroup()

    prob_tibble_XX <- get_gauss_params_by_asset(
      data_to_prob = sample_data %>% filter(!is.na(angle_XX_dff)),
      col_to_prob = "angle_XX_dff"
    )

    prob_tibble_XX_H1 <- get_gauss_params_by_asset(
      data_to_prob = sample_data %>% filter(!is.na(angle_XX_H1_dff)),
      col_to_prob = "angle_XX_H1_dff"
    ) %>%
      rename(
        gauss_loc_H1  = gauss_loc,
        gauss_scale_H1 = gauss_scale
      )


    testing_data <-
      tagged_data_15 %>%
      group_by(Asset) %>%
      arrange(Date, .by_group = TRUE) %>%
      group_by(Asset) %>%
      slice_tail(prop = 0.47) %>%
      ungroup() %>%
      left_join(prob_tibble_XX) %>%
      left_join(prob_tibble_XX_H1)

    testing2 <- testing_data %>%
      ungroup() %>%
      mutate(rownum = row_number()) %>%
      group_by(rownum) %>%
      mutate(
        predicted_high_XX = period_ahead*quantile(rnorm(n = 1000, mean = gauss_loc, sd = gauss_scale), 0.75),
        predicted_low_XX = period_ahead*quantile(rnorm(n = 1000, mean = gauss_loc, sd = gauss_scale), 0.25),

        predicted_high_XX_H1 = period_ahead*quantile(rnorm(n = 1000, mean = gauss_loc_H1, sd = gauss_scale_H1), 0.75),
        predicted_low_XX_H1 = period_ahead*quantile(rnorm(n = 1000, mean = gauss_loc_H1, sd = gauss_scale_H1), 0.25),

        predicted_ma_dff_XX = quantile(rnorm(n = 1000, mean = angle_XX_dff_ma, sd = angle_XX_dff_sd), 0.5),
        predicted_ma_dff_XX_H1 = quantile(rnorm(n = 1000, mean = angle_XX_H1_dff_ma, sd = angle_XX_H1_dff_sd), 0.5)

      )

    testing3 <- testing2 %>%
      ungroup() %>%
      left_join(asset_infor %>% dplyr::select(Asset = name, pipLocation)) %>%
      mutate(pipLocation = as.numeric(pipLocation)) %>%
      mutate(
        predicted_high_from_now = predicted_high_XX + angle_XX + period_ahead*predicted_ma_dff_XX,
        predicted_low_from_now = predicted_low_XX + angle_XX + period_ahead*predicted_ma_dff_XX,

        predicted_high_from_now_H1 = predicted_high_XX_H1 + angle_XX_H1 + period_ahead*predicted_ma_dff_XX_H1,
        predicted_low_from_now_H1 = predicted_low_XX_H1 + angle_XX_H1 + period_ahead*predicted_ma_dff_XX_H1
      ) %>%
      group_by(Asset) %>%
      mutate(
        predicted_high_from_now_ma = slider::slide_dbl(predicted_high_from_now, .f = ~ mean(.x, na.rm = T), .before = rolling_slide),
        predicted_low_from_now_ma = slider::slide_dbl(predicted_low_from_now, .f = ~ mean(.x, na.rm = T), .before = rolling_slide)

        # predicted_high_from_now_sd = slider::slide_dbl(predicted_high_from_now, .f = ~ sd(.x, na.rm = T), .before = rolling_slide),
        # predicted_low_from_now_sd = slider::slide_dbl(predicted_low_from_now, .f = ~ sd(.x, na.rm = T), .before = rolling_slide),

        # predicted_high_from_now_ma_H1 = slider::slide_dbl(predicted_high_from_now_H1, .f = ~ mean(.x, na.rm = T), .before = rolling_slide),
        # predicted_low_from_now_ma_H1 = slider::slide_dbl(predicted_low_from_now_H1, .f = ~ mean(.x, na.rm = T), .before = rolling_slide),

        # predicted_high_from_now_sd_H1 = slider::slide_dbl(predicted_high_from_now_H1, .f = ~ sd(.x, na.rm = T), .before = rolling_slide),
        # predicted_low_from_now_sd_H1 = slider::slide_dbl(predicted_low_from_now_H1, .f = ~ sd(.x, na.rm = T), .before = rolling_slide),

        # angle_XX_H1_ma = slider::slide_dbl(.x = angle_XX_H1, .f = ~mean(.x, na.rm = T), .before = rolling_slide),
        # angle_XX_H1_sd = slider::slide_dbl(.x = angle_XX_H1, .f = ~sd(.x, na.rm = T), .before = rolling_slide),
        # angle_XX_sd = slider::slide_dbl(.x = angle_XX, .f = ~sd(.x, na.rm = T), .before = rolling_slide),
        # angle_XX_ma = slider::slide_dbl(.x = angle_XX, .f = ~mean(.x, na.rm = T), .before = rolling_slide)
      ) %>%
      ungroup()


    return(testing3)

  }




tag_angle_fractal_data_trades <-
  function(
    fractal_data = testing3,
    mean_values_by_asset_for_loop = mean_values_by_asset_for_loop_15_ask,
    stop_factor = 20,
    profit_factor =40,
    risk_dollar_value = 10,
    sd_fac_1 = 0,
    sd_fac_2 = 1,
    trade_direction = "Long",
    currency_conversion = currency_conversion,
    asset_infor = asset_infor,
    return_analysis = TRUE
  ){

    # Best options: With increasing XX, rolling perod and period ahead we see increasing results
    # XX = 200,
    # XX_H1 = 50,
    # rolling_slide = 200,
    # pois_period = 10,
    # period_ahead = 20
    # Case Statement: predicted_high_from_now_ma > 0 & predicted_low_from_now_ma > 0 & angle_XX_H1 > 0
    # stop_factor = 18,
    # profit_factor =27


    tagged_trades <-
      fractal_data %>%
      mutate(
        trade_col =
          case_when(
            predicted_high_from_now_ma > 0 & predicted_low_from_now_ma > 0 & angle_XX_H1 > 0 ~ trade_direction
          )
      )
      # filter(!is.na(trade_col))

    if(return_analysis == TRUE) {

      long_bayes_loop_analysis_neg <-
        generic_trade_finder_loop(
          tagged_trades = tagged_trades ,
          asset_data_daily_raw = fractal_data,
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
          sd_fac_1 = sd_fac_1
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
          sd_fac_1 = sd_fac_1
        ) %>%
        bind_cols(trade_timings_by_asset_neg)

      return(list(analysis_data_neg, analysis_data_asset_neg, tagged_trades))

    } else {

      return(tagged_trades)
    }

  }
