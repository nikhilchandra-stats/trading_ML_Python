helpeR::load_custom_functions()
library(neuralnet)
raw_macro_data <- get_macro_event_data()

all_aud_symbols <- get_oanda_symbols() %>%
  keep(~ str_detect(.x, "AUD")|str_detect(.x, "USD_SEK|USD_NOK|USD_HUF|USD_ZAR|USD_CNY|USD_MXN"))
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

db_location <- "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data.db"
start_date_day = "2023-03-01"
start_date_day_H1 = "2022-09-01"
start_date_day_D = "2017-01-01"
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

starting_asset_data_ask_D <-
  get_db_price(
    db_location = db_location,
    start_date = start_date_day_D,
    end_date = end_date_day,
    bid_or_ask = "ask",
    time_frame = "D"
  )

mean_values_by_asset_for_loop_15_ask =
  wrangle_asset_data(
    asset_data_daily_raw = starting_asset_data_ask_15M,
    summarise_means = TRUE
  )

starting_asset_data_ask_H1 = starting_asset_data_ask_H1
starting_asset_data_ask_H1 = starting_asset_data_ask_H1
starting_asset_data_ask_15M = starting_asset_data_ask_15M

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
get_res_sup_slow_fast_fractal_data <-
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




tag_res_sup_fast_slow_fractal_trades <-
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
      ) %>%
      filter(!is.na(trade_col))

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


new_H1_data_ask <- starting_asset_data_ask_H1
new_15_data_ask <- starting_asset_data_ask_15M

tictoc::tic()
fractal_data <- get_res_sup_slow_fast_fractal_data(
  starting_asset_data_ask_H1 = new_H1_data_ask %>% group_by(Asset) %>% slice_tail(n = 21000) %>% ungroup() ,
  starting_asset_data_ask_15M = new_15_data_ask %>% group_by(Asset) %>% slice_tail(n = 21000) %>% ungroup() ,
  XX = 200,
  XX_H1 = 50,
  rolling_slide = 200,
  pois_period = 10,
  period_ahead = 20,
  asset_infor = asset_infor,
  currency_conversion
)
tagged_trades <-
  tag_res_sup_fast_slow_fractal_trades(
    fractal_data = fractal_data,
    mean_values_by_asset_for_loop = mean_values_by_asset_for_loop_15_ask,
    stop_factor = 20,
    profit_factor =40,
    risk_dollar_value = 10,
    sd_fac_1 = 0,
    sd_fac_2 = 1,
    trade_direction = "Long",
    currency_conversion = currency_conversion,
    asset_infor = asset_infor,
    return_analysis = FALSE
  )
tictoc::toc()
