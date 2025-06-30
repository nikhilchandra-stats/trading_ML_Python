helpeR::load_custom_functions()

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
start_date_day = "2021-01-01"
start_date_day_H1 = "2020-06-01"
start_date_day_D = "2017-01-01"
end_date_day = today() %>% as.character()
# end_date_day = "2023-01-01"

starting_asset_data_ask_15M <-
  get_db_price(
    db_location = db_location,
    start_date = start_date_day,
    end_date = end_date_day,
    bid_or_ask = "bid",
    time_frame = "M15"
  )

starting_asset_data_ask_H1 <-
  get_db_price(
    db_location = db_location,
    start_date = start_date_day_H1,
    end_date = end_date_day,
    bid_or_ask = "bid",
    time_frame = "H1"
  )

starting_asset_data_ask_D <-
  get_db_price(
    db_location = db_location,
    start_date = start_date_day_D,
    end_date = end_date_day,
    bid_or_ask = "bid",
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

#' get_fractal_angle_gaussian
#'
#' @param starting_asset_data_ask_H1
#' @param starting_asset_data_ask_15M
#' @param XX
#' @param XX_H1
#' @param rolling_slide
#' @param pois_period
#' @param period_ahead
#'
#' @return
#' @export
#'
#' @examples
get_fractal_angle_gaussian <- function(
    starting_asset_data_ask_H1 = starting_asset_data_ask_H1,
    starting_asset_data_ask_15M = starting_asset_data_ask_15M,
    XX = 200,
    XX_H1 = 50,
    rolling_slide = 200,
    pois_period = 10,
    period_ahead = 20,
    training_proportion = 0.4) {

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
    slice_head(prop = training_proportion) %>%
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

  return(list("prob_tibble_XX" = prob_tibble_XX, "prob_tibble_XX_H1" = prob_tibble_XX_H1))

}

#' get_angle_gauss_prob_by_asset
#'
#' @param distinct_assets
#' @param prob_tibble_XX
#' @param prob_tibble_XX_H1
#' @param samples_n
#'
#' @return
#' @export
#'
#' @examples
get_angle_gauss_prob_by_asset <- function(
    distinct_assets = starting_asset_data_ask_15M %>% distinct(Asset),
    prob_tibble_XX = angle_data_gauss_probs[[1]],
    prob_tibble_XX_H1 = angle_data_gauss_probs[[2]],
    samples_n = 1000,
    period_ahead = 20
) {

  probs_angle_guass_1 <-
    distinct_assets %>%
    left_join(prob_tibble_XX) %>%
    left_join(prob_tibble_XX_H1)

  probs_angle_guass_2 <- probs_angle_guass_1 %>%
    ungroup() %>%
    mutate(rownum = row_number()) %>%
    group_by(rownum) %>%
    mutate(
      predicted_high_XX = period_ahead*quantile(rnorm(n = samples_n, mean = gauss_loc, sd = gauss_scale), 0.75),
      predicted_low_XX = period_ahead*quantile(rnorm(n = samples_n, mean = gauss_loc, sd = gauss_scale), 0.25),
      predicted_mid_XX = period_ahead*mean(rnorm(n = samples_n, mean = gauss_loc, sd = gauss_scale)),

      predicted_high_XX_H1 = period_ahead*quantile(rnorm(n = samples_n, mean = gauss_loc_H1, sd = gauss_scale_H1), 0.75),
      predicted_low_XX_H1 = period_ahead*quantile(rnorm(n = samples_n, mean = gauss_loc_H1, sd = gauss_scale_H1), 0.25),
      predicted_mid_XX_H1 = period_ahead*mean(rnorm(n = samples_n, mean = gauss_loc_H1, sd = gauss_scale_H1))

      # predicted_ma_dff_XX = quantile(rnorm(n = samples_n, mean = angle_XX_dff_ma, sd = angle_XX_dff_sd), 0.5),
      # predicted_ma_dff_XX_H1 = quantile(rnorm(n = samples_n, mean = angle_XX_H1_dff_ma, sd = angle_XX_H1_dff_sd), 0.5)

    )

  return(probs_angle_guass_2)

}

#' get_angle_guass_data_LM_Model
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
get_angle_guass_data_LM_Model <-
  function(
    starting_asset_data_ask_H1 = starting_asset_data_ask_H1,
    starting_asset_data_ask_15M = starting_asset_data_ask_15M,
    XX = 50,
    XX_H1 = 50,
    rolling_slide = 200,
    pois_period = 10,
    period_ahead = 20,
    asset_infor,
    currency_conversion,
    testing_proportion = 0.25,
    samples_n = 500,
    trade_direction = "Short",
    mean_values_by_asset_for_loop  = mean_values_by_asset_for_loop_15_ask,
    stop_factor = 15,
    profit_factor =20
  ) {


    tictoc::tic()

    tagged_data_15 <-
      c(XX, round(XX*1.5), round(XX*1.75) ,2*XX, round(XX*2.5) ) %>%
      map(
        ~ get_angles_data_cols(
          data_to_angle = starting_asset_data_ask_15M,
          XX = .x,
          new_col_name = paste0("angle_", .x)
        ) %>%
          group_by(Asset) %>%
          arrange(Date, .by_group = TRUE) %>%
          group_by(Asset) %>%
          mutate(
            !!as.name(paste0("angle_", .x, "_dff")) := !!as.name(paste0("angle_", .x)) - lag(!!as.name(paste0("angle_", .x))),
            !!as.name(paste0("angle_", .x, "_dff", "_ma")) := slider::slide_dbl(!!as.name(paste0("angle_", .x, "_dff")), .f = ~ mean(.x, na.rm = T), .before = rolling_slide),
            !!as.name(paste0("angle_", .x, "_dff", "_sd")) := slider::slide_dbl(!!as.name(paste0("angle_", .x, "_dff")), .f = ~ sd(.x, na.rm = T), .before = rolling_slide)
          ) %>%
          ungroup()
      ) %>%
      reduce(left_join)

    gc()

    tagged_data_H1 <- c(XX_H1, round(XX_H1*1.75) ,round(XX_H1*1.5), 2*XX_H1, round(XX_H1*2.5) ) %>%
      map(
        ~ get_angles_data_cols(
          data_to_angle = starting_asset_data_ask_H1,
          XX = .x,
          new_col_name = paste0("angle_H1_", .x)
        ) %>%
          group_by(Asset) %>%
          arrange(Date, .by_group = TRUE) %>%
          group_by(Asset) %>%
          mutate(
            !!as.name(paste0("angle_H1_", .x, "_dff")) := !!as.name(paste0("angle_H1_", .x)) - lag(!!as.name(paste0("angle_H1_", .x))),
            !!as.name(paste0("angle_H1_", .x, "_dff", "_ma")) := slider::slide_dbl(!!as.name(paste0("angle_H1_", .x, "_dff")), .f = ~ mean(.x, na.rm = T), .before = rolling_slide),
            !!as.name(paste0("angle_H1_", .x, "_dff", "_sd")) := slider::slide_dbl(!!as.name(paste0("angle_H1_", .x, "_dff")), .f = ~ sd(.x, na.rm = T), .before = rolling_slide)
          ) %>%
          ungroup()
      )

    gc()

    tagged_data_H1 <- tagged_data_H1 %>% reduce(left_join)

    gc()

    tagged_data_15 <- tagged_data_15 %>%
      left_join(
        tagged_data_H1 %>% dplyr::select(Date, Asset, contains("angle_H1"))
      )

    gc()

    tagged_data_15 <-
      tagged_data_15 %>%
      group_by(Asset) %>%
      arrange(Date, .by_group = TRUE) %>%
      group_by(Asset) %>%
      fill(contains("angle_H1"), .direction = "down") %>%
      ungroup()

    training_data <-
      tagged_data_15 %>%
      group_by(Asset) %>%
      arrange(Date, .by_group = TRUE) %>%
      group_by(Asset) %>%
      slice_head(prop = testing_proportion) %>%
      ungroup() %>%
      mutate(
        trade_col = trade_direction
      )

    trade_results_train <-
      generic_trade_finder_loop(
        tagged_trades = training_data ,
        asset_data_daily_raw = training_data,
        stop_factor = stop_factor,
        profit_factor =profit_factor,
        trade_col = "trade_col",
        date_col = "Date",
        start_price_col = "Price",
        mean_values_by_asset = mean_values_by_asset_for_loop
      )

    gc()

    trade_results_train_bins <-
      trade_results_train %>% distinct(
        dates, asset, trade_returns
      ) %>%
      rename(Date = dates,
             Asset = asset) %>%
      mutate(
        regressor_col = ifelse(trade_returns > 0, 1, 0)
      )

    training_data <- training_data %>% left_join(trade_results_train_bins)

    gc()

    independants <-
      names(training_data) %>%
      keep(~ str_detect(.x, "angle_") ) %>%
      unlist()

    lm_formula <- create_lm_formula(dependant = "regressor_col",
                                    independant = independants)

    lm_model <- lm(formula = lm_formula, data = training_data)
    gc()
    summary(lm_model)

    gc()

    testing_data <-
      tagged_data_15 %>%
      group_by(Asset) %>%
      arrange(Date, .by_group = TRUE) %>%
      group_by(Asset) %>%
      slice_tail(prop = testing_proportion) %>%
      ungroup()

    predictions_testing <- predict.lm(object = lm_model, newdata = testing_data)

    testing_data <- testing_data %>%
      mutate(
        pred = as.numeric(predictions_testing)
      )

    mean_values_of_training <-
      training_data %>%
      mutate(pred = predict.lm(object = lm_model, newdata = training_data)) %>%
      group_by(Asset) %>%
      summarise(
        mean_pred = mean(pred, na.rm = T),
        sd_pred = sd(pred, na.rm = T)
      )


    tictoc::toc()

    return(list("lm_model" = lm_model,
                "testing_data" = testing_data,
                "mean_values_of_training" = mean_values_of_training)
    )

  }

#' get_angle_LM_Model_add_pred
#'
#' @param testing_data
#' @param lm_model
#'
#' @return
#' @export
#'
#' @examples
get_angle_LM_Model_add_pred <- function(
    testing_data = testing_data,
    lm_model = lm_model
) {

  predictions_testing <- predict.lm(object = lm_model, newdata = testing_data)

  testing_data <- testing_data %>%
    mutate(
      pred = as.numeric(predictions_testing)
    )

  return(testing_data)

}

#------------------------------------------Initialisation


angle_data_all <-
  get_angle_guass_data_LM_Model(
    starting_asset_data_ask_H1 = starting_asset_data_ask_H1,
    starting_asset_data_ask_15M = starting_asset_data_ask_15M,
    XX = 50,
    XX_H1 = 50,
    rolling_slide = 200,
    pois_period = 10,
    period_ahead = 20,
    asset_infor,
    currency_conversion,
    testing_proportion = 0.35,
    samples_n = 500,
    trade_direction = "Long",
    mean_values_by_asset_for_loop  = mean_values_by_asset_for_loop_15_ask,
    stop_factor = 17,
    profit_factor =27
  )

angle_data_all$testing_data %>%
  pull(angle_50_very_slow) %>%
  summary()


tag_angle_guass_trades <-
  function(
    testing_data = angle_data_all$testing_data,
    mean_values_by_asset_for_loop = mean_values_by_asset_for_loop_15_ask,
    mean_values_of_training = angle_data_all$mean_values_of_training,
    stop_factor = 17,
    profit_factor = 27,
    risk_dollar_value = 10,
    sd_fac_1 = 2,
    sd_fac_2 = 3,
    trade_direction = "Short",
    currency_conversion = currency_conversion,
    asset_infor = asset_infor,
    return_analysis = TRUE
  ){

    # Best options: With increasing XX, rolling perod and period ahead we see increasing results

    tagged_trades <-
      # fractal_data %>%
      testing_data %>%
      left_join(mean_values_of_training) %>%
      mutate(
        trade_col =
          case_when(

            pred >=  mean_pred  + sd_fac_1*sd_pred ~ trade_direction
            # angle_50_slow >= 70 ~ trade_direction,
            # angle_H1_50 >= 77 ~ trade_direction,
            # angle_100 >= 77 ~ trade_direction,
            # angle_100_slow >=  80 ~ trade_direction,
            # angle_50_very_slow >= 70 ~ trade_direction

          )
      ) %>%
      filter(!is.na(trade_col))

    if(return_analysis == TRUE) {

      long_bayes_loop_analysis_neg <-
        generic_trade_finder_loop(
          tagged_trades = tagged_trades ,
          asset_data_daily_raw = testing_data,
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

    # ts_trade_dat <-
    #   long_bayes_loop_analysis_neg %>%
    #   rename(Asset = asset) %>%
    #   mutate(trade_end_prices = abs(trade_end_prices),
    #          trade_start_prices = abs(trade_start_prices)) %>%
    #   mutate(trade_returns =
    #            case_when(
    #              trade_end_prices < trade_start_prices ~ abs(trade_returns),
    #              trade_end_prices >= trade_start_prices ~ -1*abs(trade_returns),
    #            )
    #          ) %>%
    #   convert_stop_profit_AUD(
    #     asset_infor = asset_infor,
    #     asset_col = "Asset",
    #     stop_col = "starting_stop_value",
    #     profit_col = "starting_profit_value",
    #     price_col = "trade_start_prices",
    #     risk_dollar_value = risk_dollar_value,
    #     returns_present = TRUE,
    #     trade_return_col = "trade_returns",
    #     currency_conversion = currency_conversion
    #   ) %>%
    #   filter(volume_required > 0) %>%
    #   mutate(wins = ifelse(trade_return_dollars_AUD > 0, 1, 0)) %>%
    #   rename(trade_direction = trade_col) %>%
    #   group_by(across(.cols = matches(c("trade_direction", "dates")))) %>%
    #   summarise(
    #     Trades = n(),
    #     wins = sum(wins),
    #     Total_Dollars = sum(trade_return_dollars_AUD),
    #     minimal_loss = mean(minimal_loss, na.rm = T),
    #     maximum_win = mean(maximum_win, na.rm = T)
    #   ) %>%
    #   arrange(dates) %>%
    #   mutate(cumualtive_dollars = cumsum(Total_Dollars))
    #
    # ts_trade_dat %>%
    #   ggplot(aes(x = dates, y = cumualtive_dollars)) +
    #   geom_line()


  }

#----------------------------------------- Creating Data for Algo
tictoc::tic()

trade_results_db <- "C:/Users/Nikhil Chandra/Documents/trade_data/angle_gauss_trades.db"
db_con <- connect_db(trade_results_db)
all_dates_in_data <- starting_asset_data_ask_15M %>% pull(Date) %>% unique()
max_obs <- 40000
max_date <- max(all_dates_in_data) - days( floor((30000*15)/(24*60)) )
relavent_dates <- all_dates_in_data %>% keep( ~ .x <= max_date)
date_sample <- relavent_dates %>% sample(100)

trade_params <- tibble(XX = c(25, 100, 200, 300, 400))
trade_params2 <- c(25,50,75,100) %>%
  map_dfr(~  trade_params %>% mutate(XX_H1 = .x))
trade_params3 <- c(50,100,150,200,250) %>%
  map_dfr(~  trade_params2 %>% mutate(rolling_slide = .x))
trade_params4 <- c(10,15,20,25,30,35) %>%
  map_dfr(~  trade_params3 %>% mutate(period_ahead = .x))
trade_params5 <- c(15,20,25,30) %>%
  map_dfr(~  trade_params4 %>% mutate(stop_factor = .x))
trade_params6 <- c(15,20,25,30) %>%
  map_dfr(~  trade_params5 %>% mutate(profit_factor = .x))
trade_params6 <- trade_params6 %>% filter(profit_factor > stop_factor)
c = 0
samples = 5000
for (j in 1:1 ) {

  for (i in 95:length(date_sample)) {

    loop_data_15 <-
      starting_asset_data_ask_15M %>%
      filter(Date >= date_sample[i]) %>%
      group_by(Asset) %>%
      slice_head(n = samples) %>%
      ungroup()

  tictoc::tic()
    angle_data_all <-
      get_angle_guass_data_LM_Model(
        starting_asset_data_ask_H1 = starting_asset_data_ask_H1,
        starting_asset_data_ask_15M = loop_data_15,
        XX = 50,
        XX_H1 = 50,
        rolling_slide = 200,
        pois_period = 10,
        period_ahead = 20,
        asset_infor,
        currency_conversion,
        testing_proportion = 0.35,
        samples_n = 500,
        trade_direction = "Long",
        mean_values_by_asset_for_loop  = mean_values_by_asset_for_loop_15_ask,
        stop_factor = 17,
        profit_factor =27
      )
    tictoc::toc()

    analysis_list <-
      tag_angle_guass_trades(
        testing_data = angle_data_all$testing_data,
        mean_values_by_asset_for_loop = mean_values_by_asset_for_loop_15_ask,
        mean_values_of_training = angle_data_all$mean_values_of_training,
        stop_factor = 17,
        profit_factor = 27,
        risk_dollar_value = 10,
        sd_fac_1 = 2,
        sd_fac_2 = 3,
        trade_direction = "Short",
        currency_conversion = currency_conversion,
        asset_infor = asset_infor,
        return_analysis = TRUE
      )

    analysis_data <- analysis_list[[1]] %>%
      mutate(
        sample_date = date_sample[i],
        samples = samples
      )

    analysis_data_asset <- analysis_list[[2]] %>%
      mutate(
        sample_date = date_sample[i],
        samples = samples
      )

    if(i == 1) {
      write_table_sql_lite(.data = analysis_data, table_name = "angle_gauss", conn = db_con, overwrite_true = TRUE)
      write_table_sql_lite(.data = analysis_data_asset, table_name = "angle_gauss_asset", conn = db_con, overwrite_true = TRUE)
    } else {
      append_table_sql_lite(.data = analysis_data, table_name = "angle_gauss", conn = db_con)
      append_table_sql_lite(.data = analysis_data_asset, table_name = "angle_gauss_asset", conn = db_con)
    }

  }

}

test <- DBI::dbGetQuery(conn = db_con, "SELECT * FROM angle_gauss") %>%
  mutate(
    total = sum(Trades),
    all_wins = sum(wins),
    total_perc = all_wins/total,
    total_risk_return = ( (maximum_win/minimal_loss) *total_perc) - ((1- total_perc)*(1))
  )
