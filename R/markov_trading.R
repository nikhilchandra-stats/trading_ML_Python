create_markov_model <- function(asset_data_combined = asset_data_combined,
                                training_perc = 0.5,
                                sd_divides = seq(0.5,2,0.5),
                                quantile_divides = seq(0.1,0.9, 0.1),
                                rolling_period = 100,
                                low_or_high = "Low") {

  training_data <-
    asset_data_combined %>%
    group_by(Asset)

  if(training_perc == 1) {
    training_data <- training_data
  } else {
    training_data <- training_data  %>%
      slice_head(prop = training_perc)
  }

  training_data <-
    training_data %>%
    # mutate(
    #   day_of_week = lubridate::wday(Date)
    # ) %>%
    # filter(day_of_week != 6 & day_of_week != 7) %>%
    # dplyr::select(-day_of_week) %>%
    group_by(Asset) %>%
    arrange(Date, .by_group = TRUE) %>%
    mutate(
      Open_To_Var_lag1 =
        case_when(
          low_or_high == "Low" ~ lag(Open) - lag(Low),
          low_or_high == "High" ~ lag(High) - lag(Open)
        )
    ) %>%
    group_by(Asset) %>%
    arrange(Date, .by_group = TRUE) %>%
    group_by(Asset) %>%
    mutate(
      running_sd =
        slider::slide_dbl(.x = Open_To_Var_lag1,
                          .f = ~ sd(.x, na.rm = T),
                          .before = rolling_period, .complete = TRUE),

      running_mid =
        slider::slide_dbl(.x = Open_To_Var_lag1,
                          .f = ~ median(.x, na.rm = T),
                          .before = rolling_period, .complete = TRUE),
    ) %>%
    ungroup() %>%
    filter(!is.na(running_mid))

  for (i in 1:length(sd_divides) ) {

    if(i == 1) {
      quantilised_training_data <-
        training_data %>%
        mutate(
          markov_point_low =
            case_when(
              Open_To_Var_lag1 > running_mid - running_sd*sd_divides[i] &
                Open_To_Var_lag1 <= running_mid ~ sd_divides[i]*-1,

              Open_To_Var_lag1 < running_mid + running_sd*sd_divides[i] &
                Open_To_Var_lag1 >= running_mid ~ sd_divides[i]
            ),

          !!as.name(paste0("Markov_Point_Pos_", sd_divides[i])) :=
            case_when(
              Open_To_Var_lag1 < running_mid + running_sd*sd_divides[i] &
                Open_To_Var_lag1 >= running_mid ~ 1,
              TRUE ~ 0
            ),

          !!as.name(paste0("Markov_Point_Neg_", sd_divides[i]*-1)) :=
            case_when(
              Open_To_Var_lag1 > running_mid - running_sd*sd_divides[i] &
                Open_To_Var_lag1 <= running_mid  ~ 1,
              TRUE ~ 0
            ),

          !!as.name(paste0("Markov_Point_Neg_roll_sum_", sd_divides[i]*-1)) :=
            slider::slide_dbl(.x = !!as.name(paste0("Markov_Point_Neg_", sd_divides[i]*-1)),
                              .f =  ~ sum(.x, na.rm = T),
                              .before = rolling_period),


          !!as.name(paste0("Markov_Point_Pos_roll_sum_", sd_divides[i])) :=
            slider::slide_dbl(.x = !!as.name(paste0("Markov_Point_Pos_", sd_divides[i])),
                              .f =  ~ sum(.x, na.rm = T),
                              .before = rolling_period)

        )

    }

    if(i > 1 & i != length(sd_divides) ) {

      quantilised_training_data <-
        quantilised_training_data %>%
        mutate(
          markov_point_low =
            case_when(
              Open_To_Var_lag1 > running_mid - running_sd*sd_divides[i] &
                Open_To_Var_lag1 <= running_mid - running_sd*sd_divides[i - 1] ~ sd_divides[i]*-1,

              Open_To_Var_lag1 < running_mid + running_sd*sd_divides[i] &
                Open_To_Var_lag1 >= running_mid + running_sd*sd_divides[i - 1] ~ sd_divides[i],

              TRUE ~markov_point_low
            ),

          !!as.name(paste0("Markov_Point_Pos_", sd_divides[i])) :=
            case_when(
              Open_To_Var_lag1 < running_mid + running_sd*sd_divides[i] &
                Open_To_Var_lag1 >= running_mid + running_sd*sd_divides[i - 1]~ 1,
              TRUE ~ 0
            ),

          !!as.name(paste0("Markov_Point_Neg_", sd_divides[i]*-1)) :=
            case_when(
              Open_To_Var_lag1 > running_mid - running_sd*sd_divides[i] &
                Open_To_Var_lag1 <= running_mid - running_sd*sd_divides[i - 1]~ 1,
              TRUE ~ 0
            ),

          !!as.name(paste0("Markov_Point_Neg_roll_sum_", sd_divides[i]*-1)) :=
            slider::slide_dbl(.x = !!as.name(paste0("Markov_Point_Neg_", sd_divides[i]*-1)),
                              .f =  ~ sum(.x, na.rm = T),
                              .before = rolling_period),


          !!as.name(paste0("Markov_Point_Pos_roll_sum_", sd_divides[i])) :=
            slider::slide_dbl(.x = !!as.name(paste0("Markov_Point_Pos_", sd_divides[i])),
                              .f =  ~ sum(.x, na.rm = T),
                              .before = rolling_period)

        )

    }

    if(i == length(sd_divides)) {

      quantilised_training_data <-
        quantilised_training_data %>%
        mutate(
          markov_point_low =
            case_when(
              Open_To_Var_lag1 <= running_mid - running_sd*sd_divides[i] ~ sd_divides[i]*-1,

              Open_To_Var_lag1 >= running_mid + running_sd*sd_divides[i] ~ sd_divides[i],

              TRUE ~markov_point_low
            ),

          !!as.name(paste0("Markov_Point_Pos_", sd_divides[i])) :=
            case_when(
              Open_To_Var_lag1 >= running_mid + running_sd*sd_divides[i] ~ 1,
              TRUE ~ 0
            ),

          !!as.name(paste0("Markov_Point_Neg_", sd_divides[i]*-1)) :=
            case_when(
              Open_To_Var_lag1 <= running_mid - running_sd*sd_divides[i] ~ 1,
              TRUE ~ 0
            ),

          !!as.name(paste0("Markov_Point_Neg_roll_sum_", sd_divides[i]*-1)) :=
            slider::slide_dbl(.x = !!as.name(paste0("Markov_Point_Neg_", sd_divides[i]*-1)),
                              .f =  ~ sum(.x, na.rm = T),
                              .before = rolling_period),


          !!as.name(paste0("Markov_Point_Pos_roll_sum_", sd_divides[i])) :=
            slider::slide_dbl(.x = !!as.name(paste0("Markov_Point_Pos_", sd_divides[i])),
                              .f =  ~ sum(.x, na.rm = T),
                              .before = rolling_period)

        )

    }

  }


  quantilised_training_data2 <-
    quantilised_training_data %>%
    mutate(
      total = rowSums( across( contains("roll_sum") ) )
    )

  for (i in 1:length(sd_divides) ) {

    quantilised_training_data2 <- quantilised_training_data2 %>%
      mutate(
        !!as.name(paste0("Markov_Point_Neg_roll_sum_", sd_divides[i]*-1)) :=
          !!as.name(paste0("Markov_Point_Neg_roll_sum_", sd_divides[i]*-1))/total,

        !!as.name(paste0("Markov_Point_Pos_roll_sum_", sd_divides[i])) :=
          !!as.name(paste0("Markov_Point_Pos_roll_sum_", sd_divides[i]))/total
      )

  }

  quantilised_training_data3 <-
    quantilised_training_data2 %>%
    mutate(
      total_perc =
        rowSums( across( contains("roll_sum") ) )
    )

  return(quantilised_training_data3)

}


get_markov_data_high_low <- function(
    asset_data_combined = asset_data_combined,
    training_perc = 0.5,
    sd_divides = seq(0.5,2,0.5),
    quantile_divides = seq(0.1,0.9, 0.1),
    rolling_period = 100
) {

  Lows <- create_markov_model(
    asset_data_combined = asset_data_combined,
    training_perc = training_perc,
    sd_divides = sd_divides,
    rolling_period = rolling_period,
    low_or_high = "Low"
  )

  Highs <- create_markov_model(
    asset_data_combined = asset_data_combined,
    training_perc = training_perc,
    sd_divides = sd_divides,
    rolling_period = rolling_period,
    low_or_high = "High"
  )

  return(list(Lows, Highs))

}


get_markov_cols_for_trading <- function(
    asset_data_combined = asset_data_combined,
    training_perc = 0.95,
    sd_divides = seq(0.5,2,0.5),
    quantile_divides = seq(0.1,0.9, 0.1),
    rolling_period = 100,
    markov_col_on_interest_pos = "Markov_Point_Pos_roll_sum_1.5",
    markov_col_on_interest_neg = "Markov_Point_Neg_roll_sum_-1.5",
    sum_sd_cut_off = 1.5
) {

  Markov_data <- get_markov_data_high_low(
    asset_data_combined = asset_data_combined,
    training_perc = training_perc,
    sd_divides = sd_divides,
    rolling_period = rolling_period
  ) %>%
    map(
      ~ .x %>%
        group_by(Asset) %>%
        mutate(
          moving_average_markov_prob_pos =
            slider::slide_dbl(.x = !!as.name(markov_col_on_interest_pos),
                              .f = ~ mean(.x, na.rm = T),
                              .before = rolling_period),
          moving_average_markov_prob_neg =
            slider::slide_dbl(.x = !!as.name(markov_col_on_interest_neg),
                              .f = ~ mean(.x, na.rm = T),
                              .before = rolling_period),
          Total_Positive_Prob =
            rowSums( across( contains("Pos_roll_sum") ) ),
          Total_Negative_Prob =
            rowSums( across( contains( "Neg_roll_sum") ) ),
          Total_Positive_Prob_avg =
            slider::slide_dbl(
              .x = Total_Positive_Prob,
              .f = ~ mean(.x, na.rm = T),
              .before = rolling_period
            ),
          Total_Negative_Prob_avg =
            slider::slide_dbl(
              .x = Total_Negative_Prob,
              .f = ~ mean(.x, na.rm = T),
              .before = rolling_period
            ),

          Total_Avg_Prob_Diff =
            Total_Positive_Prob_avg - Total_Negative_Prob_avg,
          Total_Avg_Prob_Diff_SD =
            slider::slide_dbl(.x= Total_Avg_Prob_Diff,
                              .f = ~ sd(.x, na.rm = T),
                              .before = rolling_period
            ),
          Total_Avg_Prob_Diff_Median =
            slider::slide_dbl(.x= Total_Avg_Prob_Diff,
                              .f = ~ mean(.x, na.rm = T),
                              .before = rolling_period
            )

        )
    )

  return(Markov_data)

}

get_markov_tag_pos_neg_diff <- function(
    asset_data_combined = asset_data_combined,
    training_perc = 0.99,
    sd_divides = seq(0.5,2,0.5),
    quantile_divides = seq(0.1,0.9, 0.1),
    rolling_period = 400,
    markov_col_on_interest_pos = "Markov_Point_Pos_roll_sum_1.5",
    markov_col_on_interest_neg = "Markov_Point_Neg_roll_sum_-1.5",
    sum_sd_cut_off = "",
    profit_factor  = 5,
    stop_factor  = 5,
    asset_data_daily_raw = asset_data_combined,
    mean_values_by_asset_for_loop = mean_values_by_asset_for_loop,
    trade_sd_fact = 2.7
) {

  # Best Trades:
  # Rolling Period = 400
  # Profit Factor = 8
  # Stop Factor = 4
  # trade_sd_fact = 3
  #Lows Direction: "Short", "Long"
  #Highs Direction: "Long", "Short"
  #Lows Results Risk Weighted Return: Long 0.09, Short = 0.24
  #Highs Results Risk Weighted Return: Long 0.33, Short = -0.36
  #Logic Pattern: Strong

  # Best Trades:
  # Rolling Period = 400
  # Profit Factor = 8
  # Stop Factor = 4
  # trade_sd_fact = 2
  #Lows Direction: "Short", "Long"
  #Highs Direction: "Long", "Short"
  #Lows Results Risk Weighted Return: Long 0.15, Short = 0.026
  #Highs Results Risk Weighted Return: Long 0.11, Short = 0.049
  #Logic Pattern: Strong

  markov_data <-
    get_markov_cols_for_trading(
      asset_data_combined = asset_data_combined,
      training_perc = training_perc,
      sd_divides = sd_divides,
      rolling_period = rolling_period,
      markov_col_on_interest_pos = markov_col_on_interest_pos,
      markov_col_on_interest_neg = markov_col_on_interest_neg,
      sum_sd_cut_off = sum_sd_cut_off
    )

  Lows <- markov_data[[1]] %>%
    filter(total >= 280) %>%
    ungroup() %>%
    mutate(
      trade_col =
        case_when(
          Total_Avg_Prob_Diff > Total_Avg_Prob_Diff_Median +  trade_sd_fact*Total_Avg_Prob_Diff_SD ~ "Short",
          Total_Avg_Prob_Diff < Total_Avg_Prob_Diff_Median -  trade_sd_fact*Total_Avg_Prob_Diff_SD ~ "Long"
        )

      # trade_col =
      #   case_when(
      #     Total_Avg_Prob_Diff > 0 ~ "Long",
      #     Total_Avg_Prob_Diff < 0 ~ "Short"
      #   )
    )

  Markov_Low_Trades <-
    generic_trade_finder_conservative(
      tagged_trades = Lows,
      asset_data_daily_raw = asset_data_daily_raw,
      stop_factor = stop_factor ,
      profit_factor = profit_factor,
      trade_col = "trade_col",
      date_col = "Date",
      max_hold_period = 100,
      start_price_col = "Price",
      mean_values_by_asset = mean_values_by_asset_for_loop,
      return_summary = TRUE
    ) %>%
    map_dfr(
      ~ .x
    )

  Markov_Low_Trades_Summary <-
    Markov_Low_Trades %>%
    group_by(trade_category, trade_direction) %>%
    summarise(
      Trades = sum(Trades, na.rm = T)
    ) %>%
    pivot_wider(names_from = trade_category, values_from = Trades) %>%
    mutate(
      Perc = `TRUE WIN`/ (`TRUE LOSS` + `TRUE WIN`)
    ) %>%
    mutate(
      risk_weighted_return =
        Perc*(profit_factor/stop_factor) - (1- Perc)*(1)
    )

  #----------------------------------High
  Highs <- markov_data[[2]] %>%
    filter(total >= 280) %>%
    mutate(
      trade_col =
        case_when(
          Total_Avg_Prob_Diff > Total_Avg_Prob_Diff_Median +  trade_sd_fact*Total_Avg_Prob_Diff_SD ~ "Long",
          Total_Avg_Prob_Diff < Total_Avg_Prob_Diff_Median -  trade_sd_fact*Total_Avg_Prob_Diff_SD ~ "Short"
        )
    )

  Markov_High_Trades <-
    generic_trade_finder_conservative(
      tagged_trades = Highs,
      asset_data_daily_raw = asset_data_daily_raw,
      stop_factor = stop_factor ,
      profit_factor = profit_factor,
      trade_col = "trade_col",
      date_col = "Date",
      max_hold_period = 100,
      start_price_col = "Price",
      mean_values_by_asset = mean_values_by_asset_for_loop,
      return_summary = TRUE
    ) %>%
    map_dfr(
      ~ .x
    )

  Markov_High_Trades_Summary <-
    Markov_High_Trades %>%
    group_by(trade_category, trade_direction) %>%
    summarise(
      Trades = sum(Trades, na.rm = T)
    ) %>%
    pivot_wider(names_from = trade_category, values_from = Trades) %>%
    mutate(
      Perc = `TRUE WIN`/ (`TRUE LOSS` + `TRUE WIN`)
    ) %>%
    mutate(
      risk_weighted_return =
        Perc*(profit_factor/stop_factor) - (1- Perc)*(1)
    )

  return(
    list(
      "Trades" = list(Lows, Highs),
      "Trade Summaries" = list(Markov_Low_Trades_Summary, Markov_High_Trades_Summary)
    )
  )

}

get_markov_tag_bayes <- function(
    asset_data_combined = asset_data_combined,
    training_perc = 0.99,
    sd_divides = seq(0.5,2,0.5),
    quantile_divides = seq(0.1,0.9, 0.1),
    rolling_period = 400,
    markov_col_on_interest_pos = "Markov_Point_Pos_roll_sum_1.5",
    markov_col_on_interest_neg = "Markov_Point_Neg_roll_sum_-1.5",
    sum_sd_cut_off = "",
    profit_factor  = 4,
    stop_factor  = 4,
    asset_data_daily_raw = asset_data_combined,
    mean_values_by_asset_for_loop = mean_values_by_asset_for_loop,
    trade_sd_fact_post = 0.5,
    trade_sd_fact_post_high = 1,
    trade_sd_fact_sigma = 0.25,
    bayes_prior = 240,
    bayes_prior_trade = 120
) {



  asset_data_combined_filt <- asset_data_combined %>%
    filter(
      Asset %in% c("HK33_HKD", "USD_JPY",
             # "BTC_USD",
             "AUD_NZD", "GBP_CHF",
             "EUR_HUF", "EUR_ZAR", "NZD_JPY", "EUR_NZD",
             # "USB02Y_USD",
             "XAU_CAD", "GBP_JPY", "EUR_NOK", "USD_SGD", "EUR_SEK",
             "DE30_EUR",
             "AUD_CAD",
             # "UK10YB_GBP",
             "XPD_USD",
             # "UK100_GBP",
             "USD_CHF", "GBP_NZD",
             "GBP_SGD", "USD_SEK", "EUR_SGD", "XCU_USD", "SUGAR_USD", "CHF_ZAR",
             "AUD_CHF", "EUR_CHF", "USD_MXN", "GBP_USD", "WTICO_USD", "EUR_JPY", "USD_NOK",
             "XAU_USD",
             # "DE10YB_EUR",
             "USD_CZK", "AUD_SGD", "USD_HUF", "WHEAT_USD",
             "EUR_USD", "SG30_SGD", "GBP_AUD", "NZD_CAD", "AU200_AUD", "XAG_USD",
             "XAU_EUR", "EUR_GBP", "USD_CNH", "USD_CAD", "NAS100_USD",
             # "USB10Y_USD",
             "EU50_EUR", "NATGAS_USD", "CAD_JPY", "FR40_EUR", "USD_ZAR", "XAU_GBP",
             # "CH20_CHF", "ESPIX_EUR", "XPT_USD",
             "EUR_AUD", "SOYBN_USD", "US2000_USD",
             "BCO_USD", "AUD_USD", "NZD_USD", "NZD_CHF", "WHEAT_USD", "AUD_JPY", "AUD_SEK")
    )

  markov_data <-
    get_markov_cols_for_trading(
      asset_data_combined = asset_data_combined_filt,
      training_perc = training_perc,
      sd_divides = sd_divides,
      rolling_period = rolling_period,
      markov_col_on_interest_pos = markov_col_on_interest_pos,
      markov_col_on_interest_neg = markov_col_on_interest_neg,
      sum_sd_cut_off = sum_sd_cut_off
    )

  #-- Posterior = dnorm(mu_n, sigma_n)
  #-- sigma Prior = lag(running_sd, 50)
  #-- sigma Current = running_sd
  #-- n = rolling period
  #-- sigma_n = sigma*sigma_prior/(n*sigma_prior + sigma)
  #-- mean_x = running_mid
  #-- u_n = sigma_n*(u_prior/sigma_prior + n*running_mid)
  #---- p(u|D) ~ N(u_n, sigma_n)

  Lows <- markov_data[[1]] %>%
    ungroup() %>%
    group_by(Asset) %>%
    mutate(
      sigma_prior = lag(running_sd, bayes_prior),
      sigma_current = running_sd,
      u_prior = lag(running_mid, bayes_prior),
      n_bayes = rolling_period,
      sigma_n = sigma_current*sigma_prior/(n_bayes*sigma_prior + sigma_current),
      u_n= sigma_n*( (u_prior/sigma_prior) + (n_bayes*running_mid/sigma_current)),
      expected_posterior = qnorm(p = 0.5, mean = u_n, sd = sigma_n),
      prob_current_low = round(pnorm(Open_To_Var_lag1, mean = expected_posterior, sd = sigma_current,
                               lower.tail=FALSE), 4)
    ) %>%
    ungroup() %>%
    # filter(total >= 280) %>%
    dplyr::select(Date, Asset, Price, Open, High, Low,
                  Open_To_Low_lag1 = Open_To_Var_lag1,
                  sigma_n_low = sigma_n,
                  u_n_low = u_n,
                  expected_posterior_low = expected_posterior,
                  prob_current_low)

  Highs <- markov_data[[2]] %>%
    ungroup() %>%
    group_by(Asset) %>%
    mutate(
      sigma_prior = lag(running_sd, bayes_prior),
      sigma_current = running_sd,
      u_prior = lag(running_mid, bayes_prior),
      n_bayes = rolling_period,
      sigma_n = sigma_current*sigma_prior/(n_bayes*sigma_prior + sigma_current),
      u_n= sigma_n*( (u_prior/sigma_prior) + (n_bayes*running_mid/sigma_current)),
      expected_posterior = qnorm(p = 0.5, mean = u_n, sd = sigma_n),
      prob_current_high =
        round( pnorm(Open_To_Var_lag1, mean = expected_posterior, sd = sigma_current,
                                lower.tail=FALSE), 4 )
    ) %>%
    ungroup() %>%
    # filter(total >= 280) %>%
    dplyr::select(Date, Asset, Price, Open, High, Low,
                  sigma_n_high = sigma_n, u_n_high = u_n ,
                  Open_To_High_lag1 = Open_To_Var_lag1,
                  expected_posterior_high = expected_posterior,
                  prob_current_high, total)

  full_trading_data <- Lows %>%
    left_join(Highs) %>%
    group_by(Asset) %>%
    mutate(
      posterior_difference = expected_posterior_high - expected_posterior_low,
      posterior_difference_op = expected_posterior_low - expected_posterior_high,
      sigma_difference = sigma_n_high - sigma_n_low,
      posterior_high_diff = expected_posterior_high - lag(expected_posterior_high)
      # probability_
    ) %>%
    group_by(Asset) %>%
    mutate(
      posterior_difference_mean =
        slider::slide_dbl(.x = posterior_difference,
                          .f = ~ mean(.x, na.rm = T),
                          .before = bayes_prior_trade),
      posterior_difference_sd =
        slider::slide_dbl(.x = posterior_difference,
                          .f = ~ sd(.x, na.rm = T),
                          .before = bayes_prior_trade),


      posterior_difference_mean_op =
        slider::slide_dbl(.x = posterior_difference_op,
                          .f = ~ mean(.x, na.rm = T),
                          .before = bayes_prior_trade),
      posterior_difference_sd_op =
        slider::slide_dbl(.x = posterior_difference_op,
                          .f = ~ sd(.x, na.rm = T),
                          .before = bayes_prior_trade),

      sigma_difference_mean =
        slider::slide_dbl(.x = sigma_difference,
                          .f = ~ mean(.x, na.rm = T),
                          .before = bayes_prior_trade),
      sigma_difference_sd =
        slider::slide_dbl(.x = sigma_difference,
                          .f = ~ sd(.x, na.rm = T),
                          .before = bayes_prior_trade),

      posterior_high_diff_mean =
        slider::slide_dbl(.x = posterior_high_diff,
                          .f = ~ mean(.x, na.rm = T),
                          .before = bayes_prior_trade),
      posterior_high_diff_sd =
        slider::slide_dbl(.x = posterior_high_diff,
                          .f = ~ sd(.x, na.rm = T),
                          .before = bayes_prior_trade)
    ) %>%
    ungroup() %>%
    filter(total >= 350)


#--- Long 57% with trade_sd_fact_sigma 2 at sigma_difference add expected_high > expected_low adds 2%
#----- Add expected_posterior_high < expected_posterior_low adds another 1.5%
#------Reduce trade_sd_fact_sigma to 1 increases trades to 2000
#------ Add posterior_difference <= posterior_difference_mean - trade_sd_fact_post*posterior_difference_sd to add 2%
#-------Long: Trades = 5000, Perc = 0.6, prof = 6, stop = 6, rolling = 400, bayes_prior_trade = 100,bayes_prior = 200
  #-posterior_difference <= posterior_difference_mean - trade_sd_fact_post*posterior_difference_sd
    #-trade_sd_fact_post: = 1
  #-sigma_difference <= sigma_difference_mean - trade_sd_fact_sigma*sigma_difference_sd
    #-trade_sd_fact_sigma: = 0.75
  #-sigma_n_high>sigma_n_low

#-------Long: Trades = 8000, Perc = 0.64, prof = 12, stop = 12, rolling = 400, bayes_prior_trade = 120,bayes_prior = 240
    #-posterior_difference <= posterior_difference_mean - trade_sd_fact_post*posterior_difference_sd
      #-trade_sd_fact_post: = 0.5
    #-sigma_difference <= sigma_difference_mean - trade_sd_fact_sigma*sigma_difference_sd
      #-trade_sd_fact_sigma: = 0.25
    #-sigma_n_high>sigma_n_low

  tagged_trades <- full_trading_data %>%
    mutate(
      trade_col =
        case_when(
          posterior_difference <= posterior_difference_mean - trade_sd_fact_post*posterior_difference_sd &
            # posterior_high_diff <= posterior_high_diff_mean - trade_sd_fact_post_high*posterior_high_diff_sd &
          sigma_difference <= sigma_difference_mean - trade_sd_fact_sigma*sigma_difference_sd &
            # prob_current_high < 0.2
            sigma_n_high>sigma_n_low
            # expected_posterior_high < expected_posterior_low
            ~ "Long",
          posterior_difference >= posterior_difference_mean + trade_sd_fact_post*posterior_difference_sd &
            # posterior_high_diff >= posterior_high_diff_mean + trade_sd_fact_post_high*posterior_high_diff_sd &
          sigma_difference >= sigma_difference_mean + trade_sd_fact_sigma*sigma_difference_sd &
            # prob_current_low < 0.2
              sigma_n_high < sigma_n_low
            # expected_posterior_high > expected_posterior_low
          ~ "Short"
        )
    )

  Markov_Trades_Bayes <-
    generic_trade_finder_conservative(
      tagged_trades = tagged_trades,
      asset_data_daily_raw = asset_data_daily_raw,
      stop_factor = stop_factor ,
      profit_factor = profit_factor,
      trade_col = "trade_col",
      date_col = "Date",
      max_hold_period = 200,
      start_price_col = "Price",
      mean_values_by_asset = mean_values_by_asset_for_loop,
      return_summary = TRUE
    ) %>%
    map_dfr(
      ~ .x
    )

  Markov_Trades_Bayes_Summary <-
    Markov_Trades_Bayes %>%
    group_by(trade_category, trade_direction) %>%
    summarise(
      Trades = sum(Trades, na.rm = T)
    ) %>%
    pivot_wider(names_from = trade_category, values_from = Trades) %>%
    mutate(
      Perc = `TRUE WIN`/ (`TRUE LOSS` + `TRUE WIN`)
    ) %>%
    mutate(
      risk_weighted_return =
        Perc*(profit_factor/stop_factor) - (1- Perc)*(1)
    )

  return(
    list(
      "tagged_trades" = tagged_trades,
      "Markov_Trades_Bayes_Summary" = Markov_Trades_Bayes_Summary
    )
  )

}
