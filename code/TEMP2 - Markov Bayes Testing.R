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
    sd_divides = seq(0.25,2.5,0.25),
    quantile_divides = seq(0.1,0.9, 0.1),
    rolling_period = 400,
    markov_col_on_interest_pos = "Markov_Point_Pos_roll_sum_1.5",
    markov_col_on_interest_neg = "Markov_Point_Neg_roll_sum_-1.5",
    sum_sd_cut_off = "",
    profit_factor  = 5,
    stop_factor  = 3,
    asset_data_daily_raw = asset_data_combined,
    mean_values_by_asset_for_loop = mean_values_by_asset_for_loop,
    trade_sd_fact = 2,
    currency_conversion = currency_conversion,
    risk_dollar_value = 5
) {

  # Best Trades:
  # rolling_period = 400
  # profit_factor = 5
  # stop_factor = 3
  # trade_sd_fact = 2
  #Lows Direction: "Short", "Long"
  #Highs Direction: "Long", "Short"
  #Lows Results Risk Weighted Return: Long 0.1
  #Highs Results Risk Weighted Return: Long 0.06
  #Trades: 30k


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
    ungroup() %>%
    filter(total >= 0.75*rolling_period) %>%
    mutate(
      trade_col =
        case_when(
          Total_Avg_Prob_Diff > Total_Avg_Prob_Diff_Median +  trade_sd_fact*Total_Avg_Prob_Diff_SD ~ "Short",
          Total_Avg_Prob_Diff < Total_Avg_Prob_Diff_Median -  trade_sd_fact*Total_Avg_Prob_Diff_SD ~ "Long"
        )
    )

  Lows_Trades <-
    generic_trade_finder_loop(
      tagged_trades = Lows ,
      asset_data_daily_raw = asset_data_combined,
      stop_factor = stop_factor,
      profit_factor =profit_factor,
      trade_col = "trade_col",
      date_col = "Date",
      start_price_col = "Price",
      mean_values_by_asset =mean_values_by_asset_for_loop
    )

  Lows_Trades_Analysis <-
    generic_anlyser(
      trade_data = Lows_Trades %>% rename(Asset = asset),
      profit_factor = profit_factor,
      stop_factor = stop_factor,
      asset_infor = asset_infor,
      currency_conversion = currency_conversion,
      asset_col = "Asset",
      stop_col = "starting_stop_value",
      profit_col = "starting_profit_value",
      price_col = "trade_start_prices",
      trade_return_col = "trade_returns",
      risk_dollar_value = risk_dollar_value
    )

  #----------------------------------High
  Highs <- markov_data[[2]] %>%
    ungroup() %>%
    filter(total >= 0.75*rolling_period) %>%
    mutate(
      trade_col =
        case_when(
          Total_Avg_Prob_Diff > Total_Avg_Prob_Diff_Median +  trade_sd_fact*Total_Avg_Prob_Diff_SD ~ "Short",
          Total_Avg_Prob_Diff < Total_Avg_Prob_Diff_Median -  trade_sd_fact*Total_Avg_Prob_Diff_SD ~ "Long"
        )
    )

  Highs_Trades <-
    generic_trade_finder_loop(
      tagged_trades = Highs,
      asset_data_daily_raw = asset_data_combined,
      stop_factor = stop_factor,
      profit_factor =profit_factor,
      trade_col = "trade_col",
      date_col = "Date",
      start_price_col = "Price",
      mean_values_by_asset =mean_values_by_asset_for_loop
    )

  Highs_Trades_Analysis <-
    generic_anlyser(
      trade_data = Highs_Trades %>% rename(Asset = asset),
      profit_factor = profit_factor,
      stop_factor = stop_factor,
      asset_infor = asset_infor,
      currency_conversion = currency_conversion,
      asset_col = "Asset",
      stop_col = "starting_stop_value",
      profit_col = "starting_profit_value",
      price_col = "trade_start_prices",
      trade_return_col = "trade_returns",
      risk_dollar_value = risk_dollar_value
    )


  return(
    list(
      "Trades" = list(Lows, Highs),
      "Trade Summaries" = list(Lows_Trades_Analysis, Highs_Trades_Analysis)
    )
  )

}

get_markov_tag_bayes <- function(
    asset_data_combined = asset_data_combined,
    training_perc = 1,
    sd_divides = seq(0.5,2,0.5),
    quantile_divides = seq(0.1,0.9, 0.1),
    rolling_period = 100,
    markov_col_on_interest_pos = "Markov_Point_Pos_roll_sum_1.5",
    markov_col_on_interest_neg = "Markov_Point_Neg_roll_sum_-1.5",
    sum_sd_cut_off = "",
    profit_factor  = 4,
    stop_factor  = 2,
    asset_data_daily_raw = asset_data_combined,
    mean_values_by_asset_for_loop = mean_values_by_asset_for_loop,
    trade_sd_fact_post = 1.5,
    trade_sd_fact_post_high = 1,
    trade_sd_fact_sigma = 0.25,
    bayes_prior = 50,
    bayes_prior_trade = 50,
    saved_LM = NULL
) {

  #----Short Term - Short = 0.39, risk = 0.18, 15000 Trades
  # trade_sd_fact_post = 1.5
  # trade_sd_fact_sigma = 0.25
  # bayes_prior = 200
  # bayes_prior_trade = 50
  # rolling_period = 100
  # stop = 2, prof = 4

  #----Short Term - Short = 0.43, risk = 0.0766, 10000 Trades
  # trade_sd_fact_post = 1.5
  # trade_sd_fact_sigma = 0.5
  # bayes_prior = 350
  # bayes_prior_trade =  100
  # rolling_period = 100
  # stop = 2, prof = 3

  #----Short Term - Short = 0.40, risk = 0.2024993, 7442 Trades
  #----Restricted Assets
  # trade_sd_fact_post = 1.5
  # trade_sd_fact_sigma = 0.25
  # bayes_prior = 200
  # bayes_prior_trade =  50
  # rolling_period = 100
  # profit_factor = 2, profit_factor = 4

  #----Short Term - Long = 0.37, risk = 0.1154352, 7442 Trades
  #----Restricted Assets
  # trade_sd_fact_post = 1.5
  # trade_sd_fact_sigma = 0.25
  # bayes_prior = 200
  # bayes_prior_trade =  50
  # rolling_period = 100
  # stop_factor = 2, profit_factor = 4


  asset_data_combined_filt <- asset_data_combined

  # asset_data_combined_filt <- asset_data_combined %>%
  #   filter(
  #     Asset %in%
  #       c(
  #         "AUD_USD", "EUR_USD", "USD_JPY", "GBP_USD",
  #         "NZD_USD", "USD_SEK", "USD_NOK", "USD_CHF", "USD_SGD",
  #          "XCU_USD", "EUR_GBP",
  #         "NAS100_USD",
  #         # "NATGAS_USD", "EU50_EUR",  "AU200_AUD","SPX500_USD",
  #         "BCO_USD", "WHEAT_USD",
  #         "XAG_USD", "WTICO_USD",
  #         "USD_MXN")
  #   )

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
    arrange(Date, .by_group = TRUE) %>%
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

  Lows$Date %>% max() - Lows$Date %>% min()

  Highs <- markov_data[[2]] %>%
    ungroup() %>%
    group_by(Asset) %>%
    arrange(Date, .by_group = TRUE) %>%
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
    arrange(Date, .by_group = TRUE) %>%
    group_by(Asset) %>%
    mutate(
      posterior_difference = expected_posterior_high - expected_posterior_low,
      posterior_difference_op = expected_posterior_low - expected_posterior_high,
      sigma_difference = sigma_n_high - sigma_n_low,
      posterior_high_diff = expected_posterior_high - lag(expected_posterior_high)
      # probability_
    ) %>%
    group_by(Asset) %>%
    arrange(Date, .by_group = TRUE) %>%
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
    filter(total >= (rolling_period - 0.25*rolling_period) )


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
  #-trade_sd_fact_post = 0.5
  #-sigma_difference <= sigma_difference_mean - trade_sd_fact_sigma*sigma_difference_sd
  #-trade_sd_fact_sigma = 0.25
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
          # ~ "Long",
          ~ "Short",
          posterior_difference >= posterior_difference_mean + trade_sd_fact_post*posterior_difference_sd &
            # posterior_high_diff >= posterior_high_diff_mean + trade_sd_fact_post_high*posterior_high_diff_sd &
            sigma_difference >= sigma_difference_mean + trade_sd_fact_sigma*sigma_difference_sd &
            # prob_current_low < 0.2
            sigma_n_high < sigma_n_low
          # expected_posterior_high > expected_posterior_low
          # ~ "Short"
          ~ "Long"
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
        Perc*(profit_factor/stop_factor) - (1- Perc)*(1),
      hypothetical_binomial_20_dollars =
        `TRUE WIN`*(20*(profit_factor/stop_factor)) - (`TRUE LOSS`)*(20)
    )

  return(
    list(
      "tagged_trades" = tagged_trades,
      "Markov_Trades_Bayes_Summary" = Markov_Trades_Bayes_Summary
    )
  )

}

get_markov_tag_bayes_loop_LM_ML <- function(
    asset_data_combined = asset_data_combined,
    training_perc = 1,
    sd_divides = seq(0.25,2,0.25),
    quantile_divides = seq(0.1,0.9, 0.1),
    rolling_period = 400,
    markov_col_on_interest_pos = "Markov_Point_Pos_roll_sum_1.5",
    markov_col_on_interest_neg = "Markov_Point_Neg_roll_sum_-1.5",
    sum_sd_cut_off = "",
    profit_factor  = 5,
    stop_factor  = 2,
    asset_data_daily_raw = asset_data_combined,
    mean_values_by_asset_for_loop = mean_values_by_asset_for_loop,
    trade_sd_fact_post = 1,
    bayes_prior = 300,
    bayes_prior_trade = 270,
    asset_infor = asset_infor,
    trade_direction = "Long",
    skip_analysis = FALSE,
    risk_dollar_value = 10,
    currency_conversion = currency_conversion,
    LM_model = NULL,
    LM_mean_preds = NULL
) {

  asset_data_combined_filt <- asset_data_combined

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

  run_of_prob_prop <- 1

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
    left_join(mean_values_by_asset_for_loop) %>%
    group_by(Asset) %>%
    arrange(Date, .by_group = TRUE) %>%
    group_by(Asset) %>%
    mutate(
      sigma_prior = lag(running_sd, bayes_prior),
      sigma_current = running_sd,
      u_prior = lag(running_mid, bayes_prior),
      n_bayes = rolling_period,
      sigma_n = sigma_current*sigma_prior/(n_bayes*sigma_prior + sigma_current),
      u_n= sigma_n*( (u_prior/sigma_prior) + (n_bayes*running_mid/sigma_current)),
      expected_posterior = qnorm(p = 0.5, mean = u_n, sd = sigma_n),
      # prob_current_low = round(pnorm(Open_To_Var_lag1, mean = expected_posterior, sd = sigma_current,
      #                                lower.tail=FALSE), 4)
      prob_current_low = round(pnorm(mean_daily  + run_of_prob_prop*sd_daily , mean = expected_posterior, sd = sigma_current,
                                     lower.tail=FALSE), 4)
    ) %>%
    ungroup() %>%
    dplyr::select(-c(mean_daily, sd_daily, mean_weekly, sd_weekly)) %>%
    # filter(total >= 280) %>%
    dplyr::select(Date, Asset, Price, Open, High, Low,
                  Open_To_Low_lag1 = Open_To_Var_lag1,
                  sigma_n_low = sigma_n,
                  u_n_low = u_n,
                  expected_posterior_low = expected_posterior,
                  prob_current_low)

  Lows$Date %>% max() - Lows$Date %>% min()

  Highs <- markov_data[[2]] %>%
    ungroup() %>%
    left_join(mean_values_by_asset_for_loop) %>%
    group_by(Asset) %>%
    arrange(Date, .by_group = TRUE) %>%
    group_by(Asset) %>%
    mutate(
      sigma_prior = lag(running_sd, bayes_prior),
      sigma_current = running_sd,
      u_prior = lag(running_mid, bayes_prior),
      n_bayes = rolling_period,
      sigma_n = sigma_current*sigma_prior/(n_bayes*sigma_prior + sigma_current),
      u_n= sigma_n*( (u_prior/sigma_prior) + (n_bayes*running_mid/sigma_current)),
      expected_posterior = qnorm(p = 0.5, mean = u_n, sd = sigma_n),
      # prob_current_high =
      #   round( pnorm(Open_To_Var_lag1, mean = expected_posterior, sd = sigma_current,
      #                lower.tail=FALSE), 4 ),
      prob_current_high =
        round( pnorm(mean_daily  + run_of_prob_prop*sd_daily , mean = expected_posterior, sd = sigma_current,
                     lower.tail=FALSE), 4 )
    ) %>%
    ungroup() %>%
    dplyr::select(-c(mean_daily, sd_daily, mean_weekly, sd_weekly)) %>%
    # filter(total >= 280) %>%
    dplyr::select(Date, Asset, Price, Open, High, Low,
                  sigma_n_high = sigma_n, u_n_high = u_n ,
                  Open_To_High_lag1 = Open_To_Var_lag1,
                  expected_posterior_high = expected_posterior,
                  prob_current_high, total)

  full_trading_data <- Lows %>%
    left_join(Highs) %>%
    group_by(Asset) %>%
    arrange(Date, .by_group = TRUE) %>%
    group_by(Asset) %>%
    mutate(
      posterior_difference = expected_posterior_high - expected_posterior_low,
      posterior_difference_op = expected_posterior_low - expected_posterior_high,
      sigma_difference = sigma_n_high - sigma_n_low,
      posterior_high_diff = expected_posterior_high - lag(expected_posterior_high),
      high_low_prob_diff = prob_current_high - prob_current_low
    ) %>%
    group_by(Asset) %>%
    arrange(Date, .by_group = TRUE) %>%
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

      # high_low_prob_diff_mean =
      #   slider::slide_dbl(.x = high_low_prob_diff,
      #                     .f = ~ mean(.x, na.rm = T),
      #                     .before = bayes_prior_trade),
      # high_low_prob_diff_sd =
      #   slider::slide_dbl(.x = high_low_prob_diff,
      #                     .f = ~ sd(.x, na.rm = T),
      #                     .before = bayes_prior_trade),
      #
      # high_prob_mean =
      #   slider::slide_dbl(.x = prob_current_high,
      #                     .f = ~ mean(.x, na.rm = T),
      #                     .before = bayes_prior_trade),
      # low_prob_mean =
      #   slider::slide_dbl(.x = prob_current_low,
      #                     .f = ~ mean(.x, na.rm = T),
      #                     .before = bayes_prior_trade),
      # high_prob_mean_long =
      #   slider::slide_dbl(.x = prob_current_high,
      #                     .f = ~ mean(.x, na.rm = T),
      #                     .before = bayes_prior_trade*2),
      # low_prob_mean_long =
      #   slider::slide_dbl(.x = prob_current_low,
      #                     .f = ~ mean(.x, na.rm = T),
      #                     .before = bayes_prior_trade*2)
    ) %>%
    ungroup() %>%
    filter(total >= (rolling_period - 0.25*rolling_period) ) %>%
    left_join(mean_values_by_asset_for_loop) %>%
    mutate(
      threshold_value_prob = mean_daily  + run_of_prob_prop*sd_daily
    )

  if(is.null(LM_model)|is.null(LM_mean_preds)) {

    full_trading_data_train <-
      full_trading_data %>%
      group_by(Asset) %>%
      slice_head(prop = 0.5) %>%
      group_by(Asset) %>%
      mutate(
        lead_close_to_close = log(Price/lag(Price))
      ) %>%
      ungroup() %>%
      filter(!is.na(lead_close_to_close)) %>%
      filter(!is.na(expected_posterior_low), !is.na(expected_posterior_high), !is.na(prob_current_low),
             !is.na(prob_current_low), !is.na(sigma_n_high), !is.na(sigma_n_low), !is.na(posterior_difference_mean),
             !is.na(posterior_difference_sd), !is.na(posterior_high_diff_mean))

    LM_model <- lm(formula = lead_close_to_close ~
                     expected_posterior_low + expected_posterior_high +
                     prob_current_low + prob_current_low +
                     sigma_n_high + sigma_n_low +
                     posterior_difference_mean  + posterior_difference_sd +
                     posterior_high_diff_mean , data=full_trading_data_train)

    full_trading_data_test <-
      full_trading_data %>%
      group_by(Asset) %>%
      slice_tail(prop = 0.45) %>%
      group_by(Asset) %>%
      ungroup()

    prediction_nn_train <- predict(object = LM_model, newdata = full_trading_data_train)

    prediction_nn_train_means <-
      full_trading_data_train %>%
      mutate(pred = prediction_nn_train) %>%
      group_by(Asset) %>%
      summarise(mean_pred = median(pred, na.rm = T),
                sd_pred = sd(pred, na.rm = T)) %>%
      ungroup()

    saveRDS(LM_model,
            glue::glue("{model_directory}LM_Markov_Bayes.rds"))
    write.csv(prediction_nn_train_means,
              glue::glue("{model_directory}prediction_nn_train_means.csv"),
              row.names = FALSE)

  }

  if(!is.null(LM_model) & !is.null(LM_mean_preds)) {
    full_trading_data_test <- full_trading_data
    prediction_nn_train_means <-LM_mean_preds
  }

  prediction_nn <- predict(object = LM_model, newdata = full_trading_data_test)


  full_trading_data_test <- full_trading_data_test %>%
    mutate(
      Pred = prediction_nn
    ) %>%
    left_join(prediction_nn_train_means)

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
  #-trade_sd_fact_post = 0.5
  #-sigma_difference <= sigma_difference_mean - trade_sd_fact_sigma*sigma_difference_sd
  #-trade_sd_fact_sigma = 0.25
  #-sigma_n_high>sigma_n_low

  #-------Long H1: Trades = 10k+, Perc = 0.411, profit_factor = 10, stop_factor = 6,
  #----------rolling = 400, bayes_prior_trade = 120,bayes_prior = 240
  #-posterior_difference <= posterior_difference_mean - trade_sd_fact_post*posterior_difference_sd
  #-trade_sd_fact_post = 1.5
  #-sigma_difference <= sigma_difference_mean - trade_sd_fact_sigma*sigma_difference_sd
  #-trade_sd_fact_sigma = 0.5
  #-sigma_n_high>sigma_n_low

  #-------Long H1 fast variant: Trades = 8000, Perc = 0.61, profit_factor = 6, stop_factor = 8,
  #----------rolling = 400, bayes_prior_trade = 120,bayes_prior = 240
  #-posterior_difference <= posterior_difference_mean - trade_sd_fact_post*posterior_difference_sd
  #-trade_sd_fact_post = 2
  #-sigma_difference <= sigma_difference_mean - trade_sd_fact_sigma*sigma_difference_sd
  #-trade_sd_fact_sigma = 1
  #-sigma_n_high>sigma_n_low

  tagged_trades <- full_trading_data_test %>%
    mutate(
      trade_col =
        case_when(

          # posterior_difference <= posterior_difference_mean - trade_sd_fact_post*posterior_difference_sd &
          # # posterior_high_diff <= posterior_high_diff_mean - trade_sd_fact_post_high*posterior_high_diff_sd &
          # sigma_difference <= sigma_difference_mean - trade_sd_fact_sigma*sigma_difference_sd &
          # sigma_n_high>sigma_n_low

          Pred >= mean_pred + trade_sd_fact_post*sd_pred

          # high_low_prob_diff >= high_low_prob_diff_mean + high_low_prob_diff_sd*trade_sd_fact_post_high
          ~ "Long"
          # posterior_difference <= posterior_difference_mean - trade_sd_fact_post*posterior_difference_sd &
          #   sigma_difference <= sigma_difference_mean - trade_sd_fact_sigma*sigma_difference_sd &
          #   sigma_n_high>sigma_n_low
          # ~ "Long"
        )
    ) %>%
    filter(!is.na(trade_col)) %>%
    dplyr::select(-mean_pred, -sd_pred, -Pred)

  distinct_assets <- tagged_trades %>% distinct(Asset)
  distinct_assets <- full_trading_data_test %>% distinct(Asset)

  #--------Key Variables for H1:

  if(skip_analysis == TRUE) {
    return(
      list(
        "tagged_trades" = tagged_trades
      )
    )
  } else {

    long_bayes_loop_analysis <-
      generic_trade_finder_loop(
        # tagged_trades = tagged_trades %>% filter(trade_col == trade_direction),
        tagged_trades = tagged_trades ,
        asset_data_daily_raw = full_trading_data_test,
        stop_factor = stop_factor,
        profit_factor =profit_factor,
        trade_col = "trade_col",
        date_col = "Date",
        start_price_col = "Price",
        mean_values_by_asset =
          wrangle_asset_data(
            asset_data_daily_raw = full_trading_data_test,
            summarise_means = TRUE
          )
      )

    distinct_assets <- long_bayes_loop_analysis %>% distinct(asset)

    analysis_data <-
      generic_anlyser(
        trade_data = long_bayes_loop_analysis %>% rename(Asset = asset),
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
        grouping_vars = "trade_direction"
      )

    analysis_data_asset <-
      generic_anlyser(
        trade_data = long_bayes_loop_analysis %>% rename(Asset = asset),
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
        grouping_vars = c("Asset","trade_direction")
      )

    return(
      list(
        "tagged_trades" = tagged_trades,
        "Markov_Trades_Bayes_Summary" = analysis_data
      )
    )

  }

}

generic_anlyser <- function(trade_data = long_bayes_loop_analysis,
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
                            grouping_vars = "trade_direction") {
  trade_data %>%
    # rename(Asset = asset) %>%
    convert_stop_profit_AUD(
      asset_infor = asset_infor,
      asset_col = asset_col,
      stop_col = stop_col,
      profit_col = profit_col,
      price_col = price_col,
      risk_dollar_value = risk_dollar_value,
      returns_present = TRUE,
      trade_return_col = trade_return_col,
      currency_conversion = currency_conversion
    ) %>%
    filter(volume_required > 0) %>%
    mutate(wins = ifelse(trade_return_dollars_AUD > 0, 1, 0)) %>%
    rename(trade_direction = trade_col) %>%
    group_by(across(.cols = matches(c(grouping_vars, "trade_direction", "dates")))) %>%
    summarise(
      Trades = n(),
      wins = sum(wins),
      Total_Dollars = sum(trade_return_dollars_AUD),
      minimal_loss = mean(minimal_loss, na.rm = T),
      maximum_win = mean(maximum_win, na.rm = T)
    ) %>%
    mutate(
      Perc = wins/(Trades)
    ) %>%
    group_by(across(.cols = matches( c(grouping_vars, "trade_direction") )) ) %>%
    arrange(dates, .by_group = TRUE) %>%
    group_by(across(.cols = matches( c(grouping_vars, "trade_direction") ))) %>%
    mutate(
      cumulative_dollars = cumsum(Total_Dollars)
    ) %>%
    group_by(across(.cols = matches( c(grouping_vars, "trade_direction") ))) %>%
    summarise(
      Trades = sum(Trades),
      wins = sum(wins),
      Final_Dollars = sum(Total_Dollars),
      Lowest_Dollars = min(cumulative_dollars),
      Dollars_quantile_25 = quantile(cumulative_dollars, 0.25),
      Dollars_quantile_75 = quantile(cumulative_dollars, 0.75),
      max_Dollars = max(cumulative_dollars),
      minimal_loss = mean(minimal_loss, na.rm = T),
      maximum_win = mean(maximum_win, na.rm = T)
    ) %>%
    mutate(
      Perc = wins/Trades,
      stop_factor = stop_factor,
      profit_factor =profit_factor,
      risk_weighted_return =
        Perc*(maximum_win/minimal_loss) - (1- Perc)*(1)
    )
}
