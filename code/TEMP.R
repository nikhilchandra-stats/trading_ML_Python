data_list_dfr_long <- read_csv(paste0(save_path, "/extracted_asset_data_h1_ts_ask_2.csv"))
data_list_dfr_short <- read_csv(paste0(save_path, "/extracted_asset_data_h1_ts_bid_2.csv"))
mean_values_by_asset_for_loop =
  wrangle_asset_data(
    asset_data_daily_raw = data_list_dfr_long,
    summarise_means = TRUE
  )

get_markov_tag_bayes_loop <- function(
    asset_data_combined = data_list_dfr_long,
    training_perc = 1,
    sd_divides = seq(0.25,2,0.25),
    quantile_divides = seq(0.1,0.9, 0.1),
    rolling_period = 400,
    markov_col_on_interest_pos = "Markov_Point_Pos_roll_sum_1.5",
    markov_col_on_interest_neg = "Markov_Point_Neg_roll_sum_-1.5",
    sum_sd_cut_off = "",
    profit_factor  = 10,
    stop_factor  = 10,
    asset_data_daily_raw = data_list_dfr_long,
    mean_values_by_asset_for_loop = mean_values_by_asset_for_loop,
    bayes_prior = 200,
    bayes_prior_trade = 100,
    lm_train_perc = 0.65,
    run_of_prob_prop = 1,
    spread_lead_test = 8,
    asset_infor = asset_infor,
    trade_direction = "Long",
    skip_analysis = FALSE,
    risk_dollar_value = 10,
    currency_conversion = currency_conversion
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
                  prob_current_low,
                  `Markov_Point_Neg_roll_sum_-0.75`,
                  `Markov_Point_Neg_roll_sum_-1.25`,
                  `Markov_Point_Neg_roll_sum_-2`,
                  Total_Negative_Prob
                  )

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
                  prob_current_high, total,
                  `Markov_Point_Pos_roll_sum_0.75`,
                  `Markov_Point_Pos_roll_sum_1.25`,
                  `Markov_Point_Pos_roll_sum_2`,
                  Total_Positive_Prob)

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
                          .before = bayes_prior_trade),

      high_low_prob_diff_mean =
        slider::slide_dbl(.x = high_low_prob_diff,
                          .f = ~ mean(.x, na.rm = T),
                          .before = bayes_prior_trade),
      high_low_prob_diff_sd =
        slider::slide_dbl(.x = high_low_prob_diff,
                          .f = ~ sd(.x, na.rm = T),
                          .before = bayes_prior_trade),

      high_prob_mean =
        slider::slide_dbl(.x = prob_current_high,
                          .f = ~ mean(.x, na.rm = T),
                          .before = bayes_prior_trade),
      low_prob_mean =
        slider::slide_dbl(.x = prob_current_low,
                          .f = ~ mean(.x, na.rm = T),
                          .before = bayes_prior_trade),
      high_prob_mean_long =
        slider::slide_dbl(.x = prob_current_high,
                          .f = ~ mean(.x, na.rm = T),
                          .before = bayes_prior_trade*2),
      low_prob_mean_long =
        slider::slide_dbl(.x = prob_current_low,
                          .f = ~ mean(.x, na.rm = T),
                          .before = bayes_prior_trade*2)
    ) %>%
    ungroup() %>%
    filter(total >= (rolling_period - 0.25*rolling_period) ) %>%
    left_join(mean_values_by_asset_for_loop) %>%
    mutate(
      threshold_value_prob = mean_daily  + run_of_prob_prop*sd_daily
    ) %>%
    group_by(Asset) %>%
    mutate(
      Open_to_Low_lead = lead(Low, spread_lead_test) - Price,
      Open_to_High_lead = lead(High, spread_lead_test) - Price,

      Open_to_Low_lead_log = log(lead(Low, spread_lead_test)/ Price),
      Open_to_High_lead_log = log(lead(High, spread_lead_test)/Price)
    ) %>%
    ungroup()

  mean_values_3_periods_ahead_asset <- full_trading_data %>%
    ungroup() %>%
    mutate(
      Open_to_Low_lead = abs(Open_to_Low_lead),
      Open_to_High_lead = abs(Open_to_High_lead)
    ) %>%
    group_by(Asset) %>%
    summarise(
      mean_daily = mean( Open_to_Low_lead, na.rm = T),
      sd_daily  = sd( Open_to_Low_lead, na.rm = T)
    )

  train_data <-
    full_trading_data %>%
    group_by(Asset) %>%
    slice_head(prop = lm_train_perc ) %>%
    ungroup()

  lm_test_short <- lm(data = train_data,
                formula = Open_to_Low_lead ~ high_prob_mean + low_prob_mean +
                   high_low_prob_diff_sd + sigma_difference +
                  prob_current_high + prob_current_low + posterior_difference +
                  expected_posterior_low + sigma_n_high + posterior_high_diff_sd +
                  posterior_difference_sd +
                  `Markov_Point_Pos_roll_sum_0.75` +
                  `Markov_Point_Pos_roll_sum_1.25` +
                  `Markov_Point_Pos_roll_sum_2` +
                  `Markov_Point_Neg_roll_sum_-0.75`+
                `Markov_Point_Neg_roll_sum_-1.25`+
                `Markov_Point_Neg_roll_sum_-2` +
                  Total_Negative_Prob +
                  Asset)
  lm_test_long <- lm(data = train_data,
                formula = Open_to_High_lead ~ high_prob_mean + low_prob_mean +
                   high_low_prob_diff_sd + sigma_difference +
                  prob_current_high + prob_current_low + posterior_difference +
                  expected_posterior_low + sigma_n_high +
                  `Markov_Point_Pos_roll_sum_0.75` +
                  `Markov_Point_Pos_roll_sum_1.25` +
                  `Markov_Point_Pos_roll_sum_2`  +
                  `Markov_Point_Neg_roll_sum_-0.75`+
                  `Markov_Point_Neg_roll_sum_-1.25`+
                  `Markov_Point_Neg_roll_sum_-2` +
                  Asset +
                  Total_Positive_Prob)
  summary(lm_test_short)
  summary(lm_test_long)

  test_data <-
    full_trading_data %>%
    group_by(Asset) %>%
    slice_tail(prop = (1 - lm_train_perc - 0.01) )

  LM_means_sd <- train_data %>%
    ungroup() %>%
    mutate(
      LM_Short_Pred = predict.lm(lm_test_short, newdata = train_data),
      LM_Long_Pred = predict.lm(lm_test_long, newdata = train_data)
    ) %>%
    group_by(Asset) %>%
    summarise(
      LM_Short_Pred_mean = mean(LM_Short_Pred, na.rm = T),
      LM_Short_Pred_sd = sd(LM_Short_Pred, na.rm = T),

      LM_Long_Pred_mean = mean(LM_Long_Pred, na.rm = T),
      LM_Long_Pred_sd = sd(LM_Long_Pred, na.rm = T)
    ) %>%
    ungroup()

  test_data_with_LM <-
    test_data %>%
    ungroup() %>%
    mutate(
      LM_Short_Pred = predict.lm(lm_test_short, newdata = test_data),
      LM_Long_Pred = predict.lm(lm_test_long, newdata = test_data)
    ) %>%
    left_join(LM_means_sd)

  #-------Long H1: Trades = 109670k+, Perc = 0.517, profit_factor = 10, stop_factor = 10,
  #----------rolling_period = 400, bayes_prior_trade = 50, bayes_prior = 50, LM_SD_Fac = 0.5, spread_lead_test = 10
  # LM_Long_Pred > LM_Long_Pred_mean + LM_Long_Pred_sd*LM_SD_Fac

  tagged_trades <- test_data_with_LM %>%
    mutate(
      trade_col =
        case_when(
          (LM_Long_Pred > LM_Long_Pred_mean + LM_Long_Pred_sd*LM_SD_Fac) ~ "Long"
        )
    ) %>%
    filter(!is.na(trade_col))


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
        asset_data_daily_raw = asset_data_combined_filt,
        stop_factor = stop_factor,
        profit_factor =profit_factor,
        trade_col = "trade_col",
        date_col = "Date",
        start_price_col = "Price",
        mean_values_by_asset = mean_values_by_asset_for_loop
      )

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
