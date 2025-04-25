get_volatility_trades <- function(
    asset_data_daily_raw_week = asset_data_daily_raw_week,
    asset_data_daily_raw = asset_data_daily_raw,
    mean_values_by_asset_for_loop = mean_values_by_asset_for_loop,
    sd_facs = 0,
    stop_fac = 0.32,
    prof_fac =0.8,
    risk_dollar_value = 100
  ) {

  asset_infor <- get_instrument_info()

  vol_data <- asset_data_daily_raw_week %>%
    group_by(Asset) %>%
    arrange(week_date, .by_group = TRUE) %>%
    ungroup() %>%
    left_join(
      asset_infor %>% dplyr::select(Asset = name, pipLocation)
    ) %>%
    mutate(
      absolute_open_to_high = abs(weekly_high - week_start_price)/(10^pipLocation),
      absolute_open_to_low = abs(week_start_price - weekly_low)/(10^pipLocation)
    ) %>%
    group_by(Asset) %>%
    mutate(
      absolute_open_to_high_mean = mean(absolute_open_to_high, na.rm = T),
      absolute_open_to_low_mean = mean(absolute_open_to_low, na.rm = T),

      absolute_open_to_high_sd = sd(absolute_open_to_high, na.rm = T),
      absolute_open_to_low_sd = sd(absolute_open_to_low, na.rm = T)
    ) %>%
    ungroup()%>%
    group_by(Asset) %>%
    arrange(week_date, .by_group = TRUE) %>%
    group_by(Asset) %>%
    mutate(
      lag_open_to_high = lag(absolute_open_to_high),
      lag_open_to_low = lag(absolute_open_to_low),

      lag_open_to_high_2 = lag(absolute_open_to_high, 3),
      lag_open_to_low_2 = lag(absolute_open_to_low, 3)
    ) %>%
    mutate(
      EUR_check = ifelse(str_detect(Asset, "EUR"), 1, 0),
      AUD_check = ifelse(str_detect(Asset, "AUD"), 1, 0),
      USD_check = ifelse(str_detect(Asset, "USD"), 1, 0),
      GBP_check = ifelse(str_detect(Asset, "GBP"), 1, 0),
      JPY_check = ifelse(str_detect(Asset, "JPY"), 1, 0),
      CAD_check = ifelse(str_detect(Asset, "CAD"), 1, 0),
      MXN_check = ifelse(str_detect(Asset, "MXN"), 1, 0),
      ZAR_check = ifelse(str_detect(Asset, "ZAR"), 1, 0)
    )

  training_data <- vol_data %>%
    slice_head(prop = 0.9)

  lm_model_high <-
    lm(data = training_data,
       formula = absolute_open_to_high ~
         lag_open_to_high + lag_open_to_low +
         EUR_check + AUD_check +
         USD_check + GBP_check +
         JPY_check + CAD_check +
         ZAR_check + MXN_check +
         lag_open_to_high_2 + lag_open_to_low_2)

  lm_model_low <-
    lm(data = training_data,
       formula = absolute_open_to_low_mean ~
         lag_open_to_high + lag_open_to_low +
         EUR_check + AUD_check +
         USD_check + GBP_check +
         JPY_check + CAD_check +
         ZAR_check + MXN_check +
         lag_open_to_high_2 + lag_open_to_low_2)

  testing_data <- vol_data %>%
    ungroup() %>%
    mutate(predicted_open_high_vol =
             predict.lm(lm_model_high,
                        vol_data ) %>% as.numeric(),

           predicted_open_low_vol =
             predict.lm(lm_model_low,
                        vol_data ) %>% as.numeric()
    )


  trading_data_vol <- testing_data %>%
    mutate(
      trade_col =
        case_when(
          predicted_open_high_vol >= median(predicted_open_high_vol, na.rm = T) + sd_facs*sd(predicted_open_high_vol, na.rm = T)|
            predicted_open_low_vol >= median(predicted_open_low_vol, na.rm = T) + sd_facs*sd(predicted_open_low_vol, na.rm = T) ~ "TRADE"
        )
    )

  daily_price_with_vol_trade <-
    asset_data_daily_raw %>%
    left_join(
      trading_data_vol %>%
        distinct(Asset, week_date, trade_col,pipLocation) %>% ungroup(),
      by = c("Date" = "week_date", "Asset")
    ) %>%
    filter(!is.na(trade_col)) %>%
    # filter(Date == max(Date, na.rm = T) | Date == (max(Date, na.rm = T) - days(2))  ) %>%
    slice_max(Date) %>%
    left_join(mean_values_by_asset_for_loop) %>%
    left_join(
      asset_infor %>% dplyr::select(Asset = name, pipLocation)
    ) %>%
    mutate(
      stop_value = mean_daily + sd_daily*stop_fac,
      profit_value = mean_daily + sd_daily*prof_fac,

      stop_value_pip = stop_value/(10^pipLocation),
      profit_value_pip = profit_value/(10^pipLocation),

      volume_1_stop_dollar = stop_value_pip*(10^pipLocation),
      volume_1_profit_dollar = profit_value_pip*(10^pipLocation),

      volume_required = (risk_dollar_value/stop_value_pip)/(10^pipLocation),

      stop_volume_equated_dollar = volume_required*stop_value_pip*(10^pipLocation),
      profit_volume_equated_dollar = volume_required*profit_value_pip*(10^pipLocation)

    )

  return(daily_price_with_vol_trade)

}
