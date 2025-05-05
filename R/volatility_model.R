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

get_tag_volatility_trades <- function(
    asset_data_daily_raw = asset_data_daily_raw,
    mean_values_by_asset_for_loop = mean_values_by_asset_for_loop,
    sd_facs = 1,
    training_prop = 0.6,
    stop_minus = 0.2,
    profit_plus = 0.2,
    rolling_period = 200,
    asset_infor = get_instrument_info()
  ) {

    vol_data <- asset_data_daily_raw %>%
      group_by(Asset) %>%
      arrange(Date, .by_group = TRUE) %>%
      ungroup() %>%
      left_join(
        asset_infor %>% dplyr::select(Asset = name, pipLocation)
      ) %>%
      mutate(
        absolute_open_to_high = abs(High - Open)/(10^pipLocation),
        absolute_open_to_low = abs(Open - Low)/(10^pipLocation)
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
      arrange(Date, .by_group = TRUE) %>%
      group_by(Asset) %>%
      mutate(
        lag_open_to_high = lag(absolute_open_to_high),
        lag_open_to_low = lag(absolute_open_to_low),
        lag_open_to_high_2 = lag(absolute_open_to_high, 3),
        lag_open_to_low_2 = lag(absolute_open_to_low, 3),

        lead_open_to_high = lead(absolute_open_to_high),
        lead_open_to_low = lead(absolute_open_to_low),
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
      ) %>%
      left_join(
        asset_infor %>%
          dplyr::select(Asset = name,  Asset_Class = type)
      )

    training_data <- vol_data %>%
      group_by(Asset) %>%
      slice_head(prop =training_prop) %>%
      ungroup()

    testing_data <-
      vol_data %>%
      group_by(Asset) %>%
      slice_tail(prop = (1 - training_prop) ) %>%
      ungroup()


    lm_model_high <-
      lm(data = training_data,
         formula = lead_open_to_high ~
           absolute_open_to_high + absolute_open_to_low +
           lag_open_to_high + lag_open_to_low +
           EUR_check + AUD_check +
           USD_check + GBP_check +
           JPY_check + CAD_check +
           ZAR_check + MXN_check +
           lag_open_to_high_2 + lag_open_to_low_2 + Asset_Class)

    lm_model_low <-
      lm(data = training_data,
         formula = lead_open_to_low ~
           absolute_open_to_high + absolute_open_to_low +
           lag_open_to_high + lag_open_to_low +
           EUR_check + AUD_check +
           USD_check + GBP_check +
           JPY_check + CAD_check +
           ZAR_check + MXN_check +
           lag_open_to_high_2 + lag_open_to_low_2 + Asset_Class)

    summary(lm_model_high)

    returned_data  <- testing_data %>%
      ungroup() %>%
      mutate(predicted_open_high_vol =
               predict.lm(lm_model_high,
                          testing_data ) %>% as.numeric(),

             predicted_open_low_vol =
               predict.lm(lm_model_low,
                          testing_data ) %>% as.numeric()
      ) %>%
      group_by(Asset) %>%
      mutate(
        predicted_open_low_vol_avg = slider::slide_dbl(.x = predicted_open_low_vol,
                                                       .f = ~ median(.x, na.rm = T), .before = rolling_period, .complete = TRUE),
        predicted_open_low_vol_sd = slider::slide_dbl(.x = predicted_open_low_vol,
                                                       .f = ~ sd(.x, na.rm = T), .before = rolling_period, .complete = TRUE),

        predicted_open_high_vol_avg = slider::slide_dbl(.x = predicted_open_high_vol,
                                                       .f = ~ median(.x, na.rm = T), .before = rolling_period, .complete = TRUE),
        predicted_open_high_vol_sd = slider::slide_dbl(.x = predicted_open_high_vol,
                                                      .f = ~ sd(.x, na.rm = T), .before = rolling_period, .complete = TRUE)
      ) %>%
      ungroup() %>%
      filter(!is.na(predicted_open_low_vol_avg)) %>%
      mutate(
        Low_Val_difference = abs(absolute_open_to_low - predicted_open_low_vol),
        High_Val_difference = abs(absolute_open_to_high - predicted_open_high_vol),
        is_high_greater_than_low_pred = predicted_open_high_vol > predicted_open_low_vol,
        is_high_greater_than_low = absolute_open_to_high > absolute_open_to_low
      )

    trading_data_vol <- returned_data %>%
      mutate(
        trade_col =
          case_when(
              predicted_open_low_vol >= predicted_open_low_vol_avg + sd_facs*predicted_open_low_vol_sd ~ "Trade",
              predicted_open_high_vol >= predicted_open_high_vol_avg + sd_facs*predicted_open_high_vol_sd ~ "Trade"
          )
      ) %>%
      dplyr::select(Date, Asset, trade_col, Open, Low, High, Price,
                    absolute_open_to_low, absolute_open_to_high,lead_open_to_low, lead_open_to_high,
                    predicted_open_low_vol, predicted_open_high_vol, predicted_open_low_vol_avg,
                    predicted_open_low_vol_sd) %>%
      left_join(asset_infor %>% rename(Asset = name)) %>%
      mutate(pipLocation = as.numeric(pipLocation)) %>%
      mutate(
        stop_value_raw = ifelse(predicted_open_high_vol < predicted_open_low_vol,
                            predicted_open_high_vol,
                            predicted_open_low_vol),
        stop_value_adj = (stop_value_raw - stop_minus*stop_value_raw)*(10^pipLocation),
        profit_value_adj = (stop_value_raw + profit_plus*stop_value_raw)*(10^pipLocation)
      )


    return(trading_data_vol)

}

trade_finder_volatility <- function(
  tagged_trades,
  asset_data_daily_raw_ask = asset_data_daily_raw_ask,
  asset_data_daily_raw_bid = asset_data_daily_raw_bid,
  mean_values_by_asset = mean_values_by_asset
  ) {


  # tagged_trades <- get_tag_volatility_trades(
  #   asset_data_daily_raw = asset_data_daily_raw,
  #   mean_values_by_asset_for_loop = mean_values_by_asset_for_loop,
  #   sd_facs = 1,
  #   training_prop = 0.6,
  #   stop_minus = 0.2,
  #   profit_plus = 0
  # )

  data_with_price_ask <-
    asset_data_daily_raw_ask %>%
    left_join(
      tagged_trades %>%
        dplyr::distinct(Asset, Date, trade_col,stop_value_raw,stop_value_adj, profit_value_adj)
    ) %>%
    dplyr::group_by(Asset) %>%
    arrange(
      Date
    )

  data_with_price_bid <-
    asset_data_daily_raw_bid %>%
    left_join(
      tagged_trades %>%
        dplyr::distinct(Asset, Date, trade_col,stop_value_raw,stop_value_adj, profit_value_adj)
    ) %>%
    dplyr::group_by(Asset) %>%
    arrange(
      Date
    )

  distinct_assets_trades <-
    data_with_price_ask %>%
    filter(!is.na(trade_col)) %>%
    distinct(Asset, Date, trade_col)

  trade_directions_speed <- distinct_assets_trades$trade_col
  trade_asset_speed <- distinct_assets_trades$Asset
  trade_date_speed <- distinct_assets_trades$Date

  distinct_assets <-
    data_with_price_ask %>%
    distinct(Asset) %>%
    pull(Asset)

  accumulating_list <- list()
  c <- 0

  for (i in 1:dim(distinct_assets_trades)[1] ) {

    asset_data_loop_ask <- data_with_price_ask %>%
      filter(Asset == distinct_assets[i]) %>%
      arrange(Date)

    asset_data_loop_bid <- data_with_price_bid %>%
      filter(Asset == distinct_assets[i]) %>%
      arrange(Date)

    distinct_trade_dates <- asset_data_loop_ask %>%
      filter(!is.na( trade_col )) %>%
      pull(Date)

    distinct_trade_direction <- asset_data_loop_ask %>%
      filter(!is.na( trade_col ) ) %>%
      pull(trade_col)

    price_data_loop_ask <- asset_data_loop_ask %>% pull(Price)
    date_data_loop_ask <- asset_data_loop_ask %>% pull(Date)
    high_loop_ask <- asset_data_loop_ask %>% pull(High)
    Low_loop_ask <- asset_data_loop_ask %>% pull(Low)
    stop_values_long <- asset_data_loop_ask %>% pull(stop_value_adj)
    profit_values_long <- asset_data_loop_ask %>% pull(profit_value_adj)

    price_data_loop_bid <- asset_data_loop_bid %>% pull(Price)
    date_data_loop_bid <- asset_data_loop_bid %>% pull(Date)
    high_loop_bid <- asset_data_loop_bid %>% pull(High)
    Low_loop_bid <- asset_data_loop_bid %>% pull(Low)
    stop_values_short <- asset_data_loop_bid %>% pull(stop_value_adj)
    profit_values_short <- asset_data_loop_bid %>% pull(profit_value_adj)

    if(length(distinct_trade_dates) > 0) {

      trade_returns_long <- numeric(length(distinct_trade_dates))
      trade_returns_short <- numeric(length(distinct_trade_dates))
      trade_start_price_long <- numeric(length(distinct_trade_dates))
      trade_start_price_short <- numeric(length(distinct_trade_dates))

      trade_time_taken_long <- numeric(length(distinct_trade_dates))
      trade_time_taken_short <- numeric(length(distinct_trade_dates))

      long_take_profit_price <- numeric(length(distinct_trade_dates))
      long_take_stop_price <- numeric(length(distinct_trade_dates))
      short_take_profit_price <- numeric(length(distinct_trade_dates))
      short_take_stop_price <- numeric(length(distinct_trade_dates))

      trade_dates_tibble <- numeric(length(distinct_trade_dates))
      trade_stop_value_tibble_long <- numeric(length(distinct_trade_dates))
      trade_stop_value_tibble_short <- numeric(length(distinct_trade_dates))

      trade_profit_value_tibble_long <- numeric(length(distinct_trade_dates))
      trade_profit_value_tibble_short <- numeric(length(distinct_trade_dates))

      for (j in 1:length(distinct_trade_dates)) {

        starting_index <- which(date_data_loop_ask == distinct_trade_dates[j])[1]
        dates_for_tracking <- date_data_loop_ask[seq(starting_index,length(date_data_loop_ask))]
        starting_price_long <- price_data_loop_ask[starting_index]
        starting_price_short <- price_data_loop_bid[starting_index]

        highs_after_long <- high_loop_ask[(starting_index + 1):length(high_loop_ask)]
        lows_after_long <- Low_loop_ask[(starting_index+ 1):length(Low_loop_ask)]
        price_after_long <- price_data_loop_ask[(starting_index + 1):length(price_data_loop_ask)]

        highs_after_short <- high_loop_bid[(starting_index + 1):length(high_loop_bid)]
        lows_after_short <- Low_loop_bid[(starting_index+ 1):length(Low_loop_bid)]
        price_after_short <- price_data_loop_bid[(starting_index + 1):length(price_data_loop_bid)]

        long_stop <-starting_price_long - stop_values_long[starting_index]
        short_stop <-starting_price_short + stop_values_short[starting_index]

        long_profit <-starting_price_long + profit_values_long[starting_index]
        short_profit <-starting_price_short - profit_values_short[starting_index]

        long_stop_point <- which(lows_after_long <= long_stop)[1]
        long_stop_point <- ifelse(is.na(long_stop_point)|length(long_stop_point) < 1, 100000,long_stop_point)
        long_profit_point <- which(highs_after_long >= long_profit)[1]
        long_profit_point <- ifelse(is.na(long_profit_point)|length(long_profit_point) < 1, 100000,long_profit_point)
        long_result <-
          case_when(
            is.na(long_stop_point) ~ profit_values_long[starting_index],
            is.na(long_profit_point) ~ -1*stop_values_long[starting_index],
            long_profit_point >= long_stop_point ~ -1*stop_values_long[starting_index],
            long_profit_point < long_stop_point ~ profit_values_long[starting_index]
          )

        time_taken_long <-min( c(long_profit_point, long_stop_point), na.rm = T)

        short_stop_point <- which( highs_after_short >= short_stop)[1]
        short_stop_point <- ifelse(is.na(short_stop_point)|length(short_stop_point) < 1, 100000,short_stop_point)
        short_profit_point <- which(lows_after_short <= short_profit)[1]
        short_profit_point <- ifelse(is.na(short_profit_point)|length(short_profit_point) < 1, 100000,short_profit_point)
        short_result <-
          case_when(
            is.na(short_stop_point) ~ profit_values_short[starting_index],
            is.na(short_profit_point) ~ -1*stop_values_short[starting_index],
            short_profit_point >= short_stop_point ~ -1*stop_values_short[starting_index],
            short_profit_point < short_stop_point ~ profit_values_short[starting_index]
          )

        time_taken_short <-min( c(short_stop_point, short_profit_point), na.rm = T)

        trade_returns_long[j] <- long_result
        trade_returns_short[j] <- short_result
        trade_start_price_long[j] <- starting_price_long
        trade_start_price_short[j] <- starting_price_short

        trade_time_taken_long[j] <- time_taken_long
        trade_time_taken_short[j] <- time_taken_short

        long_take_profit_price[j] <- long_profit
        long_take_stop_price[j] <- long_stop
        short_take_profit_price[j] <- short_profit
        short_take_stop_price[j] <- short_stop

        trade_dates_tibble[j] <- distinct_trade_dates[j]

        trade_stop_value_tibble_long[j] <- stop_values_long[starting_index]
        trade_stop_value_tibble_short[j] <- stop_values_short[starting_index]

        trade_profit_value_tibble_long[j] <- profit_values_long[starting_index]
        trade_profit_value_tibble_short[j] <- profit_values_short[starting_index]
      }

      c = c + 1
      accumulating_list[[c]] <-
        tibble(
          Date = trade_dates_tibble,
          Asset = distinct_assets[i],
          trade_start_price_long = trade_start_price_long,
          trade_start_price_short = trade_start_price_short,
          trade_returns_long = trade_returns_long,
          trade_returns_short = trade_returns_short,
          trade_time_taken_long = trade_time_taken_long,
          trade_time_taken_short = trade_time_taken_short,
          long_take_profit_price = long_take_profit_price,
          long_take_stop_price = long_take_stop_price,
          short_take_profit_price = short_take_profit_price,
          short_take_stop_price = short_take_stop_price,
          stop_value_long = trade_stop_value_tibble_long,
          stop_value_short = trade_stop_value_tibble_short,

          profit_value_long = trade_profit_value_tibble_long,
          profit_value_short = trade_profit_value_tibble_short
        )

    }

  }

  all_results <- accumulating_list %>%
    map_dfr(bind_rows)


  return(all_results)

}
