atomic_neg_corr_asset_builder <-
  function(
    ask_data = Indices_Metals_Bonds[[1]],
    bid_data = Indices_Metals_Bonds[[2]],
    slippage_percent = 0,
    risk_dollar_value_asset_1 = 5,
    risk_dollar_value_asset_2 = 5,
    biggest_negatives,
    Correlation_Data = Correlation_Data_DB
  ) {

    corr_summary <-
      Correlation_Data %>%
      filter(Asset_2 != Asset_1) %>%
      group_by(Asset_2, Asset_1,
               volatility_factor_stop, volatility_factor_profit, profit_multiple,
               running_volatility_period_max, running_volatility_period_mean) %>%
      summarise(
        correlation_mean = mean(correlation, na.rm = T),
        correlation_sd = sd(correlation),

        EV_mean_Asset_2 = mean(Mean_Return_Asset_2, na.rm = T),
        SDEV_sd_Asset_2 = sd(SDEV_Asset_2, na.rm = T),

        EV_mean_Asset_1 = mean(Mean_Return_Asset_1, na.rm = T),
        SDEV_sd_Asset_1 = sd(SDEV_Asset_2, na.rm = T)
      )

    rm(Correlation_Data)
    gc()

    biggest_negatives <-
      corr_summary %>%
      ungroup() %>%
      filter(EV_mean_Asset_2 > 0 & EV_mean_Asset_1 > 0) %>%
      # filter(volatility_factor_stop == 5,
      #        volatility_factor_profit == 12,
      #        running_volatility_period_max == 20,
      #        running_volatility_period_mean == 100) %>%
      group_by(Asset_2, Asset_1) %>%
      slice_min(correlation_mean, n = 1) %>%
      group_by(Asset_2) %>%
      slice_min(correlation_mean, n = 10)%>%
      group_by(Asset_2, Asset_1) %>%
      slice_max(EV_mean_Asset_1, n = 5)

    biggest_negatives <-
      biggest_negatives %>%
      arrange((correlation_mean))

    Asset_2_var <- biggest_negatives$Asset_2[1]
    Asset_1_var <- biggest_negatives$Asset_1[1]

    volatility_factor_stop = biggest_negatives$volatility_factor_stop[1]
    volatility_factor_profit = biggest_negatives$volatility_factor_profit[1]
    running_volatility_period_max = biggest_negatives$running_volatility_period_max[1]
    running_volatility_period_mean = biggest_negatives$running_volatility_period_mean[1]

    temp_ask <-
      ask_data %>%
      filter(Asset %in% c(Asset_2_var, Asset_1_var) )

    temp_bid <-
      bid_data %>%
      filter(Asset %in% c(Asset_2_var, Asset_1_var) )

    Asset_1_Returns <-
      get_dynamic_stop_prof_returns(
        Ask_Data = temp_ask %>% filter(Asset == Asset_1_var),
        Bid_Data = temp_bid %>% filter(Asset == Asset_1_var),
        periods_wanted = end_period,
        trade_direction = "Long",
        currency_conversion =currency_conversion,
        asset_infor = asset_infor,
        slippage_percent = slippage_percent,
        risk_dollar_value = risk_dollar_value,
        volatility_factor_stop = volatility_factor_stop,
        volatility_factor_profit = volatility_factor_profit,
        profit_multiple = profit_multiple,
        running_volatility_period_max = running_volatility_period_max,
        running_volatility_period_mean = running_volatility_period_mean
      ) %>%
      ungroup() %>%
      dplyr::select(Date, Asset, Final_Return, volatility_factor_stop, volatility_factor_profit, profit_multiple,
                    running_volatility_period_max, running_volatility_period_mean) %>%
      mutate(Asset_2 = Asset_2,
             Asset_1 = Asset_1)


    Asset_2_Returns <-
      get_dynamic_stop_prof_returns(
        Ask_Data = temp_ask %>% filter(Asset == Asset_2_var),
        Bid_Data = temp_bid %>% filter(Asset == Asset_2_var),
        periods_wanted = end_period,
        trade_direction = "Long",
        currency_conversion =currency_conversion,
        asset_infor = asset_infor,
        slippage_percent = slippage_percent,
        risk_dollar_value = risk_dollar_value,
        volatility_factor_stop = volatility_factor_stop,
        volatility_factor_profit = volatility_factor_profit,
        profit_multiple = profit_multiple,
        running_volatility_period_max = running_volatility_period_max,
        running_volatility_period_mean = running_volatility_period_mean
      ) %>%
      ungroup() %>%
      dplyr::select(Date, Asset, Final_Return, volatility_factor_stop, volatility_factor_profit, profit_multiple,
                    running_volatility_period_max, running_volatility_period_mean) %>%
      mutate(Asset_2 = Asset_2,
             Asset_1 = Asset_1)

    complete_port <-
      list(Asset_1_Returns, Asset_2_Returns) %>%
      map_dfr(bind_rows)

    return(complete_port)

  }
