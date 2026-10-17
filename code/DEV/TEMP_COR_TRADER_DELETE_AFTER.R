get_correlation_data_summary <-
  function(
    correlation_DB_Store_con = "C:/Users/Nikhil Chandra/Documents/trade_data/Day_Trader_Cor_Continuous_Models/correlation_data.db",
    date_max_filt = today() %>% as.character(),
    start_date_min = '2010-01-01'
  ) {

    correlation_DB_Store_con <-
      connect_db(correlation_DB_Store)

    Correlation_Data_DB <-
      DBI::dbGetQuery(conn = correlation_DB_Store_con,
                      statement = "SELECT * FROM COR_DATA") %>%
      mutate(
        end_date = as_date(end_date),
        start_date = as_date(start_date)
      ) %>%
      filter(
        end_date <= as_date(date_max_filt),
        start_date >= as_date(start_date_min)
      )

    DBI::dbDisconnect(correlation_DB_Store_con)
    rm(correlation_DB_Store_con)

    corr_summary <-
      Correlation_Data_DB %>%
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

    rm(Correlation_Data_DB)
    gc()

    return(corr_summary)

  }

get_cor_biggest_negatives <-
  function(
    corr_summary = get_correlation_data_summary(),
    asset_2_highest_negs = 10,
    asset_2_ev_filt = 5,
    filter_for_positive_EV_only = TRUE,
    filter_biggest_EVs_First = FALSE
    ) {

    if(filter_for_positive_EV_only == TRUE) {
      biggest_negatives <-
        corr_summary %>%
        ungroup() %>%
        filter(EV_mean_Asset_2 > 0 & EV_mean_Asset_1 > 0)
    } else {
      biggest_negatives <-
        corr_summary %>%
        ungroup()
    }

    if(filter_biggest_EVs_First == TRUE) {
      biggest_negatives <-
        biggest_negatives %>%
        group_by(Asset_2, Asset_1) %>%
        slice_max(EV_mean_Asset_1) %>%
        ungroup()
    }

    biggest_negatives <-
      biggest_negatives %>%
      # filter(volatility_factor_stop == 5,
      #        volatility_factor_profit == 12,
      #        running_volatility_period_max == 20,
      #        running_volatility_period_mean == 100) %>%
      group_by(Asset_2, Asset_1) %>%
      slice_min(correlation_mean, n = 1) %>%
      group_by(Asset_2) %>%
      slice_min(correlation_mean, n = asset_2_highest_negs)%>%
      group_by(Asset_2, Asset_1) %>%
      slice_max(EV_mean_Asset_1, n = asset_2_ev_filt)

  }

get_top_X_neg_cors <-
  function(
    biggest_negatives = biggest_negatives,
    XX = 10
    ) {

    top_rank <-
      biggest_negatives %>%
      ungroup() %>%
      slice_min(correlation_mean) %>%
      slice_head(n = 1)

    assets_to_filter_out <-
      c(top_rank$Asset_2, top_rank$Asset_1) %>% unique()

    Accumulator_Rank <- list()

    for (i in 1:(XX - 1)) {

      Next_rank <-
        biggest_negatives %>%
        ungroup() %>%
        filter(!(Asset_1 %in% assets_to_filter_out)) %>%
        filter(!(Asset_2 %in% assets_to_filter_out)) %>%
        ungroup() %>%
        slice_min(correlation_mean) %>%
        slice_head(n = 1)

      Accumulator_Rank[[i]] <- Next_rank

      assets_to_filter_out <-
        c(assets_to_filter_out, Next_rank$Asset_2, Next_rank$Asset_1) %>% unique()

    }

    Final_Ranked_Data <-
      Accumulator_Rank %>%
      map_dfr(bind_rows) %>%
      bind_rows(top_rank) %>%
      arrange(correlation_mean)

  }

rolling_cor_strategy <-
  function(
    ask_data = Indices_Metals_Bonds[[1]],
    bid_data = Indices_Metals_Bonds[[2]],
    asset_grouping_data = all_groupings,
    cor_rolling_period = 1000,
    sum_rolling_period = 100,
    slippage_percent = 0,
    risk_dollar_value = 5,
    profit_multiple = 1,
    end_period = 132,
    currency_conversion = currency_conversion,
    asset_infor = asset_infor,
    return_col_to_analyse = "period_return_24_Price",
    return_period_lag = 25
    ) {

    accumulated_reg_data <- list()
    accumulated_trade_params <- list()
    for (i in 1:dim(asset_grouping_data)) {

      volatility_factor_stop = asset_grouping_data$volatility_factor_stop[i]
      volatility_factor_profit = asset_grouping_data$volatility_factor_profit[i]
      running_volatility_period_max = asset_grouping_data$running_volatility_period_max[i]
      running_volatility_period_mean = asset_grouping_data$running_volatility_period_mean[i]

      portfolio_data_temp <-
        get_dynamic_stop_prof_returns(
          Ask_Data = ask_data %>% filter(Asset %in% c(asset_grouping_data$Asset_2[i], asset_grouping_data$Asset_1[i]) ),
          Bid_Data = bid_data %>% filter(Asset %in% c(asset_grouping_data$Asset_2[i], asset_grouping_data$Asset_1[i]) ),
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
        dplyr::select(Date, Asset, Final_Return, !!as.name(return_col_to_analyse),
                      volume_adj, stop_point, profit_point,
                      stop_return, profit_return,
                      stop_value, profit_value, adjusted_conversion,
                      trade_col,
                      volatility_factor_stop, volatility_factor_profit, profit_multiple,
                      running_volatility_period_max, running_volatility_period_mean) %>%
        mutate(Asset_2 = asset_grouping_data$Asset_2[i],
               Asset_1 = asset_grouping_data$Asset_1[i])

      temp_data <-
        portfolio_data_temp %>%
        filter(Asset %in% c(asset_grouping_data$Asset_2[i], asset_grouping_data$Asset_1[i]) ) %>%
        dplyr::select(Date, Asset, !!as.name(return_col_to_analyse)) %>%
        group_by(Asset) %>%
        arrange(Date, .by_group = TRUE) %>%
        group_by(Asset) %>%
        fill(!!as.name(return_col_to_analyse), .direction = "down") %>%
        group_by(Asset) %>%
        mutate(
          !!as.name(return_col_to_analyse) := lag(!!as.name(return_col_to_analyse), return_period_lag)
        ) %>%
        mutate(
          !!as.name(glue::glue("SlidinCumSum_{sum_rolling_period}")) :=
            slider::slide_sum(x = !!as.name(return_col_to_analyse), before = sum_rolling_period)
        )

      temp_rolling_cor <-
        temp_data %>%
        ungroup() %>%
        dplyr::select(Date, Asset, !!as.name(glue::glue("SlidinCumSum_{sum_rolling_period}")) ) %>%
        pivot_wider(names_from = Asset, values_from = !!as.name(glue::glue("SlidinCumSum_{sum_rolling_period}"))) %>%
        arrange(Date) %>%
        fill(everything(), .direction = "down") %>%
        mutate(
          !!as.name(glue::glue("{asset_grouping_data$Asset_2[i]}_{asset_grouping_data$Asset_1[i]}_SlidingCor_{cor_rolling_period}")) :=
            slider::slide2_dbl(.x = !!as.name(asset_grouping_data$Asset_2[i]),
                               .y = !!as.name(asset_grouping_data$Asset_1[i]),
                               .f = cor,
                               .before = cor_rolling_period),

          !!as.name(glue::glue("{asset_grouping_data$Asset_2[i]}_{asset_grouping_data$Asset_1[i]}_SlidingCorMean_{cor_rolling_period}")) :=
            slider::slide_mean(x = !!as.name(glue::glue("{asset_grouping_data$Asset_2[i]}_{asset_grouping_data$Asset_1[i]}_SlidingCor_{cor_rolling_period}")),
                               before = sum_rolling_period),

          !!as.name(glue::glue("{asset_grouping_data$Asset_2[i]}_{asset_grouping_data$Asset_1[i]}_SlidingCorSDEV_{cor_rolling_period}")) :=
            slider::slide_dbl(.x = !!as.name(glue::glue("{asset_grouping_data$Asset_2[i]}_{asset_grouping_data$Asset_1[i]}_SlidingCor_{cor_rolling_period}")),
                              .f = sd,
                              .before = sum_rolling_period),

          !!as.name(glue::glue("{asset_grouping_data$Asset_2[i]}_{asset_grouping_data$Asset_1[i]}_SlidingPnormCor_{cor_rolling_period}")) :=
            pnorm(
              !!as.name(glue::glue("{asset_grouping_data$Asset_2[i]}_{asset_grouping_data$Asset_1[i]}_SlidingCor_{cor_rolling_period}")),
              !!as.name(glue::glue("{asset_grouping_data$Asset_2[i]}_{asset_grouping_data$Asset_1[i]}_SlidingCorMean_{cor_rolling_period}")),
              !!as.name(glue::glue("{asset_grouping_data$Asset_2[i]}_{asset_grouping_data$Asset_1[i]}_SlidingCorSDEV_{cor_rolling_period}"))
            )
        ) %>%
        dplyr::distinct(Date,
                      !!as.name(glue::glue("{asset_grouping_data$Asset_2[i]}_{asset_grouping_data$Asset_1[i]}_SlidingCor_{cor_rolling_period}")),
                      !!as.name(glue::glue("{asset_grouping_data$Asset_2[i]}_{asset_grouping_data$Asset_1[i]}_SlidingCorMean_{cor_rolling_period}")),
                      !!as.name(glue::glue("{asset_grouping_data$Asset_2[i]}_{asset_grouping_data$Asset_1[i]}_SlidingCorSDEV_{cor_rolling_period}")),
                      !!as.name(glue::glue("{asset_grouping_data$Asset_2[i]}_{asset_grouping_data$Asset_1[i]}_SlidingPnormCor_{cor_rolling_period}"))
                      )

      accumulated_reg_data[[i]] <- temp_rolling_cor

      accumulated_trade_params[[i]] <-
        portfolio_data_temp %>%
        distinct(Date, Asset, Final_Return,
                 volume_adj, stop_point, profit_point,
                 stop_return, profit_return,
                 stop_value, profit_value, adjusted_conversion,
                 trade_col,
                 volatility_factor_stop, volatility_factor_profit, profit_multiple,
                 running_volatility_period_max, running_volatility_period_mean)


    }

    Final_Cor_Data <-
      accumulated_reg_data %>%
      reduce(left_join)

    Final_Returned_data <-
      accumulated_trade_params %>%
      reduce(bind_rows) %>%
      left_join(Final_Cor_Data)

    return(Final_Returned_data)

  }

