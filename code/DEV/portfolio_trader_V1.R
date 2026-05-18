port_trader_continuous_ret_data <-
  function(
    Indices_Metals_Bonds = Indices_Metals_Bonds,
    asset_to_trade = c("SPX500_USD"),
    stop_value_var = 20,
    profit_value_var = 100,
    risk_dollar_value_var = 30,
    currency_conversion = currency_conversion,
    asset_infor = asset_infor,
    trade_direction = "Long",
    rolling_return_period = 10
    ) {

    asset_data_with_indicator <- list()

    for (i in 1:length(asset_to_trade)) {

      asset_data <-
        Indices_Metals_Bonds %>%
        map(~ .x %>% filter(Asset == asset_to_trade[i]))

      mean_values_by_asset_for_loop_H1_ask <-
        wrangle_asset_data(
          asset_data_daily_raw = asset_data[[1]],
          summarise_means = TRUE
        ) %>%
        dplyr::select(Asset,
                      mean_movement = mean_daily,
                      sd_movement = sd_daily) %>%
        filter(Asset == asset_to_trade[i])

      bid_price <-
        asset_data[[2]] %>%
        filter(Asset == asset_to_trade[i]) %>%
        dplyr::select(Date, Asset,
                      Bid_Price = Price,
                      Ask_High = High,
                      Ask_Low = Low)

      asset_data_with_indicator[[i]] <-
        asset_data[[1]] %>%
        filter(Asset == asset_to_trade[i]) %>%
        dplyr::select(Date, Asset,
                      Ask_Price = Price,
                      Bid_High = High,
                      Bid_Low = Low) %>%
        left_join(
          bid_price
        ) %>%
        ungroup() %>%
        mutate(
          Date = as_datetime(Date)
        ) %>%
        mutate(
          trade_col = trade_direction
        )  %>%
        left_join(mean_values_by_asset_for_loop_H1_ask) %>%
        mutate(
          stop_value = stop_value_var*sd_movement + mean_movement,
          profit_value = profit_value_var*sd_movement + mean_movement,
          stop_point =
            case_when(
              trade_col == "Long" ~ lead(Ask_Price) - stop_value,
              trade_col == "Short" ~ lead(Bid_Price) + stop_value
            ),

          profit_point =
            case_when(
              trade_col == "Long" ~ lead(Ask_Price) + profit_value,
              trade_col == "Short" ~ lead(Bid_Price) - profit_value
            )

        ) %>%
        mutate(ending_value = str_extract(Asset, "_[A-Z][A-Z][A-Z]"),
               ending_value = str_remove_all(ending_value, "_")
        ) %>%
        left_join(currency_conversion, by =c("ending_value" = "not_aud_asset")) %>%
        left_join(asset_infor%>%
                    rename(Asset = name) %>%
                    dplyr::select(Asset,
                                  minimumTradeSize,
                                  marginRate,
                                  pipLocation,
                                  displayPrecision) ) %>%
        mutate(
          minimumTradeSize_OG = as.numeric(minimumTradeSize),
          minimumTradeSize = abs(log10(as.numeric(minimumTradeSize))),
          marginRate = as.numeric(marginRate),
          pipLocation = as.numeric(pipLocation),
          displayPrecision = as.numeric(displayPrecision)
        ) %>%
        ungroup() %>%
        mutate(
          stop_value = round(stop_value, abs(pipLocation) ),
          profit_value = round(profit_value, abs(pipLocation) )
        )  %>%
        mutate(
          volume_unadj =
            case_when(
              str_detect(Asset,"ZAR|CNH") ~ (risk_dollar_value_var/stop_value)*adjusted_conversion,
              TRUE ~ (risk_dollar_value_var/stop_value)/adjusted_conversion
            ),
          volume_required = volume_unadj,
          volume_adj =
            case_when(
              round(volume_unadj, minimumTradeSize) == 0 ~  minimumTradeSize_OG,
              round(volume_unadj, minimumTradeSize) != 0 ~  round(volume_unadj, minimumTradeSize)
            )
        ) %>%
        group_by(Asset) %>%
        arrange(Date, .by_group = TRUE) %>%
        ungroup() %>%
        mutate(
          across(
            .cols = c(Ask_Price, Bid_Price,Bid_High,Bid_Low,Ask_High, Ask_Low  ),
            .fns = ~ as.numeric(.)
          )
        ) %>%
        mutate(
          profit_return = profit_value*adjusted_conversion*volume_adj,
          stop_return = stop_value*adjusted_conversion*volume_adj
        ) %>%
        mutate(
          Period_1_Return =
            case_when(
              trade_col == "Long" ~
                adjusted_conversion*volume_adj*((lead(Bid_Price, 1) - Ask_Price) ),
              trade_col == "Short" ~
                adjusted_conversion*volume_adj*( Bid_Price - lead(Ask_Price,1) )
        ),
        Period_2_Return =
          case_when(
            trade_col == "Long" ~
              adjusted_conversion*volume_adj*((lead(Bid_Price, 2) - Ask_Price) ),
            trade_col == "Short" ~
              adjusted_conversion*volume_adj*( Bid_Price - lead(Ask_Price,2) )
          ),
        Period_3_Return =
          case_when(
            trade_col == "Long" ~
              adjusted_conversion*volume_adj*((lead(Bid_Price, 3) - Ask_Price) ),
            trade_col == "Short" ~
              adjusted_conversion*volume_adj*( Bid_Price - lead(Ask_Price,3) )
          ),
        Period_4_Return =
          case_when(
            trade_col == "Long" ~
              adjusted_conversion*volume_adj*((lead(Bid_Price, 4) - Ask_Price) ),
            trade_col == "Short" ~
              adjusted_conversion*volume_adj*( Bid_Price - lead(Ask_Price,4) )
          ),
        Period_5_Return =
          case_when(
            trade_col == "Long" ~
              adjusted_conversion*volume_adj*((lead(Bid_Price, 5) - Ask_Price) ),
            trade_col == "Short" ~
              adjusted_conversion*volume_adj*( Bid_Price - lead(Ask_Price,5) )
          ),
        Period_6_Return =
          case_when(
            trade_col == "Long" ~
              adjusted_conversion*volume_adj*((lead(Bid_Price, 6) - Ask_Price) ),
            trade_col == "Short" ~
              adjusted_conversion*volume_adj*( Bid_Price - lead(Ask_Price,6) )
          ),
        Period_7_Return =
          case_when(
            trade_col == "Long" ~
              adjusted_conversion*volume_adj*((lead(Bid_Price, 7) - Ask_Price) ),
            trade_col == "Short" ~
              adjusted_conversion*volume_adj*( Bid_Price - lead(Ask_Price,7) )
          ),
        Period_8_Return =
          case_when(
            trade_col == "Long" ~
              adjusted_conversion*volume_adj*((lead(Bid_Price, 8) - Ask_Price) ),
            trade_col == "Short" ~
              adjusted_conversion*volume_adj*( Bid_Price - lead(Ask_Price,8) )
          ),
        Period_9_Return =
          case_when(
            trade_col == "Long" ~
              adjusted_conversion*volume_adj*((lead(Bid_Price, 9) - Ask_Price) ),
            trade_col == "Short" ~
              adjusted_conversion*volume_adj*( Bid_Price - lead(Ask_Price,9) )
          ),
        Period_10_Return =
          case_when(
            trade_col == "Long" ~
              adjusted_conversion*volume_adj*((lead(Bid_Price, 10) - Ask_Price) ),
            trade_col == "Short" ~
              adjusted_conversion*volume_adj*( Bid_Price - lead(Ask_Price,10) )
          ),
        Period_11_Return =
          case_when(
            trade_col == "Long" ~
              adjusted_conversion*volume_adj*((lead(Bid_Price, 11) - Ask_Price) ),
            trade_col == "Short" ~
              adjusted_conversion*volume_adj*( Bid_Price - lead(Ask_Price,11) )
          ),
        Period_12_Return =
          case_when(
            trade_col == "Long" ~
              adjusted_conversion*volume_adj*((lead(Bid_Price, 12) - Ask_Price) ),
            trade_col == "Short" ~
              adjusted_conversion*volume_adj*( Bid_Price - lead(Ask_Price,12) )
          ),
        Period_13_Return =
          case_when(
            trade_col == "Long" ~
              adjusted_conversion*volume_adj*((lead(Bid_Price, 13) - Ask_Price) ),
            trade_col == "Short" ~
              adjusted_conversion*volume_adj*( Bid_Price - lead(Ask_Price,13) )
          ),
        Period_14_Return =
          case_when(
            trade_col == "Long" ~
              adjusted_conversion*volume_adj*((lead(Bid_Price, 14) - Ask_Price) ),
            trade_col == "Short" ~
              adjusted_conversion*volume_adj*( Bid_Price - lead(Ask_Price,14) )
          ),
        Period_15_Return =
          case_when(
            trade_col == "Long" ~
              adjusted_conversion*volume_adj*((lead(Bid_Price, 15) - Ask_Price) ),
            trade_col == "Short" ~
              adjusted_conversion*volume_adj*( Bid_Price - lead(Ask_Price,15) )
          ),
        Period_16_Return =
          case_when(
            trade_col == "Long" ~
              adjusted_conversion*volume_adj*((lead(Bid_Price, 16) - Ask_Price) ),
            trade_col == "Short" ~
              adjusted_conversion*volume_adj*( Bid_Price - lead(Ask_Price,16) )
          ),
        Period_17_Return =
          case_when(
            trade_col == "Long" ~
              adjusted_conversion*volume_adj*((lead(Bid_Price, 17) - Ask_Price) ),
            trade_col == "Short" ~
              adjusted_conversion*volume_adj*( Bid_Price - lead(Ask_Price,17) )
          ),
        Period_18_Return =
          case_when(
            trade_col == "Long" ~
              adjusted_conversion*volume_adj*((lead(Bid_Price, 18) - Ask_Price) ),
            trade_col == "Short" ~
              adjusted_conversion*volume_adj*( Bid_Price - lead(Ask_Price,18) )
          ),
        Period_19_Return =
          case_when(
            trade_col == "Long" ~
              adjusted_conversion*volume_adj*((lead(Bid_Price, 19) - Ask_Price) ),
            trade_col == "Short" ~
              adjusted_conversion*volume_adj*( Bid_Price - lead(Ask_Price,19) )
          ),
        Period_20_Return =
          case_when(
            trade_col == "Long" ~
              adjusted_conversion*volume_adj*((lead(Bid_Price, 20) - Ask_Price) ),
            trade_col == "Short" ~
              adjusted_conversion*volume_adj*( Bid_Price - lead(Ask_Price,20) )
          ),
        Period_21_Return =
          case_when(
            trade_col == "Long" ~
              adjusted_conversion*volume_adj*((lead(Bid_Price, 21) - Ask_Price) ),
            trade_col == "Short" ~
              adjusted_conversion*volume_adj*( Bid_Price - lead(Ask_Price,21) )
          ),
        Period_22_Return =
          case_when(
            trade_col == "Long" ~
              adjusted_conversion*volume_adj*((lead(Bid_Price, 22) - Ask_Price) ),
            trade_col == "Short" ~
              adjusted_conversion*volume_adj*( Bid_Price - lead(Ask_Price,22) )
          ),
        Period_23_Return =
          case_when(
            trade_col == "Long" ~
              adjusted_conversion*volume_adj*((lead(Bid_Price, 23) - Ask_Price) ),
            trade_col == "Short" ~
              adjusted_conversion*volume_adj*( Bid_Price - lead(Ask_Price,23) )
          ),
        Period_24_Return =
          case_when(
            trade_col == "Long" ~
              adjusted_conversion*volume_adj*((lead(Bid_Price, 24) - Ask_Price) ),
            trade_col == "Short" ~
              adjusted_conversion*volume_adj*( Bid_Price - lead(Ask_Price,24) )
          ),
        Period_25_Return =
          case_when(
            trade_col == "Long" ~
              adjusted_conversion*volume_adj*((lead(Bid_Price, 25) - Ask_Price) ),
            trade_col == "Short" ~
              adjusted_conversion*volume_adj*( Bid_Price - lead(Ask_Price,25) )
          ),
        Period_26_Return =
          case_when(
            trade_col == "Long" ~
              adjusted_conversion*volume_adj*((lead(Bid_Price, 26) - Ask_Price) ),
            trade_col == "Short" ~
              adjusted_conversion*volume_adj*( Bid_Price - lead(Ask_Price,26) )
          ),
        Period_27_Return =
          case_when(
            trade_col == "Long" ~
              adjusted_conversion*volume_adj*((lead(Bid_Price, 27) - Ask_Price) ),
            trade_col == "Short" ~
              adjusted_conversion*volume_adj*( Bid_Price - lead(Ask_Price,27) )
          ),
        Period_28_Return =
          case_when(
            trade_col == "Long" ~
              adjusted_conversion*volume_adj*((lead(Bid_Price, 28) - Ask_Price) ),
            trade_col == "Short" ~
              adjusted_conversion*volume_adj*( Bid_Price - lead(Ask_Price,28) )
          ),
        Period_29_Return =
          case_when(
            trade_col == "Long" ~
              adjusted_conversion*volume_adj*((lead(Bid_Price, 29) - Ask_Price) ),
            trade_col == "Short" ~
              adjusted_conversion*volume_adj*( Bid_Price - lead(Ask_Price,29) )
          ),
        Period_30_Return =
          case_when(
            trade_col == "Long" ~
              adjusted_conversion*volume_adj*((lead(Bid_Price, 30) - Ask_Price) ),
            trade_col == "Short" ~
              adjusted_conversion*volume_adj*( Bid_Price - lead(Ask_Price,30) )
          )
      )

    }

    asset_data_with_indicator <-
      asset_data_with_indicator %>%
      map_dfr(bind_rows)

    return(asset_data_with_indicator)

  }


port_trader_v1_gen_AR_return_model <-
  function(
    Indices_Metals_Bonds = Indices_Metals_Bonds,
    raw_macro_data = raw_macro_data,
    asset_to_trade = c("SPX500_USD"),
    base_path = "C:/Users/nikhi/Documents/trade_data/portfolio_trader_V1/",
    correlation_rolling_periods = c(100,200, 300,400, 500),
    state_space_periods = c(20, 40, 60, 100, 200,300, 400,  500),
    state_space_rolling = c(100, 200, 300, 400),
    date_for_true_simualtion = "2023-01-01",
    training_end_date = "2024-01-01",
    stop_value_var = 20,
    profit_value_var = 100,
    risk_dollar_value_var = 30,
    currency_conversion = currency_conversion,
    asset_infor = asset_infor,
    sig_thresh_LM = 0.01,
    trade_direction = "Long",
    rolling_return_period = 10
  ){

    return_ts <-
      port_trader_continuous_ret_data(
        Indices_Metals_Bonds = Indices_Metals_Bonds,
        asset_to_trade = asset_to_trade,
        stop_value_var = stop_value_var,
        profit_value_var = profit_value_var,
        risk_dollar_value_var = risk_dollar_value_var,
        currency_conversion = currency_conversion,
        asset_infor = asset_infor,
        trade_direction = trade_direction
      )
    return_ts %>%
      mutate(bin_1_to_3 = case_when(Period_1_Return < Period_3_Return ~ 1, TRUE ~ 0),
             bin_1_to_5 = case_when(Period_1_Return < Period_5_Return ~ 1, TRUE ~ 0),
             bin_1_to_5 = case_when(Period_3_Return < Period_5_Return ~ 1, TRUE ~ 0),
             bin_1_to_5 = case_when(Period_5_Return < Period_10_Return ~ 1, TRUE ~ 0),
             bin_1_to_5 = case_when(Period_3_Return < Period_10_Return ~ 1, TRUE ~ 0),
             bin_1_to_5 = case_when(Period_10_Return < Period_15_Return ~ 1, TRUE ~ 0),
             bin_1_to_5 = case_when(Period_5_Return < Period_15_Return ~ 1, TRUE ~ 0)
             ) %>%



  }

rolling_LM_AR <-
  function(df_data,
           dependant_var,
           threshold_var = 0,
           sig_thresh_LM = 0.2) {

    df_data <-
      df_data %>%
      select( contains("MA_")|contains("lagged_")|contains("MSD_"),
              !!as.name(dependant_var), hours_ahead) %>%
      mutate(
        bin_var = ifelse( !!as.name(dependant_var) >= threshold_var, 1, 0)
      ) %>%
      filter(
        if_all(everything(), ~ !is.na(.))
      )

    dependants <-
      names(df_data) %>%
      keep(~ str_detect(.x, "MA_|lagged_|MSD_|hours_ahead"))

    lm_form <-
      create_lm_formula(dependant = dependant_var, independant = dependants)

    LM_model <- lm(formula = lm_form, data = df_data)

    sig_coefs <- get_sig_coefs(LM_model, p_value_thresh_for_inputs = sig_thresh_LM)

    lm_form <-
      create_lm_formula(dependant = dependant_var, independant = sig_coefs)

    LM_model <- lm(formula = lm_form, data = df_data)

    predicted_data <- LM_model$fitted.values %>% tail(n = 1) %>% as.numeric()

    safely_reg <- safely(rolling_LM, otherwise = 0)
    test_data <-
      model_data %>%
      group_by(Asset) %>%
      slice_head(n = 2000) %>%
      ungroup() %>%
      split(.$Asset, drop = FALSE) %>%
      map_dfr(
        ~
          .x %>%
          mutate(
            AR_reg = slider::slide_dbl(.x = .,
                                       .f = ~ safely_reg(df_data = .,
                                                         dependant_var = "dependant_5",
                                                         threshold_var = 0,
                                                         sig_thresh_LM = 0.5) %>%
                                         pluck('result'),
                                       .before = 1000,
                                       .complete = TRUE)
          )
      ) %>%
      dplyr::select(Date, Trade_Date, Asset, AR_reg, returns, hours_ahead)

    return(predicted_data)

  }
