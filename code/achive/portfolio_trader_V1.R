port_trader_v1_gen_AR_return_model <-
  function(
    Indices_Metals_Bonds = Indices_Metals_Bonds,
    raw_macro_data = raw_macro_data,
    asset_to_trade = c("SPX500_USD"),
    base_path = "C:/Users/nikhi/Documents/trade_data/portfolio_trader_V1/",
    correlation_rolling_periods = c(100,200, 300,400, 500),
    state_space_periods = c(20, 40, 60, 100, 200,300, 400,  500),
    state_space_rolling = c(100, 200, 300, 400),
    date_for_true_simualtion = "2019-01-01",
    training_end_date = "2021-01-01",
    stop_value_var = 20,
    profit_value_var = 100,
    risk_dollar_value_var = 30,
    currency_conversion = currency_conversion,
    asset_infor = asset_infor,
    sig_thresh_LM = 0.01
    ) {

    actual_wins_losses <-
      get_actual_wins_losses(
        assets_to_analyse = asset_to_trade,
        asset_data = Indices_Metals_Bonds,
        stop_factor = stop_value_var,
        profit_factor = profit_value_var,
        risk_dollar_value = risk_dollar_value_var,
        trade_direction = "Long",
        currency_conversion = currency_conversion,
        asset_infor = asset_infor,
        periods_ahead = 50
      )

    actual_wins_losses <-
      actual_wins_losses %>%
      dplyr::select(Date, Asset ,contains("period_return_")) %>%
      pivot_longer(-c(Date,Asset), names_to = "string_hour", values_to = "returns") %>%
      mutate(
        hours_ahead =
          str_remove_all(string_hour, "[A-Z]+|[a-z]+|\\_") %>% as.numeric()
      ) %>%
      rename(
        Trade_Date = Date
      ) %>%
      mutate(
        Date = Trade_Date + hours(hours_ahead)
      )

    AR_data_all <- list()

    for (i in 1:length(asset_to_trade)) {
      AR_data_all[[i]] <-
        Single_Asset_V3_AR_Model_data(
          asset_data = Indices_Metals_Bonds[[1]] %>% filter(Asset == asset_to_trade[i]),
          asset_of_interest = asset_to_trade[i],
          lag_value_1 = 2,
          lag_value_2 = 4,
          lag_value_3 = 6,
          lag_value_4 = 8,
          lag_value_5 = 10,
          lag_value_6 = 12,
          lag_value_7 = 20,
          lag_value_8 = 100,
          MA_period_1 = 5,
          MA_period_2 = 10,
          MA_period_3 = 15,
          MA_period_4 = 20,
          MA_period_5 = 30,
          MA_period_6 = 40,
          MA_period_7 = 80,
          MA_period_8 = 100
        )
    }

    AR_data_all <-
      AR_data_all %>%
      map_dfr(bind_rows) %>%
      filter(if_all(everything(), ~ !is.na(.)))
    gc()

    actual_wins_losses_with_leads <-
      actual_wins_losses %>%
      group_by(Trade_Date, Asset) %>%
      mutate(
        end_value =
          case_when(
            hours_ahead == max(hours_ahead, na.rm = T) ~ returns
          ),
        mid_value =
          case_when(
            hours_ahead == 25 ~ returns
          )
       )%>%
      fill(c(end_value,mid_value), .direction = "updown") %>%
      ungroup()

    rm(actual_wins_losses)
    gc()

    model_data <-
      actual_wins_losses_with_leads %>%
      ungroup() %>%
      left_join(AR_data_all %>% ungroup(), by = c("Asset", "Date")) %>%
      filter(if_all(contains("MA_")|contains("lagged_")|contains("MSD_"), ~ !is.na(.)) )

    forward_estimator_model_data <-
      model_data %>%
      filter(hours_ahead != end_value & hours_ahead != mid_value) %>%
      group_by(Trade_Date, Asset) %>%
      mutate(
        returns = lag(returns)
      ) %>%
      ungroup() %>%
      filter(
        Trade_Date >= training_end_date
      )

    forward_estimator_model_data <-
      forward_estimator_model_data %>%
      filter(!is.na(returns)) %>%
      mutate(
        bin_var_end = ifelse(end_value >= returns, 1, 0),
        bin_var_mid = ifelse(mid_value >= returns, 1, 0)
      )

    dependants <-
      names(forward_estimator_model_data) %>%
      keep(~ str_detect(.x, "MA_|lagged_|MSD_|hours_ahead|returns"))

    lm_form <-
      create_lm_formula(dependant = "end_value", independant = dependants)

    LM_model <- lm(formula = lm_form,
                   data =
                     forward_estimator_model_data %>%
                     ungroup() %>%
                     filter(hours_ahead != max(hours_ahead, na.rm = T)) )

    sig_coefs <- get_sig_coefs(LM_model, p_value_thresh_for_inputs = sig_thresh_LM)

    lm_form <-
      create_lm_formula(dependant = "end_value", independant = sig_coefs)

    LM_model <- lm(formula = lm_form,
                   data =
                     forward_estimator_model_data %>%
                     ungroup() %>%
                     filter(hours_ahead != max(hours_ahead, na.rm = T)))

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

create_Currency_PortFolio_data <-
  function(
    portfolio_data =
      Indices_Metals_Bonds %>% map(~ .x %>% filter(Date >= "2023-01-01") ),
    pred_data = all_preds %>% filter(Date >= "2023-01-01"),
    assets_to_use = c("USD_JPY", "EUR_USD", "EUR_JPY"),
    stop_factor_var = 4,
    profit_factor_var = 15,
    risk_dollar_value_var = 5,
    end_period = 24,
    time_frame = "H1",
    trade_direction = "Long",
    currency_conversion = currency_conversion,
    asset_infor = asset_infor,
    end_point_loss = -3,
    end_point_profit = 10
    ) {

    return_structure <-
      get_portfolio_model_fast_summed(
      asset_data = portfolio_data,
      asset_of_interest = assets_to_use,
      stop_factor_var = stop_factor_var,
      profit_factor_var = profit_factor_var,
      risk_dollar_value_var = risk_dollar_value_var,
      end_period = end_period,
      time_frame = time_frame,
      trade_direction = trade_direction,
      currency_conversion = currency_conversion,
      asset_infor = asset_infor,
      end_point_loss = -3,
      end_point_profit = 10,
      sum_as_portfolio = TRUE
    )

    reg_list <- list()

    for (i in 1:length(assets_to_use)) {

      asset_return_data <-
        return_structure %>%
        ungroup() %>%
        filter(Asset == assets_to_use[i]) %>%
        mutate(
          Final_Return = lag(Final_Return, 51)
        ) %>%
        rename(
          !!as.name(glue::glue("{assets_to_use[i]}_Final_Return")) := Final_Return
        ) %>%
        dplyr::select(Date, !!as.name(glue::glue("{assets_to_use[i]}_Final_Return")))

      reg_list[[i]] <-
        pred_data %>%
        filter(Asset == assets_to_use[i]) %>%
        ungroup() %>%
        rename(
          !!as.name(glue::glue("{assets_to_use[i]}_AR_LM_Pred_period_return_50_Price")) := AR_LM_Pred_period_return_50_Price,
          !!as.name(glue::glue("{assets_to_use[i]}_AR_GLM_Pred_period_return_50_Price")) := AR_GLM_Pred_period_return_50_Price,
          !!as.name(glue::glue("{assets_to_use[i]}_state_space_LM_Pred_period_return_50_Price")) := state_space_LM_Pred_period_return_50_Price,
          !!as.name(glue::glue("{assets_to_use[i]}_state_space_GLM_Pred_period_return_50_Price")) := state_space_GLM_Pred_period_return_50_Price
        ) %>%
        dplyr::select(Date,
                      !!as.name(glue::glue("{assets_to_use[i]}_AR_LM_Pred_period_return_50_Price")),
                      !!as.name(glue::glue("{assets_to_use[i]}_AR_GLM_Pred_period_return_50_Price")),
                      !!as.name(glue::glue("{assets_to_use[i]}_state_space_LM_Pred_period_return_50_Price")),
                      !!as.name(glue::glue("{assets_to_use[i]}_state_space_GLM_Pred_period_return_50_Price"))
                      ) %>%
        left_join(asset_return_data)

    }

    dependant_variable <-
      return_structure %>%
      ungroup() %>%
      group_by(Date) %>%
      summarise(Final_Return = sum(Final_Return, na.rm = T)) %>%
      ungroup()

    reg_dat <-
      reg_list %>%
      reduce(left_join) %>%
      left_join(dependant_variable) %>%
      mutate(
        stop_factor = stop_factor_var,
        profit_factor = profit_factor_var,
        risk_dollar_value = risk_dollar_value_var,
        end_point_loss = end_point_loss,
        end_point_profit = end_point_profit
      )

  }
