Portfolio_get_port_preds_V3 <-
  function(
    Indices_Metals_Bonds = Indices_Metals_Bonds,
    raw_macro_data = raw_macro_data,
    base_path = "C:/Users/nikhi/Documents/trade_data/Day_Trader_Single_Asset_V3_Expanded_Models/",
    actuals_periods_needed = c("period_return_50_Price"),
    state_space_periods = c(20, 40, 60, 100, 200,300, 400,  500),
    state_space_rolling = c(100, 200, 300, 400),
    date_for_true_simualtion = "2019-01-01",
    training_end_date = "2021-01-01",
    assets_to_test = assets_to_port,
    stop_factor_var =5,
    profit_factor_var =10,
    risk_dollar_value_var = 10,
    end_period = 24,
    trade_direction = "Long",
    end_point_loss = -7.5,
    end_point_profit = 15,
    low_to_price_lengths = c(200),
    cor_periods = c(200),
    max_regs = 1000,
    currency_conversion,
    asset_infor
    ) {

    all_preds <-
      Portfolio_get_all_preds_frm_V3(
        Indices_Metals_Bonds = Indices_Metals_Bonds,
        raw_macro_data = raw_macro_data,
        base_path = base_path,
        actuals_periods_needed = actuals_periods_needed,
        state_space_periods = state_space_periods,
        state_space_rolling = state_space_rolling,
        date_for_true_simualtion = date_for_true_simualtion,
        training_end_date = training_end_date,
        assets_to_test = assets_to_test
      )

    portfolio_data <-
      get_portfolio_model_fast_summed(
        asset_data = Indices_Metals_Bonds,
        asset_of_interest = assets_to_test,
        stop_factor_var = stop_factor_var,
        profit_factor_var = profit_factor_var,
        risk_dollar_value_var = risk_dollar_value_var,
        end_period = end_period,
        time_frame = "H1",
        trade_direction = trade_direction,
        currency_conversion = currency_conversion,
        asset_infor = asset_infor,
        end_point_loss = end_point_loss,
        end_point_profit = end_point_profit,
        sum_as_portfolio = TRUE
      )

    all_cor_V3_Data <-
      portfolio_get_V3_cor_data_TOTAL_SUMMED(
        Indices_Metals_Bonds = Indices_Metals_Bonds,
        all_preds = all_preds,
        portfolio_data = portfolio_data,
        assets_to_port = assets_to_test,
        low_to_price_lengths = low_to_price_lengths,
        cor_periods = cor_periods,
        max_regs = max_regs
      )

    max_date_data <- all_cor_V3_Data[[1]] %>% pull(Date) %>% max(na.rm = T)

    results_temp <-
      Porfolio_get_V3_LM_Model_TOTAL_SUM(
        all_cor_V3_Data = all_cor_V3_Data[[1]],
        reg_vars = all_cor_V3_Data[[2]],
        training_end_date = max_date_data,
        regression_length = 10000,
        dependant_var = "Final_Return",
        sig_thresh_LM = 0.1,
        return_training_bound = TRUE
      ) %>%
      dplyr::select(Date,
                    # Asset,
                    Final_Return,
                    predicted_10000 = predicted,
                    trained_mean_10000 = trained_mean,
                    trained_sd_10000 = trained_sd) %>%
      filter(Date > all_dates_sim[i], Date <= all_dates_sim[i + 1])

    results_temp2 <-
      Porfolio_get_V3_LM_Model_TOTAL_SUM(
        all_cor_V3_Data = all_cor_V3_Data[[1]],
        reg_vars = all_cor_V3_Data[[2]],
        training_end_date = all_dates_sim[i],
        regression_length = 5000,
        dependant_var = "Final_Return",
        sig_thresh_LM = 0.1,
        return_training_bound = TRUE
      ) %>%
      dplyr::select(Date,
                    # Asset,
                    predicted_5000 = predicted,
                    trained_mean_5000 = trained_mean,
                    trained_sd_5000 = trained_sd) %>%
      filter(Date > all_dates_sim[i], Date <= all_dates_sim[i + 1])

    results_temp3 <-
      Porfolio_get_V3_LM_Model_TOTAL_SUM(
        all_cor_V3_Data = all_cor_V3_Data[[1]],
        reg_vars = all_cor_V3_Data[[2]],
        training_end_date = all_dates_sim[i],
        regression_length = 2500,
        dependant_var = "Final_Return",
        sig_thresh_LM = 0.1,
        return_training_bound = TRUE
      ) %>%
      dplyr::select(Date,
                    # Asset,
                    predicted_2500 = predicted,
                    trained_mean_2500 = trained_mean,
                    trained_sd_2500 = trained_sd) %>%
      filter(Date > all_dates_sim[i], Date <= all_dates_sim[i + 1])

    results_temp <-
      results_temp %>%
      left_join(results_temp2) %>%
      left_join(results_temp3)


  }
