#' gen_port_LM_with_Errors_Bayes_data
#'
#' @param cor_high_diff_data
#' @param regression_length
#' @param portfolio_actuals_data
#' @param dependant_var
#' @param date_filter_train
#' @param sig_thresh_LM
#' @param padding_value
#' @param lag_value_error
#'
#' @returns
#' @export
#'
#' @examples
gen_port_LM_with_Errors_Bayes_data <-
  function(
    cor_high_diff_data = cor_dat_X,
    regression_length = 10000,
    portfolio_actuals_data = portfolio_data,
    dependant_var = "Final_Return",
    date_filter_train = "2025-01-01",
    padding_value = 24,
    lag_value_error = 24,
    direct_return_cols = 22,
    save_location = "D:/trade_data/Day_Trader_Cor_Continuous_Models/",
    model_prefix = "Static"
  ) {

    reg_dat <-
      cor_high_diff_data %>%
      ungroup() %>%
      arrange(Date) %>%
      # mutate(
      #   across(.cols = -c(Date) & where(is.numeric), ~ lag(.) )
      # ) %>%
      filter(if_all(everything(), ~ !is.na(.)))

    reg_vars <-
      names(reg_dat) %>%
      keep(~ !str_detect(.x, "Date") & !str_detect(.x, "Final_Return")) %>%
      unlist()

    lagged_returns_x <-
      seq(lag_value_error + 1, lag_value_error + 20,1) %>%
      map(
        ~ glue::glue("Lagged_Final_Return_{.x} = lag(Final_Return, {.x}), Lagged_Final_Return_{.x} = lag(Final_Return, {.x})^2")
      ) %>%
      unlist() %>%
      paste(collapse = ",")

    lagged_returns_statement <-
      glue::glue("reg_dat %>% group_by(Asset) %>% arrange(Date, .by_group = TRUE) %>% group_by(Asset) %>% mutate({lagged_returns_x}) %>% ungroup()")

    needed_direct_period_cols <-
      seq(1,direct_return_cols,1) %>%
      map(~ glue::glue("period_return_{.x}_Price")) %>%
      unlist()

    needed_direct_lag_cols <-
      seq(1,direct_return_cols,1) %>%
      map(~ glue::glue("Period_Return_Lag_{.x} = lag(period_return_{.x}_Price, {.x + 1})")) %>%
      unlist() %>%
      paste(collapse = ",")

    needed_direct_lag_cols <-
      glue::glue("period_lag_cols %>%
                 group_by(Asset) %>%
                 arrange(Date, .by_group = TRUE) %>%
                 group_by(Asset) %>%
                 mutate({needed_direct_lag_cols}) %>%
                 ungroup()")

    period_lag_cols <-
      portfolio_actuals_data %>%
      # portfolio_data_train %>%
      arrange(Date) %>%
      ungroup() %>%
      dplyr::select(Date, Asset, matches(needed_direct_period_cols))

    period_lag_cols <- eval(parse(text = needed_direct_lag_cols)) %>%
      dplyr::select(Date, Asset, contains("Period_Return_Lag_"))

    state_space_data <-
      portfolio_LM_state_space(
        portfolio_data = portfolio_actuals_data,
        state_space_col = "Final_Return",
        required_lag = lag_value_error, #Does not need a plus 1 its built in
        roll_period_state_space = 500
      )

    state_space_data_24 <-
      portfolio_LM_state_space(
        portfolio_data = portfolio_actuals_data,
        state_space_col = "period_return_24_Price",
        required_lag = 24, #Does not need a plus 1 its built in
        roll_period_state_space = 500
      )

    state_space_data_8 <-
      portfolio_LM_state_space(
        portfolio_data = portfolio_actuals_data,
        state_space_col = "period_return_8_Price",
        required_lag = 8, #Does not need a plus 1 its built in
        roll_period_state_space = 500
      )

    state_space_data_6 <-
      portfolio_LM_state_space(
        portfolio_data = portfolio_actuals_data,
        state_space_col = "period_return_6_Price",
        required_lag = 6, #Does not need a plus 1 its built in
        roll_period_state_space = 500
      )

    state_space_data_4 <-
      portfolio_LM_state_space(
        portfolio_data = portfolio_actuals_data,
        state_space_col = "period_return_4_Price",
        required_lag = 4, #Does not need a plus 1 its built in
        roll_period_state_space = 500
      )

    state_space_data_10 <-
      portfolio_LM_state_space(
        portfolio_data = portfolio_actuals_data,
        state_space_col = "period_return_10_Price",
        required_lag = 10, #Does not need a plus 1 its built in
        roll_period_state_space = 500
      )

    state_space_data_12 <-
      portfolio_LM_state_space(
        portfolio_data = portfolio_actuals_data,
        state_space_col = "period_return_12_Price",
        required_lag = 12, #Does not need a plus 1 its built in
        roll_period_state_space = 500
      )

    state_space_data_50 <-
      portfolio_LM_state_space(
        portfolio_data = portfolio_actuals_data,
        state_space_col = "period_return_50_Price",
        required_lag = 50, #Does not need a plus 1 its built in
        roll_period_state_space = 500
      )

    state_space_data_100 <-
      portfolio_LM_state_space(
        portfolio_data = portfolio_actuals_data,
        state_space_col = "period_return_100_Price",
        required_lag = 100, #Does not need a plus 1 its built in
        roll_period_state_space = 500
      )

    state_space_data_90 <-
      portfolio_LM_state_space(
        portfolio_data = portfolio_actuals_data,
        state_space_col = "period_return_90_Price",
        required_lag = 90, #Does not need a plus 1 its built in
        roll_period_state_space = 500
      )


    technical_data_final_return <-
      portfolio_LM_roll_sum_and_mean(
        portfolio_data = portfolio_actuals_data,
        return_col = "Final_Return",
        required_lag = lag_value_error + 1,
        start_period = 20,
        end_period = 300,
        increment_period = 20
      )

    technical_data_period_24 <-
      portfolio_LM_roll_sum_and_mean(
        portfolio_data = portfolio_actuals_data,
        return_col = "period_return_24_Price",
        required_lag = 24 + 1,
        start_period = 20,
        end_period = 300,
        increment_period = 20
      )

    technical_data_period_50 <-
      portfolio_LM_roll_sum_and_mean(
        portfolio_data = portfolio_actuals_data,
        return_col = "period_return_50_Price",
        required_lag = 50 + 1,
        start_period = 20,
        end_period = 300,
        increment_period = 20
      )

    technical_data_period_12 <-
      portfolio_LM_roll_sum_and_mean(
        portfolio_data = portfolio_actuals_data,
        return_col = "period_return_12_Price",
        required_lag = 12 + 1,
        start_period = 20,
        end_period = 300,
        increment_period = 20
      )

    technical_data_period_8 <-
      portfolio_LM_roll_sum_and_mean(
        portfolio_data = portfolio_actuals_data,
        return_col = "period_return_8_Price",
        required_lag = 8 + 1,
        start_period = 20,
        end_period = 300,
        increment_period = 20
      )

    technical_data_period_4 <-
      portfolio_LM_roll_sum_and_mean(
        portfolio_data = portfolio_actuals_data,
        return_col = "period_return_4_Price",
        required_lag = 4 + 1,
        start_period = 20,
        end_period = 300,
        increment_period = 20
      )

    brownian_tech_data_1 <-
      portfolio_LM_brownian_checks(portfolio_data = portfolio_actuals_data,
                                   brownian_period = 100,
                                   col_to_use = "period_return_1_Price",
                                   lag_period_to_use = 1)

    brownian_tech_data_10 <-
      portfolio_LM_brownian_checks(portfolio_data = portfolio_actuals_data,
                                   brownian_period = 10,
                                   col_to_use = "period_return_1_Price",
                                   lag_period_to_use = 1)

    brownian_tech_data_1_20 <-
      portfolio_LM_brownian_checks(portfolio_data = portfolio_actuals_data,
                                   brownian_period = 20,
                                   col_to_use = "period_return_1_Price",
                                   lag_period_to_use = 1)

    brownian_tech_data_1_40 <-
      portfolio_LM_brownian_checks(portfolio_data = portfolio_actuals_data,
                                   brownian_period = 40,
                                   col_to_use = "period_return_1_Price",
                                   lag_period_to_use = 1)

    brownian_tech_data_1_60 <-
      portfolio_LM_brownian_checks(portfolio_data = portfolio_actuals_data,
                                   brownian_period = 60,
                                   col_to_use = "period_return_1_Price",
                                   lag_period_to_use = 1)

    brownian_tech_data_1_80 <-
      portfolio_LM_brownian_checks(portfolio_data = portfolio_actuals_data,
                                   brownian_period = 80,
                                   col_to_use = "period_return_1_Price",
                                   lag_period_to_use = 1)

    brownian_tech_data_1_200 <-
      portfolio_LM_brownian_checks(portfolio_data = portfolio_actuals_data,
                                   brownian_period = 200,
                                   col_to_use = "period_return_1_Price",
                                   lag_period_to_use = 1)


    brownian_tech_data_24 <-
      portfolio_LM_brownian_checks(portfolio_data = portfolio_actuals_data,
                                   brownian_period = 100,
                                   col_to_use = "period_return_24_Price",
                                   lag_period_to_use = 24)

    brownian_tech_data_24_200 <-
      portfolio_LM_brownian_checks(portfolio_data = portfolio_actuals_data,
                                   brownian_period = 200,
                                   col_to_use = "period_return_24_Price",
                                   lag_period_to_use = 24)

    brownian_tech_data_50_200 <-
      portfolio_LM_brownian_checks(portfolio_data = portfolio_actuals_data,
                                   brownian_period = 200,
                                   col_to_use = "period_return_50_Price",
                                   lag_period_to_use = 50)

    brownian_tech_data_50_100 <-
      portfolio_LM_brownian_checks(portfolio_data = portfolio_actuals_data,
                                   brownian_period = 100,
                                   col_to_use = "period_return_50_Price",
                                   lag_period_to_use = 50)

    brownian_tech_data_50_50 <-
      portfolio_LM_brownian_checks(portfolio_data = portfolio_actuals_data,
                                   brownian_period = 50,
                                   col_to_use = "period_return_50_Price",
                                   lag_period_to_use = 50)

    brownian_tech_data_50_24 <-
      portfolio_LM_brownian_checks(portfolio_data = portfolio_actuals_data,
                                   brownian_period = 24,
                                   col_to_use = "period_return_50_Price",
                                   lag_period_to_use = 50)

    brownian_tech_data_100_200 <-
      portfolio_LM_brownian_checks(portfolio_data = portfolio_actuals_data,
                                   brownian_period = 200,
                                   col_to_use = "period_return_100_Price",
                                   lag_period_to_use = 100)

    brownian_tech_data_80_200 <-
      portfolio_LM_brownian_checks(portfolio_data = portfolio_actuals_data,
                                   brownian_period = 200,
                                   col_to_use = "period_return_80_Price",
                                   lag_period_to_use = 80)

    reg_dat <-
      portfolio_actuals_data %>%
      ungroup() %>%
      left_join(reg_dat) %>%
      filter(if_all(everything(), ~ !is.na(.))) %>%
      mutate(Asset = as.factor(Asset)) %>%
      left_join(period_lag_cols) %>%
      left_join(state_space_data) %>%
      left_join(state_space_data_24) %>%
      left_join(state_space_data_8) %>%
      left_join(state_space_data_4) %>%
      left_join(state_space_data_12) %>%
      left_join(state_space_data_50) %>%
      left_join(state_space_data_6) %>%
      left_join(state_space_data_10) %>%
      left_join(state_space_data_100) %>%
      left_join(state_space_data_90) %>%
      left_join(technical_data_final_return)%>%
      left_join(technical_data_period_24) %>%
      left_join(technical_data_period_12) %>%
      left_join(technical_data_period_8) %>%
      left_join(technical_data_period_4) %>%
      left_join(technical_data_period_50) %>%
      left_join(brownian_tech_data_1) %>%
      left_join(brownian_tech_data_24) %>%
      left_join(brownian_tech_data_24_200) %>%
      left_join(brownian_tech_data_1_200) %>%
      left_join(brownian_tech_data_1_20) %>%
      left_join(brownian_tech_data_1_40) %>%
      left_join(brownian_tech_data_1_60) %>%
      left_join(brownian_tech_data_10) %>%
      left_join(brownian_tech_data_1_80) %>%
      left_join(brownian_tech_data_50_200) %>%
      left_join(brownian_tech_data_50_100) %>%
      left_join(brownian_tech_data_50_50) %>%
      left_join(brownian_tech_data_50_24) %>%
      left_join(brownian_tech_data_100_200) %>%
      left_join(brownian_tech_data_80_200)

    rm(state_space_data, technical_data_final_return, technical_data_period_24, period_lag_cols,
       technical_data_period_4, technical_data_period_8,
       technical_data_period_4, technical_data_period_12,
       state_space_data_24, state_space_data_8, brownian_tech_data_24,
       brownian_tech_data_1, brownian_tech_data_24_200, brownian_tech_data_1_20,
       brownian_tech_data_1_60, brownian_tech_data_1_40, brownian_tech_data_10, brownian_tech_data_50_200,
       state_space_data_50, state_space_data_12, state_space_data_4, brownian_tech_data_50_100,
       state_space_data_6, brownian_tech_data_50_50,
       brownian_tech_data_50_24, state_space_data_10,
       state_space_data_100, brownian_tech_data_80_200, brownian_tech_data_100_200,
       state_space_data_90)
    gc()

    brownian_tech_data_110_200 <-
      portfolio_LM_brownian_checks(portfolio_data = portfolio_actuals_data,
                                   brownian_period = 200,
                                   col_to_use = "period_return_110_Price",
                                   lag_period_to_use = 110)

    brownian_tech_data_90_200 <-
      portfolio_LM_brownian_checks(portfolio_data = portfolio_actuals_data,
                                   brownian_period = 200,
                                   col_to_use = "period_return_90_Price",
                                   lag_period_to_use = 90)

    brownian_tech_data_110_500 <-
      portfolio_LM_brownian_checks(portfolio_data = portfolio_actuals_data,
                                   brownian_period = 500,
                                   col_to_use = "period_return_110_Price",
                                   lag_period_to_use = 110)

    brownian_tech_data_90_500 <-
      portfolio_LM_brownian_checks(portfolio_data = portfolio_actuals_data,
                                   brownian_period = 500,
                                   col_to_use = "period_return_90_Price",
                                   lag_period_to_use = 90)

    brownian_tech_data_110_750 <-
      portfolio_LM_brownian_checks(portfolio_data = portfolio_actuals_data,
                                   brownian_period = 750,
                                   col_to_use = "period_return_110_Price",
                                   lag_period_to_use = 110)

    brownian_tech_data_90_750 <-
      portfolio_LM_brownian_checks(portfolio_data = portfolio_actuals_data,
                                   brownian_period = 750,
                                   col_to_use = "period_return_90_Price",
                                   lag_period_to_use = 90)

    reg_dat <-
      reg_dat %>%
      left_join(brownian_tech_data_110_200) %>%
      left_join(brownian_tech_data_90_200) %>%
      left_join(brownian_tech_data_90_500) %>%
      left_join(brownian_tech_data_110_500) %>%
      left_join(brownian_tech_data_90_750) %>%
      left_join(brownian_tech_data_110_750)

    rm(brownian_tech_data_110_200, brownian_tech_data_90_200, brownian_tech_data_110_750,
       brownian_tech_data_90_750, brownian_tech_data_90_500, brownian_tech_data_110_500)

    reg_dat <- eval(parse(text = lagged_returns_statement))

    # additional_static_preds <-
    #   portfolio_LM_get_static_models_preds(
    #     reg_data = reg_dat,
    #     save_location = "D:/trade_data/Day_Trader_Cor_Continuous_Models/",
    #     model_prefix = "Static"
    #   )
    #
    # additional_static_preds <-
    #   additional_static_preds %>%
    #   dplyr::select(Date,Asset, Cor_Model_1_pred, Cor_Model_1_Low_Sig_pred, Cor_Model_2_pred,
    #                 diff_dat_model_1_pred, diff_dat_model_1_Low_Sig_pred, diff_dat_model_2_pred,
    #                 return_based_model_1_pred,
    #                 return_based_model_2_pred)
    #
    # reg_dat <-
    #   reg_dat %>%
    #   left_join(additional_static_preds)

    lagged_return_cols <-
      names(reg_dat) %>%
      keep(~ str_detect(.x, "Lagged_Final_Return_")) %>%
      unlist()

    period_return_cols <-
      names(reg_dat) %>%
      keep(~ str_detect(.x, "Period_Return_Lag_")) %>%
      unlist()

    state_space_cols <-
      names(reg_dat) %>%
      keep(~ str_detect(.x, "state_space")) %>%
      unlist()

    tech_cols_cols <-
      names(reg_dat) %>%
      keep(~ str_detect(.x, "rolling_sum_|rolling_mean_|rolling_sd_|brownian_")) %>%
      unlist()

    reg_vars_additional <-
      c("Asset",
        reg_vars,
        lagged_return_cols,
        period_return_cols,
        state_space_cols,
        tech_cols_cols
        # "Cor_Model_1_pred",
        # "Cor_Model_1_Low_Sig_pred",
        # "Cor_Model_2_pred",
        # "diff_dat_model_1_pred",
        # "diff_dat_model_1_Low_Sig_pred",
        # "diff_dat_model_2_pred",
        # "return_based_model_1_pred",
        # "return_based_model_2_pred"
      ) %>% unique()

    return(
      list(
        "reg_data" = reg_dat,
        "reg_variables" = reg_vars_additional
      )
    )

  }

#' generate_portfolio_LM_with_Errors_Bayes
#'
#' @param cor_high_diff_data
#' @param regression_length
#' @param portfolio_actuals_data
#' @param dependant_var
#' @param date_filter_train
#'
#' @returns
#' @export
#'
#' @examples
gen_port_LM_with_Errors_Bayes_Gen_Model <-
  function(
    reg_data = reg_data,
    reg_variables = reg_variables,
    dependant_var = "Final_Return",
    save_location = "D:/trade_data/Day_Trader_Cor_Continuous_Models/",
    model_prefix = "Equities",
    date_filter_train = "2025-01-01",
    padding_value = 0,
    sig_thresh_LM = 1,
    interact_list_prefix = c("cor")
  ) {

    training_data <-
      reg_data %>%
      filter(Date <= date_filter_train) %>%
      group_by(Asset) %>%
      ungroup()

    training_data <-
      training_data %>%
      filter(if_all(everything(), ~ !is.na(.)))

    if(!is.null(interact_list_prefix)) {
      interact_vars <-
        names(training_data) %>%
        keep(~str_detect(.x, paste(interact_list_prefix, collapse = "|") )) %>%
        unlist()

      interact_list <-
        interact_vars %>%
        map(
          ~ c("Asset", .x)
        )
    } else {
      interact_list <- NULL
    }

    lm_form <-
      create_lm_formula(dependant = dependant_var, independant = reg_variables)

    LM_model <- lm(data = training_data, formula = lm_form)

    sig_coefs <-
      get_sig_coefs(LM_model, p_value_thresh_for_inputs = sig_thresh_LM)

    if(length(sig_coefs) < 1) {
      sig_coefs <- get_sig_coefs(LM_model, p_value_thresh_for_inputs = 10^-7)
    }

    if(length(sig_coefs) < 1) {
      sig_coefs <- get_sig_coefs(LM_model, p_value_thresh_for_inputs = 10^-6)
    }

    if(length(sig_coefs) < 1) {
      sig_coefs <- get_sig_coefs(LM_model, p_value_thresh_for_inputs = 10^-5)
    }

    if(length(sig_coefs) < 1) {
      sig_coefs <- get_sig_coefs(LM_model, p_value_thresh_for_inputs = 10^-4)
    }

    if(length(sig_coefs) < 1) {
      sig_coefs <- get_sig_coefs(LM_model, p_value_thresh_for_inputs = 10^-3)
    }

    sig_coefs <-
      sig_coefs %>%
      keep(~ !str_detect(.x, "Asset[A-Z][A-Z]") ) %>%
      unlist()

    sig_coefs <-
      c("Asset", sig_coefs) %>%
      unlist()

    # lm_form <-
    #   create_lm_formula(dependant = dependant_var,
    #                     independant = sig_coefs)

    lm_form <- create_lm_formula_interact(
      dependant = dependant_var,
      independant = sig_coefs,
      interacts = interact_list
    )

    # LM_model <- bayesreg::bayesreg(formula = lm_form,
    #                      model = "normal",
    #                      data = training_data)

    LM_model <- lm(formula = lm_form, data = training_data)

    predicted_train <-
      predict(newdata = training_data, object =  LM_model, type = "response") %>%
      as_tibble() %>%
      pull(1) %>%
      as.numeric()

    means_by_asset <-
      training_data %>%
      mutate(preds = predicted_train) %>%
      group_by(Asset) %>%
      summarise(
        trained_mean = mean(preds, na.rm = T),
        trained_sd = sd(preds, na.rm = T)
      ) %>%
      ungroup() %>%
      dplyr::select(Asset, trained_mean, trained_sd)

    saveRDS(object = LM_model,
            file = glue::glue("{save_location}/Bayes_{model_prefix}.RDS"))

    saveRDS(object = means_by_asset,
            file = glue::glue("{save_location}/Bayes_{model_prefix}_Mean_SD.RDS"))

    rm(testing_data, means_by_asset, predicted, LM_model, lm_form,
       training_data, reg_dat, cor_high_diff_data, portfolio_actuals_data)

    return()

  }

gen_port_LM_with_Errors_Bayes_Preds <-
  function(
    reg_data = reg_data,
    save_location = "D:/trade_data/Day_Trader_Cor_Continuous_Models/",
    model_prefix = "Equities",
    date_filter_train = "2025-01-01",
    padding_value = 0
  ) {

    LM_model <- readRDS(glue::glue("{save_location}/Bayes_{model_prefix}.RDS"))

    testing_data <-
      reg_data %>%
      filter(Date > (date_filter_train + hours(padding_value)) )

    predicted <- predict(newdata = testing_data, object =  LM_model, type = "response") %>%
      as_tibble() %>%
      pull(1) %>%
      as.numeric()

    means_by_asset <-
      readRDS(glue::glue("{save_location}/Bayes_{model_prefix}_Mean_SD.RDS"))

    returned_data <-
      testing_data %>%
      mutate(
        predicted = predicted
      ) %>%
      left_join(means_by_asset)

    rm(testing_data, means_by_asset, predicted, LM_model, lm_form,
       training_data, reg_dat, cor_high_diff_data, portfolio_actuals_data)

    return(returned_data)


  }

#' gen_port_LM_with_Errors_Bayes_Preds_algo
#'
#' @param Indices_Metals_Bonds
#' @param assets_to_port
#' @param stop_factor_var
#' @param profit_factor_var
#' @param risk_dollar_value_var
#' @param end_period
#' @param trade_direction
#' @param currency_conversion
#' @param asset_infor
#' @param end_point_loss
#' @param end_point_profit
#' @param training_date
#' @param save_location
#' @param model_prefix
#'
#' @returns
#' @export
#'
#' @examples
gen_port_LM_with_Errors_Bayes_Preds_algo <-
  function(
    Indices_Metals_Bonds = Indices_Metals_Bonds,
    assets_to_port = assets_to_port,
    stop_factor_var = stop_factor_var,
    profit_factor_var = profit_factor_var,
    risk_dollar_value_var = risk_dollar_value_var,
    end_period = end_period,
    trade_direction = trade_direction,
    currency_conversion = currency_conversion,
    asset_infor = asset_infor,
    end_point_loss = end_point_loss,
    end_point_profit = end_point_profit,
    low_to_price_lengths = c(100,200),
    cor_periods = c(200,100),
    regression_length = 10000,
    training_date =  "2021-09-17 10:00:00 AEST",
    save_location = "D:/trade_data/Day_Trader_Cor_Continuous_Models/",
    model_prefix = "Equity",
    correlation_data = NULL,
    direct_return_cols = 24,
    lag_value_error = end_period,
    filter_na_for_values = FALSE
  ) {

    training_date <- as_datetime(training_date)

    if(is.null(correlation_data)) {
      correlation_data <-
        get_portfolio_rolling_data(
          asset_data = Indices_Metals_Bonds[[1]],
          asset_of_interest = assets_to_port,
          low_to_price_lengths = low_to_price_lengths,
          cor_periods = cor_periods
        )
    }

    portfolio_data_test <-
      get_portfolio_model_fast_summed(
        asset_data = Indices_Metals_Bonds,
        # asset_data = Indices_Metals_Bonds %>% map(~ .x %>% filter(Date > training_date)),
        asset_of_interest = assets_to_port,
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

    reg_data_bayes_test <-
      gen_port_LM_with_Errors_Bayes_data(
        cor_high_diff_data = correlation_data,
        regression_length = regression_length,
        portfolio_actuals_data = portfolio_data_test,
        dependant_var = "Final_Return",
        date_filter_train = training_date,
        padding_value = 0,
        lag_value_error = lag_value_error,
        direct_return_cols = direct_return_cols
      )

    reg_data_bayes_test[[1]] <-
      reg_data_bayes_test[[1]] %>%
      filter(Date > training_date)

    portfolio_data_test <-
      portfolio_data_test %>%
      filter(Date > training_date)

    if(filter_na_for_values == FALSE) {
      model_prediction_data <-
        gen_port_LM_with_Errors_Bayes_Preds(
          reg_data = reg_data_bayes_test[[1]],
          save_location = save_location,
          model_prefix = model_prefix,
          date_filter_train = training_date,
          padding_value = 100
        )
    }

    if(filter_na_for_values == TRUE) {
      model_prediction_data <-
        gen_port_LM_with_Errors_Bayes_Preds(
          reg_data = reg_data_bayes_test[[1]] %>%
            filter( if_all(everything(), ~!is.na(.)) ),
          save_location = save_location,
          model_prefix = model_prefix,
          date_filter_train = training_date,
          padding_value = 100
        )
    }

    rm(reg_data_bayes_test)
    gc()

    model_prediction_data <-
      model_prediction_data %>%
      ungroup() %>%
      filter(Date > training_date) %>%
      dplyr::select(-Final_Return) %>%
      left_join(portfolio_data_test %>%  dplyr::select(Date, Asset, Final_Return)) %>%
      ungroup() %>%
      group_by(Date) %>%
      mutate(
        portfolio_pred_10000 = sum(predicted, na.rm = T)
      ) %>%
      ungroup() %>%
      group_by(Asset) %>%
      arrange(Date, .by_group = TRUE) %>%
      group_by(Asset) %>%
      mutate(
        portfolio_pred_10000_mean_roll_250 =
          slider::slide_dbl(.x  = portfolio_pred_10000, .f = ~ mean(.x, na.rm = T), .before = 250),
        portfolio_pred_10000_sd_roll_250 =
          slider::slide_dbl(.x  = portfolio_pred_10000, .f = ~ sd(.x, na.rm = T), .before = 250),

        portfolio_pred_10000_mean_roll_50 =
          slider::slide_dbl(.x  = portfolio_pred_10000, .f = ~ mean(.x, na.rm = T), .before = 50),
        portfolio_pred_10000_sd_roll_50 =
          slider::slide_dbl(.x  = portfolio_pred_10000, .f = ~ sd(.x, na.rm = T), .before = 50),

        portfolio_pred_10000_mean_roll_400 =
          slider::slide_dbl(.x  = portfolio_pred_10000, .f = ~ mean(.x, na.rm = T), .before = 400),
        portfolio_pred_10000_sd_roll_400 =
          slider::slide_dbl(.x  = portfolio_pred_10000, .f = ~ sd(.x, na.rm = T), .before = 400),

        rolling_predicted_10000_50 =
          slider::slide_dbl(predicted, .f = ~ mean(.x, na.rm = T), .before = 50),
        rolling_predicted_10000_100 =
          slider::slide_dbl(predicted, .f = ~ mean(.x, na.rm = T), .before = 100),
        rolling_predicted_10000_200 =
          slider::slide_dbl(predicted, .f = ~ mean(.x, na.rm = T), .before = 200),
        rolling_predicted_10000_400 =
          slider::slide_dbl(predicted, .f = ~ mean(.x, na.rm = T), .before = 400),

        rolling_predicted_10000_50_sd =
          slider::slide_dbl(predicted, .f = ~ sd(.x, na.rm = T), .before = 50),
        rolling_predicted_10000_100_sd =
          slider::slide_dbl(predicted, .f = ~ sd(.x, na.rm = T), .before = 100),
        rolling_predicted_10000_200_sd =
          slider::slide_dbl(predicted, .f = ~ sd(.x, na.rm = T), .before = 200),
        rolling_predicted_10000_400_sd =
          slider::slide_dbl(predicted, .f = ~ sd(.x, na.rm = T), .before = 400)

      ) %>%
      ungroup()

    rm(portfolio_data_test)
    return(model_prediction_data)


  }

#' portfolio_LM_gen_static_models
#'
#' @param cor_high_diff_data
#' @param regression_length
#' @param portfolio_actuals_data
#' @param dependant_var
#' @param date_filter_train
#' @param periods_back_from_train_date
#' @param padding_value
#' @param lag_value_error
#' @param direct_return_cols
#' @param save_location
#' @param model_prefix
#'
#' @returns
#' @export
#'
#' @examples
portfolio_LM_gen_static_models <-
  function(
    cor_high_diff_data = correlation_data %>% filter(Date <= training_date),
    regression_length = regression_length,
    portfolio_actuals_data = portfolio_data_train,
    dependant_var = "Final_Return",
    date_filter_train = training_date,
    periods_back_from_train_date = 9000,
    padding_value = 0,
    lag_value_error = lag_value_error,
    direct_return_cols = direct_return_cols,
    save_location = "D:/trade_data/Day_Trader_Cor_Continuous_Models/",
    model_prefix = "Static"
  ) {

    static_train_date <-
      date_filter_train - dhours(periods_back_from_train_date)

    reg_data_bayes_train <-
      gen_port_LM_with_Errors_Bayes_data(
        cor_high_diff_data = correlation_data %>% filter(Date <= training_date),
        regression_length = regression_length,
        portfolio_actuals_data = portfolio_data_train,
        dependant_var = "Final_Return",
        date_filter_train = training_date,
        padding_value = 0,
        lag_value_error = lag_value_error,
        direct_return_cols = direct_return_cols
      )

    reg_dat_static_train <-
      reg_data_bayes_train[[1]]%>%
      filter(Date <= static_train_date)

    reg_vars_static_train <- reg_data_bayes_train[[2]]

    rm(reg_data_bayes_train)
    gc()

    portfolio_actuals_data_train_static <-
      portfolio_actuals_data %>%
      ungroup() %>%
      filter(Date <= static_train_date)

    rm(portfolio_actuals_data)
    gc()

    reg_dat_static_train <-
      reg_dat_static_train %>%
      ungroup() %>%
      group_by(Date) %>%
      mutate(Final_Asset_Port = sum(Final_Return, na.rm = T)) %>%
      ungroup()

    cor_dat_model_regs <-
      reg_vars_static_train %>%
      keep(~ str_detect(.x, "cor")|.x == "Asset")

    cor_dat_model_1 <-
      generate_model_generic_sig_threshold(
        reg_dat = reg_dat_static_train,
        independant_vars = cor_dat_model_regs,
        dependant_var = "Final_Return",
        sig_thresh_LM = 1
      )

    saveRDS(object = cor_dat_model_1,
            file = glue::glue("{save_location}/Bayes_Cor1_{model_prefix}.RDS"))

    rm(cor_dat_model_1)
    gc()

    cor_dat_model_1_low_sig <-
      generate_model_generic_sig_threshold(
        reg_dat = reg_dat_static_train,
        independant_vars = cor_dat_model_regs,
        dependant_var = "Final_Return",
        sig_thresh_LM = 0.01
      )

    saveRDS(object = cor_dat_model_1_low_sig,
            file = glue::glue("{save_location}/Bayes_Cor1_Low_Sig_{model_prefix}.RDS"))

    rm(cor_dat_model_1_low_sig)
    gc()

    cor_dat_model_regs <-
      reg_vars_static_train %>%
      keep(~ str_detect(.x, "cor") & .x != "Asset") %>%
      unlist()

    cor_dat_model_2 <-
      generate_model_generic_sig_threshold(
        reg_dat =
          reg_dat_static_train %>%
          ungroup() %>%
          dplyr::select(Date, contains("_diff"), contains("cor"),  Final_Asset_Port) %>%
          distinct(),
        independant_vars =
          cor_dat_model_regs %>%
          keep(~ .x != "Asset") %>%
          unlist(),
        dependant_var = "Final_Asset_Port",
        sig_thresh_LM = 1,
        contains_asset = FALSE
      )

    saveRDS(object = cor_dat_model_2,
            file = glue::glue("{save_location}/Bayes_Cor2_{model_prefix}.RDS"))

    rm(cor_dat_model_2)
    gc()

    diff_dat_model_regs <-
      reg_vars_static_train %>%
      keep(~ (str_detect(.x, "diff")|.x == "Asset") & !str_detect(.x, "cor") )

    diff_dat_model_1 <-
      generate_model_generic_sig_threshold(
        reg_dat = reg_dat_static_train,
        independant_vars = diff_dat_model_regs,
        dependant_var = "Final_Return",
        sig_thresh_LM = 1
      )

    saveRDS(object = diff_dat_model_1,
            file = glue::glue("{save_location}/Bayes_Diff1_{model_prefix}.RDS"))

    rm(diff_dat_model_1)
    gc()

    diff_dat_model_1_Low_Sig <-
      generate_model_generic_sig_threshold(
        reg_dat = reg_dat_static_train,
        independant_vars = diff_dat_model_regs,
        dependant_var = "Final_Return",
        sig_thresh_LM = 0.01
      )

    saveRDS(object = diff_dat_model_1_Low_Sig,
            file = glue::glue("{save_location}/Bayes_Diff1_Low_Sig_{model_prefix}.RDS"))

    rm(diff_dat_model_1_Low_Sig)
    gc()

    diff_dat_model_regs <-
      reg_vars_static_train %>%
      keep(~ (str_detect(.x, "diff")) & !str_detect(.x, "cor") )

    diff_dat_model_2 <-
      generate_model_generic_sig_threshold(
        reg_dat =
          reg_dat_static_train %>%
          ungroup() %>%
          dplyr::select(Date, contains("_diff"), contains("cor"),  Final_Asset_Port) %>%
          distinct(),
        independant_vars =
          diff_dat_model_regs %>%
          keep(~ .x != "Asset") %>%
          unlist(),
        dependant_var = "Final_Asset_Port",
        sig_thresh_LM = 1,
        contains_asset = FALSE
      )

    saveRDS(object = diff_dat_model_2,
            file = glue::glue("{save_location}/Bayes_Diff2_{model_prefix}.RDS"))

    rm(diff_dat_model_2)
    gc()

    return_based_model_regs <-
      reg_vars_static_train %>%
      keep(~ (str_detect(.x, "Period_Return_Lag_")|.x == "Asset") )

    return_based_model_1 <-
      generate_model_generic_sig_threshold(
        reg_dat = reg_dat_static_train,
        independant_vars = return_based_model_regs,
        dependant_var = "Final_Return",
        sig_thresh_LM = 1
      )

    saveRDS(object = return_based_model_1,
            file = glue::glue("{save_location}/Bayes_Return1_{model_prefix}.RDS"))

    rm(return_based_model_1)
    gc()

    return_based_model_Low_Sig <-
      generate_model_generic_sig_threshold(
        reg_dat = reg_dat_static_train,
        independant_vars = return_based_model_regs,
        dependant_var = "Final_Return",
        sig_thresh_LM = 0.01
      )

    saveRDS(object = return_based_model_Low_Sig,
            file = glue::glue("{save_location}/Bayes_Return1_Low_Sig_{model_prefix}.RDS"))

    rm(return_based_model_1)
    gc()



  }

#' portfolio_LM_get_static_models_preds
#'
#' @param reg_data
#' @param save_location
#' @param model_prefix
#' @param date_filter_train
#'
#' @returns
#' @export
#'
#' @examples
portfolio_LM_get_static_models_preds <-
  function(
    reg_data = reg_data_bayes_train,
    save_location = "D:/trade_data/Day_Trader_Cor_Continuous_Models/",
    model_prefix = "Static"
  ) {

    Cor_Model_1 <- readRDS(file = glue::glue("{save_location}/Bayes_Cor1_{model_prefix}.RDS"))

    testing_data <-
      reg_data %>%
      filter(if_all(everything(), ~ !is.na(.)))

    Cor_Model_1_pred <- predict(newdata = testing_data, object =  Cor_Model_1, type = "response") %>%
      as_tibble() %>%
      pull(1) %>%
      as.numeric()

    rm(Cor_Model_1)
    gc()

    Cor_Model_1_Low_Sig <- readRDS(file = glue::glue("{save_location}/Bayes_Cor1_Low_Sig_{model_prefix}.RDS"))

    Cor_Model_1_Low_Sig_pred <- predict(newdata = testing_data, object =  Cor_Model_1_Low_Sig, type = "response") %>%
      as_tibble() %>%
      pull(1) %>%
      as.numeric()

    rm(Cor_Model_1_Low_Sig)
    gc()

    Cor_Model_2 <- readRDS(file = glue::glue("{save_location}/Bayes_Cor2_{model_prefix}.RDS") )

    Cor_Model_2_pred <- predict(newdata = testing_data, object =  Cor_Model_2, type = "response") %>%
      as_tibble() %>%
      pull(1) %>%
      as.numeric()

    rm(Cor_Model_2)
    gc()

    diff_dat_model_1 <- readRDS(glue::glue("{save_location}/Bayes_Diff1_{model_prefix}.RDS"))

    diff_dat_model_1_pred <- predict(newdata = testing_data, object =  diff_dat_model_1, type = "response") %>%
      as_tibble() %>%
      pull(1) %>%
      as.numeric()

    rm(diff_dat_model_1)
    gc()

    diff_dat_model_1_Low_Sig <- readRDS(glue::glue("{save_location}/Bayes_Diff1_{model_prefix}.RDS"))

    diff_dat_model_1_Low_Sig_pred <-
      predict(newdata = testing_data, object =  diff_dat_model_1_Low_Sig, type = "response") %>%
      as_tibble() %>%
      pull(1) %>%
      as.numeric()

    rm(diff_dat_model_1_Low_Sig)
    gc()

    diff_dat_model_2 <- readRDS(glue::glue("{save_location}/Bayes_Diff2_{model_prefix}.RDS"))

    diff_dat_model_2_pred <-
      predict(newdata = testing_data, object =  diff_dat_model_2, type = "response") %>%
      as_tibble() %>%
      pull(1) %>%
      as.numeric()

    rm(diff_dat_model_2)
    gc()

    return_based_model_1 <- readRDS(glue::glue("{save_location}/Bayes_Return1_{model_prefix}.RDS"))
    return_based_model_1_pred <-
      predict(newdata = testing_data, object =  return_based_model_1, type = "response") %>%
      as_tibble() %>%
      pull(1) %>%
      as.numeric()

    rm(return_based_model_1)
    gc()

    return_based_model_2 <- readRDS(glue::glue("{save_location}/Bayes_Return1_Low_Sig_{model_prefix}.RDS"))
    return_based_model_2_pred <-
      predict(newdata = testing_data, object =  return_based_model_2, type = "response") %>%
      as_tibble() %>%
      pull(1) %>%
      as.numeric()

    rm(return_based_model_2)
    gc()

    returned_data <-
      testing_data %>%
      mutate(
        Cor_Model_1_pred = Cor_Model_1_pred,
        Cor_Model_1_Low_Sig_pred = Cor_Model_1_Low_Sig_pred,
        Cor_Model_2_pred = Cor_Model_2_pred,
        diff_dat_model_1_pred = diff_dat_model_1_pred,
        diff_dat_model_1_Low_Sig_pred = diff_dat_model_1_Low_Sig_pred,
        diff_dat_model_2_pred = diff_dat_model_2_pred,
        return_based_model_1_pred = return_based_model_1_pred,
        return_based_model_2_pred = return_based_model_2_pred
      )

    return(returned_data)

  }
