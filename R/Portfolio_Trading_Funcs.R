#' generate_model_generic_sig_threshold
#'
#' @param reg_dat
#' @param independant_vars
#' @param dependant_var
#' @param sig_coef
#'
#' @returns
#' @export
#'
#' @examples
generate_model_generic_sig_threshold <-
  function(
    reg_dat = reg_dat_static_train,
    independant_vars = cor_dat_model_regs,
    dependant_var = "Final_Return",
    sig_thresh_LM = 1,
    contains_asset = TRUE
  ) {

    lm_form <-
      create_lm_formula(dependant = dependant_var, independant = independant_vars)

    LM_model <- lm(data = reg_dat, formula = lm_form)

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

    if(contains_asset == TRUE) {
      sig_coefs <-
        c("Asset", sig_coefs) %>%
        unlist()
    } else {

      sig_coefs <-
        c(sig_coefs) %>%
        unlist()

    }

    lm_form <-
      create_lm_formula(dependant = dependant_var,
                        independant = sig_coefs)

    LM_model <- bayesreg::bayesreg(formula = lm_form,
                                   model = "normal",
                                   data = reg_dat)

    return(LM_model)

  }

#' portfolio_LM_state_space
#'
#' @param portfolio_data
#' @param state_space_col
#' @param required_lag
#' @param roll_period_state_space
#'
#' @returns
#' @export
#'
#' @examples
portfolio_LM_state_space <-
  function(
    portfolio_data = portfolio_data_train,
    state_space_col = "period_return_24_Price",
    required_lag = end_period,
    roll_period_state_space = end_period
  ) {

    state_space_col_var <- state_space_col
    state_space_statement_1 <-
      seq(-6,0,0.25) %>%
      map(
        ~ glue::glue("state_space_neg_{state_space_col_var}_{abs(.x)} =
                     case_when(
                     state_space_var >= state_space_mean + state_space_sd*({.x}) &
                     state_space_var < state_space_mean + state_space_sd*({.x + 0.25}) ~ 1,
                     TRUE ~ 0
                     )")
      ) %>%
      unlist() %>%
      as.character()

    state_space_statement_min <-
      glue::glue("state_space_neg_{state_space_col_var}_7 =
                     case_when(
                     state_space_var < state_space_mean - state_space_sd*6 ~ 1,
                     TRUE ~ 0
                     )"
      )

    state_space_statement_2 <-
      seq(0.25,6,0.25) %>%
      map(
        ~ glue::glue("
        state_space_pos_{state_space_col_var}_{abs(.x)} =
        case_when(
                  state_space_var < state_space_mean + state_space_sd*({.x + 0.25}) &
                  state_space_var >= state_space_mean + state_space_sd*({.x}) ~ 1,
                  TRUE ~ 0
                     )")
      ) %>%
      unlist() %>%
      as.character()

    state_space_statement_max <-
      glue::glue("state_space_pos_{state_space_col_var}_7 =
                 case_when(state_space_var >= state_space_mean + state_space_sd*6.25 ~ 1,
                 TRUE ~ 0)")

    state_space_all <-
      c(state_space_statement_min, state_space_statement_1, state_space_statement_2, state_space_statement_max) %>%
      paste(collapse = ",")


    case_when_statement <-
      glue::glue("state_space_data %>% mutate( {state_space_all} )")

    state_space_data <-
      portfolio_data %>%
      ungroup() %>%
      group_by(Asset) %>%
      arrange(Date, .by_group = TRUE) %>%
      group_by(Asset) %>%
      mutate(
        state_space_var = lag(!!as.name(state_space_col), required_lag + 1),
        state_space_mean =
          slider::slide_dbl(.x = state_space_var, .f = ~ mean(.x, na.rm = T), .before = roll_period_state_space),
        state_space_sd =
          slider::slide_dbl(.x = state_space_var, .f = ~ sd(.x, na.rm = T), .before = roll_period_state_space)
      ) %>%
      ungroup() %>%
      dplyr::select(Date, Asset, state_space_var, state_space_mean, state_space_sd)

    state_space_data <- eval(parse(text = case_when_statement))
    state_space_data_wide <-
      state_space_data %>%
      filter(!is.na(state_space_var)) %>%
      dplyr::select(-state_space_var, -state_space_mean, -state_space_sd)

    new_names <-
      names(state_space_data_wide) %>%
      keep(~str_detect(.x, "state_space")) %>%
      unlist() %>%
      map(~str_remove_all(.x, "\\.")) %>%
      unlist()

    new_names <- c("Date", "Asset", new_names)

    names(state_space_data_wide) <- new_names

    total_state_space <-
      names(state_space_data_wide) %>%
      keep(~str_detect(.x, "state_space")) %>%
      unlist() %>%
      length()

    rolling_col_statements_100 <-
      names(state_space_data_wide) %>%
      keep(~str_detect(.x, "state_space")) %>%
      map(
        ~ glue::glue("{.x}_rolling_100 = slider::slide_dbl({.x}, ~sum(.x, na.rm = TRUE)/total_state_space, .before = 100)")
      ) %>%
      unlist() %>%
      paste(collapse = ",")

    rolling_mutate_statement <-
      glue::glue("state_space_data_wide %>% mutate({rolling_col_statements_100})")

    state_space_data_wide_rolling <-
      eval(parse(text = rolling_mutate_statement))

    rm(state_space_data, state_space_data_wide)
    gc()

    return(state_space_data_wide_rolling)


  }

#' portfolio_LM_state_space
#'
#' @param portfolio_data
#' @param required_lag
#' @param roll_period_state_space
#' @param return_col
#' @param start_period
#' @param end_period
#' @param increment_period
#'
#' @returns
#' @export
#'
#' @examples
portfolio_LM_roll_sum_and_mean <-
  function(
    portfolio_data = portfolio_data_train,
    return_col = "Final_Return",
    required_lag = end_period,
    start_period = 20,
    end_period = 300,
    increment_period = 20
  ) {

    ma_statements <-
      seq(start_period,end_period,increment_period) %>%
      map(
        ~ glue::glue("rolling_mean_{return_col}_{.x} = slider::slide_dbl(.x = lag({return_col}, {required_lag}), .f = ~ mean(.x, na.rm = T), .before = {.x} )  ")
      ) %>%
      unlist() %>%
      paste(collapse = ",")

    sd_statements <-
      seq(start_period,end_period,increment_period) %>%
      map(
        ~ glue::glue("rolling_sd_{return_col}_{.x} = slider::slide_dbl(.x = lag({return_col}, {required_lag}), .f = ~ sd(.x, na.rm = T), .before = {.x} )  ")
      ) %>%
      unlist() %>%
      paste(collapse = ",")

    sum_statements <-
      seq(start_period,end_period,increment_period) %>%
      map(
        ~ glue::glue("rolling_sum_{return_col}_{.x} = slider::slide_dbl(.x = lag({return_col}, {required_lag}), .f = ~ sum(.x, na.rm = T), .before = {.x} )  ")
      ) %>%
      unlist() %>%
      paste(collapse = ",")

    mutate_statement <-
      glue::glue("returned_data <- portfolio_data %>% group_by(Asset) %>% arrange(Date, .by_group = TRUE) %>% group_by(Asset) %>% mutate({ma_statements},{sum_statements}, {sd_statements}) %>% ungroup() ")

    returned_data <- eval(parse(text = mutate_statement))

    returned_data <-
      returned_data %>%
      dplyr::select(Date, Asset, contains("rolling_sd"), contains("rolling_mean"), contains("rolling_sum"))

    return(returned_data)


  }

portfolio_LM_brownian_checks <-
  function(
    portfolio_data = portfolio_data_train,
    brownian_period = 100,
    col_to_use = c("period_return_1_Price"),
    lag_period_to_use = 24
  ) {

    mutate_statement <-
      col_to_use %>%
      map(
        ~
          c(
            glue::glue("brownian_col_check_{.x}_{brownian_period} = slider::slide_sum(x = lag({.x}, {lag_period_to_use + 1}), before = {brownian_period}, na_rm = TRUE)"),
            glue::glue("brownian_col_mean_{.x}_{brownian_period} = slider::slide_dbl(.x = lag({.x}, {lag_period_to_use + 1}), .f = ~ mean(.x, na.rm = T) , .before = {brownian_period})"),
            glue::glue("brownian_col_sd_{.x}_{brownian_period} = slider::slide_dbl(.x = lag({.x}, {lag_period_to_use + 1}), .f = ~ sd(.x, na.rm = T) , .before = {brownian_period})"),
            glue::glue("brownian_percentile_95_{.x}_{brownian_period} = lag(brownian_col_mean_{.x}_{brownian_period}, {brownian_period} ) + 1.96*lag(brownian_col_sd_{.x}_{brownian_period}, {brownian_period})*{brownian_period}"),
            glue::glue("brownian_percentile_95_position_{.x}_{brownian_period} = brownian_col_check_{.x}_{brownian_period}/brownian_percentile_95_{.x}_{brownian_period}")
          )
      ) %>%
      unlist() %>%
      paste(collapse = ", ")

    eval_complete_statement <-
      glue::glue("portfolio_data %>%
                  group_by(Asset) %>%
                  arrange(Date, .by_group = TRUE) %>%
                  group_by(Asset) %>%
                  mutate(
                    {mutate_statement}
                  ) %>%
                 ungroup()")

    brownian_sums <- eval(parse(text = eval_complete_statement))

    brownian_sums <-
      brownian_sums %>%
      ungroup() %>%
      dplyr::select(Date, Asset, contains("brownian_"))

    return(brownian_sums)

  }

#' wide_pivot_asset_data
#'
#' @param all_asset_data
#' @param assets_to_use
#' @param cor_col_to_use
#'
#' @returns
#' @export
#'
#' @examples
wide_pivot_asset_data <- function(
    all_asset_data = Indices_Metals_Bonds[[1]],
    assets_to_use  = assets_to_port,
    cor_col_to_use = "Price"
) {

  asset_col_names <-
    assets_to_use %>%
    map( ~ c(
      .x,
      glue::glue("{.x}_{cor_col_to_use}")
    )
    )

  returned_data <-
    asset_col_names %>%
    map(
      ~ all_asset_data %>%
        filter(Asset == .x[1]) %>%
        ungroup() %>%
        mutate(
          !!as.name(.x[2]) := !!as.name(cor_col_to_use)
        ) %>%
        dplyr::select(Date, !!as.name(.x[2]))
    ) %>%
    reduce(full_join) %>%
    arrange(Date) %>%
    fill(everything(), .direction = "down")

  return(returned_data)

}

#' general_rolling_asset_cor
#'
#' @param all_asset_data
#' @param assets_to_use
#' @param cor_col_to_use
#' @param cor_periods
#'
#' @returns
#' @export
#'
#' @examples
general_rolling_asset_cor <-
  function(all_asset_data = Indices_Metals_Bonds[[1]],
           assets_to_use  = assets_to_port,
           cor_col_to_use = "Price",
           cor_periods = c(50,100)
           ) {

    all_asset_data <-
      all_asset_data %>%
      filter(Asset %in% assets_to_use) %>%
      dplyr::select(Date, Asset, !!as.name(cor_col_to_use))

    wide_data <-
      wide_pivot_asset_data(
        all_asset_data = all_asset_data,
        assets_to_use  = assets_to_port,
        cor_col_to_use = cor_col_to_use
      )

    all_asset_Vars <-
      assets_to_use %>%
      map( ~ c(
        glue::glue("{.x}_{cor_col_to_use}")
      )
      )  %>%
      unlist()

    asset_LM_cor_statements <- c()
    c = 0
    for (i in 1:length(all_asset_Vars)) {
      for (j in 1:length(all_asset_Vars)) {
        for (k in 1:length(cor_periods)) {

          if(all_asset_Vars[i] != all_asset_Vars[j]) {
            c = c + 1
            asset_LM_cor_statements[c] <-
              glue::glue("cor_{all_asset_Vars[i]}_{all_asset_Vars[j]}_{cor_periods[k]} = slider::slide2_dbl(.x = {all_asset_Vars[i]}, .y = {all_asset_Vars[j]}, .f = ~ cor(.x, .y), .before = {cor_periods[k]} )")
          }

        }
      }
    }

    asset_LM_cor_statements_collapse <-
      asset_LM_cor_statements %>%
      paste(collapse = ",")

    Final_LM_Cor_Statement <-
      glue::glue("wide_data %>% mutate({asset_LM_cor_statements_collapse})")

    returned_dat <- eval(parse(text = Final_LM_Cor_Statement))

  }

#' general_rolling_asset_cor
#'
#' @param all_asset_data
#' @param assets_to_use
#' @param cor_col_to_use
#' @param cor_periods
#'
#' @returns
#' @export
#'
#' @examples
general_rolling_Diff_cols <-
  function(all_asset_data = Indices_Metals_Bonds[[1]],
           assets_to_use  = assets_to_port,
           base_col = "Price",
           col_to_minus = "Low",
           diff_periods = c(50,100),
           perc_diff = FALSE,
           log_diff = FALSE) {

    all_asset_data <-
      all_asset_data %>%
      filter(Asset %in% assets_to_use) %>%
      dplyr::select(Date, Asset, !!as.name(base_col), !!as.name(col_to_minus))

    diff_statements <- c()

    if(perc_diff == FALSE & log_diff == FALSE) {
      for (i in 1:length(diff_periods)) {
        diff_statements[i] <-
          glue::glue("{base_col}_diff_{col_to_minus}_{diff_periods[i]} = lag({base_col}) - lag({col_to_minus}, {diff_periods[i]} )")
      }
    }

    if(perc_diff == TRUE) {
      for (i in 1:length(diff_periods)) {
        diff_statements[i] <-
          glue::glue("{base_col}_diff_{col_to_minus}_{diff_periods[i]} = (lag({base_col}) - lag({col_to_minus}, {diff_periods[i]}) )/lag({base_col}) ")
      }
    }

    if(log_diff == TRUE) {
      for (i in 1:length(diff_periods)) {
        diff_statements[i] <-
          glue::glue("{base_col}_diff_{col_to_minus}_{diff_periods[i]} = log( lag({base_col})/lag({col_to_minus}, {diff_periods[i]} ) )")
      }
    }

    all_diff_statements <-
      diff_statements %>%
      paste(collapse = ",")

    Final_LM_Cor_Statement <-
      glue::glue("all_asset_data %>% group_by(Asset) %>% arrange(Date, .by_group = TRUE) %>% group_by(Asset) %>% mutate({all_diff_statements}) %>% ungroup()")

    returned_dat <- eval(parse(text = Final_LM_Cor_Statement))

  }

#' get_portfolio_rolling_data
#'
#' @param asset_data
#' @param asset_of_interest
#' @param low_to_price_lengths
#'
#' @returns
#' @export
#'
#' @examples
get_portfolio_rolling_data <-
  function(
    asset_data = Indices_Metals_Bonds[[1]],
    asset_of_interest = assets_to_port,
    low_to_price_lengths = c(100,200),
    cor_periods = c(50,100)
  ) {

    asset_data <-
      asset_data %>%
      filter(Asset %in% asset_of_interest)

    Low_Price_Data <-
      general_rolling_Diff_cols(
        all_asset_data = asset_data,
        assets_to_use  = assets_to_port,
        base_col = "Price",
        col_to_minus = "Low",
        diff_periods = low_to_price_lengths,
        perc_diff = FALSE,
        log_diff = TRUE
      ) %>%
      dplyr::select(-Price, -Low)

    High_Price_Data <-
      general_rolling_Diff_cols(
        all_asset_data = asset_data,
        assets_to_use  = assets_to_port,
        base_col = "High",
        col_to_minus = "Price",
        diff_periods = low_to_price_lengths,
        perc_diff = FALSE,
        log_diff = TRUE
      ) %>%
      dplyr::select(-Price, -High)

    combined_data <-
      Low_Price_Data %>%
      left_join(High_Price_Data) %>%
      filter(if_all(everything(), ~!is.na(.)))

    diff_cols <-
      names(combined_data) %>%
      keep(~ str_detect(.x, "_diff_") )

    cor_dat_X <- list()
    for (i in 1:length(diff_cols)) {

      cor_dat_X[[i]] <-
        general_rolling_asset_cor(
          all_asset_data = combined_data,
          assets_to_use  = assets_to_port,
          cor_col_to_use = diff_cols[i],
          cor_periods = cor_periods
        )

    }

    cor_dat_X <-
      cor_dat_X %>%
      reduce(left_join)


    return(cor_dat_X)

  }


#' generate_portfolio_LM
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
generate_portfolio_LM <-
  function(
    cor_high_diff_data = cor_dat_X,
    regression_length = 10000,
    portfolio_actuals_data = portfolio_data,
    dependant_var = "Final_Return",
    date_filter_train = "2025-01-01",
    sig_thresh_LM = 10^-7,
    padding_value = 24
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

    reg_dat <-
      portfolio_actuals_data %>%
      ungroup() %>%
      left_join(reg_dat) %>%
      filter(if_all(everything(), ~ !is.na(.)))

    training_data <-
      reg_dat %>%
      filter(Date <= date_filter_train) %>%
      group_by(Asset) %>%
      slice_tail(n = regression_length) %>%
      ungroup()

    testing_data <-
      reg_dat %>%
      filter(Date > (date_filter_train + hours(padding_value)) )

    lm_form <-
      create_lm_formula(dependant = dependant_var, independant = reg_vars)

    LM_model <- lm(data = training_data, formula = lm_form)

    sig_coefs <- get_sig_coefs(LM_model, p_value_thresh_for_inputs = sig_thresh_LM)

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

    lm_form <-
      create_lm_formula(dependant = dependant_var,
                        independant = sig_coefs)

    LM_model <- lm(formula = lm_form, data = training_data)

    predicted <- predict.lm(newdata = testing_data, object =  LM_model)

    means_by_asset <-
      training_data %>%
      mutate(preds = LM_model$fitted.values) %>%
      group_by(Asset) %>%
      summarise(
        trained_mean = mean(preds, na.rm = T),
        trained_sd = sd(preds, na.rm = T)
      ) %>%
      ungroup() %>%
      dplyr::select(Asset, trained_mean, trained_sd)

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


#' create_Currency_PortFolio_data
#'
#' @param portfolio_data
#' @param pred_data
#' @param assets_to_use
#' @param stop_factor_var
#' @param profit_factor_var
#' @param risk_dollar_value_var
#' @param end_period
#' @param time_frame
#' @param trade_direction
#' @param currency_conversion
#' @param asset_infor
#' @param end_point_loss
#' @param end_point_profit
#'
#' @returns
#' @export
#'
#' @examples
create_PortFolio_data <-
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
    end_point_profit = 10,
    cor_periods = c(100,200,300)
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
        end_point_loss = end_point_loss,
        end_point_profit = end_point_profit,
        sum_as_portfolio = TRUE
      )

    reg_list <- list()

    for (i in 1:length(assets_to_use)) {

      asset_return_data <-
        return_structure %>%
        ungroup() %>%
        filter(Asset == assets_to_use[i]) %>%
        mutate(
          Final_Return = lag(Final_Return, 53)
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

    all_asset_LM_Vars <-
      assets_to_use %>%
      map( ~ c(
        glue::glue("{.x}_AR_LM_Pred_period_return_50_Price"),
        glue::glue("{.x}_state_space_LM_Pred_period_return_50_Price")
      )
      )  %>%
      unlist()

    asset_LM_cor_statements <- c()
    c = 0
    for (i in 1:length(all_asset_LM_Vars)) {
      for (j in 1:length(all_asset_LM_Vars)) {
        for (k in 1:length(cor_periods)) {

          if(all_asset_LM_Vars[i] != all_asset_LM_Vars[j]) {
            c = c + 1
            asset_LM_cor_statements[i] <-
              glue::glue("cor_asset_{cor_periods[k]}_LM_{i}_{j} = slider::slide2_dbl(.x = {all_asset_LM_Vars[i]}, .y = {all_asset_LM_Vars[j]}, .f = ~ cor(.x, .y), .before = {cor_periods[k]} )")
          }

        }
      }
    }

    asset_LM_cor_statements_collapse <-
      asset_LM_cor_statements %>%
      paste(collapse = ",")

    Final_LM_Cor_Statement <-
      glue::glue("reg_dat %>% mutate({asset_LM_cor_statements_collapse})")

    reg_dat_with_cor <- eval(parse(text = Final_LM_Cor_Statement))

    all_asset_Final_Return_Vars <-
      assets_to_use %>%
      map( ~
             glue::glue("{.x}_Final_Return")
      )  %>%
      unlist()

    asset_Final_Return_cor_statements <- c()
    c = 0
    for (i in 1:length(all_asset_Final_Return_Vars)) {
      for (j in 1:length(all_asset_Final_Return_Vars)) {
        for (k in 1:length(cor_periods)) {

          if(all_asset_Final_Return_Vars[i] != all_asset_Final_Return_Vars[j]) {
            c = c + 1
            asset_Final_Return_cor_statements[i] <-
              glue::glue("cor_asset_{cor_periods[k]}_Final_Return_{i}_{j} = slider::slide2_dbl(.x = {all_asset_Final_Return_Vars[i]}, .y = {all_asset_Final_Return_Vars[j]}, .f = ~ cor(.x, .y), .before = {cor_periods[k]})")
          }

        }
      }
    }

    asset_Final_Return_cor_statements_collapse <-
      asset_Final_Return_cor_statements %>%
      paste(collapse = ",") %>%
      as.character()

    Final_Return_Cor_Statement <-
      glue::glue("reg_dat_with_cor %>% mutate({asset_Final_Return_cor_statements_collapse})")

    reg_dat_with_cor <- eval(parse(text = Final_Return_Cor_Statement))

    return(reg_dat_with_cor)

  }

#' create_Portfolio_Model_Data
#'
#' @param portfolio_data
#' @param pred_data
#' @param assets_to_use
#' @param stop_factor_var
#' @param profit_factor_var
#' @param risk_dollar_value_var
#' @param end_period
#' @param time_frame
#' @param trade_direction
#' @param currency_conversion
#' @param asset_infor
#' @param cor_periods
#' @param end_point_profit
#' @param end_point_stop
#' @param training_date_end
#'
#' @returns
#' @export
#'
#' @examples
create_Portfolio_Model_Data <-
  function(
    portfolio_data = Indices_Metals_Bonds %>% map(~ .x %>% filter(Date >= "2023-01-01") ),
    pred_data = all_preds %>% filter(Date >= "2023-01-01"),
    assets_to_use = c("USD_JPY", "EUR_USD", "EUR_JPY",
                      "EUR_GBP", "GBP_USD", "AUD_USD", "EUR_AUD"),
    stop_factor_var = 15,
    profit_factor_var = 200,
    risk_dollar_value_var = 5,
    end_period = 24,
    time_frame = "H1",
    trade_direction = "Long",
    currency_conversion = currency_conversion,
    asset_infor = asset_infor,
    cor_periods = c(50,100,300),
    end_point_profit = c(1,2,5,10,20,50),
    end_point_stop = c(-10,-8,-5,-2),
    training_date_end = "2023-01-01"
  ) {

    portfolio_end_point_dummy <-
      tibble(end_point_profit = end_point_profit )

    portfolio_data_multiple_endpoints <-
      end_point_stop %>%
      map_dfr(
        ~ portfolio_end_point_dummy %>%
          mutate(end_point_loss = .x)
      ) %>%
      mutate(xx = row_number()) %>%
      split(.$xx) %>%
      map_dfr(
        ~
          create_PortFolio_data(
            portfolio_data = portfolio_data,
            pred_data = pred_data,
            assets_to_use = assets_to_use,
            stop_factor_var = stop_factor_var,
            profit_factor_var = profit_factor_var,
            risk_dollar_value_var = risk_dollar_value_var,
            end_period = end_period,
            time_frame = time_frame,
            trade_direction = trade_direction,
            currency_conversion = currency_conversion,
            asset_infor = asset_infor,
            end_point_loss = as.numeric(.x$end_point_loss[1]),
            end_point_profit = as.numeric(.x$end_point_profit[1]),
            cor_periods = cor_periods
          )
      )

    training_set <-
      portfolio_data_multiple_endpoints %>%
      filter(Date <= training_date_end)

    testing_set <-
      portfolio_data_multiple_endpoints %>%
      filter(Date > training_date_end)

    return(
      list("training_set" = training_set,
           "testing_set" = testing_set)
    )

  }


#' create_Portfolio_Model_GLM_LM
#'
#' @param Model_Data
#' @param bin_threshold
#' @param model_save_location
#' @param training_date_end
#' @param portfolio_prefix
#'
#' @returns
#' @export
#'
#' @examples
create_Portfolio_Model_GLM_LM <-
  function(
    Model_Data = Model_Data,
    bin_threshold = 0,
    model_save_location = "C:/Users/nikhi/Documents/trade_data/portfolio_trader_V1",
    training_date_end = "2022-01-01",
    portfolio_prefix = "Currency"
  ) {


    Model_data_with_bin <-
      Model_Data %>%
      pluck("training_set") %>%
      mutate(
        bin_var = ifelse(Final_Return >= bin_threshold, 1, 0)
      ) %>%
      filter(!is.na(Final_Return)) %>%
      filter(Date <= training_date_end)

    reg_vars_asset <-
      names(Model_data_with_bin) %>%
      keep(~ str_detect(.x, "_LM")|str_detect(.x, "_GLM")|str_detect(.x, "[A-Z]_Final_Return")|str_detect(.x, "cor_asset_") ) %>%
      unlist()

    reg_vars_end_points <- c("end_point_loss", "end_point_profit")

    all_reg_vars <-
      c(reg_vars_asset, reg_vars_end_points) %>% unlist()

    reg_formula <-
      create_lm_formula(dependant ="Final_Return" ,
                        independant = all_reg_vars)

    pred_model <-
      lm(data = Model_data_with_bin,
         formula = reg_formula
      )

    # summary(pred_model)
    saveRDS(object = pred_model,
            file = glue::glue("{model_save_location}/portfolio_trader_V1_LM_{portfolio_prefix}.RDS")
    )

    reg_formula_GLM <-
      create_lm_formula(dependant ="bin_var" ,
                        independant = all_reg_vars)

    pred_model_GLM <-
      glm(data = Model_data_with_bin,
          formula = reg_formula_GLM,
          family = binomial("logit")
      )

    saveRDS(object = pred_model_GLM,
            file = glue::glue("{model_save_location}/portfolio_trader_V1_GLM_{portfolio_prefix}.RDS")
    )

    # summary(pred_model_GLM)

  }

#' get_Preds_Portfolio_Model_GLM_LM
#'
#' @param Model_Data
#' @param bin_threshold
#' @param model_save_location
#' @param training_date_end
#' @param portfolio_prefix
#'
#' @returns
#' @export
#'
#' @examples
get_Preds_Portfolio_Model_GLM_LM <-
  function(
    Model_Data = Model_Data,
    bin_threshold = 0,
    model_save_location = "C:/Users/nikhi/Documents/trade_data/portfolio_trader_V1/",
    training_date_end = "2022-01-01",
    portfolio_prefix = "Currency"
  ) {

    testing_data <-
      Model_Data %>%
      pluck("testing_set") %>%
      filter(!is.na(Final_Return)) %>%
      filter(Date >= training_date_end)

    pred_model_LM <-
      readRDS(file = glue::glue("{model_save_location}/portfolio_trader_V1_LM_{portfolio_prefix}.RDS")
      )

    predicted_values_LM <-
      predict.lm(object = pred_model_LM, newdata = testing_data)

    pred_model_GLM <-
      readRDS(file = glue::glue("{model_save_location}/portfolio_trader_V1_GLM_{portfolio_prefix}.RDS")
      )

    predicted_values_GLM <-
      predict.glm(object = pred_model_GLM, newdata = testing_data, type = "response" )

    returned_data <-
      testing_data %>%
      mutate(
        !!as.name(glue::glue("{portfolio_prefix}_portfolio_model_LM")) := pred_model_LM,
        !!as.name(glue::glue("{portfolio_prefix}_portfolio_model_GLM")) := predicted_values_GLM
      )

    return(returned_data)

  }

#' Portfolio_get_all_preds_frm_V3
#'
#' @param Indices_Metals_Bonds
#' @param base_path
#' @param actuals_periods_needed
#' @param state_space_periods
#' @param state_space_rolling
#' @param date_for_true_simualtion
#' @param raw_macro_data
#' @param assets_to_test
#' @param training_end_date
#'
#' @returns
#' @export
#'
#' @examples
Portfolio_get_all_preds_frm_V3 <-
  function(
    Indices_Metals_Bonds = Indices_Metals_Bonds,
    raw_macro_data = raw_macro_data,
    base_path = "C:/Users/nikhi/Documents/trade_data/Day_Trader_Single_Asset_V3_Expanded_Models/",
    actuals_periods_needed = c("period_return_50_Price"),
    state_space_periods = c(20, 40, 60, 100, 200,300, 400,  500),
    state_space_rolling = c(100, 200, 300, 400),
    date_for_true_simualtion = "2019-01-01",
    training_end_date = "2021-01-01",
    assets_to_test
  ) {

    safely_get_probs <-
      safely(Single_Asset_V3_Read_in_Probs_Exclude_Copula, otherwise = NULL)

    all_probs <- list()
    c = 0

    for (j in 1:length(assets_to_test) ) {

      tictoc::tic()
      asset_of_interest <- assets_to_test[j]
      correlation_assets_current <- c("")

      simulated_probs <-
        safely_get_probs(
          Indices_Metals_Bonds =
            Indices_Metals_Bonds %>%
            map(~ .x %>% filter(Date >= date_for_true_simualtion)),
          asset_of_interest = asset_of_interest,
          actuals_periods_needed = actuals_periods_needed,
          training_end_date = training_end_date,
          rolling_mean_pred_period = 500,
          correlation_rolling_periods = c(1,2),
          state_space_periods = state_space_periods,
          state_space_rolling = state_space_rolling,
          copula_assets = correlation_assets_current,
          raw_macro_data = raw_macro_data,
          base_path = base_path
        ) %>%
        pluck('result')

      tictoc::toc()

      if(!is.null(simulated_probs)) {
        c = c + 1
        simulated_probs <-
          simulated_probs %>%
          reduce(bind_rows) %>%
          mutate(
            training_end_date = training_end_date,
            date_for_true_simualtion = date_for_true_simualtion
          )

        all_probs[[c]]  <- simulated_probs

        rm(simulated_probs)

      }
    }

    all_preds_dfr <-
      all_probs %>%
      map_dfr(bind_rows)

    return(all_preds_dfr)

  }

#' portfolio_get_V3_cor_data
#'
#' @param Indices_Metals_Bonds
#' @param all_preds
#' @param portfolio_data
#' @param assets_to_port
#' @param low_to_price_lengths
#' @param cor_periods
#' @param max_regs
#'
#' @returns
#' @export
#'
#' @examples
portfolio_get_V3_cor_data <-
  function(
    Indices_Metals_Bonds = Indices_Metals_Bonds,
    all_preds = all_preds,
    portfolio_data = portfolio_data,
    assets_to_port = assets_to_port,
    low_to_price_lengths = c(200),
    cor_periods = c(200),
    max_regs = 200
  ) {

    AR_LM_pivoted <-
      wide_pivot_asset_data(all_asset_data = all_preds,
                            assets_to_use = assets_to_port,
                            cor_col_to_use =  "AR_LM_Pred_period_return_50_Price") %>%
      ungroup()

    ar_reg_cols <-
      names(AR_LM_pivoted) %>%
      keep(~ .x != "Date")

    state_space_pivoted <-
      wide_pivot_asset_data(all_asset_data = all_preds,
                            assets_to_use = assets_to_port,
                            cor_col_to_use =  "state_space_LM_Pred_period_return_50_Price") %>%
      ungroup()

    ss_reg_cols <-
      names(state_space_pivoted) %>%
      keep(~ .x != "Date")

    all_dat_pivoted <-
      AR_LM_pivoted %>%
      ungroup() %>%
      left_join(state_space_pivoted %>%
                  ungroup() ) %>%
      ungroup()

    correlation_data <-
      get_portfolio_rolling_data(
        asset_data = Indices_Metals_Bonds[[1]],
        asset_of_interest = assets_to_port,
        low_to_price_lengths = low_to_price_lengths,
        cor_periods = cor_periods
      ) %>%
      ungroup()

    cor_reg_cols <-
      names(correlation_data) %>%
      keep(~ .x != "Date" & str_detect(.x, "cor"))

    cor_reg_cols <- cor_reg_cols[1:round((length(cor_reg_cols))/2)]

    diff_reg_cols <-
      names(correlation_data) %>%
      keep(~ .x != "Date" & !str_detect(.x, "cor"))

    total_reg_data <-
      portfolio_data %>%
      ungroup() %>%
      left_join(all_dat_pivoted %>%
                  ungroup() %>%
                  left_join(correlation_data %>%
                              ungroup() )
      ) %>%
      ungroup()

    reg_vars <-
      c("Asset", ss_reg_cols, ar_reg_cols, diff_reg_cols,  cor_reg_cols) %>%
      unlist() %>%
      head(max_regs)

    rm(portfolio_data, all_dat_pivoted,
       correlation_data, AR_LM_pivoted, state_space_pivoted)

    return(list( "total_reg_data" = total_reg_data, "reg_vars" = reg_vars))

  }

#' portfolio_get_V3_cor_data
#'
#' @param Indices_Metals_Bonds
#' @param all_preds
#' @param portfolio_data
#' @param assets_to_port
#' @param low_to_price_lengths
#' @param cor_periods
#' @param max_regs
#'
#' @returns
#' @export
#'
#' @examples
portfolio_get_V3_cor_data_TOTAL_SUMMED <-
  function(
    Indices_Metals_Bonds = Indices_Metals_Bonds,
    all_preds = all_preds,
    assets_to_port = assets_to_port,
    low_to_price_lengths = c(200),
    cor_periods = c(200),
    max_regs = 200
  ) {

    AR_LM_pivoted <-
      wide_pivot_asset_data(all_asset_data = all_preds,
                            assets_to_use = assets_to_port,
                            cor_col_to_use =  "AR_LM_Pred_period_return_50_Price") %>%
      ungroup()

    ar_reg_cols <-
      names(AR_LM_pivoted) %>%
      keep(~ .x != "Date")

    state_space_pivoted <-
      wide_pivot_asset_data(all_asset_data = all_preds,
                            assets_to_use = assets_to_port,
                            cor_col_to_use =  "state_space_LM_Pred_period_return_50_Price") %>%
      ungroup()

    ss_reg_cols <-
      names(state_space_pivoted) %>%
      keep(~ .x != "Date")

    all_dat_pivoted <-
      AR_LM_pivoted %>%
      ungroup() %>%
      left_join(state_space_pivoted %>%
                  ungroup() ) %>%
      ungroup()

    correlation_data <-
      get_portfolio_rolling_data(
        asset_data = Indices_Metals_Bonds[[1]],
        asset_of_interest = assets_to_port,
        low_to_price_lengths = low_to_price_lengths,
        cor_periods = cor_periods
      ) %>%
      ungroup()

    cor_reg_cols <-
      names(correlation_data) %>%
      keep(~ .x != "Date" & str_detect(.x, "cor"))

    cor_reg_cols <- cor_reg_cols[1:round((length(cor_reg_cols))/2)]

    diff_reg_cols <-
      names(correlation_data) %>%
      keep(~ .x != "Date" & !str_detect(.x, "cor"))

    reg_vars <-
      c(ss_reg_cols, ar_reg_cols, diff_reg_cols,  cor_reg_cols) %>%
      unlist() %>%
      head(max_regs)

    rm(AR_LM_pivoted, state_space_pivoted)

    return(list( "all_dat_pivoted" = all_dat_pivoted,
                 "correlation_data" = correlation_data,
                 "reg_vars" = reg_vars))

  }

#' portfolio_get_V3_cor_data_TOTAL_SUMMED_reg_dat
#'
#' @param all_dat_pivoted
#' @param correlation_data
#' @param portfolio_data
#'
#' @returns
#' @export
#'
#' @examples
portfolio_get_V3_cor_data_TOTAL_SUMMED_reg_dat <-
  function(all_dat_pivoted  = all_cor_V3_Data[[1]],
           correlation_data = all_cor_V3_Data[[2]],
           portfolio_data = portfolio_data,
           additional_cor_vars,
           lag_dependant = 24) {

    additional_cor_eval <-
      additional_cor_vars %>%
      map(
        ~ glue::glue("{.x[3]} = slider::slide2_dbl(.x = {.x[1]}, .y = {.x[2]}, .f = ~ cor(.x, .y), .before = 200 )")
      ) %>%
      unlist() %>%
      paste(collapse = ",")

    total_reg_data <-
      portfolio_data %>%
      ungroup() %>%
      group_by(Date, end_point_loss, end_point_profit, stop_factor, profit_factor) %>%
      summarise(Final_Return = sum(Final_Return, na.rm = T)) %>%
      ungroup() %>%
      left_join(all_dat_pivoted %>%
                  ungroup() %>%
                  left_join(correlation_data %>%
                              ungroup() )
      ) %>%
      ungroup() %>%
      arrange(Date) %>%
      mutate(
        Final_Return_lag = lag(Final_Return, 24),
        Final_Return_lag_2 = lag(Final_Return, 25),
        Final_Return_lag_3 = lag(Final_Return, 26),
        Final_Return_lag_4 = lag(Final_Return, 27),

        Final_Return_lag_error =
          Final_Return_lag -
          (XAG_USD_AR_LM_Pred_period_return_50_Price + HK33_HKD_AR_LM_Pred_period_return_50_Price +
             FR40_EUR_AR_LM_Pred_period_return_50_Price + BTC_USD_AR_LM_Pred_period_return_50_Price +
             NATGAS_USD_AR_LM_Pred_period_return_50_Price + JP225Y_JPY_AR_LM_Pred_period_return_50_Price +
             XAU_USD_AR_LM_Pred_period_return_50_Price ),

        Final_Return_lag_error =
          (Final_Return_lag_error +
              lag(Final_Return_lag_error,1) + lag(Final_Return_lag_error,2) + lag(Final_Return_lag_error,3) +
              lag(Final_Return_lag_error,4))/5
        # Final_Return_lag_error_ma_100 =
        #   slider::slide_dbl(.x = Final_Return_lag_error, .f = ~ mean(.x, na.rn = T), .before = 100, .complete = FALSE)
      )

    additional_cor_eval <-
      glue::glue("total_reg_data %>% mutate({additional_cor_eval})")

    total_reg_data <- eval(parse(text = additional_cor_eval))

    return(total_reg_data)

  }

#' Porfolio_get_V3_LM_Model
#'
#' @param all_cor_V3_Data
#' @param reg_vars
#' @param training_end_date
#' @param regression_length
#' @param dependant_var
#' @param sig_thresh_LM
#'
#' @returns
#' @export
#'
#' @examples
Porfolio_get_V3_LM_Model <-
  function(
    all_cor_V3_Data = all_cor_V3_Data[[1]],
    reg_vars = all_cor_V3_Data[[2]],
    training_end_date = "2025-01-01",
    regression_length = 5000,
    dependant_var = "Final_Return",
    sig_thresh_LM = 0.1
  ) {

    Dates <-
      all_cor_V3_Data %>%
      ungroup() %>%
      filter(Date <= training_end_date) %>%
      pull(Date) %>%
      unique() %>%
      tail(regression_length) %>%
      min(na.rm = T)

    gc()

    training_data <-
      all_cor_V3_Data %>%
      ungroup() %>%
      filter(Date <= training_end_date) %>%
      filter(Date >= Dates )

    testing_data <-
      all_cor_V3_Data %>%
      ungroup() %>%
      filter(Date > training_end_date)

    rm(Dates)
    # gc()

    lm_form <-
      create_lm_formula(dependant = dependant_var, independant = reg_vars)

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

    lm_form <-
      create_lm_formula(dependant = dependant_var,
                        independant = sig_coefs)

    LM_model <- lm(formula = lm_form, data = training_data)

    predicted <- predict.lm(newdata = testing_data, object =  LM_model)
    predicted_train <- predict.lm(newdata = training_data, object =  LM_model)

    means_by_asset <-
      training_data %>%
      mutate(preds = predicted_train) %>%
      group_by(Asset) %>%
      summarise(
        trained_mean = mean(preds, na.rm = T),
        trained_sd = sd(preds, na.rm = T)
      ) %>%
      ungroup() %>%
      dplyr::select(
        Asset,
        trained_mean, trained_sd)

    returned_data <-
      testing_data %>%
      mutate(
        predicted = predicted
      ) %>%
      left_join(means_by_asset) %>%
      # mutate(
      #   trained_mean = means_by_asset$trained_mean[1],
      #   trained_sd = means_by_asset$trained_sd[1]
      # ) %>%
      dplyr::select(Date,
                    Asset,
                    Final_Return, predicted, trained_mean, trained_sd)

    rm(testing_data, training_data, all_cor_V3_Data, LM_model, predicted_train,predicted )
    gc()

    return(returned_data)
  }

#' Porfolio_get_V3_LM_Model
#'
#' @param all_cor_V3_Data
#' @param reg_vars
#' @param training_end_date
#' @param regression_length
#' @param dependant_var
#' @param sig_thresh_LM
#'
#' @returns
#' @export
#'
#' @examples
Porfolio_get_V3_LM_Model_TOTAL_SUM <-
  function(
    reg_dat = all_cor_V3_Data[[1]],
    reg_vars = all_cor_V3_Data[[2]],
    training_end_date = "2025-01-01",
    regression_length = 5000,
    dependant_var = "Final_Return",
    sig_thresh_LM = 0.1,
    taking_trade = FALSE
  ) {

    Dates <-
      reg_dat %>%
      ungroup() %>%
      filter(Date <= training_end_date) %>%
      pull(Date) %>%
      unique() %>%
      tail(regression_length) %>%
      min(na.rm = T)

    gc()

    training_data <-
      reg_dat %>%
      ungroup() %>%
      filter(Date <= training_end_date) %>%
      filter(Date >= Dates )

    if(taking_trade == TRUE) {

      testing_data <-
        reg_dat %>%
        ungroup() %>%
        filter(Date >= training_end_date)

    } else {
      testing_data <-
        reg_dat %>%
        ungroup() %>%
        filter(Date > training_end_date)
    }

    rm(Dates)
    # gc()

    lm_form <-
      create_lm_formula(dependant = dependant_var, independant = reg_vars)

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

    # sig_coefs <-
    #   c("Asset", sig_coefs) %>%
    #   unlist()

    sig_coefs <-
      c(sig_coefs) %>%
      unlist()

    lm_form <-
      create_lm_formula(dependant = dependant_var,
                        independant = sig_coefs)

    LM_model <- lm(formula = lm_form, data = training_data)

    predicted <- predict.lm(newdata = testing_data, object =  LM_model)
    predicted_train <- predict.lm(newdata = training_data, object =  LM_model)

    means_by_asset <-
      training_data %>%
      mutate(preds = predicted_train) %>%
      # group_by(Asset) %>%
      summarise(
        trained_mean = mean(preds, na.rm = T),
        trained_sd = sd(preds, na.rm = T)
      ) %>%
      ungroup() %>%
      dplyr::select(
        # Asset,
        trained_mean, trained_sd)

    returned_data <-
      testing_data %>%
      mutate(
        predicted = predicted
      ) %>%
      # left_join(means_by_asset) %>%
      mutate(
        trained_mean = means_by_asset$trained_mean[1],
        trained_sd = means_by_asset$trained_sd[1]
      ) %>%
      dplyr::select(Date,
                    # Asset,
                    Final_Return, predicted, trained_mean, trained_sd)

    rm(testing_data, training_data, all_cor_V3_Data, LM_model, predicted_train,predicted )
    gc()

    return(returned_data)
  }

#' Portfolio_get_V3_Cor_DIFF_Preds
#'
#' @param asset_data
#' @param asset_of_interest
#' @param stop_factor_var
#' @param profit_factor_var
#' @param risk_dollar_value_var
#' @param end_period
#' @param time_frame
#' @param trade_direction
#' @param currency_conversion
#' @param asset_infor
#' @param end_point_loss
#' @param end_point_profit
#' @param sum_as_portfolio
#' @param low_to_price_lengths
#' @param cor_periods
#' @param max_regs
#' @param training_end_date
#' @param dependant_var
#' @param sig_thresh_LM
#'
#' @returns
#' @export
#'
#' @examples
Portfolio_get_V3_Cor_DIFF_Preds <-
  function(
    all_preds = all_preds,
    asset_data = Indices_Metals_Bonds,
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
    sum_as_portfolio = TRUE,
    low_to_price_lengths = c(200),
    cor_periods = c(200),
    max_regs = 1000,
    training_end_date = as.character(now()),
    dependant_var = "Final_Return",
    sig_thresh_LM = 0.1
  ) {

    portfolio_data <-
      get_portfolio_model_fast_summed(
        asset_data = Indices_Metals_Bonds,
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

    all_cor_V3_Data <-
      portfolio_get_V3_cor_data_TOTAL_SUMMED(
        Indices_Metals_Bonds = Indices_Metals_Bonds,
        all_preds = all_preds,
        portfolio_data = portfolio_data,
        assets_to_port = assets_to_port,
        low_to_price_lengths = low_to_price_lengths,
        cor_periods = cor_periods,
        max_regs = max_regs
      )

    results_temp <-
      Porfolio_get_V3_LM_Model_TOTAL_SUM(
        all_cor_V3_Data = all_cor_V3_Data[[1]],
        reg_vars = all_cor_V3_Data[[2]],
        training_end_date = training_end_date ,
        regression_length = 10000,
        dependant_var = dependant_var,
        sig_thresh_LM = sig_thresh_LM,
        taking_trade = TRUE
      ) %>%
      dplyr::select(Date,
                    Final_Return,
                    predicted_10000 = predicted,
                    trained_mean_10000 = trained_mean,
                    trained_sd_10000 = trained_sd)

    results_temp2 <-
      Porfolio_get_V3_LM_Model_TOTAL_SUM(
        all_cor_V3_Data = all_cor_V3_Data[[1]],
        reg_vars = all_cor_V3_Data[[2]],
        training_end_date = training_end_date,
        regression_length = 5000,
        dependant_var = dependant_var,
        sig_thresh_LM = sig_thresh_LM,
        taking_trade = TRUE
      ) %>%
      dplyr::select(Date,
                    Final_Return,
                    predicted_5000 = predicted,
                    trained_mean_5000 = trained_mean,
                    trained_sd_5000 = trained_sd)

    results_temp <-
      results_temp %>%
      left_join(results_temp2)

    return(results_temp)

  }


#' portfolio_reg_only_preds
#'
#' @param Indices_Metals_Bonds
#' @param assets_to_port
#' @param low_to_price_lengths
#' @param cor_periods
#' @param stop_factor_var
#' @param profit_factor_var
#' @param risk_dollar_value_var
#' @param end_period
#' @param trade_direction
#' @param end_point_loss
#' @param end_point_profit
#' @param sig_thresh_LM
#' @param date_filter_train
#'
#' @returns
#' @export
#'
#' @examples
portfolio_reg_only_preds <-
  function(
    Indices_Metals_Bonds = Indices_Metals_Bonds,
    assets_to_port = assets_to_port,
    low_to_price_lengths = c(100,200),
    cor_periods = c(200),
    stop_factor_var =4,
    profit_factor_var =8,
    risk_dollar_value_var = 5,
    end_period = 24,
    trade_direction = "Long",
    end_point_loss = -2.5,
    end_point_profit = 5,
    sig_thresh_LM = 1,
    date_filter_train = now(tzone = "Australia/Canberra") + hours(10)
  ) {

    portfolio_data <-
      get_portfolio_model_fast_summed(
        asset_data = Indices_Metals_Bonds,
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

    correlation_data <-
      get_portfolio_rolling_data(
        asset_data = Indices_Metals_Bonds[[1]],
        asset_of_interest = assets_to_port,
        low_to_price_lengths = low_to_price_lengths,
        cor_periods = cor_periods
      )

    results_temp <-
      generate_portfolio_LM(
        cor_high_diff_data = correlation_data,
        regression_length = 10000,
        portfolio_actuals_data = portfolio_data,
        dependant_var = "Final_Return",
        date_filter_train = date_filter_train,
        sig_thresh_LM = sig_thresh_LM,
        padding_value = 0
      ) %>%
      dplyr::select(Date, Asset,
                    Final_Return,
                    predicted_10000 = predicted,
                    trained_mean_10000 = trained_mean,
                    trained_sd_10000 = trained_sd)

    results_temp2 <-
      generate_portfolio_LM(
        cor_high_diff_data = correlation_data,
        regression_length = 5000,
        portfolio_actuals_data = portfolio_data,
        dependant_var = "Final_Return",
        date_filter_train = date_filter_train,
        sig_thresh_LM = sig_thresh_LM,
        padding_value = 0
      ) %>%
      dplyr::select(Date, Asset,
                    predicted_5000 = predicted,
                    trained_mean_5000 = trained_mean,
                    trained_sd_5000 = trained_sd)

    results_temp3 <-
      generate_portfolio_LM(
        cor_high_diff_data = correlation_data,
        regression_length = 2500,
        portfolio_actuals_data = portfolio_data,
        dependant_var = "Final_Return",
        date_filter_train = date_filter_train,
        sig_thresh_LM = sig_thresh_LM,
        padding_value = 0
      ) %>%
      dplyr::select(Date, Asset,
                    predicted_2500 = predicted,
                    trained_mean_2500 = trained_mean,
                    trained_sd_2500 = trained_sd)

    results_temp <-
      results_temp %>%
      left_join(results_temp2) %>%
      left_join(results_temp3) %>%
      mutate(
        Averaged_Pred =
          (predicted_10000 + predicted_5000 + predicted_2500)/3
      ) %>%
      ungroup() %>%
      group_by(Date) %>%
      mutate(
        portfolio_pred_10000 = sum(predicted_10000, na.rm = T),
        portfolio_pred_5000 = sum(predicted_5000, na.rm = T),
        portfolio_pred_2500 = sum(predicted_2500, na.rm = T)
      ) %>%
      ungroup() %>%
      group_by(Asset) %>%
      arrange(Date, .by_group = TRUE) %>%
      group_by(Asset) %>%
      mutate(
        portfolio_pred_10000_mean_roll_250 =
          slider::slide_dbl(.x  = portfolio_pred_10000, .f = ~ mean(.x, na.rm = T), .before = 250),
        portfolio_pred_5000_mean_roll_250 =
          slider::slide_dbl(.x  = portfolio_pred_5000, .f = ~ mean(.x, na.rm = T), .before = 250),
        portfolio_pred_2500_mean_roll_250 =
          slider::slide_dbl(.x  = portfolio_pred_2500, .f = ~ mean(.x, na.rm = T), .before = 250),

        portfolio_pred_10000_sd_roll_250 =
          slider::slide_dbl(.x  = portfolio_pred_10000, .f = ~ sd(.x, na.rm = T), .before = 250),
        portfolio_pred_5000_sd_roll_250 =
          slider::slide_dbl(.x  = portfolio_pred_5000, .f = ~ sd(.x, na.rm = T), .before = 250),
        portfolio_pred_2500_sd_roll_250 =
          slider::slide_dbl(.x  = portfolio_pred_2500, .f = ~ sd(.x, na.rm = T), .before = 250)

      ) %>%
      ungroup()

    return(results_temp)

  }

#' generate_portfolio_LM
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
generate_portfolio_LM_with_Errors <-
  function(
    cor_high_diff_data = cor_dat_X,
    regression_length = 10000,
    portfolio_actuals_data = portfolio_data,
    dependant_var = "Final_Return",
    date_filter_train = "2025-01-01",
    sig_thresh_LM = 10^-7,
    padding_value = 24,
    lag_value_error = 24
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
      seq(lag_value_error, lag_value_error + 30,1) %>%
      map(
        ~ glue::glue("Lagged_Final_Return_{.x} = lag(Final_Return, {.x}), Lagged_Final_Return_{.x} = lag(Final_Return, {.x})^2")
      ) %>%
      unlist() %>%
      paste(collapse = ",")

    lagged_returns_statement <-
      glue::glue("reg_dat %>% group_by(Asset) %>% arrange(Date, .by_group = TRUE) %>% group_by(Asset) %>% mutate({lagged_returns_x}) %>% ungroup()")
    reg_dat <-
      portfolio_actuals_data %>%
      ungroup() %>%
      left_join(reg_dat) %>%
      filter(if_all(everything(), ~ !is.na(.)))

    reg_dat <- eval(parse(text = lagged_returns_statement))

    lagged_return_cols <-
      names(reg_dat) %>%
      keep(~ str_detect(.x, "Lagged_Final_Return_")) %>%
      unlist()

    reg_vars_additional <-
      c("Asset", reg_vars, lagged_return_cols) %>% unique()

    training_data <-
      reg_dat %>%
      filter(Date <= date_filter_train) %>%
      group_by(Asset) %>%
      slice_tail(n = regression_length) %>%
      ungroup()

    testing_data <-
      reg_dat %>%
      filter(Date > (date_filter_train + hours(padding_value)) )

    lm_form <-
      create_lm_formula(dependant = dependant_var, independant = reg_vars_additional)

    LM_model <- lm(data = training_data, formula = lm_form)

    sig_coefs <- get_sig_coefs(LM_model, p_value_thresh_for_inputs = sig_thresh_LM)

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
      keep(~ !str_detect(.x, "Asset[A-Z][A-Z]")) %>%
      unlist()

    sig_coefs <-
      c("Asset", sig_coefs)

    lm_form <-
      create_lm_formula(dependant = dependant_var,
                        independant = sig_coefs)

    LM_model <- lm(formula = lm_form, data = training_data)

    predicted <- predict.lm(newdata = testing_data, object =  LM_model)

    means_by_asset <-
      training_data %>%
      mutate(preds = LM_model$fitted.values) %>%
      group_by(Asset) %>%
      summarise(
        trained_mean = mean(preds, na.rm = T),
        trained_sd = sd(preds, na.rm = T)
      ) %>%
      ungroup() %>%
      dplyr::select(Asset, trained_mean, trained_sd)

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
      left_join(brownian_tech_data_50_24)

   rm(state_space_data, technical_data_final_return, technical_data_period_24, period_lag_cols,
      technical_data_period_4, technical_data_period_8,
      technical_data_period_4, technical_data_period_12,
      state_space_data_24, state_space_data_8, brownian_tech_data_24,
      brownian_tech_data_1, brownian_tech_data_24_200, brownian_tech_data_1_20,
      brownian_tech_data_1_60, brownian_tech_data_1_40, brownian_tech_data_10, brownian_tech_data_50_200,
      state_space_data_50, state_space_data_12, state_space_data_4, brownian_tech_data_50_100,
      state_space_data_6, brownian_tech_data_50_50,
      brownian_tech_data_50_24, state_space_data_10)
   gc()

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
