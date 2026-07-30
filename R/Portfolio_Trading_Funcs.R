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
            glue::glue("brownian_percentile_95_position_{.x}_{brownian_period} = brownian_col_check_{.x}_{brownian_period}/brownian_percentile_95_{.x}_{brownian_period}"),

            #Delete if too heavy
            glue::glue("brownian_col_cumul_mean_{.x}_{brownian_period} = slider::slide_dbl(.x = brownian_col_check_{.x}_{brownian_period}, .f = ~ mean(.x, na.rm = T) , .before = {brownian_period})"),
            glue::glue("brownian_col_cumul_sd_{.x}_{brownian_period} = slider::slide_dbl(.x = brownian_col_check_{.x}_{brownian_period}, .f = ~ sd(.x, na.rm = T) , .before = {brownian_period})"),
            glue::glue("brownian_cumul_percentile_95_{.x}_{brownian_period} = lag(brownian_col_cumul_mean_{.x}_{brownian_period}, {brownian_period} ) + 1.96*lag(brownian_col_cumul_sd_{.x}_{brownian_period}, {brownian_period})*{brownian_period}"),
            glue::glue("brownian_percentile_cumul_95_position_{.x}_{brownian_period} = brownian_col_check_{.x}_{brownian_period}/brownian_cumul_percentile_95_{.x}_{brownian_period}")
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
        assets_to_use  = asset_of_interest,
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
        assets_to_use  = asset_of_interest,
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
          assets_to_use  = asset_of_interest,
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

#' portfolio_get_V3_cor_data_reg_dat
#'
#' @param all_dat_pivoted
#' @param correlation_data
#' @param portfolio_data
#'
#' @returns
#' @export
#'
#' @examples
portfolio_get_V3_cor_data_reg_dat <-
  function(all_dat_pivoted  = all_cor_V3_Data[[1]],
           correlation_data = all_cor_V3_Data[[2]],
           portfolio_data = portfolio_data,
           additional_cor_vars,
           lag_dependant = 24,
           total_lag_cols = 25,
           error_calc_cols =
             c("XAG_USD_AR_LM_Pred_period_return_50_Price",  "HK33_HKD_AR_LM_Pred_period_return_50_Price",
               "FR40_EUR_AR_LM_Pred_period_return_50_Price", "BTC_USD_AR_LM_Pred_period_return_50_Price",
               "NATGAS_USD_AR_LM_Pred_period_return_50_Price", "JP225Y_JPY_AR_LM_Pred_period_return_50_Price",
               "XAU_USD_AR_LM_Pred_period_return_50_Price" ) ) {


    error_calc_eval <- paste(error_calc_cols, collapse = " + ")
    error_calc_eval2 <- glue::glue("Final_Return_lag_1 - ({error_calc_eval})")

    additional_cor_eval <-
      additional_cor_vars %>%
      map(
        ~ glue::glue("{.x[3]} = slider::slide2_dbl(.x = {.x[1]}, .y = {.x[2]}, .f = ~ cor(.x, .y), .before = 200 )")
      ) %>%
      unlist() %>%
      paste(collapse = ",")

    error_mean_cols <-
      seq(1,11) %>%
      map( ~ glue::glue( "lag(Final_Return_lag_error, {.x})" )) %>%
      unlist() %>%
      as.character() %>%
      paste(collapse = " + ")

    error_mean_cols_2 <-
      seq(1,21) %>%
      map( ~ glue::glue( "lag(Final_Return_lag_error, {.x})" )) %>%
      unlist() %>%
      as.character() %>%
      paste(collapse = " + ")

    error_mean_cols_3 <-
      seq(1,41) %>%
      map( ~ glue::glue( "lag(Final_Return_lag_error, {.x})" )) %>%
      unlist() %>%
      as.character() %>%
      paste(collapse = " + ")

    error_mean_cols <- glue::glue("({error_mean_cols})/10")
    error_mean_cols_2 <- glue::glue("({error_mean_cols})/20")
    error_mean_cols_3 <- glue::glue("({error_mean_cols})/40")

    additional_cor_eval <-
      glue::glue("all_dat_pivoted %>% mutate({additional_cor_eval})")

    all_dat_pivoted_cor <- eval(parse(text = additional_cor_eval))

    final_lag_cols <-
      seq(1,total_lag_cols) %>%
      map(~glue::glue("Final_Return_lag_{.x} = lag(Final_Return, {lag_dependant} + {.x}), Final_Return_lag_{.x}_sq = Final_Return_lag_{.x}^2")) %>%
      unlist() %>%
      paste(collapse = ",")

    final_lag_cols <-
      glue::glue("
      total_reg_data %>%
                ungroup() %>%
                group_by(Asset) %>%
                arrange(Date, .by_group = TRUE) %>%
                group_by(Asset) %>%
                mutate({final_lag_cols})")

    total_reg_data <-
      portfolio_data %>%
      ungroup() %>%
      group_by(Date, Asset ,end_point_loss, end_point_profit, stop_factor, profit_factor) %>%
      summarise(Final_Return = sum(Final_Return, na.rm = T)) %>%
      ungroup() %>%
      left_join(all_dat_pivoted_cor %>%
                  ungroup() %>%
                  left_join(correlation_data %>%
                              ungroup() )
      )

    total_reg_data <- eval(parse(text = final_lag_cols))

    total_reg_data <-
      total_reg_data %>%
      group_by(Asset)  %>%
      mutate(
        Final_Return_lag_error =  eval(parse(text = error_calc_eval2)),
        Final_Return_lag_error_mean = eval(parse(text = error_mean_cols)),
        Final_Return_lag_error_mean2 = eval(parse(text = error_mean_cols_2)),
        Final_Return_lag_error_mean3 = eval(parse(text = error_mean_cols_3)),

        Final_Return_lag_error_mean_sq = Final_Return_lag_error_mean^2,
        Final_Return_lag_error_mean2_sq = Final_Return_lag_error_mean2^2,
        Final_Return_lag_error_mean3_sq = Final_Return_lag_error_mean3^2
      ) %>%
      ungroup() %>%
      dplyr::select(-Final_Return_lag_error)

    rm(portfolio_data, all_dat_pivoted_cor, correlation_data, all_dat_pivoted)
    gc()

    return(total_reg_data)

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
           lag_dependant = 24,
           total_lag_cols = 25,
           direct_return_cols = 10,
           error_calc_cols =
             c("XAG_USD_AR_LM_Pred_period_return_50_Price",  "HK33_HKD_AR_LM_Pred_period_return_50_Price",
               "FR40_EUR_AR_LM_Pred_period_return_50_Price", "BTC_USD_AR_LM_Pred_period_return_50_Price",
               "NATGAS_USD_AR_LM_Pred_period_return_50_Price", "JP225Y_JPY_AR_LM_Pred_period_return_50_Price",
               "XAU_USD_AR_LM_Pred_period_return_50_Price" ),
           xtnd_vars = FALSE,
           xtnd_ss_cols_PR_cols = 25,
           xtnd_ss_cols_BR_periods = c(100,200,300) ) {


    error_calc_eval <- paste(error_calc_cols, collapse = " + ")
    error_calc_eval2 <- glue::glue("Final_Return_lag_1 - ({error_calc_eval})")

    additional_cor_eval <-
      additional_cor_vars %>%
      map(
        ~ glue::glue("{.x[3]} = slider::slide2_dbl(.x = {.x[1]}, .y = {.x[2]}, .f = ~ cor(.x, .y), .before = 200 )")
      ) %>%
      unlist() %>%
      paste(collapse = ",")

    error_mean_cols <-
      seq(1,11) %>%
      map( ~ glue::glue( "lag(Final_Return_lag_error, {.x})" )) %>%
      unlist() %>%
      as.character() %>%
      paste(collapse = " + ")

    error_mean_cols_2 <-
      seq(1,21) %>%
      map( ~ glue::glue( "lag(Final_Return_lag_error, {.x})" )) %>%
      unlist() %>%
      as.character() %>%
      paste(collapse = " + ")

    error_mean_cols_3 <-
      seq(1,41) %>%
      map( ~ glue::glue( "lag(Final_Return_lag_error, {.x})" )) %>%
      unlist() %>%
      as.character() %>%
      paste(collapse = " + ")

    error_mean_cols <- glue::glue("({error_mean_cols})/10")
    error_mean_cols_2 <- glue::glue("({error_mean_cols})/20")
    error_mean_cols_3 <- glue::glue("({error_mean_cols})/40")

    additional_cor_eval <-
      glue::glue("all_dat_pivoted %>% mutate({additional_cor_eval})")

    all_dat_pivoted_cor <- eval(parse(text = additional_cor_eval))

    final_lag_cols <-
      seq(1,total_lag_cols) %>%
      map(~glue::glue("Final_Return_lag_{.x} = lag(Final_Return, {lag_dependant} + {.x}), Final_Return_lag_{.x}_sq = Final_Return_lag_{.x}^2")) %>%
      unlist() %>%
      paste(collapse = ",")

    final_lag_cols <-
      glue::glue("
      total_reg_data %>%
                ungroup() %>%
                arrange(Date, .by_group = TRUE) %>%
                mutate({final_lag_cols})")

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
                 arrange(Date) %>%
                 mutate({needed_direct_lag_cols})")

    period_lag_cols <-
      portfolio_data %>%
      arrange(Date) %>%
      ungroup() %>%
      dplyr::select(Date, matches(needed_direct_period_cols))

    period_lag_cols <- eval(parse(text = needed_direct_lag_cols)) %>%
      dplyr::select(Date, contains("Period_Return_Lag_"))

    total_reg_data <-
      portfolio_data %>%
      ungroup() %>%
      group_by(Date,end_point_loss, end_point_profit, stop_factor, profit_factor) %>%
      summarise(Final_Return = sum(Final_Return, na.rm = T)) %>%
      ungroup() %>%
      left_join(all_dat_pivoted_cor %>%
                  ungroup() %>%
                  left_join(correlation_data %>%
                              ungroup() )
      ) %>%
      left_join(period_lag_cols)

    total_reg_data <- eval(parse(text = final_lag_cols))

    total_reg_data <-
      total_reg_data %>%
      mutate(
        Final_Return_lag_error =  eval(parse(text = error_calc_eval2)),
        Final_Return_lag_error_mean = eval(parse(text = error_mean_cols)),
        Final_Return_lag_error_mean2 = eval(parse(text = error_mean_cols_2)),
        Final_Return_lag_error_mean3 = eval(parse(text = error_mean_cols_3)),

        Final_Return_lag_error_mean_sq = Final_Return_lag_error_mean^2,
        Final_Return_lag_error_mean2_sq = Final_Return_lag_error_mean2^2,
        Final_Return_lag_error_mean3_sq = Final_Return_lag_error_mean3^2
      ) %>%
      ungroup() %>%
      dplyr::select(-Final_Return_lag_error)

    if(xtnd_vars == TRUE) {

      req_ss_extnd_cols_PR <-
        seq(1,xtnd_ss_cols_PR_cols,1) %>%
        map(
          ~
            glue::glue(
              "
          state_space_data_PR_{.x} <-
              portfolio_LM_state_space(
                  portfolio_data = portfolio_data %>%
                    mutate(Asset = 'Portfolio'),
                  state_space_col = 'period_return_{.x}_Price' ,
                  required_lag = {.x}, #Does not need a plus 1 its built in
                  roll_period_state_space = 500
                )"
            )
        )  %>%
        unlist() %>%
        as.character() %>%
        paste(collapse = "\n")

      req_ss_extnd_cols_PR_names <-
        seq(1,xtnd_ss_cols_PR_cols,1) %>%
        map(
          ~ glue::glue("state_space_data_PR_{.x}")
        ) %>%
        unlist() %>%
        as.character() %>%
        paste(collapse = ",")

      req_ss_extnd_cols_PR_list <-
        glue::glue("All_state_space_data_PR <- list({req_ss_extnd_cols_PR_names})")

      rm_statement <- glue::glue("rm({req_ss_extnd_cols_PR_names})")

      eval(parse(text = req_ss_extnd_cols_PR))
      eval(parse(text = req_ss_extnd_cols_PR_list))
      eval(parse(text = rm_statement))
      gc()

      All_state_space_data_PR <-
        All_state_space_data_PR %>%
        reduce(left_join)

      All_state_space_data_PR <-
        All_state_space_data_PR %>%
        dplyr::select(Date, contains("state_space"))

      state_space_data_FR_500 <-
        portfolio_LM_state_space(
          portfolio_data = total_reg_data %>%
            mutate(Asset = "Portfolio"),
          state_space_col = "Final_Return",
          required_lag = lag_dependant, #Does not need a plus 1 its built in
          roll_period_state_space = 500
        )

      All_state_space_data_PR <-
        All_state_space_data_PR %>%
        left_join(
          state_space_data_FR_500 %>%
            dplyr::select(Date, contains("state_space"))
          )

      total_reg_data <-
        total_reg_data %>%
        left_join(All_state_space_data_PR)

      req_BR_extnd_cols_PR <-
        xtnd_ss_cols_BR_periods %>%
        map(
          ~ glue::glue("
          brownian_tech_data_{.x} <-
                portfolio_LM_brownian_checks(portfolio_data = portfolio_data %>%
                                             mutate(Asset = 'Portfolio'),
                                             brownian_period = {.x},
                                             col_to_use = 'period_return_1_Price',
                                             lag_period_to_use = 1)
          brownian_tech_data_FR_{.x} <-
                portfolio_LM_brownian_checks(portfolio_data = portfolio_data %>%
                                             mutate(Asset = 'Portfolio'),
                                             brownian_period = {.x},
                                             col_to_use = 'Final_Return',
                                             lag_period_to_use = {lag_dependant} )"
                       )
        ) %>%
        unlist() %>%
        as.character() %>%
        paste(collapse = "\n")

      req_BR_extnd_cols_PR_names <-
        xtnd_ss_cols_BR_periods %>%
        map(
          ~ glue::glue("brownian_tech_data_{.x}, brownian_tech_data_FR_{.x}")
        ) %>%
        unlist() %>%
        as.character() %>%
        paste(collapse = ",")

      req_BR_extnd_cols_PR_list <-
        glue::glue("All_BR_data_PR <- list({req_BR_extnd_cols_PR_names})")

      rm_statement <- glue::glue("rm({req_BR_extnd_cols_PR_names})")

      eval(parse(text = req_BR_extnd_cols_PR))
      eval(parse(text = req_BR_extnd_cols_PR_list))
      eval(parse(text = rm_statement))
      gc()

      All_BR_data_PR <-
        All_BR_data_PR %>%
        reduce(left_join)

      total_reg_data <-
        total_reg_data %>%
        left_join(All_BR_data_PR %>%
                    dplyr::select(Date, contains("brownian"))
        )

      rm(All_state_space_data_PR, All_BR_data_PR)
      gc()
    }

    rm(portfolio_data, all_dat_pivoted_cor, correlation_data, all_dat_pivoted)
    gc()

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

    LM_model <- lm(data = training_data %>% filter(Final_Return != 0),
                   formula = lm_form)

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

    # sig_coefs <-
    #   c(sig_coefs) %>%
    #   unlist()

    lm_form <-
      create_lm_formula(dependant = dependant_var,
                        independant = sig_coefs)

    LM_model <- lm(formula = lm_form,
                   data = training_data %>% filter(Final_Return != 0))

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

    LM_model <- lm(data = training_data %>% filter(Final_Return != 0),
                   formula = lm_form)

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
      c(sig_coefs) %>%
      unlist()

    lm_form <-
      create_lm_formula(dependant = dependant_var,
                        independant = sig_coefs)

    LM_model <- lm(formula = lm_form,
                   data = training_data %>% filter(Final_Return != 0))

    predicted <- predict.lm(newdata = testing_data, object =  LM_model)
    predicted_train <- predict.lm(newdata = training_data, object =  LM_model)

    means_by_asset <-
      training_data %>%
      mutate(preds = predicted_train) %>%
      summarise(
        trained_mean = mean(preds, na.rm = T),
        trained_sd = sd(preds, na.rm = T)
      ) %>%
      ungroup() %>%
      dplyr::select(trained_mean, trained_sd)

    returned_data <-
      testing_data %>%
      mutate(
        predicted = predicted
      ) %>%
      mutate(
        trained_mean = means_by_asset$trained_mean[1],
        trained_sd = means_by_asset$trained_sd[1]
      ) %>%
      dplyr::select(Date, Final_Return, predicted, trained_mean, trained_sd)

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
Porfolio_generate_V3_TOTAL_SUM_Bayes <-
  function(
    reg_dat = all_cor_V3_Data[[1]],
    reg_vars = all_cor_V3_Data[[2]],
    training_end_date = "2025-01-01",
    regression_length = 5000,
    dependant_var = "Final_Return",
    save_path = "C:/Users/nikhi/Documents/trade_data/single_asset_v3_Bayes_Reg_Portfolio/",
    file_name = "Equity_Port_V3_Bayes_Mean_SD",
    Bayes_or_LM = "Bayes",
    sig_thresh_LM = 0.1
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

    rm(Dates)

    lm_form <-
      create_lm_formula(dependant = dependant_var, independant = reg_vars)

    LM_model <- lm(data = training_data %>% filter(Final_Return != 0),
                   formula = lm_form)

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
      c(sig_coefs) %>%
      unlist()

    lm_form <-
      create_lm_formula(dependant = dependant_var,
                        independant = sig_coefs)

    if(Bayes_or_LM == "Bayes") {
      LM_model <- bayesreg::bayesreg(formula = lm_form,
                                     data = training_data %>% filter(Final_Return != 0),
                                     model = "normal")
    }

    if(Bayes_or_LM == "LM") {

      LM_model <- lm(formula = lm_form,
                     data = training_data %>% filter(Final_Return != 0))
    }

    training_data <-
      training_data %>%
      filter(if_all(everything() ,~ !is.na(.)))

    predicted_train <- predict(newdata = training_data,
                               object =  LM_model, type = "response")

    means_by_asset <-
      training_data %>%
      mutate(preds = predicted_train) %>%
      summarise(
        trained_mean = mean(preds, na.rm = T),
        trained_sd = sd(preds, na.rm = T)
      ) %>%
      ungroup() %>%
      dplyr::select(trained_mean, trained_sd)

    saveRDS(LM_model,
            file = glue::glue("{save_path}/{file_name}.RDS") )

    write.csv(
      means_by_asset,
      file = glue::glue("{save_path}/{file_name}_Mean_SD.csv"),
      row.names = FALSE
    )

    rm(testing_data, training_data, all_cor_V3_Data, LM_model, predicted_train,predicted )
    gc()

    return(NULL)
  }

#' Porfolio_get_preds_V3_TOTAL_SUM_Bayes
#'
#' @param reg_dat
#' @param training_end_date
#' @param save_path
#'
#' @returns
#' @export
#'
#' @examples
Porfolio_get_preds_V3_TOTAL_SUM_Bayes <-
  function(
    reg_dat = all_cor_V3_Data[[1]],
    training_end_date = "2025-01-01",
    save_path = "C:/Users/nikhi/Documents/trade_data/single_asset_v3_Bayes_Reg_Portfolio/",
    file_name = "Equity_Port_V3_Bayes_",
    regression_length = 100000
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

    testing_data <-
      reg_dat %>%
      ungroup() %>%
      filter(Date >= training_end_date)

    rm(Dates)

    LM_model <- readRDS(glue::glue("{save_path}/{file_name}.RDS"))

    predicted <-
      predict(newdata = testing_data, object =  LM_model, type = "response") %>%
      as_tibble() %>%
      pull(1) %>%
      as.numeric()

    predicted_train <- predict(newdata = training_data, object =  LM_model)

    means_by_asset <-
      read_csv(file = glue::glue("{save_path}/{file_name}_Mean_SD.csv"))

    returned_data <-
      testing_data %>%
      mutate(
        predicted = predicted
      ) %>%
      mutate(
        trained_mean = means_by_asset$trained_mean[1],
        trained_sd = means_by_asset$trained_sd[1]
      ) %>%
      dplyr::select(Date, Final_Return, predicted, trained_mean, trained_sd)

    rm(testing_data, training_data, all_cor_V3_Data, LM_model, predicted_train,predicted )
    gc()

    return(returned_data)

  }

#' Title
#'
#' @param Indices_Metals_Bonds
#' @param all_preds
#' @param assets_to_port
#' @param assets_to_trade
#' @param stop_factor_var
#' @param profit_factor_var
#' @param risk_dollar_value_var
#' @param end_period
#' @param trade_direction
#' @param end_point_loss
#' @param end_point_profit
#' @param training_date
#' @param low_to_price_lengths
#' @param cor_periods
#' @param max_regs
#' @param save_path
#' @param regression_length
#' @param total_lag_cols
#'
#' @returns
#' @export
#'
#' @examples
portfolio_V3_TOTAL_SUM_get_preds_algo <-
  function(
    Indices_Metals_Bonds = Indices_Metals_Bonds,
    all_preds = all_preds,
    assets_to_port = assets_to_port,
    assets_to_trade = assets_to_port,
    stop_factor_var = 4,
    profit_factor_var = 8,
    risk_dollar_value_var = 5,
    end_period = 24,
    trade_direction = "Long",
    end_point_loss = -5,
    end_point_profit = 10,
    training_date =  "2022-01-17 10:00:00 AEST",
    low_to_price_lengths = c(200, 50),
    cor_periods = c(100),
    max_regs = 1000,
    save_path = "C:/Users/nikhi/Documents/trade_data/single_asset_v3_Bayes_Reg_Portfolio/",
    file_name = "Equity_Port_V3_Bayes",
    regression_length = 18000,
    total_lag_cols = 40,
    direct_return_cols = direct_return_cols,
    remove_NA_Values = FALSE,
    xtnd_vars = FALSE,
    xtnd_ss_cols_PR_cols = 25,
    xtnd_ss_cols_BR_periods = c(100,200,300)
  ) {

    all_cor_V3_Data <-
      portfolio_get_V3_cor_data_TOTAL_SUMMED(
        Indices_Metals_Bonds = Indices_Metals_Bonds,
        all_preds = all_preds,
        assets_to_port = assets_to_port,
        low_to_price_lengths = low_to_price_lengths,
        cor_periods = cor_periods,
        max_regs = max_regs
      )

    wanted_period_cols <-
      seq(1,direct_return_cols,1) %>%
      map(~ glue::glue("period_return_{.x}_Price")) %>%
      unlist()


    portfolio_data_testing_data <-
      get_portfolio_model_fast_summed(
        asset_data = Indices_Metals_Bonds %>%
          map( ~ .x %>% filter( Date > ( as_date(training_date) - months(6)) ) ),
        asset_of_interest = assets_to_trade,
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
      ) %>%
      group_by(Date,end_point_loss ,
               end_point_profit, risk_dollar_value, stop_factor, profit_factor) %>%
      summarise(Final_Return = sum(Final_Return, na.rm = T),
                across(contains( "period_return_"), ~ sum(., na.rm = T)) ) %>%
      ungroup() %>%
      dplyr::select(Date,
                    end_point_loss ,
                    end_point_profit, risk_dollar_value, stop_factor, profit_factor,
                    Final_Return, matches(wanted_period_cols))

    all_cor_var_combos <-
      names(all_cor_V3_Data[[1]]) %>%
      keep(~ !str_detect(.x, "Date")) %>%
      unlist()
    error_correcting_vars <- all_cor_var_combos %>%
      keep(~ str_detect(.x, "AR_")) %>%
      unlist()

    additional_cor_vars <- list()
    c = 0
    for (i in 1:length(all_cor_var_combos)) {
      for (j in 1:length(all_cor_var_combos)) {

        if(all_cor_var_combos[i] != all_cor_var_combos[j] ) {
          c = c + 1
          additional_cor_vars[[c]] <- c(all_cor_var_combos[i], all_cor_var_combos[j], paste0("Cor_V3_LM_", c) )
        }

      }
    }

    temp_reg_data_testing <-
      portfolio_get_V3_cor_data_TOTAL_SUMMED_reg_dat(
        all_dat_pivoted  = all_cor_V3_Data[[1]],
        correlation_data = all_cor_V3_Data[[2]],
        portfolio_data = portfolio_data_testing_data,
        additional_cor_vars = additional_cor_vars,
        error_calc_cols = error_correcting_vars %>% unique() ,
        lag_dependant = end_period + 1,
        total_lag_cols = total_lag_cols,
        direct_return_cols = direct_return_cols,
        xtnd_vars = xtnd_vars,
        xtnd_ss_cols_PR_cols = xtnd_ss_cols_PR_cols,
        xtnd_ss_cols_BR_periods = xtnd_ss_cols_BR_periods
      )

    all_cor_vars <-
      names(temp_reg_data_testing) %>%
      keep(~ str_detect(.x, "cor_LM[0-9]+")|str_detect(.x, "Final_Return_lag")|str_detect(.x, "Period_Return_Lag_")) %>%
      unlist() %>%
      as.character() %>%
      unique()


    if(remove_NA_Values == TRUE) {
      temp_reg_data_testing <-
        temp_reg_data_testing %>%
        filter(if_all(everything(), ~ !is.na(.)))
    }

    testing_prediction_data <-
      Porfolio_get_preds_V3_TOTAL_SUM_Bayes(
        reg_dat = temp_reg_data_testing %>% filter(Date >= training_date),
        training_end_date = training_date,
        save_path = save_path,
        file_name = file_name,
        regression_length = regression_length
      )

    model_prediction_data <-
      testing_prediction_data %>%
      filter(Date > training_date) %>%
      arrange(Date) %>%
      mutate(
        pred_10000_mean_roll_250 =
          slider::slide_dbl(.x  = predicted, .f = ~ mean(.x, na.rm = T), .before = 250),
        pred_10000_sd_roll_250 =
          slider::slide_dbl(.x  = predicted, .f = ~ sd(.x, na.rm = T), .before = 250),

        pred_10000_mean_roll_500 =
          slider::slide_dbl(.x  = predicted, .f = ~ mean(.x, na.rm = T), .before = 500),
        pred_10000_sd_roll_500 =
          slider::slide_dbl(.x  = predicted, .f = ~ sd(.x, na.rm = T), .before = 500),

        pred_10000_mean_roll_100 =
          slider::slide_dbl(.x  = predicted, .f = ~ mean(.x, na.rm = T), .before = 100),
        pred_10000_sd_roll_100 =
          slider::slide_dbl(.x  = predicted, .f = ~ sd(.x, na.rm = T), .before = 100),

        pred_10000_mean_roll_600 =
          slider::slide_dbl(.x  = predicted, .f = ~ mean(.x, na.rm = T), .before = 600),
        pred_10000_sd_roll_600 =
          slider::slide_dbl(.x  = predicted, .f = ~ sd(.x, na.rm = T), .before = 600),

        pred_10000_mean_roll_1000 =
          slider::slide_dbl(.x  = predicted, .f = ~ mean(.x, na.rm = T), .before = 1000),
        pred_10000_sd_roll_1000 =
          slider::slide_dbl(.x  = predicted, .f = ~ sd(.x, na.rm = T), .before = 1000),

        pred_10000_mean_roll_1500 =
          slider::slide_dbl(.x  = predicted, .f = ~ mean(.x, na.rm = T), .before = 1500),
        pred_10000_sd_roll_1500 =
          slider::slide_dbl(.x  = predicted, .f = ~ sd(.x, na.rm = T), .before = 1500),

        pred_10000_mean_roll_2000 =
          slider::slide_dbl(.x  = predicted, .f = ~ mean(.x, na.rm = T), .before = 2000),
        pred_10000_sd_roll_2000 =
          slider::slide_dbl(.x  = predicted, .f = ~ sd(.x, na.rm = T), .before = 2000),

        pred_10000_mean_roll_50 =
          slider::slide_dbl(.x  = predicted, .f = ~ mean(.x, na.rm = T), .before = 50),
        pred_10000_sd_roll_50 =
          slider::slide_dbl(.x  = predicted, .f = ~ sd(.x, na.rm = T), .before = 50),

        pred_10000_mean_roll_10 =
          slider::slide_dbl(.x  = predicted, .f = ~ mean(.x, na.rm = T), .before = 10),
        pred_10000_sd_roll_10 =
          slider::slide_dbl(.x  = predicted, .f = ~ sd(.x, na.rm = T), .before = 10)
      )


    return(model_prediction_data)

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

get_available_V3_Assets <-
  function(
    directory_var = "C:/Users/nikhi/Documents/trade_data/Day_Trader_Single_Asset_V3_Expanded_Models/"
  ) {

    files_x <-
      fs::dir_info(directory_var)

    available_assets <-
      files_x %>%
      mutate(Asset = str_remove_all(path, directory_var),
             Asset = str_remove_all(Asset, "GLM_AR_period_return_32_Price_"),
             Asset = str_remove_all(Asset, "LM_AR_period_return_32_Price_"),
             Asset = str_remove_all(Asset, "GLM_Copula_period_return_50_Price_"),
             Asset = str_remove_all(Asset, "LM_Copula_period_return_50_Price_"),
             Asset = str_remove_all(Asset, "GLM_Macro_period_return_"),
             Asset = str_remove_all(Asset, "LM_Macro_period_return_"),
             Asset = str_remove_all(Asset, "GLM_state_space_period_return_50_Price_"),
             Asset = str_remove_all(Asset, "LM_state_space_period_return_50_Price_"),
             Asset = str_remove_all(Asset, "GLM_AR_period_return_35_Price_"),
             Asset = str_remove_all(Asset, "GLM_AR_period_return_50_Price_"),
             Asset = str_remove_all(Asset, "GLM_AR_period_return_36_Price_"),
             Asset = str_remove_all(Asset, "GLM_AR_period_return_42_Price_")
      ) %>%
      filter(!str_detect(Asset, "copula|AR|GLM|LM|Period|Price|Pred|SIG|Sig")) %>%
      mutate(Asset = str_remove_all(Asset, ".RDS")) %>%
      distinct(Asset)

  }

portfolio_V3_generate_random_returns <-
  function(return_list_dfr,
           algo_name = as.character(distinct_algos$algo_name[1]),
           sample_size = 250000,
           time_series_length = 800) {

    temp_dat <-
      return_list_dfr %>%
      filter(algo_name == algo_name)

    random_sample_vec <- numeric(sample_size)

    for (j in 1:250000) {


      sampled_indexes <- round(runif(n = 1,
                                     min = 1,
                                     max = dim(temp_dat)[1] - time_series_length ))

      sampled_indexes <- seq(sampled_indexes, sampled_indexes + time_series_length, 1)

      random_sample_vec[j] =
        sum(temp_dat$Final_Return[sampled_indexes], na.rm = T)
    }

    random_return_tibble <-
      list(
        list(random_sample_vec, algo_name)
      ) %>%
      map_dfr(
        ~ tibble(
          returns_sampled = .x[[1]],
          algo = .x[[2]]
        )
      )

    return(random_return_tibble)

  }

#' portfolio_get_deviation_from_total
#'
#' @param total_return_portfolio
#' @param portfolio_data
#' @param periods_to_use_deviation
#'
#' @return
#' @export
#'
#' @examples
portfolio_get_deviation_from_total <-
  function(
    total_return_portfolio = total_return_portfolio,
    portfolio_data = portfolio_data,
    periods_to_use_deviation = c(1,3,4),
    mean_periods_deviation = c(50, 100)
  ) {


    periods_to_use_deviation_cols_glue <-
      periods_to_use_deviation %>%
      map(
        ~ glue::glue("period_return_{.x}_Price")
      ) %>%
      unlist() %>%
      as.character() %>%
      paste(collapse = ",")

    select_statement <-
      glue::glue("portfolio_data %>% ungroup() %>% dplyr::select(Date, Asset, {periods_to_use_deviation_cols_glue})")

    select_statement_total <-
      glue::glue("total_return_portfolio %>% ungroup() %>% dplyr::select(Date, {periods_to_use_deviation_cols_glue})")

    portfolio_data <-
      eval(parse(text=select_statement))

    total_return_portfolio <-
      eval(parse(text=select_statement_total))

    names(total_return_portfolio) <-
      names(total_return_portfolio) %>%
      map(
        ~ case_when(str_detect(.x, "period") ~ glue::glue("total_{.x}"),
                    TRUE ~ .x)
      ) %>%
      unlist() %>%
      as.character()

    joined_data <-
      portfolio_data %>%
      left_join(total_return_portfolio)

    difference_statements <-
      periods_to_use_deviation %>%
      map(
        ~ glue::glue("single_vs_total_return_{.x} = lag(total_period_return_{.x}_Price, {.x} + 1) - lag(period_return_{.x}_Price, {.x} + 1)")
      ) %>%
      unlist() %>%
      paste(collapse = ",")


    difference_statements_mean <- list()
    for (i in 1:length(mean_periods_deviation)) {

      difference_statements_mean[[i]] <-
        periods_to_use_deviation %>%
        map(
          ~ glue::glue("single_vs_total_return_mean_{.x}_{mean_periods_deviation[i]} = slider::slide_dbl(.x = single_vs_total_return_{.x}, .f = ~mean(.x, na.rm = T), .before = {mean_periods_deviation[i]} )")
        )

    }

    difference_statements_mean <-
      difference_statements_mean %>%
      unlist() %>%
      paste(collapse = ",")

    # correlation_statements <-
    #   periods_to_use_deviation %>%
    #   map(
    #     ~ glue::glue("single_vs_total_return_cor_{.x} = slider::slide2_dbl(.x = lag(total_period_return_{.x}_Price, {.x} + 1), .y = lag(period_return_{.x}_Price, {.x} + 1), .f = ~ cor(.x, .y), .before = {cor_period})
    #                  lag(total_period_return_{.x}_Price, {.x} + 1) - lag(period_return_{.x}_Price, {.x} + 1)")
    #   ) %>%
    #   unlist() %>%
    #   paste(collapse = ",")

    execuate_statement <-
      glue::glue("joined_data %>% ungroup() %>% group_by(Asset) %>% arrange(Asset, .by_group = TRUE) %>% group_by(Asset) %>% mutate({difference_statements}, {difference_statements_mean}) %>% dplyr::select(Date, Asset, contains('single_vs_total_return_'))")

    returned_data <- eval(parse(text = execuate_statement))

    return(returned_data)

  }

#' portfolio_get_return_auto_cor
#'
#' @param portfolio_data
#' @param auto_cor_cols
#' @param cor_skip_periods
#' @param cor_period
#'
#' @return
#' @export
#'
#' @examples
portfolio_get_return_auto_cor <-
  function(
    cor_data_to_auto_cor = portfolio_data_train,
    auto_cor_cols = 25,
    cor_skip_periods = c(2,4,5,6),
    suffix_var = "",
    cor_period = 50
  ) {


    statement_accumulator <- list()
    for (i in 1:length(cor_skip_periods) ) {

      statement_accumulator[[i]] <-
        seq(1,auto_cor_cols-cor_skip_periods[i],1) %>%
        map(
          ~ glue::glue("auto_cor_return_{.x}_{.x + cor_skip_periods[i]}_{suffix_var} =
                               slider::slide2_dbl(.x = lag(period_return_{.x}_Price, {.x + 1}),
                                                   .y = lag(period_return_{.x + cor_skip_periods[i]}_Price, {.x + 1 + cor_skip_periods[i]}),
                                                   .f = ~ cor(.x, .y),
                                                   .before = {cor_period})")
        ) %>%
        paste(collapse = ",")

    }

    message("Function:portfolio_get_return_auto_cor line 2642 ")

    autocor_cols_glue <-
      statement_accumulator %>%
      unlist() %>%
      as.character() %>%
      paste(collapse = ",")

    message("Function:portfolio_get_return_auto_cor line 2650 ")

    complete_statements <-
      glue::glue("cor_data_to_auto_cor %>%
                    group_by(Asset) %>%
                    arrange(Date, .by_group = TRUE) %>%
                    group_by(Asset) %>%
                    mutate({autocor_cols_glue}) %>%
                    ungroup() %>%
                    dplyr::select(Date, Asset, contains('auto_cor_return_'))")

    message("Function:portfolio_get_return_auto_cor line 2661 ")

    message(glue::glue("{dim(cor_data_to_auto_cor)[1]} dim for Portfolio Dat in portfolio_get_return_auto_cor"))

    returned_data <- eval(parse(text = complete_statements))

    rm(cor_data_to_auto_cor)
    gc()

    return(returned_data)

  }

#' get_portfolio_dat_no_V3_New
#'
#' @param portfolio_data
#' @param xtnd_ss_cols_PR_cols
#' @param xtnd_ss_cols_BR_periods
#' @param lag_dependant
#' @param auto_cor_cols
#' @param cor_skip_periods
#' @param cor_period
#'
#' @return
#' @export
#'
#' @examples
get_portfolio_dat_no_V3_New <-
  function(
    portfolio_data = portfolio_data_train,
    xtnd_ss_cols_PR_cols = c(10,20,30,40,50, 60),
    xtnd_ss_cols_BR_periods = c(100,200,300),
    lag_dependant = end_period + 1,
    auto_cor_cols = 25,
    cor_skip_periods = c(2,4,5,6),
    cor_period = 50,
    periods_to_use_deviation = c(1,3,4),
    mean_periods_deviation = c(50, 100)
  ) {

    total_reg_data <-
      portfolio_data %>%
      ungroup() %>%
      distinct(Date, Asset)

    total_return_portfolio <-
      portfolio_data %>%
      ungroup() %>%
      group_by(Date) %>%
      summarise(
        across(.cols = c(Final_Return, contains("period_return_")),
               .fns = ~ sum(., na.rm = T))
      ) %>%
      ungroup() %>%
      mutate(Asset = "Portfolio") %>%
      mutate(across(.cols = contains("period_return_"), .fns = ~ as.numeric(.)))

    deviation_data <-
      portfolio_get_deviation_from_total(
        total_return_portfolio = total_return_portfolio,
        portfolio_data = portfolio_data,
        periods_to_use_deviation = periods_to_use_deviation,
        mean_periods_deviation = mean_periods_deviation
      )

    req_ss_extnd_cols_PR <-
      xtnd_ss_cols_PR_cols %>%
      map(
        ~
          glue::glue(
            "
          state_space_data_PR_{.x} <-
              portfolio_LM_state_space(
                  portfolio_data = portfolio_data,
                  state_space_col = 'period_return_{.x}_Price' ,
                  required_lag = {.x}, #Does not need a plus 1 its built in
                  roll_period_state_space = 500
                )"
          )
      )  %>%
      unlist() %>%
      as.character() %>%
      paste(collapse = "\n")

    req_ss_extnd_cols_PR_names <-
      xtnd_ss_cols_PR_cols %>%
      map(
        ~ glue::glue("state_space_data_PR_{.x}")
      ) %>%
      unlist() %>%
      as.character() %>%
      paste(collapse = ",")

    req_ss_extnd_cols_PR_list <-
      glue::glue("All_state_space_data_PR <- list({req_ss_extnd_cols_PR_names})")

    rm_statement <- glue::glue("rm({req_ss_extnd_cols_PR_names})")

    eval(parse(text = req_ss_extnd_cols_PR))
    eval(parse(text = req_ss_extnd_cols_PR_list))
    eval(parse(text = rm_statement))
    gc()

    All_state_space_data_PR <-
      All_state_space_data_PR %>%
      reduce(left_join)

    All_state_space_data_PR <-
      All_state_space_data_PR %>%
      dplyr::select(Date, Asset, contains("state_space"))

    message("Made it to State SPace End line 2750")

    # state_space_data_FR_500 <-
    #   portfolio_LM_state_space(
    #     portfolio_data = portfolio_data,
    #     state_space_col = "Final_Return",
    #     required_lag = lag_dependant, #Does not need a plus 1 its built in
    #     roll_period_state_space = 500
    #   )

    All_state_space_data_PR <-
      All_state_space_data_PR
      # left_join(
      #   state_space_data_FR_500 %>%
      #     dplyr::select(Date, Asset,  contains("state_space"))
      # )

    total_reg_data <-
      total_reg_data %>%
      left_join(All_state_space_data_PR %>% ungroup()) %>%
      left_join(deviation_data %>% ungroup())

    rm(deviation_data)

    message("Made it to State SPace joined with TOtal Reg End line 2771")

    req_BR_extnd_cols_PR <-
      xtnd_ss_cols_BR_periods %>%
      map(
        ~ glue::glue("
          brownian_tech_data_{.x} <-
                portfolio_LM_brownian_checks(portfolio_data = portfolio_data,
                                             brownian_period = {.x},
                                             col_to_use = 'period_return_1_Price',
                                             lag_period_to_use = 1)
          brownian_tech_data_FR_{.x} <-
                portfolio_LM_brownian_checks(portfolio_data = portfolio_data,
                                             brownian_period = {.x},
                                             col_to_use = 'Final_Return',
                                             lag_period_to_use = {lag_dependant} )"
        )
      ) %>%
      unlist() %>%
      as.character() %>%
      paste(collapse = "\n")

    req_BR_extnd_cols_PR_names <-
      xtnd_ss_cols_BR_periods %>%
      map(
        ~ glue::glue("brownian_tech_data_{.x}, brownian_tech_data_FR_{.x}")
      ) %>%
      unlist() %>%
      as.character() %>%
      paste(collapse = ",")

    req_BR_extnd_cols_PR_list <-
      glue::glue("All_BR_data_PR <- list({req_BR_extnd_cols_PR_names})")

    rm_statement <- glue::glue("rm({req_BR_extnd_cols_PR_names})")

    eval(parse(text = req_BR_extnd_cols_PR))
    message("Made it to Brownian First Statement req_BR_extnd_cols_PR line 2808")
    eval(parse(text = req_BR_extnd_cols_PR_list))
    message("Made it to Brownian Second Statement req_BR_extnd_cols_PR line 2810")
    eval(parse(text = rm_statement))
    message("Made it to Brownian Third Statement req_BR_extnd_cols_PR line 2812")
    gc()

    All_BR_data_PR <-
      All_BR_data_PR %>%
      reduce(left_join)

    message("Made it to All_BR_data_PR statement line 2819")

    total_reg_data <-
      total_reg_data %>%
      left_join(All_BR_data_PR %>%
                  dplyr::select(Date, Asset, contains("brownian"))
      )

    message("Made it to total_reg_data statement line 2827")

    rm(All_state_space_data_PR, All_BR_data_PR)

    auto_cor_cols_asset_level <-
      portfolio_get_return_auto_cor(
        cor_data_to_auto_cor = portfolio_data,
        auto_cor_cols = auto_cor_cols,
        cor_skip_periods = cor_skip_periods,
        cor_period = cor_period
      ) %>%
      ungroup()

    gc()

    message("Made it to auto_cor_cols statement line 2840")


    auto_cor_cols_total <-
      portfolio_get_return_auto_cor(
        cor_data_to_auto_cor = total_return_portfolio,
        auto_cor_cols = auto_cor_cols,
        suffix_var = "total",
        cor_skip_periods = cor_skip_periods,
        cor_period = cor_period
      ) %>%
      ungroup() %>%
      dplyr::select(-Asset)

    gc()
    rm(total_return_portfolio)
    gc()

    message("Made it to auto_cor_cols_total statement line 2854")

    total_reg_data <-
      total_reg_data %>%
      left_join(auto_cor_cols_asset_level) %>%
      left_join(auto_cor_cols_total)

    message("Made it to total_reg_data statement line 2861")

    rm(auto_cor_cols_asset_level, auto_cor_cols_total)
    gc()

    Final_Returns <-
      portfolio_data %>%
      distinct(Date, Asset, Final_Return)

    message("Made it to Final_Returns statement line 2878")

    total_reg_data <-
      total_reg_data %>%
      left_join(Final_Returns)

    message("Made it to total_reg_data statement line 2884")

    # reg_vars <-
    #   names(total_reg_data) %>%
    #   keep(~ str_detect(.x, "auto_cor|brownian|state_space"))

    gc()

    return(total_reg_data)

  }

#' portfolio_gen_model_no_V3_New
#'
#' @param reg_dat
#' @param reg_vars
#' @param training_end_date
#' @param Bayes_or_LM
#' @param save_path
#' @param dependant_var
#'
#' @return
#' @export
#'
#' @examples
portfolio_gen_model_no_V3_New <-
  function(
    reg_dat = temp_reg_data_train,
    reg_vars = all_cor_vars,
    training_end_date,
    Bayes_or_LM = "LM",
    save_path = save_location,
    dependant_var = "Final_Return",
    sig_thresh_LM = 1,
    file_name = file_name,
    reg_samples = 2500
    ) {

    training_data <-
      reg_dat %>%
      ungroup() %>%
      filter(if_all(everything(), ~ !is.na(.))) %>%
      filter(Date <= training_end_date) %>%
      group_by(Asset) %>%
      slice_sample(n = reg_samples) %>%
      ungroup()

    rm(reg_dat)
    gc()

    lm_form <-
      create_lm_formula(dependant = dependant_var, independant = reg_vars)

    LM_model <- lm(data = training_data %>% filter(Final_Return != 0),
                   formula = lm_form)

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
      c(sig_coefs, "Asset") %>%
      unlist()

    lm_form <-
      create_lm_formula(dependant = dependant_var,
                        independant = sig_coefs)

    if(Bayes_or_LM == "Bayes") {
      LM_model <- bayesreg::bayesreg(formula = lm_form,
                                     data = training_data %>% filter(Final_Return != 0),
                                     model = "normal")
    }

    if(Bayes_or_LM == "LM") {

      LM_model <- lm(formula = lm_form,
                     data = training_data %>% filter(Final_Return != 0))
    }

    training_data <-
      training_data %>%
      filter(if_all(everything() ,~ !is.na(.)))

    predicted_train <- predict(newdata = training_data,
                               object =  LM_model, type = "response")

    means_by_asset <-
      training_data %>%
      mutate(preds = predicted_train) %>%
      summarise(
        trained_mean = mean(preds, na.rm = T),
        trained_sd = sd(preds, na.rm = T)
      ) %>%
      ungroup() %>%
      dplyr::select(trained_mean, trained_sd)

    saveRDS(LM_model,
            file = glue::glue("{save_path}/{file_name}.RDS") )

    write.csv(
      means_by_asset,
      file = glue::glue("{save_path}/{file_name}_Mean_SD.csv"),
      row.names = FALSE
    )

    rm(testing_data, training_data, all_cor_V3_Data, LM_model, predicted_train,predicted )
    gc()

    return(NULL)

  }

portfolio_read_model_no_V3_New <-
  function(
    reg_dat = temp_reg_data_test,
    training_end_date,
    save_path = save_location,
    file_name = file_name
    ) {

    LM_model <- readRDS(file = glue::glue("{save_path}/{file_name}.RDS") )

    LM_model$model <- NULL
    LM_model$fitted.values <- NULL
    gc()

    reg_dat <-
      reg_dat %>% filter(Date > training_end_date)

    gc()

    predicted_test <- predict(newdata = reg_dat,
                               object =  LM_model,
                              type = "response")

    rm(LM_model)

    model_prediction_data <-
      reg_dat %>%
      filter(Date > training_end_date) %>%
      mutate(predicted = predicted_test) %>%
      ungroup() %>%
      dplyr::select(Date, Asset, Final_Return, predicted)

    rm(reg_dat)
    gc()

  model_prediction_data <-
      model_prediction_data %>%
      group_by(Date) %>%
      mutate(
        predicted_portfolio = sum(predicted, na.rm = T)
      ) %>%
      ungroup() %>%
      group_by(Asset) %>%
      arrange(Date, .by_group = TRUE) %>%
      group_by(Asset) %>%
      mutate(
        pred_10000_mean_roll_250 =
          slider::slide_dbl(.x  = predicted, .f = ~ mean(.x, na.rm = T), .before = 250),
        pred_10000_sd_roll_250 =
          slider::slide_dbl(.x  = predicted, .f = ~ sd(.x, na.rm = T), .before = 250),

        pred_10000_mean_roll_500 =
          slider::slide_dbl(.x  = predicted, .f = ~ mean(.x, na.rm = T), .before = 500),
        pred_10000_sd_roll_500 =
          slider::slide_dbl(.x  = predicted, .f = ~ sd(.x, na.rm = T), .before = 500),

        pred_10000_mean_roll_100 =
          slider::slide_dbl(.x  = predicted, .f = ~ mean(.x, na.rm = T), .before = 100),
        pred_10000_sd_roll_100 =
          slider::slide_dbl(.x  = predicted, .f = ~ sd(.x, na.rm = T), .before = 100),

        pred_10000_mean_roll_600 =
          slider::slide_dbl(.x  = predicted, .f = ~ mean(.x, na.rm = T), .before = 600),
        pred_10000_sd_roll_600 =
          slider::slide_dbl(.x  = predicted, .f = ~ sd(.x, na.rm = T), .before = 600),

        pred_10000_mean_roll_1000 =
          slider::slide_dbl(.x  = predicted, .f = ~ mean(.x, na.rm = T), .before = 1000),
        pred_10000_sd_roll_1000 =
          slider::slide_dbl(.x  = predicted, .f = ~ sd(.x, na.rm = T), .before = 1000),

        pred_10000_mean_roll_1500 =
          slider::slide_dbl(.x  = predicted, .f = ~ mean(.x, na.rm = T), .before = 1500),
        pred_10000_sd_roll_1500 =
          slider::slide_dbl(.x  = predicted, .f = ~ sd(.x, na.rm = T), .before = 1500),

        pred_10000_mean_roll_2000 =
          slider::slide_dbl(.x  = predicted, .f = ~ mean(.x, na.rm = T), .before = 2000),
        pred_10000_sd_roll_2000 =
          slider::slide_dbl(.x  = predicted, .f = ~ sd(.x, na.rm = T), .before = 2000),

        pred_10000_mean_roll_50 =
          slider::slide_dbl(.x  = predicted, .f = ~ mean(.x, na.rm = T), .before = 50),
        pred_10000_sd_roll_50 =
          slider::slide_dbl(.x  = predicted, .f = ~ sd(.x, na.rm = T), .before = 50),

        pred_10000_mean_roll_10 =
          slider::slide_dbl(.x  = predicted, .f = ~ mean(.x, na.rm = T), .before = 10),
        pred_10000_sd_roll_10 =
          slider::slide_dbl(.x  = predicted, .f = ~ sd(.x, na.rm = T), .before = 10),


        pred_portfolio_10000_mean_roll_250 =
          slider::slide_dbl(.x  = predicted_portfolio, .f = ~ mean(.x, na.rm = T), .before = 250),
        pred_portfolio_10000_sd_roll_250 =
          slider::slide_dbl(.x  = predicted_portfolio, .f = ~ sd(.x, na.rm = T), .before = 250),

        pred_portfolio_10000_mean_roll_500 =
          slider::slide_dbl(.x  = predicted_portfolio, .f = ~ mean(.x, na.rm = T), .before = 500),
        pred_portfolio_10000_sd_roll_500 =
          slider::slide_dbl(.x  = predicted_portfolio, .f = ~ sd(.x, na.rm = T), .before = 500),

        pred_portfolio_10000_mean_roll_100 =
          slider::slide_dbl(.x  = predicted_portfolio, .f = ~ mean(.x, na.rm = T), .before = 100),
        pred_portfolio_10000_sd_roll_100 =
          slider::slide_dbl(.x  = predicted_portfolio, .f = ~ sd(.x, na.rm = T), .before = 100),

        pred_portfolio_10000_mean_roll_600 =
          slider::slide_dbl(.x  = predicted_portfolio, .f = ~ mean(.x, na.rm = T), .before = 600),
        pred_portfolio_10000_sd_roll_600 =
          slider::slide_dbl(.x  = predicted_portfolio, .f = ~ sd(.x, na.rm = T), .before = 600),

        pred_portfolio_10000_mean_roll_1000 =
          slider::slide_dbl(.x  = predicted_portfolio, .f = ~ mean(.x, na.rm = T), .before = 1000),
        pred_portfolio_10000_sd_roll_1000 =
          slider::slide_dbl(.x  = predicted_portfolio, .f = ~ sd(.x, na.rm = T), .before = 1000),

        pred_portfolio_10000_mean_roll_1500 =
          slider::slide_dbl(.x  = predicted_portfolio, .f = ~ mean(.x, na.rm = T), .before = 1500),
        pred_portfolio_10000_sd_roll_1500 =
          slider::slide_dbl(.x  = predicted_portfolio, .f = ~ sd(.x, na.rm = T), .before = 1500),

        pred_portfolio_10000_mean_roll_2000 =
          slider::slide_dbl(.x  = predicted_portfolio, .f = ~ mean(.x, na.rm = T), .before = 2000),
        pred_portfolio_10000_sd_roll_2000 =
          slider::slide_dbl(.x  = predicted_portfolio, .f = ~ sd(.x, na.rm = T), .before = 2000),

        pred_portfolio_10000_mean_roll_50 =
          slider::slide_dbl(.x  = predicted_portfolio, .f = ~ mean(.x, na.rm = T), .before = 50),
        pred_portfolio_10000_sd_roll_50 =
          slider::slide_dbl(.x  = predicted_portfolio, .f = ~ sd(.x, na.rm = T), .before = 50),

        pred_portfolio_10000_mean_roll_10 =
          slider::slide_dbl(.x  = predicted_portfolio, .f = ~ mean(.x, na.rm = T), .before = 10),
        pred_portfolio_10000_sd_roll_10 =
          slider::slide_dbl(.x  = predicted_portfolio, .f = ~ sd(.x, na.rm = T), .before = 10),

      )

    return(model_prediction_data)

  }


#' Title
#'
#' @param assets_to_port
#' @param db_location
#' @param start_date
#' @param profit_factor_var
#' @param risk_dollar_value_var
#' @param end_period
#' @param trade_direction
#' @param end_point_loss
#' @param end_point_profit
#' @param regression_length
#' @param direct_return_cols
#' @param lag_value_error
#' @param low_to_price_lengths
#' @param cor_period
#' @param dependant_var
#' @param save_location
#' @param file_name
#' @param training_date
#' @param xtnd_ss_cols_PR_cols
#' @param xtnd_ss_cols_BR_periods
#' @param lag_dependant
#' @param auto_cor_cols
#' @param cor_skip_periods
#' @param periods_to_use_deviation
#' @param mean_periods_deviation
#'
#' @return
#' @export
#'
#' @examples
portfolio_no_V3_New_algo_variant <-
  function(
    assets_to_port =
      c(
        "AUD_CAD",
        "AUD_USD",
        "AUD_JPY"
      ) %>% unique(),
    stop_factor_var = 10,
    currency_conversion = currency_conversion,
    asset_infor = asset_infor,
    db_location = "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data_Most_Assets_2025-09-13 2.db",
    start_date = "2022-02-01",
    profit_factor_var = 50,
    risk_dollar_value_var = 5,
    end_period = 132,
    trade_direction = "Long",
    end_point_loss = -5,
    end_point_profit = 25,
    regression_length = 25000,
    direct_return_cols = 24,
    lag_value_error = end_period + 1,
    low_to_price_lengths = c(400),
    cor_period = c(50),
    dependant_var = "Final_Return",
    save_location = "C:/Users/Nikhil Chandra/Documents/trade_data/Day_Trader_Cor_Continuous_Models/",
    file_name = "AUD_CURR_NON_V3_NEW_MODEL",
    training_date = "2022-02-01",
    testing_date = NULL,
    xtnd_ss_cols_PR_cols = c(1,5,10,20,30,40,50,60,70,80,120, 100),
    xtnd_ss_cols_BR_periods = c(100,200,300, 50, 150, 250, 350, 25, 500),
    lag_dependant = end_period + 1,
    auto_cor_cols = 40,
    cor_skip_periods = c(1,2,4,5,6,8,10,12,14,16),
    periods_to_use_deviation = c(1,10,20,30,40,50),
    mean_periods_deviation = c(50, 100),
    estimate_trades = FALSE,
    trade_statement = NULL,
    current_time = now() %>% as_datetime()
  ) {

    if(is.null(testing_date)) { testing_date <- training_date}

    Indices_Metals_Bonds <- list()

    Indices_Metals_Bonds[[1]] <-
      get_db_data_quickly_algo(
        db_location = db_location,
        start_date = start_date,
        end_date = as.character(today() + days(30)),
        time_frame = "H1",
        bid_or_ask = "ask",
        assets =   assets_to_port
      ) %>%
      distinct()
    Indices_Metals_Bonds[[2]] <-
      get_db_data_quickly_algo(
        db_location = db_location,
        start_date = start_date,
        end_date = as.character(today() + days(30)),
        time_frame = "H1",
        bid_or_ask = "bid",
        assets =   assets_to_port
      ) %>%
      distinct()


    Indices_Metals_Bonds[[1]] <- Indices_Metals_Bonds[[1]] %>% filter(Date > testing_date)
    Indices_Metals_Bonds[[2]] <- Indices_Metals_Bonds[[2]] %>% filter(Date > testing_date)

    portfolio_data_test <-
      get_portfolio_model_fast_summed(
        asset_data = Indices_Metals_Bonds %>% map(~ .x %>% filter(Date > testing_date)),
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

        overwrite_volume = NULL,
        min_volume_only = FALSE,
        return_only_interested_col = FALSE,
        return_only_Final = TRUE
      )

    gc()

    temp_reg_data_test <-
      get_portfolio_dat_no_V3_New(
        portfolio_data = portfolio_data_test,
        xtnd_ss_cols_PR_cols = xtnd_ss_cols_PR_cols,
        xtnd_ss_cols_BR_periods = xtnd_ss_cols_BR_periods,
        lag_dependant = lag_dependant,
        auto_cor_cols = auto_cor_cols,
        cor_skip_periods = cor_skip_periods,
        cor_period = cor_period,
        periods_to_use_deviation = periods_to_use_deviation,
        mean_periods_deviation = mean_periods_deviation
      )

    gc()
    rm(portfolio_data_test)
    gc()
    rm(Indices_Metals_Bonds)
    gc()

    model_predicted_data <-
      portfolio_read_model_no_V3_New(
        reg_dat = temp_reg_data_test,
        training_end_date = training_date,
        save_path = save_location,
        file_name = file_name
      )

    message(glue::glue("Check Algo Estimated (model_predicted_data): {dim(model_predicted_data)[1]} \n"))

    model_predicted_data <-
      model_predicted_data %>%
      filter(Date > training_date)

    rm(temp_reg_data_test)
    gc()

    if(estimate_trades == TRUE & !is.null(trade_statement)) {

      model_predicted_data <-
        model_predicted_data %>%
        filter(Asset %in% assets_to_port) %>%
        group_by(Asset) %>%
        slice_max(Date)

      message(glue::glue("Check Algo Filtered and If Executed (model_predicted_data): {dim(model_predicted_data)[1]} \n"))

      max_date_pre_filt <-
        model_predicted_data %>% pull(Date) %>% max(na.rm = T)

      message(glue::glue("Check Max Date before trade statement Filter (model_predicted_data): {max_date_pre_filt} \n"))

      max_date_in_data <- floor_date(as_datetime(now(), tz = "Australia/Canberra"), "hour")
      rm(Indices_Metals_Bonds)
      gc()

      trade_dates <-
        model_predicted_data %>%
        ungroup() %>%
        slice_max(Date)

      trade_dates<-
        trade_dates %>%
        mutate(
          trade_col =
            eval(parse(text = trade_statement))
        ) %>%
        ungroup() %>%
        filter(trade_col == TRUE) %>%
        distinct(Asset, Date)

      message(glue::glue("discovered Trades: {dim(trade_dates)[1]} \n"))

      message(glue::glue("Date in Data:{trade_dates$Date[1]} Date in Max: {max_date_in_data}"))

      current_prices_ask <-
        read_all_asset_data_intra_day(
          asset_list_oanda = assets_to_port,
          save_path_oanda_assets = "D://Asset Data/oanda_data/",
          read_csv_or_API = "API",
          time_frame = "H1",
          bid_or_ask = "ask",
          how_far_back = 2,
          start_date = as_date(today() - days(3))
        )%>%
        map_dfr(bind_rows) %>%
        group_by(Asset) %>%
        slice_max(Date) %>%
        ungroup()

      single_asset_model_trades_filt <-
        trade_dates %>%
        mutate(trade_col = "Long",
               stop_factor = stop_factor_var,
               profit_factor = profit_factor_var,
               periods_ahead = end_period,
               risk_dollar_value = risk_dollar_value_var,
               end_point_loss = end_point_loss,
               end_point_profit = end_point_profit
        ) %>%
        group_by(Asset) %>%
        slice_max(Date) %>%
        ungroup() %>%
        left_join(current_prices_ask %>%
                    group_by(Asset) %>%
                    slice_max(Date) %>%
                    ungroup() %>%
                    dplyr::select(-Date)) %>%
        mutate(
          time_diff =
            abs(
              as.numeric(
                as_datetime(Date, tz = "Australia/Canberra") -
                  as_datetime(current_time, tz = "Australia/Canberra"),
                units = "mins"
              )
            ),
          date_check = max_date_in_data <= Date
        ) %>%
        group_by(Asset) %>%
        slice_min(time_diff) %>%
        ungroup()

      message(glue::glue("Trades Found Pre Filt {dim(single_asset_model_trades_filt)[1]}"))

      single_asset_model_trades_filt <-
        single_asset_model_trades_filt %>%
        # filter(time_diff <= 70 & date_check == TRUE) %>%
        filter(max_date_in_data <= Date)

      message(glue::glue("Trades Found {dim(single_asset_model_trades_filt)[1]}"))


      return(single_asset_model_trades_filt)

    }

    return(model_predicted_data)


  }
