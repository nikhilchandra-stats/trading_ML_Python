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
    total_lag_cols = 40
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
      summarise(Final_Return = sum(Final_Return, na.rm = T)) %>%
      ungroup()

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
        total_lag_cols = 40
      )

    all_cor_vars <-
      names(temp_reg_data_testing) %>%
      keep(~ str_detect(.x, "cor_LM[0-9]+")|str_detect(.x, "Final_Return_lag")) %>%
      unlist() %>%
      as.character() %>%
      unique()

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
          slider::slide_dbl(.x  = predicted, .f = ~ sd(.x, na.rm = T), .before = 100)
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
