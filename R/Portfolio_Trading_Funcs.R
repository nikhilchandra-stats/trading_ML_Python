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
