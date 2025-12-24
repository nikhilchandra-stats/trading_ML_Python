#' Title
#'
#' @param asset_data
#' @param Asset_of_interest
#' @param equity_index
#' @param gold_index
#' @param silver_index
#' @param bonds_index
#' @param interest_rates
#' @param cpi_data
#' @param sentiment_index
#' @param countries_for_int_strength
#' @param couplua_assets
#' @param pre_train_date_end
#' @param post_train_date_start
#' @param test_date_start
#' @param actual_wins_losses
#' @param neuron_adjustment
#' @param hidden_layers_var
#' @param ending_thresh
#' @param trade_direction
#' @param stop_value_var
#' @param profit_value_var
#' @param period_var
#'
#' @return
#' @export
#'
#' @examples
single_asset_Logit_indicator <-
  function(
    asset_data = Indices_Metals_Bonds[[1]],
    All_Daily_Data = All_Daily_Data,
    Asset_of_interest = "EUR_USD",
    equity_index = equity_index,
    gold_index = gold_index,
    silver_index = silver_index,
    bonds_index = bonds_index,
    interest_rates = interest_rates,
    cpi_data = cpi_data,
    sentiment_index = sentiment_index,
    countries_for_int_strength = c("EUR", "USD"),
    couplua_assets = c("XAU_EUR", "XAG_EUR", "EUR_JPY", "EU50_EUR", "EUR_AUD", "EUR_GBP"),
    pre_train_date_end = "2023-01-01",
    post_train_date_start = "2023-02-01",
    test_date_start = "2025-01-01",
    actual_wins_losses = actual_wins_losses,
    neuron_adjustment = 1.1,
    hidden_layers_var= 2,
    ending_thresh = 0.02,
    trade_direction = "Long",
    stop_value_var = 1.5,
    profit_value_var = 15,
    period_var = 48
  ){

    interest_rates_diffs <-
      interest_rates %>%
      dplyr::select(Date_for_Join= Date, contains("_Diff"))

    cpi_data_diffs <-
      cpi_data %>%
      dplyr::select(Date_for_Join = Date, contains("_Diff"))

    interest_rate_strength_Index <-
      get_Interest_Rate_strength(
        interest_rates =interest_rates_diffs %>% mutate(Date = Date_for_Join),
        countries = countries_for_int_strength
      ) %>%
      mutate(Date_for_Join = Date)

    CPI_strength_index <-
      get_CPI_Rate_strength(
        cpi_data =cpi_data_diffs %>% mutate(Date = Date_for_Join),
        countries = countries_for_int_strength
      ) %>%
      mutate(Date_for_Join = Date)

    macro_for_join <-
      asset_data %>%
      distinct(Date) %>%
      mutate(Date_for_Join = as_date(Date)) %>%
      arrange(Date) %>%
      left_join(CPI_strength_index) %>%
      left_join(interest_rate_strength_Index) %>%
      left_join(sentiment_index %>% mutate(Date_for_Join = Date)) %>%
      dplyr::select(-Date_for_Join) %>%
      fill(everything(), .direction = "down") %>%
      filter(if_all(everything(), ~ !is.na(.)))

    indexes_data_for_join <-
      asset_data %>%
      distinct(Date) %>%
      mutate(Date_for_Join = as_date(Date)) %>%
      arrange(Date) %>%
      left_join(equity_index %>% mutate(Date_for_Join = Date) %>% dplyr::select(-Average_PCA) )%>%
      left_join(gold_index %>% mutate(Date_for_Join = Date)  %>% dplyr::select(-Average_PCA) )%>%
      left_join(silver_index %>% mutate(Date_for_Join = Date)  %>% dplyr::select(-Average_PCA) )%>%
      left_join(bonds_index %>% mutate(Date_for_Join = Date)   %>% dplyr::select(-Average_PCA) ) %>%
      dplyr::select(-Date_for_Join)  %>%
      fill(everything(), .direction = "down") %>%
      filter(if_all(everything(), ~ !is.na(.))) %>%
      mutate(
        across(.cols = !contains("Date"), .fns = ~ lag(.))
      ) %>%
      filter(if_all(everything(), ~ !is.na(.)))

    copula_accumulation <- list()

    for (i in 1:length(couplua_assets)) {

      copula_accumulation[[i]] <-
        estimating_dual_copula(
          asset_data_to_use = asset_data,
          asset_to_use = c(Asset_of_interest, couplua_assets[i]),
          price_col = "Open",
          rolling_period = 100,
          samples_for_MLE = 0.15,
          test_samples = 0.85
        ) %>%
        ungroup() %>%
        dplyr::select(Date, contains("_cor")|contains("_lm"))

    }

    # actual_wins_losses <-
    #   actual_wins_losses %>%
    #   filter(trade_col == trade_direction) %>%
    #   filter(
    #     stop_factor == stop_value_var,
    #     profit_factor == profit_value_var,
    #     periods_ahead == period_var,
    #     Asset == Asset_of_interest
    #   ) %>%
    #   mutate(
    #     bin_var =
    #       case_when(
    #         trade_start_prices > trade_end_prices & trade_col == "Short" ~ "win",
    #         trade_start_prices <= trade_end_prices & trade_col == "Short" ~ "loss",
    #
    #         trade_start_prices < trade_end_prices & trade_col == "Long" ~ "win",
    #         trade_start_prices >= trade_end_prices & trade_col == "Long" ~ "loss"
    #
    #       )
    #   )

    actual_wins_losses <-
      actual_wins_losses %>%
      filter(trade_col == trade_direction) %>%
      filter(
        stop_factor == stop_value_var,
        profit_factor == profit_value_var,
        periods_ahead == period_var,
        Asset == Asset_of_interest
      ) %>%
      mutate(
        bin_var =
          case_when(
            trade_return_dollar_aud > 0 & trade_col == "Short" ~ "win",
            trade_return_dollar_aud <= 0 & trade_col == "Short" ~ "loss",

            trade_return_dollar_aud > 0 & trade_col == "Long" ~ "win",
            trade_return_dollar_aud <= 0 & trade_col == "Long" ~ "loss"

          )
      )

    daily_indicator <-
      get_daily_indicators(
        Daily_Data = All_Daily_Data,
        asset_data = asset_data,
        Asset_of_interest = Asset_of_interest
      )

    daily_indicator <-
      daily_indicator %>%
      dplyr::select(-Price, -Open, -Low, -High, -Vol., -Date_for_join) %>%
      mutate(
        across(.cols = !contains("Date") & !contains("Asset"),
               .fns = ~ lag(.))
      ) %>%
      filter(if_all(everything(), ~ !is.na(.))) %>%
      mutate(
        Date = as_datetime(Date, tz = "Australia/Canberra")
      )

    daily_join_model <-
      actual_wins_losses %>%
      dplyr::select(Date, Asset ,bin_var) %>%
      filter(
        Asset == Asset_of_interest
      ) %>%
      left_join(daily_indicator) %>%
      filter(if_all(everything(),~!is.na(.)))

    daily_vars_for_indicator <-
      names(daily_join_model) %>%
      keep(~ !str_detect(.x, "Date") &
             !str_detect(.x, "bin_var") &
             !str_detect(.x, "Asset")) %>%
      unlist() %>%
      as.character()

    daily_indicator_formula <-
      create_lm_formula(dependant = "bin_var=='win'",
                        independant = daily_vars_for_indicator)
    daily_indicator_model <-
      glm(formula = daily_indicator_formula,
          data =
            daily_join_model %>%
            filter(Date <=pre_train_date_end) %>%
            filter( if_all(everything(), ~ !is.infinite(.)) ),
          family = binomial("logit"))
    summary(daily_indicator_model)
    message("Passed Daily Model")

    daily_indicator_pred <-
      daily_join_model %>%
      mutate(
        daily_indicator_pred = predict.glm(daily_indicator_model,
                                            newdata = daily_join_model, type = "response"),
        mean_daily_pred =
          mean( ifelse(Date <= pre_train_date_end, daily_indicator_pred, NA), na.rm = T ),
        sd_daily_pred =
          sd( ifelse(Date <= pre_train_date_end, daily_indicator_pred, NA), na.rm = T )
      )
    message("Passed Daily Prediction")


    copula_for_join_model <-
      actual_wins_losses %>%
      dplyr::select(Date, Asset ,bin_var) %>%
      filter(
        Asset == Asset_of_interest
      ) %>%
      left_join(copula_accumulation %>%
                        reduce(left_join) ) %>%
      filter(if_all(everything(),~!is.na(.)))

    copula_vars_for_indicator <-
      names(copula_for_join_model) %>%
      keep(~ !str_detect(.x, "Date") &
             !str_detect(.x, "bin_var") &
             !str_detect(.x, "Asset")) %>%
      unlist() %>%
      as.character()

    copula_indicator_formula <-
      create_lm_formula(dependant = "bin_var=='win'",
                        independant = copula_vars_for_indicator)
    copula_indicator_model <-
      glm(formula = copula_indicator_formula,
          data = copula_for_join_model %>% filter(Date <=pre_train_date_end),
          family = binomial("logit"))
    summary(copula_indicator_model)
    message("Passed Copula Model")

    copula_indicator_pred <-
      copula_for_join_model %>%
      mutate(
        copula_indicator_pred = predict.glm(copula_indicator_model,
                                           newdata = copula_for_join_model, type = "response"),
        mean_copula_pred =
          mean( ifelse(Date <= pre_train_date_end, copula_indicator_pred, NA), na.rm = T ),
        sd_copula_pred =
          sd( ifelse(Date <= pre_train_date_end, copula_indicator_pred, NA), na.rm = T )
      )
    message("Passed Copula Prediction")

    macro_for_join_model <-
      actual_wins_losses %>%
      dplyr::select(Date, Asset ,bin_var) %>%
      filter(
        Asset == Asset_of_interest
      ) %>%
      left_join(macro_for_join) %>%
      filter(if_all(everything(),~!is.na(.)))

    macro_vars_for_indicator <-
      names(macro_for_join_model) %>%
      keep(~ !str_detect(.x, "Date") &
             !str_detect(.x, "bin_var") &
             !str_detect(.x, "Asset")) %>%
      unlist() %>%
      as.character()

    macro_indicator_formula <-
      create_lm_formula(dependant = "bin_var=='win'",
                        independant = macro_vars_for_indicator)
    macro_indicator_model <-
      glm(formula = macro_indicator_formula,
          data = macro_for_join_model %>% filter(Date <=pre_train_date_end),
          family = binomial("logit"))
    summary(macro_indicator_model)
    message("Passed Macro Model")

    macro_indicator_pred <-
      macro_for_join_model %>%
      mutate(
        macro_indicator_pred = predict.glm(macro_indicator_model,
                                           newdata = macro_for_join_model, type = "response"),
        mean_macro_pred =
          mean( ifelse(Date <= pre_train_date_end, macro_indicator_pred, NA), na.rm = T ),
        sd_macro_pred =
          sd( ifelse(Date <= pre_train_date_end, macro_indicator_pred, NA), na.rm = T )
      )
    message("Passed Macro Pred")

    indexes_for_join_model <-
      actual_wins_losses %>%
      dplyr::select(Date, Asset ,bin_var) %>%
      filter(
        Asset == Asset_of_interest
      ) %>%
      left_join(indexes_data_for_join) %>%
      filter(if_all(everything(),~!is.na(.)))

    indexes_vars_for_indicator <-
      names(indexes_for_join_model) %>%
      keep(~ !str_detect(.x, "Date") &
             !str_detect(.x, "bin_var") &
             !str_detect(.x, "Asset")) %>%
      unlist() %>%
      as.character()

    indexes_indicator_formula <-
      create_lm_formula(dependant = "bin_var=='win'",
                        independant = indexes_vars_for_indicator)
    indexes_indicator_model <-
      glm(formula = indexes_indicator_formula,
          data = indexes_for_join_model %>% filter(Date <=pre_train_date_end),
          family = binomial("logit"))
    summary(indexes_indicator_model)

    message("Passed Indicator Model")

    indexes_indicator_pred <-
      indexes_for_join_model %>%
      mutate(
        indexes_indicator_pred = predict.glm(indexes_indicator_model,
                                             newdata = indexes_for_join_model, type = "response"),
        mean_indexes_pred =
          mean( ifelse(Date <= pre_train_date_end, indexes_indicator_pred, NA), na.rm = T ),
        sd_indexes_pred =
          sd( ifelse(Date <= pre_train_date_end, indexes_indicator_pred, NA), na.rm = T )
      )

    message("Passed Indicator Pred")

    technical_asset_data <- asset_data %>% filter(Asset == Asset_of_interest)
    technical_data <-
      create_technical_indicators(asset_data = technical_asset_data) %>%
      dplyr::select(-Price, -Low, -High, -Open)
    rm(technical_asset_data)

    technical_for_join_model <-
      actual_wins_losses %>%
      dplyr::select(Date, Asset ,bin_var) %>%
      filter(
        Asset == Asset_of_interest
      ) %>%
      left_join(technical_data) %>%
      filter(if_all(everything(),~!is.na(.)))

    technical_vars_for_indicator <-
      names(technical_for_join_model) %>%
      keep(~ !str_detect(.x, "Date") &
             !str_detect(.x, "bin_var") &
             !str_detect(.x, "Asset")) %>%
      unlist() %>%
      as.character()

    technical_indicator_formula <-
      create_lm_formula(dependant = "bin_var=='win'",
                        independant = technical_vars_for_indicator)

    technical_indicator_model <-
      glm(formula = technical_indicator_formula,
          data = technical_for_join_model %>%
            filter(Date <=pre_train_date_end) %>%
            filter( if_all(everything(), ~ !is.infinite(.)) ),
          family = binomial("logit"))

    message("Passed Technical Model")

    summary(technical_indicator_model)

    technical_indicator_pred <-
      technical_for_join_model %>%
      mutate(
        technical_indicator_pred = predict.glm(technical_indicator_model,
                                               newdata = technical_for_join_model, type = "response"),
        mean_indicator_pred =
          mean( ifelse(Date <= pre_train_date_end, technical_indicator_pred, NA), na.rm = T ),
        sd_indicator_pred =
          sd( ifelse(Date <= pre_train_date_end, technical_indicator_pred, NA), na.rm = T )
      )

    message("Passed Technical Pred")


    combined_indicator_NN <-
      macro_indicator_pred %>%
      dplyr::select(Date, Asset, bin_var,
                    contains("pred") ,
                    contains("PC1"), contains("Sentiment")  ) %>%
      distinct() %>%
      left_join(
        indexes_indicator_pred %>%
          dplyr::select(Date, Asset,
                        contains("pred"),
                        contains("PC1")  ) %>%
          distinct()
      ) %>%
      left_join(copula_indicator_pred %>%
                  dplyr::select(Date, Asset,
                                contains("pred"),
                                contains("cor"),
                                contains("lm")  ) %>%
                  distinct()) %>%
      left_join(
        technical_indicator_pred %>% dplyr::select(-bin_var)
      ) %>%
      left_join(daily_indicator_pred %>% dplyr::select(-bin_var) %>% distinct()) %>%
      janitor::clean_names() %>%
      filter(if_all(everything() ,~!is.na(.))) %>%
      mutate(

        technical_indicator_pred_ma_15 =
          slider::slide_dbl(.x = technical_indicator_pred, .f =~mean(.x, na.rm = T), .before = 10 ),
        technical_indicator_pred_ma_10 =
          slider::slide_dbl(.x = technical_indicator_pred, .f =~mean(.x, na.rm = T), .before = 10 ),
        technical_indicator_pred_ma_5 =
          slider::slide_dbl(.x = technical_indicator_pred, .f =~mean(.x, na.rm = T), .before = 5 ),

        macro_indicator_pred_100=
          slider::slide_dbl(.x = macro_indicator_pred, .f =~mean(.x, na.rm = T), .before = 100 ),
        macro_indicator_pred_50=
          slider::slide_dbl(.x = macro_indicator_pred, .f =~mean(.x, na.rm = T), .before = 50 ),
        macro_indicator_pred_60 =
          slider::slide_dbl(.x = macro_indicator_pred, .f =~mean(.x, na.rm = T), .before = 60 ),

        indexes_indicator_pred_ma_15 =
          slider::slide_dbl(.x = indexes_indicator_pred, .f =~mean(.x, na.rm = T), .before = 15 ),
        indexes_indicator_pred_ma_10 =
          slider::slide_dbl(.x = indexes_indicator_pred, .f =~mean(.x, na.rm = T), .before = 10 ),
        indexes_indicator_pred_ma_5 =
          slider::slide_dbl(.x = indexes_indicator_pred, .f =~mean(.x, na.rm = T), .before = 5 ),

        copula_indicator_pred_ma_15 =
          slider::slide_dbl(.x = copula_indicator_pred, .f =~mean(.x, na.rm = T), .before = 15 ),
        copula_indicator_pred_ma_10 =
          slider::slide_dbl(.x = copula_indicator_pred, .f =~mean(.x, na.rm = T), .before = 10 ),
        copula_indicator_pred_ma_5 =
          slider::slide_dbl(.x = copula_indicator_pred, .f =~mean(.x, na.rm = T), .before = 5 ),

        daily_indicator_pred_ma_15 =
          slider::slide_dbl(.x = daily_indicator_pred, .f =~mean(.x, na.rm = T), .before = 15 ),
        daily_indicator_pred_ma_10 =
          slider::slide_dbl(.x = daily_indicator_pred, .f =~mean(.x, na.rm = T), .before = 10 ),
        daily_indicator_pred_ma_5 =
          slider::slide_dbl(.x = daily_indicator_pred, .f =~mean(.x, na.rm = T), .before = 5 )
      )

    message("Created Combined Model Data")

    NN_indcator_coefs <-
      names(combined_indicator_NN) %>%
      keep(~ !str_detect(.x, "date|asset|bin_var") &
             !(str_detect(.x, "mean_|sd_") & str_detect(.x, "pred"))
           )

    NN_form <-  create_lm_formula(dependant = "bin_var=='win'", independant = NN_indcator_coefs)

    logit_combined_indicator_model <-
      glm(formula = NN_form,
          data =
            combined_indicator_NN %>%
            filter(date <= test_date_start) ,
          family = binomial("logit"))

    message("Created Combined Model")

    summary(logit_combined_indicator_model)

    logit_combined_pred <-
      combined_indicator_NN %>%
      rename(Date = date, Asset = asset) %>%
      mutate(
        logit_combined_pred = predict.glm(logit_combined_indicator_model,
                                          newdata = combined_indicator_NN, type = "response"),
        mean_logit_combined_pred =
          mean( ifelse(Date <= test_date_start, logit_combined_pred, NA), na.rm = T ),
        sd_logit_combined_pred =
          sd( ifelse(Date <= test_date_start, logit_combined_pred, NA), na.rm = T ),

        averaged_pred =
          indexes_indicator_pred*macro_indicator_pred*indexes_indicator_pred*logit_combined_pred,

        mean_averaged_pred =
          mean( ifelse(Date <= test_date_start, averaged_pred, NA), na.rm = T ),
        sd_averaged_pred =
          sd( ifelse(Date <= test_date_start, averaged_pred, NA), na.rm = T ),

      ) %>%
      dplyr::select(Date, Asset, contains("pred")) %>%
      filter(Date >= test_date_start) %>%
      left_join(
        # actual_wins_losses %>%
        #   dplyr::select(Date, Asset, stop_factor, profit_factor, periods_ahead, trade_col,
        #                 Time_Periods,trade_start_prices, trade_end_prices, trade_return_dollar_aud)

        actual_wins_losses %>%
          dplyr::select(Date, Asset, stop_factor, profit_factor, periods_ahead, trade_col,
                        trade_return_dollar_aud, contains("period_return_"))
      )

    return(logit_combined_pred)

  }

#' Title
#'
#' @param asset_data
#' @param Asset_of_interest
#' @param equity_index
#' @param gold_index
#' @param silver_index
#' @param bonds_index
#' @param interest_rates
#' @param cpi_data
#' @param sentiment_index
#' @param countries_for_int_strength
#' @param couplua_assets
#' @param pre_train_date_end
#' @param post_train_date_start
#' @param test_date_start
#' @param actual_wins_losses
#' @param neuron_adjustment
#' @param hidden_layers_var
#' @param ending_thresh
#' @param trade_direction
#' @param stop_value_var
#' @param profit_value_var
#' @param period_var
#'
#' @return
#' @export
#'
#' @examples
single_asset_Logit_run_and_save_models <-
  function(
    asset_data = Indices_Metals_Bonds[[1]],
    All_Daily_Data = All_Daily_Data,
    Asset_of_interest = "EUR_USD",
    equity_index = equity_index,
    gold_index = gold_index,
    silver_index = silver_index,
    bonds_index = bonds_index,
    interest_rates = interest_rates,
    cpi_data = cpi_data,
    sentiment_index = sentiment_index,
    countries_for_int_strength = c("EUR", "USD"),
    couplua_assets = c("XAU_EUR", "XAG_EUR", "EUR_JPY", "EU50_EUR", "EUR_AUD", "EUR_GBP"),
    pre_train_date_end = "2023-01-01",
    post_train_date_start = "2023-02-01",
    test_date_start = "2025-01-01",
    actual_wins_losses = actual_wins_losses,
    neuron_adjustment = 1.1,
    hidden_layers_var= 2,
    ending_thresh = 0.02,
    trade_direction = "Long",
    stop_value_var = 1.5,
    profit_value_var = 15,
    period_var = 48,
    save_path = "C:/Users/Nikhil Chandra/Documents/trade_data/single_asset_models_v1/"
  ){

    interest_rates_diffs <-
      interest_rates %>%
      dplyr::select(Date_for_Join= Date, contains("_Diff"))

    cpi_data_diffs <-
      cpi_data %>%
      dplyr::select(Date_for_Join = Date, contains("_Diff"))

    interest_rate_strength_Index <-
      get_Interest_Rate_strength(
        interest_rates =interest_rates_diffs %>% mutate(Date = Date_for_Join),
        countries = countries_for_int_strength
      ) %>%
      mutate(Date_for_Join = Date)

    CPI_strength_index <-
      get_CPI_Rate_strength(
        cpi_data =cpi_data_diffs %>% mutate(Date = Date_for_Join),
        countries = countries_for_int_strength
      ) %>%
      mutate(Date_for_Join = Date)

    macro_for_join <-
      asset_data %>%
      distinct(Date) %>%
      mutate(Date_for_Join = as_date(Date)) %>%
      arrange(Date) %>%
      left_join(CPI_strength_index) %>%
      left_join(interest_rate_strength_Index) %>%
      left_join(sentiment_index %>% mutate(Date_for_Join = Date)) %>%
      dplyr::select(-Date_for_Join) %>%
      fill(everything(), .direction = "down") %>%
      filter(if_all(everything(), ~ !is.na(.)))

    indexes_data_for_join <-
      asset_data %>%
      distinct(Date) %>%
      mutate(Date_for_Join = as_date(Date)) %>%
      arrange(Date) %>%
      left_join(equity_index %>% mutate(Date_for_Join = Date) %>% dplyr::select(-Average_PCA) )%>%
      left_join(gold_index %>% mutate(Date_for_Join = Date)  %>% dplyr::select(-Average_PCA) )%>%
      left_join(silver_index %>% mutate(Date_for_Join = Date)  %>% dplyr::select(-Average_PCA) )%>%
      left_join(bonds_index %>% mutate(Date_for_Join = Date)   %>% dplyr::select(-Average_PCA) ) %>%
      dplyr::select(-Date_for_Join)  %>%
      fill(everything(), .direction = "down") %>%
      filter(if_all(everything(), ~ !is.na(.))) %>%
      mutate(
        across(.cols = !contains("Date"), .fns = ~ lag(.))
      ) %>%
      filter(if_all(everything(), ~ !is.na(.)))

    copula_accumulation <- list()

    for (i in 1:length(couplua_assets)) {

      copula_accumulation[[i]] <-
        estimating_dual_copula(
          asset_data_to_use = asset_data,
          asset_to_use = c(Asset_of_interest, couplua_assets[i]),
          price_col = "Open",
          rolling_period = 100,
          samples_for_MLE = 0.15,
          test_samples = 0.85
        ) %>%
        ungroup() %>%
        dplyr::select(Date, contains("_cor")|contains("_lm"))

    }

    actual_wins_losses <-
      actual_wins_losses %>%
      filter(trade_col == trade_direction) %>%
      filter(
        stop_factor == stop_value_var,
        profit_factor == profit_value_var,
        periods_ahead == period_var,
        Asset == Asset_of_interest
      ) %>%
      mutate(
        bin_var =
          case_when(
            trade_start_prices > trade_end_prices & trade_col == "Short" ~ "win",
            trade_start_prices <= trade_end_prices & trade_col == "Short" ~ "loss",

            trade_start_prices < trade_end_prices & trade_col == "Long" ~ "win",
            trade_start_prices >= trade_end_prices & trade_col == "Long" ~ "loss"

          )
      )

    daily_indicator <-
      get_daily_indicators(
        Daily_Data = All_Daily_Data,
        asset_data = asset_data,
        Asset_of_interest = Asset_of_interest
      )

    daily_indicator <-
      daily_indicator %>%
      dplyr::select(-Price, -Open, -Low, -High, -Vol., -Date_for_join) %>%
      mutate(
        across(.cols = !contains("Date") & !contains("Asset"),
               .fns = ~ lag(.))
      ) %>%
      filter(if_all(everything(), ~ !is.na(.))) %>%
      mutate(
        Date = as_datetime(Date, tz = "Australia/Canberra")
      )

    daily_join_model <-
      actual_wins_losses %>%
      dplyr::select(Date, Asset ,bin_var) %>%
      filter(
        Asset == Asset_of_interest
      ) %>%
      left_join(daily_indicator) %>%
      filter(if_all(everything(),~!is.na(.)))

    daily_vars_for_indicator <-
      names(daily_join_model) %>%
      keep(~ !str_detect(.x, "Date") &
             !str_detect(.x, "bin_var") &
             !str_detect(.x, "Asset")) %>%
      unlist() %>%
      as.character()

    daily_indicator_formula <-
      create_lm_formula(dependant = "bin_var=='win'",
                        independant = daily_vars_for_indicator)
    daily_indicator_model <-
      glm(formula = daily_indicator_formula,
          data =
            daily_join_model %>%
            filter(Date <=pre_train_date_end) %>%
            filter( if_all(everything(), ~ !is.infinite(.)) ),
          family = binomial("logit"))
    summary(daily_indicator_model)

    saveRDS(object = daily_indicator_model,
            file =
              glue::glue("{save_path}/{Asset_of_interest}_{trade_direction}_daily_indicator_model.RDS"))

    message("Passed Daily Model")

    daily_indicator_pred <-
      daily_join_model %>%
      mutate(
        daily_indicator_pred = predict.glm(daily_indicator_model,
                                           newdata = daily_join_model, type = "response"),
        mean_daily_pred =
          mean( ifelse(Date <= pre_train_date_end, daily_indicator_pred, NA), na.rm = T ),
        sd_daily_pred =
          sd( ifelse(Date <= pre_train_date_end, daily_indicator_pred, NA), na.rm = T )
      )
    message("Passed Daily Prediction")


    copula_for_join_model <-
      actual_wins_losses %>%
      dplyr::select(Date, Asset ,bin_var) %>%
      filter(
        Asset == Asset_of_interest
      ) %>%
      left_join(copula_accumulation %>%
                  reduce(left_join) ) %>%
      filter(if_all(everything(),~!is.na(.)))

    copula_vars_for_indicator <-
      names(copula_for_join_model) %>%
      keep(~ !str_detect(.x, "Date") &
             !str_detect(.x, "bin_var") &
             !str_detect(.x, "Asset")) %>%
      unlist() %>%
      as.character()

    copula_indicator_formula <-
      create_lm_formula(dependant = "bin_var=='win'",
                        independant = copula_vars_for_indicator)
    copula_indicator_model <-
      glm(formula = copula_indicator_formula,
          data = copula_for_join_model %>% filter(Date <=pre_train_date_end),
          family = binomial("logit"))

    saveRDS(object = copula_indicator_model,
            file =
              glue::glue("{save_path}/{Asset_of_interest}_{trade_direction}_copula_indicator_model.RDS"))

    summary(copula_indicator_model)
    message("Passed Copula Model")

    copula_indicator_pred <-
      copula_for_join_model %>%
      mutate(
        copula_indicator_pred = predict.glm(copula_indicator_model,
                                            newdata = copula_for_join_model, type = "response"),
        mean_copula_pred =
          mean( ifelse(Date <= pre_train_date_end, copula_indicator_pred, NA), na.rm = T ),
        sd_copula_pred =
          sd( ifelse(Date <= pre_train_date_end, copula_indicator_pred, NA), na.rm = T )
      )
    message("Passed Copula Prediction")

    macro_for_join_model <-
      actual_wins_losses %>%
      dplyr::select(Date, Asset ,bin_var) %>%
      filter(
        Asset == Asset_of_interest
      ) %>%
      left_join(macro_for_join) %>%
      filter(if_all(everything(),~!is.na(.)))

    macro_vars_for_indicator <-
      names(macro_for_join_model) %>%
      keep(~ !str_detect(.x, "Date") &
             !str_detect(.x, "bin_var") &
             !str_detect(.x, "Asset")) %>%
      unlist() %>%
      as.character()

    macro_indicator_formula <-
      create_lm_formula(dependant = "bin_var=='win'",
                        independant = macro_vars_for_indicator)
    macro_indicator_model <-
      glm(formula = macro_indicator_formula,
          data = macro_for_join_model %>% filter(Date <=pre_train_date_end),
          family = binomial("logit"))
    summary(macro_indicator_model)

    saveRDS(object = macro_indicator_model,
            file =
              glue::glue("{save_path}/{Asset_of_interest}_{trade_direction}_macro_indicator_model.RDS"))

    message("Passed Macro Model")

    macro_indicator_pred <-
      macro_for_join_model %>%
      mutate(
        macro_indicator_pred = predict.glm(macro_indicator_model,
                                           newdata = macro_for_join_model, type = "response"),
        mean_macro_pred =
          mean( ifelse(Date <= pre_train_date_end, macro_indicator_pred, NA), na.rm = T ),
        sd_macro_pred =
          sd( ifelse(Date <= pre_train_date_end, macro_indicator_pred, NA), na.rm = T )
      )
    message("Passed Macro Pred")

    indexes_for_join_model <-
      actual_wins_losses %>%
      dplyr::select(Date, Asset ,bin_var) %>%
      filter(
        Asset == Asset_of_interest
      ) %>%
      left_join(indexes_data_for_join) %>%
      filter(if_all(everything(),~!is.na(.)))

    indexes_vars_for_indicator <-
      names(indexes_for_join_model) %>%
      keep(~ !str_detect(.x, "Date") &
             !str_detect(.x, "bin_var") &
             !str_detect(.x, "Asset")) %>%
      unlist() %>%
      as.character()

    indexes_indicator_formula <-
      create_lm_formula(dependant = "bin_var=='win'",
                        independant = indexes_vars_for_indicator)
    indexes_indicator_model <-
      glm(formula = indexes_indicator_formula,
          data = indexes_for_join_model %>% filter(Date <=pre_train_date_end),
          family = binomial("logit"))
    summary(indexes_indicator_model)

    saveRDS(object = indexes_indicator_model,
            file =
              glue::glue("{save_path}/{Asset_of_interest}_{trade_direction}_indexes_indicator_model.RDS"))

    message("Passed Indicator Model")

    indexes_indicator_pred <-
      indexes_for_join_model %>%
      mutate(
        indexes_indicator_pred = predict.glm(indexes_indicator_model,
                                             newdata = indexes_for_join_model, type = "response"),
        mean_indexes_pred =
          mean( ifelse(Date <= pre_train_date_end, indexes_indicator_pred, NA), na.rm = T ),
        sd_indexes_pred =
          sd( ifelse(Date <= pre_train_date_end, indexes_indicator_pred, NA), na.rm = T )
      )

    message("Passed Indicator Pred")

    technical_asset_data <- asset_data %>% filter(Asset == Asset_of_interest)
    technical_data <-
      create_technical_indicators(asset_data = technical_asset_data) %>%
      dplyr::select(-Price, -Low, -High, -Open)
    rm(technical_asset_data)

    technical_for_join_model <-
      actual_wins_losses %>%
      dplyr::select(Date, Asset ,bin_var) %>%
      filter(
        Asset == Asset_of_interest
      ) %>%
      left_join(technical_data) %>%
      filter(if_all(everything(),~!is.na(.)))

    technical_vars_for_indicator <-
      names(technical_for_join_model) %>%
      keep(~ !str_detect(.x, "Date") &
             !str_detect(.x, "bin_var") &
             !str_detect(.x, "Asset")) %>%
      unlist() %>%
      as.character()

    technical_indicator_formula <-
      create_lm_formula(dependant = "bin_var=='win'",
                        independant = technical_vars_for_indicator)

    technical_indicator_model <-
      glm(formula = technical_indicator_formula,
          data = technical_for_join_model %>%
            filter(Date <=pre_train_date_end) %>%
            filter( if_all(everything(), ~ !is.infinite(.)) ),
          family = binomial("logit"))

    saveRDS(object = technical_indicator_model,
            file =
              glue::glue("{save_path}/{Asset_of_interest}_{trade_direction}_technical_indicator_model.RDS"))

    message("Passed Technical Model")

    summary(technical_indicator_model)

    technical_indicator_pred <-
      technical_for_join_model %>%
      mutate(
        technical_indicator_pred = predict.glm(technical_indicator_model,
                                               newdata = technical_for_join_model, type = "response"),
        mean_indicator_pred =
          mean( ifelse(Date <= pre_train_date_end, technical_indicator_pred, NA), na.rm = T ),
        sd_indicator_pred =
          sd( ifelse(Date <= pre_train_date_end, technical_indicator_pred, NA), na.rm = T )
      )

    message("Passed Technical Pred")


    combined_indicator_NN <-
      macro_indicator_pred %>%
      dplyr::select(Date, Asset, bin_var,
                    contains("pred") ,
                    contains("PC1"), contains("Sentiment")  ) %>%
      distinct() %>%
      left_join(
        indexes_indicator_pred %>%
          dplyr::select(Date, Asset,
                        contains("pred"),
                        contains("PC1")  ) %>%
          distinct()
      ) %>%
      left_join(copula_indicator_pred %>%
                  dplyr::select(Date, Asset,
                                contains("pred"),
                                contains("cor"),
                                contains("lm")  ) %>%
                  distinct()) %>%
      left_join(
        technical_indicator_pred %>% dplyr::select(-bin_var)
      ) %>%
      left_join(daily_indicator_pred %>% dplyr::select(-bin_var) %>% distinct()) %>%
      janitor::clean_names() %>%
      filter(if_all(everything() ,~!is.na(.))) %>%
      mutate(

        technical_indicator_pred_ma_15 =
          slider::slide_dbl(.x = technical_indicator_pred, .f =~mean(.x, na.rm = T), .before = 10 ),
        technical_indicator_pred_ma_10 =
          slider::slide_dbl(.x = technical_indicator_pred, .f =~mean(.x, na.rm = T), .before = 10 ),
        technical_indicator_pred_ma_5 =
          slider::slide_dbl(.x = technical_indicator_pred, .f =~mean(.x, na.rm = T), .before = 5 ),

        macro_indicator_pred_100=
          slider::slide_dbl(.x = macro_indicator_pred, .f =~mean(.x, na.rm = T), .before = 100 ),
        macro_indicator_pred_50=
          slider::slide_dbl(.x = macro_indicator_pred, .f =~mean(.x, na.rm = T), .before = 50 ),
        macro_indicator_pred_60 =
          slider::slide_dbl(.x = macro_indicator_pred, .f =~mean(.x, na.rm = T), .before = 60 ),

        indexes_indicator_pred_ma_15 =
          slider::slide_dbl(.x = indexes_indicator_pred, .f =~mean(.x, na.rm = T), .before = 15 ),
        indexes_indicator_pred_ma_10 =
          slider::slide_dbl(.x = indexes_indicator_pred, .f =~mean(.x, na.rm = T), .before = 10 ),
        indexes_indicator_pred_ma_5 =
          slider::slide_dbl(.x = indexes_indicator_pred, .f =~mean(.x, na.rm = T), .before = 5 ),

        copula_indicator_pred_ma_15 =
          slider::slide_dbl(.x = copula_indicator_pred, .f =~mean(.x, na.rm = T), .before = 15 ),
        copula_indicator_pred_ma_10 =
          slider::slide_dbl(.x = copula_indicator_pred, .f =~mean(.x, na.rm = T), .before = 10 ),
        copula_indicator_pred_ma_5 =
          slider::slide_dbl(.x = copula_indicator_pred, .f =~mean(.x, na.rm = T), .before = 5 ),

        daily_indicator_pred_ma_15 =
          slider::slide_dbl(.x = daily_indicator_pred, .f =~mean(.x, na.rm = T), .before = 15 ),
        daily_indicator_pred_ma_10 =
          slider::slide_dbl(.x = daily_indicator_pred, .f =~mean(.x, na.rm = T), .before = 10 ),
        daily_indicator_pred_ma_5 =
          slider::slide_dbl(.x = daily_indicator_pred, .f =~mean(.x, na.rm = T), .before = 5 )
      )

    message("Created Combined Model Data")

    NN_indcator_coefs <-
      names(combined_indicator_NN) %>%
      keep(~ !str_detect(.x, "date|asset|bin_var") &
             !(str_detect(.x, "mean_|sd_") & str_detect(.x, "pred"))
      )

    NN_form <-  create_lm_formula(dependant = "bin_var=='win'", independant = NN_indcator_coefs)

    logit_combined_indicator_model <-
      glm(formula = NN_form,
          data = combined_indicator_NN %>%
            filter(
              # date >= (pre_train_date_end - months(12)),
              date <= test_date_start) ,
          family = binomial("logit"))

    saveRDS(object = logit_combined_indicator_model,
            file =
              glue::glue("{save_path}/{Asset_of_interest}_{trade_direction}_logit_combined_indicator_model.RDS"))

    message("Created Combined Model")

    summary(logit_combined_indicator_model)

    logit_combined_pred <-
      combined_indicator_NN %>%
      rename(Date = date, Asset = asset) %>%
      mutate(
        logit_combined_pred = predict.glm(logit_combined_indicator_model,
                                          newdata = combined_indicator_NN, type = "response"),
        mean_logit_combined_pred =
          mean( ifelse(Date <= test_date_start, logit_combined_pred, NA), na.rm = T ),
        sd_logit_combined_pred =
          sd( ifelse(Date <= test_date_start, logit_combined_pred, NA), na.rm = T ),

        averaged_pred =
          indexes_indicator_pred*macro_indicator_pred*indexes_indicator_pred*logit_combined_pred,

        mean_averaged_pred =
          mean( ifelse(Date <= test_date_start, averaged_pred, NA), na.rm = T ),
        sd_averaged_pred =
          sd( ifelse(Date <= test_date_start, averaged_pred, NA), na.rm = T ),

      ) %>%
      dplyr::select(Date, Asset, contains("pred")) %>%
      filter(Date >= test_date_start) %>%
      left_join(
        # actual_wins_losses %>%
        #   dplyr::select(Date, Asset, stop_factor, profit_factor, periods_ahead, trade_col,
        #                 Time_Periods,trade_start_prices, trade_end_prices, trade_return_dollar_aud)

        actual_wins_losses %>%
          dplyr::select(Date, Asset, stop_factor, profit_factor, periods_ahead, trade_col,
                        trade_return_dollar_aud, contains("period_return_"))
      )

    return(logit_combined_pred)

  }

#' Title
#'
#' @param asset_data
#' @param Asset_of_interest
#' @param equity_index
#' @param gold_index
#' @param silver_index
#' @param bonds_index
#' @param interest_rates
#' @param cpi_data
#' @param sentiment_index
#' @param countries_for_int_strength
#' @param couplua_assets
#' @param pre_train_date_end
#' @param post_train_date_start
#' @param test_date_start
#' @param actual_wins_losses
#' @param neuron_adjustment
#' @param hidden_layers_var
#' @param ending_thresh
#' @param trade_direction
#' @param stop_value_var
#' @param profit_value_var
#' @param period_var
#'
#' @return
#' @export
#'
#' @examples
single_asset_Logit_run_local_models <-
  function(
    asset_data = Indices_Metals_Bonds[[1]],
    All_Daily_Data = All_Daily_Data,
    Asset_of_interest = "EUR_USD",
    equity_index = equity_index,
    gold_index = gold_index,
    silver_index = silver_index,
    bonds_index = bonds_index,
    interest_rates = interest_rates,
    cpi_data = cpi_data,
    sentiment_index = sentiment_index,
    countries_for_int_strength = c("EUR", "USD"),
    couplua_assets = c("XAU_EUR", "XAG_EUR", "EUR_JPY", "EU50_EUR", "EUR_AUD", "EUR_GBP"),
    pre_train_date_end = "2023-01-01",
    post_train_date_start = "2023-02-01",
    test_date_start = "2025-01-01",
    neuron_adjustment = 1.1,
    hidden_layers_var= 2,
    ending_thresh = 0.02,
    trade_direction = "Long",
    stop_value_var = 1.5,
    profit_value_var = 15,
    period_var = 48,
    save_path = "C:/Users/Nikhil Chandra/Documents/trade_data/single_asset_models_v1/"
  ){

    interest_rates_diffs <-
      interest_rates %>%
      dplyr::select(Date_for_Join= Date, contains("_Diff"))

    cpi_data_diffs <-
      cpi_data %>%
      dplyr::select(Date_for_Join = Date, contains("_Diff"))

    interest_rate_strength_Index <-
      get_Interest_Rate_strength(
        interest_rates =interest_rates_diffs %>% mutate(Date = Date_for_Join),
        countries = countries_for_int_strength
      ) %>%
      mutate(Date_for_Join = Date)

    CPI_strength_index <-
      get_CPI_Rate_strength(
        cpi_data =cpi_data_diffs %>% mutate(Date = Date_for_Join),
        countries = countries_for_int_strength
      ) %>%
      mutate(Date_for_Join = Date)

    macro_for_join <-
      asset_data %>%
      distinct(Date) %>%
      mutate(Date_for_Join = as_date(Date)) %>%
      arrange(Date) %>%
      left_join(CPI_strength_index) %>%
      left_join(interest_rate_strength_Index) %>%
      left_join(sentiment_index %>% mutate(Date_for_Join = Date)) %>%
      dplyr::select(-Date_for_Join) %>%
      fill(everything(), .direction = "down") %>%
      filter(if_all(everything(), ~ !is.na(.)))

    indexes_data_for_join <-
      asset_data %>%
      distinct(Date) %>%
      mutate(Date_for_Join = as_date(Date)) %>%
      arrange(Date) %>%
      left_join(equity_index %>% mutate(Date_for_Join = Date) %>% dplyr::select(-Average_PCA) )%>%
      left_join(gold_index %>% mutate(Date_for_Join = Date)  %>% dplyr::select(-Average_PCA) )%>%
      left_join(silver_index %>% mutate(Date_for_Join = Date)  %>% dplyr::select(-Average_PCA) )%>%
      left_join(bonds_index %>% mutate(Date_for_Join = Date)   %>% dplyr::select(-Average_PCA) ) %>%
      dplyr::select(-Date_for_Join)  %>%
      fill(everything(), .direction = "down") %>%
      filter(if_all(everything(), ~ !is.na(.))) %>%
      mutate(
        across(.cols = !contains("Date"), .fns = ~ lag(.))
      ) %>%
      filter(if_all(everything(), ~ !is.na(.)))

    copula_accumulation <- list()

    for (i in 1:length(couplua_assets)) {

      copula_accumulation[[i]] <-
        estimating_dual_copula(
          asset_data_to_use = asset_data,
          asset_to_use = c(Asset_of_interest, couplua_assets[i]),
          price_col = "Open",
          rolling_period = 100,
          samples_for_MLE = 0.15,
          test_samples = 0.85
        ) %>%
        ungroup() %>%
        dplyr::select(Date, contains("_cor")|contains("_lm"))

    }


    daily_indicator <-
      get_daily_indicators(
        Daily_Data = All_Daily_Data,
        asset_data = asset_data,
        Asset_of_interest = Asset_of_interest
      )

    daily_indicator <-
      daily_indicator %>%
      dplyr::select(-Price, -Open, -Low, -High, -Vol., -Date_for_join) %>%
      mutate(
        across(.cols = !contains("Date") & !contains("Asset"),
               .fns = ~ lag(.))
      ) %>%
      filter(if_all(everything(), ~ !is.na(.))) %>%
      mutate(
        Date = as_datetime(Date, tz = "Australia/Canberra")
      )

    daily_join_model <-
      daily_indicator %>%
      filter(if_all(everything(),~!is.na(.)))

    daily_vars_for_indicator <-
      names(daily_join_model) %>%
      keep(~ !str_detect(.x, "Date") &
             !str_detect(.x, "bin_var") &
             !str_detect(.x, "Asset")) %>%
      unlist() %>%
      as.character()

    daily_indicator_formula <-
      create_lm_formula(dependant = "bin_var=='win'",
                        independant = daily_vars_for_indicator)
    daily_indicator_model <-
      readRDS(glue::glue("{save_path}/{Asset_of_interest}_{trade_direction}_daily_indicator_model.RDS"))
    summary(daily_indicator_model)

    message("Passed Daily Model")

    daily_indicator_pred <-
      daily_join_model %>%
      mutate(
        daily_indicator_pred = predict.glm(daily_indicator_model,
                                           newdata = daily_join_model, type = "response"),
        mean_daily_pred =
          mean( ifelse(Date <= pre_train_date_end, daily_indicator_pred, NA), na.rm = T ),
        sd_daily_pred =
          sd( ifelse(Date <= pre_train_date_end, daily_indicator_pred, NA), na.rm = T )
      )
    message("Passed Daily Prediction")

    rm(daily_indicator_model)
    gc()


    copula_for_join_model <-
      copula_accumulation %>%
      reduce(left_join) %>%
      filter(if_all(everything(),~!is.na(.)))

    copula_vars_for_indicator <-
      names(copula_for_join_model) %>%
      keep(~ !str_detect(.x, "Date") &
             !str_detect(.x, "bin_var") &
             !str_detect(.x, "Asset")) %>%
      unlist() %>%
      as.character()

    copula_indicator_formula <-
      create_lm_formula(dependant = "bin_var=='win'",
                        independant = copula_vars_for_indicator)

    copula_indicator_model <-
      readRDS(glue::glue("{save_path}/{Asset_of_interest}_{trade_direction}_copula_indicator_model.RDS"))

    summary(copula_indicator_model)
    message("Passed Copula Model")

    copula_indicator_pred <-
      copula_for_join_model %>%
      mutate(
        copula_indicator_pred = predict.glm(copula_indicator_model,
                                            newdata = copula_for_join_model, type = "response"),
        mean_copula_pred =
          mean( ifelse(Date <= pre_train_date_end, copula_indicator_pred, NA), na.rm = T ),
        sd_copula_pred =
          sd( ifelse(Date <= pre_train_date_end, copula_indicator_pred, NA), na.rm = T )
      )
    message("Passed Copula Prediction")

    rm(copula_indicator_model)
    gc()

    macro_for_join_model <-
      macro_for_join %>%
      filter(if_all(everything(),~!is.na(.)))

    macro_vars_for_indicator <-
      names(macro_for_join_model) %>%
      keep(~ !str_detect(.x, "Date") &
             !str_detect(.x, "bin_var") &
             !str_detect(.x, "Asset")) %>%
      unlist() %>%
      as.character()

    macro_indicator_formula <-
      create_lm_formula(dependant = "bin_var=='win'",
                        independant = macro_vars_for_indicator)
    macro_indicator_model <-
      readRDS(glue::glue("{save_path}/{Asset_of_interest}_{trade_direction}_macro_indicator_model.RDS"))

    message("Passed Macro Model")

    macro_indicator_pred <-
      macro_for_join_model %>%
      mutate(
        macro_indicator_pred = predict.glm(macro_indicator_model,
                                           newdata = macro_for_join_model, type = "response"),
        mean_macro_pred =
          mean( ifelse(Date <= pre_train_date_end, macro_indicator_pred, NA), na.rm = T ),
        sd_macro_pred =
          sd( ifelse(Date <= pre_train_date_end, macro_indicator_pred, NA), na.rm = T )
      )
    message("Passed Macro Pred")

    rm(macro_indicator_model)
    gc()

    indexes_for_join_model <-
      indexes_data_for_join %>%
      filter(if_all(everything(),~!is.na(.)))

    indexes_vars_for_indicator <-
      names(indexes_for_join_model) %>%
      keep(~ !str_detect(.x, "Date") &
             !str_detect(.x, "bin_var") &
             !str_detect(.x, "Asset")) %>%
      unlist() %>%
      as.character()

    indexes_indicator_formula <-
      create_lm_formula(dependant = "bin_var=='win'",
                        independant = indexes_vars_for_indicator)
    indexes_indicator_model <-
      readRDS(glue::glue("{save_path}/{Asset_of_interest}_{trade_direction}_indexes_indicator_model.RDS"))

    message("Passed Indicator Model")

    indexes_indicator_pred <-
      indexes_for_join_model %>%
      mutate(
        indexes_indicator_pred = predict.glm(indexes_indicator_model,
                                             newdata = indexes_for_join_model, type = "response"),
        mean_indexes_pred =
          mean( ifelse(Date <= pre_train_date_end, indexes_indicator_pred, NA), na.rm = T ),
        sd_indexes_pred =
          sd( ifelse(Date <= pre_train_date_end, indexes_indicator_pred, NA), na.rm = T )
      )

    message("Passed Indicator Pred")
    rm(indexes_indicator_model)
    gc()

    technical_asset_data <- asset_data %>% filter(Asset == Asset_of_interest)
    technical_data <-
      create_technical_indicators(asset_data = technical_asset_data) %>%
      dplyr::select(-Price, -Low, -High, -Open)
    rm(technical_asset_data)

    technical_for_join_model <-
      technical_data %>%
      filter(if_all(everything(),~!is.na(.)))

    technical_vars_for_indicator <-
      names(technical_for_join_model) %>%
      keep(~ !str_detect(.x, "Date") &
             !str_detect(.x, "bin_var") &
             !str_detect(.x, "Asset")) %>%
      unlist() %>%
      as.character()

    technical_indicator_formula <-
      create_lm_formula(dependant = "bin_var=='win'",
                        independant = technical_vars_for_indicator)

    technical_indicator_model <-
      readRDS(glue::glue("{save_path}/{Asset_of_interest}_{trade_direction}_technical_indicator_model.RDS"))

    message("Passed Technical Model")

    summary(technical_indicator_model)

    technical_indicator_pred <-
      technical_for_join_model %>%
      mutate(
        technical_indicator_pred = predict.glm(technical_indicator_model,
                                               newdata = technical_for_join_model, type = "response"),
        mean_indicator_pred =
          mean( ifelse(Date <= pre_train_date_end, technical_indicator_pred, NA), na.rm = T ),
        sd_indicator_pred =
          sd( ifelse(Date <= pre_train_date_end, technical_indicator_pred, NA), na.rm = T )
      )

    message("Passed Technical Pred")
    rm(technical_indicator_model)
    gc()

    # max_dates_macro <- macro_indicator_pred %>% ungroup() %>% slice_max(Date) %>% pull(Date)
    # max_dates_index <- indexes_indicator_pred %>% ungroup() %>% slice_max(Date) %>% pull(Date)
    # max_dates_copula <- copula_indicator_pred %>% ungroup() %>% slice_max(Date) %>% pull(Date)

    combined_indicator_NN <-
      macro_indicator_pred %>%
      mutate(Asset = Asset_of_interest) %>%
      # mutate(Date = as_datetime(Date)) %>%
      dplyr::select(Date, Asset,
                    contains("pred") ,
                    contains("PC1"), contains("Sentiment")  ) %>%
      distinct() %>%
      left_join(
        indexes_indicator_pred %>%
          mutate(Asset = Asset_of_interest) %>%
          # mutate(Date = as_datetime(Date)) %>%
          dplyr::select(Date, Asset,
                        contains("pred"),
                        contains("PC1")  ) %>%
          distinct()
      ) %>%
      left_join(copula_indicator_pred %>%
                  mutate(Asset = Asset_of_interest) %>%
                  # mutate(Date = as_datetime(Date)) %>%
                  dplyr::select(Date, Asset,
                                contains("pred"),
                                contains("cor"),
                                contains("lm")  ) %>%
                  distinct()) %>%
      # fill(contains("pred")|contains("cor")|contains("lm")|contains("copula_pred"), .direction = "down") %>%
      left_join(
        technical_indicator_pred %>%
          mutate(Asset = Asset_of_interest) %>%
          # mutate(Date = as_datetime(Date)) %>%
          distinct()
      ) %>%
      left_join(daily_indicator_pred %>%
                  mutate(Asset = Asset_of_interest) %>%
                  # mutate(Date = as_datetime(Date)) %>%
                  distinct()
      ) %>%
      # fill(contains("daily_indicator_pred")|contains("Daily_")|contains("daily_"), .direction = "down") %>%
      janitor::clean_names() %>%
      filter(if_all(everything() ,~!is.na(.))) %>%
      mutate(

        technical_indicator_pred_ma_15 =
          slider::slide_dbl(.x = technical_indicator_pred, .f =~mean(.x, na.rm = T), .before = 10 ),
        technical_indicator_pred_ma_10 =
          slider::slide_dbl(.x = technical_indicator_pred, .f =~mean(.x, na.rm = T), .before = 10 ),
        technical_indicator_pred_ma_5 =
          slider::slide_dbl(.x = technical_indicator_pred, .f =~mean(.x, na.rm = T), .before = 5 ),

        macro_indicator_pred_100=
          slider::slide_dbl(.x = macro_indicator_pred, .f =~mean(.x, na.rm = T), .before = 100 ),
        macro_indicator_pred_50=
          slider::slide_dbl(.x = macro_indicator_pred, .f =~mean(.x, na.rm = T), .before = 50 ),
        macro_indicator_pred_60 =
          slider::slide_dbl(.x = macro_indicator_pred, .f =~mean(.x, na.rm = T), .before = 60 ),

        indexes_indicator_pred_ma_15 =
          slider::slide_dbl(.x = indexes_indicator_pred, .f =~mean(.x, na.rm = T), .before = 15 ),
        indexes_indicator_pred_ma_10 =
          slider::slide_dbl(.x = indexes_indicator_pred, .f =~mean(.x, na.rm = T), .before = 10 ),
        indexes_indicator_pred_ma_5 =
          slider::slide_dbl(.x = indexes_indicator_pred, .f =~mean(.x, na.rm = T), .before = 5 ),

        copula_indicator_pred_ma_15 =
          slider::slide_dbl(.x = copula_indicator_pred, .f =~mean(.x, na.rm = T), .before = 15 ),
        copula_indicator_pred_ma_10 =
          slider::slide_dbl(.x = copula_indicator_pred, .f =~mean(.x, na.rm = T), .before = 10 ),
        copula_indicator_pred_ma_5 =
          slider::slide_dbl(.x = copula_indicator_pred, .f =~mean(.x, na.rm = T), .before = 5 ),

        daily_indicator_pred_ma_15 =
          slider::slide_dbl(.x = daily_indicator_pred, .f =~mean(.x, na.rm = T), .before = 15 ),
        daily_indicator_pred_ma_10 =
          slider::slide_dbl(.x = daily_indicator_pred, .f =~mean(.x, na.rm = T), .before = 10 ),
        daily_indicator_pred_ma_5 =
          slider::slide_dbl(.x = daily_indicator_pred, .f =~mean(.x, na.rm = T), .before = 5 )
      )

    message("Created Combined Model Data")

    NN_indcator_coefs <-
      names(combined_indicator_NN) %>%
      keep(~ !str_detect(.x, "date|asset|bin_var") &
             !(str_detect(.x, "mean_|sd_") & str_detect(.x, "pred"))
      )

    NN_form <-  create_lm_formula(dependant = "bin_var=='win'", independant = NN_indcator_coefs)

    logit_combined_indicator_model <-
      readRDS(glue::glue("{save_path}/{Asset_of_interest}_{trade_direction}_logit_combined_indicator_model.RDS"))

    message("Created Combined Model")

    summary(logit_combined_indicator_model)

    logit_combined_pred <-
      combined_indicator_NN %>%
      rename(Date = date, Asset = asset) %>%
      mutate(
        logit_combined_pred = predict.glm(logit_combined_indicator_model,
                                          newdata = combined_indicator_NN, type = "response"),
        mean_logit_combined_pred =
          mean( ifelse(Date <= test_date_start, logit_combined_pred, NA), na.rm = T ),
        sd_logit_combined_pred =
          sd( ifelse(Date <= test_date_start, logit_combined_pred, NA), na.rm = T ),

        averaged_pred =
          indexes_indicator_pred*macro_indicator_pred*indexes_indicator_pred*logit_combined_pred,

        mean_averaged_pred =
          mean( ifelse(Date <= test_date_start, averaged_pred, NA), na.rm = T ),
        sd_averaged_pred =
          sd( ifelse(Date <= test_date_start, averaged_pred, NA), na.rm = T ),

      ) %>%
      dplyr::select(Date, Asset, contains("pred")) %>%
      slice_max(Date)

    rm(logit_combined_indicator_model)
    gc()

    return(logit_combined_pred)

  }


#' single_asset_model_loop_and_trade
#'
#' @param Indices_Metals_Bonds
#' @param All_Daily_Data
#' @param pre_train_date_end
#' @param post_train_date_start
#' @param test_date_start
#' @param test_end_date
#' @param raw_macro_data
#'
#' @return
#' @export
#'
#' @examples
single_asset_model_loop_and_trade <-
  function(
    Indices_Metals_Bonds = Indices_Metals_Bonds,
    All_Daily_Data =
      get_DAILY_ALGO_DATA_API_REQUEST(),
    pre_train_date_end = today() - months(12),
    post_train_date_start = today() - months(12),
    test_date_start = today() - week(1),
    test_end_date = today() + week(1),
    raw_macro_data = raw_macro_data,
    stop_value_var = 1.5,
    profit_value_var = 15,
    period_var = 48,
    start_index = 1,
    end_index = 20,
    save_path = "C:/Users/Nikhil Chandra/Documents/trade_data/single_asset_models_v1/"
  ) {

    #-------------Indicator Inputs

    equity_index <-
      get_equity_index(index_data = Indices_Metals_Bonds)

    gold_index <-
      get_Gold_index(index_data = Indices_Metals_Bonds)

    silver_index <-
      get_silver_index(index_data = Indices_Metals_Bonds)

    bonds_index <-
      get_bonds_index(index_data = Indices_Metals_Bonds)

    interest_rates <-
      get_interest_rates(
        raw_macro_data = raw_macro_data,
        lag_days = 1
      )

    cpi_data <-
      get_cpi(
        raw_macro_data = raw_macro_data,
        lag_days = 1
      )

    sentiment_index <-
      create_sentiment_index(
        raw_macro_data = raw_macro_data,
        lag_days = 1,
        date_start = "2011-01-01",
        end_date = today() %>% as.character(),
        first_difference = TRUE,
        scale_values = FALSE
      )

    indicator_mapping <- list(
      Asset = c("EUR_USD", #1
                "EU50_EUR", #2
                "SPX500_USD", #3
                "US2000_USD", #4
                "USB10Y_USD", #5
                "USD_JPY", #6
                "AUD_USD", #7
                "EUR_GBP", #8
                "AU200_AUD" ,#9
                "EUR_AUD", #10
                "WTICO_USD", #11
                "UK100_GBP", #12
                "USD_CAD", #13
                "GBP_USD", #14
                "GBP_CAD", #15
                "EUR_JPY", #16
                "EUR_NZD", #17
                "XAG_USD", #18
                "XAG_EUR", #19
                "XAG_AUD", #20
                "XAG_NZD", #21
                "HK33_HKD", #22
                "FR40_EUR", #23
                "BTC_USD", #24
                "XAG_GBP", #25
                "GBP_AUD", #26
                "USD_SEK", #27
                "USD_SGD" #28
      ),
      couplua_assets =
        list(
          # EUR_USD
          c("XAU_EUR", "XAG_EUR", "EUR_JPY", "EU50_EUR", "EUR_AUD", "EUR_GBP",
            "SPX500_USD", "XAU_USD", "USD_JPY", "GBP_USD", "EUR_NZD", "XAG_GBP", "XAU_GBP",
            "EUR_SEK", "USD_CAD") %>% unique(), #1

          # EU50_EUR
          c("XAU_EUR", "XAG_EUR", "XAU_USD", "UK100_GBP", "SG30_SGD", "EUR_GBP", "SPX500_USD",
            "SPX500_USD", "XAU_USD", "AU200_AUD", "CH20_CHF", "US2000_USD",
            "XAG_GBP", "XAU_GBP", "WTICO_USD", "FR40_EUR", "HK33_HKD") %>% unique(), #2

          # SPX500_USD
          c("US2000_USD", "AU200_AUD", "USB10Y_USD", "UK100_GBP", "XAU_USD", "EU50_EUR",
            "HK33_HKD", "FR40_EUR", "WTICO_USD", "USD_JPY", "EUR_USD", "GBP_USD", "AU200_AUD",
            "SG30_SGD", "XAU_EUR", "XAG_EUR", "XAG_GBP", "XAU_GBP", "XAG_USD" ) %>% unique(), #3

          # US2000_USD
          c("SPX500_USD",  "AU200_AUD", "USB10Y_USD", "UK100_GBP", "XAU_USD", "EU50_EUR",
            "HK33_HKD", "FR40_EUR", "WTICO_USD", "USD_JPY", "EUR_USD", "GBP_USD", "AU200_AUD",
            "SG30_SGD", "XAU_EUR", "XAG_EUR", "XAG_GBP", "XAU_GBP","XAG_USD" ) %>% unique(), #4

          # USB10Y_USD
          c("SPX500_USD",  "AU200_AUD", "UK100_GBP", "XAU_USD", "EU50_EUR",
            "HK33_HKD", "FR40_EUR", "WTICO_USD", "USD_JPY", "EUR_USD",
            "XAU_EUR", "AU200_AUD", "XAG_USD",
            "SG30_SGD", "XAU_EUR", "XAG_EUR", "XAG_GBP", "XAU_GBP") %>% unique(), #5

          # USD_JPY
          c("EUR_JPY", "XAU_JPY", "XAG_JPY", "GBP_JPY", "XAU_USD", "SPX500_USD",
            "XAG_USD","NZD_USD", "AUD_USD", "EUR_USD", "GBP_USD", "USD_CAD",
            "USD_SEK", "USD_SGD", "USB10Y_USD") %>% unique(), #6

          # AUD_USD
          c("XCU_USD", "AU200_AUD", "XAU_AUD", "GBP_AUD", "XAU_USD", "EUR_AUD",
            "XAG_USD","NZD_USD", "USD_JPY", "EUR_USD", "GBP_USD", "USD_CAD",
            "USD_SEK", "USD_SGD", "USB10Y_USD", "NZD_USD") %>% unique(), #7

          # EUR_GBP
          c("GBP_USD", "EUR_USD", "XAU_EUR", "XAU_GBP", "GBP_JPY", "EUR_JPY",
            "XAG_EUR", "XAG_GBP", "USD_JPY", "UK100_GBP", "FR40_EUR", "EU50_EUR",
            "EUR_SEK", "USD_SEK", "EUR_AUD", "EUR_NZD", "EUR_SEK") %>% unique(), #8

          # AU200_AUD
          c("XCU_USD", "US2000_USD", "UK100_GBP", "XAU_USD", "EU50_EUR",
            "HK33_HKD", "FR40_EUR", "WTICO_USD", "GBP_AUD", "EUR_AUD",
            "SG30_SGD", "XAU_EUR", "XAG_EUR", "XAG_GBP", "XAU_GBP", "XAG_USD" ) %>% unique(), #9

          # EUR_AUD
          c("XCU_USD", "AU200_AUD", "XAU_AUD", "GBP_AUD", "XAU_USD", "AUD_USD",
            "XAG_USD","NZD_USD", "USD_JPY", "EUR_USD",
            "USB10Y_USD", "NZD_USD", "FR40_EUR", "EU50_EUR",
            "EUR_SEK", "EUR_NZD", "EUR_SEK") %>% unique(), #10

          # WTICO_USD
          c("NATGAS_USD", "XAG_USD", "BCO_USD", "SPX500_USD", "UK10YB_GBP", "XAU_USD",
            "US2000_USD", "AU200_AUD", "USB10Y_USD", "UK100_GBP", "XAU_USD", "EU50_EUR",
            "HK33_HKD", "FR40_EUR", "USD_JPY", "EUR_USD", "GBP_USD",
            "XAU_EUR", "XAG_EUR", "XAG_GBP", "XAU_GBP") %>% unique(), #11

          # "UK100_GBP", #12
          c("XAU_EUR", "XAG_EUR", "XAU_USD", "SG30_SGD", "EUR_GBP", "US2000_USD",
            "SPX500_USD", "XAU_USD", "AU200_AUD", "CH20_CHF", "UK10YB_GBP", "USB10Y_USD",
            "XAG_GBP", "XAU_GBP", "WTICO_USD", "FR40_EUR", "HK33_HKD") %>% unique(), #12

          # "USD_CAD", #13
          c("XAU_JPY", "XAU_GBP", "XAU_EUR", "XAU_USD", "EUR_JPY", "GBP_JPY",
            "XAG_USD","NZD_USD", "USD_JPY", "EUR_USD", "GBP_USD", "GBP_CAD",
            "USD_SEK", "USD_SGD", "USB10Y_USD") %>% unique(), #13

          # "GBP_USD", #14
          c("GBP_JPY", "GBP_CAD", "GBP_AUD", "GBP_NZD", "XAU_GBP", "XAG_GBP", "UK100_GBP",
            "XAU_USD", "XAG_USD", "EUR_GBP", "EUR_USD", "XAG_EUR", "XAU_EUR", "USD_JPY",
            "EUR_JPY", "UK10YB_GBP", "AUD_USD", "USD_SEK", "USD_CAD") %>% unique(), #14

          # "GBP_CAD", #15
          c("GBP_JPY", "GBP_USD", "GBP_AUD", "GBP_NZD", "XAU_GBP", "XAG_GBP", "UK100_GBP",
            "XAU_USD", "XAG_USD", "EUR_GBP", "EUR_USD", "XAG_EUR", "XAU_EUR", "USD_JPY",
            "EUR_JPY", "UK10YB_GBP", "AUD_USD", "USD_SEK", "USD_CAD") %>% unique(), #15

          # "EUR_JPY", #16
          c("GBP_USD", "EUR_USD", "XAU_EUR", "XAU_JPY", "USD_JPY", "EUR_AUD",
            "EUR_GBP", "EUR_NZD", "EUR_SEK", "XAG_EUR", "XAU_USD", "XAG_USD", "USD_JPY",
            "GBP_JPY", "FR40_EUR", "EU50_EUR") %>% unique(), #16

          # "EUR_NZD", #17
          c("EUR_AUD", "EUR_USD", "XAU_EUR", "XAU_AUD", "NZD_USD", "EUR_JPY", "EUR_GBP",
            "GBP_NZD", "XAG_NZD", "XAG_EUR", "XAU_USD", "XAG_USD", "EUR_SEK",
            "FR40_EUR", "EU50_EUR", "AU200_AUD") %>% unique(), #17

          # "XAG_USD", #18
          c("XAG_JPY", "XAG_GBP", "XAG_EUR", "XAG_AUD", "XAU_USD", "EU50_EUR", "SPX500_USD",
            "XAG_NZD", "XAU_AUD", "XAU_GBP", "XAU_JPY", "XAU_EUR", "EUR_USD", "USD_JPY",
            "GBP_USD", "AUD_USD", "USD_CAD", "USD_SEK") %>% unique(), #18

          # "XAG_EUR", #19
          c("XAG_JPY", "XAG_GBP", "XAG_USD", "XAG_AUD", "XAU_USD", "EU50_EUR", "SPX500_USD",
            "XAG_NZD", "XAU_AUD", "XAU_GBP", "XAU_JPY", "XAU_EUR", "EUR_USD", "EUR_JPY",
            "EUR_GBP", "EUR_AUD", "EUR_SEK", "EUR_NZD") %>% unique(), #19

          # "XAG_AUD", #20
          c("XAG_JPY", "XAG_GBP", "XAG_USD", "XAG_EUR", "XAU_USD", "EU50_EUR", "SPX500_USD",
            "XAG_NZD", "XAU_AUD", "XAU_GBP", "XAU_JPY", "XAU_EUR", "AU200_AUD",
            "AUD_USD", "EUR_AUD", "GBP_AUD") %>% unique(), #20

          # "XAG_NZD", #21
          c("XAG_JPY", "XAG_GBP", "XAG_USD", "XAG_EUR", "XAU_USD", "EU50_EUR", "SPX500_USD",
            "XAG_AUD", "XAU_AUD", "XAU_GBP", "XAU_JPY", "XAU_EUR", "AU200_AUD",
            "NZD_USD", "GBP_NZD", "EUR_NZD") %>% unique(), #21

          # "HK33_HKD", #22
          c("US2000_USD", "AU200_AUD", "USB10Y_USD", "UK100_GBP", "XAU_USD", "EU50_EUR",
            "SPX500_USD", "FR40_EUR", "WTICO_USD", "USD_JPY", "EUR_USD", "GBP_USD", "AU200_AUD",
            "SG30_SGD", "XAU_EUR", "XAG_EUR", "XAG_GBP", "XAU_GBP", "XAG_USD") %>% unique(), #22

          c("UK100_GBP", "EU50_EUR", "XAG_USD", "AU200_AUD",
            "XAU_USD", "USB10Y_USD", "SPX500_USD") %>% unique(), #23

          # "BTC_USD", #24
          c("US2000_USD", "AU200_AUD", "USB10Y_USD", "UK100_GBP", "XAU_USD", "EU50_EUR",
            "HK33_HKD", "FR40_EUR", "WTICO_USD", "USD_JPY", "EUR_USD", "GBP_USD", "AU200_AUD",
            "SG30_SGD", "XAU_EUR", "XAG_EUR", "XAG_GBP", "XAU_GBP", "XAG_USD" ) %>% unique(), #24

          c("UK100_GBP", "XAU_GBP", "XAG_USD", "XAG_EUR", "XAU_USD",
            "XAG_AUD", "SPX500_USD") %>% unique(), #25

          c("AUD_USD", "EUR_AUD", "XAG_USD", "XAG_GBP",
            "XAU_USD", "XAG_AUD", "GBP_JPY") %>% unique(), #26

          c("AUD_USD", "EUR_USD", "GBP_USD", "USD_JPY",
            "XAU_USD", "USD_CAD", "NZD_USD") %>% unique(), #27

          c("AUD_USD", "EUR_USD", "GBP_USD", "USD_JPY",
            "XAU_USD", "USD_CAD", "NZD_USD") %>% unique() #28

        ),
      countries_for_int_strength =
        list(
          c("EUR", "USD"), #1
          c("EUR", "USD"), #2
          c("EUR", "USD", "JPY"), #3
          c("EUR", "USD", "JPY"), #4
          c("EUR", "USD", "JPY"), #5
          c("EUR", "USD", "JPY"), #6
          c("AUD", "USD", "EUR"), #7
          c("GBP", "USD", "EUR", "JPY"), #8
          c("AUD", "USD", "EUR"), #9
          c("AUD", "USD", "EUR"), #10
          c("GBP", "USD", "EUR", "AUD"), #11
          c("GBP", "USD", "EUR", "AUD"), #12
          c("GBP", "USD", "EUR", "AUD"), #13
          c("GBP", "USD", "EUR", "AUD"), #14
          c("GBP", "USD", "EUR", "JPY"), #15
          c("GBP", "USD", "EUR", "AUD"), #16

          c("GBP", "USD", "EUR", "AUD", "JPY"), #17
          c("GBP", "USD", "EUR", "AUD", "JPY"), #18
          c("GBP", "USD", "EUR", "AUD", "JPY"), #19
          c("GBP", "USD", "EUR", "AUD", "JPY"), #20
          c("GBP", "USD", "EUR", "AUD", "JPY"), #21
          c("GBP", "USD", "EUR", "AUD", "JPY"), #22
          c("GBP", "USD", "EUR", "AUD", "JPY"), #23
          c("GBP", "USD", "EUR", "AUD", "JPY"), #24
          c("GBP", "USD", "EUR", "AUD", "JPY"), #25
          c("GBP", "USD", "EUR", "AUD", "JPY"), #26
          c("GBP", "USD", "EUR", "AUD", "JPY"), #27
          c("GBP", "USD", "EUR", "AUD", "JPY") #28
        )
    )


    safely_find_preds <-
      safely(single_asset_Logit_run_local_models, otherwise = NULL)

    accumulator <- list()
    gc()

    for (j in start_index:end_index ) {

      tictoc::tic()
      countries_for_int_strength <-
        unlist(indicator_mapping$countries_for_int_strength[j])
      couplua_assets = unlist(indicator_mapping$couplua_assets[j])
      Asset_of_interest = unlist(indicator_mapping$Asset[j])

      message(Asset_of_interest)
      message(j)

      longs <-
        safely_find_preds(
          asset_data = Indices_Metals_Bonds,
          All_Daily_Data = All_Daily_Data,
          Asset_of_interest = Asset_of_interest,
          equity_index = equity_index,
          gold_index = gold_index,
          silver_index = silver_index,
          bonds_index = bonds_index,
          interest_rates = interest_rates,
          cpi_data = cpi_data,
          sentiment_index = sentiment_index,
          countries_for_int_strength = countries_for_int_strength,
          couplua_assets = couplua_assets,
          pre_train_date_end = pre_train_date_end,
          post_train_date_start = post_train_date_start,
          test_date_start = test_date_start,
          neuron_adjustment = 1.1,
          hidden_layers_var= 2,
          ending_thresh = 0.02,
          trade_direction = "Long",
          stop_value_var = stop_value_var,
          profit_value_var = profit_value_var,
          period_var = period_var,
          save_path = save_path
        ) %>%
        pluck('result') %>%
        mutate(
          trade_col = "Long"
        )

      # shorts <-
      #   safely_find_preds(
      #     asset_data = Indices_Metals_Bonds,
      #     All_Daily_Data = All_Daily_Data,
      #     Asset_of_interest = Asset_of_interest,
      #     equity_index = equity_index,
      #     gold_index = gold_index,
      #     silver_index = silver_index,
      #     bonds_index = bonds_index,
      #     interest_rates = interest_rates,
      #     cpi_data = cpi_data,
      #     sentiment_index = sentiment_index,
      #     countries_for_int_strength = countries_for_int_strength,
      #     couplua_assets = couplua_assets,
      #     pre_train_date_end = pre_train_date_end,
      #     post_train_date_start = post_train_date_start,
      #     test_date_start = test_date_start,
      #     neuron_adjustment = 1.1,
      #     hidden_layers_var= 2,
      #     ending_thresh = 0.02,
      #     trade_direction = "Short",
      #     stop_value_var = stop_value_var,
      #     profit_value_var = profit_value_var,
      #     period_var = period_var,
      #     save_path = save_path) %>%
      #   pluck('result') %>%
      #   mutate(
      #     trade_col = "Short"
      #   )

      shorts <- NULL

      accumulator[[j]] <-
        list(longs, shorts) %>%
        map_dfr(bind_rows)

      rm(longs, shorts)
      gc()
      Sys.sleep(1)
      gc()

      tictoc::toc()

    }

    accumulator_dfr <-
      accumulator %>%
      map_dfr(bind_rows) %>%
      group_by(Asset) %>%
      slice_max(Date) %>%
      mutate(
        periods_ahead = period_var,
        stop_factor = stop_value_var,
        profit_factor = profit_value_var
      ) %>%
      ungroup()

    return(accumulator_dfr)

  }


#' get_best_trade_setup_sa
#'
#' @param model_optimisation_store_path
#' @param table_to_extract
#'
#' @returns
#' @export
#'
#' @examples
get_best_trade_setup_sa <-
  function(
    model_optimisation_store_path =
      "C:/Users/nikhi/Documents/trade_data/single_asset_advanced_optimisation.db",
    table_to_extract = "summary_for_reg"
  ) {

    model_data_store_db <-
      connect_db(model_optimisation_store_path)

    summary_results <-
      DBI::dbGetQuery(conn = model_data_store_db,
                      statement =
                        glue::glue("SELECT * FROM {table_to_extract}") )

    DBI::dbDisconnect(model_data_store_db)

    all_assets <-
      summary_results %>%
      pull(Asset) %>%
      unique()

    summary_results <-
      summary_results %>%
      mutate(
        win_loss_perc =
          wins_or_loss_3_dollar_min/total_trades
      )

    best <-
      summary_results %>%
      filter(Total_Return > 0, return_25 > 0) %>%
      filter(
        profit_factor_long > stop_factor_long
      ) %>%
      group_by(Asset) %>%
      slice_max(win_loss_perc, n = 4) %>%
      group_by(Asset) %>%
      slice_max(Total_Return) %>%
      distinct()

    best_return_only <-
      summary_results %>%
      filter(Total_Return > 0, return_25 > 0) %>%
      group_by(Asset) %>%
      slice_max(Total_Return_worst_run) %>%
      group_by(Asset) %>%
      slice_max(Total_Return) %>%
      distinct()

    best_params_average <-
      best %>%
      ungroup() %>%
      summarise(
        stop_factor_short = mean(stop_factor_short, na.rm = T),
        profit_factor_short = mean(profit_factor_short, na.rm = T),
        period_var_short = mean(period_var_short, na.rm = T),
        profit_factor_long = mean(profit_factor_long, na.rm = T),
        stop_factor_long = mean(stop_factor_long ,na.rm = T),
        profit_factor_long_fast = mean(profit_factor_long_fast, na.rm = T),
        profit_factor_long_fastest = mean(profit_factor_long_fastest, na.rm = T),
        period_var_long = mean(period_var_long, na.rm = T),
        Total_Return = mean(Total_Return, na.rm = T)
      )

    param_comp_lm <-
      lm(data =
           summary_results %>%
           mutate(
             profit_factor_long_fast_2 = profit_factor_long_fast^2,
             profit_factor_long_fastest_2 = profit_factor_long_fastest^2
           ),
         formula = Total_Return ~
           stop_factor_short +
           stop_factor_short_2 +
           profit_factor_short +
           profit_factor_short_2 +
           period_var_short +
           period_var_short_2 +
           profit_factor_long +
           profit_factor_long_2 +
           stop_factor_long +
           stop_factor_long_2 +
           profit_factor_long_fast +
           profit_factor_long_fastest +
           period_var_long +
           Asset
      )

    summary(param_comp_lm)

    param_tibble <-
      c(18,15,12,9,6,2) %>%
      map_dfr(
        ~
          tibble(
            stop_factor_long = c(4,2,6, 8)
          ) %>%
          mutate(
            profit_factor_long = .x,
            profit_factor_long_fast = round(.x/2),
            profit_factor_long_fastest = (.x/3)
          )

      ) %>%
      mutate(
        kk = row_number()
      ) %>%
      split(.$kk, drop = FALSE) %>%
      map_dfr(
        ~
          tibble(
            stop_factor_short = c(3,4,3,2,3,6,6),
            profit_factor_short = c(6,12,1,1,0.5,3,1)
          ) %>%
          bind_cols(.x)
      ) %>%
      mutate(
        kk = row_number()
      ) %>%
      split(.$kk, drop = FALSE) %>%
      map_dfr(
        ~ tibble(
          period_var_long = rep(c(30,24,18,12), 6),
          period_var_short = rep(c(14,12,8,6,4,2), 4)
        ) %>%
          bind_cols(.x)
      ) %>%
      bind_rows(
        c(18) %>%
          map_dfr(
            ~
              tibble(
                # stop_factor_long = c(4,2,6, 8)
                stop_factor_long = c(6)
              ) %>%
              mutate(
                profit_factor_long = .x,
                profit_factor_long_fast = round(.x/4),
                profit_factor_long_fastest = floor((.x/7))
              )

          ) %>%
          mutate(
            kk = row_number()
          ) %>%
          split(.$kk, drop = FALSE) %>%
          map_dfr(
            ~
              tibble(
                stop_factor_short = c(3,4,3,2,3,6,6),
                profit_factor_short = c(6,12,1,1,0.5,3,1)
              ) %>%
              bind_cols(.x)
          ) %>%
          mutate(
            kk = row_number()
          ) %>%
          split(.$kk, drop = FALSE) %>%
          map_dfr(
            ~ tibble(
              period_var_long = rep(c(30,24,18,12), 6),
              period_var_short = rep(c(14,12,8,6,4,2), 4)
            ) %>%
              bind_cols(.x)
          )

      )

    param_tibble <-
      all_assets %>%
      map_dfr(
        ~
          param_tibble %>%
          mutate(Asset = .x)
      )


    predicted_outcomes <-
      predict(object = param_comp_lm,
              newdata = param_tibble %>%
                mutate(
                  stop_factor_short_2 = stop_factor_short^2,
                  profit_factor_short_2 = profit_factor_short^2,
                  period_var_short_2 = period_var_short^2,
                  profit_factor_long_2 = profit_factor_long^2,
                  stop_factor_long_2 = stop_factor_long^2
                )
      )

    predicted_outcomes <-
      param_tibble %>%
      mutate(
        stop_factor_short_2 = stop_factor_short^2,
        profit_factor_short_2 = profit_factor_short^2,
        period_var_short_2 = period_var_short^2,
        profit_factor_long_2 = profit_factor_long^2,
        stop_factor_long_2 = stop_factor_long^2
      ) %>%
      mutate(
        predicted_values = predicted_outcomes
      )

    best_prediced_outcome <-
      predicted_outcomes %>%
      group_by(Asset) %>%
      slice_max(predicted_values) %>%
      ungroup()

    best_best_prediced_outcome2 <-
      best_prediced_outcome %>%
      dplyr::select(
        Asset,
        stop_factor_short ,
        profit_factor_short ,
        period_var_short ,
        profit_factor_long,
        stop_factor_long,
        profit_factor_long_fast,
        profit_factor_long_fastest,
        period_var_long,
        Total_Return = predicted_values
      ) %>%
      distinct()

    final_results <-
      best %>%
      dplyr::select(
        Asset,
        stop_factor_short ,
        profit_factor_short ,
        period_var_short ,
        profit_factor_long,
        stop_factor_long,
        profit_factor_long_fast,
        profit_factor_long_fastest,
        period_var_long,
        Total_Return,
        wins_or_loss_3_dollar_min
      )
    # bind_rows(
    #   best_return_only %>%
    #     dplyr::select(
    #       Asset,
    #       stop_factor_short ,
    #       profit_factor_short ,
    #       period_var_short ,
    #       profit_factor_long,
    #       stop_factor_long,
    #       profit_factor_long_fast,
    #       profit_factor_long_fastest,
    #       period_var_long,
    #       Total_Return,
    #       wins_or_loss_3_dollar_min
    #     )
    # ) %>%
    # bind_rows(
    #   best_best_prediced_outcome2
    # )

    short_positions <-
      final_results %>%
      dplyr::select(Asset,
                    profit_factor = profit_factor_short,
                    stop_factor = stop_factor_short,
                    period_var = period_var_short) %>%
      mutate(trade_col = "Short")

    long_positions <-
      final_results %>%
      dplyr::select(Asset,
                    profit_factor = profit_factor_long,
                    stop_factor = stop_factor_long,
                    period_var = period_var_long) %>%
      mutate(trade_col = "Long")

    long_positions_fast <-
      final_results %>%
      dplyr::select(Asset,
                    profit_factor = profit_factor_long_fast,
                    stop_factor = stop_factor_long,
                    period_var = period_var_long) %>%
      mutate(trade_col = "Long")

    long_positions_fastest <-
      final_results %>%
      dplyr::select(Asset,
                    profit_factor = profit_factor_long_fastest,
                    stop_factor = stop_factor_long,
                    period_var = period_var_long) %>%
      mutate(trade_col = "Long")

    all_positions <-
      short_positions %>%
      bind_rows(long_positions) %>%
      bind_rows(long_positions_fast) %>%
      bind_rows(long_positions_fastest)

    return(
      list("final_results" = final_results,
           "all_positions" = all_positions)
    )

  }


get_best_trade_end_point <-
  function(
    model_optimisation_store_path =
      "C:/Users/Nikhil Chandra/Documents/trade_data/single_asset_advanced_optimisation.db"
  ) {

    model_optimisation_store_db <-
      connect_db(model_optimisation_store_path)

    all_max_point_sims <-
      DBI::dbGetQuery(conn = model_optimisation_store_db,
                      statement = "SELECT * FROM single_asset_max_point_optimisation")

    temp_summary <-
      all_max_point_sims %>%
      filter(max_point_in_trade_long > 0, long_return > 0) %>%
      group_by(Asset, profit_factor_long, stop_factor_long,
               profit_factor_short,stop_factor_short, period_var_long,  period_var_short) %>%
      summarise(
        max_point_long_mean = mean(max_point_in_trade_long, na.rm = T),
        max_point_long_low_25 = quantile(max_point_in_trade_long, 0.25, na.rm = T),
        max_point_long_high_70 = quantile(max_point_in_trade_long, 0.70, na.rm = T),
        max_point_long_high_75 = quantile(max_point_in_trade_long, 0.75, na.rm = T),
        max_point_long_high_80 = quantile(max_point_in_trade_long, 0.80, na.rm = T),
        max_point_long_high_85 = quantile(max_point_in_trade_long, 0.85, na.rm = T),
        max_point_long_high_90 = quantile(max_point_in_trade_long, 0.9, na.rm = T),
        max_point_long_high_95 = quantile(max_point_in_trade_long, 0.95, na.rm = T),

        max_period_long_mean = mean(max_point_period_long, na.rm = T),
        max_period_long_low_25 = quantile(max_point_period_long, 0.25, na.rm = T),
        max_period_long_high_75 = quantile(max_point_period_long, 0.75, na.rm = T),
        max_period_long_high_90 = quantile(max_point_period_long, 0.9, na.rm = T),
        max_period_long_high_95 = quantile(max_point_period_long, 0.95, na.rm = T)
      )

    # assets_x <- all_max_point_sims %>% distinct(Asset) %>% pull(Asset)
    #
    # for (i in 1:length(assets_x)) {
    #
    #   temp_asset_vec <-
    #     all_max_point_sims %>%
    #     filter(Asset == assets_x[i]) %>%
    #     pull(max_point_in_trade_long) %>%
    #     keep(~ !is.na(.x)) %>%
    #     unlist() %>%
    #     as.numeric()
    #
    #   dist_fit <- fitdistrplus::fitdist(temp_asset_vec, distr = "norm")
    #   mean_temp <- dist_fit[[1]][1]
    #   sd_temp <- dist_fit[[1]][2]
    #
    #
    #
    # }

    return(temp_summary)
  }


#' create_running_profits
#'
#' @param asset_of_interest
#' @param asset_data
#' @param stop_factor
#' @param profit_factor
#' @param risk_dollar_value
#' @param trade_direction
#' @param currency_conversion
#' @param asset_infor
#'
#' @return
#' @export
#'
#' @examples
create_running_profits <-
  function(
    asset_of_interest = "EUR_JPY",
    asset_data = Indices_Metals_Bonds,
    stop_factor = 2,
    profit_factor = 15,
    risk_dollar_value = 4,
    trade_direction = "Long",
    currency_conversion = currency_conversion,
    asset_infor = asset_infor
  ) {

    bid_price <-
      asset_data[[2]] %>%
      filter(Asset == asset_of_interest) %>%
      dplyr::select(Date, Asset,
                    Bid_Price = Price,
                    Ask_High = High,
                    Ask_Low = Low)

    asset_data_with_indicator <-
      asset_data[[1]] %>%
      filter(Asset == asset_of_interest) %>%
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
      mutate(

        mean_movement = mean(Ask_Price - lag(Ask_Price), na.rm = T),
        sd_movement = sd(Ask_Price - lag(Ask_Price), na.rm = T),
        stop_value = stop_factor*sd_movement + mean_movement,
        profit_value = profit_factor*sd_movement + mean_movement,
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
            str_detect(Asset,"ZAR|CNH") ~ (risk_dollar_value/stop_value)*adjusted_conversion,
            TRUE ~ (risk_dollar_value/stop_value)/adjusted_conversion
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

        period_return_1_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,2) > stop_point &
              lead(Bid_High,2) < profit_point ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price, 2) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,2) > stop_point &
              lead(Bid_High,2) > profit_point  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,2) <= stop_point|
              trade_col == "Long" & lead(Bid_Low,1) <= stop_point ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,2) < stop_point &
              lead(Ask_Low,2) > profit_point ~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,2) ),

            trade_col == "Short" & lead(Ask_High,2) < stop_point &
              lead(Ask_Low,2) < profit_point ~ profit_return,

            trade_col == "Short" & lead(Ask_High,2) >= stop_point|
              trade_col == "Short" & lead(Ask_High,1) >= stop_point ~ -1*stop_return
          ),

        period_return_2_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,3) > stop_point &
              lead(Bid_High,3) < profit_point &
              period_return_1_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price, 3) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,3) > stop_point &
              lead(Bid_High,3) > profit_point &
              period_return_1_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,3) <= stop_point|
              period_return_1_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,3) < stop_point &
              lead(Ask_Low,3) > profit_point &
              period_return_1_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,3) ),

            trade_col == "Short" & lead(Ask_High,3) < stop_point &
              lead(Ask_Low,3) < profit_point &
              period_return_1_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,3) >= stop_point|
              period_return_1_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_3_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,4) > stop_point &
              lead(Bid_High,4) < profit_point &
              period_return_2_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,4) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,4) > stop_point &
              lead(Bid_High,4) > profit_point &
              period_return_2_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,4) <= stop_point|
              period_return_2_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,4) < stop_point &
              lead(Ask_Low,4) > profit_point &
              period_return_2_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,4) ),

            trade_col == "Short" & lead(Ask_High,4) < stop_point &
              lead(Ask_Low,4) < profit_point &
              period_return_2_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,4) >= stop_point|
              period_return_2_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_4_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,5) > stop_point &
              lead(Bid_High,5) < profit_point &
              period_return_3_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,5) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,5) > stop_point &
              lead(Bid_High,5) > profit_point &
              period_return_3_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,5) <= stop_point|
              period_return_3_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,5) < stop_point &
              lead(Ask_Low,5) > profit_point &
              period_return_3_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,5) ),

            trade_col == "Short" & lead(Ask_High,5) < stop_point &
              lead(Ask_Low,5) < profit_point &
              period_return_3_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,5) >= stop_point|
              period_return_3_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_5_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,6) > stop_point &
              lead(Bid_High,6) < profit_point &
              period_return_4_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,6) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,6) > stop_point &
              lead(Bid_High,6) > profit_point &
              period_return_4_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,6) <= stop_point|
              period_return_4_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,6) < stop_point &
              lead(Ask_Low,6) > profit_point &
              period_return_4_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,6) ),

            trade_col == "Short" & lead(Ask_High,6) < stop_point &
              lead(Ask_Low,6) < profit_point &
              period_return_4_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,6) >= stop_point|
              period_return_4_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_6_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,7) > stop_point &
              lead(Bid_High,7) < profit_point &
              period_return_5_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,7) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,7) > stop_point &
              lead(Bid_High,7) > profit_point &
              period_return_5_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,7) <= stop_point|
              period_return_5_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,7) < stop_point &
              lead(Ask_Low,7) > profit_point &
              period_return_5_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,7) ),

            trade_col == "Short" & lead(Ask_High,7) < stop_point &
              lead(Ask_Low,7) < profit_point &
              period_return_5_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,7) >= stop_point|
              period_return_5_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_7_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,8) > stop_point &
              lead(Bid_High,8) < profit_point &
              period_return_6_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,8) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,8) > stop_point &
              lead(Bid_High,8) > profit_point &
              period_return_6_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,8) <= stop_point|
              period_return_6_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,8) < stop_point &
              lead(Ask_Low,8) > profit_point &
              period_return_6_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,8) ),

            trade_col == "Short" & lead(Ask_High,8) < stop_point &
              lead(Ask_Low,8) < profit_point &
              period_return_6_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,8) >= stop_point|
              period_return_6_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_8_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,9) > stop_point &
              lead(Bid_High,9) < profit_point &
              period_return_7_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,9) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,9) > stop_point &
              lead(Bid_High,9) > profit_point &
              period_return_7_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,9) <= stop_point|
              period_return_7_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,9) < stop_point &
              lead(Ask_Low,9) > profit_point &
              period_return_7_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,9) ),

            trade_col == "Short" & lead(Ask_High,9) < stop_point &
              lead(Ask_Low,9) < profit_point &
              period_return_7_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,9) >= stop_point|
              period_return_7_Price == -1*stop_return ~ -1*stop_return
          ),


        period_return_9_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,10) > stop_point &
              lead(Bid_High,10) < profit_point &
              period_return_8_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,10) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,10) > stop_point &
              lead(Bid_High,10) > profit_point &
              period_return_8_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,10) <= stop_point|
              period_return_8_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,10) < stop_point &
              lead(Ask_Low,10) > profit_point &
              period_return_8_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,10) ),

            trade_col == "Short" & lead(Ask_High,10) < stop_point &
              lead(Ask_Low,10) < profit_point &
              period_return_8_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,10) >= stop_point|
              period_return_8_Price == -1*stop_return ~ -1*stop_return
          ),


        period_return_10_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,11) > stop_point &
              lead(Bid_High,11) < profit_point &
              period_return_9_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,11) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,11) > stop_point &
              lead(Bid_High,11) > profit_point &
              period_return_9_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,11) <= stop_point|
              period_return_9_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,11) < stop_point &
              lead(Ask_Low,11) > profit_point &
              period_return_9_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,11) ),

            trade_col == "Short" & lead(Ask_High,11) < stop_point &
              lead(Ask_Low,11) < profit_point &
              period_return_9_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,11) >= stop_point|
              period_return_9_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_11_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,12) > stop_point &
              lead(Bid_High,12) < profit_point &
              period_return_10_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,12) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,12) > stop_point &
              lead(Bid_High,12) > profit_point &
              period_return_10_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,12) <= stop_point|
              period_return_10_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,12) < stop_point &
              lead(Ask_Low,12) > profit_point &
              period_return_10_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,12) ),

            trade_col == "Short" & lead(Ask_High,12) < stop_point &
              lead(Ask_Low,12) < profit_point &
              period_return_10_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,12) >= stop_point|
              period_return_10_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_12_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,13) > stop_point &
              lead(Bid_High,13) < profit_point &
              period_return_11_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,13) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,13) > stop_point &
              lead(Bid_High,13) > profit_point &
              period_return_11_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,13) <= stop_point|
              period_return_11_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,13) < stop_point &
              lead(Ask_Low,13) > profit_point &
              period_return_11_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,13) ),

            trade_col == "Short" & lead(Ask_High,13) < stop_point &
              lead(Ask_Low,13) < profit_point &
              period_return_11_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,13) >= stop_point|
              period_return_11_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_13_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,14) > stop_point &
              lead(Bid_High,14) < profit_point &
              period_return_12_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,14) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,14) > stop_point &
              lead(Bid_High,14) > profit_point &
              period_return_12_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,14) <= stop_point|
              period_return_12_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,14) < stop_point &
              lead(Ask_Low,14) > profit_point &
              period_return_12_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,14) ),

            trade_col == "Short" & lead(Ask_High,14) < stop_point &
              lead(Ask_Low,14) < profit_point &
              period_return_12_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,14) >= stop_point|
              period_return_12_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_14_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,15) > stop_point &
              lead(Bid_High,15) < profit_point &
              period_return_13_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,15) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,15) > stop_point &
              lead(Bid_High,15) > profit_point &
              period_return_13_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,15) <= stop_point|
              period_return_13_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,15) < stop_point &
              lead(Ask_Low,15) > profit_point &
              period_return_13_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,15) ),

            trade_col == "Short" & lead(Ask_High,15) < stop_point &
              lead(Ask_Low,15) < profit_point &
              period_return_13_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,15) >= stop_point|
              period_return_13_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_15_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,16) > stop_point &
              lead(Bid_High,16) < profit_point &
              period_return_14_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,16) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,16) > stop_point &
              lead(Bid_High,16) > profit_point &
              period_return_14_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,16) <= stop_point|
              period_return_14_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,16) < stop_point &
              lead(Ask_Low,16) > profit_point &
              period_return_14_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,16) ),

            trade_col == "Short" & lead(Ask_High,16) < stop_point &
              lead(Ask_Low,16) < profit_point &
              period_return_14_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,16) >= stop_point|
              period_return_14_Price == -1*stop_return ~ -1*stop_return
          ) ,

        period_return_16_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,17) > stop_point &
              lead(Bid_High,17) < profit_point &
              period_return_15_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,17) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,17) > stop_point &
              lead(Bid_High,17) > profit_point &
              period_return_15_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,17) <= stop_point|
              period_return_15_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,17) < stop_point &
              lead(Ask_Low,17) > profit_point &
              period_return_15_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,17) ),

            trade_col == "Short" & lead(Ask_High,17) < stop_point &
              lead(Ask_Low,17) < profit_point &
              period_return_15_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,17) >= stop_point|
              period_return_15_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_17_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,18) > stop_point &
              lead(Bid_High,18) < profit_point &
              period_return_16_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,18) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,18) > stop_point &
              lead(Bid_High,18) > profit_point &
              period_return_16_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,18) <= stop_point|
              period_return_16_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,18) < stop_point &
              lead(Ask_Low,18) > profit_point &
              period_return_16_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,18) ),

            trade_col == "Short" & lead(Ask_High,18) < stop_point &
              lead(Ask_Low,18) < profit_point &
              period_return_16_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,18) >= stop_point|
              period_return_16_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_18_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,19) > stop_point &
              lead(Bid_High,19) < profit_point &
              period_return_17_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,19) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,19) > stop_point &
              lead(Bid_High,19) > profit_point &
              period_return_17_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,19) <= stop_point|
              period_return_17_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,19) < stop_point &
              lead(Ask_Low,19) > profit_point &
              period_return_17_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,19) ),

            trade_col == "Short" & lead(Ask_High,19) < stop_point &
              lead(Ask_Low,19) < profit_point &
              period_return_17_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,19) >= stop_point|
              period_return_17_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_19_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,20) > stop_point &
              lead(Bid_High,20) < profit_point &
              period_return_18_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,20) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,20) > stop_point &
              lead(Bid_High,20) > profit_point &
              period_return_18_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,20) <= stop_point|
              period_return_18_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,20) < stop_point &
              lead(Ask_Low,20) > profit_point &
              period_return_18_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,20) ),

            trade_col == "Short" & lead(Ask_High,20) < stop_point &
              lead(Ask_Low,20) < profit_point &
              period_return_18_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,20) >= stop_point|
              period_return_18_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_20_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,21) > stop_point &
              lead(Bid_High,21) < profit_point &
              period_return_19_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,21) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,21) > stop_point &
              lead(Bid_High,21) > profit_point &
              period_return_19_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,21) <= stop_point|
              period_return_19_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,21) < stop_point &
              lead(Ask_Low,21) > profit_point &
              period_return_19_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,21) ),

            trade_col == "Short" & lead(Ask_High,21) < stop_point &
              lead(Ask_Low,21) < profit_point &
              period_return_19_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,21) >= stop_point|
              period_return_19_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_21_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,22) > stop_point &
              lead(Bid_High,22) < profit_point &
              period_return_20_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,22) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,22) > stop_point &
              lead(Bid_High,22) > profit_point &
              period_return_20_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,22) <= stop_point|
              period_return_20_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,22) < stop_point &
              lead(Ask_Low,22) > profit_point &
              period_return_20_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,22) ),

            trade_col == "Short" & lead(Ask_High,22) < stop_point &
              lead(Ask_Low,22) < profit_point &
              period_return_20_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,22) >= stop_point|
              period_return_20_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_22_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,23) > stop_point &
              lead(Bid_High,23) < profit_point &
              period_return_21_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,23) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,23) > stop_point &
              lead(Bid_High,23) > profit_point &
              period_return_21_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,23) <= stop_point|
              period_return_21_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,23) < stop_point &
              lead(Ask_Low,23) > profit_point &
              period_return_21_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,23) ),

            trade_col == "Short" & lead(Ask_High,23) < stop_point &
              lead(Ask_Low,23) < profit_point &
              period_return_21_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,23) >= stop_point|
              period_return_21_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_23_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,24) > stop_point &
              lead(Bid_High,24) < profit_point &
              period_return_22_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,24) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,24) > stop_point &
              lead(Bid_High,24) > profit_point &
              period_return_22_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,24) <= stop_point|
              period_return_22_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,24) < stop_point &
              lead(Ask_Low,24) > profit_point &
              period_return_22_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,24) ),

            trade_col == "Short" & lead(Ask_High,24) < stop_point &
              lead(Ask_Low,24) < profit_point &
              period_return_22_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,24) >= stop_point|
              period_return_22_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_24_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,25) > stop_point &
              lead(Bid_High,25) < profit_point &
              period_return_23_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,25) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,25) > stop_point &
              lead(Bid_High,25) > profit_point &
              period_return_23_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,25) <= stop_point|
              period_return_23_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,25) < stop_point &
              lead(Ask_Low,25) > profit_point &
              period_return_23_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,25) ),

            trade_col == "Short" & lead(Ask_High,25) < stop_point &
              lead(Ask_Low,25) < profit_point &
              period_return_23_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,25) >= stop_point|
              period_return_23_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_25_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,26) > stop_point &
              lead(Bid_High,26) < profit_point &
              period_return_24_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,26) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,26) > stop_point &
              lead(Bid_High,26) > profit_point &
              period_return_24_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,26) <= stop_point|
              period_return_24_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,26) < stop_point &
              lead(Ask_Low,26) > profit_point &
              period_return_24_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,26) ),

            trade_col == "Short" & lead(Ask_High,26) < stop_point &
              lead(Ask_Low,26) < profit_point &
              period_return_24_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,26) >= stop_point|
              period_return_24_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_26_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,27) > stop_point &
              lead(Bid_High,27) < profit_point &
              period_return_25_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,27) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,27) > stop_point &
              lead(Bid_High,27) > profit_point &
              period_return_25_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,27) <= stop_point|
              period_return_25_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,27) < stop_point &
              lead(Ask_Low,27) > profit_point &
              period_return_25_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,27) ),

            trade_col == "Short" & lead(Ask_High,27) < stop_point &
              lead(Ask_Low,27) < profit_point &
              period_return_25_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,27) >= stop_point|
              period_return_25_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_27_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,28) > stop_point &
              lead(Bid_High,28) < profit_point &
              period_return_26_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,28) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,28) > stop_point &
              lead(Bid_High,28) > profit_point &
              period_return_26_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,28) <= stop_point|
              period_return_26_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,28) < stop_point &
              lead(Ask_Low,28) > profit_point &
              period_return_26_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,28) ),

            trade_col == "Short" & lead(Ask_High,28) < stop_point &
              lead(Ask_Low,28) < profit_point &
              period_return_26_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,28) >= stop_point|
              period_return_26_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_28_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,29) > stop_point &
              lead(Bid_High,29) < profit_point &
              period_return_27_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,29) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,29) > stop_point &
              lead(Bid_High,29) > profit_point &
              period_return_27_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,29) <= stop_point|
              period_return_27_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,29) < stop_point &
              lead(Ask_Low,29) > profit_point &
              period_return_27_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,29) ),

            trade_col == "Short" & lead(Ask_High,29) < stop_point &
              lead(Ask_Low,29) < profit_point &
              period_return_27_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,29) >= stop_point|
              period_return_27_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_29_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,30) > stop_point &
              lead(Bid_High,30) < profit_point &
              period_return_28_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,30) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,30) > stop_point &
              lead(Bid_High,30) > profit_point &
              period_return_28_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,30) <= stop_point|
              period_return_28_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,30) < stop_point &
              lead(Ask_Low,30) > profit_point &
              period_return_28_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,30) ),

            trade_col == "Short" & lead(Ask_High,30) < stop_point &
              lead(Ask_Low,30) < profit_point &
              period_return_28_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,30) >= stop_point|
              period_return_28_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_30_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,31) > stop_point &
              lead(Bid_High,31) < profit_point &
              period_return_29_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,31) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,31) > stop_point &
              lead(Bid_High,31) > profit_point &
              period_return_29_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,31) <= stop_point|
              period_return_29_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,31) < stop_point &
              lead(Ask_Low,31) > profit_point &
              period_return_29_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,31) ),

            trade_col == "Short" & lead(Ask_High,31) < stop_point &
              lead(Ask_Low,31) < profit_point &
              period_return_29_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,31) >= stop_point|
              period_return_29_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_31_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,32) > stop_point &
              lead(Bid_High,32) < profit_point &
              period_return_30_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,32) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,32) > stop_point &
              lead(Bid_High,32) > profit_point &
              period_return_30_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,32) <= stop_point|
              period_return_30_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,32) < stop_point &
              lead(Ask_Low,32) > profit_point &
              period_return_30_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,32) ),

            trade_col == "Short" & lead(Ask_High,32) < stop_point &
              lead(Ask_Low,32) < profit_point &
              period_return_30_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,32) >= stop_point|
              period_return_30_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_32_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,33) > stop_point &
              lead(Bid_High,33) < profit_point &
              period_return_31_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,33) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,33) > stop_point &
              lead(Bid_High,33) > profit_point &
              period_return_31_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,33) <= stop_point|
              period_return_31_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,33) < stop_point &
              lead(Ask_Low,33) > profit_point &
              period_return_31_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,33) ),

            trade_col == "Short" & lead(Ask_High,33) < stop_point &
              lead(Ask_Low,33) < profit_point &
              period_return_31_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,33) >= stop_point|
              period_return_31_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_33_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,34) > stop_point &
              lead(Bid_High,34) < profit_point &
              period_return_32_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,34) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,34) > stop_point &
              lead(Bid_High,34) > profit_point &
              period_return_32_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,34) <= stop_point|
              period_return_32_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,34) < stop_point &
              lead(Ask_Low,34) > profit_point &
              period_return_32_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,34) ),

            trade_col == "Short" & lead(Ask_High,34) < stop_point &
              lead(Ask_Low,34) < profit_point &
              period_return_32_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,34) >= stop_point|
              period_return_32_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_34_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,35) > stop_point &
              lead(Bid_High,35) < profit_point &
              period_return_33_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,35) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,35) > stop_point &
              lead(Bid_High,35) > profit_point &
              period_return_33_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,35) <= stop_point|
              period_return_33_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,35) < stop_point &
              lead(Ask_Low,35) > profit_point &
              period_return_33_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,35) ),

            trade_col == "Short" & lead(Ask_High,35) < stop_point &
              lead(Ask_Low,35) < profit_point &
              period_return_33_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,35) >= stop_point|
              period_return_33_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_35_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,36) > stop_point &
              lead(Bid_High,36) < profit_point &
              period_return_34_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,36) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,36) > stop_point &
              lead(Bid_High,36) > profit_point &
              period_return_34_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,36) <= stop_point|
              period_return_34_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,36) < stop_point &
              lead(Ask_Low,36) > profit_point &
              period_return_34_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,36) ),

            trade_col == "Short" & lead(Ask_High,36) < stop_point &
              lead(Ask_Low,36) < profit_point &
              period_return_34_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,36) >= stop_point|
              period_return_34_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_36_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,37) > stop_point &
              lead(Bid_High,37) < profit_point &
              period_return_35_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,37) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,37) > stop_point &
              lead(Bid_High,37) > profit_point &
              period_return_35_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,37) <= stop_point|
              period_return_35_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,37) < stop_point &
              lead(Ask_Low,37) > profit_point &
              period_return_35_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,37) ),

            trade_col == "Short" & lead(Ask_High,37) < stop_point &
              lead(Ask_Low,37) < profit_point &
              period_return_35_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,37) >= stop_point|
              period_return_35_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_37_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,38) > stop_point &
              lead(Bid_High,38) < profit_point &
              period_return_36_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,38) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,38) > stop_point &
              lead(Bid_High,38) > profit_point &
              period_return_36_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,38) <= stop_point|
              period_return_36_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,38) < stop_point &
              lead(Ask_Low,38) > profit_point &
              period_return_36_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,38) ),

            trade_col == "Short" & lead(Ask_High,38) < stop_point &
              lead(Ask_Low,38) < profit_point &
              period_return_36_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,38) >= stop_point|
              period_return_36_Price == -1*stop_return ~ -1*stop_return
          ),


        period_return_38_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,39) > stop_point &
              lead(Bid_High,39) < profit_point &
              period_return_37_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,39) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,39) > stop_point &
              lead(Bid_High,39) > profit_point &
              period_return_37_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,39) <= stop_point|
              period_return_37_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,39) < stop_point &
              lead(Ask_Low,39) > profit_point &
              period_return_37_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,39) ),

            trade_col == "Short" & lead(Ask_High,39) < stop_point &
              lead(Ask_Low,39) < profit_point &
              period_return_37_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,39) >= stop_point|
              period_return_37_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_39_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,40) > stop_point &
              lead(Bid_High,40) < profit_point &
              period_return_38_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,40) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,40) > stop_point &
              lead(Bid_High,40) > profit_point &
              period_return_38_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,40) <= stop_point|
              period_return_38_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,40) < stop_point &
              lead(Ask_Low,40) > profit_point &
              period_return_38_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,40) ),

            trade_col == "Short" & lead(Ask_High,40) < stop_point &
              lead(Ask_Low,40) < profit_point &
              period_return_38_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,40) >= stop_point|
              period_return_38_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_40_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,41) > stop_point &
              lead(Bid_High,41) < profit_point &
              period_return_39_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,41) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,41) > stop_point &
              lead(Bid_High,41) > profit_point &
              period_return_39_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,41) <= stop_point|
              period_return_39_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,41) < stop_point &
              lead(Ask_Low,41) > profit_point &
              period_return_39_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,41) ),

            trade_col == "Short" & lead(Ask_High,41) < stop_point &
              lead(Ask_Low,41) < profit_point &
              period_return_39_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,41) >= stop_point|
              period_return_39_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_41_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,42) > stop_point &
              lead(Bid_High,42) < profit_point &
              period_return_40_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,42) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,42) > stop_point &
              lead(Bid_High,42) > profit_point &
              period_return_40_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,42) <= stop_point|
              period_return_40_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,42) < stop_point &
              lead(Ask_Low,42) > profit_point &
              period_return_40_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,42) ),

            trade_col == "Short" & lead(Ask_High,42) < stop_point &
              lead(Ask_Low,42) < profit_point &
              period_return_40_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,42) >= stop_point|
              period_return_40_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_42_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,43) > stop_point &
              lead(Bid_High,43) < profit_point &
              period_return_41_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,43) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,43) > stop_point &
              lead(Bid_High,43) > profit_point &
              period_return_41_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,43) <= stop_point|
              period_return_41_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,43) < stop_point &
              lead(Ask_Low,43) > profit_point &
              period_return_41_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,43) ),

            trade_col == "Short" & lead(Ask_High,43) < stop_point &
              lead(Ask_Low,43) < profit_point &
              period_return_41_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,43) >= stop_point|
              period_return_41_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_43_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,44) > stop_point &
              lead(Bid_High,44) < profit_point &
              period_return_42_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,44) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,44) > stop_point &
              lead(Bid_High,44) > profit_point &
              period_return_42_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,44) <= stop_point|
              period_return_42_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,44) < stop_point &
              lead(Ask_Low,44) > profit_point &
              period_return_42_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,44) ),

            trade_col == "Short" & lead(Ask_High,44) < stop_point &
              lead(Ask_Low,44) < profit_point &
              period_return_42_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,44) >= stop_point|
              period_return_42_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_44_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,45) > stop_point &
              lead(Bid_High,45) < profit_point &
              period_return_43_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,45) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,45) > stop_point &
              lead(Bid_High,45) > profit_point &
              period_return_43_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,45) <= stop_point|
              period_return_43_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,45) < stop_point &
              lead(Ask_Low,45) > profit_point &
              period_return_43_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,45) ),

            trade_col == "Short" & lead(Ask_High,45) < stop_point &
              lead(Ask_Low,45) < profit_point &
              period_return_43_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,45) >= stop_point|
              period_return_43_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_45_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,46) > stop_point &
              lead(Bid_High,46) < profit_point &
              period_return_44_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,46) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,46) > stop_point &
              lead(Bid_High,46) > profit_point &
              period_return_44_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,46) <= stop_point|
              period_return_44_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,46) < stop_point &
              lead(Ask_Low,46) > profit_point &
              period_return_44_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,46) ),

            trade_col == "Short" & lead(Ask_High,46) < stop_point &
              lead(Ask_Low,46) < profit_point &
              period_return_44_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,46) >= stop_point|
              period_return_44_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_46_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,47) > stop_point &
              lead(Bid_High,47) < profit_point &
              period_return_45_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,47) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,47) > stop_point &
              lead(Bid_High,47) > profit_point &
              period_return_45_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,47) <= stop_point|
              period_return_45_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,47) < stop_point &
              lead(Ask_Low,47) > profit_point &
              period_return_45_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,47) ),

            trade_col == "Short" & lead(Ask_High,47) < stop_point &
              lead(Ask_Low,47) < profit_point &
              period_return_45_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,47) >= stop_point|
              period_return_45_Price == -1*stop_return ~ -1*stop_return
          )

      )

    return(asset_data_with_indicator)

  }

#' get_sig_coefs
#'
#' @param model_object_of_interest
#' @param p_value_thresh_for_inputs
#'
#' @returns
#' @export
#'
#' @examples
get_sig_coefs <-
  function(model_object_of_interest = macro_indicator_model,
           p_value_thresh_for_inputs = 0.5) {


    all_coefs <-  model_object_of_interest %>% jtools::j_summ() %>% pluck(1)
    coef_names <- row.names(all_coefs) %>% as.character()
    filtered_coefs <-
      all_coefs %>%
      as_tibble() %>%
      mutate(all_vars = coef_names) %>%
      filter(p <= p_value_thresh_for_inputs) %>%
      filter(!str_detect(all_vars, "Intercep")) %>%
      pull(all_vars) %>%
      map( ~ str_remove_all(.x, "`") %>% str_trim() ) %>%
      unlist() %>%
      as.character()

    return(filtered_coefs)

  }

#' single_asset_Logit_indicator_adv_gen_models
#'
#' @param asset_data
#' @param All_Daily_Data
#' @param Asset_of_interest
#' @param actual_wins_losses
#' @param interest_rates
#' @param cpi_data
#' @param sentiment_index
#' @param equity_index
#' @param gold_index
#' @param silver_index
#' @param bonds_index
#' @param USD_index
#' @param EUR_index
#' @param GBP_index
#' @param AUD_index
#' @param countries_for_int_strength
#' @param date_train_end
#' @param date_train_phase_2_end
#' @param date_test_start
#' @param couplua_assets
#' @param stop_value_var
#' @param profit_value_var
#' @param period_var
#' @param bin_var_col
#' @param trade_direction
#' @param save_path
#'
#' @returns
#' @export
#'
#' @examples
single_asset_Logit_indicator_adv_gen_models <-
  function(
    asset_data = Indices_Metals_Bonds[[1]],
    All_Daily_Data = All_Daily_Data,
    Asset_of_interest = "EUR_USD",
    actual_wins_losses = actual_wins_losses,
    interest_rates = interest_rates,
    cpi_data = cpi_data,
    sentiment_index = sentiment_index,
    gdp_data = gdp_data,
    unemp_data = unemp_data,
    manufac_pmi = manufac_pmi,
    USD_Macro = USD_Macro,
    EUR_Macro = EUR_Macro,

    equity_index = equity_index,
    gold_index = gold_index,
    silver_index = silver_index,
    bonds_index = bonds_index,
    USD_index = USD_index,
    EUR_index = EUR_index,
    GBP_index = GBP_index,
    AUD_index = AUD_index,
    COMMOD_index = COMMOD_index,
    USD_STOCKS_index = USD_STOCKS_index,
    NZD_index = NZD_index,

    countries_for_int_strength = countries_for_int_strength,
    date_train_end = post_train_date_start,
    date_train_phase_2_end = post_train_date_start + months(6),
    date_test_start = post_train_date_start + months(7),

    couplua_assets = couplua_assets,

    stop_value_var = stop_value_var,
    profit_value_var = profit_value_var,
    period_var = period_var,

    bin_var_col = c("period_return_20_Price", "period_return_35_Price"),
    trade_direction = "Long",
    save_path = "C:/Users/nikhi/Documents/trade_data/single_asset_models_v2_adv"

  ) {

    asset_data_internal <-
      asset_data %>%
      filter(Asset == Asset_of_interest)

    macro_data <-
      prepare_macro_indicator_model_data(
        asset_data = asset_data_internal,
        Asset_of_interest = Asset_of_interest,
        interest_rates = interest_rates,
        cpi_data = cpi_data,
        sentiment_index = sentiment_index,
        gdp_data = gdp_data,
        unemp_data = unemp_data,
        manufac_pmi = manufac_pmi,
        USD_Macro = USD_Macro,
        EUR_Macro = EUR_Macro,
        countries_for_int_strength = countries_for_int_strength,
        date_limit = today()
      )

    macro_train_data <-
      macro_data %>%
      filter(Date <= date_train_end)

    message("Saving macro Models")
    prepare_macro_indicator_model(
      macro_for_join = macro_train_data,
      actual_wins_losses = actual_wins_losses,
      Asset_of_interest = Asset_of_interest,
      date_limit = date_train_end,
      stop_value_var = stop_value_var,
      profit_value_var = profit_value_var,
      period_var = period_var,
      bin_var_col = bin_var_col,
      trade_direction = trade_direction,
      save_path = save_path
    )

    macro_phase_2_data <-
      macro_data %>%
      filter(Date < date_train_phase_2_end)

    macro_preds <-
      single_asset_read_models_and_get_pred(
        pred_data = macro_phase_2_data,
        trade_direction = trade_direction,
        Asset_of_interest = Asset_of_interest,
        save_path = save_path,
        model_string = "_macro_"
      )

    macro_preds_averages <-
      macro_preds %>%
      summarise(
        across(.cols = contains("pred"), .fns = ~ mean(., na.rm = T))
      )

    names(macro_preds_averages) <-
      names(macro_preds_averages) %>%
      map(~ paste0(.x, "_mean")) %>%
      unlist() %>%
      as.character()

    macro_preds_sd <-
      macro_preds %>%
      summarise(
        across(.cols = contains("pred"), .fns = ~ sd(., na.rm = T))
      )

    names(macro_preds_sd) <-
      names(macro_preds_sd) %>%
      map(~ paste0(.x, "_sd")) %>%
      unlist() %>%
      as.character()


    index_pca_data <-
      get_pca_index_indicator_data(
        asset_data = asset_data_internal,
        Asset_of_interest = Asset_of_interest,
        equity_index = equity_index,
        gold_index = gold_index,
        silver_index = silver_index,
        bonds_index = bonds_index,
        USD_index = USD_index,
        EUR_index = EUR_index,
        GBP_index = GBP_index,
        AUD_index = AUD_index,
        COMMOD_index = COMMOD_index,
        USD_STOCKS_index = USD_STOCKS_index,
        NZD_index = NZD_index,
        date_limit = today()
      )

    index_pca_train_data <-
      index_pca_data %>%
      filter(Date <= date_train_end)

    prepare_index_indicator_model(
      index_pca_data = index_pca_train_data,
      actual_wins_losses = actual_wins_losses,
      Asset_of_interest = Asset_of_interest,
      date_limit = date_train_end,
      stop_value_var = stop_value_var,
      profit_value_var = profit_value_var,
      period_var = period_var,
      bin_var_col = bin_var_col,
      trade_direction = trade_direction,
      save_path = save_path
    )

    index_pca_phase_2_data <-
      index_pca_data %>%
      filter(Date < date_train_phase_2_end)

    index_preds <-
      single_asset_read_models_and_get_pred(
        pred_data = index_pca_phase_2_data,
        trade_direction = trade_direction,
        Asset_of_interest = Asset_of_interest,
        save_path = save_path,
        model_string = "_index_"
      )

    index_preds_averages <-
      index_preds %>%
      summarise(
        across(.cols = contains("pred"), .fns = ~ mean(., na.rm = T))
      )

    names(index_preds_averages) <-
      names(index_preds_averages) %>%
      map(~ paste0(.x, "_mean")) %>%
      unlist() %>%
      as.character()

    index_preds_sd <-
      index_preds%>%
      summarise(
        across(.cols = contains("pred"), .fns = ~ sd(., na.rm = T))
      )

    names(index_preds_sd) <-
      names(index_preds_sd) %>%
      map(~ paste0(.x, "_sd")) %>%
      unlist() %>%
      as.character()

    daily_data_for_modelling <-
      prepare_daily_indicator_data(
        asset_data = asset_data_internal,
        All_Daily_Data = All_Daily_Data,
        Asset_of_interest = Asset_of_interest,
        date_limit = today()
      )

    daily_data_for_modelling_train <-
      daily_data_for_modelling %>%
      filter(Date <= date_train_end)

    prepare_daily_indicator_model(
      daily_indicator = daily_data_for_modelling_train,
      actual_wins_losses = actual_wins_losses,
      Asset_of_interest = Asset_of_interest,
      date_limit = date_train_end,
      stop_value_var = stop_value_var,
      profit_value_var = profit_value_var,
      period_var = period_var,
      bin_var_col = bin_var_col,
      trade_direction = trade_direction,
      save_path = save_path
    )

    daily_phase_2_data <-
      daily_data_for_modelling %>%
      filter(Date < date_train_phase_2_end)

    daily_preds <-
      single_asset_read_models_and_get_pred(
        pred_data = daily_phase_2_data,
        trade_direction = trade_direction,
        Asset_of_interest = Asset_of_interest,
        save_path = save_path,
        model_string = "_daily_"
      )

    daily_preds_averages <-
      daily_preds %>%
      summarise(
        across(.cols = contains("pred"), .fns = ~ mean(., na.rm = T))
      )

    names(daily_preds_averages) <-
      names(daily_preds_averages) %>%
      map(~ paste0(.x, "_mean")) %>%
      unlist() %>%
      as.character()

    daily_preds_sd <-
      daily_preds%>%
      summarise(
        across(.cols = contains("pred"), .fns = ~ sd(., na.rm = T))
      )

    names(daily_preds_sd) <-
      names(daily_preds_sd) %>%
      map(~ paste0(.x, "_sd")) %>%
      unlist() %>%
      as.character()

    copula_data <-
      prepare_copula_data(
        asset_data = asset_data,
        couplua_assets = couplua_assets,
        Asset_of_interest = Asset_of_interest,
        date_limit = today()
      )

    copula_data_train <-
      copula_data %>%
      filter(Date <= date_train_end)

    prepare_copula_model(
      copula_data = copula_data_train,
      actual_wins_losses = actual_wins_losses,
      Asset_of_interest = Asset_of_interest,
      date_limit = date_train_end,
      stop_value_var = stop_value_var,
      profit_value_var = profit_value_var,
      period_var = period_var,
      bin_var_col = bin_var_col,
      trade_direction = trade_direction,
      save_path = save_path
    )

    copula_phase_2_data <-
      copula_data %>%
      filter(Date < date_train_phase_2_end)

    copula_preds <-
      single_asset_read_models_and_get_pred(
        pred_data = copula_phase_2_data,
        trade_direction = trade_direction,
        Asset_of_interest = Asset_of_interest,
        save_path = save_path,
        model_string = "_copula_"
      )

    copula_preds_averages <-
      copula_preds %>%
      summarise(
        across(.cols = contains("pred"), .fns = ~ mean(., na.rm = T))
      )

    names(copula_preds_averages) <-
      names(copula_preds_averages) %>%
      map(~ paste0(.x, "_mean")) %>%
      unlist() %>%
      as.character()

    copula_preds_sd <-
      copula_preds%>%
      summarise(
        across(.cols = contains("pred"), .fns = ~ sd(., na.rm = T))
      )

    names(copula_preds_sd) <-
      names(copula_preds_sd) %>%
      map(~ paste0(.x, "_sd")) %>%
      unlist() %>%
      as.character()

    technical_data <-
      create_technical_indicators(asset_data = asset_data_internal) %>%
      dplyr::select(-Price, -Low, -High, -Open)

    technical_data <-
      technical_data %>%
      filter(Asset == Asset_of_interest) %>%
      arrange(Date) %>%
      mutate(
        across(.cols = !contains("Date"),
               .fns = ~ lag(.) )
      )

    technical_data_train <-
      technical_data %>%
      filter(Date <= date_train_end)

    prepare_technical_model(
      technical_data = technical_data_train,
      actual_wins_losses = actual_wins_losses,
      Asset_of_interest = Asset_of_interest,
      date_limit = date_train_end,
      stop_value_var = stop_value_var,
      profit_value_var = profit_value_var,
      period_var = period_var,
      bin_var_col = bin_var_col,
      trade_direction = trade_direction,
      save_path = save_path
    )

    technical_phase_2_data <-
      technical_data %>%
      filter(Date < date_train_phase_2_end)

    technical_preds <-
      single_asset_read_models_and_get_pred(
        pred_data = technical_phase_2_data,
        trade_direction = trade_direction,
        Asset_of_interest = Asset_of_interest,
        save_path = save_path,
        model_string = "_technical_"
      )

    technical_preds_averages <-
      technical_preds %>%
      summarise(
        across(.cols = contains("pred"), .fns = ~ mean(., na.rm = T))
      )

    names(technical_preds_averages) <-
      names(technical_preds_averages) %>%
      map(~ paste0(.x, "_mean")) %>%
      unlist() %>%
      as.character()

    technical_preds_sd <-
      technical_preds%>%
      summarise(
        across(.cols = contains("pred"), .fns = ~ sd(., na.rm = T))
      )

    names(technical_preds_sd) <-
      names(technical_preds_sd) %>%
      map(~ paste0(.x, "_sd")) %>%
      unlist() %>%
      as.character()

    accumulating_probs <-
      asset_data_internal %>%
      distinct(Date, Asset) %>%
      left_join(macro_preds) %>%
      left_join(index_preds) %>%
      left_join(daily_preds) %>%
      left_join(copula_preds) %>%
      left_join(technical_preds) %>%
      left_join(macro_data) %>%
      left_join(copula_data)%>%
      left_join(
        daily_data_for_modelling %>%
          dplyr::select(Date, Asset,
                        (contains("perc_line_")&contains("_20")),
                        contains("Support"),
                        contains("Resistance"),
                        contains("Bear"),
                        contains("Bull"),
                        (contains("perc_line_")&contains("_50")),
                        contains("moving_average_markov_")
                        )
        ) %>%
      left_join(index_pca_data) %>%
      left_join(
        technical_data %>%
          dplyr::select(Date, Asset,
                        (contains("perc_line_")&contains("_500")),
                        contains("Support"),
                        contains("Resistance"),
                        contains("Bear"),
                        contains("Bull"),
                        (contains("perc_line_")&contains("_500")),
                        contains("moving_average_markov_")
          )
      ) %>%
    distinct()

    rm(macro_data, macro_phase_2_data, macro_train_data)
    rm(index_pca_data, index_pca_phase_2_data, index_pca_train_data)
    rm(daily_phase_2_data, daily_data_for_modelling, daily_data_for_modelling_train)
    rm(copula_data, copula_data_train, copula_phase_2_data)
    rm(technical_data, technical_data_train, technical_phase_2_data)
    gc()

    rm(macro_preds ,
       index_preds ,
       daily_preds ,
       copula_preds,
       technical_preds)

    gc()

    combined_model_data <-
      accumulating_probs %>%
      filter(Date < date_train_phase_2_end) %>%
      arrange(Date) %>%
      fill(!contains("Date"), .direction = "down") %>%
      filter(if_all(.cols = everything(), ~!is.na(.)))

    prepare_combined_model(
      combined_model_data = combined_model_data,
      actual_wins_losses = actual_wins_losses,
      Asset_of_interest = Asset_of_interest,
      date_limit = date_train_end,
      stop_value_var = stop_value_var,
      profit_value_var = profit_value_var,
      period_var = period_var,
      bin_var_col = bin_var_col,
      trade_direction = trade_direction,
      save_path = save_path
    )

    combined_preds <-
      single_asset_read_models_and_get_pred(
        pred_data = combined_model_data,
        trade_direction = trade_direction,
        Asset_of_interest = Asset_of_interest,
        save_path = save_path,
        model_string = "_combined_"
      )

    combined_preds_averages <-
      combined_preds %>%
      summarise(
        across(.cols = contains("pred"), .fns = ~ mean(., na.rm = T))
      )

    names(combined_preds_averages) <-
      names(combined_preds_averages) %>%
      map(~ paste0(.x, "_mean")) %>%
      unlist() %>%
      as.character()

    combined_preds_sd <-
      combined_preds%>%
      summarise(
        across(.cols = contains("pred"), .fns = ~ sd(., na.rm = T))
      )

    names(combined_preds_sd) <-
      names(combined_preds_sd) %>%
      map(~ paste0(.x, "_sd")) %>%
      unlist() %>%
      as.character()

    mean_sd_values <-
      asset_data_internal %>%
      distinct(Date, Asset) %>%
      bind_cols(macro_preds_averages) %>%
      bind_cols(macro_preds_sd) %>%
      bind_cols(index_preds_averages) %>%
      bind_cols(index_preds_sd) %>%
      bind_cols(daily_preds_averages) %>%
      bind_cols(daily_preds_sd) %>%
      bind_cols(copula_preds_averages) %>%
      bind_cols(copula_preds_sd) %>%
      bind_cols(combined_preds_averages) %>%
      bind_cols(combined_preds_sd) %>%
      bind_cols(technical_preds_averages) %>%
      bind_cols(technical_preds_sd)

    saveRDS(mean_sd_values,
            file =
              glue::glue("{save_path}/{Asset_of_interest}_{trade_direction}_mean_sd_values.RDS")
    )

  }

#' single_asset_Logit_indicator_adv_get_preds
#'
#' @param asset_data
#' @param All_Daily_Data
#' @param Asset_of_interest
#' @param actual_wins_losses
#' @param interest_rates
#' @param cpi_data
#' @param sentiment_index
#' @param equity_index
#' @param gold_index
#' @param silver_index
#' @param bonds_index
#' @param USD_index
#' @param EUR_index
#' @param GBP_index
#' @param AUD_index
#' @param countries_for_int_strength
#' @param date_train_end
#' @param date_train_phase_2_end
#' @param date_test_start
#' @param couplua_assets
#' @param stop_value_var
#' @param profit_value_var
#' @param period_var
#' @param bin_var_col
#' @param trade_direction
#' @param save_path
#'
#' @returns
#' @export
#'
#' @examples
single_asset_Logit_indicator_adv_get_preds <-
  function(
    asset_data = Indices_Metals_Bonds[[1]],
    All_Daily_Data = All_Daily_Data,
    Asset_of_interest = "EUR_USD",
    actual_wins_losses = actual_wins_losses,
    interest_rates = interest_rates,
    cpi_data = cpi_data,
    sentiment_index = sentiment_index,
    gdp_data = gdp_data,
    unemp_data = unemp_data,
    manufac_pmi = manufac_pmi,
    USD_Macro = USD_Macro,
    EUR_Macro = EUR_Macro,

    equity_index = equity_index,
    gold_index = gold_index,
    silver_index = silver_index,
    bonds_index = bonds_index,
    USD_index = USD_index,
    EUR_index = EUR_index,
    GBP_index = GBP_index,
    AUD_index = AUD_index,
    COMMOD_index = COMMOD_index,
    USD_STOCKS_index = USD_STOCKS_index,
    NZD_index = NZD_index,

    countries_for_int_strength = countries_for_int_strength,
    date_train_end = post_train_date_start,
    date_train_phase_2_end = post_train_date_start + months(6),
    date_test_start = post_train_date_start + months(7),
    couplua_assets = couplua_assets,
    stop_value_var = stop_value_var,
    profit_value_var = profit_value_var,
    period_var = period_var,
    bin_var_col = c("period_return_20_Price", "period_return_35_Price"),
    trade_direction = "Long",
    save_path = "C:/Users/nikhi/Documents/trade_data/single_asset_models_v2_adv"
  ) {


    asset_data_internal <-
      asset_data %>%
      filter(Asset == Asset_of_interest)

    macro_data <-
      prepare_macro_indicator_model_data(
        asset_data = asset_data_internal,
        Asset_of_interest = Asset_of_interest,
        interest_rates = interest_rates,
        cpi_data = cpi_data,
        sentiment_index = sentiment_index,
        gdp_data = gdp_data,
        unemp_data = unemp_data,
        manufac_pmi = manufac_pmi,
        USD_Macro = USD_Macro,
        EUR_Macro = EUR_Macro,
        countries_for_int_strength = countries_for_int_strength,
        date_limit = today()
      )

    macro_preds <-
      single_asset_read_models_and_get_pred(
        pred_data = macro_data,
        trade_direction = trade_direction,
        Asset_of_interest = Asset_of_interest,
        save_path = save_path,
        model_string = "_macro_"
      )

    index_pca_data <-
      get_pca_index_indicator_data(
        asset_data = asset_data_internal,
        Asset_of_interest = Asset_of_interest,
        equity_index = equity_index,
        gold_index = gold_index,
        silver_index = silver_index,
        bonds_index = bonds_index,
        USD_index = USD_index,
        EUR_index = EUR_index,
        GBP_index = GBP_index,
        AUD_index = AUD_index,
        COMMOD_index = COMMOD_index,
        USD_STOCKS_index = USD_STOCKS_index,
        NZD_index = NZD_index,
        date_limit = today()
      )

    index_preds <-
      single_asset_read_models_and_get_pred(
        pred_data = index_pca_data,
        trade_direction = trade_direction,
        Asset_of_interest = Asset_of_interest,
        save_path = save_path,
        model_string = "_index_"
      )

    daily_data_for_modelling <-
      prepare_daily_indicator_data(
        asset_data = asset_data_internal,
        All_Daily_Data = All_Daily_Data,
        Asset_of_interest = Asset_of_interest,
        date_limit = today()
      )

    daily_preds <-
      single_asset_read_models_and_get_pred(
        pred_data = daily_data_for_modelling,
        trade_direction = trade_direction,
        Asset_of_interest = Asset_of_interest,
        save_path = save_path,
        model_string = "_daily_"
      )

    copula_data <-
      prepare_copula_data(
        asset_data = asset_data,
        couplua_assets = couplua_assets,
        Asset_of_interest = Asset_of_interest
      )

    copula_preds <-
      single_asset_read_models_and_get_pred(
        pred_data = copula_data,
        trade_direction = trade_direction,
        Asset_of_interest = Asset_of_interest,
        save_path = save_path,
        model_string = "_copula_"
      )

    technical_data <-
      create_technical_indicators(asset_data = asset_data_internal) %>%
      dplyr::select(-Price, -Low, -High, -Open)

    technical_data <-
      technical_data %>%
      filter(Asset == Asset_of_interest) %>%
      arrange(Date) %>%
      mutate(
        across(.cols = !contains("Date"),
               .fns = ~ lag(.) )
      )

    technical_preds <-
      single_asset_read_models_and_get_pred(
        pred_data = technical_data,
        trade_direction = trade_direction,
        Asset_of_interest = Asset_of_interest,
        save_path = save_path,
        model_string = "_technical_"
      )

    accumulating_probs <-
      asset_data_internal %>%
      distinct(Date, Asset) %>%
      left_join(macro_preds) %>%
      left_join(index_preds) %>%
      left_join(daily_preds) %>%
      left_join(copula_preds)%>%
      left_join(technical_preds) %>%
      left_join(macro_data) %>%
      left_join(copula_data)%>%
      left_join(
        daily_data_for_modelling %>%
          dplyr::select(Date, Asset,
                        (contains("perc_line_")&contains("_20")),
                        contains("Support"),
                        contains("Resistance"),
                        contains("Bear"),
                        contains("Bull"),
                        (contains("perc_line_")&contains("_50")),
                        contains("moving_average_markov_")
          )
      ) %>%
      left_join(index_pca_data) %>%
      left_join(
        technical_data %>%
          dplyr::select(Date, Asset,
                        (contains("perc_line_")&contains("_500")),
                        contains("Support"),
                        contains("Resistance"),
                        contains("Bear"),
                        contains("Bull"),
                        (contains("perc_line_")&contains("_500")),
                        contains("moving_average_markov_")
          )
      ) %>%
      distinct()

    combined_model_data <-
      accumulating_probs %>%
      arrange(Date) %>%
      fill(!contains("Date"), .direction = "down") %>%
      filter(if_all(.cols = everything(), ~!is.na(.)))

    mean_sd_values <-
      readRDS(
        glue::glue("{save_path}/{Asset_of_interest}_{trade_direction}_mean_sd_values.RDS")
      )

    combined_preds <-
      single_asset_read_models_and_get_pred(
        pred_data = combined_model_data,
        trade_direction = trade_direction,
        Asset_of_interest = Asset_of_interest,
        save_path = save_path,
        model_string = "_combined_"
      ) %>%
      mutate(
        Asset = Asset_of_interest
      ) %>%
      left_join(
        accumulating_probs %>%
          dplyr::select(Date, Asset, contains("pred"))
      ) %>%
      left_join(mean_sd_values)

    rm(macro_data, macro_phase_2_data, macro_train_data)
    rm(index_pca_data, index_pca_phase_2_data, index_pca_train_data)
    rm(daily_phase_2_data, daily_data_for_modelling, daily_data_for_modelling_train)
    rm(copula_data, copula_data_train, copula_phase_2_data)
    rm(technical_data, technical_data_train, technical_phase_2_data)
    gc()

    return(combined_preds)

  }

#' single_asset_read_models_and_get_pred
#'
#' @param pred_data
#' @param trade_direction
#' @param Asset_of_interest
#' @param save_path
#' @param model_string
#'
#' @returns
#' @export
#'
#' @examples
single_asset_read_models_and_get_pred <-
  function(
    pred_data = macro_test_data,
    trade_direction = "Long",
    Asset_of_interest = "EUR_USD",
    save_path = "C:/Users/nikhi/Documents/trade_data/single_asset_models_v2_adv",
    model_string = "_macro_"
  ) {

    models_in_path <-
      fs::dir_info(save_path) %>%
      filter(str_detect(path, Asset_of_interest),
             str_detect(path, trade_direction),
             str_detect(path, model_string)) %>%
      pull(path)

    model_preds <- pred_data %>% dplyr::select(Date)

    for (i in 1:length(models_in_path)) {

      model_object <-
        readRDS(models_in_path[i])

      if(str_detect(models_in_path[i], "logit")) {
        preds <-
          predict.glm(object = model_object, newdata = pred_data, type = "response")
      }

      if(str_detect(models_in_path[i], "lin")) {
        preds <-
          predict.lm(object = model_object, newdata = pred_data, type = "response")
      }

      model_preds <-
        model_preds %>%
        mutate(
          !!as.name( glue::glue("pred{model_string}{i}") ) := preds
        )

    }

    return(model_preds)

  }


#' prepare_macro_indicator_model_data
#'
#' @param asset_data
#' @param Asset_of_interest
#' @param interest_rates
#' @param cpi_data
#' @param sentiment_index
#' @param countries_for_int_strength
#' @param date_limit
#'
#' @returns
#' @export
#'
#' @examples
prepare_macro_indicator_model_data <-
  function(
    asset_data = Indices_Metals_Bonds[[1]],
    raw_macro_data = raw_macro_data,
    Asset_of_interest = "EUR_USD",
    interest_rates = interest_rates,
    cpi_data = cpi_data,
    gdp_data = gdp_data,
    unemp_data = unemp_data,
    manufac_pmi = manufac_pmi,
    USD_Macro = USD_Macro,
    EUR_Macro = EUR_Macro,
    sentiment_index = sentiment_index,
    countries_for_int_strength = countries_for_int_strength,
    date_limit = post_train_date_start
  ) {

    internal_asset_data <-
      asset_data %>%
      filter(Asset == Asset_of_interest) %>%
      filter(Date <= date_limit)

    interest_rates_diffs <-
      interest_rates %>%
      dplyr::select(Date_for_Join= Date, contains("_Diff"))

    cpi_data_diffs <-
      cpi_data %>%
      dplyr::select(Date_for_Join = Date, contains("_Diff"))

    interest_rate_strength_Index <-
      get_Interest_Rate_strength(
        interest_rates =interest_rates_diffs %>% mutate(Date = Date_for_Join),
        countries = countries_for_int_strength
      ) %>%
      mutate(Date_for_Join = Date)

    CPI_strength_index <-
      get_CPI_Rate_strength(
        cpi_data =cpi_data_diffs %>% mutate(Date = Date_for_Join),
        countries = countries_for_int_strength
      ) %>%
      mutate(Date_for_Join = Date)

    gdp_data_transform <-
      gdp_data %>%
      mutate(Date_for_Join = date) %>%
      dplyr::select(-date)

    unemp_data_transform <-
      unemp_data %>%
      mutate(Date_for_Join = date) %>%
      dplyr::select(-date)

    manufac_pmi_transform <-
      manufac_pmi %>%
      mutate(Date_for_Join = date) %>%
      dplyr::select(-date)

    USD_Macro <-
      USD_Macro %>%
      mutate(Date_for_Join = date) %>%
      dplyr::select(-date)

    EUR_Macro <-
      EUR_Macro %>%
      mutate(Date_for_Join = date) %>%
      dplyr::select(-date)

    macro_for_join <-
      internal_asset_data %>%
      distinct(Date) %>%
      mutate(Date_for_Join = as_date(Date)) %>%
      arrange(Date) %>%
      left_join(CPI_strength_index) %>%
      left_join(interest_rate_strength_Index) %>%
      left_join(sentiment_index %>% mutate(Date_for_Join = Date)) %>%
      left_join(gdp_data_transform) %>%
      left_join(unemp_data_transform) %>%
      left_join(manufac_pmi_transform) %>%
      left_join(USD_Macro) %>%
      left_join(EUR_Macro) %>%
      dplyr::select(-Date_for_Join) %>%
      arrange(Date) %>%
      mutate(
        across(.cols = !contains("Date"),
               .fns = ~ lag(.))
      ) %>%
      fill(!contains("Date"), .direction = "down") %>%
      filter(if_all(everything(), ~ !is.na(.))) %>%
      distinct()

    return(macro_for_join)

  }


#' prepare_macro_indicator_model
#'
#' @param macro_for_join
#' @param actual_wins_losses
#' @param Asset_of_interest
#' @param date_limit
#' @param stop_value_var
#' @param profit_value_var
#' @param period_var
#' @param bin_var_col
#' @param trade_direction
#' @param save_path
#'
#' @returns
#' @export
#'
#' @examples
prepare_macro_indicator_model <-
  function(macro_for_join = macro_train_data,
           actual_wins_losses = actual_wins_losses,
           Asset_of_interest = "EUR_USD",
           date_limit = post_train_date_start,
           stop_value_var = stop_value_var,
           profit_value_var = profit_value_var,
           period_var = period_var,
           bin_var_col = c("period_return_20_Price", "period_return_35_Price"),
           trade_direction = "Long",
           save_path = "C:/Users/nikhi/Documents/trade_data/single_asset_models_v2_adv"
  ) {

    actual_wins_losses_raw <-
      actual_wins_losses

    for (i in 1:length(bin_var_col)) {

      actual_wins_losses <-
        actual_wins_losses_raw %>%
        filter(trade_col == trade_direction) %>%
        filter(
          stop_factor == stop_value_var,
          profit_factor == profit_value_var,
          periods_ahead == period_var,
          Asset == Asset_of_interest
        ) %>%
        mutate(
          bin_var =
            case_when(
              !!as.name(bin_var_col[i]) > 0 & trade_col == "Short" ~ "win",
              !!as.name(bin_var_col[i]) <= 0 & trade_col == "Short" ~ "loss",

              !!as.name(bin_var_col[i]) > 0 & trade_col == "Long" ~ "win",
              !!as.name(bin_var_col[i]) <= 0 & trade_col == "Long" ~ "loss"

            )
        ) %>%
        filter(Date <= date_limit)

      check_date <-
        macro_for_join %>% pull(Date) %>% max() %>% as_date()

      check_date <- check_date <= date_limit

      message(glue::glue("Data Date is less than Train Date Max: {check_date}"))

      macro_for_join_model <-
        actual_wins_losses %>%
        dplyr::select(Date, Asset ,bin_var, matches(bin_var_col) ) %>%
        filter(
          Asset == Asset_of_interest
        ) %>%
        left_join(macro_for_join) %>%
        fill(!contains("Date"), .direction = "down") %>%
        filter(if_all(everything(),~!is.na(.))) %>%
        filter(Date <= date_limit)

      # rm(actual_wins_losses)

      macro_vars_for_indicator <-
        names(macro_for_join_model) %>%
        keep(~ !str_detect(.x, "Date") &
               !str_detect(.x, "bin_var") &
               !str_detect(.x, "Asset") &
               !str_detect(.x, paste(bin_var_col, collapse = "|") ) &
               !str_detect(.x, "period_return_")
        ) %>%
        unlist() %>%
        as.character()

      macro_indicator_formula_logit <-
        create_lm_formula(dependant = "bin_var=='win'",
                          independant = macro_vars_for_indicator)

      macro_indicator_model <-
        glm(formula = macro_indicator_formula_logit,
            data = macro_for_join_model,
            family = binomial("logit"))

      sig_coefs <-
        get_sig_coefs(model_object_of_interest = macro_indicator_model,
                      p_value_thresh_for_inputs = 0.25)

      rm(macro_indicator_model)
      gc()

      macro_indicator_formula_logit <-
        create_lm_formula(dependant = "bin_var=='win'",
                          independant = sig_coefs)

      macro_indicator_model <-
        glm(formula = macro_indicator_formula_logit,
            data = macro_for_join_model,
            family = binomial("logit"))

      summary(macro_indicator_model)

      message(glue::glue("Passed Macro Model {Asset_of_interest} {i}"))

      saveRDS(object = macro_indicator_model,
              file =
                glue::glue("{save_path}/{Asset_of_interest}_{trade_direction}_{bin_var_col[i]}_macro_logit.RDS")
      )

      rm(macro_indicator_model)
      gc()

      macro_indicator_formula_lin <-
        create_lm_formula(dependant = bin_var_col[i],
                          independant = macro_vars_for_indicator)

      macro_indicator_model_lin <-
        lm(formula = macro_indicator_formula_lin,
           data = macro_for_join_model)

      sig_coefs <-
        get_sig_coefs(model_object_of_interest = macro_indicator_model_lin,
                      p_value_thresh_for_inputs = 0.25)

      macro_indicator_formula_lin <-
        create_lm_formula(dependant = bin_var_col[i],
                          independant = sig_coefs)

      macro_indicator_model_lin <-
        lm(formula = macro_indicator_formula_lin,
           data = macro_for_join_model)

      summary(macro_indicator_model_lin)

      message(glue::glue("Passed Macro Model Linear {Asset_of_interest} {i}"))

      saveRDS(object = macro_indicator_model_lin,
              file =
                glue::glue("{save_path}/{Asset_of_interest}_{trade_direction}_{bin_var_col[i]}_macro_lin.RDS")
      )

      rm(macro_indicator_model_lin)
      gc()

    }

    rm(actual_wins_losses)
    rm(actual_wins_losses_raw)

    gc()

  }

#' get_bonds_index
#'
#' @param index_data
#'
#' @return
#' @export
#'
#' @examples
get_USD_index_for_models <-
  function(index_data) {

    USD_assets <-
      index_data %>%
      distinct(Asset) %>%
      filter(str_detect(Asset, "AUD_USD|EUR_USD|XAU_USD|GBP_USD|USD_JPY|NZD_USD|USD_CHF|USD_SEK|USD_NOK")) %>%
      pull(Asset)

    major_USD_log_cumulative <-
      USD_assets %>%
      map_dfr(
        ~
          create_log_cumulative_returns(
            asset_data_to_use =
              index_data %>%
              filter(Asset %in% USD_assets),
            asset_to_use = c(.x[1]),
            price_col = "Open",
            return_long_format = TRUE
          )
      ) %>%
      left_join(
        index_data %>%
          filter(Asset %in% USD_assets) %>%
          dplyr::select(Date, Asset, Price, Open)
      )

    pc_USD_global <-
      create_PCA_Asset_Index(
        asset_data_to_use = major_USD_log_cumulative %>%
          group_by(Asset) %>%
          mutate(
            Return_Index_Diff = ((Price - Open)/Open)*100
          ) %>%
          ungroup() %>%
          filter(!is.na(Return_Index_Diff)),
        asset_to_use =  USD_assets,
        price_col = "Return_Index_Diff",
        scale_values = TRUE
      ) %>%
      arrange(Date) %>%
      mutate(
        across(contains("PC[0-9]"), ~cumsum(.))
      ) %>%
      rename(PC1_USD = PC1,
             PC2_USD = PC2,
             PC3_USD = PC3,
             PC4_USD = PC4) %>%
      dplyr::select(Date, matches("PC[0-9]_USD"))

    rm(major_USD_log_cumulative)
    gc()

    return(pc_USD_global)

  }

#' get_bonds_index
#'
#' @param index_data
#'
#' @return
#' @export
#'
#' @examples
get_EUR_index_for_models <-
  function(index_data) {

    EUR_assets <-
      index_data %>%
      distinct(Asset) %>%
      filter(str_detect(Asset, "EUR_AUD|EUR_USD|XAU_EUR|EUR_GBP|EUR_NZD|EUR_CHF|XAG_EUR|EUR_SEK|EUR_JPY|USD_JPY|EU50_EUR")) %>%
      pull(Asset)

    major_EUR_log_cumulative <-
      EUR_assets %>%
      map_dfr(
        ~
          create_log_cumulative_returns(
            asset_data_to_use =
              index_data %>%
              filter(Asset %in% EUR_assets),
            asset_to_use = c(.x[1]),
            price_col = "Open",
            return_long_format = TRUE
          )
      ) %>%
      left_join(
        index_data %>%
          filter(Asset %in% EUR_assets) %>%
          dplyr::select(Date, Asset, Price, Open)
      )

    pc_EUR_global <-
      create_PCA_Asset_Index(
        asset_data_to_use = major_EUR_log_cumulative %>%
          group_by(Asset) %>%
          mutate(
            Return_Index_Diff = ((Price - Open)/Open)*100
          ) %>%
          ungroup() %>%
          filter(!is.na(Return_Index_Diff)),
        asset_to_use =  EUR_assets,
        price_col = "Return_Index_Diff",
        scale_values = TRUE
      ) %>%
      arrange(Date) %>%
      mutate(
        across(contains("PC[0-9]"), ~cumsum(.))
      ) %>%
      rename(PC1_EUR = PC1,
             PC2_EUR = PC2,
             PC3_EUR = PC3,
             PC4_EUR = PC4) %>%
      dplyr::select(Date, matches("PC[0-9]_EUR"))

    rm(major_EUR_log_cumulative)
    gc()

    return(pc_EUR_global)

  }

#' get_bonds_index
#'
#' @param index_data
#'
#' @return
#' @export
#'
#' @examples
get_GBP_index_for_models <-
  function(index_data) {

    GBP_assets <-
      index_data %>%
      distinct(Asset) %>%
      filter(str_detect(Asset, "GBP_AUD|GBP_NZD|XAU_GBP|EUR_GBP|GBP_CHF|UK100_GBP|XAG_GBP|GBP_JPY|GBP_USD|GBP_CAD|USD_JPY")) %>%
      pull(Asset)

    major_GBP_log_cumulative <-
      GBP_assets %>%
      map_dfr(
        ~
          create_log_cumulative_returns(
            asset_data_to_use =
              index_data %>%
              filter(Asset %in% GBP_assets),
            asset_to_use = c(.x[1]),
            price_col = "Open",
            return_long_format = TRUE
          )
      ) %>%
      left_join(
        index_data %>%
          filter(Asset %in% GBP_assets) %>%
          dplyr::select(Date, Asset, Price, Open)
      )

    pc_GBP_global <-
      create_PCA_Asset_Index(
        asset_data_to_use = major_GBP_log_cumulative %>%
          group_by(Asset) %>%
          mutate(
            Return_Index_Diff = ((Price - Open)/Open)*100
          ) %>%
          ungroup() %>%
          filter(!is.na(Return_Index_Diff)),
        asset_to_use =  GBP_assets,
        price_col = "Return_Index_Diff",
        scale_values = TRUE
      ) %>%
      arrange(Date) %>%
      mutate(
        across(contains("PC[0-9]"), ~cumsum(.))
      ) %>%
      rename(PC1_GBP = PC1,
             PC2_GBP = PC2,
             PC3_GBP = PC3,
             PC4_GBP = PC4) %>%
      dplyr::select(Date, matches("PC[0-9]_GBP"))

    rm(major_GBP_log_cumulative)
    gc()

    return(pc_GBP_global)

  }

#' get_bonds_index
#'
#' @param index_data
#'
#' @return
#' @export
#'
#' @examples
get_AUD_index_for_models <-
  function(index_data) {

    AUD_assets <-
      index_data %>%
      distinct(Asset) %>%
      filter(str_detect(Asset, "GBP_AUD|XAU_AUD|EUR_AUD|AUD_USD|AU200_AUD|AUD_JPY|XAG_AUD|USD_JPY|NZD_USD")) %>%
      pull(Asset)

    major_AUD_log_cumulative <-
      AUD_assets %>%
      map_dfr(
        ~
          create_log_cumulative_returns(
            asset_data_to_use =
              index_data %>%
              filter(Asset %in% AUD_assets),
            asset_to_use = c(.x[1]),
            price_col = "Open",
            return_long_format = TRUE
          )
      ) %>%
      left_join(
        index_data %>%
          filter(Asset %in% AUD_assets) %>%
          dplyr::select(Date, Asset, Price, Open)
      )

    pc_AUD_global <-
      create_PCA_Asset_Index(
        asset_data_to_use = major_AUD_log_cumulative %>%
          group_by(Asset) %>%
          mutate(
            Return_Index_Diff = ((Price - Open)/Open)*100
          ) %>%
          ungroup() %>%
          filter(!is.na(Return_Index_Diff)),
        asset_to_use =  AUD_assets,
        price_col = "Return_Index_Diff",
        scale_values = TRUE
      ) %>%
      arrange(Date) %>%
      mutate(
        across(contains("PC[0-9]"), ~cumsum(.))
      ) %>%
      rename(PC1_AUD = PC1,
             PC2_AUD = PC2,
             PC3_AUD = PC3,
             PC4_AUD = PC4) %>%
      dplyr::select(Date, matches("PC[0-9]_AUD"))

    rm(major_AUD_log_cumulative)
    gc()

    return(pc_AUD_global)

  }

#' get_bonds_index
#'
#' @param index_data
#'
#' @return
#' @export
#'
#' @examples
get_NZD_index_for_models <-
  function(index_data) {

    NZD_assets <-
      index_data %>%
      distinct(Asset) %>%
      filter(str_detect(Asset, "NZD_USD|EUR_NZD|GBP_NZD|AUD_USD|XAG_NZD|XAU_AUD|EUR_AUD|GBP_AUD|AUD_USD")) %>%
      pull(Asset)

    major_NZD_log_cumulative <-
      NZD_assets %>%
      map_dfr(
        ~
          create_log_cumulative_returns(
            asset_data_to_use =
              index_data %>%
              filter(Asset %in% NZD_assets),
            asset_to_use = c(.x[1]),
            price_col = "Open",
            return_long_format = TRUE
          )
      ) %>%
      left_join(
        index_data %>%
          filter(Asset %in% NZD_assets) %>%
          dplyr::select(Date, Asset, Price, Open)
      )

    pc_NZD_global <-
      create_PCA_Asset_Index(
        asset_data_to_use = major_NZD_log_cumulative %>%
          group_by(Asset) %>%
          mutate(
            Return_Index_Diff = ((Price - Open)/Open)*100
          ) %>%
          ungroup() %>%
          filter(!is.na(Return_Index_Diff)),
        asset_to_use =  NZD_assets,
        price_col = "Return_Index_Diff",
        scale_values = TRUE
      ) %>%
      arrange(Date) %>%
      mutate(
        across(contains("PC[0-9]"), ~cumsum(.))
      ) %>%
      rename(PC1_NZD = PC1,
             PC2_NZD = PC2,
             PC3_NZD = PC3,
             PC4_NZD = PC4) %>%
      dplyr::select(Date, matches("PC[0-9]_NZD"))

    rm(major_NZD_log_cumulative, NZD_assets)
    gc()

    return(pc_NZD_global)

  }

#' get_bonds_index
#'
#' @param index_data
#'
#' @return
#' @export
#'
#' @examples
get_USD_AND_STOCKS_index_for_models <-
  function(index_data) {

    USD_STOCKS_assets <-
      index_data %>%
      distinct(Asset) %>%
      filter(str_detect(Asset, "US2000_USD|SPX500_USD|EU50_EUR|SG30_SGD|AU200_AUD|UK100_GBP|HK33_HKD|FR40_EUR|CH20_CHF|EUR_USD|GBP_USD|AUD_USD|NZD_USD|USD_SGD|USD_CAD")) %>%
      pull(Asset)

    major_USD_STOCKS_log_cumulative <-
      USD_STOCKS_assets %>%
      map_dfr(
        ~
          create_log_cumulative_returns(
            asset_data_to_use =
              index_data %>%
              filter(Asset %in% USD_STOCKS_assets),
            asset_to_use = c(.x[1]),
            price_col = "Open",
            return_long_format = TRUE
          )
      ) %>%
      left_join(
        index_data %>%
          filter(Asset %in% USD_STOCKS_assets) %>%
          dplyr::select(Date, Asset, Price, Open)
      )

    pc_USD_STOCKS_global <-
      create_PCA_Asset_Index(
        asset_data_to_use = major_USD_STOCKS_log_cumulative %>%
          group_by(Asset) %>%
          mutate(
            Return_Index_Diff = ((Price - Open)/Open)*100
          ) %>%
          ungroup() %>%
          filter(!is.na(Return_Index_Diff)),
        asset_to_use =  USD_STOCKS_assets,
        price_col = "Return_Index_Diff",
        scale_values = TRUE
      ) %>%
      arrange(Date) %>%
      mutate(
        across(contains("PC[0-9]"), ~cumsum(.))
      ) %>%
      rename(PC1_USD_STOCKS = PC1,
             PC2_USD_STOCKS = PC2,
             PC3_USD_STOCKS = PC3,
             PC4_USD_STOCKS = PC4) %>%
      dplyr::select(Date, matches("PC[0-9]_USD_STOCKS"))

    rm(major_USD_STOCKS_log_cumulative, USD_STOCKS_assets)
    gc()

    return(pc_USD_STOCKS_global)

  }

#' get_bonds_index
#'
#' @param index_data
#'
#' @return
#' @export
#'
#' @examples
get_COMMOD_index_for_models <-
  function(index_data) {

    COMMOD_assets <-
      index_data %>%
      distinct(Asset) %>%
      filter(str_detect(Asset, "WTICO_USD|BCO_USD|XCU_USD|NATGAS_USD|XAU_USD|XAG_USD")) %>%
      pull(Asset)

    major_COMMOD_log_cumulative <-
      COMMOD_assets %>%
      map_dfr(
        ~
          create_log_cumulative_returns(
            asset_data_to_use =
              index_data %>%
              filter(Asset %in% COMMOD_assets),
            asset_to_use = c(.x[1]),
            price_col = "Open",
            return_long_format = TRUE
          )
      ) %>%
      left_join(
        index_data %>%
          filter(Asset %in% COMMOD_assets) %>%
          dplyr::select(Date, Asset, Price, Open)
      )

    pc_COMMOD_global <-
      create_PCA_Asset_Index(
        asset_data_to_use = major_COMMOD_log_cumulative %>%
          group_by(Asset) %>%
          mutate(
            Return_Index_Diff = ((Price - Open)/Open)*100
          ) %>%
          ungroup() %>%
          filter(!is.na(Return_Index_Diff)),
        asset_to_use =  COMMOD_assets,
        price_col = "Return_Index_Diff",
        scale_values = TRUE
      ) %>%
      arrange(Date) %>%
      mutate(
        across(contains("PC[0-9]"), ~cumsum(.))
      ) %>%
      rename(PC1_COMMOD = PC1,
             PC2_COMMOD = PC2,
             PC3_COMMOD = PC3,
             PC4_COMMOD = PC4) %>%
      dplyr::select(Date, matches("PC[0-9]_COMMOD"))

    rm(major_COMMOD_log_cumulative, COMMOD_assets)
    gc()

    return(pc_COMMOD_global)

  }

#' get_pca_index_indicator_data
#'
#' @param asset_data
#' @param Asset_of_interest
#' @param equity_index
#' @param gold_index
#' @param silver_index
#' @param bonds_index
#' @param date_limit
#'
#' @returns
#' @export
#'
#' @examples
get_pca_index_indicator_data <-
  function(
    asset_data = Indices_Metals_Bonds[[1]],
    Asset_of_interest = "EUR_USD",
    equity_index = equity_index,
    gold_index = gold_index,
    silver_index = silver_index,
    bonds_index = bonds_index,
    USD_index = USD_index,
    EUR_index = EUR_index,
    GBP_index = GBP_index,
    AUD_index = AUD_index,
    COMMOD_index = COMMOD_index,
    USD_STOCKS_index = USD_STOCKS_index,
    NZD_index = NZD_index,

    date_limit = post_train_date_start,
    index_lag_cols = 1,
    sum_rolling_length = 30
  ) {

    internal_asset_data <-
      asset_data %>%
      filter(Asset == Asset_of_interest) %>%
      filter(Date <= date_limit)

    indexes_data_for_join <-
      internal_asset_data %>%
      ungroup() %>%
      distinct(Date) %>%
      arrange(Date) %>%
      left_join(equity_index  %>% dplyr::select(-Average_PCA) )%>%
      left_join(gold_index  %>% dplyr::select(-Average_PCA) )%>%
      left_join(silver_index  %>% dplyr::select(-Average_PCA) )%>%
      left_join(bonds_index  %>% dplyr::select(-Average_PCA) ) %>%
      left_join(USD_index) %>%
      left_join(EUR_index) %>%
      left_join(GBP_index) %>%
      left_join(AUD_index) %>%
      left_join(COMMOD_index) %>%
      left_join(USD_STOCKS_index) %>%
      left_join(NZD_index) %>%
      fill(!contains("Date"), .direction = "down") %>%
      filter(if_all(everything(), ~ !is.na(.))) %>%
      mutate(
        across(.cols = !contains("Date"), .fns = ~ lag(.))
      ) %>%
      filter(if_all(everything(), ~ !is.na(.))) %>%
      distinct()

    rolling_sums <-
      indexes_data_for_join %>%
      arrange(Date) %>%
      mutate(
        across(.cols = !contains("Date"),
               .fns = ~ slider::slide_dbl(.x = .,
                                          .f = ~ sum(.x, na.rm = T),
                                          .before = sum_rolling_length )
        )
      )

    names(rolling_sums) <-
      names(rolling_sums) %>%
      map( ~
             ifelse(
               !str_detect(.x, "Date"),
               paste0(.x, "_sum"),
               .x
             )
      ) %>%
      unlist()

    rolling_sums2 <-
      indexes_data_for_join %>%
      arrange(Date) %>%
      mutate(
        across(.cols = !contains("Date"),
               .fns = ~ slider::slide_dbl(.x = .,
                                          .f = ~ sum(.x, na.rm = T),
                                          .before = round(sum_rolling_length/2) )
        )
      )

    names(rolling_sums2) <-
      names(rolling_sums2) %>%
      map( ~
             ifelse(
               !str_detect(.x, "Date"),
               paste0(.x, "_sum_2"),
               .x
             )
      ) %>%
      unlist()

    rolling_average <-
      indexes_data_for_join %>%
      arrange(Date) %>%
      mutate(
        across(.cols = !contains("Date"),
               .fns = ~ slider::slide_dbl(.x = .,
                                          .f = ~ sum(.x, na.rm = T),
                                          .before = sum_rolling_length )
        )
      )%>%
      mutate(
        across(.cols = !contains("Date"),
               .fns = ~ slider::slide_dbl(.x = .,
                                          .f = ~ mean(.x, na.rm = T),
                                          .before = sum_rolling_length )
        )
      )

    names(rolling_average) <-
      names(rolling_average) %>%
      map( ~
             ifelse(
               !str_detect(.x, "Date"),
               paste0(.x, "_average"),
               .x
             )
      ) %>%
      unlist()

    rolling_average2 <-
      indexes_data_for_join %>%
      arrange(Date) %>%
      mutate(
        across(.cols = !contains("Date"),
               .fns = ~ slider::slide_dbl(.x = .,
                                          .f = ~ sum(.x, na.rm = T),
                                          .before = round(sum_rolling_length/2) )
        )
      )%>%
      mutate(
        across(.cols = !contains("Date"),
               .fns = ~ slider::slide_dbl(.x = .,
                                          .f = ~ mean(.x, na.rm = T),
                                          .before = round(sum_rolling_length/2) )
        )
      )

    names(rolling_average2) <-
      names(rolling_average2) %>%
      map( ~
             ifelse(
               !str_detect(.x, "Date"),
               paste0(.x, "_average_2"),
               .x
             )
      ) %>%
      unlist()

    indexes_data_for_join <-
      indexes_data_for_join %>%
      left_join(rolling_sums)%>%
      left_join(rolling_sums2)%>%
      left_join(rolling_average2) %>%
      left_join(rolling_average)

    for (k in 1:index_lag_cols) {

      indexes_data_for_join <-
        indexes_data_for_join %>%
        arrange(Date) %>%
        mutate(
          !!as.name( glue::glue("PC1_Equities_{k}_lag") ) :=
            lag(PC1_Equities, k),

          !!as.name( glue::glue("PC1_Gold_Equities_{k}_lag") ) :=
            lag(PC1_Gold_Equities, k),

          !!as.name( glue::glue("PC1_Silver_Equities_{k}_lag") ) :=
            lag(PC1_Silver_Equities, k),

          !!as.name( glue::glue("PC1_Bonds_Equities_{k}_lag") ) :=
            lag(PC1_Bonds_Equities, k),

          !!as.name( glue::glue("PC1_USD_{k}_lag") ) :=
            lag(PC1_USD, k),

          !!as.name( glue::glue("PC1_EUR_{k}_lag") ) :=
            lag(PC1_EUR, k),

          !!as.name( glue::glue("PC1_GBP_{k}_lag") ) :=
            lag(PC1_GBP, k)

        )

    }

    indexes_data_for_join <-
      indexes_data_for_join %>%
      arrange(Date) %>%
      fill(!contains("Date"), .direction = "down")


    return(indexes_data_for_join)

  }

#' Title
#'
#' @param index_pca_data
#' @param actual_wins_losses
#' @param Asset_of_interest
#' @param date_limit
#' @param stop_value_var
#' @param profit_value_var
#' @param period_var
#' @param bin_var_col
#' @param trade_direction
#' @param save_path
#'
#' @returns
#' @export
#'
#' @examples
prepare_index_indicator_model <-
  function(
    index_pca_data = index_pca_data,
    actual_wins_losses = actual_wins_losses,
    Asset_of_interest = "EUR_USD",
    date_limit = post_train_date_start,
    stop_value_var = stop_value_var,
    profit_value_var = profit_value_var,
    period_var = period_var,
    bin_var_col = c("period_return_20_Price", "period_return_35_Price"),
    trade_direction = "Long",
    save_path = "C:/Users/nikhi/Documents/trade_data/single_asset_models_v2_adv"
  ) {

    actual_wins_losses_raw <-
      actual_wins_losses

    for (i in 1:length(bin_var_col)) {

      actual_wins_losses <-
        actual_wins_losses_raw %>%
        filter(trade_col == trade_direction) %>%
        filter(
          stop_factor == stop_value_var,
          profit_factor == profit_value_var,
          periods_ahead == period_var,
          Asset == Asset_of_interest
        ) %>%
        mutate(
          bin_var =
            case_when(
              !!as.name(bin_var_col[i]) > 0 & trade_col == "Short" ~ "win",
              !!as.name(bin_var_col[i]) <= 0 & trade_col == "Short" ~ "loss",

              !!as.name(bin_var_col[i]) > 0 & trade_col == "Long" ~ "win",
              !!as.name(bin_var_col[i]) <= 0 & trade_col == "Long" ~ "loss"

            )
        ) %>%
        filter(Date <= date_limit)

      index_for_join_model <-
        actual_wins_losses %>%
        dplyr::select(Date, Asset ,bin_var, matches(bin_var_col) ) %>%
        filter(
          Asset == Asset_of_interest
        ) %>%
        left_join(index_pca_data) %>%
        arrange(Date) %>%
        fill(!contains("Date"), .direction = "down") %>%
        filter(if_all(everything(),~!is.na(.))) %>%
        filter(Date <= date_limit)

      check_date <-
        index_for_join_model %>% pull(Date) %>% max() %>% as_date()

      check_date <- check_date <= date_limit

      message(glue::glue("Data Date is less than Train Date Max: {check_date}"))


      # rm(actual_wins_losses)

      index_vars_for_indicator <-
        names(index_for_join_model) %>%
        keep(~ !str_detect(.x, "Date") &
               !str_detect(.x, "bin_var") &
               !str_detect(.x, "Asset") &
               !str_detect(.x, paste(bin_var_col, collapse = "|") ) &
               !str_detect(.x, "period_return_")
        ) %>%
        unlist() %>%
        as.character()

      index_indicator_formula_logit <-
        create_lm_formula(dependant = "bin_var=='win'",
                          independant = index_vars_for_indicator)

      index_indicator_model <-
        glm(formula = index_indicator_formula_logit,
            data = index_for_join_model,
            family = binomial("logit"))

      sig_coefs <-
        get_sig_coefs(model_object_of_interest = index_indicator_model,
                      p_value_thresh_for_inputs = 0.25)

      rm(index_indicator_model)
      gc()

      index_indicator_formula_logit <-
        create_lm_formula(dependant = "bin_var=='win'",
                          independant = sig_coefs)

      index_indicator_model <-
        glm(formula = index_indicator_formula_logit,
            data = index_for_join_model,
            family = binomial("logit"))

      summary(index_indicator_model)

      message(glue::glue("Passed Index Model {Asset_of_interest} {i}"))

      saveRDS(object = index_indicator_model,
              file =
                glue::glue("{save_path}/{Asset_of_interest}_{trade_direction}_{bin_var_col[i]}_index_logit.RDS")
      )

      rm(index_indicator_model)
      gc()

      index_indicator_formula_lin <-
        create_lm_formula(dependant = bin_var_col[i],
                          independant = index_vars_for_indicator)

      index_indicator_model_lin <-
        lm(formula = index_indicator_formula_lin,
           data = index_for_join_model)

      sig_coefs <-
        get_sig_coefs(model_object_of_interest = index_indicator_model_lin,
                      p_value_thresh_for_inputs = 0.25)

      index_indicator_formula_lin <-
        create_lm_formula(dependant = bin_var_col[i],
                          independant = sig_coefs)

      index_indicator_model_lin <-
        lm(formula = index_indicator_formula_lin,
           data = index_for_join_model)

      summary(index_indicator_model_lin)

      message(glue::glue("Passed index Model Linear {Asset_of_interest} {i}"))

      saveRDS(object = index_indicator_model_lin,
              file =
                glue::glue("{save_path}/{Asset_of_interest}_{trade_direction}_{bin_var_col[i]}_index_lin.RDS")
      )

      rm(index_indicator_model_lin)
      gc()

    }

  }

#' get_daily_indicators
#'
#' @param Daily_Data
#' @param asset_data
#' @param Asset_of_interest
#'
#' @return
#' @export
#'
#' @examples
get_daily_indicators <-
  function(Daily_Data = All_Daily_Data,
           asset_data = Indices_Metals_Bonds[[1]],
           Asset_of_interest = "EUR_USD",
           return_joined_only = TRUE) {

    daily_technical_indicators <-
      Daily_Data %>%
      filter(Asset == Asset_of_interest) %>%
      create_technical_indicators_daily() %>%
      group_by(Asset) %>%
      arrange(Date, .by_group = TRUE) %>%
      group_by(Asset) %>%
      mutate(
        across(.cols = !contains("Date") & !contains("Asset"),
               .fns = ~ lag(.))
      ) %>%
      ungroup()

    new_names <- names(daily_technical_indicators) %>%
      map(
        ~ case_when(
          .x %in% c("Date","Asset", "Price", "High", "Low", "Open") ~ .x,
          TRUE ~ paste0("Daily_", .x)
        )
      ) %>%
      unlist() %>%
      as.character()

    names(daily_technical_indicators) <- new_names

    joined_dat <-
      asset_data %>%
      filter(Asset == Asset_of_interest) %>%
      mutate(
        Date_for_join = as_date(Date)
      ) %>%
      left_join(daily_technical_indicators %>%
                  mutate(Date_for_join = as_date(Date)) %>%
                  dplyr::select(-Date, -Price, -Low, -Open, -High),
                by = c("Date_for_join", "Asset")
      ) %>%
      fill(
        contains("Daily"), .direction = "down"
      ) %>%
      distinct()

    if(return_joined_only == TRUE) {
      return(joined_dat)
    } else {
      return(
        list(joined_dat,
             daily_technical_indicators)
        )
    }

  }

#' prepare_daily_indicator_data
#'
#' @param asset_data
#' @param All_Daily_Data
#' @param Asset_of_interest
#' @param equity_index
#' @param gold_index
#' @param silver_index
#' @param bonds_index
#' @param USD_index
#' @param EUR_index
#' @param GBP_index
#' @param date_limit
#'
#' @returns
#' @export
#'
#' @examples
prepare_daily_indicator_data <-
  function(
    asset_data = Indices_Metals_Bonds[[1]],
    All_Daily_Data = All_Daily_Data,
    Asset_of_interest = "EUR_USD",
    date_limit = date_train_end
    ) {

    asset_data_internal <-
      asset_data %>%
      filter(Asset == Asset_of_interest) %>%
      filter(Date <= date_limit)

    All_Daily_Data_internal <-
      All_Daily_Data %>%
      filter(Asset == Asset_of_interest) %>%
      filter(Date <= date_limit)

    daily_indicator <-
      get_daily_indicators(
        Daily_Data = All_Daily_Data_internal,
        asset_data = asset_data_internal,
        Asset_of_interest = Asset_of_interest,
        return_joined_only = FALSE
      )

    daily_indicator <-
      daily_indicator[[1]] %>%
      dplyr::select(-Price, -Open, -Low, -High, -Vol., -Date_for_join) %>%
      mutate(
        across(.cols = !contains("Date") & !contains("Asset"),
               .fns = ~ lag(.))
      ) %>%
      arrange(Date) %>%
      fill(!contains("Date"), .direction = "down") %>%
      filter(if_all(everything(), ~ !is.na(.))) %>%
      mutate(
        Date = as_datetime(Date, tz = "Australia/Canberra")
      ) %>%
      distinct()

    return(daily_indicator)

  }

#' prepare_daily_indicator_model
#'
#' @param daily_indicator
#' @param actual_wins_losses
#' @param Asset_of_interest
#' @param date_limit
#' @param stop_value_var
#' @param profit_value_var
#' @param period_var
#' @param bin_var_col
#' @param trade_direction
#' @param save_path
#'
#' @returns
#' @export
#'
#' @examples
prepare_daily_indicator_model <-
  function(
    daily_indicator = daily_data_for_modelling_train,
    actual_wins_losses = actual_wins_losses,
    Asset_of_interest = "EUR_USD",
    date_limit = date_train_end,
    stop_value_var = stop_value_var,
    profit_value_var = profit_value_var,
    period_var = period_var,
    bin_var_col = c("period_return_20_Price", "period_return_35_Price"),
    trade_direction = "Long",
    save_path = "C:/Users/nikhi/Documents/trade_data/single_asset_models_v2_adv"
    ) {

    actual_wins_losses_raw <-
      actual_wins_losses

    daily_indicator <-
      daily_indicator %>%
      filter(Asset == Asset_of_interest,
             Date <= date_limit)

    for (i in 1:length(bin_var_col)) {

      message("Entered Loop")

      actual_wins_losses <-
        actual_wins_losses_raw %>%
        filter(trade_col == trade_direction) %>%
        filter(
          stop_factor == stop_value_var,
          profit_factor == profit_value_var,
          periods_ahead == period_var,
          Asset == Asset_of_interest
        ) %>%
        mutate(
          bin_var =
            case_when(
              !!as.name(bin_var_col[i]) > 0 & trade_col == "Short" ~ "win",
              !!as.name(bin_var_col[i]) <= 0 & trade_col == "Short" ~ "loss",

              !!as.name(bin_var_col[i]) > 0 & trade_col == "Long" ~ "win",
              !!as.name(bin_var_col[i]) <= 0 & trade_col == "Long" ~ "loss"

            )
        ) %>%
        filter(Date <= date_limit)

      check_date <-
        daily_indicator %>% pull(Date) %>% max() %>% as_date()

      check_date <- check_date <= date_limit

      message(glue::glue("Data Date is less than Train Date Max: {check_date}"))

      daily_for_join_model <-
        actual_wins_losses %>%
        dplyr::select(Date, Asset ,bin_var, matches(bin_var_col) ) %>%
        filter(
          Asset == Asset_of_interest
        ) %>%
        left_join(daily_indicator) %>%
        fill(!contains("Date"), .direction = "down") %>%
        filter(if_all(everything(),~!is.na(.))) %>%
        filter(if_all(everything(),~!is.nan(.))) %>%
        filter(if_all(everything(),~!is.infinite(.))) %>%
        filter(Date <= date_limit)

      # rm(actual_wins_losses)

      daily_vars_for_indicator <-
        names(daily_for_join_model) %>%
        keep(~ !str_detect(.x, "Date") &
               !str_detect(.x, "bin_var") &
               !str_detect(.x, "Asset") &
               !str_detect(.x, paste(bin_var_col, collapse = "|") ) &
               !str_detect(.x, "period_return_")
        ) %>%
        unlist() %>%
        as.character()

      daily_indicator_formula_logit <-
        create_lm_formula(dependant = "bin_var=='win'",
                          independant = daily_vars_for_indicator)

      daily_indicator_model <-
        glm(formula = daily_indicator_formula_logit,
            data = daily_for_join_model,
            family = binomial("logit"))

      sig_coefs <-
        get_sig_coefs(model_object_of_interest = daily_indicator_model,
                      p_value_thresh_for_inputs = 0.25)

      rm(daily_indicator_model)
      gc()

      daily_indicator_formula_logit <-
        create_lm_formula(dependant = "bin_var=='win'",
                          independant = sig_coefs)

      daily_indicator_model <-
        glm(formula = daily_indicator_formula_logit,
            data = daily_for_join_model,
            family = binomial("logit"))

      summary(daily_indicator_model)

      message(glue::glue("Passed daily Model {Asset_of_interest} {i}"))

      saveRDS(object = daily_indicator_model,
              file =
                glue::glue("{save_path}/{Asset_of_interest}_{trade_direction}_{bin_var_col[i]}_daily_logit.RDS")
      )

      rm(daily_indicator_model)
      gc()

      daily_indicator_formula_lin <-
        create_lm_formula(dependant = bin_var_col[i],
                          independant = daily_vars_for_indicator)

      daily_indicator_model_lin <-
        lm(formula = daily_indicator_formula_lin,
           data = daily_for_join_model)

      sig_coefs <-
        get_sig_coefs(model_object_of_interest = daily_indicator_model_lin,
                      p_value_thresh_for_inputs = 0.25)

      daily_indicator_formula_lin <-
        create_lm_formula(dependant = bin_var_col[i],
                          independant = sig_coefs)

      daily_indicator_model_lin <-
        lm(formula = daily_indicator_formula_lin,
           data = daily_for_join_model)

      summary(daily_indicator_model_lin)

      message(glue::glue("Passed daily Model Linear {Asset_of_interest} {i}"))

      saveRDS(object = daily_indicator_model_lin,
              file =
                glue::glue("{save_path}/{Asset_of_interest}_{trade_direction}_{bin_var_col[i]}_daily_lin.RDS")
      )

      rm(daily_indicator_model_lin)
      gc()

    }

  }


#' prepare_copula_data
#'
#' @param asset_data
#' @param couplua_assets
#' @param Asset_of_interest
#'
#' @returns
#' @export
#'
#' @examples
prepare_copula_data <-
  function(
    asset_data = asset_data,
    couplua_assets = couplua_assets,
    Asset_of_interest = Asset_of_interest,
    date_limit = today()
    ) {

    copula_accumulation <- list()
    asset_data_internal <-
      asset_data %>%
      filter(
        Date <= date_limit
      )


    for (i in 1:length(couplua_assets)) {

      copula_accumulation[[i]] <-
        estimating_dual_copula(
          asset_data_to_use = asset_data_internal,
          asset_to_use = c(Asset_of_interest, couplua_assets[i]),
          price_col = "Open",
          rolling_period = 100,
          samples_for_MLE = 0.15,
          test_samples = 0.85
        ) %>%
        ungroup() %>%
        dplyr::select(Date, contains("_cor")|contains("_lm"))

    }

    copula_data <-
      asset_data_internal %>%
      filter(Asset == Asset_of_interest) %>%
      distinct(Date) %>%
      left_join(copula_accumulation %>%
                reduce(left_join) ) %>%
      arrange(Date) %>%
      fill(!contains("Date"), .direction = "down") %>%
      distinct()


    return(copula_data)

  }

#' prepare_copula_model
#'
#' @param actual_wins_losses
#' @param Asset_of_interest
#' @param date_limit
#' @param stop_value_var
#' @param profit_value_var
#' @param period_var
#' @param bin_var_col
#' @param trade_direction
#' @param save_path
#' @param copula_data
#'
#' @returns
#' @export
#'
#' @examples
prepare_copula_model <-
  function(
    copula_data = copula_data,
    actual_wins_losses = actual_wins_losses,
    Asset_of_interest = "EUR_USD",
    date_limit = date_train_end,
    stop_value_var = stop_value_var,
    profit_value_var = profit_value_var,
    period_var = period_var,
    bin_var_col = c("period_return_20_Price", "period_return_35_Price"),
    trade_direction = "Long",
    save_path = "C:/Users/nikhi/Documents/trade_data/single_asset_models_v2_adv"
  ) {

    actual_wins_losses_raw <-
      actual_wins_losses

    copula_data <-
      copula_data %>%
      filter(Date <= date_limit)

    for (i in 1:length(bin_var_col)) {

      message("Entered Loop")

      actual_wins_losses <-
        actual_wins_losses_raw %>%
        filter(trade_col == trade_direction) %>%
        filter(
          stop_factor == stop_value_var,
          profit_factor == profit_value_var,
          periods_ahead == period_var,
          Asset == Asset_of_interest
        ) %>%
        mutate(
          bin_var =
            case_when(
              !!as.name(bin_var_col[i]) > 0 & trade_col == "Short" ~ "win",
              !!as.name(bin_var_col[i]) <= 0 & trade_col == "Short" ~ "loss",

              !!as.name(bin_var_col[i]) > 0 & trade_col == "Long" ~ "win",
              !!as.name(bin_var_col[i]) <= 0 & trade_col == "Long" ~ "loss"

            )
        ) %>%
        filter(Date <= date_limit)

      check_date <-
        copula_data %>% pull(Date) %>% max() %>% as_date()

      check_date <- check_date <= date_limit

      message(glue::glue("Data Date is less than Train Date Max: {check_date}"))

      copula_for_join_model <-
        actual_wins_losses %>%
        dplyr::select(Date, Asset ,bin_var, matches(bin_var_col) ) %>%
        filter(
          Asset == Asset_of_interest
        ) %>%
        left_join(copula_data) %>%
        fill(!contains("Date"), .direction = "down") %>%
        filter(if_all(everything(),~!is.na(.))) %>%
        filter(Date <= date_limit)

      # rm(actual_wins_losses)

      copula_vars_for_indicator <-
        names(copula_for_join_model) %>%
        keep(~ !str_detect(.x, "Date") &
               !str_detect(.x, "bin_var") &
               !str_detect(.x, "Asset") &
               !str_detect(.x, paste(bin_var_col, collapse = "|") ) &
               !str_detect(.x, "period_return_")
        ) %>%
        unlist() %>%
        as.character()

      copula_indicator_formula_logit <-
        create_lm_formula(dependant = "bin_var=='win'",
                          independant = copula_vars_for_indicator)

      copula_indicator_model <-
        glm(formula = copula_indicator_formula_logit,
            data = copula_for_join_model,
            family = binomial("logit"))

      sig_coefs <-
        get_sig_coefs(model_object_of_interest = copula_indicator_model,
                      p_value_thresh_for_inputs = 0.25)

      rm(copula_indicator_model)
      gc()

      copula_indicator_formula_logit <-
        create_lm_formula(dependant = "bin_var=='win'",
                          independant = sig_coefs)

      copula_indicator_model <-
        glm(formula = copula_indicator_formula_logit,
            data = copula_for_join_model,
            family = binomial("logit"))

      summary(copula_indicator_model)

      message(glue::glue("Passed copula Model {Asset_of_interest} {i}"))

      saveRDS(object = copula_indicator_model,
              file =
                glue::glue("{save_path}/{Asset_of_interest}_{trade_direction}_{bin_var_col[i]}_copula_logit.RDS")
      )

      rm(copula_indicator_model)
      gc()

      copula_indicator_formula_lin <-
        create_lm_formula(dependant = bin_var_col[i],
                          independant = copula_vars_for_indicator)

      copula_indicator_model_lin <-
        lm(formula = copula_indicator_formula_lin,
           data = copula_for_join_model)

      sig_coefs <-
        get_sig_coefs(model_object_of_interest = copula_indicator_model_lin,
                      p_value_thresh_for_inputs = 0.25)

      copula_indicator_formula_lin <-
        create_lm_formula(dependant = bin_var_col[i],
                          independant = sig_coefs)

      copula_indicator_model_lin <-
        lm(formula = copula_indicator_formula_lin,
           data = copula_for_join_model)

      summary(copula_indicator_model_lin)

      message(glue::glue("Passed copula Model Linear {Asset_of_interest} {i}"))

      saveRDS(object = copula_indicator_model_lin,
              file =
                glue::glue("{save_path}/{Asset_of_interest}_{trade_direction}_{bin_var_col[i]}_copula_lin.RDS")
      )

      rm(copula_indicator_model_lin)
      gc()

    }

  }

#' prepare_combined_model
#'
#' @param actual_wins_losses
#' @param Asset_of_interest
#' @param date_limit
#' @param stop_value_var
#' @param profit_value_var
#' @param period_var
#' @param bin_var_col
#' @param trade_direction
#' @param save_path
#' @param copula_data
#'
#' @returns
#' @export
#'
#' @examples
prepare_combined_model <-
  function(
    combined_model_data = combined_model_data,
    actual_wins_losses = actual_wins_losses,
    Asset_of_interest = "EUR_USD",
    date_limit = date_train_phase_2_end,
    stop_value_var = stop_value_var,
    profit_value_var = profit_value_var,
    period_var = period_var,
    bin_var_col = c("period_return_20_Price", "period_return_35_Price"),
    trade_direction = "Long",
    save_path = "C:/Users/nikhi/Documents/trade_data/single_asset_models_v2_adv"
  ) {

    actual_wins_losses_raw <-
      actual_wins_losses

    combined_model_data <-
      combined_model_data %>%
      filter(Date <= date_limit)

    for (i in 1:length(bin_var_col)) {

      message("Entered Loop")

      actual_wins_losses <-
        actual_wins_losses_raw %>%
        filter(trade_col == trade_direction) %>%
        filter(
          stop_factor == stop_value_var,
          profit_factor == profit_value_var,
          periods_ahead == period_var,
          Asset == Asset_of_interest
        ) %>%
        mutate(
          bin_var =
            case_when(
              !!as.name(bin_var_col[i]) > 0 & trade_col == "Short" ~ "win",
              !!as.name(bin_var_col[i]) <= 0 & trade_col == "Short" ~ "loss",

              !!as.name(bin_var_col[i]) > 0 & trade_col == "Long" ~ "win",
              !!as.name(bin_var_col[i]) <= 0 & trade_col == "Long" ~ "loss"

            )
        ) %>%
        filter(Date <= date_limit)

      check_date <-
        combined_model_data %>% pull(Date) %>% max() %>% as_date()

      check_date <- check_date <= date_limit

      message(glue::glue("Data Date is less than Train Date Max: {check_date}"))

      combined_for_join_model <-
        actual_wins_losses %>%
        dplyr::select(Date, Asset ,bin_var, matches(bin_var_col) ) %>%
        filter(
          Asset == Asset_of_interest
        ) %>%
        left_join(combined_model_data) %>%
        fill(!contains("Date"), .direction = "down") %>%
        filter(if_all(everything(),~!is.na(.))) %>%
        filter(if_all(everything(),~!is.nan(.))) %>%
        filter(if_all(everything(),~!is.infinite(.))) %>%
        filter(Date <= date_limit)

      # rm(actual_wins_losses)

      combined_vars_for_indicator <-
        names(combined_for_join_model) %>%
        keep(~ !str_detect(.x, "Date") &
               !str_detect(.x, "bin_var") &
               !str_detect(.x, "Asset") &
               !str_detect(.x, paste(bin_var_col, collapse = "|") ) &
               !str_detect(.x, "period_return_")
        ) %>%
        unlist() %>%
        as.character()

      combined_indicator_formula_logit <-
        create_lm_formula(dependant = "bin_var=='win'",
                          independant = combined_vars_for_indicator)

      combined_indicator_model <-
        glm(formula = combined_indicator_formula_logit,
            data = combined_for_join_model,
            family = binomial("logit"))

      sig_coefs <-
        get_sig_coefs(model_object_of_interest = combined_indicator_model,
                      p_value_thresh_for_inputs = 0.00001)

      rm(combined_indicator_model)
      gc()

      combined_indicator_formula_logit <-
        create_lm_formula(dependant = "bin_var=='win'",
                          independant = sig_coefs)

      combined_indicator_model <-
        glm(formula = combined_indicator_formula_logit,
            data = combined_for_join_model,
            family = binomial("logit"))

      summary(combined_indicator_model)

      message(glue::glue("Passed combined Model {Asset_of_interest} {i}"))

      saveRDS(object = combined_indicator_model,
              file =
                glue::glue("{save_path}/{Asset_of_interest}_{trade_direction}_{bin_var_col[i]}_combined_logit.RDS")
      )

      rm(combined_indicator_model)
      gc()

      combined_indicator_formula_lin <-
        create_lm_formula(dependant = bin_var_col[i],
                          independant = combined_vars_for_indicator)

      combined_indicator_model_lin <-
        lm(formula = combined_indicator_formula_lin,
           data = combined_for_join_model)

      sig_coefs <-
        get_sig_coefs(model_object_of_interest = combined_indicator_model_lin,
                      p_value_thresh_for_inputs = 0.00001)

      combined_indicator_formula_lin <-
        create_lm_formula(dependant = bin_var_col[i],
                          independant = sig_coefs)

      combined_indicator_model_lin <-
        lm(formula = combined_indicator_formula_lin,
           data = combined_for_join_model)

      summary(combined_indicator_model_lin)

      message(glue::glue("Passed combined Model Linear {Asset_of_interest} {i}"))

      saveRDS(object = combined_indicator_model_lin,
              file =
                glue::glue("{save_path}/{Asset_of_interest}_{trade_direction}_{bin_var_col[i]}_combined_lin.RDS")
      )

      rm(combined_indicator_model_lin)
      gc()

    }

  }

#' prepare_technical_model
#'
#' @param technical_data
#' @param actual_wins_losses
#' @param Asset_of_interest
#' @param date_limit
#' @param stop_value_var
#' @param profit_value_var
#' @param period_var
#' @param bin_var_col
#' @param trade_direction
#' @param save_path
#'
#' @returns
#' @export
#'
#' @examples
prepare_technical_model <-
  function(
    technical_data = technical_data,
    actual_wins_losses = actual_wins_losses,
    Asset_of_interest = "EUR_USD",
    date_limit = date_train_phase_2_end,
    stop_value_var = stop_value_var,
    profit_value_var = profit_value_var,
    period_var = period_var,
    bin_var_col = c("period_return_20_Price", "period_return_35_Price"),
    trade_direction = "Long",
    save_path = "C:/Users/nikhi/Documents/trade_data/single_asset_models_v2_adv"
  ) {

    actual_wins_losses_raw <-
      actual_wins_losses

    technical_data <-
      technical_data %>%
      filter(Date <= date_limit) %>%
      filter(if_all(everything() ,~ !is.nan(.) ))%>%
      filter(if_all(everything() ,~ !is.infinite(.) )) %>%
      filter(if_all(everything() ,~ !is.na(.) ))

    for (i in 1:length(bin_var_col)) {

      message("Entered Loop")

      actual_wins_losses <-
        actual_wins_losses_raw %>%
        filter(trade_col == trade_direction) %>%
        filter(
          stop_factor == stop_value_var,
          profit_factor == profit_value_var,
          periods_ahead == period_var,
          Asset == Asset_of_interest
        ) %>%
        mutate(
          bin_var =
            case_when(
              !!as.name(bin_var_col[i]) > 0 & trade_col == "Short" ~ "win",
              !!as.name(bin_var_col[i]) <= 0 & trade_col == "Short" ~ "loss",

              !!as.name(bin_var_col[i]) > 0 & trade_col == "Long" ~ "win",
              !!as.name(bin_var_col[i]) <= 0 & trade_col == "Long" ~ "loss"

            )
        ) %>%
        filter(Date <= date_limit)

      check_date <-
        technical_data %>% pull(Date) %>% max() %>% as_date()

      check_date <- check_date <= date_limit

      message(glue::glue("Data Date is less than Train Date Max: {check_date}"))

      technical_for_join_model <-
        actual_wins_losses %>%
        dplyr::select(Date, Asset ,bin_var, matches(bin_var_col) ) %>%
        filter(
          Asset == Asset_of_interest
        ) %>%
        left_join(technical_data) %>%
        fill(!contains("Date"), .direction = "down") %>%
        filter( if_all( everything(),~!is.na(.) ) ) %>%
        filter( if_all( everything(),~!is.nan(.) ) ) %>%
        filter( if_all( everything(),~!is.infinite(.) ) ) %>%
        filter(Date <= date_limit)

      # rm(actual_wins_losses)

      technical_vars_for_indicator <-
        names(technical_for_join_model) %>%
        keep(~ !str_detect(.x, "Date") &
               !str_detect(.x, "bin_var") &
               !str_detect(.x, "Asset") &
               !str_detect(.x, paste(bin_var_col, collapse = "|") ) &
               !str_detect(.x, "period_return_")
        ) %>%
        unlist() %>%
        as.character()

      technical_indicator_formula_logit <-
        create_lm_formula(dependant = "bin_var=='win'",
                          independant = technical_vars_for_indicator)

      technical_indicator_model <-
        glm(formula = technical_indicator_formula_logit,
            data = technical_for_join_model,
            family = binomial("logit"))

      sig_coefs <-
        get_sig_coefs(model_object_of_interest = technical_indicator_model,
                      p_value_thresh_for_inputs = 0.15)

      rm(technical_indicator_model)
      gc()

      technical_indicator_formula_logit <-
        create_lm_formula(dependant = "bin_var=='win'",
                          independant = sig_coefs)

      technical_indicator_model <-
        glm(formula = technical_indicator_formula_logit,
            data = technical_for_join_model,
            family = binomial("logit"))

      summary(technical_indicator_model)

      message(glue::glue("Passed Technical Model {Asset_of_interest} {i}"))

      saveRDS(object = technical_indicator_model,
              file =
                glue::glue("{save_path}/{Asset_of_interest}_{trade_direction}_{bin_var_col[i]}_technical_logit.RDS")
      )

      rm(technical_indicator_model)
      gc()

      technical_indicator_formula_lin <-
        create_lm_formula(dependant = bin_var_col[i],
                          independant = technical_vars_for_indicator)

      technical_indicator_model_lin <-
        lm(formula = technical_indicator_formula_lin,
           data = technical_for_join_model)

      sig_coefs <-
        get_sig_coefs(model_object_of_interest = technical_indicator_model_lin,
                      p_value_thresh_for_inputs = 0.15)

      technical_indicator_formula_lin <-
        create_lm_formula(dependant = bin_var_col[i],
                          independant = sig_coefs)

      technical_indicator_model_lin <-
        lm(formula = technical_indicator_formula_lin,
           data = technical_for_join_model)

      summary(technical_indicator_model_lin)

      message(glue::glue("Passed technical Model Linear {Asset_of_interest} {i}"))

      saveRDS(object = technical_indicator_model_lin,
              file =
                glue::glue("{save_path}/{Asset_of_interest}_{trade_direction}_{bin_var_col[i]}_technical_lin.RDS")
      )

      rm(technical_indicator_model_lin)
      gc()

    }

  }

#' get_GDP_countries
#'
#' @param raw_macro_data
#' @param lag_days
#'
#' @returns
#' @export
#'
#' @examples
get_GDP_countries <-
  function(raw_macro_data = raw_macro_data,
           lag_days = 3 ) {

    GDP_data <-
      raw_macro_data %>%
      mutate(
        Index_Type =
          case_when(

            str_detect(event, "Public Deficit") &
              str_detect(event, "GDP") &
              symbol == "EUR" ~ "EUR GDP",

            str_detect(event, "Current Account") &
              symbol == "USD" ~ "USD GDP",

            str_detect(event, "Current Account") &
              str_detect(event, "QoQ") &
              symbol == "NZD" ~ "NZD GDP",

            str_detect(event, "Current Account Balance") &
              str_detect(event, "Q") &
              symbol == "AUD" ~ "AUD GDP",

            str_detect(event, "Current Account") &
              str_detect(event, "Q") &
              symbol == "GBP" ~ "GBP GDP",

            str_detect(event, "Current Account") &
              str_detect(event, "Q") &
              symbol == "CAD" ~ "CAD GDP",

            str_detect(event, "Gross Domestic Product") &
              str_detect(event, "(QoQ)") &
              symbol == "CHF" ~ "CHF GDP",

            str_detect(event, "Gross Domestic Product") &
              str_detect(event, "(QoQ)") &
              symbol == "JPY" ~ "JPY GDP",

            str_detect(event, "Gross Domestic Product") &
              str_detect(event, "(QoQ)") &
              symbol == "CNY" ~ "CNY GDP"

          )
      ) %>%
      filter(!is.na(Index_Type)) %>%
      dplyr::select(Index_Type, actual,date ) %>%
      dplyr::group_by(Index_Type,date ) %>%
      summarise(
        actual = median(actual, na.rm = T)
      ) %>%
      ungroup() %>%
      mutate(date = date + lubridate::days(lag_days) ) %>%
      mutate(
        date =
          case_when(
            lubridate::wday(date) == 7 ~ date + lubridate::days(2),
            lubridate::wday(date) == 1 ~ date + lubridate::days(1),
            TRUE ~ date
          )
      ) %>%
      group_by(Index_Type) %>%
      arrange(date, .by_group = TRUE) %>%
      group_by(Index_Type) %>%
      mutate(
        actual =
          case_when(
            !(Index_Type %in% c("CHF GDP", "CNY GDP", "EUR GDP", "JPY GDP")) ~
              (actual - lag(actual))/lag(actual),
            TRUE ~ actual
          )
      ) %>%
      ungroup() %>%
      pivot_wider(names_from = Index_Type, values_from = actual, values_fn = median) %>%
      arrange(date) %>%
      fill(everything(), .direction = "down") %>%
      filter(if_all(everything(), ~ !is.na(.) ))

    return(GDP_data)

  }


#' get_unemp_countries
#'
#' @param raw_macro_data
#' @param lag_days
#'
#' @returns
#' @export
#'
#' @examples
get_unemp_countries <-
  function(raw_macro_data = raw_macro_data,
           lag_days = 3 ) {

    UR_data <-
      raw_macro_data %>%
      mutate(
        Index_Type =
          case_when(

            str_detect(event, "Unemployment Rate") &
              symbol == "EUR" ~ "EUR UR",

            str_detect(event, "Unemployment Rate") &
              symbol == "USD" ~ "USD UR",

            str_detect(event, "Unemployment Rate") &
              symbol == "NZD" ~ "NZD UR",

            str_detect(event, "Unemployment Rate") &
              symbol == "AUD" ~ "AUD UR",

            str_detect(event, "Unemployment Rate") &
              symbol == "GBP" ~ "GBP UR",

            str_detect(event, "Unemployment Rate") &
              symbol == "CAD" ~ "CAD UR",

            str_detect(event, "Unemployment Rate") &
              symbol == "CHF" ~ "CHF UR",

            str_detect(event, "Unemployment Rate") &
              symbol == "JPY" ~ "JPY UR"

          )
      ) %>%
      filter(!is.na(Index_Type)) %>%
      dplyr::select(Index_Type, actual,date ) %>%
      dplyr::group_by(Index_Type,date ) %>%
      summarise(
        actual = median(actual, na.rm = T)
      ) %>%
      ungroup() %>%
      mutate(date = date + lubridate::days(lag_days) ) %>%
      mutate(
        date =
          case_when(
            lubridate::wday(date) == 7 ~ date + lubridate::days(2),
            lubridate::wday(date) == 1 ~ date + lubridate::days(1),
            TRUE ~ date
          )
      ) %>%
      group_by(Index_Type) %>%
      arrange(date, .by_group = TRUE) %>%
      ungroup() %>%
      pivot_wider(names_from = Index_Type, values_from = actual, values_fn = median) %>%
      arrange(date) %>%
      fill(everything(), .direction = "down") %>%
      filter(if_all(everything(), ~ !is.na(.) ))

    return(UR_data)

  }

#' get_manufac_countries
#'
#' @param raw_macro_data
#' @param lag_days
#'
#' @returns
#' @export
#'
#' @examples
get_manufac_countries <-
  function(raw_macro_data = raw_macro_data,
           lag_days = 3 ) {

    Manufac_data <-
      raw_macro_data %>%
      mutate(
        Index_Type =
          case_when(

            str_detect(event, "Manufacturing PMI") &
              symbol == "EUR" ~ "EUR Manufac PMI",

            str_detect(event, "Manufacturing PMI") &
              symbol == "USD" ~ "USD Manufac PMI",

            str_detect(event, "Manufacturing Sales") &
              symbol == "NZD" ~ "NZD Manufac PMI",

            (str_detect(event, "Manufacturing PMI") &
              symbol == "AUD")|
            (str_detect(event, "AiG Performance of Mfg") &
              symbol == "AUD") ~ "AUD Manufac PMI",

            str_detect(event, "Manufacturing PMI") &
              symbol == "GBP" ~ "GBP Manufac PMI",

            str_detect(event, "Manufacturing PMI") &
              symbol == "CAD" ~ "CAD Manufac PMI",

            # str_detect(event, "Unemployment Rate") &
            #   symbol == "CHF" ~ "CHF UR",

            str_detect(event, "Manufacturing PMI") &
              symbol == "JPY" ~ "JPY Manufac PMI",

            str_detect(event, "Manufacturing PMI") &
              symbol == "CNY" ~ "CNY Manufac PMI"

          )
      ) %>%
      filter(!is.na(Index_Type)) %>%
      dplyr::select(Index_Type, actual,date ) %>%
      dplyr::group_by(Index_Type,date ) %>%
      summarise(
        actual = median(actual, na.rm = T)
      ) %>%
      ungroup() %>%
      mutate(date = date + lubridate::days(lag_days) ) %>%
      mutate(
        date =
          case_when(
            lubridate::wday(date) == 7 ~ date + lubridate::days(2),
            lubridate::wday(date) == 1 ~ date + lubridate::days(1),
            TRUE ~ date
          )
      ) %>%
      group_by(Index_Type) %>%
      arrange(date, .by_group = TRUE) %>%
      ungroup() %>%
      pivot_wider(names_from = Index_Type, values_from = actual, values_fn = median) %>%
      arrange(date) %>%
      fill(everything(), .direction = "down") %>%
      filter(if_all(everything(), ~ !is.na(.) ))

    return(Manufac_data)

  }


#' get_additional_USD_Macro
#'
#' @param raw_macro_data
#' @param lag_days
#'
#' @returns
#' @export
#'
#' @examples
get_additional_USD_Macro <-
  function(raw_macro_data = raw_macro_data,
           lag_days = 3 ) {

    USD_Macro <-
      raw_macro_data %>%
      mutate(
        Index_Type =
          case_when(

            str_detect(event, "Consumer Credit Change") &
              symbol == "USD" ~ "USD_Consumer_Credit",

            str_detect(event, "Goods and Services Trade Balance") &
              symbol == "USD" ~ "USD_Trade_Balance",

            str_detect(event, "Export Price Index") &
              str_detect(event, "MoM") &
              symbol == "USD" ~ "USD_Export_Price",

            str_detect(event, "Monthly Budget Statement") &
              symbol == "USD" ~ "USD_Budget_Statement",

            str_detect(event, "Continuing Jobless Claims") &
              symbol == "USD" ~ "USD_Jobless",

            str_detect(event, "ADP Employment Change") &
              str_detect(event, "\\(") &
              symbol == "USD" ~ "USD_Employment_Change",

            str_detect(event, "Net Long\\-Term TIC Flows") &
              symbol == "USD" ~ "USD_TIC_Flows",

            str_detect(event, "Nonfarm Payrolls") &
              symbol == "USD" ~ "USD_Payrolls",

          )
      ) %>%
      filter(!is.na(Index_Type)) %>%
      dplyr::select(Index_Type, actual,date ) %>%
      dplyr::group_by(Index_Type,date ) %>%
      summarise(
        actual = median(actual, na.rm = T)
      ) %>%
      ungroup() %>%
      mutate(date = date + lubridate::days(lag_days) ) %>%
      mutate(
        date =
          case_when(
            lubridate::wday(date) == 7 ~ date + lubridate::days(2),
            lubridate::wday(date) == 1 ~ date + lubridate::days(1),
            TRUE ~ date
          )
      ) %>%
      group_by(Index_Type) %>%
      arrange(date, .by_group = TRUE) %>%
      ungroup() %>%
      group_by(Index_Type) %>%
      mutate(
        actual = log(actual/lag(actual))
      ) %>%
      pivot_wider(names_from = Index_Type, values_from = actual, values_fn = median) %>%
      arrange(date) %>%
      fill(everything(), .direction = "down") %>%
      mutate(
        across(
          .cols = !contains("date"),
          .fns = ~ ifelse( is.infinite(.), mean(., na.rm = T), .)
        )
      ) %>%
      filter(if_all(everything(), ~ !is.na(.) )) %>%
      # mutate(
      #   # USD_Consumer_Credit = scale(USD_Consumer_Credit) %>% as.vector() %>% as.numeric(),
      #   USD_Consumer_Credit = log(USD_Consumer_Credit/lag(USD_Consumer_Credit)),
      #   USD_Trade_Balance = log(USD_Trade_Balance/lag(USD_Trade_Balance)),
      #   USD_Budget_Statement = log(USD_Budget_Statement/lag(USD_Budget_Statement))
      # ) %>%
      filter(if_all(everything(), ~ !is.na(.) ))

    return(USD_Macro)

  }

#' get_additional_USD_Macro
#'
#' @param raw_macro_data
#' @param lag_days
#'
#' @returns
#' @export
#'
#' @examples
get_additional_EUR_Macro <-
  function(raw_macro_data = raw_macro_data,
           lag_days = 3 ) {

    EUR_Macro <-
      raw_macro_data %>%
      mutate(
        Index_Type =
          case_when(

            str_detect(event, "Trade Balance EUR") &
              symbol == "EUR" ~ "EUR_Trade_Balance",

            str_detect(event, "Current Account n\\.s\\.a.") &
              symbol == "EUR" ~ "EUR_CA_nsa",

            str_detect(event, "Budget") &
              symbol == "EUR" ~ "EUR_Budget",

            str_detect(event, "Imports") &
              str_detect(event, "EUR") &
              !str_detect(event, "MoM") &
              symbol == "EUR" ~ "EUR_Imports",

            str_detect(event, "Exports\\, EUR") &
              !str_detect(event, "MoM") &
              symbol == "EUR" ~ "EUR_Exports"

          )
      ) %>%
      filter(!is.na(Index_Type)) %>%
      dplyr::select(Index_Type, actual,date ) %>%
      dplyr::group_by(Index_Type,date ) %>%
      summarise(
        actual = median(actual, na.rm = T)
      ) %>%
      ungroup() %>%
      mutate(date = date + lubridate::days(lag_days) ) %>%
      mutate(
        date =
          case_when(
            lubridate::wday(date) == 7 ~ date + lubridate::days(2),
            lubridate::wday(date) == 1 ~ date + lubridate::days(1),
            TRUE ~ date
          )
      ) %>%
      group_by(Index_Type) %>%
      arrange(date, .by_group = TRUE) %>%
      ungroup() %>%
      group_by(Index_Type) %>%
      mutate(
        actual = log(actual/lag(actual))
      ) %>%
      pivot_wider(names_from = Index_Type, values_from = actual, values_fn = median) %>%
      arrange(date) %>%
      fill(everything(), .direction = "down") %>%
      mutate(
        across(
          .cols = !contains("date"),
          .fns = ~ ifelse( is.infinite(.), mean(., na.rm = T), .)
        )
      ) %>%
      filter(if_all(everything(), ~ !is.na(.) )) %>%
      # mutate(
      #   # USD_Consumer_Credit = scale(USD_Consumer_Credit) %>% as.vector() %>% as.numeric(),
      #   USD_Consumer_Credit = log(USD_Consumer_Credit/lag(USD_Consumer_Credit)),
      #   USD_Trade_Balance = log(USD_Trade_Balance/lag(USD_Trade_Balance)),
      #   USD_Budget_Statement = log(USD_Budget_Statement/lag(USD_Budget_Statement))
      # ) %>%
      filter(if_all(everything(), ~ !is.na(.) ))

    return(EUR_Macro)

  }
