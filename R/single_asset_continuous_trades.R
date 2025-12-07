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
           Asset_of_interest = "EUR_USD") {

    daily_technical_indicators <- Daily_Data %>%
      filter(Asset == Asset_of_interest) %>%
      create_technical_indicators()

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

    return(joined_dat)

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
      mutate(Date = as_datetime(Date)) %>%
      dplyr::select(Date, Asset,
                    contains("pred") ,
                    contains("PC1"), contains("Sentiment")  ) %>%
      distinct() %>%
      left_join(
        indexes_indicator_pred %>%
          mutate(Asset = Asset_of_interest) %>%
          mutate(Date = as_datetime(Date)) %>%
          dplyr::select(Date, Asset,
                        contains("pred"),
                        contains("PC1")  ) %>%
          distinct()
      ) %>%
      left_join(copula_indicator_pred %>%
                  mutate(Asset = Asset_of_interest) %>%
                  mutate(Date = as_datetime(Date)) %>%
                  dplyr::select(Date, Asset,
                                contains("pred"),
                                contains("cor"),
                                contains("lm")  ) %>%
                  distinct()) %>%
      fill(contains("pred")|contains("cor")|contains("lm")|contains("copula_pred"), .direction = "down") %>%
      left_join(
        technical_indicator_pred %>%
          mutate(Asset = Asset_of_interest) %>%
          mutate(Date = as_datetime(Date)) %>%
          distinct()
      ) %>%
      left_join(daily_indicator_pred %>%
                  mutate(Asset = Asset_of_interest) %>%
                  mutate(Date = as_datetime(Date)) %>%
                  distinct()
                ) %>%
      fill(contains("daily_indicator_pred")|contains("Daily_")|contains("daily_"), .direction = "down") %>%
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
          save_path = save_path) %>%
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
          )

      )

    return(asset_data_with_indicator)

  }
