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
single_asset_Logit_indicator_2 <-
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

    actual_wins_losses_raw <-
      actual_wins_losses

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

    daily_training_data <-
      daily_join_model %>%
      filter(Date <= as_date(pre_train_date_end)) %>%
      filter(Date <= as_date(test_date_start)) %>%
      filter( if_all(everything(), ~ !is.infinite(.)) )

    daily_indicator_model <-
      glm(formula = daily_indicator_formula,
          data = daily_training_data,
          family = binomial("logit"))
    summary(daily_indicator_model)
    message("Passed Daily Model")

    train_preds <-
      predict.glm(daily_indicator_model,
                  newdata = daily_training_data, type = "response")
    test_preds <-
      predict.glm(daily_indicator_model,
                  newdata = daily_join_model, type = "response")
    mean_train_preds <-
      mean(train_preds, na.rm = T)
    sd_train_preds <-
      sd(train_preds, na.rm = T)

    daily_indicator_pred <-
      daily_join_model %>%
      mutate(
        daily_indicator_pred = test_preds,
        mean_daily_pred = mean_train_preds,
        sd_daily_pred = sd_train_preds
      )

    rm(daily_training_data, daily_testing_data, test_preds, train_preds)
    gc()
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

    copula_train_data <-
      copula_for_join_model %>%
      filter(Date <= as_date(pre_train_date_end)) %>%
      filter(Date <= as_date(test_end_date))

    copula_indicator_model <-
      glm(formula = copula_indicator_formula,
          data = copula_train_data,
          family = binomial("logit"))
    summary(copula_indicator_model)
    message("Passed Copula Model")

    train_preds <-
      predict.glm(copula_indicator_model,
                  newdata = copula_train_data, type = "response")

    copula_indicator_pred <-
      copula_for_join_model %>%
      mutate(
        copula_indicator_pred = predict.glm(copula_indicator_model,
                                            newdata = copula_for_join_model, type = "response"),
        mean_copula_pred = mean(train_preds, na.rm = T),
        sd_copula_pred = sd(train_preds, na.rm = T)
      )
    rm(train_preds, copula_train_data)
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

    train_data <-
      macro_for_join_model %>%
      filter(Date <= as_date(pre_train_date_end)) %>%
      filter(Date <= as_date(test_end_date))

    macro_indicator_model <-
      glm(formula = macro_indicator_formula,
          data = train_data,
          family = binomial("logit"))
    summary(macro_indicator_model)
    message("Passed Macro Model")

    train_preds <-
      predict.glm(macro_indicator_model,
                  newdata = train_data, type = "response")


    macro_indicator_pred <-
      macro_for_join_model %>%
      mutate(
        macro_indicator_pred = predict.glm(macro_indicator_model,
                                           newdata = macro_for_join_model, type = "response"),
        mean_macro_pred = mean(train_preds, na.rm = T),
        sd_macro_pred = sd(train_preds, na.rm = T)
      )
    rm(train_preds)
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

    train_data <-
      indexes_for_join_model %>%
      filter(Date <= as_date(pre_train_date_end)) %>%
      filter(Date <= as_date(test_end_date))

    indexes_indicator_model <-
      glm(formula = indexes_indicator_formula,
          data = train_data,
          family = binomial("logit"))
    summary(indexes_indicator_model)

    train_preds <-
      predict.glm(indexes_indicator_model,
                  newdata = train_data, type = "response")


    message("Passed Indicator Model")

    indexes_indicator_pred <-
      indexes_for_join_model %>%
      mutate(
        indexes_indicator_pred = predict.glm(indexes_indicator_model,
                                             newdata = indexes_for_join_model, type = "response"),
        mean_indexes_pred = mean(train_preds, na.rm= T),
        sd_indexes_pred = sd(train_preds, na.rm = T)
      )
    rm(train_preds, train_data)

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

    train_data <-
      technical_for_join_model %>%
      filter(Date <= as_date(pre_train_date_end)) %>%
      filter(Date <= as_date(test_end_date)) %>%
      filter( if_all(everything(), ~ !is.infinite(.)) )

    technical_indicator_model <-
      glm(formula = technical_indicator_formula,
          data = train_data,
          family = binomial("logit"))

    message("Passed Technical Model")

    train_preds <-
      predict.glm(technical_indicator_model,
                  newdata = train_data, type = "response")

    summary(technical_indicator_model)

    technical_indicator_pred <-
      technical_for_join_model %>%
      mutate(
        technical_indicator_pred = predict.glm(technical_indicator_model,
                                               newdata = technical_for_join_model, type = "response"),
        mean_indicator_pred = mean(train_preds, na.rm = T),
        sd_indicator_pred = sd(train_preds, na.rm = T)
      )

    rm(train_preds, train_data)

    message("Passed Technical Pred")


    combined_indicator_NN <-
      # macro_indicator_pred %>%
      # dplyr::select(Date, Asset, bin_var,
      #               contains("pred") ,
      #               contains("PC1"), contains("Sentiment")  ) %>%
      # distinct() %>%
      # left_join(
      #   indexes_indicator_pred %>%
      #     dplyr::select(Date, Asset,
      #                   contains("pred"),
      #                   contains("PC1")  ) %>%
      #     distinct()
      # ) %>%
      # left_join(copula_indicator_pred %>%
      #             dplyr::select(Date, Asset,
      #                           contains("pred"),
      #                           contains("cor"),
      #                           contains("lm")  ) %>%
      #             distinct()) %>%
      # left_join(
      #   technical_indicator_pred %>% dplyr::select(-bin_var)
      # ) %>%
      # left_join(daily_indicator_pred %>% dplyr::select(-bin_var) %>% distinct()) %>%
      # janitor::clean_names() %>%
      # filter(if_all(everything() ,~!is.na(.))) %>%
      combined_indicator_NN <-
      macro_indicator_pred %>%
      dplyr::select(Date, Asset, bin_var,
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
          dplyr::select(-bin_var) %>%
          mutate(Asset = Asset_of_interest) %>%
          mutate(Date = as_datetime(Date)) %>%
          distinct()
      ) %>%
      left_join(daily_indicator_pred %>%
                  dplyr::select(-bin_var) %>%
                  distinct()  %>%
                  mutate(Asset = Asset_of_interest) %>%
                  mutate(Date = as_datetime(Date)) %>%
                  distinct() ) %>%
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

    rm( train_data )
    train_data <-
      combined_indicator_NN %>%
      filter(date <= test_date_start)

    logit_combined_indicator_model <-
      glm(formula = NN_form,
          data = train_data ,
          family = binomial("logit"))

    message("Created Combined Model")

    train_preds <-
      predict.glm(logit_combined_indicator_model,
                  newdata = train_data, type = "response")

    summary(logit_combined_indicator_model)

    logit_combined_pred <-
      combined_indicator_NN %>%
      filter(date <= test_date_start) %>%
      rename(Date = date, Asset = asset) %>%
      mutate(
        logit_combined_pred = predict.glm(logit_combined_indicator_model,
                                          newdata = train_data, type = "response"),
        mean_logit_combined_pred = mean(train_preds, na.rm = T),
        sd_logit_combined_pred = sd(train_preds, na.rm = T),

        averaged_pred =
          indexes_indicator_pred*macro_indicator_pred*indexes_indicator_pred*logit_combined_pred

      )

    train_averaged_pred =
      logit_combined_pred %>%
      filter(
        Date <= test_date_start
      ) %>%
      pull(averaged_pred)

    test_data <-
      combined_indicator_NN %>%
      filter(date > test_date_start)

    logit_combined_pred <-
      combined_indicator_NN %>%
      filter(date > test_date_start) %>%
      rename(Date = date, Asset = asset) %>%
      mutate(
        logit_combined_pred = predict.glm(logit_combined_indicator_model,
                                          newdata = test_data, type = "response"),
        mean_logit_combined_pred = mean(train_preds, na.rm = T),
        sd_logit_combined_pred = sd(train_preds, na.rm = T),

        averaged_pred =
          indexes_indicator_pred*macro_indicator_pred*indexes_indicator_pred*logit_combined_pred

      ) %>%
      mutate(
        mean_averaged_pred = mean(train_averaged_pred, na.rm = T),
        sd_averaged_pred = sd(train_averaged_pred, na.rm = T),
      ) %>%
      dplyr::select(Date, Asset, contains("pred")) %>%
      filter(Date >= test_date_start) %>%
      left_join(
        actual_wins_losses_raw %>%
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
single_asset_Logit_run_and_save_models_2 <-
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

    saveRDS(object = daily_indicator_model,
            file =
              glue::glue("{save_path}/{Asset_of_interest}_{trade_direction}_daily_indicator_model.RDS")
            )

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

        actual_wins_losses_raw %>%
          dplyr::select(Date, Asset, stop_factor, profit_factor, periods_ahead, trade_col,
                        trade_return_dollar_aud, contains("period_return_"))
      )

    return(logit_combined_pred)

  }
