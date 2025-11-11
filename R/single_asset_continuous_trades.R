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
          data = combined_indicator_NN %>%
            filter(
                   # date >= (pre_train_date_end - months(12)),
                   date <= test_date_start) ,
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
        actual_wins_losses %>%
          dplyr::select(Date, Asset, stop_factor, profit_factor, periods_ahead, trade_col,
                        Time_Periods,trade_start_prices, trade_end_prices, trade_return_dollar_aud)
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
    weekly_data = weekly_data,
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

    weekly_indicator <-
      get_weekly_indicators(
        weekly_data = weekly_data,
        asset_data = asset_data,
        Asset_of_interest = Asset_of_interest
      )

    weekly_indicator <-
      weekly_indicator %>%
      dplyr::select(-Price, -Open, -Low, -High, -Vol., -Date_for_join) %>%
      mutate(
        across(.cols = !contains("Date") & !contains("Asset"),
               .fns = ~ lag(.))
      ) %>%
      filter(if_all(everything(), ~ !is.na(.))) %>%
      mutate(
        Date = as_datetime(Date, tz = "Australia/Canberra")
      )

    weekly_join_model <-
      actual_wins_losses %>%
      dplyr::select(Date, Asset ,bin_var) %>%
      filter(
        Asset == Asset_of_interest
      ) %>%
      left_join(weekly_indicator) %>%
      filter(if_all(everything(),~!is.na(.)))

    weekly_vars_for_indicator <-
      names(weekly_join_model) %>%
      keep(~ !str_detect(.x, "Date") &
             !str_detect(.x, "bin_var") &
             !str_detect(.x, "Asset")) %>%
      unlist() %>%
      as.character()

    weekly_indicator_formula <-
      create_lm_formula(dependant = "bin_var=='win'",
                        independant = weekly_vars_for_indicator)
    weekly_indicator_model <-
      glm(formula = weekly_indicator_formula,
          data =
            weekly_join_model %>%
            filter(Date <=pre_train_date_end) %>%
            filter( if_all(everything(), ~ !is.infinite(.)) ),
          family = binomial("logit"))
    summary(weekly_indicator_model)
    message("Passed weekly Model")

    saveRDS(object = weekly_indicator_model,
            file =
              glue::glue("{save_path}/{Asset_of_interest}_{trade_direction}_weekly_indicator_model.RDS"))

    weekly_indicator_pred <-
      weekly_join_model %>%
      mutate(
        weekly_indicator_pred = predict.glm(weekly_indicator_model,
                                            newdata = weekly_join_model, type = "response"),
        mean_weekly_pred =
          mean( ifelse(Date <= pre_train_date_end, weekly_indicator_pred, NA), na.rm = T ),
        sd_weekly_pred =
          sd( ifelse(Date <= pre_train_date_end, weekly_indicator_pred, NA), na.rm = T )
      ) %>%
      dplyr::select(
        Date, Asset, mean_weekly_pred, sd_weekly_pred, weekly_indicator_pred,bin_var,
        Weekly_perc_line_1_200, Weekly_perc_line_1_to_10_200, Weekly_perc_line_1_100,
        Weekly_High_Support, Weekly_High_Resistance, `Weekly_Markov_Point_Neg_roll_sum_-0.25`,
        `Weekly_Markov_Point_Pos_roll_sum_0.75`
      )
    message("Passed weekly Prediction")

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
      left_join(weekly_indicator_pred %>% dplyr::select(-bin_var) %>% distinct()) %>%
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
        actual_wins_losses %>%
          dplyr::select(Date, Asset, stop_factor, profit_factor, periods_ahead, trade_col,
                        Time_Periods,trade_start_prices, trade_end_prices, trade_return_dollar_aud)
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
    weekly_data = weekly_data,
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

    weekly_indicator <-
      get_weekly_indicators(
        weekly_data = weekly_data,
        asset_data = asset_data,
        Asset_of_interest = Asset_of_interest
      )

    weekly_indicator <-
      weekly_indicator %>%
      dplyr::select(-Price, -Open, -Low, -High, -Vol., -Date_for_join) %>%
      mutate(
        across(.cols = !contains("Date") & !contains("Asset"),
               .fns = ~ lag(.))
      ) %>%
      filter(if_all(everything(), ~ !is.na(.))) %>%
      mutate(
        Date = as_datetime(Date, tz = "Australia/Canberra")
      )

    weekly_join_model <-
      weekly_indicator %>%
      filter(if_all(everything(),~!is.na(.)))

    weekly_vars_for_indicator <-
      names(weekly_join_model) %>%
      keep(~ !str_detect(.x, "Date") &
             !str_detect(.x, "bin_var") &
             !str_detect(.x, "Asset")) %>%
      unlist() %>%
      as.character()

    weekly_indicator_formula <-
      create_lm_formula(dependant = "bin_var=='win'",
                        independant = weekly_vars_for_indicator)

    weekly_indicator_model <-
      readRDS(glue::glue("{save_path}/{Asset_of_interest}_{trade_direction}_weekly_indicator_model.RDS"))

    weekly_indicator_pred <-
      weekly_join_model %>%
      mutate(
        weekly_indicator_pred = predict.glm(weekly_indicator_model,
                                            newdata = weekly_join_model, type = "response"),
        mean_weekly_pred =
          mean( ifelse(Date <= pre_train_date_end, weekly_indicator_pred, NA), na.rm = T ),
        sd_weekly_pred =
          sd( ifelse(Date <= pre_train_date_end, weekly_indicator_pred, NA), na.rm = T )
      ) %>%
      dplyr::select(
        Date, Asset, mean_weekly_pred, sd_weekly_pred, weekly_indicator_pred,
        Weekly_perc_line_1_200, Weekly_perc_line_1_to_10_200, Weekly_perc_line_1_100,
        Weekly_High_Support, Weekly_High_Resistance, `Weekly_Markov_Point_Neg_roll_sum_-0.25`,
        `Weekly_Markov_Point_Pos_roll_sum_0.75`
      )
    message("Passed weekly Prediction")

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
      left_join(weekly_indicator_pred %>%
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
    weekly_data = weekly_data,
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
                "GBP_AUD", #10
                "WTICO_USD", #11
                "UK100_GBP", #12
                "USD_CAD", #13
                "GBP_USD", #14
                "GBP_CAD", #15
                "EUR_JPY", #16
                "EUR_AUD", #17
                "EUR_NZD", #18
                "XAG_USD", #19
                "XAG_EUR", #20
                "HK33_HKD", #21
                "SG30_SGD", #22
                "CH20_CHF", #23
                "XCU_USD", #24,
                "NZD_USD", #25
                "XAG_GBP", #26
                "BTC_USD", #27
                "XAU_USD", #28
                "USD_SEK"  #29
      ),
      couplua_assets =
        list(
          # "EUR_USD", #1
          c("XAU_EUR", "XAG_EUR", "EUR_JPY", "EU50_EUR", "EUR_AUD", "EUR_GBP",
            "SPX500_USD", "XAU_USD", "USD_JPY", "GBP_USD", "EUR_NZD", "XAG_GBP", "XAU_GBP",
            "EUR_SEK", "USD_CAD") %>% unique(), #1

          # "EU50_EUR", #2
          c("XAU_EUR", "XAG_EUR", "XAU_USD", "UK100_GBP", "SG30_SGD", "EUR_GBP", "SPX500_USD",
            "SPX500_USD", "XAU_USD", "AU200_AUD", "CH20_CHF", "US2000_USD",
            "XAG_GBP", "XAU_GBP", "WTICO_USD", "FR40_EUR", "HK33_HKD") %>% unique(), #2

          # "SPX500_USD", #3
          c("US2000_USD", "AU200_AUD", "USB10Y_USD", "UK100_GBP", "XAU_USD", "EU50_EUR",
            "HK33_HKD", "FR40_EUR", "WTICO_USD", "USD_JPY", "EUR_USD", "GBP_USD", "AU200_AUD",
            "SG30_SGD", "XAU_EUR", "XAG_EUR", "XAG_GBP", "XAU_GBP", "XAG_USD" ) %>% unique(), #3

          # US2000_USD #4
          c("SPX500_USD",  "AU200_AUD", "USB10Y_USD", "UK100_GBP", "XAU_USD", "EU50_EUR",
            "HK33_HKD", "FR40_EUR", "WTICO_USD", "USD_JPY", "EUR_USD", "GBP_USD", "AU200_AUD",
            "SG30_SGD", "XAU_EUR", "XAG_EUR", "XAG_GBP", "XAU_GBP","XAG_USD" ) %>% unique(), #4

          # USB10Y_USD #5
          c("SPX500_USD",  "AU200_AUD", "UK100_GBP", "XAU_USD", "EU50_EUR",
            "HK33_HKD", "FR40_EUR", "WTICO_USD", "USD_JPY", "EUR_USD",
            "XAU_EUR", "AU200_AUD", "XAG_USD",
            "SG30_SGD", "XAU_EUR", "XAG_EUR", "XAG_GBP", "XAU_GBP") %>% unique(), #5

          # USD_JPY #6
          c("EUR_JPY", "XAU_JPY", "XAG_JPY", "GBP_JPY", "XAU_USD", "SPX500_USD",
            "XAG_USD","NZD_USD", "AUD_USD", "EUR_USD", "GBP_USD", "USD_CAD",
            "USD_SEK", "USD_SGD", "USB10Y_USD") %>% unique(), #6

          # AUD_USD #7
          c("XCU_USD", "AU200_AUD", "XAU_AUD", "GBP_AUD", "XAU_USD", "EUR_AUD",
            "XAG_USD","NZD_USD", "USD_JPY", "EUR_USD", "GBP_USD", "USD_CAD",
            "USD_SEK", "USD_SGD", "USB10Y_USD", "AUD_NZD", "NZD_USD") %>% unique(), #7

          # EUR_GBP #8
          c("GBP_USD", "EUR_USD", "XAU_EUR", "XAU_GBP", "GBP_JPY", "EUR_JPY",
            "XAG_EUR", "XAG_GBP", "USD_JPY", "UK100_GBP", "FR40_EUR", "EU50_EUR",
            "EUR_SEK", "USD_SEK", "EUR_AUD", "EUR_NZD", "EUR_SEK") %>% unique(), #8

          # "AU200_AUD" ,#9
          c("XCU_USD", "US2000_USD", "UK100_GBP", "XAU_USD", "EU50_EUR",
            "HK33_HKD", "FR40_EUR", "WTICO_USD", "GBP_AUD", "AUD_NZD", "EUR_AUD",
            "SG30_SGD", "XAU_EUR", "XAG_EUR", "XAG_GBP", "XAU_GBP", "XAG_USD" ) %>% unique(), #9

          # "GBP_AUD", #10
          c("GBP_JPY", "GBP_CAD", "GBP_USD", "GBP_NZD", "XAU_GBP", "XAG_GBP", "UK100_GBP",
            "XAU_USD", "XAG_USD", "EUR_GBP", "EUR_USD", "XAG_EUR", "XAU_EUR", "USD_JPY",
            "EUR_JPY", "UK10YB_GBP", "AUD_USD", "USD_SEK", "USD_CAD") %>% unique(), #10

          # "WTICO_USD", #11
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

          # "EUR_AUD", #17
          c("XCU_USD", "AU200_AUD", "XAU_AUD", "GBP_AUD", "XAU_USD", "AUD_USD",
            "XAG_USD","NZD_USD", "USD_JPY", "EUR_USD",
            "USB10Y_USD", "AUD_NZD", "NZD_USD", "FR40_EUR", "EU50_EUR",
            "EUR_SEK", "EUR_NZD", "EUR_SEK") %>% unique(), #17

          # "EUR_NZD", #18
          c("EUR_AUD", "EUR_USD", "XAU_EUR", "XAU_AUD", "NZD_USD", "EUR_JPY", "EUR_GBP",
            "AUD_NZD", "GBP_NZD", "XAG_NZD", "XAG_EUR", "XAU_USD", "XAG_USD", "EUR_SEK",
            "FR40_EUR", "EU50_EUR", "AU200_AUD") %>% unique(), #18

          # "XAG_USD", #19
          c("XAG_JPY", "XAG_GBP", "XAG_EUR", "XAG_AUD", "XAU_USD", "EU50_EUR", "SPX500_USD",
            "XAG_NZD", "XAU_AUD", "XAU_GBP", "XAU_JPY", "XAU_EUR", "EUR_USD", "USD_JPY",
            "GBP_USD", "AUD_USD", "USD_CAD", "USD_SEK") %>% unique(), #19

          # "XAG_EUR", #20
          c("XAG_JPY", "XAG_GBP", "XAG_USD", "XAG_AUD", "XAU_USD", "EU50_EUR", "SPX500_USD",
            "XAG_NZD", "XAU_AUD", "XAU_GBP", "XAU_JPY", "XAU_EUR", "EUR_USD", "EUR_JPY",
            "EUR_GBP", "EUR_AUD", "EUR_SEK", "EUR_NZD") %>% unique(), #20

          # "HK33_HKD", #21
          c("US2000_USD", "AU200_AUD", "USB10Y_USD", "UK100_GBP", "XAU_USD", "EU50_EUR",
            "SPX500_USD", "FR40_EUR", "WTICO_USD", "USD_JPY", "EUR_USD", "GBP_USD", "AU200_AUD",
            "SG30_SGD", "XAU_EUR", "XAG_EUR", "XAG_GBP", "XAU_GBP", "XAG_USD") %>% unique(), #21

          # "SG30_SGD", #22
          c("XCU_USD", "US2000_USD", "UK100_GBP", "XAU_USD", "EU50_EUR", "CH20_CHF",
            "HK33_HKD", "FR40_EUR", "WTICO_USD", "GBP_AUD", "AUD_NZD", "EUR_AUD",
            "AU200_AUD", "XAU_EUR", "XAG_EUR", "XAG_GBP", "XAU_GBP", "XAG_USD" ) %>% unique(), #22

          # "CH20_CHF", #23
          c("XCU_USD", "US2000_USD", "UK100_GBP", "XAU_USD", "EU50_EUR",
            "HK33_HKD", "FR40_EUR", "WTICO_USD", "GBP_AUD", "AUD_NZD",
            "AU200_AUD", "XAU_EUR", "XAG_EUR", "XAG_GBP", "XAU_GBP", "XAG_USD" )%>% unique(), #23

          # "XCU_USD", #24,
          c("AU200_AUD", "US2000_USD", "UK100_GBP", "XAU_USD", "EU50_EUR",
            "HK33_HKD", "FR40_EUR", "WTICO_USD", "GBP_AUD", "AUD_NZD", "EUR_AUD",
            "SG30_SGD", "XAU_EUR", "XAG_EUR", "XAG_GBP", "XAU_GBP", "XAG_USD")%>% unique(), #24

          c("AUD_USD", "EUR_NZD", "GBP_NZD", "XAG_NZD", "XAU_USD", "XAG_USD", "XAU_AUD")%>% unique(), #25

          c("XAG_JPY", "XAG_USD", "XAG_EUR", "XAG_AUD", "XAU_USD", "EU50_EUR", "SPX500_USD", "XAU_GBP")%>% unique(), #26

          c("XAG_JPY", "XAG_USD", "XAG_EUR", "XAG_AUD", "XAU_USD", "EU50_EUR", "SPX500_USD", "XAU_GBP")%>% unique(), #27

          # "XAU_USD", #28
          c("XAG_JPY", "XAG_GBP", "XAG_EUR", "XAG_AUD", "XAG_USD", "EU50_EUR", "SPX500_USD",
            "XAG_NZD", "XAU_AUD", "XAU_GBP", "XAU_JPY", "XAU_EUR", "EUR_USD", "USD_JPY",
            "GBP_USD", "AUD_USD", "USD_CAD", "USD_SEK")%>% unique(), #28

          c("USD_JPY", "EUR_USD", "GBP_USD", "AUD_USD", "NZD_USD", "USD_CAD", "XAU_USD","XAG_USD" )%>% unique() #29

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
          c("GBP", "USD", "EUR", "AUD", "NZD"), #17
          c("GBP", "USD", "EUR", "AUD", "JPY"), #18
          c("GBP", "USD", "EUR", "AUD", "JPY"), #19
          c("GBP", "USD", "EUR", "AUD", "JPY"), #20
          c("GBP", "USD", "EUR", "AUD", "JPY"), #21
          c("GBP", "USD", "EUR", "AUD", "JPY"), #22
          c("GBP", "USD", "EUR", "AUD", "JPY"), #23
          c("GBP", "USD", "EUR", "AUD", "JPY"), #24
          c("GBP", "USD", "EUR", "AUD", "NZD"), #25
          c("GBP", "USD", "EUR", "AUD", "NZD"), #26
          c("GBP", "USD", "EUR", "AUD", "NZD"), #27
          c("GBP", "USD", "EUR", "AUD", "JPY"), #28
          c("GBP", "USD", "EUR", "AUD", "JPY") #29
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

      longs <-
        safely_find_preds(
          asset_data = Indices_Metals_Bonds,
          All_Daily_Data = All_Daily_Data,
          weekly_data = weekly_data,
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

      shorts <-
        safely_find_preds(
          asset_data = Indices_Metals_Bonds,
          All_Daily_Data = All_Daily_Data,
          weekly_data = weekly_data,
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
          trade_direction = "Short",
          stop_value_var = stop_value_var,
          profit_value_var = profit_value_var,
          period_var = period_var,
          save_path = save_path) %>%
        pluck('result') %>%
        mutate(
          trade_col = "Short"
        )

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

convert_daily_to_weekly <-
  function(All_Daily_Data = All_Daily_Data) {

    weekly_dat <-
      All_Daily_Data %>%
      mutate(week_date = floor_date(Date, "week")) %>%
      group_by(week_date, Asset) %>%
      mutate(min_date_in_week = min(Date, na.rm = T),
             max_date_in_week = max(Date, na.rm = T)) %>%
      group_by(min_date_in_week, Asset) %>%
      mutate(
        Open_Week = case_when(Date == min_date_in_week ~ Open),
        Price_Week = case_when(Date == max_date_in_week ~ Price),
        High_week = max(High, na.rm = T),
        Low_week = min(Low, na.rm = T)
      ) %>%
      ungroup() %>%
      group_by(max_date_in_week, Asset) %>%
      summarise(
        Open = mean(Open_Week, na.rm = T),
        Price = mean(Price_Week, na.rm = T),
        High = mean(High_week, na.rm = T),
        Low = mean(Low_week, na.rm = T)
      ) %>%
      ungroup() %>%
      mutate(
        Date = max_date_in_week,
        `Vol.` = 10
      ) %>%
      dplyr::select(Date, Price, Open, High, Low, `Vol.`, Asset)

    return(weekly_dat)

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
get_weekly_indicators <-
  function(weekly_data = weekly_data,
           asset_data = Indices_Metals_Bonds[[1]],
           Asset_of_interest = "EUR_USD") {

    weekly_technical_indicators <-
      weekly_data %>%
      filter(Asset == Asset_of_interest) %>%
      create_technical_indicators()

    new_names <- names(weekly_technical_indicators) %>%
      map(
        ~ case_when(
          .x %in% c("Date","Asset", "Price", "High", "Low", "Open") ~ .x,
          TRUE ~ paste0("Weekly_", .x)
        )
      ) %>%
      unlist() %>%
      as.character()

    names(weekly_technical_indicators) <- new_names

    joined_dat <-
      asset_data %>%
      filter(Asset == Asset_of_interest) %>%
      mutate(
        Date_for_join = as_date(Date)
      ) %>%
      left_join(weekly_technical_indicators %>%
                  mutate(Date_for_join = as_date(Date)) %>%
                  dplyr::select(-Date, -Price, -Low, -Open, -High),
                by = c("Date_for_join", "Asset")
      ) %>%
      fill(
        contains("Weekly"), .direction = "down"
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
single_asset_Logit_indicator <-
  function(
    asset_data = Indices_Metals_Bonds[[1]],
    All_Daily_Data = All_Daily_Data,
    weekly_data = weekly_data,
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

    weekly_indicator <-
      get_weekly_indicators(
        weekly_data = weekly_data,
        asset_data = asset_data,
        Asset_of_interest = Asset_of_interest
      )

    weekly_indicator <-
      weekly_indicator %>%
      dplyr::select(-Price, -Open, -Low, -High, -Vol., -Date_for_join) %>%
      mutate(
        across(.cols = !contains("Date") & !contains("Asset"),
               .fns = ~ lag(.))
      ) %>%
      filter(if_all(everything(), ~ !is.na(.))) %>%
      mutate(
        Date = as_datetime(Date, tz = "Australia/Canberra")
      )

    weekly_join_model <-
      actual_wins_losses %>%
      dplyr::select(Date, Asset ,bin_var) %>%
      filter(
        Asset == Asset_of_interest
      ) %>%
      left_join(weekly_indicator) %>%
      filter(if_all(everything(),~!is.na(.)))

    weekly_vars_for_indicator <-
      names(weekly_join_model) %>%
      keep(~ !str_detect(.x, "Date") &
             !str_detect(.x, "bin_var") &
             !str_detect(.x, "Asset")) %>%
      unlist() %>%
      as.character()

    weekly_indicator_formula <-
      create_lm_formula(dependant = "bin_var=='win'",
                        independant = weekly_vars_for_indicator)
    weekly_indicator_model <-
      glm(formula = weekly_indicator_formula,
          data =
            weekly_join_model %>%
            filter(Date <=pre_train_date_end) %>%
            filter( if_all(everything(), ~ !is.infinite(.)) ),
          family = binomial("logit"))
    summary(weekly_indicator_model)
    message("Passed weekly Model")

    weekly_indicator_pred <-
      weekly_join_model %>%
      mutate(
        weekly_indicator_pred = predict.glm(weekly_indicator_model,
                                            newdata = weekly_join_model, type = "response"),
        mean_weekly_pred =
          mean( ifelse(Date <= pre_train_date_end, weekly_indicator_pred, NA), na.rm = T ),
        sd_weekly_pred =
          sd( ifelse(Date <= pre_train_date_end, weekly_indicator_pred, NA), na.rm = T )
      ) %>%
      dplyr::select(
        Date, Asset, mean_weekly_pred, sd_weekly_pred, weekly_indicator_pred,bin_var,
        Weekly_perc_line_1_200, Weekly_perc_line_1_to_10_200, Weekly_perc_line_1_100,
        Weekly_High_Support, Weekly_High_Resistance, `Weekly_Markov_Point_Neg_roll_sum_-0.25`,
        `Weekly_Markov_Point_Pos_roll_sum_0.75`
      )
    message("Passed weekly Prediction")


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
      left_join(weekly_indicator_pred %>% dplyr::select(-bin_var) %>% distinct()) %>%
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
        actual_wins_losses %>%
          dplyr::select(Date, Asset, stop_factor, profit_factor, periods_ahead, trade_col,
                        Time_Periods,trade_start_prices, trade_end_prices, trade_return_dollar_aud)
      )

    return(logit_combined_pred)

  }
