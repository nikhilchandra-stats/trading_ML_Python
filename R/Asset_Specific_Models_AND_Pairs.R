#' create_asset_high_freq_data
#'
#' @param db_location
#' @param start_date
#' @param end_date
#' @param bid_or_ask
#' @param time_frame
#' @param asset
#' @param keep_bid_to_ask
#'
#' @return
#' @export
#'
#' @examples
create_asset_high_freq_data <-
  function(
    db_location = "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data For EDA.db",
    start_date = "2016-01-01",
    end_date = today() %>% as.character(),
    bid_or_ask = "ask",
    time_frame = "M15",
    asset = "AUD_USD",
    keep_bid_to_ask = TRUE
  ) {

    if(keep_bid_to_ask == TRUE) {
      combined_data <-
        get_db_price_asset(
          db_location = db_location,
          start_date = start_date,
          end_date = end_date,
          bid_or_ask = bid_or_ask,
          time_frame = time_frame,
          asset = asset
        )
    }

    if(keep_bid_to_ask == FALSE) {

      data_15_ask <-
        get_db_price_asset(
          db_location = db_location,
          start_date = start_date,
          end_date = end_date,
          bid_or_ask = "ask",
          time_frame = time_frame,
          asset = asset
        )

      data_15_bid <-
        get_db_price_asset(
          db_location = db_location,
          start_date = start_date,
          end_date = today() %>% as.character(),
          bid_or_ask = "bid",
          time_frame = time_frame,
          asset = asset
        )

      if(bid_or_ask == "ask") {

        data_15_ask2 <- data_15_ask %>%
          dplyr::select(Date, Asset, Price ,Open)

        data_15_bid2 <- data_15_bid %>%
          dplyr::select(Date, Asset, High, Low)

        combined_data <- data_15_ask2 %>%
          left_join(data_15_bid2)

      }

      if(bid_or_ask == "bid") {

        data_15_ask2 <- data_15_ask %>%
          dplyr::select(Date, Asset, High, Low)

        data_15_bid2 <- data_15_bid %>%
          dplyr::select(Date, Asset, Price ,Open)

        combined_data <-data_15_bid2 %>%
          left_join(data_15_ask2)

      }
    }

    gc()

    return(combined_data)

  }

#' get_all_AUD_USD_specific_data
#'
#' @param db_location
#' @param start_date
#' @param end_date
#'
#' @return
#' @export
#'
#' @examples
get_all_AUD_USD_specific_data <-
  function(
    db_location = "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data For EDA.db",
    start_date = "2016-01-01",
    end_date = today() %>% as.character(),
    time_frame = "H1"
  ) {

    AUD_USD <- create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "ask",
      time_frame = time_frame,
      asset = "AUD_USD",
      keep_bid_to_ask = TRUE
    )

    NZD_USD <- create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "ask",
      time_frame = time_frame,
      asset = "NZD_USD",
      keep_bid_to_ask = TRUE
    )

    XAG_USD <- create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "ask",
      time_frame = time_frame,
      asset = "XAG_USD",
      keep_bid_to_ask = TRUE
    )

    XAU_USD <- create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "ask",
      time_frame = time_frame,
      asset = "XAU_USD",
      keep_bid_to_ask = TRUE
    )

    XCU_USD <- create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "ask",
      time_frame = time_frame,
      asset = "XCU_USD",
      keep_bid_to_ask = TRUE
    )

    NZD_CHF <- create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "ask",
      time_frame = time_frame,
      asset = "NZD_CHF",
      keep_bid_to_ask = TRUE
    )

    AUD_USD_NZD_USD <-
      AUD_USD %>%
      bind_rows(NZD_USD) %>%
      bind_rows(XAG_USD) %>%
      bind_rows(XAU_USD) %>%
      bind_rows(XCU_USD) %>%
      bind_rows(NZD_CHF)

    rm(AUD_USD, NZD_USD, XAG_USD, XCU_USD, XAU_USD, NZD_CHF)
    gc()
    mean_values_by_asset_for_loop_15_ask <- wrangle_asset_data(AUD_USD_NZD_USD, summarise_means = TRUE)

    AUD_USD <- create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "bid",
      time_frame = time_frame,
      asset = "AUD_USD",
      keep_bid_to_ask = TRUE
    )

    NZD_USD <- create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "bid",
      time_frame = time_frame,
      asset = "NZD_USD",
      keep_bid_to_ask = TRUE
    )

    XAG_USD <- create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "bid",
      time_frame = time_frame,
      asset = "XAG_USD",
      keep_bid_to_ask = TRUE
    )

    XAU_USD <- create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "bid",
      time_frame = time_frame,
      asset = "XAU_USD",
      keep_bid_to_ask = TRUE
    )

    XCU_USD <- create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "bid",
      time_frame = time_frame,
      asset = "XCU_USD",
      keep_bid_to_ask = TRUE
    )

    NZD_CHF <- create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "bid",
      time_frame = time_frame,
      asset = "NZD_CHF",
      keep_bid_to_ask = TRUE
    )

    AUD_USD_NZD_USD_short <-
      AUD_USD %>%
      bind_rows(NZD_USD) %>%
      bind_rows(XAG_USD) %>%
      bind_rows(XAU_USD) %>%
      bind_rows(XCU_USD) %>%
      bind_rows(NZD_CHF)

    rm(AUD_USD, NZD_USD, XAG_USD, XCU_USD, XAU_USD, NZD_CHF)
    gc()

    return(list(AUD_USD_NZD_USD, AUD_USD_NZD_USD_short))
  }


#' get_AUD_USD_NZD_Specific_Trades
#'
#' @param AUD_USD
#' @param db_location
#' @param start_date
#' @param raw_macro_data
#' @param lag_days
#' @param lm_period
#' @param lm_train_prop
#' @param lm_test_prop
#' @param sd_fac_lm_trade
#' @param trade_direction
#'
#' @return
#' @export
#'
#' @examples
get_AUD_USD_NZD_Specific_Trades <-
  function(
    AUD_USD_NZD_USD = AUD_USD_NZD_USD_list[[1]],
    raw_macro_data = raw_macro_data,
    lag_days = 1,
    lm_period = 80,
    lm_train_prop = 0.5,
    lm_test_prop = 0.5,
    sd_fac_AUD_USD_trade = 1,
    sd_fac_NZD_USD_trade = 1,
    sd_fac_XCU_USD_trade = 1,
    trade_direction = "Long",
    stop_factor = 5,
    profit_factor = 10,
    assets_to_return = c("AUD_USD", "NZD_USD", "NZD_CHF", "XCU_USD", "XAG_USD", "XAU_USD")
  ) {

    aus_macro_data <-
      get_AUS_Indicators(raw_macro_data,
                         lag_days = lag_days)
    nzd_macro_data <-
      get_NZD_Indicators(raw_macro_data,
                         lag_days = lag_days)
    usd_macro_data <-
      get_USD_Indicators(raw_macro_data,
                         lag_days = lag_days)
    cny_macro_data <-
      get_CNY_Indicators(raw_macro_data,
                         lag_days = lag_days)
    eur_macro_data <-
      get_EUR_Indicators(raw_macro_data,
                         lag_days = lag_days)

    aud_macro_vars <- names(aus_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    nzd_macro_vars <- names(nzd_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    usd_macro_vars <- names(usd_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    cny_macro_vars <- names(cny_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    eur_macro_vars <- names(eur_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    all_macro_vars <- c(aud_macro_vars, nzd_macro_vars, usd_macro_vars, cny_macro_vars, eur_macro_vars)

    copula_data <-
      estimating_dual_copula(
        asset_data_to_use = AUD_USD_NZD_USD,
        asset_to_use = c("AUD_USD", "NZD_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      )

    copula_data_AUD_NZD_CHF <-
      estimating_dual_copula(
        asset_data_to_use = AUD_USD_NZD_USD,
        asset_to_use = c("AUD_USD", "NZD_CHF"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-AUD_USD, -AUD_USD_log1_price, -AUD_USD_quantiles_1, -AUD_USD_tangent_angle1)%>%
      dplyr::select(-NZD_CHF, -NZD_CHF_log2_price, -NZD_CHF_quantiles_2, -NZD_CHF_tangent_angle2)

    copula_data_AUD_XCU <-
      estimating_dual_copula(
        asset_data_to_use = AUD_USD_NZD_USD,
        asset_to_use = c("AUD_USD", "XCU_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-AUD_USD, -AUD_USD_log1_price, -AUD_USD_quantiles_1, -AUD_USD_tangent_angle1)

    copula_data_AUD_XAG <-
      estimating_dual_copula(
        asset_data_to_use = AUD_USD_NZD_USD,
        asset_to_use = c("AUD_USD", "XAG_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-AUD_USD, -AUD_USD_log1_price, -AUD_USD_quantiles_1, -AUD_USD_tangent_angle1)

    copula_data_NZD_XCU <-
      estimating_dual_copula(
        asset_data_to_use = AUD_USD_NZD_USD,
        asset_to_use = c("NZD_USD", "XCU_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-NZD_USD, -NZD_USD_log1_price, -NZD_USD_quantiles_1, -NZD_USD_tangent_angle1)%>%
      dplyr::select(-XCU_USD, -XCU_USD_log2_price, -XCU_USD_quantiles_2, -XCU_USD_tangent_angle2)

    copula_data_NZD_XAG <-
      estimating_dual_copula(
        asset_data_to_use = AUD_USD_NZD_USD,
        asset_to_use = c("NZD_USD", "XAG_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-NZD_USD, -NZD_USD_log1_price, -NZD_USD_quantiles_1, -NZD_USD_tangent_angle1)%>%
      dplyr::select(-XAG_USD, -XAG_USD_log2_price, -XAG_USD_quantiles_2, -XAG_USD_tangent_angle2)

    copula_data_NZD_USD_CHF <-
      estimating_dual_copula(
        asset_data_to_use = AUD_USD_NZD_USD,
        asset_to_use = c("NZD_USD", "NZD_CHF"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-NZD_USD, -NZD_USD_log1_price, -NZD_USD_quantiles_1, -NZD_USD_tangent_angle1)

    copula_data_macro <-
      copula_data %>%
      left_join(copula_data_AUD_XCU) %>%
      left_join(copula_data_AUD_XAG) %>%
      left_join(copula_data_NZD_XCU) %>%
      left_join(copula_data_NZD_XAG) %>%
      left_join(copula_data_NZD_USD_CHF) %>%
      left_join(copula_data_AUD_NZD_CHF) %>%
      mutate(Date_for_join = as_date(Date)) %>%
      left_join(
        aus_macro_data %>%
          rename(Date_for_join = date)
      ) %>%
      left_join(
        nzd_macro_data %>%
          rename(Date_for_join = date)
      ) %>%
      left_join(
        usd_macro_data %>%
          rename(Date_for_join = date)
      ) %>%
      left_join(
        cny_macro_data %>%
          rename(Date_for_join = date)
      )  %>%
      left_join(
        eur_macro_data %>%
          rename(Date_for_join = date)
      ) %>%
      fill(!contains("AUD_USD|Date"), .direction = "down") %>%
      filter(if_all(everything() ,.fns = ~ !is.na(.))) %>%
      mutate(
        dependant_var_aud_usd = log(lead(AUD_USD, lm_period)/AUD_USD),
        dependant_var_nzd_usd = log(lead(NZD_USD, lm_period)/NZD_USD),
        dependant_var_xcu_usd = log(lead(XCU_USD, lm_period)/XCU_USD),
        dependant_var_nzd_chf = log(lead(NZD_CHF, lm_period)/NZD_CHF)
      )

    max_data_in_copula <- copula_data_macro %>%
      pull(Date) %>%
      max(na.rm = T) %>%
      as.character()

    message(glue::glue("Max Date in Copula Data AUD NZD: {max_data_in_copula}"))

    lm_quant_vars <- names(copula_data_macro) %>% keep(~ str_detect(.x,"quantiles|tangent|cor"))
    lm_vars1 <- c(all_macro_vars, lm_quant_vars)

    training_data <- copula_data_macro %>%
      slice_head(prop = lm_train_prop)
    testing_data <- copula_data_macro %>%
      slice_tail(prop = lm_test_prop)

    max_data_in_testing_data <- testing_data %>%
      pull(Date) %>%
      max(na.rm = T) %>%
      as.character()

    message(glue::glue("Max Date in Testing Data AUD NZD: {max_data_in_testing_data}"))

    lm_formula_AUD_USD <- create_lm_formula(dependant = "dependant_var_aud_usd", independant = lm_vars1)
    lm_formula_AUD_USD_quant <- create_lm_formula(dependant = "dependant_var_aud_usd", independant = lm_quant_vars)
    lm_model_AUD_USD <- lm(formula = lm_formula_AUD_USD, data = training_data)
    lm_model_AUD_USD_quant <- lm(formula = lm_formula_AUD_USD_quant, data = training_data)
    summary(lm_model_AUD_USD)

    predicted_train_AUD_USD <- predict.lm(lm_model_AUD_USD, newdata = training_data)
    mean_pred_AUD_USD <- mean(predicted_train_AUD_USD, na.rm = T)
    sd_pred_AUD_USD <- sd(predicted_train_AUD_USD, na.rm = T)
    predicted_test_AUD_USD <- predict.lm(lm_model_AUD_USD, newdata = testing_data) %>% as.numeric()
    mean_pred_test_AUD_USD <- mean(predicted_test_AUD_USD, na.rm = T)
    sd_pred_test_AUD_USD <- sd(predicted_test_AUD_USD, na.rm = T)

    predicted_train_AUD_USD_quant <- predict.lm(lm_model_AUD_USD_quant, newdata = training_data)
    mean_pred_AUD_USD_quant <- mean(predicted_train_AUD_USD_quant, na.rm = T)
    sd_pred_AUD_USD_quant <- sd(predicted_train_AUD_USD_quant, na.rm = T)
    predicted_test_AUD_USD_quant <- predict.lm(lm_model_AUD_USD_quant, newdata = testing_data) %>% as.numeric()
    mean_pred_test_AUD_USD_quant <- mean(predicted_test_AUD_USD_quant, na.rm = T)
    sd_pred_test_AUD_USD_quant <- sd(predicted_test_AUD_USD_quant, na.rm = T)

    tagged_trades_AUD_USD <-
      testing_data %>%
      mutate(
        lm_pred_AUD_USD = predicted_test_AUD_USD,
        lm_pred_AUD_USD_quant = predicted_test_AUD_USD_quant
      ) %>%
      mutate(
        trade_col =
          case_when(
            # lm_pred_AUD_USD >= mean_pred_AUD_USD + sd_fac_AUD_USD_trade*sd_pred_AUD_USD &
            #   trade_direction == "Short" ~trade_direction,
            # lm_pred_AUD_USD <= mean_pred_AUD_USD - sd_fac_AUD_USD_trade*sd_pred_AUD_USD &
            #   trade_direction == "Long" ~ trade_direction,

            # lm_pred_AUD_USD >= mean_pred_AUD_USD + sd_fac_AUD_USD_trade*sd_pred_AUD_USD &
            #   trade_direction == "Short" ~trade_direction,
            lm_pred_AUD_USD <= mean_pred_AUD_USD + sd_fac_AUD_USD_trade*sd_pred_AUD_USD &
              trade_direction == "Short" ~ trade_direction,
            lm_pred_AUD_USD >= mean_pred_AUD_USD + sd_fac_AUD_USD_trade*sd_pred_AUD_USD &
              trade_direction == "Long" ~ trade_direction
            # lm_pred_AUD_USD <= mean_pred_AUD_USD + 0*sd_pred_AUD_USD &
            #   lm_pred_AUD_USD >= mean_pred_AUD_USD - sd_fac_AUD_USD_trade*sd_pred_AUD_USD &
            #   trade_direction == "Long" ~ trade_direction

          )
      ) %>%
      filter(!is.na(trade_col)) %>%
      dplyr::select(Date, trade_col) %>%
      mutate(
        Asset = "AUD_USD"
      ) %>%
      mutate(
        stop_factor = stop_factor,
        profit_factor = profit_factor
      )

    max_trades_AUD_USD <-
      tagged_trades_AUD_USD %>%
      pull(Date) %>%
      max(na.rm = T) %>%
      as.character()

    message(glue::glue("Max Date in Tagged Data AUD_USD: {max_trades_AUD_USD}"))

    lm_formula_NZD_USD <- create_lm_formula(dependant = "dependant_var_nzd_usd", independant = lm_vars1)
    lm_model_NZD_USD <- lm(formula = lm_formula_NZD_USD, data = training_data)
    lm_formula_NZD_USD_quant <- create_lm_formula(dependant = "dependant_var_nzd_usd", independant = lm_quant_vars)
    lm_model_NZD_USD_quant <- lm(formula = lm_formula_NZD_USD_quant, data = training_data)
    summary(lm_model_NZD_USD)

    predicted_train_NZD_USD <- predict.lm(lm_model_NZD_USD, newdata = training_data)
    mean_pred_NZD_USD <- mean(predicted_train_NZD_USD, na.rm = T)
    sd_pred_NZD_USD <- sd(predicted_train_NZD_USD, na.rm = T)
    predicted_test_NZD_USD <- predict.lm(lm_model_NZD_USD, newdata = testing_data) %>% as.numeric()
    mean_pred_test_NZD_USD <- mean(predicted_test_NZD_USD, na.rm = T)
    sd_pred_test_NZD_USD <- sd(predicted_test_NZD_USD, na.rm = T)

    predicted_train_NZD_USD_quant <- predict.lm(lm_model_NZD_USD_quant, newdata = training_data)
    mean_pred_NZD_USD_quant <- mean(predicted_train_NZD_USD_quant, na.rm = T)
    sd_pred_NZD_USD_quant <- sd(predicted_train_NZD_USD_quant, na.rm = T)
    predicted_test_NZD_USD_quant <- predict.lm(lm_model_NZD_USD_quant, newdata = testing_data) %>% as.numeric()
    mean_pred_test_NZD_USD_quant <- mean(predicted_test_NZD_USD_quant, na.rm = T)
    sd_pred_test_NZD_USD_quant <- sd(predicted_test_NZD_USD_quant, na.rm = T)

    tagged_trades_NZD_USD <-
      testing_data %>%
      mutate(
        lm_pred_NZD_USD = predicted_test_NZD_USD,
        lm_pred_NZD_USD_quant = predicted_test_NZD_USD_quant
      ) %>%
      mutate(
        trade_col =
          case_when(
            # lm_pred_NZD_USD >= mean_pred_NZD_USD + sd_fac_NZD_USD_trade*sd_pred_NZD_USD &
            #   trade_direction == "Short" ~ trade_direction,
            # lm_pred_NZD_USD <= mean_pred_NZD_USD - sd_fac_NZD_USD_trade*sd_pred_NZD_USD &
            #   trade_direction == "Long" ~ trade_direction

            # lm_pred_NZD_USD >= mean_pred_NZD_USD + sd_fac_NZD_USD_trade*sd_pred_NZD_USD &
            #   trade_direction == "Short" ~ trade_direction,
            lm_pred_NZD_USD <= mean_pred_NZD_USD + sd_fac_NZD_USD_trade*sd_pred_NZD_USD &
              trade_direction == "Short" ~ trade_direction,
            lm_pred_NZD_USD <= mean_pred_NZD_USD - sd_fac_NZD_USD_trade*sd_pred_NZD_USD &
              trade_direction == "Long" ~ trade_direction
            # lm_pred_NZD_USD <= mean_pred_NZD_USD + 0*sd_pred_NZD_USD &
            #   lm_pred_NZD_USD >= mean_pred_NZD_USD - sd_fac_NZD_USD_trade*sd_pred_NZD_USD &
            #   trade_direction == "Long" ~ trade_direction
          )
      ) %>%
      filter(!is.na(trade_col)) %>%
      dplyr::select(Date, trade_col) %>%
      mutate(
        Asset = "NZD_USD"
      ) %>%
      mutate(
        stop_factor = stop_factor,
        profit_factor = profit_factor
      )

    max_trades_NZD_USD <-
      tagged_trades_NZD_USD %>%
      pull(Date) %>%
      max(na.rm = T) %>%
      as.character()

    message(glue::glue("Max Date in Tagged Data NZD_USD: {max_trades_NZD_USD}"))

    #------------------------------------------------------------------XCU_USD

    lm_formula_XCU_USD <- create_lm_formula(dependant = "dependant_var_xcu_usd", independant = lm_vars1)
    lm_model_XCU_USD <- lm(formula = lm_formula_XCU_USD, data = training_data)
    lm_formula_XCU_USD_quant <- create_lm_formula(dependant = "dependant_var_xcu_usd", independant = lm_quant_vars)
    lm_model_XCU_USD_quant <- lm(formula = lm_formula_XCU_USD_quant, data = training_data)
    summary(lm_model_XCU_USD)

    predicted_train_XCU_USD <- predict.lm(lm_model_XCU_USD, newdata = training_data)
    mean_pred_XCU_USD <- mean(predicted_train_XCU_USD, na.rm = T)
    sd_pred_XCU_USD <- sd(predicted_train_XCU_USD, na.rm = T)
    predicted_test_XCU_USD <- predict.lm(lm_model_XCU_USD, newdata = testing_data) %>% as.numeric()
    mean_pred_test_XCU_USD <- mean(predicted_test_XCU_USD, na.rm = T)
    sd_pred_test_XCU_USD <- sd(predicted_test_XCU_USD, na.rm = T)

    predicted_train_XCU_USD_quant <- predict.lm(lm_model_XCU_USD_quant, newdata = training_data)
    mean_pred_XCU_USD_quant <- mean(predicted_train_XCU_USD_quant, na.rm = T)
    sd_pred_XCU_USD_quant <- sd(predicted_train_XCU_USD_quant, na.rm = T)
    predicted_test_XCU_USD_quant <- predict.lm(lm_model_XCU_USD_quant, newdata = testing_data) %>% as.numeric()
    mean_pred_test_XCU_USD_quant <- mean(predicted_test_XCU_USD_quant, na.rm = T)
    sd_pred_test_XCU_USD_quant <- sd(predicted_test_XCU_USD_quant, na.rm = T)

    tagged_trades_XCU_USD <-
      testing_data %>%
      mutate(
        lm_pred_XCU_USD = predicted_test_XCU_USD,
        lm_pred_XCU_USD_quant = predicted_test_XCU_USD_quant
      ) %>%
      mutate(
        trade_col =
          case_when(
            lm_pred_XCU_USD >= mean_pred_XCU_USD + sd_fac_XCU_USD_trade*sd_pred_XCU_USD &
              trade_direction == "Short" ~ trade_direction,
            lm_pred_XCU_USD <= mean_pred_XCU_USD - sd_fac_XCU_USD_trade*sd_pred_XCU_USD &
              trade_direction == "Long" ~ trade_direction,
          )
      ) %>%
      filter(!is.na(trade_col)) %>%
      dplyr::select(Date, trade_col) %>%
      mutate(
        Asset = "XCU_USD"
      ) %>%
      mutate(
        stop_factor = stop_factor,
        profit_factor = profit_factor
      )

    max_trades_XCU_USD <-
      tagged_trades_XCU_USD %>%
      pull(Date) %>%
      max(na.rm = T) %>%
      as.character()

    message(glue::glue("Max Date in Tagged Data XCU_USD: {max_trades_XCU_USD}"))

    return(list(tagged_trades_NZD_USD %>% filter(Asset %in% assets_to_return),
                tagged_trades_AUD_USD %>% filter(Asset %in% assets_to_return),
                tagged_trades_XCU_USD %>% filter(Asset %in% assets_to_return) ))

  }

#' get_EUR_GBP_USD_pairs_data
#'
#' @param db_location
#' @param start_date
#' @param end_date
#'
#' @return
#' @export
#'
#' @examples
get_EUR_GBP_USD_pairs_data <- function(
    db_location = "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data For EDA.db",
    start_date = "2016-01-01",
    end_date = today() %>% as.character()
) {

  EUR_USD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = "M15",
    asset = "EUR_USD",
    keep_bid_to_ask = TRUE
  )

  GBP_USD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = "M15",
    asset = "GBP_USD",
    keep_bid_to_ask = TRUE
  )

  EUR_GBP <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = "M15",
    asset = "EUR_GBP",
    keep_bid_to_ask = TRUE
  )

  EUR_JPY <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = "M15",
    asset = "EUR_JPY",
    keep_bid_to_ask = TRUE
  )

  GBP_JPY <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = "M15",
    asset = "GBP_JPY",
    keep_bid_to_ask = TRUE
  )

  USD_JPY <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = "M15",
    asset = "USD_JPY",
    keep_bid_to_ask = TRUE
  )

  EUR_USD_GBP_USD <-
    EUR_USD %>%
    bind_rows(GBP_USD) %>%
    bind_rows(EUR_GBP) %>%
    bind_rows(EUR_JPY)%>%
    bind_rows(GBP_JPY)%>%
    bind_rows(USD_JPY)
  rm(EUR_USD, GBP_USD, EUR_GBP, EUR_JPY, USD_JPY, GBP_JPY)
  gc()

  EUR_USD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = "M15",
    asset = "EUR_USD",
    keep_bid_to_ask = TRUE
  )

  GBP_USD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = "M15",
    asset = "GBP_USD",
    keep_bid_to_ask = TRUE
  )

  EUR_GBP <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = "M15",
    asset = "EUR_GBP",
    keep_bid_to_ask = TRUE
  )

  EUR_JPY <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = "M15",
    asset = "EUR_JPY",
    keep_bid_to_ask = TRUE
  )

  GBP_JPY <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = "M15",
    asset = "GBP_JPY",
    keep_bid_to_ask = TRUE
  )

  USD_JPY <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = "M15",
    asset = "USD_JPY",
    keep_bid_to_ask = TRUE
  )

  EUR_USD_GBP_USD_short <-
    EUR_USD %>%
    bind_rows(GBP_USD) %>%
    bind_rows(EUR_GBP)%>%
    bind_rows(EUR_JPY)%>%
    bind_rows(GBP_JPY)%>%
    bind_rows(USD_JPY)

  rm(EUR_USD, GBP_USD, EUR_GBP, EUR_JPY, USD_JPY, GBP_JPY)
  gc()

  return(
    list(
      EUR_USD_GBP_USD,
      EUR_USD_GBP_USD_short
    )
  )

}

#' get_EUR_GBP_Specific_Trades
#'
#' @param AUD_USD
#' @param db_location
#' @param start_date
#' @param raw_macro_data
#' @param lag_days
#' @param lm_period
#' @param lm_train_prop
#' @param lm_test_prop
#' @param sd_fac_lm_trade
#' @param trade_direction
#'
#' @return
#' @export
#'
#' @examples
get_EUR_GBP_Specific_Trades <-
  function(
    EUR_USD_GBP_USD = EUR_USD_GBP_USD_ALL[[1]],
    start_date = "2016-01-01",
    raw_macro_data = raw_macro_data,
    lag_days = 4,
    lm_period = 2,
    lm_train_prop = 0.85,
    lm_test_prop = 0.14,
    sd_fac_lm_trade_eur_usd = 1,
    sd_fac_lm_trade_gbp_usd = 1,
    sd_fac_lm_trade_eur_gbp = 1,
    sd_fac_lm_trade_eur_jpy = 1,
    sd_fac_lm_trade_gbp_jpy = 1,
    sd_fac_lm_trade_usd_jpy = 1,
    trade_direction = "Long",
    stop_factor = 10,
    profit_factor = 15
  ) {

    eur_macro_data <-
      get_EUR_Indicators(raw_macro_data,
                         lag_days = lag_days)
    gbp_macro_data <-
      get_GBP_Indicators(raw_macro_data,
                         lag_days = lag_days)
    usd_macro_data <-
      get_USD_Indicators(raw_macro_data,
                         lag_days = lag_days)
    jpy_macro_data <-
      get_JPY_Indicators(raw_macro_data,
                         lag_days = lag_days)

    eur_macro_vars <- names(eur_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    gbp_macro_vars <- names(gbp_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    usd_macro_vars <- names(usd_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    jpy_macro_vars <- names(jpy_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    all_macro_vars <- c(eur_macro_vars, gbp_macro_vars, usd_macro_vars, jpy_macro_vars)

    copula_data <-
      estimating_dual_copula(
        asset_data_to_use = EUR_USD_GBP_USD,
        asset_to_use = c("EUR_USD", "GBP_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      )

    copula_data_EUR_EUR_GBP <-
      estimating_dual_copula(
        asset_data_to_use = EUR_USD_GBP_USD,
        asset_to_use = c("EUR_USD", "EUR_GBP"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-EUR_USD, -EUR_USD_log1_price, -EUR_USD_quantiles_1, -EUR_USD_tangent_angle1)

    copula_data_GBP_EUR_GBP <-
      estimating_dual_copula(
        asset_data_to_use = EUR_USD_GBP_USD,
        asset_to_use = c("GBP_USD", "EUR_GBP"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-GBP_USD,
                    -GBP_USD_log1_price,
                    -GBP_USD_quantiles_1,
                    -GBP_USD_tangent_angle1, -EUR_GBP_log2_price, -EUR_GBP , -EUR_GBP_quantiles_2, -EUR_GBP_tangent_angle2)

    copula_data_EUR_USD_JPY <-
      estimating_dual_copula(
        asset_data_to_use = EUR_USD_GBP_USD,
        asset_to_use = c("EUR_USD", "EUR_JPY"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-EUR_USD,
                    -EUR_USD_log1_price,
                    -EUR_USD_quantiles_1,
                    -EUR_USD_tangent_angle1)

    copula_data_GBP_USD_JPY <-
      estimating_dual_copula(
        asset_data_to_use = EUR_USD_GBP_USD,
        asset_to_use = c("GBP_USD", "GBP_JPY"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-GBP_USD,
                    -GBP_USD_log1_price,
                    -GBP_USD_quantiles_1,
                    -GBP_USD_tangent_angle1)

    copula_data_EUR_JPY_USD_JPY <-
      estimating_dual_copula(
        asset_data_to_use = EUR_USD_GBP_USD,
        asset_to_use = c("EUR_JPY", "USD_JPY"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-EUR_JPY,
                    -EUR_JPY_log1_price,
                    -EUR_JPY_quantiles_1,
                    -EUR_JPY_tangent_angle1)

    copula_data_GBP_USD_USD_JPY <-
      estimating_dual_copula(
        asset_data_to_use = EUR_USD_GBP_USD,
        asset_to_use = c("GBP_JPY", "USD_JPY"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-GBP_JPY,
                    -GBP_JPY_log1_price,
                    -GBP_JPY_quantiles_1,
                    -GBP_JPY_tangent_angle1,
                    -USD_JPY,
                    -USD_JPY_log2_price,
                    -USD_JPY_quantiles_2,
                    -USD_JPY_tangent_angle2)



    copula_data_macro <-
      copula_data %>%
      left_join(copula_data_EUR_EUR_GBP) %>%
      left_join(copula_data_GBP_EUR_GBP) %>%
      left_join(copula_data_EUR_USD_JPY) %>%
      left_join(copula_data_GBP_USD_JPY) %>%
      left_join(copula_data_EUR_JPY_USD_JPY) %>%
      left_join(copula_data_GBP_USD_USD_JPY) %>%
      mutate(Date_for_join = as_date(Date)) %>%
      left_join(
        eur_macro_data %>%
          rename(Date_for_join = date)
      ) %>%
      left_join(
        gbp_macro_data %>%
          rename(Date_for_join = date)
      ) %>%
      left_join(
        usd_macro_data %>%
          rename(Date_for_join = date)
      ) %>%
      left_join(
          jpy_macro_data %>%
          rename(Date_for_join = date)
      ) %>%
      fill(!contains("AUD_USD|Date"), .direction = "down") %>%
      filter(if_all(everything() ,.fns = ~ !is.na(.))) %>%
      mutate(
        dependant_var_EUR_USD = log(lead(EUR_USD, lm_period)/EUR_USD),
        dependant_var_GBP_USD = log(lead(GBP_USD, lm_period)/GBP_USD),
        dependant_var_EUR_GBP = log(lead(EUR_GBP, lm_period)/EUR_GBP),
        dependant_var_EUR_JPY = log(lead(EUR_JPY, lm_period)/EUR_JPY),
        dependant_var_GBP_JPY = log(lead(GBP_JPY, lm_period)/GBP_JPY),
        dependant_var_USD_JPY = log(lead(USD_JPY, lm_period)/USD_JPY)
      )

    lm_quant_vars <- names(copula_data_macro) %>% keep(~ str_detect(.x,"quantiles|tangent|cor"))
    lm_vars1 <- c(all_macro_vars, lm_quant_vars)

    training_data <- copula_data_macro %>%
      slice_head(prop = lm_train_prop)
    testing_data <- copula_data_macro %>%
      slice_tail(prop = lm_test_prop)

    lm_formula_EUR_USD <- create_lm_formula(dependant = "dependant_var_EUR_USD", independant = lm_vars1)
    lm_model_EUR_USD <- lm(formula = lm_formula_EUR_USD, data = training_data)

    predicted_train_EUR_USD <- predict.lm(lm_model_EUR_USD, newdata = training_data)
    mean_pred_EUR_USD <- mean(predicted_train_EUR_USD, na.rm = T)
    sd_pred_EUR_USD <- sd(predicted_train_EUR_USD, na.rm = T)
    predicted_test_EUR_USD <- predict.lm(lm_model_EUR_USD, newdata = testing_data) %>% as.numeric()
    mean_pred_test_EUR_USD <- mean(predicted_test_EUR_USD, na.rm = T)
    sd_pred_test_EUR_USD <- sd(predicted_test_EUR_USD, na.rm = T)

    tagged_trades_EUR_USD <-
      testing_data %>%
      mutate(
        lm_pred_EUR_USD = predicted_test_EUR_USD
      ) %>%
      mutate(
        trade_col =
          case_when(
            lm_pred_EUR_USD >= mean_pred_EUR_USD + sd_fac_lm_trade_eur_usd*sd_pred_EUR_USD &
              trade_direction == "Short" ~trade_direction,
            lm_pred_EUR_USD <= mean_pred_EUR_USD - sd_fac_lm_trade_eur_usd*sd_pred_EUR_USD &
              trade_direction == "Long" ~ trade_direction
          )
      ) %>%
      filter(!is.na(trade_col)) %>%
      dplyr::select(Date, trade_col) %>%
      mutate(
        Asset = "EUR_USD"
      )

    lm_formula_GBP_USD <- create_lm_formula(dependant = "dependant_var_GBP_USD", independant = lm_vars1)
    lm_model_GBP_USD <- lm(formula = lm_formula_GBP_USD, data = training_data)

    predicted_train_GBP_USD <- predict.lm(lm_model_GBP_USD, newdata = training_data)
    mean_pred_GBP_USD <- mean(predicted_train_GBP_USD, na.rm = T)
    sd_pred_GBP_USD <- sd(predicted_train_GBP_USD, na.rm = T)
    predicted_test_GBP_USD <- predict.lm(lm_model_GBP_USD, newdata = testing_data) %>% as.numeric()
    mean_pred_test_GBP_USD <- mean(predicted_test_GBP_USD, na.rm = T)
    sd_pred_test_GBP_USD <- sd(predicted_test_GBP_USD, na.rm = T)

    tagged_trades_GBP_USD <-
      testing_data %>%
      mutate(
        lm_pred_GBP_USD = predicted_test_GBP_USD
      ) %>%
      mutate(
        trade_col =
          case_when(
            # lm_pred_GBP_USD >= mean_pred_GBP_USD + sd_fac_lm_trade_gbp_usd*sd_pred_GBP_USD &
            #   trade_direction == "Long" ~ trade_direction,
            # lm_pred_GBP_USD <= mean_pred_GBP_USD - sd_fac_lm_trade_gbp_usd*sd_pred_GBP_USD &
            #   trade_direction == "Short" ~ trade_direction

            lm_pred_GBP_USD >= mean_pred_GBP_USD + sd_fac_lm_trade_gbp_usd*sd_pred_GBP_USD &
              trade_direction == "Short" &  GBP_USD_tangent_angle2 < 0 ~ trade_direction,
            lm_pred_GBP_USD <= mean_pred_GBP_USD - sd_fac_lm_trade_gbp_usd*sd_pred_GBP_USD &
              trade_direction == "Long" &  GBP_USD_tangent_angle2 >0 ~ trade_direction

          )
      ) %>%
      filter(!is.na(trade_col)) %>%
      dplyr::select(Date, trade_col) %>%
      mutate(
        Asset = "GBP_USD"
      )

    lm_formula_EUR_GBP <- create_lm_formula(dependant = "dependant_var_EUR_GBP", independant = lm_vars1)
    lm_model_EUR_GBP <- lm(formula = lm_formula_EUR_GBP, data = training_data)

    predicted_train_EUR_GBP <- predict.lm(lm_model_EUR_GBP, newdata = training_data)
    mean_pred_EUR_GBP <- mean(predicted_train_EUR_GBP, na.rm = T)
    sd_pred_EUR_GBP <- sd(predicted_train_EUR_GBP, na.rm = T)
    predicted_test_EUR_GBP <- predict.lm(lm_model_EUR_GBP, newdata = testing_data) %>% as.numeric()
    mean_pred_test_EUR_GBP <- mean(predicted_test_EUR_GBP, na.rm = T)
    sd_pred_test_EUR_GBP <- sd(predicted_test_EUR_GBP, na.rm = T)

    tagged_trades_EUR_GBP <-
      testing_data %>%
      mutate(
        lm_pred_EUR_GBP = predicted_test_EUR_GBP
      ) %>%
      mutate(
        trade_col =
          case_when(
            lm_pred_EUR_GBP >= mean_pred_EUR_GBP + sd_fac_lm_trade_eur_gbp*sd_pred_EUR_GBP &
              trade_direction == "Long" ~ trade_direction,
            lm_pred_EUR_GBP <= mean_pred_EUR_GBP - sd_fac_lm_trade_eur_gbp*sd_pred_EUR_GBP &
              trade_direction == "Short" ~ trade_direction
          )
      ) %>%
      filter(!is.na(trade_col)) %>%
      dplyr::select(Date, trade_col) %>%
      mutate(
        Asset = "EUR_GBP"
      )


    lm_formula_EUR_JPY <- create_lm_formula(dependant = "dependant_var_EUR_JPY", independant = lm_vars1)
    lm_model_EUR_JPY <- lm(formula = lm_formula_EUR_JPY, data = training_data)

    predicted_train_EUR_JPY <- predict.lm(lm_model_EUR_JPY, newdata = training_data)
    mean_pred_EUR_JPY <- mean(predicted_train_EUR_JPY, na.rm = T)
    sd_pred_EUR_JPY <- sd(predicted_train_EUR_JPY, na.rm = T)
    predicted_test_EUR_JPY <- predict.lm(lm_model_EUR_JPY, newdata = testing_data) %>% as.numeric()
    mean_pred_test_EUR_JPY <- mean(predicted_test_EUR_JPY, na.rm = T)
    sd_pred_test_EUR_JPY <- sd(predicted_test_EUR_JPY, na.rm = T)

    tagged_trades_EUR_JPY <-
      testing_data %>%
      mutate(
        lm_pred_EUR_JPY = predicted_test_EUR_JPY
      ) %>%
      mutate(
        trade_col =
          case_when(
            lm_pred_EUR_JPY >= mean_pred_EUR_JPY + sd_fac_lm_trade_eur_jpy*sd_pred_EUR_JPY &
              trade_direction == "Short" ~ trade_direction,
            lm_pred_EUR_JPY <= mean_pred_EUR_JPY - sd_fac_lm_trade_eur_jpy*sd_pred_EUR_JPY &
              trade_direction == "Long" ~ trade_direction
          )
      ) %>%
      filter(!is.na(trade_col)) %>%
      dplyr::select(Date, trade_col) %>%
      mutate(
        Asset = "EUR_JPY"
      )

    lm_formula_GBP_JPY <- create_lm_formula(dependant = "dependant_var_GBP_JPY", independant = lm_vars1)
    lm_model_GBP_JPY <- lm(formula = lm_formula_GBP_JPY, data = training_data)

    predicted_train_GBP_JPY <- predict.lm(lm_model_GBP_JPY, newdata = training_data)
    mean_pred_GBP_JPY <- mean(predicted_train_GBP_JPY, na.rm = T)
    sd_pred_GBP_JPY <- sd(predicted_train_GBP_JPY, na.rm = T)
    predicted_test_GBP_JPY <- predict.lm(lm_model_GBP_JPY, newdata = testing_data) %>% as.numeric()
    mean_pred_test_GBP_JPY <- mean(predicted_test_GBP_JPY, na.rm = T)
    sd_pred_test_GBP_JPY <- sd(predicted_test_GBP_JPY, na.rm = T)

    tagged_trades_GBP_JPY <-
      testing_data %>%
      mutate(
        lm_pred_GBP_JPY = predicted_test_GBP_JPY
      ) %>%
      mutate(
        trade_col =
          case_when(
            lm_pred_GBP_JPY >= mean_pred_GBP_JPY + sd_fac_lm_trade_gbp_jpy*sd_pred_GBP_JPY &
              trade_direction == "Short" ~ trade_direction,
            lm_pred_GBP_JPY <= mean_pred_GBP_JPY - sd_fac_lm_trade_gbp_jpy*sd_pred_GBP_JPY &
              trade_direction == "Long" ~ trade_direction
          )
      ) %>%
      filter(!is.na(trade_col)) %>%
      dplyr::select(Date, trade_col) %>%
      mutate(
        Asset = "GBP_JPY"
      )

    lm_formula_USD_JPY <- create_lm_formula(dependant = "dependant_var_USD_JPY", independant = lm_vars1)
    lm_model_USD_JPY <- lm(formula = lm_formula_USD_JPY, data = training_data)

    predicted_train_USD_JPY <- predict.lm(lm_model_USD_JPY, newdata = training_data)
    mean_pred_USD_JPY <- mean(predicted_train_USD_JPY, na.rm = T)
    sd_pred_USD_JPY <- sd(predicted_train_USD_JPY, na.rm = T)
    predicted_test_USD_JPY <- predict.lm(lm_model_USD_JPY, newdata = testing_data) %>% as.numeric()
    mean_pred_test_USD_JPY <- mean(predicted_test_USD_JPY, na.rm = T)
    sd_pred_test_USD_JPY <- sd(predicted_test_USD_JPY, na.rm = T)

    tagged_trades_USD_JPY <-
      testing_data %>%
      mutate(
        lm_pred_USD_JPY = predicted_test_USD_JPY
      ) %>%
      mutate(
        trade_col =
          case_when(
            lm_pred_USD_JPY >= mean_pred_USD_JPY + sd_fac_lm_trade_usd_jpy*sd_pred_USD_JPY &
              trade_direction == "Short" ~ trade_direction,
            lm_pred_USD_JPY <= mean_pred_USD_JPY - sd_fac_lm_trade_usd_jpy*sd_pred_USD_JPY &
              trade_direction == "Long" ~ trade_direction
          )
      ) %>%
      filter(!is.na(trade_col)) %>%
      dplyr::select(Date, trade_col) %>%
      mutate(
        Asset = "USD_JPY"
      )

    return(list(tagged_trades_EUR_USD %>%
                  mutate( stop_factor = stop_factor,
                          profit_factor = profit_factor) ,
                tagged_trades_GBP_USD %>%
                  mutate( stop_factor = stop_factor,
                          profit_factor = profit_factor)  ,
                tagged_trades_EUR_GBP %>%
                  mutate( stop_factor = stop_factor,
                          profit_factor = profit_factor)  ,
                tagged_trades_EUR_JPY %>%
                  mutate( stop_factor = stop_factor,
                          profit_factor = profit_factor),
                tagged_trades_GBP_JPY %>%
                  mutate( stop_factor = stop_factor,
                          profit_factor = profit_factor),
                tagged_trades_USD_JPY %>%
                  mutate( stop_factor = stop_factor,
                          profit_factor = profit_factor)
                )
           )

}


#' get_SPX_US2000_XAG_XAU
#'
#' @returns
#' @export
#'
#' @examples
get_SPX_US2000_XAG_XAU <- function(
    db_location = db_location,
    start_date = "2016-01-01",
    end_date = today() %>% as.character()
    ) {

  SPX <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = "M15",
    asset = "SPX500_USD",
    keep_bid_to_ask = TRUE
  )

  US2000 <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = "M15",
    asset = "US2000_USD",
    keep_bid_to_ask = TRUE
  )

  XAG <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = "M15",
    asset = "XAG_USD",
    keep_bid_to_ask = TRUE
  )

  XAU <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = "M15",
    asset = "XAU_USD",
    keep_bid_to_ask = TRUE
  )

  XAG_SPX_US2000_USD <- SPX %>% bind_rows(US2000) %>% bind_rows(XAG)%>% bind_rows(XAU)
  rm(SPX, US2000, XAG, XAU)
  gc()

  SPX <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = "M15",
    asset = "SPX500_USD",
    keep_bid_to_ask = TRUE
  )

  US2000 <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = "M15",
    asset = "US2000_USD",
    keep_bid_to_ask = TRUE
  )

  XAG <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = "M15",
    asset = "XAG_USD",
    keep_bid_to_ask = TRUE
  )

  XAU <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = "M15",
    asset = "XAU_USD",
    keep_bid_to_ask = TRUE
  )

  XAG_SPX_US2000_USD_short <- SPX %>% bind_rows(US2000) %>% bind_rows(XAG) %>% bind_rows(XAU)
  rm(SPX, US2000, XAG, XAU)
  gc()

  return(
    list(
      XAG_SPX_US2000_USD,
      XAG_SPX_US2000_USD_short
    )
  )

}

#' get_SPX_US2000_XAG_Specific_Trades
#'
#' @param SPX_US2000_XAG
#' @param start_date
#' @param raw_macro_data
#' @param lag_days
#' @param lm_period
#' @param lm_train_prop
#' @param lm_test_prop
#' @param sd_fac_lm_trade_SPX_USD
#' @param sd_fac_lm_trade_US2000_USD
#' @param sd_fac_lm_trade_XAG_USD
#' @param sd_fac_lm_trade_XAU_USD
#' @param trade_direction
#' @param stop_factor
#' @param profit_factor
#' @param assets_to_filter
#'
#' @returns
#' @export
#'
#' @examples
get_SPX_US2000_XAG_Specific_Trades <-
  function(
    SPX_US2000_XAG = SPX_US2000_XAG,
    start_date = "2016-01-01",
    raw_macro_data = raw_macro_data,
    lag_days = 1,
    lm_period = 80,
    lm_train_prop = 0.25,
    lm_test_prop = 0.75,
    sd_fac_lm_trade_SPX_USD = 1,
    sd_fac_lm_trade_US2000_USD = 1,
    sd_fac_lm_trade_XAG_USD = 1,
    sd_fac_lm_trade_XAU_USD = 1,
    trade_direction = "Long",
    stop_factor = 10,
    profit_factor = 15
  ) {

    eur_macro_data <-
      get_EUR_Indicators(raw_macro_data,
                         lag_days = lag_days)
    gbp_macro_data <-
      get_GBP_Indicators(raw_macro_data,
                         lag_days = lag_days)
    usd_macro_data <-
      get_USD_Indicators(raw_macro_data,
                         lag_days = lag_days)
    cny_macro_data <-
      get_CNY_Indicators(raw_macro_data,
                         lag_days = lag_days)

    eur_macro_vars <- names(eur_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    gbp_macro_vars <- names(gbp_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    usd_macro_vars <- names(usd_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    cny_macro_vars <- names(cny_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    all_macro_vars <- c(eur_macro_vars, gbp_macro_vars, usd_macro_vars, cny_macro_vars)

    copula_data <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("SPX500_USD", "US2000_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      )

    copula_data_SPX_XAG <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("SPX500_USD", "XAG_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-SPX500_USD, -SPX500_USD_log1_price, -SPX500_USD_quantiles_1, -SPX500_USD_tangent_angle1)

    copula_data_US2000_XAG <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("US2000_USD", "XAG_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-US2000_USD,
                    -US2000_USD_log1_price,
                    -US2000_USD_quantiles_1,
                    -US2000_USD_tangent_angle1,
                    -XAG_USD_log2_price, -XAG_USD , -XAG_USD_quantiles_2, -XAG_USD_tangent_angle2)

    copula_data_US2000_XAU <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("US2000_USD", "XAU_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-US2000_USD,
                    -US2000_USD_log1_price,
                    -US2000_USD_quantiles_1,
                    -US2000_USD_tangent_angle1)

    copula_data_SPX_XAU <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("SPX500_USD", "XAU_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-SPX500_USD,
                    -SPX500_USD_log1_price,
                    -SPX500_USD_quantiles_1,
                    -SPX500_USD_tangent_angle1,
                    -XAU_USD_log2_price, -XAU_USD , -XAU_USD_quantiles_2, -XAU_USD_tangent_angle2)

    copula_data_macro <-
      copula_data %>%
      left_join(copula_data_SPX_XAG) %>%
      left_join(copula_data_US2000_XAG) %>%
      left_join(copula_data_US2000_XAU) %>%
      left_join(copula_data_SPX_XAU) %>%
      mutate(Date_for_join = as_date(Date)) %>%
      left_join(
        eur_macro_data %>%
          rename(Date_for_join = date)
      ) %>%
      left_join(
        gbp_macro_data %>%
          rename(Date_for_join = date)
      ) %>%
      left_join(
        usd_macro_data %>%
          rename(Date_for_join = date)
      ) %>%
      left_join(
        cny_macro_data %>%
          rename(Date_for_join = date)
      ) %>%
      fill(!contains("AUD_USD|Date"), .direction = "down") %>%
      filter(if_all(everything() ,.fns = ~ !is.na(.))) %>%
      mutate(
        dependant_var_SPX500_USD = log(lead(SPX500_USD, lm_period)/SPX500_USD),
        dependant_var_US2000_USD = log(lead(US2000_USD, lm_period)/US2000_USD),
        dependant_var_XAG_USD = log(lead(XAG_USD, lm_period)/XAG_USD),
        dependant_var_XAU_USD = log(lead(XAU_USD, lm_period)/XAU_USD)
      )

    lm_quant_vars <- names(copula_data_macro) %>% keep(~ str_detect(.x,"quantiles|tangent|cor"))
    lm_vars1 <- c(all_macro_vars, lm_quant_vars)

    training_data <- copula_data_macro %>%
      slice_head(prop = lm_train_prop)
    testing_data <- copula_data_macro %>%
      slice_tail(prop = lm_test_prop)

    lm_formula_SPX_USD <- create_lm_formula(dependant = "dependant_var_SPX500_USD", independant = lm_vars1)
    lm_model_SPX_USD <- lm(formula = lm_formula_SPX_USD, data = training_data)
    summary(lm_model_SPX_USD)
    predicted_train_SPX_USD <- predict.lm(lm_model_SPX_USD, newdata = training_data)
    mean_pred_SPX_USD <- mean(predicted_train_SPX_USD, na.rm = T)
    sd_pred_SPX_USD <- sd(predicted_train_SPX_USD, na.rm = T)
    predicted_test_SPX_USD <- predict.lm(lm_model_SPX_USD, newdata = testing_data) %>% as.numeric()
    mean_pred_test_SPX_USD <- mean(predicted_test_SPX_USD, na.rm = T)
    sd_pred_test_SPX_USD <- sd(predicted_test_SPX_USD, na.rm = T)

    tagged_trades_SPX_USD <-
      testing_data %>%
      mutate(
        lm_pred_SPX_USD = predicted_test_SPX_USD
      ) %>%
      mutate(
        trade_col =
          case_when(
            lm_pred_SPX_USD >= mean_pred_SPX_USD + sd_fac_lm_trade_SPX_USD*sd_pred_SPX_USD &
              trade_direction == "Short" ~trade_direction,
            lm_pred_SPX_USD <= mean_pred_SPX_USD - sd_fac_lm_trade_SPX_USD*sd_pred_SPX_USD &
              trade_direction == "Long" ~ trade_direction
          )
      ) %>%
      filter(!is.na(trade_col)) %>%
      dplyr::select(Date, trade_col) %>%
      mutate(
        Asset = "SPX500_USD"
      )

    lm_formula_US2000_USD <- create_lm_formula(dependant = "dependant_var_US2000_USD", independant = lm_vars1)
    lm_model_US2000_USD <- lm(formula = lm_formula_US2000_USD, data = training_data)
    summary(lm_model_US2000_USD)
    predicted_train_US2000_USD <- predict.lm(lm_model_US2000_USD, newdata = training_data)
    mean_pred_US2000_USD <- mean(predicted_train_US2000_USD, na.rm = T)
    sd_pred_US2000_USD <- sd(predicted_train_US2000_USD, na.rm = T)
    predicted_test_US2000_USD <- predict.lm(lm_model_US2000_USD, newdata = testing_data) %>% as.numeric()
    mean_pred_test_US2000_USD <- mean(predicted_test_US2000_USD, na.rm = T)
    sd_pred_test_US2000_USD <- sd(predicted_test_US2000_USD, na.rm = T)

    tagged_trades_US2000_USD <-
      testing_data %>%
      mutate(
        lm_pred_US2000_USD = predicted_test_US2000_USD
      ) %>%
      mutate(
        trade_col =
          case_when(
            lm_pred_US2000_USD >= mean_pred_US2000_USD + sd_fac_lm_trade_US2000_USD*sd_pred_US2000_USD &
              trade_direction == "Short" ~ trade_direction,
            lm_pred_US2000_USD <= mean_pred_US2000_USD - sd_fac_lm_trade_US2000_USD*sd_pred_US2000_USD &
              trade_direction == "Long" ~ trade_direction

            # lm_pred_US2000_USD >= mean_pred_US2000_USD + sd_fac_lm_trade_US2000_USD*sd_pred_US2000_USD &
            #   trade_direction == "Long" &  US2000_USD_tangent_angle2 < 0 ~ trade_direction,
            # lm_pred_US2000_USD <= mean_pred_US2000_USD - sd_fac_lm_trade_US2000_USD*sd_pred_US2000_USD &
            #   trade_direction == "Short" &  US2000_USD_tangent_angle2 >0 ~ trade_direction

          )
      ) %>%
      filter(!is.na(trade_col)) %>%
      dplyr::select(Date, trade_col) %>%
      mutate(
        Asset = "US2000_USD"
      )

    lm_formula_XAG_USD <- create_lm_formula(dependant = "dependant_var_XAG_USD", independant = lm_vars1)
    lm_model_XAG_USD <- lm(formula = lm_formula_XAG_USD, data = training_data)
    summary(lm_model_XAG_USD)
    predicted_train_XAG_USD <- predict.lm(lm_model_XAG_USD, newdata = training_data)
    mean_pred_XAG_USD <- mean(predicted_train_XAG_USD, na.rm = T)
    sd_pred_XAG_USD <- sd(predicted_train_XAG_USD, na.rm = T)
    predicted_test_XAG_USD <- predict.lm(lm_model_XAG_USD, newdata = testing_data) %>% as.numeric()
    mean_pred_test_XAG_USD <- mean(predicted_test_XAG_USD, na.rm = T)
    sd_pred_test_XAG_USD <- sd(predicted_test_XAG_USD, na.rm = T)

    tagged_trades_XAG_USD <-
      testing_data %>%
      mutate(
        lm_pred_XAG_USD = predicted_test_XAG_USD
      ) %>%
      mutate(
        trade_col =
          case_when(
            lm_pred_XAG_USD >= mean_pred_XAG_USD + sd_fac_lm_trade_XAG_USD*sd_pred_XAG_USD &
              trade_direction == "Short" ~ trade_direction,
            lm_pred_XAG_USD <= mean_pred_XAG_USD - sd_fac_lm_trade_XAG_USD*sd_pred_XAG_USD &
              trade_direction == "Long" ~ trade_direction
          )
      ) %>%
      filter(!is.na(trade_col)) %>%
      dplyr::select(Date, trade_col) %>%
      mutate(
        Asset = "XAG_USD"
      )


    lm_formula_XAU_USD <- create_lm_formula(dependant = "dependant_var_XAU_USD", independant = lm_vars1)
    lm_model_XAU_USD <- lm(formula = lm_formula_XAU_USD, data = training_data)
    summary(lm_model_XAU_USD)
    predicted_train_XAU_USD <- predict.lm(lm_model_XAU_USD, newdata = training_data)
    mean_pred_XAU_USD <- mean(predicted_train_XAU_USD, na.rm = T)
    sd_pred_XAU_USD <- sd(predicted_train_XAU_USD, na.rm = T)
    predicted_test_XAU_USD <- predict.lm(lm_model_XAU_USD, newdata = testing_data) %>% as.numeric()
    mean_pred_test_XAU_USD <- mean(predicted_test_XAU_USD, na.rm = T)
    sd_pred_test_XAU_USD <- sd(predicted_test_XAU_USD, na.rm = T)

    tagged_trades_XAU_USD <-
      testing_data %>%
      mutate(
        lm_pred_XAU_USD = predicted_test_XAU_USD
      ) %>%
      mutate(
        trade_col =
          case_when(
            lm_pred_XAU_USD >= mean_pred_XAU_USD + sd_fac_lm_trade_XAU_USD*sd_pred_XAU_USD &
              trade_direction == "Long" ~ trade_direction,
            lm_pred_XAU_USD <= mean_pred_XAU_USD - sd_fac_lm_trade_XAU_USD*sd_pred_XAU_USD &
              trade_direction == "Short" ~ trade_direction
          )
      ) %>%
      filter(!is.na(trade_col)) %>%
      dplyr::select(Date, trade_col) %>%
      mutate(
        Asset = "XAU_USD"
      )

    return(list(tagged_trades_SPX_USD %>%
                  mutate( stop_factor = stop_factor,
                          profit_factor = profit_factor) ,
                tagged_trades_US2000_USD %>%
                  mutate( stop_factor = stop_factor,
                          profit_factor = profit_factor)  ,
                tagged_trades_XAG_USD %>%
                  mutate( stop_factor = stop_factor,
                          profit_factor = profit_factor),
                tagged_trades_XAU_USD %>%
                  mutate( stop_factor = stop_factor,
                          profit_factor = profit_factor)
    )
    )

}


#' mean_values_by_asset_for_loop
#'
#' @param tagged_trades
#' @param stop_factor
#' @param profit_factor
#' @param raw_asset_data
#'
#' @return
#' @export
#'
#' @examples
run_pairs_analysis <- function(
    tagged_trades = tagged_trades_AUD_USD %>% bind_rows(tagged_trades_NZD_USD),
    stop_factor = 5,
    profit_factor = 10,
    raw_asset_data = AUD_USD_NZD_USD,
    risk_dollar_value = 10
) {

  mean_values_by_asset_for_loop <-
    wrangle_asset_data(
      asset_data_daily_raw = raw_asset_data,
      summarise_means = TRUE
    )

  long_bayes_loop_analysis<-
    generic_trade_finder_loop(
      tagged_trades = tagged_trades ,
      asset_data_daily_raw = raw_asset_data,
      stop_factor = stop_factor,
      profit_factor =profit_factor,
      trade_col = "trade_col",
      date_col = "Date",
      start_price_col = "Price",
      mean_values_by_asset = mean_values_by_asset_for_loop
    )

  trade_timings <-
    long_bayes_loop_analysis %>%
    mutate(
      ending_date_trade = as_datetime(ending_date_trade),
      dates = as_datetime(dates)
    ) %>%
    mutate(Time_Required = (ending_date_trade - dates)/dhours(1) )

  trade_timings_by_asset <- trade_timings %>%
    mutate(win_loss = ifelse(trade_returns < 0, "loss", "wins") ) %>%
    group_by(win_loss) %>%
    summarise(
      Time_Required = median(Time_Required, na.rm = T)
    ) %>%
    pivot_wider(names_from = win_loss, values_from = Time_Required) %>%
    rename(loss_time_hours = loss,
           win_time_hours = wins)

  analysis_data <-
    generic_anlyser(
      trade_data = long_bayes_loop_analysis %>% rename(Asset = asset),
      profit_factor = profit_factor,
      stop_factor = stop_factor,
      asset_infor = asset_infor,
      currency_conversion = currency_conversion,
      asset_col = "Asset",
      stop_col = "starting_stop_value",
      profit_col = "starting_profit_value",
      price_col = "trade_start_prices",
      trade_return_col = "trade_returns",
      risk_dollar_value = risk_dollar_value,
      grouping_vars = "trade_col"
    ) %>%
    bind_cols(trade_timings_by_asset)

  analysis_data_asset <-
    generic_anlyser(
      trade_data = long_bayes_loop_analysis %>% rename(Asset = asset),
      profit_factor = profit_factor,
      stop_factor = stop_factor,
      asset_infor = asset_infor,
      currency_conversion = currency_conversion,
      asset_col = "Asset",
      stop_col = "starting_stop_value",
      profit_col = "starting_profit_value",
      price_col = "trade_start_prices",
      trade_return_col = "trade_returns",
      risk_dollar_value = risk_dollar_value,
      grouping_vars = "Asset"
    ) %>%
    bind_cols(trade_timings_by_asset)

  return(list(analysis_data, analysis_data_asset))

}

#' get_stops_profs_asset_specific
#'
#' @param trades_to_convert
#' @param raw_asset_data
#' @param currency_conversion
#' @param risk_dollar_value
#'
#' @returns
#' @export
#'
#' @examples
get_stops_profs_asset_specific <-
  function(
    trades_to_convert = EUR_GBP_USD_Trades_long,
    raw_asset_data = EUR_USD_GBP_USD,
    currency_conversion = currency_conversion,
    risk_dollar_value = 5
  ) {

    mean_values_by_asset_for_loop =
      wrangle_asset_data(raw_asset_data, summarise_means = TRUE)

    trades_with_stops_profs <-
      trades_to_convert %>%
      left_join(raw_asset_data %>% dplyr::select(Date, Asset, Price, Open, High, Low)) %>%
      slice_max(Date) %>%
      mutate(kk = row_number()) %>%
      split(.$kk) %>%
      map_dfr(
        ~
          get_stops_profs_volume_trades(
            tagged_trades = .x,
            mean_values_by_asset = mean_values_by_asset_for_loop,
            trade_col = "trade_col",
            currency_conversion = currency_conversion,
            risk_dollar_value = risk_dollar_value,
            stop_factor = .x$stop_factor[1] %>% as.numeric(),
            profit_factor = .x$profit_factor[1] %>% as.numeric(),
            asset_col = "Asset",
            stop_col = "stop_value",
            profit_col = "profit_value",
            price_col = "Price",
            trade_return_col = "trade_returns"
          )
      )

    return(trades_with_stops_profs)

}


