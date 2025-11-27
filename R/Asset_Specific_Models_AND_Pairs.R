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

    EUR_AUD <- create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "ask",
      time_frame = time_frame,
      asset = "EUR_AUD",
      keep_bid_to_ask = TRUE
    )

    EUR_NZD <- create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "ask",
      time_frame = time_frame,
      asset = "EUR_NZD",
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

    XAG_EUR <- create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "ask",
      time_frame = time_frame,
      asset = "XAG_EUR",
      keep_bid_to_ask = TRUE
    )

    XAU_EUR <- create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "ask",
      time_frame = time_frame,
      asset = "XAU_EUR",
      keep_bid_to_ask = TRUE
    )

    XAG_AUD <- create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "ask",
      time_frame = time_frame,
      asset = "XAG_AUD",
      keep_bid_to_ask = TRUE
    )

    XAU_AUD <- create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "ask",
      time_frame = time_frame,
      asset = "XAU_AUD",
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

    USD_CHF <- create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "ask",
      time_frame = time_frame,
      asset = "USD_CHF",
      keep_bid_to_ask = TRUE
    )

    XAG_CHF <- create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "ask",
      time_frame = time_frame,
      asset = "XAG_CHF",
      keep_bid_to_ask = TRUE
    )

    XAU_CHF <- create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "ask",
      time_frame = time_frame,
      asset = "XAU_CHF",
      keep_bid_to_ask = TRUE
    )

    AUD_USD_NZD_USD <-
      AUD_USD %>%
      bind_rows(NZD_USD) %>%
      bind_rows(XAG_USD) %>%
      bind_rows(XAU_USD) %>%
      bind_rows(XCU_USD) %>%
      bind_rows(NZD_CHF) %>%
      bind_rows(USD_CHF) %>%
      bind_rows(EUR_AUD) %>%
      bind_rows(EUR_NZD)  %>%
      bind_rows(XAG_EUR) %>%
      bind_rows(XAU_EUR)  %>%
      bind_rows(XAG_AUD) %>%
      bind_rows(XAU_AUD) %>%
      bind_rows(XAG_CHF) %>%
      bind_rows(XAU_CHF)

    rm(AUD_USD, NZD_USD, XAG_USD, XCU_USD, XAU_USD, NZD_CHF, USD_CHF, EUR_AUD, EUR_NZD, XAG_EUR, XAU_EUR, XAG_AUD,
       XAU_AUD, XAG_CHF, XAU_CHF)
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

    EUR_AUD <- create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "bid",
      time_frame = time_frame,
      asset = "EUR_AUD",
      keep_bid_to_ask = TRUE
    )

    EUR_NZD <- create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "bid",
      time_frame = time_frame,
      asset = "EUR_NZD",
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

    XAG_EUR <- create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "bid",
      time_frame = time_frame,
      asset = "XAG_EUR",
      keep_bid_to_ask = TRUE
    )

    XAU_EUR <- create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "bid",
      time_frame = time_frame,
      asset = "XAU_EUR",
      keep_bid_to_ask = TRUE
    )

    XAG_AUD <- create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "bid",
      time_frame = time_frame,
      asset = "XAG_AUD",
      keep_bid_to_ask = TRUE
    )

    XAU_AUD <- create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "bid",
      time_frame = time_frame,
      asset = "XAU_AUD",
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

    USD_CHF <- create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "bid",
      time_frame = time_frame,
      asset = "USD_CHF",
      keep_bid_to_ask = TRUE
    )

    XAG_CHF <- create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "bid",
      time_frame = time_frame,
      asset = "XAG_CHF",
      keep_bid_to_ask = TRUE
    )

    XAU_CHF <- create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "bid",
      time_frame = time_frame,
      asset = "XAU_CHF",
      keep_bid_to_ask = TRUE
    )

    AUD_USD_NZD_USD_short <-
      AUD_USD %>%
      bind_rows(NZD_USD) %>%
      bind_rows(XAG_USD) %>%
      bind_rows(XAU_USD) %>%
      bind_rows(XCU_USD) %>%
      bind_rows(NZD_CHF) %>%
      bind_rows(USD_CHF)  %>%
      bind_rows(EUR_AUD) %>%
      bind_rows(EUR_NZD) %>%
      bind_rows(XAG_EUR) %>%
      bind_rows(XAU_EUR) %>%
      bind_rows(XAG_AUD) %>%
      bind_rows(XAU_AUD) %>%
      bind_rows(XAG_CHF) %>%
      bind_rows(XAU_CHF)

    rm(AUD_USD, NZD_USD, XAG_USD, XCU_USD, XAU_USD, NZD_CHF, USD_CHF, EUR_AUD, EUR_NZD, XAG_EUR, XAU_EUR,
       XAG_AUD, XAU_AUD, XAG_CHF, XAU_CHF)
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
    sd_fac_NZD_CHF_trade = 1,
    sd_fac_XAG_USD_trade = 1,
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
        dependant_var_nzd_chf = log(lead(NZD_CHF, lm_period)/NZD_CHF),
        dependant_var_xag_usd = log(lead(XAG_USD, lm_period)/XAG_USD)
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

    #------------------------------------------------------------------XAG_USD

    lm_formula_XAG_USD <- create_lm_formula(dependant = "dependant_var_xag_usd", independant = lm_vars1)
    lm_model_XAG_USD <- lm(formula = lm_formula_XAG_USD, data = training_data)
    lm_formula_XAG_USD_quant <- create_lm_formula(dependant = "dependant_var_xag_usd", independant = lm_quant_vars)
    lm_model_XAG_USD_quant <- lm(formula = lm_formula_XAG_USD_quant, data = training_data)
    summary(lm_model_XAG_USD)

    predicted_train_XAG_USD <- predict.lm(lm_model_XAG_USD, newdata = training_data)
    mean_pred_XAG_USD <- mean(predicted_train_XAG_USD, na.rm = T)
    sd_pred_XAG_USD <- sd(predicted_train_XAG_USD, na.rm = T)
    predicted_test_XAG_USD <- predict.lm(lm_model_XAG_USD, newdata = testing_data) %>% as.numeric()
    mean_pred_test_XAG_USD <- mean(predicted_test_XAG_USD, na.rm = T)
    sd_pred_test_XAG_USD <- sd(predicted_test_XAG_USD, na.rm = T)

    predicted_train_XAG_USD_quant <- predict.lm(lm_model_XAG_USD_quant, newdata = training_data)
    mean_pred_XAG_USD_quant <- mean(predicted_train_XAG_USD_quant, na.rm = T)
    sd_pred_XAG_USD_quant <- sd(predicted_train_XAG_USD_quant, na.rm = T)
    predicted_test_XAG_USD_quant <- predict.lm(lm_model_XAG_USD_quant, newdata = testing_data) %>% as.numeric()
    mean_pred_test_XAG_USD_quant <- mean(predicted_test_XAG_USD_quant, na.rm = T)
    sd_pred_test_XAG_USD_quant <- sd(predicted_test_XAG_USD_quant, na.rm = T)

    tagged_trades_XAG_USD <-
      testing_data %>%
      mutate(
        lm_pred_XAG_USD = predicted_test_XAG_USD,
        lm_pred_XAG_USD_quant = predicted_test_XAG_USD_quant
      ) %>%
      mutate(
        trade_col =
          case_when(
            lm_pred_XAG_USD >= mean_pred_XAG_USD + sd_fac_XAG_USD_trade*sd_pred_XAG_USD &
              trade_direction == "Short" ~ trade_direction,
            lm_pred_XAG_USD <= mean_pred_XAG_USD - sd_fac_XAG_USD_trade*sd_pred_XAG_USD &
              trade_direction == "Long" ~ trade_direction
            # lm_pred_XAG_USD <= mean_pred_XAG_USD - sd_fac_XAG_USD_trade*sd_pred_XAG_USD &
            #   trade_direction == "Short" ~ trade_direction
          )
      ) %>%
      filter(!is.na(trade_col)) %>%
      dplyr::select(Date, trade_col) %>%
      mutate(
        Asset = "XAG_USD"
      ) %>%
      mutate(
        stop_factor = stop_factor,
        profit_factor = profit_factor
      )

    max_trades_XAG_USD <-
      tagged_trades_XAG_USD %>%
      pull(Date) %>%
      max(na.rm = T) %>%
      as.character()

    message(glue::glue("Max Date in Tagged Data XAG_USD: {max_trades_XAG_USD}"))

    #------------------------------------------------------------------NZD_CHF

    lm_formula_NZD_CHF <- create_lm_formula(dependant = "dependant_var_nzd_chf", independant = lm_vars1)
    lm_model_NZD_CHF <- lm(formula = lm_formula_NZD_CHF, data = training_data)
    lm_formula_NZD_CHF_quant <- create_lm_formula(dependant = "dependant_var_nzd_chf", independant = lm_quant_vars)
    lm_model_NZD_CHF_quant <- lm(formula = lm_formula_NZD_CHF_quant, data = training_data)
    summary(lm_model_NZD_CHF)

    predicted_train_NZD_CHF <- predict.lm(lm_model_NZD_CHF, newdata = training_data)
    mean_pred_NZD_CHF <- mean(predicted_train_NZD_CHF, na.rm = T)
    sd_pred_NZD_CHF <- sd(predicted_train_NZD_CHF, na.rm = T)
    predicted_test_NZD_CHF <- predict.lm(lm_model_NZD_CHF, newdata = testing_data) %>% as.numeric()
    mean_pred_test_NZD_CHF <- mean(predicted_test_NZD_CHF, na.rm = T)
    sd_pred_test_NZD_CHF <- sd(predicted_test_NZD_CHF, na.rm = T)

    predicted_train_NZD_CHF_quant <- predict.lm(lm_model_NZD_CHF_quant, newdata = training_data)
    mean_pred_NZD_CHF_quant <- mean(predicted_train_NZD_CHF_quant, na.rm = T)
    sd_pred_NZD_CHF_quant <- sd(predicted_train_NZD_CHF_quant, na.rm = T)
    predicted_test_NZD_CHF_quant <- predict.lm(lm_model_NZD_CHF_quant, newdata = testing_data) %>% as.numeric()
    mean_pred_test_NZD_CHF_quant <- mean(predicted_test_NZD_CHF_quant, na.rm = T)
    sd_pred_test_NZD_CHF_quant <- sd(predicted_test_NZD_CHF_quant, na.rm = T)

    tagged_trades_NZD_CHF <-
      testing_data %>%
      mutate(
        lm_pred_NZD_CHF = predicted_test_NZD_CHF,
        lm_pred_NZD_CHF_quant = predicted_test_NZD_CHF_quant
      ) %>%
      mutate(
        trade_col =
          case_when(
            # lm_pred_NZD_CHF >= mean_pred_NZD_CHF + sd_fac_NZD_CHF_trade*sd_pred_NZD_CHF &
            #   trade_direction == "Short" ~ trade_direction,
            # lm_pred_NZD_CHF <= mean_pred_NZD_CHF - sd_fac_NZD_CHF_trade*sd_pred_NZD_CHF &
            #   trade_direction == "Long" ~ trade_direction,

            lm_pred_NZD_CHF <= mean_pred_NZD_CHF - sd_fac_NZD_CHF_trade*sd_pred_NZD_CHF &
              trade_direction == "Long" ~ trade_direction,
            lm_pred_NZD_CHF >= mean_pred_NZD_CHF + sd_fac_NZD_CHF_trade*sd_pred_NZD_CHF &
              trade_direction == "Short" ~ trade_direction
          )
      ) %>%
      filter(!is.na(trade_col)) %>%
      dplyr::select(Date, trade_col) %>%
      mutate(
        Asset = "NZD_CHF"
      ) %>%
      mutate(
        stop_factor = stop_factor,
        profit_factor = profit_factor
      )

    max_trades_NZD_CHF <-
      tagged_trades_NZD_CHF %>%
      pull(Date) %>%
      max(na.rm = T) %>%
      as.character()

    message(glue::glue("Max Date in Tagged Data NZD_CHF: {max_trades_NZD_CHF}"))

    return(list(tagged_trades_NZD_USD %>% filter(Asset %in% assets_to_return),
                tagged_trades_AUD_USD %>% filter(Asset %in% assets_to_return),
                tagged_trades_XCU_USD %>% filter(Asset %in% assets_to_return),
                tagged_trades_NZD_CHF %>% filter(Asset %in% assets_to_return),
                tagged_trades_XAG_USD %>% filter(Asset %in% assets_to_return)
                )
           )

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
    end_date = today() %>% as.character(),
    time_frame = "M15"
) {

  EUR_USD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "EUR_USD",
    keep_bid_to_ask = TRUE
  )

  GBP_USD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "GBP_USD",
    keep_bid_to_ask = TRUE
  )

  EUR_GBP <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "EUR_GBP",
    keep_bid_to_ask = TRUE
  )

  EUR_JPY <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "EUR_JPY",
    keep_bid_to_ask = TRUE
  )

  GBP_JPY <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "GBP_JPY",
    keep_bid_to_ask = TRUE
  )

  USD_JPY <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
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
    time_frame = time_frame,
    asset = "EUR_USD",
    keep_bid_to_ask = TRUE
  )

  GBP_USD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "GBP_USD",
    keep_bid_to_ask = TRUE
  )

  EUR_GBP <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "EUR_GBP",
    keep_bid_to_ask = TRUE
  )

  EUR_JPY <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "EUR_JPY",
    keep_bid_to_ask = TRUE
  )

  GBP_JPY <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "GBP_JPY",
    keep_bid_to_ask = TRUE
  )

  USD_JPY <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
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
    end_date = today() %>% as.character(),
    time_frame = "M15"
    ) {

  SPX <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "SPX500_USD",
    keep_bid_to_ask = TRUE
  )

  US2000 <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "US2000_USD",
    keep_bid_to_ask = TRUE
  )

  EUR50 <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "EU50_EUR",
    keep_bid_to_ask = TRUE
  )

  AU200_AUD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "AU200_AUD",
    keep_bid_to_ask = TRUE
  )

  SG30_SGD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "SG30_SGD",
    keep_bid_to_ask = TRUE
  )

  UK100_GBP <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "UK100_GBP",
    keep_bid_to_ask = TRUE
  )

  JP225Y_JPY <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "JP225Y_JPY",
    keep_bid_to_ask = TRUE
  )

  FR40_EUR <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "FR40_EUR",
    keep_bid_to_ask = TRUE
  )

  CH20_CHF <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "CH20_CHF",
    keep_bid_to_ask = TRUE
  )

  USB10Y_USD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "USB10Y_USD",
    keep_bid_to_ask = TRUE
  )

  USB02Y_USD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "USB02Y_USD",
    keep_bid_to_ask = TRUE
  )

  UK10YB_GBP <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "UK10YB_GBP",
    keep_bid_to_ask = TRUE
  )


  HK33_HKD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "HK33_HKD",
    keep_bid_to_ask = TRUE
  )

  EUR_USD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "EUR_USD",
    keep_bid_to_ask = TRUE
  )

  GBP_USD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "GBP_USD",
    keep_bid_to_ask = TRUE
  )


  XAG <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "XAG_USD",
    keep_bid_to_ask = TRUE
  )

  XAU <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "XAU_USD",
    keep_bid_to_ask = TRUE
  )

  XAG_SPX_US2000_USD <-
    SPX %>%
    bind_rows(US2000) %>%
    bind_rows(EUR50) %>%
    bind_rows(SG30_SGD) %>%
    bind_rows(AU200_AUD) %>%
    bind_rows(XAG)%>%
    bind_rows(XAU) %>%
    bind_rows(UK100_GBP) %>%
    bind_rows(JP225Y_JPY) %>%
    bind_rows(FR40_EUR) %>%
    bind_rows(CH20_CHF) %>%
    bind_rows(USB10Y_USD) %>%
    bind_rows(USB02Y_USD) %>%
    bind_rows(UK10YB_GBP) %>%
    bind_rows(HK33_HKD) %>%
    bind_rows(EUR_USD) %>%
    bind_rows(GBP_USD)
  rm(SPX, US2000,EUR50, AU200_AUD, SG30_SGD, XAG, XAU, UK100_GBP, JP225Y_JPY, FR40_EUR, CH20_CHF,
     USB10Y_USD, USB02Y_USD, EUR_USD, GBP_USD)
  gc()

  SPX <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "SPX500_USD",
    keep_bid_to_ask = TRUE
  )

  US2000 <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "US2000_USD",
    keep_bid_to_ask = TRUE
  )

  EUR50 <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "EU50_EUR",
    keep_bid_to_ask = TRUE
  )

  AU200_AUD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "AU200_AUD",
    keep_bid_to_ask = TRUE
  )

  SG30_SGD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "SG30_SGD",
    keep_bid_to_ask = TRUE
  )

  XAG <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "XAG_USD",
    keep_bid_to_ask = TRUE
  )

  XAU <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "XAU_USD",
    keep_bid_to_ask = TRUE
  )

  UK100_GBP <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "UK100_GBP",
    keep_bid_to_ask = TRUE
  )

  JP225Y_JPY <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "JP225Y_JPY",
    keep_bid_to_ask = TRUE
  )

  FR40_EUR <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "FR40_EUR",
    keep_bid_to_ask = TRUE
  )

  CH20_CHF <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "CH20_CHF",
    keep_bid_to_ask = TRUE
  )

  USB10Y_USD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "USB10Y_USD",
    keep_bid_to_ask = TRUE
  )

  USB02Y_USD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "USB02Y_USD",
    keep_bid_to_ask = TRUE
  )

  HK33_HKD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "HK33_HKD",
    keep_bid_to_ask = TRUE
  )

  EUR_USD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "EUR_USD",
    keep_bid_to_ask = TRUE
  )

  GBP_USD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "GBP_USD",
    keep_bid_to_ask = TRUE
  )


  XAG_SPX_US2000_USD_short <-
    SPX %>%
    bind_rows(US2000) %>%
    bind_rows(EUR50) %>%
    bind_rows(SG30_SGD) %>%
    bind_rows(AU200_AUD) %>%
    bind_rows(XAG)%>%
    bind_rows(XAU) %>%
    bind_rows(UK100_GBP) %>%
    bind_rows(JP225Y_JPY) %>%
    bind_rows(FR40_EUR) %>%
    bind_rows(CH20_CHF) %>%
    bind_rows(USB10Y_USD) %>%
    bind_rows(HK33_HKD) %>%
    bind_rows(EUR_USD) %>%
    bind_rows(GBP_USD)
  rm(SPX, US2000,EUR50, AU200_AUD, SG30_SGD, XAG, XAU, UK100_GBP, JP225Y_JPY, FR40_EUR, CH20_CHF,
     GBP_USD)
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
    risk_dollar_value = 10,
    return_trade_ts = FALSE
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

  if(return_trade_ts == TRUE) {
    return(long_bayes_loop_analysis)
  } else {
    return(list(analysis_data, analysis_data_asset))
  }

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
get_AUD_USD_NZD_Specific_Trades_NN <-
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
    sd_fac_NZD_CHF_trade = 1,
    sd_fac_XAG_USD_trade = 1,
    trade_direction = "Long",
    stop_factor = 5,
    profit_factor = 10,
    assets_to_return = c("AUD_USD", "NZD_USD", "NZD_CHF", "XCU_USD", "XAG_USD", "XAU_USD"),
    hidden_layers = c(10, 10),
    new_models = FALSE,
    threshold = 0.1
  ) {

    aus_macro_data <-
      get_AUS_Indicators(raw_macro_data,
                         lag_days = lag_days) %>%
      janitor::clean_names()
    nzd_macro_data <-
      get_NZD_Indicators(raw_macro_data,
                         lag_days = lag_days) %>%
      janitor::clean_names()
    usd_macro_data <-
      get_USD_Indicators(raw_macro_data,
                         lag_days = lag_days) %>%
      janitor::clean_names()
    cny_macro_data <-
      get_CNY_Indicators(raw_macro_data,
                         lag_days = lag_days) %>%
      janitor::clean_names()
    eur_macro_data <-
      get_EUR_Indicators(raw_macro_data,
                         lag_days = lag_days) %>%
      janitor::clean_names()

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
        dependant_var_aud_usd = lead(AUD_USD, lm_period) - AUD_USD,
        dependant_var_nzd_usd = lead(NZD_USD, lm_period) - NZD_USD,
        dependant_var_xcu_usd = lead(XCU_USD, lm_period) - XCU_USD,
        dependant_var_nzd_chf = lead(NZD_CHF, lm_period) - NZD_CHF,
        dependant_var_xag_usd = lead(XAG_USD, lm_period) - XAG_USD
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

    NN_vars <- c(lm_vars1, "LM_pred", "LM_quant_LM")
    NN_form <-  create_lm_formula(dependant = "dependant_var_aud_usd", independant = NN_vars)
    if(new_models == TRUE) {
      NN_model <- neuralnet::neuralnet(formula = NN_form,
                                       hidden = hidden_layers,
                                       data = training_data %>%
                                         mutate(LM_pred = predicted_train_AUD_USD,
                                                LM_quant_LM = predicted_train_AUD_USD_quant),
                                       err.fct = "sse",
                                       linear.output = TRUE,
                                       lifesign = 'full',
                                       rep = 1,
                                       algorithm = "rprop+",
                                       stepmax = 50000,
                                       threshold = 0.0005)
      saveRDS(object = NN_model,
              file = "C:/Users/Nikhil Chandra/Documents/trade_data/asset_specific_NNs/AUD_USD_NN.rds")
    } else {
      NN_model <- readRDS("C:/Users/Nikhil Chandra/Documents/trade_data/asset_specific_NNs/AUD_USD_NN.rds")
    }

    predicted_train_AUD_USD_NN <- predict(NN_model,
                                          newdata = training_data %>%
                                            mutate(LM_pred = predicted_train_AUD_USD,
                                                   LM_quant_LM = predicted_train_AUD_USD_quant) )
    mean_pred_AUD_USD_NN <- mean(predicted_train_AUD_USD_NN, na.rm = T)
    sd_pred_AUD_USD_NN <- sd(predicted_train_AUD_USD_NN, na.rm = T)
    predicted_test_AUD_USD_NN <- predict(NN_model,
                                         newdata = testing_data %>%
                                           mutate(LM_pred = predicted_test_AUD_USD,
                                                  LM_quant_LM = predicted_test_AUD_USD_quant) ) %>% as.numeric()
    mean_pred_test_AUD_USD_NN <- mean(predicted_test_AUD_USD_NN, na.rm = T)
    sd_pred_test_AUD_USD_NN <- sd(predicted_test_AUD_USD_NN, na.rm = T)

    tagged_trades_AUD_USD <-
      testing_data %>%
      mutate(
        lm_pred_AUD_USD = predicted_test_AUD_USD,
        lm_pred_AUD_USD_quant = predicted_test_AUD_USD_quant,
        lm_pred_AUD_USD_NN = predicted_test_AUD_USD_NN
      ) %>%
      mutate(
        trade_col =
          case_when(
            # lm_pred_AUD_USD_NN <= mean_pred_AUD_USD_NN - sd_fac_AUD_USD_trade*sd_pred_AUD_USD_NN &
            #   trade_direction == "Short" ~ trade_direction,
            # lm_pred_AUD_USD_NN >= mean_pred_AUD_USD_NN + sd_fac_AUD_USD_trade*sd_pred_AUD_USD_NN &
            #   trade_direction == "Long" ~ trade_direction

            # lm_pred_AUD_USD_NN < 0 & trade_direction == "Short" ~ trade_direction,
            # lm_pred_AUD_USD_NN > 0 &  trade_direction == "Long" ~ trade_direction

            lm_pred_AUD_USD_NN < mean_pred_AUD_USD_NN - 0*sd_pred_AUD_USD_NN &
              lm_pred_AUD_USD_NN >= mean_pred_AUD_USD_NN + sd_fac_AUD_USD_trade*sd_pred_AUD_USD_NN &
              trade_direction == "Short" ~ trade_direction,
            lm_pred_AUD_USD_NN > mean_pred_AUD_USD_NN + 0*sd_pred_AUD_USD_NN &
              lm_pred_AUD_USD_NN <= mean_pred_AUD_USD_NN + sd_fac_AUD_USD_trade*sd_pred_AUD_USD_NN &
              trade_direction == "Long" ~ trade_direction
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

    NN_vars <- c(lm_vars1, "LM_pred", "LM_quant_LM")
    NN_form <-  create_lm_formula(dependant = "dependant_var_nzd_usd", independant = NN_vars)
    if(new_models == TRUE) {
      NN_model <- neuralnet::neuralnet(formula = NN_form ,
                                       hidden = hidden_layers,
                                       data = training_data %>%
                                         mutate(LM_pred = predicted_train_NZD_USD,
                                                LM_quant_LM = predicted_train_NZD_USD_quant),
                                       err.fct = "sse",
                                       linear.output = TRUE,
                                       lifesign = 'full',
                                       rep = 1,
                                       algorithm = "rprop+",
                                       stepmax = 20000,
                                       threshold = 0.0005)

      saveRDS(object = NN_model,
              file = "C:/Users/Nikhil Chandra/Documents/trade_data/asset_specific_NNs/NZD_USD_NN.rds")
    } else {
      NN_model <- readRDS("C:/Users/Nikhil Chandra/Documents/trade_data/asset_specific_NNs/NZD_USD_NN.rds")
    }

    predicted_train_NZD_USD_NN <- predict(NN_model,
                                          newdata = training_data %>%
                                            mutate(LM_pred = predicted_train_NZD_USD,
                                                   LM_quant_LM = predicted_train_NZD_USD_quant)
                                          )
    mean_pred_NZD_USD_NN <- mean(predicted_train_NZD_USD_NN, na.rm = T)
    sd_pred_NZD_USD_NN <- sd(predicted_train_NZD_USD_NN, na.rm = T)
    predicted_test_NZD_USD_NN <- predict(NN_model,
                                         newdata = testing_data %>%
                                           mutate(LM_pred = predicted_test_NZD_USD,
                                                  LM_quant_LM = predicted_test_NZD_USD_quant)) %>% as.numeric()
    mean_pred_test_NZD_USD_NN <- mean(predicted_test_NZD_USD_NN, na.rm = T)
    sd_pred_test_NZD_USD_NN <- sd(predicted_test_NZD_USD_NN, na.rm = T)

    tagged_trades_NZD_USD <-
      testing_data %>%
      mutate(
        lm_pred_NZD_USD = predicted_test_NZD_USD,
        lm_pred_NZD_USD_quant = predicted_test_NZD_USD_quant,
        lm_pred_NZD_USD_NN = predicted_test_NZD_USD_NN
      ) %>%
      mutate(
        trade_col =
          case_when(
            # lm_pred_NZD_USD_NN <= mean_pred_NZD_USD_NN - sd_fac_NZD_USD_trade*sd_pred_NZD_USD_NN &
            #   trade_direction == "Short" ~ trade_direction,
            # lm_pred_NZD_USD_NN >= mean_pred_NZD_USD_NN + sd_fac_NZD_USD_trade*sd_pred_NZD_USD_NN &
            #   trade_direction == "Long" ~ trade_direction

            # lm_pred_NZD_USD_NN >0 & trade_direction == "Long" ~ trade_direction,
            # lm_pred_NZD_USD_NN <0 & trade_direction == "Short" ~ trade_direction

            lm_pred_NZD_USD_NN <= mean_pred_NZD_USD_NN - 0*sd_pred_NZD_USD_NN &
              lm_pred_NZD_USD_NN >= mean_pred_NZD_USD_NN - sd_fac_NZD_USD_trade*sd_pred_NZD_USD_NN &
              trade_direction == "Short" ~ trade_direction,
            lm_pred_NZD_USD_NN > mean_pred_NZD_USD_NN + 0*sd_pred_NZD_USD_NN &
              lm_pred_NZD_USD_NN <= mean_pred_NZD_USD_NN + sd_fac_NZD_USD_trade*sd_pred_NZD_USD_NN &
              trade_direction == "Long" ~ trade_direction
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

    NN_vars <- c(lm_vars1, "LM_pred", "LM_quant_LM")
    NN_form <-  create_lm_formula(dependant = "dependant_var_xcu_usd", independant = NN_vars)

    if(new_models == TRUE) {
      NN_model <- neuralnet::neuralnet(formula = NN_form,
                                       hidden = hidden_layers,
                                       data = training_data %>%
                                         mutate(LM_pred = predicted_train_XCU_USD,
                                                LM_quant_LM = predicted_train_XCU_USD_quant),
                                       err.fct = "sse",
                                       linear.output = TRUE,
                                       lifesign = 'full',
                                       rep = 1,
                                       algorithm = "rprop+",
                                       stepmax = 50000,
                                       threshold = 0.005)

      saveRDS(object = NN_model,
              file = "C:/Users/Nikhil Chandra/Documents/trade_data/asset_specific_NNs/XCU_USD_NN.rds")
    } else {
      NN_model <- readRDS("C:/Users/Nikhil Chandra/Documents/trade_data/asset_specific_NNs/XCU_USD_NN.rds")
    }

    predicted_train_XCU_USD_NN <- predict(NN_model,
                                          newdata = training_data %>%
                                            mutate(LM_pred = predicted_train_XCU_USD,
                                                   LM_quant_LM = predicted_train_XCU_USD_quant)
                                          )
    mean_pred_XCU_USD_NN <- mean(predicted_train_XCU_USD_NN, na.rm = T)
    sd_pred_XCU_USD_NN <- sd(predicted_train_XCU_USD_NN, na.rm = T)
    predicted_test_XCU_USD_NN <- predict(NN_model,
                                         newdata = testing_data %>%
                                           mutate(LM_pred = predicted_test_XCU_USD,
                                                  LM_quant_LM = predicted_test_XCU_USD_quant) ) %>% as.numeric()
    mean_pred_test_XCU_USD_NN <- mean(predicted_test_XCU_USD_NN, na.rm = T)
    sd_pred_test_XCU_USD_NN <- sd(predicted_test_XCU_USD_NN, na.rm = T)

    tagged_trades_XCU_USD <-
      testing_data %>%
      mutate(
        lm_pred_XCU_USD = predicted_test_XCU_USD,
        lm_pred_XCU_USD_quant = predicted_test_XCU_USD_quant,
        lm_pred_XCU_USD_NN = predicted_test_XCU_USD_NN
      ) %>%
      mutate(
        trade_col =
          case_when(
            # lm_pred_XCU_USD_NN <= mean_pred_XCU_USD_NN - sd_fac_XCU_USD_trade*sd_pred_XCU_USD_NN &
            #   trade_direction == "Short" ~ trade_direction,
            # lm_pred_XCU_USD_NN >= mean_pred_XCU_USD_NN + sd_fac_XCU_USD_trade*sd_pred_XCU_USD_NN &
            #   trade_direction == "Long" ~ trade_direction

            # lm_pred_XCU_USD_NN >0 & lm_pred_XCU_USD > 0 & trade_direction == "Long" ~ trade_direction,
            # lm_pred_XCU_USD_NN <0 & lm_pred_XCU_USD < 0 & trade_direction == "Short" ~ trade_direction

            lm_pred_XCU_USD_NN < mean_pred_XCU_USD_NN - 0*sd_pred_XCU_USD_NN &
              lm_pred_XCU_USD_NN >= mean_pred_XCU_USD_NN - sd_fac_XCU_USD_trade*sd_pred_XCU_USD_NN &
              trade_direction == "Short" ~ trade_direction,
            lm_pred_XCU_USD_NN > mean_pred_XCU_USD_NN + 0*sd_pred_XCU_USD_NN &
              lm_pred_XCU_USD_NN <= mean_pred_XCU_USD_NN + sd_fac_XCU_USD_trade*sd_pred_XCU_USD_NN &
              trade_direction == "Long" ~ trade_direction
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

    #------------------------------------------------------------------XAG_USD

    lm_formula_XAG_USD <- create_lm_formula(dependant = "dependant_var_xag_usd", independant = lm_vars1)
    lm_model_XAG_USD <- lm(formula = lm_formula_XAG_USD, data = training_data)
    lm_formula_XAG_USD_quant <- create_lm_formula(dependant = "dependant_var_xag_usd", independant = lm_quant_vars)
    lm_model_XAG_USD_quant <- lm(formula = lm_formula_XAG_USD_quant, data = training_data)
    summary(lm_model_XAG_USD)

    predicted_train_XAG_USD <- predict.lm(lm_model_XAG_USD, newdata = training_data)
    mean_pred_XAG_USD <- mean(predicted_train_XAG_USD, na.rm = T)
    sd_pred_XAG_USD <- sd(predicted_train_XAG_USD, na.rm = T)
    predicted_test_XAG_USD <- predict.lm(lm_model_XAG_USD, newdata = testing_data) %>% as.numeric()
    mean_pred_test_XAG_USD <- mean(predicted_test_XAG_USD, na.rm = T)
    sd_pred_test_XAG_USD <- sd(predicted_test_XAG_USD, na.rm = T)

    predicted_train_XAG_USD_quant <- predict.lm(lm_model_XAG_USD_quant, newdata = training_data)
    mean_pred_XAG_USD_quant <- mean(predicted_train_XAG_USD_quant, na.rm = T)
    sd_pred_XAG_USD_quant <- sd(predicted_train_XAG_USD_quant, na.rm = T)
    predicted_test_XAG_USD_quant <- predict.lm(lm_model_XAG_USD_quant, newdata = testing_data) %>% as.numeric()
    mean_pred_test_XAG_USD_quant <- mean(predicted_test_XAG_USD_quant, na.rm = T)
    sd_pred_test_XAG_USD_quant <- sd(predicted_test_XAG_USD_quant, na.rm = T)

    if(new_models == TRUE) {
      NN_model <- neuralnet::neuralnet(formula = lm_formula_XAG_USD,
                                       hidden = c(20, 20),
                                       data = training_data %>%
                                         mutate(LM_pred = predicted_train_XAG_USD,
                                                LM_quant_LM = predicted_train_XAG_USD_quant),
                                       err.fct = "sse",
                                       linear.output = TRUE,
                                       lifesign = 'full',
                                       rep = 1,
                                       algorithm = "rprop+",
                                       stepmax = 20000,
                                       threshold = 3)

      saveRDS(object = NN_model,
              file = "C:/Users/Nikhil Chandra/Documents/trade_data/asset_specific_NNs/XAG_USD_NN.rds")
    } else {
      NN_model <- readRDS("C:/Users/Nikhil Chandra/Documents/trade_data/asset_specific_NNs/XAG_USD_NN.rds")
    }

    predicted_train_XAG_USD_NN <- predict(NN_model,
                                          newdata = training_data %>%
                                            mutate(LM_pred = predicted_train_XAG_USD,
                                                   LM_quant_LM = predicted_train_XAG_USD_quant)
                                          )
    mean_pred_XAG_USD_NN <- mean(predicted_train_XAG_USD_NN, na.rm = T)
    sd_pred_XAG_USD_NN <- sd(predicted_train_XAG_USD_NN, na.rm = T)
    predicted_test_XAG_USD_NN <- predict(NN_model,
                                         newdata = testing_data %>%
                                           mutate(LM_pred = predicted_test_XAG_USD,
                                                  LM_quant_LM = predicted_test_XAG_USD_quant)
                                         ) %>% as.numeric()
    mean_pred_test_XAG_USD_NN <- mean(predicted_test_XAG_USD_NN, na.rm = T)
    sd_pred_test_XAG_USD_NN <- sd(predicted_test_XAG_USD_NN, na.rm = T)

    tagged_trades_XAG_USD <-
      testing_data %>%
      mutate(
        lm_pred_XAG_USD = predicted_test_XAG_USD,
        lm_pred_XAG_USD_quant = predicted_test_XAG_USD_quant,
        lm_pred_XAG_USD_NN = predicted_test_XAG_USD_NN
      ) %>%
      mutate(
        trade_col =
          case_when(
            # lm_pred_XAG_USD_NN <= mean_pred_XAG_USD_NN - sd_fac_XAG_USD_trade*sd_pred_XAG_USD_NN &
            #   trade_direction == "Short" ~ trade_direction,
            # lm_pred_XAG_USD_NN >= mean_pred_XAG_USD_NN + sd_fac_XAG_USD_trade*sd_pred_XAG_USD_NN &
            #   trade_direction == "Long" ~ trade_direction

            # lm_pred_XAG_USD_NN >0 & lm_pred_XAG_USD > 0 & trade_direction == "Long" ~ trade_direction,
            # lm_pred_XAG_USD_NN <0 & lm_pred_XAG_USD < 0 & trade_direction == "Short" ~ trade_direction

            lm_pred_XAG_USD_NN < mean_pred_XAG_USD_NN - 0*sd_pred_XAG_USD_NN &
              lm_pred_XAG_USD_NN >= mean_pred_XAG_USD_NN - sd_fac_XAG_USD_trade*sd_pred_XAG_USD_NN &
              trade_direction == "Short" ~ trade_direction,
            lm_pred_XAG_USD_NN > mean_pred_XAG_USD_NN + 0*sd_pred_XAG_USD_NN &
              lm_pred_XAG_USD_NN <= mean_pred_XAG_USD_NN + sd_fac_XAG_USD_trade*sd_pred_XAG_USD_NN &
              trade_direction == "Long" ~ trade_direction
          )
      ) %>%
      filter(!is.na(trade_col)) %>%
      dplyr::select(Date, trade_col) %>%
      mutate(
        Asset = "XAG_USD"
      ) %>%
      mutate(
        stop_factor = stop_factor,
        profit_factor = profit_factor
      )

    max_trades_XAG_USD <-
      tagged_trades_XAG_USD %>%
      pull(Date) %>%
      max(na.rm = T) %>%
      as.character()

    message(glue::glue("Max Date in Tagged Data XAG_USD: {max_trades_XAG_USD}"))

    #------------------------------------------------------------------NZD_CHF

    lm_formula_NZD_CHF <- create_lm_formula(dependant = "dependant_var_nzd_chf", independant = lm_vars1)
    lm_model_NZD_CHF <- lm(formula = lm_formula_NZD_CHF, data = training_data)
    lm_formula_NZD_CHF_quant <- create_lm_formula(dependant = "dependant_var_nzd_chf", independant = lm_quant_vars)
    lm_model_NZD_CHF_quant <- lm(formula = lm_formula_NZD_CHF_quant, data = training_data)
    summary(lm_model_NZD_CHF)

    predicted_train_NZD_CHF <- predict.lm(lm_model_NZD_CHF, newdata = training_data)
    mean_pred_NZD_CHF <- mean(predicted_train_NZD_CHF, na.rm = T)
    sd_pred_NZD_CHF <- sd(predicted_train_NZD_CHF, na.rm = T)
    predicted_test_NZD_CHF <- predict.lm(lm_model_NZD_CHF, newdata = testing_data) %>% as.numeric()
    mean_pred_test_NZD_CHF <- mean(predicted_test_NZD_CHF, na.rm = T)
    sd_pred_test_NZD_CHF <- sd(predicted_test_NZD_CHF, na.rm = T)

    predicted_train_NZD_CHF_quant <- predict.lm(lm_model_NZD_CHF_quant, newdata = training_data)
    mean_pred_NZD_CHF_quant <- mean(predicted_train_NZD_CHF_quant, na.rm = T)
    sd_pred_NZD_CHF_quant <- sd(predicted_train_NZD_CHF_quant, na.rm = T)
    predicted_test_NZD_CHF_quant <- predict.lm(lm_model_NZD_CHF_quant, newdata = testing_data) %>% as.numeric()
    mean_pred_test_NZD_CHF_quant <- mean(predicted_test_NZD_CHF_quant, na.rm = T)
    sd_pred_test_NZD_CHF_quant <- sd(predicted_test_NZD_CHF_quant, na.rm = T)

    if(new_models == TRUE) {
      NN_model <- neuralnet::neuralnet(formula = lm_formula_NZD_CHF,
                                       hidden = hidden_layers,
                                       data = training_data %>%
                                         mutate(LM_pred = predicted_train_NZD_CHF,
                                                LM_quant_LM = predicted_train_NZD_CHF_quant),
                                       err.fct = "sse",
                                       linear.output = TRUE,
                                       lifesign = 'full',
                                       rep = 1,
                                       algorithm = "rprop+",
                                       stepmax = 20000,
                                       threshold = 0.001)

      saveRDS(object = NN_model,
              file = "C:/Users/Nikhil Chandra/Documents/trade_data/asset_specific_NNs/NZD_CHF_NN.rds")
    } else {
      NN_model <- readRDS("C:/Users/Nikhil Chandra/Documents/trade_data/asset_specific_NNs/NZD_CHF_NN.rds")
    }

    predicted_train_NZD_CHF_NN <- predict(NN_model,
                                          newdata = training_data %>%
                                            mutate(LM_pred = predicted_train_NZD_CHF,
                                                   LM_quant_LM = predicted_train_NZD_CHF_quant)
                                          )
    mean_pred_NZD_CHF_NN <- mean(predicted_train_NZD_CHF_NN, na.rm = T)
    sd_pred_NZD_CHF_NN <- sd(predicted_train_NZD_CHF_NN, na.rm = T)
    predicted_test_NZD_CHF_NN <- predict(NN_model,
                                         newdata = testing_data %>%
                                           mutate(LM_pred = predicted_test_NZD_CHF,
                                                  LM_quant_LM = predicted_test_NZD_CHF_quant)
                                         ) %>% as.numeric()
    mean_pred_test_NZD_CHF_NN <- mean(predicted_test_NZD_CHF_NN, na.rm = T)
    sd_pred_test_NZD_CHF_NN <- sd(predicted_test_NZD_CHF_NN, na.rm = T)

    tagged_trades_NZD_CHF <-
      testing_data %>%
      mutate(
        lm_pred_NZD_CHF = predicted_test_NZD_CHF,
        lm_pred_NZD_CHF_quant = predicted_test_NZD_CHF_quant,
        lm_pred_NZD_CHF_NN = predicted_test_NZD_CHF_NN
      ) %>%
      mutate(
        trade_col =
          case_when(
            # lm_pred_NZD_CHF_NN >= mean_pred_NZD_CHF_NN + sd_fac_NZD_CHF_trade*sd_pred_NZD_CHF_NN &
            #   trade_direction == "Short" ~ trade_direction,
            # lm_pred_NZD_CHF_NN <= mean_pred_NZD_CHF_NN - sd_fac_NZD_CHF_trade*sd_pred_NZD_CHF_NN &
            #   trade_direction == "Long" ~ trade_direction

            # lm_pred_NZD_CHF_NN >0 & lm_pred_NZD_CHF > 0 & trade_direction == "Long" ~ trade_direction,
            # lm_pred_NZD_CHF_NN <0 & lm_pred_NZD_CHF < 0 & trade_direction == "Short" ~ trade_direction

            lm_pred_NZD_CHF_NN > mean_pred_NZD_CHF_NN + 0*sd_pred_NZD_CHF_NN &
              lm_pred_NZD_CHF_NN <= mean_pred_NZD_CHF_NN + sd_fac_NZD_CHF_trade*sd_pred_NZD_CHF_NN &
              trade_direction == "Long" ~ trade_direction,
            lm_pred_NZD_CHF_NN < mean_pred_NZD_CHF_NN - 0*sd_pred_NZD_CHF_NN &
              lm_pred_NZD_CHF_NN >= mean_pred_NZD_CHF_NN - sd_fac_NZD_CHF_trade*sd_pred_NZD_CHF_NN &
              trade_direction == "Short" ~ trade_direction
          )
      ) %>%
      filter(!is.na(trade_col)) %>%
      dplyr::select(Date, trade_col) %>%
      mutate(
        Asset = "NZD_CHF"
      ) %>%
      mutate(
        stop_factor = stop_factor,
        profit_factor = profit_factor
      )

    max_trades_NZD_CHF <-
      tagged_trades_NZD_CHF %>%
      pull(Date) %>%
      max(na.rm = T) %>%
      as.character()

    message(glue::glue("Max Date in Tagged Data NZD_CHF: {max_trades_NZD_CHF}"))

    return(list(tagged_trades_NZD_USD %>% filter(Asset %in% assets_to_return),
                tagged_trades_AUD_USD %>% filter(Asset %in% assets_to_return),
                tagged_trades_XCU_USD %>% filter(Asset %in% assets_to_return),
                tagged_trades_NZD_CHF %>% filter(Asset %in% assets_to_return),
                tagged_trades_XAG_USD %>% filter(Asset %in% assets_to_return)
    )
    )

}



#' create_NN_AUD_USD_XCU_NZD_data
#'
#' @return
#' @export
#'
#' @examples
create_NN_AUD_USD_XCU_NZD_data <-
  function(AUD_USD_NZD_USD,
           raw_macro_data,
           actual_wins_losses = actual_wins_losses,
           lag_days = 1,
           stop_value_var = 15,
           profit_value_var = 20,
           use_PCA_vars = FALSE) {

    # assets_to_return <- dependant_var_name

    usd_macro_data <-
      get_USD_Indicators(raw_macro_data,
                         lag_days = lag_days,
                         first_difference = TRUE
      ) %>%
      janitor::clean_names()

    eur_macro_data <-
      get_EUR_Indicators(raw_macro_data,
                         lag_days = lag_days,
                         first_difference = TRUE
      ) %>%
      janitor::clean_names()

    usd_macro_vars <- names(usd_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    eur_macro_vars <- names(eur_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    all_macro_vars <- c( usd_macro_vars, eur_macro_vars)

    min_possible_date <-
      usd_macro_data %>%
      slice_min(date) %>%
      pull(date)

    min_possible_date2 <-
      eur_macro_data %>%
      slice_min(date)%>%
      pull(date)

    min_possible_date <- min(min_possible_date, min_possible_date2)

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

    copula_data_XAG_XAU <-
      estimating_dual_copula(
        asset_data_to_use = AUD_USD_NZD_USD,
        asset_to_use = c("XAG_USD", "XAU_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAG_USD, -XAG_USD_log1_price, -XAG_USD_quantiles_1, -XAG_USD_tangent_angle1)

    copula_data_XCU_XAU <-
      estimating_dual_copula(
        asset_data_to_use = AUD_USD_NZD_USD,
        asset_to_use = c("XCU_USD", "XAU_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XCU_USD, -XCU_USD_log1_price, -XCU_USD_quantiles_1, -XCU_USD_tangent_angle1) %>%
      dplyr::select(-XAU_USD, -XAU_USD_log2_price, -XAU_USD_quantiles_2, -XAU_USD_tangent_angle2)

    copula_data_XCU_XAG <-
      estimating_dual_copula(
        asset_data_to_use = AUD_USD_NZD_USD,
        asset_to_use = c("XCU_USD", "XAG_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XCU_USD, -XCU_USD_log1_price, -XCU_USD_quantiles_1, -XCU_USD_tangent_angle1) %>%
      dplyr::select(-XAG_USD, -XAG_USD_log2_price, -XAG_USD_quantiles_2, -XAG_USD_tangent_angle2)

    # rm(AUD_USD, NZD_USD, XAG_USD, XCU_USD, XAU_USD, NZD_CHF, USD_CHF, EUR_AUD, EUR_NZD, XAG_EUR, XAU_EUR, XAG_AUD,
    #    XAU_AUD, XAG_CHF, XAU_CHF)

    copula_data_AUD_USD_XAU_AUD <-
      estimating_dual_copula(
        asset_data_to_use = AUD_USD_NZD_USD,
        asset_to_use = c("AUD_USD", "XAU_AUD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-AUD_USD, -AUD_USD_log1_price, -AUD_USD_quantiles_1, -AUD_USD_tangent_angle1)

    copula_data_AUD_USD_XAG_AUD <-
      estimating_dual_copula(
        asset_data_to_use = AUD_USD_NZD_USD,
        asset_to_use = c("AUD_USD", "XAG_AUD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-AUD_USD, -AUD_USD_log1_price, -AUD_USD_quantiles_1, -AUD_USD_tangent_angle1)

    copula_data_AUD_USD_EUR_AUD <-
      estimating_dual_copula(
        asset_data_to_use = AUD_USD_NZD_USD,
        asset_to_use = c("AUD_USD", "EUR_AUD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-AUD_USD, -AUD_USD_log1_price, -AUD_USD_quantiles_1, -AUD_USD_tangent_angle1)

    copula_data_AUD_USD_XAU_EUR <-
      estimating_dual_copula(
        asset_data_to_use = AUD_USD_NZD_USD,
        asset_to_use = c("AUD_USD", "XAU_EUR"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-AUD_USD, -AUD_USD_log1_price, -AUD_USD_quantiles_1, -AUD_USD_tangent_angle1)

    copula_data_AUD_USD_XAG_EUR <-
      estimating_dual_copula(
        asset_data_to_use = AUD_USD_NZD_USD,
        asset_to_use = c("AUD_USD", "XAG_EUR"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-AUD_USD, -AUD_USD_log1_price, -AUD_USD_quantiles_1, -AUD_USD_tangent_angle1)

    copula_data_XAG_USD_XAG_EUR <-
      estimating_dual_copula(
        asset_data_to_use = AUD_USD_NZD_USD,
        asset_to_use = c("XAG_USD", "XAG_EUR"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAG_USD, -XAG_USD_log1_price, -XAG_USD_quantiles_1, -XAG_USD_tangent_angle1)%>%
      dplyr::select(-XAG_EUR, -XAG_EUR_log2_price, -XAG_EUR_quantiles_2, -XAG_EUR_tangent_angle2)

    copula_data_XAU_USD_XAU_EUR <-
      estimating_dual_copula(
        asset_data_to_use = AUD_USD_NZD_USD,
        asset_to_use = c("XAU_USD", "XAU_EUR"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAU_USD, -XAU_USD_log1_price, -XAU_USD_quantiles_1, -XAU_USD_tangent_angle1)%>%
      dplyr::select(-XAU_EUR, -XAU_EUR_log2_price, -XAU_EUR_quantiles_2, -XAU_EUR_tangent_angle2)

    copula_data_NZD_CHF_USD_CHF <-
      estimating_dual_copula(
        asset_data_to_use = AUD_USD_NZD_USD,
        asset_to_use = c("NZD_CHF", "USD_CHF"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-NZD_CHF, -NZD_CHF_log1_price, -NZD_CHF_quantiles_1, -NZD_CHF_tangent_angle1)

    copula_data_USD_CHF_XAG_CHF <-
      estimating_dual_copula(
        asset_data_to_use = AUD_USD_NZD_USD,
        asset_to_use = c("USD_CHF", "XAG_CHF"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-USD_CHF, -USD_CHF_log1_price, -USD_CHF_quantiles_1, -USD_CHF_tangent_angle1)

    copula_data_XAG_CHF_XAU_CHF <-
      estimating_dual_copula(
        asset_data_to_use = AUD_USD_NZD_USD,
        asset_to_use = c("XAG_CHF", "XAU_CHF"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAG_CHF, -XAG_CHF_log1_price, -XAG_CHF_quantiles_1, -XAG_CHF_tangent_angle1)

    copula_data_XAU_USD_XAU_CHF <-
      estimating_dual_copula(
        asset_data_to_use = AUD_USD_NZD_USD,
        asset_to_use = c("XAU_USD", "XAU_CHF"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAU_USD, -XAU_USD_log1_price, -XAU_USD_quantiles_1, -XAU_USD_tangent_angle1)%>%
      dplyr::select(-XAU_CHF, -XAU_CHF_log2_price, -XAU_CHF_quantiles_2, -XAU_CHF_tangent_angle2)

    copula_data_XAU_EUR_XAG_EUR <-
      estimating_dual_copula(
        asset_data_to_use = AUD_USD_NZD_USD,
        asset_to_use = c("XAU_EUR", "XAG_EUR"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAU_EUR, -XAU_EUR_log1_price, -XAU_EUR_quantiles_1, -XAU_EUR_tangent_angle1)%>%
      dplyr::select(-XAG_EUR, -XAG_EUR_log2_price, -XAG_EUR_quantiles_2, -XAG_EUR_tangent_angle2)

    copula_data_XAU_EUR_XCU_USD <-
      estimating_dual_copula(
        asset_data_to_use = AUD_USD_NZD_USD,
        asset_to_use = c("XAU_EUR", "XCU_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAU_EUR, -XAU_EUR_log1_price, -XAU_EUR_quantiles_1, -XAU_EUR_tangent_angle1)%>%
      dplyr::select(-XCU_USD, -XCU_USD_log2_price, -XCU_USD_quantiles_2, -XCU_USD_tangent_angle2)


    binary_data_for_post_model <-
      actual_wins_losses %>%
      rename(Date = dates, Asset = asset) %>%
      filter(profit_factor == profit_value_var)%>%
      filter(stop_factor == stop_value_var) %>%
      mutate(
        bin_var =
          case_when(
            trade_start_prices > trade_end_prices & trade_col == "Short" ~ "win",
            trade_start_prices <= trade_end_prices & trade_col == "Short" ~ "loss",

            trade_start_prices < trade_end_prices & trade_col == "Long" ~ "win",
            trade_start_prices >= trade_end_prices & trade_col == "Long" ~ "loss"

          )
      ) %>%
      dplyr::select(Date, bin_var, Asset, trade_col,
                    profit_factor, stop_factor,
                    trade_start_prices, trade_end_prices,
                    starting_stop_value, starting_profit_value)

    copula_data_macro <-
      AUD_USD_NZD_USD %>%
      dplyr::select(Date,Asset, Price, High, Low, Open ) %>%
      left_join(copula_data) %>%
      left_join(copula_data_AUD_XCU) %>%
      left_join(copula_data_AUD_XAG) %>%
      left_join(copula_data_NZD_XCU) %>%
      left_join(copula_data_NZD_XAG) %>%
      left_join(copula_data_NZD_USD_CHF) %>%
      left_join(copula_data_AUD_NZD_CHF) %>%
      left_join(copula_data_XAG_XAU) %>%
      left_join(copula_data_XCU_XAU) %>%
      left_join(copula_data_XCU_XAG) %>%
      left_join(copula_data_AUD_USD_XAU_AUD) %>%
      left_join(copula_data_AUD_USD_XAG_AUD) %>%
      left_join(copula_data_AUD_USD_EUR_AUD) %>%
      left_join(copula_data_AUD_USD_XAU_EUR) %>%
      left_join(copula_data_AUD_USD_XAG_EUR) %>%
      left_join(copula_data_XAG_USD_XAG_EUR) %>%
      left_join(copula_data_XAU_USD_XAU_EUR) %>%
      left_join(copula_data_NZD_CHF_USD_CHF) %>%
      left_join(copula_data_USD_CHF_XAG_CHF) %>%
      left_join(copula_data_XAG_CHF_XAU_CHF) %>%
      left_join(copula_data_XAU_USD_XAU_CHF) %>%
      left_join(copula_data_XAU_EUR_XAG_EUR) %>%
      left_join(copula_data_XAU_EUR_XCU_USD) %>%
      left_join(binary_data_for_post_model) %>%
      mutate(Date_for_join = as_date(Date)) %>%
      left_join(
        usd_macro_data %>%
          rename(Date_for_join = date)
      ) %>%
      left_join(
        eur_macro_data %>%
          rename(Date_for_join = date)
      ) %>%
      filter(Date >= min_possible_date) %>%
      group_by(Asset) %>%
      arrange(Date, .by_group = TRUE) %>%
      group_by(Asset) %>%
      fill(matches(all_macro_vars, ignore.case = FALSE), .direction = "down") %>%
      mutate(hour_of_day = lubridate::hour(Date) %>% as.numeric(),
             day_of_week = lubridate::wday(Date) %>% as.numeric())

      max_date_in_testing_data <- copula_data_macro %>% pull(Date) %>% max(na.rm = T)
      message(glue::glue("Max date in Complete data: {max_date_in_testing_data}"))

    lm_quant_vars <- names(copula_data_macro) %>% keep(~ str_detect(.x,"quantiles|tangent|cor"))

    min_allowable_date <-
      copula_data_macro %>%
      ungroup() %>%
      filter(if_all(everything(), ~ !is.na(.))) %>%
      pull(Date) %>% min()

    copula_data_macro <-
      copula_data_macro %>%
      ungroup() %>%
      filter(Date >= min_allowable_date)

    if(use_PCA_vars == TRUE) {
      lm_vars1 <- c(PC_macro_vars, lm_quant_vars,
                    "fib_1", "fib_2",
                    "fib_3", "fib_4",
                    "fib_5", "fib_6",
                    "lagged_var_1", "lagged_var_2",
                    "lagged_var_3", "lagged_var_5",
                    "lagged_var_8",
                    "lagged_var_13", "lagged_var_21",
                    "lagged_var_3_ma", "lagged_var_5_ma",
                    "lagged_var_8_ma", "lagged_var_13_ma",
                    "lagged_var_21_ma"
                    # "hour_of_day", "day_of_week"
                    )
    } else {
      lm_vars1 <- c(all_macro_vars, lm_quant_vars,
                    "fib_1", "fib_2",
                    "fib_3", "fib_4",
                    "fib_5", "fib_6",
                    "lagged_var_1", "lagged_var_2",
                    "lagged_var_3", "lagged_var_5",
                    "lagged_var_8",
                    "lagged_var_13", "lagged_var_21",
                    "lagged_var_3_ma", "lagged_var_5_ma",
                    "lagged_var_8_ma", "lagged_var_13_ma",
                    "lagged_var_21_ma"
                    # "hour_of_day", "day_of_week"
                    )
    }

    return(
      list(
        "copula_data_macro" = copula_data_macro,
       "lm_vars1" =
         lm_vars1 %>%
         keep( ~ !str_detect(.x, "sd") & !str_detect(.x, "tangent") )
      )
    )

  }

#' generate_NNs_create_preds
#'
#' @param copula_data_macro
#' @param NN_samples
#' @param dependant_var_name
#' @param NN_path
#' @param training_max_date
#' @param lm_train_prop
#' @param trade_direction_var
#'
#' @return
#' @export
#'
#' @examples
generate_NNs_create_preds <- function(
    copula_data_macro = copula_data_macro,
    lm_vars1 = lm_vars1,
    NN_samples = NN_samples,
    dependant_var_name = "AUD_USD",
    NN_path = "C:/Users/Nikhil Chandra/Documents/trade_data/asset_specific_NNs/",
    training_max_date = "2021-01-01",
    lm_train_prop = lm_train_prop,
    trade_direction_var = "Long",
    stop_value_var = 15,
    profit_value_var = 20,
    max_NNs = 30,
    hidden_layers = 3,
    ending_thresh = 0.09,
    run_logit_instead = FALSE,
    p_value_thresh_for_inputs = 0.05,
    neuron_adjustment = 0.25,
    lag_price_col = "Price"
) {

  set.seed(round(runif(n = 1, min = 0, max = 100000)))
  training_data <-
    copula_data_macro %>%
    ungroup() %>%
    filter(profit_factor == profit_value_var, stop_factor == stop_value_var) %>%
    filter(Date < as_date(training_max_date)) %>%
    filter(Asset == dependant_var_name) %>%
    filter(trade_col == trade_direction_var) %>%
    group_by(Asset) %>%
    arrange(Date, .by_group = TRUE) %>%
    group_by(Asset) %>%
    mutate(
      lagged_var_1 = lag(!!as.name(lag_price_col), 1) - lag(!!as.name(lag_price_col), 2),
      lagged_var_2 = lag(lagged_var_1, 2),
      lagged_var_3 = lag(lagged_var_1, 3),
      lagged_var_5 = lag(lagged_var_1, 5),
      lagged_var_8 = lag(lagged_var_1, 8),
      lagged_var_13 = lag(lagged_var_1, 13),
      lagged_var_21 = lag(lagged_var_1, 21),

      fib_1 = lagged_var_1 + lagged_var_2,
      fib_2 = lagged_var_2 + lagged_var_3,
      fib_3 = lagged_var_3 + lagged_var_5,
      fib_4 = lagged_var_5 + lagged_var_8,
      fib_5 = lagged_var_8 + lagged_var_13,
      fib_6 = lagged_var_13 + lagged_var_21,

      lagged_var_3_ma = slider::slide_dbl(.x = lagged_var_1,
                                          .f = ~ mean(.x, na.rm = T),
                                          .before = 3),

      lagged_var_5_ma = slider::slide_dbl(.x = lagged_var_1,
                                          .f = ~ mean(.x, na.rm = T),
                                          .before = 5),

      lagged_var_8_ma = slider::slide_dbl(.x = lagged_var_1,
                                          .f = ~ mean(.x, na.rm = T),
                                          .before = 8),

      lagged_var_13_ma = slider::slide_dbl(.x = lagged_var_1,
                                           .f = ~ mean(.x, na.rm = T),
                                           .before = 13),

      lagged_var_21_ma = slider::slide_dbl(.x = lagged_var_1,
                                           .f = ~ mean(.x, na.rm = T),
                                           .before = 21)
    ) %>%
    ungroup() %>%
    distinct()

  gc()
  message("Made it past first training data wrangle")

  training_data <-
    training_data %>%
    group_by(Asset) %>%
    arrange(Date, .by_group = TRUE) %>%
    group_by(Asset) %>%
    fill(!matches(c("bin_var","trade_col"), ignore.case = FALSE), .direction = "down") %>%
    ungroup() %>%
    filter(if_all(everything() ,.fns = ~ !is.na(.)))

  max_date_in_testing_data <- training_data %>% pull(Date) %>% max(na.rm = T)
  message(glue::glue("Max date in Training data: {max_date_in_testing_data}"))

  NN_form <-  create_lm_formula(dependant = "bin_var=='win'", independant = lm_vars1)

  if(run_logit_instead == FALSE) {
    for (i in 1:max_NNs) {
      set.seed(round(runif(1,2,10000)))

      glm_model_1 <- glm(formula = NN_form,
                         data = training_data ,
                         family = binomial("logit"))

      all_coefs <- glm_model_1 %>% jtools::j_summ() %>% pluck(1)
      coef_names <- row.names(all_coefs) %>% as.character()

      filtered_coefs <-
        all_coefs %>%
        as_tibble() %>%
        mutate(all_vars = coef_names) %>%
        filter(p <= p_value_thresh_for_inputs) %>%
        filter(!str_detect(all_vars, "Intercep")) %>%
        pull(all_vars) %>%
        as.character()

      message(length(filtered_coefs))

      NN_form <-  create_lm_formula(dependant = "bin_var=='win'", independant = filtered_coefs)

      hidden_layers <- rep(
                          (length(filtered_coefs) + round(length(filtered_coefs)*neuron_adjustment)),
                           hidden_layers
                          )

      message("Made Formula")

      NN_model_1 <- neuralnet::neuralnet(formula = NN_form,
                                         hidden = hidden_layers,
                                         data = training_data %>% slice_sample(n = NN_samples),
                                         lifesign = 'full',
                                         rep = 1,
                                         stepmax = 1000000,
                                         threshold = ending_thresh)

      saveRDS(object = NN_model_1,
              file =
                glue::glue("{NN_path}/{dependant_var_name}_NN_{i}.rds")
      )

    }
  } else {
    for (i in 1:max_NNs) {
      set.seed(round(runif(1,2,10000)))

      glm_model_1 <- glm(formula = NN_form,
                         data = training_data ,
                         family = binomial("logit"))

      all_coefs <- glm_model_1 %>% jtools::j_summ() %>% pluck(1)
      coef_names <- row.names(all_coefs) %>% as.character()

      filtered_coefs <-
        all_coefs %>%
        as_tibble() %>%
        mutate(all_vars = coef_names) %>%
        filter(p <= p_value_thresh_for_inputs) %>%
        filter(!str_detect(all_vars, "Intercep")) %>%
        pull(all_vars) %>%
        as.character()

      NN_form <-  create_lm_formula(dependant = "bin_var=='win'", independant = filtered_coefs)

      NN_model_1 <- glm(formula = NN_form,
                         data = training_data ,
                         family = binomial("logit"))

      saveRDS(object = NN_model_1,
              file =
                glue::glue("{NN_path}/{dependant_var_name}_GLM_{i}.rds")
      )
    }

  }

  return(NN_model_1)

}


#' read_NNs_create_preds
#'
#' @param copula_data_macro
#' @param lm_vars1
#' @param dependant_var_name
#' @param NN_path
#' @param lm_test_prop
#' @param testing_min_date
#' @param trade_direction_var
#' @param NN_index_to_choose
#' @param stop_value_var
#' @param profit_value_var
#' @param analysis_threshs
#'
#' @return
#' @export
#'
#' @examples
read_NNs_create_preds <- function(
    copula_data_macro = copula_data_macro,
    lm_vars1 = lm_vars1,
    dependant_var_name = "AUD_USD",
    NN_path = "C:/Users/Nikhil Chandra/Documents/trade_data/asset_specific_NNs/",
    testing_min_date = "2021-01-01",
    trade_direction_var = "Long",
    NN_index_to_choose = "",
    stop_value_var = 15,
    profit_value_var = 20,
    analysis_threshs,
    run_logit_instead = FALSE,
    lag_price_col = "Price",
    return_tagged_trades = FALSE
  ) {

  set.seed(round(runif(n = 1, min = 0, max = 100000)))

  deal_with_NAs_for_trade_flag <-
    copula_data_macro %>%
      ungroup() %>%
      filter(is.na(trade_col)) %>%
      dplyr::select(Date, Asset, Price, Open, Low, High,
                    matches(lm_vars1, ignore.case = FALSE) ) %>%
      distinct() %>%
      mutate(
        trade_col = trade_direction_var,
        profit_factor = profit_value_var,
        stop_factor = stop_value_var
    )

  max_date_in_testing_data <- deal_with_NAs_for_trade_flag %>% pull(Date) %>% max(na.rm = T)
  message(glue::glue("Max date in NA Flag data: {max_date_in_testing_data}"))

  testing_data <-
    copula_data_macro %>%
    ungroup() %>%
    filter(!is.na(trade_col)) %>%
    bind_rows(deal_with_NAs_for_trade_flag) %>%
    filter(profit_factor == profit_value_var, stop_factor == stop_value_var) %>%
    filter(Date >= as_date(testing_min_date)) %>%
    filter(Asset == dependant_var_name) %>%
    filter(trade_col == trade_direction_var) %>%
    group_by(Asset) %>%
    arrange(Date, .by_group = TRUE) %>%
    group_by(Asset) %>%
    mutate(
      lagged_var_1 = lag(!!as.name(lag_price_col), 1) - lag(!!as.name(lag_price_col), 2),
      lagged_var_2 = lag(lagged_var_1, 2),
      lagged_var_3 = lag(lagged_var_1, 3),
      lagged_var_5 = lag(lagged_var_1, 5),
      lagged_var_8 = lag(lagged_var_1, 8),
      lagged_var_13 = lag(lagged_var_1, 13),
      lagged_var_21 = lag(lagged_var_1, 21),

      fib_1 = lagged_var_1 + lagged_var_2,
      fib_2 = lagged_var_2 + lagged_var_3,
      fib_3 = lagged_var_3 + lagged_var_5,
      fib_4 = lagged_var_5 + lagged_var_8,
      fib_5 = lagged_var_8 + lagged_var_13,
      fib_6 = lagged_var_13 + lagged_var_21,

      lagged_var_3_ma = slider::slide_dbl(.x = lagged_var_1,
                                          .f = ~ mean(.x, na.rm = T),
                                          .before = 3),

      lagged_var_5_ma = slider::slide_dbl(.x = lagged_var_1,
                                          .f = ~ mean(.x, na.rm = T),
                                          .before = 5),

      lagged_var_8_ma = slider::slide_dbl(.x = lagged_var_1,
                                          .f = ~ mean(.x, na.rm = T),
                                          .before = 8),

      lagged_var_13_ma = slider::slide_dbl(.x = lagged_var_1,
                                           .f = ~ mean(.x, na.rm = T),
                                           .before = 13),

      lagged_var_21_ma = slider::slide_dbl(.x = lagged_var_1,
                                           .f = ~ mean(.x, na.rm = T),
                                           .before = 21)
    ) %>%
    ungroup() %>%
    group_by(Asset) %>%
    arrange(Date, .by_group = TRUE) %>%
    group_by(Asset) %>%
    fill(!matches(c("bin_var","trade_col"), ignore.case = FALSE), .direction = "down") %>%
    ungroup() %>%
    distinct()


  max_date_in_testing_data <- testing_data %>% pull(Date) %>% max(na.rm = T)
  message(glue::glue("Max date in testing data: {max_date_in_testing_data}"))

  if(run_logit_instead == FALSE) {
    NNs_compiled <-
      fs::dir_info(NN_path)  %>%
      filter(str_detect(path, as.character(glue::glue("{dependant_var_name}_NN_{NN_index_to_choose}")) )) %>%
      split(.$path, drop = FALSE) %>%
      map_dfr(
        ~
          tibble( pred = predict(object = readRDS(.x$path[1]),newdata = testing_data) %>%
                    as.numeric()) %>%
          mutate(
            # pred = if_else(pred < 0, 0, pred),
            index = row_number()
          )
      )
  } else {

    NNs_compiled <-
      fs::dir_info(NN_path)  %>%
      filter(str_detect(path, as.character(glue::glue("{dependant_var_name}_GLM_{NN_index_to_choose}")) )) %>%
      split(.$path, drop = FALSE) %>%
      map_dfr(
        ~
          tibble( pred = predict.glm(object = readRDS(.x$path[1]),newdata = testing_data, type = "response") %>%
                    as.numeric()) %>%
          mutate(
            # pred = if_else(pred < 0, 0, pred),
            index = row_number()
          )
      )

  }

  message("Done NN's")
  message(dim(NNs_compiled))

  NNs_compiled2 <-
    NNs_compiled %>%
    group_by(index) %>%
    summarise(
      pred = mean(pred, na.rm = T)
    )

  pred_NN  <- NNs_compiled2 %>% pull(pred) %>% as.numeric()

  check_preds <-
    pred_NN %>% keep(~ !is.na(.x)) %>% length()

  max_preds <-
    pred_NN %>% max(na.rm = T)

  message(glue::glue("Number of Non-NA NN preds: {check_preds}"))

  post_testing_data <-
    testing_data %>%
    mutate(
      pred = pred_NN
    )
    # dplyr::select(
    #   Date, Asset, Price, Open, Low, High,profit_factor,
    #   stop_factor,trade_col, pred,
    #   matches(lm_vars1, ignore.case = FALSE)
    # )

  pred_at_max_date <-
    post_testing_data %>%
    ungroup() %>%
    slice_max(Date) %>%
    pull(pred) %>%
    as.numeric()

  max_date_in_post_testing_data <-
    post_testing_data %>%
    filter(!is.na(pred)) %>%
    pull(Date) %>%
    max(na.rm = T) %>%
    as.character()

  message(glue::glue("Date in Post-NN Prediction Data: {max_date_in_post_testing_data}"))
  message(glue::glue("Pred at max Date: {pred_at_max_date}"))

  trade_dollar_returns <-
    testing_data %>%
    filter(!is.na(bin_var)) %>%
    dplyr::select(Date, Asset, profit_factor, stop_factor,
                  trade_start_prices, trade_end_prices,
                  starting_stop_value, starting_profit_value,
                  trade_col) %>%
    filter(profit_factor == profit_value_var, stop_factor == stop_value_var) %>%
    filter(Asset == dependant_var_name) %>%
    convert_stop_profit_AUD(
      asset_infor = asset_infor,
      currency_conversion = currency_conversion,
      asset_col = "Asset",
      stop_col = "starting_stop_value",
      profit_col = "starting_profit_value",
      price_col = "trade_start_prices",
      risk_dollar_value = 10,
      returns_present = FALSE,
      trade_return_col = "trade_return"
    ) %>%
    mutate(
      trade_returns_AUD =
        case_when(
          trade_col == "Long" & trade_start_prices > trade_end_prices ~ maximum_win,
          trade_col == "Long" & trade_start_prices <= trade_end_prices ~ -1*minimal_loss,

          trade_col == "Short" & trade_start_prices < trade_end_prices ~ maximum_win,
          trade_col == "Short" & trade_start_prices >= trade_end_prices ~ -1*minimal_loss
        )
    ) %>%
    dplyr::select(Date, trade_returns_AUD, Asset,
                  profit_factor , stop_factor, maximum_win, minimal_loss) %>%
    distinct()

  analysis_control <-
    post_testing_data %>%
    ungroup() %>%
    left_join(trade_dollar_returns)  %>%
    filter(!is.na(bin_var)) %>%
    group_by( bin_var) %>%
    summarise(
      wins_losses = n(),
    ) %>%
    ungroup() %>%
    mutate(
      Total_control = sum(wins_losses),
      Perc_control = wins_losses/Total_control
    )  %>%
    dplyr::select( -wins_losses)

  analysis_list <- list()

  for (i in 1:length(analysis_threshs) ) {

    temp_trades  <-
      post_testing_data %>%
      filter(!is.na(bin_var)) %>%
      mutate(
        trade_col = case_when(pred >= analysis_threshs[i] ~ trade_direction_var,
                              TRUE ~ "No Trade")
      ) %>%
      ungroup()

    analysis_list[[i]] <-
      temp_trades %>%
      left_join(trade_dollar_returns) %>%
      group_by(trade_col, bin_var) %>%
      summarise(
        wins_losses = n(),
        win_amount = max(maximum_win),
        loss_amount = min(minimal_loss),
      ) %>%
      group_by(trade_col) %>%
      mutate(
        Total = sum(wins_losses),
        Perc = wins_losses/Total,
        returns =
          case_when(
            bin_var == "win" ~ wins_losses*win_amount,
            bin_var == "loss" ~ wins_losses*loss_amount
          )
      ) %>%
      filter(trade_col != "No Trade") %>%
      mutate(
        risk_weighted_return =
          (win_amount/abs(loss_amount) )*Perc - (1- Perc)
      ) %>%
      left_join(analysis_control) %>%
      mutate(
        control_risk_return =
          (win_amount/abs(loss_amount) )*Perc_control - (1- Perc_control)
      ) %>%
      filter(bin_var == "win") %>%
      dplyr::select(-bin_var) %>%
      mutate(Asset = dependant_var_name,
             profit_factor = profit_value_var,
             stop_factor = stop_value_var,
             threshold = analysis_threshs[i]) %>%
      dplyr::select(
        trade_col, Asset, wins_losses, win_amount, loss_amount, Trades = Total, Perc,
        returns, risk_weighted_return, Total_control, Perc_control, control_risk_return,
        threshold
      )
  }

  analysis <-
    analysis_list %>%
    map_dfr(bind_rows)

  if(return_tagged_trades == TRUE) {
   return(  tagged_trades <- post_testing_data %>%
              dplyr::select(Date, Asset, Price, Open, Low, High,profit_factor,
                            stop_factor,trade_col, pred))
  } else {
    return(analysis)
  }

}


#' create_NN_EUR_GBP_JPY_USD_data
#'
#' @return
#' @export
#'
#' @examples
create_NN_EUR_GBP_JPY_USD_data <-
  function(EUR_USD_JPY_GBP = EUR_USD_JPY_GBP_list[[1]],
           raw_macro_data,
           actual_wins_losses = actual_wins_losses,
           lag_days = 1,
           stop_value_var = 15,
           profit_value_var = 20,
           use_PCA_vars = FALSE) {

    # assets_to_return <- dependant_var_name

    aus_macro_data <-
      get_AUS_Indicators(raw_macro_data,
                         lag_days = lag_days,
                         first_difference = TRUE
      ) %>%
      janitor::clean_names()

    nzd_macro_data <-
      get_NZD_Indicators(raw_macro_data,
                         lag_days = lag_days,
                         first_difference = TRUE
      ) %>%
      janitor::clean_names()

    usd_macro_data <-
      get_USD_Indicators(raw_macro_data,
                         lag_days = lag_days,
                         first_difference = TRUE
      ) %>%
      janitor::clean_names()

    cny_macro_data <-
      get_CNY_Indicators(raw_macro_data,
                         lag_days = lag_days,
                         first_difference = TRUE
      ) %>%
      janitor::clean_names()

    eur_macro_data <-
      get_EUR_Indicators(raw_macro_data,
                         lag_days = lag_days,
                         first_difference = TRUE
      ) %>%
      janitor::clean_names()

    aud_macro_vars <- names(aus_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    nzd_macro_vars <- names(nzd_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    usd_macro_vars <- names(usd_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    cny_macro_vars <- names(cny_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    eur_macro_vars <- names(eur_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    all_macro_vars <- c(aud_macro_vars, nzd_macro_vars, usd_macro_vars, cny_macro_vars, eur_macro_vars)

    copula_data <-
      estimating_dual_copula(
        asset_data_to_use = EUR_USD_JPY_GBP,
        asset_to_use = c("EUR_GBP", "EUR_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      )

    copula_data_EUR_GBP_USD <-
      estimating_dual_copula(
        asset_data_to_use = EUR_USD_JPY_GBP,
        asset_to_use = c("EUR_GBP", "GBP_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-EUR_GBP, -EUR_GBP_log1_price, -EUR_GBP_quantiles_1, -EUR_GBP_tangent_angle1)

    copula_data_EUR_USD_GBP_USD <-
      estimating_dual_copula(
        asset_data_to_use = EUR_USD_JPY_GBP,
        asset_to_use = c("EUR_USD", "GBP_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-EUR_USD, -EUR_USD_log1_price, -EUR_USD_quantiles_1, -EUR_USD_tangent_angle1)%>%
      dplyr::select(-GBP_USD, -GBP_USD_log2_price, -GBP_USD_quantiles_2, -GBP_USD_tangent_angle2)

    copula_data_EUR_JPY_USD_JPY <-
      estimating_dual_copula(
        asset_data_to_use = EUR_USD_JPY_GBP,
        asset_to_use = c("EUR_JPY", "USD_JPY"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      )

    copula_data_GBP_JPY_USD_JPY <-
      estimating_dual_copula(
        asset_data_to_use = EUR_USD_JPY_GBP,
        asset_to_use = c("GBP_JPY", "USD_JPY"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-USD_JPY, -USD_JPY_log2_price, -USD_JPY_quantiles_2, -USD_JPY_tangent_angle2)

    copula_data_EUR_JPY_GBP_JPY <-
      estimating_dual_copula(
        asset_data_to_use = EUR_USD_JPY_GBP,
        asset_to_use = c("EUR_JPY", "GBP_JPY"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-EUR_JPY, -EUR_JPY_log1_price, -EUR_JPY_quantiles_1, -EUR_JPY_tangent_angle1)%>%
      dplyr::select(-GBP_JPY, -GBP_JPY_log2_price, -GBP_JPY_quantiles_2, -GBP_JPY_tangent_angle2)

    copula_data_EUR_JPY_EUR_GBP <-
      estimating_dual_copula(
        asset_data_to_use = EUR_USD_JPY_GBP,
        asset_to_use = c("EUR_JPY", "EUR_GBP"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-EUR_JPY, -EUR_JPY_log1_price, -EUR_JPY_quantiles_1, -EUR_JPY_tangent_angle1)%>%
      dplyr::select(-EUR_GBP, -EUR_GBP_log2_price, -EUR_GBP_quantiles_2, -EUR_GBP_tangent_angle2)

    copula_data_GBP_JPY_EUR_GBP <-
      estimating_dual_copula(
        asset_data_to_use = EUR_USD_JPY_GBP,
        asset_to_use = c("GBP_JPY", "EUR_GBP"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-GBP_JPY, -GBP_JPY_log1_price, -GBP_JPY_quantiles_1, -GBP_JPY_tangent_angle1)%>%
      dplyr::select(-EUR_GBP, -EUR_GBP_log2_price, -EUR_GBP_quantiles_2, -EUR_GBP_tangent_angle2)

    binary_data_for_post_model <-
      actual_wins_losses %>%
      rename(Date = dates, Asset = asset) %>%
      filter(profit_factor == profit_value_var)%>%
      filter(stop_factor == stop_value_var) %>%
      mutate(
        bin_var =
          case_when(
            trade_start_prices > trade_end_prices & trade_col == "Short" ~ "win",
            trade_start_prices <= trade_end_prices & trade_col == "Short" ~ "loss",

            trade_start_prices < trade_end_prices & trade_col == "Long" ~ "win",
            trade_start_prices >= trade_end_prices & trade_col == "Long" ~ "loss"

          )
      ) %>%
      dplyr::select(Date, bin_var, Asset, trade_col,
                    profit_factor, stop_factor,
                    trade_start_prices, trade_end_prices,
                    starting_stop_value, starting_profit_value)

    # c(copula_data, copula_data_EUR_GBP_USD, copula_data_EUR_USD_GBP_USD, copula_data_EUR_JPY_USD_JPY,
    #   copula_data_GBP_JPY_USD_JPY, copula_data_EUR_JPY_GBP_JPY, copula_data_EUR_JPY_EUR_GBP,
    #   copula_data_GBP_JPY_EUR_GBP)


    copula_data_macro <-
      EUR_USD_JPY_GBP %>%
      dplyr::select(Date,Asset, Price, High, Low, Open ) %>%
      left_join(copula_data) %>%
      left_join(copula_data_EUR_GBP_USD) %>%
      left_join(copula_data_EUR_USD_GBP_USD) %>%
      left_join(copula_data_EUR_JPY_USD_JPY) %>%
      left_join(copula_data_GBP_JPY_USD_JPY) %>%
      left_join(copula_data_EUR_JPY_GBP_JPY) %>%
      left_join(copula_data_EUR_JPY_EUR_GBP) %>%
      left_join(copula_data_GBP_JPY_EUR_GBP) %>%
      left_join(binary_data_for_post_model) %>%
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
      group_by(Asset) %>%
      arrange(Date, .by_group = TRUE) %>%
      group_by(Asset) %>%
      fill(matches(all_macro_vars, ignore.case = FALSE), .direction = "down") %>%
      mutate(hour_of_day = lubridate::hour(Date) %>% as.numeric(),
             day_of_week = lubridate::wday(Date) %>% as.numeric())

    max_date_in_testing_data <- copula_data_macro %>% pull(Date) %>% max(na.rm = T)
    message(glue::glue("Max date in Complete data: {max_date_in_testing_data}"))

    lm_quant_vars <- names(copula_data_macro) %>% keep(~ str_detect(.x,"quantiles|tangent|cor"))

    if(use_PCA_vars == TRUE) {
      lm_vars1 <- c(PC_macro_vars, lm_quant_vars,
                    "fib_1", "fib_2",
                    "fib_3", "fib_4",
                    "fib_5", "fib_6",
                    "lagged_var_1", "lagged_var_2",
                    "lagged_var_3", "lagged_var_5",
                    "lagged_var_8",
                    "lagged_var_13", "lagged_var_21",
                    "lagged_var_3_ma", "lagged_var_5_ma",
                    "lagged_var_8_ma", "lagged_var_13_ma",
                    "lagged_var_21_ma"
                    # "hour_of_day", "day_of_week"
      )
    } else {
      lm_vars1 <- c(all_macro_vars, lm_quant_vars,
                    "fib_1", "fib_2",
                    "fib_3", "fib_4",
                    "fib_5", "fib_6",
                    "lagged_var_1", "lagged_var_2",
                    "lagged_var_3", "lagged_var_5",
                    "lagged_var_8",
                    "lagged_var_13", "lagged_var_21",
                    "lagged_var_3_ma", "lagged_var_5_ma",
                    "lagged_var_8_ma", "lagged_var_13_ma",
                    "lagged_var_21_ma"
                    # "hour_of_day", "day_of_week"
      )
    }

    return(
      list(
        "copula_data_macro" = copula_data_macro,
        "lm_vars1" =
          lm_vars1 %>%
          keep( ~ !str_detect(.x, "sd") & !str_detect(.x, "tangent") )
      )
    )

  }


#' get_Logit_trades
#'
#' @param NN_path
#' @param copula_data
#' @param stop_value_var
#' @param profit_value_var
#' @param NN_path_save_path
#'
#' @return
#' @export
#'
#' @examples
get_Logit_trades <-
  function(
    logit_path_save_path = "C:/Users/Nikhil Chandra/Documents/trade_data/asset_specific_NNs/",
    Logit_sims_db = "C:/Users/Nikhil Chandra/Documents/trade_data/AUD_USD_NZD_XCU_Logit_sims.db",
    copula_data = copula_data_AUD_USD_NZD,
    sim_min = 20,
    edge_min = 0,
    stop_var = 4,
    profit_var = 8,
    outperformance_count_min = 0.51,
    risk_weighted_return_mid_min =  0.14,
    sim_table = "AUD_USD_NZD_XCU_NN_sims",
    p_value_thresh_exact_filter = NULL,
    slice_max_var = "risk_weighted_return_mid",
    combined_filter_n = NULL
) {

  date_filter_for_Logit <-
    as.character(copula_data[[1]]$Date %>% max(na.rm = T) + days(1))

  Logit_sims_db_con <- connect_db(path = Logit_sims_db)
  all_results_ts_dfr <- DBI::dbGetQuery(conn = Logit_sims_db_con,
                                        statement = as.character(glue::glue("SELECT * FROM {sim_table}")) )
  DBI::dbDisconnect(Logit_sims_db_con)
  rm(Logit_sims_db_con)


  if(!is.null(p_value_thresh_exact_filter)) {

    all_results_ts_dfr <-
      all_results_ts_dfr %>%
      ungroup() %>%
      filter(p_value_thresh_for_inputs == p_value_thresh_exact_filter)
  }

  all_asset_logit_results_sum <-
    all_results_ts_dfr %>%
    filter(stop_factor == stop_var, profit_factor == profit_var) %>%
    mutate(
      edge = risk_weighted_return - control_risk_return,
      outperformance_count = ifelse(Perc > Perc_control, 1, 0),
      returns_total = Trades*Perc*win_amount - Trades*loss_amount*(1-Perc)
    ) %>%
    group_by(Asset, threshold, NN_samples, ending_thresh, p_value_thresh_for_inputs, neuron_adjustment,
             hidden_layers, trade_col, stop_factor, profit_factor
    ) %>%
    summarise(
      win_amount = mean(win_amount, na.rm = T),
      loss_amount = mean(loss_amount, na.rm = T),
      Trades = mean(Trades, na.rm = T),
      edge = mean(edge, na.rm = T),
      Perc = mean(Perc, na.rm = T),
      Median_Actual_Return = median(returns_total, na.rm = T),
      risk_weighted_return_low = quantile(risk_weighted_return, 0.25 ,na.rm = T),
      risk_weighted_return_mid = median(risk_weighted_return, na.rm = T),
      risk_weighted_return_high = quantile(risk_weighted_return, 0.75 ,na.rm = T),
      control_trades = mean(Total_control, na.rm = T),
      control_risk_return_mid = median(control_risk_return, na.rm = T),
      simulations = n(),
      outperformance_count = sum(outperformance_count)
    ) %>%
    mutate(
      outperformance_perc = outperformance_count/simulations
    ) %>%
    filter(simulations >= sim_min,
           edge > edge_min,
           outperformance_perc > outperformance_count_min,
           risk_weighted_return_mid > risk_weighted_return_mid_min)

  if( !is.null(combined_filter_n)  ) {
    all_asset_logit_results_sum <-
      all_asset_logit_results_sum %>%
      group_by(Asset) %>%
      slice_max(risk_weighted_return_mid, n = combined_filter_n) %>%
      group_by(Asset) %>%
      slice_max(Trades)%>%
      ungroup()
  } else {
    all_asset_logit_results_sum <- all_asset_logit_results_sum %>%
      group_by(Asset) %>%
      slice_max(!!as.name(slice_max_var))%>%
      ungroup()
  }

  gernating_params <-
    all_asset_logit_results_sum %>%
    distinct(Asset, NN_samples, ending_thresh,
             p_value_thresh_for_inputs,
             neuron_adjustment,
             hidden_layers,
             trade_col,
             threshold,
             stop_factor, profit_factor)

  accumulating_trades <- list()

  for (i in 1:dim(gernating_params)[1] ) {

    check_completion <- generate_NNs_create_preds(
      copula_data_macro = copula_data[[1]],
      lm_vars1 = copula_data[[2]],
      NN_samples = gernating_params$NN_samples[i] %>% as.integer(),
      dependant_var_name = gernating_params$Asset[i] %>% as.character(),
      NN_path = logit_path_save_path,
      training_max_date = date_filter_for_Logit,
      lm_train_prop = 1,
      trade_direction_var = gernating_params$trade_col[i] %>% as.character(),
      stop_value_var = gernating_params$stop_factor[i]%>% as.numeric(),
      profit_value_var = gernating_params$profit_factor[i]%>% as.numeric(),
      max_NNs = 1,
      hidden_layers = gernating_params$hidden_layers[i] %>% as.integer(),
      ending_thresh = gernating_params$ending_thresh[i] %>% as.numeric(),
      run_logit_instead = TRUE,
      p_value_thresh_for_inputs = gernating_params$p_value_thresh_for_inputs[i] %>% as.numeric(),
      neuron_adjustment = gernating_params$neuron_adjustment[i] %>% as.numeric(),
      lag_price_col = "Price"
    )

    gc()
    Sys.sleep(2)


    NN_test_preds <-
      read_NNs_create_preds(
        copula_data_macro = copula_data[[1]],
        lm_vars1 = copula_data[[2]],
        dependant_var_name = gernating_params$Asset[i] %>% as.character(),
        NN_path = logit_path_save_path,
        testing_min_date = as.character(as_date(date_filter_for_Logit) - days(2000)),
        trade_direction_var = gernating_params$trade_col[i] %>% as.character(),
        NN_index_to_choose = "",
        stop_value_var = gernating_params$stop_factor[i]%>% as.numeric(),
        profit_value_var = gernating_params$profit_factor[i]%>% as.numeric(),
        analysis_threshs = c(0.5,0.6, 0.7, 0.8, 0.9, 0.95, 0.99, 0.999),
        run_logit_instead = TRUE,
        lag_price_col = "Price",
        return_tagged_trades = TRUE
      )

    gc()
    Sys.sleep(2)

    accumulating_trades[[i]] <-
      NN_test_preds %>%
      slice_max(Date) %>%
      filter(Asset == gernating_params$Asset[i] %>% as.character()) %>%
      mutate(
        pred_min = gernating_params$threshold[i] %>% as.numeric()
      )

    rm(NN_test_preds, check_completion)
    gc()
    Sys.sleep(2)

  }

  all_trades <-
    accumulating_trades %>%
    map_dfr(bind_rows)

  return(all_trades)

}

#' get_ts_trade_actuals_Logit_NN
#'
#' @param full_ts_trade_db_location
#'
#' @return
#' @export
#'
#' @examples
get_ts_trade_actuals_Logit_NN <-
  function(
    full_ts_trade_db_location = "C:/Users/Nikhil Chandra/Documents/trade_data/full_ts_trades_mapped_AUD_USD.db",
    data_is_daily = FALSE
    ) {

    full_ts_trade_db_con <- connect_db(path = full_ts_trade_db_location)

    if(data_is_daily == FALSE) {
      actual_wins_losses <-
        DBI::dbGetQuery(full_ts_trade_db_con,
                        "SELECT * FROM full_ts_trades_mapped") %>%
        mutate(
          dates = as_datetime(dates)
        )
    }

    if(data_is_daily == TRUE) {
      actual_wins_losses <-
        DBI::dbGetQuery(full_ts_trade_db_con,
                        "SELECT * FROM full_ts_trades_mapped") %>%
        mutate(
          dates = as_date(dates)
        )
    }

    DBI::dbDisconnect(full_ts_trade_db_con)
    rm(full_ts_trade_db_con)

    return(actual_wins_losses)
  }

#' create_NN_Idices_Silver_H1Vers_data
#'
#' @return
#' @export
#'
#' @examples
create_NN_Idices_Silver_H1Vers_data <-
  function(SPX_US2000_XAG,
           raw_macro_data,
           actual_wins_losses = actual_wins_losses,
           lag_days = 1,
           stop_value_var = 15,
           profit_value_var = 20,
           use_PCA_vars = FALSE) {

    # assets_to_return <- dependant_var_name

    aus_macro_data <-
      get_AUS_Indicators(raw_macro_data,
                         lag_days = lag_days,
                         first_difference = TRUE
      ) %>%
      janitor::clean_names()

    # nzd_macro_data <-
    #   get_NZD_Indicators(raw_macro_data,
    #                      lag_days = lag_days,
    #                      first_difference = TRUE
    #   ) %>%
    #   janitor::clean_names()

    usd_macro_data <-
      get_USD_Indicators(raw_macro_data,
                         lag_days = lag_days,
                         first_difference = TRUE
      ) %>%
      janitor::clean_names()

    cny_macro_data <-
      get_CNY_Indicators(raw_macro_data,
                         lag_days = lag_days,
                         first_difference = TRUE
      ) %>%
      janitor::clean_names()

    eur_macro_data <-
      get_EUR_Indicators(raw_macro_data,
                         lag_days = lag_days,
                         first_difference = TRUE
      ) %>%
      janitor::clean_names()

    aud_macro_vars <- names(aus_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    # nzd_macro_vars <- names(nzd_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    usd_macro_vars <- names(usd_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    cny_macro_vars <- names(cny_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    eur_macro_vars <- names(eur_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    all_macro_vars <- c(
      aud_macro_vars,
      # nzd_macro_vars,
      usd_macro_vars,
      cny_macro_vars,
      eur_macro_vars)

    copula_data <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("SPX500_USD", "US2000_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      )

    copula_data_SPX_XAU <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("SPX500_USD", "XAU_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-SPX500_USD, -SPX500_USD_log1_price, -SPX500_USD_quantiles_1, -SPX500_USD_tangent_angle1)

    copula_data_US2000_XAU <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("US2000_USD", "XAU_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-US2000_USD, -US2000_USD_log1_price, -US2000_USD_quantiles_1, -US2000_USD_tangent_angle1)%>%
      dplyr::select(-XAU_USD, -XAU_USD_log2_price, -XAU_USD_quantiles_2, -XAU_USD_tangent_angle2)

    copula_data_SPX_AU200 <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("SPX500_USD", "AU200_AUD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-SPX500_USD, -SPX500_USD_log1_price, -SPX500_USD_quantiles_1, -SPX500_USD_tangent_angle1)

    copula_data_US2000_AU200 <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("US2000_USD", "AU200_AUD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-US2000_USD, -US2000_USD_log1_price, -US2000_USD_quantiles_1, -US2000_USD_tangent_angle1)%>%
      dplyr::select(-AU200_AUD, -AU200_AUD_log2_price, -AU200_AUD_quantiles_2, -AU200_AUD_tangent_angle2)

    copula_data_AU200_AUD_XAG <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("AU200_AUD", "XAU_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-AU200_AUD, -AU200_AUD_log1_price, -AU200_AUD_quantiles_1, -AU200_AUD_tangent_angle1)%>%
      dplyr::select(-XAU_USD, -XAU_USD_log2_price, -XAU_USD_quantiles_2, -XAU_USD_tangent_angle2)

    copula_data_SPX500_EUR50 <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("SPX500_USD", "EU50_EUR"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-SPX500_USD, -SPX500_USD_log1_price, -SPX500_USD_quantiles_1, -SPX500_USD_tangent_angle1)

    copula_data_US2000_USD_EUR50 <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("US2000_USD", "EU50_EUR"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-US2000_USD, -US2000_USD_log1_price, -US2000_USD_quantiles_1, -US2000_USD_tangent_angle1)%>%
      dplyr::select(-EU50_EUR, -EU50_EUR_log2_price, -EU50_EUR_quantiles_2, -EU50_EUR_tangent_angle2)

    copula_data_XAU_USD_EU50_EUR <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("XAU_USD", "EU50_EUR"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAU_USD, -XAU_USD_log1_price, -XAU_USD_quantiles_1, -XAU_USD_tangent_angle1)%>%
      dplyr::select(-EU50_EUR, -EU50_EUR_log2_price, -EU50_EUR_quantiles_2, -EU50_EUR_tangent_angle2)

    copula_data_XAU_USD_EU50_EUR <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("XAG_USD", "EU50_EUR"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAG_USD, -XAG_USD_log1_price, -XAG_USD_quantiles_1, -XAG_USD_tangent_angle1)%>%
      dplyr::select(-EU50_EUR, -EU50_EUR_log2_price, -EU50_EUR_quantiles_2, -EU50_EUR_tangent_angle2)

    copula_data_AU200_AUD_EU50_EUR <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("AU200_AUD", "EU50_EUR"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-AU200_AUD, -AU200_AUD_log1_price, -AU200_AUD_quantiles_1, -AU200_AUD_tangent_angle1)%>%
      dplyr::select(-EU50_EUR, -EU50_EUR_log2_price, -EU50_EUR_quantiles_2, -EU50_EUR_tangent_angle2)

    copula_data_SG30_XAU_USD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("SG30_SGD", "XAU_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAU_USD, -XAU_USD_log2_price, -XAU_USD_quantiles_2, -XAU_USD_tangent_angle2)

    copula_data_SG30_SPX500_USD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("SG30_SGD", "SPX500_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-SG30_SGD, -SG30_SGD_log1_price, -SG30_SGD_quantiles_1, -SG30_SGD_tangent_angle1)%>%
      dplyr::select(-SPX500_USD, -SPX500_USD_log2_price, -SPX500_USD_quantiles_2, -SPX500_USD_tangent_angle2)

    copula_data_UK100_GBP_SPX500_USD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("UK100_GBP", "SPX500_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-SPX500_USD, -SPX500_USD_log2_price, -SPX500_USD_quantiles_2, -SPX500_USD_tangent_angle2)

    copula_data_UK100_GBP_EU50_EUR <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("UK100_GBP", "EU50_EUR"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-UK100_GBP, -UK100_GBP_log1_price, -UK100_GBP_quantiles_1, -UK100_GBP_tangent_angle1)%>%
      dplyr::select(-EU50_EUR, -EU50_EUR_log2_price, -EU50_EUR_quantiles_2, -EU50_EUR_tangent_angle2)

    copula_data_UK100_GBP_UK10YB_GBP <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("UK100_GBP", "UK10YB_GBP"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-UK100_GBP, -UK100_GBP_log1_price, -UK100_GBP_quantiles_1, -UK100_GBP_tangent_angle1)

    copula_data_UK100_GBP_XAU_USD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("UK100_GBP", "XAU_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-UK100_GBP, -UK100_GBP_log1_price, -UK100_GBP_quantiles_1, -UK100_GBP_tangent_angle1)%>%
      dplyr::select(-XAU_USD, -XAU_USD_log2_price, -XAU_USD_quantiles_2, -XAU_USD_tangent_angle2)

    copula_data_USB10Y_USD_SPX500_USD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("USB10Y_USD", "SPX500_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-SPX500_USD, -SPX500_USD_log2_price, -SPX500_USD_quantiles_2, -SPX500_USD_tangent_angle2)

    # copula_data_USB02Y_USD_SPX500_USD <-
    #   estimating_dual_copula(
    #     asset_data_to_use = SPX_US2000_XAG,
    #     asset_to_use = c("USB02Y_USD", "SPX500_USD"),
    #     price_col = "Open",
    #     rolling_period = 100,
    #     samples_for_MLE = 0.15,
    #     test_samples = 0.85
    #   ) %>%
    #   dplyr::select(-SPX500_USD, -SPX500_USD_log2_price, -SPX500_USD_quantiles_2, -SPX500_USD_tangent_angle2)

    # copula_data_USB02Y_USD_USB10Y_USD <-
    #   estimating_dual_copula(
    #     asset_data_to_use = SPX_US2000_XAG,
    #     asset_to_use = c("USB02Y_USD", "USB10Y_USD"),
    #     price_col = "Open",
    #     rolling_period = 100,
    #     samples_for_MLE = 0.15,
    #     test_samples = 0.85
    #   ) %>%
    #   dplyr::select(-USB02Y_USD, -USB02Y_USD_log1_price, -USB02Y_USD_quantiles_1, -USB02Y_USD_tangent_angle1) %>%
    #   dplyr::select(-USB10Y_USD, -USB10Y_USD_log2_price, -USB10Y_USD_quantiles_2, -USB10Y_USD_tangent_angle2)


    copula_data_CH20_CHF_SPX500_USD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("CH20_CHF", "SPX500_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-SPX500_USD, -SPX500_USD_log2_price, -SPX500_USD_quantiles_2, -SPX500_USD_tangent_angle2)

    copula_data_CH20_CHF_XAU_USD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("CH20_CHF", "XAU_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-CH20_CHF, -CH20_CHF_log1_price, -CH20_CHF_quantiles_1, -CH20_CHF_tangent_angle1) %>%
      dplyr::select(-XAU_USD, -XAU_USD_log2_price, -XAU_USD_quantiles_2, -XAU_USD_tangent_angle2)

    copula_data_CH20_CHF_EU50_EUR <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("CH20_CHF", "EU50_EUR"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-CH20_CHF, -CH20_CHF_log1_price, -CH20_CHF_quantiles_1, -CH20_CHF_tangent_angle1) %>%
      dplyr::select(-EU50_EUR, -EU50_EUR_log2_price, -EU50_EUR_quantiles_2, -EU50_EUR_tangent_angle2)

    copula_data_CH20_CHF_FR40_EUR <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("CH20_CHF", "FR40_EUR"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-CH20_CHF, -CH20_CHF_log1_price, -CH20_CHF_quantiles_1, -CH20_CHF_tangent_angle1) %>%
      dplyr::select(-FR40_EUR, -FR40_EUR_log2_price, -FR40_EUR_quantiles_2, -FR40_EUR_tangent_angle2)

    copula_data_FR40_EUR_SPX500_USD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("FR40_EUR", "SPX500_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-FR40_EUR, -FR40_EUR_log1_price, -FR40_EUR_quantiles_1, -FR40_EUR_tangent_angle1) %>%
      dplyr::select(-SPX500_USD, -SPX500_USD_log2_price, -SPX500_USD_quantiles_2, -SPX500_USD_tangent_angle2)

    copula_data_FR40_EUR_EUR_USD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("FR40_EUR", "EUR_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-FR40_EUR, -FR40_EUR_log1_price, -FR40_EUR_quantiles_1, -FR40_EUR_tangent_angle1)

    copula_data_FR40_EUR_XAU_USD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("FR40_EUR", "XAU_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-FR40_EUR, -FR40_EUR_log1_price, -FR40_EUR_quantiles_1, -FR40_EUR_tangent_angle1) %>%
      dplyr::select(-XAU_USD, -XAU_USD_log2_price, -XAU_USD_quantiles_2, -XAU_USD_tangent_angle2)

    copula_data_FR40_EUR_XAG_USD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("FR40_EUR", "XAG_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-FR40_EUR, -FR40_EUR_log1_price, -FR40_EUR_quantiles_1, -FR40_EUR_tangent_angle1) %>%
      dplyr::select(-XAG_USD, -XAG_USD_log2_price, -XAG_USD_quantiles_2, -XAG_USD_tangent_angle2)

    copula_data_FR40_EUR_USB10Y_USD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("FR40_EUR", "USB10Y_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-FR40_EUR, -FR40_EUR_log1_price, -FR40_EUR_quantiles_1, -FR40_EUR_tangent_angle1) %>%
      dplyr::select(-USB10Y_USD, -USB10Y_USD_log2_price, -USB10Y_USD_quantiles_2, -USB10Y_USD_tangent_angle2)

    copula_data_UK100_GBP_GBP_USD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("UK100_GBP", "GBP_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-UK100_GBP, -UK100_GBP_log1_price, -UK100_GBP_quantiles_1, -UK100_GBP_tangent_angle1)%>%
      dplyr::select(-GBP_USD, -GBP_USD_log2_price, -GBP_USD_quantiles_2, -GBP_USD_tangent_angle2)

    copula_data_EUR_USD_GBP_USD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("EUR_USD", "GBP_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-EUR_USD, -EUR_USD_log1_price, -EUR_USD_quantiles_1, -EUR_USD_tangent_angle1)%>%
      dplyr::select(-GBP_USD, -GBP_USD_log2_price, -GBP_USD_quantiles_2, -GBP_USD_tangent_angle2)

    copula_data_EUR_USD_EU50_EUR <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("EUR_USD", "EU50_EUR"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-EUR_USD, -EUR_USD_log1_price, -EUR_USD_quantiles_1, -EUR_USD_tangent_angle1)%>%
      dplyr::select(-EU50_EUR, -EU50_EUR_log2_price, -EU50_EUR_quantiles_2, -EU50_EUR_tangent_angle2)

    copula_data_SPX500_USD_HK33_HKD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("SPX500_USD", "HK33_HKD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-SPX500_USD, -SPX500_USD_log1_price, -SPX500_USD_quantiles_1, -SPX500_USD_tangent_angle1)

    copula_data_EU50_EUR_HK33_HKD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("EU50_EUR", "HK33_HKD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-EU50_EUR, -EU50_EUR_log1_price, -EU50_EUR_quantiles_1, -EU50_EUR_tangent_angle1)%>%
      dplyr::select(-HK33_HKD, -HK33_HKD_log2_price, -HK33_HKD_quantiles_2, -HK33_HKD_tangent_angle2)

    copula_data_XAG_USD_HK33_HKD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("XAG_USD", "HK33_HKD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAG_USD, -XAG_USD_log1_price, -XAG_USD_quantiles_1, -XAG_USD_tangent_angle1)%>%
      dplyr::select(-HK33_HKD, -HK33_HKD_log2_price, -HK33_HKD_quantiles_2, -HK33_HKD_tangent_angle2)

    copula_data_US2000_USD_HK33_HKD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("US2000_USD", "HK33_HKD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-US2000_USD, -US2000_USD_log1_price, -US2000_USD_quantiles_1, -US2000_USD_tangent_angle1)%>%
      dplyr::select(-HK33_HKD, -HK33_HKD_log2_price, -HK33_HKD_quantiles_2, -HK33_HKD_tangent_angle2)

    copula_data_XAU_USD_HK33_HKD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("XAU_USD", "HK33_HKD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAU_USD, -XAU_USD_log1_price, -XAU_USD_quantiles_1, -XAU_USD_tangent_angle1)%>%
      dplyr::select(-HK33_HKD, -HK33_HKD_log2_price, -HK33_HKD_quantiles_2, -HK33_HKD_tangent_angle2)

    copula_data_US2000_USD_UK100_GBP <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("US2000_USD", "UK100_GBP"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-US2000_USD, -US2000_USD_log1_price, -US2000_USD_quantiles_1, -US2000_USD_tangent_angle1)%>%
      dplyr::select(-UK100_GBP, -UK100_GBP_log2_price, -UK100_GBP_quantiles_2, -UK100_GBP_tangent_angle2)

    copula_data_US2000_USD_FR40_EUR <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("US2000_USD", "FR40_EUR"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-US2000_USD, -US2000_USD_log1_price, -US2000_USD_quantiles_1, -US2000_USD_tangent_angle1)%>%
      dplyr::select(-FR40_EUR, -FR40_EUR_log2_price, -FR40_EUR_quantiles_2, -FR40_EUR_tangent_angle2)

    binary_data_for_post_model <-
      actual_wins_losses %>%
      rename(Date = dates, Asset = asset) %>%
      filter(profit_factor == profit_value_var)%>%
      filter(stop_factor == stop_value_var) %>%
      mutate(
        bin_var =
          case_when(
            trade_start_prices > trade_end_prices & trade_col == "Short" ~ "win",
            trade_start_prices <= trade_end_prices & trade_col == "Short" ~ "loss",

            trade_start_prices < trade_end_prices & trade_col == "Long" ~ "win",
            trade_start_prices >= trade_end_prices & trade_col == "Long" ~ "loss"

          )
      ) %>%
      dplyr::select(Date, bin_var, Asset, trade_col,
                    profit_factor, stop_factor,
                    trade_start_prices, trade_end_prices,
                    starting_stop_value, starting_profit_value)

    copula_data_macro <-
      SPX_US2000_XAG %>%
      dplyr::select(Date,Asset, Price, High, Low, Open ) %>%
      left_join(copula_data) %>%
      left_join(copula_data_SPX_XAU) %>%
      left_join(copula_data_US2000_XAU) %>%
      left_join(copula_data_SPX_AU200) %>%
      left_join(copula_data_US2000_AU200) %>%
      left_join(copula_data_AU200_AUD_XAG) %>%
      left_join(copula_data_SPX500_EUR50) %>%
      left_join(copula_data_US2000_USD_EUR50) %>%
      left_join(copula_data_XAU_USD_EU50_EUR) %>%
      left_join(copula_data_AU200_AUD_EU50_EUR)%>%
      left_join(copula_data_SG30_XAU_USD)%>%
      left_join(copula_data_SG30_SPX500_USD) %>%
      left_join(copula_data_UK100_GBP_SPX500_USD) %>%
      left_join(copula_data_UK100_GBP_EU50_EUR) %>%
      left_join(copula_data_UK100_GBP_UK10YB_GBP) %>%
      left_join(copula_data_UK100_GBP_XAU_USD) %>%
      left_join(copula_data_USB10Y_USD_SPX500_USD) %>%
      # left_join(copula_data_USB02Y_USD_SPX500_USD) %>%
      # left_join(copula_data_USB02Y_USD_USB10Y_USD) %>%
      left_join(copula_data_CH20_CHF_SPX500_USD) %>%
      left_join(copula_data_CH20_CHF_XAU_USD) %>%
      left_join(copula_data_CH20_CHF_EU50_EUR) %>%
      left_join(copula_data_CH20_CHF_FR40_EUR) %>%
      left_join(copula_data_FR40_EUR_SPX500_USD) %>%
      left_join(copula_data_FR40_EUR_EUR_USD) %>%
      left_join(copula_data_FR40_EUR_XAU_USD) %>%
      left_join(copula_data_FR40_EUR_XAG_USD) %>%
      left_join(copula_data_FR40_EUR_USB10Y_USD) %>%
      left_join(copula_data_UK100_GBP_GBP_USD) %>%
      left_join(copula_data_EUR_USD_GBP_USD) %>%
      left_join(copula_data_EUR_USD_EU50_EUR) %>%
      left_join(copula_data_SPX500_USD_HK33_HKD) %>%
      left_join(copula_data_EU50_EUR_HK33_HKD) %>%
      left_join(copula_data_XAG_USD_HK33_HKD) %>%
      left_join(copula_data_US2000_USD_HK33_HKD) %>%
      left_join(copula_data_XAU_USD_HK33_HKD) %>%
      left_join(copula_data_US2000_USD_UK100_GBP) %>%
      left_join(copula_data_US2000_USD_FR40_EUR) %>%
      left_join(binary_data_for_post_model) %>%
      mutate(Date_for_join = as_date(Date)) %>%
      left_join(
        aus_macro_data %>%
          rename(Date_for_join = date)
      ) %>%
      # left_join(
      #   nzd_macro_data %>%
      #     rename(Date_for_join = date)
      # ) %>%
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
      group_by(Asset) %>%
      arrange(Date, .by_group = TRUE) %>%
      group_by(Asset) %>%
      fill(matches(all_macro_vars, ignore.case = FALSE), .direction = "down") %>%
      group_by(Asset) %>%
      arrange(Date, .by_group = TRUE) %>%
      group_by(Asset) %>%
      fill(everything(), .direction = "down") %>%
      ungroup() %>%
      mutate(hour_of_day = lubridate::hour(Date) %>% as.numeric(),
             day_of_week = lubridate::wday(Date) %>% as.numeric())

    max_date_in_testing_data <- copula_data_macro %>% pull(Date) %>% max(na.rm = T)
    message(glue::glue("Max date in Complete data: {max_date_in_testing_data}"))

    min_allowable_date <-
      copula_data_macro %>%
      ungroup() %>%
      filter(if_all(everything(), ~ !is.na(.))) %>%
      pull(Date) %>% min()

    gc()

    copula_data_macro <-
      copula_data_macro %>%
      ungroup() %>%
      filter(Date >= min_allowable_date)

    gc()

    lm_quant_vars <- names(copula_data_macro) %>% keep(~ str_detect(.x,"quantiles|tangent|cor"))

    if(use_PCA_vars == TRUE) {
      lm_vars1 <- c(PC_macro_vars, lm_quant_vars,
                    "fib_1", "fib_2",
                    "fib_3", "fib_4",
                    "fib_5", "fib_6",
                    "lagged_var_1", "lagged_var_2",
                    "lagged_var_3", "lagged_var_5",
                    "lagged_var_8",
                    "lagged_var_13", "lagged_var_21",
                    "lagged_var_3_ma", "lagged_var_5_ma",
                    "lagged_var_8_ma", "lagged_var_13_ma",
                    "lagged_var_21_ma"
                    # "hour_of_day", "day_of_week"
      )
    } else {
      lm_vars1 <- c(all_macro_vars, lm_quant_vars,
                    "fib_1", "fib_2",
                    "fib_3", "fib_4",
                    "fib_5", "fib_6",
                    "lagged_var_1", "lagged_var_2",
                    "lagged_var_3", "lagged_var_5",
                    "lagged_var_8",
                    "lagged_var_13", "lagged_var_21",
                    "lagged_var_3_ma", "lagged_var_5_ma",
                    "lagged_var_8_ma", "lagged_var_13_ma",
                    "lagged_var_21_ma"
                    # "hour_of_day", "day_of_week"
      )
    }

    return(
      list(
        "copula_data_macro" = copula_data_macro,
        "lm_vars1" =
          lm_vars1 %>%
          keep( ~ !str_detect(.x, "sd") & !str_detect(.x, "tangent") )
      )
    )

  }


#' create_NN_METALS_IDINCES_DAILY_QUANT
#'
#' @param METALS_INDICES
#' @param raw_macro_data
#' @param actual_wins_losses
#' @param lag_days
#' @param stop_value_var
#' @param profit_value_var
#'
#' @return
#' @export
#'
#' @examples
create_NN_METALS_IDINCES_DAILY_QUANT <-
  function(METALS_INDICES = asset_data_daily_raw_ask %>% filter(str_detect(Asset, "XAG|XAU|SPX500|US2000|UK100|AU200_AUD|SG30_SGD|EU50_EUR")),
           raw_macro_data,
           actual_wins_losses = actual_wins_losses,
           lag_days = 1,
           stop_value_var = 0.5,
           profit_value_var = 1) {

    copula_data <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("SPX500_USD", "US2000_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      )

    copula_data_SPX_XAU <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("SPX500_USD", "XAU_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-SPX500_USD, -SPX500_USD_log1_price, -SPX500_USD_quantiles_1, -SPX500_USD_tangent_angle1)

    copula_data_US2000_XAU <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("US2000_USD", "XAU_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-US2000_USD, -US2000_USD_log1_price, -US2000_USD_quantiles_1, -US2000_USD_tangent_angle1)%>%
      dplyr::select(-XAU_USD, -XAU_USD_log2_price, -XAU_USD_quantiles_2, -XAU_USD_tangent_angle2)

    copula_data_SPX_AU200 <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("SPX500_USD", "AU200_AUD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-SPX500_USD, -SPX500_USD_log1_price, -SPX500_USD_quantiles_1, -SPX500_USD_tangent_angle1)

    copula_data_US2000_AU200 <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("US2000_USD", "AU200_AUD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-US2000_USD, -US2000_USD_log1_price, -US2000_USD_quantiles_1, -US2000_USD_tangent_angle1)%>%
      dplyr::select(-AU200_AUD, -AU200_AUD_log2_price, -AU200_AUD_quantiles_2, -AU200_AUD_tangent_angle2)

    copula_data_AU200_AUD_XAG <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("AU200_AUD", "XAU_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-AU200_AUD, -AU200_AUD_log1_price, -AU200_AUD_quantiles_1, -AU200_AUD_tangent_angle1)%>%
      dplyr::select(-XAU_USD, -XAU_USD_log2_price, -XAU_USD_quantiles_2, -XAU_USD_tangent_angle2)

    copula_data_SPX500_EUR50 <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("SPX500_USD", "EU50_EUR"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-SPX500_USD, -SPX500_USD_log1_price, -SPX500_USD_quantiles_1, -SPX500_USD_tangent_angle1)

    copula_data_US2000_USD_EUR50 <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("US2000_USD", "EU50_EUR"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-US2000_USD, -US2000_USD_log1_price, -US2000_USD_quantiles_1, -US2000_USD_tangent_angle1)%>%
      dplyr::select(-EU50_EUR, -EU50_EUR_log2_price, -EU50_EUR_quantiles_2, -EU50_EUR_tangent_angle2)

    copula_data_XAU_USD_EU50_EUR <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("XAU_USD", "EU50_EUR"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAU_USD, -XAU_USD_log1_price, -XAU_USD_quantiles_1, -XAU_USD_tangent_angle1)%>%
      dplyr::select(-EU50_EUR, -EU50_EUR_log2_price, -EU50_EUR_quantiles_2, -EU50_EUR_tangent_angle2)

    copula_data_XAU_USD_EU50_EUR <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("XAG_USD", "EU50_EUR"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAG_USD, -XAG_USD_log1_price, -XAG_USD_quantiles_1, -XAG_USD_tangent_angle1)%>%
      dplyr::select(-EU50_EUR, -EU50_EUR_log2_price, -EU50_EUR_quantiles_2, -EU50_EUR_tangent_angle2)

    copula_data_AU200_AUD_EU50_EUR <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("AU200_AUD", "EU50_EUR"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-AU200_AUD, -AU200_AUD_log1_price, -AU200_AUD_quantiles_1, -AU200_AUD_tangent_angle1)%>%
      dplyr::select(-EU50_EUR, -EU50_EUR_log2_price, -EU50_EUR_quantiles_2, -EU50_EUR_tangent_angle2)

    copula_data_SG30_XAU_USD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("SG30_SGD", "XAU_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAU_USD, -XAU_USD_log2_price, -XAU_USD_quantiles_2, -XAU_USD_tangent_angle2)

    copula_data_SG30_SPX500_USD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("SG30_SGD", "SPX500_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-SG30_SGD, -SG30_SGD_log1_price, -SG30_SGD_quantiles_1, -SG30_SGD_tangent_angle1)%>%
      dplyr::select(-SPX500_USD, -SPX500_USD_log2_price, -SPX500_USD_quantiles_2, -SPX500_USD_tangent_angle2)

    copula_data_XAG_USD_EUR <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("XAG_USD", "XAG_EUR"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAG_USD, -XAG_USD_log1_price, -XAG_USD_quantiles_1, -XAG_USD_tangent_angle1)

    copula_data_XAG_USD_AUD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("XAG_USD", "XAG_AUD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAG_USD, -XAG_USD_log1_price, -XAG_USD_quantiles_1, -XAG_USD_tangent_angle1)

    copula_data_XAG_USD_JPY <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("XAG_USD", "XAG_JPY"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAG_USD, -XAG_USD_log1_price, -XAG_USD_quantiles_1, -XAG_USD_tangent_angle1)

    copula_data_XAU_USD_EUR <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("XAU_USD", "XAU_EUR"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAU_USD, -XAU_USD_log1_price, -XAU_USD_quantiles_1, -XAU_USD_tangent_angle1)

    copula_data_XAU_USD_JPY <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("XAU_USD", "XAU_AUD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAU_USD, -XAU_USD_log1_price, -XAU_USD_quantiles_1, -XAU_USD_tangent_angle1)


    copula_data_XAU_USD_AUD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("XAU_USD", "XAU_JPY"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAU_USD, -XAU_USD_log1_price, -XAU_USD_quantiles_1, -XAU_USD_tangent_angle1)

    copula_data_UK100_SPX <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("UK100_GBP", "SPX500_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-SPX500_USD, -SPX500_USD_log2_price, -SPX500_USD_quantiles_2, -SPX500_USD_tangent_angle2)

    copula_data_UK100_EU50 <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("UK100_GBP", "EU50_EUR"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-EU50_EUR, -EU50_EUR_log2_price, -EU50_EUR_quantiles_2, -EU50_EUR_tangent_angle2) %>%
      dplyr::select(-UK100_GBP, -UK100_GBP_log1_price, -UK100_GBP_quantiles_1, -UK100_GBP_tangent_angle1)

    copula_data_UK100_XAU <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("UK100_GBP", "XAU_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-UK100_GBP, -UK100_GBP_log1_price, -UK100_GBP_quantiles_1, -UK100_GBP_tangent_angle1) %>%
      dplyr::select(-XAU_USD, -XAU_USD_log2_price, -XAU_USD_quantiles_2, -XAU_USD_tangent_angle2)

    copula_data_UK100_US2000 <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("UK100_GBP", "US2000_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-UK100_GBP, -UK100_GBP_log1_price, -UK100_GBP_quantiles_1, -UK100_GBP_tangent_angle1) %>%
      dplyr::select(-US2000_USD, -US2000_USD_log2_price, -US2000_USD_quantiles_2, -US2000_USD_tangent_angle2)

    copula_data_UK100_XAUGBP <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("UK100_GBP", "XAU_GBP"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-UK100_GBP, -UK100_GBP_log1_price, -UK100_GBP_quantiles_1, -UK100_GBP_tangent_angle1)

    copula_data_UK100_XAGEUR <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("UK100_GBP", "XAG_EUR"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-UK100_GBP, -UK100_GBP_log1_price, -UK100_GBP_quantiles_1, -UK100_GBP_tangent_angle1) %>%
      dplyr::select(-XAG_EUR, -XAG_EUR_log2_price, -XAG_EUR_quantiles_2, -XAG_EUR_tangent_angle2)

    binary_data_for_post_model <-
      actual_wins_losses %>%
      rename(Date = dates, Asset = asset) %>%
      filter(profit_factor == profit_value_var)%>%
      filter(stop_factor == stop_value_var) %>%
      mutate(
        bin_var =
          case_when(
            trade_start_prices > trade_end_prices & trade_col == "Short" ~ "win",
            trade_start_prices <= trade_end_prices & trade_col == "Short" ~ "loss",

            trade_start_prices < trade_end_prices & trade_col == "Long" ~ "win",
            trade_start_prices >= trade_end_prices & trade_col == "Long" ~ "loss"

          )
      ) %>%
      dplyr::select(Date, bin_var, Asset, trade_col,
                    profit_factor, stop_factor,
                    trade_start_prices, trade_end_prices,
                    starting_stop_value, starting_profit_value)

    copula_data_macro <-
      METALS_INDICES %>%
      dplyr::select(Date,Asset, Price, High, Low, Open ) %>%
      find_pivots_fib_max_min(how_far_back = 20) %>%
      find_pivots_fib_max_min(how_far_back = 5) %>%
      left_join(copula_data) %>%
      left_join(copula_data_SPX_XAU) %>%
      left_join(copula_data_US2000_XAU) %>%
      left_join(copula_data_SPX_AU200) %>%
      left_join(copula_data_US2000_AU200) %>%
      left_join(copula_data_AU200_AUD_XAG) %>%
      left_join(copula_data_SPX500_EUR50) %>%
      left_join(copula_data_US2000_USD_EUR50) %>%
      left_join(copula_data_XAU_USD_EU50_EUR) %>%
      left_join(copula_data_AU200_AUD_EU50_EUR)%>%
      left_join(copula_data_SG30_XAU_USD)%>%
      left_join(copula_data_SG30_SPX500_USD) %>%
      left_join(copula_data_XAG_USD_EUR) %>%
      left_join(copula_data_XAG_USD_AUD) %>%
      left_join(copula_data_XAG_USD_JPY) %>%
      left_join(copula_data_XAU_USD_EUR) %>%
      left_join(copula_data_XAU_USD_AUD) %>%
      left_join(copula_data_XAU_USD_JPY) %>%
      left_join(copula_data_UK100_SPX) %>%
      left_join(copula_data_UK100_EU50) %>%
      left_join(copula_data_UK100_XAU) %>%
      left_join(copula_data_UK100_US2000) %>%
      left_join(copula_data_UK100_XAUGBP) %>%
      left_join(copula_data_UK100_XAGEUR) %>%
      left_join(binary_data_for_post_model) %>%
      mutate(Date_for_join = as_date(Date)) %>%
      group_by(Asset) %>%
      arrange(Date, .by_group = TRUE) %>%
      group_by(Asset) %>%
      # fill(matches(all_macro_vars, ignore.case = FALSE), .direction = "down") %>%
      mutate(hour_of_day = lubridate::hour(Date) %>% as.numeric(),
             day_of_week = lubridate::wday(Date) %>% as.numeric())

    max_date_in_testing_data <- copula_data_macro %>% pull(Date) %>% max(na.rm = T)
    message(glue::glue("Max date in Complete data: {max_date_in_testing_data}"))

    lm_quant_vars <- names(copula_data_macro) %>% keep(~ str_detect(.x,"quantiles|tangent|cor|perc_line"))

    lm_vars1 <- c(lm_quant_vars,
                  "fib_1", "fib_2",
                  "fib_3", "fib_4",
                  "fib_5", "fib_6",
                  "lagged_var_1", "lagged_var_2",
                  "lagged_var_3", "lagged_var_5",
                  "lagged_var_8",
                  "lagged_var_13", "lagged_var_21",
                  "lagged_var_3_ma", "lagged_var_5_ma",
                  "lagged_var_8_ma", "lagged_var_13_ma",
                  "lagged_var_21_ma"
                  # "hour_of_day", "day_of_week"
    )

    return(
      list(
        "copula_data_macro" = copula_data_macro,
        "lm_vars1" =
          lm_vars1 %>%
          keep( ~ !str_detect(.x, "sd") & !str_detect(.x, "tangent") )
      )
    )

  }


#' create_NN_METALS_IDINCES_DAILY_QUANT
#'
#' @param METALS_INDICES
#' @param raw_macro_data
#' @param actual_wins_losses
#' @param lag_days
#' @param stop_value_var
#' @param profit_value_var
#'
#' @return
#' @export
#'
#' @examples
create_NN_CRYPTO_MET_CUR_DAILY_QUANT <-
  function(METALS_INDICES = asset_data_daily_raw_ask %>%
             filter(str_detect(Asset, "XAG|XAU|SPX500|USB05Y_USD|USB10Y_USD|BTC_USD|LTC_USD|BCH_USD|USD_JPY|AUD_USD|EUR_USD|GBP_USD|USD_MXN|USD_SEK|USD_CHF")),
           raw_macro_data,
           actual_wins_losses = actual_wins_losses,
           lag_days = 1,
           stop_value_var = 0.5,
           profit_value_var = 1) {

    copula_data_SPX_XAU <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("SPX500_USD", "XAU_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      )

    copula_data_XAG_USD_EUR <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("XAG_USD", "XAG_EUR"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAG_USD, -XAG_USD_log1_price, -XAG_USD_quantiles_1, -XAG_USD_tangent_angle1)

    copula_data_XAG_USD_AUD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("XAG_USD", "XAG_AUD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAG_USD, -XAG_USD_log1_price, -XAG_USD_quantiles_1, -XAG_USD_tangent_angle1)

    copula_data_XAG_USD_JPY <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("XAG_USD", "XAG_JPY"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAG_USD, -XAG_USD_log1_price, -XAG_USD_quantiles_1, -XAG_USD_tangent_angle1)

    copula_data_XAU_USD_EUR <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("XAU_USD", "XAU_EUR"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAU_USD, -XAU_USD_log1_price, -XAU_USD_quantiles_1, -XAU_USD_tangent_angle1)

    copula_data_XAU_USD_JPY <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("XAU_USD", "XAU_AUD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAU_USD, -XAU_USD_log1_price, -XAU_USD_quantiles_1, -XAU_USD_tangent_angle1)


    copula_data_XAU_USD_AUD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("XAU_USD", "XAU_JPY"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAU_USD, -XAU_USD_log1_price, -XAU_USD_quantiles_1, -XAU_USD_tangent_angle1)

    copula_data_XAU_USD_USD_JPY <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("XAU_USD", "USD_JPY"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAU_USD, -XAU_USD_log1_price, -XAU_USD_quantiles_1, -XAU_USD_tangent_angle1)

    copula_data_XAU_USD_USD_JPY <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("XAG_USD", "USD_JPY"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAG_USD, -XAG_USD_log1_price, -XAG_USD_quantiles_1, -XAG_USD_tangent_angle1) %>%
      dplyr::select(-USD_JPY, -USD_JPY_log2_price, -USD_JPY_quantiles_2, -USD_JPY_tangent_angle2)

    copula_data_XAG_JPY_USD_JPY <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("XAU_JPY", "USD_JPY"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAU_JPY, -XAU_JPY_log1_price, -XAU_JPY_quantiles_1, -XAU_JPY_tangent_angle1) %>%
      dplyr::select(-USD_JPY, -USD_JPY_log2_price, -USD_JPY_quantiles_2, -USD_JPY_tangent_angle2)

    copula_data_BTC_USD_SPX500_USD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("BTC_USD", "SPX500_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-BTC_USD, -BTC_USD_log1_price, -BTC_USD_quantiles_1, -BTC_USD_tangent_angle1)%>%
      dplyr::select(-SPX500_USD, -SPX500_USD_log2_price, -SPX500_USD_quantiles_2, -SPX500_USD_tangent_angle2)

    copula_data_BTC_USD_XAU_USD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("BTC_USD", "XAU_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-BTC_USD, -BTC_USD_log1_price, -BTC_USD_quantiles_1, -BTC_USD_tangent_angle1)%>%
      dplyr::select(-XAU_USD, -XAU_USD_log2_price, -XAU_USD_quantiles_2, -XAU_USD_tangent_angle2)

    copula_data_BTC_USD_XAG_USD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("BTC_USD", "XAG_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-BTC_USD, -BTC_USD_log1_price, -BTC_USD_quantiles_1, -BTC_USD_tangent_angle1)%>%
      dplyr::select(-XAG_USD, -XAG_USD_log2_price, -XAG_USD_quantiles_2, -XAG_USD_tangent_angle2)

    copula_data_USD_MXN_SPX500_USD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("USD_MXN", "SPX500_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-USD_MXN, -USD_MXN_log1_price, -USD_MXN_quantiles_1, -USD_MXN_tangent_angle1)%>%
      dplyr::select(-SPX500_USD, -SPX500_USD_log2_price, -SPX500_USD_quantiles_2, -SPX500_USD_tangent_angle2)

    copula_data_USD_MXN_BTC_USD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("BTC_USD" , "USD_MXN"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-BTC_USD, -BTC_USD_log1_price, -BTC_USD_quantiles_1, -BTC_USD_tangent_angle1) %>%
      dplyr::select(-USD_MXN, -USD_MXN_log2_price, -USD_MXN_quantiles_2, -USD_MXN_tangent_angle2)


    copula_data_USD_MXN_USD_JPY <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("USD_MXN", "USD_JPY"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-USD_MXN, -USD_MXN_log1_price, -USD_MXN_quantiles_1, -USD_MXN_tangent_angle1)%>%
      dplyr::select(-USD_JPY, -USD_JPY_log2_price, -USD_JPY_quantiles_2, -USD_JPY_tangent_angle2)

    copula_data_USD_MXN_XAU_USD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("USD_MXN", "XAU_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-USD_MXN, -USD_MXN_log1_price, -USD_MXN_quantiles_1, -USD_MXN_tangent_angle1)%>%
      dplyr::select(-XAU_USD, -XAU_USD_log2_price, -XAU_USD_quantiles_2, -XAU_USD_tangent_angle2)

    copula_data_USD_MXN_USD_CHF <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("USD_MXN", "USD_CHF"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-USD_MXN, -USD_MXN_log1_price, -USD_MXN_quantiles_1, -USD_MXN_tangent_angle1)%>%
      dplyr::select(-USD_CHF, -USD_CHF_log2_price, -USD_CHF_quantiles_2, -USD_CHF_tangent_angle2)

    copula_data_USB05_USB10 <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("USB05Y_USD", "USB10Y_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      )

    copula_data_USB05_SPX500_USD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("USB05Y_USD", "SPX500_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-USB05Y_USD, -USB05Y_USD_log1_price, -USB05Y_USD_quantiles_1, -USB05Y_USD_tangent_angle1)%>%
      dplyr::select(-SPX500_USD, -SPX500_USD_log2_price, -SPX500_USD_quantiles_2, -SPX500_USD_tangent_angle2)

    copula_data_USB10_SPX500_USD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("USB10Y_USD", "SPX500_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-USB10Y_USD, -USB10Y_USD_log1_price, -USB10Y_USD_quantiles_1, -USB10Y_USD_tangent_angle1)%>%
      dplyr::select(-SPX500_USD, -SPX500_USD_log2_price, -SPX500_USD_quantiles_2, -SPX500_USD_tangent_angle2)

    copula_data_USB10_XAU_USD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("USB10Y_USD", "XAU_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-USB10Y_USD, -USB10Y_USD_log1_price, -USB10Y_USD_quantiles_1, -USB10Y_USD_tangent_angle1)%>%
      dplyr::select(-XAU_USD, -XAU_USD_log2_price, -XAU_USD_quantiles_2, -XAU_USD_tangent_angle2)

    copula_data_USD_SEK_USD_CHF <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("USD_SEK", "USD_CHF"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-USD_SEK, -USD_SEK_log1_price, -USD_SEK_quantiles_1, -USD_SEK_tangent_angle1)%>%
      dplyr::select(-USD_CHF, -USD_CHF_log2_price, -USD_CHF_quantiles_2, -USD_CHF_tangent_angle2)

    copula_data_USD_SEK_EUR_USD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("USD_SEK", "EUR_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-USD_SEK, -USD_SEK_log1_price, -USD_SEK_quantiles_1, -USD_SEK_tangent_angle1)%>%
      dplyr::select(-EUR_USD, -EUR_USD_log2_price, -EUR_USD_quantiles_2, -EUR_USD_tangent_angle2)

    copula_data_USD_CHF_EUR_USD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("USD_CHF", "EUR_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-USD_CHF, -USD_CHF_log1_price, -USD_CHF_quantiles_1, -USD_CHF_tangent_angle1)%>%
      dplyr::select(-EUR_USD, -EUR_USD_log2_price, -EUR_USD_quantiles_2, -EUR_USD_tangent_angle2)

    copula_data_USB10Y_USD_EUR_USD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("USB10Y_USD", "EUR_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-USB10Y_USD, -USB10Y_USD_log1_price, -USB10Y_USD_quantiles_1, -USB10Y_USD_tangent_angle1)%>%
      dplyr::select(-EUR_USD, -EUR_USD_log2_price, -EUR_USD_quantiles_2, -EUR_USD_tangent_angle2)

    copula_data_USB10Y_USD_USD_CHF <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("USB10Y_USD", "USD_CHF"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-USB10Y_USD, -USB10Y_USD_log1_price, -USB10Y_USD_quantiles_1, -USB10Y_USD_tangent_angle1)%>%
      dplyr::select(-USD_CHF, -USD_CHF_log2_price, -USD_CHF_quantiles_2, -USD_CHF_tangent_angle2)

    binary_data_for_post_model <-
      actual_wins_losses %>%
      rename(Date = dates, Asset = asset) %>%
      filter(profit_factor == profit_value_var)%>%
      filter(stop_factor == stop_value_var) %>%
      mutate(
        bin_var =
          case_when(
            trade_start_prices > trade_end_prices & trade_col == "Short" ~ "win",
            trade_start_prices <= trade_end_prices & trade_col == "Short" ~ "loss",

            trade_start_prices < trade_end_prices & trade_col == "Long" ~ "win",
            trade_start_prices >= trade_end_prices & trade_col == "Long" ~ "loss"

          )
      ) %>%
      dplyr::select(Date, bin_var, Asset, trade_col,
                    profit_factor, stop_factor,
                    trade_start_prices, trade_end_prices,
                    starting_stop_value, starting_profit_value)

    copula_data_macro <-
      METALS_INDICES %>%
      dplyr::select(Date,Asset, Price, High, Low, Open ) %>%
      # find_pivots_fib_max_min(how_far_back = 20) %>%
      # find_pivots_fib_max_min(how_far_back = 5) %>%
      left_join(copula_data_SPX_XAU) %>%
      left_join(copula_data_XAG_USD_EUR) %>%
      left_join(copula_data_XAG_USD_AUD) %>%
      left_join(copula_data_XAG_USD_JPY) %>%
      left_join(copula_data_XAU_USD_EUR) %>%
      left_join(copula_data_XAU_USD_JPY) %>%
      left_join(copula_data_XAU_USD_USD_JPY) %>%
      left_join(copula_data_XAG_JPY_USD_JPY) %>%
      left_join(copula_data_BTC_USD_SPX500_USD) %>%
      left_join(copula_data_BTC_USD_XAU_USD) %>%
      left_join(copula_data_BTC_USD_XAG_USD) %>%
      left_join(copula_data_USD_MXN_SPX500_USD) %>%
      left_join(copula_data_USD_MXN_USD_JPY) %>%
      left_join(copula_data_USD_MXN_XAU_USD) %>%
      left_join(copula_data_USD_MXN_USD_CHF) %>%
      left_join(copula_data_USB05_USB10) %>%
      left_join(copula_data_USB05_SPX500_USD) %>%
      left_join(copula_data_USB10_SPX500_USD) %>%
      left_join(copula_data_USB10_XAU_USD) %>%
      left_join(copula_data_USD_SEK_USD_CHF) %>%
      left_join(copula_data_USD_SEK_EUR_USD) %>%
      left_join(copula_data_USD_CHF_EUR_USD) %>%
      left_join(copula_data_USB10Y_USD_EUR_USD) %>%
      left_join(copula_data_USB10Y_USD_USD_CHF) %>%
      left_join(binary_data_for_post_model) %>%
      mutate(Date_for_join = as_date(Date)) %>%
      group_by(Asset) %>%
      arrange(Date, .by_group = TRUE) %>%
      group_by(Asset) %>%
      # fill(matches(all_macro_vars, ignore.case = FALSE), .direction = "down") %>%
      mutate(hour_of_day = lubridate::hour(Date) %>% as.numeric(),
             day_of_week = lubridate::wday(Date) %>% as.numeric())

    max_date_in_testing_data <- copula_data_macro %>% pull(Date) %>% max(na.rm = T)
    message(glue::glue("Max date in Complete data: {max_date_in_testing_data}"))

    lm_quant_vars <- names(copula_data_macro) %>% keep(~ str_detect(.x,"quantiles|tangent|cor|perc_line"))

    lm_vars1 <- c(lm_quant_vars,
                  "fib_1", "fib_2",
                  "fib_3", "fib_4",
                  "fib_5", "fib_6",
                  "lagged_var_1", "lagged_var_2",
                  "lagged_var_3", "lagged_var_5",
                  "lagged_var_8",
                  "lagged_var_13", "lagged_var_21",
                  "lagged_var_3_ma", "lagged_var_5_ma",
                  "lagged_var_8_ma", "lagged_var_13_ma",
                  "lagged_var_21_ma"
                  # "hour_of_day", "day_of_week"
    )

    return(
      list(
        "copula_data_macro" = copula_data_macro,
        "lm_vars1" =
          lm_vars1 %>%
          keep( ~ !str_detect(.x, "sd") & !str_detect(.x, "tangent") )
      )
    )

  }



#' create_NN_COMMOD_INDECES_QUANT
#'
#' @param METALS_INDICES
#' @param raw_macro_data
#' @param actual_wins_losses
#' @param lag_days
#' @param stop_value_var
#' @param profit_value_var
#'
#' @return
#' @export
#'
#' @examples
create_NN_COMMOD_INDECES_QUANT <-
  function(METALS_INDICES = asset_data_daily_raw_ask %>%
             filter(str_detect(Asset, "XAG|XAU|SPX500|USB05Y_USD|USB10Y_USD|CH20_CHF|UK100_GBP|JP225Y_JPY|EU50_EUR|FR40_EUR|JP225_USD|AU200_AUD|ESPIX_EUR|SG30_SGD|DE10YB_EUR|WTICO_USD|BCO_USD|NATGAS_USD|XCU_USD")),
           raw_macro_data,
           actual_wins_losses = actual_wins_losses,
           lag_days = 1,
           stop_value_var = 0.5,
           profit_value_var = 1) {
    # %>%
    #   dplyr::select(-SPX500_USD, -SPX500_USD_log1_price, -SPX500_USD_quantiles_1, -SPX500_USD_tangent_angle1)%>%
    #   dplyr::select(-USD_CHF, -USD_CHF_log2_price, -USD_CHF_quantiles_2, -USD_CHF_tangent_angle2)

    copula_data_SPX_XAU <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("SPX500_USD", "XAU_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      )

    copula_data_SPX_USB05Y_USD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("SPX500_USD", "USB05Y_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-SPX500_USD, -SPX500_USD_log1_price, -SPX500_USD_quantiles_1, -SPX500_USD_tangent_angle1)

    copula_data_SPX_XAG_USD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("SPX500_USD", "XAG_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      )  %>%
      dplyr::select(-SPX500_USD, -SPX500_USD_log1_price, -SPX500_USD_quantiles_1, -SPX500_USD_tangent_angle1)

    copula_data_XAU_USD_XAG_USD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("XAU_USD", "XAG_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      )   %>%
      dplyr::select(-XAU_USD, -XAU_USD_log1_price, -XAU_USD_quantiles_1, -XAU_USD_tangent_angle1)%>%
      dplyr::select(-XAG_USD, -XAG_USD_log2_price, -XAG_USD_quantiles_2, -XAG_USD_tangent_angle2)

    copula_data_EU50_EUR_SPX500_USD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("EU50_EUR", "SPX500_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-SPX500_USD, -SPX500_USD_log2_price, -SPX500_USD_quantiles_2, -SPX500_USD_tangent_angle2)

    copula_data_EU50_EUR_CH20_CHF <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("EU50_EUR", "CH20_CHF"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-EU50_EUR, -EU50_EUR_log1_price, -EU50_EUR_quantiles_1, -EU50_EUR_tangent_angle1)%>%
      dplyr::select(-CH20_CHF, -CH20_CHF_log2_price, -CH20_CHF_quantiles_2, -CH20_CHF_tangent_angle2)

    copula_data_EU50_EUR_SPX500_USD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("EU50_EUR", "XAU_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-EU50_EUR, -EU50_EUR_log1_price, -EU50_EUR_quantiles_1, -EU50_EUR_tangent_angle1)%>%
      dplyr::select(-XAU_USD, -XAU_USD_log2_price, -XAU_USD_quantiles_2, -XAU_USD_tangent_angle2)

    copula_data_EU50_EUR_UK100_GBP <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("EU50_EUR", "UK100_GBP"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-EU50_EUR, -EU50_EUR_log1_price, -EU50_EUR_quantiles_1, -EU50_EUR_tangent_angle1)%>%
      dplyr::select(-UK100_GBP, -UK100_GBP_log2_price, -UK100_GBP_quantiles_2, -UK100_GBP_tangent_angle2)

    copula_data_EU50_EUR_FR40_EUR <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("EU50_EUR", "FR40_EUR"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-EU50_EUR, -EU50_EUR_log1_price, -EU50_EUR_quantiles_1, -EU50_EUR_tangent_angle1)

    copula_data_EU50_EUR_ESPIX_EUR <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c( "ESPIX_EUR", "EU50_EUR"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-EU50_EUR, -EU50_EUR_log2_price, -EU50_EUR_quantiles_2, -EU50_EUR_tangent_angle2)

    copula_data_SPX500_USD_USB05 <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("SPX500_USD", "USB10Y_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-SPX500_USD, -SPX500_USD_log1_price, -SPX500_USD_quantiles_1, -SPX500_USD_tangent_angle1)

    copula_data_EU50_EUR_XAU_EUR <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("EU50_EUR", "XAU_EUR"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-EU50_EUR, -EU50_EUR_log1_price, -EU50_EUR_quantiles_1, -EU50_EUR_tangent_angle1)

    copula_data_ESPIX_EUR_XAU_EUR <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("ESPIX_EUR", "XAU_EUR"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-ESPIX_EUR, -ESPIX_EUR_log1_price, -ESPIX_EUR_quantiles_1, -ESPIX_EUR_tangent_angle1)%>%
      dplyr::select(-XAU_EUR, -XAU_EUR_log2_price, -XAU_EUR_quantiles_2, -XAU_EUR_tangent_angle2)

    copula_data_CH20_CHF_XAU_CHF <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("CH20_CHF", "XAU_CHF"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-CH20_CHF, -CH20_CHF_log1_price, -CH20_CHF_quantiles_1, -CH20_CHF_tangent_angle1)%>%
      dplyr::select(-XAU_CHF, -XAU_CHF_log2_price, -XAU_CHF_quantiles_2, -XAU_CHF_tangent_angle2)

    copula_data_XAU_USD_XAU_CHF <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("XAU_USD", "XAU_CHF"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAU_USD, -XAU_USD_log1_price, -XAU_USD_quantiles_1, -XAU_USD_tangent_angle1)%>%
      dplyr::select(-XAU_CHF, -XAU_CHF_log2_price, -XAU_CHF_quantiles_2, -XAU_CHF_tangent_angle2)

    copula_data_WTICO_USD_BCO_USD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("WTICO_USD", "BCO_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-WTICO_USD, -WTICO_USD_log1_price, -WTICO_USD_quantiles_1, -WTICO_USD_tangent_angle1)%>%
      dplyr::select(-BCO_USD, -BCO_USD_log2_price, -BCO_USD_quantiles_2, -BCO_USD_tangent_angle2)

    copula_data_WTICO_USD_XAU_USD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("WTICO_USD", "XAU_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-WTICO_USD, -WTICO_USD_log1_price, -WTICO_USD_quantiles_1, -WTICO_USD_tangent_angle1)%>%
      dplyr::select(-XAU_USD, -XAU_USD_log2_price, -XAU_USD_quantiles_2, -XAU_USD_tangent_angle2)

    copula_data_BCO_USD_XAU_USD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("BCO_USD", "XAU_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-BCO_USD, -BCO_USD_log1_price, -BCO_USD_quantiles_1, -BCO_USD_tangent_angle1)%>%
      dplyr::select(-XAU_USD, -XAU_USD_log2_price, -XAU_USD_quantiles_2, -XAU_USD_tangent_angle2)

    copula_data_WTICO_USD_XAG_USD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("WTICO_USD", "XAG_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-WTICO_USD, -WTICO_USD_log1_price, -WTICO_USD_quantiles_1, -WTICO_USD_tangent_angle1)%>%
      dplyr::select(-XAG_USD, -XAG_USD_log2_price, -XAG_USD_quantiles_2, -XAG_USD_tangent_angle2)

    copula_data_BCO_USD_XAG_USD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("BCO_USD", "XAG_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-BCO_USD, -BCO_USD_log1_price, -BCO_USD_quantiles_1, -BCO_USD_tangent_angle1)%>%
      dplyr::select(-XAG_USD, -XAG_USD_log2_price, -XAG_USD_quantiles_2, -XAG_USD_tangent_angle2)

    copula_data_NATGAS_WTICO_USD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("NATGAS_USD", "WTICO_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-WTICO_USD, -WTICO_USD_log2_price, -WTICO_USD_quantiles_2, -WTICO_USD_tangent_angle2)

    copula_data_NATGAS_EU50_EUR <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("NATGAS_USD", "EU50_EUR"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-NATGAS_USD, -NATGAS_USD_log1_price, -NATGAS_USD_quantiles_1, -NATGAS_USD_tangent_angle1) %>%
      dplyr::select(-EU50_EUR, -EU50_EUR_log2_price, -EU50_EUR_quantiles_2, -EU50_EUR_tangent_angle2)

    copula_data_NATGAS_XAU_USD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("NATGAS_USD", "XAU_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-NATGAS_USD, -NATGAS_USD_log1_price, -NATGAS_USD_quantiles_1, -NATGAS_USD_tangent_angle1) %>%
      dplyr::select(-XAU_USD, -XAU_USD_log2_price, -XAU_USD_quantiles_2, -XAU_USD_tangent_angle2)

    copula_data_NATGAS_XAG_USD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("NATGAS_USD", "XAG_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-NATGAS_USD, -NATGAS_USD_log1_price, -NATGAS_USD_quantiles_1, -NATGAS_USD_tangent_angle1) %>%
      dplyr::select(-XAG_USD, -XAG_USD_log2_price, -XAG_USD_quantiles_2, -XAG_USD_tangent_angle2)

    copula_data_WTICO_USD_USB05Y_USD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("WTICO_USD", "USB05Y_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-WTICO_USD, -WTICO_USD_log1_price, -WTICO_USD_quantiles_1, -WTICO_USD_tangent_angle1)%>%
      dplyr::select(-USB05Y_USD, -USB05Y_USD_log2_price, -USB05Y_USD_quantiles_2, -USB05Y_USD_tangent_angle2)

    copula_data_WTICO_USD_USB10Y_USD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("WTICO_USD", "USB10Y_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-WTICO_USD, -WTICO_USD_log1_price, -WTICO_USD_quantiles_1, -WTICO_USD_tangent_angle1)%>%
      dplyr::select(-USB10Y_USD, -USB10Y_USD_log2_price, -USB10Y_USD_quantiles_2, -USB10Y_USD_tangent_angle2)

    copula_data_BCO_USD_UK100_GBP <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("BCO_USD", "UK100_GBP"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-BCO_USD, -BCO_USD_log1_price, -BCO_USD_quantiles_1, -BCO_USD_tangent_angle1)%>%
      dplyr::select(-UK100_GBP, -UK100_GBP_log2_price, -UK100_GBP_quantiles_2, -UK100_GBP_tangent_angle2)

    binary_data_for_post_model <-
      actual_wins_losses %>%
      rename(Date = dates, Asset = asset) %>%
      filter(profit_factor == profit_value_var)%>%
      filter(stop_factor == stop_value_var) %>%
      mutate(
        bin_var =
          case_when(
            trade_start_prices > trade_end_prices & trade_col == "Short" ~ "win",
            trade_start_prices <= trade_end_prices & trade_col == "Short" ~ "loss",

            trade_start_prices < trade_end_prices & trade_col == "Long" ~ "win",
            trade_start_prices >= trade_end_prices & trade_col == "Long" ~ "loss"

          )
      ) %>%
      dplyr::select(Date, bin_var, Asset, trade_col,
                    profit_factor, stop_factor,
                    trade_start_prices, trade_end_prices,
                    starting_stop_value, starting_profit_value)

    copula_data_macro <-
      METALS_INDICES %>%
      dplyr::select(Date,Asset, Price, High, Low, Open ) %>%
      find_pivots_fib_max_min(how_far_back = 20) %>%
      find_pivots_fib_max_min(how_far_back = 5) %>%
      left_join(copula_data_SPX_XAU) %>%
      left_join(copula_data_SPX_USB05Y_USD) %>%
      left_join(copula_data_SPX_XAG_USD) %>%
      left_join(copula_data_XAU_USD_XAG_USD) %>%
      left_join(copula_data_EU50_EUR_SPX500_USD) %>%
      left_join(copula_data_EU50_EUR_CH20_CHF) %>%
      left_join(copula_data_EU50_EUR_UK100_GBP) %>%
      left_join(copula_data_EU50_EUR_FR40_EUR) %>%
      left_join(copula_data_SPX500_USD_USB05) %>%
      left_join(copula_data_EU50_EUR_XAU_EUR) %>%
      left_join(copula_data_CH20_CHF_XAU_CHF) %>%
      left_join(copula_data_XAU_USD_XAU_CHF) %>%
      left_join(copula_data_WTICO_USD_BCO_USD) %>%
      left_join(copula_data_WTICO_USD_XAU_USD) %>%
      left_join(copula_data_BCO_USD_XAU_USD) %>%
      left_join(copula_data_WTICO_USD_XAG_USD) %>%
      left_join(copula_data_BCO_USD_XAG_USD) %>%
      left_join(copula_data_NATGAS_WTICO_USD) %>%
      left_join(copula_data_NATGAS_EU50_EUR) %>%
      left_join(copula_data_NATGAS_XAU_USD) %>%
      left_join(copula_data_NATGAS_XAG_USD) %>%
      left_join(copula_data_WTICO_USD_USB05Y_USD) %>%
      left_join(copula_data_WTICO_USD_USB10Y_USD) %>%
      left_join(copula_data_BCO_USD_UK100_GBP) %>%
      left_join(binary_data_for_post_model) %>%
      mutate(Date_for_join = as_date(Date)) %>%
      group_by(Asset) %>%
      arrange(Date, .by_group = TRUE) %>%
      group_by(Asset) %>%
      # fill(matches(all_macro_vars, ignore.case = FALSE), .direction = "down") %>%
      mutate(hour_of_day = lubridate::hour(Date) %>% as.numeric(),
             day_of_week = lubridate::wday(Date) %>% as.numeric())

    max_date_in_testing_data <- copula_data_macro %>% pull(Date) %>% max(na.rm = T)
    message(glue::glue("Max date in Complete data: {max_date_in_testing_data}"))

    lm_quant_vars <- names(copula_data_macro) %>% keep(~ str_detect(.x,"quantiles|tangent|cor|perc_line"))

    lm_vars1 <- c(lm_quant_vars,
                  "fib_1", "fib_2",
                  "fib_3", "fib_4",
                  "fib_5", "fib_6",
                  "lagged_var_1", "lagged_var_2",
                  "lagged_var_3", "lagged_var_5",
                  "lagged_var_8",
                  "lagged_var_13", "lagged_var_21",
                  "lagged_var_3_ma", "lagged_var_5_ma",
                  "lagged_var_8_ma", "lagged_var_13_ma",
                  "lagged_var_21_ma"
                  # "hour_of_day", "day_of_week"
    )

    return(
      list(
        "copula_data_macro" = copula_data_macro,
        "lm_vars1" =
          lm_vars1 %>%
          keep( ~ !str_detect(.x, "sd") & !str_detect(.x, "tangent") )
      )
    )

  }


#' create_NN_COMMOD_INDECES_QUANT
#'
#' @param METALS_INDICES
#' @param raw_macro_data
#' @param actual_wins_losses
#' @param lag_days
#' @param stop_value_var
#' @param profit_value_var
#'
#' @return
#' @export
#'
#' @examples
create_NN_CURRENCY_FOCUS_DAILY_QUANT <-
  function(METALS_INDICES = asset_data_daily_raw_ask %>%
             filter(str_detect(Asset, "AUD_USD|USD_JPY|NZD_USD|USD_CHF|EUR_USD|EUR_AUD|EUR_JPY|GBP_USD|EUR_GBP|GBP_JPY|USD_MXN|USD_NOK|USD_SEK|EUR_SEK|GBP_NZD|USD_ZAR|EUR_ZAR|XAU_|USD_CAD|NZD_CAD|AUD_JPY|GBP_AUD|EUR_NZD|USB05Y_USD|USB10Y_USD")),
           raw_macro_data,
           actual_wins_losses = actual_wins_losses,
           lag_days = 1,
           stop_value_var = 0.5,
           profit_value_var = 1) {
    # %>%
    #   dplyr::select(-SPX500_USD, -SPX500_USD_log1_price, -SPX500_USD_quantiles_1, -SPX500_USD_tangent_angle1)%>%
    #   dplyr::select(-USD_CHF, -USD_CHF_log2_price, -USD_CHF_quantiles_2, -USD_CHF_tangent_angle2)

    usd_macro_data <-
      get_USD_Indicators(raw_macro_data,
                         lag_days = lag_days,
                         first_difference = TRUE) %>%
      janitor::clean_names() %>%
      distinct()

    usd_macro_vars <- names(usd_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    all_macro_vars <- c(usd_macro_vars)


    copula_data_AUD_USD_EUR_AUD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("EUR_AUD", "AUD_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      )

    copula_data_AUD_USD_GBP_AUD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("AUD_USD", "GBP_AUD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-AUD_USD, -AUD_USD_log1_price, -AUD_USD_quantiles_1, -AUD_USD_tangent_angle1)

    copula_data_EUR_AUD_GBP_AUD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("EUR_AUD", "GBP_AUD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-EUR_AUD, -EUR_AUD_log1_price, -EUR_AUD_quantiles_1, -EUR_AUD_tangent_angle1)%>%
      dplyr::select(-GBP_AUD, -GBP_AUD_log2_price, -GBP_AUD_quantiles_2, -GBP_AUD_tangent_angle2)

    copula_data_AUD_USD_XAU_USD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("AUD_USD", "XAU_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-AUD_USD, -AUD_USD_log1_price, -AUD_USD_quantiles_1, -AUD_USD_tangent_angle1)

    copula_data_XAU_USD_XAU_AUD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("XAU_USD", "XAU_AUD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAU_USD, -XAU_USD_log1_price, -XAU_USD_quantiles_1, -XAU_USD_tangent_angle1)

    copula_data_XAU_USD_XAU_AUD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("AUD_USD", "XAU_AUD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-AUD_USD, -AUD_USD_log1_price, -AUD_USD_quantiles_1, -AUD_USD_tangent_angle1) %>%
      dplyr::select(-XAU_AUD, -XAU_AUD_log2_price, -XAU_AUD_quantiles_2, -XAU_AUD_tangent_angle2)

    copula_data_EUR_USD_GBP_USD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("EUR_USD", "GBP_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-EUR_USD, -EUR_USD_log1_price, -EUR_USD_quantiles_1, -EUR_USD_tangent_angle1)%>%
      dplyr::select(-GBP_USD, -GBP_USD_log2_price, -GBP_USD_quantiles_2, -GBP_USD_tangent_angle2)

    copula_data_EUR_USD_EUR_GBP <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("EUR_USD", "EUR_GBP"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-EUR_USD, -EUR_USD_log1_price, -EUR_USD_quantiles_1, -EUR_USD_tangent_angle1)

    copula_data_EUR_USD_XAU_EUR <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("EUR_USD", "XAU_EUR"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-EUR_USD, -EUR_USD_log1_price, -EUR_USD_quantiles_1, -EUR_USD_tangent_angle1)

    copula_data_XAU_USD_XAU_EUR <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("XAU_USD", "XAU_EUR"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAU_USD, -XAU_USD_log1_price, -XAU_USD_quantiles_1, -XAU_USD_tangent_angle1)%>%
      dplyr::select(-XAU_EUR, -XAU_EUR_log2_price, -XAU_EUR_quantiles_2, -XAU_EUR_tangent_angle2)

    copula_data_XAU_EUR_XAU_GBP <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("XAU_EUR", "XAU_GBP"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAU_EUR, -XAU_EUR_log1_price, -XAU_EUR_quantiles_1, -XAU_EUR_tangent_angle1)%>%
      dplyr::select(-XAU_GBP, -XAU_GBP_log2_price, -XAU_GBP_quantiles_2, -XAU_GBP_tangent_angle2)

    copula_data_EUR_GBP_XAU_GBP <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("EUR_GBP", "XAU_GBP"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-EUR_GBP, -EUR_GBP_log1_price, -EUR_GBP_quantiles_1, -EUR_GBP_tangent_angle1)%>%
      dplyr::select(-XAU_GBP, -XAU_GBP_log2_price, -XAU_GBP_quantiles_2, -XAU_GBP_tangent_angle2)

    copula_data_AUD_USD_NZD_USD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("AUD_USD", "NZD_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-AUD_USD, -AUD_USD_log1_price, -AUD_USD_quantiles_1, -AUD_USD_tangent_angle1)

    copula_data_AUD_USD_AUD_NZD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("AUD_USD", "AUD_NZD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-AUD_USD, -AUD_USD_log1_price, -AUD_USD_quantiles_1, -AUD_USD_tangent_angle1)

    copula_data_NZD_USD_GBP_NZD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("NZD_USD", "GBP_NZD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-NZD_USD, -NZD_USD_log1_price, -NZD_USD_quantiles_1, -NZD_USD_tangent_angle1)

    copula_data_NZD_USD_EUR_NZD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("NZD_USD", "EUR_NZD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-NZD_USD, -NZD_USD_log1_price, -NZD_USD_quantiles_1, -NZD_USD_tangent_angle1)

    copula_data_EUR_NZD_XAU_EUR <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("EUR_NZD", "XAU_EUR"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-EUR_NZD, -EUR_NZD_log1_price, -EUR_NZD_quantiles_1, -EUR_NZD_tangent_angle1) %>%
      dplyr::select(-XAU_EUR, -XAU_EUR_log2_price, -XAU_EUR_quantiles_2, -XAU_EUR_tangent_angle2)

    copula_data_XAU_NZD_XAU_EUR <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("XAU_NZD", "XAU_EUR"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAU_EUR, -XAU_EUR_log2_price, -XAU_EUR_quantiles_2, -XAU_EUR_tangent_angle2)

    copula_data_USD_NOK_USD_SEK <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("USD_NOK", "USD_SEK"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      )

    copula_data_USD_SEK_EUR_SEK <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("USD_SEK", "EUR_SEK"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-USD_SEK, -USD_SEK_log1_price, -USD_SEK_quantiles_1, -USD_SEK_tangent_angle1)

    copula_data_USD_NOK_EUR_SEK <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("USD_NOK", "EUR_SEK"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-USD_NOK, -USD_NOK_log1_price, -USD_NOK_quantiles_1, -USD_NOK_tangent_angle1)%>%
      dplyr::select(-EUR_SEK, -EUR_SEK_log2_price, -EUR_SEK_quantiles_2, -EUR_SEK_tangent_angle2)

    copula_data_USD_NOK_XAU_USD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("USD_NOK", "XAU_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-USD_NOK, -USD_NOK_log1_price, -USD_NOK_quantiles_1, -USD_NOK_tangent_angle1)%>%
      dplyr::select(-XAU_USD, -XAU_USD_log2_price, -XAU_USD_quantiles_2, -XAU_USD_tangent_angle2)

    copula_data_USD_SEK_XAU_USD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("USD_SEK", "XAU_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-USD_SEK, -USD_SEK_log1_price, -USD_SEK_quantiles_1, -USD_SEK_tangent_angle1)%>%
      dplyr::select(-XAU_USD, -XAU_USD_log2_price, -XAU_USD_quantiles_2, -XAU_USD_tangent_angle2)

    copula_data_XAU_EUR_EUR_SEK <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("XAU_EUR", "EUR_SEK"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAU_EUR, -XAU_EUR_log1_price, -XAU_EUR_quantiles_1, -XAU_EUR_tangent_angle1) %>%
      dplyr::select(-EUR_SEK, -EUR_SEK_log2_price, -EUR_SEK_quantiles_2, -EUR_SEK_tangent_angle2)

    copula_data_USD_CAD_XAU_CAD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("USD_CAD", "XAU_CAD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      )

    copula_data_USD_CAD_XAU_USD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("USD_CAD", "XAU_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-USD_CAD, -USD_CAD_log1_price, -USD_CAD_quantiles_1, -USD_CAD_tangent_angle1)%>%
      dplyr::select(-XAU_USD, -XAU_USD_log2_price, -XAU_USD_quantiles_2, -XAU_USD_tangent_angle2)

    copula_data_USD_CAD_NZD_CAD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("USD_CAD", "NZD_CAD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-USD_CAD, -USD_CAD_log1_price, -USD_CAD_quantiles_1, -USD_CAD_tangent_angle1)

    copula_data_NZD_CAD_XAU_NZD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("NZD_CAD", "XAU_NZD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-NZD_CAD, -NZD_CAD_log1_price, -NZD_CAD_quantiles_1, -NZD_CAD_tangent_angle1)%>%
      dplyr::select(-XAU_NZD, -XAU_NZD_log2_price, -XAU_NZD_quantiles_2, -XAU_NZD_tangent_angle2)

    binary_data_for_post_model <-
      actual_wins_losses %>%
      rename(Date = dates, Asset = asset) %>%
      filter(profit_factor == profit_value_var)%>%
      filter(stop_factor == stop_value_var) %>%
      mutate(
        bin_var =
          case_when(
            trade_start_prices > trade_end_prices & trade_col == "Short" ~ "win",
            trade_start_prices <= trade_end_prices & trade_col == "Short" ~ "loss",

            trade_start_prices < trade_end_prices & trade_col == "Long" ~ "win",
            trade_start_prices >= trade_end_prices & trade_col == "Long" ~ "loss"

          )
      ) %>%
      dplyr::select(Date, bin_var, Asset, trade_col,
                    profit_factor, stop_factor,
                    trade_start_prices, trade_end_prices,
                    starting_stop_value, starting_profit_value) %>%
      distinct()

    copula_data_macro <-
      METALS_INDICES %>%
      dplyr::distinct(Date,Asset, Price, High, Low, Open ) %>%
      find_pivots_fib_max_min(how_far_back = 27) %>%
      find_pivots_fib_max_min(how_far_back = 11) %>%
      find_pivots_fib_max_min(how_far_back = 5) %>%
      left_join(copula_data_AUD_USD_EUR_AUD) %>%
      left_join(copula_data_AUD_USD_GBP_AUD) %>%
      left_join(copula_data_EUR_AUD_GBP_AUD) %>%
      left_join(copula_data_AUD_USD_XAU_USD) %>%
      left_join(copula_data_XAU_USD_XAU_AUD) %>%
      left_join(copula_data_EUR_USD_GBP_USD) %>%
      left_join(copula_data_EUR_USD_EUR_GBP) %>%
      left_join(copula_data_EUR_USD_XAU_EUR) %>%
      left_join(copula_data_XAU_USD_XAU_EUR) %>%
      left_join(copula_data_XAU_EUR_XAU_GBP) %>%
      left_join(copula_data_EUR_GBP_XAU_GBP) %>%
      left_join(copula_data_AUD_USD_NZD_USD) %>%
      left_join(copula_data_AUD_USD_AUD_NZD) %>%
      left_join(copula_data_NZD_USD_GBP_NZD) %>%
      left_join(copula_data_NZD_USD_EUR_NZD) %>%
      left_join(copula_data_EUR_NZD_XAU_EUR) %>%
      left_join(copula_data_XAU_NZD_XAU_EUR) %>%
      left_join(copula_data_USD_NOK_USD_SEK) %>%
      left_join(copula_data_USD_SEK_EUR_SEK) %>%
      left_join(copula_data_USD_NOK_EUR_SEK) %>%
      left_join(copula_data_USD_NOK_XAU_USD) %>%
      left_join(copula_data_USD_SEK_XAU_USD) %>%
      left_join(copula_data_XAU_EUR_EUR_SEK) %>%
      left_join(copula_data_USD_CAD_XAU_CAD) %>%
      left_join(copula_data_USD_CAD_NZD_CAD) %>%
      left_join(copula_data_USD_CAD_XAU_USD) %>%
      left_join(copula_data_NZD_CAD_XAU_NZD) %>%
      left_join(binary_data_for_post_model) %>%
      mutate(Date_for_join = as_date(Date)) %>%
      distinct() %>%
      left_join(
        usd_macro_data %>%
          rename(Date_for_join = date)
      )  %>%
      group_by(Asset) %>%
      arrange(Date, .by_group = TRUE) %>%
      group_by(Asset) %>%
      fill(matches(all_macro_vars, ignore.case = FALSE), .direction = "down") %>%
      distinct()

    max_date_in_testing_data <- copula_data_macro %>% pull(Date) %>% max(na.rm = T)
    message(glue::glue("Max date in Complete data: {max_date_in_testing_data}"))

    lm_quant_vars <- names(copula_data_macro) %>% keep(~ str_detect(.x,"quantiles|tangent|cor|perc_line"))

    lm_vars1 <- c(lm_quant_vars, all_macro_vars,
                  "fib_1", "fib_2",
                  "fib_3", "fib_4",
                  "fib_5", "fib_6",
                  "lagged_var_1", "lagged_var_2",
                  "lagged_var_3", "lagged_var_5",
                  "lagged_var_8",
                  "lagged_var_13", "lagged_var_21",
                  "lagged_var_3_ma", "lagged_var_5_ma",
                  "lagged_var_8_ma", "lagged_var_13_ma",
                  "lagged_var_21_ma"
                  # "hour_of_day", "day_of_week"
    )

    return(
      list(
        "copula_data_macro" = copula_data_macro,
        "lm_vars1" =
          lm_vars1 %>%
          keep( ~ !str_detect(.x, "sd") & !str_detect(.x, "tangent") )
      )
    )

  }


#' create_NN_COMMOD_INDECES_QUANT
#'
#' @param METALS_INDICES
#' @param raw_macro_data
#' @param actual_wins_losses
#' @param lag_days
#' @param stop_value_var
#' @param profit_value_var
#'
#' @return
#' @export
#'
#' @examples
create_NN_AUD_FOCUS_DAILY_QUANT <-
  function(METALS_INDICES = asset_data_daily_raw_ask %>%
             filter(str_detect(Asset, "AUD_USD|EUR_NZD|NZD_JPY|USD_JPY|AUD_NZD|NZD_USD|GBP_NZD|GBP_USD|AUD_CHF|EUR_USD|AUD_SGD|GBP_AUD|NZD_CAD|USD_CAD|EUR_AUD|XAU_USD|XAU_EUR|XAU_AUD|XAU_NZD")),
           raw_macro_data,
           actual_wins_losses = actual_wins_losses,
           lag_days = 1,
           stop_value_var = 0.5,
           profit_value_var = 1) {
    # %>%
    #   dplyr::select(-SPX500_USD, -SPX500_USD_log1_price, -SPX500_USD_quantiles_1, -SPX500_USD_tangent_angle1)%>%
    #   dplyr::select(-USD_CHF, -USD_CHF_log2_price, -USD_CHF_quantiles_2, -USD_CHF_tangent_angle2)

    usd_macro_data <-
      get_USD_Indicators(raw_macro_data,
                         lag_days = lag_days,
                         first_difference = TRUE) %>%
      janitor::clean_names() %>%
      distinct()

    usd_macro_vars <- names(usd_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    all_macro_vars <- c(usd_macro_vars)


    copula_data_AUD_USD_EUR_AUD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("EUR_AUD", "AUD_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      )

    copula_data_AUD_USD_GBP_AUD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("AUD_USD", "GBP_AUD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-AUD_USD, -AUD_USD_log1_price, -AUD_USD_quantiles_1, -AUD_USD_tangent_angle1)

    copula_data_EUR_AUD_GBP_AUD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("EUR_AUD", "GBP_AUD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-EUR_AUD, -EUR_AUD_log1_price, -EUR_AUD_quantiles_1, -EUR_AUD_tangent_angle1)%>%
      dplyr::select(-GBP_AUD, -GBP_AUD_log2_price, -GBP_AUD_quantiles_2, -GBP_AUD_tangent_angle2)

    copula_data_AUD_USD_XAU_USD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("AUD_USD", "XAU_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-AUD_USD, -AUD_USD_log1_price, -AUD_USD_quantiles_1, -AUD_USD_tangent_angle1)

    copula_data_XAU_USD_XAU_AUD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("XAU_USD", "XAU_AUD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAU_USD, -XAU_USD_log1_price, -XAU_USD_quantiles_1, -XAU_USD_tangent_angle1)

    copula_data_XAU_USD_XAU_AUD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("AUD_USD", "XAU_AUD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-AUD_USD, -AUD_USD_log1_price, -AUD_USD_quantiles_1, -AUD_USD_tangent_angle1) %>%
      dplyr::select(-XAU_AUD, -XAU_AUD_log2_price, -XAU_AUD_quantiles_2, -XAU_AUD_tangent_angle2)

    copula_data_EUR_USD_GBP_USD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("EUR_USD", "GBP_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-EUR_USD, -EUR_USD_log1_price, -EUR_USD_quantiles_1, -EUR_USD_tangent_angle1)%>%
      dplyr::select(-GBP_USD, -GBP_USD_log2_price, -GBP_USD_quantiles_2, -GBP_USD_tangent_angle2)

    copula_data_EUR_USD_EUR_GBP <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("EUR_USD", "EUR_GBP"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-EUR_USD, -EUR_USD_log1_price, -EUR_USD_quantiles_1, -EUR_USD_tangent_angle1)

    copula_data_EUR_USD_XAU_EUR <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("EUR_USD", "XAU_EUR"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-EUR_USD, -EUR_USD_log1_price, -EUR_USD_quantiles_1, -EUR_USD_tangent_angle1)

    copula_data_XAU_USD_XAU_EUR <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("XAU_USD", "XAU_EUR"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAU_USD, -XAU_USD_log1_price, -XAU_USD_quantiles_1, -XAU_USD_tangent_angle1)%>%
      dplyr::select(-XAU_EUR, -XAU_EUR_log2_price, -XAU_EUR_quantiles_2, -XAU_EUR_tangent_angle2)

    copula_data_XAU_EUR_XAU_GBP <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("XAU_EUR", "XAU_GBP"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAU_EUR, -XAU_EUR_log1_price, -XAU_EUR_quantiles_1, -XAU_EUR_tangent_angle1)%>%
      dplyr::select(-XAU_GBP, -XAU_GBP_log2_price, -XAU_GBP_quantiles_2, -XAU_GBP_tangent_angle2)

    copula_data_EUR_GBP_XAU_GBP <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("EUR_GBP", "XAU_GBP"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-EUR_GBP, -EUR_GBP_log1_price, -EUR_GBP_quantiles_1, -EUR_GBP_tangent_angle1)%>%
      dplyr::select(-XAU_GBP, -XAU_GBP_log2_price, -XAU_GBP_quantiles_2, -XAU_GBP_tangent_angle2)

    copula_data_AUD_USD_NZD_USD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("AUD_USD", "NZD_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-AUD_USD, -AUD_USD_log1_price, -AUD_USD_quantiles_1, -AUD_USD_tangent_angle1)

    copula_data_AUD_USD_AUD_NZD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("AUD_USD", "AUD_NZD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-AUD_USD, -AUD_USD_log1_price, -AUD_USD_quantiles_1, -AUD_USD_tangent_angle1)

    copula_data_NZD_USD_GBP_NZD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("NZD_USD", "GBP_NZD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-NZD_USD, -NZD_USD_log1_price, -NZD_USD_quantiles_1, -NZD_USD_tangent_angle1)

    copula_data_NZD_USD_EUR_NZD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("NZD_USD", "EUR_NZD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-NZD_USD, -NZD_USD_log1_price, -NZD_USD_quantiles_1, -NZD_USD_tangent_angle1)

    copula_data_EUR_NZD_XAU_EUR <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("EUR_NZD", "XAU_EUR"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-EUR_NZD, -EUR_NZD_log1_price, -EUR_NZD_quantiles_1, -EUR_NZD_tangent_angle1) %>%
      dplyr::select(-XAU_EUR, -XAU_EUR_log2_price, -XAU_EUR_quantiles_2, -XAU_EUR_tangent_angle2)

    copula_data_XAU_NZD_XAU_EUR <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("XAU_NZD", "XAU_EUR"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAU_EUR, -XAU_EUR_log2_price, -XAU_EUR_quantiles_2, -XAU_EUR_tangent_angle2)

    copula_data_USD_NOK_USD_SEK <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("USD_NOK", "USD_SEK"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      )

    copula_data_USD_SEK_EUR_SEK <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("USD_SEK", "EUR_SEK"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-USD_SEK, -USD_SEK_log1_price, -USD_SEK_quantiles_1, -USD_SEK_tangent_angle1)

    copula_data_USD_NOK_EUR_SEK <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("USD_NOK", "EUR_SEK"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-USD_NOK, -USD_NOK_log1_price, -USD_NOK_quantiles_1, -USD_NOK_tangent_angle1)%>%
      dplyr::select(-EUR_SEK, -EUR_SEK_log2_price, -EUR_SEK_quantiles_2, -EUR_SEK_tangent_angle2)

    copula_data_USD_NOK_XAU_USD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("USD_NOK", "XAU_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-USD_NOK, -USD_NOK_log1_price, -USD_NOK_quantiles_1, -USD_NOK_tangent_angle1)%>%
      dplyr::select(-XAU_USD, -XAU_USD_log2_price, -XAU_USD_quantiles_2, -XAU_USD_tangent_angle2)

    copula_data_USD_SEK_XAU_USD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("USD_SEK", "XAU_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-USD_SEK, -USD_SEK_log1_price, -USD_SEK_quantiles_1, -USD_SEK_tangent_angle1)%>%
      dplyr::select(-XAU_USD, -XAU_USD_log2_price, -XAU_USD_quantiles_2, -XAU_USD_tangent_angle2)

    copula_data_XAU_EUR_EUR_SEK <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("XAU_EUR", "EUR_SEK"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAU_EUR, -XAU_EUR_log1_price, -XAU_EUR_quantiles_1, -XAU_EUR_tangent_angle1) %>%
      dplyr::select(-EUR_SEK, -EUR_SEK_log2_price, -EUR_SEK_quantiles_2, -EUR_SEK_tangent_angle2)

    copula_data_USD_CAD_XAU_CAD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("USD_CAD", "XAU_CAD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      )

    copula_data_USD_CAD_XAU_USD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("USD_CAD", "XAU_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-USD_CAD, -USD_CAD_log1_price, -USD_CAD_quantiles_1, -USD_CAD_tangent_angle1)%>%
      dplyr::select(-XAU_USD, -XAU_USD_log2_price, -XAU_USD_quantiles_2, -XAU_USD_tangent_angle2)

    copula_data_USD_CAD_NZD_CAD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("USD_CAD", "NZD_CAD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-USD_CAD, -USD_CAD_log1_price, -USD_CAD_quantiles_1, -USD_CAD_tangent_angle1)

    copula_data_NZD_CAD_XAU_NZD <-
      estimating_dual_copula(
        asset_data_to_use = METALS_INDICES,
        asset_to_use = c("NZD_CAD", "XAU_NZD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-NZD_CAD, -NZD_CAD_log1_price, -NZD_CAD_quantiles_1, -NZD_CAD_tangent_angle1)%>%
      dplyr::select(-XAU_NZD, -XAU_NZD_log2_price, -XAU_NZD_quantiles_2, -XAU_NZD_tangent_angle2)

    binary_data_for_post_model <-
      actual_wins_losses %>%
      rename(Date = dates, Asset = asset) %>%
      filter(profit_factor == profit_value_var)%>%
      filter(stop_factor == stop_value_var) %>%
      mutate(
        bin_var =
          case_when(
            trade_start_prices > trade_end_prices & trade_col == "Short" ~ "win",
            trade_start_prices <= trade_end_prices & trade_col == "Short" ~ "loss",

            trade_start_prices < trade_end_prices & trade_col == "Long" ~ "win",
            trade_start_prices >= trade_end_prices & trade_col == "Long" ~ "loss"

          )
      ) %>%
      dplyr::select(Date, bin_var, Asset, trade_col,
                    profit_factor, stop_factor,
                    trade_start_prices, trade_end_prices,
                    starting_stop_value, starting_profit_value) %>%
      distinct()

    copula_data_macro <-
      METALS_INDICES %>%
      dplyr::distinct(Date,Asset, Price, High, Low, Open ) %>%
      find_pivots_fib_max_min(how_far_back = 27) %>%
      find_pivots_fib_max_min(how_far_back = 11) %>%
      find_pivots_fib_max_min(how_far_back = 5) %>%
      left_join(copula_data_AUD_USD_EUR_AUD) %>%
      left_join(copula_data_AUD_USD_GBP_AUD) %>%
      left_join(copula_data_EUR_AUD_GBP_AUD) %>%
      left_join(copula_data_AUD_USD_XAU_USD) %>%
      left_join(copula_data_XAU_USD_XAU_AUD) %>%
      left_join(copula_data_EUR_USD_GBP_USD) %>%
      left_join(copula_data_EUR_USD_EUR_GBP) %>%
      left_join(copula_data_EUR_USD_XAU_EUR) %>%
      left_join(copula_data_XAU_USD_XAU_EUR) %>%
      left_join(copula_data_XAU_EUR_XAU_GBP) %>%
      left_join(copula_data_EUR_GBP_XAU_GBP) %>%
      left_join(copula_data_AUD_USD_NZD_USD) %>%
      left_join(copula_data_AUD_USD_AUD_NZD) %>%
      left_join(copula_data_NZD_USD_GBP_NZD) %>%
      left_join(copula_data_NZD_USD_EUR_NZD) %>%
      left_join(copula_data_EUR_NZD_XAU_EUR) %>%
      left_join(copula_data_XAU_NZD_XAU_EUR) %>%
      left_join(copula_data_USD_NOK_USD_SEK) %>%
      left_join(copula_data_USD_SEK_EUR_SEK) %>%
      left_join(copula_data_USD_NOK_EUR_SEK) %>%
      left_join(copula_data_USD_NOK_XAU_USD) %>%
      left_join(copula_data_USD_SEK_XAU_USD) %>%
      left_join(copula_data_XAU_EUR_EUR_SEK) %>%
      left_join(copula_data_USD_CAD_XAU_CAD) %>%
      left_join(copula_data_USD_CAD_NZD_CAD) %>%
      left_join(copula_data_USD_CAD_XAU_USD) %>%
      left_join(copula_data_NZD_CAD_XAU_NZD) %>%
      left_join(binary_data_for_post_model) %>%
      mutate(Date_for_join = as_date(Date)) %>%
      distinct() %>%
      left_join(
        usd_macro_data %>%
          rename(Date_for_join = date)
      )  %>%
      group_by(Asset) %>%
      arrange(Date, .by_group = TRUE) %>%
      group_by(Asset) %>%
      fill(matches(all_macro_vars, ignore.case = FALSE), .direction = "down") %>%
      distinct()

    max_date_in_testing_data <- copula_data_macro %>% pull(Date) %>% max(na.rm = T)
    message(glue::glue("Max date in Complete data: {max_date_in_testing_data}"))

    lm_quant_vars <- names(copula_data_macro) %>% keep(~ str_detect(.x,"quantiles|tangent|cor|perc_line"))

    lm_vars1 <- c(lm_quant_vars, all_macro_vars,
                  "fib_1", "fib_2",
                  "fib_3", "fib_4",
                  "fib_5", "fib_6",
                  "lagged_var_1", "lagged_var_2",
                  "lagged_var_3", "lagged_var_5",
                  "lagged_var_8",
                  "lagged_var_13", "lagged_var_21",
                  "lagged_var_3_ma", "lagged_var_5_ma",
                  "lagged_var_8_ma", "lagged_var_13_ma",
                  "lagged_var_21_ma"
                  # "hour_of_day", "day_of_week"
    )

    return(
      list(
        "copula_data_macro" = copula_data_macro,
        "lm_vars1" =
          lm_vars1 %>%
          keep( ~ !str_detect(.x, "sd") & !str_detect(.x, "tangent") )
      )
    )

  }

#' get_ALL_DAILY_TRADES_FOR_ALGO
#'
#' @param Daily_Data_Trades_ask
#' @param actual_wins_losses_daily
#' @param currency_conversion
#' @param asset_infor
#'
#' @return
#' @export
#'
#' @examples
get_ALL_DAILY_TRADES_FOR_ALGO <-
  function(
    Daily_Data_Trades_ask = Daily_Data_Trades_ask,
    actual_wins_losses_daily = get_ts_trade_actuals_Logit_NN("C:/Users/Nikhil Chandra/Documents/trade_data/full_ts_trades_mapped_Daily_Data.db", data_is_daily = TRUE),
    currency_conversion = currency_conversion,
    asset_infor = asset_infor,
    raw_macro_data = raw_macro_data
  ) {

    Daily_means =
      wrangle_asset_data(
        asset_data_daily_raw = Daily_Data_Trades_ask,
        summarise_means = TRUE
      )

    #-----------------------Trades 1
    stop_value_var = 1
    profit_value_var = 2

    metals_indices_Logit_Data <-
      create_NN_METALS_IDINCES_DAILY_QUANT(
        METALS_INDICES = Daily_Data_Trades_ask %>% filter(str_detect(Asset, "XAG|XAU|SPX500|US2000|UK100|AU200_AUD|SG30_SGD|EU50_EUR")),
        raw_macro_data = raw_macro_data,
        actual_wins_losses = actual_wins_losses_daily,
        lag_days = 1,
        stop_value_var = stop_value_var,
        profit_value_var = profit_value_var
      )

    METALS_INDICES_DAILY_LOGIT<-
      get_Logit_trades(
        logit_path_save_path = "C:/Users/Nikhil Chandra/Documents/trade_data/asset_specific_NN_Daily_Quant/",
        Logit_sims_db =  "C:/Users/Nikhil Chandra/Documents/trade_data/METALS_INDICES_DAILY_LOGIT.db",
        copula_data = metals_indices_Logit_Data,
        sim_min = 80,
        edge_min = 0,
        stop_var = stop_value_var,
        profit_var = profit_value_var,
        outperformance_count_min = 0.55,
        risk_weighted_return_mid_min =  0.1,
        sim_table = "METALS_INDICES_sims",
        combined_filter_n = 10
      )

    #-----------------------Trades 2
    stop_value_var = 0.5
    profit_value_var = 1

    metals_indices_Logit_Data <-
      create_NN_CRYPTO_MET_CUR_DAILY_QUANT(
        METALS_INDICES = Daily_Data_Trades_ask %>% filter(str_detect(Asset, "XAG|XAU|SPX500|USB05Y_USD|USB10Y_USD|BTC_USD|LTC_USD|BCH_USD|USD_JPY|AUD_USD|EUR_USD|GBP_USD|USD_MXN|USD_SEK|USD_CHF")),
        raw_macro_data = raw_macro_data,
        actual_wins_losses = actual_wins_losses_daily,
        lag_days = 1,
        stop_value_var = stop_value_var,
        profit_value_var = profit_value_var
      )


    METALS_INDICES_DAILY_LOGIT_2 <-
      get_Logit_trades(
        logit_path_save_path = "C:/Users/Nikhil Chandra/Documents/trade_data/asset_specific_NN_Daily_Quant_2/",
        Logit_sims_db =  "C:/Users/Nikhil Chandra/Documents/trade_data/CRYPTO_MET_USD_DAILY_LOGIT.db",
        copula_data = metals_indices_Logit_Data,
        sim_min = 50,
        edge_min = 0,
        stop_var = stop_value_var,
        profit_var = profit_value_var,
        outperformance_count_min = 0.55,
        risk_weighted_return_mid_min =  0.05,
        sim_table = "CRYPTO_MET_USD_DAILY_LOGIT",
        combined_filter_n = 2
      )

    #-----------------------Trades 3
    stop_value_var = 0.5
    profit_value_var = 1

    metals_indices_Logit_Data <-
      create_NN_COMMOD_INDECES_QUANT(
        METALS_INDICES = Daily_Data_Trades_ask %>% filter(str_detect(Asset, "XAG|XAU|SPX500|USB05Y_USD|USB10Y_USD|CH20_CHF|UK100_GBP|JP225Y_JPY|EU50_EUR|FR40_EUR|JP225_USD|AU200_AUD|ESPIX_EUR|SG30_SGD|DE10YB_EUR|WTICO_USD|BCO_USD|NATGAS_USD|XCU_USD")),
        raw_macro_data = raw_macro_data,
        actual_wins_losses = actual_wins_losses_daily,
        lag_days = 1,
        stop_value_var = stop_value_var,
        profit_value_var = profit_value_var
      )


    METALS_INDICES_DAILY_LOGIT_3 <-
      get_Logit_trades(
        logit_path_save_path = "C:/Users/Nikhil Chandra/Documents/trade_data/asset_specific_NN_Daily_Quant_3/",
        Logit_sims_db =  "C:/Users/Nikhil Chandra/Documents/trade_data/COMMOD_INDICES_BONDS.db",
        copula_data = metals_indices_Logit_Data,
        sim_min = 50,
        edge_min = 0,
        stop_var = stop_value_var,
        profit_var = profit_value_var,
        outperformance_count_min = 0.55,
        risk_weighted_return_mid_min =  0.1,
        sim_table = "COMMOD_INDICES_BONDS",
        p_value_thresh_exact_filter =  NULL,
        combined_filter_n = 5
      )

    #-----------------------Trades 4
    stop_value_var = 1
    profit_value_var = 2

    metals_indices_Logit_Data <-
      create_NN_CURRENCY_FOCUS_DAILY_QUANT(
        METALS_INDICES = Daily_Data_Trades_ask %>% filter(str_detect(Asset, "AUD_USD|AUD_NZD|USD_JPY|NZD_USD|USD_CHF|EUR_USD|EUR_AUD|EUR_JPY|GBP_USD|EUR_GBP|GBP_JPY|USD_MXN|USD_NOK|USD_SEK|EUR_SEK|GBP_NZD|USD_ZAR|EUR_ZAR|XAU_|USD_CAD|NZD_CAD|AUD_JPY|GBP_AUD|EUR_NZD|USB05Y_USD|USB10Y_USD")),
        raw_macro_data = raw_macro_data,
        actual_wins_losses = actual_wins_losses_daily,
        lag_days = 1,
        stop_value_var = stop_value_var,
        profit_value_var = profit_value_var
      )


    METALS_INDICES_DAILY_LOGIT_4 <-
      get_Logit_trades(
        logit_path_save_path = "C:/Users/Nikhil Chandra/Documents/trade_data/asset_specific_NN_Daily_Quant_3/",
        Logit_sims_db =  "C:/Users/Nikhil Chandra/Documents/trade_data/LOGIT_CURRENCY_DAILY.db",
        copula_data = metals_indices_Logit_Data,
        sim_min = 80,
        edge_min = 0,
        stop_var = stop_value_var,
        profit_var = profit_value_var,
        outperformance_count_min = 0.55,
        risk_weighted_return_mid_min =  0.1,
        sim_table = "CURRENCY",
        p_value_thresh_exact_filter =  NULL,
        combined_filter_n = 5
      )

    #---------------------------------------trades 5
    stop_value_var = 1
    profit_value_var = 2

    metals_indices_Logit_Data <-
      create_NN_AUD_FOCUS_DAILY_QUANT(
        METALS_INDICES = Daily_Data_Trades_ask %>% filter(str_detect(Asset, "AUD_USD|EUR_NZD|NZD_JPY|USD_JPY|AUD_NZD|NZD_USD|GBP_NZD|GBP_USD|AUD_CHF|EUR_USD|AUD_SGD|GBP_AUD|NZD_CAD|USD_CAD|EUR_AUD|XAU_USD|XAU_EUR|XAU_AUD|XAU_GBP|XAU_NZD|XAG_USD|XAG_EUR|XAG_AUD|XAG_NZD|EUR_GBP|GBP_USD|USD_NOK|USD_SEK|EUR_SEK|USD_CAD|XAU_CAD|XAG_CAD")),
        raw_macro_data = raw_macro_data,
        actual_wins_losses = actual_wins_losses_daily,
        lag_days = 1,
        stop_value_var = stop_value_var,
        profit_value_var = profit_value_var
      )

    METALS_INDICES_DAILY_LOGIT_5 <-
      get_Logit_trades(
        logit_path_save_path = "C:/Users/Nikhil Chandra/Documents/trade_data/asset_specific_NN_Daily_Quant_5/",
        Logit_sims_db =  "C:/Users/Nikhil Chandra/Documents/trade_data/LOGIT_AUD_FOCUS_DAILY.db",
        copula_data = metals_indices_Logit_Data,
        sim_min = 55,
        edge_min = 0,
        stop_var = stop_value_var,
        profit_var = profit_value_var,
        outperformance_count_min = 0.55,
        risk_weighted_return_mid_min =  0.1,
        sim_table = "AUD_FOCUS",
        p_value_thresh_exact_filter =  NULL,
        combined_filter_n = 5
      )

    all_trades <-
      list(METALS_INDICES_DAILY_LOGIT,
           METALS_INDICES_DAILY_LOGIT_2,
           METALS_INDICES_DAILY_LOGIT_3,
           METALS_INDICES_DAILY_LOGIT_4,
           METALS_INDICES_DAILY_LOGIT_5)

    return(all_trades)

  }

#' get_DAILY_ALGO_DATA_API_REQUEST
#'
#' @return
#' @export
#'
#' @examples
get_DAILY_ALGO_DATA_API_REQUEST <-
  function() {
    asset_list_oanda <- get_oanda_symbols() %>%
      keep( ~ .x %in% unique(c("HK33_HKD", "USD_JPY",
                        "BTC_USD",
                        "AUD_NZD", "GBP_CHF",
                        "EUR_HUF", "EUR_ZAR", "NZD_JPY", "EUR_NZD",
                        "USB02Y_USD",
                        "XAU_CAD", "GBP_JPY", "EUR_NOK", "USD_SGD", "EUR_SEK",
                        "DE30_EUR", "AUD_USD", "NZD_USD",
                        "AUD_CAD",
                        "UK10YB_GBP",
                        "XPD_USD",
                        "UK100_GBP", "BCO_USD",
                        "USD_CHF", "GBP_NZD",
                        "GBP_SGD", "USD_SEK", "EUR_SGD", "XCU_USD", "SUGAR_USD", "CHF_ZAR",
                        "AUD_CHF", "EUR_CHF", "USD_MXN", "GBP_USD", "WTICO_USD", "EUR_JPY", "USD_NOK",
                        "XAU_USD",
                        "DE10YB_EUR",
                        "USD_CZK", "AUD_SGD", "USD_HUF", "WHEAT_USD",
                        "EUR_USD", "SG30_SGD", "GBP_AUD", "NZD_CAD", "AU200_AUD", "XAG_USD",
                        "XAU_EUR", "EUR_GBP", "USD_CNH", "USD_CAD", "NAS100_USD",
                        "USB10Y_USD",
                        "EU50_EUR", "NATGAS_USD", "CAD_JPY", "FR40_EUR", "USD_ZAR", "XAU_GBP",
                        "CH20_CHF", "ESPIX_EUR",
                        "XPT_USD",
                        "EUR_AUD", "SOYBN_USD",
                        "US2000_USD",
                        "XAG_USD", "XAG_EUR", "XAG_CAD", "XAG_AUD", "XAG_GBP", "XAG_JPY", "XAG_SGD", "XAG_CHF",
                        "XAG_NZD",
                        "XAU_USD", "XAU_EUR", "XAU_CAD", "XAU_AUD", "XAU_GBP", "XAU_JPY", "XAU_SGD", "XAU_CHF",
                        "XAU_NZD",
                        "BTC_USD", "LTC_USD", "BCH_USD",
                        "US30_USD", "FR40_EUR", "US2000_USD", "CH20_CHF", "SPX500_USD", "AU200_AUD",
                        "JP225_USD", "JP225Y_JPY", "SG30_SGD", "EU50_EUR", "HK33_HKD",
                        "USB02Y_USD", "USB05Y_USD", "USB30Y_USD", "USB10Y_USD", "UK100_GBP", "GBP_CAD"))
      )

    asset_infor <- get_instrument_info()

    Daily_Data_Trades_ask <-
      read_all_asset_data_intra_day(
        asset_list_oanda = asset_list_oanda,
        save_path_oanda_assets = "C:/Users/Nikhil Chandra/Documents/Asset Data/oanda_data/",
        read_csv_or_API = "API",
        time_frame = "D",
        bid_or_ask = "ask",
        how_far_back = 5000,
        start_date = "2011-01-01"
      )

    Daily_Data_Trades_ask <- Daily_Data_Trades_ask %>% map_dfr(bind_rows)
    Daily_Data_Trades_ask <- Daily_Data_Trades_ask %>%
      mutate(Date = as_date(Date))

    return(Daily_Data_Trades_ask)

  }


#' get_SPX_US2000_XAG_XAU
#'
#' @returns
#' @export
#'
#' @examples
get_Port_Buy_Data <- function(
    db_location = db_location,
    start_date = "2016-01-01",
    end_date = today() %>% as.character(),
    time_frame = "H1"
) {

  SPX <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "SPX500_USD",
    keep_bid_to_ask = TRUE
  )

  US2000 <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "US2000_USD",
    keep_bid_to_ask = TRUE
  )

  EUR50 <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "EU50_EUR",
    keep_bid_to_ask = TRUE
  )

  AU200_AUD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "AU200_AUD",
    keep_bid_to_ask = TRUE
  )

  SG30_SGD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "SG30_SGD",
    keep_bid_to_ask = TRUE
  )

  UK100_GBP <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "UK100_GBP",
    keep_bid_to_ask = TRUE
  )

  JP225Y_JPY <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "JP225Y_JPY",
    keep_bid_to_ask = TRUE
  )

  FR40_EUR <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "FR40_EUR",
    keep_bid_to_ask = TRUE
  )

  CH20_CHF <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "CH20_CHF",
    keep_bid_to_ask = TRUE
  )

  USB10Y_USD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "USB10Y_USD",
    keep_bid_to_ask = TRUE
  )

  USB02Y_USD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "USB02Y_USD",
    keep_bid_to_ask = TRUE
  )

  UK10YB_GBP <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "UK10YB_GBP",
    keep_bid_to_ask = TRUE
  )


  HK33_HKD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "HK33_HKD",
    keep_bid_to_ask = TRUE
  )

  EUR_USD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "EUR_USD",
    keep_bid_to_ask = TRUE
  )

  GBP_USD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "GBP_USD",
    keep_bid_to_ask = TRUE
  )

  USD_JPY <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "USD_JPY",
    keep_bid_to_ask = TRUE
  )

  AUD_USD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "AUD_USD",
    keep_bid_to_ask = TRUE
  )


  XAG <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "XAG_USD",
    keep_bid_to_ask = TRUE
  )

  XAG_EUR<- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "XAG_EUR",
    keep_bid_to_ask = TRUE
  )

  XAU <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "XAU_USD",
    keep_bid_to_ask = TRUE
  )

  XAU_EUR <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "XAU_EUR",
    keep_bid_to_ask = TRUE
  )

  XAU_GBP <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "XAU_GBP",
    keep_bid_to_ask = TRUE
  )

  XAG_GBP <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "XAG_GBP",
    keep_bid_to_ask = TRUE
  )

  EUR_GBP <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "EUR_GBP",
    keep_bid_to_ask = TRUE
  )

  WTICO_USD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "WTICO_USD",
    keep_bid_to_ask = TRUE
  )

  BCO_USD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "BCO_USD",
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

  XAU_JPY <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "XAU_JPY",
    keep_bid_to_ask = TRUE
  )

  XAG_JPY <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "XAG_JPY",
    keep_bid_to_ask = TRUE
  )

  XAU_AUD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "XAU_AUD",
    keep_bid_to_ask = TRUE
  )

  XAG_AUD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "XAG_AUD",
    keep_bid_to_ask = TRUE
  )

  USD_CAD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "USD_CAD",
    keep_bid_to_ask = TRUE
  )

  EUR_AUD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "ask",
      time_frame = time_frame,
      asset = "EUR_AUD",
      keep_bid_to_ask = TRUE
    )

  NZD_USD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "ask",
      time_frame = time_frame,
      asset = "NZD_USD",
      keep_bid_to_ask = TRUE
    )

  EUR_NZD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "ask",
      time_frame = time_frame,
      asset = "EUR_NZD",
      keep_bid_to_ask = TRUE
    )

  AUD_NZD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "ask",
      time_frame = time_frame,
      asset = "AUD_NZD",
      keep_bid_to_ask = TRUE
    )

  GBP_AUD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "ask",
      time_frame = time_frame,
      asset = "GBP_AUD",
      keep_bid_to_ask = TRUE
    )

  GBP_NZD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "ask",
      time_frame = time_frame,
      asset = "GBP_NZD",
      keep_bid_to_ask = TRUE
    )

  GBP_CAD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "ask",
      time_frame = time_frame,
      asset = "GBP_CAD",
      keep_bid_to_ask = TRUE
    )

  GBP_JPY <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "ask",
      time_frame = time_frame,
      asset = "GBP_JPY",
      keep_bid_to_ask = TRUE
    )

  USD_SGD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "ask",
      time_frame = time_frame,
      asset = "USD_SGD",
      keep_bid_to_ask = TRUE
    )

  EUR_JPY <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "ask",
      time_frame = time_frame,
      asset = "EUR_JPY",
      keep_bid_to_ask = TRUE
    )

  BTC_USD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "ask",
      time_frame = time_frame,
      asset = "BTC_USD",
      keep_bid_to_ask = TRUE
    )

  ETH_USD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "ask",
      time_frame = time_frame,
      asset = "ETH_USD",
      keep_bid_to_ask = TRUE
    )

  NATGAS_USD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "ask",
      time_frame = time_frame,
      asset = "NATGAS_USD",
      keep_bid_to_ask = TRUE
    )

  EUR_SEK <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "ask",
      time_frame = time_frame,
      asset = "EUR_SEK",
      keep_bid_to_ask = TRUE
    )

  USD_SEK <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "ask",
      time_frame = time_frame,
      asset = "USD_SEK",
      keep_bid_to_ask = TRUE
    )

  LTC_USD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "ask",
      time_frame = time_frame,
      asset = "LTC_USD",
      keep_bid_to_ask = TRUE
    )

  XAG_NZD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "ask",
      time_frame = time_frame,
      asset = "XAG_NZD",
      keep_bid_to_ask = TRUE
    )

  XAG_SPX_US2000_USD <-
    SPX %>%
    bind_rows(US2000) %>%
    bind_rows(EUR50) %>%
    bind_rows(SG30_SGD) %>%
    bind_rows(AU200_AUD) %>%
    bind_rows(XAG)%>%
    bind_rows(XAU) %>%
    bind_rows(USD_JPY) %>%
    bind_rows(AUD_USD) %>%
    bind_rows(UK100_GBP) %>%
    bind_rows(JP225Y_JPY) %>%
    bind_rows(FR40_EUR) %>%
    bind_rows(CH20_CHF) %>%
    bind_rows(USB10Y_USD) %>%
    bind_rows(USB02Y_USD) %>%
    bind_rows(UK10YB_GBP) %>%
    bind_rows(HK33_HKD) %>%
    bind_rows(EUR_USD) %>%
    bind_rows(GBP_USD) %>%
    bind_rows(XAG_EUR) %>%
    bind_rows(XAU_EUR) %>%
    bind_rows(XAU_GBP) %>%
    bind_rows(XAG_GBP) %>%
    bind_rows(EUR_GBP) %>%
    bind_rows(WTICO_USD) %>%
    bind_rows(BCO_USD) %>%
    bind_rows(XCU_USD) %>%
    bind_rows(XAU_JPY)%>%
    bind_rows(XAG_JPY) %>%
    bind_rows(XAU_AUD) %>%
    bind_rows(XAG_AUD) %>%
    bind_rows(USD_CAD) %>%
    bind_rows(EUR_AUD) %>%
    bind_rows(NZD_USD) %>%
    bind_rows(EUR_NZD) %>%
    bind_rows(AUD_NZD) %>%
    bind_rows(GBP_AUD) %>%
    bind_rows(GBP_NZD) %>%
    bind_rows(GBP_CAD) %>%
    bind_rows(GBP_JPY) %>%
    bind_rows(USD_SGD) %>%
    bind_rows(EUR_JPY) %>%
    bind_rows(BTC_USD) %>%
    bind_rows(ETH_USD) %>%
    bind_rows(NATGAS_USD) %>%
    bind_rows(EUR_SEK) %>%
    bind_rows(USD_SEK) %>%
    bind_rows(LTC_USD) %>%
    bind_rows(XAG_NZD)

  rm(SPX, US2000,EUR50, AU200_AUD, SG30_SGD, XAG, XAU, UK100_GBP, JP225Y_JPY, FR40_EUR, CH20_CHF,
     USB10Y_USD, USB02Y_USD, EUR_USD, GBP_USD,XAU_EUR, XAG_EUR, XAU_EUR, XAU_GBP, XAG_GBP, EUR_GBP,
     WTICO_USD, BCO_USD, XCU_USD, XAG_JPY, XAU_JPY,XAU_AUD, XAG_AUD, AUD_USD, USD_JPY,
     USD_CAD,
     EUR_AUD,
     NZD_USD,
     EUR_NZD,
     AUD_NZD,
     GBP_AUD,
     GBP_NZD,
     GBP_CAD,
     GBP_JPY,
     USD_SGD,
     EUR_JPY,
     BTC_USD,
     ETH_USD,
     NATGAS_USD,
     EUR_SEK,
     USD_SEK,
     LTC_USD,
     XAG_AUD,
     XAG_NZD)
  gc()

  SPX <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "SPX500_USD",
    keep_bid_to_ask = TRUE
  )

  US2000 <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "US2000_USD",
    keep_bid_to_ask = TRUE
  )

  EUR50 <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "EU50_EUR",
    keep_bid_to_ask = TRUE
  )

  AU200_AUD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "AU200_AUD",
    keep_bid_to_ask = TRUE
  )

  SG30_SGD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "SG30_SGD",
    keep_bid_to_ask = TRUE
  )

  USD_JPY <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "USD_JPY",
    keep_bid_to_ask = TRUE
  )

  AUD_USD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "AUD_USD",
    keep_bid_to_ask = TRUE
  )

  XAG <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "XAG_USD",
    keep_bid_to_ask = TRUE
  )

  XAU <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "XAU_USD",
    keep_bid_to_ask = TRUE
  )

  UK100_GBP <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "UK100_GBP",
    keep_bid_to_ask = TRUE
  )

  JP225Y_JPY <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "JP225Y_JPY",
    keep_bid_to_ask = TRUE
  )

  FR40_EUR <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "FR40_EUR",
    keep_bid_to_ask = TRUE
  )

  CH20_CHF <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "CH20_CHF",
    keep_bid_to_ask = TRUE
  )

  USB10Y_USD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "USB10Y_USD",
    keep_bid_to_ask = TRUE
  )

  USB02Y_USD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "USB02Y_USD",
    keep_bid_to_ask = TRUE
  )

  UK10YB_GBP <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "UK10YB_GBP",
    keep_bid_to_ask = TRUE
  )

  HK33_HKD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "HK33_HKD",
    keep_bid_to_ask = TRUE
  )

  EUR_USD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "EUR_USD",
    keep_bid_to_ask = TRUE
  )

  GBP_USD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "GBP_USD",
    keep_bid_to_ask = TRUE
  )

  XAG_EUR<- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "XAG_EUR",
    keep_bid_to_ask = TRUE
  )

  XAU_EUR<- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "XAU_EUR",
    keep_bid_to_ask = TRUE
  )

  XAU_GBP<- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "XAU_GBP",
    keep_bid_to_ask = TRUE
  )

  XAG_GBP<- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "XAG_GBP",
    keep_bid_to_ask = TRUE
  )

  EUR_GBP <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "EUR_GBP",
    keep_bid_to_ask = TRUE
  )

  WTICO_USD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "WTICO_USD",
    keep_bid_to_ask = TRUE
  )

  BCO_USD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "BCO_USD",
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

  XAU_JPY <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "XAU_JPY",
    keep_bid_to_ask = TRUE
  )

  XAG_JPY <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "XAG_JPY",
    keep_bid_to_ask = TRUE
  )


  XAU_AUD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "XAU_AUD",
    keep_bid_to_ask = TRUE
  )

  XAG_AUD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "XAG_AUD",
    keep_bid_to_ask = TRUE
  )

  USD_CAD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "USD_CAD",
    keep_bid_to_ask = TRUE
  )

  EUR_AUD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "bid",
      time_frame = time_frame,
      asset = "EUR_AUD",
      keep_bid_to_ask = TRUE
    )

  NZD_USD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "bid",
      time_frame = time_frame,
      asset = "NZD_USD",
      keep_bid_to_ask = TRUE
    )

  EUR_NZD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "bid",
      time_frame = time_frame,
      asset = "EUR_NZD",
      keep_bid_to_ask = TRUE
    )

  AUD_NZD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "bid",
      time_frame = time_frame,
      asset = "AUD_NZD",
      keep_bid_to_ask = TRUE
    )

  GBP_AUD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "bid",
      time_frame = time_frame,
      asset = "GBP_AUD",
      keep_bid_to_ask = TRUE
    )

  GBP_NZD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "bid",
      time_frame = time_frame,
      asset = "GBP_NZD",
      keep_bid_to_ask = TRUE
    )

  GBP_CAD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "bid",
      time_frame = time_frame,
      asset = "GBP_CAD",
      keep_bid_to_ask = TRUE
    )

  GBP_JPY <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "bid",
      time_frame = time_frame,
      asset = "GBP_JPY",
      keep_bid_to_ask = TRUE
    )

  USD_SGD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "bid",
      time_frame = time_frame,
      asset = "USD_SGD",
      keep_bid_to_ask = TRUE
    )

  EUR_JPY <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "bid",
      time_frame = time_frame,
      asset = "EUR_JPY",
      keep_bid_to_ask = TRUE
    )

  BTC_USD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "bid",
      time_frame = time_frame,
      asset = "BTC_USD",
      keep_bid_to_ask = TRUE
    )

  ETH_USD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "bid",
      time_frame = time_frame,
      asset = "ETH_USD",
      keep_bid_to_ask = TRUE
    )

  NATGAS_USD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "bid",
      time_frame = time_frame,
      asset = "NATGAS_USD",
      keep_bid_to_ask = TRUE
    )

  EUR_SEK <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "bid",
      time_frame = time_frame,
      asset = "EUR_SEK",
      keep_bid_to_ask = TRUE
    )

  USD_SEK <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "bid",
      time_frame = time_frame,
      asset = "USD_SEK",
      keep_bid_to_ask = TRUE
    )

  LTC_USD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "bid",
      time_frame = time_frame,
      asset = "LTC_USD",
      keep_bid_to_ask = TRUE
    )

  XAG_NZD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "bid",
      time_frame = time_frame,
      asset = "XAG_NZD",
      keep_bid_to_ask = TRUE
    )


  XAG_SPX_US2000_USD_short <-
    SPX %>%
    bind_rows(US2000) %>%
    bind_rows(EUR50) %>%
    bind_rows(SG30_SGD) %>%
    bind_rows(AU200_AUD) %>%
    bind_rows(USD_JPY) %>%
    bind_rows(AUD_USD) %>%
    bind_rows(XAG)%>%
    bind_rows(XAU) %>%
    bind_rows(UK100_GBP) %>%
    bind_rows(JP225Y_JPY) %>%
    bind_rows(FR40_EUR) %>%
    bind_rows(CH20_CHF) %>%
    bind_rows(USB10Y_USD) %>%
    bind_rows(USB02Y_USD) %>%
    bind_rows(UK10YB_GBP) %>%
    bind_rows(HK33_HKD) %>%
    bind_rows(EUR_USD) %>%
    bind_rows(GBP_USD) %>%
    bind_rows(XAG_EUR) %>%
    bind_rows(XAU_EUR) %>%
    bind_rows(XAU_GBP) %>%
    bind_rows(XAG_GBP) %>%
    bind_rows(EUR_GBP) %>%
    bind_rows(WTICO_USD) %>%
    bind_rows(BCO_USD) %>%
    bind_rows(XCU_USD) %>%
    bind_rows(XAU_JPY)%>%
    bind_rows(XAG_JPY) %>%
    bind_rows(XAU_AUD) %>%
    bind_rows(XAG_AUD) %>%
    bind_rows(USD_CAD) %>%
    bind_rows(EUR_AUD) %>%
    bind_rows(NZD_USD) %>%
    bind_rows(EUR_NZD) %>%
    bind_rows(AUD_NZD) %>%
    bind_rows(GBP_AUD) %>%
    bind_rows(GBP_NZD) %>%
    bind_rows(GBP_CAD) %>%
    bind_rows(GBP_JPY) %>%
    bind_rows(USD_SGD) %>%
    bind_rows(EUR_JPY) %>%
    bind_rows(BTC_USD) %>%
    bind_rows(ETH_USD) %>%
    bind_rows(NATGAS_USD) %>%
    bind_rows(EUR_SEK) %>%
    bind_rows(USD_SEK) %>%
    bind_rows(LTC_USD) %>%
    bind_rows(XAG_AUD) %>%
    bind_rows(XAG_NZD)

  rm(SPX, US2000,EUR50, AU200_AUD, SG30_SGD, XAG, XAU, UK100_GBP, JP225Y_JPY, FR40_EUR, CH20_CHF,
     GBP_USD, XAG_EUR, XAU_EUR, XAU_GBP, XAG_GBP, EUR_GBP, XCU_USD, WTICO_USD, BCO_USD,
     USD_CAD,
     EUR_AUD,
     NZD_USD,
     EUR_NZD,
     AUD_NZD,
     GBP_AUD,
     GBP_NZD,
     GBP_CAD,
     GBP_JPY,
     USD_SGD,
     EUR_JPY,
     BTC_USD,
     ETH_USD,
     NATGAS_USD,
     EUR_SEK,
     USD_SEK,
     LTC_USD,
     XAG_AUD,
     XAG_NZD)
  gc()

  return(
    list(
      XAG_SPX_US2000_USD,
      XAG_SPX_US2000_USD_short
    )
  )

}


#' create_NN_Idices_Silver_H1Vers_data
#'
#' @return
#' @export
#'
#' @examples
create_LM_Hourly_Portfolio_Buy <-
  function(SPX_US2000_XAG,
           raw_macro_data,
           actual_wins_losses = actual_wins_losses,
           lag_days = 1,
           stop_value_var = 15,
           profit_value_var = 20,
           use_PCA_vars = FALSE,
           period_var,
           bin_factor = 1) {

    # assets_to_return <- dependant_var_name

    # aus_macro_data <-
    #   get_AUS_Indicators(raw_macro_data,
    #                      lag_days = lag_days,
    #                      first_difference = TRUE
    #   ) %>%
    #   janitor::clean_names()
    #
    # # nzd_macro_data <-
    # #   get_NZD_Indicators(raw_macro_data,
    # #                      lag_days = lag_days,
    # #                      first_difference = TRUE
    # #   ) %>%
    # #   janitor::clean_names()
    #
    # usd_macro_data <-
    #   get_USD_Indicators(raw_macro_data,
    #                      lag_days = lag_days,
    #                      first_difference = TRUE
    #   ) %>%
    #   janitor::clean_names()
    #
    # cny_macro_data <-
    #   get_CNY_Indicators(raw_macro_data,
    #                      lag_days = lag_days,
    #                      first_difference = TRUE
    #   ) %>%
    #   janitor::clean_names()
    #
    # eur_macro_data <-
    #   get_EUR_Indicators(raw_macro_data,
    #                      lag_days = lag_days,
    #                      first_difference = TRUE
    #   ) %>%
    #   janitor::clean_names()
    #
    # aud_macro_vars <- names(aus_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    # # nzd_macro_vars <- names(nzd_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    # usd_macro_vars <- names(usd_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    # cny_macro_vars <- names(cny_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    # eur_macro_vars <- names(eur_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    # all_macro_vars <- c(
    #   aud_macro_vars,
    #   # nzd_macro_vars,
    #   usd_macro_vars,
    #   cny_macro_vars,
    #   eur_macro_vars)


    major_indices_log_cumulative <-
      c("SPX500_USD", "US2000_USD", "NAS100_USD", "SG30_SGD", "AU200_AUD", "EU50_EUR", "DE30_EUR",
        "UK100_GBP", "CH20_CHF", "FR40_EUR", "HK33_HKD") %>%
      map_dfr(
        ~
          create_log_cumulative_returns(
            asset_data_to_use =
              SPX_US2000_XAG %>%
              filter(Asset %in% c("SPX500_USD", "US2000_USD", "NAS100_USD", "SG30_SGD", "AU200_AUD", "EU50_EUR", "DE30_EUR",
                                        "UK100_GBP", "CH20_CHF", "FR40_EUR", "HK33_HKD")),
            asset_to_use = c(.x[1]),
            price_col = "Open",
            return_long_format = TRUE
          )
      ) %>%
      left_join(
        SPX_US2000_XAG %>%
          filter(Asset %in% c("SPX500_USD", "US2000_USD", "NAS100_USD", "SG30_SGD", "AU200_AUD", "EU50_EUR", "DE30_EUR",
                              "UK100_GBP", "CH20_CHF", "FR40_EUR", "HK33_HKD")) %>%
          dplyr::select(Date, Asset, Price, Open)
      )

    pc_equities_global <-
      create_PCA_Asset_Index(
        asset_data_to_use = major_indices_log_cumulative %>%
          group_by(Asset) %>%
          mutate(
            Return_Index_Diff = ((Price - Open)/Open)*100
          ) %>%
          ungroup() %>%
          filter(!is.na(Return_Index_Diff)),
        asset_to_use =  c("SPX500_USD", "US2000_USD", "NAS100_USD", "SG30_SGD", "AU200_AUD", "EU50_EUR", "DE30_EUR",
                               "UK100_GBP", "CH20_CHF", "FR40_EUR", "HK33_HKD"),
        price_col = "Return_Index_Diff",
        scale_values = TRUE
      )

    rm(major_indices_log_cumulative)

    pc_equities_global <-
      pc_equities_global %>%
      arrange(Date) %>%
      mutate(
        across(matches("PC[0-9]"), ~cumsum(.))
      ) %>%
      rename(PC1_Global_Equities = PC1,
             PC2_Global_Equities = PC2,
             PC3_Global_Equities = PC3,
             PC4_Global_Equities = PC4) %>%
      dplyr::select(-PC5, -PC6)

    major_bonds_log_cumulative <-
      c("UK10YB_GBP", "USB10Y_USD", "USB02Y_USD") %>%
      map_dfr(
        ~
          create_log_cumulative_returns(
            asset_data_to_use =
              SPX_US2000_XAG %>%
              filter(Asset %in% c("UK10YB_GBP", "USB10Y_USD", "USB02Y_USD")),
            asset_to_use = c(.x[1]),
            price_col = "Open",
            return_long_format = TRUE
          )
      ) %>%
      left_join(
        SPX_US2000_XAG %>%
          filter(Asset %in% c("UK10YB_GBP", "USB10Y_USD", "USB02Y_USD")) %>%
          dplyr::select(Date, Asset, Price, Open)
      )

    pc_bonds_global <-
      create_PCA_Asset_Index(
        asset_data_to_use = major_bonds_log_cumulative %>%
          group_by(Asset) %>%
          mutate(
            Return_Index_Diff = ((Price - Open)/Open)*100
          ) %>%
          ungroup() %>%
          filter(!is.na(Return_Index_Diff)),
        asset_to_use =  c("UK10YB_GBP", "USB10Y_USD", "USB02Y_USD"),
        price_col = "Return_Index_Diff",
        scale_values = TRUE
      ) %>%
      arrange(Date) %>%
      mutate(
        across(contains("PC[0-9]"), ~cumsum(.))
      ) %>%
      rename(PC1_Bonds_Equities = PC1,
             PC2_Bonds_Equities = PC2,
             PC3_Bonds_Equities = PC3)

    rm(major_bonds_log_cumulative)
    gc()

    major_gold_log_cumulative <-
      c("XAU_USD", "XAU_EUR", "XAU_GBP") %>%
      map_dfr(
        ~
          create_log_cumulative_returns(
            asset_data_to_use =
              SPX_US2000_XAG %>%
              filter(Asset %in% c("XAU_USD", "XAU_EUR", "XAU_GBP")),
            asset_to_use = c(.x[1]),
            price_col = "Open",
            return_long_format = TRUE
          )
      ) %>%
      left_join(
        SPX_US2000_XAG %>%
          filter(Asset %in% c("XAU_USD", "XAU_EUR", "XAU_GBP")) %>%
          dplyr::select(Date, Asset, Price, Open)
      )

    pc_gold_global <-
      create_PCA_Asset_Index(
        asset_data_to_use = major_gold_log_cumulative %>%
          group_by(Asset) %>%
          mutate(
            Return_Index_Diff = ((Price - Open)/Open)*100
          ) %>%
          ungroup() %>%
          filter(!is.na(Return_Index_Diff)),
        asset_to_use =  c("XAU_USD", "XAU_EUR", "XAU_GBP"),
        price_col = "Return_Index_Diff",
        scale_values = TRUE
      ) %>%
      arrange(Date) %>%
      mutate(
        across(contains("PC[0-9]"), ~cumsum(.))
      ) %>%
      rename(PC1_Gold_Equities = PC1,
             PC2_Gold_Equities = PC2,
             PC3_Gold_Equities = PC3)

    rm(major_gold_log_cumulative)
    gc()

    major_silver_log_cumulative <-
      c("XAG_USD", "XAG_EUR", "XAG_GBP") %>%
      map_dfr(
        ~
          create_log_cumulative_returns(
            asset_data_to_use =
              SPX_US2000_XAG %>%
              filter(Asset %in% c("XAG_USD", "XAG_EUR", "XAG_GBP")),
            asset_to_use = c(.x[1]),
            price_col = "Open",
            return_long_format = TRUE
          )
      ) %>%
      left_join(
        SPX_US2000_XAG %>%
          filter(Asset %in% c("XAG_USD", "XAG_EUR", "XAG_GBP")) %>%
          dplyr::select(Date, Asset, Price, Open)
      )

    pc_silver_global <-
      create_PCA_Asset_Index(
        asset_data_to_use = major_silver_log_cumulative %>%
          group_by(Asset) %>%
          mutate(
            Return_Index_Diff = ((Price - Open)/Open)*100
          ) %>%
          ungroup() %>%
          filter(!is.na(Return_Index_Diff)),
        asset_to_use =  c("XAG_USD", "XAG_EUR", "XAG_GBP"),
        price_col = "Return_Index_Diff",
        scale_values = TRUE
      ) %>%
      arrange(Date) %>%
      mutate(
        across(contains("PC[0-9]"), ~cumsum(.))
      ) %>%
      rename(PC1_Silver_Equities = PC1,
             PC2_Silver_Equities = PC2,
             PC3_Silver_Equities = PC3)

    rm(major_silver_log_cumulative)
    gc()

    major_commod_log_cumulative <-
      c("XAU_USD", "XAG_USD", "XCU_USD", "WTICO_USD", "BCO_USD") %>%
      map_dfr(
        ~
          create_log_cumulative_returns(
            asset_data_to_use =
              SPX_US2000_XAG %>%
              filter(Asset %in% c("XAU_USD", "XAG_USD", "XCU_USD", "WTICO_USD", "BCO_USD")),
            asset_to_use = c(.x[1]),
            price_col = "Open",
            return_long_format = TRUE
          )
      ) %>%
      left_join(
        SPX_US2000_XAG %>%
          filter(Asset %in% c("XAU_USD", "XAG_USD", "XCU_USD", "WTICO_USD", "BCO_USD")) %>%
          dplyr::select(Date, Asset, Price, Open)
      )

    pc_commod_global <-
      create_PCA_Asset_Index(
        asset_data_to_use = major_commod_log_cumulative %>%
          group_by(Asset) %>%
          mutate(
            Return_Index_Diff = ((Price - Open)/Open)*100
          ) %>%
          ungroup() %>%
          filter(!is.na(Return_Index_Diff)),
        asset_to_use =  c("XAU_USD", "XAG_USD", "XCU_USD", "WTICO_USD", "BCO_USD"),
        price_col = "Return_Index_Diff",
        scale_values = TRUE
      ) %>%
      arrange(Date) %>%
      mutate(
        across(contains("PC[0-9]"), ~cumsum(.))
      ) %>%
      rename(PC1_Commod = PC1,
             PC2_Commod = PC2,
             PC3_Commod = PC3,
             PC4_Commod = PC4)

    rm(major_commod_log_cumulative)
    gc()

    copula_data <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("SPX500_USD", "US2000_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      )

    copula_data_SPX_XAU <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("SPX500_USD", "XAU_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-SPX500_USD, -SPX500_USD_log1_price, -SPX500_USD_quantiles_1, -SPX500_USD_tangent_angle1)

    copula_data_US2000_XAU <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("US2000_USD", "XAU_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-US2000_USD, -US2000_USD_log1_price, -US2000_USD_quantiles_1, -US2000_USD_tangent_angle1)%>%
      dplyr::select(-XAU_USD, -XAU_USD_log2_price, -XAU_USD_quantiles_2, -XAU_USD_tangent_angle2)

    copula_data_SPX_AU200 <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("SPX500_USD", "AU200_AUD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-SPX500_USD, -SPX500_USD_log1_price, -SPX500_USD_quantiles_1, -SPX500_USD_tangent_angle1)

    copula_data_US2000_AU200 <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("US2000_USD", "AU200_AUD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-US2000_USD, -US2000_USD_log1_price, -US2000_USD_quantiles_1, -US2000_USD_tangent_angle1)%>%
      dplyr::select(-AU200_AUD, -AU200_AUD_log2_price, -AU200_AUD_quantiles_2, -AU200_AUD_tangent_angle2)

    copula_data_AU200_AUD_XAG <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("AU200_AUD", "XAU_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-AU200_AUD, -AU200_AUD_log1_price, -AU200_AUD_quantiles_1, -AU200_AUD_tangent_angle1)%>%
      dplyr::select(-XAU_USD, -XAU_USD_log2_price, -XAU_USD_quantiles_2, -XAU_USD_tangent_angle2)

    copula_data_SPX500_EUR50 <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("SPX500_USD", "EU50_EUR"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-SPX500_USD, -SPX500_USD_log1_price, -SPX500_USD_quantiles_1, -SPX500_USD_tangent_angle1)

    copula_data_US2000_USD_EUR50 <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("US2000_USD", "EU50_EUR"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-US2000_USD, -US2000_USD_log1_price, -US2000_USD_quantiles_1, -US2000_USD_tangent_angle1)%>%
      dplyr::select(-EU50_EUR, -EU50_EUR_log2_price, -EU50_EUR_quantiles_2, -EU50_EUR_tangent_angle2)

    copula_data_XAU_USD_EU50_EUR <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("XAU_USD", "EU50_EUR"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAU_USD, -XAU_USD_log1_price, -XAU_USD_quantiles_1, -XAU_USD_tangent_angle1)%>%
      dplyr::select(-EU50_EUR, -EU50_EUR_log2_price, -EU50_EUR_quantiles_2, -EU50_EUR_tangent_angle2)

    copula_data_XAU_USD_EU50_EUR <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("XAG_USD", "EU50_EUR"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAG_USD, -XAG_USD_log1_price, -XAG_USD_quantiles_1, -XAG_USD_tangent_angle1)%>%
      dplyr::select(-EU50_EUR, -EU50_EUR_log2_price, -EU50_EUR_quantiles_2, -EU50_EUR_tangent_angle2)

    copula_data_AU200_AUD_EU50_EUR <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("AU200_AUD", "EU50_EUR"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-AU200_AUD, -AU200_AUD_log1_price, -AU200_AUD_quantiles_1, -AU200_AUD_tangent_angle1)%>%
      dplyr::select(-EU50_EUR, -EU50_EUR_log2_price, -EU50_EUR_quantiles_2, -EU50_EUR_tangent_angle2)

    copula_data_SG30_XAU_USD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("SG30_SGD", "XAU_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAU_USD, -XAU_USD_log2_price, -XAU_USD_quantiles_2, -XAU_USD_tangent_angle2)

    copula_data_SG30_SPX500_USD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("SG30_SGD", "SPX500_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-SG30_SGD, -SG30_SGD_log1_price, -SG30_SGD_quantiles_1, -SG30_SGD_tangent_angle1)%>%
      dplyr::select(-SPX500_USD, -SPX500_USD_log2_price, -SPX500_USD_quantiles_2, -SPX500_USD_tangent_angle2)

    copula_data_UK100_GBP_SPX500_USD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("UK100_GBP", "SPX500_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-SPX500_USD, -SPX500_USD_log2_price, -SPX500_USD_quantiles_2, -SPX500_USD_tangent_angle2)

    copula_data_UK100_GBP_EU50_EUR <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("UK100_GBP", "EU50_EUR"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-UK100_GBP, -UK100_GBP_log1_price, -UK100_GBP_quantiles_1, -UK100_GBP_tangent_angle1)%>%
      dplyr::select(-EU50_EUR, -EU50_EUR_log2_price, -EU50_EUR_quantiles_2, -EU50_EUR_tangent_angle2)

    copula_data_UK100_GBP_UK10YB_GBP <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("UK100_GBP", "UK10YB_GBP"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-UK100_GBP, -UK100_GBP_log1_price, -UK100_GBP_quantiles_1, -UK100_GBP_tangent_angle1)

    copula_data_UK100_GBP_XAU_USD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("UK100_GBP", "XAU_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-UK100_GBP, -UK100_GBP_log1_price, -UK100_GBP_quantiles_1, -UK100_GBP_tangent_angle1)%>%
      dplyr::select(-XAU_USD, -XAU_USD_log2_price, -XAU_USD_quantiles_2, -XAU_USD_tangent_angle2)

    copula_data_USB10Y_USD_SPX500_USD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("USB10Y_USD", "SPX500_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-SPX500_USD, -SPX500_USD_log2_price, -SPX500_USD_quantiles_2, -SPX500_USD_tangent_angle2)

    copula_data_CH20_CHF_SPX500_USD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("CH20_CHF", "SPX500_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-SPX500_USD, -SPX500_USD_log2_price, -SPX500_USD_quantiles_2, -SPX500_USD_tangent_angle2)

    copula_data_CH20_CHF_XAU_USD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("CH20_CHF", "XAU_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-CH20_CHF, -CH20_CHF_log1_price, -CH20_CHF_quantiles_1, -CH20_CHF_tangent_angle1) %>%
      dplyr::select(-XAU_USD, -XAU_USD_log2_price, -XAU_USD_quantiles_2, -XAU_USD_tangent_angle2)

    copula_data_CH20_CHF_EU50_EUR <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("CH20_CHF", "EU50_EUR"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-CH20_CHF, -CH20_CHF_log1_price, -CH20_CHF_quantiles_1, -CH20_CHF_tangent_angle1) %>%
      dplyr::select(-EU50_EUR, -EU50_EUR_log2_price, -EU50_EUR_quantiles_2, -EU50_EUR_tangent_angle2)

    copula_data_CH20_CHF_FR40_EUR <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("CH20_CHF", "FR40_EUR"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-CH20_CHF, -CH20_CHF_log1_price, -CH20_CHF_quantiles_1, -CH20_CHF_tangent_angle1) %>%
      dplyr::select(-FR40_EUR, -FR40_EUR_log2_price, -FR40_EUR_quantiles_2, -FR40_EUR_tangent_angle2)

    copula_data_FR40_EUR_SPX500_USD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("FR40_EUR", "SPX500_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-FR40_EUR, -FR40_EUR_log1_price, -FR40_EUR_quantiles_1, -FR40_EUR_tangent_angle1) %>%
      dplyr::select(-SPX500_USD, -SPX500_USD_log2_price, -SPX500_USD_quantiles_2, -SPX500_USD_tangent_angle2)

    copula_data_FR40_EUR_EUR_USD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("FR40_EUR", "EUR_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-FR40_EUR, -FR40_EUR_log1_price, -FR40_EUR_quantiles_1, -FR40_EUR_tangent_angle1)

    copula_data_FR40_EUR_XAU_USD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("FR40_EUR", "XAU_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-FR40_EUR, -FR40_EUR_log1_price, -FR40_EUR_quantiles_1, -FR40_EUR_tangent_angle1) %>%
      dplyr::select(-XAU_USD, -XAU_USD_log2_price, -XAU_USD_quantiles_2, -XAU_USD_tangent_angle2)

    copula_data_FR40_EUR_XAG_USD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("FR40_EUR", "XAG_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-FR40_EUR, -FR40_EUR_log1_price, -FR40_EUR_quantiles_1, -FR40_EUR_tangent_angle1) %>%
      dplyr::select(-XAG_USD, -XAG_USD_log2_price, -XAG_USD_quantiles_2, -XAG_USD_tangent_angle2)

    copula_data_FR40_EUR_USB10Y_USD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("FR40_EUR", "USB10Y_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-FR40_EUR, -FR40_EUR_log1_price, -FR40_EUR_quantiles_1, -FR40_EUR_tangent_angle1) %>%
      dplyr::select(-USB10Y_USD, -USB10Y_USD_log2_price, -USB10Y_USD_quantiles_2, -USB10Y_USD_tangent_angle2)

    copula_data_UK100_GBP_GBP_USD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("UK100_GBP", "GBP_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-UK100_GBP, -UK100_GBP_log1_price, -UK100_GBP_quantiles_1, -UK100_GBP_tangent_angle1)%>%
      dplyr::select(-GBP_USD, -GBP_USD_log2_price, -GBP_USD_quantiles_2, -GBP_USD_tangent_angle2)

    copula_data_EUR_USD_GBP_USD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("EUR_USD", "GBP_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-EUR_USD, -EUR_USD_log1_price, -EUR_USD_quantiles_1, -EUR_USD_tangent_angle1)%>%
      dplyr::select(-GBP_USD, -GBP_USD_log2_price, -GBP_USD_quantiles_2, -GBP_USD_tangent_angle2)

    copula_data_EUR_USD_EU50_EUR <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("EUR_USD", "EU50_EUR"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-EUR_USD, -EUR_USD_log1_price, -EUR_USD_quantiles_1, -EUR_USD_tangent_angle1)%>%
      dplyr::select(-EU50_EUR, -EU50_EUR_log2_price, -EU50_EUR_quantiles_2, -EU50_EUR_tangent_angle2)

    copula_data_SPX500_USD_HK33_HKD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("SPX500_USD", "HK33_HKD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-SPX500_USD, -SPX500_USD_log1_price, -SPX500_USD_quantiles_1, -SPX500_USD_tangent_angle1)

    copula_data_EU50_EUR_HK33_HKD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("EU50_EUR", "HK33_HKD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-EU50_EUR, -EU50_EUR_log1_price, -EU50_EUR_quantiles_1, -EU50_EUR_tangent_angle1)%>%
      dplyr::select(-HK33_HKD, -HK33_HKD_log2_price, -HK33_HKD_quantiles_2, -HK33_HKD_tangent_angle2)

    copula_data_XAG_USD_HK33_HKD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("XAG_USD", "HK33_HKD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAG_USD, -XAG_USD_log1_price, -XAG_USD_quantiles_1, -XAG_USD_tangent_angle1)%>%
      dplyr::select(-HK33_HKD, -HK33_HKD_log2_price, -HK33_HKD_quantiles_2, -HK33_HKD_tangent_angle2)

    copula_data_US2000_USD_HK33_HKD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("US2000_USD", "HK33_HKD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-US2000_USD, -US2000_USD_log1_price, -US2000_USD_quantiles_1, -US2000_USD_tangent_angle1)%>%
      dplyr::select(-HK33_HKD, -HK33_HKD_log2_price, -HK33_HKD_quantiles_2, -HK33_HKD_tangent_angle2)

    copula_data_XAU_USD_HK33_HKD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("XAU_USD", "HK33_HKD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAU_USD, -XAU_USD_log1_price, -XAU_USD_quantiles_1, -XAU_USD_tangent_angle1)%>%
      dplyr::select(-HK33_HKD, -HK33_HKD_log2_price, -HK33_HKD_quantiles_2, -HK33_HKD_tangent_angle2)

    copula_data_US2000_USD_UK100_GBP <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("US2000_USD", "UK100_GBP"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-US2000_USD, -US2000_USD_log1_price, -US2000_USD_quantiles_1, -US2000_USD_tangent_angle1)%>%
      dplyr::select(-UK100_GBP, -UK100_GBP_log2_price, -UK100_GBP_quantiles_2, -UK100_GBP_tangent_angle2)

    copula_data_US2000_USD_FR40_EUR <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("US2000_USD", "FR40_EUR"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-US2000_USD, -US2000_USD_log1_price, -US2000_USD_quantiles_1, -US2000_USD_tangent_angle1)%>%
      dplyr::select(-FR40_EUR, -FR40_EUR_log2_price, -FR40_EUR_quantiles_2, -FR40_EUR_tangent_angle2)

    copula_data_XAG_EUR_XAG_USD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("XAG_EUR", "XAG_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAG_USD, -XAG_USD_log2_price, -XAG_USD_quantiles_2, -XAG_USD_tangent_angle2)

    copula_data_XAU_EUR_XAU_USD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("XAU_EUR", "XAU_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAU_USD, -XAU_USD_log2_price, -XAU_USD_quantiles_2, -XAU_USD_tangent_angle2)

    copula_data_XAU_EUR_EUR_USD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("XAU_EUR", "EUR_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAU_EUR, -XAU_EUR_log1_price, -XAU_EUR_quantiles_1, -XAU_EUR_tangent_angle1)%>%
      dplyr::select(-EUR_USD, -EUR_USD_log2_price, -EUR_USD_quantiles_2, -EUR_USD_tangent_angle2)

    copula_data_XAG_EUR_EUR_USD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("XAG_EUR", "EUR_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAG_EUR, -XAG_EUR_log1_price, -XAG_EUR_quantiles_1, -XAG_EUR_tangent_angle1)%>%
      dplyr::select(-EUR_USD, -EUR_USD_log2_price, -EUR_USD_quantiles_2, -EUR_USD_tangent_angle2)

    copula_data_XAG_GBP_XAG_USD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("XAG_GBP", "XAG_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAG_USD, -XAG_USD_log2_price, -XAG_USD_quantiles_2, -XAG_USD_tangent_angle2)

    copula_data_XAU_GBP_XAU_USD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("XAU_GBP", "XAU_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAU_USD, -XAU_USD_log2_price, -XAU_USD_quantiles_2, -XAU_USD_tangent_angle2)

    copula_data_XAG_GBP_GBP_USD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("XAG_GBP", "GBP_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAG_GBP, -XAG_GBP_log1_price, -XAG_GBP_quantiles_1, -XAG_GBP_tangent_angle1)%>%
      dplyr::select(-GBP_USD, -GBP_USD_log2_price, -GBP_USD_quantiles_2, -GBP_USD_tangent_angle2)

    copula_data_XAU_GBP_GBP_USD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("XAU_GBP", "GBP_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAU_GBP, -XAU_GBP_log1_price, -XAU_GBP_quantiles_1, -XAU_GBP_tangent_angle1)%>%
      dplyr::select(-GBP_USD, -GBP_USD_log2_price, -GBP_USD_quantiles_2, -GBP_USD_tangent_angle2)

    copula_data_EUR_GBP_GBP_USD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("EUR_GBP", "GBP_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-GBP_USD, -GBP_USD_log2_price, -GBP_USD_quantiles_2, -GBP_USD_tangent_angle2)

    copula_data_EUR_GBP_EUR_USD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("EUR_GBP", "EUR_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-EUR_GBP, -EUR_GBP_log1_price, -EUR_GBP_quantiles_1, -EUR_GBP_tangent_angle1) %>%
      dplyr::select(-EUR_USD, -EUR_USD_log2_price, -EUR_USD_quantiles_2, -EUR_USD_tangent_angle2)

    copula_data_SPX500_USD_PC1_Global_Equities <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG %>%
          filter(Asset == "SPX500_USD") %>%
          bind_rows(
            pc_equities_global %>%
              dplyr::select(Date, Open = PC1_Global_Equities) %>%
              mutate(Asset = "PC1_Global_Equities")
          ),
        asset_to_use = c("SPX500_USD", "PC1_Global_Equities"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-SPX500_USD, -SPX500_USD_log1_price, -SPX500_USD_quantiles_1, -SPX500_USD_tangent_angle1) %>%
      dplyr::select(-PC1_Global_Equities)

    copula_data_US2000_USD_PC1_Global_Equities <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG %>%
          filter(Asset == "US2000_USD") %>%
          bind_rows(
            pc_equities_global %>%
              dplyr::select(Date, Open = PC1_Global_Equities) %>%
              mutate(Asset = "PC1_Global_Equities")
          ),
        asset_to_use = c("US2000_USD", "PC1_Global_Equities"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-US2000_USD, -US2000_USD_log1_price, -US2000_USD_quantiles_1, -US2000_USD_tangent_angle1) %>%
      dplyr::select(-PC1_Global_Equities, -PC1_Global_Equities_log2_price,
                    -PC1_Global_Equities_quantiles_2, -PC1_Global_Equities_tangent_angle2)

    copula_data_EU50_EUR_PC1_Global_Equities <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG %>%
          filter(Asset == "EU50_EUR") %>%
          bind_rows(
            pc_equities_global %>%
              dplyr::select(Date, Open = PC1_Global_Equities) %>%
              mutate(Asset = "PC1_Global_Equities")
          ),
        asset_to_use = c("EU50_EUR", "PC1_Global_Equities"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-EU50_EUR, -EU50_EUR_log1_price, -EU50_EUR_quantiles_1, -EU50_EUR_tangent_angle1) %>%
      dplyr::select(-PC1_Global_Equities, -PC1_Global_Equities_log2_price,
                    -PC1_Global_Equities_quantiles_2, -PC1_Global_Equities_tangent_angle2)

    # copula_data_XAG_JPY_XAG_USD <-
    #   estimating_dual_copula(
    #     asset_data_to_use = SPX_US2000_XAG,
    #     asset_to_use = c("XAG_JPY", "XAG_USD"),
    #     price_col = "Open",
    #     rolling_period = 100,
    #     samples_for_MLE = 0.15,
    #     test_samples = 0.85
    #   ) %>%
    #   dplyr::select(-XAG_USD, -XAG_USD_log2_price, -XAG_USD_quantiles_2, -XAG_USD_tangent_angle2)
    #
    # copula_data_XAU_JPY_XAU_USD <-
    #   estimating_dual_copula(
    #     asset_data_to_use = SPX_US2000_XAG,
    #     asset_to_use = c("XAU_JPY", "XAU_USD"),
    #     price_col = "Open",
    #     rolling_period = 100,
    #     samples_for_MLE = 0.15,
    #     test_samples = 0.85
    #   ) %>%
    #   dplyr::select(-XAU_USD, -XAU_USD_log2_price, -XAU_USD_quantiles_2, -XAU_USD_tangent_angle2)
    #
    # copula_data_XAG_JPY_XAG_EUR <-
    #   estimating_dual_copula(
    #     asset_data_to_use = SPX_US2000_XAG,
    #     asset_to_use = c("XAG_JPY", "XAG_EUR"),
    #     price_col = "Open",
    #     rolling_period = 100,
    #     samples_for_MLE = 0.15,
    #     test_samples = 0.85
    #   ) %>%
    #   dplyr::select(-XAG_JPY, -XAG_JPY_log1_price, -XAG_JPY_quantiles_1, -XAG_JPY_tangent_angle1) %>%
    #   dplyr::select(-XAG_EUR, -XAG_EUR_log2_price, -XAG_EUR_quantiles_2, -XAG_EUR_tangent_angle2)

    # copula_data_WTICO_USD_BCO_USD <-
    #   estimating_dual_copula(
    #     asset_data_to_use = SPX_US2000_XAG,
    #     asset_to_use = c("WTICO_USD", "BCO_USD"),
    #     price_col = "Open",
    #     rolling_period = 100,
    #     samples_for_MLE = 0.15,
    #     test_samples = 0.85
    #   )
    #
    # # PC1_Commod_Equities
    # copula_data_WTICO_USD_Commod_PC1 <-
    #   estimating_dual_copula(
    #     asset_data_to_use = SPX_US2000_XAG  %>%
    #       filter(Asset == "WTICO_USD") %>%
    #       bind_rows(
    #         pc_commod_global %>%
    #           dplyr::select(Date, Open = PC1_Commod) %>%
    #           mutate(Asset = "PC1_Commod")
    #       ),
    #     asset_to_use = c("WTICO_USD", "PC1_Commod"),
    #     price_col = "Open",
    #     rolling_period = 100,
    #     samples_for_MLE = 0.15,
    #     test_samples = 0.85
    #   ) %>%
    #   dplyr::select(-WTICO_USD, -WTICO_USD_log1_price, -WTICO_USD_quantiles_1, -WTICO_USD_tangent_angle1) %>%
    #   dplyr::select(-PC1_Commod)
    #
    # copula_data_BCO_USD_Commod_PC1 <-
    #   estimating_dual_copula(
    #     asset_data_to_use = SPX_US2000_XAG  %>%
    #       filter(Asset == "BCO_USD") %>%
    #       bind_rows(
    #         pc_commod_global %>%
    #           dplyr::select(Date, Open = PC1_Commod) %>%
    #           mutate(Asset = "PC1_Commod")
    #       ),
    #     asset_to_use = c("BCO_USD", "PC1_Commod"),
    #     price_col = "Open",
    #     rolling_period = 100,
    #     samples_for_MLE = 0.15,
    #     test_samples = 0.85
    #   ) %>%
    #   dplyr::select(-BCO_USD, -BCO_USD_log1_price, -BCO_USD_quantiles_1, -BCO_USD_tangent_angle1) %>%
    #   dplyr::select(-PC1_Commod, -PC1_Commod_log2_price, -PC1_Commod_quantiles_2, -PC1_Commod_tangent_angle2)

    # copula_data_XCU_USD_Commod_PC1 <-
    #   estimating_dual_copula(
    #     asset_data_to_use = SPX_US2000_XAG  %>%
    #       filter(Asset == "XCU_USD") %>%
    #       bind_rows(
    #         pc_commod_global %>%
    #           dplyr::select(Date, Open = PC1_Commod) %>%
    #           mutate(Asset = "PC1_Commod")
    #       ),
    #     asset_to_use = c("XCU_USD", "PC1_Commod"),
    #     price_col = "Open",
    #     rolling_period = 100,
    #     samples_for_MLE = 0.15,
    #     test_samples = 0.85
    #   ) %>%
    #   dplyr::select(-XCU_USD, -XCU_USD_log1_price, -XCU_USD_quantiles_1, -XCU_USD_tangent_angle1) %>%
    #   dplyr::select(-PC1_Commod, -PC1_Commod_log2_price, -PC1_Commod_quantiles_2, -PC1_Commod_tangent_angle2)
    #
    # copula_data_WTICO_USD_Commod_PC2 <-
    #   estimating_dual_copula(
    #     asset_data_to_use = SPX_US2000_XAG  %>%
    #       filter(Asset == "WTICO_USD") %>%
    #       bind_rows(
    #         pc_commod_global %>%
    #           dplyr::select(Date, Open = PC2_Commod) %>%
    #           mutate(Asset = "PC2_Commod")
    #       ),
    #     asset_to_use = c("WTICO_USD", "PC2_Commod"),
    #     price_col = "Open",
    #     rolling_period = 100,
    #     samples_for_MLE = 0.15,
    #     test_samples = 0.85
    #   ) %>%
    #   dplyr::select(-WTICO_USD, -WTICO_USD_log1_price, -WTICO_USD_quantiles_1, -WTICO_USD_tangent_angle1) %>%
    #   dplyr::select(-PC2_Commod)
    #
    # copula_data_BCO_USD_Commod_PC2 <-
    #   estimating_dual_copula(
    #     asset_data_to_use = SPX_US2000_XAG  %>%
    #       filter(Asset == "BCO_USD") %>%
    #       bind_rows(
    #         pc_commod_global %>%
    #           dplyr::select(Date, Open = PC2_Commod) %>%
    #           mutate(Asset = "PC2_Commod")
    #       ),
    #     asset_to_use = c("BCO_USD", "PC2_Commod"),
    #     price_col = "Open",
    #     rolling_period = 100,
    #     samples_for_MLE = 0.15,
    #     test_samples = 0.85
    #   ) %>%
    #   dplyr::select(-BCO_USD, -BCO_USD_log1_price, -BCO_USD_quantiles_1, -BCO_USD_tangent_angle1) %>%
    #   dplyr::select(-PC2_Commod, -PC2_Commod_log2_price, -PC2_Commod_quantiles_2, -PC2_Commod_tangent_angle2)

    gc()

    if(!is.null(bin_factor)) {
      binary_data_for_post_model <-
        actual_wins_losses %>%
        filter(profit_factor == profit_value_var)%>%
        filter(stop_factor == stop_value_var) %>%
        filter(periods_ahead == period_var) %>%
        group_by(Asset) %>%
        mutate(max_win = max(trade_return_dollar_aud, na.rm=T),
               max_loss = min(trade_return_dollar_aud, na.rm=T),
               quantile_bin = quantile(trade_return_dollar_aud, bin_factor, na.rm = T) ) %>%
        ungroup() %>%
        mutate(
          bin_var =
            case_when(
              trade_return_dollar_aud > 0  ~ "win",
              trade_return_dollar_aud <= 0~ "loss"
            )
        ) %>%
        dplyr::select(Date, bin_var, Asset, trade_col,
                      profit_factor, stop_factor, periods_ahead, trade_return_dollar_aud, estimated_margin)
    } else {
      binary_data_for_post_model <-
        actual_wins_losses %>%
        filter(profit_factor == profit_value_var)%>%
        filter(stop_factor == stop_value_var) %>%
        mutate(
          bin_var =
            case_when(
              trade_start_prices > trade_end_prices & trade_col == "Short" ~ "win",
              trade_start_prices <= trade_end_prices & trade_col == "Short" ~ "loss",

              trade_start_prices < trade_end_prices & trade_col == "Long" ~ "win",
              trade_start_prices >= trade_end_prices & trade_col == "Long" ~ "loss"

            )
        ) %>%
        dplyr::select(Date, bin_var, Asset, trade_col,
                      profit_factor, stop_factor,
                      trade_start_prices, trade_end_prices,
                      starting_stop_value, starting_profit_value,
                      trade_return_dollar_aud, periods_ahead)
    }


    gc()

    copula_data_macro <-
      SPX_US2000_XAG %>%
      dplyr::select(Date,Asset, Price, High, Low, Open ) %>%
      find_pivots_fib_max_min(how_far_back = 100) %>%
      rename(line_1_100 = line_1,
             line_10_100 = line_10,
             line_1_max_100 = line_1_max,
             line_10_max_100 = line_10_max,
             perc_line_1_100 = perc_line_1,
             perc_line_10_100 = perc_line_10,
             perc_line_1_to_10_100 = perc_line_1_to_10,
             perc_line_1_mean_100 = perc_line_1_mean,
             perc_line_1_sd_100 = perc_line_1_sd,
             perc_line_10_mean_100 = perc_line_10_mean,
             perc_line_10_sd_100 = perc_line_10_sd,
             perc_line_1_to_10_mean_100 = perc_line_1_to_10_mean,
             perc_line_1_to_10_sd_100 = perc_line_1_to_10_sd) %>%
      find_pivots_fib_max_min(how_far_back = 50)%>%
      rename(line_1_50 = line_1,
             line_10_50 = line_10,
             line_1_max_50 = line_1_max,
             line_10_max_50 = line_10_max,
             perc_line_1_50 = perc_line_1,
             perc_line_10_50 = perc_line_10,
             perc_line_1_to_10_50 = perc_line_1_to_10,
             perc_line_1_mean_50 = perc_line_1_mean,
             perc_line_1_sd_50 = perc_line_1_sd,
             perc_line_10_mean_50 = perc_line_10_mean,
             perc_line_10_sd_50 = perc_line_10_sd,
             perc_line_1_to_10_mean_50 = perc_line_1_to_10_mean,
             perc_line_1_to_10_sd_50 = perc_line_1_to_10_sd)%>%
      find_pivots_fib_max_min(how_far_back = 10)%>%
      rename(line_1_10 = line_1,
             line_10_10 = line_10,
             line_1_max_10 = line_1_max,
             line_10_max_10 = line_10_max,
             perc_line_1_10 = perc_line_1,
             perc_line_10_10 = perc_line_10,
             perc_line_1_to_10_10 = perc_line_1_to_10,
             perc_line_1_mean_10 = perc_line_1_mean,
             perc_line_1_sd_10 = perc_line_1_sd,
             perc_line_10_mean_10 = perc_line_10_mean,
             perc_line_10_sd_10 = perc_line_10_sd,
             perc_line_1_to_10_mean_10 = perc_line_1_to_10_mean,
             perc_line_1_to_10_sd_10 = perc_line_1_to_10_sd)

    rm(SPX_US2000_XAG)
    gc()
    Sys.sleep(2)
    gc()

    copula_data_macro <-
      copula_data_macro %>%
      left_join(copula_data) %>%
      left_join(copula_data_SPX_XAU) %>%
      left_join(copula_data_US2000_XAU) %>%
      left_join(copula_data_SPX_AU200) %>%
      left_join(copula_data_US2000_AU200) %>%
      left_join(copula_data_AU200_AUD_XAG) %>%
      left_join(copula_data_SPX500_EUR50) %>%
      left_join(copula_data_US2000_USD_EUR50) %>%
      left_join(copula_data_XAU_USD_EU50_EUR) %>%
      left_join(copula_data_AU200_AUD_EU50_EUR)%>%
      left_join(copula_data_SG30_XAU_USD)%>%
      left_join(copula_data_SG30_SPX500_USD) %>%
      left_join(copula_data_UK100_GBP_SPX500_USD) %>%
      left_join(copula_data_UK100_GBP_EU50_EUR) %>%
      left_join(copula_data_UK100_GBP_UK10YB_GBP) %>%
      left_join(copula_data_UK100_GBP_XAU_USD) %>%
      left_join(copula_data_USB10Y_USD_SPX500_USD)

    rm(copula_data,
           copula_data_SPX_XAU,
           copula_data_US2000_XAU,
           copula_data_SPX_AU200,
           copula_data_US2000_AU200,
           copula_data_AU200_AUD_XAG,
           copula_data_SPX500_EUR50,
           copula_data_US2000_USD_EUR50,
           copula_data_XAU_USD_EU50_EUR,
           copula_data_AU200_AUD_EU50_EUR,
           copula_data_SG30_XAU_USD,
           copula_data_SG30_SPX500_USD,
           copula_data_UK100_GBP_SPX500_USD,
           copula_data_UK100_GBP_EU50_EUR,
           copula_data_UK100_GBP_UK10YB_GBP,
           copula_data_UK100_GBP_XAU_USD,
           copula_data_USB10Y_USD_SPX500_USD)

    gc()
    gc()

    message("Made it to first rm()")

    copula_data_macro <- copula_data_macro %>%
      # left_join(copula_data_USB02Y_USD_SPX500_USD) %>%
      # left_join(copula_data_USB02Y_USD_USB10Y_USD) %>%
      left_join(copula_data_CH20_CHF_SPX500_USD) %>%
      left_join(copula_data_CH20_CHF_XAU_USD) %>%
      left_join(copula_data_CH20_CHF_EU50_EUR) %>%
      left_join(copula_data_CH20_CHF_FR40_EUR) %>%
      left_join(copula_data_FR40_EUR_SPX500_USD) %>%
      left_join(copula_data_FR40_EUR_EUR_USD) %>%
      left_join(copula_data_FR40_EUR_XAU_USD) %>%
      left_join(copula_data_FR40_EUR_XAG_USD) %>%
      left_join(copula_data_FR40_EUR_USB10Y_USD) %>%
      left_join(copula_data_UK100_GBP_GBP_USD) %>%
      left_join(copula_data_EUR_USD_GBP_USD) %>%
      left_join(copula_data_EUR_USD_EU50_EUR) %>%
      left_join(copula_data_SPX500_USD_HK33_HKD) %>%
      left_join(copula_data_EU50_EUR_HK33_HKD) %>%
      left_join(copula_data_XAG_USD_HK33_HKD) %>%
      left_join(copula_data_US2000_USD_HK33_HKD) %>%
      left_join(copula_data_XAU_USD_HK33_HKD) %>%
      left_join(copula_data_US2000_USD_UK100_GBP) %>%
      left_join(copula_data_US2000_USD_FR40_EUR) %>%
      left_join(copula_data_XAG_EUR_XAG_USD) %>%
      left_join(copula_data_XAU_EUR_XAU_USD) %>%
      left_join(copula_data_XAU_EUR_EUR_USD) %>%
      left_join(copula_data_XAG_EUR_EUR_USD) %>%
      left_join(copula_data_XAG_GBP_XAG_USD) %>%
      left_join(copula_data_XAU_GBP_XAU_USD) %>%
      left_join(copula_data_XAG_GBP_GBP_USD) %>%
      left_join(copula_data_XAU_GBP_GBP_USD) %>%
      left_join(copula_data_EUR_GBP_GBP_USD) %>%
      left_join(copula_data_EUR_GBP_EUR_USD) %>%
      left_join(pc_equities_global %>% dplyr::select(-Average_PCA)) %>%
      left_join(pc_bonds_global %>% dplyr::select(-Average_PCA)) %>%
      left_join(pc_gold_global %>% dplyr::select(-Average_PCA)) %>%
      left_join(pc_silver_global %>% dplyr::select(-Average_PCA)) %>%
      # left_join(pc_commod_global %>% dplyr::select(-Average_PCA)) %>%
      left_join(copula_data_SPX500_USD_PC1_Global_Equities) %>%
      left_join(copula_data_US2000_USD_PC1_Global_Equities) %>%
      left_join(copula_data_EU50_EUR_PC1_Global_Equities)
      # left_join(copula_data_XAG_JPY_XAG_USD) %>%
      # left_join(copula_data_XAU_JPY_XAU_USD) %>%
      # left_join(copula_data_XAG_JPY_XAG_EUR)
      # left_join(copula_data_WTICO_USD_BCO_USD) %>%
      # left_join(copula_data_WTICO_USD_Commod_PC1) %>%
      # left_join(copula_data_BCO_USD_Commod_PC1) %>%
      # left_join(copula_data_XCU_USD_Commod_PC1) %>%
      # left_join(copula_data_WTICO_USD_Commod_PC2) %>%
      # left_join(copula_data_BCO_USD_Commod_PC2) %>%

    rm(copula_data_XAG_JPY_XAG_EUR, copula_data_XAU_JPY_XAU_USD, copula_data_XAG_JPY_XAG_USD,
       copula_data_EU50_EUR_PC1_Global_Equities, copula_data_US2000_USD_PC1_Global_Equities,
       copula_data_SPX500_USD_PC1_Global_Equities, pc_silver_global, pc_gold_global,pc_bonds_global, pc_equities_global,
       copula_data_EUR_GBP_EUR_USD, copula_data_EUR_GBP_GBP_USD, copula_data_XAU_GBP_GBP_USD, copula_data_XAG_GBP_GBP_USD,
       copula_data_XAU_GBP_XAU_USD, copula_data_XAG_GBP_XAG_USD, copula_data_XAG_EUR_EUR_USD, copula_data_XAU_EUR_EUR_USD,
       copula_data_XAU_EUR_XAU_USD, copula_data_XAG_EUR_XAG_USD, copula_data_US2000_USD_FR40_EUR, copula_data_US2000_USD_UK100_GBP,
       copula_data_XAU_USD_HK33_HKD, copula_data_XAG_USD_HK33_HKD, copula_data_EU50_EUR_HK33_HKD, copula_data_SPX500_USD_HK33_HKD,
       copula_data_EUR_USD_EU50_EUR, copula_data_EUR_USD_GBP_USD, copula_data_UK100_GBP_GBP_USD,
       copula_data_FR40_EUR_USB10Y_USD,copula_data_FR40_EUR_XAG_USD, copula_data_FR40_EUR_XAU_USD,
       copula_data_FR40_EUR_EUR_USD, copula_data_FR40_EUR_SPX500_USD, copula_data_CH20_CHF_FR40_EUR,
       copula_data_CH20_CHF_EU50_EUR, copula_data_CH20_CHF_XAU_USD, copula_data_CH20_CHF_SPX500_USD,
       copula_data_USB10Y_USD_SPX500_USD, copula_data_UK100_GBP_XAU_USD, copula_data_UK100_GBP_UK10YB_GBP,
       copula_data_UK100_GBP_EU50_EUR, copula_data_UK100_GBP_SPX500_USD, copula_data_SG30_SPX500_USD,
       copula_data_SG30_XAU_USD, copula_data_AU200_AUD_EU50_EUR, copula_data_XAU_USD_EU50_EUR,
       copula_data_US2000_USD_EUR50, copula_data_SPX500_EUR50, copula_data_AU200_AUD_XAG, copula_data_US2000_AU200,
       SPX_US2000_XAG)
    gc()

    gc()

    message("Made it to second rm()")

    min_allowable_date <-
      copula_data_macro %>%
      ungroup() %>%
      filter(if_all(everything(), ~ !is.na(.))) %>%
      pull(Date) %>% min()
    copula_data_macro <-
      copula_data_macro %>%
      ungroup() %>%
      filter(Date >= min_allowable_date)
    gc()

    copula_data_macro <-
      copula_data_macro %>%
      left_join(binary_data_for_post_model)
    gc()
    rm(binary_data_for_post_model)
    gc()

    message("Made it to third rm() left join of actuals")

    copula_data_macro <-
      copula_data_macro %>%
      group_by(Asset) %>%
      arrange(Date, .by_group = TRUE) %>%
      # group_by(Asset) %>%
      # fill(matches(all_macro_vars, ignore.case = FALSE), .direction = "down") %>%
      ungroup()

    message("Made it to to arrangement of copula_data by Date")

    copula_data_macro <-
      copula_data_macro %>%
      group_by(Asset) %>%
      fill(contains("quantiles"), .direction = "down") %>%
      group_by(Asset) %>%
      fill(contains("tangent"), .direction = "down") %>%
      group_by(Asset) %>%
      fill(contains("cor"), .direction = "down") %>%
      group_by(Asset) %>%
      fill(contains("PC"), .direction = "down") %>%
      group_by(Asset) %>%
      fill(contains("AUD|XAG|XAU|SPX|US2000|FR40|EUR|USD|JPY|HK33"), .direction = "down") %>%
      # fill(everything(), .direction = "down") %>%
      ungroup()

    gc()

    message("Made it to finish creating data")

    max_date_in_testing_data <- copula_data_macro %>%
      distinct(Date) %>% pull(Date) %>% max(na.rm = T)
    message(glue::glue("Max date in Complete data: {max_date_in_testing_data}"))

    min_allowable_date <-
      copula_data_macro %>%
      ungroup() %>%
      filter(if_all(everything(), ~ !is.na(.))) %>%
      pull(Date) %>% min()

    gc()

    copula_data_macro <-
      copula_data_macro %>%
      ungroup() %>%
      filter(Date >= min_allowable_date)

    gc()

    lm_quant_vars <- names(copula_data_macro) %>% keep(~ str_detect(.x,"quantiles|tangent|cor|PC|perc_|line_|Support|Resistance|Bear|Bull") )

    if(use_PCA_vars == TRUE) {
      lm_vars1 <- c(
                    # PC_macro_vars,
                    lm_quant_vars,
                    "lagged_var_13",
                    "lagged_var_21",
                    "lagged_var_3_ma",
                    # "hour_of_day", "day_of_week"
      )
    } else {
      lm_vars1 <- c(
                    # all_macro_vars,
                    lm_quant_vars,
                    "lagged_var_13",
                    "lagged_var_3_ma",
                    "lagged_var_21_ma"
                    # "hour_of_day", "day_of_week"
      )
    }

    return(
      list(
        "copula_data_macro" = copula_data_macro,
        "lm_vars1" =
          lm_vars1 %>%
          keep( ~ !str_detect(.x, "sd") & !str_detect(.x, "tangent") )
      )
    )

  }

#' read_NNs_create_preds
#'
#' @param copula_data_macro
#' @param lm_vars1
#' @param dependant_var_name
#' @param NN_path
#' @param lm_test_prop
#' @param testing_min_date
#' @param trade_direction_var
#' @param NN_index_to_choose
#' @param stop_value_var
#' @param profit_value_var
#' @param analysis_threshs
#'
#' @return
#' @export
#'
#' @examples
read_NNs_create_preds_portfolio <- function(
    copula_data_macro = copula_data_macro,
    lm_vars1 = lm_vars1,
    dependant_var_name = "AUD_USD",
    NN_path = "C:/Users/Nikhil Chandra/Documents/trade_data/asset_specific_NNs/",
    testing_min_date = "2021-01-01",
    trade_direction_var = "Long",
    NN_index_to_choose = "",
    stop_value_var = 15,
    profit_value_var = 20,
    run_logit_instead = FALSE,
    lag_price_col = "Price",
    return_tagged_trades = FALSE,
    model_type = "logit",
    LM_interval = "fit"
) {

  set.seed(round(runif(n = 1, min = 0, max = 100000)))

  deal_with_NAs_for_trade_flag <-
    copula_data_macro %>%
    ungroup() %>%
    filter(is.na(trade_col)) %>%
    dplyr::select(Date, Asset, Price, Open, Low, High,
                  matches(lm_vars1, ignore.case = FALSE) ) %>%
    distinct() %>%
    mutate(
      trade_col = trade_direction_var,
      profit_factor = profit_value_var,
      stop_factor = stop_value_var
    )

  gc()

  max_date_in_testing_data <-
    deal_with_NAs_for_trade_flag %>%
    ungroup() %>%
    distinct(Date) %>%
    pull(Date) %>%
    max(na.rm = T)

  gc()

  message(glue::glue("Max date in NA Flag data: {max_date_in_testing_data}"))

  testing_data <-
    copula_data_macro %>%
    ungroup() %>%
    filter(!is.na(trade_col)) %>%
    bind_rows(deal_with_NAs_for_trade_flag) %>%
    filter(profit_factor == profit_value_var, stop_factor == stop_value_var) %>%
    filter(Date >= as_date(testing_min_date)) %>%
    filter(Asset == dependant_var_name) %>%
    filter(trade_col == trade_direction_var)

  gc()
  rm(copula_data_macro)
  gc()

  testing_data <-
    testing_data%>%
    group_by(Asset) %>%
    arrange(Date, .by_group = TRUE) %>%
    group_by(Asset) %>%
    mutate(
      lagged_var_1 = lag(!!as.name(lag_price_col), 1) - lag(!!as.name(lag_price_col), 2),
      lagged_var_2 = lag(lagged_var_1, 2),
      lagged_var_3 = lag(lagged_var_1, 3),
      lagged_var_5 = lag(lagged_var_1, 5),
      lagged_var_8 = lag(lagged_var_1, 8),
      lagged_var_13 = lag(lagged_var_1, 13),
      lagged_var_21 = lag(lagged_var_1, 21),

      fib_1 = lagged_var_1 + lagged_var_2,
      fib_2 = lagged_var_2 + lagged_var_3,
      fib_3 = lagged_var_3 + lagged_var_5,
      fib_4 = lagged_var_5 + lagged_var_8,
      fib_5 = lagged_var_8 + lagged_var_13,
      fib_6 = lagged_var_13 + lagged_var_21,

      lagged_var_3_ma = slider::slide_dbl(.x = lagged_var_1,
                                          .f = ~ mean(.x, na.rm = T),
                                          .before = 3),

      lagged_var_5_ma = slider::slide_dbl(.x = lagged_var_1,
                                          .f = ~ mean(.x, na.rm = T),
                                          .before = 5),

      lagged_var_8_ma = slider::slide_dbl(.x = lagged_var_1,
                                          .f = ~ mean(.x, na.rm = T),
                                          .before = 8),

      lagged_var_13_ma = slider::slide_dbl(.x = lagged_var_1,
                                           .f = ~ mean(.x, na.rm = T),
                                           .before = 13),

      lagged_var_21_ma = slider::slide_dbl(.x = lagged_var_1,
                                           .f = ~ mean(.x, na.rm = T),
                                           .before = 21)
    ) %>%
    ungroup()

  gc()

  testing_data <-
    testing_data%>%
    group_by(Asset) %>%
    arrange(Date, .by_group = TRUE) %>%
    group_by(Asset) %>%
    # fill(!matches(c("bin_var","trade_col"), ignore.case = FALSE), .direction = "down") %>%
    group_by(Asset) %>%
    fill(contains("quantiles"), .direction = "down") %>%
    group_by(Asset) %>%
    fill(contains("tangent"), .direction = "down") %>%
    group_by(Asset) %>%
    fill(contains("cor"), .direction = "down") %>%
    group_by(Asset) %>%
    fill(contains("PC"), .direction = "down") %>%
    group_by(Asset) %>%
    fill(contains("AUD|XAG|XAU|SPX|US2000|FR40|EUR|USD|JPY|HK33"), .direction = "down") %>%
    # fill(everything(), .direction = "down") %>%
    ungroup() %>%
    distinct()


  max_date_in_testing_data <- testing_data %>% pull(Date) %>% max(na.rm = T)
  message(glue::glue("Max date in testing data: {max_date_in_testing_data}"))

  if(run_logit_instead == FALSE) {
    NNs_compiled <-
      fs::dir_info(NN_path)  %>%
      filter(str_detect(path, as.character(glue::glue("{dependant_var_name}_NN_{NN_index_to_choose}")) )) %>%
      split(.$path, drop = FALSE) %>%
      map_dfr(
        ~
          tibble( pred = predict(object = readRDS(.x$path[1]),newdata = testing_data) %>%
                    as.numeric()) %>%
          mutate(
            index = row_number()
          )
      )
  } else {


    if(model_type %in% c("logit", "probit")) {
      NNs_compiled <-
        fs::dir_info(NN_path)  %>%
        filter(str_detect(path, as.character(glue::glue("{dependant_var_name}_GLM_{NN_index_to_choose}")) )) %>%
        split(.$path, drop = FALSE) %>%
        map_dfr(
          ~
            tibble( pred = predict.glm(object = readRDS(.x$path[1]),newdata = testing_data, type = "response") %>%
                      as.numeric()) %>%
            mutate(
              index = row_number()
            )
        )
    }

    if(model_type == "LM") {
      NNs_compiled <-
        fs::dir_info(NN_path)  %>%
        filter(str_detect(path, as.character(glue::glue("{dependant_var_name}_LM_{NN_index_to_choose}")) )) %>%
        split(.$path, drop = FALSE) %>%
        map_dfr(
          ~
            predict(object = readRDS(.x$path[1]),
                    newdata = testing_data,
                    interval = "confidence")%>%
            as_tibble() %>%
            mutate(
              pred := !!as.name(LM_interval)
            ) %>%
            mutate(
              index = row_number()
            )
        )
    }

  }

  message("Done NN's")
  message(dim(NNs_compiled))

  if(model_type != "LM") {
    NNs_compiled2 <-
      NNs_compiled %>%
      group_by(index) %>%
      summarise(
        pred = mean(pred, na.rm = T)
      )
  }

  if(model_type == "LM") {
    NNs_compiled2 <-
      NNs_compiled %>%
      group_by(index) %>%
      summarise(
        pred = mean(pred, na.rm = T),
        lwr = mean(lwr, na.rm = T),
        upr = mean(upr, na.rm = T)
      )
  }

  pred_NN  <- NNs_compiled2 %>% pull(pred) %>% as.numeric()

  if(model_type == "LM") {
    pred_NN_lwr  <- NNs_compiled2 %>% pull(lwr) %>% as.numeric()
    pred_NN_upr  <- NNs_compiled2 %>% pull(upr) %>% as.numeric()
  }

  check_preds <-
    pred_NN %>% keep(~ !is.na(.x)) %>% length()

  max_preds <-
    pred_NN %>% max(na.rm = T)

  message(glue::glue("Number of Non-NA NN preds: {check_preds}"))

  if(model_type == "LM") {
    post_testing_data <-
      testing_data %>%
      mutate(
        pred = pred_NN,
        pred_lwr = pred_NN_lwr,
        pred_upr  = pred_NN_upr
      )
  } else {
    post_testing_data <-
      testing_data %>%
      mutate(
        pred = pred_NN,
        pred_lwr = pred_NN,
        pred_upr  = pred_NN
      )
  }

  pred_at_max_date <-
    post_testing_data %>%
    ungroup() %>%
    slice_max(Date) %>%
    pull(pred) %>%
    as.numeric()

  max_date_in_post_testing_data <-
    post_testing_data %>%
    filter(!is.na(pred)) %>%
    pull(Date) %>%
    max(na.rm = T) %>%
    as.character()

  message(glue::glue("Date in Post-NN Prediction Data: {max_date_in_post_testing_data}"))
  message(glue::glue("Pred at max Date: {pred_at_max_date}"))

  trade_dollar_returns <-
    testing_data %>%
    filter(!is.na(bin_var)) %>%
    left_join(post_testing_data %>% dplyr::select(Date, Asset, pred, pred_lwr, pred_upr)) %>%
    dplyr::select(Date, Asset, profit_factor, stop_factor,
                  trade_returns_AUD = trade_return_dollar_aud,
                  trade_col, periods_ahead, pred, trade_start_prices ,
                  trade_end_prices, bin_var, pred_lwr, pred_upr ) %>%
    filter(profit_factor == profit_value_var, stop_factor == stop_value_var) %>%
    filter(Asset == dependant_var_name) %>%
    mutate(minimal_loss = min(trade_returns_AUD, na.rm = T),
           maximum_win = max(trade_returns_AUD, na.rm = T),
           average_win = mean(trade_returns_AUD, na.rm = T),
           low_quantile_win = quantile(trade_returns_AUD,0.25 ,na.rm = T),
           high_quantile_win = quantile(trade_returns_AUD,0.75 ,na.rm = T)  ) %>%
    dplyr::select(Date, trade_returns_AUD, Asset,
                  profit_factor , stop_factor, maximum_win, minimal_loss,
                  average_win, low_quantile_win, high_quantile_win, pred,
                  pred_lwr, pred_upr,
                  trade_start_prices , trade_end_prices, bin_var,
                  trade_col, periods_ahead) %>%
    distinct() %>%
    arrange(Date) %>%
    mutate(
      return_50_thresh = case_when(pred >= 0.5 ~ trade_returns_AUD, TRUE ~ 0),
      return_60_thresh = case_when(pred >= 0.6 ~ trade_returns_AUD, TRUE ~ 0),
      return_70_thresh = case_when(pred >= 0.7 ~ trade_returns_AUD, TRUE ~ 0),
      return_80_thresh = case_when(pred >= 0.8 ~ trade_returns_AUD, TRUE ~ 0),
      return_90_thresh = case_when(pred >= 0.9 ~ trade_returns_AUD, TRUE ~ 0),
      return_95_thresh = case_when(pred >= 0.95 ~ trade_returns_AUD, TRUE ~ 0),
      control_cumsum = cumsum(trade_returns_AUD),
      return_50_thresh_cumsum = cumsum(return_50_thresh),
      return_60_thresh_cumsum = cumsum(return_60_thresh),
      return_80_thresh_cumsum = cumsum(return_80_thresh)
    )

  if(return_tagged_trades == TRUE) {
    return(  tagged_trades <- post_testing_data %>%
               dplyr::select(Date, Asset, Price, Open, Low, High,profit_factor,
                             stop_factor,trade_col, pred))
  } else {
    return(trade_dollar_returns)
  }

}

#' create_NN_Idices_Silver_H1Vers_data
#'
#' @return
#' @export
#'
#' @examples
create_LM_Hourly_USD_GBP_EUR_Portfolio_Buy <-
  function(SPX_US2000_XAG,
           raw_macro_data,
           actual_wins_losses = actual_wins_losses,
           lag_days = 1,
           stop_value_var = 15,
           profit_value_var = 20,
           use_PCA_vars = FALSE,
           period_var) {

    major_gold_log_cumulative <-
      c("XAU_USD", "XAU_EUR", "XAU_GBP", "XAU_JPY", "XAU_AUD") %>%
      map_dfr(
        ~
          create_log_cumulative_returns(
            asset_data_to_use =
              SPX_US2000_XAG %>%
              filter(Asset %in% c("XAU_USD", "XAU_EUR", "XAU_GBP", "XAU_JPY", "XAU_AUD")),
            asset_to_use = c(.x[1]),
            price_col = "Open",
            return_long_format = TRUE
          )
      ) %>%
      left_join(
        SPX_US2000_XAG %>%
          filter(Asset %in% c("XAU_USD", "XAU_EUR", "XAU_GBP", "XAU_JPY", "XAU_AUD")) %>%
          dplyr::select(Date, Asset, Price, Open)
      )

    pc_gold_global <-
      create_PCA_Asset_Index(
        asset_data_to_use = major_gold_log_cumulative %>%
          group_by(Asset) %>%
          mutate(
            Return_Index_Diff = ((Price - Open)/Open)*100
          ) %>%
          ungroup() %>%
          filter(!is.na(Return_Index_Diff)),
        asset_to_use =  c("XAU_USD", "XAU_EUR", "XAU_GBP", "XAU_JPY", "XAU_AUD"),
        price_col = "Return_Index_Diff",
        scale_values = TRUE
      ) %>%
      arrange(Date) %>%
      mutate(
        across(contains("PC[0-9]"), ~cumsum(.))
      ) %>%
      rename(PC1_Gold = PC1,
             PC2_Gold = PC2,
             PC3_Gold = PC3) %>%
      mutate(
        across(
          c(PC1_Gold_Equities, PC2_Gold_Equities, PC3_Gold_Equities),
          .fns = ~ lag(.)
        )
      )

    rm(major_gold_log_cumulative)
    gc()

    major_silver_log_cumulative <-
      c("XAG_USD", "XAG_EUR", "XAG_GBP", "XAG_JPY", "XAG_AUD") %>%
      map_dfr(
        ~
          create_log_cumulative_returns(
            asset_data_to_use =
              SPX_US2000_XAG %>%
              filter(Asset %in% c("XAG_USD", "XAG_EUR", "XAG_GBP", "XAG_JPY", "XAG_AUD")),
            asset_to_use = c(.x[1]),
            price_col = "Open",
            return_long_format = TRUE
          )
      ) %>%
      left_join(
        SPX_US2000_XAG %>%
          filter(Asset %in% c("XAG_USD", "XAG_EUR", "XAG_GBP", "XAG_JPY", "XAG_AUD")) %>%
          dplyr::select(Date, Asset, Price, Open)
      )

    pc_silver_global <-
      create_PCA_Asset_Index(
        asset_data_to_use = major_silver_log_cumulative %>%
          group_by(Asset) %>%
          mutate(
            Return_Index_Diff = ((Price - Open)/Open)*100
          ) %>%
          ungroup() %>%
          filter(!is.na(Return_Index_Diff)),
        asset_to_use =  c("XAG_USD", "XAG_EUR", "XAG_GBP", "XAG_JPY", "XAG_AUD"),
        price_col = "Return_Index_Diff",
        scale_values = TRUE
      ) %>%
      arrange(Date) %>%
      mutate(
        across(contains("PC[0-9]"), ~cumsum(.))
      ) %>%
      rename(PC1_Silver = PC1,
             PC2_Silver = PC2,
             PC3_Silver = PC3) %>%
      mutate(
        across(
          c(PC1_Silver_Equities, PC2_Silver_Equities, PC3_Silver_Equities),
          .fns = ~ lag(.)
        )
      )

    rm(major_silver_log_cumulative)
    gc()


    major_Dollar_log_cumulative <-
      c("EUR_USD", "GBP_USD", "USD_JPY", "AUD_USD") %>%
      map_dfr(
        ~
          create_log_cumulative_returns(
            asset_data_to_use =
              SPX_US2000_XAG %>%
              filter(Asset %in% c("EUR_USD", "GBP_USD", "USD_JPY", "AUD_USD")),
            asset_to_use = c(.x[1]),
            price_col = "Open",
            return_long_format = TRUE
          )
      ) %>%
      left_join(
        SPX_US2000_XAG %>%
          filter(Asset %in% c("EUR_USD", "GBP_USD", "USD_JPY", "AUD_USD")) %>%
          dplyr::select(Date, Asset, Price, Open)
      )

    pc_Dollar_global <-
      create_PCA_Asset_Index(
        asset_data_to_use =
          major_Dollar_log_cumulative %>%
          group_by(Asset) %>%
          mutate(
            Return_Index_Diff = ((Price - Open)/Open)*100
          ) %>%
          ungroup() %>%
          filter(!is.na(Return_Index_Diff)),
        asset_to_use =  c("EUR_USD", "GBP_USD", "USD_JPY", "AUD_USD"),
        price_col = "Return_Index_Diff",
        scale_values = TRUE
      ) %>%
      arrange(Date) %>%
      mutate(
        across(contains("PC[0-9]"), ~cumsum(.))
      ) %>%
      rename(PC1_Dollar = PC1,
             PC2_Dollar = PC2,
             PC3_Dollar = PC3) %>%
      mutate(
        across(
          c(PC1_Dollar, PC2_Dollar, PC3_Dollar),
          .fns = ~ lag(.)
        )
      )

    rm(pc_Dollar_global)
    gc()


    copula_data_EUR_USD_GBP_USD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("EUR_USD", "GBP_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-EUR_USD, -EUR_USD_log1_price, -EUR_USD_quantiles_1, -EUR_USD_tangent_angle1)%>%
      dplyr::select(-GBP_USD, -GBP_USD_log2_price, -GBP_USD_quantiles_2, -GBP_USD_tangent_angle2)


    copula_data_XAG_EUR_XAG_USD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("XAG_EUR", "XAG_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAG_USD, -XAG_USD_log2_price, -XAG_USD_quantiles_2, -XAG_USD_tangent_angle2)

    copula_data_XAU_EUR_XAU_USD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("XAU_EUR", "XAU_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAU_USD, -XAU_USD_log2_price, -XAU_USD_quantiles_2, -XAU_USD_tangent_angle2)

    copula_data_XAU_EUR_EUR_USD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("XAU_EUR", "EUR_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAU_EUR, -XAU_EUR_log1_price, -XAU_EUR_quantiles_1, -XAU_EUR_tangent_angle1)%>%
      dplyr::select(-EUR_USD, -EUR_USD_log2_price, -EUR_USD_quantiles_2, -EUR_USD_tangent_angle2)

    copula_data_XAG_EUR_EUR_USD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("XAG_EUR", "EUR_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAG_EUR, -XAG_EUR_log1_price, -XAG_EUR_quantiles_1, -XAG_EUR_tangent_angle1)%>%
      dplyr::select(-EUR_USD, -EUR_USD_log2_price, -EUR_USD_quantiles_2, -EUR_USD_tangent_angle2)

    copula_data_XAG_GBP_XAG_USD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("XAG_GBP", "XAG_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAG_USD, -XAG_USD_log2_price, -XAG_USD_quantiles_2, -XAG_USD_tangent_angle2)

    copula_data_XAU_GBP_XAU_USD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("XAU_GBP", "XAU_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAU_USD, -XAU_USD_log2_price, -XAU_USD_quantiles_2, -XAU_USD_tangent_angle2)

    copula_data_XAG_GBP_GBP_USD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("XAG_GBP", "GBP_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAG_GBP, -XAG_GBP_log1_price, -XAG_GBP_quantiles_1, -XAG_GBP_tangent_angle1)%>%
      dplyr::select(-GBP_USD, -GBP_USD_log2_price, -GBP_USD_quantiles_2, -GBP_USD_tangent_angle2)

    copula_data_XAU_GBP_GBP_USD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("XAU_GBP", "GBP_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAU_GBP, -XAU_GBP_log1_price, -XAU_GBP_quantiles_1, -XAU_GBP_tangent_angle1)%>%
      dplyr::select(-GBP_USD, -GBP_USD_log2_price, -GBP_USD_quantiles_2, -GBP_USD_tangent_angle2)

    copula_data_EUR_GBP_GBP_USD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("EUR_GBP", "GBP_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-GBP_USD, -GBP_USD_log2_price, -GBP_USD_quantiles_2, -GBP_USD_tangent_angle2)

    copula_data_EUR_GBP_EUR_USD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("EUR_GBP", "EUR_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-EUR_GBP, -EUR_GBP_log1_price, -EUR_GBP_quantiles_1, -EUR_GBP_tangent_angle1) %>%
      dplyr::select(-EUR_USD, -EUR_USD_log2_price, -EUR_USD_quantiles_2, -EUR_USD_tangent_angle2)

    copula_data_XAU_JPY_GBP_USD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("XAU_JPY", "GBP_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAU_JPY, -XAU_JPY_log1_price, -XAU_JPY_quantiles_1, -XAU_JPY_tangent_angle1)%>%
      dplyr::select(-GBP_USD, -GBP_USD_log2_price, -GBP_USD_quantiles_2, -GBP_USD_tangent_angle2)

    copula_data_XAU_EUR_GBP_USD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("XAU_EUR", "GBP_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAU_EUR, -XAU_EUR_log1_price, -XAU_EUR_quantiles_1, -XAU_EUR_tangent_angle1)%>%
      dplyr::select(-GBP_USD, -GBP_USD_log2_price, -GBP_USD_quantiles_2, -GBP_USD_tangent_angle2)

    copula_data_XAU_EUR_EUR_GBP <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("XAU_EUR", "EUR_GBP"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAU_EUR, -XAU_EUR_log1_price, -XAU_EUR_quantiles_1, -XAU_EUR_tangent_angle1)%>%
      dplyr::select(-EUR_GBP, -EUR_GBP_log2_price, -EUR_GBP_quantiles_2, -EUR_GBP_tangent_angle2)

    copula_data_XAU_GBP_EUR_GBP <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("XAU_GBP", "EUR_GBP"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAU_GBP, -XAU_GBP_log1_price, -XAU_GBP_quantiles_1, -XAU_GBP_tangent_angle1)%>%
      dplyr::select(-EUR_GBP, -EUR_GBP_log2_price, -EUR_GBP_quantiles_2, -EUR_GBP_tangent_angle2)

    copula_data_XAG_GBP_EUR_GBP <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("XAG_GBP", "EUR_GBP"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAG_GBP, -XAG_GBP_log1_price, -XAG_GBP_quantiles_1, -XAG_GBP_tangent_angle1)%>%
      dplyr::select(-EUR_GBP, -EUR_GBP_log2_price, -EUR_GBP_quantiles_2, -EUR_GBP_tangent_angle2)

    copula_data_XAG_EUR_EUR_GBP <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("XAG_EUR", "EUR_GBP"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAG_EUR, -XAG_EUR_log1_price, -XAG_EUR_quantiles_1, -XAG_EUR_tangent_angle1)%>%
      dplyr::select(-EUR_GBP, -EUR_GBP_log2_price, -EUR_GBP_quantiles_2, -EUR_GBP_tangent_angle2)

    copula_data_XAG_USD_XAG_JPY <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("XAG_USD", "XAG_JPY"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAG_USD, -XAG_USD_log1_price, -XAG_USD_quantiles_1, -XAG_USD_tangent_angle1)

    copula_data_XAG_EUR_XAG_JPY <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("XAG_EUR", "XAG_JPY"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAG_EUR, -XAG_EUR_log1_price, -XAG_EUR_quantiles_1, -XAG_EUR_tangent_angle1) %>%
      dplyr::select(-XAG_JPY, -XAG_JPY_log2_price, -XAG_JPY_quantiles_2, -XAG_JPY_tangent_angle2)

    copula_data_XAG_GBP_XAG_JPY <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("XAG_GBP", "XAG_JPY"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAG_GBP, -XAG_GBP_log1_price, -XAG_GBP_quantiles_1, -XAG_GBP_tangent_angle1) %>%
      dplyr::select(-XAG_JPY, -XAG_JPY_log2_price, -XAG_JPY_quantiles_2, -XAG_JPY_tangent_angle2)

    copula_data_XAG_AUD_XAG_JPY <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("XAG_AUD", "XAG_JPY"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAG_JPY, -XAG_JPY_log2_price, -XAG_JPY_quantiles_2, -XAG_JPY_tangent_angle2)

    copula_data_XAG_USD_USD_JPY <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("XAG_USD", "USD_JPY"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAG_USD, -XAG_USD_log1_price, -XAG_USD_quantiles_1, -XAG_USD_tangent_angle1)

    copula_data_XAG_EUR_USD_JPY <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("XAG_EUR", "USD_JPY"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAG_EUR, -XAG_EUR_log1_price, -XAG_EUR_quantiles_1, -XAG_EUR_tangent_angle1) %>%
      dplyr::select(-USD_JPY, -USD_JPY_log2_price, -USD_JPY_quantiles_2, -USD_JPY_tangent_angle2)

    copula_data_XAU_USD_XAU_JPY <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("XAU_USD", "XAU_JPY"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAU_USD, -XAU_USD_log1_price, -XAU_USD_quantiles_1, -XAU_USD_tangent_angle1) %>%
      dplyr::select(-XAU_JPY, -XAU_JPY_log2_price, -XAU_JPY_quantiles_2, -XAU_JPY_tangent_angle2)

    copula_data_XAU_EUR_XAG_JPY <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("XAU_EUR", "XAG_JPY"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAU_EUR, -XAU_EUR_log1_price, -XAU_EUR_quantiles_1, -XAU_EUR_tangent_angle1) %>%
      dplyr::select(-XAG_JPY, -XAG_JPY_log2_price, -XAG_JPY_quantiles_2, -XAG_JPY_tangent_angle2)

    copula_data_XAU_GBP_XAG_JPY <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("XAU_GBP", "XAG_JPY"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAU_GBP, -XAU_GBP_log1_price, -XAU_GBP_quantiles_1, -XAU_GBP_tangent_angle1) %>%
      dplyr::select(-XAG_JPY, -XAG_JPY_log2_price, -XAG_JPY_quantiles_2, -XAG_JPY_tangent_angle2)

    copula_data_XAU_AUD_XAG_JPY <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("XAU_AUD", "XAG_JPY"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAU_AUD, -XAU_AUD_log1_price, -XAU_AUD_quantiles_1, -XAU_AUD_tangent_angle1) %>%
      dplyr::select(-XAG_JPY, -XAG_JPY_log2_price, -XAG_JPY_quantiles_2, -XAG_JPY_tangent_angle2)

    copula_data_XAU_USD_USD_JPY <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("XAU_USD", "USD_JPY"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAU_USD, -XAU_USD_log1_price, -XAU_USD_quantiles_1, -XAU_USD_tangent_angle1) %>%
      dplyr::select(-USD_JPY, -USD_JPY_log2_price, -USD_JPY_quantiles_2, -USD_JPY_tangent_angle2)

    copula_data_EUR_USD_PC1_Gold_PCA <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG %>%
          filter(Asset == "EUR_USD") %>%
          bind_rows(
            pc_gold_global %>%
              dplyr::select(Date, Open = PC1_Global_Equities) %>%
              mutate(Asset = "PC1_Gold")
          ),
        asset_to_use = c("EUR_USD", "PC1_Gold"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-EUR_USD, -EUR_USD_log1_price, -EUR_USD_quantiles_1, -EUR_USD_tangent_angle1) %>%
      dplyr::select(-PC1_Gold,
                    -PC1_Gold_log2_price,
                    -PC1_Gold_quantiles_2,
                    -PC1_Gold_tangent_angle2)

    copula_data_GBP_USD_PC1_Gold_PCA <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG %>%
          filter(Asset == "GBP_USD") %>%
          bind_rows(
            pc_gold_global %>%
              dplyr::select(Date, Open = PC1_Global_Equities) %>%
              mutate(Asset = "PC1_Gold")
          ),
        asset_to_use = c("GBP_USD", "PC1_Gold"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-GBP_USD, -GBP_USD_log1_price, -GBP_USD_quantiles_1, -GBP_USD_tangent_angle1) %>%
      dplyr::select(-PC1_Gold,
                    -PC1_Gold_log2_price,
                    -PC1_Gold_quantiles_2,
                    -PC1_Gold_tangent_angle2)

    copula_data_USD_JPY_PC1_Gold_PCA <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG %>%
          filter(Asset == "USD_JPY") %>%
          bind_rows(
            pc_gold_global %>%
              dplyr::select(Date, Open = PC1_Global_Equities) %>%
              mutate(Asset = "PC1_Gold")
          ),
        asset_to_use = c("USD_JPY", "PC1_Gold"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-USD_JPY, -USD_JPY_log1_price, -USD_JPY_quantiles_1, -USD_JPY_tangent_angle1) %>%
      dplyr::select(-PC1_Gold,
                    -PC1_Gold_log2_price,
                    -PC1_Gold_quantiles_2,
                    -PC1_Gold_tangent_angle2)

    copula_data_AUD_USD_PC1_Gold_PCA <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG %>%
          filter(Asset == "AUD_USD") %>%
          bind_rows(
            pc_gold_global %>%
              dplyr::select(Date, Open = PC1_Global_Equities) %>%
              mutate(Asset = "PC1_Gold")
          ),
        asset_to_use = c("AUD_USD", "PC1_Gold"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-AUD_USD, -AUD_USD_log1_price, -AUD_USD_quantiles_1, -AUD_USD_tangent_angle1) %>%
      dplyr::select(-PC1_Gold,
                    -PC1_Gold_log2_price,
                    -PC1_Gold_quantiles_2,
                    -PC1_Gold_tangent_angle2)

    copula_data_XAG_USD_PC1_Gold_PCA <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG %>%
          filter(Asset == "XAG_USD") %>%
          bind_rows(
            pc_gold_global %>%
              dplyr::select(Date, Open = PC1_Global_Equities) %>%
              mutate(Asset = "PC1_Gold")
          ),
        asset_to_use = c("XAG_USD", "PC1_Gold"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAG_USD, -XAG_USD_log1_price, -XAG_USD_quantiles_1, -XAG_USD_tangent_angle1) %>%
      dplyr::select(-PC1_Gold,
                    -PC1_Gold_log2_price,
                    -PC1_Gold_quantiles_2,
                    -PC1_Gold_tangent_angle2)

    copula_data_XAG_EUR_PC1_Gold_PCA <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG %>%
          filter(Asset == "XAG_EUR") %>%
          bind_rows(
            pc_gold_global %>%
              dplyr::select(Date, Open = PC1_Global_Equities) %>%
              mutate(Asset = "PC1_Gold")
          ),
        asset_to_use = c("XAG_EUR", "PC1_Gold"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAG_EUR, -XAG_EUR_log1_price, -XAG_EUR_quantiles_1, -XAG_EUR_tangent_angle1) %>%
      dplyr::select(-PC1_Gold,
                    -PC1_Gold_log2_price,
                    -PC1_Gold_quantiles_2,
                    -PC1_Gold_tangent_angle2)

    copula_data_XAG_GBP_PC1_Gold_PCA <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG %>%
          filter(Asset == "XAG_GBP") %>%
          bind_rows(
            pc_gold_global %>%
              dplyr::select(Date, Open = PC1_Global_Equities) %>%
              mutate(Asset = "PC1_Gold")
          ),
        asset_to_use = c("XAG_GBP", "PC1_Gold"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAG_GBP, -XAG_GBP_log1_price, -XAG_GBP_quantiles_1, -XAG_GBP_tangent_angle1) %>%
      dplyr::select(-PC1_Gold,
                    -PC1_Gold_log2_price,
                    -PC1_Gold_quantiles_2,
                    -PC1_Gold_tangent_angle2)

    copula_data_XAG_AUD_PC1_Gold_PCA <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG %>%
          filter(Asset == "XAG_AUD") %>%
          bind_rows(
            pc_gold_global %>%
              dplyr::select(Date, Open = PC1_Global_Equities) %>%
              mutate(Asset = "PC1_Gold")
          ),
        asset_to_use = c("XAG_AUD", "PC1_Gold"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAG_AUD, -XAG_AUD_log1_price, -XAG_AUD_quantiles_1, -XAG_AUD_tangent_angle1) %>%
      dplyr::select(-PC1_Gold,
                    -PC1_Gold_log2_price,
                    -PC1_Gold_quantiles_2,
                    -PC1_Gold_tangent_angle2)

    copula_data_XAU_USD_PC1_Gold_PCA <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG %>%
          filter(Asset == "XAU_USD") %>%
          bind_rows(
            pc_gold_global %>%
              dplyr::select(Date, Open = PC1_Global_Equities) %>%
              mutate(Asset = "PC1_Gold")
          ),
        asset_to_use = c("XAU_USD", "PC1_Gold"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAU_USD, -XAU_USD_log1_price, -XAU_USD_quantiles_1, -XAU_USD_tangent_angle1) %>%
      dplyr::select(-PC1_Gold,
                    -PC1_Gold_log2_price,
                    -PC1_Gold_quantiles_2,
                    -PC1_Gold_tangent_angle2)

    copula_data_XAU_EUR_PC1_Gold_PCA <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG %>%
          filter(Asset == "XAU_EUR") %>%
          bind_rows(
            pc_gold_global %>%
              dplyr::select(Date, Open = PC1_Global_Equities) %>%
              mutate(Asset = "PC1_Gold")
          ),
        asset_to_use = c("XAU_EUR", "PC1_Gold"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAU_EUR, -XAU_EUR_log1_price, -XAU_EUR_quantiles_1, -XAU_EUR_tangent_angle1) %>%
      dplyr::select(-PC1_Gold,
                    -PC1_Gold_log2_price,
                    -PC1_Gold_quantiles_2,
                    -PC1_Gold_tangent_angle2)

    copula_data_XAU_GBP_PC1_Gold_PCA <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG %>%
          filter(Asset == "XAU_GBP") %>%
          bind_rows(
            pc_gold_global %>%
              dplyr::select(Date, Open = PC1_Global_Equities) %>%
              mutate(Asset = "PC1_Gold")
          ),
        asset_to_use = c("XAU_GBP", "PC1_Gold"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAU_GBP, -XAU_GBP_log1_price, -XAU_GBP_quantiles_1, -XAU_GBP_tangent_angle1) %>%
      dplyr::select(-PC1_Gold,
                    -PC1_Gold_log2_price,
                    -PC1_Gold_quantiles_2,
                    -PC1_Gold_tangent_angle2)

    copula_data_XAU_AUD_PC1_Gold_PCA <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG %>%
          filter(Asset == "XAU_AUD") %>%
          bind_rows(
            pc_gold_global %>%
              dplyr::select(Date, Open = PC1_Global_Equities) %>%
              mutate(Asset = "PC1_Gold")
          ),
        asset_to_use = c("XAU_AUD", "PC1_Gold"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAU_AUD, -XAU_AUD_log1_price, -XAU_AUD_quantiles_1, -XAU_AUD_tangent_angle1) %>%
      dplyr::select(-PC1_Gold,
                    -PC1_Gold_log2_price,
                    -PC1_Gold_quantiles_2,
                    -PC1_Gold_tangent_angle2)

    copula_data_XAG_USD_PC1_Silver_PCA <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG %>%
          filter(Asset == "XAG_USD") %>%
          bind_rows(
            pc_silver_global %>%
              dplyr::select(Date, Open = PC1_Silver) %>%
              mutate(Asset = "PC1_Silver")
          ),
        asset_to_use = c("XAG_USD", "PC1_Silver"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAG_USD, -XAG_USD_log1_price, -XAG_USD_quantiles_1, -XAG_USD_tangent_angle1) %>%
      dplyr::select(-PC1_Silver,
                    -PC1_Silver_log2_price,
                    -PC1_Silver_quantiles_2,
                    -PC1_Silver_tangent_angle2)

    copula_data_XAG_EUR_PC1_Silver_PCA <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG %>%
          filter(Asset == "XAG_EUR") %>%
          bind_rows(
            pc_silver_global %>%
              dplyr::select(Date, Open = PC1_Silver) %>%
              mutate(Asset = "PC1_Silver")
          ),
        asset_to_use = c("XAG_EUR", "PC1_Silver"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAG_EUR, -XAG_EUR_log1_price, -XAG_EUR_quantiles_1, -XAG_EUR_tangent_angle1) %>%
      dplyr::select(-PC1_Silver,
                    -PC1_Silver_log2_price,
                    -PC1_Silver_quantiles_2,
                    -PC1_Silver_tangent_angle2)

    copula_data_XAG_GBP_PC1_Silver_PCA <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG %>%
          filter(Asset == "XAG_GBP") %>%
          bind_rows(
            pc_silver_global %>%
              dplyr::select(Date, Open = PC1_Silver) %>%
              mutate(Asset = "PC1_Silver")
          ),
        asset_to_use = c("XAG_GBP", "PC1_Silver"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAG_GBP, -XAG_GBP_log1_price, -XAG_GBP_quantiles_1, -XAG_GBP_tangent_angle1) %>%
      dplyr::select(-PC1_Silver,
                    -PC1_Silver_log2_price,
                    -PC1_Silver_quantiles_2,
                    -PC1_Silver_tangent_angle2)

    copula_data_XAG_JPY_PC1_Silver_PCA <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG %>%
          filter(Asset == "XAG_JPY") %>%
          bind_rows(
            pc_silver_global %>%
              dplyr::select(Date, Open = PC1_Silver) %>%
              mutate(Asset = "PC1_Silver")
          ),
        asset_to_use = c("XAG_JPY", "PC1_Silver"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAG_JPY, -XAG_JPY_log1_price, -XAG_JPY_quantiles_1, -XAG_JPY_tangent_angle1) %>%
      dplyr::select(-PC1_Silver,
                    -PC1_Silver_log2_price,
                    -PC1_Silver_quantiles_2,
                    -PC1_Silver_tangent_angle2)

    copula_data_PC1_GOLD_PC1_Silver_PCA <-
      estimating_dual_copula(
        asset_data_to_use =
          pc_gold_global %>%
          dplyr::select(Date, Open = PC1_Global_Equities) %>%
          mutate(Asset = "PC1_Gold") %>%
          bind_rows(
            pc_silver_global %>%
              dplyr::select(Date, Open = PC1_Silver) %>%
              mutate(Asset = "PC1_Silver")
          ),
        asset_to_use = c("PC1_Gold", "PC1_Silver"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-PC1_Gold,
                    -PC1_Gold_log1_price,
                    -PC1_Gold_quantiles_1,
                    -PC1_Gold_tangent_angle1) %>%
      dplyr::select(-PC1_Silver,
                    -PC1_Silver_log2_price,
                    -PC1_Silver_quantiles_2,
                    -PC1_Silver_tangent_angle2)

    gc()

    binary_data_for_post_model <-
      actual_wins_losses %>%
      filter(profit_factor == profit_value_var)%>%
      filter(stop_factor == stop_value_var) %>%
      filter(periods_ahead == period_var) %>%
      mutate(
        bin_var =
          case_when(
            trade_return_dollar_aud >0  ~ "win",
            trade_return_dollar_aud <=0~ "loss"

          )
      ) %>%
      dplyr::select(Date, bin_var, Asset, trade_col,
                    profit_factor, stop_factor, periods_ahead, trade_return_dollar_aud, estimated_margin)

    gc()

    copula_data_macro <-
      SPX_US2000_XAG %>%
      dplyr::select(Date,Asset, Price, High, Low, Open )

    rm(SPX_US2000_XAG)
    gc()
    Sys.sleep(2)

    copula_data_macro <-
      copula_data_macro %>%
      left_join(copula_data_EUR_USD_GBP_USD) %>%
      left_join(copula_data_XAG_EUR_XAG_USD) %>%
      left_join(copula_data_XAU_EUR_XAU_USD) %>%
      left_join(copula_data_XAU_EUR_EUR_USD) %>%
      left_join(copula_data_XAG_EUR_EUR_USD) %>%
      left_join(copula_data_XAG_GBP_XAG_USD) %>%
      left_join(copula_data_XAU_GBP_XAU_USD) %>%
      left_join(copula_data_XAG_GBP_GBP_USD) %>%
      left_join(copula_data_XAU_GBP_GBP_USD) %>%
      left_join(copula_data_EUR_GBP_GBP_USD) %>%
      left_join(copula_data_EUR_GBP_EUR_USD) %>%
      left_join(copula_data_XAU_JPY_GBP_USD) %>%
      left_join(copula_data_XAU_EUR_GBP_USD) %>%
      left_join(copula_data_XAU_EUR_EUR_GBP) %>%
      left_join(copula_data_XAU_GBP_EUR_GBP) %>%
      left_join(copula_data_XAG_GBP_EUR_GBP) %>%
      left_join(copula_data_XAG_EUR_EUR_GBP) %>%

      left_join(copula_data_XAG_USD_XAG_JPY) %>%
      left_join(copula_data_XAG_EUR_XAG_JPY) %>%
      left_join(copula_data_XAG_GBP_XAG_JPY) %>%
      left_join(copula_data_XAG_AUD_XAG_JPY) %>%
      left_join(copula_data_XAG_USD_USD_JPY) %>%
      left_join(copula_data_XAG_EUR_USD_JPY) %>%
      left_join(copula_data_XAU_USD_XAU_JPY) %>%
      left_join(copula_data_XAU_EUR_XAG_JPY) %>%
      left_join(copula_data_XAU_GBP_XAG_JPY) %>%
      left_join(copula_data_XAU_AUD_XAG_JPY) %>%
      left_join(copula_data_XAU_USD_USD_JPY) %>%

      left_join(copula_data_EUR_USD_PC1_Gold_PCA) %>%
      left_join(copula_data_GBP_USD_PC1_Gold_PCA) %>%
      left_join(copula_data_USD_JPY_PC1_Gold_PCA) %>%
      left_join(copula_data_XAG_USD_PC1_Gold_PCA) %>%
      left_join(copula_data_XAG_EUR_PC1_Gold_PCA) %>%
      left_join(copula_data_XAG_GBP_PC1_Gold_PCA) %>%
      left_join(copula_data_XAG_AUD_PC1_Gold_PCA) %>%
      left_join(copula_data_XAU_USD_PC1_Gold_PCA) %>%
      left_join(copula_data_XAU_EUR_PC1_Gold_PCA) %>%
      left_join(copula_data_XAU_GBP_PC1_Gold_PCA) %>%
      left_join(copula_data_XAU_AUD_PC1_Gold_PCA) %>%
      left_join(copula_data_AUD_USD_PC1_Gold_PCA) %>%

      left_join(copula_data_XAG_USD_PC1_Silver_PCA) %>%
      left_join(copula_data_XAG_EUR_PC1_Silver_PCA) %>%
      left_join(copula_data_XAG_GBP_PC1_Silver_PCA) %>%
      left_join(copula_data_XAG_JPY_PC1_Silver_PCA) %>%
      left_join(copula_data_PC1_GOLD_PC1_Silver_PCA) %>%
      left_join(pc_gold_global %>% dplyr::select(-Average_PCA)) %>%
      left_join(pc_Dollar_global %>% dplyr::select(-Average_PCA))%>%
      left_join(pc_silver_global %>% dplyr::select(-Average_PCA))

    rm( copula_data_EUR_USD_GBP_USD,
               copula_data_XAG_EUR_XAG_USD,
               copula_data_XAU_EUR_XAU_USD,
               copula_data_XAU_EUR_EUR_USD,
               copula_data_XAG_EUR_EUR_USD,
               copula_data_XAG_GBP_XAG_USD,
               copula_data_XAU_GBP_XAU_USD,
               copula_data_XAG_GBP_GBP_USD,
               copula_data_XAU_GBP_GBP_USD,
               copula_data_EUR_GBP_GBP_USD,
               copula_data_EUR_GBP_EUR_USD,
               copula_data_XAU_JPY_GBP_USD,
               copula_data_XAU_EUR_GBP_USD,
               copula_data_XAU_EUR_EUR_GBP,
               copula_data_XAU_GBP_EUR_GBP,
               copula_data_XAG_GBP_EUR_GBP,
               copula_data_XAG_EUR_EUR_GBP,
               copula_data_XAG_USD_XAG_JPY,
               copula_data_XAG_EUR_XAG_JPY,
               copula_data_XAG_GBP_XAG_JPY,
               copula_data_XAG_AUD_XAG_JPY,
               copula_data_XAG_USD_USD_JPY,
               copula_data_XAG_EUR_USD_JPY,
               copula_data_XAU_USD_XAU_JPY,
               copula_data_XAU_EUR_XAG_JPY,
               copula_data_XAU_GBP_XAG_JPY,
               copula_data_XAU_AUD_XAG_JPY,
               copula_data_XAU_USD_USD_JPY,
               copula_data_EUR_USD_PC1_Gold_PCA,
               copula_data_GBP_USD_PC1_Gold_PCA,
               copula_data_USD_JPY_PC1_Gold_PCA,
               copula_data_XAG_USD_PC1_Gold_PCA,
               copula_data_XAG_EUR_PC1_Gold_PCA,
               copula_data_XAG_GBP_PC1_Gold_PCA,
               copula_data_XAG_AUD_PC1_Gold_PCA,
               copula_data_XAU_USD_PC1_Gold_PCA,
               copula_data_XAU_EUR_PC1_Gold_PCA,
               copula_data_XAU_GBP_PC1_Gold_PCA,
               copula_data_XAU_AUD_PC1_Gold_PCA,
               copula_data_AUD_USD_PC1_Gold_PCA,

              pc_gold_global,
              pc_silver_global,
              pc_Dollar_global
        )

    gc()

    message("Made it to second rm()")

    min_allowable_date <-
      copula_data_macro %>%
      ungroup() %>%
      filter(if_all(everything(), ~ !is.na(.))) %>%
      pull(Date) %>% min()
    copula_data_macro <-
      copula_data_macro %>%
      ungroup() %>%
      filter(Date >= min_allowable_date)
    gc()

    copula_data_macro <-
      copula_data_macro %>%
      left_join(binary_data_for_post_model)
    gc()
    rm(binary_data_for_post_model)
    gc()

    message("Made it to third rm() left join of actuals")

    copula_data_macro <-
      copula_data_macro %>%
      group_by(Asset) %>%
      arrange(Date, .by_group = TRUE) %>%
      # group_by(Asset) %>%
      # fill(matches(all_macro_vars, ignore.case = FALSE), .direction = "down") %>%
      ungroup()

    message("Made it to to arrangement of copula_data by Date")

    copula_data_macro <-
      copula_data_macro %>%
      group_by(Asset) %>%
      fill(contains("quantiles"), .direction = "down") %>%
      group_by(Asset) %>%
      fill(contains("tangent"), .direction = "down") %>%
      group_by(Asset) %>%
      fill(contains("cor"), .direction = "down") %>%
      group_by(Asset) %>%
      fill(contains("PC"), .direction = "down") %>%
      group_by(Asset) %>%
      fill(contains("AUD|XAG|XAU|SPX|US2000|FR40|EUR|USD|JPY|HK33"), .direction = "down") %>%
      # fill(everything(), .direction = "down") %>%
      ungroup()

    gc()

    message("Made it to finish creating data")

    max_date_in_testing_data <- copula_data_macro %>%
      distinct(Date) %>% pull(Date) %>% max(na.rm = T)
    message(glue::glue("Max date in Complete data: {max_date_in_testing_data}"))

    min_allowable_date <-
      copula_data_macro %>%
      ungroup() %>%
      filter(if_all(everything(), ~ !is.na(.))) %>%
      pull(Date) %>% min()

    gc()

    copula_data_macro <-
      copula_data_macro %>%
      ungroup() %>%
      filter(Date >= min_allowable_date)

    gc()

    lm_quant_vars <- names(copula_data_macro) %>% keep(~ str_detect(.x,"quantiles|tangent|cor|PC"))

    if(use_PCA_vars == TRUE) {
      lm_vars1 <- c(
        # PC_macro_vars,
        lm_quant_vars,
        "lagged_var_13",
        "lagged_var_21",
        "lagged_var_3_ma",
        # "hour_of_day", "day_of_week"
      )
    } else {
      lm_vars1 <- c(
        # all_macro_vars,
        lm_quant_vars,
        "lagged_var_13",
        "lagged_var_3_ma",
        "lagged_var_21_ma"
        # "hour_of_day", "day_of_week"
      )
    }

    return(
      list(
        "copula_data_macro" = copula_data_macro,
        "lm_vars1" =
          lm_vars1 %>%
          keep( ~ !str_detect(.x, "sd") & !str_detect(.x, "tangent") )
      )
    )

  }

#' generate_GLM_AUD_create_preds
#'
#' @param copula_data_macro
#' @param NN_samples
#' @param dependant_var_name
#' @param NN_path
#' @param training_max_date
#' @param lm_train_prop
#' @param trade_direction_var
#'
#' @return
#' @export
#'
#' @examples
generate_GLM_AUD_create_preds <- function(
    copula_data_macro = copula_data_macro,
    lm_vars1 = lm_vars1,
    NN_samples = NN_samples,
    dependant_var_name = "AUD_USD",
    NN_path = "C:/Users/Nikhil Chandra/Documents/trade_data/asset_specific_NNs/",
    training_max_date = "2021-01-01",
    lm_train_prop = lm_train_prop,
    trade_direction_var = "Long",
    stop_value_var = 15,
    profit_value_var = 20,
    max_NNs = 30,
    hidden_layers = 3,
    ending_thresh = 0.09,
    run_logit_instead = FALSE,
    p_value_thresh_for_inputs = 0.05,
    neuron_adjustment = 0.25,
    lag_price_col = "Price"
) {

  set.seed(round(runif(n = 1, min = 0, max = 100000)))
  training_data <-
    copula_data_macro %>%
    ungroup() %>%
    filter(profit_factor == profit_value_var, stop_factor == stop_value_var) %>%
    filter(Date < as_date(training_max_date)) %>%
    filter(Asset == dependant_var_name) %>%
    filter(trade_col == trade_direction_var) %>%
    group_by(Asset) %>%
    arrange(Date, .by_group = TRUE) %>%
    group_by(Asset) %>%
    mutate(
      lagged_var_1 = lag(!!as.name(lag_price_col), 1) - lag(!!as.name(lag_price_col), 2),
      lagged_var_2 = lag(lagged_var_1, 2),
      lagged_var_3 = lag(lagged_var_1, 3),
      lagged_var_5 = lag(lagged_var_1, 5),
      lagged_var_8 = lag(lagged_var_1, 8),
      lagged_var_13 = lag(lagged_var_1, 13),
      lagged_var_21 = lag(lagged_var_1, 21),

      fib_1 = lagged_var_1 + lagged_var_2,
      fib_2 = lagged_var_2 + lagged_var_3,
      fib_3 = lagged_var_3 + lagged_var_5,
      fib_4 = lagged_var_5 + lagged_var_8,
      fib_5 = lagged_var_8 + lagged_var_13,
      fib_6 = lagged_var_13 + lagged_var_21,

      lagged_var_3_ma = slider::slide_dbl(.x = lagged_var_1,
                                          .f = ~ mean(.x, na.rm = T),
                                          .before = 3),

      lagged_var_5_ma = slider::slide_dbl(.x = lagged_var_1,
                                          .f = ~ mean(.x, na.rm = T),
                                          .before = 5),

      lagged_var_8_ma = slider::slide_dbl(.x = lagged_var_1,
                                          .f = ~ mean(.x, na.rm = T),
                                          .before = 8),

      lagged_var_13_ma = slider::slide_dbl(.x = lagged_var_1,
                                           .f = ~ mean(.x, na.rm = T),
                                           .before = 13),

      lagged_var_21_ma = slider::slide_dbl(.x = lagged_var_1,
                                           .f = ~ mean(.x, na.rm = T),
                                           .before = 21)
    ) %>%
    ungroup() %>%
    distinct()

  gc()
  message("Made it past first training data wrangle")

  training_data <-
    training_data %>%
    group_by(Asset) %>%
    arrange(Date, .by_group = TRUE) %>%
    group_by(Asset) %>%
    fill(!matches(c("bin_var","trade_col"), ignore.case = FALSE), .direction = "down") %>%
    ungroup() %>%
    filter(if_all(everything() ,.fns = ~ !is.na(.)))

  max_date_in_testing_data <- training_data %>% pull(Date) %>% max(na.rm = T)
  message(glue::glue("Max date in Training data: {max_date_in_testing_data}"))

  NN_form <-  create_lm_formula(dependant = "trade_return_dollar_aud", independant = lm_vars1)

  if(run_logit_instead == FALSE) {
    for (i in 1:max_NNs) {
      set.seed(round(runif(1,2,10000)))

      glm_model_1 <- glm(formula = NN_form,
                         data = training_data ,
                         family = binomial("logit"))

      all_coefs <- glm_model_1 %>% jtools::j_summ() %>% pluck(1)
      coef_names <- row.names(all_coefs) %>% as.character()

      filtered_coefs <-
        all_coefs %>%
        as_tibble() %>%
        mutate(all_vars = coef_names) %>%
        filter(p <= p_value_thresh_for_inputs) %>%
        filter(!str_detect(all_vars, "Intercep")) %>%
        pull(all_vars) %>%
        as.character()

      message(length(filtered_coefs))

      NN_form <-  create_lm_formula(dependant = "trade_return_dollar_aud", independant = filtered_coefs)

      hidden_layers <- rep(
        (length(filtered_coefs) + round(length(filtered_coefs)*neuron_adjustment)),
        hidden_layers
      )

      message("Made Formula")

      NN_model_1 <- neuralnet::neuralnet(formula = NN_form,
                                         hidden = hidden_layers,
                                         data = training_data %>% slice_sample(n = NN_samples),
                                         lifesign = 'full',
                                         rep = 1,
                                         stepmax = 1000000,
                                         threshold = ending_thresh)

      saveRDS(object = NN_model_1,
              file =
                glue::glue("{NN_path}/{dependant_var_name}_NN_{i}.rds")
      )

    }
  } else {
    for (i in 1:max_NNs) {
      set.seed(round(runif(1,2,10000)))

      glm_model_1 <- glm(formula = NN_form,
                         data = training_data ,
                         family = gaussian)

      all_coefs <- glm_model_1 %>% jtools::j_summ() %>% pluck(1)
      coef_names <- row.names(all_coefs) %>% as.character()

      filtered_coefs <-
        all_coefs %>%
        as_tibble() %>%
        mutate(all_vars = coef_names) %>%
        filter(p <= p_value_thresh_for_inputs) %>%
        filter(!str_detect(all_vars, "Intercep")) %>%
        pull(all_vars) %>%
        as.character()

      NN_form <-  create_lm_formula(dependant = "trade_return_dollar_aud", independant = filtered_coefs)

      NN_model_1 <- glm(formula = NN_form,
                        data = training_data ,
                        family = gaussian)

      saveRDS(object = NN_model_1,
              file =
                glue::glue("{NN_path}/{dependant_var_name}_GLM_{i}.rds")
      )
    }

  }

  return(NN_model_1)

}

#' get_SPX_US2000_XAG_XAU
#'
#' @returns
#' @export
#'
#' @examples
get_Port_Buy_SPX_Focus_Data <- function(
    db_location = db_location,
    start_date = "2016-01-01",
    end_date = today() %>% as.character(),
    time_frame = "H1"
) {

  SPX <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "SPX500_USD",
    keep_bid_to_ask = TRUE
  )

  US2000 <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "US2000_USD",
    keep_bid_to_ask = TRUE
  )

  EUR50 <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "EU50_EUR",
    keep_bid_to_ask = TRUE
  )

  AU200_AUD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "AU200_AUD",
    keep_bid_to_ask = TRUE
  )

  UK100_GBP <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "UK100_GBP",
    keep_bid_to_ask = TRUE
  )

  JP225Y_JPY <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "JP225Y_JPY",
    keep_bid_to_ask = TRUE
  )

  FR40_EUR <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "FR40_EUR",
    keep_bid_to_ask = TRUE
  )

  CH20_CHF <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "CH20_CHF",
    keep_bid_to_ask = TRUE
  )

  USB10Y_USD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "USB10Y_USD",
    keep_bid_to_ask = TRUE
  )

  USB02Y_USD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "USB02Y_USD",
    keep_bid_to_ask = TRUE
  )

  USB05Y_USD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "USB05Y_USD",
    keep_bid_to_ask = TRUE
  )

  HK33_HKD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "HK33_HKD",
    keep_bid_to_ask = TRUE
  )

  XAU <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "XAU_USD",
    keep_bid_to_ask = TRUE
  )

  XAU_EUR <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "XAU_EUR",
    keep_bid_to_ask = TRUE
  )

  XAU_GBP <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "XAU_GBP",
    keep_bid_to_ask = TRUE
  )

  XAU_JPY <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "XAU_JPY",
    keep_bid_to_ask = TRUE
  )

  XAU_AUD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "XAU_AUD",
    keep_bid_to_ask = TRUE
  )

  XAG <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "XAG_USD",
    keep_bid_to_ask = TRUE
  )

  XAG_EUR <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "XAG_EUR",
    keep_bid_to_ask = TRUE
  )

  XAG_GBP <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "XAG_GBP",
    keep_bid_to_ask = TRUE
  )

  XAG_JPY <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "XAG_JPY",
    keep_bid_to_ask = TRUE
  )

  XAG_AUD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "XAG_AUD",
    keep_bid_to_ask = TRUE
  )

  EUR_USD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "EUR_USD",
    keep_bid_to_ask = TRUE
  )

  AUD_USD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "AUD_USD",
    keep_bid_to_ask = TRUE
  )

  USD_JPY <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "USD_JPY",
    keep_bid_to_ask = TRUE
  )

  GBP_USD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "USD_JPY",
    keep_bid_to_ask = TRUE
  )

  USD_CHF <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "USD_CHF",
    keep_bid_to_ask = TRUE
  )


  XAG_SPX_US2000_USD <-
    SPX %>%
    bind_rows(US2000) %>%
    bind_rows(EUR50) %>%
    bind_rows(AU200_AUD) %>%
    bind_rows(XAU) %>%
    bind_rows(UK100_GBP) %>%
    bind_rows(JP225Y_JPY) %>%
    bind_rows(FR40_EUR) %>%
    bind_rows(CH20_CHF) %>%
    bind_rows(USB10Y_USD) %>%
    bind_rows(USB02Y_USD) %>%
    bind_rows(USB05Y_USD) %>%
    bind_rows(HK33_HKD) %>%
    bind_rows(XAU_EUR) %>%
    bind_rows(XAU_GBP) %>%
    bind_rows(XAU_JPY)%>%
    bind_rows(XAU_AUD)%>%
    bind_rows(XAG) %>%
    bind_rows(XAG_EUR) %>%
    bind_rows(XAG_GBP) %>%
    bind_rows(XAG_JPY)%>%
    bind_rows(XAG_AUD) %>%
    bind_rows(AUD_USD) %>%
    bind_rows(EUR_USD) %>%
    bind_rows(USD_JPY) %>%
    bind_rows(USD_CHF)%>%
    bind_rows(GBP_USD)



  rm(SPX, US2000,EUR50, AU200_AUD, SG30_SGD, XAG, XAU, UK100_GBP, JP225Y_JPY, FR40_EUR, CH20_CHF,
     USB10Y_USD, USB02Y_USD, EUR_USD, GBP_USD,XAU_EUR, XAG_EUR, XAU_EUR, XAU_GBP, XAG_GBP, EUR_GBP,
     WTICO_USD, BCO_USD, XCU_USD, XAG_JPY, XAU_JPY,XAU_AUD, XAG_AUD, AUD_USD, USD_JPY )
  gc()

  SPX <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "SPX500_USD",
    keep_bid_to_ask = TRUE
  )

  US2000 <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "US2000_USD",
    keep_bid_to_ask = TRUE
  )

  EUR50 <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "EU50_EUR",
    keep_bid_to_ask = TRUE
  )

  AU200_AUD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "AU200_AUD",
    keep_bid_to_ask = TRUE
  )

  UK100_GBP <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "UK100_GBP",
    keep_bid_to_ask = TRUE
  )

  JP225Y_JPY <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "JP225Y_JPY",
    keep_bid_to_ask = TRUE
  )

  FR40_EUR <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "FR40_EUR",
    keep_bid_to_ask = TRUE
  )

  CH20_CHF <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "CH20_CHF",
    keep_bid_to_ask = TRUE
  )

  USB10Y_USD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "USB10Y_USD",
    keep_bid_to_ask = TRUE
  )

  USB02Y_USD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "USB02Y_USD",
    keep_bid_to_ask = TRUE
  )

  USB05Y_USD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "USB05Y_USD",
    keep_bid_to_ask = TRUE
  )

  HK33_HKD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "HK33_HKD",
    keep_bid_to_ask = TRUE
  )

  XAU <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "XAU_USD",
    keep_bid_to_ask = TRUE
  )

  XAU_EUR <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "XAU_EUR",
    keep_bid_to_ask = TRUE
  )

  XAU_GBP <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "XAU_GBP",
    keep_bid_to_ask = TRUE
  )

  XAU_JPY <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "XAU_JPY",
    keep_bid_to_ask = TRUE
  )

  XAU_AUD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "XAU_AUD",
    keep_bid_to_ask = TRUE
  )

  XAG <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "XAG_USD",
    keep_bid_to_ask = TRUE
  )

  XAG_EUR <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "XAG_EUR",
    keep_bid_to_ask = TRUE
  )

  XAG_GBP <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "XAG_GBP",
    keep_bid_to_ask = TRUE
  )

  XAG_JPY <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "XAG_JPY",
    keep_bid_to_ask = TRUE
  )

  XAG_AUD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "XAG_AUD",
    keep_bid_to_ask = TRUE
  )

  EUR_USD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "EUR_USD",
    keep_bid_to_ask = TRUE
  )

  AUD_USD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "AUD_USD",
    keep_bid_to_ask = TRUE
  )

  USD_JPY <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "USD_JPY",
    keep_bid_to_ask = TRUE
  )

  GBP_USD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "USD_JPY",
    keep_bid_to_ask = TRUE
  )

  USD_CHF <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "USD_CHF",
    keep_bid_to_ask = TRUE
  )

  XAG_SPX_US2000_USD_short <-
    SPX %>%
    bind_rows(US2000) %>%
    bind_rows(EUR50) %>%
    bind_rows(AU200_AUD) %>%
    bind_rows(XAU) %>%
    bind_rows(UK100_GBP) %>%
    bind_rows(JP225Y_JPY) %>%
    bind_rows(FR40_EUR) %>%
    bind_rows(CH20_CHF) %>%
    bind_rows(USB10Y_USD) %>%
    bind_rows(USB02Y_USD) %>%
    bind_rows(USB05Y_USD) %>%
    bind_rows(HK33_HKD) %>%
    bind_rows(XAU_EUR) %>%
    bind_rows(XAU_GBP) %>%
    bind_rows(XAU_JPY)%>%
    bind_rows(XAU_AUD) %>%
    bind_rows(XAG) %>%
    bind_rows(XAG_EUR) %>%
    bind_rows(XAG_GBP) %>%
    bind_rows(XAG_JPY)%>%
    bind_rows(XAG_AUD) %>%
    bind_rows(AUD_USD) %>%
    bind_rows(EUR_USD) %>%
    bind_rows(USD_JPY) %>%
    bind_rows(USD_CHF)%>%
    bind_rows(GBP_USD)

  rm(SPX, US2000,EUR50, AU200_AUD, SG30_SGD, XAG, XAU, UK100_GBP, JP225Y_JPY, FR40_EUR, CH20_CHF,
     USB10Y_USD, USB02Y_USD, EUR_USD, GBP_USD,XAU_EUR, XAG_EUR, XAU_EUR, XAU_GBP, XAG_GBP, EUR_GBP,
     WTICO_USD, BCO_USD, XCU_USD, XAG_JPY, XAU_JPY,XAU_AUD, XAG_AUD, AUD_USD, USD_JPY )
  gc()

  return(
    list(
      XAG_SPX_US2000_USD,
      XAG_SPX_US2000_USD_short
    )
  )

}

#' create_NN_Idices_Silver_H1Vers_data
#'
#' @return
#' @export
#'
#' @examples
create_LM_Hourly_SPX_Focus <-
  function(SPX_US2000_XAG = Indices_Metals_Bonds[[1]],
           raw_macro_data,
           actual_wins_losses = actual_wins_losses,
           lag_days = 1,
           stop_value_var = 15,
           profit_value_var = 20,
           use_PCA_vars = FALSE,
           period_var,
           bin_factor = 1) {

    # assets_to_return <- dependant_var_name

    # aus_macro_data <-
    #   get_AUS_Indicators(raw_macro_data,
    #                      lag_days = lag_days,
    #                      first_difference = TRUE
    #   ) %>%
    #   janitor::clean_names()
    #
    # # nzd_macro_data <-
    # #   get_NZD_Indicators(raw_macro_data,
    # #                      lag_days = lag_days,
    # #                      first_difference = TRUE
    # #   ) %>%
    # #   janitor::clean_names()
    #
    usd_macro_data <-
      get_USD_Indicators(raw_macro_data,
                         lag_days = lag_days,
                         first_difference = TRUE
      ) %>%
      janitor::clean_names()
    #
    # cny_macro_data <-
    #   get_CNY_Indicators(raw_macro_data,
    #                      lag_days = lag_days,
    #                      first_difference = TRUE
    #   ) %>%
    #   janitor::clean_names()
    #
    eur_macro_data <-
      get_EUR_Indicators(raw_macro_data,
                         lag_days = lag_days,
                         first_difference = TRUE
      ) %>%
      janitor::clean_names()

    gbp_macro_data <-
      get_GBP_Indicators(raw_macro_data,
                         lag_days = lag_days,
                         first_difference = TRUE
      ) %>%
      janitor::clean_names()
    #
    # aud_macro_vars <- names(aus_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    # # nzd_macro_vars <- names(nzd_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    usd_macro_vars <- names(usd_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    # cny_macro_vars <- names(cny_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    eur_macro_vars <- names(eur_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    gbp_macro_vars <- names(gbp_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    all_macro_vars <- c(
      # aud_macro_vars,
      # nzd_macro_vars,
      usd_macro_vars,
      # cny_macro_vars,
      eur_macro_vars,
      gbp_macro_vars
      )

    major_indices_log_cumulative <-
      c("SPX500_USD", "US2000_USD", "NAS100_USD", "AU200_AUD", "EU50_EUR", "DE30_EUR",
        "UK100_GBP", "CH20_CHF", "FR40_EUR", "HK33_HKD") %>%
      map_dfr(
        ~
          create_log_cumulative_returns(
            asset_data_to_use =
              SPX_US2000_XAG %>%
              filter(Asset %in% c("SPX500_USD", "US2000_USD", "NAS100_USD", "AU200_AUD", "EU50_EUR", "DE30_EUR",
                                  "UK100_GBP", "CH20_CHF", "FR40_EUR", "HK33_HKD")),
            asset_to_use = c(.x[1]),
            price_col = "Open",
            return_long_format = TRUE
          )
      ) %>%
      left_join(
        SPX_US2000_XAG %>%
          filter(Asset %in% c("SPX500_USD", "US2000_USD", "NAS100_USD", "AU200_AUD", "EU50_EUR", "DE30_EUR",
                              "UK100_GBP", "CH20_CHF", "FR40_EUR", "HK33_HKD")) %>%
          dplyr::select(Date, Asset, Price, Open)
      )

    pc_equities_global <-
      create_PCA_Asset_Index(
        asset_data_to_use = major_indices_log_cumulative %>%
          group_by(Asset) %>%
          mutate(
            Return_Index_Diff = ((Price - Open)/Open)*100
          ) %>%
          ungroup() %>%
          filter(!is.na(Return_Index_Diff)),
        asset_to_use =  c("SPX500_USD", "US2000_USD", "NAS100_USD", "AU200_AUD", "EU50_EUR", "DE30_EUR",
                          "UK100_GBP", "CH20_CHF", "FR40_EUR", "HK33_HKD"),
        price_col = "Return_Index_Diff",
        scale_values = TRUE
      )

    rm(major_indices_log_cumulative)

    pc_equities_global <-
      pc_equities_global %>%
      arrange(Date) %>%
      mutate(
        across(matches("PC[0-9]"), ~cumsum(.))
      ) %>%
      rename(PC1_Global_Equities = PC1,
             PC2_Global_Equities = PC2,
             PC3_Global_Equities = PC3,
             PC4_Global_Equities = PC4) %>%
      dplyr::select(-PC5, -PC6)

    major_bonds_log_cumulative <-
      c("USB05Y_USD", "USB10Y_USD", "USB02Y_USD") %>%
      map_dfr(
        ~
          create_log_cumulative_returns(
            asset_data_to_use =
              SPX_US2000_XAG %>%
              filter(Asset %in% c("USB05Y_USD", "USB10Y_USD", "USB02Y_USD")),
            asset_to_use = c(.x[1]),
            price_col = "Open",
            return_long_format = TRUE
          )
      ) %>%
      left_join(
        SPX_US2000_XAG %>%
          filter(Asset %in% c("USB05Y_USD", "USB10Y_USD", "USB02Y_USD")) %>%
          dplyr::select(Date, Asset, Price, Open)
      )

    pc_bonds_global <-
      create_PCA_Asset_Index(
        asset_data_to_use = major_bonds_log_cumulative %>%
          group_by(Asset) %>%
          mutate(
            Return_Index_Diff = ((Price - Open)/Open)*100
          ) %>%
          ungroup() %>%
          filter(!is.na(Return_Index_Diff)),
        asset_to_use =  c("USB05Y_USD", "USB10Y_USD", "USB02Y_USD"),
        price_col = "Return_Index_Diff",
        scale_values = TRUE
      ) %>%
      arrange(Date) %>%
      mutate(
        across(contains("PC[0-9]"), ~cumsum(.))
      ) %>%
      rename(PC1_Bonds_Equities = PC1,
             PC2_Bonds_Equities = PC2,
             PC3_Bonds_Equities = PC3)

    rm(major_bonds_log_cumulative)
    gc()


    major_gold_log_cumulative <-
      c("XAU_USD", "XAU_EUR", "XAU_GBP", "XAU_AUD", "XAU_JPY") %>%
      map_dfr(
        ~
          create_log_cumulative_returns(
            asset_data_to_use =
              SPX_US2000_XAG %>%
              filter(Asset %in% c("XAU_USD", "XAU_EUR", "XAU_GBP", "XAU_AUD", "XAU_JPY")),
            asset_to_use = c(.x[1]),
            price_col = "Open",
            return_long_format = TRUE
          )
      ) %>%
      left_join(
        SPX_US2000_XAG %>%
          filter(Asset %in% c("XAU_USD", "XAU_EUR", "XAU_GBP", "XAU_AUD", "XAU_JPY")) %>%
          dplyr::select(Date, Asset, Price, Open)
      )

    pc_gold_global <-
      create_PCA_Asset_Index(
        asset_data_to_use = major_gold_log_cumulative %>%
          group_by(Asset) %>%
          mutate(
            Return_Index_Diff = ((Price - Open)/Open)*100
          ) %>%
          ungroup() %>%
          filter(!is.na(Return_Index_Diff)),
        asset_to_use =  c("XAU_USD", "XAU_EUR", "XAU_GBP", "XAU_AUD", "XAU_JPY"),
        price_col = "Return_Index_Diff",
        scale_values = TRUE
      ) %>%
      arrange(Date) %>%
      mutate(
        across(contains("PC[0-9]"), ~cumsum(.))
      ) %>%
      rename(PC1_Gold_Equities = PC1,
             PC2_Gold_Equities = PC2,
             PC3_Gold_Equities = PC3) %>%
      dplyr::select(-PC4, -PC5)

    rm(major_gold_log_cumulative)
    gc()

    major_silver_log_cumulative <-
      c("XAG_USD", "XAG_EUR", "XAG_GBP", "XAG_AUD", "XAG_JPY") %>%
      map_dfr(
        ~
          create_log_cumulative_returns(
            asset_data_to_use =
              SPX_US2000_XAG %>%
              filter(Asset %in% c("XAG_USD", "XAG_EUR", "XAG_GBP", "XAG_AUD", "XAG_JPY")),
            asset_to_use = c(.x[1]),
            price_col = "Open",
            return_long_format = TRUE
          )
      ) %>%
      left_join(
        SPX_US2000_XAG %>%
          filter(Asset %in% c("XAG_USD", "XAG_EUR", "XAG_GBP", "XAG_AUD", "XAG_JPY")) %>%
          dplyr::select(Date, Asset, Price, Open)
      )

    pc_silver_global <-
      create_PCA_Asset_Index(
        asset_data_to_use = major_silver_log_cumulative %>%
          group_by(Asset) %>%
          mutate(
            Return_Index_Diff = ((Price - Open)/Open)*100
          ) %>%
          ungroup() %>%
          filter(!is.na(Return_Index_Diff)),
        asset_to_use =  c("XAG_USD", "XAG_EUR", "XAG_GBP", "XAG_AUD", "XAG_JPY"),
        price_col = "Return_Index_Diff",
        scale_values = TRUE
      ) %>%
      arrange(Date) %>%
      mutate(
        across(contains("PC[0-9]"), ~cumsum(.))
      ) %>%
      rename(PC1_Silver_Equities = PC1,
             PC2_Silver_Equities = PC2,
             PC3_Silver_Equities = PC3) %>%
      dplyr::select(-PC4, -PC5)

    rm(major_silver_log_cumulative)
    gc()

    copula_data <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("SPX500_USD", "US2000_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      )

    copula_data_SPX500_USD_USB10Y_USD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("SPX500_USD", "USB10Y_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-SPX500_USD, -SPX500_USD_log1_price, -SPX500_USD_quantiles_1, -SPX500_USD_tangent_angle1)
      # dplyr::select(-EU50_EUR, -EU50_EUR_log2_price, -EU50_EUR_quantiles_2, -EU50_EUR_tangent_angle2)


    copula_data_SPX500_USD_USB05Y_USD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("SPX500_USD", "USB05Y_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-SPX500_USD, -SPX500_USD_log1_price, -SPX500_USD_quantiles_1, -SPX500_USD_tangent_angle1)
    # dplyr::select(-EU50_EUR, -EU50_EUR_log2_price, -EU50_EUR_quantiles_2, -EU50_EUR_tangent_angle2)

    copula_data_SPX500_USD_EU50_EUR <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("SPX500_USD", "EU50_EUR"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-SPX500_USD, -SPX500_USD_log1_price, -SPX500_USD_quantiles_1, -SPX500_USD_tangent_angle1)

    copula_data_SPX500_USD_AU200_AUD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("SPX500_USD", "AU200_AUD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-SPX500_USD, -SPX500_USD_log1_price, -SPX500_USD_quantiles_1, -SPX500_USD_tangent_angle1)

    copula_data_SPX500_USD_XAU_USD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("SPX500_USD", "XAU_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-SPX500_USD, -SPX500_USD_log1_price, -SPX500_USD_quantiles_1, -SPX500_USD_tangent_angle1)

    copula_data_SPX500_USD_UK100_GBP <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("SPX500_USD", "UK100_GBP"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-SPX500_USD, -SPX500_USD_log1_price, -SPX500_USD_quantiles_1, -SPX500_USD_tangent_angle1)

    copula_data_SPX500_USD_FR40_EUR <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("SPX500_USD", "FR40_EUR"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-SPX500_USD, -SPX500_USD_log1_price, -SPX500_USD_quantiles_1, -SPX500_USD_tangent_angle1)

    copula_data_SPX500_USD_CH20_CHF <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("SPX500_USD", "CH20_CHF"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-SPX500_USD, -SPX500_USD_log1_price, -SPX500_USD_quantiles_1, -SPX500_USD_tangent_angle1)

    copula_data_SPX500_USD_HK33_HKD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("SPX500_USD", "HK33_HKD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-SPX500_USD, -SPX500_USD_log1_price, -SPX500_USD_quantiles_1, -SPX500_USD_tangent_angle1)

    copula_data_SPX500_USD_XAU_EUR <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("SPX500_USD", "XAU_EUR"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-SPX500_USD, -SPX500_USD_log1_price, -SPX500_USD_quantiles_1, -SPX500_USD_tangent_angle1)

    copula_data_SPX500_USD_XAU_GBP <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("SPX500_USD", "XAU_GBP"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-SPX500_USD, -SPX500_USD_log1_price, -SPX500_USD_quantiles_1, -SPX500_USD_tangent_angle1)

    copula_data_SPX500_USD_XAU_JPY <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("SPX500_USD", "XAU_JPY"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-SPX500_USD, -SPX500_USD_log1_price, -SPX500_USD_quantiles_1, -SPX500_USD_tangent_angle1)

    copula_data_SPX500_USD_XAU_AUD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("SPX500_USD", "XAU_AUD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-SPX500_USD, -SPX500_USD_log1_price, -SPX500_USD_quantiles_1, -SPX500_USD_tangent_angle1)

    copula_data_XAG_USD_XAG_EUR <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("XAG_USD", "XAG_EUR"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      )

    copula_data_XAG_USD_XAG_AUD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("XAG_USD", "XAG_AUD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAG_USD, -XAG_USD_log1_price, -XAG_USD_quantiles_1, -XAG_USD_tangent_angle1)

    copula_data_XAG_USD_XAG_GBP <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("XAG_USD", "XAG_GBP"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAG_USD, -XAG_USD_log1_price, -XAG_USD_quantiles_1, -XAG_USD_tangent_angle1)

    copula_data_XAG_USD_XAG_JPY <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("XAG_USD", "XAG_JPY"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAG_USD, -XAG_USD_log1_price, -XAG_USD_quantiles_1, -XAG_USD_tangent_angle1)

    copula_data_EUR_USD_XAU_EUR <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("EUR_USD", "XAU_EUR"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-EUR_USD, -EUR_USD_log1_price, -EUR_USD_quantiles_1, -EUR_USD_tangent_angle1)%>%
      dplyr::select(-XAU_EUR, -XAU_EUR_log2_price, -XAU_EUR_quantiles_2, -XAU_EUR_tangent_angle2)

    copula_data_EUR_USD_XAG_EUR <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("EUR_USD", "XAG_EUR"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-EUR_USD, -EUR_USD_log1_price, -EUR_USD_quantiles_1, -EUR_USD_tangent_angle1)%>%
      dplyr::select(-XAG_EUR, -XAG_EUR_log2_price, -XAG_EUR_quantiles_2, -XAG_EUR_tangent_angle2)

    copula_data_EUR_USD_XAU_USD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("EUR_USD", "XAU_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-EUR_USD, -EUR_USD_log1_price, -EUR_USD_quantiles_1, -EUR_USD_tangent_angle1)%>%
      dplyr::select(-XAU_USD, -XAU_USD_log2_price, -XAU_USD_quantiles_2, -XAU_USD_tangent_angle2)

    copula_data_EUR_USD_XAG_USD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("EUR_USD", "XAG_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-EUR_USD, -EUR_USD_log1_price, -EUR_USD_quantiles_1, -EUR_USD_tangent_angle1)%>%
      dplyr::select(-XAG_USD, -XAG_USD_log2_price, -XAG_USD_quantiles_2, -XAG_USD_tangent_angle2)


    copula_data_AUD_USD_XAU_AUD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("AUD_USD", "XAU_AUD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-AUD_USD, -AUD_USD_log1_price, -AUD_USD_quantiles_1, -AUD_USD_tangent_angle1)%>%
      dplyr::select(-XAU_AUD, -XAU_AUD_log2_price, -XAU_AUD_quantiles_2, -XAU_AUD_tangent_angle2)

    copula_data_AUD_USD_XAG_AUD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("AUD_USD", "XAG_AUD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-AUD_USD, -AUD_USD_log1_price, -AUD_USD_quantiles_1, -AUD_USD_tangent_angle1)%>%
      dplyr::select(-XAG_AUD, -XAG_AUD_log2_price, -XAG_AUD_quantiles_2, -XAG_AUD_tangent_angle2)

    copula_data_AUD_USD_XAU_USD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("AUD_USD", "XAU_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-AUD_USD, -AUD_USD_log1_price, -AUD_USD_quantiles_1, -AUD_USD_tangent_angle1)%>%
      dplyr::select(-XAU_USD, -XAU_USD_log2_price, -XAU_USD_quantiles_2, -XAU_USD_tangent_angle2)

    copula_data_AUD_USD_XAG_USD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("AUD_USD", "XAG_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-AUD_USD, -AUD_USD_log1_price, -AUD_USD_quantiles_1, -AUD_USD_tangent_angle1)%>%
      dplyr::select(-XAG_USD, -XAG_USD_log2_price, -XAG_USD_quantiles_2, -XAG_USD_tangent_angle2)


    copula_data_US2000_USD_XAU_USD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("US2000_USD", "XAU_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-US2000_USD, -US2000_USD_log1_price, -US2000_USD_quantiles_1, -US2000_USD_tangent_angle1)%>%
      dplyr::select(-XAU_USD, -XAU_USD_log2_price, -XAU_USD_quantiles_2, -XAU_USD_tangent_angle2)

    copula_data_US2000_USD_USB10Y_USD <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("US2000_USD", "USB10Y_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-US2000_USD, -US2000_USD_log1_price, -US2000_USD_quantiles_1, -US2000_USD_tangent_angle1)%>%
      dplyr::select(-USB10Y_USD, -USB10Y_USD_log2_price, -USB10Y_USD_quantiles_2, -USB10Y_USD_tangent_angle2)


    if(!is.null(bin_factor)) {
      binary_data_for_post_model <-
        actual_wins_losses %>%
        filter(profit_factor == profit_value_var)%>%
        filter(stop_factor == stop_value_var) %>%
        filter(periods_ahead == period_var) %>%
        group_by(Asset) %>%
        mutate(max_win = max(trade_return_dollar_aud, na.rm=T),
               max_loss = min(trade_return_dollar_aud, na.rm=T),
               quantile_bin = quantile(trade_return_dollar_aud, bin_factor, na.rm = T) ) %>%
        ungroup() %>%
        mutate(
          bin_var =
            case_when(
              trade_return_dollar_aud > 0  ~ "win",
              trade_return_dollar_aud <= 0~ "loss"
            )
        ) %>%
        dplyr::select(Date, bin_var, Asset, trade_col,
                      profit_factor, stop_factor, periods_ahead, trade_return_dollar_aud, estimated_margin)
    } else {
      binary_data_for_post_model <-
        actual_wins_losses %>%
        filter(profit_factor == profit_value_var)%>%
        filter(stop_factor == stop_value_var) %>%
        mutate(
          bin_var =
            case_when(
              trade_start_prices > trade_end_prices & trade_col == "Short" ~ "win",
              trade_start_prices <= trade_end_prices & trade_col == "Short" ~ "loss",

              trade_start_prices < trade_end_prices & trade_col == "Long" ~ "win",
              trade_start_prices >= trade_end_prices & trade_col == "Long" ~ "loss"

            )
        ) %>%
        dplyr::select(Date, bin_var, Asset, trade_col,
                      profit_factor, stop_factor,
                      trade_start_prices, trade_end_prices,
                      starting_stop_value, starting_profit_value,
                      trade_return_dollar_aud, periods_ahead)
    }

    gc()

    mean_sd_values <-
      wrangle_asset_data(SPX_US2000_XAG, summarise_means = TRUE)

    copula_data_macro <-
      SPX_US2000_XAG %>%
      dplyr::select(Date,Asset, Price, High, Low, Open ) %>%

      find_pivots_fib_max_min(how_far_back = 200) %>%
      rename(line_1_200 = line_1,
             line_10_200 = line_10,
             line_1_max_200 = line_1_max,
             line_10_max_200 = line_10_max,
             perc_line_1_200 = perc_line_1,
             perc_line_10_200 = perc_line_10,
             perc_line_1_to_10_200 = perc_line_1_to_10,
             perc_line_1_mean_200 = perc_line_1_mean,
             perc_line_1_sd_200 = perc_line_1_sd,
             perc_line_10_mean_200 = perc_line_10_mean,
             perc_line_10_sd_200 = perc_line_10_sd,
             perc_line_1_to_10_mean_200 = perc_line_1_to_10_mean,
             perc_line_1_to_10_sd_200 = perc_line_1_to_10_sd) %>%

      find_pivots_fib_max_min(how_far_back = 100) %>%
      rename(line_1_100 = line_1,
             line_10_100 = line_10,
             line_1_max_100 = line_1_max,
             line_10_max_100 = line_10_max,
             perc_line_1_100 = perc_line_1,
             perc_line_10_100 = perc_line_10,
             perc_line_1_to_10_100 = perc_line_1_to_10,
             perc_line_1_mean_100 = perc_line_1_mean,
             perc_line_1_sd_100 = perc_line_1_sd,
             perc_line_10_mean_100 = perc_line_10_mean,
             perc_line_10_sd_100 = perc_line_10_sd,
             perc_line_1_to_10_mean_100 = perc_line_1_to_10_mean,
             perc_line_1_to_10_sd_100 = perc_line_1_to_10_sd) %>%
      find_pivots_fib_max_min(how_far_back = 50)%>%
        rename(line_1_50 = line_1,
               line_10_50 = line_10,
               line_1_max_50 = line_1_max,
               line_10_max_50 = line_10_max,
               perc_line_1_50 = perc_line_1,
               perc_line_10_50 = perc_line_10,
               perc_line_1_to_10_50 = perc_line_1_to_10,
               perc_line_1_mean_50 = perc_line_1_mean,
               perc_line_1_sd_50 = perc_line_1_sd,
               perc_line_10_mean_50 = perc_line_10_mean,
               perc_line_10_sd_50 = perc_line_10_sd,
               perc_line_1_to_10_mean_50 = perc_line_1_to_10_mean,
               perc_line_1_to_10_sd_50 = perc_line_1_to_10_sd)%>%
      find_pivots_fib_max_min(how_far_back = 10)%>%
      rename(line_1_10 = line_1,
             line_10_10 = line_10,
             line_1_max_10 = line_1_max,
             line_10_max_10 = line_10_max,
             perc_line_1_10 = perc_line_1,
             perc_line_10_10 = perc_line_10,
             perc_line_1_to_10_10 = perc_line_1_to_10,
             perc_line_1_mean_10 = perc_line_1_mean,
             perc_line_1_sd_10 = perc_line_1_sd,
             perc_line_10_mean_10 = perc_line_10_mean,
             perc_line_10_sd_10 = perc_line_10_sd,
             perc_line_1_to_10_mean_10 = perc_line_1_to_10_mean,
             perc_line_1_to_10_sd_10 = perc_line_1_to_10_sd)

    rm(SPX_US2000_XAG)
    gc()
    Sys.sleep(2)

    copula_data_macro <-
      copula_data_macro %>%
      mutate(
        temp_high_to_low = High - Low,
        temp_high_to_price = High - Price,
        temp_price_to_low = Price - Low,
        temp_price_to_open = abs(Price - Open)
      ) %>%
      group_by(Asset) %>%
      mutate(
        temp_high_to_low_mean = mean(temp_high_to_low, na.rm = T),
        temp_high_to_price_mean = mean(temp_high_to_price, na.rm = T),
        temp_price_to_low_mean = mean(temp_price_to_low, na.rm = T),
        temp_price_to_open_mean = mean(temp_price_to_open, na.rm = T),

        temp_high_to_low_sd = sd(temp_high_to_low, na.rm = T),
        temp_high_to_price_sd = sd(temp_high_to_price, na.rm = T),
        temp_price_to_low_sd = sd(temp_price_to_low, na.rm = T),
        temp_price_to_open_sd = sd(temp_price_to_open, na.rm = T)
      ) %>%
      ungroup() %>%
      mutate(
        High_Support =
          ifelse(
          temp_high_to_price/temp_high_to_low <= 0.15 &
          Price > Open &
          abs(temp_price_to_open/temp_high_to_low) <= 0.15,
          1,
          0
          ),

        High_Resistance =
          ifelse(
            temp_price_to_low/temp_high_to_low <= 0.15 &
              Price < Open &
              temp_price_to_open/temp_high_to_low <= 0.15,
            1,
            0
          ),
        High_Support2 =
          ifelse(
            temp_high_to_low >= temp_high_to_low_mean + temp_high_to_low_sd*1.5 &
              temp_price_to_open <= temp_price_to_open_mean - temp_price_to_open_sd*1.5 &
              Price > Open,
            1,
            0
          ),
        High_Resistance2 =
          ifelse(
            temp_high_to_low >= temp_high_to_low_mean + temp_high_to_low_sd*1.5 &
              temp_price_to_open <= temp_price_to_open_mean - temp_price_to_open_sd*1.5 &
              Price < Open,
            1,
            0
          )
      ) %>%
      group_by(Asset) %>%
      mutate(
        Bull_3 =
          ifelse(
            Price > Open & lag(Price) > lag(Open) & lag(Price,2) > lag(Open,2) ,
            1,
            0
          ),

        Bear_3 =
          ifelse(
            Price < Open & lag(Price) < lag(Open) & lag(Price,2) < lag(Open,2) ,
            1,
            0
          ),

        Bull_3 =
          ifelse(
            Price > Open & lag(Price) > lag(Open) & lag(Price,2) > lag(Open,2) ,
            1,
            0
          ),

        Bull_3_strong =
          ifelse(
            Price > Open & lag(Price) > lag(Open) & lag(Price,2) > lag(Open,2) &
              temp_price_to_open > lag(temp_price_to_open) &
              lag(temp_price_to_open) > lag(temp_price_to_open, 2),
            1,
            0
          ),

        Bear_3_strong =
          ifelse(
            Price < Open & lag(Price) < lag(Open) & lag(Price,2) < lag(Open,2) &
              temp_price_to_open < lag(temp_price_to_open) &
              lag(temp_price_to_open) < lag(temp_price_to_open, 2),
            1,
            0
          )
      ) %>%
      dplyr::select(
        -c(
          temp_high_to_low_mean,
          temp_high_to_price_mean,
          temp_price_to_low_mean,
          temp_price_to_open_mean,

          temp_high_to_low_sd,
          temp_high_to_price_sd,
          temp_price_to_low_sd ,
          temp_price_to_open_sd,

          temp_high_to_low ,
          temp_high_to_price,
          temp_price_to_low ,
          temp_price_to_open
        )
      )

    gc()

    copula_data_macro <-
      copula_data_macro %>%
      left_join(copula_data) %>%
      left_join(copula_data_SPX500_USD_USB10Y_USD) %>%
      # left_join(copula_data_SPX500_USD_USB02Y_USD) %>%
      left_join(copula_data_SPX500_USD_USB05Y_USD) %>%
      left_join(copula_data_SPX500_USD_EU50_EUR) %>%
      left_join(copula_data_SPX500_USD_AU200_AUD) %>%
      left_join(copula_data_SPX500_USD_XAU_USD) %>%
      left_join(copula_data_SPX500_USD_UK100_GBP) %>%
      left_join(copula_data_SPX500_USD_FR40_EUR) %>%
      left_join(copula_data_SPX500_USD_CH20_CHF) %>%
      left_join(copula_data_SPX500_USD_HK33_HKD) %>%
      left_join(copula_data_SPX500_USD_XAU_EUR) %>%
      left_join(copula_data_SPX500_USD_XAU_GBP) %>%
      left_join(copula_data_SPX500_USD_XAU_JPY) %>%
      left_join(copula_data_SPX500_USD_XAU_AUD) %>%
      left_join(copula_data_XAG_USD_XAG_EUR) %>%
      left_join(copula_data_XAG_USD_XAG_AUD) %>%
      left_join(copula_data_XAG_USD_XAG_GBP) %>%
      left_join(copula_data_XAG_USD_XAG_JPY) %>%
      left_join(copula_data_EUR_USD_XAU_EUR) %>%
      left_join(copula_data_EUR_USD_XAG_EUR) %>%
      left_join(copula_data_EUR_USD_XAU_USD) %>%
      left_join(copula_data_EUR_USD_XAG_USD) %>%
      left_join(copula_data_AUD_USD_XAU_AUD) %>%
      left_join(copula_data_AUD_USD_XAG_AUD) %>%
      left_join(copula_data_AUD_USD_XAU_USD) %>%
      left_join(copula_data_AUD_USD_XAG_USD) %>%
      left_join(copula_data_US2000_USD_XAU_USD) %>%
      left_join(copula_data_US2000_USD_USB10Y_USD)

    gc()
    rm(copula_data ,
               copula_data_SPX500_USD_USB10Y_USD ,
               copula_data_SPX500_USD_USB02Y_USD ,
               copula_data_SPX500_USD_USB05Y_USD ,
               copula_data_SPX500_USD_EU50_EUR ,
               copula_data_SPX500_USD_AU200_AUD ,
               copula_data_SPX500_USD_XAU_USD ,
               copula_data_SPX500_USD_UK100_GBP ,
               copula_data_SPX500_USD_FR40_EUR ,
               copula_data_SPX500_USD_CH20_CHF ,
               copula_data_SPX500_USD_HK33_HKD ,
               copula_data_SPX500_USD_XAU_EUR ,
               copula_data_SPX500_USD_XAU_GBP ,
               copula_data_SPX500_USD_XAU_JPY ,
               copula_data_SPX500_USD_XAU_AUD,
               copula_data_US2000_USD_USB10Y_USD,
       copula_data_US2000_USD_XAU_USD,
       copula_data_AUD_USD_XAG_USD,
       copula_data_AUD_USD_XAU_USD,
       copula_data_AUD_USD_XAU_AUD,
       copula_data_EUR_USD_XAG_USD,
       copula_data_EUR_USD_XAU_USD,
       copula_data_EUR_USD_XAG_EUR,
       copula_data_EUR_USD_XAU_EUR,
       copula_data_XAG_USD_XAG_JPY
             )
    gc()

    message("Made it to first rm()")

    copula_data_macro <-
      copula_data_macro %>%
      left_join(pc_equities_global %>% dplyr::select(-Average_PCA)) %>%
      left_join(pc_bonds_global %>% dplyr::select(-Average_PCA)) %>%
      left_join(pc_gold_global %>% dplyr::select(-Average_PCA))  %>%
      left_join(pc_silver_global %>% dplyr::select(-Average_PCA)) %>%
      mutate(Date_for_join = as_date(Date)) %>%
      left_join(
        usd_macro_data %>%
          rename(Date_for_join = date)
      ) %>%
      left_join(
        eur_macro_data %>%
          rename(Date_for_join = date)
      ) %>%
      left_join(
        gbp_macro_data %>%
          rename(Date_for_join = date)
      ) %>%
      dplyr::select(-Date_for_join) %>%
      group_by(Asset) %>%
      arrange(Date, .by_group = TRUE)

    rm(usd_macro_data, eur_macro_data, aus_macro_data, raw_macro_data, gbp_macro_data)
    gc()
    Sys.sleep(2)
    gc()
    gc()
    rm(pc_equities_global, pc_bonds_global, pc_gold_global, pc_silver_global)
    gc()

    copula_data_macro <-
      copula_data_macro %>%
      group_by(Asset) %>%
      fill(matches(gbp_macro_vars, ignore.case = FALSE), .direction = "down") %>%
      group_by(Asset) %>%
      fill(matches(usd_macro_vars, ignore.case = FALSE), .direction = "down") %>%
      group_by(Asset) %>%
      fill(matches(eur_macro_vars, ignore.case = FALSE), .direction = "down") %>%
      ungroup()

    message("Made it to second rm()")

    min_allowable_date <-
      copula_data_macro %>%
      ungroup() %>%
      filter(if_all(everything(), ~ !is.na(.))) %>%
      pull(Date) %>% min()

    copula_data_macro <-
      copula_data_macro %>%
      ungroup() %>%
      filter(Date >= min_allowable_date)
    gc()

    copula_data_macro <-
      copula_data_macro %>%
      left_join(binary_data_for_post_model)
    gc()
    rm(binary_data_for_post_model)
    gc()

    message("Made it to third rm() left join of actuals")

    copula_data_macro <-
      copula_data_macro %>%
      group_by(Asset) %>%
      arrange(Date, .by_group = TRUE) %>%
      ungroup()

    message("Made it to to arrangement of copula_data by Date")

    copula_data_macro <-
      copula_data_macro %>%
      group_by(Asset) %>%
      fill(contains("quantiles"), .direction = "down") %>%
      group_by(Asset) %>%
      fill(contains("tangent"), .direction = "down") %>%
      group_by(Asset) %>%
      fill(contains("cor"), .direction = "down") %>%
      group_by(Asset) %>%
      fill(contains("PC"), .direction = "down") %>%
      group_by(Asset) %>%
      fill(contains("AUD|XAG|XAU|SPX|US2000|FR40|EUR|USD|JPY|HK33|CHF|USB0|US2000|AU200|UK100"), .direction = "down") %>%
      ungroup()

    gc()

    message("Made it to finish creating data")

    max_date_in_testing_data <- copula_data_macro %>%
      distinct(Date) %>% pull(Date) %>% max(na.rm = T)
    message(glue::glue("Max date in Complete data: {max_date_in_testing_data}"))

    min_allowable_date <-
      copula_data_macro %>%
      ungroup() %>%
      filter(if_all(everything(), ~ !is.na(.))) %>%
      pull(Date) %>% min()

    gc()

    copula_data_macro <-
      copula_data_macro %>%
      ungroup() %>%
      filter(Date >= min_allowable_date)

    gc()

    lm_quant_vars <- names(copula_data_macro) %>% keep(~ str_detect(.x,"quantiles|tangent|cor|PC|perc_|line_|Support|Resistance|Bear|Bull"))

    if(use_PCA_vars == TRUE) {
      lm_vars1 <- c(
        # PC_macro_vars,
        lm_quant_vars,
        "lagged_var_13",
        "lagged_var_21",
        "lagged_var_3_ma",
        # "hour_of_day", "day_of_week"
      )
    } else {
      lm_vars1 <- c(
        all_macro_vars,
        lm_quant_vars,
        "lagged_var_13",
        "lagged_var_3_ma",
        "lagged_var_21_ma"
        # "hour_of_day", "day_of_week"
      )
    }

    return(
      list(
        "copula_data_macro" = copula_data_macro,
        "lm_vars1" =
          lm_vars1
      )
    )

  }

#' generate_Logit_gen_model_create_preds
#'
#' @param copula_data_macro
#' @param NN_samples
#' @param dependant_var_name
#' @param NN_path
#' @param training_max_date
#' @param lm_train_prop
#' @param trade_direction_var
#'
#' @return
#' @export
#'
#' @examples
generate_Logit_gen_model_create_preds <- function(
    copula_data_macro = copula_data_Indices_Silver[[1]],
    lm_vars1 = copula_data_Indices_Silver[[2]],
    NN_samples = NN_samples,
    dependant_var_name = available_assets2[i],
    NN_path = "C:/Users/Nikhil Chandra/Documents/trade_data/asset_specific_NNs_Portfolio/",
    training_max_date = date_sequence[k],
    lm_train_prop = 1,
    trade_direction_var = analysis_direction,
    stop_value_var = stop_value_var,
    profit_value_var = profit_value_var,
    max_NNs = 1,
    hidden_layers = hidden_layers,
    ending_thresh = ending_thresh,
    run_logit_instead = TRUE,
    p_value_thresh_for_inputs = p_value_thresh_for_inputs,
    neuron_adjustment = neuron_adjustment,
    lag_price_col = "Price",
    testing_min_date_p1 = (as_date(date_sequence[k]) + days(1)) %>% as.character(),
    phase_1_testing_weeks = 4,
    period_var= period_var,
    return_tagged_trades = FALSE
) {

  set.seed(round(runif(n = 1, min = 0, max = 100000)))
  copula_data_macro <-
    copula_data_macro %>%
    ungroup() %>%
    filter(profit_factor == profit_value_var, stop_factor == stop_value_var) %>%
    # filter(Date < as_date(training_max_date)) %>%
    filter(Asset == dependant_var_name) %>%
    filter(trade_col == trade_direction_var) %>%
    group_by(Asset) %>%
    arrange(Date, .by_group = TRUE) %>%
    group_by(Asset) %>%
    mutate(
      lagged_var_1 = lag(!!as.name(lag_price_col), 1) - lag(!!as.name(lag_price_col), 2),
      lagged_var_2 = lag(lagged_var_1, 2),
      lagged_var_3 = lag(lagged_var_1, 3),
      lagged_var_5 = lag(lagged_var_1, 5),
      lagged_var_8 = lag(lagged_var_1, 8),
      lagged_var_13 = lag(lagged_var_1, 13),
      lagged_var_21 = lag(lagged_var_1, 21),

      fib_1 = lagged_var_1 + lagged_var_2,
      fib_2 = lagged_var_2 + lagged_var_3,
      fib_3 = lagged_var_3 + lagged_var_5,
      fib_4 = lagged_var_5 + lagged_var_8,
      fib_5 = lagged_var_8 + lagged_var_13,
      fib_6 = lagged_var_13 + lagged_var_21,

      lagged_var_3_ma = slider::slide_dbl(.x = lagged_var_1,
                                          .f = ~ mean(.x, na.rm = T),
                                          .before = 3),

      lagged_var_5_ma = slider::slide_dbl(.x = lagged_var_1,
                                          .f = ~ mean(.x, na.rm = T),
                                          .before = 5),

      lagged_var_8_ma = slider::slide_dbl(.x = lagged_var_1,
                                          .f = ~ mean(.x, na.rm = T),
                                          .before = 8),

      lagged_var_13_ma = slider::slide_dbl(.x = lagged_var_1,
                                           .f = ~ mean(.x, na.rm = T),
                                           .before = 13),

      lagged_var_21_ma = slider::slide_dbl(.x = lagged_var_1,
                                           .f = ~ mean(.x, na.rm = T),
                                           .before = 21)
    ) %>%
    ungroup() %>%
    distinct()

  gc()
  message("Made it past first training data wrangle")

  copula_data_macro <-
    copula_data_macro %>%
    group_by(Asset) %>%
    arrange(Date, .by_group = TRUE) %>%
    # group_by(Asset) %>%
    # fill(!matches(c("bin_var","trade_col"), ignore.case = FALSE), .direction = "down") %>%
    # ungroup() %>%
    group_by(Asset) %>%
    fill(contains("aud_", ignore.case = FALSE), .direction = "down") %>%
    group_by(Asset) %>%
    fill(contains("usd_", ignore.case = FALSE), .direction = "down") %>%
    group_by(Asset) %>%
    fill(contains("eur_", ignore.case = FALSE), .direction = "down") %>%
    group_by(Asset) %>%
    fill(contains("quantiles"), .direction = "down") %>%
    group_by(Asset) %>%
    fill(contains("tangent"), .direction = "down") %>%
    group_by(Asset) %>%
    fill(contains("cor"), .direction = "down") %>%
    group_by(Asset) %>%
    fill(contains("PC"), .direction = "down") %>%
    group_by(Asset) %>%
    fill(contains("AUD|XAG|XAU|SPX|US2000|FR40|EUR|USD|JPY|HK33|CHF|USB0|US2000|AU200|UK100"), .direction = "down") %>%
    ungroup() %>%
    filter(if_all(everything() ,.fns = ~ !is.na(.)))


  # Training Phase 1
  training_data <-
    copula_data_macro %>%
    ungroup() %>%
    filter(Date < as_date(training_max_date))

  max_date_in_training_data <- training_data %>% pull(Date) %>% max(na.rm = T)
  message(glue::glue("Max date in Training data: {max_date_in_training_data}"))

  NN_form <-  create_lm_formula(dependant = "bin_var=='win'", independant = lm_vars1)
  glm_model_1 <- glm(formula = NN_form,
                         data = training_data ,
                         family = binomial("logit"))

  all_coefs <- glm_model_1 %>% jtools::j_summ() %>% pluck(1)
  coef_names <- row.names(all_coefs) %>% as.character()
  filtered_coefs <-
        all_coefs %>%
        as_tibble() %>%
        mutate(all_vars = coef_names) %>%
        filter(p <= p_value_thresh_for_inputs) %>%
        filter(!str_detect(all_vars, "Intercep")) %>%
        pull(all_vars) %>%
        as.character()
  NN_form <-  create_lm_formula(dependant = "bin_var=='win'", independant = filtered_coefs)
  NN_model_1 <- glm(formula = NN_form,
                        data = training_data ,
                        family = binomial("logit"))
  saveRDS(object = NN_model_1,
          file =glue::glue("{NN_path}/{dependant_var_name}_GLM_{1}.rds"))
  rm(NN_model_1)
  gc()
  gauss_form <-  create_lm_formula(dependant = "trade_return_dollar_aud", independant = lm_vars1)
  gauss_model_1 <- lm(formula = gauss_form,
                          data = training_data)
  all_coefs_gauss <- gauss_model_1 %>% jtools::j_summ() %>% pluck(1)
  coef_names_gauss <- row.names(all_coefs_gauss) %>% as.character()
  filtered_coefs_gauss <-
        all_coefs_gauss %>%
        as_tibble() %>%
        mutate(all_vars = coef_names_gauss) %>%
        filter(p <= p_value_thresh_for_inputs) %>%
        filter(!str_detect(all_vars, "Intercep")) %>%
        pull(all_vars) %>%
        as.character()
  gauss_form <-  create_lm_formula(dependant = "trade_return_dollar_aud", independant = filtered_coefs_gauss)
  gauss_model_1 <- lm(formula = gauss_form,
                          data = training_data)
  saveRDS(object = gauss_model_1,
          file = glue::glue("{NN_path}/{dependant_var_name}_LM_{1}.rds"))

  max_train_2_date <- ( as_date(max_date_in_training_data) + dweeks(phase_1_testing_weeks)) %>% as_date() %>% as.character()

  rm(gauss_model_1)
  gc()

  post_P1_testing_data <-
    copula_data_macro %>%
    ungroup() %>%
    distinct() %>%
    dplyr::select(Date, Asset, matches(filtered_coefs) )

  p1_testing <-
    read_NNs_create_preds_portfolio(
      copula_data_macro = copula_data_macro,
      lm_vars1 = lm_vars1,
      dependant_var_name = dependant_var_name,
      NN_path = NN_path,
      testing_min_date = testing_min_date_p1,
      trade_direction_var = analysis_direction,
      NN_index_to_choose = "",
      stop_value_var = stop_value_var,
      profit_value_var = profit_value_var,
      run_logit_instead = run_logit_instead,
      lag_price_col = lag_price_col,
      return_tagged_trades = FALSE,
      model_type = "logit",
      LM_interval = "fit"
    ) %>%
    dplyr::select(-pred_upr, -pred_lwr)

  gc()

  p1_testing_gauss <-
    read_NNs_create_preds_portfolio(
      copula_data_macro = copula_data_macro,
      lm_vars1 = lm_vars1,
      dependant_var_name = dependant_var_name,
      NN_path = NN_path,
      testing_min_date = testing_min_date_p1,
      trade_direction_var = analysis_direction,
      NN_index_to_choose = "",
      stop_value_var = stop_value_var,
      profit_value_var = profit_value_var,
      run_logit_instead = run_logit_instead,
      lag_price_col = lag_price_col,
      return_tagged_trades = FALSE,
      model_type = "LM",
      LM_interval = "fit"
    ) %>%
    dplyr::select(Date, Asset,
                  gauss_pred = pred,
                  gauss_pred_upr = pred_upr,
                  gauss_pred_lwr = pred_lwr)

  gc()

  p1_testing <-
    p1_testing%>%
    mutate(
      NN_samples = NN_samples,
      hidden_layers = hidden_layers,
      ending_thresh = ending_thresh,
      p_value_thresh_for_inputs = p_value_thresh_for_inputs,
      neuron_adjustment = neuron_adjustment
    ) %>%
    left_join(p1_testing_gauss)

  rm(p1_testing_gauss)
  gc()

  p2_training <-
    p1_testing %>%
    filter(Date <= max_train_2_date & Date >= as_date(max_date_in_training_data) ) %>%
    left_join( post_P1_testing_data %>%
                 dplyr::select(Date, Asset, matches(filtered_coefs) ) ) %>%
    mutate(win_or_loss_prev =
             case_when(
               trade_returns_AUD > 0 ~ 1,
               TRUE ~ 0
             ),
           win_or_loss_prev = lag(win_or_loss_prev, period_var),
           prev_pred_current = pred,
           prev_pred = lag(pred),
           prev_pred_gauss = lag(gauss_pred),
           prev_pred_gauss_lwr = lag(gauss_pred_lwr),

           pred_MA_5 = slider::slide_dbl(.x = prev_pred, .f = ~ mean(.x, na.rm = T), .before = 5),
           pred_MA_10 = slider::slide_dbl(.x = prev_pred, .f = ~ mean(.x, na.rm = T), .before = 10),
           pred_MA_10 = slider::slide_dbl(.x = prev_pred, .f = ~ mean(.x, na.rm = T), .before = 10),
           pred_MA_15 = slider::slide_dbl(.x = prev_pred, .f = ~ mean(.x, na.rm = T), .before = 15),
           pred_MA_20 = slider::slide_dbl(.x = prev_pred, .f = ~ mean(.x, na.rm = T), .before = 20),
           pred_MA_25 = slider::slide_dbl(.x = prev_pred, .f = ~ mean(.x, na.rm = T), .before = 25),
           pred_MA_30 = slider::slide_dbl(.x = prev_pred, .f = ~ mean(.x, na.rm = T), .before = 30),

           pred_MA_5_gauss = slider::slide_dbl(.x = prev_pred_gauss, .f = ~ mean(.x, na.rm = T), .before = 5),
           pred_MA_10_gauss = slider::slide_dbl(.x = prev_pred_gauss, .f = ~ mean(.x, na.rm = T), .before = 10),
           pred_MA_10_gauss = slider::slide_dbl(.x = prev_pred_gauss, .f = ~ mean(.x, na.rm = T), .before = 10),
           pred_MA_15_gauss = slider::slide_dbl(.x = prev_pred_gauss, .f = ~ mean(.x, na.rm = T), .before = 15),
           pred_MA_20_gauss = slider::slide_dbl(.x = prev_pred_gauss, .f = ~ mean(.x, na.rm = T), .before = 20),
           pred_MA_25_gauss = slider::slide_dbl(.x = prev_pred_gauss, .f = ~ mean(.x, na.rm = T), .before = 25),
           pred_MA_30_gauss = slider::slide_dbl(.x = prev_pred_gauss, .f = ~ mean(.x, na.rm = T), .before = 30),

           pred_MA_5_gauss_lwr = slider::slide_dbl(.x = prev_pred_gauss_lwr, .f = ~ mean(.x, na.rm = T), .before = 5),
           pred_MA_10_gauss_lwr = slider::slide_dbl(.x = prev_pred_gauss_lwr, .f = ~ mean(.x, na.rm = T), .before = 10),
           pred_MA_10_gauss_lwr = slider::slide_dbl(.x = prev_pred_gauss_lwr, .f = ~ mean(.x, na.rm = T), .before = 10),
           pred_MA_15_gauss_lwr = slider::slide_dbl(.x = prev_pred_gauss_lwr, .f = ~ mean(.x, na.rm = T), .before = 15),
           pred_MA_20_gauss_lwr = slider::slide_dbl(.x = prev_pred_gauss_lwr, .f = ~ mean(.x, na.rm = T), .before = 20),
           pred_MA_25_gauss_lwr = slider::slide_dbl(.x = prev_pred_gauss_lwr, .f = ~ mean(.x, na.rm = T), .before = 25),
           pred_MA_30_gauss_lwr = slider::slide_dbl(.x = prev_pred_gauss_lwr, .f = ~ mean(.x, na.rm = T), .before = 30),


           pred_SD_5 = slider::slide_dbl(.x = prev_pred, .f = ~ sd(.x, na.rm = T), .before = 5),
           pred_SD_10 = slider::slide_dbl(.x = prev_pred, .f = ~ sd(.x, na.rm = T), .before = 10),
           pred_SD_10 = slider::slide_dbl(.x = prev_pred, .f = ~ sd(.x, na.rm = T), .before = 10),
           pred_SD_15 = slider::slide_dbl(.x = prev_pred, .f = ~ sd(.x, na.rm = T), .before = 15),
           pred_SD_20 = slider::slide_dbl(.x = prev_pred, .f = ~ sd(.x, na.rm = T), .before = 20),
           pred_SD_25 = slider::slide_dbl(.x = prev_pred, .f = ~ sd(.x, na.rm = T), .before = 25),
           pred_SD_30 = slider::slide_dbl(.x = prev_pred, .f = ~ sd(.x, na.rm = T), .before = 30),

           pred_SD_5_gauss = slider::slide_dbl(.x = prev_pred_gauss, .f = ~ sd(.x, na.rm = T), .before = 5),
           pred_SD_10_gauss = slider::slide_dbl(.x = prev_pred_gauss, .f = ~ sd(.x, na.rm = T), .before = 10),
           pred_SD_10_gauss = slider::slide_dbl(.x = prev_pred_gauss, .f = ~ sd(.x, na.rm = T), .before = 10),
           pred_SD_15_gauss = slider::slide_dbl(.x = prev_pred_gauss, .f = ~ sd(.x, na.rm = T), .before = 15),
           pred_SD_20_gauss = slider::slide_dbl(.x = prev_pred_gauss, .f = ~ sd(.x, na.rm = T), .before = 20),
           pred_SD_25_gauss = slider::slide_dbl(.x = prev_pred_gauss, .f = ~ sd(.x, na.rm = T), .before = 25),
           pred_SD_30_gauss = slider::slide_dbl(.x = prev_pred_gauss, .f = ~ sd(.x, na.rm = T), .before = 30)
    )

  gc()

  # Train Second Model
  pred_ma_cols <- names(p2_training) %>%
    keep(~ str_detect(.x, "pred_MA|prev_pred|pred_SD")) %>%
    unlist() %>%
    as.character()

  NN_form <-  create_lm_formula(dependant = "bin_var=='win'",
                                independant =
                                  c(filtered_coefs, "prev_pred",
                                    "prev_pred_current", pred_ma_cols) )
  glm_model_1 <- glm(formula = NN_form,
                     data = p2_training ,
                     family = binomial("probit"))


  p2_testing <-
    p1_testing %>%
    filter(Date > max_train_2_date) %>%
    left_join(post_P1_testing_data %>%
                dplyr::select(Date, Asset, matches(filtered_coefs) ) ) %>%
    mutate(win_or_loss_prev =
             case_when(
               trade_returns_AUD > 0 ~ 1,
               TRUE ~ 0
             ),
           win_or_loss_prev = lag(win_or_loss_prev, period_var),
           prev_pred_current = pred,
           prev_pred = lag(pred),
           prev_pred_gauss = lag(gauss_pred),
           prev_pred_gauss_lwr = lag(gauss_pred_lwr),

           pred_MA_5 = slider::slide_dbl(.x = prev_pred, .f = ~ mean(.x, na.rm = T), .before = 5),
           pred_MA_10 = slider::slide_dbl(.x = prev_pred, .f = ~ mean(.x, na.rm = T), .before = 10),
           pred_MA_10 = slider::slide_dbl(.x = prev_pred, .f = ~ mean(.x, na.rm = T), .before = 10),
           pred_MA_15 = slider::slide_dbl(.x = prev_pred, .f = ~ mean(.x, na.rm = T), .before = 15),
           pred_MA_20 = slider::slide_dbl(.x = prev_pred, .f = ~ mean(.x, na.rm = T), .before = 20),
           pred_MA_25 = slider::slide_dbl(.x = prev_pred, .f = ~ mean(.x, na.rm = T), .before = 25),
           pred_MA_30 = slider::slide_dbl(.x = prev_pred, .f = ~ mean(.x, na.rm = T), .before = 30),

           pred_MA_5_gauss = slider::slide_dbl(.x = prev_pred_gauss, .f = ~ mean(.x, na.rm = T), .before = 5),
           pred_MA_10_gauss = slider::slide_dbl(.x = prev_pred_gauss, .f = ~ mean(.x, na.rm = T), .before = 10),
           pred_MA_10_gauss = slider::slide_dbl(.x = prev_pred_gauss, .f = ~ mean(.x, na.rm = T), .before = 10),
           pred_MA_15_gauss = slider::slide_dbl(.x = prev_pred_gauss, .f = ~ mean(.x, na.rm = T), .before = 15),
           pred_MA_20_gauss = slider::slide_dbl(.x = prev_pred_gauss, .f = ~ mean(.x, na.rm = T), .before = 20),
           pred_MA_25_gauss = slider::slide_dbl(.x = prev_pred_gauss, .f = ~ mean(.x, na.rm = T), .before = 25),
           pred_MA_30_gauss = slider::slide_dbl(.x = prev_pred_gauss, .f = ~ mean(.x, na.rm = T), .before = 30),

           pred_MA_5_gauss_lwr = slider::slide_dbl(.x = prev_pred_gauss_lwr, .f = ~ mean(.x, na.rm = T), .before = 5),
           pred_MA_10_gauss_lwr = slider::slide_dbl(.x = prev_pred_gauss_lwr, .f = ~ mean(.x, na.rm = T), .before = 10),
           pred_MA_10_gauss_lwr = slider::slide_dbl(.x = prev_pred_gauss_lwr, .f = ~ mean(.x, na.rm = T), .before = 10),
           pred_MA_15_gauss_lwr = slider::slide_dbl(.x = prev_pred_gauss_lwr, .f = ~ mean(.x, na.rm = T), .before = 15),
           pred_MA_20_gauss_lwr = slider::slide_dbl(.x = prev_pred_gauss_lwr, .f = ~ mean(.x, na.rm = T), .before = 20),
           pred_MA_25_gauss_lwr = slider::slide_dbl(.x = prev_pred_gauss_lwr, .f = ~ mean(.x, na.rm = T), .before = 25),
           pred_MA_30_gauss_lwr = slider::slide_dbl(.x = prev_pred_gauss_lwr, .f = ~ mean(.x, na.rm = T), .before = 30),


           pred_SD_5 = slider::slide_dbl(.x = prev_pred, .f = ~ sd(.x, na.rm = T), .before = 5),
           pred_SD_10 = slider::slide_dbl(.x = prev_pred, .f = ~ sd(.x, na.rm = T), .before = 10),
           pred_SD_10 = slider::slide_dbl(.x = prev_pred, .f = ~ sd(.x, na.rm = T), .before = 10),
           pred_SD_15 = slider::slide_dbl(.x = prev_pred, .f = ~ sd(.x, na.rm = T), .before = 15),
           pred_SD_20 = slider::slide_dbl(.x = prev_pred, .f = ~ sd(.x, na.rm = T), .before = 20),
           pred_SD_25 = slider::slide_dbl(.x = prev_pred, .f = ~ sd(.x, na.rm = T), .before = 25),
           pred_SD_30 = slider::slide_dbl(.x = prev_pred, .f = ~ sd(.x, na.rm = T), .before = 30),

           pred_SD_5_gauss = slider::slide_dbl(.x = prev_pred_gauss, .f = ~ sd(.x, na.rm = T), .before = 5),
           pred_SD_10_gauss = slider::slide_dbl(.x = prev_pred_gauss, .f = ~ sd(.x, na.rm = T), .before = 10),
           pred_SD_10_gauss = slider::slide_dbl(.x = prev_pred_gauss, .f = ~ sd(.x, na.rm = T), .before = 10),
           pred_SD_15_gauss = slider::slide_dbl(.x = prev_pred_gauss, .f = ~ sd(.x, na.rm = T), .before = 15),
           pred_SD_20_gauss = slider::slide_dbl(.x = prev_pred_gauss, .f = ~ sd(.x, na.rm = T), .before = 20),
           pred_SD_25_gauss = slider::slide_dbl(.x = prev_pred_gauss, .f = ~ sd(.x, na.rm = T), .before = 25),
           pred_SD_30_gauss = slider::slide_dbl(.x = prev_pred_gauss, .f = ~ sd(.x, na.rm = T), .before = 30)
    )

  rm(p1_testing)

  NNs_compiled <-
    tibble( pred = predict.glm(object = glm_model_1,
                               newdata = p2_testing,
                               type = "response") %>%
              as.numeric()) %>%
    mutate(
      index = row_number()
    )

  NNs_compiled2 <-
    NNs_compiled %>%
    group_by(index) %>%
    summarise(
      pred = mean(pred, na.rm = T)
    )

  pred_NN  <- NNs_compiled2 %>% pull(pred) %>% as.numeric()

  check_preds <-
    pred_NN %>% keep(~ !is.na(.x)) %>% length()

  max_preds <-
    pred_NN %>% max(na.rm = T)

  message(glue::glue("Number of Non-NA NN preds: {check_preds}"))

  post_testing_data <-
    p2_testing %>%
    mutate(
      pred = pred_NN
    )

  pred_at_max_date <-
    post_testing_data %>%
    ungroup() %>%
    slice_max(Date) %>%
    pull(pred) %>%
    as.numeric()

  max_date_in_post_testing_data <-
    post_testing_data %>%
    filter(!is.na(pred)) %>%
    pull(Date) %>%
    max(na.rm = T) %>%
    as.character()

  message(glue::glue("Date in Post-NN Prediction Data: {max_date_in_post_testing_data}"))
  message(glue::glue("Pred at max Date: {pred_at_max_date}"))

  periods_ahead <- period_var

  trade_dollar_returns <-
    p2_testing %>%
    dplyr::rename(pred_original = pred) %>%
    filter(!is.na(bin_var)) %>%
    left_join(post_testing_data %>% dplyr::select(Date, Asset, pred)) %>%
    dplyr::select(Date, Asset, profit_factor, stop_factor,
                  trade_returns_AUD,
                  trade_col, periods_ahead, pred, trade_start_prices , trade_end_prices, bin_var ) %>%
    filter(profit_factor == profit_value_var, stop_factor == stop_value_var) %>%
    filter(Asset == dependant_var_name) %>%
    mutate(minimal_loss = min(trade_returns_AUD, na.rm = T),
           maximum_win = max(trade_returns_AUD, na.rm = T),
           average_win = mean(trade_returns_AUD, na.rm = T),
           low_quantile_win = quantile(trade_returns_AUD,0.25 ,na.rm = T),
           high_quantile_win = quantile(trade_returns_AUD,0.75 ,na.rm = T)  ) %>%
    dplyr::select(Date, trade_returns_AUD, Asset,
                  profit_factor , stop_factor, maximum_win, minimal_loss,
                  average_win, low_quantile_win, high_quantile_win, pred,
                  trade_start_prices , trade_end_prices, bin_var) %>%
    distinct() %>%
    mutate(
      phase_1_testing_weeks = phase_1_testing_weeks
    ) %>%
    arrange(Date) %>%
    mutate(
      return_50_thresh = case_when(pred >= 0.5 ~ trade_returns_AUD, TRUE ~ 0),
      return_60_thresh = case_when(pred >= 0.6 ~ trade_returns_AUD, TRUE ~ 0),
      return_70_thresh = case_when(pred >= 0.7 ~ trade_returns_AUD, TRUE ~ 0),
      return_80_thresh = case_when(pred >= 0.8 ~ trade_returns_AUD, TRUE ~ 0),
      return_90_thresh = case_when(pred >= 0.9 ~ trade_returns_AUD, TRUE ~ 0),
      return_95_thresh = case_when(pred >= 0.95 ~ trade_returns_AUD, TRUE ~ 0),
      control_cumsum = cumsum(trade_returns_AUD),
      return_50_thresh_cumsum = cumsum(return_50_thresh),
      return_60_thresh_cumsum = cumsum(return_60_thresh),
      return_80_thresh_cumsum = cumsum(return_80_thresh)
    )

  if(return_tagged_trades == TRUE) {
    return(  tagged_trades <- post_testing_data %>%
               dplyr::select(Date, Asset, Price, Open, Low, High,profit_factor,
                             stop_factor,trade_col, pred))
  } else {
    return(trade_dollar_returns)
  }

}



#' generate_Logit_single_asset_model
#'
#' @param actual_wins_losses
#' @param raw_macro_data
#' @param countries_to_use
#' @param asset_var
#' @param pred_filter
#'
#' @return
#' @export
#'
#' @examples
generate_Logit_single_asset_model <-
  function(
    actual_wins_losses,
    raw_macro_data,
    countries_to_use = c("eur", "usd"),
    asset_var = "EUR_USD",
    pred_filter = 0.5,
    train_split_date = "2022-03-01",
    lag_days= 1,
    profit_value_var = profit_value_var,
    stop_value_var = stop_value_var,
    p_value_thresh_for_inputs = 0.15,
    periods_ahead_var = periods_ahead_var,
    trade_direction = "Long",
    save_model_location = "C:/Users/Nikhil Chandra/Documents/trade_data/asset_specific_NNs_Portfolio_macro_only"
  ) {

    model_data <-
      actual_wins_losses %>%
      filter(profit_factor == profit_value_var)%>%
      filter(stop_factor == stop_value_var) %>%
      filter(Asset == asset_var) %>%
      filter(periods_ahead == periods_ahead_var) %>%
      filter(trade_col == trade_direction) %>%
      mutate(
        bin_var =
          case_when(
            trade_start_prices > trade_end_prices & trade_col == "Short" ~ "win",
            trade_start_prices <= trade_end_prices & trade_col == "Short" ~ "loss",

            trade_start_prices < trade_end_prices & trade_col == "Long" ~ "win",
            trade_start_prices >= trade_end_prices & trade_col == "Long" ~ "loss"

          )
      ) %>%
      dplyr::select(Date, bin_var, Asset, trade_col,
                    profit_factor, stop_factor,
                    trade_start_prices, trade_end_prices,
                    starting_stop_value, starting_profit_value,
                    trade_return_dollar_aud, periods_ahead, End_Date)

    eur_macro_data <-
      get_EUR_Indicators(raw_macro_data,
                         lag_days = lag_days,
                         first_difference = TRUE
      ) %>%
      janitor::clean_names()

    usd_macro_data <-
      get_USD_Indicators(raw_macro_data,
                         lag_days = lag_days,
                         first_difference = TRUE
      ) %>%
      janitor::clean_names()

    aud_macro_data <-
      get_AUS_Indicators(raw_macro_data,
                         lag_days = lag_days,
                         first_difference = TRUE
      ) %>%
      janitor::clean_names()

    cad_macro_data <-
      get_CAD_Indicators(raw_macro_data,
                         lag_days = lag_days,
                         first_difference = TRUE
      ) %>%
      janitor::clean_names()

    nzd_macro_data <-
      get_NZD_Indicators(raw_macro_data,
                         lag_days = lag_days,
                         first_difference = TRUE
      ) %>%
      janitor::clean_names()

    gbp_macro_data <-
      get_GBP_Indicators(raw_macro_data,
                         lag_days = lag_days,
                         first_difference = TRUE
      ) %>%
      janitor::clean_names()

    cny_macro_data <-
      get_CNY_Indicators(raw_macro_data,
                         lag_days = lag_days,
                         first_difference = TRUE
      ) %>%
      janitor::clean_names()

    jpy_macro_data <-
      get_JPY_Indicators(raw_macro_data,
                         lag_days = lag_days,
                         first_difference = TRUE
      ) %>%
      janitor::clean_names()

    usd_macro_vars <- names(usd_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    eur_macro_vars <- names(eur_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    aud_macro_vars <- names(aud_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    cad_macro_vars <- names(cad_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    nzd_macro_vars <- names(nzd_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    gbp_macro_vars <- names(gbp_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    cny_macro_vars <- names(cny_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    jpy_macro_vars <- names(jpy_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()

    countries_to_use <-
      countries_to_use %>%
      map(~ paste0(.x, "_")) %>%
      unlist() %>%
      paste(collapse = "|")

    all_macro_vars <- c(
      usd_macro_vars,
      eur_macro_vars,
      aud_macro_vars,
      cad_macro_vars,
      nzd_macro_vars,
      gbp_macro_vars,
      cny_macro_vars,
      jpy_macro_vars
    ) %>%
      keep(
        ~ str_detect(.x, countries_to_use)
      ) %>%
      unlist() %>%
      as.character()

    model_data_wrangle <-
      model_data %>%
      mutate(Date_for_join = as_date(Date)) %>%
      left_join(
        eur_macro_data %>%
          rename(Date_for_join = date)
      )   %>%
      left_join(
        usd_macro_data %>%
          rename(Date_for_join = date)
      )   %>%
      left_join(
        aud_macro_data %>%
          rename(Date_for_join = date)
      )   %>%
      left_join(
        cad_macro_data %>%
          rename(Date_for_join = date)
      )   %>%
      left_join(
        gbp_macro_data %>%
          rename(Date_for_join = date)
      )   %>%
      left_join(
        cny_macro_data %>%
          rename(Date_for_join = date)
      )   %>%
      left_join(
        jpy_macro_data %>%
          rename(Date_for_join = date)
      )   %>%
      group_by(Asset) %>%
      arrange(Date, .by_group = TRUE) %>%
      group_by(Asset) %>%
      fill(matches(all_macro_vars, ignore.case = FALSE), .direction = "down") %>%
      ungroup()

    train_data <-
      model_data_wrangle %>%
      filter(Date <= train_split_date) %>%
      ungroup() %>%
      filter(if_all(everything() ,.fns = ~ !is.na(.)))

    test_data <-
      model_data_wrangle %>%
      filter(Date > train_split_date)

    NN_form <-  create_lm_formula(dependant = "bin_var=='win'", independant = all_macro_vars)
    logit_model_1 <- glm(formula = NN_form,
                         data = train_data ,
                         family = binomial("logit"))
    summary(logit_model_1)
    all_coefs <- logit_model_1 %>% jtools::j_summ() %>% pluck(1)
    coef_names <- row.names(all_coefs) %>% as.character()
    filtered_coefs <-
      all_coefs %>%
      as_tibble() %>%
      mutate(all_vars = coef_names) %>%
      filter(p <= p_value_thresh_for_inputs) %>%
      filter(!str_detect(all_vars, "Intercep")) %>%
      pull(all_vars) %>%
      as.character()
    NN_form <-  create_lm_formula(dependant = "bin_var=='win'", independant = filtered_coefs)
    logit_model_1 <- glm(formula = NN_form,
                         data = train_data ,
                         family = binomial("logit"))
    summary(logit_model_1)

    NN_form <-  create_lm_formula(dependant = "bin_var=='win'", independant = all_macro_vars)
    probit_model_1 <- glm(formula = NN_form,
                          data = train_data ,
                          family = binomial("probit"))
    summary(logit_model_1)
    all_coefs <- probit_model_1 %>% jtools::j_summ() %>% pluck(1)
    coef_names <- row.names(all_coefs) %>% as.character()
    filtered_coefs <-
      all_coefs %>%
      as_tibble() %>%
      mutate(all_vars = coef_names) %>%
      filter(p <= p_value_thresh_for_inputs) %>%
      filter(!str_detect(all_vars, "Intercep")) %>%
      pull(all_vars) %>%
      as.character()
    NN_form <-  create_lm_formula(dependant = "bin_var=='win'", independant = filtered_coefs)
    probit_model_1 <- glm(formula = NN_form,
                          data = train_data ,
                          family = binomial("logit"))
    summary(probit_model_1)

    if(!is.null(save_model_location)) {
      saveRDS(object = logit_model_1,
              file = glue::glue("{save_model_location}/Logit_Model_{asset_var}_{periods_ahead_var}_{trade_direction}.rds") )
      saveRDS(object = probit_model_1,
              file = glue::glue("{save_model_location}/Probit_Model_{asset_var}_{periods_ahead_var}_{trade_direction}.rds") )
    }

    predicted_values_logit <-
      predict.glm(logit_model_1,
                  newdata = test_data,
                  type = "response") %>%
      as.numeric()

    predicted_values_probit <-
      predict.glm(probit_model_1,
                  newdata = test_data,
                  type = "response")%>%
      as.numeric()

    review_returns <-
      test_data %>%
      dplyr::select(Date, bin_var, Asset, trade_col,
                    profit_factor, stop_factor,
                    trade_start_prices, trade_end_prices,
                    starting_stop_value, starting_profit_value,
                    trade_return_dollar_aud, periods_ahead, End_Date) %>%
      mutate(
        Logit_Pred = predicted_values_logit,
        Probit_Pred = predicted_values_probit
      )

    review_returns_filt <-
      review_returns %>%
      filter(Logit_Pred > pred_filter) %>%
      arrange(Date) %>%
      mutate(
        Cumulative_EUR_USD = cumsum(trade_return_dollar_aud)
      )

    return(list(review_returns, review_returns_filt))

  }


#' get_Port_Buy_Data_remaining_assets
#'
#' @param db_location
#' @param start_date
#' @param end_date
#' @param time_frame
#'
#' @returns
#' @export
#'
#' @examples
get_Port_Buy_Data_remaining_assets <- function(
    db_location = db_location,
    start_date = "2016-01-01",
    end_date = today() %>% as.character(),
    time_frame = "H1"
) {

  USD_CAD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame,
    asset = "USD_CAD",
    keep_bid_to_ask = TRUE
  )

  EUR_AUD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "ask",
      time_frame = time_frame,
      asset = "EUR_AUD",
      keep_bid_to_ask = TRUE
    )

  NZD_USD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "ask",
      time_frame = time_frame,
      asset = "NZD_USD",
      keep_bid_to_ask = TRUE
    )

  EUR_NZD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "ask",
      time_frame = time_frame,
      asset = "EUR_NZD",
      keep_bid_to_ask = TRUE
    )

  AUD_NZD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "ask",
      time_frame = time_frame,
      asset = "AUD_NZD",
      keep_bid_to_ask = TRUE
    )

  GBP_AUD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "ask",
      time_frame = time_frame,
      asset = "GBP_AUD",
      keep_bid_to_ask = TRUE
    )

  GBP_NZD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "ask",
      time_frame = time_frame,
      asset = "GBP_NZD",
      keep_bid_to_ask = TRUE
    )

  GBP_CAD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "ask",
      time_frame = time_frame,
      asset = "GBP_CAD",
      keep_bid_to_ask = TRUE
    )

  GBP_JPY <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "ask",
      time_frame = time_frame,
      asset = "GBP_JPY",
      keep_bid_to_ask = TRUE
    )

  USD_SGD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "ask",
      time_frame = time_frame,
      asset = "USD_SGD",
      keep_bid_to_ask = TRUE
    )

  EUR_JPY <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "ask",
      time_frame = time_frame,
      asset = "EUR_JPY",
      keep_bid_to_ask = TRUE
    )

  BTC_USD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "ask",
      time_frame = time_frame,
      asset = "BTC_USD",
      keep_bid_to_ask = TRUE
    )

  ETH_USD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "ask",
      time_frame = time_frame,
      asset = "ETH_USD",
      keep_bid_to_ask = TRUE
    )

  NATGAS_USD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "ask",
      time_frame = time_frame,
      asset = "NATGAS_USD",
      keep_bid_to_ask = TRUE
    )

  EUR_SEK <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "ask",
      time_frame = time_frame,
      asset = "EUR_SEK",
      keep_bid_to_ask = TRUE
    )

  USD_SEK <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "ask",
      time_frame = time_frame,
      asset = "USD_SEK",
      keep_bid_to_ask = TRUE
    )

  LTC_USD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "ask",
      time_frame = time_frame,
      asset = "LTC_USD",
      keep_bid_to_ask = TRUE
    )

  XAG_AUD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "ask",
      time_frame = time_frame,
      asset = "XAG_AUD",
      keep_bid_to_ask = TRUE
    )

  XAG_NZD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "ask",
      time_frame = time_frame,
      asset = "XAG_NZD",
      keep_bid_to_ask = TRUE
    )

  all_assets_long <-
    USD_CAD %>%
    bind_rows(EUR_AUD) %>%
    bind_rows(NZD_USD) %>%
    bind_rows(EUR_NZD) %>%
    bind_rows(AUD_NZD) %>%
    bind_rows(GBP_AUD) %>%
    bind_rows(GBP_NZD) %>%
    bind_rows(GBP_CAD) %>%
    bind_rows(GBP_JPY) %>%
    bind_rows(USD_SGD) %>%
    bind_rows(EUR_JPY) %>%
    bind_rows(BTC_USD) %>%
    bind_rows(ETH_USD) %>%
    bind_rows(NATGAS_USD) %>%
    bind_rows(EUR_SEK) %>%
    bind_rows(USD_SEK) %>%
    bind_rows(LTC_USD) %>%
    bind_rows(XAG_AUD) %>%
    bind_rows(XAG_NZD)

  rm( USD_CAD,
      EUR_AUD,
      NZD_USD,
      EUR_NZD,
      AUD_NZD,
      GBP_AUD,
      GBP_NZD,
      GBP_CAD,
      GBP_JPY,
      USD_SGD,
      EUR_JPY,
      BTC_USD,
      ETH_USD,
      NATGAS_USD,
      EUR_SEK,
      USD_SEK,
      LTC_USD,
      XAG_AUD,
      XAG_NZD)
  gc()


  USD_CAD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame,
    asset = "USD_CAD",
    keep_bid_to_ask = TRUE
  )

  EUR_AUD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "bid",
      time_frame = time_frame,
      asset = "EUR_AUD",
      keep_bid_to_ask = TRUE
    )

  NZD_USD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "bid",
      time_frame = time_frame,
      asset = "NZD_USD",
      keep_bid_to_ask = TRUE
    )

  EUR_NZD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "bid",
      time_frame = time_frame,
      asset = "EUR_NZD",
      keep_bid_to_ask = TRUE
    )

  AUD_NZD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "bid",
      time_frame = time_frame,
      asset = "AUD_NZD",
      keep_bid_to_ask = TRUE
    )

  GBP_AUD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "bid",
      time_frame = time_frame,
      asset = "GBP_AUD",
      keep_bid_to_ask = TRUE
    )

  GBP_NZD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "bid",
      time_frame = time_frame,
      asset = "GBP_NZD",
      keep_bid_to_ask = TRUE
    )

  GBP_CAD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "bid",
      time_frame = time_frame,
      asset = "GBP_CAD",
      keep_bid_to_ask = TRUE
    )

  GBP_JPY <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "bid",
      time_frame = time_frame,
      asset = "GBP_JPY",
      keep_bid_to_ask = TRUE
    )

  USD_SGD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "bid",
      time_frame = time_frame,
      asset = "USD_SGD",
      keep_bid_to_ask = TRUE
    )

  EUR_JPY <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "bid",
      time_frame = time_frame,
      asset = "EUR_JPY",
      keep_bid_to_ask = TRUE
    )

  BTC_USD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "bid",
      time_frame = time_frame,
      asset = "BTC_USD",
      keep_bid_to_ask = TRUE
    )

  ETH_USD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "bid",
      time_frame = time_frame,
      asset = "ETH_USD",
      keep_bid_to_ask = TRUE
    )

  NATGAS_USD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "bid",
      time_frame = time_frame,
      asset = "NATGAS_USD",
      keep_bid_to_ask = TRUE
    )

  EUR_SEK <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "bid",
      time_frame = time_frame,
      asset = "EUR_SEK",
      keep_bid_to_ask = TRUE
    )

  USD_SEK <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "bid",
      time_frame = time_frame,
      asset = "USD_SEK",
      keep_bid_to_ask = TRUE
    )

  LTC_USD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "bid",
      time_frame = time_frame,
      asset = "LTC_USD",
      keep_bid_to_ask = TRUE
    )

  XAG_AUD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "bid",
      time_frame = time_frame,
      asset = "XAG_AUD",
      keep_bid_to_ask = TRUE
    )

  XAG_NZD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "bid",
      time_frame = time_frame,
      asset = "XAG_NZD",
      keep_bid_to_ask = TRUE
    )

  all_assets_short <-
    USD_CAD %>%
    bind_rows(EUR_AUD) %>%
    bind_rows(NZD_USD) %>%
    bind_rows(EUR_NZD) %>%
    bind_rows(AUD_NZD) %>%
    bind_rows(GBP_AUD) %>%
    bind_rows(GBP_NZD) %>%
    bind_rows(GBP_CAD) %>%
    bind_rows(GBP_JPY) %>%
    bind_rows(USD_SGD) %>%
    bind_rows(EUR_JPY) %>%
    bind_rows(BTC_USD) %>%
    bind_rows(ETH_USD) %>%
    bind_rows(NATGAS_USD) %>%
    bind_rows(EUR_SEK) %>%
    bind_rows(USD_SEK) %>%
    bind_rows(LTC_USD) %>%
    bind_rows(XAG_AUD) %>%
    bind_rows(XAG_NZD)

  rm( USD_CAD,
      EUR_AUD,
      NZD_USD,
      EUR_NZD,
      AUD_NZD,
      GBP_AUD,
      GBP_NZD,
      GBP_CAD,
      GBP_JPY,
      USD_SGD,
      EUR_JPY,
      BTC_USD,
      ETH_USD,
      NATGAS_USD,
      EUR_SEK,
      USD_SEK,
      LTC_USD,
      XAG_AUD,
      XAG_NZD)
  gc()

  return(
    list(
      all_assets_long,
      all_assets_short
    )
  )

}


#' get_X_hours_port_trades
#'
#' @param raw_macro_data
#' @param daily_port_best_results_store
#' @param save_model_location
#' @param start_date
#' @param lag_days
#'
#' @return
#' @export
#'
#' @examples
get_X_hours_port_trades <- function(raw_macro_data = raw_macro_data,
                                    daily_port_best_results_store =
                                      "C:/Users/Nikhil Chandra/Documents/trade_data/Indices_Silver_Logit_sims_Daily_Port_best_results.db",
                                    save_model_location = "C:/Users/Nikhil Chandra/Documents/trade_data/asset_specific_NNs_Portfolio_macro_only",
                                    start_date = "2023-02-01",
                                    lag_days = 0
) {

  daily_port_best_results_store_con <- connect_db(path = daily_port_best_results_store)
  assets_to_use <- DBI::dbGetQuery(conn = daily_port_best_results_store_con,
                                   statement = "SELECT * FROM Indices_Silver_Logit_sims_Daily_Port_best_results")
  DBI::dbDisconnect(daily_port_best_results_store_con)

  eur_macro_data <-
    get_EUR_Indicators(raw_macro_data,
                       lag_days = lag_days,
                       first_difference = TRUE
    ) %>%
    janitor::clean_names()

  usd_macro_data <-
    get_USD_Indicators(raw_macro_data,
                       lag_days = lag_days,
                       first_difference = TRUE
    ) %>%
    janitor::clean_names()

  aud_macro_data <-
    get_AUS_Indicators(raw_macro_data,
                       lag_days = lag_days,
                       first_difference = TRUE
    ) %>%
    janitor::clean_names()

  cad_macro_data <-
    get_CAD_Indicators(raw_macro_data,
                       lag_days = lag_days,
                       first_difference = TRUE
    ) %>%
    janitor::clean_names()

  nzd_macro_data <-
    get_NZD_Indicators(raw_macro_data,
                       lag_days = lag_days,
                       first_difference = TRUE
    ) %>%
    janitor::clean_names()

  gbp_macro_data <-
    get_GBP_Indicators(raw_macro_data,
                       lag_days = lag_days,
                       first_difference = TRUE
    ) %>%
    janitor::clean_names()

  cny_macro_data <-
    get_CNY_Indicators(raw_macro_data,
                       lag_days = lag_days,
                       first_difference = TRUE
    ) %>%
    janitor::clean_names()

  jpy_macro_data <-
    get_JPY_Indicators(raw_macro_data,
                       lag_days = lag_days,
                       first_difference = TRUE
    ) %>%
    janitor::clean_names()

  usd_macro_vars <- names(usd_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
  eur_macro_vars <- names(eur_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
  aud_macro_vars <- names(aud_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
  cad_macro_vars <- names(cad_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
  nzd_macro_vars <- names(nzd_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
  gbp_macro_vars <- names(gbp_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
  cny_macro_vars <- names(cny_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
  jpy_macro_vars <- names(jpy_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()

  all_macro_vars <- c(
    usd_macro_vars,
    eur_macro_vars,
    aud_macro_vars,
    cad_macro_vars,
    nzd_macro_vars,
    gbp_macro_vars,
    cny_macro_vars,
    jpy_macro_vars
  )

  model_data_wrangle <-
    tibble(Date = seq( as_date(start_date), today(), "day" ) ) %>%
    mutate(Date_for_join = as_date(Date)) %>%
    left_join(
      eur_macro_data %>%
        rename(Date_for_join = date)
    )   %>%
    left_join(
      usd_macro_data %>%
        rename(Date_for_join = date)
    )   %>%
    left_join(
      aud_macro_data %>%
        rename(Date_for_join = date)
    )   %>%
    left_join(
      cad_macro_data %>%
        rename(Date_for_join = date)
    )   %>%
    left_join(
      gbp_macro_data %>%
        rename(Date_for_join = date)
    )   %>%
    left_join(
      cny_macro_data %>%
        rename(Date_for_join = date)
    )   %>%
    left_join(
      jpy_macro_data %>%
        rename(Date_for_join = date)
    )   %>%
    arrange(Date, .by_group = TRUE) %>%
    fill(matches(all_macro_vars, ignore.case = FALSE), .direction = "down") %>%
    ungroup()

  accumulation_list <- list()

  for (i in 1:dim(assets_to_use)[1] ) {

    asset_to_get_model_for <-
      assets_to_use$Asset[i] %>% as.character()

    periods_ahead_var <-
      assets_to_use$period_var[i] %>% as.character()

    trade_direction <-
      assets_to_use$trade_col[i] %>% as.character()

    pred_thresh <-
      assets_to_use$Pred_Thresh[i] %>% as.numeric()

    stop_value_var <-
      assets_to_use$stop_factor[i] %>% as.numeric()

    profit_value_var <-
      assets_to_use$profit_factor[i] %>% as.numeric()

    logit_model_to_use <-
      readRDS(glue::glue("{save_model_location}/Logit_Model_{asset_to_get_model_for}_{periods_ahead_var}_{trade_direction}.rds") )

    predicted_values <-
      predict.glm(logit_model_to_use,
                  newdata = model_data_wrangle,
                  type = "response") %>%
      as.numeric()

    accumulation_list[[i]] <-
      model_data_wrangle %>%
      dplyr::select(Date) %>%
      mutate(
        Logit_Pred = predicted_values,
        Asset = asset_to_get_model_for,
        trade_col = trade_direction,
        pred_thresh = pred_thresh,
        periods_ahead = periods_ahead_var,
        stop_factor = stop_value_var,
        profit_factor = profit_value_var
      )

  }

  returned_value <-
    accumulation_list %>%
    map_dfr(bind_rows) %>%
    group_by(Asset) %>%
    slice_max(Date) %>%
    ungroup()

  return(returned_value)

}

#' get_SPX_US2000_XAG_XAU
#'
#' @returns
#' @export
#'
#' @examples
get_Port_Buy_Data_2 <- function(
    db_location = db_location,
    start_date = "2016-01-01",
    end_date = today() %>% as.character(),
    time_frame = "H1",
    bid_or_ask_var = "ask"
) {


  EUR_CHF <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = bid_or_ask_var,
    time_frame = time_frame,
    asset = "EUR_CHF",
    keep_bid_to_ask = TRUE
  )

  EUR_SEK <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = bid_or_ask_var,
    time_frame = time_frame,
    asset = "EUR_SEK",
    keep_bid_to_ask = TRUE
  )

  GBP_CHF <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = bid_or_ask_var,
    time_frame = time_frame,
    asset = "GBP_CHF",
    keep_bid_to_ask = TRUE
  )

  GBP_JPY <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask_var,
      time_frame = time_frame,
      asset = "GBP_JPY",
      keep_bid_to_ask = TRUE
    )

  USD_CZK <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask_var,
      time_frame = time_frame,
      asset = "USD_CZK",
      keep_bid_to_ask = TRUE
    )

  USD_NOK <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask_var,
      time_frame = time_frame,
      asset = "USD_NOK",
      keep_bid_to_ask = TRUE
    )

  XAG_CAD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask_var,
      time_frame = time_frame,
      asset = "XAG_CAD",
      keep_bid_to_ask = TRUE
    )

  XAG_CHF <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask_var,
      time_frame = time_frame,
      asset = "XAG_CHF",
      keep_bid_to_ask = TRUE
    )

  XAG_JPY <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask_var,
      time_frame = time_frame,
      asset = "XAG_JPY",
      keep_bid_to_ask = TRUE
    )

  GBP_NZD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask_var,
      time_frame = time_frame,
      asset = "GBP_NZD",
      keep_bid_to_ask = TRUE
    )

  NZD_CHF <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask_var,
      time_frame = time_frame,
      asset = "NZD_CHF",
      keep_bid_to_ask = TRUE
    )

  USD_MXN <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask_var,
      time_frame = time_frame,
      asset = "USD_MXN",
      keep_bid_to_ask = TRUE
    )

  XPD_USD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask_var,
      time_frame = time_frame,
      asset = "XPD_USD",
      keep_bid_to_ask = TRUE
    )

  XPT_USD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask_var,
      time_frame = time_frame,
      asset = "XPT_USD",
      keep_bid_to_ask = TRUE
    )

  NATGAS_USD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask_var,
      time_frame = time_frame,
      asset = "NATGAS_USD",
      keep_bid_to_ask = TRUE
    )

  SG30_SGD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask_var,
      time_frame = time_frame,
      asset = "SG30_SGD",
      keep_bid_to_ask = TRUE
    )

  SOYBN_USD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask_var,
      time_frame = time_frame,
      asset = "SOYBN_USD",
      keep_bid_to_ask = TRUE
    )

  WHEAT_USD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask_var,
      time_frame = time_frame,
      asset = "WHEAT_USD",
      keep_bid_to_ask = TRUE
    )

  SUGAR_USD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask_var,
      time_frame = time_frame,
      asset = "SUGAR_USD",
      keep_bid_to_ask = TRUE
    )

  DE30_EUR <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask_var,
      time_frame = time_frame,
      asset = "DE30_EUR",
      keep_bid_to_ask = TRUE
    )

  UK10YB_GBP <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask_var,
      time_frame = time_frame,
      asset = "UK10YB_GBP",
      keep_bid_to_ask = TRUE
    )

  JP225_USD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask_var,
      time_frame = time_frame,
      asset = "JP225_USD",
      keep_bid_to_ask = TRUE
    )

  CH20_CHF <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask_var,
      time_frame = time_frame,
      asset = "CH20_CHF",
      keep_bid_to_ask = TRUE
    )

  NL25_EUR <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask_var,
      time_frame = time_frame,
      asset = "NL25_EUR",
      keep_bid_to_ask = TRUE
    )

  XAG_SGD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask_var,
      time_frame = time_frame,
      asset = "XAG_SGD",
      keep_bid_to_ask = TRUE
    )

  BCH_USD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask_var,
      time_frame = time_frame,
      asset = "BCH_USD",
      keep_bid_to_ask = TRUE
    )

  LTC_USD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask_var,
      time_frame = time_frame,
      asset = "LTC_USD",
      keep_bid_to_ask = TRUE
    )

  EUR_USD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask_var,
      time_frame = time_frame,
      asset = "EUR_USD",
      keep_bid_to_ask = TRUE
    )

  EU50_EUR <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask_var,
      time_frame = time_frame,
      asset = "EU50_EUR",
      keep_bid_to_ask = TRUE
    )

  SPX500_USD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask_var,
      time_frame = time_frame,
      asset = "SPX500_USD",
      keep_bid_to_ask = TRUE
    )

  US2000_USD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask_var,
      time_frame = time_frame,
      asset = "US2000_USD",
      keep_bid_to_ask = TRUE
    )

  USB10Y_USD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask_var,
      time_frame = time_frame,
      asset = "USB10Y_USD",
      keep_bid_to_ask = TRUE
    )

  USD_JPY <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask_var,
      time_frame = time_frame,
      asset = "USD_JPY",
      keep_bid_to_ask = TRUE
    )

  AUD_USD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask_var,
      time_frame = time_frame,
      asset = "AUD_USD",
      keep_bid_to_ask = TRUE
    )

  XAG_USD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask_var,
      time_frame = time_frame,
      asset = "XAG_USD",
      keep_bid_to_ask = TRUE
    )

  XAG_EUR <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask_var,
      time_frame = time_frame,
      asset = "XAG_EUR",
      keep_bid_to_ask = TRUE
    )

  BTC_USD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask_var,
      time_frame = time_frame,
      asset = "BTC_USD",
      keep_bid_to_ask = TRUE
    )

  XAU_USD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask_var,
      time_frame = time_frame,
      asset = "XAU_USD",
      keep_bid_to_ask = TRUE
    )

  XAU_EUR <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask_var,
      time_frame = time_frame,
      asset = "XAU_EUR",
      keep_bid_to_ask = TRUE
    )

  GBP_USD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask_var,
      time_frame = time_frame,
      asset = "GBP_USD",
      keep_bid_to_ask = TRUE
    )

  USD_CAD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask_var,
      time_frame = time_frame,
      asset = "USD_CAD",
      keep_bid_to_ask = TRUE
    )

  USD_SEK <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask_var,
      time_frame = time_frame,
      asset = "USD_SEK",
      keep_bid_to_ask = TRUE
    )

  EUR_NOK <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask_var,
      time_frame = time_frame,
      asset = "EUR_NOK",
      keep_bid_to_ask = TRUE
    )

  EUR_AUD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask_var,
      time_frame = time_frame,
      asset = "EUR_AUD",
      keep_bid_to_ask = TRUE
    )

  GBP_AUD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask_var,
      time_frame = time_frame,
      asset = "GBP_AUD",
      keep_bid_to_ask = TRUE
    )

  XAG_GBP <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask_var,
      time_frame = time_frame,
      asset = "XAG_GBP",
      keep_bid_to_ask = TRUE
    )

  XAU_GBP <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask_var,
      time_frame = time_frame,
      asset = "XAU_GBP",
      keep_bid_to_ask = TRUE
    )

  EUR_JPY <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask_var,
      time_frame = time_frame,
      asset = "EUR_JPY",
      keep_bid_to_ask = TRUE
    )

  USD_SGD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask_var,
      time_frame = time_frame,
      asset = "USD_SGD",
      keep_bid_to_ask = TRUE
    )

  XAU_SGD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask_var,
      time_frame = time_frame,
      asset = "XAU_SGD",
      keep_bid_to_ask = TRUE
    )

  XAU_CAD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask_var,
      time_frame = time_frame,
      asset = "XAU_CAD",
      keep_bid_to_ask = TRUE
    )

  NZD_USD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask_var,
      time_frame = time_frame,
      asset = "NZD_USD",
      keep_bid_to_ask = TRUE
    )

  XAU_NZD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask_var,
      time_frame = time_frame,
      asset = "XAU_NZD",
      keep_bid_to_ask = TRUE
    )

  XAG_NZD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask_var,
      time_frame = time_frame,
      asset = "XAG_NZD",
      keep_bid_to_ask = TRUE
    )

  FR40_EUR <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask_var,
      time_frame = time_frame,
      asset = "FR40_EUR",
      keep_bid_to_ask = TRUE
    )

  UK100_GBP <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask_var,
      time_frame = time_frame,
      asset = "UK100_GBP",
      keep_bid_to_ask = TRUE
    )

  AU200_AUD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask_var,
      time_frame = time_frame,
      asset = "AU200_AUD",
      keep_bid_to_ask = TRUE
    )

  HK33_HKD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask_var,
      time_frame = time_frame,
      asset = "HK33_HKD",
      keep_bid_to_ask = TRUE
    )

  SG30_SGD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask_var,
      time_frame = time_frame,
      asset = "SG30_SGD",
      keep_bid_to_ask = TRUE
    )

  US2000_USD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask_var,
      time_frame = time_frame,
      asset = "US2000_USD",
      keep_bid_to_ask = TRUE
    )

  XAU_GBP <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask_var,
      time_frame = time_frame,
      asset = "XAU_GBP",
      keep_bid_to_ask = TRUE
    )

  XAG_AUD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask_var,
      time_frame = time_frame,
      asset = "XAG_AUD",
      keep_bid_to_ask = TRUE
    )

  XAU_AUD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask_var,
      time_frame = time_frame,
      asset = "XAU_AUD",
      keep_bid_to_ask = TRUE
    )

  XAU_JPY <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask_var,
      time_frame = time_frame,
      asset = "XAU_JPY",
      keep_bid_to_ask = TRUE
    )

  USB02Y_USD <-
    create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = bid_or_ask_var,
      time_frame = time_frame,
      asset = "USB02Y_USD",
      keep_bid_to_ask = TRUE
    )

  returned <-
    EUR_CHF %>%
    bind_rows(EUR_SEK) %>%
    bind_rows(GBP_CHF)%>%
    bind_rows(GBP_JPY)%>%
    bind_rows(USD_CZK)%>%
    bind_rows(USD_NOK) %>%
    bind_rows(XAG_CAD)%>%
    bind_rows(XAG_CHF)%>%
    bind_rows(XAG_JPY) %>%
    bind_rows(GBP_NZD) %>%
    bind_rows(NZD_CHF) %>%
    bind_rows(USD_MXN) %>%
    bind_rows(XPD_USD) %>%
    bind_rows(XPT_USD) %>%
    bind_rows(NATGAS_USD) %>%
    bind_rows(SG30_SGD) %>%
    bind_rows(SOYBN_USD) %>%
    bind_rows(WHEAT_USD) %>%
    bind_rows(SUGAR_USD) %>%
    bind_rows(DE30_EUR) %>%
    bind_rows(UK10YB_GBP) %>%
    bind_rows(JP225_USD) %>%
    bind_rows(CH20_CHF) %>%
    bind_rows(NL25_EUR) %>%
    bind_rows(XAG_SGD) %>%
    bind_rows(BCH_USD) %>%
    bind_rows(LTC_USD) %>%
    bind_rows(EUR_USD) %>%
    bind_rows(EU50_EUR) %>%
    bind_rows(SPX500_USD) %>%
    bind_rows(US2000_USD) %>%
    bind_rows(USB10Y_USD) %>%
    bind_rows(USD_JPY) %>%
    bind_rows(AUD_USD) %>%
    bind_rows(XAG_USD) %>%
    bind_rows(XAG_EUR) %>%
    bind_rows(BTC_USD) %>%
    bind_rows(XAU_USD) %>%
    bind_rows(XAU_EUR) %>%
    bind_rows(GBP_USD) %>%
    bind_rows(USD_CAD) %>%
    bind_rows(USD_SEK) %>%
    bind_rows(EUR_AUD) %>%
    bind_rows(GBP_AUD) %>%
    bind_rows(XAG_GBP) %>%
    bind_rows(XAU_GBP) %>%
    bind_rows(EUR_JPY) %>%
    bind_rows(XAU_SGD) %>%
    bind_rows(XAU_CAD) %>%
    bind_rows(NZD_USD) %>%
    bind_rows(XAU_NZD) %>%
    bind_rows(XAG_NZD) %>%
    bind_rows(FR40_EUR) %>%
    bind_rows(UK100_GBP) %>%
    bind_rows(AU200_AUD) %>%
    bind_rows(HK33_HKD) %>%
    bind_rows(SG30_SGD) %>%
    bind_rows(US2000_USD) %>%
    bind_rows(XAG_AUD) %>%
    bind_rows(XAU_AUD) %>%
    bind_rows(XAU_JPY) %>%
    bind_rows(USB02Y_USD) %>%
    distinct()

  rm(EUR_CHF,
     EUR_SEK,
     GBP_CHF,
     GBP_JPY,
     USD_CZK,
     USD_NOK,
     XAG_CAD,
     XAG_CHF,
     XAG_JPY,
     GBP_NZD,
     NZD_CHF,
     USD_MXN,
     XPD_USD,
     XPT_USD,
     NATGAS_USD,
     SG30_SGD,
     SOYBN_USD,
     WHEAT_USD,
     SUGAR_USD,
     DE30_EUR,
     UK10YB_GBP,
     JP225_USD,
     CH20_CHF,
     NL25_EUR,
     XAG_SGD,
     BCH_USD,
     LTC_USD,
     EUR_USD,
     EU50_EUR,
     SPX500_USD,
     US2000_USD,
     USB10Y_USD,
     USD_JPY,
     AUD_USD,
     XAG_USD,
     XAG_EUR,
     BTC_USD,
     XAU_USD,
     XAU_EUR,
     GBP_USD,
     USD_CAD,
     USD_SEK,
     EUR_AUD,
     GBP_AUD,
     XAG_GBP,
     XAU_GBP,
     EUR_JPY,
     XAU_SGD,
     XAU_CAD,
     NZD_USD,
     XAU_NZD,
     XAG_NZD,
     SG30_SGD,
     US2000_USD)

  gc()

  return(returned)

}

get_Port_Buy_Data_2_complete <-
  function(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    time_frame = "H1"
    ) {
    Indices_Metals_Bonds <-
      list(
        get_Port_Buy_Data_2(
          db_location = db_location,
          start_date = start_date,
          end_date = end_date,
          time_frame = time_frame,
          bid_or_ask_var = "ask"
        ),
        get_Port_Buy_Data_2(
          db_location = db_location,
          start_date = start_date,
          end_date = end_date,
          time_frame = time_frame,
          bid_or_ask_var = "bid"
        )
      )

    return(Indices_Metals_Bonds)
  }
