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

  XAG_SPX_US2000_USD <- SPX %>% bind_rows(US2000) %>% bind_rows(XAG)%>% bind_rows(XAU)
  rm(SPX, US2000, XAG, XAU)
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

    aus_macro_data <-
      get_AUS_Indicators(raw_macro_data,
                         lag_days = lag_days,
                         first_difference = TRUE
      ) %>%
      janitor::clean_names()

    aus_macro_data_PCA <-
      aus_macro_data %>%
      dplyr::select(-date) %>%
      prcomp(scale = TRUE) %>%
      pluck("x") %>%
      as_tibble() %>%
      dplyr::select(AUD_PC1= PC1,
                    AUD_PC2= PC2,
                    AUD_PC3= PC3,
                    AUD_PC4= PC4,
                    AUD_PC5= PC5) %>%
      mutate(
        date = aus_macro_data %>% pull(date)
      )

    nzd_macro_data <-
      get_NZD_Indicators(raw_macro_data,
                         lag_days = lag_days,
                         first_difference = TRUE
      ) %>%
      janitor::clean_names()

    nzd_macro_data_PCA <-
      nzd_macro_data %>%
      dplyr::select(-date) %>%
      prcomp(scale = TRUE) %>%
      pluck("x") %>%
      as_tibble() %>%
      dplyr::select(NZD_PC1= PC1,
                    NZD_PC2= PC2,
                    NZD_PC3= PC3,
                    NZD_PC4= PC4,
                    NZD_PC5= PC5) %>%
      mutate(
        date = nzd_macro_data %>% pull(date)
      )

    usd_macro_data <-
      get_USD_Indicators(raw_macro_data,
                         lag_days = lag_days,
                         first_difference = TRUE
      ) %>%
      janitor::clean_names()

    usd_macro_data_PCA <-
      usd_macro_data %>%
      dplyr::select(-date) %>%
      prcomp(scale = TRUE) %>%
      pluck("x") %>%
      as_tibble() %>%
      dplyr::select(USD_PC1= PC1,
                    USD_PC2= PC2,
                    USD_PC3= PC3,
                    USD_PC4= PC4,
                    USD_PC5= PC5) %>%
      mutate(
        date = usd_macro_data %>% pull(date)
      )

    cny_macro_data <-
      get_CNY_Indicators(raw_macro_data,
                         lag_days = lag_days,
                         first_difference = TRUE
      ) %>%
      janitor::clean_names()

    cny_macro_data_PCA <-
      cny_macro_data %>%
      dplyr::select(-date) %>%
      prcomp(scale = TRUE) %>%
      pluck("x") %>%
      as_tibble() %>%
      dplyr::select(CNY_PC1= PC1,
                    CNY_PC2= PC2,
                    CNY_PC3= PC3,
                    CNY_PC4= PC4,
                    CNY_PC5= PC5) %>%
      mutate(
        date = cny_macro_data %>% pull(date)
      )

    eur_macro_data <-
      get_EUR_Indicators(raw_macro_data,
                         lag_days = lag_days,
                         first_difference = TRUE
      ) %>%
      janitor::clean_names()

    eur_macro_data_PCA <-
      eur_macro_data %>%
      dplyr::select(-date) %>%
      prcomp(scale = TRUE) %>%
      pluck("x") %>%
      as_tibble() %>%
      dplyr::select(EUR_PC1= PC1,
                    EUR_PC2= PC2,
                    EUR_PC3= PC3,
                    EUR_PC4= PC4,
                    EUR_PC5= PC5) %>%
      mutate(
        date = eur_macro_data %>% pull(date)
      )

    aud_macro_vars <- names(aus_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    nzd_macro_vars <- names(nzd_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    usd_macro_vars <- names(usd_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    cny_macro_vars <- names(cny_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    eur_macro_vars <- names(eur_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    all_macro_vars <- c(aud_macro_vars, nzd_macro_vars, usd_macro_vars, cny_macro_vars, eur_macro_vars)

    PC_macro_vars <-
      c("AUD_PC1", "AUD_PC2",
        "NZD_PC1", "NZD_PC2",
        "USD_PC1", "USD_PC2",
        "CNY_PC1", "CNY_PC2",
        "EUR_PC1", "EUR_PC2")

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
    distinct() %>%
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
                glue::glue("C:/Users/Nikhil Chandra/Documents/trade_data/asset_specific_NNs/{dependant_var_name}_NN_{i}.rds")
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
                glue::glue("C:/Users/Nikhil Chandra/Documents/trade_data/asset_specific_NNs/{dependant_var_name}_GLM_{i}.rds")
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

create_NN_AUD_USD_XCU_NZD <- function(
    AUD_USD_NZD_USD = AUD_USD_NZD_USD_list[[1]],
    raw_macro_data = raw_macro_data,
    actual_wins_losses = actual_wins_losses,
    lag_days = 1,
    lm_train_prop = 0.4,
    lm_test_prop = 0.55,
    realised_trade_data_db_loc = "C:/Users/Nikhil Chandra/Documents/trade_data/full_ts_trades_mapped_AUD_USD.db",
    max_NNs = 5,
    hidden_layers = c(33),
    NN_samples = 1000,
    NN_thresh = 0.09,
    dependant_var_name = "AUD_USD",
    analysis_direction = "Short",
    rerun_base_models = FALSE,
    use_NN = FALSE,
    average_NN_logit = FALSE,
    stop_value_var = 15,
    profit_value_var = 20,
    prob_fac = 0.5,
    asset_infor = asset_infor,
    currency_conversion = currency_conversion,
    analysis_threshs = seq(0.5,0.99999, 0.05),
    sample_based_sep = FALSE
){

  assets_to_return <- dependant_var_name

  aus_macro_data <-
    get_AUS_Indicators(raw_macro_data,
                       lag_days = lag_days
                       # first_difference = TRUE
                       ) %>%
    janitor::clean_names()

  nzd_macro_data <-
    get_NZD_Indicators(raw_macro_data,
                       lag_days = lag_days
                       # first_difference = TRUE
                       ) %>%
    janitor::clean_names()
  usd_macro_data <-
    get_USD_Indicators(raw_macro_data,
                       lag_days = lag_days
                       # first_difference = TRUE
                       ) %>%
    janitor::clean_names()
  cny_macro_data <-
    get_CNY_Indicators(raw_macro_data,
                       lag_days = lag_days
                       # first_difference = TRUE
                       ) %>%
    janitor::clean_names()
  eur_macro_data <-
    get_EUR_Indicators(raw_macro_data,
                       lag_days = lag_days
                       # first_difference = TRUE
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

  binary_data_for_post_model <-
    actual_wins_losses %>%
    filter(asset %in% assets_to_return) %>%
    filter(trade_col == analysis_direction) %>%
    rename(Date = dates, Asset = asset) %>%
    filter(profit_factor == profit_value_var, stop_factor == stop_value_var) %>%
    mutate(
      bin_var =
        case_when(
          trade_start_prices > trade_end_prices & trade_col == "Short" ~ "win",
          trade_start_prices <= trade_end_prices & trade_col == "Short" ~ "loss",

          trade_start_prices < trade_end_prices & trade_col == "Long" ~ "win",
          trade_start_prices >= trade_end_prices & trade_col == "Long" ~ "loss"

          # trade_returns <= 0 ~ "loss"
        )
    ) %>%
    dplyr::select(Date, bin_var)

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
      lagged_var_1 = lag(!!as.name(dependant_var_name), 1),
      lagged_var_5 = lag(!!as.name(dependant_var_name), 5),
      lagged_var_10 = lag(!!as.name(dependant_var_name), 10),
      dependant_var = lead(!!as.name(dependant_var_name), 1) - !!as.name(dependant_var_name),
      dependant_var_5 = lead(!!as.name(dependant_var_name), 5) - !!as.name(dependant_var_name),
      dependant_var_10 = lead(!!as.name(dependant_var_name), 10) - !!as.name(dependant_var_name)
    ) %>%
    mutate(
      dependant_var_bin = ifelse(dependant_var > 0, "long", "short"),
      dependant_var_bin_5 = ifelse(dependant_var_5 > 0, "long", "short"),
      dependant_var_bin_10 = ifelse(dependant_var_10 > 0, "long", "short")
    ) %>%
    left_join(binary_data_for_post_model) %>%
    mutate(
      bin_var = lead(bin_var)
    ) %>%
    filter(!is.na(bin_var)) %>%
    mutate(hour_of_day = lubridate::hour(Date) %>% as.numeric(),
           day_of_week = lubridate::wday(Date) %>% as.numeric())

  lm_quant_vars <- names(copula_data_macro) %>% keep(~ str_detect(.x,"quantiles|tangent|cor"))
  lm_vars1 <- c(all_macro_vars, lm_quant_vars,
                "lagged_var_1", "lagged_var_5", "lagged_var_10",
                "hour_of_day", "day_of_week")

  set.seed(round(runif(n = 1, min = 0, max = 100000)))


  if(sample_based_sep == TRUE) {
    training_data <- copula_data_macro %>%
      slice_sample(prop = lm_train_prop) %>%
      filter(!is.na(lagged_var_10))

    training_dates <- training_data %>% pull(Date) %>% unique()

    testing_data <- copula_data_macro %>%
      filter(!(Date %in% training_dates)) %>%
      slice_sample(prop = lm_test_prop)
  } else {

    training_data <- copula_data_macro %>%
      slice_head(prop = lm_train_prop) %>%
      filter(!is.na(lagged_var_10))

    training_dates <- training_data %>% pull(Date) %>% unique()

    testing_data <- copula_data_macro %>%
      filter(!(Date %in% training_dates)) %>%
      slice_tail(prop = lm_test_prop)

  }

  max_data_in_testing_data <- testing_data %>%
    pull(Date) %>%
    max(na.rm = T) %>%
    as.character()

  message(glue::glue("Max Date in Testing Data AUD NZD: {max_data_in_testing_data}"))
  NN_form <-  create_lm_formula(dependant = "bin_var=='win'", independant = lm_vars1)

  if(rerun_base_models == TRUE) {

    for (i in 1:max_NNs) {
      set.seed(round(runif(1,2,10000)))
      NN_model_1 <- neuralnet::neuralnet(formula = NN_form,
                                         hidden = c(33),
                                         data = training_data %>% slice_sample(n = NN_samples),
                                         lifesign = 'full',
                                         rep = 1,
                                         stepmax = 1000000,
                                         threshold = 0.09)

      saveRDS(object = NN_model_1,
              file =
                glue::glue("C:/Users/Nikhil Chandra/Documents/trade_data/asset_specific_NNs/{dependant_var_name}_NN_{i}.rds")
      )

    }


    # NN_model_1 <- readRDS(glue::glue("C:/Users/Nikhil Chandra/Documents/trade_data/asset_specific_NNs/{dependant_var_name}_NN.rds"))
    pred_NN  <- predict(object = NN_model_1,newdata = testing_data) %>%
      as.numeric()

  }

  logit_model <- glm(formula = NN_form, family = binomial("logit"), data = training_data)
  summary(logit_model)

  pred_train  <- predict.glm(object = logit_model,
                       newdata = training_data,
                       type = "response") %>%
    as.numeric()

  mean_pred <- mean(pred_train)
  sd_pred <- sd(pred_train)

  pred  <- predict.glm(object = logit_model,
                       newdata = testing_data,
                       type = "response") %>%
    as.numeric()

  if(use_NN == TRUE) {

    NNs_compiled <-
      fs::dir_info("C:/Users/Nikhil Chandra/Documents/trade_data/asset_specific_NNs/")  %>%
      filter(str_detect(path, as.character(glue::glue("{dependant_var_name}_NN_")) )) %>%
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

    NNs_compiled2 <- NNs_compiled %>%
      group_by(index) %>%
      summarise(
        pred = mean(pred, na.rm = T)
      )

    pred_NN  <- NNs_compiled2 %>% pull(pred) %>% as.numeric()

  }

  post_testing_data <-
    testing_data %>%
    mutate(
      pred =
        case_when(
          use_NN == TRUE & average_NN_logit == FALSE ~ pred_NN,
          use_NN == TRUE & average_NN_logit == TRUE ~ (pred_NN + pred)/2,
          TRUE ~ pred
          )
    )

  tagged_trades  <-
    post_testing_data %>%
    mutate(
      trade_col = case_when(pred >= prob_fac ~ analysis_direction,
                            TRUE ~ "No Trade")
    ) %>%
    ungroup()

  trade_dollar_returns <-
    actual_wins_losses %>%
    rename(Date = dates, Asset = asset) %>%
    filter(profit_factor == profit_value_var, stop_factor == stop_value_var) %>%
    filter(Asset %in% assets_to_return) %>%
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
      trade_returns_orig = trade_returns,
      trade_returns_AUD =
        case_when(
          trade_col == "Long" & trade_start_prices > trade_end_prices ~ maximum_win,
          trade_col == "Long" & trade_start_prices <= trade_end_prices ~ -1*minimal_loss,

          trade_col == "Short" & trade_start_prices < trade_end_prices ~ maximum_win,
          trade_col == "Short" & trade_start_prices >= trade_end_prices ~ -1*minimal_loss
        )
    ) %>%
    dplyr::select(Date, trade_returns_AUD, Asset, trade_returns_orig,
                  profit_factor , stop_factor) %>%
    distinct()

  analysis_control <-
    post_testing_data %>%
    ungroup() %>%
    left_join(trade_dollar_returns)  %>%
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
      mutate(
        trade_col = case_when(pred >= analysis_threshs[i] ~ analysis_direction,
                              TRUE ~ "No Trade")
      ) %>%
      ungroup()

    analysis_list[[i]] <-
      temp_trades %>%
      left_join(trade_dollar_returns) %>%
      group_by(trade_col, bin_var) %>%
      summarise(
        wins_losses = n(),
        win_amount = max(trade_returns_AUD),
        loss_amount = min(trade_returns_AUD),
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

  return(
    list(analysis,
         tagged_trades)
  )

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
    sim_table = "AUD_USD_NZD_XCU_NN_sims"
) {

  date_filter_for_Logit <-
    as.character(copula_data[[1]]$Date %>% max(na.rm = T) + days(1))

  Logit_sims_db_con <- connect_db(path = Logit_sims_db)
  all_results_ts_dfr <- DBI::dbGetQuery(conn = Logit_sims_db_con,
                                        statement = as.character(glue::glue("SELECT * FROM {sim_table}")) )
  DBI::dbDisconnect(Logit_sims_db_con)
  rm(Logit_sims_db_con)

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
           outperformance_count > outperformance_count_min,
           risk_weighted_return_mid > risk_weighted_return_mid_min) %>%
    # filter(simulations >= 20, edge > 0, outperformance_count > 0.51, risk_weighted_return_mid > 0.1) %>%
    group_by(Asset) %>%
    slice_max(risk_weighted_return_mid)%>%
    ungroup()


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

    accumulating_trades[[i]] <-
      NN_test_preds %>%
      slice_max(Date) %>%
      filter(Asset == gernating_params$Asset[i] %>% as.character()) %>%
      mutate(
        pred_min = gernating_params$threshold[i] %>% as.numeric()
      )

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
    full_ts_trade_db_location = "C:/Users/Nikhil Chandra/Documents/trade_data/full_ts_trades_mapped_AUD_USD.db"
    ) {

    full_ts_trade_db_con <- connect_db(path = full_ts_trade_db_location)
    actual_wins_losses <-
      DBI::dbGetQuery(full_ts_trade_db_con,
                      "SELECT * FROM full_ts_trades_mapped") %>%
      mutate(
        dates = as_datetime(dates)
      )
    DBI::dbDisconnect(full_ts_trade_db_con)
    rm(full_ts_trade_db_con)

    return(actual_wins_losses)
  }
