#' create_NN_AUD_USD_XCU_NZD_data
#'
#' @return
#' @export
#'
#' @examples
create_NN_Indices_data <-
  function(SPX_US2000_XAG = SPX_US2000_XAG_ALL[[1]],
           raw_macro_data,
           actual_wins_losses = actual_wins_losses,
           lag_days = 1,
           stop_value_var = 15,
           profit_value_var = 20,
           use_PCA_vars = FALSE,
           rolling_period_index_PCA_cor = 15) {


    Indices_log_cumulative <-
      c("SPX500_USD", "EU50_EUR", "US2000_USD", "AU200_AUD", "XAG_USD", "XAU_USD") %>%
      map_dfr(
        ~
          create_log_cumulative_returns(
            asset_data_to_use = SPX_US2000_XAG,
            asset_to_use = c(.x[1]),
            price_col = "Open",
            return_long_format = TRUE
          )
      ) %>%
      left_join(
        SPX_US2000_XAG %>% distinct(Date, Asset, Price, Open, High, Low)
      )

    Indices_log_cumulative <-
      Indices_log_cumulative %>%
      group_by(Asset) %>%
      mutate(
        Return_Index_Diff = ((Price - Open)/Open)*100
      ) %>%
      ungroup() %>%
      filter(!is.na(Return_Index_Diff))

    Indices_data_Index <-
      create_PCA_Asset_Index(
        asset_data_to_use = Indices_log_cumulative,
        asset_to_use = c("SPX500_USD", "EU50_EUR", "US2000_USD", "AU200_AUD", "XAG_USD", "XAU_USD"),
        price_col = "Return_Index_Diff"
      ) %>%
      rename(
        Average_PCA_Index = Average_PCA
      )

    Indices_data_pca <-
      Indices_log_cumulative %>%
      left_join(Indices_data_Index)

    correlation_PCA_dat <-
      c("SPX500_USD", "EU50_EUR", "US2000_USD", "AU200_AUD", "XAG_USD", "XAU_USD") %>%
      map_dfr(
        ~
          get_PCA_Index_rolling_cor_sd_mean(
            raw_asset_data_for_PCA_cor = Indices_log_cumulative %>% filter(Asset == .x),
            PCA_data = Indices_data_pca,
            rolling_period = rolling_period_index_PCA_cor
          )
      )

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

    PC_macro_vars <-
      c("AUD_PC1", "AUD_PC2",
        "NZD_PC1", "NZD_PC2",
        "USD_PC1", "USD_PC2",
        "CNY_PC1", "CNY_PC2",
        "EUR_PC1", "EUR_PC2")

    copula_data_SPX500_US2000 <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("SPX500_USD", "US2000_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      )

    copula_data_SPX500_XAU <-
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
      dplyr::select(-US2000_USD, -US2000_USD_log1_price,
                    -US2000_USD_quantiles_1, -US2000_USD_tangent_angle1) %>%
      dplyr::select(-XAU_USD, -XAU_USD_log2_price,
                    -XAU_USD_quantiles_2, -XAU_USD_tangent_angle2)

    copula_data_SPX500_XAG <-
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
      dplyr::select(-US2000_USD, -US2000_USD_log1_price, -US2000_USD_quantiles_1, -US2000_USD_tangent_angle1) %>%
      dplyr::select(-XAG_USD, -XAG_USD_log2_price,
                    -XAG_USD_quantiles_2, -XAG_USD_tangent_angle2)

    copula_data_SPX500_EU50 <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("SPX500_USD", "EU50_EUR"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-SPX500_USD, -SPX500_USD_log1_price, -SPX500_USD_quantiles_1, -SPX500_USD_tangent_angle1)

    copula_data_US2000_EU50 <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("US2000_USD", "EU50_EUR"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-US2000_USD, -US2000_USD_log1_price, -US2000_USD_quantiles_1, -US2000_USD_tangent_angle1)%>%
      dplyr::select(-EU50_EUR, -EU50_EUR_log2_price,
                    -EU50_EUR_quantiles_2, -EU50_EUR_tangent_angle2)

    copula_data_XAU_EU50 <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("XAU_USD", "EU50_EUR"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAU_USD, -XAU_USD_log1_price, -XAU_USD_quantiles_1, -XAU_USD_tangent_angle1)%>%
      dplyr::select(-EU50_EUR, -EU50_EUR_log2_price,
                    -EU50_EUR_quantiles_2, -EU50_EUR_tangent_angle2)

    copula_data_XAG_EU50 <-
      estimating_dual_copula(
        asset_data_to_use = SPX_US2000_XAG,
        asset_to_use = c("XAG_USD", "EU50_EUR"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAG_USD, -XAG_USD_log1_price, -XAG_USD_quantiles_1, -XAG_USD_tangent_angle1)%>%
      dplyr::select(-EU50_EUR, -EU50_EUR_log2_price,
                    -EU50_EUR_quantiles_2, -EU50_EUR_tangent_angle2)


    binary_data_for_post_model <-
      actual_wins_losses %>%
      # rename(Date = dates, Asset = asset) %>%
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
      correlation_PCA_dat %>%
      dplyr::select(Date,Asset, Price, High, Low, Open, PC1, PC2, PC3, PC4, PC4, PC5,
                    Return_Index,
                    contains("rolling_cor_PC")) %>%
      filter(!is.na(PC1)) %>%
      group_by(Asset) %>%
      arrange(Date, .by_group = TRUE) %>%
      group_by(Asset) %>%
      mutate(across(where(is.numeric), ~ lag(.) )) %>%
      ungroup() %>%
      left_join(copula_data_SPX500_US2000) %>%
      left_join(copula_data_SPX500_XAU) %>%
      left_join(copula_data_US2000_XAU) %>%
      left_join(copula_data_SPX500_XAG) %>%
      left_join(copula_data_US2000_XAG) %>%
      left_join(copula_data_SPX500_EU50) %>%
      left_join(copula_data_US2000_EU50) %>%
      left_join(copula_data_XAU_EU50) %>%
      left_join(copula_data_XAG_EU50) %>%
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
                    "lagged_var_21_ma", "PC1", "PC2" ,"PC3", "PC4", "PC5"
                    # "rolling_cor_PC1", "rolling_cor_PC2", "rolling_cor_PC3",
                    # "rolling_cor_PC4", "rolling_cor_PC4", "rolling_cor_PC5",
                    # "rolling_cor_PC1_mean", "rolling_cor_PC2_mean", "rolling_cor_PC3_mean",
                    # "rolling_cor_PC4_mean", "rolling_cor_PC4_mean", "rolling_cor_PC5_mean"
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
                    "lagged_var_21_ma", "PC1", "PC2" ,"PC3", "PC4", "PC5"
                    # "rolling_cor_PC1_mean", "rolling_cor_PC2_mean", "rolling_cor_PC3_mean",
                    # "rolling_cor_PC4_mean", "rolling_cor_PC4_mean", "rolling_cor_PC5_mean"
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
