#' Single_Asset_V3_get_all_data
#'
#' @param Indices_Metals_Bonds
#' @param asset_of_interest
#' @param copula_assets
#' @param raw_macro_data
#' @param correlation_rolling_periods
#' @param state_space_periods
#' @param state_space_rolling
#' @param loop_list_cols
#' @param state_space_periods
#' @param state_space_rolling
#'
#' @returns
#' @export
#'
#' @examples
Single_Asset_V3_get_all_data_for_model_Exc_copula <-
  function(
    Indices_Metals_Bonds = Indices_Metals_Bonds,
    asset_of_interest = asset_of_interest,
    copula_assets = copula_assets,
    raw_macro_data = raw_macro_data,
    correlation_rolling_periods = c(100,200, 300,400, 500),
    state_space_periods = c(20, 40, 60, 100, 200,300, 400,  500),
    state_space_rolling = c(100, 200, 300, 400),
    loop_list_cols = c("Price", "Low", "High")
  ) {

    asset_data = Indices_Metals_Bonds[[1]] %>% filter(Asset == asset_of_interest)

    AR_model_data <-
      Single_Asset_V3_AR_Model_data(
        asset_data = asset_data,
        asset_of_interest = asset_of_interest,
        lag_value_1 = 10,
        lag_value_2 = 20,
        lag_value_3 = 30,
        lag_value_4 = 40,
        lag_value_5 = 50,
        lag_value_6 = 60,
        lag_value_7 = 70,
        lag_value_8 = 100,
        MA_period_1 = 10,
        MA_period_2 = 20,
        MA_period_3 = 30,
        MA_period_4 = 40,
        MA_period_5 = 20,
        MA_period_6 = 20,
        MA_period_7 = 55,
        MA_period_8 = 80
      )

    loop_list_cols <- c("Price", "Low", "High")
    state_space_list <- list()
    c = 0
    for (j in 1:length(loop_list_cols) ) {
      for (i in 1:length(state_space_periods)) {
        for (k in 1:length(state_space_rolling)) {
          c = c + 1
          state_space_list[[c]] <-
            Single_Asset_V3_state_space(
              asset_data = asset_data,
              asset_of_interest = asset_of_interest,
              Price_diff_lag = state_space_periods[i],
              roll_period_state_space = state_space_rolling[k],
              price_col = loop_list_cols[j]
            )
        }
      }
    }

    state_space_data <-
      state_space_list %>%
      reduce(left_join)

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

    gdp_data <-
      get_GDP_countries(
        raw_macro_data = raw_macro_data,
        lag_days = 1
      )

    unemp_data <-
      get_unemp_countries(
        raw_macro_data = raw_macro_data,
        lag_days = 1
      )

    manufac_pmi <-
      get_manufac_countries(
        raw_macro_data = raw_macro_data,
        lag_days = 1
      )

    USD_Macro <-
      get_additional_USD_Macro(
        raw_macro_data = raw_macro_data,
        lag_days = 1
      )

    EUR_Macro <-
      get_additional_EUR_Macro(
        raw_macro_data = raw_macro_data,
        lag_days = 1
      )

    macro_model_data <-
      prepare_macro_indicator_model_data(
        asset_data = asset_data,
        raw_macro_data = raw_macro_data,
        Asset_of_interest = asset_of_interest,
        interest_rates = interest_rates,
        cpi_data = cpi_data,
        gdp_data = gdp_data,
        unemp_data = unemp_data,
        manufac_pmi = manufac_pmi,
        USD_Macro = USD_Macro,
        EUR_Macro = EUR_Macro,
        sentiment_index = sentiment_index,
        countries_for_int_strength = c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"),
        date_limit = as.character(today() + days(1))
      ) %>%
      mutate(
        Asset = asset_of_interest
      )

    return(
      list(
        "AR_model_data" = AR_model_data,
        "state_space_data" = state_space_data,
        "macro_model_data" = macro_model_data
      )
    )

  }


#' Single_Asset_V3_Read_in_Probs_with_Macro
#'
#' @param Indices_Metals_Bonds
#' @param asset_of_interest
#' @param actuals_periods_needed
#' @param training_end_date
#' @param rolling_mean_pred_period
#' @param correlation_rolling_periods
#' @param state_space_periods
#' @param state_space_rolling
#' @param copula_assets
#' @param raw_macro_data
#' @param base_path
#'
#' @return
#' @export
#'
#' @examples
Single_Asset_V3_Read_in_Probs_Exclude_Copula <-
  function(Indices_Metals_Bonds,
           asset_of_interest = "GBP_JPY",
           actuals_periods_needed = c("period_return_24_Price", "period_return_35_Price", "period_return_46_Price"),
           training_end_date = "2025-05-01",
           rolling_mean_pred_period = 500,
           correlation_rolling_periods = c(100,200, 300,400, 500),
           state_space_periods = c(20, 40, 60, 100, 200,300, 400,  500),
           state_space_rolling = c(100, 200, 300, 400),
           copula_assets = c("GBP_USD", "EUR_JPY", "USD_JPY", "XAU_JPY", "GBP_CHF", "XAG_GBP", "GBP_NZD", "UK100_GBP", "EUR_USD", "GBP_AUD"),
           raw_macro_data = raw_macro_data,
           base_path = "C:/Users/Nikhil Chandra/Documents/trade_data/single_asset_models_v3/" ) {

    asset_data = Indices_Metals_Bonds[[1]] %>% filter(Asset == asset_of_interest)

    all_data_list <-
      Single_Asset_V3_get_all_data_for_model_Exc_copula(
        Indices_Metals_Bonds = Indices_Metals_Bonds,
        asset_of_interest = asset_of_interest,
        copula_assets = copula_assets,
        raw_macro_data = raw_macro_data,
        correlation_rolling_periods = correlation_rolling_periods,
        state_space_periods = state_space_periods,
        state_space_rolling = state_space_rolling,
        loop_list_cols = c("Price", "Low", "High")
      )

    AR_model_data <-
      all_data_list$AR_model_data

    state_space_data <-
      all_data_list$state_space_data

    macro_model_data <-
      all_data_list$macro_model_data

    rm(all_data_list)
    gc()

    AR_preds_list <- list()

    for (i in 1:length(actuals_periods_needed)) {
      AR_preds_list[[i]] <-
        Single_Asset_V3_AR_read_model(
          AR_model_data = AR_model_data,
          asset_of_interest = asset_of_interest,
          period_of_analysis = actuals_periods_needed[i],
          training_end_date = training_end_date,
          roll_mean_period = rolling_mean_pred_period,
          base_path = base_path
        )
    }

    AR_Train_Preds_mean <-
      AR_preds_list %>%
      map(~.x %>% pluck("training_data")) %>%
      reduce(left_join)

    AR_Test_Preds <-
      AR_preds_list %>%
      map(~.x %>% pluck("testing_data")) %>%
      reduce(left_join)

    rm(AR_preds_list)

    state_space_preds_list <- list()

    for (i in 1:length(actuals_periods_needed)) {
      state_space_preds_list[[i]] <-
        Single_Asset_V3_state_space_read_Model(
          state_space_data = state_space_data,
          asset_of_interest = asset_of_interest,
          period_of_analysis = actuals_periods_needed[i],
          training_end_date = training_end_date,
          roll_mean_period = rolling_mean_pred_period,
          base_path = base_path
        )
    }

    state_space_Train_Preds_mean <-
      state_space_preds_list %>%
      map(~.x %>% pluck("training_data")) %>%
      reduce(left_join)

    state_space_Test_Preds <-
      state_space_preds_list %>%
      map(~.x %>% pluck("testing_data")) %>%
      reduce(left_join)

    Macro_preds_list <- list()

    for (i in 1:length(actuals_periods_needed)) {
      Macro_preds_list[[i]] <-
        Single_Asset_V3_macro_read_Model(
          macro_model_data = macro_model_data,
          asset_of_interest = asset_of_interest,
          period_of_analysis = actuals_periods_needed[i],
          training_end_date = training_end_date,
          roll_mean_period = rolling_mean_pred_period,
          base_path = base_path
        )
    }

    Macro_Train_Preds_mean <-
      Macro_preds_list %>%
      map(~.x %>% pluck("training_data")) %>%
      reduce(left_join)

    Macro_Test_Preds <-
      Macro_preds_list %>%
      map(~.x %>% pluck("testing_data")) %>%
      reduce(left_join)

    rm(Macro_preds_list)
    gc()


    complete_preds_train <-
      AR_Train_Preds_mean %>%
      left_join(
        state_space_Train_Preds_mean
      )%>%
      left_join(
        Macro_Train_Preds_mean
      )

    first_non_NA_date <-
      complete_preds_train %>%
      filter(if_all(everything(), ~!is.na(.))) %>%
      pull(Date) %>%
      min(na.rm = T)

    complete_preds_train <-
      complete_preds_train %>%
      filter(Date >= first_non_NA_date)

    complete_preds_test <-
      AR_Test_Preds %>%
      left_join(
        state_space_Test_Preds
      )%>%
      left_join(
        Macro_Test_Preds
      )

    return(
      list(
        "complete_preds_test" = complete_preds_test,
        "complete_preds_train" = complete_preds_train
      )
    )


  }


#' Single_Asset_V3_get_all_preds
#'
#' @param Indices_Metals_Bonds
#' @param base_path
#' @param actuals_periods_needed
#' @param correlation_rolling_periods
#' @param state_space_periods
#' @param state_space_rolling
#' @param date_for_true_simualtion
#' @param training_end_date
#' @param asset_index_start
#' @param asset_index_end
#'
#' @returns
#' @export
#'
#' @examples
Single_Asset_V3_get_all_preds <-
  function(
    Indices_Metals_Bonds = Indices_Metals_Bonds,
    raw_macro_data = raw_macro_data,
    base_path = "C:/Users/nikhi/Documents/trade_data/Day_Trader_Single_Asset_V3_Expanded_Models/",
    actuals_periods_needed = c("period_return_50_Price"),
    correlation_rolling_periods = c(100,200, 300,400, 500),
    state_space_periods = c(20, 40, 60, 100, 200,300, 400,  500),
    state_space_rolling = c(100, 200, 300, 400),
    date_for_true_simualtion = "2019-01-01",
    training_end_date = "2021-01-01",
    asset_index_start = 1,
    asset_index_end = 38
    ) {


    assets_to_test <- c("EUR_USD", #1
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
                        # "XAG_EUR", #19
                        # "XAG_AUD", #20
                        # "XAG_NZD", #21
                        "HK33_HKD", #22
                        "FR40_EUR", #23
                        "BTC_USD", #24
                        # "XAG_GBP", #25
                        "GBP_AUD", #26
                        "USD_SEK", #27
                        "USD_SGD", #28
                        "NZD_USD", #29
                        "GBP_NZD", #30
                        "XCU_USD", #31
                        "NATGAS_USD", #32
                        "GBP_JPY", #33
                        "SG30_SGD", #34
                        "XAU_USD", #35
                        "EUR_SEK", #36
                        # "XAU_AUD", #37
                        "UK10YB_GBP", #38
                        "JP225Y_JPY", #39
                        "ETH_USD" #40
    )

    correlation_asset_list <-
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

        # # "XAG_EUR", #19
        # c("XAG_JPY", "XAG_GBP", "XAG_USD", "XAG_AUD", "XAU_USD", "EU50_EUR", "SPX500_USD",
        #   "XAG_NZD", "XAU_AUD", "XAU_GBP", "XAU_JPY", "XAU_EUR", "EUR_USD", "EUR_JPY",
        #   "EUR_GBP", "EUR_AUD", "EUR_SEK", "EUR_NZD") %>% unique(), #19
        #
        # # "XAG_AUD", #20
        # c("XAG_JPY", "XAG_GBP", "XAG_USD", "XAG_EUR", "XAU_USD", "EU50_EUR", "SPX500_USD",
        #   "XAG_NZD", "XAU_AUD", "XAU_GBP", "XAU_JPY", "XAU_EUR", "AU200_AUD",
        #   "AUD_USD", "EUR_AUD", "GBP_AUD") %>% unique(), #20
        #
        # # "XAG_NZD", #21
        # c("XAG_JPY", "XAG_GBP", "XAG_USD", "XAG_EUR", "XAU_USD", "EU50_EUR", "SPX500_USD",
        #   "XAG_AUD", "XAU_AUD", "XAU_GBP", "XAU_JPY", "XAU_EUR", "AU200_AUD",
        #   "NZD_USD", "GBP_NZD", "EUR_NZD") %>% unique(), #21

        # "HK33_HKD", #22
        c("US2000_USD", "AU200_AUD", "USB10Y_USD", "UK100_GBP", "XAU_USD", "EU50_EUR",
          "SPX500_USD", "FR40_EUR", "WTICO_USD", "USD_JPY", "EUR_USD", "GBP_USD", "AU200_AUD",
          "SG30_SGD", "XAU_EUR", "XAG_EUR", "XAG_GBP", "XAU_GBP", "XAG_USD") %>% unique(), #22

        # "FR40_EUR" #23
        c("UK100_GBP", "EU50_EUR", "XAG_USD", "AU200_AUD",
          "XAU_USD", "USB10Y_USD", "SPX500_USD", "EUR_USD", "EUR_AUD",
          "XAU_EUR", "XAG_EUR", "EUR_NZD", "EUR_JPY") %>% unique(), #23

        # "BTC_USD", #24
        c("US2000_USD", "AU200_AUD", "USB10Y_USD", "UK100_GBP", "XAU_USD", "EU50_EUR",
          "HK33_HKD", "FR40_EUR", "WTICO_USD", "USD_JPY", "EUR_USD", "GBP_USD", "AU200_AUD",
          "SG30_SGD", "XAU_EUR", "XAG_EUR", "XAG_GBP", "XAU_GBP", "XAG_USD" ) %>% unique(), #24

        # # "XAG_GBP", #25
        # c("XAG_JPY", "XAG_NZD", "XAG_USD", "XAG_EUR", "XAU_USD", "EU50_EUR", "SPX500_USD",
        #   "XAG_AUD", "XAU_AUD", "XAU_GBP", "XAU_JPY", "XAU_EUR", "UK100_GBP",
        #   "GBP_USD", "GBP_NZD", "GBP_AUD") %>% unique(), #25

        # "GBP_AUD" #26
        c("GBP_JPY", "GBP_CAD", "GBP_USD", "GBP_NZD", "XAU_GBP", "XAG_GBP", "UK100_GBP",
          "XAU_AUD", "XAG_USD", "EUR_GBP", "EUR_USD", "XAG_AUD", "XAU_EUR", "AU200_AUD",
          "EUR_JPY", "UK10YB_GBP", "AUD_USD", "USD_SEK", "EUR_AUD") %>% unique(), #26

        # "USD_SEK" #27
        c("AUD_USD", "EUR_USD", "GBP_USD", "USD_JPY",
          "XAU_USD", "USD_CAD", "NZD_USD", "XAG_USD", "XAG_USD") %>% unique(), #27

        # "USD_SGD" #28
        c("AUD_USD", "EUR_USD", "GBP_USD", "USD_JPY",
          "XAU_USD", "USD_CAD", "NZD_USD", "XAG_USD", "WTICO_USD", "BCO_USD",
          "XCU_USD", "USD_SEK", "SPX500_USD", "EU50_EUR", "UK100_GBP",
          "NATGAS_USD") %>% unique(), #28,

        # "NZD_USD", #29
        c("XAG_JPY", "XAG_GBP", "XAG_USD", "XAG_EUR", "GBP_USD", "EUR_USD", "AUD_USD",
          "XAG_AUD", "XAU_AUD", "USD_CAD", "USD_JPY", "XAU_EUR", "AU200_AUD",
          "GBP_NZD", "EUR_NZD") %>% unique(), #29

        # "GBP_NZD", #30
        c("GBP_JPY", "GBP_CAD", "GBP_USD", "XAU_GBP", "XAG_GBP", "UK100_GBP",
          "GBP_JPY", "XAG_USD", "EUR_GBP", "NZD_USD", "EUR_NZD", "AUD_USD", "XAG_NZD",
          "AUD_USD", "UK10YB_GBP") %>% unique(), #30

        # "XCU_USD", #31
        c("XAG_JPY", "XAG_GBP", "XAG_EUR", "XAG_AUD", "XAU_USD", "EU50_EUR", "SPX500_USD",
          "XAG_NZD", "XAU_AUD", "XAU_GBP", "XAU_JPY", "XAU_EUR", "EUR_USD", "USD_JPY",
          "GBP_USD", "AUD_USD", "USD_CAD", "USD_SEK", "XAG_USD") %>% unique(), #31

        # "NATGAS_USD" #32
        c("XAG_JPY", "XAG_GBP", "XAG_EUR", "XAG_AUD", "XAU_USD", "EU50_EUR", "SPX500_USD",
          "XAG_NZD", "XAU_AUD", "XAU_GBP", "XAU_JPY", "XAU_EUR", "EUR_USD", "USD_JPY",
          "GBP_USD", "AUD_USD", "USD_CAD", "WTICO_USD", "XAG_USD") %>% unique(), #32

        # "GBP_JPY" #33
        c("GBP_CAD", "GBP_USD", "XAU_GBP", "XAG_GBP", "UK100_GBP",
          "GBP_NZD", "XAG_USD", "EUR_GBP", "EUR_JPY", "XAU_JPY", "USD_JPY", "XAG_JPY",
          "AUD_USD", "UK10YB_GBP") %>% unique(), #33

        # "SG30_SGD" #34
        c("AUD_USD", "EUR_USD", "GBP_USD", "USD_JPY",
          "XAU_USD", "US2000_USD", "NZD_USD", "XAG_USD", "WTICO_USD", "BCO_USD",
          "XCU_USD", "HK33_HKD", "SPX500_USD", "EU50_EUR", "UK100_GBP",
          "NATGAS_USD"), #34

        # "XAU_USD", #35
        c("XAG_JPY", "XAG_GBP", "XAG_EUR", "XAG_AUD", "XAG_USD", "EU50_EUR", "SPX500_USD",
          "XAG_NZD", "XAU_AUD", "XAU_GBP", "XAU_JPY", "XAU_EUR", "EUR_USD", "USD_JPY",
          "GBP_USD", "AUD_USD", "USD_CAD", "USD_SEK") %>% unique(), #35

        # "EUR_SEK", #36
        c("GBP_USD", "EUR_USD", "XAU_EUR", "USD_SEK", "EUR_AUD",
          "EUR_GBP", "EUR_NZD", "EUR_JPY", "XAG_EUR", "XAU_USD", "XAG_USD",
          "GBP_JPY", "FR40_EUR", "EU50_EUR") %>% unique(), #36

        # # "XAU_AUD", #37
        # c("XAG_JPY", "XAG_GBP", "XAG_EUR", "XAG_AUD", "XAG_USD", "EU50_EUR", "SPX500_USD",
        #   "XAG_NZD", "XAU_USD", "XAU_GBP", "XAU_JPY", "XAU_EUR", "AU200_AUD", "USD_JPY",
        #   "GBP_AUD", "AUD_USD", "EUR_AUD", "AUD_USD") %>% unique(), #37

        # "UK10YB_GBP", #38
        c("XAU_GBP", "XAG_GBP", "XAU_USD", "EUR_GBP", "XAU_EUR", "GBP_AUD", "GBP_NZD",
          "SPX500_USD", "BCO_USD", "UK100_GBP", "USB10Y_USD", "GBP_CAD", "GBP_JPY",
          "XAG_GBP", "WTICO_USD", "GBP_USD") %>% unique(), #38

        # "JP225Y_JPY" #39
        c("US2000_USD", "AU200_AUD", "USB10Y_USD", "UK100_GBP", "XAU_USD", "EU50_EUR",
          "SPX500_USD", "FR40_EUR", "WTICO_USD", "USD_JPY", "EUR_USD", "AU200_AUD",
          "SG30_SGD", "XAU_EUR", "XAG_JPY", "XAG_GBP", "XAU_JPY", "XAG_USD") %>% unique(), #39

        # "ETH_USD" #40
        c("US2000_USD", "AU200_AUD", "USB10Y_USD", "UK100_GBP", "XAU_USD", "EU50_EUR",
          "HK33_HKD", "FR40_EUR", "WTICO_USD", "USD_JPY", "EUR_USD", "GBP_USD", "AU200_AUD",
          "BTC_USD", "XAU_EUR", "XAG_EUR", "XAG_GBP", "XAU_GBP", "XAG_USD" ) %>% unique()
      )

    safely_get_probs <-
      safely(Single_Asset_V3_Read_in_Probs_Exclude_Copula, otherwise = NULL)

    all_probs <- list()
    c = 0

    for (j in asset_index_start:asset_index_end ) {

      tictoc::tic()
      asset_of_interest <- assets_to_test[j]
      correlation_assets_current <- correlation_asset_list[[j]]

      simulated_probs <-
        safely_get_probs(
          Indices_Metals_Bonds =
            Indices_Metals_Bonds %>%
            map(~ .x %>% filter(Date >= date_for_true_simualtion)),
          asset_of_interest = asset_of_interest,
          actuals_periods_needed = actuals_periods_needed,
          training_end_date = training_end_date,
          rolling_mean_pred_period = 500,
          correlation_rolling_periods = correlation_rolling_periods,
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

    return(all_probs)

  }
