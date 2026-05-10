#' single_asset_algo_generate_preds
#'
#' @param All_Daily_Data
#' @param Indices_Metals_Bonds
#' @param raw_macro_data
#' @param currency_conversion
#' @param asset_infor
#' @param start_index
#' @param end_index
#' @param risk_dollar_value
#' @param trade_direction
#' @param stop_value_var
#' @param profit_value_var
#' @param period_var
#' @param bin_var_col
#' @param date_train_end_pre
#' @param date_train_phase_2_end_pre
#' @param training_date_start_post
#' @param training_date_end_post
#' @param model_data_store_path
#' @param save_path
#'
#' @returns
#' @export
#'
#' @examples
single_asset_algo_generate_preds_STRPED_SPEED <-
  function(
    All_Daily_Data = All_Daily_Data,
    Indices_Metals_Bonds = Indices_Metals_Bonds,
    raw_macro_data = raw_macro_data,
    currency_conversion = currency_conversion,
    asset_infor = asset_infor,
    start_index = 1,
    end_index = 40,
    risk_dollar_value = 15,
    trade_direction = "Long",
    stop_value_var = 5,
    profit_value_var = 30,
    period_var = 24,
    bin_var_col = c("period_return_20_Price", "period_return_24_Price", "period_return_28_Price"),
    date_train_end_pre = "2023-06-01",
    date_train_phase_2_end_pre = "2024-06-01",
    training_date_start_post = "2024-07-04",
    training_date_end_post = "2025-09-01",
    test_end_date = as.character(today()),
    post_bins_cols =
      c("period_return_24_Price",
        "period_return_30_Price",
        "period_return_44_Price"),
    post_dependant_threshold = 5,
    post_dependant_var = "period_return_24_Price",
    model_data_store_path = "C:/Users/nikhi/Documents/trade_data/Day_Trader_Single_Asset_V2_trade_store_stop_2.db",
    save_path = "C:/Users/nikhi/Documents/trade_data/Day_Trader_Single_Asset_V2_trade_store_stop_2/"
  ) {

    equity_index <-
      get_equity_index(index_data = Indices_Metals_Bonds[[1]])

    gold_index <-
      get_Gold_index(index_data = Indices_Metals_Bonds[[1]])

    silver_index <-
      get_silver_index(index_data = Indices_Metals_Bonds[[1]])

    bonds_index <-
      get_bonds_index(index_data = Indices_Metals_Bonds[[1]])

    USD_index <-
      get_USD_index_for_models(index_data = Indices_Metals_Bonds[[1]])

    EUR_index <-
      get_EUR_index_for_models(index_data = Indices_Metals_Bonds[[1]])

    GBP_index <-
      get_GBP_index_for_models(index_data = Indices_Metals_Bonds[[1]])

    AUD_index <-
      get_AUD_index_for_models(index_data = Indices_Metals_Bonds[[1]])

    COMMOD_index <-
      get_COMMOD_index_for_models(index_data = Indices_Metals_Bonds[[1]])

    USD_STOCKS_index <-
      get_USD_AND_STOCKS_index_for_models(index_data = Indices_Metals_Bonds[[1]])

    NZD_index <-
      get_NZD_index_for_models(index_data = Indices_Metals_Bonds[[1]])

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
        end_date = today(tz = "Australia/Canberra") %>% as.character(),
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
                "XAU_USD",#19
                "HK33_HKD", #20
                "NATGAS_USD" #21
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

          # "XAU_USD", #19
          c("XAG_JPY", "XAG_GBP", "XAG_EUR", "XAG_AUD", "XAG_USD", "EU50_EUR", "SPX500_USD",
            "XAG_NZD", "XAU_AUD", "XAU_GBP", "XAU_JPY", "XAU_EUR", "EUR_USD", "USD_JPY",
            "GBP_USD", "AUD_USD", "USD_CAD", "USD_SEK") %>% unique(), #19

          # "HK33_HKD", #20
          c("US2000_USD", "AU200_AUD", "USB10Y_USD", "UK100_GBP", "XAU_USD", "EU50_EUR",
            "SPX500_USD", "FR40_EUR", "WTICO_USD", "USD_JPY", "EUR_USD", "GBP_USD", "AU200_AUD",
            "SG30_SGD", "XAU_EUR", "XAG_EUR", "XAG_GBP", "XAU_GBP", "XAG_USD") %>% unique(), #20

          # "NATGAS_USD" #21
          c("XAG_JPY", "XAG_GBP", "XAG_EUR", "XAG_AUD", "XAU_USD", "EU50_EUR", "SPX500_USD",
            "XAG_NZD", "XAU_AUD", "XAU_GBP", "XAU_JPY", "XAU_EUR", "EUR_USD", "USD_JPY",
            "GBP_USD", "AUD_USD", "USD_CAD", "WTICO_USD", "XAG_USD") %>% unique(), #21
        ),
      countries_for_int_strength =
        list(
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #1
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #2
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #3
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #4
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #5
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #6
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #7
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #8
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #9
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #10
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #11
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #12
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #13
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #14
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #15
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #16

          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #17
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #18
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #19
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #20
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD") #21
        )
    )
    assets_to_analyse <-
      indicator_mapping$Asset

    all_pred_data <- list()

    date_test_start = as.character(as_date(date_train_phase_2_end_pre) + days(3))

    for (j in start_index:end_index ) {

      tictoc::tic()

      countries_for_int_strength <-
        unlist(indicator_mapping$countries_for_int_strength[j])
      couplua_assets = unlist(indicator_mapping$couplua_assets[j])
      Asset_of_interest = unlist(indicator_mapping$Asset[j])

      message(Asset_of_interest)

      long_sim <-
        single_asset_Logit_indicator_adv_get_preds(
          asset_data = Indices_Metals_Bonds[[1]],
          All_Daily_Data = All_Daily_Data,
          Asset_of_interest = Asset_of_interest,
          actual_wins_losses = NULL,

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

          date_train_end = date_train_end_pre,
          date_train_phase_2_end = date_train_phase_2_end_pre,
          date_test_start = as.character(date_test_start),

          couplua_assets = couplua_assets,

          stop_value_var = stop_value_var,
          profit_value_var = profit_value_var,
          period_var = period_var,

          bin_var_col = bin_var_col,
          trade_direction = trade_direction,
          save_path = save_path
        )

      long_sim_transformed <-
        long_sim %>%
        filter(Date >= date_test_start) %>%
        mutate(
          trade_col = trade_direction,
          test_end_date = test_end_date,
          date_train_end = date_train_end_pre,
          date_train_phase_2_end = date_train_phase_2_end_pre,
          date_test_start = date_test_start,
          sim_index = 1,
          bin_var_col = paste(bin_var_col, collapse = ", ")
        )

      complete_sim <-
        list(long_sim_transformed) %>%
        map_dfr(bind_rows)

      all_pred_data[[j]] <- complete_sim

      rm(complete_sim, long_sim_transformed, long_sim)
      gc()

      tictoc::toc()

    }

    all_pred_data <-
      all_pred_data %>%
      map_dfr(bind_rows)


    post_preds_all <-
      read_post_models_and_get_preds(
        indicator_data = all_pred_data,
        post_model_data_save_path =save_path,
        test_date_start = training_date_start_post,
        test_date_end = as.character(today() + days(100)) ,
        dependant_var = post_dependant_var,
        dependant_threshold = post_dependant_threshold,
        ignore_dependant_var = TRUE
      )


    post_preds_all_rolling <-
      get_rolling_post_preds(
        post_pred_data = post_preds_all,
        rolling_periods = c(3,50,100,200,400,500,2000),
        test_date_start = training_date_end_post,
        test_date_end = as.character(today() + days(100)),
        pred_price_cols = post_bins_cols
      )

    post_preds_all_rolling_and_originals <-
      post_preds_all_rolling %>%
      left_join(
        all_pred_data %>%
          dplyr::select(Date, Asset, contains("pred_combined"),
                        contains("pred_macro"), contains("pred_index"),
                        contains("pred_daily"), contains("pred_copula"),
                        contains("pred_technical")) %>%
          distinct()
      )

    rm(all_pred_data, post_preds_all_rolling, post_preds_all)
    gc()

    return(post_preds_all_rolling_and_originals)

  }
