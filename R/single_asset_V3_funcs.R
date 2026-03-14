#' Single_Asset_V3_get_all_preds
#'
#' @param Indices_Metals_Bonds
#' @param asset_of_interest
#' @param actuals_periods_needed
#' @param training_end_date
#' @param bin_threshold
#' @param rolling_mean_pred_period
#' @param correlation_rolling_periods
#' @param copula_assets
#' @param training_end_date
#' @param rolling_mean_pred_period
#' @param bin_threshold
#' @param start_index
#' @param end_index
#'
#' @return
#' @export
#'
#' @examples
Single_Asset_V3_Gen_all_models <-
  function(
    Indices_Metals_Bonds = Indices_Metals_Bonds,
    actuals_periods_needed = c("period_return_35_Price", "period_return_46_Price"),
    correlation_rolling_periods = c(100,200, 300),
    training_end_date = "2025-05-01",
    rolling_mean_pred_period = 500,
    bin_threshold = 5,
    start_index = 1,
    end_index = 27,
    base_path = "C:/Users/Nikhil Chandra/Documents/trade_data/single_asset_models_v3/"
  ) {

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
                "USD_SGD", #28
                "NZD_USD", #29
                "GBP_NZD", #30
                "XCU_USD", #31
                "NATGAS_USD", #32
                "GBP_JPY", #33
                "SG30_SGD", #34
                "XAU_USD", #35
                "EUR_SEK", #36
                "XAU_AUD", #37
                "UK10YB_GBP", #38
                "JP225Y_JPY", #39
                "ETH_USD" #40
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

          # "FR40_EUR" #23
          c("UK100_GBP", "EU50_EUR", "XAG_USD", "AU200_AUD",
            "XAU_USD", "USB10Y_USD", "SPX500_USD", "EUR_USD", "EUR_AUD",
            "XAU_EUR", "XAG_EUR", "EUR_NZD", "EUR_JPY") %>% unique(), #23

          # "BTC_USD", #24
          c("US2000_USD", "AU200_AUD", "USB10Y_USD", "UK100_GBP", "XAU_USD", "EU50_EUR",
            "HK33_HKD", "FR40_EUR", "WTICO_USD", "USD_JPY", "EUR_USD", "GBP_USD", "AU200_AUD",
            "SG30_SGD", "XAU_EUR", "XAG_EUR", "XAG_GBP", "XAU_GBP", "XAG_USD" ) %>% unique(), #24

          # "XAG_GBP", #25
          c("XAG_JPY", "XAG_NZD", "XAG_USD", "XAG_EUR", "XAU_USD", "EU50_EUR", "SPX500_USD",
            "XAG_AUD", "XAU_AUD", "XAU_GBP", "XAU_JPY", "XAU_EUR", "UK100_GBP",
            "GBP_USD", "GBP_NZD", "GBP_AUD") %>% unique(), #25

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

          # "XAU_AUD", #37
          c("XAG_JPY", "XAG_GBP", "XAG_EUR", "XAG_AUD", "XAG_USD", "EU50_EUR", "SPX500_USD",
            "XAG_NZD", "XAU_USD", "XAU_GBP", "XAU_JPY", "XAU_EUR", "AU200_AUD", "USD_JPY",
            "GBP_AUD", "AUD_USD", "EUR_AUD", "AUD_USD") %>% unique(), #37

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
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #21
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #22
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #23
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #24
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #25
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #26
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #27
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #28
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #29
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #30
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #31
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #32
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #33
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #34
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #35
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #36
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #37
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #38
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #39
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD") #40
        )
    )

    all_assets <-
      indicator_mapping$Asset

    raw_base_preds <-
      list()

    for (i in start_index:end_index ) {
      tictoc::tic()
      asset_loop <- indicator_mapping$Asset[i]
      copula_assets <- indicator_mapping$couplua_assets[[i]]

      pred_generated <-
        Single_Asset_V3_Gen_Model(
          Indices_Metals_Bonds = Indices_Metals_Bonds,
          asset_of_interest = asset_loop,
          actuals_periods_needed = actuals_periods_needed,
          training_end_date = training_end_date,
          bin_threshold = bin_threshold,
          rolling_mean_pred_period = rolling_mean_pred_period,
          correlation_rolling_periods = correlation_rolling_periods,
          copula_assets = copula_assets,
          base_path = base_path
        )

      raw_base_preds[[i]] <-
        pred_generated %>%
        pluck("complete_preds_test") %>%
        mutate(
          training_end_date = training_end_date,
          rolling_mean_pred_period = rolling_mean_pred_period,
          bin_threshold = bin_threshold
        )
      tictoc::toc()
    }


    returned <-
      raw_base_preds %>%
      map_dfr(bind_rows) %>%
      ungroup() %>%
      mutate(
        averaged_35_LM_pred =
          (state_space_LM_Pred_period_return_35_Price +
             AR_LM_Pred_period_return_35_Price +
             Copula_LM_Pred_period_return_35_Price)/3,

        averaged_35_GLM_pred =
          (state_space_GLM_Pred_period_return_35_Price +
             AR_GLM_Pred_period_return_35_Price +
             Copula_GLM_Pred_period_return_35_Price)/3,

        averaged_35_46_GLM_pred =
          (state_space_GLM_Pred_period_return_35_Price +
             AR_GLM_Pred_period_return_35_Price +
             Copula_GLM_Pred_period_return_35_Price +
             state_space_GLM_Pred_period_return_46_Price +
             AR_GLM_Pred_period_return_46_Price +
             Copula_GLM_Pred_period_return_46_Price)/6,

        averaged_35_46_LM_pred =
          (state_space_LM_Pred_period_return_35_Price +
             AR_LM_Pred_period_return_35_Price +
             Copula_LM_Pred_period_return_35_Price +
             state_space_LM_Pred_period_return_46_Price +
             AR_LM_Pred_period_return_46_Price +
             Copula_LM_Pred_period_return_46_Price)/6
      )

    return(returned)

  }


#' Single_Asset_V3_get_all_preds
#'
#' @param Indices_Metals_Bonds
#' @param asset_of_interest
#' @param actuals_periods_needed
#' @param training_end_date
#' @param bin_threshold
#' @param rolling_mean_pred_period
#' @param correlation_rolling_periods
#' @param copula_assets
#' @param training_end_date
#' @param rolling_mean_pred_period
#' @param bin_threshold
#' @param start_index
#' @param end_index
#'
#' @return
#' @export
#'
#' @examples
Single_Asset_V3_get_all_preds <-
  function(
    Indices_Metals_Bonds = Indices_Metals_Bonds,
    actuals_periods_needed = c("period_return_35_Price", "period_return_46_Price"),
    correlation_rolling_periods = c(100,200, 300),
    training_end_date = "2025-05-01",
    rolling_mean_pred_period = 500,
    bin_threshold = 5,
    start_index = 1,
    end_index = 27,
    base_path = "C:/Users/Nikhil Chandra/Documents/trade_data/single_asset_models_v3/"
  ) {

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
                "USD_SGD", #28
                "NZD_USD", #29
                "GBP_NZD", #30
                "XCU_USD", #31
                "NATGAS_USD", #32
                "GBP_JPY", #33
                "SG30_SGD", #34
                "XAU_USD", #35
                "EUR_SEK", #36
                "XAU_AUD", #37
                "UK10YB_GBP", #38
                "JP225Y_JPY", #39
                "ETH_USD" #40
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

          # "FR40_EUR" #23
          c("UK100_GBP", "EU50_EUR", "XAG_USD", "AU200_AUD",
            "XAU_USD", "USB10Y_USD", "SPX500_USD", "EUR_USD", "EUR_AUD",
            "XAU_EUR", "XAG_EUR", "EUR_NZD", "EUR_JPY") %>% unique(), #23

          # "BTC_USD", #24
          c("US2000_USD", "AU200_AUD", "USB10Y_USD", "UK100_GBP", "XAU_USD", "EU50_EUR",
            "HK33_HKD", "FR40_EUR", "WTICO_USD", "USD_JPY", "EUR_USD", "GBP_USD", "AU200_AUD",
            "SG30_SGD", "XAU_EUR", "XAG_EUR", "XAG_GBP", "XAU_GBP", "XAG_USD" ) %>% unique(), #24

          # "XAG_GBP", #25
          c("XAG_JPY", "XAG_NZD", "XAG_USD", "XAG_EUR", "XAU_USD", "EU50_EUR", "SPX500_USD",
            "XAG_AUD", "XAU_AUD", "XAU_GBP", "XAU_JPY", "XAU_EUR", "UK100_GBP",
            "GBP_USD", "GBP_NZD", "GBP_AUD") %>% unique(), #25

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

          # "XAU_AUD", #37
          c("XAG_JPY", "XAG_GBP", "XAG_EUR", "XAG_AUD", "XAG_USD", "EU50_EUR", "SPX500_USD",
            "XAG_NZD", "XAU_USD", "XAU_GBP", "XAU_JPY", "XAU_EUR", "AU200_AUD", "USD_JPY",
            "GBP_AUD", "AUD_USD", "EUR_AUD", "AUD_USD") %>% unique(), #37

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
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #21
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #22
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #23
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #24
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #25
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #26
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #27
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #28
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #29
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #30
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #31
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #32
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #33
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #34
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #35
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #36
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #37
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #38
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #39
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD") #40
        )
    )

    all_assets <-
      indicator_mapping$Asset

    raw_base_preds <-
      list()

    for (i in start_index:end_index ) {
      tictoc::tic()
      asset_loop <- indicator_mapping$Asset[i]
      copula_assets <- indicator_mapping$couplua_assets[[i]]


      pred_generated <-
        Single_Asset_V3_Read_in_Probs(
          Indices_Metals_Bonds = Indices_Metals_Bonds,
          asset_of_interest = asset_loop,
          actuals_periods_needed = actuals_periods_needed,
          training_end_date = training_end_date,
          bin_threshold = bin_threshold,
          rolling_mean_pred_period = rolling_mean_pred_period,
          correlation_rolling_periods = correlation_rolling_periods,
          copula_assets = copula_assets,
          base_path = base_path
        )

      raw_base_preds[[i]] <-
        pred_generated %>%
        pluck("complete_preds_test") %>%
        mutate(
          training_end_date = training_end_date,
          rolling_mean_pred_period = rolling_mean_pred_period,
          bin_threshold = bin_threshold
        )
      tictoc::toc()
    }


    returned <-
      raw_base_preds %>%
      map_dfr(bind_rows) %>%
      ungroup() %>%
      mutate(
        averaged_35_LM_pred =
          (state_space_LM_Pred_period_return_35_Price +
             AR_LM_Pred_period_return_35_Price +
             Copula_LM_Pred_period_return_35_Price)/3,

        averaged_35_GLM_pred =
          (state_space_GLM_Pred_period_return_35_Price +
             AR_GLM_Pred_period_return_35_Price +
             Copula_GLM_Pred_period_return_35_Price)/3,

        averaged_35_46_GLM_pred =
          (state_space_GLM_Pred_period_return_35_Price +
             AR_GLM_Pred_period_return_35_Price +
             Copula_GLM_Pred_period_return_35_Price +
             state_space_GLM_Pred_period_return_46_Price +
             AR_GLM_Pred_period_return_46_Price +
             Copula_GLM_Pred_period_return_46_Price)/6,

        averaged_35_46_LM_pred =
          (state_space_LM_Pred_period_return_35_Price +
             AR_LM_Pred_period_return_35_Price +
             Copula_LM_Pred_period_return_35_Price +
             state_space_LM_Pred_period_return_46_Price +
             AR_LM_Pred_period_return_46_Price +
             Copula_LM_Pred_period_return_46_Price)/6
      )

    return(returned)

  }

#' Single_Asset_V3_Gen_Model
#'
#' @param Indices_Metals_Bonds
#' @param actual_wins_losses
#' @param asset_of_interest
#' @param actuals_periods_needed
#' @param training_end_date
#'
#' @return
#' @export
#'
#' @examples
Single_Asset_V3_Gen_Model <-
  function(Indices_Metals_Bonds,
           actual_wins_losses,
           asset_of_interest = "GBP_JPY",
           actuals_periods_needed = c("period_return_24_Price", "period_return_35_Price", "period_return_46_Price"),
           training_end_date = "2025-05-01",
           bin_threshold = 5,
           rolling_mean_pred_period = 500,
           correlation_rolling_periods = c(100,200, 300),
           state_space_periods = c(20, 40, 60, 100, 200),
           state_space_rolling = c(100, 200),
           copula_assets = c("GBP_USD", "EUR_JPY", "USD_JPY", "XAU_JPY", "GBP_CHF", "XAG_GBP", "GBP_NZD", "UK100_GBP", "EUR_USD", "GBP_AUD"),
           raw_macro_data = raw_macro_data,
           base_path = "C:/Users/Nikhil Chandra/Documents/trade_data/single_asset_models_v3/",
           sig_thresh_AR = 0.01,
           sig_thresh_Copula = 0.01,
           sig_thresh_statespace = 0.01,
           sig_thresh_macro = 0.01) {

    asset_data = Indices_Metals_Bonds[[1]] %>% filter(Asset == asset_of_interest)
    actual_wins_losses_asset <- actual_wins_losses %>% filter(Asset == asset_of_interest)

    AR_preds_list <-
      single_asset_v3_gen_AR_Model(
      Indices_Metals_Bonds = asset_data,
      actual_wins_losses = actual_wins_losses_asset,
      asset_of_interest = asset_of_interest,
      actuals_periods_needed = actuals_periods_needed,
      training_end_date = training_end_date,
      bin_threshold = bin_threshold,
      rolling_mean_pred_period = rolling_mean_pred_period,
      base_path = base_path,
      sig_thresh = sig_thresh_AR
    )

    AR_Train_Preds_mean <-
      AR_preds_list %>%
      pluck("training_data")

    AR_Test_Preds <-
      AR_preds_list %>%
      pluck("testing_data")

    rm(AR_preds_list)

    copula_preds_list <-
      single_asset_v3_gen_Copula_Model(
      Indices_Metals_Bonds = Indices_Metals_Bonds,
      actual_wins_losses_asset = actual_wins_losses_asset,
      asset_of_interest = asset_of_interest,
      actuals_periods_needed = actuals_periods_needed,
      training_end_date = training_end_date,
      bin_threshold = bin_threshold,
      rolling_mean_pred_period = rolling_mean_pred_period,
      sig_thresh = sig_thresh_Copula,
      copula_assets = copula_assets,
      correlation_rolling_periods = correlation_rolling_periods,
      base_path = base_path
    )

    Copula_Train_Preds_mean <-
      copula_preds_list %>%
      pluck("training_data")

    Copula_Test_Preds <-
      copula_preds_list %>%
      pluck("testing_data")

    rm(copula_preds_list)
    gc()

    loop_list_cols <- c("Price", "Low", "High")
    state_space_periods = c(20, 40, 60, 100, 200)
    state_space_rolling = c(100, 200)

    state_space_preds_list <-
      single_asset_v3_gen_state_space_Model(
      asset_data = asset_data,
      actual_wins_losses_asset = actual_wins_losses_asset,
      asset_of_interest = asset_of_interest,
      actuals_periods_needed = actuals_periods_needed,
      training_end_date = training_end_date,
      bin_threshold = bin_threshold,
      rolling_mean_pred_period = rolling_mean_pred_period,
      sig_thresh = sig_thresh_statespace,
      state_space_periods = state_space_periods,
      state_space_rolling = state_space_rolling,
      base_path = base_path
    )

    state_space_Train_Preds_mean <-
      state_space_preds_list %>%
      pluck("training_data")

    state_space_Test_Preds <-
      state_space_preds_list %>%
      pluck("testing_data")

    Macro_preds_list <-
      single_asset_v3_gen_macro_Model(
        asset_data_macro  = asset_data,
        actual_wins_losses = actual_wins_losses_asset,
        asset_of_interest = asset_of_interest,
        actuals_periods_needed = actuals_periods_needed,
        training_end_date = training_end_date,
        bin_threshold = bin_threshold,
        rolling_mean_pred_period = rolling_mean_pred_period,
        sig_thresh = sig_thresh_macro,
        raw_macro_data = raw_macro_data,
        base_path = base_path
      )

    Macro_Train_Preds_mean <-
      Macro_preds_list %>%
      pluck("training_data")

    Macro_Test_Preds <-
      Macro_preds_list %>%
      pluck("testing_data")


    complete_preds_train <-
      AR_Train_Preds_mean %>%
      left_join(
        Copula_Train_Preds_mean
      ) %>%
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
        Copula_Test_Preds
      ) %>%
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

#' Single_Asset_V3_Read_in_Probs
#'
#' @param Indices_Metals_Bonds
#' @param actual_wins_losses
#' @param asset_of_interest
#' @param actuals_periods_needed
#' @param training_end_date
#'
#' @return
#' @export
#'
#' @examples
Single_Asset_V3_Read_in_Probs <-
  function(Indices_Metals_Bonds,
           asset_of_interest = "GBP_JPY",
           actuals_periods_needed = c("period_return_24_Price", "period_return_35_Price", "period_return_46_Price"),
           training_end_date = "2025-05-01",
           bin_threshold = 5,
           rolling_mean_pred_period = 500,
           correlation_rolling_periods = c(100,200, 300),
           copula_assets = c("GBP_USD", "EUR_JPY", "USD_JPY", "XAU_JPY", "GBP_CHF", "XAG_GBP", "GBP_NZD", "UK100_GBP", "EUR_USD", "GBP_AUD"),
           base_path = "C:/Users/Nikhil Chandra/Documents/trade_data/single_asset_models_v3/" ) {

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
        MA_period_1 = 10,
        MA_period_2 = 20,
        MA_period_3 = 30,
        MA_period_4 = 40,
        MA_period_5 = 20,
        MA_period_6 = 20
      )

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

    copula_list <- list()

    for (i in 1:length(correlation_rolling_periods)) {

      copula_list[[i]] <-
        Single_Asset_V3_Cop_data(
          All_Asset_Data =
            Indices_Metals_Bonds[[1]] %>%
            filter(Asset == asset_of_interest| Asset %in% copula_assets),
          asset_of_interest = asset_of_interest,
          copula_assets = copula_assets,
          rolling_period_cor = correlation_rolling_periods[i]
        )

    }

    copula_data <- copula_list %>% reduce(left_join)

    copula_preds_list <- list()

    for (i in 1:length(actuals_periods_needed)) {
      copula_preds_list[[i]] <-
        Single_Asset_V3_Copula_read_Model(
          copula_data = copula_data,
          asset_of_interest = asset_of_interest,
          period_of_analysis = actuals_periods_needed[i],
          training_end_date = training_end_date,
          roll_mean_period = rolling_mean_pred_period,
          base_path = base_path
        )
    }

    Copula_Train_Preds_mean <-
      copula_preds_list %>%
      map(~.x %>% pluck("training_data")) %>%
      reduce(left_join)

    Copula_Test_Preds <-
      copula_preds_list %>%
      map(~.x %>% pluck("testing_data")) %>%
      reduce(left_join)

    rm(copula_preds_list)

    state_space_list <- list()
    loop_list_cols <- c("Price", "Low", "High")
    state_space_periods = c(20, 40, 60, 100, 200)
    state_space_rolling = c(100, 200)
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


    complete_preds_train <-
      AR_Train_Preds_mean %>%
      left_join(
        Copula_Train_Preds_mean
      ) %>%
      left_join(
        state_space_Train_Preds_mean
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
        Copula_Test_Preds
      ) %>%
      left_join(
        state_space_Test_Preds
      )

    return(
      list(
        "complete_preds_test" = complete_preds_test,
        "complete_preds_train" = complete_preds_train
      )
    )

  }

#' single_asset_v3_gen_state_space_Model
#'
#' @param asset_data
#' @param actual_wins_losses
#' @param asset_of_interest
#' @param actuals_periods_needed
#' @param training_end_date
#' @param bin_threshold
#' @param rolling_mean_pred_period
#' @param sig_thresh
#' @param state_space_periods
#' @param state_space_rolling
#' @param base_path
#'
#' @returns
#' @export
#'
#' @examples
single_asset_v3_gen_state_space_Model <-
  function(
    asset_data = asset_data,
    actual_wins_losses_asset = actual_wins_losses_asset,
    asset_of_interest = "EUR_USD",
    actuals_periods_needed = c("period_return_24_Price", "period_return_35_Price", "period_return_46_Price"),
    training_end_date = "2025-05-01",
    bin_threshold = 5,
    rolling_mean_pred_period = 500,
    sig_thresh = 0.15,
    state_space_periods = c(20, 40, 60, 100, 200),
    state_space_rolling = c(100, 200),
    base_path = "C:/Users/Nikhil Chandra/Documents/trade_data/single_asset_models_v1/"
  ) {

    state_space_list <- list()
    loop_list_cols <- c("Price", "Low", "High")
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

    for (i in 1:length(actuals_periods_needed)) {
      Single_Asset_V3_state_space_Gen_Model(
        state_space_data = state_space_data,
        asset_of_interest = asset_of_interest,
        actual_wins_losses_asset = actual_wins_losses_asset,
        period_of_analysis = actuals_periods_needed[i],
        training_end_date = training_end_date,
        bin_threshold = bin_threshold,
        sig_thresh = sig_thresh,
        base_path = base_path
      )
    }

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

    return(
      list(
        "training_data" = state_space_Train_Preds_mean,
        "testing_data" = state_space_Test_Preds
      )
    )

  }

#' Single_Asset_V3_state_space
#'
#' @return
#' @export
#'
#' @examples
Single_Asset_V3_state_space <-
  function(
    asset_data = asset_data,
    asset_of_interest = asset_of_interest,
    Price_diff_lag = 20,
    roll_period_state_space = 100,
    price_col = "Price"
  ) {

    state_space_dat <-
      asset_data %>%
      group_by(Asset) %>%
      arrange(Date) %>%
      mutate(
        Price_diff = lag(!!as.name(price_col)) - lag(!!as.name(price_col), Price_diff_lag),
        state_space_mean =
          slider::slide_dbl(.x = Price_diff, .f = ~ mean(.x, na.rm = T), .before = roll_period_state_space),
        state_space_sd =
          slider::slide_dbl(.x = Price_diff, .f = ~ mean(.x, na.rm = T), .before = roll_period_state_space)
        # state_space_sd =
        #   slider::slide_dbl(.x = Price_diff, .f = ~ sd(.x, na.rm = T), .before = roll_period_state_space)

      ) %>%
      mutate(

        state_space_min =
          case_when(
            Price_diff <= state_space_mean - state_space_sd*4 ~ 1,
            TRUE ~ 0
          ),

        state_space_below_min_3 =
          case_when(
            Price_diff > state_space_mean - state_space_sd*4 & Price_diff <= state_space_mean - state_space_sd*3.5 ~ 1,
            TRUE ~ 0
          ),

        state_space_below_min_2 =
          case_when(
            Price_diff > state_space_mean - state_space_sd*3.5 & Price_diff <= state_space_mean - state_space_sd*3 ~ 1,
            TRUE ~ 0
          ),

        state_space_below_min =
          case_when(
            Price_diff > state_space_mean - state_space_sd*3 & Price_diff <= state_space_mean - state_space_sd*2.5 ~ 1,
            TRUE ~ 0
          ),

        state_space_lowest =
          case_when(
            Price_diff > state_space_mean - state_space_sd*2.5 & Price_diff <= state_space_mean - state_space_sd*2 ~ 1,
            TRUE ~ 0
          ),

        state_space_lowest_middle =
          case_when(
            Price_diff > state_space_mean - state_space_sd*2 & Price_diff <= state_space_mean - state_space_sd*1.5 ~ 1,
            TRUE ~ 0
          ),

        state_space_second_lowest =
          case_when(
            Price_diff > state_space_mean - state_space_sd*1.5 & Price_diff <= state_space_mean - state_space_sd*1 ~ 1,
            TRUE ~ 0
          ),
        state_space_third_lowest =
          case_when(
            Price_diff > state_space_mean - state_space_sd*1 & Price_diff <= state_space_mean - state_space_sd*0.5 ~ 1,
            TRUE ~ 0
          ),

        state_space_fourth_lowest_middle =
          case_when(
            Price_diff > state_space_mean - state_space_sd*0.5 & Price_diff <= state_space_mean - state_space_sd*0.25 ~ 1,
            TRUE ~ 0
          ),

        state_space_fourth_lowest =
          case_when(
            Price_diff > state_space_mean - state_space_sd*0.25 & Price_diff <= state_space_mean - state_space_sd*0 ~ 1,
            TRUE ~ 0
          ),

        state_space_fourth_highest =
          case_when(
            Price_diff > state_space_mean + state_space_sd*0 & Price_diff <= state_space_mean + state_space_sd*0.25 ~ 1,
            TRUE ~ 0
          ),

        state_space_fourth_highest_middle =
          case_when(
            Price_diff > state_space_mean + state_space_sd*0.25 & Price_diff <= state_space_mean + state_space_sd*0.5 ~ 1,
            TRUE ~ 0
          ),

        state_space_third_highest =
          case_when(
            Price_diff > state_space_mean + state_space_sd*0.5 & Price_diff <= state_space_mean + state_space_sd*1 ~ 1,
            TRUE ~ 0
          ),
        state_space_second_highest =
          case_when(
            Price_diff > state_space_mean + state_space_sd*1 & Price_diff <= state_space_mean + state_space_sd*1.5 ~ 1,
            TRUE ~ 0
          ),
        state_space_highest =
          case_when(
            Price_diff > state_space_mean + state_space_sd*1.5 & Price_diff <= state_space_mean + state_space_sd*2 ~ 1,
            TRUE ~ 0
          ),

        state_space_highest_middle =
          case_when(
            Price_diff > state_space_mean + state_space_sd*2 & Price_diff <= state_space_mean + state_space_sd*2.5 ~ 1,
            TRUE ~ 0
          ),

        state_space_below_max =
          case_when(
            Price_diff > state_space_mean + state_space_sd*2.5 & Price_diff <= state_space_mean + state_space_sd*3 ~ 1,
            TRUE ~ 0
          ),

        state_space_below_max_2 =
          case_when(
            Price_diff > state_space_mean + state_space_sd*3 & Price_diff <= state_space_mean + state_space_sd*3.5 ~ 1,
            TRUE ~ 0
          ),

        state_space_below_max_3 =
          case_when(
            Price_diff > state_space_mean + state_space_sd*3.5 & Price_diff <= state_space_mean + state_space_sd*4 ~ 1,
            TRUE ~ 0
          ),

        state_space_max =
          case_when(
            Price_diff > state_space_mean + state_space_sd*4 ~ 1,
            TRUE ~ 0
          )

      ) %>%
      mutate(
        across(
          .cols = c(state_space_max, state_space_below_max_3, state_space_below_max_2, state_space_below_max,
                    state_space_highest, state_space_second_highest,
                    state_space_highest_middle,
                    state_space_third_highest,
                    state_space_fourth_highest,
                    state_space_fourth_highest_middle,
                    state_space_fourth_lowest ,
                    state_space_fourth_lowest_middle,
                    state_space_third_lowest, state_space_second_lowest,
                    state_space_lowest_middle,
                    state_space_lowest, state_space_below_min, state_space_below_min_2,
                    state_space_below_min_3, state_space_min),
          .fns = ~
            slider::slide_dbl(.x = ., .f = ~ sum(.x, na.rm = T), .before = roll_period_state_space)
        )
      ) %>%
      filter(!is.na(Price_diff)) %>%
      mutate(
        total_state_space =
          state_space_max + state_space_below_max +
          state_space_below_max_3 + state_space_below_max_2 +
          state_space_highest + state_space_second_highest +
          state_space_highest_middle +
          state_space_third_highest +
          state_space_fourth_highest +
          state_space_fourth_highest_middle +
          state_space_fourth_lowest +
          state_space_fourth_lowest_middle +
          state_space_third_lowest + state_space_second_lowest +
          state_space_lowest_middle + state_space_lowest +
          state_space_below_min + state_space_below_min_2 +
          state_space_below_min_3 + state_space_min,

        !!as.name( glue::glue("perc_space_max_{Price_diff_lag}_{roll_period_state_space}_{price_col}") ) :=
          state_space_max/total_state_space,
        !!as.name( glue::glue("perc_state_space_below_max_{Price_diff_lag}_{roll_period_state_space}_{price_col}") ) :=
          state_space_below_max/total_state_space,
        !!as.name( glue::glue("perc_state_space_below_max_2_{Price_diff_lag}_{roll_period_state_space}_{price_col}") ) :=
          state_space_below_max_2/total_state_space,
        !!as.name( glue::glue("perc_state_space_below_max_3_{Price_diff_lag}_{roll_period_state_space}_{price_col}") ) :=
          state_space_below_max_3/total_state_space,

        !!as.name( glue::glue("perc_space_highest_{Price_diff_lag}_{roll_period_state_space}_{price_col}") ) :=
          state_space_highest/total_state_space,
        !!as.name( glue::glue("perc_space_second_highest_{Price_diff_lag}_{roll_period_state_space}_{price_col}") ) :=
          state_space_second_highest/total_state_space,

        !!as.name( glue::glue("perc_state_space_highest_middle_{Price_diff_lag}_{roll_period_state_space}_{price_col}") ) :=
          state_space_highest_middle/total_state_space,


        !!as.name( glue::glue("perc_space_third_highest_{Price_diff_lag}_{roll_period_state_space}_{price_col}") ) :=
          state_space_third_highest/total_state_space,
        !!as.name( glue::glue("perc_state_space_fourth_highest_{Price_diff_lag}_{roll_period_state_space}_{price_col}") ) :=
          state_space_fourth_highest/total_state_space,

        !!as.name( glue::glue("perc_state_space_fourth_highest_middle_{Price_diff_lag}_{roll_period_state_space}_{price_col}") ) :=
          state_space_fourth_highest_middle/total_state_space,

        !!as.name( glue::glue("perc_state_space_fourth_lowest_{Price_diff_lag}_{roll_period_state_space}_{price_col}") ) :=
          state_space_fourth_lowest/total_state_space,

        !!as.name( glue::glue("perc_state_space_fourth_lowest_middle_{Price_diff_lag}_{roll_period_state_space}_{price_col}") ) :=
          state_space_fourth_lowest_middle/total_state_space,

        !!as.name( glue::glue("perc_space_third_lowest_{Price_diff_lag}_{roll_period_state_space}_{price_col}") ) :=
          state_space_third_lowest/total_state_space,
        !!as.name( glue::glue("perc_space_second_lowest_{Price_diff_lag}_{roll_period_state_space}_{price_col}") ) :=
          state_space_second_lowest/total_state_space,

        !!as.name( glue::glue("perc_state_space_lowest_middle_{Price_diff_lag}_{roll_period_state_space}_{price_col}") ) :=
          state_space_lowest_middle/total_state_space,

        !!as.name( glue::glue("perc_space_space_lowest_{Price_diff_lag}_{roll_period_state_space}_{price_col}") ) :=
          state_space_lowest/total_state_space,

        !!as.name( glue::glue("perc_state_space_below_min_{Price_diff_lag}_{roll_period_state_space}_{price_col}") ) :=
          state_space_below_min/total_state_space,
        !!as.name( glue::glue("perc_state_space_below_min_2_{Price_diff_lag}_{roll_period_state_space}_{price_col}") ) :=
          state_space_below_min_2/total_state_space,
        !!as.name( glue::glue("perc_state_space_below_min_3_{Price_diff_lag}_{roll_period_state_space}_{price_col}") ) :=
          state_space_below_min_3/total_state_space,

        !!as.name( glue::glue("perc_space_space_min_{Price_diff_lag}_{roll_period_state_space}_{price_col}") ) :=
          state_space_min/total_state_space
      ) %>%
      dplyr::select(Date, Asset, contains("perc_space_")) %>%
      arrange(Date) %>%
      fill(contains("perc_space_"), .direction = "down")

  }

#' Single_Asset_V3_state_space_Gen_Model
#'
#' @param state_space_data
#' @param asset_of_interest
#' @param actual_wins_losses_asset
#' @param period_of_analysis
#' @param training_end_date
#' @param bin_threshold
#' @param sig_thresh
#'
#' @return
#' @export
#'
#' @examples
Single_Asset_V3_state_space_Gen_Model <-
  function(
    state_space_data = state_space_data,
    asset_of_interest = asset_of_interest,
    actual_wins_losses_asset = actual_wins_losses_asset,
    period_of_analysis = actuals_periods_needed[1],
    training_end_date = training_end_date,
    bin_threshold = bin_threshold,
    sig_thresh = 0.01,
    base_path = "C:/Users/Nikhil Chandra/Documents/trade_data/single_asset_models_v3/"
  ) {

    joined_data <-
      state_space_data %>%
      group_by(Asset) %>%
      arrange(Date, .by_group = TRUE) %>%
      left_join(
        actual_wins_losses_asset %>%
          filter(Asset == asset_of_interest) %>%
          dplyr::select(Date, Asset, !!as.name(period_of_analysis))
      ) %>%
      filter(
        Date <= training_end_date
      ) %>%
      mutate(
        bin_var = ifelse( !!as.name(period_of_analysis) >= bin_threshold, 1, 0)
      )

    dependants <-
      names(joined_data) %>%
      keep(~ str_detect(.x, "perc_space_"))

    lm_form <-
      create_lm_formula(dependant = period_of_analysis, independant = dependants)

    LM_model <- lm(formula = lm_form, data = joined_data)

    sig_coefs <- get_sig_coefs(LM_model, p_value_thresh_for_inputs = sig_thresh)

    lm_form <-
      create_lm_formula(dependant = period_of_analysis, independant = sig_coefs)

    LM_model <- lm(formula = lm_form, data = joined_data)

    saveRDS(LM_model,
            glue::glue("{base_path}/LM_state_space_{period_of_analysis}_{asset_of_interest}.RDS")
    )


    dependants <-
      names(joined_data) %>%
      keep(~ str_detect(.x, "perc_space_"))

    Glm_form <-
      create_lm_formula(dependant = "bin_var", independant = dependants)

    GLM_model <- glm(formula = Glm_form, data = joined_data, family = binomial("logit"))

    sig_coefs <- get_sig_coefs(GLM_model, p_value_thresh_for_inputs = sig_thresh)

    Glm_form <-
      create_lm_formula(dependant = "bin_var", independant = sig_coefs)

    GLM_model <- glm(formula = Glm_form, data = joined_data, family = binomial("logit"))

    saveRDS(GLM_model,
            glue::glue("{base_path}/GLM_state_space_{period_of_analysis}_{asset_of_interest}.RDS")
    )

  }

#' Single_Asset_V3_state_space_read_Model
#'
#' @param state_space_data
#' @param asset_of_interest
#' @param period_of_analysis
#' @param training_end_date
#' @param roll_mean_period
#'
#' @return
#' @export
#'
#' @examples
Single_Asset_V3_state_space_read_Model <-
  function(
    state_space_data = state_space_data,
    asset_of_interest = asset_of_interest,
    period_of_analysis = actuals_periods_needed[1],
    training_end_date = training_end_date,
    roll_mean_period = 100,
    base_path = "C:/Users/Nikhil Chandra/Documents/trade_data/single_asset_models_v3/"
  ) {

    LM_model <-
      readRDS(
        glue::glue("{base_path}/LM_state_space_{period_of_analysis}_{asset_of_interest}.RDS")
      )

    preds_all <- predict.lm(object = LM_model, newdata = state_space_data)

    GLM_model <-
      readRDS(
        glue::glue("{base_path}/GLM_state_space_{period_of_analysis}_{asset_of_interest}.RDS")
      )

    preds_all_GLM <- predict(object = GLM_model, newdata = state_space_data, type = "response")

    complete_state_space_data <-
      state_space_data %>%
      filter(Asset == asset_of_interest) %>%
      distinct(Date, Asset) %>%
      mutate(
        !!as.name(glue::glue("state_space_LM_Pred_{period_of_analysis}")) := preds_all,
        !!as.name(glue::glue("state_space_GLM_Pred_{period_of_analysis}")) := preds_all_GLM
      ) %>%
      mutate(
        !!as.name(glue::glue("state_space_LM_Pred_{period_of_analysis}_mean")) :=
          slider::slide_dbl(.x =
                              !!as.name(glue::glue("state_space_LM_Pred_{period_of_analysis}")),
                            .f = ~ mean(.x, na.rm = T),
                            .before = roll_mean_period),
        !!as.name(glue::glue("state_space_LM_Pred_{period_of_analysis}_sd")) :=
          slider::slide_dbl(.x =
                              !!as.name(glue::glue("state_space_LM_Pred_{period_of_analysis}")),
                            .f = ~ sd(.x, na.rm = T),
                            .before = roll_mean_period),

        !!as.name(glue::glue("state_space_GLM_Pred_{period_of_analysis}_mean")) :=
          slider::slide_dbl(.x =
                              !!as.name(glue::glue("state_space_GLM_Pred_{period_of_analysis}")),
                            .f = ~ mean(.x, na.rm = T),
                            .before = roll_mean_period),
        !!as.name(glue::glue("state_space_GLM_Pred_{period_of_analysis}_sd")) :=
          slider::slide_dbl(.x =
                              !!as.name(glue::glue("state_space_GLM_Pred_{period_of_analysis}")),
                            .f = ~ sd(.x, na.rm = T),
                            .before = roll_mean_period)
      )

    testing_data <-
      complete_state_space_data %>%
      filter(Date > training_end_date)

    training_data <-
      complete_state_space_data %>%
      filter(Date <= training_end_date)

    return(list("testing_data" = testing_data, "training_data" = training_data) )

  }

#' Single_Asset_V3_Cop_data
#'
#' @param All_Asset_Data
#' @param asset_of_interest
#' @param copula_assets
#' @param rolling_period_cor
#'
#' @return
#' @export
#'
#' @examples
Single_Asset_V3_Cop_data <-
  function(All_Asset_Data = Indices_Metals_Bonds[[1]] %>% filter(Asset == asset_of_interest| Asset %in% copula_assets),
           asset_of_interest = asset_of_interest,
           copula_assets = copula_assets,
           rolling_period_cor = 100) {

    asset_data_cop <-
      All_Asset_Data %>%
      filter(Asset == asset_of_interest)

    cop_accumulator <- list()

    for (i in 1:length(copula_assets) ) {

      col_prefix <- paste0(asset_of_interest, "_", copula_assets[i])

      cop_comparison_data <-
        All_Asset_Data %>%
        filter(Asset == copula_assets[i]) %>%
        dplyr::select(
          Date,
          Price_2 = Price,
          High_2 = High,
          Low_2 = Low
        )

      cop_accumulator[[i]] <-
        asset_data_cop %>%
        left_join(cop_comparison_data) %>%
        arrange(Date) %>%
        fill(c(Price_2, High_2, Low_2), .direction = "down") %>%
        mutate(
          High_Max_1 = slider::slide_dbl(.x = High,
                                          .f = ~ max(.x, na.rm = TRUE),
                                          .before = rolling_period_cor),
          High_Max_2 = slider::slide_dbl(.x = High_2,
                                          .f = ~ max(.x, na.rm = TRUE),
                                          .before = rolling_period_cor),

          Low_Min_1 = slider::slide_dbl(.x = Low,
                                         .f = ~ min(.x, na.rm = TRUE),
                                         .before = rolling_period_cor),
          Low_Min_2 = slider::slide_dbl(.x = Low_2,
                                         .f = ~ min(.x, na.rm = TRUE),
                                         .before = rolling_period_cor),

          MA_Price_1 = slider::slide_dbl(.x = Price,
                                         .f = ~ mean(.x, na.rm = T),
                                         .before = rolling_period_cor ),

          MA_Price_2 = slider::slide_dbl(.x = Price,
                                         .f = ~ mean(.x, na.rm = T),
                                         .before = rolling_period_cor ),

          SD_Price_1 = slider::slide_dbl(.x = Price,
                                         .f = ~ sd(.x, na.rm = T),
                                         .before = rolling_period_cor ),

          SD_Price_2 = slider::slide_dbl(.x = Price,
                                         .f = ~ sd(.x, na.rm = T),
                                         .before = rolling_period_cor )
        ) %>%
        mutate(

          !!as.name(paste0(col_prefix,"_" ,"cor_price", "_", rolling_period_cor)) :=
            slider::slide2_dbl(.x = (Price), .y = (Price_2), .f = ~ cor(.x, .y), .before = rolling_period_cor),
          !!as.name(paste0(col_prefix,"_" ,"cor_Low", "_", rolling_period_cor)) :=
            slider::slide2_dbl(.x = (Low), .y = (Low_2), .f = ~ cor(.x, .y), .before = rolling_period_cor),
          !!as.name(paste0(col_prefix,"_" ,"cor_High", "_", rolling_period_cor)) :=
            slider::slide2_dbl(.x = (High), .y = (High_2), .f = ~ cor(.x, .y), .before = rolling_period_cor),

          !!as.name(paste0(col_prefix,"_" ,"cor_price_mean", "_", rolling_period_cor)) :=
            slider::slide_dbl(.x = !!as.name(paste0(col_prefix,"_" ,"cor_price", "_", rolling_period_cor)),  .f = ~ mean(.x, na.rm = T), .before = rolling_period_cor),
          !!as.name(paste0(col_prefix,"_" ,"cor_Low_mean", "_", rolling_period_cor)) :=
            slider::slide_dbl(.x = !!as.name(paste0(col_prefix,"_" ,"cor_Low", "_", rolling_period_cor)),  .f = ~ mean(.x, na.rm = T), .before = rolling_period_cor),
          !!as.name(paste0(col_prefix,"_" ,"cor_High_mean", "_", rolling_period_cor)) :=
            slider::slide_dbl(.x = !!as.name(paste0(col_prefix,"_" ,"cor_High", "_", rolling_period_cor)), .f = ~ mean(.x, na.rm = T), .before = rolling_period_cor),

          !!as.name(paste0(col_prefix,"_" ,"cor_price_sd", "_", rolling_period_cor)) :=
            slider::slide_dbl(.x = !!as.name(paste0(col_prefix,"_" ,"cor_price", "_", rolling_period_cor)),  .f = ~ sd(.x, na.rm = T), .before = rolling_period_cor),
          !!as.name(paste0(col_prefix,"_" ,"cor_Low_sd", "_", rolling_period_cor)) :=
            slider::slide_dbl(.x = !!as.name(paste0(col_prefix,"_" ,"cor_Low", "_", rolling_period_cor)),  .f = ~ sd(.x, na.rm = T), .before = rolling_period_cor),
          !!as.name(paste0(col_prefix,"_" ,"cor_High_sd", "_", rolling_period_cor)) :=
            slider::slide_dbl(.x = !!as.name(paste0(col_prefix,"_" ,"cor_High", "_", rolling_period_cor)), .f = ~ sd(.x, na.rm = T), .before = rolling_period_cor),


          !!as.name(paste0(col_prefix,"_" ,"cor_price_max_point", "_", rolling_period_cor)) :=
            slider::slide2_dbl(.x = (High_Max_1), .y = (High_Max_2), .f = ~ cor(.x, .y), .before = rolling_period_cor),
          !!as.name(paste0(col_prefix,"_" ,"cor_price_min_point", "_", rolling_period_cor)) :=
            slider::slide2_dbl(.x = (Low_Min_1), .y = (Low_Min_2), .f = ~ cor(.x, .y), .before = rolling_period_cor),

          !!as.name(paste0(col_prefix,"_" ,"cor_price_max_point_price_diff", "_", rolling_period_cor)) :=
            slider::slide2_dbl(.x = (High_Max_1 - Price ), .y = (High_Max_2 - Price_2), .f = ~ cor(.x, .y), .before = rolling_period_cor),
          !!as.name(paste0(col_prefix,"_" ,"cor_price_min_point_price_diff", "_", rolling_period_cor)) :=
            slider::slide2_dbl(.x = (Low_Min_1 - Price), .y = (Low_Min_2 - Price_2), .f = ~ cor(.x, .y), .before = rolling_period_cor),

          !!as.name(paste0(col_prefix,"_" ,"cor_price_MA_Price", "_", rolling_period_cor)) :=
            slider::slide2_dbl(.x = (MA_Price_1), .y = (MA_Price_2), .f = ~ cor(.x, .y), .before = rolling_period_cor),

          !!as.name(paste0(col_prefix,"_" ,"cor_price_SD_Price", "_", rolling_period_cor)) :=
            slider::slide2_dbl(.x = (SD_Price_1), .y = (SD_Price_2), .f = ~ cor(.x, .y), .before = rolling_period_cor)

        ) %>%
        dplyr::select(-Price, -Price_2, -High, -High_2, -Low, -Low_2, -Vol., -Open, -High_Max_1, -High_Max_2, -Low_Min_1, -Low_Min_2,
                      -MA_Price_1, -MA_Price_2, -SD_Price_1, -SD_Price_2) %>%
        group_by(Asset) %>%
        arrange(Date, .by_group = TRUE) %>%
        group_by(Asset) %>%
        mutate(across(
          .cols = contains("cor_"),
          .fns = ~ lag(.)
        ))

    }

    returned_data <-
      cop_accumulator %>%
      reduce(left_join)


  }

#' Title
#'
#' @param Indices_Metals_Bonds
#' @param actual_wins_losses
#' @param asset_of_interest
#' @param actuals_periods_needed
#' @param training_end_date
#' @param bin_threshold
#' @param rolling_mean_pred_period
#' @param sig_thresh
#' @param base_path
#'
#' @returns
#' @export
#'
#' @examples
single_asset_v3_gen_Copula_Model <-
  function(
    Indices_Metals_Bonds = Indices_Metals_Bonds,
    actual_wins_losses_asset = actual_wins_losses_asset,
    asset_of_interest = "EUR_USD",
    actuals_periods_needed = c("period_return_24_Price", "period_return_35_Price", "period_return_46_Price"),
    training_end_date = "2025-05-01",
    bin_threshold = 5,
    rolling_mean_pred_period = 500,
    sig_thresh = 0.15,
    copula_assets = copula_assets,
    correlation_rolling_periods = correlation_rolling_periods,
    base_path = "C:/Users/Nikhil Chandra/Documents/trade_data/single_asset_models_v3/"
  ) {

    copula_list <- list()

    for (i in 1:length(correlation_rolling_periods)) {

      copula_list[[i]] <-
        Single_Asset_V3_Cop_data(
          All_Asset_Data =
            Indices_Metals_Bonds[[1]] %>%
            filter(Asset == asset_of_interest| Asset %in% copula_assets),
          asset_of_interest = asset_of_interest,
          copula_assets = copula_assets,
          rolling_period_cor = correlation_rolling_periods[i]
        )

    }

    copula_data <- copula_list %>% reduce(left_join)

    for (i in 1:length(actuals_periods_needed)) {
      Single_Asset_V3_Copula_Gen_Model(
        copula_data = copula_data,
        asset_of_interest = asset_of_interest,
        actual_wins_losses_asset = actual_wins_losses_asset,
        period_of_analysis = actuals_periods_needed[i],
        training_end_date = training_end_date,
        bin_threshold = bin_threshold,
        sig_thresh = sig_thresh,
        base_path = base_path
      )
    }

    copula_preds_list <- list()

    for (i in 1:length(actuals_periods_needed)) {
      copula_preds_list[[i]] <-
        Single_Asset_V3_Copula_read_Model(
          copula_data = copula_data,
          asset_of_interest = asset_of_interest,
          period_of_analysis = actuals_periods_needed[i],
          training_end_date = training_end_date,
          roll_mean_period = rolling_mean_pred_period,
          base_path = base_path
        )
    }

    Copula_Train_Preds_mean <-
      copula_preds_list %>%
      map(~.x %>% pluck("training_data")) %>%
      reduce(left_join)

    Copula_Test_Preds <-
      copula_preds_list %>%
      map(~.x %>% pluck("testing_data")) %>%
      reduce(left_join)

    rm(copula_preds_list)
    gc()

    return(
      list(
        "training_data" = Copula_Train_Preds_mean,
        "testing_data" = Copula_Test_Preds
      )
    )

  }

#' Single_Asset_V3_Copula_Gen_Model
#'
#' @param copula_data
#' @param asset_of_interest
#' @param actual_wins_losses_asset
#' @param period_of_analysis
#' @param training_end_date
#' @param bin_threshold
#' @param sig_thresh
#'
#' @return
#' @export
#'
#' @examples
Single_Asset_V3_Copula_Gen_Model <-
  function(
    copula_data = copula_data,
    asset_of_interest = asset_of_interest,
    actual_wins_losses_asset = actual_wins_losses_asset,
    period_of_analysis = actuals_periods_needed[1],
    training_end_date = training_end_date,
    bin_threshold = bin_threshold,
    sig_thresh = 0.15,
    base_path = "C:/Users/Nikhil Chandra/Documents/trade_data/single_asset_models_v3/"
  ) {

    joined_data <-
      copula_data %>%
      group_by(Asset) %>%
      arrange(Date, .by_group = TRUE) %>%
      fill(contains("cor"), .direction = "down") %>%
      left_join(
        actual_wins_losses_asset %>%
          filter(Asset == asset_of_interest) %>%
          dplyr::select(Date, Asset, !!as.name(period_of_analysis))
      ) %>%
      filter(
        Date <= training_end_date
      ) %>%
      mutate(
        bin_var = ifelse( !!as.name(period_of_analysis) >= bin_threshold, 1, 0)
      )

    dependants <-
      names(joined_data) %>%
      keep(~ str_detect(.x, "cor_"))

    lm_form <-
      create_lm_formula(dependant = period_of_analysis, independant = dependants)

    LM_model <- lm(formula = lm_form, data = joined_data)

    sig_coefs <- get_sig_coefs(LM_model, p_value_thresh_for_inputs = sig_thresh)

    lm_form <-
      create_lm_formula(dependant = period_of_analysis, independant = sig_coefs)

    LM_model <- lm(formula = lm_form, data = joined_data)

    saveRDS(LM_model,
            glue::glue("{base_path}/LM_Copula_{period_of_analysis}_{asset_of_interest}.RDS")
    )

    dependants <-
      names(joined_data) %>%
      keep(~ str_detect(.x, "cor_"))

    Glm_form <-
      create_lm_formula(dependant = "bin_var", independant = dependants)

    GLM_model <- glm(formula = Glm_form, data = joined_data, family = binomial("logit"))

    sig_coefs <- get_sig_coefs(GLM_model, p_value_thresh_for_inputs = sig_thresh)

    Glm_form <-
      create_lm_formula(dependant = "bin_var", independant = sig_coefs)

    GLM_model <- glm(formula = Glm_form, data = joined_data, family = binomial("logit"))

    saveRDS(GLM_model,
            glue::glue("{base_path}/GLM_Copula_{period_of_analysis}_{asset_of_interest}.RDS")
    )

    rm(GLM_model)

  }

#' Single_Asset_V3_Copula_read_Model
#'
#' @param copula_data
#' @param asset_of_interest
#' @param period_of_analysis
#' @param training_end_date
#' @param roll_mean_period
#'
#' @return
#' @export
#'
#' @examples
Single_Asset_V3_Copula_read_Model <-
  function(
    copula_data = copula_data,
    asset_of_interest = asset_of_interest,
    period_of_analysis = actuals_periods_needed[1],
    training_end_date = training_end_date,
    roll_mean_period = 100,
    base_path = "C:/Users/Nikhil Chandra/Documents/trade_data/single_asset_models_v3/"
  ) {

    LM_model <-
      readRDS(
        glue::glue("{base_path}/LM_Copula_{period_of_analysis}_{asset_of_interest}.RDS")
      )

    preds_all <- predict.lm(object = LM_model, newdata = copula_data)

    GLM_model <-
      readRDS(
        glue::glue("{base_path}/GLM_Copula_{period_of_analysis}_{asset_of_interest}.RDS")
      )

    preds_all_GLM <- predict(object = GLM_model, newdata = copula_data, type = "response")

    complete_copula_data <-
      copula_data %>%
      filter(Asset == asset_of_interest) %>%
      distinct(Date, Asset) %>%
      mutate(
        !!as.name(glue::glue("Copula_LM_Pred_{period_of_analysis}")) := preds_all,
        !!as.name(glue::glue("Copula_GLM_Pred_{period_of_analysis}")) := preds_all_GLM
      ) %>%
      mutate(
        !!as.name(glue::glue("Copula_LM_Pred_{period_of_analysis}_mean")) :=
          slider::slide_dbl(.x =
                              !!as.name(glue::glue("Copula_LM_Pred_{period_of_analysis}")),
                            .f = ~ mean(.x, na.rm = T),
                            .before = roll_mean_period),
        !!as.name(glue::glue("Copula_LM_Pred_{period_of_analysis}_sd")) :=
          slider::slide_dbl(.x =
                              !!as.name(glue::glue("Copula_LM_Pred_{period_of_analysis}")),
                            .f = ~ sd(.x, na.rm = T),
                            .before = roll_mean_period),

        !!as.name(glue::glue("Copula_GLM_Pred_{period_of_analysis}_mean")) :=
          slider::slide_dbl(.x =
                              !!as.name(glue::glue("Copula_GLM_Pred_{period_of_analysis}")),
                            .f = ~ mean(.x, na.rm = T),
                            .before = roll_mean_period),
        !!as.name(glue::glue("Copula_GLM_Pred_{period_of_analysis}_sd")) :=
          slider::slide_dbl(.x =
                              !!as.name(glue::glue("Copula_GLM_Pred_{period_of_analysis}")),
                            .f = ~ sd(.x, na.rm = T),
                            .before = roll_mean_period)
      )

    testing_data <-
      complete_copula_data %>%
      filter(Date > training_end_date)

    training_data <-
      complete_copula_data %>%
      filter(Date <= training_end_date)

    return(list("testing_data" = testing_data, "training_data" = training_data) )


  }

#' single_asset_v3_gen_AR_Model
#'
#' @param Indices_Metals_Bonds
#' @param actual_wins_losses
#' @param asset_of_interest
#' @param actuals_periods_needed
#' @param training_end_date
#' @param bin_threshold
#' @param rolling_mean_pred_period
#' @param base_path
#'
#' @return
#' @export
#'
#' @examples
single_asset_v3_gen_AR_Model <-
  function(
    Indices_Metals_Bonds,
    actual_wins_losses,
    asset_of_interest = "EUR_USD",
    actuals_periods_needed = c("period_return_24_Price", "period_return_35_Price", "period_return_46_Price"),
    training_end_date = "2025-05-01",
    bin_threshold = 5,
    rolling_mean_pred_period = 500,
    sig_thresh = 0.15,
    base_path = "C:/Users/Nikhil Chandra/Documents/trade_data/single_asset_models_v3/"
  ) {

    asset_data = Indices_Metals_Bonds %>% filter(Asset == asset_of_interest)
    actual_wins_losses_asset <- actual_wins_losses %>% filter(Asset == asset_of_interest)

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

    for (i in 1:length(actuals_periods_needed)) {
      Single_Asset_V3_AR_Gen_Model(
        AR_model_data = AR_model_data,
        asset_of_interest = asset_of_interest,
        actual_wins_losses_asset = actual_wins_losses_asset,
        period_of_analysis = actuals_periods_needed[i],
        training_end_date = training_end_date,
        bin_threshold = bin_threshold,
        sig_thresh = sig_thresh,
        base_path = base_path
      )
    }

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

    return(
      list(
        "training_data" = AR_Train_Preds_mean,
        "testing_data" = AR_Test_Preds
      )
    )

  }

#' Single_Asset_V3_AR_read_model
#'
#' @param AR_model_data
#' @param asset_of_interest
#' @param period_of_analysis
#' @param training_end_date
#' @param bin_threshold
#' @param sig_thresh
#'
#' @return
#' @export
#'
#' @examples
Single_Asset_V3_AR_read_model <-
  function(
    AR_model_data = AR_model_data,
    asset_of_interest = asset_of_interest,
    period_of_analysis = actuals_periods_needed[1],
    training_end_date = training_end_date,
    roll_mean_period = 100,
    base_path = "C:/Users/Nikhil Chandra/Documents/trade_data/single_asset_models_v3/"
  ) {

    LM_model <-
      readRDS(
        glue::glue("{base_path}/LM_AR_{period_of_analysis}_{asset_of_interest}.RDS")
      )

    preds_all <- predict.lm(object = LM_model, newdata = AR_model_data)

    GLM_model <-
      readRDS(
        glue::glue("{base_path}/GLM_AR_{period_of_analysis}_{asset_of_interest}.RDS")
      )

    preds_all_GLM <- predict(object = GLM_model, newdata = AR_model_data, type = "response")

    complete_AR_data <-
      AR_model_data %>%
      filter(Asset == asset_of_interest) %>%
      distinct(Date, Asset) %>%
      mutate(
        !!as.name(glue::glue("AR_LM_Pred_{period_of_analysis}")) := preds_all,
        !!as.name(glue::glue("AR_GLM_Pred_{period_of_analysis}")) := preds_all_GLM
      ) %>%
      mutate(
        !!as.name(glue::glue("AR_LM_Pred_{period_of_analysis}_mean")) :=
          slider::slide_dbl(.x =
                              !!as.name(glue::glue("AR_LM_Pred_{period_of_analysis}")),
                            .f = ~ mean(.x, na.rm = T),
                            .before = roll_mean_period),
        !!as.name(glue::glue("AR_LM_Pred_{period_of_analysis}_sd")) :=
          slider::slide_dbl(.x =
                              !!as.name(glue::glue("AR_LM_Pred_{period_of_analysis}")),
                            .f = ~ sd(.x, na.rm = T),
                            .before = roll_mean_period),

        !!as.name(glue::glue("AR_GLM_Pred_{period_of_analysis}_mean")) :=
          slider::slide_dbl(.x =
                              !!as.name(glue::glue("AR_GLM_Pred_{period_of_analysis}")),
                            .f = ~ mean(.x, na.rm = T),
                            .before = roll_mean_period),
        !!as.name(glue::glue("AR_GLM_Pred_{period_of_analysis}_sd")) :=
          slider::slide_dbl(.x =
                              !!as.name(glue::glue("AR_GLM_Pred_{period_of_analysis}")),
                            .f = ~ sd(.x, na.rm = T),
                            .before = roll_mean_period)
      )

    testing_data <-
      complete_AR_data %>%
      filter(Date > training_end_date)

    training_data <-
      complete_AR_data %>%
      filter(Date <= training_end_date)

    return(list("testing_data" = testing_data, "training_data" = training_data) )

  }

#' Single_Asset_V3_AR_Gen_Model
#'
#' @param AR_model_data
#' @param asset_of_interest
#' @param actual_wins_losses_asset
#' @param period_of_analysis
#' @param training_end_date
#' @param bin_threshold
#' @param sig_thresh
#'
#' @return
#' @export
#'
#' @examples
Single_Asset_V3_AR_Gen_Model <-
  function(
    AR_model_data = AR_model_data,
    asset_of_interest = asset_of_interest,
    actual_wins_losses_asset = actual_wins_losses_asset,
    period_of_analysis = actuals_periods_needed[1],
    training_end_date = training_end_date,
    bin_threshold = bin_threshold,
    sig_thresh = 0.15,
    base_path = "C:/Users/Nikhil Chandra/Documents/trade_data/single_asset_models_v3/"
  ) {

    joined_data <-
      AR_model_data %>%
      left_join(
        actual_wins_losses_asset %>%
          filter(Asset == asset_of_interest) %>%
          dplyr::select(Date, Asset, !!as.name(period_of_analysis))
      ) %>%
      filter(
        Date <= training_end_date
      ) %>%
      mutate(
        bin_var = ifelse( !!as.name(period_of_analysis) >= bin_threshold, 1, 0)
      )

    dependants <-
      names(joined_data) %>%
      keep(~ str_detect(.x, "MA_|lagged_|MSD_"))

    lm_form <-
      create_lm_formula(dependant = period_of_analysis, independant = dependants)

    LM_model <- lm(formula = lm_form, data = joined_data)

    sig_coefs <- get_sig_coefs(LM_model, p_value_thresh_for_inputs = sig_thresh)

    lm_form <-
      create_lm_formula(dependant = period_of_analysis, independant = sig_coefs)

    LM_model <- lm(formula = lm_form, data = joined_data)

    saveRDS(LM_model,
            glue::glue("{base_path}/LM_AR_{period_of_analysis}_{asset_of_interest}.RDS")
    )

    rm(LM_model)

    dependants <-
      names(joined_data) %>%
      keep(~ str_detect(.x, "MA_|lagged_|MSD_"))

    Glm_form <-
      create_lm_formula(dependant = "bin_var", independant = dependants)

    GLM_model <- glm(formula = Glm_form, data = joined_data, family = binomial("logit"))

    sig_coefs <- get_sig_coefs(GLM_model, p_value_thresh_for_inputs = sig_thresh)

    Glm_form <-
      create_lm_formula(dependant = "bin_var", independant = sig_coefs)

    GLM_model <- glm(formula = Glm_form, data = joined_data, family = binomial("logit"))

    saveRDS(GLM_model,
            glue::glue("{base_path}/GLM_AR_{period_of_analysis}_{asset_of_interest}.RDS")
    )

  }

#' Single_Asset_V3_AR_Model_data
#'
#' @param asset_data
#' @param asset_of_interest
#' @param lag_value_1
#' @param lag_value_2
#' @param lag_value_3
#' @param lag_value_4
#' @param lag_value_5
#' @param lag_value_6
#' @param MA_period_1
#' @param MA_period_2
#' @param MA_period_3
#' @param MA_period_4
#'
#' @return
#' @export
#'
#' @examples
Single_Asset_V3_AR_Model_data <-
  function(
    asset_data = Indices_Metals_Bonds,
    asset_of_interest = asset_of_interest,
    lag_value_1 = 2,
    lag_value_2 = 4,
    lag_value_3 = 6,
    lag_value_4 = 8,
    lag_value_5 = 10,
    lag_value_6 = 12,
    lag_value_7 = 20,
    lag_value_8 = 100,

    MA_period_1 = 5,
    MA_period_2 = 10,
    MA_period_3 = 15,
    MA_period_4 = 20,
    MA_period_5 = 30,
    MA_period_6 = 40,
    MA_period_7 = 80,
    MA_period_8 = 100
  ) {

    returned_data <-
      asset_data %>%
      ungroup() %>%
      filter(Asset == asset_of_interest) %>%
      group_by(Asset) %>%
      arrange(Date, .by_group = TRUE) %>%
      ungroup() %>%
      mutate(
        lagged_Price = lag(Price) - lag(Price, lag_value_1 + 1),
        lagged_High = lag(High) - lag(Price, lag_value_1 + 1),
        lagged_Low = lag(Low) - lag(Price, lag_value_1 + 1),

        lagged_Price2 = lag(Price) - lag(Price, lag_value_2 + 1),
        lagged_High2 = lag(High) - lag(Price, lag_value_2 + 1),
        lagged_Low2 = lag(Low) - lag(Price, lag_value_2 + 1),

        lagged_Price3 = lag(Price) - lag(Price, lag_value_3 + 1),
        lagged_High3 = lag(High) - lag(Price, lag_value_3 + 1),
        lagged_Low3 = lag(Low) - lag(Price, lag_value_3 + 1),

        lagged_Price4 = lag(Price) - lag(Price, lag_value_4 + 1),
        lagged_High4 = lag(High) - lag(Price, lag_value_4 + 1),
        lagged_Low4 = lag(Low) - lag(Price, lag_value_4 + 1),

        lagged_Price5 = lag(Price) - lag(Price, lag_value_5 + 1),
        lagged_High5 = lag(High) - lag(Price, lag_value_5 + 1),
        lagged_Low5 = lag(Low) - lag(Price, lag_value_5 + 1),

        lagged_Price6 = lag(Price) - lag(Price, lag_value_6 + 1),
        lagged_High6 = lag(High) - lag(Price, lag_value_6 + 1),
        lagged_Low6 = lag(Low) - lag(Price, lag_value_6 + 1),

        lagged_Price7 = lag(Price) - lag(Price, lag_value_7 + 1),
        lagged_High7 = lag(High) - lag(Price, lag_value_7 + 1),
        lagged_Low7 = lag(Low) - lag(Price, lag_value_7 + 1),

        lagged_Price8 = lag(Price) - lag(Price, lag_value_8 + 1),
        lagged_High8 = lag(High) - lag(Price, lag_value_8 + 1),
        lagged_Low8 = lag(Low) - lag(Price, lag_value_8 + 1),

        lagged_Price_sq = lagged_Price^2,
        lagged_High_sq = lagged_High^2,
        lagged_Low_sq = lagged_Low^2,

        lagged_Price2_sq = lagged_Price2^2,
        lagged_High2_sq = lagged_High2^2,
        lagged_Low2_sq = lagged_Low2^2,

        lagged_Price3_sq = lagged_Price3^2,
        lagged_High3_sq = lagged_High3^2,
        lagged_Low3_sq = lagged_Low3^2,

        lagged_Price4_sq = lagged_Price4^2,
        lagged_High4_sq = lagged_High4^2,
        lagged_Low4_sq = lagged_Low4^2,

        lagged_Price5_sq= lagged_Price5^2,
        lagged_High5_sq = lagged_High5^2,
        lagged_Low5_sq = lagged_Low5^2,

        lagged_Price6_sq= lagged_Price6^2,
        lagged_High6_sq = lagged_High6^2,
        lagged_Low6_sq = lagged_Low6^2,

        lagged_Price7_sq= lagged_Price7^2,
        lagged_High7_sq = lagged_High7^2,
        lagged_Low7_sq = lagged_Low7^2

      ) %>%
      mutate(
        MA_Price_1 = slider::slide_dbl(.x = lagged_Price, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_1),
        MA_High_1 = slider::slide_dbl(.x = lagged_High, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_1),
        MA_Low_1 = slider::slide_dbl(.x = lagged_Low, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_1),

        MA_Price_2 = slider::slide_dbl(.x = lagged_Price2, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_2),
        MA_High_2 = slider::slide_dbl(.x = lagged_High2, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_2),
        MA_Low_2 = slider::slide_dbl(.x = lagged_Low2, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_2),

        MA_Price_3 = slider::slide_dbl(.x = lagged_Price3, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_3),
        MA_High_3 = slider::slide_dbl(.x = lagged_High3, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_3),
        MA_Low_3 = slider::slide_dbl(.x = lagged_Low3, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_3),

        MA_Price_4 = slider::slide_dbl(.x = lagged_Price4, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_4),
        MA_High_4 = slider::slide_dbl(.x = lagged_High4, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_4),
        MA_Low_4 = slider::slide_dbl(.x = lagged_Low4, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_4),

        MA_Price_5 = slider::slide_dbl(.x = lagged_Price5, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_5),
        MA_High_5 = slider::slide_dbl(.x = lagged_High5, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_5),
        MA_Low_5 = slider::slide_dbl(.x = lagged_Low5, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_5),

        MA_Price_6 = slider::slide_dbl(.x = lagged_Price6, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_6),
        MA_High_6 = slider::slide_dbl(.x = lagged_High6, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_6),
        MA_Low_6 = slider::slide_dbl(.x = lagged_Low6, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_6),

        MA_Price_7 = slider::slide_dbl(.x = lagged_Price7, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_7),
        MA_High_7 = slider::slide_dbl(.x = lagged_High7, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_7),
        MA_Low_7 = slider::slide_dbl(.x = lagged_Low7, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_7),

        MA_Price_8 = slider::slide_dbl(.x = lagged_Price8, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_8),
        MA_High_8 = slider::slide_dbl(.x = lagged_High8, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_8),
        MA_Low_8 = slider::slide_dbl(.x = lagged_Low8, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_8),

        MSD_Price_1 = slider::slide_dbl(.x = lagged_Price, .f = ~ sd(.x, na.rm = T) ,.before = MA_period_1),
        MSD_High_1 = slider::slide_dbl(.x = lagged_High, .f = ~ sd(.x, na.rm = T) ,.before = MA_period_1),
        MSD_Low_1 = slider::slide_dbl(.x = lagged_Low, .f = ~ sd(.x, na.rm = T) ,.before = MA_period_1),

        MSD_Price_2 = slider::slide_dbl(.x = lagged_Price2, .f = ~ sd(.x, na.rm = T) ,.before = MA_period_2),
        MSD_High_2 = slider::slide_dbl(.x = lagged_High2, .f = ~ sd(.x, na.rm = T) ,.before = MA_period_2),
        MSD_Low_2 = slider::slide_dbl(.x = lagged_Low2, .f = ~ sd(.x, na.rm = T) ,.before = MA_period_2),

        MSD_Price_3 = slider::slide_dbl(.x = lagged_Price3, .f = ~ sd(.x, na.rm = T) ,.before = MA_period_3),
        MSD_High_3 = slider::slide_dbl(.x = lagged_High3, .f = ~ sd(.x, na.rm = T) ,.before = MA_period_3),
        MSD_Low_3 = slider::slide_dbl(.x = lagged_Low3, .f = ~ sd(.x, na.rm = T) ,.before = MA_period_3),

        MSD_Price_4 = slider::slide_dbl(.x = lagged_Price4, .f = ~ sd(.x, na.rm = T) ,.before = MA_period_4),
        MSD_High_4 = slider::slide_dbl(.x = lagged_High4, .f = ~ sd(.x, na.rm = T) ,.before = MA_period_4),
        MSD_Low_4 = slider::slide_dbl(.x = lagged_Low4, .f = ~ sd(.x, na.rm = T) ,.before = MA_period_4),

        MSD_Price_5 = slider::slide_dbl(.x = lagged_Price5, .f = ~ sd(.x, na.rm = T) ,.before = MA_period_5),
        MSD_High_5 = slider::slide_dbl(.x = lagged_High5, .f = ~ sd(.x, na.rm = T) ,.before = MA_period_5),
        MSD_Low_5 = slider::slide_dbl(.x = lagged_Low5, .f = ~ sd(.x, na.rm = T) ,.before = MA_period_5),

        MSD_Price_6 = slider::slide_dbl(.x = lagged_Price6, .f = ~ sd(.x, na.rm = T) ,.before = MA_period_6),
        MSD_High_6 = slider::slide_dbl(.x = lagged_High6, .f = ~ sd(.x, na.rm = T) ,.before = MA_period_6),
        MSD_Low_6 = slider::slide_dbl(.x = lagged_Low6, .f = ~ sd(.x, na.rm = T) ,.before = MA_period_6),

        MSD_Price_7 = slider::slide_dbl(.x = lagged_Price7, .f = ~ sd(.x, na.rm = T) ,.before = MA_period_7),
        MSD_High_7 = slider::slide_dbl(.x = lagged_High7, .f = ~ sd(.x, na.rm = T) ,.before = MA_period_7),
        MSD_Low_7 = slider::slide_dbl(.x = lagged_Low7, .f = ~ sd(.x, na.rm = T) ,.before = MA_period_7),

        MSD_Price_8 = slider::slide_dbl(.x = lagged_Price8, .f = ~ sd(.x, na.rm = T) ,.before = MA_period_8),
        MSD_High_8 = slider::slide_dbl(.x = lagged_High8, .f = ~ sd(.x, na.rm = T) ,.before = MA_period_8),
        MSD_Low_8 = slider::slide_dbl(.x = lagged_Low8, .f = ~ sd(.x, na.rm = T) ,.before = MA_period_8),

        MA_Price_1_sq = slider::slide_dbl(.x = lagged_Price_sq, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_1),
        MA_High_1_sq = slider::slide_dbl(.x = lagged_High_sq, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_1),
        MA_Low_1_sq = slider::slide_dbl(.x = lagged_Low_sq, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_1),

        MA_Price_2_sq = slider::slide_dbl(.x = lagged_Price2_sq, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_2),
        MA_High_2_sq = slider::slide_dbl(.x = lagged_High2_sq, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_2),
        MA_Low_2_sq = slider::slide_dbl(.x = lagged_Low2_sq, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_2),

        MA_Price_3_sq = slider::slide_dbl(.x = lagged_Price3_sq, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_3),
        MA_High_3_sq = slider::slide_dbl(.x = lagged_High3_sq, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_3),
        MA_Low_3_sq = slider::slide_dbl(.x = lagged_Low3_sq, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_3),

        MA_Price_4_sq = slider::slide_dbl(.x = lagged_Price4_sq, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_4),
        MA_High_4_sq = slider::slide_dbl(.x = lagged_High4_sq, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_4),
        MA_Low_4_sq = slider::slide_dbl(.x = lagged_Low4_sq, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_4),

        MA_Price_5_sq = slider::slide_dbl(.x = lagged_Price5_sq, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_5),
        MA_High_5_sq = slider::slide_dbl(.x = lagged_High5_sq, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_5),
        MA_Low_5_sq = slider::slide_dbl(.x = lagged_Low5_sq, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_5),

        MA_Price_6_sq = slider::slide_dbl(.x = lagged_Price6_sq, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_6),
        MA_High_6_sq = slider::slide_dbl(.x = lagged_High6_sq, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_6),
        MA_Low_6_sq = slider::slide_dbl(.x = lagged_Low6_sq, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_6),

        MA_Price_7_sq = slider::slide_dbl(.x = lagged_Price7_sq, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_7),
        MA_High_7_sq = slider::slide_dbl(.x = lagged_High7_sq, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_7),
        MA_Low_7_sq = slider::slide_dbl(.x = lagged_Low7_sq, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_7),

        lagged_Pos_1_Price = ifelse(lagged_Price > 0, 1, 0),
        lagged_Pos_2_Price = ifelse(lagged_Price2 > 0, 1, 0),
        lagged_Pos_3_Price = ifelse(lagged_Price3 > 0, 1, 0),
        lagged_Pos_4_Price = ifelse(lagged_Price4 > 0, 1, 0),
        lagged_Pos_5_Price = ifelse(lagged_Price5 > 0, 1, 0),
        lagged_Pos_6_Price = ifelse(lagged_Price6 > 0, 1, 0),
        lagged_Pos_7_Price = ifelse(lagged_Price7 > 0, 1, 0),
        lagged_Pos_8_Price = ifelse(lagged_Price8 > 0, 1, 0),


        lagged_Pos_1_High = ifelse(lagged_High > 0, 1, 0),
        lagged_Pos_2_High = ifelse(lagged_High2 > 0, 1, 0),
        lagged_Pos_3_High = ifelse(lagged_High3 > 0, 1, 0),
        lagged_Pos_4_High = ifelse(lagged_High4 > 0, 1, 0),
        lagged_Pos_5_High = ifelse(lagged_High5 > 0, 1, 0),
        lagged_Pos_6_High = ifelse(lagged_High6 > 0, 1, 0),
        lagged_Pos_7_High = ifelse(lagged_High7 > 0, 1, 0),
        lagged_Pos_8_High = ifelse(lagged_High8 > 0, 1, 0),

        lagged_Pos_Neg_1_Price = ifelse( (lagged_Price > 0 & lag(lagged_Price) <= 0)|
                                           (lagged_Price <= 0 & lag(lagged_Price) > 0) , 1, 0),

        lagged_Pos_Neg_2_Price = ifelse( (lagged_Price2 > 0 & lag(lagged_Price2) <= 0)|
                                           (lagged_Price2 <= 0 & lag(lagged_Price2) > 0) , 1, 0),

        lagged_Pos_Neg_3_Price = ifelse( (lagged_Price3 > 0 & lag(lagged_Price3) <= 0)|
                                           (lagged_Price3 <= 0 & lag(lagged_Price3) > 0) , 1, 0),

        lagged_Pos_Neg_4_Price = ifelse( (lagged_Price4 > 0 & lag(lagged_Price4) <= 0)|
                                           (lagged_Price4 <= 0 & lag(lagged_Price4) > 0) , 1, 0),

        lagged_Pos_Neg_5_Price = ifelse( (lagged_Price5 > 0 & lag(lagged_Price5) <= 0)|
                                           (lagged_Price5 <= 0 & lag(lagged_Price5) > 0) , 1, 0),

        lagged_Pos_Neg_6_Price = ifelse( (lagged_Price6 > 0 & lag(lagged_Price6) <= 0)|
                                           (lagged_Price6 <= 0 & lag(lagged_Price6) > 0) , 1, 0),

        lagged_Pos_Neg_7_Price = ifelse( (lagged_Price7 > 0 & lag(lagged_Price7) <= 0)|
                                           (lagged_Price7 <= 0 & lag(lagged_Price7) > 0) , 1, 0),

        lagged_Pos_Neg_8_Price = ifelse( (lagged_Price8 > 0 & lag(lagged_Price8) <= 0)|
                                           (lagged_Price8 <= 0 & lag(lagged_Price8) > 0) , 1, 0 ),

        lagged_Pos_1_Price_interact = lagged_Pos_1_Price*lagged_Price,
        lagged_Pos_2_Price_interact = lagged_Pos_2_Price*lagged_Price2,
        lagged_Pos_3_Price_interact = lagged_Pos_3_Price*lagged_Price3,
        lagged_Pos_4_Price_interact = lagged_Pos_4_Price*lagged_Price4,
        lagged_Pos_5_Price_interact = lagged_Pos_5_Price*lagged_Price5,
        lagged_Pos_6_Price_interact = lagged_Pos_6_Price*lagged_Price6,
        lagged_Pos_7_Price_interact = lagged_Pos_7_Price*lagged_Price7,
        lagged_Pos_8_Price_interact = lagged_Pos_8_Price*lagged_Price8,

        MA_Pos_1_Price_interact = lagged_Pos_1_Price*MA_Price_1,
        MA_Pos_2_Price_interact = lagged_Pos_2_Price*MA_Price_2,
        MA_Pos_3_Price_interact = lagged_Pos_3_Price*MA_Price_3,
        MA_Pos_4_Price_interact = lagged_Pos_4_Price*MA_Price_4,
        MA_Pos_5_Price_interact = lagged_Pos_5_Price*MA_Price_5,
        MA_Pos_6_Price_interact = lagged_Pos_6_Price*MA_Price_6,
        MA_Pos_7_Price_interact = lagged_Pos_7_Price*MA_Price_7,
        MA_Pos_8_Price_interact = lagged_Pos_8_Price*MA_Price_8,


        MA_Pos_1_Vol_interact = lagged_Pos_1_Price*MA_Price_1_sq,
        MA_Pos_2_Vol_interact = lagged_Pos_2_Price*MA_Price_2_sq,
        MA_Pos_3_Vol_interact = lagged_Pos_3_Price*MA_Price_3_sq,
        MA_Pos_4_Vol_interact = lagged_Pos_4_Price*MA_Price_4_sq,
        MA_Pos_5_Vol_interact = lagged_Pos_5_Price*MA_Price_5_sq,
        MA_Pos_6_Vol_interact = lagged_Pos_6_Price*MA_Price_6_sq,
        MA_Pos_7_Vol_interact = lagged_Pos_7_Price*MA_Price_7_sq

      ) %>%
      group_by(Asset) %>%
      arrange(Date, .by_group = TRUE) %>%
      ungroup() %>%
      mutate(
        lagged_Pos_1_Price_cum = slider::slide_dbl(.x = lagged_Pos_1_Price,
                                                   .f = ~ sum(.x, na.rm = T),
                                                   .before = MA_period_1),

        lagged_Pos_2_Price_cum = slider::slide_dbl(.x = lagged_Pos_2_Price,
                                                   .f = ~ sum(.x, na.rm = T),
                                                   .before = MA_period_2),

        lagged_Pos_3_Price_cum = slider::slide_dbl(.x = lagged_Pos_3_Price,
                                                   .f = ~ sum(.x, na.rm = T),
                                                   .before = MA_period_3),

        lagged_Pos_4_Price_cum = slider::slide_dbl(.x = lagged_Pos_4_Price,
                                                   .f = ~ sum(.x, na.rm = T),
                                                   .before = MA_period_4),

        lagged_Pos_5_Price_cum = slider::slide_dbl(.x = lagged_Pos_5_Price,
                                                   .f = ~ sum(.x, na.rm = T),
                                                   .before = MA_period_5),

        lagged_Pos_6_Price_cum = slider::slide_dbl(.x = lagged_Pos_6_Price,
                                                   .f = ~ sum(.x, na.rm = T),
                                                   .before = MA_period_6),

        lagged_Pos_7_Price_cum = slider::slide_dbl(.x = lagged_Pos_7_Price,
                                                   .f = ~ sum(.x, na.rm = T),
                                                   .before = MA_period_7),

        lagged_Pos_8_Price_cum = slider::slide_dbl(.x = lagged_Pos_8_Price,
                                                   .f = ~ sum(.x, na.rm = T),
                                                   .before = MA_period_8),

        lagged_Pos_Neg_1_Price_cum = slider::slide_dbl(.x = lagged_Pos_Neg_1_Price,
                                                       .f = ~ sum(.x, na.rm = T),
                                                       .before = MA_period_1),

        lagged_Pos_Neg_2_Price_cum = slider::slide_dbl(.x = lagged_Pos_Neg_2_Price,
                                                       .f = ~ sum(.x, na.rm = T),
                                                       .before = MA_period_2),

        lagged_Pos_Neg_3_Price_cum = slider::slide_dbl(.x = lagged_Pos_Neg_3_Price,
                                                       .f = ~ sum(.x, na.rm = T),
                                                       .before = MA_period_3),

        lagged_Pos_Neg_4_Price_cum = slider::slide_dbl(.x = lagged_Pos_Neg_4_Price,
                                                       .f = ~ sum(.x, na.rm = T),
                                                       .before = MA_period_4),

        lagged_Pos_Neg_5_Price_cum = slider::slide_dbl(.x = lagged_Pos_Neg_5_Price,
                                                       .f = ~ sum(.x, na.rm = T),
                                                       .before = MA_period_5),

        lagged_Pos_Neg_6_Price_cum = slider::slide_dbl(.x = lagged_Pos_Neg_6_Price,
                                                       .f = ~ sum(.x, na.rm = T),
                                                       .before = MA_period_6),

        lagged_Pos_Neg_7_Price_cum = slider::slide_dbl(.x = lagged_Pos_Neg_7_Price,
                                                       .f = ~ sum(.x, na.rm = T),
                                                       .before = MA_period_7),

        lagged_Pos_Neg_8_Price_cum = slider::slide_dbl(.x = lagged_Pos_Neg_8_Price,
                                                       .f = ~ sum(.x, na.rm = T),
                                                       .before = MA_period_8)
      ) %>%
      mutate(
        lagged_Pos_Multi_Flag_1 =
          ifelse(lagged_Price > 0 & lag(lagged_Price) > 0 & lag(lagged_Price, 2) > 0,
                 1,
                 0),
        lagged_Pos_Multi_Flag_2 =
          ifelse(lagged_Price2 > 0 & lag(lagged_Price2) > 0 & lag(lagged_Price2, 2) > 0,
                 1,
                 0),
        lagged_Pos_Multi_Flag_3 =
          ifelse(lagged_Price3 > 0 & lag(lagged_Price3) > 0 & lag(lagged_Price3, 2) > 0,
                 1,
                 0),
        lagged_Pos_Multi_Flag_4 =
          ifelse(lagged_Price4 > 0 & lag(lagged_Price4) > 0 & lag(lagged_Price4, 2) > 0,
                 1,
                 0),
        lagged_Pos_Multi_Flag_5 =
          ifelse(lagged_Price5 > 0 & lag(lagged_Price5) > 0 & lag(lagged_Price5, 2) > 0,
                 1,
                 0),
        lagged_Pos_Multi_Flag_6 =
          ifelse(lagged_Price6 > 0 & lag(lagged_Price6) > 0 & lag(lagged_Price6, 2) > 0,
                 1,
                 0),

        lagged_Pos_Multi_Flag_1_cum =
          slider::slide_dbl(.x = lagged_Pos_Multi_Flag_1, .f = ~ sum(.x, na.rm = T), .before = MA_period_1),
        lagged_Pos_Multi_Flag_2_cum =
          slider::slide_dbl(.x = lagged_Pos_Multi_Flag_2, .f = ~ sum(.x, na.rm = T), .before = MA_period_2),
        lagged_Pos_Multi_Flag_3_cum =
          slider::slide_dbl(.x = lagged_Pos_Multi_Flag_3, .f = ~ sum(.x, na.rm = T), .before = MA_period_3),
        lagged_Pos_Multi_Flag_4_cum =
          slider::slide_dbl(.x = lagged_Pos_Multi_Flag_4, .f = ~ sum(.x, na.rm = T), .before = MA_period_4),
        lagged_Pos_Multi_Flag_5_cum =
          slider::slide_dbl(.x = lagged_Pos_Multi_Flag_5, .f = ~ sum(.x, na.rm = T), .before = MA_period_5),

        lagged_max_point_1 =
          slider::slide_dbl(.x= lag(High, 1), .f = ~ max(.x, na.rm  = T), .before = lag_value_1 + MA_period_1),
        lagged_min_point_1 =
          slider::slide_dbl(.x= lag(Low, 1), .f = ~ min(.x, na.rm  = T), .before = lag_value_1 + MA_period_1),
        lagged_max_minus_min_1 = lagged_max_point_1 - lagged_min_point_1,
        lagged_max_minus_Price_1 = lagged_max_point_1 - lag(Price, 1),
        lagged_min_minus_Price_1 = lag(Price, 1) - lagged_min_point_1,

        lagged_max_point_2 =
          slider::slide_dbl(.x= lag(High, 1), .f = ~ max(.x, na.rm  = T), .before = lag_value_2 + MA_period_2),
        lagged_min_point_2 =
          slider::slide_dbl(.x= lag(Low, 1), .f = ~ min(.x, na.rm  = T), .before = lag_value_2 + MA_period_2),
        lagged_max_minus_min_2 = lagged_max_point_2 - lagged_min_point_2,
        lagged_max_minus_Price_2 = lagged_max_point_2 - lag(Price, 1),
        lagged_min_minus_Price_2 = lag(Price, 1) - lagged_min_point_2,

        lagged_max_point_3 =
          slider::slide_dbl(.x= lag(High, 1), .f = ~ max(.x, na.rm  = T), .before = lag_value_3 + MA_period_3),
        lagged_min_point_3 =
          slider::slide_dbl(.x= lag(Low, 1), .f = ~ min(.x, na.rm  = T), .before = lag_value_3 + MA_period_3),
        lagged_max_minus_min_3 = lagged_max_point_3 - lagged_min_point_3,
        lagged_max_minus_Price_3 = lagged_max_point_3 - lag(Price, 1),
        lagged_min_minus_Price_3 = lag(Price, 1) - lagged_min_point_3,

        lagged_max_point_4 =
          slider::slide_dbl(.x= lag(High, 1), .f = ~ max(.x, na.rm  = T), .before = lag_value_4 + MA_period_4),
        lagged_min_point_4 =
          slider::slide_dbl(.x= lag(Low, 1), .f = ~ min(.x, na.rm  = T), .before = lag_value_4 + MA_period_4),
        lagged_max_minus_min_4 = lagged_max_point_4 - lagged_min_point_4,
        lagged_max_minus_Price_4 = lagged_max_point_4 - lag(Price, 1),
        lagged_min_minus_Price_4 = lag(Price, 1) - lagged_min_point_4,

        lagged_max_point_5 =
          slider::slide_dbl(.x= lag(High, 1), .f = ~ max(.x, na.rm  = T), .before = lag_value_5 + MA_period_5),
        lagged_min_point_5 =
          slider::slide_dbl(.x= lag(Low, 1), .f = ~ min(.x, na.rm  = T), .before = lag_value_5 + MA_period_5),
        lagged_max_minus_min_5 = lagged_max_point_5 - lagged_min_point_5,
        lagged_max_minus_Price_5 = lagged_max_point_5 - lag(Price, 1),
        lagged_min_minus_Price_5 = lag(Price, 1) - lagged_min_point_5,

        # lagged_perc_estimate_1_High =
        #   lagged_Price/(MA_High_3),
        # lagged_perc_estimate_2 =
        #   lagged_Price2/(MA_High_4),
        # lagged_perc_estimate_3 =
        #   lagged_Price3/(MA_High_5),
        # lagged_perc_estimate_4 =
        #   lagged_Price4/(MA_High_6),
        # lagged_perc_estimate_5 =
        #   lagged_Price5/(MA_High_7),
        #
        # lagged_perc_estimate_1_Low =
        #   lagged_Price/(MA_Low_3),
        # lagged_perc_estimate_2 =
        #   lagged_Price2/(MA_Low_4),
        # lagged_perc_estimate_3 =
        #   lagged_Price3/(MA_Low_5),
        # lagged_perc_estimate_4 =
        #   lagged_Price4/(MA_Low_6),
        # lagged_perc_estimate_5 =
        #   lagged_Price5/(MA_Low_7)
      )

    return(returned_data)

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

#' Single_Asset_V3_Macro_Gen_Model
#'
#' @param AR_model_data
#' @param asset_of_interest
#' @param actual_wins_losses_asset
#' @param period_of_analysis
#' @param training_end_date
#' @param bin_threshold
#' @param sig_thresh
#'
#' @return
#' @export
#'
#' @examples
Single_Asset_V3_Macro_Gen_Model <-
  function(
    macro_model_data = macro_model_data,
    asset_of_interest = asset_of_interest,
    actual_wins_losses_asset = actual_wins_losses_asset,
    period_of_analysis = actuals_periods_needed[1],
    training_end_date = training_end_date,
    bin_threshold = bin_threshold,
    sig_thresh = 0.15,
    base_path = "C:/Users/nikhi/Documents/trade_data/Day_Trader_Single_Asset_V3_Expanded_Models/"
  ) {

    joined_data <-
      macro_model_data %>%
      left_join(
        actual_wins_losses_asset %>%
          filter(Asset == asset_of_interest) %>%
          dplyr::select(Date, Asset, !!as.name(period_of_analysis))
      ) %>%
      filter(
        Date <= training_end_date
      ) %>%
      mutate(
        bin_var = ifelse( !!as.name(period_of_analysis) >= bin_threshold, 1, 0)
      )

    dependants <-
      names(joined_data) %>%
      keep(~ str_detect(.x, "GBP|AUD|JPY|USD|EUR|CAD|NZD|CNY|CHF"))

    lm_form <-
      create_lm_formula(dependant = period_of_analysis, independant = dependants)

    LM_model <- lm(formula = lm_form, data = joined_data)

    sig_coefs <- get_sig_coefs(LM_model, p_value_thresh_for_inputs = sig_thresh)

    lm_form <-
      create_lm_formula(dependant = period_of_analysis, independant = sig_coefs)

    LM_model <- lm(formula = lm_form, data = joined_data)

    saveRDS(LM_model,
            glue::glue("{base_path}/LM_Macro_{period_of_analysis}_{asset_of_interest}.RDS")
    )

    rm(LM_model)

    dependants <-
      names(joined_data) %>%
      keep(~ str_detect(.x, "GBP|AUD|JPY|USD|EUR|CAD|NZD|CNY|CHF"))

    Glm_form <-
      create_lm_formula(dependant = "bin_var", independant = dependants)

    GLM_model <- glm(formula = Glm_form, data = joined_data, family = binomial("logit"))

    sig_coefs <- get_sig_coefs(GLM_model, p_value_thresh_for_inputs = sig_thresh)

    Glm_form <-
      create_lm_formula(dependant = "bin_var", independant = sig_coefs)

    GLM_model <- glm(formula = Glm_form, data = joined_data, family = binomial("logit"))

    saveRDS(GLM_model,
            glue::glue("{base_path}/GLM_Macro_{period_of_analysis}_{asset_of_interest}.RDS")
    )

  }

#' Single_Asset_V3_macro_read_Model
#'
#' @param state_space_data
#' @param asset_of_interest
#' @param period_of_analysis
#' @param training_end_date
#' @param roll_mean_period
#'
#' @return
#' @export
#'
#' @examples
Single_Asset_V3_macro_read_Model <-
  function(
    macro_model_data = macro_model_data,
    asset_of_interest = asset_of_interest,
    period_of_analysis = actuals_periods_needed[1],
    training_end_date = training_end_date,
    roll_mean_period = 100,
    base_path = "C:/Users/nikhi/Documents/trade_data/Day_Trader_Single_Asset_V3_Expanded_Models/"
  ) {

    LM_model <-
      readRDS(
        glue::glue("{base_path}/LM_Macro_{period_of_analysis}_{asset_of_interest}.RDS")
      )

    preds_all <- predict.lm(object = LM_model, newdata = macro_model_data)

    GLM_model <-
      readRDS(
        glue::glue("{base_path}/GLM_Macro_{period_of_analysis}_{asset_of_interest}.RDS")
      )

    preds_all_GLM <- predict(object = GLM_model, newdata = macro_model_data, type = "response")

    complete_Macro_data <-
      macro_model_data %>%
      filter(Asset == asset_of_interest) %>%
      distinct(Date, Asset) %>%
      mutate(
        !!as.name(glue::glue("Macro_LM_Pred_{period_of_analysis}")) := preds_all,
        !!as.name(glue::glue("Macro_GLM_Pred_{period_of_analysis}")) := preds_all_GLM
      ) %>%
      mutate(
        !!as.name(glue::glue("Macro_LM_Pred_{period_of_analysis}_mean")) :=
          slider::slide_dbl(.x =
                              !!as.name(glue::glue("Macro_LM_Pred_{period_of_analysis}")),
                            .f = ~ mean(.x, na.rm = T),
                            .before = roll_mean_period),
        !!as.name(glue::glue("Macro_LM_Pred_{period_of_analysis}_sd")) :=
          slider::slide_dbl(.x =
                              !!as.name(glue::glue("Macro_LM_Pred_{period_of_analysis}")),
                            .f = ~ sd(.x, na.rm = T),
                            .before = roll_mean_period),

        !!as.name(glue::glue("Macro_GLM_Pred_{period_of_analysis}_mean")) :=
          slider::slide_dbl(.x =
                              !!as.name(glue::glue("Macro_GLM_Pred_{period_of_analysis}")),
                            .f = ~ mean(.x, na.rm = T),
                            .before = roll_mean_period),
        !!as.name(glue::glue("Macro_GLM_Pred_{period_of_analysis}_sd")) :=
          slider::slide_dbl(.x =
                              !!as.name(glue::glue("Macro_GLM_Pred_{period_of_analysis}")),
                            .f = ~ sd(.x, na.rm = T),
                            .before = roll_mean_period)
      )

    testing_data <-
      complete_Macro_data %>%
      filter(Date > training_end_date)

    training_data <-
      complete_Macro_data %>%
      filter(Date <= training_end_date)

    return(list("testing_data" = testing_data, "training_data" = training_data) )

  }

#' single_asset_v3_gen_macro_Model
#'
#' @param asset_data_macro
#' @param actual_wins_losses
#' @param asset_of_interest
#' @param actuals_periods_needed
#' @param training_end_date
#' @param bin_threshold
#' @param rolling_mean_pred_period
#' @param sig_thresh
#' @param raw_macro_data
#' @param base_path
#'
#' @returns
#' @export
#'
#' @examples
single_asset_v3_gen_macro_Model <-
  function(
    asset_data_macro = Indices_Metals_Bonds[[1]] %>% filter(Asset == "EUR_USD"),
    actual_wins_losses,
    asset_of_interest = "EUR_USD",
    actuals_periods_needed = c("period_return_24_Price", "period_return_35_Price", "period_return_46_Price"),
    training_end_date = "2025-05-01",
    bin_threshold = 5,
    rolling_mean_pred_period = 500,
    sig_thresh = 0.15,
    raw_macro_data = raw_macro_data,
    base_path = "C:/Users/nikhi/Documents/trade_data/Day_Trader_Single_Asset_V3_Expanded_Models/"
  ) {

    asset_data = asset_data_macro %>% filter(Asset == asset_of_interest)
    actual_wins_losses_asset <-
      actual_wins_losses %>% filter(Asset == asset_of_interest)

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

    for (i in 1:length(actuals_periods_needed)) {
      Single_Asset_V3_Macro_Gen_Model(
        macro_model_data = macro_model_data,
        asset_of_interest = asset_of_interest,
        actual_wins_losses_asset = actual_wins_losses_asset,
        period_of_analysis = actuals_periods_needed[i],
        training_end_date = training_end_date,
        bin_threshold = bin_threshold,
        sig_thresh = sig_thresh,
        base_path = base_path
      )
    }

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

    return(
      list(
        "training_data" = Macro_Train_Preds_mean,
        "testing_data" = Macro_Test_Preds
      )
    )

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


#' construct_Performance_to_Thresh_Curve
#'
#' @param pred_data
#' @param pred_col
#' @param actual_wins_losses
#' @param thresh_vector
#' @param period_return_col
#' @param sim_start_date
#'
#' @return
#' @export
#'
#' @examples
construct_Performance_to_Thresh_Curve <-
  function(
    pred_data = generated_preds_from_db,
    pred_col = "AR_LM_Pred_period_return_46_Price",
    actual_wins_losses = actual_wins_losses,
    thresh_vector = c(0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
    period_return_col = "period_return_35_Price",
    sim_start_date = "2023-01-01"
  ) {

    test_performance <-
      pred_data %>%
      filter(Date >= as_datetime(sim_start_date)) %>%
      left_join(
        actual_wins_losses %>%
          dplyr::select(Date, Asset, !!as.name(period_return_col))
      )

    control_data <-
      test_performance %>%
      group_by(Date) %>%
      summarise(
        !!as.name(period_return_col) := sum(!!as.name(period_return_col), na.rm = T)
      ) %>%
      ungroup() %>%
      arrange(Date) %>%
      mutate(
        cumulative_return = cumsum(!!as.name(period_return_col))
      ) %>%
      mutate(
        trade_col = "Control"
      ) %>%
      mutate(
        win_loss = ifelse(!!as.name(period_return_col) > 0, 1, 0)
      )

    control_win_loss_summary <-
      control_data %>%
      summarise(
        wins = sum(win_loss, na.rm = T) ,
        total_trades = n(),
        average_win = mean(
          ifelse( !!as.name(period_return_col) > 0,!!as.name(period_return_col), NA  ),
          na.rm = T),
        average_loss = mean(
          ifelse( !!as.name(period_return_col) <= 0,!!as.name(period_return_col), NA  ),
          na.rm = T),
        Final_Winnings = sum(!!as.name(period_return_col)),

        Return_25 = quantile(!!as.name(period_return_col), 0.25 ,na.rm = T),
        Return_Middle = median(!!as.name(period_return_col), na.rm = T),
        Return_75 = quantile(!!as.name(period_return_col), 0.75 ,na.rm = T),
        Ratio_of_25_to_75 = abs(Return_75)/abs(Return_25),
        Return_SD = sd(!!as.name(period_return_col), na.rm = T)
      ) %>%
      mutate(
        required_adjustment_var =
          ifelse( (100 - total_trades) < 0, 0,  (100 - total_trades)),
        Perc_UnAdj = (wins )/(total_trades),
        Perc_Adj = (wins)/(total_trades + required_adjustment_var),
        Binomial_Expectation_Adj = (Perc_Adj*average_win) + (average_loss*(1 - Perc_Adj)),
        Binomial_Expectation_Adj_1000 = (1000*Perc_Adj*average_win) + (1000*average_loss*(1 - Perc_Adj))
      ) %>%
      mutate(
        trade_col = "Control"
      )

    random_testing_perc <- numeric()
    random_testing_return <- numeric()

    for (j in 1:3000) {
      random_testing_perc[j] <-
        sum(control_data$win_loss %>% sample(size = 480), na.rm = T)/480
      random_testing_return[j] <-
        sum(control_data %>% pull(!!as.name(period_return_col)) %>% sample(size = 480), na.rm = T)
    }

    control_win_loss_summary <-
      control_win_loss_summary %>%
      mutate(
        random_returns_mid = mean(random_testing_return, na.rm = T),
        random_returns_05 = quantile(random_testing_return, 0.05 , na.rm = T),
        random_returns_25 = quantile(random_testing_return, 0.25 , na.rm = T),
        random_returns_75 = quantile(random_testing_return, 0.75 , na.rm = T),
        random_returns_sd = sd(random_testing_return, na.rm = T),

        random_perc_mid = mean(random_testing_perc, na.rm = T),
        random_perc_05 = quantile(random_testing_perc, 0.05 , na.rm = T),
        random_perc_25 = quantile(random_testing_perc, 0.25 , na.rm = T),
        random_perc_75 = quantile(random_testing_perc, 0.75 , na.rm = T),
        random_perc_sd = sd(random_testing_perc, na.rm = T)
      )

    rm(test_performance)

    Trade_win_loss_summary <- list()

    for (i in 1:length(thresh_vector)) {

      trade_statement <-
        glue::glue("{pred_col} >= {thresh_vector[i]}")

      total_periods = as.numeric(control_win_loss_summary$total_trades[1])

      test_performance <-
        pred_data %>%
        filter(Date >= as_datetime(sim_start_date)) %>%
        left_join(
          actual_wins_losses %>%
            dplyr::select(Date, Asset,!!as.name(period_return_col))
        ) %>%
        mutate(
          trade_col =
            eval(parse(text = trade_statement))
        ) %>%
        mutate(
          trade_col =
            case_when(
              trade_col == TRUE ~ "Long"
            )
        )

      Trade_Data <-
        test_performance %>%
        filter(trade_col == "Long") %>%
        group_by(Date) %>%
        summarise(
          !!as.name(period_return_col) := sum(!!as.name(period_return_col), na.rm = T)
        ) %>%
        ungroup() %>%
        arrange(Date) %>%
        mutate(
          cumulative_return := cumsum(!!as.name(period_return_col))
        ) %>%
        mutate(
          trade_col = "Long"
        ) %>%
        mutate(
          win_loss = ifelse(!!as.name(period_return_col) > 0, 1, 0)
        )

      Trade_win_loss_summary[[i]] <-
        Trade_Data %>%
        summarise(
          wins = sum(win_loss, na.rm = T) ,
          total_trades = n(),
          average_win = mean(
            ifelse( !!as.name(period_return_col) > 0,!!as.name(period_return_col), NA  ),
            na.rm = T),
          average_loss = mean(
            ifelse( !!as.name(period_return_col) <= 0,!!as.name(period_return_col), NA  ),
            na.rm = T),
          Final_Winnings = sum(!!as.name(period_return_col)),
          Detection_Perc = total_trades/total_periods,

          Return_25 = quantile(!!as.name(period_return_col), 0.25 ,na.rm = T),
          Return_Middle = median(!!as.name(period_return_col), na.rm = T),
          Return_75 = quantile(!!as.name(period_return_col), 0.75 ,na.rm = T),
          Ratio_of_25_to_75 = abs(Return_75)/abs(Return_25),
          Return_SD = sd(!!as.name(period_return_col), na.rm = T)
        ) %>%
        mutate(
          required_adjustment_var =
            ifelse( (100 - total_trades) < 0, 0,  (100 - total_trades)),
          Perc_UnAdj = (wins )/(total_trades),
          Perc_Adj = (wins)/(total_trades + (required_adjustment_var) ),
          Binomial_Expectation_Adj = (Perc_Adj*average_win) + (average_loss*(1 - Perc_Adj)),
          Binomial_Expectation_Adj_1000 = (1000*Perc_Adj*average_win) + (1000*average_loss*(1 - Perc_Adj)),
          Binomial_Expectation_Adj_1000_Detected = Detection_Perc*Binomial_Expectation_Adj_1000
        ) %>%
        mutate(
          trade_col = "Long",
          threshold = thresh_vector[i]
        )

      random_testing_perc <- numeric()
      random_testing_return <- numeric()
      required_sample_length <-
        round(length(Trade_Data$win_loss)/20)

      if(required_sample_length >= 50) {

        for (j in 1:3000) {
          random_testing_perc[j] <-
            sum(Trade_Data$win_loss %>% sample(size = required_sample_length), na.rm = T)/required_sample_length
          random_testing_return[j] <-
            sum(Trade_Data %>% pull(!!as.name(period_return_col)) %>% sample(size = required_sample_length), na.rm = T)
        }

        random_returns_mid = mean(random_testing_return, na.rm = T)
        random_returns_05 = quantile(random_testing_return, 0.05 , na.rm = T)
        random_returns_25 = quantile(random_testing_return, 0.25 , na.rm = T)
        random_returns_75 = quantile(random_testing_return, 0.75 , na.rm = T)
        random_returns_sd = sd(random_testing_return, na.rm = T)

        random_perc_mid = mean(random_testing_perc, na.rm = T)
        random_perc_05 = quantile(random_testing_perc, 0.05 , na.rm = T)
        random_perc_25 = quantile(random_testing_perc, 0.25 , na.rm = T)
        random_perc_75 = quantile(random_testing_perc, 0.75 , na.rm = T)
        random_perc_sd = sd(random_testing_perc, na.rm = T)

        Trade_win_loss_summary[[i]] <-
          Trade_win_loss_summary[[i]] %>%
          mutate(
            random_returns_mid = random_returns_mid,
            random_returns_05 = random_returns_05,
            random_returns_25 = random_returns_25,
            random_returns_75 = random_returns_75,
            random_returns_sd = random_returns_sd,

            random_perc_mid = random_perc_mid,
            random_perc_05 = random_perc_05,
            random_perc_25 = random_perc_25,
            random_perc_75 = random_perc_75,
            random_perc_sd = random_perc_sd
          )

      }

    }

    Trade_win_loss_summary_dfr <-
      Trade_win_loss_summary %>%
      map_dfr(bind_rows) %>%
      bind_rows(control_win_loss_summary)

    return(Trade_win_loss_summary_dfr)

  }


#' construct_time_series
#'
#' @param actual_wins_losses
#' @param pred_data
#' @param Asset_Var
#' @param trade_statement
#' @param trade_direction
#' @param win_thresh
#'
#' @returns
#' @export
#'
#' @examples
construct_time_series <-
  function(
    actual_wins_losses = actual_wins_losses,
    pred_data = testing_pred_data,
    Asset_Var = "EUR_USD",
    trade_statement = "state_space_LM_Pred_period_return_50_Price >= 2",
    trade_direction = "Long",
    win_thresh = 0
  ) {


    tagged_trade_combined <-
      pred_data %>%
      ungroup() %>%
      filter(Asset == Asset_Var) %>%
      mutate(
        trade_col =
          eval(parse(text = trade_statement)),
        trade_col =
          ifelse(trade_col == TRUE, trade_direction, paste0("No Trade ", trade_direction) )
      )  %>%
      dplyr::select(Date, Asset,trade_col,
                    -contains("period_return_") ) %>%
      distinct() %>%
      left_join(actual_wins_losses %>%
                  dplyr::select(Date, Asset, trade_col,  contains("period_return_")) %>%
                  filter(Asset == Asset_Var) %>%
                  filter(trade_col == trade_direction) %>%
                  dplyr::select(-trade_col) %>%
                  dplyr::select(Date, Asset,  contains("period_return_")) %>%
                  distinct()
      ) %>%
      pivot_longer(-c(Date, Asset, trade_col),
                   values_to = "Returns", names_to = "Period") %>%
      mutate(
        Period = str_remove_all(Period, "[A-Z]+|[a-z]+|_") %>% str_trim() %>% as.numeric()
      )

    summary_data <-
      tagged_trade_combined %>%
      filter(!is.na(trade_col)) %>%
      mutate(
        wins = ifelse(
          Returns > win_thresh,
          1,
          0
        )
      ) %>%
      group_by(Asset, trade_col, Period) %>%
      summarise(
        total_trades = n_distinct(Date),
        wins = sum(wins, na.rm = T),
        Total_Returns = sum(Returns, na.rm = T),
        Average_Return = mean(Returns, na.rm = T),
        Return_25 = quantile(Returns, 0.25, na.rm = T),
        Return_75 = quantile(Returns, 0.75, na.rm = T)

      ) %>%
      ungroup() %>%
      mutate(
        perc =wins/total_trades
      ) %>%
      mutate(
        trade_statement = trade_statement
      )


    portfolio_ts_long <-
      tagged_trade_combined %>%
      filter(!is.na(trade_col)) %>%
      filter(trade_col == "Long") %>%
      group_by(trade_col, Period) %>%
      arrange(Date, .by_group = TRUE) %>%
      group_by(trade_col, Period) %>%
      mutate(
        Total_Returns_cumulative =
          cumsum(Returns)
      )

    portfolio_ts_control <-
      tagged_trade_combined %>%
      group_by(Period) %>%
      arrange(Date, .by_group = TRUE) %>%
      group_by(Period) %>%
      mutate(
        trade_col = "Control",
        Total_Returns_cumulative =
          cumsum(Returns)
      )

    portfolio_ts <-
      portfolio_ts_long %>%
      bind_rows(portfolio_ts_control)

    return(portfolio_ts)

  }
