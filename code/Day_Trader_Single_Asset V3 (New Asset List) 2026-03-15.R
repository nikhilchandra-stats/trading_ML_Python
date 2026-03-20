helpeR::load_custom_functions()

all_aud_symbols <- get_oanda_symbols() %>%
  keep(~ str_detect(.x, "AUD")|str_detect(.x, "USD_SEK|USD_NOK|USD_HUF|USD_ZAR|USD_CNY|USD_MXN"))
asset_infor <- get_instrument_info()
aud_assets <- read_all_asset_data_intra_day(
  asset_list_oanda = all_aud_symbols,
  save_path_oanda_assets = "C:/Users/Nikhil Chandra/Documents/trade_data//oanda_data/",
  read_csv_or_API = "API",
  time_frame = "D",
  bid_or_ask = "bid",
  how_far_back = 10,
  start_date = (today() - days(7)) %>% as.character()
)
aud_assets <- aud_assets %>% map_dfr(bind_rows)
aud_usd_today <- get_aud_conversion(asset_data_daily_raw = aud_assets)

currency_conversion <-
  aud_usd_today %>%
  mutate(
    not_aud_asset = ending_value
  ) %>%
  dplyr::select(not_aud_asset, adjusted_conversion) %>%
  bind_rows(
    tibble(not_aud_asset = "AUD", adjusted_conversion = 1)
  )

asset_list_oanda =
  c("HK33_HKD", "USD_JPY",
    "BTC_USD",
    "AUD_NZD", "GBP_CHF",
    "EUR_HUF", "EUR_ZAR", "NZD_JPY", "EUR_NZD",
    "USB02Y_USD",
    "XAU_CAD", "GBP_JPY", "EUR_NOK", "USD_SGD", "EUR_SEK",
    "DE30_EUR",
    "AUD_CAD",
    "UK10YB_GBP",
    "XPD_USD",
    "UK100_GBP", "NZD_USD",
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
    "USB02Y_USD", "USB05Y_USD", "USB30Y_USD", "USB10Y_USD", "UK100_GBP") %>%
  unique()

asset_infor <- get_instrument_info()
raw_macro_data <- get_macro_event_data()
#---------------------Data
load_custom_functions()
db_location =  "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data_Most_Assets_2025-09-13 2.db"
start_date = "2018-01-01"
end_date = today() %>% as.character()

bin_factor = NULL
stop_value_var = 5
profit_value_var = 50
period_var = 24

Indices_Metals_Bonds <- list()

Indices_Metals_Bonds[[1]] <-
  get_db_data_quickly_algo(
    db_location = db_location,
    start_date = start_date,
    end_date = as.character(today() + days(30)),
    time_frame = "H1",
    bid_or_ask = "ask",
    assets =   c("EUR_CHF" , "EUR_SEK" , "GBP_CHF", "GBP_JPY",
                   "USD_CZK", "USD_NOK" , "XAG_CAD", "XAG_CHF",
                   "XAG_JPY" , "GBP_NZD" , "NZD_CHF" , "USD_MXN",
                   "XPD_USD","XPT_USD","NATGAS_USD","SG30_SGD" ,
                   "SOYBN_USD", "WHEAT_USD", "SUGAR_USD" ,"DE30_EUR" ,
                   "UK10YB_GBP","JP225_USD","CH20_CHF","NL25_EUR" ,
                   "XAG_SGD", "BCH_USD", "LTC_USD" , "EUR_USD" ,
                   "EU50_EUR","SPX500_USD" , "US2000_USD" , "USB10Y_USD" ,
                   "USD_JPY" , "AUD_USD" , "XAG_USD" , "XAG_EUR" ,
                   "BTC_USD" , "XAU_USD" , "XAU_EUR" , "GBP_USD" , "USD_CAD" ,
                   "USD_SEK" , "EUR_AUD" , "GBP_AUD" , "XAG_GBP" ,"XAU_GBP" ,
                   "EUR_JPY" , "XAU_SGD" , "XAU_CAD" , "NZD_USD" , "XAU_NZD" ,
                   "XAG_NZD" , "FR40_EUR" , "UK100_GBP" , "AU200_AUD" ,
                   "HK33_HKD" , "SG30_SGD" , "US2000_USD" , "XAG_AUD" ,
                   "XAU_AUD" , "XAU_JPY" , "USB02Y_USD" , "USD_SGD" , "XAU_CHF")
  ) %>%
  distinct()
Indices_Metals_Bonds[[2]] <-
  get_db_data_quickly_algo(
    db_location = db_location,
    start_date = start_date,
    end_date = as.character(today() + days(30)),
    time_frame = "H1",
    bid_or_ask = "bid",
    assets =   c("EUR_CHF" , "EUR_SEK" , "GBP_CHF", "GBP_JPY",
                 "USD_CZK", "USD_NOK" , "XAG_CAD", "XAG_CHF",
                 "XAG_JPY" , "GBP_NZD" , "NZD_CHF" , "USD_MXN",
                 "XPD_USD","XPT_USD","NATGAS_USD","SG30_SGD" ,
                 "SOYBN_USD", "WHEAT_USD", "SUGAR_USD" ,"DE30_EUR" ,
                 "UK10YB_GBP","JP225_USD","CH20_CHF","NL25_EUR" ,
                 "XAG_SGD", "BCH_USD", "LTC_USD" , "EUR_USD" ,
                 "EU50_EUR","SPX500_USD" , "US2000_USD" , "USB10Y_USD" ,
                 "USD_JPY" , "AUD_USD" , "XAG_USD" , "XAG_EUR" ,
                 "BTC_USD" , "XAU_USD" , "XAU_EUR" , "GBP_USD" , "USD_CAD" ,
                 "USD_SEK" , "EUR_AUD" , "GBP_AUD" , "XAG_GBP" ,"XAU_GBP" ,
                 "EUR_JPY" , "XAU_SGD" , "XAU_CAD" , "NZD_USD" , "XAU_NZD" ,
                 "XAG_NZD" , "FR40_EUR" , "UK100_GBP" , "AU200_AUD" ,
                 "HK33_HKD" , "SG30_SGD" , "US2000_USD" , "XAG_AUD" ,
                 "XAU_AUD" , "XAU_JPY" , "USB02Y_USD" , "USD_SGD" , "XAU_CHF")
  ) %>%
  distinct()

actual_wins_losses <-
  get_actual_wins_losses(
    assets_to_analyse =
      c(
        "WHEAT_USD", #1 WHEAT_USD
        "SUGAR_USD", #2 SUGAR_USD
        "DE30_EUR", #3 DE30_EUR
        "UK10YB_GBP", #4 UK10YB_GBP
        "EUR_CHF", #5 EUR_CHF
        "EUR_SEK", #6 EUR_SEK
        "GBP_CHF", #7 GBP_CHF
        "GBP_JPY", #8 GBP_JPY
        "USD_CZK",  #9 USD_CZK
        "USD_NOK", #10 USD_NOK
        "XAG_CAD",  #11 XAG_CAD
        "XAG_CHF",  #12 XAG_CHF
        "XAG_JPY",   #13 XAG_JPY
        "GBP_NZD", #14 GBP_NZD
        "NZD_CHF" #15 NZD_CHF
      ),
    asset_data = Indices_Metals_Bonds,
    stop_factor = 10,
    profit_factor = 50,
    risk_dollar_value = 10,
    trade_direction = "Long",
    currency_conversion = currency_conversion,
    asset_infor = asset_infor,
    periods_ahead = period_var
  )


# source("C:/Users/nikhi/Documents/Repos/trading_ML_Python/code/Temp From Work Pc/Single_Asset_V3_Funcs.R")
load_custom_functions()
assets_to_test <- c(
  "WHEAT_USD", #1 WHEAT_USD
  "SUGAR_USD", #2 SUGAR_USD
  "DE30_EUR", #3 DE30_EUR
  "UK10YB_GBP", #4 UK10YB_GBP
  "EUR_CHF", #5 EUR_CHF
  "EUR_SEK", #6 EUR_SEK
  "GBP_CHF", #7 GBP_CHF
  "GBP_JPY", #8 GBP_JPY
  "USD_CZK",  #9 USD_CZK
  "USD_NOK", #10 USD_NOK
  "XAG_CAD",  #11 XAG_CAD
  "XAG_CHF",  #12 XAG_CHF
  "XAG_JPY",   #13 XAG_JPY
  "GBP_NZD", #14 GBP_NZD
  "NZD_CHF", #15 NZD_CHF
  "USD_MXN",  #16 USD_MXN
  "CH20_CHF" #17 CH20_CHF
)

correlation_asset_list <-
  list(

    c(
      "USD_NOK", "EUR_USD", "USD_JPY", "AUD_USD", "XAG_USD", "XAU_USD", "GBP_USD",
      "USD_CAD", "USD_SEK", "NZD_USD", "NATGAS_USD", "XPT_USD", "USB10Y_USD", "SOYBN_USD",
      "SUGAR_USD","SPX500_USD", "US2000_USD"
    ) %>% unique(), #1 WHEAT_USD #####HERE

    c(
      "USD_NOK", "EUR_USD", "USD_JPY", "AUD_USD", "XAG_USD", "XAU_USD", "GBP_USD",
      "USD_CAD", "NZD_USD", "NATGAS_USD", "XPT_USD", "USB10Y_USD", "SOYBN_USD",
      "WHEAT_USD","SPX500_USD", "US2000_USD"
    ) %>% unique(), #2 SUGAR_USD

    c(
      "USB10Y_USD", "USD_SGD", "XAU_USD", "XAG_EUR", "AU200_AUD", "US2000_USD", "SPX500_USD",
      "NL25_EUR", "NL25_EUR", "FR40_EUR", "EU50_EUR", "JP225_USD", "XAG_USD",
      "CH20_CHF", "XAU_EUR", "EUR_USD"
    ) %>% unique(), #3 DE30_EUR

    c(
      "XAG_GBP", "AU200_AUD", "US2000_USD", "SPX500_USD",
      "NL25_EUR", "NL25_EUR", "FR40_EUR", "EU50_EUR", "JP225_USD", "XAG_USD",
      "CH20_CHF", "XAU_GBP", "GBP_USD", "UK100_GBP"
    ) %>% unique(), #4 UK10YB_GBP

    c(
      "EUR_SEK", "DE30_EUR", "XAG_CHF", "EUR_USD", "EU50_EUR", "XAG_EUR", "XAU_EUR",
      "EUR_AUD", "EUR_JPY", "FR40_EUR", "GBP_CHF", "NZD_CHF", "CH20_CHF", "XAU_USD"
    ) %>% unique() , #5 EUR_CHF

    c("EUR_CHF", "DE30_EUR", "NL25_EUR", "EUR_USD", "EU50_EUR", "XAG_EUR", "XAU_EUR",
      "EUR_AUD", "EUR_JPY", "FR40_EUR", "XAU_USD") %>% unique(), #6 EUR_SEK

    c("GBP_JPY", "GBP_NZD", "UK10YB_GBP", "GBP_USD", "GBP_AUD", "XAG_GBP", "XAU_GBP",
      "UK100_GBP", "EUR_JPY", "FR40_EUR", "EUR_USD",  "EUR_CHF", "NZD_CHF", "CH20_CHF",
      "XAU_USD") %>% unique(), #7 GBP_CHF

    c("GBP_CHF", "GBP_NZD", "UK10YB_GBP", "GBP_USD", "GBP_AUD", "XAG_GBP", "XAU_GBP",
      "UK100_GBP", "XAG_JPY", "USD_JPY", "EUR_JPY", "XAU_JPY", "XAU_USD") %>% unique(), #8 GBP_JPY

    c("USD_NOK", "EUR_USD", "USD_JPY", "AUD_USD", "XAG_USD", "XAU_USD", "GBP_USD",
      "USD_CAD", "USD_SEK", "NZD_USD", "EUR_SEK") %>% unique(), #9 USD_CZK

    c("USD_NOK", "EUR_USD", "USD_JPY", "AUD_USD", "XAG_USD", "XAU_USD", "GBP_USD",
      "USD_CAD", "USD_SEK", "NZD_USD", "EUR_SEK") %>% unique(), #10 USD_NOK

    c("XAG_CHF", "XAG_JPY", "XAG_SGD", "XAG_USD", "XAG_EUR", "XAG_GBP", "XAG_NZD",
      "XAG_AUD", "XAU_USD", "XAU_EUR", "XAU_GBP",
      "XAU_SGD", "XAU_CAD", "XAU_NZD", "XAU_AUD") %>% unique(), #11 XAG_CAD

    c("XAG_CAD", "XAG_JPY", "XAG_SGD", "XAG_USD", "XAG_EUR", "XAG_GBP", "XAG_NZD",
      "XAG_AUD", "XAU_USD", "XAU_EUR", "XAU_GBP",
      "XAU_SGD", "XAU_CAD", "XAU_NZD", "XAU_AUD") %>% unique(), #12 XAG_CHF

    c("XAG_CAD", "XAG_CHF", "XAG_SGD", "XAG_USD", "XAG_EUR", "XAG_GBP", "XAG_NZD",
      "XAG_AUD", "XAU_USD", "XAU_EUR", "XAU_GBP",
      "XAU_SGD", "XAU_CAD", "XAU_NZD", "XAU_AUD") %>% unique(), #13 XAG_JPY

    c("GBP_CHF", "GBP_JPY", "UK10YB_GBP", "GBP_USD", "GBP_AUD", "XAG_GBP", "XAU_GBP",
      "UK100_GBP", "NZD_CHF", "NZD_USD", "XAU_NZD", "XAG_NZD") %>% unique(), #14 GBP_NZD

    c( "GBP_NZD", "NZD_USD", "XAU_NZD", "XAG_NZD", "EUR_CHF", "GBP_CHF", "XAG_CHF",
       "CH20_CHF") %>% unique(), #15 NZD_CHF

    c( "USD_NOK", "EUR_USD", "USD_JPY", "AUD_USD", "XAG_USD", "XAU_USD", "GBP_USD",
       "USD_CAD", "USD_SEK", "NZD_USD") %>% unique(), #16 USD_MXN

    c(
      "USB10Y_USD", "USD_SGD", "XAU_USD", "XAG_CHF", "AU200_AUD", "US2000_USD", "SPX500_USD",
      "NL25_EUR", "NL25_EUR", "FR40_EUR", "EU50_EUR", "DE30_EUR", "XAG_USD",
      "JP225_USD", "XAU_CHF", "EUR_CHF"
    ) %>% unique(), #17 CH20_CHF

    c("USD_NOK", "EUR_USD", "USD_JPY", "AUD_USD", "XAG_USD", "XAU_USD", "GBP_USD",
      "USD_CAD", "USD_SEK", "NZD_USD",
      "NATGAS_USD", "XPD_USD", "USB10Y_USD") %>% unique(), #18 XPT_USD

    c(
      "USD_NOK", "EUR_USD", "USD_JPY", "AUD_USD", "XAG_USD", "XAU_USD", "GBP_USD",
      "USD_CAD", "USD_SEK", "NZD_USD",
      "XPT_USD", "XPD_USD", "USB10Y_USD"
    ) %>% unique(), #19 NATGAS_USD

    c("USB10Y_USD", "USD_SGD", "XAU_SGD", "XAG_SGD", "AU200_AUD", "US2000_USD", "SPX500_USD",
      "NL25_EUR", "NL25_EUR", "FR40_EUR", "EU50_EUR", "JP225_USD", "XPT_USD", "XAU_USD", "DE30_EUR",
      "CH20_CHF") %>% unique(), #20 SG30_SGD

    c(
      "USD_NOK", "EUR_USD", "USD_JPY", "AUD_USD", "XAG_USD", "XAU_USD", "GBP_USD",
      "USD_CAD", "USD_SEK", "NZD_USD", "NATGAS_USD", "XPT_USD", "USB10Y_USD", "WHEAT_USD",
      "SUGAR_USD","SPX500_USD", "US2000_USD"
    ) %>% unique(), #21 SOYBN_USD

    c(
      "USB10Y_USD", "USD_SGD", "XAU_USD", "XAG_EUR", "AU200_AUD", "US2000_USD", "SPX500_USD",
      "NL25_EUR", "NL25_EUR", "FR40_EUR", "EU50_EUR", "DE30_EUR", "XAG_USD",
      "CH20_CHF", "XAU_EUR", "EUR_USD"
    ) %>% unique(), #22 JP225_USD

    c( "USD_NOK", "EUR_USD", "USD_JPY", "AUD_USD", "XAG_USD", "XAU_USD", "GBP_USD",
       "USD_CAD", "USD_SEK", "NZD_USD",
       "NATGAS_USD", "XPT_USD", "USB10Y_USD") %>% unique(), #23 XPD_USD

    c(
      "USB10Y_USD", "USD_SGD", "XAU_USD", "XAG_CHF", "AU200_AUD", "US2000_USD", "SPX500_USD",
      "CH20_CHF", "FR40_EUR", "EU50_EUR", "DE30_EUR", "XAG_USD",
      "JP225_USD", "XAU_CHF", "EUR_CHF"
    ) %>% unique(), #24 NL25_EUR

    c("XAG_CAD", "XAG_JPY", "XAG_CHF", "XAG_USD", "XAG_EUR", "XAG_GBP", "XAG_NZD",
      "XAG_AUD", "XAU_USD", "XAU_EUR", "XAU_GBP",
      "XAU_SGD", "XAU_CAD", "XAU_NZD", "XAU_AUD") %>% unique(), #25 XAG_SGD

    c(
      "USB10Y_USD", "USD_SGD", "XAU_USD", "BTC_USD", "LTC_USD", "US2000_USD", "SPX500_USD",
      "NL25_EUR", "NL25_EUR", "FR40_EUR", "EU50_EUR", "JP225_USD", "XAG_USD",
      "CH20_CHF", "XAU_GBP", "GBP_USD", "UK100_GBP"
    ) %>% unique(), #26 BCH_USD

    c(
      "USB10Y_USD", "USD_SGD", "XAU_USD", "BTC_USD", "BCH_USD", "US2000_USD", "SPX500_USD",
      "NL25_EUR", "NL25_EUR", "FR40_EUR", "EU50_EUR", "JP225_USD", "XAG_USD",
      "CH20_CHF", "XAU_GBP", "GBP_USD", "UK100_GBP"
    ) %>% unique() #27 LTC_USD

  )

actuals_periods_needed = c("period_return_50_Price")
correlation_rolling_periods = c(100,200, 300,400, 500)
state_space_periods = c(20, 40, 60, 100, 200,300, 400,  500)
state_space_rolling = c(100, 200, 300, 400)
# sig_thresh_vec <- c(0.99, 0.1, 0.05, 0.01, 10^-3, 10^-5, 10^-7, 10^-9)
sig_thresh_vec <- c(0.99, 10^-3, 10^-5, 10^-7, 10^-9)
bin_threshold_vec <- c(5)
safely_gen_models <- safely(Single_Asset_V3_Gen_Model_No_data_gen, otherwise = NULL)
result_db_path <- "C:/Users/Nikhil Chandra/Documents/trade_data/Day_Trader_Single_Asset_V3_Expanded_Models/SIG_THRESH_FINDER_WORK_PC_2026-03-18.DB"
reset_DB <- FALSE
c = 0

for (j in 12:length(assets_to_test)) {

  asset_of_interest <- assets_to_test[j]
  correlation_assets_current <- correlation_asset_list[[j]]

  tictoc::tic()
  required_data <-
    Single_Asset_V3_get_all_data_for_model(
      Indices_Metals_Bonds = Indices_Metals_Bonds,
      asset_of_interest = asset_of_interest,
      copula_assets = correlation_assets_current,
      raw_macro_data = raw_macro_data,
      correlation_rolling_periods = correlation_rolling_periods,
      state_space_periods = state_space_periods,
      state_space_rolling = state_space_rolling,
      loop_list_cols = c("Price", "Low", "High")
    )
  tictoc::toc()

  for (i in 1:length(sig_thresh_vec)) {
    for (k in 1:length(bin_threshold_vec)) {

      c = c + 1

      sig_thresh_current <- sig_thresh_vec[i]
      bin_threshold_current <- bin_threshold_vec[k]

      # pred_data <-
      #   Single_Asset_V3_Gen_Model(
      #     Indices_Metals_Bonds = Indices_Metals_Bonds,
      #     actual_wins_losses = actual_wins_losses,
      #     asset_of_interest = asset_of_interest,
      #     actuals_periods_needed = actuals_periods_needed,
      #     training_end_date = "2021-06-01",
      #     bin_threshold = bin_threshold_current,
      #     rolling_mean_pred_period = 500,
      #     correlation_rolling_periods = correlation_rolling_periods,
      #     state_space_periods = state_space_periods,
      #     state_space_rolling = state_space_rolling,
      #     copula_assets = correlation_assets_current,
      #     base_path = "C:/Users/Nikhil Chandra/Documents/trade_data/Day_Trader_Single_Asset_V3_Expanded_Models/",
      #     sig_thresh_AR = sig_thresh_current,
      #     sig_thresh_Copula = sig_thresh_current,
      #     sig_thresh_statespace = sig_thresh_current,
      #     raw_macro_data = raw_macro_data
      #
      #   )

      tictoc::tic()
      pred_data <-
        safely_gen_models(
          Indices_Metals_Bonds = Indices_Metals_Bonds,
          actual_wins_losses = actual_wins_losses,
          AR_model_data = required_data$AR_model_data,
          copula_data = required_data$copula_data,
          state_space_data = required_data$state_space_data,
          macro_model_data = required_data$macro_model_data,
          asset_of_interest = asset_of_interest,
          actuals_periods_needed = actuals_periods_needed,
          training_end_date = "2022-01-01",
          bin_threshold = bin_threshold_current,
          rolling_mean_pred_period = 500,
          base_path = "C:/Users/Nikhil Chandra/Documents/trade_data/Day_Trader_Single_Asset_V3_Expanded_Models/",
          sig_thresh_AR = sig_thresh_current,
          sig_thresh_Copula = sig_thresh_current,
          sig_thresh_statespace = sig_thresh_current,
          sig_thresh_macro = sig_thresh_current
        ) %>%
        pluck('result')
      tictoc::toc()

      if(!is.null(pred_data)) {

        testing_pred_data <-
          pred_data[[1]] %>%
          ungroup() %>%
          mutate(
            averaged_50_LM_pred =
              (state_space_LM_Pred_period_return_50_Price +
                 AR_LM_Pred_period_return_50_Price)/2,

            averaged_50_GLM_pred =
              (state_space_GLM_Pred_period_return_50_Price +
                 AR_GLM_Pred_period_return_50_Price )/2

            # averaged_42_50_GLM_pred =
            #   (state_space_GLM_Pred_period_return_42_Price +
            #      AR_GLM_Pred_period_return_42_Price +
            #      state_space_GLM_Pred_period_return_50_Price +
            #      AR_GLM_Pred_period_return_50_Price)/4,
            #
            # averaged_42_50_LM_pred =
            #   (state_space_LM_Pred_period_return_42_Price +
            #      AR_LM_Pred_period_return_42_Price +
            #      state_space_LM_Pred_period_return_50_Price +
            #      AR_LM_Pred_period_return_50_Price)/4
          )

        AR_LM_Pred_analysis_LM <-
          construct_Performance_to_Thresh_Curve(
            pred_data = testing_pred_data,
            pred_col = "AR_LM_Pred_period_return_50_Price",
            actual_wins_losses = actual_wins_losses,
            # thresh_vector = seq(0, 0.9, 0.05),
            thresh_vector = seq(-15, 10, 1),
            period_return_col = "period_return_50_Price",
            sim_start_date = "2022-01-01"
          ) %>%
          mutate(Asset = asset_of_interest,
                 pred_col_used = "AR_LM_Pred_period_return_50_Price")

        AR_GLM_Pred_analysis_GLM <-
          construct_Performance_to_Thresh_Curve(
            pred_data = testing_pred_data,
            pred_col = "AR_GLM_Pred_period_return_50_Price",
            actual_wins_losses = actual_wins_losses,
            thresh_vector = seq(0, 0.9, 0.05),
            # thresh_vector = seq(-15, 10, 1),
            period_return_col = "period_return_50_Price",
            sim_start_date = "2022-01-01"
          ) %>%
          mutate(Asset = asset_of_interest,
                 pred_col_used = "AR_GLM_Pred_period_return_50_Price")

        state_space_LM_Pred_analysis_LM <-
          construct_Performance_to_Thresh_Curve(
            pred_data = testing_pred_data,
            pred_col = "state_space_LM_Pred_period_return_50_Price",
            actual_wins_losses = actual_wins_losses,
            # thresh_vector = seq(0, 0.9, 0.05),
            thresh_vector = seq(-15, 10, 1),
            period_return_col = "period_return_50_Price",
            sim_start_date = "2022-01-01"
          ) %>%
          mutate(Asset = asset_of_interest,
                 pred_col_used = "state_space_LM_Pred_period_return_50_Price")

        state_space_GLM_Pred_analysis_GLM <-
          construct_Performance_to_Thresh_Curve(
            pred_data = testing_pred_data,
            pred_col = "state_space_GLM_Pred_period_return_50_Price",
            actual_wins_losses = actual_wins_losses,
            thresh_vector = seq(0, 0.9, 0.05),
            # thresh_vector = seq(-15, 10, 1),
            period_return_col = "period_return_50_Price",
            sim_start_date = "2022-01-01"
          ) %>%
          mutate(Asset = asset_of_interest,
                 pred_col_used = "state_space_GLM_Pred_period_return_50_Price")

        copula_LM_Pred_analysis_LM <-
          construct_Performance_to_Thresh_Curve(
            pred_data = testing_pred_data,
            pred_col = "Copula_LM_Pred_period_return_50_Price",
            actual_wins_losses = actual_wins_losses,
            # thresh_vector = seq(0, 0.9, 0.05),
            thresh_vector = seq(-15, 10, 1),
            period_return_col = "period_return_50_Price",
            sim_start_date = "2022-01-01"
          ) %>%
          mutate(Asset = asset_of_interest,
                 pred_col_used = "Copula_LM_Pred_period_return_50_Price")

        copula_GLM_Pred_analysis_GLM <-
          construct_Performance_to_Thresh_Curve(
            pred_data = testing_pred_data,
            pred_col = "Copula_GLM_Pred_period_return_50_Price",
            actual_wins_losses = actual_wins_losses,
            thresh_vector = seq(0, 0.9, 0.05),
            # thresh_vector = seq(-15, 10, 1),
            period_return_col = "period_return_50_Price",
            sim_start_date = "2022-01-01"
          ) %>%
          mutate(Asset = asset_of_interest,
                 pred_col_used = "Copula_GLM_Pred_period_return_50_Price")

        Macro_LM_Pred_analysis_LM <-
          construct_Performance_to_Thresh_Curve(
            pred_data = testing_pred_data,
            pred_col = "Macro_LM_Pred_period_return_50_Price",
            actual_wins_losses = actual_wins_losses,
            # thresh_vector = seq(0, 0.9, 0.05),
            thresh_vector = seq(-15, 10, 1),
            period_return_col = "period_return_50_Price",
            sim_start_date = "2022-01-01"
          ) %>%
          mutate(Asset = asset_of_interest,
                 pred_col_used = "Macro_LM_Pred_period_return_50_Price")

        Macro_GLM_Pred_analysis_GLM <-
          construct_Performance_to_Thresh_Curve(
            pred_data = testing_pred_data,
            pred_col = "Macro_GLM_Pred_period_return_50_Price",
            actual_wins_losses = actual_wins_losses,
            thresh_vector = seq(0, 0.9, 0.05),
            # thresh_vector = seq(-15, 10, 1),
            period_return_col = "period_return_50_Price",
            sim_start_date = "2022-01-01"
          ) %>%
          mutate(Asset = asset_of_interest,
                 pred_col_used = "Macro_GLM_Pred_period_return_50_Price")

        AR_Macro_pred_analysis_LM <-
          construct_Performance_to_2_Thresh_Curve(
            pred_data = testing_pred_data,
            pred_col1 = "Macro_LM_Pred_period_return_50_Price",
            pred_col2 = "AR_LM_Pred_period_return_50_Price",
            pred_col3 = NULL,
            actual_wins_losses = actual_wins_losses,
            # thresh_vector = seq(0, 0.9, 0.05),
            thresh_vector = seq(-15, 10, 1),
            period_return_col = "period_return_50_Price",
            sim_start_date = "2022-01-01"
          ) %>%
          mutate(Asset = asset_of_interest,
                 pred_col_used = "Macro_LM_Pred_period_return_50_Price & AR_LM_Pred_period_return_50_Price")

        AR_Macro_pred_analysis_GLM <-
          construct_Performance_to_2_Thresh_Curve(
            pred_data = testing_pred_data,
            pred_col1 = "Macro_GLM_Pred_period_return_50_Price",
            pred_col2 = "AR_GLM_Pred_period_return_50_Price",
            actual_wins_losses = actual_wins_losses,
            thresh_vector = seq(0, 0.9, 0.05),
            # thresh_vector = seq(-15, 10, 1),
            period_return_col = "period_return_50_Price",
            sim_start_date = "2022-01-01"
          ) %>%
          mutate(Asset = asset_of_interest,
                 pred_col_used = "Macro_GLM_Pred_period_return_50_Price & AR_GLM_Pred_period_return_50_Price")

        AR_state_space_pred_analysis_LM <-
          construct_Performance_to_2_Thresh_Curve(
            pred_data = testing_pred_data,
            pred_col1 = "state_space_LM_Pred_period_return_50_Price",
            pred_col2 = "AR_LM_Pred_period_return_50_Price",
            actual_wins_losses = actual_wins_losses,
            # thresh_vector = seq(0, 0.9, 0.05),
            thresh_vector = seq(-15, 10, 1),
            period_return_col = "period_return_50_Price",
            sim_start_date = "2022-01-01"
          ) %>%
          mutate(Asset = asset_of_interest,
                 pred_col_used = "state_space_LM_Pred_period_return_50_Price & AR_LM_Pred_period_return_50_Price")

        AR_state_space_pred_analysis_GLM <-
          construct_Performance_to_2_Thresh_Curve(
            pred_data = testing_pred_data,
            pred_col1 = "state_space_GLM_Pred_period_return_50_Price",
            pred_col2 = "AR_GLM_Pred_period_return_50_Price",
            actual_wins_losses = actual_wins_losses,
            thresh_vector = seq(0, 0.9, 0.05),
            # thresh_vector = seq(-15, 10, 1),
            period_return_col = "period_return_50_Price",
            sim_start_date = "2022-01-01"
          ) %>%
          mutate(Asset = asset_of_interest,
                 pred_col_used = "state_space_GLM_Pred_period_return_50_Price & AR_GLM_Pred_period_return_50_Price")


        AR_state_space_macro_pred_analysis_LM <-
          construct_Performance_to_2_Thresh_Curve(
            pred_data = testing_pred_data,
            pred_col1 = "state_space_LM_Pred_period_return_50_Price",
            pred_col2 = "AR_LM_Pred_period_return_50_Price",
            pred_col3 = "Macro_LM_Pred_period_return_50_Price",
            actual_wins_losses = actual_wins_losses,
            # thresh_vector = seq(0, 0.9, 0.05),
            thresh_vector = seq(-15, 10, 1),
            period_return_col = "period_return_50_Price",
            sim_start_date = "2022-01-01"
          ) %>%
          mutate(Asset = asset_of_interest,
                 pred_col_used =
                   "state_space_LM_Pred_period_return_50_Price & AR_LM_Pred_period_return_50_Price & Macro_LM_Pred_period_return_50_Price")

        AR_state_space_macro_pred_analysis_GLM <-
          construct_Performance_to_2_Thresh_Curve(
            pred_data = testing_pred_data,
            pred_col1 = "state_space_GLM_Pred_period_return_50_Price",
            pred_col2 = "AR_GLM_Pred_period_return_50_Price",
            pred_col3 = "Macro_GLM_Pred_period_return_50_Price",
            actual_wins_losses = actual_wins_losses,
            thresh_vector = seq(0, 0.9, 0.05),
            # thresh_vector = seq(-15, 10, 1),
            period_return_col = "period_return_50_Price",
            sim_start_date = "2022-01-01"
          ) %>%
          mutate(Asset = asset_of_interest,
                 pred_col_used =
                   "state_space_GLM_Pred_period_return_50_Price & AR_GLM_Pred_period_return_50_Price & Macro_GLM_Pred_period_return_50_Price")

        all_results_dfr <-
          AR_LM_Pred_analysis_LM %>%
          bind_rows(AR_GLM_Pred_analysis_GLM) %>%
          bind_rows(state_space_LM_Pred_analysis_LM)%>%
          bind_rows(state_space_GLM_Pred_analysis_GLM)%>%
          bind_rows(copula_LM_Pred_analysis_LM) %>%
          bind_rows(copula_GLM_Pred_analysis_GLM) %>%
          bind_rows(Macro_LM_Pred_analysis_LM) %>%
          bind_rows(Macro_GLM_Pred_analysis_GLM) %>%
          bind_rows(AR_Macro_pred_analysis_LM) %>%
          bind_rows(AR_Macro_pred_analysis_GLM) %>%
          bind_rows(AR_state_space_pred_analysis_LM) %>%
          bind_rows(AR_state_space_pred_analysis_GLM) %>%
          bind_rows(AR_state_space_macro_pred_analysis_LM) %>%
          bind_rows(AR_state_space_macro_pred_analysis_GLM) %>%
          mutate(
            sig_thresh_current = sig_thresh_vec[i],
            bin_threshold_current = bin_threshold_vec[k]
          )

        if(c == 1 & reset_DB == TRUE){
          # db_con <- connect_db(result_db_path)
          # write_table_sql_lite(.data = all_results_dfr,
          #                      table_name = "SIG_THRESH_FINDER",
          #                      conn = db_con,
          #                      overwrite_true = TRUE)
          # DBI::dbDisconnect(db_con)

          db_con <- connect_db(result_db_path)
          append_table_sql_lite(.data = all_results_dfr,
                                table_name = "SIG_THRESH_FINDER",
                                conn = db_con)
          DBI::dbDisconnect(db_con)

        } else {
          db_con <- connect_db(result_db_path)
          append_table_sql_lite(.data = all_results_dfr,
                                table_name = "SIG_THRESH_FINDER",
                                conn = db_con)
          DBI::dbDisconnect(db_con)
        }

      }

    }
  }
}

db_con <- connect_db(result_db_path)
AR_LM_Pred_analysis <-
  DBI::dbGetQuery(conn = db_con,
                  statement = "SELECT * FROM SIG_THRESH_FINDER")
DBI::dbDisconnect(db_con)

analyse_control_vs_model <-
  function(pred_analysis_data = AR_LM_Pred_analysis,
           thresh_min_LM = 0.5,
           thresh_min_GLM = 0.5) {

    control_data <-
      pred_analysis_data %>%
      filter(trade_col == "Control") %>%
      dplyr::select(Asset,
                    pred_col_used,
                    Final_Winnings_Control = Final_Winnings,
                    random_returns_mid_control = random_returns_mid,
                    random_perc_mid_control = random_perc_mid,
                    random_returns_05_control = random_returns_05,
                    random_returns_75_control = random_returns_75,
                    Perc_Control = Perc_UnAdj) %>%
      distinct() %>%
      group_by(pred_col_used, Asset) %>%
      slice_max(random_perc_mid_control) %>%
      ungroup() %>%
      distinct()


    model_data <-
      pred_analysis_data %>%
      filter(
        (str_detect(pred_col_used, "GLM") & threshold >= thresh_min_GLM) |
        (str_detect(pred_col_used, "_LM") & threshold >= thresh_min_LM)
        ) %>%
      dplyr::select(Asset,
                    pred_col_used,
                    total_trades,
                    Final_Winnings,
                    random_returns_mid,
                    random_returns_05,
                    random_returns_75,
                    random_perc_mid,
                    Perc_Adj,
                    sig_thresh_current,
                    threshold) %>%
      distinct() %>%
      left_join(
        control_data
      ) %>%
      mutate(
        Final_Winnings_Diff = Final_Winnings - Final_Winnings_Control,
        random_returns_mid_Diff = random_returns_mid - random_returns_mid_control,
        perc_diff = random_perc_mid - Perc_Control
      )

    return(model_data)

  }


control_diffs <-
  analyse_control_vs_model(
    pred_analysis_data = AR_LM_Pred_analysis,
    thresh_min_LM = 0.5,
    thresh_min_GLM = 0.5
  ) %>%
  filter(total_trades >= 1000)

final_winnings_diff <-
  control_diffs %>%
  group_by(Asset, pred_col_used) %>%
  slice_max(Final_Winnings_Diff) %>%
  ungroup() %>%
  group_by(Asset, pred_col_used) %>%
  slice_max(random_perc_mid) %>%
  group_by(Asset, pred_col_used) %>%
  slice_max(random_returns_mid)

random_returns_diff <-
  control_diffs %>%
  group_by(Asset, pred_col_used) %>%
  slice_max(random_returns_mid_Diff) %>%
  ungroup() %>%
  group_by(Asset, pred_col_used) %>%
  slice_max(Final_Winnings) %>%
  group_by(Asset, pred_col_used) %>%
  slice_max(random_returns_mid)


max_returns_single <-
  AR_LM_Pred_analysis %>%
  filter(total_trades >= 1000) %>%
  filter(threshold >= 0.5, str_detect(pred_col_used, "_GLM") ,
         !str_detect(pred_col_used, "&")) %>%
  # filter(average_win > average_loss) %>%
  group_by(pred_col_used, Asset) %>%
  slice_max(Final_Winnings) %>%
  group_by(pred_col_used, Asset) %>%
  slice_max(random_returns_mid) %>%
  group_by(pred_col_used, Asset) %>%
  slice_max(random_perc_mid)

max_returns_multi <-
  AR_LM_Pred_analysis %>%
  # filter(total_trades >= 1000) %>%
  filter(threshold > 0.4, random_returns_mid > 0, str_detect(pred_col_used, "_GLM") ,
         str_detect(pred_col_used, "&")) %>%
  filter(average_win > average_loss) %>%
  group_by(pred_col_used, Asset) %>%
  slice_max(Final_Winnings) %>%
  group_by(pred_col_used, Asset) %>%
  slice_max(random_returns_mid) %>%
  group_by(pred_col_used, Asset) %>%
  slice_max(random_perc_mid)

max_returns_multi_no_XAG <-
  max_returns_multi %>%
  ungroup() %>%
  filter(!str_detect(Asset, "XAG"))

max_returns <-
  AR_LM_Pred_analysis %>%
  filter(total_trades >= 1000) %>%
  mutate(ratio_05_75_random_returns = abs(random_returns_75/random_returns_05)) %>%
  filter(threshold > 0, random_returns_mid > 0, str_detect(pred_col_used, "_GLM") & ratio_05_75_random_returns > 1) %>%
  filter(average_win > average_loss) %>%
  group_by(pred_col_used, Asset) %>%
  slice_max(random_returns_mid)

AR_LM_Pred_analysis %>%
  filter(!is.na(threshold)) %>%
  filter(pred_col_used == "AR_GLM_Pred_period_return_50_Price") %>%
  group_by(Asset) %>%
  mutate(
    trades_x =
      case_when(
        Final_Winnings == max(Final_Winnings, na.rm = T) ~
          glue::glue("{total_trades}\n{round(Final_Winnings)}")
      )
  ) %>%
  mutate(
    sig_thresh = as.character(sig_thresh_current)
  ) %>%
  ggplot(aes(x = threshold, y = Final_Winnings, color = sig_thresh))  +
  geom_line(show.legend = FALSE) +
  geom_point(show.legend = FALSE) +
  geom_label(aes(label = trades_x), size = 3, show.legend = FALSE, color = "black") +
  facet_wrap(.~Asset, scales = "free") +
  theme_minimal()

max_points <-
  AR_LM_Pred_analysis %>%
  filter(total_trades >= 500,
         threshold >= 0) %>%
  group_by(Asset) %>%
  slice_max(Final_Winnings, n = 5) %>%
  ungroup()

TS_analysis <-
  construct_time_series(
    actual_wins_losses = actual_wins_losses,
    pred_data = testing_pred_data,
    Asset_Var = asset_of_interest,
    # trade_statement = "AR_LM_Pred_period_return_50_Price >= 1 &
    #                     state_space_LM_Pred_period_return_50_Price >= 2",
    trade_statement = "Copula_GLM_Pred_period_return_50_Price >= 0.35",
    trade_direction = "Long",
    win_thresh = 0
  ) %>%
  filter(Period == 50)

TS_analysis %>%
  ggplot(aes(x = Date, y = Total_Returns_cumulative, color = trade_col)) +
  geom_line() +
  facet_wrap(.~trade_col, scales = "free") +
  theme_minimal() +
  theme(legend.position = "bottom")

all_control_winnings <-
  AR_LM_Pred_analysis %>%
  filter(trade_col == "Control") %>%
  distinct(Asset, sig_thresh, bin_threshold, Final_Winnings, Return_Middle, Ratio_of_25_to_75) %>%
  rename(
    Control_Winnings = Final_Winnings,
    Control_Middle = Return_Middle,
    Control_Ratio = Ratio_of_25_to_75
  )

comparison_frame <-
  AR_LM_Pred_analysis %>%
  dplyr::select(Asset, sig_thresh, bin_threshold, threshold, total_trades,
                Final_Winnings, Return_Middle, Ratio_of_25_to_75) %>%
  left_join(
    all_control_winnings
  ) %>%
  mutate(
    Final_Value_diff = Final_Winnings - Control_Winnings
  ) %>%
  group_by(Asset) %>%
  slice_max(Final_Value_diff)
