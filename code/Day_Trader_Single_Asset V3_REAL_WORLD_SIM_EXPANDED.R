helpeR::load_custom_functions()

all_aud_symbols <- get_oanda_symbols() %>%
  keep(~ str_detect(.x, "AUD")|str_detect(.x, "USD_SEK|USD_NOK|USD_HUF|USD_ZAR|USD_CNY|USD_MXN"))
asset_infor <- get_instrument_info()
aud_assets <- read_all_asset_data_intra_day(
  asset_list_oanda = all_aud_symbols,
  save_path_oanda_assets = "C:/Users/nikhi/Documents/Asset Data/oanda_data/",
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
db_location = "C:/Users/nikhi/Documents/Asset Data/Oanda_Asset_Data_Most_Assets_2025-09-13.db"
start_date = "2018-06-01"
end_date = today() %>% as.character()

bin_factor = NULL
stop_value_var = 5
profit_value_var = 50
period_var = 24

All_Daily_Data <-
  get_DAILY_ALGO_DATA_API_REQUEST(
    c("EUR_USD", #1
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
    ) %>% unique()
  )

Indices_Metals_Bonds <- list()

Indices_Metals_Bonds[[1]] <-
  get_db_data_quickly_algo(
    db_location = db_location,
    start_date = start_date,
    end_date = as.character(today() + days(30)),
    time_frame = "H1",
    bid_or_ask = "ask",
    assets =   c("SPX500_USD","US2000_USD","EU50_EUR","SG30_SGD" ,
                 "AU200_AUD" ,"XAG_USD","XAU_USD","USD_JPY" ,
                 "AUD_USD" ,"UK100_GBP" ,"JP225Y_JPY","FR40_EUR" ,
                 "CH20_CHF","USB10Y_USD","USB02Y_USD" ,"UK10YB_GBP" ,
                 "HK33_HKD" ,"EUR_USD" ,"GBP_USD" ,"XAG_EUR" ,
                 "XAU_EUR" ,"XAU_GBP" ,"XAG_GBP" ,"EUR_GBP" ,
                 "WTICO_USD" ,"BCO_USD" ,"XCU_USD" ,"XAU_JPY",
                 "XAG_JPY" ,"XAU_AUD" ,"XAG_AUD" ,"USD_CAD" ,
                 "EUR_AUD" ,"NZD_USD" ,"EUR_NZD" ,"AUD_NZD" ,
                 "GBP_AUD" ,"GBP_NZD" ,"GBP_CAD" ,"GBP_JPY" ,
                 "USD_SGD" ,"EUR_JPY" , "BTC_USD" ,"ETH_USD" ,"NATGAS_USD" ,
                 "EUR_SEK" ,"USD_SEK" ,"LTC_USD" , "XAG_NZD")
  ) %>%
  distinct()
Indices_Metals_Bonds[[2]] <-
  get_db_data_quickly_algo(
    db_location = db_location,
    start_date = start_date,
    end_date = as.character(today() + days(30)),
    time_frame = "H1",
    bid_or_ask = "bid",
    assets =   c("SPX500_USD","US2000_USD","EU50_EUR","SG30_SGD" ,
                 "AU200_AUD" ,"XAG_USD","XAU_USD","USD_JPY" ,
                 "AUD_USD" ,"UK100_GBP" ,"JP225Y_JPY","FR40_EUR" ,
                 "CH20_CHF","USB10Y_USD","USB02Y_USD" ,"UK10YB_GBP" ,
                 "HK33_HKD" ,"EUR_USD" ,"GBP_USD" ,"XAG_EUR" ,
                 "XAU_EUR" ,"XAU_GBP" ,"XAG_GBP" ,"EUR_GBP" ,
                 "WTICO_USD" ,"BCO_USD" ,"XCU_USD" ,"XAU_JPY",
                 "XAG_JPY" ,"XAU_AUD" ,"XAG_AUD" ,"USD_CAD" ,
                 "EUR_AUD" ,"NZD_USD" ,"EUR_NZD" ,"AUD_NZD" ,
                 "GBP_AUD" ,"GBP_NZD" ,"GBP_CAD" ,"GBP_JPY" ,
                 "USD_SGD" ,"EUR_JPY" , "BTC_USD" ,"ETH_USD" ,"NATGAS_USD" ,
                 "EUR_SEK" ,"USD_SEK" ,"LTC_USD" , "XAG_NZD")
  ) %>%
  distinct()

actual_wins_losses <-
  get_actual_wins_losses(
    assets_to_analyse =
      c("EUR_USD", #1
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
assets_to_test <- c("EUR_USD", #1 EUR_USD
                    "SPX500_USD", #2 SPX500_USD
                    "US2000_USD", #3 US2000_USD
                    "USD_JPY", #4 USD_JPY
                    "EUR_JPY", #5 EUR_JPY
                    "AUD_USD", #6 AUD_USD
                    "GBP_USD", #7 GBP_USD
                    "EUR_GBP", #8 EUR_GBP
                    "AU200_AUD", #9 AU200_AUD
                    "XAU_USD", #10 XAU_USD
                    "UK100_GBP", #11 UK100_GBP
                    "XAG_USD", #12 XAG_USD
                    "GBP_JPY", #13 GBP_JPY
                    "USD_CAD", #14 USD_CAD
                    "EU50_EUR", #15 EU50_EUR
                    "HK33_HKD" #16 HK33_HKD
                    )

correlation_asset_list <-
  list(
    #1 EUR_USD,
    c("XAU_EUR", "XAG_EUR", "EUR_JPY", "EU50_EUR", "EUR_AUD", "EUR_GBP",
      "SPX500_USD", "XAU_USD", "USD_JPY", "GBP_USD", "EUR_NZD", "XAG_GBP", "XAU_GBP",
      "EUR_SEK", "USD_CAD") %>% unique(),

    #2 SPX500_USD
    c("US2000_USD", "AU200_AUD", "USB10Y_USD", "UK100_GBP", "XAU_USD", "EU50_EUR",
      "HK33_HKD", "FR40_EUR", "WTICO_USD", "USD_JPY", "EUR_USD", "GBP_USD", "AU200_AUD",
      "SG30_SGD", "XAU_EUR", "XAG_EUR", "XAG_GBP", "XAU_GBP", "XAG_USD" ) %>% unique(),

    #3 US2000_USD
    c("SPX500_USD",  "AU200_AUD", "USB10Y_USD", "UK100_GBP", "XAU_USD", "EU50_EUR",
      "HK33_HKD", "FR40_EUR", "WTICO_USD", "USD_JPY", "EUR_USD", "GBP_USD", "AU200_AUD",
      "SG30_SGD", "XAU_EUR", "XAG_EUR", "XAG_GBP", "XAU_GBP","XAG_USD" ) %>% unique(),

    #4 USD_JPY
    c("EUR_JPY", "XAU_JPY", "XAG_JPY", "GBP_JPY", "XAU_USD", "SPX500_USD",
      "XAG_USD","NZD_USD", "AUD_USD", "EUR_USD", "GBP_USD", "USD_CAD",
      "USD_SEK", "USD_SGD", "USB10Y_USD") %>% unique(),

    #5 "EUR_JPY"
    c("GBP_USD", "EUR_USD", "XAU_EUR", "XAU_JPY", "USD_JPY", "EUR_AUD",
      "EUR_GBP", "EUR_NZD", "EUR_SEK", "XAG_EUR", "XAU_USD", "XAG_USD", "USD_JPY",
      "GBP_JPY", "FR40_EUR", "EU50_EUR") %>% unique(),

    #6 "AUD_USD"
    c("XCU_USD", "AU200_AUD", "XAU_AUD", "GBP_AUD", "XAU_USD", "EUR_AUD",
      "XAG_USD","NZD_USD", "USD_JPY", "EUR_USD", "GBP_USD", "USD_CAD",
      "USD_SEK", "USD_SGD", "USB10Y_USD", "NZD_USD") %>% unique(),

    #7 "GBP_USD"
    c("GBP_JPY", "GBP_CAD", "GBP_AUD", "GBP_NZD", "XAU_GBP", "XAG_GBP", "UK100_GBP",
      "XAU_USD", "XAG_USD", "EUR_GBP", "EUR_USD", "XAG_EUR", "XAU_EUR", "USD_JPY",
      "EUR_JPY", "UK10YB_GBP", "AUD_USD", "USD_SEK", "USD_CAD") %>% unique(),

    #8"EUR_GBP"
    c("GBP_USD", "EUR_USD", "XAU_EUR", "XAU_GBP", "GBP_JPY", "EUR_JPY",
      "XAG_EUR", "XAG_GBP", "USD_JPY", "UK100_GBP", "FR40_EUR", "EU50_EUR",
      "EUR_SEK", "USD_SEK", "EUR_AUD", "EUR_NZD", "EUR_SEK") %>% unique(),

    #9"AU200_AUD"
    c("XCU_USD", "US2000_USD", "UK100_GBP", "XAU_USD", "EU50_EUR",
      "HK33_HKD", "FR40_EUR", "WTICO_USD", "GBP_AUD", "EUR_AUD",
      "SG30_SGD", "XAU_EUR", "XAG_EUR", "XAG_GBP", "XAU_GBP", "XAG_USD" ) %>% unique(),

    #10"XAU_USD"
    c("XAG_JPY", "XAG_GBP", "XAG_EUR", "XAG_AUD", "XAG_USD", "EU50_EUR", "SPX500_USD",
      "XAG_NZD", "XAU_USD", "XAU_GBP", "XAU_JPY", "XAU_EUR", "AU200_AUD", "USD_JPY",
      "GBP_AUD", "AUD_USD", "EUR_AUD", "AUD_USD") %>% unique(),

    #11"UK100_GBP"
    c("XAU_EUR", "XAG_EUR", "XAU_USD", "SG30_SGD", "EUR_GBP", "US2000_USD",
      "SPX500_USD", "XAU_USD", "AU200_AUD", "CH20_CHF", "UK10YB_GBP", "USB10Y_USD",
      "XAG_GBP", "XAU_GBP", "WTICO_USD", "FR40_EUR", "HK33_HKD") %>% unique(),

    #12"XAG_USD"
    c("XAU_EUR", "XAG_EUR", "XAU_USD", "SG30_SGD", "EUR_GBP", "US2000_USD",
      "SPX500_USD", "XAU_USD", "AU200_AUD", "CH20_CHF", "UK10YB_GBP", "USB10Y_USD",
      "XAG_GBP", "XAU_GBP", "WTICO_USD", "FR40_EUR", "HK33_HKD") %>% unique(),

    #13"GBP_JPY"
    c("GBP_CAD", "GBP_USD", "XAU_GBP", "XAG_GBP", "UK100_GBP",
      "GBP_NZD", "XAG_USD", "EUR_GBP", "EUR_JPY", "XAU_JPY", "USD_JPY", "XAG_JPY",
      "AUD_USD", "UK10YB_GBP") %>% unique(),

    #14"USD_CAD"
    c("XAU_JPY", "XAU_GBP", "XAU_EUR", "XAU_USD", "EUR_JPY", "GBP_JPY",
      "XAG_USD","NZD_USD", "USD_JPY", "EUR_USD", "GBP_USD", "GBP_CAD",
      "USD_SEK", "USD_SGD", "USB10Y_USD") %>% unique(),

    #15"EU50_EUR"
    c("XAU_EUR", "XAG_EUR", "XAU_USD", "UK100_GBP", "SG30_SGD", "EUR_GBP", "SPX500_USD",
      "SPX500_USD", "XAU_USD", "AU200_AUD", "CH20_CHF", "US2000_USD",
      "XAG_GBP", "XAU_GBP", "WTICO_USD", "FR40_EUR", "HK33_HKD") %>% unique(),

    #15"HK33_HKD"
    c("US2000_USD", "AU200_AUD", "USB10Y_USD", "UK100_GBP", "XAU_USD", "EU50_EUR",
      "SPX500_USD", "FR40_EUR", "WTICO_USD", "USD_JPY", "EUR_USD", "GBP_USD", "AU200_AUD",
      "SG30_SGD", "XAU_EUR", "XAG_EUR", "XAG_GBP", "XAU_GBP", "XAG_USD") %>% unique()

  )
actuals_periods_needed = c("period_return_50_Price")
correlation_rolling_periods = c(100,200, 300,400, 500)
state_space_periods = c(20, 40, 60, 100, 200,300, 400,  500)
state_space_rolling = c(100, 200, 300, 400)
# sig_thresh_vec <- c(0.99, 0.1, 0.05, 0.01, 10^-3, 10^-5, 10^-7, 10^-9)
sig_thresh_vec <- c(0.99, 10^-3, 10^-5, 10^-7, 10^-9)
bin_threshold_vec <- c(0)
result_db_path <- "C:/Users/nikhi/Documents/trade_data/Day_Trader_Single_Asset_V3_Expanded_Models/SIG_THRESH_FINDER.DB"
reset_DB <- FALSE
c = 0

for (j in 7:length(assets_to_test)) {

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
      #     base_path = "C:/Users/nikhi/Documents/trade_data/Day_Trader_Single_Asset_V3_Expanded_Models/",
      #     sig_thresh_AR = sig_thresh_current,
      #     sig_thresh_Copula = sig_thresh_current,
      #     sig_thresh_statespace = sig_thresh_current,
      #     raw_macro_data = raw_macro_data
      #
      #   )

      tictoc::tic()
      pred_data <-
        Single_Asset_V3_Gen_Model_No_data_gen(
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
              base_path = "C:/Users/nikhi/Documents/trade_data/Day_Trader_Single_Asset_V3_Expanded_Models/",
              sig_thresh_AR = sig_thresh_current,
              sig_thresh_Copula = sig_thresh_current,
              sig_thresh_statespace = sig_thresh_current,
              sig_thresh_macro = sig_thresh_current
        )
      tictoc::toc()

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

      all_results_dfr <-
        AR_LM_Pred_analysis_LM %>%
        bind_rows(AR_GLM_Pred_analysis_GLM) %>%
        bind_rows(state_space_LM_Pred_analysis_LM)%>%
        bind_rows(state_space_GLM_Pred_analysis_GLM)%>%
        bind_rows(copula_LM_Pred_analysis_LM) %>%
        bind_rows(copula_GLM_Pred_analysis_GLM) %>%
        bind_rows(Macro_LM_Pred_analysis_LM) %>%
        bind_rows(Macro_GLM_Pred_analysis_GLM) %>%
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

AR_LM_Pred_analysis %>%
  filter(!is.na(threshold)) %>%
  group_by(Asset) %>%
  mutate(
    trades_x =
      case_when(
        Final_Winnings == max(Final_Winnings, na.rm = T) ~
          glue::glue("{total_trades}\n{round(Final_Winnings)}")
      )
  ) %>%
  mutate(
    sig_thresh = as.character(sig_thresh)
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


