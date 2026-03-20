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
start_date = "2016-01-01"
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

result_db_path <- "C:/Users/Nikhil Chandra/Documents/trade_data/Day_Trader_Single_Asset_V3_Expanded_Models/SIG_THRESH_FINDER_WORK_PC_2026-03-18.DB"
db_con <- connect_db(result_db_path)
Best_Sigs <- DBI::dbGetQuery(conn = db_con,
                statement = "SELECT * FROM BEST_SIG_PER_ASSET" )
DBI::dbDisconnect(db_con)

actuals_periods_needed = c("period_return_50_Price")
correlation_rolling_periods = c(100,200, 300,400, 500)
state_space_periods = c(20, 40, 60, 100, 200,300, 400,  500)
state_space_rolling = c(100, 200, 300, 400)
# sig_thresh_vec <- c(0.99, 0.1, 0.05, 0.01, 10^-3, 10^-5, 10^-7, 10^-9)
sig_thresh_vec <- c(0.99, 10^-3, 10^-5, 10^-7, 10^-9)
bin_threshold_vec <- c(0)
safely_gen_models <- safely(Single_Asset_V3_Gen_Model_No_data_gen, otherwise = NULL)
result_db_path <- "C:/Users/Nikhil Chandra/Documents/trade_data/Day_Trader_Single_Asset_V3_Expanded_Models/SIM_RESULTS_WORK_PC"
date_for_true_simualtion <- "2019-01-01"
training_end_date <- "2022-01-01"
reset_DB <- TRUE
c = 0

for (j in 1:length(assets_to_test)) {

  asset_of_interest <- assets_to_test[j]
  correlation_assets_current <- correlation_asset_list[[j]]
  required_sigs <-
    Best_Sigs %>%
    filter(Asset == asset_of_interest) %>%
    filter(!str_detect(pred_col_used, "&"))

  sig_thresh_AR_LM =
    required_sigs %>% filter(str_detect(pred_col_used, "AR")&str_detect(pred_col_used, "_LM")) %>%
    pull(sig_thresh_current) %>%
    max(na.rm = T)
  sig_thresh_AR_GLM =
    required_sigs %>% filter(str_detect(pred_col_used, "AR")&str_detect(pred_col_used, "GLM")) %>%
    pull(sig_thresh_current) %>%
    max(na.rm = T)
  sig_thresh_Copula_LM =
    required_sigs %>% filter(str_detect(pred_col_used, "Copula")&str_detect(pred_col_used, "_LM")) %>%
    pull(sig_thresh_current) %>%
    max(na.rm = T)
  sig_thresh_Copula_GLM =
    required_sigs %>% filter(str_detect(pred_col_used, "Copula") & str_detect(pred_col_used, "GLM")) %>%
    pull(sig_thresh_current) %>%
    max(na.rm = T)
  sig_thresh_statespace_LM =
    required_sigs %>% filter(str_detect(pred_col_used, "state_space") & str_detect(pred_col_used, "_LM")) %>%
    pull(sig_thresh_current) %>%
    max(na.rm = T)
  sig_thresh_statespace_GLM =
    required_sigs %>% filter(str_detect(pred_col_used, "state_space") & str_detect(pred_col_used, "GLM")) %>%
    pull(sig_thresh_current) %>%
    max(na.rm = T)
  sig_thresh_macro_LM =
    required_sigs %>% filter(str_detect(pred_col_used, "Macro") & str_detect(pred_col_used, "_LM")) %>%
    pull(sig_thresh_current) %>%
    max(na.rm = T)
  sig_thresh_macro_GLM =
    required_sigs %>% filter(str_detect(pred_col_used, "Macro") & str_detect(pred_col_used, "GLM")) %>%
    pull(sig_thresh_current) %>%
    max(na.rm = T)

  if(is.infinite(sig_thresh_AR_LM)) {sig_thresh_AR_LM <- 0.1}
  if(is.infinite(sig_thresh_AR_GLM)) {sig_thresh_AR_GLM <- 0.1}
  if(is.infinite(sig_thresh_Copula_LM)) {sig_thresh_Copula_LM <- 0.1}
  if(is.infinite(sig_thresh_Copula_GLM)) {sig_thresh_Copula_GLM <- 0.1}
  if(is.infinite(sig_thresh_statespace_LM)) {sig_thresh_statespace_LM <- 0.1}
  if(is.infinite(sig_thresh_statespace_GLM)) {sig_thresh_statespace_GLM <- 0.1}
  if(is.infinite(sig_thresh_macro_LM)) {sig_thresh_macro_LM <- 0.1}
  if(is.infinite(sig_thresh_macro_GLM)) {sig_thresh_macro_GLM <- 0.1}

  Single_Asset_V3_Gen_Model(
    Indices_Metals_Bonds = Indices_Metals_Bonds,
    actual_wins_losses = actual_wins_losses,
    asset_of_interest = asset_of_interest,
    actuals_periods_needed = actuals_periods_needed,
    training_end_date = training_end_date,
    bin_threshold = bin_threshold_vec[1],
    rolling_mean_pred_period = 500,
    correlation_rolling_periods = correlation_rolling_periods,
    state_space_periods = state_space_periods,
    state_space_rolling = state_space_rolling,
    copula_assets = correlation_assets_current,
    raw_macro_data = raw_macro_data,
    base_path = "C:/Users/Nikhil Chandra/Documents/trade_data/Day_Trader_Single_Asset_V3_Expanded_Models/",
    sig_thresh_AR_LM = sig_thresh_AR_LM,
    sig_thresh_AR_GLM = sig_thresh_AR_GLM,
    sig_thresh_Copula_LM = sig_thresh_Copula_LM,
    sig_thresh_Copula_GLM = sig_thresh_macro_GLM,
    sig_thresh_statespace_LM = sig_thresh_statespace_LM,
    sig_thresh_statespace_GLM = sig_thresh_statespace_GLM,
    sig_thresh_macro_LM = sig_thresh_macro_LM,
    sig_thresh_macro_GLM = sig_thresh_macro_GLM
  )

  simulated_probs <-
    Single_Asset_V3_Read_in_Probs_with_Macro(
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
      base_path = "C:/Users/Nikhil Chandra/Documents/trade_data/Day_Trader_Single_Asset_V3_Expanded_Models/"
    )

  simulated_probs <-
    simulated_probs %>%
    reduce(bind_rows) %>%
    mutate(
      training_end_date = training_end_date,
      date_for_true_simualtion = date_for_true_simualtion
    )

  if(j == 1 & reset_DB == TRUE) {

    db_con <- connect_db(result_db_path)
    write_table_sql_lite(.data = simulated_probs,
                         table_name = "SIM_RESULTS_WORK_PC",
                         conn = db_con,
                         overwrite_true = TRUE)
    DBI::dbDisconnect(db_con)

  } else {

    db_con <- connect_db(result_db_path)
    append_table_sql_lite(.data = simulated_probs,
                          table_name = "SIM_RESULTS_WORK_PC",
                          conn = db_con)
    DBI::dbDisconnect(db_con)

  }

}
