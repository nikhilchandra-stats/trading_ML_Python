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
start_date = "2019-06-01"
end_date = today() %>% as.character()

bin_factor = NULL
stop_value_var = 10
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

# single_asset_algo_generate_models(
#   All_Daily_Data = All_Daily_Data,
#   Indices_Metals_Bonds = Indices_Metals_Bonds,
#   raw_macro_data = raw_macro_data,
#   currency_conversion = currency_conversion,
#   asset_infor = asset_infor,
#   # start_index = 1,
#   start_index = 1,
#   end_index = 38,
#   risk_dollar_value = 15,
#   trade_direction = "Long",
#   stop_value_var = 10,
#   profit_value_var = 60,
#   period_var = 24,
#   bin_var_col = c("period_return_24_Price", "period_return_40_Price", "period_return_50_Price"),
#   # date_train_end_pre = as.character(as_date("2023-06-01") + days(24) ),
#   # date_train_phase_2_end_pre = as.character(as_date("2024-06-01") + days(24) ),
#   # training_date_start_post = as.character(as_date("2024-07-04") + days(24) ),
#   # training_date_end_post = as.character(as_date("2025-09-01") + days(24) ),
#   # test_end_date = as.character(today()),
#
#   date_train_end_pre = as.character(as_date("2021-01-01")  ),
#   date_train_phase_2_end_pre = as.character(as_date("2022-01-01")  ),
#   training_date_start_post = as.character(as_date("2022-01-01")  ),
#   training_date_end_post = as.character(as_date("2023-01-01")  ),
#   test_end_date = as.character(today()),
#
#   post_bins_cols =
#     c("period_return_24_Price", "period_return_40_Price", "period_return_50_Price"),
#   post_dependant_threshold = 0,
#   model_data_store_path = "C:/Users/nikhi/Documents/trade_data/Day_Trader_Single_Asset_V2_trade_store_stop_2.db",
#   save_path = "C:/Users/nikhi/Documents/trade_data/Day_Trader_Single_Asset_V2_trade_store_stop_2"
# )

Cut_Down_Data <-
  Indices_Metals_Bonds %>%
  map(~ .x %>% filter(Date >= "2019-01-01"))

post_preds_all_rolling_and_originals <-
  single_asset_algo_generate_preds(
    All_Daily_Data = All_Daily_Data,
    Indices_Metals_Bonds = Cut_Down_Data,
    raw_macro_data = raw_macro_data,
    currency_conversion = currency_conversion,
    asset_infor = asset_infor,
    # start_index = 1,
    # end_index = 40,
    start_index = 1,
    end_index = 19,
    risk_dollar_value = 15,
    trade_direction = "Long",
    stop_value_var = 10,
    profit_value_var = 60,
    period_var = 24,
    bin_var_col = c("period_return_24_Price", "period_return_40_Price", "period_return_50_Price"),

    # date_train_end_pre = as.character(as_date("2023-06-01")  ),
    # date_train_phase_2_end_pre = as.character(as_date("2024-06-01")),
    # training_date_start_post = as.character(as_date("2024-07-04")),
    # training_date_end_post = as.character(as_date("2025-09-01")),
    # test_end_date = as.character(today()),

    date_train_end_pre = as.character(as_date("2021-01-01")  ),
    date_train_phase_2_end_pre = as.character(as_date("2022-01-01")  ),
    training_date_start_post = as.character(as_date("2022-01-01")  ),
    training_date_end_post = as.character(as_date("2023-01-01")  ),
    test_end_date = as.character(today()),

    post_dependant_var = "period_return_50_Price",
    post_bins_cols =
      c("period_return_24_Price", "period_return_40_Price", "period_return_50_Price"),
    post_dependant_threshold = 0,
    model_data_store_path = "C:/Users/nikhi/Documents/trade_data/Day_Trader_Single_Asset_V2_trade_store_stop_2.db",
    save_path = "C:/Users/nikhi/Documents/trade_data/Day_Trader_Single_Asset_V2_trade_store_stop_2"
  )

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
    stop_factor = 5,
    profit_factor = 15,
    risk_dollar_value = 10,
    trade_direction = "Long",
    currency_conversion = currency_conversion,
    asset_infor = asset_infor,
    periods_ahead = period_var
  )

post_preds_all_rolling_and_originals_2 <-
  post_preds_all_rolling_and_originals %>%
  mutate(
    Averaged_Multi_prob_Momentum =
      (pred_index_2 + pred_daily_2 + pred_technical_2  +
         pred_index_4 + pred_daily_4 + pred_technical_4  +
         pred_index_6 + pred_daily_6 + pred_technical_6  )/9,

    Averaged_Multi_prob_GLM =
      (pred_index_2 + pred_daily_2 + pred_technical_2 + pred_copula_2 +
         pred_index_4 + pred_daily_4 + pred_technical_4 + pred_copula_4 +
         pred_index_6 + pred_daily_6 + pred_technical_6 + pred_copula_6 )/12,

    Averaged_Multi_prob_macro_GLM =
      (pred_index_2 + pred_daily_2 + pred_technical_2 + pred_copula_2 + pred_macro_2 +
         pred_index_4 + pred_daily_4 + pred_technical_4 + pred_copula_4 +  pred_macro_4 +
         pred_index_6 + pred_daily_6 + pred_technical_6 + pred_copula_6 + pred_macro_6  )/15,

    Averaged_Multi_prob_Momentum_Marco =
      (pred_index_2 + pred_daily_2 + pred_technical_2  + pred_macro_2 +
         pred_index_4 + pred_daily_4 + pred_technical_4  +  pred_macro_4 +
         pred_index_6 + pred_daily_6 + pred_technical_6  + pred_macro_6  )/12,

    Averaged_FULL_GLM =
      (pred_index_2 + pred_daily_2 + pred_technical_2 + pred_copula_2  + pred_GLM_period_return_24_Price +
         pred_index_4 + pred_daily_4 + pred_technical_4 + pred_copula_4  + pred_GLM_period_return_40_Price +
         pred_index_6 + pred_daily_6 + pred_technical_6 + pred_copula_6 + pred_GLM_period_return_50_Price
      )/15,

    Averaged_FULL_LM =
      (pred_index_1 + pred_daily_1 + pred_technical_1 + pred_copula_1   +
         pred_index_3 + pred_daily_3 + pred_technical_3 + pred_copula_3   +
         pred_index_5 + pred_daily_5 + pred_technical_5 + pred_copula_5
      )/12

  )
generated_preds <-
  post_preds_all_rolling_and_originals_2 %>%
  mutate(
    across(.cols = c(Date),
           .fns = ~ as_datetime(., tz = "Australia/Canberra"))
  ) %>%
  filter(
    Date >= as.character(as_date("2023-01-01")  )
  )



trade_statement <-
  "
  (
  pred_LM_period_return_24_Price >
            mean_50_pred_LM_period_return_24_Price + sd_500_pred_LM_period_return_24_Price*1.85 &
  Asset == 'EUR_USD'
  )|
  (
  pred_LM_period_return_40_Price >
            mean_50_pred_LM_period_return_40_Price + sd_500_pred_LM_period_return_40_Price*1.85 &
  Asset == 'EUR_USD'
  )|
  (
  pred_LM_period_return_50_Price >
            mean_50_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*1.85 &
  Asset == 'EUR_USD'
  )|
  (
  pred_technical_6 >= 0.675 &
  pred_technical_6 <= 0.7 &
  Asset == 'EUR_USD'
  )|
  (
  Averaged_Multi_prob_macro_GLM >= 0.625 &
  Averaged_Multi_prob_macro_GLM <= 0.64 &
  Asset == 'EUR_USD'
  )|
  (
  Averaged_Multi_prob_Momentum_Marco > 0.52 &
  Averaged_Multi_prob_Momentum_Marco < 0.55 &
  Asset == 'EUR_USD'
  )|
  (
  pred_GLM_period_return_50_Price > 0.56 &
  pred_GLM_period_return_50_Price < 0.57 &
  Asset == 'EUR_USD'
  )|
  (
 pred_GLM_period_return_50_Price > mean_50_pred_GLM_period_return_50_Price &
 pred_GLM_period_return_50_Price > mean_500_pred_GLM_period_return_50_Price &
 pred_GLM_period_return_50_Price > mean_200_pred_GLM_period_return_50_Price &
 pred_GLM_period_return_50_Price > mean_100_pred_GLM_period_return_50_Price &
 mean_200_pred_GLM_period_return_50_Price < mean_500_pred_GLM_period_return_50_Price &
 mean_100_pred_GLM_period_return_50_Price < mean_200_pred_GLM_period_return_50_Price &
 pred_LM_period_return_50_Price > mean_50_pred_LM_period_return_50_Price &
 pred_LM_period_return_50_Price > mean_500_pred_LM_period_return_50_Price &
 pred_LM_period_return_50_Price > mean_200_pred_LM_period_return_50_Price &
 pred_LM_period_return_50_Price > mean_100_pred_LM_period_return_50_Price &
 Asset == 'EUR_USD'
 )|
  (
  pred_LM_period_return_50_Price >
            mean_50_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*1.7 &
  pred_LM_period_return_50_Price <
            mean_50_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*10 &
  Asset == 'EUR_USD'
  )|
  (
  pred_LM_period_return_50_Price >
            mean_200_pred_LM_period_return_50_Price + sd_200_pred_LM_period_return_50_Price*2 &
  pred_LM_period_return_50_Price <
            mean_200_pred_LM_period_return_50_Price + sd_200_pred_LM_period_return_50_Price*10 &
  Asset == 'EUR_USD'
  )|
  (
  pred_LM_period_return_50_Price >
            mean_500_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*1.85 &
  pred_LM_period_return_50_Price <
            mean_500_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*10 &
  Asset == 'EUR_USD'
  )|
  (
  pred_GLM_period_return_50_Price > 0.52 &
  pred_GLM_period_return_50_Price < 0.6 &
  Asset == 'EU50_EUR'
  )|
  (
  pred_GLM_period_return_40_Price > 0.54 &
  pred_GLM_period_return_40_Price < 0.59 &
  Asset == 'EU50_EUR'
  )|
  (
  pred_LM_period_return_50_Price > 4 &
  pred_LM_period_return_50_Price < 6 &
  Asset == 'EU50_EUR'
  )|
  (
  pred_combined_6 >= 0.99999999999 &
  pred_combined_6 <= 1 &
  Asset == 'EU50_EUR'
  )|
  (
  pred_combined_4 >= 0.9999 &
  pred_combined_4 <= 1 &
  Asset == 'EU50_EUR'
  )|
  (
  pred_index_6 >= 0.545 &
  pred_index_6 <= 0.57 &
  Asset == 'EU50_EUR'
  )|
  (
  pred_GLM_period_return_50_Price > 0.5 &
  pred_GLM_period_return_50_Price < 0.55 &
  Asset == 'SPX500_USD'
  )|
  (
  pred_GLM_period_return_50_Price > 0.6 &
  pred_GLM_period_return_50_Price < 0.62 &
  Asset == 'SPX500_USD'
  )|
  (
  pred_LM_period_return_50_Price >
            mean_50_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*1.75 &
  Asset == 'SPX500_USD'
  )|
  (
  pred_LM_period_return_40_Price >
            mean_50_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_40_Price*1.25 &
  pred_LM_period_return_40_Price <
            mean_50_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_40_Price*1.75 &
  Asset == 'SPX500_USD'
  )|
  (
  pred_GLM_period_return_50_Price >
            mean_50_pred_GLM_period_return_50_Price + sd_500_pred_GLM_period_return_50_Price*1.65 &
  Asset == 'SPX500_USD'
  )|
  (
  pred_combined_6 >= 0.99 &
  Averaged_Multi_prob_Momentum_Marco > 0.5 &
  Averaged_Multi_prob_Momentum_Marco < 0.8 &
  Asset == 'SPX500_USD'
  )|
  (
  pred_combined_6 >= 0.99 &
  Averaged_FULL_GLM > 0.5 &
  Averaged_FULL_GLM < 0.55 &
  Asset == 'SPX500_USD'
  )|
  (
  pred_GLM_period_return_50_Price > mean_50_pred_GLM_period_return_50_Price &
  pred_GLM_period_return_50_Price > mean_500_pred_GLM_period_return_50_Price &
  pred_GLM_period_return_50_Price > mean_200_pred_GLM_period_return_50_Price &
  pred_GLM_period_return_50_Price > mean_100_pred_GLM_period_return_50_Price &
  mean_100_pred_GLM_period_return_50_Price < mean_500_pred_GLM_period_return_50_Price &
  mean_200_pred_GLM_period_return_50_Price < mean_500_pred_GLM_period_return_50_Price &
  mean_100_pred_GLM_period_return_50_Price < mean_200_pred_GLM_period_return_50_Price &

  pred_LM_period_return_50_Price > mean_50_pred_LM_period_return_50_Price &
  pred_LM_period_return_50_Price > mean_500_pred_LM_period_return_50_Price &
  pred_LM_period_return_50_Price > mean_200_pred_LM_period_return_50_Price &
  pred_LM_period_return_50_Price > mean_100_pred_LM_period_return_50_Price &
  Asset == 'SPX500_USD'
  )|

  (
  pred_GLM_period_return_50_Price > 0.95 &
  pred_GLM_period_return_50_Price < 0.99 &
  Asset == 'US2000_USD'
  )|
  (
  pred_LM_period_return_50_Price >
            mean_50_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*1.15 &
  pred_LM_period_return_50_Price <
            mean_50_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*1.75 &
  Asset == 'US2000_USD'
  )|
  (
   Averaged_FULL_LM >= 95 &
   Averaged_FULL_LM <= 1000 &
   Asset == 'US2000_USD'
  )|
  (
  pred_daily_6 > 0.99999 &
  pred_index_6 > 0.99995 &
  pred_technical_6 > 0.75 &
  Asset == 'US2000_USD'
  )|
  (
  pred_combined_6 >= 0.9999999999999995 &
  Asset == 'US2000_USD'
  )|

  (
  pred_GLM_period_return_50_Price > 0.85 &
  pred_GLM_period_return_50_Price < 0.9 &
  Asset == 'USB10Y_USD'
  )|
  (
  pred_technical_6 >= 0.75 &
  pred_technical_6 <= 0.86 &
  Asset == 'USB10Y_USD'
  )|
  (
  pred_index_6 >= 0.96 &
  Asset == 'USB10Y_USD'
  )|
  (
  pred_index_4 >= 0.55 &
  Asset == 'USB10Y_USD'
  )|
   (
   pred_LM_period_return_50_Price > mean_50_pred_LM_period_return_50_Price &
   pred_LM_period_return_50_Price > mean_500_pred_LM_period_return_50_Price &
   pred_LM_period_return_50_Price > mean_200_pred_LM_period_return_50_Price &
   pred_LM_period_return_50_Price > mean_100_pred_LM_period_return_50_Price &
   pred_GLM_period_return_50_Price > mean_50_pred_GLM_period_return_50_Price &
   pred_GLM_period_return_50_Price > mean_500_pred_GLM_period_return_50_Price &
   pred_GLM_period_return_50_Price > mean_200_pred_GLM_period_return_50_Price &
   pred_GLM_period_return_50_Price > mean_100_pred_GLM_period_return_50_Price &
   mean_100_pred_LM_period_return_50_Price > mean_200_pred_LM_period_return_50_Price &
   mean_100_pred_LM_period_return_50_Price > mean_500_pred_LM_period_return_50_Price &
   mean_200_pred_LM_period_return_50_Price > mean_500_pred_LM_period_return_50_Price &
   mean_50_pred_LM_period_return_50_Price > mean_200_pred_LM_period_return_50_Price &
   mean_50_pred_LM_period_return_50_Price > mean_500_pred_LM_period_return_50_Price &
   mean_100_pred_GLM_period_return_50_Price > mean_200_pred_GLM_period_return_50_Price &
   mean_100_pred_GLM_period_return_50_Price > mean_500_pred_GLM_period_return_50_Price &
   Asset == 'USB10Y_USD'
   )|

  (
  pred_LM_period_return_50_Price >
            mean_50_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*1.25 &
  Asset == 'USD_JPY'
  )|
  (
  pred_GLM_period_return_50_Price >= 0.6 &
  pred_GLM_period_return_50_Price < 0.675 &
  Asset == 'USD_JPY'
  )|
  (
  pred_GLM_period_return_40_Price >= 0.61 &
  pred_GLM_period_return_40_Price < 0.65 &
  Asset == 'USD_JPY'
  )|
  (
  pred_technical_6 >= 0.575 &
  pred_technical_6 <= 1 &
  Asset == 'USD_JPY'
  )|
  (
  pred_combined_6 >= 0.9 &
  pred_combined_6 <= 0.95 &
  Asset == 'USD_JPY'
  )|
  (
  pred_combined_4 >= 0.65 &
  pred_combined_4 <= 1 &
  Asset == 'USD_JPY'
  )|
  (
  pred_index_5 >= 0.25 &
  pred_index_5 <= 0.9 &
  Asset == 'USD_JPY'
  )|

  (
  pred_LM_period_return_50_Price <
            mean_500_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*1.65 &
  pred_LM_period_return_50_Price >
            mean_500_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*1.1 &
  Asset == 'AUD_USD'
  )|
  (
  pred_GLM_period_return_40_Price >= 0.999997 &
  pred_GLM_period_return_40_Price <= 0.999999 &
  Asset == 'AUD_USD'
  )|
  (
  pred_technical_6 >= 0.8 &
  pred_technical_6 <= 1 &
  Asset == 'AUD_USD'
  )|
  (
  Averaged_Multi_prob_macro_GLM >= 0.55 &
  Averaged_Multi_prob_macro_GLM <= 0.6 &
  Asset == 'AUD_USD'
  )|
  (
  pred_index_6 > 0.7 &
  pred_index_6 < 1 &
  Asset == 'AUD_USD'
  )|
  (
  pred_index_4 > 0.99 &
  pred_index_4 < 1 &
  Asset == 'AUD_USD'
  )|
  (
  pred_daily_6 > 0.65 &
  pred_daily_6 < 0.75 &
  Asset == 'AUD_USD'
  )|
  (
  pred_daily_4 > 0.85 &
  pred_daily_4 < 1 &
  Asset == 'AUD_USD'
  )|


 (
 pred_GLM_period_return_50_Price >= 0.99 &
 pred_GLM_period_return_50_Price < 0.999 &
 Asset == 'EUR_GBP'
 )|
 (
 pred_combined_6 >= 0.99999999999 &
 pred_combined_6 <= 1 &
 Asset == 'EUR_GBP'
 )|
 (
 pred_technical_6 >= 0.7 &
 pred_technical_6 <= 1 &
 Asset == 'EUR_GBP'
 )|
 (
 pred_technical_4 >= 0.675 &
 pred_technical_4 <= 1 &
 Asset == 'EUR_GBP'
 )|
 (
 pred_technical_5 >= 7.5 &
 pred_technical_5 <= 1000 &
 Asset == 'EUR_GBP'
 )|
 (
 pred_technical_3 >= 6 &
 pred_technical_3 <= 1000 &
 Asset == 'EUR_GBP'
 )|
 (
 pred_daily_6 >= 0.925 &
 pred_daily_6 < 1 &
 Asset == 'EUR_GBP'
 )|
 (
 pred_daily_4 >= 0.925 &
 pred_daily_4 < 1 &
 Asset == 'EUR_GBP'
 )|
 (
 pred_daily_2 >= 0.9125 &
 pred_daily_2 < 1 &
 Asset == 'EUR_GBP'
 )|
 (
  pred_LM_period_return_50_Price >
            mean_50_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*2 &
  Asset == 'EUR_GBP'
  )|
 (
  mean_50_pred_GLM_period_return_50_Price >
    mean_100_pred_GLM_period_return_50_Price + 0.485*sd_100_pred_GLM_period_return_50_Price &
  mean_50_pred_GLM_period_return_50_Price <
    mean_100_pred_GLM_period_return_50_Price + 0.7*sd_100_pred_GLM_period_return_50_Price &
  mean_3_pred_GLM_period_return_50_Price > mean_200_pred_GLM_period_return_50_Price &
  mean_3_pred_GLM_period_return_50_Price > mean_100_pred_GLM_period_return_50_Price &
  Asset == 'EUR_GBP'
  )|
 (
  mean_3_pred_GLM_period_return_50_Price < mean_200_pred_GLM_period_return_50_Price &
  mean_3_pred_GLM_period_return_50_Price > mean_100_pred_GLM_period_return_50_Price &
  mean_200_pred_GLM_period_return_50_Price < mean_500_pred_GLM_period_return_50_Price &
  mean_100_pred_GLM_period_return_50_Price < mean_500_pred_GLM_period_return_50_Price &
  Asset == 'EUR_GBP'
 )|
  (
  pred_combined_6 >= 0.5 &
  pred_combined_6 <= 0.9999 &
  pred_combined_4 >= 0.5 &
  pred_GLM_period_return_50_Price > mean_100_pred_GLM_period_return_50_Price &
  Asset == 'EUR_GBP'
  )|
 (
 pred_technical_6 >= 0.7 &
 pred_technical_6 < 1 &
 pred_technical_4 >= 0.65 &
 pred_technical_4 < 1 &
 pred_technical_2 >= 0.65 &
 pred_technical_2 < 1 &
 Asset == 'EUR_GBP'
 )|
   (
  pred_LM_period_return_50_Price >
            mean_50_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*0.25 &
  pred_LM_period_return_50_Price <
            mean_50_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*0.4 &
  Asset == 'AU200_AUD'
  )|
 (
  mean_50_pred_GLM_period_return_50_Price >
    mean_100_pred_GLM_period_return_50_Price + 0.59*sd_100_pred_GLM_period_return_50_Price &
  mean_50_pred_GLM_period_return_50_Price <
    mean_100_pred_GLM_period_return_50_Price + 0.7*sd_100_pred_GLM_period_return_50_Price &
  mean_3_pred_GLM_period_return_50_Price > mean_200_pred_GLM_period_return_50_Price &
  Asset == 'AU200_AUD'
  )|
  (
  pred_combined_6 >= 0.5 &
  pred_combined_6 <= 0.9999 &
  pred_combined_4 >= 0.5 &
  pred_GLM_period_return_50_Price > mean_100_pred_GLM_period_return_50_Price &
  Asset == 'AU200_AUD'
  )|
  (
  pred_technical_6 >= 0.6 &
  pred_technical_6 < 0.85 &
  pred_technical_4 >= 0.6 &
  pred_technical_4 < 0.725 &
  Asset == 'AU200_AUD'
  )|

  (
  pred_combined_6 >= 0.5 &
  pred_GLM_period_return_50_Price > 0.5 &
  pred_GLM_period_return_50_Price < 0.6 &
  pred_daily_6 > 0.5 &
  pred_index_6 > 0.5 &
  Asset == 'EUR_AUD'
  )|
 (
   pred_LM_period_return_50_Price >
            mean_500_pred_LM_period_return_50_Price + 0*sd_500_pred_LM_period_return_50_Price &
  pred_LM_period_return_50_Price <
            mean_500_pred_LM_period_return_50_Price + 0.3*sd_500_pred_LM_period_return_50_Price &
  mean_3_pred_LM_period_return_50_Price > mean_200_pred_LM_period_return_50_Price &
  mean_3_pred_LM_period_return_50_Price > mean_100_pred_LM_period_return_50_Price &
  Asset == 'EUR_AUD'
  )|
 (
  pred_GLM_period_return_50_Price >
    mean_100_pred_GLM_period_return_50_Price + 2.1*sd_100_pred_GLM_period_return_50_Price &
  pred_GLM_period_return_50_Price <
    mean_100_pred_GLM_period_return_50_Price + 10*sd_100_pred_GLM_period_return_50_Price &
  mean_3_pred_GLM_period_return_50_Price > mean_200_pred_GLM_period_return_50_Price &
  mean_3_pred_GLM_period_return_50_Price > mean_100_pred_GLM_period_return_50_Price &
  Asset == 'EUR_AUD'
  )|
 (
 pred_technical_6 >= 0.6 &
 pred_technical_6 < 0.8 &
 pred_technical_4 >= 0.55 &
 pred_technical_4 < 0.65 &
 Asset == 'EUR_AUD'
 )|
 (
 pred_GLM_period_return_50_Price > mean_50_pred_GLM_period_return_50_Price &
 pred_GLM_period_return_50_Price > mean_500_pred_GLM_period_return_50_Price &
 pred_GLM_period_return_50_Price > mean_200_pred_GLM_period_return_50_Price &
 pred_GLM_period_return_50_Price > mean_100_pred_GLM_period_return_50_Price &
 mean_100_pred_GLM_period_return_50_Price < mean_500_pred_GLM_period_return_50_Price &
 mean_200_pred_GLM_period_return_50_Price < mean_500_pred_GLM_period_return_50_Price &
 mean_100_pred_GLM_period_return_50_Price < mean_200_pred_GLM_period_return_50_Price &
 pred_LM_period_return_50_Price > mean_50_pred_LM_period_return_50_Price &
 pred_LM_period_return_50_Price > mean_500_pred_LM_period_return_50_Price &
 pred_LM_period_return_50_Price > mean_200_pred_LM_period_return_50_Price &
 pred_LM_period_return_50_Price > mean_100_pred_LM_period_return_50_Price &
 Asset == 'EUR_AUD'
 )|
  (
  pred_combined_6 >= 0.95 &
  pred_daily_6 > 0.95 &
  pred_index_6 > 0.95 &
  Asset == 'WTICO_USD'
  )|
 (
  pred_GLM_period_return_50_Price >
    mean_100_pred_GLM_period_return_50_Price + 1.25*sd_100_pred_GLM_period_return_50_Price &
  pred_GLM_period_return_50_Price <
    mean_100_pred_GLM_period_return_50_Price + 1.5*sd_100_pred_GLM_period_return_50_Price &
  mean_3_pred_GLM_period_return_50_Price > mean_200_pred_GLM_period_return_50_Price &
  mean_3_pred_GLM_period_return_50_Price > mean_100_pred_GLM_period_return_50_Price &
  Asset == 'WTICO_USD'
  )|
 (
 pred_technical_6 >= 0.52 &
 pred_technical_6 < 0.65 &
 pred_technical_4 >= 0.52 &
 pred_technical_4 < 0.6 &
 Asset == 'WTICO_USD'
 )|
 (
 pred_GLM_period_return_50_Price > mean_50_pred_GLM_period_return_50_Price &
 pred_GLM_period_return_50_Price > mean_500_pred_GLM_period_return_50_Price &
 pred_GLM_period_return_50_Price > mean_200_pred_GLM_period_return_50_Price &
 pred_GLM_period_return_50_Price > mean_100_pred_GLM_period_return_50_Price &
 mean_100_pred_GLM_period_return_50_Price < mean_500_pred_GLM_period_return_50_Price &
 mean_200_pred_GLM_period_return_50_Price < mean_500_pred_GLM_period_return_50_Price &
 mean_100_pred_GLM_period_return_50_Price < mean_200_pred_GLM_period_return_50_Price &
 Asset == 'WTICO_USD'
 )|
  (
  pred_LM_period_return_50_Price >
            mean_50_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*0.75 &
  pred_LM_period_return_50_Price <
            mean_50_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*0.9 &
  Asset == 'UK100_GBP'
  )|
  (
  pred_LM_period_return_50_Price >
            mean_50_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*2 &
  pred_LM_period_return_50_Price <
            mean_50_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*10 &
  Asset == 'UK100_GBP'
  )|
  (
  pred_combined_6 >= 0.99 &
  pred_combined_6 <= 0.999999 &
  # pred_combined_6 <= 0.99999999999 &
  Asset == 'UK100_GBP'
  )|
  (
 pred_technical_6 >= 0.6 &
 pred_technical_6 < 0.7 &
 Asset == 'UK100_GBP'
 )|
   (
  pred_combined_6 >= 0.425 &
  pred_GLM_period_return_50_Price > 0.425 &
  pred_daily_6 > 0.425 &
  pred_index_6 > 0.425 &
  Asset == 'UK100_GBP'
  )|
   (
  pred_combined_1 >= 360 &
  pred_combined_1 <= 390 &
  # pred_combined_6 <= 0.99999999999 &
  Asset == 'UK100_GBP'
  )|
  (
 pred_technical_2 >= 0.6 &
 pred_technical_2 < 0.68 &
 Asset == 'UK100_GBP'
 )|
   (
 pred_daily_6 >= 0.6 &
 pred_daily_6 < 0.99 &
 Asset == 'UK100_GBP'
 )|

  (
  pred_LM_period_return_50_Price >
            mean_500_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*1.5 &
  pred_LM_period_return_50_Price <
            mean_500_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*10 &
  Asset == 'USD_CAD'
  )|
  (
  pred_LM_period_return_50_Price >
            mean_200_pred_LM_period_return_50_Price + sd_200_pred_LM_period_return_50_Price*1.75 &
  pred_LM_period_return_50_Price <
            mean_200_pred_LM_period_return_50_Price + sd_200_pred_LM_period_return_50_Price*10 &
  Asset == 'USD_CAD'
  )|
 (
  pred_LM_period_return_50_Price >
            mean_50_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*1.15 &
  pred_LM_period_return_50_Price <
            mean_50_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*10 &
  Asset == 'USD_CAD'
  )|
 (
 pred_technical_6 >= 0.6125 &
 pred_technical_6 < 1 &
 pred_technical_4 >= 0.6125 &
 pred_technical_4 < 1 &
 Asset == 'USD_CAD'
 )|
  (
  pred_GLM_period_return_50_Price > 0.99 &
  pred_combined_6 > 0.99 &
  pred_daily_6 > 0.5 &
  Asset == 'USD_CAD'
  )|
  (
  pred_combined_6 >= 0.999 &
  pred_GLM_period_return_50_Price > 0.5 &
  Asset == 'USD_CAD'
  )|
    (
  pred_combined_2 >= 0.525 &
  pred_combined_2 < 0.575 &
  Asset == 'GBP_USD'
  )|
  (
  pred_combined_6 >= 0.6 &
  pred_combined_6 < 0.65 &
  Asset == 'GBP_USD'
  )|
  (
  pred_LM_period_return_50_Price >
            mean_500_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*0.55 &
  pred_LM_period_return_50_Price <
            mean_500_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*0.8 &
  Asset == 'GBP_USD'
  )|
  (
  pred_LM_period_return_50_Price >
            mean_200_pred_LM_period_return_50_Price + sd_200_pred_LM_period_return_50_Price*2.5 &
  pred_LM_period_return_50_Price <
            mean_200_pred_LM_period_return_50_Price + sd_200_pred_LM_period_return_50_Price*10 &
  Asset == 'GBP_USD'
  )|
  (
  pred_GLM_period_return_50_Price > 0.75 &
  pred_GLM_period_return_50_Price < 0.775 &
  Asset == 'GBP_USD'
  )|
 (
 pred_LM_period_return_50_Price > mean_50_pred_LM_period_return_50_Price &
 pred_LM_period_return_50_Price > mean_500_pred_LM_period_return_50_Price &
 pred_LM_period_return_50_Price > mean_200_pred_LM_period_return_50_Price &
 pred_LM_period_return_50_Price > mean_100_pred_LM_period_return_50_Price &
 mean_100_pred_LM_period_return_50_Price < mean_500_pred_LM_period_return_50_Price &
 pred_GLM_period_return_50_Price > mean_50_pred_GLM_period_return_50_Price &
 Asset == 'GBP_USD'
 )|
 (
 pred_LM_period_return_50_Price > mean_50_pred_LM_period_return_50_Price &
 pred_LM_period_return_50_Price > mean_500_pred_LM_period_return_50_Price &
 pred_LM_period_return_50_Price > mean_200_pred_LM_period_return_50_Price &
 pred_LM_period_return_50_Price > mean_100_pred_LM_period_return_50_Price &
 mean_50_pred_LM_period_return_50_Price < mean_200_pred_LM_period_return_50_Price &
 pred_GLM_period_return_50_Price > mean_50_pred_GLM_period_return_50_Price &
 Asset == 'GBP_USD'
 )|
  (
  pred_combined_2 >= 0.99 &
  pred_combined_6 >= 0.99 &
  pred_combined_4 >= 0.99 &
  mean_100_pred_LM_period_return_50_Price < mean_200_pred_LM_period_return_50_Price &
  mean_3_pred_LM_period_return_50_Price < mean_100_pred_LM_period_return_50_Price &
  Asset == 'GBP_CAD'
  )|
  (
  pred_daily_6 >= 0.99 &
  pred_daily_4 >= 0.99 &
  pred_daily_2 >= 0.955 &
  Asset == 'GBP_CAD'
  )|
  (
  pred_index_6 >= 0.525 &
  pred_index_4 >= 0.525 &
  Asset == 'GBP_CAD'
  )|
  (
  pred_LM_period_return_50_Price >
            mean_50_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*1 &
  pred_LM_period_return_50_Price <
            mean_50_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*1.5 &
  Asset == 'EUR_JPY'
  )|
  (
  pred_combined_6 >= 0.99999999 &
  pred_combined_4 >= 0.99999999 &
  mean_100_pred_LM_period_return_50_Price < mean_500_pred_LM_period_return_50_Price &
  mean_50_pred_LM_period_return_50_Price < mean_200_pred_LM_period_return_50_Price &
  mean_50_pred_LM_period_return_50_Price < mean_500_pred_LM_period_return_50_Price &
  mean_100_pred_GLM_period_return_50_Price < mean_500_pred_GLM_period_return_50_Price &
  mean_50_pred_GLM_period_return_50_Price < mean_200_pred_GLM_period_return_50_Price &
  mean_50_pred_GLM_period_return_50_Price < mean_500_pred_GLM_period_return_50_Price &
  Asset == 'EUR_JPY'
  )|
  (
  pred_index_6 >= 0.8 &
  pred_index_6 <= 0.85 &
  pred_index_4 >= 0.675 &
  pred_index_4 <= 1 &
  Asset == 'EUR_JPY'
  )|
 (
 pred_LM_period_return_50_Price > mean_50_pred_LM_period_return_50_Price &
 pred_LM_period_return_50_Price > mean_500_pred_LM_period_return_50_Price &
 pred_LM_period_return_50_Price > mean_200_pred_LM_period_return_50_Price &
 pred_LM_period_return_50_Price > mean_100_pred_LM_period_return_50_Price &
 mean_200_pred_LM_period_return_50_Price < mean_500_pred_LM_period_return_50_Price &
 mean_50_pred_LM_period_return_50_Price < mean_200_pred_LM_period_return_50_Price &
 Asset == 'EUR_JPY'
 )|
   (
  pred_LM_period_return_50_Price >
            mean_50_pred_LM_period_return_50_Price + sd_200_pred_LM_period_return_50_Price*0.9 &
  pred_LM_period_return_50_Price <
            mean_50_pred_LM_period_return_50_Price + sd_200_pred_LM_period_return_50_Price*1.2 &
  Asset == 'EUR_NZD'
  )|
  (
  pred_combined_6 >= 0.99999999 &
  pred_combined_4 >= 0.99999999 &
  pred_combined_2 >= 0.9999 &
  mean_100_pred_LM_period_return_50_Price < mean_500_pred_LM_period_return_50_Price &
  mean_50_pred_LM_period_return_50_Price < mean_200_pred_LM_period_return_50_Price &
  mean_50_pred_LM_period_return_50_Price < mean_500_pred_LM_period_return_50_Price &
  mean_50_pred_LM_period_return_50_Price < mean_100_pred_LM_period_return_50_Price &
  mean_100_pred_GLM_period_return_50_Price < mean_500_pred_GLM_period_return_50_Price &
  mean_100_pred_GLM_period_return_50_Price < mean_200_pred_GLM_period_return_50_Price &
  mean_50_pred_GLM_period_return_50_Price < mean_200_pred_GLM_period_return_50_Price &
  mean_50_pred_GLM_period_return_50_Price < mean_500_pred_GLM_period_return_50_Price &
  mean_50_pred_GLM_period_return_50_Price < mean_100_pred_GLM_period_return_50_Price &
  Asset == 'EUR_NZD'
  )|
 (
 pred_LM_period_return_50_Price > mean_50_pred_LM_period_return_50_Price &
 pred_LM_period_return_50_Price > mean_500_pred_LM_period_return_50_Price &
 pred_LM_period_return_50_Price > mean_200_pred_LM_period_return_50_Price &
 pred_LM_period_return_50_Price > mean_100_pred_LM_period_return_50_Price &
 mean_200_pred_LM_period_return_50_Price < mean_500_pred_LM_period_return_50_Price &
 mean_50_pred_LM_period_return_50_Price < mean_200_pred_LM_period_return_50_Price &
 Asset == 'EUR_NZD'
 )|
 (
 pred_GLM_period_return_50_Price > 0.50 &
 pred_GLM_period_return_50_Price >= 0.6 &
 Asset == 'XAG_USD'
 )|
 (
 pred_GLM_period_return_40_Price > 0.65 &
 pred_GLM_period_return_40_Price <= 1 &
 Asset == 'XAG_USD'
 )|
 (
 Averaged_Multi_prob_Momentum > 0.5 &
 Averaged_Multi_prob_Momentum <= 1 &
 Asset == 'XAG_USD'
 )|
  (
  pred_LM_period_return_50_Price >
            mean_50_pred_LM_period_return_50_Price + sd_200_pred_LM_period_return_50_Price*1.25 &
  pred_LM_period_return_50_Price <
            mean_50_pred_LM_period_return_50_Price + sd_200_pred_LM_period_return_50_Price*2 &
  Asset == 'XAG_USD'
  )|
  (
  pred_LM_period_return_50_Price >
            mean_100_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*1 &
  pred_LM_period_return_50_Price <
            mean_100_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*1.85 &
  Asset == 'XAG_USD'
  )|
  (
  pred_combined_6 >= 0.99999999 &
  pred_combined_4 >= 0.99999999 &
  pred_combined_2 >= 0.9999 &
  mean_100_pred_LM_period_return_50_Price < mean_500_pred_LM_period_return_50_Price &
  mean_50_pred_LM_period_return_50_Price < mean_200_pred_LM_period_return_50_Price &
  mean_50_pred_LM_period_return_50_Price < mean_500_pred_LM_period_return_50_Price &
  mean_50_pred_LM_period_return_50_Price < mean_100_pred_LM_period_return_50_Price &
  mean_100_pred_GLM_period_return_50_Price < mean_500_pred_GLM_period_return_50_Price &
  mean_100_pred_GLM_period_return_50_Price < mean_200_pred_GLM_period_return_50_Price &
  mean_50_pred_GLM_period_return_50_Price < mean_200_pred_GLM_period_return_50_Price &
  mean_50_pred_GLM_period_return_50_Price < mean_500_pred_GLM_period_return_50_Price &
  mean_50_pred_GLM_period_return_50_Price < mean_100_pred_GLM_period_return_50_Price &
  Asset == 'XAG_USD'
  )|
 (
 pred_LM_period_return_50_Price > mean_50_pred_LM_period_return_50_Price &
 pred_LM_period_return_50_Price > mean_500_pred_LM_period_return_50_Price &
 pred_LM_period_return_50_Price > mean_200_pred_LM_period_return_50_Price &
 pred_LM_period_return_50_Price > mean_100_pred_LM_period_return_50_Price &
 mean_200_pred_LM_period_return_50_Price < mean_500_pred_LM_period_return_50_Price &
 mean_50_pred_LM_period_return_50_Price < mean_200_pred_LM_period_return_50_Price &
 Asset == 'XAG_USD'
 )

  "

trade_statement <-
  "
  (
  pred_LM_period_return_50_Price >
            mean_50_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*2 &
  pred_LM_period_return_50_Price <
            mean_50_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*10 &
  Asset == 'UK100_GBP'
  )|
  (
  pred_combined_6 >= 0.99 &
  pred_combined_6 <= 0.999999 &
  # pred_combined_6 <= 0.99999999999 &
  Asset == 'UK100_GBP'
  )|
  (
 pred_technical_6 >= 0.6 &
 pred_technical_6 < 0.7 &
 Asset == 'UK100_GBP'
 )|
   (
  pred_combined_6 >= 0.425 &
  pred_GLM_period_return_50_Price > 0.425 &
  pred_daily_6 > 0.425 &
  pred_index_6 > 0.425 &
  Asset == 'UK100_GBP'
  )|
   (
  pred_combined_1 >= 360 &
  pred_combined_1 <= 390 &
  # pred_combined_6 <= 0.99999999999 &
  Asset == 'UK100_GBP'
  )|
  (
 pred_technical_2 >= 0.6 &
 pred_technical_2 < 0.68 &
 Asset == 'UK100_GBP'
 )|
   (
 pred_daily_6 >= 0.6 &
 pred_daily_6 < 0.99 &
 Asset == 'UK100_GBP'
 )

"

cumulative_returns_sim_data <-
  get_total_portfolio_summary(
    generated_preds = generated_preds %>%
      # filter(Asset == "UK100_GBP")
      filter( str_detect(Asset, "[A-Z]"))
    ,
    trade_statement = trade_statement,
    actual_wins_losses =actual_wins_losses %>%
      # filter(Asset == "UK100_GBP")
      filter( str_detect(Asset, "[A-Z]"))
    ,
    trade_direction = "Long",
    return_col = "period_return_50_Price"
  )

cumulative_returns_sim_data %>%
  ggplot(aes(x = Date, y = Cumulative_Return)) +
  geom_line() +
  facet_wrap(.~trade_col, scales = "free") +
  scale_y_continuous(n.breaks = 20) +
  theme_minimal()

asset_summaries_control <-
  get_asset_random_sim_returns(
    generated_preds = generated_preds %>%
      filter(Asset == "UK100_GBP")
      # filter( str_detect(Asset, "[A-Z]"))
    ,
    trade_statement = "str_detect(Asset, '[A-Z]')",
    actual_wins_losses = actual_wins_losses %>%
      filter(Asset == "UK100_GBP")
      # filter( str_detect(Asset, "[A-Z]"))
    ,
    trade_direction = "Long",
    return_col = "period_return_50_Price",
    simulations = 5000,
    samples = 50
  )

asset_summaries <-
  get_asset_random_sim_returns(
    generated_preds = generated_preds %>%
      filter(Asset == "UK100_GBP")
    # filter( str_detect(Asset, "[A-Z]"))
    ,
    trade_statement = trade_statement,
    actual_wins_losses = actual_wins_losses %>%
      filter(Asset == "UK100_GBP")
    # filter( str_detect(Asset, "[A-Z]"))
    ,
    trade_direction = "Long",
    return_col = "period_return_50_Price",
    simulations = 7000,
    samples = 50
  )


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
  )

