helpeR::load_custom_functions()

all_aud_symbols <- get_oanda_symbols() %>%
  keep(~ str_detect(.x, "AUD")|str_detect(.x, "USD_SEK|USD_NOK|USD_HUF|USD_ZAR|USD_CNY|USD_MXN"))
asset_infor <- get_instrument_info()
aud_assets <- read_all_asset_data_intra_day(
  asset_list_oanda = all_aud_symbols,
  save_path_oanda_assets = "C:/Users/nikhi/Documents/trade_data//oanda_data/",
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
start_date = "2015-06-01"
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
  )

result_db_path <- "C:/Users/nikhi/Documents/trade_data/Day_Trader_Single_Asset_V3_Expanded_Models/SIG_THRESH_FINDER_Redone_MULTI_COND.DB"
db_con <- connect_db(result_db_path)
Best_Sigs <- DBI::dbGetQuery(conn = db_con,
                             statement = "SELECT * FROM BEST_SIG_PER_ASSET" )
DBI::dbDisconnect(db_con)
rm(db_con)

actuals_periods_needed = c("period_return_50_Price")
correlation_rolling_periods = c(100,200, 300,400, 500)
state_space_periods = c(20, 40, 60, 100, 200,300, 400,  500)
state_space_rolling = c(100, 200, 300, 400)
# sig_thresh_vec <- c(0.99, 0.1, 0.05, 0.01, 10^-3, 10^-5, 10^-7, 10^-9)
sig_thresh_vec <- c(0.99, 10^-3, 10^-5, 10^-7, 10^-9)
bin_threshold_vec <- c(0)
safely_gen <- safely(Single_Asset_V3_Gen_Model_No_data_gen, otherwise = NULL)
result_db_path <- "C:/Users/nikhi/Documents/trade_data/Day_Trader_Single_Asset_V3_Expanded_Models/SIM_RESULTS_MSI_PC"
date_for_true_simualtion <- "2019-01-01"
training_end_date <- "2021-01-01"
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
    base_path = "C:/Users/nikhi/Documents/trade_data/Day_Trader_Single_Asset_V3_Expanded_Models/",
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
      base_path = "C:/Users/nikhi/Documents/trade_data/Day_Trader_Single_Asset_V3_Expanded_Models/"
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
                         table_name = "SIM_RESULTS_MSI_PC",
                         conn = db_con,
                         overwrite_true = TRUE)
    DBI::dbDisconnect(db_con)

  } else {

    db_con <- connect_db(result_db_path)
    append_table_sql_lite(.data = simulated_probs,
                          table_name = "SIM_RESULTS_MSI_PC",
                          conn = db_con)
    DBI::dbDisconnect(db_con)

  }

}


db_con <- connect_db(result_db_path)
generated_preds <- DBI::dbGetQuery(conn = db_con, statement = "SELECT * FROM SIM_RESULTS_MSI_PC")
DBI::dbDisconnect(db_con)
trade_direction <- "Long"

generated_preds <-
  generated_preds %>%
  mutate(
    across(.cols = c(Date, training_end_date, date_for_true_simualtion),
           .fns = ~ as_datetime(., tz = "Australia/Canberra"))
  ) %>%
  filter(Date > training_end_date) %>%
  filter(Date > date_for_true_simualtion)

#' get_control_trade_cumulative_returns
#'
#' @param generated_preds
#' @param trade_statement
#' @param trade_direction
#' @param return_col
#'
#' @returns
#' @export
#'
#' @examples
get_control_trade_cumulative_returns <-
  function(
    generated_preds = generated_preds,
    trade_statement = trade_statement,
    actual_wins_losses = actual_wins_losses,
    trade_direction = "Long",
    return_col = "period_return_50_Price"
    ) {

    control_data_asset <-
      generated_preds %>%
      left_join(
        actual_wins_losses %>%
          dplyr::select(Date, Asset, trade_col,  contains(return_col), volume_required) %>%
          filter(trade_col == trade_direction) %>%
          dplyr::select(-trade_col) %>%
          dplyr::select(Date, Asset,  contains(return_col), volume_required) %>%
          distinct()
      ) %>%
      distinct() %>%
      dplyr::select(Date, Asset, !!as.name(return_col), volume_required) %>%
      group_by(Asset) %>%
      arrange(Date, .by_group = TRUE) %>%
      group_by(Asset) %>%
      mutate(
        cumulative_return := cumsum(!!as.name(return_col))
      )

    trade_data <-
      generated_preds %>%
      mutate(
        trade_col =
          eval(parse(text = trade_statement)),
        trade_col =
          ifelse(trade_col == TRUE, trade_direction, paste0("No Trade ", trade_direction) )
      ) %>%
      left_join(
        actual_wins_losses %>%
          dplyr::select(Date, Asset, trade_col,  contains(return_col), volume_required) %>%
          filter(trade_col == trade_direction) %>%
          dplyr::select(-trade_col) %>%
          dplyr::select(Date, Asset,  contains(return_col), volume_required) %>%
          distinct()
      ) %>%
      filter(trade_col == trade_direction) %>%
      group_by(Asset) %>%
      arrange(Date, .by_group = TRUE) %>%
      group_by(Asset) %>%
      mutate(
        cumulative_return = cumsum(!!as.name(return_col))
      ) %>%
      dplyr::select(Date, Asset, trade_col,
                    !!as.name(return_col), cumulative_return, volume_required)

    return(
      list(
        "control_data_asset" = control_data_asset,
        "trade_data" = trade_data
      )
    )

  }

#' get_margin_details
#'
#' @param currency_conversion
#' @param actual_wins_losses
#' @param asset_infor
#'
#' @returns
#' @export
#'
#' @examples
get_margin_details <-
  function(
    currency_conversion = currency_conversion,
    actual_wins_losses = actual_wins_losses,
    asset_infor = asset_infor
    ) {

    margin_required <-
      actual_wins_losses %>%
      dplyr::select(Date, Asset, volume_required, Ask_Price) %>%
      mutate(ending_value = str_extract(Asset, "_[A-Z][A-Z][A-Z]"),
             ending_value = str_remove_all(ending_value, "_")
      ) %>%
      left_join(currency_conversion, by =c("ending_value" = "not_aud_asset")) %>%
      left_join(asset_infor %>% rename(Asset = name)) %>%
      mutate(
        minimumTradeSize_OG = as.numeric(minimumTradeSize),
        minimumTradeSize = abs(log10(as.numeric(minimumTradeSize))),
        marginRate = as.numeric(marginRate),
        pipLocation = as.numeric(pipLocation),
        displayPrecision = as.numeric(displayPrecision)
      ) %>%
      ungroup() %>%
      mutate(
        volume_adjustment = 1,
        AUD_Price =
          case_when(
            !is.na(adjusted_conversion) ~ (Ask_Price*adjusted_conversion)/volume_adjustment,
            TRUE ~ Ask_Price/volume_adjustment
          ),
        trade_value = AUD_Price*volume_required*marginRate,
        estimated_margin = trade_value
      ) %>%
      dplyr::select(Date, Asset, volume_required, estimated_margin)

    return(margin_required)

  }

get_total_portfolio_summary <-
  function(
    generated_preds = generated_preds,
    trade_statement = trade_statement,
    actual_wins_losses = actual_wins_losses,
    trade_direction = "Long",
    return_col = "period_return_50_Price"
    ) {

    sim_data_list <-
      get_control_trade_cumulative_returns(
        generated_preds = generated_preds,
        trade_statement = trade_statement,
        trade_direction = trade_direction,
        actual_wins_losses = actual_wins_losses,
        return_col = return_col
      )


    summarised_data <-
      sim_data_list %>%
      map(
        ~ .x %>%
          group_by(Date) %>%
          summarise(
            Total_Return = sum( !!as.name(return_col), na.rm= T)
          ) %>%
          arrange(Date) %>%
          mutate(
            Cumulative_Return = cumsum(Total_Return)
          )
      )

    summarised_data_combined <-
      summarised_data[[1]] %>%
      mutate(trade_col = "Control") %>%
      bind_rows(
        summarised_data[[2]] %>%
      mutate(trade_col = trade_direction)
      )

    return(summarised_data_combined)

  }

#' generate_random_sampling_returns
#'
#' @param timeseries_returns
#' @param simulations
#' @param samples
#' @param return_col
#'
#' @returns
#' @export
#'
#' @examples
generate_random_sampling_returns <-
  function(timeseries_returns = EUR_USD,
           simulations = 5000,
           samples = 100,
           return_col = "period_return_50_Price") {

    timeseries_returns <-
      timeseries_returns %>%
      mutate(
        win = ifelse(!!as.name(return_col) > 0, 1, 0)
      )

    asset_var <- timeseries_returns$Asset %>% unique() %>% as.character()

    returns_vec <- timeseries_returns %>% pull(!!as.name(return_col))
    win_loss_vec <- timeseries_returns %>% pull(win)

    wins_random <- numeric(simulations)
    returns_random <- numeric(simulations)
    average_win <- numeric(simulations)
    average_loss <- numeric(simulations)

    set.seed(simulations)

    for (i in 1:simulations) {

      wins_sampled <-
        win_loss_vec %>% sample(samples, replace = TRUE)
      returns_sampled <-
        returns_vec %>% sample(samples, replace = TRUE)

      wins_random[i] <- wins_sampled %>% sum(na.rm = T)
      returns_random[i] <- sum(returns_sampled, na.rm = T)
      average_win[i] <- returns_sampled[returns_sampled > 0] %>% mean(na.rm = T)
      average_loss[i] <- returns_sampled[returns_sampled <= 0] %>% mean(na.rm = T)
    }

    simulation_data_frame_random <-
      tibble(
        Asset = asset_var,
        samples_used = samples,
        wins_random = wins_random,
        returns_random = returns_random,
        average_win = average_win,
        average_loss = average_loss
      ) %>%
      mutate(
        win_perc =wins_random/samples
      ) %>%
      group_by(Asset, samples_used) %>%
      summarise(
        across(.cols =
                 c(wins_random, win_perc, average_win, average_loss),
               .fns = ~ mean(., na.rm = T)
               ),

        returns_random_mean = mean(returns_random, na.rm = T),
        returns_random_05 = quantile(returns_random, 0.05, na.rm = T),
        returns_random_25 = quantile(returns_random, 0.25, na.rm = T),
        returns_random_50 = quantile(returns_random, 0.5, na.rm = T),
        returns_random_75 = quantile(returns_random, 0.75, na.rm = T)
        )

    return(simulation_data_frame_random)

  }

#' get_asset_random_sim_returns
#'
#' @param generated_preds
#' @param trade_statement
#' @param trade_direction
#' @param return_col
#' @param simulations
#' @param samples
#'
#' @returns
#' @export
#'
#' @examples
get_asset_random_sim_returns <-
  function(
    generated_preds = generated_preds,
    trade_statement = trade_statement,
    actual_wins_losses = actual_wins_losses,
    trade_direction = "Long",
    return_col = "period_return_50_Price",
    simulations = 5000,
    samples = 20
  ) {

    sim_data_list <-
      get_control_trade_cumulative_returns(
        generated_preds = generated_preds,
        trade_statement = trade_statement,
        actual_wins_losses = actual_wins_losses,
        trade_direction = trade_direction,
        return_col = return_col
      )

    safely_sample <-
      safely(generate_random_sampling_returns, otherwise = NULL)

    sim_data_asset_all <-
      sim_data_list[[2]] %>%
      split(.$Asset, drop = FALSE) %>%
      map(
        ~ .x %>%
          safely_sample(
            simulations = simulations,
            samples = samples,
            return_col = return_col
          ) %>%
          pluck('result')
      )

    complete_summaries <-
      sim_data_list[[2]] %>%
      mutate(
        win = ifelse(!!as.name(return_col) > 0, 1, 0)
      ) %>%
      group_by(Asset) %>%
      summarise(
        Total_returns = sum(!!as.name(return_col), na.rm = T),
        Total_wins = sum(win, na.rm = T),
        Total_Trades = n(),
        Total_Perc = Total_wins/Total_Trades
      )

    sim_data_asset_all_dfr <-
      sim_data_asset_all %>%
      keep(~ !is.null(.x)) %>%
      map_dfr(bind_rows) %>%
      left_join(complete_summaries)

    return(sim_data_asset_all_dfr)

  }

trade_statement <-
  "
  (

  (AR_GLM_Pred_period_return_50_Price >= 0.55 & state_space_GLM_Pred_period_return_50_Price >= 0.55 &  Asset == 'EUR_USD') |
  (AR_GLM_Pred_period_return_50_Price >= 0.675 & state_space_GLM_Pred_period_return_50_Price >= 0.675 &  Asset == 'AU200_AUD')|
  ( (state_space_GLM_Pred_period_return_50_Price >= 0.95|AR_GLM_Pred_period_return_50_Price >= 0.6) &  Asset == 'AUD_USD') |
  (state_space_GLM_Pred_period_return_50_Price >= 0.7 &  Asset == 'EU50_EUR')|
  (AR_GLM_Pred_period_return_50_Price >= 0.6 & state_space_GLM_Pred_period_return_50_Price >= 0.6 &  Asset == 'EUR_AUD')|
  (AR_GLM_Pred_period_return_50_Price >= 0.6 & state_space_GLM_Pred_period_return_50_Price >= 0.6 &  Asset == 'USD_CAD')|
  (AR_GLM_Pred_period_return_50_Price >= 0.6 & state_space_GLM_Pred_period_return_50_Price >= 0.6 &  Asset == 'USD_JPY')|
  (AR_GLM_Pred_period_return_50_Price >= 0.6 & state_space_GLM_Pred_period_return_50_Price >= 0.6 &  Asset == 'GBP_AUD')|
  (AR_GLM_Pred_period_return_50_Price >= 0.625 & state_space_GLM_Pred_period_return_50_Price >= 0.625 &  Asset == 'GBP_USD')|
  (AR_GLM_Pred_period_return_50_Price >= 0.6 & state_space_GLM_Pred_period_return_50_Price >= 0.6 &  Asset == 'HK33_HKD')|
  (AR_GLM_Pred_period_return_50_Price >= 0.675 & state_space_GLM_Pred_period_return_50_Price >= 0.675 &  Asset == 'NZD_USD')|
  (AR_GLM_Pred_period_return_50_Price >= 0.675 & state_space_GLM_Pred_period_return_50_Price >= 0.675 &  Asset == 'SG30_SGD')|
  (AR_GLM_Pred_period_return_50_Price >= 0.60 & state_space_GLM_Pred_period_return_50_Price >= 0.60 &  Asset == 'SPX500_USD')|
  (AR_GLM_Pred_period_return_50_Price >= 0.675 & state_space_GLM_Pred_period_return_50_Price >= 0.675 &  Asset == 'UK10YB_GBP')|
  (AR_GLM_Pred_period_return_50_Price >= 0.6 & state_space_GLM_Pred_period_return_50_Price >= 0.6 &  Asset == 'US2000_USD')|
  (AR_GLM_Pred_period_return_50_Price >= 0.6 & state_space_GLM_Pred_period_return_50_Price >= 0.6 &  Asset == 'USB10Y_USD')|
  (AR_GLM_Pred_period_return_50_Price >= 0.6 & state_space_GLM_Pred_period_return_50_Price >= 0.6 &  Asset == 'USD_CAD')|
  (AR_GLM_Pred_period_return_50_Price >= 0.6 & state_space_GLM_Pred_period_return_50_Price >= 0.6 &  Asset == 'USD_JPY')|
  (AR_GLM_Pred_period_return_50_Price >= 0.65 & state_space_GLM_Pred_period_return_50_Price >= 0.6 &  Asset == 'USD_SEK')|
  (AR_GLM_Pred_period_return_50_Price >= 0.7 & state_space_GLM_Pred_period_return_50_Price >= 0.6 &  Asset == 'USD_SGD')|
  (AR_GLM_Pred_period_return_50_Price >= 0.6 & state_space_GLM_Pred_period_return_50_Price >= 0.6 &  Asset == 'WTICO_USD')|
  (AR_GLM_Pred_period_return_50_Price >= 0.675 & state_space_GLM_Pred_period_return_50_Price >= 0.675 &  Asset == 'XCU_USD')|
  (AR_GLM_Pred_period_return_50_Price >= 0.6 & state_space_GLM_Pred_period_return_50_Price >= 0.6 &  Asset == 'FR40_EUR')|
  (state_space_GLM_Pred_period_return_50_Price >= 0.9 &  Asset == 'BTC_USD')|
  (AR_GLM_Pred_period_return_50_Price >= 0.65 &  Asset == 'GBP_CAD')|
  (AR_GLM_Pred_period_return_50_Price >= 0.575 & state_space_GLM_Pred_period_return_50_Price >= 0.575 &  Asset == 'EUR_JPY')|
  ( (state_space_GLM_Pred_period_return_50_Price >= 0.875|AR_GLM_Pred_period_return_50_Price >= 0.6) & Asset == 'GBP_JPY')|
  ( (state_space_GLM_Pred_period_return_50_Price >= 0.525 & AR_GLM_Pred_period_return_50_Price >= 0.525) & Asset == 'XAG_AUD')|
  (AR_GLM_Pred_period_return_50_Price >= 0.65 &  Asset == 'XAG_USD')|
  (state_space_GLM_Pred_period_return_50_Price >= 0.575 & AR_GLM_Pred_period_return_50_Price >0.575 & Asset == 'XAU_USD')

  )|
  (

  (AR_LM_Pred_period_return_50_Price >= 1 & state_space_LM_Pred_period_return_50_Price >= 1 &  Asset == 'EUR_USD') |
  (AR_LM_Pred_period_return_50_Price >= 2 & state_space_LM_Pred_period_return_50_Price >= 2 &  Asset == 'GBP_USD')|
  (AR_LM_Pred_period_return_50_Price >= 2 & state_space_LM_Pred_period_return_50_Price >= 2 &  Asset == 'SG30_SGD')|
  (AR_LM_Pred_period_return_50_Price >= 2 & state_space_LM_Pred_period_return_50_Price >= 2 &  Asset == 'SPX500_USD')|
  (AR_LM_Pred_period_return_50_Price >= 2 & state_space_LM_Pred_period_return_50_Price >= 2 &  Asset == 'US2000_USD')|
  (AR_LM_Pred_period_return_50_Price >= 2 & state_space_LM_Pred_period_return_50_Price >= 2 &  Asset == 'USD_CAD')|
  (AR_LM_Pred_period_return_50_Price >= 2 & state_space_LM_Pred_period_return_50_Price >= 2 &  Asset == 'USD_JPY')|
  (AR_LM_Pred_period_return_50_Price >= 2 & state_space_LM_Pred_period_return_50_Price >= 2 &  Asset == 'XCU_USD')|
  (AR_LM_Pred_period_return_50_Price >= 2 & state_space_LM_Pred_period_return_50_Price >= 2 &  Asset == 'FR40_EUR')|
  (AR_LM_Pred_period_return_50_Price >= 2 &  Asset == 'GBP_CAD')|
  (AR_LM_Pred_period_return_50_Price >= 1 & state_space_LM_Pred_period_return_50_Price >= 1 &  Asset == 'EUR_JPY')|
  ( (state_space_LM_Pred_period_return_50_Price >= 5|AR_LM_Pred_period_return_50_Price >= 2) & Asset == 'GBP_JPY')|
  ( (state_space_LM_Pred_period_return_50_Price >= 2 & AR_LM_Pred_period_return_50_Price >= 2) & Asset == 'XAG_AUD')|
  (AR_LM_Pred_period_return_50_Price >= 2 &  Asset == 'XAG_USD')|
  (state_space_LM_Pred_period_return_50_Price >= 8.5 &  Asset == 'USD_SEK')|
  ( (AR_LM_Pred_period_return_50_Price >= 5 | state_space_LM_Pred_period_return_50_Price >= 6.5) & Asset == 'USD_CAD')|
  (state_space_LM_Pred_period_return_50_Price >= 5 & Asset == 'BTC_USD')|

  (((state_space_LM_Pred_period_return_50_Price >=
      state_space_LM_Pred_period_return_50_Price_mean +
      3*state_space_LM_Pred_period_return_50_Price_sd)|
    (AR_LM_Pred_period_return_50_Price >= 3)) & Asset == 'AUD_USD')|
  (AR_LM_Pred_period_return_50_Price >= 2.75 & state_space_LM_Pred_period_return_50_Price >= 2.75 &  Asset == 'USD_SGD')|

  (((state_space_LM_Pred_period_return_50_Price >= 4)|
  (AR_LM_Pred_period_return_50_Price >= 2.75 &
  state_space_LM_Pred_period_return_50_Price >= 2.75)) &
  Asset == 'NZD_USD')|

  (
  ((AR_LM_Pred_period_return_50_Price >= 0 & AR_LM_Pred_period_return_50_Price <= 0.1)|
    (state_space_LM_Pred_period_return_50_Price >= 6 & Asset == 'WTICO_USD')) &
   Asset == 'WTICO_USD'
  )|

  (state_space_LM_Pred_period_return_50_Price >= 7.25 &  Asset == 'EUR_AUD')|
  (state_space_LM_Pred_period_return_50_Price >= 8 & Asset == 'GBP_AUD')|
  (state_space_LM_Pred_period_return_50_Price >= 5.9 &  Asset == 'AU200_AUD')|
  (state_space_LM_Pred_period_return_50_Price >= 5.5 &  Asset == 'USD_JPY')|
  (state_space_LM_Pred_period_return_50_Price > 0 & AR_LM_Pred_period_return_50_Price > 0 & Asset == 'EU50_EUR')|
  (AR_LM_Pred_period_return_50_Price >= 3.25 & state_space_LM_Pred_period_return_50_Price >= 3.25 & Asset == 'UK10YB_GBP')|
  (AR_LM_Pred_period_return_50_Price >= 7 &  Asset == 'HK33_HKD')|
  (state_space_LM_Pred_period_return_50_Price >= 16.5 &  Asset == 'HK33_HKD')|
  (state_space_LM_Pred_period_return_50_Price >= 6.65 & Asset == 'XAU_USD')|
  (state_space_LM_Pred_period_return_50_Price >= 1.25 &
  AR_LM_Pred_period_return_50_Price >= 1.25 &
  Asset == 'XAU_USD')

  )
"
cumulative_returns_sim_data <-
  get_total_portfolio_summary(
    generated_preds = generated_preds,
    trade_statement = trade_statement,
    actual_wins_losses =actual_wins_losses,
    trade_direction = "Long",
    return_col = "period_return_50_Price"
  )

cumulative_returns_sim_data %>%
  ggplot(aes(x = Date, y = Cumulative_Return)) +
  geom_line() +
  facet_wrap(.~trade_col, scales = "free") +
  scale_y_continuous(n.breaks = 20) +
  theme_minimal()

asset_summaries <-
  get_asset_random_sim_returns(
    generated_preds = generated_preds,
    trade_statement = trade_statement,
    actual_wins_losses = actual_wins_losses,
    trade_direction = "Long",
    return_col = "period_return_50_Price",
    simulations = 5000,
    samples = 50
  )

