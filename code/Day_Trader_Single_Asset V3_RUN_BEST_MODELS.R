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
