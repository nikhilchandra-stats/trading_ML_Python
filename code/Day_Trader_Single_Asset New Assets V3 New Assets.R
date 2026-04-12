helpeR::load_custom_functions()

all_aud_symbols <- get_oanda_symbols() %>%
  keep(~ str_detect(.x, "AUD")|str_detect(.x, "USD_SEK|USD_NOK|USD_HUF|USD_ZAR|USD_CNY|USD_MXN|USD_CZK"))
asset_infor <- get_instrument_info()
aud_assets <- read_all_asset_data_intra_day(
  asset_list_oanda = all_aud_symbols,
  save_path_oanda_assets = "C:/Users/nikhi/Documents/Asset Data/oanda_data/",
  read_csv_or_API = "API",
  time_frame = "D",
  bid_or_ask = "bid",
  how_far_back = 11,
  start_date = (today() - days(2)) %>% as.character()
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
    "UK100_GBP",
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
    "USB02Y_USD", "USB05Y_USD", "USB30Y_USD", "USB10Y_USD", "UK100_GBP",
    "EUR_CHF", #1 EUR_CHF
    "EUR_SEK" , #2 EUR_SEK
    "GBP_CHF", #3 GBP_CHF
    "GBP_JPY", #4 GBP_JPY
    "USD_CZK", #5 USD_CZK
    "USD_NOK" , #6 USD_NOK
    "XAG_CAD", #7 XAG_CAD
    "XAG_CHF", #8 XAG_CHF
    "XAG_JPY" , #9 XAG_JPY
    "GBP_NZD" , #10 GBP_NZD
    "NZD_CHF" , #11 NZD_CHF
    "USD_MXN" , #12 USD_MXN
    "XPD_USD" , #13 XPD_USD
    "XPT_USD" , #14 XPT_USD
    "NATGAS_USD" , #15 NATGAS_USD
    "SG30_SGD" , #16 SG30_SGD
    "SOYBN_USD" , #17 SOYBN_USD
    "WHEAT_USD" , #18 WHEAT_USD
    "SUGAR_USD" , #19 SUGAR_USD
    "DE30_EUR" , #20 DE30_EUR
    "UK10YB_GBP" , #21 UK10YB_GBP
    "JP225_USD" , #22 JP225_USD
    "CH20_CHF" , #23 CH20_CHF
    "NL25_EUR" , #24 NL25_EUR
    "XAG_SGD" , #25 XAG_SGD,
    "BCH_USD" , #26 BCH_USD
    "LTC_USD" #27 LTC_USD
  ) %>%
  unique()

asset_infor <- get_instrument_info()
#---------------------Data
load_custom_functions()
db_location = "C:/Users/nikhi/Documents/Asset Data/Oanda_Asset_Data_Most_Assets_2025-09-13.db"
start_date = "2019-06-01"
end_date = today() %>% as.character()

stop_factor_var = 10
profit_factor_var = 50
risk_dollar_value_var = 15

#I Am just testing a new parameter against the original Model
# stop_factor_var = 10
# profit_factor_var = 5
# risk_dollar_value_var = 10

Indices_Metals_Bonds <- list()

Indices_Metals_Bonds[[1]] <-
  get_db_data_quickly_algo(
    db_location = db_location,
    start_date = start_date,
    end_date = today() %>% as.character(),
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
    end_date = today() %>% as.character(),
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
    stop_factor = stop_factor_var,
    profit_factor = profit_factor_var,
    risk_dollar_value = risk_dollar_value_var,
    trade_direction = "Long",
    currency_conversion = currency_conversion,
    asset_infor = asset_infor,
    periods_ahead = 24
  )


pred_generation_db_path <-
  "C:/Users/nikhi/Documents/trade_data/tech_preds.db"
pred_gen_db_con <- connect_db(pred_generation_db_path)


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

pred_generation_db_path <-
  "C:/Users/nikhi/Documents/trade_data/tech_preds_2.db"
pred_gen_db_con <- connect_db(pred_generation_db_path)

raw_base_preds <-
  list()

training_end_date <- "2025-05-01"
rolling_mean_pred_period = 500
bin_threshold = 5

for (i in 1:length(indicator_mapping$Asset) ) {

  asset_loop <- indicator_mapping$Asset[i]
  copula_assets <- indicator_mapping$couplua_assets[[i]]

  pred_generated <-
    Single_Asset_V3_Gen_Model(
      Indices_Metals_Bonds,
      actual_wins_losses,
      asset_of_interest = asset_loop,
      actuals_periods_needed = c("period_return_24_Price", "period_return_35_Price", "period_return_46_Price"),
      training_end_date = training_end_date,
      bin_threshold = bin_threshold,
      rolling_mean_pred_period = rolling_mean_pred_period,
      correlation_rolling_periods = c(100,200, 300),
      copula_assets = copula_assets,
      base_path = "C:/Users/nikhi/Documents/trade_data/single_asset_models_v2/"
    )

  all_preds <-
    pred_generated[[1]] %>%
    bind_rows(pred_generated[[2]]) %>%
    mutate(
      training_end_date = training_end_date,
      rolling_mean_pred_period = rolling_mean_pred_period,
      bin_threshold = bin_threshold
    )

  if(i == 1){
    write_table_sql_lite(.data = all_preds,
                         table_name = "tech_preds",
                         conn = pred_gen_db_con,
                         overwrite_true = TRUE)
  } else {
    append_table_sql_lite(.data = all_preds,
                          table_name = "tech_preds",
                          conn = pred_gen_db_con)
  }

  rm(all_preds, pred_generated)

  gc()

}

# all_preds <-
#   Single_Asset_V3_get_all_preds(
#     Indices_Metals_Bonds = Indices_Metals_Bonds,
#     actuals_periods_needed = c("period_return_35_Price", "period_return_46_Price"),
#     correlation_rolling_periods = c(100,200, 300),
#     training_end_date = "2025-05-01",
#     rolling_mean_pred_period = 500,
#     bin_threshold = 5,
#     start_index = 1,
#     end_index = 38,
#     base_path = "C:/Users/nikhi/Documents/trade_data/single_asset_models_v2/"
#   )
#
# all_preds_dfr <-
#   all_preds %>%
#   mutate(
#     training_end_date = "2025-05-01",
#     rolling_mean_pred_period = 500,
#     bin_threshold = 5
#   )
#
#
# write_table_sql_lite(.data = all_preds_dfr,
#                      table_name = "tech_preds",
#                      conn = pred_gen_db_con,
#                      overwrite_true = TRUE)



pred_generation_db_path <-
  "C:/Users/nikhi/Documents/trade_data/tech_preds_2.db"
pred_gen_db_con <- connect_db(pred_generation_db_path)

generated_preds_from_db <-
  DBI::dbGetQuery(conn = pred_gen_db_con,
                  statement = "SELECT * FROM tech_preds") %>%
  mutate(
    Date = as_datetime(Date)
  )

trade_statement <-
  "
    # (averaged_35_GLM_pred >= 0.55 & averaged_35_GLM_pred < 0.65)
    state_space_LM_Pred_period_return_35_Price > 1 & AR_LM_Pred_period_return_35_Price > 0
"

test_performance <-
  generated_preds_from_db %>%
  filter(!(Asset %in% c("XPD_USD", "XPT_USD"))) %>%
  mutate(
    momentum_GLM_35_46 =
      (state_space_GLM_Pred_period_return_35_Price +
         AR_GLM_Pred_period_return_35_Price +
         state_space_GLM_Pred_period_return_46_Price +
         AR_GLM_Pred_period_return_46_Price )/4,

    momentum_GLM_35 =
      (state_space_GLM_Pred_period_return_35_Price +
         AR_GLM_Pred_period_return_35_Price )/2,

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

  ) %>%
  filter(Date >= as_datetime(training_end_date)) %>%
  left_join(
    actual_wins_losses %>%
      dplyr::select(Date, Asset,
                    period_return_8_Price, period_return_12_Price, period_return_16_Price,
                    period_return_24_Price, period_return_35_Price, period_return_46_Price)
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

control_data <-
  test_performance %>%
  group_by(Date) %>%
  summarise(
    period_return_35_Price = sum(period_return_35_Price, na.rm = T)
  ) %>%
  ungroup() %>%
  arrange(Date) %>%
  mutate(
    cumulative_return = cumsum(period_return_35_Price)
  ) %>%
  mutate(
    trade_col = "Control"
  )

trade_data <-
  test_performance %>%
  filter(trade_col == "Long") %>%
  group_by(Date) %>%
  summarise(
    # period_return_35_Price = sum(period_return_35_Price, na.rm = T)
    period_return_35_Price = sum(period_return_35_Price, na.rm = T)
  ) %>%
  ungroup() %>%
  arrange(Date) %>%
  mutate(
    # cumulative_return = cumsum(period_return_35_Price)
    cumulative_return = cumsum(period_return_35_Price)
  ) %>%
  mutate(
    trade_col = "Long"
  )


plot_dat_Copula <-
  control_data %>%
  bind_rows(trade_data)


plot_dat_Copula %>%
  ggplot(aes(x = Date, y = cumulative_return, color = trade_col)) +
  geom_line() +
  theme_minimal() +
  facet_wrap(.~ trade_col, scales = "free") +
  theme(legend.position = "bottom")


control_data_asset <-
  test_performance %>%
  group_by(Date, Asset) %>%
  summarise(
    period_return_35_Price = sum(period_return_35_Price, na.rm = T)
  ) %>%
  ungroup() %>%
  group_by(Asset) %>%
  arrange(Date, .by_group = TRUE) %>%
  mutate(
    cumulative_return = cumsum(period_return_35_Price),
    wins = ifelse(period_return_35_Price > 0 , 1, 0),
    avg_win = ifelse(period_return_35_Price > 0, period_return_35_Price, NA),
    losing_return =
      ifelse(period_return_35_Price < 0, period_return_35_Price, NA)
  ) %>%
  mutate(
    trade_col = "Control"
  ) %>%
  group_by(Asset) %>%
  summarise(
    control_total_trades = n_distinct(Date),
    control_wins = sum(wins, na.rm = T),
    control_returns_total = sum(period_return_35_Price, na.rm = T),
    control_returns_25 = quantile(cumulative_return, 0.25),
    avg_win = mean(avg_win, na.rm = T),
    avg_loss = mean(losing_return, na.rm = T)
  ) %>%
  mutate(
    control_Perc = control_wins/control_total_trades
  ) %>%
  mutate(
    winning_greater_than_0_control = ifelse(control_returns_total > 0, 1, 0)
  ) %>%
  summarise(
    control_returns = sum(control_total_trades, na.rm = T),
    control_returns_25 = sum(control_returns_25, na.rm = T),
    control_total_trades = sum(control_total_trades, na.rm = T),
    control_perc = sum(control_wins, na.rm = T)/sum(control_total_trades, na.rm = T),
    control_avg_win = mean(avg_win, na.rm = T),
    control_avg_loss = mean(avg_loss, na.rm = T),
    winning_greater_than_0_control = sum(winning_greater_than_0_control, na.rm = T)/n_distinct(Asset)
  ) %>%
  mutate(
    binomial_expected_control = control_avg_win*control_perc + (control_avg_loss*(1 - control_perc))
  )

trade_data <-
  test_performance %>%
  filter(trade_col == "Long") %>%
  group_by(Date, Asset) %>%
  summarise(
    period_return_35_Price = sum(period_return_35_Price, na.rm = T)
  ) %>%
  ungroup() %>%
  group_by(Asset) %>%
  arrange(Date, .by_group = TRUE) %>%
  mutate(
    cumulative_return = cumsum(period_return_35_Price),
    wins = ifelse(period_return_35_Price > 0 , 1, 0),
    winning_return =
      ifelse(period_return_35_Price > 0, period_return_35_Price, NA),
    losing_return =
      ifelse(period_return_35_Price < 0, period_return_35_Price, NA)
  ) %>%
  mutate(
    trade_col = "Long"
  ) %>%
  group_by(Asset, trade_col) %>%
  summarise(
    total_trades = n_distinct(Date),
    wins = sum(wins, na.rm = T),
    returns_total = sum(period_return_35_Price, na.rm = T),
    returns_25 = quantile(cumulative_return, 0.25),
    returns_10 = quantile(cumulative_return, 0.1),
    avg_win = mean(winning_return, na.rm = T),
    avg_loss = mean(losing_return, na.rm = T)
  ) %>%
  mutate(
    Perc = wins/total_trades
  ) %>%
  ungroup() %>%
  mutate(
    Perc = wins/total_trades
  ) %>%
  mutate(
    winning_greater_than_0 = ifelse(returns_total > 0, 1, 0)
  ) %>%
  summarise(
    returns = sum(returns_total, na.rm = T),
    returns_25 = sum(returns_25, na.rm = T),
    total_trades = sum(total_trades, na.rm = T),
    perc = sum(wins, na.rm = T)/sum(total_trades, na.rm = T),
    avg_win = mean(avg_win, na.rm = T),
    avg_loss = mean(avg_loss, na.rm = T),
    winning_greater_than_0 = sum(winning_greater_than_0, na.rm = T)/n_distinct(Asset)
  ) %>%
  mutate(
    binomial_expected = avg_win*perc + (avg_loss*(1 - perc))
  )

final_results <-
  trade_data %>%
  bind_cols(control_data_asset) %>%
  mutate(ratio_adj = total_trades/control_total_trades) %>%
  mutate(control_returns = control_returns*ratio_adj) %>%
  mutate(
    binomial_diff = binomial_expected - binomial_expected_control
  )

plot_dat_Copula %>%
  filter(trade_col == "Long") %>%
  mutate(
    Movement_300 = cumulative_return - lag(cumulative_return, 100)
  ) %>%
  summarise(
    returns_99 = quantile(Movement_300, 0.99, na.rm = T),
    returns_90 = quantile(Movement_300, 0.90, na.rm = T),
    returns_75 = quantile(Movement_300, 0.75, na.rm = T),
    returns_50 = quantile(Movement_300, 0.5, na.rm = T),
    returns_25 = quantile(Movement_300, 0.25, na.rm = T),
    returns_10 = quantile(Movement_300, 0.1, na.rm = T),
    returns_01 = quantile(Movement_300, 0.01, na.rm = T)
  )



asset_returns_control <-
  test_performance %>%
  group_by(Date, Asset) %>%
  summarise(
    period_return_35_Price = sum(period_return_35_Price, na.rm = T)
  ) %>%
  ungroup() %>%
  group_by(Asset) %>%
  arrange(Date, .by_group = TRUE) %>%
  mutate(
    cumulative_return = cumsum(period_return_35_Price),
    wins = ifelse(period_return_35_Price > 0 , 1, 0),
    winning_return =
      ifelse(period_return_35_Price > 0, period_return_35_Price, NA),
    losing_return =
      ifelse(period_return_35_Price < 0, period_return_35_Price, NA)
  ) %>%
  mutate(
    trade_col = "Control"
  ) %>%
  group_by(Asset, trade_col) %>%
  summarise(
    total_trades = n_distinct(Date),
    wins = sum(wins, na.rm = T),
    returns_total = sum(period_return_35_Price, na.rm = T),
    returns_25 = quantile(cumulative_return, 0.25),
    returns_10 = quantile(cumulative_return, 0.1),
    avg_win = mean(winning_return, na.rm = T),
    avg_loss = mean(losing_return, na.rm = T)
  ) %>%
  mutate(
    Perc = wins/total_trades
  ) %>%
  ungroup() %>%
  mutate(
    Perc = wins/total_trades
  ) %>%
  mutate(
    winning_greater_than_0 = ifelse(returns_total > 0, 1, 0),
    binomial_expected = avg_win*Perc + (avg_loss*(1 - Perc))
  )

asset_returns <-
  test_performance %>%
  filter(trade_col == "Long") %>%
  group_by(Date, Asset) %>%
  summarise(
    period_return_35_Price = sum(period_return_35_Price, na.rm = T)
  ) %>%
  ungroup() %>%
  group_by(Asset) %>%
  arrange(Date, .by_group = TRUE) %>%
  mutate(
    cumulative_return = cumsum(period_return_35_Price),
    wins = ifelse(period_return_35_Price > 0 , 1, 0),
    winning_return =
      ifelse(period_return_35_Price > 0, period_return_35_Price, NA),
    losing_return =
      ifelse(period_return_35_Price < 0, period_return_35_Price, NA)
  ) %>%
  mutate(
    trade_col = "Long"
  ) %>%
  group_by(Asset, trade_col) %>%
  summarise(
    total_trades = n_distinct(Date),
    wins = sum(wins, na.rm = T),
    returns_total = sum(period_return_35_Price, na.rm = T),
    returns_25 = quantile(cumulative_return, 0.25),
    returns_10 = quantile(cumulative_return, 0.1),
    avg_win = mean(winning_return, na.rm = T),
    avg_loss = mean(losing_return, na.rm = T)
  ) %>%
  mutate(
    Perc = wins/total_trades
  ) %>%
  ungroup() %>%
  mutate(
    Perc = wins/total_trades
  ) %>%
  mutate(
    winning_greater_than_0 = ifelse(returns_total > 0, 1, 0),
    binomial_expected = avg_win*Perc + (avg_loss*(1 - Perc))
  ) %>%
  left_join(asset_returns_control %>%
              dplyr::select(Asset,  binomial_expected_control = binomial_expected))

#-----------------------------------------------------
post_LM_test_date <- "2026-01-26"
post_LM_data <-
  generated_preds_from_db %>%
  filter(Date >= as_datetime(training_end_date)) %>%
  filter(!(Asset %in% c("XPD_USD", "XPT_USD"))) %>%
  mutate(
    momentum_GLM_35_46 =
      (state_space_GLM_Pred_period_return_35_Price +
         AR_GLM_Pred_period_return_35_Price +
         state_space_GLM_Pred_period_return_46_Price +
         AR_GLM_Pred_period_return_46_Price )/4,

    momentum_GLM_35 =
      (state_space_GLM_Pred_period_return_35_Price +
         AR_GLM_Pred_period_return_35_Price )/2,

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

  ) %>%
  # filter(Date >= as_datetime(training_end_date)) %>%
  left_join(
    actual_wins_losses %>%
      dplyr::select(Date, Asset,
                    period_return_8_Price, period_return_12_Price, period_return_16_Price,
                    period_return_24_Price, period_return_35_Price, period_return_46_Price)
  )

post_LM_data_train <-
  post_LM_data %>%
  filter(Date >= as_datetime(training_end_date)) %>%
  filter(Date <= post_LM_test_date)

post_LM_form <-
  create_lm_formula(dependant = "period_return_35_Price",
                    independant = c("AR_LM_Pred_period_return_24_Price", "AR_GLM_Pred_period_return_24_Price",
                                    "AR_LM_Pred_period_return_35_Price", "AR_GLM_Pred_period_return_35_Price",
                                    "AR_LM_Pred_period_return_46_Price", "AR_GLM_Pred_period_return_46_Price",
                                    "state_space_LM_Pred_period_return_24_Price", "state_space_GLM_Pred_period_return_24_Price",
                                    "state_space_LM_Pred_period_return_35_Price", "state_space_GLM_Pred_period_return_35_Price",
                                    "state_space_LM_Pred_period_return_46_Price", "state_space_GLM_Pred_period_return_46_Price",
                                    "Copula_LM_Pred_period_return_35_Price", "Copula_GLM_Pred_period_return_35_Price",
                                    "Copula_LM_Pred_period_return_24_Price", "Copula_GLM_Pred_period_return_24_Price",
                                    "Asset")  )

post_LM <-
  lm(formula = post_LM_form
       , data = post_LM_data_train)

summary(post_LM)

predicted_values <- predict(post_LM, post_LM_data )

trade_statement <-
  "
    # (averaged_35_GLM_pred >= 0.55 & averaged_35_GLM_pred < 0.65)
    post_LM > 3
"

test_performance <-
  generated_preds_from_db %>%
  filter(Date >= as_datetime(training_end_date)) %>%
  filter(!(Asset %in% c("XPD_USD", "XPT_USD"))) %>%
  mutate(
    momentum_GLM_35_46 =
      (state_space_GLM_Pred_period_return_35_Price +
         AR_GLM_Pred_period_return_35_Price +
         state_space_GLM_Pred_period_return_46_Price +
         AR_GLM_Pred_period_return_46_Price )/4,

    momentum_GLM_35 =
      (state_space_GLM_Pred_period_return_35_Price +
         AR_GLM_Pred_period_return_35_Price )/2,

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
         Copula_LM_Pred_period_return_46_Price)/6,

    post_LM = predicted_values

  ) %>%
  filter(Date >= as_datetime(training_end_date)) %>%
  filter(Date > as_datetime(post_LM_test_date)) %>%
  left_join(
    actual_wins_losses %>%
      dplyr::select(Date, Asset,
                    period_return_8_Price, period_return_12_Price, period_return_16_Price,
                    period_return_24_Price, period_return_35_Price, period_return_46_Price)
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

control_data <-
  test_performance %>%
  filter(Date > as_datetime(post_LM_test_date)) %>%
  group_by(Date) %>%
  summarise(
    period_return_35_Price = sum(period_return_35_Price, na.rm = T)
  ) %>%
  ungroup() %>%
  arrange(Date) %>%
  mutate(
    cumulative_return = cumsum(period_return_35_Price)
  ) %>%
  mutate(
    trade_col = "Control"
  )

trade_data <-
  test_performance %>%
  filter(Date > as_datetime(post_LM_test_date)) %>%
  filter(trade_col == "Long") %>%
  group_by(Date) %>%
  summarise(
    # period_return_35_Price = sum(period_return_35_Price, na.rm = T)
    period_return_35_Price = sum(period_return_35_Price, na.rm = T)
  ) %>%
  ungroup() %>%
  arrange(Date) %>%
  mutate(
    # cumulative_return = cumsum(period_return_35_Price)
    cumulative_return = cumsum(period_return_35_Price)
  ) %>%
  mutate(
    trade_col = "Long"
  )


plot_dat_Copula <-
  control_data %>%
  bind_rows(trade_data)


plot_dat_Copula %>%
  ggplot(aes(x = Date, y = cumulative_return, color = trade_col)) +
  geom_line() +
  theme_minimal() +
  facet_wrap(.~ trade_col, scales = "free") +
  theme(legend.position = "bottom")


control_data_asset <-
  test_performance %>%
  filter(Date > as_datetime(post_LM_test_date)) %>%
  group_by(Date, Asset) %>%
  summarise(
    period_return_35_Price = sum(period_return_35_Price, na.rm = T)
  ) %>%
  ungroup() %>%
  group_by(Asset) %>%
  arrange(Date, .by_group = TRUE) %>%
  mutate(
    cumulative_return = cumsum(period_return_35_Price),
    wins = ifelse(period_return_35_Price > 0 , 1, 0),
    avg_win = ifelse(period_return_35_Price > 0, period_return_35_Price, NA),
    losing_return =
      ifelse(period_return_35_Price < 0, period_return_35_Price, NA)
  ) %>%
  mutate(
    trade_col = "Control"
  ) %>%
  group_by(Asset) %>%
  summarise(
    control_total_trades = n_distinct(Date),
    control_wins = sum(wins, na.rm = T),
    control_returns_total = sum(period_return_35_Price, na.rm = T),
    control_returns_25 = quantile(cumulative_return, 0.25),
    avg_win = mean(avg_win, na.rm = T),
    avg_loss = mean(losing_return, na.rm = T)
  ) %>%
  mutate(
    control_Perc = control_wins/control_total_trades
  ) %>%
  mutate(
    winning_greater_than_0_control = ifelse(control_returns_total > 0, 1, 0)
  ) %>%
  summarise(
    control_returns = sum(control_total_trades, na.rm = T),
    control_returns_25 = sum(control_returns_25, na.rm = T),
    control_total_trades = sum(control_total_trades, na.rm = T),
    control_perc = sum(control_wins, na.rm = T)/sum(control_total_trades, na.rm = T),
    control_avg_win = mean(avg_win, na.rm = T),
    control_avg_loss = mean(avg_loss, na.rm = T),
    winning_greater_than_0_control = sum(winning_greater_than_0_control, na.rm = T)/n_distinct(Asset)
  ) %>%
  mutate(
    binomial_expected_control = control_avg_win*control_perc + (control_avg_loss*(1 - control_perc))
  )

trade_data <-
  test_performance %>%
  filter(Date > as_datetime(post_LM_test_date)) %>%
  filter(trade_col == "Long") %>%
  group_by(Date, Asset) %>%
  summarise(
    period_return_35_Price = sum(period_return_35_Price, na.rm = T)
  ) %>%
  ungroup() %>%
  group_by(Asset) %>%
  arrange(Date, .by_group = TRUE) %>%
  mutate(
    cumulative_return = cumsum(period_return_35_Price),
    wins = ifelse(period_return_35_Price > 0 , 1, 0),
    winning_return =
      ifelse(period_return_35_Price > 0, period_return_35_Price, NA),
    losing_return =
      ifelse(period_return_35_Price < 0, period_return_35_Price, NA)
  ) %>%
  mutate(
    trade_col = "Long"
  ) %>%
  group_by(Asset, trade_col) %>%
  summarise(
    total_trades = n_distinct(Date),
    wins = sum(wins, na.rm = T),
    returns_total = sum(period_return_35_Price, na.rm = T),
    returns_25 = quantile(cumulative_return, 0.25),
    returns_10 = quantile(cumulative_return, 0.1),
    avg_win = mean(winning_return, na.rm = T),
    avg_loss = mean(losing_return, na.rm = T)
  ) %>%
  mutate(
    Perc = wins/total_trades
  ) %>%
  ungroup() %>%
  mutate(
    Perc = wins/total_trades
  ) %>%
  mutate(
    winning_greater_than_0 = ifelse(returns_total > 0, 1, 0)
  ) %>%
  summarise(
    returns = sum(returns_total, na.rm = T),
    returns_25 = sum(returns_25, na.rm = T),
    total_trades = sum(total_trades, na.rm = T),
    perc = sum(wins, na.rm = T)/sum(total_trades, na.rm = T),
    avg_win = mean(avg_win, na.rm = T),
    avg_loss = mean(avg_loss, na.rm = T),
    winning_greater_than_0 = sum(winning_greater_than_0, na.rm = T)/n_distinct(Asset)
  ) %>%
  mutate(
    binomial_expected = avg_win*perc + (avg_loss*(1 - perc))
  )

final_results <-
  trade_data %>%
  bind_cols(control_data_asset) %>%
  mutate(ratio_adj = total_trades/control_total_trades) %>%
  mutate(control_returns = control_returns*ratio_adj) %>%
  mutate(
    binomial_diff = binomial_expected - binomial_expected_control
  )

plot_dat_Copula %>%
  filter(trade_col == "Long") %>%
  mutate(
    Movement_300 = cumulative_return - lag(cumulative_return, 100)
  ) %>%
  summarise(
    returns_99 = quantile(Movement_300, 0.99, na.rm = T),
    returns_90 = quantile(Movement_300, 0.90, na.rm = T),
    returns_75 = quantile(Movement_300, 0.75, na.rm = T),
    returns_50 = quantile(Movement_300, 0.5, na.rm = T),
    returns_25 = quantile(Movement_300, 0.25, na.rm = T),
    returns_10 = quantile(Movement_300, 0.1, na.rm = T),
    returns_01 = quantile(Movement_300, 0.01, na.rm = T)
  )



asset_returns_control <-
  test_performance %>%
  group_by(Date, Asset) %>%
  summarise(
    period_return_35_Price = sum(period_return_35_Price, na.rm = T)
  ) %>%
  ungroup() %>%
  group_by(Asset) %>%
  arrange(Date, .by_group = TRUE) %>%
  mutate(
    cumulative_return = cumsum(period_return_35_Price),
    wins = ifelse(period_return_35_Price > 0 , 1, 0),
    winning_return =
      ifelse(period_return_35_Price > 0, period_return_35_Price, NA),
    losing_return =
      ifelse(period_return_35_Price < 0, period_return_35_Price, NA)
  ) %>%
  mutate(
    trade_col = "Control"
  ) %>%
  group_by(Asset, trade_col) %>%
  summarise(
    total_trades = n_distinct(Date),
    wins = sum(wins, na.rm = T),
    returns_total = sum(period_return_35_Price, na.rm = T),
    returns_25 = quantile(cumulative_return, 0.25),
    returns_10 = quantile(cumulative_return, 0.1),
    avg_win = mean(winning_return, na.rm = T),
    avg_loss = mean(losing_return, na.rm = T)
  ) %>%
  mutate(
    Perc = wins/total_trades
  ) %>%
  ungroup() %>%
  mutate(
    Perc = wins/total_trades
  ) %>%
  mutate(
    winning_greater_than_0 = ifelse(returns_total > 0, 1, 0),
    binomial_expected = avg_win*Perc + (avg_loss*(1 - Perc))
  )

asset_returns <-
  test_performance %>%
  filter(trade_col == "Long") %>%
  group_by(Date, Asset) %>%
  summarise(
    period_return_35_Price = sum(period_return_35_Price, na.rm = T)
  ) %>%
  ungroup() %>%
  group_by(Asset) %>%
  arrange(Date, .by_group = TRUE) %>%
  mutate(
    cumulative_return = cumsum(period_return_35_Price),
    wins = ifelse(period_return_35_Price > 0 , 1, 0),
    winning_return =
      ifelse(period_return_35_Price > 0, period_return_35_Price, NA),
    losing_return =
      ifelse(period_return_35_Price < 0, period_return_35_Price, NA)
  ) %>%
  mutate(
    trade_col = "Long"
  ) %>%
  group_by(Asset, trade_col) %>%
  summarise(
    total_trades = n_distinct(Date),
    wins = sum(wins, na.rm = T),
    returns_total = sum(period_return_35_Price, na.rm = T),
    returns_25 = quantile(cumulative_return, 0.25),
    returns_10 = quantile(cumulative_return, 0.1),
    avg_win = mean(winning_return, na.rm = T),
    avg_loss = mean(losing_return, na.rm = T)
  ) %>%
  mutate(
    Perc = wins/total_trades
  ) %>%
  ungroup() %>%
  mutate(
    Perc = wins/total_trades
  ) %>%
  mutate(
    winning_greater_than_0 = ifelse(returns_total > 0, 1, 0),
    binomial_expected = avg_win*Perc + (avg_loss*(1 - Perc))
  ) %>%
  left_join(asset_returns_control %>%
              dplyr::select(Asset,  binomial_expected_control = binomial_expected))


Asset_Specific_conditions <-
  list()

threshes <- seq(0, 5, 0.25)

for (i in 1:length(threshes)) {

  predicted_values <- predict(post_LM, post_LM_data )

  trade_statement <-
   glue::glue( "
    # (averaged_35_GLM_pred >= 0.55 & averaged_35_GLM_pred < 0.65)
    post_LM > {threshes[i]}
    ")

  test_performance <-
    generated_preds_from_db %>%
    filter(Date >= as_datetime(training_end_date)) %>%
    filter(!(Asset %in% c("XPD_USD", "XPT_USD"))) %>%
    mutate(
      momentum_GLM_35_46 =
        (state_space_GLM_Pred_period_return_35_Price +
           AR_GLM_Pred_period_return_35_Price +
           state_space_GLM_Pred_period_return_46_Price +
           AR_GLM_Pred_period_return_46_Price )/4,

      momentum_GLM_35 =
        (state_space_GLM_Pred_period_return_35_Price +
           AR_GLM_Pred_period_return_35_Price )/2,

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
           Copula_LM_Pred_period_return_46_Price)/6,

      post_LM = predicted_values

    ) %>%
    filter(Date >= as_datetime(training_end_date)) %>%
    filter(Date > as_datetime(post_LM_test_date)) %>%
    left_join(
      actual_wins_losses %>%
        dplyr::select(Date, Asset,
                      period_return_8_Price, period_return_12_Price, period_return_16_Price,
                      period_return_24_Price, period_return_35_Price, period_return_46_Price)
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


  Asset_Specific_conditions[[i]] <-
    test_performance %>%
    filter(trade_col == "Long") %>%
    group_by(Date, Asset) %>%
    summarise(
      period_return_46_Price = sum(period_return_46_Price, na.rm = T)
    ) %>%
    ungroup() %>%
    group_by(Asset) %>%
    arrange(Date, .by_group = TRUE) %>%
    mutate(
      cumulative_return = cumsum(period_return_46_Price),
      wins = ifelse(period_return_46_Price > 0 , 1, 0),
      winning_return =
        ifelse(period_return_46_Price > 0, period_return_46_Price, NA),
      losing_return =
        ifelse(period_return_46_Price < 0, period_return_46_Price, NA)
    ) %>%
    mutate(
      trade_col = "Long"
    ) %>%
    group_by(Asset, trade_col) %>%
    summarise(
      total_trades = n_distinct(Date),
      wins = sum(wins, na.rm = T),
      returns_total = sum(period_return_46_Price, na.rm = T),
      returns_25 = quantile(cumulative_return, 0.25),
      returns_10 = quantile(cumulative_return, 0.1),
      avg_win = mean(winning_return, na.rm = T),
      avg_loss = mean(losing_return, na.rm = T)
    ) %>%
    mutate(
      Perc = wins/total_trades
    ) %>%
    ungroup() %>%
    mutate(
      Perc = wins/total_trades
    ) %>%
    mutate(
      winning_greater_than_0 = ifelse(returns_total > 0, 1, 0),
      binomial_expected = avg_win*Perc + (avg_loss*(1 - Perc))
    ) %>%
    left_join(asset_returns_control %>%
                dplyr::select(Asset,  binomial_expected_control = binomial_expected)) %>%
    mutate(
      Threshold_for_Post_LM = threshes[i]
    )

}

Asset_Specific_conditions_dfr <-
  Asset_Specific_conditions %>%
  map_dfr(bind_rows) %>%
  group_by(Asset) %>%
  slice_max(binomial_expected)

Asset_Specific_conditions_dfr$returns_total %>% sum(na.rm = T)

