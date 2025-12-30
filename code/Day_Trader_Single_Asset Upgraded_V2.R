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
start_date = "2015-01-01"
end_date = today() %>% as.character()

bin_factor = NULL
stop_value_var = 5
profit_value_var = 30
period_var = 24

All_Daily_Data <-
  get_DAILY_ALGO_DATA_API_REQUEST()

Indices_Metals_Bonds <- get_Port_Buy_Data(
  db_location = db_location,
  start_date = start_date,
  end_date = today() %>% as.character(),
  time_frame = "H1"
)

gc()

#-------------Indicator Inputs

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
    raw_macro_data,
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
            "SG30_SGD" #34
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
        "NATGAS_USD") #34
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
      c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD") #34
    )
)

assets_to_analyse <-
  indicator_mapping$Asset

temp_actual_wins_losses <- list()

for (i in 1:length(assets_to_analyse)) {

  temp_actual_wins_losses[[i]] <-
    create_running_profits(
      asset_of_interest = assets_to_analyse[i],
      asset_data = Indices_Metals_Bonds,
      stop_factor = stop_value_var,
      profit_factor = profit_value_var,
      risk_dollar_value = 15,
      trade_direction = "Long",
      currency_conversion = currency_conversion,
      asset_infor = asset_infor
    )

}

actual_wins_losses <-
  temp_actual_wins_losses %>%
  map_dfr(bind_rows) %>%
  dplyr::select(-volume_unadj, -minimumTradeSize_OG, -marginRate,
                -adjusted_conversion, -pipLocation, -minimumTradeSize_OG) %>%
  dplyr::rename(
    High = Bid_High,
    Low =  Bid_Low
  ) %>%
  mutate(
    trade_return_dollar_aud = !!as.name(glue::glue("period_return_{period_var}_Price") ),

    trade_start_prices =
      case_when(
        trade_col == "Long" ~ Ask_Price,
        trade_col == "Short" ~ Bid_Price
      ),
    trade_end_prices =
      case_when(
        trade_col == "Long" ~ Bid_Price,
        trade_col == "Short" ~ Ask_Price
      ),
    stop_factor = stop_value_var,
    profit_factor = profit_value_var,
    periods_ahead = period_var
  )

indicator_mapping_accumulator_long <- list()
indicator_mapping_accumulator_short <- list()

model_data_store_path <-
  "C:/Users/nikhi/Documents/trade_data/Day_Trader_Single_Asset_V2_trade_store_stop_2.db"
model_data_store_db <-
  connect_db(model_data_store_path)

#Original Date for Sim
# date_seq_simulations <-
#   seq(as_date("2022-01-01"), as_date("2024-08-01"), "6 month")
date_seq_simulations <-
  seq(as_date("2023-06-01"), as_date("2024-08-01"), "6 month")
c = 0
redo_db = TRUE

# for (k in 1:length(date_seq_simulations)) {
for (k in 1:1) {

  for (j in 1:length(indicator_mapping$Asset) ) {

    test_end_date = today()
    date_train_end = date_seq_simulations[k]
    date_train_phase_2_end = date_train_end + months(12)
    date_test_start = date_train_phase_2_end + days(3)

    countries_for_int_strength <-
      unlist(indicator_mapping$countries_for_int_strength[j])
    couplua_assets = unlist(indicator_mapping$couplua_assets[j])
    Asset_of_interest = unlist(indicator_mapping$Asset[j])

    single_asset_Logit_indicator_adv_gen_models(
        asset_data = Indices_Metals_Bonds[[1]],
        All_Daily_Data = All_Daily_Data,
        Asset_of_interest = Asset_of_interest,
        actual_wins_losses = actual_wins_losses,

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
        date_train_end = date_train_end,
        date_train_phase_2_end = date_train_phase_2_end,
        date_test_start = date_test_start,

        couplua_assets = couplua_assets,

        stop_value_var = stop_value_var,
        profit_value_var = profit_value_var,
        period_var = period_var,

        bin_var_col = c("period_return_15_Price", "period_return_25_Price", "period_return_35_Price"),
        trade_direction = "Long",
        save_path = "C:/Users/nikhi/Documents/trade_data/Day_Trader_Single_Asset_V2_trade_store_stop_2"
      )

    long_sim <-
      single_asset_Logit_indicator_adv_get_preds(
        asset_data = Indices_Metals_Bonds[[1]],
        All_Daily_Data = All_Daily_Data,
        Asset_of_interest = Asset_of_interest,
        actual_wins_losses = actual_wins_losses,

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
        date_train_end = date_train_end,
        date_train_phase_2_end = date_train_phase_2_end,
        date_test_start = date_test_start,

        couplua_assets = couplua_assets,

        stop_value_var = stop_value_var,
        profit_value_var = profit_value_var,
        period_var = period_var,

        bin_var_col = c("period_return_15_Price", "period_return_25_Price", "period_return_35_Price"),
        trade_direction = "Long",
        save_path = "C:/Users/nikhi/Documents/trade_data/Day_Trader_Single_Asset_V2_trade_store_stop_2"
      )

    long_sim_transformed <-
      long_sim %>%
      filter(Date >= date_test_start) %>%
      left_join(actual_wins_losses %>%
                  filter(trade_col == "Long",
                         stop_factor == stop_value_var,
                         profit_factor == profit_value_var)
                ) %>%
      mutate(
        trade_col = "Long",
        test_end_date = test_end_date,
        date_train_end = date_train_end,
        date_train_phase_2_end = date_train_phase_2_end,
        date_test_start = date_test_start,
        sim_index = k,
        bin_var_col = "period_return_15_Price, period_return_25_Price, period_return_35_Price"
      )

    complete_sim <-
      list(long_sim_transformed) %>%
      map_dfr(bind_rows)

    if(dim(complete_sim)[1] > 0) {
      c = c + 1
      if(redo_db == TRUE & c == 1) {
        write_table_sql_lite(.data = complete_sim,
                             table_name = "single_asset_improved",
                             conn = model_data_store_db,
                             overwrite_true = TRUE)
        redo_db = FALSE
      }

      if(redo_db == FALSE) {
        append_table_sql_lite(.data = complete_sim,
                              table_name = "single_asset_improved",
                              conn = model_data_store_db)
      }
    }

    rm(short_sim, long_sim, long_sim_transformed)

    gc()
    Sys.sleep(1)
    gc()

  }

}

indicator_data <-
  get_indicator_pred_from_db(
  model_data_store_path =
    "C:/Users/nikhi/Documents/trade_data/Day_Trader_Single_Asset_V2_trade_store_stop_2.db"
)

c("period_return_24_Price",
  "period_return_30_Price",
  "period_return_44_Price") %>%
  map(
    ~
      prepare_post_ss_gen2_model(
        indicator_data = indicator_data,
        new_post_DB = TRUE,
        new_sym = TRUE,
        post_model_data_save_path =
          "C:/Users/nikhi/Documents/trade_data/Day_Trader_Single_Asset_V2_trade_store_stop_2/",
        dependant_var = .x,
        dependant_threshold = 5,
        # training_date_start = "2023-01-04",
        # training_date_end = "2024-01-01"
        # training_date_start = "2024-07-04",
        # training_date_end = "2025-07-01"
        training_date_start = "2024-07-04",
        training_date_end = "2025-09-01"
      )
  )

post_preds_all <-
  read_post_models_and_get_preds(
    indicator_data = indicator_data,
    post_model_data_save_path =
      "C:/Users/nikhi/Documents/trade_data/Day_Trader_Single_Asset_V2_trade_store_stop_2/",
    # test_date_start = "2023-01-04",
    test_date_start = "2024-07-04",
    test_date_end = today(),
    dependant_var = "period_return_24_Price",
    dependant_threshold = 5
  )


post_preds_all_rolling <-
  get_rolling_post_preds(
    post_pred_data = post_preds_all,
    rolling_periods = c(3,50,100,200,400,500,2000),
    # test_date_start = "2024-02-01",
    # test_date_start = "2025-08-01",
    test_date_start = "2025-10-01",
    test_date_end = today(),
    pred_price_cols =
      c("period_return_24_Price",
        "period_return_30_Price",
        "period_return_44_Price")
  )

trade_statement =
  "(mean_3_pred_GLM_period_return_24_Price >
          mean_50_pred_GLM_period_return_24_Price + sd_50_pred_GLM_period_return_24_Price*2 |
  pred_GLM_period_return_24_Price >
            mean_50_pred_GLM_period_return_24_Price + sd_50_pred_GLM_period_return_24_Price*2)|
  (mean_3_pred_LM_period_return_24_Price >
          mean_500_pred_LM_period_return_24_Price + sd_500_pred_LM_period_return_24_Price*2 |
  pred_LM_period_return_24_Price >
            mean_500_pred_LM_period_return_24_Price + sd_500_pred_LM_period_return_24_Price*2)|
  (mean_3_pred_GLM_period_return_24_Price >
            mean_500_pred_GLM_period_return_24_Price + sd_500_pred_GLM_period_return_24_Price*2.5 |
    pred_GLM_period_return_24_Price >
              mean_500_pred_GLM_period_return_24_Price + sd_500_pred_GLM_period_return_24_Price*2.5)|
  (mean_3_pred_LM_period_return_24_Price >
          mean_50_pred_LM_period_return_24_Price + sd_50_pred_LM_period_return_24_Price*2.5 |
  pred_LM_period_return_24_Price >
            mean_50_pred_LM_period_return_24_Price + sd_50_pred_LM_period_return_24_Price*2.5)|
  (mean_3_pred_GLM_period_return_24_Price >
            mean_100_pred_GLM_period_return_24_Price + sd_100_pred_GLM_period_return_24_Price*2.5 |
    pred_GLM_period_return_24_Price >
              mean_100_pred_GLM_period_return_24_Price + sd_100_pred_GLM_period_return_24_Price*2.5)|
  (mean_3_pred_LM_period_return_24_Price >
          mean_100_pred_LM_period_return_24_Price + sd_100_pred_LM_period_return_24_Price*2 |
  pred_LM_period_return_24_Price >
            mean_100_pred_LM_period_return_24_Price + sd_100_pred_LM_period_return_24_Price*2)|
  (mean_3_pred_LM_period_return_24_Price >
          mean_500_pred_LM_period_return_24_Price + sd_500_pred_LM_period_return_24_Price*2 |
  pred_LM_period_return_24_Price >
            mean_500_pred_LM_period_return_24_Price + sd_500_pred_LM_period_return_24_Price*2)|
  (mean_3_pred_LM_period_return_24_Price >
          mean_2000_pred_LM_period_return_24_Price + sd_2000_pred_LM_period_return_24_Price*2 |
  pred_LM_period_return_24_Price >
            mean_2000_pred_LM_period_return_24_Price + sd_2000_pred_LM_period_return_24_Price*2)"

trade_statement =
  "
      ((
      pred_GLM_period_return_24_Price > mean_50_pred_GLM_period_return_24_Price &
      mean_50_pred_GLM_period_return_24_Price > mean_2000_pred_GLM_period_return_24_Price &
      pred_GLM_period_return_24_Price > mean_2000_pred_GLM_period_return_24_Price &

      pred_GLM_period_return_30_Price > mean_50_pred_GLM_period_return_30_Price &
      mean_50_pred_GLM_period_return_30_Price > mean_2000_pred_GLM_period_return_30_Price &
      pred_GLM_period_return_30_Price > mean_2000_pred_GLM_period_return_30_Price &

      pred_GLM_period_return_44_Price > mean_50_pred_GLM_period_return_44_Price &
      mean_50_pred_GLM_period_return_44_Price > mean_2000_pred_GLM_period_return_44_Price &
      pred_GLM_period_return_44_Price > mean_2000_pred_GLM_period_return_44_Price
      ) &
      (
      pred_LM_period_return_24_Price > mean_50_pred_LM_period_return_24_Price &
      pred_LM_period_return_44_Price > mean_50_pred_LM_period_return_44_Price &
      pred_LM_period_return_30_Price > mean_50_pred_LM_period_return_30_Price &
      mean_50_pred_LM_period_return_24_Price > mean_2000_pred_LM_period_return_24_Price &
      mean_50_pred_LM_period_return_44_Price > mean_2000_pred_LM_period_return_44_Price &
      mean_50_pred_LM_period_return_30_Price > mean_2000_pred_LM_period_return_30_Price &
      pred_LM_period_return_24_Price < -1 &
      pred_LM_period_return_44_Price < -1 &
      pred_LM_period_return_30_Price < -1
      ))|
      (pred_LM_period_return_24_Price >
          mean_50_pred_LM_period_return_24_Price + sd_50_pred_LM_period_return_24_Price*1 &

        mean_3_pred_LM_period_return_24_Price >
          mean_50_pred_LM_period_return_24_Price + sd_50_pred_LM_period_return_24_Price*1 &

        pred_LM_period_return_30_Price >
          mean_50_pred_LM_period_return_30_Price + sd_50_pred_LM_period_return_30_Price*1 &

        mean_3_pred_LM_period_return_30_Price >
          mean_50_pred_LM_period_return_30_Price + sd_50_pred_LM_period_return_30_Price*1 &

        pred_LM_period_return_44_Price >
          mean_50_pred_LM_period_return_30_Price + sd_50_pred_LM_period_return_44_Price*1 &

        mean_3_pred_LM_period_return_44_Price >
          mean_50_pred_LM_period_return_44_Price + sd_50_pred_LM_period_return_44_Price*1)|
        (pred_GLM_period_return_24_Price >
          mean_50_pred_GLM_period_return_24_Price + sd_50_pred_GLM_period_return_24_Price*1 &

        mean_3_pred_GLM_period_return_24_Price >
          mean_50_pred_GLM_period_return_24_Price + sd_50_pred_GLM_period_return_24_Price*1 &

        pred_GLM_period_return_30_Price >
          mean_50_pred_GLM_period_return_30_Price + sd_50_pred_GLM_period_return_30_Price*1 &

        mean_3_pred_GLM_period_return_30_Price >
          mean_50_pred_GLM_period_return_30_Price + sd_50_pred_GLM_period_return_30_Price*1 &

        pred_GLM_period_return_44_Price >
          mean_50_pred_GLM_period_return_30_Price + sd_50_pred_GLM_period_return_44_Price*1 &

        mean_3_pred_GLM_period_return_44_Price >
          mean_50_pred_GLM_period_return_44_Price + sd_50_pred_GLM_period_return_44_Price*1 &
      pred_LM_period_return_24_Price > 0 &
      pred_LM_period_return_44_Price > 0 &
      pred_LM_period_return_30_Price > 0
          )|
      (mean_3_pred_LM_period_return_24_Price <
                mean_2000_pred_LM_period_return_24_Price - sd_2000_pred_LM_period_return_24_Price*2 &
          mean_3_pred_LM_period_return_30_Price <
                mean_2000_pred_LM_period_return_30_Price - sd_2000_pred_LM_period_return_30_Price*2 &
          mean_3_pred_LM_period_return_44_Price <
                  mean_2000_pred_LM_period_return_30_Price - sd_2000_pred_LM_period_return_44_Price*2 &

        mean_3_pred_LM_period_return_24_Price <
                mean_50_pred_LM_period_return_24_Price - sd_50_pred_LM_period_return_24_Price*2 &
          mean_3_pred_LM_period_return_30_Price <
                mean_50_pred_LM_period_return_30_Price - sd_50_pred_LM_period_return_30_Price*2 &
          mean_3_pred_LM_period_return_44_Price <
                  mean_50_pred_LM_period_return_30_Price - sd_50_pred_LM_period_return_44_Price*2 &

        pred_LM_period_return_24_Price <
                  mean_2000_pred_LM_period_return_24_Price - sd_2000_pred_LM_period_return_24_Price*2 &
            pred_LM_period_return_30_Price <
                  mean_2000_pred_LM_period_return_30_Price - sd_2000_pred_LM_period_return_30_Price*2 &
            pred_LM_period_return_44_Price <
                  mean_2000_pred_LM_period_return_30_Price - sd_2000_pred_LM_period_return_44_Price*2 &
        pred_LM_period_return_24_Price <
                  mean_50_pred_LM_period_return_24_Price - sd_50_pred_LM_period_return_24_Price*2 &
            pred_LM_period_return_30_Price <
                  mean_50_pred_LM_period_return_30_Price - sd_50_pred_LM_period_return_30_Price*2 &
            pred_LM_period_return_44_Price <
                  mean_50_pred_LM_period_return_30_Price - sd_50_pred_LM_period_return_44_Price*2 &
          pred_LM_period_return_24_Price > 0 &
          pred_LM_period_return_44_Price > 0 &
          pred_LM_period_return_30_Price > 0   )
"

win_thresh = 10

comnbined_statement_best_results <-
  post_preds_all_rolling %>%
  pull(Asset) %>%
  unique() %>%
  map(
    ~
    post_ss_model_analyse_condition(
        tagged_trade_col_data = post_preds_all_rolling,
        trade_statement = trade_statement,
        Asset_Var = .x,
        win_thresh = win_thresh,
        trade_direction = "Long",
        actual_wins_losses = actual_wins_losses
      ) %>%
      pluck(1) %>%
      ungroup() %>%
      filter(Period <= 24) %>%
      filter(Period >= 24) %>%
      filter(trade_col == "Long") %>%
      # group_by(Asset) %>%
      # slice_max(perc, n = 1) %>%
      group_by(Asset) %>%
      slice_max(Total_Returns) %>%
      ungroup()
  ) %>%
  keep(~ dim(.x)[1] > 0) %>%
  map_dfr(bind_rows)
  # janitor::adorn_totals()

comnbined_statement_best_params <-
  comnbined_statement_best_results %>%
  dplyr::select(Asset, Period) %>%
  mutate(
      best_result = TRUE
  ) %>%
  distinct()

comnbined_statement_control <-
  post_preds_all_rolling %>%
  pull(Asset) %>%
  unique() %>%
  map(
    ~
      post_ss_model_analyse_condition(
        tagged_trade_col_data = post_preds_all_rolling,
        trade_statement = "pred_LM_period_return_24_Price > 0 |
                          pred_LM_period_return_24_Price <= 0 |
                          is.na(pred_LM_period_return_24_Price)|
                          is.nan(pred_LM_period_return_24_Price) |
                          is.infinite(pred_LM_period_return_24_Price)",
        Asset_Var = .x,
        win_thresh = win_thresh,
        trade_direction = "Long",
        actual_wins_losses = actual_wins_losses
      ) %>%
      pluck(1) %>%
      ungroup()
  ) %>%
  keep(~ dim(.x)[1] > 0) %>%
  map_dfr(bind_rows) %>%
  dplyr::left_join(comnbined_statement_best_params) %>%
  filter(best_result == TRUE) %>%
  arrange(Total_Returns) %>%
  # janitor::adorn_totals() %>%
  dplyr::select(Asset, Period,
                total_trades_control = total_trades,
                Total_Returns_Control = Total_Returns,
                Average_Return_Control = Average_Return,
                perc_control = perc)

comapre_results_summary <-
  comnbined_statement_best_results %>%
  dplyr::select(-trade_statement, -trade_col) %>%
  left_join(comnbined_statement_control) %>%
  mutate(
    trade_percent = total_trades/total_trades_control,
    Total_Returns_Control_adj = Total_Returns_Control*trade_percent
  ) %>%
  mutate(
    Returns_Diff = Total_Returns - Total_Returns_Control_adj,
    perc_diff = perc - perc_control
  )

comapre_results_summary$perc_diff %>% mean()
comapre_results_summary$Returns_Diff %>% mean()

comapre_results_summary <-
  comapre_results_summary %>%
  janitor::adorn_totals()

portfolio_ts <-
  post_preds_all_rolling %>%
  # filter(Asset != 'BTC_USD') %>%
  pull(Asset) %>%
  unique() %>%
  map(
    ~
      post_ss_model_analyse_condition(
        tagged_trade_col_data = post_preds_all_rolling,
        trade_statement = trade_statement,
        Asset_Var = .x,
        win_thresh = win_thresh,
        trade_direction = "Long",
        actual_wins_losses = actual_wins_losses
      ) %>%
      pluck(2) %>%
      ungroup() %>%
      left_join(
        comnbined_statement_best_params
      ) %>%
      filter(best_result == TRUE)
  ) %>%
  keep(~ dim(.x)[1] > 0) %>%
  map_dfr(bind_rows) %>%
  filter(trade_col == "Long")

portfolio_ts_control <-
  post_preds_all_rolling %>%
  # filter(Asset != 'BTC_USD') %>%
  pull(Asset) %>%
  unique() %>%
  map(
    ~
      post_ss_model_analyse_condition(
        tagged_trade_col_data = post_preds_all_rolling,
        trade_statement = "pred_LM_period_return_24_Price > 0 |
                          pred_LM_period_return_24_Price <= 0 |
                          is.na(pred_LM_period_return_24_Price)|
                          is.nan(pred_LM_period_return_24_Price) |
                          is.infinite(pred_LM_period_return_24_Price)",
        Asset_Var = .x,
        win_thresh = win_thresh,
        trade_direction = "Long",
        actual_wins_losses = actual_wins_losses
      ) %>%
      pluck(2) %>%
      ungroup() %>%
      left_join(
        comnbined_statement_best_params
      ) %>%
      filter(best_result == TRUE)
  ) %>%
  keep(~ dim(.x)[1] > 0) %>%
  map_dfr(bind_rows) %>%
  mutate(trade_col = "No Trade Long")

portfolio_ts <-
  portfolio_ts %>%
  bind_rows(portfolio_ts_control) %>%
  group_by(Date, trade_col) %>%
  summarise(Returns = sum(Returns, na.rm = T )) %>%
  group_by(trade_col) %>%
  arrange(Date, .by_group = TRUE) %>%
  group_by(trade_col) %>%
  mutate(
    Returns_cumulative = cumsum(Returns)
  )

portfolio_ts %>%
  ggplot(aes(x = Date, y = Returns_cumulative, color = trade_col)) +
  geom_line() +
  facet_wrap(.~ trade_col, scales = "free_y") +
  theme_minimal() +
  theme(legend.position = "bottom")


temp_actual_wins_losses <- list()

for (i in 1:length(assets_to_analyse)) {

  temp_actual_wins_losses[[i]] <-
    create_running_profits(
      asset_of_interest = assets_to_analyse[i],
      asset_data = Indices_Metals_Bonds,
      stop_factor = stop_value_var,
      profit_factor = profit_value_var,
      risk_dollar_value = 15,
      trade_direction = "Short",
      currency_conversion = currency_conversion,
      asset_infor = asset_infor
    )

}

actual_wins_losses_Short <-
  temp_actual_wins_losses %>%
  map_dfr(bind_rows) %>%
  dplyr::select(-volume_unadj, -minimumTradeSize_OG, -marginRate,
                -adjusted_conversion, -pipLocation, -minimumTradeSize_OG) %>%
  dplyr::rename(
    High = Bid_High,
    Low =  Bid_Low
  ) %>%
  mutate(
    trade_return_dollar_aud = !!as.name(glue::glue("period_return_{period_var}_Price") ),

    trade_start_prices =
      case_when(
        trade_col == "Long" ~ Ask_Price,
        trade_col == "Short" ~ Bid_Price
      ),
    trade_end_prices =
      case_when(
        trade_col == "Long" ~ Bid_Price,
        trade_col == "Short" ~ Ask_Price
      ),
    stop_factor = stop_value_var,
    profit_factor = profit_value_var,
    periods_ahead = period_var
  )

trade_statement_short =
  "(pred_LM_period_return_24_Price <
          mean_50_pred_LM_period_return_24_Price - sd_50_pred_LM_period_return_24_Price*1 &

        mean_3_pred_LM_period_return_24_Price <
          mean_50_pred_LM_period_return_24_Price - sd_50_pred_LM_period_return_24_Price*1 &

        pred_LM_period_return_30_Price <
          mean_50_pred_LM_period_return_30_Price - sd_50_pred_LM_period_return_30_Price*1 &

        mean_3_pred_LM_period_return_30_Price <
          mean_50_pred_LM_period_return_30_Price - sd_50_pred_LM_period_return_30_Price*1 &

        pred_LM_period_return_44_Price <
          mean_50_pred_LM_period_return_30_Price - sd_50_pred_LM_period_return_44_Price*1 &

        mean_3_pred_LM_period_return_44_Price <
          mean_50_pred_LM_period_return_44_Price - sd_50_pred_LM_period_return_44_Price*1)|

      (pred_LM_period_return_24_Price >
          mean_50_pred_LM_period_return_24_Price + sd_50_pred_LM_period_return_24_Price*1.5 &

        mean_3_pred_LM_period_return_24_Price >
          mean_50_pred_LM_period_return_24_Price + sd_50_pred_LM_period_return_24_Price*1.5 &

        pred_LM_period_return_30_Price >
          mean_50_pred_LM_period_return_30_Price + sd_50_pred_LM_period_return_30_Price*1.5 &

        mean_3_pred_LM_period_return_30_Price >
          mean_50_pred_LM_period_return_30_Price + sd_50_pred_LM_period_return_30_Price*1.5 &

        pred_LM_period_return_44_Price >
          mean_50_pred_LM_period_return_30_Price + sd_50_pred_LM_period_return_44_Price*1.5 &

        mean_3_pred_LM_period_return_44_Price >
          mean_50_pred_LM_period_return_44_Price + sd_50_pred_LM_period_return_44_Price*1.5)|

        (pred_GLM_period_return_24_Price >
          mean_50_pred_GLM_period_return_24_Price + sd_50_pred_GLM_period_return_24_Price*1.5 &

        mean_3_pred_GLM_period_return_24_Price >
          mean_50_pred_GLM_period_return_24_Price + sd_50_pred_GLM_period_return_24_Price*1.5 &

        pred_GLM_period_return_30_Price >
          mean_50_pred_GLM_period_return_30_Price + sd_50_pred_GLM_period_return_30_Price*1.5 &

        mean_3_pred_GLM_period_return_30_Price >
          mean_50_pred_GLM_period_return_30_Price + sd_50_pred_GLM_period_return_30_Price*1.5 &

        pred_GLM_period_return_44_Price >
          mean_50_pred_GLM_period_return_30_Price + sd_50_pred_GLM_period_return_44_Price*1.5 &

        mean_3_pred_GLM_period_return_44_Price >
          mean_50_pred_GLM_period_return_44_Price + sd_50_pred_GLM_period_return_44_Price*1.5)|

       (pred_GLM_period_return_24_Price <
           mean_50_pred_GLM_period_return_24_Price - sd_50_pred_GLM_period_return_24_Price*1 &

           mean_3_pred_GLM_period_return_24_Price <
           mean_50_pred_GLM_period_return_24_Price - sd_50_pred_GLM_period_return_24_Price*1 &

           pred_GLM_period_return_30_Price <
           mean_50_pred_GLM_period_return_30_Price - sd_50_pred_GLM_period_return_30_Price*1 &

           mean_3_pred_GLM_period_return_30_Price <
           mean_50_pred_GLM_period_return_30_Price - sd_50_pred_GLM_period_return_30_Price*1 &

           pred_GLM_period_return_44_Price <
           mean_50_pred_GLM_period_return_30_Price - sd_50_pred_GLM_period_return_44_Price*1 &

           mean_3_pred_GLM_period_return_44_Price <
           mean_50_pred_GLM_period_return_44_Price - sd_50_pred_GLM_period_return_44_Price*1)"

win_thresh = 10

comnbined_statement_best_results_short <-
  post_preds_all_rolling %>%
  pull(Asset) %>%
  unique() %>%
  map(
    ~
      post_ss_model_analyse_condition(
        tagged_trade_col_data = post_preds_all_rolling,
        trade_statement = trade_statement_short,
        Asset_Var = .x,
        win_thresh = win_thresh,
        trade_direction = "Long",
        actual_wins_losses = actual_wins_losses
      ) %>%
      pluck(1) %>%
      ungroup() %>%
      filter(Period <= 46) %>%
      filter(Period >= 40) %>%
      filter(trade_col == "Long") %>%
      # group_by(Asset) %>%
      # slice_max(perc, n = 10) %>%
      group_by(Asset) %>%
      slice_max(Total_Returns) %>%
      ungroup()
  ) %>%
  map_dfr(bind_rows) %>%
  arrange(Total_Returns) %>%
  janitor::adorn_totals()


comnbined_statement_best_params <-
  comnbined_statement_best_results_short %>%
  dplyr::select(Asset, Period) %>%
  mutate(
    best_result = TRUE
  ) %>%
  distinct()

portfolio_ts <-
  post_preds_all_rolling %>%
  # filter(Asset != 'BTC_USD') %>%
  pull(Asset) %>%
  unique() %>%
  map(
    ~
      post_ss_model_analyse_condition(
        tagged_trade_col_data = post_preds_all_rolling,
        trade_statement = trade_statement_short,
        Asset_Var = .x,
        win_thresh = win_thresh,
        trade_direction = "Long",
        actual_wins_losses = actual_wins_losses
      ) %>%
      pluck(2) %>%
      ungroup() %>%
      left_join(
        comnbined_statement_best_params
      ) %>%
      filter(best_result == TRUE)
  ) %>%
  map_dfr(bind_rows)

portfolio_ts <-
  portfolio_ts %>%
  group_by(Date, trade_col) %>%
  summarise(Returns = sum(Returns, na.rm = T )) %>%
  group_by(trade_col) %>%
  arrange(Date, .by_group = TRUE) %>%
  group_by(trade_col) %>%
  mutate(
    Returns_cumulative = cumsum(Returns)
  )

portfolio_ts %>%
  ggplot(aes(x = Date, y = Returns_cumulative, color = trade_col)) +
  geom_line() +
  facet_wrap(.~ trade_col, scales = "free_y") +
  theme_minimal() +
  theme(legend.position = "bottom")

#---------------------------------------------------------
syms_in_data <-
  indicator_data %>%
  pull(Asset) %>%
  unique()

for (uu in 1:length(rolling_periods) ) {

  testing_data_post_all_assets <-
    all_asset_post_model_data  %>%
    filter(Date > date_test_start + months(post_training_months))

  testing_data_post_all_assets <-
    testing_data_post_all_assets %>%
    mutate(
      Post_Pred = predict.glm(object = glm_model,
                              newdata = testing_data_post_all_assets,
                              type = "response"),
      Post_Pred_lin = predict.lm(object = lm_model,
                                 newdata = testing_data_post_all_assets)
    ) %>%
    group_by(Asset) %>%
    arrange(Date, .by_group = TRUE) %>%
    ungroup() %>%
    group_by(Asset) %>%
    mutate(
      rolling_pred_mean =
        slider::slide_dbl(
          .x = Post_Pred,
          .f = ~mean(.x, na.rm = T),
          .before = rolling_periods[uu]),

      rolling_pred_sd =
        slider::slide_dbl(
          .x = Post_Pred,
          .f = ~sd(.x, na.rm = T),
          .before = rolling_periods[uu]),

      rolling_pred_mean_lin =
        slider::slide_dbl(
          .x = Post_Pred_lin,
          .f = ~mean(.x, na.rm = T),
          .before = rolling_periods[uu]),

      rolling_pred_sd_lin =
        slider::slide_dbl(
          .x = Post_Pred_lin,
          .f = ~sd(.x, na.rm = T),
          .before = rolling_periods[uu])
    )

  for (kk in 1:length(pred_threshs) ) {

    post_return_raw <-
      testing_data_post_all_assets %>%
      mutate(
        trade_col_Post_Pred_Bin =
          case_when(
            Post_Pred >= rolling_pred_mean + rolling_pred_sd*pred_threshs[kk] ~ "Long",
            TRUE ~ "No Trades"
          ),
        trade_col_Post_Pred_Lin =
          case_when(
            Post_Pred_lin >= rolling_pred_mean_lin + rolling_pred_sd_lin*pred_threshs[kk] ~ "Long",
            TRUE ~ "No Trades"
          )
      ) %>%
      dplyr::select(Date, Asset,trade_col_Post_Pred_Bin,
                    trade_col_Post_Pred_Lin, Post_Pred, contains("period_return_")) %>%
      pivot_longer(-c(Date, Asset,
                      trade_col_Post_Pred_Bin,
                      trade_col_Post_Pred_Lin, Post_Pred),
                   values_to = "Return", names_to = "Period") %>%
      mutate(
        Period = str_remove_all(Period, "[A-Z]+|[a-z]+|_") %>% str_trim() %>% as.numeric()
      ) %>%
      mutate(
        win = ifelse(Return > 0, 1, 0)
      ) %>%
      mutate(
        pred_thresh = pred_threshs[kk],
        rolling_periods = rolling_periods[uu],
        post_training_months = post_training_months
      )

    post_bin_return <-
      post_return_raw %>%
      group_by(Asset, trade_col_Post_Pred_Bin, Period,
               pred_thresh, rolling_periods, post_training_months) %>%
      summarise(
        total_trades = n_distinct(Date),
        wins = sum(win, na.rm = T),
        perc = wins/total_trades,
        total_return = sum(Return, na.rm = T),

        very_low_return = round( quantile(Return, 0.1, na.rm = T), 4),
        low_return = round( quantile(Return, 0.25, na.rm = T), 4),
        mid_return = round( quantile(Return, 0.5, na.rm = T), 4 ),
        high_return = round( quantile(Return, 0.75, na.rm = T), 4 ),
        very_high_return = round( quantile(Return, 0.9, na.rm = T), 4 ),
        mean_return = mean(Return, na.rm = T)
      )

    post_lin_return <-
      post_return_raw %>%
      group_by(Asset, trade_col_Post_Pred_Lin, Period,
               pred_thresh, rolling_periods, post_training_months) %>%
      summarise(
        total_trades = n_distinct(Date),
        wins = sum(win, na.rm = T),
        perc = wins/total_trades,
        total_return = sum(Return, na.rm = T),

        very_low_return = round( quantile(Return, 0.1, na.rm = T), 4),
        low_return = round( quantile(Return, 0.25, na.rm = T), 4),
        mid_return = round( quantile(Return, 0.5, na.rm = T), 4 ),
        high_return = round( quantile(Return, 0.75, na.rm = T), 4 ),
        very_high_return = round( quantile(Return, 0.9, na.rm = T), 4 ),
        mean_return = mean(Return, na.rm = T)
      )

    if(new_post_DB == TRUE) {

      write_table_sql_lite(.data = post_bin_return,
                           table_name = "raw_post_model_bin_analysis_sum",
                           conn = post_model_data_store_db,
                           overwrite_true = TRUE )

      write_table_sql_lite(.data = post_lin_return,
                           table_name = "raw_post_model_lin_analysis_sum",
                           conn = post_model_data_store_db,
                           overwrite_true = TRUE )

      new_post_DB = FALSE
    }

    if(new_post_DB == FALSE) {

      append_table_sql_lite(.data = post_bin_return,
                            table_name = "raw_post_model_bin_analysis_sum",
                            conn = post_model_data_store_db)

      append_table_sql_lite(.data = post_lin_return,
                            table_name = "raw_post_model_lin_analysis_sum",
                            conn = post_model_data_store_db)

      new_post_DB = FALSE
    }

  }

}

control_returns_lin <-
  DBI::dbGetQuery(conn = post_model_data_store_db,
                  statement = "SELECT * FROM raw_post_model_lin_analysis_sum") %>%
  group_by(Asset, Period, pred_thresh, rolling_periods, post_training_months) %>%
  summarise(wins_control = sum(wins),
            total_return_control = sum(total_return),
            total_trades_control = sum(total_trades)) %>%
  mutate(perc_control = wins_control/total_trades_control) %>%
  ungroup()

post_lin_best_return <-
  DBI::dbGetQuery(conn = post_model_data_store_db,
                  statement = "SELECT * FROM raw_post_model_lin_analysis_sum") %>%
  # filter(total_return > 0) %>%
  filter(total_trades > 500) %>%
  # filter(Period == 24) %>%
  group_by(Asset, trade_col_Post_Pred_Lin) %>%
  slice_max(total_return)%>%
  ungroup() %>%
  left_join(control_returns_lin) %>%
  dplyr::select(Asset,pred_thresh ,trade_col_Post_Pred_Lin, total_return,
                total_return_control, perc, perc_control,
                total_trades_control,
                total_trades) %>%
  mutate(
    return_scale_factor = total_trades/total_trades_control,
        return_edge = total_return - return_scale_factor*total_return_control,
         perc_edge = round(perc - perc_control, 5)
    ) %>%
  dplyr::select(-return_scale_factor)

post_lin_best_perc <-
  DBI::dbGetQuery(conn = post_model_data_store_db,
                  statement = "SELECT * FROM raw_post_model_lin_analysis_sum") %>%
  # filter(total_return > 0) %>%
  filter(total_trades > 500) %>%
  # filter(Period == 24) %>%
  group_by(Asset, trade_col_Post_Pred_Lin) %>%
  slice_max(perc) %>%
  group_by(Asset, trade_col_Post_Pred_Lin) %>%
  slice_max(total_return) %>%
  left_join(control_returns_lin) %>%
  dplyr::select(Asset,pred_thresh ,trade_col_Post_Pred_Lin, total_return,
                total_return_control, perc, perc_control,
                total_trades_control,
                total_trades) %>%
  mutate(return_edge = total_return - total_return_control,
         perc_edge = round(perc - perc_control, 5))

control_returns_bin <-
  DBI::dbGetQuery(conn = post_model_data_store_db,
                  statement = "SELECT * FROM raw_post_model_bin_analysis_sum") %>%
  group_by(Asset, Period, pred_thresh, rolling_periods, post_training_months) %>%
  summarise(wins_control = sum(wins),
            total_return_control = sum(total_return),
            total_trades_control = sum(total_trades)) %>%
  mutate(perc_control = wins_control/total_trades_control) %>%
  ungroup()

post_bin_best_return <-
  DBI::dbGetQuery(conn = post_model_data_store_db,
                  statement = "SELECT * FROM raw_post_model_bin_analysis_sum") %>%
  filter(total_trades > 100) %>%
  group_by(Asset, trade_col_Post_Pred_Bin) %>%
  slice_max(perc) %>%
  group_by(Asset, trade_col_Post_Pred_Bin) %>%
  slice_max(total_return) %>%
  left_join(control_returns_bin) %>%
  dplyr::select(Asset,pred_thresh ,trade_col_Post_Pred_Bin, total_return,
                total_return_control, perc, perc_control,
                total_trades_control,
                total_trades) %>%
  mutate(
    return_scale_factor = total_trades/total_trades_control,
    return_edge = total_return - return_scale_factor*total_return_control,
    perc_edge = round(perc - perc_control, 5)
  ) %>%
  dplyr::select(-return_scale_factor)

DBI::dbDisconnect(post_model_data_store_db)
rm(post_model_data_store_db)

#-------------------------------------------Asset Specific Models
post_model_accumulator <- list()
post_model_LM_accumulator <- list()

post_model_data_store_path <-
  "C:/Users/nikhi/Documents/trade_data/Day_Trader_Single_Asset_V2_post_model_unrest.db"
post_model_data_store_db <-
  connect_db(post_model_data_store_path)

new_post_DB <- TRUE

pred_threshs <- seq(0, 4, 0.25)
rolling_periods <- c(50,2000, 100)
post_training_months <- 18
new_sym = TRUE

for (oo in 1:length(syms_in_data) ) {

  test_combined <-
    indicator_data %>%
    ungroup() %>%
    filter(Date >= date_test_start) %>%
    filter(Asset == syms_in_data[oo]) %>%
    dplyr::select(-contains("_sd")) %>%
    dplyr::select(-contains("_mean")) %>%
    dplyr::select(Date, Asset,
                  contains("pred_"),
                  contains("period_return_")
                  # period_return_35_Price,
                  # period_return_25_Price,
                  # period_return_15_Price
                  ) %>%
    mutate(
      Averaged_Bin_Pred = (pred_macro_4 + pred_index_4 + pred_combined_4 +
                             pred_daily_4 + pred_technical_4 + pred_copula_4)/6,

      Averaged_Bin_Pred_2 = (pred_macro_6 + pred_index_6 + pred_combined_6 +
                               pred_daily_6 + pred_technical_6 + pred_copula_6)/6,


      Averaged_Bin_Pred_3 = (pred_macro_2 + pred_index_2 + pred_combined_2 +
                               pred_daily_2 + pred_technical_2 + pred_copula_2)/6,

      Comb_Averaged_Bin_Pred_3 = (Averaged_Bin_Pred + Averaged_Bin_Pred_2 + Averaged_Bin_Pred_3)/3,

      high_return_date =
        case_when(
          period_return_35_Price > 5 & period_return_25_Price > 2.5 & period_return_15_Price > 0 ~ "Detected",
          TRUE ~ "Dont Trade"
        )
    )

  training_data_post_model <-
    test_combined %>%
    filter(Date <= date_test_start + months(post_training_months))

  lm_vars <- names(test_combined) %>%
    keep(~ str_detect(.x, "pred")) %>%
    unlist()

  lm_form <-
    create_lm_formula(dependant = "period_return_35_Price", independant = lm_vars)

  lm_model <-
    lm(data = training_data_post_model%>%
         filter(if_all(contains("pred"), ~ !is.nan(.) & !is.infinite(.) & !is.na(.)  )),
       formula = lm_form)

  summary(lm_model)

  sig_coefs <-
    get_sig_coefs(model_object_of_interest = lm_model,
                  p_value_thresh_for_inputs = 0.25)

  lm_form <-
    create_lm_formula(dependant = "period_return_35_Price", independant = sig_coefs)

  lm_model <-
    lm(data = training_data_post_model %>%
         filter(if_all(contains("pred"), ~ !is.nan(.) & !is.infinite(.) & !is.na(.)  )),
       formula = lm_form)

  post_model_LM_accumulator[[oo]] <- lm_model

  summary(lm_model)

  glm_form <-
    create_lm_formula(dependant = "high_return_date == 'Detected' ", independant = lm_vars)

  glm_model <-
    glm(data =training_data_post_model%>%
          filter(if_all(contains("pred"), ~ !is.nan(.) & !is.infinite(.) & !is.na(.)  )),
        formula = glm_form, family = binomial("logit"))

  summary(glm_model)

  sig_coefs <-
    get_sig_coefs(model_object_of_interest = glm_model,
                  p_value_thresh_for_inputs = 0.99)

  glm_form <-
    create_lm_formula(dependant = "high_return_date == 'Detected' ", independant = sig_coefs)

  glm_model <-
    glm(data = training_data_post_model %>%
          filter(if_all(contains("pred"), ~ !is.nan(.) & !is.infinite(.) & !is.na(.)  )),
        formula = glm_form, family = binomial("logit"))

  summary(glm_model)

  post_model_accumulator[[oo]] <- glm_model

  for (uu in 1:length(rolling_periods) ) {

    testing_data_post <-
      test_combined %>%
      filter(Date > date_test_start + months(post_training_months))

    testing_data_post <-
      testing_data_post %>%
      mutate(
        Post_Pred = predict.glm(object = glm_model,
                                newdata = testing_data_post,
                                type = "response"),
        Post_Pred_lin = predict.lm(object = lm_model,
                                   newdata = testing_data_post)
      ) %>%
      group_by(Asset) %>%
      arrange(Date, .by_group = TRUE) %>%
      ungroup() %>%
      group_by(Asset) %>%
      mutate(
        rolling_pred_mean =
          slider::slide_dbl(
            .x = Post_Pred,
            .f = ~mean(.x, na.rm = T),
            .before = rolling_periods[uu]),

        rolling_pred_sd =
          slider::slide_dbl(
            .x = Post_Pred,
            .f = ~sd(.x, na.rm = T),
            .before = rolling_periods[uu]),

        rolling_pred_mean_lin =
          slider::slide_dbl(
            .x = Post_Pred_lin,
            .f = ~mean(.x, na.rm = T),
            .before = rolling_periods[uu]),

        rolling_pred_sd_lin =
          slider::slide_dbl(
            .x = Post_Pred_lin,
            .f = ~sd(.x, na.rm = T),
            .before = rolling_periods[uu])
      )

    for (kk in 1:length(pred_threshs) ) {

      post_return_raw <-
        testing_data_post %>%
        mutate(
          trade_col_Post_Pred_Bin =
            case_when(
              Post_Pred >= rolling_pred_mean + rolling_pred_sd*pred_threshs[kk] ~ "Long",
              TRUE ~ "No Trades"
            ),
          trade_col_Post_Pred_Lin =
            case_when(
              Post_Pred_lin >= rolling_pred_mean_lin + rolling_pred_sd_lin*pred_threshs[kk] ~ "Long",
              TRUE ~ "No Trades"
            )
        ) %>%
        dplyr::select(Date, Asset,trade_col_Post_Pred_Bin,
                      trade_col_Post_Pred_Lin, Post_Pred, contains("period_return_")) %>%
        pivot_longer(-c(Date, Asset,
                        trade_col_Post_Pred_Bin,
                        trade_col_Post_Pred_Lin, Post_Pred),
                     values_to = "Return", names_to = "Period") %>%
        mutate(
          Period = str_remove_all(Period, "[A-Z]+|[a-z]+|_") %>% str_trim() %>% as.numeric()
        ) %>%
        mutate(
          win = ifelse(Return > 0, 1, 0)
        ) %>%
        mutate(
          pred_thresh = pred_threshs[kk],
          rolling_periods = rolling_periods[uu],
          post_training_months = post_training_months
        )

      post_bin_return <-
        post_return_raw %>%
        group_by(Asset, trade_col_Post_Pred_Bin, Period,
                 pred_thresh, rolling_periods, post_training_months) %>%
        summarise(
          total_trades = n_distinct(Date),
          wins = sum(win, na.rm = T),
          perc = wins/total_trades,
          total_return = sum(Return, na.rm = T),

          very_low_return = round( quantile(Return, 0.1, na.rm = T), 4),
          low_return = round( quantile(Return, 0.25, na.rm = T), 4),
          mid_return = round( quantile(Return, 0.5, na.rm = T), 4 ),
          high_return = round( quantile(Return, 0.75, na.rm = T), 4 ),
          very_high_return = round( quantile(Return, 0.9, na.rm = T), 4 ),
          mean_return = mean(Return, na.rm = T)
        )

      post_lin_return <-
        post_return_raw %>%
        group_by(Asset, trade_col_Post_Pred_Lin, Period,
                 pred_thresh, rolling_periods, post_training_months) %>%
        summarise(
          total_trades = n_distinct(Date),
          wins = sum(win, na.rm = T),
          perc = wins/total_trades,
          total_return = sum(Return, na.rm = T),

          very_low_return = round( quantile(Return, 0.1, na.rm = T), 4),
          low_return = round( quantile(Return, 0.25, na.rm = T), 4),
          mid_return = round( quantile(Return, 0.5, na.rm = T), 4 ),
          high_return = round( quantile(Return, 0.75, na.rm = T), 4 ),
          very_high_return = round( quantile(Return, 0.9, na.rm = T), 4 ),
          mean_return = mean(Return, na.rm = T)
        )

      if(new_sym == TRUE) {

        raw_db_temp <-
          glue::glue("C:/Users/nikhi/Documents/trade_data/Single_Asset_V2_Adv_Post_Results_Asset/{syms_in_data[oo]}_raw_post.db")

        raw_db_temp <-
          connect_db(raw_db_temp)

        write_table_sql_lite(.data = post_return_raw,
                             table_name = "raw_post_model_analysis_raw",
                             conn = raw_db_temp,
                             overwrite_true = TRUE )

        new_sym = FALSE

      }


      if(new_sym == FALSE) {
        append_table_sql_lite(.data = post_return_raw,
                              table_name = "raw_post_model_analysis_raw",
                              conn = raw_db_temp)
        }

      if(new_post_DB == TRUE) {

        write_table_sql_lite(.data = post_bin_return,
                             table_name = "raw_post_model_bin_analysis_sum",
                             conn = post_model_data_store_db,
                             overwrite_true = TRUE )

        write_table_sql_lite(.data = post_lin_return,
                             table_name = "raw_post_model_lin_analysis_sum",
                             conn = post_model_data_store_db,
                             overwrite_true = TRUE )

        new_post_DB = FALSE
      }

      if(new_post_DB == FALSE) {

        append_table_sql_lite(.data = post_bin_return,
                             table_name = "raw_post_model_bin_analysis_sum",
                             conn = post_model_data_store_db)

        append_table_sql_lite(.data = post_lin_return,
                              table_name = "raw_post_model_lin_analysis_sum",
                              conn = post_model_data_store_db)

        new_post_DB = FALSE
      }

    }

  }

  DBI::dbDisconnect(raw_db_temp)
  rm(raw_db_temp)
  gc()

  new_sym = TRUE

}

post_bin_best_return <-
  DBI::dbGetQuery(conn = post_model_data_store_db,
                  statement = "SELECT * FROM raw_post_model_bin_analysis_sum") %>%
  group_by(Asset, trade_col_Post_Pred_Bin) %>%
  slice_max(total_return)

control_returns_lin <-
  DBI::dbGetQuery(conn = post_model_data_store_db,
                  statement = "SELECT * FROM raw_post_model_lin_analysis_sum") %>%
  group_by(Asset, Period, pred_thresh, rolling_periods, post_training_months) %>%
  summarise(wins_control = sum(wins),
            total_return_control = sum(total_return),
            total_trades_control = sum(total_trades)) %>%
  mutate(perc_control = wins_control/total_trades_control) %>%
  ungroup()

post_lin_best_return <-
  DBI::dbGetQuery(conn = post_model_data_store_db,
                  statement = "SELECT * FROM raw_post_model_lin_analysis_sum") %>%
  # filter(total_return > 0) %>%
  filter(total_trades > 500) %>%
  # filter(Period == 24) %>%
  group_by(Asset, trade_col_Post_Pred_Lin) %>%
  slice_max(perc) %>%
  group_by(Asset, trade_col_Post_Pred_Lin) %>%
  slice_max(total_return) %>%
  left_join(control_returns_lin) %>%
  dplyr::select(Asset,pred_thresh ,trade_col_Post_Pred_Lin, total_return,
                total_return_control, perc, perc_control,
                total_trades_control,
                total_trades) %>%
  mutate(return_edge = total_return - total_return_control,
         perc_edge = round(perc - perc_control, 5))

best_distinct_trades <-
  post_lin_best_return %>%
  distinct(Asset, trade_col_Post_Pred_Lin,
           Period, rolling_periods, pred_thresh,
           total_return) %>%
  filter(total_return > 0)

for (hh in 1:dim(best_distinct_trades)[1]) {

  pred_thresh_temp <-
    best_distinct_trades$pred_thresh[hh] %>% as.numeric()
  rolling_periods_temp <-
    best_distinct_trades$rolling_periods[hh] %>% as.numeric()
  Period_temp <-
    best_distinct_trades$Period[hh] %>% as.numeric()
  Asset_temp <-
    best_distinct_trades$Asset[hh] %>% as.character()

  sql_statement <-
  glue::glue("SELECT [Date],[Asset],[Return] FROM raw_post_model_analysis_raw
   WHERE pred_thresh = {pred_thresh_temp} AND
   rolling_periods = {rolling_periods_temp} AND
   Period = {Period_temp} AND
   Asset = '{Asset_temp}'")

  return_asset_data_temp <-
    DBI::dbGetQuery(conn = post_model_data_store_db,
                    statement = sql_statement) %>%
    mutate(
      Date = as_datetime(Date)
    )


}


#-------------------------------Portfolio Analyser
#--------------------------------------------------------------

start_date = "2025-11-01"
end_date = "2025-12-12"

Indices_Metals_Bonds <-
  get_Port_Buy_Data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    time_frame = "H1"
  )

distinct_assets <-
  Indices_Metals_Bonds[[1]] %>%
  pull(Asset) %>%
  unique()

port_return_list <- list()

for (i in 1:length(distinct_assets)) {

  trades_to_tag_with_returns_long <-
    Indices_Metals_Bonds[[1]] %>%
    ungroup() %>%
    distinct(Asset, Date) %>%
    mutate(trade_col = "Long") %>%
    filter(trade_col == "Long")

  port_return_list[[i]] <-
    get_portfolio_model(
      asset_data = Indices_Metals_Bonds,
      asset_of_interest = distinct_assets[i],
      tagged_trades = trades_to_tag_with_returns_long,
      stop_factor_long = 6,
      profit_factor_long = 20,
      risk_dollar_value_long = 5,
      end_period = 20,
      time_frame = "H1"
    )

}

port_return_dfr <-
  port_return_list %>%
  map_dfr( ~
             .x %>%
             dplyr::select(adjusted_Date, Asset, Return, Date, close_Date, period_since_open)
  ) %>%
  ungroup() %>%
  group_by(adjusted_Date, Asset, Date,close_Date ) %>%
  summarise(Return = sum(Return),
            trades_open = n_distinct(Date)) %>%
  group_by(adjusted_Date) %>%
  mutate(
    Total_Return = sum(Return)
  )

