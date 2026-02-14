helpeR::load_custom_functions()

all_aud_symbols <- get_oanda_symbols() %>%
  keep(~ str_detect(.x, "AUD")|str_detect(.x, "USD_SEK|USD_NOK|USD_HUF|USD_ZAR|USD_CNY|USD_MXN|USD_CZK"))
asset_infor <- get_instrument_info()
aud_assets <- read_all_asset_data_intra_day(
  asset_list_oanda = all_aud_symbols,
  save_path_oanda_assets = "C:/Users/Nikhil Chandra/Documents/Asset Data/oanda_data/",
  read_csv_or_API = "API",
  time_frame = "D",
  bid_or_ask = "bid",
  how_far_back = 10,
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
db_location = "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data_Most_Assets_2025-09-13 2.db"
start_date = "2019-06-01"
end_date = today() %>% as.character()

# bin_factor = NULL
# stop_value_var = 2
# profit_value_var = 15
# period_var = 48

All_Daily_Data <-
  get_DAILY_ALGO_DATA_API_REQUEST()

Indices_Metals_Bonds <- list()

Indices_Metals_Bonds[[1]] <-
  get_db_data_quickly_algo(
    db_location = db_location,
    start_date = start_date,
    end_date = today() %>% as.character(),
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
    end_date = today() %>% as.character(),
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
      c("EUR_CHF", #1 EUR_CHF
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
        "XAG_SGD", #25 XAG_SGD
        "BCH_USD" , #26 BCH_USD
        "LTC_USD" #27 LTC_USD
      ),
    asset_data = Indices_Metals_Bonds,
    stop_factor = 5,
    profit_factor = 30,
    risk_dollar_value = 10,
    trade_direction = "Long",
    currency_conversion = currency_conversion,
    asset_infor = asset_infor,
    periods_ahead = 24
  )


assets_to_get_results <-
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
    "ETH_USD", #40
    # "EUR_CHF", #1 EUR_CHF
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
    # "XPD_USD" , #13 XPD_USD
    # "XPT_USD" , #14 XPT_USD
    "NATGAS_USD" , #15 NATGAS_USD
    "SG30_SGD" , #16 SG30_SGD
    # "SOYBN_USD" , #17 SOYBN_USD
    # "WHEAT_USD" , #18 WHEAT_USD
    # "SUGAR_USD" , #19 SUGAR_USD
    "DE30_EUR" , #20 DE30_EUR
    "UK10YB_GBP" , #21 UK10YB_GBP
    "JP225_USD" , #22 JP225_USD
    "CH20_CHF" , #23 CH20_CHF
    "NL25_EUR" , #24 NL25_EUR
    "XAG_SGD" , #25 XAG_SGD
    "BCH_USD" , #26 BCH_USD
    "LTC_USD"
  ) %>% unique()

get_all_realised_generic(
  realised_DB_path = "C:/Users/Nikhil Chandra/Documents/trade_data/trade_tracker_realised.db",
  write_or_append = "append",
  account_var = 1,
  algo_start_date = "2026-01-10",
  distinct_assets = assets_to_get_results
)

newest_results <-
  get_realised_trades_from_db_generic(
    realised_DB_path = "C:/Users/Nikhil Chandra/Documents/trade_data/trade_tracker_realised.db",
    table_name = "realised_return"
  ) %>%
  mutate(date_closed = as_datetime(date_closed))

newest_results_for_join <-
  newest_results %>%
  dplyr::select(Asset, date_open, date_closed, realizedPL) %>%
  mutate(
    join_date = floor_date(date_open, "hour") - hours(1)
  )

comparison <-
  actual_wins_losses %>%
  filter(Date >= "2026-01-01") %>%
  dplyr::select(Date, Asset,
                period_return_35_Price_sim = period_return_35_Price,
                trade_start_prices_sim = trade_start_prices,
                trade_end_prices_sim = trade_end_prices) %>%
  mutate(
    join_date =
      floor_date(Date, "hour")
  ) %>%
  left_join(newest_results_for_join)

comparison_found <-
  comparison %>%
  filter(!is.na(realizedPL)) %>%
  mutate(
    mismatch =
      case_when(
        period_return_35_Price_sim > 0 & realizedPL > 0 ~ "Match",
        period_return_35_Price_sim < 0 & realizedPL < 0 ~ "Match",
        TRUE ~ "Mismatch"
      )
  ) %>%
  filter(!is.na(period_return_35_Price_sim))

comparison_found %>%
  filter(mismatch == "Mismatch") %>%
  pull(realizedPL) %>%
  sum()

comparison_found_analysis <-
  comparison_found %>%
  filter(mismatch == "Match") %>%
  mutate(
    # return_diff =
    #        case_when(
    #          realizedPL > 0 &  period_return_35_Price_sim > 0~ realizedPL - period_return_35_Price_sim,
    #          realizedPL <0 &  period_return_35_Price_sim < 0 ~ realizedPL - period_return_35_Price_sim
    #          )
    return_diff = realizedPL - period_return_35_Price_sim
         ) %>%
  mutate(
    mag_diff =
      case_when(
        realizedPL > period_return_35_Price_sim ~ "Real Higher",
        realizedPL < period_return_35_Price_sim ~ "Sim Higher"
      ),

    direction_grouping =
      case_when(
        realizedPL > 0 ~ "Greater than 0",
        realizedPL < 0 ~ "Less than 0"
      )
  )

comparison_found_analysis %>%
  group_by(mag_diff) %>%
  summarise(
    low_01 = quantile(abs(return_diff), 0.01),
    low_25 = quantile(abs(return_diff), 0.25),
    mid = mean(abs(return_diff) ),
    high_25 = quantile( abs(return_diff), 0.75),
    high_01 = quantile( abs(return_diff), 0.99),
  )

comparison_found_analysis %>%
  group_by(direction_grouping) %>%
  summarise(
    low_01 = quantile((return_diff), 0.01),
    low_25 = quantile((return_diff), 0.25),
    mid = mean((return_diff) ),
    high_25 = quantile( (return_diff), 0.75),
    high_01 = quantile( (return_diff), 0.99),
  )
