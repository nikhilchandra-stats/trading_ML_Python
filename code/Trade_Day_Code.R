helpeR::load_custom_functions()
one_drive_path <- helperfunctions35South::create_one_drive_path(
  path_extension = "raw data")
#--------------------------------------Trade Day Volatility
asset_infor <- get_instrument_info()
asset_list_oanda <- get_oanda_symbols() %>%
  keep( ~ .x %in% c("HK33_HKD", "USD_JPY", "BTC_USD", "AUD_NZD", "GBP_CHF",
                    "EUR_HUF", "EUR_ZAR", "NZD_JPY", "EUR_NZD", "USB02Y_USD",
                    "XAU_CAD", "GBP_JPY", "EUR_NOK", "USD_SGD", "EUR_SEK", "DE30_EUR",
                    "AUD_CAD", "UK10YB_GBP", "XPD_USD", "UK100_GBP", "USD_CHF", "GBP_NZD",
                    "GBP_SGD", "USD_SEK", "EUR_SGD", "XCU_USD", "SUGAR_USD", "CHF_ZAR",
                    "AUD_CHF", "EUR_CHF", "USD_MXN", "GBP_USD", "WTICO_USD", "EUR_JPY", "USD_NOK",
                    "XAU_USD", "DE10YB_EUR", "USD_CZK", "AUD_SGD", "USD_HUF", "WHEAT_USD",
                    "EUR_USD", "SG30_SGD", "GBP_AUD", "NZD_CAD", "AU200_AUD", "XAG_USD",
                    "XAU_EUR", "EUR_GBP", "USD_CNH", "USD_CAD", "NAS100_USD", "USB10Y_USD",
                    "EU50_EUR", "NATGAS_USD", "CAD_JPY", "FR40_EUR", "USD_ZAR", "XAU_GBP",
                    "CH20_CHF", "ESPIX_EUR", "XPT_USD", "EUR_AUD", "SOYBN_USD", "US2000_USD",
                    "BCO_USD", "AUD_USD", "NZD_USD", "NZD_CHF", "WHEAT_USD", "AUD_JPY", "AUD_SEK")
  )

asset_infor <- get_instrument_info()

extracted_asset_data <-
  read_all_asset_data(
    asset_list_oanda = asset_list_oanda,
    save_path_oanda_assets = "C:/Users/Nikhil Chandra/Documents/Asset Data/oanda_data/",
    read_csv_or_API = "API"
  )

asset_data_combined <- extracted_asset_data %>% map_dfr(bind_rows)
asset_data_daily_raw <- extracted_asset_data %>% map_dfr(bind_rows)
asset_data_daily <- asset_data_daily_raw
aud_usd_today <- asset_data_daily_raw %>% filter(str_detect(Asset, "AUD")) %>%
  slice_max(Date)  %>%
  dplyr::select(Price, Asset) %>%
  mutate(ending_value = str_extract(Asset, "_[A-Z][A-Z][A-Z]") %>% str_remove_all("_")) %>%
  mutate(
    adjusted_conversion =
      case_when(
        ending_value != "AUD" ~ 1/Price,
        TRUE ~ Price
      )
  )

asset_data_daily_raw_week <- extracted_asset_data  %>%
  map_dfr(~ .x) %>%
  mutate(
    week_date = Date,
    week_start_price = Price,
    weekly_high = High,
    weekly_low = Low
  ) %>%
  dplyr::select(-Date, Price, -Low, -High, -Open)

mean_values_by_asset_for_loop =
  wrangle_asset_data(
    asset_data_daily_raw = asset_data_daily_raw,
    summarise_means = TRUE
  )

trade_params <- read_csv("data/volatility_trade_params.csv")

trades_today <-
  get_volatility_trades(
  asset_data_daily_raw_week = asset_data_daily_raw_week,
  asset_data_daily_raw = asset_data_daily_raw,
  mean_values_by_asset_for_loop = mean_values_by_asset_for_loop,
  sd_facs = 0,
  stop_fac = 0.56,
  prof_fac =1.4,
  risk_dollar_value = 30
)

trades_today_to_take <- trades_today %>%
  ungroup() %>%
  dplyr::select(-pipLocation) %>%
  left_join(asset_infor, by = c("Asset" = "name") ) %>%
  mutate(
    volume_required = round(volume_required, digits = 1),
    minimumTradeSize = as.numeric(minimumTradeSize),
    marginRate = as.numeric(marginRate),
    Margain_Required = round(marginRate*(volume_required/minimumTradeSize) )
  ) %>%
  filter(volume_required > 0)

