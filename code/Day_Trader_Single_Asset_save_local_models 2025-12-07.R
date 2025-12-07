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
stop_value_var = 2
profit_value_var = 15
period_var = 24

All_Daily_Data <-
  get_DAILY_ALGO_DATA_API_REQUEST()

Indices_Metals_Bonds <- get_Port_Buy_Data(
  db_location = db_location,
  start_date = start_date,
  end_date = today() %>% as.character(),
  time_frame = "H1"
)

Indices_Metals_Bonds[[2]] <- NULL
gc()
rm(missing_assets)
gc()

asset_list_oanda <-
  Indices_Metals_Bonds[[1]] %>%
  distinct(Asset) %>%
  pull(Asset) %>%
  as.character()

#-------------Indicator Inputs

equity_index <-
  get_equity_index(index_data = Indices_Metals_Bonds[[1]])

gold_index <-
  get_Gold_index(index_data = Indices_Metals_Bonds[[1]])

silver_index <-
  get_silver_index(index_data = Indices_Metals_Bonds[[1]])

bonds_index <-
  get_bonds_index(index_data = Indices_Metals_Bonds[[1]])

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
            "USD_SGD" #28
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

      c("UK100_GBP", "EU50_EUR", "XAG_USD", "AU200_AUD",
        "XAU_USD", "USB10Y_USD", "SPX500_USD") %>% unique(), #23

      # "BTC_USD", #24
      c("US2000_USD", "AU200_AUD", "USB10Y_USD", "UK100_GBP", "XAU_USD", "EU50_EUR",
        "HK33_HKD", "FR40_EUR", "WTICO_USD", "USD_JPY", "EUR_USD", "GBP_USD", "AU200_AUD",
        "SG30_SGD", "XAU_EUR", "XAG_EUR", "XAG_GBP", "XAU_GBP", "XAG_USD" ) %>% unique(), #24

      c("UK100_GBP", "XAU_GBP", "XAG_USD", "XAG_EUR", "XAU_USD",
        "XAG_AUD", "SPX500_USD") %>% unique(), #25

      c("AUD_USD", "EUR_AUD", "XAG_USD", "XAG_GBP",
        "XAU_USD", "XAG_AUD", "GBP_JPY") %>% unique(), #26

      c("AUD_USD", "EUR_USD", "GBP_USD", "USD_JPY",
        "XAU_USD", "USD_CAD", "NZD_USD") %>% unique(), #27

      c("AUD_USD", "EUR_USD", "GBP_USD", "USD_JPY",
        "XAU_USD", "USD_CAD", "NZD_USD") %>% unique() #28

    ),
  countries_for_int_strength =
    list(
      c("EUR", "USD"), #1
      c("EUR", "USD"), #2
      c("EUR", "USD", "JPY"), #3
      c("EUR", "USD", "JPY"), #4
      c("EUR", "USD", "JPY"), #5
      c("EUR", "USD", "JPY"), #6
      c("AUD", "USD", "EUR"), #7
      c("GBP", "USD", "EUR", "JPY"), #8
      c("AUD", "USD", "EUR"), #9
      c("AUD", "USD", "EUR"), #10
      c("GBP", "USD", "EUR", "AUD"), #11
      c("GBP", "USD", "EUR", "AUD"), #12
      c("GBP", "USD", "EUR", "AUD"), #13
      c("GBP", "USD", "EUR", "AUD"), #14
      c("GBP", "USD", "EUR", "JPY"), #15
      c("GBP", "USD", "EUR", "AUD"), #16

      c("GBP", "USD", "EUR", "AUD", "JPY"), #17
      c("GBP", "USD", "EUR", "AUD", "JPY"), #18
      c("GBP", "USD", "EUR", "AUD", "JPY"), #19
      c("GBP", "USD", "EUR", "AUD", "JPY"), #20
      c("GBP", "USD", "EUR", "AUD", "JPY"), #21
      c("GBP", "USD", "EUR", "AUD", "JPY"), #22
      c("GBP", "USD", "EUR", "AUD", "JPY"), #23
      c("GBP", "USD", "EUR", "AUD", "JPY"), #24
      c("GBP", "USD", "EUR", "AUD", "JPY"), #25
      c("GBP", "USD", "EUR", "AUD", "JPY"), #26
      c("GBP", "USD", "EUR", "AUD", "JPY"), #27
      c("GBP", "USD", "EUR", "AUD", "JPY") #28
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
      risk_dollar_value = 9,
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

pre_train_date_end = today() - months(12)
post_train_date_start = today() - months(12)
test_date_start = today() - weeks(1)
test_end_date = today() + weeks(1)

for (j in 1:length(indicator_mapping$Asset) ) {

  countries_for_int_strength <-
    unlist(indicator_mapping$countries_for_int_strength[j])
  couplua_assets = unlist(indicator_mapping$couplua_assets[j])
  Asset_of_interest = unlist(indicator_mapping$Asset[j])

  long_sim <-
    single_asset_Logit_run_and_save_models(
      asset_data = Indices_Metals_Bonds[[1]] ,
      Asset_of_interest = Asset_of_interest,
      All_Daily_Data = All_Daily_Data,
      equity_index = equity_index,
      gold_index = gold_index,
      silver_index = silver_index,
      bonds_index = bonds_index,
      interest_rates = interest_rates,
      cpi_data = cpi_data,
      sentiment_index = sentiment_index,
      countries_for_int_strength =  countries_for_int_strength,
      couplua_assets = couplua_assets,
      pre_train_date_end = pre_train_date_end,
      post_train_date_start = post_train_date_start,
      test_date_start = test_date_start,
      actual_wins_losses = actual_wins_losses,
      neuron_adjustment = 1.1,
      hidden_layers_var= 2,
      ending_thresh = 0.02,
      trade_direction = "Long",
      stop_value_var = stop_value_var,
      profit_value_var = profit_value_var,
      period_var = period_var,
      save_path = "C:/Users/nikhi/Documents/trade_data/single_asset_models_v1/"
    ) %>%
    pluck('result')


  short_sim <-
    single_asset_Logit_run_and_save_models(
      asset_data = Indices_Metals_Bonds[[1]],
      Asset_of_interest = Asset_of_interest,
      All_Daily_Data = All_Daily_Data,
      equity_index = equity_index,
      gold_index = gold_index,
      silver_index = silver_index,
      bonds_index = bonds_index,
      interest_rates = interest_rates,
      cpi_data = cpi_data,
      sentiment_index = sentiment_index,
      countries_for_int_strength =  countries_for_int_strength,
      couplua_assets = couplua_assets,
      pre_train_date_end = pre_train_date_end,
      post_train_date_start = post_train_date_start,
      test_date_start = test_date_start,
      actual_wins_losses = actual_wins_losses,
      neuron_adjustment = 1.1,
      hidden_layers_var= 2,
      ending_thresh = 0.02,
      trade_direction = "Short",
      stop_value_var = stop_value_var,
      profit_value_var = profit_value_var,
      period_var = period_var,
      save_path = "C:/Users/nikhi/Documents/trade_data/single_asset_models_v1/"
    ) %>%
    pluck('result')

}
