helpeR::load_custom_functions()

all_aud_symbols <- get_oanda_symbols() %>%
  keep(~ str_detect(.x, "AUD")|str_detect(.x, "USD_SEK|USD_NOK|USD_HUF|USD_ZAR|USD_CNY|USD_MXN"))
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
    "USB02Y_USD", "USB05Y_USD", "USB30Y_USD", "USB10Y_USD", "UK100_GBP") %>%
  unique()

asset_infor <- get_instrument_info()
raw_macro_data <- get_macro_event_data()
#---------------------Data
load_custom_functions()
db_location = "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data_Most_Assets_2025-09-13.db"
start_date = "2021-01-01"
end_date = today() %>% as.character()

bin_factor = NULL
stop_value_var = 2
profit_value_var = 15
period_var = 48
full_ts_trade_db_location = "C:/Users/Nikhil Chandra/Documents/trade_data/full_ts_trades_mapped_period_version.db"
full_ts_trade_db_con <- connect_db(path = full_ts_trade_db_location)
actual_wins_losses <-
  DBI::dbGetQuery(full_ts_trade_db_con,
                  glue::glue("SELECT * FROM full_ts_trades_mapped
                  WHERE stop_factor = {stop_value_var} AND
                        periods_ahead = {period_var} AND Date >= {start_date}")
  ) %>%
  mutate(
    Date = as_datetime(Date)
  )

actual_wins_losses <-
  actual_wins_losses %>%
  filter(stop_factor == stop_value_var,
         profit_factor == profit_value_var,
         periods_ahead == period_var)

DBI::dbDisconnect(full_ts_trade_db_con)
rm(full_ts_trade_db_con)
gc()

All_Daily_Data <-
  get_DAILY_ALGO_DATA_API_REQUEST()

Indices_Metals_Bonds <- get_Port_Buy_Data(
  db_location = db_location,
  start_date = start_date,
  end_date = today() %>% as.character(),
  time_frame = "H1"
)

missing_assets <- get_Port_Buy_Data_remaining_assets(
  db_location = db_location,
  start_date = start_date,
  end_date = today() %>% as.character(),
  time_frame = "H1"
)

Indices_Metals_Bonds[[1]] <-
  Indices_Metals_Bonds[[1]] %>%
  bind_rows(missing_assets[[1]] %>%
              filter(Asset != "XAG_AUD"))

Indices_Metals_Bonds[[2]] <-
  Indices_Metals_Bonds[[2]] %>%
  bind_rows(missing_assets %>%
              filter(Asset != "XAG_AUD"))

rm(missing_assets)
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
  Asset = c("EUR_USD",
            # "EU50_EUR",
            "SPX500_USD"
            # "US2000_USD",
            # "USB10Y_USD"
            ),
  couplua_assets =
    list( c("XAU_EUR", "XAG_EUR", "EUR_JPY", "EU50_EUR", "EUR_AUD", "EUR_GBP"),
          # c("XAU_EUR", "XAG_EUR", "EUR_JPY", "EUR_USD", "EUR_AUD", "EUR_GBP"),
          c("US2000_USD", "AU200_AUD", "USB10Y_USD", "UK100_GBP", "XAU_USD", "JP225Y_JPY")
          # c("SPX500_USD", "AU200_AUD", "USB10Y_USD", "UK100_GBP", "XAU_USD", "JP225Y_JPY"),
          # c("SPX500_USD", "AU200_AUD", "USB10Y_USD", "UK100_GBP", "XAU_USD", "JP225Y_JPY")
          ),
  countries_for_int_strength =
    list(
      c("EUR", "USD"),
      # c("EUR", "USD"),
      c("EUR", "USD", "JPY")
      # c("EUR", "USD", "JPY"),
      # c("EUR", "USD", "JPY")
    )
  )
indicator_mapping_accumulator_long <- list()
indicator_mapping_accumulator_short <- list()

for (i in 1:length(indicator_mapping$Asset) ) {

  countries_for_int_strength <-
    unlist(indicator_mapping$countries_for_int_strength[i])
  couplua_assets = unlist(indicator_mapping$couplua_assets[i])
  Asset_of_interest = unlist(indicator_mapping$Asset[i])

  indicator_mapping_accumulator_long[[i]] <-
    single_asset_Logit_indicator(
    asset_data = Indices_Metals_Bonds[[1]],
    Asset_of_interest = indicator_mapping$Asset[i],
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
    pre_train_date_end = "2023-01-01",
    post_train_date_start = "2023-02-01",
    test_date_start = "2025-01-01",
    actual_wins_losses = actual_wins_losses,
    neuron_adjustment = 1.1,
    hidden_layers_var= 2,
    ending_thresh = 0.02,
    trade_direction = "Long",
    stop_value_var = stop_value_var,
    profit_value_var = profit_value_var,
    period_var = period_var
  )


  indicator_mapping_accumulator_short[[i]] <-
    single_asset_Logit_indicator(
      asset_data = Indices_Metals_Bonds[[1]],
      Asset_of_interest = indicator_mapping$Asset[i],
      equity_index = equity_index,
      gold_index = gold_index,
      silver_index = silver_index,
      bonds_index = bonds_index,
      interest_rates = interest_rates,
      cpi_data = cpi_data,
      sentiment_index = sentiment_index,
      countries_for_int_strength =  countries_for_int_strength,
      couplua_assets = couplua_assets,
      pre_train_date_end = "2023-01-01",
      post_train_date_start = "2023-01-01",
      test_date_start = "2025-01-01",
      actual_wins_losses = actual_wins_losses,
      neuron_adjustment = 1.1,
      hidden_layers_var= 2,
      ending_thresh = 0.02,
      trade_direction = "Short",
      stop_value_var = stop_value_var,
      profit_value_var = profit_value_var,
      period_var = period_var
    )

}

single_asset_analyser <-
  function(logit_combined_pred,
           pred_thresh = 0.5,
           pred_col = "logit_combined_pred") {

    returned <-
      logit_combined_pred %>%
      filter(!!as.name(pred_col) >= pred_thresh) %>%
      mutate(
        pred_thresh = as.character(pred_thresh)
      ) %>%
      arrange(Date) %>%
      mutate(
        cumulative_returns_aud = cumsum(trade_return_dollar_aud)
      )

    return(returned)

  }

single_asset_analyser_sd_range <-
  function(logit_combined_pred,
           pred_thresh = 0.1,
           pred_col = "technical_indicator_pred_ma_15",
           pred_col_mean = "mean_logit_combined_pred",
           pred_col_sd = "sd_logit_combined_pred") {

    returned <-
      logit_combined_pred %>%
      filter( !!as.name(pred_col) >= (!!as.name(pred_col_mean) + !!as.name(pred_col_sd)*pred_thresh)  ) %>%
      mutate(
        pred_thresh = as.character(pred_thresh)
      ) %>%
      arrange(Date) %>%
      mutate(
        cumulative_returns_aud = cumsum(trade_return_dollar_aud)
      )

    return(returned)

  }


analysis_accumulator <- list()
for (i in 1:length(indicator_mapping_accumulator_long)) {

  analysis_accumulator[[i]] <-
    list(
      -5,0
    ) %>%
    map_dfr(
      ~ single_asset_analyser_sd_range(
        logit_combined_pred = indicator_mapping_accumulator_long[[i]],
        pred_thresh = .x,
        pred_col = "averaged_pred",
        pred_col_mean = "mean_averaged_pred",
        pred_col_sd = "sd_averaged_pred"
      )
    )

}


analysis_dfr <-
  analysis_accumulator %>%
  map_dfr(bind_rows)

analysis_dfr %>%
  ggplot(aes(x = Date, y = cumulative_returns_aud, color = pred_thresh)) +
  geom_line() +
  theme_minimal() +
  facet_wrap(Asset~pred_thresh, scales = "free") +
  theme(legend.position = "bottom")


