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
            "EUR_AUD", #17
            "EUR_NZD", #18
            "XAG_USD", #19
            "XAG_EUR", #20
            "XAG_AUD", #21
            "XAG_NZD", #22
            "HK33_HKD", #23
            "FR40_EUR", #24
            "BTC_USD", #25
            "XAG_GBP", #26
            "GBP_AUD", #27
            "USD_SEK", #28
            # "ETH_USD", #29
            "USD_SGD" #30
  ),
  couplua_assets =
    list( c("XAU_EUR", "XAG_EUR", "EUR_JPY", "EU50_EUR", "EUR_AUD", "EUR_GBP"), #1
          c("XAU_EUR", "XAG_EUR", "XAU_USD", "UK100_GBP", "EUR_AUD", "EUR_GBP", "SPX500_USD"), #2
          c("US2000_USD", "AU200_AUD", "USB10Y_USD", "UK100_GBP", "XAU_USD", "EU50_EUR"), #3
          c("SPX500_USD", "AU200_AUD", "USB10Y_USD", "UK100_GBP", "XAU_USD", "EU50_EUR"), #4
          c("SPX500_USD", "AU200_AUD", "US2000_USD", "UK100_GBP", "XAU_USD", "EU50_EUR"), #5
          c("EUR_JPY", "XAU_JPY", "XAG_JPY", "GBP_JPY", "XAU_USD", "SPX500_USD"), #6
          c("XCU_USD", "AU200_AUD", "XAU_AUD", "GBP_AUD", "XAU_USD", "EUR_AUD"), #7
          c("GBP_USD", "EUR_USD", "XAU_EUR", "XAU_GBP", "GBP_JPY", "EUR_JPY"), #8
          c("XCU_USD", "SPX500_USD", "XAU_AUD", "GBP_AUD", "XAU_USD", "UK100_GBP"), #9
          c("EUR_USD", "EUR_GBP", "XAU_AUD", "GBP_AUD", "AUD_USD", "XAU_EUR"), #10
          c("NATGAS_USD", "XAG_USD", "BCO_USD", "SPX500_USD", "UK10YB_GBP", "XAU_USD", "UK100_GBP"), #11
          c("GBP_USD", "XAG_GBP", "EU50_EUR", "SPX500_USD", "UK10YB_GBP", "XAU_USD", "XAU_GBP"), #12
          c("GBP_USD", "GBP_CAD", "EUR_USD", "XAU_USD", "XAG_EUR", "XAU_GBP", "XAU_EUR"), #13
          c("GBP_JPY", "GBP_CAD", "GBP_AUD", "GBP_NZD", "XAU_GBP", "XAG_GBP", "UK100_GBP"), #14
          c("GBP_JPY", "GBP_USD", "GBP_AUD", "GBP_NZD", "XAU_GBP", "XAG_GBP", "UK100_GBP"), #15
          c("GBP_USD", "EUR_USD", "XAU_EUR", "XAU_JPY", "USD_JPY", "EUR_AUD", "EUR_GBP"), #16
          c("EUR_NZD", "EUR_USD", "XAU_EUR", "XAU_AUD", "AUD_USD", "EUR_JPY", "EUR_GBP"), #17
          c("EUR_AUD", "EUR_USD", "XAU_EUR", "XAU_AUD", "NZD_USD", "EUR_JPY", "EUR_GBP"), #18
          c("XAG_JPY", "XAG_GBP", "XAG_EUR", "XAG_AUD", "XAU_USD", "EU50_EUR", "SPX500_USD"), #19
          c("XAG_JPY", "XAG_GBP", "XAG_USD", "XAG_AUD", "XAU_USD", "EU50_EUR", "SPX500_USD"), #20
          c("XAG_JPY", "EUR_AUD", "XAG_USD", "XAG_NZD", "XAU_USD", "AUD_USD", "SPX500_USD"), #21
          c("XAG_JPY", "EUR_NZD", "XAG_USD", "XAG_AUD", "XAU_USD", "NZD_USD", "SPX500_USD"), #22
          c("UK100_GBP", "EU50_EUR", "XAG_USD", "AU200_AUD", "XAU_USD", "USB10Y_USD", "SPX500_USD"), #23
          c("UK100_GBP", "EU50_EUR", "XAG_USD", "AU200_AUD", "XAU_USD", "USB10Y_USD", "SPX500_USD"), #24
          c("UK100_GBP", "EU50_EUR", "XAG_USD", "AU200_AUD", "XAU_USD", "USB10Y_USD", "SPX500_USD"), #25
          c("UK100_GBP", "XAU_GBP", "XAG_USD", "XAG_EUR", "XAU_USD", "XAG_AUD", "SPX500_USD"), #26
          c("AUD_USD", "EUR_AUD", "XAG_USD", "XAG_GBP", "XAU_USD", "XAG_AUD", "GBP_JPY"), #27
          c("AUD_USD", "EUR_USD", "GBP_USD", "USD_JPY", "XAU_USD", "USD_CAD", "NZD_USD"), #28
          # c("XAU_USD", "XAG_USD", "SPX500_USD", "USB10Y_USD", "EU50_EUR", "UK100_GBP"), #29
          c("AUD_USD", "EUR_USD", "GBP_USD", "USD_JPY", "XAU_USD", "USD_CAD", "NZD_USD") #30

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
      c("GBP", "USD", "EUR", "AUD", "NZD"), #17
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
      c("GBP", "USD", "EUR", "AUD", "JPY"), #28
      # c("GBP", "USD", "EUR", "AUD", "JPY"), #29
      c("GBP", "USD", "EUR", "AUD", "JPY") #30
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

indicator_mapping_accumulator_long <- list()
indicator_mapping_accumulator_short <- list()
safely_single_asset_Logit_indicator <-
  safely(single_asset_Logit_indicator_2, otherwise = NULL)
model_data_store_path <-
  "C:/Users/nikhi/Documents/trade_data/single_asset_improved_indcator_trades_ts_retest_2.db"
model_data_store_db <-
  connect_db(model_data_store_path)

date_seq_simulations <-
  seq(as_date("2024-06-01"), as_date("2024-08-01"), "month")
c = 20
redo_db = FALSE

for (k in 1:length(date_seq_simulations)) {

  for (j in 4:length(indicator_mapping$Asset) ) {

    pre_train_date_end = date_seq_simulations[k]
    post_train_date_start = date_seq_simulations[k]
    test_date_start = post_train_date_start + months(12)
    test_end_date = today()

    countries_for_int_strength <-
      unlist(indicator_mapping$countries_for_int_strength[j])
    couplua_assets = unlist(indicator_mapping$couplua_assets[j])
    Asset_of_interest = unlist(indicator_mapping$Asset[j])

    long_sim <-
      safely_single_asset_Logit_indicator(
        asset_data = Indices_Metals_Bonds[[1]] %>%
          filter(Date <= test_end_date),
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
        actual_wins_losses = actual_wins_losses %>%
          filter(Date <= test_end_date),
        neuron_adjustment = 1.1,
        hidden_layers_var= 2,
        ending_thresh = 0.02,
        trade_direction = "Long",
        stop_value_var = stop_value_var,
        profit_value_var = profit_value_var,
        period_var = period_var
      ) %>%
      pluck('result')


    # short_sim <-
    #   safely_single_asset_Logit_indicator(
    #     asset_data = Indices_Metals_Bonds[[1]] %>%
    #       filter(Date <= test_end_date),
    #     Asset_of_interest = Asset_of_interest,
    #     All_Daily_Data = All_Daily_Data,
    #     equity_index = equity_index,
    #     gold_index = gold_index,
    #     silver_index = silver_index,
    #     bonds_index = bonds_index,
    #     interest_rates = interest_rates,
    #     cpi_data = cpi_data,
    #     sentiment_index = sentiment_index,
    #     countries_for_int_strength =  countries_for_int_strength,
    #     couplua_assets = couplua_assets,
    #     pre_train_date_end = pre_train_date_end,
    #     post_train_date_start = post_train_date_start,
    #     test_date_start = test_date_start,
    #     actual_wins_losses = actual_wins_losses %>%
    #       filter(Date <= test_end_date),
    #     neuron_adjustment = 1.1,
    #     hidden_layers_var= 2,
    #     ending_thresh = 0.02,
    #     trade_direction = "Short",
    #     stop_value_var = stop_value_var,
    #     profit_value_var = profit_value_var,
    #     period_var = period_var
    #   ) %>%
    #   pluck('result')

    short_sim <- NULL

    complete_sim <-
      list(long_sim,
           short_sim) %>%
      map_dfr(bind_rows) %>%
      mutate(
        pre_train_date_end = pre_train_date_end,
        post_train_date_start = post_train_date_start,
        test_date_start = test_date_start,
        test_end_date = test_end_date,
        sim_index = k
      )

    if(dim(complete_sim)[1] > 0) {
      c = c + 1
      if(redo_db == TRUE & c == 1) {
        # write_table_sql_lite(.data = complete_sim,
        #                      table_name = "single_asset_improved",
        #                      conn = model_data_store_db,
        #                      overwrite_true = TRUE)
        redo_db = FALSE
      }

      if(redo_db == FALSE) {
        append_table_sql_lite(.data = complete_sim,
                              table_name = "single_asset_improved",
                              conn = model_data_store_db)
      }
    }

    rm(short_sim, long_sim)

    gc()
    Sys.sleep(1)
    gc()

  }

}

model_data_store_path <-
  "C:/Users/nikhi/Documents/trade_data/single_asset_improved_indcator_trades_ts_retest_2.db"
model_data_store_db <-
  connect_db(model_data_store_path)

indicator_data <-
  DBI::dbGetQuery(conn = model_data_store_db,
                  statement = "SELECT * FROM single_asset_improved") %>%
  distinct() %>%
  group_by(sim_index, Asset) %>%
  mutate(Date = as_datetime(Date),
         test_date_start = as_date(test_date_start),
         test_end_date = as_date(test_end_date),
         Date_filt = as_date(Date))
DBI::dbDisconnect(model_data_store_db)
rm(model_data_store_db)
gc()

indicator_data <-
  indicator_data %>%
  filter(periods_ahead == period_var, stop_factor == stop_value_var)

current_pred <- 0

results_summary_ts  <-
  indicator_data %>%
  filter(sim_index ==2)  %>%
  ungroup() %>%
  mutate(test_date_start = as_datetime(test_date_start),
         test_end_date = as_datetime(test_end_date) ) %>%
  filter(
    logit_combined_pred > mean_logit_combined_pred + current_pred*sd_logit_combined_pred &
      averaged_pred >  mean_averaged_pred + sd_averaged_pred*current_pred
  ) %>%
  dplyr::select(Asset, test_date_start, test_end_date,sim_index, Date, trade_return_dollar_aud,
                trade_col,
                contains("period_return_")) %>%
  group_by(Asset, sim_index) %>%
  arrange(Date, .by_group = TRUE) %>%
  group_by(Asset, sim_index) %>%
  mutate(
    cum_return = cumsum(trade_return_dollar_aud)
  ) %>%
  ungroup()

results_summary <-
  results_summary_ts%>%
  group_by(Asset, trade_col) %>%
  summarise(trade_return_dollar_aud = sum(trade_return_dollar_aud, na.rm = T))


results_summary_ts  %>%
  filter(Asset %in% c("EUR_GBP", "GBP_CAD", "XAG_GBP", "XAG_EUR", "UK100_GBP", "EU50_EUR")) %>%
  ggplot(aes(x = Date, y = cum_return)) +
  geom_line() +
  facet_wrap(.~ Asset, scales = "free") +
  theme_minimal()

results_summary <-
  results_summary_ts%>%
  filter(trade_col == "Long") %>%
  group_by(trade_col, Date) %>%
  summarise(trade_return_dollar_aud = sum(trade_return_dollar_aud, na.rm = T)) %>%
  ungroup() %>%
  arrange(Date) %>%
  mutate(
    cum_return = cumsum(trade_return_dollar_aud)
  ) %>%
  ungroup() %>%
  mutate(
    hour_24_change = cum_return - lag(cum_return, 24)
  )

results_summary$hour_24_change %>% summary()

results_summary %>%
  ggplot(aes(x = Date, y = cum_return)) +
  geom_line()
