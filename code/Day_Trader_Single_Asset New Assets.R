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
start_date = "2015-01-01"
end_date = today() %>% as.character()

bin_factor = NULL
stop_value_var = 1
profit_value_var = 2
period_var = 3
full_ts_trade_db_location = "C:/Users/Nikhil Chandra/Documents/trade_data/full_ts_trades_mapped_many_assets.db"
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

Indices_Metals_Bonds <- get_Port_Buy_Data_2_complete(
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
  Asset = c("EUR_CHF", #1 EUR_CHF
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
            "XPT_USD" ,
            "NATGAS_USD" ,
            "SG30_SGD" ,
            "SOYBN_USD" ,
            "WHEAT_USD" ,
            "SUGAR_USD" ,
            "DE30_EUR" ,
            "UK10YB_GBP" ,
            "JP225_USD" ,
            "CH20_CHF" ,
            "NL25_EUR" ,
            "XAG_SGD" ,
            "BCH_USD" ,
            "LTC_USD" ),
  couplua_assets =
    list(
      c("EUR_SEK", "DE30_EUR", "XAG_CHF", "EUR_USD", "EU50_EUR", "XAG_EUR", "XAU_EUR",
            "EUR_AUD", "EUR_JPY", "FR40_EUR", "GBP_CHF", "NZD_CHF", "CH20_CHF", "XAU_USD"), #1 EUR_CHF

      c("EUR_CHF", "DE30_EUR", "NL25_EUR", "EUR_USD", "EU50_EUR", "XAG_EUR", "XAU_EUR",
        "EUR_AUD", "EUR_JPY", "FR40_EUR", "XAU_USD"), #2 EUR_SEK

      c("GBP_JPY", "GBP_NZD", "UK10YB_GBP", "GBP_USD", "GBP_AUD", "XAG_GBP", "XAU_GBP",
        "UK100_GBP", "EUR_JPY", "FR40_EUR", "EUR_USD",  "EUR_CHF", "NZD_CHF", "CH20_CHF",
        "XAU_USD"), #3 GBP_CHF

      c("GBP_CHF", "GBP_NZD", "UK10YB_GBP", "GBP_USD", "GBP_AUD", "XAG_GBP", "XAU_GBP",
        "UK100_GBP", "XAG_JPY", "USD_JPY", "EUR_JPY", "XAU_JPY", "XAU_USD"), #4 GBP_JPY

      c("USD_NOK", "EUR_USD", "USD_JPY", "AUD_USD", "XAG_USD", "XAU_USD", "GBP_USD",
        "USD_CAD", "USD_SEK", "NZD_USD", "EUR_SEK"), #5 USD_CZK

      c("USD_NOK", "EUR_USD", "USD_JPY", "AUD_USD", "XAG_USD", "XAU_USD", "GBP_USD",
        "USD_CAD", "USD_SEK", "NZD_USD", "EUR_SEK"), #6 USD_NOK

      c("XAG_CHF", "XAG_JPY", "XAG_SGD", "XAG_USD", "XAG_EUR", "XAG_GBP", "XAG_NZD",
        "XAG_AUD", "XAU_USD", "XAU_EUR", "XAU_GBP", "XAU_SGD", "XAU_CAD", "XAU_NZD", "XAU_AUD"), #7 XAG_CAD

      c("XAG_CAD", "XAG_JPY", "XAG_SGD", "XAG_USD", "XAG_EUR", "XAG_GBP", "XAG_NZD",
        "XAG_AUD", "XAU_USD", "XAU_EUR", "XAU_GBP", "XAU_SGD", "XAU_CAD", "XAU_NZD", "XAU_AUD"), #8 XAG_CHF

      c("XAG_CAD", "XAG_CHF", "XAG_SGD", "XAG_USD", "XAG_EUR", "XAG_GBP", "XAG_NZD",
        "XAG_AUD", "XAU_USD", "XAU_EUR", "XAU_GBP", "XAU_SGD", "XAU_CAD", "XAU_NZD", "XAU_AUD"), #9 XAG_JPY

      c("GBP_CHF", "GBP_JPY", "UK10YB_GBP", "GBP_USD", "GBP_AUD", "XAG_GBP", "XAU_GBP",
        "UK100_GBP", "NZD_CHF", "NZD_USD", "XAU_NZD", "XAG_NZD"), #10 GBP_NZD

      c( "GBP_NZD", "NZD_USD", "XAU_NZD", "XAG_NZD", "EUR_CHF", "GBP_CHF", "XAG_CHF",
         "CH20_CHF"), #11 NZD_CHF

      c( "USD_NOK", "EUR_USD", "USD_JPY", "AUD_USD", "XAG_USD", "XAU_USD", "GBP_USD",
         "USD_CAD", "USD_SEK", "NZD_USD"), #12 USD_MXN

      c( "USD_NOK", "EUR_USD", "USD_JPY", "AUD_USD", "XAG_USD", "XAU_USD", "GBP_USD",
         "USD_CAD", "USD_SEK", "NZD_USD", "NATGAS_USD", "XPT_USD"), #13 XPD_USD

    ),
  countries_for_int_strength =
    list(
      c("GBP", "USD", "EUR", "AUD", "JPY"),  #1
      c("GBP", "USD", "EUR", "AUD", "JPY"),  #2
      c("GBP", "USD", "EUR", "AUD", "JPY"),  #3
      c("GBP", "USD", "EUR", "AUD", "JPY"),  #4
      c("GBP", "USD", "EUR", "AUD", "JPY"),  #5
      c("GBP", "USD", "EUR", "AUD", "JPY"),  #6
      c("GBP", "USD", "EUR", "AUD", "JPY"),  #7
      c("GBP", "USD", "EUR", "AUD", "JPY"),  #8
      c("GBP", "USD", "EUR", "AUD", "JPY"),  #9
      c("GBP", "USD", "EUR", "AUD", "JPY"),  #10
      c("GBP", "USD", "EUR", "AUD", "JPY"),  #11
      c("GBP", "USD", "EUR", "AUD", "JPY")  #12
    )
)
indicator_mapping_accumulator_long <- list()
indicator_mapping_accumulator_short <- list()
safely_single_asset_Logit_indicator <-
  safely(single_asset_Logit_indicator, otherwise = NULL)
model_data_store_path <-
  "C:/Users/Nikhil Chandra/Documents/trade_data/single_asset_improved_indcator_trades_ts.db"
model_data_store_db <-
  connect_db(model_data_store_path)

date_seq_simulations <-
  seq(as_date("2019-01-01"), as_date("2024-07-01"), "month")
c = 0
redo_db = FALSE

for (k in 40:length(date_seq_simulations)) {

  # for (k in 19:34) {

  for (j in 1:length(indicator_mapping$Asset) ) {

    # for (j in 1:16 ) {

    pre_train_date_end = date_seq_simulations[k]
    post_train_date_start = date_seq_simulations[k]
    test_date_start = post_train_date_start + months(12)
    test_end_date = test_date_start + months(2)

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


    short_sim <-
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
        trade_direction = "Short",
        stop_value_var = stop_value_var,
        profit_value_var = profit_value_var,
        period_var = period_var
      ) %>%
      pluck('result')

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
  "C:/Users/Nikhil Chandra/Documents/trade_data/single_asset_improved_indcator_trades_ts.db"
model_data_store_db <-
  connect_db(model_data_store_path)

indicator_data <-
  DBI::dbGetQuery(conn = model_data_store_db,
                  statement = "SELECT * FROM single_asset_improved") %>%
  distinct() %>%
  group_by(sim_index, Asset) %>%
  filter(periods_ahead == period_var, stop_factor == stop_value_var) %>%
  mutate(Date = as_datetime(Date),
         test_date_start = as_date(test_date_start),
         test_end_date = as_date(test_end_date),
         Date_filt = as_date(Date))
DBI::dbDisconnect(model_data_store_db)
rm(model_data_store_db)
gc()


asset_optimisation_store_path <-
  "C:/Users/Nikhil Chandra/Documents/trade_data/single_asset_improved_asset_optimisation.db"
asset_optimisation_store_db <-
  connect_db(asset_optimisation_store_path)

pred_thresh <- seq(-2,2, 0.1)

control_results <-
  indicator_data %>%
  ungroup() %>%
  mutate(

    trade_return_dollar_aud =
      case_when(
        str_detect(Asset, "JPY") ~ trade_return_dollar_aud/1000,
        TRUE ~ trade_return_dollar_aud
      )

  ) %>%
  mutate(
    wins =
      case_when(
        trade_return_dollar_aud > 0 ~ 1,
        TRUE ~ 0
      )
  ) %>%
  group_by(Asset, sim_index, trade_col) %>%
  summarise(
    trade_return_dollar_aud = sum(trade_return_dollar_aud, na.rm = T),
    wins = sum(wins, na.rm = T),
    total_trades = n()
  ) %>%
  ungroup() %>%
  mutate(
    Win_Perc = wins/total_trades
  ) %>%
  group_by(Asset, trade_col) %>%
  summarise(
    Mid = mean(trade_return_dollar_aud, na.rm = T),
    lower = quantile(trade_return_dollar_aud, 0.25 ,na.rm = T),
    upper = quantile(trade_return_dollar_aud, 0.75 ,na.rm = T),
    simulations = n_distinct(sim_index),
    pred_thresh = "control",
    Win_Perc_mean = mean(Win_Perc, na.rm = T),
    wins_mean = mean(wins, na.rm = T),
    total_trades_mean = mean(total_trades, na.rm = T)

  )

write_table_sql_lite(.data = control_results,
                     table_name = "single_asset_improved_asset_optimisation",
                     conn = asset_optimisation_store_db,
                     overwrite_true = TRUE)

# DBI::dbDisconnect(asset_optimisation_store_db)
# gc()

for (j in 1:length(pred_thresh)) {

  current_pred <- pred_thresh[j]

  if(current_pred < 0) {
    model_results <-
      indicator_data %>%
      ungroup() %>%
      filter(
        logit_combined_pred <= mean_logit_combined_pred + current_pred*sd_logit_combined_pred &
          averaged_pred <=  mean_averaged_pred + sd_averaged_pred*current_pred
      )
  }

  if(current_pred >= 0) {
    model_results <-
      indicator_data %>%
      ungroup() %>%
      filter(
        logit_combined_pred >= mean_logit_combined_pred + current_pred*sd_logit_combined_pred &
          averaged_pred >=  mean_averaged_pred + sd_averaged_pred*current_pred
      )
  }

  model_results <-
    model_results %>%
    ungroup() %>%
    mutate(

      trade_return_dollar_aud =
        case_when(
          str_detect(Asset, "JPY") ~ trade_return_dollar_aud/1000,
          TRUE ~ trade_return_dollar_aud
        )

    ) %>%
    mutate(
      wins =
        case_when(
          trade_return_dollar_aud > 0 ~ 1,
          TRUE ~ 0
        )
    ) %>%
    group_by(Asset, sim_index, trade_col) %>%
    summarise(
      trade_return_dollar_aud = sum(trade_return_dollar_aud, na.rm = T),
      wins = sum(wins, na.rm = T),
      total_trades = n()
    ) %>%
    ungroup() %>%
    mutate(
      Win_Perc = wins/total_trades
    ) %>%
    group_by(Asset, trade_col) %>%
    summarise(
      Mid = mean(trade_return_dollar_aud, na.rm = T),
      lower = quantile(trade_return_dollar_aud, 0.25 ,na.rm = T),
      upper = quantile(trade_return_dollar_aud, 0.75 ,na.rm = T),
      simulations = n_distinct(sim_index),
      pred_thresh = as.character(current_pred),
      Win_Perc_mean = mean(Win_Perc, na.rm = T),
      wins_mean = mean(wins, na.rm = T),
      total_trades_mean = mean(total_trades, na.rm = T)
    )

  append_table_sql_lite(.data = model_results,
                        table_name = "single_asset_improved_asset_optimisation",
                        conn = asset_optimisation_store_db)

  rm(model_results)

}

asset_optimisation_store_path =
  "C:/Users/Nikhil Chandra/Documents/trade_data/single_asset_improved_asset_optimisation.db"

asset_optimisation_store_db <-
  connect_db(asset_optimisation_store_path)

all_model_results <-
  DBI::dbGetQuery(conn = asset_optimisation_store_db,
                  statement = "SELECT * FROM single_asset_improved_asset_optimisation")
DBI::dbDisconnect(asset_optimisation_store_db)
gc()

best_results <-
  all_model_results %>%
  filter(pred_thresh != "control") %>%
  filter(lower > 0) %>%
  group_by(Asset, trade_col) %>%
  slice_max(Win_Perc_mean, n = 10) %>%
  group_by(Asset, trade_col) %>%
  slice_max(total_trades_mean, n = 1)

best_overall_thresh <-
  all_model_results %>%
  filter(pred_thresh != "control") %>%
  group_by(trade_col, pred_thresh) %>%
  summarise(Mid = median(Mid, na.rm= T),
            lower = median(lower, na.rm= T),
            Win_Perc_mean = median(Win_Perc_mean, na.rm = T),
            wins_mean = sum(wins_mean),
            total_trades_mean = sum(total_trades_mean) ) %>%
  group_by(trade_col) %>%
  slice_max(Win_Perc_mean, n = 10) %>%
  group_by(Asset, trade_col) %>%
  slice_max(total_trades_mean, n = 1)



longs <-
  indicator_data %>%
  filter(trade_col == "Long") %>%
  left_join(
    best_results %>%
      ungroup() %>%
      dplyr::select(Asset, trade_col, pred_thresh) %>%
      mutate(pred_thresh = as.numeric(pred_thresh))
  ) %>%
  filter(
    (logit_combined_pred >= mean_logit_combined_pred + pred_thresh*sd_logit_combined_pred &
       averaged_pred >=  mean_averaged_pred + sd_averaged_pred*pred_thresh & pred_thresh >= 0)|
      (logit_combined_pred < mean_logit_combined_pred + pred_thresh*sd_logit_combined_pred &
         averaged_pred <  mean_averaged_pred + sd_averaged_pred*pred_thresh & pred_thresh < 0)
  )

shorts <-
  indicator_data %>%
  filter(trade_col == "Short") %>%
  left_join(
    best_results %>%
      ungroup() %>%
      dplyr::select(Asset, trade_col, pred_thresh) %>%
      mutate(pred_thresh = as.numeric(pred_thresh))
  ) %>%
  filter(
    (logit_combined_pred >= mean_logit_combined_pred + pred_thresh*sd_logit_combined_pred &
       averaged_pred >=  mean_averaged_pred + sd_averaged_pred*pred_thresh & pred_thresh >= 0)|
      (logit_combined_pred < mean_logit_combined_pred + pred_thresh*sd_logit_combined_pred &
         averaged_pred <  mean_averaged_pred + sd_averaged_pred*pred_thresh & pred_thresh < 0)
  )

all_trades <-
  shorts %>%
  bind_rows(longs) %>%
  mutate(
    trade_return_dollar_aud =
      case_when(
        str_detect(Asset, "JPY") ~ trade_return_dollar_aud/1000,
        TRUE ~ trade_return_dollar_aud
      )
  ) %>%
  mutate(
    trade_end_date = as_datetime(Date, tz = "Australia/Canberra") + dhours(Time_Periods),
    Date = trade_end_date
  ) %>%
  group_by(Date, Asset, trade_col ) %>%
  summarise(trade_return_dollar_aud = mean(trade_return_dollar_aud, na.rm = T)) %>%
  mutate(
    Date = as_datetime(Date, tz = "Australia/Canberra")
  ) %>%
  group_by(Date) %>%
  summarise(trade_return_dollar_aud = sum(trade_return_dollar_aud, na.rm = T)) %>%
  ungroup() %>%
  arrange(Date) %>%
  mutate(
    cumulative_returns = cumsum(trade_return_dollar_aud)
  )

all_trades %>%
  ggplot(aes(x = Date, y = cumulative_returns)) +
  geom_line() +
  theme_minimal()
