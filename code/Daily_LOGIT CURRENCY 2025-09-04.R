helpeR::load_custom_functions()
library(neuralnet)
raw_macro_data <- get_macro_event_data()
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

asset_list_oanda <- get_oanda_symbols() %>%
  keep( ~ .x %in% c("HK33_HKD", "USD_JPY",
                    "BTC_USD",
                    "AUD_NZD", "GBP_CHF",
                    "EUR_HUF", "EUR_ZAR", "NZD_JPY", "EUR_NZD",
                    "USB02Y_USD",
                    "XAU_CAD", "GBP_JPY", "EUR_NOK", "USD_SGD", "EUR_SEK",
                    "DE30_EUR", "AUD_USD", "NZD_USD",
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
                    "USB02Y_USD", "USB05Y_USD", "USB30Y_USD", "USB10Y_USD", "UK100_GBP")
  )

extracted_asset_data_ask1 <-
  read_all_asset_data_intra_day(
    asset_list_oanda = asset_list_oanda,
    save_path_oanda_assets = "C:/Users/Nikhil Chandra/Documents/Asset Data/oanda_data/",
    read_csv_or_API = "API",
    time_frame = "D",
    bid_or_ask = "ask",
    how_far_back = 5000,
    start_date = "2005-01-01"
  )

max_date_for_next_data_get <-
  extracted_asset_data_ask1 %>%
  map_dfr(bind_rows) %>%
  group_by(Asset) %>%
  slice_max(Date) %>%
  ungroup() %>%
  pull(Date) %>%
  min() %>%
  as_date()

extracted_asset_data_ask2 <-
  read_all_asset_data_intra_day(
    asset_list_oanda = asset_list_oanda,
    save_path_oanda_assets = "C:/Users/Nikhil Chandra/Documents/Asset Data/oanda_data/",
    read_csv_or_API = "API",
    time_frame = "D",
    bid_or_ask = "ask",
    how_far_back = 5000,
    start_date = as.character(max_date_for_next_data_get)
  )

extracted_asset_data_ask <-
  extracted_asset_data_ask1 %>%
  map_dfr(bind_rows) %>%
  bind_rows(extracted_asset_data_ask2 %>%
              map_dfr(bind_rows)
  ) %>%
  distinct() %>%
  group_by(Date, Asset) %>%
  mutate(XX= row_number()) %>%
  group_by(Asset, Date) %>%
  slice_min(XX) %>%
  ungroup() %>%
  dplyr::select(-XX) %>%
  distinct()

asset_data_combined_ask <- extracted_asset_data_ask
asset_data_combined_ask <- asset_data_combined_ask %>%
  mutate(Date = as_date(Date)) %>%
  distinct()
asset_data_daily_raw_ask <-asset_data_combined_ask

asset_data_daily_raw_ask <-asset_data_combined_ask
rm(asset_data_combined_ask, extracted_asset_data_ask)
gc()

mean_values_by_asset_for_loop =
  wrangle_asset_data(
    asset_data_daily_raw = asset_data_daily_raw_ask,
    summarise_means = TRUE
  )

load_custom_functions()
stop_value_var = 1
profit_value_var = 2
available_assets <- asset_data_daily_raw_ask %>% filter(str_detect(Asset, "AUD_USD|AUD_NZD|USD_JPY|NZD_USD|USD_CHF|EUR_USD|EUR_AUD|EUR_JPY|GBP_USD|EUR_GBP|GBP_JPY|USD_MXN|USD_NOK|USD_SEK|EUR_SEK|GBP_NZD|USD_ZAR|EUR_ZAR|XAU_|USD_CAD|NZD_CAD|AUD_JPY|GBP_AUD|EUR_NZD|USB05Y_USD|USB10Y_USD")) %>% pull(Asset) %>% unique()
METALS_INDICES <- asset_data_daily_raw_ask %>% filter(str_detect(Asset, "AUD_USD|AUD_NZD|USD_JPY|NZD_USD|USD_CHF|EUR_USD|EUR_AUD|EUR_JPY|GBP_USD|EUR_GBP|GBP_JPY|USD_MXN|USD_NOK|USD_SEK|EUR_SEK|GBP_NZD|USD_ZAR|EUR_ZAR|XAU_|USD_CAD|NZD_CAD|AUD_JPY|GBP_AUD|EUR_NZD|USB05Y_USD|USB10Y_USD"))

actual_wins_losses <- get_ts_trade_actuals_Logit_NN("C:/Users/Nikhil Chandra/Documents/trade_data/full_ts_trades_mapped_Daily_Data.db", data_is_daily = TRUE)
actual_wins_losses <-
  actual_wins_losses %>%
  filter(asset %in% available_assets) %>%
  ungroup() %>%
  filter(trade_col == "Long")

lm_test_prop <- 1
accumulating_data <- list()
all_results_ts <- list()

NN_sims_db <- "C:/Users/Nikhil Chandra/Documents/trade_data/LOGIT_CURRENCY_DAILY.db"
NN_sims_db_con <- connect_db(path = NN_sims_db)
safely_generate_NN <- safely(generate_NNs_create_preds, otherwise = NULL)

metals_indices_Logit_Data <-
  create_NN_CURRENCY_FOCUS_DAILY_QUANT(
    METALS_INDICES = asset_data_daily_raw_ask %>% filter(str_detect(Asset, "AUD_USD|AUD_NZD|USD_JPY|NZD_USD|USD_CHF|EUR_USD|EUR_AUD|EUR_JPY|GBP_USD|EUR_GBP|GBP_JPY|USD_MXN|USD_NOK|USD_SEK|EUR_SEK|GBP_NZD|USD_ZAR|EUR_ZAR|XAU_|USD_CAD|NZD_CAD|AUD_JPY|GBP_AUD|EUR_NZD|USB05Y_USD|USB10Y_USD")),
    raw_macro_data,
    actual_wins_losses = actual_wins_losses,
    lag_days = 1,
    stop_value_var = stop_value_var,
    profit_value_var = profit_value_var
  )

min_allowable_date <-
  (metals_indices_Logit_Data[[1]] %>%
     filter(if_all(everything(), ~ !is.na(.))) %>%
     pull(Date) %>% min()) + lubridate::days(2000)

date_sequence <-
  seq(as_date(min_allowable_date), as_date("2025-06-01"), "week") %>%
  keep(~ as_date(.x) >= (as_date(min_allowable_date) + lubridate::days(30) ) )


redo_db = TRUE
stop_value_var <- stop_value_var
profit_value_var <- profit_value_var

params_to_test <-
  tibble(
    NN_samples = c(3000, 3000, 3000, 3000, 3000),
    hidden_layers = c(0, 0,0,0, 0),
    ending_thresh = c(0,0,0,0, 0),
    # p_value_thresh_for_inputs = c(0.1, 0.01, 0.001, 0.0001, 0.00001),
    p_value_thresh_for_inputs = c(0.00001 ,0.0001 , 0.001, 0.01, 0.1),
    neuron_adjustment = c(0,0,0,0, 0),
    trade_direction_var = c("Long", "Long", "Long", "Long", "Long")
  )


for (j in 3:dim(params_to_test)[1]) {

  NN_samples = params_to_test$NN_samples[j] %>% as.numeric()
  hidden_layers = params_to_test$hidden_layers[j] %>% as.numeric()
  ending_thresh = params_to_test$ending_thresh[j] %>% as.numeric()
  p_value_thresh_for_inputs = params_to_test$p_value_thresh_for_inputs[j] %>% as.numeric()
  neuron_adjustment = params_to_test$neuron_adjustment[j] %>% as.numeric()
  analysis_direction <- params_to_test$trade_direction_var[j] %>% as.character()

  for (k in 42:length(date_sequence)) {

    gc()

    max_test_date <- (date_sequence[k] + dmonths(6)) %>% as_date() %>% as.character()
    accumulating_data <- list()

    available_assets <- available_assets %>%
      keep(~ !str_detect(.x, "XAG|XAU")) %>%
      unlist() %>%
      unique()

    for (i in 1:length(available_assets)) {

      check_completion <- safely_generate_NN(
        copula_data_macro = metals_indices_Logit_Data[[1]],
        lm_vars1 = metals_indices_Logit_Data[[2]],
        NN_samples = NN_samples,
        dependant_var_name = available_assets[i],
        NN_path = "C:/Users/Nikhil Chandra/Documents/trade_data/asset_specific_NN_Daily_Quant_4/",
        training_max_date = date_sequence[k],
        lm_train_prop = 1,
        trade_direction_var = analysis_direction,
        stop_value_var = stop_value_var,
        profit_value_var = profit_value_var,
        max_NNs = 1,
        hidden_layers = hidden_layers,
        ending_thresh = ending_thresh,
        run_logit_instead = TRUE,
        p_value_thresh_for_inputs = p_value_thresh_for_inputs,
        neuron_adjustment = neuron_adjustment,
        lag_price_col = "Price"
      ) %>%
        pluck('result')

      gc()

      if(!is.null(check_completion)) {
        rm(check_completion)
        gc()
        message("Made it to Prediction, NN generated Success")
        NN_test_preds <-
          read_NNs_create_preds(
            copula_data_macro = metals_indices_Logit_Data[[1]] %>%
              filter(Date <= max_test_date),
            lm_vars1 = metals_indices_Logit_Data[[2]],
            dependant_var_name = available_assets[i],
            NN_path = "C:/Users/Nikhil Chandra/Documents/trade_data/asset_specific_NN_Daily_Quant_4/",
            testing_min_date = (as_date(date_sequence[k]) + days(1)) %>% as.character(),
            trade_direction_var = analysis_direction,
            NN_index_to_choose = "",
            stop_value_var = stop_value_var,
            profit_value_var = profit_value_var,
            analysis_threshs = c(0.5,0.6, 0.7, 0.8, 0.9, 0.95, 0.99, 0.999),
            run_logit_instead = TRUE,
            lag_price_col = "Price",
            return_tagged_trades = FALSE
          )

        accumulating_data[[i]] <-
          NN_test_preds %>%
          mutate(
            NN_samples = NN_samples,
            hidden_layers = hidden_layers,
            ending_thresh = ending_thresh,
            p_value_thresh_for_inputs = p_value_thresh_for_inputs,
            neuron_adjustment = neuron_adjustment
          )
      }

    }

    all_asset_logit_results <-
      accumulating_data %>%
      map_dfr(bind_rows) %>%
      mutate(
        sim_date = date_sequence[k],
        max_test_date = max_test_date,
        profit_factor = profit_value_var,
        stop_factor = stop_value_var
      )

    all_results_ts[[k]] <- all_asset_logit_results

    if(redo_db == TRUE) {
      write_table_sql_lite(.data = all_asset_logit_results,
                           table_name = "CURRENCY",
                           conn = NN_sims_db_con)
      redo_db = FALSE
    } else {
      append_table_sql_lite(.data = all_asset_logit_results,
                            table_name = "CURRENCY",
                            conn = NN_sims_db_con)

    }

    rm(all_asset_logit_results)
    rm(accumulating_data)
    rm(check_completion)

  }

}

all_results_ts_dfr <- DBI::dbGetQuery(conn = NN_sims_db_con,
                                      statement = "SELECT * FROM CURRENCY")

distinct_params <-
  all_results_ts_dfr %>%
  group_by(
    NN_samples, ending_thresh,
    p_value_thresh_for_inputs,
    neuron_adjustment,
    hidden_layers
  ) %>%
  summarise(XX = n())

all_asset_logit_results_sum <-
  all_results_ts_dfr %>%
  # filter(p_value_thresh_for_inputs != 1e-05) %>%
  mutate(
    edge = risk_weighted_return - control_risk_return,
    outperformance_count = ifelse(Perc > Perc_control, 1, 0),
    returns_total = Trades*Perc*win_amount - Trades*loss_amount*(1-Perc)
  ) %>%
  group_by(Asset, threshold, NN_samples, ending_thresh, p_value_thresh_for_inputs, neuron_adjustment,
           hidden_layers, trade_col, stop_factor, profit_factor
           # win_amount, loss_amount
  ) %>%
  summarise(
    win_amount = mean(win_amount, na.rm = T),
    loss_amount = mean(loss_amount, na.rm = T),
    Trades = mean(Trades, na.rm = T),
    edge = mean(edge, na.rm = T),
    Perc = mean(Perc, na.rm = T),
    Median_Actual_Return = median(returns_total, na.rm = T),
    risk_weighted_return_low = quantile(risk_weighted_return, 0.25 ,na.rm = T),
    risk_weighted_return_mid = median(risk_weighted_return, na.rm = T),
    risk_weighted_return_high = quantile(risk_weighted_return, 0.75 ,na.rm = T),
    control_trades = mean(Total_control, na.rm = T),
    control_risk_return_mid = median(control_risk_return, na.rm = T),
    simulations = n(),
    outperformance_count = sum(outperformance_count)
  ) %>%
  mutate(
    outperformance_perc = outperformance_count/simulations
  ) %>%
  filter(risk_weighted_return_mid > 0.1, edge > 0, simulations > 70, outperformance_perc > 0.55) %>%
  group_by(Asset) %>%
  # slice_max(risk_weighted_return_mid) %>%
  group_by(Asset) %>%
  slice_max(Trades)
# filter(hidden_layers == 3, neuron_adjustment == 0, p_value_thresh_for_inputs == 0.3, ending_thresh == 0.02) %>%
# group_by(Asset) %>%
# slice_max(risk_weighted_return_mid, n = 2)
# filter(simulations >= 100, edge > 0.1, outperformance_perc > 0.55, risk_weighted_return_mid > 0.08) %>%
group_by(Asset) %>%
  slice_max(risk_weighted_return_mid)
# filter(NN_samples == 10000)
