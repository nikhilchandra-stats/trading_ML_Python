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
  start_date = (today() - days(5)) %>% as.character()
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
  keep( ~ .x %in%   c("XAG_USD", "XAG_EUR", "XAG_CAD", "XAG_AUD", "XAG_GBP", "XAG_JPY", "XAG_SGD", "XAG_CHF",
                      "XAG_NZD",
                      "XAU_USD", "XAU_EUR", "XAU_CAD", "XAU_AUD", "XAU_GBP", "XAU_JPY", "XAU_SGD", "XAU_CHF",
                      "XAU_NZD",
                      "BTC_USD", "LTC_USD", "BCH_USD",
                      "US30_USD", "FR40_EUR", "US2000_USD", "CH20_CHF", "SPX500_USD", "AU200_AUD",
                      "JP225_USD", "JP225Y_JPY", "SG30_SGD", "EU50_EUR", "HK33_HKD",
                      "USB02Y_USD", "USB05Y_USD", "USB30Y_USD", "USB10Y_USD", "UK100_GBP",
                      "AUD_USD", "EUR_USD", "GBP_USD", "USD_CHF", "USD_JPY", "USD_MXN", "USD_SEK", "USD_NOK",
                      "NZD_USD", "USD_CAD", "USD_SGD", "ETH_USD", "XPT_USD", "XPD_USD")
  ) %>%
  unique()

asset_infor <- get_instrument_info()
raw_macro_data <- get_macro_event_data()

#---------------------Data
db_location = "C:/Users/nikhi/Documents/Asset Data/Oanda_Asset_Data_missing_Assets.db"
trade_actual_db <- "C:/Users/nikhi/Documents/trade_data/trade_actuals_addtional_assets.db"
start_date = "2016-01-01"
end_date = today() %>% as.character()

#------------------------------------------------------Test with big LM Prop
load_custom_functions()
stop_value_var = 3
profit_value_var = 6
METALS_CRYPTO <-
  get_All_Metals_USD_Currency(
    db_location = db_location,
    start_date = start_date,
    end_date = today() %>% as.character(),
    time_frame = "H1"
  )

METALS_CRYPTO_long <-METALS_CRYPTO[[1]]
METALS_CRYPTO_short <- METALS_CRYPTO[[2]]

trade_actual_db_con <- connect_db(trade_actual_db)
actual_wins_losses <-
  DBI::dbGetQuery(trade_actual_db_con,
                  "SELECT * FROM trade_actual") %>%
  mutate(
    Date = as_datetime(Date)
  )
DBI::dbDisconnect(trade_actual_db_con)
rm(trade_actual_db_con)

actual_wins_losses <- actual_wins_losses %>%
  rename( asset = Asset,
          dates = Date)

lm_test_prop <- 1
accumulating_data <- list()
available_assets <-
        METALS_CRYPTO_long %>%
        filter(!(Asset %in% c("ETH_USD", "BTC_USD", "LTC_USD")) ) %>%
        pull(Asset) %>%
        unique()

all_results_ts <- list()

NN_sims_db <- "C:/Users/nikhi/Documents/trade_data/BONDS_METALS_INDX_1_sims.db"
NN_sims_db_con <- connect_db(path = NN_sims_db)
safely_generate_NN <- safely(generate_NNs_create_preds, otherwise = NULL)

copula_data_BONDS_METALS_INDEX <-
  create_NN_BONDS_METALS_INDEX_data(
    METALS_CRYPTO = METALS_CRYPTO[[1]] %>% filter(!(Asset %in% c("ETH_USD", "BTC_USD", "LTC_USD")) ),
    raw_macro_data,
    actual_wins_losses = actual_wins_losses,
    lag_days = 1,
    stop_value_var = stop_value_var,
    profit_value_var = profit_value_var,
    use_PCA_vars = FALSE
  )

min_allowable_date <-
  copula_data_BONDS_METALS_INDEX[[1]] %>%
  filter(if_all(everything(), ~ !is.na(.))) %>%
  pull(Date) %>% min()

date_sequence <-
  seq(as_date(min_allowable_date), as_date("2025-06-01"), "week") %>%
  keep(~ as_date(.x) >= (as_date(min_allowable_date) + lubridate::dhours(5000) ) )


redo_db = TRUE
stop_value_var <- stop_value_var
profit_value_var <- profit_value_var

params_to_test <- tibble(
  NN_samples = c(30000, 30000, 30000, 30000 ),
  hidden_layers = c(0,0,0,0),
  ending_thresh = c(0,0,0,0),
  p_value_thresh_for_inputs = c(0.0001, 0.001,0.01, 0.1),
  neuron_adjustment = c(0,0,0,0),
  trade_direction_var = c("Long", "Long", "Long", "Long")
)
gc()

for (j in 1:dim(params_to_test)[1]) {

  NN_samples = params_to_test$NN_samples[j] %>% as.numeric()
  hidden_layers = params_to_test$hidden_layers[j] %>% as.numeric()
  ending_thresh = params_to_test$ending_thresh[j] %>% as.numeric()
  p_value_thresh_for_inputs = params_to_test$p_value_thresh_for_inputs[j] %>% as.numeric()
  neuron_adjustment = params_to_test$neuron_adjustment[j] %>% as.numeric()
  analysis_direction <- params_to_test$trade_direction_var[j] %>% as.character()

  for (k in 177:length(date_sequence)) {

    gc()

    max_test_date <- (date_sequence[k] + dmonths(1)) %>% as_date() %>% as.character()
    accumulating_data <- list()

    for (i in 1:length(available_assets)) {

      check_completion <- safely_generate_NN(
        copula_data_macro = copula_data_BONDS_METALS_INDEX[[1]],
        lm_vars1 = copula_data_BONDS_METALS_INDEX[[2]],
        NN_samples = NN_samples,
        dependant_var_name = available_assets[i],
        NN_path = "C:/Users/nikhi/Documents/trade_data/asset_specific_NNs_H1_1/",
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
            copula_data_macro = copula_data_BONDS_METALS_INDEX[[1]] %>%
              filter(Date <= max_test_date),
            lm_vars1 = copula_data_BONDS_METALS_INDEX[[2]],
            dependant_var_name = available_assets[i],
            NN_path = "C:/Users/nikhi/Documents/trade_data/asset_specific_NNs_H1_1/",
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
      # write_table_sql_lite(.data = all_asset_logit_results,
      #                      table_name = "BONDS_METALS_INDEX",
      #                      conn = NN_sims_db_con)
      redo_db = FALSE
    } else {
      append_table_sql_lite(.data = all_asset_logit_results,
                            table_name = "BONDS_METALS_INDEX",
                            conn = NN_sims_db_con)

    }

    rm(all_asset_logit_results)
    rm(accumulating_data)
    rm(check_completion)

  }

}


all_results_ts_dfr <- DBI::dbGetQuery(conn = NN_sims_db_con,
                                      statement = "SELECT * FROM BONDS_METALS_INDEX")

distinct_params <-
  all_results_ts_dfr %>%
  distinct(
    NN_samples, ending_thresh,
    p_value_thresh_for_inputs,
    neuron_adjustment,
    hidden_layers
  )

all_asset_logit_results_sum <-
  all_results_ts_dfr %>%
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
  # filter(hidden_layers == 3, neuron_adjustment == 0, p_value_thresh_for_inputs == 0.3, ending_thresh == 0.02) %>%
  # group_by(Asset) %>%
  # slice_max(risk_weighted_return_mid, n = 2)
  filter(simulations >= 100, edge > 0, outperformance_count > 0.6, risk_weighted_return_mid > 0.1) %>%
  group_by(Asset) %>%
  slice_max(risk_weighted_return_mid)
# filter(NN_samples == 10000)
