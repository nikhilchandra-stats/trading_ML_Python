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
  c("XAG_USD", "XAG_EUR", "XAG_CAD", "XAG_AUD", "XAG_GBP", "XAG_JPY", "XAG_SGD", "XAG_CHF",
    "XAG_NZD",
    "XAU_USD", "XAU_EUR", "XAU_CAD", "XAU_AUD", "XAU_GBP", "XAU_JPY", "XAU_SGD", "XAU_CHF",
    "XAU_NZD",
    "BTC_USD", "LTC_USD", "BCH_USD",
    "US30_USD", "FR40_EUR", "US2000_USD", "CH20_CHF", "SPX500_USD", "AU200_AUD",
    "JP225_USD", "JP225Y_JPY", "SG30_SGD", "EU50_EUR", "HK33_HKD",
    "USB02Y_USD", "USB05Y_USD", "USB30Y_USD", "USB10Y_USD", "UK100_GBP",
    "AUD_USD", "EUR_USD", "GBP_USD", "USD_CHF", "USD_JPY", "USD_MXN", "USD_SEK", "USD_NOK",
    "NZD_USD", "USD_CAD", "USD_SGD", "ETH_USD", "XPT_USD", "XPD_USD",
    "USD_JPY", "GBP_JPY", "USD_SGD", "EUR_SEK",
    "DE30_EUR",
    "USD_CHF", "USD_SEK", "XCU_USD", "SUGAR_USD",
    "USD_MXN", "GBP_USD", "WTICO_USD", "EUR_JPY", "USD_NOK",
    "XAU_USD",
    "USD_CZK",  "WHEAT_USD",
    "EUR_USD", "SG30_SGD", "AU200_AUD", "XAG_USD",
    "EUR_GBP", "USD_CNH", "USD_CAD", "NAS100_USD",
    "EU50_EUR", "NATGAS_USD", "SOYBN_USD",
    "US2000_USD",
    "BCO_USD", "AUD_USD", "NZD_USD", "NZD_CHF", "WHEAT_USD",
    "JP225_USD", "SPX500_USD") %>%
  unique()

asset_infor <- get_instrument_info()
raw_macro_data <- get_macro_event_data()
#---------------------Data
load_custom_functions()
db_location = "C:/Users/nikhi/Documents/Asset Data/Oanda_Asset_Data_Most_Assets_2025-09-13.db"
start_date = "2012-01-01"
end_date = today() %>% as.character()

Indices_Metals_Bonds <- get_Port_Buy_Data(
  db_location = db_location,
  start_date = start_date,
  end_date = today() %>% as.character(),
  time_frame = "H1"
)

#------------------------------------------------------Test with big LM Prop
load_custom_functions()
bin_factor = 0.25
stop_value_var = 2
profit_value_var = 15
period_var = 8
available_assets <- Indices_Metals_Bonds[[1]] %>% distinct(Asset) %>% pull(Asset) %>% unique()
available_assets <-
  available_assets %>%
  keep(~ str_detect(.x, "XAU|XAG|AUD_USD|EUR_USD|USD_JPY|GBP_USD|EUR_GBP")) %>%
  unlist() %>%
  as.character()
full_ts_trade_db_location = "C:/Users/nikhi/Documents/trade_data/full_ts_trades_mapped_period_version.db"
full_ts_trade_db_con <- connect_db(path = full_ts_trade_db_location)
actual_wins_losses <-
  DBI::dbGetQuery(full_ts_trade_db_con,
                  glue::glue("SELECT * FROM full_ts_trades_mapped
                  WHERE trade_col = 'Long' AND stop_factor = {stop_value_var} AND
                        periods_ahead = {period_var} AND Asset <> 'WTICO_USD' AND
                        Asset <> 'BCO_USD' AND Asset <> 'XCU_USD'")
  ) %>%
  mutate(
    Date = as_datetime(Date)
  )

actual_wins_losses <-
  actual_wins_losses %>%
  filter(Asset %in% available_assets) %>%
  filter(trade_col == "Long") %>%
  filter(stop_factor == stop_value_var,
         profit_factor == profit_value_var,
         periods_ahead == period_var)

DBI::dbDisconnect(full_ts_trade_db_con)
rm(full_ts_trade_db_con)
lm_test_prop <- 1
accumulating_data <- list()
all_results_ts <- list()
gc()

NN_sims_db <- "C:/Users/nikhi/Documents/trade_data/Indices_Silver_Logit_sims_Daily_Port.db"
NN_sims_db_con <- connect_db(path = NN_sims_db)
safely_generate_NN <- safely(generate_NNs_create_preds, otherwise = NULL)

Indices_Metals_Bonds[[1]] <-
  Indices_Metals_Bonds[[1]] %>%
  filter(Asset %in% available_assets) %>%
  filter(!(Asset %in% c("WTICO_USD", "BCO_USD", "XCU_USD")))
gc()
Indices_Metals_Bonds[[2]] <- NULL
gc()
Sys.sleep(5)
gc()

copula_data_Indices_Silver <-
  create_LM_Hourly_USD_GBP_EUR_Portfolio_Buy(
    SPX_US2000_XAG = Indices_Metals_Bonds[[1]] ,
    raw_macro_data = raw_macro_data,
    actual_wins_losses = actual_wins_losses,
    lag_days = 1,
    stop_value_var = stop_value_var,
    profit_value_var = profit_value_var,
    use_PCA_vars = FALSE,
    period_var = period_var,
    bin_factor = 0.25
  )

gc()

min_allowable_date <-
  copula_data_Indices_Silver[[1]] %>%
  filter(if_all(everything(), ~ !is.na(.))) %>%
  pull(Date) %>% min()

gc()

date_sequence <-
  seq(as_date(min_allowable_date), as_date("2025-09-25"), "4 weeks") %>%
  keep(~ as_date(.x) >= (as_date(min_allowable_date) + lubridate::dhours(45000) ) )

gc()

redo_db = TRUE
stop_value_var <- stop_value_var
profit_value_var <- profit_value_var

copula_data_Indices_Silver[[1]] <-
  copula_data_Indices_Silver[[1]] %>%
  filter(Date >= min_allowable_date)

gc()

params_to_test <-
  tibble(
    NN_samples = c(45000, 45000, 45000, 45000, 45000),
    hidden_layers = c(0, 0,0,0, 0),
    ending_thresh = c(0,0,0,0, 0),
    p_value_thresh_for_inputs = c(0.1, 0.01, 0.2, 0.0001, 0.5),
    neuron_adjustment = c(0,0,0,0, 0),
    trade_direction_var = c("Long", "Long", "Long", "Long", "Long")
  )

for (j in 1:1) {

  NN_samples = params_to_test$NN_samples[j] %>% as.numeric()
  hidden_layers = params_to_test$hidden_layers[j] %>% as.numeric()
  ending_thresh = params_to_test$ending_thresh[j] %>% as.numeric()
  p_value_thresh_for_inputs = params_to_test$p_value_thresh_for_inputs[j] %>% as.numeric()
  neuron_adjustment = params_to_test$neuron_adjustment[j] %>% as.numeric()
  analysis_direction <- params_to_test$trade_direction_var[j] %>% as.character()

  for (k in 1) {

    gc()

    max_test_date <- (date_sequence[k] + dmonths(36)) %>% as_date() %>% as.character()
    accumulating_data <- list()

    available_assets2 <-
      available_assets %>%
      keep(~ !str_detect(.x, "WTI|BCO|XCU")) %>%
      # unlist( ) %>%
      as.character()

    for (i in 1:length(available_assets2)) {

      if(k == 1) {
        check_completion <- safely_generate_NN(
          copula_data_macro = copula_data_Indices_Silver[[1]],
          lm_vars1 = copula_data_Indices_Silver[[2]],
          NN_samples = NN_samples,
          dependant_var_name = available_assets2[i],
          NN_path = "C:/Users/nikhi/Documents/trade_data/asset_specific_NNs_Portfolio/",
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
      }
      gc()

      if(!is.null(check_completion)) {
        rm(check_completion)
        gc()
        message("Made it to Prediction, NN generated Success")
        NN_test_preds <-
          read_NNs_create_preds_portfolio(
            copula_data_macro = copula_data_Indices_Silver[[1]] %>%
              filter(Date <= max_test_date),
            lm_vars1 = copula_data_Indices_Silver[[2]],
            dependant_var_name = available_assets2[i],
            NN_path = "C:/Users/nikhi/Documents/trade_data/asset_specific_NNs_Portfolio/",
            testing_min_date = (as_date(date_sequence[k]) + days(1)) %>% as.character(),
            trade_direction_var = analysis_direction,
            NN_index_to_choose = "",
            stop_value_var = stop_value_var,
            profit_value_var = profit_value_var,
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
        stop_factor = stop_value_var,
        bin_factor = bin_factor
      )

    all_results_ts[[k]] <- all_asset_logit_results

    if(redo_db == TRUE) {
      write_table_sql_lite(.data = all_asset_logit_results,
                           table_name = "Indices_Silver_Logit_sims",
                           conn = NN_sims_db_con, overwrite_true = TRUE)
      redo_db = FALSE
    } else {
      append_table_sql_lite(.data = all_asset_logit_results,
                            table_name = "Indices_Silver_Logit_sims",
                            conn = NN_sims_db_con)

    }

    rm(all_asset_logit_results)
    rm(accumulating_data)
    rm(check_completion)

  }

}

all_results_ts_dfr <- DBI::dbGetQuery(conn = NN_sims_db_con,
                                      statement = "SELECT * FROM Indices_Silver_Logit_sims") %>%
  filter(!str_detect(Asset, "XAU"))

acumulating_summary <- list()
c = 0
for (i in c(0, 0.5,0.6,0.7,0.8,0.9)) {

  #
  c = c + 1
  acumulating_summary[[c]] <-
    all_results_ts_dfr %>%
    mutate(Date = as_datetime(Date)) %>%
    mutate(
      Returns =
        case_when(
          pred >= i  ~ trade_returns_AUD,
          TRUE ~ 0
        )
    ) %>%
    mutate(pred_thresh = as.character(i) ) %>%
    group_by(Date, pred_thresh) %>%
    summarise(Returns = sum(Returns, na.rm = T)) %>%
    ungroup() %>%
    arrange(Date) %>%
    mutate(
      Cumulative_Return = cumsum(Returns)
    )

}

all_results_ts_dfr_sum <-
  acumulating_summary %>%
  map_dfr(bind_rows)


all_results_ts_dfr_sum %>%
  ggplot(aes(x = Date, y = Cumulative_Return, color = pred_thresh)) +
  geom_line() +
  theme_minimal() +
  facet_wrap(.~pred_thresh, scales = "free") +
  theme(legend.position = "bottom")


all_results_ts_dfr <- DBI::dbGetQuery(conn = NN_sims_db_con,
                                      statement = "SELECT * FROM Indices_Silver_Logit_sims") %>%
  filter(!str_detect(Asset, "XAU"))

acumulating_summary <- list()
c = 0
for (i in c(0, 0.5,0.6,0.7,0.8,0.9)) {

  #
  c = c + 1
  acumulating_summary[[c]] <-
    all_results_ts_dfr %>%
    mutate(Date = as_datetime(Date)) %>%
    mutate(
      Returns =
        case_when(
          pred >= i  ~ trade_returns_AUD,
          TRUE ~ 0
        )
    ) %>%
    mutate(pred_thresh = as.character(i) ) %>%
    group_by(Date, pred_thresh, Asset) %>%
    summarise(Returns = sum(Returns, na.rm = T)) %>%
    ungroup() %>%
    group_by(Asset) %>%
    arrange(Date, .by_group = TRUE) %>%
    mutate(
      Cumulative_Return = cumsum(Returns)
    ) %>%
    group_by(Asset) %>%
    summarise(
      Mean_Returns = mean(Returns, na.rm = T),
      Returns_75 = quantile(Returns, 0.75, na.rm = T),
      Returns_25 = quantile(Returns, 0.25, na.rm = T),
      Final_returns = sum(Returns, na.rm = T)
    )

}

acumulating_summary_sum <-
  acumulating_summary %>%
  map_dfr(bind_rows)
