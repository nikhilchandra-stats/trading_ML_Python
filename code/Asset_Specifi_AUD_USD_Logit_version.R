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
  keep( ~ .x %in% c("HK33_HKD", "USD_JPY","SPX500_USD",
                    "AUD_NZD", "GBP_CHF",
                    "EUR_HUF", "EUR_ZAR", "NZD_JPY", "EUR_NZD",
                    "XAU_CAD", "GBP_JPY", "EUR_NOK", "USD_SGD", "EUR_SEK",
                    "DE30_EUR",
                    "AUD_CAD",
                    "XPD_USD",
                    "UK100_GBP",
                    "USD_CHF", "GBP_NZD",
                    "GBP_SGD", "USD_SEK", "EUR_SGD", "XCU_USD", "SUGAR_USD", "CHF_ZAR",
                    "AUD_CHF", "EUR_CHF", "USD_MXN", "GBP_USD", "WTICO_USD", "EUR_JPY", "USD_NOK",
                    "XAU_USD",
                    "USD_CZK", "AUD_SGD", "USD_HUF", "WHEAT_USD",
                    "EUR_USD", "SG30_SGD", "GBP_AUD", "NZD_CAD", "AU200_AUD", "XAG_USD",
                    "XAU_EUR", "EUR_GBP", "USD_CNH", "USD_CAD", "NAS100_USD",
                    "EU50_EUR", "NATGAS_USD", "CAD_JPY", "FR40_EUR", "USD_ZAR", "XAU_GBP",
                    "EUR_AUD", "SOYBN_USD",
                    "US2000_USD",
                    "BCO_USD", "AUD_USD", "NZD_USD", "NZD_CHF", "WHEAT_USD", "AUD_JPY", "AUD_SEK")
  )

asset_infor <- get_instrument_info()
raw_macro_data <- get_macro_event_data()
#---------------------Data
db_location = "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data For EDA.db"
start_date = "2011-01-01"
end_date = today() %>% as.character()

#------------------------------------------------------Test with big LM Prop
load_custom_functions()
stop_value_var = 8
profit_value_var = 12
AUD_USD_NZD_USD_list <-
  get_all_AUD_USD_specific_data(
    db_location = db_location,
    start_date = start_date,
    end_date = today() %>% as.character(),
    time_frame = "H1"
  )

full_ts_trade_db_location = "C:/Users/Nikhil Chandra/Documents/trade_data/full_ts_trades_mapped_AUD_USD.db"
full_ts_trade_db_con <- connect_db(path = full_ts_trade_db_location)
actual_wins_losses <-
  DBI::dbGetQuery(full_ts_trade_db_con,
                  "SELECT * FROM full_ts_trades_mapped") %>%
  mutate(
    dates = as_datetime(dates)
  )
DBI::dbDisconnect(full_ts_trade_db_con)
rm(full_ts_trade_db_con)
lm_test_prop <- 1
accumulating_data <- list()
available_assets <- c("AUD_USD", "NZD_USD", "XCU_USD", "NZD_CHF", "XAG_USD", "XAU_USD")
date_sequence <- seq(as_date("2022-01-01"), as_date("2025-06-01"), "week") %>% sample(size = 10)
all_results_ts <- list()

NN_sims_db <- "C:/Users/Nikhil Chandra/Documents/trade_data/AUD_USD_NZD_XCU_Logit_sims.db"
NN_sims_db_con <- connect_db(path = NN_sims_db)
safely_generate_NN <- safely(generate_NNs_create_preds, otherwise = NULL)

min_allowable_date <-
  copula_data_AUD_USD_NZD[[1]] %>%
  filter(if_all(everything(), ~ !is.na(.))) %>%
  pull(Date) %>% min()

date_sequence <-
  seq(as_date(min_allowable_date), as_date("2025-06-01"), "week") %>%
  keep(~ as_date(.x) >= (as_date(min_allowable_date) + lubridate::dhours(5000) ) ) %>%
  sample(size = 200)

copula_data_AUD_USD_NZD <-
  create_NN_AUD_USD_XCU_NZD_data(
    AUD_USD_NZD_USD = AUD_USD_NZD_USD_list[[1]],
    raw_macro_data = raw_macro_data,
    actual_wins_losses = actual_wins_losses,
    lag_days = 1,
    stop_value_var = stop_value_var,
    profit_value_var = profit_value_var,
    use_PCA_vars = FALSE
  )


redo_db = FALSE
stop_value_var <- stop_value_var
profit_value_var <- profit_value_var

params_to_test <-
  tibble(
    NN_samples = c(30000),
    hidden_layers = c(0),
    ending_thresh = c(0),
    p_value_thresh_for_inputs = c(0.01),
    neuron_adjustment = c(0),
    trade_direction_var = c("Long")
  )

for (j in 1:dim(params_to_test)[1]) {

  NN_samples = params_to_test$NN_samples[j] %>% as.numeric()
  hidden_layers = params_to_test$hidden_layers[j] %>% as.numeric()
  ending_thresh = params_to_test$ending_thresh[j] %>% as.numeric()
  p_value_thresh_for_inputs = params_to_test$p_value_thresh_for_inputs[j] %>% as.numeric()
  neuron_adjustment = params_to_test$neuron_adjustment[j] %>% as.numeric()
  analysis_direction <- params_to_test$trade_direction_var[j] %>% as.character()

  for (k in 1:length(date_sequence)) {

    gc()

    max_test_date <- (date_sequence[k] + dmonths(1)) %>% as_date() %>% as.character()
    accumulating_data <- list()

    for (i in 1:length(available_assets)) {

      check_completion <- safely_generate_NN(
        copula_data_macro = copula_data_AUD_USD_NZD[[1]],
        lm_vars1 = copula_data_AUD_USD_NZD[[2]],
        NN_samples = NN_samples,
        dependant_var_name = available_assets[i],
        NN_path = "C:/Users/Nikhil Chandra/Documents/trade_data/asset_specific_NNs/",
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
            copula_data_macro = copula_data_AUD_USD_NZD[[1]] %>%
              filter(Date <= max_test_date),
            lm_vars1 = copula_data_AUD_USD_NZD[[2]],
            dependant_var_name = available_assets[i],
            NN_path = "C:/Users/Nikhil Chandra/Documents/trade_data/asset_specific_NNs/",
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
      #                      table_name = "AUD_USD_NZD_XCU_NN_sims",
      #                      conn = NN_sims_db_con)
      redo_db = FALSE
    } else {
      append_table_sql_lite(.data = all_asset_logit_results,
                            table_name = "AUD_USD_NZD_XCU_NN_sims",
                            conn = NN_sims_db_con)

    }

    rm(all_asset_logit_results)
    rm(accumulating_data)
    rm(check_completion)

  }

}

all_results_ts_dfr <- DBI::dbGetQuery(conn = NN_sims_db_con,
                                      statement = "SELECT * FROM AUD_USD_NZD_XCU_NN_sims")

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
  filter(simulations >= 10, edge > 0, outperformance_count > 0.51, risk_weighted_return_mid > 0.15) %>%
  group_by(Asset) %>%
  slice_max(risk_weighted_return_mid)
# filter(NN_samples == 10000)


#' get_Logit_trades
#'
#' @param NN_path
#' @param copula_data
#' @param stop_value_var
#' @param profit_value_var
#' @param NN_path_save_path
#'
#' @return
#' @export
#'
#' @examples
get_Logit_trades <- function(
    logit_path_save_path = "C:/Users/Nikhil Chandra/Documents/trade_data/asset_specific_NNs/",
    Logit_sims_db = "C:/Users/Nikhil Chandra/Documents/trade_data/AUD_USD_NZD_XCU_Logit_sims.db",
    copula_data = copula_data_AUD_USD_NZD,
    stop_value_var = 4,
    profit_value_var = 8,
    sim_min = 20,
    edge_min = 0,
    outperformance_count_min = 0.51,
    risk_weighted_return_mid_min =  0.14,
    sim_table = "AUD_USD_NZD_XCU_NN_sims"
) {

  date_filter_for_Logit <-
    as.character(copula_data[[1]]$Date %>% max(na.rm = T) + days(1))

  Logit_sims_db_con <- connect_db(path = Logit_sims_db)
  all_results_ts_dfr <- DBI::dbGetQuery(conn = Logit_sims_db_con,
                                        statement = as.character(glue::glue("SELECT * FROM {sim_table}")) )
  DBI::dbDisconnect(Logit_sims_db_con)
  rm(Logit_sims_db_con)

  all_asset_logit_results_sum <-
    all_results_ts_dfr %>%
    mutate(
      edge = risk_weighted_return - control_risk_return,
      outperformance_count = ifelse(Perc > Perc_control, 1, 0),
      returns_total = Trades*Perc*win_amount - Trades*loss_amount*(1-Perc)
    ) %>%
    group_by(Asset, threshold, NN_samples, ending_thresh, p_value_thresh_for_inputs, neuron_adjustment,
             hidden_layers, trade_col, stop_factor, profit_factor
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
    filter(simulations >= sim_min,
           edge > edge_min,
           outperformance_count > outperformance_count_min,
           risk_weighted_return_mid > risk_weighted_return_mid_min) %>%
    group_by(Asset) %>%
    slice_max(risk_weighted_return_mid, n = 5) %>%
    group_by(Asset) %>%
    slice_max(Trades) %>%
    ungroup()


  gernating_params <-
    all_asset_logit_results_sum %>%
    distinct(Asset, NN_samples, ending_thresh,
             p_value_thresh_for_inputs,
             neuron_adjustment,
             hidden_layers,
             trade_col,
             threshold,
             stop_factor, profit_factor)

  accumulating_trades <- list()

  for (i in 1:dim(gernating_params)[1] ) {

      check_completion <- generate_NNs_create_preds(
        copula_data_macro = copula_data[[1]],
        lm_vars1 = copula_data[[2]],
        NN_samples = gernating_params$NN_samples[i] %>% as.integer(),
        dependant_var_name = gernating_params$Asset[i] %>% as.character(),
        NN_path = logit_path_save_path,
        training_max_date = date_filter_for_Logit,
        lm_train_prop = 1,
        trade_direction_var = gernating_params$trade_col[i] %>% as.character(),
        stop_value_var = gernating_params$stop_factor[i]%>% as.numeric(),
        profit_value_var = gernating_params$profit_factor[i]%>% as.numeric(),
        max_NNs = 1,
        hidden_layers = gernating_params$hidden_layers[i] %>% as.integer(),
        ending_thresh = gernating_params$ending_thresh[i] %>% as.numeric(),
        run_logit_instead = TRUE,
        p_value_thresh_for_inputs = gernating_params$p_value_thresh_for_inputs[i] %>% as.numeric(),
        neuron_adjustment = gernating_params$neuron_adjustment[i] %>% as.numeric(),
        lag_price_col = "Price"
      )


    NN_test_preds <-
      read_NNs_create_preds(
        copula_data_macro = copula_data_AUD_USD_NZD[[1]],
        lm_vars1 = copula_data_AUD_USD_NZD[[2]],
        dependant_var_name = available_assets[i],
        NN_path = logit_path_save_path,
        testing_min_date = as.character(as_date(date_filter_for_Logit) - days(2000)),
        trade_direction_var = analysis_direction,
        NN_index_to_choose = "",
        stop_value_var = gernating_params$stop_factor[i]%>% as.numeric(),
        profit_value_var = gernating_params$profit_factor[i]%>% as.numeric(),
        analysis_threshs = c(0.5,0.6, 0.7, 0.8, 0.9, 0.95, 0.99, 0.999),
        run_logit_instead = TRUE,
        lag_price_col = "Price",
        return_tagged_trades = TRUE
      )

    accumulating_trades[[i]] <-
      NN_test_preds %>%
      slice_max(Date) %>%
      filter(Asset == available_assets[i]) %>%
      mutate(
        pred_min = gernating_params$threshold[i] %>% as.numeric()
      )

  }

  all_trades <-
    accumulating_trades %>%
    map_dfr(bind_rows)

}
