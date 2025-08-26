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

#' mean_values_by_asset_for_loop
#'
#' @param tagged_trades
#' @param stop_factor
#' @param profit_factor
#' @param raw_asset_data
#'
#' @return
#' @export
#'
#' @examples
get_trade_results_ts_aud <- function(
    tagged_trades = SPX_XAG_US2000_Long_trades %>% slice_sample(n = 1000),
    stop_factor = 10,
    profit_factor = 15,
    raw_asset_data = SPX_US2000_XAG,
    risk_dollar_value = 10,
    currency_conversion = currency_conversion
) {

  mean_values_by_asset_for_loop <-
    wrangle_asset_data(
      asset_data_daily_raw = raw_asset_data,
      summarise_means = TRUE
    )

  long_bayes_loop_analysis<-
    generic_trade_finder_loop(
      tagged_trades = tagged_trades ,
      asset_data_daily_raw = raw_asset_data,
      stop_factor = stop_factor,
      profit_factor =profit_factor,
      trade_col = "trade_col",
      date_col = "Date",
      start_price_col = "Price",
      mean_values_by_asset = mean_values_by_asset_for_loop
    )

  trades_with_aud_ts <-
    long_bayes_loop_analysis %>%
    rename(Asset = asset,
           Date = dates) %>%
    mutate(
      stop_factor = stop_factor,
      profit_factor = profit_factor
    ) %>%
    left_join(
      raw_asset_data %>% dplyr::select(Date, Asset, Price , Low, High, Open)
    ) %>%
    get_stops_profs_volume_trades(
      mean_values_by_asset = mean_values_by_asset_for_loop,
      trade_col = "trade_col",
      currency_conversion = currency_conversion,
      risk_dollar_value = risk_dollar_value,
      stop_factor = stop_factor,
      profit_factor = profit_factor,
      asset_col = "Asset",
      stop_col = "stop_value",
      profit_col = "profit_value",
      price_col = "Price",
      trade_return_col = "trade_returns"
    ) %>%
    mutate(
      trade_returns =
        case_when(
          trade_col == "Long" & trade_start_prices > trade_end_prices ~ maximum_win,
          trade_col == "Long" & trade_start_prices <= trade_end_prices ~ minimal_loss,
          trade_col == "Short" & trade_start_prices < trade_end_prices ~ maximum_win,
          trade_col == "Short" & trade_start_prices >= trade_end_prices ~ minimal_loss
        )
    ) %>%
    dplyr::select(Date, Asset, trade_returns, stop_factor, trade_col,
                  profit_factor, volume_required, estimated_margin,
                  trade_start_prices, trade_end_prices, minimal_loss, maximum_win,
                  starting_stop_value, starting_profit_value)

  return(trades_with_aud_ts)

  }

#---------------------Data
db_location <- "C:/Users/nikhi/Documents/Asset Data/Oanda_Asset_Data EDA.db"
trade_actual_db <- "C:/Users/nikhi/Documents/trade_data/trade_actuals.db"
start_date = "2016-01-01"
end_date = today() %>% as.character()

update_local_db_file(
  db_location = db_location,
  time_frame = "M15",
  bid_or_ask = "ask",
  how_far_back = 20
)

update_local_db_file(
  db_location = db_location,
  time_frame = "M15",
  bid_or_ask = "bid",
  how_far_back = 20
)

SPX_US2000_XAG_ALL <- get_SPX_US2000_XAG_XAU(
  db_location = db_location,
  start_date = "2016-01-01",
  end_date = today() %>% as.character()
)
SPX_US2000_XAG <-SPX_US2000_XAG_ALL[[1]]
SPX_US2000_XAG_short <- SPX_US2000_XAG_ALL[[2]]

SPX_XAG_US2000_Long_trades <-
  SPX_US2000_XAG %>%
  mutate(
    trade_col = "Long"
  )

SPX_XAG_US2000_Long_Data <-
  get_trade_results_ts_aud(
    tagged_trades = SPX_XAG_US2000_Long_trades,
    stop_factor = 10,
    profit_factor = 15,
    raw_asset_data = SPX_US2000_XAG,
    risk_dollar_value = 5,
    currency_conversion = currency_conversion
  )

trade_actual_db_con <- connect_db(trade_actual_db)
write_table_sql_lite(
  SPX_XAG_US2000_Long_Data,
  table_name =  "trade_actual",
  conn = trade_actual_db_con,
  overwrite_true = TRUE)
DBI::dbDisconnect(trade_actual_db_con)
rm(trade_actual_db_con)


SPX_XAG_US2000_Short_trades <-
  SPX_US2000_XAG %>%
  mutate(
    trade_col = "Short"
  )

SPX_XAG_US2000_Short_Data <-
  get_trade_results_ts_aud(
    tagged_trades = SPX_XAG_US2000_Short_trades,
    stop_factor = 10,
    profit_factor = 15,
    raw_asset_data = SPX_US2000_XAG,
    risk_dollar_value = 5,
    currency_conversion = currency_conversion
  )

trade_actual_db <- "C:/Users/nikhi/Documents/trade_data/trade_actuals.db"
trade_actual_db_con <- connect_db(trade_actual_db)
append_table_sql_lite(
  SPX_XAG_US2000_Short_Data,
  table_name =  "trade_actual",
  conn = trade_actual_db_con)
DBI::dbDisconnect(trade_actual_db_con)
rm(trade_actual_db_con)

#-------------------------------------------------------------
trade_actual_db <- "C:/Users/nikhi/Documents/trade_data/trade_actuals.db"
trade_actual_db_con <- connect_db(trade_actual_db)
actual_wins_losses <-
  DBI::dbGetQuery(trade_actual_db_con,
                  "SELECT * FROM trade_actual") %>%
  mutate(
    Date = as_datetime(Date)
  )
DBI::dbDisconnect(trade_actual_db_con)
rm(trade_actual_db_con)


load_custom_functions()
db_location <- "C:/Users/nikhi/Documents/Asset Data/Oanda_Asset_Data EDA.db"
trade_actual_db <- "C:/Users/nikhi/Documents/trade_data/trade_actuals.db"
start_date = "2016-01-01"
end_date = today() %>% as.character()
SPX_US2000_XAG_ALL <- get_SPX_US2000_XAG_XAU(
  db_location = db_location,
  start_date = "2016-01-01",
  end_date = today() %>% as.character()
)
SPX_US2000_XAG <-SPX_US2000_XAG_ALL[[1]]
SPX_US2000_XAG_short <- SPX_US2000_XAG_ALL[[2]]
lm_test_prop <- 0.9999
accumulating_data <- list()
available_assets <- c("SPX500_USD", "EU50_EUR", "US2000_USD", "AU200_AUD", "XAG_USD", "XAU_USD")
date_sequence <- seq(as_date("2018-01-01"), as_date("2025-06-01"), "week") %>%
  sample(200) %>%
  keep(
    ~ as_date(.x) >= (as_date("2018-01-01") + lubridate::dminutes(15*20000))
  )
all_results_ts <- list()
profit_value_var <- 15
stop_value_var <- 10

NN_sims_db <- "C:/Users/nikhi/Documents/trade_data/INDICES_15M_NN_sims.db"
NN_sims_db_con <- connect_db(path = NN_sims_db)
redo_db = FALSE
safely_generate_NN <- safely(generate_NNs_create_preds, otherwise = NULL)
copula_data_Indices <-
  create_NN_Indices_data(
    SPX_US2000_XAG = SPX_US2000_XAG_ALL[[1]],
    raw_macro_data = raw_macro_data,
    actual_wins_losses = actual_wins_losses,
    lag_days = 1,
    stop_value_var = stop_value_var,
    profit_value_var = profit_value_var,
    use_PCA_vars = FALSE,
    rolling_period_index_PCA_cor = 15
  )

params_to_test <- tibble(
                NN_samples = 10000,
                hidden_layers = 6,
                ending_thresh = 0.02,
                p_value_thresh_for_inputs = 0.35,
                neuron_adjustment = 0,
                trade_direction_var = "Long"
                )

params_to_test <- tibble(
  NN_samples = 5000,
  hidden_layers = 5,
  ending_thresh = 0.01,
  p_value_thresh_for_inputs = 0.001,
  neuron_adjustment = 0.25,
  trade_direction_var = "Long"
)


redo_db = FALSE

for (j in 1:dim(params_to_test)[1]) {

  NN_samples = params_to_test$NN_samples[j] %>% as.numeric()
  hidden_layers = params_to_test$hidden_layers[j] %>% as.numeric()
  ending_thresh = params_to_test$ending_thresh[j] %>% as.numeric()
  p_value_thresh_for_inputs = params_to_test$p_value_thresh_for_inputs[j] %>% as.numeric()
  neuron_adjustment = params_to_test$neuron_adjustment[j] %>% as.numeric()
  analysis_direction <- params_to_test$trade_direction_var[j] %>% as.character()

  for (k in 142:length(date_sequence)) {

    max_test_date <- (date_sequence[k] + dmonths(1)) %>% as_date() %>% as.character()

    accumulating_data <- list()

    for (i in 1:length(available_assets)) {

      check_completion <- generate_NNs_create_preds(
        copula_data_macro = copula_data_Indices[[1]],
        lm_vars1 = copula_data_Indices[[2]],
        NN_samples = NN_samples,
        dependant_var_name = available_assets[i],
        NN_path = "C:/Users/nikhi/Documents/trade_data/asset_specific_NNs/",
        training_max_date = date_sequence[k],
        lm_train_prop = 1,
        trade_direction_var = analysis_direction,
        stop_value_var = stop_value_var,
        profit_value_var = profit_value_var,
        max_NNs = 1,
        hidden_layers = hidden_layers,
        ending_thresh = ending_thresh,
        run_logit_instead = FALSE,
        p_value_thresh_for_inputs = p_value_thresh_for_inputs,
        neuron_adjustment = neuron_adjustment,
        lag_price_col = "Price"
      )

      gc()

      if(!is.null(check_completion)) {
        message("Made it to Prediction, NN generated Success")
        NN_test_preds <-
          read_NNs_create_preds(
            copula_data_macro = copula_data_Indices[[1]] %>% filter(Date <= max_test_date),
            lm_vars1 = copula_data_Indices[[2]],
            dependant_var_name = available_assets[i],
            NN_path = "C:/Users/nikhi/Documents/trade_data/asset_specific_NNs/",
            testing_min_date = (as_date(date_sequence[k]) + days(1)) %>% as.character(),
            trade_direction_var = analysis_direction,
            NN_index_to_choose = "",
            stop_value_var = stop_value_var,
            profit_value_var = profit_value_var,
            analysis_threshs = c(0.5,0.6, 0.7, 0.8, 0.9, 0.95, 0.99, 0.999),
            run_logit_instead = FALSE,
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
        max_test_date = max_test_date
      )

    gc()

    if(redo_db == TRUE) {
      # write_table_sql_lite(.data = all_asset_logit_results,
      #                       table_name = "INDICES_15_NN_sims",
      #                       conn = NN_sims_db_con,
      #                       overwrite_true = TRUE)
      redo_db = FALSE
    } else {
      append_table_sql_lite(.data = all_asset_logit_results,
                            table_name = "INDICES_15_NN_sims",
                            conn = NN_sims_db_con)

    }

    rm(all_asset_logit_results)
    rm(accumulating_data)
    rm(check_completion)

  }

}

all_results_ts_dfr <- DBI::dbGetQuery(conn = NN_sims_db_con,
                                      statement = "SELECT * FROM INDICES_15_NN_sims")

distinct_params <-
  all_results_ts_dfr %>%
  distinct( NN_samples,
            hidden_layers,
            ending_thresh,
            p_value_thresh_for_inputs,
            neuron_adjustment)


all_results_ts_dfr_sum <-
  all_results_ts_dfr %>%
  mutate(Control_Wins = round(Perc_control*Total_control)) %>%
  group_by(Asset, threshold, trade_col,
           NN_samples,
           hidden_layers,
           ending_thresh,
           p_value_thresh_for_inputs,
           neuron_adjustment) %>%
  summarise(Trades = sum(Trades),
            wins_losses = sum(wins_losses),
            win_amount = mean(win_amount),
            loss_amount = mean(loss_amount),
            control_trades = sum(Total_control),
            Control_Wins = sum(Control_Wins)) %>%
  mutate(Perc = wins_losses/Trades,
         Perc_Control = Control_Wins/control_trades,
         risk_weighted_return = Perc*(win_amount/loss_amount) - (1- Perc),
         risk_weighted_return_control = Perc_Control*(win_amount/loss_amount) - (1- Perc_Control)
  ) %>%
  ungroup() %>%
  filter(p_value_thresh_for_inputs == 0.001)

all_asset_logit_results_sum <-
  all_results_ts_dfr %>%
  mutate(
    edge = risk_weighted_return - control_risk_return,
    actual_return = Trades*(Perc)*win_amount - (1-Perc)*loss_amount*Trades,
    Outperformance =
      case_when(
        Perc > Perc_control ~ 1,
        TRUE ~ 0
      )
    # actual_return_control = Trades*(Perc_control)*win_amount - (1-Perc_control)*loss_amount*Trades
  ) %>%
  group_by(Asset, threshold, trade_col,
           NN_samples,
           hidden_layers,
           ending_thresh,
           p_value_thresh_for_inputs,
           neuron_adjustment) %>%
  summarise(
    sim_number = n(),
    Trades = mean(Trades, na.rm = T),
    edge = mean(edge, na.rm = T),
    Perc = mean(Perc, na.rm = T),
    actual_return = median(actual_return, na.rm = T),
    risk_weighted_return_low = quantile(risk_weighted_return, 0.25 ,na.rm = T),
    risk_weighted_return_mid = median(risk_weighted_return, na.rm = T),
    risk_weighted_return_high = quantile(risk_weighted_return, 0.75 ,na.rm = T),
    low_to_high_ratio = abs(risk_weighted_return_high/risk_weighted_return_low),
    Outperformance = sum(Outperformance, na.rm = T),
    total_sims = n(),
    control_trades = mean(Total_control, na.rm = T),
    control_risk_return = mean(control_risk_return, na.rm = T)
  ) %>%
  ungroup() %>%
  mutate(
    Outperformance_Perc = Outperformance/total_sims
  ) %>%
  # filter(hidden_layers == 5, p_value_thresh_for_inputs  ==  0.001, neuron_adjustment == 0.15)
  # filter(hidden_layers == 6) %>%
  filter(edge > 0, Outperformance_Perc > 0.5, total_sims >= 30) %>%
  group_by(Asset) %>%
  slice_max(risk_weighted_return_mid, n = 1)

