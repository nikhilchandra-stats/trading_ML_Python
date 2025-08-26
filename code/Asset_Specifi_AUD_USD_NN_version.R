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

update_local_db_file(
  db_location = db_location,
  time_frame = "H1",
  bid_or_ask = "ask",
  how_far_back = 26
)

update_local_db_file(
  db_location = db_location,
  time_frame = "H1",
  bid_or_ask = "bid",
  how_far_back = 26
)

AUD_USD_NZD_USD_list <-
  get_all_AUD_USD_specific_data(
    db_location = db_location,
    start_date = start_date,
    end_date = today() %>% as.character(),
    time_frame = "H1"
  )

random_results_list <-
  list()

#Beta Binomial - beta(x + a, n - x + b),  x = number of sucesses, a,b hyper priors beta
#
mean(rbeta(n = 900000, shape1 = 5000, shape2 = 5000))
samples <- 1000
random_results_db_location <- "C:/Users/Nikhil Chandra/Documents/trade_data/random_results.db"
db_con <- connect_db(random_results_db_location)
stop_factor = 8
profit_factor = 16
analysis_syms = c("XCU_USD", "NZD_CHF")
trade_samples = 5000
new_table = FALSE
time_frame = "H1"

for (i in 1:samples) {

  temp_results <-
    get_random_results_trades(
      raw_asset_data_ask = AUD_USD_NZD_USD_list[[1]],
      raw_asset_data_bid = AUD_USD_NZD_USD_list[[2]],
      stop_factor = stop_factor,
      profit_factor = profit_factor,
      risk_dollar_value = 10,
      analysis_syms = analysis_syms,
      trade_samples = trade_samples
    )

  complete_results <-
    temp_results[[1]] %>%
    bind_rows(temp_results[[2]]) %>%
    mutate(trade_samples = trade_samples,
           time_frame = time_frame)

  if(new_table == TRUE) {
    write_table_sql_lite(.data = complete_results,
                         table_name = "random_results",
                         conn = db_con,
                         overwrite_true = TRUE)
  }

  if(new_table == FALSE) {
    append_table_sql_lite(
      .data = complete_results,
      table_name = "random_results",
      conn = db_con
    )
  }

}

DBI::dbDisconnect(db_con)

control_random_samples <-
  get_random_samples_MLE_beta(
    random_results_db_location = "C:/Users/Nikhil Chandra/Documents/trade_data/random_results.db",
    stop_factor = 8,
    profit_factor = 16,
    analysis_syms = c("AUD_USD", "NZD_USD", "XCU_USD", "NZD_CHF"),
    time_frame = "H1",
    return_summary = TRUE
  )

AUD_NZD_Trades_long <-
  AUD_USD_NZD_USD_list[[1]] %>%
  filter(Date >= "2010-01-01") %>%
  mutate(
    trade_col = "Long"
  )

AUD_NZD_Long_Data_15_20 <-
  run_pairs_analysis(
    tagged_trades = AUD_NZD_Trades_long,
    stop_factor = 15,
    profit_factor = 20,
    raw_asset_data = AUD_USD_NZD_USD_list[[1]],
    risk_dollar_value = 10,
    return_trade_ts = TRUE
  )

AUD_NZD_Long_Data_10_15 <-
  run_pairs_analysis(
    tagged_trades = AUD_NZD_Trades_long,
    stop_factor = 10,
    profit_factor = 15,
    raw_asset_data = AUD_USD_NZD_USD_list[[1]],
    risk_dollar_value = 10,
    return_trade_ts = TRUE
  )

AUD_NZD_Long_Data_8_12 <-
  run_pairs_analysis(
    tagged_trades = AUD_NZD_Trades_long,
    stop_factor = 8,
    profit_factor = 12,
    raw_asset_data = AUD_USD_NZD_USD_list[[1]],
    risk_dollar_value = 10,
    return_trade_ts = TRUE
  )

AUD_NZD_Trades_Short <-
  AUD_USD_NZD_USD_list[[1]] %>%
  filter(Date >= "2010-01-01") %>%
  mutate(
    trade_col = "Short"
  )

AUD_NZD_Short_Data_15_20 <-
  run_pairs_analysis(
    tagged_trades = AUD_NZD_Trades_Short,
    stop_factor = 15,
    profit_factor = 20,
    raw_asset_data = AUD_USD_NZD_USD_list[[1]],
    risk_dollar_value = 10,
    return_trade_ts = TRUE
  )

AUD_NZD_Short_Data_10_15 <-
  run_pairs_analysis(
    tagged_trades = AUD_NZD_Trades_Short,
    stop_factor = 10,
    profit_factor = 15,
    raw_asset_data = AUD_USD_NZD_USD_list[[1]],
    risk_dollar_value = 10,
    return_trade_ts = TRUE
  )

AUD_NZD_Short_Data_8_12 <-
  run_pairs_analysis(
    tagged_trades = AUD_NZD_Trades_Short,
    stop_factor = 8,
    profit_factor = 12,
    raw_asset_data = AUD_USD_NZD_USD_list[[1]],
    risk_dollar_value = 10,
    return_trade_ts = TRUE
  )

full_ts_trade_db_location = "C:/Users/Nikhil Chandra/Documents/trade_data/full_ts_trades_mapped_AUD_USD.db"

AUD_NZD_Trades_Short <-
  AUD_USD_NZD_USD_list[[1]] %>%
  filter(Date >= "2010-01-01") %>%
  mutate(
    trade_col = "Short"
  )

AUD_NZD_Trades_long <-
  AUD_USD_NZD_USD_list[[1]] %>%
  filter(Date >= "2010-01-01") %>%
  mutate(
    trade_col = "Long"
  )

AUD_NZD_Long_Data_5_6 <-
  run_pairs_analysis(
    tagged_trades = AUD_NZD_Trades_long,
    stop_factor = 5,
    profit_factor = 6,
    raw_asset_data = AUD_USD_NZD_USD_list[[1]],
    risk_dollar_value = 10,
    return_trade_ts = TRUE
  )

AUD_NZD_Short_Data_5_6 <-
  run_pairs_analysis(
    tagged_trades = AUD_NZD_Trades_Short,
    stop_factor = 5,
    profit_factor = 6,
    raw_asset_data = AUD_USD_NZD_USD_list[[1]],
    risk_dollar_value = 10,
    return_trade_ts = TRUE
  )

full_data_for_upload <-
  AUD_NZD_Long_Data_5_6 %>%
  mutate(
    stop_factor = 5,
    profit_factor = 6
  ) %>%
  bind_rows(
    AUD_NZD_Short_Data_5_6 %>%
      mutate(
        stop_factor = 5,
        profit_factor = 6
      )
  )

full_ts_trade_db_con <- connect_db(path = full_ts_trade_db_location)
append_table_sql_lite(.data = full_data_for_upload,
                     table_name = "full_ts_trades_mapped",
                     conn = full_ts_trade_db_con)
DBI::dbDisconnect(full_ts_trade_db_con)
rm(full_ts_trade_db_con)
gc()

rm(full_data_for_upload)
gc()

#------------------------------------------------------------------------------
full_ts_trade_db_location = "C:/Users/Nikhil Chandra/Documents/trade_data/full_ts_trades_mapped_AUD_USD.db"

AUD_NZD_Trades_Short <-
  AUD_USD_NZD_USD_list[[1]] %>%
  filter(Date >= "2010-01-01") %>%
  mutate(
    trade_col = "Short"
  )

AUD_NZD_Trades_long <-
  AUD_USD_NZD_USD_list[[1]] %>%
  filter(Date >= "2010-01-01") %>%
  mutate(
    trade_col = "Long"
  )

AUD_NZD_Long_Data_4_8 <-
  run_pairs_analysis(
    tagged_trades = AUD_NZD_Trades_long,
    stop_factor = 4,
    profit_factor = 8,
    raw_asset_data = AUD_USD_NZD_USD_list[[1]],
    risk_dollar_value = 10,
    return_trade_ts = TRUE
  )

AUD_NZD_Short_Data_4_8 <-
  run_pairs_analysis(
    tagged_trades = AUD_NZD_Trades_Short,
    stop_factor = 4,
    profit_factor = 8,
    raw_asset_data = AUD_USD_NZD_USD_list[[1]],
    risk_dollar_value = 10,
    return_trade_ts = TRUE
  )

full_data_for_upload <-
  AUD_NZD_Long_Data_4_8 %>%
  mutate(
    stop_factor = 4,
    profit_factor = 8
  ) %>%
  bind_rows(
    AUD_NZD_Short_Data_4_8 %>%
      mutate(
        stop_factor = 4,
        profit_factor = 8
      )
  )

full_ts_trade_db_con <- connect_db(path = full_ts_trade_db_location)
append_table_sql_lite(.data = full_data_for_upload,
                      table_name = "full_ts_trades_mapped",
                      conn = full_ts_trade_db_con)
DBI::dbDisconnect(full_ts_trade_db_con)
rm(full_ts_trade_db_con)
gc()

rm(full_data_for_upload)
gc()


#------------------------------------------------------Test with big LM Prop
load_custom_functions()
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
available_assets <- c("AUD_USD", "NZD_USD", "XCU_USD", "NZD_CHF", "XAG_USD")
date_sequence <- seq(as_date("2022-01-01"), as_date("2025-06-01"), "week") %>% sample(size = 10)
all_results_ts <- list()

NN_sims_db <- "C:/Users/Nikhil Chandra/Documents/trade_data/AUD_USD_NZD_XCU_NN_sims.db"
NN_sims_db_con <- connect_db(path = NN_sims_db)
safely_generate_NN <- safely(generate_NNs_create_preds, otherwise = NULL)
date_sequence <-
  seq(as_date("2022-01-01"), as_date("2025-06-01"), "week") %>%
  sample(size = 60)

copula_data_AUD_USD_NZD <-
  create_NN_AUD_USD_XCU_NZD_data(
    AUD_USD_NZD_USD = AUD_USD_NZD_USD_list[[1]],
    raw_macro_data = raw_macro_data,
    actual_wins_losses = actual_wins_losses,
    lag_days = 1,
    stop_value_var = 8,
    profit_value_var = 12,
    use_PCA_vars = FALSE
  )


redo_db = FALSE
stop_value_var <- 8
profit_value_var <- 12

params_to_test <-
  tibble(
    NN_samples = c(5000, 15000,10000, 10000, 5000, 5000),
    hidden_layers = c(5, 2,3, 10, 20, 20),
    ending_thresh = c(0.02, 0.02,0.02, 0.01, 0.02, 0.02),
    p_value_thresh_for_inputs = c(0.3, 0.3,0.3, 0.00001, 0.1, 0.00001),
    neuron_adjustment = c(0, 0, 0,0, 0.5, 0.5),
    trade_direction_var = c("Long", "Long","Long", "Long", "Long", "Long")
  )

params_to_test <-
  tibble(
    NN_samples = c(5000 ),
    hidden_layers = c(20),
    ending_thresh = c(0.02),
    p_value_thresh_for_inputs = c(0.1),
    neuron_adjustment = c(0.50),
    trade_direction_var = c("Long")
  )

for (j in 1:dim(params_to_test)[1]) {

  NN_samples = params_to_test$NN_samples[j] %>% as.numeric()
  hidden_layers = params_to_test$hidden_layers[j] %>% as.numeric()
  ending_thresh = params_to_test$ending_thresh[j] %>% as.numeric()
  p_value_thresh_for_inputs = params_to_test$p_value_thresh_for_inputs[j] %>% as.numeric()
  neuron_adjustment = params_to_test$neuron_adjustment[j] %>% as.numeric()
  analysis_direction <- params_to_test$trade_direction_var[j] %>% as.character()

  # AUD_USD, NZD_USD, XCU_USD Good Params
  # NN_samples = 5000
  # hidden_layers = 5
  # ending_thresh = 0.02
  # p_value_thresh_for_inputs = 0.3
  # neuron_adjustment = 0
  # analysis_direction <- "Long"
  # redo_db = TRUE
  # stop_value_var <- 8
  # profit_value_var <- 12

  # AUD_USD, XAG_USD, XCU_USD Good Params
  # NN_samples = 15000
  # hidden_layers = 2
  # ending_thresh = 0.02
  # p_value_thresh_for_inputs = 0.3
  # neuron_adjustment = 0
  # analysis_direction <- "Long"
  # redo_db = TRUE
  # stop_value_var <- 8
  # profit_value_var <- 12

  # AUD_USD, XAG_USD, XCU_USD More Good Params
  # NN_samples = 2000
  # hidden_layers = 10
  # ending_thresh = 0.01
  # p_value_thresh_for_inputs = 0.00001
  # neuron_adjustment = 0
  # analysis_direction <- "Long"
  # redo_db = TRUE
  # stop_value_var <- 8
  # profit_value_var <- 12

  for (k in 14:length(date_sequence)) {

    gc()

    max_test_date <- (date_sequence[k] + dmonths(1)) %>% as_date() %>% as.character()
    accumulating_data <- list()
    # available_assets <- available_assets %>%
    #   keep(~ .x != "NZD_CHF") %>%
    #   unlist() %>%
    #   as.character()

    for (i in 1:length(available_assets)) {
      # message(i)
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
        run_logit_instead = FALSE,
        p_value_thresh_for_inputs = p_value_thresh_for_inputs,
        neuron_adjustment = neuron_adjustment,
        lag_price_col = "Price"
      ) %>%
        pluck('result')

      gc()

      if(!is.null(check_completion)) {
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

# all_results_ts_dfr <-
#   all_results_ts %>%
#   map_dfr(bind_rows)

all_results_ts_dfr_sum <-
  all_results_ts_dfr %>%
  mutate(Control_Wins = round(Perc_control*Total_control)) %>%
  group_by(Asset, threshold, trade_col, NN_samples, ending_thresh, p_value_thresh_for_inputs, neuron_adjustment,
           hidden_layers, win_amount, loss_amount
           ) %>%
  summarise(Trades = sum(Trades),
            wins_losses = sum(wins_losses),
            control_trades = sum(Total_control),
            Control_Wins = sum(Control_Wins)) %>%
  mutate(Perc = wins_losses/Trades,
         Perc_Control = Control_Wins/control_trades,
         risk_weighted_return = Perc*(win_amount/loss_amount) - (1- Perc),
         risk_weighted_return_control = Perc_Control*(win_amount/loss_amount) - (1- Perc_Control)
         ) %>%
  filter(hidden_layers == 20, neuron_adjustment == 0.50)
  # filter(
  #   NN_samples == 2000,
  #   hidden_layers == 2,
  #   ending_thresh == 0.02,
  #   p_value_thresh_for_inputs == 0.1,
  #   neuron_adjustment == 0.25,
  #   analysis_direction == "Long",
  # )

all_asset_logit_results_sum <-
  all_results_ts_dfr %>%
  mutate(
    edge = risk_weighted_return - control_risk_return,
    outperformance_count = ifelse(Perc > Perc_control, 1, 0),
    returns_total = Trades*Perc*win_amount - Trades*loss_amount*(1-Perc)
  ) %>%
  group_by(Asset, threshold, NN_samples, ending_thresh, p_value_thresh_for_inputs, neuron_adjustment,
           hidden_layers, trade_col
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
  filter(simulations >= 10, edge > 0, outperformance_count > 0.51, risk_weighted_return_mid > 0.1) %>%
  group_by(Asset) %>%
  slice_max(risk_weighted_return_mid)
  # filter(NN_samples == 10000)


#' AUD_NZD_create_trade_NNs
#'
#' @param NN_path
#' @param copula_data_AUD_USD_NZD
#' @param stop_value_var
#' @param profit_value_var
#' @param NN_path_save_path
#'
#' @return
#' @export
#'
#' @examples
get_NN_trades <- function(
    NN_path = "C:/Users/Nikhil Chandra/Documents/trade_data/asset_specific_NNs/",
    copula_data = copula_data_AUD_USD_NZD,
    stop_value_var = 8,
    profit_value_var = 12,
    NN_path_save_path = "C:/Users/Nikhil Chandra/Documents/trade_data/asset_specific_NNs/trading_algo",
    NN_sims_db = "C:/Users/Nikhil Chandra/Documents/trade_data/AUD_USD_NZD_XCU_NN_sims.db",
    table_name = "AUD_USD_NZD_XCU_NN_sims",
    skip_NN_generation = FALSE
  ) {

  date_filter_for_NN <-
    as.character(copula_data[[1]]$Date %>% max(na.rm = T) + days(1))

  NN_sims_db_con <- connect_db(path = NN_sims_db)
  all_results_ts_dfr <- DBI::dbGetQuery(conn = NN_sims_db_con,
                                        statement =
                                          as.character(glue::glue("SELECT * FROM {table_name}"))
                                        )
  DBI::dbDisconnect(NN_sims_db_con)
  rm(NN_sims_db_con)

  all_asset_logit_results_sum <-
    all_results_ts_dfr %>%
    mutate(
      edge = risk_weighted_return - control_risk_return,
      outperformance_count = ifelse(Perc > Perc_control, 1, 0),
      returns_total = Trades*Perc*win_amount - Trades*loss_amount*(1-Perc)
    ) %>%
    group_by(Asset, threshold, NN_samples, ending_thresh, p_value_thresh_for_inputs, neuron_adjustment,
             hidden_layers, trade_col
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
    filter(simulations >= 10, edge > 0, outperformance_count > 0.51, risk_weighted_return_mid > 0.1) %>%
    group_by(Asset) %>%
    slice_max(risk_weighted_return_mid) %>%
    group_by(Asset) %>%
    slice_max(Trades) %>%
    ungroup()


  gernating_params <-
    all_asset_logit_results_sum %>%
    distinct(Asset, NN_samples, ending_thresh,
             p_value_thresh_for_inputs,
             neuron_adjustment,
             hidden_layers,
             trade_col)

  accumulating_trades <- list()

  for (i in 1:dim(gernating_params)[1] ) {

    if(skip_NN_generation == TRUE) {
      check_completion <- generate_NNs_create_preds(
        copula_data_macro = copula_data[[1]],
        lm_vars1 = copula_data[[2]],
        NN_samples = gernating_params$NN_samples[i] %>% as.integer(),
        dependant_var_name = gernating_params$Asset[i] %>% as.character(),
        NN_path = NN_path_save_path,
        training_max_date = date_filter_for_NN,
        lm_train_prop = 1,
        trade_direction_var = gernating_params$trade_col[i] %>% as.character(),
        stop_value_var = stop_value_var,
        profit_value_var = profit_value_var,
        max_NNs = 1,
        hidden_layers = gernating_params$hidden_layers[i] %>% as.integer(),
        ending_thresh = gernating_params$ending_thresh[i] %>% as.numeric(),
        run_logit_instead = FALSE,
        p_value_thresh_for_inputs = gernating_params$p_value_thresh_for_inputs[i] %>% as.numeric(),
        neuron_adjustment = gernating_params$neuron_adjustment[i] %>% as.numeric(),
        lag_price_col = "Price"
      )
    }

    NN_test_preds <-
      read_NNs_create_preds(
        copula_data_macro = copula_data[[1]],
        lm_vars1 = copula_data[[2]],
        dependant_var_name = gernating_params$Asset[i] %>% as.character(),
        NN_path = NN_path_save_path,
        testing_min_date = as.character(as_date(date_filter_for_NN) - days(1000)),
        trade_direction_var = gernating_params$trade_col[i] %>% as.character(),
        NN_index_to_choose = "",
        stop_value_var = stop_value_var,
        profit_value_var = profit_value_var,
        analysis_threshs = c(0.5,0.6, 0.7, 0.8, 0.9, 0.95, 0.99, 0.999),
        run_logit_instead = FALSE,
        lag_price_col = "Price",
        return_tagged_trades = TRUE
      )

    accumulating_trades[[i]] <-
      NN_test_preds %>%
      slice_max(Date) %>%
      filter(Asset == available_assets[i]) %>%
      mutate(
        stop_factor = stop_value_var,
        profit_factor = profit_value_var
      ) %>%
      mutate(
        pred_min = gernating_params$threshold[i] %>% as.numeric()
      )

  }

}
