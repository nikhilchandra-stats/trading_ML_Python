helperfunctions35South::load_custom_functions()
one_drive_path <- helperfunctions35South::create_one_drive_path(
  path_extension = "raw data")
library(neuralnet)
all_aud_symbols <- get_oanda_symbols() %>%
  keep(~ str_detect(.x, "AUD")|str_detect(.x, "USD_SEK|USD_NOK|USD_HUF|USD_ZAR|USD_CNY|USD_MXN|USD_CNH"))
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
raw_macro_data <- niksmacrohelpers::get_macro_event_data()

currency_conversion <-
  aud_usd_today %>%
  mutate(
    not_aud_asset = ending_value
  ) %>%
  dplyr::select(not_aud_asset, adjusted_conversion) %>%
  bind_rows(
    tibble(not_aud_asset = "AUD", adjusted_conversion = 1)
  )


db_location = "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data For EDA.db"
start_date = "2011-01-01"
end_date = today() %>% as.character()
time_frame = "H1"

commod_USD <-
  get_all_commod_USD(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame
  )

commod_USD_bid <-
  get_all_commod_USD(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame
  )

mean_values_by_asset_for_loop <-
  wrangle_asset_data(
    asset_data_daily_raw = commod_USD,
    summarise_means = TRUE
  )

commod_USD_bid$Asset %>% unique()
gc()

full_ts_trade_db_location = "C:/Users/Nikhil Chandra/Documents/trade_data/full_ts_trades_mapped.db"
full_ts_trade_db_con <- connect_db(full_ts_trade_db_location)

commod_Trades_long <-
  commod_USD %>%
  filter(Date >= "2016-01-01") %>%
  mutate(
    trade_col = "Long"
  )

commod_Long_Data_15_20 <-
  run_pairs_analysis(
    tagged_trades = commod_Trades_long,
    stop_factor = 15,
    profit_factor = 20,
    raw_asset_data = commod_USD,
    risk_dollar_value = 10,
    return_trade_ts = TRUE
  ) %>%
  mutate(  stop_factor = 15,
           profit_factor = 20)

append_table_sql_lite(.data = commod_Long_Data_15_20,
                      table_name = "full_ts_trades_mapped",
                      conn = full_ts_trade_db_con)

commod_Long_Data_10_15 <-
  run_pairs_analysis(
    tagged_trades = commod_Trades_long,
    stop_factor = 10,
    profit_factor = 15,
    raw_asset_data = commod_USD,
    risk_dollar_value = 10,
    return_trade_ts = TRUE
  ) %>%
  mutate(  stop_factor = 10,
           profit_factor = 15)

append_table_sql_lite(.data = commod_Long_Data_10_15,
                      table_name = "full_ts_trades_mapped",
                      conn = full_ts_trade_db_con)

commod_Long_Data_8_12 <-
  run_pairs_analysis(
    tagged_trades = commod_Trades_long,
    stop_factor = 8,
    profit_factor = 12,
    raw_asset_data = commod_USD,
    risk_dollar_value = 10,
    return_trade_ts = TRUE
  ) %>%
  mutate(  stop_factor = 8,
           profit_factor = 12)

append_table_sql_lite(.data = commod_Long_Data_8_12,
                      table_name = "full_ts_trades_mapped",
                      conn = full_ts_trade_db_con)

commod_Long_Data_4_8 <-
  run_pairs_analysis(
    tagged_trades = commod_Trades_long,
    stop_factor = 4,
    profit_factor = 8,
    raw_asset_data = commod_USD,
    risk_dollar_value = 10,
    return_trade_ts = TRUE
  ) %>%
  mutate(  stop_factor = 4,
           profit_factor = 8)

append_table_sql_lite(.data = commod_Long_Data_4_8,
                      table_name = "full_ts_trades_mapped",
                      conn = full_ts_trade_db_con)


commod_Long_Data_5_6 <-
  run_pairs_analysis(
    tagged_trades = commod_Trades_long,
    stop_factor = 5,
    profit_factor = 6,
    raw_asset_data = commod_USD,
    risk_dollar_value = 10,
    return_trade_ts = TRUE
  ) %>%
  mutate(  stop_factor = 5,
           profit_factor = 6)

append_table_sql_lite(.data = commod_Long_Data_5_6,
                      table_name = "full_ts_trades_mapped",
                      conn = full_ts_trade_db_con)


rm(commod_Long_Data_15_20, commod_Long_Data_10_15,
   commod_Long_Data_4_8, commod_Long_Data_8_12, commod_Trades_long,
   commod_Long_Data_5_6)
gc()
#-------------------------------------------------------
commod_Trades_Short <-
  commod_USD_bid %>%
  filter(Date >= "2016-01-01") %>%
  mutate(
    trade_col = "Short"
  )

commod_Short_Data_15_20 <-
  run_pairs_analysis(
    tagged_trades = commod_Trades_Short,
    stop_factor = 15,
    profit_factor = 20,
    raw_asset_data = commod_USD_bid,
    risk_dollar_value = 10,
    return_trade_ts = TRUE
  ) %>%
  mutate(  stop_factor = 15,
           profit_factor = 20)

append_table_sql_lite(.data = commod_Short_Data_15_20,
                      table_name = "full_ts_trades_mapped",
                      conn = full_ts_trade_db_con)

commod_Short_Data_10_15 <-
  run_pairs_analysis(
    tagged_trades = commod_Trades_Short,
    stop_factor = 10,
    profit_factor = 15,
    raw_asset_data = commod_USD_bid,
    risk_dollar_value = 10,
    return_trade_ts = TRUE
  ) %>%
  mutate(  stop_factor = 10,
           profit_factor = 15)

append_table_sql_lite(.data = commod_Short_Data_10_15,
                      table_name = "full_ts_trades_mapped",
                      conn = full_ts_trade_db_con)

commod_Short_Data_8_12 <-
  run_pairs_analysis(
    tagged_trades = commod_Trades_Short,
    stop_factor = 8,
    profit_factor = 12,
    raw_asset_data = commod_USD_bid,
    risk_dollar_value = 10,
    return_trade_ts = TRUE
  ) %>%
  mutate(  stop_factor = 8,
           profit_factor = 12)

append_table_sql_lite(.data = commod_Short_Data_8_12,
                      table_name = "full_ts_trades_mapped",
                      conn = full_ts_trade_db_con)

commod_Short_Data_4_8 <-
  run_pairs_analysis(
    tagged_trades = commod_Trades_Short,
    stop_factor = 4,
    profit_factor = 8,
    raw_asset_data = commod_USD_bid,
    risk_dollar_value = 10,
    return_trade_ts = TRUE
  ) %>%
  mutate(  stop_factor = 4,
           profit_factor = 8)

append_table_sql_lite(.data = commod_Short_Data_4_8,
                      table_name = "full_ts_trades_mapped",
                      conn = full_ts_trade_db_con)

commod_Short_Data_5_6 <-
  run_pairs_analysis(
    tagged_trades = commod_Trades_Short,
    stop_factor = 5,
    profit_factor = 6,
    raw_asset_data = commod_USD_bid,
    risk_dollar_value = 10,
    return_trade_ts = TRUE
  ) %>%
  mutate(  stop_factor = 5,
           profit_factor = 6)

append_table_sql_lite(.data = commod_Short_Data_5_6,
                      table_name = "full_ts_trades_mapped",
                      conn = full_ts_trade_db_con)


DBI::dbDisconnect(full_ts_trade_db_con)
rm(full_ts_trade_db_con)
gc()

rm(commod_Short_Data_15_20, commod_Short_Data_10_15,
   commod_Short_Data_4_8, commod_Short_Data_8_12, commod_Trades_Short,
   commod_Short_Data_5_6)
gc()


#--------------------------------------------------------

#----------------------------------------------------------------------------------
analysis_syms = c("BCO_USD", "WTICO_USD" ,"NATGAS_USD", "SOYBN_USD", "SUGAR_USD",
                  "WHEAT_USD", "XAG_USD", "XAU_USD", "XCU_USD")
commod_log_cumulative <-
  analysis_syms %>%
  map_dfr(
    ~
      create_log_cumulative_returns(
        asset_data_to_use = commod_USD,
        asset_to_use = c(.x[1]),
        price_col = "Open",
        return_long_format = TRUE
      )
  ) %>%
  left_join(
    commod_USD %>% distinct(Date, Asset, Price, Open, High, Low)
  )

commod_log_cumulative_bid <-
  analysis_syms %>%
  map_dfr(
    ~
      create_log_cumulative_returns(
        asset_data_to_use = commod_USD_bid,
        asset_to_use = c(.x[1]),
        price_col = "Open",
        return_long_format = TRUE
      )
  ) %>%
  left_join(
    commod_USD_bid %>% distinct(Date, Asset, Price, Open, High, Low)
  )


full_ts_trade_db_location = "C:/Users/Nikhil Chandra/Documents/trade_data/full_ts_trades_mapped.db"
full_ts_trade_db_con <- connect_db(full_ts_trade_db_location)

actual_wins_losses <-
  DBI::dbGetQuery(full_ts_trade_db_con,
                  "SELECT * FROM full_ts_trades_mapped") %>%
  mutate(
    dates = as_datetime(dates)
  ) %>%
  filter(asset %in% analysis_syms)
DBI::dbDisconnect(full_ts_trade_db_con)
rm(full_ts_trade_db_con)

#------------------------------------------------------
modelling_data_all <-
  create_NN_Commod_data(
    commod_data = commod_log_cumulative,
    raw_macro_data  = raw_macro_data,
    actual_wins_losses = actual_wins_losses,
    assets_to_use_PCA_index = analysis_syms,
    lag_days = 1,
    stop_value_var = 4,
    profit_value_var = 8,
    rolling_period_index_PCA_cor = 15,
    lagged_var_name = "Price"
  )
random_dates <-
  seq(as_date("2020-01-01"), as_date("2025-03-01"), "week") %>% sample(size = 100,replace = FALSE)

WTI_results <- list()
BCO_results <- list()
NATGAS_results <- list()

for (i in 1:length(random_dates)) {


  check_completion <- safely_generate_NN(
    copula_data_macro = modelling_data_all[[1]],
    lm_vars1 = modelling_data_all[[2]],
    NN_samples = 1000,
    dependant_var_name = "WTICO_USD",
    NN_path = "C:/Users/Nikhil Chandra/Documents/trade_data/asset_specific_NNs/",
    training_max_date = as.character(random_dates[i]) ,
    lm_train_prop = 0.999,
    trade_direction_var = "Long",
    stop_value_var = 4,
    profit_value_var = 8,
    max_NNs = 1,
    hidden_layers = 4,
    ending_thresh = 0.01,
    run_logit_instead = FALSE,
    p_value_thresh_for_inputs = 0.05
  )

  gc()

  WTI_results[[i]] <-
    read_NNs_create_preds(
      copula_data_macro = modelling_data_all[[1]] %>% filter(Date <= (as_date(random_dates[i]) + dweeks(3)) ),
      lm_vars1 = modelling_data_all[[2]],
      dependant_var_name = "WTICO_USD",
      NN_path = "C:/Users/Nikhil Chandra/Documents/trade_data/asset_specific_NNs/",
      testing_min_date = as.character(random_dates[i]),
      lm_test_prop = 0.9999,
      trade_direction_var = "Long",
      NN_index_to_choose = "",
      stop_value_var = 4,
      profit_value_var = 8,
      analysis_threshs = c(0.5, 0.6, 0.7, 0.8, 0.9, 0.95),
      run_logit_instead = FALSE,
      lag_price_col = "Price",
      return_tagged_trades = FALSE
    ) %>%
    mutate(
      sim_date = random_dates[i]
    )

  gc()


  generate_NNs_create_preds_futures(
    copula_data_macro = modelling_data_all[[1]],
    lm_vars1 = modelling_data_all[[2]],
    NN_samples = 1000,
    dependant_var_name = "BCO_USD",
    NN_path = "C:/Users/Nikhil Chandra/Documents/trade_data/asset_specific_NNs/",
    training_max_date = as.character(random_dates[i]) ,
    lm_train_prop = 0.999,
    trade_direction_var = "Long",
    stop_value_var = 4,
    profit_value_var = 8,
    max_NNs = 1,
    hidden_layers = c(33,33,33,33),
    ending_thresh = 0.01,
    run_logit_instead = FALSE,
    p_value_thresh_for_inputs = 0.05
  )

  BCO_results[[i]] <-
    read_NNs_create_preds_futures(
      copula_data_macro = modelling_data_all[[1]] %>% filter(Date <= (as_date(random_dates[i]) + dweeks(3)) ),
      lm_vars1 = modelling_data_all[[2]],
      dependant_var_name = "BCO_USD",
      NN_path = "C:/Users/Nikhil Chandra/Documents/trade_data/asset_specific_NNs/",
      testing_min_date = as.character(random_dates[i]),
      lm_test_prop = 0.9999,
      trade_direction_var = "Long",
      NN_index_to_choose = "",
      stop_value_var = 4,
      profit_value_var = 8,
      analysis_threshs = c(0.5, 0.6, 0.7, 0.8, 0.9, 0.95),
      run_logit_instead = FALSE
    ) %>%
    mutate(
      sim_date = random_dates[i]
    )

  generate_NNs_create_preds_futures(
    copula_data_macro = modelling_data_all[[1]],
    lm_vars1 = modelling_data_all[[2]],
    NN_samples = 1000,
    dependant_var_name = "NATGAS_USD",
    NN_path = "C:/Users/Nikhil Chandra/Documents/trade_data/asset_specific_NNs/",
    training_max_date = as.character(random_dates[i]) ,
    lm_train_prop = 0.999,
    trade_direction_var = "Long",
    stop_value_var = 4,
    profit_value_var = 8,
    max_NNs = 1,
    hidden_layers = c(33,33,33,33),
    ending_thresh = 0.01,
    run_logit_instead = FALSE,
    p_value_thresh_for_inputs = 0.05
  )

  NATGAS_results[[i]] <-
    read_NNs_create_preds_futures(
      copula_data_macro = modelling_data_all[[1]] %>% filter(Date <= (as_date(random_dates[i]) + dweeks(3)) ),
      lm_vars1 = modelling_data_all[[2]],
      dependant_var_name = "NATGAS_USD",
      NN_path = "C:/Users/Nikhil Chandra/Documents/trade_data/asset_specific_NNs/",
      testing_min_date = as.character(random_dates[i]),
      lm_test_prop = 0.9999,
      trade_direction_var = "Long",
      NN_index_to_choose = "",
      stop_value_var = 4,
      profit_value_var = 8,
      analysis_threshs = c(0.5, 0.6, 0.7, 0.8, 0.9, 0.95),
      run_logit_instead = FALSE
    ) %>%
    mutate(
      sim_date = random_dates[i]
    )

}

NN_sims_db <- "C:/Users/Nikhil Chandra/Documents/trade_data/COMMOD_NN_sims.db"
NN_sims_db_con <- connect_db(path = NN_sims_db)

all_trades <-
  list(WTI_results,
       BCO_results,
       NATGAS_results) %>%
  map_dfr(bind_rows)

# write_table_sql_lite(.data = all_trades,
#                      table_name = "COMMOD_NN_sims",
#                      conn = NN_sims_db_con,
#                      overwrite_true = TRUE)

all_trades_sum <-
  all_trades %>%
  mutate(Control_Wins = round(Perc_control*Total_control)) %>%
  group_by(threshold, Asset) %>%
  summarise(
          Average_Trades_3_weeks = mean(Trades, na.rm = T),
            Trades = sum(Trades, na.rm = T),
            wins_losses = sum(wins_losses, na.rm = T),
            win_amount = mean(win_amount),
            loss_amount  = mean(loss_amount),
            Total_control = sum(Total_control),
            Control_Wins = sum(Control_Wins)
            ) %>%
  mutate(
    Perc = wins_losses/Trades,
    control_perc = Control_Wins/Total_control,
    risk_weighted_return = Perc*(win_amount/loss_amount) - (1-Perc),
    risk_weighted_return_control = control_perc*(win_amount/loss_amount) - (1-control_perc)
  )


#' create_NN_AUD_USD_XCU_NZD_data
#'
#' @return
#' @export
#'
#' @examples
create_NN_Commod_data <-
  function(commod_data = commod_log_cumulative,
           raw_macro_data  = raw_macro_data,
           actual_wins_losses = actual_wins_losses,
           assets_to_use_PCA_index = analysis_syms,
           lag_days = 1,
           stop_value_var = 15,
           profit_value_var = 20,
           rolling_period_index_PCA_cor = 15,
           lagged_var_name = "Price") {

    # assets_to_return <- dependant_var_name

    aus_macro_data <-
      get_AUS_Indicators(raw_macro_data,
                         lag_days = lag_days,
                         first_difference = TRUE
      ) %>%
      janitor::clean_names()

    nzd_macro_data <-
      get_NZD_Indicators(raw_macro_data,
                         lag_days = lag_days,
                         first_difference = TRUE
      ) %>%
      janitor::clean_names()

    usd_macro_data <-
      get_USD_Indicators(raw_macro_data,
                         lag_days = lag_days,
                         first_difference = TRUE
      ) %>%
      janitor::clean_names()

    cny_macro_data <-
      get_CNY_Indicators(raw_macro_data,
                         lag_days = lag_days,
                         first_difference = TRUE
      ) %>%
      janitor::clean_names()

    eur_macro_data <-
      get_EUR_Indicators(raw_macro_data,
                         lag_days = lag_days,
                         first_difference = TRUE
      ) %>%
      janitor::clean_names()

    eur_macro_data_PCA <-
      eur_macro_data %>%
      dplyr::select(-date) %>%
      prcomp(scale = TRUE) %>%
      pluck("x") %>%
      as_tibble() %>%
      dplyr::select(EUR_PC1= PC1,
                    EUR_PC2= PC2,
                    EUR_PC3= PC3,
                    EUR_PC4= PC4,
                    EUR_PC5= PC5) %>%
      mutate(
        date = eur_macro_data %>% pull(date)
      )

    aud_macro_vars <- names(aus_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    nzd_macro_vars <- names(nzd_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    usd_macro_vars <- names(usd_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    cny_macro_vars <- names(cny_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    eur_macro_vars <- names(eur_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    all_macro_vars <- c(aud_macro_vars, nzd_macro_vars, usd_macro_vars, cny_macro_vars, eur_macro_vars)
    PC_macro_vars <-
      c("AUD_PC1", "AUD_PC2",
        "NZD_PC1", "NZD_PC2",
        "USD_PC1", "USD_PC2",
        "CNY_PC1", "CNY_PC2",
        "EUR_PC1", "EUR_PC2")

    commod_data <-
      commod_data %>%
      group_by(Asset) %>%
      mutate(
        Return_Index_Diff = ((Price - Open)/Open)*100
      ) %>%
      ungroup() %>%
      filter(!is.na(Return_Index_Diff))

      commod_data_Index <-
        create_PCA_Asset_Index(
          asset_data_to_use = commod_data,
          asset_to_use = assets_to_use_PCA_index,
          price_col = "Return_Index_Diff"
        ) %>%
        rename(
          Average_PCA_Index = Average_PCA
        )

    commod_data_pca <-
      commod_data %>%
      left_join(commod_data_Index)

    correlation_PCA_dat <-
      c("BCO_USD", "NATGAS_USD", "SOYBN_USD", "SUGAR_USD", "WHEAT_USD", "WTICO_USD", "XCU_USD") %>%
      map_dfr(
        ~
          get_PCA_Index_rolling_cor_sd_mean(
            raw_asset_data_for_PCA_cor = commod_data %>% filter(Asset == .x),
            PCA_data = commod_data_pca,
            rolling_period = rolling_period_index_PCA_cor
          )
      )

    copula_data_WTI_BCO <-
      estimating_dual_copula(
        asset_data_to_use = commod_data,
        asset_to_use = c("WTICO_USD", "BCO_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      )

    copula_data_WTI_XAU <-
      estimating_dual_copula(
        asset_data_to_use = commod_data,
        asset_to_use = c("WTICO_USD", "XAU_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-WTICO_USD,
                    -WTICO_USD_log1_price,
                    -WTICO_USD_quantiles_1,
                    -WTICO_USD_tangent_angle1)

    copula_data_BCO_XAU <-
      estimating_dual_copula(
        asset_data_to_use = commod_data,
        asset_to_use = c("BCO_USD", "XAU_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-BCO_USD,
                    -BCO_USD_log1_price,
                    -BCO_USD_quantiles_1,
                    -BCO_USD_tangent_angle1,
                    -XAU_USD,
                    -XAU_USD_log2_price,
                    -XAU_USD_quantiles_2,
                    -XAU_USD_tangent_angle2)

    copula_data_BCO_NATGAS <-
      estimating_dual_copula(
        asset_data_to_use = commod_data,
        asset_to_use = c("BCO_USD", "NATGAS_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-BCO_USD,
                    -BCO_USD_log1_price,
                    -BCO_USD_quantiles_1,
                    -BCO_USD_tangent_angle1)

    copula_data_WTI_NATGAS <-
      estimating_dual_copula(
        asset_data_to_use = commod_data,
        asset_to_use = c("WTICO_USD", "NATGAS_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-WTICO_USD,
                    -WTICO_USD_log1_price,
                    -WTICO_USD_quantiles_1,
                    -WTICO_USD_tangent_angle1,
                    -NATGAS_USD,
                    -NATGAS_USD_log2_price,
                    -NATGAS_USD_quantiles_2,
                    -NATGAS_USD_tangent_angle2)

    copula_data_XAG_XAU <-
      estimating_dual_copula(
        asset_data_to_use = commod_data,
        asset_to_use = c("XAG_USD", "XAU_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-XAU_USD,
                    -XAU_USD_log2_price,
                    -XAU_USD_quantiles_2,
                    -XAU_USD_tangent_angle2)

    copula_data_WTI_XAG <-
      estimating_dual_copula(
        asset_data_to_use = commod_data,
        asset_to_use = c("WTICO_USD", "XAG_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(
                    -WTICO_USD,
                    -WTICO_USD_log1_price,
                    -WTICO_USD_quantiles_1,
                    -WTICO_USD_tangent_angle1,
                    -XAG_USD,
                    -XAG_USD_log2_price,
                    -XAG_USD_quantiles_2,
                    -XAG_USD_tangent_angle2)

    copula_data_BCO_XAG <-
      estimating_dual_copula(
        asset_data_to_use = commod_data,
        asset_to_use = c("BCO_USD", "XAG_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(
        -BCO_USD,
        -BCO_USD_log1_price,
        -BCO_USD_quantiles_1,
        -BCO_USD_tangent_angle1,
        -XAG_USD,
        -XAG_USD_log2_price,
        -XAG_USD_quantiles_2,
        -XAG_USD_tangent_angle2)

    copula_data_XCU_XAG <-
      estimating_dual_copula(
        asset_data_to_use = commod_data,
        asset_to_use = c("XCU_USD", "XAG_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(
        -XAG_USD,
        -XAG_USD_log2_price,
        -XAG_USD_quantiles_2,
        -XAG_USD_tangent_angle2)

    copula_data_XCU_WTI <-
      estimating_dual_copula(
        asset_data_to_use = commod_data,
        asset_to_use = c("XCU_USD", "WTICO_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(
        -XCU_USD,
        -XCU_USD_log1_price,
        -XCU_USD_quantiles_1,
        -XCU_USD_tangent_angle1,
        -WTICO_USD,
        -WTICO_USD_log2_price,
        -WTICO_USD_quantiles_2,
        -WTICO_USD_tangent_angle2)

    copula_data_WHEAT_SOY <-
      estimating_dual_copula(
        asset_data_to_use = commod_data,
        asset_to_use = c("WHEAT_USD", "SOYBN_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      )

    copula_data_WHEAT_SUGAR <-
      estimating_dual_copula(
        asset_data_to_use = commod_data,
        asset_to_use = c("WHEAT_USD", "SUGAR_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(
        -WHEAT_USD,
        -WHEAT_USD_log1_price,
        -WHEAT_USD_quantiles_1,
        -WHEAT_USD_tangent_angle1,
      )

    binary_data_for_post_model <-
      actual_wins_losses %>%
      rename(Date = dates, Asset = asset) %>%
      filter(profit_factor == profit_value_var, stop_factor == stop_value_var) %>%
      filter(Asset %in% assets_to_use_PCA_index) %>%
      mutate(
        bin_var =
          case_when(
            trade_start_prices > trade_end_prices & trade_col == "Short" ~ "win",
            trade_start_prices <= trade_end_prices & trade_col == "Short" ~ "loss",

            trade_start_prices < trade_end_prices & trade_col == "Long" ~ "win",
            trade_start_prices >= trade_end_prices & trade_col == "Long" ~ "loss"
          )
      ) %>%
      dplyr::select(Date, bin_var, Asset, trade_col,
                    profit_factor, stop_factor,
                    trade_start_prices, trade_end_prices,
                    starting_stop_value, starting_profit_value)

    copula_data_macro <-
      correlation_PCA_dat %>%
      group_by(Asset) %>%
      arrange(Date, .by_group = TRUE) %>%
      group_by(Asset) %>%
      mutate(across(where(is.numeric), ~ lag(.)))  %>%
      left_join(copula_data_WTI_BCO)
    #   left_join(copula_data_WTI_XAG) %>%
    #   left_join(copula_data_WTI_NATGAS) %>%
    #   left_join(copula_data_WTI_XAU) %>%
    #   left_join(copula_data_BCO_NATGAS) %>%
    #   left_join(copula_data_BCO_XAG) %>%
    #   left_join(copula_data_BCO_XAU) %>%
    #   left_join(copula_data_WHEAT_SOY) %>%
    #   left_join(copula_data_WHEAT_SUGAR) %>%
    #   mutate(Date_for_join = as_date(Date)) %>%
    #   filter(if_all(everything() ,.fns = ~ !is.na(.))) %>%
    #   left_join(
    #     aus_macro_data %>%
    #       rename(Date_for_join = date)
    #   ) %>%
    #   left_join(
    #     nzd_macro_data %>%
    #       rename(Date_for_join = date)
    #   ) %>%
    #   left_join(
    #     usd_macro_data %>%
    #       rename(Date_for_join = date)
    #   ) %>%
    #   left_join(
    #     cny_macro_data %>%
    #       rename(Date_for_join = date)
    #   )  %>%
    #   left_join(
    #     eur_macro_data %>%
    #       rename(Date_for_join = date)
    #   ) %>%
    #   group_by(Asset) %>%
    #   arrange(Date, .by_group = TRUE) %>%
    #   group_by(Asset) %>%
    #   fill(matches(all_macro_vars), .direction = "down") %>%
    #   ungroup() %>%
    #   filter(if_all(everything() ,.fns = ~ !is.na(.))) %>%
    #   mutate(hour_of_day = lubridate::hour(Date) %>% as.numeric(),
    #          day_of_week = lubridate::wday(Date) %>% as.numeric()) %>%
    #   group_by(Asset) %>%
    #   mutate(
    #     lagged_var_1 = lag(!!as.name(lagged_var_name), 1),
    #     lagged_var_2 = lag(!!as.name(lagged_var_name), 2),
    #     lagged_var_3 = lag(!!as.name(lagged_var_name), 3),
    #     lagged_var_5 = lag(!!as.name(lagged_var_name), 5),
    #     lagged_var_8 = lag(!!as.name(lagged_var_name), 8),
    #     lagged_var_13 = lag(!!as.name(lagged_var_name), 13),
    #     lagged_var_21 = lag(!!as.name(lagged_var_name), 21),
    #
    #     fib_1 = lagged_var_1 + lagged_var_2,
    #     fib_2 = lagged_var_2 + lagged_var_3,
    #     fib_3 = lagged_var_3 + lagged_var_5,
    #     fib_4 = lagged_var_5 + lagged_var_8,
    #     fib_5 = lagged_var_8 + lagged_var_13,
    #     fib_6 = lagged_var_13 + lagged_var_21,
    #
    #     lagged_var_3_ma = slider::slide_dbl(.x = lagged_var_1,
    #                                         .f = ~ mean(.x, na.rm = T),
    #                                         .before = 3),
    #
    #     lagged_var_5_ma = slider::slide_dbl(.x = lagged_var_1,
    #                                         .f = ~ mean(.x, na.rm = T),
    #                                         .before = 5),
    #
    #     lagged_var_8_ma = slider::slide_dbl(.x = lagged_var_1,
    #                                         .f = ~ mean(.x, na.rm = T),
    #                                         .before = 8),
    #
    #     lagged_var_13_ma = slider::slide_dbl(.x = lagged_var_1,
    #                                          .f = ~ mean(.x, na.rm = T),
    #                                          .before = 13),
    #
    #     lagged_var_21_ma = slider::slide_dbl(.x = lagged_var_1,
    #                                          .f = ~ mean(.x, na.rm = T),
    #                                          .before = 21)
    #   ) %>%
    #   ungroup()%>%
    #   left_join(binary_data_for_post_model))
    #
    # lm_quant_vars <- names(copula_data_macro) %>% keep(~ str_detect(.x,"quantiles|tangent|cor"))
    #
    # lm_vars1 <- c(all_macro_vars,
    #                 lm_quant_vars,
    #                 "fib_1", "fib_2",
    #                 "fib_3", "fib_4",
    #                 "fib_5", "fib_6",
    #                 "lagged_var_1", "lagged_var_2",
    #                 "lagged_var_3", "lagged_var_5",
    #                 "lagged_var_8",
    #                 "lagged_var_13", "lagged_var_21",
    #                 "lagged_var_3_ma", "lagged_var_5_ma",
    #                 "lagged_var_8_ma", "lagged_var_13_ma",
    #                 "lagged_var_21_ma",
    #                 "hour_of_day", "day_of_week"
    #                 )
    #
    # return(
    #   list(
    #     "copula_data_macro" = copula_data_macro,
    #     "lm_vars1" = lm_vars1
    #   )
    # )

  }

