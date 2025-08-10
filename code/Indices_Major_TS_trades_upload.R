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

major_indices <-
  get_all_major_indices(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame
  )

major_indices_bid <-
  get_all_major_indices(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame
  )

mean_values_by_asset_for_loop <-
  wrangle_asset_data(
    asset_data_daily_raw = major_indices,
    summarise_means = TRUE
  )

major_indices$Asset %>% unique()
gc()

control_random_samples <-
  get_random_samples_MLE_beta(
    random_results_db_location = "C:/Users/Nikhil Chandra/Documents/trade_data/random_results.db",
    stop_factor = 5,
    profit_factor = 10,
    analysis_syms = c("AU200_AUD", "SPX500_USD", "EU50_EUR", "US2000_USD"),
    time_frame = "H1",
    return_summary = TRUE
  )

major_indices_log_cumulative <-
  c("SPX500_USD", "US2000_USD", "NAS100_USD", "SG30_SGD", "AU200_AUD", "EU50_EUR", "DE30_EUR") %>%
  map_dfr(
    ~
      create_log_cumulative_returns(
        asset_data_to_use = major_indices,
        asset_to_use = c(.x[1]),
        price_col = "Open",
        return_long_format = TRUE
      )
  ) %>%
  left_join(
    major_indices %>% distinct(Date, Asset, Price, Open, High, Low)
  )

major_indices_log_cumulative_bid <-
  c("SPX500_USD", "US2000_USD", "NAS100_USD", "SG30_SGD", "AU200_AUD", "EU50_EUR", "DE30_EUR") %>%
  map_dfr(
    ~
      create_log_cumulative_returns(
        asset_data_to_use = major_indices_bid,
        asset_to_use = c(.x[1]),
        price_col = "Open",
        return_long_format = TRUE
      )
  ) %>%
  left_join(
    major_indices_bid %>% distinct(Date, Asset, Price, Open, High, Low)
  )

full_ts_trade_db_location = "C:/Users/Nikhil Chandra/Documents/trade_data/full_ts_trades_mapped.db"
full_ts_trade_db_con <- connect_db(full_ts_trade_db_location)

Indices_Trades_long <-
  major_indices_log_cumulative %>%
  filter(Date >= "2016-01-01") %>%
  mutate(
    trade_col = "Long"
  )

Indices_Long_Data_15_20 <-
  run_pairs_analysis(
    tagged_trades = Indices_Trades_long,
    stop_factor = 15,
    profit_factor = 20,
    raw_asset_data = major_indices_log_cumulative,
    risk_dollar_value = 10,
    return_trade_ts = TRUE
  ) %>%
  mutate(  stop_factor = 15,
           profit_factor = 20)

append_table_sql_lite(.data = Indices_Long_Data_15_20,
   table_name = "full_ts_trades_mapped",
   conn = full_ts_trade_db_con)

Indices_Long_Data_10_15 <-
  run_pairs_analysis(
    tagged_trades = Indices_Trades_long,
    stop_factor = 10,
    profit_factor = 15,
    raw_asset_data = major_indices_log_cumulative,
    risk_dollar_value = 10,
    return_trade_ts = TRUE
  ) %>%
  mutate(  stop_factor = 10,
           profit_factor = 15)

append_table_sql_lite(.data = Indices_Long_Data_10_15,
                      table_name = "full_ts_trades_mapped",
                      conn = full_ts_trade_db_con)

Indices_Long_Data_8_12 <-
  run_pairs_analysis(
    tagged_trades = Indices_Trades_long,
    stop_factor = 8,
    profit_factor = 12,
    raw_asset_data = major_indices_log_cumulative,
    risk_dollar_value = 10,
    return_trade_ts = TRUE
  ) %>%
  mutate(  stop_factor = 8,
           profit_factor = 12)

append_table_sql_lite(.data = Indices_Long_Data_8_12,
                      table_name = "full_ts_trades_mapped",
                      conn = full_ts_trade_db_con)

Indices_Long_Data_4_8 <-
  run_pairs_analysis(
    tagged_trades = Indices_Trades_long,
    stop_factor = 4,
    profit_factor = 8,
    raw_asset_data = major_indices_log_cumulative,
    risk_dollar_value = 10,
    return_trade_ts = TRUE
  ) %>%
  mutate(  stop_factor = 4,
           profit_factor = 8)

append_table_sql_lite(.data = Indices_Long_Data_4_8,
                      table_name = "full_ts_trades_mapped",
                      conn = full_ts_trade_db_con)


Indices_Long_Data_5_6 <-
  run_pairs_analysis(
    tagged_trades = Indices_Trades_long,
    stop_factor = 5,
    profit_factor = 6,
    raw_asset_data = major_indices_log_cumulative,
    risk_dollar_value = 10,
    return_trade_ts = TRUE
  ) %>%
  mutate(  stop_factor = 5,
           profit_factor = 6)

append_table_sql_lite(.data = Indices_Long_Data_5_6,
                      table_name = "full_ts_trades_mapped",
                      conn = full_ts_trade_db_con)


rm(Indices_Long_Data_15_20, Indices_Long_Data_10_15,
   Indices_Long_Data_4_8, Indices_Long_Data_8_12, Indices_Trades_long,
   Indices_Long_Data_5_6)
gc()
#-------------------------------------------------------
Indices_Trades_Short <-
  major_indices_log_cumulative_bid %>%
  filter(Date >= "2016-01-01") %>%
  mutate(
    trade_col = "Short"
  )

Indices_Short_Data_15_20 <-
  run_pairs_analysis(
    tagged_trades = Indices_Trades_Short,
    stop_factor = 15,
    profit_factor = 20,
    raw_asset_data = major_indices_log_cumulative_bid,
    risk_dollar_value = 10,
    return_trade_ts = TRUE
  ) %>%
  mutate(  stop_factor = 15,
           profit_factor = 20)

append_table_sql_lite(.data = Indices_Short_Data_15_20,
                      table_name = "full_ts_trades_mapped",
                      conn = full_ts_trade_db_con)

Indices_Short_Data_10_15 <-
  run_pairs_analysis(
    tagged_trades = Indices_Trades_Short,
    stop_factor = 10,
    profit_factor = 15,
    raw_asset_data = major_indices_log_cumulative_bid,
    risk_dollar_value = 10,
    return_trade_ts = TRUE
  ) %>%
  mutate(  stop_factor = 10,
           profit_factor = 15)

append_table_sql_lite(.data = Indices_Short_Data_10_15,
                      table_name = "full_ts_trades_mapped",
                      conn = full_ts_trade_db_con)

Indices_Short_Data_8_12 <-
  run_pairs_analysis(
    tagged_trades = Indices_Trades_Short,
    stop_factor = 8,
    profit_factor = 12,
    raw_asset_data = major_indices_log_cumulative_bid,
    risk_dollar_value = 10,
    return_trade_ts = TRUE
  ) %>%
  mutate(  stop_factor = 8,
           profit_factor = 12)

append_table_sql_lite(.data = Indices_Short_Data_8_12,
                      table_name = "full_ts_trades_mapped",
                      conn = full_ts_trade_db_con)

Indices_Short_Data_4_8 <-
  run_pairs_analysis(
    tagged_trades = Indices_Trades_Short,
    stop_factor = 4,
    profit_factor = 8,
    raw_asset_data = major_indices_log_cumulative_bid,
    risk_dollar_value = 10,
    return_trade_ts = TRUE
  ) %>%
  mutate(  stop_factor = 4,
           profit_factor = 8)

append_table_sql_lite(.data = Indices_Short_Data_4_8,
                      table_name = "full_ts_trades_mapped",
                      conn = full_ts_trade_db_con)

Indices_Short_Data_5_6 <-
  run_pairs_analysis(
    tagged_trades = Indices_Trades_Short,
    stop_factor = 5,
    profit_factor = 6,
    raw_asset_data = major_indices_log_cumulative_bid,
    risk_dollar_value = 10,
    return_trade_ts = TRUE
  ) %>%
  mutate(  stop_factor = 5,
           profit_factor = 6)

append_table_sql_lite(.data = Indices_Short_Data_5_6,
                      table_name = "full_ts_trades_mapped",
                      conn = full_ts_trade_db_con)


DBI::dbDisconnect(full_ts_trade_db_con)
rm(full_ts_trade_db_con)
gc()

rm(Indices_Short_Data_15_20, Indices_Short_Data_10_15,
   Indices_Short_Data_4_8, Indices_Short_Data_8_12, Indices_Trades_Short,
   Indices_Short_Data_5_6)
gc()





#--------------------------------------------
all_trade_ts_actuals <-
  get_Major_Indices_trade_ts_actuals(
    full_ts_trade_db_location = "C:/Users/Nikhil Chandra/Documents/trade_data/full_ts_trades_mapped.db",
    asset_infor = asset_infor,
    currency_conversion = currency_conversion,
    stop_factor_var = 4,
    profit_factor_var = 8
  )
longs = all_trade_ts_actuals[[1]]
shorts = all_trade_ts_actuals[[2]]
lag_days = 1
raw_macro_data = get_macro_event_data()
dependant_var_name = "SPX500_USD"
use_PCA_vars = TRUE
date_split_train = "2025-05-01"
train_sample = 9000000
trade_direction_var = "Long"
analysis_threshs = c(0.6, 0.7)

#-------------------------------------

date_sequence <-
  seq(as_date("2019-01-01"), as_date("2025-06-30"), "week") %>%
  as.character()
index_trade_sim_results_db <- "C:/Users/Nikhil Chandra/Documents/trade_data/Index_Trade_Sim_GLM.db"
index_trade_sim_results_db_con <- connect_db(path = index_trade_sim_results_db)

for (i in 138:length(date_sequence)) {

  SPX_results <-
    get_Indices_GLM_analysis(
      major_indices = major_indices,
      longs = all_trade_ts_actuals[[1]],
      shorts = all_trade_ts_actuals[[2]],
      lag_days = 1,
      raw_macro_data = raw_macro_data,
      dependant_var_name = "SPX500_USD",
      use_PCA_vars = FALSE,
      date_split_train = date_sequence[i],
      train_sample = train_sample,
      trade_direction_var = "Short",
      trade_direction_analysis = "Short",
      analysis_threshs = c(0.5, 0.4, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99, 0.999),
      lagged_vars_only = FALSE,
      split_padding = 0,
      max_date_in_analysis =
        as.character(as_date(date_sequence[i]) + dweeks(2))
    ) %>%
    mutate(
      training_last_date = date_sequence[i],
      testing_last_date = as.character(as_date(date_sequence[i]) + dweeks(2))
    ) %>%
    mutate(actual_return =
             Trades*(Perc*(win_amount) - (1-Perc)*loss_amount),
           outperformance =
             ifelse(risk_weighted_return > control_risk_return, 1, 0))

  US2000_results <-
    get_Indices_GLM_analysis(
      major_indices = major_indices,
      longs = all_trade_ts_actuals[[1]],
      shorts = all_trade_ts_actuals[[2]],
      lag_days = 1,
      raw_macro_data = raw_macro_data,
      dependant_var_name = "US2000_USD",
      use_PCA_vars = FALSE,
      date_split_train = date_sequence[i],
      train_sample = train_sample,
      trade_direction_var = "Short",
      trade_direction_analysis = "Short",
      lagged_vars_only = FALSE,
      analysis_threshs = c(0.5, 0.4, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99, 0.999),
      split_padding = 0,
      max_date_in_analysis =
        as.character(as_date(date_sequence[i]) + dweeks(2))
    ) %>%
    mutate(
      training_last_date = date_sequence[i],
      testing_last_date = as.character(as_date(date_sequence[i]) + dweeks(2))
    ) %>%
    mutate(actual_return =
             Trades*(Perc*(win_amount) - (1-Perc)*loss_amount),
           outperformance =
             ifelse(risk_weighted_return > control_risk_return, 1, 0))

  EU50_EUR_results <-
    get_Indices_GLM_analysis(
      major_indices = major_indices,
      longs = all_trade_ts_actuals[[1]],
      shorts = all_trade_ts_actuals[[2]],
      lag_days = 1,
      raw_macro_data = raw_macro_data,
      dependant_var_name = "EU50_EUR",
      use_PCA_vars = FALSE,
      date_split_train = date_sequence[i],
      train_sample = train_sample,
      trade_direction_var = "Short",
      trade_direction_analysis = "Short",
      lagged_vars_only = FALSE,
      analysis_threshs = c(0.5, 0.4, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99, 0.999),
      split_padding = 0,
      max_date_in_analysis =
        as.character(as_date(date_sequence[i]) + dweeks(2))
    ) %>%
    mutate(
      training_last_date = date_sequence[i],
      testing_last_date = as.character(as_date(date_sequence[i]) + dweeks(2))
    ) %>%
    mutate(actual_return =
             Trades*(Perc*(win_amount) - (1-Perc)*loss_amount),
           outperformance =
             ifelse(risk_weighted_return > control_risk_return, 1, 0))

  append_table_sql_lite(.data = SPX_results,
                       table_name = "Index_Trade_Sim_GLM",
                       conn = index_trade_sim_results_db_con)
  append_table_sql_lite(.data = EU50_EUR_results,
                        table_name = "Index_Trade_Sim_GLM",
                        conn = index_trade_sim_results_db_con)
  append_table_sql_lite(.data = US2000_results,
                        table_name = "Index_Trade_Sim_GLM",
                        conn = index_trade_sim_results_db_con)

}

trade_sim_results_from_db <-
  DBI::dbGetQuery(conn = index_trade_sim_results_db_con,
                  statement = "SELECT * FROM Index_Trade_Sim_GLM") %>%
  filter(trade_col == "Short" ) %>%
  group_by(Asset, trade_col, threshold) %>%
  summarise(
    Trades = mean(Trades, na.rm = T),

    actual_return_low = quantile(actual_return, 0.25, na.rm = T),
    actual_return = median(actual_return, na.rm = T),
    actual_return_High = quantile(actual_return, 0.75, na.rm = T),

    risk_weighted_return_low = quantile(risk_weighted_return, 0.25, na.rm = T),
    risk_weighted_return = median(risk_weighted_return, na.rm = T),
    risk_weighted_return_High = quantile(risk_weighted_return, 0.75, na.rm = T),
    outperformance = sum(outperformance, na.rm = T),
    total_sims = n()
  ) %>%
  mutate(
    outperformance = outperformance/total_sims
  )

trade_sim_results_from_db <-
  DBI::dbGetQuery(conn = index_trade_sim_results_db_con,
                  statement = "SELECT * FROM Index_Trade_Sim_GLM") %>%
  filter(trade_col == "Short" ) %>%
  mutate(Control_Wins = round(Perc_control*Total_control)) %>%
  group_by(Asset, trade_col, threshold) %>%
  summarise(Trades = sum(Trades),
            wins_losses = sum(wins_losses),
            win_amount = mean(win_amount),
            loss_amount = mean(loss_amount),
            control_trades = sum(Total_control),
            Control_Wins = sum(Control_Wins),
            outperformance = sum(outperformance, na.rm = T),
            total_sims = n() ) %>%
  mutate(Perc = wins_losses/Trades,
         Perc_Control = Control_Wins/control_trades,
         risk_weighted_return = Perc*(win_amount/loss_amount) - (1- Perc),
         risk_weighted_return_control = Perc_Control*(win_amount/loss_amount) - (1- Perc_Control)
  ) %>%
  mutate(
    outperformance = outperformance/total_sims
  )


#---------------------------------------------------------------------------

SPX_results <-
  get_Indices_GLM_analysis(
    major_indices = major_indices,
    longs = all_trade_ts_actuals[[1]],
    shorts = all_trade_ts_actuals[[2]],
    lag_days = 1,
    raw_macro_data = raw_macro_data,
    dependant_var_name = "SPX500_USD",
    use_PCA_vars = FALSE,
    date_split_train = date_split_train,
    train_sample = train_sample,
    trade_direction_var = "Long",
    trade_direction_analysis = "Long",
    analysis_threshs = c(0.5, 0.4, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99, 0.999),
    lagged_vars_only = FALSE
  )

US2000_results <-
  get_Indices_GLM_analysis(
    major_indices = major_indices,
    longs = all_trade_ts_actuals[[1]],
    shorts = all_trade_ts_actuals[[2]],
    lag_days = 1,
    raw_macro_data = raw_macro_data,
    dependant_var_name = "US2000_USD",
    use_PCA_vars = FALSE,
    date_split_train = date_split_train,
    train_sample = train_sample,
    trade_direction_var = "Long",
    trade_direction_analysis = "Long",
    lagged_vars_only = FALSE,
    analysis_threshs = c(0.5, 0.4, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99, 0.999)
  )

AU200_AUD_results <-
  get_Indices_GLM_analysis(
    major_indices = major_indices,
    longs = all_trade_ts_actuals[[1]],
    shorts = all_trade_ts_actuals[[2]],
    lag_days = 1,
    raw_macro_data = raw_macro_data,
    dependant_var_name = "AU200_AUD",
    use_PCA_vars = FALSE,
    date_split_train = date_split_train,
    train_sample = train_sample,
    trade_direction_var = "Long",
    trade_direction_analysis = "Long",
    lagged_vars_only = FALSE,
    analysis_threshs = c(0.5, 0.4, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99, 0.999)
  )

EU50_EUR_results <-
  get_Indices_GLM_analysis(
    major_indices = major_indices,
    longs = all_trade_ts_actuals[[1]],
    shorts = all_trade_ts_actuals[[2]],
    lag_days = 1,
    raw_macro_data = raw_macro_data,
    dependant_var_name = "EU50_EUR",
    use_PCA_vars = FALSE,
    date_split_train = date_split_train,
    train_sample = train_sample,
    trade_direction_var = "Long",
    trade_direction_analysis = "Long",
    lagged_vars_only = FALSE,
    analysis_threshs = c(0.5, 0.4, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99, 0.999)
  )

#---------------------------------------------------------------------------------
SPX_results_short <-
  get_Indices_GLM_analysis(
    major_indices = major_indices,
    longs = all_trade_ts_actuals[[1]],
    shorts = all_trade_ts_actuals[[2]],
    lag_days = 1,
    raw_macro_data = raw_macro_data,
    dependant_var_name = "SPX500_USD",
    use_PCA_vars = FALSE,
    date_split_train = date_split_train,
    train_sample = train_sample,
    trade_direction_var = "Short",
    trade_direction_analysis = "Short",
    analysis_threshs = c(0.5, 0.4, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99, 0.999),
    lagged_vars_only = FALSE
  )

US2000_results_short <-
  get_Indices_GLM_analysis(
    major_indices = major_indices,
    longs = all_trade_ts_actuals[[1]],
    shorts = all_trade_ts_actuals[[2]],
    lag_days = 1,
    raw_macro_data = raw_macro_data,
    dependant_var_name = "US2000_USD",
    use_PCA_vars = FALSE,
    date_split_train = date_split_train,
    train_sample = train_sample,
    trade_direction_var = "Short",
    trade_direction_analysis = "Short",
    lagged_vars_only = FALSE,
    analysis_threshs = c(0.5, 0.4, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99, 0.999)
  )

AU200_AUD_results_short <-
  get_Indices_GLM_analysis(
    major_indices = major_indices,
    longs = all_trade_ts_actuals[[1]],
    shorts = all_trade_ts_actuals[[2]],
    lag_days = 1,
    raw_macro_data = raw_macro_data,
    dependant_var_name = "AU200_AUD",
    use_PCA_vars = FALSE,
    date_split_train = date_split_train,
    train_sample = train_sample,
    trade_direction_var = "Short",
    trade_direction_analysis = "Short",
    lagged_vars_only = FALSE,
    analysis_threshs = c(0.5, 0.4, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99, 0.999)
  )

EU50_EUR_results_short <-
  get_Indices_GLM_analysis(
    major_indices = major_indices,
    longs = all_trade_ts_actuals[[1]],
    shorts = all_trade_ts_actuals[[2]],
    lag_days = 1,
    raw_macro_data = raw_macro_data,
    dependant_var_name = "EU50_EUR",
    use_PCA_vars = FALSE,
    date_split_train = date_split_train,
    train_sample = train_sample,
    trade_direction_var = "Short",
    trade_direction_analysis = "Short",
    lagged_vars_only = FALSE,
    analysis_threshs = c(0.5, 0.4, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99, 0.999)
  )

#---------------------------------------How to execute the trades


