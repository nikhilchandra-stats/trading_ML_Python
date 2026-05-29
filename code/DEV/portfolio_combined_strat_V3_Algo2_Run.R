helpeR::load_custom_functions()

all_aud_symbols <- get_oanda_symbols() %>%
  keep(~ str_detect(.x, "AUD")|str_detect(.x, "USD_SEK|USD_NOK|USD_HUF|USD_ZAR|USD_CNY|USD_MXN"))
asset_infor <- get_instrument_info()
aud_assets <- read_all_asset_data_intra_day(
  asset_list_oanda = all_aud_symbols,
  save_path_oanda_assets = "D:/trade_data//oanda_data/",
  read_csv_or_API = "API",
  time_frame = "D",
  bid_or_ask = "bid",
  how_far_back = 10,
  start_date = (today() - days(7)) %>% as.character()
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
    "UK100_GBP", "NZD_USD",
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
db_location = "D:/Asset Data/Oanda_Asset_Data_Most_Assets_2025-09-13.db"
start_date = "2019-01-01"
end_date = today() %>% as.character()

bin_factor = NULL
stop_value_var = 10
profit_value_var = 50
period_var = 50

Indices_Metals_Bonds <- list()

assets_to_port <-
  c(    "XAG_USD", #18
        "HK33_HKD", #22
        "FR40_EUR", #23
        "BTC_USD", #24
        "NATGAS_USD", #32
        "JP225Y_JPY",
        "XAU_USD"
  ) %>% unique()

Indices_Metals_Bonds[[1]] <-
  get_db_data_quickly_algo(
    db_location = db_location,
    start_date = start_date,
    end_date = as.character(today() + days(30)),
    time_frame = "H1",
    bid_or_ask = "ask",
    assets = assets_to_port
  ) %>%
  distinct()
Indices_Metals_Bonds[[2]] <-
  get_db_data_quickly_algo(
    db_location = db_location,
    start_date = start_date,
    end_date = as.character(today() + days(30)),
    time_frame = "H1",
    bid_or_ask = "bid",
    assets =  assets_to_port
  ) %>%
  distinct()

Indices_Metals_Bonds[[1]] <- Indices_Metals_Bonds[[1]] %>% filter(Date >= "2019-01-01")
Indices_Metals_Bonds[[2]] <- Indices_Metals_Bonds[[2]] %>% filter(Date >= "2019-01-01")

stop_factor_var =5
profit_factor_var =10
risk_dollar_value_var = 10
end_period = 24
trade_direction = "Long"
end_point_loss = -7.5
end_point_profit = 15

tictoc::tic()
all_preds <-
  Portfolio_get_all_preds_frm_V3(
    Indices_Metals_Bonds = Indices_Metals_Bonds,
    raw_macro_data = raw_macro_data,
    base_path = "D:/trade_data/Day_Trader_Single_Asset_V3_Expanded_Models/",
    actuals_periods_needed = c("period_return_50_Price"),
    state_space_periods = c(20, 40, 60, 100, 200,300, 400,  500),
    state_space_rolling = c(100, 200, 300, 400),
    date_for_true_simualtion = "2019-01-01",
    training_end_date = "2021-01-01",
    assets_to_test = assets_to_port
  )

all_cor_V3_Data <-
  portfolio_get_V3_cor_data_TOTAL_SUMMED(
    Indices_Metals_Bonds = Indices_Metals_Bonds,
    all_preds = all_preds,
    assets_to_port = assets_to_port,
    low_to_price_lengths = c(200, 400),
    cor_periods = c(200, 100),
    max_regs = 1000
  )
tictoc::toc()

all_dates_sim <-
  all_cor_V3_Data[[1]] %>%
  filter(Date >= as_datetime("2020-01-01") + dhours(15000) ) %>%
  pull(Date) %>%
  unique()

sim_list <- list()
db_sim_results_con <- connect_db("D:/trade_data/db_sim_results_Algo_V3.db")
redo_db <- TRUE

for (i in 3805:(length(all_dates_sim) - 1) ) {

  tictoc::tic()
  portfolio_data <-
    get_portfolio_model_fast_summed(
      asset_data = Indices_Metals_Bonds %>%
        map( ~ .x %>% filter( Date <= all_dates_sim[i + 1] ) ),
      asset_of_interest = assets_to_port,
      stop_factor_var = stop_factor_var,
      profit_factor_var = profit_factor_var,
      risk_dollar_value_var = risk_dollar_value_var,
      end_period = end_period,
      time_frame = "H1",
      trade_direction = trade_direction,
      currency_conversion = currency_conversion,
      asset_infor = asset_infor,
      end_point_loss = end_point_loss,
      end_point_profit = end_point_profit,
      sum_as_portfolio = TRUE
    ) %>%
    group_by(Date, end_point_loss , end_point_profit, risk_dollar_value, stop_factor, profit_factor) %>%
    summarise(Final_Return = sum(Final_Return, na.rm = T)) %>%
    ungroup()

  temp_reg_data <-
    portfolio_get_V3_cor_data_TOTAL_SUMMED_reg_dat(
      all_dat_pivoted  = all_cor_V3_Data[[1]],
      correlation_data = all_cor_V3_Data[[2]],
      portfolio_data = portfolio_data,
      additional_cor_vars = list(
        c("XAG_USD_state_space_LM_Pred_period_return_50_Price", "XAU_USD_state_space_LM_Pred_period_return_50_Price", "cor_LM1"),
        c("HK33_HKD_state_space_LM_Pred_period_return_50_Price", "XAU_USD_state_space_LM_Pred_period_return_50_Price", "cor_LM2"),
        c("FR40_EUR_state_space_LM_Pred_period_return_50_Price", "XAU_USD_state_space_LM_Pred_period_return_50_Price", "cor_LM3"),
        c("BTC_USD_state_space_LM_Pred_period_return_50_Price", "XAU_USD_state_space_LM_Pred_period_return_50_Price", "cor_LM4"),
        c("FR40_EUR_state_space_LM_Pred_period_return_50_Price", "HK33_HKD_state_space_LM_Pred_period_return_50_Price", "cor_LM5"),

        c("XAG_USD_AR_LM_Pred_period_return_50_Price", "XAU_USD_AR_LM_Pred_period_return_50_Price", "cor_LM1_AR"),
        c("HK33_HKD_AR_LM_Pred_period_return_50_Price", "XAU_USD_AR_LM_Pred_period_return_50_Price", "cor_LM2_AR"),
        c("FR40_EUR_AR_LM_Pred_period_return_50_Price", "XAU_USD_AR_LM_Pred_period_return_50_Price", "cor_LM3_AR"),
        c("BTC_USD_AR_LM_Pred_period_return_50_Price", "XAU_USD_AR_LM_Pred_period_return_50_Price", "cor_LM4_AR"),
        c("FR40_EUR_AR_LM_Pred_period_return_50_Price", "HK33_HKD_AR_LM_Pred_period_return_50_Price", "cor_LM5_AR"),

        c("XAG_USD_AR_LM_Pred_period_return_50_Price", "XAG_USD_state_space_LM_Pred_period_return_50_Price", "cor_LM1_AR_SS"),
        c("HK33_HKD_AR_LM_Pred_period_return_50_Price", "HK33_HKD_state_space_LM_Pred_period_return_50_Price", "cor_LM2_AR_SS"),
        c("FR40_EUR_AR_LM_Pred_period_return_50_Price", "FR40_EUR_state_space_LM_Pred_period_return_50_Price", "cor_LM3_AR_SS"),
        c("BTC_USD_AR_LM_Pred_period_return_50_Price", "BTC_USD_state_space_LM_Pred_period_return_50_Price", "cor_LM4_AR_SS"),
        c("FR40_EUR_AR_LM_Pred_period_return_50_Price", "FR40_EUR_state_space_LM_Pred_period_return_50_Price", "cor_LM5_AR_SS")
        )
    )

  results_temp <-
    Porfolio_get_V3_LM_Model_TOTAL_SUM(
      reg_dat = temp_reg_data,
      reg_vars = c(all_cor_V3_Data[[3]],
                   "cor_LM1", "cor_LM2", "cor_LM3", "cor_LM4", "cor_LM5",
                   "cor_LM1_AR", "cor_LM2_AR", "cor_LM3_AR", "cor_LM4_AR", "cor_LM5_AR",
                   "cor_LM1_AR_SS", "cor_LM2_AR_SS", "cor_LM3_AR_SS", "cor_LM4_AR_SS", "cor_LM5_AR_SS",
                   "Final_Return_lag_error", "Final_Return_lag" ,"Final_Return_lag_2",
                   "Final_Return_lag_3", "Final_Return_lag_4"
                   # "Final_Return_lag_error_ma_100"
                   ),
      training_end_date = all_dates_sim[i],
      regression_length = 10000,
      dependant_var = "Final_Return",
      sig_thresh_LM = 1
    ) %>%
    dplyr::select(Date,
                  # Asset,
                  Final_Return,
                  predicted_10000 = predicted,
                  trained_mean_10000 = trained_mean,
                  trained_sd_10000 = trained_sd) %>%
    filter(Date > all_dates_sim[i], Date <= all_dates_sim[i + 1])

  results_temp2 <-
    Porfolio_get_V3_LM_Model_TOTAL_SUM(
      reg_dat = temp_reg_data,
      reg_vars = c(all_cor_V3_Data[[3]],
                   "cor_LM1", "cor_LM2", "cor_LM3", "cor_LM4", "cor_LM5",
                   "cor_LM1_AR", "cor_LM2_AR", "cor_LM3_AR", "cor_LM4_AR", "cor_LM5_AR",
                   "cor_LM1_AR_SS", "cor_LM2_AR_SS", "cor_LM3_AR_SS", "cor_LM4_AR_SS", "cor_LM5_AR_SS",
                   "Final_Return_lag_error", "Final_Return_lag" ,"Final_Return_lag_2",
                   "Final_Return_lag_3", "Final_Return_lag_4"
                   # "Final_Return_lag_error_ma_100"
                   ),
      training_end_date = all_dates_sim[i],
      regression_length = 5000,
      dependant_var = "Final_Return",
      sig_thresh_LM = 1
    ) %>%
    dplyr::select(Date,
                  # Asset,
                  # Final_Return,
                  predicted_5000 = predicted,
                  trained_mean_5000 = trained_mean,
                  trained_sd_5000 = trained_sd) %>%
    filter(Date > all_dates_sim[i], Date <= all_dates_sim[i + 1])

  results_temp3 <-
    Porfolio_get_V3_LM_Model_TOTAL_SUM(
      reg_dat = temp_reg_data,
      reg_vars = c(all_cor_V3_Data[[3]],
                   "cor_LM1", "cor_LM2", "cor_LM3", "cor_LM4", "cor_LM5",
                   "cor_LM1_AR", "cor_LM2_AR", "cor_LM3_AR", "cor_LM4_AR", "cor_LM5_AR",
                   "cor_LM1_AR_SS", "cor_LM2_AR_SS", "cor_LM3_AR_SS", "cor_LM4_AR_SS", "cor_LM5_AR_SS",
                   "Final_Return_lag_error", "Final_Return_lag" ,"Final_Return_lag_2",
                   "Final_Return_lag_3", "Final_Return_lag_4"
                   # "Final_Return_lag_error_ma_100"
                   ),
      training_end_date = all_dates_sim[i],
      regression_length = 2500,
      dependant_var = "Final_Return",
      sig_thresh_LM = 1
    ) %>%
    dplyr::select(Date,
                  # Asset,
                  # Final_Return,
                  predicted_2500 = predicted,
                  trained_mean_2500 = trained_mean,
                  trained_sd_2500 = trained_sd) %>%
    filter(Date > all_dates_sim[i], Date <= all_dates_sim[i + 1])

  results_temp4 <-
    Porfolio_get_V3_LM_Model_TOTAL_SUM(
      reg_dat = temp_reg_data,
      reg_vars = c(all_cor_V3_Data[[3]],
                   "cor_LM1", "cor_LM2", "cor_LM3", "cor_LM4", "cor_LM5",
                   "cor_LM1_AR", "cor_LM2_AR", "cor_LM3_AR", "cor_LM4_AR", "cor_LM5_AR",
                   "cor_LM1_AR_SS", "cor_LM2_AR_SS", "cor_LM3_AR_SS", "cor_LM4_AR_SS", "cor_LM5_AR_SS",
                   "Final_Return_lag_error", "Final_Return_lag" ,"Final_Return_lag_2",
                   "Final_Return_lag_3", "Final_Return_lag_4"
                   # "Final_Return_lag_error_ma_100"
                   ),
      training_end_date = all_dates_sim[i],
      regression_length = 1500,
      dependant_var = "Final_Return",
      sig_thresh_LM = 1
    ) %>%
    dplyr::select(Date,
                  # Asset,
                  # Final_Return,
                  predicted_1500 = predicted,
                  trained_mean_1500 = trained_mean,
                  trained_sd_1500 = trained_sd) %>%
    filter(Date > all_dates_sim[i], Date <= all_dates_sim[i + 1])

  results_temp <-
    results_temp %>%
    left_join(results_temp2) %>%
    left_join(results_temp3)%>%
    left_join(results_temp4)
  tictoc::toc()

  sim_list[[i]] <- results_temp


  if(i == 1 & redo_db == TRUE) {
    write_table_sql_lite(.data = results_temp,
                         table_name = "db_sim_results_Algo2",
                         conn = db_sim_results_con,
                         overwrite_true = TRUE)
  } else {
    append_table_sql_lite(.data = results_temp,
                          table_name = "db_sim_results_Algo2",
                          conn = db_sim_results_con)
  }
}

actual_final_returns <-
  get_portfolio_model_fast_summed(
    asset_data = Indices_Metals_Bonds,
    asset_of_interest = assets_to_port,
    stop_factor_var = stop_factor_var,
    profit_factor_var = profit_factor_var,
    risk_dollar_value_var = risk_dollar_value_var,
    end_period = end_period,
    time_frame = "H1",
    trade_direction = trade_direction,
    currency_conversion = currency_conversion,
    asset_infor = asset_infor,
    end_point_loss = end_point_loss,
    end_point_profit = end_point_profit,
    sum_as_portfolio = TRUE
  ) %>%
  group_by(Date) %>%
  summarise(Final_Return = sum(Final_Return, na.rm = T))

model_prediction_data <-
  DBI::dbGetQuery(conn = db_sim_results_con,
                  statement = "SELECT * FROM db_sim_results_Algo2") %>%
  mutate(Date = as_datetime(Date, tz = "Australia/Canberra")) %>%
  mutate(
    Averaged_Pred =
      (predicted_10000 + predicted_5000 + predicted_2500 + predicted_1500)/4
  ) %>%
  dplyr::select(-Final_Return) %>%
  left_join(actual_final_returns %>%  dplyr::select(Date, Final_Return)) %>%
  ungroup() %>%
  arrange(Date) %>%
  mutate(
    pred_10000_mean_roll_250 =
      slider::slide_dbl(.x  = predicted_10000, .f = ~ mean(.x, na.rm = T), .before = 250),
    pred_5000_mean_roll_250 =
      slider::slide_dbl(.x  = predicted_5000, .f = ~ mean(.x, na.rm = T), .before = 250),
    portfolio_pred_2500_mean_roll_250 =
      slider::slide_dbl(.x  = predicted_2500, .f = ~ mean(.x, na.rm = T), .before = 250),
    portfolio_pred_1500_mean_roll_250 =
      slider::slide_dbl(.x  = predicted_1500, .f = ~ mean(.x, na.rm = T), .before = 250),

    pred_10000_sd_roll_250 =
      slider::slide_dbl(.x  = predicted_10000, .f = ~ sd(.x, na.rm = T), .before = 250),
    pred_5000_sd_roll_250 =
      slider::slide_dbl(.x  = predicted_5000, .f = ~ sd(.x, na.rm = T), .before = 250),
    portfolio_pred_2500_sd_roll_250 =
      slider::slide_dbl(.x  = predicted_2500, .f = ~ sd(.x, na.rm = T), .before = 250),
    portfolio_pred_1500_sd_roll_250 =
      slider::slide_dbl(.x  = predicted_1500, .f = ~ sd(.x, na.rm = T), .before = 250)

  ) %>%
  ungroup()

model_prediction_data %>%
  pull(Date) %>% max()

which(all_dates_sim == max(model_prediction_data$Date, na.rm = T))

trade_statment <-
  "
   (predicted_5000 < trained_mean_5000 - 0.5*trained_sd_5000)|
   (predicted_10000 < trained_mean_10000 - 0.5*trained_sd_10000)
"

trade_statment <-
  "pred_5000_mean_roll_250 < 6.75"

trade_statment <-
  "pred_10000_mean_roll_250 < -5"

trade_statment <-
  "
  (predicted_2500 > 0 & predicted_10000 < 0)|
   (predicted_2500 > trained_mean_2500 + 1*trained_sd_2500)|
   (pred_5000_mean_roll_250 > 0 & predicted_10000 < 0)
"

trade_statment <-
  "pred_10000_sd_roll_250 > 17"

trade_statment <-
  "pred_5000_sd_roll_250 > 10"


analyse_performance <-
  model_prediction_data %>%
  mutate(
    trade_col = eval(parse(text = trade_statment))
  ) %>%
  mutate(
    trade_col = case_when(trade_col == TRUE ~ "Long", TRUE ~ "No Trade")
  )

analyse_performance <-
  analyse_performance %>%
  filter(trade_col == "Long") %>%
  group_by(Date) %>%
  summarise(Final_Return = sum(Final_Return)) %>%
  ungroup() %>%
  arrange(Date) %>%
  mutate(Final_Return_Cumulative = cumsum(Final_Return))

analyse_performance %>%
  ggplot(aes(x = Date, y = Final_Return_Cumulative)) +
  geom_line() +
  theme_minimal()

analyse_performance_sum <-
  model_prediction_data %>%
  mutate(
    trade_col = eval(parse(text = trade_statment))
  ) %>%
  mutate(
    trade_col = case_when(trade_col == TRUE ~ "Long", TRUE ~ "No Trade")
  ) %>%
  mutate(
    pos_detect_TRUE =
      ifelse(trade_col == "Long" & Final_Return > 0, 1, 0),
    pos_detect_Ned =
      ifelse(trade_col == "Long" & Final_Return <= 0, 1, 0),

    neg_detect_TRUE =
      ifelse(trade_col == "No Trade" & Final_Return <= 0, 1, 0),
    neg_detect_Ned =
      ifelse(trade_col == "No Trade" & Final_Return > 0, 1, 0)

  ) %>%
  summarise(
    TRUE_pos_rate = sum(pos_detect_TRUE, na.rm=T)/( sum(pos_detect_TRUE, na.rm = T) + sum(pos_detect_Ned, na.rm = T) ),
    TRUE_neg_rate = sum(neg_detect_TRUE, na.rm = T)/( sum(neg_detect_TRUE, na.rm = T) + sum(neg_detect_Ned, na.rm = T))
  )

