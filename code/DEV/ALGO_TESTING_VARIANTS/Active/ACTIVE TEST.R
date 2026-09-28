helpeR::load_custom_functions()

all_aud_symbols <- get_oanda_symbols() %>%
  keep(~ str_detect(.x, "AUD")|str_detect(.x, "USD_SEK|USD_NOK|USD_HUF|USD_ZAR|USD_CNY|USD_MXN"))
asset_infor <- get_instrument_info()
aud_assets <- read_all_asset_data_intra_day(
  asset_list_oanda = all_aud_symbols,
  save_path_oanda_assets = "C:/Users/nikhi/Documents//trade_data//oanda_data/",
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
#---------------------Data
load_custom_functions()
db_location = "C:/Users/nikhi/Documents//Asset Data/Oanda_Asset_Data_Most_Assets_2025-09-13.db"

tictoc::tic()
model_predicted_data_raw <-
  portfolio_no_V3_New_algo_pnorm_version(
    assets_to_port =
      c(
        "SPX500_USD",
        "CH20_CHF",
        "DE30_EUR",
        "EU50_EUR",
        "US2000_USD",
        "HK33_HKD",
        "JP225Y_JPY",
        "UK100_GBP"
      ) %>% unique(),
    stop_factor_var = 3,
    currency_conversion = currency_conversion,
    asset_infor = asset_infor,
    db_location = db_location,
    start_date = "2021-11-01",
    profit_factor_var = 6,
    risk_dollar_value_var = 5,
    end_period = 132,
    trade_direction = "Long",
    end_point_loss = -5,
    end_point_profit = 10,
    regression_length = 25000,
    direct_return_cols = 24,
    lag_value_error = 132 + 1,
    low_to_price_lengths = c(400),
    cor_period = c(50),
    dependant_var = "Final_Return",
    save_location = "C:/Users/nikhi/Documents//trade_data/Day_Trader_Cor_Continuous_Models/",
    file_name = "FAST_EQUITY_EXPNDED_ONLY_FAST_NON_V3",
    training_date = "2022-02-01",
    testing_date ="2021-11-01",

    xtnd_ss_cols_PR_cols = c(1,5,10,15, 20, 25, 30,35, 40,45, 50,55, 60,65, 70, 75,80, 90, 100,110, 120, 130),
    xtnd_ss_cols_BR_periods = c(100,200,300, 50, 150, 250, 350, 25, 500),
    lag_dependant = 132 + 1,
    auto_cor_cols = 20,
    cor_skip_periods = c(1,2,4,5,6,8,10),
    periods_to_use_deviation = c(1,10,20,30,40,50, 60, 70, 80, 90, 100),
    mean_periods_deviation = c(50,75 ,100),

    estimate_trades = FALSE,
    trade_statement = NULL
  )
tictoc::toc()

model_predicted_data <-
  model_predicted_data_raw


# Trade Statement ---------------------------------------------------------

trade_statment <-
  "
  # (pnorm_1001_port > 0.7)|
  (pnorm_250_roll_250_port > 0.64 & pnorm_250_roll_250_port < 1 & Asset == 'CH20_CHF')|
  (pnorm_250_roll_250_port > 0.525 & pnorm_250_roll_250_port < 1 & Asset == 'DE30_EUR')|
  (pnorm_250_roll_250_port > 0.50 & pnorm_250_roll_250_port < 1 & Asset == 'EU50_EUR')|
  (pnorm_250_roll_250_port > 0.57 & pnorm_250_roll_250_port < 1 & Asset == 'HK33_HKD')|
  (pnorm_250_roll_250_port > 0.525 & pnorm_250_roll_250_port < 1 & Asset == 'JP225Y_JPY')|
  (pnorm_250_roll_250_port > 0.54 & pnorm_250_roll_250_port < 1 & Asset == 'SPX500_USD')|
  (pnorm_250_roll_250_port > 0.525 & pnorm_250_roll_250_port < 1 & Asset == 'UK100_GBP')|
  (pnorm_250_roll_250_port > 0.55 & pnorm_250_roll_250_port < 0.6 & Asset == 'US2000_USD')
"

analyse_performance <-
  model_predicted_data %>%
  # filter(Asset == "US2000_USD") %>%
  mutate(
    trade_col = eval(parse(text = trade_statment))
  ) %>%
  mutate(
    trade_col = case_when(trade_col == TRUE ~ "Long", TRUE ~ "No Trade")
  )

control <-
  analyse_performance %>%
  group_by(Date) %>%
  summarise(Final_Return = sum(Final_Return)) %>%
  ungroup() %>%
  arrange(Date) %>%
  mutate(Final_Return_Cumulative = cumsum(Final_Return)) %>%
  mutate(trade_col = "Control")

analyse_performance <-
  analyse_performance %>%
  # filter(Date >= '2026-09-07') %>%
  filter(trade_col == "Long") %>%
  group_by(Date) %>%
  summarise(Final_Return = sum(Final_Return)) %>%
  ungroup() %>%
  arrange(Date) %>%
  mutate(Final_Return_Cumulative = cumsum(Final_Return)) %>%
  mutate(trade_col = "Long")

dim(control)[1]
dim(analyse_performance)[1]


analyse_performance %>%
  bind_rows(control) %>%
  ggplot(aes(x = Date, y = Final_Return_Cumulative
             ,color = trade_col
  )) +
  geom_line() +
  geom_hline(yintercept = 0, linetype = "dashed", color = 'darkred') +
  facet_wrap(.~trade_col, scales = "free") +
  theme_minimal() +
  scale_y_continuous(n.breaks = 20) +
  theme(legend.position = "bottom")

upload_results_con <-
  connect_db("C:/Users/nikhi/Documents/trade_data/real_results_new_algo.db")

write_table_sql_lite(.data = analyse_performance,
                     conn = upload_results_con,
                     table_name = "real_results_new_algo")

DBI::dbDisconnect(upload_results_con)
rm(upload_results_con)


tictoc::tic()
model_predicted_data_raw <-
  portfolio_no_V3_New_algo_pnorm_version(
    assets_to_port =
      c(
        "XAU_USD",
        "BTC_USD",
        "XCU_USD",
        "USD_JPY",
        "NAS100_USD"
      ) %>% unique(),
    stop_factor_var = 10,
    currency_conversion = currency_conversion,
    asset_infor = asset_infor,
    db_location = db_location,
    start_date = "2021-11-01",
    profit_factor_var = 100,
    risk_dollar_value_var = 5,
    end_period = 132,
    trade_direction = "Long",
    end_point_loss = -5,
    end_point_profit = 50,
    regression_length = 25000,
    direct_return_cols = 24,
    lag_value_error = 132 + 1,
    low_to_price_lengths = c(400),
    cor_period = c(50),
    dependant_var = "Final_Return",
    save_location = "C:/Users/nikhi/Documents//trade_data/Day_Trader_Cor_Continuous_Models/",
    file_name = "MIXED_NON_V3_NEW_MODEL",
    training_date = "2022-02-01",
    testing_date ="2021-11-01",
    xtnd_ss_cols_PR_cols = c(1,5,10,15, 20, 25, 30,35, 40,45, 50,55, 60,65, 70, 75,80, 90, 100,110, 120, 130),
    xtnd_ss_cols_BR_periods = c(100,200,300, 50, 150, 250, 350, 25, 500),
    lag_dependant = 132 + 1,
    auto_cor_cols = 20,
    cor_skip_periods = c(1,2,4,5,6,8,10),
    periods_to_use_deviation = c(1,10,20,30,40,50, 60, 70, 80, 90, 100),
    mean_periods_deviation = c(50,75 ,100),
    run_additional_calc = TRUE,

    estimate_trades = FALSE,
    trade_statement = NULL
  )
tictoc::toc()

model_predicted_data <-
  model_predicted_data_raw

col_to_test <- "pred_10000_mean_roll_500"
col_to_test_mean <- "pred_10000_mean_roll_2000"
col_to_test_sd <- "pred_10000_sd_roll_2000"

names(model_predicted_data)
trade_statment <-
  "
  (pnorm_error_5_rate_50 > 0.525 & pnorm_error_5_rate_50 <= 1 &
  pnorm_2000_mean_1000_roll_2000_port > 0.525)|
  (pnorm_error_5_rate_100 > 0.525 & pnorm_error_5_rate_100 < 1 &
  pnorm_2000_mean_1000_roll_2000_port > 0.525 & pnorm_2000_mean_1000_roll_2000_port < 1)|
  (pnorm_error_200_rate_100 > 0.55 & pnorm_error_200_rate_100 > 0 &
  pnorm_2000_mean_1000_roll_2000_port > 0.55)|
  (pnorm_error_5_rate_100 > 0.5 & pnorm_error_5_rate_100 < 1 &
  pnorm_1500_mean_10_port > 0.7)|
  (pnorm_error_20_rate_50 > 0.55 & pnorm_error_20_rate_50 < 0.95 &
  pnorm_1500_mean_10_port > 0.55)|
  (pnorm_error_20_rate_50 > 0.55 & pnorm_error_20_rate_50 < 0.95 &
  pnorm_1001_port > 0.55)|
  (pnorm_error_5_rate_50 > 0.55 & pnorm_error_5_rate_50 < 0.925 &
  pnorm_1001_port > 0.525)|
  (pnorm_error_5_rate_50 > 0.55 & pnorm_error_5_rate_50 < 0.9125 &
  pnorm_1001_port > 0.5)|
  (pnorm_error_5_rate_50 > 0.6 & pnorm_error_5_rate_50 < 0.9125 &
  pnorm_2000_mean_1000_roll_2000_port > 0.5)|
  (rolling_error_var_pnorm > 0.85 & pnorm_error_200_rate_50 > 0.85)
"

analyse_performance <-
  model_predicted_data %>%
  # filter(Asset == "BTC_USD") %>%
  group_by(Asset) %>%
  mutate(
    trade_col = eval(parse(text = trade_statment))
  ) %>%
  ungroup() %>%
  mutate(
    trade_col = case_when(trade_col == TRUE ~ "Long", TRUE ~ "No Trade")
  )

control <-
  analyse_performance %>%
  group_by(Date) %>%
  summarise(Final_Return = sum(Final_Return)) %>%
  ungroup() %>%
  arrange(Date) %>%
  mutate(Final_Return_Cumulative = cumsum(Final_Return)) %>%
  mutate(trade_col = "Control")

analyse_performance <-
  analyse_performance %>%
  filter(Date >= "2026-09-07") %>%
  filter(trade_col == "Long") %>%
  group_by(Date) %>%
  summarise(Final_Return = sum(Final_Return)) %>%
  ungroup() %>%
  arrange(Date) %>%
  mutate(Final_Return_Cumulative = cumsum(Final_Return)) %>%
  mutate(trade_col = "Long")

dim(control)[1]
dim(analyse_performance)[1]


analyse_performance %>%
  bind_rows(control) %>%
  # filter(Date <= "2023-01-01") %>%
  ggplot(aes(x = Date, y = Final_Return_Cumulative
             ,color = trade_col
  )) +
  geom_line() +
  geom_hline(yintercept = 0, linetype = "dashed", color = 'darkred') +
  facet_wrap(.~trade_col, scales = "free") +
  theme_minimal() +
  scale_y_continuous(n.breaks = 20) +
  theme(legend.position = "bottom")

upload_results_con <-
  connect_db("C:/Users/nikhi/Documents/trade_data/real_results_new_algo.db")

append_table_sql_lite(.data = analyse_performance,
                      conn = upload_results_con,
                      table_name = "real_results_new_algo")

DBI::dbDisconnect(upload_results_con)
rm(upload_results_con)


tictoc::tic()
model_predicted_data_raw <-
  portfolio_no_V3_New_algo_pnorm_version(
    assets_to_port =
      c(
        "NZD_USD",
        "AUD_CAD",
        "AUD_USD",
        "AUD_NZD",
        "AUD_CHF"
      ) %>% unique(),
    stop_factor_var = 10,
    currency_conversion = currency_conversion,
    asset_infor = asset_infor,
    db_location = db_location,
    start_date = "2021-11-01",
    profit_factor_var = 15,
    risk_dollar_value_var = 5,
    end_period = 132,
    trade_direction = "Long",
    end_point_loss = -5,
    end_point_profit = 7.25,
    regression_length = 25000,
    direct_return_cols = 24,
    lag_value_error = 132 + 1,
    low_to_price_lengths = c(400),
    cor_period = c(50),
    dependant_var = "Final_Return",
    save_location = "C:/Users/nikhi/Documents//trade_data/Day_Trader_Cor_Continuous_Models/",
    # file_name = "EQUITY_EXPNDED_ONLY_NON_V3_NEW_MODEL",
    file_name = "AUD_ONLY_STOP_10_PROF_15_NON_V3_NEW_MODEL",
    training_date = "2022-02-01",
    testing_date ="2021-11-01",
    xtnd_ss_cols_PR_cols = c(1,5,10,15, 20, 25, 30,35, 40,45, 50,55, 60,65, 70, 75,80, 90, 100,110, 120, 130),
    xtnd_ss_cols_BR_periods = c(100,200,300,400 ,50, 150, 250, 350, 25, 500),
    lag_dependant = 132 + 1,
    auto_cor_cols = 10,
    cor_skip_periods = c(1,2,4,5),
    periods_to_use_deviation = c(1,10,15,20,30,40,50, 60, 70, 80, 90, 100),
    mean_periods_deviation = c(25,50,75,100, 150, 200, 250),

    run_additional_calc = TRUE,
    estimate_trades = FALSE,
    trade_statement = NULL
  )
tictoc::toc()

model_predicted_data <-
  model_predicted_data_raw %>%
  mutate(
    pnorm_50 = pcauchy(predicted, location = pred_10000_mean_roll_50, scale = pred_10000_sd_roll_50),
    pnorm_50_roll_50 = slider::slide_dbl(.x  = pnorm_50, .f = ~ mean(.x, na.rm = T), .before = 50),

    pnorm_50_port = pcauchy(predicted_portfolio, location = pred_portfolio_10000_mean_roll_50, scale = pred_portfolio_10000_sd_roll_50),
    pnorm_50_roll_50_port = slider::slide_dbl(.x  = pnorm_50_port, .f = ~ mean(.x, na.rm = T), .before = 50),

    pnorm_1001_port_roll_100 =
      slider::slide_dbl(.x  = pnorm_1001_port, .f = ~ mean(.x, na.rm = T), .before = 100),

  )


# Trade Statement ---------------------------------------------------------
names(model_predicted_data)
col_to_test <- "predicted_portfolio"
col_to_test_mean <- "pred_portfolio_10000_mean_roll_2000"
col_to_test_sd <- "pred_portfolio_10000_sd_roll_2000"

trade_statment <-
  "
  (rolling_error_var_pnorm > 0.99 & Asset == 'AUD_USD')|
  (rolling_error_var_pnorm > 0.965 & Asset == 'AUD_CAD')|
  (rolling_error_var_pnorm > 0.99 & Asset == 'AUD_CHF')|
  (pnorm_50_port > 0.85 & Asset == 'AUD_USD')|
  (pnorm_100_roll_100_port > 0.75 & Asset == 'NZD_USD')|
  (pnorm_1001_port > 0.7 & pnorm_1001_port < 1 & Asset == 'AUD_USD')|
  (pnorm_250 > 0.61 & Asset == 'AUD_NZD')|
  (pnorm_50_roll_50_port > 0.63 & Asset == 'AUD_USD')|
  (pnorm_50_roll_50_port > 0.775)|
  (pnorm_100_port > 0.875)|
  (pnorm_error_75_vs_50_rate_50 > 0.925 & pnorm_error_75_vs_50_rate_50 < 0.99 & Asset == 'AUD_NZD')|
  (pnorm_error_200_rate_100 > 0.99 & pnorm_error_200_rate_100 <= 1 & Asset == 'AUD_CAD')


"

analyse_performance <-
  model_predicted_data %>%
  # filter(Asset %in% c( "AUD_CAD")) %>%
  mutate(
    trade_col = eval(parse(text = trade_statment))
  ) %>%
  mutate(
    trade_col = case_when(trade_col == TRUE ~ "Long", TRUE ~ "No Trade")
  )

control <-
  analyse_performance %>%
  group_by(Date) %>%
  summarise(Final_Return = sum(Final_Return)) %>%
  ungroup() %>%
  arrange(Date) %>%
  mutate(Final_Return_Cumulative = cumsum(Final_Return)) %>%
  mutate(trade_col = "Control")

analyse_performance <-
  analyse_performance %>%
  # filter(Date >= '2026-09-07') %>%
  filter(trade_col == "Long") %>%
  group_by(Date) %>%
  summarise(Final_Return = sum(Final_Return)) %>%
  ungroup() %>%
  arrange(Date) %>%
  mutate(Final_Return_Cumulative = cumsum(Final_Return)) %>%
  mutate(trade_col = "Long")

dim(control)[1]
dim(analyse_performance)[1]


analyse_performance %>%
  bind_rows(control) %>%
  # filter(Date >= '2026-01-01') %>%
  # filter(Date <= '2027-09-01') %>%
  ggplot(aes(x = Date, y = Final_Return_Cumulative
             ,color = trade_col
  )) +
  geom_line() +
  geom_hline(yintercept = 0, linetype = "dashed", color = 'darkred') +
  facet_wrap(.~trade_col, scales = "free") +
  theme_minimal() +
  scale_y_continuous(n.breaks = 30) +
  theme(legend.position = "bottom", axis.text = element_text(size = 7))


analyse_performance %>%
  ungroup() %>%
  arrange(Date) %>%
  mutate(
    X = Final_Return_Cumulative - lag(Final_Return_Cumulative, 50),
    XX = Final_Return_Cumulative - lag(Final_Return_Cumulative, 100),
    XX2 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 150),
    XX3 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 200),
    XX4 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 400),
    XX5 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 500),
    XX6 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 1000),
    XX7 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 1250),
    XX8 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 1500)
  ) %>%
  summarise(
    min0 = min(X, na.rm = T),
    min1 = min(XX, na.rm = T),
    min2 = min(XX2, na.rm = T),
    min3 = min(XX3, na.rm = T),
    min4 = min(XX4, na.rm = T),
    min5 = min(XX5, na.rm = T),
    min6 = min(XX6, na.rm = T),
    min7 = min(XX7, na.rm = T),
    min8 = min(XX8, na.rm = T)
  )

upload_results_con <-
  connect_db("C:/Users/nikhi/Documents/trade_data/real_results_new_algo.db")

append_table_sql_lite(.data = analyse_performance,
                      conn = upload_results_con,
                      table_name = "real_results_new_algo")

DBI::dbDisconnect(upload_results_con)
rm(upload_results_con)

tictoc::tic()
model_predicted_data_raw <-
  portfolio_no_V3_New_algo_pnorm_version(
    assets_to_port =
      c(
        "AUD_USD",
        "EUR_GBP",
        "USD_CHF",
        "USD_CAD",
        "USD_JPY"
      ) %>% unique(),
    stop_factor_var = 10,
    currency_conversion = currency_conversion,
    asset_infor = asset_infor,
    db_location = db_location,
    start_date = "2021-01-01",
    profit_factor_var = 10,
    risk_dollar_value_var = 5,
    end_period = 132,
    trade_direction = "Long",
    end_point_loss = -5,
    end_point_profit = 5,
    regression_length = 25000,
    direct_return_cols = 24,
    lag_value_error = 132 + 1,
    low_to_price_lengths = c(400),
    cor_period = c(50),
    dependant_var = "Final_Return",
    save_location = "C:/Users/nikhi/Documents//trade_data/Day_Trader_Cor_Continuous_Models/",
    # file_name = "EQUITY_EXPNDED_ONLY_NON_V3_NEW_MODEL",
    file_name = "USD_ONLY_STOP_10_PROF_10_NON_V3_NEW_MODEL",
    training_date = "2021-02-01",
    testing_date ="2021-01-01",
    xtnd_ss_cols_PR_cols = c(1,5,10,15, 20, 25, 30,35, 40,45, 50,55, 60,65, 70, 75,80, 90, 100,110, 120, 130),
    xtnd_ss_cols_BR_periods = c(100,200,300,400 ,50, 150, 250, 350, 25, 500),
    lag_dependant = 132 + 1,
    auto_cor_cols = 5,
    cor_skip_periods = c(1,2,3),
    periods_to_use_deviation = c(1,5,10,15,20,30,40,50, 60, 70, 80, 90, 100),
    mean_periods_deviation = c(25,50,75,100),

    run_additional_calc = TRUE,
    estimate_trades = FALSE,
    trade_statement = NULL
  )
tictoc::toc()

model_predicted_data <-
  model_predicted_data_raw %>%
  mutate(
    pnorm_50 = pcauchy(predicted, location = pred_10000_mean_roll_50, scale = pred_10000_sd_roll_50),
    pnorm_50_roll_50 = slider::slide_dbl(.x  = pnorm_50, .f = ~ mean(.x, na.rm = T), .before = 50),

    pnorm_50_port = pcauchy(predicted_portfolio, location = pred_portfolio_10000_mean_roll_50, scale = pred_portfolio_10000_sd_roll_50),
    pnorm_50_roll_50_port = slider::slide_dbl(.x  = pnorm_50_port, .f = ~ mean(.x, na.rm = T), .before = 50),

  )


# Trade Statement ---------------------------------------------------------
names(model_predicted_data)
col_to_test <- "predicted_portfolio"
col_to_test_mean <- "pred_portfolio_10000_mean_roll_2000"
col_to_test_sd <- "pred_portfolio_10000_sd_roll_2000"

trade_statment <-
  "
    (pnorm_100_roll_100 > 0.64 & pnorm_100_roll_100 <= 1 & Asset == 'USD_CHF')|
  (pnorm_100_roll_100 > 0.55 & pnorm_100_roll_100 <= 1 & Asset == 'USD_CAD')|
  (pnorm_500_roll_500 > 0.54 & pnorm_500_roll_500 <= 1 & Asset == 'AUD_USD')|
  (pnorm_500_roll_500 > 0.53 & pnorm_500_roll_500 <= 1 & Asset == 'EUR_GBP')|
  (rolling_error_var_pnorm > 0.9 & rolling_error_var_pnorm <= 0.99)|
  (pnorm_error_5_vs_100rate_100 > 0.9 & pnorm_error_5_vs_100rate_100 <= 0.99 & Asset == 'USD_JPY')|
  (pnorm_error_5_vs_100rate_100 > 0.95 & pnorm_error_5_vs_100rate_100 <= 1 & Asset == 'EUR_GBP')|
  (pnorm_error_75_vs_50_rate_50 > 0.8 & pnorm_error_75_vs_50_rate_50 <= 0.99 )|
  (pnorm_250_roll_250_port > 0.73 & pnorm_250_roll_250_port <= 1)
"

analyse_performance <-
  model_predicted_data %>%
  # filter(Asset %in% c( "USD_CAD")) %>%
  mutate(
    trade_col = eval(parse(text = trade_statment))
  ) %>%
  mutate(
    trade_col = case_when(trade_col == TRUE ~ "Long", TRUE ~ "No Trade")
  )

control <-
  analyse_performance %>%
  group_by(Date) %>%
  summarise(Final_Return = sum(Final_Return)) %>%
  ungroup() %>%
  arrange(Date) %>%
  mutate(Final_Return_Cumulative = cumsum(Final_Return)) %>%
  mutate(trade_col = "Control")

analyse_performance <-
  analyse_performance %>%
  filter(trade_col == "Long") %>%
  group_by(Date) %>%
  summarise(Final_Return = sum(Final_Return)) %>%
  ungroup() %>%
  arrange(Date) %>%
  mutate(Final_Return_Cumulative = cumsum(Final_Return)) %>%
  mutate(trade_col = "Long")

dim(control)[1]
dim(analyse_performance)[1]


analyse_performance %>%
  bind_rows(control) %>%
  # filter(Date >= '2026-01-01') %>%
  # filter(Date <= '2027-09-01') %>%
  ggplot(aes(x = Date, y = Final_Return_Cumulative
             ,color = trade_col
  )) +
  geom_line() +
  geom_hline(yintercept = 0, linetype = "dashed", color = 'darkred') +
  facet_wrap(.~trade_col, scales = "free") +
  theme_minimal() +
  scale_y_continuous(n.breaks = 30) +
  theme(legend.position = "bottom", axis.text = element_text(size = 7))

analyse_performance %>%
  ungroup() %>%
  arrange(Date) %>%
  mutate(
    X = Final_Return_Cumulative - lag(Final_Return_Cumulative, 50),
    XX = Final_Return_Cumulative - lag(Final_Return_Cumulative, 100),
    XX2 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 150),
    XX3 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 200),
    XX4 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 400),
    XX5 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 500),
    XX6 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 1000),
    XX7 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 1250),
    XX8 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 2500)
  ) %>%
  summarise(
    min0 = min(X, na.rm = T),
    min1 = min(XX, na.rm = T),
    min2 = min(XX2, na.rm = T),
    min3 = min(XX3, na.rm = T),
    min4 = min(XX4, na.rm = T),
    min5 = min(XX5, na.rm = T),
    min6 = min(XX6, na.rm = T),
    min7 = min(XX7, na.rm = T),
    min8 = min(XX8, na.rm = T)
  )

upload_results_con <-
  connect_db("C:/Users/nikhi/Documents/trade_data/real_results_new_algo.db")

append_table_sql_lite(.data = analyse_performance,
                      conn = upload_results_con,
                      table_name = "real_results_new_algo")

DBI::dbDisconnect(upload_results_con)
rm(upload_results_con)


tictoc::tic()
model_predicted_data_raw <-
  portfolio_no_V3_New_algo_pnorm_version(
    assets_to_port =
      c(
        "XAU_USD",
        "SPX500_USD",
        "WTICO_USD",
        "EU50_EUR",
        "JP225_USD"
      ) %>% unique(),
    stop_factor_var = 3,
    currency_conversion = currency_conversion,
    asset_infor = asset_infor,
    db_location = db_location,
    start_date = "2024-01-01",
    profit_factor_var = 6,
    risk_dollar_value_var = 5,
    end_period = 132,
    trade_direction = "Long",
    end_point_loss = -5,
    end_point_profit = 10,
    regression_length = 25000,
    direct_return_cols = 24,
    lag_value_error = 132 + 1,
    low_to_price_lengths = c(400),
    cor_period = c(50),
    dependant_var = "Final_Return",
    save_location = "C:/Users/nikhi/Documents//trade_data/Day_Trader_Cor_Continuous_Models/",
    # file_name = "EQUITY_EXPNDED_ONLY_NON_V3_NEW_MODEL",
    file_name = "MIXED_LONG_TRAIN_NON_V3_NEW_MODEL",
    training_date = "2025-07-01",
    testing_date ="2024-01-01",
    xtnd_ss_cols_PR_cols = c(1,5,10,15, 20, 25, 30,35, 40,45, 50,55, 60,65, 70, 75,80, 90, 100,110, 120, 130),
    xtnd_ss_cols_BR_periods = c(100,200,300,400 ,50, 150, 250, 350, 25, 500, 10),
    lag_dependant = 132 + 1,
    auto_cor_cols = 5,
    cor_skip_periods = c(1,2,3),
    periods_to_use_deviation = c(5,10,15,20,30,40,50, 60, 70, 80, 90, 100),
    mean_periods_deviation = c(10,25,50,75,100),

    run_additional_calc = TRUE,
    estimate_trades = FALSE,
    trade_statement = NULL
  )
tictoc::toc()

model_predicted_data <-
  model_predicted_data_raw %>%
  mutate(
    pnorm_50 = pcauchy(predicted, location = pred_10000_mean_roll_50, scale = pred_10000_sd_roll_50),
    pnorm_50_roll_50 = slider::slide_dbl(.x  = pnorm_50, .f = ~ mean(.x, na.rm = T), .before = 50),

    pnorm_50_port = pcauchy(predicted_portfolio, location = pred_portfolio_10000_mean_roll_50, scale = pred_portfolio_10000_sd_roll_50),
    pnorm_50_roll_50_port = slider::slide_dbl(.x  = pnorm_50_port, .f = ~ mean(.x, na.rm = T), .before = 50),

  )


# Trade Statement ---------------------------------------------------------
names(model_predicted_data)
col_to_test <- "predicted_portfolio"
col_to_test_mean <- "pred_portfolio_10000_mean_roll_2000"
col_to_test_sd <- "pred_portfolio_10000_sd_roll_2000"

trade_statment <-
  "
    (rolling_error_var_pnorm >= 0.91 & Asset == 'SPX500_USD' )|
  (rolling_error_var_pnorm >= 0.51 & Asset == 'XAU_USD' )|
  (rolling_error_var_pnorm >= 0.91 & Asset == 'JP225_USD' )|
  (rolling_error_var_pnorm >= 0.56 & Asset == 'WTICO_USD' )|
  (rolling_error_var_pnorm >= 0.56 & Asset == 'WTICO_USD' )|

  (pnorm_error_5_rate_50 >= 0.88 & Asset == 'SPX500_USD')|
  (pnorm_error_5_rate_50 >= 0.55 & Asset == 'XAU_USD')|
  (pnorm_error_5_rate_50 >= 0.86 & Asset == 'EU50_EUR')|
  (pnorm_error_5_rate_50 >= 0.98 & Asset == 'WTICO_USD')|

  (pnorm_100 >= 0.68 & Asset == 'SPX500_USD')|
  (pnorm_100 >= 0.74 & Asset == 'JP225_USD')|
  (pnorm_100 >= 0.71 & Asset == 'XAU_USD')|
  (pnorm_100 >= 0.73 & Asset == 'WTICO_USD')|

  (pnorm_250 >= 0.73 & Asset == 'JP225_USD')|
  (pnorm_250 >= 0.57 & Asset == 'XAU_USD')|
  (pnorm_250 >= 0.75 & Asset == 'SPX500_USD')|
  (pnorm_250 >= 0.77 & Asset == 'EU50_EUR')|
  (pnorm_250 >= 0.79 & Asset == 'WTICO_USD')|

  (pnorm_250_port >= 0.6 & Asset == 'WTICO_USD')|
  (pnorm_250_port >= 0.55 & Asset == 'XAU_USD')|
  (pnorm_250_port >= 0.56 & Asset == 'JP225_USD')|
  (pnorm_250_port >= 0.69 & Asset == 'EU50_EUR')|
  (pnorm_250_port >= 0.85 & Asset == 'SPX500_USD')|

  (pnorm_500_port >= 0.60 & Asset == 'WTICO_USD')|
  (pnorm_500_port >= 0.55 & Asset == 'XAU_USD')|
  (pnorm_500_port >= 0.51 & Asset == 'JP225_USD')|
  (pnorm_500_port >= 0.68 & Asset == 'EU50_EUR')|

  (pnorm_1001_port >= 0.59 & Asset == 'WTICO_USD')|
  (pnorm_1001_port >= 0.56 & Asset == 'XAU_USD')|
  (pnorm_1001_port >= 0.55 & Asset == 'JP225_USD')|
  (pnorm_1001_port >= 0.58 & Asset == 'EU50_EUR')
"


analyse_performance <-
  model_predicted_data %>%
  # filter(Asset %in% c( "WTICO_USD")) %>%
  mutate(
    trade_col = eval(parse(text = trade_statment))
  ) %>%
  mutate(
    trade_col = case_when(trade_col == TRUE ~ "Long", TRUE ~ "No Trade")
  )

control <-
  analyse_performance %>%
  group_by(Date) %>%
  summarise(Final_Return = sum(Final_Return)) %>%
  ungroup() %>%
  arrange(Date) %>%
  mutate(Final_Return_Cumulative = cumsum(Final_Return)) %>%
  mutate(trade_col = "Control")

analyse_performance <-
  analyse_performance %>%
  filter(trade_col == "Long") %>%
  group_by(Date) %>%
  summarise(Final_Return = sum(Final_Return)) %>%
  ungroup() %>%
  arrange(Date) %>%
  mutate(Final_Return_Cumulative = cumsum(Final_Return)) %>%
  mutate(trade_col = "Long")

dim(control)[1]
dim(analyse_performance)[1]


analyse_performance %>%
  bind_rows(control) %>%
  # filter(Date >= '2026-01-01') %>%
  # filter(Date <= '2027-09-01') %>%
  ggplot(aes(x = Date, y = Final_Return_Cumulative
             ,color = trade_col
  )) +
  geom_line() +
  geom_hline(yintercept = 0, linetype = "dashed", color = 'darkred') +
  # facet_wrap(.~trade_col, scales = "free") +
  theme_minimal() +
  scale_y_continuous(n.breaks = 30) +
  theme(legend.position = "bottom", axis.text = element_text(size = 7))

analyse_performance %>%
  ungroup() %>%
  arrange(Date) %>%
  mutate(
    X = Final_Return_Cumulative - lag(Final_Return_Cumulative, 50),
    XX = Final_Return_Cumulative - lag(Final_Return_Cumulative, 100),
    XX2 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 150),
    XX3 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 200),
    XX4 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 400),
    XX5 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 500),
    XX6 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 1000),
    XX7 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 1250),
    XX8 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 2500)
  ) %>%
  summarise(
    min0 = min(X, na.rm = T),
    min1 = min(XX, na.rm = T),
    min2 = min(XX2, na.rm = T),
    min3 = min(XX3, na.rm = T),
    min4 = min(XX4, na.rm = T),
    min5 = min(XX5, na.rm = T),
    min6 = min(XX6, na.rm = T),
    min7 = min(XX7, na.rm = T),
    min8 = min(XX8, na.rm = T)
  )

upload_results_con <-
  connect_db("C:/Users/nikhi/Documents/trade_data/real_results_new_algo.db")

append_table_sql_lite(.data = analyse_performance,
                      conn = upload_results_con,
                      table_name = "real_results_new_algo")

DBI::dbDisconnect(upload_results_con)
rm(upload_results_con)

tictoc::tic()
model_predicted_data_raw <-
  portfolio_no_V3_New_algo_pnorm_version(
    assets_to_port =
      c(
        "SPX500_USD",
        "CH20_CHF",
        "DE30_EUR",
        "XAU_USD",
        "XAG_USD",
        "HK33_HKD",
        "JP225Y_JPY",
        "UK100_GBP"
      ) %>% unique(),
    stop_factor_var = 5,
    currency_conversion = currency_conversion,
    asset_infor = asset_infor,
    db_location = db_location,
    start_date = "2021-11-01",
    # profit_factor_var = 60,
    profit_factor_var = 80,
    risk_dollar_value_var = 5,
    end_period = 132,
    trade_direction = "Long",
    end_point_loss = -5,
    # end_point_profit = 55,
    end_point_profit = 75,
    regression_length = 25000,
    direct_return_cols = 24,
    lag_value_error = 132 + 1,
    low_to_price_lengths = c(400),
    cor_period = c(50),
    dependant_var = "Final_Return",
    save_location = "C:/Users/nikhi/Documents//trade_data/Day_Trader_Cor_Continuous_Models/",
    # file_name = "EQUITY_EXPNDED_ONLY_NON_V3_NEW_MODEL",
    file_name = "EQUITY_GOLD_EXPNDED_ONLY_NON_V3_NEW_MODEL",
    training_date = "2022-02-01",
    testing_date ="2021-11-01",
    xtnd_ss_cols_PR_cols = c(1,5,10,15, 20, 25, 30,35, 40,45, 50,55, 60,65, 70, 75,80, 90, 100,110, 120, 130),
    xtnd_ss_cols_BR_periods = c(100,200,300,400 ,50, 150, 250, 350, 25, 500),
    lag_dependant = 132 + 1,
    auto_cor_cols = 20,
    cor_skip_periods = c(1,2,4,5,6,8,10),
    periods_to_use_deviation = c(1,10,20,30,40,50, 60, 70, 80, 90, 100),
    mean_periods_deviation = c(50,75,100),

    estimate_trades = FALSE,
    trade_statement = NULL
  )
tictoc::toc()

model_predicted_data <-
  model_predicted_data_raw


# Trade Statement ---------------------------------------------------------
names(model_predicted_data)
col_to_test <- "predicted_portfolio"
col_to_test_mean <- "pred_portfolio_10000_mean_roll_2000"
col_to_test_sd <- "pred_portfolio_10000_sd_roll_2000"

trade_statment <-
  "
  (pnorm_1001_port > 0.775 & pnorm_1001_port <= 1 & Asset == 'CH20_CHF')|
  (pnorm_1001_port > 0.7 & pnorm_1001_port <= 1 & Asset == 'DE30_EUR')|
  (pnorm_1001_port > 0.79 & pnorm_1001_port <= 1 & Asset == 'HK33_HKD')|
  (pnorm_1001_port > 0.725 & pnorm_1001_port <= 1 & Asset == 'JP225Y_JPY')|
  (pnorm_1001_port > 0.75 & pnorm_1001_port <= 1 & Asset == 'UK100_GBP')|
  (pnorm_1001_port > 0.75 & pnorm_1001_port <= 0.8 & Asset == 'XAU_USD')|
  (pnorm_2000_mean_1000_port >= 0.5655 & pnorm_2000_mean_1000_port <= 1 & Asset == 'SPX500_USD')|
  (pnorm_2000 > 0.65 & pnorm_2000 <= 0.85 & Asset == 'XAU_USD')|
  (pnorm_2000_port > 0.65 & pnorm_2000_port <= 0.85 & Asset == 'XAG_USD')|
  (pnorm_2000_port > 0.775 & pnorm_2000_port <= 1 & Asset == 'CH20_CHF')|
  (pnorm_2000_port > 0.8 & pnorm_2000_port <= 1 & Asset == 'UK100_GBP')|
  (pnorm_250_port > 0.51 & pnorm_250_port <= 1 & Asset == 'XAG_USD')|
  (pnorm_1001_port > 0.7 & pnorm_1001_port <= 1 & Asset == 'SPX500_USD')|
  (pnorm_2000_port > 0.68 & pnorm_2000_port <= 1 & Asset == 'JP225Y_JPY')
"

analyse_performance <-
  model_predicted_data %>%
  # filter(Asset %in% c( "JP225Y_JPY")) %>%
  mutate(
    trade_col = eval(parse(text = trade_statment))
  ) %>%
  mutate(
    trade_col = case_when(trade_col == TRUE ~ "Long", TRUE ~ "No Trade")
  )

control <-
  analyse_performance %>%
  group_by(Date) %>%
  summarise(Final_Return = sum(Final_Return)) %>%
  ungroup() %>%
  arrange(Date) %>%
  mutate(Final_Return_Cumulative = cumsum(Final_Return)) %>%
  mutate(trade_col = "Control")

analyse_performance <-
  analyse_performance %>%
  # filter(Date >= '2026-09-07') %>%
  filter(trade_col == "Long") %>%
  group_by(Date) %>%
  summarise(Final_Return = sum(Final_Return)) %>%
  ungroup() %>%
  arrange(Date) %>%
  mutate(Final_Return_Cumulative = cumsum(Final_Return)) %>%
  mutate(trade_col = "Long")

dim(control %>% ungroup() %>%  distinct(Date))[1]
dim(analyse_performance %>% ungroup() %>% distinct(Date))[1]


analyse_performance %>%
  bind_rows(control) %>%
  # filter(Date >= '2026-01-01') %>%
  # filter(Date <= '2027-09-01') %>%
  ggplot(aes(x = Date, y = Final_Return_Cumulative
             ,color = trade_col
  )) +
  geom_line() +
  # geom_hline(yintercept = 0, linetype = "dashed", color = 'darkred') +
  facet_wrap(.~trade_col, scales = "free") +
  theme_minimal() +
  scale_y_continuous(n.breaks = 30) +
  theme(legend.position = "bottom", axis.text = element_text(size = 7))

analyse_performance %>%
  ungroup() %>%
  arrange(Date) %>%
  mutate(
    X = Final_Return_Cumulative - lag(Final_Return_Cumulative, 50),
    XX = Final_Return_Cumulative - lag(Final_Return_Cumulative, 100),
    XX2 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 150),
    XX3 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 200),
    XX4 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 400),
    XX5 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 500),
    XX6 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 1000),
    XX7 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 1250)
  ) %>%
  summarise(
    min0 = min(X, na.rm = T),
    min1 = min(XX, na.rm = T),
    min2 = min(XX2, na.rm = T),
    min3 = min(XX3, na.rm = T),
    min4 = min(XX4, na.rm = T),
    min5 = min(XX5, na.rm = T),
    min6 = min(XX6, na.rm = T),
    min7 = min(XX7, na.rm = T)
  )

analyse_performance %>%
  ungroup() %>%
  arrange(Date) %>%
  mutate(
    X = Final_Return_Cumulative - lag(Final_Return_Cumulative, 50),
    XX = Final_Return_Cumulative - lag(Final_Return_Cumulative, 100),
    XX2 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 150),
    XX3 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 200),
    XX4 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 400),
    XX5 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 500),
    XX6 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 1000)
  ) %>%
  summarise(
    min0 = min(X, na.rm = T),
    min1 = min(XX, na.rm = T),
    min2 = min(XX2, na.rm = T),
    min3 = min(XX3, na.rm = T),
    min4 = min(XX4, na.rm = T),
    min5 = min(XX5, na.rm = T),
    min6 = min(XX6, na.rm = T)
  )

upload_results_con <-
  connect_db("C:/Users/nikhi/Documents/trade_data/real_results_new_algo.db")

append_table_sql_lite(.data = analyse_performance,
                      conn = upload_results_con,
                      table_name = "real_results_new_algo")

DBI::dbDisconnect(upload_results_con)
rm(upload_results_con)
