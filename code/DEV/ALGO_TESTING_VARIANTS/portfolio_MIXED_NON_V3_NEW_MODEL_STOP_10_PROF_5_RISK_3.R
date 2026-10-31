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
    profit_factor_var = 5,
    risk_dollar_value_var = 3,
    end_period = 132,
    trade_direction = "Long",
    end_point_loss = -3,
    end_point_profit = 1.5,
    regression_length = 25000,
    direct_return_cols = 24,
    lag_value_error = 132 + 1,
    low_to_price_lengths = c(400),
    cor_period = c(50),
    dependant_var = "Final_Return",
    save_location = "C:/Users/nikhi/Documents//trade_data/Day_Trader_Cor_Continuous_Models/",
    file_name = "MIXED_NON_V3_NEW_MODEL_STOP_10_PROF_5_RISK_3",
    training_date = "2022-02-01",
    testing_date ="2021-11-01",
    xtnd_ss_cols_PR_cols = c(1,5,10,15, 20, 25, 30,35, 40,45, 50,55, 60,65, 70, 75,80, 90, 100,110, 120, 130),
    xtnd_ss_cols_BR_periods = c(100,200,300, 50, 150, 250, 350, 25, 500),
    lag_dependant = 132 + 1,
    auto_cor_cols = 3,
    cor_skip_periods = c(1),
    periods_to_use_deviation = c(1,10,20,30,40,50),
    mean_periods_deviation = c(50),
    run_additional_calc = TRUE,

    estimate_trades = FALSE,
    trade_statement = NULL
  )
tictoc::toc()

model_predicted_data <-
  model_predicted_data_raw %>%
  dplyr::select(Date, Asset,predicted_portfolio,
                predicted,
                contains('pnorm'),
                # contains("pred"),
                Final_Return)

names(model_predicted_data)

trade_statment <-
  "
  # (pnorm_100 > 0.76 & Asset == 'NAS100_USD')|
  # (pnorm_100 > 0.65 & Asset == 'USD_JPY')|
  # (pnorm_50 > 0.7 & Asset == 'USD_JPY')|
  # (pnorm_50 > 0.81 & Asset == 'XAU_USD')|
  # (pnorm_50 > 0.860 & Asset == 'XCU_USD')|
  # (pnorm_50 > 0.72 & Asset == 'NAS100_USD')|
  # (pnorm_250 > 0.78 & Asset == 'NAS100_USD')|
  # (pnorm_250 > 0.77 & Asset == 'USD_JPY')|
  # (pnorm_250_port > 0.66)|
  # (pnorm_500 > 0.835 & Asset == 'BTC_USD')|
  # (pnorm_500 > 0.835 & Asset == 'NAS100_USD')|
  # (pnorm_500 > 0.760 & Asset == 'XAU_USD')|
  # (pnorm_500 > 0.835 & Asset == 'USD_JPY')|
  # (pnorm_500 > 0.6 & pnorm_500 < 0.8 & Asset == 'XCU_USD')|
  # (pnorm_1001 > 0.685 & Asset == 'XAU_USD')|
  # (pnorm_1001_port > 0.585 & Asset == 'XAU_USD')|
  # (pnorm_1001_port > 0.560 & Asset == 'NAS100_USD')|
  # (pnorm_1001_port > 0.8 & Asset == 'USD_JPY')|
  # (pnorm_1001_port > 0.785 & Asset == 'XCU_USD')|

  (pnorm_error_5_vs_100rate_100 > 0.985 & Asset == 'XCU_USD')|
  (pnorm_error_5_vs_100rate_100 > 0.985 & Asset == 'NAS100_USD')|
  (pnorm_error_75_vs_50_rate_50 > 0.910 & Asset == 'XAU_USD')|
  (pnorm_error_75_vs_50_rate_50 > 0.985 & Asset == 'USD_JPY')|
  (pnorm_error_75_vs_50_rate_50 > 0.935 & Asset == 'XCU_USD')|
  (pnorm_error_75_rate_50 > 0.985 & Asset == 'BTC_USD')|
  (pnorm_error_75_rate_50 > 0.985 & Asset == 'XAU_USD')|
  (pnorm_error_75_rate_50 > 0.785 & Asset == 'USD_JPY')|
  (pnorm_error_5_rate_50 > 0.985 & Asset == 'BTC_USD')|
  (pnorm_error_5_rate_50 > 0.985 & Asset == 'NAS100_USD')|
  (pnorm_error_5_rate_50 > 0.985 & Asset == 'XAU_USD')|
  (pnorm_error_5_rate_50 > 0.785 & Asset == 'XCU_USD')|
  (pnorm_error_5_rate_50 > 0.98 & Asset == 'USD_JPY')|
  (pnorm_error_5_rate_100 > 0.99 & Asset == 'USD_JPY')|
  (pnorm_error_5_rate_100 > 0.785 & Asset == 'XCU_USD')|
  (pnorm_error_5_rate_100 > 0.89 & Asset == 'XAU_USD')|
  (pnorm_error_5_rate_100 > 0.96 & Asset == 'BTC_USD')|
  (pnorm_error_5_rate_100 > 0.98 & Asset == 'NAS100_USD')|
  (pnorm_error_200_rate_50 > 0.985 & Asset == 'BTC_USD')|
  (pnorm_error_200_rate_50 > 0.985 & Asset == 'NAS100_USD')|
  (pnorm_error_200_rate_50 > 0.985 & Asset == 'USD_JPY')|
  (pnorm_error_200_rate_50 > 0.985 & Asset == 'XAU_USD')

"

#New Better Statement
trade_statment <-
  "
 (pnorm_1500_mean_10_roll_1500_port > 0.57 & Asset == 'XAU_USD')|
 (pnorm_error_100_rate_50 > 0.99 & Asset == 'XAU_USD')|
 (pnorm_error_200_rate_50 > 0.99 & Asset == 'XAU_USD')|
 (pnorm_error_75_rate_50 > 0.99 & Asset == 'XAU_USD')|
 (pnorm_error_5_rate_100 > 0.95 & Asset == 'XAU_USD')|
 (pnorm_1001_roll_1001 > 0.57 & Asset == 'XAU_USD')|
 (pnorm_1500_roll_1500_port > 0.55 & Asset == 'XAU_USD')|

 (pnorm_error_5_vs_100rate_100 > 0.99 & Asset == 'NAS100_USD')|
 (pnorm_error_200_rate_100 > 0.97 & Asset == 'NAS100_USD')|
 (pnorm_1001_port_roll_100 > 0.69 & Asset == 'NAS100_USD')|
 (pnorm_error_5_rate_50 > 0.99 & Asset == 'NAS100_USD')|
 (pnorm_2000_mean_1000 > 0.55 & Asset == 'NAS100_USD')|
 (rolling_error_var_pnorm > 0.87 & Asset == 'NAS100_USD')|
 (pnorm_2000_roll_2000 > 0.51 & Asset == 'NAS100_USD')|

 (pnorm_1500_mean_10_roll_1500_port > 0.57 & Asset == 'XCU_USD')|
 (pnorm_1500_roll_1500_port > 0.57 & Asset == 'XCU_USD')|
 (pnorm_2000_roll_2000_port > 0.57 & Asset == 'XCU_USD')|
 (pnorm_error_5_vs_100rate_100 > 0.99 & Asset == 'XCU_USD')|
 (pnorm_1001_roll_1001 > 0.55 & Asset == 'XCU_USD')|
 (pnorm_100_roll_100 > 0.53 & Asset == 'XCU_USD')|
 (pnorm_error_100_rate_100 > 0.95 & Asset == 'XCU_USD')|

 (pnorm_250_roll_250_port > 0.61 & Asset == 'USD_JPY')|
 (pnorm_error_75_vs_50_rate_50 > 0.99 & Asset == 'USD_JPY')|
 (pnorm_500_port > 0.81 & Asset == 'USD_JPY')|
 (pnorm_error_200_rate_50 > 0.99 & Asset == 'USD_JPY')|
 (pnorm_error_200_rate_100 > 0.97 & Asset == 'USD_JPY')|
 (pnorm_2000_roll_2000_port > 0.53 & Asset == 'USD_JPY')|
 (pnorm_100_roll_100_port > 0.65 & Asset == 'USD_JPY')|

 (pnorm_1001_port_roll_100 > 0.71 & Asset == 'BTC_USD')|
 (pnorm_error_100_rate_50 > 0.99 & Asset == 'BTC_USD')|
 (pnorm_1500_mean_10_roll_1500 > 0.53 & Asset == 'BTC_USD')|
 (pnorm_error_5_rate_50 > 0.97 & Asset == 'BTC_USD')|
 (pnorm_error_20_rate_50 > 0.97 & Asset == 'BTC_USD')

"

analyse_performance <-
  model_predicted_data %>%
  # filter(Asset %in% c('XAU_USD')) %>%
  filter(Date >= "2026-05-01") %>%
  # filter(Date >= "2026-09-01") %>%
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
  # facet_wrap(.~trade_col, scales = "free") +
  theme_minimal() +
  scale_y_continuous(n.breaks = 20) +
  theme(legend.position = "bottom")

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


# upload_results_con <-
#   connect_db("C:/Users/nikhi/Documents/trade_data/real_results_new_algo.db")
#
# append_table_sql_lite(.data = analyse_performance,
#                       conn = upload_results_con,
#                       table_name = "real_results_new_algo")
#
# DBI::dbDisconnect(upload_results_con)
# rm(upload_results_con)

# AUC Pred Section --------------------------------------------------------

names(model_predicted_data)

return_thresh <- 0
auc_roc_list <- list()
col_to_test <- "pnorm_error_200_rate_50"
col_to_test_port <- "pnorm_error_200_rate_50"
c = 0

# for (i in seq(-1,15,0.25)) {
for (i in seq(0.01,0.99, 0.025)) {
  c = c + 1

  trade_statment <-
    glue::glue("
    ({col_to_test} > i &
    {col_to_test} < 1000)
  ")

  trade_statment_port <-
    glue::glue("
    ({col_to_test_port} > i &
    {col_to_test_port} < 1000)
  ")

  trade_statment_double <-
    glue::glue("{trade_statment} & {trade_statment_port}")

  total_trades_control <-
    model_predicted_data %>%
    group_by(Asset) %>%
    summarise(Total_Trades_Control = n_distinct(Date),
              Final_Return_Control = sum(Final_Return, na.rm = T),
              # Average_Return_Control = Final_Return_Control/Total_Trades_Control
              Average_Return_Control = mean(Final_Return, na.rm = T),
              sd_Return_Control = sd(Final_Return, na.rm = T)
    ) %>%
    ungroup()

  trades_per_year <-
    model_predicted_data %>%
    mutate(
      trade_col = eval(parse(text = trade_statment))
    ) %>%
    mutate(
      trade_col = case_when(trade_col == TRUE ~ "Long", TRUE ~ "No Trade")
    ) %>%
    filter(trade_col == "Long") %>%
    mutate(xx = month(Date)) %>%
    group_by(Asset, xx) %>%
    summarise(Total_Trades = n_distinct(Date)) %>%
    group_by(Asset) %>%
    summarise(Average_Trades_Per_Month = mean(Total_Trades, na.rm = T)) %>%
    ungroup()

  returns_total <-
    model_predicted_data %>%
    mutate(
      trade_col = eval(parse(text = trade_statment))
    ) %>%
    mutate(
      trade_col = case_when(trade_col == TRUE ~ "Long", TRUE ~ "No Trade")
    ) %>%
    filter(trade_col == "Long") %>%
    group_by(Asset) %>%
    summarise(Final_Return = sum(Final_Return, na.rm = T)) %>%
    mutate(
      threshold = i,
      col_to_test = col_to_test
    ) %>%
    ungroup() %>%
    mutate(
      Total_Final = sum(Final_Return, na.rm = T)
    )

  returns_total_TRU_POS <-
    model_predicted_data %>%
    mutate(
      trade_col = eval(parse(text = trade_statment_port))
    ) %>%
    mutate(
      trade_col = case_when(trade_col == TRUE ~ "Long", TRUE ~ "No Trade")
    ) %>%
    group_by(Date, trade_col) %>%
    summarise(Final_Return = sum(Final_Return, na.rm = TRUE))  %>%
    ungroup() %>%
    mutate(
      pos_detect_TRUE =
        ifelse(trade_col == "Long" & Final_Return > return_thresh, 1, 0),
      pos_detect_Ned =
        ifelse(trade_col == "Long" & Final_Return <= return_thresh, 1, 0),

      neg_detect_TRUE =
        ifelse(trade_col == "No Trade" & Final_Return <= return_thresh, 1, 0),
      neg_detect_Ned =
        ifelse(trade_col == "No Trade" & Final_Return > return_thresh, 1, 0)

    ) %>%
    summarise(
      Total_Wins = sum(pos_detect_TRUE, na.rm=T),
      Total_Trades = sum(pos_detect_TRUE, na.rm=T) + sum(pos_detect_Ned, na.rm=T),
      Total_No_Trades = sum(neg_detect_Ned, na.rm=T) + sum(neg_detect_TRUE, na.rm=T),
      TRUE_pos_rate = Total_Wins/Total_Trades,
      TRUE_neg_rate = sum(neg_detect_TRUE, na.rm = T)/Total_No_Trades
    ) %>%
    mutate(
      threshold = i,
      col_to_test = col_to_test,
      Asset = "Portfolio"
    ) %>%
    ungroup()

  auc_roc_list[[c]] <-
    model_predicted_data %>%
    mutate(
      trade_col = eval(parse(text = trade_statment))
    ) %>%
    mutate(
      trade_col = case_when(trade_col == TRUE ~ "Long", TRUE ~ "No Trade")
    ) %>%
    mutate(
      pos_detect_TRUE =
        ifelse(trade_col == "Long" & Final_Return > return_thresh, 1, 0),
      pos_detect_Ned =
        ifelse(trade_col == "Long" & Final_Return <= return_thresh, 1, 0),

      neg_detect_TRUE =
        ifelse(trade_col == "No Trade" & Final_Return <= return_thresh, 1, 0),
      neg_detect_Ned =
        ifelse(trade_col == "No Trade" & Final_Return > return_thresh, 1, 0)

    ) %>%
    group_by(Asset) %>%
    summarise(
      Total_Wins = sum(pos_detect_TRUE, na.rm=T),
      Total_Trades = sum(pos_detect_TRUE, na.rm=T) + sum(pos_detect_Ned, na.rm=T),
      Total_No_Trades = sum(neg_detect_Ned, na.rm=T) + sum(neg_detect_TRUE, na.rm=T),
      TRUE_pos_rate = Total_Wins/Total_Trades,
      TRUE_neg_rate = sum(neg_detect_TRUE, na.rm = T)/Total_No_Trades,
      Average_Return = mean(ifelse(trade_col == "Long", Final_Return, NA), na.rm = T),
      sd_Return = sd(ifelse(trade_col == "Long", Final_Return, NA), na.rm = T)
    ) %>%
    mutate(
      threshold = i,
      col_to_test = col_to_test
    ) %>%
    bind_rows(returns_total_TRU_POS) %>%
    left_join(returns_total) %>%
    fill(Total_Final, .direction = "down") %>%
    mutate(
      Final_Return = ifelse(is.na(Final_Return) & Total_Trades > 0, sum(Final_Return, na.rm = T), Final_Return),
      Final_Return = ifelse(Total_Trades == 0, 0, Final_Return)
    ) %>%
    left_join(total_trades_control) %>%
    mutate() %>%
    left_join(trades_per_year) %>%
    mutate(
      Avg_Returns_Per_Month = Average_Return*Average_Trades_Per_Month,
      # percentile_10th = 250*quantile(rnorm(n = 5000, mean = Average_Return, sd = sd_Return), 0.1, na.rm = T),
      # percentile_20th = 250*quantile(rnorm(n = 5000, mean = Average_Return, sd = sd_Return), 0.2, na.rm = T),
      # percentile_40th = 250*quantile(rnorm(n = 5000, mean = Average_Return, sd = sd_Return), 0.4, na.rm = T)
    ) %>%
    split(.$Asset, drop = FALSE) %>%
    map_dfr(
      ~ .x %>%
        mutate(percentile_30th = 250*quantile(rnorm(n = 15000, mean = Average_Return, sd = sd_Return), 0.3, na.rm = T))
    )

}

auc_roc <-
  auc_roc_list %>%
  map_dfr(~ .x %>% filter(Total_Trades >= 1250))

auc_roc %>%
  ggplot(aes(x = threshold, y = TRUE_pos_rate)) +
  geom_line() +
  geom_point() +
  facet_wrap(.~Asset, scales = "free") +
  theme_minimal()

auc_roc %>%
  ggplot(aes(x = threshold, y = Final_Return)) +
  geom_line() +
  geom_point() +
  facet_wrap(.~Asset, scales = "free") +
  theme_minimal()

auc_roc %>%
  ggplot(aes(x = threshold, y = Average_Return)) +
  geom_line() +
  geom_point() +
  facet_wrap(.~Asset, scales = "free") +
  theme_minimal()

auc_roc %>%
  ggplot(aes(x = threshold, y = Avg_Returns_Per_Month)) +
  geom_line() +
  geom_point() +
  facet_wrap(.~Asset, scales = "free") +
  theme_minimal()

auc_roc %>%
  ggplot(aes(x = threshold, y = Total_Final)) +
  geom_line() +
  geom_point() +
  facet_wrap(.~Asset, scales = "free") +
  theme_minimal()

auc_roc %>%
  filter(Total_Trades >= 4000) %>%
  ggplot(aes(x = threshold, y = percentile_30th)) +
  geom_smooth() +
  geom_point(size = 0.5) +
  facet_wrap(.~Asset, scales = "free") +
  theme_minimal()

auc_roc_filt <-
  auc_roc %>%
  filter(threshold >= 0.5) %>%
  filter(!is.nan(TRUE_pos_rate)) %>%
  group_by(Asset) %>%
  slice_max(Final_Return) %>%
  group_by(Asset) %>%
  slice_head(n = 1)

auc_roc_filt_max_prob <-
  auc_roc %>%
  filter(threshold >= 0.5, Total_Trades > 1000) %>%
  filter(!is.nan(TRUE_pos_rate)) %>%
  group_by(Asset) %>%
  slice_max(TRUE_pos_rate) %>%
  group_by(Asset) %>%
  slice_head(n = 1)


# AUC Roll Mean Section ---------------------------------------------------

names(model_predicted_data)
auc_roc_list <- list()
return_thresh <- 5
col_to_test <- "pred_10000_mean_roll_1500"
col_to_test_mean <- "pred_mean_inf"
col_to_test_sd <- "pred_sd_inf"

col_to_test_port <- "pred_10000_mean_roll_1500"
col_to_test_mean_port <- "pred_mean_inf"
col_to_test_sd_port <- "pred_sd_inf"

col_to_test <- "pred_portfolio_10000_mean_roll_2000"
col_to_test_mean <- "pred_port_mean_inf"
col_to_test_sd <- "pred_port_sd_inf"

col_to_test_port <- "pred_portfolio_10000_mean_roll_2000"
col_to_test_mean_port <- "pred_port_mean_inf"
col_to_test_sd_port <- "pred_port_sd_inf"
c = 0

for (i in seq(-3,3, 0.2)) {
  c = c + 1

  trade_statment <-
    glue::glue("
    ({col_to_test} >= {col_to_test_mean} + i*{col_to_test_sd} &
    {col_to_test} <= {col_to_test_mean} + 50*{col_to_test_sd}  )
  ")

  trade_statment_port <-
    glue::glue("
    ({col_to_test_port} >= {col_to_test_mean_port} + i*{col_to_test_sd_port} &
    {col_to_test_port} <= {col_to_test_mean_port} + 50*{col_to_test_sd_port}
    )
  ")

  total_trades_control <-
    model_predicted_data %>%
    group_by(Asset) %>%
    summarise(Total_Trades_Control = n_distinct(Date),
              Final_Return_Control = sum(Final_Return, na.rm = T),
              # Average_Return_Control = Final_Return_Control/Total_Trades_Control
              Average_Return_Control = mean(Final_Return, na.rm = T),
              sd_Return_Control = sd(Final_Return, na.rm = T)
    ) %>%
    ungroup() %>%
    bind_rows(
      model_predicted_data %>%
        summarise(Total_Trades_Control = n_distinct(Date),
                  Final_Return_Control = sum(Final_Return, na.rm = T),
                  # Average_Return_Control = Final_Return_Control/Total_Trades_Control
                  Average_Return_Control = mean(Final_Return, na.rm = T),
                  sd_Return_Control = sd(Final_Return, na.rm = T)
        ) %>%
        ungroup()  %>%
        mutate(
          Asset = "Portfolio"
        )
    )

  trades_per_year <-
    model_predicted_data %>%
    mutate(
      trade_col = eval(parse(text = trade_statment))
    ) %>%
    mutate(
      trade_col = case_when(trade_col == TRUE ~ "Long", TRUE ~ "No Trade")
    ) %>%
    filter(trade_col == "Long") %>%
    mutate(xx = month(Date)) %>%
    group_by(Asset, xx) %>%
    summarise(Total_Trades = n_distinct(Date)) %>%
    group_by(Asset) %>%
    summarise(Average_Trades_Per_Month = mean(Total_Trades, na.rm = T)) %>%
    ungroup()

  returns_total <-
    model_predicted_data %>%
    mutate(
      trade_col = eval(parse(text = trade_statment))
    ) %>%
    mutate(
      trade_col = case_when(trade_col == TRUE ~ "Long", TRUE ~ "No Trade")
    ) %>%
    filter(trade_col == "Long") %>%
    group_by(Asset) %>%
    summarise(Final_Return = sum(Final_Return, na.rm = T)) %>%
    mutate(
      threshold = i,
      col_to_test = col_to_test
    ) %>%
    ungroup() %>%
    mutate(
      Total_Final = sum(Final_Return, na.rm = T)
    )

  returns_total_TRU_POS <-
    model_predicted_data %>%
    mutate(
      trade_col = eval(parse(text = trade_statment_port))
    ) %>%
    mutate(
      trade_col = case_when(trade_col == TRUE ~ "Long", TRUE ~ "No Trade")
    ) %>%
    group_by(Date, trade_col) %>%
    summarise(Final_Return = sum(Final_Return, na.rm = TRUE))  %>%
    ungroup() %>%
    mutate(
      pos_detect_TRUE =
        ifelse(trade_col == "Long" & Final_Return > return_thresh, 1, 0),
      pos_detect_Ned =
        ifelse(trade_col == "Long" & Final_Return <= return_thresh, 1, 0),

      neg_detect_TRUE =
        ifelse(trade_col == "No Trade" & Final_Return <= return_thresh, 1, 0),
      neg_detect_Ned =
        ifelse(trade_col == "No Trade" & Final_Return > return_thresh, 1, 0)

    ) %>%
    summarise(
      Total_Wins = sum(pos_detect_TRUE, na.rm=T),
      Total_Trades = sum(pos_detect_TRUE, na.rm=T) + sum(pos_detect_Ned, na.rm=T),
      Total_No_Trades = sum(neg_detect_Ned, na.rm=T) + sum(neg_detect_TRUE, na.rm=T),
      TRUE_pos_rate = Total_Wins/Total_Trades,
      TRUE_neg_rate = sum(neg_detect_TRUE, na.rm = T)/Total_No_Trades
    ) %>%
    mutate(
      threshold = i,
      col_to_test = col_to_test,
      Asset = "Portfolio"
    ) %>%
    ungroup()

  auc_roc_list[[c]] <-
    model_predicted_data %>%
    mutate(
      trade_col = eval(parse(text = trade_statment))
    ) %>%
    mutate(
      trade_col = case_when(trade_col == TRUE ~ "Long", TRUE ~ "No Trade")
    ) %>%
    mutate(
      pos_detect_TRUE =
        ifelse(trade_col == "Long" & Final_Return > return_thresh, 1, 0),
      pos_detect_Ned =
        ifelse(trade_col == "Long" & Final_Return <= return_thresh, 1, 0),

      neg_detect_TRUE =
        ifelse(trade_col == "No Trade" & Final_Return <= return_thresh, 1, 0),
      neg_detect_Ned =
        ifelse(trade_col == "No Trade" & Final_Return > return_thresh, 1, 0)

    ) %>%
    group_by(Asset) %>%
    summarise(
      Total_Wins = sum(pos_detect_TRUE, na.rm=T),
      Total_Trades = sum(pos_detect_TRUE, na.rm=T) + sum(pos_detect_Ned, na.rm=T),
      Total_No_Trades = sum(neg_detect_Ned, na.rm=T) + sum(neg_detect_TRUE, na.rm=T),
      TRUE_pos_rate = Total_Wins/Total_Trades,
      TRUE_neg_rate = sum(neg_detect_TRUE, na.rm = T)/Total_No_Trades,
      Average_Return = mean(ifelse(trade_col == "Long", Final_Return, NA), na.rm = T),
      sd_Return = sd(ifelse(trade_col == "Long", Final_Return, NA), na.rm = T)
    ) %>%
    mutate(
      threshold = i,
      col_to_test = col_to_test
    ) %>%
    bind_rows(returns_total_TRU_POS) %>%
    left_join(returns_total) %>%
    fill(Total_Final, .direction = "down") %>%
    mutate(
      Final_Return = ifelse(is.na(Final_Return) & Total_Trades > 0, sum(Final_Return, na.rm = T), Final_Return),
      Final_Return = ifelse(Total_Trades == 0, 0, Final_Return)
    ) %>%
    left_join(total_trades_control) %>%
    mutate() %>%
    left_join(trades_per_year) %>%
    mutate(
      Avg_Returns_Per_Month = Average_Return*Average_Trades_Per_Month,
      # percentile_10th = 250*quantile(rnorm(n = 5000, mean = Average_Return, sd = sd_Return), 0.1, na.rm = T),
      # percentile_20th = 250*quantile(rnorm(n = 5000, mean = Average_Return, sd = sd_Return), 0.2, na.rm = T),
      # percentile_40th = 250*quantile(rnorm(n = 5000, mean = Average_Return, sd = sd_Return), 0.4, na.rm = T)
    ) %>%
    split(.$Asset, drop = FALSE) %>%
    map_dfr(
      ~ .x %>%
        mutate(percentile_30th = 250*quantile(rnorm(n = 15000, mean = Average_Return, sd = sd_Return), 0.3, na.rm = T))
    )

}

auc_roc <-
  auc_roc_list %>%
  map_dfr(bind_rows)

auc_roc %>%
  ggplot(aes(x = threshold, y = TRUE_pos_rate)) +
  geom_line() +
  geom_point() +
  facet_wrap(.~Asset, scales = "free") +
  theme_minimal()

auc_roc %>%
  ggplot(aes(x = threshold, y = Final_Return)) +
  geom_line() +
  geom_point() +
  facet_wrap(.~Asset, scales = "free") +
  theme_minimal()

auc_roc %>%
  ggplot(aes(x = threshold, y = Average_Return)) +
  geom_line() +
  geom_point() +
  facet_wrap(.~Asset, scales = "free") +
  theme_minimal()

auc_roc %>%
  ggplot(aes(x = threshold, y = Avg_Returns_Per_Month)) +
  geom_line() +
  geom_point() +
  facet_wrap(.~Asset, scales = "free") +
  theme_minimal()

auc_roc %>%
  ggplot(aes(x = threshold, y = Total_Final)) +
  geom_line() +
  geom_point() +
  facet_wrap(.~Asset, scales = "free") +
  theme_minimal()

auc_roc %>%
  filter(Total_Trades >= 4000) %>%
  ggplot(aes(x = threshold, y = percentile_30th)) +
  geom_smooth() +
  geom_point(size = 0.5) +
  facet_wrap(.~Asset, scales = "free") +
  theme_minimal()

auc_roc_filt <-
  auc_roc %>%
  # filter(threshold >= 0.5) %>%
  filter(!is.nan(TRUE_pos_rate)) %>%
  group_by(Asset) %>%
  slice_max(Final_Return) %>%
  group_by(Asset) %>%
  slice_head(n = 1)

auc_roc_filt <-
  auc_roc %>%
  filter(threshold >= 0.5) %>%
  filter(Total_Trades >= 1000) %>%
  filter(!is.na(percentile_30th)) %>%
  group_by(Asset) %>%
  slice_max(TRUE_pos_rate)

# Simulation of Parameters ------------------------------------------------

names(model_predicted_data)
db_data_store_path <- "C:/Users/nikhi/Documents//trade_data/Day_Trader_Cor_Continuous_Models/MIXED_NON_V3_NEW_MODEL_STOP_10_PROF_5_RISK_3_results.db"
db_data_store <- connect_db(db_data_store_path)
redo_DB <- TRUE
kk = 0

vars_to_test <-
  names(model_predicted_data) %>%
  keep(~str_detect(.x, "pnorm")) %>%
  unlist()

for (j in 1:length(vars_to_test)) {

  return_thresh <- 0
  auc_roc_list <- list()
  col_to_test <- vars_to_test[j]
  col_to_test_port <- vars_to_test[j]
  c = 0
  kk = kk + 1

  # for (i in seq(-5,15,0.1)) {
  for (i in seq(0.01,0.99, 0.02)) {
    c = c + 1

    trade_statment <-
      glue::glue("
    ({col_to_test} > i &
    {col_to_test} < 1000)
  ")

    trade_statment_port <-
      glue::glue("
    ({col_to_test_port} > i &
    {col_to_test_port} < 1000)
  ")

    trade_statment_double <-
      glue::glue("{trade_statment} & {trade_statment_port}")

    total_trades_control <-
      model_predicted_data %>%
      group_by(Asset) %>%
      summarise(Total_Trades_Control = n_distinct(Date),
                Final_Return_Control = sum(Final_Return, na.rm = T),
                # Average_Return_Control = Final_Return_Control/Total_Trades_Control
                Average_Return_Control = mean(Final_Return, na.rm = T),
                sd_Return_Control = sd(Final_Return, na.rm = T)
      ) %>%
      ungroup()

    trades_per_year <-
      model_predicted_data %>%
      mutate(
        trade_col = eval(parse(text = trade_statment))
      ) %>%
      mutate(
        trade_col = case_when(trade_col == TRUE ~ "Long", TRUE ~ "No Trade")
      ) %>%
      filter(trade_col == "Long") %>%
      mutate(xx = month(Date)) %>%
      group_by(Asset, xx) %>%
      summarise(Total_Trades = n_distinct(Date)) %>%
      group_by(Asset) %>%
      summarise(Average_Trades_Per_Month = mean(Total_Trades, na.rm = T)) %>%
      ungroup()

    returns_total <-
      model_predicted_data %>%
      mutate(
        trade_col = eval(parse(text = trade_statment))
      ) %>%
      mutate(
        trade_col = case_when(trade_col == TRUE ~ "Long", TRUE ~ "No Trade")
      ) %>%
      filter(trade_col == "Long") %>%
      group_by(Asset) %>%
      summarise(Final_Return = sum(Final_Return, na.rm = T)) %>%
      mutate(
        threshold = i,
        col_to_test = col_to_test
      ) %>%
      ungroup() %>%
      mutate(
        Total_Final = sum(Final_Return, na.rm = T)
      )

    losses_worst <-
      model_predicted_data %>%
      mutate(
        trade_col = eval(parse(text = trade_statment))
      ) %>%
      mutate(
        trade_col = case_when(trade_col == TRUE ~ "Long", TRUE ~ "No Trade")
      ) %>%
      filter(trade_col == "Long") %>%
      group_by(Asset, Date) %>%
      summarise(Final_Return = sum(Final_Return, na.rm = T)) %>%
      mutate(
        threshold = i,
        col_to_test = col_to_test
      ) %>%
      ungroup() %>%
      group_by(Asset) %>%
      arrange(Date, .by_group = TRUE) %>%
      group_by(Asset) %>%
      mutate(
        Final_Return_Cumulative = cumsum(Final_Return)
      ) %>%
      group_by(Asset) %>%
      mutate(
        X = Final_Return_Cumulative - lag(Final_Return_Cumulative, 50),
        XX = Final_Return_Cumulative - lag(Final_Return_Cumulative, 100),
        XX2 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 150),
        XX3 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 200),
        XX4 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 400),
        XX5 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 500),
        XX6 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 1000),
        XX7 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 1250),
        XX8 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 2500),
        XX9 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 2750),
        XX10 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 3000),
        XX11 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 3250),
        XX12 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 3500),
        XX13 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 25)
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
        min8 = min(XX8, na.rm = T),
        min9 = min(XX9, na.rm = T),
        min10 = min(XX10, na.rm = T),
        min11 = min(XX11, na.rm = T),
        min12 = min(XX12, na.rm = T),
        min13 = min(XX13, na.rm = T)
      )   %>%
      ungroup() %>%
      pivot_longer(cols = -c(Asset), names_to = "min_var", values_to = "worst_downturn") %>%
      group_by(Asset) %>%
      slice_min(worst_downturn) %>%
      ungroup() %>%
      dplyr::select(-min_var)

    returns_total_TRU_POS <-
      model_predicted_data %>%
      mutate(
        trade_col = eval(parse(text = trade_statment_port))
      ) %>%
      mutate(
        trade_col = case_when(trade_col == TRUE ~ "Long", TRUE ~ "No Trade")
      ) %>%
      group_by(Date, trade_col) %>%
      summarise(Final_Return = sum(Final_Return, na.rm = TRUE))  %>%
      ungroup() %>%
      mutate(
        pos_detect_TRUE =
          ifelse(trade_col == "Long" & Final_Return > return_thresh, 1, 0),
        pos_detect_Ned =
          ifelse(trade_col == "Long" & Final_Return <= return_thresh, 1, 0),

        neg_detect_TRUE =
          ifelse(trade_col == "No Trade" & Final_Return <= return_thresh, 1, 0),
        neg_detect_Ned =
          ifelse(trade_col == "No Trade" & Final_Return > return_thresh, 1, 0)

      ) %>%
      summarise(
        Total_Wins = sum(pos_detect_TRUE, na.rm=T),
        Total_Trades = sum(pos_detect_TRUE, na.rm=T) + sum(pos_detect_Ned, na.rm=T),
        Total_No_Trades = sum(neg_detect_Ned, na.rm=T) + sum(neg_detect_TRUE, na.rm=T),
        TRUE_pos_rate = Total_Wins/Total_Trades,
        TRUE_neg_rate = sum(neg_detect_TRUE, na.rm = T)/Total_No_Trades
      ) %>%
      mutate(
        threshold = i,
        col_to_test = col_to_test,
        Asset = "Portfolio"
      ) %>%
      ungroup()

    auc_roc_list[[c]] <-
      model_predicted_data %>%
      mutate(
        trade_col = eval(parse(text = trade_statment))
      ) %>%
      mutate(
        trade_col = case_when(trade_col == TRUE ~ "Long", TRUE ~ "No Trade")
      ) %>%
      mutate(
        pos_detect_TRUE =
          ifelse(trade_col == "Long" & Final_Return > return_thresh, 1, 0),
        pos_detect_Ned =
          ifelse(trade_col == "Long" & Final_Return <= return_thresh, 1, 0),

        neg_detect_TRUE =
          ifelse(trade_col == "No Trade" & Final_Return <= return_thresh, 1, 0),
        neg_detect_Ned =
          ifelse(trade_col == "No Trade" & Final_Return > return_thresh, 1, 0)

      ) %>%
      group_by(Asset) %>%
      summarise(
        Total_Wins = sum(pos_detect_TRUE, na.rm=T),
        Total_Trades = sum(pos_detect_TRUE, na.rm=T) + sum(pos_detect_Ned, na.rm=T),
        Total_No_Trades = sum(neg_detect_Ned, na.rm=T) + sum(neg_detect_TRUE, na.rm=T),
        TRUE_pos_rate = Total_Wins/Total_Trades,
        TRUE_neg_rate = sum(neg_detect_TRUE, na.rm = T)/Total_No_Trades,
        Average_Return = mean(ifelse(trade_col == "Long", Final_Return, NA), na.rm = T),
        sd_Return = sd(ifelse(trade_col == "Long", Final_Return, NA), na.rm = T)
      ) %>%
      mutate(
        threshold = i,
        col_to_test = col_to_test
      ) %>%
      bind_rows(returns_total_TRU_POS) %>%
      left_join(returns_total) %>%
      fill(Total_Final, .direction = "down") %>%
      mutate(
        Final_Return = ifelse(is.na(Final_Return) & Total_Trades > 0, sum(Final_Return, na.rm = T), Final_Return),
        Final_Return = ifelse(Total_Trades == 0, 0, Final_Return)
      ) %>%
      left_join(total_trades_control) %>%
      mutate() %>%
      left_join(trades_per_year) %>%
      mutate(
        Avg_Returns_Per_Month = Average_Return*Average_Trades_Per_Month,
        # percentile_10th = 250*quantile(rnorm(n = 5000, mean = Average_Return, sd = sd_Return), 0.1, na.rm = T),
        # percentile_20th = 250*quantile(rnorm(n = 5000, mean = Average_Return, sd = sd_Return), 0.2, na.rm = T),
        # percentile_40th = 250*quantile(rnorm(n = 5000, mean = Average_Return, sd = sd_Return), 0.4, na.rm = T)
      ) %>%
      split(.$Asset, drop = FALSE) %>%
      map_dfr(
        ~ .x %>%
          mutate(percentile_30th = 250*quantile(rnorm(n = 15000, mean = Average_Return, sd = sd_Return), 0.3, na.rm = T))
      ) %>%
      left_join(losses_worst)

  }

  auc_roc <-
    auc_roc_list %>%
    map_dfr( ~ .x %>% filter(Total_Trades > 1000))

  if(  kk== 1 & redo_DB == TRUE) {
    write_table_sql_lite(.data = auc_roc,
                         table_name = "MODEL_RESULTS",
                         conn = db_data_store)
    redo_DB = FALSE
  } else  {
    append_table_sql_lite(.data = auc_roc,
                          table_name = "MODEL_RESULTS",
                          conn = db_data_store)
  }

  # auc_roc %>%
  #   ggplot(aes(x = threshold, y = TRUE_pos_rate)) +
  #   geom_line() +
  #   geom_point() +
  #   facet_wrap(.~Asset, scales = "free") +
  #   theme_minimal()
  #
  # auc_roc %>%
  #   ggplot(aes(x = threshold, y = Final_Return)) +
  #   geom_line() +
  #   geom_point() +
  #   facet_wrap(.~Asset, scales = "free") +
  #   theme_minimal()
  #
  # auc_roc %>%
  #   ggplot(aes(x = threshold, y = Average_Return)) +
  #   geom_line() +
  #   geom_point() +
  #   facet_wrap(.~Asset, scales = "free") +
  #   theme_minimal()
  #
  # auc_roc %>%
  #   ggplot(aes(x = threshold, y = Avg_Returns_Per_Month)) +
  #   geom_line() +
  #   geom_point() +
  #   facet_wrap(.~Asset, scales = "free") +
  #   theme_minimal()
  #
  # auc_roc %>%
  #   ggplot(aes(x = threshold, y = Total_Final)) +
  #   geom_line() +
  #   geom_point() +
  #   facet_wrap(.~Asset, scales = "free") +
  #   theme_minimal()
  #
  # auc_roc %>%
  #   filter(Total_Trades >= 1000) %>%
  #   ggplot(aes(x = threshold, y = percentile_30th)) +
  #   geom_smooth() +
  #   geom_point(size = 0.5) +
  #   facet_wrap(.~Asset, scales = "free") +
  #   theme_minimal()

  auc_roc_filt <-
    auc_roc %>%
    filter(threshold >= 0.5) %>%
    filter(!is.nan(TRUE_pos_rate)) %>%
    group_by(Asset) %>%
    slice_max(Final_Return) %>%
    group_by(Asset) %>%
    slice_head(n = 1)

  auc_roc_filt_max_prob <-
    auc_roc %>%
    filter(threshold >= 0.5) %>%
    filter(Total_Trades > 1000) %>%
    filter(!is.na(TRUE_pos_rate)) %>%
    group_by(Asset) %>%
    slice_max(TRUE_pos_rate)


}

all_sim_data <-
  DBI::dbGetQuery(conn = db_data_store, "SELECT * FROM MODEL_RESULTS")

best_returns <-
  all_sim_data %>%
  filter(threshold >= 0.55) %>%
  group_by(col_to_test, Asset) %>%
  slice_max(Final_Return) %>%
  ungroup() %>%
  group_by(Asset) %>%
  slice_max(Final_Return, n = 3)
