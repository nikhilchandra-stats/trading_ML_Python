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
start_date = "2022-01-01"
end_date = today() %>% as.character()
Indices_Metals_Bonds <- list()

assets_to_port =
  c(
    "SPX500_USD",
    "DE30_EUR",
    "XAU_USD",
    "USD_JPY",
    "JP225_USD"
  ) %>% unique()

tictoc::tic()

Indices_Metals_Bonds[[1]] <-
  get_db_data_quickly_algo(
    db_location = db_location,
    start_date = start_date,
    end_date = as.character(today() + days(30)),
    time_frame = "H1",
    bid_or_ask = "ask",
    assets =   assets_to_port
  ) %>%
  distinct()
Indices_Metals_Bonds[[2]] <-
  get_db_data_quickly_algo(
    db_location = db_location,
    start_date = start_date,
    end_date = as.character(today() + days(30)),
    time_frame = "H1",
    bid_or_ask = "bid",
    assets =   assets_to_port
  ) %>%
  distinct()

Indices_Metals_Bonds[[1]] <- Indices_Metals_Bonds[[1]] %>% filter(Date >= "2022-01-01")
Indices_Metals_Bonds[[2]] <- Indices_Metals_Bonds[[2]] %>% filter(Date >= "2022-01-01")

stop_factor_var = 10
profit_factor_var = 40
risk_dollar_value_var = 5
end_period = 130
trade_direction = "Long"
end_point_loss = -5
end_point_profit = 70

regression_length = 25000
direct_return_cols = 24
lag_value_error = end_period + 1
low_to_price_lengths = c(400)
cor_periods = c(50)
dependant_var = "Final_Return"
save_location = "D:/trade_data/Day_Trader_Cor_Continuous_Models/"
model_prefix = "Equity"
training_date <- "2022-08-16 18:00:00 AEST"

model_prediction_data <-
  gen_port_LM_with_Errors_Bayes_Preds_algo(
    Indices_Metals_Bonds = Indices_Metals_Bonds,
    assets_to_port = assets_to_port,
    stop_factor_var = stop_factor_var,
    profit_factor_var = profit_factor_var,
    risk_dollar_value_var = risk_dollar_value_var,
    end_period = end_period,
    trade_direction = trade_direction,
    currency_conversion = currency_conversion,
    asset_infor = asset_infor,
    end_point_loss = end_point_loss,
    end_point_profit = end_point_profit,
    low_to_price_lengths = low_to_price_lengths,
    cor_periods = cor_periods,
    regression_length = regression_length,
    training_date =  training_date,
    save_location = save_location,
    model_prefix = model_prefix,
    correlation_data = NULL,
    direct_return_cols = direct_return_cols,
    lag_value_error = lag_value_error,
    filter_na_for_values = TRUE
  )

tictoc::toc()

model_prediction_data <-
  model_prediction_data %>%
  mutate(
    rolling_predicted_10000_50_sd =
      slider::slide_dbl(predicted, .f = ~ sd(.x, na.rm = T), .before = 50),
    rolling_predicted_10000_100_sd =
      slider::slide_dbl(predicted, .f = ~ sd(.x, na.rm = T), .before = 100),
    rolling_predicted_10000_200_sd =
      slider::slide_dbl(predicted, .f = ~ sd(.x, na.rm = T), .before = 200),
    rolling_predicted_10000_400_sd =
      slider::slide_dbl(predicted, .f = ~ sd(.x, na.rm = T), .before = 400),

    rolling_predicted_10000_2000_sd =
      slider::slide_dbl(predicted, .f = ~ sd(.x, na.rm = T), .before = 2000),
    rolling_predicted_10000_2000 =
      slider::slide_dbl(predicted, .f = ~ mean(.x, na.rm = T), .before = 2000),

    portfolio_pred_10000_mean_roll_600 =
      slider::slide_dbl(.x  = portfolio_pred_10000, .f = ~ mean(.x, na.rm = T), .before = 600),
    portfolio_pred_10000_sd_roll_600 =
      slider::slide_dbl(.x  = portfolio_pred_10000, .f = ~ sd(.x, na.rm = T), .before = 600),

    portfolio_pred_10000_mean_roll_1000 =
      slider::slide_dbl(.x  = portfolio_pred_10000, .f = ~ mean(.x, na.rm = T), .before = 1000),
    portfolio_pred_10000_sd_roll_1000 =
      slider::slide_dbl(.x  = portfolio_pred_10000, .f = ~ sd(.x, na.rm = T), .before = 1000)
  ) %>%
  dplyr::select(Date, Asset, contains("pred"), Final_Return)

trade_statment <-
  "(portfolio_pred_10000 > 10)|(predicted > 7)|((portfolio_pred_10000_mean_roll_50 > 11))|
   (predicted > rolling_predicted_10000_2000 + 0.9*rolling_predicted_10000_2000_sd &
   predicted < rolling_predicted_10000_2000 + 2.1*rolling_predicted_10000_2000_sd)|
   (predicted > rolling_predicted_10000_400 + 0.75*rolling_predicted_10000_400_sd &
   predicted < rolling_predicted_10000_400 + 1.75*rolling_predicted_10000_400_sd)|
   (predicted > rolling_predicted_10000_200 + 0.5*rolling_predicted_10000_200_sd &
   predicted < rolling_predicted_10000_200 + 1.5*rolling_predicted_10000_200_sd)"

trade_statment <-
  "(portfolio_pred_10000 > portfolio_pred_10000_mean_roll_50 + 1.5*portfolio_pred_10000_sd_roll_50 &
   portfolio_pred_10000 < portfolio_pred_10000_mean_roll_50 + 10*portfolio_pred_10000_sd_roll_50)|
   (portfolio_pred_10000 > portfolio_pred_10000_mean_roll_250 + 1.5*portfolio_pred_10000_sd_roll_250 &
   portfolio_pred_10000 < portfolio_pred_10000_mean_roll_250 + 10*portfolio_pred_10000_sd_roll_250)|
   (portfolio_pred_10000 > portfolio_pred_10000_mean_roll_400 + 1.25*portfolio_pred_10000_sd_roll_400 &
   portfolio_pred_10000 < portfolio_pred_10000_mean_roll_400 + 10*portfolio_pred_10000_sd_roll_400 )|
   (portfolio_pred_10000 > portfolio_pred_10000_mean_roll_1000 + 1.2*portfolio_pred_10000_sd_roll_1000 &
   portfolio_pred_10000 < portfolio_pred_10000_mean_roll_1000 + 10*portfolio_pred_10000_sd_roll_1000 )|
   (portfolio_pred_10000 > portfolio_pred_10000_mean_roll_600 + 1.25*portfolio_pred_10000_sd_roll_600 &
   portfolio_pred_10000 < portfolio_pred_10000_mean_roll_600 + 10*portfolio_pred_10000_sd_roll_600 )|
   (portfolio_pred_10000 > 17)
"

trade_statment <-
  "(predicted < 7 & predicted > 2.5)"



analyse_performance <-
  model_prediction_data %>%
  mutate(
    trade_col = eval(parse(text = trade_statment))
  ) %>%
  mutate(
    trade_col = case_when(trade_col == TRUE ~ "Long", TRUE ~ "No Trade")
  )

dim(analyse_performance %>% filter(trade_col == "Long"))[1]
dim(analyse_performance)[1]

control_data <-
  model_prediction_data %>%
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

analyse_performance %>%
  bind_rows(control_data) %>%
  # filter(Date <= "2022-01-31") %>%
  ggplot(aes(x = Date, y = Final_Return_Cumulative, color = trade_col)) +
  geom_line(size = 0.8) +
  facet_wrap(.~trade_col, scales = "free") +
  theme_minimal() +
  scale_y_continuous(n.breaks = 30, labels = scales::label_dollar()) +
  theme(legend.position = "bottom")

total_worst_case <- run_all_sims_temp(samples_x = 200000,
                                      sim_length = 300,
                                      control_data = control_data,
                                      analyse_performance = analyse_performance)


sim_result_list <- list()
control_data_asset <-
  model_prediction_data %>%
  group_by(Date, Asset) %>%
  summarise(Final_Return = sum(Final_Return)) %>%
  ungroup() %>%
  group_by(Asset) %>%
  arrange(Date, .by_group = TRUE) %>%
  group_by(Asset) %>%
  mutate(Final_Return_Cumulative = cumsum(Final_Return)) %>%
  ungroup() %>%
  mutate(trade_col = "Control")

analyse_performance_asset <-
  analyse_performance %>%
  filter(trade_col == "Long") %>%
  group_by(Date, Asset) %>%
  summarise(Final_Return = sum(Final_Return)) %>%
  ungroup() %>%
  group_by(Asset) %>%
  arrange(Date, .by_group = TRUE) %>%
  group_by(Asset) %>%
  mutate(Final_Return_Cumulative = cumsum(Final_Return)) %>%
  ungroup() %>%
  mutate(trade_col = "Long")

for (i in 1:length(assets_to_port) ) {

  sim_result_list[[i]] <-
    run_all_sims_temp(samples_x = 100000,
                      sim_length = 1000,
                      control_data = control_data_asset %>% filter(Asset == assets_to_port[i]),
                      analyse_performance = analyse_performance_asset %>% filter(Asset == assets_to_port[i])) %>%
    mutate(Asset = assets_to_port[i])

}

worst_cases <-
  sim_result_list %>% map_dfr(bind_rows)

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
  group_by(Asset) %>%
  summarise(
    TRUE_pos_rate = sum(pos_detect_TRUE, na.rm=T)/( sum(pos_detect_TRUE, na.rm = T) + sum(pos_detect_Ned, na.rm = T) ),
    TRUE_neg_rate = sum(neg_detect_TRUE, na.rm = T)/( sum(neg_detect_TRUE, na.rm = T) + sum(neg_detect_Ned, na.rm = T))
  )


run_all_sims_temp <-
  function(
    samples_x = 100000,
    sim_length = 1000,
    control_data,
    analyse_performance
  ) {

    sampled_control <- numeric(samples_x)
    sampled_trades <- numeric(samples_x)
    dim_control <- dim(control_data)[1]
    dim_trades <- dim(analyse_performance)[1]
    returns_control <- control_data %>% pull(Final_Return) %>% as.numeric()
    returns_trades <- analyse_performance %>% pull(Final_Return) %>% as.numeric()

    for (i in 1:samples_x) {

      control_samples <- round(runif(n= 1, min = 1, max = dim_control - sim_length))
      trade_samples <- round(runif(n= 1, min = 1, max = dim_trades - sim_length))

      sampled_control[i] <- sum(returns_control[control_samples:(control_samples + sim_length)])
      sampled_trades[i] <- sum(returns_trades[trade_samples:(trade_samples + sim_length)])

    }

    summary_results <-
      tibble(
        mean_results_control = mean(sampled_control, na.rm = T),
        mean_results_trades = mean(sampled_trades, na.rm = T),
        quan_95_control = quantile(sampled_control,0.95 ,na.rm = T),
        quan_95_trades = quantile(sampled_trades,0.95 ,na.rm = T),
        quan_05_control = quantile(sampled_control,0.05 ,na.rm = T),
        quan_05_trades = quantile(sampled_trades,0.05 ,na.rm = T),
        quan_01_control = quantile(sampled_control,0.0001 ,na.rm = T),
        quan_01_trades = quantile(sampled_trades,0.0001 ,na.rm = T)
      )

    return(summary_results)
  }


