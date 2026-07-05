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
start_date = "2022-06-01"
end_date = today() %>% as.character()
Indices_Metals_Bonds <- list()

assets_to_port =
  c(
    "CAD_JPY",
    "NZD_JPY",
    "GBP_JPY",
    "USD_JPY",
    "EUR_JPY",
    "AUD_JPY"
  ) %>% unique()

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

Indices_Metals_Bonds[[1]] <- Indices_Metals_Bonds[[1]] %>% filter(Date >= start_date)
Indices_Metals_Bonds[[2]] <- Indices_Metals_Bonds[[2]] %>% filter(Date >= start_date)

stop_factor_var = 10
profit_factor_var = 50
risk_dollar_value_var = 5
end_period = 130
trade_direction = "Long"
end_point_loss = -5
end_point_profit = 30

regression_length = 12000
direct_return_cols = 50
lag_value_error = end_period + 1
low_to_price_lengths = c(400)
cor_periods = c(50,100)
dependant_var = "Final_Return"
save_location = "D:/trade_data/Day_Trader_Cor_Continuous_Models/"
model_prefix = "ANTI_USD"

correlation_data <-
  get_portfolio_rolling_data(
    asset_data = Indices_Metals_Bonds[[1]],
    asset_of_interest = assets_to_port,
    low_to_price_lengths = low_to_price_lengths,
    cor_periods = cor_periods
  )

all_dates_sim <-
  correlation_data %>%
  filter(Date >= as_datetime(start_date) + dhours(regression_length) ) %>%
  pull(Date) %>%
  unique()

training_date <- all_dates_sim %>% min(na.rm = T)

portfolio_data_train <-
  get_portfolio_model_fast_summed(
    asset_data = Indices_Metals_Bonds %>% map(~ .x %>% filter(Date <= training_date)),
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
    sum_as_portfolio = TRUE,

    overwrite_volume = NULL,
    min_volume_only = FALSE,
    return_only_interested_col = FALSE
  )

# portfolio_LM_gen_static_models(
#   cor_high_diff_data = correlation_data %>% filter(Date <= training_date),
#   regression_length = regression_length,
#   portfolio_actuals_data = portfolio_data_train,
#   dependant_var = "Final_Return",
#   date_filter_train = training_date,
#   periods_back_from_train_date = 12000,
#   padding_value = 0,
#   lag_value_error = lag_value_error,
#   direct_return_cols = direct_return_cols,
#   save_location = "D:/trade_data/Day_Trader_Cor_Continuous_Models/",
#   model_prefix = "Static"
# )

reg_data_bayes_train <-
  gen_port_LM_with_Errors_Bayes_data(
    cor_high_diff_data = correlation_data %>% filter(Date <= training_date),
    regression_length = regression_length,
    portfolio_actuals_data = portfolio_data_train,
    dependant_var = dependant_var,
    date_filter_train = training_date,
    padding_value = 0,
    lag_value_error = lag_value_error,
    direct_return_cols = direct_return_cols
  )

# reg_vars <- reg_data_bayes_train[[2]]
reg_vars <-
  names(reg_data_bayes_train[[1]]) %>%
  keep(~ (.x == "Asset"|
            (str_detect(.x, "state_space")&str_detect(.x, "rolling"))|
            (str_detect(.x, "Price_diff_Low"))|
            (str_detect(.x, "Period_Return_Lag_"))|
            (str_detect(.x, "Lagged_Final_Return_"))|
            (str_detect(.x, "rolling_sum_|rolling_mean_|rolling_sd_|brownian_"))|
            (str_detect(.x, "cor"))
          # .x %in%
          # c(
          #   "Cor_Model_1_pred",
          #   "Cor_Model_1_Low_Sig_pred",
          #   "Cor_Model_2_pred",
          #   "diff_dat_model_1_pred",
          #   "diff_dat_model_1_Low_Sig_pred",
          #   "diff_dat_model_2_pred",
          #   "return_based_model_1_pred",
          #   "return_based_model_2_pred")
          #
  )
  )  %>%
  unlist()

reg_vars <- reg_data_bayes_train[[2]]

gen_port_LM_with_Errors_Bayes_Gen_Model(
  reg_data = reg_data_bayes_train[[1]],
  reg_variables = reg_vars,
  save_location = save_location,
  model_prefix = model_prefix,
  dependant_var = dependant_var,
  date_filter_train = training_date,
  padding_value = 0,
  sig_thresh_LM = 1,
  interact_list_prefix = NULL
)

gc()
# training_date <- "2022-08-16 18:00:00 AEST"
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
    correlation_data = correlation_data,
    direct_return_cols = direct_return_cols,
    lag_value_error = lag_value_error,
    filter_na_for_values = TRUE
  )

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
      slider::slide_dbl(predicted, .f = ~ mean(.x, na.rm = T), .before = 2000)
  )

trade_statment <-
  "(predicted > 17 & predicted < 1000)|
   (portfolio_pred_10000 > 10 & portfolio_pred_10000 < 25)"

trade_statment <-
  "(portfolio_pred_10000 > 10 & portfolio_pred_10000 < 25)"


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

analyse_performance %>%
  bind_rows(control_data) %>%
  # filter(Date <= "2022-01-31") %>%
  ggplot(aes(x = Date, y = Final_Return_Cumulative, color = trade_col)) +
  geom_line(size = 0.8) +
  facet_wrap(.~trade_col, scales = "free") +
  theme_minimal() +
  scale_y_continuous(n.breaks = 10, labels = scales::label_dollar()) +
  theme(legend.position = "bottom")

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



check_list <- list()
sd_check <- c(0, 1,1.25, 1.5, 1.75, 2, 2.5, 3, 3.5, 4)
for (j in 1:length(sd_check) ) {

  trade_statment <-
    glue::glue("portfolio_pred_2500 > trained_mean_2500 + {sd_check[j]}*trained_sd_2500")

  check_list[[j]] <-
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
    ) %>%
    mutate(sd_check = sd_check[j])

}

check_list_dfr <-
  check_list %>%
  map_dfr(bind_rows)
