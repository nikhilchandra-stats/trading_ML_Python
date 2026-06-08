helpeR::load_custom_functions()

all_aud_symbols <- get_oanda_symbols() %>%
  keep(~ str_detect(.x, "AUD")|str_detect(.x, "USD_SEK|USD_NOK|USD_HUF|USD_ZAR|USD_CNY|USD_MXN"))
asset_infor <- get_instrument_info()
aud_assets <- read_all_asset_data_intra_day(
  asset_list_oanda = all_aud_symbols,
  save_path_oanda_assets = "C:/Users/nikhi/Documents/trade_data//oanda_data/",
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
db_location = "C:/Users/nikhi/Documents/Asset Data/Oanda_Asset_Data_Most_Assets_2025-09-13.db"
start_date = "2019-01-01"
end_date = today() %>% as.character()
Indices_Metals_Bonds <- list()

assets_to_port <-
  c(
    "SPX500_USD",
    "AU200_AUD",
    "EU50_EUR",
    "US2000_USD",
    "XAU_USD",
    "XCU_USD",
    "AUD_USD",
    "UK100_GBP",
    "USD_JPY",
    "WTICO_USD",
    "HK33_HKD",
    "USD_SEK"

  ) %>% unique()

assets_to_trade <-
  c(
    "SPX500_USD",
    "AU200_AUD",
    "EU50_EUR",
    "US2000_USD",
    "XAU_USD",
    "XCU_USD",
    "AUD_USD",
    "UK100_GBP",
    "USD_JPY",
    "WTICO_USD",
    "HK33_HKD",
    "USD_SEK"

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

all_preds <-
  Portfolio_get_all_preds_frm_V3(
    Indices_Metals_Bonds = Indices_Metals_Bonds,
    raw_macro_data = raw_macro_data,
    base_path = "C:/Users/nikhi/Documents/trade_data/Day_Trader_Single_Asset_V3_Expanded_Models/",
    actuals_periods_needed = c("period_return_50_Price"),
    state_space_periods = c(20, 40, 60, 100, 200,300, 400,  500),
    state_space_rolling = c(100, 200, 300, 400),
    date_for_true_simualtion = "2019-01-01",
    training_end_date = "2021-01-01",
    assets_to_test = assets_to_port
  )

stop_factor_var = 10
profit_factor_var = 15
risk_dollar_value_var = 10
end_period = 24
trade_direction = "Long"
end_point_loss = -10
end_point_profit = 20
training_date <-  "2022-01-17 10:00:00 AEST"
save_path = "C:/Users/nikhi/Documents/trade_data/single_asset_v3_Bayes_Reg_Portfolio/"
file_name = "Equity_Port_V3_Bayes_More_vars"
regression_length = 18000
direct_return_cols = 23

tictoc::tic()
all_cor_V3_Data <-
  portfolio_get_V3_cor_data_TOTAL_SUMMED(
    Indices_Metals_Bonds = Indices_Metals_Bonds,
    all_preds = all_preds,
    assets_to_port = assets_to_port,
    # low_to_price_lengths = c(200, 50),
    low_to_price_lengths = c(200, 50, 400),
    cor_periods = c(100),
    max_regs = 1000
  )

all_cor_var_combos <-
  names(all_cor_V3_Data[[1]]) %>%
  keep(~ !str_detect(.x, "Date")) %>%
  unlist()
error_correcting_vars <- all_cor_var_combos %>%
  keep(~ str_detect(.x, "AR_")) %>%
  unlist()

additional_cor_vars <- list()
c = 0
for (i in 1:length(all_cor_var_combos)) {
  for (j in 1:length(all_cor_var_combos)) {

    if(all_cor_var_combos[i] != all_cor_var_combos[j] ) {
      c = c + 1
      additional_cor_vars[[c]] <- c(all_cor_var_combos[i], all_cor_var_combos[j], paste0("Cor_V3_LM_", c) )
    }

  }
}

wanted_period_cols <-
  seq(1,direct_return_cols,1) %>%
  map(~ glue::glue("period_return_{.x}_Price")) %>%
  unlist()

portfolio_data_training_data <-
  get_portfolio_model_fast_summed(
    asset_data = Indices_Metals_Bonds %>%
      map( ~ .x %>% filter( Date <= training_date ) ),
    # asset_of_interest = assets_to_port,
    asset_of_interest = assets_to_trade,
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
  group_by(Date,end_point_loss ,
           end_point_profit, risk_dollar_value, stop_factor, profit_factor) %>%
  summarise(Final_Return = sum(Final_Return, na.rm = T),
            across(contains( "period_return_"), ~ sum(., na.rm = T)) ) %>%
  ungroup() %>%
  dplyr::select(Date,
                end_point_loss ,
                end_point_profit, risk_dollar_value, stop_factor, profit_factor,
                Final_Return, matches(wanted_period_cols))

temp_reg_data_train <-
  portfolio_get_V3_cor_data_TOTAL_SUMMED_reg_dat(
    all_dat_pivoted  = all_cor_V3_Data[[1]] %>% filter(Date <= training_date),
    correlation_data = all_cor_V3_Data[[2]] %>% filter(Date <= training_date),
    portfolio_data = portfolio_data_training_data,
    additional_cor_vars = additional_cor_vars,
    error_calc_cols = error_correcting_vars %>% unique(),
    lag_dependant = end_period + 1,
    total_lag_cols = 40,
    direct_return_cols = direct_return_cols
  )

all_cor_vars <-
  names(temp_reg_data_train) %>%
  keep(~ str_detect(.x, "cor_LM[0-9]+")|str_detect(.x, "Final_Return_lag")|str_detect(.x, "Period_Return_Lag_")) %>%
  unlist() %>%
  as.character() %>%
  unique()

results_temp <-
  Porfolio_generate_V3_TOTAL_SUM_Bayes(
    reg_dat = temp_reg_data_train %>% filter(Date <= training_date),
    reg_vars =
      c(all_cor_V3_Data[[3]],all_cor_vars) %>%
      unique(),
    training_end_date = training_date,
    regression_length = regression_length,
    dependant_var = "Final_Return",
    save_path = save_path,
    file_name = file_name,
    Bayes_or_LM = "Bayes",
    sig_thresh_LM = 0.00001
  )

tictoc::toc()

#--------------------------------------Testing

wanted_period_cols <-
  seq(1,direct_return_cols,1) %>%
  map(~ glue::glue("period_return_{.x}_Price")) %>%
  unlist()

portfolio_data_testing_data <-
  get_portfolio_model_fast_summed(
    asset_data = Indices_Metals_Bonds %>%
      map( ~ .x %>% filter( Date > ( as_date(training_date) - months(6)) ) ),
    # asset_of_interest = assets_to_port,
    asset_of_interest = assets_to_trade,
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
  group_by(Date,end_point_loss ,
           end_point_profit, risk_dollar_value, stop_factor, profit_factor) %>%
  summarise(Final_Return = sum(Final_Return, na.rm = T),
            across(contains( "period_return_"), ~ sum(., na.rm = T)) ) %>%
  ungroup() %>%
  dplyr::select(Date,
                end_point_loss ,
                end_point_profit, risk_dollar_value, stop_factor, profit_factor,
                Final_Return, matches(wanted_period_cols))

temp_reg_data_testing <-
  portfolio_get_V3_cor_data_TOTAL_SUMMED_reg_dat(
    all_dat_pivoted  = all_cor_V3_Data[[1]],
    correlation_data = all_cor_V3_Data[[2]],
    portfolio_data = portfolio_data_testing_data,
    additional_cor_vars = additional_cor_vars,
    error_calc_cols = error_correcting_vars %>% unique() ,
    lag_dependant = end_period + 1,
    total_lag_cols = 40,
    direct_return_cols = direct_return_cols
  )

all_cor_vars <-
  names(temp_reg_data_testing) %>%
  keep(~ str_detect(.x, "cor_LM[0-9]+")|str_detect(.x, "Final_Return_lag")|str_detect(.x, "Period_Return_Lag_")) %>%
  unlist() %>%
  as.character() %>%
  unique()

testing_prediction_data <-
  Porfolio_get_preds_V3_TOTAL_SUM_Bayes(
    reg_dat = temp_reg_data_testing %>% filter(Date >= training_date),
    training_end_date = training_date,
    save_path = save_path,
    file_name = file_name,
    regression_length = regression_length
  )

model_prediction_data <-
  testing_prediction_data %>%
  filter(Date > training_date) %>%
  arrange(Date) %>%
  mutate(
    pred_10000_mean_roll_250 =
      slider::slide_dbl(.x  = predicted, .f = ~ mean(.x, na.rm = T), .before = 250),
    pred_10000_sd_roll_250 =
      slider::slide_dbl(.x  = predicted, .f = ~ sd(.x, na.rm = T), .before = 250),

    pred_10000_mean_roll_500 =
      slider::slide_dbl(.x  = predicted, .f = ~ mean(.x, na.rm = T), .before = 500),
    pred_10000_sd_roll_500 =
      slider::slide_dbl(.x  = predicted, .f = ~ sd(.x, na.rm = T), .before = 500),

    pred_10000_mean_roll_100 =
      slider::slide_dbl(.x  = predicted, .f = ~ mean(.x, na.rm = T), .before = 100),
    pred_10000_sd_roll_100 =
      slider::slide_dbl(.x  = predicted, .f = ~ sd(.x, na.rm = T), .before = 100),

    pred_10000_mean_roll_600 =
      slider::slide_dbl(.x  = predicted, .f = ~ mean(.x, na.rm = T), .before = 600),
    pred_10000_sd_roll_600 =
      slider::slide_dbl(.x  = predicted, .f = ~ sd(.x, na.rm = T), .before = 600)
  )

# db_con <- connect_db(path = "C:/Users/nikhi/Documents/trade_data/Equities_Bayes_Model_Results_1.db")
# append_table_sql_lite(.data = model_prediction_data,
#                      table_name = "Equities_Bayes_Model_V3",
#                      conn = db_con)

# db_con <- connect_db(path = "C:/Users/nikhi/Documents/trade_data/Equities_Bayes_Model_Results_1.db")
# model_prediction_data <-
#   DBI::dbGetQuery(conn = db_con, statement = "SELECT * FROM Equities_Bayes_Model_V3") %>%
#   mutate(Date = as_datetime(Date, tz = "Australia/Canberra"))

#10 Dollars
trade_statment <-
  "(pred_10000_mean_roll_250 > 31)|(predicted > 32.5)|(pred_10000_mean_roll_500 > 17.5)"

trade_statment <-
  "(pred_10000_mean_roll_250 > 28)|(pred_10000_mean_roll_500 > 22)|(pred_10000_mean_roll_100 > 56)|
   (predicted > trained_mean + 3.25*trained_sd)|(pred_10000_mean_roll_600 > 15)"

trade_statment <-
  "(predicted > 40)|(pred_10000_mean_roll_250 > 26.5)|(pred_10000_mean_roll_500 > 15)"

trade_statment <-
  "pred_10000_mean_roll_500 > 15"

# #Set to 400
# trade_statment <-
#   "(predicted < 80 & predicted > 50)"

analyse_performance <-
  model_prediction_data %>%
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

analyse_performance %>%
  bind_rows(control) %>%
  ggplot(aes(x = Date, y = Final_Return_Cumulative
             ,color = trade_col
  )) +
  geom_line() +
  geom_hline(yintercept = 0, linetype = "dashed", color = 'darkred') +
  facet_wrap(.~trade_col, scales = "free") +
  theme_minimal() +
  scale_y_continuous(n.breaks = 10) +
  theme(legend.position = "bottom")

# analyse_performance %>%
#   bind_rows(control) %>%
#   filter(Date <= "2022-04-01") %>%
#   ggplot(aes(x = Date, y = Final_Return_Cumulative
#              ,color = trade_col
#   )) +
#   geom_line() +
#   geom_hline(yintercept = 0, linetype = "dashed", color = 'darkred') +
#   facet_wrap(.~trade_col, scales = "free") +
#   theme_minimal() +
#   scale_y_continuous(n.breaks = 10) +
#   theme(legend.position = "bottom")


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

