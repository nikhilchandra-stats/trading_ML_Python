helpeR::load_custom_functions()

all_aud_symbols <- get_oanda_symbols() %>%
  keep(~ str_detect(.x, "AUD")|str_detect(.x, "USD_SEK|USD_NOK|USD_HUF|USD_ZAR|USD_CNY|USD_MXN"))
asset_infor <- get_instrument_info()
aud_assets <- read_all_asset_data_intra_day(
  asset_list_oanda = all_aud_symbols,
  save_path_oanda_assets = "C:/Users/Nikhil Chandra/Documents/trade_data//oanda_data/",
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
    "USB02Y_USD", "USB05Y_USD", "USB30Y_USD", "USB10Y_USD", "UK100_GBP", "GBP_CAD") %>%
  unique()

asset_infor <- get_instrument_info()
#---------------------Data
load_custom_functions()
# db_location = "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data_Most_Assets_2025-09-13 2.db"
db_location = "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data_Most_Assets_2025-09-13.db"

start_date = "2017-06-01"
end_date = today() %>% as.character()
Indices_Metals_Bonds <- list()

assets_to_port =
  c(
    "USD_CHF",
    "USD_MXN",
    "USD_SEK",
    "USD_JPY"
  ) %>% unique()

stop_factor_var = 10
profit_factor_var = 50
risk_dollar_value_var = 5
end_period = 132
trade_direction = "Long"
end_point_loss = -5
end_point_profit = 25

regression_length = 25000
direct_return_cols = 24
lag_value_error = end_period + 1
low_to_price_lengths = c(400)
cor_period = c(50)
dependant_var = "Final_Return"
save_location = "C:/Users/Nikhil Chandra/Documents/trade_data/Day_Trader_Cor_Continuous_Models/"
file_name = "USD_CURR_NON_V3_NEW_MODEL"
training_date = "2022-02-01"
testing_date = as_date(training_date) + months(3)

xtnd_ss_cols_PR_cols = c(1,5,10,20,30,40,50,60,70,80,120, 100)
xtnd_ss_cols_BR_periods = c(100,200,300, 50, 150, 250, 350, 25, 500)
lag_dependant = end_period + 1
auto_cor_cols = 40
cor_skip_periods = c(1,2,4,5,6,8,10,12,14,16)

periods_to_use_deviation = c(1,10,20,30,40,50)
mean_periods_deviation = c(50, 100)

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

Indices_Metals_Bonds[[1]] <- Indices_Metals_Bonds[[1]] %>% filter(Date <= training_date)
Indices_Metals_Bonds[[2]] <- Indices_Metals_Bonds[[2]] %>% filter(Date <= training_date)

portfolio_data_train <-
  get_portfolio_model_fast_summed(
    asset_data = Indices_Metals_Bonds %>% map(~ .x %>% filter(Date < training_date)),
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
    return_only_interested_col = FALSE,
    return_only_Final = TRUE
  )

rm(Indices_Metals_Bonds)
gc()

temp_reg_data_train <-
  get_portfolio_dat_no_V3_New(
    portfolio_data = portfolio_data_train,
    xtnd_ss_cols_PR_cols = xtnd_ss_cols_PR_cols,
    xtnd_ss_cols_BR_periods = xtnd_ss_cols_BR_periods,
    lag_dependant = lag_dependant,
    auto_cor_cols = auto_cor_cols,
    cor_skip_periods = cor_skip_periods,
    cor_period = cor_period,
    periods_to_use_deviation = periods_to_use_deviation,
    mean_periods_deviation = mean_periods_deviation
  )

all_cor_vars <-
  names(temp_reg_data_train) %>%
  keep(~ str_detect(.x, "auto_cor|brownian|state_space|single_vs_total_return")) %>%
  unlist() %>%
  as.character() %>%
  unique()

rm(portfolio_data_train)
gc()
gc()

portfolio_gen_model_no_V3_New(
  reg_dat = temp_reg_data_train,
  reg_vars = all_cor_vars,
  training_end_date = training_date,
  Bayes_or_LM = "LM",
  save_path = save_location,
  dependant_var = "Final_Return",
  sig_thresh_LM = 1,
  file_name = file_name,
  reg_samples = 50000
)

rm(temp_reg_data_train)
gc()

#-----------------------------------------------------------------------------------
load_custom_functions()
db_location = db_location
testing_date = "2021-11-01"
start_date = testing_date
end_date = today() %>% as.character()
Indices_Metals_Bonds <- list()

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


Indices_Metals_Bonds[[1]] <- Indices_Metals_Bonds[[1]] %>% filter(Date >= testing_date)
Indices_Metals_Bonds[[2]] <- Indices_Metals_Bonds[[2]] %>% filter(Date >= testing_date)

portfolio_data_test <-
  get_portfolio_model_fast_summed(
    asset_data = Indices_Metals_Bonds %>% map(~ .x %>% filter(Date > testing_date)),
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
    return_only_interested_col = FALSE,
    return_only_Final = TRUE
  )

gc()

temp_reg_data_test <-
  get_portfolio_dat_no_V3_New(
    portfolio_data = portfolio_data_test,
    xtnd_ss_cols_PR_cols = xtnd_ss_cols_PR_cols,
    xtnd_ss_cols_BR_periods = xtnd_ss_cols_BR_periods,
    lag_dependant = lag_dependant,
    auto_cor_cols = auto_cor_cols,
    cor_skip_periods = cor_skip_periods,
    cor_period = cor_period,
    periods_to_use_deviation = periods_to_use_deviation,
    mean_periods_deviation = mean_periods_deviation
  )

gc()
rm(portfolio_data_test)
gc()
rm(Indices_Metals_Bonds)
gc()

model_predicted_data <-
  portfolio_read_model_no_V3_New(
    reg_dat = temp_reg_data_test,
    training_end_date = training_date,
    save_path = save_location,
    file_name = file_name
  )

model_predicted_data <-
  model_predicted_data %>%
  filter(Date > training_date)

rm(temp_reg_data_test)
gc()


#Sig 1

trade_statment <-
  "
   (pred_portfolio_10000_mean_roll_10 < pred_portfolio_10000_mean_roll_1000 - 1.3*pred_portfolio_10000_sd_roll_1000 &
   pred_portfolio_10000_mean_roll_10 > pred_portfolio_10000_mean_roll_1000 - 40*pred_portfolio_10000_sd_roll_1000)
"

trade_statment <-
  "
   (pred_10000_mean_roll_10 > pred_10000_mean_roll_1000 + 3*pred_10000_sd_roll_1000 &
   pred_10000_mean_roll_10 < pred_10000_mean_roll_1000 + 40*pred_10000_sd_roll_1000)
"

analyse_performance <-
  model_predicted_data %>%
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
  ggplot(aes(x = Date, y = Final_Return_Cumulative
             ,color = trade_col
  )) +
  geom_line() +
  geom_hline(yintercept = 0, linetype = "dashed", color = 'darkred') +
  facet_wrap(.~trade_col, scales = "free") +
  theme_minimal() +
  scale_y_continuous(n.breaks = 20) +
  theme(legend.position = "bottom")

