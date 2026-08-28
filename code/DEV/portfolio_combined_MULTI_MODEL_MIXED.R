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
#---------------------Data
load_custom_functions()
db_location = "C:/Users/nikhi/Documents/Asset Data/Oanda_Asset_Data_Most_Assets_2025-09-13.db"
start_date = "2010-06-01"
end_date = today() %>% as.character()
Indices_Metals_Bonds <- list()

assets_to_port =
  c(
    "USD_SEK",
    "USD_CZK",
    "USD_MXN"
  ) %>% unique()

stop_factor_var = 5
profit_factor_var = 5
risk_dollar_value_var = 5
end_period = 132
trade_direction = "Long"
end_point_loss = -5
end_point_profit = 5

regression_length = 25000
direct_return_cols = 24
lag_value_error = end_period + 1
low_to_price_lengths = c(400)
cor_period = c(50)
dependant_var = "Final_Return"
save_location = "C:/Users/nikhi/Documents/trade_data/Day_Trader_Non_V3_Random_Models/"
training_date = "2021-02-01"
testing_date = as_date(training_date) + months(3)

xtnd_ss_cols_PR_cols = c(1,5,10,20,30,40,50,60,70,80,120, 100)
xtnd_ss_cols_BR_periods = c(100,200,300, 50, 150, 250, 350, 25, 500)
lag_dependant = end_period + 1
auto_cor_cols = 40
cor_skip_periods = c(1,2,4,5,6,8,10,12,14,16)

periods_to_use_deviation = c(1,10,20,30,40,50)
mean_periods_deviation = c(50, 100)
reg_iterations = 130
file_name_base_line = "TEST_DELETE_"

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
  get_portfolio_model_fast_summed_port_optimised(
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

for (i in 1:reg_iterations) {

  file_name = glue::glue("{file_name_base_line}{i}")

  portfolio_gen_model_no_V3_New(
    reg_dat =
      temp_reg_data_train %>%
      ungroup() %>%
      group_by(Asset) %>%
      slice_sample(n = 2500) %>%
      ungroup()  ,
    reg_vars = all_cor_vars,
    training_end_date = training_date,
    Bayes_or_LM = "LM",
    save_path = save_location,
    dependant_var = "Final_Return",
    sig_thresh_LM = 1,
    file_name = file_name,
    reg_samples = 30000
  )

  gc()

}

#-----------------------------------------------------------------------------------
load_custom_functions()
db_location = db_location
testing_date = "2022-11-01"
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
  get_portfolio_model_fast_summed_port_optimised(
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


model_predicted_data_list <- list()

for (i in 1:reg_iterations) {

  file_name = glue::glue("{file_name_base_line}{i}")

  model_predicted_data_list[[i]] <-
    portfolio_read_model_no_V3_New(
      reg_dat = temp_reg_data_test,
      training_end_date = training_date,
      save_path = save_location,
      file_name = file_name
    )

}

model_predicted_data <-
  model_predicted_data_list %>%
  map_dfr(bind_rows) %>%
  filter(Date > training_date) %>%
  group_by(Date, Asset) %>%
  summarise(Final_Return = mean(Final_Return, na.rm = T),
            predicted_mean = mean(predicted, na.rm  = T),
            predicted_25 = quantile(predicted, 0.25 ,na.rm  = T),
            predicted_50 = quantile(predicted, 0.5 ,na.rm  = T),
            predicted_75 = quantile(predicted, 0.75 ,na.rm  = T) ) %>%
  ungroup() %>%
  group_by(Date) %>%
  mutate(
    predicted_portfolio = sum(predicted_mean, na.rm = T),
    predicted_portfolio_25 = sum(predicted_25, na.rm = T),
    predicted_portfolio_50 = sum(predicted_50, na.rm = T),
    predicted_portfolio_75 = sum(predicted_75, na.rm = T)
  ) %>%
  group_by(Asset) %>%
  arrange(Date, .by_group = TRUE) %>%
  group_by(Asset) %>%
  mutate(
    pred_10000_mean_roll_250 =
      slider::slide_dbl(.x  = predicted_50, .f = ~ mean(.x, na.rm = T), .before = 250),
    pred_10000_sd_roll_250 =
      slider::slide_dbl(.x  = predicted_50, .f = ~ sd(.x, na.rm = T), .before = 250),

    pred_10000_mean_roll_250_port =
      slider::slide_dbl(.x  = predicted_portfolio_50, .f = ~ mean(.x, na.rm = T), .before = 250),
    pred_10000_sd_roll_250_port =
      slider::slide_dbl(.x  = predicted_portfolio_50, .f = ~ sd(.x, na.rm = T), .before = 250)
    )

rm(temp_reg_data_test)
gc()

trade_statment <-
  ""

trade_statment <-
  "
  (
  predicted_50 > 1 &
  predicted_50 < 1000 &
  Asset == 'XAG_EUR'
  )|
  (
  predicted_50 > 1 &
  predicted_50 < 1000 &
  Asset == 'XAG_GBP'
  )|
  (
  predicted_50 > 1 &
  predicted_50 < 1000 &
  Asset == 'XAU_EUR'
  )|
  (
  predicted_50 > 2.7 &
  predicted_50 < 5 &
  Asset == 'XAU_USD'
  )

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

auc_roc_list <- list()
col_to_test <- "predicted_50"
c = 0

for (i in seq(0,10,0.1)) {
   c = c + 1

  trade_statment <-
    glue::glue("
    ({col_to_test} > i &
    {col_to_test} < 1000)
  ")

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
    summarise(Final_Return = sum(Final_Return)) %>%
    mutate(
      threshold = i,
      col_to_test = col_to_test
    ) %>%
    ungroup() %>%
    mutate(
      Total_Final = sum(Final_Return, na.rm = T)
    )

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
    ) %>%
    mutate(
      threshold = i,
      col_to_test = col_to_test
    ) %>%
    left_join(returns_total)

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
  ggplot(aes(x = threshold, y = Total_Final)) +
  geom_line() +
  geom_point() +
  facet_wrap(.~Asset, scales = "free") +
  theme_minimal()
