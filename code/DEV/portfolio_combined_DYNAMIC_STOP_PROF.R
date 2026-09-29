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
    "EUR_AUD", "SOYBN_USD","CN50_USD",
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
training_data_db <- "C:/Users/nikhi/Documents/trade_data/Day_Trader_Cor_Continuous_Models/training_data.db"
start_date = "2013-01-01"
end_date = today() %>% as.character()
Indices_Metals_Bonds <- list()

assets_to_port =
  c(
    "FR40_EUR",
    "BTC_USD",
    "CN50_USD"
  ) %>% unique()


end_period = 132
trade_direction = "Long"
slippage_percent = 0
risk_dollar_value = 5
volatility_factor_stop = 2
volatility_factor_profit = 3
profit_multiple = 1.1

regression_length = 25000
direct_return_cols = 24
lag_value_error = end_period + 1
dependant_var = "Final_Return"
save_location = "C:/Users/nikhi/Documents/trade_data/Day_Trader_Cor_Continuous_Models/"
file_name = "DYNAMIC_MIXED_NO_V3"
training_date = "2020-02-01"
testing_date = as_date(training_date) + months(3)

xtnd_ss_cols_PR_cols = c(1,5,10,20,30,40,50,60,70,80,120, 100)
xtnd_ss_cols_BR_periods = c(100,200,300, 50, 150, 250, 350, 25, 500)
xtnd_rolling_volatility = c(20,50,60,80,100)
xtnd_rolling_bull_bear = c(50,100)
lag_dependant = end_period + 1
auto_cor_cols = 40
cor_period = c(50)
cor_skip_periods = c(1,2,4,5,6,8,10,12,14,16)
periods_to_use_deviation = c(1,10,20,30,40,50)
mean_periods_deviation = c(50, 100)


Indices_Metals_Bonds[[1]] <-
  get_db_data_quickly_algo(
    db_location = db_location,
    start_date = start_date,
    end_date = as.character(today() + days(30)),
    # end_date = sim_end %>% as_date() %>% as.character(),
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
    # end_date = sim_end %>% as_date() %>% as.character(),
    time_frame = "H1",
    bid_or_ask = "bid",
    assets =   assets_to_port
  ) %>%
  distinct()

final_sim_date <-
 as_datetime(training_date, tz = "Australia/Canberra") - dhours(5000)
sim_date_vector <-
  seq(as_datetime(start_date, tz = "Australia/Canberra"), final_sim_date, "hours")

volatility_factor_stop_vec <-
  tibble(volatility_factor_stop = c(1,3,5,7,9, 11))

running_volatility_tibble <-
  c(1,3,5,7,9, 11) %>%
  map_dfr(
    ~
      volatility_factor_stop_vec %>%
      mutate(
        volatility_factor_profit = .x
      )
  )

running_volatility_tibble <-
  c(20,100,200) %>%
  map_dfr(
    ~
      running_volatility_tibble %>%
      mutate(running_volatility_period_max = .x)
  )

running_volatility_tibble <-
  c(100,200) %>%
  map_dfr(
    ~
      running_volatility_tibble %>%
      mutate(running_volatility_period_mean = .x)
  )

temp_reg_data_train_list <-list()

for (i in 1:dim(running_volatility_tibble)[1] ) {

  volatility_factor_stop = running_volatility_tibble$volatility_factor_stop[i]
  volatility_factor_profit = running_volatility_tibble$volatility_factor_profit[i]

  running_volatility_period_max = running_volatility_tibble$running_volatility_period_max[i]
  running_volatility_period_mean = running_volatility_tibble$running_volatility_period_mean[i]

  sim_start = sim_date_vector %>% sample(size = 1)
  sim_end = sim_start + dhours(5000)

  temp_ask <- Indices_Metals_Bonds[[1]] %>% ungroup() %>%  filter(Date <= sim_end, Date >= sim_start)
  temp_bid <- Indices_Metals_Bonds[[2]] %>% ungroup() %>%  filter(Date <= sim_end, Date >= sim_start)

  portfolio_data_train <-
    get_dynamic_stop_prof_returns(
      Ask_Data = temp_ask,
      Bid_Data = temp_bid,
      periods_wanted = end_period,
      trade_direction = "Long",
      currency_conversion =currency_conversion,
      asset_infor = asset_infor,
      slippage_percent = slippage_percent,
      risk_dollar_value = risk_dollar_value,
      volatility_factor_stop = volatility_factor_stop,
      volatility_factor_profit = volatility_factor_profit,
      profit_multiple = profit_multiple,
      running_volatility_period_max = running_volatility_period_max,
      running_volatility_period_mean = running_volatility_period_mean
    )


  temp_reg_data_train_list[[i]] <-
    get_dynamic_portfolio_no_V3(
      portfolio_data = portfolio_data_train,
      xtnd_ss_cols_PR_cols = xtnd_ss_cols_PR_cols,
      xtnd_ss_cols_BR_periods = xtnd_ss_cols_BR_periods,
      xtnd_rolling_volatility = xtnd_rolling_volatility,
      xtnd_rolling_bull_bear = xtnd_rolling_bull_bear,
      lag_dependant = lag_dependant,
      auto_cor_cols = auto_cor_cols,
      cor_skip_periods = cor_skip_periods,
      cor_period = cor_period,
      periods_to_use_deviation = periods_to_use_deviation,
      mean_periods_deviation = mean_periods_deviation
    ) %>%
    ungroup() %>%
    filter(if_all(everything(), ~ !is.na(.) & !is.infinite(.) & !is.nan(.) )) %>%
    slice_sample(n = 2500) %>%
    mutate(
      running_volatility_period_max = running_volatility_tibble$running_volatility_period_max[i],
      running_volatility_period_mean = running_volatility_tibble$running_volatility_period_mean[i]
    )%>%
    mutate(
      volatility_factor_stop = running_volatility_tibble$volatility_factor_stop[i],
      volatility_factor_profit = running_volatility_tibble$volatility_factor_profit[i]
    )

}

temp_reg_data_train <-
  temp_reg_data_train_list %>%
  map_dfr(bind_rows)


training_data_db_con <- connect_db(training_data_db)

# append_table_sql_lite(.data = temp_reg_data_train,
#                      table_name = "training_data",
#                      conn = training_data_db_con)

DBI::dbDisconnect(training_data_db_con)
rm(training_data_db_con)
gc()

temp_reg_data_train <-
  DBI::dbGetQuery(conn = training_data_db_con, statement = "SELECT * FROM training_data") %>%
  mutate(
    Date = as_datetime(Date, tz = "Australia/Canberra")
  ) %>%
  group_by(running_volatility_period_max, running_volatility_period_mean, Asset) %>%
  slice_sample(n = 500) %>%
  ungroup()

DBI::dbDisconnect(training_data_db_con)
rm(training_data_db_con)
gc()

all_cor_vars <-
  names(temp_reg_data_train) %>%
  keep(~
         str_detect(.x, "auto_cor|brownian|state_space|single_vs_total_return|roll_vol|cumulative_return_1_diff_roll|Bull|Bear")|
         (.x == "volatility_factor_stop")|
         (.x == "volatility_factor_profit")|
         (.x == "running_volatility_period_max")|
         (.x == "running_volatility_period_mean") ) %>%
  unlist() %>%
  as.character() %>%
  unique()

rm(portfolio_data_train, Indices_Metals_Bonds)
gc()
gc()

portfolio_gen_model_no_V3_New(
  reg_dat = temp_reg_data_train %>% ungroup() ,
  reg_vars = all_cor_vars,
  training_end_date = training_date,
  Bayes_or_LM = "LM",
  save_path = save_location,
  dependant_var = "Final_Return",
  sig_thresh_LM = 1,
  file_name = file_name,
  reg_samples = 100000
)

rm(temp_reg_data_train)
gc()

