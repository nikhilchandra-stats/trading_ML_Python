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
Indices_Metals_Bonds <- list()

assets_to_port =
  c("SPX500_USD", "XAU_USD", "EU50_EUR", "JP225_USD", "USD_JPY", "UK100_GBP") %>% unique()

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

Indices_Metals_Bonds[[1]] <- Indices_Metals_Bonds[[1]] %>% filter(Date >= "2019-01-01")
Indices_Metals_Bonds[[2]] <- Indices_Metals_Bonds[[2]] %>% filter(Date >= "2019-01-01")

stop_factor_var =4
profit_factor_var =8
risk_dollar_value_var = 5
end_period = 24
trade_direction = "Long"
end_point_loss = -2.5
end_point_profit = 5

portfolio_data <-
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
  )

correlation_data <-
  get_portfolio_rolling_data(
    asset_data = Indices_Metals_Bonds[[1]],
    asset_of_interest = assets_to_port,
    low_to_price_lengths = c(100,200),
    cor_periods = c(200,100)
  )

all_dates_sim <-
  correlation_data %>%
  filter(Date >= as_datetime("2020-01-01") + dhours(16000) ) %>%
  pull(Date) %>%
  unique()

sim_list <- list()
db_sim_results_con <- connect_db("D:/trade_data/db_sim_results_FULL_Port_Equity_ERROR_CRRCT.db")
redo_db <- TRUE

for (i in 2316:(length(all_dates_sim) - 1) ) {

  if(i %% 50 == 0) {gc()}

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
    )

  results_temp <-
    generate_portfolio_LM_with_Errors(
      cor_high_diff_data =
        correlation_data %>%
        filter( Date <= all_dates_sim[i + 1] ),
      regression_length = 12000,
      portfolio_actuals_data = portfolio_data,
      dependant_var = "Final_Return",
      date_filter_train = all_dates_sim[i],
      sig_thresh_LM = 0.1,
      padding_value = 0,
      lag_value_error = end_period + 1
    ) %>%
    dplyr::select(Date, Asset,
                  Final_Return,
                  predicted_10000 = predicted,
                  trained_mean_10000 = trained_mean,
                  trained_sd_10000 = trained_sd) %>%
    filter(Date > all_dates_sim[i] ,
           Date <= all_dates_sim[i + 1] )

  results_temp2 <-
    generate_portfolio_LM_with_Errors(
      cor_high_diff_data = correlation_data %>%
        filter( Date <= all_dates_sim[i + 1] ),
      regression_length = 7500,
      portfolio_actuals_data = portfolio_data,
      dependant_var = "Final_Return",
      date_filter_train = all_dates_sim[i],
      sig_thresh_LM = 0.1,
      padding_value = 0,
      lag_value_error = end_period + 1
    ) %>%
    dplyr::select(Date, Asset,
                  predicted_5000 = predicted,
                  trained_mean_5000 = trained_mean,
                  trained_sd_5000 = trained_sd) %>%
    filter(Date > all_dates_sim[i] ,
           Date <= all_dates_sim[i + 1] )

  results_temp3 <-
    generate_portfolio_LM_with_Errors(
      cor_high_diff_data = correlation_data %>%
        filter( Date <= all_dates_sim[i + 1] ),
      regression_length = 3000,
      portfolio_actuals_data = portfolio_data,
      dependant_var = "Final_Return",
      date_filter_train = all_dates_sim[i],
      sig_thresh_LM = 0.1,
      padding_value = 0,
      lag_value_error = end_period + 1
    ) %>%
    dplyr::select(Date, Asset,
                  predicted_2500 = predicted,
                  trained_mean_2500 = trained_mean,
                  trained_sd_2500 = trained_sd) %>%
    filter(Date > all_dates_sim[i] ,
           Date <= all_dates_sim[i + 1] )

  results_temp <-
    results_temp %>%
    left_join(results_temp2) %>%
    left_join(results_temp3)

  sim_list[[i]] <-
    results_temp

  tictoc::toc()

  if(i == 1 & redo_db == TRUE) {
    write_table_sql_lite(.data = results_temp,
                         table_name = "db_sim_results",
                         conn = db_sim_results_con,
                         overwrite_true = TRUE)
  } else {
    append_table_sql_lite(.data = results_temp,
                          table_name = "db_sim_results",
                          conn = db_sim_results_con)
  }

  rm(portfolio_data, results_temp, results_temp3, results_temp2)

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
  )

model_prediction_data <-
  DBI::dbGetQuery(conn = db_sim_results_con,
                  statement = "SELECT * FROM db_sim_results") %>%
  mutate(Date = as_datetime(Date, tz = "Australia/Canberra")) %>%
  mutate(
    Averaged_Pred =
      (predicted_10000 + predicted_5000 + predicted_2500)/3
  ) %>%
  dplyr::select(-Final_Return) %>%
  left_join(actual_final_returns %>%  dplyr::select(Date, Asset, Final_Return)) %>%
  ungroup() %>%
  group_by(Date) %>%
  mutate(
    portfolio_pred_10000 = sum(predicted_10000, na.rm = T),
    portfolio_pred_5000 = sum(predicted_5000, na.rm = T),
    portfolio_pred_2500 = sum(predicted_2500, na.rm = T)
  ) %>%
  ungroup() %>%
  group_by(Asset) %>%
  arrange(Date, .by_group = TRUE) %>%
  group_by(Asset) %>%
  mutate(
    portfolio_pred_10000_mean_roll_250 =
      slider::slide_dbl(.x  = portfolio_pred_10000, .f = ~ mean(.x, na.rm = T), .before = 250),
    portfolio_pred_5000_mean_roll_250 =
      slider::slide_dbl(.x  = portfolio_pred_5000, .f = ~ mean(.x, na.rm = T), .before = 250),
    portfolio_pred_2500_mean_roll_250 =
      slider::slide_dbl(.x  = portfolio_pred_2500, .f = ~ mean(.x, na.rm = T), .before = 250),

    portfolio_pred_10000_sd_roll_250 =
      slider::slide_dbl(.x  = portfolio_pred_10000, .f = ~ sd(.x, na.rm = T), .before = 250),
    portfolio_pred_5000_sd_roll_250 =
      slider::slide_dbl(.x  = portfolio_pred_5000, .f = ~ sd(.x, na.rm = T), .before = 250),
    portfolio_pred_2500_sd_roll_250 =
      slider::slide_dbl(.x  = portfolio_pred_2500, .f = ~ sd(.x, na.rm = T), .before = 250),

    rolling_predicted_10000_50 =
      slider::slide_dbl(predicted_10000, .f = ~ mean(.x, na.rm = T), .before = 50),
    rolling_predicted_10000_100 =
      slider::slide_dbl(predicted_10000, .f = ~ mean(.x, na.rm = T), .before = 100),
    rolling_predicted_10000_200 =
      slider::slide_dbl(predicted_10000, .f = ~ mean(.x, na.rm = T), .before = 200),
    rolling_predicted_10000_400 =
      slider::slide_dbl(predicted_10000, .f = ~ mean(.x, na.rm = T), .before = 400),

    rolling_predicted_5000_50 =
      slider::slide_dbl(predicted_5000, .f = ~ mean(.x, na.rm = T), .before = 50),
    rolling_predicted_5000_100 =
      slider::slide_dbl(predicted_5000, .f = ~ mean(.x, na.rm = T), .before = 100),
    rolling_predicted_5000_200 =
      slider::slide_dbl(predicted_5000, .f = ~ mean(.x, na.rm = T), .before = 200),
    rolling_predicted_5000_400 =
      slider::slide_dbl(predicted_5000, .f = ~ mean(.x, na.rm = T), .before = 400)

  ) %>%
  ungroup()

# model_prediction_data <-
#   sim_list %>%
#   map_dfr(bind_rows) %>%
#   mutate(Date = as_datetime(Date, tz = "Australia/Canberra")) %>%
#   mutate(
#     Averaged_Pred =
#       (predicted_10000 + predicted_5000 + predicted_2500)/3
#   )

model_prediction_data %>%
  pull(Date) %>% max()

which(all_dates_sim == max(model_prediction_data$Date, na.rm = T))

trade_statment <-
  "
Averaged_Pred > 1
"

trade_statment <-
  "
(predicted_2500 > 3)|(portfolio_pred_5000 > 4)|(portfolio_pred_10000 > 4)|(portfolio_pred_5000_mean_roll_250 > 4)
"

# trade_statment <-
#   "
# portfolio_pred_2500 > portfolio_pred_2500_mean_roll_250
# "

analyse_performance <-
  model_prediction_data %>%
  mutate(
    trade_col = eval(parse(text = trade_statment))
  ) %>%
  mutate(
    trade_col = case_when(trade_col == TRUE ~ "Long", TRUE ~ "No Trade")
  )

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
  ggplot(aes(x = Date, y = Final_Return_Cumulative, color = trade_col)) +
  geom_line(size = 1) +
  facet_wrap(.~trade_col, scales = "free") +
  theme_minimal() +
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
