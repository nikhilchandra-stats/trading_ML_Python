helpeR::load_custom_functions()

all_aud_symbols <- get_oanda_symbols() %>%
  keep(~ str_detect(.x, "AUD")|str_detect(.x, "USD_SEK|USD_NOK|USD_HUF|USD_ZAR|USD_CNY|USD_MXN|USD_CZK"))
asset_infor <- get_instrument_info()
aud_assets <- read_all_asset_data_intra_day(
  asset_list_oanda = all_aud_symbols,
  save_path_oanda_assets = "C:/Users/Nikhil Chandra/Documents/Asset Data/oanda_data/",
  read_csv_or_API = "API",
  time_frame = "D",
  bid_or_ask = "bid",
  how_far_back = 10,
  start_date = (today() - days(2)) %>% as.character()
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
    "UK100_GBP",
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
    "USB02Y_USD", "USB05Y_USD", "USB30Y_USD", "USB10Y_USD", "UK100_GBP",
    "EUR_CHF", #1 EUR_CHF
    "EUR_SEK" , #2 EUR_SEK
    "GBP_CHF", #3 GBP_CHF
    "GBP_JPY", #4 GBP_JPY
    "USD_CZK", #5 USD_CZK
    "USD_NOK" , #6 USD_NOK
    "XAG_CAD", #7 XAG_CAD
    "XAG_CHF", #8 XAG_CHF
    "XAG_JPY" , #9 XAG_JPY
    "GBP_NZD" , #10 GBP_NZD
    "NZD_CHF" , #11 NZD_CHF
    "USD_MXN" , #12 USD_MXN
    "XPD_USD" , #13 XPD_USD
    "XPT_USD" , #14 XPT_USD
    "NATGAS_USD" , #15 NATGAS_USD
    "SG30_SGD" , #16 SG30_SGD
    "SOYBN_USD" , #17 SOYBN_USD
    "WHEAT_USD" , #18 WHEAT_USD
    "SUGAR_USD" , #19 SUGAR_USD
    "DE30_EUR" , #20 DE30_EUR
    "UK10YB_GBP" , #21 UK10YB_GBP
    "JP225_USD" , #22 JP225_USD
    "CH20_CHF" , #23 CH20_CHF
    "NL25_EUR" , #24 NL25_EUR
    "XAG_SGD" , #25 XAG_SGD,
    "BCH_USD" , #26 BCH_USD
    "LTC_USD" #27 LTC_USD
  ) %>%
  unique()

asset_infor <- get_instrument_info()
#---------------------Data
load_custom_functions()
db_location = "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data_Most_Assets_2025-09-13 2.db"
start_date = "2019-06-01"
end_date = today() %>% as.character()

# bin_factor = NULL
# stop_value_var = 2
# profit_value_var = 15
# period_var = 48

All_Daily_Data <-
  get_DAILY_ALGO_DATA_API_REQUEST()

Indices_Metals_Bonds <- list()

Indices_Metals_Bonds[[1]] <-
  get_db_data_quickly_algo(
    db_location = db_location,
    start_date = start_date,
    end_date = today() %>% as.character(),
    time_frame = "H1",
    bid_or_ask = "ask",
    assets =   c("EUR_CHF" , "EUR_SEK" , "GBP_CHF", "GBP_JPY",
                 "USD_CZK", "USD_NOK" , "XAG_CAD", "XAG_CHF",
                 "XAG_JPY" , "GBP_NZD" , "NZD_CHF" , "USD_MXN",
                 "XPD_USD","XPT_USD","NATGAS_USD","SG30_SGD" ,
                 "SOYBN_USD", "WHEAT_USD", "SUGAR_USD" ,"DE30_EUR" ,
                 "UK10YB_GBP","JP225_USD","CH20_CHF","NL25_EUR" ,
                 "XAG_SGD", "BCH_USD", "LTC_USD" , "EUR_USD" ,
                 "EU50_EUR","SPX500_USD" , "US2000_USD" , "USB10Y_USD" ,
                 "USD_JPY" , "AUD_USD" , "XAG_USD" , "XAG_EUR" ,
                 "BTC_USD" , "XAU_USD" , "XAU_EUR" , "GBP_USD" , "USD_CAD" ,
                 "USD_SEK" , "EUR_AUD" , "GBP_AUD" , "XAG_GBP" ,"XAU_GBP" ,
                 "EUR_JPY" , "XAU_SGD" , "XAU_CAD" , "NZD_USD" , "XAU_NZD" ,
                 "XAG_NZD" , "FR40_EUR" , "UK100_GBP" , "AU200_AUD" ,
                 "HK33_HKD" , "SG30_SGD" , "US2000_USD" , "XAG_AUD" ,
                 "XAU_AUD" , "XAU_JPY" , "USB02Y_USD" , "USD_SGD" , "XAU_CHF")
  ) %>%
  distinct()

Indices_Metals_Bonds[[2]] <-
  get_db_data_quickly_algo(
    db_location = db_location,
    start_date = start_date,
    end_date = today() %>% as.character(),
    time_frame = "H1",
    bid_or_ask = "bid",
    assets =   c("EUR_CHF" , "EUR_SEK" , "GBP_CHF", "GBP_JPY",
                 "USD_CZK", "USD_NOK" , "XAG_CAD", "XAG_CHF",
                 "XAG_JPY" , "GBP_NZD" , "NZD_CHF" , "USD_MXN",
                 "XPD_USD","XPT_USD","NATGAS_USD","SG30_SGD" ,
                 "SOYBN_USD", "WHEAT_USD", "SUGAR_USD" ,"DE30_EUR" ,
                 "UK10YB_GBP","JP225_USD","CH20_CHF","NL25_EUR" ,
                 "XAG_SGD", "BCH_USD", "LTC_USD" , "EUR_USD" ,
                 "EU50_EUR","SPX500_USD" , "US2000_USD" , "USB10Y_USD" ,
                 "USD_JPY" , "AUD_USD" , "XAG_USD" , "XAG_EUR" ,
                 "BTC_USD" , "XAU_USD" , "XAU_EUR" , "GBP_USD" , "USD_CAD" ,
                 "USD_SEK" , "EUR_AUD" , "GBP_AUD" , "XAG_GBP" ,"XAU_GBP" ,
                 "EUR_JPY" , "XAU_SGD" , "XAU_CAD" , "NZD_USD" , "XAU_NZD" ,
                 "XAG_NZD" , "FR40_EUR" , "UK100_GBP" , "AU200_AUD" ,
                 "HK33_HKD" , "SG30_SGD" , "US2000_USD" , "XAG_AUD" ,
                 "XAU_AUD" , "XAU_JPY" , "USB02Y_USD" , "USD_SGD" , "XAU_CHF")
  ) %>%
  distinct()

actual_wins_losses <-
  get_actual_wins_losses(
    assets_to_analyse =
      c("EUR_CHF", #1 EUR_CHF
        "EUR_SEK" , #2 EUR_SEK
        "GBP_CHF", #3 GBP_CHF
        "GBP_JPY", #4 GBP_JPY
        "USD_CZK", #5 USD_CZK
        "USD_NOK" , #6 USD_NOK
        "XAG_CAD", #7 XAG_CAD
        "XAG_CHF", #8 XAG_CHF
        "XAG_JPY" , #9 XAG_JPY
        "GBP_NZD" , #10 GBP_NZD
        "NZD_CHF" , #11 NZD_CHF
        "USD_MXN" , #12 USD_MXN
        "XPD_USD" , #13 XPD_USD
        "XPT_USD" , #14 XPT_USD
        "NATGAS_USD" , #15 NATGAS_USD
        "SG30_SGD" , #16 SG30_SGD
        "SOYBN_USD" , #17 SOYBN_USD
        "WHEAT_USD" , #18 WHEAT_USD
        "SUGAR_USD" , #19 SUGAR_USD
        "DE30_EUR" , #20 DE30_EUR
        "UK10YB_GBP" , #21 UK10YB_GBP
        "JP225_USD" , #22 JP225_USD
        "CH20_CHF" , #23 CH20_CHF
        "NL25_EUR" , #24 NL25_EUR
        "XAG_SGD", #25 XAG_SGD
        "BCH_USD" , #26 BCH_USD
        "LTC_USD" #27 LTC_USD
      ),
    asset_data = Indices_Metals_Bonds,
    stop_factor = 5,
    profit_factor = 30,
    risk_dollar_value = 10,
    trade_direction = "Long",
    currency_conversion = currency_conversion,
    asset_infor = asset_infor,
    periods_ahead = 24
  )


pred_generation_db_path <-
  "C:/Users/Nikhil Chandra/Documents/trade_data/tech_preds.db"
pred_gen_db_con <- connect_db(pred_generation_db_path)


# Indices_Metals_Bonds,
# actual_wins_losses,
# asset_of_interest = asset_loop,
# actuals_periods_needed = c("period_return_24_Price", "period_return_35_Price", "period_return_46_Price"),
# training_end_date = training_end_date,
# bin_threshold = bin_threshold,
# rolling_mean_pred_period = rolling_mean_pred_period,
# correlation_rolling_periods = c(100,200, 300),
# copula_assets = copula_assets
# training_end_date <- "2025-05-01"
# rolling_mean_pred_period = 500
# bin_threshold = 5

all_preds <-
  Single_Asset_V3_get_all_preds(
    Indices_Metals_Bonds = Indices_Metals_Bonds,
    actuals_periods_needed = c("period_return_35_Price", "period_return_46_Price"),
    correlation_rolling_periods = c(100,200, 300),
    training_end_date = "2025-05-01",
    rolling_mean_pred_period = 500,
    bin_threshold = 5,
    start_index = 1,
    end_index = 21,
    base_path = "C:/Users/Nikhil Chandra/Documents/trade_data/single_asset_models_v1/"
  )

all_preds_dfr <-
  all_preds %>%
  mutate(
    training_end_date = "2025-05-01",
    rolling_mean_pred_period = 500,
    bin_threshold = 5
  )


write_table_sql_lite(.data = all_preds_dfr,
                     table_name = "tech_preds",
                     conn = pred_gen_db_con,
                     overwrite_true = TRUE)

generated_preds_from_db <-
  DBI::dbGetQuery(conn = pred_gen_db_con,
                  statement = "SELECT * FROM tech_preds") %>%
  mutate(
    Date = as_datetime(Date)
  )

trade_statement <-
  "

        (AR_GLM_Pred_period_return_35_Price_mean > 0.5 & AR_LM_Pred_period_return_35_Price > 1)|
        (AR_GLM_Pred_period_return_35_Price > 0.58 & AR_GLM_Pred_period_return_46_Price > 0.58 &
        AR_LM_Pred_period_return_35_Price > 0 & AR_LM_Pred_period_return_46_Price > 0)|
        (
        Copula_GLM_Pred_period_return_35_Price> 0.92 &
        Copula_GLM_Pred_period_return_35_Price> 0.92 &
        Copula_GLM_Pred_period_return_46_Price> 0.92
        )|
        (averaged_35_LM_pred > 1  & averaged_35_GLM_pred > 0.6 & averaged_35_46_GLM_pred > 0.6)
        # (averaged_35_46_LM_pred > 5 & averaged_35_46_LM_pred < 8)
"

test_performance <-
  generated_preds_from_db %>%
  filter(!(Asset %in% c("XPD_USD", "XPT_USD"))) %>%
  mutate(
    averaged_35_LM_pred =
      (state_space_LM_Pred_period_return_35_Price +
         AR_LM_Pred_period_return_35_Price +
         Copula_LM_Pred_period_return_35_Price)/3,

    averaged_35_GLM_pred =
      (state_space_GLM_Pred_period_return_35_Price +
         AR_GLM_Pred_period_return_35_Price +
         Copula_GLM_Pred_period_return_35_Price)/3,

    averaged_35_46_GLM_pred =
      (state_space_GLM_Pred_period_return_35_Price +
         AR_GLM_Pred_period_return_35_Price +
         Copula_GLM_Pred_period_return_35_Price +
         state_space_GLM_Pred_period_return_46_Price +
         AR_GLM_Pred_period_return_46_Price +
         Copula_GLM_Pred_period_return_46_Price)/6,

    averaged_35_46_LM_pred =
      (state_space_LM_Pred_period_return_35_Price +
         AR_LM_Pred_period_return_35_Price +
         Copula_LM_Pred_period_return_35_Price +
         state_space_LM_Pred_period_return_46_Price +
         AR_LM_Pred_period_return_46_Price +
         Copula_LM_Pred_period_return_46_Price)/6

  ) %>%
  filter(Date >= as_datetime(training_end_date)) %>%
  left_join(
    actual_wins_losses %>%
      dplyr::select(Date, Asset,
                    period_return_8_Price, period_return_12_Price, period_return_16_Price,
                    period_return_24_Price, period_return_35_Price, period_return_46_Price)
  ) %>%
  mutate(
    trade_col =
      eval(parse(text = trade_statement))
  ) %>%
  mutate(
    trade_col =
      case_when(
        trade_col == TRUE ~ "Long"
      )
  )

control_data <-
  test_performance %>%
  group_by(Date) %>%
  summarise(
    period_return_35_Price = sum(period_return_35_Price, na.rm = T)
  ) %>%
  ungroup() %>%
  arrange(Date) %>%
  mutate(
    cumulative_return = cumsum(period_return_35_Price)
  ) %>%
  mutate(
    trade_col = "Control"
  )

trade_data <-
  test_performance %>%
  filter(trade_col == "Long") %>%
  group_by(Date) %>%
  summarise(
    # period_return_35_Price = sum(period_return_35_Price, na.rm = T)
    period_return_35_Price = sum(period_return_35_Price, na.rm = T)
  ) %>%
  ungroup() %>%
  arrange(Date) %>%
  mutate(
    # cumulative_return = cumsum(period_return_35_Price)
    cumulative_return = cumsum(period_return_35_Price)
  ) %>%
  mutate(
    trade_col = "Long"
  )


plot_dat_Copula <-
  control_data %>%
  bind_rows(trade_data)


plot_dat_Copula %>%
  ggplot(aes(x = Date, y = cumulative_return, color = trade_col)) +
  geom_line() +
  theme_minimal() +
  facet_wrap(.~ trade_col, scales = "free") +
  theme(legend.position = "bottom")


control_data_asset <-
  test_performance %>%
  group_by(Date, Asset) %>%
  summarise(
    period_return_35_Price = sum(period_return_35_Price, na.rm = T)
  ) %>%
  ungroup() %>%
  group_by(Asset) %>%
  arrange(Date, .by_group = TRUE) %>%
  mutate(
    cumulative_return = cumsum(period_return_35_Price),
    wins = ifelse(period_return_35_Price > 0 , 1, 0),
    avg_win = ifelse(period_return_35_Price > 0, period_return_35_Price, NA),
    losing_return =
      ifelse(period_return_35_Price < 0, period_return_35_Price, NA)
  ) %>%
  mutate(
    trade_col = "Control"
  ) %>%
  group_by(Asset) %>%
  summarise(
    control_total_trades = n_distinct(Date),
    control_wins = sum(wins, na.rm = T),
    control_returns_total = sum(period_return_35_Price, na.rm = T),
    control_returns_25 = quantile(cumulative_return, 0.25),
    avg_win = mean(avg_win, na.rm = T),
    avg_loss = mean(losing_return, na.rm = T)
  ) %>%
  mutate(
    control_Perc = control_wins/control_total_trades
  ) %>%
  mutate(
    winning_greater_than_0_control = ifelse(control_returns_total > 0, 1, 0)
  ) %>%
  summarise(
    control_returns = sum(control_total_trades, na.rm = T),
    control_returns_25 = sum(control_returns_25, na.rm = T),
    control_total_trades = sum(control_total_trades, na.rm = T),
    control_perc = sum(control_wins, na.rm = T)/sum(control_total_trades, na.rm = T),
    control_avg_win = mean(avg_win, na.rm = T),
    control_avg_loss = mean(avg_loss, na.rm = T),
    winning_greater_than_0_control = sum(winning_greater_than_0_control, na.rm = T)/n_distinct(Asset)
  ) %>%
  mutate(
    binomial_expected_control = control_avg_win*control_perc + (control_avg_loss*(1 - control_perc))
  )

trade_data <-
  test_performance %>%
  filter(trade_col == "Long") %>%
  group_by(Date, Asset) %>%
  summarise(
    period_return_35_Price = sum(period_return_35_Price, na.rm = T)
  ) %>%
  ungroup() %>%
  group_by(Asset) %>%
  arrange(Date, .by_group = TRUE) %>%
  mutate(
    cumulative_return = cumsum(period_return_35_Price),
    wins = ifelse(period_return_35_Price > 0 , 1, 0),
    winning_return =
      ifelse(period_return_35_Price > 0, period_return_35_Price, NA),
    losing_return =
      ifelse(period_return_35_Price < 0, period_return_35_Price, NA)
  ) %>%
  mutate(
    trade_col = "Long"
  ) %>%
  group_by(Asset, trade_col) %>%
  summarise(
    total_trades = n_distinct(Date),
    wins = sum(wins, na.rm = T),
    returns_total = sum(period_return_35_Price, na.rm = T),
    returns_25 = quantile(cumulative_return, 0.25),
    returns_10 = quantile(cumulative_return, 0.1),
    avg_win = mean(winning_return, na.rm = T),
    avg_loss = mean(losing_return, na.rm = T)
  ) %>%
  mutate(
    Perc = wins/total_trades
  ) %>%
  ungroup() %>%
  mutate(
    Perc = wins/total_trades
  ) %>%
  mutate(
    winning_greater_than_0 = ifelse(returns_total > 0, 1, 0)
  ) %>%
  summarise(
    returns = sum(returns_total, na.rm = T),
    returns_25 = sum(returns_25, na.rm = T),
    total_trades = sum(total_trades, na.rm = T),
    perc = sum(wins, na.rm = T)/sum(total_trades, na.rm = T),
    avg_win = mean(avg_win, na.rm = T),
    avg_loss = mean(avg_loss, na.rm = T),
    winning_greater_than_0 = sum(winning_greater_than_0, na.rm = T)/n_distinct(Asset)
  ) %>%
  mutate(
    binomial_expected = avg_win*perc + (avg_loss*(1 - perc))
  )

final_results <-
  trade_data %>%
  bind_cols(control_data_asset) %>%
  mutate(ratio_adj = total_trades/control_total_trades) %>%
  mutate(control_returns = control_returns*ratio_adj) %>%
  mutate(
    binomial_diff = binomial_expected - binomial_expected_control
  )

plot_dat_Copula %>%
  filter(trade_col == "Long") %>%
  mutate(
    Movement_300 = cumulative_return - lag(cumulative_return, 100)
  ) %>%
  summarise(
    returns_99 = quantile(Movement_300, 0.99, na.rm = T),
    returns_90 = quantile(Movement_300, 0.90, na.rm = T),
    returns_75 = quantile(Movement_300, 0.75, na.rm = T),
    returns_50 = quantile(Movement_300, 0.5, na.rm = T),
    returns_25 = quantile(Movement_300, 0.25, na.rm = T),
    returns_10 = quantile(Movement_300, 0.1, na.rm = T),
    returns_01 = quantile(Movement_300, 0.01, na.rm = T)
  )



asset_returns_control <-
  test_performance %>%
  group_by(Date, Asset) %>%
  summarise(
    period_return_35_Price = sum(period_return_35_Price, na.rm = T)
  ) %>%
  ungroup() %>%
  group_by(Asset) %>%
  arrange(Date, .by_group = TRUE) %>%
  mutate(
    cumulative_return = cumsum(period_return_35_Price),
    wins = ifelse(period_return_35_Price > 0 , 1, 0),
    winning_return =
      ifelse(period_return_35_Price > 0, period_return_35_Price, NA),
    losing_return =
      ifelse(period_return_35_Price < 0, period_return_35_Price, NA)
  ) %>%
  mutate(
    trade_col = "Control"
  ) %>%
  group_by(Asset, trade_col) %>%
  summarise(
    total_trades = n_distinct(Date),
    wins = sum(wins, na.rm = T),
    returns_total = sum(period_return_35_Price, na.rm = T),
    returns_25 = quantile(cumulative_return, 0.25),
    returns_10 = quantile(cumulative_return, 0.1),
    avg_win = mean(winning_return, na.rm = T),
    avg_loss = mean(losing_return, na.rm = T)
  ) %>%
  mutate(
    Perc = wins/total_trades
  ) %>%
  ungroup() %>%
  mutate(
    Perc = wins/total_trades
  ) %>%
  mutate(
    winning_greater_than_0 = ifelse(returns_total > 0, 1, 0),
    binomial_expected = avg_win*Perc + (avg_loss*(1 - Perc))
  )

asset_returns <-
  test_performance %>%
  filter(trade_col == "Long") %>%
  group_by(Date, Asset) %>%
  summarise(
    period_return_35_Price = sum(period_return_35_Price, na.rm = T)
  ) %>%
  ungroup() %>%
  group_by(Asset) %>%
  arrange(Date, .by_group = TRUE) %>%
  mutate(
    cumulative_return = cumsum(period_return_35_Price),
    wins = ifelse(period_return_35_Price > 0 , 1, 0),
    winning_return =
      ifelse(period_return_35_Price > 0, period_return_35_Price, NA),
    losing_return =
      ifelse(period_return_35_Price < 0, period_return_35_Price, NA)
  ) %>%
  mutate(
    trade_col = "Long"
  ) %>%
  group_by(Asset, trade_col) %>%
  summarise(
    total_trades = n_distinct(Date),
    wins = sum(wins, na.rm = T),
    returns_total = sum(period_return_35_Price, na.rm = T),
    returns_25 = quantile(cumulative_return, 0.25),
    returns_10 = quantile(cumulative_return, 0.1),
    avg_win = mean(winning_return, na.rm = T),
    avg_loss = mean(losing_return, na.rm = T)
  ) %>%
  mutate(
    Perc = wins/total_trades
  ) %>%
  ungroup() %>%
  mutate(
    Perc = wins/total_trades
  ) %>%
  mutate(
    winning_greater_than_0 = ifelse(returns_total > 0, 1, 0),
    binomial_expected = avg_win*Perc + (avg_loss*(1 - Perc))
  ) %>%
  left_join(asset_returns_control %>%
              dplyr::select(Asset,  binomial_expected_control = binomial_expected))

