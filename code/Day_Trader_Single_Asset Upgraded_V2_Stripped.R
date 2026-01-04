helpeR::load_custom_functions()

all_aud_symbols <- get_oanda_symbols() %>%
  keep(~ str_detect(.x, "AUD")|str_detect(.x, "USD_SEK|USD_NOK|USD_HUF|USD_ZAR|USD_CNY|USD_MXN"))
asset_infor <- get_instrument_info()
aud_assets <- read_all_asset_data_intra_day(
  asset_list_oanda = all_aud_symbols,
  save_path_oanda_assets = "C:/Users/nikhi/Documents/Asset Data/oanda_data/",
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
start_date = "2015-01-01"
end_date = today() %>% as.character()

bin_factor = NULL
stop_value_var = 5
profit_value_var = 30
period_var = 24

All_Daily_Data <-
  get_DAILY_ALGO_DATA_API_REQUEST(
    c("EUR_USD", #1
      "EU50_EUR", #2
      "SPX500_USD", #3
      "US2000_USD", #4
      "USB10Y_USD", #5
      "USD_JPY", #6
      "AUD_USD", #7
      "EUR_GBP", #8
      "AU200_AUD" ,#9
      "EUR_AUD", #10
      "WTICO_USD", #11
      "UK100_GBP", #12
      "USD_CAD", #13
      "GBP_USD", #14
      "GBP_CAD", #15
      "EUR_JPY", #16
      "EUR_NZD", #17
      "XAG_USD", #18
      "XAG_EUR", #19
      "XAG_AUD", #20
      "XAG_NZD", #21
      "HK33_HKD", #22
      "FR40_EUR", #23
      "BTC_USD", #24
      "XAG_GBP", #25
      "GBP_AUD", #26
      "USD_SEK", #27
      "USD_SGD", #28
      "NZD_USD", #29
      "GBP_NZD", #30
      "XCU_USD", #31
      "NATGAS_USD", #32
      "GBP_JPY", #33
      "SG30_SGD", #34
      "XAU_USD", #35
      "EUR_SEK", #36
      "XAU_AUD", #37
      "UK10YB_GBP", #38
      "JP225Y_JPY", #39
      "ETH_USD" #40
    ) %>% unique()
  )

Indices_Metals_Bonds <- get_Port_Buy_Data(
  db_location = db_location,
  start_date = start_date,
  end_date = today() %>% as.character(),
  time_frame = "H1"
)

gc()

#-------------Indicator Inputs
single_asset_algo_generate_models(
  All_Daily_Data = All_Daily_Data,
  Indices_Metals_Bonds = Indices_Metals_Bonds,
  raw_macro_data = raw_macro_data,
  currency_conversion = currency_conversion,
  asset_infor = asset_infor,
  start_index = 1,
  end_index = 40,
  risk_dollar_value = 15,
  trade_direction = "Long",
  stop_value_var = 5,
  profit_value_var = 30,
  period_var = 24,
  bin_var_col = c("period_return_20_Price", "period_return_24_Price", "period_return_28_Price"),
  date_train_end_pre = "2023-06-01",
  date_train_phase_2_end_pre = "2024-06-01",
  training_date_start_post = "2024-07-04",
  training_date_end_post = "2025-09-01",
  test_end_date = as.character(today()),
  post_bins_cols =
    c("period_return_24_Price",
      "period_return_30_Price",
      "period_return_44_Price"),
  post_dependant_threshold = 5,
  model_data_store_path = "C:/Users/nikhi/Documents/trade_data/Day_Trader_Single_Asset_V2_trade_store_stop_2.db",
  save_path = "C:/Users/nikhi/Documents/trade_data/Day_Trader_Single_Asset_V2_trade_store_stop_2"
)

actual_wins_losses <-
  get_actual_wins_losses(
  assets_to_analyse =
    c("EUR_USD", #1
      "EU50_EUR", #2
      "SPX500_USD", #3
      "US2000_USD", #4
      "USB10Y_USD", #5
      "USD_JPY", #6
      "AUD_USD", #7
      "EUR_GBP", #8
      "AU200_AUD" ,#9
      "EUR_AUD", #10
      "WTICO_USD", #11
      "UK100_GBP", #12
      "USD_CAD", #13
      "GBP_USD", #14
      "GBP_CAD", #15
      "EUR_JPY", #16
      "EUR_NZD", #17
      "XAG_USD", #18
      "XAG_EUR", #19
      "XAG_AUD", #20
      "XAG_NZD", #21
      "HK33_HKD", #22
      "FR40_EUR", #23
      "BTC_USD", #24
      "XAG_GBP", #25
      "GBP_AUD", #26
      "USD_SEK", #27
      "USD_SGD", #28
      "NZD_USD", #29
      "GBP_NZD", #30
      "XCU_USD", #31
      "NATGAS_USD", #32
      "GBP_JPY", #33
      "SG30_SGD", #34
      "XAU_USD", #35
      "EUR_SEK", #36
      "XAU_AUD", #37
      "UK10YB_GBP", #38
      "JP225Y_JPY", #39
      "ETH_USD" #40
    ),
  asset_data = Indices_Metals_Bonds,
  stop_factor = 5,
  profit_factor = 30,
  risk_dollar_value = 15,
  trade_direction = "Long",
  currency_conversion = currency_conversion,
  asset_infor = asset_infor
)

post_bins_cols =
  c("period_return_24_Price",
    "period_return_30_Price",
    "period_return_44_Price")
model_data_store_path = "C:/Users/nikhi/Documents/trade_data/Day_Trader_Single_Asset_V2_trade_store_stop_2.db"
save_path = "C:/Users/nikhi/Documents/trade_data/Day_Trader_Single_Asset_V2_trade_store_stop_2"
post_dependant_threshold = 5

indicator_data <-
  get_indicator_pred_from_db(
    model_data_store_path = model_data_store_path,
    table_name = "single_asset_improved"
  )

post_bins_cols %>%
  map(
    ~
      prepare_post_ss_gen2_model(
        indicator_data = indicator_data,
        new_post_DB = TRUE,
        new_sym = TRUE,
        post_model_data_save_path = save_path,
        dependant_var = .x,
        dependant_threshold = post_dependant_threshold,
        training_date_start = training_date_start_post,
        training_date_end = training_date_end_post
      )
  )

post_preds_all <-
  read_post_models_and_get_preds(
    indicator_data = indicator_data,
    post_model_data_save_path =
      "C:/Users/nikhi/Documents/trade_data/Day_Trader_Single_Asset_V2_trade_store_stop_2/",
    # test_date_start = "2023-01-04",
    test_date_start = "2024-07-04",
    test_date_end = today(),
    dependant_var = "period_return_24_Price",
    dependant_threshold = 5
  )


post_preds_all_rolling <-
  get_rolling_post_preds(
    post_pred_data = post_preds_all,
    rolling_periods = c(3,50,100,200,400,500,2000),
    # test_date_start = "2024-02-01",
    # test_date_start = "2025-08-01",
    test_date_start = "2025-10-01",
    test_date_end = today(),
    pred_price_cols =
      c("period_return_24_Price",
        "period_return_30_Price",
        "period_return_44_Price")
  )

post_preds_all_rolling_and_originals <-
  post_preds_all_rolling %>%
  left_join(
    indicator_data %>%
      dplyr::select(Date, Asset, contains("pred_combined"),
                    contains("pred_macro"), contains("pred_index"),
                    contains("pred_daily"), contains("pred_copula"),
                    contains("pred_technical")) %>%
      distinct()
  )

# Use this for Periods 24 only 3.91% Edge with 200 return edge (USE THIS)
trade_statement =
  "((mean_3_pred_GLM_period_return_24_Price >
          mean_50_pred_GLM_period_return_24_Price + sd_50_pred_GLM_period_return_24_Price*2 |
  pred_GLM_period_return_24_Price >
            mean_50_pred_GLM_period_return_24_Price + sd_50_pred_GLM_period_return_24_Price*2)|
  (mean_3_pred_LM_period_return_24_Price >
          mean_500_pred_LM_period_return_24_Price + sd_500_pred_LM_period_return_24_Price*2 |
  pred_LM_period_return_24_Price >
            mean_500_pred_LM_period_return_24_Price + sd_500_pred_LM_period_return_24_Price*2)|
  (mean_3_pred_GLM_period_return_24_Price >
            mean_500_pred_GLM_period_return_24_Price + sd_500_pred_GLM_period_return_24_Price*2.5 |
    pred_GLM_period_return_24_Price >
              mean_500_pred_GLM_period_return_24_Price + sd_500_pred_GLM_period_return_24_Price*2.5)|
  (mean_3_pred_LM_period_return_24_Price >
          mean_50_pred_LM_period_return_24_Price + sd_50_pred_LM_period_return_24_Price*2.5 |
  pred_LM_period_return_24_Price >
            mean_50_pred_LM_period_return_24_Price + sd_50_pred_LM_period_return_24_Price*2.5)|
  (mean_3_pred_GLM_period_return_24_Price >
            mean_100_pred_GLM_period_return_24_Price + sd_100_pred_GLM_period_return_24_Price*2.5 |
    pred_GLM_period_return_24_Price >
              mean_100_pred_GLM_period_return_24_Price + sd_100_pred_GLM_period_return_24_Price*2.5)|
  (mean_3_pred_LM_period_return_24_Price >
          mean_100_pred_LM_period_return_24_Price + sd_100_pred_LM_period_return_24_Price*2 |
  pred_LM_period_return_24_Price >
            mean_100_pred_LM_period_return_24_Price + sd_100_pred_LM_period_return_24_Price*2)|
  (mean_3_pred_LM_period_return_24_Price >
          mean_500_pred_LM_period_return_24_Price + sd_500_pred_LM_period_return_24_Price*2 |
  pred_LM_period_return_24_Price >
            mean_500_pred_LM_period_return_24_Price + sd_500_pred_LM_period_return_24_Price*2)|
  (mean_3_pred_LM_period_return_24_Price >
          mean_2000_pred_LM_period_return_24_Price + sd_2000_pred_LM_period_return_24_Price*2 |
  pred_LM_period_return_24_Price >
            mean_2000_pred_LM_period_return_24_Price + sd_2000_pred_LM_period_return_24_Price*2))|

    ( pred_technical_4 >= pred_technical_4_mean + pred_technical_4_sd*2.75|
      pred_technical_6 >= pred_technical_6_mean + pred_technical_6_sd*2.5)"

win_thresh = 10

comnbined_statement_best_results <-
  post_preds_all_rolling_and_originals %>%
  pull(Asset) %>%
  unique() %>%
  map(
    ~
      post_ss_model_analyse_condition(
        tagged_trade_col_data = post_preds_all_rolling_and_originals,
        actual_wins_losses = actual_wins_losses,
        trade_statement = trade_statement,
        Asset_Var = .x,
        win_thresh = win_thresh,
        trade_direction = "Long",
        actual_wins_losses = actual_wins_losses
      ) %>%
      pluck(1) %>%
      ungroup() %>%
      filter(Period <= 44) %>%
      filter(Period >= 24) %>%
      filter(trade_col == "Long") %>%
      # group_by(Asset) %>%
      # slice_max(perc, n = 1) %>%
      group_by(Asset) %>%
      slice_max(Total_Returns) %>%
      ungroup()
  ) %>%
  keep(~ dim(.x)[1] > 0) %>%
  map_dfr(bind_rows)
# janitor::adorn_totals()

comnbined_statement_best_params <-
  comnbined_statement_best_results %>%
  dplyr::select(Asset, Period) %>%
  mutate(
    best_result = TRUE
  ) %>%
  distinct()

comnbined_statement_control <-
  post_preds_all_rolling_and_originals %>%
  pull(Asset) %>%
  unique() %>%
  map(
    ~
      post_ss_model_analyse_condition(
        tagged_trade_col_data = post_preds_all_rolling_and_originals,
        trade_statement = "pred_LM_period_return_24_Price > 0 |
                          pred_LM_period_return_24_Price <= 0 |
                          is.na(pred_LM_period_return_24_Price)|
                          is.nan(pred_LM_period_return_24_Price) |
                          is.infinite(pred_LM_period_return_24_Price)",
        Asset_Var = .x,
        win_thresh = win_thresh,
        trade_direction = "Long",
        actual_wins_losses = actual_wins_losses
      ) %>%
      pluck(1) %>%
      ungroup()
  ) %>%
  keep(~ dim(.x)[1] > 0) %>%
  map_dfr(bind_rows) %>%
  dplyr::left_join(comnbined_statement_best_params) %>%
  filter(best_result == TRUE) %>%
  arrange(Total_Returns) %>%
  # janitor::adorn_totals() %>%
  dplyr::select(Asset, Period,
                total_trades_control = total_trades,
                Total_Returns_Control = Total_Returns,
                Average_Return_Control = Average_Return,
                perc_control = perc)

comapre_results_summary <-
  comnbined_statement_best_results %>%
  dplyr::select(-trade_statement, -trade_col) %>%
  left_join(comnbined_statement_control) %>%
  mutate(
    trade_percent = total_trades/total_trades_control,
    Total_Returns_Control_adj = Total_Returns_Control*trade_percent
  ) %>%
  mutate(
    Returns_Diff = Total_Returns - Total_Returns_Control_adj,
    perc_diff = perc - perc_control
  )

comapre_results_summary$perc_diff %>% mean()
comapre_results_summary$Returns_Diff %>% mean()

comapre_results_summary <-
  comapre_results_summary %>%
  janitor::adorn_totals()

portfolio_ts <-
  post_preds_all_rolling_and_originals %>%
  # filter(Asset != 'BTC_USD') %>%
  pull(Asset) %>%
  unique() %>%
  map(
    ~
      post_ss_model_analyse_condition(
        tagged_trade_col_data = post_preds_all_rolling_and_originals,
        trade_statement = trade_statement,
        Asset_Var = .x,
        win_thresh = win_thresh,
        trade_direction = "Long",
        actual_wins_losses = actual_wins_losses
      ) %>%
      pluck(2) %>%
      ungroup() %>%
      left_join(
        comnbined_statement_best_params
      ) %>%
      filter(best_result == TRUE)
  ) %>%
  keep(~ dim(.x)[1] > 0) %>%
  map_dfr(bind_rows) %>%
  filter(trade_col == "Long")

portfolio_ts_control <-
  post_preds_all_rolling_and_originals %>%
  # filter(Asset != 'BTC_USD') %>%
  pull(Asset) %>%
  unique() %>%
  map(
    ~
      post_ss_model_analyse_condition(
        tagged_trade_col_data = post_preds_all_rolling_and_originals,
        trade_statement = "pred_LM_period_return_24_Price > 0 |
                          pred_LM_period_return_24_Price <= 0 |
                          is.na(pred_LM_period_return_24_Price)|
                          is.nan(pred_LM_period_return_24_Price) |
                          is.infinite(pred_LM_period_return_24_Price)",
        Asset_Var = .x,
        win_thresh = win_thresh,
        trade_direction = "Long",
        actual_wins_losses = actual_wins_losses
      ) %>%
      pluck(2) %>%
      ungroup() %>%
      left_join(
        comnbined_statement_best_params
      ) %>%
      filter(best_result == TRUE)
  ) %>%
  keep(~ dim(.x)[1] > 0) %>%
  map_dfr(bind_rows) %>%
  mutate(trade_col = "No Trade Long")

portfolio_ts <-
  portfolio_ts %>%
  bind_rows(portfolio_ts_control) %>%
  group_by(Date, trade_col) %>%
  summarise(Returns = sum(Returns, na.rm = T )) %>%
  group_by(trade_col) %>%
  arrange(Date, .by_group = TRUE) %>%
  group_by(trade_col) %>%
  mutate(
    Returns_cumulative = cumsum(Returns)
  )

portfolio_ts %>%
  ggplot(aes(x = Date, y = Returns_cumulative, color = trade_col)) +
  geom_line() +
  facet_wrap(.~ trade_col, scales = "free_y") +
  theme_minimal() +
  theme(legend.position = "bottom")
