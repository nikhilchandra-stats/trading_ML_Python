helpeR::load_custom_functions()

all_aud_symbols <- get_oanda_symbols() %>%
  keep(~ str_detect(.x, "AUD")|str_detect(.x, "USD_SEK|USD_NOK|USD_HUF|USD_ZAR|USD_CNY|USD_MXN|USD_CZK"))
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
db_location = "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data_Most_Assets_2025-09-13 2.db"
start_date = "2020-03-01"
end_date = today() %>% as.character()

bin_factor = NULL
stop_value_var = 15
profit_value_var = 60
period_var = 50

Indices_Metals_Bonds <- list()

Indices_Metals_Bonds[[1]] <-
  get_db_data_quickly_algo(
    db_location = db_location,
    start_date = start_date,
    end_date = as.character(today() + days(30)),
    time_frame = "H1",
    bid_or_ask = "ask",
    assets =   c("WHEAT_USD", #1 WHEAT_USD
                 "SUGAR_USD", #2 SUGAR_USD
                 "DE30_EUR", #3 DE30_EUR
                 "UK10YB_GBP", #4 UK10YB_GBP
                 "EUR_CHF", #5 EUR_CHF
                 "GBP_CHF", #7 GBP_CHF
                 "USD_CZK",  #9 USD_CZK
                 "USD_NOK", #10 USD_NOK
                 "GBP_NZD", #14 GBP_NZD
                 "NZD_CHF", #15 NZD_CHF
                 "CH20_CHF", #17 CH20_CHF
                 "XPT_USD", #18
                 "SOYBN_USD", #19
                 "JP225_USD", #20
                 "XPD_USD", #21
                 "NL25_EUR" #22
    )
  ) %>%
  distinct()
Indices_Metals_Bonds[[2]] <-
  get_db_data_quickly_algo(
    db_location = db_location,
    start_date = start_date,
    end_date = as.character(today() + days(30)),
    time_frame = "H1",
    bid_or_ask = "bid",
    assets =   c(
      "WHEAT_USD", #1 WHEAT_USD
      "SUGAR_USD", #2 SUGAR_USD
      "DE30_EUR", #3 DE30_EUR
      "UK10YB_GBP", #4 UK10YB_GBP
      "EUR_CHF", #5 EUR_CHF
      "GBP_CHF", #6 GBP_CHF
      "USD_CZK",  #7 USD_CZK
      "USD_NOK", #8 USD_NOK
      "GBP_NZD", #9 GBP_NZD
      "NZD_CHF", #10 NZD_CHF
      "CH20_CHF", #11 CH20_CHF
      "XPT_USD", #12
      "SOYBN_USD", #13
      "JP225_USD", #14
      "XPD_USD", #15
      "NL25_EUR" #16
    )
  ) %>%
  distinct()

tictoc::tic()
all_preds <-
  Single_Asset_V3_get_all_preds(
    Indices_Metals_Bonds = Indices_Metals_Bonds,
    raw_macro_data = raw_macro_data,
    # base_path = "C:/Users/Nikhil Chandra/Documents/trade_data/Day_Trader_Single_Asset_V4_Expanded_Models/",
    base_path = "C:/Users/Nikhil Chandra/Documents/trade_data/Day_Trader_Single_Asset_V4_Expanded_Models/",
    actuals_periods_needed = c("period_return_50_Price"),
    correlation_rolling_periods = c(100,200, 300,400, 500),
    state_space_periods = c(20, 40, 60, 100, 200,300, 400,  500),
    state_space_rolling = c(100, 200, 300, 400),
    date_for_true_simualtion = "2020-01-01",
    training_end_date = "2021-01-01",
    asset_index_start = 1,
    asset_index_end = 14
    # asset_index_end = 12
  )
tictoc::toc()

all_preds_dfr <-
  all_preds
# map_dfr(bind_rows)

trade_direction <- "Long"
actual_wins_losses <-
  get_actual_wins_losses(
    assets_to_analyse =
      c(
        "WHEAT_USD", #1 WHEAT_USD
        "SUGAR_USD", #2 SUGAR_USD
        "DE30_EUR", #3 DE30_EUR
        "UK10YB_GBP", #4 UK10YB_GBP
        "EUR_CHF", #5 EUR_CHF
        "GBP_CHF", #7 GBP_CHF
        "USD_CZK",  #9 USD_CZK
        "USD_NOK", #10 USD_NOK
        "GBP_NZD", #14 GBP_NZD
        "NZD_CHF", #15 NZD_CHF
        "CH20_CHF", #17 CH20_CHF
        "XPT_USD", #18
        "SOYBN_USD", #19
        "JP225_USD", #20
        "XPD_USD", #21
        "NL25_EUR" #22
      ),
    asset_data = Indices_Metals_Bonds,
    # stop_factor = stop_value_var,
    # profit_factor = profit_value_var,
    # risk_dollar_value = 10,

    # stop_factor = 4, #Original Testing accidently done with 4
    stop_factor = 10,
    profit_factor = 5,
    risk_dollar_value = 10,


    trade_direction = "Long",
    currency_conversion = currency_conversion,
    asset_infor = asset_infor,
    periods_ahead = period_var
  )

generated_preds_from_db <-
  all_preds_dfr %>%
  mutate(
    across(.cols = c(Date, training_end_date, date_for_true_simualtion),
           .fns = ~ as_datetime(., tz = "Australia/Canberra"))
  ) %>%
  filter(Date > training_end_date) %>%
  filter(Date > date_for_true_simualtion)


actual_wins_losses <-
  get_actual_wins_losses(
    assets_to_analyse =
      c(
        # "WHEAT_USD", #1 WHEAT_USD
        # "SUGAR_USD", #2 SUGAR_USD
        # "DE30_EUR", #3 DE30_EUR
        # "UK10YB_GBP", #4 UK10YB_GBP
        # "EUR_CHF", #5 EUR_CHF
        # "GBP_CHF", #7 GBP_CHF
        # "USD_CZK",  #9 USD_CZK
        # "USD_NOK", #10 USD_NOK
        # "GBP_NZD", #14 GBP_NZD
        # "NZD_CHF", #15 NZD_CHF
        # "CH20_CHF", #17 CH20_CHF
        # "XPT_USD", #18
        # "SOYBN_USD", #19
        # "JP225_USD", #20
        # "XPD_USD", #21
        # "NL25_EUR" #22
        # "CH20_CHF",
        # "JP225_USD",
        "DE30_EUR"
      ),
    asset_data = Indices_Metals_Bonds,
    # stop_factor = stop_value_var,
    # profit_factor = profit_value_var,
    # risk_dollar_value = 10,

    # stop_factor = 4, #Original Testing accidently done with 4
    stop_factor = 15,
    profit_factor = 100,
    risk_dollar_value = 10,


    trade_direction = "Long",
    currency_conversion = currency_conversion,
    asset_infor = asset_infor,
    periods_ahead = period_var
  )

trade_statement <-
  "
  # # stop = 2, profit = 3, time = 10
  (AR_GLM_Pred_period_return_50_Price >= 0.625 &
  AR_GLM_Pred_period_return_50_Price <= 1 &
  Asset == 'DE30_EUR')|
  (state_space_GLM_Pred_period_return_50_Price >= 0.7 &
  state_space_GLM_Pred_period_return_50_Price <= 0.83 &
  Asset == 'DE30_EUR')|
  # stop = 2, profit = 3, time = 10
  (AR_GLM_Pred_period_return_50_Price >= 0.55 &
  AR_GLM_Pred_period_return_50_Price <= 1 &
  Asset == 'JP225_USD')|
  (state_space_GLM_Pred_period_return_50_Price >= 0.55 &
  state_space_GLM_Pred_period_return_50_Price <= 0.58 &
  Asset == 'JP225_USD')|
  (
  state_space_GLM_Pred_period_return_50_Price > 0.69 &
  state_space_GLM_Pred_period_return_50_Price < 0.725 &
  Asset == 'CH20_CHF'
  )|
  (
  AR_GLM_Pred_period_return_50_Price > 0.58 &
  AR_GLM_Pred_period_return_50_Price < 0.6 &
  Asset == 'CH20_CHF'
  )

  "

cumulative_returns_sim_data <-
  get_total_portfolio_summary(
    generated_preds = generated_preds_from_db %>%
      filter(Asset %in%  c("CH20_CHF", "DE30_EUR", "JP225_USD"))
      # filter(str_detect(Asset, "[A-Z]"))
    ,
    trade_statement = trade_statement,
    actual_wins_losses =
      actual_wins_losses %>%
      filter(Asset %in%  c("CH20_CHF", "DE30_EUR", "JP225_USD"))
      # filter(str_detect(Asset, "[A-Z]"))
    ,
    trade_direction = "Long",
    return_col = "period_return_10_Price"
  )

cumulative_returns_sim_data %>%
  ggplot(aes(x = Date, y = Cumulative_Return)) +
  geom_line() +
  facet_wrap(.~trade_col, scales = "free") +
  scale_y_continuous(n.breaks = 20) +
  theme_minimal()

asset_summaries <-
  get_asset_random_sim_returns(
    generated_preds =
      generated_preds_from_db %>%
      filter(Asset %in%  c("CH20_CHF", "DE30_EUR", "JP225_USD"))
      # filter(str_detect(Asset, "[A-Z]"))
    ,
    trade_statement = trade_statement,
    actual_wins_losses =
      actual_wins_losses %>%
      filter(Asset %in%  c("CH20_CHF", "DE30_EUR", "JP225_USD"))
      # filter(str_detect(Asset, "[A-Z]"))
    ,
    trade_direction = "Long",
    return_col = "period_return_10_Price",
    simulations = 5000,
    samples = 50
  )

asset_summaries_control <-
  get_asset_random_sim_returns(
    generated_preds =
      generated_preds_from_db %>%
      filter(Asset %in%  c("CH20_CHF", "DE30_EUR", "JP225_USD"))
      # filter(str_detect(Asset, "[A-Z]"))
    ,
    trade_statement = "str_detect(Asset, '[A-Z]')",
    actual_wins_losses =
      actual_wins_losses %>%
      filter(Asset %in%  c("CH20_CHF", "DE30_EUR", "JP225_USD"))
      # filter(str_detect(Asset, "[A-Z]"))
    ,
    trade_direction = "Long",
    return_col = "period_return_10_Price",
    simulations = 5000,
    samples = 50
  )

distinct_stops_profs <-
  actual_wins_losses %>%
  distinct(Asset, stop_return, profit_return)

traded_assets <-
  c(
    # "WHEAT_USD", #1 WHEAT_USD
    # "SUGAR_USD", #2 SUGAR_USD
    "DE30_EUR" #3 DE30_EUR
    # "UK10YB_GBP", #4 UK10YB_GBP
    # "EUR_CHF", #5 EUR_CHF
    # "EUR_SEK", #6 EUR_SEK
    # "GBP_CHF", #7 GBP_CHF
    # "GBP_JPY", #8 GBP_JPY
    # "USD_CZK",  #9 USD_CZK
    # "USD_NOK", #10 USD_NOK
    # "XAG_CAD",  #11 XAG_CAD
    # "XAG_CHF",  #12 XAG_CHF
    # "XAG_JPY",   #13 XAG_JPY
    # "GBP_NZD", #14 GBP_NZD
    # "NZD_CHF", #15 NZD_CHF
    # "USD_MXN",  #16 USD_MXN
    # "CH20_CHF", #17 CH20_CHF
    # "XPT_USD", #18
    # "SOYBN_USD", #19
    # "JP225_USD", #20
    # "XPD_USD", #21
    # "NL25_EUR" #22
  ) %>% unique()

return_structure <-
  get_portfolio_struc_with_end_points(
    Indices_Metals_Bonds =
      Indices_Metals_Bonds %>% map(~ .x %>% filter(Date >= "2023-01-01") ),
    trade_data = generated_preds_from_db %>% filter(Date >= "2023-01-01"),
  traded_assets = c("DE30_EUR"),
  trade_statement_for_filter = "str_detect(Asset, '[A-Z]')",
  low_point_end = c(-15,-8, -6,-3, -4,-2),
  high_point_end = c(2.5, 5,10,15,30, 40),
  stop_factor_var = 5,
  profit_factor_var = 50,
  risk_dollar_value_var = 10,
  end_period_var = 50,
  time_frame_var = "H1",
  trade_direction = "Long"
)

return_structure_summary <-
  return_structure %>%
  ungroup() %>%
  filter(period_since_open == true_end_point) %>%
  group_by(low_point_end, high_point_end, profit_factor, stop_factor, trade_direction) %>%
  summarise(Return = sum(Return, na.rm = T))


construct_portfolio_sim <-
  function(
    portfolio_structure = portfolio_structure,
    starting_capital = 20000
  ) {

    distinct_dates <-
      portfolio_structure %>%
      distinct(adjusted_Date) %>%
      pull(adjusted_Date)

    all_end_points <-
      portfolio_structure %>%
      filter(period_since_open == close_Date) %>%
      group_by(adjusted_Date) %>%
      summarise(Return = sum(Return, na.rm = T)) %>%
      ungroup() %>%
      arrange(adjusted_Date) %>%
      mutate(
        Cumulative_Return = cumsum(Return) + starting_capital
      ) %>%
      mutate(
        REALISED_THIS_DATE = Return,
        END_TRADE_DATES = adjusted_Date
      )

    all_portfolio_NAV <-
      portfolio_structure %>%
      group_by(adjusted_Date) %>%
      summarise(Return = sum(Return, na.rm = T)) %>%
      ungroup() %>%
      arrange(adjusted_Date) %>%
      left_join(all_end_points) %>%
      fill(Cumulative_Return, .direction = "down") %>%
      mutate(
        REALISED_THIS_DATE =
          ifelse(is.na(REALISED_THIS_DATE), 0, REALISED_THIS_DATE)
      ) %>%
      mutate(
        NAV = Cumulative_Return + (Return - REALISED_THIS_DATE)
      )


    all_portfolio_NAV %>%
      ggplot(aes(x = adjusted_Date, y = NAV)) +
      geom_line() +
      theme_minimal()

    max_portfolio_deviation <-
      all_portfolio_NAV %>%
      dplyr::select(adjusted_Date, Return) %>%
      mutate(
        Deviation = starting_capital + Return
      )

    max_portfolio_deviation %>%
      ggplot(aes(x = adjusted_Date, y = Deviation)) +
      geom_line() +
      theme_minimal()

  }
