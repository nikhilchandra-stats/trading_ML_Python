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
db_location = "D:/Asset Data//Oanda_Asset_Data_Most_Assets_2025-09-13.db"
start_date = "2019-01-01"
end_date = today() %>% as.character()

bin_factor = NULL
stop_value_var = 10
profit_value_var = 50
period_var = 50

Indices_Metals_Bonds <- list()

Indices_Metals_Bonds[[1]] <-
  get_db_data_quickly_algo(
    db_location = db_location,
    start_date = start_date,
    end_date = as.character(today() + days(30)),
    time_frame = "H1",
    bid_or_ask = "ask",
    assets =   c("SPX500_USD","US2000_USD","EU50_EUR","SG30_SGD" ,
                 "AU200_AUD" ,"XAG_USD","XAU_USD","USD_JPY" ,
                 "AUD_USD" ,"UK100_GBP" ,"JP225Y_JPY","FR40_EUR" ,
                 "CH20_CHF","USB10Y_USD","USB02Y_USD" ,"UK10YB_GBP" ,
                 "HK33_HKD" ,"EUR_USD" ,"GBP_USD" ,"XAG_EUR" ,
                 "XAU_EUR" ,"XAU_GBP" ,"XAG_GBP" ,"EUR_GBP" ,
                 "WTICO_USD" ,"BCO_USD" ,"XCU_USD" ,"XAU_JPY",
                 "XAG_JPY" ,"XAU_AUD" ,"XAG_AUD" ,"USD_CAD" ,
                 "EUR_AUD" ,"NZD_USD" ,"EUR_NZD" ,"AUD_NZD" ,
                 "GBP_AUD" ,"GBP_NZD" ,"GBP_CAD" ,"GBP_JPY" ,
                 "USD_SGD" ,"EUR_JPY" , "BTC_USD" ,"ETH_USD" ,"NATGAS_USD" ,
                 "EUR_SEK" ,"USD_SEK" ,"LTC_USD" , "XAG_NZD")
  ) %>%
  distinct()
Indices_Metals_Bonds[[2]] <-
  get_db_data_quickly_algo(
    db_location = db_location,
    start_date = start_date,
    end_date = as.character(today() + days(30)),
    time_frame = "H1",
    bid_or_ask = "bid",
    assets =   c("SPX500_USD","US2000_USD","EU50_EUR","SG30_SGD" ,
                 "AU200_AUD" ,"XAG_USD","XAU_USD","USD_JPY" ,
                 "AUD_USD" ,"UK100_GBP" ,"JP225Y_JPY","FR40_EUR" ,
                 "CH20_CHF","USB10Y_USD","USB02Y_USD" ,"UK10YB_GBP" ,
                 "HK33_HKD" ,"EUR_USD" ,"GBP_USD" ,"XAG_EUR" ,
                 "XAU_EUR" ,"XAU_GBP" ,"XAG_GBP" ,"EUR_GBP" ,
                 "WTICO_USD" ,"BCO_USD" ,"XCU_USD" ,"XAU_JPY",
                 "XAG_JPY" ,"XAU_AUD" ,"XAG_AUD" ,"USD_CAD" ,
                 "EUR_AUD" ,"NZD_USD" ,"EUR_NZD" ,"AUD_NZD" ,
                 "GBP_AUD" ,"GBP_NZD" ,"GBP_CAD" ,"GBP_JPY" ,
                 "USD_SGD" ,"EUR_JPY" , "BTC_USD" ,"ETH_USD" ,"NATGAS_USD" ,
                 "EUR_SEK" ,"USD_SEK" ,"LTC_USD" , "XAG_NZD")
  ) %>%
  distinct()

tictoc::tic()
all_preds <-
  Single_Asset_V3_get_all_preds(
    Indices_Metals_Bonds = Indices_Metals_Bonds,
    raw_macro_data = raw_macro_data,
    base_path = "D:/trade_data/Day_Trader_Single_Asset_V3_Expanded_Models/",
    actuals_periods_needed = c("period_return_50_Price"),
    correlation_rolling_periods = c(100,200, 300,400, 500),
    state_space_periods = c(20, 40, 60, 100, 200,300, 400,  500),
    state_space_rolling = c(100, 200, 300, 400),
    date_for_true_simualtion = "2019-01-01",
    training_end_date = "2021-01-01",
    asset_index_start = 1,
    asset_index_end = 35
  )
tictoc::toc()


all_preds_dfr <- all_preds

trade_direction <- "Long"

generated_preds <-
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
        # "EUR_USD", #1
        # "EU50_EUR", #2
        # "SPX500_USD", #3
        # "US2000_USD", #4
        # "USB10Y_USD", #5
        # "USD_JPY", #6
        # "AUD_USD", #7
        # "EUR_GBP", #8
        # "AU200_AUD" ,#9
        # "EUR_AUD", #10
        # "WTICO_USD", #11
        # "UK100_GBP", #12
        # "USD_CAD", #13
        # "GBP_USD", #14
        # "GBP_CAD", #15
        # "EUR_JPY", #16
        # "EUR_NZD", #17
        # "XAG_USD", #18
        # "XAG_EUR", #19
        # "XAG_AUD", #20
        # "XAG_NZD", #21
        # "HK33_HKD", #22
        # "FR40_EUR", #23
        # "BTC_USD", #24
        # "XAG_GBP", #25
        # "GBP_AUD", #26
        # "USD_SEK", #27
        # "USD_SGD", #28
        # "NZD_USD", #29
        # "GBP_NZD", #30
        # "XCU_USD", #31
        # "NATGAS_USD", #32
        # "GBP_JPY", #33
        # "SG30_SGD", #34
        # "XAU_USD", #35
        # "EUR_SEK", #36
        # "XAU_AUD", #37
        # "UK10YB_GBP", #38
        # "JP225Y_JPY", #39
        # "ETH_USD" #40
        "EU50_EUR"
      ),
    asset_data = Indices_Metals_Bonds,
    # stop_factor = stop_value_var,
    # profit_factor = profit_value_var,
    stop_factor = 3,
    profit_factor = 6,
    risk_dollar_value = 10,
    trade_direction = "Long",
    currency_conversion = currency_conversion,
    asset_infor = asset_infor,
    periods_ahead = period_var
  )

trade_statement <-
  "
  # #stop_factor = 2, profit_factor = 3, period = 12
  # (state_space_LM_Pred_period_return_50_Price >= 15 &
  # state_space_LM_Pred_period_return_50_Price <= 25 &
  # Asset == 'HK33_HKD')|
  # (state_space_GLM_Pred_period_return_50_Price >= 0.96 &
  # state_space_GLM_Pred_period_return_50_Price <= 0.98 &
  # Asset == 'HK33_HKD')|
  #
  #  #stop_factor = 5, profit_factor = 10, period = 12
  #  (state_space_GLM_Pred_period_return_50_Price >= 0.98 &
  #  state_space_GLM_Pred_period_return_50_Price <= 1 &
  #  Asset == 'BTC_USD')|
  #  (AR_GLM_Pred_period_return_50_Price >= 0.9 &
  #  AR_GLM_Pred_period_return_50_Price <= 0.99 &
  #  Asset == 'BTC_USD')|
  #
  #  #stop_factor = 3, profit_factor = 6, period = 24
  #  (state_space_GLM_Pred_period_return_50_Price >= 0.99 &
  #  state_space_GLM_Pred_period_return_50_Price <= 1 &
  #  Asset == 'AUD_USD')|
  #  (AR_GLM_Pred_period_return_50_Price >= 0.675 &
  #  AR_GLM_Pred_period_return_50_Price <= 1 &
  #  Asset == 'AUD_USD')|
  #
  #  #stop_factor = 3, profit_factor = 6, period = 12
  #  (state_space_GLM_Pred_period_return_50_Price >= 0.95 &
  #  state_space_GLM_Pred_period_return_50_Price <= 1 &
  #  Asset == 'USD_JPY')|
  #  (state_space_LM_Pred_period_return_50_Price >= 9.5 &
  #  state_space_LM_Pred_period_return_50_Price <= 1000 &
  #  Asset == 'USD_JPY')|
  #
  #  #stop_factor = 2, profit_factor = 4, period = 12
  #  (state_space_LM_Pred_period_return_50_Price >= 1.75 &
  #  state_space_LM_Pred_period_return_50_Price <= 2 &
  #  Asset == 'WTICO_USD')|
  #  (state_space_LM_Pred_period_return_50_Price >= 6 &
  #  state_space_LM_Pred_period_return_50_Price <= 1000 &
  #  Asset == 'WTICO_USD')|
  #  (state_space_GLM_Pred_period_return_50_Price >= 0.55 &
  #  state_space_GLM_Pred_period_return_50_Price <= 0.625 &
  #  Asset == 'WTICO_USD')|
  #
  #  #stop_factor = 5, profit_factor = 10, period = 24
  #  (state_space_LM_Pred_period_return_50_Price >= 0.4 &
  #  state_space_LM_Pred_period_return_50_Price <= 1 &
  #  Asset == 'SG30_SGD')|
  #
  #  # #stop_factor = 4, profit_factor = 8, period = 12
  #  (AR_GLM_Pred_period_return_50_Price >= 0.75 &
  #  state_space_GLM_Pred_period_return_50_Price >= 0.75 &
  #  Asset == 'XCU_USD')|
  #  (AR_LM_Pred_period_return_50_Price >= 0 &
  #   state_space_LM_Pred_period_return_50_Price >= 0 &
  #   AR_LM_Pred_period_return_50_Price <= 0.25 &
  #   Asset == 'XCU_USD')|
  #   (
  #   AR_LM_Pred_period_return_50_Price >=
  #   AR_LM_Pred_period_return_50_Price_mean + 0.25*AR_LM_Pred_period_return_50_Price_sd &
  #   AR_LM_Pred_period_return_50_Price >= 0.5 &
  #   Asset == 'XCU_USD'
  #   )

  #  # #stop_factor = 3, profit_factor = 6, period = 12
  # (state_space_LM_Pred_period_return_50_Price >= 7 & Asset == 'XAU_USD')|
  # (state_space_LM_Pred_period_return_50_Price >= 2.75 &
  # AR_LM_Pred_period_return_50_Price >= 0 &
  # AR_LM_Pred_period_return_50_Price < 2 &
  # Asset == 'XAU_USD')|
  # (
  # state_space_GLM_Pred_period_return_50_Price >= 0.9 &
  # state_space_GLM_Pred_period_return_50_Price <= 0.99 &
  # Asset == 'XAU_USD'
  # )

  #  # #stop_factor = 3, profit_factor = 6, period = 12
  # (state_space_LM_Pred_period_return_50_Price >= 9 &
  # state_space_LM_Pred_period_return_50_Price < 1000 &
  #   Asset == 'AU200_AUD')|
  # (state_space_GLM_Pred_period_return_50_Price >= 0.955 &
  #    state_space_GLM_Pred_period_return_50_Price <= 1 &
  #    Asset == 'AU200_AUD')|
  # (
  #   state_space_LM_Pred_period_return_50_Price >=
  #   state_space_LM_Pred_period_return_50_Price_mean + 1.25*state_space_LM_Pred_period_return_50_Price_sd &
  #   state_space_LM_Pred_period_return_50_Price <=
  #   state_space_LM_Pred_period_return_50_Price_mean + 2*state_space_LM_Pred_period_return_50_Price_sd &
  #   state_space_LM_Pred_period_return_50_Price > 6 &
  #   Asset == 'AU200_AUD'
  # )|
  # (
  #   state_space_GLM_Pred_period_return_50_Price >=
  #   state_space_GLM_Pred_period_return_50_Price_mean + 0.25*state_space_GLM_Pred_period_return_50_Price_sd &
  #   state_space_GLM_Pred_period_return_50_Price <=
  #   state_space_GLM_Pred_period_return_50_Price_mean + 1.25*state_space_GLM_Pred_period_return_50_Price_sd &
  #   AR_GLM_Pred_period_return_50_Price > 0.65 &
  #   Asset == 'AU200_AUD'
  # )

  #  # #stop_factor = 3, profit_factor = 6, period = 12
  (state_space_GLM_Pred_period_return_50_Price >= 0.5 &
   state_space_GLM_Pred_period_return_50_Price <= 0.57 &
   Asset == 'EU50_EUR')|
    (
    AR_LM_Pred_period_return_50_Price >=
    AR_LM_Pred_period_return_50_Price_mean + 0*AR_LM_Pred_period_return_50_Price_sd &
    AR_LM_Pred_period_return_50_Price <=
    AR_LM_Pred_period_return_50_Price_mean + 10*AR_LM_Pred_period_return_50_Price_sd &
    AR_LM_Pred_period_return_50_Price >= 0 &
    Asset == 'EU50_EUR'
   )|
  (state_space_LM_Pred_period_return_50_Price > 0 &
    AR_LM_Pred_period_return_50_Price > 0 &
    Asset == 'EU50_EUR')

  "

cumulative_returns_sim_data <-
  get_total_portfolio_summary(
    generated_preds = generated_preds %>%
      filter(Asset == "EU50_EUR")
      # filter( str_detect(Asset, "[A-Z]"))
    ,
    trade_statement = trade_statement,
    actual_wins_losses =actual_wins_losses %>%
      filter(Asset == "EU50_EUR")
      # filter( str_detect(Asset, "[A-Z]"))
    ,
    trade_direction = "Long",
    return_col = "period_return_12_Price"
  )

cumulative_returns_sim_data %>%
  ggplot(aes(x = Date, y = Cumulative_Return)) +
  geom_line() +
  facet_wrap(.~trade_col, scales = "free") +
  scale_y_continuous(n.breaks = 20) +
  theme_minimal()

asset_summaries_control <-
  get_asset_random_sim_returns(
    generated_preds = generated_preds %>%
      filter(Asset == "EU50_EUR")
      # filter( str_detect(Asset, "[A-Z]"))
    ,
    trade_statement = "str_detect(Asset, '[A-Z]')",
    actual_wins_losses = actual_wins_losses %>%
      filter(Asset == "EU50_EUR")
      # filter( str_detect(Asset, "[A-Z]"))
    ,
    trade_direction = "Long",
    return_col = "period_return_12_Price",
    simulations = 5000,
    samples = 50
  )

asset_summaries <-
  get_asset_random_sim_returns(
    generated_preds = generated_preds %>%
      filter(Asset == "EU50_EUR")
      # filter( str_detect(Asset, "[A-Z]"))
    ,
    trade_statement = trade_statement,
    actual_wins_losses = actual_wins_losses %>%
      filter(Asset == "EU50_EUR")
      # filter( str_detect(Asset, "[A-Z]"))
    ,
    trade_direction = "Long",
    return_col = "period_return_12_Price",
    simulations = 7000,
    samples = 50
  )

traded_assets <- c("EUR_USD", #1
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
                   "UK10YB_GBP" #38
) %>% unique()

portfolio_structure <- list()

for (i in 1:length(traded_assets)) {

  tagged_trades <-
    generated_preds %>%
    mutate(
      trade_col =
        eval(parse(text = trade_statement)),
      trade_col =
        ifelse(trade_col == TRUE, trade_direction, paste0("No Trade ", trade_direction) )
    ) %>%
    distinct(Asset, Date, trade_col) %>%
    filter(trade_col == "Long") %>%
    filter(Asset == traded_assets[i])

  portfolio_structure[[i]] <-
    get_portfolio_model(
      asset_data = Indices_Metals_Bonds,
      asset_of_interest = traded_assets[i],
      tagged_trades = tagged_trades,
      stop_factor_long = 10,
      profit_factor_long = 50,
      risk_dollar_value_long = 10,
      end_period = 50,
      time_frame = "H1",
      trade_direction = "Long"
    )

}

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

