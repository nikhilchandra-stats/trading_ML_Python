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
    "USB02Y_USD", "USB05Y_USD", "USB30Y_USD", "USB10Y_USD", "UK100_GBP") %>%
  unique()

asset_infor <- get_instrument_info()
#---------------------Data
load_custom_functions()
db_location = "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data_Most_Assets_2025-09-13.db"
start_date = "2017-01-01"
end_date = today() %>% as.character()
Indices_Metals_Bonds <- list()

assets_to_port =
  c(
    "EUR_JPY",
    "EUR_USD",
    "EUR_GBP",
    "EU50_EUR"
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
file_name = "EUR_EXPNDED_ONLY_NON_V3_NEW_MODEL"
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
  get_dynamic_stop_prof_returns(
    Ask_Data = Indices_Metals_Bonds[[1]],
    Bid_Data = Indices_Metals_Bonds[[2]],
    periods_wanted = end_period,
    trade_direction = "Long",
    currency_conversion =currency_conversion,
    asset_infor = asset_infor,
    slippage_percent = 0,
    risk_dollar_value = 5,
    volatility_factor_stop = 2,
    volatility_factor_profit = 2,
    profit_multiple = 1.1,
    running_volatility_period_max = 20,
    running_volatility_period_mean = 100
  )


get_dynamic_portfolio_no_V3 <-
  function(
    portfolio_data = portfolio_data_train,
    xtnd_ss_cols_PR_cols = c(1,2,5,10),
    roll_period_state_space = 500,
    xtnd_ss_cols_BR_periods = c(100,200,300),
    xtnd_rolling_volatility = c(20,50,60,80,100),
    lag_dependant = end_period + 1,
    auto_cor_cols = 25,
    cor_skip_periods = c(2,4,5,6),
    cor_period = 50,
    periods_to_use_deviation = c(1,3,4),
    mean_periods_deviation = c(50, 100)
    ) {

    total_reg_data <-
      portfolio_data %>%
      ungroup() %>%
      distinct(Date, Asset)

    total_return_portfolio <-
      portfolio_data %>%
      ungroup() %>%
      group_by(Date) %>%
      summarise(
        across(.cols = c(Final_Return, contains("period_return_")),
               .fns = ~ sum(., na.rm = T))
      ) %>%
      ungroup() %>%
      mutate(Asset = "Portfolio") %>%
      mutate(across(.cols = contains("period_return_"), .fns = ~ as.numeric(.)))

    deviation_data <-
      portfolio_get_deviation_from_total(
        total_return_portfolio = total_return_portfolio,
        portfolio_data = portfolio_data,
        periods_to_use_deviation = periods_to_use_deviation,
        mean_periods_deviation = mean_periods_deviation
      )

    req_ss_extnd_cols_PR <-
      xtnd_ss_cols_PR_cols %>%
      map(
        ~
          glue::glue(
            "
          state_space_data_PR_{.x} <-
              portfolio_LM_state_space(
                  portfolio_data = portfolio_data,
                  state_space_col = 'period_return_{.x}_Price' ,
                  required_lag = {.x}, #Does not need a plus 1 its built in
                  roll_period_state_space = {roll_period_state_space}
                )"
          )
      )  %>%
      unlist() %>%
      as.character() %>%
      paste(collapse = "\n")

    req_ss_extnd_cols_PR_names <-
      xtnd_ss_cols_PR_cols %>%
      map(
        ~ glue::glue("state_space_data_PR_{.x}")
      ) %>%
      unlist() %>%
      as.character() %>%
      paste(collapse = ",")

    req_ss_extnd_cols_PR_list <-
      glue::glue("All_state_space_data_PR <- list({req_ss_extnd_cols_PR_names})")

    rm_statement <- glue::glue("rm({req_ss_extnd_cols_PR_names})")

    eval(parse(text = req_ss_extnd_cols_PR))
    eval(parse(text = req_ss_extnd_cols_PR_list))
    eval(parse(text = rm_statement))
    gc()

    All_state_space_data_PR <-
      All_state_space_data_PR %>%
      reduce(left_join)

    All_state_space_data_PR <-
      All_state_space_data_PR %>%
      dplyr::select(Date, Asset, contains("state_space"))

    message("Made it to State SPace End line 2750")

    All_state_space_data_PR <- All_state_space_data_PR

    total_reg_data <-
      total_reg_data %>%
      left_join(All_state_space_data_PR %>% ungroup()) %>%
      left_join(deviation_data %>% ungroup())

    rm(deviation_data)

    message("Made it to State SPace joined with TOtal Reg End line 2771")

    req_BR_extnd_cols_PR <-
      xtnd_ss_cols_BR_periods %>%
      map(
        ~ glue::glue("
          brownian_tech_data_{.x} <-
                portfolio_LM_brownian_checks(portfolio_data = portfolio_data,
                                             brownian_period = {.x},
                                             col_to_use = 'period_return_1_Price',
                                             lag_period_to_use = 1)
          brownian_tech_data_FR_{.x} <-
                portfolio_LM_brownian_checks(portfolio_data = portfolio_data,
                                             brownian_period = {.x},
                                             col_to_use = 'Final_Return',
                                             lag_period_to_use = {lag_dependant} )"
        )
      ) %>%
      unlist() %>%
      as.character() %>%
      paste(collapse = "\n")

    req_BR_extnd_cols_PR_names <-
      xtnd_ss_cols_BR_periods %>%
      map(
        ~ glue::glue("brownian_tech_data_{.x}, brownian_tech_data_FR_{.x}")
      ) %>%
      unlist() %>%
      as.character() %>%
      paste(collapse = ",")

    req_BR_extnd_cols_PR_list <-
      glue::glue("All_BR_data_PR <- list({req_BR_extnd_cols_PR_names})")

    rm_statement <- glue::glue("rm({req_BR_extnd_cols_PR_names})")

    eval(parse(text = req_BR_extnd_cols_PR))
    message("Made it to Brownian First Statement req_BR_extnd_cols_PR line 2808")
    eval(parse(text = req_BR_extnd_cols_PR_list))
    message("Made it to Brownian Second Statement req_BR_extnd_cols_PR line 2810")
    eval(parse(text = rm_statement))
    message("Made it to Brownian Third Statement req_BR_extnd_cols_PR line 2812")
    gc()

    All_BR_data_PR <-
      All_BR_data_PR %>%
      reduce(left_join)

    message("Made it to All_BR_data_PR statement line 2819")

    total_reg_data <-
      total_reg_data %>%
      left_join(All_BR_data_PR %>%
                  dplyr::select(Date, Asset, contains("brownian"))
      )

    message("Made it to total_reg_data statement line 2827")

    rm(All_state_space_data_PR, All_BR_data_PR)


    vola_roll_max_statements <-
      xtnd_rolling_volatility %>%
      map(
        ~
          glue::glue("roll_vol_{.x}_max = slider::slide_dbl(.x = ( lag(Bid_High) - lag(Ask_Price) ), .f = ~ (max(cumsum(diff(.x)), na.rm = T)) ,.before = {.x})")
      ) %>%
      unlist()

    vola_roll_min_statements <-
      xtnd_rolling_volatility %>%
      map(
        ~
          glue::glue("roll_vol_{.x}_min = slider::slide_dbl(.x = (lag(Ask_Price) - lag(Bid_Low)), .f = ~ (min(cumsum(diff(.x)), na.rm = T)) ,.before = {.x})")

      ) %>%
      unlist()

    vola_roll_all <-
      list(
        vola_roll_max_statements,
        vola_roll_min_statements
      ) %>%
      unlist() %>%
      paste(collapse = ",")

    vola_roll_all_mutate <-
      glue::glue("portfolio_data %>%
                   group_by(Asset) %>%
                   arrange(Date, .by_group = TRUE) %>%
                   group_by(Asset) %>%
                   mutate({vola_roll_all}) %>%
                   dplyr::select(Date, Asset, contains('roll_vol_')) ")

    all_vol_data <- eval(parse(text = vola_roll_all_mutate))

    total_reg_data <-
      total_reg_data %>%
      left_join(all_vol_data %>%
                  ungroup() %>%
                  dplyr::select(Date, Asset, contains("roll_vol_"))
      )

    rm(all_vol_data)
    gc()

    bull_bear_data <-
      get_bull_bear_rolling(
        portfolio_data = portfolio_data,
        roll_periods = c(50,100,200)
      )

    total_reg_data <-
      total_reg_data %>%
      left_join(
        bull_bear_data %>%
          ungroup() %>%
          dplyr::select(Date, Asset, contains("cumulative"), contains("Bull"), contains("Bear"))
      )

    rm(bull_bear_data)
    gc()

    return(total_reg_data)

  }


