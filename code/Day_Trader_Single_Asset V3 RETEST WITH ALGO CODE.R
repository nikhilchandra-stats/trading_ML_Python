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
    profit_factor = 60,
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


#' get_control_trade_cumulative_returns
#'
#' @param generated_preds
#' @param trade_statement
#' @param trade_direction
#' @param return_col
#'
#' @returns
#' @export
#'
#' @examples
get_control_trade_cumulative_returns <-
  function(
    generated_preds = generated_preds,
    trade_statement = trade_statement,
    actual_wins_losses = actual_wins_losses,
    trade_direction = "Long",
    return_col = "period_return_50_Price"
  ) {

    control_data_asset <-
      generated_preds %>%
      left_join(
        actual_wins_losses %>%
          dplyr::select(Date, Asset, trade_col,  contains(return_col), volume_required) %>%
          filter(trade_col == trade_direction) %>%
          dplyr::select(-trade_col) %>%
          dplyr::select(Date, Asset,  contains(return_col), volume_required) %>%
          distinct()
      ) %>%
      distinct() %>%
      dplyr::select(Date, Asset, !!as.name(return_col), volume_required) %>%
      group_by(Asset) %>%
      arrange(Date, .by_group = TRUE) %>%
      group_by(Asset) %>%
      mutate(
        cumulative_return := cumsum(!!as.name(return_col))
      )

    trade_data <-
      generated_preds %>%
      mutate(
        trade_col =
          eval(parse(text = trade_statement)),
        trade_col =
          ifelse(trade_col == TRUE, trade_direction, paste0("No Trade ", trade_direction) )
      ) %>%
      left_join(
        actual_wins_losses %>%
          dplyr::select(Date, Asset, trade_col,  contains(return_col), volume_required) %>%
          filter(trade_col == trade_direction) %>%
          dplyr::select(-trade_col) %>%
          dplyr::select(Date, Asset,  contains(return_col), volume_required) %>%
          distinct()
      ) %>%
      filter(trade_col == trade_direction) %>%
      group_by(Asset) %>%
      arrange(Date, .by_group = TRUE) %>%
      group_by(Asset) %>%
      mutate(
        cumulative_return = cumsum(!!as.name(return_col))
      ) %>%
      dplyr::select(Date, Asset, trade_col,
                    !!as.name(return_col), cumulative_return, volume_required)

    return(
      list(
        "control_data_asset" = control_data_asset,
        "trade_data" = trade_data
      )
    )

  }

#' get_margin_details
#'
#' @param currency_conversion
#' @param actual_wins_losses
#' @param asset_infor
#'
#' @returns
#' @export
#'
#' @examples
get_margin_details <-
  function(
    currency_conversion = currency_conversion,
    actual_wins_losses = actual_wins_losses,
    asset_infor = asset_infor
  ) {

    margin_required <-
      actual_wins_losses %>%
      dplyr::select(Date, Asset, volume_required, Ask_Price) %>%
      mutate(ending_value = str_extract(Asset, "_[A-Z][A-Z][A-Z]"),
             ending_value = str_remove_all(ending_value, "_")
      ) %>%
      left_join(currency_conversion, by =c("ending_value" = "not_aud_asset")) %>%
      left_join(asset_infor %>% rename(Asset = name)) %>%
      mutate(
        minimumTradeSize_OG = as.numeric(minimumTradeSize),
        minimumTradeSize = abs(log10(as.numeric(minimumTradeSize))),
        marginRate = as.numeric(marginRate),
        pipLocation = as.numeric(pipLocation),
        displayPrecision = as.numeric(displayPrecision)
      ) %>%
      ungroup() %>%
      mutate(
        volume_adjustment = 1,
        AUD_Price =
          case_when(
            !is.na(adjusted_conversion) ~ (Ask_Price*adjusted_conversion)/volume_adjustment,
            TRUE ~ Ask_Price/volume_adjustment
          ),
        trade_value = AUD_Price*volume_required*marginRate,
        estimated_margin = trade_value
      ) %>%
      dplyr::select(Date, Asset, volume_required, estimated_margin)

    return(margin_required)

  }

get_total_portfolio_summary <-
  function(
    generated_preds = generated_preds,
    trade_statement = trade_statement,
    actual_wins_losses = actual_wins_losses,
    trade_direction = "Long",
    return_col = "period_return_50_Price"
  ) {

    sim_data_list <-
      get_control_trade_cumulative_returns(
        generated_preds = generated_preds,
        trade_statement = trade_statement,
        trade_direction = trade_direction,
        actual_wins_losses = actual_wins_losses,
        return_col = return_col
      )


    summarised_data <-
      sim_data_list %>%
      map(
        ~ .x %>%
          group_by(Date) %>%
          summarise(
            Total_Return = sum( !!as.name(return_col), na.rm= T)
          ) %>%
          arrange(Date) %>%
          mutate(
            Cumulative_Return = cumsum(Total_Return)
          )
      )

    summarised_data_combined <-
      summarised_data[[1]] %>%
      mutate(trade_col = "Control") %>%
      bind_rows(
        summarised_data[[2]] %>%
          mutate(trade_col = trade_direction)
      )

    return(summarised_data_combined)

  }

#' generate_random_sampling_returns
#'
#' @param timeseries_returns
#' @param simulations
#' @param samples
#' @param return_col
#'
#' @returns
#' @export
#'
#' @examples
generate_random_sampling_returns <-
  function(timeseries_returns = EUR_USD,
           simulations = 5000,
           samples = 100,
           return_col = "period_return_50_Price") {

    timeseries_returns <-
      timeseries_returns %>%
      mutate(
        win = ifelse(!!as.name(return_col) > 0, 1, 0)
      )

    asset_var <- timeseries_returns$Asset %>% unique() %>% as.character()

    returns_vec <- timeseries_returns %>% pull(!!as.name(return_col))
    win_loss_vec <- timeseries_returns %>% pull(win)

    wins_random <- numeric(simulations)
    returns_random <- numeric(simulations)
    average_win <- numeric(simulations)
    average_loss <- numeric(simulations)

    set.seed(simulations)

    for (i in 1:simulations) {

      wins_sampled <-
        win_loss_vec %>% sample(samples, replace = TRUE)
      returns_sampled <-
        returns_vec %>% sample(samples, replace = TRUE)

      wins_random[i] <- wins_sampled %>% sum(na.rm = T)
      returns_random[i] <- sum(returns_sampled, na.rm = T)
      average_win[i] <- returns_sampled[returns_sampled > 0] %>% mean(na.rm = T)
      average_loss[i] <- returns_sampled[returns_sampled <= 0] %>% mean(na.rm = T)
    }

    simulation_data_frame_random <-
      tibble(
        Asset = asset_var,
        samples_used = samples,
        wins_random = wins_random,
        returns_random = returns_random,
        average_win = average_win,
        average_loss = average_loss
      ) %>%
      mutate(
        win_perc =wins_random/samples
      ) %>%
      group_by(Asset, samples_used) %>%
      summarise(
        across(.cols =
                 c(wins_random, win_perc, average_win, average_loss),
               .fns = ~ mean(., na.rm = T)
        ),

        returns_random_mean = mean(returns_random, na.rm = T),
        returns_random_05 = quantile(returns_random, 0.05, na.rm = T),
        returns_random_25 = quantile(returns_random, 0.25, na.rm = T),
        returns_random_50 = quantile(returns_random, 0.5, na.rm = T),
        returns_random_75 = quantile(returns_random, 0.75, na.rm = T)
      )

    return(simulation_data_frame_random)

  }

#' get_asset_random_sim_returns
#'
#' @param generated_preds
#' @param trade_statement
#' @param trade_direction
#' @param return_col
#' @param simulations
#' @param samples
#'
#' @returns
#' @export
#'
#' @examples
get_asset_random_sim_returns <-
  function(
    generated_preds = generated_preds,
    trade_statement = trade_statement,
    actual_wins_losses = actual_wins_losses,
    trade_direction = "Long",
    return_col = "period_return_50_Price",
    simulations = 5000,
    samples = 20
  ) {

    sim_data_list <-
      get_control_trade_cumulative_returns(
        generated_preds = generated_preds,
        trade_statement = trade_statement,
        actual_wins_losses = actual_wins_losses,
        trade_direction = trade_direction,
        return_col = return_col
      )

    safely_sample <-
      safely(generate_random_sampling_returns, otherwise = NULL)

    sim_data_asset_all <-
      sim_data_list[[2]] %>%
      split(.$Asset, drop = FALSE) %>%
      map(
        ~ .x %>%
          safely_sample(
            simulations = simulations,
            samples = samples,
            return_col = return_col
          ) %>%
          pluck('result')
      )

    complete_summaries <-
      sim_data_list[[2]] %>%
      mutate(
        win = ifelse(!!as.name(return_col) > 0, 1, 0)
      ) %>%
      group_by(Asset) %>%
      summarise(
        Total_returns = sum(!!as.name(return_col), na.rm = T),
        Total_wins = sum(win, na.rm = T),
        Total_Trades = n(),
        Total_Perc = Total_wins/Total_Trades
      )

    sim_data_asset_all_dfr <-
      sim_data_asset_all %>%
      keep(~ !is.null(.x)) %>%
      map_dfr(bind_rows) %>%
      left_join(complete_summaries)

    return(sim_data_asset_all_dfr)

  }

trade_statement <-
  "
  (state_space_GLM_Pred_period_return_50_Price > 0.97 &
  state_space_GLM_Pred_period_return_50_Price < 0.99 &
      Asset == 'WHEAT_USD')|
  (state_space_LM_Pred_period_return_50_Price > 2.75 &
  state_space_LM_Pred_period_return_50_Price < 5.5 &
      Asset == 'WHEAT_USD')|
  (
  AR_GLM_Pred_period_return_50_Price > 0.475 &
  AR_GLM_Pred_period_return_50_Price < 0.9999999999 &
      Asset == 'WHEAT_USD'
  )|
  (
  AR_LM_Pred_period_return_50_Price > -0.1 &
  AR_LM_Pred_period_return_50_Price < 5 &
      Asset == 'WHEAT_USD'
  )|

  (state_space_GLM_Pred_period_return_50_Price < 0.075 &
  state_space_GLM_Pred_period_return_50_Price > 0 &
      Asset == 'SUGAR_USD')|

  (state_space_LM_Pred_period_return_50_Price < -8 &
  state_space_LM_Pred_period_return_50_Price > -500 &
      Asset == 'SUGAR_USD')|

  (
  AR_GLM_Pred_period_return_50_Price > 0.45 &
  AR_GLM_Pred_period_return_50_Price < 0.49 &
      Asset == 'SUGAR_USD'
  )|

  (
  AR_LM_Pred_period_return_50_Price > 0.25 &
  AR_LM_Pred_period_return_50_Price < 500 &
      Asset == 'SUGAR_USD'
  )|

  (state_space_GLM_Pred_period_return_50_Price > 0.71 &
  state_space_GLM_Pred_period_return_50_Price < 0.96 &
      Asset == 'DE30_EUR')|

  (
  state_space_LM_Pred_period_return_50_Price > 1 &
  state_space_LM_Pred_period_return_50_Price < 4.25 &
      Asset == 'DE30_EUR'
  )|

  (
  AR_GLM_Pred_period_return_50_Price > 0.61 &
  AR_GLM_Pred_period_return_50_Price < 0.64 &
      Asset == 'DE30_EUR'
  )|
  (
  AR_LM_Pred_period_return_50_Price > 0 &
  AR_LM_Pred_period_return_50_Price < 100 &
      Asset == 'DE30_EUR'
  )|

  (state_space_GLM_Pred_period_return_50_Price > 0.65 &
  state_space_GLM_Pred_period_return_50_Price < 0.7 &
      Asset == 'UK10YB_GBP')|

  (
  AR_LM_Pred_period_return_50_Price > 2.5 &
  AR_LM_Pred_period_return_50_Price < 100 &
      Asset == 'EUR_CHF'
  )|

  (state_space_LM_Pred_period_return_50_Price < -7.5  &
   state_space_LM_Pred_period_return_50_Price > -1000  &
        Asset == 'GBP_CHF')|
  (state_space_LM_Pred_period_return_50_Price > 0  &
   state_space_LM_Pred_period_return_50_Price < 0.1  &
        Asset == 'GBP_CHF')|
  (AR_LM_Pred_period_return_50_Price > 0.65  &
        Asset == 'GBP_CHF')|
  (AR_GLM_Pred_period_return_50_Price >
      AR_GLM_Pred_period_return_50_Price_mean + 1.4*AR_GLM_Pred_period_return_50_Price_sd &
  AR_GLM_Pred_period_return_50_Price <
      AR_GLM_Pred_period_return_50_Price_mean + 1.75*AR_GLM_Pred_period_return_50_Price_sd &
        Asset == 'GBP_CHF')|

  (state_space_GLM_Pred_period_return_50_Price < 0.085 &
    state_space_GLM_Pred_period_return_50_Price > 0.04 &
        Asset == 'USD_CZK')|
  (state_space_GLM_Pred_period_return_50_Price >
      state_space_GLM_Pred_period_return_50_Price_mean + 2.25*state_space_GLM_Pred_period_return_50_Price_sd &
        Asset == 'USD_CZK')|
  (state_space_LM_Pred_period_return_50_Price >
      state_space_LM_Pred_period_return_50_Price_mean + 1.25*state_space_LM_Pred_period_return_50_Price_sd &
  state_space_LM_Pred_period_return_50_Price <
      state_space_LM_Pred_period_return_50_Price_mean + 1.5*state_space_LM_Pred_period_return_50_Price_sd &
        Asset == 'USD_CZK')|
    (
    AR_LM_Pred_period_return_50_Price > 0 &
    AR_LM_Pred_period_return_50_Price < 20 &
        Asset == 'USD_CZK'
    )|
    (
    AR_LM_Pred_period_return_50_Price >
      AR_LM_Pred_period_return_50_Price_mean + 2*AR_LM_Pred_period_return_50_Price_sd &
    AR_LM_Pred_period_return_50_Price <
      AR_LM_Pred_period_return_50_Price_mean + 2.5*AR_LM_Pred_period_return_50_Price_sd &
        Asset == 'USD_CZK'
    )|

    (state_space_GLM_Pred_period_return_50_Price >
      state_space_GLM_Pred_period_return_50_Price_mean + 1.2*state_space_GLM_Pred_period_return_50_Price_sd &
    state_space_GLM_Pred_period_return_50_Price <
      state_space_GLM_Pred_period_return_50_Price_mean + 1.5*state_space_GLM_Pred_period_return_50_Price_sd &
    state_space_GLM_Pred_period_return_50_Price > 0.5 &
        Asset == 'USD_NOK')|
    (state_space_GLM_Pred_period_return_50_Price >
      state_space_GLM_Pred_period_return_50_Price_mean + 1.85*state_space_GLM_Pred_period_return_50_Price_sd &
    state_space_GLM_Pred_period_return_50_Price <
      state_space_GLM_Pred_period_return_50_Price_mean + 2.4*state_space_GLM_Pred_period_return_50_Price_sd &
    state_space_GLM_Pred_period_return_50_Price > 0.5 &
        Asset == 'USD_NOK')|
    (
    state_space_GLM_Pred_period_return_50_Price < 0.05 &
    state_space_GLM_Pred_period_return_50_Price > 0.00 &
    Asset == 'GBP_NZD'
    )|
    (
    state_space_LM_Pred_period_return_50_Price < -7.5 &
    state_space_LM_Pred_period_return_50_Price > -1000 &
    Asset == 'GBP_NZD'
    )|
    (
    AR_LM_Pred_period_return_50_Price > 0.4 &
    AR_LM_Pred_period_return_50_Price < 0.95 &
    Asset == 'GBP_NZD'
    )|
    (
    AR_GLM_Pred_period_return_50_Price > 0.5 &
    AR_GLM_Pred_period_return_50_Price < 1 &
    Asset == 'GBP_NZD'
    )|
  (
  AR_LM_Pred_period_return_50_Price > 1 &
  AR_LM_Pred_period_return_50_Price < 1000 &
  Asset == 'NZD_CHF'
  )|
  (
  AR_GLM_Pred_period_return_50_Price > 0.565 &
  AR_GLM_Pred_period_return_50_Price < 1 &
  Asset == 'NZD_CHF'
  )|
  (
  state_space_LM_Pred_period_return_50_Price > 1.6 &
  state_space_LM_Pred_period_return_50_Price < 2.1 &
  Asset == 'USD_MXN'
  )|
  (
  state_space_GLM_Pred_period_return_50_Price > 0.57 &
  state_space_GLM_Pred_period_return_50_Price < 0.61 &
  Asset == 'USD_MXN'
  )|
  (
  AR_GLM_Pred_period_return_50_Price > 0.6 &
  AR_GLM_Pred_period_return_50_Price < 0.7 &
  Asset == 'USD_MXN'
  )|
  (
  AR_LM_Pred_period_return_50_Price > 0.7 &
  AR_LM_Pred_period_return_50_Price < 0.9 &
  Asset == 'USD_MXN'
  )|
  (
  state_space_LM_Pred_period_return_50_Price > 39 &
  state_space_LM_Pred_period_return_50_Price < 50 &
  Asset == 'CH20_CHF'
  )|
  (
  state_space_GLM_Pred_period_return_50_Price > 0.675 &
  state_space_GLM_Pred_period_return_50_Price < 0.725 &
  Asset == 'CH20_CHF'
  )|
  (
  state_space_LM_Pred_period_return_50_Price > 39 &
  state_space_LM_Pred_period_return_50_Price < 50 &
  Asset == 'XPT_USD'
  )|
  (
  state_space_GLM_Pred_period_return_50_Price > 0.8 &
  state_space_GLM_Pred_period_return_50_Price < 0.85 &
  Asset == 'XPT_USD'
  )|
  (
  state_space_GLM_Pred_period_return_50_Price > 0.5 &
  state_space_GLM_Pred_period_return_50_Price < 0.54 &
  Asset == 'SOYBN_USD'
  )|
  (
  AR_LM_Pred_period_return_50_Price > 0.25 &
  AR_LM_Pred_period_return_50_Price < 1 &
  Asset == 'SOYBN_USD'
  )|
  (
  AR_GLM_Pred_period_return_50_Price > 0.61 &
  AR_GLM_Pred_period_return_50_Price < 0.75 &
  Asset == 'SOYBN_USD'
  )|
  (
  state_space_GLM_Pred_period_return_50_Price > 0.665 &
  state_space_GLM_Pred_period_return_50_Price < 0.7 &
  Asset == 'JP225_USD'
  )|
  (
  state_space_LM_Pred_period_return_50_Price > 0.25 &
  state_space_LM_Pred_period_return_50_Price < 0.86 &
  Asset == 'JP225_USD'
  )|
  (
  AR_LM_Pred_period_return_50_Price > 0.5 &
  AR_LM_Pred_period_return_50_Price < 1.3 &
  Asset == 'JP225_USD'
  )|
  (
  AR_GLM_Pred_period_return_50_Price > 0.55 &
  AR_GLM_Pred_period_return_50_Price < 1 &
  Asset == 'JP225_USD'
  )

  "

cumulative_returns_sim_data <-
  get_total_portfolio_summary(
    generated_preds = generated_preds_from_db %>%
      # filter(Asset == "WHEAT_USD")
      filter(str_detect(Asset, "[A-Z]"))
    ,
    trade_statement = trade_statement,
    actual_wins_losses =
      actual_wins_losses %>%
      # filter(Asset == "WHEAT_USD")
      filter(str_detect(Asset, "[A-Z]"))
    ,
    trade_direction = "Long",
    return_col = "period_return_50_Price"
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
      # filter(Asset == "WHEAT_USD")
      filter(str_detect(Asset, "[A-Z]"))
    ,
    trade_statement = trade_statement,
    actual_wins_losses =
      actual_wins_losses %>%
      # filter(Asset == "WHEAT_USD")
      filter(str_detect(Asset, "[A-Z]"))
    ,
    trade_direction = "Long",
    return_col = "period_return_50_Price",
    simulations = 5000,
    samples = 50
  )

asset_summaries_control <-
  get_asset_random_sim_returns(
    generated_preds =
      generated_preds_from_db %>%
      # filter(Asset == "WHEAT_USD")
      filter(str_detect(Asset, "[A-Z]"))
    ,
    trade_statement = "str_detect(Asset, '[A-Z]')",
    actual_wins_losses =
      actual_wins_losses %>%
      # filter(Asset == "WHEAT_USD")
      filter(str_detect(Asset, "[A-Z]"))
    ,
    trade_direction = "Long",
    return_col = "period_return_50_Price",
    simulations = 5000,
    samples = 50
  )

distinct_stops_profs <-
  actual_wins_losses %>%
  distinct(Asset, stop_return, profit_return)

traded_assets <-
  c(
    "WHEAT_USD", #1 WHEAT_USD
    "SUGAR_USD", #2 SUGAR_USD
    "DE30_EUR", #3 DE30_EUR
    "UK10YB_GBP", #4 UK10YB_GBP
    "EUR_CHF", #5 EUR_CHF
    # "EUR_SEK", #6 EUR_SEK
    "GBP_CHF", #7 GBP_CHF
    # "GBP_JPY", #8 GBP_JPY
    "USD_CZK",  #9 USD_CZK
    "USD_NOK", #10 USD_NOK
    # "XAG_CAD",  #11 XAG_CAD
    # "XAG_CHF",  #12 XAG_CHF
    # "XAG_JPY",   #13 XAG_JPY
    "GBP_NZD", #14 GBP_NZD
    "NZD_CHF", #15 NZD_CHF
    "USD_MXN",  #16 USD_MXN
    "CH20_CHF", #17 CH20_CHF
    "XPT_USD", #18
    "SOYBN_USD", #19
    "JP225_USD", #20
    "XPD_USD", #21
    "NL25_EUR" #22
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

portfolio_structure <-
  portfolio_structure %>%
  map_dfr(bind_rows)

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
