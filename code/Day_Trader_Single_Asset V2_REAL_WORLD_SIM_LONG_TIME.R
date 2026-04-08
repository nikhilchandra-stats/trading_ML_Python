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
start_date = "2013-01-01"
end_date = today() %>% as.character()

bin_factor = NULL
stop_value_var = 10
profit_value_var = 50
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

single_asset_algo_generate_models(
  All_Daily_Data = All_Daily_Data,
  Indices_Metals_Bonds = Indices_Metals_Bonds,
  raw_macro_data = raw_macro_data,
  currency_conversion = currency_conversion,
  asset_infor = asset_infor,
  # start_index = 1,
  start_index = 1,
  end_index = 38,
  risk_dollar_value = 15,
  trade_direction = "Long",
  stop_value_var = 10,
  profit_value_var = 60,
  period_var = 24,
  bin_var_col = c("period_return_24_Price", "period_return_40_Price", "period_return_50_Price"),
  # date_train_end_pre = as.character(as_date("2023-06-01") + days(24) ),
  # date_train_phase_2_end_pre = as.character(as_date("2024-06-01") + days(24) ),
  # training_date_start_post = as.character(as_date("2024-07-04") + days(24) ),
  # training_date_end_post = as.character(as_date("2025-09-01") + days(24) ),
  # test_end_date = as.character(today()),

  date_train_end_pre = as.character(as_date("2021-01-01")  ),
  date_train_phase_2_end_pre = as.character(as_date("2022-01-01")  ),
  training_date_start_post = as.character(as_date("2022-01-01")  ),
  training_date_end_post = as.character(as_date("2023-01-01")  ),
  test_end_date = as.character(today()),

  post_bins_cols =
    c("period_return_24_Price", "period_return_40_Price", "period_return_50_Price"),
  post_dependant_threshold = 0,
  model_data_store_path = "C:/Users/nikhi/Documents/trade_data/Day_Trader_Single_Asset_V2_trade_store_stop_2.db",
  save_path = "C:/Users/nikhi/Documents/trade_data/Day_Trader_Single_Asset_V2_trade_store_stop_2"
)

Cut_Down_Data <-
  Indices_Metals_Bonds %>%
  map(~ .x %>% filter(Date >= "2019-01-01"))

post_preds_all_rolling_and_originals <-
  single_asset_algo_generate_preds(
    All_Daily_Data = All_Daily_Data,
    Indices_Metals_Bonds = Cut_Down_Data,
    raw_macro_data = raw_macro_data,
    currency_conversion = currency_conversion,
    asset_infor = asset_infor,
    # start_index = 1,
    # end_index = 40,
    start_index = 1,
    end_index = 19,
    risk_dollar_value = 15,
    trade_direction = "Long",
    stop_value_var = 10,
    profit_value_var = 60,
    period_var = 24,
    bin_var_col = c("period_return_24_Price", "period_return_40_Price", "period_return_50_Price"),

    # date_train_end_pre = as.character(as_date("2023-06-01")  ),
    # date_train_phase_2_end_pre = as.character(as_date("2024-06-01")),
    # training_date_start_post = as.character(as_date("2024-07-04")),
    # training_date_end_post = as.character(as_date("2025-09-01")),
    # test_end_date = as.character(today()),

    date_train_end_pre = as.character(as_date("2021-01-01")  ),
    date_train_phase_2_end_pre = as.character(as_date("2022-01-01")  ),
    training_date_start_post = as.character(as_date("2022-01-01")  ),
    training_date_end_post = as.character(as_date("2023-01-01")  ),
    test_end_date = as.character(today()),

    post_dependant_var = "period_return_50_Price",
    post_bins_cols =
      c("period_return_24_Price", "period_return_40_Price", "period_return_50_Price"),
    post_dependant_threshold = 0,
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
    stop_factor = 10,
    profit_factor = 60,
    risk_dollar_value = 10,
    trade_direction = "Long",
    currency_conversion = currency_conversion,
    asset_infor = asset_infor,
    periods_ahead = period_var
  )

post_preds_all_rolling_and_originals_2 <-
  post_preds_all_rolling_and_originals %>%
  mutate(
    Averaged_Multi_prob_Momentum =
      (pred_index_2 + pred_daily_2 + pred_technical_2  +
         pred_index_4 + pred_daily_4 + pred_technical_4  +
         pred_index_6 + pred_daily_6 + pred_technical_6  )/9,

    Averaged_Multi_prob_GLM =
      (pred_index_2 + pred_daily_2 + pred_technical_2 + pred_copula_2 +
         pred_index_4 + pred_daily_4 + pred_technical_4 + pred_copula_4 +
         pred_index_6 + pred_daily_6 + pred_technical_6 + pred_copula_6 )/12,

    Averaged_Multi_prob_macro_GLM =
      (pred_index_2 + pred_daily_2 + pred_technical_2 + pred_copula_2 + pred_macro_2 +
         pred_index_4 + pred_daily_4 + pred_technical_4 + pred_copula_4 +  pred_macro_4 +
         pred_index_6 + pred_daily_6 + pred_technical_6 + pred_copula_6 + pred_macro_6  )/15,

    Averaged_Multi_prob_Momentum_Marco =
      (pred_index_2 + pred_daily_2 + pred_technical_2  + pred_macro_2 +
         pred_index_4 + pred_daily_4 + pred_technical_4  +  pred_macro_4 +
         pred_index_6 + pred_daily_6 + pred_technical_6  + pred_macro_6  )/12,

    Averaged_FULL_GLM =
      (pred_index_2 + pred_daily_2 + pred_technical_2 + pred_copula_2  + pred_GLM_period_return_24_Price +
         pred_index_4 + pred_daily_4 + pred_technical_4 + pred_copula_4  + pred_GLM_period_return_40_Price +
         pred_index_6 + pred_daily_6 + pred_technical_6 + pred_copula_6 + pred_GLM_period_return_50_Price
      )/15,

    Averaged_FULL_LM =
      (pred_index_1 + pred_daily_1 + pred_technical_1 + pred_copula_1   +
         pred_index_3 + pred_daily_3 + pred_technical_3 + pred_copula_3   +
         pred_index_5 + pred_daily_5 + pred_technical_5 + pred_copula_5
      )/12

  )
generated_preds <-
  post_preds_all_rolling_and_originals_2 %>%
  mutate(
    across(.cols = c(Date),
           .fns = ~ as_datetime(., tz = "Australia/Canberra"))
  ) %>%
  filter(
    Date >= as.character(as_date("2023-01-01")  )
  )

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
  (
  pred_LM_period_return_24_Price >
            mean_50_pred_LM_period_return_24_Price + sd_500_pred_LM_period_return_24_Price*1.85 &
  Asset == 'EUR_USD'
  )|
  (
  pred_LM_period_return_40_Price >
            mean_50_pred_LM_period_return_40_Price + sd_500_pred_LM_period_return_40_Price*1.85 &
  Asset == 'EUR_USD'
  )|
  (
  pred_LM_period_return_50_Price >
            mean_50_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*1.85 &
  Asset == 'EUR_USD'
  )|
  (
  pred_technical_6 >= 0.675 &
  pred_technical_6 <= 0.7 &
  Asset == 'EUR_USD'
  )|
  (
  Averaged_Multi_prob_macro_GLM >= 0.625 &
  Averaged_Multi_prob_macro_GLM <= 0.64 &
  Asset == 'EUR_USD'
  )|
  (
  Averaged_Multi_prob_Momentum_Marco > 0.52 &
  Averaged_Multi_prob_Momentum_Marco < 0.55 &
  Asset == 'EUR_USD'
  )|
  (
  pred_GLM_period_return_50_Price > 0.56 &
  pred_GLM_period_return_50_Price < 0.57 &
  Asset == 'EUR_USD'
  )|
  (
 pred_GLM_period_return_50_Price > mean_50_pred_GLM_period_return_50_Price &
 pred_GLM_period_return_50_Price > mean_500_pred_GLM_period_return_50_Price &
 pred_GLM_period_return_50_Price > mean_200_pred_GLM_period_return_50_Price &
 pred_GLM_period_return_50_Price > mean_100_pred_GLM_period_return_50_Price &
 mean_200_pred_GLM_period_return_50_Price < mean_500_pred_GLM_period_return_50_Price &
 mean_100_pred_GLM_period_return_50_Price < mean_200_pred_GLM_period_return_50_Price &
 pred_LM_period_return_50_Price > mean_50_pred_LM_period_return_50_Price &
 pred_LM_period_return_50_Price > mean_500_pred_LM_period_return_50_Price &
 pred_LM_period_return_50_Price > mean_200_pred_LM_period_return_50_Price &
 pred_LM_period_return_50_Price > mean_100_pred_LM_period_return_50_Price &
 Asset == 'EUR_USD'
 )|
  (
  pred_LM_period_return_50_Price >
            mean_50_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*1.7 &
  pred_LM_period_return_50_Price <
            mean_50_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*10 &
  Asset == 'EUR_USD'
  )|
  (
  pred_LM_period_return_50_Price >
            mean_200_pred_LM_period_return_50_Price + sd_200_pred_LM_period_return_50_Price*2 &
  pred_LM_period_return_50_Price <
            mean_200_pred_LM_period_return_50_Price + sd_200_pred_LM_period_return_50_Price*10 &
  Asset == 'EUR_USD'
  )|
  (
  pred_LM_period_return_50_Price >
            mean_500_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*1.85 &
  pred_LM_period_return_50_Price <
            mean_500_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*10 &
  Asset == 'EUR_USD'
  )|
  (
  pred_GLM_period_return_50_Price > 0.52 &
  pred_GLM_period_return_50_Price < 0.6 &
  Asset == 'EU50_EUR'
  )|
  (
  pred_GLM_period_return_40_Price > 0.54 &
  pred_GLM_period_return_40_Price < 0.59 &
  Asset == 'EU50_EUR'
  )|
  (
  pred_LM_period_return_50_Price > 4 &
  pred_LM_period_return_50_Price < 6 &
  Asset == 'EU50_EUR'
  )|
  (
  pred_combined_6 >= 0.99999999999 &
  pred_combined_6 <= 1 &
  Asset == 'EU50_EUR'
  )|
  (
  pred_combined_4 >= 0.9999 &
  pred_combined_4 <= 1 &
  Asset == 'EU50_EUR'
  )|
  (
  pred_index_6 >= 0.545 &
  pred_index_6 <= 0.57 &
  Asset == 'EU50_EUR'
  )|
  (
  pred_GLM_period_return_50_Price > 0.5 &
  pred_GLM_period_return_50_Price < 0.55 &
  Asset == 'SPX500_USD'
  )|
  (
  pred_GLM_period_return_50_Price > 0.6 &
  pred_GLM_period_return_50_Price < 0.62 &
  Asset == 'SPX500_USD'
  )|
  (
  pred_LM_period_return_50_Price >
            mean_50_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*1.75 &
  Asset == 'SPX500_USD'
  )|
  (
  pred_LM_period_return_40_Price >
            mean_50_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_40_Price*1.25 &
  pred_LM_period_return_40_Price <
            mean_50_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_40_Price*1.75 &
  Asset == 'SPX500_USD'
  )|
  (
  pred_GLM_period_return_50_Price >
            mean_50_pred_GLM_period_return_50_Price + sd_500_pred_GLM_period_return_50_Price*1.65 &
  Asset == 'SPX500_USD'
  )|
  (
  pred_combined_6 >= 0.99 &
  Averaged_Multi_prob_Momentum_Marco > 0.5 &
  Averaged_Multi_prob_Momentum_Marco < 0.8 &
  Asset == 'SPX500_USD'
  )|
  (
  pred_combined_6 >= 0.99 &
  Averaged_FULL_GLM > 0.5 &
  Averaged_FULL_GLM < 0.55 &
  Asset == 'SPX500_USD'
  )|
  (
  pred_GLM_period_return_50_Price > mean_50_pred_GLM_period_return_50_Price &
  pred_GLM_period_return_50_Price > mean_500_pred_GLM_period_return_50_Price &
  pred_GLM_period_return_50_Price > mean_200_pred_GLM_period_return_50_Price &
  pred_GLM_period_return_50_Price > mean_100_pred_GLM_period_return_50_Price &
  mean_100_pred_GLM_period_return_50_Price < mean_500_pred_GLM_period_return_50_Price &
  mean_200_pred_GLM_period_return_50_Price < mean_500_pred_GLM_period_return_50_Price &
  mean_100_pred_GLM_period_return_50_Price < mean_200_pred_GLM_period_return_50_Price &

  pred_LM_period_return_50_Price > mean_50_pred_LM_period_return_50_Price &
  pred_LM_period_return_50_Price > mean_500_pred_LM_period_return_50_Price &
  pred_LM_period_return_50_Price > mean_200_pred_LM_period_return_50_Price &
  pred_LM_period_return_50_Price > mean_100_pred_LM_period_return_50_Price &
  Asset == 'SPX500_USD'
  )|

  (
  pred_GLM_period_return_50_Price > 0.95 &
  pred_GLM_period_return_50_Price < 0.99 &
  Asset == 'US2000_USD'
  )|
  (
  pred_LM_period_return_50_Price >
            mean_50_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*1.15 &
  pred_LM_period_return_50_Price <
            mean_50_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*1.75 &
  Asset == 'US2000_USD'
  )|
  (
   Averaged_FULL_LM >= 95 &
   Averaged_FULL_LM <= 1000 &
   Asset == 'US2000_USD'
  )|
  (
  pred_daily_6 > 0.99999 &
  pred_index_6 > 0.99995 &
  pred_technical_6 > 0.75 &
  Asset == 'US2000_USD'
  )|
  (
  pred_combined_6 >= 0.9999999999999995 &
  Asset == 'US2000_USD'
  )|

  (
  pred_GLM_period_return_50_Price > 0.85 &
  pred_GLM_period_return_50_Price < 0.9 &
  Asset == 'USB10Y_USD'
  )|
  (
  pred_technical_6 >= 0.75 &
  pred_technical_6 <= 0.86 &
  Asset == 'USB10Y_USD'
  )|

  (
  pred_LM_period_return_50_Price >
            mean_50_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*1.25 &
  Asset == 'USD_JPY'
  )|
  (
  pred_GLM_period_return_50_Price >= 0.6 &
  pred_GLM_period_return_50_Price < 0.675 &
  Asset == 'USD_JPY'
  )|
  (
  pred_GLM_period_return_40_Price >= 0.61 &
  pred_GLM_period_return_40_Price < 0.65 &
  Asset == 'USD_JPY'
  )|
  (
  pred_technical_6 >= 0.575 &
  pred_technical_6 <= 1 &
  Asset == 'USD_JPY'
  )|
  (
  pred_combined_6 >= 0.9 &
  pred_combined_6 <= 0.95 &
  Asset == 'USD_JPY'
  )|
  (
  pred_combined_4 >= 0.65 &
  pred_combined_4 <= 1 &
  Asset == 'USD_JPY'
  )|
  (
  pred_index_5 >= 0.25 &
  pred_index_5 <= 0.9 &
  Asset == 'USD_JPY'
  )|

  (
  pred_LM_period_return_50_Price <
            mean_500_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*1.65 &
  pred_LM_period_return_50_Price >
            mean_500_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*1.1 &
  Asset == 'AUD_USD'
  )|
  (
  pred_GLM_period_return_40_Price >= 0.999997 &
  pred_GLM_period_return_40_Price <= 0.999999 &
  Asset == 'AUD_USD'
  )|
  (
  pred_technical_6 >= 0.8 &
  pred_technical_6 <= 1 &
  Asset == 'AUD_USD'
  )|
  (
  Averaged_Multi_prob_macro_GLM >= 0.55 &
  Averaged_Multi_prob_macro_GLM <= 0.6 &
  Asset == 'AUD_USD'
  )|
  (
  pred_index_6 > 0.7 &
  pred_index_6 < 1 &
  Asset == 'AUD_USD'
  )|
  (
  pred_index_4 > 0.99 &
  pred_index_4 < 1 &
  Asset == 'AUD_USD'
  )|
  (
  pred_daily_6 > 0.65 &
  pred_daily_6 < 0.75 &
  Asset == 'AUD_USD'
  )|
  (
  pred_daily_4 > 0.85 &
  pred_daily_4 < 1 &
  Asset == 'AUD_USD'
  )|


 (
 pred_GLM_period_return_50_Price >= 0.99 &
 pred_GLM_period_return_50_Price < 0.999 &
 Asset == 'EUR_GBP'
 )|
 (
 pred_combined_6 >= 0.99999999999 &
 pred_combined_6 <= 1 &
 Asset == 'EUR_GBP'
 )|
 (
 pred_technical_6 >= 0.7 &
 pred_technical_6 <= 1 &
 Asset == 'EUR_GBP'
 )|
 (
 pred_technical_4 >= 0.675 &
 pred_technical_4 <= 1 &
 Asset == 'EUR_GBP'
 )|
 (
 pred_technical_5 >= 7.5 &
 pred_technical_5 <= 1000 &
 Asset == 'EUR_GBP'
 )|
 (
 pred_technical_3 >= 6 &
 pred_technical_3 <= 1000 &
 Asset == 'EUR_GBP'
 )|
 (
 pred_daily_6 >= 0.925 &
 pred_daily_6 < 1 &
 Asset == 'EUR_GBP'
 )|
 (
 pred_daily_4 >= 0.925 &
 pred_daily_4 < 1 &
 Asset == 'EUR_GBP'
 )|
 (
 pred_daily_2 >= 0.9125 &
 pred_daily_2 < 1 &
 Asset == 'EUR_GBP'
 )|
 (
  pred_LM_period_return_50_Price >
            mean_50_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*2 &
  Asset == 'EUR_GBP'
  )|
 (
  mean_50_pred_GLM_period_return_50_Price >
    mean_100_pred_GLM_period_return_50_Price + 0.485*sd_100_pred_GLM_period_return_50_Price &
  mean_50_pred_GLM_period_return_50_Price <
    mean_100_pred_GLM_period_return_50_Price + 0.7*sd_100_pred_GLM_period_return_50_Price &
  mean_3_pred_GLM_period_return_50_Price > mean_200_pred_GLM_period_return_50_Price &
  mean_3_pred_GLM_period_return_50_Price > mean_100_pred_GLM_period_return_50_Price &
  Asset == 'EUR_GBP'
  )|
 (
  mean_3_pred_GLM_period_return_50_Price < mean_200_pred_GLM_period_return_50_Price &
  mean_3_pred_GLM_period_return_50_Price > mean_100_pred_GLM_period_return_50_Price &
  mean_200_pred_GLM_period_return_50_Price < mean_500_pred_GLM_period_return_50_Price &
  mean_100_pred_GLM_period_return_50_Price < mean_500_pred_GLM_period_return_50_Price &
  Asset == 'EUR_GBP'
 )|
  (
  pred_combined_6 >= 0.5 &
  pred_combined_6 <= 0.9999 &
  pred_combined_4 >= 0.5 &
  pred_GLM_period_return_50_Price > mean_100_pred_GLM_period_return_50_Price &
  Asset == 'EUR_GBP'
  )|
 (
 pred_technical_6 >= 0.7 &
 pred_technical_6 < 1 &
 pred_technical_4 >= 0.65 &
 pred_technical_4 < 1 &
 pred_technical_2 >= 0.65 &
 pred_technical_2 < 1 &
 Asset == 'EUR_GBP'
 )|
   (
  pred_LM_period_return_50_Price >
            mean_50_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*0.25 &
  pred_LM_period_return_50_Price <
            mean_50_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*0.4 &
  Asset == 'AU200_AUD'
  )|
 (
  mean_50_pred_GLM_period_return_50_Price >
    mean_100_pred_GLM_period_return_50_Price + 0.59*sd_100_pred_GLM_period_return_50_Price &
  mean_50_pred_GLM_period_return_50_Price <
    mean_100_pred_GLM_period_return_50_Price + 0.7*sd_100_pred_GLM_period_return_50_Price &
  mean_3_pred_GLM_period_return_50_Price > mean_200_pred_GLM_period_return_50_Price &
  Asset == 'AU200_AUD'
  )|
  (
  pred_combined_6 >= 0.5 &
  pred_combined_6 <= 0.9999 &
  pred_combined_4 >= 0.5 &
  pred_GLM_period_return_50_Price > mean_100_pred_GLM_period_return_50_Price &
  Asset == 'AU200_AUD'
  )|
  (
  pred_technical_6 >= 0.6 &
  pred_technical_6 < 0.85 &
  pred_technical_4 >= 0.6 &
  pred_technical_4 < 0.725 &
  Asset == 'AU200_AUD'
  )|

  (
  pred_combined_6 >= 0.5 &
  pred_GLM_period_return_50_Price > 0.5 &
  pred_GLM_period_return_50_Price < 0.6 &
  pred_daily_6 > 0.5 &
  pred_index_6 > 0.5 &
  Asset == 'EUR_AUD'
  )|
 (
   pred_LM_period_return_50_Price >
            mean_500_pred_LM_period_return_50_Price + 0*sd_500_pred_LM_period_return_50_Price &
  pred_LM_period_return_50_Price <
            mean_500_pred_LM_period_return_50_Price + 0.3*sd_500_pred_LM_period_return_50_Price &
  mean_3_pred_LM_period_return_50_Price > mean_200_pred_LM_period_return_50_Price &
  mean_3_pred_LM_period_return_50_Price > mean_100_pred_LM_period_return_50_Price &
  Asset == 'EUR_AUD'
  )|
 (
  pred_GLM_period_return_50_Price >
    mean_100_pred_GLM_period_return_50_Price + 2.1*sd_100_pred_GLM_period_return_50_Price &
  pred_GLM_period_return_50_Price <
    mean_100_pred_GLM_period_return_50_Price + 10*sd_100_pred_GLM_period_return_50_Price &
  mean_3_pred_GLM_period_return_50_Price > mean_200_pred_GLM_period_return_50_Price &
  mean_3_pred_GLM_period_return_50_Price > mean_100_pred_GLM_period_return_50_Price &
  Asset == 'EUR_AUD'
  )|
 (
 pred_technical_6 >= 0.6 &
 pred_technical_6 < 0.8 &
 pred_technical_4 >= 0.55 &
 pred_technical_4 < 0.65 &
 Asset == 'EUR_AUD'
 )|
 (
 pred_GLM_period_return_50_Price > mean_50_pred_GLM_period_return_50_Price &
 pred_GLM_period_return_50_Price > mean_500_pred_GLM_period_return_50_Price &
 pred_GLM_period_return_50_Price > mean_200_pred_GLM_period_return_50_Price &
 pred_GLM_period_return_50_Price > mean_100_pred_GLM_period_return_50_Price &
 mean_100_pred_GLM_period_return_50_Price < mean_500_pred_GLM_period_return_50_Price &
 mean_200_pred_GLM_period_return_50_Price < mean_500_pred_GLM_period_return_50_Price &
 mean_100_pred_GLM_period_return_50_Price < mean_200_pred_GLM_period_return_50_Price &
 pred_LM_period_return_50_Price > mean_50_pred_LM_period_return_50_Price &
 pred_LM_period_return_50_Price > mean_500_pred_LM_period_return_50_Price &
 pred_LM_period_return_50_Price > mean_200_pred_LM_period_return_50_Price &
 pred_LM_period_return_50_Price > mean_100_pred_LM_period_return_50_Price &
 Asset == 'EUR_AUD'
 )|
  (
  pred_combined_6 >= 0.95 &
  pred_daily_6 > 0.95 &
  pred_index_6 > 0.95 &
  Asset == 'WTICO_USD'
  )|
 (
  pred_GLM_period_return_50_Price >
    mean_100_pred_GLM_period_return_50_Price + 1.25*sd_100_pred_GLM_period_return_50_Price &
  pred_GLM_period_return_50_Price <
    mean_100_pred_GLM_period_return_50_Price + 1.5*sd_100_pred_GLM_period_return_50_Price &
  mean_3_pred_GLM_period_return_50_Price > mean_200_pred_GLM_period_return_50_Price &
  mean_3_pred_GLM_period_return_50_Price > mean_100_pred_GLM_period_return_50_Price &
  Asset == 'WTICO_USD'
  )|
 (
 pred_technical_6 >= 0.52 &
 pred_technical_6 < 0.65 &
 pred_technical_4 >= 0.52 &
 pred_technical_4 < 0.6 &
 Asset == 'WTICO_USD'
 )|
 (
 pred_GLM_period_return_50_Price > mean_50_pred_GLM_period_return_50_Price &
 pred_GLM_period_return_50_Price > mean_500_pred_GLM_period_return_50_Price &
 pred_GLM_period_return_50_Price > mean_200_pred_GLM_period_return_50_Price &
 pred_GLM_period_return_50_Price > mean_100_pred_GLM_period_return_50_Price &
 mean_100_pred_GLM_period_return_50_Price < mean_500_pred_GLM_period_return_50_Price &
 mean_200_pred_GLM_period_return_50_Price < mean_500_pred_GLM_period_return_50_Price &
 mean_100_pred_GLM_period_return_50_Price < mean_200_pred_GLM_period_return_50_Price &
 Asset == 'WTICO_USD'
 )|
  (
  pred_LM_period_return_50_Price >
            mean_50_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*0.75 &
  pred_LM_period_return_50_Price <
            mean_50_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*0.9 &
  Asset == 'UK100_GBP'
  )|
 (
  pred_GLM_period_return_50_Price >= 0.999 &
  Asset == 'UK100_GBP'
  )|
 (
 pred_technical_6 >= 0.6 &
 pred_technical_6 < 0.65 &
 Asset == 'UK100_GBP'
 )|
  (
  pred_combined_6 >= 0.5 &
  pred_GLM_period_return_50_Price > 0.5 &
  pred_daily_6 > 0.5 &
  pred_index_6 > 0.5 &
  Asset == 'UK100_GBP'
  )|
 (
 pred_GLM_period_return_50_Price > mean_50_pred_GLM_period_return_50_Price &
 pred_GLM_period_return_50_Price > mean_500_pred_GLM_period_return_50_Price &
 pred_GLM_period_return_50_Price > mean_200_pred_GLM_period_return_50_Price &
 pred_GLM_period_return_50_Price > mean_100_pred_GLM_period_return_50_Price &
 mean_100_pred_GLM_period_return_50_Price < mean_500_pred_GLM_period_return_50_Price &
 mean_100_pred_GLM_period_return_50_Price < mean_200_pred_GLM_period_return_50_Price &
 pred_LM_period_return_50_Price > mean_50_pred_LM_period_return_50_Price &
 pred_LM_period_return_50_Price > mean_500_pred_LM_period_return_50_Price &
 pred_LM_period_return_50_Price > mean_200_pred_LM_period_return_50_Price &
 pred_LM_period_return_50_Price > mean_100_pred_LM_period_return_50_Price &
 Asset == 'UK100_GBP'
 )|

  (
  pred_LM_period_return_50_Price >
            mean_500_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*1.5 &
  pred_LM_period_return_50_Price <
            mean_500_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*10 &
  Asset == 'USD_CAD'
  )|
  (
  pred_LM_period_return_50_Price >
            mean_200_pred_LM_period_return_50_Price + sd_200_pred_LM_period_return_50_Price*1.75 &
  pred_LM_period_return_50_Price <
            mean_200_pred_LM_period_return_50_Price + sd_200_pred_LM_period_return_50_Price*10 &
  Asset == 'USD_CAD'
  )|
 (
  pred_LM_period_return_50_Price >
            mean_50_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*1.15 &
  pred_LM_period_return_50_Price <
            mean_50_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*10 &
  Asset == 'USD_CAD'
  )|
 (
 pred_technical_6 >= 0.6125 &
 pred_technical_6 < 1 &
 pred_technical_4 >= 0.6125 &
 pred_technical_4 < 1 &
 Asset == 'USD_CAD'
 )|
  (
  pred_GLM_period_return_50_Price > 0.99 &
  pred_combined_6 > 0.99 &
  pred_daily_6 > 0.5 &
  Asset == 'USD_CAD'
  )|
  (
  pred_combined_6 >= 0.999 &
  pred_GLM_period_return_50_Price > 0.5 &
  Asset == 'USD_CAD'
  )|
    (
  pred_combined_2 >= 0.525 &
  pred_combined_2 < 0.575 &
  Asset == 'GBP_USD'
  )|
  (
  pred_combined_6 >= 0.6 &
  pred_combined_6 < 0.65 &
  Asset == 'GBP_USD'
  )|
  (
  pred_LM_period_return_50_Price >
            mean_500_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*0.55 &
  pred_LM_period_return_50_Price <
            mean_500_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*0.8 &
  Asset == 'GBP_USD'
  )|
  (
  pred_LM_period_return_50_Price >
            mean_200_pred_LM_period_return_50_Price + sd_200_pred_LM_period_return_50_Price*2.5 &
  pred_LM_period_return_50_Price <
            mean_200_pred_LM_period_return_50_Price + sd_200_pred_LM_period_return_50_Price*10 &
  Asset == 'GBP_USD'
  )|
  (
  pred_GLM_period_return_50_Price > 0.75 &
  pred_GLM_period_return_50_Price < 0.775 &
  Asset == 'GBP_USD'
  )|
 (
 pred_LM_period_return_50_Price > mean_50_pred_LM_period_return_50_Price &
 pred_LM_period_return_50_Price > mean_500_pred_LM_period_return_50_Price &
 pred_LM_period_return_50_Price > mean_200_pred_LM_period_return_50_Price &
 pred_LM_period_return_50_Price > mean_100_pred_LM_period_return_50_Price &
 mean_100_pred_LM_period_return_50_Price < mean_500_pred_LM_period_return_50_Price &
 pred_GLM_period_return_50_Price > mean_50_pred_GLM_period_return_50_Price &
 Asset == 'GBP_USD'
 )|
 (
 pred_LM_period_return_50_Price > mean_50_pred_LM_period_return_50_Price &
 pred_LM_period_return_50_Price > mean_500_pred_LM_period_return_50_Price &
 pred_LM_period_return_50_Price > mean_200_pred_LM_period_return_50_Price &
 pred_LM_period_return_50_Price > mean_100_pred_LM_period_return_50_Price &
 mean_50_pred_LM_period_return_50_Price < mean_200_pred_LM_period_return_50_Price &
 pred_GLM_period_return_50_Price > mean_50_pred_GLM_period_return_50_Price &
 Asset == 'GBP_USD'
 )

  "

trade_statement <-
"
 #  (
 #  pred_combined_2 >= 0.525 &
 #  pred_combined_2 < 0.575 &
 #  Asset == 'GBP_USD'
 #  )|
 #  (
 #  pred_combined_6 >= 0.6 &
 #  pred_combined_6 < 0.65 &
 #  Asset == 'GBP_USD'
 #  )|
 #  (
 #  pred_LM_period_return_50_Price >
 #            mean_500_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*0.55 &
 #  pred_LM_period_return_50_Price <
 #            mean_500_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*0.8 &
 #  Asset == 'GBP_USD'
 #  )|
 #  (
 #  pred_LM_period_return_50_Price >
 #            mean_200_pred_LM_period_return_50_Price + sd_200_pred_LM_period_return_50_Price*2.5 &
 #  pred_LM_period_return_50_Price <
 #            mean_200_pred_LM_period_return_50_Price + sd_200_pred_LM_period_return_50_Price*10 &
 #  Asset == 'GBP_USD'
 #  )|
 #  (
 #  pred_GLM_period_return_50_Price > 0.75 &
 #  pred_GLM_period_return_50_Price < 0.775 &
 #  Asset == 'GBP_USD'
 #  )|
 # (
 # pred_LM_period_return_50_Price > mean_50_pred_LM_period_return_50_Price &
 # pred_LM_period_return_50_Price > mean_500_pred_LM_period_return_50_Price &
 # pred_LM_period_return_50_Price > mean_200_pred_LM_period_return_50_Price &
 # pred_LM_period_return_50_Price > mean_100_pred_LM_period_return_50_Price &
 # mean_100_pred_LM_period_return_50_Price < mean_500_pred_LM_period_return_50_Price &
 # pred_GLM_period_return_50_Price > mean_50_pred_GLM_period_return_50_Price &
 # Asset == 'GBP_USD'
 # )|
 # (
 # pred_LM_period_return_50_Price > mean_50_pred_LM_period_return_50_Price &
 # pred_LM_period_return_50_Price > mean_500_pred_LM_period_return_50_Price &
 # pred_LM_period_return_50_Price > mean_200_pred_LM_period_return_50_Price &
 # pred_LM_period_return_50_Price > mean_100_pred_LM_period_return_50_Price &
 # mean_50_pred_LM_period_return_50_Price < mean_200_pred_LM_period_return_50_Price &
 # pred_GLM_period_return_50_Price > mean_50_pred_GLM_period_return_50_Price &
 # Asset == 'GBP_USD'
 # )

"

cumulative_returns_sim_data <-
  get_total_portfolio_summary(
    generated_preds = generated_preds %>%
      # filter(Asset == "GBP_USD")
      filter( str_detect(Asset, "[A-Z]"))
    ,
    trade_statement = trade_statement,
    actual_wins_losses =actual_wins_losses %>%
      # filter(Asset == "GBP_USD")
      filter( str_detect(Asset, "[A-Z]"))
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

asset_summaries_control <-
  get_asset_random_sim_returns(
    generated_preds = generated_preds %>%
      # filter(Asset == "GBP_USD")
      filter( str_detect(Asset, "[A-Z]"))
    ,
    trade_statement = "str_detect(Asset, '[A-Z]')",
    actual_wins_losses = actual_wins_losses %>%
      # filter(Asset == "GBP_USD")
      filter( str_detect(Asset, "[A-Z]"))
    ,
    trade_direction = "Long",
    return_col = "period_return_50_Price",
    simulations = 5000,
    samples = 50
  )

asset_summaries <-
  get_asset_random_sim_returns(
    generated_preds = generated_preds %>%
      # filter(Asset == "GBP_USD")
    filter( str_detect(Asset, "[A-Z]"))
    ,
    trade_statement = trade_statement,
    actual_wins_losses = actual_wins_losses %>%
      # filter(Asset == "GBP_USD")
    filter( str_detect(Asset, "[A-Z]"))
    ,
    trade_direction = "Long",
    return_col = "period_return_50_Price",
    simulations = 7000,
    samples = 50
  )


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
  )

