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
training_data_db <- "C:/Users/Nikhil Chandra/Documents/trade_data/Day_Trader_Cor_Continuous_Models/training_data.db"
start_date = "2019-01-01"
training_date = today() %>% as.character()
end_date = today() %>% as.character()
assets_to_port <- c("HK33_HKD", "XAU_USD", "XAG_USD", "EUR_USD", "USD_JPY", "SPX500_USD", "EUR_JPY", "EU50_EUR",
                    "JP225_USD", "UK100_GBP", "BTC_USD", "NATGAS_USD", "DE30_EUR", "WTICO_USD",
                    "FR40_EUR", "WHEAT_USD", "SOYBN_USD", "SUGAR_USD", "XCU_USD", "AUD_USD", "USD_CAD",
                    "GBP_USD", "NZD_USD", "USD_CHF", "NAS100_USD", "CH20_CHF", "USD_NOK", "USD_SEK") %>% unique()

Indices_Metals_Bonds[[1]] <-
  get_db_data_quickly_algo(
    db_location = db_location,
    start_date = start_date,
    end_date = training_date,
    # end_date = sim_end %>% as_date() %>% as.character(),
    time_frame = "H1",
    bid_or_ask = "ask",
    assets =assets_to_port
  ) %>%
  distinct()
Indices_Metals_Bonds[[2]] <-
  get_db_data_quickly_algo(
    db_location = db_location,
    start_date = start_date,
    end_date = training_date,
    # end_date = sim_end %>% as_date() %>% as.character(),
    time_frame = "H1",
    bid_or_ask = "bid",
    assets =assets_to_port
  ) %>%
  distinct()

final_sim_date <-
  as_datetime(training_date, tz = "Australia/Canberra") - dhours(8000)
sim_date_vector <-
  seq(as_datetime(start_date, tz = "Australia/Canberra"), final_sim_date, "hours")

volatility_factor_stop_vec <-
  tibble(volatility_factor_stop = c(1,3,5,7, 9,12))

running_volatility_tibble <-
  c(1,3,5,7, 9,12) %>%
  map_dfr(
    ~
      volatility_factor_stop_vec %>%
      mutate(
        volatility_factor_profit = .x
      )
  )

running_volatility_tibble <-
  c(20, 60, 90 ,120) %>%
  map_dfr(
    ~
      running_volatility_tibble %>%
      mutate(running_volatility_period_max = .x)
  )

running_volatility_tibble <-
  c(100,200,300) %>%
  map_dfr(
    ~
      running_volatility_tibble %>%
      mutate(running_volatility_period_mean = .x)
  )



temp_reg_data_train_list <-list()
all_assets_to_test <-
  assets_to_port %>%
  map_dfr(
    ~
      tibble(
        Asset_1 = assets_to_port,
      ) %>%
      mutate(
        Asset_2 = .x
      )
  ) %>%
  filter(Asset_1 != Asset_2)

rm(assets_to_port)

correlation_DB_Store <-
  "C:/Users/Nikhil Chandra/Documents/trade_data/Day_Trader_Cor_Continuous_Models/correlation_store.db"


c = 0

for (i in 1:dim(running_volatility_tibble)[1] ) {

  temp_list_statment <- list()

 for (j in 1:dim(all_assets_to_test)[1]) {

   volatility_factor_stop = running_volatility_tibble$volatility_factor_stop[i]
   volatility_factor_profit = running_volatility_tibble$volatility_factor_profit[i]

   running_volatility_period_max = running_volatility_tibble$running_volatility_period_max[i]
   running_volatility_period_mean = running_volatility_tibble$running_volatility_period_mean[i]

   profit_multiple = 1
   risk_dollar_value = 5
   slippage_percent = 0
   end_period = 132

   temp_list_statment[[j]] <-
     glue::glue(
     "
     temp_{j} <-
      get_dynamic_stop_prof_returns(
        Ask_Data = temp_ask,
        Bid_Data = temp_bid,
        periods_wanted = {end_period},
        trade_direction = 'Long',
        currency_conversion =currency_conversion,
        asset_infor = asset_infor,
        slippage_percent = {slippage_percent},
        risk_dollar_value = {risk_dollar_value},
        volatility_factor_stop = {volatility_factor_stop},
        volatility_factor_profit = {volatility_factor_profit},
        profit_multiple = {profit_multiple},
        running_volatility_period_max = {running_volatility_period_max},
        running_volatility_period_mean = {running_volatility_period_mean}
      ) %>%
      ungroup() %>%
      dplyr::select(Date, Asset, Final_Return, volatility_factor_stop, volatility_factor_profit, profit_multiple,
                    running_volatility_period_max, running_volatility_period_mean) %>%
      filter(!is.na(Final_Return))"
   )

 }

  temp_list_statment_eval <-
    temp_list_statment %>%
    unlist() %>%
    paste(collapse = "\n")

  eval(parse(text = temp_list_statment_eval))


  for (j in 1:dim(all_assets_to_test)[1]) {

    c = c + 1

    assets_to_port =
      c(
        all_assets_to_test$Asset_1[j],
        all_assets_to_test$Asset_2[j]
      ) %>% unique()

    temp_ask <- Indices_Metals_Bonds[[1]] %>% ungroup() %>% filter(Asset %in% assets_to_port)
    temp_bid <- Indices_Metals_Bonds[[2]] %>% ungroup() %>% filter(Asset %in% assets_to_port)

    volatility_factor_stop = running_volatility_tibble$volatility_factor_stop[i]
    volatility_factor_profit = running_volatility_tibble$volatility_factor_profit[i]

    running_volatility_period_max = running_volatility_tibble$running_volatility_period_max[i]
    running_volatility_period_mean = running_volatility_tibble$running_volatility_period_mean[i]

    profit_multiple = 1
    risk_dollar_value = 5
    slippage_percent = 0
    end_period = 132

    sim_start = sim_date_vector %>% sample(size = 1)
    sim_end = sim_start + dhours(10000)

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

    final_loop_temp <-
      portfolio_data_train %>%
      ungroup() %>%
      dplyr::select(Date, Asset, Final_Return, volatility_factor_stop, volatility_factor_profit, profit_multiple,
                    running_volatility_period_max, running_volatility_period_mean) %>%
      filter(!is.na(Final_Return))

    distinct_params <-
      final_loop_temp %>%
      distinct(
        volatility_factor_stop, volatility_factor_profit, profit_multiple,
        running_volatility_period_max, running_volatility_period_mean
      )

    covariance_matrix <-
      final_loop_temp %>%
      dplyr::select(Date, Asset,Final_Return ) %>%
      pivot_wider(names_from = Asset, values_from = Final_Return)  %>%
      dplyr::select(-Date) %>%
      mutate(
        COV_Period = running_volatility_period_mean*5,
        !!as.name(glue::glue("{all_assets_to_test$Asset_1[j]}_COV_{all_assets_to_test$Asset_2[j]}"))
        := slider::slide2_dbl(.x = !!as.name(all_assets_to_test$Asset_1[j]),
                              .y = !!as.name(all_assets_to_test$Asset_2[j]),
                              .f = cor,
                              .before = running_volatility_period_mean,
                              .complete = FALSE)
      ) %>%
      rename(Asset_1_Return = 1,
             Asset_2_Return = 2,
             Correlation = 4) %>%
      mutate(
        Asset_1 = all_assets_to_test$Asset_1[j],
        Asset_2 = all_assets_to_test$Asset_2[j]
      ) %>%
      bind_cols(distinct_params)

    correlation_DB_Store_con <-
      connect_db(correlation_DB_Store)

    if(c == 1) {

      write_table_sql_lite(conn = correlation_DB_Store_con,
                           .data = covariance_matrix,
                           table_name = "COR_DATA")

    } else {

      append_table_sql_lite(conn = correlation_DB_Store_con,
                           .data = covariance_matrix,
                           table_name = "COR_DATA")

    }

  }

}

