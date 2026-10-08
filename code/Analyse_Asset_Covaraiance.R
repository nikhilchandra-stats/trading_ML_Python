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
correlation_DB_Store <- "C:/Users/Nikhil Chandra/Documents/trade_data/Day_Trader_Cor_Continuous_Models/correlation_data.db"
start_date = "2009-01-01"
# training_date = today() %>% as.character()
training_date = '2021-01-01'
# end_date = today() %>% as.character()
end_date = '2021-01-01'
assets_to_port <- c("HK33_HKD", "XAU_USD", "XAG_USD", "EUR_USD", "USD_JPY", "SPX500_USD", "EUR_JPY", "EU50_EUR",
                    "JP225_USD", "UK100_GBP", "BTC_USD", "NATGAS_USD", "DE30_EUR", "WTICO_USD",
                    "FR40_EUR", "WHEAT_USD", "SOYBN_USD", "SUGAR_USD", "XCU_USD", "AUD_USD", "USD_CAD",
                    "GBP_USD", "NZD_USD", "USD_CHF", "NAS100_USD", "CH20_CHF", "USD_NOK", "USD_SEK",
                    "US2000_USD", "USB30Y_USD", "AUD_NZD", "EUR_GBP", "EUR_NZD", "GBP_JPY", "CAD_JPY",
                    "EUR_CAD") %>% unique()

Indices_Metals_Bonds <- list()

date_increment = 10000
final_sim_date <-
  as_datetime(training_date, tz = "Australia/Canberra") - dhours(date_increment)
sim_date_vector <-
  seq(as_datetime(start_date, tz = "Australia/Canberra"), final_sim_date, "hours")

volatility_factor_stop_vec <-
  tibble(volatility_factor_stop = c(2,5,8,10,12))

running_volatility_tibble <-
  c(2,5,8,10,12) %>%
  map_dfr(
    ~
      volatility_factor_stop_vec %>%
      mutate(
        volatility_factor_profit = .x
      )
  )

running_volatility_tibble <-
  c(20, 60,120, 200) %>%
  map_dfr(
    ~
      running_volatility_tibble %>%
      mutate(running_volatility_period_max = .x)
  )

running_volatility_tibble <-
  c(100,200,50) %>%
  map_dfr(
    ~
      running_volatility_tibble %>%
      mutate(running_volatility_period_mean = .x)
  )

c = 0
redo_DB = FALSE
for (j in 1:100) {

  sim_start = sim_date_vector %>% sample(size = 1)
  sim_end = sim_start + dhours(date_increment)
  Indices_Metals_Bonds <- list()
  gc()

  Indices_Metals_Bonds[[1]] <-
    get_db_data_quickly_algo(
      db_location = db_location,
      start_date = sim_start %>% as_date() %>% as.character(),
      end_date = sim_end %>% as_date() %>% as.character(),
      # end_date = sim_end %>% as_date() %>% as.character(),
      time_frame = "H1",
      bid_or_ask = "ask",
      assets =assets_to_port
    ) %>%
    distinct()
  Indices_Metals_Bonds[[2]] <-
    get_db_data_quickly_algo(
      db_location = db_location,
      start_date = sim_start %>% as_date() %>% as.character(),
      end_date = sim_end %>% as_date() %>% as.character(),
      # end_date = sim_end %>% as_date() %>% as.character(),
      time_frame = "H1",
      bid_or_ask = "bid",
      assets =assets_to_port
    ) %>%
    distinct()

  for (i in 1:dim(running_volatility_tibble)[1] ) {

    c = c + 1
    volatility_factor_stop = running_volatility_tibble$volatility_factor_stop[i]
    volatility_factor_profit = running_volatility_tibble$volatility_factor_profit[i]

    running_volatility_period_max = running_volatility_tibble$running_volatility_period_max[i]
    running_volatility_period_mean = running_volatility_tibble$running_volatility_period_mean[i]

    profit_multiple = 1
    risk_dollar_value = 5
    slippage_percent = 0
    end_period = 132

    temp_ask <- Indices_Metals_Bonds[[1]] %>% ungroup()
    temp_bid <- Indices_Metals_Bonds[[2]] %>% ungroup()

    tictoc::tic()
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
      ) %>%
      ungroup() %>%
      dplyr::select(Date, Asset, Final_Return, volatility_factor_stop, volatility_factor_profit, profit_multiple,
                    running_volatility_period_max, running_volatility_period_mean) %>%
      filter(!is.na(Final_Return))

    distinct_params <-
      portfolio_data_train %>%
      distinct(
        volatility_factor_stop, volatility_factor_profit, profit_multiple,
        running_volatility_period_max, running_volatility_period_mean
      )

    COV_matrix <-
      portfolio_data_train %>%
      dplyr::select(Date, Asset, Final_Return) %>%
      pivot_wider(names_from = Asset, values_from = Final_Return) %>%
      filter(if_all(everything(), ~ !is.na(.))) %>%
      dplyr::select(-Date) %>%
      cor()

    asset_rows <- row.names(COV_matrix)

    COV_matrix_tibble <-
      COV_matrix %>%
      as_tibble() %>%
      mutate(
        Asset_2 = asset_rows
      ) %>%
      pivot_longer(-Asset_2, values_to = "correlation", names_to = "Asset_1") %>%
      mutate(
        start_date = sim_start %>% as_date() %>% as.character(),
        end_date = sim_end %>% as_date() %>% as.character()
      ) %>%
      bind_cols(distinct_params)

    Expected_Returns <-
      portfolio_data_train %>%
      dplyr::select(Date, Asset, Final_Return) %>%
      group_by(Asset) %>%
      summarise(
        Mean_Return = mean(Final_Return, na.rm = T),
        SDEV = sd(Final_Return, na.rm = T)
      ) %>%
      rename(
        Asset_2 = Asset
      )

    COV_matrix_tibble <-
      COV_matrix_tibble %>%
      left_join(
        Expected_Returns %>%
          rename(
            Mean_Return_Asset_2 = Mean_Return,
            SDEV_Asset_2 = SDEV
          )
        )%>%
      left_join(
        Expected_Returns %>%
          rename(
            Asset_1 = Asset_2,
            Mean_Return_Asset_1 = Mean_Return,
            SDEV_Asset_1 = SDEV
          )
      )

    tictoc::toc()

    correlation_DB_Store_con <-
      connect_db(correlation_DB_Store)

    if(c == 1 & redo_DB == TRUE) {

      write_table_sql_lite(conn = correlation_DB_Store_con,
                           .data = COV_matrix_tibble,
                           table_name = "COR_DATA")

    } else {

      append_table_sql_lite(conn = correlation_DB_Store_con,
                            .data = COV_matrix_tibble,
                            table_name = "COR_DATA")

    }

  }

  rm(Indices_Metals_Bonds)
  gc()


}

corr_summary <-
  get_correlation_data_summary(
    correlation_DB_Store_con = "C:/Users/Nikhil Chandra/Documents/trade_data/Day_Trader_Cor_Continuous_Models/correlation_data.db",
    date_max_filt = '2020-01-01',
    start_date_min = '2010-01-01'
  )
biggest_negatives <-
  get_cor_biggest_negatives(
    corr_summary = corr_summary,
    asset_2_highest_negs = 10,
    asset_2_ev_filt = 5,
    filter_for_positive_EV_only = TRUE,
    filter_biggest_EVs_First = FALSE
  )
biggest_negatives_max_EV <-
  get_cor_biggest_negatives(
    corr_summary = corr_summary,
    asset_2_highest_negs = 10,
    asset_2_ev_filt = 5,
    filter_for_positive_EV_only = TRUE,
    filter_biggest_EVs_First = TRUE
  )

assets_to_test <-
  get_top_X_neg_cors(biggest_negatives = biggest_negatives_max_EV)

# Assess Portoflios -------------------------------------------------------
Indices_Metals_Bonds <- list()
gc()
sim_start <- "2016-01-01"
sim_end <- today() %>% as.character()

all_required_assets <-
  c(assets_to_test$Asset_2, assets_to_test$Asset_1) %>% unique()


Indices_Metals_Bonds[[1]] <-
  get_db_data_quickly_algo(
    db_location = db_location,
    start_date = sim_start %>% as_date() %>% as.character(),
    end_date = sim_end %>% as_date() %>% as.character(),
    # end_date = sim_end %>% as_date() %>% as.character(),
    time_frame = "H1",
    bid_or_ask = "ask",
    assets =all_required_assets
  ) %>%
  distinct()
Indices_Metals_Bonds[[2]] <-
  get_db_data_quickly_algo(
    db_location = db_location,
    start_date = sim_start %>% as_date() %>% as.character(),
    end_date = sim_end %>% as_date() %>% as.character(),
    # end_date = sim_end %>% as_date() %>% as.character(),
    time_frame = "H1",
    bid_or_ask = "bid",
    assets =all_required_assets
  ) %>%
  distinct()

strategy_analysis_1 <-
  rolling_cor_strategy(
    ask_data = Indices_Metals_Bonds[[1]],
    bid_data = Indices_Metals_Bonds[[2]],
    asset_grouping_data = assets_to_test,
    cor_rolling_period = 200,
    sum_rolling_period = 200,
    slippage_percent = 0,
    risk_dollar_value = 5,
    profit_multiple = 1,
    end_period = 132,
    currency_conversion = currency_conversion,
    asset_infor = asset_infor
  )

strategy_analysis_2 <-
  rolling_cor_strategy(
    ask_data = Indices_Metals_Bonds[[1]],
    bid_data = Indices_Metals_Bonds[[2]],
    asset_grouping_data = assets_to_test,
    cor_rolling_period = 1000,
    sum_rolling_period = 200,
    slippage_percent = 0,
    risk_dollar_value = 5,
    profit_multiple = 1,
    end_period = 132,
    currency_conversion = currency_conversion,
    asset_infor = asset_infor
  )

names(strategy_analysis_1)
names(strategy_analysis_2)

trade_statment <-
  "
  (CAD_JPY_USD_CAD_SlidingPnormCor_200 > 0.8 & Asset %in% c('CAD_JPY', 'USD_CAD') )|
  (USD_CHF_GBP_USD_SlidingPnormCor_200 > 0.8 & Asset %in% c('USD_CHF', 'GBP_USD') )|
  (USD_NOK_EUR_USD_SlidingPnormCor_200 > 0.8 & Asset %in% c('USD_NOK', 'EUR_USD'))|
  (NZD_USD_USD_SEK_SlidingPnormCor_200 > 0.8 & Asset %in% c('NZD_USD', 'USD_SEK'))|
  (EUR_NZD_AUD_USD_SlidingPnormCor_200 > 0.8 & Asset %in% c('EUR_NZD', 'AUD_USD'))|
  (GBP_JPY_EUR_GBP_SlidingPnormCor_200 > 0.8 & Asset %in% c('GBP_JPY', 'EUR_GBP'))|
  (USD_JPY_XAU_USD_SlidingPnormCor_200 > 0.8 & Asset %in% c('USD_JPY'))|
  (EUR_CAD_CH20_CHF_SlidingPnormCor_200 > 0.8 & Asset %in% c('EUR_CAD'))|
  (NATGAS_USD_BTC_USD_SlidingPnormCor_200 > 0.8 & Asset %in% c('NATGAS_USD'))
"

trade_statment2 <-
  "
  (CAD_JPY_USD_CAD_SlidingPnormCor_1000 > 0.8 & Asset %in% c('CAD_JPY', 'USD_CAD') )|
  (USD_CHF_GBP_USD_SlidingPnormCor_1000 < 0.1 & Asset %in% c('USD_CHF', 'GBP_USD') )|
  (USD_NOK_EUR_USD_SlidingPnormCor_1000 < 0.2 & Asset %in% c('USD_NOK', 'EUR_USD') )|
  (NZD_USD_USD_SEK_SlidingPnormCor_1000 > 0.9 & Asset %in% c('NZD_USD', 'USD_SEK') )
"

analyse_performance <-
  strategy_analysis_1 %>%
  # filter(Asset %in% c(
  #                     'USD_CHF', 'GBP_USD', 'CAD_JPY',
  #                     'USD_CAD', 'USD_NOK', 'EUR_USD',
  #                     # 'JP225_USD', 'USB30Y_USD',
  #                     # 'USD_JPY', 'XAU_USD',
  #                     'NZD_USD', 'USD_SEK',
  #                     'EUR_NZD', 'AUD_USD'
  #                     ) ) %>%
  # filter(Asset %in% c('EUR_NZD', 'AUD_USD') ) %>%
  filter(!(Asset %in% c('JP225_USD', 'USB30Y_USD', 'XAU_USD', 'CH20_CHF', 'BTC_USD'))) %>%
  filter(!is.na(Final_Return)) %>%
  mutate(
    trade_col = eval(parse(text = trade_statment))
  ) %>%
  mutate(
    trade_col = case_when(trade_col == TRUE ~ "Long", TRUE ~ "No Trade")
  ) %>%
  dplyr::select(Date, Final_Return, trade_col)

analyse_performance2 <-
  strategy_analysis_2 %>%
  filter(Asset %in% c('NZD_USD', 'USD_SEK') ) %>%
  filter(!is.na(Final_Return)) %>%
  mutate(
    trade_col = eval(parse(text = trade_statment2))
  ) %>%
  mutate(
    trade_col = case_when(trade_col == TRUE ~ "Long", TRUE ~ "No Trade")
  ) %>%
  dplyr::select(Date, Final_Return, trade_col)


control <-
  analyse_performance2 %>%
  # bind_rows(analyse_performance %>% dplyr::select(Date, Final_Return, trade_col)) %>%
  group_by(Date) %>%
  summarise(Final_Return = sum(Final_Return)) %>%
  ungroup() %>%
  filter(!is.na(Final_Return)) %>%
  arrange(Date) %>%
  mutate(Final_Return_Cumulative = cumsum(Final_Return)) %>%
  mutate(trade_col = "Control")

analyse_performance_comp <-
  analyse_performance2 %>%
  # bind_rows(analyse_performance %>% dplyr::select(Date, Final_Return, trade_col)) %>%
  filter(trade_col == "Long") %>%
  group_by(Date) %>%
  summarise(Final_Return = sum(Final_Return)) %>%
  ungroup() %>%
  arrange(Date) %>%
  mutate(Final_Return_Cumulative = cumsum(Final_Return)) %>%
  mutate(trade_col = "Long")

analyse_performance_comp %>%
  bind_rows(control) %>%
  # filter(Date <= '2024-01-01') %>%
  ggplot(aes(x = Date, y = Final_Return_Cumulative
             ,color = trade_col
  )) +
  geom_line() +
  geom_hline(yintercept = 0, linetype = "dashed", color = 'darkred') +
  # facet_wrap(.~trade_col, scales = "free") +
  theme_minimal() +
  scale_y_continuous(n.breaks = 20) +
  theme(legend.position = "bottom")

