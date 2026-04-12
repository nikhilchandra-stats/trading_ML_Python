helpeR::load_custom_functions()

all_aud_symbols <- get_oanda_symbols() %>%
  keep(~ str_detect(.x, "AUD")|str_detect(.x, "USD_SEK|USD_NOK|USD_HUF|USD_ZAR|USD_CNY|USD_MXN"))
asset_infor <- get_instrument_info()
aud_assets <- read_all_asset_data_intra_day(
  asset_list_oanda = all_aud_symbols,
  save_path_oanda_assets = "C:/Users/Nikhil Chandra/Documents/Asset Data/oanda_data/",
  read_csv_or_API = "API",
  time_frame = "D",
  bid_or_ask = "bid",
  how_far_back = 10,
  start_date = (today() - days(3)) %>% as.character()
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
  c(
    "XAG_USD", "XAG_EUR", "XAG_CAD", "XAG_AUD", "XAG_GBP", "XAG_JPY", "XAG_SGD", "XAG_CHF",
    "XAG_NZD",
    "XAU_USD", "XAU_EUR", "XAU_CAD", "XAU_AUD", "XAU_GBP", "XAU_JPY", "XAU_SGD", "XAU_CHF",
    "XAU_NZD",
    "BTC_USD", "LTC_USD", "BCH_USD",
    "US30_USD", "FR40_EUR", "US2000_USD", "CH20_CHF", "SPX500_USD", "AU200_AUD",
    "JP225_USD", "JP225Y_JPY", "SG30_SGD", "EU50_EUR", "HK33_HKD",
    "USB02Y_USD", "USB05Y_USD", "USB30Y_USD", "USB10Y_USD", "UK100_GBP",
    "AUD_USD", "EUR_USD", "GBP_USD", "USD_CHF", "USD_JPY", "USD_MXN", "USD_SEK", "USD_NOK",
    "NZD_USD", "USD_CAD", "USD_SGD", "ETH_USD", "XPT_USD", "XPD_USD",
    "USD_JPY", "GBP_JPY", "USD_SGD", "EUR_SEK",
    "DE30_EUR",
    "USD_CHF", "USD_SEK", "XCU_USD", "SUGAR_USD",
    "USD_MXN", "GBP_USD", "WTICO_USD", "EUR_JPY", "USD_NOK",
    "XAU_USD",
    "USD_CZK",  "WHEAT_USD",
    "EUR_USD", "SG30_SGD", "AU200_AUD", "XAG_USD",
    "EUR_GBP", "USD_CNH", "USD_CAD", "NAS100_USD",
    "EU50_EUR", "NATGAS_USD", "SOYBN_USD",
    "US2000_USD",
    "BCO_USD", "AUD_USD", "NZD_USD", "NZD_CHF", "WHEAT_USD",
    "JP225_USD", "SPX500_USD",
    "UK10YB_GBP",
    "HK33_HKD", "USD_JPY",
    "BTC_USD",
    "AUD_NZD", "GBP_CHF",
    "EUR_HUF", "EUR_ZAR", "NZD_JPY", "EUR_NZD",
    "USB02Y_USD",
    "XAU_CAD", "GBP_JPY", "EUR_NOK", "USD_SGD", "EUR_SEK",
    "DE30_EUR",
    "AUD_CAD", "NZD_USD", "ETH_USD","BCO_USD", "AUD_USD",
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
    "GBP_CAD"
    ) %>%
  unique()

asset_infor <- get_instrument_info()
raw_macro_data <- get_macro_event_data()

db_location = "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data_Most_Assets_2025-09-13.db"
update_local_db_file(
  db_location = db_location,
  time_frame = "D",
  bid_or_ask = "ask",
  how_far_back = 25,
  asset_list_oanda = asset_list_oanda
)
update_local_db_file(
  db_location = db_location,
  time_frame = "H1",
  bid_or_ask = "ask",
  how_far_back = 25,
  asset_list_oanda = asset_list_oanda
)

update_local_db_file(
  db_location = db_location,
  time_frame = "D",
  bid_or_ask = "bid",
  asset_list_oanda = asset_list_oanda,
  how_far_back = 25
)
update_local_db_file(
  db_location = db_location,
  time_frame = "H1",
  bid_or_ask = "bid",
  asset_list_oanda = asset_list_oanda,
  how_far_back = 25
)

#--------------------------------------------------------------

start_date = "2025-11-01"
end_date = "2025-12-12"

Indices_Metals_Bonds <-
  get_Port_Buy_Data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    time_frame = "H1"
  )

distinct_assets <-
  Indices_Metals_Bonds[[1]] %>%
  pull(Asset) %>%
  unique()

port_return_list <- list()

for (i in 1:length(distinct_assets)) {

  trades_to_tag_with_returns_long <-
    Indices_Metals_Bonds[[1]] %>%
    ungroup() %>%
    distinct(Asset, Date) %>%
    mutate(trade_col = "Long") %>%
    filter(trade_col == "Long")

  port_return_list[[i]] <-
    get_portfolio_model(
      asset_data = Indices_Metals_Bonds,
      asset_of_interest = distinct_assets[i],
      tagged_trades = trades_to_tag_with_returns_long,
      stop_factor_long = 6,
      profit_factor_long = 20,
      risk_dollar_value_long = 5,
      end_period = 20,
      time_frame = "H1"
    )

}

port_return_dfr <-
  port_return_list %>%
  map_dfr( ~
             .x %>%
             dplyr::select(adjusted_Date, Asset, Return, Date, close_Date, period_since_open)
  ) %>%
  ungroup() %>%
  group_by(adjusted_Date, Asset, Date,close_Date ) %>%
  summarise(Return = sum(Return),
            trades_open = n_distinct(Date)) %>%
  group_by(adjusted_Date) %>%
  mutate(
    Total_Return = sum(Return)
  )

assets_to_get_results <-
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

get_all_realised_generic(
  realised_DB_path = "C:/Users/Nikhil Chandra/Documents/trade_data/trade_tracker_realised.db",
  write_or_append = "append",
  account_var = 1,
  algo_start_date = "2026-01-10",
  distinct_assets = assets_to_get_results
)

newest_results <-
  get_realised_trades_from_db_generic(
    realised_DB_path = "C:/Users/Nikhil Chandra/Documents/trade_data/trade_tracker_realised.db",
    table_name = "realised_return"
  ) %>%
  mutate(date_closed = as_datetime(date_closed))

newest_results_sum <-
  newest_results %>%
  group_by(id, Asset, account_var, initialUnits) %>%
  mutate(kk = row_number()) %>%
  slice_min(kk) %>%
  ungroup() %>%
  dplyr::select(-kk) %>%
  dplyr::select(Asset, Date = date_open, initialUnits, date_closed ) %>%
  mutate(
    Date = floor_date(Date, unit = "hour"),
    date_closed = floor_date(date_closed, unit = "hour"),
    trade_col =
      case_when(
        initialUnits > 0 ~ "Long",
        initialUnits < 0 ~ "Short"
      )
  ) %>%
  ungroup() %>%
  filter(trade_col == "Long") %>%
  distinct(Asset, Date, trade_col, date_closed) %>%
  group_by(Date, Asset) %>%
  mutate(kk = row_number()) %>%
  slice_min(kk) %>%
  ungroup() %>%
  dplyr::select(-kk) %>%
  mutate(filter_var = TRUE) %>%
  filter(Date >= "2026-01-19")


results_sum <-
  port_return_dfr %>%
  ungroup() %>%
  left_join(newest_results_sum ) %>%
  filter(!is.na(filter_var),  filter_var == TRUE) %>%
  # filter(Asset != "BTC_USD") %>%
  filter(adjusted_Date <= date_closed  ) %>%
  group_by(adjusted_Date) %>%
  mutate(
    Total_Return = sum(Return,na.rm = T)
  ) %>%
  ungroup() %>%
  summarise(
    min_portfolio = min(Total_Return, na.rm = T),
    Portfolio_05 = quantile(Total_Return, 0.05, na.rm = T),
    Portfolio_10 = quantile(Total_Return, 0.1, na.rm = T),
    Portfolio_25 = quantile(Total_Return, 0.25, na.rm = T),
    Portfolio_50 = quantile(Total_Return, 0.50, na.rm = T),
    Portfolio_mean = mean(Total_Return, na.rm = T),
    Portfolio_75 = quantile(Total_Return, 0.75, na.rm = T),
    Portfolio_80 = quantile(Total_Return, 0.80, na.rm = T),
    Portfolio_90 = quantile(Total_Return, 0.90, na.rm = T),
    Portfolio_95 = quantile(Total_Return, 0.95, na.rm = T),
    max_portfolio = max(Total_Return, na.rm = T)
  )

running_portfolio_ts <-
  port_return_dfr %>%
  ungroup() %>%
  left_join(newest_results_sum ) %>%
  filter(!is.na(filter_var),  filter_var == TRUE) %>%
  # filter(Asset != "BTC_USD") %>%
  filter(adjusted_Date <= date_closed  ) %>%
  group_by(adjusted_Date) %>%
  summarise(
    Total_Return = sum(Return,na.rm = T)
  )



running_portfolio_ts %>%
  ggplot(aes(x = adjusted_Date, y = Total_Return)) +
  geom_line() +
  theme_minimal()

results_sum_asset <-
  port_return_dfr %>%
  ungroup() %>%
  left_join(newest_results_sum ) %>%
  filter(!is.na(filter_var),  filter_var == TRUE) %>%
  # filter(Asset != "BTC_USD") %>%
  filter(adjusted_Date <= date_closed  ) %>%
  group_by(adjusted_Date, Asset) %>%
  mutate(
    Total_Return = sum(Return,na.rm = T)
  ) %>%
  ungroup() %>%
  group_by(Asset) %>%
  summarise(
    min_portfolio = min(Total_Return, na.rm = T),
    Portfolio_05 = quantile(Total_Return, 0.05, na.rm = T),
    Portfolio_10 = quantile(Total_Return, 0.1, na.rm = T),
    Portfolio_25 = quantile(Total_Return, 0.25, na.rm = T),
    Portfolio_50 = quantile(Total_Return, 0.50, na.rm = T),
    Portfolio_mean = mean(Total_Return, na.rm = T),
    Portfolio_75 = quantile(Total_Return, 0.75, na.rm = T),
    Portfolio_90 = quantile(Total_Return, 0.90, na.rm = T),
    max_portfolio = max(Total_Return, na.rm = T),
    trades = n_distinct(Date)
  )

newest_results_sum_actuals <-
  newest_results %>%
  filter(date_open >= "2026-01-19") %>%
  dplyr::select(Asset, Date = date_open, initialUnits, date_closed , realizedPL, financing, dividendAdjustment) %>%
  mutate(
    across(.cols = c(realizedPL, financing, dividendAdjustment), .fns = ~ as.numeric(.)),
    net_result = realizedPL + financing + dividendAdjustment,
    gross_result = realizedPL
  ) %>%
  mutate(
    Date = floor_date(Date, unit = "hour"),
    date_closed = floor_date(date_closed, unit = "hour"),
    trade_col =
      case_when(
        initialUnits > 0 ~ "Long",
        initialUnits < 0 ~ "Short"
      )
  ) %>%
  ungroup() %>%
  filter(trade_col == "Long") %>%
  arrange(date_closed) %>%
  mutate(
    cumulative_return_gross = cumsum(gross_result),
    cumulative_return = cumsum(net_result),
    gross_result = cumsum(gross_result)
  )

newest_results_sum_actuals$financing %>% sum() + (newest_results_sum_actuals$dividendAdjustment %>% sum())


newest_results_sum_actuals %>%
  ggplot(aes(x = date_closed)) +
  geom_line(aes(y = cumulative_return_gross), color = "red") +
  geom_line(aes(y = cumulative_return), color = "black") +
  theme_minimal()

newest_results_sum_actuals <-
  newest_results %>%
  filter(date_open >= "2026-01-19") %>%
  dplyr::select(Asset, Date = date_open, initialUnits,
                date_closed , realizedPL, financing, dividendAdjustment) %>%
  mutate(
    across(.cols = c(realizedPL, financing, dividendAdjustment), .fns = ~ as.numeric(.)),
    net_result = realizedPL + financing + dividendAdjustment,
    gross_result = realizedPL,
    financing = financing + dividendAdjustment,
    financing_perc = financing/gross_result
  ) %>%
  mutate(
    Date = floor_date(Date, unit = "hour"),
    date_closed = floor_date(date_closed, unit = "hour"),
    trade_col =
      case_when(
        initialUnits > 0 ~ "Long",
        initialUnits < 0 ~ "Short"
      )
  ) %>%
  ungroup() %>%
  filter(trade_col == "Long") %>%
  group_by(Asset) %>%
  summarise(
            Returns_mean = mean(net_result),
            Returns_25 = quantile(net_result, 0.25),
            Returns_75 = quantile(net_result, 0.75),
            Returns = sum(net_result),
            gross_result = sum(gross_result),
            financing = sum(financing),
            financing_perc = abs(financing/gross_result),
            trades = n()
            )

newest_results_sum_actuals$Returns %>% sum()
newest_results_sum_actuals$gross_result %>% sum()
newest_results_sum_actuals$financing %>% sum()

