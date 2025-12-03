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
  start_date = (today() - days(5)) %>% as.character()
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
  c("XAG_USD", "XAG_EUR", "XAG_CAD", "XAG_AUD", "XAG_GBP", "XAG_JPY", "XAG_SGD", "XAG_CHF",
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
    "EUR_AUD", "EUR_NZD", "EUR_CHF", "ESPIX_EUR" ,"EUR_NZD" ,
    "GBP_AUD", "GBP_NZD", "UK100_GBP", "UK10YB_GBP", "GBP_CHF", "GBP_CAD",
    "NL25_EUR") %>%
  unique()

asset_infor <- get_instrument_info()
raw_macro_data <- get_macro_event_data()

db_location = "D:/Asset Data/Oanda_Asset_Data Algo 2.db"
start_date = "2025-11-17"
end_date = "2025-11-30"

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
      risk_dollar_value_long = 3,
      end_period = 33,
      time_frame = "H1"
    )

}

newest_results <-
  get_current_new_algo_trades(realised_DB_path ="D:/trade_data/trade_tracker_realised.db")

trade_tracker_DB <- connect_db("D:/trade_data/trade_tracker_daily_buy_close.db")
all_trades_so_far <-
  DBI::dbGetQuery(conn = trade_tracker_DB,
                  "SELECT * FROM trade_tracker")
DBI::dbDisconnect(trade_tracker_DB)
gc()

trade_tracker_DB <- connect_db("D:/trade_data/trade_tracker_daily_buy_close 2.db")
all_trades_so_far2 <-
  DBI::dbGetQuery(conn = trade_tracker_DB,
                  "SELECT * FROM trade_tracker")
DBI::dbDisconnect(trade_tracker_DB)
gc()

all_trades_so_far_comb <-
  all_trades_so_far %>%
  bind_rows(all_trades_so_far2) %>%
  distinct() %>%
  filter()

distinct_assets <-
  all_trades_so_far_comb %>%
  distinct(Asset, account_var, trade_col, tradeID, periods_ahead) %>%
  rename(id = tradeID) %>%
  mutate(inLocalDB = TRUE)

newest_results_sum <-
  newest_results %>%
  group_by(id, Asset, account_var, initialUnits) %>%
  mutate(kk = row_number()) %>%
  slice_min(kk) %>%
  ungroup() %>%
  left_join(distinct_assets) %>%
  filter(inLocalDB == TRUE, !is.na(inLocalDB)) %>%
  dplyr::select(-inLocalDB) %>%
  dplyr::select(-kk) %>%
  dplyr::select(Asset, Date = date_open , periods_ahead, trade_col) %>%
  mutate(
    Date = floor_date(Date, unit = "hour")
  ) %>%
  filter(trade_col == "Long") %>%
  distinct(Asset, Date, periods_ahead)

port_return_dfr <-
  port_return_list %>%
  map_dfr( ~
             .x %>%
             dplyr::select(adjusted_Date, Asset, Return, Date, close_Date, period_since_open)
  ) %>%
  ungroup() %>%
  left_join(newest_results_sum) %>%
  filter(!is.na(periods_ahead)) %>%
  group_by(adjusted_Date, Asset, Date,close_Date, period_since_open ) %>%
  summarise(Return = sum(Return, na.rm = T),
            trades_open = n_distinct(Date)) %>%
  group_by(adjusted_Date) %>%
  mutate(
    Total_Return = sum(Return)
  )


results_sum_asset <-
  port_return_dfr %>%
  ungroup() %>%
  group_by(Asset) %>%
  summarise(
  min_portfolio = min(Return, na.rm = T),
  Portfolio_05 = quantile(Return, 0.05, na.rm = T),
  Portfolio_10 = quantile(Return, 0.1, na.rm = T),
  Portfolio_25 = quantile(Return, 0.25, na.rm = T),
  Portfolio_50 = quantile(Return, 0.50, na.rm = T),
  Portfolio_mean = mean(Return, na.rm = T),
  Portfolio_75 = quantile(Return, 0.75, na.rm = T),
  trades = n_distinct(Date)
  ) %>%
  ungroup()

results_sum <-
  port_return_list %>%
  map_dfr( ~
             .x %>%
             dplyr::select(adjusted_Date, Asset, Return, Date, close_Date, period_since_open)
  ) %>%
  ungroup() %>%
  left_join(newest_results_sum) %>%
  filter(!is.na(periods_ahead)) %>%
  distinct(Date, adjusted_Date, Return) %>%
  group_by(adjusted_Date) %>%
  summarise(Total_Return = sum(Return, na.rm = TRUE),
            trades = n_distinct(Date)) %>%
  ungroup() %>%
  summarise(
    min_portfolio = min(Total_Return, na.rm = T),
    Portfolio_05 = quantile(Total_Return, 0.05, na.rm = T),
    Portfolio_10 = quantile(Total_Return, 0.1, na.rm = T),
    Portfolio_25 = quantile(Total_Return, 0.25, na.rm = T),
    Portfolio_50 = quantile(Total_Return, 0.50, na.rm = T),
    Portfolio_mean = mean(Total_Return, na.rm = T),
    Portfolio_75 = quantile(Total_Return, 0.75, na.rm = T),
    trades_mean = mean(trades, na.rm = T)
  ) %>%
  ungroup()


plot_data <-
  port_return_list %>%
  map_dfr( ~
             .x %>%
             dplyr::select(adjusted_Date, Asset, Return, Date, close_Date, period_since_open)
  ) %>%
  ungroup() %>%
  left_join(newest_results_sum) %>%
  filter(!is.na(periods_ahead)) %>%
  distinct(Date, adjusted_Date, Return) %>%
  group_by(adjusted_Date) %>%
  summarise(Total_Return = sum(Return, na.rm = TRUE),
            trades = n_distinct(Date)) %>%
  ungroup() %>%
  mutate(
    max_point_port = max(Total_Return, na.rm = T),
    Date_at_max =
      ifelse(Total_Return == max_point_port, adjusted_Date, NA),
    Date_at_max = as_datetime(Date_at_max)
  ) %>%
  fill(Date_at_max, .direction = "updown")

plot_data %>%
  ggplot(aes(x = adjusted_Date, y = Total_Return)) +
  geom_line() +
  theme_minimal()

USD_CAD <-
  port_return_dfr %>%
  filter(Asset == "USD_CAD") %>%
  group_by(Date) %>%
  mutate(id = Date %>% as.character() )

USD_CAD %>%
  ggplot(aes(x = adjusted_Date, color = id, y = Return)) +
  geom_line(show.legend = FALSE) +
  theme_minimal()
