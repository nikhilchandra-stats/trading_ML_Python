helpeR::load_custom_functions()

all_aud_symbols <- get_oanda_symbols() %>%
  keep(~ str_detect(.x, "AUD")|str_detect(.x, "USD_SEK|USD_NOK|USD_HUF|USD_ZAR|USD_CNY|USD_MXN"))
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
db_location = "C:/Users/nikhi/Documents/Asset Data/Oanda_Asset_Data_Most_Assets_2025-09-13.db"
start_date = "2019-01-01"
end_date = today() %>% as.character()
Indices_Metals_Bonds <- list()

assets_to_port <-
  c(
    "SPX500_USD",
    "AU200_AUD",
    "EU50_EUR",
    "US2000_USD",
    "XAU_USD",
    "XCU_USD",
    "AUD_USD",
    "UK100_GBP",
    "USD_JPY",
    "WTICO_USD",
    "HK33_HKD",
    "USD_SEK",

    "USD_SEK",
    "USD_SGD",
    "USD_CAD",
    "USD_JPY",
    "EUR_JPY",
    "GBP_CAD",
    "GBP_JPY",
    "EUR_GBP",
    "EUR_SEK",
    "EUR_AUD",
    "GBP_AUD"

  ) %>% unique()

assets_to_trade <-
  c(
    "SPX500_USD",
    "AU200_AUD",
    "EU50_EUR",
    "US2000_USD",
    "XAU_USD",
    "XCU_USD",
    "AUD_USD",
    "UK100_GBP",
    "USD_JPY",
    "WTICO_USD",
    "HK33_HKD",
    "USD_SEK",

    "USD_SEK",
    "USD_SGD",
    "USD_CAD",
    "USD_JPY",
    "EUR_JPY",
    "GBP_CAD",
    "GBP_JPY",
    "EUR_GBP",
    "EUR_SEK",
    "EUR_AUD",
    "GBP_AUD",

    "EUR_USD",
    "GBP_USD",
    "FR40_EUR",
    "NATGAS_USD",
    "XAU_USD",
    "EU50_EUR",
    "UK100_GBP",
    "EUR_GBP",
    "EUR_JPY",
    "BTC_USD",
    "XAG_USD"

  ) %>% unique()

Indices_Metals_Bonds[[1]] <-
  get_db_data_quickly_algo(
    db_location = db_location,
    start_date = start_date,
    end_date = as.character(today() + days(30)),
    time_frame = "H1",
    bid_or_ask = "ask",
    assets = assets_to_port
  ) %>%
  distinct()
Indices_Metals_Bonds[[2]] <-
  get_db_data_quickly_algo(
    db_location = db_location,
    start_date = start_date,
    end_date = as.character(today() + days(30)),
    time_frame = "H1",
    bid_or_ask = "bid",
    assets =  assets_to_port
  ) %>%
  distinct()

Indices_Metals_Bonds[[1]] <- Indices_Metals_Bonds[[1]] %>% filter(Date >= "2019-01-01")
Indices_Metals_Bonds[[2]] <- Indices_Metals_Bonds[[2]] %>% filter(Date >= "2019-01-01")

db_con <-
  connect_db("C:/Users/nikhi/Documents/trade_data/single_asset_v3_Bayes_Reg_Portfolio/Equity_Port_V3_Results.db")

return_data <- DBI::dbGetQuery(conn = db_con,
                statement = "SELECT * FROM Port_V3_Sim_Data" ) %>%
  mutate(
    Date = as_datetime(Date, tz = "Australia/Canberra")
  )

Date_Tibble <-
  Indices_Metals_Bonds[[1]] %>%
  distinct(Date)

trade_statment_currency <-
  "(predicted < 70 & predicted > 57)|
  (pred_10000_mean_roll_250 < 19 & pred_10000_mean_roll_250 > 10)|
  (pred_10000_mean_roll_500 < 18 & pred_10000_mean_roll_500 > 10)|
  (pred_10000_mean_roll_600 < 20 & pred_10000_mean_roll_600 > 13)|
  (pred_10000_mean_roll_1000 < 30 & pred_10000_mean_roll_1000 > 25)|
  (pred_10000_mean_roll_1500 < 1000 & pred_10000_mean_roll_1500 > 12)|
  (pred_10000_mean_roll_2000 < 1000 & pred_10000_mean_roll_2000 > 6)"

trade_statment_Equity_1 <-
  "(predicted > 40)|(pred_10000_mean_roll_250 > 26.5)|

   (pred_10000_mean_roll_500 > 15)|(pred_10000_mean_roll_600 > 12.5)|

   (pred_10000_mean_roll_100 > 32.5)|(pred_10000_mean_roll_1000 > 10)|

   (pred_10000_mean_roll_100 > pred_10000_mean_roll_1000 + 1.25*pred_10000_sd_roll_1000)|

   (pred_10000_mean_roll_50 < pred_10000_mean_roll_1000 + 1.25*pred_10000_sd_roll_1000 &
   pred_10000_mean_roll_50 > pred_10000_mean_roll_1000 + 0.95*pred_10000_sd_roll_1000)|

   (pred_10000_mean_roll_10 > 40)|

   (pred_10000_mean_roll_1500 < 10 & pred_10000_mean_roll_1500 > 7.5)|

   (
    pred_10000_mean_roll_10 > pred_10000_mean_roll_1500 &
    pred_10000_mean_roll_10 > pred_10000_mean_roll_1000 &
    pred_10000_mean_roll_10 > pred_10000_mean_roll_2000 &
    pred_10000_mean_roll_10 > pred_10000_mean_roll_600 &
    pred_10000_mean_roll_10 > 30 &
    pred_10000_mean_roll_1500 > 1 & pred_10000_mean_roll_1000 > 1 & pred_10000_mean_roll_2000 > 1
   )
"

trade_statment_Equity_2 <-
  "(predicted < 65 & predicted > 35)|
  (pred_10000_mean_roll_1000 < 3 & pred_10000_mean_roll_1000 > 1)|
  (pred_10000_mean_roll_600 < 20 & pred_10000_mean_roll_600 > 13)|
(pred_10000_mean_roll_500 < 18 & pred_10000_mean_roll_500 > 13)|
(pred_10000_mean_roll_50 < 60 & pred_10000_mean_roll_50 > 40)|
(pred_10000_mean_roll_100 < 60 & pred_10000_mean_roll_100 > 30)|
(predicted > pred_10000_mean_roll_1000 &
  predicted > pred_10000_mean_roll_600 &
  predicted > pred_10000_mean_roll_250 &
  predicted > pred_10000_mean_roll_500 &
  predicted > pred_10000_mean_roll_100 &
   pred_10000_mean_roll_100 > 10 &
   predicted > 10)"

trade_statment_Currency_USD_EUR_ONLY <-
  "(predicted < 1000 & predicted > 70)|
   (pred_10000_mean_roll_250 < 1000 & pred_10000_mean_roll_250 > 60)|
(pred_10000_mean_roll_100 < 1000 & pred_10000_mean_roll_100 > 95)|
(predicted > pred_10000_mean_roll_2000 + 1.55*pred_10000_sd_roll_2000)|
(predicted > pred_10000_mean_roll_1500 + 1.45*pred_10000_sd_roll_1500)|
(predicted > pred_10000_mean_roll_600 + 1.8*pred_10000_sd_roll_600)|
(predicted > pred_10000_mean_roll_1000 + 1.4*pred_10000_sd_roll_1000)|
(predicted > pred_10000_mean_roll_2000 + 1.8*pred_10000_sd_roll_2000 )|
(pred_10000_mean_roll_50 < 1000 & pred_10000_mean_roll_50 > 90 )|
(pred_10000_mean_roll_10 < 1000 & pred_10000_mean_roll_10 > 90 )
"

trade_statment_EQUITY_ONLY_SIG_1 <-
  "(pred_10000_mean_roll_250 < 45 & pred_10000_mean_roll_250 > 31)|
   (pred_10000_mean_roll_500 < 60 & pred_10000_mean_roll_500 > 47)|
   (pred_10000_mean_roll_100 < 100 & pred_10000_mean_roll_100 > 70)|
   (pred_10000_mean_roll_1000 < 90 & pred_10000_mean_roll_1000 > 75)|
   (pred_10000_mean_roll_50 < 60 & pred_10000_mean_roll_50 > 45)|
   (pred_10000_mean_roll_10 < 80 & pred_10000_mean_roll_10 > 55)|
   (predicted > pred_10000_mean_roll_2000 &
   predicted > pred_10000_mean_roll_1500 &
   predicted > pred_10000_mean_roll_1000 &
   predicted > pred_10000_mean_roll_500 &
   predicted > pred_10000_mean_roll_250 &
   predicted > pred_10000_mean_roll_50 &
   predicted < 100 & predicted > 30)"

equity_1_returns <-
  return_data %>%
  filter(algo_name == "Equity_Port_V3_Bayes_More_vars") %>%
  mutate(
    trade_col = eval(parse(text = trade_statment_Equity_1))
  ) %>%
  mutate(
    trade_col = case_when(trade_col == TRUE ~ "Long", TRUE ~ "No Trade")
  )%>%
  filter(trade_col == "Long") %>%
  dplyr::select(Date, Equity_Return_1 = Final_Return)

equity_2_returns <-
  return_data %>%
  filter(algo_name == "Equity_Port_V3_Bayes_Currency") %>%
  mutate(
    trade_col = eval(parse(text = trade_statment_Equity_2))
  ) %>%
  mutate(
    trade_col = case_when(trade_col == TRUE ~ "Long", TRUE ~ "No Trade")
  ) %>%
  filter(trade_col == "Long") %>%
  dplyr::select(Date, Equity_Return_2 = Final_Return)

currency_returns <-
  return_data %>%
  filter(algo_name == "Currency_Port_V3_USD_Focus") %>%
  mutate(
    trade_col = eval(parse(text = trade_statment_currency))
  ) %>%
  mutate(
    trade_col = case_when(trade_col == TRUE ~ "Long", TRUE ~ "No Trade")
  ) %>%
  filter(trade_col == "Long") %>%
  dplyr::select(Date, Currency_Return = Final_Return)

currency_returns_USD_EUR_ONLY <-
  return_data %>%
  # filter(algo_name == "CURRENCY_USD_EUR_ONLY") %>%
  filter(algo_name == "CURRENCY_USD_EUR_ONLY_SIG_01") %>%
  mutate(
    trade_col = eval(parse(text = trade_statment_Currency_USD_EUR_ONLY))
  ) %>%
  mutate(
    trade_col = case_when(trade_col == TRUE ~ "Long", TRUE ~ "No Trade")
  ) %>%
  filter(trade_col == "Long") %>%
  dplyr::select(Date, Currency_Return_USD_EUR_ONLY = Final_Return)

Equiry_Only_returns <-
  return_data %>%
  filter(algo_name == "Equity_Only_Focus") %>%
  mutate(
    trade_col = eval(parse(text = trade_statment_EQUITY_ONLY_SIG_1))
  ) %>%
  mutate(
    trade_col = case_when(trade_col == TRUE ~ "Long", TRUE ~ "No Trade")
  ) %>%
  filter(trade_col == "Long") %>%
  dplyr::select(Date, Equiry_Only_returns = Final_Return)


return_structure <-
  Date_Tibble %>%
  distinct() %>%
  filter(Date > "2021-09-01") %>%
  left_join(equity_1_returns) %>%
  left_join(equity_2_returns) %>%
  left_join(currency_returns) %>%
  left_join(currency_returns_USD_EUR_ONLY) %>%
  left_join(Equiry_Only_returns) %>%
  distinct() %>%
  mutate(
    across(.cols = c(Equity_Return_2, Currency_Return,
                     Equity_Return_1, Currency_Return_USD_EUR_ONLY,
                     Equiry_Only_returns),
           .fns = ~ ifelse(is.na(.), 0, .))
  ) %>%
  arrange(Date) %>%
  mutate(
    Equity_Return_2_cumulative = cumsum(Equity_Return_2),
    Currency_Return_cumulative = cumsum(Currency_Return),
    Equity_Return_1_cumulative = cumsum(Equity_Return_1),
    Currency_Return_USD_EUR_ONLY_cumulative = cumsum(Currency_Return_USD_EUR_ONLY),
    Equiry_Only_returns_cumulative = cumsum(Equiry_Only_returns)
  ) %>%
  mutate(
    Total_Returns =
      Currency_Return_USD_EUR_ONLY +
      Equity_Return_1 +
      Currency_Return +
      Equiry_Only_returns,
    Total_Returns = cumsum(Total_Returns)
  )

return_structure %>%
  filter(Date > "2021-09-01") %>%
  ggplot(aes(x = Date, y = Total_Returns)) +
  geom_line() +
  scale_y_continuous(labels = scales::label_comma(accuracy = 1), n.breaks = 12) +
  theme_minimal()

return_structure %>%
  filter(Date > "2021-09-01") %>%
  ggplot(aes(x = Date)) +
  geom_line(aes(y = Equity_Return_2_cumulative), color = "black") +
  geom_line(aes(y = Equity_Return_1_cumulative), color = "red") +
  geom_line(aes(y = Currency_Return_cumulative), color = "darkorange") +
  geom_line(aes(y = Currency_Return_USD_EUR_ONLY_cumulative), color = "darkgreen") +
  geom_line(aes(y = Equiry_Only_returns_cumulative), color = "darkblue") +
  theme_minimal()

negative_return_correlation <-
  return_structure %>%
  filter(Date > "2021-12-01") %>%
  filter(Equity_Return_1 < 0)

Currency_Return_USD_EUR_ONLY_rand <- numeric(250000)
Equity_Only_Return_rand <- numeric(250000)
Equity_Return_1_rand <- numeric(250000)
Equity_Return_2_rand <- numeric(250000)
Currency_Return_rand <- numeric(250000)

for (j in 1:250000) {


  sampled_indexes <- round(runif(n = 1,
                                 min = 1,
                                 max = dim(return_structure)[1] - 800 ))

  sampled_indexes <- seq(sampled_indexes, sampled_indexes + 800, 1)

  Equity_Return_1_rand[j] =
    sum(return_structure$Equity_Return_1[sampled_indexes], na.rm = T)
  Equity_Return_2_rand[j] =
    sum(return_structure$Equity_Return_2[sampled_indexes], na.rm = T)
  Currency_Return_rand[j] =
    sum(return_structure$Currency_Return[sampled_indexes], na.rm = T)
  Currency_Return_USD_EUR_ONLY_rand[j] =
    sum(return_structure$Currency_Return_USD_EUR_ONLY[sampled_indexes], na.rm = T)
  Equity_Only_Return_rand[j] =
    sum(return_structure$Equiry_Only_returns[sampled_indexes], na.rm = T)

}

random_return_tibble <-
  list(
    list(Equity_Return_1_rand, "Equity_Return_1_rand"),
    list(Equity_Return_2_rand, "Equity_Return_2_rand"),
    list(Currency_Return_rand, "Currency_Return_rand"),
    list(Currency_Return_USD_EUR_ONLY_rand, "Currency_Return_USD_EUR_ONLY_rand"),
    list(Equity_Only_Return_rand, "Equity_Only_Return_rand")
  ) %>%
  map_dfr(
    ~ tibble(
      returns_sampled = .x[[1]],
      algo = .x[[2]]
    )
  )

random_return_tibble %>%
  ggplot(aes(x = returns_sampled)) +
  geom_density() +
  facet_wrap(.~algo, scales = "free") +
  theme_minimal()

random_return_tibble_sum <-
  random_return_tibble %>%
  group_by(algo) %>%
  summarise(
    low_return = quantile(returns_sampled, 0.05),
    mid_return = quantile(returns_sampled, 0.5),
    high_return = quantile(returns_sampled, 0.95)
  )

random_return_tibble_sum_total <-
  random_return_tibble %>%
  filter(algo != "Equity_Return_2_rand") %>%
  group_by(algo) %>%
  mutate(xx = row_number()) %>%
  ungroup() %>%
  group_by(xx) %>%
  summarise(returns_sampled = sum(returns_sampled, na.rm = TRUE)) %>%
  ungroup() %>%
  summarise(
    low_return = quantile(returns_sampled, 0.05),
    mid_return = quantile(returns_sampled, 0.5),
    high_return = quantile(returns_sampled, 0.95)
  )
