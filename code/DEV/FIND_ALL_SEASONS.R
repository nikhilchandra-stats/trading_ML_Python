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
#---------------------Data
load_custom_functions()
db_location = "C:/Users/nikhi/Documents/Asset Data/Oanda_Asset_Data_Most_Assets_2025-09-13.db"
start_date = "2012-01-01"
training_date = "2026-07-01"
end_date = today() %>% as.character()

all_assets_to_port =
  c(
    "SPX500_USD",
    "XAG_USD",
    "DE30_EUR",
    "NATGAS_USD",
    "HK33_HKD",
    "JP225_USD",
    "XAU_USD",
    "USB30Y_USD",
    "USD_JPY",
    "USD_CAD",
    "EUR_AUD",
    "EUR_GBP",
    "CN50_USD",
    "AU200_AUD",
    "UK100_GBP",
    "CH20_CHF",
    "XCU_USD",
    "EU50_EUR",
    "BTC_USD",
    "EU50_EUR"
  ) %>% unique()

stop_factor_var = 5
profit_factor_var = 100
risk_dollar_value_var = 5
end_period = 132
trade_direction = "Long"
end_point_loss = -5
end_point_profit = 100

asset_returns <- list()

for (i in 1:length(all_assets_to_port)) {

  assets_to_port <- all_assets_to_port[i]

  Indices_Metals_Bonds <- list()
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

  asset_returns[[i]] <-
    get_portfolio_model_fast_summed(
      asset_data = Indices_Metals_Bonds %>% map(~ .x %>% filter(Date < training_date)),
      asset_of_interest = assets_to_port,
      stop_factor_var = stop_factor_var,
      profit_factor_var = profit_factor_var,
      risk_dollar_value_var = risk_dollar_value_var,
      end_period = end_period,
      time_frame = "H1",
      trade_direction = trade_direction,
      currency_conversion = currency_conversion,
      asset_infor = asset_infor,
      end_point_loss = end_point_loss,
      end_point_profit = end_point_profit,
      sum_as_portfolio = TRUE,

      overwrite_volume = NULL,
      min_volume_only = FALSE,
      return_only_interested_col = FALSE
      # return_only_Final = TRUE
    ) %>%
    dplyr::select(Date, Asset, end_point_loss, end_point_profit, risk_dollar_value, stop_factor,
                  profit_factor, end_point_point_win, end_point_point_loss, Final_Return)

  gc()

  rm(Indices_Metals_Bonds)
  gc()

}

asset_returns_dfr <-
  asset_returns %>%
  map_dfr(bind_rows)

rm(asset_returns)
gc()

min_values <-
  asset_returns_dfr %>%
  group_by(Asset) %>%
  summarise(
    Date = min(Date, na.rm = T),
    Final_Return_mid = quantile( ifelse(Final_Return < 0, Final_Return, NA) , 0.75 , na.rm = T),
    Final_Return = min(Final_Return,na.rm = T)
  )

summary_returns <-
  asset_returns_dfr %>%
  ungroup() %>%
  filter(Date < '2024-01-01') %>%
  # filter(Asset %in% c("SPX500_USD", "DE30_EUR", "XAU_USD", "USD_JPY", "USD_CAD",
  #                     "JP225_USD", "HK33_HKD",
  #                     "USD_JPY",
  #                     "USD_CAD",
  #                     "EUR_AUD",
  #                     "EUR_GBP", "CH20_CHF", "AU200_AUD", "NATGAS_USD")) %>%
  # filter(Asset %in% c("SPX500_USD", "DE30_EUR",
  #                     "USD_JPY", "XAU_USD", "JP225_USD", "USD_CAD", "BTC_USD")) %>%
  filter(Asset %in% c("SPX500_USD")) %>%
  group_by(Date, end_point_loss, end_point_profit, risk_dollar_value, stop_factor,
           profit_factor, end_point_point_win, end_point_point_loss) %>%
  summarise(Final_Return = sum(Final_Return, na.rm = T)) %>%
  ungroup() %>%
  mutate(Cumulative_Final_Return = cumsum(Final_Return) )

summary_returns %>%
  ggplot(aes(x = Date, y = Cumulative_Final_Return)) +
  geom_line() +
  scale_y_continuous(n.breaks = 12) +
  theme_minimal()

