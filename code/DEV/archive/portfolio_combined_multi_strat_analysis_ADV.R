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
    "USD_JPY",
    "EUR_USD"

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
  connect_db("C:/Users/nikhi/Documents/trade_data/single_asset_v3_Bayes_Reg_Portfolio/Port_V3_Results_Store.db")

return_data <- DBI::dbGetQuery(conn = db_con,
                               statement = "SELECT * FROM Port_V3_Sim_Data" ) %>%
  mutate(
    Date = as_datetime(Date, tz = "Australia/Canberra")
  )

DBI::dbDisconnect(db_con)

Date_Tibble <-
  Indices_Metals_Bonds[[1]] %>%
  distinct(Date)

source("code/trade_statement/trade_statement_v3_Portfolio.R")

distinct_algos <-
  return_data %>%
  distinct(algo_name, trade_statement) %>%
  filter(!is.na(trade_statement))

return_list <- list()

for (i in 1:dim(distinct_algos)[1] ) {

  algo_temp <-
    as.character(distinct_algos$algo_name[i])
  trade_statement_temp <-
    as.character(distinct_algos$trade_statement[i])
  temp_data <-
    return_data %>%
    filter(algo_name == algo_temp) %>%
    mutate(
      trade_col = eval(parse(text = trade_statement_temp)),
      trade_col =
        case_when(
          trade_col == TRUE ~ "Long",
          TRUE ~ "No Trade"
        )
    ) %>%
    dplyr::select(Date, algo_name, Final_Return, trade_col)

  min_date_dat <-
    temp_data %>% pull(Date) %>% min()

  return_list[[i]] <-
    Date_Tibble %>%
    filter(Date >= min_date_dat) %>%
    left_join(temp_data %>%
                filter(trade_col == "Long")
              ) %>%
    mutate(trade_col = ifelse(is.na(trade_col) , "No Trade", "Long" )) %>%
    mutate(
      Final_Return = ifelse(is.na(Final_Return), 0, Final_Return)
    ) %>%
    fill(algo_name, .direction = "updown")

}

return_list_dfr <-
  return_list %>%
  map_dfr(bind_rows)

random_returns_combined_portfolio <-
  distinct_algos %>%
  pull(algo_name) %>%
  map_dfr(
    ~ portfolio_V3_generate_random_returns(return_list_dfr = return_list_dfr,
                                         algo_name = .x,
                                         sample_size = 500000,
                                         time_series_length = 120)
  ) %>%
  group_by(algo) %>%
  mutate(xx = row_number()) %>%
  ungroup() %>%
  group_by(xx) %>%
  summarise(returns_sampled = sum(returns_sampled, na.rm = TRUE)) %>%
  ungroup() %>%
  summarise(
    low_return = quantile(returns_sampled, 0.05, na.rm = T),
    mid_return = mean(returns_sampled, na.rm = T),
    high_return = quantile(returns_sampled, 0.95, na.rm = T),
    sd_return = sd(returns_sampled, na.rm = T)
  )

simulated_52_weeks_rand <- numeric(500000)
for (i in 1:500000) {
  simulated_52_weeks_rand[i] <-
    sum(rnorm(n = 52,
          mean = random_returns_combined_portfolio$mid_return[1],
          sd = random_returns_combined_portfolio$sd_return[1]))
}

tibble(returns_sampled = simulated_52_weeks_rand) %>%
  ungroup() %>%
  summarise(
    low_return = quantile(returns_sampled, 0.01, na.rm = T),
    mid_return = mean(returns_sampled, na.rm = T),
    high_return = quantile(returns_sampled, 0.95, na.rm = T),
    sd_return = sd(returns_sampled, na.rm = T)
  )

tibble(returns_sampled = simulated_52_weeks_rand) %>%
  ggplot(aes(x = returns_sampled)) +
  geom_density() +
  theme_minimal()
