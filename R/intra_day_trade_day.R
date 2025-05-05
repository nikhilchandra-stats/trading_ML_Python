
helperfunctions35South::load_custom_functions()
one_drive_path <- helperfunctions35South::create_one_drive_path(
  path_extension = "raw data")
library(neuralnet)

all_aud_symbols <- get_oanda_symbols() %>%
  keep(~ str_detect(.x, "AUD"))
all_aud_symbols <- get_oanda_symbols() %>%
  keep( ~ .x %in% c(all_aud_symbols, "USD_SEK", "USD_ZAR", "USD_NOK", "USD_MXN", "USD_HUF"))

asset_infor <- get_instrument_info()

aud_conversion_data <-
  read_all_asset_data_intra_day(
    asset_list_oanda = all_aud_symbols,
    save_path_oanda_assets = "C:/Users/Nikhil Chandra/Documents/Asset Data/oanda_data/",
    read_csv_or_API = "API",
    time_frame = "D",
    bid_or_ask = "ask",
    how_far_back = 5000,
    start_date = "2011-01-01"
  )
aud_conversion_data <- aud_conversion_data %>% map_dfr(bind_rows)
aud_usd_today <- get_aud_conversion(asset_data_daily_raw = aud_conversion_data)

currency_conversion <-
  aud_usd_today %>%
  mutate(
    not_aud_asset = ending_value
  ) %>%
  dplyr::select(not_aud_asset, adjusted_conversion) %>%
  bind_rows(
    tibble(not_aud_asset = "AUD", adjusted_conversion = 1)
  )


#---------------------------------------------------

asset_list_oanda <- get_oanda_symbols() %>%
  keep( ~ .x %in% c("USD_JPY", "GBP_JPY", "USD_SGD", "EUR_SEK",
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
                    "JP225_USD", "SPX500_USD")
  )

asset_infor <- get_instrument_info()

save_path <- "C:/Users/Nikhil Chandra/Documents/trade_data/quick_load/"

data_list_dfr <- get_intra_day_asset_data(asset_list_oanda = asset_list_oanda,
                                          bid_or_ask = "ask")

write.csv(data_list_dfr,
          paste0(save_path, "/extracted_asset_data_h1_ts_ask_2.csv"),
          row.names = FALSE)

data_list_dfr <- read_csv(paste0(save_path, "/extracted_asset_data_h1_ts_ask_2.csv"))

data_list_dfr <- data_list_dfr %>%
  mutate(
    Date = as_datetime(Date)
  )

aud_usd_today <- get_aud_conversion(asset_data_daily_raw = data_list_dfr)

currency_conversion <-
  aud_usd_today %>%
  mutate(
    not_aud_asset = ending_value
  ) %>%
  dplyr::select(not_aud_asset, adjusted_conversion) %>%
  bind_rows(
    tibble(not_aud_asset = "AUD", adjusted_conversion = 1)
  )

mean_values_by_asset_for_loop =
  wrangle_asset_data(
    asset_data_daily_raw = data_list_dfr,
    summarise_means = TRUE
  )

#--------------------------------------------------------------------------------------
#-------Long H1: Trades = 10k+, Perc = 0.411, profit_factor = 10, stop_factor = 6,
#----------rolling = 400, bayes_prior_trade = 120,bayes_prior = 240
#-posterior_difference <= posterior_difference_mean - trade_sd_fact_post*posterior_difference_sd
#-trade_sd_fact_post = 1.5
#-sigma_difference <= sigma_difference_mean - trade_sd_fact_sigma*sigma_difference_sd
#-trade_sd_fact_sigma = 0.5
#-sigma_n_high>sigma_n_low

#-------Long H1 fast variant: Trades = 8000, Perc = 0.61, profit_factor = 6, stop_factor = 8,
#----------rolling = 400, bayes_prior_trade = 120,bayes_prior = 240
#-posterior_difference <= posterior_difference_mean - trade_sd_fact_post*posterior_difference_sd
#-trade_sd_fact_post = 2
#-sigma_difference <= sigma_difference_mean - trade_sd_fact_sigma*sigma_difference_sd
#-trade_sd_fact_sigma = 1
#-sigma_n_high>sigma_n_low
bayes_data <- get_markov_tag_bayes_loop(
  asset_data_combined = data_for_finding_trades,
  training_perc = 1,
  sd_divides = seq(0.5,2,0.5),
  quantile_divides = seq(0.1,0.9, 0.1),
  rolling_period = 400,
  markov_col_on_interest_pos = "Markov_Point_Pos_roll_sum_1.5",
  markov_col_on_interest_neg = "Markov_Point_Neg_roll_sum_-1.5",
  sum_sd_cut_off = "",
  profit_factor  = 10,
  stop_factor  = 6,
  asset_data_daily_raw = data_for_finding_trades,
  mean_values_by_asset_for_loop = mean_values_by_asset_for_loop,
  trade_sd_fact_post = 1.5,
  trade_sd_fact_post_high = 1,
  trade_sd_fact_sigma = 0.5,
  bayes_prior = 240,
  bayes_prior_trade = 120,
  asset_infor = asset_infor,
  trade_direction = "Long",
  skip_analysis = FALSE
)
static_bayes_probs <- bayes_data$Markov_Trades_Bayes_Summary

end_time <- glue::glue("{floor_date(now(), 'week')} 11:59:00 AEST") %>% as_datetime(tz = "Australia/Canberra") + days(5)
current_time <- now()
trade_taken_this_hour <- 0
while(current_time < end_time) {

  current_time <- now() %>% as_datetime()
  current_minute <- lubridate::minute(current_time)
  current_hour <- lubridate::hour(current_time)

  if(current_minute > 57) {
    latest_data_for_long <-
      read_all_asset_data_intra_day(
        asset_list_oanda = asset_list_oanda,
        save_path_oanda_assets = "C:/Users/Nikhil Chandra/Documents/Asset Data/oanda_data/",
        read_csv_or_API = "API",
        time_frame = "H1",
        bid_or_ask = "ask",
        how_far_back = 5000,
        start_date = as.character(today() - days(3))
      )
    latest_data_for_long <- latest_data_for_long %>%
      map_dfr(bind_rows) %>%
      mutate(Date = as_datetime(Date))

    data_for_finding_trades <-
      data_list_dfr %>%
      bind_rows(latest_data_for_long) %>%
      distinct()
  }

}



