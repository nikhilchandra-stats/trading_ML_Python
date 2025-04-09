helperfunctions35South::load_custom_functions()
one_drive_path <- helperfunctions35South::create_one_drive_path(
  path_extension = "raw data")
library(neuralnet)

bind_pois_to_daily_price_temp <- function(raw_pois_data = raw_pois_data,
                                     asset_data_daily_raw = asset_data_daily_raw,
                                     rolling_period_var = 5,
                                     prior_period_var = 5,
                                     prior_weight_var = 0.5,
                                     sd_fac_low = 0.5,
                                     sd_fac_high = 1) {

  tagged_trades <-
    raw_pois_data %>%
    filter(rolling_period %in% c(rolling_period_var),
           prior_period %in% c(prior_period_var),
           prior_weight %in% c(prior_weight_var) ) %>%
    mutate(
      trade_col =
        case_when(
          # Pois_Change > 0.07386647 +  sd_fac_low*0.3984324 &
          #   Pois_Change <= 0.07386647 + sd_fac_high*0.3984324 ~ "Long",
          #
          # Pois_Change < 0.07386647 -  sd_fac_low*0.3984324 &
          #   Pois_Change >= 0.07386647 -  sd_fac_high*0.3984324 ~ "Short"

          Pois_Change > median(Pois_Change, na.rm = T) +  sd_fac_low*sd(Pois_Change, na.rm = T) &
            Pois_Change <= median(Pois_Change, na.rm = T) + sd_fac_high*sd(Pois_Change, na.rm = T) ~ "Long",

          Pois_Change < median(Pois_Change, na.rm = T) -  sd_fac_low*sd(Pois_Change, na.rm = T) &
            Pois_Change >= median(Pois_Change, na.rm = T) -  sd_fac_high*sd(Pois_Change, na.rm = T) ~ "Short"

        )
    )
    # mutate(
    #   # week_date =
    #   #   case_when(
    #   #     week_date == max(week_date, na.rm = T) ~ week_date + lubridate::days(1),
    #   #     TRUE ~ week_date
    #   #   )
    #   week_date = week_date + lubridate::days(1)
    # )

  tagged_trades_with_Price <- asset_data_daily_raw %>%
    left_join(
      tagged_trades,
      by = c("Asset", "Date" = "week_date")
    ) %>%
    mutate(
      sd_fac_low = sd_fac_low,
      sd_fac_high = sd_fac_high
    )

  return(tagged_trades_with_Price)

}


asset_list_oanda <- get_oanda_symbols() %>%
  keep( ~ .x %in% c("HK33_HKD", "USD_JPY", "BTC_USD", "AUD_NZD", "GBP_CHF",
                    "EUR_HUF", "EUR_ZAR", "NZD_JPY", "EUR_NZD", "USB02Y_USD",
                    "XAU_CAD", "GBP_JPY", "EUR_NOK", "USD_SGD", "EUR_SEK", "DE30_EUR",
                    "AUD_CAD", "UK10YB_GBP", "XPD_USD", "UK100_GBP", "USD_CHF", "GBP_NZD",
                    "GBP_SGD", "USD_SEK", "EUR_SGD", "XCU_USD", "SUGAR_USD", "CHF_ZAR",
                    "AUD_CHF", "EUR_CHF", "USD_MXN", "GBP_USD", "WTICO_USD", "EUR_JPY", "USD_NOK",
                    "XAU_USD", "DE10YB_EUR", "USD_CZK", "AUD_SGD", "USD_HUF", "WHEAT_USD",
                    "EUR_USD", "SG30_SGD", "GBP_AUD", "NZD_CAD", "AU200_AUD", "XAG_USD",
                    "XAU_EUR", "EUR_GBP", "USD_CNH", "USD_CAD", "NAS100_USD", "USB10Y_USD",
                    "EU50_EUR", "NATGAS_USD", "CAD_JPY", "FR40_EUR", "USD_ZAR", "XAU_GBP",
                    "CH20_CHF", "ESPIX_EUR", "XPT_USD", "EUR_AUD", "SOYBN_USD", "US2000_USD",
                    "BCO_USD")
  )

asset_infor <- get_instrument_info()

extracted_asset_data <- list()
save_path_oanda_assets <- "C:/Users/Nikhil Chandra/Documents/Asset Data/oanda_data/"
for (i in 1:length(asset_list_oanda)) {

  extracted_asset_data[[i]] <-
    get_oanda_data_candles_normalised(
      assets = c(asset_list_oanda[i]),
      granularity = "D",
      date_var = "2011-01-01",
      date_var_start = NULL,
      time = "T15:00:00.000000000Z",
      how_far_back = 5000,
      bid_or_ask = "bid",
      sleep_time = 0
    ) %>%
    mutate(
      Asset = asset_list_oanda[i]
    )

  write.csv(
    x = extracted_asset_data[[i]],
    file= glue::glue("{save_path_oanda_assets}{asset_list_oanda[i]}.csv"),
    row.names = FALSE
  )

}

asset_data_combined_dily <- extracted_asset_data %>% map_dfr(bind_rows)


rolling_periods <- c(5,30,60,100,150,200, 250, 300)
prior_periods <- c(5,30,60,100,150,200, 250, 300)
prior_weights <- c(0.2,0.5,1,1.5, 2, 2.5)
db_pois_data <- "C:/Users/Nikhil Chandra/Documents/trade_data/Pois_Tagged_TimeSeries_2025-04-08.db"
db_con <- connect_db(db_pois_data)
c = 0
for (j in 1:length(rolling_periods)) {
  for (i in 1:length(prior_periods)) {
    for (k in 1:length(prior_weights)) {
      c = c + 1
      temp <- get_pois_calc(
        asset_data = asset_data_combined_dily,
        rolling_period = rolling_periods[j],
        lm_dependant_var = "Weekly_Close_to_Close",
        independant_var = c("Pois_Change"),
        prior_weight = prior_weights[k],
        prior_period = prior_periods[i]
      )

      if(c == 1){
        write_table_sql_lite(conn = db_con,
                             .data = temp,
                             table_name = "raw_pois_data",
                             overwrite_true = TRUE)
        db_con <- connect_db(db_pois_data)
      } else {
        append_table_sql_lite(conn = db_con,
                              .data = temp,
                              table_name = "raw_pois_data")
      }

    }
  }
}


pois_temp <- DBI::dbGetQuery(conn = db_con,
                             "SELECT * FROM raw_pois_data") %>%
  distinct()

pois_temp <- pois_temp %>%
  mutate(week_date = as_date(week_date))

sd_fac_low <- 0.5
sd_fac_high <- 200
stop_fac <- 2.5
prof_fac <- 5

asset_data_daily_raw <- asset_data_combined_dily

mean_values_by_asset_for_loop =
  wrangle_asset_data(
    asset_data_daily_raw = asset_data_daily_raw,
    summarise_means = TRUE
  )

gc()

trade_frame <-
  bind_pois_to_daily_price_temp(
  raw_pois_data = pois_temp,
  asset_data_daily_raw = asset_data_daily_raw,
  rolling_period_var = c(rolling_periods),
  prior_period_var = c(prior_periods),
  prior_weight_var = c(prior_weights),
  sd_fac_low = sd_fac_low,
  sd_fac_high = sd_fac_high
)

trade_results <-
  generic_trade_finder_conservative(
    tagged_trades = trade_frame,
    asset_data_daily_raw = asset_data_daily_raw,
    stop_factor = stop_fac,
    profit_factor = prof_fac,
    trade_col = "trade_col",
    date_col = "Date",
    max_hold_period = 100,
    start_price_col = "Price",
    mean_values_by_asset = mean_values_by_asset_for_loop,
    return_summary = TRUE,
    additional_grouping_vars = c("Asset",  "rolling_period", "prior_period", "prior_weight")
  ) %>%
  map_dfr(bind_rows)

trade_results_sum <- trade_results %>%
  group_by(stop_factor, profit_factor,
           rolling_period, prior_period, prior_weight, trade_direction, trade_category) %>%
  summarise(
    wins = sum(Trades, na.rm = T)
  ) %>%
  # pivot_wider(names_from = trade_category, values_from = wins)%>%
  # mutate(across(.cols = c(`TRUE WIN`, `TRUE LOSS`),
  #               .fns = ~ ifelse(is.na(.), 0, .))) %>%
  # mutate(
  #   Total_Trades = `TRUE WIN` + `TRUE LOSS`,
  #   Perc =  `TRUE WIN`/Total_Trades
  # )
  pivot_wider(names_from = trade_category, values_from = wins)%>%
  mutate(across(.cols = c(`TRUE WIN`, `TRUE LOSS`, `NA`),
                .fns = ~ ifelse(is.na(.), 0, .))) %>%
  mutate(
    Total_Trades = `TRUE WIN` + `TRUE LOSS` + `NA`,
    Perc =  `TRUE WIN`/Total_Trades
  )

trade_results_sum_filt <- trade_results_sum %>%
  filter(Total_Trades > 50) %>%
  group_by(trade_direction) %>%
  slice_max(Perc, n = 5)

write.csv(trade_results_sum_filt,
          file = "data/Poisson_Trade_Results.csv",
          row.names = FALSE)

#-------------
join_trade_infor <-
  trade_results_sum_filt %>%
  distinct(stop_factor, profit_factor, rolling_period, prior_period ,prior_weight, trade_direction, Perc) %>%
  rename(trade_col = trade_direction)

latest_trades <- trade_frame %>%
  filter(Date == "2025-04-06") %>%
  left_join(join_trade_infor) %>%
  filter(!is.na(trade_col))

latest_trades_long <- latest_trades %>%
  filter(!is.na(Perc)) %>%
  left_join(mean_values_by_asset_for_loop)  %>%
  left_join(asset_infor %>% rename(Asset = name))%>%
  mutate(
    profit_point = Price + mean_daily + sd_daily*profit_factor,
    stop_point = Price - mean_daily + sd_daily*stop_factor,

    profit_points =  mean_daily + sd_daily*profit_factor,
    stop_points = mean_daily + sd_daily*stop_factor,

    profit_points_pip =  profit_points/(10^pipLocation),
    stop_points_pip = stop_points/(10^pipLocation)
  )


