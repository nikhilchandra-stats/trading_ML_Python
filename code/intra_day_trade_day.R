
helperfunctions35South::load_custom_functions()
one_drive_path <- helperfunctions35South::create_one_drive_path(
  path_extension = "raw data")

all_aud_symbols <- get_oanda_symbols() %>%
  keep( ~
          (.x %in% c( "USD_SEK", "USD_ZAR", "USD_NOK", "USD_MXN", "USD_HUF")) |
          str_detect(.x, "AUD")
           )

asset_infor <- get_instrument_info()

aud_conversion_data <-
  read_all_asset_data_intra_day(
    asset_list_oanda = all_aud_symbols,
    save_path_oanda_assets = "C:/Users/Nikhil Chandra/Documents/Asset Data/oanda_data/",
    read_csv_or_API = "API",
    time_frame = "D",
    bid_or_ask = "ask",
    how_far_back = 5000,
    start_date = "2025-01-01"
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

data_list_dfr_long <-
  read_all_asset_data_intra_day(
    asset_list_oanda = asset_list_oanda,
    save_path_oanda_assets = "C:/Users/Nikhil Chandra/Documents/Asset Data/oanda_data/",
    read_csv_or_API = "API",
    time_frame = "H1",
    bid_or_ask = "ask",
    how_far_back = 5000,
    start_date = "2024-09-01"
  )

data_list_dfr_short <-   read_all_asset_data_intra_day(
  asset_list_oanda = asset_list_oanda,
  save_path_oanda_assets = "C:/Users/Nikhil Chandra/Documents/Asset Data/oanda_data/",
  read_csv_or_API = "API",
  time_frame = "H1",
  bid_or_ask = "bid",
  how_far_back = 5000,
  start_date = "2024-09-01"
)

data_list_dfr_short <- data_list_dfr_short %>% map_dfr(bind_rows)
data_list_dfr_long <- data_list_dfr_long %>% map_dfr(bind_rows)


# write.csv(data_list_dfr_long,
#           paste0(save_path, "/extracted_asset_data_h1_ts_ask_2.csv"),
#           row.names = FALSE)
# write.csv(data_list_dfr_short,
#           paste0(save_path, "/extracted_asset_data_h1_ts_bid_2.csv"),
#           row.names = FALSE)

# data_list_dfr_long <- read_csv(paste0(save_path, "/extracted_asset_data_h1_ts_ask_2.csv"))
# data_list_dfr_short <- read_csv(paste0(save_path, "/extracted_asset_data_h1_ts_bid_2.csv"))

data_list_dfr_long <- data_list_dfr_long %>%
  mutate(
    Date = as_datetime(Date)
  )

mean_values_by_asset_for_loop =
  wrangle_asset_data(
    asset_data_daily_raw = data_list_dfr_long,
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

end_time <- glue::glue("{floor_date(now(), 'week')} 11:59:00 AEST") %>% as_datetime(tz = "Australia/Canberra") + days(5)
current_time <- now()
trade_taken_this_hour <- 0
data_list_dfr_long_loop <- data_list_dfr_long %>%
  group_by(Asset) %>%
  arrange(Date, .by_group = TRUE) %>%
  group_by(Asset) %>%
  slice_tail(n = 2500) %>%
  ungroup()
max_date_in_data <- data_list_dfr_long_loop %>%
  group_by(Asset) %>%
  slice_max(Date)

profit_factor1  = 10
stop_factor1  = 6
profit_factor2  = 6
stop_factor2  = 8
risk_dollar_value <- 5
margain_threshold <- 0.05
long_account_num <- 1
account_number_long <- "001-011-1615559-001"
account_name_long <- "primary"

while(current_time < end_time) {

  current_time <- now() %>% as_datetime()
  current_minute <- lubridate::minute(current_time)
  current_hour <- lubridate::hour(current_time)

  if(current_minute > 57 & trade_taken_this_hour == 0) {

    latest_data_for_long <-
      read_all_asset_data_intra_day(
        asset_list_oanda = asset_list_oanda,
        save_path_oanda_assets = "C:/Users/Nikhil Chandra/Documents/Asset Data/oanda_data/",
        read_csv_or_API = "API",
        time_frame = "H1",
        bid_or_ask = "ask",
        how_far_back = 5000,
        start_date = as.character(today() - days(6))
      )
    latest_data_for_long <- latest_data_for_long %>%
      map_dfr(bind_rows) %>%
      mutate(Date = as_datetime(Date))

    data_for_finding_trades_long <-
      data_list_dfr_long_loop %>%
      dplyr::select(-`Vol.`) %>%
      ungroup() %>%
      bind_rows(latest_data_for_long %>% ungroup() %>% dplyr::select(-`Vol.`)  ) %>%
      distinct() %>%
      group_by(Date, Asset) %>%
      mutate(counts = row_number()) %>%
      slice_max(counts)  %>%
      ungroup() %>%
      dplyr::select(-counts)

    trade_data_long <-
      get_markov_tag_bayes_loop(
        asset_data_combined = data_for_finding_trades_long,
        training_perc = 1,
        sd_divides = seq(0.5,2,0.5),
        quantile_divides = seq(0.1,0.9, 0.1),
        rolling_period = 400,
        markov_col_on_interest_pos = "Markov_Point_Pos_roll_sum_1.5",
        markov_col_on_interest_neg = "Markov_Point_Neg_roll_sum_-1.5",
        sum_sd_cut_off = "",
        profit_factor  = profit_factor1,
        stop_factor  = stop_factor1,
        asset_data_daily_raw = data_for_finding_trades_long,
        mean_values_by_asset_for_loop = mean_values_by_asset_for_loop,
        trade_sd_fact_post = 1.5,
        trade_sd_fact_post_high = 1,
        trade_sd_fact_sigma = 0.5,
        bayes_prior = 240,
        bayes_prior_trade = 120,
        asset_infor = asset_infor,
        trade_direction = "Long",
        skip_analysis = FALSE,
        currency_conversion = currency_conversion
      )

    trade_data_long2 <-
      get_markov_tag_bayes_loop(
        asset_data_combined = data_for_finding_trades_long,
        training_perc = 1,
        sd_divides = seq(0.5,2,0.5),
        quantile_divides = seq(0.1,0.9, 0.1),
        rolling_period = 400,
        markov_col_on_interest_pos = "Markov_Point_Pos_roll_sum_1.5",
        markov_col_on_interest_neg = "Markov_Point_Neg_roll_sum_-1.5",
        sum_sd_cut_off = "",
        profit_factor  = profit_factor2,
        stop_factor  = stop_factor2,
        asset_data_daily_raw = data_for_finding_trades_long,
        mean_values_by_asset_for_loop = mean_values_by_asset_for_loop,
        trade_sd_fact_post = 2,
        trade_sd_fact_post_high = 1,
        trade_sd_fact_sigma = 1,
        bayes_prior = 240,
        bayes_prior_trade = 120,
        asset_infor = asset_infor,
        trade_direction = "Long",
        skip_analysis = FALSE,
        currency_conversion = currency_conversion
      )

    tagged_trades_long1 <-
      trade_data_long$tagged_trades %>%
      group_by(Asset) %>%
      slice_max(Date) %>%
      filter(!is.na(trade_col)) %>%
      dplyr::select(Date, Asset, Price, Open, High, trade_col) %>%
      filter(trade_col == "Long") %>%
      bind_rows()
    tagged_trades_long2 <-
      trade_data_long2$tagged_trades %>%
      slice_max(Date) %>%
      filter(!is.na(trade_col)) %>%
      dplyr::select(Date, Asset, Price, Open, High, trade_col) %>%
      filter(trade_col == "Long") %>%
      bind_rows()

    account_list <- get_list_of_accounts()
    account_name_long <- "primary"
    account_number_long <- "001-011-1615559-001"
    get_oanda_account_number(account_name = account_name_long)
    current_trades_long <- get_list_of_positions(account_var = long_account_num)
    current_trades_long <- current_trades_long %>%
      mutate(direction = stringr::str_to_title(direction)) %>%
      rename(Asset = instrument )

    message(glue::glue("Trades to Take {dim(tagged_trades_long1)[1] + dim(tagged_trades_long2)[1]}"))

    if(dim(tagged_trades_long1)[1] > 0 & dim(tagged_trades_long2)[1] > 0) {

      if(dim(tagged_trades_long2)[1] > 0) {
        trades_for_now_long_aud2 <-
          get_stops_profs_volume_trades(mean_values_by_asset = mean_values_by_asset_for_loop,
                                        tagged_trades = tagged_trades_long2,
                                        trade_col = "trade_col",
                                        currency_conversion = currency_conversion,
                                        risk_dollar_value = risk_dollar_value,
                                        stop_factor = stop_factor2,
                                        profit_factor = profit_factor2)

        message(glue::glue("Trades to Take Adj 2: {dim(trades_for_now_long_aud2)[1]}"))

      }

      if(dim(tagged_trades_long1)[1] > 0) {
        trades_for_now_long_aud1 <-
          get_stops_profs_volume_trades(mean_values_by_asset = mean_values_by_asset_for_loop,
                                        tagged_trades = tagged_trades_long1,
                                        trade_col = "trade_col",
                                        currency_conversion = currency_conversion,
                                        risk_dollar_value = risk_dollar_value,
                                        stop_factor = stop_factor1,
                                        profit_factor = profit_factor1)

        message(glue::glue("Trades to Take Adj 1: {dim(trades_for_now_long_aud1)[1]}"))
      }

      trades_for_now_long_aud <-
        trades_for_now_long_aud1 %>%
        bind_rows(trades_for_now_long_aud2) %>%
        filter( abs(volume_required) > 0)  %>%
        left_join(current_trades_long %>%
                    mutate(units  =
                             case_when(
                               direction == "Short" ~ -1*units,
                               direction == "Long" ~ units
                             ))
        ) %>%
        filter(
          trade_col == direction | is.na(direction)
        )

      if(dim(trades_for_now_long_aud)[1] > 0) {

        for (i in 1:dim(trades_for_now_long_aud)[1]) {

          account_details_long <- get_account_summary(account_var = long_account_num)
          margain_available_long <- account_details_long$marginAvailable %>% as.numeric()
          margain_used_long <- account_details_long$marginUsed%>% as.numeric()
          total_margain_long <- margain_available_long + margain_used_long
          percentage_margain_available_long <- margain_available_long/total_margain_long
          Sys.sleep(1)

          if(percentage_margain_available_long > margain_threshold) {

            asset <- trades_for_now_long_aud$Asset[i] %>% as.character()
            volume_trade <- trades_for_now_long_aud$volume_required[i] %>% as.numeric()

            loss_var <- trades_for_now_long_aud$stop_value[i] %>% as.numeric()
            profit_var <- trades_for_now_long_aud$profit_value[i] %>% as.numeric()

            if(loss_var > 9) { loss_var <- round(loss_var)}
            if(profit_var > 9) { profit_var <- round(profit_var)}

            # This is misleading because it is price distance and not pip distance
            http_return <- oanda_place_order_pip_stop(
              asset = asset,
              volume = volume_trade,
              stopLoss = loss_var,
              takeProfit = profit_var,
              type = "MARKET",
              timeinForce = "FOK",
              acc_name = account_name_long,
              position_fill = "OPEN_ONLY" ,
              price
            )

          }

        }

      }
    }

    trade_taken_this_hour <- 1
    current_time <- now() %>% as_datetime()
    current_minute <- lubridate::minute(current_time)
    current_hour <- lubridate::hour(current_time)

    rm(account_details)
    rm(asset)
    rm(loss_var)
    rm(profit_var)
    rm(margain_available)
    rm(margain_used)
    rm(percentage_margain_available)
    rm(total_margain)
    rm(tagged_trades_long1)
    rm(tagged_trades_long2)
    rm(trades_for_now_long_aud)

  }

  if(current_minute > 30 &  current_minute < 40 & trade_taken_this_hour == 1) {
    trade_taken_this_hour <- 0
    message("Trade Counter Reset")
    }



  Sys.sleep(30)

}

#-------------------------------------------------------------------Review Analysis

#--------------------------------------------------------------------------------------
#-------Long H1: Trades = 10k+, Perc = 0.411, profit_factor = 10, stop_factor = 6,
#----------rolling = 400, bayes_prior_trade = 120,bayes_prior = 240
#-posterior_difference <= posterior_difference_mean - trade_sd_fact_post*posterior_difference_sd
#-trade_sd_fact_post = 1.5
#-sigma_difference <= sigma_difference_mean - trade_sd_fact_sigma*sigma_difference_sd
#-trade_sd_fact_sigma = 0.5
#-sigma_n_high>sigma_n_low

data_list_dfr_long <- read_csv(paste0(save_path, "/extracted_asset_data_h1_ts_ask_2.csv"))
data_list_dfr_short <- read_csv(paste0(save_path, "/extracted_asset_data_h1_ts_bid_2.csv"))
mean_values_by_asset_for_loop =
  wrangle_asset_data(
    asset_data_daily_raw = data_list_dfr_long,
    summarise_means = TRUE
  )

profit_factor  = 10
stop_factor  = 6
trade_data_long <-
  get_markov_tag_bayes_loop(
    asset_data_combined = data_list_dfr_long,
    training_perc = 1,
    sd_divides = seq(0.5,2,0.5),
    quantile_divides = seq(0.1,0.9, 0.1),
    rolling_period = 400,
    markov_col_on_interest_pos = "Markov_Point_Pos_roll_sum_1.5",
    markov_col_on_interest_neg = "Markov_Point_Neg_roll_sum_-1.5",
    sum_sd_cut_off = "",
    profit_factor  = profit_factor,
    stop_factor  = stop_factor,
    asset_data_daily_raw = data_list_dfr_long,
    mean_values_by_asset_for_loop = mean_values_by_asset_for_loop,
    trade_sd_fact_post = 1.5,
    trade_sd_fact_post_high = 1,
    trade_sd_fact_sigma = 0.5,
    bayes_prior = 240,
    bayes_prior_trade = 120,
    asset_infor = asset_infor,
    trade_direction = "Long",
    skip_analysis = FALSE,
    risk_dollar_value = 5,
    currency_conversion = currency_conversion
  )

test <- trade_data_long$Markov_Trades_Bayes_Summary

#-------Long H1 fast variant: Trades = 8000, Perc = 0.61, profit_factor = 6, stop_factor = 8,
#----------rolling = 400, bayes_prior_trade = 120,bayes_prior = 240
#-posterior_difference <= posterior_difference_mean - trade_sd_fact_post*posterior_difference_sd
#-trade_sd_fact_post = 2
#-sigma_difference <= sigma_difference_mean - trade_sd_fact_sigma*sigma_difference_sd
#-trade_sd_fact_sigma = 1
#-sigma_n_high>sigma_n_low

profit_factor  = 6
stop_factor  = 8
trade_data_long <-
  get_markov_tag_bayes_loop(
    asset_data_combined = data_list_dfr_long,
    training_perc = 1,
    sd_divides = seq(0.5,2,0.5),
    quantile_divides = seq(0.1,0.9, 0.1),
    rolling_period = 400,
    markov_col_on_interest_pos = "Markov_Point_Pos_roll_sum_1.5",
    markov_col_on_interest_neg = "Markov_Point_Neg_roll_sum_-1.5",
    sum_sd_cut_off = "",
    profit_factor  = profit_factor,
    stop_factor  = stop_factor,
    asset_data_daily_raw = data_list_dfr_long,
    mean_values_by_asset_for_loop = mean_values_by_asset_for_loop,
    trade_sd_fact_post = 2,
    trade_sd_fact_post_high = 1,
    trade_sd_fact_sigma = 1,
    bayes_prior = 240,
    bayes_prior_trade = 120,
    asset_infor = asset_infor,
    trade_direction = "Long",
    skip_analysis = FALSE,
    risk_dollar_value = 5,
    currency_conversion = currency_conversion
  )

test2 <- trade_data_long$Markov_Trades_Bayes_Summary

#-------Long H1 fast variant: Trades = 33,000, Perc = 0.58, profit_factor = 6, stop_factor = 8,
#----------rolling = 600, bayes_prior_trade = 150,bayes_prior = 300
# trade_sd_fact_post = 1.0,
# trade_sd_fact_post_high = 1,
# trade_sd_fact_sigma = 0.25
#-sigma_n_high>sigma_n_low
#-posterior_difference <= posterior_difference_mean - trade_sd_fact_post*posterior_difference_sd
#-sigma_difference <= sigma_difference_mean - trade_sd_fact_sigma*sigma_difference_sd

profit_factor  = 6
stop_factor  = 8
trade_data_long <-
  get_markov_tag_bayes_loop(
    asset_data_combined = data_list_dfr_long,
    training_perc = 1,
    sd_divides = seq(0.5,2,0.5),
    quantile_divides = seq(0.1,0.9, 0.1),
    rolling_period = 600,
    markov_col_on_interest_pos = "Markov_Point_Pos_roll_sum_1.5",
    markov_col_on_interest_neg = "Markov_Point_Neg_roll_sum_-1.5",
    sum_sd_cut_off = "",
    profit_factor  = profit_factor,
    stop_factor  = stop_factor,
    asset_data_daily_raw = data_list_dfr_long,
    mean_values_by_asset_for_loop = mean_values_by_asset_for_loop,
    trade_sd_fact_post = 1.0,
    trade_sd_fact_post_high = 1,
    trade_sd_fact_sigma = 0.25,
    bayes_prior = 300,
    bayes_prior_trade = 150,
    asset_infor = asset_infor,
    trade_direction = "Long",
    skip_analysis = FALSE,
    risk_dollar_value = 5,
    currency_conversion = currency_conversion
  )

test3 <- trade_data_long$Markov_Trades_Bayes_Summary
