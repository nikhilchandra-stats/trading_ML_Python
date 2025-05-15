

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

asset_infor <- get_instrument_info()


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
db_location = "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data.db"
model_directory = "C:/Users/Nikhil Chandra/Documents/trade_data/"
start_date_day = "2011-01-01"
end_date_day = today() %>% as.character()
LM_model <- readRDS(glue::glue("{model_directory}LM_Markov_Bayes.rds"))
prediction_nn_train_means <-
  read_csv(glue::glue("{model_directory}prediction_nn_train_means.csv"))


starting_asset_data_ask_H1 <-
  get_db_price(
    db_location = db_location,
    start_date = start_date_day,
    end_date = end_date_day,
    bid_or_ask = "ask",
    time_frame = "H1"
  )

asset_list_oanda <- starting_asset_data_ask_H1 %>%
  distinct(Asset) %>% pull(Asset)

mean_values_by_asset_for_loop_H1 =
  wrangle_asset_data(
    asset_data_daily_raw = starting_asset_data_ask_H1,
    summarise_means = TRUE
  )

asset_data_for_loop_ask <-
  update_data_internal_bayes(starting_data = starting_asset_data_ask_H1)

end_time <- glue::glue("{floor_date(now(), 'week')} 11:59:00 AEST") %>% as_datetime(tz = "Australia/Canberra") + days(5)
current_time <- now()
trade_taken_this_hour <- 0
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

    #------------------Update the Data
    asset_data_for_loop_ask <-
      update_data_internal_bayes(starting_data = asset_data_for_loop_ask)

    profit_factor  <- 8
    stop_factor  <- 8
    risk_dollar_value <- 5
    for_trade_extraction <-
      asset_data_for_loop_ask %>%
      group_by(Asset) %>%
      arrange(Date, .by_group = TRUE) %>%
      slice_tail(n = 5000)

    current_max_data_date <-
      asset_data_for_loop_ask%>%
      slice_max(Date) %>% pull(Date) %>% unique() %>% as_datetime()

    tictoc::tic()
    trade_data_long <-
      get_markov_tag_bayes_loop_LM_ML(
        asset_data_combined = for_trade_extraction,
        training_perc = 1,
        sd_divides = seq(0.25,2,0.25),
        quantile_divides = seq(0.1,0.9, 0.1),
        rolling_period = 400,
        markov_col_on_interest_pos = "Markov_Point_Pos_roll_sum_1.5",
        markov_col_on_interest_neg = "Markov_Point_Neg_roll_sum_-1.5",
        sum_sd_cut_off = "",
        profit_factor  = profit_factor,
        stop_factor  = stop_factor,
        asset_data_daily_raw = for_trade_extraction,
        mean_values_by_asset_for_loop = mean_values_by_asset_for_loop_H1,
        trade_sd_fact_post = 3,
        bayes_prior = 300,
        bayes_prior_trade = 270,
        asset_infor = asset_infor,
        trade_direction = "Long",
        skip_analysis = TRUE,
        risk_dollar_value = 5,
        currency_conversion = currency_conversion,
        LM_model = LM_model,
        LM_mean_preds = LM_mean_preds
      )
    tictoc::toc()

    total_trades <-
      list(trade_data_long) %>%
      map_dfr(
       ~ prepare_trades_markov_bayes_LM(
          tagged_trades = .x,
          current_max_date = current_max_data_date,
          currency_conversion = currency_conversion,
          mean_values_by_asset_for_loop_H1 = mean_values_by_asset_for_loop_H1,
          stop_factor = stop_factor,
          profit_factor = profit_factor,
          risk_dollar_value = risk_dollar_value
        )
      )

    if(dim(total_trades)[1] > 0) {

      for (i in 1:dim(total_trades)[1]) {

        account_details_long <- get_account_summary(account_var = long_account_num)
        margain_available_long <- account_details_long$marginAvailable %>% as.numeric()
        margain_used_long <- account_details_long$marginUsed%>% as.numeric()
        total_margain_long <- margain_available_long + margain_used_long
        percentage_margain_available_long <- margain_available_long/total_margain_long
        Sys.sleep(1)

        if(percentage_margain_available_long[1] > margain_threshold) {

          asset <- total_trades$Asset[i] %>% as.character()
          volume_trade <- total_trades$volume_required[i] %>% as.numeric()

          loss_var <- total_trades$stop_value[i] %>% as.numeric()
          profit_var <- total_trades$profit_value[i] %>% as.numeric()

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

db_location = "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data.db"
model_directory = "C:/Users/Nikhil Chandra/Documents/trade_data/"
start_date_day = "2011-01-01"
end_date_day = today() %>% as.character()
LM_model <- readRDS(glue::glue("{model_directory}LM_Markov_Bayes.rds"))
prediction_nn_train_means <-
  read_csv(glue::glue("{model_directory}prediction_nn_train_means.csv"))


starting_asset_data_ask_H1 <-
  get_db_price(
    db_location = db_location,
    start_date = start_date_day,
    end_date = end_date_day,
    bid_or_ask = "ask",
    time_frame = "H1"
  )

mean_values_by_asset_for_loop_H1 =
  wrangle_asset_data(
    asset_data_daily_raw = starting_asset_data_ask_H1,
    summarise_means = TRUE
  )

profit_factor  = 8
stop_factor  = 8
for_trade_extraction <-
  starting_asset_data_ask_H1 %>%
  group_by(Asset) %>%
  arrange(Date, .by_group = TRUE) %>%
  slice_tail(n = 5000)

tictoc::tic()
trade_data_long <-
  get_markov_tag_bayes_loop_LM_ML(
    asset_data_combined = starting_asset_data_ask_H1,
    training_perc = 1,
    sd_divides = seq(0.25,2,0.25),
    quantile_divides = seq(0.1,0.9, 0.1),
    rolling_period = 400,
    markov_col_on_interest_pos = "Markov_Point_Pos_roll_sum_1.5",
    markov_col_on_interest_neg = "Markov_Point_Neg_roll_sum_-1.5",
    sum_sd_cut_off = "",
    profit_factor  = profit_factor,
    stop_factor  = stop_factor,
    asset_data_daily_raw = starting_asset_data_ask_H1,
    mean_values_by_asset_for_loop = mean_values_by_asset_for_loop_H1,
    trade_sd_fact_post = 3,
    bayes_prior = 300,
    bayes_prior_trade = 270,
    asset_infor = asset_infor,
    trade_direction = "Long",
    skip_analysis = FALSE,
    risk_dollar_value = 5,
    currency_conversion = currency_conversion,
    LM_model = LM_model,
    LM_mean_preds = LM_mean_preds
  )
tictoc::toc()
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
    asset_data_combined = asset_data_combined,
    training_perc = 1,
    sd_divides = seq(0.25,2,0.25),
    quantile_divides = seq(0.1,0.9, 0.1),
    rolling_period = 400,
    markov_col_on_interest_pos = "Markov_Point_Pos_roll_sum_1.5",
    markov_col_on_interest_neg = "Markov_Point_Neg_roll_sum_-1.5",
    sum_sd_cut_off = "",
    profit_factor  = 5,
    stop_factor  = 2,
    asset_data_daily_raw = asset_data_combined,
    mean_values_by_asset_for_loop = mean_values_by_asset_for_loop,
    trade_sd_fact_post = 1,
    bayes_prior = 300,
    bayes_prior_trade = 270,
    asset_infor = asset_infor,
    trade_direction = "Long",
    skip_analysis = FALSE,
    risk_dollar_value = 10,
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
