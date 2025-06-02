helperfunctions35South::load_custom_functions()
one_drive_path <- helperfunctions35South::create_one_drive_path(
  path_extension = "raw data")
library(neuralnet)
raw_macro_data <- get_macro_event_data()
eur_data <- get_EUR_exports()
AUD_exports_total <- get_AUS_exports()  %>%
  pivot_longer(-TIME_PERIOD, names_to = "category", values_to = "Aus_Export") %>%
  rename(date = TIME_PERIOD) %>%
  group_by(date) %>%
  summarise(Aus_Export = sum(Aus_Export, na.rm = T))
USD_exports_total <- get_US_exports()  %>%
  pivot_longer(-date, names_to = "category", values_to = "US_Export") %>%
  group_by(date) %>%
  summarise(US_Export = sum(US_Export, na.rm = T)) %>%
  left_join(AUD_exports_total) %>%
  ungroup()
USD_exports_total <- USD_exports_total %>%
  mutate(
    month_date = lubridate::floor_date(date, "month")
  )
AUD_exports_total <- AUD_exports_total %>%
  mutate(
    month_date = lubridate::floor_date(date, "month")
  )
all_aud_symbols <- get_oanda_symbols() %>%
  keep(~ str_detect(.x, "AUD")|str_detect(.x, "USD_SEK|USD_NOK|USD_HUF|USD_ZAR|USD_CNY|USD_MXN|USD_CNH"))
asset_infor <- get_instrument_info()
aud_assets <- read_all_asset_data_intra_day(
  asset_list_oanda = all_aud_symbols,
  save_path_oanda_assets = "C:/Users/Nikhil Chandra/Documents/Asset Data/oanda_data/",
  read_csv_or_API = "API",
  time_frame = "D",
  bid_or_ask = "bid",
  how_far_back = 10,
  start_date = (today() - days(2)) %>% as.character()
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

#---------------------------------------------Daily Regression Join
db_location = "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data.db"
start_date_day = "2011-01-01"
end_date_day = today() %>% as.character()

H1_model_High_SD_25_71_neg <- readRDS(
  glue::glue("C:/Users/Nikhil Chandra/Documents/trade_data/H1_LM_Markov_NN_25_SD_71Perc_2025-05-13.rds")
)

H1_LM_Markov_NN_Long_56_prof_10_4sd2025_05_17 <- readRDS(
  glue::glue("C:/Users/Nikhil Chandra/Documents/trade_data/H1_LM_Markov_NN_Long_56_prof_10_4sd2025-05-17.rds")
)

H1_LM_Markov_NN_Hidden35 <-
  readRDS(glue::glue("C:/Users/Nikhil Chandra/Documents/trade_data/H1_LM_Markov_NN_Hidden35_withLag_2025-05-17.rds"))

LM_ML_Lead_5_21_21_5_layer =
  readRDS("C:/Users/Nikhil Chandra/Documents/trade_data/LM_ML_Lead_5_21_21_5_layer_2025-05-20.rds")


#-------------------------------Trade Loop
asset_list_oanda =
  c("USD_JPY", "GBP_JPY", "USD_SGD", "EUR_SEK",
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

end_time <- glue::glue("{floor_date(now(), 'week')} 11:59:00 AEST") %>% as_datetime(tz = "Australia/Canberra") + days(5)
current_time <- now()
trade_taken_this_hour <- 0
data_updated <- 0

margain_threshold <- 0.05
long_account_num <- 1
account_number_long <- "001-011-1615559-001"
account_name_long <- "primary"

short_account_num <- 3
account_number_short <- "001-011-1615559-004"
account_name_short <- "corr_no_macro"

db_location = "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data.db"
start_date_day = "2011-01-01"
end_date_day = today() %>% as.character()

starting_asset_data_ask_daily <-
  get_db_price(
    db_location = db_location,
    start_date = start_date_day,
    end_date = end_date_day,
    bid_or_ask = "ask",
    time_frame = "D"
  )

starting_asset_data_ask_H1 <-
  get_db_price(
    db_location = db_location,
    start_date = start_date_day,
    end_date = end_date_day,
    bid_or_ask = "ask",
    time_frame = "H1"
  )

mean_values_by_asset_for_loop_D_ask =
  wrangle_asset_data(
    asset_data_daily_raw = starting_asset_data_ask_daily,
    summarise_means = TRUE
  )

mean_values_by_asset_for_loop_H1_ask =
  wrangle_asset_data(
    asset_data_daily_raw = starting_asset_data_ask_H1,
    summarise_means = TRUE
  )

starting_asset_data_bid_daily <-
  get_db_price(
    db_location = db_location,
    start_date = start_date_day,
    end_date = end_date_day,
    bid_or_ask = "bid",
    time_frame = "D"
  )

starting_asset_data_bid_H1 <-
  get_db_price(
    db_location = db_location,
    start_date = start_date_day,
    end_date = end_date_day,
    bid_or_ask = "bid",
    time_frame = "H1"
  )

mean_values_by_asset_for_loop_D_bid =
  wrangle_asset_data(
    asset_data_daily_raw = starting_asset_data_bid_daily,
    summarise_means = TRUE
  )

mean_values_by_asset_for_loop_H1_bid =
  wrangle_asset_data(
    asset_data_daily_raw = starting_asset_data_bid_H1,
    summarise_means = TRUE
  )

#-----Upload new Data to DB
update_local_db_file(
  db_location = db_location,
  time_frame = "D",
  bid_or_ask = "ask"
)
update_local_db_file(
  db_location = db_location,
  time_frame = "H1",
  bid_or_ask = "ask"
)

update_local_db_file(
  db_location = db_location,
  time_frame = "D",
  bid_or_ask = "bid",
  asset_list_oanda = asset_list_oanda
)
update_local_db_file(
  db_location = db_location,
  time_frame = "H1",
  bid_or_ask = "bid",
  asset_list_oanda = asset_list_oanda
)

#------------------------------------------------------------Loop
#------------------------------------------------------------
while (current_time < end_time) {

  current_time <- now() %>% as_datetime()
  current_minute <- lubridate::minute(current_time)
  current_hour <- lubridate::hour(current_time)
  current_date <- now() %>% as_date(tz = "Australia/Canberra")

  #----------------------Refresh Data Stores and LM model
  if(current_minute > 0 & current_minute < 3 & data_updated == 0) {

    update_local_db_file(
      db_location = db_location,
      time_frame = "D",
      bid_or_ask = "ask",
      asset_list_oanda = asset_list_oanda
    )
    update_local_db_file(
      db_location = db_location,
      time_frame = "H1",
      bid_or_ask = "ask",
      asset_list_oanda = asset_list_oanda
    )

    update_local_db_file(
      db_location = db_location,
      time_frame = "D",
      bid_or_ask = "bid",
      asset_list_oanda = asset_list_oanda
    )
    update_local_db_file(
      db_location = db_location,
      time_frame = "H1",
      bid_or_ask = "bid",
      asset_list_oanda = asset_list_oanda
    )

    data_updated <- 1

    raw_macro_data <- get_macro_event_data()

    new_daily_data_ask <-
      updated_data_internal(starting_asset_data = starting_asset_data_ask_daily,
                            end_date_day = current_date,
                            time_frame = "D", bid_or_ask = "ask") %>%
      distinct()
    new_H1_data_ask <-
      updated_data_internal(starting_asset_data = starting_asset_data_ask_H1,
                            end_date_day = current_date,
                            time_frame = "H1", bid_or_ask = "ask")%>%
      distinct()

    new_daily_data_bid <-
      updated_data_internal(starting_asset_data = starting_asset_data_bid_daily,
                            end_date_day = current_date,
                            time_frame = "D", bid_or_ask = "bid") %>%
      distinct()
    new_H1_data_bid <-
      updated_data_internal(starting_asset_data = starting_asset_data_bid_H1,
                            end_date_day = current_date,
                            time_frame = "H1", bid_or_ask = "bid")%>%
      distinct()

    Hour_data_with_LM_ask <-
      run_LM_join_to_H1(
        daily_data_internal = new_daily_data_ask,
        H1_data_internal = new_H1_data_ask,
        raw_macro_data = raw_macro_data,
        AUD_exports_total = AUD_exports_total,
        USD_exports_total = USD_exports_total,
        eur_data = eur_data
      )

    Hour_data_with_LM_markov_ask <-
      extract_required_markov_data(
        Hour_data_with_LM = Hour_data_with_LM_ask,
        new_daily_data_ask = new_daily_data_ask,
        currency_conversion = currency_conversion,
        mean_values_by_asset_for_loop = mean_values_by_asset_for_loop_D_ask,
        profit_factor  = 5,
        stop_factor  = 3,
        risk_dollar_value = 5,
        trade_sd_fact = 2
      )

    Hour_data_with_LM_bid <-
      run_LM_join_to_H1(
        daily_data_internal = new_daily_data_bid,
        H1_data_internal = new_H1_data_bid,
        raw_macro_data = raw_macro_data,
        AUD_exports_total = AUD_exports_total,
        USD_exports_total = USD_exports_total,
        eur_data = eur_data
      )

    Hour_data_with_LM_markov_bid <-
      extract_required_markov_data(
        Hour_data_with_LM = Hour_data_with_LM_bid,
        new_daily_data_ask = new_daily_data_bid,
        currency_conversion = currency_conversion,
        mean_values_by_asset_for_loop = mean_values_by_asset_for_loop_D_bid,
        profit_factor  = 5,
        stop_factor  = 3,
        risk_dollar_value = 5,
        trade_sd_fact = 2
      )

    trades_1 <-
      get_NN_best_trades_from_mult_anaysis(
        db_path = "C:/Users/Nikhil Chandra/Documents/trade_data/NN_simulation_results 2025-05-24 Trading Copy.db",
        network_name = "H1_LM_Markov_NN_Long_56_prof_10_4sd2025_05_17",
        NN_model = H1_LM_Markov_NN_Long_56_prof_10_4sd2025_05_17,
        Hour_data_with_LM_markov = Hour_data_with_LM_markov_ask,
        mean_values_by_asset_for_loop_H1 = mean_values_by_asset_for_loop_H1_ask,
        currency_conversion = currency_conversion,
        asset_infor = asset_infor,
        risk_dollar_value = 10,
        win_threshold = 0.6,
        slice_max = TRUE
      )

    trades_1_50 <-
      get_NN_best_trades_from_mult_anaysis(
        db_path = "C:/Users/Nikhil Chandra/Documents/trade_data/NN_simulation_results 2025-05-24 50% highProf.db",
        network_name = "H1_LM_Markov_NN_Long_56_prof_10_4sd2025_05_17",
        NN_model = H1_LM_Markov_NN_Long_56_prof_10_4sd2025_05_17,
        Hour_data_with_LM_markov = Hour_data_with_LM_markov_ask,
        mean_values_by_asset_for_loop_H1 = mean_values_by_asset_for_loop_H1_ask,
        currency_conversion = currency_conversion,
        asset_infor = asset_infor,
        risk_dollar_value = 10,
        win_threshold = 0.2,
        risk_weighted_thresh = 0.6,
        slice_max = TRUE
      )

    if(!is.null(trades_1)) {trades_1 <- trades_1 %>% filter(trade_col == "Long")}
    if(!is.null(trades_1_50)) {trades_1_50 <- trades_1_50 %>% filter(trade_col == "Long")}

    trades_2 <-
      get_NN_best_trades_from_mult_anaysis(
        db_path = "C:/Users/Nikhil Chandra/Documents/trade_data/NN_simulation_results 2025-05-24 Trading Copy.db",
        network_name = "H1_LM_Markov_NN_25_SD_71Perc_2025-05-13",
        NN_model = H1_model_High_SD_25_71_neg,
        Hour_data_with_LM_markov = Hour_data_with_LM_markov_ask,
        mean_values_by_asset_for_loop_H1 = mean_values_by_asset_for_loop_H1_ask,
        currency_conversion = currency_conversion,
        asset_infor = asset_infor,
        risk_dollar_value = 10,
        win_threshold = 0.6,
        slice_max = TRUE
      )

    trades_2_50 <-
      get_NN_best_trades_from_mult_anaysis(
        db_path = "C:/Users/Nikhil Chandra/Documents/trade_data/NN_simulation_results 2025-05-24 50% highProf.db",
        network_name = "H1_LM_Markov_NN_25_SD_71Perc_2025-05-13",
        NN_model = H1_model_High_SD_25_71_neg,
        Hour_data_with_LM_markov = Hour_data_with_LM_markov_ask,
        mean_values_by_asset_for_loop_H1 = mean_values_by_asset_for_loop_H1_ask,
        currency_conversion = currency_conversion,
        asset_infor = asset_infor,
        risk_dollar_value = 10,
        win_threshold = 0.2,
        risk_weighted_thresh = 0.6,
        slice_max = TRUE
      )

    if(!is.null(trades_2)) {trades_2 <- trades_2 %>% filter(trade_col == "Long")}
    if(!is.null(trades_2_50)) {trades_2_50 <- trades_2_50 %>% filter(trade_col == "Long")}

    trades_3 <-
      get_NN_best_trades_from_mult_anaysis(
        db_path = "C:/Users/Nikhil Chandra/Documents/trade_data/NN_simulation_results 2025-05-24 Trading Copy.db",
        network_name = "H1_LM_Markov_NN_Hidden35",
        NN_model = H1_LM_Markov_NN_Hidden35,
        Hour_data_with_LM_markov = Hour_data_with_LM_markov_bid,
        mean_values_by_asset_for_loop_H1 = mean_values_by_asset_for_loop_H1_bid,
        currency_conversion = currency_conversion,
        asset_infor = asset_infor,
        risk_dollar_value = 10,
        win_threshold = 0.54,
        risk_weighted_thresh = 0.02,
        slice_max = TRUE
      )

    if(!is.null(trades_3)) {trades_3 <- trades_3 %>% filter(trade_col == "Short")}

    trades_4 <-
      get_NN_best_trades_from_mult_anaysis(
        db_path = "C:/Users/Nikhil Chandra/Documents/trade_data/NN_simulation_results 2025-05-24 50% highProf.db",
        network_name = "LM_ML_Lead_5_21_21_5_layer",
        NN_model = LM_ML_Lead_5_21_21_5_layer,
        Hour_data_with_LM_markov = Hour_data_with_LM_markov_bid,
        mean_values_by_asset_for_loop_H1 = mean_values_by_asset_for_loop_H1_bid,
        currency_conversion = currency_conversion,
        asset_infor = asset_infor,
        risk_dollar_value = 10,
        win_threshold = 0.2,
        risk_weighted_thresh = 0.2,
        slice_max = TRUE
      )

    if(!is.null(trades_4)) {trades_4 <- trades_4 %>% filter(trade_col == "Long")}

    # H1_Model_data_train_bid <-
    #   Hour_data_with_LM_markov_bid %>%
    #   group_by(Asset) %>%
    #   slice_head(prop = 0.4)
    #
    # H1_Model_data_test_bid <-
    #   Hour_data_with_LM_markov_bid %>%
    #   group_by(Asset) %>%
    #   slice_tail(prop = 0.55)
    #
    # trades_3 <-
    #   combine_NN_predictions(
    #             training_data = H1_Model_data_train_bid,
    #             testing_data = H1_Model_data_test_bid,
    #             stop_factor = 6,
    #             profit_factor = 6,
    #             mean_values_by_asset_for_loop_H1_bid = mean_values_by_asset_for_loop_H1_bid,
    #             mean_values_by_asset_for_loop_H1_ask = mean_values_by_asset_for_loop_H1_ask,
    #             sd_fac = 3,
    #             currency_conversion = currency_conversion,
    #             risk_dollar_value = risk_dollar_value,
    #             asset_list_temp =
    #               c("USD_JPY", "GBP_JPY", "USD_SGD", "EUR_SEK", "USD_CHF", "USD_SEK", "XCU_USD",
    #                 "USD_MXN", "GBP_USD", "WTICO_USD", "SUGAR_USD", "USD_NOK", "USD_CZK", "WHEAT_USD",
    #                 "EUR_USD", "XAG_USD", "EUR_GBP", "USD_CNH", "USD_CAD", "SOYBN_USD", "AUD_USD", "NZD_USD",
    #                 "NZD_CHF", "WHEAT_USD", "USD_NOK"),
    #             trade_direction = "Short",
    #             test_results = FALSE
    #             )

    total_trades <- trades_1 %>%
      bind_rows(trades_2) %>%
      bind_rows(trades_3) %>%
      bind_rows(trades_2_50) %>%
      bind_rows(trades_1_50) %>%
      bind_rows(trades_4)

    greater_prof_trades <-
      total_trades %>%
      filter(stop_factor < profit_factor)

    if(dim(greater_prof_trades)[1] > 0) {
      greater_prof_trades <- greater_prof_trades %>%
        group_by(Asset, trade_col) %>%
        slice_max(risk_weighted_returns) %>%
        ungroup()

      greater_prof_trades_assets <- greater_prof_trades %>% distinct(Asset) %>% pull(Asset)

    } else {
      greater_prof_trades_assets <- c("XXXXXXXXXX")
      greater_prof_trades_assets <- NULL
    }

    equal_prof_trades <- total_trades %>%
      filter(!(Asset %in% greater_prof_trades_assets))

    if(dim(equal_prof_trades)[1] > 0) {

      equal_prof_trades <-
        equal_prof_trades %>%
        group_by(Asset, trade_col) %>%
        slice_min(stop_value) %>%
        group_by(Asset, trade_col) %>%
        slice_min(risk_weighted_returns) %>%
        group_by(Asset, trade_col) %>%
        mutate(
          kk = row_number()
        ) %>%
        group_by(Asset, trade_col) %>%
        slice_min(kk) %>%
        ungroup()

    } else {
      equal_prof_trades <- NULL
    }

    if(!is.null(equal_prof_trades)) {
      total_trades <-
        equal_prof_trades %>%
        bind_rows(greater_prof_trades)
    }

    if( is.null(equal_prof_trades) & !is.null(greater_prof_trades) ) {
      total_trades <-
        greater_prof_trades %>%
        bind_rows(equal_prof_trades)
    }

    if(dim(total_trades)[1] > 0) {

      get_oanda_account_number(account_name = account_name_long)
      current_trades_long <- get_list_of_positions(account_var = long_account_num)
      current_trades_long <- current_trades_long %>%
        mutate(direction = stringr::str_to_title(direction)) %>%
        rename(Asset = instrument )

      current_trades_short <- get_list_of_positions(account_var = short_account_num)
      current_trades_short <- current_trades_short %>%
        mutate(direction = stringr::str_to_title(direction)) %>%
        rename(Asset = instrument )

      total_trades <- total_trades %>%
        filter( (abs(volume_required) >= 0.1 &
                 Asset %in% c("SPX500_USD", "JP225_USD", "EU50_EUR", "US2000_USD", "SG30_SGD", "AU200_AUD",
                              "NAS100_USD", "DE30_EUR", "HK33_HKD")) |
                  (abs(volume_required) >= 1 &
                     !(Asset %in% c("SPX500_USD", "JP225_USD", "EU50_EUR", "US2000_USD", "SG30_SGD", "AU200_AUD",
                                  "NAS100_USD", "DE30_EUR", "HK33_HKD"))
                       )
               ) %>%
        slice_max(Date)

      current_trades <- current_trades_long %>%
        bind_rows(current_trades_short) %>%
        mutate(trade_col = direction)

      if(dim(total_trades)[1] > 1) {

        total_trades <-
          total_trades %>%
          left_join(current_trades) %>%
          mutate(
            std_units = case_when(
              # type == "CURRENCY" ~ units/1000,
              Asset %in% c("WHEAT_USD", "SOYBN_USD", "XCU_USD", "XAG_USD") ~ units*100,
              Asset %in% c("SG30_SGD", "SPX500_USD", "AU200_AUD",
                           "JP225_USD", "DE30_EUR", "US2000_USD", "XAU_USD") ~ units*1000,
              TRUE ~ units
            )
          ) %>%
          arrange(std_units) %>%
          dplyr::select(-std_units, -direction, -units)

      }

      for (i in 1:dim(total_trades)[1]) {

        account_details_long <- get_account_summary(account_var = long_account_num)
        margain_available_long <- account_details_long$marginAvailable %>% as.numeric()
        margain_used_long <- account_details_long$marginUsed%>% as.numeric()
        total_margain_long <- margain_available_long + margain_used_long
        percentage_margain_available_long <- margain_available_long/total_margain_long

        account_details_short <- get_account_summary(account_var = short_account_num)
        margain_available_short <- account_details_short$marginAvailable %>% as.numeric()
        margain_used_short <- account_details_short$marginUsed%>% as.numeric()
        total_margain_short <- margain_available_short + margain_used_short
        percentage_margain_available_short <- margain_available_short/total_margain_short

        Sys.sleep(1)

        trade_direction <- total_trades$trade_col[i] %>% as.character()
        asset <- total_trades$Asset[i] %>% as.character()
        volume_trade <- total_trades$volume_required[i] %>% as.numeric()
        volume_trade <- ifelse(trade_direction == "Short" & volume_trade > 0, -1*volume_trade, volume_trade)
        volume_trade <- ifelse(trade_direction == "Long" & volume_trade < 0, -1*volume_trade, volume_trade)

        loss_var <- total_trades$stop_value[i] %>% as.numeric()
        profit_var <- total_trades$profit_value[i] %>% as.numeric()

        if(loss_var > 9) { loss_var <- round(loss_var)}
        if(profit_var > 9) { profit_var <- round(profit_var)}

        if(percentage_margain_available_long[1] > margain_threshold & trade_direction == "Long") {

          volume_trade <- ifelse(volume_trade < 0, -1*volume_trade, volume_trade)

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

        if(percentage_margain_available_short[1] > margain_threshold & trade_direction == "Short") {

          volume_trade <- ifelse(volume_trade > 0, -1*volume_trade, volume_trade)

          # This is misleading because it is price distance and not pip distance
          http_return <- oanda_place_order_pip_stop(
            asset = asset,
            volume = volume_trade,
            stopLoss = loss_var,
            takeProfit = profit_var,
            type = "MARKET",
            timeinForce = "FOK",
            acc_name = account_name_short,
            position_fill = "OPEN_ONLY" ,
            price
          )

        }

      }

    }

  }


  if(current_minute > 30 &  current_minute < 40 & data_updated == 1) {data_updated <- 0}

}



#----------------------------------------------------------------BEST CONDITION
#----------------------------------------------------------------BEST CONDITION
#----------------------------------------------------------------BEST CONDITION
#---------------------------------Winning Condiditions:
#----------Results 67% Win Rate, 13% Risk Weighted Return, final = 22405.79
#----------Lowest: -129.093
#----------Trades = 17k
#This Trading route does the opposite of the model and succeeds. It has more
#trades and taken in conjunction with the first model we can get a continuous
#trading model.
#Params: train = 0.4, test = 0.55, hidden = 20, threshold = 0.04, stepmax = 20000,
# SD = 2,  predicted < Average_NN_Pred - SD_NN_Pred*2 ~ "Long"
# profit_factor  = 14
# stop_factor  = 14

db_location = "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data.db"
start_date_day = "2011-01-01"
end_date_day = today() %>% as.character()

starting_asset_data_ask_daily <-
  get_db_price(
    db_location = db_location,
    start_date = start_date_day,
    end_date = end_date_day,
    bid_or_ask = "ask",
    time_frame = "D"
  )

starting_asset_data_ask_H1 <-
  get_db_price(
    db_location = db_location,
    start_date = start_date_day,
    end_date = end_date_day,
    bid_or_ask = "ask",
    time_frame = "H1"
  )

mean_values_by_asset_for_loop_D =
  wrangle_asset_data(
    asset_data_daily_raw = starting_asset_data_ask_daily,
    summarise_means = TRUE
  )

mean_values_by_asset_for_loop_H1 =
  wrangle_asset_data(
    asset_data_daily_raw = starting_asset_data_ask_H1,
    summarise_means = TRUE
  )

Hour_data_with_LM <-
  run_LM_join_to_H1(
    daily_data_internal = starting_asset_data_ask_daily,
    H1_data_internal = starting_asset_data_ask_H1,
    raw_macro_data = raw_macro_data,
    AUD_exports_total = AUD_exports_total,
    USD_exports_total = USD_exports_total,
    eur_data = eur_data
  )

Hour_data_with_LM_markov <-
  extract_required_markov_data(
    Hour_data_with_LM = Hour_data_with_LM,
    new_daily_data_ask = starting_asset_data_ask_daily,
    currency_conversion = currency_conversion,
    mean_values_by_asset_for_loop = mean_values_by_asset_for_loop_D,
    profit_factor  = 5,
    stop_factor  = 3,
    risk_dollar_value = 5,
    trade_sd_fact = 2
  ) %>%
  filter(!is.na(Price_to_Low_lag3), !is.na(Price_to_Low_lead), !is.na(Price_to_High_lag2))

H1_Model_data_train <-
  Hour_data_with_LM_markov %>%
  group_by(Asset) %>%
  slice_head(prop = 0.4)

H1_Model_data_test <-
  Hour_data_with_LM_markov %>%
  group_by(Asset) %>%
  slice_tail(prop = 0.55)

mean_values_by_asset_for_loop_D =
  wrangle_asset_data(
    asset_data_daily_raw = starting_asset_data_ask_daily,
    summarise_means = TRUE
  )

mean_values_by_asset_for_loop_H1 =
  wrangle_asset_data(
    asset_data_daily_raw = starting_asset_data_ask_H1,
    summarise_means = TRUE
  )

H1_model_High <- neuralnet::neuralnet(formula = Price_to_Price_lead ~
                                        Pred_trade + mean_value + mean_value +
                                        sd_value + Total_Avg_Prob_Diff_Low +
                                        Total_Avg_Prob_Diff_High + Total_Avg_Prob_Diff_SD_Low +
                                        Total_Avg_Prob_Diff_SD_High +
                                        Price_to_High_lag + Price_to_Low_lag +
                                        Price_to_High_lag2 + Price_to_Low_lag2 +
                                        Price_to_High_lag3 + Price_to_Low_lag3,
                                      hidden = c(21, 21, 21),
                                      data = H1_Model_data_train,
                                      err.fct = "sse",
                                      linear.output = TRUE,
                                      lifesign = 'full',
                                      rep = 1,
                                      algorithm = "rprop+",
                                      stepmax = 20000,
                                      threshold = 0.03)

saveRDS(
  H1_model_High,
  glue::glue("C:/Users/Nikhil Chandra/Documents/trade_data/LM_ML_Lead_21_21_21_layer_{today()}.rds")
)

# H1_model_High_SD_2_65_neg <-
#   readRDS(
#     "C:/Users/Nikhil Chandra/Documents/trade_data/Holy_GRAIL_MODEL_H1_LM_Markov_NN_2_SD_65Perc.rds"
#   )

prediction_nn <- predict(object = H1_model_High, newdata = H1_Model_data_test)
prediction_nn_train <- predict(object = H1_model_High, newdata = H1_Model_data_train)

average_train_predictions <-
  H1_Model_data_train %>%
  ungroup() %>%
  mutate(
    train_predictions = prediction_nn_train %>% as.numeric()
  ) %>%
  group_by(Asset) %>%
  summarise(
    Average_NN_Pred = mean(train_predictions, na.rm = T),
    SD_NN_Pred = sd(train_predictions, na.rm = T)
  ) %>%
  ungroup()

tagged_trades <-
  H1_Model_data_test %>%
  ungroup() %>%
  mutate(
    predicted = prediction_nn %>% as.numeric()
  ) %>%
  left_join(average_train_predictions) %>%
  mutate(
    trade_col =
      case_when(
        # predicted >=  Average_NN_Pred + SD_NN_Pred*3 &
        #   predicted <=  Average_NN_Pred + SD_NN_Pred*500~ "Long"

        predicted >=  Average_NN_Pred - SD_NN_Pred*500 &
          predicted <=  Average_NN_Pred - SD_NN_Pred*4~ "Long"
      )
  ) %>%
  filter(!is.na(trade_col))

profit_factor  = 10
stop_factor  = 10
risk_dollar_value <- 10

tagged_trades$Asset %>% unique()

long_bayes_loop_analysis <-
  generic_trade_finder_loop(
    tagged_trades = tagged_trades ,
    asset_data_daily_raw = H1_Model_data_test,
    stop_factor = stop_factor,
    profit_factor =profit_factor,
    trade_col = "trade_col",
    date_col = "Date",
    start_price_col = "Price",
    mean_values_by_asset = mean_values_by_asset_for_loop_H1
  )

trade_timings <-
  long_bayes_loop_analysis %>%
  mutate(
    ending_date_trade = as_datetime(ending_date_trade),
    dates = as_datetime(dates)
  ) %>%
  mutate(Time_Required = (ending_date_trade - dates)/ddays(1) )

trade_timings_by_asset <- trade_timings %>%
  mutate(win_loss = ifelse(trade_returns < 0, "loss", "wins") ) %>%
  group_by(asset, win_loss) %>%
  summarise(
    Time_Required = median(Time_Required, na.rm = T),
    Totals = n()
  ) %>%
  pivot_wider(names_from = win_loss, values_from = Time_Required)

analysis_data <-
  generic_anlyser(
    trade_data = long_bayes_loop_analysis %>% rename(Asset = asset),
    profit_factor = profit_factor,
    stop_factor = stop_factor,
    asset_infor = asset_infor,
    currency_conversion = currency_conversion,
    asset_col = "Asset",
    stop_col = "starting_stop_value",
    profit_col = "starting_profit_value",
    price_col = "trade_start_prices",
    trade_return_col = "trade_returns",
    risk_dollar_value = risk_dollar_value,
    grouping_vars = "trade_col"
  )

analysis_data_asset <-
  generic_anlyser(
    trade_data = long_bayes_loop_analysis %>% rename(Asset = asset),
    profit_factor = profit_factor,
    stop_factor = stop_factor,
    asset_infor = asset_infor,
    currency_conversion = currency_conversion,
    asset_col = "Asset",
    stop_col = "starting_stop_value",
    profit_col = "starting_profit_value",
    price_col = "trade_start_prices",
    trade_return_col = "trade_returns",
    risk_dollar_value = risk_dollar_value,
    grouping_vars = "Asset"
  )
