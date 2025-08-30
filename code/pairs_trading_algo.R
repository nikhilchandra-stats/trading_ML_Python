helpeR::load_custom_functions()
library(neuralnet)

all_aud_symbols <-
  get_oanda_symbols() %>%
  keep(~ str_detect(.x, "AUD")|str_detect(.x, "USD_SEK|USD_NOK|USD_HUF|USD_ZAR|USD_CNY|USD_MXN"))
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

asset_list_oanda <- get_oanda_symbols() %>%
  keep( ~ .x %in% c("HK33_HKD", "USD_JPY","SPX500_USD",
                    "AUD_NZD", "GBP_CHF",
                    "EUR_HUF", "EUR_ZAR", "NZD_JPY", "EUR_NZD",
                    "XAU_CAD", "GBP_JPY", "EUR_NOK", "USD_SGD", "EUR_SEK",
                    "DE30_EUR",
                    "AUD_CAD",
                    "XPD_USD",
                    "UK100_GBP",
                    "USD_CHF", "GBP_NZD",
                    "GBP_SGD", "USD_SEK", "EUR_SGD", "XCU_USD", "SUGAR_USD", "CHF_ZAR",
                    "AUD_CHF", "EUR_CHF", "USD_MXN", "GBP_USD", "WTICO_USD", "EUR_JPY", "USD_NOK",
                    "XAU_USD",
                    "USD_CZK", "AUD_SGD", "USD_HUF", "WHEAT_USD",
                    "EUR_USD", "SG30_SGD", "GBP_AUD", "NZD_CAD", "AU200_AUD", "XAG_USD",
                    "XAU_EUR", "EUR_GBP", "USD_CNH", "USD_CAD", "NAS100_USD",
                    "EU50_EUR", "NATGAS_USD", "CAD_JPY", "FR40_EUR", "USD_ZAR", "XAU_GBP",
                    "EUR_AUD", "SOYBN_USD",
                    "US2000_USD",
                    "BCO_USD", "AUD_USD", "NZD_USD", "NZD_CHF", "WHEAT_USD", "AUD_JPY", "AUD_SEK")
  )

asset_infor <- get_instrument_info()

db_location <- "C:/Users/nikhi/Documents/Asset Data/Oanda_Asset_Data.db"
start_date = (lubridate::add_with_rollback(today(), -years(3))) %>% as.character()
end_date_day = today() %>% as.character()

db_location <- "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data.db"

starting_asset_data_ask_15M <-
  get_db_price(
    db_location = db_location,
    start_date = start_date,
    end_date = today() %>% as.character(),
    bid_or_ask = "ask",
    time_frame = "M15"
  )
starting_asset_data_bid_15M <-
  get_db_price(
    db_location = db_location,
    start_date = start_date,
    end_date = today() %>% as.character(),
    bid_or_ask = "bid",
    time_frame = "M15"
  )


mean_values_by_asset_for_loop_15_ask =
  wrangle_asset_data(
    asset_data_daily_raw = starting_asset_data_ask_15M,
    summarise_means = TRUE
  )


gc()

#----------------------------------------- Creating Data for Algo

current_time <- now() %>% as_datetime()
current_minute <- lubridate::minute(current_time)
current_hour <- lubridate::hour(current_time)
current_date <- now() %>% as_date(tz = "Australia/Canberra")

update_local_db_file(
  db_location = db_location,
  time_frame = "M15",
  bid_or_ask = "bid",
  asset_list_oanda = asset_list_oanda,
  how_far_back = 5
)

gc()

update_local_db_file(
  db_location = db_location,
  time_frame = "M15",
  bid_or_ask = "ask",
  asset_list_oanda = asset_list_oanda,
  how_far_back = 5
)

gc()

new_15_data_ask <-
  updated_data_internal(starting_asset_data = starting_asset_data_ask_15M,
                        end_date_day = current_date,
                        time_frame = "M15", bid_or_ask = "ask",
                        db_location = db_location)%>%
  distinct()

new_15_data_bid <-
  updated_data_internal(starting_asset_data = starting_asset_data_bid_15M,
                        end_date_day = current_date,
                        time_frame = "M15", bid_or_ask = "bid",
                        db_location = db_location)%>%
  distinct()

end_time <- glue::glue("{floor_date(now(), 'week')} 23:59:00 AEST") %>% as_datetime(tz = "Australia/Canberra") + days(5)
current_time <- now()
trade_taken_this_hour <- 0
data_updated <- 0

margain_threshold <- 0.05
long_account_num <-2
account_number_long <-  "001-011-1615559-003"
account_name_long <- "mt4_hedging"

short_account_num <- 3
account_number_short <- "001-011-1615559-004"
account_name_short <- "corr_no_macro"

gc()

while(current_time < end_time) {

  current_time <- now() %>% as_datetime()
  current_minute <- lubridate::minute(current_time)
  current_hour <- lubridate::hour(current_time)
  current_date <- now() %>% as_date(tz = "Australia/Canberra")

  start_date = (lubridate::add_with_rollback(current_date, -years(2)))
  start_date_2 = (lubridate::add_with_rollback(current_date, -years(3)))

  if(
      # (current_minute > 0 & current_minute < 3 & data_updated == 0)|
      (current_minute > 15 & current_minute < 18 & data_updated == 0)|
      (current_minute > 30 & current_minute < 33 & data_updated == 0)|
      (current_minute > 45 & current_minute < 48 & data_updated == 0)  ) {

tictoc::tic()

    update_local_db_file(
      db_location = db_location,
      time_frame = "M15",
      bid_or_ask = "bid",
      asset_list_oanda = asset_list_oanda,
      how_far_back = 2
    )

    gc()

    update_local_db_file(
      db_location = db_location,
      time_frame = "M15",
      bid_or_ask = "ask",
      asset_list_oanda = asset_list_oanda,
      how_far_back = 2
    )

    gc()

    new_15_data_ask <-
      updated_data_internal(starting_asset_data = starting_asset_data_ask_15M,
                            end_date_day = current_date,
                            time_frame = "M15", bid_or_ask = "ask",
                            db_location = db_location)%>%
      distinct() %>%
      filter(Date >= start_date_2)

    new_15_data_bid <-
      updated_data_internal(starting_asset_data = starting_asset_data_bid_15M,
                            end_date_day = current_date,
                            time_frame = "M15", bid_or_ask = "bid",
                            db_location = db_location)%>%
      distinct() %>%
      filter(Date >= start_date_2)

    data_updated <- 1

    raw_macro_data <- get_macro_event_data()

    tictoc::tic()

    AUD_USD_NZD_USD_all_data <-
      get_all_AUD_USD_specific_data(
        db_location = db_location,
        start_date = "2016-01-01",
        end_date = today() %>% as.character()
      )

    AUD_NZD_Trades_long <-
      get_AUD_USD_NZD_Specific_Trades(
        AUD_USD_NZD_USD = AUD_USD_NZD_USD_all_data[[1]],
        start_date = "2016-01-01",
        raw_macro_data = raw_macro_data,
        lag_days = 1,
        lm_period = 700,
        lm_train_prop = 0.74,
        lm_test_prop = 0.24,
        sd_fac_lm_trade = 1.5,
        sd_fac_lm_trade2 = 3,
        sd_fac_lm_trade3 = 3,
        trade_direction = "Long",
        stop_factor = 12,
        profit_factor = 18,
        assets_to_return = c("AUD_USD")
      ) %>%
      map_dfr(bind_rows) %>%
      group_by(Asset) %>%
      slice_max(Date) %>%
      filter(Date >= (current_time - lubridate::minutes(20)) )

    AUD_NZD_Trades_short <-
      get_AUD_USD_NZD_Specific_Trades(
        AUD_USD_NZD_USD = AUD_USD_NZD_USD_all_data[[2]],
        start_date = "2016-01-01",
        raw_macro_data = raw_macro_data,
        lag_days = 1,
        lm_period = 700,
        lm_train_prop = 0.74,
        lm_test_prop = 0.24,
        sd_fac_lm_trade = 0.75,
        sd_fac_lm_trade2 = 4,
        sd_fac_lm_trade3 = 4,
        trade_direction = "Short",
        stop_factor = 12,
        profit_factor = 18,
        assets_to_return = c("AUD_USD", "NZD_USD")
      ) %>%
      map_dfr(bind_rows) %>%
      group_by(Asset) %>%
      slice_max(Date) %>%
      filter(Date >= (current_time - lubridate::minutes(20)) )

    short_AUD_positions <- AUD_NZD_Trades_short %>% filter(Asset == "AUD_USD") %>% dim() %>% pluck(1)
    long_AUD_positions <- AUD_NZD_Trades_long %>% filter(Asset == "AUD_USD") %>% dim() %>% pluck(1)

    short_NZD_positions <- AUD_NZD_Trades_short %>% filter(Asset == "NZD_USD") %>% dim() %>% pluck(1)
    long_NZD_positions <- AUD_NZD_Trades_long %>% filter(Asset == "NZD_USD") %>% dim() %>% pluck(1)

    message(glue::glue("AUD_USD Long: {long_AUD_positions}, AUD_USD Short: {short_AUD_positions}"))
    message(glue::glue("NZD_USD Long: {long_NZD_positions}, NZD_USD Short: {short_NZD_positions}"))

    tictoc::toc()

    total_trades_long_1 <-
      get_pairs_cor_reg_trades_to_take(
        db_path = glue::glue("C:/Users/Nikhil Chandra/Documents/trade_data/pairs_2025-07-03.db"),
        min_risk_win = 0.02,
        return_filter_col = "median_return",
        max_win_time = 150,
        starting_asset_data_ask_15M = new_15_data_ask %>%
          filter(Date >= start_date),
        starting_asset_data_bid_15M = new_15_data_bid%>%
          filter(Date >= start_date),
        mean_values_by_asset = mean_values_by_asset_for_loop_15_ask,
        trade_direction = "Long",
        risk_dollar_value = 2.5,
        currency_conversion = currency_conversion,
        asset_infor = asset_infor
      )

    total_trades_All <-
      list(total_trades_long_1,
           AUD_NZD_Trades_long,
           AUD_NZD_Trades_short
           ) %>%
      map_dfr(bind_rows)

    message(glue::glue("Trades Found all times {dim(total_trades_All)[1]}"))

    if(!is.null(total_trades_All) & dim(total_trades_All)[1] > 0 ) {

      max_date_in_data <-
        total_trades_All %>%
        slice_max(Date) %>%
        filter(!is.na(trade_col)) %>%
        pull(Date) %>%
        pluck(1) %>%
        as.character()

      message(glue::glue("Max Date in Data {max_date_in_data}"))

      total_trades <-
        total_trades_All %>%
        slice_max(Date) %>%
        filter(!is.na(trade_col)) %>%
        filter(Date >= (current_time - lubridate::minutes(20)) )

      message(glue::glue("Trades Found in current Time {dim(total_trades)[1]}"))

      if(dim(total_trades)[1] > 0) {
        check_timing <- difftime(current_time, total_trades$Date[1], units = "mins") %>% as.numeric()
        if( check_timing > 25) {total_trades = NULL}
      }

    } else {
      total_trades <- NULL
    }

    tictoc::toc()

    message("Made it to end of trade estimation code")

    trade_to_procceed_check1 <-
      ifelse(!is.null(total_trades),
             dim(total_trades)[1] > 0,
             FALSE)
    trade_to_procceed_check2 <-
      ifelse(
        !is.null(total_trades),
        !is.null(total_trades),
        FALSE
      )

    trade_to_procceed <-
      trade_to_procceed_check1 & trade_to_procceed_check2

    if(trade_to_procceed == TRUE) {

      total_trades <-
        total_trades %>%
        split(.$Asset, drop = FALSE) %>%
        map_dfr(
          ~ .x %>%
            get_stops_profs_volume_trades(
              mean_values_by_asset = mean_values_by_asset_for_loop_15_ask,
              trade_col = "trade_col",
              currency_conversion = currency_conversion,
              risk_dollar_value = 2.5,
              stop_factor = .x$stop_factor[1] %>% as.numeric(),
              profit_factor = .x$profit_factor[1] %>% as.numeric(),
              asset_col = "Asset",
              stop_col = "stop_value",
              profit_col = "profit_value",
              price_col = "Price",
              trade_return_col = "trade_returns"
            )
        )

      message("Made it to end of trade stop prof calculation")

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

        if(dim(total_trades)[1] > 0) {

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

  }

  if((current_minute > 12 & current_minute < 15 & data_updated == 1)|
     (current_minute > 27 & current_minute < 30 & data_updated == 1)|
     (current_minute > 42 & current_minute < 45 & data_updated == 1)|
     (current_minute > 55 & current_minute < 59 & data_updated == 1)
     ) {data_updated <- 0}


}
