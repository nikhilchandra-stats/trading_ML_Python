helperfunctions35South::load_custom_functions()
one_drive_path <- helperfunctions35South::create_one_drive_path(
  path_extension = "raw data")
library(neuralnet)

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

#---------------------------------------------Daily Regression Join
db_location = "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data.db"
start_date_day = "2011-01-01"
end_date_day = today() %>% as.character()

#-------------------------------Trade Loop
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
    "JP225_USD", "SPX500_USD") %>%
  unique()

end_time <- glue::glue("{floor_date(now(), 'week')} 23:59:00 AEST") %>% as_datetime(tz = "Australia/Canberra") + days(5)
current_time <- now()
trade_taken_this_hour <- 0
data_updated <- 0

margain_threshold <- 0.01
long_account_num <- 1
account_number_long <- "001-011-1615559-001"
account_name_long <- "primary"

short_account_num <- 3
account_number_short <- "001-011-1615559-004"
account_name_short <- "corr_no_macro"

long_account_num_equity <- 4
account_number_long_equity <- "001-011-1615559-002"
account_name_long_equity <- "equity_long"

short_account_num_equity <- 5
account_number_short_equity <- "001-011-1615559-005"
account_name_short_equity <- "equity_short"

db_location = "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data_Most_Assets_2025-09-13.db"
start_date_day = "2011-01-01"
end_date_day = today() %>% as.character()

#-----Upload new Data to DB
update_local_db_file(
  db_location = db_location,
  time_frame = "H1",
  bid_or_ask = "ask",
  how_far_back = 15
)

update_local_db_file(
  db_location = db_location,
  time_frame = "H1",
  bid_or_ask = "bid",
  asset_list_oanda = asset_list_oanda,
  how_far_back = 15
)

starting_asset_data_ask_H1 <-
  get_db_price(
    db_location = db_location,
    start_date = "2016-01-01",
    end_date = end_date_day,
    bid_or_ask = "ask",
    time_frame = "H1"
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
    start_date = "2016-01-01",
    end_date = end_date_day,
    bid_or_ask = "bid",
    time_frame = "H1"
  )

mean_values_by_asset_for_loop_H1_bid =
  wrangle_asset_data(
    asset_data_daily_raw = starting_asset_data_bid_H1,
    summarise_means = TRUE
  )

mean_values_by_asset_for_loop_H1_ask =
  wrangle_asset_data(
    asset_data_daily_raw = starting_asset_data_ask_H1,
    summarise_means = TRUE
  )

all_trade_ts_actuals_Logit <-
  get_ts_trade_actuals_Logit_NN(
    full_ts_trade_db_location = "C:/Users/Nikhil Chandra/Documents/trade_data/full_ts_trades_mapped_AUD_USD.db"
  ) %>%
  bind_rows(
    get_ts_trade_actuals_Logit_NN(
      full_ts_trade_db_location = "C:/Users/Nikhil Chandra/Documents/trade_data/full_ts_trades_mapped_ALL_EUR_USD_JPY_GBP.db"
    )
  )%>%
  bind_rows(
    get_ts_trade_actuals_Logit_NN(
      full_ts_trade_db_location = "C:/Users/Nikhil Chandra/Documents/trade_data/full_ts_trades_mapped.db"
    )
  )

all_trade_ts_actuals_Logit1 <-
  all_trade_ts_actuals_Logit %>%
  filter(profit_factor == 8, stop_factor == 4) %>%
  filter(
    asset %in%
      c("EUR_USD", "EUR_GBP", "GBP_USD", "GBP_JPY", "EUR_JPY", "USD_JPY",
        "SPX500_USD", "US2000_USD", "EU50_EUR", "AU200_AUD", "SG30_SGD", "XAG_USD", "XAU_USD",
        "AUD_USD", "NZD_USD", "XCU_USD", "NZD_CHF", "XAG_USD", "XAU_USD",
        "EUR_AUD", "EUR_NZD", "XAG_EUR", "XAU_EUR", "USD_CHF", "XAU_CHF", "XAG_CHF"
      )
  )

all_trade_ts_actuals_Logit2 <-
  all_trade_ts_actuals_Logit %>%
  filter(profit_factor == 4, stop_factor == 2) %>%
  filter(
    asset %in%
      c("EUR_USD", "GBP_USD",
        "SPX500_USD", "US2000_USD", "EU50_EUR", "AU200_AUD", "SG30_SGD", "XAG_USD", "XAU_USD",
        "UK100_GBP", "JP225Y_JPY", "FR40_EUR", "CH20_CHF", "USB10Y_USD", "USB02Y_USD", "UK10YB_GBP",
        "HK33_HKD"
      )
  )

all_trade_ts_actuals_Logit <-
  all_trade_ts_actuals_Logit1 %>%
  bind_rows(all_trade_ts_actuals_Logit2)

rm(all_trade_ts_actuals_Logit1, all_trade_ts_actuals_Logit2)

all_trade_ts_actuals_Logit <- all_trade_ts_actuals_Logit %>%
  filter(dates >= "2016-01-01")

gc()

trade_tracker_DB_path <-
  "C:/Users/Nikhil Chandra/Documents/trade_data/trade_tracker_daily_buy_close.db"
trade_tracker_DB <- connect_db(trade_tracker_DB_path)

while (current_time < end_time) {

  current_time <- now() %>% as_datetime()
  current_minute <- lubridate::minute(current_time)
  current_hour <- lubridate::hour(current_time)
  current_date <- now() %>% as_date(tz = "Australia/Canberra")

  #----------------------Refresh Data Stores and LM model
  if(current_minute > 0 & current_minute < 7 & current_hour == 0) {

    total_trades <-
      tibble(
        Asset  = c("AUD_USD", "EUR_USD"),
        volume_required = c(1,1),
        trade_col = c("Long", "Long"),
        stop_value = c(0.001, 0.001),
        profit_value = c(0.001, 0.001),
        periods_ahead = 24
      )

    if(dim(total_trades)[1] > 0) {

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

        cleaned_trade_details <-
          extract_put_request_return(http_return) %>%
          mutate(Asset = asset,
                 trade_col = trade_direction,
                 account_var = long_account_num,
                 account_name = account_name_long,
                 status = "OPEN",
                 periods_ahead = total_trades$periods_ahead[i] %>% as.numeric())

        write_table_sql_lite(.data = cleaned_trade_details,
                             table_name = "trade_tracker",
                             conn = trade_tracker_DB)

      }

    }


  }

  if(current_minute > 0 & current_minute < 7 & current_hour != 0) {

    trades_from_DB <-
      DBI::dbGetQuery(conn = trade_tracker_DB,
                      statement = "SELECT * FROM trade_tracker")

    account_Long_1_Ids <-
      trades_from_DB %>%
      filter(status == "OPEN") %>%
      filter(account_var == long_account_num) %>%
      dplyr::select(tradeID, Asset, periods_ahead, account_name)

    account_Long_2_Ids <-
      trades_from_DB %>%
      filter(status == "OPEN") %>%
      filter(account_var == long_account_num_equity) %>%
      dplyr::select(tradeID, Asset, periods_ahead, account_name)

    open_positions_account_long1 <-
      get_Open_positions(account_var = long_account_num) %>%
      mutate(id = as.character(id)) %>%
      left_join(
        account_Long_1_Ids, by = c("id" = "tradeID", "Asset")
      ) %>%
      filter(!is.na(periods_ahead))

    open_positions_account_long2 <-
      get_Open_positions(account_var = long_account_num_equity)  %>%
      mutate(id = as.character(id)) %>%
      left_join(
        account_Long_2_Ids, by = c("id" = "tradeID", "Asset")
      ) %>%
      filter(!is.na(periods_ahead))

    positions_tagged_as_part_of_algo <-
      open_positions_account_long1 %>%
      mutate(openTime = as_datetime(openTime, tz = "Australia/Canberra"),
             time_in_process =  as.numeric(current_time - openTime, units = "hours"),
             flagged_for_close = time_in_process >= periods_ahead) %>%
      bind_rows(
        open_positions_account_long2 %>%
          mutate(openTime = as_datetime(openTime, tz = "Australia/Canberra"),
                 time_in_process =  as.numeric(current_time - openTime, units = "hours"),
                 flagged_for_close = time_in_process >= periods_ahead)
      )


    if(dim(positions_tagged_as_part_of_algo)[1] > 0) {

      for (i in 1:dim(positions_tagged_as_part_of_algo)[1] ) {

        account_name_for_close <-
          positions_tagged_as_part_of_algo$account_name[i] %>% as.character()
        id_for_close <-
          positions_tagged_as_part_of_algo$id[i] %>% as.character()
        units_for_close <-
          positions_tagged_as_part_of_algo$currentUnits[i] %>% as.numeric()

        oanda_close_trade_ID(
          tradeID = id_for_close,
          units = units_for_close,
          account = account_name_for_close
        )

      }

    }


  }

}
