helpeR::load_custom_functions()
library(neuralnet)
raw_macro_data <- get_macro_event_data()
all_aud_symbols <- get_oanda_symbols() %>%
  keep(~ str_detect(.x, "AUD")|str_detect(.x, "USD_SEK|USD_NOK|USD_HUF|USD_ZAR|USD_CNY|USD_MXN"))
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
asset_list_oanda <- get_oanda_symbols() %>%
  keep( ~ .x %in% c("HK33_HKD", "USD_JPY",
                    "BTC_USD",
                    "AUD_NZD", "GBP_CHF",
                    "EUR_HUF", "EUR_ZAR", "NZD_JPY", "EUR_NZD",
                    "USB02Y_USD",
                    "XAU_CAD", "GBP_JPY", "EUR_NOK", "USD_SGD", "EUR_SEK",
                    "DE30_EUR",
                    "AUD_CAD",
                    "UK10YB_GBP",
                    "XPD_USD",
                    "UK100_GBP",
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
                    "USB02Y_USD", "USB05Y_USD", "USB30Y_USD", "USB10Y_USD", "UK100_GBP")
  )

asset_infor <- get_instrument_info()

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

data_updated <- 0

while (current_time < end_time) {

  current_time <- now() %>% as_datetime()
  current_minute <- lubridate::minute(current_time)
  current_hour <- lubridate::hour(current_time)
  current_date <- now() %>% as_date(tz = "Australia/Canberra")

  if(current_hour == 0 & data_updated == 0) {

    data_updated <- 1

    raw_macro_data <- get_macro_event_data()

    Daily_DATA_ALGO_ALL_ASSETS <-
      get_DAILY_ALGO_DATA_API_REQUEST()

    DAILY_TRADES <- get_ALL_DAILY_TRADES_FOR_ALGO(
      Daily_Data_Trades_ask = Daily_DATA_ALGO_ALL_ASSETS,
      actual_wins_losses_daily = get_ts_trade_actuals_Logit_NN("C:/Users/Nikhil Chandra/Documents/trade_data/full_ts_trades_mapped_Daily_Data.db", data_is_daily = TRUE),
      currency_conversion = currency_conversion,
      asset_infor = asset_infor,
      raw_macro_data = raw_macro_data
    )

    DAILY_TRADES_dfr <-
      DAILY_TRADES %>%
      map_dfr(bind_rows) %>%
      filter(pred >= pred_min) %>%
      filter(pred >= pred_min) %>%
      dplyr::select(-pred, -pred_min)%>%
      group_by(Asset) %>%
      slice_max(Date) %>%
      ungroup()

    all_tagged_trades_equity_dfr <-
      list(
        DAILY_TRADES_dfr) %>%
      map_dfr(bind_rows)

    message(glue::glue("Equity Trades: {dim(all_tagged_trades_equity_dfr)[1]}"))

    mean_values_by_asset_for_loop_H1_ask =
      wrangle_asset_data(
        asset_data_daily_raw = Daily_DATA_ALGO_ALL_ASSETS,
        summarise_means = TRUE
      )

    if(dim(all_tagged_trades_equity_dfr)[1] > 0 &
       !is.null(all_tagged_trades_equity_dfr) ) {
      all_tagged_trades_equity_dfr_stops_profs <-
        all_tagged_trades_equity_dfr %>%
        ungroup() %>%
        mutate(kk = row_number()) %>%
        split(.$kk) %>%
        map_dfr(
          ~
            get_stops_profs_volume_trades(
              tagged_trades = .x,
              mean_values_by_asset = mean_values_by_asset_for_loop_H1_ask,
              trade_col = "trade_col",
              currency_conversion = currency_conversion,
              risk_dollar_value = 5,
              stop_factor = .x$stop_factor[1] %>% as.numeric(),
              profit_factor = .x$profit_factor[1] %>% as.numeric(),
              asset_col = "Asset",
              stop_col = "stop_value",
              profit_col = "profit_value",
              price_col = "Price",
              trade_return_col = "trade_returns"
            )
        )
    } else {
      all_tagged_trades_equity_dfr_stops_profs <- all_tagged_trades_equity_dfr
    }


    if(dim(all_tagged_trades_equity_dfr_stops_profs)[1] > 0 &
       !is.null(all_tagged_trades_equity_dfr_stops_profs)) {

      for (i in 1:dim(all_tagged_trades_equity_dfr_stops_profs)[1]) {

        account_details_long <- get_account_summary(account_var = long_account_num_equity)
        margain_available_long <- account_details_long$marginAvailable %>% as.numeric()
        margain_used_long <- account_details_long$marginUsed%>% as.numeric()
        total_margain_long <- margain_available_long + margain_used_long
        percentage_margain_available_long <- margain_available_long/total_margain_long

        account_details_short <- get_account_summary(account_var = short_account_num_equity)
        margain_available_short <- account_details_short$marginAvailable %>% as.numeric()
        margain_used_short <- account_details_short$marginUsed%>% as.numeric()
        total_margain_short <- margain_available_short + margain_used_short
        percentage_margain_available_short <- margain_available_short/total_margain_short

        Sys.sleep(1)

        trade_direction <- all_tagged_trades_equity_dfr_stops_profs$trade_col[i] %>% as.character()
        asset <- all_tagged_trades_equity_dfr_stops_profs$Asset[i] %>% as.character()
        volume_trade <- all_tagged_trades_equity_dfr_stops_profs$volume_required[i] %>% as.numeric()
        volume_trade <- ifelse(trade_direction == "Short" & volume_trade > 0, -1*volume_trade, volume_trade)
        volume_trade <- ifelse(trade_direction == "Long" & volume_trade < 0, -1*volume_trade, volume_trade)

        loss_var <- all_tagged_trades_equity_dfr_stops_profs$stop_value[i] %>% as.numeric()
        profit_var <- all_tagged_trades_equity_dfr_stops_profs$profit_value[i] %>% as.numeric()

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
            acc_name = account_name_long_equity,
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
            acc_name = account_name_short_equity,
            position_fill = "OPEN_ONLY" ,
            price
          )

        }

      }

    }

    rm(Daily_DATA_ALGO_ALL_ASSETS, DAILY_TRADES, raw_macro_data)
    gc()
    Sys.sleep(2)


  }

  Sys.sleep(2)
  gc()
  Sys.sleep(2)

  if(current_minute > 30 &  current_minute < 40 & current_hour != 0 & data_updated == 1) {data_updated <- 0}

}

