helpeR::load_custom_functions()
all_aud_symbols <-
  get_oanda_symbols() %>%
  keep(~ str_detect(.x, "AUD")|str_detect(.x, "USD_SEK|USD_NOK|USD_HUF|USD_ZAR|USD_CNY|USD_MXN|USD_CNH"))
asset_infor <- get_instrument_info()
aud_assets <- read_all_asset_data_intra_day(
  asset_list_oanda = all_aud_symbols,
  save_path_oanda_assets = "C:/Users/Nikhil Chandra/Documents/Asset Data/oanda_data/",
  read_csv_or_API = "API",
  time_frame = "D",
  bid_or_ask = "bid",
  how_far_back = 10,
  start_date = (today() - days(30)) %>% as.character()
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
  keep( ~ .x %in% c("XAG_USD", "XAG_EUR", "XAG_CAD", "XAG_AUD", "XAG_GBP", "XAG_JPY", "XAG_SGD", "XAG_CHF",
                    "XAG_NZD",
                    "XAU_USD", "XAU_EUR", "XAU_CAD", "XAU_AUD", "XAU_GBP", "XAU_JPY", "XAU_SGD", "XAU_CHF",
                    "XAU_NZD",
                    "BTC_USD", "LTC_USD", "BCH_USD",
                    "US30_USD", "FR40_EUR", "US2000_USD", "CH20_CHF", "SPX500_USD", "AU200_AUD",
                    "JP225_USD", "JP225Y_JPY", "SG30_SGD", "EU50_EUR", "HK33_HKD",
                    "USB02Y_USD", "USB05Y_USD", "USB30Y_USD", "USB10Y_USD", "UK100_GBP",
                    "AUD_USD", "EUR_USD", "GBP_USD", "USD_CHF", "USD_JPY", "USD_MXN", "USD_SEK", "USD_NOK",
                    "NZD_USD", "USD_CAD", "USD_SGD", "ETH_USD", "XPT_USD", "XPD_USD")
  )

asset_infor <- get_instrument_info()

db_location <- "C:/Users/nikhi/Documents/Asset Data/Oanda_Asset_Data_missing_Assets.db"
start_date_day_H1 = "2019-06-01"
end_date_day = today() %>% as.character()

starting_asset_data_ask_H1 <-
  get_db_price(
    db_location = db_location,
    start_date = start_date_day_H1,
    end_date = end_date_day,
    bid_or_ask = "ask",
    time_frame = "H1"
  )

starting_asset_data_bid_H1 <-
  get_db_price(
    db_location = db_location,
    start_date = start_date_day_H1,
    end_date = end_date_day,
    bid_or_ask = "bid",
    time_frame = "H1"
  )

mean_values_by_asset_for_loop_H1_ask =
  wrangle_asset_data(
    asset_data_daily_raw = starting_asset_data_ask_H1,
    summarise_means = TRUE
  )

gc()

rm(starting_asset_data_bid_H1)
rm(starting_asset_data_ask_H1)
gc()
Sys.sleep(10)
gc()

#----------------------------------------- Creating Data for Algo

current_time <- now() %>% as_datetime()
current_minute <- lubridate::minute(current_time)
current_hour <- lubridate::hour(current_time)
current_date <- now() %>% as_date(tz = "Australia/Canberra")

update_local_db_file(
  db_location = db_location,
  time_frame = "H1",
  bid_or_ask = "ask",
  asset_list_oanda = asset_list_oanda,
  how_far_back = 10
)

update_local_db_file(
  db_location = db_location,
  time_frame = "H1",
  bid_or_ask = "bid",
  asset_list_oanda = asset_list_oanda,
  how_far_back = 10
)

gc()

end_time <- glue::glue("{floor_date(now(), 'week')} 23:59:00 AEST") %>% as_datetime(tz = "Australia/Canberra") + days(5)
current_time <- now()
trade_taken_this_hour <- 0
data_updated <- 0

margain_threshold <- 0.001
long_account_num <-2
account_number_long <-  "001-011-1615559-003"
account_name_long <- "mt4_hedging"

short_account_num <- 3
account_number_short <- "001-011-1615559-004"
account_name_short <- "corr_no_macro"

trade_actual_db <- "C:/Users/nikhi/Documents/trade_data/trade_actuals_addtional_assets.db"
trade_actual_db_con <- connect_db(trade_actual_db)
actual_wins_losses <-
  DBI::dbGetQuery(trade_actual_db_con,
                  "SELECT * FROM trade_actual") %>%
  mutate(
    Date = as_datetime(Date)
  )
DBI::dbDisconnect(trade_actual_db_con)
rm(trade_actual_db_con)

db_loc_tracking_algo_path <- "C:/Users/nikhi/Documents/trade_data/ALGO_TRACKING_METALS_BONDS_CURR.db"
db_loc_tracking_algo <- connect_db(db_loc_tracking_algo_path)

gc()
c = 0
stop_value_var = 3
profit_value_var = 6
actual_wins_losses <-
  actual_wins_losses %>%
  filter(profit_factor == profit_value_var, stop_factor == stop_value_var) %>%
  filter(Date >= "2021-01-01") %>%
  filter(Asset  %in% c("XAG_USD", "USD_CHF", "XAU_CHF", "XAG_CHF", "GBP_USD", "AUD_USD", "USD_JPY",
                       "LTC_USD", "USB30Y_USD", "UK100_GBP", "USB10Y_USD", "USB05Y_USD", "USB02Y_USD",
                       "EUR_USD", "FR40_EUR", "EU50_EUR", "SPX500_USD", "ETH_USD", "BTC_USD", "XAU_JPY",
                       "XAU_AUD", "XAU_GBP", "XAU_EUR", "XAU_USD", "XAG_JPY", "XAG_AUD", "XAG_GBP", "XAG_EUR"))
gc()

while(current_time < end_time) {

  current_time <- now() %>% as_datetime()
  current_minute <- lubridate::minute(current_time)
  current_hour <- lubridate::hour(current_time)
  current_date <- now() %>% as_date(tz = "Australia/Canberra")

  if( (current_minute > 0 & current_minute < 7 & data_updated == 0) ) {

    gc()

    tictoc::tic()

    update_local_db_file(
      db_location = db_location,
      time_frame = "H1",
      bid_or_ask = "ask",
      asset_list_oanda = asset_list_oanda,
      how_far_back = 12
    )

    update_local_db_file(
      db_location = db_location,
      time_frame = "H1",
      bid_or_ask = "bid",
      asset_list_oanda = asset_list_oanda,
      how_far_back = 12
    )

    data_updated <- 1

    raw_macro_data <- get_macro_event_data()

    tictoc::tic()

    METALS_CRYPTO <-
      get_All_Metals_USD_Currency(
        db_location = db_location,
        start_date = "2021-01-01",
        end_date = today() %>% as.character(),
        time_frame = "H1"
      )

    copula_data_BONDS_METALS_INDEX <-
      create_NN_BONDS_METALS_INDEX_data(
        METALS_CRYPTO = METALS_CRYPTO[[1]] %>% filter(!(Asset %in% c("ETH_USD", "BTC_USD", "LTC_USD")) ),
        raw_macro_data,
        actual_wins_losses = actual_wins_losses  %>%
          rename( asset = Asset,
                  dates = Date),
        lag_days = 1,
        stop_value_var = 3,
        profit_value_var = 6,
        use_PCA_vars = FALSE
      )

    BONDS_METALS_INDEX_logit_analysis <-
      get_Logit_trades(
        logit_path_save_path = "C:/Users/nikhi/Documents/trade_data/asset_specific_NNs_H1_1/",
        Logit_sims_db = "C:/Users/nikhi/Documents/trade_data/BONDS_METALS_INDX_1_sims.db",
        copula_data = copula_data_BONDS_METALS_INDEX,
        sim_min = 100,
        edge_min = 0,
        stop_var = 3,
        profit_var = 6,
        outperformance_count_min = 0.61,
        risk_weighted_return_mid_min =  0.1,
        sim_table = "BONDS_METALS_INDEX",
        slice_max_var = "Trades",
        combined_filter_n = 10
      )

    BONDS_METALS_INDEX_logit_analysis_short <-
      get_Logit_trades(
        logit_path_save_path = "C:/Users/nikhi/Documents/trade_data/asset_specific_NNs_H1_1_short/",
        Logit_sims_db = "C:/Users/nikhi/Documents/trade_data/BONDS_METALS_INDX_1_sims_short.db",
        copula_data = copula_data_BONDS_METALS_INDEX,
        sim_min = 50,
        edge_min = 0,
        stop_var = 3,
        profit_var = 6,
        outperformance_count_min = 0.55,
        risk_weighted_return_mid_min =  0.1,
        sim_table = "BONDS_METALS_INDEX",
        slice_max_var = "Trades",
        combined_filter_n = 10
      )

    tictoc::toc()

    BONDS_METALS_INDEX_logit_trades <-
      BONDS_METALS_INDEX_logit_analysis %>%
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
      ) %>%
      # filter(Date >= (current_time + dminutes(35))) %>%
      filter(pred >= pred_min) %>%
      dplyr::select(-pred, -pred_min)

    BONDS_METALS_INDEX_logit_trades_short <-
      BONDS_METALS_INDEX_logit_analysis_short %>%
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
      ) %>%
      # filter(Date >= (current_time + dminutes(35))) %>%
      filter(pred >= pred_min) %>%
      dplyr::select(-pred, -pred_min)

    All_trades_list <-
      list(
        BONDS_METALS_INDEX_logit_trades,
        BONDS_METALS_INDEX_logit_trades_short
      ) %>%
      map_dfr(bind_rows)

    current_trades_long <- get_list_of_positions(account_var = long_account_num)
    current_trades_long <- current_trades_long %>%
      mutate(direction = stringr::str_to_title(direction)) %>%
      rename(Asset = instrument )

    current_trades_short <- get_list_of_positions(account_var = short_account_num)
    current_trades_short <- current_trades_short %>%
      mutate(direction = stringr::str_to_title(direction)) %>%
      rename(Asset = instrument )

    symbols_to_keep <-
      current_trades_long %>%
      bind_rows(current_trades_short) %>%
      mutate(
        syms_to_remove =
          case_when(
            Asset %in% c("SPX500_USD",  "EU50_EUR") &  units >= 5/1.5 ~ "Remove",
            Asset %in% c("US2000_USD", "AU200_AUD") &  units >= 5/1.5 ~ "Remove" ,
            Asset %in% c("XAG_USD", "XAG_EUR", "XAG_JPY", "XAG_AUD", "XAG_NZD", "XAG_GBP") &  units >= 200/1.5 ~ "Remove",
            Asset %in% c("USB02Y_USD", "USB05Y_USD", "USB30Y_USD", "USB10Y_USD") &  units >= 114 ~ "Remove",
            Asset %in% c("XAU_USD", "XAU_EUR", "XAU_JPY", "XAU_AUD", "XAU_NZD", "XAU_GBP") &  units >= 0 ~ "Remove",
            TRUE ~ "Keep"
          )
      ) %>%
      filter(syms_to_remove == "Remove") %>%
      dplyr::select(Asset, trade_col = direction, syms_to_remove)

    rm(BONDS_METALS_INDEX_logit_analysis)
    rm(copula_data_BONDS_METALS_INDEX)
    gc()
    Sys.sleep(10)
    gc()

    message(glue::glue("Long Trades Metals Bonds Currencies: {dim(All_trades_list)[1]}"))
    message(glue::glue("Symbols to keep {paste(symbols_to_keep, collapse = ',')}"))

    tictoc::toc()

    All_trades <-
      list(
        All_trades_list
      ) %>%
      map_dfr(bind_rows)

    if(dim(All_trades)[1] < 1 ) {
      All_trades <- NULL
    }


    if(!is.null(All_trades)) {

      total_trades <-
        All_trades %>%
        slice_max(Date) %>%
        filter(!is.na(trade_col))

    } else {
      total_trades <- NULL
    }

    if(!is.null(total_trades)) {
      total_trades <-
        total_trades %>%
        left_join(
          symbols_to_keep
        ) %>%
        mutate(
          syms_to_remove =
            case_when(
              is.na(syms_to_remove) ~ "Keep",
              TRUE ~ syms_to_remove
            )
        ) %>%
        dplyr::filter(syms_to_remove == "Keep" & !stringr::str_detect(Asset, "XAU_")) %>%
        dplyr::select(-syms_to_remove)

      if(dim(total_trades)[1] < 1) {
        total_trades <- NULL
      }

    } else {
      total_trades <- NULL
    }

    message(glue::glue("Long Trades Metals Bonds Currencies:"))
    message(glue::glue("{dim(total_trades)[1]}"))


    message("Made it to end of trade estimation code")
    c = c + 1
    message(glue::glue("Loop Number: {c}"))

    if(!is.null(total_trades)) {

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

        total_trades <-
          total_trades %>%
          slice_max(Date)

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

      rm(total_trades, All_trades, All_trades_list, EUR_GBP_JPY_logit_trades)


    }

    # message(glue::glue("Made it to end of loop SPX: {dim(total_trades)[1]}"))

  }

  if( (current_minute > 40 & current_minute < 55) ) {
  data_updated <- 0
  }

  # message(glue::glue("Time is: {current_time}"))

}
