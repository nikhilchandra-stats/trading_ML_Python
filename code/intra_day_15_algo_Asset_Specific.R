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

db_location <- "C:/Users/nikhi/Documents/Asset Data/Oanda_Asset_Data Asset Algo Use.db"
start_date_day_15M = "2021-01-01"
end_date_day = today() %>% as.character()

starting_asset_data_ask_15M <-
  get_db_price(
    db_location = db_location,
    start_date = start_date_day_15M,
    end_date = end_date_day,
    bid_or_ask = "ask",
    time_frame = "M15"
  )

starting_asset_data_bid_15M <-
  get_db_price(
    db_location = db_location,
    start_date = start_date_day_15M,
    end_date = end_date_day,
    bid_or_ask = "bid",
    time_frame = "M15"
  )

mean_values_by_asset_for_loop_15_ask =
  wrangle_asset_data(
    asset_data_daily_raw = starting_asset_data_ask_15M,
    summarise_means = TRUE
  )

gc()

rm(starting_asset_data_bid_15M)
rm(starting_asset_data_ask_15M)
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
  time_frame = "M15",
  bid_or_ask = "ask",
  asset_list_oanda = asset_list_oanda,
  how_far_back = 30
)

update_local_db_file(
  db_location = db_location,
  time_frame = "M15",
  bid_or_ask = "bid",
  asset_list_oanda = asset_list_oanda,
  how_far_back = 30
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

trade_actual_db <- "C:/Users/nikhi/Documents/trade_data/trade_actuals.db"
trade_actual_db_con <- connect_db(trade_actual_db)
actual_wins_losses <-
  DBI::dbGetQuery(trade_actual_db_con,
                  "SELECT * FROM trade_actual") %>%
  mutate(
    Date = as_datetime(Date)
  )
DBI::dbDisconnect(trade_actual_db_con)
rm(trade_actual_db_con)


gc()
c = 1

stop_value_var = 10
profit_value_var = 15

actual_wins_losses <-
  actual_wins_losses %>%
  filter(profit_factor == profit_value_var, stop_factor == stop_value_var) %>%
  filter(Date >= "2022-01-01") %>%
  filter(Asset %in% c("SPX500_USD", "EU50_EUR", "US2000_USD", "AU200_AUD", "XAG_USD", "XAU_USD"))

while(current_time < end_time) {

  current_time <- now() %>% as_datetime()
  current_minute <- lubridate::minute(current_time)
  current_hour <- lubridate::hour(current_time)
  current_date <- now() %>% as_date(tz = "Australia/Canberra")
  gc()


  if(
      # (current_minute > 0 & current_minute < 2 & data_updated == 0)|
      (current_minute > 15 & current_minute < 17 & data_updated == 0)|
      (current_minute > 30 & current_minute < 32 & data_updated == 0 )|
      (current_minute > 45 & current_minute < 47 & data_updated == 0)  ) {

    gc()

    Sys.sleep(10)

    update_local_db_file(
      db_location = db_location,
      time_frame = "M15",
      bid_or_ask = "ask",
      asset_list_oanda = asset_list_oanda,
      how_far_back = 7
    )

    update_local_db_file(
      db_location = db_location,
      time_frame = "M15",
      bid_or_ask = "bid",
      asset_list_oanda = asset_list_oanda,
      how_far_back = 7
    )

    data_updated <- 1

    raw_macro_data <- get_macro_event_data()

    SPX_US2000_XAG_ALL <- get_SPX_US2000_XAG_XAU(
      db_location = db_location,
      start_date = "2022-01-01",
      end_date = today() %>% as.character()
    )

    copula_data_Indices <-
      create_NN_Indices_data(
        SPX_US2000_XAG = SPX_US2000_XAG_ALL[[1]],
        raw_macro_data = raw_macro_data,
        actual_wins_losses = actual_wins_losses,
        lag_days = 1,
        stop_value_var = 10,
        profit_value_var = 15,
        use_PCA_vars = FALSE,
        rolling_period_index_PCA_cor = 15
      )

    # SPX_US2000_XAG_NN_analysis <-
    #   get_NN_trades_Asset_Specific(
    #   NN_path = "C:/Users/nikhi/Documents/trade_data/asset_specific_NNs/",
    #   copula_data = copula_data_Indices,
    #   stop_value_var = 10,
    #   profit_value_var = 15,
    #   NN_path_save_path = "C:/Users/nikhi/Documents/trade_data/asset_specific_NNs/",
    #   NN_sims_db = "C:/Users/nikhi/Documents/trade_data/INDICES_15M_NN_sims.db",
    #   table_name = "INDICES_15_NN_sims",
    #   skip_NN_generation = TRUE,
    #   testing_min_date_days_lag = 1000,
    #   sim_min = 30,
    #   edge_min = 0,
    #   outperformance_count_min = 0.50 ,
    #   risk_weighted_return_mid_min = 0.14
    # )

    SPX_US2000_XAG_logit_analysis <-
      get_Logit_trades(
        logit_path_save_path = "C:/Users/nikhi/Documents/trade_data/asset_specific_NNs/",
        Logit_sims_db = "C:/Users/nikhi/Documents/trade_data/INDICES_15M_NN_sims_logit_Long.db",
        copula_data = copula_data_Indices,
        sim_min = 100,
        edge_min = 0,
        stop_var = 10,
        profit_var = 15,
        outperformance_count_min = 0.55,
        risk_weighted_return_mid_min =  0.13,
        sim_table = "INDICES_15_NN_sims",
        combined_filter_n = 10
      )

    SPX_US2000_XAG_logit_analysis_short <-
      get_Logit_trades(
        logit_path_save_path = "C:/Users/nikhi/Documents/trade_data/asset_specific_NNs_short/",
        Logit_sims_db = "C:/Users/nikhi/Documents/trade_data/INDICES_15M_NN_sims_logit_Short.db",
        copula_data = copula_data_Indices,
        sim_min = 100,
        edge_min = 0,
        stop_var = 10,
        profit_var = 15,
        outperformance_count_min = 0.55,
        risk_weighted_return_mid_min =  0.1,
        sim_table = "INDICES_15_NN_sims",
        combined_filter_n = 10
      )

    # SPX_XAG_US2000_Long_trades_NN <-
    #   SPX_US2000_XAG_NN_analysis %>%
    #   ungroup() %>%
    #   mutate(kk = row_number()) %>%
    #   split(.$kk) %>%
    #   map_dfr(
    #     ~
    #       get_stops_profs_volume_trades(
    #         tagged_trades = .x,
    #         mean_values_by_asset = mean_values_by_asset_for_loop_15_ask,
    #         trade_col = "trade_col",
    #         currency_conversion = currency_conversion,
    #         risk_dollar_value = 5,
    #         stop_factor = .x$stop_factor[1] %>% as.numeric(),
    #         profit_factor = .x$profit_factor[1] %>% as.numeric(),
    #         asset_col = "Asset",
    #         stop_col = "stop_value",
    #         profit_col = "profit_value",
    #         price_col = "Price",
    #         trade_return_col = "trade_returns"
    #       )
    #   ) %>%
    #   # filter(Date >= (current_time + dminutes(35))) %>%
    #   filter(pred >= pred_min) %>%
    #   dplyr::select(-pred, -pred_min)

    SPX_XAG_US2000_Long_trades_logit <-
      SPX_US2000_XAG_logit_analysis %>%
      ungroup() %>%
      mutate(kk = row_number()) %>%
      split(.$kk) %>%
      map_dfr(
        ~
          get_stops_profs_volume_trades(
            tagged_trades = .x,
            mean_values_by_asset = mean_values_by_asset_for_loop_15_ask,
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

    SPX_XAG_US2000_Long_trades_logit_short <-
      SPX_US2000_XAG_logit_analysis_short %>%
      ungroup() %>%
      mutate(kk = row_number()) %>%
      split(.$kk) %>%
      map_dfr(
        ~
          get_stops_profs_volume_trades(
            tagged_trades = .x,
            mean_values_by_asset = mean_values_by_asset_for_loop_15_ask,
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
          # SPX_XAG_US2000_Long_trades_NN,
           SPX_XAG_US2000_Long_trades_logit,
           SPX_XAG_US2000_Long_trades_logit_short
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
            Asset %in% c("SPX500_USD",  "EU50_EUR") &  units >= 2 ~ "Remove",
            Asset %in% c("US2000_USD", "AU200_AUD") &  units >= 2 ~ "Remove" ,
            Asset %in% c("XAG_USD") &  units >= 250/1.25 ~ "Remove",
            TRUE ~ "Keep"
          )
      ) %>%
      filter(syms_to_remove == "Remove") %>%
      dplyr::select(Asset, trade_col = direction, syms_to_remove)

    rm(SPX_US2000_XAG_ALL)
    rm(copula_data_Indices)
    gc()
    Sys.sleep(10)
    gc()

    message(glue::glue("Long Trades SPX, XAG, US2000: {dim(All_trades_list)[1]}"))

    # tictoc::toc()

    All_trades <-
      list(
           All_trades_list
           ) %>%
      map_dfr(bind_rows)

    if(dim(All_trades)[1] < 1 ) {
      All_trades <- NULL
    }

    message(glue::glue("Long Trades SPX, XAG, US2000: {!is.null(All_trades)}"))

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
        dplyr::filter(syms_to_remove == "Keep")%>%
        dplyr::select(-syms_to_remove)

      message(glue::glue("Long Trades SPX, XAG, US2000: {dim(total_trades)[1]}"))

      if(dim(total_trades)[1] < 1) {
        total_trades <- NULL
      }

    } else {
      total_trades <- NULL
    }


    message(glue::glue("Long Trades SPX, XAG, US2000: {!is.null(total_trades)}"))

    message(glue::glue("Long Trades SPX, XAG, US2000: {!is.null(total_trades)}"))

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
          # filter( (abs(volume_required) >= 0.1 &
          #            Asset %in% c("SPX500_USD", "JP225_USD", "EU50_EUR", "US2000_USD", "SG30_SGD", "AU200_AUD",
          #                         "NAS100_USD", "DE30_EUR", "HK33_HKD")) |
          #           (abs(volume_required) >= 1 &
          #              !(Asset %in% c("SPX500_USD", "JP225_USD", "EU50_EUR", "US2000_USD", "SG30_SGD", "AU200_AUD",
          #                             "NAS100_USD", "DE30_EUR", "HK33_HKD"))
          #           )
          # ) %>%
          slice_max(Date)

        # current_trades <- current_trades_long %>%
        #   bind_rows(current_trades_short) %>%
        #   mutate(trade_col = direction)

        # if(dim(total_trades)[1] > 0) {
        #
        #   total_trades <-
        #     total_trades %>%
        #     left_join(current_trades) %>%
        #     mutate(
        #       std_units = case_when(
        #         # type == "CURRENCY" ~ units/1000,
        #         Asset %in% c("WHEAT_USD", "SOYBN_USD", "XCU_USD", "XAG_USD") ~ units*100,
        #         Asset %in% c("SG30_SGD", "SPX500_USD", "AU200_AUD",
        #                      "JP225_USD", "DE30_EUR", "US2000_USD", "XAU_USD") ~ units*1000,
        #         TRUE ~ units
        #       )
        #     ) %>%
        #     arrange(std_units) %>%
        #     dplyr::select(-std_units, -direction, -units)
        #
        # }

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

    message(glue::glue("Long Trades SPX, XAG, US2000: {!is.null(total_trades)}"))

  }

    data_updated <- 0
    # message(glue::glue("Time is: {current_time}"))

}

