helpeR::load_custom_functions()

all_aud_symbols <- get_oanda_symbols() %>%
  keep(~ str_detect(.x, "AUD")|str_detect(.x, "USD_SEK|USD_NOK|USD_HUF|USD_ZAR|USD_CNY|USD_MXN"))
asset_infor <- get_instrument_info()
aud_assets <- read_all_asset_data_intra_day(
  asset_list_oanda = all_aud_symbols,
  save_path_oanda_assets = "C:/Users/nikhi/Documents/Asset Data/oanda_data/",
  read_csv_or_API = "API",
  time_frame = "D",
  bid_or_ask = "bid",
  how_far_back = 10,
  start_date = (today() - days(4)) %>% as.character()
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
    "JP225_USD", "SPX500_USD",
    "UK10YB_GBP",
    "HK33_HKD", "USD_JPY",
    "BTC_USD",
    "AUD_NZD", "GBP_CHF",
    "EUR_HUF", "EUR_ZAR", "NZD_JPY", "EUR_NZD",
    "USB02Y_USD",
    "XAU_CAD", "GBP_JPY", "EUR_NOK", "USD_SGD", "EUR_SEK",
    "DE30_EUR",
    "AUD_CAD", "NZD_USD", "ETH_USD","BCO_USD", "AUD_USD",
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
    "USB02Y_USD", "USB05Y_USD", "USB30Y_USD", "USB10Y_USD", "UK100_GBP",
    "GBP_CAD"
  ) %>%
  unique()

asset_infor <- get_instrument_info()
raw_macro_data <- get_macro_event_data()
#---------------------Data
load_custom_functions()
db_location = "C:/Users/nikhi/Documents/Asset Data/Oanda_Asset_Data_Most_Assets_2025-09-13 Second Algo.db"
start_date = "2020-06-01"
end_date = today() %>% as.character()

All_Daily_Data <-
  get_DAILY_ALGO_DATA_API_REQUEST()

Indices_Metals_Bonds <- get_Port_Buy_Data(
  db_location = db_location,
  start_date = start_date,
  end_date = today() %>% as.character(),
  time_frame = "H1"
)

Indices_Metals_Bonds[[2]] <- NULL
Indices_Metals_Bonds <- Indices_Metals_Bonds[[1]]
gc()
rm(missing_assets)
gc()
asset_list_oanda_single_asset <-
  Indices_Metals_Bonds %>%
  distinct(Asset) %>%
  pull(Asset) %>%
  as.character()

#-------------Indicator Inputs

end_time <- glue::glue("{floor_date(now(), 'week')} 04:59:00 AEST") %>% as_datetime(tz = "Australia/Canberra") + days(6)
current_time <- now()


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

trade_tracker_DB_path <-
  "C:/Users/nikhi/Documents/trade_data/trade_tracker_daily_buy_close 2.db"
trade_tracker_DB <- connect_db(trade_tracker_DB_path)

db_location = "C:/Users/nikhi/Documents/Asset Data/Oanda_Asset_Data_Most_Assets_2025-09-13 Second Algo.db"
end_date_day = today() %>% as.character()

mean_values_by_asset_for_loop_H1_ask <-
  wrangle_asset_data(
    asset_data_daily_raw =
      get_db_price(
        db_location = db_location,
        start_date = "2020-01-01",
        end_date = end_date_day,
        bid_or_ask = "ask",
        time_frame = "H1"
      ),
    summarise_means = TRUE
  )

rm(starting_asset_data_ask_H1)
trades_opened <- 0
trades_closed <- 0
risk_dollar_value = 5

gc()
load_custom_functions()
gc()

assets_to_use <-
  c("EUR_USD", #1
    "EU50_EUR", #2
    "SPX500_USD", #3
    "US2000_USD", #4
    "USB10Y_USD", #5
    "USD_JPY", #6
    "AUD_USD", #7
    "EUR_GBP", #8
    "AU200_AUD" ,#9
    "EUR_AUD", #10
    "WTICO_USD", #11
    "UK100_GBP", #12
    "USD_CAD", #13
    "GBP_USD", #14
    "GBP_CAD", #15
    "EUR_JPY", #16
    "EUR_NZD", #17
    "XAG_USD", #18
    "XAG_EUR", #19
    "XAG_AUD", #20
    "XAG_NZD", #21
    "HK33_HKD", #22
    "FR40_EUR", #23
    "BTC_USD", #24
    "XAG_GBP", #25
    "GBP_AUD", #26
    "USD_SEK", #27
    "USD_SGD" #28
  )

assets_to_use <- assets_to_use[15:28]
safely_upload_to_db <- safely(update_local_db_file, otherwise = "error")

while (current_time < end_time) {

  current_time <- now() %>% as_datetime()
  current_minute <- lubridate::minute(current_time)
  current_hour <- lubridate::hour(current_time)
  current_date <- now() %>% as_date(tz = "Australia/Canberra")

  #----------------------Refresh Data Stores and LM model
  if(current_minute > 0 &
     current_minute < 5 &
     trades_opened == 0
     # ( (current_hour) == 0)
  ) {

    gc()
    Sys.sleep(2)
    gc()

    #-------------------------------------Update Data
    raw_macro_data <- niksmacrohelpers::get_macro_event_data()
    trades_opened <- 1

    how_far_back_date <- seq(today() - days(15), today(), by =  "days" ) %>%
      keep(
        ~ wday(.x) == 3
      ) %>%
      unlist() %>%
      as_date() %>%
      min()

    how_far_back_var <-
      as.numeric(today() - how_far_back_date)

    days_back_x <- lubridate::wday(today() - hours(14))

    u1 <- safely_upload_to_db(
      db_location = db_location,
      time_frame = "D",
      bid_or_ask = "ask",
      asset_list_oanda = asset_list_oanda,
      how_far_back = how_far_back_var
    ) %>%
      pluck('result')

    u2 <- safely_upload_to_db(
      db_location = db_location,
      time_frame = "H1",
      bid_or_ask = "ask",
      asset_list_oanda = asset_list_oanda,
      how_far_back = how_far_back_var
    )%>%
      pluck('result')

    u3 <- safely_upload_to_db(
      db_location = db_location,
      time_frame = "D",
      bid_or_ask = "bid",
      asset_list_oanda = asset_list_oanda,
      how_far_back = how_far_back_var
    ) %>%
      pluck('result')

    u4 <- safely_upload_to_db(
      db_location = db_location,
      time_frame = "H1",
      bid_or_ask = "bid",
      asset_list_oanda = asset_list_oanda,
      how_far_back = how_far_back_var
    )%>%
      pluck('result')

    if(u1 != "error" & u2 != "error" & u3 != "error" & u4 != "error") {

      Indices_Metals_Bonds <-
        updated_data_internal(
          starting_asset_data = Indices_Metals_Bonds,
          end_date_day = now() + days(1),
          time_frame = "H1",
          bid_or_ask = "ask",
          db_location = db_location) %>%
        distinct() %>%
        filter(Asset %in% asset_list_oanda_single_asset)

      All_Daily_Data <-
        updated_data_internal(
          starting_asset_data = All_Daily_Data,
          end_date_day = now() + days(1),
          time_frame = "D",
          bid_or_ask = "ask",
          db_location = db_location) %>%
        distinct() %>%
        filter(Asset %in% asset_list_oanda_single_asset)


      #--------------------------------------------------Macro Only Trades
      current_prices_ask <-
        read_all_asset_data_intra_day(
          asset_list_oanda = asset_list_oanda,
          save_path_oanda_assets = "C:/Users/nikhi/Documents/Asset Data/oanda_data/",
          read_csv_or_API = "API",
          time_frame = "H1",
          bid_or_ask = "ask",
          how_far_back = 800,
          start_date = as.character(how_far_back_date)
        )%>%
        map_dfr(bind_rows) %>%
        group_by(Asset) %>%
        slice_max(Date) %>%
        ungroup()

      current_prices_bid <-
        read_all_asset_data_intra_day(
          asset_list_oanda = asset_list_oanda,
          save_path_oanda_assets = "C:/Users/nikhi/Documents/Asset Data/oanda_data/",
          read_csv_or_API = "API",
          time_frame = "H1",
          bid_or_ask = "bid",
          how_far_back = 800,
          start_date = as.character(how_far_back_date)
        ) %>%
        map_dfr(bind_rows) %>%
        group_by(Asset) %>%
        slice_max(Date) %>%
        ungroup()

      if(current_hour %% 2 == 0) {
        total_trades_macro_only_port_stops <- NULL
      } else {
        total_trades_macro_only_port_stops <- NULL
      }



      #-----------Single Asset Model

      if( current_hour %% 2 == 0 | current_hour %% 2 != 0) {
        tictoc::tic()
        single_asset_model_trades <-
          single_asset_model_loop_and_trade(
            Indices_Metals_Bonds = Indices_Metals_Bonds,
            All_Daily_Data = All_Daily_Data,
            pre_train_date_end = today() - months(12),
            post_train_date_start = today() - months(12),
            test_date_start = today() - weeks(1),
            test_end_date = today() + weeks(1),
            raw_macro_data = raw_macro_data,
            stop_value_var = 2,
            profit_value_var = 15,
            period_var = 24,
            start_index = 15,
            end_index = 28,
            save_path = "C:/Users/nikhi/Documents/trade_data/single_asset_models_v1/"
          )
        tictoc::toc()

      }

      asset_optimisation_store_path =
        "C:/Users/nikhi/Documents/trade_data/single_asset_improved_asset_optimisation_more_cop 2.db"

      asset_optimisation_store_db <-
        connect_db(asset_optimisation_store_path)

      all_model_results <-
        DBI::dbGetQuery(conn = asset_optimisation_store_db,
                        statement = "SELECT * FROM single_asset_improved_asset_optimisation")
      DBI::dbDisconnect(asset_optimisation_store_db)
      gc()

      best_results <-
        all_model_results %>%
        filter(pred_thresh != "control") %>%
        filter(pred_thresh > 0) %>%
        filter(Mid > 0,
               lower >0
        ) %>%
        group_by(Asset, trade_col) %>%
        # slice_max(Win_Perc_mean, n = 10) %>%
        # group_by(Asset, trade_col) %>%
        # slice_max(Mid, n = 5) %>%
        # group_by(Asset, trade_col) %>%
        # slice_max(total_trades_mean, n = 1) %>%
        slice_max(Mid, n = 5) %>%
        group_by(Asset, trade_col) %>%
        slice_max(simulations) %>%
        group_by(Asset, trade_col) %>%
        slice_min(pred_thresh) %>%
        ungroup()

      single_asset_model_trades_filt <-
        single_asset_model_trades %>%
        left_join(
          best_results %>%
            ungroup() %>%
            dplyr::select(Asset, trade_col, pred_thresh) %>%
            mutate(pred_thresh = as.numeric(pred_thresh))
        ) %>%
        filter(
          (logit_combined_pred >= mean_logit_combined_pred + pred_thresh*sd_logit_combined_pred &
             averaged_pred >=  mean_averaged_pred + sd_averaged_pred*pred_thresh & pred_thresh >= 0)|
            (logit_combined_pred < mean_logit_combined_pred + pred_thresh*sd_logit_combined_pred &
               averaged_pred <  mean_averaged_pred + sd_averaged_pred*pred_thresh & pred_thresh < 0)
        ) %>%
        # filter(
        #   (logit_combined_pred >= mean_logit_combined_pred + 0*sd_logit_combined_pred &
        #      averaged_pred >=  mean_averaged_pred + sd_averaged_pred*0 & 0 >= 0)|
        #     (logit_combined_pred < mean_logit_combined_pred + 0*sd_logit_combined_pred &
        #        averaged_pred <  mean_averaged_pred + sd_averaged_pred*0 & 0 < 0)
        # ) %>%
        group_by(Asset) %>%
        mutate(
          abs_logit_pred = abs(logit_combined_pred)
        ) %>%
        group_by(Asset) %>%
        slice_max(abs_logit_pred) %>%
        dplyr::select(Date, Asset, trade_col, stop_factor, profit_factor, periods_ahead) %>%
        left_join(current_prices_ask %>% dplyr::select(Asset, Price, Open, High, Low)) %>%
        filter(!is.na(Price))  %>%
        ungroup()

      #------Second Phase Optimisation
      best_trade_setups <-
        get_best_trade_setup_sa(
          model_optimisation_store_path =
            "C:/Users/nikhi/Documents/trade_data/single_asset_advanced_optimisation.db",
          table_to_extract = "summary_for_reg"
        )

      single_asset_model_trades_filt <-
        single_asset_model_trades_filt %>%
        rename(trade_col_og = trade_col) %>%
        dplyr::select(Date, Asset, Price, Open, High, Low, trade_col_og) %>%
        left_join(best_trade_setups[[2]] ) %>%
        mutate(
          trade_col_revised =
            case_when(
              trade_col_og == "Short" & trade_col == "Long" ~ "Short",
              trade_col_og == "Short" & trade_col == "Short" ~ "Long",
              TRUE ~ trade_col
            )
        ) %>%
        mutate(
          trade_col = trade_col_revised
        ) %>%
        mutate(
          risk_dollar_value =
            case_when(
              trade_col_og == "Short" & trade_col_revised == "Long" ~ 5,
              trade_col_og == "Short" & trade_col_revised == "Short" ~ 2,

              trade_col_og == "Long" & trade_col_revised == "Long" ~ 2,
              trade_col_og == "Long" & trade_col_revised == "Short" ~ 5,
            ),
          required_risk = risk_dollar_value,
        ) %>%
        dplyr::select(-trade_col_revised, -trade_col_og) %>%
        rename(
          periods_ahead = period_var
        ) %>%
        distinct()



      if(dim(single_asset_model_trades_filt)[1] > 0) {
        single_asset_model_trades_filt <-
          single_asset_model_trades_filt%>%
          mutate(kk = row_number()) %>%
          split(.$kk) %>%
          map_dfr(
            ~
              get_stops_profs_volume_trades(
                tagged_trades = .x,
                mean_values_by_asset = mean_values_by_asset_for_loop_H1_ask,
                trade_col = "trade_col",
                currency_conversion = currency_conversion,
                risk_dollar_value = .x$risk_dollar_value[1] %>% as.numeric(),
                stop_factor = .x$stop_factor[1] %>% as.numeric(),
                profit_factor = .x$profit_factor[1] %>% as.numeric(),
                asset_col = "Asset",
                stop_col = "stop_value",
                profit_col = "profit_value",
                price_col = "Price",
                trade_return_col = "trade_returns"
              )
          ) %>%
          mutate(
            periods_ahead = as.character(periods_ahead)
          ) %>%
          group_by(Asset, trade_col) %>%
          mutate(
            trades_to_keep =
              case_when(
                minimal_loss <= 1.25*required_risk ~ "Keep",
                minimal_loss > 1.25*required_risk & profit_factor == max(profit_factor, na.rm = T) ~ "Keep"
              )
          ) %>%
          ungroup() %>%
          filter(trades_to_keep == "Keep") %>%
          distinct()

      } else {
        single_asset_model_trades_filt <- NULL
      }

      #-------------------------All Trades
      total_trades <-
        list(total_trades_macro_only_port_stops,
             single_asset_model_trades_filt) %>%
        map_dfr(bind_rows)

      rm(single_asset_model_trades_filt,
         total_trades_macro_only_port_stops,
         single_asset_model_trades,
         raw_macro_data,
         total_trades_macro_only_port)

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

            cleaned_trade_details <-
              extract_put_request_return(http_return) %>%
              mutate(Asset = asset,
                     trade_col = trade_direction,
                     account_var = long_account_num,
                     account_name = account_name_long,
                     status = "OPEN",
                     periods_ahead = total_trades$periods_ahead[i] %>% as.numeric())

            append_table_sql_lite(.data = cleaned_trade_details,
                                  table_name = "trade_tracker",
                                  conn = trade_tracker_DB)

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

            cleaned_trade_details <-
              extract_put_request_return(http_return) %>%
              mutate(Asset = asset,
                     trade_col = trade_direction,
                     account_var = short_account_num,
                     account_name = account_name_short,
                     status = "OPEN",
                     periods_ahead = total_trades$periods_ahead[i] %>% as.numeric())

            append_table_sql_lite(.data = cleaned_trade_details,
                                  table_name = "trade_tracker",
                                  conn = trade_tracker_DB)

          }

        }

      }

    }

    gc()
    Sys.sleep(2)
    gc()

  }

  if(trades_closed == 0 ) {

    rm(positions_tagged_as_part_of_algo)

    trades_closed <- 1

    trades_from_DB <-
      DBI::dbGetQuery(conn = trade_tracker_DB,
                      statement = "SELECT * FROM trade_tracker") %>%
      filter(
        Asset %in% assets_to_use
      )

    if(dim(trades_from_DB)[1] > 0){

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

      account_Short_1_Ids <-
        trades_from_DB %>%
        filter(status == "OPEN") %>%
        filter(account_var == short_account_num) %>%
        dplyr::select(tradeID, Asset, periods_ahead, account_name)

      account_Short_2_Ids <-
        trades_from_DB %>%
        filter(status == "OPEN") %>%
        filter(account_var == short_account_num_equity) %>%
        dplyr::select(tradeID, Asset, periods_ahead, account_name)

      open_positions_account_long1 <-
        get_Open_positions(account_var = long_account_num) %>%
        mutate(id = as.character(id)) %>%
        left_join(
          account_Long_1_Ids, by = c("id" = "tradeID", "Asset")
        ) %>%
        filter(!is.na(periods_ahead))

      # message(open_positions_account_long1)

      open_positions_account_long2 <-
        get_Open_positions(account_var = long_account_num_equity)  %>%
        mutate(id = as.character(id)) %>%
        left_join(
          account_Long_2_Ids, by = c("id" = "tradeID", "Asset")
        ) %>%
        filter(!is.na(periods_ahead))

      # message(open_positions_account_long2)

      open_positions_account_short1 <-
        get_Open_positions(account_var = short_account_num) %>%
        mutate(id = as.character(id)) %>%
        left_join(
          account_Short_1_Ids, by = c("id" = "tradeID", "Asset")
        ) %>%
        filter(!is.na(periods_ahead))

      # message(open_positions_account_short1)

      open_positions_account_short2 <-
        get_Open_positions(account_var = short_account_num_equity)  %>%
        mutate(id = as.character(id)) %>%
        left_join(
          account_Short_2_Ids, by = c("id" = "tradeID", "Asset")
        ) %>%
        filter(!is.na(periods_ahead))

      # message(open_positions_account_short2)

      positions_tagged_as_part_of_algo_raw <-
        open_positions_account_long1 %>%
        mutate(openTime = as_datetime(openTime, tz = "Australia/Canberra"),
               time_in_process =  abs(as.numeric(current_time - openTime, units = "hours")),
               flagged_for_close = time_in_process >= periods_ahead) %>%
        bind_rows(
          open_positions_account_long2 %>%
            mutate(openTime = as_datetime(openTime, tz = "Australia/Canberra"),
                   time_in_process =  abs(as.numeric(current_time - openTime, units = "hours")),
                   flagged_for_close = time_in_process >= periods_ahead)
        )%>%
        bind_rows(
          open_positions_account_short1 %>%
            mutate(openTime = as_datetime(openTime, tz = "Australia/Canberra"),
                   time_in_process =  abs(as.numeric(current_time - openTime, units = "hours")),
                   flagged_for_close = time_in_process >= periods_ahead)
        )%>%
        bind_rows(
          open_positions_account_short2 %>%
            mutate(openTime = as_datetime(openTime, tz = "Australia/Canberra"),
                   time_in_process =  abs(as.numeric(current_time - openTime, units = "hours")),
                   flagged_for_close = time_in_process >= periods_ahead)
        ) %>%
        mutate(time_in_process = as.numeric(time_in_process))

      estimated_running_profit <-
        positions_tagged_as_part_of_algo_raw %>%
        mutate(unrealizedPL = as.numeric(unrealizedPL)) %>%
        summarise(unrealizedPL = sum(unrealizedPL, na.rm = T),
                  EstimatedTotal_risk = risk_dollar_value*n())

      if(estimated_running_profit$unrealizedPL[1] < 3.5*estimated_running_profit$EstimatedTotal_risk[1] ) {
        positions_tagged_as_part_of_algo <-
          positions_tagged_as_part_of_algo_raw %>%
          filter(
            (flagged_for_close  == TRUE| time_in_process >= periods_ahead | time_in_process >= 48) &
              id != "18487"
          )
      }

      if(estimated_running_profit$unrealizedPL[1] >= 3.5*estimated_running_profit$EstimatedTotal_risk[1] ) {
        positions_tagged_as_part_of_algo <-
          positions_tagged_as_part_of_algo_raw %>%
          filter(
            id != "18487"
          )
      }

      if(dim(positions_tagged_as_part_of_algo)[1] > 0) {

        positions_tagged_as_part_of_algo <-
          positions_tagged_as_part_of_algo %>%
          mutate(
            market_open_times =
              case_when(
                Asset %in% c("CH20_CHF", "EU50_EUR") &
                  lubridate::wday(today()) == 2 & current_hour < 18 ~ FALSE,
                Asset %in% c("UK100_GBP") &
                  lubridate::wday(today()) == 2 & current_hour <= 12 ~ FALSE,
                TRUE ~ TRUE
              )
          ) %>%
          filter(market_open_times == TRUE)

      }

      if(dim(positions_tagged_as_part_of_algo)[1] > 0) {

        for (i in 1:dim(positions_tagged_as_part_of_algo)[1] ) {

          account_name_for_close <-
            positions_tagged_as_part_of_algo$account_name[i] %>% as.character()
          account_num_for_close <-
            positions_tagged_as_part_of_algo$account_var[i] %>% as.character()
          id_for_close <-
            positions_tagged_as_part_of_algo$id[i] %>% as.character()
          units_for_close <-
            positions_tagged_as_part_of_algo$currentUnits[i] %>% as.numeric()

          returned_code <- oanda_close_trade_ID(
            tradeID = id_for_close,
            units = units_for_close,
            account = account_name_for_close
          )

          check_if_position_still_open <-
            get_Open_positions(account_var = account_num_for_close) %>%
            mutate(id = as.character(id)) %>%
            filter(id == id_for_close )

          Sys.sleep(1)

          if(dim(check_if_position_still_open)[1] > 0) {
            oanda_close_trade_ID(
              tradeID = id_for_close,
              units = units_for_close,
              account = account_name_for_close
            )
          }

          Sys.sleep(2)

        }

      }
    }


    Sys.sleep(60)

    rm(positions_tagged_as_part_of_algo, check_if_position_still_open)


  }


  if( current_minute >= 59 & current_minute <= 59 ) {
    trades_opened <- 0
    trades_closed <- 0
  }

  if( current_minute >= 58 & current_minute <= 58 ) {
    trades_closed <- 0
  }

  if( current_minute >= 57 & current_minute <= 57 ) {
    trades_closed <- 0
  }

  if( current_minute >= 56 & current_minute <= 56 ) {
    trades_closed <- 0
  }

  if( current_minute >= 5 & current_minute <= 5 ) {
    trades_closed <- 0
    Sys.sleep(60)
  }

  if( current_minute >= 10 & current_minute <= 10 ) {
    trades_closed <- 0
    Sys.sleep(60)
  }

  if( current_minute >= 15 & current_minute <= 15 ) {
    trades_closed <- 0
    Sys.sleep(60)
  }

  if( current_minute >= 20 & current_minute <= 20 ) {
    trades_closed <- 0
    Sys.sleep(60)
  }

  if( current_minute >= 25 & current_minute <= 25 ) {
    trades_closed <- 0
    Sys.sleep(60)
  }

  if( current_minute >= 30 & current_minute <= 30 ) {
    trades_closed <- 0
    Sys.sleep(60)
  }

  if( current_minute >= 35 & current_minute <= 35 ) {
    trades_closed <- 0
    Sys.sleep(60)
  }

  if( current_minute >= 40 & current_minute <= 40 ) {
    trades_closed <- 0
    Sys.sleep(60)
  }

  if( current_minute >= 45 & current_minute <= 45 ) {
    trades_closed <- 0
    Sys.sleep(60)
  }

  if( current_minute >= 55 & current_minute <= 55 ) {
    trades_closed <- 0
    Sys.sleep(60)
  }

}
