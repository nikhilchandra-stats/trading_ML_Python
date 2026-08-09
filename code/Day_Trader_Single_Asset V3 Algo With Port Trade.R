helpeR::load_custom_functions()

all_aud_symbols <- get_oanda_symbols() %>%
  keep(~ str_detect(.x, "AUD")|str_detect(.x, "USD_SEK|USD_NOK|USD_HUF|USD_ZAR|USD_CNY|USD_MXN"))
asset_infor <- get_instrument_info()
aud_assets <- read_all_asset_data_intra_day(
  asset_list_oanda = all_aud_symbols,
  save_path_oanda_assets = "C:/Users/nikhi/Documents//Asset Data/oanda_data/",
  read_csv_or_API = "API",
  time_frame = "D",
  bid_or_ask = "bid",
  how_far_back = 12,
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
# raw_macro_data <- get_macro_event_data()
#---------------------Data
load_custom_functions()
db_location = "C:/Users/nikhi/Documents//Asset Data/Oanda_Asset_Data_Most_Assets_2025-09-13.db"
start_date = "2022-01-01"
end_date = today() %>% as.character()

Indices_Metals_Bonds <- list()

assets_to_port <-
  c(
    "SPX500_USD",
    "CH20_CHF",
    "DE30_EUR",
    "EU50_EUR",
    "US2000_USD",
    "HK33_HKD",
    "JP225Y_JPY",
    "UK100_GBP"
  ) %>% unique()

assets_to_trade <-
  c(
    "SPX500_USD",
    "CH20_CHF",
    "DE30_EUR",
    "EU50_EUR",
    "US2000_USD",
    "HK33_HKD",
    "JP225Y_JPY",
    "UK100_GBP"
  ) %>% unique()

Indices_Metals_Bonds[[1]] <-
  get_db_data_quickly_algo(
    db_location = db_location,
    start_date = start_date,
    end_date = as.character(today() + days(30)),
    time_frame = "H1",
    bid_or_ask = "ask",
    assets = assets_to_port
  ) %>%
  distinct()
Indices_Metals_Bonds[[2]] <-
  get_db_data_quickly_algo(
    db_location = db_location,
    start_date = start_date,
    end_date = as.character(today() + days(30)),
    time_frame = "H1",
    bid_or_ask = "bid",
    assets = assets_to_port
  ) %>%
  distinct()

gc()
rm(missing_assets)
gc()
asset_list_oanda_single_asset <-
  Indices_Metals_Bonds[[1]] %>%
  distinct(Asset) %>%
  pull(Asset) %>%
  as.character()

#-------------Indicator Inputs

end_time <- glue::glue("{floor_date(now(), 'week')} 07:59:00 AEST") %>% as_datetime(tz = "Australia/Canberra") + days(6)
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
  "C:/Users/nikhi/Documents//trade_data/trade_tracker_daily_buy_close endpoints.db"
trade_tracker_DB <- connect_db(trade_tracker_DB_path)

db_location = "C:/Users/nikhi/Documents//Asset Data/Oanda_Asset_Data_Most_Assets_2025-09-13.db"
end_date_day = today() %>% as.character()

mean_values_by_asset_for_loop_H1_ask <-
  wrangle_asset_data(
    asset_data_daily_raw = Indices_Metals_Bonds[[1]],
    summarise_means = TRUE
  )

rm(starting_asset_data_ask_H1)
trades_opened <- 0
trades_closed <- 0

gc()
load_custom_functions()
gc()

assets_to_use <- assets_to_port

db_con_trade_statements <-
  connect_db("C:/Users/nikhi/Documents/trade_data/single_asset_v3_Bayes_Reg_Portfolio/Port_V3_Results_Store.db")

trade_statement_DFR <-
  DBI::dbGetQuery(conn = db_con_trade_statements,
                               statement = "SELECT * FROM Port_V3_Sim_Data" ) %>%
  mutate(
    Date = as_datetime(Date, tz = "Australia/Canberra")
  ) %>%
  distinct(algo_name, trade_statement) %>%
  filter(!is.na(trade_statement))

DBI::dbDisconnect(db_con_trade_statements)
rm(db_con_trade_statements)

safely_upload_to_db <- safely(update_local_db_file, otherwise = "error")
run_trades = FALSE

while (current_time < end_time) {

  current_time <- now() %>% as_datetime()
  current_minute <- lubridate::minute(current_time)
  current_hour <- lubridate::hour(current_time)
  current_date <- now() %>% as_date(tz = "Australia/Canberra")

  #----------------------Refresh Data Stores and LM model
  if(current_minute > 0 &
     current_minute < 5 &
     trades_opened == 0 &
     run_trades == TRUE
     # ( (current_hour) == 0)
  ) {

    gc()
    Sys.sleep(2)
    gc()

    #-------------------------------------Update Data
    raw_macro_data <- NULL
    trades_opened <- 1
    how_far_back_date <- seq(today() - days(28), today(), by =  "days" ) %>%
      keep(
        ~ wday(.x) == 3
      ) %>%
      unlist() %>%
      as_date() %>%
      min()

    how_far_back_var <-
      as.numeric(today() - how_far_back_date)

    u1 <- "pass"

    u2 <- safely_upload_to_db(
      db_location = db_location,
      time_frame = "H1",
      bid_or_ask = "ask",
      asset_list_oanda = asset_list_oanda,
      how_far_back = how_far_back_var,
      take_last_value = TRUE
    )%>%
      pluck('result')

    u3 <- "pass"

    u4 <- safely_upload_to_db(
      db_location = db_location,
      time_frame = "H1",
      bid_or_ask = "bid",
      asset_list_oanda = asset_list_oanda,
      how_far_back = how_far_back_var,
      take_last_value = TRUE
    )%>%
      pluck('result')

    if(u2 == "error") {
      Sys.sleep(30)
      u2 <- safely_upload_to_db(
        db_location = db_location,
        time_frame = "H1",
        bid_or_ask = "ask",
        asset_list_oanda = asset_list_oanda,
        how_far_back = how_far_back_var
      )%>%
        pluck('result')
    }

    if(u4 == "error") {
      Sys.sleep(30)
      u4 <- safely_upload_to_db(
        db_location = db_location,
        time_frame = "H1",
        bid_or_ask = "bid",
        asset_list_oanda = asset_list_oanda,
        how_far_back = how_far_back_var
      )%>%
        pluck('result')
    }

    if(u1 != "error" & u2 != "error" & u3 != "error" & u4 != "error") {

      Indices_Metals_Bonds <- list()

      Indices_Metals_Bonds[[1]] <-
        get_db_data_quickly_algo(
          db_location = db_location,
          start_date = start_date,
          end_date = as.character(today() + days(30)),
          time_frame = "H1",
          bid_or_ask = "ask",
          assets = assets_to_port
        ) %>%
        distinct()

      Indices_Metals_Bonds[[2]] <-
        get_db_data_quickly_algo(
          db_location = db_location,
          start_date = start_date,
          end_date = as.character(today() + days(30)),
          time_frame = "H1",
          bid_or_ask = "bid",
          assets = assets_to_port
        ) %>%
        distinct()

      #-----------Single Asset Model

      if(
        run_trades == TRUE
        # current_hour != 0
      ) {

        tictoc::tic()
        all_preds <-
          Portfolio_get_all_preds_frm_V3(
            Indices_Metals_Bonds = Indices_Metals_Bonds,
            raw_macro_data = raw_macro_data,
            base_path = "C:/Users/nikhi/Documents/trade_data/Day_Trader_Single_Asset_V3_Expanded_Models/",
            actuals_periods_needed = c("period_return_50_Price"),
            state_space_periods = c(20, 40, 60, 100, 200,300, 400,  500),
            state_space_rolling = c(100, 200, 300, 400),
            date_for_true_simualtion = "2019-01-01",
            training_end_date = "2021-01-01",
            assets_to_test = assets_to_port
          )

        GLOBAL_EQUITY_Assets <-
          c(
            "SPX500_USD",
            "CH20_CHF",
            "DE30_EUR",
            "EU50_EUR",
            "US2000_USD",
            "HK33_HKD",
            "JP225Y_JPY",
            "UK100_GBP"
          ) %>% unique()

        stop_factor_var = 10
        profit_factor_var = 50
        risk_dollar_value_var = 5
        end_period = 72
        trade_direction = "Long"
        end_point_loss = -5
        end_point_profit = 100
        training_date <-  "2022-01-17 10:00:00 AEST"
        save_path = "C:/Users/nikhi/Documents/trade_data/single_asset_v3_Bayes_Reg_Portfolio/"
        file_name = "GLOBAL_EQUITY_Extd_risk10_stop10_prof50_SIG1"
        regression_length = 18000
        direct_return_cols = end_period
        total_lag_cols = end_period + 1
        sig_thresh_LM = 1
        xtnd_vars = TRUE
        xtnd_ss_cols_PR_cols = 30
        xtnd_ss_cols_BR_periods = c(100,200,300, 400)
        low_to_price_lengths = c(400)
        cor_periods = c(100)
        max_regs = 1000

        EQUITY_PRED_DATA <-
          portfolio_V3_TOTAL_SUM_get_preds_algo(
            Indices_Metals_Bonds =
              Indices_Metals_Bonds %>%
              map(~ .x %>% filter(Asset %in% GLOBAL_EQUITY_Assets)),
            all_preds = all_preds %>% filter(Asset %in% GLOBAL_EQUITY_Assets),
            assets_to_port = GLOBAL_EQUITY_Assets,
            assets_to_trade = GLOBAL_EQUITY_Assets,
            stop_factor_var = stop_factor_var,
            profit_factor_var = profit_factor_var,
            risk_dollar_value_var = risk_dollar_value_var,
            end_period = end_period,
            trade_direction = trade_direction,
            end_point_loss = end_point_loss,
            end_point_profit = end_point_profit,
            training_date =  training_date,
            low_to_price_lengths = low_to_price_lengths,
            cor_periods = cor_periods,
            max_regs = max_regs,
            save_path = save_path,
            file_name = file_name,
            regression_length = regression_length,
            total_lag_cols = total_lag_cols,
            direct_return_cols = direct_return_cols,
            remove_NA_Values = TRUE,
            xtnd_vars =xtnd_vars,
            xtnd_ss_cols_PR_cols =xtnd_ss_cols_PR_cols,
            xtnd_ss_cols_BR_periods = xtnd_ss_cols_BR_periods
          )

        EQUITY_TRADE_STATEMENT <-
          trade_statement_DFR %>%
          filter(algo_name == "GLOBAL_EQUITY_Extd_risk10_stop10_prof50_SIG1") %>%
          pull(trade_statement) %>%
          pluck(1) %>%
          as.character()

        trade_dates_EQUITY <-
          EQUITY_PRED_DATA %>%
          ungroup() %>%
          slice_max(Date) %>%
          mutate(
            trade_col =
              eval(parse(text = EQUITY_TRADE_STATEMENT))
          ) %>%
          ungroup() %>%
          filter(trade_col == TRUE) %>%
          distinct(Date)

        trade_dates_EQUITY <-
          GLOBAL_EQUITY_Assets %>%
          map_dfr(~ trade_dates_EQUITY %>% mutate(Asset = .x) )

        tictoc::toc()

        max_date_in_data <- floor_date(as_datetime(now(), tz = "Australia/Canberra"), "hour")
        rm(Indices_Metals_Bonds)
        gc()

        current_prices_ask <-
          read_all_asset_data_intra_day(
            asset_list_oanda = assets_to_port,
            save_path_oanda_assets = "C:/Users/nikhi/Documents//Asset Data/oanda_data/",
            read_csv_or_API = "API",
            time_frame = "H1",
            bid_or_ask = "ask",
            how_far_back = 2,
            start_date = as_date(today() - days(3))
          )%>%
          map_dfr(bind_rows) %>%
          group_by(Asset) %>%
          slice_max(Date) %>%
          ungroup()

        single_asset_model_trades_filt <-
          trade_dates_EQUITY %>%
          mutate(trade_col = "Long",
                 stop_factor = stop_factor_var,
                 profit_factor = profit_factor_var,
                 periods_ahead = end_period,
                 risk_dollar_value = risk_dollar_value_var,
                 end_point_loss = end_point_loss,
                 end_point_profit = end_point_profit
          ) %>%
          group_by(Asset) %>%
          slice_max(Date) %>%
          ungroup() %>%
          left_join(current_prices_ask %>%
                      group_by(Asset) %>%
                      slice_max(Date) %>%
                      ungroup() %>%
                      dplyr::select(-Date)) %>%
          mutate(
            time_diff =
              abs(
                as.numeric(
                  as_datetime(Date, tz = "Australia/Canberra") -
                    as_datetime(current_time, tz = "Australia/Canberra"),
                  units = "mins"
                )
              ),
            date_check = max_date_in_data <= Date
          ) %>%
          group_by(Asset) %>%
          slice_min(time_diff) %>%
          ungroup() %>%
          filter(time_diff <= 70 & date_check == TRUE) %>%
          filter(max_date_in_data <= Date)

      } else {

        single_asset_model_trades_filt <-
          tibble(xx = "a") %>%
          filter(xx == "b")

      }

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
            periods_ahead = as.character(periods_ahead),
            end_point_loss = as.character(end_point_loss),
            end_point_profit = as.character(end_point_profit)
          ) %>%
          ungroup() %>%
          distinct()

      } else {
        single_asset_model_trades_filt <- NULL
      }

      #-------------------------All Trades
      total_trades <-
        list(single_asset_model_trades_filt) %>%
        map_dfr(bind_rows)

      rm(
        # single_asset_model_trades_filt,
        total_trades_macro_only_port_stops,
        single_asset_model_trades,
        raw_macro_data,
        total_trades_macro_only_port)

      account_details_long <- get_account_summary(account_var = long_account_num)
      margain_available_long <- account_details_long$marginAvailable %>% as.numeric()
      margain_used_long <- account_details_long$marginUsed%>% as.numeric()
      total_margain_long <- margain_available_long + margain_used_long
      percentage_margain_available_long <- margain_available_long/total_margain_long

      if(dim(total_trades)[1] > 0 & percentage_margain_available_long[1] > margain_threshold) {

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

          if(trade_direction == "Long") {

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
                     periods_ahead = total_trades$periods_ahead[i] %>% as.numeric(),
                     end_point_loss = total_trades$end_point_loss[i] %>% as.numeric(),
                     end_point_profit = total_trades$end_point_profit[i] %>% as.numeric()
              )

            append_table_sql_lite(.data = cleaned_trade_details,
                                  table_name = "trade_tracker_endpoints",
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
                     periods_ahead = total_trades$periods_ahead[i] %>% as.numeric(),
                     end_point_loss = total_trades$end_point_loss[i] %>% as.numeric(),
                     end_point_profit = total_trades$end_point_profit[i] %>% as.numeric() )

            append_table_sql_lite(.data = cleaned_trade_details,
                                  table_name = "trade_tracker_endpoints",
                                  conn = trade_tracker_DB)

          }

        }

      }

    }

    if(u1 == "error" | u2 == "error" | u3 == "error" | u4 == "error") {
      gc()
      Sys.sleep(540)
    }

    gc()
    Sys.sleep(2)
    gc()
    trades_closed = 0
    trades_opened <- 0

  }


# Close Trades ------------------------------------------------------------

  if(
    now(tzone = "Australia/Canberra") < as_datetime("2026-08-10 06:00:00", tz = "Australia/Canberra")
  ){
    Sys.sleep(600)
  }

  if(trades_closed == 0 &
     now(tzone = "Australia/Canberra") >= as_datetime("2026-08-10 07:00:00", tz = "Australia/Canberra") ) {

    rm(positions_tagged_as_part_of_algo)

    trades_closed <- 1

    trades_from_DB <-
      DBI::dbGetQuery(conn = trade_tracker_DB,
                      statement = "SELECT * FROM trade_tracker_endpoints") %>%
      filter(
        Asset %in% assets_to_use
      )

    if(dim(trades_from_DB)[1] > 0){

      account_Long_1_Ids <-
        trades_from_DB %>%
        filter(status == "OPEN") %>%
        filter(account_var == long_account_num) %>%
        dplyr::select(tradeID, Asset, periods_ahead, account_name, end_point_loss, end_point_profit)

      account_Long_2_Ids <-
        trades_from_DB %>%
        filter(status == "OPEN") %>%
        filter(account_var == long_account_num_equity) %>%
        dplyr::select(tradeID, Asset, periods_ahead, account_name, end_point_loss, end_point_profit)

      account_Short_1_Ids <-
        trades_from_DB %>%
        filter(status == "OPEN") %>%
        filter(account_var == short_account_num) %>%
        dplyr::select(tradeID, Asset, periods_ahead, account_name, end_point_loss, end_point_profit)

      account_Short_2_Ids <-
        trades_from_DB %>%
        filter(status == "OPEN") %>%
        filter(account_var == short_account_num_equity) %>%
        dplyr::select(tradeID, Asset, periods_ahead, account_name, end_point_loss, end_point_profit)

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
        mutate(time_in_process = as.numeric(time_in_process),
               unrealizedPL = as.numeric(unrealizedPL),
               trueUnrealizedPL = as.numeric(trueUnrealizedPL),
               end_point_profit = as.numeric(end_point_profit),
               end_point_loss = as.numeric(end_point_loss) ) %>%
        mutate(
          flagged_for_close =
            (time_in_process >= periods_ahead) | (trueUnrealizedPL >= end_point_profit) |
            (trueUnrealizedPL <= end_point_loss),
          how_far_from_profit = ( abs(trueUnrealizedPL) - abs(end_point_profit) ),
          how_far_from_loss = ( abs(trueUnrealizedPL) - abs(end_point_loss) )
        )

      positions_tagged_as_part_of_algo <-
        positions_tagged_as_part_of_algo_raw %>%
        filter(
          (time_in_process >= periods_ahead) | (trueUnrealizedPL >= end_point_profit) |
            (trueUnrealizedPL <= end_point_loss) | flagged_for_close == TRUE
        )


      if(dim(positions_tagged_as_part_of_algo)[1] > 0) {

        positions_tagged_as_part_of_algo <-
          positions_tagged_as_part_of_algo %>%
          mutate(
            market_open_times =
              case_when(
                Asset %in% c("CH20_CHF") &
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


    Sys.sleep(5)

    rm(positions_tagged_as_part_of_algo, check_if_position_still_open)


  }


  if( current_minute >= 59 & current_minute <= 59 ) {
    trades_opened <- 0
    trades_closed <- 0
  }

  if( current_minute >= 58 & current_minute <= 59 ) {
    trades_closed <- 0
    trades_opened <- 0
  }

  if( current_minute >= 57 & current_minute <= 58 ) {
    trades_closed <- 0
    trades_opened <- 0
  }

  if( current_minute >= 56 & current_minute <= 57 ) {
    trades_closed <- 0
    trades_opened <- 0
  }

  if( current_minute >= 5 & current_minute <= 5 ) {
    trades_closed <- 0
    Sys.sleep(10)
  }

  if( current_minute >= 10 & current_minute <= 10 ) {
    trades_closed <- 0
    Sys.sleep(10)
  }

  if( current_minute >= 15 & current_minute <= 15 ) {
    trades_closed <- 0
    Sys.sleep(10)
  }

  if( current_minute >= 20 & current_minute <= 20 ) {
    trades_closed <- 0
    Sys.sleep(10)
  }

  if( current_minute >= 25 & current_minute <= 25 ) {
    trades_closed <- 0
    Sys.sleep(10)
  }

  if( current_minute >= 30 & current_minute <= 30 ) {
    trades_closed <- 0
    Sys.sleep(10)
  }

  if( current_minute >= 35 & current_minute <= 35 ) {
    trades_closed <- 0
    Sys.sleep(10)
  }

  if( current_minute >= 40 & current_minute <= 40 ) {
    trades_closed <- 0
    Sys.sleep(10)
  }

  if( current_minute >= 45 & current_minute <= 45 ) {
    trades_closed <- 0
    Sys.sleep(10)
  }

  if( current_minute >= 55 & current_minute <= 55 ) {
    trades_closed <- 0
    trades_opened <- 0
    Sys.sleep(10)
  }

  if( current_minute >= 54 & current_minute <= 55 ) {
    trades_closed <- 0
    trades_opened <- 0
    Sys.sleep(10)
  }

  # if(now(tzone = "Australia/Canberra") < as_datetime("2026-07-13 07:00:00", tz = "Australia/Canberra")) {
  #   time_remaining <-
  #     as.numeric(as_datetime("2026-07-13 07:00:00", tz = "Australia/Canberra") - now(tzone = "Australia/Canberra"), "mins")
  #   Sys.sleep(time_remaining*60 - 60*10)
  # }

}
