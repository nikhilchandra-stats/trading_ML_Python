helperfunctions35South::load_custom_functions()
one_drive_path <- helperfunctions35South::create_one_drive_path(
  path_extension = "raw data")
library(neuralnet)


all_aud_symbols <- get_oanda_symbols() %>%
  keep(~ str_detect(.x, "AUD"))

asset_list_oanda <- get_oanda_symbols() %>%
  keep( ~ .x %in% c("HK33_HKD", "USD_JPY",
                    # "BTC_USD",
                    "AUD_NZD", "GBP_CHF",
                    "EUR_HUF", "EUR_ZAR", "NZD_JPY", "EUR_NZD",
                    # "USB02Y_USD",
                    "XAU_CAD", "GBP_JPY", "EUR_NOK", "USD_SGD", "EUR_SEK",
                    "DE30_EUR",
                    "AUD_CAD",
                    # "UK10YB_GBP",
                    "XPD_USD",
                    "UK100_GBP",
                    "USD_CHF", "GBP_NZD",
                    "GBP_SGD", "USD_SEK", "EUR_SGD", "XCU_USD", "SUGAR_USD", "CHF_ZAR",
                    "AUD_CHF", "EUR_CHF", "USD_MXN", "GBP_USD", "WTICO_USD", "EUR_JPY", "USD_NOK",
                    "XAU_USD",
                    # "DE10YB_EUR",
                    "USD_CZK", "AUD_SGD", "USD_HUF", "WHEAT_USD",
                    "EUR_USD", "SG30_SGD", "GBP_AUD", "NZD_CAD", "AU200_AUD", "XAG_USD",
                    "XAU_EUR", "EUR_GBP", "USD_CNH", "USD_CAD", "NAS100_USD",
                    # "USB10Y_USD",
                    "EU50_EUR", "NATGAS_USD", "CAD_JPY", "FR40_EUR", "USD_ZAR", "XAU_GBP",
                    # "CH20_CHF", "ESPIX_EUR",
                    # "XPT_USD",
                    "EUR_AUD", "SOYBN_USD",
                    # "US2000_USD",
                    "BCO_USD", "AUD_USD", "NZD_USD", "NZD_CHF", "WHEAT_USD", "AUD_JPY", "AUD_SEK")
  )

asset_infor <- get_instrument_info()
#Long opens on Ask
#Short opens on Bid

extracted_asset_data_ask <-
  read_all_asset_data_intra_day(
    asset_list_oanda = asset_list_oanda,
    save_path_oanda_assets = "C:/Users/Nikhil Chandra/Documents/Asset Data/oanda_data/",
    read_csv_or_API = "API",
    time_frame = "D",
    bid_or_ask = "ask",
    how_far_back = 5000,
    start_date = "2011-01-01"
  )

asset_data_combined_ask <- extracted_asset_data_ask %>% map_dfr(bind_rows)
asset_data_combined_ask <- asset_data_combined_ask %>%
  mutate(Date = as_date(Date))
asset_data_daily_raw_ask <-asset_data_combined_ask

extracted_asset_data_bid <-
  read_all_asset_data_intra_day(
    asset_list_oanda = asset_list_oanda,
    save_path_oanda_assets = "C:/Users/Nikhil Chandra/Documents/Asset Data/oanda_data/",
    read_csv_or_API = "API",
    time_frame = "D",
    bid_or_ask = "bid",
    how_far_back = 5000,
    start_date = "2011-01-01"
  )

asset_data_combined_bid <- extracted_asset_data_bid %>% map_dfr(bind_rows)
asset_data_combined_bid <- asset_data_combined_bid %>%
  mutate(Date = as_date(Date))
asset_data_daily_raw_bid <-asset_data_combined_bid

#-----------------------------------------------------Required Primatives
aud_usd_today <- get_aud_conversion(asset_data_daily_raw = asset_data_daily_raw_bid)

currency_conversion <-
  aud_usd_today %>%
  mutate(
    not_aud_asset = ending_value
  ) %>%
  dplyr::select(not_aud_asset, adjusted_conversion) %>%
  bind_rows(
    tibble(not_aud_asset = "AUD", adjusted_conversion = 1)
  )

mean_values_by_asset_for_loop =
  wrangle_asset_data(
    asset_data_daily_raw = asset_data_daily_raw_bid,
    summarise_means = TRUE
  )

#----------------------------------------------------------------
risk_dollar_value <- 20
trade_params <-
  c(-1.5 ,-1,-0.5,0, 0.25, 0.5) %>%
  map_dfr(
    ~ tibble(
      profit_plus = c(0.25, 0.5, 0.75, 1, 1.25, 1.5, 2, 2.25, 2.5)
    ) %>%
      mutate(stop_minus = .x)
  ) %>%
  filter(
    abs(stop_minus) <profit_plus
  )

trade_params2 <-
  c(0, 0.25, 0.5, 0.75, 1, 1.25, 1.5) %>%
  map_dfr(
    ~ trade_params %>%
      mutate(sd_facs = .x)
  )

trade_params3 <-
  c(200, 300) %>%
  map_dfr(
    ~ trade_params2 %>%
      mutate(rolling_period = .x)
  )

target_assets <-
  c("USD_JPY",
    "USD_CHF","XCU_USD", "SUGAR_USD",
    "GBP_USD", "WTICO_USD", "EUR_JPY", "USD_NOK",
    "WHEAT_USD",
    "EUR_USD",  "AU200_AUD", "XAG_USD",
    "EUR_GBP", "USD_CNH", "USD_CAD", "NAS100_USD",
    "EU50_EUR", "NATGAS_USD","SOYBN_USD",
    "US2000_USD",
    "BCO_USD", "AUD_USD", "NZD_USD")

raw_vol_results_db <- connect_db("C:/Users/Nikhil Chandra/Documents/trade_data/volatility_model.db")
results_list <- list()

for (j in 268:dim(trade_params3)[1]) {


  sd_facs <- trade_params3$sd_facs[j] %>% as.numeric()
  stop_minus <- trade_params3$stop_minus[j] %>% as.numeric()
  profit_plus <- trade_params3$profit_plus[j] %>% as.numeric()
  rolling_period <- trade_params3$rolling_period[j] %>% as.numeric()

  tagged_trades <- get_tag_volatility_trades(
    asset_data_daily_raw = asset_data_daily_raw_ask,
    mean_values_by_asset_for_loop = mean_values_by_asset_for_loop,
    sd_facs = sd_facs,
    training_prop = 0.8,
    stop_minus = stop_minus,
    profit_plus = profit_plus,
    rolling_period = rolling_period,
    asset_infor = asset_infor
  )

  results_temp <- trade_finder_volatility(
    tagged_trades = tagged_trades,
    asset_data_daily_raw_ask = asset_data_daily_raw_ask,
    asset_data_daily_raw_bid = asset_data_daily_raw_bid,
    mean_values_by_asset = mean_values_by_asset
  )

  results_temp2 <- results_temp %>%
    mutate(
      trade_returns = trade_returns_short + trade_returns_long
    ) %>%
    mutate(
      sd_facs = trade_params3$sd_facs[j] %>% as.numeric(),
      stop_minus = trade_params3$stop_minus[j] %>% as.numeric(),
      profit_plus = trade_params3$profit_plus[j] %>% as.numeric(),
      rolling_period = trade_params3$rolling_period[j] %>% as.numeric(),
      loop_index = j
    )

  if(j ==1 ) {
    write_table_sql_lite(conn = raw_vol_results_db,
                         .data = results_temp2,
                         table_name = "volatility_model", overwrite_true = TRUE)
    raw_vol_results_db <- connect_db("C:/Users/Nikhil Chandra/Documents/trade_data/volatility_model.db")
  } else {
    append_table_sql_lite(
      conn = raw_vol_results_db,
      .data = results_temp2,
      table_name = "volatility_model"
    )
  }

  results_temp2_aud_long <-
    results_temp2 %>%
    convert_stop_profit_AUD(
      asset_infor = asset_infor,
      asset_col = "Asset",
      stop_col = "stop_value_long",
      profit_col = "profit_value_long",
      price_col = "trade_start_price_long",
      risk_dollar_value = risk_dollar_value,
      returns_present = TRUE,
      trade_return_col = "trade_returns_long",
      currency_conversion = currency_conversion
    ) %>%
    dplyr::select(Date, Asset,
                  Long_Volume_req = volume_required,
                  Long_AUD_return = trade_return_dollars_AUD)

  results_temp2_aud_short <-
    results_temp2 %>%
    convert_stop_profit_AUD(
      asset_infor = asset_infor,
      asset_col = "Asset",
      stop_col = "stop_value_short",
      profit_col = "profit_value_short",
      price_col = "trade_start_price_short",
      risk_dollar_value = risk_dollar_value,
      returns_present = TRUE,
      trade_return_col = "trade_returns_short",
      currency_conversion = currency_conversion
    ) %>%
    dplyr::select(Date, Asset,
                  short_Volume_req = volume_required,
                  short_AUD_return = trade_return_dollars_AUD)

  full_AUD_Results <- results_temp2_aud_short%>%
    left_join(results_temp2_aud_long) %>%
    filter(short_Volume_req >0 & Long_Volume_req > 0) %>%
    mutate(
      sd_facs = trade_params3$sd_facs[j] %>% as.numeric(),
      stop_minus = trade_params3$stop_minus[j] %>% as.numeric(),
      profit_plus = trade_params3$profit_plus[j] %>% as.numeric(),
      rolling_period = trade_params3$rolling_period[j] %>% as.numeric()
    ) %>%
    left_join(
      results_temp2 %>% dplyr::select(Date, Asset, trade_time_taken_long, trade_time_taken_short)
    )

  results_list[[j]] <- full_AUD_Results

}


results_vol_ts_dfr <- results_list %>%
  map_dfr(bind_rows) %>%
  mutate(
    Actual_Return_AUD_per_Trade = Long_AUD_return + short_AUD_return
  ) %>%
  # group_by(Date, sd_facs, stop_minus, profit_plus, rolling_period)  %>%
  group_by(sd_facs, stop_minus, profit_plus, rolling_period) %>%
  summarise(
            average_loss = round(mean(ifelse(short_AUD_return < 0, short_AUD_return, NA), na.rm = T)),
            average_win = round(mean(ifelse(short_AUD_return > 0, short_AUD_return, NA), na.rm = T)),
            short_AUD_return = sum(round(short_AUD_return), na.rm = T),
            Long_AUD_return = sum(round(Long_AUD_return), na.rm = T),
            Actual_Return_AUD_per_Trade = sum(round(Actual_Return_AUD_per_Trade), na.rm = T),
            total_trades = n()
            ) %>%
  mutate(
    reg_ratio = abs(average_win/average_loss)
  )

results_vol_ts_dfr %>%
  ggplot(aes(x = reg_ratio, y = Actual_Return_AUD_per_Trade, color = total_trades)) +
  geom_point() +
  theme_minimal()

lm_model <- lm(Actual_Return_AUD_per_Trade ~ sd_facs + reg_ratio +
                 total_trades + sd_facs*total_trades + reg_ratio*total_trades, data = results_vol_ts_dfr)
summary(lm_model)

lm_model <- lm(Actual_Return_AUD_per_Trade ~ sd_facs + reg_ratio , data = results_vol_ts_dfr)
summary(lm_model)
#--------------------------------------Todays Trades
risk_dollar_value <- 10

tagged_trades <- get_tag_volatility_trades(
  asset_data_daily_raw = asset_data_daily_raw,
  mean_values_by_asset_for_loop = mean_values_by_asset_for_loop,
  sd_facs = 0.25,
  training_prop = 0.8,
  stop_minus = -1.5,
  profit_plus = 2.25,
  rolling_period = 200
)

tagged_trades2 <- get_tag_volatility_trades(
  asset_data_daily_raw = asset_data_daily_raw,
  mean_values_by_asset_for_loop = mean_values_by_asset_for_loop,
  sd_facs = 0.00,
  training_prop = 0.8,
  stop_minus = -1,
  profit_plus = 1.25,
  rolling_period = 300
)

tagged_trades_today <- tagged_trades %>%
  mutate(sd_var = 0.25) %>%
  bind_rows(tagged_trades2 %>% mutate(sd_var = 0)) %>%
  slice_max(Date) %>%
  filter(!is.na(trade_col)) %>%
    filter(Asset %in% target_assets) %>%
  group_by(Asset) %>%
  slice_max(sd_var) %>%
  ungroup() %>%
    mutate(ending_value = str_extract(Asset, "_[A-Z][A-Z][A-Z]") %>% str_remove_all("_")) %>%
    left_join(currency_conversion, by =c("ending_value" = "not_aud_asset")) %>%
    mutate(
      minimumTradeSize = abs(log10(as.numeric(minimumTradeSize))),
      marginRate = as.numeric(marginRate),
      pipLocation = as.numeric(pipLocation),
      displayPrecision = as.numeric(displayPrecision)
    ) %>%
    ungroup() %>%
    mutate(
      stop_value = round(stop_value_adj, abs(pipLocation) ),
      profit_value = round(profit_value_adj, abs(pipLocation) )
    ) %>%
    mutate(trade_start_price = Price) %>%
    mutate(
      asset_size = floor(log10(trade_start_price)),
      volume_adjustment =
        case_when(
          str_detect(Asset, "ZAR") & type == "CURRENCY" ~ 10,
          str_detect(Asset, "JPY") & type == "CURRENCY" ~ 100,
          str_detect(Asset, "NOK") & type == "CURRENCY" ~ 10,
          str_detect(Asset, "SEK") & type == "CURRENCY" ~ 10,
          str_detect(Asset, "CZK") & type == "CURRENCY" ~ 10,
          TRUE ~ 1
        )
    ) %>%
    mutate(
      AUD_Price =
        case_when(
          !is.na(adjusted_conversion) ~ (trade_start_price*adjusted_conversion)/volume_adjustment,
          TRUE ~ trade_start_price/volume_adjustment
        ),
      stop_value_AUD =
        case_when(
          !is.na(adjusted_conversion) ~ (stop_value*adjusted_conversion)/volume_adjustment,
          TRUE ~ stop_value/volume_adjustment
        ),
      profit_value_AUD =
        case_when(
          !is.na(adjusted_conversion) ~ (profit_value*adjusted_conversion)/volume_adjustment,
          TRUE ~ profit_value/volume_adjustment
        ),

      volume_unadj =  risk_dollar_value/stop_value_AUD,
      volume_required = volume_unadj,
      volume_adj = round(volume_unadj, minimumTradeSize),
      minimal_loss =  volume_adj*stop_value_AUD,
      trade_value = AUD_Price*volume_adj*marginRate,
      estimated_margin = trade_value,
      volume_required = volume_adj
    )%>%
    dplyr::select(-c(displayPrecision,
                     tradeUnitsPrecision,
                     maximumOrderUnits,
                     maximumPositionSize,
                     maximumTrailingStopDistance,
                     longRate,
                     shortRate,
                     minimumTrailingStopDistance,
                     minimumGuaranteedStopLossDistance,
                     guaranteedStopLossOrderMode,
                     guaranteedStopLossOrderExecutionPremium)
    )


#---------------------------------------------
#We use Account  number 3, 001-011-1615559-001
account_list <- get_list_of_accounts()
long_account_var <- 1
short_account_var <- 3
account_name_long <- "primary"
account_name_short <- "corr_no_macro"
account_number_long <- "001-011-1615559-001"
account_number_short <- "001-011-1615559-004"

current_trades_long <- get_list_of_positions(account_var = long_account_var)
current_trades_long <- current_trades_long %>%
  mutate(direction = stringr::str_to_title(direction)) %>%
  rename(Asset = instrument )

current_trades_short <- get_list_of_positions(account_var = short_account_var)
current_trades_short <- current_trades_short %>%
  mutate(direction = stringr::str_to_title(direction)) %>%
  rename(Asset = instrument )

trade_list_for_today_long <-
  tagged_trades_today %>%
  filter(volume_required > 0) %>%
  filter(!(Asset %in% current_trades_long$Asset)) %>%
  filter(Asset != "SG30_SGD") %>%
  left_join(current_trades_long %>%
              mutate(units  =
                       case_when(
                         direction == "Short" ~ -1*units,
                         direction == "Long" ~ units
                       ))
  ) %>%
  mutate(trade_col = "Long") %>%
  filter(
    trade_col == direction | is.na(direction)
  ) %>%
  mutate(
    volume_required =
      case_when(
        trade_col == "Long" ~ volume_required,
        trade_col == "Short" ~ -1*volume_required,
      )
  ) %>%
  arrange(estimated_margin)
  # filter(!(Asset %in% c("USD_CHF", "XCU_USD")))

trade_list_for_today_short <-
  trade_list_for_today_long %>%
  mutate(
    volume_required = -1*volume_required
  )

for (i in 1:dim(trade_list_for_today_long)[1]) {

  account_details <- get_account_summary(account_var = long_account_var) %>% slice_head(n = 1)
  margain_available <- account_details$marginAvailable %>% as.numeric()
  margain_used <- account_details$marginUsed%>% as.numeric()
  total_margain <- margain_available + margain_used
  percentage_margain_available <- margain_available/total_margain
  Sys.sleep(2)
  account_details_short <- get_account_summary(account_var = short_account_var) %>% slice_head(n = 1)
  margain_available_short <- account_details_short$marginAvailable %>% as.numeric()
  margain_used_short <- account_details$marginUsed%>% as.numeric()
  total_margain_short <- margain_available_short + margain_used_short
  percentage_margain_available_short <- margain_available_short/total_margain_short
  Sys.sleep(2)

  if(percentage_margain_available > 0.15 & percentage_margain_available_short> 0.15) {

    asset <- trade_list_for_today_long$Asset[i] %>% as.character()
    volume_trade <- trade_list_for_today_long$volume_required[i] %>% as.numeric()
    loss_var <- trade_list_for_today_long$stop_value[i] %>% as.numeric()
    profit_var <- trade_list_for_today_long$profit_value[i] %>% as.numeric()
    volume_trade_short <- volume_trade*-1

    if(loss_var > 9) { loss_var <- round(loss_var)}
    if(profit_var > 9) { profit_var <- round(profit_var)}

    profit_var > loss_var
    trade_list_for_today_long$estimated_margin[i]

    # if(asset == "NATGAS_USD") {
    #   loss_var <- round(loss_var, 3)
    #   profit_var <- round(profit_var, 3)
    # }

    # This is misleading because it is price distance and not pip distance
    if(profit_var > loss_var) {
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

      Sys.sleep(1)

      http_return <- oanda_place_order_pip_stop(
        asset = asset,
        volume = volume_trade_short,
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

  if(percentage_margain_available < 0.15 & percentage_margain_available_short < 0.15) {
    break
  }

}

