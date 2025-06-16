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

# extracted_asset_data <-
#   read_all_asset_data(
#     asset_list_oanda = asset_list_oanda,
#     save_path_oanda_assets = "C:/Users/Nikhil Chandra/Documents/Asset Data/oanda_data/",
#     read_csv_or_API = "API"
#   )

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

mean_values_by_asset_for_loop =
  wrangle_asset_data(
    asset_data_daily_raw = asset_data_combined_ask,
    summarise_means = TRUE
  )

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

reg_data_list <- run_reg_daily_variant(
  raw_macro_data = raw_macro_data,
  eur_data = eur_data,
  AUD_exports_total = AUD_exports_total,
  USD_exports_total = USD_exports_total,
  asset_data_daily_raw = asset_data_daily_raw_ask,
  train_percent = 0.57
)

regression_prediction <- reg_data_list[[2]]

raw_LM_trade_df <- reg_data_list[[2]]

LM_preped <- prep_LM_daily_trade_data(
  asset_data_daily_raw = asset_data_daily_raw_ask,
  raw_LM_trade_df = reg_data_list[[2]],
  raw_LM_trade_df_training = reg_data_list[[3]]
)

trade_with_daily_data <- LM_preped %>% pluck("LM Merged to Daily")

#Reprosecute Trading Parameters
#-------------------------------Desired Params Long trades = 297, Win = 0.62
# sd_factor_high  = 12
# sd_factor_low  = 6

asset_infor <- get_instrument_info()

mean_values_by_asset_for_loop =
  wrangle_asset_data(
    asset_data_daily_raw = asset_data_daily_raw_ask,
    summarise_means = TRUE
  )

trade_params <-
  tibble(
    sd_factor_low = c(0,0.5, 1, 1.5, 2, 2.5, 3, 4,5,6,7,8,9,10, 12,
                      13, 14, 15)
  ) %>%
  mutate(
    sd_factor_high = sd_factor_low*2,
    sd_factor_high = ifelse(sd_factor_high == 0, 1, sd_factor_high)
  )  %>%
  bind_rows(
    tibble(
      sd_factor_low = c(0,0.5, 1, 1.5, 2, 2.5, 3, 4,5,6,7,8,9,10, 12,
                        13, 14, 15)
    ) %>%
      mutate(
        sd_factor_high = sd_factor_low*1.5,
        sd_factor_high = ifelse(sd_factor_high == 0, 1*2, sd_factor_high)
      )
  ) %>%
  bind_rows(
    tibble(
      sd_factor_low = c(0,0.5, 1, 1.5, 2, 2.5, 3, 4,5,6,7,8,9,10, 12,
                        13, 14, 15)
    ) %>%
      mutate(
        sd_factor_high = sd_factor_low*3,
        sd_factor_high = ifelse(sd_factor_high == 0, 1*3, sd_factor_high)
      )
  ) %>%
  bind_rows(
    tibble(
      sd_factor_low = c(0,0.5, 1, 1.5, 2, 2.5, 3, 4,5,6,7,8,9,10, 12,
                        13, 14, 15)
    ) %>%
      mutate(
        sd_factor_high = sd_factor_low*4,
        sd_factor_high = ifelse(sd_factor_high == 0, 1*4, sd_factor_high)
      )
  )

profit_factor  = 7
stop_factor  = 4
risk_dollar_value <- 10
trade_with_daily_data <- LM_preped %>% pluck("LM Merged to Daily")

new_trades_this_week <- list()
retest_data <- list()
retest_ts_data <- list()
store_tagged_trades <- list()

for (j in 1:dim(trade_params)[1]) {

  sd_factor_low <- trade_params$sd_factor_low[j] %>% as.numeric()
  sd_factor_high <- trade_params$sd_factor_high[j] %>% as.numeric()

  temp_for_trade_pos <- trade_with_daily_data %>%
    mutate(
      trade_col =
        case_when(

          between(Pred_trade,mean_value  + sd_value*sd_factor_low,  mean_value  + sd_value*sd_factor_high) ~ "Long"
          # between(Pred_trade,mean_value  - sd_value*sd_factor_high,  mean_value  - sd_value*sd_factor_low) ~ "Long"

        )
    )

  temp_for_trade_neg <- trade_with_daily_data %>%
    mutate(
      trade_col =
        case_when(

          # between(Pred_trade,mean_value  + sd_value*sd_factor_low,  mean_value  + sd_value*sd_factor_high) ~ "Long"
          between(Pred_trade,mean_value  - sd_value*sd_factor_high,  mean_value  - sd_value*sd_factor_low) ~ "Long"

        )
    )

  temp_for_trade <- temp_for_trade_pos %>%
    mutate(pos_neg = "pos")
    bind_rows(temp_for_trade_neg %>%
                mutate(pos_neg = "neg"))

  store_tagged_trades[[j]] <- temp_for_trade

  retest_long_pos <-
    generic_trade_finder_loop(
      tagged_trades = temp_for_trade_pos,
      asset_data_daily_raw = asset_data_daily_raw_ask,
      stop_factor = stop_factor,
      profit_factor =profit_factor,
      trade_col = "trade_col",
      date_col = "Date",
      start_price_col = "Price",
      mean_values_by_asset = mean_values_by_asset_for_loop
    ) %>%
    mutate(pos_neg = "pos")

  retest_long_aud_pos <- retest_long_pos %>%
    rename(Asset = asset) %>%
    generic_anlyser(
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
      grouping_vars = c("trade_direction", "pos_neg")
    ) %>%
    mutate(
      sd_factor_low = sd_factor_low,
      sd_factor_high = sd_factor_high
    )

  retest_long_neg <-
    generic_trade_finder_loop(
      tagged_trades = temp_for_trade_neg,
      asset_data_daily_raw = asset_data_daily_raw_ask,
      stop_factor = stop_factor,
      profit_factor =profit_factor,
      trade_col = "trade_col",
      date_col = "Date",
      start_price_col = "Price",
      mean_values_by_asset = mean_values_by_asset_for_loop
    ) %>%
    mutate(pos_neg = "neg")

  retest_long_aud_neg <- retest_long_neg %>%
    rename(Asset = asset) %>%
    generic_anlyser(
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
      grouping_vars = c("trade_direction", "pos_neg")
    ) %>%
    mutate(
      sd_factor_low = sd_factor_low,
      sd_factor_high = sd_factor_high
    )

  retest_long_aud <- retest_long_aud_pos %>%
    bind_rows(retest_long_aud_neg)

  retest_data[[j]] <- retest_long_aud

  trade_with_daily_data <- LM_preped %>% pluck("LM Merged to Daily")

  chance_of_win <- retest_long_aud %>%
    distinct(trade_direction, Perc)

  if( dim(chance_of_win)[1] == 0 ) { chance_of_win = 0 }

  new_trades_data_long_pos <- trade_with_daily_data %>%
    mutate(
      trade_col =
        case_when(
          between(Pred_trade,mean_value  + sd_value*sd_factor_low,  mean_value  + sd_value*sd_factor_high)  ~ "Long"
          # between(Pred_trade,mean_value  - sd_value*sd_factor_high,  mean_value  - sd_value*sd_factor_low) ~ "Long"
        )
    ) %>%
    # mutate(Date = as_date(Date)) %>%
    # filter(Date >= today()  - days(5))
    dplyr::slice_max(Date) %>%
    mutate(
      sd_factor_low = sd_factor_low,
      sd_factor_high = sd_factor_high,
      pos_neg = "pos"
    ) %>%
    left_join(
      chance_of_win, by = c("trade_col" = "trade_direction")
    )

  new_trades_data_long_neg <- trade_with_daily_data %>%
    mutate(
      trade_col =
        case_when(
          # between(Pred_trade,mean_value  + sd_value*sd_factor_low,  mean_value  + sd_value*sd_factor_high)  ~ "Long"
          between(Pred_trade,mean_value  - sd_value*sd_factor_high,  mean_value  - sd_value*sd_factor_low) ~ "Long"
        )
    ) %>%
    # mutate(Date = as_date(Date)) %>%
    # filter(Date >= today()  - days(5))
    dplyr::slice_max(Date) %>%
    mutate(
      sd_factor_low = sd_factor_low,
      sd_factor_high = sd_factor_high ,
      pos_neg = "neg"
    ) %>%
    left_join(
      chance_of_win, by = c("trade_col" = "trade_direction")
    )

  new_trades_data_long <- new_trades_data_long_pos %>%
    bind_rows(new_trades_data_long_neg)

  new_trades_this_week[[j]] <- new_trades_data_long

}

reanalyse_results <-
  retest_data %>% map_dfr(bind_rows) %>%
  filter(risk_weighted_return > 0.07) %>%
  mutate(redont_risk_weighted_return =
           1000*( (Perc*maximum_win) - (minimal_loss*(1 - Perc)) )
         )

trades_for_today <-
  new_trades_this_week %>%
  map_dfr(
    ~.x %>% filter(!is.na(trade_col))
  ) %>%
  dplyr::select(-Perc) %>%
  left_join(reanalyse_results %>%
              dplyr::select(stop_factor,
                            profit_factor,
                            Final_Dollars,
                            sd_factor_high,
                            sd_factor_low,
                            trade_col = trade_direction,
                            risk_weighted_return,
                            pos_neg)
            ) %>%
  filter(!is.na(risk_weighted_return))  %>%
  get_stops_profs_volume_trades(mean_values_by_asset = mean_values_by_asset_for_loop,
                                trade_col = "trade_col",
                                currency_conversion = currency_conversion,
                                risk_dollar_value = risk_dollar_value,
                                stop_factor = stop_factor,
                                profit_factor = profit_factor) %>%
  filter( (abs(volume_required) >= 0.1 &
             Asset %in% c("SPX500_USD", "JP225_USD", "EU50_EUR", "US2000_USD", "SG30_SGD", "AU200_AUD",
                          "NAS100_USD", "DE30_EUR", "HK33_HKD")) |
            (abs(volume_required) >= 1 &
               !(Asset %in% c("SPX500_USD", "JP225_USD", "EU50_EUR", "US2000_USD", "SG30_SGD", "AU200_AUD",
                              "NAS100_USD", "DE30_EUR", "HK33_HKD"))
            )
  ) %>%
  filter(!is.na(trade_col)) %>%
  filter(trade_col == "Long") %>%
  group_by(Asset) %>%
  slice_max(risk_weighted_return) %>%
  dplyr::select(-pos_neg) %>%
  distinct()


#---------------------------------------------
#We use Account  number 2, 001-011-1615559-003
account_list <- get_list_of_accounts()
account_name <- "mt4_hedging"
account_number <- "001-011-1615559-003"
get_oanda_account_number(account_name = account_name)
current_trades <- get_list_of_positions(account_var = 2)
current_trades <- current_trades %>%
  mutate(direction = stringr::str_to_title(direction)) %>%
  rename(Asset = instrument )

trade_list_for_today <- trades_for_today %>%
  filter(volume_required > 0) %>%
  left_join(current_trades %>%
              mutate(units  =
                       case_when(
                         direction == "Short" ~ -1*units,
                         direction == "Long" ~ units
                       ))
  ) %>%
  filter(
    trade_col == direction | is.na(direction)
  ) %>%
  mutate(
    volume_required =
      case_when(
        trade_col == "Long" ~ volume_required,
        trade_col == "Short" ~ -1*volume_required,
      )
  )

trade_list_for_today <- trade_list_for_today %>%
  group_by(Asset) %>%
  slice_max(risk_weighted_return) %>%
  ungroup() %>%
  # filter(risk_weighted_return > 0.1) %>%
  arrange(estimated_margin)  %>%
  mutate(
    units = as.numeric(units),
    units = ifelse(is.na(units), 0, units)
  ) %>%
  arrange(units)

how_many_assets <- setdiff(asset_data_combined_ask$Asset %>% unique(), trade_list_for_today$Asset)


for (i in 1:dim(trade_list_for_today)[1]) {

  account_details <- get_account_summary(account_var = 2)
  margain_available <- account_details$marginAvailable %>% as.numeric()
  margain_used <- account_details$marginUsed%>% as.numeric()
  total_margain <- margain_available + margain_used
  percentage_margain_available <- margain_available/total_margain
  Sys.sleep(2)

  if(percentage_margain_available[1] > 0.01) {

    asset <- trade_list_for_today$Asset[i] %>% as.character()
    volume_trade <- trade_list_for_today$volume_required[i] %>% as.numeric()

    loss_var <- trade_list_for_today$stop_value[i] %>% as.numeric()
    profit_var <- trade_list_for_today$profit_value[i] %>% as.numeric()

    # if(loss_var > 9) { loss_var <- round(loss_var)}
    # if(profit_var > 9) { profit_var <- round(profit_var)}

    trade_list_for_today$estimated_margin[i]

    profit_var>=loss_var

    # This is misleading because it is price distance and not pip distance
    http_return <- oanda_place_order_pip_stop(
      asset = asset,
      volume = volume_trade,
      stopLoss = loss_var,
      takeProfit = profit_var,
      type = "MARKET",
      timeinForce = "FOK",
      acc_name = account_name,
      position_fill = "OPEN_ONLY" ,
      price
    )

  }

}

