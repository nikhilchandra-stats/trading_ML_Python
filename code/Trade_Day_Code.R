helpeR::load_custom_functions()
one_drive_path <- helperfunctions35South::create_one_drive_path(
  path_extension = "raw data")
#--------------------------------------Trade Day Volatility
asset_infor <- get_instrument_info()
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
                    # "UK100_GBP",
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
                    # "CH20_CHF", "ESPIX_EUR", "XPT_USD",
                    "EUR_AUD", "SOYBN_USD", "US2000_USD",
                    "BCO_USD", "AUD_USD", "NZD_USD", "NZD_CHF", "WHEAT_USD", "AUD_JPY", "AUD_SEK")
  )

asset_infor <- get_instrument_info()

extracted_asset_data <-
  read_all_asset_data(
    asset_list_oanda = asset_list_oanda,
    save_path_oanda_assets = "C:/Users/Nikhil Chandra/Documents/Asset Data/oanda_data/",
    read_csv_or_API = "API"
  )

asset_data_combined <- extracted_asset_data %>% map_dfr(bind_rows)
asset_data_daily_raw <- extracted_asset_data %>% map_dfr(bind_rows)
asset_data_daily <- asset_data_daily_raw
aud_usd_today <- asset_data_daily_raw %>% filter(str_detect(Asset, "AUD")) %>%
  slice_max(Date)  %>%
  dplyr::select(Price, Asset) %>%
  mutate(ending_value = str_extract(Asset, "_[A-Z][A-Z][A-Z]") %>% str_remove_all("_")) %>%
  mutate(
    adjusted_conversion =
      case_when(
        ending_value != "AUD" ~ 1/Price,
        TRUE ~ Price
      )
  )

asset_data_daily_raw_week <- extracted_asset_data  %>%
  map_dfr(~ .x) %>%
  mutate(
    week_date = Date,
    week_start_price = Price,
    weekly_high = High,
    weekly_low = Low
  ) %>%
  dplyr::select(-Date, Price, -Low, -High, -Open)

mean_values_by_asset_for_loop =
  wrangle_asset_data(
    asset_data_daily_raw = asset_data_daily_raw,
    summarise_means = TRUE
  )

currency_conversion <-
  aud_usd_today %>%
  mutate(
    not_aud_asset = str_remove_all(Asset, "AUD") %>% str_remove_all("_")
  ) %>%
  dplyr::select(not_aud_asset, adjusted_conversion) %>%
  bind_rows(
    tibble(not_aud_asset = "AUD", adjusted_conversion = 1)
  ) %>%
  mutate(
    adjusted_conversion =
      case_when(
        not_aud_asset == "JPY" ~ adjusted_conversion*100,
        TRUE ~ adjusted_conversion
      )
  )

trade_params <- read_csv("data/volatility_trade_params.csv")

trades_today <-
  get_volatility_trades(
  asset_data_daily_raw_week = asset_data_daily_raw_week,
  asset_data_daily_raw = asset_data_daily_raw,
  mean_values_by_asset_for_loop = mean_values_by_asset_for_loop,
  sd_facs = 0,
  stop_fac = 0.56,
  prof_fac =1.4,
  risk_dollar_value = 10
)

risk_dollar_value = 10
trades_today_to_take <- trades_today %>%
  ungroup() %>%
  dplyr::select(-volume_1_stop_dollar,
                -volume_1_profit_dollar,
                -stop_volume_equated_dollar,
                -profit_volume_equated_dollar) %>%
  mutate(ending_value = str_extract(Asset, "_[A-Z][A-Z][A-Z]") %>% str_remove_all("_")) %>%
  left_join(currency_conversion, by =c("ending_value" = "not_aud_asset")) %>%
  dplyr::select(-pipLocation) %>%
  left_join(asset_infor, by = c("Asset" = "name") ) %>%
  mutate(
    minimumTradeSize = abs(log10(as.numeric(minimumTradeSize))),
    marginRate = as.numeric(marginRate),
    pipLocation = as.numeric(pipLocation),
    displayPrecision = as.numeric(displayPrecision)
  ) %>%
  ungroup() %>%
  mutate(
    stop_value = round(stop_value, abs(pipLocation) ),
    profit_value = round(profit_value, abs(pipLocation) )
  ) %>%
  mutate(
    asset_size = floor(log10(Price)),
    volume_adjustment =
      case_when(
        str_detect(ending_value, "ZAR") & type == "CURRENCY" ~ 10,
        str_detect(ending_value, "JPY") & type == "CURRENCY" ~ 100,
        str_detect(ending_value, "NOK") & type == "CURRENCY" ~ 10,
        str_detect(ending_value, "SEK") & type == "CURRENCY" ~ 10,
        str_detect(ending_value, "CZK") & type == "CURRENCY" ~ 10,
        TRUE ~ 1
      )
  ) %>%
  mutate(
    AUD_Price =
      case_when(
        !is.na(adjusted_conversion) ~ (Price*adjusted_conversion)/volume_adjustment,
        TRUE ~ Price/volume_adjustment
        ),
    stop_value_AUD =
      case_when(
        !is.na(adjusted_conversion) ~ (stop_value*adjusted_conversion)/volume_adjustment,
        TRUE ~ stop_value/volume_adjustment
      ),

    volume_unadj =  risk_dollar_value/stop_value_AUD,
    volume_required = volume_unadj,
    volume_adj = round(volume_unadj, minimumTradeSize),
    minimal_loss =  volume_adj*stop_value_AUD,
    trade_value = AUD_Price*volume_adj*marginRate,
    estimated_margin = trade_value,
    volume_required = volume_adj
  ) %>%
  arrange(desc(estimated_margin)) %>%
  filter(estimated_margin <= 120) %>%
  filter(stop_value < profit_value)
  # mutate(
  #   reverse_volume_calc = risk_dollar_value/stop_value,
  #   reverse_volume_calc =
  #     case_when(
  #       !is.na(adjusted_conversion) ~ reverse_volume_calc/adjusted_conversion,
  #       TRUE ~ reverse_volume_calc
  #     ),
  #   reverse_volume_calc_pip_weighted = reverse_volume_calc*(volume_adjustment),
  #   reverse_volume_calc_pip_weighted = round(reverse_volume_calc_pip_weighted, minimumTradeSize),
  #   volume_required = reverse_volume_calc_pip_weighted,
  #   marginRate = as.numeric(marginRate),
  #   trade_value = Price*volume_required*adjusted_conversion*marginRate,
  #   estimated_margin =
  #     case_when(
  #       Asset %in% c("USB10Y_USD", "EU50_EUR", "NAS100_USD",
  #                    "NATGAS_USD", "AU200_AUD", "SG30_SGD",
  #                    "XAU_USD", "XAU_CAD", "WHEAT_USD", "DE10YB_EUR", "DE30_EUR",
  #                    "US2000_USD", "ESPIX_EUR", "SOYBN_USD", "BCO_USD",
  #                    "UK100_GBP", "UK10YB_GBP", "XAU_GBP", "USB02Y_USD",
  #                    "HK33_HKD", "WTICO_USD", "SUGAR_USD", "XCU_USD",
  #                    "CH20_CHF", "XAU_EUR", "BTC_USD") ~ trade_value,
  #       TRUE ~ trade_value
  #       )
  # )

#------------Long Positions

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
  trades_today_to_take %>%
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

trade_list_for_today_short <-
  trade_list_for_today_long %>%
  mutate(
    volume_required = -1*volume_required
  )

for (i in 1:dim(trade_list_for_today_long)[1]) {

  account_details <- get_account_summary(account_var = long_account_var)
  margain_available <- account_details$marginAvailable %>% as.numeric()
  margain_used <- account_details$marginUsed%>% as.numeric()
  total_margain <- margain_available + margain_used
  percentage_margain_available <- margain_available/total_margain
  Sys.sleep(2)
  account_details_short <- get_account_summary(account_var = short_account_var)
  margain_available_short <- account_details$marginAvailable %>% as.numeric()
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

    # if(asset == "NATGAS_USD") {
    #   loss_var <- round(loss_var, 3)
    #   profit_var <- round(profit_var, 3)
    # }

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

  if(percentage_margain_available < 0.15 & percentage_margain_available_short < 0.15) {
    break
  }

}
