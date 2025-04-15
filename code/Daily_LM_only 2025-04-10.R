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


asset_list_oanda <- get_oanda_symbols() %>%
  keep( ~ .x %in% c("HK33_HKD", "USD_JPY", "BTC_USD", "AUD_NZD", "GBP_CHF",
                    "EUR_HUF", "EUR_ZAR", "NZD_JPY", "EUR_NZD", "USB02Y_USD",
                    "XAU_CAD", "GBP_JPY", "EUR_NOK", "USD_SGD", "EUR_SEK", "DE30_EUR",
                    "AUD_CAD", "UK10YB_GBP", "XPD_USD", "UK100_GBP", "USD_CHF", "GBP_NZD",
                    "GBP_SGD", "USD_SEK", "EUR_SGD", "XCU_USD", "SUGAR_USD", "CHF_ZAR",
                    "AUD_CHF", "EUR_CHF", "USD_MXN", "GBP_USD", "WTICO_USD", "EUR_JPY", "USD_NOK",
                    "XAU_USD", "DE10YB_EUR", "USD_CZK", "AUD_SGD", "USD_HUF", "WHEAT_USD",
                    "EUR_USD", "SG30_SGD", "GBP_AUD", "NZD_CAD", "AU200_AUD", "XAG_USD",
                    "XAU_EUR", "EUR_GBP", "USD_CNH", "USD_CAD", "NAS100_USD", "USB10Y_USD",
                    "EU50_EUR", "NATGAS_USD", "CAD_JPY", "FR40_EUR", "USD_ZAR", "XAU_GBP",
                    "CH20_CHF", "ESPIX_EUR", "XPT_USD", "EUR_AUD", "SOYBN_USD", "US2000_USD",
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

reg_data_list <- run_reg_daily_variant(
  raw_macro_data = raw_macro_data,
  eur_data = eur_data,
  AUD_exports_total = AUD_exports_total,
  USD_exports_total = USD_exports_total,
  asset_data_daily_raw = asset_data_daily_raw,
  train_percent = 0.57
)

regression_prediction <- reg_data_list[[2]]

raw_LM_trade_df <- reg_data_list[[2]]

LM_preped <- prep_LM_daily_trade_data(
  asset_data_daily_raw = asset_data_daily_raw,
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
    asset_data_daily_raw = asset_data_daily_raw,
    summarise_means = TRUE
  )

trade_params <-
  tibble(
    sd_factor_low = c(0,1,2,3,4,5,6,7,8,9,10, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21)
  ) %>%
  mutate(
    sd_factor_high = sd_factor_low*2,
    sd_factor_high = ifelse(sd_factor_high == 0, 1, sd_factor_high)
  )
profit_factor  = 6
stop_factor  = 4

trade_with_daily_data <- LM_preped %>% pluck("LM Merged to Daily")

new_trades_this_week <- list()
retest_data <- list()

for (j in 1:dim(trade_params)[1]) {

  sd_factor_low <- trade_params$sd_factor_low[j] %>% as.numeric()
  sd_factor_high <- trade_params$sd_factor_high[j] %>% as.numeric()

  temp_for_trade <- trade_with_daily_data %>%
    mutate(
      trade_col =
        case_when(

          between(Pred_trade,mean_value  + sd_value*sd_factor_low,  mean_value  + sd_value*sd_factor_high) ~ "Long",
          between(Pred_trade,mean_value  - sd_value*sd_factor_high,  mean_value  - sd_value*sd_factor_low) ~ "Short"

        )
    )

  retest_long <-
    generic_trade_finder_conservative(
      tagged_trades = temp_for_trade,
      asset_data_daily_raw = asset_data_daily_raw,
      stop_factor = stop_factor ,
      profit_factor = profit_factor,
      trade_col = "trade_col",
      date_col = "Date",
      max_hold_period = 100,
      start_price_col = "Price",
      mean_values_by_asset = mean_values_by_asset_for_loop,
      return_summary = TRUE
    ) %>%
    map_dfr(
      ~ .x
    )

  retest_long_sum <- retest_long %>%
    group_by(trade_category, trade_direction) %>%
    summarise(
      Trades = sum(Trades, na.rm = T)
    ) %>%
    pivot_wider(names_from = trade_category, values_from = Trades) %>%
    mutate(
      Perc = `TRUE WIN`/ (`TRUE LOSS` + `TRUE WIN`)
    ) %>%
    mutate(
      sd_factor_low = sd_factor_low,
      sd_factor_high = sd_factor_high
    )

  retest_data[[j]] <- retest_long_sum

  trade_with_daily_data <- LM_preped %>% pluck("LM Merged to Daily")

  chance_of_win <- retest_long_sum %>%
    distinct(trade_direction, Perc)

  if( dim(chance_of_win)[1] == 0 ) { chance_of_win = 0 }

  new_trades_data_long <- trade_with_daily_data %>%
    mutate(
      trade_col =
        case_when(
          between(Pred_trade,mean_value  + sd_value*sd_factor_low,  mean_value  + sd_value*sd_factor_high) ~ "Long",
          between(Pred_trade,mean_value  - sd_value*sd_factor_high,  mean_value  - sd_value*sd_factor_low) ~ "Short"
        )
    ) %>%
    dplyr::slice_max(Date, n = 1) %>%
    mutate(
      sd_factor_low = sd_factor_low,
      sd_factor_high = sd_factor_high
    ) %>%
    left_join(
      chance_of_win, by = c("trade_col" = "trade_direction")
    )

  new_trades_this_week[[j]] <- new_trades_data_long

}

retest_data <- retest_data %>% map_dfr(bind_rows)
new_trades_this_week <- new_trades_this_week %>% map_dfr(bind_rows)

retest_data_filt <- retest_data %>%
  mutate(Total = `TRUE LOSS` + `TRUE WIN`) %>%
  filter(Total > 100) %>%
  group_by(trade_direction) %>%
  slice_max(Perc, n = 10)  %>%
  mutate(
    risk_weighted_return =
      Perc*(profit_factor/stop_factor) - (1- Perc)*(1)
  )

risk_dollar_value <- 25

new_trades_this_week_filt <-
  new_trades_this_week %>%
  filter(!is.na(trade_col)) %>%
  group_by(Asset, trade_col) %>%
  slice_max(Perc) %>%
  left_join(mean_values_by_asset_for_loop)  %>%
  left_join(asset_infor %>% rename(Asset = name))%>%
  mutate(
    profit_point = Price + mean_daily + sd_daily*profit_factor,
    stop_point = Price - mean_daily + sd_daily*stop_factor,

    profit_points =  mean_daily + sd_daily*profit_factor,
    stop_points = mean_daily + sd_daily*stop_factor,

    profit_points_pip =  profit_points/(10^pipLocation),
    stop_points_pip = stop_points/(10^pipLocation)
  ) %>%
  filter(!is.na(trade_col))  %>%
  mutate(
    risk_weighted_return =
      Perc*(profit_factor/stop_factor) - (1- Perc)*(1)
  ) %>%
  # filter(risk_weighted_return > 0.2) %>%
  filter(risk_weighted_return > 0.1) %>%
  mutate(

    mean_pip = mean_daily/(10^pipLocation),
    sd_daily_pip = sd_daily/(10^pipLocation),
    volume_1_stop_dollar = stop_points_pip*(10^pipLocation),
    volume_1_profit_dollar = profit_points_pip*(10^pipLocation),

    # volume_required = round(risk_dollar_value/volume_1_stop_dollar),
    volume_required = round( (risk_dollar_value/stop_points_pip)/(10^pipLocation) )

  )

#This is where we check the volume required against different volumes
#This is simple, the stop value in actual (ie; the raw change in asset price)
# then multiply that aganst different volumes.
currency_conversion <-
  aud_usd_today %>%
  mutate(
    not_aud_asset = str_remove_all(Asset, "AUD") %>% str_remove_all("_")
  ) %>%
  dplyr::select(not_aud_asset, adjusted_conversion) %>%
  bind_rows(
    tibble(not_aud_asset = "AUD", adjusted_conversion = 1)
  )

analyse_volume <- new_trades_this_week_filt %>%
  distinct(Asset, Price, stop_points, stop_points_pip, profit_points, pipLocation, minimumTradeSize) %>%
  mutate(
    minimumTradeSize = abs(log10(as.numeric(minimumTradeSize)))
  ) %>%
  mutate(
    asset_size = floor(log10(Price))
  ) %>%
  mutate(ending_value = str_extract(Asset, "_[A-Z][A-Z][A-Z]") %>% str_remove_all("_")) %>%
  left_join(currency_conversion, by =c("ending_value" = "not_aud_asset")) %>%
  mutate(
    volume_adjustment =
      case_when(
        # asset_size == -1 ~ 1,
        # asset_size == 0 ~ 1,
        # asset_size == 1 ~ 0.1,
        # asset_size == 2 ~ 0.01,
        # asset_size == 3 ~ 1,
        # asset_size == 4 ~ 1,
        # TRUE ~ 1,

        str_detect(Asset, "ZAR") ~ 10,
        str_detect(Asset, "NOK") ~ 10,
        str_detect(Asset, "SEK") ~ 10,
        TRUE ~ 1
      ),
    #Unweighted
    loss_per_1 = 1*stop_points,
    loss_per_10 = 10*stop_points,
    loss_per_100 = 100*stop_points,
    loss_per_1000 = 1000*stop_points,
    risk_dollar_value_AUD =
      case_when(
        !is.na(adjusted_conversion) ~ risk_dollar_value*adjusted_conversion,
        TRUE ~ risk_dollar_value
      ),
    reverse_volume_calc = risk_dollar_value/stop_points,
    reverse_volume_calc =
      case_when(
        !is.na(adjusted_conversion) ~ reverse_volume_calc/adjusted_conversion,
        TRUE ~ reverse_volume_calc
      ),
    reverse_volume_calc_pip_weighted = reverse_volume_calc*(volume_adjustment),
    reverse_volume_calc_pip_weighted = round(reverse_volume_calc_pip_weighted, minimumTradeSize),
    volume_required = reverse_volume_calc_pip_weighted
  )



required_trades <-
  new_trades_this_week_filt %>%
  mutate(ending_value = str_extract(Asset, "_[A-Z][A-Z][A-Z]") %>% str_remove_all("_")) %>%
  mutate(
    minimumTradeSize = abs(log10(as.numeric(minimumTradeSize)))
  ) %>%
  left_join(currency_conversion, by =c("ending_value" = "not_aud_asset")) %>%
  mutate(
    asset_size = floor(log10(Price)),
    volume_adjustment =
      case_when(
        # asset_size == -1 ~ 1,
        # asset_size == 0 ~ 1,
        # asset_size == 1 ~ 0.1,
        # asset_size == 2 ~ 0.01,
        # asset_size == 3 ~ 1,
        # asset_size == 4 ~ 1,
        # TRUE ~ 1,

        str_detect(Asset, "ZAR") ~ 100,
        str_detect(Asset, "NOK") ~ 10,
        str_detect(Asset, "SEK") ~ 10,
        TRUE ~ 1
      )
  ) %>%
  mutate(
    risk_dollar_value_AUD =
      case_when(
        !is.na(adjusted_conversion) ~ risk_dollar_value*adjusted_conversion,
        TRUE ~ risk_dollar_value
      ),
    reverse_volume_calc = risk_dollar_value/stop_points,
    reverse_volume_calc =
      case_when(
        !is.na(adjusted_conversion) ~ reverse_volume_calc/adjusted_conversion,
        TRUE ~ reverse_volume_calc
      ),
    reverse_volume_calc_pip_weighted = reverse_volume_calc*(volume_adjustment),
    reverse_volume_calc_pip_weighted = round(reverse_volume_calc_pip_weighted, minimumTradeSize),
    volume_required = reverse_volume_calc_pip_weighted,
    estimated_margin = as.numeric(marginRate)*volume_required
  )

write.csv(retest_data_filt %>%
            mutate(stop_factor = stop_factor,
                   profit_factor = profit_factor),
          file = "data/Regression_Trade_Results.csv",
          row.names = FALSE)


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

trade_list_for_today <- required_trades %>%
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
  ) %>%
  mutate(
    stop_points =
      case_when(
        stop_points > 1 ~ round(stop_points, 2),
        stop_points < 1 & stop_points > 0 ~ round(stop_points, 4)
      ),
    profit_points =
      case_when(
        profit_points > 1 ~ round(profit_points, 2),
        profit_points < 1 & profit_points > 0 ~ round(profit_points, 4)
      )
  )


for (i in 1:dim(trade_list_for_today)[1]) {

  account_details <- get_account_summary(account_var = 2)
  margain_available <- account_details$marginAvailable %>% as.numeric()
  margain_used <- account_details$marginUsed%>% as.numeric()
  total_margain <- margain_available + margain_used
  percentage_margain_available <- margain_available/total_margain
  Sys.sleep(2)

  if(percentage_margain_available > 0.5) {

    asset <- trade_list_for_today$Asset[i] %>% as.character()
    volume_trade <- trade_list_for_today$volume_required[i] %>% as.numeric()
    # volume_trade <- 1000
    loss_var <- trade_list_for_today$stop_points[i] %>% as.numeric()
    profit_var <- trade_list_for_today$profit_points[i] %>% as.numeric()

    if(loss_var > 10) { loss_var <- round(loss_var)}
    if(profit_var > 10) { loss_var <- round(profit_var)}

    if(asset == "NATGAS_USD") {
      loss_var <- round(loss_var, 3)
      profit_var <- round(profit_var, 3)
    }

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

