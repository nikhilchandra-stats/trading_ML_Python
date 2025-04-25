helperfunctions35South::load_custom_functions()
one_drive_path <- helperfunctions35South::create_one_drive_path(
  path_extension = "raw data")

#--------------------------------------Trade Day Volatility
asset_infor <- get_instrument_info()
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

asset_data_daily_raw_week <- extracted_asset_data  %>%
  map_dfr(~ .x) %>%
  mutate(
    week_date = Date,
    week_start_price = Price,
    weekly_high = High,
    weekly_low = Low
  ) %>%
  dplyr::select(-Date, Price, -Low, -High, -Open)

vol_data <- asset_data_daily_raw_week %>%
  group_by(Asset) %>%
  arrange(week_date, .by_group = TRUE) %>%
  ungroup() %>%
  # mutate(week_start_price = exp(week_start_price)) %>%
  left_join(
    asset_infor %>% dplyr::select(Asset = name, pipLocation)
  ) %>%
  mutate(
    absolute_open_to_high = abs(weekly_high - week_start_price)/(10^pipLocation),
    absolute_open_to_low = abs(week_start_price - weekly_low)/(10^pipLocation)
  ) %>%
  group_by(Asset) %>%
  mutate(
    absolute_open_to_high_mean = mean(absolute_open_to_high, na.rm = T),
    absolute_open_to_low_mean = mean(absolute_open_to_low, na.rm = T),

    absolute_open_to_high_sd = sd(absolute_open_to_high, na.rm = T),
    absolute_open_to_low_sd = sd(absolute_open_to_low, na.rm = T)
  ) %>%
  ungroup()%>%
  group_by(Asset) %>%
  arrange(week_date, .by_group = TRUE) %>%
  group_by(Asset) %>%
  mutate(
    lag_open_to_high = lag(absolute_open_to_high),
    lag_open_to_low = lag(absolute_open_to_low),

    lag_open_to_high_2 = lag(absolute_open_to_high, 3),
    lag_open_to_low_2 = lag(absolute_open_to_low, 3)
  ) %>%
  mutate(
    EUR_check = ifelse(str_detect(Asset, "EUR"), 1, 0),
    AUD_check = ifelse(str_detect(Asset, "AUD"), 1, 0),
    USD_check = ifelse(str_detect(Asset, "USD"), 1, 0),
    GBP_check = ifelse(str_detect(Asset, "GBP"), 1, 0),
    JPY_check = ifelse(str_detect(Asset, "JPY"), 1, 0),
    CAD_check = ifelse(str_detect(Asset, "CAD"), 1, 0),
    MXN_check = ifelse(str_detect(Asset, "MXN"), 1, 0),
    ZAR_check = ifelse(str_detect(Asset, "ZAR"), 1, 0)
  )

training_data <- vol_data %>%
  group_by(Asset) %>%
  slice_head(prop = 0.5) %>%
  ungroup()

lm_model_high <-
  lm(data = training_data,
     formula = absolute_open_to_high ~
       lag_open_to_high + lag_open_to_low +
       EUR_check + AUD_check +
       USD_check + GBP_check +
       JPY_check + CAD_check +
       ZAR_check + MXN_check +
       lag_open_to_high_2 + lag_open_to_low_2)
summary(lm_model_high)

lm_model_low <-
  lm(data = training_data,
     formula = absolute_open_to_low_mean ~
       lag_open_to_high + lag_open_to_low +
       EUR_check + AUD_check +
       USD_check + GBP_check +
       JPY_check + CAD_check +
       ZAR_check + MXN_check +
       lag_open_to_high_2 + lag_open_to_low_2)
summary(lm_model_low)

testing_data <- vol_data %>%
  ungroup() %>%
  group_by(Asset) %>%
  slice_tail(prop = 0.5) %>%
  ungroup() %>%
  mutate(predicted_open_high_vol =
           predict.lm(lm_model_high,
                      vol_data %>%
                        ungroup() %>%
                        group_by(Asset) %>%
                        slice_tail(prop = 0.5) %>%
                        ungroup() ) %>% as.numeric(),

         predicted_open_low_vol =
           predict.lm(lm_model_low,
                      vol_data %>%
                        ungroup() %>%
                        group_by(Asset) %>%
                        slice_tail(prop = 0.5) %>%
                        ungroup()  ) %>% as.numeric()
  ) %>%
  mutate(
    error_high = predicted_open_high_vol - absolute_open_to_high,
    error_low = predicted_open_low_vol - absolute_open_to_low
  )

error_summary <-
  testing_data %>%
  group_by(Asset) %>%
  summarise(
    error_high_mean = mean(error_high, na.rm = T),
    quant_high_75 = quantile(error_high, 0.75, na.rm = T),

    error_low_mean = mean(error_low, na.rm = T),
    quant_low_75 = quantile(error_low, 0.75, na.rm = T)
  )

prof_stop_tibble <-
  tibble(
    prof_value = seq(0.2,5)
  ) %>%
  mutate(
    loss_value = prof_value*0.4
  ) %>%
  bind_rows(
    tibble(
      prof_value = seq(0.2,5)
    ) %>%
      mutate(
        loss_value = prof_value*0.6
      )
  )%>%
  bind_rows(
    tibble(
      prof_value = seq(4,5)
    ) %>%
      mutate(
        loss_value = prof_value*0.8
      )
  )

sd_value <- 0

trading_data_vol <- testing_data %>%
  mutate(
    trade_col =
      case_when(
        predicted_open_high_vol >= median(predicted_open_high_vol, na.rm = T) + sd_value*sd(predicted_open_high_vol, na.rm = T)|
          predicted_open_low_vol >= median(predicted_open_low_vol, na.rm = T) + sd_value*sd(predicted_open_low_vol, na.rm = T) ~ "TRADE"
      )
  )

daily_price_with_vol_trade <-
  asset_data_daily_raw %>%
  left_join(
    trading_data_vol %>%
      distinct(Asset, week_date, trade_col,pipLocation) %>% ungroup(),
    by = c("Date" = "week_date", "Asset")
  ) %>% filter(Asset %in% c("AUD_USD", "EUR_USD", "USD_JPY", "EUR_GBP", "GBP_USD", "NZD_USD", "USD_CHF",
                            "XAU_USD", "AU200_AUD", "USD_SEK", "XAG_USD", "USD_NOK", "WTICO_USD",
                            "EU50_EUR"))

mean_values_by_asset_for_loop =
  wrangle_asset_data(
    asset_data_daily_raw = asset_data_daily_raw,
    summarise_means = TRUE
  )

accumualtor <- list()

for (i in 1:dim(prof_stop_tibble)) {

  prof_value <- prof_stop_tibble$prof_value[i] %>% as.numeric()
  loss_value <- prof_stop_tibble$loss_value[i] %>% as.numeric()

  accumualtor[[i]] <-
    generic_trade_finder_volatility(
      tagged_trades = daily_price_with_vol_trade,
      asset_data_daily_raw = asset_data_daily_raw,
      stop_factor = loss_value,
      profit_factor = prof_value,
      trade_col = "trade_col",
      date_col = "Date",
      max_hold_period = 100,
      start_price_col = "Price",
      mean_values_by_asset = mean_values_by_asset_for_loop,
      return_summary = TRUE,
      additional_grouping_vars = c("Asset"),
      pip_info = asset_infor,
      prof_loss_dollar_equiv = 100
    ) %>%
    mutate(
      prof_value = prof_value,
      loss_value = loss_value
    )

}

vol_trade_summary <- accumualtor %>%
  map_dfr(bind_rows)

vol_trade_summary_total <- vol_trade_summary %>%
  mutate(
    per_symbol_profitable =
      case_when(
        Profit > 0 ~ 1,
        Profit < 0 ~ 0
      )
  ) %>%
  group_by(prof_value, loss_value) %>%
  summarise(Profit_dollar  = sum(Profit_dollar , na.rm = T),
            Profit  = sum(Profit , na.rm = T),
            avg_perc = mean(Perc, na.rm = T),
            avg_win = mean(average_win_pip, na.rm = T),
            avg_loss = mean(average_loss_pip, na.rm = T),
            per_symbol_profitable = sum(per_symbol_profitable, na.rm = T),
            total_symbols = n()
  ) %>%
  mutate(
    expected_win_avg = avg_perc*avg_win,
    expected_return_avg = avg_perc*avg_win - (1 - avg_perc)*avg_loss,
    per_symbol_profitable_perc =per_symbol_profitable/total_symbols,
    symbol_weighted_profit = per_symbol_profitable_perc*Profit
  )

vol_trade_summary_filt <- vol_trade_summary %>%
  ungroup() %>%
  #filter for minimum desired per symbol profitable
  # filter(per_symbol_profitable_perc > 0.95)
  filter(prof_value == 1.4 & loss_value < 0.57 & loss_value > 0.55) %>%
  left_join(asset_infor %>%
              dplyr::select(Asset = name, pipLocation)
  ) %>%
  mutate(
    Profit_Pip = Profit/(10^pipLocation)
  )

write.csv(x = vol_trade_summary_total,
          file = "data/volatility_trade_params.csv",
          row.names = FALSE)

#---------------------------------------------------------------------Trades Today
latest_trades_to_take <- daily_price_with_vol_trade %>%
  slice_max(Date)
filter(!is.na(trade_col)) %>%
  slice_max(Date)

#--------------------------------------------------------------------------------

generic_trade_finder_volatility <- function(
    tagged_trades = daily_price_with_vol_trade,
    asset_data_daily_raw = asset_data_daily_raw,
    stop_factor = 0.3,
    profit_factor = 0.8,
    trade_col = "trade_col",
    date_col = "Date",
    max_hold_period = 100,
    start_price_col = "Price",
    mean_values_by_asset = mean_values_by_asset_for_loop,
    return_summary = TRUE,
    additional_grouping_vars = c("Asset"),
    pip_info = asset_infor,
    prof_loss_dollar_equiv = 200
) {

  tagged_trades_short <- tagged_trades %>%
    mutate(
      trade_col =
        case_when(
          trade_col == "TRADE" ~ "Short"
        )
    )

  tagged_trades_long <- tagged_trades %>%
    mutate(
      trade_col =
        case_when(
          trade_col == "TRADE" ~ "Long"
        )
    )

  trade_results_short <-
    generic_trade_finder_conservative(
      tagged_trades = tagged_trades_short,
      asset_data_daily_raw = asset_data_daily_raw,
      stop_factor = stop_factor,
      profit_factor = profit_factor,
      trade_col = trade_col,
      date_col = date_col,
      max_hold_period = max_hold_period,
      start_price_col = start_price_col,
      mean_values_by_asset = mean_values_by_asset,
      return_summary = FALSE,
      additional_grouping_vars = additional_grouping_vars
    ) %>%
    pluck(2)

  trade_results_short_losses <- trade_results_short %>%
    dplyr::select(Date, Asset, stop_value, trade_category) %>%
    filter(trade_category == "TRUE LOSS") %>%
    pivot_wider(names_from = trade_category, values_from = stop_value) %>%
    rename(
      `Short Direction Loss` = `TRUE LOSS`
    )

  trade_results_short_win <- trade_results_short %>%
    dplyr::select(Date, Asset, profit_value, trade_category) %>%
    filter(trade_category == "TRUE WIN") %>%
    pivot_wider(names_from = trade_category, values_from = profit_value)%>%
    rename(
      `Short Direction Win` = `TRUE WIN`
    )

  trade_results_long <-
    generic_trade_finder_conservative(
      tagged_trades = tagged_trades_long,
      asset_data_daily_raw = asset_data_daily_raw,
      stop_factor = stop_factor,
      profit_factor = profit_factor,
      trade_col = trade_col,
      date_col = date_col,
      max_hold_period = max_hold_period,
      start_price_col = start_price_col,
      mean_values_by_asset = mean_values_by_asset,
      return_summary = FALSE,
      additional_grouping_vars = additional_grouping_vars
    ) %>%
    pluck(1)

  trade_results_long_losses <- trade_results_long %>%
    dplyr::select(Date, Asset, stop_value, trade_category) %>%
    filter(trade_category == "TRUE LOSS") %>%
    pivot_wider(names_from = trade_category, values_from = stop_value) %>%
    rename(
      `Long Direction Loss` = `TRUE LOSS`
    )

  trade_results_long_win <- trade_results_long %>%
    dplyr::select(Date, Asset, profit_value, trade_category) %>%
    filter(trade_category == "TRUE WIN") %>%
    pivot_wider(names_from = trade_category, values_from = profit_value)%>%
    rename(
      `Long Direction Win` = `TRUE WIN`
    )

  volume_required <- trade_results_short %>%
    distinct(Asset, profit_value, stop_value) %>%
    left_join(
      asset_infor %>% dplyr::select(Asset = name, pipLocation)
    ) %>%
    mutate(
      profit_value = profit_value/(10^pipLocation),
      stop_value = stop_value/(10^pipLocation)
    ) %>%
    mutate(
      required_volume =prof_loss_dollar_equiv/stop_value,
      required_volume = round(required_volume/(10^pipLocation))
    )  %>%
    dplyr::select(
      Asset, required_volume, pipLocation
    )

  variant_1 <- trade_results_short_losses %>%
    left_join(trade_results_long_win) %>%
    left_join(trade_results_long_losses) %>%
    mutate(
      across(where(is.numeric),
             .fns = ~ ifelse(is.na(.), 0, .))
    ) %>%
    mutate(
      Profit = `Long Direction Win` - (`Long Direction Loss` + `Short Direction Loss`),
      win_loss =
        case_when(
          Profit < 0 ~ 0,
          Profit > 0 ~ 1
        )
    ) %>%
    left_join(
      volume_required
    ) %>%
    mutate(
      pip_return = Profit/(10^pipLocation),
      Profit_dollar = required_volume*pip_return
    )

  variant_1_summary <-
    variant_1 %>%
    mutate(
      win_value = case_when(Profit > 0 ~ `Long Direction Win` - `Short Direction Loss`),
      loss_value = case_when(Profit < 0 ~ `Long Direction Loss` + `Short Direction Loss`)
    ) %>%
    group_by(Asset) %>%
    fill(c(win_value, loss_value), .direction = "updown") %>%
    group_by(Asset) %>%
    summarise(
      Profit = sum(Profit, na.rm = T),
      Profit = sum(Profit/(10^pipLocation), na.rm = T),
      Profit_dollar = sum(Profit_dollar, na.rm = T),
      win_loss  = sum(win_loss),
      total_trades = n(),
      average_win_pip = round(mean( (win_value)/(10^pipLocation), na.rm = T), digits = 4),
      average_loss_pip = round(mean( (loss_value)/(10^pipLocation), na.rm = T), digits = 4)
    )  %>%
    mutate(
      Perc =win_loss/total_trades
    ) %>%
    mutate(
      Variant = "Short Loss join Long Win and Long Loss"
    )


  variant_2 <- trade_results_long_losses %>%
    left_join(trade_results_short_win) %>%
    left_join(trade_results_short_losses) %>%
    mutate(
      across(where(is.numeric),
             .fns = ~ ifelse(is.na(.), 0, .))
    ) %>%
    mutate(
      Profit = `Short Direction Win` - (`Long Direction Loss` + `Short Direction Loss`),
      win_loss =
        case_when(
          Profit < 0 ~ 0,
          Profit > 0 ~ 1
        )
    ) %>%
    left_join(
      volume_required
    ) %>%
    mutate(
      pip_return = Profit/(10^pipLocation),
      Profit_dollar = required_volume*pip_return
    )

  variant_2_summary <-
    variant_2 %>%
    mutate(
      win_value = case_when(Profit > 0 ~ `Short Direction Win` - `Long Direction Loss`),
      loss_value = case_when(Profit < 0 ~ `Long Direction Loss` + `Short Direction Loss`)
    ) %>%
    group_by(Asset) %>%
    fill(c(win_value, loss_value), .direction = "updown") %>%
    group_by(Asset) %>%
    summarise(
      Profit = sum(Profit, na.rm = T),
      Profit = sum(Profit/(10^pipLocation), na.rm = T),
      Profit_dollar = sum(Profit_dollar, na.rm = T),
      win_loss  = sum(win_loss),
      total_trades = n(),
      average_win_pip = round(mean( (win_value)/(10^pipLocation), na.rm = T), digits = 4),
      average_loss_pip = round(mean( (loss_value)/(10^pipLocation), na.rm = T), digits = 4)
    )  %>%
    mutate(
      Perc =win_loss/total_trades
    ) %>%
    mutate(
      Variant = "Long Loss join Short Win and Short Loss"
    )

  total_trade_summary <-
    variant_1_summary %>%
    bind_rows(variant_2_summary)

  # test<-variant_1 %>%
  #   distinct(Date, Asset, `Short Direction Loss`, `Long Direction Win`, `Long Direction Loss`) %>%
  #   mutate(
  #     Variant = "Short Loss join Long Win and Long Loss"
  #   ) %>%
  #   left_join(
  #     variant_2 %>%
  #       distinct(Date, Asset) %>%
  #       mutate(
  #         Variant2 = "Long Loss join Short Win and Short Loss"
  #       )
  #   )

  if(return_summary == TRUE) {
    return(total_trade_summary)
  } else {
    return(
      list("Variant 1" = variant_1,
           "Variant 2" = variant_2)
    )
  }

}

