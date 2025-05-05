helperfunctions35South::load_custom_functions()
one_drive_path <- helperfunctions35South::create_one_drive_path(
  path_extension = "raw data")
library(neuralnet)

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
                    "US2000_USD",
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
asset_data_daily_raw <-asset_data_combined

mean_values_by_asset_for_loop =
  wrangle_asset_data(
    asset_data_daily_raw = asset_data_daily_raw,
    summarise_means = TRUE
  )

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

#---------------
# Best Trades:
# Rolling Period = 400
# Profit Factor = 5
# Stop Factor = 5
# trade_sd_fact = 2.7
#Lows Direction: "Short", "Long"
#Highs Direction: "Long", "Short"
#Lows Results Risk Weighted Return: Long -0.001, Short = 0.10
#Highs Results Risk Weighted Return: Long 0.19, Short = -0.2
#Logic Pattern: Strong
profit_factor  = 5
stop_factor  = 5
risk_dollar_value <- 20
markov_trades_raw <-
  get_markov_tag_pos_neg_diff(
    asset_data_combined = asset_data_combined,
    training_perc = 1,
    sd_divides = seq(0.5,2,0.5),
    quantile_divides = seq(0.1,0.9, 0.1),
    rolling_period = 400,
    markov_col_on_interest_pos = "Markov_Point_Pos_roll_sum_1.5",
    markov_col_on_interest_neg = "Markov_Point_Neg_roll_sum_-1.5",
    sum_sd_cut_off = "",
    profit_factor  = profit_factor,
    stop_factor  = stop_factor,
    asset_data_daily_raw = asset_data_combined,
    mean_values_by_asset_for_loop = mean_values_by_asset_for_loop,
    trade_sd_fact = 2.5
  )

summary_data <-
  list(markov_trades_raw$`Trade Summaries`[[1]] %>% mutate(Markov_Col = "Low"),
       markov_trades_raw$`Trade Summaries`[[2]] %>% mutate(Markov_Col = "High")
       ) %>%
  map_dfr(bind_rows) %>%
  rename(
    trade_col = trade_direction
  )

trades_today <-
  list(markov_trades_raw$Trades[[1]] %>% mutate(Markov_Col = "Low"),
       markov_trades_raw$Trades[[2]] %>% mutate(Markov_Col = "High")
  )
  # map_dfr(bind_rows)  %>%
  # left_join(summary_data) %>%
  # slice_max(Date)

trades_today <- trades_today %>%
  map(
    ~ .x %>%
      left_join(summary_data) %>%
      slice_max(Date) %>%
      filter(!is.na(trade_col)) %>%
      left_join(mean_values_by_asset_for_loop) %>%
      left_join(asset_infor %>% rename(Asset = name)) %>%
      mutate(ending_value = str_extract(Asset, "_[A-Z][A-Z][A-Z]") %>% str_remove_all("_")) %>%
      left_join(currency_conversion, by =c("ending_value" = "not_aud_asset")) %>%
      mutate(
        profit_point = Price + mean_daily + sd_daily*profit_factor,
        stop_point = Price - mean_daily + sd_daily*stop_factor,

        stop_value =  mean_daily + sd_daily*stop_factor,
        profit_value = mean_daily + sd_daily*profit_factor,

        profit_points_pip =   profit_value/(10^pipLocation),
        stop_points_pip = stop_value/(10^pipLocation)
      ) %>%
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
            str_detect(ending_value, "HUF") & type == "CURRENCY" ~ 10,
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
  ) %>%
  map_dfr(bind_rows) %>%
  filter( (trade_col == "Long" & Markov_Col == "High") |
           (trade_col == "Short" & Markov_Col == "Low"),
          risk_weighted_return > 0.14
          )

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

trade_list_for_today <- trades_today %>%
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
    loss_var <- trade_list_for_today$stop_value[i] %>% as.numeric()
    profit_var <- trade_list_for_today$profit_value[i] %>% as.numeric()

    if(loss_var > 9) { loss_var <- round(loss_var)}
    if(profit_var > 9) { profit_var <- round(profit_var)}

    trade_list_for_today$estimated_margin[i]

    profit_var>=loss_var

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
      acc_name = account_name,
      position_fill = "OPEN_ONLY" ,
      price
    )

  }

}


#-------------------------------------------------------------------------------------------------------

# Best Trades:
#-------Long: Trades = 8000, Perc = 0.64, prof = 12, stop = 12, rolling = 400, bayes_prior_trade = 120,bayes_prior = 240
#-posterior_difference <= posterior_difference_mean - trade_sd_fact_post*posterior_difference_sd
#-trade_sd_fact_post: = 0.5
#-sigma_difference <= sigma_difference_mean - trade_sd_fact_sigma*sigma_difference_sd
#-trade_sd_fact_sigma: = 0.25
#-sigma_n_high>sigma_n_low
profit_factor  = 6
stop_factor  = 6
risk_dollar_value <- 30
markov_trades_raw_bayes <-
  get_markov_tag_bayes(
    asset_data_combined = asset_data_combined,
    training_perc = 1,
    sd_divides = seq(0.5,2,0.5),
    quantile_divides = seq(0.1,0.9, 0.1),
    rolling_period = 400,
    markov_col_on_interest_pos = "Markov_Point_Pos_roll_sum_1.5",
    markov_col_on_interest_neg = "Markov_Point_Neg_roll_sum_-1.5",
    sum_sd_cut_off = "",
    profit_factor  = profit_factor,
    stop_factor  = stop_factor,
    asset_data_daily_raw = asset_data_combined,
    mean_values_by_asset_for_loop = mean_values_by_asset_for_loop,
    trade_sd_fact_post = 0.5,
    trade_sd_fact_post_high = 1,
    trade_sd_fact_sigma = 0.25,
    bayes_prior = 240,
    bayes_prior_trade = 120
  )

summary_data <-
  list(markov_trades_raw_bayes$Markov_Trades_Bayes_Summary) %>%
  map_dfr(bind_rows) %>%
  rename(
    trade_col = trade_direction
  )

trades_today <-
  markov_trades_raw_bayes[[1]] %>%
      slice_max(Date) %>%
      filter(!is.na(trade_col)) %>%
      left_join(summary_data) %>%
      left_join(mean_values_by_asset_for_loop) %>%
      left_join(asset_infor %>% rename(Asset = name)) %>%
      mutate(ending_value = str_extract(Asset, "_[A-Z][A-Z][A-Z]") %>% str_remove_all("_")) %>%
      left_join(currency_conversion, by =c("ending_value" = "not_aud_asset")) %>%
      mutate(

        stop_value =  mean_daily + sd_daily*stop_factor,
        profit_value = mean_daily + sd_daily*profit_factor,

        profit_points_pip =  profit_value/(10^pipLocation),
        stop_points_pip = stop_value/(10^pipLocation)

      ) %>%
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
            str_detect(ending_value, "HUF") & type == "CURRENCY" ~ 10,
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
      ) %>%
  filter(
    trade_col == "Long"
  )

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

trade_list_for_today <- trades_today %>%
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
    loss_var <- trade_list_for_today$stop_value[i] %>% as.numeric()
    profit_var <- trade_list_for_today$profit_value[i] %>% as.numeric()

    trade_list_for_today$estimated_margin[i]

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
      acc_name = account_name,
      position_fill = "OPEN_ONLY" ,
      price
    )

  }

}


#---------------
# Best Trades:
#-------Long: Trades = 8000, Perc = 0.64, prof = 12, stop = 12, rolling = 400, bayes_prior_trade = 120,bayes_prior = 240
#-posterior_difference <= posterior_difference_mean - trade_sd_fact_post*posterior_difference_sd
#-trade_sd_fact_post: = 0.5
#-sigma_difference <= sigma_difference_mean - trade_sd_fact_sigma*sigma_difference_sd
#-trade_sd_fact_sigma: = 0.25
#-sigma_n_high>sigma_n_low
profit_factor  = 12
stop_factor  = 12
risk_dollar_value <- 30
markov_trades_raw <-
  get_markov_tag_bayes(
    asset_data_combined = asset_data_combined,
    training_perc = 0.99,
    sd_divides = seq(0.5,2,0.5),
    quantile_divides = seq(0.1,0.9, 0.1),
    rolling_period = 400,
    markov_col_on_interest_pos = "Markov_Point_Pos_roll_sum_1.5",
    markov_col_on_interest_neg = "Markov_Point_Neg_roll_sum_-1.5",
    sum_sd_cut_off = "",
    profit_factor  = 12,
    stop_factor  = 12,
    asset_data_daily_raw = asset_data_combined,
    mean_values_by_asset_for_loop = mean_values_by_asset_for_loop,
    trade_sd_fact_post = 0.5,
    trade_sd_fact_post_high = 1,
    trade_sd_fact_sigma = 0.25,
    bayes_prior = 240,
    bayes_prior_trade = 120
  )


summary_data <-
  list(markov_trades_raw_bayes$Markov_Trades_Bayes_Summary) %>%
  map_dfr(bind_rows) %>%
  rename(
    trade_col = trade_direction
  )

trades_today <-
  markov_trades_raw_bayes$tagged_trades %>%
  slice_max(Date) %>%
  filter(!is.na(trade_col)) %>%
  left_join(summary_data) %>%
  left_join(mean_values_by_asset_for_loop) %>%
  left_join(asset_infor %>% rename(Asset = name)) %>%
  mutate(ending_value = str_extract(Asset, "_[A-Z][A-Z][A-Z]") %>% str_remove_all("_")) %>%
  left_join(currency_conversion, by =c("ending_value" = "not_aud_asset")) %>%
  mutate(

    stop_value =  mean_daily + sd_daily*stop_factor,
    profit_value = mean_daily + sd_daily*profit_factor,

    profit_points_pip =  profit_value/(10^pipLocation),
    stop_points_pip = stop_value/(10^pipLocation)

  ) %>%
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
  ) %>%
  filter(
    trade_col == "Long"
  )

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

trade_list_for_today <- trades_today %>%
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
    loss_var <- trade_list_for_today$stop_value[i] %>% as.numeric()
    profit_var <- trade_list_for_today$profit_value[i] %>% as.numeric()

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
      acc_name = account_name,
      position_fill = "OPEN_ONLY" ,
      price
    )

  }

}

