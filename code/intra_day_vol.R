helperfunctions35South::load_custom_functions()
one_drive_path <- helperfunctions35South::create_one_drive_path(
  path_extension = "raw data")
library(neuralnet)

asset_list_oanda <- get_oanda_symbols() %>%
  keep( ~ .x %in% c("USD_JPY", "GBP_JPY", "USD_SGD", "EUR_SEK",
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
                    "JP225_USD", "SPX500_USD")
  )

asset_infor <- get_instrument_info()


#----------------------------------------------------------------------------------------
risk_dollar_value <- 20
# sd_facs == 0.25,
# rolling_period == 200,
# stop_minus == -1.5,
# profit_plus == 2.25

# sd_facs = 0.00,
# training_prop = 0.8,
# stop_minus = -1,
# profit_plus = 1.25,
# rolling_period = 300

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

raw_vol_results_db <- connect_db("C:/Users/Nikhil Chandra/Documents/trade_data/volatility_model_intra.db")
results_list <- list()

for (j in 66:dim(trade_params3)[1]) {


  sd_facs <- trade_params3$sd_facs[j] %>% as.numeric()
  stop_minus <- trade_params3$stop_minus[j] %>% as.numeric()
  profit_plus <- trade_params3$profit_plus[j] %>% as.numeric()
  rolling_period <- trade_params3$rolling_period[j] %>% as.numeric()

  tagged_trades <- get_tag_volatility_trades(
    asset_data_daily_raw = asset_data_daily_raw,
    mean_values_by_asset_for_loop = mean_values_by_asset_for_loop,
    sd_facs = sd_facs,
    training_prop = 0.8,
    stop_minus = stop_minus,
    profit_plus = profit_plus, rolling_period = rolling_period
  )

  results_temp <- trade_finder_volatility(
    tagged_trades = tagged_trades,
    asset_data_daily_raw = asset_data_daily_raw,
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

  summary_temp <-
    results_temp2 %>%
    # filter(Asset %in% target_assets) %>%
    left_join(asset_infor %>% rename(Asset = name)) %>%
    mutate(ending_value = str_extract(Asset, "_[A-Z][A-Z][A-Z]") %>% str_remove_all("_")) %>%
    left_join(currency_conversion, by =c("ending_value" = "not_aud_asset")) %>%
    mutate(
      minimumTradeSize = abs(log10(as.numeric(minimumTradeSize))),
      marginRate = as.numeric(marginRate),
      pipLocation = as.numeric(pipLocation),
      displayPrecision = as.numeric(displayPrecision)
    ) %>%
    mutate(
      trade_returns_pip = trade_returns/(10^pipLocation)
    ) %>%
    ungroup() %>%
    mutate(
      stop_value = round(stop_value, abs(pipLocation) ),
      trade_returns = round(trade_returns, abs(pipLocation) )
    ) %>%
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
      return_value_AUD =
        case_when(
          !is.na(adjusted_conversion) ~ (trade_returns*adjusted_conversion)/volume_adjustment,
          TRUE ~ trade_returns/volume_adjustment
        ),

      volume_unadj =  risk_dollar_value/stop_value_AUD,
      volume_required = volume_unadj,
      volume_adj = round(volume_unadj, minimumTradeSize),
      minimal_loss =  volume_adj*stop_value_AUD,
      trade_return_dollars_AUD =  volume_adj*return_value_AUD,
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

  results_list[[j]] <- summary_temp %>%
    mutate(
      wins = ifelse(trade_return_dollars_AUD > 0, 1, 0)
    ) %>%
    group_by(Date, Asset) %>%
    summarise(
      trade_return_dollars_AUD = sum(trade_return_dollars_AUD, na.rm = T),
      trade_returns_pip = sum(trade_returns_pip, na.rm = T),
      total_trades = n(),
      wins = sum(wins, na.rm = T)
    ) %>%
    mutate(
      across(.cols = c(trade_return_dollars_AUD, trade_returns_pip, total_trades, wins),
             .fns = ~ ifelse(is.infinite(.)|is.na(.)|is.nan(.), 0, .))
    ) %>%
    arrange(Date) %>%
    mutate(
      cumulative_dollars = cumsum(trade_return_dollars_AUD)
    ) %>%
    mutate(
      sd_facs = trade_params3$sd_facs[j] %>% as.numeric(),
      stop_minus = trade_params3$stop_minus[j] %>% as.numeric(),
      profit_plus = trade_params3$profit_plus[j] %>% as.numeric(),
      rolling_period = trade_params3$rolling_period[j] %>% as.numeric()
    )

}

analyse_return_ts <-
  results_list %>%
  keep(~!is.null(.x)) %>%
  map_dfr(
    ~ .x %>%
      filter(Asset %in% c(
                          "AUD_USD", "EUR_USD", "USD_JPY", "GBP_USD",
                          "NZD_USD", "USD_SEK", "USD_NOK", "EUR_GBP",
                          "EUR_JPY"
                          # "DE30_EUR","SG30_SGD", "SG30_SGD", "XCU_USD",
                          # "NAS100_USD", "FR40_EUR", "NATGAS_USD", "EU50_EUR",
                          # "BCO_USD", "WHEAT_USD",
                          # "XAG_USD", "AU200_AUD","SPX500_USD"
                          )
             ) %>%
      mutate(Date=as_datetime(Date)) %>%
      group_by(Date, sd_facs, rolling_period, stop_minus, profit_plus) %>%
      mutate(wins = ifelse(trade_return_dollars_AUD > 0, 1, 0)) %>%
      summarise(
        total_trades = n(),
        wins = sum(wins, na.rm = T),
        trade_return_dollars_AUD = sum(trade_return_dollars_AUD, na.rm = T)
      ) %>%
      group_by(sd_facs, rolling_period, stop_minus, profit_plus) %>%
      arrange(Date, .by_group = TRUE) %>%
      group_by(sd_facs, rolling_period, stop_minus, profit_plus) %>%
      mutate(
        cumulative_dollars = cumsum(trade_return_dollars_AUD)
      ) %>%
      group_by(sd_facs, rolling_period, stop_minus, profit_plus) %>%
      mutate(start_date = min(Date, na.rm = T),
             start_date = as_datetime(start_date),
             lowest_point = min(cumulative_dollars, na.rm = T),
             low_point_25 = quantile(cumulative_dollars, 0.25 ,na.rm = T),
             mid_point_50 = quantile(cumulative_dollars, 0.5 ,na.rm = T),
             high_point_point = quantile(cumulative_dollars, 0.75 ,na.rm = T),
             max_point = max(cumulative_dollars, na.rm = T),
             total_trades = sum(total_trades, na.rm = T),
             wins = sum(wins, na.rm = T)
      ) %>%
      slice_tail(prop = 0.9) %>%
      group_by(sd_facs, rolling_period, stop_minus, profit_plus) %>%
      slice_min(Date)
  )

all_positive <- analyse_return_ts %>%
  mutate(
    perc = wins/total_trades
  ) %>%
  dplyr::select(-cumulative_dollars, -trade_return_dollars_AUD) %>%
  filter(lowest_point > -100)

analyse_return_ts2 <- results_list %>%
  keep(~!is.null(.x)) %>%
  map(
    ~ .x %>%
      filter(sd_facs == 0,
             rolling_period == 200,
             stop_minus == 0.5,
             profit_plus == 1.5)
  ) %>%
  keep(
    ~ dim(.x)[1] > 0
  ) %>%
  pluck(1) %>%
  mutate(Date = as_datetime(Date)) %>%
  filter(Asset %in%
           c(
             "AUD_USD", "EUR_USD", "USD_JPY", "GBP_USD",
             "NZD_USD", "USD_SEK", "USD_NOK", "EUR_GBP",
             "EUR_JPY"
             # "DE30_EUR","SG30_SGD", "SG30_SGD", "XCU_USD",
             # "NAS100_USD", "FR40_EUR", "NATGAS_USD", "EU50_EUR",
             # "BCO_USD", "WHEAT_USD",
             # "XAG_USD", "AU200_AUD","SPX500_USD"
             )
         ) %>%
  group_by(Date, sd_facs, rolling_period, stop_minus, profit_plus) %>%
  summarise(
    total_trades_date = n(),
    wins_date = sum(wins, na.rm = T),
    trade_return_dollars_AUD = sum(trade_return_dollars_AUD, na.rm = T)
  ) %>%
  group_by(Date, sd_facs, rolling_period, stop_minus, profit_plus) %>%
  arrange(Date, .by_group = TRUE) %>%
  group_by(sd_facs, rolling_period, stop_minus, profit_plus) %>%
  mutate(
    cumulative_dollars = cumsum(trade_return_dollars_AUD),
    total_trades = sum(total_trades_date),
    total_wins = sum(wins_date, na.rm = T),
  )

analyse_return_ts2 %>%
  ggplot(aes(x = Date, y = cumulative_dollars)) +
  geom_line() +
  theme_minimal()
