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

extracted_asset_data_bid <-
  read_all_asset_data_intra_day(
    asset_list_oanda = asset_list_oanda,
    save_path_oanda_assets = "C:/Users/Nikhil Chandra/Documents/Asset Data/oanda_data/",
    read_csv_or_API = "API",
    time_frame = "D",
    bid_or_ask = "ask",
    how_far_back = 5000,
    start_date = "2011-01-01"
  )

asset_data_combined_bid <- extracted_asset_data_bid %>% map_dfr(bind_rows)
asset_data_combined_bid <- asset_data_combined_bid %>%
  mutate(Date = as_date(Date))
asset_data_daily_raw_bid <-asset_data_combined_bid

mean_values_by_asset_for_loop =
  wrangle_asset_data(
    asset_data_daily_raw = asset_data_daily_raw_bid,
    summarise_means = TRUE
  )

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
risk_dollar_value <- 20

trade_params <-
  seq(1,3) %>%
  map_dfr(
    ~ tibble(trade_sd_fact = c(1.25, 1.5,2,2.25,2.5,2.7,3)) %>%
      mutate(
        stop_factor = .x
      ) %>%
      mutate(
        profit_factor = 2*stop_factor
      )
  ) %>%
  bind_rows(
    seq(2,5) %>%
      map_dfr(
        ~ tibble(trade_sd_fact = c(1.25, 1.5,2,2.25,2.5,2.7,3)) %>%
          mutate(
            stop_factor = .x
          ) %>%
          mutate(
            profit_factor = stop_factor
          )
      )
  )


target_assets <-
  c("USD_JPY", "GBP_JPY", "USD_SGD", "EUR_SEK",
    # "DE30_EUR",
    "USD_CHF", "USD_SEK", "XCU_USD", "SUGAR_USD",
    "USD_MXN", "GBP_USD", "WTICO_USD", "EUR_JPY", "USD_NOK",
    # "XAU_USD",
    # "USD_CZK",
    "WHEAT_USD",
    "EUR_USD",
    "SG30_SGD",
    "AU200_AUD", "XAG_USD",
    "EUR_GBP",
    # "USD_CNH",
    "USD_CAD", "NAS100_USD",
    "EU50_EUR", "NATGAS_USD", "SOYBN_USD",
    "US2000_USD",
    "BCO_USD", "AUD_USD", "NZD_USD", "NZD_CHF", "WHEAT_USD",
    # "JP225_USD",
    "SPX500_USD")

asset_data_loop_ask <- asset_data_combined_ask %>%
  filter(Asset %in% target_assets)
win_loss_ratio <- list()
plots_list <- list()

for (i in 1:dim(trade_params)[1] ) {

  trade_sd_fact <- trade_params$trade_sd_fact[i] %>% as.numeric()
  profit_factor <- trade_params$profit_factor[i] %>% as.numeric()
  stop_factor <- trade_params$stop_factor[i] %>% as.numeric()

  #Current Best - Long: Perc 0.4, risk_return = 0.214, 1000 Trades,
  # trade_sd_fact <- 3
  # profit_factor <- 4
  # stop_factor <- 2
  # rolling_period = 400
  # High

  #Current Best - Long: Perc 0.40 risk_return = 0.194, 40000 Trades,
  # trade_sd_fact <- 2
  # profit_factor <- 4
  # stop_factor <- 2
  # rolling_period = 400
  # High

  # trade_sd_fact <- 2
  # profit_factor <- 4
  # stop_factor <- 1

  markov_trades_raw <-
    get_markov_tag_pos_neg_diff(
      asset_data_combined = asset_data_loop_ask,
      training_perc = 1,
      sd_divides = seq(0.5,2,0.5),
      quantile_divides = seq(0.1,0.9, 0.1),
      rolling_period = 400,
      markov_col_on_interest_pos = "Markov_Point_Pos_roll_sum_1.5",
      markov_col_on_interest_neg = "Markov_Point_Neg_roll_sum_-1.5",
      sum_sd_cut_off = "",
      profit_factor  = profit_factor,
      stop_factor  = stop_factor,
      asset_data_daily_raw = asset_data_loop_ask,
      mean_values_by_asset_for_loop = mean_values_by_asset_for_loop,
      trade_sd_fact = trade_sd_fact
    )

  tagged_trades_markov_1 <-
    list(markov_trades_raw$Trades[[1]] %>% mutate(Markov_Col = "Low"),
         markov_trades_raw$Trades[[2]] %>% mutate(Markov_Col = "High")
    ) %>%
    map_dfr(
      ~
        generic_trade_finder_loop(
          tagged_trades = .x,
          asset_data_daily_raw = asset_data_loop_ask,
          stop_factor = stop_factor,
          profit_factor =profit_factor,
          trade_col = "trade_col",
          date_col = "Date",
          start_price_col = "Price",
          mean_values_by_asset =
            wrangle_asset_data(
              asset_data_daily_raw = asset_data_loop_ask,
              summarise_means = TRUE
            )
        ) %>%
        # filter(asset %in% target_assets) %>%
        mutate(
          Markov_Col = .x$Markov_Col[1] %>% as.character(),
          trade_sd_fact = trade_sd_fact,
          profit_factor = profit_factor,
          stop_factor = stop_factor
        )
    )

  trade_ts_asset <-
    tagged_trades_markov_1 %>%
    mutate(
      trade_groups = paste0(trade_col, "-", Markov_Col, "-", trade_sd_fact, "-", stop_factor , "-", profit_factor )
      ) %>%
    split(.$trade_groups) %>%
    map(
       ~
         analyse_trailing_trades(
           trade_data = .x,
           asset_data_daily_raw = asset_data_loop_ask,
           asset_infor = asset_infor,
           risk_dollar_value = risk_dollar_value,
           currency_conversion = currency_conversion
         ) %>%
         pluck(2) %>%
         mutate(
           trade_col = .x$trade_col[1] %>% as.character(),
           Markov_Col = .x$Markov_Col[1] %>% as.character(),
           trade_sd_fact = .x$trade_sd_fact[1] %>% as.numeric(),
           stop_factor = .x$stop_factor[1] %>% as.numeric(),
           profit_factor = .x$profit_factor[1] %>% as.numeric(),
           trade_groups = .x$trade_groups[1] %>% as.character()
         )
    )


  plots_list[[i]] <-
    trade_ts_asset %>%
    map_dfr(bind_rows) %>%
    filter(trade_col == "Long") %>%
    ggplot(aes(x = dates, y = cumulative_return_aud )) +
    geom_line() +
    facet_wrap(.~trade_groups, scales = "free") +
    theme_minimal()


  win_loss_ratio[[i]] <-
    convert_stop_profit_AUD(
      trade_data =
        tagged_trades_markov_1 %>%
        rename(Asset = asset) ,
      asset_infor = asset_infor,
      asset_col = "Asset",
      stop_col = "starting_stop_value",
      profit_col = "starting_profit_value",
      price_col = "trade_start_prices",
      risk_dollar_value = risk_dollar_value,
      returns_present = TRUE,
      trade_return_col = "trade_returns",
      currency_conversion = currency_conversion
    ) %>%
    filter(volume_required > 0) %>%
    mutate(wins =
             ifelse(trade_return_dollars_AUD >0 , 1, 0)
           ) %>%
    group_by(Markov_Col, trade_col, profit_factor, stop_factor, trade_sd_fact) %>%
    summarise(
      total_dollars = sum(trade_return_dollars_AUD),
      wins = sum(wins, na.rm = T),
      total_trades = n()
    ) %>%
    ungroup() %>%
    group_by(Markov_Col, trade_col, profit_factor, stop_factor, trade_sd_fact) %>%
    mutate(
      Perc = wins/total_trades
    ) %>%
    mutate(
      risk_weighted_return =
        Perc*(profit_factor/stop_factor) - (1- Perc)*(1)
    ) %>%
    filter(trade_col == "Long")


}

best_params <- trade_params %>%
  mutate(rowx = row_number())

win_loss_ratio_dfr_long <- win_loss_ratio %>%
  map_dfr(bind_rows)

best_params2 <- best_params %>%
  left_join(win_loss_ratio_dfr_long)

plots_list_long <- plots_list

write.csv(win_loss_ratio_dfr_long,
          file = "C:/Users/Nikhil Chandra/Documents/trade_data/markov_results_long.csv",
          row.names = FALSE)

plots_list_long[[7]]
plots_list_long[[5]]
plots_list_long[[12]]
#-----------------------------------------------------------------
trade_params <-
  seq(1,3) %>%
  map_dfr(
    ~ tibble(trade_sd_fact = c(1.25, 1.5,2,2.25,2.5,2.7,3)) %>%
      mutate(
        stop_factor = .x
      ) %>%
      mutate(
        profit_factor = 2*stop_factor
      )
  ) %>%
  bind_rows(
    seq(2,5) %>%
      map_dfr(
        ~ tibble(trade_sd_fact = c(1.25, 1.5,2,2.25,2.5,2.7,3)) %>%
          mutate(
            stop_factor = .x
          ) %>%
          mutate(
            profit_factor = stop_factor
          )
      )
  )

asset_data_loop_bid <- asset_data_combined_bid %>%
  filter(Asset %in% target_assets)

win_loss_ratio_short <- list()
trade_ts_asset <- list()

for (i in 1:dim(trade_params)[1] ) {

  trade_sd_fact <- trade_params$trade_sd_fact[i] %>% as.numeric()
  profit_factor <- trade_params$profit_factor[i] %>% as.numeric()
  stop_factor <- trade_params$stop_factor[i] %>% as.numeric()

  #Current Best - Long: Perc 0.4, risk_return = 0.214, 1000 Trades,
  # trade_sd_fact <- 3
  # profit_factor <- 4
  # stop_factor <- 2
  # rolling_period = 400
  # High

  #Current Best - Long: Perc 0.40 risk_return = 0.194, 40000 Trades,
  # trade_sd_fact <- 2
  # profit_factor <- 4
  # stop_factor <- 2
  # rolling_period = 400
  # High

  # trade_sd_fact <- 2
  # profit_factor <- 4
  # stop_factor <- 1

  markov_trades_raw <-
    get_markov_tag_pos_neg_diff(
      asset_data_combined = asset_data_loop_bid,
      training_perc = 1,
      sd_divides = seq(0.5,2,0.5),
      quantile_divides = seq(0.1,0.9, 0.1),
      rolling_period = 400,
      markov_col_on_interest_pos = "Markov_Point_Pos_roll_sum_1.5",
      markov_col_on_interest_neg = "Markov_Point_Neg_roll_sum_-1.5",
      sum_sd_cut_off = "",
      profit_factor  = profit_factor,
      stop_factor  = stop_factor,
      asset_data_daily_raw = asset_data_loop_bid,
      mean_values_by_asset_for_loop = mean_values_by_asset_for_loop,
      trade_sd_fact = trade_sd_fact
    )

  tagged_trades_markov_1 <-
    list(markov_trades_raw$Trades[[1]] %>% mutate(Markov_Col = "Low"),
         markov_trades_raw$Trades[[2]] %>% mutate(Markov_Col = "High")
    ) %>%
    map_dfr(
      ~
        generic_trade_finder_loop(
          tagged_trades = .x,
          asset_data_daily_raw = asset_data_loop_bid,
          stop_factor = stop_factor,
          profit_factor =profit_factor,
          trade_col = "trade_col",
          date_col = "Date",
          start_price_col = "Price",
          mean_values_by_asset =
            wrangle_asset_data(
              asset_data_daily_raw = asset_data_loop_bid,
              summarise_means = TRUE
            )
        ) %>%
        # filter(asset %in% target_assets) %>%
        mutate(
          Markov_Col = .x$Markov_Col[1] %>% as.character(),
          trade_sd_fact = trade_sd_fact,
          profit_factor = profit_factor,
          stop_factor = stop_factor
        )
    )

  trade_ts_asset <-
    tagged_trades_markov_1 %>%
    mutate(
      trade_groups = paste0(trade_col, "-", Markov_Col, "-", trade_sd_fact, "-", stop_factor , "-", profit_factor )
    ) %>%
    split(.$trade_groups) %>%
    map(
      ~
        analyse_trailing_trades(
          trade_data = .x,
          asset_data_daily_raw = asset_data_loop_bid,
          asset_infor = asset_infor,
          risk_dollar_value = risk_dollar_value,
          currency_conversion = currency_conversion
        ) %>%
        pluck(2) %>%
        mutate(
          trade_col = .x$trade_col[1] %>% as.character(),
          Markov_Col = .x$Markov_Col[1] %>% as.character(),
          trade_sd_fact = .x$trade_sd_fact[1] %>% as.numeric(),
          stop_factor = .x$stop_factor[1] %>% as.numeric(),
          profit_factor = .x$profit_factor[1] %>% as.numeric(),
          trade_groups = .x$trade_groups[1] %>% as.character()
        )
    )


  plots_list[[i]] <-
    trade_ts_asset %>%
    map_dfr(bind_rows) %>%
    filter(trade_col == "Short") %>%
    ggplot(aes(x = dates, y = cumulative_return_aud )) +
    geom_line() +
    facet_wrap(.~trade_groups, scales = "free") +
    theme_minimal()


  win_loss_ratio[[i]] <-
    convert_stop_profit_AUD(
      trade_data =
        tagged_trades_markov_1 %>%
        rename(Asset = asset) ,
      asset_infor = asset_infor,
      asset_col = "Asset",
      stop_col = "starting_stop_value",
      profit_col = "starting_profit_value",
      price_col = "trade_start_prices",
      risk_dollar_value = risk_dollar_value,
      returns_present = TRUE,
      trade_return_col = "trade_returns",
      currency_conversion = currency_conversion
    ) %>%
    filter(volume_required > 0) %>%
    mutate(wins =
             ifelse(trade_return_dollars_AUD >0 , 1, 0)
    ) %>%
    group_by(Markov_Col, trade_col, profit_factor, stop_factor, trade_sd_fact) %>%
    summarise(
      total_dollars = sum(trade_return_dollars_AUD),
      wins = sum(wins, na.rm = T),
      total_trades = n()
    ) %>%
    ungroup() %>%
    group_by(Markov_Col, trade_col, profit_factor, stop_factor, trade_sd_fact) %>%
    mutate(
      Perc = wins/total_trades
    ) %>%
    mutate(
      risk_weighted_return =
        Perc*(profit_factor/stop_factor) - (1- Perc)*(1)
    ) %>%
    filter(trade_col == "Short")


}

win_loss_ratio_dfr_short <- win_loss_ratio %>%
  map_dfr(bind_rows)

write.csv(win_loss_ratio_dfr_short,
          file = "C:/Users/Nikhil Chandra/Documents/trade_data/markov_results_short.csv",
          row.names = FALSE)

best_params2_short <- best_params %>%
  left_join(win_loss_ratio_dfr_short)

plots_list_short <- plots_list

plots_list_short[[6]]
plots_list_short[[7]]
plots_list_short[[13]]
plots_list_short[[15]]
plots_list_short[[43]]
plots_list_short[[36]]
