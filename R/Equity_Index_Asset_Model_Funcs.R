#' get_bench_mark_results
#'
#' @param asset_data
#' @param Asset
#' @param min_date
#'
#' @return
#' @export
#'
#' @examples
get_bench_mark_results <-
  function(asset_data = major_indices_log_cumulative,
           Asset_Var = "SPX500_USD",
           min_date = "2018-01-01",
           trade_direction = "Long",
           risk_dollar_value = 5,
           stop_factor = 4,
           profit_factor = 8,
           mean_values_by_asset_for_loop = mean_values_by_asset_for_loop,
           currency_conversion = currency_conversion
  ) {

    tagged_trades <-
      asset_data %>%
      ungroup() %>%
      filter(Asset == Asset_Var, Date >= as_date(min_date)) %>%
      mutate(trade_col = trade_direction)

    bench_mark_ts <- run_pairs_analysis(
      tagged_trades = tagged_trades %>% filter(trade_col == trade_direction),
      stop_factor = stop_factor,
      profit_factor = profit_factor,
      raw_asset_data = asset_data,
      risk_dollar_value = risk_dollar_value,
      return_trade_ts = TRUE
    )

    bench_mark_analysis <- run_pairs_analysis(
      tagged_trades = tagged_trades %>% filter(trade_col == trade_direction),
      stop_factor = stop_factor,
      profit_factor = profit_factor,
      raw_asset_data = asset_data,
      risk_dollar_value = risk_dollar_value
    )

    bench_mark_analysis_total <- bench_mark_analysis[[1]]
    bench_mark_analysis_asset <- bench_mark_analysis[[2]]

    benchmark_ts_with_returns <-
      get_stops_profs_volume_trades(tagged_trades = bench_mark_ts %>%
                                      rename(Asset = asset, Date = dates) %>%
                                      left_join(asset_data %>%
                                                  dplyr::select(Date, Asset, Price, Low, High, Open)),
                                    mean_values_by_asset =  mean_values_by_asset_for_loop,
                                    trade_col = "trade_col",
                                    currency_conversion = currency_conversion,
                                    risk_dollar_value = risk_dollar_value,
                                    stop_factor = stop_factor,
                                    profit_factor =profit_factor,
                                    asset_col = "Asset",
                                    stop_col = "stop_value",
                                    profit_col = "profit_value",
                                    price_col = "Price",
                                    trade_return_col = "trade_returns") %>%
      mutate(
        trade_returns =
          case_when(
            trade_col == "Long" & trade_start_prices < trade_end_prices ~ maximum_win,
            trade_col == "Long" & trade_start_prices >= trade_end_prices ~ -1*minimal_loss,
            trade_col == "Short" & trade_start_prices > trade_end_prices ~ maximum_win,
            trade_col == "Short" & trade_start_prices <= trade_end_prices ~ -1*minimal_loss
          )
      ) %>%
      dplyr::select(Date, trade_returns, Asset)

    benchmark_ts_sum <- benchmark_ts_with_returns %>%
      group_by(Date) %>%
      summarise(trade_returns = sum(trade_returns, na.rm = T)) %>%
      mutate(
        cumulative_returns = cumsum(trade_returns)
      )

    return(
      list(benchmark_ts_sum,
           benchmark_ts_with_returns,
           bench_mark_analysis_asset)
    )

  }

#' get_2_asset_combined_results
#'
#' @param asset_1_tag_trades
#' @param asset_2_tag_trades
#' @param asset_data_ask
#' @param asset_data_bid
#'
#' @return
#' @export
#'
#' @examples
get_5_index_equity_combined_results <-
  function(
    asset_1_tag_trades = asset_1_tag,
    asset_2_tag_trades = asset_2_tag,
    asset_3_tag_trades = asset_3_tag,
    asset_4_tag_trades = asset_4_tag,
    asset_5_tag_trades = asset_5_tag,
    asset_6_tag_trades = asset_6_tag,
    asset_data_ask = major_indices_log_cumulative,
    asset_data_bid = major_indices_log_cumulative_bid,
    risk_dollar_value = risk_dollar_value,
    profit_factor = profit_factor,
    stop_factor = stop_factor,
    profit_factor_long = profit_factor_long,
    stop_factor_long = stop_factor_long,
    control_random_samples = control_random_samples
  ){

    long_analysis <- run_pairs_analysis(
      tagged_trades = asset_1_tag_trades %>% filter(trade_col == "Long"),
      stop_factor = stop_factor,
      profit_factor = profit_factor,
      raw_asset_data = asset_data_ask,
      risk_dollar_value = risk_dollar_value
    )

    long_analysis_total <- long_analysis[[1]]
    long_analysis_asset <- long_analysis[[2]]

    long_comparison <- long_analysis_asset %>%
      dplyr::select(trade_direction , Asset, Trades, wins, Final_Dollars,
                    risk_weighted_return_strat = risk_weighted_return,
                    profit_factor, stop_factor) %>%
      left_join(control_random_samples %>%
                  ungroup() %>%
                  dplyr::select(-stop_factor, -profit_factor)) %>%
      mutate(
        p_value_risk =
          round(pnorm(risk_weighted_return_strat, mean = mean_risk, sd = sd_risk), 4)
      )

    short_analysis <- run_pairs_analysis(
      tagged_trades = asset_1_tag_trades %>% filter(trade_col == "Short"),
      stop_factor = stop_factor,
      profit_factor = profit_factor,
      raw_asset_data = asset_data_bid,
      risk_dollar_value = risk_dollar_value
    )

    short_analysis_total <- short_analysis[[1]]
    short_analysis_asset <- short_analysis[[2]]
    short_comparison <- short_analysis_asset %>%
      dplyr::select(trade_direction, Asset, Trades, wins, Final_Dollars,
                    risk_weighted_return_strat = risk_weighted_return,
                    profit_factor, stop_factor)  %>%
      left_join(control_random_samples %>%
                  ungroup() %>%
                  dplyr::select(-stop_factor, -profit_factor)) %>%
      mutate(
        p_value_risk =
          pnorm(risk_weighted_return_strat, mean = mean_risk, sd = sd_risk)
      )

    long_analysis2 <- run_pairs_analysis(
      tagged_trades = asset_2_tag_trades %>% filter(trade_col == "Long"),
      stop_factor = stop_factor_long,
      profit_factor = profit_factor_long,
      raw_asset_data = asset_data_ask,
      risk_dollar_value = risk_dollar_value
    )

    long_analysis_total2 <- long_analysis2[[1]]
    long_analysis_asset2 <- long_analysis2[[2]]

    long_comparison2 <- long_analysis_asset2 %>%
      dplyr::select(trade_direction , Asset, Trades,wins,  Final_Dollars,
                    risk_weighted_return_strat = risk_weighted_return,
                    profit_factor, stop_factor) %>%
      left_join(control_random_samples %>%
                  ungroup() %>%
                  dplyr::select(-profit_factor, -stop_factor)) %>%
      mutate(
        p_value_risk =
          round(pnorm(risk_weighted_return_strat, mean = mean_risk, sd = sd_risk), 4)
      )

    short_analysis2 <- run_pairs_analysis(
      tagged_trades = asset_2_tag_trades %>% filter(trade_col == "Short"),
      stop_factor = stop_factor,
      profit_factor = profit_factor,
      raw_asset_data = asset_data_bid,
      risk_dollar_value = risk_dollar_value
    )

    short_analysis_total2 <- short_analysis2[[1]]
    short_analysis_asset2 <- short_analysis2[[2]]
    short_comparison2 <- short_analysis_asset2 %>%
      dplyr::select(trade_direction, Asset, Trades, wins,  Final_Dollars,
                    risk_weighted_return_strat = risk_weighted_return,
                    profit_factor, stop_factor)  %>%
      left_join(control_random_samples %>%
                  ungroup() %>%
                  dplyr::select(-stop_factor, -profit_factor)) %>%
      mutate(
        p_value_risk =
          pnorm(risk_weighted_return_strat, mean = mean_risk, sd = sd_risk)
      )


    long_analysis3 <- run_pairs_analysis(
      tagged_trades = asset_3_tag_trades %>% filter(trade_col == "Long"),
      stop_factor = stop_factor,
      profit_factor = profit_factor,
      raw_asset_data = asset_data_ask,
      risk_dollar_value = risk_dollar_value
    )

    long_analysis_total3 <- long_analysis3[[1]]
    long_analysis_asset3 <- long_analysis3[[2]]

    long_comparison3 <- long_analysis_asset3 %>%
      dplyr::select(trade_direction , Asset, Trades, wins,  Final_Dollars,
                    risk_weighted_return_strat = risk_weighted_return,
                    profit_factor, stop_factor) %>%
      left_join(control_random_samples %>%
                  ungroup() %>%
                  dplyr::select(-stop_factor, -profit_factor)) %>%
      mutate(
        p_value_risk =
          round(pnorm(risk_weighted_return_strat, mean = mean_risk, sd = sd_risk), 4)
      )

    short_analysis3 <- run_pairs_analysis(
      tagged_trades = asset_3_tag_trades %>% filter(trade_col == "Short"),
      stop_factor = stop_factor,
      profit_factor = profit_factor,
      raw_asset_data = asset_data_bid,
      risk_dollar_value = risk_dollar_value
    )

    short_analysis_total3 <- short_analysis3[[1]]
    short_analysis_asset3 <- short_analysis3[[2]]
    short_comparison3 <- short_analysis_asset3 %>%
      dplyr::select(trade_direction, Asset, Trades, wins, Final_Dollars,
                    risk_weighted_return_strat = risk_weighted_return,
                    profit_factor, stop_factor)  %>%
      left_join(control_random_samples %>%
                  ungroup() %>%
                  dplyr::select(-stop_factor, -profit_factor)) %>%
      mutate(
        p_value_risk =
          pnorm(risk_weighted_return_strat, mean = mean_risk, sd = sd_risk)
      )


    long_analysis4 <- run_pairs_analysis(
      tagged_trades = asset_4_tag_trades %>% filter(trade_col == "Long"),
      stop_factor = stop_factor,
      profit_factor = profit_factor,
      raw_asset_data = asset_data_ask,
      risk_dollar_value = risk_dollar_value
    )

    long_analysis_total4 <- long_analysis4[[1]]
    long_analysis_asset4 <- long_analysis4[[2]]

    long_comparison4 <- long_analysis_asset4 %>%
      dplyr::select(trade_direction , Asset, Trades,wins,  Final_Dollars,
                    risk_weighted_return_strat = risk_weighted_return,
                    profit_factor, stop_factor) %>%
      left_join(control_random_samples %>%
                  ungroup() %>%
                  dplyr::select(-stop_factor, -profit_factor)) %>%
      mutate(
        p_value_risk =
          round(pnorm(risk_weighted_return_strat, mean = mean_risk, sd = sd_risk), 4)
      )


    short_analysis4 <- run_pairs_analysis(
      tagged_trades = asset_4_tag_trades %>% filter(trade_col == "Short"),
      stop_factor = stop_factor,
      profit_factor = profit_factor,
      raw_asset_data = asset_data_bid,
      risk_dollar_value = risk_dollar_value
    )

    short_analysis_total4 <- short_analysis4[[1]]
    short_analysis_asset4 <- short_analysis4[[2]]
    short_comparison4 <- short_analysis_asset4 %>%
      dplyr::select(trade_direction, Asset, Trades, wins, Final_Dollars,
                    risk_weighted_return_strat = risk_weighted_return,
                    profit_factor, stop_factor)  %>%
      left_join(control_random_samples %>%
                  ungroup() %>%
                  dplyr::select(-stop_factor, -profit_factor)) %>%
      mutate(
        p_value_risk =
          pnorm(risk_weighted_return_strat, mean = mean_risk, sd = sd_risk)
      )


    long_analysis5 <- run_pairs_analysis(
      tagged_trades = asset_5_tag_trades %>% filter(trade_col == "Long"),
      stop_factor = stop_factor,
      profit_factor = profit_factor,
      raw_asset_data = asset_data_ask,
      risk_dollar_value = risk_dollar_value
    )

    long_analysis_total5 <- long_analysis5[[1]]
    long_analysis_asset5 <- long_analysis5[[2]]

    long_comparison5 <- long_analysis_asset5 %>%
      dplyr::select(trade_direction , Asset, Trades, wins,  Final_Dollars,
                    risk_weighted_return_strat = risk_weighted_return,
                    profit_factor, stop_factor) %>%
      left_join(control_random_samples %>%
                  ungroup() %>%
                  dplyr::select(-stop_factor, -profit_factor)) %>%
      mutate(
        p_value_risk =
          round(pnorm(risk_weighted_return_strat, mean = mean_risk, sd = sd_risk), 4)
      )


    short_analysis5 <- run_pairs_analysis(
      tagged_trades = asset_5_tag_trades %>% filter(trade_col == "Short"),
      stop_factor = stop_factor,
      profit_factor = profit_factor,
      raw_asset_data = asset_data_bid,
      risk_dollar_value = risk_dollar_value
    )

    short_analysis_total5 <- short_analysis5[[1]]
    short_analysis_asset5 <- short_analysis5[[2]]
    short_comparison5 <- short_analysis_asset5 %>%
      dplyr::select(trade_direction, Asset, Trades, wins, Final_Dollars,
                    risk_weighted_return_strat = risk_weighted_return,
                    profit_factor, stop_factor)  %>%
      left_join(control_random_samples %>%
                  ungroup() %>%
                  dplyr::select(-stop_factor, -profit_factor)) %>%
      mutate(
        p_value_risk =
          pnorm(risk_weighted_return_strat, mean = mean_risk, sd = sd_risk)
      )

    #-

    long_analysis6 <- run_pairs_analysis(
      tagged_trades = asset_6_tag_trades %>% filter(trade_col == "Long"),
      stop_factor = stop_factor,
      profit_factor = profit_factor,
      raw_asset_data = asset_data_ask,
      risk_dollar_value = risk_dollar_value
    )

    long_analysis_total6 <- long_analysis6[[1]]
    long_analysis_asset6 <- long_analysis6[[2]]

    long_comparison6 <- long_analysis_asset6 %>%
      dplyr::select(trade_direction , Asset, Trades, wins,  Final_Dollars,
                    risk_weighted_return_strat = risk_weighted_return,
                    profit_factor, stop_factor) %>%
      left_join(control_random_samples %>%
                  ungroup() %>%
                  dplyr::select(-stop_factor, -profit_factor)) %>%
      mutate(
        p_value_risk =
          round(pnorm(risk_weighted_return_strat, mean = mean_risk, sd = sd_risk), 4)
      )


    short_analysis6 <- run_pairs_analysis(
      tagged_trades = asset_6_tag_trades %>% filter(trade_col == "Short"),
      stop_factor = stop_factor,
      profit_factor = profit_factor,
      raw_asset_data = asset_data_bid,
      risk_dollar_value = risk_dollar_value
    )

    short_analysis_total6 <- short_analysis6[[1]]
    short_analysis_asset6 <- short_analysis6[[2]]
    short_comparison6 <- short_analysis_asset6 %>%
      dplyr::select(trade_direction, Asset, Trades, wins, Final_Dollars,
                    risk_weighted_return_strat = risk_weighted_return,
                    profit_factor, stop_factor)  %>%
      left_join(control_random_samples %>%
                  ungroup() %>%
                  dplyr::select(-stop_factor, -profit_factor)) %>%
      mutate(
        p_value_risk =
          pnorm(risk_weighted_return_strat, mean = mean_risk, sd = sd_risk)
      )

    full_comparison <-
      long_comparison %>%
      bind_rows(short_comparison) %>%
      bind_rows(long_comparison2) %>%
      bind_rows(short_comparison2) %>%
      bind_rows(long_comparison3) %>%
      bind_rows(short_comparison3) %>%
      bind_rows(long_comparison4) %>%
      bind_rows(short_comparison4)%>%
      bind_rows(long_comparison5) %>%
      bind_rows(short_comparison5)%>%
      bind_rows(long_comparison6) %>%
      bind_rows(short_comparison6)

    return(full_comparison)

  }

#' equity_index_asset_model
#'
#' @param major_indices_log_cumulative
#' @param assets_to_use
#' @param samples_for_MLE
#' @param test_samples
#' @param rolling_period
#' @param asset1
#' @param asset2
#' @param asset_1_fac
#' @param asset_2_fac
#'
#' @return
#' @export
#'
#' @examples
equity_index_asset_model_trades <-
  function(
    major_indices_log_cumulative = major_indices_log_cumulative_raw ,
    PCA_Data = NULL,
    assets_to_use = c("SPX500_USD", "US2000_USD", "NAS100_USD", "SG30_SGD", "AU200_AUD", "EU50_EUR", "DE30_EUR"),
    samples_for_MLE = 0.5,
    test_samples = 0.45,
    rolling_period = 100,
    date_filter_min = "2018-01-01",
    stop_factor = 4,
    profit_factor = 8,
    stop_factor_long = 10,
    profit_factor_long = 15
  ) {

    major_indices_log_cumulative <-
      major_indices_log_cumulative %>%
      group_by(Asset) %>%
      mutate(
        Return_Index_Diff = ((Price - Open)/Open)*100
      ) %>%
      ungroup() %>%
      filter(!is.na(Return_Index_Diff)) %>%
      filter(Date >= date_filter_min)

    if(is.null(PCA_Data)) {
      major_indices_PCA_Index <-
        create_PCA_Asset_Index(
          asset_data_to_use = major_indices_log_cumulative,
          asset_to_use = assets_to_use,
          price_col = "Return_Index"
        ) %>%
        rename(
          Average_PCA_Index = Average_PCA
        )
    }

    if(!is.null(PCA_Data)) {
      major_indices_PCA_Index <-
        PCA_Data  %>%
        rename(
          Average_PCA_Index = Average_PCA
        )
    }

    major_indices_cumulative_pca <-
      major_indices_log_cumulative %>%
      left_join(major_indices_PCA_Index)

    asset_1 <-
      get_PCA_Index_rolling_cor_sd_mean(
        raw_asset_data_for_PCA_cor = major_indices_log_cumulative %>% filter(Asset == "SPX500_USD"),
        PCA_data = major_indices_cumulative_pca,
        rolling_period = rolling_period
      )

    asset_2 <-
      get_PCA_Index_rolling_cor_sd_mean(
        raw_asset_data_for_PCA_cor = major_indices_log_cumulative %>% filter(Asset == "US2000_USD"),
        PCA_data = major_indices_cumulative_pca,
        rolling_period = rolling_period
      )

    asset_3 <-
      get_PCA_Index_rolling_cor_sd_mean(
        raw_asset_data_for_PCA_cor = major_indices_log_cumulative %>% filter(Asset == "EU50_EUR"),
        PCA_data = major_indices_cumulative_pca,
        rolling_period = rolling_period
      )

    asset_4 <-
      get_PCA_Index_rolling_cor_sd_mean(
        raw_asset_data_for_PCA_cor = major_indices_log_cumulative %>% filter(Asset == "AU200_AUD"),
        PCA_data = major_indices_cumulative_pca,
        rolling_period = rolling_period
      )

    asset_5 <-
      get_PCA_Index_rolling_cor_sd_mean(
        raw_asset_data_for_PCA_cor = major_indices_log_cumulative %>% filter(Asset == "SG30_SGD"),
        PCA_data = major_indices_cumulative_pca,
        rolling_period = rolling_period
      )

    asset_1_tag <-
      asset_1 %>%
      mutate(trade_col =
               case_when(
                 rolling_cor_PC1 <= rolling_cor_PC1_mean - 2.75*rolling_cor_PC1_sd & tan_angle < 0 ~ "Long",
                 rolling_cor_PC1 >= rolling_cor_PC1_mean + 2.75*rolling_cor_PC1_sd & tan_angle < 0 ~ "Long",
                 rolling_cor_PC1 >= rolling_cor_PC1_mean + 2*rolling_cor_PC1_sd & tan_angle > 0 ~ "Short",
                 rolling_cor_PC2 <= rolling_cor_PC2_mean - 3*rolling_cor_PC2_sd & tan_angle < 0 ~ "Long",
                 rolling_cor_PC2 >= rolling_cor_PC2_mean + 2.75*rolling_cor_PC2_sd & tan_angle < 0 ~ "Long",
                 rolling_cor_PC2 >= rolling_cor_PC2_mean + 3.1*rolling_cor_PC2_sd & tan_angle > 0  ~ "Short",
                 rolling_cor_PC3 <= rolling_cor_PC3_mean - 2.75*rolling_cor_PC3_sd & tan_angle < 0 ~ "Long",
                 rolling_cor_PC3 <= rolling_cor_PC3_mean - 2.75*rolling_cor_PC3_sd & tan_angle > 0 ~ "Short",
                 rolling_cor_PC4 <= rolling_cor_PC4_mean - 3*rolling_cor_PC4_sd & tan_angle < 0 ~ "Long",
                 rolling_cor_PC4 <= rolling_cor_PC4_mean - 3*rolling_cor_PC4_sd & tan_angle > 0 ~ "Long",
                 rolling_cor_PC4 >= rolling_cor_PC4_mean + 3*rolling_cor_PC4_sd & tan_angle < 0 ~ "Long",
                 rolling_cor_PC4 >= rolling_cor_PC4_mean + 3.25*rolling_cor_PC4_sd & tan_angle > 0 ~ "Long",
                 rolling_cor_PC5 >= rolling_cor_PC5_mean + 3*rolling_cor_PC5_sd & tan_angle < 0 ~ "Long",
                 rolling_cor_PC5 >= rolling_cor_PC5_mean + 3*rolling_cor_PC5_sd & tan_angle > 0 ~ "Short"
               )
      ) %>%
      filter(!is.na(trade_col))

    asset_2_tag <-
      asset_2 %>%
      mutate(trade_col =
               case_when(
                 rolling_cor_PC1 <= rolling_cor_PC1_mean - 3*rolling_cor_PC1_sd & tan_angle < 0 ~ "Long",
                 rolling_cor_PC1 >= rolling_cor_PC1_mean + 3*rolling_cor_PC1_sd & tan_angle < 0 ~ "Long",
                 rolling_cor_PC1 >= rolling_cor_PC1_mean + 2*rolling_cor_PC1_sd & tan_angle > 0 ~ "Short",
                 rolling_cor_PC2 <= rolling_cor_PC2_mean - 2.75*rolling_cor_PC2_sd & tan_angle < 0 &
                   rolling_cor_PC2 >= rolling_cor_PC2_mean - 3.5*rolling_cor_PC2_sd & tan_angle < 0 ~ "Long",
                 rolling_cor_PC2 >= rolling_cor_PC2_mean + 2.75*rolling_cor_PC2_sd & tan_angle < 0  &
                   rolling_cor_PC2 <= rolling_cor_PC2_mean + 3.5*rolling_cor_PC2_sd & tan_angle < 0  ~ "Long",
                 rolling_cor_PC2 >= rolling_cor_PC2_mean + 3*rolling_cor_PC2_sd & tan_angle > 0  ~ "Short",
                 rolling_cor_PC3 <= rolling_cor_PC3_mean - 3.5*rolling_cor_PC3_sd & tan_angle > 0 ~ "Long",
                 rolling_cor_PC4 <= rolling_cor_PC4_mean - 2.75*rolling_cor_PC4_sd & tan_angle < 0 ~ "Long",
                 rolling_cor_PC4 <= rolling_cor_PC4_mean - 2.75*rolling_cor_PC4_sd & tan_angle > 0 ~ "Short",
                 rolling_cor_PC5 <= rolling_cor_PC5_mean - 3.5*rolling_cor_PC5_sd & tan_angle < 0 ~ "Long",
                 rolling_cor_PC5 <= rolling_cor_PC5_mean - 3.5*rolling_cor_PC5_sd & tan_angle > 0 ~ "Short",
                 rolling_cor_PC5 >= rolling_cor_PC5_mean + 3.5*rolling_cor_PC5_sd & tan_angle < 0 ~ "Short"
               )
      ) %>%
      filter(!is.na(trade_col))

    # EU50_EUR
    asset_3_tag <-
      asset_3 %>%
      mutate(trade_col =
               case_when(
                 rolling_cor_PC1 <= rolling_cor_PC1_mean - 2.5*rolling_cor_PC1_sd & tan_angle > 0 ~ "Long",
                 rolling_cor_PC1 >= rolling_cor_PC1_mean + 2*rolling_cor_PC1_sd & tan_angle < 0 ~ "Long",
                 rolling_cor_PC1 >= rolling_cor_PC1_mean + 2*rolling_cor_PC1_sd & tan_angle > 0 ~ "Short",

                 rolling_cor_PC2 <= rolling_cor_PC2_mean - 3.5*rolling_cor_PC2_sd & tan_angle < 0 ~ "Long",
                 rolling_cor_PC2 <= rolling_cor_PC2_mean - 3.2*rolling_cor_PC2_sd & tan_angle > 0  ~ "Long",

                 rolling_cor_PC2 >= rolling_cor_PC2_mean + 3.2*rolling_cor_PC2_sd & tan_angle < 0 ~ "Long",
                 rolling_cor_PC2 >= rolling_cor_PC2_mean + 2.75*rolling_cor_PC2_sd & tan_angle > 0  ~ "Short",

                 rolling_cor_PC3 <= rolling_cor_PC3_mean - 3.5*rolling_cor_PC3_sd & tan_angle < 0 ~ "Long",
                 rolling_cor_PC3 <= rolling_cor_PC3_mean - 3.25*rolling_cor_PC3_sd & tan_angle > 0 ~ "Long",

                 rolling_cor_PC3 >= rolling_cor_PC3_mean + 3.25*rolling_cor_PC3_sd & tan_angle < 0 ~ "Long",
                 rolling_cor_PC3 >= rolling_cor_PC3_mean + 3.25*rolling_cor_PC3_sd & tan_angle > 0 ~ "Long",

                 rolling_cor_PC4 <= rolling_cor_PC4_mean - 3.2*rolling_cor_PC4_sd & tan_angle < 0 ~ "Long",
                 rolling_cor_PC4 <= rolling_cor_PC4_mean - 2.75*rolling_cor_PC4_sd & tan_angle > 0 ~ "Long",
                 rolling_cor_PC4 >= rolling_cor_PC4_mean + 3.5*rolling_cor_PC4_sd & tan_angle > 0 ~ "Short",
                 rolling_cor_PC5 <= rolling_cor_PC5_mean - 3*rolling_cor_PC5_sd & tan_angle < 0 ~ "Long",
                 rolling_cor_PC5 <= rolling_cor_PC5_mean - 3*rolling_cor_PC5_sd & tan_angle > 0 ~ "Long",
                 rolling_cor_PC5 >= rolling_cor_PC5_mean + 3*rolling_cor_PC5_sd & tan_angle > 0 ~ "Long"
               )
      ) %>%
      filter(!is.na(trade_col))

    # AU200_AUD
    asset_4_tag <-
      asset_4 %>%
      mutate(trade_col =
               case_when(
                 rolling_cor_PC1 <= rolling_cor_PC1_mean - 3*rolling_cor_PC1_sd & tan_angle > 0 ~ "Long",
                 rolling_cor_PC1 <= rolling_cor_PC1_mean - 3*rolling_cor_PC1_sd & tan_angle < 0 ~ "Long",
                 rolling_cor_PC1 >= rolling_cor_PC1_mean + 3*rolling_cor_PC1_sd & tan_angle < 0 ~ "Long",
                 rolling_cor_PC1 >= rolling_cor_PC1_mean + 2.75*rolling_cor_PC1_sd & tan_angle > 0 ~ "Short",

                 rolling_cor_PC2 <= rolling_cor_PC2_mean - 3.6*rolling_cor_PC2_sd & tan_angle < 0 ~ "Long",
                 rolling_cor_PC2 <= rolling_cor_PC2_mean - 3.3*rolling_cor_PC2_sd & tan_angle > 0  ~ "Long",

                 rolling_cor_PC2 >= rolling_cor_PC2_mean + 3.3*rolling_cor_PC2_sd & tan_angle < 0 ~ "Long",
                 rolling_cor_PC2 >= rolling_cor_PC2_mean + 3.3*rolling_cor_PC2_sd & tan_angle > 0  ~ "Long",

                 rolling_cor_PC3 <= rolling_cor_PC3_mean - 3.25*rolling_cor_PC3_sd & tan_angle < 0 ~ "Long",
                 rolling_cor_PC3 <= rolling_cor_PC3_mean - 3.25*rolling_cor_PC3_sd & tan_angle > 0 ~ "Long",

                 rolling_cor_PC3 >= rolling_cor_PC3_mean + 3.25*rolling_cor_PC3_sd & tan_angle < 0 ~ "Long",
                 rolling_cor_PC3 >= rolling_cor_PC3_mean + 3.25*rolling_cor_PC3_sd & tan_angle > 0 ~ "Long",

                 rolling_cor_PC4 <= rolling_cor_PC4_mean - 3.0*rolling_cor_PC4_sd & tan_angle < 0 ~ "Short",
                 rolling_cor_PC4 >= rolling_cor_PC4_mean + 3.2*rolling_cor_PC4_sd & tan_angle > 0 ~ "Long",

                 rolling_cor_PC5 <= rolling_cor_PC5_mean - 3*rolling_cor_PC5_sd & tan_angle < 0 ~ "Short",
                 rolling_cor_PC5 >= rolling_cor_PC5_mean + 3*rolling_cor_PC5_sd & tan_angle < 0 ~ "Long"
               )
      ) %>%
      filter(!is.na(trade_col))

    asset_5_tag <-
      asset_5 %>%
      mutate(trade_col =
               case_when(
                 rolling_cor_PC1 <= rolling_cor_PC1_mean - 3.25*rolling_cor_PC1_sd & tan_angle > 0 ~ "Long",
                 rolling_cor_PC1 >= rolling_cor_PC1_mean + 3.25*rolling_cor_PC1_sd & tan_angle < 0 ~ "Long",
                 rolling_cor_PC1 >= rolling_cor_PC1_mean + 2.5*rolling_cor_PC1_sd & tan_angle > 0 ~ "Short",
                 rolling_cor_PC2 <= rolling_cor_PC2_mean - 3.25*rolling_cor_PC2_sd & tan_angle > 0  ~ "Long",
                 rolling_cor_PC2 >= rolling_cor_PC2_mean + 3*rolling_cor_PC2_sd & tan_angle < 0 ~ "Short",
                 rolling_cor_PC2 >= rolling_cor_PC2_mean + 3*rolling_cor_PC2_sd & tan_angle > 0  ~ "Short",
                 rolling_cor_PC3 <= rolling_cor_PC3_mean - 3.5*rolling_cor_PC3_sd & tan_angle > 0 ~ "Short",
                 rolling_cor_PC3 >= rolling_cor_PC3_mean + 3*rolling_cor_PC3_sd & tan_angle < 0 ~ "Long",
                 rolling_cor_PC4 <= rolling_cor_PC4_mean - 2.75*rolling_cor_PC4_sd & tan_angle < 0 ~ "Short",
                 rolling_cor_PC4 <= rolling_cor_PC4_mean - 2.75*rolling_cor_PC4_sd & tan_angle > 0 ~ "Long"

               )
      ) %>%
      filter(!is.na(trade_col))

    return(list(
      asset_1_tag,
      asset_2_tag,
      asset_3_tag,
      asset_4_tag,
      asset_5_tag
    ))

  }


#' equity_index_asset_model_trades_diff_vers
#'
#' @param major_indices_log_cumulative
#' @param assets_to_use
#' @param samples_for_MLE
#' @param test_samples
#' @param rolling_period
#' @param asset1
#' @param asset2
#' @param asset_1_fac
#' @param asset_2_fac
#'
#' @return
#' @export
#'
#' @examples
equity_index_asset_model_trades_diff_vers <-
  function(
    major_indices_log_cumulative = major_indices_log_cumulative_raw ,
    PCA_Data = NULL,
    assets_to_use = c("SPX500_USD", "US2000_USD", "NAS100_USD", "SG30_SGD", "AU200_AUD", "EU50_EUR", "DE30_EUR"),
    samples_for_MLE = 0.5,
    test_samples = 0.45,
    rolling_period = 100,
    date_filter_min = "2018-01-01",
    stop_factor = 4,
    profit_factor = 8,
    stop_factor_long = 4,
    profit_factor_long = 8
  ) {

    major_indices_log_cumulative <-
      major_indices_log_cumulative %>%
      group_by(Asset) %>%
      mutate(
        Return_Index_Diff = ((Price - Open)/Open)*100
      ) %>%
      ungroup() %>%
      filter(!is.na(Return_Index_Diff)) %>%
      filter(Date >= date_filter_min)

    if(is.null(PCA_Data)) {
      major_indices_PCA_Index <-
        create_PCA_Asset_Index(
          asset_data_to_use = major_indices_log_cumulative,
          asset_to_use = assets_to_use,
          price_col = "Return_Index_Diff"
        ) %>%
        rename(
          Average_PCA_Index = Average_PCA
        )
    }

    if(!is.null(PCA_Data)) {
      major_indices_PCA_Index <-
        PCA_Data  %>%
        rename(
          Average_PCA_Index = Average_PCA
        )
    }

    major_indices_cumulative_pca <-
      major_indices_log_cumulative %>%
      left_join(major_indices_PCA_Index)

    asset_1 <-
      get_PCA_Index_rolling_cor_sd_mean(
        raw_asset_data_for_PCA_cor = major_indices_log_cumulative %>% filter(Asset == "SPX500_USD"),
        PCA_data = major_indices_cumulative_pca,
        rolling_period = rolling_period
      )

    asset_2 <-
      get_PCA_Index_rolling_cor_sd_mean(
        raw_asset_data_for_PCA_cor = major_indices_log_cumulative %>% filter(Asset == "US2000_USD"),
        PCA_data = major_indices_cumulative_pca,
        rolling_period = rolling_period
      )

    asset_3 <-
      get_PCA_Index_rolling_cor_sd_mean(
        raw_asset_data_for_PCA_cor = major_indices_log_cumulative %>% filter(Asset == "EU50_EUR"),
        PCA_data = major_indices_cumulative_pca,
        rolling_period = rolling_period
      )

    asset_4 <-
      get_PCA_Index_rolling_cor_sd_mean(
        raw_asset_data_for_PCA_cor = major_indices_log_cumulative %>% filter(Asset == "AU200_AUD"),
        PCA_data = major_indices_cumulative_pca,
        rolling_period = rolling_period
      )

    asset_5 <-
      get_PCA_Index_rolling_cor_sd_mean(
        raw_asset_data_for_PCA_cor = major_indices_log_cumulative %>% filter(Asset == "SG30_SGD"),
        PCA_data = major_indices_cumulative_pca,
        rolling_period = rolling_period
      )

    asset_1_tag <-
      asset_1 %>%
      mutate(trade_col =
               case_when(
                 rolling_cor_PC1 <= rolling_cor_PC1_mean - 2.0*rolling_cor_PC1_sd & tan_angle > 0 ~ "Long", #19%
                 rolling_cor_PC1 >= rolling_cor_PC1_mean + 2.75*rolling_cor_PC1_sd & tan_angle < 0 ~ "Long", #16%
                 rolling_cor_PC2 >= rolling_cor_PC2_mean + 2*rolling_cor_PC2_sd & tan_angle > 0 ~ "Long",  #20%
                 rolling_cor_PC3 <= rolling_cor_PC3_mean - 2.25*rolling_cor_PC3_sd & tan_angle > 0 ~ "Long", #4%
                 rolling_cor_PC3 <= rolling_cor_PC3_mean - 2.25*rolling_cor_PC3_sd & tan_angle < 0 ~ "Short", #13%
                 rolling_cor_PC3 >= rolling_cor_PC3_mean + 2.5*rolling_cor_PC3_sd & tan_angle > 0 ~ "Long", #4%
                 rolling_cor_PC3 >= rolling_cor_PC3_mean + 2.5*rolling_cor_PC3_sd & tan_angle < 0 ~ "Long", #13%
                 rolling_cor_PC4 <= rolling_cor_PC4_mean - 2.5*rolling_cor_PC4_sd & tan_angle > 0 ~ "Long", #15%
                 rolling_cor_PC4 <= rolling_cor_PC4_mean - 2.5*rolling_cor_PC4_sd & tan_angle < 0 ~ "Long",  #15%
                 rolling_cor_PC4 >= rolling_cor_PC4_mean + 2.25*rolling_cor_PC4_sd & tan_angle < 0 ~ "Short" #18%
               )
      ) %>%
      filter(!is.na(trade_col))

    asset_2_tag <-
      asset_2 %>%
      mutate(trade_col =
               case_when(
                 rolling_cor_PC1 <= rolling_cor_PC1_mean - 2.75*rolling_cor_PC1_sd & tan_angle > 0 ~ "Short", #7%
                 rolling_cor_PC1 >= rolling_cor_PC1_mean + 2*rolling_cor_PC1_sd & tan_angle > 0 ~ "Long", #13%
                 rolling_cor_PC2 <= rolling_cor_PC2_mean - 2.25*rolling_cor_PC2_sd & tan_angle > 0 ~ "Long", #7%
                 rolling_cor_PC2 >= rolling_cor_PC2_mean + 2.5*rolling_cor_PC2_sd & tan_angle < 0 ~ "Short",
                 rolling_cor_PC2 >= rolling_cor_PC2_mean + 2.25*rolling_cor_PC2_sd & tan_angle > 0 ~ "Long",
                 rolling_cor_PC4 <= rolling_cor_PC4_mean - 2.5*rolling_cor_PC4_sd & tan_angle > 0 ~ "Long"
               )
      ) %>%
      filter(!is.na(trade_col))

    # EU50_EUR
    asset_3_tag <-
      asset_3 %>%
      mutate(trade_col =
               case_when(
                 rolling_cor_PC1 <= rolling_cor_PC1_mean - 2*rolling_cor_PC1_sd & tan_angle > 0 ~ "Short", #8%
                 rolling_cor_PC1 >= rolling_cor_PC1_mean + 2*rolling_cor_PC1_sd & tan_angle < 0 ~ "Long", #16%
                 rolling_cor_PC2 >= rolling_cor_PC2_mean + 2*rolling_cor_PC2_sd & tan_angle > 0 ~ "Long", #19%
                 rolling_cor_PC2 >= rolling_cor_PC2_mean + 2*rolling_cor_PC2_sd & tan_angle < 0 ~ "Short", #8%
                 rolling_cor_PC3 <= rolling_cor_PC3_mean - 2*rolling_cor_PC3_sd & tan_angle > 0 ~ "Long",
                 rolling_cor_PC3 <= rolling_cor_PC3_mean - 2*rolling_cor_PC3_sd & tan_angle < 0 ~ "Short",
                 rolling_cor_PC4 <= rolling_cor_PC4_mean - 2*rolling_cor_PC4_sd & tan_angle > 0 ~ "Long", #23%
                 rolling_cor_PC4 >= rolling_cor_PC4_mean + 2*rolling_cor_PC4_sd & tan_angle < 0 ~ "Short" #9%,
               )
      ) %>%
      filter(!is.na(trade_col))

    # AU200_AUD
    asset_4_tag <-
      asset_4 %>%
      mutate(trade_col =
               case_when(
                 rolling_cor_PC1 <= rolling_cor_PC1_mean - 2*rolling_cor_PC1_sd & tan_angle < 0 ~ "Short",
                 rolling_cor_PC1 >= rolling_cor_PC1_mean + 2*rolling_cor_PC1_sd & tan_angle > 0 ~ "Long", #12%
                 rolling_cor_PC2 >= rolling_cor_PC2_mean + 2*rolling_cor_PC2_sd & tan_angle < 0 ~ "Long", #9%
                 rolling_cor_PC2 <= rolling_cor_PC2_mean - 2*rolling_cor_PC2_sd & tan_angle < 0 ~ "Long", #11%
                 rolling_cor_PC3 <= rolling_cor_PC3_mean - 2*rolling_cor_PC3_sd & tan_angle > 0 ~ "Long"
               )
      ) %>%
      filter(!is.na(trade_col))

    asset_5_tag <-
      asset_5 %>%
      mutate(trade_col =
               case_when(

                 rolling_cor_PC2 >= rolling_cor_PC2_mean + 2*rolling_cor_PC2_sd & tan_angle < 0 ~ "Long",
                 rolling_cor_PC2 >= rolling_cor_PC2_mean + 2*rolling_cor_PC2_sd & tan_angle > 0 ~ "Long",
                 rolling_cor_PC2 <= rolling_cor_PC2_mean - 2*rolling_cor_PC2_sd & tan_angle < 0 ~ "Short",
                 rolling_cor_PC3 <= rolling_cor_PC3_mean - 2.25*rolling_cor_PC3_sd & tan_angle < 0 ~ "Short" #11,

               )
      ) %>%
      filter(!is.na(trade_col))

    return(list(
      asset_1_tag,
      asset_2_tag,
      asset_3_tag,
      asset_4_tag,
      asset_5_tag
    ))

  }


#' equity_index_asset_model
#'
#' @param major_indices_log_cumulative
#' @param assets_to_use
#' @param samples_for_MLE
#' @param test_samples
#' @param rolling_period
#' @param asset1
#' @param asset2
#' @param asset_1_fac
#' @param asset_2_fac
#'
#' @return
#' @export
#'
#' @examples
equity_index_asset_model <-
  function(
    asset_data_ask = major_indices_log_cumulative,
    asset_data_bid = major_indices_log_cumulative_bid,
    asset_1_tag,
    asset_2_tag,
    asset_3_tag,
    asset_4_tag,
    asset_5_tag,
    risk_dollar_value = 5,
    stop_factor = 4,
    profit_factor = 8,
    stop_factor_long = 10,
    profit_factor_long = 15,
    control_random_samples,
    US2000_bench_mark = US2000_bench_mark,
    SPX_bench_mark = SPX_bench_mark
  ) {

    combined_results <-
      get_5_index_equity_combined_results(
        asset_1_tag_trades = asset_1_tag,
        asset_2_tag_trades = asset_2_tag,
        asset_3_tag_trades = asset_3_tag,
        asset_4_tag_trades = asset_4_tag,
        asset_5_tag_trades = asset_5_tag,
        asset_data_ask = asset_data_ask,
        asset_data_bid = asset_data_bid,
        risk_dollar_value = risk_dollar_value,
        profit_factor = profit_factor,
        stop_factor = stop_factor,
        profit_factor_long = profit_factor_long,
        stop_factor_long = stop_factor_long,
        control_random_samples = control_random_samples
      )

    benchmark_risk_results <-
      SPX_bench_mark[[3]] %>%
      dplyr::select(Asset, trade_direction,
                    risk_return_benchmark = risk_weighted_return,
                    final_dollars_benchmark = Final_Dollars)

    US2000_bench_risk <- US2000_bench_mark[[3]] %>%
      dplyr::select(Asset, trade_direction,
                    risk_return_benchmark = risk_weighted_return,
                    final_dollars_benchmark = Final_Dollars)

    combined_results_benched <-
      combined_results %>%
      left_join(benchmark_risk_results %>% bind_rows(US2000_bench_risk))

    long_analysis_ts <- run_pairs_analysis(
      tagged_trades = asset_1_tag %>% filter(trade_col == "Long"),
      stop_factor = stop_factor,
      profit_factor = profit_factor,
      raw_asset_data = asset_data_ask,
      risk_dollar_value = risk_dollar_value,
      return_trade_ts = TRUE
    )

    short_analysis_ts <- run_pairs_analysis(
      tagged_trades = asset_1_tag %>% filter(trade_col == "Short"),
      stop_factor = stop_factor,
      profit_factor = profit_factor,
      raw_asset_data = asset_data_bid,
      risk_dollar_value = risk_dollar_value,
      return_trade_ts = TRUE
    )

    long_analysis_ts_2 <- run_pairs_analysis(
      tagged_trades = asset_2_tag %>% filter(trade_col == "Long"),
      stop_factor = stop_factor_long,
      profit_factor = profit_factor_long,
      raw_asset_data = asset_data_ask,
      risk_dollar_value = risk_dollar_value,
      return_trade_ts = TRUE
    )

    short_analysis_ts_2 <- run_pairs_analysis(
      tagged_trades = asset_2_tag %>% filter(trade_col == "Short"),
      stop_factor = stop_factor,
      profit_factor = profit_factor,
      raw_asset_data = asset_data_bid,
      risk_dollar_value = risk_dollar_value,
      return_trade_ts = TRUE
    )

    long_analysis_ts_3 <- run_pairs_analysis(
      tagged_trades = asset_3_tag %>% filter(trade_col == "Long"),
      stop_factor = stop_factor,
      profit_factor = profit_factor,
      raw_asset_data = asset_data_ask,
      risk_dollar_value = risk_dollar_value,
      return_trade_ts = TRUE
    )

    short_analysis_ts_3 <- run_pairs_analysis(
      tagged_trades = asset_3_tag %>% filter(trade_col == "Short"),
      stop_factor = stop_factor,
      profit_factor = profit_factor,
      raw_asset_data = asset_data_bid,
      risk_dollar_value = risk_dollar_value,
      return_trade_ts = TRUE
    )

    long_analysis_ts_4 <- run_pairs_analysis(
      tagged_trades = asset_4_tag %>% filter(trade_col == "Long"),
      stop_factor = stop_factor,
      profit_factor = profit_factor,
      raw_asset_data = asset_data_ask,
      risk_dollar_value = risk_dollar_value,
      return_trade_ts = TRUE
    )

    short_analysis_ts_4 <- run_pairs_analysis(
      tagged_trades = asset_4_tag %>% filter(trade_col == "Short"),
      stop_factor = stop_factor,
      profit_factor = profit_factor,
      raw_asset_data = asset_data_bid,
      risk_dollar_value = risk_dollar_value,
      return_trade_ts = TRUE
    )

    long_analysis_ts_5 <- run_pairs_analysis(
      tagged_trades = asset_5_tag %>% filter(trade_col == "Long"),
      stop_factor = stop_factor,
      profit_factor = profit_factor,
      raw_asset_data = asset_data_ask,
      risk_dollar_value = risk_dollar_value,
      return_trade_ts = TRUE
    )

    short_analysis_ts_5 <- run_pairs_analysis(
      tagged_trades = asset_5_tag %>% filter(trade_col == "Short"),
      stop_factor = stop_factor,
      profit_factor = profit_factor,
      raw_asset_data = asset_data_bid,
      risk_dollar_value = risk_dollar_value,
      return_trade_ts = TRUE
    )

    trade_returns_ts <-
      list(
        list(long_analysis_ts %>%
               rename(Asset = asset, Date = dates) %>%
               left_join(asset_data_ask %>%
                           dplyr::select(Date, Asset, Price, Low, High, Open)),
             "SPX500_USD"),
        list(long_analysis_ts_2 %>%
               rename(Asset = asset, Date = dates) %>%
               left_join(asset_data_ask %>%
                           dplyr::select(Date, Asset, Price, Low, High, Open)),
             "US2000_USD"),
        list(long_analysis_ts_3 %>%
               rename(Asset = asset, Date = dates) %>%
               left_join(asset_data_ask %>%
                           dplyr::select(Date, Asset, Price, Low, High, Open)),
             "EU50_EUR"),
        list(long_analysis_ts_4 %>%
               rename(Asset = asset, Date = dates) %>%
               left_join(asset_data_ask %>%
                           dplyr::select(Date, Asset, Price, Low, High, Open)),
             "AU200_AUD"),
        list(long_analysis_ts_5 %>%
               rename(Asset = asset, Date = dates) %>%
               left_join(asset_data_ask %>%
                           dplyr::select(Date, Asset, Price, Low, High, Open)),
             "SG30_SGD"),
        list(short_analysis_ts %>%
               rename(Asset = asset, Date = dates) %>%
               left_join(asset_data_bid %>%
                           dplyr::select(Date, Asset, Price, Low, High, Open)),
             "SPX500_USD"),
        list(short_analysis_ts_2 %>%
               rename(Asset = asset, Date = dates) %>%
               left_join(asset_data_bid %>%
                           dplyr::select(Date, Asset, Price, Low, High, Open)),
             "US2000_USD"),
        list(short_analysis_ts_3 %>%
               rename(Asset = asset, Date = dates) %>%
               left_join(asset_data_bid %>%
                           dplyr::select(Date, Asset, Price, Low, High, Open)),
             "EU50_EUR"),
        list(short_analysis_ts_4 %>%
               rename(Asset = asset, Date = dates) %>%
               left_join(asset_data_bid %>%
                           dplyr::select(Date, Asset, Price, Low, High, Open)),
             "AU200_AUD"),
        list(short_analysis_ts_5 %>%
               rename(Asset = asset, Date = dates) %>%
               left_join(asset_data_bid %>%
                           dplyr::select(Date, Asset, Price, Low, High, Open)),
             "SG30_SGD")
      ) %>%
      map(
        ~
          get_stops_profs_volume_trades(tagged_trades = .x[[1]],
                                        mean_values_by_asset =  mean_values_by_asset_for_loop,
                                        trade_col = "trade_col",
                                        currency_conversion = currency_conversion,
                                        risk_dollar_value = risk_dollar_value,
                                        stop_factor = stop_factor,
                                        profit_factor =profit_factor,
                                        asset_col = "Asset",
                                        stop_col = "stop_value",
                                        profit_col = "profit_value",
                                        price_col = "Price",
                                        trade_return_col = "trade_returns") %>%
          mutate(
            trade_returns =
              case_when(
                trade_col == "Long" & trade_start_prices < trade_end_prices ~ maximum_win,
                trade_col == "Long" & trade_start_prices > trade_end_prices ~ -1*minimal_loss,
                trade_col == "Short" & trade_start_prices > trade_end_prices ~ maximum_win,
                trade_col == "Short" & trade_start_prices < trade_end_prices ~ -1*minimal_loss
              )
          ) %>%
          dplyr::select(Date, trade_returns, Asset)
      ) %>%
      reduce(bind_rows) %>%
      distinct()

    trade_returns_ts_sum <-
      trade_returns_ts %>%
      distinct() %>%
      group_by(Date) %>%
      summarise(trade_returns = sum(trade_returns, na.rm = T)) %>%
      mutate(
        cumulative_returns = cumsum(trade_returns)
      )

    bench_mark_ts_sum <-
      SPX_bench_mark[[1]] %>%
      distinct(Date, trade_returns) %>%
      arrange(Date) %>%
      mutate(cumulative_returns = cumsum(trade_returns))

    plot_data <- trade_returns_ts %>%
      group_by(Date) %>%
      summarise(trade_returns = sum(trade_returns, na.rm = T)) %>%
      mutate(
        cumulative_returns = cumsum(trade_returns)
      ) %>%
      mutate(
        strat_or_control = "Strat"
      ) %>%
      bind_rows(bench_mark_ts_sum  %>%
                  mutate(
                    strat_or_control = "Control"
                  )
      )

    returned_plot <- plot_data %>%
      ggplot(aes(x = Date, y = cumulative_returns, color = strat_or_control)) +
      geom_line() +
      theme_minimal() +
      theme(legend.position = "bottom", legend.title = element_blank())

    total_trades <- combined_results_benched$Trades %>% sum()
    total_time_periods <- major_indices_log_cumulative %>% pull(Date) %>% unique() %>% length()
    total_trades/total_time_periods

    return(
      list(
        combined_results_benched,
        returned_plot
      )
    )

  }

#' commod_model_trades_diff_vers
#'
#' @param major_indices_log_cumulative
#' @param assets_to_use
#' @param samples_for_MLE
#' @param test_samples
#' @param rolling_period
#' @param asset1
#' @param asset2
#' @param asset_1_fac
#' @param asset_2_fac
#'
#' @return
#' @export
#'
#' @examples
commod_model_trades_diff_vers <-
  function(
    commod_data = commod_log_cumulative ,
    PCA_Data = NULL,
    assets_to_use = analysis_syms,
    samples_for_MLE = 0.5,
    test_samples = 0.45,
    rolling_period = 100,
    date_filter_min = "2011-01-01",
    stop_factor = 12,
    profit_factor = 15
  ) {

    commod_data <-
      commod_data %>%
      group_by(Asset) %>%
      mutate(
        Return_Index_Diff = ((Price - Open)/Open)*100
      ) %>%
      ungroup() %>%
      filter(!is.na(Return_Index_Diff)) %>%
      filter(Date >= date_filter_min)

    if(is.null(PCA_Data)) {
      commod_data_Index <-
        create_PCA_Asset_Index(
          asset_data_to_use = commod_data,
          asset_to_use = assets_to_use,
          price_col = "Return_Index_Diff"
        ) %>%
        rename(
          Average_PCA_Index = Average_PCA
        )
    }

    if(!is.null(PCA_Data)) {
      commod_data_Index <-
        PCA_Data  %>%
        rename(
          Average_PCA_Index = Average_PCA
        )
    }

    commod_data_pca <-
      commod_data %>%
      left_join(commod_data_Index)

    asset_1 <-
      get_PCA_Index_rolling_cor_sd_mean(
        raw_asset_data_for_PCA_cor = commod_data %>% filter(Asset == "BCO_USD"),
        PCA_data = commod_data_pca,
        rolling_period = rolling_period
      )

    asset_2 <-
      get_PCA_Index_rolling_cor_sd_mean(
        raw_asset_data_for_PCA_cor = commod_data %>% filter(Asset == "NATGAS_USD"),
        PCA_data = commod_data_pca,
        rolling_period = rolling_period
      )

    asset_3 <-
      get_PCA_Index_rolling_cor_sd_mean(
        raw_asset_data_for_PCA_cor = commod_data %>% filter(Asset == "SOYBN_USD"),
        PCA_data = commod_data_pca,
        rolling_period = rolling_period
      )

    asset_4 <-
      get_PCA_Index_rolling_cor_sd_mean(
        raw_asset_data_for_PCA_cor = commod_data %>% filter(Asset == "SUGAR_USD"),
        PCA_data = commod_data_pca,
        rolling_period = rolling_period
      )

    asset_5 <-
      get_PCA_Index_rolling_cor_sd_mean(
        raw_asset_data_for_PCA_cor = commod_data %>% filter(Asset == "WHEAT_USD"),
        PCA_data = commod_data_pca,
        rolling_period = rolling_period
      )

    asset_6 <-
      get_PCA_Index_rolling_cor_sd_mean(
        raw_asset_data_for_PCA_cor = commod_data %>% filter(Asset == "WTICO_USD"),
        PCA_data = commod_data_pca,
        rolling_period = rolling_period
      )

    asset_7 <-
      get_PCA_Index_rolling_cor_sd_mean(
        raw_asset_data_for_PCA_cor = commod_data %>% filter(Asset == "XCU_USD"),
        PCA_data = commod_data_pca,
        rolling_period = rolling_period
      )


    # BCO_USD
    asset_1_tag <-
      asset_1 %>%
      mutate(trade_col =
               case_when(
                 rolling_cor_PC1 <= rolling_cor_PC1_mean - 2*rolling_cor_PC1_sd & tan_angle > 0 ~ "Long",
                 rolling_cor_PC1 <= rolling_cor_PC1_mean - 2.5*rolling_cor_PC1_sd & tan_angle < 0 ~ "Short",
                 rolling_cor_PC1 >= rolling_cor_PC1_mean + 2.5*rolling_cor_PC1_sd & tan_angle < 0 ~ "Short",
                 rolling_cor_PC2 <= rolling_cor_PC2_mean - 2*rolling_cor_PC2_sd & tan_angle > 0 ~ "Long",
                 rolling_cor_PC2 <= rolling_cor_PC2_mean - 2*rolling_cor_PC2_sd & tan_angle < 0 ~ "Short",
                 rolling_cor_PC3 >= rolling_cor_PC3_mean + 2*rolling_cor_PC3_sd & tan_angle < 0 ~ "Short", #9%
                 rolling_cor_PC3 >= rolling_cor_PC3_mean + 2*rolling_cor_PC3_sd & tan_angle > 0 ~ "Long" #23%
               )
      ) %>%
      filter(!is.na(trade_col))


    # NATGAS_USD
    asset_2_tag <-
      asset_2 %>%
      mutate(trade_col =
               case_when(
                 rolling_cor_PC1 <= rolling_cor_PC1_mean - 2.5*rolling_cor_PC1_sd & tan_angle > 0 ~ "Long", #15%
                 rolling_cor_PC1 <= rolling_cor_PC1_mean - 2.75*rolling_cor_PC1_sd & tan_angle < 0 ~ "Short", #8%
                 rolling_cor_PC2 <= rolling_cor_PC2_mean - 2*rolling_cor_PC2_sd & tan_angle > 0 ~ "Long",
                 rolling_cor_PC2 >= rolling_cor_PC2_mean + 2*rolling_cor_PC2_sd & tan_angle > 0 ~ "Long",
                 rolling_cor_PC3 <= rolling_cor_PC3_mean - 2*rolling_cor_PC3_sd & tan_angle > 0 ~ "Long", #13%
                 rolling_cor_PC3 >= rolling_cor_PC3_mean + 2*rolling_cor_PC3_sd & tan_angle > 0 ~ "Long" #11%
               )
      ) %>%
      filter(!is.na(trade_col))

    # SOYBN_USD
    asset_3_tag <-
      asset_3 %>%
      mutate(trade_col =
               case_when(
                 rolling_cor_PC1 <= rolling_cor_PC1_mean - 2.75*rolling_cor_PC1_sd & tan_angle < 0 ~ "Short", #7%
                 rolling_cor_PC1 >= rolling_cor_PC1_mean + 2.75*rolling_cor_PC1_sd & tan_angle > 0 ~ "Long",
                 rolling_cor_PC2 >= rolling_cor_PC2_mean + 2.5*rolling_cor_PC2_sd & tan_angle > 0 ~ "Long",
                 rolling_cor_PC2 >= rolling_cor_PC2_mean + 2.5*rolling_cor_PC2_sd & tan_angle < 0 ~ "Short"
               )
      ) %>%
      filter(!is.na(trade_col))

    # SUGAR_USD
    asset_4_tag <-
      asset_4 %>%
      mutate(trade_col =
               case_when(
                 rolling_cor_PC1 <= rolling_cor_PC1_mean - 2.5*rolling_cor_PC1_sd & tan_angle < 0 ~ "Short", #7%
                 rolling_cor_PC1 >= rolling_cor_PC1_mean + 2*rolling_cor_PC1_sd & tan_angle < 0 ~ "Short", #16%
                 rolling_cor_PC2 <= rolling_cor_PC2_mean - 2*rolling_cor_PC2_sd & tan_angle > 0 ~ "Short",
                 rolling_cor_PC2 <= rolling_cor_PC2_mean - 2*rolling_cor_PC2_sd & tan_angle < 0 ~ "Long",
                 rolling_cor_PC3 <= rolling_cor_PC3_mean - 2*rolling_cor_PC3_sd & tan_angle < 0 ~ "Short", #8%
               )
      ) %>%
      filter(!is.na(trade_col))

    # WHEAT_USD
    asset_5_tag <-
      asset_5 %>%
      mutate(trade_col =
               case_when(
                 rolling_cor_PC1 >= rolling_cor_PC1_mean + 3*rolling_cor_PC1_sd & tan_angle > 0 ~ "Short",#%-2%
                 rolling_cor_PC1 >= rolling_cor_PC1_mean + 3*rolling_cor_PC1_sd & tan_angle < 0 ~ "Long", #6%
                 rolling_cor_PC2 <= rolling_cor_PC2_mean - 2.5*rolling_cor_PC2_sd & tan_angle > 0 ~ "Short",
                 rolling_cor_PC3 >= rolling_cor_PC3_mean + 2*rolling_cor_PC3_sd & tan_angle < 0 ~ "Long", #7%
                 rolling_cor_PC3 >= rolling_cor_PC3_mean + 2*rolling_cor_PC3_sd & tan_angle > 0 ~ "Long" #-6%

               )
      ) %>%
      filter(!is.na(trade_col))

    # WTICO_USD
    asset_6_tag <-
      asset_6 %>%
      mutate(trade_col =
               case_when(
                 rolling_cor_PC1 <= rolling_cor_PC1_mean - 2.75*rolling_cor_PC1_sd & tan_angle > 0 ~ "Long",
                 rolling_cor_PC1 <= rolling_cor_PC1_mean - 2.75*rolling_cor_PC1_sd & tan_angle < 0 ~ "Short",
                 rolling_cor_PC2 <= rolling_cor_PC2_mean - 2*rolling_cor_PC2_sd & tan_angle > 0 ~ "Long",
                 rolling_cor_PC2 <= rolling_cor_PC2_mean - 2*rolling_cor_PC2_sd & tan_angle < 0 ~ "Short",
                 rolling_cor_PC3 >= rolling_cor_PC3_mean + 2*rolling_cor_PC3_sd & tan_angle > 0 ~ "Long" #14%
               )
      ) %>%
      filter(!is.na(trade_col))

    return(list(
      asset_1_tag,
      asset_2_tag,
      asset_3_tag,
      asset_4_tag,
      asset_5_tag,
      asset_6_tag
    ))

  }

