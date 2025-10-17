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


#' get_Major_Indices_trade_ts_actuals
#'
#' @param full_ts_trade_db_location
#' @param asset_infor
#' @param currency_conversion
#' @param stop_factor_var
#' @param profit_factor_var
#'
#' @return
#' @export
#'
#' @examples
get_Major_Indices_trade_ts_actuals <-
  function(
    full_ts_trade_db_location = full_ts_trade_db_location,
    asset_infor = asset_infor,
    currency_conversion = currency_conversion,
    stop_factor_var = 10,
    profit_factor_var = 15
  ) {

    full_ts_trade_db_con <- connect_db(full_ts_trade_db_location)

    shorts <-
      DBI::dbGetQuery(conn = full_ts_trade_db_con,
                      "SELECT * FROM full_ts_trades_mapped") %>%
      filter(trade_col == "Short") %>%
      filter(stop_factor == stop_factor_var,
             profit_factor == profit_factor_var) %>%
      rename(
        Date = dates,
        Asset = asset
      ) %>%
      convert_stop_profit_AUD(
        asset_infor = asset_infor,
        currency_conversion = currency_conversion,
        asset_col = "Asset",
        stop_col = "starting_stop_value",
        profit_col = "starting_profit_value",
        price_col = "trade_start_prices",
        risk_dollar_value = 10,
        returns_present = FALSE,
        trade_return_col = "trade_return") %>%
      mutate(
        trade_returns_orig = trade_returns,
        trade_returns =
          case_when(
            trade_returns_orig > 0 ~ maximum_win,
            trade_returns_orig <= 0 ~ -1*minimal_loss,
          )
      ) %>%
      dplyr::select(-trade_returns_orig)

    longs <-
      DBI::dbGetQuery(conn = full_ts_trade_db_con,
                      "SELECT * FROM full_ts_trades_mapped") %>%
      filter(trade_col == "Long") %>%
      filter(stop_factor == stop_factor_var,
             profit_factor == profit_factor_var) %>%
      rename(
        Date = dates,
        Asset = asset
      ) %>%
      convert_stop_profit_AUD(
        asset_infor = asset_infor,
        currency_conversion = currency_conversion,
        asset_col = "Asset",
        stop_col = "starting_stop_value",
        profit_col = "starting_profit_value",
        price_col = "trade_start_prices",
        risk_dollar_value = 10,
        returns_present = FALSE,
        trade_return_col = "trade_return") %>%
      mutate(
        trade_returns_orig = trade_returns,
        trade_returns =
          case_when(
            trade_returns_orig > 0 ~ maximum_win,
            trade_returns_orig <= 0 ~ -1*minimal_loss,
          )
      ) %>%
      dplyr::select(-trade_returns_orig)

    DBI::dbDisconnect(full_ts_trade_db_con)

    return(
      list(longs, shorts)
    )

  }

#' get_GLM_Indices_dat
#'
#' @param major_indices
#' @param lag_days
#' @param raw_macro_data
#' @param dependant_var_name
#' @param use_PCA_vars
#' @param date_split_train
#' @param train_sample
#' @param trade_direction_var
#' @param trade_direction_analysis
#' @param lagged_vars_only
#'
#' @return
#' @export
#'
#' @examples
get_GLM_Indices_dat <-
  function(
    major_indices = major_indices,
    lag_days = 1,
    raw_macro_data = get_macro_event_data(),
    use_PCA_vars = FALSE,
    train_sample = train_sample,
    lagged_vars_only = FALSE
  ) {


    aus_macro_data <-
      get_AUS_Indicators(raw_macro_data,
                         lag_days = lag_days,
                         first_difference = TRUE
      ) %>%
      janitor::clean_names()

    aus_macro_data_PCA <-
      aus_macro_data %>%
      dplyr::select(-date) %>%
      prcomp() %>%
      pluck("x") %>%
      as_tibble() %>%
      dplyr::select(AUD_PC1= PC1,
                    AUD_PC2= PC2,
                    AUD_PC3= PC3,
                    AUD_PC4= PC4,
                    AUD_PC5= PC5) %>%
      mutate(
        date = aus_macro_data %>% pull(date)
      )

    nzd_macro_data <-
      get_NZD_Indicators(raw_macro_data,
                         lag_days = lag_days,
                         first_difference = TRUE
      ) %>%
      janitor::clean_names()

    nzd_macro_data_PCA <-
      nzd_macro_data %>%
      dplyr::select(-date) %>%
      prcomp() %>%
      pluck("x") %>%
      as_tibble() %>%
      dplyr::select(NZD_PC1= PC1,
                    NZD_PC2= PC2,
                    NZD_PC3= PC3,
                    NZD_PC4= PC4,
                    NZD_PC5= PC5) %>%
      mutate(
        date = nzd_macro_data %>% pull(date)
      )

    usd_macro_data <-
      get_USD_Indicators(raw_macro_data,
                         lag_days = lag_days,
                         first_difference = TRUE
      ) %>%
      janitor::clean_names()

    usd_macro_data_PCA <-
      usd_macro_data %>%
      dplyr::select(-date) %>%
      prcomp() %>%
      pluck("x") %>%
      as_tibble() %>%
      dplyr::select(USD_PC1= PC1,
                    USD_PC2= PC2,
                    USD_PC3= PC3,
                    USD_PC4= PC4,
                    USD_PC5= PC5) %>%
      mutate(
        date = usd_macro_data %>% pull(date)
      )

    cny_macro_data <-
      get_CNY_Indicators(raw_macro_data,
                         lag_days = lag_days,
                         first_difference = TRUE
      ) %>%
      janitor::clean_names()

    cny_macro_data_PCA <-
      cny_macro_data %>%
      dplyr::select(-date) %>%
      prcomp() %>%
      pluck("x") %>%
      as_tibble() %>%
      dplyr::select(CNY_PC1= PC1,
                    CNY_PC2= PC2,
                    CNY_PC3= PC3,
                    CNY_PC4= PC4,
                    CNY_PC5= PC5) %>%
      mutate(
        date = cny_macro_data %>% pull(date)
      )

    eur_macro_data <-
      get_EUR_Indicators(raw_macro_data,
                         lag_days = lag_days,
                         first_difference = TRUE
      ) %>%
      janitor::clean_names()

    eur_macro_data_PCA <-
      eur_macro_data %>%
      dplyr::select(-date) %>%
      prcomp() %>%
      pluck("x") %>%
      as_tibble() %>%
      dplyr::select(EUR_PC1= PC1,
                    EUR_PC2= PC2,
                    EUR_PC3= PC3,
                    EUR_PC4= PC4,
                    EUR_PC5= PC5) %>%
      mutate(
        date = eur_macro_data %>% pull(date)
      )

    aud_macro_vars <- names(aus_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    nzd_macro_vars <- names(nzd_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    usd_macro_vars <- names(usd_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    cny_macro_vars <- names(cny_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    eur_macro_vars <- names(eur_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    all_macro_vars <- c(aud_macro_vars, nzd_macro_vars, usd_macro_vars, cny_macro_vars, eur_macro_vars)
    PC_macro_vars <-
      c("AUD_PC1", "AUD_PC2", "AUD_PC3", "AUD_PC4", "AUD_PC5",
        "NZD_PC1", "NZD_PC2", "NZD_PC3", "NZD_PC4", "NZD_PC5",
        "USD_PC1", "USD_PC2", "USD_PC3", "USD_PC4", "USD_PC5",
        "CNY_PC1", "CNY_PC2", "CNY_PC3", "CNY_PC4", "CNY_PC5",
        "EUR_PC1", "EUR_PC2", "EUR_PC3", "EUR_PC4", "EUR_PC5")

    copula_data_SPX_US2000 <-
      estimating_dual_copula(
        asset_data_to_use = major_indices,
        asset_to_use = c("SPX500_USD", "US2000_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      )

    copula_data_SPX_EU50 <-
      estimating_dual_copula(
        asset_data_to_use = major_indices,
        asset_to_use = c("SPX500_USD", "EU50_EUR"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-SPX500_USD, -SPX500_USD_log1_price, -SPX500_USD_quantiles_1,
                    -SPX500_USD_tangent_angle1)

    copula_data_SPX_AU200 <-
      estimating_dual_copula(
        asset_data_to_use = major_indices,
        asset_to_use = c("SPX500_USD", "AU200_AUD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-SPX500_USD, -SPX500_USD_log1_price, -SPX500_USD_quantiles_1,
                    -SPX500_USD_tangent_angle1)

    complete_data <-
      copula_data_SPX_US2000 %>%
      left_join(copula_data_SPX_EU50) %>%
      left_join(copula_data_SPX_AU200) %>%
      mutate(Date_for_join = as_date(Date)) %>%
      left_join(
        aus_macro_data %>%
          rename(Date_for_join = date)
      ) %>%
      left_join(
        nzd_macro_data %>%
          rename(Date_for_join = date)
      ) %>%
      left_join(
        usd_macro_data %>%
          rename(Date_for_join = date)
      ) %>%
      left_join(
        cny_macro_data %>%
          rename(Date_for_join = date)
      )  %>%
      left_join(
        eur_macro_data %>%
          rename(Date_for_join = date)
      ) %>%
      left_join(aus_macro_data_PCA %>%
                  rename(Date_for_join = date) )%>%
      left_join(usd_macro_data_PCA %>%
                  rename(Date_for_join = date) )%>%
      left_join(cny_macro_data_PCA %>%
                  rename(Date_for_join = date) )%>%
      left_join(eur_macro_data_PCA %>%
                  rename(Date_for_join = date) ) %>%
      left_join(nzd_macro_data_PCA %>%
                  rename(Date_for_join = date) ) %>%
      fill(!contains("SPX500_USD|Date"), .direction = "down") %>%
      filter(if_all(everything() ,.fns = ~ !is.na(.))) %>%
      mutate(hour_of_day = lubridate::hour(Date) %>% as.numeric(),
             day_of_week = lubridate::wday(Date) %>% as.numeric())

    max_date_in_data <-
      complete_data %>% pull(Date) %>% max(na.rm = T)

    message(glue::glue("Max Date in Indicies GLM data Stage 1: {max_date_in_data}"))


    lm_quant_vars <- names(complete_data) %>% keep(~ str_detect(.x,"quantiles|tangent|cor"))

    if(use_PCA_vars == TRUE) {
      lm_vars1 <- c(PC_macro_vars, lm_quant_vars,
                    "fib_1", "fib_2",
                    "fib_3", "fib_4",
                    "fib_5", "fib_6",
                    "lagged_var_1", "lagged_var_2",
                    "lagged_var_3", "lagged_var_5",
                    "lagged_var_8",
                    "lagged_var_13", "lagged_var_21",
                    "lagged_var_3_ma", "lagged_var_5_ma",
                    "lagged_var_8_ma", "lagged_var_13_ma",
                    "lagged_var_21_ma",
                    "hour_of_day", "day_of_week")
    } else {
      lm_vars1 <- c(all_macro_vars, lm_quant_vars,
                    "fib_1", "fib_2",
                    "fib_3", "fib_4",
                    "fib_5", "fib_6",
                    "lagged_var_1", "lagged_var_2",
                    "lagged_var_3", "lagged_var_5",
                    "lagged_var_8",
                    "lagged_var_13", "lagged_var_21",
                    "lagged_var_3_ma", "lagged_var_5_ma",
                    "lagged_var_8_ma", "lagged_var_13_ma",
                    "lagged_var_21_ma",
                    "hour_of_day", "day_of_week")
    }

    if(lagged_vars_only == TRUE) {
      lm_vars1 <- c(lm_quant_vars,
                    "fib_1", "fib_2",
                    "fib_3", "fib_4",
                    "fib_5", "fib_6",
                    "lagged_var_1", "lagged_var_2",
                    "lagged_var_3", "lagged_var_5",
                    "lagged_var_8",
                    "lagged_var_13", "lagged_var_21",
                    "lagged_var_3_ma", "lagged_var_5_ma",
                    "lagged_var_8_ma", "lagged_var_13_ma",
                    "lagged_var_21_ma")
    } else {
      lm_vars1 <- c(all_macro_vars, lm_quant_vars,
                    "fib_1", "fib_2",
                    "fib_3", "fib_4",
                    "fib_5", "fib_6",
                    "lagged_var_1", "lagged_var_2",
                    "lagged_var_3", "lagged_var_5",
                    "lagged_var_8",
                    "lagged_var_13", "lagged_var_21",
                    "lagged_var_3_ma", "lagged_var_5_ma",
                    "lagged_var_8_ma", "lagged_var_13_ma",
                    "lagged_var_21_ma",
                    "hour_of_day", "day_of_week")
    }

    return(
      list(complete_data,
           lm_vars1)
    )

  }

#' Title
#'
#' @param macro_joined_data
#' @param dependant_var_name
#' @param longs
#' @param shorts
#' @param trade_direction_var
#'
#' @return
#' @export
#'
#' @examples
get_Indcies_GLM_model <-
  function(
    macro_joined_data = GLM_indicie_data[[1]],
    dependant_var_name = "SPX500_USD",
    longs = all_trade_ts_actuals[[1]],
    shorts = all_trade_ts_actuals[[2]],
    trade_direction_var = "Long"
  ) {

    returns_data <-
      longs %>%
      dplyr::select(Asset,
                    Date,
                    minimal_loss,
                    maximum_win,
                    trade_col,
                    trade_start_prices,
                    trade_end_prices,
                    profit_factor,
                    stop_factor) %>%
      mutate(Date = as_datetime(Date)) %>%
      bind_rows(
        shorts  %>%
          dplyr::select(Asset,
                        Date,
                        minimal_loss,
                        maximum_win,
                        trade_col,
                        trade_start_prices,
                        trade_end_prices,
                        profit_factor,
                        stop_factor) %>%
          mutate(Date = as_datetime(Date))
      ) %>%
      mutate(
        bin_var =
          case_when(
            trade_start_prices > trade_end_prices & trade_col == "Short" ~ "win",
            trade_start_prices <= trade_end_prices & trade_col == "Short" ~ "loss",

            trade_start_prices < trade_end_prices & trade_col == "Long" ~ "win",
            trade_start_prices >= trade_end_prices & trade_col == "Long" ~ "loss"
          )
      ) %>%
      filter(Asset == dependant_var_name)%>%
      filter(trade_col == trade_direction_var)

    asset_filtered_data <-
      macro_joined_data %>%
      mutate(Asset = dependant_var_name) %>%
      left_join(returns_data) %>%
      filter(Asset == dependant_var_name) %>%
      mutate(
        lagged_var_1 = lag(!!as.name(dependant_var_name), 1),
        lagged_var_2 = lag(!!as.name(dependant_var_name), 2),
        lagged_var_3 = lag(!!as.name(dependant_var_name), 3),
        lagged_var_5 = lag(!!as.name(dependant_var_name), 5),
        lagged_var_8 = lag(!!as.name(dependant_var_name), 8),
        lagged_var_13 = lag(!!as.name(dependant_var_name), 13),
        lagged_var_21 = lag(!!as.name(dependant_var_name), 21),

        fib_1 = lagged_var_1 + lagged_var_2,
        fib_2 = lagged_var_2 + lagged_var_3,
        fib_3 = lagged_var_3 + lagged_var_5,
        fib_4 = lagged_var_5 + lagged_var_8,
        fib_5 = lagged_var_8 + lagged_var_13,
        fib_6 = lagged_var_13 + lagged_var_21,

        lagged_var_3_ma = slider::slide_dbl(.x = lagged_var_1,
                                            .f = ~ mean(.x, na.rm = T),
                                            .before = 3),

        lagged_var_5_ma = slider::slide_dbl(.x = lagged_var_1,
                                            .f = ~ mean(.x, na.rm = T),
                                            .before = 5),

        lagged_var_8_ma = slider::slide_dbl(.x = lagged_var_1,
                                            .f = ~ mean(.x, na.rm = T),
                                            .before = 8),

        lagged_var_13_ma = slider::slide_dbl(.x = lagged_var_1,
                                             .f = ~ mean(.x, na.rm = T),
                                             .before = 13),

        lagged_var_21_ma = slider::slide_dbl(.x = lagged_var_1,
                                             .f = ~ mean(.x, na.rm = T),
                                             .before = 21)
      )

    max_date_in_data <-
      asset_filtered_data %>% pull(Date) %>% max(na.rm = T)

    message(glue::glue("Max Date in Indicies GLM data Stage 2: {max_date_in_data}"))

    return(asset_filtered_data)

  }

#' Title
#'
#' @param preapred_Index_data
#' @param lm_vars1
#'
#' @return
#' @export
#'
#' @examples
get_Index_GLM_trades <-
  function(
    preapred_Index_data = SPX_Index_data,
    lm_vars1 = lm_vars1,
    get_max_dates_data = TRUE,
    date_split_train = "2024-10-01",
    train_sample = 10000,
    trade_direction_var = "Long"
  ) {

    GLM_form <-  create_lm_formula(dependant = "bin_var=='win'", independant = lm_vars1)

    training_data <-
      preapred_Index_data %>%
      filter(Date < date_split_train) %>%
      filter(trade_col == trade_direction_var) %>%
      slice_sample(n = train_sample) %>%
      filter(if_all(everything() ,.fns = ~ !is.na(.)))

    testing_data <-
      preapred_Index_data %>%
      filter(Date > date_split_train)

    GLM_model <-
      glm(formula = GLM_form, family = binomial("logit"), data = training_data)
    summary(GLM_model)

    preds_glm <-
      predict.glm(object = GLM_model, newdata = testing_data, type = "response")

    testing_data_with_pred <-
      testing_data %>%
      mutate(
        pred = preds_glm
      )

    max_date_in_testing <- testing_data_with_pred %>% pull(Date) %>% max(na.rm = T)
    message(glue::glue("Max Date in Testing Data: {max_date_in_testing}"))


    if(get_max_dates_data == TRUE) {
      testing_data_with_pred <-
        testing_data_with_pred %>%
        slice_max(Date)
    }

    return(
      list(
        "tagged_trades" = testing_data_with_pred,
        "glm_model" = GLM_model
      )
    )

  }

#' Title
#'
#' @param testing_data
#'
#' @return
#' @export
#'
#' @examples
get_Index_GLM_analysis_only <- function(
    testing_data = SPX_trades_GLM,
    longs,
    shorts,
    dependant_var_name,
    trade_direction_var,
    trade_direction_analysis,
    analysis_threshs = c(0.5,0.6,0.7,0.8,0.90,0.95),
    GLM_model,
    lm_vars = GLM_index_data[[2]]
) {

  returns_data <-
    longs %>%
    dplyr::select(Asset,
                  Date,
                  minimal_loss,
                  maximum_win,
                  trade_col,
                  trade_start_prices,
                  trade_end_prices,
                  profit_factor,
                  stop_factor) %>%
    mutate(Date = as_datetime(Date)) %>%
    bind_rows(
      shorts  %>%
        dplyr::select(Asset,
                      Date,
                      minimal_loss,
                      maximum_win,
                      trade_col,
                      trade_start_prices,
                      trade_end_prices,
                      profit_factor,
                      stop_factor) %>%
        mutate(Date = as_datetime(Date))
    ) %>%
    mutate(
      bin_var =
        case_when(
          trade_start_prices > trade_end_prices & trade_col == "Short" ~ "win",
          trade_start_prices <= trade_end_prices & trade_col == "Short" ~ "loss",

          trade_start_prices < trade_end_prices & trade_col == "Long" ~ "win",
          trade_start_prices >= trade_end_prices & trade_col == "Long" ~ "loss"
        )
    ) %>%
    filter(Asset == dependant_var_name)%>%
    filter(trade_col == trade_direction_var)

  needed_analysis_bins <-
    returns_data %>%
    dplyr::select(Date, trade_col, bin_var, Asset) %>%
    dplyr::filter(Asset == dependant_var_name)%>%
    dplyr::filter(trade_col == trade_direction_analysis)

  testing_data <- testing_data %>%
    dplyr::select(Date,Asset, profit_factor, stop_factor,
                  trade_col, bin_var, maximum_win, minimal_loss,
                  matches(lm_vars, ignore.case = FALSE)
    )

  preds_glm <-
    predict.glm(object = GLM_model, newdata = testing_data, type = "response")

  testing_data_with_pred <-
    testing_data %>%
    mutate(
      pred = preds_glm
    ) %>%
    dplyr::select(-bin_var, -trade_col) %>%
    left_join(needed_analysis_bins)

  analysis_control <-
    testing_data_with_pred %>%
    ungroup() %>%
    filter(Asset == dependant_var_name) %>%
    filter(trade_col == trade_direction_analysis) %>%
    group_by( bin_var) %>%
    summarise(
      wins_losses = n(),
    ) %>%
    ungroup() %>%
    mutate(
      Total_control = sum(wins_losses),
      Perc_control = wins_losses/Total_control
    )  %>%
    dplyr::select( -wins_losses)

  analysis_list <- list()

  for (i in 1:length(analysis_threshs) ) {

    temp_trades  <-
      testing_data_with_pred %>%
      filter(Asset == dependant_var_name) %>%
      filter(trade_col == trade_direction_analysis) %>%
      dplyr::select(-trade_col) %>%
      mutate(
        trade_col = case_when(pred >= analysis_threshs[i] ~ trade_direction_analysis,
                              TRUE ~ "No Trade")
      ) %>%
      ungroup()

    analysis_list[[i]] <-
      temp_trades %>%
      group_by(trade_col, bin_var) %>%
      summarise(
        wins_losses = n(),
        win_amount = max(maximum_win),
        loss_amount = min(minimal_loss),
      ) %>%
      group_by(trade_col) %>%
      mutate(
        Total = sum(wins_losses),
        Perc = wins_losses/Total,
        returns =
          case_when(
            bin_var == "win" ~ wins_losses*win_amount,
            bin_var == "loss" ~ wins_losses*loss_amount
          )
      ) %>%
      filter(trade_col != "No Trade") %>%
      mutate(
        risk_weighted_return =
          (win_amount/abs(loss_amount) )*Perc - (1- Perc)
      ) %>%
      left_join(analysis_control) %>%
      mutate(
        control_risk_return =
          (win_amount/abs(loss_amount) )*Perc_control - (1- Perc_control)
      ) %>%
      filter(bin_var == "win") %>%
      dplyr::select(-bin_var) %>%
      mutate(Asset = dependant_var_name,
             threshold = analysis_threshs[i]) %>%
      dplyr::select(
        trade_col, Asset, wins_losses, win_amount, loss_amount, Trades = Total, Perc,
        returns, risk_weighted_return, Total_control, Perc_control, control_risk_return,
        threshold
      )
  }

  analysis <-
    analysis_list %>%
    map_dfr(bind_rows)

  return(analysis)

}

#' get_Indices_GLM_analysis
#'
#' @param major_indices
#' @param longs
#' @param shorts
#' @param lag_days
#' @param raw_macro_data
#' @param dependant_var_name
#' @param use_PCA_vars
#' @param date_split_train
#' @param train_sample
#' @param trade_direction_var
#' @param trade_direction_analysis
#' @param lagged_vars_only
#' @param analysis_threshs
#'
#' @return
#' @export
#'
#' @examples
get_Indices_GLM_analysis <-
  function(
    major_indices = major_indices,
    longs = all_trade_ts_actuals[[1]],
    shorts = all_trade_ts_actuals[[2]],
    lag_days = 1,
    raw_macro_data = get_macro_event_data(),
    dependant_var_name = "SPX500_USD",
    use_PCA_vars = FALSE,
    date_split_train = "2024-06-01",
    train_sample = train_sample,
    trade_direction_var = "Long",
    trade_direction_analysis = "Long",
    lagged_vars_only = FALSE,
    analysis_threshs = c(0.6, 0.7, 0.8, 0.9, 0.95),
    split_padding = 30,
    max_date_in_analysis = "2025-09-10"
  ) {

    returns_data <-
      longs %>%
      dplyr::select(Asset,
                    Date,
                    minimal_loss,
                    maximum_win,
                    trade_col,
                    trade_start_prices,
                    trade_end_prices,
                    profit_factor,
                    stop_factor) %>%
      mutate(Date = as_datetime(Date)) %>%
      bind_rows(
        shorts  %>%
          dplyr::select(Asset,
                        Date,
                        minimal_loss,
                        maximum_win,
                        trade_col,
                        trade_start_prices,
                        trade_end_prices,
                        profit_factor,
                        stop_factor) %>%
          mutate(Date = as_datetime(Date))
      ) %>%
      mutate(
        bin_var =
          case_when(
            trade_start_prices > trade_end_prices & trade_col == "Short" ~ "win",
            trade_start_prices <= trade_end_prices & trade_col == "Short" ~ "loss",

            trade_start_prices < trade_end_prices & trade_col == "Long" ~ "win",
            trade_start_prices >= trade_end_prices & trade_col == "Long" ~ "loss"
          )
      ) %>%
      filter(Asset == dependant_var_name)

    aus_macro_data <-
      get_AUS_Indicators(raw_macro_data,
                         lag_days = lag_days,
                         first_difference = TRUE
      ) %>%
      janitor::clean_names()

    aus_macro_data_PCA <-
      aus_macro_data %>%
      dplyr::select(-date) %>%
      prcomp() %>%
      pluck("x") %>%
      as_tibble() %>%
      dplyr::select(AUD_PC1= PC1,
                    AUD_PC2= PC2,
                    AUD_PC3= PC3,
                    AUD_PC4= PC4,
                    AUD_PC5= PC5) %>%
      mutate(
        date = aus_macro_data %>% pull(date)
      )

    nzd_macro_data <-
      get_NZD_Indicators(raw_macro_data,
                         lag_days = lag_days,
                         first_difference = TRUE
      ) %>%
      janitor::clean_names()

    nzd_macro_data_PCA <-
      nzd_macro_data %>%
      dplyr::select(-date) %>%
      prcomp() %>%
      pluck("x") %>%
      as_tibble() %>%
      dplyr::select(NZD_PC1= PC1,
                    NZD_PC2= PC2,
                    NZD_PC3= PC3,
                    NZD_PC4= PC4,
                    NZD_PC5= PC5) %>%
      mutate(
        date = nzd_macro_data %>% pull(date)
      )

    usd_macro_data <-
      get_USD_Indicators(raw_macro_data,
                         lag_days = lag_days,
                         first_difference = TRUE
      ) %>%
      janitor::clean_names()

    usd_macro_data_PCA <-
      usd_macro_data %>%
      dplyr::select(-date) %>%
      prcomp() %>%
      pluck("x") %>%
      as_tibble() %>%
      dplyr::select(USD_PC1= PC1,
                    USD_PC2= PC2,
                    USD_PC3= PC3,
                    USD_PC4= PC4,
                    USD_PC5= PC5) %>%
      mutate(
        date = usd_macro_data %>% pull(date)
      )

    cny_macro_data <-
      get_CNY_Indicators(raw_macro_data,
                         lag_days = lag_days,
                         first_difference = TRUE
      ) %>%
      janitor::clean_names()

    cny_macro_data_PCA <-
      cny_macro_data %>%
      dplyr::select(-date) %>%
      prcomp() %>%
      pluck("x") %>%
      as_tibble() %>%
      dplyr::select(CNY_PC1= PC1,
                    CNY_PC2= PC2,
                    CNY_PC3= PC3,
                    CNY_PC4= PC4,
                    CNY_PC5= PC5) %>%
      mutate(
        date = cny_macro_data %>% pull(date)
      )

    eur_macro_data <-
      get_EUR_Indicators(raw_macro_data,
                         lag_days = lag_days,
                         first_difference = TRUE
      ) %>%
      janitor::clean_names()

    eur_macro_data_PCA <-
      eur_macro_data %>%
      dplyr::select(-date) %>%
      prcomp() %>%
      pluck("x") %>%
      as_tibble() %>%
      dplyr::select(EUR_PC1= PC1,
                    EUR_PC2= PC2,
                    EUR_PC3= PC3,
                    EUR_PC4= PC4,
                    EUR_PC5= PC5) %>%
      mutate(
        date = eur_macro_data %>% pull(date)
      )

    aud_macro_vars <- names(aus_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    nzd_macro_vars <- names(nzd_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    usd_macro_vars <- names(usd_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    cny_macro_vars <- names(cny_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    eur_macro_vars <- names(eur_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    all_macro_vars <- c(aud_macro_vars, nzd_macro_vars, usd_macro_vars, cny_macro_vars, eur_macro_vars)
    PC_macro_vars <-
      c("AUD_PC1", "AUD_PC2", "AUD_PC3", "AUD_PC4", "AUD_PC5",
        "NZD_PC1", "NZD_PC2", "NZD_PC3", "NZD_PC4", "NZD_PC5",
        "USD_PC1", "USD_PC2", "USD_PC3", "USD_PC4", "USD_PC5",
        "CNY_PC1", "CNY_PC2", "CNY_PC3", "CNY_PC4", "CNY_PC5",
        "EUR_PC1", "EUR_PC2", "EUR_PC3", "EUR_PC4", "EUR_PC5")

    copula_data_SPX_US2000 <-
      estimating_dual_copula(
        asset_data_to_use = major_indices,
        asset_to_use = c("SPX500_USD", "US2000_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      )

    copula_data_SPX_EU50 <-
      estimating_dual_copula(
        asset_data_to_use = major_indices,
        asset_to_use = c("SPX500_USD", "EU50_EUR"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-SPX500_USD, -SPX500_USD_log1_price, -SPX500_USD_quantiles_1,
                    -SPX500_USD_tangent_angle1)

    copula_data_SPX_AU200 <-
      estimating_dual_copula(
        asset_data_to_use = major_indices,
        asset_to_use = c("SPX500_USD", "AU200_AUD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      ) %>%
      dplyr::select(-SPX500_USD, -SPX500_USD_log1_price, -SPX500_USD_quantiles_1,
                    -SPX500_USD_tangent_angle1)

    complete_data <-
      copula_data_SPX_US2000 %>%
      left_join(copula_data_SPX_EU50) %>%
      left_join(copula_data_SPX_AU200) %>%
      mutate(Date_for_join = as_date(Date)) %>%
      left_join(
        aus_macro_data %>%
          rename(Date_for_join = date)
      ) %>%
      left_join(
        nzd_macro_data %>%
          rename(Date_for_join = date)
      ) %>%
      left_join(
        usd_macro_data %>%
          rename(Date_for_join = date)
      ) %>%
      left_join(
        cny_macro_data %>%
          rename(Date_for_join = date)
      )  %>%
      left_join(
        eur_macro_data %>%
          rename(Date_for_join = date)
      ) %>%
      left_join(aus_macro_data_PCA %>%
                  rename(Date_for_join = date) )%>%
      left_join(usd_macro_data_PCA %>%
                  rename(Date_for_join = date) )%>%
      left_join(cny_macro_data_PCA %>%
                  rename(Date_for_join = date) )%>%
      left_join(eur_macro_data_PCA %>%
                  rename(Date_for_join = date) ) %>%
      left_join(nzd_macro_data_PCA %>%
                  rename(Date_for_join = date) ) %>%
      fill(!contains("SPX500_USD|Date"), .direction = "down") %>%
      filter(if_all(everything() ,.fns = ~ !is.na(.))) %>%
      left_join(returns_data %>%
                  filter(trade_col == trade_direction_var) ) %>%
      # mutate(
      #   bin_var = lead(bin_var)
      # ) %>%
      filter(!is.na(bin_var)) %>%
      mutate(hour_of_day = lubridate::hour(Date) %>% as.numeric(),
             day_of_week = lubridate::wday(Date) %>% as.numeric()) %>%
      filter(trade_col == trade_direction_var)

    asset_filtered_data <- complete_data %>%
      filter(Asset == dependant_var_name) %>%
      filter(trade_col == trade_direction_var) %>%
      mutate(
        lagged_var_1 = lag(!!as.name(dependant_var_name), 1),
        lagged_var_2 = lag(!!as.name(dependant_var_name), 2),
        lagged_var_3 = lag(!!as.name(dependant_var_name), 3),
        lagged_var_5 = lag(!!as.name(dependant_var_name), 5),
        lagged_var_8 = lag(!!as.name(dependant_var_name), 8),
        lagged_var_13 = lag(!!as.name(dependant_var_name), 13),
        lagged_var_21 = lag(!!as.name(dependant_var_name), 21),

        fib_1 = lagged_var_1 + lagged_var_2,
        fib_2 = lagged_var_2 + lagged_var_3,
        fib_3 = lagged_var_3 + lagged_var_5,
        fib_4 = lagged_var_5 + lagged_var_8,
        fib_5 = lagged_var_8 + lagged_var_13,
        fib_6 = lagged_var_13 + lagged_var_21,

        lagged_var_3_ma = slider::slide_dbl(.x = lagged_var_1,
                                            .f = ~ mean(.x, na.rm = T),
                                            .before = 3),

        lagged_var_5_ma = slider::slide_dbl(.x = lagged_var_1,
                                            .f = ~ mean(.x, na.rm = T),
                                            .before = 5),

        lagged_var_8_ma = slider::slide_dbl(.x = lagged_var_1,
                                            .f = ~ mean(.x, na.rm = T),
                                            .before = 8),

        lagged_var_13_ma = slider::slide_dbl(.x = lagged_var_1,
                                             .f = ~ mean(.x, na.rm = T),
                                             .before = 13),

        lagged_var_21_ma = slider::slide_dbl(.x = lagged_var_1,
                                             .f = ~ mean(.x, na.rm = T),
                                             .before = 21)
      ) %>%
      group_by(Asset) %>%
      arrange(Date, .by_group = TRUE) %>%
      group_by(Asset) %>%
      fill(everything(), .direction = "down") %>%
      ungroup() %>%
      filter(if_all(everything() ,.fns = ~ !is.na(.)))

    lm_quant_vars <- names(asset_filtered_data) %>% keep(~ str_detect(.x,"quantiles|tangent|cor"))

    if(use_PCA_vars == TRUE) {
      lm_vars1 <- c(PC_macro_vars, lm_quant_vars,
                    "fib_1", "fib_2",
                    "fib_3", "fib_4",
                    "fib_5", "fib_6",
                    "lagged_var_1", "lagged_var_2",
                    "lagged_var_3", "lagged_var_5",
                    "lagged_var_8",
                    "lagged_var_13", "lagged_var_21",
                    "lagged_var_3_ma", "lagged_var_5_ma",
                    "lagged_var_8_ma", "lagged_var_13_ma",
                    "lagged_var_21_ma",
                    "hour_of_day", "day_of_week")
    } else {
      lm_vars1 <- c(all_macro_vars, lm_quant_vars,
                    "fib_1", "fib_2",
                    "fib_3", "fib_4",
                    "fib_5", "fib_6",
                    "lagged_var_1", "lagged_var_2",
                    "lagged_var_3", "lagged_var_5",
                    "lagged_var_8",
                    "lagged_var_13", "lagged_var_21",
                    "lagged_var_3_ma", "lagged_var_5_ma",
                    "lagged_var_8_ma", "lagged_var_13_ma",
                    "lagged_var_21_ma",
                    "hour_of_day", "day_of_week")
    }

    if(lagged_vars_only == TRUE) {
      lm_vars1 <- c(lm_quant_vars,
                    "fib_1", "fib_2",
                    "fib_3", "fib_4",
                    "fib_5", "fib_6",
                    "lagged_var_1", "lagged_var_2",
                    "lagged_var_3", "lagged_var_5",
                    "lagged_var_8",
                    "lagged_var_13", "lagged_var_21",
                    "lagged_var_3_ma", "lagged_var_5_ma",
                    "lagged_var_8_ma", "lagged_var_13_ma",
                    "lagged_var_21_ma")
    } else {
      lm_vars1 <- c(all_macro_vars, lm_quant_vars,
                    "fib_1", "fib_2",
                    "fib_3", "fib_4",
                    "fib_5", "fib_6",
                    "lagged_var_1", "lagged_var_2",
                    "lagged_var_3", "lagged_var_5",
                    "lagged_var_8",
                    "lagged_var_13", "lagged_var_21",
                    "lagged_var_3_ma", "lagged_var_5_ma",
                    "lagged_var_8_ma", "lagged_var_13_ma",
                    "lagged_var_21_ma",
                    "hour_of_day", "day_of_week")
    }

    GLM_form <-  create_lm_formula(dependant = "bin_var=='win'", independant = lm_vars1)

    training_data <-
      asset_filtered_data %>%
      filter(Date < date_split_train) %>%
      slice_sample(n = train_sample)

    testing_data <-
      asset_filtered_data %>%
      filter(Date > ( as_date(date_split_train) + days(split_padding)) )

    GLM_model <-
      glm(formula = GLM_form, family = binomial("logit"), data = training_data)
    summary(GLM_model)

    preds_glm <-
      predict.glm(object = GLM_model, newdata = testing_data, type = "response")

    needed_analysis_bins <-
      returns_data %>%
      dplyr::select(Date, trade_col, bin_var, Asset) %>%
      dplyr::filter(Asset == dependant_var_name)%>%
      dplyr::filter(trade_col == trade_direction_analysis)

    testing_data_with_pred <-
      testing_data %>%
      mutate(
        pred = preds_glm
      ) %>%
      dplyr::select(-bin_var, -trade_col) %>%
      left_join(needed_analysis_bins) %>%
      filter(Date <= as_date(max_date_in_analysis))

    analysis_control <-
      testing_data_with_pred %>%
      ungroup() %>%
      filter(Asset == dependant_var_name) %>%
      filter(trade_col == trade_direction_analysis) %>%
      group_by( bin_var) %>%
      summarise(
        wins_losses = n(),
      ) %>%
      ungroup() %>%
      mutate(
        Total_control = sum(wins_losses),
        Perc_control = wins_losses/Total_control
      )  %>%
      dplyr::select( -wins_losses)

    analysis_list <- list()

    for (i in 1:length(analysis_threshs) ) {

      temp_trades  <-
        testing_data_with_pred %>%
        filter(Asset == dependant_var_name) %>%
        filter(trade_col == trade_direction_analysis) %>%
        dplyr::select(-trade_col) %>%
        mutate(
          trade_col = case_when(pred >= analysis_threshs[i] ~ trade_direction_analysis,
                                TRUE ~ "No Trade")
        ) %>%
        ungroup()

      analysis_list[[i]] <-
        temp_trades %>%
        group_by(trade_col, bin_var) %>%
        summarise(
          wins_losses = n(),
          win_amount = max(maximum_win),
          loss_amount = min(minimal_loss),
        ) %>%
        group_by(trade_col) %>%
        mutate(
          Total = sum(wins_losses),
          Perc = wins_losses/Total,
          returns =
            case_when(
              bin_var == "win" ~ wins_losses*win_amount,
              bin_var == "loss" ~ wins_losses*loss_amount
            )
        ) %>%
        filter(trade_col != "No Trade") %>%
        mutate(
          risk_weighted_return =
            (win_amount/abs(loss_amount) )*Perc - (1- Perc)
        ) %>%
        left_join(analysis_control) %>%
        mutate(
          control_risk_return =
            (win_amount/abs(loss_amount) )*Perc_control - (1- Perc_control)
        ) %>%
        filter(bin_var == "win") %>%
        dplyr::select(-bin_var) %>%
        mutate(Asset = dependant_var_name,
               threshold = analysis_threshs[i]) %>%
        dplyr::select(
          trade_col, Asset, wins_losses, win_amount, loss_amount, Trades = Total, Perc,
          returns, risk_weighted_return, Total_control, Perc_control, control_risk_return,
          threshold
        )
    }

    analysis <-
      analysis_list %>%
      map_dfr(bind_rows)

    return(analysis)


  }


#' get_all_GLM_index_trades
#'
#' @param major_indices
#' @param major_indices_bid
#' @param raw_macro_data
#' @param all_trade_ts_actuals
#' @param train_sample
#' @param date_split_train
#' @param US2000_thresh
#' @param SPX500_thresh
#' @param EU50_thresh
#'
#' @return
#' @export
#'
#' @examples
get_all_GLM_index_trades <-
  function(
    major_indices = major_indices,
    major_indices_bid = major_indices_bid,
    raw_macro_data = raw_macro_data,
    all_trade_ts_actuals = all_trade_ts_actuals,
    train_sample = 1000000,
    date_split_train ="2025-07-31",
    US2000_thresh = 0.8,
    SPX500_thresh = 0.8,
    EU50_thresh = 0.9
  ) {

    GLM_index_data <-
      get_GLM_Indices_dat(
        major_indices = major_indices,
        lag_days = 1,
        raw_macro_data = raw_macro_data,
        use_PCA_vars = FALSE,
        train_sample = train_sample,
        lagged_vars_only = FALSE
      )

    #----------------------------SPX

    SPX_Index_data <-
      get_Indcies_GLM_model(
        macro_joined_data = GLM_index_data[[1]],
        dependant_var_name = "SPX500_USD",
        longs = all_trade_ts_actuals[[1]],
        shorts = all_trade_ts_actuals[[2]],
        trade_direction_var = "Long"
      )

    SPX_trades_GLM <-
      get_Index_GLM_trades(
        preapred_Index_data = SPX_Index_data,
        lm_vars1 = GLM_index_data[[2]],
        get_max_dates_data = FALSE,
        date_split_train = date_split_train,
        train_sample = train_sample,
        trade_direction_var = "Long"
      )

    SPX_tagged_trades <-
      SPX_trades_GLM %>%
      pluck(1) %>%
      dplyr::select(Date, Asset, stop_factor, profit_factor, pred, trade_col) %>%
      left_join(major_indices %>% dplyr::select(Date, Asset, Price, Open, High, Low)) %>%
      filter(pred >= SPX500_thresh) %>%
      slice_max(Date) %>%
      filter(Asset == "SPX500_USD")
    # filter(Date <= (now() - dhours(1)) )

    #----------------------------US2000

    US2000_Index_data <-
      get_Indcies_GLM_model(
        macro_joined_data = GLM_index_data[[1]],
        dependant_var_name = "US2000_USD",
        longs = all_trade_ts_actuals[[1]],
        shorts = all_trade_ts_actuals[[2]],
        trade_direction_var = "Long"
      )

    US2000_trades_GLM <-
      get_Index_GLM_trades(
        preapred_Index_data = US2000_Index_data,
        lm_vars1 = GLM_index_data[[2]],
        get_max_dates_data = FALSE,
        date_split_train = date_split_train,
        train_sample = train_sample,
        trade_direction_var = "Long"
      )

    US2000_tagged_trades <-
      US2000_trades_GLM %>%
      pluck(1) %>%
      dplyr::select(Date, Asset, stop_factor, profit_factor, pred, trade_col) %>%
      left_join(major_indices %>% dplyr::select(Date, Asset, Price, Open, High, Low)) %>%
      filter(pred >= US2000_thresh) %>%
      slice_max(Date) %>%
      filter(Asset == "US2000_USD")

    #----------------------------EU50

    EU50_Index_data <-
      get_Indcies_GLM_model(
        macro_joined_data = GLM_index_data[[1]],
        dependant_var_name = "EU50_EUR",
        longs = all_trade_ts_actuals[[1]],
        shorts = all_trade_ts_actuals[[2]],
        trade_direction_var = "Long"
      )

    EU50_trades_GLM <-
      get_Index_GLM_trades(
        preapred_Index_data = EU50_Index_data,
        lm_vars1 = GLM_index_data[[2]],
        get_max_dates_data = FALSE,
        date_split_train = date_split_train,
        train_sample = train_sample,
        trade_direction_var = "Long"
      )

    EU50_tagged_trades <-
      EU50_trades_GLM %>%
      pluck(1) %>%
      dplyr::select(Date, Asset, stop_factor, profit_factor, pred, trade_col) %>%
      left_join(major_indices %>% dplyr::select(Date, Asset, Price, Open, High, Low)) %>%
      filter(pred >= EU50_thresh) %>%
      slice_max(Date) %>%
      filter(Asset == "EU50_EUR")

    all_trades <-
      SPX_tagged_trades %>%
      bind_rows(US2000_tagged_trades) %>%
      bind_rows(EU50_tagged_trades)

    return(all_trades)

  }

