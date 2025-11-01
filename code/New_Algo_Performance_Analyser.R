helpeR::load_custom_functions()

analyse_new_algos(
  trade_tracker_DB_path = "C:/Users/nikhi/Documents/trade_data/trade_tracker_daily_buy_close.db",
  realised_DB_path = "C:/Users/nikhi/Documents/trade_data/trade_tracker_realised.db",
  algo_start_date = "2025-10-13"
)

analyse_new_algos(
  trade_tracker_DB_path = "C:/Users/nikhi/Documents/trade_data/trade_tracker_daily_buy_close 2.db",
  realised_DB_path = "C:/Users/nikhi/Documents/trade_data/trade_tracker_realised.db",
  algo_start_date = "2025-10-13"
)

newest_results <-
  get_current_new_algo_trades(realised_DB_path ="C:/Users/nikhi/Documents/trade_data/trade_tracker_realised.db")

newest_results_sum <-
  newest_results %>%
  group_by(id, Asset, account_var, initialUnits) %>%
  mutate(kk = row_number()) %>%
  slice_min(kk) %>%
  ungroup() %>%
  dplyr::select(-kk) %>%
  mutate(
    across(.cols = c(initialUnits, dividendAdjustment, financing),
           .fns = ~ as.numeric(.))
  ) %>%
  mutate(Net_Profit = realizedPL + dividendAdjustment + financing)

write.csv(newest_results_sum,
          "C:/Users/nikhi/Documents/Repos/macrodatasetsraw/data/trade_results_msi_pc_new_algos.csv",
          row.names = FALSE)

get_realised_trades_from_repo <-
  function(filt_date = "2025-10-20") {

    d1 <- read_csv("https://raw.githubusercontent.com/nikhilchandra-stats/macrodatasetsraw/refs/heads/master/data/trade_results_desktop_algos.csv")
    d2 <- read_csv("https://raw.githubusercontent.com/nikhilchandra-stats/macrodatasetsraw/refs/heads/master/data/trade_results_msi_pc_new_algos.csv")

    d1_results <-
      d1 %>%
      group_by(id, Asset, account_var, initialUnits) %>%
      mutate(kk = row_number()) %>%
      slice_min(kk) %>%
      ungroup() %>%
      dplyr::select(-kk) %>%
      filter(date_open >= filt_date) %>%
      pull(Net_Profit) %>%
      sum()

    d2_results <-
      d2 %>%
      group_by(id, Asset, account_var, initialUnits) %>%
      mutate(kk = row_number()) %>%
      slice_min(kk) %>%
      ungroup() %>%
      dplyr::select(-kk) %>%
      filter(date_open >= filt_date) %>%
      pull(Net_Profit) %>%
      sum()

    combined_results <-
      d1 %>%
      bind_rows(d2) %>%
      group_by(id, Asset, account_var, initialUnits) %>%
      mutate(kk = row_number()) %>%
      slice_min(kk) %>%
      ungroup() %>%
      dplyr::select(-kk) %>%
      filter(date_open >= filt_date) %>%
      arrange(date_closed) %>%
      mutate(cumulative_return = cumsum(Net_Profit))

    total_return <-
      combined_results$Net_Profit %>% sum()

    combined_results %>%
      ggplot(aes(x = date_closed, y = cumulative_return)) +
      geom_line() +
      theme_minimal()

    }
