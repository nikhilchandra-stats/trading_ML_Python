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
          "C:/Users/nikhi/Documents/Repos/macrodatasetsraw/data/trade_results_desktop_algos.csv",
          row.names = FALSE)
