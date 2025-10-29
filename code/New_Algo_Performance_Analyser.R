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
  filter(date_closed >= "2025-10-20") %>%
  mutate(
    across(.cols = c(initialUnits, dividendAdjustment, financing),
           .fns = ~ as.numeric(.))
  ) %>%
  mutate(Net_Profit = realizedPL + dividendAdjustment + financing)

newest_results_sum2 <- newest_results_sum %>%
  group_by(date_closed) %>%
  summarise(Net_Profit = sum(Net_Profit)) %>%
  ungroup() %>%
  arrange(date_closed) %>%
  mutate(
    cumulative_return = cumsum(Net_Profit)
  )

newest_results_sum2
