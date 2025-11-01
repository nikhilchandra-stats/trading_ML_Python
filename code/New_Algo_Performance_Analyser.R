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
          "C:/Users/nikhi/Documents/Repos/macrodatasetsraw/data/trade_results_desktop_algos.csv",
          row.names = FALSE)

trade_tracker_DB_path <-
  "C:/Users/nikhi/Documents/trade_data/trade_tracker_daily_buy_close 2.db"
trade_tracker_DB <- connect_db(trade_tracker_DB_path)
trades_from_DB <-
  DBI::dbGetQuery(conn = trade_tracker_DB,
                  statement = "SELECT * FROM trade_tracker")
DBI::dbDisconnect(trade_tracker_DB)

trade_tracker_DB_path <-
  "C:/Users/nikhi/Documents/trade_data/trade_tracker_daily_buy_close.db"
trade_tracker_DB <- connect_db(trade_tracker_DB_path)
trades_from_DB2 <-
  DBI::dbGetQuery(conn = trade_tracker_DB,
                  statement = "SELECT * FROM trade_tracker")
DBI::dbDisconnect(trade_tracker_DB)


all_this_pc_trades <-
  trades_from_DB %>%
  bind_rows(trades_from_DB2) %>%
  group_by(tradeID, Asset, units, account_var) %>%
  mutate(kk = row_number()) %>%
  group_by(tradeID, Asset, units, account_var) %>%
  slice_min(kk) %>%
  ungroup() %>%
  dplyr::select(tradeID, Asset) %>%
  mutate(in_this_db = TRUE)

newest_results_sum2 <- newest_results_sum %>%
  left_join(all_this_pc_trades, by = c("id" = "tradeID", "Asset")) %>%
  filter(in_this_db == TRUE) %>%
  filter(date_open >= "2025-10-27") %>%
  mutate(
    cumulative_return = cumsum(Net_Profit)
  )

newest_results_sum2$financing %>% sum()

