helpeR::load_custom_functions()

analyse_new_algos(
  trade_tracker_DB_path = "D:/trade_data/trade_tracker_daily_buy_close.db",
  realised_DB_path = "D:/trade_data/trade_tracker_realised.db",
  algo_start_date = "2025-10-13"
)

analyse_new_algos(
  trade_tracker_DB_path = "D:/trade_data/trade_tracker_daily_buy_close 2.db",
  realised_DB_path = "D:/trade_data/trade_tracker_realised.db",
  algo_start_date = "2025-10-13"
)

newest_results <-
  get_current_new_algo_trades(realised_DB_path ="D:/trade_data/trade_tracker_realised.db")

trade_tracker_DB <- connect_db("D:/trade_data/trade_tracker_daily_buy_close.db")
all_trades_so_far <-
  DBI::dbGetQuery(conn = trade_tracker_DB,
                  "SELECT * FROM trade_tracker")
DBI::dbDisconnect(trade_tracker_DB)
gc()

trade_tracker_DB <- connect_db("D:/trade_data/trade_tracker_daily_buy_close 2.db")
all_trades_so_far2 <-
  DBI::dbGetQuery(conn = trade_tracker_DB,
                  "SELECT * FROM trade_tracker")
DBI::dbDisconnect(trade_tracker_DB)
gc()

all_trades_so_far_comb <-
  all_trades_so_far %>%
  bind_rows(all_trades_so_far2) %>%
  distinct() %>%
  filter()

distinct_assets <-
  all_trades_so_far_comb %>%
  distinct(Asset, account_var, trade_col, tradeID) %>%
  rename(id = tradeID) %>%
  mutate(inLocalDB = TRUE)

newest_results_sum <-
  newest_results %>%
  group_by(id, Asset, account_var, initialUnits) %>%
  mutate(kk = row_number()) %>%
  slice_min(kk) %>%
  ungroup() %>%
  left_join(distinct_assets) %>%
  filter(inLocalDB == TRUE, !is.na(inLocalDB)) %>%
  dplyr::select(-inLocalDB) %>%
  dplyr::select(-kk) %>%
  mutate(
    across(.cols = c(initialUnits, dividendAdjustment, financing),
           .fns = ~ as.numeric(.))
  ) %>%
  mutate(Net_Profit = realizedPL + dividendAdjustment + financing)

write.csv(newest_results_sum,
          "C:/Users/Nikhil/Documents/macrodatasetsraw/data/trade_results_msi_pc_old_algos.csv",
          row.names = FALSE)

restults <- newest_results_sum %>%
  ungroup() %>%
  filter(date_open >= "2025-11-24") %>%
  filter(trade_col == "Long") %>%
  arrange(date_open) %>%
  mutate(
    cumulative_return = cumsum(Net_Profit)
  ) %>%
  ggplot(aes(x = date_open, y = cumulative_return)) +
  geom_line()
