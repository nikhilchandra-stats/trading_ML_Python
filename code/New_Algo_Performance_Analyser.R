helpeR::load_custom_functions()

analyse_new_algos(
  trade_tracker_DB_path = "C:/Users/nikhi/Documents/trade_data/trade_tracker_daily_buy_close endpoints.db",
  realised_DB_path = "C:/Users/nikhi/Documents/trade_data/trade_tracker_realised.db",
  algo_start_date = "2026-09-01"
)

analyse_new_algos(
  trade_tracker_DB_path = "C:/Users/nikhi/Documents/trade_data/trade_tracker_daily_buy_close endpoints 2.db",
  realised_DB_path = "C:/Users/nikhi/Documents/trade_data/trade_tracker_realised.db",
  algo_start_date = "2026-09-01"
)

newest_results <-
  get_current_new_algo_trades(realised_DB_path ="C:/Users/nikhi/Documents/trade_data/trade_tracker_realised.db")

trade_tracker_DB <- connect_db( "C:/Users/nikhi/Documents/trade_data/trade_tracker_daily_buy_close endpoints.db")
all_trades_so_far <-
  DBI::dbGetQuery(conn = trade_tracker_DB,
                  "SELECT * FROM trade_tracker_endpoints")
DBI::dbDisconnect(trade_tracker_DB)
gc()

trade_tracker_DB <- connect_db("C:/Users/nikhi/Documents/trade_data/trade_tracker_daily_buy_close endpoints 2.db")
all_trades_so_far2 <-
  DBI::dbGetQuery(conn = trade_tracker_DB,
                  "SELECT * FROM trade_tracker_endpoints")
DBI::dbDisconnect(trade_tracker_DB)
gc()

all_trades_so_far_comb <-
  all_trades_so_far %>%
  bind_rows(all_trades_so_far2) %>%
  distinct()

distinct_assets <-
  all_trades_so_far_comb %>%
  distinct(Asset, account_var, trade_col, tradeID) %>%
  rename(id = tradeID) %>%
  mutate(inLocalDB = TRUE)

newest_results_sum <-
  newest_results %>%
  filter(date_open >= '2026-09-07') %>%
  left_join(distinct_assets) %>%
  filter(inLocalDB == TRUE, !is.na(inLocalDB )) %>%
  # filter(date_open >= "2025-11-17") %>%
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


combined_results <-
  newest_results_sum %>%
  group_by(id, Asset, account_var, initialUnits) %>%
  mutate(kk = row_number()) %>%
  slice_min(kk) %>%
  ungroup() %>%
  dplyr::select(-kk) %>%
  arrange(date_closed) %>%
  mutate(cumulative_return = cumsum(Net_Profit))

combined_results %>%
  ggplot(aes(x = date_closed, y = cumulative_return)) +
  geom_line() +
  theme_minimal()

upload_results_con <-
  connect_db("C:/Users/nikhi/Documents/trade_data/real_results_new_algo.db")

all_result_data <-
  DBI::dbGetQuery(conn = upload_results_con,
                  statement = "SELECT * FROM real_results_new_algo")

all_result_data_sum <-
  all_result_data %>%
  mutate(Date = as_datetime(Date)) %>%
  filter(Date >= '2026-09-07') %>%
  group_by(Date, trade_col) %>%
  summarise(Final_Return = sum(Final_Return)) %>%
  ungroup() %>%
  group_by(trade_col) %>%
  arrange(Date, .by_group = TRUE) %>%
  group_by(trade_col) %>%
  mutate(Final_Return_Cumulative = cumsum(Final_Return))  %>%
  ungroup()

combined_realised_simulated <-
  combined_results %>%
  dplyr::select(Date = date_open,
                Net_Profit = Net_Profit) %>%
  group_by(Date) %>%
  summarise(Final_Return = sum(Net_Profit, na.rm = T)) %>%
  arrange(Date) %>%
  mutate(Final_Return_Cumulative = cumsum(Final_Return)) %>%
  mutate(
    return_type = "Real"
  ) %>%
  bind_rows(
    all_result_data_sum %>% mutate(return_type = "Sim")
  )



combined_realised_simulated %>%
  ungroup() %>%
  ggplot(aes(x = Date, y = Final_Return_Cumulative
             ,color = return_type
  )) +
  geom_line() +
  geom_hline(yintercept = 0, linetype = "dashed", color = 'darkred') +
  # facet_wrap(.~trade_col, scales = "free") +
  theme_minimal() +
  scale_y_continuous(n.breaks = 30) +
  theme(legend.position = "bottom", axis.text = element_text(size = 7))
