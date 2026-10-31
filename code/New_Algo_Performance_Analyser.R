helpeR::load_custom_functions()

analyse_new_algos(
  trade_tracker_DB_path = "C:/Users/nikhi/Documents/trade_data/trade_tracker_daily_buy_close endpoints.db",
  realised_DB_path = "C:/Users/nikhi/Documents/trade_data/trade_tracker_realised.db",
  algo_start_date = "2026-10-01"
)

analyse_new_algos(
  trade_tracker_DB_path = "C:/Users/nikhi/Documents/trade_data/trade_tracker_daily_buy_close endpoints 2.db",
  realised_DB_path = "C:/Users/nikhi/Documents/trade_data/trade_tracker_realised.db",
  algo_start_date = "2026-10-01"
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

DBI::dbDisconnect(upload_results_con)
gc()
rm(upload_results_con)

all_result_data_sum <-
  all_result_data %>%
  filter(algo_name != "GAIN_NEG_COR_NON_V3_NEW_MODEL") %>%
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

all_result_data_sum_Complete <-
  all_result_data %>%
  filter(algo_name != "GAIN_NEG_COR_NON_V3_NEW_MODEL") %>%
  mutate(Date = as_datetime(Date)) %>%
  group_by(Date, trade_col) %>%
  summarise(Final_Return = sum(Final_Return)) %>%
  ungroup() %>%
  group_by(trade_col) %>%
  arrange(Date, .by_group = TRUE) %>%
  group_by(trade_col) %>%
  mutate(Final_Return_Cumulative = cumsum(Final_Return))  %>%
  ungroup()

all_result_data_sum_Complete %>%
  ungroup() %>%
  ggplot(aes(x = Date, y = Final_Return_Cumulative)) +
  geom_line() +
  geom_hline(yintercept = 0, linetype = "dashed", color = 'darkred') +
  # facet_wrap(.~trade_col, scales = "free") +
  theme_minimal() +
  scale_y_continuous(n.breaks = 30) +
  theme(legend.position = "bottom", axis.text = element_text(size = 7))


all_result_data_sum_Complete %>%
  ungroup() %>%
  # filter(Date >= '2023-01-01' & Date <= '2024-01-01') %>%
  arrange(Date) %>%
  mutate(
    Final_Return_Cumulative = cumsum(Final_Return)
  ) %>%
  ggplot(aes(x = Date, y = Final_Return_Cumulative)) +
  geom_line() +
  geom_hline(yintercept = 0, linetype = "dashed", color = 'darkred') +
  # facet_wrap(.~trade_col, scales = "free") +
  theme_minimal() +
  scale_y_continuous(n.breaks = 30) +
  theme(legend.position = "bottom", axis.text = element_text(size = 7))

all_result_data_sum_Complete %>%
  ungroup() %>%
  arrange(Date) %>%
  mutate(
    X = Final_Return_Cumulative - lag(Final_Return_Cumulative, 50),
    XX = Final_Return_Cumulative - lag(Final_Return_Cumulative, 100),
    XX2 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 150),
    XX3 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 200),
    XX4 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 400),
    XX5 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 500),
    XX6 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 1000),
    XX7 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 1250),
    XX8 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 2500),
    XX9 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 25),
    XX10 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 10),
    XX11 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 975)

  ) %>%
  summarise(
    min0 = min(X, na.rm = T),
    min1 = min(XX, na.rm = T),
    min2 = min(XX2, na.rm = T),
    min3 = min(XX3, na.rm = T),
    min4 = min(XX4, na.rm = T),
    min5 = min(XX5, na.rm = T),
    min6 = min(XX6, na.rm = T),
    min7 = min(XX7, na.rm = T),
    min8 = min(XX8, na.rm = T),
    min9 = min(XX9, na.rm = T),
    min10 = min(XX10, na.rm = T),
    min11 = min(XX11, na.rm = T)
  )

test_dates <-
  all_result_data_sum_Complete %>%
  ungroup() %>%
  arrange(Date) %>%
  mutate(
    X = Final_Return_Cumulative - lag(Final_Return_Cumulative, 50),
    XX = Final_Return_Cumulative - lag(Final_Return_Cumulative, 100),
    XX2 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 150),
    XX3 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 200),
    XX4 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 400),
    XX5 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 500),
    XX6 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 1000),
    XX7 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 1250),
    XX8 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 900),
    XX9 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 950),
    XX10 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 1050),
    XX11 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 1100)
  ) %>%
  mutate(
    Date = floor_date(Date, 'week')
  ) %>%
  group_by(Date) %>%
  summarise(across(.cols = contains("X"), ~min(., na.rm = T))) %>%
  slice_min(XX6, n = 10)

# Algo Dependant ----------------------------------------------------------


Results_by_Algo <-
  all_result_data %>%
  mutate(Date = as_datetime(Date)) %>%
  filter(Date >= "2026-09-07") %>%
  group_by(Date, trade_col, algo_name) %>%
  summarise(Final_Return = sum(Final_Return)) %>%
  ungroup() %>%
  group_by(trade_col, algo_name) %>%
  arrange(Date, .by_group = TRUE) %>%
  group_by(trade_col, algo_name) %>%
  mutate(Final_Return_Cumulative = cumsum(Final_Return))  %>%
  ungroup()

Results_by_Algo %>%
  filter(trade_col == "Long") %>%
  ggplot(aes(x = Date,
             y = Final_Return_Cumulative
             # colour = algo_name
             )
         ) +
  geom_line() +
  facet_wrap(.~algo_name, scales = "free") +
  theme_minimal()


test_dates <-
  Results_by_Algo %>%
  ungroup() %>%
  group_by(algo_name) %>%
  arrange(Date, .by_group = TRUE) %>%
  group_by(algo_name) %>%
  mutate(
    X = Final_Return_Cumulative - lag(Final_Return_Cumulative, 50),
    XX = Final_Return_Cumulative - lag(Final_Return_Cumulative, 100),
    XX2 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 150),
    XX3 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 200),
    XX4 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 400),
    XX5 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 500),
    XX6 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 1000),
    XX7 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 1250),
    XX8 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 900),
    XX9 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 950),
    XX10 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 1050),
    XX11 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 1100),
    XX12 = Final_Return_Cumulative - lag(Final_Return_Cumulative, 1128)
  ) %>%
  mutate(
    Date = floor_date(Date, 'day')
  ) %>%
  group_by(Date, algo_name) %>%
  summarise(across(.cols = contains("X"), ~min(., na.rm = T))) %>%
  ungroup() %>%
  group_by(algo_name) %>%
  slice_min(XX6, n = 3)
