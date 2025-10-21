longs <-
  indicator_data %>%
  filter(trade_col == "Long") %>%
  filter(
    logit_combined_pred >= mean_logit_combined_pred + 0*sd_logit_combined_pred &
      averaged_pred >= sd_averaged_pred*0 + mean_averaged_pred
  )

shorts <-
  indicator_data %>%
  filter(trade_col == "Short") %>%
  filter(
    logit_combined_pred >= mean_logit_combined_pred + 0*sd_logit_combined_pred &
      averaged_pred >= sd_averaged_pred*0 + mean_averaged_pred
  )

all_trades <-
  shorts %>%
  bind_rows(longs) %>%
  mutate(
    trade_return_dollar_aud =
      case_when(
        str_detect(Asset, "JPY") ~ trade_return_dollar_aud/1000,
        TRUE ~ trade_return_dollar_aud
      )
  ) %>%
  mutate(
    trade_end_date = as_datetime(Date, tz = "Australia/Canberra") + dhours(Time_Periods),
    Date = trade_end_date
  ) %>%
  group_by(Date, Asset, trade_col ) %>%
  summarise(trade_return_dollar_aud = mean(trade_return_dollar_aud, na.rm = T)) %>%
  mutate(
    Date = as_datetime(Date, tz = "Australia/Canberra")
  ) %>%
  group_by(Date) %>%
  summarise(trade_return_dollar_aud = sum(trade_return_dollar_aud, na.rm = T)) %>%
  ungroup() %>%
  arrange(Date) %>%
  mutate(
    cumulative_returns = cumsum(trade_return_dollar_aud)
  )

all_trades %>%
  ggplot(aes(x = Date, y = cumulative_returns)) +
  geom_line() +
  theme_minimal()

indicator_data %>%
  distinct(Asset) %>%
  pull(Asset) %>%
  unique()


model_data_store_path <-
  "C:/Users/Nikhil Chandra/Documents/trade_data/single_asset_improved_indcator_trades_ts.db"
model_data_store_db <-
  connect_db(model_data_store_path)

indicator_data <-
  DBI::dbGetQuery(conn = model_data_store_db,
                  statement = "SELECT * FROM single_asset_improved") %>%
  distinct() %>%
  group_by(sim_index, Asset) %>%
  mutate(Date = as_datetime(Date),
         test_date_start = as_date(test_date_start),
         test_end_date = as_date(test_end_date),
         Date_filt = as_date(Date))
DBI::dbDisconnect(model_data_store_db)
rm(model_data_store_db)
gc()

asset_optimisation_store_path <-
  "C:/Users/Nikhil Chandra/Documents/trade_data/single_asset_improved_asset_optimisation.db"
asset_optimisation_store_db <-
  connect_db(model_data_store_path)

pred_thresh <- seq(-2,2, 0.1)

model_results_long <-
  indicator_data %>%
  ungroup()

model_results_short <-
  ungroup() %>%
  filter(trade_col == "Short") %>%
  select(Date, Asset, contains("pred"), trade_col) %>%
  distinct() %>%
  group_by(Date, Asset, trade_col) %>%
  summarise(
    across(contains("pred"), .fns = ~ mean(., na.rm = T))
  ) %>%
  ungroup() %>%
  mutate(
    Date = as_datetime(Date)
  )

control_results <-
  indicator_data %>%
  ungroup() %>%
  mutate(

    trade_return_dollar_aud =
      case_when(
        str_detect(Asset, "JPY") ~ trade_return_dollar_aud/1000,
        TRUE ~ trade_return_dollar_aud
      )

  ) %>%
  mutate(
    wins =
      case_when(
        trade_return_dollar_aud > 0 ~ 1,
        TRUE ~ 0
      )
  ) %>%
  group_by(Asset, sim_index, trade_col) %>%
  summarise(
    trade_return_dollar_aud = sum(trade_return_dollar_aud, na.rm = T),
    wins = sum(wins, na.rm = T),
    total_trades = n()
  ) %>%
  ungroup() %>%
  mutate(
    Win_Perc = wins/total_trades
  ) %>%
  group_by(Asset, trade_col) %>%
  summarise(
    Mid_Control = mean(trade_return_dollar_aud, na.rm = T),
    lower_Control = quantile(trade_return_dollar_aud, 0.25 ,na.rm = T),
    upper_Control = quantile(trade_return_dollar_aud, 0.75 ,na.rm = T),
    simulations = n_distinct(sim_index),
    pred_thresh = "control",
    Win_Perc_mean = mean(Win_Perc, na.rm = T),
    wins_mean = mean(wins, na.rm = T),
    total_trades_mean = mean(total_trades, na.rm = T)

  )

write_table_sql_lite(.data = control_results,
                     table_name = "single_asset_improved_asset_optimisation",
                     conn = asset_optimisation_store_db,
                     overwrite_true = TRUE)

for (j in 1:length(pred_thresh)) {

  current_pred <- pred_thresh[j]

  model_results <-
    indicator_data %>%
    ungroup() %>%
    filter(
      logit_combined_pred >= mean_logit_combined_pred + current_pred*sd_logit_combined_pred &
        averaged_pred >=  mean_averaged_pred + sd_averaged_pred*current_pred
    ) %>%
    ungroup() %>%
    mutate(

      trade_return_dollar_aud =
        case_when(
          str_detect(Asset, "JPY") ~ trade_return_dollar_aud/1000,
          TRUE ~ trade_return_dollar_aud
        )

    ) %>%
    mutate(
      wins =
        case_when(
          trade_return_dollar_aud > 0 ~ 1,
          TRUE ~ 0
        )
    ) %>%
    group_by(Asset, sim_index, trade_col) %>%
    summarise(
      trade_return_dollar_aud = sum(trade_return_dollar_aud, na.rm = T),
      wins = sum(wins, na.rm = T),
      total_trades = n()
    ) %>%
    ungroup() %>%
    mutate(
      Win_Perc = wins/total_trades
    ) %>%
    group_by(Asset, trade_col) %>%
    summarise(
      Mid_Control = mean(trade_return_dollar_aud, na.rm = T),
      lower_Control = quantile(trade_return_dollar_aud, 0.25 ,na.rm = T),
      upper_Control = quantile(trade_return_dollar_aud, 0.75 ,na.rm = T),
      simulations = n_distinct(sim_index),
      pred_thresh = as.character(current_pred),
      Win_Perc_mean = mean(Win_Perc, na.rm = T),
      Win_Perc_mean = mean(Win_Perc, na.rm = T),
      wins_mean = mean(wins, na.rm = T),
      total_trades_mean = mean(total_trades, na.rm = T)
    )

  append_table_sql_lite(.data = model_results,
                       table_name = "single_asset_improved_asset_optimisation",
                       conn = asset_optimisation_store_db,
                       overwrite_true = TRUE)

}
