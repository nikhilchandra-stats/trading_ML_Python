AUD_exports = get_AUS_exports()
all_vars_in_reg <- 140
asset_data_combined <- fs::dir_info("C:/Users/Nikhil Chandra/Documents/Asset Data/Futures/") %>%
  mutate(asset_name =
           str_remove(path, "C\\:\\/Users/Nikhil Chandra\\/Documents\\/Asset Data\\/Futures\\/") %>%
           str_remove("\\.csv") %>%
           str_remove("Historical Data")%>%
           str_remove("Stock Price") %>%
           str_remove("History")
  ) %>%
  dplyr::select(path, asset_name) %>%
  split(.$asset_name, drop = FALSE) %>%
  map_dfr( ~ read_csv(.x[1,1] %>% as.character()) %>%
             mutate(Asset = .x[1,2])
           )


futures_trades_data<-
  run_reg_for_trades_with_NN_grouped(
        raw_macro_data = raw_macro_data,
        asset_data = asset_data_combined,
        hidden_layers = c(round(all_vars_in_reg/2)
                          # round(all_vars_in_reg/2),
                          # round(all_vars_in_reg/2),
                          # round(all_vars_in_reg/2),
                          # round(all_vars_in_reg/2)
        ),
        iterations = 400000,
        AUD_exports = AUD_exports
      )

names(futures_trades_data[[1]][[1]])


futures_trades_data_df <- futures_trades_data %>%
  map_dfr(~ .x[[1]]) %>%
  dplyr::select(date, Price, Open, High, Low, change_var,
                pred, pred_lm, trade_NN, trade_LM, month_date, Asset) %>%
  mutate(weekly_date = lubridate::floor_date(date, "week")) %>%
  mutate(
    short_long =
      case_when(
        # pred < 0 & pred <= (mean_daily_return - 1.5*sd_daily_return) ~ "Very Very Short",
        # pred < 0 & pred <= (mean_daily_return - 0.75*sd_daily_return) ~ "Very Short",
        pred < 0  ~ "Short",
        # pred > 0 & pred >= (mean_daily_return + 1.5*sd_daily_return) ~ "Very Very Long",
        # pred > 0 & pred >= (mean_daily_return + 0.75*sd_daily_return) ~ "Very Long",
        pred > 0 ~ "Long"
      )
  ) %>%
  mutate(
    win_loss_NN = ifelse(trade_NN < 0, 0, 1),
    win_loss_LM = ifelse(trade_LM < 0, 0, 1)
  ) %>%
  filter(!is.na(trade_LM))

futures_trades_data_df_sum <- futures_trades_data_df %>%
  filter(str_detect(Asset, "AUD|USD|EUR|JPY")) %>%
  group_by(Asset, short_long) %>%
  summarise(
    wins_NN = sum(win_loss_NN, na.rm = T),
    wins_LM = sum(win_loss_LM, na.rm = T),
    total_trades = n()
  ) %>%
  mutate(
    Perc_NN = wins_NN/total_trades,
    Perc_LM = wins_LM/total_trades
  ) %>%
  filter(!is.na(short_long)) %>%
  ungroup() %>%
  group_by(short_long) %>%
  mutate(
    Total_Trades= sum(total_trades),
    Total_wins_NN = sum(wins_NN),
    Perc_Total_NN = Total_wins_NN/Total_Trades,

    Total_wins_LM = sum(wins_LM),
    Perc_Total_LM = Total_wins_LM/Total_Trades
  )

futures_trades_data_df_week_sum <- futures_trades_data_df %>%
  group_by(Asset, weekly_date) %>%
  slice_min(date) %>%
  group_by(Asset, short_long) %>%
  summarise(monthly_return_mean = mean(trade, na.rm = T),
            monthly_return_mean = quantile(trade,0.1, na.rm = T),

            monthly_return_total = sum(trade, na.rm = T),
            weekly_return_average = round(mean(trade, na.rm = T),4),
            weekly_return_low = round(quantile(trade,0.1 ,na.rm = T),4)
  ) %>%
  filter(!is.na(monthly_return_mean))
