test_performance <-
  AR_Test_Preds %>%
  left_join(
    actual_wins_losses_asset %>%
      dplyr::select(Date, Asset, period_return_24_Price, period_return_35_Price, period_return_46_Price)
  ) %>%
  mutate(
    trade_col = case_when(AR_LM_Pred_period_return_24_Price >
                            AR_LM_Pred_period_return_24_Price_mean + AR_LM_Pred_period_return_24_Price_sd*0 &
                            AR_GLM_Pred_period_return_24_Price >
                            AR_GLM_Pred_period_return_24_Price_mean + AR_GLM_Pred_period_return_24_Price_sd*0 ~ "Long")
  )

plot_dat_AR <-
  test_performance %>%
  filter(trade_col == "Long") %>%
  bind_rows(
    test_performance %>%
      mutate(trade_col = "Control")
  ) %>%
  group_by(trade_col) %>%
  arrange(Date, .by_group = TRUE) %>%
  group_by(trade_col) %>%
  mutate(
    cumulative_return = cumsum(period_return_24_Price)
  )

plot_dat_AR %>%
  ggplot(aes(x = Date, y = cumulative_return, color = trade_col)) +
  geom_line() +
  theme_minimal() +
  facet_wrap(.~ trade_col)

#---------------------------------------------------------
test_performance <-
  Copula_Test_Preds %>%
  left_join(
    actual_wins_losses_asset %>%
      dplyr::select(Date, Asset,
                    period_return_8_Price, period_return_12_Price, period_return_16_Price,
                    period_return_24_Price, period_return_35_Price, period_return_46_Price)
  ) %>%
  mutate(
    trade_col = case_when(
      Copula_LM_Pred_period_return_35_Price >
                            Copula_LM_Pred_period_return_35_Price_mean + Copula_LM_Pred_period_return_35_Price_sd*0
                            ~ "Long")
  )

plot_dat_Copula <-
  test_performance %>%
  filter(trade_col == "Long") %>%
  bind_rows(
    test_performance %>%
      mutate(trade_col = "Control")
  ) %>%
  group_by(trade_col) %>%
  arrange(Date, .by_group = TRUE) %>%
  group_by(trade_col) %>%
  mutate(
    cumulative_return = cumsum(period_return_35_Price)
  )

plot_dat_Copula %>%
  ggplot(aes(x = Date, y = cumulative_return, color = trade_col)) +
  geom_line() +
  theme_minimal() +
  facet_wrap(.~ trade_col)

#---------------------------------------------------------
test_performance <-
  state_space_Test_Preds %>%
  left_join(
    actual_wins_losses_asset %>%
      dplyr::select(Date, Asset,
                    period_return_8_Price, period_return_12_Price, period_return_16_Price,
                    period_return_24_Price, period_return_35_Price, period_return_46_Price)
  ) %>%
  mutate(
    trade_col = case_when(
      state_space_LM_Pred_period_return_35_Price >
        state_space_LM_Pred_period_return_35_Price_mean + state_space_LM_Pred_period_return_35_Price_sd*0
      ~ "Long")
  )

plot_dat_Copula <-
  test_performance %>%
  filter(trade_col == "Long") %>%
  bind_rows(
    test_performance %>%
      mutate(trade_col = "Control")
  ) %>%
  group_by(trade_col) %>%
  arrange(Date, .by_group = TRUE) %>%
  group_by(trade_col) %>%
  mutate(
    cumulative_return = cumsum(period_return_35_Price)
  )

plot_dat_Copula %>%
  ggplot(aes(x = Date, y = cumulative_return, color = trade_col)) +
  geom_line() +
  theme_minimal() +
  facet_wrap(.~ trade_col)
