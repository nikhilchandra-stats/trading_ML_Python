trade_statement <-
  "
  # (AR_LM_Pred_period_return_50_Price >= 1 & state_space_LM_Pred_period_return_50_Price >= 1 &  Asset == 'EUR_USD') |
  # (AR_LM_Pred_period_return_50_Price >= 2 & state_space_LM_Pred_period_return_50_Price >= 2 &  Asset == 'AU200_AUD')|
  # (state_space_LM_Pred_period_return_50_Price >= 3 &  Asset == 'EU50_EUR')|
  # (AR_LM_Pred_period_return_50_Price >= 2 & state_space_LM_Pred_period_return_50_Price >= 2 &  Asset == 'EUR_AUD')|
  # (AR_LM_Pred_period_return_50_Price >= 2 & state_space_LM_Pred_period_return_50_Price >= 2 &  Asset == 'USD_JPY')|
  # (AR_LM_Pred_period_return_50_Price >= 2 & state_space_LM_Pred_period_return_50_Price >= 2 &  Asset == 'GBP_AUD')|
  # (AR_LM_Pred_period_return_50_Price >= 2 & state_space_LM_Pred_period_return_50_Price >= 2 &  Asset == 'GBP_USD')|
  # (AR_LM_Pred_period_return_50_Price >= 2 & state_space_LM_Pred_period_return_50_Price >= 2 &  Asset == 'HK33_HKD')|
  # (AR_LM_Pred_period_return_50_Price >= 2 & state_space_LM_Pred_period_return_50_Price >= 2 &  Asset == 'SG30_SGD')|
  # (AR_LM_Pred_period_return_50_Price >= 2 & state_space_LM_Pred_period_return_50_Price >= 2 &  Asset == 'SPX500_USD')|
  # (AR_LM_Pred_period_return_50_Price >= 2 & state_space_LM_Pred_period_return_50_Price >= 2 &  Asset == 'UK10YB_GBP')|
  # (AR_LM_Pred_period_return_50_Price >= 2 & state_space_LM_Pred_period_return_50_Price >= 2 &  Asset == 'US2000_USD')|
  # (AR_LM_Pred_period_return_50_Price >= 2 & state_space_LM_Pred_period_return_50_Price >= 2 &  Asset == 'USD_CAD')|
  # (AR_LM_Pred_period_return_50_Price >= 2 & state_space_LM_Pred_period_return_50_Price >= 2 &  Asset == 'USD_JPY')|
  # (AR_LM_Pred_period_return_50_Price >= 2 & state_space_LM_Pred_period_return_50_Price >= 2 &  Asset == 'WTICO_USD')|
  # (AR_LM_Pred_period_return_50_Price >= 2 & state_space_LM_Pred_period_return_50_Price >= 2 &  Asset == 'XCU_USD')|
  # (AR_LM_Pred_period_return_50_Price >= 2 & state_space_LM_Pred_period_return_50_Price >= 2 &  Asset == 'FR40_EUR')|
  # (AR_LM_Pred_period_return_50_Price >= 2 &  Asset == 'GBP_CAD')|
  # (AR_LM_Pred_period_return_50_Price >= 1 & state_space_LM_Pred_period_return_50_Price >= 1 &  Asset == 'EUR_JPY')|
  # ( (state_space_LM_Pred_period_return_50_Price >= 5|AR_LM_Pred_period_return_50_Price >= 2) & Asset == 'GBP_JPY')|
  # ( (state_space_LM_Pred_period_return_50_Price >= 2 & AR_LM_Pred_period_return_50_Price >= 2) & Asset == 'XAG_AUD')|
  # (AR_LM_Pred_period_return_50_Price >= 2 &  Asset == 'XAG_USD')|
  # (state_space_LM_Pred_period_return_50_Price >= 1 & AR_LM_Pred_period_return_50_Price > 1 & Asset == 'XAU_USD')|
  # (state_space_LM_Pred_period_return_50_Price >= 8.5 &  Asset == 'USD_SEK')|
  # ( (AR_LM_Pred_period_return_50_Price >= 5 | state_space_LM_Pred_period_return_50_Price >= 6.5) & Asset == 'USD_CAD')|
  # (state_space_LM_Pred_period_return_50_Price >= 5 & Asset == 'BTC_USD')|
  # (((state_space_LM_Pred_period_return_50_Price >=
  #     state_space_LM_Pred_period_return_50_Price_mean +
  #     3*state_space_LM_Pred_period_return_50_Price_sd)|
  #   (AR_LM_Pred_period_return_50_Price >= 3)) & Asset == 'AUD_USD')|
  # (AR_LM_Pred_period_return_50_Price >= 2.75 & state_space_LM_Pred_period_return_50_Price >= 2.75 &  Asset == 'USD_SGD')|

  # (AR_LM_Pred_period_return_50_Price >= 2.75 & state_space_LM_Pred_period_return_50_Price >= 2.75 &  Asset == 'NZD_USD')

  (((state_space_LM_Pred_period_return_50_Price >= 4)|
  (AR_LM_Pred_period_return_50_Price >= 2.75 &
  state_space_LM_Pred_period_return_50_Price >= 2.75)) &
  Asset == 'NZD_USD')
"

cumulative_returns_sim_data <-
  get_total_portfolio_summary(
    generated_preds = generated_preds %>% filter(Asset == "NZD_USD"),
    trade_statement = trade_statement,
    actual_wins_losses = actual_wins_losses %>% filter(Asset == "NZD_USD") ,
    trade_direction = "Long",
    return_col = "period_return_50_Price"
  )

cumulative_returns_sim_data %>%
  ggplot(aes(x = Date, y = Cumulative_Return)) +
  geom_line() +
  scale_y_continuous(n.breaks = 20) +
  facet_wrap(.~trade_col, scales = "free") +
  theme_minimal()

asset_summaries <-
  get_asset_random_sim_returns(
    generated_preds = generated_preds,
    trade_statement = trade_statement,
    actual_wins_losses = actual_wins_losses,
    trade_direction = "Long",
    return_col = "period_return_50_Price",
    simulations = 5000,
    samples = 50
  )

