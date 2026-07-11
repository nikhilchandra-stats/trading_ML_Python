trade_statement <-
  "
  (AR_LM_Pred_period_return_50_Price >= 1 & state_space_LM_Pred_period_return_50_Price >= 1 &  Asset == 'EUR_USD') |
  (AR_LM_Pred_period_return_50_Price >= 2 & state_space_LM_Pred_period_return_50_Price >= 2 &  Asset == 'GBP_USD')|
  (AR_LM_Pred_period_return_50_Price >= 2 & state_space_LM_Pred_period_return_50_Price >= 2 &  Asset == 'SG30_SGD')|
  (AR_LM_Pred_period_return_50_Price >= 2 & state_space_LM_Pred_period_return_50_Price >= 2 &  Asset == 'SPX500_USD')|
  (AR_LM_Pred_period_return_50_Price >= 2 & state_space_LM_Pred_period_return_50_Price >= 2 &  Asset == 'US2000_USD')|
  (AR_LM_Pred_period_return_50_Price >= 2 & state_space_LM_Pred_period_return_50_Price >= 2 &  Asset == 'USD_CAD')|
  (AR_LM_Pred_period_return_50_Price >= 2 & state_space_LM_Pred_period_return_50_Price >= 2 &  Asset == 'USD_JPY')|
  (AR_LM_Pred_period_return_50_Price >= 2 & state_space_LM_Pred_period_return_50_Price >= 2 &  Asset == 'XCU_USD')|
  (AR_LM_Pred_period_return_50_Price >= 2 & state_space_LM_Pred_period_return_50_Price >= 2 &  Asset == 'FR40_EUR')|
  (AR_LM_Pred_period_return_50_Price >= 2 &  Asset == 'GBP_CAD')|
  (AR_LM_Pred_period_return_50_Price >= 1 & state_space_LM_Pred_period_return_50_Price >= 1 &  Asset == 'EUR_JPY')|
  ( (state_space_LM_Pred_period_return_50_Price >= 5|AR_LM_Pred_period_return_50_Price >= 2) & Asset == 'GBP_JPY')|
  ( (state_space_LM_Pred_period_return_50_Price >= 2 & AR_LM_Pred_period_return_50_Price >= 2) & Asset == 'XAG_AUD')|
  (AR_LM_Pred_period_return_50_Price >= 2 &  Asset == 'XAG_USD')|
  (state_space_LM_Pred_period_return_50_Price >= 8.5 &  Asset == 'USD_SEK')|
  ( (AR_LM_Pred_period_return_50_Price >= 5 | state_space_LM_Pred_period_return_50_Price >= 6.5) & Asset == 'USD_CAD')|
  (state_space_LM_Pred_period_return_50_Price >= 5 & Asset == 'BTC_USD')|

  (((state_space_LM_Pred_period_return_50_Price >=
      state_space_LM_Pred_period_return_50_Price_mean +
      3*state_space_LM_Pred_period_return_50_Price_sd)|
    (AR_LM_Pred_period_return_50_Price >= 3)) & Asset == 'AUD_USD')|
  (AR_LM_Pred_period_return_50_Price >= 2.75 & state_space_LM_Pred_period_return_50_Price >= 2.75 &  Asset == 'USD_SGD')|

  (((state_space_LM_Pred_period_return_50_Price >= 4)|
  (AR_LM_Pred_period_return_50_Price >= 2.75 &
  state_space_LM_Pred_period_return_50_Price >= 2.75)) &
  Asset == 'NZD_USD')|

  (
  ((AR_LM_Pred_period_return_50_Price >= 0 & AR_LM_Pred_period_return_50_Price <= 0.1)|
    (state_space_LM_Pred_period_return_50_Price >= 6 & Asset == 'WTICO_USD')) &
   Asset == 'WTICO_USD'
  )|

  (state_space_LM_Pred_period_return_50_Price >= 7.25 &  Asset == 'EUR_AUD')|
  (state_space_LM_Pred_period_return_50_Price >= 8 & Asset == 'GBP_AUD')|
  (state_space_LM_Pred_period_return_50_Price >= 5.9 &  Asset == 'AU200_AUD')|
  (state_space_LM_Pred_period_return_50_Price >= 5.5 &  Asset == 'USD_JPY')|
  (state_space_LM_Pred_period_return_50_Price > 0 & AR_LM_Pred_period_return_50_Price > 0 & Asset == 'EU50_EUR')|
  (AR_LM_Pred_period_return_50_Price >= 3.25 & state_space_LM_Pred_period_return_50_Price >= 3.25 & Asset == 'UK10YB_GBP')|
  (AR_LM_Pred_period_return_50_Price >= 7 &  Asset == 'HK33_HKD')|
  (state_space_LM_Pred_period_return_50_Price >= 16.5 &  Asset == 'HK33_HKD')|
  (state_space_LM_Pred_period_return_50_Price >= 6.65 & Asset == 'XAU_USD')|
  (state_space_LM_Pred_period_return_50_Price >= 1.25 &
  AR_LM_Pred_period_return_50_Price >= 1.25 &
  Asset == 'XAU_USD')
"

cumulative_returns_sim_data <-
  get_total_portfolio_summary(
    generated_preds = generated_preds %>% filter(),
    trade_statement = trade_statement,
    actual_wins_losses = actual_wins_losses ,
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

