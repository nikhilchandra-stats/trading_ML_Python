trade_statement <-
  "
  (

  (AR_GLM_Pred_period_return_50_Price >= 0.55 & state_space_GLM_Pred_period_return_50_Price >= 0.55 &  Asset == 'EUR_USD') |
  # (AR_GLM_Pred_period_return_50_Price >= 0.675 & state_space_GLM_Pred_period_return_50_Price >= 0.675 &  Asset == 'AU200_AUD')|
  ( (state_space_GLM_Pred_period_return_50_Price >= 0.95|AR_GLM_Pred_period_return_50_Price >= 0.6) &  Asset == 'AUD_USD') |
  (state_space_GLM_Pred_period_return_50_Price >= 0.7 &  Asset == 'EU50_EUR')|
  (AR_GLM_Pred_period_return_50_Price >= 0.6 & state_space_GLM_Pred_period_return_50_Price >= 0.6 &  Asset == 'EUR_AUD')|
  (AR_GLM_Pred_period_return_50_Price >= 0.6 & state_space_GLM_Pred_period_return_50_Price >= 0.6 &  Asset == 'USD_CAD')|
  (AR_GLM_Pred_period_return_50_Price >= 0.6 & state_space_GLM_Pred_period_return_50_Price >= 0.6 &  Asset == 'USD_JPY')|
  (AR_GLM_Pred_period_return_50_Price >= 0.6 & state_space_GLM_Pred_period_return_50_Price >= 0.6 &  Asset == 'GBP_AUD')|
  (AR_GLM_Pred_period_return_50_Price >= 0.625 & state_space_GLM_Pred_period_return_50_Price >= 0.625 &  Asset == 'GBP_USD')|
  (AR_GLM_Pred_period_return_50_Price >= 0.6 & state_space_GLM_Pred_period_return_50_Price >= 0.6 &  Asset == 'HK33_HKD')|
  (AR_GLM_Pred_period_return_50_Price >= 0.675 & state_space_GLM_Pred_period_return_50_Price >= 0.675 &  Asset == 'NZD_USD')|
  (AR_GLM_Pred_period_return_50_Price >= 0.675 & state_space_GLM_Pred_period_return_50_Price >= 0.675 &  Asset == 'SG30_SGD')|
  (AR_GLM_Pred_period_return_50_Price >= 0.60 & state_space_GLM_Pred_period_return_50_Price >= 0.60 &  Asset == 'SPX500_USD')|
  (AR_GLM_Pred_period_return_50_Price >= 0.675 & state_space_GLM_Pred_period_return_50_Price >= 0.675 &  Asset == 'UK10YB_GBP')|
  (AR_GLM_Pred_period_return_50_Price >= 0.6 & state_space_GLM_Pred_period_return_50_Price >= 0.6 &  Asset == 'US2000_USD')|
  (AR_GLM_Pred_period_return_50_Price >= 0.6 & state_space_GLM_Pred_period_return_50_Price >= 0.6 &  Asset == 'USB10Y_USD')|
  (AR_GLM_Pred_period_return_50_Price >= 0.6 & state_space_GLM_Pred_period_return_50_Price >= 0.6 &  Asset == 'USD_CAD')|
  (AR_GLM_Pred_period_return_50_Price >= 0.6 & state_space_GLM_Pred_period_return_50_Price >= 0.6 &  Asset == 'USD_JPY')|
  (AR_GLM_Pred_period_return_50_Price >= 0.65 & state_space_GLM_Pred_period_return_50_Price >= 0.6 &  Asset == 'USD_SEK')|
  (AR_GLM_Pred_period_return_50_Price >= 0.7 & state_space_GLM_Pred_period_return_50_Price >= 0.6 &  Asset == 'USD_SGD')|
  (AR_GLM_Pred_period_return_50_Price >= 0.6 & state_space_GLM_Pred_period_return_50_Price >= 0.6 &  Asset == 'WTICO_USD')|
  (AR_GLM_Pred_period_return_50_Price >= 0.675 & state_space_GLM_Pred_period_return_50_Price >= 0.675 &  Asset == 'XCU_USD')|
  (AR_GLM_Pred_period_return_50_Price >= 0.6 & state_space_GLM_Pred_period_return_50_Price >= 0.6 &  Asset == 'FR40_EUR')|
  (state_space_GLM_Pred_period_return_50_Price >= 0.9 &  Asset == 'BTC_USD')|
  (AR_GLM_Pred_period_return_50_Price >= 0.65 &  Asset == 'GBP_CAD')|
  (AR_GLM_Pred_period_return_50_Price >= 0.575 & state_space_GLM_Pred_period_return_50_Price >= 0.575 &  Asset == 'EUR_JPY')|
  ( (state_space_GLM_Pred_period_return_50_Price >= 0.875|AR_GLM_Pred_period_return_50_Price >= 0.6) & Asset == 'GBP_JPY')|
  ( (state_space_GLM_Pred_period_return_50_Price >= 0.525 & AR_GLM_Pred_period_return_50_Price >= 0.525) & Asset == 'XAG_AUD')|
  (AR_GLM_Pred_period_return_50_Price >= 0.65 &  Asset == 'XAG_USD')|
  (state_space_GLM_Pred_period_return_50_Price >= 0.575 & AR_GLM_Pred_period_return_50_Price >0.575 & Asset == 'XAU_USD')

  )|
  (

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
  # (state_space_LM_Pred_period_return_50_Price >= 5.9 &  Asset == 'AU200_AUD')|
  (state_space_LM_Pred_period_return_50_Price >= 5.5 &  Asset == 'USD_JPY')|
  (state_space_LM_Pred_period_return_50_Price > 0 & AR_LM_Pred_period_return_50_Price > 0 & Asset == 'EU50_EUR')|
  (AR_LM_Pred_period_return_50_Price >= 3.25 & state_space_LM_Pred_period_return_50_Price >= 3.25 & Asset == 'UK10YB_GBP')|
  (AR_LM_Pred_period_return_50_Price >= 7 &  Asset == 'HK33_HKD')|
  (state_space_LM_Pred_period_return_50_Price >= 16.5 &  Asset == 'HK33_HKD')|
  (state_space_LM_Pred_period_return_50_Price >= 6.65 & Asset == 'XAU_USD')|
  (state_space_LM_Pred_period_return_50_Price >= 1.25 &
  AR_LM_Pred_period_return_50_Price >= 1.25 &
  Asset == 'XAU_USD')
  )|
  (
    (
    AR_LM_Pred_period_return_50_Price >=
    AR_LM_Pred_period_return_50_Price_mean + 1*AR_LM_Pred_period_return_50_Price_sd &
    AR_LM_Pred_period_return_50_Price >= 2 & AR_LM_Pred_period_return_50_Price <= 4 &
    Asset == 'SPX500_USD'
    )|
  (
    (state_space_LM_Pred_period_return_50_Price >=
    state_space_LM_Pred_period_return_50_Price_mean + 1.5*state_space_LM_Pred_period_return_50_Price_sd) &
    (state_space_LM_Pred_period_return_50_Price > 3.5) &
    Asset == 'SPX500_USD'
  )|
  (
    AR_GLM_Pred_period_return_50_Price >=
    AR_GLM_Pred_period_return_50_Price_mean + 1.5*AR_GLM_Pred_period_return_50_Price_sd &
    AR_GLM_Pred_period_return_50_Price >= 0.65 & AR_GLM_Pred_period_return_50_Price <= 0.99 &
    Asset == 'SPX500_USD'
  )|
  #   (
  #   AR_LM_Pred_period_return_50_Price >=
  #   AR_LM_Pred_period_return_50_Price_mean + 1.5*AR_LM_Pred_period_return_50_Price_sd &
  #   Asset == 'UK100_GBP'
  #   )|
  # (
  #   (state_space_LM_Pred_period_return_50_Price >=
  #   state_space_LM_Pred_period_return_50_Price_mean + 1.5*state_space_LM_Pred_period_return_50_Price_sd) &
  #   (state_space_LM_Pred_period_return_50_Price > 4) &
  #   Asset == 'UK100_GBP'
  # )|
  (
    (state_space_LM_Pred_period_return_50_Price >=
    state_space_LM_Pred_period_return_50_Price_mean + 1.75*state_space_LM_Pred_period_return_50_Price_sd) &
    (state_space_LM_Pred_period_return_50_Price > 5.5) &
    Asset == 'XAU_USD'
  )|

  (
    AR_LM_Pred_period_return_50_Price >=
    AR_LM_Pred_period_return_50_Price_mean + 0*AR_LM_Pred_period_return_50_Price_sd &
    AR_LM_Pred_period_return_50_Price >= 2 &
    Asset == 'GBP_CAD'
    )|

  (
    AR_GLM_Pred_period_return_50_Price >=
    AR_GLM_Pred_period_return_50_Price_mean + 0*AR_GLM_Pred_period_return_50_Price_sd &
    AR_GLM_Pred_period_return_50_Price >= 0.6 & AR_GLM_Pred_period_return_50_Price <= 1 &
    Asset == 'GBP_CAD'
  )|

  (
    (state_space_GLM_Pred_period_return_50_Price >=
    state_space_GLM_Pred_period_return_50_Price_mean + 0*state_space_GLM_Pred_period_return_50_Price_sd) &
    (state_space_GLM_Pred_period_return_50_Price > 0.9) &
    Asset == 'GBP_CAD'
  )|

  (
    (state_space_LM_Pred_period_return_50_Price >=
    state_space_LM_Pred_period_return_50_Price_mean + 1*state_space_LM_Pred_period_return_50_Price_sd) &
    (state_space_LM_Pred_period_return_50_Price > 2) &
    Asset == 'NZD_USD'
  )|
  (
    (state_space_GLM_Pred_period_return_50_Price >=
    state_space_GLM_Pred_period_return_50_Price_mean + 2.25*state_space_GLM_Pred_period_return_50_Price_sd) &
    (state_space_GLM_Pred_period_return_50_Price > 0.5) &
    Asset == 'NZD_USD'
  )|

  (
    AR_GLM_Pred_period_return_50_Price >=
    AR_GLM_Pred_period_return_50_Price_mean + 1.5*AR_GLM_Pred_period_return_50_Price_sd &
    AR_GLM_Pred_period_return_50_Price >= 0.63 & AR_GLM_Pred_period_return_50_Price <= 1 &
    Asset == 'USB10Y_USD'
  )|

  (
    AR_LM_Pred_period_return_50_Price >=
    AR_LM_Pred_period_return_50_Price_mean + 1.33*AR_LM_Pred_period_return_50_Price_sd &
    AR_LM_Pred_period_return_50_Price >= 0 &
    Asset == 'EUR_JPY'
  )|

  (
    (state_space_LM_Pred_period_return_50_Price >=
    state_space_LM_Pred_period_return_50_Price_mean + 2*state_space_LM_Pred_period_return_50_Price_sd) &
    (state_space_LM_Pred_period_return_50_Price > 0) &
    Asset == 'EUR_JPY'
  )|

  (
    (state_space_LM_Pred_period_return_50_Price >=
    state_space_LM_Pred_period_return_50_Price_mean + 1.85*state_space_LM_Pred_period_return_50_Price_sd) &
    (state_space_LM_Pred_period_return_50_Price > 0) &
    Asset == 'AU200_AUD'
  )|
  (
    (state_space_GLM_Pred_period_return_50_Price >=
    state_space_GLM_Pred_period_return_50_Price_mean + 0*state_space_GLM_Pred_period_return_50_Price_sd) &
    (state_space_GLM_Pred_period_return_50_Price > 0.835) &
    Asset == 'AU200_AUD'
  )|
  (
    AR_LM_Pred_period_return_50_Price >=
    AR_LM_Pred_period_return_50_Price_mean + 0*AR_LM_Pred_period_return_50_Price_sd &
    AR_LM_Pred_period_return_50_Price >= 0.5 &
    Asset == 'XCU_USD'
  )|
  (
    AR_LM_Pred_period_return_50_Price >=
    AR_LM_Pred_period_return_50_Price_mean + 1.5*AR_LM_Pred_period_return_50_Price_sd &
    AR_LM_Pred_period_return_50_Price >= 0 &
    Asset == 'EU50_EUR'
  )|

  (
    AR_LM_Pred_period_return_50_Price >=
    AR_LM_Pred_period_return_50_Price_mean + 0*AR_LM_Pred_period_return_50_Price_sd &
    AR_LM_Pred_period_return_50_Price >= 2.5 &
    Asset == 'AUD_USD'
    )|

  (
    (state_space_LM_Pred_period_return_50_Price >=
    state_space_LM_Pred_period_return_50_Price_mean + 0*state_space_LM_Pred_period_return_50_Price_sd) &
    (state_space_LM_Pred_period_return_50_Price > 8) &
    Asset == 'AUD_USD'
  )|

  (
    AR_GLM_Pred_period_return_50_Price >=
    AR_GLM_Pred_period_return_50_Price_mean + 2.5*AR_GLM_Pred_period_return_50_Price_sd &
    AR_GLM_Pred_period_return_50_Price >= 0.5 & AR_GLM_Pred_period_return_50_Price <= 1 &
    Asset == 'AUD_USD'
  )|

  (
    (state_space_GLM_Pred_period_return_50_Price >=
    state_space_GLM_Pred_period_return_50_Price_mean + 2.65*state_space_GLM_Pred_period_return_50_Price_sd) &
    (state_space_GLM_Pred_period_return_50_Price > 0.5) &
    Asset == 'AUD_USD'
  )|

  (
    (state_space_GLM_Pred_period_return_50_Price >=
    state_space_GLM_Pred_period_return_50_Price_mean + 1*state_space_GLM_Pred_period_return_50_Price_sd) &
    (state_space_GLM_Pred_period_return_50_Price > 0.75) &
    Asset == 'GBP_AUD'
  )|

    (AR_LM_Pred_period_return_50_Price >=
    AR_LM_Pred_period_return_50_Price_mean + 0*AR_LM_Pred_period_return_50_Price_sd &
    AR_LM_Pred_period_return_50_Price >= 0 &
    Asset == 'WTICO_USD')|

    (
      state_space_LM_Pred_period_return_50_Price >=
      state_space_LM_Pred_period_return_50_Price_mean + 0*state_space_LM_Pred_period_return_50_Price_sd &
      state_space_LM_Pred_period_return_50_Price > 5 &
      Asset == 'WTICO_USD'
    )|

  (
    (state_space_GLM_Pred_period_return_50_Price >=
    state_space_GLM_Pred_period_return_50_Price_mean + 0*state_space_GLM_Pred_period_return_50_Price_sd) &
    (state_space_GLM_Pred_period_return_50_Price > 0.925) &
    Asset == 'BTC_USD'
  )|

  (
    AR_LM_Pred_period_return_50_Price >=
    AR_LM_Pred_period_return_50_Price_mean + 2.25*AR_LM_Pred_period_return_50_Price_sd &
    AR_LM_Pred_period_return_50_Price >= 1 &
    Asset == 'HK33_HKD'
    )|

   (
    AR_LM_Pred_period_return_50_Price >=
    AR_LM_Pred_period_return_50_Price_mean + 0*AR_LM_Pred_period_return_50_Price_sd &
    AR_LM_Pred_period_return_50_Price >= 9.5 &
    Asset == 'HK33_HKD'
   )|

  (
    (state_space_GLM_Pred_period_return_50_Price >=
    state_space_GLM_Pred_period_return_50_Price_mean + 1.9*state_space_GLM_Pred_period_return_50_Price_sd) &
    (state_space_GLM_Pred_period_return_50_Price > 0.75) &
    Asset == 'HK33_HKD'
  )|

  (
    AR_LM_Pred_period_return_50_Price >=
      AR_LM_Pred_period_return_50_Price_mean + 1*AR_LM_Pred_period_return_50_Price_sd &
    AR_LM_Pred_period_return_50_Price <=
      AR_LM_Pred_period_return_50_Price_mean + 1.9*AR_LM_Pred_period_return_50_Price_sd &
    AR_LM_Pred_period_return_50_Price >= 3.5 &
    Asset == 'US2000_USD'
    )|

  (
    (state_space_LM_Pred_period_return_50_Price >=
    state_space_LM_Pred_period_return_50_Price_mean + 0*state_space_LM_Pred_period_return_50_Price_sd) &
    (state_space_LM_Pred_period_return_50_Price > 6) &
    Asset == 'US2000_USD'
  )|

  (
    AR_LM_Pred_period_return_50_Price >=
      AR_LM_Pred_period_return_50_Price_mean + 2*AR_LM_Pred_period_return_50_Price_sd &
    AR_LM_Pred_period_return_50_Price <=
      AR_LM_Pred_period_return_50_Price_mean + 10*AR_LM_Pred_period_return_50_Price_sd &
    AR_LM_Pred_period_return_50_Price >= 0 &
    Asset == 'FR40_EUR'
    )|

  (
    (state_space_LM_Pred_period_return_50_Price >=
    state_space_LM_Pred_period_return_50_Price_mean + 1*state_space_LM_Pred_period_return_50_Price_sd) &
    (state_space_LM_Pred_period_return_50_Price > 15) &
    Asset == 'FR40_EUR'
  )|

  (
    (state_space_LM_Pred_period_return_50_Price >=
    state_space_LM_Pred_period_return_50_Price_mean + 0*state_space_LM_Pred_period_return_50_Price_sd) &
    (state_space_LM_Pred_period_return_50_Price > 6) &
    Asset == 'USD_SEK'
  )|

  (
    (state_space_GLM_Pred_period_return_50_Price >=
    state_space_GLM_Pred_period_return_50_Price_mean + 2*state_space_GLM_Pred_period_return_50_Price_sd) &
    (state_space_GLM_Pred_period_return_50_Price <=
    state_space_GLM_Pred_period_return_50_Price_mean + 2.5*state_space_GLM_Pred_period_return_50_Price_sd) &
    (state_space_GLM_Pred_period_return_50_Price > 0.6) &
    Asset == 'USD_SEK'
  )|

  (
    (state_space_LM_Pred_period_return_50_Price >=
    state_space_LM_Pred_period_return_50_Price_mean + 2.75*state_space_LM_Pred_period_return_50_Price_sd) &
    (state_space_LM_Pred_period_return_50_Price > 0) &
    Asset == 'EUR_AUD'
  )|

  (
    (state_space_GLM_Pred_period_return_50_Price >=
    state_space_GLM_Pred_period_return_50_Price_mean + 2.3*state_space_GLM_Pred_period_return_50_Price_sd) &
    (state_space_GLM_Pred_period_return_50_Price > 0.5) &
    Asset == 'EUR_AUD'
  )|

  (
    AR_GLM_Pred_period_return_50_Price >=
    AR_GLM_Pred_period_return_50_Price_mean + 2.25*AR_GLM_Pred_period_return_50_Price_sd &
    AR_GLM_Pred_period_return_50_Price >= 0 & AR_GLM_Pred_period_return_50_Price <= 1 &
    Asset == 'XAG_USD'
  )

  )
"

trade_statement_2 <-
  "
  #stop_factor = 2, profit_factor = 3, period = 12
  (state_space_LM_Pred_period_return_50_Price >= 15 &
  state_space_LM_Pred_period_return_50_Price <= 25 &
  Asset == 'HK33_HKD')|
  (state_space_GLM_Pred_period_return_50_Price >= 0.96 &
  state_space_GLM_Pred_period_return_50_Price <= 0.98 &
  Asset == 'HK33_HKD')|

   #stop_factor = 5, profit_factor = 10, period = 12
   (state_space_GLM_Pred_period_return_50_Price >= 0.98 &
   state_space_GLM_Pred_period_return_50_Price <= 1 &
   Asset == 'BTC_USD')|
   (AR_GLM_Pred_period_return_50_Price >= 0.9 &
   AR_GLM_Pred_period_return_50_Price <= 0.99 &
   Asset == 'BTC_USD')|

   #stop_factor = 3, profit_factor = 6, period = 24
   (state_space_GLM_Pred_period_return_50_Price >= 0.99 &
   state_space_GLM_Pred_period_return_50_Price <= 1 &
   Asset == 'AUD_USD')|
   (AR_GLM_Pred_period_return_50_Price >= 0.675 &
   AR_GLM_Pred_period_return_50_Price <= 1 &
   Asset == 'AUD_USD')|

   #stop_factor = 3, profit_factor = 6, period = 12
   (state_space_GLM_Pred_period_return_50_Price >= 0.95 &
   state_space_GLM_Pred_period_return_50_Price <= 1 &
   Asset == 'USD_JPY')|
   (state_space_LM_Pred_period_return_50_Price >= 9.5 &
   state_space_LM_Pred_period_return_50_Price <= 1000 &
   Asset == 'USD_JPY')|

   #stop_factor = 2, profit_factor = 4, period = 12
   (state_space_LM_Pred_period_return_50_Price >= 1.75 &
   state_space_LM_Pred_period_return_50_Price <= 2 &
   Asset == 'WTICO_USD')|
   (state_space_LM_Pred_period_return_50_Price >= 6 &
   state_space_LM_Pred_period_return_50_Price <= 1000 &
   Asset == 'WTICO_USD')|
   (state_space_GLM_Pred_period_return_50_Price >= 0.55 &
   state_space_GLM_Pred_period_return_50_Price <= 0.625 &
   Asset == 'WTICO_USD')|

   #stop_factor = 5, profit_factor = 10, period = 24
   (state_space_LM_Pred_period_return_50_Price >= 0.4 &
   state_space_LM_Pred_period_return_50_Price <= 1 &
   Asset == 'SG30_SGD')|

   # #stop_factor = 4, profit_factor = 8, period = 12
   (AR_GLM_Pred_period_return_50_Price >= 0.75 &
   state_space_GLM_Pred_period_return_50_Price >= 0.75 &
   Asset == 'XCU_USD')|
   (AR_LM_Pred_period_return_50_Price >= 0 &
    state_space_LM_Pred_period_return_50_Price >= 0 &
    AR_LM_Pred_period_return_50_Price <= 0.25 &
    Asset == 'XCU_USD')|
    (
    AR_LM_Pred_period_return_50_Price >=
    AR_LM_Pred_period_return_50_Price_mean + 0.25*AR_LM_Pred_period_return_50_Price_sd &
    AR_LM_Pred_period_return_50_Price >= 0.5 &
    Asset == 'XCU_USD'
    )|
  # #stop_factor = 3, profit_factor = 6, period = 12
  (state_space_LM_Pred_period_return_50_Price >= 7 & Asset == 'XAU_USD')|
  (state_space_LM_Pred_period_return_50_Price >= 2.75 &
  AR_LM_Pred_period_return_50_Price >= 0 &
  AR_LM_Pred_period_return_50_Price < 2 &
  Asset == 'XAU_USD')|
  # #stop_factor = 3, profit_factor = 6, period = 12
  (
  state_space_GLM_Pred_period_return_50_Price >= 0.9 &
  state_space_GLM_Pred_period_return_50_Price <= 0.99 &
  Asset == 'XAU_USD'
  )|
    (state_space_LM_Pred_period_return_50_Price >= 9 &
  state_space_LM_Pred_period_return_50_Price < 1000 &
    Asset == 'AU200_AUD')|
  (state_space_GLM_Pred_period_return_50_Price >= 0.955 &
     state_space_GLM_Pred_period_return_50_Price <= 1 &
     Asset == 'AU200_AUD')|
  (
    state_space_LM_Pred_period_return_50_Price >=
    state_space_LM_Pred_period_return_50_Price_mean + 1.25*state_space_LM_Pred_period_return_50_Price_sd &
    state_space_LM_Pred_period_return_50_Price <=
    state_space_LM_Pred_period_return_50_Price_mean + 2*state_space_LM_Pred_period_return_50_Price_sd &
    state_space_LM_Pred_period_return_50_Price > 6 &
    Asset == 'AU200_AUD'
  )|
  (
    state_space_GLM_Pred_period_return_50_Price >=
    state_space_GLM_Pred_period_return_50_Price_mean + 0.25*state_space_GLM_Pred_period_return_50_Price_sd &
    state_space_GLM_Pred_period_return_50_Price <=
    state_space_GLM_Pred_period_return_50_Price_mean + 1.25*state_space_GLM_Pred_period_return_50_Price_sd &
    AR_GLM_Pred_period_return_50_Price > 0.65 &
    Asset == 'AU200_AUD'
  )|
  #  # #stop_factor = 3, profit_factor = 6, period = 12
  (state_space_GLM_Pred_period_return_50_Price >= 0.5 &
   state_space_GLM_Pred_period_return_50_Price <= 0.57 &
   Asset == 'EU50_EUR')|
    (
    AR_LM_Pred_period_return_50_Price >=
    AR_LM_Pred_period_return_50_Price_mean + 0*AR_LM_Pred_period_return_50_Price_sd &
    AR_LM_Pred_period_return_50_Price <=
    AR_LM_Pred_period_return_50_Price_mean + 10*AR_LM_Pred_period_return_50_Price_sd &
    AR_LM_Pred_period_return_50_Price >= 0 &
    Asset == 'EU50_EUR'
   )|
  (state_space_LM_Pred_period_return_50_Price > 0 &
    AR_LM_Pred_period_return_50_Price > 0 &
    Asset == 'EU50_EUR')|

  #Stop = 4, Profit = 8, Period = 24
  (state_space_GLM_Pred_period_return_50_Price >= 0.7 &
  state_space_GLM_Pred_period_return_50_Price <= 1 &
  Asset == 'USD_JPY')|

    #Stop = 5, Profit = 10, Period = 24
  (state_space_LM_Pred_period_return_50_Price >= 4 &
  state_space_LM_Pred_period_return_50_Price <= 9 &
  Asset == 'NZD_USD')|

  #Stop = 4, Profit = 8, Period = 24
  (state_space_GLM_Pred_period_return_50_Price >= 0.8 &
  state_space_GLM_Pred_period_return_50_Price <= 0.9 &
  Asset == 'SPX500_USD')|
  (AR_LM_Pred_period_return_50_Price >= 3 &
  AR_LM_Pred_period_return_50_Price <= 4 &
  Asset == 'SPX500_USD')

  "
