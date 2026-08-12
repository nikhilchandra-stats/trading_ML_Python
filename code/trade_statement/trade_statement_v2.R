trade_statement <-
  "
  (
  pred_LM_period_return_40_Price >
            mean_50_pred_LM_period_return_40_Price + sd_500_pred_LM_period_return_40_Price*0.2 &
  pred_LM_period_return_40_Price <
            mean_50_pred_LM_period_return_40_Price + sd_500_pred_LM_period_return_40_Price*0.5 &
  Asset == 'EUR_USD'
  )|
  (
  pred_LM_period_return_50_Price >
            mean_50_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*0.35 &
  pred_LM_period_return_50_Price <
            mean_50_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*0.7 &
  Asset == 'EUR_USD'
  )|
  (
  pred_technical_6 >= 0.675 &
  pred_technical_6 <= 0.7 &
  Asset == 'EUR_USD'
  )|
  (
  Averaged_Multi_prob_macro_GLM >= 0.55 &
  Averaged_Multi_prob_macro_GLM <= 0.99 &
  Asset == 'EUR_USD'
  )|
  (
  Averaged_Multi_prob_Momentum_Marco > 0.52 &
  Averaged_Multi_prob_Momentum_Marco < 0.55 &
  Asset == 'EUR_USD'
  )|
  (
  pred_GLM_period_return_50_Price > 0.56 &
  pred_GLM_period_return_50_Price < 0.99 &
  Asset == 'EUR_USD'
  )|
  (
  pred_GLM_period_return_50_Price > 0.52 &
  pred_GLM_period_return_50_Price < 0.6 &
  Asset == 'EU50_EUR'
  )|
  (
  pred_GLM_period_return_40_Price > 0.54 &
  pred_GLM_period_return_40_Price < 0.59 &
  Asset == 'EU50_EUR'
  )|
  (
  pred_LM_period_return_50_Price > 4 &
  pred_LM_period_return_50_Price < 6 &
  Asset == 'EU50_EUR'
  )|
  (
  pred_combined_6 >= 0.99999999999 &
  pred_combined_6 <= 1 &
  Asset == 'EU50_EUR'
  )|
  (
  pred_combined_4 >= 0.9999 &
  pred_combined_4 <= 1 &
  Asset == 'EU50_EUR'
  )|
  (
  pred_index_6 >= 0.545 &
  pred_index_6 <= 0.57 &
  Asset == 'EU50_EUR'
  )|
  (
  pred_GLM_period_return_50_Price > 0.9 &
  pred_GLM_period_return_50_Price < 0.95 &
  Asset == 'SPX500_USD'
  )|
  (
  pred_LM_period_return_50_Price >
            mean_50_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*1.5 &
  pred_LM_period_return_50_Price <
            mean_50_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*99 &
  Asset == 'SPX500_USD'
  )|
  (
  pred_combined_6 >= 0.9999 &
  Averaged_FULL_GLM > 0.5 &
  Averaged_FULL_GLM < 0.55 &
  Asset == 'SPX500_USD'
  )|
  (
  pred_daily_6 > 0.9 &
  pred_daily_6 < 0.99999999 &
  Asset == 'SPX500_USD'
  )|
  (
  pred_technical_6 > 0.5 &
  pred_technical_6 < 1 &
  Asset == 'SPX500_USD'
  )|
  (
  pred_index_2 > 0.99 &
  pred_index_2 < 0.9999 &
  Asset == 'SPX500_USD'
  )|
  (
  pred_GLM_period_return_50_Price > 0.85 &
  pred_GLM_period_return_50_Price < 0.9 &
  Asset == 'US2000_USD'
  )|
  (
   Averaged_FULL_LM >= 95 &
   Averaged_FULL_LM <= 1000 &
   Asset == 'US2000_USD'
  )|
  (
  pred_technical_6 > 0.73 &
  pred_technical_6 < 0.77 &
  Asset == 'US2000_USD'
  )|
  (
  pred_index_2 > 0.5 &
  pred_index_2 < 0.6 &
  pred_index_4 > 0.5 &
  pred_index_4 < 0.6 &
  Asset == 'US2000_USD'
  )|
  (
  pred_combined_6 >= 0.5 &
  pred_combined_6 < 0.9 &
  Asset == 'US2000_USD'
  )|
  (
  pred_combined_2 >= 0.65 &
  pred_combined_2 < 0.98 &
  Asset == 'US2000_USD'
  )|

  (
  pred_GLM_period_return_50_Price > 0.85 &
  pred_GLM_period_return_50_Price < 0.9 &
  Asset == 'USB10Y_USD'
  )|
  (
  pred_technical_6 >= 0.75 &
  pred_technical_6 <= 0.86 &
  Asset == 'USB10Y_USD'
  )|
  (
  pred_index_6 >= 0.96 &
  Asset == 'USB10Y_USD'
  )|
  (
  pred_index_4 >= 0.55 &
  Asset == 'USB10Y_USD'
  )|
   (
   pred_LM_period_return_50_Price > mean_50_pred_LM_period_return_50_Price &
   pred_LM_period_return_50_Price > mean_500_pred_LM_period_return_50_Price &
   pred_LM_period_return_50_Price > mean_200_pred_LM_period_return_50_Price &
   pred_LM_period_return_50_Price > mean_100_pred_LM_period_return_50_Price &
   pred_GLM_period_return_50_Price > mean_50_pred_GLM_period_return_50_Price &
   pred_GLM_period_return_50_Price > mean_500_pred_GLM_period_return_50_Price &
   pred_GLM_period_return_50_Price > mean_200_pred_GLM_period_return_50_Price &
   pred_GLM_period_return_50_Price > mean_100_pred_GLM_period_return_50_Price &
   mean_100_pred_LM_period_return_50_Price > mean_200_pred_LM_period_return_50_Price &
   mean_100_pred_LM_period_return_50_Price > mean_500_pred_LM_period_return_50_Price &
   mean_200_pred_LM_period_return_50_Price > mean_500_pred_LM_period_return_50_Price &
   mean_50_pred_LM_period_return_50_Price > mean_200_pred_LM_period_return_50_Price &
   mean_50_pred_LM_period_return_50_Price > mean_500_pred_LM_period_return_50_Price &
   mean_100_pred_GLM_period_return_50_Price > mean_200_pred_GLM_period_return_50_Price &
   mean_100_pred_GLM_period_return_50_Price > mean_500_pred_GLM_period_return_50_Price &
   Asset == 'USB10Y_USD'
   )|

  (
  pred_LM_period_return_50_Price >
            mean_50_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*1.25 &
  Asset == 'USD_JPY'
  )|
  (
  pred_GLM_period_return_50_Price >= 0.6 &
  pred_GLM_period_return_50_Price < 0.675 &
  Asset == 'USD_JPY'
  )|
  (
  pred_GLM_period_return_40_Price >= 0.61 &
  pred_GLM_period_return_40_Price < 0.65 &
  Asset == 'USD_JPY'
  )|
  (
  pred_technical_6 >= 0.575 &
  pred_technical_6 <= 1 &
  Asset == 'USD_JPY'
  )|
  (
  pred_combined_6 >= 0.9 &
  pred_combined_6 <= 0.95 &
  Asset == 'USD_JPY'
  )|
  (
  pred_combined_4 >= 0.65 &
  pred_combined_4 <= 1 &
  Asset == 'USD_JPY'
  )|
  (
  pred_index_5 >= 0.25 &
  pred_index_5 <= 0.9 &
  Asset == 'USD_JPY'
  )|

  (
  pred_LM_period_return_50_Price <
            mean_500_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*1.65 &
  pred_LM_period_return_50_Price >
            mean_500_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*1.1 &
  Asset == 'AUD_USD'
  )|
  (
  pred_GLM_period_return_40_Price >= 0.999997 &
  pred_GLM_period_return_40_Price <= 0.999999 &
  Asset == 'AUD_USD'
  )|
  (
  pred_technical_6 >= 0.8 &
  pred_technical_6 <= 1 &
  Asset == 'AUD_USD'
  )|
  (
  Averaged_Multi_prob_macro_GLM >= 0.55 &
  Averaged_Multi_prob_macro_GLM <= 0.6 &
  Asset == 'AUD_USD'
  )|
  (
  pred_index_6 > 0.7 &
  pred_index_6 < 1 &
  Asset == 'AUD_USD'
  )|
  (
  pred_index_4 > 0.99 &
  pred_index_4 < 1 &
  Asset == 'AUD_USD'
  )|
  (
  pred_daily_6 > 0.65 &
  pred_daily_6 < 0.75 &
  Asset == 'AUD_USD'
  )|
  (
  pred_daily_4 > 0.85 &
  pred_daily_4 < 1 &
  Asset == 'AUD_USD'
  )|


 (
 pred_GLM_period_return_50_Price >= 0.99 &
 pred_GLM_period_return_50_Price < 0.999 &
 Asset == 'EUR_GBP'
 )|
 (
 pred_combined_6 >= 0.99999999999 &
 pred_combined_6 <= 1 &
 Asset == 'EUR_GBP'
 )|
 (
 pred_technical_6 >= 0.7 &
 pred_technical_6 <= 1 &
 Asset == 'EUR_GBP'
 )|
 (
 pred_technical_4 >= 0.675 &
 pred_technical_4 <= 1 &
 Asset == 'EUR_GBP'
 )|
 (
 pred_technical_5 >= 7.5 &
 pred_technical_5 <= 1000 &
 Asset == 'EUR_GBP'
 )|
 (
 pred_technical_3 >= 6 &
 pred_technical_3 <= 1000 &
 Asset == 'EUR_GBP'
 )|
 (
 pred_daily_6 >= 0.925 &
 pred_daily_6 < 1 &
 Asset == 'EUR_GBP'
 )|
 (
 pred_daily_4 >= 0.925 &
 pred_daily_4 < 1 &
 Asset == 'EUR_GBP'
 )|
 (
 pred_daily_2 >= 0.9125 &
 pred_daily_2 < 1 &
 Asset == 'EUR_GBP'
 )|
 (
  pred_LM_period_return_50_Price >
            mean_50_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*2 &
  Asset == 'EUR_GBP'
  )|
 (
  mean_50_pred_GLM_period_return_50_Price >
    mean_100_pred_GLM_period_return_50_Price + 0.485*sd_100_pred_GLM_period_return_50_Price &
  mean_50_pred_GLM_period_return_50_Price <
    mean_100_pred_GLM_period_return_50_Price + 0.7*sd_100_pred_GLM_period_return_50_Price &
  mean_3_pred_GLM_period_return_50_Price > mean_200_pred_GLM_period_return_50_Price &
  mean_3_pred_GLM_period_return_50_Price > mean_100_pred_GLM_period_return_50_Price &
  Asset == 'EUR_GBP'
  )|
 (
  mean_3_pred_GLM_period_return_50_Price < mean_200_pred_GLM_period_return_50_Price &
  mean_3_pred_GLM_period_return_50_Price > mean_100_pred_GLM_period_return_50_Price &
  mean_200_pred_GLM_period_return_50_Price < mean_500_pred_GLM_period_return_50_Price &
  mean_100_pred_GLM_period_return_50_Price < mean_500_pred_GLM_period_return_50_Price &
  Asset == 'EUR_GBP'
 )|
  (
  pred_combined_6 >= 0.5 &
  pred_combined_6 <= 0.9999 &
  pred_combined_4 >= 0.5 &
  pred_GLM_period_return_50_Price > mean_100_pred_GLM_period_return_50_Price &
  Asset == 'EUR_GBP'
  )|
 (
 pred_technical_6 >= 0.7 &
 pred_technical_6 < 1 &
 pred_technical_4 >= 0.65 &
 pred_technical_4 < 1 &
 pred_technical_2 >= 0.65 &
 pred_technical_2 < 1 &
 Asset == 'EUR_GBP'
 )|
   (
  pred_LM_period_return_50_Price >
            mean_50_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*0.25 &
  pred_LM_period_return_50_Price <
            mean_50_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*0.4 &
  Asset == 'AU200_AUD'
  )|
 (
  mean_50_pred_GLM_period_return_50_Price >
    mean_100_pred_GLM_period_return_50_Price + 0.59*sd_100_pred_GLM_period_return_50_Price &
  mean_50_pred_GLM_period_return_50_Price <
    mean_100_pred_GLM_period_return_50_Price + 0.7*sd_100_pred_GLM_period_return_50_Price &
  mean_3_pred_GLM_period_return_50_Price > mean_200_pred_GLM_period_return_50_Price &
  Asset == 'AU200_AUD'
  )|
  (
  pred_combined_6 >= 0.5 &
  pred_combined_6 <= 0.9999 &
  pred_combined_4 >= 0.5 &
  pred_GLM_period_return_50_Price > mean_100_pred_GLM_period_return_50_Price &
  Asset == 'AU200_AUD'
  )|
  (
  pred_technical_6 >= 0.6 &
  pred_technical_6 < 0.85 &
  pred_technical_4 >= 0.6 &
  pred_technical_4 < 0.725 &
  Asset == 'AU200_AUD'
  )|

 (
 pred_daily_4 >= 0.99999 &
 pred_daily_4 <= 0.999999 &
 Asset == 'EUR_AUD'
 )|
 (
 pred_daily_4 >= 0.68 &
 pred_daily_4 <= 0.7 &
 Asset == 'EUR_AUD'
 )|
 (
   pred_LM_period_return_50_Price >
            mean_500_pred_LM_period_return_50_Price + 2.25*sd_500_pred_LM_period_return_50_Price &
  pred_LM_period_return_50_Price <
            mean_500_pred_LM_period_return_50_Price + 20*sd_500_pred_LM_period_return_50_Price &
  Asset == 'EUR_AUD'
  )|
 (
 pred_technical_6 >= 0.6 &
 pred_technical_6 < 0.8 &
 pred_technical_4 >= 0.55 &
 pred_technical_4 < 0.65 &
 Asset == 'EUR_AUD'
 )|
 (
 pred_index_6 >= 0.8 &
 pred_index_6 <= 0.95 &
 Asset == 'EUR_AUD'
 )|
 (
 pred_GLM_period_return_50_Price > mean_50_pred_GLM_period_return_50_Price &
 pred_GLM_period_return_50_Price > mean_500_pred_GLM_period_return_50_Price &
 pred_GLM_period_return_50_Price > mean_200_pred_GLM_period_return_50_Price &
 pred_GLM_period_return_50_Price > mean_100_pred_GLM_period_return_50_Price &
 mean_100_pred_GLM_period_return_50_Price < mean_500_pred_GLM_period_return_50_Price &
 mean_200_pred_GLM_period_return_50_Price < mean_500_pred_GLM_period_return_50_Price &
 mean_100_pred_GLM_period_return_50_Price < mean_200_pred_GLM_period_return_50_Price &
 pred_LM_period_return_50_Price > mean_50_pred_LM_period_return_50_Price &
 pred_LM_period_return_50_Price > mean_500_pred_LM_period_return_50_Price &
 pred_LM_period_return_50_Price > mean_200_pred_LM_period_return_50_Price &
 pred_LM_period_return_50_Price > mean_100_pred_LM_period_return_50_Price &
 Asset == 'EUR_AUD'
 )|
  (
  pred_combined_6 >= 0.95 &
  pred_daily_6 > 0.95 &
  pred_index_6 > 0.95 &
  Asset == 'WTICO_USD'
  )|
 (
  pred_GLM_period_return_50_Price >
    mean_100_pred_GLM_period_return_50_Price + 1.25*sd_100_pred_GLM_period_return_50_Price &
  pred_GLM_period_return_50_Price <
    mean_100_pred_GLM_period_return_50_Price + 1.5*sd_100_pred_GLM_period_return_50_Price &
  mean_3_pred_GLM_period_return_50_Price > mean_200_pred_GLM_period_return_50_Price &
  mean_3_pred_GLM_period_return_50_Price > mean_100_pred_GLM_period_return_50_Price &
  Asset == 'WTICO_USD'
  )|
 (
 pred_technical_6 >= 0.52 &
 pred_technical_6 < 0.65 &
 pred_technical_4 >= 0.52 &
 pred_technical_4 < 0.6 &
 Asset == 'WTICO_USD'
 )|
 (
 pred_GLM_period_return_50_Price > mean_50_pred_GLM_period_return_50_Price &
 pred_GLM_period_return_50_Price > mean_500_pred_GLM_period_return_50_Price &
 pred_GLM_period_return_50_Price > mean_200_pred_GLM_period_return_50_Price &
 pred_GLM_period_return_50_Price > mean_100_pred_GLM_period_return_50_Price &
 mean_100_pred_GLM_period_return_50_Price < mean_500_pred_GLM_period_return_50_Price &
 mean_200_pred_GLM_period_return_50_Price < mean_500_pred_GLM_period_return_50_Price &
 mean_100_pred_GLM_period_return_50_Price < mean_200_pred_GLM_period_return_50_Price &
 Asset == 'WTICO_USD'
 )|

 (
  pred_LM_period_return_50_Price >
            mean_500_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*1.5 &
  pred_LM_period_return_50_Price <
            mean_500_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*10 &
  Asset == 'USD_CAD'
  )|
  (
  pred_LM_period_return_50_Price >
            mean_200_pred_LM_period_return_50_Price + sd_200_pred_LM_period_return_50_Price*1.75 &
  pred_LM_period_return_50_Price <
            mean_200_pred_LM_period_return_50_Price + sd_200_pred_LM_period_return_50_Price*10 &
  Asset == 'USD_CAD'
  )|
 (
  pred_LM_period_return_50_Price >
            mean_50_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*1.15 &
  pred_LM_period_return_50_Price <
            mean_50_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*10 &
  Asset == 'USD_CAD'
  )|
 (
 pred_technical_6 >= 0.6125 &
 pred_technical_6 < 1 &
 pred_technical_4 >= 0.6125 &
 pred_technical_4 < 1 &
 Asset == 'USD_CAD'
 )|
  (
  pred_GLM_period_return_50_Price > 0.99 &
  pred_combined_6 > 0.99 &
  pred_daily_6 > 0.5 &
  Asset == 'USD_CAD'
  )|
  (
  pred_combined_6 >= 0.999 &
  pred_GLM_period_return_50_Price > 0.5 &
  Asset == 'USD_CAD'
  )|
  (
  pred_combined_2 >= 0.85 &
  pred_combined_2 < 0.99 &
  Asset == 'GBP_USD'
  )|
  (
  pred_combined_6 >= 0.75 &
  pred_combined_6 < 0.9 &
  Asset == 'GBP_USD'
  )|
  (
  pred_GLM_period_return_50_Price > 0.57 &
  pred_GLM_period_return_50_Price < 0.6 &
  Asset == 'GBP_USD'
  )|
  (pred_daily_6 >= 0.5 &
  pred_daily_6 < 0.65 &
  Asset == 'GBP_USD')|
  (
  pred_combined_2 >= 0.99 &
  pred_combined_6 >= 0.99 &
  pred_combined_4 >= 0.99 &
  mean_100_pred_LM_period_return_50_Price < mean_200_pred_LM_period_return_50_Price &
  mean_3_pred_LM_period_return_50_Price < mean_100_pred_LM_period_return_50_Price &
  Asset == 'GBP_CAD'
  )|
  (
  pred_daily_6 >= 0.99 &
  pred_daily_4 >= 0.99 &
  pred_daily_2 >= 0.955 &
  Asset == 'GBP_CAD'
  )|
  (
  pred_index_6 >= 0.525 &
  pred_index_4 >= 0.525 &
  Asset == 'GBP_CAD'
  )|
  (
  pred_LM_period_return_50_Price >
            mean_50_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*1 &
  pred_LM_period_return_50_Price <
            mean_50_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*1.5 &
  Asset == 'EUR_JPY'
  )|
  (
  pred_combined_6 >= 0.99999999 &
  pred_combined_4 >= 0.99999999 &
  mean_100_pred_LM_period_return_50_Price < mean_500_pred_LM_period_return_50_Price &
  mean_50_pred_LM_period_return_50_Price < mean_200_pred_LM_period_return_50_Price &
  mean_50_pred_LM_period_return_50_Price < mean_500_pred_LM_period_return_50_Price &
  mean_100_pred_GLM_period_return_50_Price < mean_500_pred_GLM_period_return_50_Price &
  mean_50_pred_GLM_period_return_50_Price < mean_200_pred_GLM_period_return_50_Price &
  mean_50_pred_GLM_period_return_50_Price < mean_500_pred_GLM_period_return_50_Price &
  Asset == 'EUR_JPY'
  )|
  (
  pred_index_6 >= 0.8 &
  pred_index_6 <= 0.85 &
  pred_index_4 >= 0.675 &
  pred_index_4 <= 1 &
  Asset == 'EUR_JPY'
  )|
 (
 pred_LM_period_return_50_Price > mean_50_pred_LM_period_return_50_Price &
 pred_LM_period_return_50_Price > mean_500_pred_LM_period_return_50_Price &
 pred_LM_period_return_50_Price > mean_200_pred_LM_period_return_50_Price &
 pred_LM_period_return_50_Price > mean_100_pred_LM_period_return_50_Price &
 mean_200_pred_LM_period_return_50_Price < mean_500_pred_LM_period_return_50_Price &
 mean_50_pred_LM_period_return_50_Price < mean_200_pred_LM_period_return_50_Price &
 Asset == 'EUR_JPY'
 )|
   (
  pred_LM_period_return_50_Price >
            mean_50_pred_LM_period_return_50_Price + sd_200_pred_LM_period_return_50_Price*0.9 &
  pred_LM_period_return_50_Price <
            mean_50_pred_LM_period_return_50_Price + sd_200_pred_LM_period_return_50_Price*1.2 &
  Asset == 'EUR_NZD'
  )|
  (
  pred_combined_6 >= 0.99999999 &
  pred_combined_4 >= 0.99999999 &
  pred_combined_2 >= 0.9999 &
  mean_100_pred_LM_period_return_50_Price < mean_500_pred_LM_period_return_50_Price &
  mean_50_pred_LM_period_return_50_Price < mean_200_pred_LM_period_return_50_Price &
  mean_50_pred_LM_period_return_50_Price < mean_500_pred_LM_period_return_50_Price &
  mean_50_pred_LM_period_return_50_Price < mean_100_pred_LM_period_return_50_Price &
  mean_100_pred_GLM_period_return_50_Price < mean_500_pred_GLM_period_return_50_Price &
  mean_100_pred_GLM_period_return_50_Price < mean_200_pred_GLM_period_return_50_Price &
  mean_50_pred_GLM_period_return_50_Price < mean_200_pred_GLM_period_return_50_Price &
  mean_50_pred_GLM_period_return_50_Price < mean_500_pred_GLM_period_return_50_Price &
  mean_50_pred_GLM_period_return_50_Price < mean_100_pred_GLM_period_return_50_Price &
  Asset == 'EUR_NZD'
  )|
 (
 pred_LM_period_return_50_Price > mean_50_pred_LM_period_return_50_Price &
 pred_LM_period_return_50_Price > mean_500_pred_LM_period_return_50_Price &
 pred_LM_period_return_50_Price > mean_200_pred_LM_period_return_50_Price &
 pred_LM_period_return_50_Price > mean_100_pred_LM_period_return_50_Price &
 mean_200_pred_LM_period_return_50_Price < mean_500_pred_LM_period_return_50_Price &
 mean_50_pred_LM_period_return_50_Price < mean_200_pred_LM_period_return_50_Price &
 Asset == 'EUR_NZD'
 )|
 (
 pred_GLM_period_return_50_Price > 0.50 &
 pred_GLM_period_return_50_Price >= 0.6 &
 Asset == 'XAG_USD'
 )|
 (
 pred_GLM_period_return_40_Price > 0.65 &
 pred_GLM_period_return_40_Price <= 1 &
 Asset == 'XAG_USD'
 )|
 (
 Averaged_Multi_prob_Momentum > 0.5 &
 Averaged_Multi_prob_Momentum <= 1 &
 Asset == 'XAG_USD'
 )|
  (
  pred_LM_period_return_50_Price >
            mean_50_pred_LM_period_return_50_Price + sd_200_pred_LM_period_return_50_Price*1.25 &
  pred_LM_period_return_50_Price <
            mean_50_pred_LM_period_return_50_Price + sd_200_pred_LM_period_return_50_Price*2 &
  Asset == 'XAG_USD'
  )|
  (
  pred_LM_period_return_50_Price >
            mean_100_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*1 &
  pred_LM_period_return_50_Price <
            mean_100_pred_LM_period_return_50_Price + sd_500_pred_LM_period_return_50_Price*1.85 &
  Asset == 'XAG_USD'
  )|
  (
  pred_combined_6 >= 0.99999999 &
  pred_combined_4 >= 0.99999999 &
  pred_combined_2 >= 0.9999 &
  mean_100_pred_LM_period_return_50_Price < mean_500_pred_LM_period_return_50_Price &
  mean_50_pred_LM_period_return_50_Price < mean_200_pred_LM_period_return_50_Price &
  mean_50_pred_LM_period_return_50_Price < mean_500_pred_LM_period_return_50_Price &
  mean_50_pred_LM_period_return_50_Price < mean_100_pred_LM_period_return_50_Price &
  mean_100_pred_GLM_period_return_50_Price < mean_500_pred_GLM_period_return_50_Price &
  mean_100_pred_GLM_period_return_50_Price < mean_200_pred_GLM_period_return_50_Price &
  mean_50_pred_GLM_period_return_50_Price < mean_200_pred_GLM_period_return_50_Price &
  mean_50_pred_GLM_period_return_50_Price < mean_500_pred_GLM_period_return_50_Price &
  mean_50_pred_GLM_period_return_50_Price < mean_100_pred_GLM_period_return_50_Price &
  Asset == 'XAG_USD'
  )|
 (
 pred_LM_period_return_50_Price > mean_50_pred_LM_period_return_50_Price &
 pred_LM_period_return_50_Price > mean_500_pred_LM_period_return_50_Price &
 pred_LM_period_return_50_Price > mean_200_pred_LM_period_return_50_Price &
 pred_LM_period_return_50_Price > mean_100_pred_LM_period_return_50_Price &
 mean_200_pred_LM_period_return_50_Price < mean_500_pred_LM_period_return_50_Price &
 mean_50_pred_LM_period_return_50_Price < mean_200_pred_LM_period_return_50_Price &
 Asset == 'XAG_USD'
 )

  "

trade_statement_2 <-
  "
  #Stop Factor = 6, Profit Factor = 12, End Point = 24
  (
  pred_copula_3 >= -200 &
  pred_copula_3 <= -100 &
  Asset == 'EUR_USD'
  )|
  (
  pred_copula_1 >= 40 &
  pred_copula_1 <= 1000 &
  Asset == 'EUR_USD'
  )|
  (
  pred_copula_5 >= -250 &
  pred_copula_5 <= 0 &
  Asset == 'EUR_USD'
  )|

  #Stop Factor = 4, Profit Factor = 8, End Point = 24
  (
  pred_copula_1 > 50 &
  pred_copula_1 < 1000 &
  Asset == 'USD_JPY'
  )|
  (
  pred_copula_5 > 400 &
  pred_copula_5 < 1000 &
  Asset == 'USD_JPY'
  )|
  (
  pred_combined_5 > 375 &
  pred_combined_5 < 400 &
  Asset == 'USD_JPY'
  )|
  (
  pred_technical_6 > 0.6 &
  pred_technical_6 < 1 &
  Asset == 'USD_JPY'
  )|
  (
  pred_technical_4 > 0.7 &
  pred_technical_4 < 1 &
  Asset == 'USD_JPY'
  )|
  (
  pred_LM_period_return_50_Price > 3 &
  pred_LM_period_return_50_Price < 100 &
  Asset == 'USD_JPY'
  )|
  (
  pred_LM_period_return_24_Price > 1 &
  pred_LM_period_return_24_Price < 100 &
  Asset == 'USD_JPY'
  )|
  (
  pred_GLM_period_return_24_Price > 0.575 &
  pred_GLM_period_return_24_Price < 1 &
  Asset == 'USD_JPY'
  )|
  (
  mean_3_pred_LM_period_return_24_Price > mean_50_pred_LM_period_return_24_Price + 1.75*sd_50_pred_LM_period_return_24_Price &
  Asset == 'USD_JPY'
  )|
  (
  mean_3_pred_LM_period_return_24_Price > mean_100_pred_LM_period_return_24_Price + 2.25*sd_100_pred_LM_period_return_24_Price &
  Asset == 'USD_JPY'
  )|
  (
  mean_3_pred_LM_period_return_24_Price > mean_500_pred_LM_period_return_24_Price + 1*sd_500_pred_LM_period_return_24_Price &
  mean_3_pred_LM_period_return_24_Price < mean_500_pred_LM_period_return_24_Price + 1.5*sd_500_pred_LM_period_return_24_Price &
  Asset == 'USD_JPY'
  )|

  #Stop Factor = 6, Profit Factor = 12, End Point = 30
  (
  pred_copula_1 >= 5400 &
  pred_copula_1 <= 6000 &
  Asset == 'AUD_USD'
  )|
  (
  pred_copula_3 >= 15000 &
  pred_copula_3 <= 16000 &
  Asset == 'AUD_USD'
  )|
  (
  pred_copula_5 >= 5500 &
  pred_copula_5 <= 7000 &
  Asset == 'AUD_USD'
  )|

  #Stop Factor = 6, Profit Factor = 12, End Point = 24
  (
  pred_copula_3 >= 500 &
  pred_copula_3 <= 1500 &
  Asset == 'USD_CAD'
  )|
  (
  pred_copula_1 >= 1500 &
  pred_copula_1 <= 2000 &
  Asset == 'USD_CAD'
  )|
  (
  pred_copula_5 >= 1200 &
  pred_copula_5 <= 3000 &
  Asset == 'USD_CAD'
  )|

  #Stop Factor = 6, Profit Factor = 12, End Point = 24
  (
  pred_copula_1 >= 3.75 &
  pred_copula_1 <= 2000 &
  Asset == 'XAG_EUR'
  )|
  (
  pred_copula_5 >= -37 &
  pred_copula_5 <= -30.5 &
  Asset == 'XAG_EUR'
  )|

  #Stop Factor = 6, Profit Factor = 12, End Point = 24
  (
  pred_copula_1 >= 50 &
  pred_copula_1 <= 500 &
  Asset == 'GBP_USD'
  )|
  (
  pred_copula_3 >= 1840 &
  pred_copula_3 <= 2500 &
  Asset == 'GBP_USD'
  )|
  (
  pred_copula_3 >= 300 &
  pred_copula_3 <= 1000 &
  Asset == 'GBP_USD'
  )|
  (
  pred_copula_5 >= 2200 &
  pred_copula_5 <= 2500 &
  Asset == 'GBP_USD'
  )|

  #Stop Factor = 6, Profit Factor = 12, End Point = 24
  (
  pred_copula_1 >= -550 &
  pred_copula_1 <= -100 &
  Asset == 'WTICO_USD'
  )|
  (
  pred_copula_1 >= -700 &
  pred_copula_1 <= -650 &
  Asset == 'WTICO_USD'
  )|
  (
  pred_copula_3 >= -850 &
  pred_copula_3 <= -800 &
  Asset == 'WTICO_USD'
  )|
  (
  pred_copula_5 >= -600 &
  pred_copula_5 <= 2500 &
  Asset == 'WTICO_USD'
  )|
  (
  pred_technical_2 >= 0.6 &
  pred_technical_2 <= 0.65 &
  Asset == 'WTICO_USD'
  )|

  #Stop Factor = 4, Profit Factor = 8, End Point = 24
  (
  pred_daily_1 > 200 &
  pred_daily_1 < 1000 &
  Asset == 'SPX500_USD'
  )|
  (
  pred_daily_3 > 350 &
  pred_daily_3 < 1000 &
  Asset == 'SPX500_USD'
  )|
  (
  pred_daily_5 > 330 &
  pred_daily_5 < 1000 &
  Asset == 'SPX500_USD'
  )|
  (
  pred_combined_6 > 0.99999999 &
  pred_combined_6 < 1 &
  Asset == 'SPX500_USD'
  )|
  (
  pred_combined_2 > 0.95 &
  pred_combined_2 < 0.99 &
  Asset == 'SPX500_USD'
  )|
  (
  pred_combined_4 > 0.999 &
  pred_combined_4 < 0.99999 &
  Asset == 'SPX500_USD'
  )|
  (
  pred_daily_4 > 0.95 &
  pred_daily_4 < 0.99999999999 &
  Asset == 'SPX500_USD'
  )|
  (
  pred_copula_1 > 700 &
  pred_copula_1 < 780 &
  Asset == 'SPX500_USD'
  )|

  #Stop Factor = 3, Profit Factor = 6, End Point = 24
  (
  pred_copula_1 >= 155 &
  pred_copula_1 <= 200 &
  Asset == 'XAU_USD'
  )|
  (
  pred_copula_3 >= 570 &
  # pred_copula_3 <= 650 &
  pred_copula_3 <= 610 &
  Asset == 'XAU_USD'
  )|
  (
  pred_copula_5 >= 650 &
  pred_copula_5 <= 750 &
  Asset == 'XAU_USD'
  )|
  (
  pred_combined_1 >= 730 &
  pred_combined_1 <= 1000 &
  Asset == 'XAU_USD'
  )|
  (
  pred_GLM_period_return_24_Price >= 0.54 &
  pred_GLM_period_return_24_Price <= 0.56 &
  Asset == 'XAU_USD'
  )|
  (
  pred_combined_3 > 2000 &
  pred_combined_3 < 3000  &
  Asset == 'XAU_USD'
  )|
  (
  pred_combined_5 > 2000 &
  pred_combined_5 < 3000  &
  Asset == 'XAU_USD'
  )|
  (
  pred_daily_1 > 0 &
  pred_daily_1 < 0.5  &
  Asset == 'XAU_USD'
  )|
  (
  pred_daily_3 > 0.5 &
  pred_daily_3 < 3  &
  Asset == 'XAU_USD'
  )|
  (
  pred_daily_5 > 1.5 &
  pred_daily_5 < 5  &
  Asset == 'XAU_USD'
  )|
  (
  pred_technical_2 > 0.55 &
  pred_technical_2 < 0.7  &
  Asset == 'XAU_USD'
  )|
  (
  pred_technical_4 > 0.57 &
  pred_technical_4 < 0.78  &
  Asset == 'XAU_USD'
  )|
  (
  mean_3_pred_LM_period_return_24_Price > mean_50_pred_LM_period_return_24_Price + 2*sd_50_pred_LM_period_return_24_Price &
  Asset == 'XAU_USD'
  )|
  (
  mean_3_pred_LM_period_return_24_Price > mean_100_pred_LM_period_return_24_Price + 2.5*sd_100_pred_LM_period_return_24_Price &
  Asset == 'XAU_USD'
  )|
  (
  mean_3_pred_LM_period_return_24_Price > mean_200_pred_LM_period_return_24_Price + 1.25*sd_400_pred_LM_period_return_24_Price &
  mean_3_pred_LM_period_return_24_Price < mean_200_pred_LM_period_return_24_Price + 1.5*sd_400_pred_LM_period_return_24_Price &
  Asset == 'XAU_USD'
  )|

  #Stop Factor = 3, Profit Factor = 6, End Point = 24
  (
  pred_copula_1 >= 190 &
  pred_copula_1 <= 230 &
  Asset == 'HK33_HKD'
  )|
  (
  pred_combined_1 >= 140 &
  pred_combined_1 <= 200 &
  Asset == 'HK33_HKD'
  )|
  (
  pred_combined_3 >= 15 &
  pred_combined_3 <= 45 &
  Asset == 'HK33_HKD'
  )|
  (
  pred_combined_5 >= 3100 &
  pred_combined_5 <= 10000 &
  Asset == 'HK33_HKD'
  )|

  #Stop Factor = 5, Profit Factor = 10, End Point = 24
  (
  pred_technical_2 >= 0.6 &
  pred_technical_2 <= 1 &
  Asset == 'NATGAS_USD'
  )|
  (
  pred_technical_4 >= 0.51 &
  pred_technical_4 <= 0.57 &
  Asset == 'NATGAS_USD'
  )|
  (
  pred_technical_6 >= 0.61 &
  pred_technical_6 <= 0.7 &
  Asset == 'NATGAS_USD'
  )|
  (
  Averaged_FULL_LM >= -100 &
  Averaged_FULL_LM <= -31 &
  Asset == 'NATGAS_USD'
  )|
  (
  pred_daily_4 >= 0.53 &
  pred_daily_4 <= 0.65 &
  Asset == 'NATGAS_USD'
  )|
  (
  pred_daily_6 >= 0.52 &
  pred_daily_6 <= 0.59 &
  Asset == 'NATGAS_USD'
  )|

  #Stop Factor = 4, Profit Factor = 8, End Point = 24
  (
  pred_copula_2 >= 0.51 &
  pred_copula_2 <= 0.675 &
  Asset == 'EU50_EUR'
  )|
  (
  pred_copula_4 >= 0.4 &
  pred_copula_4 <= 0.5 &
  Asset == 'EU50_EUR'
  )|
  (
  pred_copula_6 >= 0.525 &
  pred_copula_6 <= 0.6 &
  Asset == 'EU50_EUR'
  )|
  (
  pred_GLM_period_return_40_Price >= 0.65 &
  pred_GLM_period_return_40_Price <= 0.7 &
  Asset == 'EU50_EUR'
  )|
    #Stop Factor = 4, Profit Factor = 8, End Point = 24
  #Stop Factor = 4, Profit Factor = 8, End Point = 24
  (
  pred_LM_period_return_40_Price <=
    mean_500_pred_LM_period_return_40_Price - 0.25*sd_500_pred_LM_period_return_40_Price  &
  pred_LM_period_return_40_Price <=
    mean_200_pred_LM_period_return_40_Price - 0.25*sd_200_pred_LM_period_return_40_Price  &
  pred_LM_period_return_40_Price >=
    mean_3_pred_LM_period_return_40_Price + 1*sd_3_pred_LM_period_return_40_Price  &
  Asset == 'UK100_GBP'
  )|
    (
  pred_LM_period_return_24_Price <=
    mean_500_pred_LM_period_return_24_Price - 1.5*sd_500_pred_LM_period_return_24_Price  &
  pred_LM_period_return_24_Price <=
    mean_200_pred_LM_period_return_24_Price - 0.25*sd_200_pred_LM_period_return_24_Price  &
  pred_LM_period_return_24_Price >=
    mean_3_pred_LM_period_return_24_Price + 0*sd_3_pred_LM_period_return_24_Price  &
  Asset == 'UK100_GBP'
  )|
  (
  pred_combined_5 >= 800   &
  pred_combined_5 <= 925 &
  Asset == 'UK100_GBP'
  )|
  (
  pred_technical_3 >= 6 &
  pred_technical_3 < 100 &
  Asset == 'UK100_GBP'
  )|
  (
  pred_technical_5 >= 10 &
  pred_technical_5 < 500 &
  Asset == 'UK100_GBP'
  )|

  #Stop Factor = 4, Profit Factor = 8, End Point = 24
  (
  pred_LM_period_return_40_Price >=
    mean_500_pred_LM_period_return_40_Price + 1.3*sd_500_pred_LM_period_return_40_Price &
  Asset == 'XCU_USD'
  )|
  (
  pred_LM_period_return_24_Price <=
    mean_500_pred_LM_period_return_24_Price - 1.5*sd_500_pred_LM_period_return_24_Price  &
  pred_LM_period_return_24_Price <=
    mean_200_pred_LM_period_return_24_Price - 0.25*sd_200_pred_LM_period_return_24_Price  &
  pred_LM_period_return_24_Price >=
    mean_3_pred_LM_period_return_24_Price + 0*sd_3_pred_LM_period_return_24_Price  &
  Asset == 'XCU_USD'
  )|
  (
  pred_LM_period_return_50_Price >=
    mean_500_pred_LM_period_return_50_Price + 1.3*sd_500_pred_LM_period_return_50_Price &
  Asset == 'XCU_USD'
  )|
  (
  pred_technical_1 >= 6 &
  pred_technical_1 <= 10 &
  Asset == 'XCU_USD'
  )|
  (
  pred_technical_3 >= 9 &
  pred_technical_3 <= 11 &
  Asset == 'XCU_USD'
  )|
  (
  pred_technical_5 >= 12 &
  pred_technical_5 <= 15.5 &
  Asset == 'XCU_USD'
  )|
    #Stop Factor = 4, Profit Factor = 8, End Point = 24
  (
  pred_combined_1 >= 0 &
  Asset == 'FR40_EUR'
  )|
  (
  pred_combined_3 >= 0 &
  Asset == 'FR40_EUR'
  )|
  (
  pred_combined_5 >= 0 &
  Asset == 'FR40_EUR'
  )|
  (
  pred_technical_5 < 5 &
  pred_technical_5 >= 1 &
  Asset == 'FR40_EUR'
  )|
  (
  pred_technical_1 < 100 &
  pred_technical_1 >= 2 &
  Asset == 'FR40_EUR'
  )|
  (
  pred_daily_5 < 100 &
  pred_daily_5 >= 6 &
  Asset == 'FR40_EUR'
  )|
  (
  pred_daily_1 < 100 &
  pred_daily_1 >= 6 &
  Asset == 'FR40_EUR'
  )


"
