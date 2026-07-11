trade_statment_currency <-
  "(predicted < 70 & predicted > 57)|
  (pred_10000_mean_roll_250 < 19 & pred_10000_mean_roll_250 > 10)|
  (pred_10000_mean_roll_500 < 18 & pred_10000_mean_roll_500 > 10)|
  (pred_10000_mean_roll_600 < 20 & pred_10000_mean_roll_600 > 13)|
  (pred_10000_mean_roll_1000 < 30 & pred_10000_mean_roll_1000 > 25)|
  (pred_10000_mean_roll_1500 < 1000 & pred_10000_mean_roll_1500 > 12)|
  (pred_10000_mean_roll_2000 < 1000 & pred_10000_mean_roll_2000 > 6)"

trade_statment_Equity_1 <-
  "(predicted > 40)|(pred_10000_mean_roll_250 > 26.5)|

   (pred_10000_mean_roll_500 > 15)|(pred_10000_mean_roll_600 > 12.5)|

   (pred_10000_mean_roll_100 > 32.5)|(pred_10000_mean_roll_1000 > 10)|

   (pred_10000_mean_roll_100 > pred_10000_mean_roll_1000 + 1.25*pred_10000_sd_roll_1000)|

   (pred_10000_mean_roll_50 < pred_10000_mean_roll_1000 + 1.25*pred_10000_sd_roll_1000 &
   pred_10000_mean_roll_50 > pred_10000_mean_roll_1000 + 0.95*pred_10000_sd_roll_1000)|

   (pred_10000_mean_roll_10 > 40)|

   (pred_10000_mean_roll_1500 < 10 & pred_10000_mean_roll_1500 > 7.5)|

   (
    pred_10000_mean_roll_10 > pred_10000_mean_roll_1500 &
    pred_10000_mean_roll_10 > pred_10000_mean_roll_1000 &
    pred_10000_mean_roll_10 > pred_10000_mean_roll_2000 &
    pred_10000_mean_roll_10 > pred_10000_mean_roll_600 &
    pred_10000_mean_roll_10 > 30 &
    pred_10000_mean_roll_1500 > 1 & pred_10000_mean_roll_1000 > 1 & pred_10000_mean_roll_2000 > 1
   )
"

trade_statment_Equity_2 <-
  "(predicted < 65 & predicted > 35)|
  (pred_10000_mean_roll_1000 < 3 & pred_10000_mean_roll_1000 > 1)|
  (pred_10000_mean_roll_600 < 20 & pred_10000_mean_roll_600 > 13)|
(pred_10000_mean_roll_500 < 18 & pred_10000_mean_roll_500 > 13)|
(pred_10000_mean_roll_50 < 60 & pred_10000_mean_roll_50 > 40)|
(pred_10000_mean_roll_100 < 60 & pred_10000_mean_roll_100 > 30)|
(predicted > pred_10000_mean_roll_1000 &
  predicted > pred_10000_mean_roll_600 &
  predicted > pred_10000_mean_roll_250 &
  predicted > pred_10000_mean_roll_500 &
  predicted > pred_10000_mean_roll_100 &
   pred_10000_mean_roll_100 > 10 &
   predicted > 10)"

trade_statment_Currency_USD_EUR_ONLY <-
  "(predicted < 1000 & predicted > 70)|
   (pred_10000_mean_roll_250 < 1000 & pred_10000_mean_roll_250 > 60)|
(pred_10000_mean_roll_100 < 1000 & pred_10000_mean_roll_100 > 95)|
(predicted > pred_10000_mean_roll_2000 + 1.55*pred_10000_sd_roll_2000)|
(predicted > pred_10000_mean_roll_1500 + 1.45*pred_10000_sd_roll_1500)|
(predicted > pred_10000_mean_roll_600 + 1.8*pred_10000_sd_roll_600)|
(predicted > pred_10000_mean_roll_1000 + 1.4*pred_10000_sd_roll_1000)|
(predicted > pred_10000_mean_roll_2000 + 1.8*pred_10000_sd_roll_2000 )|
(pred_10000_mean_roll_50 < 1000 & pred_10000_mean_roll_50 > 90 )|
(pred_10000_mean_roll_10 < 1000 & pred_10000_mean_roll_10 > 90 )
"

trade_statment_EQUITY_ONLY_SIG_1 <-
  "(pred_10000_mean_roll_250 < 45 & pred_10000_mean_roll_250 > 31)|
   (pred_10000_mean_roll_500 < 60 & pred_10000_mean_roll_500 > 47)|
   (pred_10000_mean_roll_100 < 100 & pred_10000_mean_roll_100 > 70)|
   (pred_10000_mean_roll_1000 < 90 & pred_10000_mean_roll_1000 > 75)|
   (pred_10000_mean_roll_50 < 60 & pred_10000_mean_roll_50 > 45)|
   (pred_10000_mean_roll_10 < 80 & pred_10000_mean_roll_10 > 55)|
   (predicted > pred_10000_mean_roll_2000 &
   predicted > pred_10000_mean_roll_1500 &
   predicted > pred_10000_mean_roll_1000 &
   predicted > pred_10000_mean_roll_500 &
   predicted > pred_10000_mean_roll_250 &
   predicted > pred_10000_mean_roll_50 &
   predicted < 100 & predicted > 30)"

trade_statment_EQUITY_ONLY_SIG_01_risk_5 <-
  "(predicted > 13 & predicted < 31)|
  (predicted > pred_10000_mean_roll_1500 + 0.45*pred_10000_sd_roll_1500 &
    predicted < pred_10000_mean_roll_1500 + 1*pred_10000_sd_roll_1500)|
    (predicted > pred_10000_mean_roll_250 + 0.825*pred_10000_sd_roll_250 &
    predicted < pred_10000_mean_roll_250 + 1.5*pred_10000_sd_roll_250)|
    (predicted > pred_10000_mean_roll_500 + 0.9*pred_10000_sd_roll_500 &
    predicted < pred_10000_mean_roll_500 + 1.7*pred_10000_sd_roll_500)|
    (predicted > pred_10000_mean_roll_2000 + 0.4*pred_10000_sd_roll_2000 &
    predicted < pred_10000_mean_roll_2000 + 0.85*pred_10000_sd_roll_2000)|
    (pred_10000_mean_roll_100 > 20 & pred_10000_mean_roll_100 < 35)|
    (pred_10000_mean_roll_10 > 18 & pred_10000_mean_roll_10 < 30)|
    (pred_10000_mean_roll_10 > pred_10000_mean_roll_1500 + 0.6*pred_10000_sd_roll_1500 &
    pred_10000_mean_roll_10 < pred_10000_mean_roll_1500 + 0.75*pred_10000_sd_roll_1500)|
    (pred_10000_mean_roll_10 > pred_10000_mean_roll_2000 + 1.25*pred_10000_sd_roll_2000 &
    pred_10000_mean_roll_10 < pred_10000_mean_roll_2000 + 2*pred_10000_sd_roll_2000)
"

trade_statment_CURRENCY_Rest_of_World_SIG_01 <-
  "
  (predicted < 60 & predicted > 38 )|
  (pred_10000_mean_roll_10 < 50 & pred_10000_mean_roll_10 > 38 )|
  (predicted > pred_10000_mean_roll_1000 + 1.75*pred_10000_sd_roll_1000 &
   predicted < pred_10000_mean_roll_1000 + 2.75*pred_10000_sd_roll_1000)|
  (predicted > pred_10000_mean_roll_2000 + 1.15*pred_10000_sd_roll_2000 &
   predicted < pred_10000_mean_roll_2000 + 2.75*pred_10000_sd_roll_2000)|
   (predicted > pred_10000_mean_roll_1500 + 1.4*pred_10000_sd_roll_1500 &
   predicted < pred_10000_mean_roll_1500 + 2*pred_10000_sd_roll_1500)|
   (pred_10000_mean_roll_250 > 15 & pred_10000_mean_roll_250 < 25)|
   (pred_10000_mean_roll_100 > 27 & pred_10000_mean_roll_100 < 1000)|
   (pred_10000_mean_roll_50 > 24 & pred_10000_mean_roll_50 < 29)
"

trade_statment_COMMODODITIES_ONLY_5dollar_SIG01 <-
  "
(pred_10000_mean_roll_10 < pred_10000_mean_roll_1500 + 1.4*pred_10000_sd_roll_1500 &
  pred_10000_mean_roll_10 > pred_10000_mean_roll_1500 + 1*pred_10000_sd_roll_1500)|
  (predicted < pred_10000_mean_roll_2000 + 1.5*pred_10000_sd_roll_2000 &
  predicted > pred_10000_mean_roll_2000 + 0.9*pred_10000_sd_roll_2000)|
  (predicted < pred_10000_mean_roll_1000 + 2.2*pred_10000_sd_roll_1000 &
  predicted > pred_10000_mean_roll_1000 + 1.7*pred_10000_sd_roll_1000)|
  (pred_10000_mean_roll_10 < pred_10000_mean_roll_500 + 10*pred_10000_sd_roll_500 &
  pred_10000_mean_roll_10 > pred_10000_mean_roll_500 + 1.9*pred_10000_sd_roll_500)|
  (pred_10000_mean_roll_10 < pred_10000_mean_roll_250 + 10*pred_10000_sd_roll_250 &
  pred_10000_mean_roll_10 > pred_10000_mean_roll_250 + 1.9*pred_10000_sd_roll_250)|
  (pred_10000_mean_roll_10 < pred_10000_mean_roll_100 + 1.8*pred_10000_sd_roll_100 &
  pred_10000_mean_roll_10 > pred_10000_mean_roll_100 + 1.55*pred_10000_sd_roll_100)|
  (pred_10000_mean_roll_10 < pred_10000_mean_roll_50 + 1.4*pred_10000_sd_roll_50 &
  pred_10000_mean_roll_10 > pred_10000_mean_roll_50 + 1*pred_10000_sd_roll_50)|
  (pred_10000_mean_roll_50 < pred_10000_mean_roll_250 + 1.7*pred_10000_sd_roll_250 &
  pred_10000_mean_roll_50 > pred_10000_mean_roll_250 + 1.3*pred_10000_sd_roll_250)|
  (pred_10000_mean_roll_50 < pred_10000_mean_roll_600 + 1000*pred_10000_sd_roll_600 &
  pred_10000_mean_roll_50 > pred_10000_mean_roll_600 + 1.65*pred_10000_sd_roll_600)
"
