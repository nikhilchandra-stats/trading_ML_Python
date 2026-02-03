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
      # state_space_LM_Pred_period_return_35_Price >
      #   state_space_LM_Pred_period_return_35_Price_mean + state_space_LM_Pred_period_return_35_Price_sd*1
      state_space_GLM_Pred_period_return_35_Price >
        state_space_GLM_Pred_period_return_35_Price_mean + state_space_GLM_Pred_period_return_35_Price_sd*0.25
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
  facet_wrap(.~ trade_col) +
  theme(legend.position = "bottom")

#-------------------------------------------------------
indicator_mapping <- list(
  Asset = c("EUR_CHF", #1 EUR_CHF
            "EUR_SEK" , #2 EUR_SEK
            "GBP_CHF", #3 GBP_CHF
            "GBP_JPY", #4 GBP_JPY
            "USD_CZK", #5 USD_CZK
            "USD_NOK" , #6 USD_NOK
            "XAG_CAD", #7 XAG_CAD
            "XAG_CHF", #8 XAG_CHF
            "XAG_JPY" , #9 XAG_JPY
            "GBP_NZD" , #10 GBP_NZD
            "NZD_CHF" , #11 NZD_CHF
            "USD_MXN" , #12 USD_MXN
            "XPD_USD" , #13 XPD_USD
            "XPT_USD" , #14 XPT_USD
            "NATGAS_USD" , #15 NATGAS_USD
            "SG30_SGD" , #16 SG30_SGD
            "SOYBN_USD" , #17 SOYBN_USD
            "WHEAT_USD" , #18 WHEAT_USD
            "SUGAR_USD" , #19 SUGAR_USD
            "DE30_EUR" , #20 DE30_EUR
            "UK10YB_GBP" , #21 UK10YB_GBP
            "JP225_USD" , #22 JP225_USD
            "CH20_CHF" , #23 CH20_CHF
            "NL25_EUR" , #24 NL25_EUR
            "XAG_SGD" , #25 XAG_SGD
            "BCH_USD" , #26 BCH_USD
            "LTC_USD" ), #27 LTC_USD
  couplua_assets =
    list(
      c(
        "EUR_SEK", "DE30_EUR", "XAG_CHF", "EUR_USD", "EU50_EUR", "XAG_EUR", "XAU_EUR",
        "EUR_AUD", "EUR_JPY", "FR40_EUR", "GBP_CHF", "NZD_CHF", "CH20_CHF", "XAU_USD"
      ) %>% unique() , #1 EUR_CHF

      c("EUR_CHF", "DE30_EUR", "NL25_EUR", "EUR_USD", "EU50_EUR", "XAG_EUR", "XAU_EUR",
        "EUR_AUD", "EUR_JPY", "FR40_EUR", "XAU_USD") %>% unique(), #2 EUR_SEK

      c("GBP_JPY", "GBP_NZD", "UK10YB_GBP", "GBP_USD", "GBP_AUD", "XAG_GBP", "XAU_GBP",
        "UK100_GBP", "EUR_JPY", "FR40_EUR", "EUR_USD",  "EUR_CHF", "NZD_CHF", "CH20_CHF",
        "XAU_USD") %>% unique(), #3 GBP_CHF

      c("GBP_CHF", "GBP_NZD", "UK10YB_GBP", "GBP_USD", "GBP_AUD", "XAG_GBP", "XAU_GBP",
        "UK100_GBP", "XAG_JPY", "USD_JPY", "EUR_JPY", "XAU_JPY", "XAU_USD") %>% unique(), #4 GBP_JPY

      c("USD_NOK", "EUR_USD", "USD_JPY", "AUD_USD", "XAG_USD", "XAU_USD", "GBP_USD",
        "USD_CAD", "USD_SEK", "NZD_USD", "EUR_SEK") %>% unique(), #5 USD_CZK

      c("USD_NOK", "EUR_USD", "USD_JPY", "AUD_USD", "XAG_USD", "XAU_USD", "GBP_USD",
        "USD_CAD", "USD_SEK", "NZD_USD", "EUR_SEK") %>% unique(), #6 USD_NOK

      c("XAG_CHF", "XAG_JPY", "XAG_SGD", "XAG_USD", "XAG_EUR", "XAG_GBP", "XAG_NZD",
        "XAG_AUD", "XAU_USD", "XAU_EUR", "XAU_GBP",
        "XAU_SGD", "XAU_CAD", "XAU_NZD", "XAU_AUD") %>% unique(), #7 XAG_CAD

      c("XAG_CAD", "XAG_JPY", "XAG_SGD", "XAG_USD", "XAG_EUR", "XAG_GBP", "XAG_NZD",
        "XAG_AUD", "XAU_USD", "XAU_EUR", "XAU_GBP",
        "XAU_SGD", "XAU_CAD", "XAU_NZD", "XAU_AUD") %>% unique(), #8 XAG_CHF

      c("XAG_CAD", "XAG_CHF", "XAG_SGD", "XAG_USD", "XAG_EUR", "XAG_GBP", "XAG_NZD",
        "XAG_AUD", "XAU_USD", "XAU_EUR", "XAU_GBP",
        "XAU_SGD", "XAU_CAD", "XAU_NZD", "XAU_AUD") %>% unique(), #9 XAG_JPY

      c("GBP_CHF", "GBP_JPY", "UK10YB_GBP", "GBP_USD", "GBP_AUD", "XAG_GBP", "XAU_GBP",
        "UK100_GBP", "NZD_CHF", "NZD_USD", "XAU_NZD", "XAG_NZD") %>% unique(), #10 GBP_NZD

      c( "GBP_NZD", "NZD_USD", "XAU_NZD", "XAG_NZD", "EUR_CHF", "GBP_CHF", "XAG_CHF",
         "CH20_CHF") %>% unique(), #11 NZD_CHF

      c( "USD_NOK", "EUR_USD", "USD_JPY", "AUD_USD", "XAG_USD", "XAU_USD", "GBP_USD",
         "USD_CAD", "USD_SEK", "NZD_USD") %>% unique(), #12 USD_MXN

      c( "USD_NOK", "EUR_USD", "USD_JPY", "AUD_USD", "XAG_USD", "XAU_USD", "GBP_USD",
         "USD_CAD", "USD_SEK", "NZD_USD",
         "NATGAS_USD", "XPT_USD", "USB10Y_USD") %>% unique(), #13 XPD_USD

      c("USD_NOK", "EUR_USD", "USD_JPY", "AUD_USD", "XAG_USD", "XAU_USD", "GBP_USD",
        "USD_CAD", "USD_SEK", "NZD_USD",
        "NATGAS_USD", "XPD_USD", "USB10Y_USD") %>% unique(), #14 XPT_USD

      c(
        "USD_NOK", "EUR_USD", "USD_JPY", "AUD_USD", "XAG_USD", "XAU_USD", "GBP_USD",
        "USD_CAD", "USD_SEK", "NZD_USD",
        "XPT_USD", "XPD_USD", "USB10Y_USD"
      ) %>% unique(), #15 NATGAS_USD

      c("USB10Y_USD", "USD_SGD", "XAU_SGD", "XAG_SGD", "AU200_AUD", "US2000_USD", "SPX500_USD",
        "NL25_EUR", "NL25_EUR", "FR40_EUR", "EU50_EUR", "JP225_USD", "XPT_USD", "XAU_USD", "DE30_EUR",
        "CH20_CHF") %>% unique(), #16 SG30_SGD

      c(
        "USD_NOK", "EUR_USD", "USD_JPY", "AUD_USD", "XAG_USD", "XAU_USD", "GBP_USD",
        "USD_CAD", "USD_SEK", "NZD_USD", "NATGAS_USD", "XPT_USD", "USB10Y_USD", "WHEAT_USD",
        "SUGAR_USD","SPX500_USD", "US2000_USD"
      ) %>% unique(), #17 SOYBN_USD

      c(
        "USD_NOK", "EUR_USD", "USD_JPY", "AUD_USD", "XAG_USD", "XAU_USD", "GBP_USD",
        "USD_CAD", "USD_SEK", "NZD_USD", "NATGAS_USD", "XPT_USD", "USB10Y_USD", "SOYBN_USD",
        "SUGAR_USD","SPX500_USD", "US2000_USD"
      ) %>% unique(), #18 WHEAT_USD #####HERE

      c(
        "USD_NOK", "EUR_USD", "USD_JPY", "AUD_USD", "XAG_USD", "XAU_USD", "GBP_USD",
        "USD_CAD", "NZD_USD", "NATGAS_USD", "XPT_USD", "USB10Y_USD", "SOYBN_USD",
        "WHEAT_USD","SPX500_USD", "US2000_USD"
      ) %>% unique(), #19 SUGAR_USD

      c(
        "USB10Y_USD", "USD_SGD", "XAU_USD", "XAG_EUR", "AU200_AUD", "US2000_USD", "SPX500_USD",
        "NL25_EUR", "NL25_EUR", "FR40_EUR", "EU50_EUR", "JP225_USD", "XAG_USD",
        "CH20_CHF", "XAU_EUR", "EUR_USD"
      ) %>% unique(), #20 DE30_EUR

      c(
        "XAG_GBP", "AU200_AUD", "US2000_USD", "SPX500_USD",
        "NL25_EUR", "NL25_EUR", "FR40_EUR", "EU50_EUR", "JP225_USD", "XAG_USD",
        "CH20_CHF", "XAU_GBP", "GBP_USD", "UK100_GBP"
      ) %>% unique(), #21 UK10YB_GBP

      c(
        "USB10Y_USD", "USD_SGD", "XAU_USD", "XAG_EUR", "AU200_AUD", "US2000_USD", "SPX500_USD",
        "NL25_EUR", "NL25_EUR", "FR40_EUR", "EU50_EUR", "DE30_EUR", "XAG_USD",
        "CH20_CHF", "XAU_EUR", "EUR_USD"
      ) %>% unique(), #22 JP225_USD

      c(
        "USB10Y_USD", "USD_SGD", "XAU_USD", "XAG_CHF", "AU200_AUD", "US2000_USD", "SPX500_USD",
        "NL25_EUR", "NL25_EUR", "FR40_EUR", "EU50_EUR", "DE30_EUR", "XAG_USD",
        "JP225_USD", "XAU_CHF", "EUR_CHF"
      ) %>% unique(), #23 CH20_CHF

      c(
        "USB10Y_USD", "USD_SGD", "XAU_USD", "XAG_CHF", "AU200_AUD", "US2000_USD", "SPX500_USD",
        "CH20_CHF", "FR40_EUR", "EU50_EUR", "DE30_EUR", "XAG_USD",
        "JP225_USD", "XAU_CHF", "EUR_CHF"
      ) %>% unique(), #24 NL25_EUR

      c("XAG_CAD", "XAG_JPY", "XAG_CHF", "XAG_USD", "XAG_EUR", "XAG_GBP", "XAG_NZD",
        "XAG_AUD", "XAU_USD", "XAU_EUR", "XAU_GBP",
        "XAU_SGD", "XAU_CAD", "XAU_NZD", "XAU_AUD") %>% unique(), #25 XAG_SGD

      c(
        "USB10Y_USD", "USD_SGD", "XAU_USD", "BTC_USD", "LTC_USD", "US2000_USD", "SPX500_USD",
        "NL25_EUR", "NL25_EUR", "FR40_EUR", "EU50_EUR", "JP225_USD", "XAG_USD",
        "CH20_CHF", "XAU_GBP", "GBP_USD", "UK100_GBP"
      ) %>% unique(), #26 BCH_USD

      c(
        "USB10Y_USD", "USD_SGD", "XAU_USD", "BTC_USD", "BCH_USD", "US2000_USD", "SPX500_USD",
        "NL25_EUR", "NL25_EUR", "FR40_EUR", "EU50_EUR", "JP225_USD", "XAG_USD",
        "CH20_CHF", "XAU_GBP", "GBP_USD", "UK100_GBP"
      ) %>% unique() #27 LTC_USD

    ),
  countries_for_int_strength =
    list(

      c("GBP", "USD", "EUR", "AUD", "JPY"),  #1
      c("GBP", "USD", "EUR", "AUD", "JPY"),  #2
      c("GBP", "USD", "EUR", "AUD", "JPY"),  #3
      c("GBP", "USD", "EUR", "AUD", "JPY"),  #4
      c("GBP", "USD", "EUR", "AUD", "JPY"),  #5
      c("GBP", "USD", "EUR", "AUD", "JPY"),  #6
      c("GBP", "USD", "EUR", "AUD", "JPY"),  #7
      c("GBP", "USD", "EUR", "AUD", "JPY"),  #8
      c("GBP", "USD", "EUR", "AUD", "JPY"),  #9
      c("GBP", "USD", "EUR", "AUD", "JPY"),  #10
      c("GBP", "USD", "EUR", "AUD", "JPY"),  #11
      c("GBP", "USD", "EUR", "AUD", "JPY"),  #12
      c("GBP", "USD", "EUR", "AUD", "JPY"),  #13
      c("GBP", "USD", "EUR", "AUD", "JPY"),  #14
      c("GBP", "USD", "EUR", "AUD", "JPY"),  #15
      c("GBP", "USD", "EUR", "AUD", "JPY"),  #16
      c("GBP", "USD", "EUR", "AUD", "JPY"),  #17
      c("GBP", "USD", "EUR", "AUD", "JPY"),  #18
      c("GBP", "USD", "EUR", "AUD", "JPY"),  #19
      c("GBP", "USD", "EUR", "AUD", "JPY"),  #20
      c("GBP", "USD", "EUR", "AUD", "JPY"),  #21
      c("GBP", "USD", "EUR", "AUD", "JPY"),  #22
      c("GBP", "USD", "EUR", "AUD", "JPY"),  #23
      c("GBP", "USD", "EUR", "AUD", "JPY"),  #24
      c("GBP", "USD", "EUR", "AUD", "JPY"),  #25
      c("GBP", "USD", "EUR", "AUD", "JPY"),  #26
      c("GBP", "USD", "EUR", "AUD", "JPY")  #27

    )
)

pred_generation_db_path <-
  "C:/Users/Nikhil Chandra/Documents/trade_data/tech_preds.db"
pred_gen_db_con <- connect_db(pred_generation_db_path)

raw_base_preds <-
  list()

training_end_date <- "2025-05-01"
rolling_mean_pred_period = 500
bin_threshold = 5

for (i in 23:length(indicator_mapping$Asset) ) {

  asset_loop <- indicator_mapping$Asset[i]
  copula_assets <- indicator_mapping$couplua_assets[[i]]

  pred_generated <-
    Single_Asset_V3_Gen_Model(
    Indices_Metals_Bonds,
    actual_wins_losses,
    asset_of_interest = asset_loop,
    actuals_periods_needed = c("period_return_24_Price", "period_return_35_Price", "period_return_46_Price"),
    training_end_date = training_end_date,
    bin_threshold = bin_threshold,
    rolling_mean_pred_period = rolling_mean_pred_period,
    correlation_rolling_periods = c(100,200, 300),
    copula_assets = copula_assets
  )

  all_preds <-
    pred_generated[[1]] %>%
    bind_rows(pred_generated[[1]]) %>%
    mutate(
      training_end_date = training_end_date,
      rolling_mean_pred_period = rolling_mean_pred_period,
      bin_threshold = bin_threshold
    )

  if(i == 1){
    write_table_sql_lite(.data = all_preds,
                         table_name = "tech_preds",
                         conn = pred_gen_db_con,
                         overwrite_true = TRUE)
  } else {
    append_table_sql_lite(.data = all_preds,
                         table_name = "tech_preds",
                         conn = pred_gen_db_con)
  }

  rm(all_preds, pred_generated)

  gc()

}


generated_preds_from_db <-
  DBI::dbGetQuery(conn = pred_gen_db_con,
                  statement = "SELECT * FROM tech_preds") %>%
  mutate(
    Date = as_datetime(Date)
  )

test_performance <-
  generated_preds_from_db %>%
  mutate(
    averaged_35_LM_pred =
      (state_space_LM_Pred_period_return_35_Price +
         AR_LM_Pred_period_return_35_Price +
         Copula_LM_Pred_period_return_35_Price)/3,

    averaged_35_GLM_pred =
      (state_space_GLM_Pred_period_return_35_Price +
         AR_GLM_Pred_period_return_35_Price +
         Copula_GLM_Pred_period_return_35_Price)/3,

    averaged_35_46_GLM_pred =
      (state_space_GLM_Pred_period_return_35_Price +
         AR_GLM_Pred_period_return_35_Price +
         Copula_GLM_Pred_period_return_35_Price +
         state_space_GLM_Pred_period_return_46_Price +
         AR_GLM_Pred_period_return_46_Price +
         Copula_GLM_Pred_period_return_46_Price)/6,

    averaged_35_46_LM_pred =
      (state_space_LM_Pred_period_return_35_Price +
         AR_LM_Pred_period_return_35_Price +
         Copula_LM_Pred_period_return_35_Price +
         state_space_LM_Pred_period_return_46_Price +
         AR_LM_Pred_period_return_46_Price +
         Copula_LM_Pred_period_return_46_Price)/6

  ) %>%
  filter(Date >= as_datetime(training_end_date)) %>%
  left_join(
    actual_wins_losses %>%
      dplyr::select(Date, Asset,
                    period_return_8_Price, period_return_12_Price, period_return_16_Price,
                    period_return_24_Price, period_return_35_Price, period_return_46_Price)
  ) %>%
  mutate(
    trade_col = case_when(

      # state_space_GLM_Pred_period_return_35_Price > 0.6 & state_space_GLM_Pred_period_return_35_Price < 0.7|
      # AR_GLM_Pred_period_return_35_Price > 0.8|

      # Copula_GLM_Pred_period_return_35_Price > 0.99 | ## Very Very Good meets all expectations
      # averaged_35_46_GLM_pred > 0.55  ## Very Very Good meets all expectations

      averaged_35_GLM_pred > 0.53

      # (averaged_35_GLM_pred > 0.5 & averaged_35_GLM_pred < 0.55 &
      # averaged_35_46_GLM_pred > 0.7)
      ~ "Long")
  )

control_data <-
  test_performance %>%
  group_by(Date) %>%
  summarise(
    period_return_35_Price = sum(period_return_35_Price, na.rm = T)
  ) %>%
  ungroup() %>%
  arrange(Date) %>%
  mutate(
    cumulative_return = cumsum(period_return_35_Price)
  ) %>%
  mutate(
    trade_col = "Control"
  )

trade_data <-
  test_performance %>%
  filter(trade_col == "Long") %>%
  group_by(Date) %>%
  summarise(
    period_return_35_Price = sum(period_return_35_Price, na.rm = T)
  ) %>%
  ungroup() %>%
  arrange(Date) %>%
  mutate(
    cumulative_return = cumsum(period_return_35_Price)
  ) %>%
  mutate(
    trade_col = "Long"
  )


plot_dat_Copula <-
  control_data %>%
  bind_rows(trade_data)


plot_dat_Copula %>%
  ggplot(aes(x = Date, y = cumulative_return, color = trade_col)) +
  geom_line() +
  theme_minimal() +
  facet_wrap(.~ trade_col, scales = "free") +
  theme(legend.position = "bottom")


control_data_asset <-
  test_performance %>%
  group_by(Date, Asset) %>%
  summarise(
    period_return_35_Price = sum(period_return_35_Price, na.rm = T)
  ) %>%
  ungroup() %>%
  group_by(Asset) %>%
  arrange(Date, .by_group = TRUE) %>%
  mutate(
    cumulative_return = cumsum(period_return_35_Price),
    wins = ifelse(period_return_35_Price > 0 , 1, 0)
  ) %>%
  mutate(
    trade_col = "Control"
  ) %>%
  group_by(Asset) %>%
  summarise(
    control_total_trades = n_distinct(Date),
    control_wins = sum(wins, na.rm = T),
    control_returns_total = sum(period_return_35_Price, na.rm = T),
    # returns_25 = quantile(cumulative_return, 0.25),
    # returns_10 = quantile(cumulative_return, 0.1)
  ) %>%
  mutate(
    control_Perc = control_wins/control_total_trades
  )

trade_data <-
  test_performance %>%
  filter(trade_col == "Long") %>%
  group_by(Date, Asset) %>%
  summarise(
    period_return_35_Price = sum(period_return_35_Price, na.rm = T)
  ) %>%
  ungroup() %>%
  group_by(Asset) %>%
  arrange(Date, .by_group = TRUE) %>%
  mutate(
    cumulative_return = cumsum(period_return_35_Price),
    wins = ifelse(period_return_35_Price > 0 , 1, 0)
  ) %>%
  mutate(
    trade_col = "Long"
  ) %>%
  group_by(Asset, trade_col) %>%
  summarise(
    total_trades = n_distinct(Date),
    wins = sum(wins, na.rm = T),
    returns_total = sum(period_return_35_Price, na.rm = T),
    returns_25 = quantile(cumulative_return, 0.25),
    returns_10 = quantile(cumulative_return, 0.1)
  ) %>%
  mutate(
    Perc = wins/total_trades
  ) %>%
  ungroup() %>%
  left_join(
    control_data_asset %>% ungroup()
  ) %>%
  mutate(
    perc_diff = Perc - control_Perc,
    Return_diff = returns_total - control_returns_total
  ) %>%
  mutate(
    winning_strat = ifelse(perc_diff > 0, 1, 0),
    winning_return = ifelse(returns_total >  control_returns_total, 1, 0),
    winning_greater_than_0 = ifelse(returns_total > 0, 1, 0),
    winning_greater_than_0_control = ifelse(control_returns_total > 0, 1, 0)
  )

trade_data %>%
  summarise(
    Perc_diff_mean = mean(perc_diff, na.rm = T),
    winning_assets = sum(winning_strat, na.rm = T)/n_distinct(Asset),
    winning_assets_return = sum(winning_return, na.rm = T)/n_distinct(Asset),
    winning_greater_than_0 = sum(winning_greater_than_0, na.rm = T)/n_distinct(Asset),
    winning_greater_than_0_control = sum(winning_greater_than_0_control, na.rm = T)/n_distinct(Asset)
  )

plot_dat_Copula %>%
  filter(trade_col == "Long") %>%
  mutate(
    Movement_300 = cumulative_return - lag(cumulative_return, 300)
  ) %>%
  summarise(
    returns_99 = quantile(Movement_300, 0.99, na.rm = T),
    returns_90 = quantile(Movement_300, 0.90, na.rm = T),
    returns_75 = quantile(Movement_300, 0.75, na.rm = T),
    returns_50 = quantile(Movement_300, 0.5, na.rm = T),
    returns_25 = quantile(Movement_300, 0.25, na.rm = T),
    returns_10 = quantile(Movement_300, 0.1, na.rm = T),
    returns_01 = quantile(Movement_300, 0.01, na.rm = T)
  )
