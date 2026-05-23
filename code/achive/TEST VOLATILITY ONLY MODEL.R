
#---Euities and Precious Metals
port_test_data <-
  get_portfolio_model_fast_summed(
  asset_data = Indices_Metals_Bonds %>% map(~ .x %>% filter(Date >= "2018-01-01") ),
  asset_of_interest = c("XAU_USD", "SPX500_USD",
                        "HK33_HKD", "EU50_EUR", "XAG_USD"),
  stop_factor_var = 4,
  profit_factor_var = 8,
  risk_dollar_value_var = 5,
  end_period = 24,
  time_frame = "H1",
  trade_direction = "Long",
  currency_conversion = currency_conversion,
  asset_infor = asset_infor,
  end_point_loss = -4,
  end_point_profit = 20,
  sum_as_portfolio = FALSE
)

trade_statements <-
  "(low_minus_open_100 <= low_minus_open_500_mean - 1*low_minus_open_500_sd) &
   (vol_total_100 >= vol_total_100_500_mean + 1*vol_total_100_500_sd)"

tag_volatility <-
  Indices_Metals_Bonds[[1]] %>%
  filter(Asset %in% c("XAU_USD", "SPX500_USD",
                                 "HK33_HKD", "EU50_EUR",  "XAG_USD")
  ) %>%
  group_by(Asset) %>%
  arrange(Date, .by_group = TRUE) %>%
  group_by(Asset) %>%
  mutate(

    Vol_Low_100 = abs( lag(Low) - lag(Open, 100)),
    Vol_High_100 = abs( lag(High) - lag(Open, 100)),
    vol_total_100 = abs(lag(High) - lag(Low, 100)),

    Vol_Low_100 = slider::slide_dbl(Vol_Low_100, .f = ~ mean(.x, na.rm = T), .before = 10),
    Vol_High_100 = slider::slide_dbl(Vol_High_100, .f = ~ mean(.x, na.rm = T), .before = 10),
    vol_total_100 = slider::slide_dbl(vol_total_100, .f = ~ mean(.x, na.rm = T), .before = 10),

    low_minus_open_100 =   lag(Open, 100) -lag(Low),
    low_minus_open_500_mean =
      slider::slide_dbl(.x = low_minus_open_100, .f = ~ mean(.x, na.rm = T), .before = 500),
    low_minus_open_500_sd =
      slider::slide_dbl(.x = low_minus_open_100, .f = ~ sd(.x, na.rm = T), .before = 500),

    vol_total_100_500_mean =
      slider::slide_dbl(.x = vol_total_100, .f = ~ mean(.x, na.rm = T), .before = 500),
    vol_total_100_500_sd =
      slider::slide_dbl(.x = vol_total_100, .f = ~ sd(.x, na.rm = T), .before = 500),

    Vol_Low_50 = abs( lag(Low) - lag(Open, 30)),
    Vol_High_50 = abs( lag(High) - lag(Open, 30)),
    vol_total_50 = abs(lag(High) - lag(Low, 30)),

    Vol_Low_50 = slider::slide_dbl(Vol_Low_50, .f = ~ mean(.x, na.rm = T), .before = 10),
    Vol_High_50 = slider::slide_dbl(Vol_High_50, .f = ~ mean(.x, na.rm = T), .before = 10),
    vol_total_50 = slider::slide_dbl(vol_total_50, .f = ~ mean(.x, na.rm = T), .before = 10),

    low_minus_open_50 =   lag(Open, 30) -lag(Low),
    low_minus_open_100_mean =
      slider::slide_dbl(.x = low_minus_open_50, .f = ~ mean(.x, na.rm = T), .before = 100),
    low_minus_open_100_sd =
      slider::slide_dbl(.x = low_minus_open_50, .f = ~ sd(.x, na.rm = T), .before = 100)
  )

tag_volatility2 <-
  tag_volatility %>%
  ungroup() %>%
  mutate(
    trade_col = eval(parse(text = trade_statements))
  )

trade_df <-
  tag_volatility2 %>%
  filter(trade_col == TRUE) %>%
  distinct(Asset, Date, trade_col)

trade_dates <-
  trade_df %>%
  pull(Date)

trade_results <-
  port_test_data %>%
  ungroup() %>%
  dplyr::select(-trade_col) %>%
  left_join(trade_df) %>%
  filter(trade_col == TRUE) %>%
  # filter(Date %in% trade_dates) %>%
  group_by(Date) %>%
  summarise(Final_Return = sum(Final_Return, na.rm = T)) %>%
  ungroup() %>%
  arrange(Date) %>%
  mutate(
    cumulative_sum = cumsum(Final_Return)
  )

trade_results %>%
  ggplot(aes(x = Date, y = cumulative_sum)) +
  geom_line() +
  theme_minimal()

#---Euities and Precious Metals and Currency
port_test_data <-
  get_portfolio_model_fast_summed(
    asset_data = Indices_Metals_Bonds %>% map(~ .x %>% filter(Date >= "2018-01-01") ),
    asset_of_interest = c(
                          "AUD_USD", "EUR_USD", "GBP_USD",
                          "USD_CAD", "USD_SEK", "USD_JPY",
                          "EUR_JPY", "GBP_JPY"
                          # "XAG_USD", "XAG_EUR", "XAG_GBP", "XAG_JPY", "XAG_AUD"
                          ),
    stop_factor_var = 12,
    profit_factor_var = 24,
    risk_dollar_value_var = 5,
    end_period = 50,
    time_frame = "H1",
    trade_direction = "Long",
    currency_conversion = currency_conversion,
    asset_infor = asset_infor,
    end_point_loss = -2.5,
    end_point_profit = 5,
    sum_as_portfolio = FALSE
  )

trade_statements <-
  "(low_minus_open_100 <= low_minus_open_500_mean - 2.25*low_minus_open_500_sd)"

tag_volatility <-
  Indices_Metals_Bonds[[1]] %>%
  filter(Asset %in%
           c(
             "AUD_USD", "EUR_USD", "GBP_USD",
             "USD_CAD", "USD_SEK", "USD_JPY",
             "EUR_JPY", "GBP_JPY"
             # "XAG_USD", "XAG_EUR", "XAG_GBP", "XAG_JPY", "XAG_AUD"
           )
  ) %>%
  group_by(Asset) %>%
  arrange(Date, .by_group = TRUE) %>%
  group_by(Asset) %>%
  mutate(

    Vol_Low_100 = abs( lag(Low) - lag(Open, 200)),
    Vol_High_100 = abs( lag(High) - lag(Open, 200)),
    vol_total_100 = abs(lag(High) - lag(Low, 200)),

    Vol_Low_100 = slider::slide_dbl(Vol_Low_100, .f = ~ mean(.x, na.rm = T), .before = 10),
    Vol_High_100 = slider::slide_dbl(Vol_High_100, .f = ~ mean(.x, na.rm = T), .before = 10),
    vol_total_100 = slider::slide_dbl(vol_total_100, .f = ~ mean(.x, na.rm = T), .before = 10),

    low_minus_open_100 =   lag(Open, 200) -lag(Low),
    low_minus_open_500_mean =
      slider::slide_dbl(.x = low_minus_open_100, .f = ~ mean(.x, na.rm = T), .before = 500),
    low_minus_open_500_sd =
      slider::slide_dbl(.x = low_minus_open_100, .f = ~ sd(.x, na.rm = T), .before = 500),

    vol_total_100_500_mean =
      slider::slide_dbl(.x = vol_total_100, .f = ~ mean(.x, na.rm = T), .before = 500),
    vol_total_100_500_sd =
      slider::slide_dbl(.x = vol_total_100, .f = ~ sd(.x, na.rm = T), .before = 500)
  )

tag_volatility2 <-
  tag_volatility %>%
  ungroup() %>%
  mutate(
    trade_col = eval(parse(text = trade_statements))
  )

trade_df <-
  tag_volatility2 %>%
  filter(trade_col == TRUE) %>%
  distinct(Asset, Date, trade_col)

trade_dates <-
  trade_df %>%
  pull(Date)

trade_results <-
  port_test_data %>%
  ungroup() %>%
  dplyr::select(-trade_col) %>%
  left_join(trade_df) %>%
  filter(trade_col == TRUE) %>%
  # filter(Date %in% trade_dates) %>%
  group_by(Date) %>%
  summarise(Final_Return = sum(Final_Return, na.rm = T)) %>%
  ungroup() %>%
  arrange(Date) %>%
  mutate(
    cumulative_sum = cumsum(Final_Return)
  )

trade_results %>%
  ggplot(aes(x = Date, y = cumulative_sum)) +
  geom_line() +
  theme_minimal()


#--------------------------------------------------------------------------
actual_wins_losses <-
  get_actual_wins_losses(
    assets_to_analyse =
      c("XAU_USD", "SPX500_USD", "UK100_GBP",
        "HK33_HKD", "EU50_EUR", "JP225Y_JPY", "XCU_USD",
        "WTICO_USD", "NATGAS_USD", "SG30_SGD", "BTC_USD", "ETH_USD"),
    asset_data = Indices_Metals_Bonds,
    stop_factor = 4,
    profit_factor = 8,
    risk_dollar_value = 5,
    trade_direction = "Long",
    currency_conversion = currency_conversion,
    asset_infor = asset_infor,
    periods_ahead = period_var
  )

tag_volatility <-
  Indices_Metals_Bonds[[1]] %>%
  filter(Asset %in%
           c("XAU_USD", "SPX500_USD", "UK100_GBP",
             "HK33_HKD", "EU50_EUR", "JP225Y_JPY", "XCU_USD",
             "WTICO_USD", "NATGAS_USD", "SG30_SGD", "BTC_USD", "ETH_USD")
  ) %>%
  group_by(Asset) %>%
  arrange(Date, .by_group = TRUE) %>%
  group_by(Asset) %>%
  mutate(

    Vol_Low_100 = abs( lag(Low) - lag(Open, 100)),
    Vol_High_100 = abs( lag(High) - lag(Open, 100)),
    vol_total_100 = abs(lag(High) - lag(Low, 100)),

    Vol_Low_100 = slider::slide_dbl(Vol_Low_100, .f = ~ mean(.x, na.rm = T), .before = 10),
    Vol_High_100 = slider::slide_dbl(Vol_High_100, .f = ~ mean(.x, na.rm = T), .before = 10),
    vol_total_100 = slider::slide_dbl(vol_total_100, .f = ~ mean(.x, na.rm = T), .before = 10),

    low_minus_open_100 =   lag(Open, 100) -lag(Low),
    low_minus_open_500_mean =
      slider::slide_dbl(.x = low_minus_open_100, .f = ~ mean(.x, na.rm = T), .before = 500),
    low_minus_open_500_sd =
      slider::slide_dbl(.x = low_minus_open_100, .f = ~ sd(.x, na.rm = T), .before = 500),

    vol_total_100_500_mean =
      slider::slide_dbl(.x = vol_total_100, .f = ~ mean(.x, na.rm = T), .before = 500),
    vol_total_100_500_sd =
      slider::slide_dbl(.x = vol_total_100, .f = ~ sd(.x, na.rm = T), .before = 500),

    Vol_Low_50 = abs( lag(Low) - lag(Open, 50)),
    Vol_High_50 = abs( lag(High) - lag(Open, 50)),
    vol_total_50 = abs(lag(High) - lag(Low, 50)),

    Vol_Low_50 = slider::slide_dbl(Vol_Low_50, .f = ~ mean(.x, na.rm = T), .before = 10),
    Vol_High_50 = slider::slide_dbl(Vol_High_50, .f = ~ mean(.x, na.rm = T), .before = 10),
    vol_total_50 = slider::slide_dbl(vol_total_50, .f = ~ mean(.x, na.rm = T), .before = 10),

    low_minus_open_50 =   lag(Open, 50) -lag(Low),
    low_minus_open_100_mean =
      slider::slide_dbl(.x = low_minus_open_50, .f = ~ mean(.x, na.rm = T), .before = 100),
    low_minus_open_100_sd =
      slider::slide_dbl(.x = low_minus_open_50, .f = ~ sd(.x, na.rm = T), .before = 100)
  )

cumulative_returns_sim_data <-
  get_total_portfolio_summary(
    generated_preds = tag_volatility,
    trade_statement = trade_statements,
    actual_wins_losses =actual_wins_losses,
    trade_direction = "Long",
    return_col = "period_return_50_Price"
  )

cumulative_returns_sim_data %>%
  ggplot(aes(x = Date, y = Cumulative_Return)) +
  geom_line() +
  facet_wrap(.~trade_col, scales = "free") +
  scale_y_continuous(n.breaks = 20) +
  theme_minimal()

asset_summaries <-
  get_asset_random_sim_returns(
    generated_preds = tag_volatility,
    trade_statement = trade_statements,
    actual_wins_losses = actual_wins_losses,
    trade_direction = "Long",
    return_col = "period_return_50_Price",
    simulations = 5000,
    samples = 50
  )

check_values_stops_profs <-
  actual_wins_losses %>%
  distinct(Asset,volume_adj,minimumTradeSize ,stop_return, profit_return)

all_assets <- Indices_Metals_Bonds[[1]] %>% distinct(Asset)
