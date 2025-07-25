helperfunctions35South::load_custom_functions()
one_drive_path <- helperfunctions35South::create_one_drive_path(
  path_extension = "raw data")
library(neuralnet)
all_aud_symbols <- get_oanda_symbols() %>%
  keep(~ str_detect(.x, "AUD")|str_detect(.x, "USD_SEK|USD_NOK|USD_HUF|USD_ZAR|USD_CNY|USD_MXN|USD_CNH"))
asset_infor <- get_instrument_info()
aud_assets <- read_all_asset_data_intra_day(
  asset_list_oanda = all_aud_symbols,
  save_path_oanda_assets = "C:/Users/Nikhil Chandra/Documents/Asset Data/oanda_data/",
  read_csv_or_API = "API",
  time_frame = "D",
  bid_or_ask = "bid",
  how_far_back = 10,
  start_date = (today() - days(2)) %>% as.character()
)
aud_assets <- aud_assets %>% map_dfr(bind_rows)
aud_usd_today <- get_aud_conversion(asset_data_daily_raw = aud_assets)
raw_macro_data <- niksmacrohelpers::get_macro_event_data()

currency_conversion <-
  aud_usd_today %>%
  mutate(
    not_aud_asset = ending_value
  ) %>%
  dplyr::select(not_aud_asset, adjusted_conversion) %>%
  bind_rows(
    tibble(not_aud_asset = "AUD", adjusted_conversion = 1)
  )


db_location = "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data For EDA.db"
start_date = "2011-01-01"
end_date = today() %>% as.character()
time_frame = "H1"

asset_data_raw_ask <- get_db_price(
  db_location = db_location,
  start_date = start_date,
  end_date = end_date,
  bid_or_ask = "ask",
  time_frame = "H1"
)

asset_data_raw_bid <- get_db_price(
  db_location = db_location,
  start_date = start_date,
  end_date = end_date,
  bid_or_ask = "bid",
  time_frame = "H1"
)

asset_data_raw_ask$Asset %>% unique()

samples <- 1200
random_results_db_location <- "C:/Users/Nikhil Chandra/Documents/trade_data/random_results.db"
db_con <- connect_db(random_results_db_location)
stop_factor = 12
profit_factor = 40
analysis_syms = c("XAG_USD", "AUD_USD", "EUR_USD", "AU200_AUD", "WHEAT_USD",
                             "USD_CNH", "WTICO_USD", "SOYBN_USD", "SUGAR_USD", "NATGAS_USD",
                             "EUR_SEK", "USD_SGD", "USD_MXN")
trade_samples = 1000
new_table = FALSE
time_frame = "H1"

for (i in 1:samples) {

  temp_results <-
    get_random_results_trades(
      raw_asset_data_ask = asset_data_raw_ask,
      raw_asset_data_bid = asset_data_raw_bid,
      stop_factor = stop_factor,
      profit_factor = profit_factor,
      risk_dollar_value = 10,
      analysis_syms = analysis_syms,
      trade_samples = trade_samples
    )

  complete_results <-
    temp_results[[1]] %>%
    bind_rows(temp_results[[2]]) %>%
    mutate(trade_samples = trade_samples,
           time_frame = time_frame)

  if(new_table == TRUE) {
    write_table_sql_lite(.data = complete_results,
                         table_name = "random_results",
                         conn = db_con,
                         overwrite_true = TRUE)
  }

  if(new_table == FALSE) {
    append_table_sql_lite(
      .data = complete_results,
      table_name = "random_results",
      conn = db_con
    )
  }

}

DBI::dbDisconnect(db_con)

control_random_samples <-
  get_random_samples_MLE_beta(
    random_results_db_location = "C:/Users/Nikhil Chandra/Documents/trade_data/random_results.db",
    stop_factor = 12,
    profit_factor = 40,
    analysis_syms = analysis_syms,
    time_frame = "H1",
    return_summary = TRUE
  )

SPX_bench_mark <-
  get_bench_mark_results(
    asset_data = asset_data_raw_ask %>% filter(Date >= "2018-01-01"),
    Asset_Var = "SPX500_USD",
    min_date = "2018-01-01",
    trade_direction = "Long",
    risk_dollar_value = 5,
    stop_factor = 4,
    profit_factor = 8,
    mean_values_by_asset_for_loop = wrangle_asset_data(asset_data_raw_ask, summarise_means = TRUE),
    currency_conversion = currency_conversion
  )

data_with_rolling_binom <-
  asset_data_raw_ask %>%
  # filter(Asset %in% c("XAG_USD", "AUD_USD", "EUR_USD", "AU200_AUD", "WHEAT_USD",
  #                     "USD_CNH", "WTICO_USD", "SOYBN_USD", "SUGAR_USD", "NATGAS_USD",
  #                     "EUR_SEK", "USD_SGD", "USD_MXN")) %>%
  group_by(Asset) %>%
  mutate(
    X_Highs = case_when(
      High > lag(High) ~ 1,
      TRUE ~  0
      ),
    X_Lows = case_when(
      Low > lag(Low) ~ 1,
      TRUE ~  0
    )
  ) %>%
  mutate(
    Rolling_Bayes_P_Highs =
      slider::slide_dbl(.x = X_Highs,
                        .f = ~ rolling_bayesian_binom(
                          .vec = .x,
                          prior_weight = 20,
                          prior_p = 0.5,
                          samples = 1000,
                          quantile_posterior = 0.5
                            ),
                        .before = 20, .complete = TRUE),

    Rolling_Bayes_P_Lows =
      slider::slide_dbl(.x = X_Lows,
                        .f = ~ rolling_bayesian_binom(
                          .vec = .x,
                          prior_weight = 20,
                          prior_p = 0.5,
                          samples = 1000,
                          quantile_posterior = 0.5
                        ),
                        .before = 20, .complete = TRUE),

    Rolling_Bayes_P_Highs_slow =
      slider::slide_dbl(.x = X_Highs,
                        .f = ~ rolling_bayesian_binom(
                          .vec = .x,
                          prior_weight = 50,
                          prior_p = 0.5,
                          samples = 1000,
                          quantile_posterior = 0.5
                        ),
                        .before = 50, .complete = TRUE),

    Rolling_Bayes_P_Lows_slow =
      slider::slide_dbl(.x = X_Lows,
                        .f = ~ rolling_bayesian_binom(
                          .vec = .x,
                          prior_weight = 50,
                          prior_p = 0.5,
                          samples = 1000,
                          quantile_posterior = 0.5
                        ),
                        .before = 50, .complete = TRUE)
  )

data_with_rolling_binom2 <-
  data_with_rolling_binom %>%
  group_by(Asset) %>%
  mutate(
    Rolling_Bayes_P_Highs_mean =
      slider::slide_dbl(Rolling_Bayes_P_Highs, .f = ~ mean(.x, na.rm = T), .before = 100),
    Rolling_Bayes_P_Highs_sd =
      slider::slide_dbl(Rolling_Bayes_P_Highs, .f = ~ sd(.x, na.rm = T), .before = 100),

    Rolling_Bayes_P_Lows_mean =
      slider::slide_dbl(Rolling_Bayes_P_Lows, .f = ~ mean(.x, na.rm = T), .before = 100),
    Rolling_Bayes_P_Lows_sd =
      slider::slide_dbl(Rolling_Bayes_P_Lows, .f = ~ sd(.x, na.rm = T), .before = 100),


    Rolling_Bayes_P_Highs_mean_slow =
      slider::slide_dbl(Rolling_Bayes_P_Highs_slow, .f = ~ mean(.x, na.rm = T), .before = 100),
    Rolling_Bayes_P_Highs_sd_slow =
      slider::slide_dbl(Rolling_Bayes_P_Highs_slow, .f = ~ sd(.x, na.rm = T), .before = 100),

    Rolling_Bayes_P_Lows_mean_slow =
      slider::slide_dbl(Rolling_Bayes_P_Lows_slow, .f = ~ mean(.x, na.rm = T), .before = 100),
    Rolling_Bayes_P_Lows_sd_slow =
      slider::slide_dbl(Rolling_Bayes_P_Lows_slow, .f = ~ sd(.x, na.rm = T), .before = 100),

    Prob_Slow_minus_High =
      Rolling_Bayes_P_Highs_slow - Rolling_Bayes_P_Highs,
    Prob_Slow_minus_High_mean =
      slider::slide_dbl(.x = Prob_Slow_minus_High,
                        .f = ~ mean(.x, na.rm = T),
                        .before = 100),
    Prob_Slow_minus_High_sd =
      slider::slide_dbl(.x = Prob_Slow_minus_High,
                        .f = ~ sd(.x, na.rm = T),
                        .before = 100),

    Prob_Slow_minus_Lows =
      Rolling_Bayes_P_Lows_slow - Rolling_Bayes_P_Lows,
    Prob_Slow_minus_Lows_mean =
      slider::slide_dbl(.x = Prob_Slow_minus_Lows,
                        .f = ~ mean(.x, na.rm = T),
                        .before = 100),
    Prob_Slow_minus_Lows_sd =
      slider::slide_dbl(.x = Prob_Slow_minus_Lows,
                        .f = ~ sd(.x, na.rm = T),
                        .before = 100)
  )

tagged_trades <-
  data_with_rolling_binom2 %>%
  group_by(Asset) %>%
  mutate(
    tan_angle = atan((Price - lag(Price, 20))/20),
    tan_angle_slow = atan((Price - lag(Price, 50))/50)
  ) %>%
  ungroup() %>%
  ungroup() %>%
  mutate(
    trade_col =
      case_when(


        # Prob_Slow_minus_High <= Prob_Slow_minus_High_mean - 3*Prob_Slow_minus_High_sd &
        #   tan_angle_slow > 0  ~ "Long",
        #
        # Prob_Slow_minus_High <= Prob_Slow_minus_High_mean - 3*Prob_Slow_minus_High_sd &
        #   tan_angle < 0  ~ "Long",
        #
        # Prob_Slow_minus_Lows <= Prob_Slow_minus_Lows_mean - 3*Prob_Slow_minus_Lows_sd &
        #   tan_angle_slow > 0  ~ "Long",
        #
        # Prob_Slow_minus_Lows <= Prob_Slow_minus_Lows_mean - 3*Prob_Slow_minus_Lows_sd &
        #   tan_angle < 0  ~ "Long"

        # Rolling_Bayes_P_Highs >= 0.6 & Rolling_Bayes_P_Highs <= 0.65 ~ "Long",
        # Rolling_Bayes_P_Highs >= 0.35 & Rolling_Bayes_P_Highs <= 0.4 ~ "Short"

        Rolling_Bayes_P_Highs >  Rolling_Bayes_P_Highs_slow ~ "Long",
        Rolling_Bayes_P_Highs <  Rolling_Bayes_P_Highs_slow ~ "Short"
      )
  ) %>%
  filter(!is.na(trade_col))

long_analysis <-
  run_pairs_analysis(
  tagged_trades = tagged_trades %>% filter(trade_col == "Long"),
  stop_factor = 10,
  profit_factor = 15,
  raw_asset_data = asset_data_raw_ask,
  risk_dollar_value = 10
)

long_results <- long_analysis[[2]] %>%
  mutate(
    risk_weighted_return_strat = risk_weighted_return
  ) %>%
  left_join(control_random_samples %>%
              ungroup() %>%
              dplyr::select(-stop_factor, -profit_factor)) %>%
  mutate(
    p_value_risk =
      round(pnorm(risk_weighted_return_strat, mean = mean_risk, sd = sd_risk), 4)
  )

long_results_total <- long_analysis[[1]]


short_analysis <-
  run_pairs_analysis(
    tagged_trades = tagged_trades %>% filter(trade_col == "Short"),
    stop_factor = 15,
    profit_factor = 20,
    raw_asset_data = asset_data_raw_bid,
    risk_dollar_value = 10
  )

short_results <- short_analysis[[2]] %>%
  mutate(
    risk_weighted_return_strat = risk_weighted_return
  ) %>%
  left_join(control_random_samples %>%
              ungroup() %>%
              dplyr::select(-stop_factor, -profit_factor)) %>%
  mutate(
    p_value_risk =
      round(pnorm(risk_weighted_return_strat, mean = mean_risk, sd = sd_risk), 4)
  )

short_results_total <- short_analysis[[1]]
