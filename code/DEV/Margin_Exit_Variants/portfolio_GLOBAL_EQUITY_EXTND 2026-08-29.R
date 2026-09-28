helpeR::load_custom_functions()

all_aud_symbols <- get_oanda_symbols() %>%
  keep(~ str_detect(.x, "AUD")|str_detect(.x, "USD_SEK|USD_NOK|USD_HUF|USD_ZAR|USD_CNY|USD_MXN"))
asset_infor <- get_instrument_info()
aud_assets <- read_all_asset_data_intra_day(
  asset_list_oanda = all_aud_symbols,
  save_path_oanda_assets = "C:/Users/nikhi/Documents//trade_data//oanda_data/",
  read_csv_or_API = "API",
  time_frame = "D",
  bid_or_ask = "bid",
  how_far_back = 10,
  start_date = (today() - days(7)) %>% as.character()
)
aud_assets <- aud_assets %>% map_dfr(bind_rows)
aud_usd_today <- get_aud_conversion(asset_data_daily_raw = aud_assets)

currency_conversion <-
  aud_usd_today %>%
  mutate(
    not_aud_asset = ending_value
  ) %>%
  dplyr::select(not_aud_asset, adjusted_conversion) %>%
  bind_rows(
    tibble(not_aud_asset = "AUD", adjusted_conversion = 1)
  )

asset_list_oanda =
  c("HK33_HKD", "USD_JPY",
    "BTC_USD",
    "AUD_NZD", "GBP_CHF",
    "EUR_HUF", "EUR_ZAR", "NZD_JPY", "EUR_NZD",
    "USB02Y_USD",
    "XAU_CAD", "GBP_JPY", "EUR_NOK", "USD_SGD", "EUR_SEK",
    "DE30_EUR",
    "AUD_CAD",
    "UK10YB_GBP",
    "XPD_USD",
    "UK100_GBP", "NZD_USD",
    "USD_CHF", "GBP_NZD",
    "GBP_SGD", "USD_SEK", "EUR_SGD", "XCU_USD", "SUGAR_USD", "CHF_ZAR",
    "AUD_CHF", "EUR_CHF", "USD_MXN", "GBP_USD", "WTICO_USD", "EUR_JPY", "USD_NOK",
    "XAU_USD",
    "DE10YB_EUR",
    "USD_CZK", "AUD_SGD", "USD_HUF", "WHEAT_USD",
    "EUR_USD", "SG30_SGD", "GBP_AUD", "NZD_CAD", "AU200_AUD", "XAG_USD",
    "XAU_EUR", "EUR_GBP", "USD_CNH", "USD_CAD", "NAS100_USD",
    "USB10Y_USD",
    "EU50_EUR", "NATGAS_USD", "CAD_JPY", "FR40_EUR", "USD_ZAR", "XAU_GBP",
    "CH20_CHF", "ESPIX_EUR",
    "XPT_USD",
    "EUR_AUD", "SOYBN_USD",
    "US2000_USD",
    "XAG_USD", "XAG_EUR", "XAG_CAD", "XAG_AUD", "XAG_GBP", "XAG_JPY", "XAG_SGD", "XAG_CHF",
    "XAG_NZD",
    "XAU_USD", "XAU_EUR", "XAU_CAD", "XAU_AUD", "XAU_GBP", "XAU_JPY", "XAU_SGD", "XAU_CHF",
    "XAU_NZD",
    "BTC_USD", "LTC_USD", "BCH_USD",
    "US30_USD", "FR40_EUR", "US2000_USD", "CH20_CHF", "SPX500_USD", "AU200_AUD",
    "JP225_USD", "JP225Y_JPY", "SG30_SGD", "EU50_EUR", "HK33_HKD",
    "USB02Y_USD", "USB05Y_USD", "USB30Y_USD", "USB10Y_USD", "UK100_GBP") %>%
  unique()

asset_infor <- get_instrument_info()
#---------------------Data
load_custom_functions()
db_location = "C:/Users/nikhi/Documents//Asset Data/Oanda_Asset_Data_Most_Assets_2025-09-13.db"
start_date = "2021-11-01"
training_date = "2022-02-01"
stop_factor_var = 10
currency_conversion = currency_conversion
asset_infor = asset_infor
db_location = db_location
profit_factor_var = 50
risk_dollar_value_var = 5
end_period = 132
trade_direction = "Long"
end_point_loss = -5
end_point_profit = 100

tictoc::tic()
model_predicted_data_raw <-
  portfolio_no_V3_New_algo_pnorm_version(
    assets_to_port =
      c(
        "SPX500_USD",
        "CH20_CHF",
        "DE30_EUR",
        "EU50_EUR",
        "US2000_USD",
        "HK33_HKD",
        "JP225Y_JPY",
        "UK100_GBP"
      ) %>% unique(),
    stop_factor_var = stop_factor_var,
    currency_conversion = currency_conversion,
    asset_infor = asset_infor,
    db_location = db_location,
    start_date = start_date,
    profit_factor_var = profit_factor_var,
    risk_dollar_value_var = risk_dollar_value_var,
    end_period = end_period,
    trade_direction = trade_direction,
    end_point_loss = end_point_loss,
    end_point_profit = end_point_profit,
    regression_length = 25000,
    direct_return_cols = 24,
    lag_value_error = 132 + 1,
    low_to_price_lengths = c(400),
    cor_period = c(50),
    dependant_var = "Final_Return",
    save_location = "C:/Users/nikhi/Documents//trade_data/Day_Trader_Cor_Continuous_Models/",
    file_name = "EQUITY_EXPNDED_ONLY_NON_V3_NEW_MODEL",
    training_date = training_date,
    testing_date = start_date,
    xtnd_ss_cols_PR_cols = c(1,5,10,20,30,40,50,60,70,80,120, 100),
    xtnd_ss_cols_BR_periods = c(100,200,300, 50, 150, 250, 350, 25, 500),
    lag_dependant = 132 + 1,
    auto_cor_cols = 40,
    cor_skip_periods = c(1,2,4,5,6,8,10,12,14,16),
    periods_to_use_deviation = c(1,10,20,30,40,50),
    mean_periods_deviation = c(50, 100),

    estimate_trades = FALSE,
    trade_statement = NULL
  )
tictoc::toc()

db_location = "C:/Users/nikhi/Documents//Asset Data/Oanda_Asset_Data_Most_Assets_2025-09-13.db"
end_date = today() %>% as.character()
Indices_Metals_Bonds <- list()
assets_to_port =
  c(
    "SPX500_USD",
    "CH20_CHF",
    "DE30_EUR",
    "EU50_EUR",
    "US2000_USD",
    "HK33_HKD",
    "JP225Y_JPY",
    "UK100_GBP"
  ) %>% unique()

Indices_Metals_Bonds[[1]] <-
  get_db_data_quickly_algo(
    db_location = db_location,
    start_date = start_date,
    end_date = as.character(today() + days(30)),
    time_frame = "H1",
    bid_or_ask = "ask",
    assets =   assets_to_port
  ) %>%
  distinct()
Indices_Metals_Bonds[[2]] <-
  get_db_data_quickly_algo(
    db_location = db_location,
    start_date = start_date,
    end_date = as.character(today() + days(30)),
    time_frame = "H1",
    bid_or_ask = "bid",
    assets =   assets_to_port
  ) %>%
  distinct()

Indices_Metals_Bonds[[1]] <- Indices_Metals_Bonds[[1]] %>% filter(Date > training_date)
Indices_Metals_Bonds[[2]] <- Indices_Metals_Bonds[[2]] %>% filter(Date > training_date)

profit_loss_data <-
  get_portfolio_model_fast_summed_port_optimised(
    asset_data = Indices_Metals_Bonds %>% map(~ .x %>% filter(Date > training_date, Date > start_date) ),
    asset_of_interest = assets_to_port,
    stop_factor_var = stop_factor_var,
    profit_factor_var = profit_factor_var,
    risk_dollar_value_var = risk_dollar_value_var,
    end_period = end_period,
    time_frame = "H1",
    trade_direction = trade_direction,
    currency_conversion = currency_conversion,
    asset_infor = asset_infor,
    end_point_loss = end_point_loss,
    end_point_profit = end_point_profit,
    sum_as_portfolio = TRUE,

    overwrite_volume = NULL,
    min_volume_only = FALSE,
    return_only_interested_col = FALSE,
    return_only_Final = TRUE
  ) %>%
  mutate(
    Actual_End_Point =
      case_when(
        end_point_point_win < end_point_point_loss ~ end_point_point_win,
        end_point_point_win >= end_point_point_loss ~ end_point_point_loss
      ),
    End_Point_Date = Date + dhours(Actual_End_Point)
  )

profit_loss_long <-
  profit_loss_data %>%
  ungroup() %>%
  dplyr::select(Date, Asset, contains("period_return")) %>%
  pivot_longer(-c(Date, Asset), names_to = "string_var", values_to = "Return") %>%
  mutate(
    Periods_Since = str_remove_all(string_var, "[a-z]+|[A-Z]+|_") %>% str_trim() %>% as.numeric()
  )

trade_end_dates <-
  profit_loss_data %>%
  ungroup() %>%
  distinct(Date, Asset, End_Point_Date, Actual_End_Point) %>%
  left_join(
    profit_loss_long %>%
      distinct(Date, Asset, Return, Periods_Since),
    by = c("Date", "Asset", "Actual_End_Point" = "Periods_Since")
  )

gc()

trade_statment <-
  "
  (pnorm_1001_port > 0.77 & pnorm_1001_port < 100 & Asset == 'CH20_CHF')|
  (pnorm_1001_port > 0.68 & pnorm_1001_port < 100 & Asset == 'DE30_EUR')|
  (pnorm_1001_port > 0.65 & pnorm_1001_port < 100 & Asset == 'EU50_EUR')|
  (pnorm_1001_port > 0.63 & pnorm_1001_port < 100 & Asset == 'HK33_HKD')|
  (pnorm_1001_port > 0.6 & pnorm_1001_port < 100 & Asset == 'JP225Y_JPY')|
  (pnorm_1001_port > 0.75 & pnorm_1001_port < 0.89 & Asset == 'SPX500_USD')|
  (pnorm_1001_port > 0.73 & pnorm_1001_port < 100 & Asset == 'UK100_GBP')|
  (pnorm_1001_port > 0.73 & pnorm_1001_port < 100 & Asset == 'US2000_USD')
"

all_dates <-
  Indices_Metals_Bonds[[1]]$Date %>% unique()


distinct_trade_dates <-
  model_predicted_data_raw %>%
  mutate(
    trade_col = eval(parse(text = trade_statment))
  ) %>%
  mutate(
    trade_col = case_when(trade_col == TRUE ~ "Long", TRUE ~ "No Trade")
  ) %>%
  filter(trade_col == "Long") %>%
  distinct(Asset, Date) %>%
  rename(Trade_Date = Date)

active_trades <- list()
running_PL_vec <- numeric(length(all_dates))
closed_PL_vec <- numeric(length(all_dates))
no_trades = 0
close_all_limit = 500

for (i in 1:length(all_dates)) {

  trades_taken <-
    distinct_trade_dates %>%
    filter(Trade_Date == all_dates[i] )

  # message(glue::glue("Trades this Date {dim(trades_taken)[1]}"))

  trade_details_temp <-
    profit_loss_long %>%
    left_join(trades_taken) %>%
    filter(Trade_Date == Date)

  # message(glue::glue("Trades this Date {dim(trade_details_temp %>% distinct(Date))[1]}"))

  if(no_trades == 0) {
    active_trades_dfr <- trade_details_temp
    no_trades = 1
  } else {
    active_trades_dfr <- active_trades_dfr %>% bind_rows(trade_details_temp)
  }

  # message(glue::glue("Active Trades {dim(active_trades_dfr %>% distinct(Date))[1]}"))

  # active_trades[[list_length + 1]] <- trade_details_temp
  # active_trades_dfr <-
  #   active_trades %>%
  #   map_dfr(bind_rows)

  complete_trades <-
    active_trades_dfr %>%
    ungroup() %>%
    left_join(trade_end_dates %>% dplyr::select(Date, Asset, Actual_End_Point, End_Point_Date)) %>%
    mutate(
      current_periods_since = as.numeric(all_dates[i] -Date, "hours")
    ) %>%
    filter(End_Point_Date <= all_dates[i]) %>%
    filter(current_periods_since == Actual_End_Point | Periods_Since >= end_period) %>%
    filter(Periods_Since == Actual_End_Point) %>%
    dplyr::select(Date, Asset, Return) %>%
    mutate(Flag = TRUE)

  # message(glue::glue("Completed Trades {dim(complete_trades)[1]}"))

  active_trades_dfr_analysis <-
    active_trades_dfr %>%
    ungroup() %>%
    left_join(trade_end_dates) %>%
    filter(End_Point_Date > all_dates[i]) %>%
    mutate(
      hours_since_trade = as.numeric(all_dates[i] - Date, "hours")
    ) %>%
    filter(
      hours_since_trade == 0 | Periods_Since == hours_since_trade
    ) %>%
    group_by(Date, Asset) %>%
    slice_min(Periods_Since) %>%
    ungroup()

  running_pl <-
    active_trades_dfr_analysis %>%
    filter(hours_since_trade != 0) %>%
    pull(Return) %>%
    sum(na.rm = T)

  running_PL_vec[i] <- running_pl

  # message(running_pl)


  if(running_pl >= close_all_limit) {

    closed_PL_vec[i] <- running_pl

    active_trades_dfr <-
      active_trades_dfr %>%
      mutate(
        xx =1
      ) %>%
      filter(xx !=1)

    # message(closed_PL_vec[i])
  } else {

    closed_PL_vec[i] <-
      complete_trades %>%
      pull(Return) %>%
      sum(na.rm = T)

    active_trades_dfr <-
      active_trades_dfr  %>%
      left_join(
        complete_trades %>%
          dplyr::select(-Return )
      ) %>%
      filter(is.na(Flag)) %>%
      dplyr::select(-Flag)

  }

}

time_series_analysis <-
  tibble(
    Date = all_dates,
    running_PL = running_PL_vec,
    closed_PL = closed_PL_vec
  ) %>%
  mutate(
    cumulative_return = cumsum(closed_PL)
  )
  # filter(Date <= "2022-11-10")

time_series_analysis %>%
  ggplot(aes(x = Date, y = cumulative_return)) +
  geom_line() +
  theme_minimal()

test <- profit_loss_data %>%
  distinct(Date, Asset, Final_Return)


test2 <-
  test %>%
  left_join(distinct_trade_dates %>% mutate(Date = Trade_Date)) %>%
  left_join(trade_end_dates, by = c("Date", "Asset") )

test3 <-
  test2 %>%
  filter(!is.na(Trade_Date)) %>%
  group_by(Date) %>%
  summarise(
    Return_1 = sum(Final_Return, na.rm = T),
    Return_2 = sum(Return, na.rm = T)
  ) %>%
  ungroup() %>%
  arrange(Date) %>%
  mutate(
    Return_1 = cumsum(Return_1),
    Return_2 = cumsum(Return_2)
  )

test3 %>%
  filter(Date <= "2022-11-10") %>%
  ggplot(aes(x = Date, y = Return_2)) +
  geom_line() +
  scale_y_continuous(n.breaks = 20) +
  theme_minimal()
