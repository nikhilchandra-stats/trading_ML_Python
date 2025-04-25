#---------------
# Best Trades:
# Rolling Period = 400
# Profit Factor = 5
# Stop Factor = 5
# trade_sd_fact = 2.7
#Lows Direction: "Short", "Long"
#Highs Direction: "Long", "Short"
#Lows Results Risk Weighted Return: Long -0.001, Short = 0.10
#Highs Results Risk Weighted Return: Long 0.19, Short = -0.2
#Logic Pattern: Strong
profit_factor  = 2
stop_factor  = 2
risk_dollar_value <- 20
markov_trades_raw <-
  get_markov_tag_pos_neg_diff(
    asset_data_combined = asset_data_combined,
    training_perc = 1,
    sd_divides = seq(0.5,2,0.5),
    quantile_divides = seq(0.1,0.9, 0.1),
    rolling_period = 400,
    markov_col_on_interest_pos = "Markov_Point_Pos_roll_sum_1.5",
    markov_col_on_interest_neg = "Markov_Point_Neg_roll_sum_-1.5",
    sum_sd_cut_off = "",
    profit_factor  = profit_factor,
    stop_factor  = stop_factor,
    asset_data_daily_raw = asset_data_combined,
    mean_values_by_asset_for_loop = mean_values_by_asset_for_loop,
    trade_sd_fact = 2.5
  )

tagged_trades_markov_1 <-
  trades_today <-
  list(markov_trades_raw$Trades[[1]] %>% mutate(Markov_Col = "Low"),
       markov_trades_raw$Trades[[2]] %>% mutate(Markov_Col = "High")
  ) %>%
  map_dfr(
    ~ .x
  ) %>%
  filter(
    (trade_col == "Long" & Markov_Col == "High")
  )


trade_data <-
  generic_trade_finder_trailing(
    tagged_trades = tagged_trades_markov_1,
    asset_data_daily_raw = asset_data_daily_raw,
    stop_factor = stop_factor,
    profit_factor =profit_factor,
    trade_col = "trade_col",
    date_col = "Date",
    start_price_col = "Price",
    mean_values_by_asset =
      wrangle_asset_data(
        asset_data_daily_raw = asset_data_daily_raw,
        summarise_means = TRUE
      ),
    trail_points = c(0.5, 0.75, 1.25, 1.5, 1.75),
    trail_stops = c(0,0,0,0,0)
  )

analysis_trades_markov_1 <- analyse_trailing_trades(
  trade_data = trade_data %>% filter(dates > "2016-01-01"),
    # filter(asset %in% c("USD_CAD", "USD_SEK", "USD_NOK",
    #                     "USD_JPY", "CAD_JPY", "USD_SGD", "AUD_USD", "EUR_JPY",
    #                     "GBP_JPY", "GBP_USD", "NZD_USD", "WTICO_USD", "BCO_USD",
    #                     "XAG_USD")
    #        ),
  asset_data_daily_raw = asset_data_daily_raw,
  asset_infor = asset_infor,
  risk_dollar_value = 20
)

analysis_trades_markov_1_ts <- analysis_trades_markov_1[[2]]

perc_analysis <- analysis_trades_markov_1[[1]] %>%
  mutate(
    win_loss = ifelse(trade_returns > 0, 1, 0)
  ) %>%
  summarise(
    total = n(),
    wins = sum(win_loss, na.rm = T)
  ) %>%
  mutate(
    perc = wins/total
  )



#------------------------------------------------------------LM Modelling
helperfunctions35South::load_custom_functions()
one_drive_path <- helperfunctions35South::create_one_drive_path(
  path_extension = "raw data")
library(neuralnet)

raw_macro_data <- get_macro_event_data()

eur_data <- get_EUR_exports()

AUD_exports_total <- get_AUS_exports()  %>%
  pivot_longer(-TIME_PERIOD, names_to = "category", values_to = "Aus_Export") %>%
  rename(date = TIME_PERIOD) %>%
  group_by(date) %>%
  summarise(Aus_Export = sum(Aus_Export, na.rm = T))

USD_exports_total <- get_US_exports()  %>%
  pivot_longer(-date, names_to = "category", values_to = "US_Export") %>%
  group_by(date) %>%
  summarise(US_Export = sum(US_Export, na.rm = T)) %>%
  left_join(AUD_exports_total) %>%
  ungroup()

USD_exports_total <- USD_exports_total %>%
  mutate(
    month_date = lubridate::floor_date(date, "month")
  )

AUD_exports_total <- AUD_exports_total %>%
  mutate(
    month_date = lubridate::floor_date(date, "month")
  )


asset_list_oanda <- get_oanda_symbols() %>%
  keep( ~ .x %in% c("HK33_HKD", "USD_JPY",
                    # "BTC_USD",
                    "AUD_NZD", "GBP_CHF",
                    "EUR_HUF", "EUR_ZAR", "NZD_JPY", "EUR_NZD",
                    "USB02Y_USD",
                    "XAU_CAD", "GBP_JPY", "EUR_NOK", "USD_SGD", "EUR_SEK",
                    "DE30_EUR",
                    "AUD_CAD",
                    "UK10YB_GBP",
                    "XPD_USD",
                    "UK100_GBP",
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
                    "BCO_USD", "AUD_USD", "NZD_USD", "NZD_CHF", "WHEAT_USD", "AUD_JPY", "AUD_SEK")
  )

asset_infor <- get_instrument_info()

extracted_asset_data <-
  read_all_asset_data(
    asset_list_oanda = asset_list_oanda,
    save_path_oanda_assets = "C:/Users/Nikhil Chandra/Documents/Asset Data/oanda_data/",
    read_csv_or_API = "API"
  )

asset_data_combined <- extracted_asset_data %>% map_dfr(bind_rows)
asset_data_daily_raw <- extracted_asset_data %>% map_dfr(bind_rows)
asset_data_daily <- asset_data_daily_raw
aud_usd_today <- asset_data_daily_raw %>% filter(str_detect(Asset, "AUD")) %>%
  slice_max(Date)  %>%
  dplyr::select(Price, Asset) %>%
  mutate(ending_value = str_extract(Asset, "_[A-Z][A-Z][A-Z]") %>% str_remove_all("_")) %>%
  mutate(
    adjusted_conversion =
      case_when(
        ending_value != "AUD" ~ 1/Price,
        TRUE ~ Price
      )
  )

reg_data_list <- run_reg_daily_variant(
  raw_macro_data = raw_macro_data,
  eur_data = eur_data,
  AUD_exports_total = AUD_exports_total,
  USD_exports_total = USD_exports_total,
  asset_data_daily_raw = asset_data_daily_raw,
  train_percent = 0.57
)

regression_prediction <- reg_data_list[[2]]

raw_LM_trade_df <- reg_data_list[[2]]

LM_preped <- prep_LM_daily_trade_data(
  asset_data_daily_raw = asset_data_daily_raw,
  raw_LM_trade_df = reg_data_list[[2]],
  raw_LM_trade_df_training = reg_data_list[[3]]
)

trade_with_daily_data <- LM_preped %>% pluck("LM Merged to Daily")

#Reprosecute Trading Parameters
#-------------------------------Desired Params Long trades = 297, Win = 0.62
# sd_factor_high  = 12
# sd_factor_low  = 6

asset_infor <- get_instrument_info()

mean_values_by_asset_for_loop =
  wrangle_asset_data(
    asset_data_daily_raw = asset_data_daily_raw,
    summarise_means = TRUE
  )

trade_params <-
  tibble(
    sd_factor_low = c(0,0.5, 1, 1.5, 2, 2.5, 3, 4,5,6,7,8,9,10, 12,
                      13, 14, 15)
  ) %>%
  mutate(
    sd_factor_high = sd_factor_low*2,
    sd_factor_high = ifelse(sd_factor_high == 0, 1, sd_factor_high)
  )  %>%
  bind_rows(
    tibble(
      sd_factor_low = c(0,0.5, 1, 1.5, 2, 2.5, 3, 4,5,6,7,8,9,10, 12,
                        13, 14, 15)
    ) %>%
      mutate(
        sd_factor_high = sd_factor_low*4,
        sd_factor_high = ifelse(sd_factor_high == 0, 1*4, sd_factor_high)
      )
  ) %>%
  mutate(
    profit_factor  = 7,
    stop_factor  = 4
  )

trade_params <- trade_params %>%
  bind_rows(
    trade_params %>%
      mutate(
        profit_factor  = 3,
        stop_factor  = 2
      )
  )


trade_with_daily_data <- LM_preped %>% pluck("LM Merged to Daily")

returns_ts_LM <- list()
perc_LM <- list()
all_trade_data_LM <- list()

for (j in 1:dim(trade_params)[1]) {

  sd_factor_low <- trade_params$sd_factor_low[j] %>% as.numeric()
  sd_factor_high <- trade_params$sd_factor_high[j] %>% as.numeric()
  stop_factor <- trade_params$stop_factor[j] %>% as.numeric()
  profit_factor <- trade_params$profit_factor[j] %>% as.numeric()

  temp_for_trade <- trade_with_daily_data %>%
    mutate(
      trade_col =
        case_when(
          between(Pred_trade,mean_value  + sd_value*sd_factor_low,  mean_value  + sd_value*sd_factor_high) ~ "Long",
          between(Pred_trade,mean_value  - sd_value*sd_factor_high,  mean_value  - sd_value*sd_factor_low) ~ "Short"
        )
    )

  trade_data_LM <-
    generic_trade_finder_trailing(
      tagged_trades = temp_for_trade,
      asset_data_daily_raw = asset_data_daily_raw,
      stop_factor = stop_factor,
      profit_factor =profit_factor,
      trade_col = "trade_col",
      date_col = "Date",
      start_price_col = "Price",
      mean_values_by_asset =
        wrangle_asset_data(
          asset_data_daily_raw = asset_data_daily_raw,
          summarise_means = TRUE
        ),
      trail_points = c(0.5, 0.75, 1.25, 1.5, 1.75),
      trail_stops = c(0,0,0,0,0)
    )

  analysis_trades_markov_1 <- analyse_trailing_trades(
    trade_data = trade_data_LM,
    asset_data_daily_raw = asset_data_daily_raw,
    asset_infor = asset_infor,
    risk_dollar_value = 20
  )

  analysis_trades_markov_1_ts <-
    analysis_trades_markov_1[[2]]  %>%
    mutate(
      LM_low_sd_param = sd_factor_low,
      LM_high_sd_param = sd_factor_high,
      stop_factor = stop_factor,
      profit_factor = profit_factor
    )

  perc_analysis <- analysis_trades_markov_1[[1]] %>%
    mutate(
      win_loss = ifelse(trade_returns > 0, 1, 0)
    ) %>%
    summarise(
      total = n(),
      wins = sum(win_loss, na.rm = T)
    ) %>%
    mutate(
      perc = wins/total
    ) %>%
    mutate(
      LM_low_sd_param = sd_factor_low,
      LM_high_sd_param = sd_factor_high,
      stop_factor = stop_factor,
      profit_factor = profit_factor
    )

  all_trade_data_LM[[j]] <-
    analysis_trades_markov_1[[1]] %>%
    mutate(
      LM_low_sd_param = sd_factor_low,
      LM_high_sd_param = sd_factor_high,
      stop_factor = stop_factor,
      profit_factor = profit_factor
    )
  perc_LM[[j]] <- perc_analysis
  returns_ts_LM[[j]] <-analysis_trades_markov_1_ts

}

perc_LM_analysis <- perc_LM %>%
  map_dfr(
    ~ .x %>%
      mutate(
        risk_weighted_return =
          perc*(profit_factor/stop_factor) - (1- perc)*(1)
      )
  )

returns_ts_LM_analysis <-
  returns_ts_LM %>%
  map_dfr(~ .x %>%
            group_by(LM_low_sd_param, LM_high_sd_param, stop_factor, profit_factor, dates) %>%
            summarise(
              Total_Return = sum(Total_Return, na.rm = T),
              cumulative_return = sum(cumulative_return, na.rm = T)
            )
          )

returns_ts_LM_analysis_1 <- returns_ts_LM_analysis %>%
  filter(LM_low_sd_param == 4 & LM_high_sd_param == 16)

