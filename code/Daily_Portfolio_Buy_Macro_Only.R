helpeR::load_custom_functions()

all_aud_symbols <- get_oanda_symbols() %>%
  keep(~ str_detect(.x, "AUD")|str_detect(.x, "USD_SEK|USD_NOK|USD_HUF|USD_ZAR|USD_CNY|USD_MXN"))
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
  c("XAG_USD", "XAG_EUR", "XAG_CAD", "XAG_AUD", "XAG_GBP", "XAG_JPY", "XAG_SGD", "XAG_CHF",
    "XAG_NZD",
    "XAU_USD", "XAU_EUR", "XAU_CAD", "XAU_AUD", "XAU_GBP", "XAU_JPY", "XAU_SGD", "XAU_CHF",
    "XAU_NZD",
    "BTC_USD", "LTC_USD", "BCH_USD",
    "US30_USD", "FR40_EUR", "US2000_USD", "CH20_CHF", "SPX500_USD", "AU200_AUD",
    "JP225_USD", "JP225Y_JPY", "SG30_SGD", "EU50_EUR", "HK33_HKD",
    "USB02Y_USD", "USB05Y_USD", "USB30Y_USD", "USB10Y_USD", "UK100_GBP",
    "AUD_USD", "EUR_USD", "GBP_USD", "USD_CHF", "USD_JPY", "USD_MXN", "USD_SEK", "USD_NOK",
    "NZD_USD", "USD_CAD", "USD_SGD", "ETH_USD", "XPT_USD", "XPD_USD",
    "USD_JPY", "GBP_JPY", "USD_SGD", "EUR_SEK",
    "DE30_EUR",
    "USD_CHF", "USD_SEK", "XCU_USD", "SUGAR_USD",
    "USD_MXN", "GBP_USD", "WTICO_USD", "EUR_JPY", "USD_NOK",
    "XAU_USD",
    "USD_CZK",  "WHEAT_USD",
    "EUR_USD", "SG30_SGD", "AU200_AUD", "XAG_USD",
    "EUR_GBP", "USD_CNH", "USD_CAD", "NAS100_USD",
    "EU50_EUR", "NATGAS_USD", "SOYBN_USD",
    "US2000_USD",
    "BCO_USD", "AUD_USD", "NZD_USD", "NZD_CHF", "WHEAT_USD",
    "JP225_USD", "SPX500_USD") %>%
  unique()

asset_infor <- get_instrument_info()
raw_macro_data <- get_macro_event_data()

load_custom_functions()
start_date = "2011-01-01"
bin_factor = NULL
stop_value_var = 2
profit_value_var = 15
period_var = 48
available_assets <- c("EUR_USD", "GBP_USD", "AUD_USD", "XAG_USD", "XAG_EUR", "EUR_GBP")
full_ts_trade_db_location = "C:/Users/Nikhil Chandra/Documents/trade_data/full_ts_trades_mapped_period_version.db"
full_ts_trade_db_con <- connect_db(path = full_ts_trade_db_location)
actual_wins_losses <-
  DBI::dbGetQuery(full_ts_trade_db_con,
                  glue::glue("SELECT * FROM full_ts_trades_mapped
                  WHERE stop_factor = {stop_value_var} AND
                        periods_ahead = {period_var} AND Date >= {start_date}")
  ) %>%
  mutate(
    Date = as_datetime(Date)
  )

actual_wins_losses <-
  actual_wins_losses %>%
  # filter(Asset %in% available_assets) %>%
  filter(stop_factor == stop_value_var,
         profit_factor == profit_value_var,
         periods_ahead == period_var)

DBI::dbDisconnect(full_ts_trade_db_con)
rm(full_ts_trade_db_con)
gc()

paramas_list <-
  list(
    c(
      "EUR_USD", #1
      "GBP_USD", #2
      "AUD_USD", #3
      "USD_JPY", #4
      "AU200_AUD", #5
      "EUR_GBP", #6
      "SPX500_USD", #7
      "EU50_EUR", #8
      "UK100_GBP", #9
      "US2000_USD", #10
      "USB02Y_USD", #11
      "USB10Y_USD", #12
      "UK10YB_GBP", #13
      "WTICO_USD", #14
      "BCO_USD", #15
      "XCU_USD", #16
      "XAG_USD", #17
      "SG30_SGD", #18
      "JP225Y_JPY", #19
      "FR40_EUR", #20
      "CH20_CHF", #21
      "HK33_HKD", #22
      "XAG_EUR", #23
      "XAG_GBP", #24
      "XAG_JPY",  #25
      "USD_CAD", #26
      "EUR_AUD", #27
      "NZD_USD", #28
      "EUR_NZD", #29
      "AUD_NZD", #30
      "GBP_AUD", #31
      "GBP_NZD", #32
      "GBP_CAD", #33
      "GBP_JPY", #34
      "USD_SGD", #35
      "EUR_JPY", #36
      "BTC_USD", #37
      "ETH_USD", #38
      "NATGAS_USD", #39
      "EUR_SEK", #40
      "LTC_USD"  #41
      ),
    list(
         c("usd", "gbp", "eur", "aud", "cad", "cny"), #1
         c("usd", "gbp", "eur", "aud", "cad", "cny"), #2
         c("usd", "gbp", "eur", "aud", "cad", "cny"), #3
         c("usd", "gbp", "eur", "aud", "cad", "cny"), #4
         c("usd", "gbp", "eur", "aud", "cad", "cny"), #5
         c("usd", "gbp", "eur", "aud", "cad", "cny"), #6
         c("usd", "gbp", "eur", "aud", "cad", "cny"), #7
         c("usd", "gbp", "eur", "aud", "cad", "cny"), #8
         c("usd", "gbp", "eur", "aud", "cad", "cny"), #9
         c("usd", "gbp", "eur", "aud", "cad", "cny"), #10
         c("usd", "gbp", "eur", "aud", "cad", "cny"), #11

         c("usd", "gbp", "eur", "aud", "cad", "cny"), #12
         c("usd", "gbp", "eur", "aud", "cad", "cny"), #13
         c("usd", "gbp", "eur", "aud", "cad", "cny"), #14
         c("usd", "gbp", "eur", "aud", "cad", "cny"), #15
         c("usd", "gbp", "eur", "aud", "cad", "cny"), #16
         c("usd", "gbp", "eur", "aud", "cad", "cny"), #17
         c("usd", "gbp", "eur", "aud", "cad", "cny", "jpy"), #18
         c("usd", "gbp", "eur", "aud", "cad", "cny", "jpy"), #19
         c("usd", "gbp", "eur", "aud", "cad", "cny"), #20
         c("usd", "gbp", "eur", "aud", "cad", "cny"), #21
         c("usd", "gbp", "eur", "aud", "cad", "cny"), #22
         c("usd", "gbp", "eur", "aud", "cad", "cny", "jpy"), #23
         c("usd", "gbp", "eur", "aud", "cad", "cny", "jpy"), #24
         c("usd", "gbp", "eur", "aud", "cad", "cny", "jpy"), #25
         c("usd", "cad"), #26
         c("eur", "aud"), #27
         c("usd", "gbp", "eur", "aud", "cad", "cny", "jpy"), #28
         c("usd", "gbp", "eur", "aud", "cad", "cny", "jpy"), #29
         c("usd", "gbp", "eur", "aud", "cad", "cny", "jpy"), #30
         c("usd", "gbp", "eur", "aud"), #31
         c("usd", "gbp", "eur", "aud", "cad", "cny", "jpy"), #32
         c("usd", "gbp", "eur", "cad"), #33
         c("usd", "gbp", "eur", "jpy"), #34
         c("usd", "gbp", "eur", "aud", "cad", "cny", "jpy"), #35
         c("usd", "gbp", "eur","jpy"), #36
         c("usd", "gbp", "eur", "aud", "cad", "cny", "jpy"), #37
         c("usd", "gbp", "eur", "aud", "cad", "cny", "jpy"), #38
         c("usd", "gbp", "eur", "aud", "cad", "cny", "jpy"), #39
         c("usd", "gbp", "eur"), #40
         c("usd", "gbp", "eur", "aud", "cad", "cny", "jpy") #41

         ),
    p_value_thresh = seq(0.15,
                         length(
                           c(
                             "EUR_USD", #1
                             "GBP_USD", #2
                             "AUD_USD", #3
                             "USD_JPY", #4
                             "AU200_AUD", #5
                             "EUR_GBP", #6
                             "SPX500_USD", #7
                             "EU50_EUR", #8
                             "UK100_GBP", #9
                             "US2000_USD", #10
                             "USB02Y_USD", #11
                             "USB10Y_USD", #12
                             "UK10YB_GBP", #13
                             "WTICO_USD", #14
                             "BCO_USD", #15
                             "XCU_USD", #16
                             "XAG_USD", #17
                             "SG30_SGD", #18
                             "JP225Y_JPY", #19
                             "FR40_EUR", #20
                             "CH20_CHF", #21
                             "HK33_HKD", #22
                             "XAG_EUR", #23
                             "XAG_GBP", #24
                             "XAG_JPY", #25
                             "USD_CAD", #26
                             "EUR_AUD", #27
                             "NZD_USD", #28
                             "EUR_NZD", #29
                             "AUD_NZD", #30
                             "GBP_AUD", #31
                             "GBP_NZD", #32
                             "GBP_CAD", #33
                             "GBP_JPY", #34
                             "USD_SGD", #35
                             "EUR_JPY", #36
                             "BTC_USD", #37
                             "ETH_USD",  #38
                             "NATGAS_USD", #39
                             "EUR_SEK", #40
                             "LTC_USD"  #41
                           )
                                )
                         )
  )

analysis_list_long <- list()
analysis_list_short <- list()

# goodParams:
# p_value_thresh = 0.15
# stop_value_var = 4
# profit_value_var = 15
# period_var = 48
# Best So Far:------------
# p_value_thresh = 0.15
# start_date = "2011-01-01"
# stop_value_var = 2
# profit_value_var = 15
# period_var = 48
# split_dates_dates <-
#   sim_dates %>%
#   map(~ .x + years(9)) %>%
#   unlist() %>%
#   as_date()
# Best So Far 2:------------
# p_value_thresh = 10^-7
# start_date = "2011-01-01"
# stop_value_var = 2
# profit_value_var = 15
# period_var = 48
# split_dates_dates <-
#   sim_dates %>%
#   map(~ .x + years(9)) %>%
#   unlist() %>%
#   as_date()
# Best So Far 3:------------
# Based on Analysis post 2020 we need to add post-2020 data to make sure we can
# model
# p_value_thresh = 10^-7, 0.15 also Works
# start_date = "2011-01-01"
# stop_value_var = 2
# profit_value_var = 15
# period_var = 48
# split_dates_dates <-
#   sim_dates %>%
#   map(~ .x + years(11)) %>%
#   unlist() %>%
#   as_date()

sim_dates <-
  c(as_date("2011-01-01"))
split_dates_dates <-
  sim_dates %>%
  map(~ (.x + years(14)) + months(1) ) %>%
  unlist() %>%
  as_date()
end_dates <-
  split_dates_dates %>%
  map(~ .x + years(10)) %>%
  unlist() %>%
  as_date()

DBI::dbDisconnect(NN_sims_db_con)
gc()
redo_db = TRUE
NN_sims_db <- "C:/Users/Nikhil Chandra/Documents/trade_data/Indices_Silver_Logit_sims_Daily_Port.db"
NN_sims_db_con <- connect_db(path = NN_sims_db)
c = 0
safely_gen_model <-
  safely(generate_Logit_single_asset_model, otherwise = NULL)

for (k in 1:length(sim_dates)) {

  for (i in 1:length(paramas_list[[1]]) ) {

    gc()

    temp_long <-
      safely_gen_model(
        actual_wins_losses = actual_wins_losses %>%
          filter(Date >= sim_dates[k] & Date <= end_dates[k]),
        raw_macro_data = raw_macro_data,
        countries_to_use = paramas_list[[2]][[i]],
        asset_var = paramas_list[[1]][[i]],
        pred_filter = 0.5,
        train_split_date = split_dates_dates[k],
        lag_days= 1,
        profit_value_var = profit_value_var,
        stop_value_var = stop_value_var,
        p_value_thresh_for_inputs = paramas_list[[3]][[i]],
        periods_ahead_var = period_var,
        trade_direction = "Long"
      ) %>%
      pluck('result')

    temp_short <-
      safely_gen_model(
        actual_wins_losses = actual_wins_losses %>%
          filter(Date >= sim_dates[k] & Date <= end_dates[k]),
        raw_macro_data = raw_macro_data,
        countries_to_use = paramas_list[[2]][[i]],
        asset_var = paramas_list[[1]][[i]],
        pred_filter = 0.5,
        train_split_date = split_dates_dates[k],
        lag_days= 1,
        profit_value_var = profit_value_var,
        stop_value_var = stop_value_var,
        p_value_thresh_for_inputs = paramas_list[[3]][[i]],
        periods_ahead_var = period_var,
        trade_direction = "Short",
        save_model_location = "C:/Users/Nikhil Chandra/Documents/trade_data/asset_specific_NNs_Portfolio_macro_only"
      ) %>%
      pluck('result')

    if(!is.null(temp_short) & !is.null(temp_long)) {

      c = c + 1

      analysis_list_long[[c]] <-
        temp_long[[1]] %>%
        mutate(
          sim_date_start = sim_dates[k],
          split_date = split_dates_dates[k],
          sim_end_date = end_dates[k],
          p_value_thresh_for_inputs = paramas_list[[3]][[i]]
        )

      analysis_list_short[[c]] <-
        temp_short[[1]] %>%
        mutate(
          sim_date_start = sim_dates[k],
          split_date = split_dates_dates[k],
          sim_end_date = end_dates[k],
          p_value_thresh_for_inputs = paramas_list[[3]][[i]]
        )


      all_asset_logit_results <-
        analysis_list_long[[c]] %>%
        bind_rows(analysis_list_short[[c]])

      if(redo_db == TRUE) {
        NN_sims_db_con <- connect_db(path = NN_sims_db)
        write_table_sql_lite(.data = all_asset_logit_results,
                             table_name = "Indices_Silver_Logit_sims",
                             conn = NN_sims_db_con, overwrite_true = TRUE)
        redo_db = FALSE
      } else {
        append_table_sql_lite(.data = all_asset_logit_results,
                              table_name = "Indices_Silver_Logit_sims",
                              conn = NN_sims_db_con)

      }

    }

  }

}


db_data <-
  DBI::dbGetQuery(conn = NN_sims_db_con, statement = "SELECT * FROM Indices_Silver_Logit_sims") %>%
  mutate(
    trade_return_dollar_aud =
      case_when(
        str_detect(Asset, "JPY") ~ trade_return_dollar_aud/100,
        TRUE ~ trade_return_dollar_aud
      )
  )

complete_portfolio_structure_asset <-
  db_data %>%
  mutate(
    # Date = End_Date,
    Date = as_datetime(Date)
  ) %>%
  distinct() %>%
  filter(Logit_Pred > 0.5) %>%
  group_by(Date, Asset, trade_col) %>%
  summarise(trade_return_dollar_aud = median(trade_return_dollar_aud, na.rm = T)) %>%
  group_by(Date, Asset, trade_col) %>%
  summarise(trade_return_dollar_aud = sum(trade_return_dollar_aud, na.rm = T)) %>%
  group_by(Asset, trade_col) %>%
  arrange(Date , .by_group = TRUE) %>%
  group_by(Asset, trade_col) %>%
  mutate(
    cumulative_sum = cumsum(trade_return_dollar_aud)
  ) %>%
  ungroup()

complete_portfolio_structure_asset %>%
  ggplot(aes(x = Date, y = cumulative_sum, color = trade_col)) +
  geom_line() +
  theme_minimal() +
  facet_wrap(.~ Asset, scales = "free") +
  theme(legend.position = "bottom")


complete_portfolio_structure <-
  db_data %>%
  mutate(
    # Date = End_Date,
    Date = as_datetime(Date)
  ) %>%
  filter(Logit_Pred > 0.8) %>%
  # filter(trade_col == "Long") %>%
  group_by(Date, Asset, trade_col) %>%
  summarise(trade_return_dollar_aud = median(trade_return_dollar_aud, na.rm = T)) %>%
  # filter(!(Asset %in% c("USD_JPY"))) %>%
  group_by(Date) %>%
  summarise(trade_return_dollar_aud = sum(trade_return_dollar_aud, na.rm = T)) %>%
  ungroup() %>%
  arrange(Date) %>%
  mutate(
    cumulative_sum = cumsum(trade_return_dollar_aud)
  ) %>%
  ungroup()

complete_portfolio_structure %>%
  ggplot(aes(x = Date, y = cumulative_sum)) +
  geom_line() +
  theme_minimal() +
  theme(legend.position = "bottom")


analysis_by_sim_date <-
  db_data %>%
  mutate(
    # Date = End_Date,
    Date = as_datetime(Date)
  )  %>%
  mutate(
    Returns_Pred_0 =
      case_when(
        Logit_Pred >= 0  ~ trade_return_dollar_aud,
        TRUE ~ 0
      )
  ) %>%
  mutate(
    # Returns_Pred_10 =
    #   case_when(
    #     Logit_Pred >= 0.1  ~ trade_return_dollar_aud,
    #     TRUE ~ 0
    #   ),
    # Returns_Pred_20 =
    #   case_when(
    #     Logit_Pred >= 0.2  ~ trade_return_dollar_aud,
    #     TRUE ~ 0
    #   ),
    Returns_Pred_30 =
      case_when(
        Logit_Pred >= 0.3  ~ trade_return_dollar_aud,
        TRUE ~ 0
      ),
    Returns_Pred_40 =
      case_when(
        Logit_Pred >= 0.4  ~ trade_return_dollar_aud,
        TRUE ~ 0
      ),
    Returns_Pred_50 =
      case_when(
        Logit_Pred >= 0.5  ~ trade_return_dollar_aud,
        TRUE ~ 0
      ),
    Returns_Pred_60 =
      case_when(
        Logit_Pred >= 0.6  ~ trade_return_dollar_aud,
        TRUE ~ 0
      ),
    Returns_Pred_70 =
      case_when(
        Logit_Pred >= 0.7  ~ trade_return_dollar_aud,
        TRUE ~ 0
      ),
    Returns_Pred_80 =
      case_when(
        Logit_Pred >= 0.8  ~ trade_return_dollar_aud,
        TRUE ~ 0
      ),
    Returns_Pred_90 =
      case_when(
        Logit_Pred >= 0.9  ~ trade_return_dollar_aud,
        TRUE ~ 0
      )
  ) %>%
  mutate(
    sim_date_start = as_date(sim_date_start)
  ) %>%
  group_by(sim_date_start, Asset, trade_col) %>%
  summarise(
    across(contains("Returns_Pred"), ~ round(sum(., na.rm = T)) )
  ) %>%
  group_by( Asset, trade_col) %>%
  summarise(
    across(contains("Returns_Pred"), ~ round(mean(., na.rm = T)) )
  )

control_data <-
  analysis_by_sim_date %>%
  dplyr::select(
    Asset, trade_col, Control_Returns = Returns_Pred_0
  )

analysis_by_sim_date_long <-
  analysis_by_sim_date %>%
  dplyr::select(-Returns_Pred_0) %>%
  pivot_longer(-c(Asset, trade_col), names_to = "Pred_Thresh", values_to = "Returns") %>%
  mutate(
    Pred_Thresh =
      (str_remove_all(Pred_Thresh, "[A-Z]+|[a-z]+|\\_") %>% str_trim() %>% as.numeric())/100
  ) %>%
  left_join(control_data) %>%
  mutate(
    Better_Then_Control = Returns>Control_Returns
  ) %>%
  group_by(Asset) %>%
  filter(any(Better_Then_Control == TRUE)) %>%
  # filter(Returns > 0) %>%
  group_by(Asset, trade_col) %>%
  slice_max(Returns) %>%
  group_by(Asset, trade_col) %>%
  slice_min(Pred_Thresh)

analyse_ideal_scenario <-
  db_data %>%
  left_join(
    analysis_by_sim_date_long %>%
      dplyr::select(Asset, trade_col, Pred_Filter = Pred_Thresh)
  ) %>%
  mutate(
    # Date = End_Date,
    Date = as_datetime(Date)
  ) %>%
  filter(Logit_Pred > Pred_Filter) %>%
  # filter(trade_col == "Long") %>%
  group_by(Date, Asset, trade_col) %>%
  summarise(trade_return_dollar_aud = median(trade_return_dollar_aud, na.rm = T)) %>%
  # filter(!(Asset %in% c("USD_JPY"))) %>%
  group_by(Date) %>%
  summarise(trade_return_dollar_aud = sum(trade_return_dollar_aud, na.rm = T)) %>%
  ungroup() %>%
  arrange(Date) %>%
  mutate(
    cumulative_sum = cumsum(trade_return_dollar_aud)
  ) %>%
  ungroup()

analyse_ideal_scenario %>%
  ggplot(aes(x = Date, y = cumulative_sum)) +
  geom_line() +
  theme_minimal() +
  theme(legend.position = "bottom")

#-------------------------------------Store Results
analysis_by_sim_date_long <-
  analysis_by_sim_date_long %>%
  mutate(
    stop_factor = stop_value_var,
    profit_factor = profit_value_var,
    period_var = period_var
  )
daily_port_best_results_store <-
  "C:/Users/Nikhil Chandra/Documents/trade_data/Indices_Silver_Logit_sims_Daily_Port_best_results.db"
daily_port_best_results_store_con <- connect_db(path = daily_port_best_results_store)
write_table_sql_lite(.data = analysis_by_sim_date_long,
                     table_name = "Indices_Silver_Logit_sims_Daily_Port_best_results",
                     conn = daily_port_best_results_store_con,
                     overwrite_true = TRUE)
DBI::dbDisconnect(daily_port_best_results_store_con)
gc()


