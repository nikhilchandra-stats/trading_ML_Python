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
  start_date = (today() - days(5)) %>% as.character()
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

major_indices <-
  get_all_major_indices(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame
  )

major_indices_bid <-
  get_all_major_indices(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame
  )

mean_values_by_asset_for_loop <-
  wrangle_asset_data(
    asset_data_daily_raw = major_indices,
    summarise_means = TRUE
  )

major_indices$Asset %>% unique()
gc()

control_random_samples <-
  get_random_samples_MLE_beta(
    random_results_db_location = "C:/Users/Nikhil Chandra/Documents/trade_data/random_results.db",
    stop_factor = 5,
    profit_factor = 10,
    analysis_syms = c("AU200_AUD", "SPX500_USD", "EU50_EUR", "US2000_USD"),
    time_frame = "H1",
    return_summary = TRUE
  )

major_indices_log_cumulative <-
  c("SPX500_USD", "US2000_USD", "NAS100_USD", "SG30_SGD", "AU200_AUD", "EU50_EUR", "DE30_EUR") %>%
  map_dfr(
    ~
      create_log_cumulative_returns(
        asset_data_to_use = major_indices,
        asset_to_use = c(.x[1]),
        price_col = "Open",
        return_long_format = TRUE
      )
  ) %>%
  left_join(
    major_indices %>% distinct(Date, Asset, Price, Open, High, Low)
  )

major_indices_log_cumulative_bid <-
  c("SPX500_USD", "US2000_USD", "NAS100_USD", "SG30_SGD", "AU200_AUD", "EU50_EUR", "DE30_EUR") %>%
  map_dfr(
    ~
      create_log_cumulative_returns(
        asset_data_to_use = major_indices_bid,
        asset_to_use = c(.x[1]),
        price_col = "Open",
        return_long_format = TRUE
      )
  ) %>%
  left_join(
    major_indices_bid %>% distinct(Date, Asset, Price, Open, High, Low)
  )

full_ts_trade_db_location = "C:/Users/Nikhil Chandra/Documents/trade_data/full_ts_trades_mapped.db"
full_ts_trade_db_con <- connect_db(full_ts_trade_db_location)

Indices_Trades_long <-
  major_indices_log_cumulative %>%
  filter(Date >= "2016-01-01") %>%
  mutate(
    trade_col = "Long"
  )

Indices_Long_Data_15_20 <-
  run_pairs_analysis(
    tagged_trades = Indices_Trades_long,
    stop_factor = 15,
    profit_factor = 20,
    raw_asset_data = major_indices_log_cumulative,
    risk_dollar_value = 10,
    return_trade_ts = TRUE
  ) %>%
  mutate(  stop_factor = 15,
           profit_factor = 20)

append_table_sql_lite(.data = Indices_Long_Data_15_20,
   table_name = "full_ts_trades_mapped",
   conn = full_ts_trade_db_con)

Indices_Long_Data_10_15 <-
  run_pairs_analysis(
    tagged_trades = Indices_Trades_long,
    stop_factor = 10,
    profit_factor = 15,
    raw_asset_data = major_indices_log_cumulative,
    risk_dollar_value = 10,
    return_trade_ts = TRUE
  ) %>%
  mutate(  stop_factor = 10,
           profit_factor = 15)

append_table_sql_lite(.data = Indices_Long_Data_10_15,
                      table_name = "full_ts_trades_mapped",
                      conn = full_ts_trade_db_con)

Indices_Long_Data_8_12 <-
  run_pairs_analysis(
    tagged_trades = Indices_Trades_long,
    stop_factor = 8,
    profit_factor = 12,
    raw_asset_data = major_indices_log_cumulative,
    risk_dollar_value = 10,
    return_trade_ts = TRUE
  ) %>%
  mutate(  stop_factor = 8,
           profit_factor = 12)

append_table_sql_lite(.data = Indices_Long_Data_8_12,
                      table_name = "full_ts_trades_mapped",
                      conn = full_ts_trade_db_con)

Indices_Long_Data_4_8 <-
  run_pairs_analysis(
    tagged_trades = Indices_Trades_long,
    stop_factor = 4,
    profit_factor = 8,
    raw_asset_data = major_indices_log_cumulative,
    risk_dollar_value = 10,
    return_trade_ts = TRUE
  ) %>%
  mutate(  stop_factor = 4,
           profit_factor = 8)

append_table_sql_lite(.data = Indices_Long_Data_4_8,
                      table_name = "full_ts_trades_mapped",
                      conn = full_ts_trade_db_con)


Indices_Long_Data_5_6 <-
  run_pairs_analysis(
    tagged_trades = Indices_Trades_long,
    stop_factor = 5,
    profit_factor = 6,
    raw_asset_data = major_indices_log_cumulative,
    risk_dollar_value = 10,
    return_trade_ts = TRUE
  ) %>%
  mutate(  stop_factor = 5,
           profit_factor = 6)

append_table_sql_lite(.data = Indices_Long_Data_5_6,
                      table_name = "full_ts_trades_mapped",
                      conn = full_ts_trade_db_con)


rm(Indices_Long_Data_15_20, Indices_Long_Data_10_15,
   Indices_Long_Data_4_8, Indices_Long_Data_8_12, Indices_Trades_long,
   Indices_Long_Data_5_6)
gc()
#-------------------------------------------------------
Indices_Trades_Short <-
  major_indices_log_cumulative_bid %>%
  filter(Date >= "2016-01-01") %>%
  mutate(
    trade_col = "Short"
  )

Indices_Short_Data_15_20 <-
  run_pairs_analysis(
    tagged_trades = Indices_Trades_Short,
    stop_factor = 15,
    profit_factor = 20,
    raw_asset_data = major_indices_log_cumulative_bid,
    risk_dollar_value = 10,
    return_trade_ts = TRUE
  ) %>%
  mutate(  stop_factor = 15,
           profit_factor = 20)

append_table_sql_lite(.data = Indices_Short_Data_15_20,
                      table_name = "full_ts_trades_mapped",
                      conn = full_ts_trade_db_con)

Indices_Short_Data_10_15 <-
  run_pairs_analysis(
    tagged_trades = Indices_Trades_Short,
    stop_factor = 10,
    profit_factor = 15,
    raw_asset_data = major_indices_log_cumulative_bid,
    risk_dollar_value = 10,
    return_trade_ts = TRUE
  ) %>%
  mutate(  stop_factor = 10,
           profit_factor = 15)

append_table_sql_lite(.data = Indices_Short_Data_10_15,
                      table_name = "full_ts_trades_mapped",
                      conn = full_ts_trade_db_con)

Indices_Short_Data_8_12 <-
  run_pairs_analysis(
    tagged_trades = Indices_Trades_Short,
    stop_factor = 8,
    profit_factor = 12,
    raw_asset_data = major_indices_log_cumulative_bid,
    risk_dollar_value = 10,
    return_trade_ts = TRUE
  ) %>%
  mutate(  stop_factor = 8,
           profit_factor = 12)

append_table_sql_lite(.data = Indices_Short_Data_8_12,
                      table_name = "full_ts_trades_mapped",
                      conn = full_ts_trade_db_con)

Indices_Short_Data_4_8 <-
  run_pairs_analysis(
    tagged_trades = Indices_Trades_Short,
    stop_factor = 4,
    profit_factor = 8,
    raw_asset_data = major_indices_log_cumulative_bid,
    risk_dollar_value = 10,
    return_trade_ts = TRUE
  ) %>%
  mutate(  stop_factor = 4,
           profit_factor = 8)

append_table_sql_lite(.data = Indices_Short_Data_4_8,
                      table_name = "full_ts_trades_mapped",
                      conn = full_ts_trade_db_con)

Indices_Short_Data_5_6 <-
  run_pairs_analysis(
    tagged_trades = Indices_Trades_Short,
    stop_factor = 5,
    profit_factor = 6,
    raw_asset_data = major_indices_log_cumulative_bid,
    risk_dollar_value = 10,
    return_trade_ts = TRUE
  ) %>%
  mutate(  stop_factor = 5,
           profit_factor = 6)

append_table_sql_lite(.data = Indices_Short_Data_5_6,
                      table_name = "full_ts_trades_mapped",
                      conn = full_ts_trade_db_con)


DBI::dbDisconnect(full_ts_trade_db_con)
rm(full_ts_trade_db_con)
gc()

rm(Indices_Short_Data_15_20, Indices_Short_Data_10_15,
   Indices_Short_Data_4_8, Indices_Short_Data_8_12, Indices_Trades_Short,
   Indices_Short_Data_5_6)
gc()


#' get_Major_Indices_trade_ts_actuals
#'
#' @param full_ts_trade_db_location
#' @param asset_infor
#' @param currency_conversion
#' @param stop_factor_var
#' @param profit_factor_var
#'
#' @return
#' @export
#'
#' @examples
get_Major_Indices_trade_ts_actuals <-
  function(
    full_ts_trade_db_location = full_ts_trade_db_location,
    asset_infor = asset_infor,
    currency_conversion = currency_conversion,
    stop_factor_var = 10,
    profit_factor_var = 15
    ) {

    full_ts_trade_db_con <- connect_db(full_ts_trade_db_location)

    shorts <-
      DBI::dbGetQuery(conn = full_ts_trade_db_con,
                      "SELECT * FROM full_ts_trades_mapped") %>%
      filter(trade_col == "Short") %>%
      filter(stop_factor == stop_factor_var,
             profit_factor == profit_factor_var) %>%
      rename(
        Date = dates,
        Asset = asset
      ) %>%
      convert_stop_profit_AUD(
        asset_infor = asset_infor,
        currency_conversion = currency_conversion,
        asset_col = "Asset",
        stop_col = "starting_stop_value",
        profit_col = "starting_profit_value",
        price_col = "trade_start_prices",
        risk_dollar_value = 10,
        returns_present = FALSE,
        trade_return_col = "trade_return") %>%
      mutate(
        trade_returns_orig = trade_returns,
        trade_returns =
          case_when(
            trade_returns_orig > 0 ~ maximum_win,
            trade_returns_orig <= 0 ~ -1*minimal_loss,
          )
      ) %>%
      dplyr::select(-trade_returns_orig)

    longs <-
      DBI::dbGetQuery(conn = full_ts_trade_db_con,
                      "SELECT * FROM full_ts_trades_mapped") %>%
      filter(trade_col == "Long") %>%
      filter(stop_factor == stop_factor_var,
             profit_factor == profit_factor_var) %>%
      rename(
        Date = dates,
        Asset = asset
      ) %>%
      convert_stop_profit_AUD(
        asset_infor = asset_infor,
        currency_conversion = currency_conversion,
        asset_col = "Asset",
        stop_col = "starting_stop_value",
        profit_col = "starting_profit_value",
        price_col = "trade_start_prices",
        risk_dollar_value = 10,
        returns_present = FALSE,
        trade_return_col = "trade_return") %>%
      mutate(
        trade_returns_orig = trade_returns,
        trade_returns =
          case_when(
            trade_returns_orig > 0 ~ maximum_win,
            trade_returns_orig <= 0 ~ -1*minimal_loss,
          )
      ) %>%
      dplyr::select(-trade_returns_orig)

    DBI::dbDisconnect(full_ts_trade_db_con)

    return(
      list(longs, shorts)
    )

  }


#--------------------------------------------
all_trade_ts_actuals <-
  get_Major_Indices_trade_ts_actuals(
    full_ts_trade_db_location = "C:/Users/Nikhil Chandra/Documents/trade_data/full_ts_trades_mapped.db",
    asset_infor = asset_infor,
    currency_conversion = currency_conversion,
    stop_factor_var = 10,
    profit_factor_var = 15
  )
longs <- all_trade_ts_actuals[[1]]
shorts <- all_trade_ts_actuals[[2]]
lag_days <- 1
raw_macro_data <- get_macro_event_data()

returns_data <-
  longs %>%
  dplyr::select(Asset,
                Date,
                trade_returns_long = trade_returns) %>%
  mutate(Date = as_datetime(Date)) %>%
  left_join(
    shorts %>%
      dplyr::select(Asset,
                    Date,
                    trade_returns_short = trade_returns) %>%
      mutate(Date = as_datetime(Date))
  ) %>%
  rename(
     Date = Date,
     Asset = Asset
  ) %>%
  filter(!is.na(trade_returns_short), !is.na(trade_returns_long))


aus_macro_data <-
  get_AUS_Indicators(raw_macro_data,
                     lag_days = lag_days) %>%
  janitor::clean_names()
nzd_macro_data <-
  get_NZD_Indicators(raw_macro_data,
                     lag_days = lag_days) %>%
  janitor::clean_names()
usd_macro_data <-
  get_USD_Indicators(raw_macro_data,
                     lag_days = lag_days) %>%
  janitor::clean_names()
cny_macro_data <-
  get_CNY_Indicators(raw_macro_data,
                     lag_days = lag_days) %>%
  janitor::clean_names()
eur_macro_data <-
  get_EUR_Indicators(raw_macro_data,
                     lag_days = lag_days) %>%
  janitor::clean_names()

aud_macro_vars <- names(aus_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
nzd_macro_vars <- names(nzd_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
usd_macro_vars <- names(usd_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
cny_macro_vars <- names(cny_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
eur_macro_vars <- names(eur_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
all_macro_vars <- c(
                    # aud_macro_vars,
                    # nzd_macro_vars,
                    usd_macro_vars,
                    cny_macro_vars,
                    eur_macro_vars
                    )

copula_data_SPX_US2000 <-
  estimating_dual_copula(
    asset_data_to_use = major_indices,
    asset_to_use = c("SPX500_USD", "US2000_USD"),
    price_col = "Open",
    rolling_period = 100,
    samples_for_MLE = 0.15,
    test_samples = 0.85
  )

lm_data <-
  major_indices %>%
  group_by(Asset) %>%
  arrange(Date, .by_group = TRUE) %>%
  group_by(Asset) %>%
  mutate(High_to_Open = abs(lag(High) - lag(Open)),
         High_to_Open_5 = abs(lag(High) - lag(Open, 5)),
         High_to_Open_10 = abs(lag(High) - lag(Open, 10)),
         High_to_Open_15 = abs(lag(High) - lag(Open, 15)),
         High_to_Open_20 = abs(lag(High) - lag(Open, 20)),
         High_to_Open_ma = slider::slide_dbl(.x = High_to_Open, .f = ~ mean(.x, na.rm = T) ,.before = 50),
         High_to_Open_5_ma = slider::slide_dbl(.x = High_to_Open_5, .f = ~ mean(.x, na.rm = T) ,.before = 50),
         High_to_Open_10_ma = slider::slide_dbl(.x = High_to_Open_10, .f = ~ mean(.x, na.rm = T) ,.before = 50),
         High_to_Open_15_ma = slider::slide_dbl(.x = High_to_Open_15, .f = ~ mean(.x, na.rm = T) ,.before = 50),
         High_to_Open_20_ma = slider::slide_dbl(.x = High_to_Open_20, .f = ~ mean(.x, na.rm = T) ,.before = 50),
         High_to_Open_lead_1 = abs(lead(High) - lead(Open)),
         High_to_Open_lead_10 = abs(lead(High, 10) - lead(Open)),
         High_to_Open_lead_20 = abs(lead(High, 20) - lead(Open)),

         High_to_Open_5_tan = atan((lag(High) - lag(Open, 5))/5),
         High_to_Open_10_tan = atan((lag(High) - lag(Open, 10))/10),
         High_to_Open_15_tan = atan((lag(High) - lag(Open, 15))/15)
  ) %>%
  mutate(Low_to_Open = abs(lag(Low) - lag(Open)),
         Low_to_Open_5 = abs(lag(Low) - lag(Open, 5)),
         Low_to_Open_10 = abs(lag(Low) - lag(Open, 10)),
         Low_to_Open_15 = abs(lag(Low) - lag(Open, 15)),
         Low_to_Open_20 = abs(lag(Low) - lag(Open, 20)),
         Low_to_Open_ma = slider::slide_dbl(.x = Low_to_Open, .f = ~ mean(.x, na.rm = T) ,.before = 50),
         Low_to_Open_5_ma = slider::slide_dbl(.x = Low_to_Open_5, .f = ~ mean(.x, na.rm = T) ,.before = 50),
         Low_to_Open_10_ma = slider::slide_dbl(.x = Low_to_Open_10, .f = ~ mean(.x, na.rm = T) ,.before = 50),
         Low_to_Open_15_ma = slider::slide_dbl(.x = Low_to_Open_15, .f = ~ mean(.x, na.rm = T) ,.before = 50),
         Low_to_Open_20_ma = slider::slide_dbl(.x = Low_to_Open_20, .f = ~ mean(.x, na.rm = T) ,.before = 50),
         Low_to_Open_lead_1 = abs(lead(Low) - lead(Open)),
         Low_to_Open_lead_10 = abs(lead(Low, 10) - lead(Open)),
         Low_to_Open_lead_20 = abs(lead(Low, 20) - lead(Open)),

         Low_to_Open_5_tan = atan((lag(Low) - lag(Open, 5))/5),
         Low_to_Open_10_tan = atan((lag(Low) - lag(Open, 10))/10),
         Low_to_Open_15_tan = atan((lag(Low) - lag(Open, 15))/15)
  ) %>%
  ungroup()

ask_independants <-
  names(lm_data) %>%
  keep(~ str_detect(.x, "High_to_") & !str_detect(.x, "lead"))
bid_independants <-
  names(lm_data) %>%
  keep(~ str_detect(.x, "Low_to_") & !str_detect(.x, "lead"))

major_indices_with_wins_losses <-
  lm_data %>%
  ungroup() %>%
  left_join(returns_data) %>%
  filter(!is.na(trade_returns_long))

returns_min_date <-
  major_indices_with_wins_losses %>%
  pull(Date) %>%
  min()

complete_LM_data <-
  major_indices_with_wins_losses %>%
  mutate(
    combined_return = trade_returns_long + trade_returns_short,
    bin_var =
      case_when(
        combined_return > 0 ~ "win",
        combined_return <= 0 ~ "loss"
      )
  ) %>%
  mutate(Date_for_join = as_date(Date)) %>%
  # left_join(
  #   aus_macro_data %>%
  #     rename(Date_for_join = date)
  # ) %>%
  # left_join(
  #   nzd_macro_data %>%
  #     rename(Date_for_join = date)
  # ) %>%
  left_join(
    usd_macro_data %>%
      rename(Date_for_join = date)
  ) %>%
  left_join(
    cny_macro_data %>%
      rename(Date_for_join = date)
  )  %>%
  left_join(
    eur_macro_data %>%
      rename(Date_for_join = date)
  ) %>%
  group_by(Asset) %>%
  fill(!contains("AUD_USD|Date"), .direction = "down") %>%
  ungroup() %>%
  left_join(copula_data_SPX_US2000) %>%
  filter(!is.na(SPX500_USD_US2000_USD_quant_lm_mean), !is.na(SPX500_USD_US2000_USD_cor_mean)) %>%
  filter(if_all(everything() ,.fns = ~ !is.na(.)))

NN_train <- complete_LM_data %>%
  group_by(Asset) %>%
  slice_head(prop = 0.5) %>%
  ungroup()

NN_test <- complete_LM_data %>%
  group_by(Asset) %>%
  slice_tail(prop = 0.4) %>%
  ungroup()

SPX500_LM_dat <- NN_train %>%
  filter(Asset == "SPX500_USD")
SPX500_test_dat <- NN_test %>%
  filter(Asset == "SPX500_USD")

copula_vars <-
  names(NN_train) %>% keep(~ str_detect(.x,"quantiles|tangent|cor"))

NN_model_form <-
  create_lm_formula(dependant = "bin_var == 'win'",
                    independant = c(ask_independants, bid_independants, copula_vars, all_macro_vars)
  )


logit_model_1 <- glm(formula = NN_model_form,
                     data = SPX500_LM_dat,
                     family = binomial(link = "logit"))
summary(logit_model_1)
logit_predicted <- predict.glm(object = logit_model_1,
                               newdata = SPX500_test_dat,
                               type = 'response') %>%
  as.numeric()


NN_model_1_SPX <- neuralnet::neuralnet(formula = NN_model_form,
                                   hidden = c(60),
                                   data = SPX500_LM_dat,
                                   lifesign = 'full',
                                   rep = 1,
                                   stepmax = 1000000,
                                   threshold = 1)

NN_pred_1_SPX <-
  predict(object = NN_model_1,
              newdata = SPX500_test_dat) %>%
  as.numeric()

NN_SPX_pred <- list()
threshes <- c(0.5, 0.52, 0.54, 0.56, 0.58, 0.6, 0.7, 0.8, 0.9, 0.9999)
# threshes <- c(0.05, 0.1, 0.2, 0.3, 0.4, 0.5)

SPX_win_loss_AUD <-
  SPX500_test_dat %>%
  dplyr::distinct(trade_returns_long,
                  trade_returns_short,
                  combined_return,
                  bin_var) %>%
  group_by(bin_var) %>%
  distinct(combined_return)


control_SPX <-
  SPX500_test_dat %>%
  group_by( bin_var) %>%
  summarise(counts_control = n()) %>%
  mutate(
    Perc_Control = counts_control/sum(counts_control)
  )

for (i in 1:length(threshes)) {

  NN_SPX_pred[[i]] <-
    SPX500_test_dat %>%
    mutate(
      NN_SPX_pred = logit_predicted
      # NN_pred = NN_pred

    ) %>%
    mutate(
      pred_thresh =
        case_when(
          NN_SPX_pred > threshes[i] ~ "Take Trade",
          TRUE ~ "No Trade"
        )
    ) %>%
    filter(pred_thresh == "Take Trade" ) %>%
    group_by(pred_thresh, bin_var) %>%
    summarise(counts = n(),
              returns = sum(trade_returns_long)) %>%
    # group_by(Asset) %>%
    mutate(
      Perc = counts/sum(counts),
      Thresh = threshes[i],
      Total_return = sum(returns)
    ) %>%
    dplyr::select(-returns)
}

NN_SPX_pred_dfr <-
  NN_SPX_pred %>%
  map_dfr(bind_rows) %>%
  left_join(control_SPX) %>%
  filter(bin_var == "win") %>%
  bind_cols(SPX_win_loss_AUD %>%
              pivot_wider(names_from = bin_var, values_from = combined_return)) %>%
  mutate(
    total_return = counts*(Perc*win - (1 - Perc)*loss),
    control_return = counts_control*(Perc_Control*win - (1 - Perc_Control)*loss)
  )


