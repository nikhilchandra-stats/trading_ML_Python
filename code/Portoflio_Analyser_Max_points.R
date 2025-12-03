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
  start_date = (today() - days(5)) %>% as.character()
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
    "JP225_USD", "SPX500_USD",
    "EUR_AUD", "EUR_NZD", "EUR_CHF", "ESPIX_EUR" ,"EUR_NZD" ,
    "GBP_AUD", "GBP_NZD", "UK100_GBP", "UK10YB_GBP", "GBP_CHF", "GBP_CAD",
    "NL25_EUR") %>%
  unique()

asset_infor <- get_instrument_info()
raw_macro_data <- get_macro_event_data()

db_location = "D:/Asset Data/Oanda_Asset_Data Algo 2.db"
start_date = "2025-11-17"
end_date = "2025-11-30"

Indices_Metals_Bonds <-
  get_Port_Buy_Data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    time_frame = "H1"
  )

model_data_store_path <-
  "C:/Users/Nikhil Chandra/Documents/trade_data/single_asset_improved_indcator_trades_ts_more_cop.db"
model_data_store_db <-
  connect_db(model_data_store_path)
gc()

indicator_data <-
  DBI::dbGetQuery(conn = model_data_store_db,
                  statement = "SELECT * FROM single_asset_improved") %>%
  distinct() %>%
  group_by(sim_index, Asset) %>%
  mutate(Date = as_datetime(Date),
         test_date_start = as_date(test_date_start),
         test_end_date = as_date(test_end_date),
         Date_filt = as_date(Date)) %>%
  filter(start_date <= test_date_start)


DBI::dbDisconnect(model_data_store_db)
rm(model_data_store_db)
gc()

pred_thresh <- 0

indicator_data <-
  indicator_data %>%
  group_by(Asset, Date, trade_col) %>%
  slice_max(sim_index) %>%
  ungroup()

distinct_assets <-
  Indices_Metals_Bonds[[1]] %>%
  pull(Asset) %>%
  unique()

port_return_list <- list()

for (i in 1:length(distinct_assets)) {

  # indicator_data_tagged <-
  #   indicator_data %>%
  #   ungroup() %>%
  #   filter(Date >= test_date_start) %>%
  #   filter(
  #     (logit_combined_pred >= mean_logit_combined_pred + pred_thresh*sd_logit_combined_pred &
  #        averaged_pred >=  mean_averaged_pred + sd_averaged_pred*pred_thresh & pred_thresh >= 0)
  #   ) %>%
  #   ungroup()

  # trades_to_tag_with_returns_long <-
  #   indicator_data_tagged %>%
  #   ungroup() %>%
  #   distinct(Asset, Date, trade_col) %>%
  #   filter(Date >= start_date) %>%
  #   filter(trade_col == "Long")

  trades_to_tag_with_returns_long <-
    Indices_Metals_Bonds[[1]] %>%
    ungroup() %>%
    distinct(Asset, Date) %>%
    mutate(trade_col = "Long") %>%
    filter(trade_col == "Long")

  port_return_list[[i]] <-
    get_portfolio_model(
    asset_data = Indices_Metals_Bonds,
    asset_of_interest = distinct_assets[i],
    tagged_trades = trades_to_tag_with_returns_long,
    stop_factor_long = 6,
    profit_factor_long = 20,
    risk_dollar_value_long = 5,
    end_period = 20,
    time_frame = "H1"
  )

}

port_return_dfr <-
  port_return_list %>%
  map_dfr( ~
             .x %>%
             dplyr::select(adjusted_Date, Asset, Return, Date, close_Date, period_since_open)
           ) %>%
  ungroup() %>%
  group_by(adjusted_Date, Asset, Date,close_Date ) %>%
  summarise(Return = sum(Return),
            trades_open = n_distinct(Date)) %>%
  group_by(adjusted_Date) %>%
  mutate(
    Total_Return = sum(Return)
  )

newest_results <-
  get_current_new_algo_trades(realised_DB_path ="D:/trade_data/trade_tracker_realised.db")

trade_tracker_DB <- connect_db("D:/trade_data/trade_tracker_daily_buy_close.db")
all_trades_so_far <-
  DBI::dbGetQuery(conn = trade_tracker_DB,
                  "SELECT * FROM trade_tracker")
DBI::dbDisconnect(trade_tracker_DB)
gc()

trade_tracker_DB <- connect_db("D:/trade_data/trade_tracker_daily_buy_close 2.db")
all_trades_so_far2 <-
  DBI::dbGetQuery(conn = trade_tracker_DB,
                  "SELECT * FROM trade_tracker")
DBI::dbDisconnect(trade_tracker_DB)
gc()

all_trades_so_far_comb <-
  all_trades_so_far %>%
  bind_rows(all_trades_so_far2) %>%
  distinct() %>%
  filter()

distinct_assets <-
  all_trades_so_far_comb %>%
  distinct(Asset, account_var, trade_col, tradeID, periods_ahead) %>%
  rename(id = tradeID) %>%
  mutate(inLocalDB = TRUE)

newest_results_sum <-
  newest_results %>%
  group_by(id, Asset, account_var, initialUnits) %>%
  mutate(kk = row_number()) %>%
  slice_min(kk) %>%
  ungroup() %>%
  left_join(distinct_assets) %>%
  filter(inLocalDB == TRUE, !is.na(inLocalDB)) %>%
  dplyr::select(-inLocalDB) %>%
  dplyr::select(-kk) %>%
  dplyr::select(Asset, Date = date_open , periods_ahead, trade_col) %>%
  mutate(
    Date = floor_date(Date, unit = "hour")
  ) %>%
  filter(trade_col == "Long")


results_sum <-
  port_return_dfr %>%
  ungroup() %>%
  left_join(newest_results_sum) %>%
  filter(!is.na(periods_ahead))
  summarise(
    min_portfolio = min(Total_Return, na.rm = T),
    Portfolio_05 = quantile(Total_Return, 0.05, na.rm = T),
    Portfolio_10 = quantile(Total_Return, 0.1, na.rm = T),
    Portfolio_25 = quantile(Total_Return, 0.25, na.rm = T),
    Portfolio_50 = quantile(Total_Return, 0.50, na.rm = T),
    Portfolio_mean = mean(Total_Return, na.rm = T),
    Portfolio_75 = quantile(Total_Return, 0.75, na.rm = T)
  )

port_return_dfr  <-
  port_return_list %>%
  map_dfr( ~
             .x %>%
             dplyr::select(adjusted_Date, Asset, Return, Date, close_Date, period_since_open)
  ) %>%
  ungroup()

tag_portfolio_data_for_models <-
  function(port_return_dfr,
           trade_thresh = 3) {

    tagged_trade_data  <-
      port_return_dfr %>%
      ungroup() %>%
      filter(close_Date == period_since_open) %>%
      mutate(
        bin_var =
          case_when(
            Return >= trade_thresh ~ "win",
            Return < trade_thresh ~ "loss"
          )
      )

    return(tagged_trade_data)

  }

tagged_trade_data <-
  tag_portfolio_data_for_models(port_return_dfr)

create_macro_models <- function(raw_macro_data = raw_macro_data,
                                asset_data = Indices_Metals_Bonds[[1]],
                                tagged_trade_data = tagged_trade_data,
                                pre_train_date_end = "2025-03-01") {

  countries_for_int_strength <-
    c("GBP", "USD", "EUR", "AUD", "JPY", "CAD", "CNY", "NZD")

  interest_rates <-
    get_interest_rates(
      raw_macro_data = raw_macro_data,
      lag_days = 1
    )

  cpi_data <-
    get_cpi(
      raw_macro_data = raw_macro_data,
      lag_days = 1
    )

  sentiment_index <-
    create_sentiment_index(
      raw_macro_data,
      lag_days = 1,
      date_start = "2011-01-01",
      end_date = today() %>% as.character(),
      first_difference = TRUE,
      scale_values = FALSE
    )

  interest_rates_diffs <-
    interest_rates %>%
    dplyr::select(Date_for_Join= Date, contains("_Diff"))

  cpi_data_diffs <-
    cpi_data %>%
    dplyr::select(Date_for_Join = Date, contains("_Diff"))

  interest_rate_strength_Index <-
    get_Interest_Rate_strength(
      interest_rates =interest_rates_diffs %>% mutate(Date = Date_for_Join),
      countries = countries_for_int_strength
    ) %>%
    mutate(Date_for_Join = Date)

  CPI_strength_index <-
    get_CPI_Rate_strength(
      cpi_data =cpi_data_diffs %>% mutate(Date = Date_for_Join),
      countries = countries_for_int_strength
    ) %>%
    mutate(Date_for_Join = Date)

  macro_for_join <-
    asset_data %>%
    distinct(Date) %>%
    mutate(Date_for_Join = as_date(Date)) %>%
    arrange(Date) %>%
    left_join(CPI_strength_index) %>%
    left_join(interest_rate_strength_Index) %>%
    left_join(sentiment_index %>% mutate(Date_for_Join = Date)) %>%
    dplyr::select(-Date_for_Join) %>%
    fill(everything(), .direction = "down") %>%
    filter(if_all(everything(), ~ !is.na(.)))

  macro_for_join_model <-
    tagged_trade_data %>%
    dplyr::select(Date, Asset ,bin_var, Return) %>%
    left_join(macro_for_join) %>%
    filter(if_all(everything(),~!is.na(.))) %>%
    mutate(AUD_var =
             ifelse(str_detect(Asset, "AUD"), 1, 0),
           USD_var =
             ifelse(str_detect(Asset, "USD"), 1, 0),
           CAD_var =
             ifelse(str_detect(Asset, "CAD"), 1, 0),
           JPY_var =
             ifelse(str_detect(Asset, "JPY"), 1, 0),
           EUR_var =
             ifelse(str_detect(Asset, "EUR"), 1, 0),
           GBP_var =
             ifelse(str_detect(Asset, "GBP"), 1, 0),
           SEK_var =
             ifelse(str_detect(Asset, "SEK"), 1, 0),
           NZD_var =
             ifelse(str_detect(Asset, "NZD"), 1, 0),
           HKD_var =
             ifelse(str_detect(Asset, "HKD"), 1, 0),
           XAU_var =
             ifelse(str_detect(Asset, "XAU"), 1, 0),
           XAG_var =
             ifelse(str_detect(Asset, "XAG"), 1, 0),
           BTC_var =
             ifelse(str_detect(Asset, "BTC"), 1, 0),
           CHF_var =
             ifelse(str_detect(Asset, "CHF"), 1, 0),
           equity_var =
             ifelse(str_detect(Asset, "SPX|US2000|NL25|HK33|JP225|EU50|UK100|FR40|AU200|DE30|SG30|CH20"), 1, 0),
           commod_var =
             ifelse(str_detect(Asset, "WTICO|BCO|SOY|WHEAT|SUGAR"), 1, 0),
           bond_var =
             ifelse(str_detect(Asset, "USB|UK10YB"), 1, 0)
           )

  macro_vars_for_indicator <-
    names(macro_for_join_model) %>%
    keep(~ !str_detect(.x, "Date") &
           !str_detect(.x, "bin_var") &
           !str_detect(.x, "Asset") &
           !str_detect(.x, "Return")
         ) %>%
    unlist() %>%
    as.character()

  macro_indicator_formula <-
    create_lm_formula(dependant = "bin_var=='win'",
                      independant = macro_vars_for_indicator)

  macro_indicator_model <-
    glm(formula = macro_indicator_formula,
        data = macro_for_join_model %>% filter(Date <=pre_train_date_end),
        family = binomial("logit"))

  summary(macro_indicator_model)
  message("Passed Macro Model")

  macro_indicator_pred <-
    macro_for_join_model %>%
    mutate(
      macro_indicator_pred = predict.glm(macro_indicator_model,
                                         newdata = macro_for_join_model, type = "response"),
      mean_macro_pred =
        mean( ifelse(Date <= pre_train_date_end, macro_indicator_pred, NA), na.rm = T ),
      sd_macro_pred =
        sd( ifelse(Date <= pre_train_date_end, macro_indicator_pred, NA), na.rm = T )
    )
  message("Passed Macro Pred")

}
