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
raw_macro_data <- get_macro_event_data()
#---------------------Data
load_custom_functions()
db_location = "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data_Most_Assets_2025-09-13.db"
start_date = "2021-01-01"
end_date = today() %>% as.character()

bin_factor = NULL
stop_value_var = 2
profit_value_var = 4
period_var = 24
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

DBI::dbDisconnect(full_ts_trade_db_con)
rm(full_ts_trade_db_con)
gc()

All_Daily_Data <-
  get_DAILY_ALGO_DATA_API_REQUEST()

Indices_Metals_Bonds <- get_Port_Buy_Data(
  db_location = db_location,
  start_date = start_date,
  end_date = today() %>% as.character(),
  time_frame = "H1"
)

missing_assets <- get_Port_Buy_Data_remaining_assets(
  db_location = db_location,
  start_date = start_date,
  end_date = today() %>% as.character(),
  time_frame = "H1"
)

Indices_Metals_Bonds[[1]] <-
  Indices_Metals_Bonds[[1]] %>%
  bind_rows(missing_assets[[1]] %>%
              filter(Asset != "XAG_AUD"))

Indices_Metals_Bonds[[2]] <-
  Indices_Metals_Bonds[[2]] %>%
  bind_rows(missing_assets[[2]] %>%
              filter(Asset != "XAG_AUD"))

rm(missing_assets)
gc()
asset_of_interest = "HK33_HKD"

create_daily_indicator_model <-
  function(
    asset_data = Indices_Metals_Bonds[[1]] %>% filter(Asset == asset_of_interest),
    actual_wins_losses = actual_wins_losses,
    All_Daily_Data = All_Daily_Data,
    Asset_of_interest = "HK33_HKD",
    pre_train_date_end = "2023-01-01",
    trade_direction = "Long",
    stop_value_var = stop_value_var,
    profit_value_var = profit_value_var,
    period_var = period_var,
    model_save_location = "C:/Users/Nikhil Chandra/Documents/trade_data/asset_specific_Daily_Indicator_Model"
    ) {

    actual_bins <-
      actual_wins_losses %>%
      filter(trade_col == trade_direction) %>%
      filter(
        stop_factor == stop_value_var,
        profit_factor == profit_value_var,
        periods_ahead == period_var,
        Asset == Asset_of_interest
      ) %>%
      mutate(
        bin_var =
          case_when(
            trade_start_prices > trade_end_prices & trade_col == "Short" ~ "win",
            trade_start_prices <= trade_end_prices & trade_col == "Short" ~ "loss",

            trade_start_prices < trade_end_prices & trade_col == "Long" ~ "win",
            trade_start_prices >= trade_end_prices & trade_col == "Long" ~ "loss"

          )
      )

    daily_indicator <-
      get_daily_indicators(
        Daily_Data = All_Daily_Data,
        asset_data = asset_data,
        Asset_of_interest = Asset_of_interest
      )

    daily_indicator <-
      daily_indicator %>%
      dplyr::select(-Price, -Open, -Low, -High, -Vol., -Date_for_join) %>%
      mutate(
        across(.cols = !contains("Date") & !contains("Asset"),
               .fns = ~ lag(.))
      ) %>%
      filter(if_all(everything(), ~ !is.na(.))) %>%
      mutate(
        Date = as_datetime(Date, tz = "Australia/Canberra")
      )

    daily_join_model <-
      actual_bins %>%
      dplyr::select(Date, Asset ,bin_var) %>%
      filter(
        Asset == Asset_of_interest
      ) %>%
      left_join(daily_indicator) %>%
      filter(if_all(everything(),~!is.na(.)))

    daily_vars_for_indicator <-
      names(daily_join_model) %>%
      keep(~ !str_detect(.x, "Date") &
             !str_detect(.x, "bin_var") &
             !str_detect(.x, "Asset")) %>%
      unlist() %>%
      as.character()

    daily_indicator_formula <-
      create_lm_formula(dependant = "bin_var=='win'",
                        independant = daily_vars_for_indicator)
    daily_indicator_model <-
      glm(formula = daily_indicator_formula,
          data = daily_join_model %>% filter(Date <=pre_train_date_end),
          family = binomial("logit"))
    summary(daily_indicator_model)
    message("Passed Daily Model")

    daily_indicator_pred <-
      daily_join_model %>%
      mutate(
        daily_indicator_pred = predict.glm(daily_indicator_model,
                                           newdata = daily_join_model, type = "response"),
        mean_daily_pred =
          mean( ifelse(Date <= pre_train_date_end, daily_indicator_pred, NA), na.rm = T ),
        sd_daily_pred =
          sd( ifelse(Date <= pre_train_date_end, daily_indicator_pred, NA), na.rm = T )
      )
    message("Passed Daily Prediction")

    saveRDS(object = daily_indicator_model,
            file = glue::glue("{model_save_location}/Daily_Indicator_{Asset_of_interest}_{period_var}_{stop_value_var}_{profit_value_var}.RDS"))

    return(daily_indicator_pred)

  }


daily_indicator_data <-
  create_daily_indicator_model(
    asset_data = Indices_Metals_Bonds[[1]] %>% filter(Asset == asset_of_interest),
    actual_wins_losses = actual_wins_losses,
    All_Daily_Data = All_Daily_Data,
    Asset_of_interest = asset_of_interest,
    pre_train_date_end = "2023-01-01",
    trade_direction = "Long",
    stop_value_var = stop_value_var,
    profit_value_var = profit_value_var,
    period_var = period_var,
    model_save_location = "C:/Users/Nikhil Chandra/Documents/trade_data/asset_specific_Daily_Indicator_Model"
  )

daily_trader <- function(
  asset_data_ask = Indices_Metals_Bonds[[1]] %>% filter(Asset == asset_of_interest),
  asset_data_bid = Indices_Metals_Bonds[[2]] %>% filter(Asset == asset_of_interest),
  asset_of_interest = asset_of_interest,
  train_date_for_indicator = "2023-01-01",
  daily_indicator_data = daily_indicator_data,
  currency_conversion = currency_conversion,
  asset_infor = asset_infor,
  sd_factor = 0.5
  ) {

  ask_data_with_indicator <-
    asset_data_ask %>%
    left_join(
      daily_indicator_data %>%
        distinct()
    ) %>%
    mutate(

      daily_indicator_pred_ma_15 =
        slider::slide_dbl(.x = daily_indicator_pred, .f =~mean(.x, na.rm = T), .before = 15 ),
      daily_indicator_pred_ma_10 =
        slider::slide_dbl(.x = daily_indicator_pred, .f =~mean(.x, na.rm = T), .before = 10 ),
      daily_indicator_pred_ma_5 =
        slider::slide_dbl(.x = daily_indicator_pred, .f =~mean(.x, na.rm = T), .before = 5 ),

      daily_indicator_pred_sd_15 =
        slider::slide_dbl(.x = daily_indicator_pred, .f =~sd(.x, na.rm = T), .before = 15 ),
      daily_indicator_pred_sd_10 =
        slider::slide_dbl(.x = daily_indicator_pred, .f =~sd(.x, na.rm = T), .before = 10 ),
      daily_indicator_pred_sd_5 =
        slider::slide_dbl(.x = daily_indicator_pred, .f =~sd(.x, na.rm = T), .before = 5 )
    ) %>%
    filter(Date >= train_date_for_indicator)

  bid_data_with_indicator <-
    asset_data_bid %>%
    left_join(
      daily_indicator_data %>%
        distinct()
    ) %>%
    mutate(
      daily_indicator_pred_ma_15 =
        slider::slide_dbl(.x = daily_indicator_pred, .f =~mean(.x, na.rm = T), .before = 15 ),
      daily_indicator_pred_ma_10 =
        slider::slide_dbl(.x = daily_indicator_pred, .f =~mean(.x, na.rm = T), .before = 10 ),
      daily_indicator_pred_ma_5 =
        slider::slide_dbl(.x = daily_indicator_pred, .f =~mean(.x, na.rm = T), .before = 5 ),

      daily_indicator_pred_sd_15 =
        slider::slide_dbl(.x = daily_indicator_pred, .f =~sd(.x, na.rm = T), .before = 15 ),
      daily_indicator_pred_sd_10 =
        slider::slide_dbl(.x = daily_indicator_pred, .f =~sd(.x, na.rm = T), .before = 10 ),
      daily_indicator_pred_sd_5 =
        slider::slide_dbl(.x = daily_indicator_pred, .f =~sd(.x, na.rm = T), .before = 5 )
    ) %>%
    filter(Date >= train_date_for_indicator)

  overall_sd <- ask_data_with_indicator$sd_daily_pred[1]
  overall_mean <- ask_data_with_indicator$mean_daily_pred[1]
  threshold_point <- overall_mean + sd_factor*overall_sd

  trade_tracker <-
    tibble(
      start_price = rep(0, 20000),
      current_price = rep(0, 20000),
      Asset = rep("a", 20000),
      running_PL = rep(0, 20000),
      starting_pred = rep(0, 20000),
      starting_pred_15 = rep(0, 20000),

      current_pred = rep(0, 20000),
      current_pred_15 = rep(0, 20000),

      trade_id = rep(0, 20000)
    )
  c = 0

  for (i in 1:dim(ask_data_with_indicator)[1] ) {

    current_pred<- ask_data_with_indicator$daily_indicator_pred[i]
    current_pred_15<- ask_data_with_indicator$daily_indicator_pred_ma_15[i]
    current_pred_10<- ask_data_with_indicator$daily_indicator_pred_ma_10[i]

    current_pred_15_sd<- ask_data_with_indicator$daily_indicator_pred_sd_15[i]
    current_pred_10_sd <- ask_data_with_indicator$daily_indicator_pred_sd_10[i]

    current_price <- ask_data_with_indicator$Price[i]
    current_High <- ask_data_with_indicator$High[i]
    current_Low <- ask_data_with_indicator$Low[i]
    current_asset <- ask_data_with_indicator$Asset[i]

    if(threshold_point <= current_pred) {
      c = c + 1
      trade_tracker$start_price[c] <- current_price
      trade_tracker$current_price[c] <- current_price
      trade_tracker$Asset[c] <- current_asset

      trade_tracker$starting_pred[c] <- current_pred
      trade_tracker$trade_id[c] <- c

    }

    active_trades <-
      trade_tracker %>%
      filter(Asset != "a")

  }

}
