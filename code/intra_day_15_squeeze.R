helpeR::load_custom_functions()
library(neuralnet)
raw_macro_data <- get_macro_event_data()

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

asset_list_oanda <- get_oanda_symbols() %>%
  keep( ~ .x %in% c("HK33_HKD", "USD_JPY","SPX500_USD",
                    "AUD_NZD", "GBP_CHF",
                    "EUR_HUF", "EUR_ZAR", "NZD_JPY", "EUR_NZD",
                    "XAU_CAD", "GBP_JPY", "EUR_NOK", "USD_SGD", "EUR_SEK",
                    "DE30_EUR",
                    "AUD_CAD",
                    "XPD_USD",
                    "UK100_GBP",
                    "USD_CHF", "GBP_NZD",
                    "GBP_SGD", "USD_SEK", "EUR_SGD", "XCU_USD", "SUGAR_USD", "CHF_ZAR",
                    "AUD_CHF", "EUR_CHF", "USD_MXN", "GBP_USD", "WTICO_USD", "EUR_JPY", "USD_NOK",
                    "XAU_USD",
                    "USD_CZK", "AUD_SGD", "USD_HUF", "WHEAT_USD",
                    "EUR_USD", "SG30_SGD", "GBP_AUD", "NZD_CAD", "AU200_AUD", "XAG_USD",
                    "XAU_EUR", "EUR_GBP", "USD_CNH", "USD_CAD", "NAS100_USD",
                    "EU50_EUR", "NATGAS_USD", "CAD_JPY", "FR40_EUR", "USD_ZAR", "XAU_GBP",
                    "EUR_AUD", "SOYBN_USD",
                    "US2000_USD",
                    "BCO_USD", "AUD_USD", "NZD_USD", "NZD_CHF", "WHEAT_USD", "AUD_JPY", "AUD_SEK")
  )

asset_infor <- get_instrument_info()

db_location <- "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data.db"
start_date_day = "2023-03-01"
start_date_day_H1 = "2022-09-01"
end_date_day = today() %>% as.character()

starting_asset_data_ask_15M <-
  get_db_price(
    db_location = db_location,
    start_date = start_date_day,
    end_date = end_date_day,
    bid_or_ask = "ask",
    time_frame = "M15"
  )

starting_asset_data_ask_H1 <-
  get_db_price(
    db_location = db_location,
    start_date = start_date_day_H1,
    end_date = end_date_day,
    bid_or_ask = "ask",
    time_frame = "H1"
  )

mean_values_by_asset_for_loop_15_ask =
  wrangle_asset_data(
    asset_data_daily_raw = starting_asset_data_ask_15M,
    summarise_means = TRUE
  )


starting_asset_data_ask_H1 = starting_asset_data_ask_H1
starting_asset_data_ask_15M = starting_asset_data_ask_15M
XX = 100
rolling_slide = 200
pois_period = 10
stop_factor = 16

get_res_sup_slow_fast_fractal_data <-
  function(
    starting_asset_data_ask_H1 = starting_asset_data_ask_H1,
    starting_asset_data_ask_15M = starting_asset_data_ask_15M,
    XX = 100,
    rolling_slide = 200,
    pois_period = 10
    ) {

    starting_asset_data_ask_H1_Tag <-
      starting_asset_data_ask_H1 %>%
      group_by(Asset) %>%
      mutate(
        High_Max_XX_H = slider::slide_dbl(High,
                                          .f = ~ max(.x, na.rm = T),
                                          .before = XX),
        Low_Max_XX_H = slider::slide_dbl(Low,
                                         .f =  ~ min(.x, na.rm = T),
                                         .before = XX),

        High_Max_XX_slow_H = slider::slide_dbl(High,
                                               .f = ~ max(.x, na.rm = T),
                                               .before = XX*2),
        Low_Max_XX_slow_H = slider::slide_dbl(Low,
                                              .f =  ~ min(.x, na.rm = T),
                                              .before = XX*2),

        High_Max_XX_very_slow_H = slider::slide_dbl(High,
                                                    .f = ~ max(.x, na.rm = T),
                                                    .before = XX*4),
        Low_Max_XX_very_slow_H = slider::slide_dbl(Low,
                                                   .f =  ~ min(.x, na.rm = T),
                                                   .before = XX*4)
      ) %>%
      ungroup()

    squeeze_detection <-
      starting_asset_data_ask_15M %>%
      group_by(Asset) %>%
      mutate(
        longs = ifelse(Price - Open >0 , 1, 0)
      ) %>%
      mutate(

        High_Max_XX = slider::slide_dbl(High,
                                        .f = ~ max(.x, na.rm = T),
                                        .before = XX),
        Low_Max_XX = slider::slide_dbl(Low,
                                       .f =  ~ min(.x, na.rm = T),
                                       .before = XX),

        High_Max_XX_slow = slider::slide_dbl(High,
                                             .f = ~ max(.x, na.rm = T),
                                             .before = XX*2),
        Low_Max_XX_slow = slider::slide_dbl(Low,
                                            .f =  ~ min(.x, na.rm = T),
                                            .before = XX*2),

        High_Max_XX_very_slow = slider::slide_dbl(High,
                                                  .f = ~ max(.x, na.rm = T),
                                                  .before = XX*4),
        Low_Max_XX_very_slow = slider::slide_dbl(Low,
                                                 .f =  ~ min(.x, na.rm = T),
                                                 .before = XX*4)

      ) %>%
      ungroup() %>%
      left_join(
        starting_asset_data_ask_H1_Tag %>%
          dplyr::select(Date, Asset, High_Max_XX_H, Low_Max_XX_H,
                        High_Max_XX_slow_H, Low_Max_XX_slow_H,High_Max_XX_very_slow_H, Low_Max_XX_very_slow_H )
      ) %>%
      dplyr::group_by(Asset) %>%
      arrange(Date, .by_group = TRUE) %>%
      dplyr::group_by(Asset) %>%
      fill(
        c(High_Max_XX_H, Low_Max_XX_H,
          High_Max_XX_slow_H, Low_Max_XX_slow_H,High_Max_XX_very_slow_H, Low_Max_XX_very_slow_H),
        .direction = "down"
      ) %>%
      ungroup()

    squeeze_detection <- squeeze_detection %>%
      mutate(
        Res_Diff_H1_XX = High_Max_XX_H - High,
        Res_Diff_H1_XX_slow = High_Max_XX_slow_H - High,
        Res_Diff_H1_XX_very_slow = High_Max_XX_very_slow_H - High,

        Sup_Diff_H1_XX = Low - Low_Max_XX_H,
        Sup_Diff_H1_XX_slow = Low - Low_Max_XX_slow_H,
        Sup_Diff_H1_XX_very_slow = Low - Low_Max_XX_very_slow_H
      ) %>%
      group_by(Asset) %>%
      mutate(
        Res_Diff_H1_XX_run_mean = slider::slide_dbl(Res_Diff_H1_XX, .f = ~ mean(.x, na.rm = T),
                                                    .before = rolling_slide, .complete = FALSE),
        Res_Diff_H1_XX_slow_run_mean =
          slider::slide_dbl(Res_Diff_H1_XX_slow, .f = ~ mean(.x, na.rm = T), .before = rolling_slide, .complete = FALSE),
        Res_Diff_H1_XX_very_slow_run_mean =
          slider::slide_dbl(Res_Diff_H1_XX_very_slow, .f = ~ mean(.x, na.rm = T), .before = rolling_slide, .complete = FALSE),

        Res_Diff_H1_XX_run_sd = slider::slide_dbl(Res_Diff_H1_XX, .f = ~ sd(.x, na.rm = T), .before = rolling_slide, .complete = FALSE),
        Res_Diff_H1_XX_slow_run_sd =
          slider::slide_dbl(Res_Diff_H1_XX_slow, .f = ~ sd(.x, na.rm = T), .before = rolling_slide, .complete = FALSE),
        Res_Diff_H1_XX_very_slow_run_sd =
          slider::slide_dbl(Res_Diff_H1_XX_very_slow, .f = ~ sd(.x, na.rm = T), .before = rolling_slide, .complete = FALSE),


        Sup_Diff_H1_XX_run_mean = slider::slide_dbl(Sup_Diff_H1_XX, .f = ~ mean(.x, na.rm = T), .before = rolling_slide, .complete = FALSE),
        Sup_Diff_H1_XX_slow_run_mean =
          slider::slide_dbl(Sup_Diff_H1_XX_slow, .f = ~ mean(.x, na.rm = T), .before = rolling_slide, .complete = FALSE),
        Sup_Diff_H1_XX_very_slow_run_mean =
          slider::slide_dbl(Sup_Diff_H1_XX_very_slow, .f = ~ mean(.x, na.rm = T), .before = rolling_slide, .complete = FALSE),

        Sup_Diff_H1_XX_run_sd = slider::slide_dbl(Sup_Diff_H1_XX, .f = ~ sd(.x, na.rm = T), .before = rolling_slide, .complete = FALSE),
        Sup_Diff_H1_XX_slow_run_sd =
          slider::slide_dbl(Sup_Diff_H1_XX_slow, .f = ~ sd(.x, na.rm = T), .before = rolling_slide, .complete = FALSE),
        Sup_Diff_H1_XX_very_slow_run_sd =
          slider::slide_dbl(Sup_Diff_H1_XX_very_slow, .f = ~ sd(.x, na.rm = T), .before = rolling_slide, .complete = FALSE)
      ) %>%
      ungroup()

    return(squeeze_detection)

  }

profit_factor =16
risk_dollar_value = 10
sd_fac_1 = 3.5
sd_fac_2 = 3.5
sd_fac_3 = 3.5




tagged_trades <-
  squeeze_detection %>%
  filter(!is.na(Sup_Diff_H1_XX_slow), !is.na(Res_Diff_H1_XX_run_mean)) %>%
  group_by(Asset) %>%
  mutate(
    trade_col =
      case_when(
        Sup_Diff_H1_XX <= Sup_Diff_H1_XX_run_mean - sd_fac_1*Sup_Diff_H1_XX_run_sd ~"Long",
        Sup_Diff_H1_XX_slow <= Sup_Diff_H1_XX_slow_run_mean - sd_fac_2*Sup_Diff_H1_XX_slow_run_sd ~"Long",
        Sup_Diff_H1_XX_very_slow <= Sup_Diff_H1_XX_very_slow_run_mean - sd_fac_3*Sup_Diff_H1_XX_very_slow_run_sd ~"Long"
      )
  ) %>%
  filter(trade_col == "Long")

long_bayes_loop_analysis_neg <-
  generic_trade_finder_loop(
    tagged_trades = tagged_trades ,
    asset_data_daily_raw = starting_asset_data_ask_15M,
    stop_factor = stop_factor,
    profit_factor =profit_factor,
    trade_col = "trade_col",
    date_col = "Date",
    start_price_col = "Price",
    mean_values_by_asset = mean_values_by_asset_for_loop_15_ask
  )

trade_timings_neg <-
  long_bayes_loop_analysis_neg %>%
  mutate(
    ending_date_trade = as_datetime(ending_date_trade),
    dates = as_datetime(dates)
  ) %>%
  mutate(Time_Required = (ending_date_trade - dates)/dhours(1) )

trade_timings_by_asset_neg <- trade_timings_neg %>%
  mutate(win_loss = ifelse(trade_returns < 0, "loss", "wins") ) %>%
  group_by(win_loss) %>%
  summarise(
    Time_Required = median(Time_Required, na.rm = T)
  ) %>%
  pivot_wider(names_from = win_loss, values_from = Time_Required) %>%
  rename(loss_time_hours = loss,
         win_time_hours = wins)

analysis_data_neg <-
  generic_anlyser(
    trade_data = long_bayes_loop_analysis_neg %>% rename(Asset = asset),
    profit_factor = profit_factor,
    stop_factor = stop_factor,
    asset_infor = asset_infor,
    currency_conversion = currency_conversion,
    asset_col = "Asset",
    stop_col = "starting_stop_value",
    profit_col = "starting_profit_value",
    price_col = "trade_start_prices",
    trade_return_col = "trade_returns",
    risk_dollar_value = risk_dollar_value,
    grouping_vars = "trade_col"
  ) %>%
  mutate(
    sd_fac = sd_fac
    # Network_Name = Network_Name,
    # direction_sd = "negative"
  ) %>%
  bind_cols(trade_timings_by_asset_neg)

analysis_data_asset_neg <-
  generic_anlyser(
    trade_data = long_bayes_loop_analysis_neg %>% rename(Asset = asset),
    profit_factor = profit_factor,
    stop_factor = stop_factor,
    asset_infor = asset_infor,
    currency_conversion = currency_conversion,
    asset_col = "Asset",
    stop_col = "starting_stop_value",
    profit_col = "starting_profit_value",
    price_col = "trade_start_prices",
    trade_return_col = "trade_returns",
    risk_dollar_value = risk_dollar_value,
    grouping_vars = "Asset"
  ) %>%
  mutate(
    sd_fac = sd_fac
    # Network_Name = Network_Name,
    # direction_sd = "negative"
  ) %>%
  bind_cols(trade_timings_by_asset_neg)

