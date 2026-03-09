helpeR::load_custom_functions()

all_aud_symbols <- get_oanda_symbols() %>%
  keep(~ str_detect(.x, "AUD")|str_detect(.x, "USD_SEK|USD_NOK|USD_HUF|USD_ZAR|USD_CNY|USD_MXN|USD_CZK"))
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
    "USB02Y_USD", "USB05Y_USD", "USB30Y_USD", "USB10Y_USD", "UK100_GBP",
    "EUR_CHF", #1 EUR_CHF
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
    "XAG_SGD" , #25 XAG_SGD,
    "BCH_USD" , #26 BCH_USD
    "LTC_USD" #27 LTC_USD
  ) %>%
  unique()

asset_infor <- get_instrument_info()
#---------------------Data
load_custom_functions()
db_location = "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data_Most_Assets_2025-09-13 2.db"
start_date = "2017-01-01"
end_date = today() %>% as.character()

# bin_factor = NULL
# stop_value_var = 2
# profit_value_var = 15
# period_var = 48

All_Daily_Data <-
  get_DAILY_ALGO_DATA_API_REQUEST()

Indices_Metals_Bonds <- list()

Indices_Metals_Bonds[[1]] <-
  get_db_data_quickly_algo(
    db_location = db_location,
    start_date = start_date,
    end_date = today() %>% as.character(),
    time_frame = "H1",
    bid_or_ask = "ask",
    # assets =   c("SPX500_USD","US2000_USD","EU50_EUR","SG30_SGD" ,
    #              "AU200_AUD" ,"XAG_USD","XAU_USD","USD_JPY" ,
    #              "AUD_USD" ,"UK100_GBP" ,"JP225Y_JPY","FR40_EUR" ,
    #              "CH20_CHF","USB10Y_USD","USB02Y_USD" ,"UK10YB_GBP" ,
    #              "HK33_HKD" ,"EUR_USD" ,"GBP_USD" ,"XAG_EUR" ,
    #              "XAU_EUR" ,"XAU_GBP" ,"XAG_GBP" ,"EUR_GBP" ,
    #              "WTICO_USD" ,"BCO_USD" ,"XCU_USD" ,"XAU_JPY",
    #              "XAG_JPY" ,"XAU_AUD" ,"XAG_AUD" ,"USD_CAD" ,
    #              "EUR_AUD" ,"NZD_USD" ,"EUR_NZD" ,"AUD_NZD" ,
    #              "GBP_AUD" ,"GBP_NZD" ,"GBP_CAD" ,"GBP_JPY" ,
    #              "USD_SGD" ,"EUR_JPY" , "BTC_USD" ,"ETH_USD" ,"NATGAS_USD" ,
    #              "EUR_SEK" ,"USD_SEK" ,"LTC_USD" , "XAG_NZD") %>%
    #   unique()

    assets =
      c("EUR_USD", "AUD_USD", "EUR_GBP", "USD_JPY", "GBP_JPY", "EUR_NZD", "GBP_AUD", "XAG_USD",
        "EUR_JPY", "SPX500_USD", "HK33_HKD", "AU200_AUD", "GBP_CAD", "NZD_USD", "USD_CAD", "XAU_USD",
        "EU50_EUR", "UK100_GBP", "NATGAS_USD", "WTICO_USD") %>% unique()
  ) %>%
  distinct()

Indices_Metals_Bonds[[2]] <-
  get_db_data_quickly_algo(
    db_location = db_location,
    start_date = start_date,
    end_date = today() %>% as.character(),
    time_frame = "H1",
    bid_or_ask = "bid",
    # assets =   c("SPX500_USD","US2000_USD","EU50_EUR","SG30_SGD" ,
    #              "AU200_AUD" ,"XAG_USD","XAU_USD","USD_JPY" ,
    #              "AUD_USD" ,"UK100_GBP" ,"JP225Y_JPY","FR40_EUR" ,
    #              "CH20_CHF","USB10Y_USD","USB02Y_USD" ,"UK10YB_GBP" ,
    #              "HK33_HKD" ,"EUR_USD" ,"GBP_USD" ,"XAG_EUR" ,
    #              "XAU_EUR" ,"XAU_GBP" ,"XAG_GBP" ,"EUR_GBP" ,
    #              "WTICO_USD" ,"BCO_USD" ,"XCU_USD" ,"XAU_JPY",
    #              "XAG_JPY" ,"XAU_AUD" ,"XAG_AUD" ,"USD_CAD" ,
    #              "EUR_AUD" ,"NZD_USD" ,"EUR_NZD" ,"AUD_NZD" ,
    #              "GBP_AUD" ,"GBP_NZD" ,"GBP_CAD" ,"GBP_JPY" ,
    #              "USD_SGD" ,"EUR_JPY" , "BTC_USD" ,"ETH_USD" ,"NATGAS_USD" ,
    #              "EUR_SEK" ,"USD_SEK" ,"LTC_USD" , "XAG_NZD") %>%
    #   unique(),
    assets =
      c("EUR_USD", "AUD_USD", "EUR_GBP", "USD_JPY", "GBP_JPY", "EUR_NZD", "GBP_AUD", "XAG_USD",
        "EUR_JPY", "SPX500_USD", "HK33_HKD", "AU200_AUD", "GBP_CAD", "NZD_USD", "USD_CAD", "XAU_USD",
        "EU50_EUR", "UK100_GBP", "NATGAS_USD", "WTICO_USD") %>% unique()
  ) %>%
  distinct()

actual_wins_losses <-
  get_actual_wins_losses(
    assets =
      c("EUR_USD", "AUD_USD", "EUR_GBP", "USD_JPY", "GBP_JPY", "EUR_NZD", "GBP_AUD", "XAG_USD",
        "EUR_JPY", "SPX500_USD", "HK33_HKD", "AU200_AUD", "GBP_CAD", "NZD_USD", "USD_CAD", "XAU_USD",
        "EU50_EUR", "UK100_GBP", "NATGAS_USD", "WTICO_USD") %>% unique(),
    asset_data = Indices_Metals_Bonds,
    stop_factor = 10,
    # profit_factor = 30,
    profit_factor = 50,
    risk_dollar_value = 10,
    trade_direction = "Long",
    currency_conversion = currency_conversion,
    asset_infor = asset_infor,
    periods_ahead = 24
  )

Indices_Metals_Bonds = Indices_Metals_Bonds
actual_wins_losses = actual_wins_losses
actuals_periods_needed = c("period_return_24_Price", "period_return_35_Price", "period_return_46_Price")
AR_assets =
  c("EUR_USD", "AUD_USD", "EUR_GBP", "USD_JPY", "GBP_JPY", "EUR_NZD", "GBP_AUD", "XAG_USD",
    "EUR_JPY", "SPX500_USD", "HK33_HKD", "AU200_AUD", "GBP_CAD", "NZD_USD", "USD_CAD", "XAU_USD",
    "EU50_EUR", "UK100_GBP", "NATGAS_USD", "WTICO_USD") %>% unique()
sig_threshes = c(0.99, 0.5, 0.1, 0.00001, 0.0000000000001)
pred_col = "Averaged_AR_Pred_GLM"
period_return_col = "period_return_46_Price"
bin_threshold_vec = c(0, 3,  5, 7)
db_save_path = "C:/Users/Nikhil Chandra/Documents/trade_data/single_Asset_V3_AR_test_conditions.db"
training_end_date = "2022-06-01"

c = 0

safely_gen_preds <-
  safely(single_asset_v3_gen_AR_Model, otherwise = NULL)

for (i in 1:length(AR_assets)) {
  for (j in 1:length(sig_threshes)) {
    for (k in 1:length(bin_threshold_vec)) {

      internal_asset <- list()

      internal_asset[[1]] <-
        Indices_Metals_Bonds[[1]] %>% filter(Asset == AR_assets[i])
      internal_asset[[2]] <-
        Indices_Metals_Bonds[[2]] %>% filter(Asset == AR_assets[i])

      actual_wins_losses_internal <-
        actual_wins_losses %>%
        filter(Asset == AR_assets[i])

      AR_preds_list <-
        safely_gen_preds(
          Indices_Metals_Bonds = internal_asset,
          actual_wins_losses = actual_wins_losses_internal,
          asset_of_interest = AR_assets[i],
          actuals_periods_needed = actuals_periods_needed,
          training_end_date = training_end_date,
          bin_threshold = bin_threshold_vec[k],
          rolling_mean_pred_period = 500,
          base_path = "C:/Users/Nikhil Chandra/Documents/trade_data/single_asset_models_v1/",
          sig_thresh = sig_threshes[j]
        ) %>%
        pluck('result')

      gc()

      message( as.character(dim(AR_preds_list)[1]) )

      if(!is.null(AR_preds_list)) {

        c = c + 1
        message( as.character(c) )

        generated_preds_from_db <-
          AR_preds_list %>%
          pluck("testing_data") %>%
          mutate(
            training_end_date = training_end_date
          ) %>%
          mutate(
            Averaged_AR_Pred =
              (AR_LM_Pred_period_return_46_Price + AR_LM_Pred_period_return_35_Price + AR_LM_Pred_period_return_24_Price)/3,
            Averaged_AR_Pred_GLM =
              (AR_GLM_Pred_period_return_46_Price + AR_GLM_Pred_period_return_35_Price + AR_GLM_Pred_period_return_24_Price)/3
          )

        rm(AR_preds_list)

        gc()

        AR_LM_Pred_analysis <-
          construct_Performance_to_Thresh_Curve(
            pred_data = generated_preds_from_db,
            pred_col = pred_col,
            actual_wins_losses = actual_wins_losses_internal,
            # thresh_vector = seq(-5,10, 0.25),
            thresh_vector = seq(0, 0.85, 0.025),
            period_return_col = period_return_col,
            sim_start_date = training_end_date
          ) %>%
          mutate(
            Asset = AR_assets[i],
            period_return_col = period_return_col,
            pred_col = pred_col,
            bin_threshold = bin_threshold_vec[k],
            sig_thresh = sig_threshes[j],
            trade_end_date = training_end_date
          )

        message( as.character(dim(AR_LM_Pred_analysis)[1]) )

        rm(generated_preds_from_db, AR_preds_list)
        gc()

        if(c == 1) {
          db_con <- connect_db(db_save_path)
          write_table_sql_lite(.data = AR_LM_Pred_analysis,
                                table_name = "single_Asset_V3_AR_test_conditions",
                                conn = db_con,
                                overwrite_true = TRUE
          )
          rm(AR_LM_Pred_analysis)
          gc()
          DBI::dbDisconnect(db_con)
        } else {
          db_con <- connect_db(db_save_path)
          append_table_sql_lite(.data = AR_LM_Pred_analysis,
                                table_name = "single_Asset_V3_AR_test_conditions",
                                conn = db_con)
          rm(AR_LM_Pred_analysis)
          gc()
          DBI::dbDisconnect(db_con)
        }

      }

      rm(actual_wins_losses_internal, internal_asset)
      gc()

    }

  }

}

db_con <- connect_db("C:/Users/Nikhil Chandra/Documents/trade_data/single_Asset_V3_AR_test_conditions.db")
AR_LM_Pred_analysis <- DBI::dbGetQuery(db_con, "SELECT * FROM single_Asset_V3_AR_test_conditions")
assets_done <- AR_LM_Pred_analysis %>% pull(Asset) %>% unique()

AR_LM_Pred_analysis %>%
  filter(bin_threshold == 0) %>%
  filter(!is.na(threshold)) %>%
  group_by(Asset) %>%
  mutate(
    trades_x =
      case_when(
        Final_Winnings == max(Final_Winnings, na.rm = T) ~
          glue::glue("{total_trades}\n{round(Final_Winnings)}")
      )
  ) %>%
  mutate(
    sig_thresh = as.character(sig_thresh)
  ) %>%
  ggplot(aes(x = threshold, y = Final_Winnings, color = sig_thresh))  +
  geom_line(show.legend = FALSE) +
  geom_point(show.legend = FALSE) +
  geom_label(aes(label = trades_x), size = 3, show.legend = FALSE, color = "black") +
  facet_wrap(.~Asset, scales = "free") +
  theme_minimal()

AR_LM_Pred_analysis %>%
  filter(!is.na(threshold)) %>%
  ggplot(aes(x = threshold, y = Binomial_Expectation_Adj, color = Asset)) +
  geom_line(show.legend = FALSE) +
  geom_point(show.legend = FALSE) +
  facet_wrap(.~Asset, scales = "free") +
  theme_minimal()

AR_LM_Pred_analysis %>%
  filter(!is.na(threshold)) %>%
  ggplot(aes(x = threshold, y = Return_Middle, color = Asset)) +
  geom_line(show.legend = FALSE) +
  geom_point(show.legend = FALSE) +
  facet_wrap(.~Asset, scales = "free") +
  theme_minimal()

AR_LM_Pred_analysis %>%
  filter(!is.na(threshold)) %>%
  ggplot(aes(x = threshold, y = Ratio_of_25_to_75 , color = Asset)) +
  geom_line(show.legend = FALSE) +
  geom_point( show.legend = FALSE) +
  geom_hline(yintercept = 1, linetype = "dashed", color = "red", size = 0.7) +
  facet_wrap(.~Asset, scales = "free") +
  theme_minimal()

all_control_winnings <-
  AR_LM_Pred_analysis %>%
  filter(trade_col == "Control") %>%
  distinct(Asset, sig_thresh, bin_threshold, Final_Winnings, Return_Middle, Ratio_of_25_to_75) %>%
  rename(
    Control_Winnings = Final_Winnings,
    Control_Middle = Return_Middle,
    Control_Ratio = Ratio_of_25_to_75
  )

max_points <-
  AR_LM_Pred_analysis %>%
  filter(total_trades >= 500, threshold > 0.1) %>%
  group_by(Asset) %>%
  slice_max(Final_Winnings, n = 3) %>%
  ungroup()

comparison_frame <-
  AR_LM_Pred_analysis %>%
  dplyr::select(Asset, sig_thresh, bin_threshold, threshold, total_trades,
                Final_Winnings, Return_Middle, Ratio_of_25_to_75) %>%
  left_join(
    all_control_winnings
  ) %>%
  mutate(
    Final_Value_diff = Final_Winnings - Control_Winnings
  ) %>%
  group_by(Asset) %>%
  slice_max(Final_Value_diff)

