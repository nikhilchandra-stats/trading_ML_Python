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
db_location = "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data_Most_Assets_2025-09-13 Second Algo.db"
start_date = "2015-01-01"
end_date = today() %>% as.character()

model_data_store_path <-
  "C:/Users/Nikhil Chandra/Documents/trade_data/single_asset_improved_indcator_trades_ts.db"
model_data_store_db <-
  connect_db(model_data_store_path)

indicator_data <-
  DBI::dbGetQuery(conn = model_data_store_db,
                  statement = "SELECT * FROM single_asset_improved") %>%
  distinct() %>%
  group_by(sim_index, Asset) %>%
  mutate(Date = as_datetime(Date),
         test_date_start = as_date(test_date_start),
         test_end_date = as_date(test_end_date),
         Date_filt = as_date(Date))
DBI::dbDisconnect(model_data_store_db)
rm(model_data_store_db)
gc()

Indices_Metals_Bonds <-
  get_Port_Buy_Data(
    db_location = db_location,
    start_date = start_date,
    end_date = today() %>% as.character(),
    time_frame = "H1"
  )

asset_of_interest <- "EUR_JPY"
asset_data = Indices_Metals_Bonds
stop_factor = 2
profit_factor = 15
risk_dollar_value = 4

indicator_data_asset_data_long <-
  indicator_data %>%
  ungroup() %>%
  filter(Asset == asset_of_interest, trade_col == "Long") %>%
  select(Date, Asset, contains("pred")) %>%
  distinct() %>%
  group_by(Date, Asset) %>%
  summarise(
    across(contains("pred"), .fns = ~ mean(., na.rm = T))
  ) %>%
  ungroup() %>%
  mutate(
    Date = as_datetime(Date)
  ) %>%
  dplyr::select(Date, Asset,
                logit_combined_pred_long = logit_combined_pred,
                mean_logit_combined_pred_long = mean_logit_combined_pred,
                sd_logit_combined_pred_long = sd_logit_combined_pred,
                averaged_pred_long = averaged_pred,
                mean_averaged_pred_long = mean_averaged_pred,
                sd_averaged_pred_long = sd_averaged_pred)

indicator_data_asset_data_short <-
  indicator_data %>%
  ungroup() %>%
  filter(Asset == asset_of_interest, trade_col == "Short") %>%
  select(Date, Asset, contains("pred")) %>%
  distinct() %>%
  group_by(Date, Asset) %>%
  summarise(
    across(contains("pred"), .fns = ~ mean(., na.rm = T))
  ) %>%
  ungroup() %>%
  mutate(
    Date = as_datetime(Date)
  ) %>%
  dplyr::select(Date, Asset,
                logit_combined_pred_short = logit_combined_pred,
                mean_logit_combined_pred_short = mean_logit_combined_pred,
                sd_logit_combined_pred_short = sd_logit_combined_pred,
                averaged_pred_short = averaged_pred,
                mean_averaged_pred_short = mean_averaged_pred,
                sd_averaged_pred_short = sd_averaged_pred)

indicator_max_date <- indicator_data_asset_data_long %>% pull(Date) %>% max() %>% as_datetime(tz = "Australia/Canberra")
indicator_min_date <- indicator_data_asset_data_long %>% pull(Date) %>% min() %>% as_datetime(tz = "Australia/Canberra")

bid_price <-
  asset_data[[2]] %>%
  filter(Asset == asset_of_interest) %>%
  dplyr::select(Date, Asset,
                Bid_Price = Price,
                Ask_High = High,
                Ask_Low = Low)

asset_data_with_indicator <-
  asset_data[[1]] %>%
  filter(Asset == asset_of_interest) %>%
  dplyr::select(Date, Asset,
                Ask_Price = Price,
                Bid_High = High,
                Bid_Low = Low) %>%
  left_join(
    bid_price
  ) %>%
  ungroup() %>%
  mutate(
    Date = as_datetime(Date)
  ) %>%
  left_join(indicator_data_asset_data_long) %>%
  left_join(indicator_data_asset_data_short) %>%
  filter(Date >= indicator_min_date) %>%
  mutate(
      trade_col =
        case_when(
          logit_combined_pred_long >= mean_logit_combined_pred_long + 2*sd_logit_combined_pred_long &
        averaged_pred_long >= sd_averaged_pred_long*2 + mean_averaged_pred_long ~ "Long"
        ),
      trade_col =
        case_when(
          logit_combined_pred_short <= mean_logit_combined_pred_short - 1*sd_logit_combined_pred_short &
            averaged_pred_short <= mean_averaged_pred_short - sd_averaged_pred_short*1 &
            is.na(trade_col) ~ "Short",
          TRUE ~ trade_col
        )
  ) %>%
  mutate(

    mean_movement = mean(Ask_Price - lag(Ask_Price), na.rm = T),
    sd_movement = sd(Ask_Price - lag(Ask_Price), na.rm = T),
    stop_value = stop_factor*sd_movement + mean_movement,
    profit_value = profit_factor*sd_movement + mean_movement,
    stop_point =
      case_when(
        trade_col == "Long" ~ lead(Ask_Price) - stop_value,
        trade_col == "Short" ~ lead(Bid_Price) + stop_value
      )

  ) %>%
  mutate(ending_value = str_extract(Asset, "_[A-Z][A-Z][A-Z]"),
         ending_value = str_remove_all(ending_value, "_")
  ) %>%
  left_join(currency_conversion, by =c("ending_value" = "not_aud_asset")) %>%
  left_join(asset_infor%>%
              rename(Asset = name) %>%
              dplyr::select(Asset,
                            minimumTradeSize,
                            marginRate,
                            pipLocation,
                            displayPrecision) ) %>%
  mutate(
    minimumTradeSize_OG = as.numeric(minimumTradeSize),
    minimumTradeSize = abs(log10(as.numeric(minimumTradeSize))),
    marginRate = as.numeric(marginRate),
    pipLocation = as.numeric(pipLocation),
    displayPrecision = as.numeric(displayPrecision)
  ) %>%
  ungroup() %>%
  mutate(
    stop_value = round(stop_value, abs(pipLocation) ),
    profit_value = round(profit_value, abs(pipLocation) )
  )  %>%
  mutate(
    volume_unadj =
      case_when(
        str_detect(Asset,"SEK|NOK|ZAR|MXN|CNH") ~ (risk_dollar_value/stop_value)*adjusted_conversion,
        TRUE ~ (risk_dollar_value/stop_value)/adjusted_conversion
      ),
    volume_required = volume_unadj,
    volume_adj =
      case_when(
        round(volume_unadj, minimumTradeSize) == 0 ~  minimumTradeSize_OG,
        round(volume_unadj, minimumTradeSize) != 0 ~  round(volume_unadj, minimumTradeSize)
      )
  ) %>%
  group_by(Asset) %>%
  arrange(Date, .by_group = TRUE) %>%
  ungroup() %>%
  mutate(
    # period_return_1_high =
    #   case_when(
    #     trade_col == "Long" ~
    #       adjusted_conversion*volume_adj*(lead(Bid_High,2) - lead(Ask_Price)),
    #     trade_col == "Short" ~ adjusted_conversion*volume_adj*(-1*(lead(Bid_Price) - lead(Ask_Low, 2))),
    #   ),
    # period_return_1_low =
    #   case_when(
    #     trade_col == "Long" ~
    #       adjusted_conversion*volume_adj*(-1*( lead(Ask_Price)-  lead(Bid_Low, 2) )),
    #     trade_col == "Short" ~
    #       adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Low, 2)),
    #   ),
    period_return_1_Price =
      case_when(
        trade_col == "Long" & lead(Bid_Low,2) > stop_point~
          adjusted_conversion*volume_adj*( (lead(Bid_Price, 2) - lead(Ask_Price)) ),
        trade_col == "Long" & lead(Bid_Low,2) <= stop_point ~ -1*risk_dollar_value

        trade_col == "Short" & lead(Ask_High,2) < stop_point ~
          adjusted_conversion*volume_adj*(lead(Ask_Price,2) - lead(Bid_Price) ),
        trade_col == "Short" & lead(Ask_High,2) >= stop_point
      ),


    # period_return_2_high =
    #   case_when(
    #     trade_col == "Long" ~
    #       adjusted_conversion*volume_adj*(lead(Bid_High,3) - lead(Ask_Price)),
    #     trade_col == "Short" ~ adjusted_conversion*volume_adj*(-1*(lead(Bid_Price) - lead(Ask_Low, 3))),
    #   ),
    # period_return_2_low =
    #   case_when(
    #     trade_col == "Long" ~
    #       adjusted_conversion*volume_adj*(-1*( lead(Ask_Price)-  lead(Bid_Low, 3) )),
    #     trade_col == "Short" ~
    #       adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Low, 3)),
    #   ),
    period_return_2_Price =
      case_when(
        trade_col == "Long" ~
          adjusted_conversion*volume_adj*( (lead(Bid_Price, 3) - lead(Ask_Price)) ),
        trade_col == "Short" ~
          adjusted_conversion*volume_adj*(lead(Ask_Price,3) - lead(Bid_Price) ),
      ),

    # period_return_3_high =
    #   case_when(
    #     trade_col == "Long" ~
    #       adjusted_conversion*volume_adj*(lead(Bid_High,4) - lead(Ask_Price)),
    #     trade_col == "Short" ~ adjusted_conversion*volume_adj*(-1*(lead(Bid_Price) - lead(Ask_Low, 4))),
    #   ),
    # period_return_3_low =
    #   case_when(
    #     trade_col == "Long" ~
    #       adjusted_conversion*volume_adj*(-1*( lead(Ask_Price)-  lead(Bid_Low, 4) )),
    #     trade_col == "Short" ~
    #       adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Low, 4)),
    #   ),
    period_return_3_Price =
      case_when(
        trade_col == "Long" ~
          adjusted_conversion*volume_adj*( (lead(Bid_Price, 4) - lead(Ask_Price)) ),
        trade_col == "Short" ~
          adjusted_conversion*volume_adj*(lead(Ask_Price,4) - lead(Bid_Price) ),
      ),

    # period_return_4_high =
    #   case_when(
    #     trade_col == "Long" ~
    #       adjusted_conversion*volume_adj*(lead(Bid_High,5) - lead(Ask_Price)),
    #     trade_col == "Short" ~ adjusted_conversion*volume_adj*(-1*(lead(Bid_Price) - lead(Ask_Low,5))),
    #   ),
    # period_return_4_low =
    #   case_when(
    #     trade_col == "Long" ~
    #       adjusted_conversion*volume_adj*(-1*( lead(Ask_Price)-  lead(Bid_Low,5) )),
    #     trade_col == "Short" ~
    #       adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Low,5)),
    #   ),
    period_return_4_Price =
      case_when(
        trade_col == "Long" ~
          adjusted_conversion*volume_adj*( (lead(Bid_Price,5) - lead(Ask_Price)) ),
        trade_col == "Short" ~
          adjusted_conversion*volume_adj*(lead(Ask_Price,5) - lead(Bid_Price) ),
      ),

    period_return_5_Price =
      case_when(
        trade_col == "Long" ~
          adjusted_conversion*volume_adj*( (lead(Bid_Price,6) - lead(Ask_Price)) ),
        trade_col == "Short" ~
          adjusted_conversion*volume_adj*(lead(Ask_Price,6) - lead(Bid_Price) ),
      ),

    period_return_24_Price =
      case_when(
        trade_col == "Long" ~
          adjusted_conversion*volume_adj*( (lead(Bid_Price,25) - lead(Ask_Price)) ),
        trade_col == "Short" ~
          adjusted_conversion*volume_adj*(lead(Ask_Price,25) - lead(Bid_Price) ),
      )

  )


asset_data_with_indicator %>%
  dplyr::select(Asset, trade_col,
                period_return_1_Price,
                period_return_2_Price,
                period_return_3_Price,
                period_return_4_Price,
                period_return_5_Price) %>%
  pivot_longer(-c(Asset, trade_col), names_to = "Period", values_to = "Values") %>%
  ggplot(aes(x = Values, fill = Period)) +
  geom_density(alpha = 0.5) +
  facet_wrap(.~Period) +
  theme_minimal() +
  theme(legend.position = "bottom")

summary_values <-
  asset_data_with_indicator %>%
  dplyr::select(Asset, trade_col,
                period_return_1_Price,
                period_return_2_Price,
                period_return_3_Price,
                period_return_4_Price,
                period_return_5_Price,
                period_return_24_Price) %>%
  pivot_longer(-c(Asset, trade_col), names_to = "Period", values_to = "Values") %>%
  group_by(trade_col, Asset, Period) %>%
  summarise(
    mean_return = mean(Values, na.rm = T),
    quant_25_return = quantile(Values,0.25, na.rm = T),
    median_return = median(Values, na.rm = T),
    quant_75_return = quantile(Values,0.75, na.rm = T)
  ) %>%
  filter(if_all(everything(),~!is.nan(.)))
