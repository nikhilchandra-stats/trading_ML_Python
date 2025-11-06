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
start_date = "2022-01-01"
end_date = "2024-01-01"

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


#' create_running_profits
#'
#' @param asset_of_interest
#' @param asset_data
#' @param stop_factor
#' @param profit_factor
#' @param risk_dollar_value
#' @param trade_direction
#' @param currency_conversion
#' @param asset_infor
#'
#' @return
#' @export
#'
#' @examples
create_running_profits <-
  function(
    asset_of_interest = "EUR_JPY",
    asset_data = Indices_Metals_Bonds,
    stop_factor = 2,
    profit_factor = 15,
    risk_dollar_value = 4,
    trade_direction = "Long",
    currency_conversion = currency_conversion,
    asset_infor = asset_infor
    ) {

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
      mutate(
        trade_col = trade_direction
      )  %>%
      mutate(

        mean_movement = mean(Ask_Price - lag(Ask_Price), na.rm = T),
        sd_movement = sd(Ask_Price - lag(Ask_Price), na.rm = T),
        stop_value = stop_factor*sd_movement + mean_movement,
        profit_value = profit_factor*sd_movement + mean_movement,
        stop_point =
          case_when(
            trade_col == "Long" ~ lead(Ask_Price) - stop_value,
            trade_col == "Short" ~ lead(Bid_Price) + stop_value
          ),

        profit_point =
          case_when(
            trade_col == "Long" ~ lead(Ask_Price) + profit_value,
            trade_col == "Short" ~ lead(Bid_Price) - profit_value
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
        across(
          .cols = c(Ask_Price, Bid_Price,Bid_High,Bid_Low,Ask_High, Ask_Low  ),
          .fns = ~ as.numeric(.)
        )
      ) %>%
      mutate(
        profit_return = profit_value*adjusted_conversion*volume_adj
      ) %>%
      mutate(

        period_return_1_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,2) > stop_point &
              lead(Bid_High,2) < profit_point ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price, 2) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,2) > stop_point &
              lead(Bid_High,2) > profit_point  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,2) <= stop_point|
              trade_col == "Long" & lead(Bid_Low,1) <= stop_point ~ -1*risk_dollar_value,

            trade_col == "Short" & lead(Ask_High,2) < stop_point &
              lead(Ask_Low,2) > profit_point ~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,2) ),

            trade_col == "Short" & lead(Ask_High,2) < stop_point &
              lead(Ask_Low,2) < profit_point ~ profit_return,

            trade_col == "Short" & lead(Ask_High,2) >= stop_point|
              trade_col == "Short" & lead(Ask_High,1) >= stop_point ~ -1*risk_dollar_value
          ),

        period_return_2_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,3) > stop_point &
              lead(Bid_High,3) < profit_point &
              period_return_1_Price != -1*risk_dollar_value ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price, 3) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,3) > stop_point &
              lead(Bid_High,3) > profit_point &
              period_return_1_Price != -1*risk_dollar_value  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,3) <= stop_point|
              period_return_1_Price== -1*risk_dollar_value  ~ -1*risk_dollar_value,

            trade_col == "Short" & lead(Ask_High,3) < stop_point &
              lead(Ask_Low,3) > profit_point &
              period_return_1_Price != -1*risk_dollar_value~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,3) ),

            trade_col == "Short" & lead(Ask_High,3) < stop_point &
              lead(Ask_Low,3) < profit_point &
              period_return_1_Price != -1*risk_dollar_value~ profit_return,

            trade_col == "Short" & lead(Ask_High,3) >= stop_point|
              period_return_1_Price == -1*risk_dollar_value ~ -1*risk_dollar_value
          ),

        period_return_3_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,4) > stop_point &
              lead(Bid_High,4) < profit_point &
              period_return_2_Price != -1*risk_dollar_value ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,4) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,4) > stop_point &
              lead(Bid_High,4) > profit_point &
              period_return_2_Price != -1*risk_dollar_value  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,4) <= stop_point|
              period_return_2_Price== -1*risk_dollar_value  ~ -1*risk_dollar_value,

            trade_col == "Short" & lead(Ask_High,4) < stop_point &
              lead(Ask_Low,4) > profit_point &
              period_return_2_Price != -1*risk_dollar_value~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,4) ),

            trade_col == "Short" & lead(Ask_High,4) < stop_point &
              lead(Ask_Low,4) < profit_point &
              period_return_2_Price != -1*risk_dollar_value~ profit_return,

            trade_col == "Short" & lead(Ask_High,4) >= stop_point|
              period_return_2_Price == -1*risk_dollar_value ~ -1*risk_dollar_value
          ),

        period_return_4_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,5) > stop_point &
              lead(Bid_High,5) < profit_point &
              period_return_3_Price != -1*risk_dollar_value ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,5) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,5) > stop_point &
              lead(Bid_High,5) > profit_point &
              period_return_3_Price != -1*risk_dollar_value  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,5) <= stop_point|
              period_return_3_Price== -1*risk_dollar_value  ~ -1*risk_dollar_value,

            trade_col == "Short" & lead(Ask_High,5) < stop_point &
              lead(Ask_Low,5) > profit_point &
              period_return_3_Price != -1*risk_dollar_value~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,5) ),

            trade_col == "Short" & lead(Ask_High,5) < stop_point &
              lead(Ask_Low,5) < profit_point &
              period_return_3_Price != -1*risk_dollar_value~ profit_return,

            trade_col == "Short" & lead(Ask_High,5) >= stop_point|
              period_return_3_Price == -1*risk_dollar_value ~ -1*risk_dollar_value
          ),

        period_return_5_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,6) > stop_point &
              lead(Bid_High,6) < profit_point &
              period_return_4_Price != -1*risk_dollar_value ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,6) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,6) > stop_point &
              lead(Bid_High,6) > profit_point &
              period_return_4_Price != -1*risk_dollar_value  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,6) <= stop_point|
              period_return_4_Price== -1*risk_dollar_value  ~ -1*risk_dollar_value,

            trade_col == "Short" & lead(Ask_High,6) < stop_point &
              lead(Ask_Low,6) > profit_point &
              period_return_4_Price != -1*risk_dollar_value~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,6) ),

            trade_col == "Short" & lead(Ask_High,6) < stop_point &
              lead(Ask_Low,6) < profit_point &
              period_return_4_Price != -1*risk_dollar_value~ profit_return,

            trade_col == "Short" & lead(Ask_High,6) >= stop_point|
              period_return_4_Price == -1*risk_dollar_value ~ -1*risk_dollar_value
          ),

        period_return_6_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,7) > stop_point &
              lead(Bid_High,7) < profit_point &
              period_return_5_Price != -1*risk_dollar_value ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,7) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,7) > stop_point &
              lead(Bid_High,7) > profit_point &
              period_return_5_Price != -1*risk_dollar_value  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,7) <= stop_point|
              period_return_5_Price== -1*risk_dollar_value  ~ -1*risk_dollar_value,

            trade_col == "Short" & lead(Ask_High,7) < stop_point &
              lead(Ask_Low,7) > profit_point &
              period_return_5_Price != -1*risk_dollar_value~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,7) ),

            trade_col == "Short" & lead(Ask_High,7) < stop_point &
              lead(Ask_Low,7) < profit_point &
              period_return_5_Price != -1*risk_dollar_value~ profit_return,

            trade_col == "Short" & lead(Ask_High,7) >= stop_point|
              period_return_5_Price == -1*risk_dollar_value ~ -1*risk_dollar_value
          ),

        period_return_7_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,8) > stop_point &
              lead(Bid_High,8) < profit_point &
              period_return_6_Price != -1*risk_dollar_value ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,8) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,8) > stop_point &
              lead(Bid_High,8) > profit_point &
              period_return_6_Price != -1*risk_dollar_value  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,8) <= stop_point|
              period_return_6_Price== -1*risk_dollar_value  ~ -1*risk_dollar_value,

            trade_col == "Short" & lead(Ask_High,8) < stop_point &
              lead(Ask_Low,8) > profit_point &
              period_return_6_Price != -1*risk_dollar_value~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,8) ),

            trade_col == "Short" & lead(Ask_High,8) < stop_point &
              lead(Ask_Low,8) < profit_point &
              period_return_6_Price != -1*risk_dollar_value~ profit_return,

            trade_col == "Short" & lead(Ask_High,8) >= stop_point|
              period_return_6_Price == -1*risk_dollar_value ~ -1*risk_dollar_value
          ),

        period_return_8_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,9) > stop_point &
              lead(Bid_High,9) < profit_point &
              period_return_7_Price != -1*risk_dollar_value ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,9) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,9) > stop_point &
              lead(Bid_High,9) > profit_point &
              period_return_7_Price != -1*risk_dollar_value  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,9) <= stop_point|
              period_return_7_Price== -1*risk_dollar_value  ~ -1*risk_dollar_value,

            trade_col == "Short" & lead(Ask_High,9) < stop_point &
              lead(Ask_Low,9) > profit_point &
              period_return_7_Price != -1*risk_dollar_value~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,9) ),

            trade_col == "Short" & lead(Ask_High,9) < stop_point &
              lead(Ask_Low,9) < profit_point &
              period_return_7_Price != -1*risk_dollar_value~ profit_return,

            trade_col == "Short" & lead(Ask_High,9) >= stop_point|
              period_return_7_Price == -1*risk_dollar_value ~ -1*risk_dollar_value
          ),


        period_return_9_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,10) > stop_point &
              lead(Bid_High,10) < profit_point &
              period_return_8_Price != -1*risk_dollar_value ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,10) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,10) > stop_point &
              lead(Bid_High,10) > profit_point &
              period_return_8_Price != -1*risk_dollar_value  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,10) <= stop_point|
              period_return_8_Price== -1*risk_dollar_value  ~ -1*risk_dollar_value,

            trade_col == "Short" & lead(Ask_High,10) < stop_point &
              lead(Ask_Low,10) > profit_point &
              period_return_8_Price != -1*risk_dollar_value~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,10) ),

            trade_col == "Short" & lead(Ask_High,10) < stop_point &
              lead(Ask_Low,10) < profit_point &
              period_return_8_Price != -1*risk_dollar_value~ profit_return,

            trade_col == "Short" & lead(Ask_High,10) >= stop_point|
              period_return_8_Price == -1*risk_dollar_value ~ -1*risk_dollar_value
          ),


        period_return_10_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,11) > stop_point &
              lead(Bid_High,11) < profit_point &
              period_return_9_Price != -1*risk_dollar_value ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,11) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,11) > stop_point &
              lead(Bid_High,11) > profit_point &
              period_return_9_Price != -1*risk_dollar_value  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,11) <= stop_point|
              period_return_9_Price== -1*risk_dollar_value  ~ -1*risk_dollar_value,

            trade_col == "Short" & lead(Ask_High,11) < stop_point &
              lead(Ask_Low,11) > profit_point &
              period_return_9_Price != -1*risk_dollar_value~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,11) ),

            trade_col == "Short" & lead(Ask_High,11) < stop_point &
              lead(Ask_Low,11) < profit_point &
              period_return_9_Price != -1*risk_dollar_value~ profit_return,

            trade_col == "Short" & lead(Ask_High,11) >= stop_point|
              period_return_9_Price == -1*risk_dollar_value ~ -1*risk_dollar_value
          ),

        period_return_11_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,12) > stop_point &
              lead(Bid_High,12) < profit_point &
              period_return_10_Price != -1*risk_dollar_value ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,12) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,12) > stop_point &
              lead(Bid_High,12) > profit_point &
              period_return_10_Price != -1*risk_dollar_value  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,12) <= stop_point|
              period_return_10_Price== -1*risk_dollar_value  ~ -1*risk_dollar_value,

            trade_col == "Short" & lead(Ask_High,12) < stop_point &
              lead(Ask_Low,12) > profit_point &
              period_return_10_Price != -1*risk_dollar_value~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,12) ),

            trade_col == "Short" & lead(Ask_High,12) < stop_point &
              lead(Ask_Low,12) < profit_point &
              period_return_10_Price != -1*risk_dollar_value~ profit_return,

            trade_col == "Short" & lead(Ask_High,12) >= stop_point|
              period_return_10_Price == -1*risk_dollar_value ~ -1*risk_dollar_value
          ),

        period_return_12_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,13) > stop_point &
              lead(Bid_High,13) < profit_point &
              period_return_11_Price != -1*risk_dollar_value ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,13) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,13) > stop_point &
              lead(Bid_High,13) > profit_point &
              period_return_11_Price != -1*risk_dollar_value  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,13) <= stop_point|
              period_return_11_Price== -1*risk_dollar_value  ~ -1*risk_dollar_value,

            trade_col == "Short" & lead(Ask_High,13) < stop_point &
              lead(Ask_Low,13) > profit_point &
              period_return_11_Price != -1*risk_dollar_value~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,13) ),

            trade_col == "Short" & lead(Ask_High,13) < stop_point &
              lead(Ask_Low,13) < profit_point &
              period_return_11_Price != -1*risk_dollar_value~ profit_return,

            trade_col == "Short" & lead(Ask_High,13) >= stop_point|
              period_return_11_Price == -1*risk_dollar_value ~ -1*risk_dollar_value
          ),

      )

    return(asset_data_with_indicator)

  }

get_multi_sngle_asst_impr_indcator <-
  function(
    indicator_data = indicator_data,
    indicator_asset_list = c("EUR_JPY", "EUR_USD"),
    indicator_stop = 2,
    indicator_period= 24
    ) {

    indicator_data_asset_data_long <-
      indicator_data %>%
      ungroup() %>%
      filter(Asset %in% indicator_asset_list, trade_col == "Long",
              stop_factor == indicator_stop, periods_ahead == indicator_period) %>%
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
                    sd_averaged_pred_long = sd_averaged_pred) %>%
      pivot_longer(-c(Date, Asset), names_to = "var_names", values_to = "value") %>%
      group_by(Date, var_names) %>%
      summarise(value = mean(value, na.rm = T)) %>%
      ungroup() %>%
      distinct() %>%
      pivot_wider( values_from = value, names_from = var_names)

      # mutate(
      #   new_var_name = paste0(Asset, "_", var_names)
      # ) %>%
      # dplyr::select(-var_names, -Asset) %>%
      # distinct() %>%
      # pivot_wider( values_from = value, names_from = new_var_name)

    indicator_data_asset_data_short <-
      indicator_data %>%
      ungroup() %>%
      filter(Asset %in% indicator_asset_list, trade_col == "Short" ,
             stop_factor == indicator_stop, periods_ahead == indicator_period) %>%
      select(Date,Asset, contains("pred")) %>%
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
                    sd_averaged_pred_short = sd_averaged_pred) %>%
      pivot_longer(-c(Date, Asset), names_to = "var_names", values_to = "value") %>%
      group_by(Date, var_names) %>%
      summarise(value = mean(value, na.rm = T)) %>%
      ungroup() %>%
      distinct() %>%
      pivot_wider( values_from = value, names_from = var_names)
      # mutate(
      #   new_var_name = paste0(Asset, "_", var_names)
      # ) %>%
      # dplyr::select(-var_names, -Asset) %>%
      # distinct() %>%
      # pivot_wider( values_from = value, names_from = new_var_name)

    indicators_joined <-
      indicator_data_asset_data_long %>%
      left_join(indicator_data_asset_data_short)

    return(indicators_joined)

  }

get_return_indicator_joined_data <-
  function(
    asset_of_interest = "EUR_JPY",
    asset_data = Indices_Metals_Bonds,
    stop_factor_long = 1,
    profit_factor_long = 15,
    stop_factor_short = 1,
    profit_factor_short = 2,
    risk_dollar_value = 4,
    trade_direction = "Long",
    currency_conversion = currency_conversion,
    asset_infor = asset_infor,
    indicator_data = indicator_data,
    indicator_asset_list = c("EUR_JPY", "EUR_USD", "USD_JPY", "EUR_GBP", "EUR_NZD"),
    long_factor = 0,
    indicator_stop = 2,
    indicator_period= 24
    ) {


    indicators_joined_raw <-
      get_multi_sngle_asst_impr_indcator(
      indicator_data = indicator_data,
      indicator_asset_list = indicator_asset_list,
      indicator_stop = indicator_stop,
      indicator_period= indicator_period
    ) %>%
      mutate(Asset = asset_of_interest)

    Longs <- create_running_profits(
      asset_of_interest = asset_of_interest,
      asset_data = asset_data,
      stop_factor = stop_factor_long,
      profit_factor = profit_factor_long,
      risk_dollar_value = risk_dollar_value,
      trade_direction = "Long",
      currency_conversion = currency_conversion,
      asset_infor = asset_infor
    )

    Shorts <- create_running_profits(
      asset_of_interest = asset_of_interest,
      asset_data = asset_data,
      stop_factor = stop_factor_short,
      profit_factor = profit_factor_short,
      risk_dollar_value = risk_dollar_value,
      trade_direction = "Short",
      currency_conversion = currency_conversion,
      asset_infor = asset_infor
    )


    short_results <-
      Shorts %>%
      dplyr::select(Date, Asset, contains("period_return_") )

    names(short_results) <-
      names(short_results) %>%
      map(
          ~ case_when(
            str_detect(.x, "period_return_") ~ paste0("Short_",.x),
            TRUE ~ .x
          )
        ) %>%
      unlist() %>%
      as.character()

    long_results <-
      Longs %>%
      dplyr::select(Date, Asset, contains("period_return_") )

    names(long_results) <-
      names(long_results) %>%
      map(
        ~ case_when(
          str_detect(.x, "period_return_") ~ paste0("Long_",.x),
          TRUE ~ .x
        )
      ) %>%
      unlist() %>%
      as.character()

    all_results <-
      long_results %>%
      left_join(short_results)

    indicators_joined <-
      indicators_joined_raw %>%
      left_join(all_results) %>%
      filter(!is.na(Long_period_return_1_Price)) %>%
      mutate(
        trade_col =
          case_when(
            logit_combined_pred_long >=
              mean_logit_combined_pred_long + sd_logit_combined_pred_long*long_factor &
              averaged_pred_long >=
              mean_averaged_pred_long + sd_averaged_pred_long*long_factor ~ "Long"
          )
      ) %>%
      dplyr::select(
        Date, Asset, trade_col,
       !!as.name(glue::glue("Long_period_return_{period_var}_Price")),
       !!as.name(glue::glue("Short_period_return_{period_var}_Price"))
      )

    results_summary <- indicators_joined %>%
      filter(trade_col == "Long") %>%
      mutate(Total_Return =
               !!as.name(glue::glue("Long_period_return_{period_var}_Price")) +
               !!as.name(glue::glue("Short_period_return_{period_var}_Price"))) %>%
      mutate(
        month_date = floor_date(Date, unit = "month"),
        Total_Return_cum = cumsum(!!as.name(glue::glue("Long_period_return_{period_var}_Price")))
      )

    subtitle <- glue::glue("{paste(indicator_asset_list, collapse = ',')}")

    results_summary %>%
      ggplot(aes(x = Date, y = Total_Return_cum)) +
      geom_line() +
      labs(subtitle = subtitle, title = "Returns") +
      theme_minimal()

  }



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
