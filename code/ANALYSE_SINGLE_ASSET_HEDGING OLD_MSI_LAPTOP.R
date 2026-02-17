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
db_location = "D:/Asset Data/Oanda_Asset_Data.db"
start_date = "2023-01-01"
end_date = "2025-11-01"

model_data_store_path <-
  "D:/trade_data/single_asset_improved_indcator_trades_ts_more_cop.db"
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

indicator_data <-
  indicator_data %>%
  group_by(Asset, Date, trade_col) %>%
  slice_max(sim_index) %>%
  ungroup()

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
        profit_return = profit_value*adjusted_conversion*volume_adj,
        stop_return = stop_value*adjusted_conversion*volume_adj
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
              trade_col == "Long" & lead(Bid_Low,1) <= stop_point ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,2) < stop_point &
              lead(Ask_Low,2) > profit_point ~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,2) ),

            trade_col == "Short" & lead(Ask_High,2) < stop_point &
              lead(Ask_Low,2) < profit_point ~ profit_return,

            trade_col == "Short" & lead(Ask_High,2) >= stop_point|
              trade_col == "Short" & lead(Ask_High,1) >= stop_point ~ -1*stop_return
          ),

        period_return_2_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,3) > stop_point &
              lead(Bid_High,3) < profit_point &
              period_return_1_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price, 3) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,3) > stop_point &
              lead(Bid_High,3) > profit_point &
              period_return_1_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,3) <= stop_point|
              period_return_1_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,3) < stop_point &
              lead(Ask_Low,3) > profit_point &
              period_return_1_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,3) ),

            trade_col == "Short" & lead(Ask_High,3) < stop_point &
              lead(Ask_Low,3) < profit_point &
              period_return_1_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,3) >= stop_point|
              period_return_1_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_3_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,4) > stop_point &
              lead(Bid_High,4) < profit_point &
              period_return_2_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,4) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,4) > stop_point &
              lead(Bid_High,4) > profit_point &
              period_return_2_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,4) <= stop_point|
              period_return_2_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,4) < stop_point &
              lead(Ask_Low,4) > profit_point &
              period_return_2_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,4) ),

            trade_col == "Short" & lead(Ask_High,4) < stop_point &
              lead(Ask_Low,4) < profit_point &
              period_return_2_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,4) >= stop_point|
              period_return_2_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_4_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,5) > stop_point &
              lead(Bid_High,5) < profit_point &
              period_return_3_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,5) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,5) > stop_point &
              lead(Bid_High,5) > profit_point &
              period_return_3_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,5) <= stop_point|
              period_return_3_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,5) < stop_point &
              lead(Ask_Low,5) > profit_point &
              period_return_3_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,5) ),

            trade_col == "Short" & lead(Ask_High,5) < stop_point &
              lead(Ask_Low,5) < profit_point &
              period_return_3_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,5) >= stop_point|
              period_return_3_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_5_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,6) > stop_point &
              lead(Bid_High,6) < profit_point &
              period_return_4_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,6) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,6) > stop_point &
              lead(Bid_High,6) > profit_point &
              period_return_4_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,6) <= stop_point|
              period_return_4_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,6) < stop_point &
              lead(Ask_Low,6) > profit_point &
              period_return_4_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,6) ),

            trade_col == "Short" & lead(Ask_High,6) < stop_point &
              lead(Ask_Low,6) < profit_point &
              period_return_4_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,6) >= stop_point|
              period_return_4_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_6_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,7) > stop_point &
              lead(Bid_High,7) < profit_point &
              period_return_5_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,7) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,7) > stop_point &
              lead(Bid_High,7) > profit_point &
              period_return_5_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,7) <= stop_point|
              period_return_5_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,7) < stop_point &
              lead(Ask_Low,7) > profit_point &
              period_return_5_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,7) ),

            trade_col == "Short" & lead(Ask_High,7) < stop_point &
              lead(Ask_Low,7) < profit_point &
              period_return_5_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,7) >= stop_point|
              period_return_5_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_7_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,8) > stop_point &
              lead(Bid_High,8) < profit_point &
              period_return_6_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,8) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,8) > stop_point &
              lead(Bid_High,8) > profit_point &
              period_return_6_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,8) <= stop_point|
              period_return_6_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,8) < stop_point &
              lead(Ask_Low,8) > profit_point &
              period_return_6_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,8) ),

            trade_col == "Short" & lead(Ask_High,8) < stop_point &
              lead(Ask_Low,8) < profit_point &
              period_return_6_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,8) >= stop_point|
              period_return_6_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_8_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,9) > stop_point &
              lead(Bid_High,9) < profit_point &
              period_return_7_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,9) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,9) > stop_point &
              lead(Bid_High,9) > profit_point &
              period_return_7_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,9) <= stop_point|
              period_return_7_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,9) < stop_point &
              lead(Ask_Low,9) > profit_point &
              period_return_7_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,9) ),

            trade_col == "Short" & lead(Ask_High,9) < stop_point &
              lead(Ask_Low,9) < profit_point &
              period_return_7_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,9) >= stop_point|
              period_return_7_Price == -1*stop_return ~ -1*stop_return
          ),


        period_return_9_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,10) > stop_point &
              lead(Bid_High,10) < profit_point &
              period_return_8_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,10) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,10) > stop_point &
              lead(Bid_High,10) > profit_point &
              period_return_8_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,10) <= stop_point|
              period_return_8_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,10) < stop_point &
              lead(Ask_Low,10) > profit_point &
              period_return_8_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,10) ),

            trade_col == "Short" & lead(Ask_High,10) < stop_point &
              lead(Ask_Low,10) < profit_point &
              period_return_8_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,10) >= stop_point|
              period_return_8_Price == -1*stop_return ~ -1*stop_return
          ),


        period_return_10_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,11) > stop_point &
              lead(Bid_High,11) < profit_point &
              period_return_9_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,11) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,11) > stop_point &
              lead(Bid_High,11) > profit_point &
              period_return_9_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,11) <= stop_point|
              period_return_9_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,11) < stop_point &
              lead(Ask_Low,11) > profit_point &
              period_return_9_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,11) ),

            trade_col == "Short" & lead(Ask_High,11) < stop_point &
              lead(Ask_Low,11) < profit_point &
              period_return_9_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,11) >= stop_point|
              period_return_9_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_11_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,12) > stop_point &
              lead(Bid_High,12) < profit_point &
              period_return_10_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,12) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,12) > stop_point &
              lead(Bid_High,12) > profit_point &
              period_return_10_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,12) <= stop_point|
              period_return_10_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,12) < stop_point &
              lead(Ask_Low,12) > profit_point &
              period_return_10_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,12) ),

            trade_col == "Short" & lead(Ask_High,12) < stop_point &
              lead(Ask_Low,12) < profit_point &
              period_return_10_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,12) >= stop_point|
              period_return_10_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_12_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,13) > stop_point &
              lead(Bid_High,13) < profit_point &
              period_return_11_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,13) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,13) > stop_point &
              lead(Bid_High,13) > profit_point &
              period_return_11_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,13) <= stop_point|
              period_return_11_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,13) < stop_point &
              lead(Ask_Low,13) > profit_point &
              period_return_11_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,13) ),

            trade_col == "Short" & lead(Ask_High,13) < stop_point &
              lead(Ask_Low,13) < profit_point &
              period_return_11_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,13) >= stop_point|
              period_return_11_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_13_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,14) > stop_point &
              lead(Bid_High,14) < profit_point &
              period_return_12_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,14) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,14) > stop_point &
              lead(Bid_High,14) > profit_point &
              period_return_12_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,14) <= stop_point|
              period_return_12_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,14) < stop_point &
              lead(Ask_Low,14) > profit_point &
              period_return_12_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,14) ),

            trade_col == "Short" & lead(Ask_High,14) < stop_point &
              lead(Ask_Low,14) < profit_point &
              period_return_12_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,14) >= stop_point|
              period_return_12_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_14_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,15) > stop_point &
              lead(Bid_High,15) < profit_point &
              period_return_13_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,15) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,15) > stop_point &
              lead(Bid_High,15) > profit_point &
              period_return_13_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,15) <= stop_point|
              period_return_13_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,15) < stop_point &
              lead(Ask_Low,15) > profit_point &
              period_return_13_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,15) ),

            trade_col == "Short" & lead(Ask_High,15) < stop_point &
              lead(Ask_Low,15) < profit_point &
              period_return_13_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,15) >= stop_point|
              period_return_13_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_15_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,16) > stop_point &
              lead(Bid_High,16) < profit_point &
              period_return_14_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,16) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,16) > stop_point &
              lead(Bid_High,16) > profit_point &
              period_return_14_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,16) <= stop_point|
              period_return_14_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,16) < stop_point &
              lead(Ask_Low,16) > profit_point &
              period_return_14_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,16) ),

            trade_col == "Short" & lead(Ask_High,16) < stop_point &
              lead(Ask_Low,16) < profit_point &
              period_return_14_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,16) >= stop_point|
              period_return_14_Price == -1*stop_return ~ -1*stop_return
          ) ,

        period_return_16_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,17) > stop_point &
              lead(Bid_High,17) < profit_point &
              period_return_15_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,17) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,17) > stop_point &
              lead(Bid_High,17) > profit_point &
              period_return_15_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,17) <= stop_point|
              period_return_15_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,17) < stop_point &
              lead(Ask_Low,17) > profit_point &
              period_return_15_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,17) ),

            trade_col == "Short" & lead(Ask_High,17) < stop_point &
              lead(Ask_Low,17) < profit_point &
              period_return_15_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,17) >= stop_point|
              period_return_15_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_17_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,18) > stop_point &
              lead(Bid_High,18) < profit_point &
              period_return_16_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,18) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,18) > stop_point &
              lead(Bid_High,18) > profit_point &
              period_return_16_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,18) <= stop_point|
              period_return_16_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,18) < stop_point &
              lead(Ask_Low,18) > profit_point &
              period_return_16_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,18) ),

            trade_col == "Short" & lead(Ask_High,18) < stop_point &
              lead(Ask_Low,18) < profit_point &
              period_return_16_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,18) >= stop_point|
              period_return_16_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_18_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,19) > stop_point &
              lead(Bid_High,19) < profit_point &
              period_return_17_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,19) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,19) > stop_point &
              lead(Bid_High,19) > profit_point &
              period_return_17_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,19) <= stop_point|
              period_return_17_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,19) < stop_point &
              lead(Ask_Low,19) > profit_point &
              period_return_17_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,19) ),

            trade_col == "Short" & lead(Ask_High,19) < stop_point &
              lead(Ask_Low,19) < profit_point &
              period_return_17_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,19) >= stop_point|
              period_return_17_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_19_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,20) > stop_point &
              lead(Bid_High,20) < profit_point &
              period_return_18_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,20) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,20) > stop_point &
              lead(Bid_High,20) > profit_point &
              period_return_18_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,20) <= stop_point|
              period_return_18_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,20) < stop_point &
              lead(Ask_Low,20) > profit_point &
              period_return_18_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,20) ),

            trade_col == "Short" & lead(Ask_High,20) < stop_point &
              lead(Ask_Low,20) < profit_point &
              period_return_18_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,20) >= stop_point|
              period_return_18_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_20_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,21) > stop_point &
              lead(Bid_High,21) < profit_point &
              period_return_19_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,21) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,21) > stop_point &
              lead(Bid_High,21) > profit_point &
              period_return_19_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,21) <= stop_point|
              period_return_19_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,21) < stop_point &
              lead(Ask_Low,21) > profit_point &
              period_return_19_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,21) ),

            trade_col == "Short" & lead(Ask_High,21) < stop_point &
              lead(Ask_Low,21) < profit_point &
              period_return_19_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,21) >= stop_point|
              period_return_19_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_21_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,22) > stop_point &
              lead(Bid_High,22) < profit_point &
              period_return_20_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,22) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,22) > stop_point &
              lead(Bid_High,22) > profit_point &
              period_return_20_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,22) <= stop_point|
              period_return_20_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,22) < stop_point &
              lead(Ask_Low,22) > profit_point &
              period_return_20_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,22) ),

            trade_col == "Short" & lead(Ask_High,22) < stop_point &
              lead(Ask_Low,22) < profit_point &
              period_return_20_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,22) >= stop_point|
              period_return_20_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_22_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,23) > stop_point &
              lead(Bid_High,23) < profit_point &
              period_return_21_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,23) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,23) > stop_point &
              lead(Bid_High,23) > profit_point &
              period_return_21_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,23) <= stop_point|
              period_return_21_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,23) < stop_point &
              lead(Ask_Low,23) > profit_point &
              period_return_21_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,23) ),

            trade_col == "Short" & lead(Ask_High,23) < stop_point &
              lead(Ask_Low,23) < profit_point &
              period_return_21_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,23) >= stop_point|
              period_return_21_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_23_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,24) > stop_point &
              lead(Bid_High,24) < profit_point &
              period_return_22_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,24) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,24) > stop_point &
              lead(Bid_High,24) > profit_point &
              period_return_22_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,24) <= stop_point|
              period_return_22_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,24) < stop_point &
              lead(Ask_Low,24) > profit_point &
              period_return_22_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,24) ),

            trade_col == "Short" & lead(Ask_High,24) < stop_point &
              lead(Ask_Low,24) < profit_point &
              period_return_22_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,24) >= stop_point|
              period_return_22_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_24_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,25) > stop_point &
              lead(Bid_High,25) < profit_point &
              period_return_23_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,25) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,25) > stop_point &
              lead(Bid_High,25) > profit_point &
              period_return_23_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,25) <= stop_point|
              period_return_23_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,25) < stop_point &
              lead(Ask_Low,25) > profit_point &
              period_return_23_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,25) ),

            trade_col == "Short" & lead(Ask_High,25) < stop_point &
              lead(Ask_Low,25) < profit_point &
              period_return_23_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,25) >= stop_point|
              period_return_23_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_25_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,26) > stop_point &
              lead(Bid_High,26) < profit_point &
              period_return_24_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,26) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,26) > stop_point &
              lead(Bid_High,26) > profit_point &
              period_return_24_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,26) <= stop_point|
              period_return_24_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,26) < stop_point &
              lead(Ask_Low,26) > profit_point &
              period_return_24_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,26) ),

            trade_col == "Short" & lead(Ask_High,26) < stop_point &
              lead(Ask_Low,26) < profit_point &
              period_return_24_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,26) >= stop_point|
              period_return_24_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_26_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,27) > stop_point &
              lead(Bid_High,27) < profit_point &
              period_return_25_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,27) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,27) > stop_point &
              lead(Bid_High,27) > profit_point &
              period_return_25_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,27) <= stop_point|
              period_return_25_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,27) < stop_point &
              lead(Ask_Low,27) > profit_point &
              period_return_25_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,27) ),

            trade_col == "Short" & lead(Ask_High,27) < stop_point &
              lead(Ask_Low,27) < profit_point &
              period_return_25_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,27) >= stop_point|
              period_return_25_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_27_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,28) > stop_point &
              lead(Bid_High,28) < profit_point &
              period_return_26_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,28) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,28) > stop_point &
              lead(Bid_High,28) > profit_point &
              period_return_26_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,28) <= stop_point|
              period_return_26_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,28) < stop_point &
              lead(Ask_Low,28) > profit_point &
              period_return_26_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,28) ),

            trade_col == "Short" & lead(Ask_High,28) < stop_point &
              lead(Ask_Low,28) < profit_point &
              period_return_26_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,28) >= stop_point|
              period_return_26_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_28_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,29) > stop_point &
              lead(Bid_High,29) < profit_point &
              period_return_27_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,29) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,29) > stop_point &
              lead(Bid_High,29) > profit_point &
              period_return_27_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,29) <= stop_point|
              period_return_27_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,29) < stop_point &
              lead(Ask_Low,29) > profit_point &
              period_return_27_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,29) ),

            trade_col == "Short" & lead(Ask_High,29) < stop_point &
              lead(Ask_Low,29) < profit_point &
              period_return_27_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,29) >= stop_point|
              period_return_27_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_29_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,30) > stop_point &
              lead(Bid_High,30) < profit_point &
              period_return_28_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,30) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,30) > stop_point &
              lead(Bid_High,30) > profit_point &
              period_return_28_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,30) <= stop_point|
              period_return_28_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,30) < stop_point &
              lead(Ask_Low,30) > profit_point &
              period_return_28_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,30) ),

            trade_col == "Short" & lead(Ask_High,30) < stop_point &
              lead(Ask_Low,30) < profit_point &
              period_return_28_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,30) >= stop_point|
              period_return_28_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_30_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,31) > stop_point &
              lead(Bid_High,31) < profit_point &
              period_return_29_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,31) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,31) > stop_point &
              lead(Bid_High,31) > profit_point &
              period_return_29_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,31) <= stop_point|
              period_return_29_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,31) < stop_point &
              lead(Ask_Low,31) > profit_point &
              period_return_29_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,31) ),

            trade_col == "Short" & lead(Ask_High,31) < stop_point &
              lead(Ask_Low,31) < profit_point &
              period_return_29_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,31) >= stop_point|
              period_return_29_Price == -1*stop_return ~ -1*stop_return
          )

      )

    return(asset_data_with_indicator)

  }

get_multi_sngle_asst_impr_indcator <-
  function(
    indicator_data = indicator_data,
    indicator_asset_list = c("EUR_JPY"),
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
    stop_factor_long = 4,
    profit_factor_long = 12,
    stop_factor_short = 5,
    profit_factor_short = 2,
    risk_dollar_value_long = 5,
    risk_dollar_value_short = 5,
    trade_direction = "Long",
    currency_conversion = currency_conversion,
    asset_infor = asset_infor,
    indicator_data = indicator_data,
    indicator_asset_list = c("EUR_JPY"),
    long_factor = 0,
    indicator_stop = 2,
    indicator_period= 24,
    period_var_long = 16,
    period_var_short = 4
  ) {


    # indicators_joined_raw <-
    #   get_multi_sngle_asst_impr_indcator(
    #   indicator_data = indicator_data,
    #   indicator_asset_list = indicator_asset_list,
    #   indicator_stop = indicator_stop,
    #   indicator_period= indicator_period
    # ) %>%
    #   mutate(Asset = asset_of_interest)

    indicators_joined_raw_long <-
      indicator_data %>%
      ungroup() %>%
      filter(Asset %in% indicator_asset_list, trade_col == "Long",
             stop_factor == indicator_stop, periods_ahead == indicator_period) %>%
      select(Date, Asset, contains("pred")) %>%
      distinct() %>%
      dplyr::select(Date, Asset,
                    logit_combined_pred_long = logit_combined_pred,
                    mean_logit_combined_pred_long = mean_logit_combined_pred,
                    sd_logit_combined_pred_long = sd_logit_combined_pred,
                    averaged_pred_long = averaged_pred,
                    mean_averaged_pred_long = mean_averaged_pred,
                    sd_averaged_pred_long = sd_averaged_pred) %>%
      distinct()

    indicators_joined_raw_short <-
      indicator_data %>%
      ungroup() %>%
      filter(Asset %in% indicator_asset_list, trade_col == "Short",
             stop_factor == indicator_stop, periods_ahead == indicator_period) %>%
      select(Date, Asset, contains("pred")) %>%
      distinct() %>%
      dplyr::select(
        Date, Asset,
        logit_combined_pred_short = logit_combined_pred,
        mean_logit_combined_pred_short = mean_logit_combined_pred,
        sd_logit_combined_pred_short = sd_logit_combined_pred,
        averaged_pred_short = averaged_pred,
        mean_averaged_pred_short = mean_averaged_pred,
        sd_averaged_pred_short = sd_averaged_pred
      ) %>%
      distinct()

    indicators_joined_raw <-
      indicators_joined_raw_long %>%
      left_join(indicators_joined_raw_short) %>%
      filter(
        Asset == asset_of_interest
      ) %>%
      distinct() %>%
      mutate(
        Date = as_datetime(Date)
      )


    Longs <- create_running_profits(
      asset_of_interest = asset_of_interest,
      asset_data = asset_data,
      stop_factor = stop_factor_long,
      profit_factor = profit_factor_long,
      risk_dollar_value = risk_dollar_value_long,
      trade_direction = "Long",
      currency_conversion = currency_conversion,
      asset_infor = asset_infor
    )

    Shorts <- create_running_profits(
      asset_of_interest = asset_of_interest,
      asset_data = asset_data,
      stop_factor = stop_factor_short,
      profit_factor = profit_factor_short,
      risk_dollar_value = risk_dollar_value_short,
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

    # periods_in_long <- seq(1,period_var_long)
    # periods_in_short <- seq(1,period_var_short)
    #
    # cols_for_long <- periods_in_long %>%
    #   map(~
    #         glue::glue("Long_period_return_{.x}_Price" )
    #       ) %>%
    #   unlist()
    #
    # cols_for_short <- periods_in_short %>%
    #   map(~
    #         glue::glue("Short_period_return_{.x}_Price" )
    #   ) %>%
    #   unlist()

    all_results <-
      long_results %>%
      left_join(short_results)


    indicators_joined <-
      indicators_joined_raw %>%
      left_join(all_results) %>%
      filter(!is.na(Long_period_return_1_Price)) %>%
      # rowwise() %>%
      # mutate(
      #   min_point_in_trade_long =
      #     min(c_across(contains("Long_period_return_")), na.rm = T),
      #   max_point_in_trade_long =
      #     max(c_across(contains("Long_period_return_")), na.rm = T),
      #
      #   min_point_in_trade_short =
      #     min(c_across(contains("Short_period_return_")), na.rm = T),
      #   max_point_in_trade_short =
      #     max(c_across(contains("Short_period_return_")), na.rm = T)
      # ) %>%
      ungroup() %>%
      mutate(
        trade_col =
          case_when(
            logit_combined_pred_long >=
              mean_logit_combined_pred_long + sd_logit_combined_pred_long*long_factor &
              averaged_pred_long >=
              mean_averaged_pred_long + sd_averaged_pred_long*long_factor ~ trade_direction
          )
      ) %>%
      dplyr::select(
        Date, Asset, trade_col,
        # min_point_in_trade_long,
        # max_point_in_trade_long,
        # min_point_in_trade_short,
        # max_point_in_trade_short,
        long_return = !!as.name(glue::glue("Long_period_return_{period_var_long}_Price")),
        short_return = !!as.name(glue::glue("Short_period_return_{period_var_short}_Price"))
      ) %>%
      group_by(Date, Asset, trade_col) %>%
      summarise(
        long_return = mean(long_return, na.rm = T),
        short_return = mean(short_return, na.rm = T)
      ) %>%
      ungroup()

    results_summary <- indicators_joined %>%
      filter(trade_col == trade_direction) %>%
      mutate(Total_Return =
               # case_when(
               #   trade_col != "Long" | is.na(trade_col) ~ !!as.name(glue::glue("Short_period_return_{period_var_short}_Price")),
               #   trade_col == "Long" ~ !!as.name(glue::glue("Long_period_return_{period_var_long}_Price")),
               # )
               long_return +
               short_return
      ) %>%
      mutate(
        month_date = floor_date(Date, unit = "month"),
        long_return_cum = cumsum(long_return),
        short_return_cum = cumsum(short_return),
        Total_Return_cum = cumsum(Total_Return)
      )

    subtitle <- glue::glue("{paste(indicator_asset_list, collapse = ',')}")

    return(results_summary)

  }


param_tibble <-
  c(18,15,12) %>%
  # c(18) %>%
  map_dfr(
    ~
      tibble(
        stop_factor_long = c(4,2,6)
        # stop_factor_long = c(6)
      ) %>%
      mutate(
        profit_factor_long = .x,
        profit_factor_long_fast = floor(.x/4),
        profit_factor_long_fastest = floor(.x/7)
      )

  ) %>%
  mutate(
    kk = row_number()
  ) %>%
  split(.$kk, drop = FALSE) %>%
  map_dfr(
    ~
      tibble(
        stop_factor_short = c(3,4,3,2,3,6,6),
        profit_factor_short = c(6,12,1,1,0.5,3,1)
      ) %>%
      bind_cols(.x)
  ) %>%
  mutate(
    kk = row_number()
  ) %>%
  split(.$kk, drop = FALSE) %>%
  map_dfr(
    ~ tibble(
      period_var_long = rep(c(30,24,18,12), 5),
      period_var_short = rep(c(12,8,4,2,1), 4)
    ) %>%
      bind_cols(.x)
  )

distinct_assets <-
  indicator_data %>%
  ungroup() %>%
  distinct(Asset) %>%
  pull(Asset)

# asset_list <-
#   c("EUR_USD", "SPX500_USD", "EU50_EUR", "GBP_USD", "XAG_USD", "USD_CAD", "AUD_USD",
#     "WTICO_USD", "BTC_USD", "XAG_GBP", "GBP_AUD", "UK100_GBP", "AU200_AUD", "US2000_USD",
#     "EUR_GBP", "GBP_CAD", "USD_CAD", "XAG_EUR", "EUR_AUD", "XAG_AUD", "EUR_NZD", "BTC_USD")

asset_list <-
  c("USB10Y_USD", "USD_JPY", "EUR_JPY", "XAG_NZD", "HK33_HKD", "FR40_EUR", "USD_SEK", "USD_SGD")

model_optimisation_store_path <-
  "E:/trade_data/single_asset_advanced_optimisation.db"
model_data_store_db <-
  connect_db(model_optimisation_store_path)

c = 5

for (i in 1216:dim(param_tibble)[1] ) {

  for (j in 1:length(asset_list)) {

    c = c + 1
    asset_of_interest <- asset_list[j] %>% as.character()

    profit_factor_long <- param_tibble$profit_factor_long[i] %>% as.numeric()
    profit_factor_long_fast <- param_tibble$profit_factor_long_fast[i] %>% as.numeric()
    profit_factor_long_fastest <- param_tibble$profit_factor_long_fastest[i] %>% as.numeric()
    stop_factor_long <- param_tibble$stop_factor_long[i] %>% as.numeric()

    profit_factor_short <- param_tibble$profit_factor_short[i] %>% as.numeric()
    stop_factor_short <- param_tibble$stop_factor_short[i] %>% as.numeric()

    period_var_long <- param_tibble$period_var_long[i] %>% as.numeric()
    period_var_short <- param_tibble$period_var_short[i] %>% as.numeric()

    temp <-
      get_return_indicator_joined_data(
        asset_of_interest = asset_of_interest,
        asset_data = Indices_Metals_Bonds,
        stop_factor_long = stop_factor_long,
        profit_factor_long = profit_factor_long,
        stop_factor_short = stop_factor_short,
        profit_factor_short = profit_factor_short,
        risk_dollar_value_long = 4,
        risk_dollar_value_short = 4,
        trade_direction = "Long",
        currency_conversion = currency_conversion,
        asset_infor = asset_infor,
        indicator_data = indicator_data,
        indicator_asset_list = c(asset_of_interest),
        long_factor = 0,
        indicator_stop = 4,
        indicator_period= 48,
        period_var_long = period_var_long,
        period_var_short = period_var_short
      ) %>%
      mutate(
        profit_factor_long = param_tibble$profit_factor_long[i] %>% as.numeric(),
        stop_factor_long = param_tibble$stop_factor_long[i] %>% as.numeric(),

        profit_factor_short = param_tibble$profit_factor_short[i] %>% as.numeric(),
        stop_factor_short = param_tibble$stop_factor_short[i] %>% as.numeric(),

        period_var_long = param_tibble$period_var_long[i] %>% as.numeric(),
        period_var_short = param_tibble$period_var_short[i] %>% as.numeric()
      ) %>%
      mutate(
        Asset = asset_of_interest
      )


    temp_fast <-
      get_return_indicator_joined_data(
        asset_of_interest = asset_of_interest,
        asset_data = Indices_Metals_Bonds,
        stop_factor_long = stop_factor_long,
        profit_factor_long = profit_factor_long_fast,
        stop_factor_short = stop_factor_short,
        profit_factor_short = profit_factor_short,
        risk_dollar_value_long = 4,
        risk_dollar_value_short = 4,
        trade_direction = "Long",
        currency_conversion = currency_conversion,
        asset_infor = asset_infor,
        indicator_data = indicator_data,
        indicator_asset_list = c(asset_of_interest),
        long_factor = 0,
        indicator_stop = 4,
        indicator_period= 48,
        period_var_long = period_var_long,
        period_var_short = period_var_short
      )  %>%
      mutate(
        profit_factor_long_fast = param_tibble$profit_factor_long_fast[i] %>% as.numeric()
      ) %>%
      mutate(
        Asset = asset_of_interest
      ) %>%
      dplyr::select(
        Date, Asset, trade_col,
        long_return_fast = long_return,
        long_return_cum_fast = long_return_cum,
        profit_factor_long_fast
      )


    temp_fastest <-
      get_return_indicator_joined_data(
        asset_of_interest = asset_of_interest,
        asset_data = Indices_Metals_Bonds,
        stop_factor_long = stop_factor_long,
        profit_factor_long = profit_factor_long_fastest,
        stop_factor_short = stop_factor_short,
        profit_factor_short = profit_factor_short,
        risk_dollar_value_long = 4,
        risk_dollar_value_short = 4,
        trade_direction = "Long",
        currency_conversion = currency_conversion,
        asset_infor = asset_infor,
        indicator_data = indicator_data,
        indicator_asset_list = c(asset_of_interest),
        long_factor = 0,
        indicator_stop = 4,
        indicator_period= 48,
        period_var_long = period_var_long,
        period_var_short = period_var_short
      )  %>%
      mutate(
        profit_factor_long_fastest = param_tibble$profit_factor_long_fastest[i] %>% as.numeric()
      ) %>%
      mutate(
        Asset = asset_of_interest
      ) %>%
      dplyr::select(
        Date, Asset, trade_col,
        long_return_fastest = long_return,
        long_return_cum_fastest = long_return_cum,
        profit_factor_long_fastest
      )

    results_summary <-
      temp %>%
      left_join(temp_fast)%>%
      left_join(temp_fastest)

    if(c == 1) {
      # write_table_sql_lite(conn = model_data_store_db,
      #                      .data = results_summary,
      #                      table_name = "single_asset_advanced_optimisation",
      #                      overwrite_true = TRUE)
    } else {

      append_table_sql_lite(conn = model_data_store_db,
                            .data = results_summary,
                            table_name = "single_asset_advanced_optimisation")

    }

    rm(temp)
    gc()

  }

}

model_optimisation_store_path <-
  "E:/trade_data/single_asset_advanced_optimisation.db"
model_data_store_db <-
  connect_db(model_optimisation_store_path)

asset_list <-
  c("EUR_USD", "SPX500_USD", "EU50_EUR", "GBP_USD", "XAG_USD", "USD_CAD", "AUD_USD",
    "WTICO_USD", "BTC_USD", "XAG_GBP", "GBP_AUD", "UK100_GBP", "AU200_AUD", "US2000_USD",
    "EUR_GBP", "GBP_CAD", "USD_CAD", "XAG_EUR", "EUR_AUD", "XAG_AUD", "EUR_NZD", "BTC_USD",
    "USB10Y_USD", "USD_JPY", "EUR_JPY", "XAG_NZD", "HK33_HKD", "FR40_EUR", "USD_SEK", "USD_SGD")

oo = 0

for (k in 1:length(asset_list) ) {

  results_summary <-
    DBI::dbGetQuery(conn = model_data_store_db,
                    statement =
                    glue::glue("SELECT * FROM single_asset_advanced_optimisation
                                WHERE Asset = '{asset_list[k]}'") ) %>%
    mutate(
      Date = as_datetime(Date)
    ) %>%
    mutate(
      multi_win =
        case_when(
          long_return > 0 & short_return> 0 ~ 1,
          TRUE ~ 0
        ),
      multi_loss =
        case_when(
          long_return < 0 & short_return < 0 ~ 1,
          TRUE ~ 0
        )
    ) %>%
    ungroup() %>%
    group_by(
      stop_factor_short , profit_factor_short ,
      period_var_short , profit_factor_long , stop_factor_long,
      period_var_long, profit_factor_long_fast, profit_factor_long_fastest,
      trade_col, Asset
    ) %>%
    arrange(Date, .by_group = TRUE) %>%
    group_by(
      stop_factor_short , profit_factor_short ,
      period_var_short , profit_factor_long , stop_factor_long,
      period_var_long, profit_factor_long_fast, profit_factor_long_fastest,
      trade_col, Asset
    ) %>%
    mutate(long_only_cum = cumsum(long_return),
           long_only_cum_fast = cumsum(long_return_fast),
           short_return_cum = cumsum(short_return),
           Total_Return = long_return + short_return + long_return_fast + long_return_fastest,
           Total_Return_cum = cumsum(Total_Return),
           period_run_25_total = Total_Return_cum - lag(Total_Return_cum, 25),
           period_run_25_long = long_only_cum - lag(long_only_cum, 25),
           period_run_25_short = short_return_cum - lag(short_return_cum, 25),
           period_run_25_long_fast = long_only_cum_fast - lag(long_only_cum_fast, 25),

           win_loss = ifelse(Total_Return > 3.5,1, 0)
           ) %>%
    group_by(
      stop_factor_short , profit_factor_short ,
      period_var_short , profit_factor_long , stop_factor_long,
      period_var_long, profit_factor_long_fast, profit_factor_long_fastest,
      trade_col, Asset
    ) %>%
    summarise(
      Total_Return = sum(Total_Return, na.rm = T),
      Total_Return_worst_run = quantile(period_run_25_total, 0.25 ,na.rm  = T),
      total_trades = n(),
      return_25 = quantile(Total_Return_cum, 0.25, na.rm = T),
      return_75 = quantile(Total_Return_cum, 0.75, na.rm = T),
      multi_win = sum(multi_win),
      multi_loss = sum(multi_loss),
      win_loss_3_dollars = sum(win_loss),

      long_return = sum(long_return),
      long_return_25 = quantile(long_only_cum, 0.25, na.rm = T),
      long_return_75 = quantile(long_only_cum, 0.75, na.rm = T),
      worst_long_run = quantile(period_run_25_long, 0.25 ,na.rm = T),

      short_return = sum(short_return),
      short_return_25 = quantile(short_return_cum, 0.25, na.rm = T),
      short_return_75 = quantile(short_return_cum, 0.75, na.rm = T),
      worst_short_run = quantile(period_run_25_short, 0.25, na.rm = T),

      long_return_fast = sum(long_return_fast, na.rm = T),
      long_return_fastest = sum(long_return_fastest, na.rm = T),
      worst_long_run_fast = quantile(period_run_25_long_fast, 0.25 ,na.rm = T),
    ) %>%
    ungroup() %>%
    mutate(
      Total_Return_fast = long_return_fast + short_return,
      Total_Return_fastest = long_return_fastest + short_return
      # Total_summed_return =
      #   Total_Return_fast +
      #   Total_Return_fastest +
      #   long_return +
      #   short_return
    ) %>%
    mutate(
      multi_win_perc =multi_win/total_trades,
      multi_loss_perc =multi_loss/total_trades
    ) %>%
    mutate(
      stop_factor_short_2 = stop_factor_short^2,
      profit_factor_short_2 = profit_factor_short^2,
      period_var_short_2 = period_var_short^2,
      profit_factor_long_2 = profit_factor_long^2,
      stop_factor_long_2 = stop_factor_long^2,
      period_var_long_2 = period_var_long^2
    )

  oo = oo + 1

  if(oo == 1) {
    write_table_sql_lite(conn = model_data_store_db,
                         .data = results_summary,
                         table_name = "summary_for_reg",
                         overwrite_true = TRUE)
  } else {

    append_table_sql_lite(conn = model_data_store_db,
                          .data = results_summary,
                          table_name = "summary_for_reg")

  }

  rm(all_results_dfr)
  gc()


}

model_optimisation_store_path <-
  "E:/trade_data/single_asset_advanced_optimisation.db"
model_data_store_db <-
  connect_db(model_optimisation_store_path)

summary_results <-
  DBI::dbGetQuery(conn = model_data_store_db,
                  statement =
                    glue::glue("SELECT * FROM summary_for_reg") )

DBI::dbDisconnect(model_data_store_db)

get_best_trade_setup_sa <-
  function(
    model_optimisation_store_path =
      "E:/trade_data/single_asset_advanced_optimisation.db",
    table_to_extract = "summary_for_reg"
  ) {

    model_data_store_db <-
      connect_db(model_optimisation_store_path)

    summary_results <-
      DBI::dbGetQuery(conn = model_data_store_db,
                      statement =
                        glue::glue("SELECT * FROM {table_to_extract}") )

    DBI::dbDisconnect(model_data_store_db)

    all_assets <-
      summary_results %>%
      pull(Asset) %>%
      unique()

    best <-
      summary_results %>%
      group_by(Asset) %>%
      # slice_max(short_return, n = 1) %>%
      # group_by(Asset) %>%
      # slice_max(Total_Return_worst_run) %>%
      group_by(Asset) %>%
      slice_max(Total_Return) %>%
      distinct()

    best_params_average <-
      best %>%
      ungroup() %>%
      summarise(
        stop_factor_short = mean(stop_factor_short, na.rm = T),
        profit_factor_short = mean(profit_factor_short, na.rm = T),
        period_var_short = mean(period_var_short, na.rm = T),
        profit_factor_long = mean(profit_factor_long, na.rm = T),
        stop_factor_long = mean(stop_factor_long ,na.rm = T),
        profit_factor_long_fast = mean(profit_factor_long_fast, na.rm = T),
        profit_factor_long_fastest = mean(profit_factor_long_fastest, na.rm = T),
        period_var_long = mean(period_var_long, na.rm = T),
        Total_Return = mean(Total_Return, na.rm = T)
      )

    param_comp_lm <-
      lm(data =
           summary_results %>%
           mutate(
             profit_factor_long_fast_2 = profit_factor_long_fast^2,
             profit_factor_long_fastest_2 = profit_factor_long_fastest^2
           ),
         formula = Total_Return ~
           stop_factor_short +
           stop_factor_short_2 +
           profit_factor_short +
           profit_factor_short_2 +
           period_var_short +
           period_var_short_2 +
           profit_factor_long +
           profit_factor_long_2 +
           stop_factor_long +
           stop_factor_long_2 +
           profit_factor_long_fast +
           profit_factor_long_fastest +
           period_var_long +
           Asset
      )

    summary(param_comp_lm)

    param_tibble <-
      c(18,15,12,9,6,2) %>%
      map_dfr(
        ~
          tibble(
            stop_factor_long = c(4,2,6, 8)
          ) %>%
          mutate(
            profit_factor_long = .x,
            profit_factor_long_fast = round(.x/2),
            profit_factor_long_fastest = (.x/3)
          )

      ) %>%
      mutate(
        kk = row_number()
      ) %>%
      split(.$kk, drop = FALSE) %>%
      map_dfr(
        ~
          tibble(
            stop_factor_short = c(3,4,3,2,3,6,6),
            profit_factor_short = c(6,12,1,1,0.5,3,1)
          ) %>%
          bind_cols(.x)
      ) %>%
      mutate(
        kk = row_number()
      ) %>%
      split(.$kk, drop = FALSE) %>%
      map_dfr(
        ~ tibble(
          period_var_long = rep(c(30,24,18,12), 6),
          period_var_short = rep(c(14,12,8,6,4,2), 4)
        ) %>%
          bind_cols(.x)
      )

    param_tibble <-
      all_assets %>%
      map_dfr(
        ~
          param_tibble %>%
          mutate(Asset = .x)
      )


    predicted_outcomes <-
      predict(object = param_comp_lm,
              newdata = param_tibble %>%
                mutate(
                  stop_factor_short_2 = stop_factor_short^2,
                  profit_factor_short_2 = profit_factor_short^2,
                  period_var_short_2 = period_var_short^2,
                  profit_factor_long_2 = profit_factor_long^2,
                  stop_factor_long_2 = stop_factor_long^2
                )
      )

    predicted_outcomes <-
      param_tibble %>%
      mutate(
        stop_factor_short_2 = stop_factor_short^2,
        profit_factor_short_2 = profit_factor_short^2,
        period_var_short_2 = period_var_short^2,
        profit_factor_long_2 = profit_factor_long^2,
        stop_factor_long_2 = stop_factor_long^2
      ) %>%
      mutate(
        predicted_values = predicted_outcomes
      )

    best_prediced_outcome <-
      predicted_outcomes %>%
      group_by(Asset) %>%
      slice_max(predicted_values) %>%
      ungroup()

    best_best_prediced_outcome2 <-
      best_prediced_outcome %>%
      dplyr::select(
        stop_factor_short ,
        profit_factor_short ,
        period_var_short ,
        profit_factor_long,
        stop_factor_long,
        profit_factor_long_fast,
        profit_factor_long_fastest,
        period_var_long,
        Total_return = predicted_values
      ) %>%
      distinct()

    final_results <-
      best %>%
      dplyr::select(
        Asset,
        stop_factor_short ,
        profit_factor_short ,
        period_var_short ,
        profit_factor_long,
        stop_factor_long,
        profit_factor_long_fast,
        profit_factor_long_fastest,
        period_var_long,
        Total_Return
      ) %>%
      split(.$Asset, drop = FALSE) %>%
      map_dfr(
        ~ .x %>%
          ungroup() %>%
          bind_rows(
            best_best_prediced_outcome2 %>%
              ungroup()
          ) %>%
          ungroup() %>%
          summarise(
            Asset = Asset[1],
            stop_factor_short = mean(stop_factor_short, na.rm = T),
            profit_factor_short = mean(profit_factor_short, na.rm = T),
            period_var_short = mean(period_var_short, na.rm = T),
            profit_factor_long = mean(profit_factor_long, na.rm = T),
            stop_factor_long = mean(stop_factor_long ,na.rm = T),
            profit_factor_long_fast = mean(profit_factor_long_fast, na.rm = T),
            profit_factor_long_fastest = mean(profit_factor_long_fastest, na.rm = T),
            period_var_long = mean(period_var_long, na.rm = T),
            Total_Return = mean(Total_Return, na.rm = T)
          )
      )

    final_results_real <-
      best %>%
      ungroup() %>%
      summarise(
        Asset = Asset[1],
        stop_factor_short = mean(stop_factor_short, na.rm = T),
        profit_factor_short = mean(profit_factor_short, na.rm = T),
        period_var_short = mean(period_var_short, na.rm = T),
        profit_factor_long = mean(profit_factor_long, na.rm = T),
        stop_factor_long = mean(stop_factor_long ,na.rm = T),
        profit_factor_long_fast = mean(profit_factor_long_fast, na.rm = T),
        profit_factor_long_fastest = mean(profit_factor_long_fastest, na.rm = T),
        period_var_long = mean(period_var_long, na.rm = T)
      )


    return(final_results_real)

  }
