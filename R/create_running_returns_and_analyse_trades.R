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
    asset_of_interest = "XAU_USD",
    asset_data = Indices_Metals_Bonds,
    stop_factor = 5,
    profit_factor = 30,
    risk_dollar_value = 10,
    trade_direction = "Long",
    currency_conversion = currency_conversion,
    asset_infor = asset_infor
  ) {

    mean_values_by_asset_for_loop_H1_ask <-
      wrangle_asset_data(
        asset_data_daily_raw = asset_data[[1]],
        summarise_means = TRUE
      ) %>%
      dplyr::select(Asset,
                    mean_movement = mean_daily,
                    sd_movement = sd_daily) %>%
      filter(Asset == asset_of_interest)

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
      left_join(mean_values_by_asset_for_loop_H1_ask) %>%
      mutate(

        # mean_movement = mean(Ask_Price - lag(Ask_Price), na.rm = T),
        # sd_movement = sd(Ask_Price - lag(Ask_Price), na.rm = T),
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
            str_detect(Asset,"ZAR|CNH") ~ (risk_dollar_value/stop_value)*adjusted_conversion,
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

            (trade_col == "Long" & lead(Bid_Low,2) <= stop_point)|
              (trade_col == "Long" & lead(Bid_Low,1) <= stop_point) ~ -1*stop_return,

            trade_col == "Long" & lead(Bid_Low,2) > stop_point &
              lead(Bid_High,2) < profit_point ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price, 2) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,2) > stop_point &
              lead(Bid_High,2) > profit_point  ~ profit_return,

            trade_col == "Short" & lead(Ask_High,2) >= stop_point|
              trade_col == "Short" & lead(Ask_High,1) >= stop_point ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,2) < stop_point &
              lead(Ask_Low,2) > profit_point ~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,2) ),

            trade_col == "Short" & lead(Ask_High,2) < stop_point &
              lead(Ask_Low,2) < profit_point ~ profit_return
          ),

        period_return_2_Price =
          case_when(

            trade_col == "Long" &
              (lead(Bid_Low,3) <= stop_point|period_return_1_Price<= -1*stop_return) &
              period_return_1_Price < profit_return ~ -1*stop_return,

            trade_col == "Long" & lead(Bid_Low,3) > stop_point &
              lead(Bid_High,3) < profit_point &
              period_return_1_Price > -1*stop_return &
              period_return_1_Price < profit_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price, 3) - lead(Ask_Price)) ),

            trade_col == "Long" &
              ((lead(Bid_Low,3) > stop_point &
                  lead(Bid_High,3) > profit_point &
                  period_return_1_Price > -1*stop_return)|
                 period_return_1_Price >= profit_return) ~ profit_return,

            trade_col == "Short" & lead(Ask_High,3) >= stop_point|
              period_return_1_Price <= -1*stop_return ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,3) < stop_point &
              lead(Ask_Low,3) > profit_point &
              period_return_1_Price > -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,3) ),

            trade_col == "Short" & lead(Ask_High,3) < stop_point &
              lead(Ask_Low,3) < profit_point &
              period_return_1_Price > -1*stop_return~ profit_return
          ),

        period_return_3_Price =
          case_when(
            trade_col == "Long" &
              (lead(Bid_Low,4) <= stop_point|period_return_2_Price<= -1*stop_return) &
              period_return_2_Price < profit_return ~ -1*stop_return,

            trade_col == "Long" & lead(Bid_Low,4) > stop_point &
              lead(Bid_High,4) < profit_point &
              period_return_2_Price > -1*stop_return &
              period_return_2_Price < profit_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,4) - lead(Ask_Price)) ),

            trade_col == "Long" &
              ((lead(Bid_Low,4) > stop_point &
                  lead(Bid_High,4) > profit_point &
                  period_return_2_Price > -1*stop_return)|
                 period_return_2_Price >= profit_return) ~ profit_return,

            trade_col == "Short" & lead(Ask_High,4) >= stop_point|
              period_return_2_Price <= -1*stop_return ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,4) < stop_point &
              lead(Ask_Low,4) > profit_point &
              period_return_2_Price > -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,4) ),

            trade_col == "Short" & lead(Ask_High,4) < stop_point &
              lead(Ask_Low,4) < profit_point &
              period_return_2_Price > -1*stop_return~ profit_return
          ),

        period_return_4_Price =
          case_when(

            trade_col == "Long" &
              (lead(Bid_Low,5) <= stop_point|period_return_3_Price<= -1*stop_return) &
              period_return_3_Price < profit_return ~ -1*stop_return,

            trade_col == "Long" & lead(Bid_Low,5) > stop_point &
              lead(Bid_High,5) < profit_point &
              period_return_3_Price > -1*stop_return &
              period_return_3_Price < profit_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,5) - lead(Ask_Price)) ),

            trade_col == "Long" &
              ((lead(Bid_Low,5) > stop_point &
                  lead(Bid_High,5) > profit_point &
                  period_return_3_Price > -1*stop_return)|
                 period_return_3_Price >= profit_return) ~ profit_return,

            trade_col == "Short" & lead(Ask_High,5) >= stop_point|
              period_return_3_Price <= -1*stop_return ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,5) < stop_point &
              lead(Ask_Low,5) > profit_point &
              period_return_3_Price > -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,5) ),

            trade_col == "Short" & lead(Ask_High,5) < stop_point &
              lead(Ask_Low,5) < profit_point &
              period_return_3_Price > -1*stop_return~ profit_return
          ),

        period_return_5_Price =
          case_when(

            trade_col == "Long" &
              (lead(Bid_Low,6) <= stop_point|period_return_4_Price<= -1*stop_return) &
              period_return_4_Price < profit_return ~ -1*stop_return,

            trade_col == "Long" & lead(Bid_Low,6) > stop_point &
              lead(Bid_High,6) < profit_point &
              period_return_4_Price > -1*stop_return &
              period_return_4_Price < profit_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,6) - lead(Ask_Price)) ),

            trade_col == "Long" &
              ((lead(Bid_Low,6) > stop_point &
                  lead(Bid_High,6) > profit_point &
                  period_return_4_Price > -1*stop_return)|
                 period_return_4_Price >= profit_return) ~ profit_return,

            trade_col == "Short" & lead(Ask_High,6) >= stop_point|
              period_return_4_Price <= -1*stop_return ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,6) < stop_point &
              lead(Ask_Low,6) > profit_point &
              period_return_4_Price > -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,6) ),

            trade_col == "Short" & lead(Ask_High,6) < stop_point &
              lead(Ask_Low,6) < profit_point &
              period_return_4_Price > -1*stop_return~ profit_return
          ),

        period_return_6_Price =
          case_when(

            trade_col == "Long" &
              (lead(Bid_Low,7) <= stop_point|period_return_5_Price<= -1*stop_return) &
              period_return_5_Price < profit_return ~ -1*stop_return,

            trade_col == "Long" & lead(Bid_Low,7) > stop_point &
              lead(Bid_High,7) < profit_point &
              period_return_5_Price > -1*stop_return &
              period_return_5_Price < profit_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,7) - lead(Ask_Price)) ),

            trade_col == "Long" &
              ((lead(Bid_Low,7) > stop_point &
                  lead(Bid_High,7) > profit_point &
                  period_return_5_Price > -1*stop_return)|
                 period_return_5_Price >= profit_return) ~ profit_return,

            trade_col == "Short" & lead(Ask_High,7) >= stop_point|
              period_return_5_Price <= -1*stop_return ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,7) < stop_point &
              lead(Ask_Low,7) > profit_point &
              period_return_5_Price > -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,7) ),

            trade_col == "Short" & lead(Ask_High,7) < stop_point &
              lead(Ask_Low,7) < profit_point &
              period_return_5_Price > -1*stop_return~ profit_return
          ),

        period_return_7_Price =
          case_when(
            trade_col == "Long" &
              (lead(Bid_Low,8) <= stop_point|period_return_6_Price<= -1*stop_return) &
              period_return_6_Price < profit_return ~ -1*stop_return,

            trade_col == "Long" & lead(Bid_Low,8) > stop_point &
              lead(Bid_High,8) < profit_point &
              period_return_6_Price > -1*stop_return &
              period_return_6_Price < profit_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,8) - lead(Ask_Price)) ),

            trade_col == "Long" &
              ((lead(Bid_Low,8) > stop_point &
                  lead(Bid_High,8) > profit_point &
                  period_return_6_Price > -1*stop_return)|
                 period_return_6_Price >= profit_return) ~ profit_return,

            trade_col == "Short" & lead(Ask_High,8) >= stop_point|
              period_return_6_Price <= -1*stop_return ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,8) < stop_point &
              lead(Ask_Low,8) > profit_point &
              period_return_6_Price > -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,8) ),

            trade_col == "Short" & lead(Ask_High,8) < stop_point &
              lead(Ask_Low,8) < profit_point &
              period_return_6_Price > -1*stop_return~ profit_return
          ),

        period_return_8_Price =
          case_when(

            trade_col == "Long" &
              (lead(Bid_Low,9) <= stop_point|period_return_7_Price<= -1*stop_return) &
              period_return_7_Price < profit_return ~ -1*stop_return,

            trade_col == "Long" & lead(Bid_Low,9) > stop_point &
              lead(Bid_High,9) < profit_point &
              period_return_7_Price > -1*stop_return &
              period_return_7_Price < profit_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,9) - lead(Ask_Price)) ),

            trade_col == "Long" &
              ((lead(Bid_Low,9) > stop_point &
                  lead(Bid_High,9) > profit_point &
                  period_return_7_Price > -1*stop_return)|
                 period_return_7_Price >= profit_return) ~ profit_return,

            trade_col == "Short" & lead(Ask_High,9) >= stop_point|
              period_return_7_Price <= -1*stop_return ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,9) < stop_point &
              lead(Ask_Low,9) > profit_point &
              period_return_7_Price > -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,9) ),

            trade_col == "Short" & lead(Ask_High,9) < stop_point &
              lead(Ask_Low,9) < profit_point &
              period_return_7_Price > -1*stop_return~ profit_return
          ),


        period_return_9_Price =
          case_when(

            trade_col == "Long" &
              (lead(Bid_Low,10) <= stop_point|period_return_8_Price<= -1*stop_return) &
              period_return_8_Price < profit_return ~ -1*stop_return,

            trade_col == "Long" & lead(Bid_Low,10) > stop_point &
              lead(Bid_High,10) < profit_point &
              period_return_8_Price > -1*stop_return &
              period_return_8_Price < profit_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,10) - lead(Ask_Price)) ),

            trade_col == "Long" &
              ((lead(Bid_Low,10) > stop_point &
                  lead(Bid_High,10) > profit_point &
                  period_return_8_Price > -1*stop_return)|
                 period_return_8_Price >= profit_return) ~ profit_return,

            trade_col == "Short" & lead(Ask_High,10) >= stop_point|
              period_return_8_Price <= -1*stop_return ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,10) < stop_point &
              lead(Ask_Low,10) > profit_point &
              period_return_8_Price > -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,10) ),

            trade_col == "Short" & lead(Ask_High,10) < stop_point &
              lead(Ask_Low,10) < profit_point &
              period_return_8_Price > -1*stop_return~ profit_return
          ),


        period_return_10_Price =
          case_when(

            trade_col == "Long" &
              (lead(Bid_Low,11) <= stop_point|period_return_9_Price<= -1*stop_return) &
              period_return_9_Price < profit_return ~ -1*stop_return,

            trade_col == "Long" & lead(Bid_Low,11) > stop_point &
              lead(Bid_High,11) < profit_point &
              period_return_9_Price > -1*stop_return &
              period_return_9_Price < profit_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,11) - lead(Ask_Price)) ),

            trade_col == "Long" &
              ((lead(Bid_Low,11) > stop_point &
                  lead(Bid_High,11) > profit_point &
                  period_return_9_Price > -1*stop_return)|
                 period_return_9_Price >= profit_return) ~ profit_return,

            trade_col == "Short" & lead(Ask_High,11) >= stop_point|
              period_return_9_Price <= -1*stop_return ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,11) < stop_point &
              lead(Ask_Low,11) > profit_point &
              period_return_9_Price > -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,11) ),

            trade_col == "Short" & lead(Ask_High,11) < stop_point &
              lead(Ask_Low,11) < profit_point &
              period_return_9_Price > -1*stop_return~ profit_return
          ),

        period_return_11_Price =
          case_when(
            trade_col == "Long" &
              (lead(Bid_Low,12) <= stop_point|period_return_10_Price<= -1*stop_return) &
              period_return_10_Price < profit_return ~ -1*stop_return,

            trade_col == "Long" & lead(Bid_Low,12) > stop_point &
              lead(Bid_High,12) < profit_point &
              period_return_10_Price > -1*stop_return &
              period_return_10_Price < profit_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,12) - lead(Ask_Price)) ),

            trade_col == "Long" &
              ((lead(Bid_Low,12) > stop_point &
                  lead(Bid_High,12) > profit_point &
                  period_return_10_Price > -1*stop_return)|
                 period_return_10_Price >= profit_return) ~ profit_return,

            trade_col == "Short" & lead(Ask_High,12) >= stop_point|
              period_return_10_Price <= -1*stop_return ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,12) < stop_point &
              lead(Ask_Low,12) > profit_point &
              period_return_10_Price > -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,12) ),

            trade_col == "Short" & lead(Ask_High,12) < stop_point &
              lead(Ask_Low,12) < profit_point &
              period_return_10_Price > -1*stop_return~ profit_return
          ),

        period_return_12_Price =
          case_when(
            trade_col == "Long" &
              (lead(Bid_Low,13) <= stop_point|period_return_11_Price<= -1*stop_return) &
              period_return_11_Price < profit_return ~ -1*stop_return,

            trade_col == "Long" & lead(Bid_Low,13) > stop_point &
              lead(Bid_High,13) < profit_point &
              period_return_11_Price > -1*stop_return &
              period_return_11_Price < profit_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,13) - lead(Ask_Price)) ),

            trade_col == "Long" &
              ((lead(Bid_Low,13) > stop_point &
                  lead(Bid_High,13) > profit_point &
                  period_return_11_Price > -1*stop_return)|
                 period_return_11_Price >= profit_return) ~ profit_return,

            trade_col == "Short" & lead(Ask_High,13) >= stop_point|
              period_return_11_Price <= -1*stop_return ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,13) < stop_point &
              lead(Ask_Low,13) > profit_point &
              period_return_11_Price > -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,13) ),

            trade_col == "Short" & lead(Ask_High,13) < stop_point &
              lead(Ask_Low,13) < profit_point &
              period_return_11_Price > -1*stop_return~ profit_return
          ),

        period_return_13_Price =
          case_when(

            trade_col == "Long" &
              (lead(Bid_Low,14) <= stop_point|period_return_12_Price<= -1*stop_return) &
              period_return_12_Price < profit_return ~ -1*stop_return,

            trade_col == "Long" & lead(Bid_Low,14) > stop_point &
              lead(Bid_High,14) < profit_point &
              period_return_12_Price > -1*stop_return &
              period_return_12_Price < profit_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,14) - lead(Ask_Price)) ),

            trade_col == "Long" &
              ((lead(Bid_Low,14) > stop_point &
                  lead(Bid_High,14) > profit_point &
                  period_return_12_Price > -1*stop_return)|
                 period_return_12_Price >= profit_return) ~ profit_return,

            trade_col == "Short" & lead(Ask_High,14) >= stop_point|
              period_return_12_Price <= -1*stop_return ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,14) < stop_point &
              lead(Ask_Low,14) > profit_point &
              period_return_12_Price > -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,14) ),

            trade_col == "Short" & lead(Ask_High,14) < stop_point &
              lead(Ask_Low,14) < profit_point &
              period_return_12_Price > -1*stop_return~ profit_return
          ),

        period_return_14_Price =
          case_when(

            trade_col == "Long" &
              (lead(Bid_Low,15) <= stop_point|period_return_13_Price<= -1*stop_return) &
              period_return_13_Price < profit_return ~ -1*stop_return,

            trade_col == "Long" & lead(Bid_Low,15) > stop_point &
              lead(Bid_High,15) < profit_point &
              period_return_13_Price > -1*stop_return &
              period_return_13_Price < profit_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,15) - lead(Ask_Price)) ),

            trade_col == "Long" &
              ((lead(Bid_Low,15) > stop_point &
                  lead(Bid_High,15) > profit_point &
                  period_return_13_Price > -1*stop_return)|
                 period_return_13_Price >= profit_return) ~ profit_return,

            trade_col == "Short" & lead(Ask_High,15) >= stop_point|
              period_return_13_Price <= -1*stop_return ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,15) < stop_point &
              lead(Ask_Low,15) > profit_point &
              period_return_13_Price > -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,15) ),

            trade_col == "Short" & lead(Ask_High,15) < stop_point &
              lead(Ask_Low,15) < profit_point &
              period_return_13_Price > -1*stop_return~ profit_return
          ),

        period_return_15_Price =
          case_when(

            trade_col == "Long" &
              (lead(Bid_Low,16) <= stop_point|period_return_14_Price<= -1*stop_return) &
              period_return_14_Price < profit_return ~ -1*stop_return,

            trade_col == "Long" & lead(Bid_Low,16) > stop_point &
              lead(Bid_High,16) < profit_point &
              period_return_14_Price > -1*stop_return &
              period_return_14_Price < profit_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,16) - lead(Ask_Price)) ),

            trade_col == "Long" &
              ((lead(Bid_Low,16) > stop_point &
                  lead(Bid_High,16) > profit_point &
                  period_return_14_Price > -1*stop_return)|
                 period_return_14_Price >= profit_return) ~ profit_return,

            trade_col == "Short" & lead(Ask_High,16) >= stop_point|
              period_return_14_Price <= -1*stop_return ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,16) < stop_point &
              lead(Ask_Low,16) > profit_point &
              period_return_14_Price > -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,16) ),

            trade_col == "Short" & lead(Ask_High,16) < stop_point &
              lead(Ask_Low,16) < profit_point &
              period_return_14_Price > -1*stop_return~ profit_return
          ) ,

        period_return_16_Price =
          case_when(
            trade_col == "Long" &
              (lead(Bid_Low,17) <= stop_point|period_return_15_Price<= -1*stop_return) &
              period_return_15_Price < profit_return ~ -1*stop_return,

            trade_col == "Long" & lead(Bid_Low,17) > stop_point &
              lead(Bid_High,17) < profit_point &
              period_return_15_Price > -1*stop_return &
              period_return_15_Price < profit_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,17) - lead(Ask_Price)) ),

            trade_col == "Long" &
              ((lead(Bid_Low,17) > stop_point &
                  lead(Bid_High,17) > profit_point &
                  period_return_15_Price > -1*stop_return)|
                 period_return_15_Price >= profit_return) ~ profit_return,

            trade_col == "Short" & lead(Ask_High,17) >= stop_point|
              period_return_15_Price <= -1*stop_return ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,17) < stop_point &
              lead(Ask_Low,17) > profit_point &
              period_return_15_Price > -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,17) ),

            trade_col == "Short" & lead(Ask_High,17) < stop_point &
              lead(Ask_Low,17) < profit_point &
              period_return_15_Price > -1*stop_return~ profit_return
          ),

        period_return_17_Price =
          case_when(

            trade_col == "Long" &
              (lead(Bid_Low,18) <= stop_point|period_return_16_Price<= -1*stop_return) &
              period_return_16_Price < profit_return ~ -1*stop_return,

            trade_col == "Long" & lead(Bid_Low,18) > stop_point &
              lead(Bid_High,18) < profit_point &
              period_return_16_Price > -1*stop_return &
              period_return_16_Price < profit_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,18) - lead(Ask_Price)) ),

            trade_col == "Long" &
              ((lead(Bid_Low,18) > stop_point &
                  lead(Bid_High,18) > profit_point &
                  period_return_16_Price > -1*stop_return)|
                 period_return_16_Price >= profit_return) ~ profit_return,

            trade_col == "Short" & lead(Ask_High,18) >= stop_point|
              period_return_16_Price <= -1*stop_return ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,18) < stop_point &
              lead(Ask_Low,18) > profit_point &
              period_return_16_Price > -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,18) ),

            trade_col == "Short" & lead(Ask_High,18) < stop_point &
              lead(Ask_Low,18) < profit_point &
              period_return_16_Price > -1*stop_return~ profit_return
          ),

        period_return_18_Price =
          case_when(

            trade_col == "Long" &
              (lead(Bid_Low,19) <= stop_point|period_return_17_Price<= -1*stop_return) &
              period_return_17_Price < profit_return ~ -1*stop_return,

            trade_col == "Long" & lead(Bid_Low,19) > stop_point &
              lead(Bid_High,19) < profit_point &
              period_return_17_Price > -1*stop_return &
              period_return_17_Price < profit_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,19) - lead(Ask_Price)) ),

            trade_col == "Long" &
              ((lead(Bid_Low,19) > stop_point &
                  lead(Bid_High,19) > profit_point &
                  period_return_17_Price > -1*stop_return)|
                 period_return_17_Price >= profit_return) ~ profit_return,

            trade_col == "Short" & lead(Ask_High,19) >= stop_point|
              period_return_17_Price <= -1*stop_return ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,19) < stop_point &
              lead(Ask_Low,19) > profit_point &
              period_return_17_Price > -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,19) ),

            trade_col == "Short" & lead(Ask_High,19) < stop_point &
              lead(Ask_Low,19) < profit_point &
              period_return_17_Price > -1*stop_return~ profit_return
          ),

        period_return_19_Price =
          case_when(

            trade_col == "Long" &
              (lead(Bid_Low,20) <= stop_point|period_return_18_Price<= -1*stop_return) &
              period_return_18_Price < profit_return ~ -1*stop_return,

            trade_col == "Long" & lead(Bid_Low,20) > stop_point &
              lead(Bid_High,20) < profit_point &
              period_return_18_Price > -1*stop_return &
              period_return_18_Price < profit_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,20) - lead(Ask_Price)) ),

            trade_col == "Long" &
              ((lead(Bid_Low,20) > stop_point &
                  lead(Bid_High,20) > profit_point &
                  period_return_18_Price > -1*stop_return)|
                 period_return_18_Price >= profit_return) ~ profit_return,

            trade_col == "Short" & lead(Ask_High,20) >= stop_point|
              period_return_18_Price <= -1*stop_return ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,20) < stop_point &
              lead(Ask_Low,20) > profit_point &
              period_return_18_Price > -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,20) ),

            trade_col == "Short" & lead(Ask_High,20) < stop_point &
              lead(Ask_Low,20) < profit_point &
              period_return_18_Price > -1*stop_return~ profit_return
          ),

        period_return_20_Price =
          case_when(

            trade_col == "Long" &
              (lead(Bid_Low,21) <= stop_point|period_return_19_Price<= -1*stop_return) &
              period_return_19_Price < profit_return ~ -1*stop_return,

            trade_col == "Long" & lead(Bid_Low,21) > stop_point &
              lead(Bid_High,21) < profit_point &
              period_return_19_Price > -1*stop_return &
              period_return_19_Price < profit_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,21) - lead(Ask_Price)) ),

            trade_col == "Long" &
              ((lead(Bid_Low,21) > stop_point &
                  lead(Bid_High,21) > profit_point &
                  period_return_19_Price > -1*stop_return)|
                 period_return_19_Price >= profit_return) ~ profit_return,

            trade_col == "Short" & lead(Ask_High,21) >= stop_point|
              period_return_19_Price <= -1*stop_return ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,21) < stop_point &
              lead(Ask_Low,21) > profit_point &
              period_return_19_Price > -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,21) ),

            trade_col == "Short" & lead(Ask_High,21) < stop_point &
              lead(Ask_Low,21) < profit_point &
              period_return_19_Price > -1*stop_return~ profit_return
          ),

        period_return_21_Price =
          case_when(

            trade_col == "Long" &
              (lead(Bid_Low,22) <= stop_point|period_return_20_Price<= -1*stop_return) &
              period_return_20_Price < profit_return ~ -1*stop_return,

            trade_col == "Long" & lead(Bid_Low,22) > stop_point &
              lead(Bid_High,22) < profit_point &
              period_return_20_Price > -1*stop_return &
              period_return_20_Price < profit_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,22) - lead(Ask_Price)) ),

            trade_col == "Long" &
              ((lead(Bid_Low,22) > stop_point &
                  lead(Bid_High,22) > profit_point &
                  period_return_20_Price > -1*stop_return)|
                 period_return_20_Price >= profit_return) ~ profit_return,

            trade_col == "Short" & lead(Ask_High,22) >= stop_point|
              period_return_20_Price <= -1*stop_return ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,22) < stop_point &
              lead(Ask_Low,22) > profit_point &
              period_return_20_Price > -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,22) ),

            trade_col == "Short" & lead(Ask_High,22) < stop_point &
              lead(Ask_Low,22) < profit_point &
              period_return_20_Price > -1*stop_return~ profit_return
          ),

        period_return_22_Price =
          case_when(

            trade_col == "Long" &
              (lead(Bid_Low,23) <= stop_point|period_return_21_Price<= -1*stop_return) &
              period_return_21_Price < profit_return ~ -1*stop_return,

            trade_col == "Long" & lead(Bid_Low,23) > stop_point &
              lead(Bid_High,23) < profit_point &
              period_return_21_Price > -1*stop_return &
              period_return_21_Price < profit_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,23) - lead(Ask_Price)) ),

            trade_col == "Long" &
              ((lead(Bid_Low,23) > stop_point &
                  lead(Bid_High,23) > profit_point &
                  period_return_21_Price > -1*stop_return)|
                 period_return_21_Price >= profit_return) ~ profit_return,

            trade_col == "Short" & lead(Ask_High,23) >= stop_point|
              period_return_21_Price <= -1*stop_return ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,23) < stop_point &
              lead(Ask_Low,23) > profit_point &
              period_return_21_Price > -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,23) ),

            trade_col == "Short" & lead(Ask_High,23) < stop_point &
              lead(Ask_Low,23) < profit_point &
              period_return_21_Price > -1*stop_return~ profit_return
          ),

        period_return_23_Price =
          case_when(

            trade_col == "Long" &
              (lead(Bid_Low,24) <= stop_point|period_return_22_Price<= -1*stop_return) &
              period_return_22_Price < profit_return ~ -1*stop_return,

            trade_col == "Long" & lead(Bid_Low,24) > stop_point &
              lead(Bid_High,24) < profit_point &
              period_return_22_Price > -1*stop_return &
              period_return_22_Price < profit_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,24) - lead(Ask_Price)) ),

            trade_col == "Long" &
              ((lead(Bid_Low,24) > stop_point &
                  lead(Bid_High,24) > profit_point &
                  period_return_22_Price > -1*stop_return)|
                 period_return_22_Price >= profit_return) ~ profit_return,

            trade_col == "Short" & lead(Ask_High,24) >= stop_point|
              period_return_22_Price <= -1*stop_return ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,24) < stop_point &
              lead(Ask_Low,24) > profit_point &
              period_return_22_Price > -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,24) ),

            trade_col == "Short" & lead(Ask_High,24) < stop_point &
              lead(Ask_Low,24) < profit_point &
              period_return_22_Price > -1*stop_return~ profit_return
          ),

        period_return_24_Price =
          case_when(

            trade_col == "Long" &
              (lead(Bid_Low,25) <= stop_point|period_return_23_Price<= -1*stop_return) &
              period_return_23_Price < profit_return ~ -1*stop_return,

            trade_col == "Long" & lead(Bid_Low,25) > stop_point &
              lead(Bid_High,25) < profit_point &
              period_return_23_Price > -1*stop_return &
              period_return_23_Price < profit_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,25) - lead(Ask_Price)) ),

            trade_col == "Long" &
              ((lead(Bid_Low,25) > stop_point &
                  lead(Bid_High,25) > profit_point &
                  period_return_23_Price > -1*stop_return)|
                 period_return_23_Price >= profit_return) ~ profit_return,

            trade_col == "Short" & lead(Ask_High,25) >= stop_point|
              period_return_23_Price <= -1*stop_return ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,25) < stop_point &
              lead(Ask_Low,25) > profit_point &
              period_return_23_Price > -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,25) ),

            trade_col == "Short" & lead(Ask_High,25) < stop_point &
              lead(Ask_Low,25) < profit_point &
              period_return_23_Price > -1*stop_return~ profit_return
          ),

        period_return_25_Price =
          case_when(

            trade_col == "Long" &
              (lead(Bid_Low,26) <= stop_point|period_return_24_Price<= -1*stop_return) &
              period_return_24_Price < profit_return ~ -1*stop_return,

            trade_col == "Long" & lead(Bid_Low,26) > stop_point &
              lead(Bid_High,26) < profit_point &
              period_return_24_Price > -1*stop_return &
              period_return_24_Price < profit_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,26) - lead(Ask_Price)) ),

            trade_col == "Long" &
              ((lead(Bid_Low,26) > stop_point &
                  lead(Bid_High,26) > profit_point &
                  period_return_24_Price > -1*stop_return)|
                 period_return_24_Price >= profit_return) ~ profit_return,

            trade_col == "Short" & lead(Ask_High,26) >= stop_point|
              period_return_24_Price <= -1*stop_return ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,26) < stop_point &
              lead(Ask_Low,26) > profit_point &
              period_return_24_Price > -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,26) ),

            trade_col == "Short" & lead(Ask_High,26) < stop_point &
              lead(Ask_Low,26) < profit_point &
              period_return_24_Price > -1*stop_return~ profit_return
          ),

        period_return_26_Price =
          case_when(

            trade_col == "Long" &
              (lead(Bid_Low,27) <= stop_point|period_return_25_Price<= -1*stop_return) &
              period_return_25_Price < profit_return ~ -1*stop_return,

            trade_col == "Long" & lead(Bid_Low,27) > stop_point &
              lead(Bid_High,27) < profit_point &
              period_return_25_Price > -1*stop_return &
              period_return_25_Price < profit_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,27) - lead(Ask_Price)) ),

            trade_col == "Long" &
              ((lead(Bid_Low,27) > stop_point &
                  lead(Bid_High,27) > profit_point &
                  period_return_25_Price > -1*stop_return)|
                 period_return_25_Price >= profit_return) ~ profit_return,

            trade_col == "Short" & lead(Ask_High,27) >= stop_point|
              period_return_25_Price <= -1*stop_return ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,27) < stop_point &
              lead(Ask_Low,27) > profit_point &
              period_return_25_Price > -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,27) ),

            trade_col == "Short" & lead(Ask_High,27) < stop_point &
              lead(Ask_Low,27) < profit_point &
              period_return_25_Price > -1*stop_return~ profit_return
          ),

        period_return_27_Price =
          case_when(
            trade_col == "Long" &
              (lead(Bid_Low,28) <= stop_point|period_return_26_Price<= -1*stop_return) &
              period_return_26_Price < profit_return ~ -1*stop_return,

            trade_col == "Long" & lead(Bid_Low,28) > stop_point &
              lead(Bid_High,28) < profit_point &
              period_return_26_Price > -1*stop_return &
              period_return_26_Price < profit_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,28) - lead(Ask_Price)) ),

            trade_col == "Long" &
              ((lead(Bid_Low,28) > stop_point &
                  lead(Bid_High,28) > profit_point &
                  period_return_26_Price > -1*stop_return)|
                 period_return_26_Price >= profit_return) ~ profit_return,

            trade_col == "Short" & lead(Ask_High,28) >= stop_point|
              period_return_26_Price <= -1*stop_return ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,28) < stop_point &
              lead(Ask_Low,28) > profit_point &
              period_return_26_Price > -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,28) ),

            trade_col == "Short" & lead(Ask_High,28) < stop_point &
              lead(Ask_Low,28) < profit_point &
              period_return_26_Price > -1*stop_return~ profit_return
          ),

        period_return_28_Price =
          case_when(

            trade_col == "Long" &
              (lead(Bid_Low,29) <= stop_point|period_return_27_Price<= -1*stop_return) &
              period_return_27_Price < profit_return ~ -1*stop_return,

            trade_col == "Long" & lead(Bid_Low,29) > stop_point &
              lead(Bid_High,29) < profit_point &
              period_return_27_Price > -1*stop_return &
              period_return_27_Price < profit_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,29) - lead(Ask_Price)) ),

            trade_col == "Long" &
              ((lead(Bid_Low,29) > stop_point &
                  lead(Bid_High,29) > profit_point &
                  period_return_27_Price > -1*stop_return)|
                 period_return_27_Price >= profit_return) ~ profit_return,

            trade_col == "Short" & lead(Ask_High,29) >= stop_point|
              period_return_27_Price <= -1*stop_return ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,29) < stop_point &
              lead(Ask_Low,29) > profit_point &
              period_return_27_Price > -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,29) ),

            trade_col == "Short" & lead(Ask_High,29) < stop_point &
              lead(Ask_Low,29) < profit_point &
              period_return_27_Price > -1*stop_return~ profit_return
          ),

        period_return_29_Price =
          case_when(

            trade_col == "Long" &
              (lead(Bid_Low,30) <= stop_point|period_return_28_Price<= -1*stop_return) &
              period_return_28_Price < profit_return ~ -1*stop_return,

            trade_col == "Long" & lead(Bid_Low,30) > stop_point &
              lead(Bid_High,30) < profit_point &
              period_return_28_Price > -1*stop_return &
              period_return_28_Price < profit_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,30) - lead(Ask_Price)) ),

            trade_col == "Long" &
              ((lead(Bid_Low,30) > stop_point &
                  lead(Bid_High,30) > profit_point &
                  period_return_28_Price > -1*stop_return)|
                 period_return_28_Price >= profit_return) ~ profit_return,

            trade_col == "Short" & lead(Ask_High,30) >= stop_point|
              period_return_28_Price <= -1*stop_return ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,30) < stop_point &
              lead(Ask_Low,30) > profit_point &
              period_return_28_Price > -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,30) ),

            trade_col == "Short" & lead(Ask_High,30) < stop_point &
              lead(Ask_Low,30) < profit_point &
              period_return_28_Price > -1*stop_return~ profit_return
          ),

        period_return_30_Price =
          case_when(

            trade_col == "Long" &
              (lead(Bid_Low,31) <= stop_point|period_return_29_Price<= -1*stop_return) &
              period_return_29_Price < profit_return ~ -1*stop_return,

            trade_col == "Long" & lead(Bid_Low,31) > stop_point &
              lead(Bid_High,31) < profit_point &
              period_return_29_Price > -1*stop_return &
              period_return_29_Price < profit_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,31) - lead(Ask_Price)) ),

            trade_col == "Long" &
              ((lead(Bid_Low,31) > stop_point &
                  lead(Bid_High,31) > profit_point &
                  period_return_29_Price > -1*stop_return)|
                 period_return_29_Price >= profit_return) ~ profit_return,

            trade_col == "Short" & lead(Ask_High,31) >= stop_point|
              period_return_29_Price <= -1*stop_return ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,31) < stop_point &
              lead(Ask_Low,31) > profit_point &
              period_return_29_Price > -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,31) ),

            trade_col == "Short" & lead(Ask_High,31) < stop_point &
              lead(Ask_Low,31) < profit_point &
              period_return_29_Price > -1*stop_return~ profit_return
          ),

        period_return_31_Price =
          case_when(
            trade_col == "Long" &
              (lead(Bid_Low,32) <= stop_point|period_return_30_Price<= -1*stop_return) &
              period_return_30_Price < profit_return ~ -1*stop_return,

            trade_col == "Long" & lead(Bid_Low,32) > stop_point &
              lead(Bid_High,32) < profit_point &
              period_return_30_Price > -1*stop_return &
              period_return_30_Price < profit_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,32) - lead(Ask_Price)) ),

            trade_col == "Long" &
              ((lead(Bid_Low,32) > stop_point &
                  lead(Bid_High,32) > profit_point &
                  period_return_30_Price > -1*stop_return)|
                 period_return_30_Price >= profit_return) ~ profit_return,

            trade_col == "Short" & lead(Ask_High,32) >= stop_point|
              period_return_30_Price <= -1*stop_return ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,32) < stop_point &
              lead(Ask_Low,32) > profit_point &
              period_return_30_Price > -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,32) ),

            trade_col == "Short" & lead(Ask_High,32) < stop_point &
              lead(Ask_Low,32) < profit_point &
              period_return_30_Price > -1*stop_return~ profit_return
          ),

        period_return_32_Price =
          case_when(

            trade_col == "Long" &
              (lead(Bid_Low,33) <= stop_point|period_return_31_Price<= -1*stop_return) &
              period_return_31_Price < profit_return ~ -1*stop_return,

            trade_col == "Long" & lead(Bid_Low,33) > stop_point &
              lead(Bid_High,33) < profit_point &
              period_return_31_Price > -1*stop_return &
              period_return_31_Price < profit_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,33) - lead(Ask_Price)) ),

            trade_col == "Long" &
              ((lead(Bid_Low,33) > stop_point &
                  lead(Bid_High,33) > profit_point &
                  period_return_31_Price > -1*stop_return)|
                 period_return_31_Price >= profit_return) ~ profit_return,

            trade_col == "Short" & lead(Ask_High,33) >= stop_point|
              period_return_31_Price <= -1*stop_return ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,33) < stop_point &
              lead(Ask_Low,33) > profit_point &
              period_return_31_Price > -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,33) ),

            trade_col == "Short" & lead(Ask_High,33) < stop_point &
              lead(Ask_Low,33) < profit_point &
              period_return_31_Price > -1*stop_return~ profit_return
          ),

        period_return_33_Price =
          case_when(

            trade_col == "Long" &
              (lead(Bid_Low,34) <= stop_point|period_return_32_Price<= -1*stop_return) &
              period_return_32_Price < profit_return ~ -1*stop_return,

            trade_col == "Long" & lead(Bid_Low,34) > stop_point &
              lead(Bid_High,34) < profit_point &
              period_return_32_Price > -1*stop_return &
              period_return_32_Price < profit_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,34) - lead(Ask_Price)) ),

            trade_col == "Long" &
              ((lead(Bid_Low,34) > stop_point &
                  lead(Bid_High,34) > profit_point &
                  period_return_32_Price > -1*stop_return)|
                 period_return_32_Price >= profit_return) ~ profit_return,

            trade_col == "Short" & lead(Ask_High,34) >= stop_point|
              period_return_32_Price <= -1*stop_return ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,34) < stop_point &
              lead(Ask_Low,34) > profit_point &
              period_return_32_Price > -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,34) ),

            trade_col == "Short" & lead(Ask_High,34) < stop_point &
              lead(Ask_Low,34) < profit_point &
              period_return_32_Price > -1*stop_return~ profit_return
          ),

        period_return_34_Price =
          case_when(

            trade_col == "Long" &
              (lead(Bid_Low,35) <= stop_point|period_return_33_Price<= -1*stop_return) &
              period_return_33_Price < profit_return ~ -1*stop_return,

            trade_col == "Long" & lead(Bid_Low,35) > stop_point &
              lead(Bid_High,35) < profit_point &
              period_return_33_Price > -1*stop_return &
              period_return_33_Price < profit_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,35) - lead(Ask_Price)) ),

            trade_col == "Long" &
              ((lead(Bid_Low,35) > stop_point &
                  lead(Bid_High,35) > profit_point &
                  period_return_33_Price > -1*stop_return)|
                 period_return_33_Price >= profit_return) ~ profit_return,

            trade_col == "Short" & lead(Ask_High,35) >= stop_point|
              period_return_33_Price <= -1*stop_return ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,35) < stop_point &
              lead(Ask_Low,35) > profit_point &
              period_return_33_Price > -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,35) ),

            trade_col == "Short" & lead(Ask_High,35) < stop_point &
              lead(Ask_Low,35) < profit_point &
              period_return_33_Price > -1*stop_return~ profit_return
          ),

        period_return_35_Price =
          case_when(

            trade_col == "Long" &
              (lead(Bid_Low,36) <= stop_point|period_return_34_Price<= -1*stop_return) &
              period_return_34_Price < profit_return ~ -1*stop_return,

            trade_col == "Long" & lead(Bid_Low,36) > stop_point &
              lead(Bid_High,36) < profit_point &
              period_return_34_Price > -1*stop_return &
              period_return_34_Price < profit_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,36) - lead(Ask_Price)) ),

            trade_col == "Long" &
              ((lead(Bid_Low,36) > stop_point &
                  lead(Bid_High,36) > profit_point &
                  period_return_34_Price > -1*stop_return)|
                 period_return_34_Price >= profit_return) ~ profit_return,

            trade_col == "Short" & lead(Ask_High,36) >= stop_point|
              period_return_34_Price <= -1*stop_return ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,36) < stop_point &
              lead(Ask_Low,36) > profit_point &
              period_return_34_Price > -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,36) ),

            trade_col == "Short" & lead(Ask_High,36) < stop_point &
              lead(Ask_Low,36) < profit_point &
              period_return_34_Price > -1*stop_return~ profit_return
          ),

        period_return_36_Price =
          case_when(

            trade_col == "Long" &
              (lead(Bid_Low,37) <= stop_point|period_return_35_Price<= -1*stop_return) &
              period_return_35_Price < profit_return ~ -1*stop_return,

            trade_col == "Long" & lead(Bid_Low,37) > stop_point &
              lead(Bid_High,37) < profit_point &
              period_return_35_Price > -1*stop_return &
              period_return_35_Price < profit_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,37) - lead(Ask_Price)) ),

            trade_col == "Long" &
              ((lead(Bid_Low,37) > stop_point &
                  lead(Bid_High,37) > profit_point &
                  period_return_35_Price > -1*stop_return)|
                 period_return_35_Price >= profit_return) ~ profit_return,

            trade_col == "Short" & lead(Ask_High,37) >= stop_point|
              period_return_35_Price <= -1*stop_return ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,37) < stop_point &
              lead(Ask_Low,37) > profit_point &
              period_return_35_Price > -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,37) ),

            trade_col == "Short" & lead(Ask_High,37) < stop_point &
              lead(Ask_Low,37) < profit_point &
              period_return_35_Price > -1*stop_return~ profit_return
          ),

        period_return_37_Price =
          case_when(

            trade_col == "Long" &
              (lead(Bid_Low,38) <= stop_point|period_return_36_Price<= -1*stop_return) &
              period_return_36_Price < profit_return ~ -1*stop_return,

            trade_col == "Long" & lead(Bid_Low,38) > stop_point &
              lead(Bid_High,38) < profit_point &
              period_return_36_Price > -1*stop_return &
              period_return_36_Price < profit_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,38) - lead(Ask_Price)) ),

            trade_col == "Long" &
              ((lead(Bid_Low,38) > stop_point &
                  lead(Bid_High,38) > profit_point &
                  period_return_36_Price > -1*stop_return)|
                 period_return_36_Price >= profit_return) ~ profit_return,

            trade_col == "Short" & lead(Ask_High,38) >= stop_point|
              period_return_36_Price <= -1*stop_return ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,38) < stop_point &
              lead(Ask_Low,38) > profit_point &
              period_return_36_Price > -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,38) ),

            trade_col == "Short" & lead(Ask_High,38) < stop_point &
              lead(Ask_Low,38) < profit_point &
              period_return_36_Price > -1*stop_return~ profit_return
          ),


        period_return_38_Price =
          case_when(

            trade_col == "Long" &
              (lead(Bid_Low,39) <= stop_point|period_return_37_Price<= -1*stop_return) &
              period_return_37_Price < profit_return ~ -1*stop_return,

            trade_col == "Long" & lead(Bid_Low,39) > stop_point &
              lead(Bid_High,39) < profit_point &
              period_return_37_Price > -1*stop_return &
              period_return_37_Price < profit_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,39) - lead(Ask_Price)) ),

            trade_col == "Long" &
              ((lead(Bid_Low,39) > stop_point &
                  lead(Bid_High,39) > profit_point &
                  period_return_37_Price > -1*stop_return)|
                 period_return_37_Price >= profit_return) ~ profit_return,

            trade_col == "Short" & lead(Ask_High,39) >= stop_point|
              period_return_37_Price <= -1*stop_return ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,39) < stop_point &
              lead(Ask_Low,39) > profit_point &
              period_return_37_Price > -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,39) ),

            trade_col == "Short" & lead(Ask_High,39) < stop_point &
              lead(Ask_Low,39) < profit_point &
              period_return_37_Price > -1*stop_return~ profit_return
          ),

        period_return_39_Price =
          case_when(

            trade_col == "Long" &
              (lead(Bid_Low,40) <= stop_point|period_return_38_Price<= -1*stop_return) &
              period_return_38_Price < profit_return ~ -1*stop_return,

            trade_col == "Long" & lead(Bid_Low,40) > stop_point &
              lead(Bid_High,40) < profit_point &
              period_return_38_Price > -1*stop_return &
              period_return_38_Price < profit_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,40) - lead(Ask_Price)) ),

            trade_col == "Long" &
              ((lead(Bid_Low,40) > stop_point &
                  lead(Bid_High,40) > profit_point &
                  period_return_38_Price > -1*stop_return)|
                 period_return_38_Price >= profit_return) ~ profit_return,

            trade_col == "Short" & lead(Ask_High,40) >= stop_point|
              period_return_38_Price <= -1*stop_return ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,40) < stop_point &
              lead(Ask_Low,40) > profit_point &
              period_return_38_Price > -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,40) ),

            trade_col == "Short" & lead(Ask_High,40) < stop_point &
              lead(Ask_Low,40) < profit_point &
              period_return_38_Price > -1*stop_return~ profit_return
          ),

        period_return_40_Price =
          case_when(

            trade_col == "Long" &
              (lead(Bid_Low,41) <= stop_point|period_return_39_Price<= -1*stop_return) &
              period_return_39_Price < profit_return ~ -1*stop_return,

            trade_col == "Long" & lead(Bid_Low,41) > stop_point &
              lead(Bid_High,41) < profit_point &
              period_return_39_Price > -1*stop_return &
              period_return_39_Price < profit_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,41) - lead(Ask_Price)) ),

            trade_col == "Long" &
              ((lead(Bid_Low,41) > stop_point &
                  lead(Bid_High,41) > profit_point &
                  period_return_39_Price > -1*stop_return)|
                 period_return_39_Price >= profit_return) ~ profit_return,

            trade_col == "Short" & lead(Ask_High,41) >= stop_point|
              period_return_39_Price <= -1*stop_return ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,41) < stop_point &
              lead(Ask_Low,41) > profit_point &
              period_return_39_Price > -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,41) ),

            trade_col == "Short" & lead(Ask_High,41) < stop_point &
              lead(Ask_Low,41) < profit_point &
              period_return_39_Price > -1*stop_return~ profit_return
          ),

        period_return_41_Price =
          case_when(

            trade_col == "Long" &
              (lead(Bid_Low,42) <= stop_point|period_return_40_Price<= -1*stop_return) &
              period_return_40_Price < profit_return ~ -1*stop_return,

            trade_col == "Long" & lead(Bid_Low,42) > stop_point &
              lead(Bid_High,42) < profit_point &
              period_return_40_Price > -1*stop_return &
              period_return_40_Price < profit_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,42) - lead(Ask_Price)) ),

            trade_col == "Long" &
              ((lead(Bid_Low,42) > stop_point &
                  lead(Bid_High,42) > profit_point &
                  period_return_40_Price > -1*stop_return)|
                 period_return_40_Price >= profit_return) ~ profit_return,

            trade_col == "Short" & lead(Ask_High,42) >= stop_point|
              period_return_40_Price <= -1*stop_return ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,42) < stop_point &
              lead(Ask_Low,42) > profit_point &
              period_return_40_Price > -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,42) ),

            trade_col == "Short" & lead(Ask_High,42) < stop_point &
              lead(Ask_Low,42) < profit_point &
              period_return_40_Price > -1*stop_return~ profit_return
          ),

        period_return_42_Price =
          case_when(

            trade_col == "Long" &
              (lead(Bid_Low,43) <= stop_point|period_return_41_Price<= -1*stop_return) &
              period_return_41_Price < profit_return ~ -1*stop_return,

            trade_col == "Long" & lead(Bid_Low,43) > stop_point &
              lead(Bid_High,43) < profit_point &
              period_return_41_Price > -1*stop_return &
              period_return_41_Price < profit_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,43) - lead(Ask_Price)) ),

            trade_col == "Long" &
              ((lead(Bid_Low,43) > stop_point &
                  lead(Bid_High,43) > profit_point &
                  period_return_41_Price > -1*stop_return)|
                 period_return_41_Price >= profit_return) ~ profit_return,

            trade_col == "Short" & lead(Ask_High,43) >= stop_point|
              period_return_41_Price <= -1*stop_return ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,43) < stop_point &
              lead(Ask_Low,43) > profit_point &
              period_return_41_Price > -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,43) ),

            trade_col == "Short" & lead(Ask_High,43) < stop_point &
              lead(Ask_Low,43) < profit_point &
              period_return_41_Price > -1*stop_return~ profit_return
          ),

        period_return_43_Price =
          case_when(

            trade_col == "Long" &
              (lead(Bid_Low,44) <= stop_point|period_return_42_Price<= -1*stop_return) &
              period_return_42_Price < profit_return ~ -1*stop_return,

            trade_col == "Long" & lead(Bid_Low,44) > stop_point &
              lead(Bid_High,44) < profit_point &
              period_return_42_Price > -1*stop_return &
              period_return_42_Price < profit_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,44) - lead(Ask_Price)) ),

            trade_col == "Long" &
              ((lead(Bid_Low,44) > stop_point &
                  lead(Bid_High,44) > profit_point &
                  period_return_42_Price > -1*stop_return)|
                 period_return_42_Price >= profit_return) ~ profit_return,

            trade_col == "Short" & lead(Ask_High,44) >= stop_point|
              period_return_42_Price <= -1*stop_return ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,44) < stop_point &
              lead(Ask_Low,44) > profit_point &
              period_return_42_Price > -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,44) ),

            trade_col == "Short" & lead(Ask_High,44) < stop_point &
              lead(Ask_Low,44) < profit_point &
              period_return_42_Price > -1*stop_return~ profit_return
          ),

        period_return_44_Price =
          case_when(

            trade_col == "Long" &
              (lead(Bid_Low,45) <= stop_point|period_return_43_Price<= -1*stop_return) &
              period_return_43_Price < profit_return ~ -1*stop_return,

            trade_col == "Long" & lead(Bid_Low,45) > stop_point &
              lead(Bid_High,45) < profit_point &
              period_return_43_Price > -1*stop_return &
              period_return_43_Price < profit_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,45) - lead(Ask_Price)) ),

            trade_col == "Long" &
              ((lead(Bid_Low,45) > stop_point &
                  lead(Bid_High,45) > profit_point &
                  period_return_43_Price > -1*stop_return)|
                 period_return_43_Price >= profit_return) ~ profit_return,

            trade_col == "Short" & lead(Ask_High,45) >= stop_point|
              period_return_43_Price <= -1*stop_return ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,45) < stop_point &
              lead(Ask_Low,45) > profit_point &
              period_return_43_Price > -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,45) ),

            trade_col == "Short" & lead(Ask_High,45) < stop_point &
              lead(Ask_Low,45) < profit_point &
              period_return_43_Price > -1*stop_return~ profit_return
          ),

        period_return_45_Price =
          case_when(

            trade_col == "Long" &
              (lead(Bid_Low,46) <= stop_point|period_return_44_Price<= -1*stop_return) &
              period_return_44_Price < profit_return ~ -1*stop_return,

            trade_col == "Long" & lead(Bid_Low,46) > stop_point &
              lead(Bid_High,46) < profit_point &
              period_return_44_Price > -1*stop_return &
              period_return_44_Price < profit_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,46) - lead(Ask_Price)) ),

            trade_col == "Long" &
              ((lead(Bid_Low,46) > stop_point &
                  lead(Bid_High,46) > profit_point &
                  period_return_44_Price > -1*stop_return)|
                 period_return_44_Price >= profit_return) ~ profit_return,

            trade_col == "Short" & lead(Ask_High,46) >= stop_point|
              period_return_44_Price <= -1*stop_return ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,46) < stop_point &
              lead(Ask_Low,46) > profit_point &
              period_return_44_Price > -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,46) ),

            trade_col == "Short" & lead(Ask_High,46) < stop_point &
              lead(Ask_Low,46) < profit_point &
              period_return_44_Price > -1*stop_return~ profit_return
          ),

        period_return_46_Price =
          case_when(

            trade_col == "Long" &
              (lead(Bid_Low,47) <= stop_point|period_return_45_Price<= -1*stop_return) &
              period_return_45_Price < profit_return ~ -1*stop_return,

            trade_col == "Long" & lead(Bid_Low,47) > stop_point &
              lead(Bid_High,47) < profit_point &
              period_return_45_Price > -1*stop_return &
              period_return_45_Price < profit_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,47) - lead(Ask_Price)) ),

            trade_col == "Short" & lead(Ask_High,47) >= stop_point|
              period_return_45_Price <= -1*stop_return ~ -1*stop_return,

            trade_col == "Long" &
              ((lead(Bid_Low,47) > stop_point &
                  lead(Bid_High,47) > profit_point &
                  period_return_45_Price > -1*stop_return)|
                 period_return_45_Price >= profit_return) ~ profit_return,

            trade_col == "Short" & lead(Ask_High,47) < stop_point &
              lead(Ask_Low,47) > profit_point &
              period_return_45_Price > -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,47) ),

            trade_col == "Short" & lead(Ask_High,47) < stop_point &
              lead(Ask_Low,47) < profit_point &
              period_return_45_Price > -1*stop_return~ profit_return
          ),

        period_return_47_Price =
          case_when(

            trade_col == "Long" &
              (lead(Bid_Low,48) <= stop_point|period_return_46_Price<= -1*stop_return) &
              period_return_46_Price < profit_return ~ -1*stop_return,

            trade_col == "Long" & lead(Bid_Low,48) > stop_point &
              lead(Bid_High,48) < profit_point &
              period_return_46_Price > -1*stop_return &
              period_return_46_Price < profit_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,48) - lead(Ask_Price)) ),

            trade_col == "Long" &
              ((lead(Bid_Low,48) > stop_point &
                  lead(Bid_High,48) > profit_point &
                  period_return_46_Price > -1*stop_return)|
                 period_return_46_Price >= profit_return) ~ profit_return,

            trade_col == "Short" & lead(Ask_High,48) >= stop_point|
              period_return_46_Price <= -1*stop_return ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,48) < stop_point &
              lead(Ask_Low,48) > profit_point &
              period_return_46_Price > -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,48) ),

            trade_col == "Short" & lead(Ask_High,48) < stop_point &
              lead(Ask_Low,48) < profit_point &
              period_return_46_Price > -1*stop_return~ profit_return
          ),

        period_return_48_Price =
          case_when(

            trade_col == "Long" &
              (lead(Bid_Low,49) <= stop_point| period_return_47_Price<= -1*stop_return) &
              period_return_47_Price < profit_return ~ -1*stop_return,

            trade_col == "Long" & lead(Bid_Low,49) > stop_point &
              lead(Bid_High,49) < profit_point &
              period_return_47_Price > -1*stop_return &
              period_return_47_Price < profit_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,49) - lead(Ask_Price)) ),

            trade_col == "Long" &
              ((lead(Bid_Low,49) > stop_point &
                  lead(Bid_High,49) > profit_point &
                  period_return_47_Price > -1*stop_return)|
                 period_return_47_Price >= profit_return) ~ profit_return,

            trade_col == "Short" & lead(Ask_High,49) >= stop_point|
              period_return_47_Price <= -1*stop_return ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,49) < stop_point &
              lead(Ask_Low,49) > profit_point &
              period_return_47_Price > -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,49) ),

            trade_col == "Short" & lead(Ask_High,49) < stop_point &
              lead(Ask_Low,49) < profit_point &
              period_return_47_Price > -1*stop_return~ profit_return
          ),

        period_return_49_Price =
          case_when(

            trade_col == "Long" &
              (lead(Bid_Low,50) <= stop_point| period_return_48_Price<= -1*stop_return) &
              period_return_48_Price < profit_return ~ -1*stop_return,

            trade_col == "Long" & lead(Bid_Low,50) > stop_point &
              lead(Bid_High,50) < profit_point &
              period_return_48_Price > -1*stop_return &
              period_return_48_Price < profit_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,50) - lead(Ask_Price)) ),

            trade_col == "Long" &
              ((lead(Bid_Low,50) > stop_point &
                  lead(Bid_High,50) > profit_point &
                  period_return_48_Price > -1*stop_return )|
                 period_return_48_Price >= profit_return) ~ profit_return,

            trade_col == "Short" & lead(Ask_High,50) >= stop_point|
              period_return_48_Price <= -1*stop_return ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,50) < stop_point &
              lead(Ask_Low,50) > profit_point &
              period_return_48_Price > -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,50) ),

            trade_col == "Short" & lead(Ask_High,50) < stop_point &
              lead(Ask_Low,50) < profit_point &
              period_return_48_Price > -1*stop_return~ profit_return
          ),

        period_return_50_Price =
          case_when(

            trade_col == "Long" &
              (lead(Bid_Low,51) <= stop_point | period_return_49_Price<= -1*stop_return) &
              period_return_49_Price < profit_return ~ -1*stop_return,

            trade_col == "Long" & lead(Bid_Low,51) > stop_point &
              lead(Bid_High,51) < profit_point &
              period_return_49_Price > -1*stop_return &
              period_return_49_Price < profit_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,51) - lead(Ask_Price)) ),

            trade_col == "Long" &
              ((lead(Bid_Low,51) > stop_point &
                  lead(Bid_High,51) > profit_point &
                  period_return_49_Price > -1*stop_return) |
                 period_return_49_Price >= profit_return) ~ profit_return,

            trade_col == "Short" & lead(Ask_High,51) >= stop_point|
              period_return_49_Price <= -1*stop_return ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,51) < stop_point &
              lead(Ask_Low,51) > profit_point &
              period_return_49_Price > -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,51) ),

            trade_col == "Short" & lead(Ask_High,51) < stop_point &
              lead(Ask_Low,51) < profit_point &
              period_return_49_Price > -1*stop_return~ profit_return
          )

      )

    return(asset_data_with_indicator)

  }


#' get_actual_wins_losses
#'
#' @param assets_to_analyse
#' @param asset_data
#' @param stop_factor
#' @param profit_factor
#' @param risk_dollar_value
#' @param trade_direction
#' @param currency_conversion
#' @param asset_infor
#'
#' @returns
#' @export
#'
#' @examples
get_actual_wins_losses <- function(
    assets_to_analyse =
      c("EUR_USD", #1
        "EU50_EUR", #2
        "SPX500_USD", #3
        "US2000_USD", #4
        "USB10Y_USD", #5
        "USD_JPY", #6
        "AUD_USD", #7
        "EUR_GBP", #8
        "AU200_AUD" ,#9
        "EUR_AUD", #10
        "WTICO_USD", #11
        "UK100_GBP", #12
        "USD_CAD", #13
        "GBP_USD", #14
        "GBP_CAD", #15
        "EUR_JPY", #16
        "EUR_NZD", #17
        "XAG_USD", #18
        "XAG_EUR", #19
        "XAG_AUD", #20
        "XAG_NZD", #21
        "HK33_HKD", #22
        "FR40_EUR", #23
        "BTC_USD", #24
        "XAG_GBP", #25
        "GBP_AUD", #26
        "USD_SEK", #27
        "USD_SGD", #28
        "NZD_USD", #29
        "GBP_NZD", #30
        "XCU_USD", #31
        "NATGAS_USD", #32
        "GBP_JPY", #33
        "SG30_SGD", #34
        "XAU_USD", #35
        "EUR_SEK", #36
        "XAU_AUD", #37
        "UK10YB_GBP", #38
        "JP225Y_JPY", #39
        "ETH_USD" #40
      ),
    asset_data = Indices_Metals_Bonds,
    stop_factor = 5,
    profit_factor = 30,
    risk_dollar_value = 15,
    trade_direction = "Long",
    currency_conversion = currency_conversion,
    asset_infor = asset_infor,
    periods_ahead = period_var

) {

  temp_actual_wins_losses <- list()

  for (i in 1:length(assets_to_analyse)) {

    temp_actual_wins_losses[[i]] <-
      create_running_profits(
        asset_of_interest = assets_to_analyse[i],
        asset_data = asset_data,
        stop_factor = stop_factor,
        profit_factor = profit_factor,
        risk_dollar_value = risk_dollar_value,
        trade_direction = trade_direction,
        currency_conversion = currency_conversion,
        asset_infor = asset_infor
      )

  }

  actual_wins_losses <-
    temp_actual_wins_losses %>%
    map_dfr(bind_rows) %>%
    dplyr::select(-volume_unadj, -minimumTradeSize_OG, -marginRate,
                  -adjusted_conversion, -pipLocation, -minimumTradeSize_OG) %>%
    dplyr::rename(
      High = Bid_High,
      Low =  Bid_Low
    ) %>%
    mutate(
      trade_return_dollar_aud = !!as.name(glue::glue("period_return_{periods_ahead}_Price") ),

      trade_start_prices =
        case_when(
          trade_col == "Long" ~ Ask_Price,
          trade_col == "Short" ~ Bid_Price
        ),
      trade_end_prices =
        case_when(
          trade_col == "Long" ~ Bid_Price,
          trade_col == "Short" ~ Ask_Price
        ),
      stop_factor = stop_factor,
      profit_factor = profit_factor,
      periods_ahead = periods_ahead
    )

}

#' create_porfolio_sim
#'
#' @param trades_taken
#' @param actual_wins_losses
#'
#' @returns
#' @export
#'
#' @examples
create_porfolio_sim <-
  function(trades_taken = trades_taken,
           actual_wins_losses = actual_wins_losses) {

    min_date_sim = trades_taken$Date %>% min(na.rm = T)
    max_date_sim = trades_taken$Date %>% max(na.rm = T)

    margin_required <-
      actual_wins_losses %>%
      dplyr::select(Date, Asset, volume_required, Ask_Price) %>%
      mutate(ending_value = str_extract(Asset, "_[A-Z][A-Z][A-Z]"),
             ending_value = str_remove_all(ending_value, "_")
      ) %>%
      left_join(currency_conversion, by =c("ending_value" = "not_aud_asset")) %>%
      left_join(asset_infor %>% rename(Asset = name)) %>%
      mutate(
        minimumTradeSize_OG = as.numeric(minimumTradeSize),
        minimumTradeSize = abs(log10(as.numeric(minimumTradeSize))),
        marginRate = as.numeric(marginRate),
        pipLocation = as.numeric(pipLocation),
        displayPrecision = as.numeric(displayPrecision)
      ) %>%
      ungroup() %>%
      mutate(
        volume_adjustment = 1,
        AUD_Price =
          case_when(
            !is.na(adjusted_conversion) ~ (Ask_Price*adjusted_conversion)/volume_adjustment,
            TRUE ~ Ask_Price/volume_adjustment
          ),
        trade_value = AUD_Price*volume_required*marginRate,
        estimated_margin = trade_value
      ) %>%
      dplyr::select(Date, Asset, volume_required, estimated_margin)

    stop_profit_points <-
      actual_wins_losses %>%
      distinct(Date, Asset, stop_return, profit_return)

    returns_long_pivot <-
      actual_wins_losses %>%
      filter(Asset %in%
               (trades_taken %>%
                  pull(Asset) %>%
                  unique())
      ) %>%
      filter(Date >= min_date_sim, Date <= max_date_sim) %>%
      dplyr::select(Asset, Date, contains("period_return")) %>%
      pivot_longer(-c(Date, Asset), values_to = "Return", names_to = "Period") %>%
      mutate(
        Period = str_remove_all(Period, "[A-Z]+|[a-z]+|_") %>% str_trim() %>% as.numeric()
      ) %>%
      left_join(margin_required %>% distinct(Date, Asset, estimated_margin))%>%
      left_join(trades_taken %>% distinct() %>% rename(Period_End_Point = Period)) %>%
      filter(!is.na(Period_End_Point)) %>%
      left_join(stop_profit_points) %>%
      mutate(
        Adjusted_Date = Date + dhours(Period - 1)
      ) %>%
      filter(Period <= Period_End_Point) %>%
      mutate(
        Stopped_End =
          case_when( Return <= -1*stop_return ~Adjusted_Date )
      )   %>%
      group_by(Date, Asset) %>%
      mutate(
        Stopped_End = min(Stopped_End, na.rm = T)
      ) %>%
      ungroup() %>%
      mutate(
        Stopped_End =
          case_when( is.infinite(Stopped_End) ~ NA,
                     TRUE ~ Stopped_End )
      ) %>%
      filter(is.na(Stopped_End) | Adjusted_Date <= Stopped_End)

    returns_long_pivot_sum <-
      returns_long_pivot %>%
      group_by(Adjusted_Date) %>%
      summarise(
        margin_at_date = sum(estimated_margin, na.rm = T),
        running_PL = sum(Return, na.rm = T)
      )

    rm(returns_long_pivot, actual_wins_losses, margin_required, stop_profit_points)

    gc()

    return(returns_long_pivot_sum)

  }


#' get_control_trade_cumulative_returns
#'
#' @param generated_preds
#' @param trade_statement
#' @param trade_direction
#' @param return_col
#'
#' @returns
#' @export
#'
#' @examples
get_control_trade_cumulative_returns <-
  function(
    generated_preds = generated_preds,
    trade_statement = trade_statement,
    actual_wins_losses = actual_wins_losses,
    trade_direction = "Long",
    return_col = "period_return_50_Price"
  ) {

    control_data_asset <-
      generated_preds %>%
      left_join(
        actual_wins_losses %>%
          dplyr::select(Date, Asset, trade_col,  contains(return_col), volume_required) %>%
          filter(trade_col == trade_direction) %>%
          dplyr::select(-trade_col) %>%
          dplyr::select(Date, Asset,  contains(return_col), volume_required) %>%
          distinct()
      ) %>%
      distinct() %>%
      dplyr::select(Date, Asset, !!as.name(return_col), volume_required) %>%
      group_by(Asset) %>%
      arrange(Date, .by_group = TRUE) %>%
      group_by(Asset) %>%
      mutate(
        cumulative_return := cumsum(!!as.name(return_col))
      )

    trade_data <-
      generated_preds %>%
      mutate(
        trade_col =
          eval(parse(text = trade_statement)),
        trade_col =
          ifelse(trade_col == TRUE, trade_direction, paste0("No Trade ", trade_direction) )
      ) %>%
      left_join(
        actual_wins_losses %>%
          dplyr::select(Date, Asset, trade_col,  contains(return_col), volume_required) %>%
          filter(trade_col == trade_direction) %>%
          dplyr::select(-trade_col) %>%
          dplyr::select(Date, Asset,  contains(return_col), volume_required) %>%
          distinct()
      ) %>%
      filter(trade_col == trade_direction) %>%
      group_by(Asset) %>%
      arrange(Date, .by_group = TRUE) %>%
      group_by(Asset) %>%
      mutate(
        cumulative_return = cumsum(!!as.name(return_col))
      ) %>%
      dplyr::select(Date, Asset, trade_col,
                    !!as.name(return_col), cumulative_return, volume_required)

    return(
      list(
        "control_data_asset" = control_data_asset,
        "trade_data" = trade_data
      )
    )

  }

#' get_margin_details
#'
#' @param currency_conversion
#' @param actual_wins_losses
#' @param asset_infor
#'
#' @returns
#' @export
#'
#' @examples
get_margin_details <-
  function(
    currency_conversion = currency_conversion,
    actual_wins_losses = actual_wins_losses,
    asset_infor = asset_infor
  ) {

    margin_required <-
      actual_wins_losses %>%
      dplyr::select(Date, Asset, volume_required, Ask_Price) %>%
      mutate(ending_value = str_extract(Asset, "_[A-Z][A-Z][A-Z]"),
             ending_value = str_remove_all(ending_value, "_")
      ) %>%
      left_join(currency_conversion, by =c("ending_value" = "not_aud_asset")) %>%
      left_join(asset_infor %>% rename(Asset = name)) %>%
      mutate(
        minimumTradeSize_OG = as.numeric(minimumTradeSize),
        minimumTradeSize = abs(log10(as.numeric(minimumTradeSize))),
        marginRate = as.numeric(marginRate),
        pipLocation = as.numeric(pipLocation),
        displayPrecision = as.numeric(displayPrecision)
      ) %>%
      ungroup() %>%
      mutate(
        volume_adjustment = 1,
        AUD_Price =
          case_when(
            !is.na(adjusted_conversion) ~ (Ask_Price*adjusted_conversion)/volume_adjustment,
            TRUE ~ Ask_Price/volume_adjustment
          ),
        trade_value = AUD_Price*volume_required*marginRate,
        estimated_margin = trade_value
      ) %>%
      dplyr::select(Date, Asset, volume_required, estimated_margin)

    return(margin_required)

  }

get_total_portfolio_summary <-
  function(
    generated_preds = generated_preds,
    trade_statement = trade_statement,
    actual_wins_losses = actual_wins_losses,
    trade_direction = "Long",
    return_col = "period_return_50_Price"
  ) {

    sim_data_list <-
      get_control_trade_cumulative_returns(
        generated_preds = generated_preds,
        trade_statement = trade_statement,
        trade_direction = trade_direction,
        actual_wins_losses = actual_wins_losses,
        return_col = return_col
      )


    summarised_data <-
      sim_data_list %>%
      map(
        ~ .x %>%
          group_by(Date) %>%
          summarise(
            Total_Return = sum( !!as.name(return_col), na.rm= T)
          ) %>%
          arrange(Date) %>%
          mutate(
            Cumulative_Return = cumsum(Total_Return)
          )
      )

    summarised_data_combined <-
      summarised_data[[1]] %>%
      mutate(trade_col = "Control") %>%
      bind_rows(
        summarised_data[[2]] %>%
          mutate(trade_col = trade_direction)
      )

    return(summarised_data_combined)

  }

#' generate_random_sampling_returns
#'
#' @param timeseries_returns
#' @param simulations
#' @param samples
#' @param return_col
#'
#' @returns
#' @export
#'
#' @examples
generate_random_sampling_returns <-
  function(timeseries_returns = EUR_USD,
           simulations = 5000,
           samples = 100,
           return_col = "period_return_50_Price") {

    timeseries_returns <-
      timeseries_returns %>%
      mutate(
        win = ifelse(!!as.name(return_col) > 0, 1, 0)
      )

    asset_var <- timeseries_returns$Asset %>% unique() %>% as.character()

    returns_vec <- timeseries_returns %>% pull(!!as.name(return_col))
    win_loss_vec <- timeseries_returns %>% pull(win)

    wins_random <- numeric(simulations)
    returns_random <- numeric(simulations)
    average_win <- numeric(simulations)
    average_loss <- numeric(simulations)

    set.seed(simulations)

    for (i in 1:simulations) {

      wins_sampled <-
        win_loss_vec %>% sample(samples, replace = TRUE)
      returns_sampled <-
        returns_vec %>% sample(samples, replace = TRUE)

      wins_random[i] <- wins_sampled %>% sum(na.rm = T)
      returns_random[i] <- sum(returns_sampled, na.rm = T)
      average_win[i] <- returns_sampled[returns_sampled > 0] %>% mean(na.rm = T)
      average_loss[i] <- returns_sampled[returns_sampled <= 0] %>% mean(na.rm = T)
    }

    simulation_data_frame_random <-
      tibble(
        Asset = asset_var,
        samples_used = samples,
        wins_random = wins_random,
        returns_random = returns_random,
        average_win = average_win,
        average_loss = average_loss
      ) %>%
      mutate(
        win_perc =wins_random/samples
      ) %>%
      group_by(Asset, samples_used) %>%
      summarise(
        across(.cols =
                 c(wins_random, win_perc, average_win, average_loss),
               .fns = ~ mean(., na.rm = T)
        ),

        returns_random_mean = mean(returns_random, na.rm = T),
        returns_random_05 = quantile(returns_random, 0.05, na.rm = T),
        returns_random_25 = quantile(returns_random, 0.25, na.rm = T),
        returns_random_50 = quantile(returns_random, 0.5, na.rm = T),
        returns_random_75 = quantile(returns_random, 0.75, na.rm = T)
      )

    return(simulation_data_frame_random)

  }

#' get_asset_random_sim_returns
#'
#' @param generated_preds
#' @param trade_statement
#' @param trade_direction
#' @param return_col
#' @param simulations
#' @param samples
#'
#' @returns
#' @export
#'
#' @examples
get_asset_random_sim_returns <-
  function(
    generated_preds = generated_preds,
    trade_statement = trade_statement,
    actual_wins_losses = actual_wins_losses,
    trade_direction = "Long",
    return_col = "period_return_50_Price",
    simulations = 5000,
    samples = 20
  ) {

    sim_data_list <-
      get_control_trade_cumulative_returns(
        generated_preds = generated_preds,
        trade_statement = trade_statement,
        actual_wins_losses = actual_wins_losses,
        trade_direction = trade_direction,
        return_col = return_col
      )

    safely_sample <-
      safely(generate_random_sampling_returns, otherwise = NULL)

    sim_data_asset_all <-
      sim_data_list[[2]] %>%
      split(.$Asset, drop = FALSE) %>%
      map(
        ~ .x %>%
          safely_sample(
            simulations = simulations,
            samples = samples,
            return_col = return_col
          ) %>%
          pluck('result')
      )

    complete_summaries <-
      sim_data_list[[2]] %>%
      mutate(
        win = ifelse(!!as.name(return_col) > 0, 1, 0)
      ) %>%
      group_by(Asset) %>%
      summarise(
        Total_returns = sum(!!as.name(return_col), na.rm = T),
        Total_wins = sum(win, na.rm = T),
        Total_Trades = n(),
        Total_Perc = Total_wins/Total_Trades
      )

    sim_data_asset_all_dfr <-
      sim_data_asset_all %>%
      keep(~ !is.null(.x)) %>%
      map_dfr(bind_rows) %>%
      left_join(complete_summaries)

    return(sim_data_asset_all_dfr)

  }

#' construct_ending_point_model
#'
#' @param portfolio_structure
#' @param low_point_end
#' @param high_point_end
#'
#' @return
#' @export
#'
#' @examples
construct_ending_point_model <-
  function(portfolio_structure = portfolio_structure,
           low_point_end = -3,
           high_point_end = 15) {


    end_point_analysis <-
      portfolio_structure %>%
      mutate(
        end_point_low =
          case_when(
            Return <= low_point_end ~ period_since_open
          ),
        end_point_high =
          case_when(
            Return >= high_point_end ~ period_since_open
          )
      ) %>%
      group_by(Date) %>%
      summarise(
        end_point_low = min(end_point_low, na.rm = T),
        end_point_high = min(end_point_high, na.rm = T),
        last_point = mean(close_Date)
      ) %>%
      ungroup() %>%
      mutate(
        across(c(end_point_low, end_point_high),
               .fns = ~ ifelse(is.infinite(.),
                               last_point,
                               .) )
      ) %>%
      mutate(
        true_end_point =
          case_when(
            end_point_low <= end_point_high ~ end_point_low,
            !is.infinite(end_point_low) & is.infinite(end_point_high) ~ end_point_low,
            end_point_high > end_point_low ~ end_point_high,
            is.infinite(end_point_low) & !is.infinite(end_point_high) ~ end_point_high,
            is.infinite(end_point_low) & is.infinite(end_point_high) ~ last_point
          )
      )

    return_analysis <-
      portfolio_structure %>%
      left_join(end_point_analysis) %>%
      filter(period_since_open <= true_end_point)

    return(return_analysis)

  }


#' get_portfolio_struc_with_end_points
#'
#' @param Indices_Metals_Bonds
#' @param trade_data
#' @param traded_assets
#' @param trade_statement_for_filter
#' @param low_point_end
#' @param high_point_end
#' @param stop_factor_var
#' @param profit_factor_var
#' @param risk_dollar_value_var
#' @param end_period_var
#' @param time_frame_var
#' @param trade_direction
#'
#' @return
#' @export
#'
#' @examples
get_portfolio_struc_with_end_points <-
  function(
    Indices_Metals_Bonds =
      Indices_Metals_Bonds %>% map(~ .x %>% filter(Date >= "2024-01-01") ),
    trade_data = pred_data %>% filter(Date >= "2024-01-01"),
    traded_assets = c("EUR_USD"),
    trade_statement_for_filter = "str_detect(Asset, '[A-Z]')",
    low_point_end = -3,
    high_point_end = 15,
    stop_factor_var = 10,
    profit_factor_var = 50,
    risk_dollar_value_var = 10,
    end_period_var = 50,
    time_frame_var = "H1",
    trade_direction = "Long"
  ) {

    portfolio_structure <- list()

    for (i in 1:length(traded_assets)) {

      tagged_trades <-
        trade_data %>%
        mutate(
          trade_col =
            eval(parse(text = trade_statement_for_filter)),
          trade_col =
            ifelse(trade_col == TRUE, trade_direction, paste0("No Trade ", trade_direction) )
        ) %>%
        distinct(Asset, Date, trade_col) %>%
        filter(trade_col == "Long") %>%
        filter(Asset == traded_assets[i])

      portfolio_structure[[i]] <-
        get_portfolio_model(
          asset_data = Indices_Metals_Bonds,
          asset_of_interest = traded_assets[i],
          tagged_trades = tagged_trades,
          stop_factor_long = stop_factor_var,
          profit_factor_long = profit_factor_var,
          risk_dollar_value_long = risk_dollar_value_var,
          end_period = end_period_var,
          time_frame = time_frame_var,
          trade_direction = trade_direction
        )

    }

    portfolio_structure <-
      portfolio_structure %>%
      map_dfr(bind_rows)

    return_data <- list()
    c = 0

    for (i in 1:length(low_point_end)) {
      for (j in 1:length(high_point_end)) {

        c = c + 1
        return_data[[c]] <-
          construct_ending_point_model(
            portfolio_structure = portfolio_structure,
            low_point_end = low_point_end[i],
            high_point_end = high_point_end[j]
          ) %>%
          mutate(
            low_point_end = low_point_end[i],
            high_point_end = high_point_end[j],
            stop_factor = stop_factor_var,
            profit_factor = profit_factor_var,
            risk_dollar_value_long = risk_dollar_value_var,
            end_period = end_period_var,
            trade_direction = trade_direction
          )

      }
    }

    return_data <-
      return_data %>%
      map_dfr(bind_rows)

    return(return_data)

  }


#' construct_portfolio_sim
#'
#' @param portfolio_structure
#' @param starting_capital
#'
#' @return
#' @export
#'
#' @examples
construct_portfolio_sim <-
  function(
    portfolio_structure = portfolio_structure,
    starting_capital = 20000
  ) {

    distinct_dates <-
      portfolio_structure %>%
      distinct(adjusted_Date) %>%
      pull(adjusted_Date)

    all_end_points <-
      portfolio_structure %>%
      filter(period_since_open == close_Date) %>%
      group_by(adjusted_Date) %>%
      summarise(Return = sum(Return, na.rm = T)) %>%
      ungroup() %>%
      arrange(adjusted_Date) %>%
      mutate(
        Cumulative_Return = cumsum(Return) + starting_capital
      ) %>%
      mutate(
        REALISED_THIS_DATE = Return,
        END_TRADE_DATES = adjusted_Date
      )

    all_portfolio_NAV <-
      portfolio_structure %>%
      group_by(adjusted_Date) %>%
      summarise(Return = sum(Return, na.rm = T)) %>%
      ungroup() %>%
      arrange(adjusted_Date) %>%
      left_join(all_end_points) %>%
      fill(Cumulative_Return, .direction = "down") %>%
      mutate(
        REALISED_THIS_DATE =
          ifelse(is.na(REALISED_THIS_DATE), 0, REALISED_THIS_DATE)
      ) %>%
      mutate(
        NAV = Cumulative_Return + (Return - REALISED_THIS_DATE)
      )


    all_portfolio_NAV %>%
      ggplot(aes(x = adjusted_Date, y = NAV)) +
      geom_line() +
      theme_minimal()

    max_portfolio_deviation <-
      all_portfolio_NAV %>%
      dplyr::select(adjusted_Date, Return) %>%
      mutate(
        Deviation = starting_capital + Return
      )

    max_portfolio_deviation %>%
      ggplot(aes(x = adjusted_Date, y = Deviation)) +
      geom_line() +
      theme_minimal()

  }


#' analyse_trade_return_structure
#'
#' @param return_structure
#' @param trade_data
#' @param trade_statement_for_filter
#' @param trade_direction
#' @param asset_of_interest
#'
#' @return
#' @export
#'
#' @examples
analyse_trade_return_structure_asset <-
  function(
    return_structure,
    trade_data = generated_preds_from_db %>% filter(Date >= "2021-01-01"),
    trade_statement_for_filter = trade_statement,
    trade_direction = "Long",
    asset_of_interest = "DE30_EUR"
  ) {

    return_structure_sum <-
      return_structure  %>%
      ungroup() %>%
      filter(Asset == asset_of_interest) %>%
      filter(period_since_open == true_end_point) %>%
      group_by(Asset, Date, low_point_end, high_point_end, profit_factor, stop_factor, trade_direction, adjusted_Date) %>%
      summarise(Return = sum(Return, na.rm = T))

    rm(return_structure)
    gc()

    trade_dates <-
      trade_data %>%
      ungroup() %>%
      filter(Asset == asset_of_interest) %>%
      mutate(
        trade_col =
          eval(parse(text = trade_statement_for_filter)),
        trade_col =
          ifelse(trade_col == TRUE, trade_direction, paste0("No Trade ") )
      ) %>%
      distinct(Asset, Date, trade_col) %>%
      filter(trade_col == "Long")

    Final_Values_control <-
      return_structure_sum %>%
      ungroup() %>%
      ungroup() %>%
      filter(Asset == asset_of_interest) %>%
      group_by(Asset, low_point_end, high_point_end, profit_factor, stop_factor, trade_direction) %>%
      summarise(Return = sum(Return, na.rm = T)) %>%
      mutate(trade_col = "Control")

    Final_Values_trades <-
      return_structure_sum %>%
      ungroup() %>%
      ungroup() %>%
      filter(Asset == asset_of_interest) %>%
      left_join(trade_dates) %>%
      filter(!is.na(trade_col), trade_col == "Long") %>%
      group_by(Asset, low_point_end, high_point_end, profit_factor, stop_factor, trade_direction) %>%
      summarise(Return = sum(Return, na.rm = T))

    distinct_structures <-
      Final_Values_control %>%
      ungroup() %>%
      distinct(Asset, low_point_end, high_point_end, profit_factor, stop_factor, trade_direction)

    accumulator <- list()

    for (o in 1:dim(distinct_structures)[1] ) {

      asset_o <- distinct_structures$Asset[o]
      low_point_end_o <- distinct_structures$low_point_end[o]
      high_point_end_o <- distinct_structures$high_point_end[o]
      profit_factor_o <- distinct_structures$profit_factor[o]
      stop_factor_o <- distinct_structures$stop_factor[o]
      trade_direction_o <- distinct_structures$trade_direction[o]

      control_returns <-
        return_structure_sum %>%
        filter(Asset == asset_o,
               low_point_end == low_point_end_o,
               high_point_end == high_point_end_o,
               profit_factor == profit_factor_o,
               stop_factor == stop_factor_o,
               trade_direction == trade_direction_o) %>%
        pull(Return) %>%
        as.numeric()

      trade_returns <-
        return_structure_sum %>%
        ungroup() %>%
        filter(
          Asset == asset_o,
          low_point_end == low_point_end_o,
          high_point_end == high_point_end_o,
          profit_factor == profit_factor_o,
          stop_factor == stop_factor_o,
          trade_direction == trade_direction_o
        ) %>%
        ungroup() %>%
        filter(Asset == asset_of_interest) %>%
        left_join(trade_dates) %>%
        filter(!is.na(trade_col), trade_col == "Long") %>%
        pull(Return) %>%
        as.numeric()

      random_control <- numeric(5000)
      random_trades <- numeric(5000)

      avg_win_trades <- numeric(5000)
      avg_loss_trades <- numeric(5000)
      perc_trades <- numeric(5000)

      perc_control <- numeric(5000)
      avg_win_control <- numeric(5000)
      avg_loss_control <- numeric(5000)

      for (i in 1:length(random_control)) {

        sample_index <- round(runif(n = 500, min = 1, max = length(control_returns)))
        sample_index_trades <- round(runif(n = 500, min = 1, max = length(trade_returns)))

        random_control[i] <- sum(control_returns[sample_index], na.rm = T)
        random_trades[i] <- sum(trade_returns[sample_index_trades], na.rm = T)

        temp_returns <- trade_returns[sample_index_trades]
        perc_trades <- length(temp_returns[temp_returns > 0])/length(temp_returns)
        avg_win_trades <- mean(temp_returns[temp_returns > 0], na.rm = T)
        avg_loss_trades <- mean(temp_returns[temp_returns <= 0], na.rm = T)

        temp_control <- control_returns[sample_index]
        perc_control <- length(temp_control[temp_control > 0])/length(temp_control)
        avg_win_control <- mean(temp_control[temp_control > 0], na.rm = T)
        avg_loss_control <- mean(temp_control[temp_control <= 0], na.rm = T)

      }

      accumulator[[o]] <-
        tibble(
          Asset = asset_of_interest,
          low_point_end = low_point_end_o,
          high_point_end = high_point_end_o,
          profit_factor = profit_factor_o,
          stop_factor = stop_factor_o,
          trade_direction = trade_direction_o,

          random_control_mean = mean(random_control, na.rm = T),
          random_trades_mean = mean(random_trades, na.rm = T),

          random_control_10 = quantile(random_control,0.1 ,na.rm = T),
          random_trades_10 = quantile(random_trades,0.1, na.rm = T),

          random_control_25 = quantile(random_control, 0.25, na.rm = T),
          random_trades_25 = quantile(random_trades,0.25, na.rm = T),

          random_control_75 = quantile(random_control, 0.75, na.rm = T),
          random_trades_75 = quantile(random_trades, 0.75, na.rm = T),

          perc_control = mean(perc_control, na.rm = T),
          perc_trades = mean(perc_trades, na.rm = T),

          avg_win_control = mean(avg_win_control, na.rm = T),
          avg_win_trades = mean(avg_win_trades, na.rm = T),

          avg_loss_control = mean(avg_loss_control, na.rm = T),
          avg_loss_trades = mean(avg_loss_trades, na.rm = T),

          total_returns_control = sum(control_returns, na.rm = T),
          total_returns_trade = sum(trade_returns, na.rm = T)

        )
    }


    returned_tibble <-
      accumulator %>%
      map_dfr(bind_rows)

    return(returned_tibble)

  }


#' analyse_trade_return_structure
#'
#' @param return_structure
#' @param trade_data
#' @param trade_statement_for_filter
#' @param trade_direction
#' @param asset_of_interest
#'
#' @return
#' @export
#'
#' @examples
analyse_trade_return_structure <-
  function(
    return_structure = return_structure,
    trade_data = generated_preds_from_db %>% filter(Date >= "2023-01-01"),
    trade_statement_for_filter = trade_statement,
    trade_direction = "Long",
    asset_of_interest = c("DE30_EUR")
  ) {

    analysis_returns <- list()
    for (i in 1:length(asset_of_interest)) {

      analysis_returns[[i]] <-
        analyse_trade_return_structure_asset(
          return_structure = return_structure,
          trade_data = trade_data,
          trade_statement_for_filter = trade_statement_for_filter,
          trade_direction = trade_direction,
          asset_of_interest = asset_of_interest[i]
        )

    }

    analysis_returns_dfr <-
      analysis_returns %>%
      map_dfr(bind_rows)

    return(analysis_returns_dfr)

  }


#' analyse_trade_return_structure
#'
#' @param return_structure
#' @param trade_data
#' @param trade_statement_for_filter
#' @param trade_direction
#' @param asset_of_interest
#'
#' @return
#' @export
#'
#' @examples
plot_analysis_trade_return_structure_asset <-
  function(
    return_structure,
    trade_data = generated_preds_from_db %>% filter(Date >= "2021-01-01"),
    trade_statement_for_filter = trade_statement,
    trade_direction = "Long",
    asset_of_interest = "DE30_EUR",
    min_end_point = -3,
    max_end_point = 20
  ) {

    return_structure_sum <-
      return_structure  %>%
      ungroup() %>%
      filter(Asset %in% c(asset_of_interest)) %>%
      filter(period_since_open == true_end_point) %>%
      group_by(Asset, Date, low_point_end, high_point_end, profit_factor, stop_factor, trade_direction, adjusted_Date) %>%
      summarise(Return = sum(Return, na.rm = T))

    rm(return_structure)
    gc()

    trade_dates <-
      trade_data %>%
      ungroup() %>%
      filter(Asset == asset_of_interest) %>%
      mutate(
        trade_col =
          eval(parse(text = trade_statement_for_filter)),
        trade_col =
          ifelse(trade_col == TRUE, trade_direction, paste0("No Trade ") )
      ) %>%
      distinct(Asset, Date, trade_col) %>%
      filter(trade_col == "Long")

    control_data <-
      return_structure_sum %>%
      ungroup() %>%
      ungroup() %>%
      filter(high_point_end == max_end_point,
             low_point_end == min_end_point) %>%
      filter(Asset %in% c(asset_of_interest)) %>%
      group_by(Asset, Date, low_point_end, high_point_end, profit_factor, stop_factor, trade_direction) %>%
      summarise(Return = sum(Return, na.rm = T)) %>%
      mutate(trade_col = "Control") %>%
      ungroup() %>%
      group_by(Asset) %>%
      arrange(Date, .by_group = TRUE) %>%
      mutate(cumulative_return = cumsum(Return)) %>%
      ungroup()

    trade_data <-
      return_structure_sum %>%
      ungroup() %>%
      ungroup() %>%
      filter(high_point_end == max_end_point,
             low_point_end == min_end_point) %>%
      filter(Asset == asset_of_interest) %>%
      left_join(trade_dates) %>%
      filter(!is.na(trade_col), trade_col == "Long") %>%
      group_by(Asset, Date,
               low_point_end,
               high_point_end, profit_factor, stop_factor, trade_direction) %>%
      summarise(Return = sum(Return, na.rm = T)) %>%
      ungroup() %>%
      ungroup() %>%
      mutate(trade_col = "Long") %>%
      group_by(Asset) %>%
      arrange(Date, .by_group = TRUE) %>%
      mutate(cumulative_return = cumsum(Return)) %>%
      ungroup()


    returned_tibble <-
      trade_data %>%
      bind_rows(control_data)

    return(returned_tibble)

  }
