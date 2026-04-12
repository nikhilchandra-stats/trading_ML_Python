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
            str_detect(Asset,"NOK|ZAR|MXN|CNH") ~ (risk_dollar_value/stop_value)*adjusted_conversion,
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
          ),

        period_return_31_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,32) > stop_point &
              lead(Bid_High,32) < profit_point &
              period_return_30_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,32) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,32) > stop_point &
              lead(Bid_High,32) > profit_point &
              period_return_30_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,32) <= stop_point|
              period_return_30_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,32) < stop_point &
              lead(Ask_Low,32) > profit_point &
              period_return_30_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,32) ),

            trade_col == "Short" & lead(Ask_High,32) < stop_point &
              lead(Ask_Low,32) < profit_point &
              period_return_30_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,32) >= stop_point|
              period_return_30_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_32_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,33) > stop_point &
              lead(Bid_High,33) < profit_point &
              period_return_31_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,33) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,33) > stop_point &
              lead(Bid_High,33) > profit_point &
              period_return_31_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,33) <= stop_point|
              period_return_31_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,33) < stop_point &
              lead(Ask_Low,33) > profit_point &
              period_return_31_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,33) ),

            trade_col == "Short" & lead(Ask_High,33) < stop_point &
              lead(Ask_Low,33) < profit_point &
              period_return_31_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,33) >= stop_point|
              period_return_31_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_33_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,34) > stop_point &
              lead(Bid_High,34) < profit_point &
              period_return_32_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,34) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,34) > stop_point &
              lead(Bid_High,34) > profit_point &
              period_return_32_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,34) <= stop_point|
              period_return_32_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,34) < stop_point &
              lead(Ask_Low,34) > profit_point &
              period_return_32_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,34) ),

            trade_col == "Short" & lead(Ask_High,34) < stop_point &
              lead(Ask_Low,34) < profit_point &
              period_return_32_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,34) >= stop_point|
              period_return_32_Price == -1*stop_return ~ -1*stop_return
          )

      )

    return(asset_data_with_indicator)

  }


#' get_portfolio_model
#'
#' @param asset_data
#' @param asset_of_interest
#' @param stop_factor_long
#' @param profit_factor_long
#' @param risk_dollar_value_long
#' @param end_period
#' @param time_frame
#'
#' @return
#' @export
#'
#' @examples
get_portfolio_model <-
  function(
    asset_data = Indices_Metals_Bonds,
    asset_of_interest = distinct_assets[1],
    tagged_trades = tagged_trades,
    stop_factor_long = 4,
    profit_factor_long = 15,
    risk_dollar_value_long = 5,
    end_period = 24,
    time_frame = "H1",
    trade_direction = "Long"
  ) {

    Trades <-
        create_running_profits(
        asset_of_interest = asset_of_interest,
        asset_data = asset_data,
        stop_factor = stop_factor_long,
        profit_factor = profit_factor_long,
        risk_dollar_value = risk_dollar_value_long,
        trade_direction = trade_direction,
        currency_conversion = currency_conversion,
        asset_infor = asset_infor
        )

    tagged_trades_distinct <-
      tagged_trades %>%
      filter(trade_col == trade_direction) %>%
      distinct(Date, Asset) %>%
      mutate(tagged_trade = TRUE)

    Longs_pivoted_for_portfolio <-
      Trades %>%
      dplyr::select(Date, Asset, volume_required, stop_return, profit_return, stop_value, profit_value,
                    contains("period_return_")) %>%
      left_join(tagged_trades_distinct) %>%
      filter(tagged_trade == TRUE) %>%
      dplyr::select(-tagged_trade) %>%
      pivot_longer(-c(Date, Asset, volume_required, stop_return, profit_return, stop_value, profit_value),
                   values_to = "Return",
                   names_to = "Period") %>%
      mutate(
        period_since_open =
          str_remove_all(Period, "[A-Z]+|[a-z]+|_") %>%
          str_trim() %>%
          as.numeric(),

        adjusted_Date =
          case_when(
            time_frame == "H1" ~ Date + dhours(period_since_open),
            time_frame == "D" ~ Date + days(period_since_open)
          )
      ) %>%
      filter(
        period_since_open <= end_period
      ) %>%
      mutate(
        identify_close =
          case_when(
            abs(Return) == abs(stop_return) & Return < 0 | abs(Return) == abs(profit_return) & Return > 0 ~ period_since_open
          )
      ) %>%
      group_by(Date, Asset) %>%
      mutate(
        close_Date = min(identify_close, na.rm = T),
        close_Date = ifelse(
          is.infinite(close_Date),
          end_period,
          close_Date
        )
      ) %>%
      ungroup() %>%
      filter(period_since_open <= close_Date) %>%
      dplyr::select(-identify_close)

    return(Longs_pivoted_for_portfolio)

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
