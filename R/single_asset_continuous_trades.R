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
          ),

        period_return_34_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,35) > stop_point &
              lead(Bid_High,35) < profit_point &
              period_return_33_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,35) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,35) > stop_point &
              lead(Bid_High,35) > profit_point &
              period_return_33_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,35) <= stop_point|
              period_return_33_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,35) < stop_point &
              lead(Ask_Low,35) > profit_point &
              period_return_33_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,35) ),

            trade_col == "Short" & lead(Ask_High,35) < stop_point &
              lead(Ask_Low,35) < profit_point &
              period_return_33_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,35) >= stop_point|
              period_return_33_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_35_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,36) > stop_point &
              lead(Bid_High,36) < profit_point &
              period_return_34_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,36) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,36) > stop_point &
              lead(Bid_High,36) > profit_point &
              period_return_34_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,36) <= stop_point|
              period_return_34_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,36) < stop_point &
              lead(Ask_Low,36) > profit_point &
              period_return_34_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,36) ),

            trade_col == "Short" & lead(Ask_High,36) < stop_point &
              lead(Ask_Low,36) < profit_point &
              period_return_34_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,36) >= stop_point|
              period_return_34_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_36_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,37) > stop_point &
              lead(Bid_High,37) < profit_point &
              period_return_35_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,37) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,37) > stop_point &
              lead(Bid_High,37) > profit_point &
              period_return_35_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,37) <= stop_point|
              period_return_35_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,37) < stop_point &
              lead(Ask_Low,37) > profit_point &
              period_return_35_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,37) ),

            trade_col == "Short" & lead(Ask_High,37) < stop_point &
              lead(Ask_Low,37) < profit_point &
              period_return_35_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,37) >= stop_point|
              period_return_35_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_37_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,38) > stop_point &
              lead(Bid_High,38) < profit_point &
              period_return_36_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,38) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,38) > stop_point &
              lead(Bid_High,38) > profit_point &
              period_return_36_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,38) <= stop_point|
              period_return_36_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,38) < stop_point &
              lead(Ask_Low,38) > profit_point &
              period_return_36_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,38) ),

            trade_col == "Short" & lead(Ask_High,38) < stop_point &
              lead(Ask_Low,38) < profit_point &
              period_return_36_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,38) >= stop_point|
              period_return_36_Price == -1*stop_return ~ -1*stop_return
          ),


        period_return_38_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,39) > stop_point &
              lead(Bid_High,39) < profit_point &
              period_return_37_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,39) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,39) > stop_point &
              lead(Bid_High,39) > profit_point &
              period_return_37_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,39) <= stop_point|
              period_return_37_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,39) < stop_point &
              lead(Ask_Low,39) > profit_point &
              period_return_37_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,39) ),

            trade_col == "Short" & lead(Ask_High,39) < stop_point &
              lead(Ask_Low,39) < profit_point &
              period_return_37_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,39) >= stop_point|
              period_return_37_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_39_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,40) > stop_point &
              lead(Bid_High,40) < profit_point &
              period_return_38_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,40) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,40) > stop_point &
              lead(Bid_High,40) > profit_point &
              period_return_38_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,40) <= stop_point|
              period_return_38_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,40) < stop_point &
              lead(Ask_Low,40) > profit_point &
              period_return_38_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,40) ),

            trade_col == "Short" & lead(Ask_High,40) < stop_point &
              lead(Ask_Low,40) < profit_point &
              period_return_38_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,40) >= stop_point|
              period_return_38_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_40_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,41) > stop_point &
              lead(Bid_High,41) < profit_point &
              period_return_39_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,41) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,41) > stop_point &
              lead(Bid_High,41) > profit_point &
              period_return_39_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,41) <= stop_point|
              period_return_39_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,41) < stop_point &
              lead(Ask_Low,41) > profit_point &
              period_return_39_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,41) ),

            trade_col == "Short" & lead(Ask_High,41) < stop_point &
              lead(Ask_Low,41) < profit_point &
              period_return_39_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,41) >= stop_point|
              period_return_39_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_41_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,42) > stop_point &
              lead(Bid_High,42) < profit_point &
              period_return_40_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,42) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,42) > stop_point &
              lead(Bid_High,42) > profit_point &
              period_return_40_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,42) <= stop_point|
              period_return_40_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,42) < stop_point &
              lead(Ask_Low,42) > profit_point &
              period_return_40_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,42) ),

            trade_col == "Short" & lead(Ask_High,42) < stop_point &
              lead(Ask_Low,42) < profit_point &
              period_return_40_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,42) >= stop_point|
              period_return_40_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_42_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,43) > stop_point &
              lead(Bid_High,43) < profit_point &
              period_return_41_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,43) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,43) > stop_point &
              lead(Bid_High,43) > profit_point &
              period_return_41_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,43) <= stop_point|
              period_return_41_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,43) < stop_point &
              lead(Ask_Low,43) > profit_point &
              period_return_41_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,43) ),

            trade_col == "Short" & lead(Ask_High,43) < stop_point &
              lead(Ask_Low,43) < profit_point &
              period_return_41_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,43) >= stop_point|
              period_return_41_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_43_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,44) > stop_point &
              lead(Bid_High,44) < profit_point &
              period_return_42_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,44) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,44) > stop_point &
              lead(Bid_High,44) > profit_point &
              period_return_42_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,44) <= stop_point|
              period_return_42_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,44) < stop_point &
              lead(Ask_Low,44) > profit_point &
              period_return_42_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,44) ),

            trade_col == "Short" & lead(Ask_High,44) < stop_point &
              lead(Ask_Low,44) < profit_point &
              period_return_42_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,44) >= stop_point|
              period_return_42_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_44_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,45) > stop_point &
              lead(Bid_High,45) < profit_point &
              period_return_43_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,45) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,45) > stop_point &
              lead(Bid_High,45) > profit_point &
              period_return_43_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,45) <= stop_point|
              period_return_43_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,45) < stop_point &
              lead(Ask_Low,45) > profit_point &
              period_return_43_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,45) ),

            trade_col == "Short" & lead(Ask_High,45) < stop_point &
              lead(Ask_Low,45) < profit_point &
              period_return_43_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,45) >= stop_point|
              period_return_43_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_45_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,46) > stop_point &
              lead(Bid_High,46) < profit_point &
              period_return_44_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,46) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,46) > stop_point &
              lead(Bid_High,46) > profit_point &
              period_return_44_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,46) <= stop_point|
              period_return_44_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,46) < stop_point &
              lead(Ask_Low,46) > profit_point &
              period_return_44_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,46) ),

            trade_col == "Short" & lead(Ask_High,46) < stop_point &
              lead(Ask_Low,46) < profit_point &
              period_return_44_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,46) >= stop_point|
              period_return_44_Price == -1*stop_return ~ -1*stop_return
          ),

        period_return_46_Price =
          case_when(
            trade_col == "Long" & lead(Bid_Low,47) > stop_point &
              lead(Bid_High,47) < profit_point &
              period_return_45_Price != -1*stop_return ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price ,47) - lead(Ask_Price)) ),

            trade_col == "Long" & lead(Bid_Low,47) > stop_point &
              lead(Bid_High,47) > profit_point &
              period_return_45_Price != -1*stop_return  ~ profit_return,

            trade_col == "Long" & lead(Bid_Low,47) <= stop_point|
              period_return_45_Price== -1*stop_return  ~ -1*stop_return,

            trade_col == "Short" & lead(Ask_High,47) < stop_point &
              lead(Ask_Low,47) > profit_point &
              period_return_45_Price != -1*stop_return~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,47) ),

            trade_col == "Short" & lead(Ask_High,47) < stop_point &
              lead(Ask_Low,47) < profit_point &
              period_return_45_Price != -1*stop_return~ profit_return,

            trade_col == "Short" & lead(Ask_High,47) >= stop_point|
              period_return_45_Price == -1*stop_return ~ -1*stop_return
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
    periods_ahead = periods_ahead,
    trade_direction = "Long",
    currency_conversion = currency_conversion,
    asset_infor = asset_infor

  ) {

  temp_actual_wins_losses <- list()

  for (i in 1:length(assets_to_analyse)) {

    temp_actual_wins_losses[[i]] <-
      create_running_profits(
        asset_of_interest = assets_to_analyse[i],
        asset_data = Indices_Metals_Bonds,
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

#' get_sig_coefs
#'
#' @param model_object_of_interest
#' @param p_value_thresh_for_inputs
#'
#' @returns
#' @export
#'
#' @examples
get_sig_coefs <-
  function(model_object_of_interest = macro_indicator_model,
           p_value_thresh_for_inputs = 0.5) {


    all_coefs <-  model_object_of_interest %>% jtools::j_summ() %>% pluck(1)
    coef_names <- row.names(all_coefs) %>% as.character()
    filtered_coefs <-
      all_coefs %>%
      as_tibble() %>%
      mutate(all_vars = coef_names) %>%
      filter(p <= p_value_thresh_for_inputs) %>%
      filter(!str_detect(all_vars, "Intercep")) %>%
      pull(all_vars) %>%
      map( ~ str_remove_all(.x, "`") %>% str_trim() ) %>%
      unlist() %>%
      as.character()

    return(filtered_coefs)

  }

#' single_asset_Logit_indicator_adv_gen_models
#'
#' @param asset_data
#' @param All_Daily_Data
#' @param Asset_of_interest
#' @param actual_wins_losses
#' @param interest_rates
#' @param cpi_data
#' @param sentiment_index
#' @param equity_index
#' @param gold_index
#' @param silver_index
#' @param bonds_index
#' @param USD_index
#' @param EUR_index
#' @param GBP_index
#' @param AUD_index
#' @param countries_for_int_strength
#' @param date_train_end
#' @param date_train_phase_2_end
#' @param date_test_start
#' @param couplua_assets
#' @param stop_value_var
#' @param profit_value_var
#' @param period_var
#' @param bin_var_col
#' @param trade_direction
#' @param save_path
#'
#' @returns
#' @export
#'
#' @examples
single_asset_Logit_indicator_adv_gen_models <-
  function(
    asset_data = Indices_Metals_Bonds[[1]],
    All_Daily_Data = All_Daily_Data,
    Asset_of_interest = "EUR_USD",
    actual_wins_losses = actual_wins_losses,
    interest_rates = interest_rates,
    cpi_data = cpi_data,
    sentiment_index = sentiment_index,
    gdp_data = gdp_data,
    unemp_data = unemp_data,
    manufac_pmi = manufac_pmi,
    USD_Macro = USD_Macro,
    EUR_Macro = EUR_Macro,

    equity_index = equity_index,
    gold_index = gold_index,
    silver_index = silver_index,
    bonds_index = bonds_index,
    USD_index = USD_index,
    EUR_index = EUR_index,
    GBP_index = GBP_index,
    AUD_index = AUD_index,
    COMMOD_index = COMMOD_index,
    USD_STOCKS_index = USD_STOCKS_index,
    NZD_index = NZD_index,

    countries_for_int_strength = countries_for_int_strength,
    date_train_end = post_train_date_start,
    date_train_phase_2_end = post_train_date_start + months(6),
    date_test_start = post_train_date_start + months(7),

    couplua_assets = couplua_assets,

    stop_value_var = stop_value_var,
    profit_value_var = profit_value_var,
    period_var = period_var,

    bin_var_col = c("period_return_20_Price", "period_return_35_Price"),
    trade_direction = "Long",
    save_path = "C:/Users/nikhi/Documents/trade_data/single_asset_models_v2_adv"

  ) {

    asset_data_internal <-
      asset_data %>%
      filter(Asset == Asset_of_interest)

    macro_data <-
      prepare_macro_indicator_model_data(
        asset_data = asset_data_internal,
        Asset_of_interest = Asset_of_interest,
        interest_rates = interest_rates,
        cpi_data = cpi_data,
        sentiment_index = sentiment_index,
        gdp_data = gdp_data,
        unemp_data = unemp_data,
        manufac_pmi = manufac_pmi,
        USD_Macro = USD_Macro,
        EUR_Macro = EUR_Macro,
        countries_for_int_strength = countries_for_int_strength,
        date_limit = now(tzone = "Australia/Canberra")
      )

    macro_train_data <-
      macro_data %>%
      filter(Date <= date_train_end)

    message("Saving macro Models")
    prepare_macro_indicator_model(
      macro_for_join = macro_train_data,
      actual_wins_losses = actual_wins_losses,
      Asset_of_interest = Asset_of_interest,
      date_limit = date_train_end,
      stop_value_var = stop_value_var,
      profit_value_var = profit_value_var,
      period_var = period_var,
      bin_var_col = bin_var_col,
      trade_direction = trade_direction,
      save_path = save_path
    )

    macro_phase_2_data <-
      macro_data %>%
      filter(Date < date_train_phase_2_end)

    macro_preds <-
      single_asset_read_models_and_get_pred(
        pred_data = macro_phase_2_data,
        trade_direction = trade_direction,
        Asset_of_interest = Asset_of_interest,
        save_path = save_path,
        model_string = "_macro_"
      )

    macro_preds_averages <-
      macro_preds %>%
      filter(if_all( everything(), ~ !is.nan(.) & !is.infinite(.) ) ) %>%
      summarise(
        across(.cols = contains("pred"), .fns = ~ mean(., na.rm = T))
      )

    names(macro_preds_averages) <-
      names(macro_preds_averages) %>%
      map(~ paste0(.x, "_mean")) %>%
      unlist() %>%
      as.character()

    macro_preds_sd <-
      macro_preds %>%
      filter(if_all( everything(), ~ !is.nan(.) & !is.infinite(.) ) ) %>%
      summarise(
        across(.cols = contains("pred"), .fns = ~ sd(., na.rm = T))
      )

    names(macro_preds_sd) <-
      names(macro_preds_sd) %>%
      map(~ paste0(.x, "_sd")) %>%
      unlist() %>%
      as.character()


    index_pca_data <-
      get_pca_index_indicator_data(
        asset_data = asset_data_internal,
        Asset_of_interest = Asset_of_interest,
        equity_index = equity_index,
        gold_index = gold_index,
        silver_index = silver_index,
        bonds_index = bonds_index,
        USD_index = USD_index,
        EUR_index = EUR_index,
        GBP_index = GBP_index,
        AUD_index = AUD_index,
        COMMOD_index = COMMOD_index,
        USD_STOCKS_index = USD_STOCKS_index,
        NZD_index = NZD_index,
        date_limit = now(tzone = "Australia/Canberra")
      )

    index_pca_train_data <-
      index_pca_data %>%
      filter(Date <= date_train_end)

    prepare_index_indicator_model(
      index_pca_data = index_pca_train_data,
      actual_wins_losses = actual_wins_losses,
      Asset_of_interest = Asset_of_interest,
      date_limit = date_train_end,
      stop_value_var = stop_value_var,
      profit_value_var = profit_value_var,
      period_var = period_var,
      bin_var_col = bin_var_col,
      trade_direction = trade_direction,
      save_path = save_path
    )

    index_pca_phase_2_data <-
      index_pca_data %>%
      filter(Date < date_train_phase_2_end)

    index_preds <-
      single_asset_read_models_and_get_pred(
        pred_data = index_pca_phase_2_data,
        trade_direction = trade_direction,
        Asset_of_interest = Asset_of_interest,
        save_path = save_path,
        model_string = "_index_"
      )

    index_preds_averages <-
      index_preds %>%
      filter(if_all( everything(), ~ !is.nan(.) & !is.infinite(.) ) ) %>%
      summarise(
        across(.cols = contains("pred"), .fns = ~ mean(., na.rm = T))
      )

    names(index_preds_averages) <-
      names(index_preds_averages) %>%
      map(~ paste0(.x, "_mean")) %>%
      unlist() %>%
      as.character()

    index_preds_sd <-
      index_preds%>%
      filter(if_all( everything(), ~ !is.nan(.) & !is.infinite(.) ) ) %>%
      summarise(
        across(.cols = contains("pred"), .fns = ~ sd(., na.rm = T))
      )

    names(index_preds_sd) <-
      names(index_preds_sd) %>%
      map(~ paste0(.x, "_sd")) %>%
      unlist() %>%
      as.character()

    daily_data_for_modelling <-
      prepare_daily_indicator_data(
        asset_data = asset_data_internal,
        All_Daily_Data = All_Daily_Data,
        Asset_of_interest = Asset_of_interest,
        date_limit = now(tzone = "Australia/Canberra")
      )

    daily_data_for_modelling_train <-
      daily_data_for_modelling %>%
      filter(Date <= date_train_end)

    prepare_daily_indicator_model(
      daily_indicator = daily_data_for_modelling_train,
      actual_wins_losses = actual_wins_losses,
      Asset_of_interest = Asset_of_interest,
      date_limit = date_train_end,
      stop_value_var = stop_value_var,
      profit_value_var = profit_value_var,
      period_var = period_var,
      bin_var_col = bin_var_col,
      trade_direction = trade_direction,
      save_path = save_path
    )

    daily_phase_2_data <-
      daily_data_for_modelling %>%
      filter(Date < date_train_phase_2_end)

    daily_preds <-
      single_asset_read_models_and_get_pred(
        pred_data = daily_phase_2_data,
        trade_direction = trade_direction,
        Asset_of_interest = Asset_of_interest,
        save_path = save_path,
        model_string = "_daily_"
      )

    daily_preds_averages <-
      daily_preds %>%
      filter(if_all( everything(), ~ !is.nan(.) & !is.infinite(.) ) ) %>%
      summarise(
        across(.cols = contains("pred"), .fns = ~ mean(., na.rm = T))
      )

    names(daily_preds_averages) <-
      names(daily_preds_averages) %>%
      map(~ paste0(.x, "_mean")) %>%
      unlist() %>%
      as.character()

    daily_preds_sd <-
      daily_preds%>%
      filter(if_all( everything(), ~ !is.nan(.) & !is.infinite(.) ) ) %>%
      summarise(
        across(.cols = contains("pred"), .fns = ~ sd(., na.rm = T))
      )

    names(daily_preds_sd) <-
      names(daily_preds_sd) %>%
      map(~ paste0(.x, "_sd")) %>%
      unlist() %>%
      as.character()

    copula_data <-
      prepare_copula_data(
        asset_data = asset_data,
        couplua_assets = couplua_assets,
        Asset_of_interest = Asset_of_interest,
        date_limit = now(tzone = "Australia/Canberra")
      )

    copula_data_train <-
      copula_data %>%
      filter(Date <= date_train_end)

    prepare_copula_model(
      copula_data = copula_data_train,
      actual_wins_losses = actual_wins_losses,
      Asset_of_interest = Asset_of_interest,
      date_limit = date_train_end,
      stop_value_var = stop_value_var,
      profit_value_var = profit_value_var,
      period_var = period_var,
      bin_var_col = bin_var_col,
      trade_direction = trade_direction,
      save_path = save_path
    )

    copula_phase_2_data <-
      copula_data %>%
      filter(Date < date_train_phase_2_end)

    copula_preds <-
      single_asset_read_models_and_get_pred(
        pred_data = copula_phase_2_data,
        trade_direction = trade_direction,
        Asset_of_interest = Asset_of_interest,
        save_path = save_path,
        model_string = "_copula_"
      )

    copula_preds_averages <-
      copula_preds %>%
      filter(if_all( everything(), ~ !is.nan(.) & !is.infinite(.) ) ) %>%
      summarise(
        across(.cols = contains("pred"), .fns = ~ mean(., na.rm = T))
      )

    names(copula_preds_averages) <-
      names(copula_preds_averages) %>%
      map(~ paste0(.x, "_mean")) %>%
      unlist() %>%
      as.character()

    copula_preds_sd <-
      copula_preds%>%
      filter(if_all( everything(), ~ !is.nan(.) & !is.infinite(.) ) ) %>%
      summarise(
        across(.cols = contains("pred"), .fns = ~ sd(., na.rm = T))
      )

    names(copula_preds_sd) <-
      names(copula_preds_sd) %>%
      map(~ paste0(.x, "_sd")) %>%
      unlist() %>%
      as.character()

    technical_data <-
      create_technical_indicators(asset_data = asset_data_internal) %>%
      dplyr::select(-Price, -Low, -High, -Open)

    technical_data <-
      technical_data %>%
      filter(Asset == Asset_of_interest) %>%
      arrange(Date) %>%
      mutate(
        across(.cols = !contains("Date"),
               .fns = ~ lag(.) )
      )

    technical_data_train <-
      technical_data %>%
      filter(Date <= date_train_end)

    prepare_technical_model(
      technical_data = technical_data_train,
      actual_wins_losses = actual_wins_losses,
      Asset_of_interest = Asset_of_interest,
      date_limit = date_train_end,
      stop_value_var = stop_value_var,
      profit_value_var = profit_value_var,
      period_var = period_var,
      bin_var_col = bin_var_col,
      trade_direction = trade_direction,
      save_path = save_path
    )

    technical_phase_2_data <-
      technical_data %>%
      filter(Date < date_train_phase_2_end)

    technical_preds <-
      single_asset_read_models_and_get_pred(
        pred_data = technical_phase_2_data,
        trade_direction = trade_direction,
        Asset_of_interest = Asset_of_interest,
        save_path = save_path,
        model_string = "_technical_"
      )

    technical_preds_averages <-
      technical_preds %>%
      filter(if_all( everything(), ~ !is.nan(.) & !is.infinite(.) ) ) %>%
      summarise(
        across(.cols = contains("pred"), .fns = ~ mean(., na.rm = T))
      )

    names(technical_preds_averages) <-
      names(technical_preds_averages) %>%
      map(~ paste0(.x, "_mean")) %>%
      unlist() %>%
      as.character()

    technical_preds_sd <-
      technical_preds%>%
      filter(if_all( everything(), ~ !is.nan(.) & !is.infinite(.) ) ) %>%
      summarise(
        across(.cols = contains("pred"), .fns = ~ sd(., na.rm = T))
      )

    names(technical_preds_sd) <-
      names(technical_preds_sd) %>%
      map(~ paste0(.x, "_sd")) %>%
      unlist() %>%
      as.character()

    accumulating_probs <-
      asset_data_internal %>%
      distinct(Date, Asset) %>%
      left_join(macro_preds) %>%
      left_join(index_preds) %>%
      left_join(daily_preds) %>%
      left_join(copula_preds) %>%
      left_join(technical_preds) %>%
      left_join(macro_data) %>%
      left_join(copula_data)%>%
      left_join(
        daily_data_for_modelling %>%
          dplyr::select(Date, Asset,
                        (contains("perc_line_")&contains("_20")),
                        contains("Support"),
                        contains("Resistance"),
                        contains("Bear"),
                        contains("Bull"),
                        (contains("perc_line_")&contains("_50")),
                        contains("moving_average_markov_")
                        )
        ) %>%
      left_join(index_pca_data) %>%
      left_join(
        technical_data %>%
          dplyr::select(Date, Asset,
                        (contains("perc_line_")&contains("_500")),
                        contains("Support"),
                        contains("Resistance"),
                        contains("Bear"),
                        contains("Bull"),
                        (contains("perc_line_")&contains("_500")),
                        contains("moving_average_markov_")
          )
      ) %>%
    distinct()

    rm(macro_data, macro_phase_2_data, macro_train_data)
    rm(index_pca_data, index_pca_phase_2_data, index_pca_train_data)
    rm(daily_phase_2_data, daily_data_for_modelling, daily_data_for_modelling_train)
    rm(copula_data, copula_data_train, copula_phase_2_data)
    rm(technical_data, technical_data_train, technical_phase_2_data)
    gc()

    rm(macro_preds ,
       index_preds ,
       daily_preds ,
       copula_preds,
       technical_preds)

    gc()

    combined_model_data <-
      accumulating_probs %>%
      filter(Date < date_train_phase_2_end) %>%
      arrange(Date) %>%
      fill(!contains("Date"), .direction = "down") %>%
      filter(if_all(.cols = everything(), ~!is.na(.)))

    prepare_combined_model(
      combined_model_data = combined_model_data,
      actual_wins_losses = actual_wins_losses,
      Asset_of_interest = Asset_of_interest,
      date_limit = date_train_end,
      stop_value_var = stop_value_var,
      profit_value_var = profit_value_var,
      period_var = period_var,
      bin_var_col = bin_var_col,
      trade_direction = trade_direction,
      save_path = save_path
    )

    combined_preds <-
      single_asset_read_models_and_get_pred(
        pred_data = combined_model_data,
        trade_direction = trade_direction,
        Asset_of_interest = Asset_of_interest,
        save_path = save_path,
        model_string = "_combined_"
      )

    combined_preds_averages <-
      combined_preds %>%
      summarise(
        across(.cols = contains("pred"), .fns = ~ mean(., na.rm = T))
      )

    names(combined_preds_averages) <-
      names(combined_preds_averages) %>%
      map(~ paste0(.x, "_mean")) %>%
      unlist() %>%
      as.character()

    combined_preds_sd <-
      combined_preds%>%
      summarise(
        across(.cols = contains("pred"), .fns = ~ sd(., na.rm = T))
      )

    names(combined_preds_sd) <-
      names(combined_preds_sd) %>%
      map(~ paste0(.x, "_sd")) %>%
      unlist() %>%
      as.character()

    mean_sd_values <-
      asset_data_internal %>%
      distinct(Date, Asset) %>%
      bind_cols(macro_preds_averages) %>%
      bind_cols(macro_preds_sd) %>%
      bind_cols(index_preds_averages) %>%
      bind_cols(index_preds_sd) %>%
      bind_cols(daily_preds_averages) %>%
      bind_cols(daily_preds_sd) %>%
      bind_cols(copula_preds_averages) %>%
      bind_cols(copula_preds_sd) %>%
      bind_cols(combined_preds_averages) %>%
      bind_cols(combined_preds_sd) %>%
      bind_cols(technical_preds_averages) %>%
      bind_cols(technical_preds_sd)

    saveRDS(mean_sd_values,
            file =
              glue::glue("{save_path}/{Asset_of_interest}_{trade_direction}_mean_sd_values.RDS")
    )

  }

#' single_asset_Logit_indicator_adv_get_preds
#'
#' @param asset_data
#' @param All_Daily_Data
#' @param Asset_of_interest
#' @param actual_wins_losses
#' @param interest_rates
#' @param cpi_data
#' @param sentiment_index
#' @param equity_index
#' @param gold_index
#' @param silver_index
#' @param bonds_index
#' @param USD_index
#' @param EUR_index
#' @param GBP_index
#' @param AUD_index
#' @param countries_for_int_strength
#' @param date_train_end
#' @param date_train_phase_2_end
#' @param date_test_start
#' @param couplua_assets
#' @param stop_value_var
#' @param profit_value_var
#' @param period_var
#' @param bin_var_col
#' @param trade_direction
#' @param save_path
#'
#' @returns
#' @export
#'
#' @examples
single_asset_Logit_indicator_adv_get_preds <-
  function(
    asset_data = Indices_Metals_Bonds[[1]],
    All_Daily_Data = All_Daily_Data,
    Asset_of_interest = "EUR_USD",
    actual_wins_losses = actual_wins_losses,
    interest_rates = interest_rates,
    cpi_data = cpi_data,
    sentiment_index = sentiment_index,
    gdp_data = gdp_data,
    unemp_data = unemp_data,
    manufac_pmi = manufac_pmi,
    USD_Macro = USD_Macro,
    EUR_Macro = EUR_Macro,

    equity_index = equity_index,
    gold_index = gold_index,
    silver_index = silver_index,
    bonds_index = bonds_index,
    USD_index = USD_index,
    EUR_index = EUR_index,
    GBP_index = GBP_index,
    AUD_index = AUD_index,
    COMMOD_index = COMMOD_index,
    USD_STOCKS_index = USD_STOCKS_index,
    NZD_index = NZD_index,

    countries_for_int_strength = countries_for_int_strength,
    date_train_end = post_train_date_start,
    date_train_phase_2_end = post_train_date_start + months(6),
    date_test_start = post_train_date_start + months(7),
    couplua_assets = couplua_assets,
    stop_value_var = stop_value_var,
    profit_value_var = profit_value_var,
    period_var = period_var,
    bin_var_col = c("period_return_20_Price", "period_return_35_Price"),
    trade_direction = "Long",
    save_path = "C:/Users/nikhi/Documents/trade_data/single_asset_models_v2_adv"
  ) {


    asset_data_internal <-
      asset_data %>%
      filter(Asset == Asset_of_interest)

    macro_data <-
      prepare_macro_indicator_model_data(
        asset_data = asset_data_internal,
        Asset_of_interest = Asset_of_interest,
        interest_rates = interest_rates,
        cpi_data = cpi_data,
        sentiment_index = sentiment_index,
        gdp_data = gdp_data,
        unemp_data = unemp_data,
        manufac_pmi = manufac_pmi,
        USD_Macro = USD_Macro,
        EUR_Macro = EUR_Macro,
        countries_for_int_strength = countries_for_int_strength,
        date_limit = now(tzone = "Australia/Canberra")
      )

    macro_preds <-
      single_asset_read_models_and_get_pred(
        pred_data = macro_data,
        trade_direction = trade_direction,
        Asset_of_interest = Asset_of_interest,
        save_path = save_path,
        model_string = "_macro_"
      )

    index_pca_data <-
      get_pca_index_indicator_data(
        asset_data = asset_data_internal,
        Asset_of_interest = Asset_of_interest,
        equity_index = equity_index,
        gold_index = gold_index,
        silver_index = silver_index,
        bonds_index = bonds_index,
        USD_index = USD_index,
        EUR_index = EUR_index,
        GBP_index = GBP_index,
        AUD_index = AUD_index,
        COMMOD_index = COMMOD_index,
        USD_STOCKS_index = USD_STOCKS_index,
        NZD_index = NZD_index,
        date_limit = now(tzone = "Australia/Canberra")
      )

    index_preds <-
      single_asset_read_models_and_get_pred(
        pred_data = index_pca_data,
        trade_direction = trade_direction,
        Asset_of_interest = Asset_of_interest,
        save_path = save_path,
        model_string = "_index_"
      )

    daily_data_for_modelling <-
      prepare_daily_indicator_data(
        asset_data = asset_data_internal,
        All_Daily_Data = All_Daily_Data,
        Asset_of_interest = Asset_of_interest,
        date_limit = now(tzone = "Australia/Canberra")
      ) %>%
      filter(Asset == Asset_of_interest) %>%
      dplyr::select(-Asset)

    daily_preds <-
      single_asset_read_models_and_get_pred(
        pred_data = daily_data_for_modelling,
        trade_direction = trade_direction,
        Asset_of_interest = Asset_of_interest,
        save_path = save_path,
        model_string = "_daily_"
      )

    copula_data <-
      prepare_copula_data(
        asset_data = asset_data,
        couplua_assets = couplua_assets,
        Asset_of_interest = Asset_of_interest
      )

    copula_preds <-
      single_asset_read_models_and_get_pred(
        pred_data = copula_data,
        trade_direction = trade_direction,
        Asset_of_interest = Asset_of_interest,
        save_path = save_path,
        model_string = "_copula_"
      )

    technical_data <-
      create_technical_indicators(asset_data = asset_data_internal) %>%
      dplyr::select(-Price, -Low, -High, -Open)

    technical_data <-
      technical_data %>%
      filter(Asset == Asset_of_interest) %>%
      arrange(Date) %>%
      mutate(
        across(.cols = !contains("Date"),
               .fns = ~ lag(.) )
      ) %>%
      ungroup() %>%
      dplyr::select(-Asset)

    technical_preds <-
      single_asset_read_models_and_get_pred(
        pred_data = technical_data,
        trade_direction = trade_direction,
        Asset_of_interest = Asset_of_interest,
        save_path = save_path,
        model_string = "_technical_"
      )

    accumulating_probs <-
      asset_data_internal %>%
      distinct(Date, Asset) %>%
      left_join(macro_preds) %>%
      left_join(index_preds) %>%
      left_join(daily_preds) %>%
      left_join(copula_preds)%>%
      left_join(technical_preds) %>%
      left_join(macro_data) %>%
      left_join(copula_data)%>%
      left_join(
        daily_data_for_modelling %>%
          dplyr::select(Date,
                        (contains("perc_line_")&contains("_20")),
                        contains("Support"),
                        contains("Resistance"),
                        contains("Bear"),
                        contains("Bull"),
                        (contains("perc_line_")&contains("_50")),
                        contains("moving_average_markov_")
          )
      ) %>%
      left_join(index_pca_data) %>%
      left_join(
        technical_data %>%
          dplyr::select(Date,
                        (contains("perc_line_")&contains("_500")),
                        contains("Support"),
                        contains("Resistance"),
                        contains("Bear"),
                        contains("Bull"),
                        (contains("perc_line_")&contains("_500")),
                        contains("moving_average_markov_")
          )
      ) %>%
      distinct()

    combined_model_data <-
      accumulating_probs %>%
      ungroup() %>%
      arrange(Date) %>%
      fill(!contains("Date"), .direction = "down") %>%
      filter(if_all(.cols = everything(), ~!is.na(.)))

    mean_sd_values <-
      readRDS(
        glue::glue("{save_path}/{Asset_of_interest}_{trade_direction}_mean_sd_values.RDS")
      ) %>%
      dplyr::select(-Asset_sd, -Asset_mean) %>%
      dplyr::select(-Date) %>%
      distinct() %>%
      group_by(Asset) %>%
      summarise(across(.cols = where(is.numeric),
                       .fns = ~ mean(., na.rm = T))) %>%
      ungroup()


    combined_preds <-
      single_asset_read_models_and_get_pred(
        pred_data = combined_model_data,
        trade_direction = trade_direction,
        Asset_of_interest = Asset_of_interest,
        save_path = save_path,
        model_string = "_combined_"
      ) %>%
      mutate(
        Asset = Asset_of_interest
      ) %>%
      left_join(
        accumulating_probs %>%
          dplyr::select(Date, Asset, contains("pred"))
      ) %>%
      left_join(mean_sd_values)

    rm(macro_data, macro_phase_2_data, macro_train_data)
    rm(index_pca_data, index_pca_phase_2_data, index_pca_train_data)
    rm(daily_phase_2_data, daily_data_for_modelling, daily_data_for_modelling_train)
    rm(copula_data, copula_data_train, copula_phase_2_data)
    rm(technical_data, technical_data_train, technical_phase_2_data)
    gc()

    return(combined_preds)

  }

#' single_asset_read_models_and_get_pred
#'
#' @param pred_data
#' @param trade_direction
#' @param Asset_of_interest
#' @param save_path
#' @param model_string
#'
#' @returns
#' @export
#'
#' @examples
single_asset_read_models_and_get_pred <-
  function(
    pred_data = macro_test_data,
    trade_direction = "Long",
    Asset_of_interest = "EUR_USD",
    save_path = "C:/Users/nikhi/Documents/trade_data/single_asset_models_v2_adv",
    model_string = "_macro_"
  ) {

    models_in_path <-
      fs::dir_info(save_path) %>%
      filter(str_detect(path, Asset_of_interest),
             str_detect(path, trade_direction),
             str_detect(path, model_string)) %>%
      pull(path)

    model_preds <- pred_data %>% dplyr::select(Date)

    for (i in 1:length(models_in_path)) {

      model_object <-
        readRDS(models_in_path[i])

      if(str_detect(models_in_path[i], "logit")) {
        preds <-
          predict.glm(object = model_object, newdata = pred_data, type = "response")
      }

      if(str_detect(models_in_path[i], "lin")) {
        preds <-
          predict.lm(object = model_object, newdata = pred_data, type = "response")
      }

      model_preds <-
        model_preds %>%
        mutate(
          !!as.name( glue::glue("pred{model_string}{i}") ) := preds
        )

    }

    return(model_preds)

  }


#' prepare_macro_indicator_model_data
#'
#' @param asset_data
#' @param Asset_of_interest
#' @param interest_rates
#' @param cpi_data
#' @param sentiment_index
#' @param countries_for_int_strength
#' @param date_limit
#'
#' @returns
#' @export
#'
#' @examples
prepare_macro_indicator_model_data <-
  function(
    asset_data = Indices_Metals_Bonds[[1]],
    raw_macro_data = raw_macro_data,
    Asset_of_interest = "EUR_USD",
    interest_rates = interest_rates,
    cpi_data = cpi_data,
    gdp_data = gdp_data,
    unemp_data = unemp_data,
    manufac_pmi = manufac_pmi,
    USD_Macro = USD_Macro,
    EUR_Macro = EUR_Macro,
    sentiment_index = sentiment_index,
    countries_for_int_strength = countries_for_int_strength,
    date_limit = post_train_date_start
  ) {

    internal_asset_data <-
      asset_data %>%
      filter(Asset == Asset_of_interest) %>%
      filter(Date <= date_limit)

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

    gdp_data_transform <-
      gdp_data %>%
      mutate(Date_for_Join = date) %>%
      dplyr::select(-date)

    unemp_data_transform <-
      unemp_data %>%
      mutate(Date_for_Join = date) %>%
      dplyr::select(-date)

    manufac_pmi_transform <-
      manufac_pmi %>%
      mutate(Date_for_Join = date) %>%
      dplyr::select(-date)

    USD_Macro <-
      USD_Macro %>%
      mutate(Date_for_Join = date) %>%
      dplyr::select(-date)

    EUR_Macro <-
      EUR_Macro %>%
      mutate(Date_for_Join = date) %>%
      dplyr::select(-date)

    macro_for_join <-
      internal_asset_data %>%
      distinct(Date) %>%
      mutate(Date_for_Join = as_date(Date)) %>%
      arrange(Date) %>%
      left_join(CPI_strength_index) %>%
      left_join(interest_rate_strength_Index) %>%
      left_join(sentiment_index %>% mutate(Date_for_Join = Date)) %>%
      left_join(gdp_data_transform) %>%
      left_join(unemp_data_transform) %>%
      left_join(manufac_pmi_transform) %>%
      left_join(USD_Macro) %>%
      left_join(EUR_Macro) %>%
      dplyr::select(-Date_for_Join) %>%
      arrange(Date) %>%
      mutate(
        across(.cols = !contains("Date"),
               .fns = ~ lag(.))
      ) %>%
      fill(!contains("Date"), .direction = "down") %>%
      filter(if_all(everything(), ~ !is.na(.))) %>%
      distinct()

    return(macro_for_join)

  }


#' prepare_macro_indicator_model
#'
#' @param macro_for_join
#' @param actual_wins_losses
#' @param Asset_of_interest
#' @param date_limit
#' @param stop_value_var
#' @param profit_value_var
#' @param period_var
#' @param bin_var_col
#' @param trade_direction
#' @param save_path
#'
#' @returns
#' @export
#'
#' @examples
prepare_macro_indicator_model <-
  function(macro_for_join = macro_train_data,
           actual_wins_losses = actual_wins_losses,
           Asset_of_interest = "EUR_USD",
           date_limit = post_train_date_start,
           stop_value_var = stop_value_var,
           profit_value_var = profit_value_var,
           period_var = period_var,
           bin_var_col = c("period_return_20_Price", "period_return_35_Price"),
           trade_direction = "Long",
           save_path = "C:/Users/nikhi/Documents/trade_data/single_asset_models_v2_adv"
  ) {

    actual_wins_losses_raw <-
      actual_wins_losses

    for (i in 1:length(bin_var_col)) {

      actual_wins_losses <-
        actual_wins_losses_raw %>%
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
              !!as.name(bin_var_col[i]) > 0 & trade_col == "Short" ~ "win",
              !!as.name(bin_var_col[i]) <= 0 & trade_col == "Short" ~ "loss",

              !!as.name(bin_var_col[i]) > 0 & trade_col == "Long" ~ "win",
              !!as.name(bin_var_col[i]) <= 0 & trade_col == "Long" ~ "loss"

            )
        ) %>%
        filter(Date <= date_limit)

      check_date <-
        macro_for_join %>% pull(Date) %>% max() %>% as_date()

      check_date <- check_date <= date_limit

      message(glue::glue("Data Date is less than Train Date Max: {check_date}"))

      macro_for_join_model <-
        actual_wins_losses %>%
        dplyr::select(Date, Asset ,bin_var, matches(bin_var_col) ) %>%
        filter(
          Asset == Asset_of_interest
        ) %>%
        left_join(macro_for_join) %>%
        fill(!contains("Date"), .direction = "down") %>%
        filter(if_all(everything(),~!is.na(.))) %>%
        filter(Date <= date_limit)

      # rm(actual_wins_losses)

      macro_vars_for_indicator <-
        names(macro_for_join_model) %>%
        keep(~ !str_detect(.x, "Date") &
               !str_detect(.x, "bin_var") &
               !str_detect(.x, "Asset") &
               !str_detect(.x, paste(bin_var_col, collapse = "|") ) &
               !str_detect(.x, "period_return_")
        ) %>%
        unlist() %>%
        as.character()

      macro_indicator_formula_logit <-
        create_lm_formula(dependant = "bin_var=='win'",
                          independant = macro_vars_for_indicator)

      macro_indicator_model <-
        glm(formula = macro_indicator_formula_logit,
            data = macro_for_join_model,
            family = binomial("logit"))

      sig_coefs <-
        get_sig_coefs(model_object_of_interest = macro_indicator_model,
                      # p_value_thresh_for_inputs = 0.25
                      p_value_thresh_for_inputs = 0.9
                      )

      rm(macro_indicator_model)
      gc()

      macro_indicator_formula_logit <-
        create_lm_formula(dependant = "bin_var=='win'",
                          independant = sig_coefs)

      macro_indicator_model <-
        glm(formula = macro_indicator_formula_logit,
            data = macro_for_join_model,
            family = binomial("logit"))

      summary(macro_indicator_model)

      message(glue::glue("Passed Macro Model {Asset_of_interest} {i}"))

      saveRDS(object = macro_indicator_model,
              file =
                glue::glue("{save_path}/{Asset_of_interest}_{trade_direction}_{bin_var_col[i]}_macro_logit.RDS")
      )

      rm(macro_indicator_model)
      gc()

      macro_indicator_formula_lin <-
        create_lm_formula(dependant = bin_var_col[i],
                          independant = macro_vars_for_indicator)

      macro_indicator_model_lin <-
        lm(formula = macro_indicator_formula_lin,
           data = macro_for_join_model)

      sig_coefs <-
        get_sig_coefs(model_object_of_interest = macro_indicator_model_lin,
                      # p_value_thresh_for_inputs = 0.25
                      p_value_thresh_for_inputs = 0.9
                      )

      macro_indicator_formula_lin <-
        create_lm_formula(dependant = bin_var_col[i],
                          independant = sig_coefs)

      macro_indicator_model_lin <-
        lm(formula = macro_indicator_formula_lin,
           data = macro_for_join_model)

      summary(macro_indicator_model_lin)

      message(glue::glue("Passed Macro Model Linear {Asset_of_interest} {i}"))

      saveRDS(object = macro_indicator_model_lin,
              file =
                glue::glue("{save_path}/{Asset_of_interest}_{trade_direction}_{bin_var_col[i]}_macro_lin.RDS")
      )

      rm(macro_indicator_model_lin)
      gc()

    }

    rm(actual_wins_losses)
    rm(actual_wins_losses_raw)

    gc()

  }

#' get_bonds_index
#'
#' @param index_data
#'
#' @return
#' @export
#'
#' @examples
get_USD_index_for_models <-
  function(index_data) {

    USD_assets <-
      index_data %>%
      distinct(Asset) %>%
      filter(str_detect(Asset, "AUD_USD|EUR_USD|XAU_USD|GBP_USD|USD_JPY|NZD_USD|USD_CHF|USD_SEK|USD_NOK")) %>%
      pull(Asset)

    major_USD_log_cumulative <-
      USD_assets %>%
      map_dfr(
        ~
          create_log_cumulative_returns(
            asset_data_to_use =
              index_data %>%
              filter(Asset %in% USD_assets),
            asset_to_use = c(.x[1]),
            price_col = "Open",
            return_long_format = TRUE
          )
      ) %>%
      left_join(
        index_data %>%
          filter(Asset %in% USD_assets) %>%
          dplyr::select(Date, Asset, Price, Open)
      )

    pc_USD_global <-
      create_PCA_Asset_Index(
        asset_data_to_use = major_USD_log_cumulative %>%
          group_by(Asset) %>%
          mutate(
            Return_Index_Diff = ((Price - Open)/Open)*100
          ) %>%
          ungroup() %>%
          filter(!is.na(Return_Index_Diff)),
        asset_to_use =  USD_assets,
        price_col = "Return_Index_Diff",
        scale_values = TRUE
      ) %>%
      arrange(Date) %>%
      mutate(
        across(contains("PC[0-9]"), ~cumsum(.))
      ) %>%
      rename(PC1_USD = PC1,
             PC2_USD = PC2,
             PC3_USD = PC3,
             PC4_USD = PC4) %>%
      dplyr::select(Date, matches("PC[0-9]_USD"))

    rm(major_USD_log_cumulative)
    gc()

    return(pc_USD_global)

  }

#' get_bonds_index
#'
#' @param index_data
#'
#' @return
#' @export
#'
#' @examples
get_EUR_index_for_models <-
  function(index_data) {

    EUR_assets <-
      index_data %>%
      distinct(Asset) %>%
      filter(str_detect(Asset, "EUR_AUD|EUR_USD|XAU_EUR|EUR_GBP|EUR_NZD|EUR_CHF|XAG_EUR|EUR_SEK|EUR_JPY|USD_JPY|EU50_EUR")) %>%
      pull(Asset)

    major_EUR_log_cumulative <-
      EUR_assets %>%
      map_dfr(
        ~
          create_log_cumulative_returns(
            asset_data_to_use =
              index_data %>%
              filter(Asset %in% EUR_assets),
            asset_to_use = c(.x[1]),
            price_col = "Open",
            return_long_format = TRUE
          )
      ) %>%
      left_join(
        index_data %>%
          filter(Asset %in% EUR_assets) %>%
          dplyr::select(Date, Asset, Price, Open)
      )

    pc_EUR_global <-
      create_PCA_Asset_Index(
        asset_data_to_use = major_EUR_log_cumulative %>%
          group_by(Asset) %>%
          mutate(
            Return_Index_Diff = ((Price - Open)/Open)*100
          ) %>%
          ungroup() %>%
          filter(!is.na(Return_Index_Diff)),
        asset_to_use =  EUR_assets,
        price_col = "Return_Index_Diff",
        scale_values = TRUE
      ) %>%
      arrange(Date) %>%
      mutate(
        across(contains("PC[0-9]"), ~cumsum(.))
      ) %>%
      rename(PC1_EUR = PC1,
             PC2_EUR = PC2,
             PC3_EUR = PC3,
             PC4_EUR = PC4) %>%
      dplyr::select(Date, matches("PC[0-9]_EUR"))

    rm(major_EUR_log_cumulative)
    gc()

    return(pc_EUR_global)

  }

#' get_bonds_index
#'
#' @param index_data
#'
#' @return
#' @export
#'
#' @examples
get_GBP_index_for_models <-
  function(index_data) {

    GBP_assets <-
      index_data %>%
      distinct(Asset) %>%
      filter(str_detect(Asset, "GBP_AUD|GBP_NZD|XAU_GBP|EUR_GBP|GBP_CHF|UK100_GBP|XAG_GBP|GBP_JPY|GBP_USD|GBP_CAD|USD_JPY")) %>%
      pull(Asset)

    major_GBP_log_cumulative <-
      GBP_assets %>%
      map_dfr(
        ~
          create_log_cumulative_returns(
            asset_data_to_use =
              index_data %>%
              filter(Asset %in% GBP_assets),
            asset_to_use = c(.x[1]),
            price_col = "Open",
            return_long_format = TRUE
          )
      ) %>%
      left_join(
        index_data %>%
          filter(Asset %in% GBP_assets) %>%
          dplyr::select(Date, Asset, Price, Open)
      )

    pc_GBP_global <-
      create_PCA_Asset_Index(
        asset_data_to_use = major_GBP_log_cumulative %>%
          group_by(Asset) %>%
          mutate(
            Return_Index_Diff = ((Price - Open)/Open)*100
          ) %>%
          ungroup() %>%
          filter(!is.na(Return_Index_Diff)),
        asset_to_use =  GBP_assets,
        price_col = "Return_Index_Diff",
        scale_values = TRUE
      ) %>%
      arrange(Date) %>%
      mutate(
        across(contains("PC[0-9]"), ~cumsum(.))
      ) %>%
      rename(PC1_GBP = PC1,
             PC2_GBP = PC2,
             PC3_GBP = PC3,
             PC4_GBP = PC4) %>%
      dplyr::select(Date, matches("PC[0-9]_GBP"))

    rm(major_GBP_log_cumulative)
    gc()

    return(pc_GBP_global)

  }

#' get_bonds_index
#'
#' @param index_data
#'
#' @return
#' @export
#'
#' @examples
get_AUD_index_for_models <-
  function(index_data) {

    AUD_assets <-
      index_data %>%
      distinct(Asset) %>%
      filter(str_detect(Asset, "GBP_AUD|XAU_AUD|EUR_AUD|AUD_USD|AU200_AUD|AUD_JPY|XAG_AUD|USD_JPY|NZD_USD")) %>%
      pull(Asset)

    major_AUD_log_cumulative <-
      AUD_assets %>%
      map_dfr(
        ~
          create_log_cumulative_returns(
            asset_data_to_use =
              index_data %>%
              filter(Asset %in% AUD_assets),
            asset_to_use = c(.x[1]),
            price_col = "Open",
            return_long_format = TRUE
          )
      ) %>%
      left_join(
        index_data %>%
          filter(Asset %in% AUD_assets) %>%
          dplyr::select(Date, Asset, Price, Open)
      )

    pc_AUD_global <-
      create_PCA_Asset_Index(
        asset_data_to_use = major_AUD_log_cumulative %>%
          group_by(Asset) %>%
          mutate(
            Return_Index_Diff = ((Price - Open)/Open)*100
          ) %>%
          ungroup() %>%
          filter(!is.na(Return_Index_Diff)),
        asset_to_use =  AUD_assets,
        price_col = "Return_Index_Diff",
        scale_values = TRUE
      ) %>%
      arrange(Date) %>%
      mutate(
        across(contains("PC[0-9]"), ~cumsum(.))
      ) %>%
      rename(PC1_AUD = PC1,
             PC2_AUD = PC2,
             PC3_AUD = PC3,
             PC4_AUD = PC4) %>%
      dplyr::select(Date, matches("PC[0-9]_AUD"))

    rm(major_AUD_log_cumulative)
    gc()

    return(pc_AUD_global)

  }

#' get_bonds_index
#'
#' @param index_data
#'
#' @return
#' @export
#'
#' @examples
get_NZD_index_for_models <-
  function(index_data) {

    NZD_assets <-
      index_data %>%
      distinct(Asset) %>%
      filter(str_detect(Asset, "NZD_USD|EUR_NZD|GBP_NZD|AUD_USD|XAG_NZD|XAU_AUD|EUR_AUD|GBP_AUD|AUD_USD")) %>%
      pull(Asset)

    major_NZD_log_cumulative <-
      NZD_assets %>%
      map_dfr(
        ~
          create_log_cumulative_returns(
            asset_data_to_use =
              index_data %>%
              filter(Asset %in% NZD_assets),
            asset_to_use = c(.x[1]),
            price_col = "Open",
            return_long_format = TRUE
          )
      ) %>%
      left_join(
        index_data %>%
          filter(Asset %in% NZD_assets) %>%
          dplyr::select(Date, Asset, Price, Open)
      )

    pc_NZD_global <-
      create_PCA_Asset_Index(
        asset_data_to_use = major_NZD_log_cumulative %>%
          group_by(Asset) %>%
          mutate(
            Return_Index_Diff = ((Price - Open)/Open)*100
          ) %>%
          ungroup() %>%
          filter(!is.na(Return_Index_Diff)),
        asset_to_use =  NZD_assets,
        price_col = "Return_Index_Diff",
        scale_values = TRUE
      ) %>%
      arrange(Date) %>%
      mutate(
        across(contains("PC[0-9]"), ~cumsum(.))
      ) %>%
      rename(PC1_NZD = PC1,
             PC2_NZD = PC2,
             PC3_NZD = PC3,
             PC4_NZD = PC4) %>%
      dplyr::select(Date, matches("PC[0-9]_NZD"))

    rm(major_NZD_log_cumulative, NZD_assets)
    gc()

    return(pc_NZD_global)

  }

#' get_bonds_index
#'
#' @param index_data
#'
#' @return
#' @export
#'
#' @examples
get_USD_AND_STOCKS_index_for_models <-
  function(index_data) {

    USD_STOCKS_assets <-
      index_data %>%
      distinct(Asset) %>%
      filter(str_detect(Asset, "US2000_USD|SPX500_USD|EU50_EUR|SG30_SGD|AU200_AUD|UK100_GBP|HK33_HKD|FR40_EUR|CH20_CHF|EUR_USD|GBP_USD|AUD_USD|NZD_USD|USD_SGD|USD_CAD")) %>%
      pull(Asset)

    major_USD_STOCKS_log_cumulative <-
      USD_STOCKS_assets %>%
      map_dfr(
        ~
          create_log_cumulative_returns(
            asset_data_to_use =
              index_data %>%
              filter(Asset %in% USD_STOCKS_assets),
            asset_to_use = c(.x[1]),
            price_col = "Open",
            return_long_format = TRUE
          )
      ) %>%
      left_join(
        index_data %>%
          filter(Asset %in% USD_STOCKS_assets) %>%
          dplyr::select(Date, Asset, Price, Open)
      )

    pc_USD_STOCKS_global <-
      create_PCA_Asset_Index(
        asset_data_to_use = major_USD_STOCKS_log_cumulative %>%
          group_by(Asset) %>%
          mutate(
            Return_Index_Diff = ((Price - Open)/Open)*100
          ) %>%
          ungroup() %>%
          filter(!is.na(Return_Index_Diff)),
        asset_to_use =  USD_STOCKS_assets,
        price_col = "Return_Index_Diff",
        scale_values = TRUE
      ) %>%
      arrange(Date) %>%
      mutate(
        across(contains("PC[0-9]"), ~cumsum(.))
      ) %>%
      rename(PC1_USD_STOCKS = PC1,
             PC2_USD_STOCKS = PC2,
             PC3_USD_STOCKS = PC3,
             PC4_USD_STOCKS = PC4) %>%
      dplyr::select(Date, matches("PC[0-9]_USD_STOCKS"))

    rm(major_USD_STOCKS_log_cumulative, USD_STOCKS_assets)
    gc()

    return(pc_USD_STOCKS_global)

  }

#' get_bonds_index
#'
#' @param index_data
#'
#' @return
#' @export
#'
#' @examples
get_COMMOD_index_for_models <-
  function(index_data) {

    COMMOD_assets <-
      index_data %>%
      distinct(Asset) %>%
      filter(str_detect(Asset, "WTICO_USD|BCO_USD|XCU_USD|NATGAS_USD|XAU_USD|XAG_USD")) %>%
      pull(Asset)

    major_COMMOD_log_cumulative <-
      COMMOD_assets %>%
      map_dfr(
        ~
          create_log_cumulative_returns(
            asset_data_to_use =
              index_data %>%
              filter(Asset %in% COMMOD_assets),
            asset_to_use = c(.x[1]),
            price_col = "Open",
            return_long_format = TRUE
          )
      ) %>%
      left_join(
        index_data %>%
          filter(Asset %in% COMMOD_assets) %>%
          dplyr::select(Date, Asset, Price, Open)
      )

    pc_COMMOD_global <-
      create_PCA_Asset_Index(
        asset_data_to_use = major_COMMOD_log_cumulative %>%
          group_by(Asset) %>%
          mutate(
            Return_Index_Diff = ((Price - Open)/Open)*100
          ) %>%
          ungroup() %>%
          filter(!is.na(Return_Index_Diff)),
        asset_to_use =  COMMOD_assets,
        price_col = "Return_Index_Diff",
        scale_values = TRUE
      ) %>%
      arrange(Date) %>%
      mutate(
        across(contains("PC[0-9]"), ~cumsum(.))
      ) %>%
      rename(PC1_COMMOD = PC1,
             PC2_COMMOD = PC2,
             PC3_COMMOD = PC3,
             PC4_COMMOD = PC4) %>%
      dplyr::select(Date, matches("PC[0-9]_COMMOD"))

    rm(major_COMMOD_log_cumulative, COMMOD_assets)
    gc()

    return(pc_COMMOD_global)

  }

#' get_pca_index_indicator_data
#'
#' @param asset_data
#' @param Asset_of_interest
#' @param equity_index
#' @param gold_index
#' @param silver_index
#' @param bonds_index
#' @param date_limit
#'
#' @returns
#' @export
#'
#' @examples
get_pca_index_indicator_data <-
  function(
    asset_data = Indices_Metals_Bonds[[1]],
    Asset_of_interest = "EUR_USD",
    equity_index = equity_index,
    gold_index = gold_index,
    silver_index = silver_index,
    bonds_index = bonds_index,
    USD_index = USD_index,
    EUR_index = EUR_index,
    GBP_index = GBP_index,
    AUD_index = AUD_index,
    COMMOD_index = COMMOD_index,
    USD_STOCKS_index = USD_STOCKS_index,
    NZD_index = NZD_index,

    date_limit = post_train_date_start,
    index_lag_cols = 1,
    sum_rolling_length = 30
  ) {

    internal_asset_data <-
      asset_data %>%
      filter(Asset == Asset_of_interest) %>%
      filter(Date <= date_limit)

    indexes_data_for_join <-
      internal_asset_data %>%
      ungroup() %>%
      distinct(Date) %>%
      arrange(Date) %>%
      left_join(equity_index  %>% dplyr::select(-Average_PCA) )%>%
      left_join(gold_index  %>% dplyr::select(-Average_PCA) )%>%
      left_join(silver_index  %>% dplyr::select(-Average_PCA) )%>%
      left_join(bonds_index  %>% dplyr::select(-Average_PCA) ) %>%
      left_join(USD_index) %>%
      left_join(EUR_index) %>%
      left_join(GBP_index) %>%
      left_join(AUD_index) %>%
      left_join(COMMOD_index) %>%
      left_join(USD_STOCKS_index) %>%
      left_join(NZD_index) %>%
      fill(!contains("Date"), .direction = "down") %>%
      filter(if_all(everything(), ~ !is.na(.))) %>%
      mutate(
        across(.cols = !contains("Date"), .fns = ~ lag(.))
      ) %>%
      filter(if_all(everything(), ~ !is.na(.))) %>%
      distinct()

    rolling_sums <-
      indexes_data_for_join %>%
      arrange(Date) %>%
      mutate(
        across(.cols = !contains("Date"),
               .fns = ~ slider::slide_dbl(.x = .,
                                          .f = ~ sum(.x, na.rm = T),
                                          .before = sum_rolling_length )
        )
      )

    names(rolling_sums) <-
      names(rolling_sums) %>%
      map( ~
             ifelse(
               !str_detect(.x, "Date"),
               paste0(.x, "_sum"),
               .x
             )
      ) %>%
      unlist()

    rolling_sums2 <-
      indexes_data_for_join %>%
      arrange(Date) %>%
      mutate(
        across(.cols = !contains("Date"),
               .fns = ~ slider::slide_dbl(.x = .,
                                          .f = ~ sum(.x, na.rm = T),
                                          .before = round(sum_rolling_length/2) )
        )
      )

    names(rolling_sums2) <-
      names(rolling_sums2) %>%
      map( ~
             ifelse(
               !str_detect(.x, "Date"),
               paste0(.x, "_sum_2"),
               .x
             )
      ) %>%
      unlist()

    rolling_average <-
      indexes_data_for_join %>%
      arrange(Date) %>%
      mutate(
        across(.cols = !contains("Date"),
               .fns = ~ slider::slide_dbl(.x = .,
                                          .f = ~ sum(.x, na.rm = T),
                                          .before = sum_rolling_length )
        )
      )%>%
      mutate(
        across(.cols = !contains("Date"),
               .fns = ~ slider::slide_dbl(.x = .,
                                          .f = ~ mean(.x, na.rm = T),
                                          .before = sum_rolling_length )
        )
      )

    names(rolling_average) <-
      names(rolling_average) %>%
      map( ~
             ifelse(
               !str_detect(.x, "Date"),
               paste0(.x, "_average"),
               .x
             )
      ) %>%
      unlist()

    rolling_average2 <-
      indexes_data_for_join %>%
      arrange(Date) %>%
      mutate(
        across(.cols = !contains("Date"),
               .fns = ~ slider::slide_dbl(.x = .,
                                          .f = ~ sum(.x, na.rm = T),
                                          .before = round(sum_rolling_length/2) )
        )
      )%>%
      mutate(
        across(.cols = !contains("Date"),
               .fns = ~ slider::slide_dbl(.x = .,
                                          .f = ~ mean(.x, na.rm = T),
                                          .before = round(sum_rolling_length/2) )
        )
      )

    names(rolling_average2) <-
      names(rolling_average2) %>%
      map( ~
             ifelse(
               !str_detect(.x, "Date"),
               paste0(.x, "_average_2"),
               .x
             )
      ) %>%
      unlist()

    indexes_data_for_join <-
      indexes_data_for_join %>%
      left_join(rolling_sums)%>%
      left_join(rolling_sums2)%>%
      left_join(rolling_average2) %>%
      left_join(rolling_average)

    for (k in 1:index_lag_cols) {

      indexes_data_for_join <-
        indexes_data_for_join %>%
        arrange(Date) %>%
        mutate(
          !!as.name( glue::glue("PC1_Equities_{k}_lag") ) :=
            lag(PC1_Equities, k),

          !!as.name( glue::glue("PC1_Gold_Equities_{k}_lag") ) :=
            lag(PC1_Gold_Equities, k),

          !!as.name( glue::glue("PC1_Silver_Equities_{k}_lag") ) :=
            lag(PC1_Silver_Equities, k),

          !!as.name( glue::glue("PC1_Bonds_Equities_{k}_lag") ) :=
            lag(PC1_Bonds_Equities, k),

          !!as.name( glue::glue("PC1_USD_{k}_lag") ) :=
            lag(PC1_USD, k),

          !!as.name( glue::glue("PC1_EUR_{k}_lag") ) :=
            lag(PC1_EUR, k),

          !!as.name( glue::glue("PC1_GBP_{k}_lag") ) :=
            lag(PC1_GBP, k)

        )

    }

    indexes_data_for_join <-
      indexes_data_for_join %>%
      arrange(Date) %>%
      fill(!contains("Date"), .direction = "down")


    return(indexes_data_for_join)

  }

#' Title
#'
#' @param index_pca_data
#' @param actual_wins_losses
#' @param Asset_of_interest
#' @param date_limit
#' @param stop_value_var
#' @param profit_value_var
#' @param period_var
#' @param bin_var_col
#' @param trade_direction
#' @param save_path
#'
#' @returns
#' @export
#'
#' @examples
prepare_index_indicator_model <-
  function(
    index_pca_data = index_pca_data,
    actual_wins_losses = actual_wins_losses,
    Asset_of_interest = "EUR_USD",
    date_limit = post_train_date_start,
    stop_value_var = stop_value_var,
    profit_value_var = profit_value_var,
    period_var = period_var,
    bin_var_col = c("period_return_20_Price", "period_return_35_Price"),
    trade_direction = "Long",
    save_path = "C:/Users/nikhi/Documents/trade_data/single_asset_models_v2_adv"
  ) {

    actual_wins_losses_raw <-
      actual_wins_losses

    for (i in 1:length(bin_var_col)) {

      actual_wins_losses <-
        actual_wins_losses_raw %>%
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
              !!as.name(bin_var_col[i]) > 0 & trade_col == "Short" ~ "win",
              !!as.name(bin_var_col[i]) <= 0 & trade_col == "Short" ~ "loss",

              !!as.name(bin_var_col[i]) > 0 & trade_col == "Long" ~ "win",
              !!as.name(bin_var_col[i]) <= 0 & trade_col == "Long" ~ "loss"

            )
        ) %>%
        filter(Date <= date_limit)

      index_for_join_model <-
        actual_wins_losses %>%
        dplyr::select(Date, Asset ,bin_var, matches(bin_var_col) ) %>%
        filter(
          Asset == Asset_of_interest
        ) %>%
        left_join(index_pca_data) %>%
        arrange(Date) %>%
        fill(!contains("Date"), .direction = "down") %>%
        filter(if_all(everything(),~!is.na(.))) %>%
        filter(Date <= date_limit)

      check_date <-
        index_for_join_model %>% pull(Date) %>% max() %>% as_date()

      check_date <- check_date <= date_limit

      message(glue::glue("Data Date is less than Train Date Max: {check_date}"))


      # rm(actual_wins_losses)

      index_vars_for_indicator <-
        names(index_for_join_model) %>%
        keep(~ !str_detect(.x, "Date") &
               !str_detect(.x, "bin_var") &
               !str_detect(.x, "Asset") &
               !str_detect(.x, paste(bin_var_col, collapse = "|") ) &
               !str_detect(.x, "period_return_")
        ) %>%
        unlist() %>%
        as.character()

      index_indicator_formula_logit <-
        create_lm_formula(dependant = "bin_var=='win'",
                          independant = index_vars_for_indicator)

      index_indicator_model <-
        glm(formula = index_indicator_formula_logit,
            data = index_for_join_model,
            family = binomial("logit"))

      sig_coefs <-
        get_sig_coefs(model_object_of_interest = index_indicator_model,
                      p_value_thresh_for_inputs = 0.25)

      rm(index_indicator_model)
      gc()

      index_indicator_formula_logit <-
        create_lm_formula(dependant = "bin_var=='win'",
                          independant = sig_coefs)

      index_indicator_model <-
        glm(formula = index_indicator_formula_logit,
            data = index_for_join_model,
            family = binomial("logit"))

      summary(index_indicator_model)

      message(glue::glue("Passed Index Model {Asset_of_interest} {i}"))

      saveRDS(object = index_indicator_model,
              file =
                glue::glue("{save_path}/{Asset_of_interest}_{trade_direction}_{bin_var_col[i]}_index_logit.RDS")
      )

      rm(index_indicator_model)
      gc()

      index_indicator_formula_lin <-
        create_lm_formula(dependant = bin_var_col[i],
                          independant = index_vars_for_indicator)

      index_indicator_model_lin <-
        lm(formula = index_indicator_formula_lin,
           data = index_for_join_model)

      sig_coefs <-
        get_sig_coefs(model_object_of_interest = index_indicator_model_lin,
                      p_value_thresh_for_inputs = 0.25)

      index_indicator_formula_lin <-
        create_lm_formula(dependant = bin_var_col[i],
                          independant = sig_coefs)

      index_indicator_model_lin <-
        lm(formula = index_indicator_formula_lin,
           data = index_for_join_model)

      summary(index_indicator_model_lin)

      message(glue::glue("Passed index Model Linear {Asset_of_interest} {i}"))

      saveRDS(object = index_indicator_model_lin,
              file =
                glue::glue("{save_path}/{Asset_of_interest}_{trade_direction}_{bin_var_col[i]}_index_lin.RDS")
      )

      rm(index_indicator_model_lin)
      gc()

    }

  }

#' get_daily_indicators
#'
#' @param Daily_Data
#' @param asset_data
#' @param Asset_of_interest
#'
#' @return
#' @export
#'
#' @examples
get_daily_indicators <-
  function(Daily_Data = All_Daily_Data,
           asset_data = Indices_Metals_Bonds[[1]],
           Asset_of_interest = "EUR_USD",
           return_joined_only = TRUE) {

    daily_technical_indicators <-
      Daily_Data %>%
      filter(Asset == Asset_of_interest) %>%
      create_technical_indicators_daily() %>%
      group_by(Asset) %>%
      arrange(Date, .by_group = TRUE) %>%
      group_by(Asset) %>%
      mutate(
        across(.cols = !contains("Date") & !contains("Asset"),
               .fns = ~ lag(.))
      ) %>%
      ungroup()

    new_names <- names(daily_technical_indicators) %>%
      map(
        ~ case_when(
          .x %in% c("Date","Asset", "Price", "High", "Low", "Open") ~ .x,
          TRUE ~ paste0("Daily_", .x)
        )
      ) %>%
      unlist() %>%
      as.character()

    names(daily_technical_indicators) <- new_names

    joined_dat <-
      asset_data %>%
      filter(Asset == Asset_of_interest) %>%
      mutate(
        Date_for_join = as_date(Date)
      ) %>%
      left_join(daily_technical_indicators %>%
                  mutate(Date_for_join = as_date(Date)) %>%
                  dplyr::select(-Date, -Price, -Low, -Open, -High),
                by = c("Date_for_join", "Asset")
      ) %>%
      fill(
        contains("Daily"), .direction = "down"
      ) %>%
      distinct()

    if(return_joined_only == TRUE) {
      return(joined_dat)
    } else {
      return(
        list(joined_dat,
             daily_technical_indicators)
        )
    }

  }

#' prepare_daily_indicator_data
#'
#' @param asset_data
#' @param All_Daily_Data
#' @param Asset_of_interest
#' @param equity_index
#' @param gold_index
#' @param silver_index
#' @param bonds_index
#' @param USD_index
#' @param EUR_index
#' @param GBP_index
#' @param date_limit
#'
#' @returns
#' @export
#'
#' @examples
prepare_daily_indicator_data <-
  function(
    asset_data = Indices_Metals_Bonds[[1]],
    All_Daily_Data = All_Daily_Data,
    Asset_of_interest = "EUR_USD",
    date_limit = date_train_end
    ) {

    asset_data_internal <-
      asset_data %>%
      filter(Asset == Asset_of_interest) %>%
      filter(Date <= date_limit)

    All_Daily_Data_internal <-
      All_Daily_Data %>%
      filter(Asset == Asset_of_interest) %>%
      filter(Date <= date_limit)

    daily_indicator <-
      get_daily_indicators(
        Daily_Data = All_Daily_Data_internal,
        asset_data = asset_data_internal,
        Asset_of_interest = Asset_of_interest,
        return_joined_only = FALSE
      )

    daily_indicator <-
      daily_indicator[[1]] %>%
      dplyr::select(-Price, -Open, -Low, -High, -Vol., -Date_for_join) %>%
      mutate(
        across(.cols = !contains("Date") & !contains("Asset"),
               .fns = ~ lag(.))
      ) %>%
      arrange(Date) %>%
      fill(!contains("Date"), .direction = "down") %>%
      filter(if_all(everything(), ~ !is.na(.))) %>%
      mutate(
        Date = as_datetime(Date, tz = "Australia/Canberra")
      ) %>%
      distinct()

    return(daily_indicator)

  }

#' prepare_daily_indicator_model
#'
#' @param daily_indicator
#' @param actual_wins_losses
#' @param Asset_of_interest
#' @param date_limit
#' @param stop_value_var
#' @param profit_value_var
#' @param period_var
#' @param bin_var_col
#' @param trade_direction
#' @param save_path
#'
#' @returns
#' @export
#'
#' @examples
prepare_daily_indicator_model <-
  function(
    daily_indicator = daily_data_for_modelling_train,
    actual_wins_losses = actual_wins_losses,
    Asset_of_interest = "EUR_USD",
    date_limit = date_train_end,
    stop_value_var = stop_value_var,
    profit_value_var = profit_value_var,
    period_var = period_var,
    bin_var_col = c("period_return_20_Price", "period_return_35_Price"),
    trade_direction = "Long",
    save_path = "C:/Users/nikhi/Documents/trade_data/single_asset_models_v2_adv"
    ) {

    actual_wins_losses_raw <-
      actual_wins_losses

    daily_indicator <-
      daily_indicator %>%
      filter(Asset == Asset_of_interest,
             Date <= date_limit)

    for (i in 1:length(bin_var_col)) {

      message("Entered Loop")

      actual_wins_losses <-
        actual_wins_losses_raw %>%
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
              !!as.name(bin_var_col[i]) > 0 & trade_col == "Short" ~ "win",
              !!as.name(bin_var_col[i]) <= 0 & trade_col == "Short" ~ "loss",

              !!as.name(bin_var_col[i]) > 0 & trade_col == "Long" ~ "win",
              !!as.name(bin_var_col[i]) <= 0 & trade_col == "Long" ~ "loss"

            )
        ) %>%
        filter(Date <= date_limit)

      check_date <-
        daily_indicator %>% pull(Date) %>% max() %>% as_date()

      check_date <- check_date <= date_limit

      message(glue::glue("Data Date is less than Train Date Max: {check_date}"))

      daily_for_join_model <-
        actual_wins_losses %>%
        dplyr::select(Date, Asset ,bin_var, matches(bin_var_col) ) %>%
        filter(
          Asset == Asset_of_interest
        ) %>%
        left_join(daily_indicator) %>%
        fill(!contains("Date"), .direction = "down") %>%
        filter(if_all(everything(),~!is.na(.))) %>%
        filter(if_all(everything(),~!is.nan(.))) %>%
        filter(if_all(everything(),~!is.infinite(.))) %>%
        filter(Date <= date_limit)

      # rm(actual_wins_losses)

      daily_vars_for_indicator <-
        names(daily_for_join_model) %>%
        keep(~ !str_detect(.x, "Date") &
               !str_detect(.x, "bin_var") &
               !str_detect(.x, "Asset") &
               !str_detect(.x, paste(bin_var_col, collapse = "|") ) &
               !str_detect(.x, "period_return_")
        ) %>%
        unlist() %>%
        as.character()

      daily_indicator_formula_logit <-
        create_lm_formula(dependant = "bin_var=='win'",
                          independant = daily_vars_for_indicator)

      daily_indicator_model <-
        glm(formula = daily_indicator_formula_logit,
            data = daily_for_join_model,
            family = binomial("logit"))

      sig_coefs <-
        get_sig_coefs(model_object_of_interest = daily_indicator_model,
                      # p_value_thresh_for_inputs = 0.25
                      p_value_thresh_for_inputs = 0.9
                      )

      rm(daily_indicator_model)
      gc()

      daily_indicator_formula_logit <-
        create_lm_formula(dependant = "bin_var=='win'",
                          independant = sig_coefs)

      daily_indicator_model <-
        glm(formula = daily_indicator_formula_logit,
            data = daily_for_join_model,
            family = binomial("logit"))

      summary(daily_indicator_model)

      message(glue::glue("Passed daily Model {Asset_of_interest} {i}"))

      saveRDS(object = daily_indicator_model,
              file =
                glue::glue("{save_path}/{Asset_of_interest}_{trade_direction}_{bin_var_col[i]}_daily_logit.RDS")
      )

      rm(daily_indicator_model)
      gc()

      daily_indicator_formula_lin <-
        create_lm_formula(dependant = bin_var_col[i],
                          independant = daily_vars_for_indicator)

      daily_indicator_model_lin <-
        lm(formula = daily_indicator_formula_lin,
           data = daily_for_join_model)

      sig_coefs <-
        get_sig_coefs(model_object_of_interest = daily_indicator_model_lin,
                      # p_value_thresh_for_inputs = 0.25
                      p_value_thresh_for_inputs = 0.9
                      )

      daily_indicator_formula_lin <-
        create_lm_formula(dependant = bin_var_col[i],
                          independant = sig_coefs)

      daily_indicator_model_lin <-
        lm(formula = daily_indicator_formula_lin,
           data = daily_for_join_model)

      summary(daily_indicator_model_lin)

      message(glue::glue("Passed daily Model Linear {Asset_of_interest} {i}"))

      saveRDS(object = daily_indicator_model_lin,
              file =
                glue::glue("{save_path}/{Asset_of_interest}_{trade_direction}_{bin_var_col[i]}_daily_lin.RDS")
      )

      rm(daily_indicator_model_lin)
      gc()

    }

  }


#' prepare_copula_data
#'
#' @param asset_data
#' @param couplua_assets
#' @param Asset_of_interest
#'
#' @returns
#' @export
#'
#' @examples
prepare_copula_data <-
  function(
    asset_data = asset_data,
    couplua_assets = couplua_assets,
    Asset_of_interest = Asset_of_interest,
    date_limit = now(tzone = "Australia/Canberra")
    ) {

    copula_accumulation <- list()
    asset_data_internal <-
      asset_data %>%
      filter(
        Date <= date_limit
      )


    for (i in 1:length(couplua_assets)) {

      copula_accumulation[[i]] <-
        estimating_dual_copula(
          asset_data_to_use = asset_data_internal,
          asset_to_use = c(Asset_of_interest, couplua_assets[i]),
          price_col = "Open",
          rolling_period = 100,
          samples_for_MLE = 0.15,
          test_samples = 0.85
        ) %>%
        ungroup() %>%
        dplyr::select(Date, contains("_cor")|contains("_lm"))

    }

    copula_data <-
      asset_data_internal %>%
      filter(Asset == Asset_of_interest) %>%
      distinct(Date) %>%
      left_join(copula_accumulation %>%
                reduce(left_join) ) %>%
      arrange(Date) %>%
      fill(!contains("Date"), .direction = "down") %>%
      distinct()


    return(copula_data)

  }

#' prepare_copula_model
#'
#' @param actual_wins_losses
#' @param Asset_of_interest
#' @param date_limit
#' @param stop_value_var
#' @param profit_value_var
#' @param period_var
#' @param bin_var_col
#' @param trade_direction
#' @param save_path
#' @param copula_data
#'
#' @returns
#' @export
#'
#' @examples
prepare_copula_model <-
  function(
    copula_data = copula_data,
    actual_wins_losses = actual_wins_losses,
    Asset_of_interest = "EUR_USD",
    date_limit = date_train_end,
    stop_value_var = stop_value_var,
    profit_value_var = profit_value_var,
    period_var = period_var,
    bin_var_col = c("period_return_20_Price", "period_return_35_Price"),
    trade_direction = "Long",
    save_path = "C:/Users/nikhi/Documents/trade_data/single_asset_models_v2_adv"
  ) {

    actual_wins_losses_raw <-
      actual_wins_losses

    copula_data <-
      copula_data %>%
      filter(Date <= date_limit)

    for (i in 1:length(bin_var_col)) {

      message("Entered Loop")

      actual_wins_losses <-
        actual_wins_losses_raw %>%
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
              !!as.name(bin_var_col[i]) > 0 & trade_col == "Short" ~ "win",
              !!as.name(bin_var_col[i]) <= 0 & trade_col == "Short" ~ "loss",

              !!as.name(bin_var_col[i]) > 0 & trade_col == "Long" ~ "win",
              !!as.name(bin_var_col[i]) <= 0 & trade_col == "Long" ~ "loss"

            )
        ) %>%
        filter(Date <= date_limit)

      check_date <-
        copula_data %>% pull(Date) %>% max() %>% as_date()

      check_date <- check_date <= date_limit

      message(glue::glue("Data Date is less than Train Date Max: {check_date}"))

      copula_for_join_model <-
        actual_wins_losses %>%
        dplyr::select(Date, Asset ,bin_var, matches(bin_var_col) ) %>%
        filter(
          Asset == Asset_of_interest
        ) %>%
        left_join(copula_data) %>%
        fill(!contains("Date"), .direction = "down") %>%
        filter(if_all(everything(),~!is.na(.))) %>%
        filter(Date <= date_limit)

      # rm(actual_wins_losses)

      copula_vars_for_indicator <-
        names(copula_for_join_model) %>%
        keep(~ !str_detect(.x, "Date") &
               !str_detect(.x, "bin_var") &
               !str_detect(.x, "Asset") &
               !str_detect(.x, paste(bin_var_col, collapse = "|") ) &
               !str_detect(.x, "period_return_")
        ) %>%
        unlist() %>%
        as.character()

      copula_indicator_formula_logit <-
        create_lm_formula(dependant = "bin_var=='win'",
                          independant = copula_vars_for_indicator)

      copula_indicator_model <-
        glm(formula = copula_indicator_formula_logit,
            data = copula_for_join_model,
            family = binomial("logit"))

      sig_coefs <-
        get_sig_coefs(model_object_of_interest = copula_indicator_model,
                      # p_value_thresh_for_inputs = 0.25
                      p_value_thresh_for_inputs = 0.9
                      )

      rm(copula_indicator_model)
      gc()

      copula_indicator_formula_logit <-
        create_lm_formula(dependant = "bin_var=='win'",
                          independant = sig_coefs)

      copula_indicator_model <-
        glm(formula = copula_indicator_formula_logit,
            data = copula_for_join_model,
            family = binomial("logit"))

      summary(copula_indicator_model)

      message(glue::glue("Passed copula Model {Asset_of_interest} {i}"))

      saveRDS(object = copula_indicator_model,
              file =
                glue::glue("{save_path}/{Asset_of_interest}_{trade_direction}_{bin_var_col[i]}_copula_logit.RDS")
      )

      rm(copula_indicator_model)
      gc()

      copula_indicator_formula_lin <-
        create_lm_formula(dependant = bin_var_col[i],
                          independant = copula_vars_for_indicator)

      copula_indicator_model_lin <-
        lm(formula = copula_indicator_formula_lin,
           data = copula_for_join_model)

      sig_coefs <-
        get_sig_coefs(model_object_of_interest = copula_indicator_model_lin,
                      # p_value_thresh_for_inputs = 0.25
                      p_value_thresh_for_inputs = 0.9
                      )

      copula_indicator_formula_lin <-
        create_lm_formula(dependant = bin_var_col[i],
                          independant = sig_coefs)

      copula_indicator_model_lin <-
        lm(formula = copula_indicator_formula_lin,
           data = copula_for_join_model)

      summary(copula_indicator_model_lin)

      message(glue::glue("Passed copula Model Linear {Asset_of_interest} {i}"))

      saveRDS(object = copula_indicator_model_lin,
              file =
                glue::glue("{save_path}/{Asset_of_interest}_{trade_direction}_{bin_var_col[i]}_copula_lin.RDS")
      )

      rm(copula_indicator_model_lin)
      gc()

    }

  }

#' prepare_combined_model
#'
#' @param actual_wins_losses
#' @param Asset_of_interest
#' @param date_limit
#' @param stop_value_var
#' @param profit_value_var
#' @param period_var
#' @param bin_var_col
#' @param trade_direction
#' @param save_path
#' @param copula_data
#'
#' @returns
#' @export
#'
#' @examples
prepare_combined_model <-
  function(
    combined_model_data = combined_model_data,
    actual_wins_losses = actual_wins_losses,
    Asset_of_interest = "EUR_USD",
    date_limit = date_train_phase_2_end,
    stop_value_var = stop_value_var,
    profit_value_var = profit_value_var,
    period_var = period_var,
    bin_var_col = c("period_return_20_Price", "period_return_35_Price"),
    trade_direction = "Long",
    save_path = "C:/Users/nikhi/Documents/trade_data/single_asset_models_v2_adv"
  ) {

    actual_wins_losses_raw <-
      actual_wins_losses

    combined_model_data <-
      combined_model_data %>%
      filter(Date <= date_limit)

    for (i in 1:length(bin_var_col)) {

      message("Entered Loop")

      actual_wins_losses <-
        actual_wins_losses_raw %>%
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
              !!as.name(bin_var_col[i]) > 0 & trade_col == "Short" ~ "win",
              !!as.name(bin_var_col[i]) <= 0 & trade_col == "Short" ~ "loss",

              !!as.name(bin_var_col[i]) > 0 & trade_col == "Long" ~ "win",
              !!as.name(bin_var_col[i]) <= 0 & trade_col == "Long" ~ "loss"

            )
        ) %>%
        filter(Date <= date_limit)

      check_date <-
        combined_model_data %>% pull(Date) %>% max() %>% as_date()

      check_date <- check_date <= date_limit

      message(glue::glue("Data Date is less than Train Date Max: {check_date}"))

      combined_for_join_model <-
        actual_wins_losses %>%
        dplyr::select(Date, Asset ,bin_var, matches(bin_var_col) ) %>%
        filter(
          Asset == Asset_of_interest
        ) %>%
        left_join(combined_model_data) %>%
        fill(!contains("Date"), .direction = "down") %>%
        filter(if_all(everything(),~!is.na(.))) %>%
        filter(if_all(everything(),~!is.nan(.))) %>%
        filter(if_all(everything(),~!is.infinite(.))) %>%
        filter(Date <= date_limit)

      # rm(actual_wins_losses)

      combined_vars_for_indicator <-
        names(combined_for_join_model) %>%
        keep(~ !str_detect(.x, "Date") &
               !str_detect(.x, "bin_var") &
               !str_detect(.x, "Asset") &
               !str_detect(.x, paste(bin_var_col, collapse = "|") ) &
               !str_detect(.x, "period_return_")
        ) %>%
        unlist() %>%
        as.character()

      combined_indicator_formula_logit <-
        create_lm_formula(dependant = "bin_var=='win'",
                          independant = combined_vars_for_indicator)

      combined_indicator_model <-
        glm(formula = combined_indicator_formula_logit,
            data = combined_for_join_model,
            family = binomial("logit"))

      sig_coefs <-
        get_sig_coefs(model_object_of_interest = combined_indicator_model,
                      p_value_thresh_for_inputs = 0.00001)

      rm(combined_indicator_model)
      gc()

      combined_indicator_formula_logit <-
        create_lm_formula(dependant = "bin_var=='win'",
                          independant = sig_coefs)

      combined_indicator_model <-
        glm(formula = combined_indicator_formula_logit,
            data = combined_for_join_model,
            family = binomial("logit"))

      summary(combined_indicator_model)

      message(glue::glue("Passed combined Model {Asset_of_interest} {i}"))

      saveRDS(object = combined_indicator_model,
              file =
                glue::glue("{save_path}/{Asset_of_interest}_{trade_direction}_{bin_var_col[i]}_combined_logit.RDS")
      )

      rm(combined_indicator_model)
      gc()

      combined_indicator_formula_lin <-
        create_lm_formula(dependant = bin_var_col[i],
                          independant = combined_vars_for_indicator)

      combined_indicator_model_lin <-
        lm(formula = combined_indicator_formula_lin,
           data = combined_for_join_model)

      sig_coefs <-
        get_sig_coefs(model_object_of_interest = combined_indicator_model_lin,
                      p_value_thresh_for_inputs = 0.00001)

      combined_indicator_formula_lin <-
        create_lm_formula(dependant = bin_var_col[i],
                          independant = sig_coefs)

      combined_indicator_model_lin <-
        lm(formula = combined_indicator_formula_lin,
           data = combined_for_join_model)

      summary(combined_indicator_model_lin)

      message(glue::glue("Passed combined Model Linear {Asset_of_interest} {i}"))

      saveRDS(object = combined_indicator_model_lin,
              file =
                glue::glue("{save_path}/{Asset_of_interest}_{trade_direction}_{bin_var_col[i]}_combined_lin.RDS")
      )

      rm(combined_indicator_model_lin)
      gc()

    }

  }

#' prepare_technical_model
#'
#' @param technical_data
#' @param actual_wins_losses
#' @param Asset_of_interest
#' @param date_limit
#' @param stop_value_var
#' @param profit_value_var
#' @param period_var
#' @param bin_var_col
#' @param trade_direction
#' @param save_path
#'
#' @returns
#' @export
#'
#' @examples
prepare_technical_model <-
  function(
    technical_data = technical_data,
    actual_wins_losses = actual_wins_losses,
    Asset_of_interest = "EUR_USD",
    date_limit = date_train_phase_2_end,
    stop_value_var = stop_value_var,
    profit_value_var = profit_value_var,
    period_var = period_var,
    bin_var_col = c("period_return_20_Price", "period_return_35_Price"),
    trade_direction = "Long",
    save_path = "C:/Users/nikhi/Documents/trade_data/single_asset_models_v2_adv"
  ) {

    actual_wins_losses_raw <-
      actual_wins_losses

    technical_data <-
      technical_data %>%
      filter(Date <= date_limit) %>%
      filter(if_all(everything() ,~ !is.nan(.) ))%>%
      filter(if_all(everything() ,~ !is.infinite(.) )) %>%
      filter(if_all(everything() ,~ !is.na(.) ))

    for (i in 1:length(bin_var_col)) {

      message("Entered Loop")

      actual_wins_losses <-
        actual_wins_losses_raw %>%
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
              !!as.name(bin_var_col[i]) > 0 & trade_col == "Short" ~ "win",
              !!as.name(bin_var_col[i]) <= 0 & trade_col == "Short" ~ "loss",

              !!as.name(bin_var_col[i]) > 0 & trade_col == "Long" ~ "win",
              !!as.name(bin_var_col[i]) <= 0 & trade_col == "Long" ~ "loss"

            )
        ) %>%
        filter(Date <= date_limit)

      check_date <-
        technical_data %>% pull(Date) %>% max() %>% as_date()

      check_date <- check_date <= date_limit

      message(glue::glue("Data Date is less than Train Date Max: {check_date}"))

      technical_for_join_model <-
        actual_wins_losses %>%
        dplyr::select(Date, Asset ,bin_var, matches(bin_var_col) ) %>%
        filter(
          Asset == Asset_of_interest
        ) %>%
        left_join(technical_data) %>%
        fill(!contains("Date"), .direction = "down") %>%
        filter( if_all( everything(),~!is.na(.) ) ) %>%
        filter( if_all( everything(),~!is.nan(.) ) ) %>%
        filter( if_all( everything(),~!is.infinite(.) ) ) %>%
        filter(Date <= date_limit)

      # rm(actual_wins_losses)

      technical_vars_for_indicator <-
        names(technical_for_join_model) %>%
        keep(~ !str_detect(.x, "Date") &
               !str_detect(.x, "bin_var") &
               !str_detect(.x, "Asset") &
               !str_detect(.x, paste(bin_var_col, collapse = "|") ) &
               !str_detect(.x, "period_return_")
        ) %>%
        unlist() %>%
        as.character()

      technical_indicator_formula_logit <-
        create_lm_formula(dependant = "bin_var=='win'",
                          independant = technical_vars_for_indicator)

      technical_indicator_model <-
        glm(formula = technical_indicator_formula_logit,
            data = technical_for_join_model,
            family = binomial("logit"))

      sig_coefs <-
        get_sig_coefs(model_object_of_interest = technical_indicator_model,
                      # p_value_thresh_for_inputs = 0.15
                      p_value_thresh_for_inputs = 0.9
                      )

      rm(technical_indicator_model)
      gc()

      technical_indicator_formula_logit <-
        create_lm_formula(dependant = "bin_var=='win'",
                          independant = sig_coefs)

      technical_indicator_model <-
        glm(formula = technical_indicator_formula_logit,
            data = technical_for_join_model,
            family = binomial("logit"))

      summary(technical_indicator_model)

      message(glue::glue("Passed Technical Model {Asset_of_interest} {i}"))

      saveRDS(object = technical_indicator_model,
              file =
                glue::glue("{save_path}/{Asset_of_interest}_{trade_direction}_{bin_var_col[i]}_technical_logit.RDS")
      )

      rm(technical_indicator_model)
      gc()

      technical_indicator_formula_lin <-
        create_lm_formula(dependant = bin_var_col[i],
                          independant = technical_vars_for_indicator)

      technical_indicator_model_lin <-
        lm(formula = technical_indicator_formula_lin,
           data = technical_for_join_model)

      sig_coefs <-
        get_sig_coefs(model_object_of_interest = technical_indicator_model_lin,
                      # p_value_thresh_for_inputs = 0.15
                      p_value_thresh_for_inputs = 0.9
                      )

      technical_indicator_formula_lin <-
        create_lm_formula(dependant = bin_var_col[i],
                          independant = sig_coefs)

      technical_indicator_model_lin <-
        lm(formula = technical_indicator_formula_lin,
           data = technical_for_join_model)

      summary(technical_indicator_model_lin)

      message(glue::glue("Passed technical Model Linear {Asset_of_interest} {i}"))

      saveRDS(object = technical_indicator_model_lin,
              file =
                glue::glue("{save_path}/{Asset_of_interest}_{trade_direction}_{bin_var_col[i]}_technical_lin.RDS")
      )

      rm(technical_indicator_model_lin)
      gc()

    }

  }

#' get_GDP_countries
#'
#' @param raw_macro_data
#' @param lag_days
#'
#' @returns
#' @export
#'
#' @examples
get_GDP_countries <-
  function(raw_macro_data = raw_macro_data,
           lag_days = 3 ) {

    GDP_data <-
      raw_macro_data %>%
      mutate(
        Index_Type =
          case_when(

            str_detect(event, "Public Deficit") &
              str_detect(event, "GDP") &
              symbol == "EUR" ~ "EUR GDP",

            str_detect(event, "Current Account") &
              symbol == "USD" ~ "USD GDP",

            str_detect(event, "Current Account") &
              str_detect(event, "QoQ") &
              symbol == "NZD" ~ "NZD GDP",

            str_detect(event, "Current Account Balance") &
              str_detect(event, "Q") &
              symbol == "AUD" ~ "AUD GDP",

            str_detect(event, "Current Account") &
              str_detect(event, "Q") &
              symbol == "GBP" ~ "GBP GDP",

            str_detect(event, "Current Account") &
              str_detect(event, "Q") &
              symbol == "CAD" ~ "CAD GDP",

            str_detect(event, "Gross Domestic Product") &
              str_detect(event, "(QoQ)") &
              symbol == "CHF" ~ "CHF GDP",

            str_detect(event, "Gross Domestic Product") &
              str_detect(event, "(QoQ)") &
              symbol == "JPY" ~ "JPY GDP",

            str_detect(event, "Gross Domestic Product") &
              str_detect(event, "(QoQ)") &
              symbol == "CNY" ~ "CNY GDP"

          )
      ) %>%
      filter(!is.na(Index_Type)) %>%
      dplyr::select(Index_Type, actual,date ) %>%
      dplyr::group_by(Index_Type,date ) %>%
      summarise(
        actual = median(actual, na.rm = T)
      ) %>%
      ungroup() %>%
      mutate(date = date + lubridate::days(lag_days) ) %>%
      mutate(
        date =
          case_when(
            lubridate::wday(date) == 7 ~ date + lubridate::days(2),
            lubridate::wday(date) == 1 ~ date + lubridate::days(1),
            TRUE ~ date
          )
      ) %>%
      group_by(Index_Type) %>%
      arrange(date, .by_group = TRUE) %>%
      group_by(Index_Type) %>%
      mutate(
        actual =
          case_when(
            !(Index_Type %in% c("CHF GDP", "CNY GDP", "EUR GDP", "JPY GDP")) ~
              (actual - lag(actual))/lag(actual),
            TRUE ~ actual
          )
      ) %>%
      ungroup() %>%
      pivot_wider(names_from = Index_Type, values_from = actual, values_fn = median) %>%
      arrange(date) %>%
      fill(everything(), .direction = "down") %>%
      filter(if_all(everything(), ~ !is.na(.) ))

    return(GDP_data)

  }


#' get_unemp_countries
#'
#' @param raw_macro_data
#' @param lag_days
#'
#' @returns
#' @export
#'
#' @examples
get_unemp_countries <-
  function(raw_macro_data = raw_macro_data,
           lag_days = 3 ) {

    UR_data <-
      raw_macro_data %>%
      mutate(
        Index_Type =
          case_when(

            str_detect(event, "Unemployment Rate") &
              symbol == "EUR" ~ "EUR UR",

            str_detect(event, "Unemployment Rate") &
              symbol == "USD" ~ "USD UR",

            str_detect(event, "Unemployment Rate") &
              symbol == "NZD" ~ "NZD UR",

            str_detect(event, "Unemployment Rate") &
              symbol == "AUD" ~ "AUD UR",

            str_detect(event, "Unemployment Rate") &
              symbol == "GBP" ~ "GBP UR",

            str_detect(event, "Unemployment Rate") &
              symbol == "CAD" ~ "CAD UR",

            str_detect(event, "Unemployment Rate") &
              symbol == "CHF" ~ "CHF UR",

            str_detect(event, "Unemployment Rate") &
              symbol == "JPY" ~ "JPY UR"

          )
      ) %>%
      filter(!is.na(Index_Type)) %>%
      dplyr::select(Index_Type, actual,date ) %>%
      dplyr::group_by(Index_Type,date ) %>%
      summarise(
        actual = median(actual, na.rm = T)
      ) %>%
      ungroup() %>%
      mutate(date = date + lubridate::days(lag_days) ) %>%
      mutate(
        date =
          case_when(
            lubridate::wday(date) == 7 ~ date + lubridate::days(2),
            lubridate::wday(date) == 1 ~ date + lubridate::days(1),
            TRUE ~ date
          )
      ) %>%
      group_by(Index_Type) %>%
      arrange(date, .by_group = TRUE) %>%
      ungroup() %>%
      pivot_wider(names_from = Index_Type, values_from = actual, values_fn = median) %>%
      arrange(date) %>%
      fill(everything(), .direction = "down") %>%
      filter(if_all(everything(), ~ !is.na(.) ))

    return(UR_data)

  }

#' get_manufac_countries
#'
#' @param raw_macro_data
#' @param lag_days
#'
#' @returns
#' @export
#'
#' @examples
get_manufac_countries <-
  function(raw_macro_data = raw_macro_data,
           lag_days = 3 ) {

    Manufac_data <-
      raw_macro_data %>%
      mutate(
        Index_Type =
          case_when(

            str_detect(event, "Manufacturing PMI") &
              symbol == "EUR" ~ "EUR Manufac PMI",

            str_detect(event, "Manufacturing PMI") &
              symbol == "USD" ~ "USD Manufac PMI",

            str_detect(event, "Manufacturing Sales") &
              symbol == "NZD" ~ "NZD Manufac PMI",

            (str_detect(event, "Manufacturing PMI") &
              symbol == "AUD")|
            (str_detect(event, "AiG Performance of Mfg") &
              symbol == "AUD") ~ "AUD Manufac PMI",

            str_detect(event, "Manufacturing PMI") &
              symbol == "GBP" ~ "GBP Manufac PMI",

            str_detect(event, "Manufacturing PMI") &
              symbol == "CAD" ~ "CAD Manufac PMI",

            # str_detect(event, "Unemployment Rate") &
            #   symbol == "CHF" ~ "CHF UR",

            str_detect(event, "Manufacturing PMI") &
              symbol == "JPY" ~ "JPY Manufac PMI",

            str_detect(event, "Manufacturing PMI") &
              symbol == "CNY" ~ "CNY Manufac PMI"

          )
      ) %>%
      filter(!is.na(Index_Type)) %>%
      dplyr::select(Index_Type, actual,date ) %>%
      dplyr::group_by(Index_Type,date ) %>%
      summarise(
        actual = median(actual, na.rm = T)
      ) %>%
      ungroup() %>%
      mutate(date = date + lubridate::days(lag_days) ) %>%
      mutate(
        date =
          case_when(
            lubridate::wday(date) == 7 ~ date + lubridate::days(2),
            lubridate::wday(date) == 1 ~ date + lubridate::days(1),
            TRUE ~ date
          )
      ) %>%
      group_by(Index_Type) %>%
      arrange(date, .by_group = TRUE) %>%
      ungroup() %>%
      pivot_wider(names_from = Index_Type, values_from = actual, values_fn = median) %>%
      arrange(date) %>%
      fill(everything(), .direction = "down") %>%
      filter(if_all(everything(), ~ !is.na(.) ))

    return(Manufac_data)

  }


#' get_additional_USD_Macro
#'
#' @param raw_macro_data
#' @param lag_days
#'
#' @returns
#' @export
#'
#' @examples
get_additional_USD_Macro <-
  function(raw_macro_data = raw_macro_data,
           lag_days = 3 ) {

    USD_Macro <-
      raw_macro_data %>%
      mutate(
        Index_Type =
          case_when(

            str_detect(event, "Consumer Credit Change") &
              symbol == "USD" ~ "USD_Consumer_Credit",

            str_detect(event, "Goods and Services Trade Balance") &
              symbol == "USD" ~ "USD_Trade_Balance",

            str_detect(event, "Export Price Index") &
              str_detect(event, "MoM") &
              symbol == "USD" ~ "USD_Export_Price",

            str_detect(event, "Monthly Budget Statement") &
              symbol == "USD" ~ "USD_Budget_Statement",

            str_detect(event, "Continuing Jobless Claims") &
              symbol == "USD" ~ "USD_Jobless",

            str_detect(event, "ADP Employment Change") &
              str_detect(event, "\\(") &
              symbol == "USD" ~ "USD_Employment_Change",

            str_detect(event, "Net Long\\-Term TIC Flows") &
              symbol == "USD" ~ "USD_TIC_Flows",

            str_detect(event, "Nonfarm Payrolls") &
              symbol == "USD" ~ "USD_Payrolls",

          )
      ) %>%
      filter(!is.na(Index_Type)) %>%
      dplyr::select(Index_Type, actual,date ) %>%
      dplyr::group_by(Index_Type,date ) %>%
      summarise(
        actual = median(actual, na.rm = T)
      ) %>%
      ungroup() %>%
      mutate(date = date + lubridate::days(lag_days) ) %>%
      mutate(
        date =
          case_when(
            lubridate::wday(date) == 7 ~ date + lubridate::days(2),
            lubridate::wday(date) == 1 ~ date + lubridate::days(1),
            TRUE ~ date
          )
      ) %>%
      group_by(Index_Type) %>%
      arrange(date, .by_group = TRUE) %>%
      ungroup() %>%
      group_by(Index_Type) %>%
      mutate(
        actual = log(actual/lag(actual))
      ) %>%
      pivot_wider(names_from = Index_Type, values_from = actual, values_fn = median) %>%
      arrange(date) %>%
      fill(everything(), .direction = "down") %>%
      mutate(
        across(
          .cols = !contains("date"),
          .fns = ~ ifelse( is.infinite(.), mean(., na.rm = T), .)
        )
      ) %>%
      filter(if_all(everything(), ~ !is.na(.) )) %>%
      # mutate(
      #   # USD_Consumer_Credit = scale(USD_Consumer_Credit) %>% as.vector() %>% as.numeric(),
      #   USD_Consumer_Credit = log(USD_Consumer_Credit/lag(USD_Consumer_Credit)),
      #   USD_Trade_Balance = log(USD_Trade_Balance/lag(USD_Trade_Balance)),
      #   USD_Budget_Statement = log(USD_Budget_Statement/lag(USD_Budget_Statement))
      # ) %>%
      filter(if_all(everything(), ~ !is.na(.) ))

    return(USD_Macro)

  }

#' get_additional_USD_Macro
#'
#' @param raw_macro_data
#' @param lag_days
#'
#' @returns
#' @export
#'
#' @examples
get_additional_EUR_Macro <-
  function(raw_macro_data = raw_macro_data,
           lag_days = 3 ) {

    EUR_Macro <-
      raw_macro_data %>%
      mutate(
        Index_Type =
          case_when(

            str_detect(event, "Trade Balance EUR") &
              symbol == "EUR" ~ "EUR_Trade_Balance",

            str_detect(event, "Current Account n\\.s\\.a.") &
              symbol == "EUR" ~ "EUR_CA_nsa",

            str_detect(event, "Budget") &
              symbol == "EUR" ~ "EUR_Budget",

            str_detect(event, "Imports") &
              str_detect(event, "EUR") &
              !str_detect(event, "MoM") &
              symbol == "EUR" ~ "EUR_Imports",

            str_detect(event, "Exports\\, EUR") &
              !str_detect(event, "MoM") &
              symbol == "EUR" ~ "EUR_Exports"

          )
      ) %>%
      filter(!is.na(Index_Type)) %>%
      dplyr::select(Index_Type, actual,date ) %>%
      dplyr::group_by(Index_Type,date ) %>%
      summarise(
        actual = median(actual, na.rm = T)
      ) %>%
      ungroup() %>%
      mutate(date = date + lubridate::days(lag_days) ) %>%
      mutate(
        date =
          case_when(
            lubridate::wday(date) == 7 ~ date + lubridate::days(2),
            lubridate::wday(date) == 1 ~ date + lubridate::days(1),
            TRUE ~ date
          )
      ) %>%
      group_by(Index_Type) %>%
      arrange(date, .by_group = TRUE) %>%
      ungroup() %>%
      group_by(Index_Type) %>%
      mutate(
        actual = log(actual/lag(actual))
      ) %>%
      pivot_wider(names_from = Index_Type, values_from = actual, values_fn = median) %>%
      arrange(date) %>%
      fill(everything(), .direction = "down") %>%
      mutate(
        across(
          .cols = !contains("date"),
          .fns = ~ ifelse( is.infinite(.), mean(., na.rm = T), .)
        )
      ) %>%
      filter(if_all(everything(), ~ !is.na(.) )) %>%
      # mutate(
      #   # USD_Consumer_Credit = scale(USD_Consumer_Credit) %>% as.vector() %>% as.numeric(),
      #   USD_Consumer_Credit = log(USD_Consumer_Credit/lag(USD_Consumer_Credit)),
      #   USD_Trade_Balance = log(USD_Trade_Balance/lag(USD_Trade_Balance)),
      #   USD_Budget_Statement = log(USD_Budget_Statement/lag(USD_Budget_Statement))
      # ) %>%
      filter(if_all(everything(), ~ !is.na(.) ))

    return(EUR_Macro)

  }


#' get_indicator_pred_from_db
#'
#' @param model_data_store_path
#'
#' @returns
#' @export
#'
#' @examples
get_indicator_pred_from_db <-
  function(
    model_data_store_path =
      "C:/Users/nikhi/Documents/trade_data/Day_Trader_Single_Asset_V2_trade_store.db",
    table_name = "single_asset_improved"
  ) {

    model_data_store_db <-
      connect_db(model_data_store_path)

    indicator_data <-
      DBI::dbGetQuery(conn = model_data_store_db,
                      statement = glue::glue("SELECT * FROM {table_name}") ) %>%
      distinct() %>%
      mutate(
        Date = as_datetime(Date),
        test_end_date = as_date(test_end_date),
        date_train_end = as_date(date_train_end),
        date_train_phase_2_end = as_date(date_train_phase_2_end),
        date_test_start = as_date(date_test_start),
      ) %>%
      filter(sim_index == 1) %>%
      group_by(Asset) %>%
      arrange(Date, .by_group = TRUE) %>%
      filter(Date >= (date_test_start + days(1)) )

    DBI::dbDisconnect(model_data_store_db)
    rm(model_data_store_db)
    gc()

    return(indicator_data)

  }

#' prepare_post_ss_gen2_model
#'
#' @param indicator_data
#' @param new_post_DB
#' @param pred_threshs
#' @param rolling_periods
#' @param post_training_months
#' @param new_sym
#' @param post_model_data_save_path
#' @param dependant_var
#' @param dependant_threshold
#'
#' @returns
#' @export
#'
#' @examples
prepare_post_ss_gen2_model <-
  function(
    indicator_data = indicator_data,
    actual_wins_losses = actual_wins_losses,
    new_post_DB = TRUE,
    new_sym = TRUE,
    post_model_data_save_path =
      "C:/Users/nikhi/Documents/trade_data/single_asset_models_v2_adv/",
    dependant_var = "period_return_24_Price",
    dependant_threshold = 5,
    training_date_start = "2023-01-04",
    training_date_end = "2023-07-01"
  ) {

    indicator_data <-
      indicator_data %>%
      filter(
        Date >= training_date_start
      ) %>%
      filter(
        Date <= training_date_end
      ) %>%
      dplyr::select(-contains("_sd")) %>%
      dplyr::select(-contains("_mean")) %>%
      group_by(Asset) %>%
      arrange(Date, .by_group = TRUE) %>%
      group_by(Asset) %>%
      fill(contains("pred_"), .direction = "down") %>%
      ungroup()

    all_asset_post_model_data <-
      indicator_data %>%
      ungroup() %>%
      dplyr::select(-contains("period_return_")) %>%
      dplyr::select(-contains("_sd")) %>%
      dplyr::select(-contains("_mean")) %>%
      left_join(
        actual_wins_losses %>%
          dplyr::select(Date, Asset, contains("period_return_")) %>%
          distinct()
      ) %>%
      dplyr::select(Date, Asset,
                    date_test_start,
                    contains("pred_"),
                    contains("period_return_")
      ) %>%
      mutate(
        high_return_date =
          case_when(
            !!as.name(dependant_var) > dependant_threshold ~ "Detected",
            TRUE ~ "Dont Trade"
          )
      ) %>%
      mutate(
        Equity_Asset =
          case_when(
            Asset %in% c("AU200_AUD", "FR40_EUR", "HK33_HKD", "EU50_EUR", "SPX500_USD",
                         "UK100_GBP", "US2000_USD", "USB10Y_USD") ~ 1,
            TRUE ~ 0
          ),
        Commod_Asset =
          case_when(
            Asset == "WTICO_USD"|str_detect(Asset, "XAG|XAU|XCU") ~ 1,
            TRUE ~ 0
          ),
        Currency_Asset_AUD =
          case_when(
            str_detect(Asset, "AUD") ~ 1,
            TRUE ~ 0
          ),
        Currency_Asset_NZD =
          case_when(
            str_detect(Asset, "NZD") ~ 1,
            TRUE ~ 0
          ),
        Currency_Asset_JPY =
          case_when(
            str_detect(Asset, "JPY") ~ 1,
            TRUE ~ 0
          ),
        Currency_Asset_GBP =
          case_when(
            str_detect(Asset, "GBP") ~ 1,
            TRUE ~ 0
          ),
        Currency_Asset_EUR =
          case_when(
            str_detect(Asset, "EUR") ~ 1,
            TRUE ~ 0
          ),
        Currency_Asset_USD =
          case_when(
            str_detect(Asset, "USD") ~ 1,
            TRUE ~ 0
          ),
        Crypto_Asset =
          case_when(
            str_detect(Asset, "BTC|ETH") ~ 1,
            TRUE ~ 0
          ),
        Metal_Asset =
          case_when(
            str_detect(Asset, "XAG") ~ 1,
            TRUE ~ 0
          ),

        Time_of_Day = hour(Date),
        Day_of_week = wday(Date)
      )

    training_data_post_model_all_assets <- all_asset_post_model_data

    lm_vars <- names(all_asset_post_model_data) %>%
      keep(~ str_detect(.x, "pred|Time_of_Day|Day_of_week|Commod_Asset|Currency_Asset|Equity_Asset|Metal_Asset|Crypto_Asset")) %>%
      unlist()

    lm_form <-
      create_lm_formula(dependant = dependant_var, independant = lm_vars)

    lm_model <-
      lm(data = training_data_post_model_all_assets%>%
           filter(if_all(contains("pred"), ~ !is.nan(.) & !is.infinite(.) & !is.na(.)  )),
         formula = lm_form)

    summary(lm_model)

    saveRDS(object = lm_model,
            file =
              glue::glue("{post_model_data_save_path}/LM_Post_All_{dependant_var}.RDS")
            )

    sig_coefs <-
      get_sig_coefs(model_object_of_interest = lm_model,
                    p_value_thresh_for_inputs = 0.99)

    lm_form <-
      create_lm_formula(dependant = dependant_var, independant = sig_coefs)

    lm_model <-
      lm(data = training_data_post_model_all_assets %>%
           filter(if_all(contains("pred"), ~ !is.nan(.) & !is.infinite(.) & !is.na(.)  )),
         formula = lm_form)

    summary(lm_model)

    glm_form <-
      create_lm_formula(dependant = "high_return_date == 'Detected' ", independant = lm_vars)

    glm_model <-
      glm(data =training_data_post_model_all_assets%>%
            filter(if_all(contains("pred"), ~ !is.nan(.) & !is.infinite(.) & !is.na(.)  )),
          formula = glm_form, family = binomial("logit"))

    summary(glm_model)

    sig_coefs <-
      get_sig_coefs(model_object_of_interest = glm_model,
                    p_value_thresh_for_inputs = 0.99)

    glm_form <-
      create_lm_formula(dependant = "high_return_date == 'Detected' ", independant = sig_coefs)

    glm_model <-
      glm(data = training_data_post_model_all_assets %>%
            filter(if_all(contains("pred"), ~ !is.nan(.) & !is.infinite(.) & !is.na(.)  )),
          formula = glm_form, family = binomial("logit"))

    summary(glm_model)

    saveRDS(object = glm_model,
            file =
              glue::glue("{post_model_data_save_path}/GLM_Post_All_{dependant_var}_bin_{dependant_threshold}.RDS")
    )

  }

#' read_post_models_and_get_preds
#'
#' @returns
#' @export
#'
#' @examples
read_post_models_and_get_preds <-
  function(
    indicator_data = indicator_data,
    post_model_data_save_path =
      "C:/Users/nikhi/Documents/trade_data/single_asset_models_v2_adv/",
    test_date_start = "2023-08-01",
    test_date_end = today(),
    dependant_var = "period_return_24_Price",
    dependant_threshold = 5,
    ignore_dependant_var = FALSE
    ) {

    indicator_data_internal <-
      indicator_data %>%
      dplyr::select(-test_end_date) %>%
      # filter(
      #   Date >=test_date_start,
      #   Date <= test_date_end
      # )  %>%
      group_by(Asset) %>%
      arrange(Date, .by_group = TRUE) %>%
      group_by(Asset) %>%
      fill(contains("pred_"), .direction = "down") %>%
      ungroup()

    all_asset_post_model_data <-
      indicator_data_internal %>%
      ungroup() %>%
      group_by(Asset) %>%
      arrange(Date, .by_group = TRUE) %>%
      ungroup() %>%
      dplyr::select(-contains("_sd")) %>%
      dplyr::select(-contains("_mean")) %>%
      dplyr::select(Date, Asset,
                    contains("pred_"),
                    contains("period_return_")
      )

    if(ignore_dependant_var == FALSE) {

    all_asset_post_model_data <-
        all_asset_post_model_data %>%
        mutate(
          high_return_date =
            case_when(
              !!as.name(dependant_var) > dependant_threshold ~ "Detected",
              TRUE ~ "Dont Trade"
            )
        )

    }

  all_asset_post_model_data <-
      all_asset_post_model_data%>%
      mutate(
        Equity_Asset =
          case_when(
            Asset %in% c("AU200_AUD", "FR40_EUR", "HK33_HKD", "EU50_EUR", "SPX500_USD",
                         "UK100_GBP", "US2000_USD", "USB10Y_USD") ~ 1,
            TRUE ~ 0
          ),
        Commod_Asset =
          case_when(
            Asset == "WTICO_USD"|str_detect(Asset, "XAG|XAU|XCU") ~ 1,
            TRUE ~ 0
          ),
        Currency_Asset_AUD =
          case_when(
            str_detect(Asset, "AUD") ~ 1,
            TRUE ~ 0
          ),
        Currency_Asset_NZD =
          case_when(
            str_detect(Asset, "NZD") ~ 1,
            TRUE ~ 0
          ),
        Currency_Asset_JPY =
          case_when(
            str_detect(Asset, "JPY") ~ 1,
            TRUE ~ 0
          ),
        Currency_Asset_GBP =
          case_when(
            str_detect(Asset, "GBP") ~ 1,
            TRUE ~ 0
          ),
        Currency_Asset_EUR =
          case_when(
            str_detect(Asset, "EUR") ~ 1,
            TRUE ~ 0
          ),
        Currency_Asset_USD =
          case_when(
            str_detect(Asset, "USD") ~ 1,
            TRUE ~ 0
          ),
        Crypto_Asset =
          case_when(
            str_detect(Asset, "BTC|ETH") ~ 1,
            TRUE ~ 0
          ),
        Metal_Asset =
          case_when(
            str_detect(Asset, "XAG") ~ 1,
            TRUE ~ 0
          ),

        Time_of_Day = hour(Date),
        Day_of_week = wday(Date)
      )

    lin_models_in_path <-
      fs::dir_info(post_model_data_save_path) %>%
      filter(str_detect(path, "LM_Post_All_")) %>%
      pull(path)

    returned_dat <-
      all_asset_post_model_data

      for (j in 1:length(lin_models_in_path)) {

        model_temp <-
          readRDS(lin_models_in_path[j])

        model_type =
          ifelse( str_detect(lin_models_in_path[j], "GLM"), "GLM", "LM")

        period_return <-
          lin_models_in_path[j] %>%
          str_extract( "period_return_[0-9]+_Price") %>%
          str_remove_all("\\.RDS") %>%
          str_trim()

        preds <-
          predict(object = model_temp,
                  newdata = all_asset_post_model_data,
                  type = "response")

        returned_dat <-
          returned_dat %>%
          mutate(
            !!as.name( glue::glue("pred_{model_type}_{period_return}")) :=  preds
          )


      }

    returned_dat <-
      returned_dat %>%
      filter(
        Date >=test_date_start,
        Date <= test_date_end
      )

    return(returned_dat)

  }


#' get_rolling_post_preds
#'
#' @param post_pred_data
#' @param rolling_periods
#' @param test_date_start
#' @param test_date_end
#' @param pred_price_cols
#'
#' @returns
#' @export
#'
#' @examples
get_rolling_post_preds <-
  function(
    post_pred_data = post_preds_all,
    rolling_periods = c(50,2000, 100),
    test_date_start = "2023-08-01",
    test_date_end = today(),
    pred_price_cols =
      c("period_return_24_Price",
        "period_return_30_Price",
        "period_return_44_Price")
  ) {

    post_pred_data_internal <-
      post_pred_data %>%
      dplyr::select(-contains("_sd")) %>%
      dplyr::select(-contains("_mean")) %>%
      # filter(Date >= test_date_start)%>%
      # filter(Date <= test_date_end) %>%
      dplyr::select(Date, Asset,
                    contains("pred_GLM_period"),
                    contains("pred_LM_period"),
                    contains("period_return")
      )

    for (uu in 1:length(rolling_periods) ) {

      for (ii in 1:length(pred_price_cols)) {

        post_pred_data_internal <-
          post_pred_data_internal %>%
          group_by(Asset) %>%
          arrange(Date, .by_group = TRUE) %>%
          ungroup() %>%
          group_by(Asset) %>%
          mutate(
            !!as.name( glue::glue("mean_{rolling_periods[uu]}_pred_GLM_{pred_price_cols[ii]}") ) :=
              slider::slide_dbl(
                .x = !!as.name( glue::glue("pred_GLM_{pred_price_cols[ii]}") ),
                .f = ~mean(.x, na.rm = T),
                .before = rolling_periods[uu]),

            !!as.name( glue::glue("sd_{rolling_periods[uu]}_pred_GLM_{pred_price_cols[ii]}") ) :=
              slider::slide_dbl(
                .x = !!as.name( glue::glue("pred_GLM_{pred_price_cols[ii]}") ),
                .f = ~sd(.x, na.rm = T),
                .before = rolling_periods[uu]),

            !!as.name( glue::glue("mean_{rolling_periods[uu]}_pred_LM_{pred_price_cols[ii]}") ) :=
              slider::slide_dbl(
                .x = !!as.name( glue::glue("pred_LM_{pred_price_cols[ii]}") ),
                .f = ~mean(.x, na.rm = T),
                .before = rolling_periods[uu]),

            !!as.name( glue::glue("sd_{rolling_periods[uu]}_pred_LM_{pred_price_cols[ii]}") ) :=
              slider::slide_dbl(
                .x = !!as.name( glue::glue("pred_LM_{pred_price_cols[ii]}") ),
                .f = ~sd(.x, na.rm = T),
                .before = rolling_periods[uu])
          )

      }
    }

    post_pred_data_internal <-
      post_pred_data_internal %>%
      filter(
        Date >=test_date_start,
        Date <= test_date_end
      )


    return(post_pred_data_internal)

  }


#' get_post_pred_thresh_cols
#'
#' @param post_preds_all_rolling
#' @param pred_threshs
#' @param rolling_periods
#' @param pred_price_cols
#'
#' @returns
#' @export
#'
#' @examples
get_post_pred_thresh_cols <-
  function(
    post_preds_all_rolling = post_preds_all_rolling,
    pred_threshs = seq(0, 3, 0.25),
    rolling_periods = c(50,2000, 100),
    pred_price_cols =
      c("period_return_24_Price",
        "period_return_30_Price",
        "period_return_44_Price"),
    test_date_start = "2023-08-01",
    test_date_end = today()
  ) {

    trade_tagged_data <-
      post_preds_all_rolling %>%
      filter(Date >= test_date_start) %>%
      filter(Date <= test_date_end)

    for (kk in 1:length(pred_price_cols)) {
      for (uu in 1:length(rolling_periods)) {
        for (oo in 1:length(pred_threshs)) {

          thresh <- pred_threshs[oo]
          period_col <- pred_price_cols[kk]
          rolling_period <- rolling_periods[uu]

          pred_col_GLM <- glue::glue("pred_GLM_{period_col}")
          pred_col_GLM_mean <- glue::glue("mean_{rolling_period}_pred_GLM_{period_col}")
          pred_col_GLM_sd <- glue::glue("sd_{rolling_period}_pred_GLM_{period_col}")

          pred_col_LM <- glue::glue("pred_LM_{period_col}")
          pred_col_LM_mean <- glue::glue("mean_{rolling_period}_pred_LM_{period_col}")
          pred_col_LM_sd <- glue::glue("sd_{rolling_period}_pred_LM_{period_col}")

          trade_tagged_data <-
            trade_tagged_data %>%
            mutate(
              !!as.name(glue::glue("trade_col_GLM_{period_col}_{rolling_period}")) :=
                !!as.name(pred_col_GLM) >= !!as.name(pred_col_GLM_mean) + thresh*!!as.name(pred_col_GLM_sd),

              !!as.name(glue::glue("trade_col_LM_{period_col}_{rolling_period}")) :=
                !!as.name(pred_col_LM) >= !!as.name(pred_col_LM_mean) + thresh*!!as.name(pred_col_LM_sd)
            )

        }
      }
    }

    return(trade_tagged_data)

  }

#' post_ss_model_analyse_condition
#'
#' @param tagged_trade_col_data
#' @param return_col
#' @param trade_statement
#'
#' @returns
#' @export
#'
#' @examples
post_ss_model_analyse_condition <-
  function(
    tagged_trade_col_data = post_preds_all_rolling,
    trade_statement =
      "
    (mean_50_pred_LM_period_return_24_Price > mean_100_pred_LM_period_return_24_Price &
    mean_50_pred_LM_period_return_24_Price > mean_2000_pred_LM_period_return_24_Price &
    pred_LM_period_return_24_Price > 0 &

    mean_50_pred_LM_period_return_44_Price > mean_100_pred_LM_period_return_44_Price &
    mean_50_pred_LM_period_return_44_Price > mean_2000_pred_LM_period_return_44_Price &
    pred_LM_period_return_44_Price > 0 &

    mean_50_pred_LM_period_return_30_Price > mean_100_pred_LM_period_return_30_Price &
    mean_50_pred_LM_period_return_30_Price > mean_2000_pred_LM_period_return_30_Price &
    pred_LM_period_return_30_Price > 0) ",
    Asset_Var = 'EUR_USD',
    win_thresh = 5,
    trade_direction = "Long",
    actual_wins_losses = actual_wins_losses
  ) {

    tagged_trade_combined <-
      tagged_trade_col_data %>%
      ungroup() %>%
      filter(Asset == Asset_Var) %>%
      mutate(
        trade_col =
          eval(parse(text = trade_statement)),
        trade_col =
          ifelse(trade_col == TRUE, trade_direction, paste0("No Trade ", trade_direction) )
      )  %>%
      dplyr::select(Date, Asset,trade_col,
                    -contains("period_return_") ) %>%
      distinct() %>%
      left_join(actual_wins_losses %>%
                  dplyr::select(Date, Asset, trade_col,  contains("period_return_")) %>%
                  filter(Asset == Asset_Var) %>%
                  filter(trade_col == trade_direction) %>%
                  dplyr::select(-trade_col) %>%
                  dplyr::select(Date, Asset,  contains("period_return_")) %>%
                  distinct()
      ) %>%
      pivot_longer(-c(Date, Asset, trade_col),
                   values_to = "Returns", names_to = "Period") %>%
      mutate(
        Period = str_remove_all(Period, "[A-Z]+|[a-z]+|_") %>% str_trim() %>% as.numeric()
      )

    summary_data <-
      tagged_trade_combined %>%
      filter(!is.na(trade_col)) %>%
      mutate(
        wins = ifelse(
          Returns > win_thresh,
          1,
          0
        )
      ) %>%
      group_by(Asset, trade_col, Period) %>%
      summarise(
        total_trades = n_distinct(Date),
        wins = sum(wins, na.rm = T),
        Total_Returns = sum(Returns, na.rm = T),
        Average_Return = mean(Returns, na.rm = T),
        Return_25 = quantile(Returns, 0.25, na.rm = T),
        Return_75 = quantile(Returns, 0.75, na.rm = T)

      ) %>%
      ungroup() %>%
      mutate(
        perc =wins/total_trades
      ) %>%
      mutate(
        trade_statement = trade_statement
      )


    portfolio_ts <-
      tagged_trade_combined %>%
      filter(!is.na(trade_col)) %>%
      group_by(trade_col, Period) %>%
      arrange(Date, .by_group = TRUE) %>%
      group_by(trade_col, Period) %>%
      mutate(
        Total_Returns_cumulative =
          cumsum(Returns)
      )

    rm(tagged_trade_combined,
       tagged_trade_col_data)

    return(
      list(
        "asset_summary" = summary_data,
        "portfolio_ts" = portfolio_ts
      )
    )

  }


#' single_asset_algo_generate_models
#'
#' @param All_Daily_Data
#' @param Indices_Metals_Bonds
#' @param raw_macro_data
#' @param currency_conversion
#' @param asset_infor
#' @param start_index
#' @param end_index
#' @param risk_dollar_value
#' @param trade_direction
#' @param stop_value_var
#' @param profit_value_var
#' @param period_var
#' @param bin_var_col
#' @param date_train_end_pre
#' @param date_train_phase_2_end_pre
#' @param training_date_start_post
#' @param training_date_end_post
#' @param model_data_store_path
#' @param save_path
#'
#' @returns
#' @export
#'
#' @examples
single_asset_algo_generate_models <-
  function(
    All_Daily_Data = All_Daily_Data,
    Indices_Metals_Bonds = Indices_Metals_Bonds,
    raw_macro_data = raw_macro_data,
    currency_conversion = currency_conversion,
    asset_infor = asset_infor,
    start_index = 1,
    end_index = 40,
    risk_dollar_value = 15,
    trade_direction = "Long",
    stop_value_var = 5,
    profit_value_var = 30,
    period_var = 24,
    bin_var_col = c("period_return_20_Price", "period_return_24_Price", "period_return_28_Price"),
    date_train_end_pre = "2023-06-01",
    date_train_phase_2_end_pre = "2024-06-01",
    training_date_start_post = "2024-07-04",
    training_date_end_post = "2025-09-01",
    test_end_date = as.character(today()),
    post_bins_cols =
      c("period_return_24_Price",
        "period_return_30_Price",
        "period_return_44_Price"),
    post_dependant_threshold = 5,
    post_dependant_var = "period_return_24_Price",
    model_data_store_path = "C:/Users/nikhi/Documents/trade_data/Day_Trader_Single_Asset_V2_trade_store_stop_2.db",
    save_path = "C:/Users/nikhi/Documents/trade_data/Day_Trader_Single_Asset_V2_trade_store_stop_2"
  ) {

    equity_index <-
      get_equity_index(index_data = Indices_Metals_Bonds[[1]])

    gold_index <-
      get_Gold_index(index_data = Indices_Metals_Bonds[[1]])

    silver_index <-
      get_silver_index(index_data = Indices_Metals_Bonds[[1]])

    bonds_index <-
      get_bonds_index(index_data = Indices_Metals_Bonds[[1]])

    USD_index <-
      get_USD_index_for_models(index_data = Indices_Metals_Bonds[[1]])

    EUR_index <-
      get_EUR_index_for_models(index_data = Indices_Metals_Bonds[[1]])

    GBP_index <-
      get_GBP_index_for_models(index_data = Indices_Metals_Bonds[[1]])

    AUD_index <-
      get_AUD_index_for_models(index_data = Indices_Metals_Bonds[[1]])

    COMMOD_index <-
      get_COMMOD_index_for_models(index_data = Indices_Metals_Bonds[[1]])

    USD_STOCKS_index <-
      get_USD_AND_STOCKS_index_for_models(index_data = Indices_Metals_Bonds[[1]])

    NZD_index <-
      get_NZD_index_for_models(index_data = Indices_Metals_Bonds[[1]])

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
        raw_macro_data = raw_macro_data,
        lag_days = 1,
        date_start = "2011-01-01",
        end_date = today() %>% as.character(),
        first_difference = TRUE,
        scale_values = FALSE
      )

    gdp_data <-
      get_GDP_countries(
        raw_macro_data = raw_macro_data,
        lag_days = 1
      )

    unemp_data <-
      get_unemp_countries(
        raw_macro_data = raw_macro_data,
        lag_days = 1
      )

    manufac_pmi <-
      get_manufac_countries(
        raw_macro_data = raw_macro_data,
        lag_days = 1
      )

    USD_Macro <-
      get_additional_USD_Macro(
        raw_macro_data = raw_macro_data,
        lag_days = 1
      )

    EUR_Macro <-
      get_additional_EUR_Macro(
        raw_macro_data = raw_macro_data,
        lag_days = 1
      )

    indicator_mapping <- list(
      Asset = c("EUR_USD", #1
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
      couplua_assets =
        list(
          # EUR_USD
          c("XAU_EUR", "XAG_EUR", "EUR_JPY", "EU50_EUR", "EUR_AUD", "EUR_GBP",
            "SPX500_USD", "XAU_USD", "USD_JPY", "GBP_USD", "EUR_NZD", "XAG_GBP", "XAU_GBP",
            "EUR_SEK", "USD_CAD") %>% unique(), #1

          # EU50_EUR
          c("XAU_EUR", "XAG_EUR", "XAU_USD", "UK100_GBP", "SG30_SGD", "EUR_GBP", "SPX500_USD",
            "SPX500_USD", "XAU_USD", "AU200_AUD", "CH20_CHF", "US2000_USD",
            "XAG_GBP", "XAU_GBP", "WTICO_USD", "FR40_EUR", "HK33_HKD") %>% unique(), #2

          # SPX500_USD
          c("US2000_USD", "AU200_AUD", "USB10Y_USD", "UK100_GBP", "XAU_USD", "EU50_EUR",
            "HK33_HKD", "FR40_EUR", "WTICO_USD", "USD_JPY", "EUR_USD", "GBP_USD", "AU200_AUD",
            "SG30_SGD", "XAU_EUR", "XAG_EUR", "XAG_GBP", "XAU_GBP", "XAG_USD" ) %>% unique(), #3

          # US2000_USD
          c("SPX500_USD",  "AU200_AUD", "USB10Y_USD", "UK100_GBP", "XAU_USD", "EU50_EUR",
            "HK33_HKD", "FR40_EUR", "WTICO_USD", "USD_JPY", "EUR_USD", "GBP_USD", "AU200_AUD",
            "SG30_SGD", "XAU_EUR", "XAG_EUR", "XAG_GBP", "XAU_GBP","XAG_USD" ) %>% unique(), #4

          # USB10Y_USD
          c("SPX500_USD",  "AU200_AUD", "UK100_GBP", "XAU_USD", "EU50_EUR",
            "HK33_HKD", "FR40_EUR", "WTICO_USD", "USD_JPY", "EUR_USD",
            "XAU_EUR", "AU200_AUD", "XAG_USD",
            "SG30_SGD", "XAU_EUR", "XAG_EUR", "XAG_GBP", "XAU_GBP") %>% unique(), #5

          # USD_JPY
          c("EUR_JPY", "XAU_JPY", "XAG_JPY", "GBP_JPY", "XAU_USD", "SPX500_USD",
            "XAG_USD","NZD_USD", "AUD_USD", "EUR_USD", "GBP_USD", "USD_CAD",
            "USD_SEK", "USD_SGD", "USB10Y_USD") %>% unique(), #6

          # AUD_USD
          c("XCU_USD", "AU200_AUD", "XAU_AUD", "GBP_AUD", "XAU_USD", "EUR_AUD",
            "XAG_USD","NZD_USD", "USD_JPY", "EUR_USD", "GBP_USD", "USD_CAD",
            "USD_SEK", "USD_SGD", "USB10Y_USD", "NZD_USD") %>% unique(), #7

          # EUR_GBP
          c("GBP_USD", "EUR_USD", "XAU_EUR", "XAU_GBP", "GBP_JPY", "EUR_JPY",
            "XAG_EUR", "XAG_GBP", "USD_JPY", "UK100_GBP", "FR40_EUR", "EU50_EUR",
            "EUR_SEK", "USD_SEK", "EUR_AUD", "EUR_NZD", "EUR_SEK") %>% unique(), #8

          # AU200_AUD
          c("XCU_USD", "US2000_USD", "UK100_GBP", "XAU_USD", "EU50_EUR",
            "HK33_HKD", "FR40_EUR", "WTICO_USD", "GBP_AUD", "EUR_AUD",
            "SG30_SGD", "XAU_EUR", "XAG_EUR", "XAG_GBP", "XAU_GBP", "XAG_USD" ) %>% unique(), #9

          # EUR_AUD
          c("XCU_USD", "AU200_AUD", "XAU_AUD", "GBP_AUD", "XAU_USD", "AUD_USD",
            "XAG_USD","NZD_USD", "USD_JPY", "EUR_USD",
            "USB10Y_USD", "NZD_USD", "FR40_EUR", "EU50_EUR",
            "EUR_SEK", "EUR_NZD", "EUR_SEK") %>% unique(), #10

          # WTICO_USD
          c("NATGAS_USD", "XAG_USD", "BCO_USD", "SPX500_USD", "UK10YB_GBP", "XAU_USD",
            "US2000_USD", "AU200_AUD", "USB10Y_USD", "UK100_GBP", "XAU_USD", "EU50_EUR",
            "HK33_HKD", "FR40_EUR", "USD_JPY", "EUR_USD", "GBP_USD",
            "XAU_EUR", "XAG_EUR", "XAG_GBP", "XAU_GBP") %>% unique(), #11

          # "UK100_GBP", #12
          c("XAU_EUR", "XAG_EUR", "XAU_USD", "SG30_SGD", "EUR_GBP", "US2000_USD",
            "SPX500_USD", "XAU_USD", "AU200_AUD", "CH20_CHF", "UK10YB_GBP", "USB10Y_USD",
            "XAG_GBP", "XAU_GBP", "WTICO_USD", "FR40_EUR", "HK33_HKD") %>% unique(), #12

          # "USD_CAD", #13
          c("XAU_JPY", "XAU_GBP", "XAU_EUR", "XAU_USD", "EUR_JPY", "GBP_JPY",
            "XAG_USD","NZD_USD", "USD_JPY", "EUR_USD", "GBP_USD", "GBP_CAD",
            "USD_SEK", "USD_SGD", "USB10Y_USD") %>% unique(), #13

          # "GBP_USD", #14
          c("GBP_JPY", "GBP_CAD", "GBP_AUD", "GBP_NZD", "XAU_GBP", "XAG_GBP", "UK100_GBP",
            "XAU_USD", "XAG_USD", "EUR_GBP", "EUR_USD", "XAG_EUR", "XAU_EUR", "USD_JPY",
            "EUR_JPY", "UK10YB_GBP", "AUD_USD", "USD_SEK", "USD_CAD") %>% unique(), #14

          # "GBP_CAD", #15
          c("GBP_JPY", "GBP_USD", "GBP_AUD", "GBP_NZD", "XAU_GBP", "XAG_GBP", "UK100_GBP",
            "XAU_USD", "XAG_USD", "EUR_GBP", "EUR_USD", "XAG_EUR", "XAU_EUR", "USD_JPY",
            "EUR_JPY", "UK10YB_GBP", "AUD_USD", "USD_SEK", "USD_CAD") %>% unique(), #15

          # "EUR_JPY", #16
          c("GBP_USD", "EUR_USD", "XAU_EUR", "XAU_JPY", "USD_JPY", "EUR_AUD",
            "EUR_GBP", "EUR_NZD", "EUR_SEK", "XAG_EUR", "XAU_USD", "XAG_USD", "USD_JPY",
            "GBP_JPY", "FR40_EUR", "EU50_EUR") %>% unique(), #16

          # "EUR_NZD", #17
          c("EUR_AUD", "EUR_USD", "XAU_EUR", "XAU_AUD", "NZD_USD", "EUR_JPY", "EUR_GBP",
            "GBP_NZD", "XAG_NZD", "XAG_EUR", "XAU_USD", "XAG_USD", "EUR_SEK",
            "FR40_EUR", "EU50_EUR", "AU200_AUD") %>% unique(), #17

          # "XAG_USD", #18
          c("XAG_JPY", "XAG_GBP", "XAG_EUR", "XAG_AUD", "XAU_USD", "EU50_EUR", "SPX500_USD",
            "XAG_NZD", "XAU_AUD", "XAU_GBP", "XAU_JPY", "XAU_EUR", "EUR_USD", "USD_JPY",
            "GBP_USD", "AUD_USD", "USD_CAD", "USD_SEK") %>% unique(), #18

          # "XAG_EUR", #19
          c("XAG_JPY", "XAG_GBP", "XAG_USD", "XAG_AUD", "XAU_USD", "EU50_EUR", "SPX500_USD",
            "XAG_NZD", "XAU_AUD", "XAU_GBP", "XAU_JPY", "XAU_EUR", "EUR_USD", "EUR_JPY",
            "EUR_GBP", "EUR_AUD", "EUR_SEK", "EUR_NZD") %>% unique(), #19

          # "XAG_AUD", #20
          c("XAG_JPY", "XAG_GBP", "XAG_USD", "XAG_EUR", "XAU_USD", "EU50_EUR", "SPX500_USD",
            "XAG_NZD", "XAU_AUD", "XAU_GBP", "XAU_JPY", "XAU_EUR", "AU200_AUD",
            "AUD_USD", "EUR_AUD", "GBP_AUD") %>% unique(), #20

          # "XAG_NZD", #21
          c("XAG_JPY", "XAG_GBP", "XAG_USD", "XAG_EUR", "XAU_USD", "EU50_EUR", "SPX500_USD",
            "XAG_AUD", "XAU_AUD", "XAU_GBP", "XAU_JPY", "XAU_EUR", "AU200_AUD",
            "NZD_USD", "GBP_NZD", "EUR_NZD") %>% unique(), #21

          # "HK33_HKD", #22
          c("US2000_USD", "AU200_AUD", "USB10Y_USD", "UK100_GBP", "XAU_USD", "EU50_EUR",
            "SPX500_USD", "FR40_EUR", "WTICO_USD", "USD_JPY", "EUR_USD", "GBP_USD", "AU200_AUD",
            "SG30_SGD", "XAU_EUR", "XAG_EUR", "XAG_GBP", "XAU_GBP", "XAG_USD") %>% unique(), #22

          # "FR40_EUR" #23
          c("UK100_GBP", "EU50_EUR", "XAG_USD", "AU200_AUD",
            "XAU_USD", "USB10Y_USD", "SPX500_USD", "EUR_USD", "EUR_AUD",
            "XAU_EUR", "XAG_EUR", "EUR_NZD", "EUR_JPY") %>% unique(), #23

          # "BTC_USD", #24
          c("US2000_USD", "AU200_AUD", "USB10Y_USD", "UK100_GBP", "XAU_USD", "EU50_EUR",
            "HK33_HKD", "FR40_EUR", "WTICO_USD", "USD_JPY", "EUR_USD", "GBP_USD", "AU200_AUD",
            "SG30_SGD", "XAU_EUR", "XAG_EUR", "XAG_GBP", "XAU_GBP", "XAG_USD" ) %>% unique(), #24

          # "XAG_GBP", #25
          c("XAG_JPY", "XAG_NZD", "XAG_USD", "XAG_EUR", "XAU_USD", "EU50_EUR", "SPX500_USD",
            "XAG_AUD", "XAU_AUD", "XAU_GBP", "XAU_JPY", "XAU_EUR", "UK100_GBP",
            "GBP_USD", "GBP_NZD", "GBP_AUD") %>% unique(), #25

          # "GBP_AUD" #26
          c("GBP_JPY", "GBP_CAD", "GBP_USD", "GBP_NZD", "XAU_GBP", "XAG_GBP", "UK100_GBP",
            "XAU_AUD", "XAG_USD", "EUR_GBP", "EUR_USD", "XAG_AUD", "XAU_EUR", "AU200_AUD",
            "EUR_JPY", "UK10YB_GBP", "AUD_USD", "USD_SEK", "EUR_AUD") %>% unique(), #26

          # "USD_SEK" #27
          c("AUD_USD", "EUR_USD", "GBP_USD", "USD_JPY",
            "XAU_USD", "USD_CAD", "NZD_USD", "XAG_USD", "XAG_USD") %>% unique(), #27

          # "USD_SGD" #28
          c("AUD_USD", "EUR_USD", "GBP_USD", "USD_JPY",
            "XAU_USD", "USD_CAD", "NZD_USD", "XAG_USD", "WTICO_USD", "BCO_USD",
            "XCU_USD", "USD_SEK", "SPX500_USD", "EU50_EUR", "UK100_GBP",
            "NATGAS_USD") %>% unique(), #28,

          # "NZD_USD", #29
          c("XAG_JPY", "XAG_GBP", "XAG_USD", "XAG_EUR", "GBP_USD", "EUR_USD", "AUD_USD",
            "XAG_AUD", "XAU_AUD", "USD_CAD", "USD_JPY", "XAU_EUR", "AU200_AUD",
            "GBP_NZD", "EUR_NZD") %>% unique(), #29

          # "GBP_NZD", #30
          c("GBP_JPY", "GBP_CAD", "GBP_USD", "XAU_GBP", "XAG_GBP", "UK100_GBP",
            "GBP_JPY", "XAG_USD", "EUR_GBP", "NZD_USD", "EUR_NZD", "AUD_USD", "XAG_NZD",
            "AUD_USD", "UK10YB_GBP") %>% unique(), #30

          # "XCU_USD", #31
          c("XAG_JPY", "XAG_GBP", "XAG_EUR", "XAG_AUD", "XAU_USD", "EU50_EUR", "SPX500_USD",
            "XAG_NZD", "XAU_AUD", "XAU_GBP", "XAU_JPY", "XAU_EUR", "EUR_USD", "USD_JPY",
            "GBP_USD", "AUD_USD", "USD_CAD", "USD_SEK", "XAG_USD") %>% unique(), #31

          # "NATGAS_USD" #32
          c("XAG_JPY", "XAG_GBP", "XAG_EUR", "XAG_AUD", "XAU_USD", "EU50_EUR", "SPX500_USD",
            "XAG_NZD", "XAU_AUD", "XAU_GBP", "XAU_JPY", "XAU_EUR", "EUR_USD", "USD_JPY",
            "GBP_USD", "AUD_USD", "USD_CAD", "WTICO_USD", "XAG_USD") %>% unique(), #32

          # "GBP_JPY" #33
          c("GBP_CAD", "GBP_USD", "XAU_GBP", "XAG_GBP", "UK100_GBP",
            "GBP_NZD", "XAG_USD", "EUR_GBP", "EUR_JPY", "XAU_JPY", "USD_JPY", "XAG_JPY",
            "AUD_USD", "UK10YB_GBP") %>% unique(), #33

          # "SG30_SGD" #34
          c("AUD_USD", "EUR_USD", "GBP_USD", "USD_JPY",
            "XAU_USD", "US2000_USD", "NZD_USD", "XAG_USD", "WTICO_USD", "BCO_USD",
            "XCU_USD", "HK33_HKD", "SPX500_USD", "EU50_EUR", "UK100_GBP",
            "NATGAS_USD"), #34

          # "XAU_USD", #35
          c("XAG_JPY", "XAG_GBP", "XAG_EUR", "XAG_AUD", "XAG_USD", "EU50_EUR", "SPX500_USD",
            "XAG_NZD", "XAU_AUD", "XAU_GBP", "XAU_JPY", "XAU_EUR", "EUR_USD", "USD_JPY",
            "GBP_USD", "AUD_USD", "USD_CAD", "USD_SEK") %>% unique(), #35

          # "EUR_SEK", #36
          c("GBP_USD", "EUR_USD", "XAU_EUR", "USD_SEK", "EUR_AUD",
            "EUR_GBP", "EUR_NZD", "EUR_JPY", "XAG_EUR", "XAU_USD", "XAG_USD",
            "GBP_JPY", "FR40_EUR", "EU50_EUR") %>% unique(), #36

          # "XAU_AUD", #37
          c("XAG_JPY", "XAG_GBP", "XAG_EUR", "XAG_AUD", "XAG_USD", "EU50_EUR", "SPX500_USD",
            "XAG_NZD", "XAU_USD", "XAU_GBP", "XAU_JPY", "XAU_EUR", "AU200_AUD", "USD_JPY",
            "GBP_AUD", "AUD_USD", "EUR_AUD", "AUD_USD") %>% unique(), #37

          # "UK10YB_GBP", #38
          c("XAU_GBP", "XAG_GBP", "XAU_USD", "EUR_GBP", "XAU_EUR", "GBP_AUD", "GBP_NZD",
            "SPX500_USD", "BCO_USD", "UK100_GBP", "USB10Y_USD", "GBP_CAD", "GBP_JPY",
            "XAG_GBP", "WTICO_USD", "GBP_USD") %>% unique(), #38

          # "JP225Y_JPY" #39
          c("US2000_USD", "AU200_AUD", "USB10Y_USD", "UK100_GBP", "XAU_USD", "EU50_EUR",
            "SPX500_USD", "FR40_EUR", "WTICO_USD", "USD_JPY", "EUR_USD", "AU200_AUD",
            "SG30_SGD", "XAU_EUR", "XAG_JPY", "XAG_GBP", "XAU_JPY", "XAG_USD") %>% unique(), #39

          # "ETH_USD" #40
          c("US2000_USD", "AU200_AUD", "USB10Y_USD", "UK100_GBP", "XAU_USD", "EU50_EUR",
            "HK33_HKD", "FR40_EUR", "WTICO_USD", "USD_JPY", "EUR_USD", "GBP_USD", "AU200_AUD",
            "BTC_USD", "XAU_EUR", "XAG_EUR", "XAG_GBP", "XAU_GBP", "XAG_USD" ) %>% unique()
        ),
      countries_for_int_strength =
        list(
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #1
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #2
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #3
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #4
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #5
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #6
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #7
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #8
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #9
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #10
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #11
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #12
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #13
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #14
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #15
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #16

          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #17
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #18
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #19
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #20
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #21
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #22
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #23
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #24
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #25
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #26
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #27
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #28
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #29
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #30
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #31
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #32
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #33
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #34
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #35
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #36
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #37
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #38
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #39
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD") #40
        )
    )

    assets_to_analyse <-
      indicator_mapping$Asset

    temp_actual_wins_losses <- list()

    for (i in 1:length(assets_to_analyse)) {

      temp_actual_wins_losses[[i]] <-
        create_running_profits(
          asset_of_interest = assets_to_analyse[i],
          asset_data = Indices_Metals_Bonds,
          stop_factor = stop_value_var,
          profit_factor = profit_value_var,
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
        trade_return_dollar_aud = !!as.name(glue::glue("period_return_{period_var}_Price") ),

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
        stop_factor = stop_value_var,
        profit_factor = profit_value_var,
        periods_ahead = period_var
      )

    model_data_store_db <-
      connect_db(model_data_store_path)

    date_test_start = as.character(as_date(date_train_phase_2_end_pre) + days(3))
    c = 0
    redo_db = TRUE

    for (j in 1:length(indicator_mapping$Asset) ) {

      countries_for_int_strength <-
        unlist(indicator_mapping$countries_for_int_strength[j])
      couplua_assets = unlist(indicator_mapping$couplua_assets[j])
      Asset_of_interest = unlist(indicator_mapping$Asset[j])

      single_asset_Logit_indicator_adv_gen_models(
        asset_data = Indices_Metals_Bonds[[1]],
        All_Daily_Data = All_Daily_Data,
        Asset_of_interest = Asset_of_interest,
        actual_wins_losses = actual_wins_losses,
        interest_rates = interest_rates,
        cpi_data = cpi_data,
        sentiment_index = sentiment_index,
        gdp_data = gdp_data,
        unemp_data = unemp_data,
        manufac_pmi = manufac_pmi,
        USD_Macro = USD_Macro,
        EUR_Macro = EUR_Macro,
        equity_index = equity_index,
        gold_index = gold_index,
        silver_index = silver_index,
        bonds_index = bonds_index,
        USD_index = USD_index,
        EUR_index = EUR_index,
        GBP_index = GBP_index,
        AUD_index = AUD_index,
        COMMOD_index = COMMOD_index,
        USD_STOCKS_index = USD_STOCKS_index,
        NZD_index = NZD_index,
        countries_for_int_strength = countries_for_int_strength,

        date_train_end = date_train_end_pre,
        date_train_phase_2_end = date_train_phase_2_end_pre,
        date_test_start = as.character(date_test_start),

        couplua_assets = couplua_assets,
        stop_value_var = stop_value_var,
        profit_value_var = profit_value_var,
        period_var = period_var,
        bin_var_col = bin_var_col,
        trade_direction = trade_direction,
        save_path = save_path
      )

      long_sim <-
        single_asset_Logit_indicator_adv_get_preds(
          asset_data = Indices_Metals_Bonds[[1]],
          All_Daily_Data = All_Daily_Data,
          Asset_of_interest = Asset_of_interest,
          actual_wins_losses = actual_wins_losses,

          interest_rates = interest_rates,
          cpi_data = cpi_data,
          sentiment_index = sentiment_index,
          gdp_data = gdp_data,
          unemp_data = unemp_data,
          manufac_pmi = manufac_pmi,
          USD_Macro = USD_Macro,
          EUR_Macro = EUR_Macro,

          equity_index = equity_index,
          gold_index = gold_index,
          silver_index = silver_index,
          bonds_index = bonds_index,
          USD_index = USD_index,
          EUR_index = EUR_index,
          GBP_index = GBP_index,
          AUD_index = AUD_index,
          COMMOD_index = COMMOD_index,
          USD_STOCKS_index = USD_STOCKS_index,
          NZD_index = NZD_index,

          countries_for_int_strength = countries_for_int_strength,

          date_train_end = date_train_end_pre,
          date_train_phase_2_end = date_train_phase_2_end_pre,
          date_test_start = as.character(date_test_start),

          couplua_assets = couplua_assets,

          stop_value_var = stop_value_var,
          profit_value_var = profit_value_var,
          period_var = period_var,

          bin_var_col = bin_var_col,
          trade_direction = trade_direction,
          save_path = save_path
        )

      long_sim_transformed <-
        long_sim %>%
        filter(Date >= date_test_start) %>%
        left_join(actual_wins_losses %>%
                    filter(trade_col == trade_direction,
                           stop_factor == stop_value_var,
                           profit_factor == profit_value_var)
        ) %>%
        mutate(
          trade_col = trade_direction,
          test_end_date = test_end_date,
          date_train_end = date_train_end_pre,
          date_train_phase_2_end = date_train_phase_2_end_pre,
          date_test_start = date_test_start,
          sim_index = 1,
          bin_var_col = paste(bin_var_col, collapse = ", ")
        )

      complete_sim <-
        list(long_sim_transformed) %>%
        map_dfr(bind_rows)

      if(dim(complete_sim)[1] > 0) {
        c = c + 1
        if(redo_db == TRUE & c == 1) {
          write_table_sql_lite(.data = complete_sim,
                               table_name = "single_asset_improved",
                               conn = model_data_store_db,
                               overwrite_true = TRUE)
          redo_db = FALSE
        }

        if(redo_db == FALSE) {
          append_table_sql_lite(.data = complete_sim,
                                table_name = "single_asset_improved",
                                conn = model_data_store_db)
        }
      }

    }

    DBI::dbDisconnect(model_data_store_db)
    gc()
    rm(model_data_store_db)

    indicator_data <-
      get_indicator_pred_from_db(
        model_data_store_path = model_data_store_path,
        table_name = "single_asset_improved"
      )

    post_bins_cols %>%
      map(
        ~
          prepare_post_ss_gen2_model(
            indicator_data = indicator_data,
            actual_wins_losses = actual_wins_losses,
            new_post_DB = TRUE,
            new_sym = TRUE,
            post_model_data_save_path = save_path,
            dependant_var = .x,
            dependant_threshold = post_dependant_threshold,
            training_date_start = training_date_start_post,
            training_date_end = training_date_end_post
          )
      )

  }


#' single_asset_algo_generate_preds
#'
#' @param All_Daily_Data
#' @param Indices_Metals_Bonds
#' @param raw_macro_data
#' @param currency_conversion
#' @param asset_infor
#' @param start_index
#' @param end_index
#' @param risk_dollar_value
#' @param trade_direction
#' @param stop_value_var
#' @param profit_value_var
#' @param period_var
#' @param bin_var_col
#' @param date_train_end_pre
#' @param date_train_phase_2_end_pre
#' @param training_date_start_post
#' @param training_date_end_post
#' @param model_data_store_path
#' @param save_path
#'
#' @returns
#' @export
#'
#' @examples
single_asset_algo_generate_preds <-
  function(
    All_Daily_Data = All_Daily_Data,
    Indices_Metals_Bonds = Indices_Metals_Bonds,
    raw_macro_data = raw_macro_data,
    currency_conversion = currency_conversion,
    asset_infor = asset_infor,
    start_index = 1,
    end_index = 40,
    risk_dollar_value = 15,
    trade_direction = "Long",
    stop_value_var = 5,
    profit_value_var = 30,
    period_var = 24,
    bin_var_col = c("period_return_20_Price", "period_return_24_Price", "period_return_28_Price"),
    date_train_end_pre = "2023-06-01",
    date_train_phase_2_end_pre = "2024-06-01",
    training_date_start_post = "2024-07-04",
    training_date_end_post = "2025-09-01",
    test_end_date = as.character(today()),
    post_bins_cols =
      c("period_return_24_Price",
        "period_return_30_Price",
        "period_return_44_Price"),
    post_dependant_threshold = 5,
    post_dependant_var = "period_return_24_Price",
    model_data_store_path = "C:/Users/nikhi/Documents/trade_data/Day_Trader_Single_Asset_V2_trade_store_stop_2.db",
    save_path = "C:/Users/nikhi/Documents/trade_data/Day_Trader_Single_Asset_V2_trade_store_stop_2/"
  ) {

    equity_index <-
      get_equity_index(index_data = Indices_Metals_Bonds[[1]])

    gold_index <-
      get_Gold_index(index_data = Indices_Metals_Bonds[[1]])

    silver_index <-
      get_silver_index(index_data = Indices_Metals_Bonds[[1]])

    bonds_index <-
      get_bonds_index(index_data = Indices_Metals_Bonds[[1]])

    USD_index <-
      get_USD_index_for_models(index_data = Indices_Metals_Bonds[[1]])

    EUR_index <-
      get_EUR_index_for_models(index_data = Indices_Metals_Bonds[[1]])

    GBP_index <-
      get_GBP_index_for_models(index_data = Indices_Metals_Bonds[[1]])

    AUD_index <-
      get_AUD_index_for_models(index_data = Indices_Metals_Bonds[[1]])

    COMMOD_index <-
      get_COMMOD_index_for_models(index_data = Indices_Metals_Bonds[[1]])

    USD_STOCKS_index <-
      get_USD_AND_STOCKS_index_for_models(index_data = Indices_Metals_Bonds[[1]])

    NZD_index <-
      get_NZD_index_for_models(index_data = Indices_Metals_Bonds[[1]])

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
        raw_macro_data = raw_macro_data,
        lag_days = 1,
        date_start = "2011-01-01",
        end_date = today(tz = "Australia/Canberra") %>% as.character(),
        first_difference = TRUE,
        scale_values = FALSE
      )

    gdp_data <-
      get_GDP_countries(
        raw_macro_data = raw_macro_data,
        lag_days = 1
      )

    unemp_data <-
      get_unemp_countries(
        raw_macro_data = raw_macro_data,
        lag_days = 1
      )

    manufac_pmi <-
      get_manufac_countries(
        raw_macro_data = raw_macro_data,
        lag_days = 1
      )

    USD_Macro <-
      get_additional_USD_Macro(
        raw_macro_data = raw_macro_data,
        lag_days = 1
      )

    EUR_Macro <-
      get_additional_EUR_Macro(
        raw_macro_data = raw_macro_data,
        lag_days = 1
      )

    indicator_mapping <- list(
      Asset = c("EUR_USD", #1
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
                # "FR40_EUR", #23
                # "BTC_USD", #24
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
      couplua_assets =
        list(
          # EUR_USD
          c("XAU_EUR", "XAG_EUR", "EUR_JPY", "EU50_EUR", "EUR_AUD", "EUR_GBP",
            "SPX500_USD", "XAU_USD", "USD_JPY", "GBP_USD", "EUR_NZD", "XAG_GBP", "XAU_GBP",
            "EUR_SEK", "USD_CAD") %>% unique(), #1

          # EU50_EUR
          c("XAU_EUR", "XAG_EUR", "XAU_USD", "UK100_GBP", "SG30_SGD", "EUR_GBP", "SPX500_USD",
            "SPX500_USD", "XAU_USD", "AU200_AUD", "CH20_CHF", "US2000_USD",
            "XAG_GBP", "XAU_GBP", "WTICO_USD", "FR40_EUR", "HK33_HKD") %>% unique(), #2

          # SPX500_USD
          c("US2000_USD", "AU200_AUD", "USB10Y_USD", "UK100_GBP", "XAU_USD", "EU50_EUR",
            "HK33_HKD", "FR40_EUR", "WTICO_USD", "USD_JPY", "EUR_USD", "GBP_USD", "AU200_AUD",
            "SG30_SGD", "XAU_EUR", "XAG_EUR", "XAG_GBP", "XAU_GBP", "XAG_USD" ) %>% unique(), #3

          # US2000_USD
          c("SPX500_USD",  "AU200_AUD", "USB10Y_USD", "UK100_GBP", "XAU_USD", "EU50_EUR",
            "HK33_HKD", "FR40_EUR", "WTICO_USD", "USD_JPY", "EUR_USD", "GBP_USD", "AU200_AUD",
            "SG30_SGD", "XAU_EUR", "XAG_EUR", "XAG_GBP", "XAU_GBP","XAG_USD" ) %>% unique(), #4

          # USB10Y_USD
          c("SPX500_USD",  "AU200_AUD", "UK100_GBP", "XAU_USD", "EU50_EUR",
            "HK33_HKD", "FR40_EUR", "WTICO_USD", "USD_JPY", "EUR_USD",
            "XAU_EUR", "AU200_AUD", "XAG_USD",
            "SG30_SGD", "XAU_EUR", "XAG_EUR", "XAG_GBP", "XAU_GBP") %>% unique(), #5

          # USD_JPY
          c("EUR_JPY", "XAU_JPY", "XAG_JPY", "GBP_JPY", "XAU_USD", "SPX500_USD",
            "XAG_USD","NZD_USD", "AUD_USD", "EUR_USD", "GBP_USD", "USD_CAD",
            "USD_SEK", "USD_SGD", "USB10Y_USD") %>% unique(), #6

          # AUD_USD
          c("XCU_USD", "AU200_AUD", "XAU_AUD", "GBP_AUD", "XAU_USD", "EUR_AUD",
            "XAG_USD","NZD_USD", "USD_JPY", "EUR_USD", "GBP_USD", "USD_CAD",
            "USD_SEK", "USD_SGD", "USB10Y_USD", "NZD_USD") %>% unique(), #7

          # EUR_GBP
          c("GBP_USD", "EUR_USD", "XAU_EUR", "XAU_GBP", "GBP_JPY", "EUR_JPY",
            "XAG_EUR", "XAG_GBP", "USD_JPY", "UK100_GBP", "FR40_EUR", "EU50_EUR",
            "EUR_SEK", "USD_SEK", "EUR_AUD", "EUR_NZD", "EUR_SEK") %>% unique(), #8

          # AU200_AUD
          c("XCU_USD", "US2000_USD", "UK100_GBP", "XAU_USD", "EU50_EUR",
            "HK33_HKD", "FR40_EUR", "WTICO_USD", "GBP_AUD", "EUR_AUD",
            "SG30_SGD", "XAU_EUR", "XAG_EUR", "XAG_GBP", "XAU_GBP", "XAG_USD" ) %>% unique(), #9

          # EUR_AUD
          c("XCU_USD", "AU200_AUD", "XAU_AUD", "GBP_AUD", "XAU_USD", "AUD_USD",
            "XAG_USD","NZD_USD", "USD_JPY", "EUR_USD",
            "USB10Y_USD", "NZD_USD", "FR40_EUR", "EU50_EUR",
            "EUR_SEK", "EUR_NZD", "EUR_SEK") %>% unique(), #10

          # WTICO_USD
          c("NATGAS_USD", "XAG_USD", "BCO_USD", "SPX500_USD", "UK10YB_GBP", "XAU_USD",
            "US2000_USD", "AU200_AUD", "USB10Y_USD", "UK100_GBP", "XAU_USD", "EU50_EUR",
            "HK33_HKD", "FR40_EUR", "USD_JPY", "EUR_USD", "GBP_USD",
            "XAU_EUR", "XAG_EUR", "XAG_GBP", "XAU_GBP") %>% unique(), #11

          # "UK100_GBP", #12
          c("XAU_EUR", "XAG_EUR", "XAU_USD", "SG30_SGD", "EUR_GBP", "US2000_USD",
            "SPX500_USD", "XAU_USD", "AU200_AUD", "CH20_CHF", "UK10YB_GBP", "USB10Y_USD",
            "XAG_GBP", "XAU_GBP", "WTICO_USD", "FR40_EUR", "HK33_HKD") %>% unique(), #12

          # "USD_CAD", #13
          c("XAU_JPY", "XAU_GBP", "XAU_EUR", "XAU_USD", "EUR_JPY", "GBP_JPY",
            "XAG_USD","NZD_USD", "USD_JPY", "EUR_USD", "GBP_USD", "GBP_CAD",
            "USD_SEK", "USD_SGD", "USB10Y_USD") %>% unique(), #13

          # "GBP_USD", #14
          c("GBP_JPY", "GBP_CAD", "GBP_AUD", "GBP_NZD", "XAU_GBP", "XAG_GBP", "UK100_GBP",
            "XAU_USD", "XAG_USD", "EUR_GBP", "EUR_USD", "XAG_EUR", "XAU_EUR", "USD_JPY",
            "EUR_JPY", "UK10YB_GBP", "AUD_USD", "USD_SEK", "USD_CAD") %>% unique(), #14

          # "GBP_CAD", #15
          c("GBP_JPY", "GBP_USD", "GBP_AUD", "GBP_NZD", "XAU_GBP", "XAG_GBP", "UK100_GBP",
            "XAU_USD", "XAG_USD", "EUR_GBP", "EUR_USD", "XAG_EUR", "XAU_EUR", "USD_JPY",
            "EUR_JPY", "UK10YB_GBP", "AUD_USD", "USD_SEK", "USD_CAD") %>% unique(), #15

          # "EUR_JPY", #16
          c("GBP_USD", "EUR_USD", "XAU_EUR", "XAU_JPY", "USD_JPY", "EUR_AUD",
            "EUR_GBP", "EUR_NZD", "EUR_SEK", "XAG_EUR", "XAU_USD", "XAG_USD", "USD_JPY",
            "GBP_JPY", "FR40_EUR", "EU50_EUR") %>% unique(), #16

          # "EUR_NZD", #17
          c("EUR_AUD", "EUR_USD", "XAU_EUR", "XAU_AUD", "NZD_USD", "EUR_JPY", "EUR_GBP",
            "GBP_NZD", "XAG_NZD", "XAG_EUR", "XAU_USD", "XAG_USD", "EUR_SEK",
            "FR40_EUR", "EU50_EUR", "AU200_AUD") %>% unique(), #17

          # "XAG_USD", #18
          c("XAG_JPY", "XAG_GBP", "XAG_EUR", "XAG_AUD", "XAU_USD", "EU50_EUR", "SPX500_USD",
            "XAG_NZD", "XAU_AUD", "XAU_GBP", "XAU_JPY", "XAU_EUR", "EUR_USD", "USD_JPY",
            "GBP_USD", "AUD_USD", "USD_CAD", "USD_SEK") %>% unique(), #18

          # "XAG_EUR", #19
          c("XAG_JPY", "XAG_GBP", "XAG_USD", "XAG_AUD", "XAU_USD", "EU50_EUR", "SPX500_USD",
            "XAG_NZD", "XAU_AUD", "XAU_GBP", "XAU_JPY", "XAU_EUR", "EUR_USD", "EUR_JPY",
            "EUR_GBP", "EUR_AUD", "EUR_SEK", "EUR_NZD") %>% unique(), #19

          # "XAG_AUD", #20
          c("XAG_JPY", "XAG_GBP", "XAG_USD", "XAG_EUR", "XAU_USD", "EU50_EUR", "SPX500_USD",
            "XAG_NZD", "XAU_AUD", "XAU_GBP", "XAU_JPY", "XAU_EUR", "AU200_AUD",
            "AUD_USD", "EUR_AUD", "GBP_AUD") %>% unique(), #20

          # "XAG_NZD", #21
          c("XAG_JPY", "XAG_GBP", "XAG_USD", "XAG_EUR", "XAU_USD", "EU50_EUR", "SPX500_USD",
            "XAG_AUD", "XAU_AUD", "XAU_GBP", "XAU_JPY", "XAU_EUR", "AU200_AUD",
            "NZD_USD", "GBP_NZD", "EUR_NZD") %>% unique(), #21

          # "HK33_HKD", #22
          c("US2000_USD", "AU200_AUD", "USB10Y_USD", "UK100_GBP", "XAU_USD", "EU50_EUR",
            "SPX500_USD", "FR40_EUR", "WTICO_USD", "USD_JPY", "EUR_USD", "GBP_USD", "AU200_AUD",
            "SG30_SGD", "XAU_EUR", "XAG_EUR", "XAG_GBP", "XAU_GBP", "XAG_USD") %>% unique(), #22

          # # "FR40_EUR" #23
          # c("UK100_GBP", "EU50_EUR", "XAG_USD", "AU200_AUD",
          #   "XAU_USD", "USB10Y_USD", "SPX500_USD", "EUR_USD", "EUR_AUD",
          #   "XAU_EUR", "XAG_EUR", "EUR_NZD", "EUR_JPY") %>% unique(), #23

          # # "BTC_USD", #24
          # c("US2000_USD", "AU200_AUD", "USB10Y_USD", "UK100_GBP", "XAU_USD", "EU50_EUR",
          #   "HK33_HKD", "FR40_EUR", "WTICO_USD", "USD_JPY", "EUR_USD", "GBP_USD", "AU200_AUD",
          #   "SG30_SGD", "XAU_EUR", "XAG_EUR", "XAG_GBP", "XAU_GBP", "XAG_USD" ) %>% unique(), #24

          # "XAG_GBP", #25
          c("XAG_JPY", "XAG_NZD", "XAG_USD", "XAG_EUR", "XAU_USD", "EU50_EUR", "SPX500_USD",
            "XAG_AUD", "XAU_AUD", "XAU_GBP", "XAU_JPY", "XAU_EUR", "UK100_GBP",
            "GBP_USD", "GBP_NZD", "GBP_AUD") %>% unique(), #25

          # "GBP_AUD" #26
          c("GBP_JPY", "GBP_CAD", "GBP_USD", "GBP_NZD", "XAU_GBP", "XAG_GBP", "UK100_GBP",
            "XAU_AUD", "XAG_USD", "EUR_GBP", "EUR_USD", "XAG_AUD", "XAU_EUR", "AU200_AUD",
            "EUR_JPY", "UK10YB_GBP", "AUD_USD", "USD_SEK", "EUR_AUD") %>% unique(), #26

          # "USD_SEK" #27
          c("AUD_USD", "EUR_USD", "GBP_USD", "USD_JPY",
            "XAU_USD", "USD_CAD", "NZD_USD", "XAG_USD", "XAG_USD") %>% unique(), #27

          # "USD_SGD" #28
          c("AUD_USD", "EUR_USD", "GBP_USD", "USD_JPY",
            "XAU_USD", "USD_CAD", "NZD_USD", "XAG_USD", "WTICO_USD", "BCO_USD",
            "XCU_USD", "USD_SEK", "SPX500_USD", "EU50_EUR", "UK100_GBP",
            "NATGAS_USD") %>% unique(), #28,

          # "NZD_USD", #29
          c("XAG_JPY", "XAG_GBP", "XAG_USD", "XAG_EUR", "GBP_USD", "EUR_USD", "AUD_USD",
            "XAG_AUD", "XAU_AUD", "USD_CAD", "USD_JPY", "XAU_EUR", "AU200_AUD",
            "GBP_NZD", "EUR_NZD") %>% unique(), #29

          # "GBP_NZD", #30
          c("GBP_JPY", "GBP_CAD", "GBP_USD", "XAU_GBP", "XAG_GBP", "UK100_GBP",
            "GBP_JPY", "XAG_USD", "EUR_GBP", "NZD_USD", "EUR_NZD", "AUD_USD", "XAG_NZD",
            "AUD_USD", "UK10YB_GBP") %>% unique(), #30

          # "XCU_USD", #31
          c("XAG_JPY", "XAG_GBP", "XAG_EUR", "XAG_AUD", "XAU_USD", "EU50_EUR", "SPX500_USD",
            "XAG_NZD", "XAU_AUD", "XAU_GBP", "XAU_JPY", "XAU_EUR", "EUR_USD", "USD_JPY",
            "GBP_USD", "AUD_USD", "USD_CAD", "USD_SEK", "XAG_USD") %>% unique(), #31

          # "NATGAS_USD" #32
          c("XAG_JPY", "XAG_GBP", "XAG_EUR", "XAG_AUD", "XAU_USD", "EU50_EUR", "SPX500_USD",
            "XAG_NZD", "XAU_AUD", "XAU_GBP", "XAU_JPY", "XAU_EUR", "EUR_USD", "USD_JPY",
            "GBP_USD", "AUD_USD", "USD_CAD", "WTICO_USD", "XAG_USD") %>% unique(), #32

          # "GBP_JPY" #33
          c("GBP_CAD", "GBP_USD", "XAU_GBP", "XAG_GBP", "UK100_GBP",
            "GBP_NZD", "XAG_USD", "EUR_GBP", "EUR_JPY", "XAU_JPY", "USD_JPY", "XAG_JPY",
            "AUD_USD", "UK10YB_GBP") %>% unique(), #33

          # "SG30_SGD" #34
          c("AUD_USD", "EUR_USD", "GBP_USD", "USD_JPY",
            "XAU_USD", "US2000_USD", "NZD_USD", "XAG_USD", "WTICO_USD", "BCO_USD",
            "XCU_USD", "HK33_HKD", "SPX500_USD", "EU50_EUR", "UK100_GBP",
            "NATGAS_USD"), #34

          # "XAU_USD", #35
          c("XAG_JPY", "XAG_GBP", "XAG_EUR", "XAG_AUD", "XAG_USD", "EU50_EUR", "SPX500_USD",
            "XAG_NZD", "XAU_AUD", "XAU_GBP", "XAU_JPY", "XAU_EUR", "EUR_USD", "USD_JPY",
            "GBP_USD", "AUD_USD", "USD_CAD", "USD_SEK") %>% unique(), #35

          # "EUR_SEK", #36
          c("GBP_USD", "EUR_USD", "XAU_EUR", "USD_SEK", "EUR_AUD",
            "EUR_GBP", "EUR_NZD", "EUR_JPY", "XAG_EUR", "XAU_USD", "XAG_USD",
            "GBP_JPY", "FR40_EUR", "EU50_EUR") %>% unique(), #36

          # "XAU_AUD", #37
          c("XAG_JPY", "XAG_GBP", "XAG_EUR", "XAG_AUD", "XAG_USD", "EU50_EUR", "SPX500_USD",
            "XAG_NZD", "XAU_USD", "XAU_GBP", "XAU_JPY", "XAU_EUR", "AU200_AUD", "USD_JPY",
            "GBP_AUD", "AUD_USD", "EUR_AUD", "AUD_USD") %>% unique(), #37

          # "UK10YB_GBP", #38
          c("XAU_GBP", "XAG_GBP", "XAU_USD", "EUR_GBP", "XAU_EUR", "GBP_AUD", "GBP_NZD",
            "SPX500_USD", "BCO_USD", "UK100_GBP", "USB10Y_USD", "GBP_CAD", "GBP_JPY",
            "XAG_GBP", "WTICO_USD", "GBP_USD") %>% unique(), #38

          # "JP225Y_JPY" #39
          c("US2000_USD", "AU200_AUD", "USB10Y_USD", "UK100_GBP", "XAU_USD", "EU50_EUR",
            "SPX500_USD", "FR40_EUR", "WTICO_USD", "USD_JPY", "EUR_USD", "AU200_AUD",
            "SG30_SGD", "XAU_EUR", "XAG_JPY", "XAG_GBP", "XAU_JPY", "XAG_USD") %>% unique(), #39

          # "ETH_USD" #40
          c("US2000_USD", "AU200_AUD", "USB10Y_USD", "UK100_GBP", "XAU_USD", "EU50_EUR",
            "HK33_HKD", "FR40_EUR", "WTICO_USD", "USD_JPY", "EUR_USD", "GBP_USD", "AU200_AUD",
            "BTC_USD", "XAU_EUR", "XAG_EUR", "XAG_GBP", "XAU_GBP", "XAG_USD" ) %>% unique()
        ),
      countries_for_int_strength =
        list(
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #1
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #2
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #3
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #4
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #5
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #6
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #7
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #8
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #9
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #10
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #11
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #12
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #13
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #14
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #15
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #16

          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #17
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #18
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #19
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #20
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #21
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #22
          # c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #23
          # c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #24
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #25
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #26
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #27
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #28
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #29
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #30
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #31
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #32
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #33
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #34
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #35
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #36
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #37
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #38
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #39
          c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD") #40
        )
    )

    assets_to_analyse <-
      indicator_mapping$Asset

    all_pred_data <- list()

    date_test_start = as.character(as_date(date_train_phase_2_end_pre) + days(3))

    for (j in start_index:end_index ) {

      tictoc::tic()

      countries_for_int_strength <-
        unlist(indicator_mapping$countries_for_int_strength[j])
      couplua_assets = unlist(indicator_mapping$couplua_assets[j])
      Asset_of_interest = unlist(indicator_mapping$Asset[j])


      long_sim <-
        single_asset_Logit_indicator_adv_get_preds(
          asset_data = Indices_Metals_Bonds[[1]],
          All_Daily_Data = All_Daily_Data,
          Asset_of_interest = Asset_of_interest,
          actual_wins_losses = NULL,

          interest_rates = interest_rates,
          cpi_data = cpi_data,
          sentiment_index = sentiment_index,
          gdp_data = gdp_data,
          unemp_data = unemp_data,
          manufac_pmi = manufac_pmi,
          USD_Macro = USD_Macro,
          EUR_Macro = EUR_Macro,

          equity_index = equity_index,
          gold_index = gold_index,
          silver_index = silver_index,
          bonds_index = bonds_index,
          USD_index = USD_index,
          EUR_index = EUR_index,
          GBP_index = GBP_index,
          AUD_index = AUD_index,
          COMMOD_index = COMMOD_index,
          USD_STOCKS_index = USD_STOCKS_index,
          NZD_index = NZD_index,

          countries_for_int_strength = countries_for_int_strength,

          date_train_end = date_train_end_pre,
          date_train_phase_2_end = date_train_phase_2_end_pre,
          date_test_start = as.character(date_test_start),

          couplua_assets = couplua_assets,

          stop_value_var = stop_value_var,
          profit_value_var = profit_value_var,
          period_var = period_var,

          bin_var_col = bin_var_col,
          trade_direction = trade_direction,
          save_path = save_path
        )

      long_sim_transformed <-
        long_sim %>%
        filter(Date >= date_test_start) %>%
        mutate(
          trade_col = trade_direction,
          test_end_date = test_end_date,
          date_train_end = date_train_end_pre,
          date_train_phase_2_end = date_train_phase_2_end_pre,
          date_test_start = date_test_start,
          sim_index = 1,
          bin_var_col = paste(bin_var_col, collapse = ", ")
        )

      complete_sim <-
        list(long_sim_transformed) %>%
        map_dfr(bind_rows)

      all_pred_data[[j]] <- complete_sim

      rm(complete_sim, long_sim_transformed, long_sim)
      gc()

      tictoc::toc()

    }

    all_pred_data <-
      all_pred_data %>%
      map_dfr(bind_rows)


    post_preds_all <-
      read_post_models_and_get_preds(
        indicator_data = all_pred_data,
        post_model_data_save_path =save_path,
        test_date_start = training_date_start_post,
        test_date_end = as.character(today() + days(100)) ,
        dependant_var = post_dependant_var,
        dependant_threshold = post_dependant_threshold,
        ignore_dependant_var = TRUE
      )


    post_preds_all_rolling <-
      get_rolling_post_preds(
        post_pred_data = post_preds_all,
        rolling_periods = c(3,50,100,200,400,500,2000),
        test_date_start = "2025-10-01",
        test_date_end = as.character(today() + days(100)),
        pred_price_cols = post_bins_cols
      )

    post_preds_all_rolling_and_originals <-
      post_preds_all_rolling %>%
      left_join(
        all_pred_data %>%
          dplyr::select(Date, Asset, contains("pred_combined"),
                        contains("pred_macro"), contains("pred_index"),
                        contains("pred_daily"), contains("pred_copula"),
                        contains("pred_technical")) %>%
          distinct()
      )

    rm(all_pred_data, post_preds_all_rolling, post_preds_all)
    gc()

    return(post_preds_all_rolling_and_originals)

  }


# Use this for Periods 24 only 3.91% Edge with 200 return edge (USE THIS)

#' simulate_factors
#'
#' @param post_preds_all_rolling_and_originals
#' @param actual_wins_losses
#' @param win_thresh
#' @param macro_factor_tech_vec
#' @param tech_factor_vec
#' @param macro_factor_daily_vec
#' @param daily_factor_vec
#' @param macro_factor_post_pred_vec
#' @param post_pred_factor_vec
#' @param macro_factor_copula_vec
#' @param copula_factor_vec
#' @param sim_save_db_path
#'
#' @returns
#' @export
#'
#' @examples
simulate_factors <-
  function(
    post_preds_all_rolling_and_originals = post_preds_all_rolling_and_originals,
    actual_wins_losses = actual_wins_losses,
    win_thresh = 10,
    macro_factor_tech_vec = c(0,10,20),
    tech_factor_vec = c(0,1,2,3),

    macro_factor_daily_vec = c(0,10,20),
    daily_factor_vec = c(0,1,2,3),

    macro_factor_post_pred_vec = c(0,10,20),
    post_pred_factor_vec = c(0,1,2,3),

    macro_factor_copula_vec = c(0,10,20),
    copula_factor_vec = c(0,1,2,3),

    sim_save_db_path = "C:/Users/nikhi/Documents/trade_data/Day_Trader_Single_Asset_V2_condition_sim.db"
  ) {

    c = 0
    save_db <- connect_db(sim_save_db_path)
    redo_db <- TRUE

    for (i in 1:length(macro_factor_tech_vec)) {
      for (j in 1:length(tech_factor_vec)) {
        for (k in 1:length(macro_factor_daily_vec)) {
          for (o in 1:length(daily_factor_vec)) {
            for (p in 1:length(macro_factor_post_pred_vec)) {
              for (q in 1:length(post_pred_factor_vec)) {
                for (r in 1:length(macro_factor_copula_vec)) {
                  for (s in 1:length(copula_factor_vec)) {

                    macro_factor_tech <- macro_factor_tech_vec[i]
                    tech_factor <- tech_factor_vec[j]
                    macro_factor_daily <- macro_factor_daily_vec[k]
                    daily_factor <- daily_factor_vec[o]
                    macro_factor_post_pred <- macro_factor_post_pred_vec[p]
                    post_pred_factor <- post_pred_factor_vec[q]
                    macro_factor_copula <- macro_factor_copula_vec[r]
                    copula_factor <- copula_factor_vec[s]

                    trade_statement =
                      glue::glue(
                        "
        (
        pred_macro_1 <= pred_macro_1_mean - pred_macro_1_sd*{macro_factor_tech} &
        pred_macro_3 <= pred_macro_3_mean - pred_macro_3_sd*{macro_factor_tech} &
        (pred_technical_1 >= pred_technical_1_mean + pred_technical_1_sd*{tech_factor})
        )|
        (
        pred_macro_1 <= pred_macro_1_mean - pred_macro_1_sd*{macro_factor_daily} &
        pred_macro_3 <= pred_macro_3_mean - pred_macro_3_sd*{macro_factor_daily} &
        (pred_daily_1 >= pred_daily_1_mean + pred_daily_1_sd*{daily_factor})
        )|
        (
        pred_macro_1 <= pred_macro_1_mean - pred_macro_1_sd*{macro_factor_post_pred} &
        pred_macro_3 <= pred_macro_3_mean - pred_macro_3_sd*{macro_factor_post_pred} &
        (mean_3_pred_GLM_period_return_24_Price >
              mean_50_pred_GLM_period_return_24_Price + sd_50_pred_GLM_period_return_24_Price*{post_pred_factor})
        )|
        (
        pred_macro_1 <= pred_macro_1_mean - pred_macro_1_sd*{macro_factor_copula} &
        pred_macro_3 <= pred_macro_3_mean - pred_macro_3_sd*{macro_factor_copula} &
        (pred_copula_1 >= pred_copula_1_mean + pred_copula_1_sd*{copula_factor})
        )

      "
                      ) %>%
                      as.character()

                    win_thresh = win_thresh

                    post_preds_all_rolling_and_originals_2 <-
                      post_preds_all_rolling_and_originals %>%
                      filter(
                        Date >= as.character(as_date("2025-09-01") + days(45))
                      )

                    comnbined_statement_best_results <-
                      post_preds_all_rolling_and_originals_2 %>%
                      filter(Asset != "BTC_USD", Asset != "FR40_EUR") %>%
                      pull(Asset) %>%
                      unique() %>%
                      map(
                        ~
                          post_ss_model_analyse_condition(
                            tagged_trade_col_data = post_preds_all_rolling_and_originals_2,
                            actual_wins_losses = actual_wins_losses %>%
                              filter(Date >= as.character(as_date("2025-09-01") + days(45))),
                            trade_statement = trade_statement,
                            Asset_Var = .x,
                            win_thresh = win_thresh,
                            trade_direction = "Long"
                          ) %>%
                          pluck(1) %>%
                          ungroup() %>%
                          # filter(Period <= 44) %>%
                          filter(Period <= 35) %>%
                          filter(Period >= 35) %>%
                          filter(trade_col == "Long") %>%
                          # group_by(Asset) %>%
                          # slice_max(perc, n = 1) %>%
                          group_by(Asset) %>%
                          slice_max(Total_Returns) %>%
                          ungroup()
                      ) %>%
                      keep(~ dim(.x)[1] > 0) %>%
                      map_dfr(bind_rows)
                    # janitor::adorn_totals()

                    comnbined_statement_best_params <-
                      comnbined_statement_best_results %>%
                      dplyr::select(Asset, Period) %>%
                      mutate(
                        best_result = TRUE
                      ) %>%
                      distinct()

                    comnbined_statement_control <-
                      post_preds_all_rolling_and_originals_2 %>%
                      pull(Asset) %>%
                      unique() %>%
                      map(
                        ~
                          post_ss_model_analyse_condition(
                            tagged_trade_col_data = post_preds_all_rolling_and_originals_2,
                            trade_statement = "pred_LM_period_return_24_Price > 0 |
                          pred_LM_period_return_24_Price <= 0 |
                          is.na(pred_LM_period_return_24_Price)|
                          is.nan(pred_LM_period_return_24_Price) |
                          is.infinite(pred_LM_period_return_24_Price)",
                            Asset_Var = .x,
                            win_thresh = win_thresh,
                            trade_direction = "Long",
                            actual_wins_losses = actual_wins_losses %>%
                              filter(Date >= as.character(as_date("2025-09-01") + days(45)))
                          ) %>%
                          pluck(1) %>%
                          ungroup()
                      ) %>%
                      keep(~ dim(.x)[1] > 0) %>%
                      map_dfr(bind_rows) %>%
                      dplyr::left_join(comnbined_statement_best_params) %>%
                      filter(best_result == TRUE) %>%
                      arrange(Total_Returns) %>%
                      # janitor::adorn_totals() %>%
                      dplyr::select(Asset, Period,
                                    total_trades_control = total_trades,
                                    Total_Returns_Control = Total_Returns,
                                    Average_Return_Control = Average_Return,
                                    perc_control = perc)

                    comapre_results_summary <-
                      comnbined_statement_best_results %>%
                      dplyr::select(-trade_statement, -trade_col) %>%
                      left_join(comnbined_statement_control) %>%
                      mutate(
                        trade_percent = total_trades/total_trades_control,
                        Total_Returns_Control_adj = Total_Returns_Control*trade_percent
                      ) %>%
                      mutate(
                        Returns_Diff = Total_Returns - Total_Returns_Control_adj,
                        perc_diff = perc - perc_control
                      )

                    comapre_results_summary$perc_diff %>% mean()
                    comapre_results_summary$Returns_Diff %>% mean()

                    comapre_results_summary <-
                      comapre_results_summary %>%
                      janitor::adorn_totals() %>%
                      mutate(
                        macro_factor_tech = macro_factor_tech_vec[i],
                        tech_factor = tech_factor_vec[j],
                        macro_factor_daily = macro_factor_daily_vec[k],
                        daily_factor = daily_factor_vec[o],
                        macro_factor_post_pred = macro_factor_post_pred_vec[p],
                        post_pred_factor = post_pred_factor_vec[q],
                        macro_factor_copula = macro_factor_copula_vec[r],
                        copula_factor = copula_factor_vec[s]
                      ) %>%
                      as_tibble()

                    comapre_results_summary <-
                      comnbined_statement_best_results %>%
                      dplyr::select(-trade_statement, -trade_col) %>%
                      left_join(comnbined_statement_control) %>%
                      mutate(
                        trade_percent = total_trades/total_trades_control,
                        Total_Returns_Control_adj = Total_Returns_Control*trade_percent
                      ) %>%
                      mutate(
                        Returns_Diff = Total_Returns - Total_Returns_Control_adj,
                        perc_diff = perc - perc_control
                      )

                    trades_taken <-
                      post_preds_all_rolling_and_originals_2 %>%
                      filter(Asset != "BTC_USD", Asset != "FR40_EUR") %>%
                      mutate(
                        trade_col =
                          eval(parse(text = trade_statement))
                      ) %>%
                      filter(trade_col == TRUE) %>%
                      distinct(Asset, Date) %>%
                      left_join(
                        comnbined_statement_best_params %>%
                          distinct(Asset, Period)
                      )

                    margin_required <- create_porfolio_sim(
                      trades_taken = trades_taken,
                      actual_wins_losses = actual_wins_losses %>%
                        filter(Date >= as.character(as_date("2025-09-01") + days(45)) )
                    )

                    margin_required_sum <-
                      margin_required %>%
                      ungroup()  %>%
                      dplyr::filter(Adjusted_Date >= median(Adjusted_Date) ) %>%
                      summarise(
                        mean_margin = mean(margin_at_date, na.rm = T),
                        margin_90 = quantile(margin_at_date, 0.95, na.rm = T),
                        mean_portfolio = mean(running_PL, na.rm = T),
                        portfolio_90 = quantile(running_PL, 0.95, na.rm = T)
                      )

                    trades_taken_ts_returns <-
                      trades_taken %>%
                      left_join(actual_wins_losses %>%
                                  dplyr::select(Asset, Date, period_return_35_Price)) %>%
                      group_by(Date) %>%
                      summarise(Returns = sum(period_return_35_Price, na.rm = T )) %>%
                      arrange(Date, .by_group = TRUE) %>%
                      mutate(
                        Returns_cumulative = cumsum(Returns)
                      ) %>%
                      mutate(
                        trade_col = "Trade Long"
                      )

                    worst_loss <-
                      trades_taken_ts_returns %>%
                      arrange(Date) %>%
                      mutate(
                        loss_100 = Returns_cumulative - lag(Returns_cumulative, 100),
                        max_100 = Returns_cumulative - lag(Returns_cumulative, 100)
                      )  %>%
                      filter(!is.na(loss_100)) %>%
                      mutate(
                        across(
                          .cols = contains("loss"),
                          .fns = ~ slider::slide_dbl(.x = ., .f = ~ min(.x, na.rm = T), .before = 500)
                        ),
                        across(
                          .cols = contains("max_"),
                          .fns = ~ slider::slide_dbl(.x = ., .f = ~ max(.x, na.rm = T), .before = 500)
                        )
                      ) %>%
                      summarise(
                        across(
                          .cols = contains("loss"),
                          .fns = ~ min(., na.rm = T)
                        ),
                        across(
                          .cols = contains("max_"),
                          .fns = ~ max(., na.rm = T)
                        )
                      ) %>%
                      mutate(
                        ratio_win_to_loss = abs(max_100/loss_100),
                        mean_perc_diff_vs_control = comapre_results_summary$perc_diff %>% mean(),
                        mean_return_diff_vs_control = comapre_results_summary$Returns_Diff %>% mean(),
                        Total_Trades = comapre_results_summary$total_trades %>% sum(),
                        Total_Returns = comapre_results_summary$Total_Returns %>% sum()
                      ) %>%
                      bind_cols(margin_required_sum) %>%
                      mutate(
                        macro_factor_tech = macro_factor_tech_vec[i],
                        tech_factor = tech_factor_vec[j],
                        macro_factor_daily = macro_factor_daily_vec[k],
                        daily_factor = daily_factor_vec[o],
                        macro_factor_post_pred = macro_factor_post_pred_vec[p],
                        post_pred_factor = post_pred_factor_vec[q],
                        macro_factor_copula = macro_factor_copula_vec[r],
                        copula_factor = copula_factor_vec[s]
                      ) %>%
                      as_tibble()

                    rm(post_preds_all_rolling_and_originals_2,
                       trades_taken_ts_returns,
                       trades_taken
                       )

                    gc()

                    c = c + 1
                    if(redo_db == TRUE & c == 1) {
                      write_table_sql_lite(.data = worst_loss,
                                           table_name = "single_asset_improved",
                                           conn = save_db,
                                           overwrite_true = TRUE)

                      write_table_sql_lite(.data = comapre_results_summary,
                                           table_name = "single_asset_improved_Asset",
                                           conn = save_db,
                                           overwrite_true = TRUE)
                      redo_db = FALSE
                    }

                    if(redo_db == FALSE) {
                      append_table_sql_lite(.data = worst_loss,
                                            table_name = "single_asset_improved",
                                            conn = save_db)
                      append_table_sql_lite(.data = comapre_results_summary,
                                            table_name = "single_asset_improved_Asset",
                                            conn = save_db)
                    }

                  }
                }
              }
            }
          }
        }
      }
    }

    DBI::dbDisconnect(save_db)
    rm(save_db)
    gc()

  }
