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
create_running_profits_get_volumes <-
  function(
    asset_of_interest = "HK33_HKD",
    asset_data = Indices_Metals_Bonds,
    stop_factor = 35,
    profit_factor = 200,
    risk_dollar_value = 100,
    trade_direction = "Long",
    currency_conversion = currency_conversion,
    asset_infor = asset_infor,
    overwrite_volume = NULL,
    min_volume_only = FALSE,
    periods_wanted = 5,
    return_only_interested_col = FALSE
  ) {


    first_statement <-
      " period_return_1_Price =
          case_when(

            (trade_col == 'Long' & lead(Bid_Low,2) <= stop_point)|
              (trade_col == 'Long' & lead(Bid_Low,1) <= stop_point) ~ -1*stop_return,

            trade_col == 'Long' & lead(Bid_Low,2) > stop_point &
              lead(Bid_High,2) < profit_point ~
              adjusted_conversion*volume_adj*( (lead(Bid_Price, 2) - lead(Ask_Price)) ),

            trade_col == 'Long' & lead(Bid_Low,2) > stop_point &
              lead(Bid_High,2) > profit_point  ~ profit_return,

            trade_col == 'Short' & lead(Ask_High,2) >= stop_point|
              trade_col == 'Short' & lead(Ask_High,1) >= stop_point ~ -1*stop_return,

            trade_col == 'Short' & lead(Ask_High,2) < stop_point &
              lead(Ask_Low,2) > profit_point ~
              adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,2) ),

            trade_col == 'Short' & lead(Ask_High,2) < stop_point &
              lead(Ask_Low,2) < profit_point ~ profit_return
          )"

    required_case_whens <-
      seq(2,periods_wanted) %>%
      map(
        ~
          glue::glue(
            "
                period_return_{.x}_Price =
                      case_when(

                        trade_col == 'Long' &
                          (lead(Bid_Low,{.x} + 1) <= stop_point|period_return_{.x - 1}_Price<= -1*stop_return) &
                          period_return_{.x - 1}_Price < profit_return ~ -1*stop_return,

                        trade_col == 'Long' & lead(Bid_Low,{.x} + 1) > stop_point &
                          lead(Bid_High,{.x} + 1) < profit_point &
                          period_return_{.x - 1}_Price > -1*stop_return &
                          period_return_{.x - 1}_Price < profit_return ~
                          adjusted_conversion*volume_adj*( (lead(Bid_Price, {.x} + 1) - lead(Ask_Price)) ),

                        trade_col == 'Long' &
                          ((lead(Bid_Low,{.x} + 1) > stop_point &
                              lead(Bid_High,{.x} + 1) > profit_point &
                              period_return_{.x - 1}_Price > -1*stop_return)|
                             period_return_{.x - 1}_Price >= profit_return) ~ profit_return,

                        trade_col == 'Short' & lead(Ask_High,{.x} + 1) >= stop_point|
                          period_return_{.x - 1}_Price <= -1*stop_return ~ -1*stop_return,

                        trade_col == 'Short' & lead(Ask_High,{.x} + 1) < stop_point &
                          lead(Ask_Low,{.x} + 1) > profit_point &
                          period_return_1_Price > -1*stop_return~
                          adjusted_conversion*volume_adj*(lead(Bid_Price) - lead(Ask_Price,{.x} + 1) ),

                        trade_col == 'Short' & lead(Ask_High,{.x} + 1) < stop_point &
                          lead(Ask_Low,{.x} + 1) < profit_point &
                          period_return_{.x - 1}_Price > -1*stop_return~ profit_return
                      )
            "
          )
      )

    required_case_whens <-
      list(first_statement, required_case_whens)

    required_case_whens <-
      required_case_whens %>%
      unlist() %>%
      paste(collapse = ",")

    Final_Statement_required <-
      glue::glue("asset_data_with_indicator %>% mutate({required_case_whens})")

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
          ),
        volume_adj =
          ifelse(!is.null(overwrite_volume), overwrite_volume,volume_adj),

        volume_adj =
          ifelse(min_volume_only == TRUE, minimumTradeSize,volume_adj)
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
        stop_return = stop_value*adjusted_conversion*volume_adj,
        AUD_Price =
          case_when(
            !is.na(adjusted_conversion) ~ (Ask_Price*adjusted_conversion),
            TRUE ~ Ask_Price
          ),
        trade_value = AUD_Price*volume_required*marginRate,
        AUD_per_pip = stop_return/stop_value
      )

    final_data <- eval(parse(text = Final_Statement_required))

    rm(asset_data, bid_price)
    gc()

    if(return_only_interested_col == TRUE) {

      final_data <-
        final_data %>%
        dplyr::select(Date, Asset, Ask_Price, Bid_High, Bid_Low, Bid_Price, Ask_High, Ask_Low,
                      trade_col, mean_movement, sd_movement, stop_value, profit_value, stop_point,
                      profit_point, ending_value, adjusted_conversion, minimumTradeSize, minimumTradeSize_OG,
                      marginRate, pipLocation, volume_unadj, volume_required, volume_adj, profit_return, stop_return,
                      AUD_Price, trade_value, AUD_per_pip,
                      !!as.name(glue::glue("period_return_{periods_wanted}_Price"))
                      )

    }

    return(final_data)

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
get_actual_wins_losses_extended <- function(
    assets_to_analyse =
      c("SPX500_USD", "XAU_USD", "EU50_EUR", "JP225_USD", "USD_JPY", "UK100_GBP", "DE30_EUR",
        "AU200_AUD", "WTICO_USD", "HK33_HKD") %>% unique(),
    asset_data = Indices_Metals_Bonds,
    stop_factor = 5,
    profit_factor = 30,
    risk_dollar_value = 15,
    trade_direction = "Long",
    currency_conversion = currency_conversion,
    asset_infor = asset_infor,
    periods_ahead = period_var,
    overwrite_volume = NULL,
    min_volume_only = FALSE,
    return_only_interested_col = FALSE

) {

  temp_actual_wins_losses <- list()

  for (i in 1:length(assets_to_analyse)) {

    temp_actual_wins_losses[[i]] <-
      create_running_profits_get_volumes(
        asset_of_interest = assets_to_analyse[i],
        asset_data = asset_data,
        stop_factor = stop_factor,
        profit_factor = profit_factor,
        risk_dollar_value = risk_dollar_value,
        trade_direction = trade_direction,
        currency_conversion = currency_conversion,
        asset_infor = asset_infor,
        overwrite_volume = overwrite_volume,
        min_volume_only = min_volume_only,
        periods_wanted = periods_ahead,
        return_only_interested_col = return_only_interested_col
      )

  }

  actual_wins_losses <-
    temp_actual_wins_losses %>%
    map_dfr(bind_rows) %>%
    dplyr::select(-volume_unadj, -marginRate,
                  -adjusted_conversion, -pipLocation) %>%
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

  rm(temp_actual_wins_losses)
  gc()

  return(actual_wins_losses)

}

