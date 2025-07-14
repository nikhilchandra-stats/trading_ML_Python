#' get_cor_trade_results
#'
#' @param testing_data
#' @param raw_asset_data
#' @param sd_fac1
#' @param sd_fac2
#' @param stop_factor
#' @param profit_factor
#' @param trade_direction
#' @param mean_values_by_asset_for_loop
#' @param currency_conversion
#' @param asset_infor
#' @param risk_dollar_value
#' @param return_analysis
#'
#' @return
#' @export
#'
#' @examples
get_cor_trade_results_v2 <-
  function(
    testing_ramapped = testing_ramapped,
    raw_asset_data_ask = starting_asset_data_ask_15,
    raw_asset_data_bid = starting_asset_data_bid_15,
    AUD_USD_sd = 1,
    NZD_USD_sd = 1,
    EUR_JPY_SD = 4,
    GBP_JPY_SD = 0,
    SPX_US2000_SD = 1.25,
    EUR_DE30_SD = 1,
    GBP_EUR_USD_SD = 1,
    EUR_GBP_USD_USD_SD = 0,
    NZD_CHF_USD_SD = 0,
    WTI_BCO_SD = 0,
    stop_factor = 17,
    profit_factor = 25,
    mean_values_by_asset_for_loop = mean_values_by_asset_for_loop_15_ask,
    currency_conversion = currency_conversion,
    asset_infor = asset_infor,
    risk_dollar_value = 10,
    return_analysis = TRUE
  ) {

    names(testing_ramapped) %>%
      keep(~ str_detect(.x, "tangent"))
    names(testing_ramapped) %>%
      keep(~ str_detect(.x, "cor"))

    tagged_trades <-
      testing_ramapped %>%
      mutate(
        trade_col =
          case_when(
            (AUD_USD_NZD_USD_cor >= AUD_USD_NZD_USD_cor_mean + AUD_USD_NZD_USD_cor_sd*NZD_USD_sd) &
              Asset == "NZD_USD" & NZD_USD_tangent_angle2 > 0 & AUD_USD_tangent_angle1 < 0~ "Short",

            (AUD_USD_NZD_USD_cor >= AUD_USD_NZD_USD_cor_mean + AUD_USD_NZD_USD_cor_sd*NZD_USD_sd) &
              Asset == "NZD_USD" & NZD_USD_tangent_angle2 < 0 & AUD_USD_tangent_angle1 > 0~ "Long",

            (AUD_USD_NZD_USD_cor <= AUD_USD_NZD_USD_cor_mean - AUD_USD_NZD_USD_cor_sd*NZD_USD_sd) &
              Asset == "NZD_USD" & NZD_USD_tangent_angle2 > 0 & AUD_USD_tangent_angle1 < 0~ "Short",

            (AUD_USD_NZD_USD_cor <= AUD_USD_NZD_USD_cor_mean - AUD_USD_NZD_USD_cor_sd*NZD_USD_sd) &
              Asset == "NZD_USD" & NZD_USD_tangent_angle2 < 0 & AUD_USD_tangent_angle1 > 0~ "Long",

            (AUD_USD_NZD_USD_cor >= AUD_USD_NZD_USD_cor_mean + AUD_USD_NZD_USD_cor_sd*NZD_USD_sd) &
              Asset == "AUD_USD" & NZD_USD_tangent_angle2 < 0 & AUD_USD_tangent_angle1 > 0~ "Long",

            (AUD_USD_NZD_USD_cor >= AUD_USD_NZD_USD_cor_mean + AUD_USD_NZD_USD_cor_sd*NZD_USD_sd) &
              Asset == "AUD_USD" & NZD_USD_tangent_angle2 > 0 & AUD_USD_tangent_angle1 < 0~ "Short",

            (AUD_USD_NZD_USD_cor <= AUD_USD_NZD_USD_cor_mean - AUD_USD_NZD_USD_cor_sd*NZD_USD_sd) &
              Asset == "AUD_USD" & NZD_USD_tangent_angle2 > 0 & AUD_USD_tangent_angle1 < 0~ "Short",

            (AUD_USD_NZD_USD_cor <= AUD_USD_NZD_USD_cor_mean - AUD_USD_NZD_USD_cor_sd*NZD_USD_sd) &
              Asset == "AUD_USD" & NZD_USD_tangent_angle2 < 0 & AUD_USD_tangent_angle1 > 0~ "Long",


            (EUR_JPY_GBP_JPY_cor >= EUR_JPY_GBP_JPY_cor_mean + EUR_JPY_GBP_JPY_cor_sd*EUR_JPY_SD) &
              Asset == "EUR_JPY" & EUR_JPY_tangent_angle1 < 0 & GBP_JPY_tangent_angle2> 0 ~ "Long",

            (EUR_JPY_GBP_JPY_cor >= EUR_JPY_GBP_JPY_cor_mean + EUR_JPY_GBP_JPY_cor_sd*EUR_JPY_SD) &
              Asset == "EUR_JPY" & EUR_JPY_tangent_angle1 > 0 & GBP_JPY_tangent_angle2 < 0 ~ "Short",

            (EUR_JPY_GBP_JPY_cor <= EUR_JPY_GBP_JPY_cor_mean - EUR_JPY_GBP_JPY_cor_sd*EUR_JPY_SD) &
              Asset == "EUR_JPY" & EUR_JPY_tangent_angle1 < 0 & GBP_JPY_tangent_angle2> 0 ~ "Long",

            (EUR_JPY_GBP_JPY_cor <= EUR_JPY_GBP_JPY_cor_mean - EUR_JPY_GBP_JPY_cor_sd*EUR_JPY_SD) &
              Asset == "EUR_JPY" & EUR_JPY_tangent_angle1 > 0 & GBP_JPY_tangent_angle2 < 0 ~ "Short",

            (EUR_JPY_GBP_JPY_cor >= EUR_JPY_GBP_JPY_cor_mean + EUR_JPY_GBP_JPY_cor_sd*EUR_JPY_SD) &
              Asset == "GBP_JPY" & EUR_JPY_tangent_angle1 < 0 & GBP_JPY_tangent_angle2> 0 ~ "Short",

            (EUR_JPY_GBP_JPY_cor >= EUR_JPY_GBP_JPY_cor_mean + EUR_JPY_GBP_JPY_cor_sd*EUR_JPY_SD) &
              Asset == "GBP_JPY" & EUR_JPY_tangent_angle1 > 0 & GBP_JPY_tangent_angle2 < 0 ~ "Long",

            (EUR_JPY_GBP_JPY_cor <= EUR_JPY_GBP_JPY_cor_mean - EUR_JPY_GBP_JPY_cor_sd*EUR_JPY_SD) &
              Asset == "GBP_JPY" & EUR_JPY_tangent_angle1 < 0 & GBP_JPY_tangent_angle2> 0 ~ "Short",

            (EUR_JPY_GBP_JPY_cor <= EUR_JPY_GBP_JPY_cor_mean - EUR_JPY_GBP_JPY_cor_sd*EUR_JPY_SD) &
              Asset == "GBP_JPY" & EUR_JPY_tangent_angle1 > 0 & GBP_JPY_tangent_angle2 < 0 ~ "Long",


            (US2000_USD_SPX500_USD_cor >= US2000_USD_SPX500_USD_cor_mean + US2000_USD_SPX500_USD_cor_sd*SPX_US2000_SD) &
              Asset == "SPX500_USD" & SPX500_USD_tangent_angle2 > 0 & US2000_USD_tangent_angle1 < 0~ "Short",

            (US2000_USD_SPX500_USD_cor >= US2000_USD_SPX500_USD_cor_mean + US2000_USD_SPX500_USD_cor_sd*SPX_US2000_SD) &
              Asset == "SPX500_USD" & SPX500_USD_tangent_angle2 < 0 & US2000_USD_tangent_angle1 > 0~ "Long",

            (US2000_USD_SPX500_USD_cor <= US2000_USD_SPX500_USD_cor_mean - US2000_USD_SPX500_USD_cor_sd*SPX_US2000_SD) &
              Asset == "SPX500_USD" & SPX500_USD_tangent_angle2 > 0 & US2000_USD_tangent_angle1 < 0~ "Short",

            (US2000_USD_SPX500_USD_cor <= US2000_USD_SPX500_USD_cor_mean - US2000_USD_SPX500_USD_cor_sd*SPX_US2000_SD) &
              Asset == "SPX500_USD" & SPX500_USD_tangent_angle2 < 0 & US2000_USD_tangent_angle1 > 0~ "Long",

            (US2000_USD_SPX500_USD_cor >= US2000_USD_SPX500_USD_cor_mean + US2000_USD_SPX500_USD_cor_sd*SPX_US2000_SD) &
              Asset == "US2000_USD" & SPX500_USD_tangent_angle2 > 0 & US2000_USD_tangent_angle1 < 0~ "Short",

            (US2000_USD_SPX500_USD_cor >= US2000_USD_SPX500_USD_cor_mean + US2000_USD_SPX500_USD_cor_sd*SPX_US2000_SD) &
              Asset == "US2000_USD" & SPX500_USD_tangent_angle2 < 0 & US2000_USD_tangent_angle1 > 0~ "Long",

            (US2000_USD_SPX500_USD_cor <= US2000_USD_SPX500_USD_cor_mean - US2000_USD_SPX500_USD_cor_sd*SPX_US2000_SD) &
              Asset == "US2000_USD" & SPX500_USD_tangent_angle2 > 0 & US2000_USD_tangent_angle1 < 0~ "Short",

            (US2000_USD_SPX500_USD_cor <= US2000_USD_SPX500_USD_cor_mean - US2000_USD_SPX500_USD_cor_sd*SPX_US2000_SD) &
              Asset == "US2000_USD" & SPX500_USD_tangent_angle2 < 0 & US2000_USD_tangent_angle1 > 0~ "Long",


            # (US2000_USD_SPX500_USD_cor >= US2000_USD_SPX500_USD_cor_mean + US2000_USD_SPX500_USD_cor_sd*SPX_US2000_SD) &
            #   Asset == "SPX500_USD" & SPX500_USD_tangent_angle2 < 0 & US2000_USD_tangent_angle1 > 0~ "Short",
            #
            # (US2000_USD_SPX500_USD_cor >= US2000_USD_SPX500_USD_cor_mean + US2000_USD_SPX500_USD_cor_sd*SPX_US2000_SD) &
            #   Asset == "SPX500_USD" & SPX500_USD_tangent_angle2 > 0 & US2000_USD_tangent_angle1 < 0~ "Long",
            #
            # (US2000_USD_SPX500_USD_cor <= US2000_USD_SPX500_USD_cor_mean - US2000_USD_SPX500_USD_cor_sd*SPX_US2000_SD) &
            #   Asset == "SPX500_USD" & SPX500_USD_tangent_angle2 < 0 & US2000_USD_tangent_angle1 > 0~ "Short",
            #
            # (US2000_USD_SPX500_USD_cor <= US2000_USD_SPX500_USD_cor_mean - US2000_USD_SPX500_USD_cor_sd*SPX_US2000_SD) &
            #   Asset == "SPX500_USD" & SPX500_USD_tangent_angle2 > 0 & US2000_USD_tangent_angle1 < 0~ "Long",
            #
            # (US2000_USD_SPX500_USD_cor >= US2000_USD_SPX500_USD_cor_mean + US2000_USD_SPX500_USD_cor_sd*SPX_US2000_SD) &
            #   Asset == "US2000_USD" & SPX500_USD_tangent_angle2 < 0 & US2000_USD_tangent_angle1 > 0~ "Short",
            #
            # (US2000_USD_SPX500_USD_cor >= US2000_USD_SPX500_USD_cor_mean + US2000_USD_SPX500_USD_cor_sd*SPX_US2000_SD) &
            #   Asset == "US2000_USD" & SPX500_USD_tangent_angle2 > 0 & US2000_USD_tangent_angle1 < 0~ "Long",
            #
            # (US2000_USD_SPX500_USD_cor <= US2000_USD_SPX500_USD_cor_mean - US2000_USD_SPX500_USD_cor_sd*SPX_US2000_SD) &
            #   Asset == "US2000_USD" & SPX500_USD_tangent_angle2 < 0 & US2000_USD_tangent_angle1 > 0~ "Short",
            #
            # (US2000_USD_SPX500_USD_cor <= US2000_USD_SPX500_USD_cor_mean - US2000_USD_SPX500_USD_cor_sd*SPX_US2000_SD) &
            #   Asset == "US2000_USD" & SPX500_USD_tangent_angle2 > 0 & US2000_USD_tangent_angle1 < 0~ "Long",


            # (EU50_EUR_DE30_EUR_cor >= EU50_EUR_DE30_EUR_cor_mean + EU50_EUR_DE30_EUR_cor_sd*EUR_DE30_SD) &
            #   Asset == "EU50_EUR" & EU50_EUR_tangent_angle1 > 0 & DE30_EUR_tangent_angle2 < 0 ~ "Short",
            #
            # (EU50_EUR_DE30_EUR_cor >= EU50_EUR_DE30_EUR_cor_mean + EU50_EUR_DE30_EUR_cor_sd*EUR_DE30_SD) &
            #   Asset == "EU50_EUR" & EU50_EUR_tangent_angle1 < 0 & DE30_EUR_tangent_angle2 > 0 ~ "Long",
            #
            # (EU50_EUR_DE30_EUR_cor <= EU50_EUR_DE30_EUR_cor_mean - EU50_EUR_DE30_EUR_cor_sd*EUR_DE30_SD) &
            #   Asset == "EU50_EUR" & EU50_EUR_tangent_angle1 > 0 & DE30_EUR_tangent_angle2 < 0 ~ "Short",
            #
            # (EU50_EUR_DE30_EUR_cor <= EU50_EUR_DE30_EUR_cor_mean - EU50_EUR_DE30_EUR_cor_sd*EUR_DE30_SD) &
            #   Asset == "EU50_EUR" & EU50_EUR_tangent_angle1 < 0 & DE30_EUR_tangent_angle2 > 0 ~ "Long",
            #
            (EU50_EUR_DE30_EUR_cor >= EU50_EUR_DE30_EUR_cor_mean + EU50_EUR_DE30_EUR_cor_sd*EUR_DE30_SD) &
              Asset == "EU50_EUR" & EU50_EUR_tangent_angle1 > 0 & DE30_EUR_tangent_angle2 < 0 ~ "Short",

            (EU50_EUR_DE30_EUR_cor >= EU50_EUR_DE30_EUR_cor_mean + EU50_EUR_DE30_EUR_cor_sd*EUR_DE30_SD) &
              Asset == "EU50_EUR" & EU50_EUR_tangent_angle1 < 0 & DE30_EUR_tangent_angle2 > 0 ~ "Long",

            (EU50_EUR_DE30_EUR_cor <= EU50_EUR_DE30_EUR_cor_mean - EU50_EUR_DE30_EUR_cor_sd*EUR_DE30_SD) &
              Asset == "EU50_EUR" & EU50_EUR_tangent_angle1 > 0 & DE30_EUR_tangent_angle2 < 0 ~ "Short",

            (EU50_EUR_DE30_EUR_cor <= EU50_EUR_DE30_EUR_cor_mean - EU50_EUR_DE30_EUR_cor_sd*EUR_DE30_SD) &
              Asset == "EU50_EUR" & EU50_EUR_tangent_angle1 > 0 & DE30_EUR_tangent_angle2 < 0 ~ "Long",


            (GBP_USD_EUR_GBP_cor >= GBP_USD_EUR_GBP_cor_mean + GBP_USD_EUR_GBP_cor_sd*GBP_EUR_USD_SD) &
              Asset == "EUR_GBP" & EUR_GBP_tangent_angle2 > 0 & GBP_USD_tangent_angle2 < 0~ "Short",

            (GBP_USD_EUR_GBP_cor >= GBP_USD_EUR_GBP_cor_mean + GBP_USD_EUR_GBP_cor_sd*GBP_EUR_USD_SD) &
              Asset == "EUR_GBP" & EUR_GBP_tangent_angle2 < 0 & GBP_USD_tangent_angle2 > 0~ "Long",

            (GBP_USD_EUR_GBP_cor <= GBP_USD_EUR_GBP_cor_mean - GBP_USD_EUR_GBP_cor_sd*GBP_EUR_USD_SD) &
              Asset == "EUR_GBP" & EUR_GBP_tangent_angle2 > 0 & GBP_USD_tangent_angle2 < 0~ "Short",

            (GBP_USD_EUR_GBP_cor <= GBP_USD_EUR_GBP_cor_mean - GBP_USD_EUR_GBP_cor_sd*GBP_EUR_USD_SD) &
              Asset == "EUR_GBP" & EUR_GBP_tangent_angle2 < 0 & GBP_USD_tangent_angle2 > 0~ "Long",

            (GBP_USD_EUR_GBP_cor >= GBP_USD_EUR_GBP_cor_mean + GBP_USD_EUR_GBP_cor_sd*GBP_EUR_USD_SD) &
              Asset == "GBP_USD" & EUR_GBP_tangent_angle2 < 0 & GBP_USD_tangent_angle2 > 0~ "Short",

            (GBP_USD_EUR_GBP_cor >= GBP_USD_EUR_GBP_cor_mean + GBP_USD_EUR_GBP_cor_sd*GBP_EUR_USD_SD) &
              Asset == "GBP_USD" & EUR_GBP_tangent_angle2 > 0 & GBP_USD_tangent_angle2 < 0~ "Long",

            (GBP_USD_EUR_GBP_cor <= GBP_USD_EUR_GBP_cor_mean - GBP_USD_EUR_GBP_cor_sd*GBP_EUR_USD_SD) &
              Asset == "GBP_USD" & EUR_GBP_tangent_angle2 < 0 & GBP_USD_tangent_angle2 > 0~ "Short",

            (GBP_USD_EUR_GBP_cor <= GBP_USD_EUR_GBP_cor_mean - GBP_USD_EUR_GBP_cor_sd*GBP_EUR_USD_SD) &
              Asset == "GBP_USD" & EUR_GBP_tangent_angle2 > 0 & GBP_USD_tangent_angle2 < 0~ "Long",

            (WTICO_USD_BCO_USD_cor >= WTICO_USD_BCO_USD_cor_mean + WTICO_USD_BCO_USD_cor_sd*WTI_BCO_SD) &
              Asset == "WTICO_USD" & WTICO_USD_tangent_angle1 < 0 & BCO_USD_tangent_angle2 > 0~ "Long",

            (WTICO_USD_BCO_USD_cor >= WTICO_USD_BCO_USD_cor_mean + WTICO_USD_BCO_USD_cor_sd*WTI_BCO_SD) &
              Asset == "WTICO_USD" & WTICO_USD_tangent_angle1 > 0 & BCO_USD_tangent_angle2 < 0~ "Short",

            (WTICO_USD_BCO_USD_cor <= WTICO_USD_BCO_USD_cor_mean - WTICO_USD_BCO_USD_cor_sd*WTI_BCO_SD) &
              Asset == "WTICO_USD" & WTICO_USD_tangent_angle1 < 0 & BCO_USD_tangent_angle2 > 0~ "Long",

            (WTICO_USD_BCO_USD_cor <= WTICO_USD_BCO_USD_cor_mean - WTICO_USD_BCO_USD_cor_sd*WTI_BCO_SD) &
              Asset == "WTICO_USD" & WTICO_USD_tangent_angle1 > 0 & BCO_USD_tangent_angle2 < 0~ "Short",

            (WTICO_USD_BCO_USD_cor >= WTICO_USD_BCO_USD_cor_mean + WTICO_USD_BCO_USD_cor_sd*WTI_BCO_SD) &
              Asset == "BCO_USD" & WTICO_USD_tangent_angle1 > 0 & BCO_USD_tangent_angle2 < 0~ "Long",

            (WTICO_USD_BCO_USD_cor >= WTICO_USD_BCO_USD_cor_mean + WTICO_USD_BCO_USD_cor_sd*WTI_BCO_SD) &
              Asset == "BCO_USD" & WTICO_USD_tangent_angle1 < 0 & BCO_USD_tangent_angle2 > 0~ "Short",

            (WTICO_USD_BCO_USD_cor <= WTICO_USD_BCO_USD_cor_mean - WTICO_USD_BCO_USD_cor_sd*WTI_BCO_SD) &
              Asset == "BCO_USD" & WTICO_USD_tangent_angle1 > 0 & BCO_USD_tangent_angle2 < 0~ "Long",

            (WTICO_USD_BCO_USD_cor <= WTICO_USD_BCO_USD_cor_mean - WTICO_USD_BCO_USD_cor_sd*WTI_BCO_SD) &
              Asset == "BCO_USD" & WTICO_USD_tangent_angle1 < 0 & BCO_USD_tangent_angle2 > 0~ "Short",

            (NZD_CHF_USD_CHF_cor >= NZD_CHF_USD_CHF_cor_mean + NZD_CHF_USD_CHF_cor_sd*NZD_CHF_USD_SD) &
              Asset == "NZD_CHF" & NZD_CHF_tangent_angle1 < 0 & USD_CHF_tangent_angle2 > 0~ "Long",

            (NZD_CHF_USD_CHF_cor >= NZD_CHF_USD_CHF_cor_mean + NZD_CHF_USD_CHF_cor_sd*NZD_CHF_USD_SD) &
              Asset == "NZD_CHF" & NZD_CHF_tangent_angle1 > 0 & USD_CHF_tangent_angle2 < 0~ "Short",

            (NZD_CHF_USD_CHF_cor <= NZD_CHF_USD_CHF_cor_mean - NZD_CHF_USD_CHF_cor_sd*NZD_CHF_USD_SD) &
              Asset == "NZD_CHF" & NZD_CHF_tangent_angle1 < 0 & USD_CHF_tangent_angle2 > 0~ "Long",

            (NZD_CHF_USD_CHF_cor <= NZD_CHF_USD_CHF_cor_mean - NZD_CHF_USD_CHF_cor_sd*NZD_CHF_USD_SD) &
              Asset == "NZD_CHF" & NZD_CHF_tangent_angle1 > 0 & USD_CHF_tangent_angle2 < 0~ "Short",


            (NZD_CHF_USD_CHF_cor >= NZD_CHF_USD_CHF_cor_mean + NZD_CHF_USD_CHF_cor_sd*NZD_CHF_USD_SD) &
              Asset == "USD_CHF" & NZD_CHF_tangent_angle1 > 0 & USD_CHF_tangent_angle2 < 0~ "Long",

            (NZD_CHF_USD_CHF_cor >= NZD_CHF_USD_CHF_cor_mean + NZD_CHF_USD_CHF_cor_sd*NZD_CHF_USD_SD) &
              Asset == "USD_CHF" & NZD_CHF_tangent_angle1 < 0 & USD_CHF_tangent_angle2 > 0~ "Short",

            (NZD_CHF_USD_CHF_cor <= NZD_CHF_USD_CHF_cor_mean - NZD_CHF_USD_CHF_cor_sd*NZD_CHF_USD_SD) &
              Asset == "USD_CHF" & NZD_CHF_tangent_angle1 > 0 & USD_CHF_tangent_angle2 < 0~ "Long",

            (NZD_CHF_USD_CHF_cor <= NZD_CHF_USD_CHF_cor_mean - NZD_CHF_USD_CHF_cor_sd*NZD_CHF_USD_SD) &
              Asset == "USD_CHF" & NZD_CHF_tangent_angle1 < 0 & USD_CHF_tangent_angle2 > 0~ "Short"

#             (NZD_CHF_USD_CHF_cor >= NZD_CHF_USD_CHF_cor_mean + NZD_CHF_USD_CHF_cor_sd*NZD_CHF_USD_SD) &
#               Asset == "NZD_CHF" & USD_CHF_tangent_angle2 > 0~ trade_direction

          )
      ) %>%
      filter(!is.na(trade_col))


    if(return_analysis == TRUE) {

      asset_in_trades <- tagged_trades %>%
        ungroup() %>%
        distinct(Asset) %>%
        pull(Asset) %>%
        unique()

      long_bayes_loop_analysis_shorts <-
        generic_trade_finder_loop(
          tagged_trades = tagged_trades %>% filter(trade_col == "Short") ,
          # asset_data_daily_raw = starting_asset_data_bid_15,
          asset_data_daily_raw = raw_asset_data_bid,
          stop_factor = stop_factor,
          profit_factor =profit_factor,
          trade_col = "trade_col",
          date_col = "Date",
          start_price_col = "Price",
          mean_values_by_asset = mean_values_by_asset_for_loop
        )

      long_bayes_loop_analysis_longs <-
        generic_trade_finder_loop(
          tagged_trades = tagged_trades %>% filter(trade_col == "Long") ,
          # asset_data_daily_raw = starting_asset_data_ask_15,
          asset_data_daily_raw = raw_asset_data_ask,
          stop_factor = stop_factor,
          profit_factor =profit_factor,
          trade_col = "trade_col",
          date_col = "Date",
          start_price_col = "Price",
          mean_values_by_asset = mean_values_by_asset_for_loop
        )

      trade_timings_neg <-
        long_bayes_loop_analysis_shorts %>%
        bind_rows(long_bayes_loop_analysis_longs) %>%
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
          trade_data =  long_bayes_loop_analysis_shorts %>%
            bind_rows(long_bayes_loop_analysis_longs) %>%
            rename(Asset = asset),
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
          AUD_USD_sd = AUD_USD_sd,
          NZD_USD_sd = NZD_USD_sd,
          EUR_JPY_SD = EUR_JPY_SD,
          GBP_JPY_SD = GBP_JPY_SD,
          SPX_US2000_SD = SPX_US2000_SD,
          EUR_DE30_SD = EUR_DE30_SD,
          GBP_EUR_USD_SD = GBP_EUR_USD_SD,
          EUR_GBP_USD_USD_SD = EUR_GBP_USD_USD_SD,
          NZD_CHF_USD_SD = NZD_CHF_USD_SD,
          WTI_BCO_SD = WTI_BCO_SD,
          trade_type = "Asset_Specific"
        ) %>%
        bind_cols(trade_timings_by_asset_neg)

      analysis_data_asset_neg <-
        generic_anlyser(
          trade_data = long_bayes_loop_analysis_shorts %>%
                       bind_rows(long_bayes_loop_analysis_longs) %>%
                       rename(Asset = asset),
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
          AUD_USD_sd = AUD_USD_sd,
          NZD_USD_sd = NZD_USD_sd,
          EUR_JPY_SD = EUR_JPY_SD,
          GBP_JPY_SD = GBP_JPY_SD,
          SPX_US2000_SD = SPX_US2000_SD,
          EUR_DE30_SD = EUR_DE30_SD,
          GBP_EUR_USD_SD = GBP_EUR_USD_SD,
          EUR_GBP_USD_USD_SD = EUR_GBP_USD_USD_SD,
          NZD_CHF_USD_SD = NZD_CHF_USD_SD,
          WTI_BCO_SD = WTI_BCO_SD,
          trade_type = "Asset_Specific"
        ) %>%
        bind_cols(trade_timings_by_asset_neg)

      return(
        list(
          analysis_data_neg,
          analysis_data_asset_neg,
          tagged_trades
        )
      )

    } else {

      return(
        list(
          tagged_trades
        )
      )

    }

  }
