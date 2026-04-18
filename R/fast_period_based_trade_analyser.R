

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


#' get_best_trade_setup_sa
#'
#' @param model_optimisation_store_path
#' @param table_to_extract
#'
#' @returns
#' @export
#'
#' @examples
get_best_trade_setup_sa <-
  function(
    model_optimisation_store_path =
      "C:/Users/nikhi/Documents/trade_data/single_asset_advanced_optimisation.db",
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

    summary_results <-
      summary_results %>%
      mutate(
        win_loss_perc =
          wins_or_loss_3_dollar_min/total_trades
      )

    best <-
      summary_results %>%
      filter(Total_Return > 0, return_25 > 0) %>%
      filter(
        profit_factor_long > stop_factor_long
      ) %>%
      group_by(Asset) %>%
      slice_max(win_loss_perc, n = 4) %>%
      group_by(Asset) %>%
      slice_max(Total_Return) %>%
      distinct()

    best_return_only <-
      summary_results %>%
      filter(Total_Return > 0, return_25 > 0) %>%
      group_by(Asset) %>%
      slice_max(Total_Return_worst_run) %>%
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
      ) %>%
      bind_rows(
        c(18) %>%
          map_dfr(
            ~
              tibble(
                # stop_factor_long = c(4,2,6, 8)
                stop_factor_long = c(6)
              ) %>%
              mutate(
                profit_factor_long = .x,
                profit_factor_long_fast = round(.x/4),
                profit_factor_long_fastest = floor((.x/7))
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
        Asset,
        stop_factor_short ,
        profit_factor_short ,
        period_var_short ,
        profit_factor_long,
        stop_factor_long,
        profit_factor_long_fast,
        profit_factor_long_fastest,
        period_var_long,
        Total_Return = predicted_values
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
        Total_Return,
        wins_or_loss_3_dollar_min
      )
    # bind_rows(
    #   best_return_only %>%
    #     dplyr::select(
    #       Asset,
    #       stop_factor_short ,
    #       profit_factor_short ,
    #       period_var_short ,
    #       profit_factor_long,
    #       stop_factor_long,
    #       profit_factor_long_fast,
    #       profit_factor_long_fastest,
    #       period_var_long,
    #       Total_Return,
    #       wins_or_loss_3_dollar_min
    #     )
    # ) %>%
    # bind_rows(
    #   best_best_prediced_outcome2
    # )

    short_positions <-
      final_results %>%
      dplyr::select(Asset,
                    profit_factor = profit_factor_short,
                    stop_factor = stop_factor_short,
                    period_var = period_var_short) %>%
      mutate(trade_col = "Short")

    long_positions <-
      final_results %>%
      dplyr::select(Asset,
                    profit_factor = profit_factor_long,
                    stop_factor = stop_factor_long,
                    period_var = period_var_long) %>%
      mutate(trade_col = "Long")

    long_positions_fast <-
      final_results %>%
      dplyr::select(Asset,
                    profit_factor = profit_factor_long_fast,
                    stop_factor = stop_factor_long,
                    period_var = period_var_long) %>%
      mutate(trade_col = "Long")

    long_positions_fastest <-
      final_results %>%
      dplyr::select(Asset,
                    profit_factor = profit_factor_long_fastest,
                    stop_factor = stop_factor_long,
                    period_var = period_var_long) %>%
      mutate(trade_col = "Long")

    all_positions <-
      short_positions %>%
      bind_rows(long_positions) %>%
      bind_rows(long_positions_fast) %>%
      bind_rows(long_positions_fastest)

    return(
      list("final_results" = final_results,
           "all_positions" = all_positions)
    )

  }
