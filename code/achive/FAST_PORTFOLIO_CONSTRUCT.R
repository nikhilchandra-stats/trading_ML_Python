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
get_portfolio_model_fast_summed <-
  function(
    asset_data = Indices_Metals_Bonds %>% map(~ .x %>% filter(Date >= "2023-01-01") ),
    asset_of_interest = c("EUR_USD", "EUR_JPY", "EUR_GBP", "GBP_USD", "USD_JPY"),
    tagged_trades = tagged_trades,
    stop_factor_var = 4,
    profit_factor_var = 15,
    risk_dollar_value_var = 5,
    end_period = 24,
    time_frame = "H1",
    trade_direction = "Long",
    currency_conversion = currency_conversion,
    asset_infor = asset_infor,
    end_point_loss = -3,
    end_point_profit = 10,
    sum_as_portfolio = FALSE
  ) {

    actuals_data <-
      get_actual_wins_losses(
      assets_to_analyse =asset_of_interest,
      asset_data = Indices_Metals_Bonds,
      stop_factor = stop_factor_var,
      profit_factor = profit_factor_var,
      risk_dollar_value = risk_dollar_value_var,
      trade_direction = trade_direction,
      currency_conversion = currency_conversion,
      asset_infor = asset_infor,
      periods_ahead = end_period
    )

    construct_string_loss <-
      seq(1,50,1) %>%
      map(~ glue::glue("period_return_{.x}_Price <= {end_point_loss} ~ {.x}") ) %>%
      unlist() %>%
      paste(collapse = ",")

    construct_string_2_loss <-
      glue::glue("case_when({construct_string_loss})") %>% as.character()

    construct_string_win <-
      seq(1,50,1) %>%
      map(~ glue::glue("period_return_{.x}_Price >= {end_point_profit} ~ {.x}") ) %>%
      unlist() %>%
      paste(collapse = ",")

    construct_string_2_win <-
      glue::glue("case_when({construct_string_win})") %>% as.character()

    extract_loss_return_string <-
      seq(1,50,1) %>%
      map(~ glue::glue("period_return_{.x}_Price <= {end_point_loss} ~ period_return_{.x}_Price") ) %>%
      unlist() %>%
      paste(collapse = ",")

    construct_extract_loss_return_string <-
      glue::glue("case_when({extract_loss_return_string})") %>% as.character()

    extract_win_return_string <-
      seq(1,50,1) %>%
      map(~ glue::glue("period_return_{.x}_Price >= {end_point_profit} ~ period_return_{.x}_Price") ) %>%
      unlist() %>%
      paste(collapse = ",")

    construct_extract_win_return_string <-
      glue::glue("case_when({extract_win_return_string})") %>% as.character()

    test <- actuals_data %>%
      mutate(
        end_point_point_loss = eval(parse(text = construct_string_2_loss))
      ) %>%
      mutate(
        end_point_point_loss = ifelse(is.na(end_point_point_loss), 50, end_point_point_loss)
      ) %>%
      mutate(
        end_point_point_win = eval(parse(text = construct_string_2_win))
      ) %>%
      mutate(
        end_point_point_win = ifelse(is.na(end_point_point_win), 50, end_point_point_win)
      ) %>%
      mutate(
        return_loss = eval(parse(text = construct_extract_loss_return_string)),
        return_win = eval(parse(text = construct_extract_win_return_string)),
        across(.cols = c(return_loss, return_win),
               .fns = ~ ifelse(is.na(.), period_return_50_Price, .))
      ) %>%
      mutate(
        Final_Return =
          case_when(
            end_point_point_win < end_point_point_loss ~ return_win,
            end_point_point_win >= end_point_point_loss ~ return_loss
          )
      )

    return(test)

  }
