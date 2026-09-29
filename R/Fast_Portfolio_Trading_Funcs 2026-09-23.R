fast_return_estimates_atomic <-
  function(
    Asset = Asset_data_combine$Asset,
    risk_dollar_value = 5,
    Asset_Price = Asset_data_combine$Price,
    stop_distance = abs(Asset_data_combine$running_30_period_return),
    profit_distance = 2*abs(Asset_data_combine$running_30_period_return),
    trade_col = "Long",
    slippage_percent = 0,
    currency_conversion =currency_conversion,
    asset_infor = asset_infor,
    min_volume_only = FALSE,
    return_col = "volume"
  ) {

    ending_value = str_extract(Asset, "_[A-Z][A-Z][A-Z]")
    ending_value = str_remove_all(ending_value, "_")

    adjusted_conversion <-
      currency_conversion %>%
      filter(not_aud_asset %in% ending_value) %>%
      pull(adjusted_conversion)

    asset_params <- asset_infor%>%
      filter(name %in% Asset) %>%
      dplyr::select(name,
                    minimumTradeSize,
                    marginRate,
                    pipLocation,
                    displayPrecision)

    minimumTradeSize_OG = as.numeric(asset_params$minimumTradeSize[1] )
    minimumTradeSize = abs(log10(as.numeric(asset_params$minimumTradeSize[1] )) )
    marginRate = as.numeric(asset_params$marginRate[1] )
    pipLocation = as.numeric(asset_params$pipLocation[1] )
    displayPrecision = as.numeric(asset_params$displayPrecision[1])

    if(trade_col == "Long") {
      stop_point = Asset_Price - stop_distance
      prof_point = Asset_Price + profit_distance
    }

    if(trade_col == "Short") {
      stop_point = Asset_Price - stop_distance
      prof_point = Asset_Price + profit_distance
    }

    stop_value = round(stop_distance, abs(pipLocation) )

    profit_value = round(profit_distance, abs(pipLocation) )

    volume_unadj =
      case_when(
        str_detect(Asset,"ZAR|CNH") ~ (risk_dollar_value/stop_value)*adjusted_conversion,
        TRUE ~ (risk_dollar_value/stop_value)/adjusted_conversion
      )

    volume_required = volume_unadj

    volume_adj =
      case_when(
        round(volume_unadj, minimumTradeSize) == 0 ~  minimumTradeSize_OG,
        round(volume_unadj, minimumTradeSize) != 0 ~  round(volume_unadj, minimumTradeSize)
      )

    volume_adj =
      case_when(min_volume_only == TRUE ~ minimumTradeSize,
                TRUE ~ volume_adj)

    profit_return = profit_value*adjusted_conversion*volume_adj

    stop_return = stop_value*adjusted_conversion*volume_adj

    stop_return = stop_return + slippage_percent*stop_return

    AUD_Price =
      case_when(
        !is.na(adjusted_conversion) ~ (Asset_Price*adjusted_conversion),
        TRUE ~ Asset_Price
      )

    trade_value = AUD_Price*volume_required*marginRate
    AUD_per_pip = stop_return/stop_value

    if(return_col == "volume") {
        return(volume_adj)
    }

    if(return_col == "stop_return") {
      return(stop_return)
    }

    if(return_col == "profit_return") {
      return(profit_return)
    }

    if(return_col == "stop_point") {
      return(stop_point)
    }

    if(return_col == "prof_point") {
      return(prof_point)
    }


    if(return_col == "stop_value") {
      return(stop_value)
    }

    if(return_col == "profit_value") {
      return(profit_value)
    }

    if(return_col == 'adjusted_conversion') {
      return(adjusted_conversion)
    }

  }


get_dynamic_stop_prof_returns <-
  function(
    Ask_Data = Indices_Metals_Bonds[[1]],
    Bid_Data = Indices_Metals_Bonds[[2]],
    periods_wanted = 20,
    trade_direction = "Long",
    currency_conversion =currency_conversion,
    asset_infor = asset_infor,
    slippage_percent = 0,
    risk_dollar_value = 5,
    volatility_factor_stop = 2,
    volatility_factor_profit = 2,
    profit_multiple = 2,
    running_volatility_period_max = 20,
    running_volatility_period_mean = 100
  ) {

    asset_data_with_indicator <-
      Ask_Data %>%
      left_join(
        Bid_Data %>%
          dplyr::select(Asset, Date,
                        Bid_Price = Price,
                        Bid_Low = Low,
                        Bid_High = High)
      ) %>%
      rename(
        Ask_Price =Price,
        Ask_Low = Low,
        Ask_High = High
      ) %>%
      group_by(Asset) %>%
      arrange(Date, .by_group = TRUE)  %>%
      group_by(Asset) %>%
      mutate(
        Analysis_Price = lag(Ask_Price),
        running_max =
          slider::slide_dbl(.x = Ask_Price, .f = ~ abs(max(cumsum(diff(.x)), na.rm = T)) ,.before = running_volatility_period_max),
        running_min =
          slider::slide_dbl(.x = Ask_Price, .f = ~ abs(min(cumsum(diff(.x)), na.rm = T)) ,.before = running_volatility_period_max),

        running_max_mean = slider::slide_dbl(.x = running_max, .f = ~ mean(.x, na.rm = T), .before = running_volatility_period_mean),
        running_max_sd = slider::slide_dbl(.x = running_max, .f = ~ sd(.x, na.rm = T), .before = running_volatility_period_mean),

        running_min_mean = slider::slide_dbl(.x = running_min, .f = ~ mean(.x, na.rm = T), .before = running_volatility_period_mean),
        running_min_sd = slider::slide_dbl(.x = running_min, .f = ~ sd(.x, na.rm = T), .before = running_volatility_period_mean)
      ) %>%
      ungroup() %>%
      mutate(
        running_volatility_stop =
          case_when(trade_direction == "Long" ~
                      running_min_mean + running_min_sd*volatility_factor_stop,
                    trade_direction == "Short" ~
                      running_max_mean + running_max_sd*volatility_factor_stop),

        running_volatility_prof =
          case_when(trade_direction == "Long" ~
                      running_max_mean + running_max_sd*volatility_factor_profit,
                    trade_direction == "Short" ~
                      running_min_mean + running_min_sd*volatility_factor_profit)
      ) %>%
      group_by(Asset) %>%
      mutate(
        volume_adj =
          fast_return_estimates_atomic(
            Asset = Asset,
            risk_dollar_value = risk_dollar_value,
            Asset_Price = Ask_Price,
            stop_distance = abs(running_volatility_stop),
            profit_distance = profit_multiple*abs(running_volatility_prof),
            trade_col = trade_direction,
            slippage_percent = slippage_percent,
            currency_conversion =currency_conversion,
            asset_infor = asset_infor,
            min_volume_only = FALSE,
            return_col = "volume"
          ),

        stop_return =
          fast_return_estimates_atomic(
            Asset = Asset,
            risk_dollar_value = risk_dollar_value,
            Asset_Price = Ask_Price,
            stop_distance = abs(running_volatility_stop),
            profit_distance = profit_multiple*abs(running_volatility_prof),
            trade_col = trade_direction,
            slippage_percent = slippage_percent,
            currency_conversion =currency_conversion,
            asset_infor = asset_infor,
            min_volume_only = FALSE,
            return_col = "stop_return"
          ),

        profit_return =
          fast_return_estimates_atomic(
            Asset = Asset,
            risk_dollar_value = risk_dollar_value,
            Asset_Price = Ask_Price,
            stop_distance = abs(running_volatility_stop),
            profit_distance = profit_multiple*abs(running_volatility_prof),
            trade_col = trade_direction,
            slippage_percent = slippage_percent,
            currency_conversion =currency_conversion,
            asset_infor = asset_infor,
            min_volume_only = FALSE,
            return_col = "profit_return"
          ),

        stop_point =
          fast_return_estimates_atomic(
            Asset = Asset,
            risk_dollar_value = risk_dollar_value,
            Asset_Price = Ask_Price,
            stop_distance = abs(running_volatility_stop),
            profit_distance = profit_multiple*abs(running_volatility_prof),
            trade_col = trade_direction,
            slippage_percent = slippage_percent,
            currency_conversion =currency_conversion,
            asset_infor = asset_infor,
            min_volume_only = FALSE,
            return_col = "stop_point"
          ),

        profit_point =
          fast_return_estimates_atomic(
            Asset = Asset,
            risk_dollar_value = risk_dollar_value,
            Asset_Price = Ask_Price,
            stop_distance = abs(running_volatility_stop),
            profit_distance = profit_multiple*abs(running_volatility_prof),
            trade_col = trade_direction,
            slippage_percent = slippage_percent,
            currency_conversion =currency_conversion,
            asset_infor = asset_infor,
            min_volume_only = FALSE,
            return_col = "prof_point"
          ),


        stop_value =
          fast_return_estimates_atomic(
            Asset = Asset,
            risk_dollar_value = risk_dollar_value,
            Asset_Price = Ask_Price,
            stop_distance = abs(running_volatility_stop),
            profit_distance = profit_multiple*abs(running_volatility_prof),
            trade_col = trade_direction,
            slippage_percent = slippage_percent,
            currency_conversion =currency_conversion,
            asset_infor = asset_infor,
            min_volume_only = FALSE,
            return_col = "stop_value"
          ),

        profit_value =
          fast_return_estimates_atomic(
            Asset = Asset,
            risk_dollar_value = risk_dollar_value,
            Asset_Price = Ask_Price,
            stop_distance = abs(running_volatility_stop),
            profit_distance = profit_multiple*abs(running_volatility_prof),
            trade_col = trade_direction,
            slippage_percent = slippage_percent,
            currency_conversion =currency_conversion,
            asset_infor = asset_infor,
            min_volume_only = FALSE,
            return_col = "profit_value"
          ),

        adjusted_conversion =
          fast_return_estimates_atomic(
            Asset = Asset,
            risk_dollar_value = risk_dollar_value,
            Asset_Price = Ask_Price,
            stop_distance = abs(running_volatility_stop),
            profit_distance = profit_multiple*abs(running_volatility_prof),
            trade_col = trade_direction,
            slippage_percent = slippage_percent,
            currency_conversion =currency_conversion,
            asset_infor = asset_infor,
            min_volume_only = FALSE,
            return_col = "adjusted_conversion"
          )

      ) %>%
      mutate(
        volatility_factor_profit = volatility_factor_profit,
        volatility_factor_stop = volatility_factor_stop,
        profit_multiple = profit_multiple,
        running_volatility_period_max = running_volatility_period_max,
        running_volatility_period_mean = running_volatility_period_mean
      )

    asset_data_with_indicator =
      asset_data_with_indicator %>%
      mutate(
        trade_col = trade_direction
      )

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

    final_data <- eval(parse(text = Final_Statement_required))

    final_data <-
      final_data %>%
      mutate(
        Final_Return = !!as.name(glue::glue("period_return_{periods_wanted}_Price"))
      )

    return(final_data)

  }

#' get_dynamic_portfolio_no_V3
#'
#' @param portfolio_data
#' @param xtnd_ss_cols_PR_cols
#' @param roll_period_state_space
#' @param xtnd_ss_cols_BR_periods
#' @param xtnd_rolling_volatility
#' @param xtnd_rolling_bull_bear
#' @param lag_dependant
#' @param auto_cor_cols
#' @param cor_skip_periods
#' @param cor_period
#' @param periods_to_use_deviation
#' @param mean_periods_deviation
#'
#' @return
#' @export
#'
#' @examples
get_dynamic_portfolio_no_V3 <-
  function(
    portfolio_data = portfolio_data_train,
    xtnd_ss_cols_PR_cols = c(1,2,5,10),
    roll_period_state_space = 500,
    xtnd_ss_cols_BR_periods = c(100,200,300),
    xtnd_rolling_volatility = c(20,50,60,80,100),
    xtnd_rolling_bull_bear = c(50,100),
    lag_dependant = end_period + 1,
    auto_cor_cols = 25,
    cor_skip_periods = c(2,4,5,6),
    cor_period = 50,
    periods_to_use_deviation = c(1,3,4),
    mean_periods_deviation = c(50, 100)
  ) {

    total_reg_data <-
      portfolio_data %>%
      ungroup() %>%
      distinct(Date, Asset)

    total_return_portfolio <-
      portfolio_data %>%
      ungroup() %>%
      group_by(Date) %>%
      summarise(
        across(.cols = c(Final_Return, contains("period_return_")),
               .fns = ~ sum(., na.rm = T))
      ) %>%
      ungroup() %>%
      mutate(Asset = "Portfolio") %>%
      mutate(across(.cols = contains("period_return_"), .fns = ~ as.numeric(.)))

    deviation_data <-
      portfolio_get_deviation_from_total(
        total_return_portfolio = total_return_portfolio,
        portfolio_data = portfolio_data,
        periods_to_use_deviation = periods_to_use_deviation,
        mean_periods_deviation = mean_periods_deviation
      )

    req_ss_extnd_cols_PR <-
      xtnd_ss_cols_PR_cols %>%
      map(
        ~
          glue::glue(
            "
          state_space_data_PR_{.x} <-
              portfolio_LM_state_space(
                  portfolio_data = portfolio_data,
                  state_space_col = 'period_return_{.x}_Price' ,
                  required_lag = {.x}, #Does not need a plus 1 its built in
                  roll_period_state_space = {roll_period_state_space}
                )"
          )
      )  %>%
      unlist() %>%
      as.character() %>%
      paste(collapse = "\n")

    req_ss_extnd_cols_PR_names <-
      xtnd_ss_cols_PR_cols %>%
      map(
        ~ glue::glue("state_space_data_PR_{.x}")
      ) %>%
      unlist() %>%
      as.character() %>%
      paste(collapse = ",")

    req_ss_extnd_cols_PR_list <-
      glue::glue("All_state_space_data_PR <- list({req_ss_extnd_cols_PR_names})")

    rm_statement <- glue::glue("rm({req_ss_extnd_cols_PR_names})")

    eval(parse(text = req_ss_extnd_cols_PR))
    eval(parse(text = req_ss_extnd_cols_PR_list))
    eval(parse(text = rm_statement))
    gc()

    All_state_space_data_PR <-
      All_state_space_data_PR %>%
      reduce(left_join)

    All_state_space_data_PR <-
      All_state_space_data_PR %>%
      dplyr::select(Date, Asset, contains("state_space"))

    message("Made it to State SPace End line 2750")

    All_state_space_data_PR <- All_state_space_data_PR

    total_reg_data <-
      total_reg_data %>%
      left_join(All_state_space_data_PR %>% ungroup()) %>%
      left_join(deviation_data %>% ungroup())

    rm(deviation_data)

    message("Made it to State SPace joined with TOtal Reg End line 2771")

    req_BR_extnd_cols_PR <-
      xtnd_ss_cols_BR_periods %>%
      map(
        ~ glue::glue("
          brownian_tech_data_{.x} <-
                portfolio_LM_brownian_checks(portfolio_data = portfolio_data,
                                             brownian_period = {.x},
                                             col_to_use = 'period_return_1_Price',
                                             lag_period_to_use = 1)
          brownian_tech_data_FR_{.x} <-
                portfolio_LM_brownian_checks(portfolio_data = portfolio_data,
                                             brownian_period = {.x},
                                             col_to_use = 'Final_Return',
                                             lag_period_to_use = {lag_dependant} )"
        )
      ) %>%
      unlist() %>%
      as.character() %>%
      paste(collapse = "\n")

    req_BR_extnd_cols_PR_names <-
      xtnd_ss_cols_BR_periods %>%
      map(
        ~ glue::glue("brownian_tech_data_{.x}, brownian_tech_data_FR_{.x}")
      ) %>%
      unlist() %>%
      as.character() %>%
      paste(collapse = ",")

    req_BR_extnd_cols_PR_list <-
      glue::glue("All_BR_data_PR <- list({req_BR_extnd_cols_PR_names})")

    rm_statement <- glue::glue("rm({req_BR_extnd_cols_PR_names})")

    eval(parse(text = req_BR_extnd_cols_PR))
    message("Made it to Brownian First Statement req_BR_extnd_cols_PR line 2808")
    eval(parse(text = req_BR_extnd_cols_PR_list))
    message("Made it to Brownian Second Statement req_BR_extnd_cols_PR line 2810")
    eval(parse(text = rm_statement))
    message("Made it to Brownian Third Statement req_BR_extnd_cols_PR line 2812")
    gc()

    All_BR_data_PR <-
      All_BR_data_PR %>%
      reduce(left_join)

    message("Made it to All_BR_data_PR statement line 2819")

    total_reg_data <-
      total_reg_data %>%
      left_join(All_BR_data_PR %>%
                  dplyr::select(Date, Asset, contains("brownian"))
      )

    message("Made it to total_reg_data statement line 2827")

    rm(All_state_space_data_PR, All_BR_data_PR)


    vola_roll_max_statements <-
      xtnd_rolling_volatility %>%
      map(
        ~
          glue::glue("roll_vol_{.x}_max = slider::slide_dbl(.x = ( lag(Bid_High) - lag(Ask_Price) ), .f = ~ (max(cumsum(diff(.x)), na.rm = T)) ,.before = {.x})")
      ) %>%
      unlist()

    vola_roll_min_statements <-
      xtnd_rolling_volatility %>%
      map(
        ~
          glue::glue("roll_vol_{.x}_min = slider::slide_dbl(.x = (lag(Ask_Price) - lag(Bid_Low)), .f = ~ (min(cumsum(diff(.x)), na.rm = T)) ,.before = {.x})")

      ) %>%
      unlist()

    vola_roll_all <-
      list(
        vola_roll_max_statements,
        vola_roll_min_statements
      ) %>%
      unlist() %>%
      paste(collapse = ",")

    vola_roll_all_mutate <-
      glue::glue("portfolio_data %>%
                   group_by(Asset) %>%
                   arrange(Date, .by_group = TRUE) %>%
                   group_by(Asset) %>%
                   mutate({vola_roll_all}) %>%
                   dplyr::select(Date, Asset, contains('roll_vol_')) ")

    all_vol_data <- eval(parse(text = vola_roll_all_mutate))

    total_reg_data <-
      total_reg_data %>%
      left_join(all_vol_data %>%
                  ungroup() %>%
                  dplyr::select(Date, Asset, contains("roll_vol_"))
      )

    rm(all_vol_data)
    gc()

    bull_bear_data <-
      get_bull_bear_rolling(
        portfolio_data = portfolio_data,
        roll_periods = xtnd_rolling_bull_bear
      )

    total_reg_data <-
      total_reg_data %>%
      left_join(
        bull_bear_data %>%
          ungroup() %>%
          dplyr::select(Date, Asset, contains("cumulative"), contains("Bull"), contains("Bear"))
      )

    rm(bull_bear_data)
    gc()

    Final_Returns <-
      portfolio_data %>%
      distinct(Date, Asset, Final_Return)

    message("Made it to Final_Returns statement line 2878")

    total_reg_data <-
      total_reg_data %>%
      left_join(Final_Returns)

    message("Made it to total_reg_data statement line 2884")

    return(total_reg_data)

  }




#' get_post_no_V3_probs
#'
#' @param assets_to_port
#' @param model_predicted_data
#' @param currency_conversion
#' @param asset_infor
#' @param db_location
#' @param start_date
#' @param risk_dollar_value_var
#' @param save_location
#' @param file_name
#' @param training_date_post
#' @param vol_stop_vec
#' @param vol_profit_vec
#' @param periods_wanted
#' @param trade_direction
#' @param slippage_percent
#' @param risk_dollar_value
#' @param profit_multiple
#' @param running_volatility_period_max
#' @param running_volatility_period_mean
#'
#' @return
#' @export
#'
#' @examples
get_post_no_V3_probs <-
  function(
    assets_to_port =
      c(
        "EUR_JPY",
        "EUR_USD",
        "EUR_GBP",
        "EU50_EUR"
      ) %>% unique(),
    model_predicted_data = model_predicted_data,
    currency_conversion = currency_conversion,
    asset_infor = asset_infor,
    db_location = db_location,
    start_date = "2021-11-01",
    risk_dollar_value_var = 5,
    save_location = "C:/Users/Nikhil Chandra/Documents/trade_data/Day_Trader_Cor_Continuous_Models/",
    file_name = "POST_EUR_EXPNDED_ONLY_NON_V3_NEW_MODEL",
    training_date_post = "2023-02-01",
    vol_stop_vec = c(0.5,1,1.5,2,3),
    vol_profit_vec = c(0.5,1,1.5,2,3),
    periods_wanted = 52,
    trade_direction = "Long",
    slippage_percent = 0,
    risk_dollar_value = 5,
    profit_multiple = 1,
    running_volatility_period_max = 50,
    running_volatility_period_mean = 200
  ) {

    Indices_Metals_Bonds <- list()
    model_name = file_name

    Indices_Metals_Bonds[[1]] <-
      get_db_data_quickly_algo(
        db_location = db_location,
        start_date = start_date,
        end_date = as.character(today() + days(30)),
        time_frame = "H1",
        bid_or_ask = "ask",
        assets =   assets_to_port
      ) %>%
      distinct()
    Indices_Metals_Bonds[[2]] <-
      get_db_data_quickly_algo(
        db_location = db_location,
        start_date = start_date,
        end_date = as.character(today() + days(30)),
        time_frame = "H1",
        bid_or_ask = "bid",
        assets =   assets_to_port
      ) %>%
      distinct()

    Indices_Metals_Bonds[[1]] <- Indices_Metals_Bonds[[1]] %>% filter(Date >= training_date_post)
    Indices_Metals_Bonds[[2]] <- Indices_Metals_Bonds[[2]] %>% filter(Date >= training_date_post)

    c = 0
    portfolio_data_test_additional <- list()

    for (i in 1:length(vol_stop_vec)) {
      for (j in 1:length(vol_profit_vec)) {

        c = c + 1
        portfolio_data_test_additional[[c]] <-
          get_dynamic_stop_prof_returns(
            Ask_Data = Indices_Metals_Bonds[[1]],
            Bid_Data = Indices_Metals_Bonds[[2]],
            periods_wanted = periods_wanted,
            trade_direction = trade_direction,
            currency_conversion =currency_conversion,
            asset_infor = asset_infor,
            slippage_percent = slippage_percent,
            risk_dollar_value = risk_dollar_value,
            volatility_factor_stop = vol_stop_vec[i],
            volatility_factor_profit = vol_profit_vec[j],
            profit_multiple = profit_multiple,
            running_volatility_period_max = running_volatility_period_max,
            running_volatility_period_mean = running_volatility_period_mean
          )

      }
    }

    portfolio_data_test_additional <-
      portfolio_data_test_additional %>%
      map_dfr(bind_rows)

    post_model_data <-
      portfolio_data_test_additional %>%
      left_join(
        model_predicted_data %>%
          dplyr::select(Date, Asset, contains("pred"), contains("pnorm"))
      )

    rm(model_predicted_data, portfolio_data_test_additional)
    gc()

    LM_model <- readRDS( glue::glue("{save_location}/{file_name}.RDS") )

    post_model_data <-
      post_model_data %>% filter(Date > training_date_post)

    gc()

    predicted_test <- predict(newdata = post_model_data,
                              object =  LM_model,
                              type = "response")

    rm(LM_model)

    model_prediction_data <-
      post_model_data %>%
      filter(Date > training_date_post) %>%
      mutate(predicted = predicted_test) %>%
      ungroup() %>%
      dplyr::select(Date, Asset, Final_Return, predicted)

    rm(reg_dat)
    gc()

  }

#' generate_post_model
#'
#' @param assets_to_port
#' @param model_predicted_data
#' @param currency_conversion
#' @param asset_infor
#' @param db_location
#' @param dependant_var
#' @param start_date
#' @param risk_dollar_value_var
#' @param save_location
#' @param file_name
#' @param training_date
#' @param training_date_post
#' @param vol_stop_vec
#' @param vol_profit_vec
#' @param periods_wanted
#' @param trade_direction
#' @param slippage_percent
#' @param risk_dollar_value
#' @param profit_multiple
#' @param running_volatility_period_max
#' @param running_volatility_period_mean
#' @param sig_thresh_LM
#' @param GLM_or_LM
#'
#' @return
#' @export
#'
#' @examples
generate_post_model <-
  function(
    assets_to_port =
      c(
        "EUR_JPY",
        "EUR_USD",
        "EUR_GBP",
        "EU50_EUR"
      ) %>% unique(),
    model_predicted_data = model_predicted_data,
    currency_conversion = currency_conversion,
    asset_infor = asset_infor,
    db_location = db_location,
    dependant_var = "period_return_52_Price",
    start_date = "2021-11-01",
    risk_dollar_value_var = 5,
    save_location = "C:/Users/Nikhil Chandra/Documents/trade_data/Day_Trader_Cor_Continuous_Models/",
    file_name = "POST_EUR_EXPNDED_ONLY_NON_V3_NEW_MODEL",
    training_date = "2022-02-01",
    training_date_post = "2023-02-01",
    vol_stop_vec = c(0.5,1,1.5,2,3),
    vol_profit_vec = c(0.5,1,1.5,2,3),
    periods_wanted = 52,
    trade_direction = "Long",
    slippage_percent = 0,
    risk_dollar_value = 5,
    profit_multiple = 1,
    running_volatility_period_max = 50,
    running_volatility_period_mean = 200,
    sig_thresh_LM = 1,
    GLM_or_LM = "LM"
  ) {


    Indices_Metals_Bonds <- list()
    model_name = file_name

    Indices_Metals_Bonds[[1]] <-
      get_db_data_quickly_algo(
        db_location = db_location,
        start_date = start_date,
        end_date = as.character(today() + days(30)),
        time_frame = "H1",
        bid_or_ask = "ask",
        assets =   assets_to_port
      ) %>%
      distinct()
    Indices_Metals_Bonds[[2]] <-
      get_db_data_quickly_algo(
        db_location = db_location,
        start_date = start_date,
        end_date = as.character(today() + days(30)),
        time_frame = "H1",
        bid_or_ask = "bid",
        assets =   assets_to_port
      ) %>%
      distinct()

    Indices_Metals_Bonds[[1]] <- Indices_Metals_Bonds[[1]] %>% filter(Date >= training_date)
    Indices_Metals_Bonds[[2]] <- Indices_Metals_Bonds[[2]] %>% filter(Date >= training_date)
    Indices_Metals_Bonds[[1]] <- Indices_Metals_Bonds[[1]] %>% filter(Date <= training_date_post)
    Indices_Metals_Bonds[[2]] <- Indices_Metals_Bonds[[2]] %>% filter(Date <= training_date_post)

    c = 0
    portfolio_data_test_additional <- list()

    for (i in 1:length(vol_stop_vec)) {
      for (j in 1:length(vol_profit_vec)) {

        c = c + 1
        portfolio_data_test_additional[[c]] <-
          get_dynamic_stop_prof_returns(
            Ask_Data = Indices_Metals_Bonds[[1]],
            Bid_Data = Indices_Metals_Bonds[[2]],
            periods_wanted = periods_wanted,
            trade_direction = trade_direction,
            currency_conversion =currency_conversion,
            asset_infor = asset_infor,
            slippage_percent = slippage_percent,
            risk_dollar_value = risk_dollar_value,
            volatility_factor_stop = vol_stop_vec[i],
            volatility_factor_profit = vol_profit_vec[j],
            profit_multiple = profit_multiple,
            running_volatility_period_max = running_volatility_period_max,
            running_volatility_period_mean = running_volatility_period_mean
          )

      }
    }

    portfolio_data_test_additional <-
      portfolio_data_test_additional %>%
      map_dfr(bind_rows)

    post_model_data <-
      portfolio_data_test_additional %>%
      left_join(
        model_predicted_data %>%
          dplyr::select(Date, Asset, contains("pred"), contains("pnorm"))
      )

    rm(model_predicted_data, portfolio_data_test_additional)
    gc()

    reg_vars <-
      post_model_data %>%
      dplyr::select(
        Asset, contains("pred")|contains("pnorm")|contains("volatility")|contains("running")
      )

    reg_vars <- names(reg_vars)

    training_data <-
      post_model_data %>%
      ungroup() %>%
      filter(if_all(everything(), ~ !is.na(.))) %>%
      filter(Date <= training_date_post)

    training_data <-
      training_data %>%
      mutate(
        bin_var = ifelse(!!as.name(dependant_var) > 0, 1, 0)
      )

    rm(post_model_data)
    gc()

    if(GLM_or_LM == "LM") {
      lm_form <-
        create_lm_formula(dependant = dependant_var, independant = reg_vars)

      LM_model <- lm(data = training_data,
                     formula = lm_form)
    }

    if(GLM_or_LM == "GLM") {
      lm_form <-
        create_lm_formula(dependant = "bin_var", independant = reg_vars)

      LM_model <- glm(data = training_data,
                      formula = lm_form, family = binomial("logit"))
    }


    sig_coefs <-
      get_sig_coefs(LM_model, p_value_thresh_for_inputs = sig_thresh_LM)

    if(length(sig_coefs) < 1) {
      sig_coefs <- get_sig_coefs(LM_model, p_value_thresh_for_inputs = 10^-7)
    }

    if(length(sig_coefs) < 1) {
      sig_coefs <- get_sig_coefs(LM_model, p_value_thresh_for_inputs = 10^-6)
    }

    if(length(sig_coefs) < 1) {
      sig_coefs <- get_sig_coefs(LM_model, p_value_thresh_for_inputs = 10^-5)
    }

    if(length(sig_coefs) < 1) {
      sig_coefs <- get_sig_coefs(LM_model, p_value_thresh_for_inputs = 10^-4)
    }

    if(length(sig_coefs) < 1) {
      sig_coefs <- get_sig_coefs(LM_model, p_value_thresh_for_inputs = 10^-3)
    }

    sig_coefs <-
      sig_coefs %>%
      keep(~ !str_detect(.x, "Asset[A-Z][A-Z]") ) %>%
      unlist()

    sig_coefs <-
      c(sig_coefs, "Asset") %>%
      unlist()

    if(GLM_or_LM == "LM") {
      lm_form <-
        create_lm_formula(dependant = dependant_var,
                          independant = sig_coefs)

      LM_model <- lm(formula = lm_form,
                     data = training_data %>% filter(Final_Return != 0))
    }

    if(GLM_or_LM == "GLM") {

      lm_form <-
        create_lm_formula(dependant = "bin_var", independant = sig_coefs)

      LM_model <- glm(data = training_data,
                      formula = lm_form, family = binomial("logit"))
    }

    LM_model$model <- NULL
    LM_model$fitted.values <- NULL

    rm(training_data)
    gc()

    saveRDS(LM_model,
            file = glue::glue("{save_location}/{file_name}.RDS") )

  }

portfolio_Dynamic_no_V3_algo_variant_Loop <-
  function(
    assets_to_port =
      c(
        "AUD_CAD",
        "AUD_USD",
        "AUD_JPY"
      ) %>% unique(),
    currency_conversion = currency_conversion,
    asset_infor = asset_infor,
    db_location = "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data_Most_Assets_2025-09-13 2.db",
    start_date = "2022-02-01",
    end_period = 132,

    slippage_percent = 0,
    risk_dollar_value = 5,
    volatility_factor_stop = 2,
    volatility_factor_profit = 3,
    profit_multiple = 1.1,
    running_volatility_period_max = 20,
    running_volatility_period_mean = 100,

    trade_direction = "Long",
    regression_length = 25000,
    direct_return_cols = 24,
    lag_value_error = end_period + 1,

    cor_period = c(50),
    dependant_var = "Final_Return",
    save_location = "C:/Users/Nikhil Chandra/Documents/trade_data/Day_Trader_Cor_Continuous_Models/",
    file_name = "AUD_CURR_NON_V3_NEW_MODEL",
    training_date = "2022-02-01",
    testing_date = NULL,
    xtnd_ss_cols_PR_cols = c(1,5,10,20,30,40,50,60,70,80,120, 100),
    xtnd_ss_cols_BR_periods = c(100,200,300, 50, 150, 250, 350, 25, 500),
    xtnd_rolling_volatility = c(20,50,60,80,100),
    xtnd_rolling_bull_bear = c(50,100),
    lag_dependant = end_period + 1,
    auto_cor_cols = 40,
    cor_skip_periods = c(1,2,4,5,6,8,10,12,14,16),
    periods_to_use_deviation = c(1,10,20,30,40,50),
    mean_periods_deviation = c(50, 100),
    run_additional_calc = FALSE,
    last_date = NULL,

    estimate_trades = FALSE,
    trade_statement = NULL,
    current_time = now() %>% as_datetime()

  ) {

    if(is.null(testing_date)) { testing_date <- training_date}

    Indices_Metals_Bonds <- list()

    if(is.null(last_date)){

      Indices_Metals_Bonds[[1]] <-
        get_db_data_quickly_algo(
          db_location = db_location,
          start_date = start_date,
          end_date = as.character(today() + days(30)),
          time_frame = "H1",
          bid_or_ask = "ask",
          assets =   assets_to_port
        ) %>%
        distinct()
      Indices_Metals_Bonds[[2]] <-
        get_db_data_quickly_algo(
          db_location = db_location,
          start_date = start_date,
          end_date = as.character(today() + days(30)),
          time_frame = "H1",
          bid_or_ask = "bid",
          assets =   assets_to_port
        ) %>%
        distinct()

    } else {

      Indices_Metals_Bonds[[1]] <-
        get_db_data_quickly_algo(
          db_location = db_location,
          start_date = start_date,
          # end_date = as.character(today() + days(30)),
          end_date = last_date,
          time_frame = "H1",
          bid_or_ask = "ask",
          assets =   assets_to_port
        ) %>%
        distinct()
      Indices_Metals_Bonds[[2]] <-
        get_db_data_quickly_algo(
          db_location = db_location,
          start_date = start_date,
          # end_date = as.character(today() + days(30)),
          end_date = last_date,
          time_frame = "H1",
          bid_or_ask = "bid",
          assets =   assets_to_port
        ) %>%
        distinct()

    }



    Indices_Metals_Bonds[[1]] <- Indices_Metals_Bonds[[1]] %>% filter(Date > testing_date)
    Indices_Metals_Bonds[[2]] <- Indices_Metals_Bonds[[2]] %>% filter(Date > testing_date)

    volatility_factor_stop_vec <-
      tibble(volatility_factor_stop = volatility_factor_stop)

    running_volatility_tibble <-
      volatility_factor_profit %>%
      map_dfr(
        ~
          volatility_factor_stop_vec %>%
          mutate(
            volatility_factor_profit = .x
          )
      )

    running_volatility_tibble <-
      running_volatility_period_max %>%
      map_dfr(
        ~
          running_volatility_tibble %>%
          mutate(running_volatility_period_max = .x)
      )

    running_volatility_tibble <-
      running_volatility_period_mean %>%
      map_dfr(
        ~
          running_volatility_tibble %>%
          mutate(running_volatility_period_mean = .x)
      )

    model_predicted_data <- list()

    for (i in 1:dim(running_volatility_tibble)[1] ) {

      volatility_factor_stop = running_volatility_tibble$volatility_factor_stop[i]
      volatility_factor_profit = running_volatility_tibble$volatility_factor_profit[i]

      running_volatility_period_max = running_volatility_tibble$running_volatility_period_max[i]
      running_volatility_period_mean = running_volatility_tibble$running_volatility_period_mean[i]

      message("Portfolio Returns TEst calc start")
      portfolio_data_test <-
        get_dynamic_stop_prof_returns(
          Ask_Data = Indices_Metals_Bonds[[1]],
          Bid_Data = Indices_Metals_Bonds[[2]],
          periods_wanted = end_period,
          trade_direction = "Long",
          currency_conversion =currency_conversion,
          asset_infor = asset_infor,
          slippage_percent = slippage_percent,
          risk_dollar_value = risk_dollar_value,
          volatility_factor_stop = volatility_factor_stop,
          volatility_factor_profit = volatility_factor_profit,
          profit_multiple = profit_multiple,
          running_volatility_period_max = running_volatility_period_max,
          running_volatility_period_mean = running_volatility_period_mean
        )
      message("Portfolio Returns TEst calc End")

      gc()

      message("temp_reg_data_test Returns TEst calc start")
      temp_reg_data_test <-
        get_dynamic_portfolio_no_V3(
          portfolio_data = portfolio_data_test,
          xtnd_ss_cols_PR_cols = xtnd_ss_cols_PR_cols,
          xtnd_ss_cols_BR_periods = xtnd_ss_cols_BR_periods,
          xtnd_rolling_volatility = xtnd_rolling_volatility,
          xtnd_rolling_bull_bear = xtnd_rolling_bull_bear,
          lag_dependant = lag_dependant,
          auto_cor_cols = auto_cor_cols,
          cor_skip_periods = cor_skip_periods,
          cor_period = cor_period,
          periods_to_use_deviation = periods_to_use_deviation,
          mean_periods_deviation = mean_periods_deviation
        ) %>%
        ungroup() %>%
        filter(if_all(everything(), ~ !is.na(.) & !is.infinite(.) & !is.nan(.) )) %>%
        mutate(
          running_volatility_period_max = running_volatility_period_max,
          running_volatility_period_mean = running_volatility_period_mean,

          volatility_factor_stop = volatility_factor_stop,
          volatility_factor_profit = volatility_factor_profit
        )
      message("temp_reg_data_test Returns TEst calc End")

      gc()
      needed_port_join_on <-
        portfolio_data_test %>%
        ungroup() %>%
        dplyr::select(Date, Asset, volume_adj,
                      stop_value, profit_value,
                      volatility_factor_profit,
                      volatility_factor_stop,
                      running_volatility_period_max,
                      running_volatility_period_mean)

      rm(portfolio_data_test)
      gc()

      model_predicted_data[[i]] <-
        portfolio_read_model_no_V3_New(
          reg_dat = temp_reg_data_test,
          training_end_date = training_date,
          save_path = save_location,
          file_name = file_name
        )

      message(glue::glue("Check Algo Estimated (model_predicted_data): {dim(model_predicted_data[[i]])[1]} \n"))

      model_predicted_data[[i]] <-
        model_predicted_data[[i]] %>%
        ungroup() %>%
        filter(Date > training_date) %>%
        group_by(Asset) %>%
        arrange(Date, .by_group = TRUE) %>%
        group_by(Asset) %>%
        mutate(
          pnorm_100 = pcauchy(predicted, location = pred_10000_mean_roll_100, scale = pred_10000_sd_roll_100),
          pnorm_100_roll_100 = slider::slide_dbl(.x  = pnorm_100, .f = ~ mean(.x, na.rm = T), .before = 100),

          pnorm_100_port = pcauchy(predicted_portfolio, location = pred_portfolio_10000_mean_roll_100, scale = pred_portfolio_10000_sd_roll_100),
          pnorm_100_roll_100_port = slider::slide_dbl(.x  = pnorm_100_port, .f = ~ mean(.x, na.rm = T), .before = 100),

          pnorm_250 = pcauchy(predicted, location = pred_10000_mean_roll_250, scale = pred_10000_sd_roll_250),
          pnorm_250_roll_250 = slider::slide_dbl(.x  = pnorm_250, .f = ~ mean(.x, na.rm = T), .before = 250),

          pnorm_250_port = pcauchy(predicted_portfolio, location = pred_portfolio_10000_mean_roll_250, scale = pred_portfolio_10000_sd_roll_250),
          pnorm_250_roll_250_port = slider::slide_dbl(.x  = pnorm_250_port, .f = ~ mean(.x, na.rm = T), .before = 250),

          pnorm_500 = pcauchy(predicted, location = pred_10000_mean_roll_500, scale = pred_10000_sd_roll_500),
          pnorm_500_roll_500 = slider::slide_dbl(.x  = pnorm_500, .f = ~ mean(.x, na.rm = T), .before = 500),

          pnorm_500_port = pcauchy(predicted_portfolio, location = pred_portfolio_10000_mean_roll_500, scale = pred_portfolio_10000_sd_roll_500),
          pnorm_500_roll_500_port = slider::slide_dbl(.x  = pnorm_500_port, .f = ~ mean(.x, na.rm = T), .before = 500),

          pnorm_1001 = pcauchy(predicted, location = pred_10000_mean_roll_1000, scale = pred_10000_sd_roll_1000),
          pnorm_1001_roll_1001 = slider::slide_dbl(.x  = pnorm_1001, .f = ~ mean(.x, na.rm = T), .before = 1001),

          pnorm_1001_port = pcauchy(predicted_portfolio, location = pred_portfolio_10000_mean_roll_1000, scale = pred_portfolio_10000_sd_roll_1000),
          pnorm_1001_roll_1001_port = slider::slide_dbl(.x  = pnorm_1001_port, .f = ~ mean(.x, na.rm = T), .before = 1001),

          pnorm_1500 = pcauchy(predicted, location = pred_10000_mean_roll_1500, scale = pred_10000_sd_roll_1500),
          pnorm_1500_roll_1500 = slider::slide_dbl(.x  = pnorm_1500, .f = ~ mean(.x, na.rm = T), .before = 1500),

          pnorm_1500_port = pcauchy(predicted_portfolio, location = pred_portfolio_10000_mean_roll_1500, scale = pred_portfolio_10000_sd_roll_1500),
          pnorm_1500_roll_1500_port = slider::slide_dbl(.x  = pnorm_1500_port, .f = ~ mean(.x, na.rm = T), .before = 1500),

          pnorm_2000 = pcauchy(predicted, location = pred_10000_mean_roll_2000, scale = pred_10000_sd_roll_2000),
          pnorm_2000_roll_2000 = slider::slide_dbl(.x  = pnorm_2000, .f = ~ mean(.x, na.rm = T), .before = 2000),

          pnorm_2000_port = pcauchy(predicted_portfolio, location = pred_portfolio_10000_mean_roll_2000, scale = pred_portfolio_10000_sd_roll_2000),
          pnorm_2000_roll_2000_port = slider::slide_dbl(.x  = pnorm_2000_port, .f = ~ mean(.x, na.rm = T), .before = 2000),

          pnorm_1500_mean_10 = pcauchy(pred_10000_mean_roll_10, location = pred_10000_mean_roll_1500, scale = pred_10000_sd_roll_1500),
          pnorm_1500_mean_10_roll_1500 = slider::slide_dbl(.x  = pnorm_1500_mean_10, .f = ~ mean(.x, na.rm = T), .before = 1500),

          pnorm_1500_mean_10_port = pcauchy(pred_portfolio_10000_mean_roll_10, location = pred_portfolio_10000_mean_roll_1500, scale = pred_portfolio_10000_sd_roll_1500),
          pnorm_1500_mean_10_roll_1500_port = slider::slide_dbl(.x  = pnorm_1500_mean_10_port, .f = ~ mean(.x, na.rm = T), .before = 1500),

          pnorm_2000_mean_1000 = pcauchy(pred_10000_mean_roll_1000, location = pred_10000_mean_roll_2000, scale = pred_10000_sd_roll_2000),
          pnorm_2000_mean_1000_roll_2000 = slider::slide_dbl(.x  = pnorm_2000_mean_1000, .f = ~ mean(.x, na.rm = T), .before = 2000),

          pnorm_2000_mean_1000_port = pcauchy(pred_portfolio_10000_mean_roll_1000, location = pred_portfolio_10000_mean_roll_2000, scale = pred_portfolio_10000_sd_roll_2000),
          pnorm_2000_mean_1000_roll_2000_port = slider::slide_dbl(.x  = pnorm_2000_mean_1000_port, .f = ~ mean(.x, na.rm = T), .before = 2000)


        ) %>%
        ungroup() %>%
        left_join(needed_port_join_on)

      rm(temp_reg_data_test)
      gc()

    }

    rm(Indices_Metals_Bonds)
    gc()

    model_predicted_data <-
      model_predicted_data %>%
      map_dfr(bind_rows)

    if(run_additional_calc == TRUE) {
      model_predicted_data <-
        additional_error_rate_calc(model_predicted_data_raw = model_predicted_data)
    }

    if(estimate_trades == TRUE & !is.null(trade_statement)) {

      model_predicted_data <-
        model_predicted_data %>%
        filter(Asset %in% assets_to_port) %>%
        group_by(Asset) %>%
        slice_max(Date) %>%
        group_by(Asset) %>%
        slice_max(predicted)

      message(glue::glue("Check Algo Filtered and If Executed (model_predicted_data): {dim(model_predicted_data)[1]} \n"))

      max_date_pre_filt <-
        model_predicted_data %>% pull(Date) %>% max(na.rm = T)

      message(glue::glue("Check Max Date before trade statement Filter (model_predicted_data): {max_date_pre_filt} \n"))

      max_date_in_data <- floor_date(as_datetime(now(), tz = "Australia/Canberra"), "hour")
      rm(Indices_Metals_Bonds)
      gc()

      trade_dates <-
        model_predicted_data %>%
        ungroup() %>%
        slice_max(Date)

      trade_dates<-
        trade_dates %>%
        mutate(
          trade_col =
            eval(parse(text = trade_statement))
        ) %>%
        ungroup() %>%
        filter(trade_col == TRUE) %>%
        distinct(Asset, Date)

      message(glue::glue("discovered Trades: {dim(trade_dates)[1]} \n"))

      message(glue::glue("Date in Data:{trade_dates$Date[1]} Date in Max: {max_date_in_data}"))

      current_prices_ask <-
        read_all_asset_data_intra_day(
          asset_list_oanda = assets_to_port,
          save_path_oanda_assets = "D://Asset Data/oanda_data/",
          read_csv_or_API = "API",
          time_frame = "H1",
          bid_or_ask = "ask",
          how_far_back = 2,
          start_date = as_date(today() - days(3))
        )%>%
        map_dfr(bind_rows) %>%
        group_by(Asset) %>%
        slice_max(Date) %>%
        ungroup()

      single_asset_model_trades_filt <-
        trade_dates %>%
        mutate(trade_col = "Long",
               stop_factor = stop_factor_var,
               profit_factor = profit_factor_var,
               periods_ahead = end_period,
               risk_dollar_value = risk_dollar_value_var,
               end_point_loss = end_point_loss,
               end_point_profit = end_point_profit
        ) %>%
        group_by(Asset) %>%
        slice_max(Date) %>%
        ungroup() %>%
        left_join(current_prices_ask %>%
                    group_by(Asset) %>%
                    slice_max(Date) %>%
                    ungroup() %>%
                    dplyr::select(-Date)) %>%
        mutate(
          time_diff =
            abs(
              as.numeric(
                as_datetime(Date, tz = "Australia/Canberra") -
                  as_datetime(current_time, tz = "Australia/Canberra"),
                units = "mins"
              )
            ),
          date_check = max_date_in_data <= Date
        ) %>%
        group_by(Asset) %>%
        slice_min(time_diff) %>%
        ungroup()

      message(glue::glue("Trades Found Pre Filt {dim(single_asset_model_trades_filt)[1]}"))

      single_asset_model_trades_filt <-
        single_asset_model_trades_filt %>%
        # filter(time_diff <= 70 & date_check == TRUE) %>%
        filter(max_date_in_data <= Date)

      message(glue::glue("Trades Found {dim(single_asset_model_trades_filt)[1]}"))


      return(single_asset_model_trades_filt)

    }

    return(model_predicted_data)


  }
