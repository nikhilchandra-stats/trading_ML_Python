#' estimating_dual_copula
#'
#' @param asset_data_to_use
#' @param asset_to_use
#' @param price_col
#' @param rolling_period
#' @param samples_for_MLE
#' @param test_samples
#'
#' @return
#' @export
#'
#' @examples
estimating_dual_copula <- function(
    asset_data_to_use = starting_asset_data_bid_15,
    asset_to_use = c("AUD_USD", "NZD_USD"),
    price_col = "Open",
    rolling_period = 100,
    samples_for_MLE = 0.5,
    test_samples = 0.4
) {

  asset1 <- asset_data_to_use %>%
    filter(Asset == asset_to_use[1]) %>% distinct(Date, !!as.name(price_col)) %>%
    rename(
      !!as.name(asset_to_use[1]) := !!as.name(price_col)
    )
  asset2 <- asset_data_to_use %>% filter(Asset == asset_to_use[2]) %>% distinct(Date, !!as.name(price_col))%>%
    rename(
      !!as.name(asset_to_use[2]) := !!as.name(price_col)
    )

  combined_data <- asset1 %>%
    left_join(asset2) %>%
    mutate(
      log1_price = log(!!as.name(asset_to_use[1])),
      log2_price = log(!!as.name(asset_to_use[2]))
    )

  asset1_modeling <-combined_data %>%
    slice_head(prop = samples_for_MLE) %>%
    pull(log1_price)

  asset2_modeling <-combined_data %>%
    slice_head(prop = samples_for_MLE) %>%
    pull(log2_price)

  mle1 <- fitdistrplus::fitdist(asset1_modeling %>% keep(~ !is.na(.x) & !is.nan(.x)) , distr = "cauchy")
  mle1_1 <- mle1$estimate[1] %>% as.numeric()
  mle1_2 <- mle1$estimate[2] %>% as.numeric()
  mle2 <- fitdistrplus::fitdist(asset2_modeling %>% keep(~ !is.na(.x) & !is.nan(.x)) , distr = "cauchy")
  mle2_1 <- mle2$estimate[1] %>% as.numeric()
  mle2_2 <- mle2$estimate[2] %>% as.numeric()

  combined_data2 <- combined_data %>%
    mutate(
      quantiles_1 = pcauchy(log1_price, location = mle1_1, scale = mle1_2),
      quantiles_2 = pcauchy(log2_price, location = mle2_1, scale = mle2_2)
    ) %>%
    filter(!is.na(log1_price), !is.na(log2_price)) %>%
    mutate(
      correlation_vars =
        slider::slide2_dbl(.x = !!as.name(asset_to_use[1]), .y = !!as.name(asset_to_use[2]),
                           .f = ~ cor(.x,.y), .before = rolling_period),
      tangent_angle1 = atan(
        (!!as.name(asset_to_use[1]) - lag(!!as.name(asset_to_use[1]), rolling_period))/rolling_period
      ),
      tangent_angle2 = atan(
        (!!as.name(asset_to_use[2]) - lag(!!as.name(asset_to_use[2]), rolling_period))/rolling_period
      )
    )

  if(!is.null(test_samples)) {
    returned <- combined_data2 %>%
      slice_tail(prop = test_samples)
  } else {
    returned <- combined_data2
  }

  returned <- returned %>%
    mutate(across(.cols = !matches("Date", ignore.case = FALSE),
                  .fns = ~ lag(.)))

  new_names <-
    names(returned) %>%
    map(
      ~
        case_when(
          .x != "Date" &
            .x != asset_to_use[1] &
            .x != asset_to_use[2] &
            str_detect(.x, "1") &
            !str_detect(.x, "0") ~ paste0(asset_to_use[1], "_",.x),
          .x != "Date" &
            .x != asset_to_use[1] &
            .x != asset_to_use[2] &
            str_detect(.x, "2") &
            !str_detect(.x, "0") ~ paste0(asset_to_use[2], "_",.x),
          .x == "correlation_vars" ~ paste0(asset_to_use[1], "_",asset_to_use[2],"_","cor"),
          .x == "Date" ~ "Date",
          .x == asset_to_use[1] ~ asset_to_use[1],
          .x == asset_to_use[2] ~ asset_to_use[2]

        )
    ) %>%
    unlist()

  names(returned) <-new_names

  return(returned)

}


#' get_correlation_data_set
#'
#' @param asset_data_to_use
#' @param samples_for_MLE
#' @param test_samples
#' @param assets_to_filter
#'
#' @return
#' @export
#'
#' @examples
get_correlation_data_set <- function(
    asset_data_to_use = starting_asset_data_bid_15,
    samples_for_MLE = 0.5,
    test_samples = 0.4,
    assets_to_filter = c(c("AUD_USD", "NZD_USD"),
                         c("EUR_USD", "GBP_USD"),
                         c("EUR_JPY", "EUR_USD"),
                         c("GBP_USD", "EUR_GBP"),
                         c("AU200_AUD", "SPX500_USD"),
                         c("US2000_USD", "SPX500_USD"),
                         c("WTICO_USD", "BCO_USD"),
                         c("XAG_USD", "XAU_USD"),
                         c("USD_CAD", "USD_JPY")

    )
) {

  AUD_NZD_USD <-
    estimating_dual_copula(
      asset_data_to_use = asset_data_to_use,
      asset_to_use = c("AUD_USD", "NZD_USD"),
      price_col = "Open",
      rolling_period = 100,
      samples_for_MLE = samples_for_MLE,
      test_samples = test_samples
    )

  AU_200_SPX <-
    estimating_dual_copula(
      asset_data_to_use = asset_data_to_use,
      asset_to_use = c("AU200_AUD", "SPX500_USD"),
      price_col = "Open",
      rolling_period = 100,
      samples_for_MLE = samples_for_MLE,
      test_samples = test_samples
    )

  SPX_US200 <-
    estimating_dual_copula(
      asset_data_to_use = asset_data_to_use,
      asset_to_use = c("US2000_USD", "SPX500_USD"),
      price_col = "Open",
      rolling_period = 100,
      samples_for_MLE = samples_for_MLE,
      test_samples = test_samples
    ) %>%
    dplyr::select(Date, contains("US2000"), US2000_USD_SPX500_USD_cor)

  EUR_USD_GBP <-
    estimating_dual_copula(
      asset_data_to_use = asset_data_to_use,
      asset_to_use = c("EUR_USD", "GBP_USD"),
      price_col = "Open",
      rolling_period = 100,
      samples_for_MLE = samples_for_MLE,
      test_samples = test_samples
    )

  EUR_USD_JPY <-
    estimating_dual_copula(
      asset_data_to_use = asset_data_to_use,
      asset_to_use = c("EUR_JPY", "EUR_USD"),
      price_col = "Open",
      rolling_period = 100,
      samples_for_MLE = samples_for_MLE,
      test_samples = test_samples
    ) %>%
    dplyr::select(Date, contains("EUR_JPY"), EUR_JPY_EUR_USD_cor)

  EUR_GBP <-
    estimating_dual_copula(
      asset_data_to_use = asset_data_to_use,
      asset_to_use = c("GBP_USD", "EUR_GBP"),
      price_col = "Open",
      rolling_period = 100,
      samples_for_MLE = samples_for_MLE,
      test_samples = test_samples
    ) %>%
    dplyr::select(Date, contains("EUR_GBP"), GBP_USD_EUR_GBP_cor)

  EUR_JPY_GBP <-
    estimating_dual_copula(
      asset_data_to_use = asset_data_to_use,
      asset_to_use = c("EUR_JPY", "GBP_JPY"),
      price_col = "Open",
      rolling_period = 100,
      samples_for_MLE = samples_for_MLE,
      test_samples = test_samples
    ) %>%
    dplyr::select(Date, EUR_JPY_GBP_JPY_cor)

  CAD_USD_JPY <-
    estimating_dual_copula(
      asset_data_to_use = asset_data_to_use,
      asset_to_use = c("USD_CAD", "USD_JPY"),
      price_col = "Open",
      rolling_period = 100,
      samples_for_MLE = samples_for_MLE,
      test_samples = test_samples
    ) %>%
    dplyr::select(Date, USD_CAD_USD_JPY_cor, contains("USD_CAD"))

  XAG_XAU_USD <-
    estimating_dual_copula(
      asset_data_to_use = asset_data_to_use,
      asset_to_use = c("XAG_USD", "XAU_USD"),
      price_col = "Open",
      rolling_period = 100,
      samples_for_MLE = samples_for_MLE,
      test_samples = test_samples
    )

  BCO_WTI_USD <-
    estimating_dual_copula(
      asset_data_to_use = asset_data_to_use,
      asset_to_use = c("WTICO_USD", "BCO_USD"),
      price_col = "Open",
      rolling_period = 100,
      samples_for_MLE = samples_for_MLE,
      test_samples = test_samples
    )


  asset_joined_copulas <-
    asset_data_to_use %>%
    left_join(AUD_NZD_USD) %>%
    left_join(EUR_USD_GBP) %>%
    left_join(EUR_USD_JPY) %>%
    left_join(EUR_GBP) %>%
    left_join(SPX_US200) %>%
    left_join(AU_200_SPX) %>%
    left_join(EUR_JPY_GBP) %>%
    left_join(CAD_USD_JPY) %>%
    left_join(XAG_XAU_USD) %>%
    left_join(BCO_WTI_USD) %>%
    filter(if_all(contains("_cor"), ~ !is.na(.)))

  return(asset_joined_copulas)

}


#' get_correlation_reg_dat
#'
#' @param asset_data_to_use
#' @param samples_for_MLE
#' @param test_samples
#' @param regression_train_prop
#' @param dependant_period
#' @param assets_to_filter
#'
#' @return
#' @export
#'
#' @examples
get_correlation_reg_dat <- function(
    asset_data_to_use = starting_asset_data_bid_15,
    samples_for_MLE = 0.5,
    test_samples = 0.4,
    regression_train_prop = 0.5,
    dependant_period = 10,
    assets_to_filter = c(c("AUD_USD", "NZD_USD"),
                         c("EUR_USD", "GBP_USD"),
                         c("EUR_JPY", "EUR_USD"),
                         c("GBP_USD", "EUR_GBP"),
                         c("AU200_AUD", "SPX500_USD"),
                         c("US2000_USD", "SPX500_USD"),
                         c("WTICO_USD", "BCO_USD"),
                         c("XAG_USD", "XAU_USD"),
                         c("USD_CAD", "USD_JPY"))
) {

  asset_joined_copulas <-
    get_correlation_data_set(
      asset_data_to_use = asset_data_to_use,
      samples_for_MLE = samples_for_MLE,
      test_samples = test_samples,
      assets_to_filter = assets_to_filter
    ) %>%
    group_by(Asset) %>%
    mutate(
      dependant_var = log(lead(Price, dependant_period)/Price)
    ) %>%
    ungroup() %>%
    filter(!is.na(dependant_var))

  regressors <- names(asset_joined_copulas) %>%
    keep(~ str_detect(.x, "quantiles|_cor|tangent|log"))
  lm_formula <- create_lm_formula(dependant = "dependant_var",
                                  independant = regressors)

  train_data <- asset_joined_copulas %>%
    group_by(Asset) %>%
    slice_head(prop = regression_train_prop) %>%
    ungroup() %>%
    filter(Asset %in% assets_to_filter)

  lm_model <- lm(formula = lm_formula, data = train_data)
  summary(lm_model)

  training_predictions <-
    predict.lm(object = lm_model, newdata = train_data) %>% as.numeric()

  mean_sd_predictons <-train_data%>%
    mutate(
      pred = training_predictions
    ) %>%
    group_by(Asset) %>%
    summarise(
      mean_pred = mean(pred, na.rm = T),
      sd_pred = sd(pred, na.rm = T)
    )

  testing_data <- asset_joined_copulas %>%
    group_by(Asset) %>%
    slice_tail(prop = (1 - regression_train_prop) ) %>%
    ungroup() %>%
    filter(Asset %in% assets_to_filter)

  predictions <- predict.lm(object = lm_model, newdata = testing_data) %>% as.numeric()

  testing_data <- testing_data %>%
    mutate(
      pred =predictions
    ) %>%
    left_join(mean_sd_predictons)

  return(list(testing_data, lm_model))

}

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
get_cor_trade_results <-
  function(
    testing_data = testing_ramapped,
    raw_asset_data = starting_asset_data_ask_15,
    sd_fac1 = 2,
    sd_fac2 = 2,
    stop_factor = 17,
    profit_factor = 25,
    trade_direction = "Long",
    mean_values_by_asset_for_loop = mean_values_by_asset_for_loop_15_ask,
    currency_conversion = currency_conversion,
    asset_infor = asset_infor,
    risk_dollar_value = 10,
    return_analysis = TRUE,
    pos_or_neg = "neg"
  ) {

    if(pos_or_neg == "pos") {
      tagged_trades <-
        testing_data %>%
        mutate(
          trade_col =
            case_when(
              pred >= mean_pred + sd_pred*sd_fac1 ~ trade_direction
              # pred <= mean_pred - sd_pred*sd_fac2 ~ trade_direction

              # pred >= mean_pred + sd_pred*sd_fac1 &
              #   pred <= mean_pred - sd_pred*sd_fac2 ~ trade_direction

            )
        ) %>%
        filter(!is.na(trade_col))
    }

    if(pos_or_neg == "neg") {
      tagged_trades <-
        testing_data %>%
        mutate(
          trade_col =
            case_when(
              pred <= mean_pred - sd_pred*sd_fac1 ~ trade_direction,
              pred >= mean_pred + sd_pred*sd_fac2 ~ trade_direction
            )
        ) %>%
        filter(!is.na(trade_col))
    }

    asset_in_trades <- tagged_trades %>%
      ungroup() %>%
      distinct(Asset) %>%
      pull(Asset) %>%
      unique()

    raw_asset_data <-
      raw_asset_data %>%
      ungroup() %>%
      filter(Asset %in% asset_in_trades)

    if(return_analysis == TRUE) {
      long_bayes_loop_analysis_neg <-
        generic_trade_finder_loop(
          tagged_trades = tagged_trades ,
          asset_data_daily_raw = raw_asset_data,
          stop_factor = stop_factor,
          profit_factor =profit_factor,
          trade_col = "trade_col",
          date_col = "Date",
          start_price_col = "Price",
          mean_values_by_asset = mean_values_by_asset_for_loop
        )

      trade_timings_neg <-
        long_bayes_loop_analysis_neg %>%
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
          trade_data = long_bayes_loop_analysis_neg %>% rename(Asset = asset),
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
          sd_fac1 = sd_fac1,
          pos_or_neg = pos_or_neg,
          sd_fac2 = sd_fac2
        ) %>%
        bind_cols(trade_timings_by_asset_neg)

      analysis_data_asset_neg <-
        generic_anlyser(
          trade_data = long_bayes_loop_analysis_neg %>% rename(Asset = asset),
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
          sd_fac1 = sd_fac1,
          pos_or_neg = pos_or_neg,
          sd_fac2= sd_fac2
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

