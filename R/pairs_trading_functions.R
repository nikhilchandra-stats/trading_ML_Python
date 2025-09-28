#' bivariate_normal_density
#'
#' @param x1
#' @param x2
#' @param mean_vec
#' @param sigma
#'
#' @return
#' @export
#'
#' @examples
bivariate_normal_density <- function(x1 = 0.2,
                                     x2 = 0.1,
                                     mean_vec = c(0.05, 0.06),
                                     sigma = sigma_matrix) {

  x_var = c(x1, x2)
  a1 <- (1/(2*pi))^(length(x_var)/2)
  sigma <- as.matrix(sigma)
  det_sigma <- det(sigma)
  det_sigma_sqrt <- det_sigma^(-1/2)

  a2 <- as.matrix((x_var - mean_vec))
  a2_T <- t(a2)
  sigma_inv <- MASS::ginv(sigma)

  p_x_1 <- a1*det_sigma_sqrt
  p_x_2 <- (-0.5)*( a2_T%*%sigma_inv%*%a2 )
  p_x_2_exp <- exp(p_x_2)
  px <- p_x_1*p_x_2_exp

  px <- px %>% as.numeric()

  return(px)

}

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
cauchy_dual_copula_generic <-  function(
    asset_data_to_use = major_indices_cumulative_pca %>% filter(Asset == "SPX500_USD"),
    cols_to_use = c("Return_Index", "Average_PCA"),
    rolling_period = 100,
    samples_for_MLE = 0.5,
    test_samples = 0.4
) {

  combined_data <- asset_data_to_use %>%
    dplyr::select(Date, !!as.name(cols_to_use[1]), !!as.name(cols_to_use[2]) ) %>%
    filter(if_all(.cols = everything(), ~ !is.na(.)))

  asset1_modeling <-combined_data %>%
    slice_head(prop = samples_for_MLE) %>%
    pull(!!as.name(cols_to_use[1]))

  asset2_modeling <-combined_data %>%
    slice_head(prop = samples_for_MLE) %>%
    pull(!!as.name(cols_to_use[2]))

  mle1 <- fitdistrplus::fitdist(asset1_modeling %>% keep(~ !is.na(.x) & !is.nan(.x)) , distr = "cauchy")
  mle1_1 <- mle1$estimate[1] %>% as.numeric()
  mle1_2 <- mle1$estimate[2] %>% as.numeric()
  mle2 <- fitdistrplus::fitdist(asset2_modeling %>% keep(~ !is.na(.x) & !is.nan(.x)) , distr = "cauchy")
  mle2_1 <- mle2$estimate[1] %>% as.numeric()
  mle2_2 <- mle2$estimate[2] %>% as.numeric()

  mle_training_cdf_1 <-
    pcauchy(asset1_modeling, location = mle1_1, scale = mle1_2)
  mle_training_cdf_2 <-
    pcauchy(asset2_modeling, location = mle2_1, scale = mle2_2)

  static_metrics <-
    combined_data %>%
    slice_head(prop = samples_for_MLE) %>%
    mutate(
      FX1 = pcauchy(!!as.name(cols_to_use[1]), location = mle1_1, scale = mle1_2),
      FX2 = pcauchy(!!as.name(cols_to_use[2]), location = mle2_1, scale = mle2_2),
      CDF_INV_FX1_ZNORM = pnorm(FX1),
      CDF_INV_FX2_ZNORM = pnorm(FX2)
    )


  rho <- cor(static_metrics$FX1, static_metrics$FX2)

  sigma_matrix =  matrix(
    c(
      1, rho,
      rho, 1
    ),
    nrow = 2
  )

  mean_values <- c(mean(mle_training_cdf_1, na.rm  = T),
                   mean(mle_training_cdf_2, na.rm  = T)
                   )

  combined_data2 <-
    combined_data %>%
    ungroup() %>%
    slice_tail(prop = test_samples) %>%
    mutate(
      #-------Probability Integral Transform
      FX1 = pcauchy(!!as.name(cols_to_use[1]), location = mle1_1, scale = mle1_2),
      FX2 = pcauchy(!!as.name(cols_to_use[2]), location = mle2_1, scale = mle2_2),
      CDF_INV_FX1_ZNORM = pnorm(FX1),
      CDF_INV_FX2_ZNORM = pnorm(FX2),
      #Estimate the Probability of FX with a standard normal
      joint_density_copula =
        slider::slide2_dbl(
          .x = CDF_INV_FX1_ZNORM,
          .y = CDF_INV_FX2_ZNORM,
          .f =
            ~
            bivariate_normal_density(x1 = .x,
                                     x2 = .y,
                                     mean_vec = mean_values,
                                     sigma = sigma_matrix
            ),
          .before = 0
        ),

      joint_density = CDF_INV_FX1_ZNORM*CDF_INV_FX2_ZNORM*joint_density_copula
    )

  return(combined_data2)

}


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
    test_samples = 0.4,
    skip_log = FALSE
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

  if(skip_log == FALSE) {
    combined_data <- asset1 %>%
      left_join(asset2) %>%
      mutate(
        log1_price = log(!!as.name(asset_to_use[1])),
        log2_price = log(!!as.name(asset_to_use[2]))
      )
  }

  if(skip_log == TRUE) {
    combined_data <-
      asset1 %>%
      left_join(asset2) %>%
      mutate(
        log1_price = !!as.name(asset_to_use[1]),
        log2_price = !!as.name(asset_to_use[2])
      )
  }

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

  quantiles_1_LM = pcauchy(asset1_modeling, location = mle1_1, scale = mle1_2)
  quantiles_2_LM = pcauchy(asset2_modeling, location = mle2_1, scale = mle2_2)

  temp_LM_tibble <- tibble(y = quantiles_1_LM, x = quantiles_2_LM)
  temp_LM <- lm(data = temp_LM_tibble, formula = y ~ x)

  combined_data2 <- combined_data %>%
    mutate(
      quantiles_1 = pcauchy(log1_price, location = mle1_1, scale = mle1_2),
      quantiles_2 = pcauchy(log2_price, location = mle2_1, scale = mle2_2)
    )

  dual_quantile_LM_pred =
    predict.lm(temp_LM,
               newdata = combined_data2 %>% dplyr::select(x = quantiles_2)) %>%
    as.numeric()

  rm(quantiles_1_LM, quantiles_2_LM, temp_LM_tibble, temp_LM, mle1, mle1_1, mle1_2, mle2, mle2_1, mle2_1, mle2_2)
  gc()

  combined_data2 <- combined_data2 %>%
    mutate(quant_lm_pred = dual_quantile_LM_pred) %>%
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

  rm(combined_data2)
  gc()

  returned <- returned %>%
    mutate(across(.cols = !matches("Date", ignore.case = FALSE),
                  .fns = ~ lag(.))) %>%
    mutate(
      correlation_vars_mean =
        slider::slide_dbl(.x = correlation_vars, .f = ~ mean(., na.rm = T), .before = rolling_period),
      correlation_vars_sd =
        slider::slide_dbl(.x = correlation_vars, .f = ~ sd(., na.rm = T), .before = rolling_period),
      quant_lm_mean =
        slider::slide_dbl(.x = quant_lm_pred, .f = ~ mean(., na.rm = T), .before = rolling_period),
      quant_lm_sd =
        slider::slide_dbl(.x = quant_lm_pred, .f = ~ sd(., na.rm = T), .before = rolling_period),
    )

  gc()

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
          .x == "correlation_vars" & !str_detect(.x, "_mean|_sd") ~ paste0(asset_to_use[1], "_",asset_to_use[2],"_","cor"),
          .x == "correlation_vars_mean" ~ paste0(asset_to_use[1], "_",asset_to_use[2],"_","cor_mean"),
          .x == "correlation_vars_sd" ~ paste0(asset_to_use[1], "_",asset_to_use[2],"_","cor_sd"),

          .x == "quant_lm_pred" & !str_detect(.x, "_mean|_sd") ~ paste0(asset_to_use[1], "_",asset_to_use[2],"_","quant_lm"),
          .x == "quant_lm_mean" ~ paste0(asset_to_use[1], "_",asset_to_use[2],"_","quant_lm_mean"),
          .x == "quant_lm_sd" ~ paste0(asset_to_use[1], "_",asset_to_use[2],"_","quant_lm_sd"),

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
    dplyr::select(Date, contains("US2000"), contains("US2000_USD_SPX500_USD_cor"))

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
    dplyr::select(Date, contains("EUR_JPY"), contains("EUR_JPY_EUR_USD_cor"))

  EUR_GBP <-
    estimating_dual_copula(
      asset_data_to_use = asset_data_to_use,
      asset_to_use = c("GBP_USD", "EUR_GBP"),
      price_col = "Open",
      rolling_period = 100,
      samples_for_MLE = samples_for_MLE,
      test_samples = test_samples
    ) %>%
    dplyr::select(Date, contains("EUR_GBP"), contains("GBP_USD_EUR_GBP_cor"))

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
    dplyr::select(Date, contains("USD_CAD_USD_JPY_cor"), contains("USD_CAD"))

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
    asset_data_to_use = new_15_data_bid,
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
    ungroup()

  regressors <- names(asset_joined_copulas) %>%
    keep(~ str_detect(.x, "quantiles|_cor|tangent|log"))
  lm_formula <- create_lm_formula(dependant = "dependant_var",
                                  independant = regressors)

  train_data <- asset_joined_copulas %>%
    filter(!is.na(dependant_var)) %>%
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


    if(return_analysis == TRUE) {

      asset_in_trades <- tagged_trades %>%
        ungroup() %>%
        distinct(Asset) %>%
        pull(Asset) %>%
        unique()

      raw_asset_data <-
        raw_asset_data %>%
        ungroup() %>%
        filter(Asset %in% asset_in_trades)

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


#' get_pairs_cor_reg_trades_to_take
#'
#' @param db_path
#' @param min_risk_win
#' @param return_filter_col
#' @param max_win_time
#' @param starting_asset_data_ask_15M
#' @param starting_asset_data_bid_15M
#' @param mean_values_by_asset
#' @param trade_direction
#' @param risk_dollar_value
#' @param currency_conversion
#' @param asset_infor
#'
#' @return
#' @export
#'
#' @examples
get_pairs_cor_reg_trades_to_take <- function(
    db_path = glue::glue("C:/Users/Nikhil Chandra/Documents/trade_data/pairs_2025-07-03.db"),
    min_risk_win = 0.01,
    return_filter_col = "median_return",
    max_win_time = 150,
    starting_asset_data_ask_15M = new_15_data_ask,
    starting_asset_data_bid_15M = new_15_data_bid,
    mean_values_by_asset = mean_values_by_asset_for_loop_15_ask,
    trade_direction = "Long",
    risk_dollar_value = 10,
    currency_conversion = currency_conversion,
    asset_infor = asset_infor) {

  db_con <- connect_db(db_path)

  current_analysis <-
    DBI::dbGetQuery(conn = db_con, statement = "SELECT * FROM pairs_lm") %>%
    group_by(sd_fac1, sd_fac2, pos_or_neg, profit_factor, stop_factor, trade_direction) %>%
    summarise(

      min_return = quantile(risk_weighted_return, 0.01),
      low_return = quantile(risk_weighted_return, 0.25),
      mean_return = mean(risk_weighted_return, na.rm = T),
      median_return = median(risk_weighted_return, na.rm = T),
      high_return = quantile(risk_weighted_return, 0.75),
      max_return = quantile(risk_weighted_return, 0.99),

      win_time_hours = median(win_time_hours, na.rm = T)

    ) %>%
    mutate(
      low_to_high_ratio = abs(high_return)/abs(low_return)
    ) %>%
    filter(win_time_hours < max_win_time) %>%
    filter(!!as.name(return_filter_col) >= min_risk_win) %>%
    group_by(sd_fac1, sd_fac2, pos_or_neg, profit_factor, stop_factor, trade_direction) %>%
    slice_min(win_time_hours)

  DBI::dbDisconnect(db_con)

  all_trades_for_today <- list()

  tictoc::tic()

  for (k in 1:dim(current_analysis)[1] ) {

    samples_for_MLE = 0.5
    test_samples = 0.4
    regression_train_prop = 0.5
    dependant_period = 10

    stop_factor <- current_analysis$stop_factor[k] %>% as.numeric()
    profit_factor <- current_analysis$profit_factor[k] %>% as.numeric()
    win_time_hours <- current_analysis$win_time_hours[k] %>% as.numeric()
    sd_fac1 <- current_analysis$sd_fac1[k] %>% as.numeric()
    sd_fac2 <- current_analysis$sd_fac2[k] %>% as.numeric()
    pos_or_neg <- current_analysis$pos_or_neg[k] %>% as.character()
    trade_direction <- current_analysis$trade_direction[k] %>% as.character()

    cor_data_list <-
      get_correlation_reg_dat(
        asset_data_to_use = starting_asset_data_bid_15M,
        # asset_data_to_use = sim_data_bid,
        samples_for_MLE = samples_for_MLE,
        test_samples = test_samples,
        regression_train_prop = regression_train_prop,
        dependant_period = dependant_period,
        assets_to_filter = c(
          c("AUD_USD", "NZD_USD"),
          c("EUR_USD", "GBP_USD"),
          c("EUR_JPY", "EUR_USD"),
          c("GBP_USD", "EUR_GBP"),
          c("AU200_AUD", "SPX500_USD"),
          c("US2000_USD", "SPX500_USD"),
          c("WTICO_USD", "BCO_USD"),
          c("XAG_USD", "XAU_USD"),
          c("USD_CAD", "USD_JPY")
        )
      )

    testing_data <- cor_data_list[[1]]
    testing_ramapped <-
      testing_data %>% dplyr::select(-c(Price, Open, High, Low))

    date_in_testing_data <-
      testing_ramapped %>%
      pull(Date) %>%
      max(na.rm = T) %>%
      pluck(1) %>%
      as.character()

    message(glue::glue("Max date in testing Data {date_in_testing_data}"))

    mean_values_by_asset_for_loop_15_bid =
      wrangle_asset_data(
        asset_data_daily_raw = starting_asset_data_bid_15M,
        summarise_means = TRUE
      )
    mean_values_by_asset_for_loop_15_ask =
      wrangle_asset_data(
        asset_data_daily_raw = starting_asset_data_ask_15M,
        summarise_means = TRUE
      )

    # get_cor_trade_results
    #safely_get_trades
    trade_results <-
      get_cor_trade_results(
        testing_data = testing_ramapped,
        raw_asset_data = starting_asset_data_ask_15M,
        # raw_asset_data = sim_data_ask,
        sd_fac1 = sd_fac1,
        sd_fac2 = sd_fac2,
        stop_factor = stop_factor,
        profit_factor = profit_factor,
        trade_direction = trade_direction,
        mean_values_by_asset_for_loop = mean_values_by_asset_for_loop_15_ask,
        currency_conversion = currency_conversion,
        asset_infor = asset_infor,
        risk_dollar_value = risk_dollar_value,
        return_analysis = FALSE,
        pos_or_neg = pos_or_neg
      )

    tagged_trades <- trade_results[[1]]

    date_in_tagged_trades <-
      tagged_trades %>%
      pull(Date) %>%
      max(na.rm = T) %>%
      pluck(1) %>%
      as.character()

    message(glue::glue("Max date in Tagged Trades {date_in_tagged_trades}"))

    if(dim(tagged_trades)[1] > 0) {

      temp_trades <- tagged_trades %>%
        ungroup() %>%
        slice_max(Date)

      if(dim(temp_trades)[1] > 0) {
        temp_trades2 <- temp_trades %>%
          filter(!is.na(trade_col), trade_col == trade_direction) %>%
          mutate(
            win_time_hours = win_time_hours,
            sd_fac1 = sd_fac1,
            sd_fac2 = sd_fac2,
            trade_direction = trade_direction,
            stop_factor = stop_factor,
            profit_factor = profit_factor
          )
      }

      if( dim(temp_trades2)[1] > 0) {
        all_trades_for_today[[k]] <- temp_trades2
      } else {
        all_trades_for_today[[k]] <- NULL
      }

    }

  }

  tictoc::toc()

  all_trades_for_today_dfr <- all_trades_for_today %>%
    keep(~ !is.null(.x)) %>%
    map_dfr(bind_rows) %>%
    dplyr::select(Date, Asset, trade_col, trade_direction, win_time_hours,
                  sd_fac1, sd_fac2, stop_factor, profit_factor)

  if(dim(all_trades_for_today_dfr)[1] > 0) {

    returned_data <-
      all_trades_for_today_dfr %>%
      ungroup() %>%
      left_join(starting_asset_data_ask_15M %>%
                  dplyr::select(Date, Asset, Price, Open, High, Low)) %>%
      left_join(current_analysis  ) %>%
      group_by(Asset) %>%
      slice_min(win_time_hours) %>%
      ungroup() %>%
      dplyr::group_by(Date, Asset,  Price, Low, High, Open, trade_col) %>%
      slice_max(profit_factor) %>%
      ungroup()

    stops_profs <- returned_data %>%
      distinct(Date, Asset, stop_factor, profit_factor, Price, Low, High, Open)

    stops_profs_distinct <- stops_profs %>% distinct(stop_factor, profit_factor)

    returned_data2 <- list()

    for (o in 1:dim(stops_profs_distinct)[1] ) {

      stop_factor <- stops_profs$stop_factor[o] %>% as.numeric()
      profit_factor <- stops_profs$profit_factor[o] %>% as.numeric()

      returned_data2[[o]] <- generic_trade_finder_loop(
        tagged_trades = returned_data ,
        asset_data_daily_raw = starting_asset_data_ask_15M,
        stop_factor = stop_factor,
        profit_factor =profit_factor,
        trade_col = "trade_col",
        date_col = "Date",
        start_price_col = "Price",
        mean_values_by_asset = mean_values_by_asset_for_loop_15_ask
      ) %>%
        rename(Date = dates, Asset = asset) %>%
        mutate(stop_factor = stop_factor,
               profit_factor = profit_factor) %>%
        left_join(
          stops_profs
        )

    }

    returned_data3 <-
      returned_data2 %>%
      map_dfr(bind_rows) %>%
      group_by(Asset) %>%
      slice_min(profit_factor) %>%
      ungroup()

  } else {
    returned_data3 <- NULL
  }

  DBI::dbDisconnect(db_con)
  rm(db_con)

  return(returned_data3)

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
get_correlation_data_set_v2 <- function(
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
                         c("USD_CAD", "USD_JPY"),
                         c("SG30_SGD", "SPX500_USD"),
                         c("NZD_CHF", "NZD_USD"),
                         c("SPX500_USD", "XAU_USD"),
                         c("NZD_CHF", "USD_CHF"),
                         c("EU50_EUR", "DE30_EUR"),
                         c("EU50_EUR", "SPX500_USD"),
                         c("DE30_EUR", "SPX500_USD"),
                         c("USD_SEK", "EUR_SEK")

    ),
    rolling_period = 100
) {

  AUD_NZD_USD <-
    estimating_dual_copula(
      asset_data_to_use = asset_data_to_use,
      asset_to_use = c("AUD_USD", "NZD_USD"),
      price_col = "Open",
      rolling_period = rolling_period,
      samples_for_MLE = samples_for_MLE,
      test_samples = test_samples
    )

  gc()

  EUR_SEK_USD <-
    estimating_dual_copula(
      asset_data_to_use = asset_data_to_use,
      asset_to_use = c("USD_SEK", "EUR_SEK"),
      price_col = "Open",
      rolling_period = rolling_period,
      samples_for_MLE = samples_for_MLE,
      test_samples = test_samples
    )

  gc()

  EU50_DE30 <-
    estimating_dual_copula(
      asset_data_to_use = asset_data_to_use,
      asset_to_use = c("EU50_EUR", "DE30_EUR"),
      price_col = "Open",
      rolling_period = rolling_period,
      samples_for_MLE = samples_for_MLE,
      test_samples = test_samples
    )

  gc()

  EU50_SPX <-
    estimating_dual_copula(
      asset_data_to_use = asset_data_to_use,
      asset_to_use = c("EU50_EUR", "SPX500_USD"),
      price_col = "Open",
      rolling_period = rolling_period,
      samples_for_MLE = samples_for_MLE,
      test_samples = test_samples
    ) %>%
    dplyr::select(Date, EU50_EUR_SPX500_USD_cor)

  gc()

  DE30_SPX <-
    estimating_dual_copula(
      asset_data_to_use = asset_data_to_use,
      asset_to_use = c("DE30_EUR", "SPX500_USD"),
      price_col = "Open",
      rolling_period = rolling_period,
      samples_for_MLE = samples_for_MLE,
      test_samples = test_samples
    ) %>%
    dplyr::select(Date, DE30_EUR_SPX500_USD_cor)

  gc()

  CHF_NZD_USD <-
    estimating_dual_copula(
      asset_data_to_use = asset_data_to_use,
      asset_to_use = c("NZD_CHF", "NZD_USD"),
      price_col = "Open",
      rolling_period = rolling_period,
      samples_for_MLE = samples_for_MLE,
      test_samples = test_samples
    )%>%
    dplyr::select(Date, contains("NZD_CHF"), NZD_CHF_NZD_USD_cor)

  gc()

  USD_CHF_NZD <-
    estimating_dual_copula(
      asset_data_to_use = asset_data_to_use,
      asset_to_use = c("NZD_CHF", "USD_CHF"),
      price_col = "Open",
      rolling_period = rolling_period,
      samples_for_MLE = samples_for_MLE,
      test_samples = test_samples
    )%>%
    dplyr::select(Date, contains("USD_CHF"), NZD_CHF_USD_CHF_cor)

  gc()

  AU_200_SPX <-
    estimating_dual_copula(
      asset_data_to_use = asset_data_to_use,
      asset_to_use = c("AU200_AUD", "SPX500_USD"),
      price_col = "Open",
      rolling_period = rolling_period,
      samples_for_MLE = samples_for_MLE,
      test_samples = test_samples
    )

  gc()

  SPX_US200 <-
    estimating_dual_copula(
      asset_data_to_use = asset_data_to_use,
      asset_to_use = c("US2000_USD", "SPX500_USD"),
      price_col = "Open",
      rolling_period = rolling_period,
      samples_for_MLE = samples_for_MLE,
      test_samples = test_samples
    ) %>%
    dplyr::select(Date, contains("US2000"), US2000_USD_SPX500_USD_cor)

  gc()

  SPX_XAU <-
    estimating_dual_copula(
      asset_data_to_use = asset_data_to_use,
      asset_to_use = c("XAU_USD", "SPX500_USD"),
      price_col = "Open",
      rolling_period = rolling_period,
      samples_for_MLE = samples_for_MLE,
      test_samples = test_samples
    ) %>%
    dplyr::select(Date, XAU_USD_SPX500_USD_cor)

  gc()

  SG30_SPX <-
    estimating_dual_copula(
      asset_data_to_use = asset_data_to_use,
      asset_to_use = c("SG30_SGD", "SPX500_USD"),
      price_col = "Open",
      rolling_period = rolling_period,
      samples_for_MLE = samples_for_MLE,
      test_samples = test_samples
    ) %>%
    dplyr::select(Date, contains("SG30_SGD"), SG30_SGD_SPX500_USD_cor)

  gc()

  EUR_USD_GBP <-
    estimating_dual_copula(
      asset_data_to_use = asset_data_to_use,
      asset_to_use = c("EUR_USD", "GBP_USD"),
      price_col = "Open",
      rolling_period = rolling_period,
      samples_for_MLE = samples_for_MLE,
      test_samples = test_samples
    )

  gc()

  EUR_USD_JPY <-
    estimating_dual_copula(
      asset_data_to_use = asset_data_to_use,
      asset_to_use = c("EUR_JPY", "EUR_USD"),
      price_col = "Open",
      rolling_period = rolling_period,
      samples_for_MLE = samples_for_MLE,
      test_samples = test_samples
    ) %>%
    dplyr::select(Date, contains("EUR_JPY"), EUR_JPY_EUR_USD_cor)

  gc()

  EUR_GBP <-
    estimating_dual_copula(
      asset_data_to_use = asset_data_to_use,
      asset_to_use = c("GBP_USD", "EUR_GBP"),
      price_col = "Open",
      rolling_period = rolling_period,
      samples_for_MLE = samples_for_MLE,
      test_samples = test_samples
    ) %>%
    dplyr::select(Date, contains("EUR_GBP"), GBP_USD_EUR_GBP_cor)

  gc()

  EUR_JPY_GBP <-
    estimating_dual_copula(
      asset_data_to_use = asset_data_to_use,
      asset_to_use = c("EUR_JPY", "GBP_JPY"),
      price_col = "Open",
      rolling_period = rolling_period,
      samples_for_MLE = samples_for_MLE,
      test_samples = test_samples
    ) %>%
    dplyr::select(Date, EUR_JPY_GBP_JPY_cor, contains("GBP_JPY"))

  gc()

  CAD_USD_JPY <-
    estimating_dual_copula(
      asset_data_to_use = asset_data_to_use,
      asset_to_use = c("USD_CAD", "USD_JPY"),
      price_col = "Open",
      rolling_period = rolling_period,
      samples_for_MLE = samples_for_MLE,
      test_samples = test_samples
    ) %>%
    dplyr::select(Date, USD_CAD_USD_JPY_cor, contains("USD_CAD"))

  gc()

  XAG_XAU_USD <-
    estimating_dual_copula(
      asset_data_to_use = asset_data_to_use,
      asset_to_use = c("XAG_USD", "XAU_USD"),
      price_col = "Open",
      rolling_period = rolling_period,
      samples_for_MLE = samples_for_MLE,
      test_samples = test_samples
    )

  gc()

  BCO_WTI_USD <-
    estimating_dual_copula(
      asset_data_to_use = asset_data_to_use,
      asset_to_use = c("WTICO_USD", "BCO_USD"),
      price_col = "Open",
      rolling_period = rolling_period,
      samples_for_MLE = samples_for_MLE,
      test_samples = test_samples
    )

  gc()


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
    left_join(SG30_SPX) %>%
    left_join(SPX_XAU) %>%
    left_join(CHF_NZD_USD) %>%
    left_join(USD_CHF_NZD) %>%
    left_join(EU50_DE30) %>%
    left_join(EU50_SPX) %>%
    left_join(DE30_SPX) %>%
    left_join(EUR_SEK_USD) %>%
    filter(if_all(contains("_cor"), ~ !is.na(.)))

  gc()

  return(asset_joined_copulas)

}


#' get_correlation_reg_dat_v2
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
get_correlation_reg_dat_v2 <- function(
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
                         c("USD_CAD", "USD_JPY"),
                         c("SG30_SGD", "SPX500_USD"),
                         c("NZD_CHF", "NZD_USD"),
                         c("SPX500_USD", "XAU_USD"),
                         c("NZD_CHF", "USD_CHF"),
                         c("EU50_EUR", "DE30_EUR"),
                         c("EU50_EUR", "SPX500_USD"),
                         c("DE30_EUR", "SPX500_USD"),
                         c("USD_SEK", "EUR_SEK")),
    rolling_period = 100
) {

  asset_joined_copulas <-
    get_correlation_data_set_v2(
      asset_data_to_use = asset_data_to_use,
      samples_for_MLE = samples_for_MLE,
      test_samples = test_samples,
      assets_to_filter = assets_to_filter,
      rolling_period = rolling_period
    ) %>%
    group_by(Asset) %>%
    mutate(
      dependant_var = log(lead(Price, dependant_period)/Price)
    ) %>%
    ungroup()

  regressors <- names(asset_joined_copulas) %>%
    keep(~ str_detect(.x, "quantiles|_cor|tangent|log"))
  lm_formula <- create_lm_formula(dependant = "dependant_var",
                                  independant = regressors)

  train_data <- asset_joined_copulas %>%
    filter(!is.na(dependant_var)) %>%
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

  cor_summs_mean <-
    train_data %>%
    dplyr::select(contains("_cor"))

  new_names_cor_summs <-
    names(cor_summs_mean) %>%
    map(
      ~
        paste0(.x, "_mean")
    ) %>%
    unlist()

  names(cor_summs_mean) <- new_names_cor_summs

  cor_summs_mean <- cor_summs_mean %>%
    summarise(
      across(
        .cols = everything(),
        .fns = ~ mean(., na.rm = T)
      )
    )

  cor_summs_sd <-
    train_data %>%
    dplyr::select(contains("_cor"))

  new_names_cor_summs <-
    names(cor_summs_sd) %>%
    map(
      ~
        paste0(.x, "_sd")
    ) %>%
    unlist()

  names(cor_summs_sd) <- new_names_cor_summs

  cor_summs_sd <- cor_summs_sd %>%
    summarise(
      across(
        .cols = everything(),
        .fns = ~ sd(., na.rm = T)
      )
    )

  testing_data2 <-
    testing_data %>%
    bind_cols(cor_summs_mean)%>%
    bind_cols(cor_summs_sd)

  return(list(testing_data2, lm_model))

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
get_cor_trade_results_v2 <-
  function(
    testing_data = testing_ramapped,
    raw_asset_data = starting_asset_data_ask_15,
    AUD_USD_sd = 0,
    NZD_USD_sd = 0,
    EUR_JPY_SD = 0,
    GBP_JPY_SD = 0,
    SPX_US2000_SD = 0,
    EUR_DE30_SD = 0,
    GBP_EUR_USD_SD = 0,
    EUR_GBP_USD_USD_SD = 0,
    NZD_CHF_USD_SD = 0,
    WTI_BCO_SD = 0,
    stop_factor = 17,
    profit_factor = 25,
    trade_direction = "Long",
    mean_values_by_asset_for_loop = mean_values_by_asset_for_loop_15_ask,
    currency_conversion = currency_conversion,
    asset_infor = asset_infor,
    risk_dollar_value = 10,
    return_analysis = TRUE
  ) {

    # names(testing_data) %>%
    #   keep(~ str_detect(.x, "tangent"))
    # names(testing_data) %>%
    #   keep(~ str_detect(.x, "cor"))

    tagged_trades <-
      testing_data %>%
      mutate(
        trade_col =
          case_when(
            (AUD_USD_NZD_USD_cor <= AUD_USD_NZD_USD_cor_mean - AUD_USD_NZD_USD_cor_sd*NZD_USD_sd) &
              Asset == "NZD_USD" & NZD_USD_tangent_angle2 < 0~ trade_direction,

            (AUD_USD_NZD_USD_cor <= AUD_USD_NZD_USD_cor_mean - AUD_USD_NZD_USD_cor_sd*NZD_USD_sd) &
              Asset == "AUD_USD" & NZD_USD_tangent_angle2 < 0~ trade_direction,

            (AUD_USD_NZD_USD_cor >= AUD_USD_NZD_USD_cor_mean + AUD_USD_NZD_USD_cor_sd*AUD_USD_sd) &
              Asset == "AUD_USD" & AUD_USD_tangent_angle1 < 0~ trade_direction,

            (EUR_JPY_GBP_JPY_cor >= EUR_JPY_GBP_JPY_cor_mean + EUR_JPY_GBP_JPY_cor_sd*EUR_JPY_SD) &
              Asset == "EUR_JPY" & EUR_JPY_tangent_angle1 < 0~ trade_direction,

            (EUR_JPY_GBP_JPY_cor <= EUR_JPY_GBP_JPY_cor_mean - EUR_JPY_GBP_JPY_cor_sd*EUR_JPY_SD) &
              Asset == "EUR_JPY" & EUR_JPY_tangent_angle1 > 0~ trade_direction,

            (US2000_USD_SPX500_USD_cor <= US2000_USD_SPX500_USD_cor_mean - US2000_USD_SPX500_USD_cor_sd*SPX_US2000_SD) &
              Asset == "SPX500_USD" & SPX500_USD_tangent_angle2 > 0~ trade_direction,

            (US2000_USD_SPX500_USD_cor >= US2000_USD_SPX500_USD_cor_mean + US2000_USD_SPX500_USD_cor_sd*SPX_US2000_SD) &
              Asset == "SPX500_USD" & SPX500_USD_tangent_angle2 < 0~ trade_direction,

            (US2000_USD_SPX500_USD_cor >= US2000_USD_SPX500_USD_cor_mean + US2000_USD_SPX500_USD_cor_sd*SPX_US2000_SD) &
              Asset == "US2000_USD" & US2000_USD_tangent_angle1 < 0~ trade_direction,

            (EU50_EUR_DE30_EUR_cor >= EU50_EUR_DE30_EUR_cor_mean + EU50_EUR_DE30_EUR_cor_sd*EUR_DE30_SD) &
              Asset == "EU50_EUR" & EU50_EUR_tangent_angle1 < 0~ trade_direction,

            (EU50_EUR_DE30_EUR_cor <= EU50_EUR_DE30_EUR_cor_mean - EU50_EUR_DE30_EUR_cor_sd*EUR_DE30_SD) &
              Asset == "EU50_EUR" & EU50_EUR_tangent_angle1 > 0~ trade_direction,

            (EU50_EUR_DE30_EUR_cor <= EU50_EUR_DE30_EUR_cor_mean - EU50_EUR_DE30_EUR_cor_sd*EUR_DE30_SD) &
              Asset == "EU50_EUR" & DE30_EUR_tangent_angle2 > 0~ trade_direction,

            (GBP_USD_EUR_GBP_cor <= GBP_USD_EUR_GBP_cor_mean - GBP_USD_EUR_GBP_cor_sd*GBP_EUR_USD_SD) &
              Asset == "EUR_GBP" & EUR_GBP_tangent_angle2 < 0~ trade_direction,

            (GBP_USD_EUR_GBP_cor >= GBP_USD_EUR_GBP_cor_mean + GBP_USD_EUR_GBP_cor_sd*GBP_EUR_USD_SD) &
              Asset == "GBP_USD" & GBP_USD_tangent_angle2 > 0~ trade_direction,

            (EUR_USD_GBP_USD_cor <= EUR_USD_GBP_USD_cor_mean - EUR_USD_GBP_USD_cor_sd*EUR_GBP_USD_USD_SD) &
              Asset == "GBP_USD" & GBP_USD_tangent_angle2 > 0~ trade_direction,

            (EUR_USD_GBP_USD_cor >= EUR_USD_GBP_USD_cor_mean + EUR_USD_GBP_USD_cor_sd*EUR_GBP_USD_USD_SD) &
              Asset == "EUR_USD" & GBP_USD_tangent_angle2 < 0~ trade_direction,

            (EUR_USD_GBP_USD_cor >= EUR_USD_GBP_USD_cor_mean + EUR_USD_GBP_USD_cor_sd*EUR_GBP_USD_USD_SD) &
              Asset == "GBP_USD" & EUR_USD_tangent_angle1 < 0~ trade_direction,

            (EUR_USD_GBP_USD_cor <= EUR_USD_GBP_USD_cor_mean - EUR_USD_GBP_USD_cor_sd*EUR_GBP_USD_USD_SD) &
              Asset == "EUR_USD" & EUR_USD_tangent_angle1 > 0~ trade_direction,

            (WTICO_USD_BCO_USD_cor <= WTICO_USD_BCO_USD_cor_mean - WTICO_USD_BCO_USD_cor_sd*WTI_BCO_SD) &
              Asset == "WTICO_USD" & WTICO_USD_tangent_angle1 < 0~ trade_direction,

            (WTICO_USD_BCO_USD_cor <= WTICO_USD_BCO_USD_cor_mean - WTICO_USD_BCO_USD_cor_sd*WTI_BCO_SD) &
              Asset == "BCO_USD" & BCO_USD_tangent_angle2 > 0~ trade_direction,

            (NZD_CHF_USD_CHF_cor <= NZD_CHF_USD_CHF_cor_mean - NZD_CHF_USD_CHF_cor_sd*NZD_CHF_USD_SD) &
              Asset == "NZD_CHF" & NZD_CHF_tangent_angle1 < 0~ trade_direction,

            (NZD_CHF_USD_CHF_cor <= NZD_CHF_USD_CHF_cor_mean - NZD_CHF_USD_CHF_cor_sd*NZD_CHF_USD_SD) &
              Asset == "NZD_CHF" & USD_CHF_tangent_angle2 > 0~ trade_direction

          )
      ) %>%
      filter(!is.na(trade_col))


    if(return_analysis == TRUE) {

      asset_in_trades <- tagged_trades %>%
        ungroup() %>%
        distinct(Asset) %>%
        pull(Asset) %>%
        unique()

      raw_asset_data_trade <-
        raw_asset_data %>%
        ungroup() %>%
        filter(Asset %in% asset_in_trades)

      long_bayes_loop_analysis_neg <-
        generic_trade_finder_loop(
          tagged_trades = tagged_trades ,
          asset_data_daily_raw = raw_asset_data_trade,
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


#' get_pairs_cor_reg_trades_to_take
#'
#' @param db_path
#' @param min_risk_win
#' @param return_filter_col
#' @param max_win_time
#' @param starting_asset_data_ask_15M
#' @param starting_asset_data_bid_15M
#' @param mean_values_by_asset
#' @param trade_direction
#' @param risk_dollar_value
#' @param currency_conversion
#' @param asset_infor
#'
#' @return
#' @export
#'
#' @examples
get_pairs_cor_reg_trades_to_take_asset_specific <- function(
    db_path = glue::glue("C:/Users/Nikhil Chandra/Documents/trade_data/pairs_2025-07-03.db"),
    min_risk_win = 0.05,
    return_filter_col = "median_return",
    max_win_time = 150,
    starting_asset_data_ask_15M = new_15_data_ask,
    starting_asset_data_bid_15M = new_15_data_bid,
    mean_values_by_asset = mean_values_by_asset_for_loop_15_ask,
    trade_direction = "Long",
    risk_dollar_value = 10,
    currency_conversion = currency_conversion,
    asset_infor = asset_infor) {


  db_con <- connect_db(db_path)

  current_analysis <-
    DBI::dbGetQuery(conn = db_con, statement = "SELECT * FROM pairs_asset_specific") %>%
    filter(trade_direction == trade_direction) %>%
    group_by(profit_factor, stop_factor, trade_direction) %>%
    summarise(

      min_return = quantile(risk_weighted_return, 0.01),
      low_return = quantile(risk_weighted_return, 0.25),
      mean_return = mean(risk_weighted_return, na.rm = T),
      median_return = median(risk_weighted_return, na.rm = T),
      high_return = quantile(risk_weighted_return, 0.75),
      max_return = quantile(risk_weighted_return, 0.99),

      win_time_hours = median(win_time_hours, na.rm = T)

    ) %>%
    mutate(
      low_to_high_ratio = abs(high_return)/abs(low_return)
    ) %>%
    filter(win_time_hours < max_win_time) %>%
    filter(!!as.name(return_filter_col) >= min_risk_win) %>%
    group_by(profit_factor, stop_factor, trade_direction) %>%
    slice_min(win_time_hours)

  DBI::dbDisconnect(db_con)

  all_trades_for_today <- list()

  all_trades_for_today <- list()
  safely_get_trades_v2 <- safely(get_cor_trade_results_v2, otherwise = NULL)
  safely_get_correlation_reg_dat_v2 <- safely(get_correlation_reg_dat_v2, otherwise = NULL)

  tictoc::tic()

  for (k in 1:dim(current_analysis)[1] ) {

    samples_for_MLE = 0.5
    test_samples = 0.4
    regression_train_prop = 0.5
    dependant_period = 10

    stop_factor <- current_analysis$stop_factor[k] %>% as.numeric()
    profit_factor <- current_analysis$profit_factor[k] %>% as.numeric()
    win_time_hours <- current_analysis$win_time_hours[k] %>% as.numeric()
    trade_direction <- current_analysis$trade_direction[k] %>% as.character()

    cor_data_list <-
      safely_get_correlation_reg_dat_v2(
        asset_data_to_use = starting_asset_data_bid_15M,
        samples_for_MLE = samples_for_MLE,
        test_samples = test_samples,
        regression_train_prop = regression_train_prop,
        dependant_period = dependant_period,
        assets_to_filter = c(c("AUD_USD", "NZD_USD"),
                             c("EUR_USD", "GBP_USD"),
                             c("EUR_JPY", "EUR_USD"),
                             c("GBP_USD", "EUR_GBP"),
                             c("AU200_AUD", "SPX500_USD"),
                             c("US2000_USD", "SPX500_USD"),
                             c("WTICO_USD", "BCO_USD"),
                             c("XAG_USD", "XAU_USD"),
                             c("USD_CAD", "USD_JPY"),
                             c("SG30_SGD", "SPX500_USD"),
                             c("NZD_CHF", "NZD_USD"),
                             c("SPX500_USD", "XAU_USD"),
                             c("NZD_CHF", "USD_CHF"),
                             c("EU50_EUR", "DE30_EUR"),
                             c("EU50_EUR", "SPX500_USD"),
                             c("DE30_EUR", "SPX500_USD"),
                             c("USD_SEK", "EUR_SEK"))
      ) %>%
      pluck('result')

    testing_data <- cor_data_list[[1]]
    testing_ramapped <-
      testing_data %>% dplyr::select(-c(Price, Open, High, Low))

    date_in_testing_data <-
      testing_ramapped %>%
      pull(Date) %>%
      max(na.rm = T) %>%
      pluck(1) %>%
      as.character()

    message(glue::glue("Max date in testing Data {date_in_testing_data}"))

    mean_values_by_asset_for_loop_15_bid =
      wrangle_asset_data(
        asset_data_daily_raw = starting_asset_data_bid_15M,
        summarise_means = TRUE
      )
    mean_values_by_asset_for_loop_15_ask =
      wrangle_asset_data(
        asset_data_daily_raw = starting_asset_data_ask_15M,
        summarise_means = TRUE
      )

    # get_cor_trade_results
    #safely_get_trades

    trade_results <-
      safely_get_trades_v2(
        testing_data = testing_ramapped,
        raw_asset_data = starting_asset_data_ask_15M,
        AUD_USD_sd = 0,
        NZD_USD_sd = 0,
        EUR_JPY_SD = 0,
        GBP_JPY_SD = 0,
        SPX_US2000_SD = 0,
        EUR_DE30_SD = 0,
        GBP_EUR_USD_SD = 0,
        EUR_GBP_USD_USD_SD = 0,
        NZD_CHF_USD_SD = 0,
        WTI_BCO_SD = 0,
        stop_factor = stop_factor,
        profit_factor = profit_factor,
        trade_direction = trade_direction,
        mean_values_by_asset_for_loop = mean_values_by_asset_for_loop_15_ask,
        currency_conversion = currency_conversion,
        asset_infor = asset_infor,
        risk_dollar_value = risk_dollar_value,
        return_analysis = FALSE
      ) %>%
      pluck('result')

    tagged_trades <- trade_results[[1]]

    date_in_tagged_trades <-
      tagged_trades %>%
      pull(Date) %>%
      max(na.rm = T) %>%
      pluck(1) %>%
      as.character()

    message(glue::glue("Max date in Tagged Trades {date_in_tagged_trades}"))

    if(dim(tagged_trades)[1] > 0) {

      temp_trades <- tagged_trades %>%
        ungroup() %>%
        slice_max(Date)

      if(dim(temp_trades)[1] > 0) {
        temp_trades2 <- temp_trades %>%
          filter(!is.na(trade_col), trade_col == trade_direction) %>%
          mutate(
            win_time_hours = win_time_hours,
            trade_direction = trade_direction,
            stop_factor = stop_factor,
            profit_factor = profit_factor
          )
      }

      if( dim(temp_trades2)[1] > 0) {
        all_trades_for_today[[k]] <- temp_trades2
      } else {
        all_trades_for_today[[k]] <- NULL
      }

    }

  }

  tictoc::toc()

  all_trades_for_today_dfr <- all_trades_for_today %>%
    keep(~ !is.null(.x)) %>%
    map_dfr(bind_rows) %>%
    dplyr::select(Date, Asset, trade_col, trade_direction, win_time_hours,
                  stop_factor, profit_factor)

  if(dim(all_trades_for_today_dfr)[1] > 0) {

    returned_data <-
      all_trades_for_today_dfr %>%
      ungroup() %>%
      left_join(starting_asset_data_ask_15M %>%
                  dplyr::select(Date, Asset, Price, Open, High, Low)) %>%
      left_join(current_analysis  ) %>%
      group_by(Asset) %>%
      slice_min(win_time_hours) %>%
      ungroup() %>%
      dplyr::group_by(Date, Asset,  Price, Low, High, Open, trade_col) %>%
      slice_max(profit_factor) %>%
      ungroup()

    stops_profs <- returned_data %>%
      distinct(Date, Asset, stop_factor, profit_factor, Price, Low, High, Open)

    stops_profs_distinct <- stops_profs %>% distinct(stop_factor, profit_factor)

    returned_data2 <- list()

    for (o in 1:dim(stops_profs_distinct)[1] ) {

      stop_factor <- stops_profs$stop_factor[o] %>% as.numeric()
      profit_factor <- stops_profs$profit_factor[o] %>% as.numeric()

      returned_data2[[o]] <- generic_trade_finder_loop(
        tagged_trades = returned_data ,
        asset_data_daily_raw = starting_asset_data_ask_15M,
        stop_factor = stop_factor,
        profit_factor =profit_factor,
        trade_col = "trade_col",
        date_col = "Date",
        start_price_col = "Price",
        mean_values_by_asset = mean_values_by_asset_for_loop_15_ask
      ) %>%
        rename(Date = dates, Asset = asset) %>%
        mutate(stop_factor = stop_factor,
               profit_factor = profit_factor) %>%
        left_join(
          stops_profs
        )

    }

    returned_data3 <-
      returned_data2 %>%
      map_dfr(bind_rows) %>%
      group_by(Asset) %>%
      slice_min(profit_factor) %>%
      ungroup()

  } else {
    returned_data3 <- NULL
  }


  return(returned_data3)

}

vector_based_AR <- function(v1 = cor_data_list$AUD_USD_NZD_USD_cor) {

}

#' get_rolling_correlation_estimates
#'
#' @param asset_data_to_use
#' @param samples_for_MLE
#' @param test_samples
#' @param assets_to_filter
#' @param rolling_period
#'
#' @return
#' @export
#'
#' @examples
get_rolling_correlation_estimates <-
  function(
    asset_data_to_use = starting_asset_data_bid_15,
    samples_for_MLE = 0.15,
    test_samples = 0.85,
    assets_to_filter = c(c("AUD_USD", "NZD_USD"),
                         c("EUR_USD", "GBP_USD"),
                         c("EUR_JPY", "EUR_USD"),
                         c("GBP_USD", "EUR_GBP"),
                         c("AU200_AUD", "SPX500_USD"),
                         c("US2000_USD", "SPX500_USD"),
                         c("WTICO_USD", "BCO_USD"),
                         c("XAG_USD", "XAU_USD"),
                         c("USD_CAD", "USD_JPY"),
                         c("SG30_SGD", "SPX500_USD"),
                         c("NZD_CHF", "NZD_USD"),
                         c("SPX500_USD", "XAU_USD"),
                         c("NZD_CHF", "USD_CHF"),
                         c("EU50_EUR", "DE30_EUR"),
                         c("EU50_EUR", "SPX500_USD"),
                         c("DE30_EUR", "SPX500_USD"),
                         c("USD_SEK", "EUR_SEK"),
                         c("EUR_JPY", "GBP_JPY")),
    rolling_period = 100
  ) {

    asset_joined_copulas <-
      get_correlation_data_set_v2(
        asset_data_to_use = asset_data_to_use,
        samples_for_MLE = samples_for_MLE,
        test_samples = test_samples,
        assets_to_filter = assets_to_filter,
        rolling_period = rolling_period
      )

    return(asset_joined_copulas)

  }


#' get_all_major_indices
#'
#' @param db_location
#' @param start_date
#' @param end_date
#' @param bid_or_ask
#'
#' @return
#' @export
#'
#' @examples
get_all_major_indices <- function(
    db_location = "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data For EDA.db",
    start_date = "2016-01-01",
    end_date = today() %>% as.character(),
    bid_or_ask = "ask",
    time_frame = "M15"
) {

  SPX500_USD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = bid_or_ask,
    time_frame = time_frame,
    asset = "SPX500_USD",
    keep_bid_to_ask = TRUE
  )

  US2000_USD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = bid_or_ask,
    time_frame = time_frame,
    asset = "US2000_USD",
    keep_bid_to_ask = TRUE
  )

  DE30_EUR <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = bid_or_ask,
    time_frame = time_frame,
    asset = "DE30_EUR",
    keep_bid_to_ask = TRUE
  )

  EU50_EUR <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = bid_or_ask,
    time_frame = time_frame,
    asset = "EU50_EUR",
    keep_bid_to_ask = TRUE
  )

  AU200_AUD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = bid_or_ask,
    time_frame = time_frame,
    asset = "AU200_AUD",
    keep_bid_to_ask = TRUE
  )

  NAS100_USD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = bid_or_ask,
    time_frame = time_frame,
    asset = "NAS100_USD",
    keep_bid_to_ask = TRUE
  )

  SG30_SGD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = bid_or_ask,
    time_frame = time_frame,
    asset = "SG30_SGD",
    keep_bid_to_ask = TRUE
  )

  all_dat <-
    SPX500_USD %>%
    bind_rows(US2000_USD) %>%
    bind_rows(SG30_SGD) %>%
    bind_rows(NAS100_USD) %>%
    bind_rows(AU200_AUD) %>%
    bind_rows(EU50_EUR) %>%
    bind_rows(DE30_EUR)%>%
    bind_rows(US2000_USD)

  return(all_dat)

}

#' create_log_cumulative_returns
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
create_log_cumulative_returns <- function(
    asset_data_to_use = SPX500_USD,
    asset_to_use = c("SPX500_USD"),
    price_col = "Open",
    return_long_format = FALSE
) {

  asset1 <- asset_data_to_use %>%
    filter(Asset == asset_to_use[1]) %>% distinct(Date, !!as.name(price_col)) %>%
    rename(
      !!as.name(asset_to_use[1]) := !!as.name(price_col)
    )

  combined_data <-
    asset1 %>%
    mutate(
      log1_price = !!as.name(asset_to_use[1]),
      log1_return = log(log1_price/lag(log1_price))
    ) %>%
    filter(!is.na(log1_return))

  combined_data <-
    combined_data %>%
    arrange(Date) %>%
    mutate(
      !!as.name(glue::glue("{asset_to_use[1]}_Return_Index")) :=
        cumsum(log1_return)
    )

  if(return_long_format == TRUE) {

    combined_data <-
      combined_data %>%
      arrange(Date) %>%
      dplyr::select(Date,
                    Return_Index = !!as.name(glue::glue("{asset_to_use[1]}_Return_Index"))
                    ) %>%
      mutate(
        Asset = asset_to_use[1]
      )

  } else {

    combined_data <-
      combined_data %>%
      dplyr::select(
        Date, !!as.name(glue::glue("{asset_to_use[1]}_Return_Index"))
      )
  }

  return(combined_data)

}

#' create_log_cumulative_returns
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
create_PCA_Asset_Index <- function(
    asset_data_to_use = major_indices_log_cumulative,
    asset_to_use = c("SPX500_USD", "US2000_USD", "NAS100_USD"),
    price_col = "Return_Index",
    scale_values = FALSE
) {

  pca_data <-
    asset_data_to_use %>%
    dplyr::select(Date, Asset, !!as.name(price_col)) %>%
    filter(Asset %in% asset_to_use) %>%
    rename(pivot_data = !!as.name(price_col)) %>%
    pivot_wider(names_from = Asset, values_from = pivot_data) %>%
    arrange(Date) %>%
    fill(everything(), .direction = "down") %>%
    filter(if_all(.cols = everything(), .fns = ~ !is.na(.)))

  pca_index_dat <- pca_data %>% dplyr::select(-Date)

  pca_calc <- prcomp(pca_index_dat, scale = scale_values)
  pca_calc1 <- pca_calc$x %>%
    as_tibble() %>%
    mutate(
      Average_PCA = (PC1 + PC2)/2
    )

  pca_calc1 %>%
    mutate(index = row_number()) %>%
    ggplot(aes(x = index)) +
    geom_line(aes(y = PC1)) +
    geom_line(aes(y = PC2), color = "darkred", linetype = "dashed") +
    geom_line(aes(y = PC3), color = "darkgreen", linetype = "dashed") +
    geom_line(aes(y = PC4), color = "darkorange", linetype = "dashed") +
    theme_minimal()

  if(length(asset_to_use) >= 6) {
    returned_data <-
      pca_data %>%
      dplyr::select(Date) %>%
      mutate(
        Average_PCA = pca_calc1$Average_PCA %>% as.numeric(),
        PC1 = pca_calc1$PC1 %>% as.numeric(),
        PC2 = pca_calc1$PC2 %>% as.numeric(),
        PC3 = pca_calc1$PC3 %>% as.numeric(),
        PC4 = pca_calc1$PC4 %>% as.numeric(),
        PC5 = pca_calc1$PC5 %>% as.numeric(),
        PC6 = pca_calc1$PC6 %>% as.numeric()
      )
  }

  if(length(asset_to_use) == 5) {
    returned_data <-
      pca_data %>%
      dplyr::select(Date) %>%
      mutate(
        Average_PCA = pca_calc1$Average_PCA %>% as.numeric(),
        PC1 = pca_calc1$PC1 %>% as.numeric(),
        PC2 = pca_calc1$PC2 %>% as.numeric(),
        PC3 = pca_calc1$PC3 %>% as.numeric(),
        PC4 = pca_calc1$PC4 %>% as.numeric(),
        PC5 = pca_calc1$PC5 %>% as.numeric()
      )
  }

  if(length(asset_to_use) == 4) {
    returned_data <-
      pca_data %>%
      dplyr::select(Date) %>%
      mutate(
        Average_PCA = pca_calc1$Average_PCA %>% as.numeric(),
        PC1 = pca_calc1$PC1 %>% as.numeric(),
        PC2 = pca_calc1$PC2 %>% as.numeric(),
        PC3 = pca_calc1$PC3 %>% as.numeric(),
        PC4 = pca_calc1$PC4 %>% as.numeric()
      )
  }

  if(length(asset_to_use) == 3) {
    returned_data <-
      pca_data %>%
      dplyr::select(Date) %>%
      mutate(
        Average_PCA = pca_calc1$Average_PCA %>% as.numeric(),
        PC1 = pca_calc1$PC1 %>% as.numeric(),
        PC2 = pca_calc1$PC2 %>% as.numeric(),
        PC3 = pca_calc1$PC3 %>% as.numeric()
      )
  }

  if(length(asset_to_use) == 2) {
    returned_data <-
      pca_data %>%
      dplyr::select(Date) %>%
      mutate(
        Average_PCA = pca_calc1$Average_PCA %>% as.numeric(),
        PC1 = pca_calc1$PC1 %>% as.numeric(),
        PC2 = pca_calc1$PC2 %>% as.numeric()
      )
  }

  return(returned_data)

}

#' prepare_PCA_wide_data_rolling
#'
#' This version of the function prepares a rolling PCA over equities
#'
#' @param asset_data_to_use
#' @param asset_to_use
#' @param price_col
#' @param min_sample_size
#' @param save_db_path
#'
#' @return
#' @export
#'
#' @examples
prepare_PCA_wide_data_rolling <-
  function(
    asset_data_to_use = major_indices_log_cumulative,
    asset_to_use = c("SPX500_USD", "US2000_USD", "NAS100_USD", "SG30_SGD", "AU200_AUD", "EU50_EUR", "DE30_EUR"),
    price_col = "Return_Index",
    min_sample_size = 1000,
    save_db_path = "C:/Users/Nikhil Chandra/Documents/trade_data/PCA_Rolling_equity_temp.db"
  ) {

    pca_data <-
      asset_data_to_use %>%
      dplyr::select(Date, Asset, !!as.name(price_col)) %>%
      filter(Asset %in% asset_to_use) %>%
      rename(pivot_data = !!as.name(price_col)) %>%
      pivot_wider(names_from = Asset, values_from = pivot_data) %>%
      arrange(Date) %>%
      fill(everything(), .direction = "down") %>%
      filter(if_all(.cols = everything(), .fns = ~ !is.na(.)))

    pca_index_dat <- pca_data %>% dplyr::select(-Date)

    PC1 <- numeric(length(min_sample_size:dim(pca_data)[1]))
    PC2 <- numeric(length(min_sample_size:dim(pca_data)[1]))
    PC3 <- numeric(length(min_sample_size:dim(pca_data)[1]))
    PC4 <- numeric(length(min_sample_size:dim(pca_data)[1]))
    PC5 <- numeric(length(min_sample_size:dim(pca_data)[1]))
    PC6 <- numeric(length(min_sample_size:dim(pca_data)[1]))
    c = 0

    for (i in min_sample_size:dim(pca_data)[1]) {

      c = c + 1
      pca_calc <- prcomp(pca_index_dat[1:i,])
      xx <- pca_calc$x
      PC1[c] <- xx[i,1] %>% as.numeric()
      PC2[c] <- xx[i,2] %>% as.numeric()
      PC3[c] <- xx[i,3] %>% as.numeric()
      PC4[c] <- xx[i,4] %>% as.numeric()
      PC5[c] <- xx[i,5] %>% as.numeric()
      PC6[c] <- xx[i,6] %>% as.numeric()

    }

    returned_data <-
      pca_data[min_sample_size:dim(pca_data)[1], ]

    returned_data <-
      returned_data %>%
      dplyr::select(Date) %>%
      mutate(
        Average_PCA = (PC1 + PC2)/2 %>% as.numeric(),
        PC1 = PC1 %>% as.numeric(),
        PC2 = PC2 %>% as.numeric(),
        PC3 = PC3 %>% as.numeric(),
        PC4 = PC4 %>% as.numeric(),
        PC5 = PC5 %>% as.numeric(),
        PC6 = PC6 %>% as.numeric()
      )

    if(!is.null(save_db_path)) {
      db_con <- connect_db(save_db_path)
      write_table_sql_lite(conn = db_con,
                           .data = returned_data,
                           table_name = "PCA_Rolling_equity_temp",
                           overwrite_true = TRUE)
      DBI::dbDisconnect(db_con)
    }

  }

#' get_PCA_Index_rolling_cor_sd_mean
#'
#' @param raw_asset_data_for_PCA_cor
#' @param PCA_data
#'
#' @return
#' @export
#'
#' @examples
get_PCA_Index_rolling_cor_sd_mean <-
  function(
    raw_asset_data_for_PCA_cor = asset_data_to_use %>% filter(Asset == "SPX500_USD"),
    PCA_data = returned_data,
    rolling_period = 100
    ) {

    returned_data_rolling_PCA_cor <-
      raw_asset_data_for_PCA_cor %>%
      left_join(PCA_data) %>%
      filter(!is.na(PC1)) %>%
      group_by(Asset) %>%
      mutate(

        tan_angle = lag(atan((Price - lag(Price, rolling_period))/rolling_period)),

        rolling_cor_PC1 = slider::slide2_dbl(.x = Return_Index,
                                             .y = PC1,
                                             .f = ~ cor(.x, .y),
                                             .before = rolling_period),
        rolling_cor_PC2 = slider::slide2_dbl(.x = Return_Index,
                                             .y = PC2,
                                             .f = ~ cor(.x, .y),
                                             .before = rolling_period),
        rolling_cor_PC3 = slider::slide2_dbl(.x = Return_Index,
                                             .y = PC3,
                                             .f = ~ cor(.x, .y),
                                             .before = rolling_period),
        rolling_cor_PC4 = slider::slide2_dbl(.x = Return_Index,
                                             .y = PC4,
                                             .f = ~ cor(.x, .y),
                                             .before = rolling_period),
        rolling_cor_PC5 = slider::slide2_dbl(.x = Return_Index,
                                             .y = PC5,
                                             .f = ~ cor(.x, .y),
                                             .before = rolling_period),

        rolling_cor_PC1_mean = slider::slide_dbl(.x = rolling_cor_PC1,
                                                 .f = ~ mean(.x, na.rm = T),
                                                 .before = rolling_period),

        rolling_cor_PC2_mean = slider::slide_dbl(.x = rolling_cor_PC2,
                                                 .f = ~ mean(.x, na.rm = T),
                                                 .before = rolling_period),

        rolling_cor_PC3_mean = slider::slide_dbl(.x = rolling_cor_PC3,
                                                 .f = ~ mean(.x, na.rm = T),
                                                 .before = rolling_period),

        rolling_cor_PC4_mean = slider::slide_dbl(.x = rolling_cor_PC4,
                                                 .f = ~ mean(.x, na.rm = T),
                                                 .before = rolling_period),

        rolling_cor_PC5_mean = slider::slide_dbl(.x = rolling_cor_PC5,
                                                 .f = ~ mean(.x, na.rm = T),
                                                 .before = rolling_period),

        rolling_cor_PC1_sd = slider::slide_dbl(.x = rolling_cor_PC1,
                                               .f = ~ sd(.x, na.rm = T),
                                               .before = rolling_period),

        rolling_cor_PC2_sd = slider::slide_dbl(.x = rolling_cor_PC2,
                                               .f = ~ sd(.x, na.rm = T),
                                               .before = rolling_period),

        rolling_cor_PC3_sd = slider::slide_dbl(.x = rolling_cor_PC3,
                                               .f = ~ sd(.x, na.rm = T),
                                               .before = rolling_period),

        rolling_cor_PC4_sd = slider::slide_dbl(.x = rolling_cor_PC4,
                                               .f = ~ sd(.x, na.rm = T),
                                               .before = rolling_period),

        rolling_cor_PC5_sd = slider::slide_dbl(.x = rolling_cor_PC5,
                                               .f = ~ sd(.x, na.rm = T),
                                               .before = rolling_period),

        rolling_tan_angle_mean = slider::slide_dbl(.x = tan_angle,
                                              .f = ~  mean(.x, na.rm = T),
                                              .before = rolling_period),

        rolling_tan_angle_sd = slider::slide_dbl(.x = tan_angle,
                                                   .f = ~  sd(.x, na.rm = T),
                                                   .before = rolling_period)
      )

    return(returned_data_rolling_PCA_cor)

  }


#' rolling_cauchy
#'
#' @param .vec
#' @param summarise_func
#'
#' @return
#' @export
#'
#' @examples
rolling_cauchy <-
  function(.vec,
           summarise_func = "max") {

    mean_var <- mean(.vec, na.rm = T)
    sd_var <- sd(.vec, na.rm = T)
    mle_training_cdf_1 <-pnorm(.vec, mean = mean_var, sd  = sd_var)
    mle_training_cdf_2 <-pcauchy(.vec, location  = mean_var, scale =  sd_var)


    if(summarise_func == "max") {
      mle_training_cdf_1 <- mle_training_cdf_1[length(mle_training_cdf_1)] %>% as.numeric()
      mle_training_cdf_2 <- mle_training_cdf_2[length(mle_training_cdf_2)] %>% as.numeric()
      return_value <- (mle_training_cdf_1+mle_training_cdf_2)/2
    }

    if(summarise_func == "mean") {
      mle_training_cdf_1 <- mean(mle_training_cdf_1, na.rm = T)
      mle_training_cdf_2 <- mean(mle_training_cdf_2, na.rm = T)
      return_value <- (mle_training_cdf_1+mle_training_cdf_2)/2
    }

    return(return_value)

  }


#' get_all_commod_USD
#'
#' @param db_location
#' @param start_date
#' @param end_date
#' @param bid_or_ask
#'
#' @return
#' @export
#'
#' @examples
get_all_commod_USD <- function(
    db_location = "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data For EDA.db",
    start_date = "2016-01-01",
    end_date = today() %>% as.character(),
    bid_or_ask = "ask",
    time_frame = "M15"
) {

  BCO_USD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = bid_or_ask,
    time_frame = time_frame,
    asset = "BCO_USD",
    keep_bid_to_ask = TRUE
  )

  WTICO_USD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = bid_or_ask,
    time_frame = time_frame,
    asset = "WTICO_USD",
    keep_bid_to_ask = TRUE
  )

  XAG_USD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = bid_or_ask,
    time_frame = time_frame,
    asset = "XAG_USD",
    keep_bid_to_ask = TRUE
  )

  XCU_USD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = bid_or_ask,
    time_frame = time_frame,
    asset = "XCU_USD",
    keep_bid_to_ask = TRUE
  )

  XAU_USD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = bid_or_ask,
    time_frame = time_frame,
    asset = "XAU_USD",
    keep_bid_to_ask = TRUE
  )

  WHEAT_USD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = bid_or_ask,
    time_frame = time_frame,
    asset = "WHEAT_USD",
    keep_bid_to_ask = TRUE
  )

  SOYBN_USD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = bid_or_ask,
    time_frame = time_frame,
    asset = "SOYBN_USD",
    keep_bid_to_ask = TRUE
  )

  NATGAS_USD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = bid_or_ask,
    time_frame = time_frame,
    asset = "NATGAS_USD",
    keep_bid_to_ask = TRUE
  )

  SUGAR_USD <- create_asset_high_freq_data(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = bid_or_ask,
    time_frame = time_frame,
    asset = "SUGAR_USD",
    keep_bid_to_ask = TRUE
  )

  all_dat <-
    BCO_USD %>%
    bind_rows(WTICO_USD) %>%
    bind_rows(XAG_USD) %>%
    bind_rows(XCU_USD) %>%
    bind_rows(XAU_USD) %>%
    bind_rows(WHEAT_USD) %>%
    bind_rows(SOYBN_USD)%>%
    bind_rows(NATGAS_USD)%>%
    bind_rows(SUGAR_USD)

  return(all_dat)

}
