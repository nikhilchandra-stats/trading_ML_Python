helpeR::load_custom_functions()

all_aud_symbols <- get_oanda_symbols() %>%
  keep(~ str_detect(.x, "AUD")|str_detect(.x, "USD_SEK|USD_NOK|USD_HUF|USD_ZAR|USD_CNY|USD_MXN"))
asset_infor <- get_instrument_info()
aud_assets <- read_all_asset_data_intra_day(
  asset_list_oanda = all_aud_symbols,
  save_path_oanda_assets = "C:/Users/Nikhil Chandra/Documents/Asset Data/oanda_data/",
  read_csv_or_API = "API",
  time_frame = "D",
  bid_or_ask = "bid",
  how_far_back = 10,
  start_date = (today() - days(2)) %>% as.character()
)
aud_assets <- aud_assets %>% map_dfr(bind_rows)
aud_usd_today <- get_aud_conversion(asset_data_daily_raw = aud_assets)

currency_conversion <-
  aud_usd_today %>%
  mutate(
    not_aud_asset = ending_value
  ) %>%
  dplyr::select(not_aud_asset, adjusted_conversion) %>%
  bind_rows(
    tibble(not_aud_asset = "AUD", adjusted_conversion = 1)
  )

asset_list_oanda <- get_oanda_symbols() %>%
  keep( ~ .x %in% c("HK33_HKD", "USD_JPY","SPX500_USD",
                    "AUD_NZD", "GBP_CHF",
                    "EUR_HUF", "EUR_ZAR", "NZD_JPY", "EUR_NZD",
                    "XAU_CAD", "GBP_JPY", "EUR_NOK", "USD_SGD", "EUR_SEK",
                    "DE30_EUR",
                    "AUD_CAD",
                    "XPD_USD",
                    "UK100_GBP",
                    "USD_CHF", "GBP_NZD",
                    "GBP_SGD", "USD_SEK", "EUR_SGD", "XCU_USD", "SUGAR_USD", "CHF_ZAR",
                    "AUD_CHF", "EUR_CHF", "USD_MXN", "GBP_USD", "WTICO_USD", "EUR_JPY", "USD_NOK",
                    "XAU_USD",
                    "USD_CZK", "AUD_SGD", "USD_HUF", "WHEAT_USD",
                    "EUR_USD", "SG30_SGD", "GBP_AUD", "NZD_CAD", "AU200_AUD", "XAG_USD",
                    "XAU_EUR", "EUR_GBP", "USD_CNH", "USD_CAD", "NAS100_USD",
                    "EU50_EUR", "NATGAS_USD", "CAD_JPY", "FR40_EUR", "USD_ZAR", "XAU_GBP",
                    "EUR_AUD", "SOYBN_USD",
                    "US2000_USD",
                    "BCO_USD", "AUD_USD", "NZD_USD", "NZD_CHF", "WHEAT_USD", "AUD_JPY", "AUD_SEK")
  )

asset_infor <- get_instrument_info()
#---------------------Data
db_location <- "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data.db"
starting_asset_data_ask_15 <-
  get_db_price(
    db_location = db_location,
    start_date = "2023-01-01",
    end_date = today() %>% as.character(),
    bid_or_ask = "ask",
    time_frame = "M15"
  )
starting_asset_data_bid_15 <-
  get_db_price(
    db_location = db_location,
    start_date = "2023-01-01",
    end_date = today() %>% as.character(),
    bid_or_ask = "bid",
    time_frame = "M15"
  )
mean_values_by_asset_for_loop_15_bid =
  wrangle_asset_data(
    asset_data_daily_raw = starting_asset_data_bid_15,
    summarise_means = TRUE
  )

#------------------------------------------
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



get_correlation_data_set <- function(
    asset_data_to_use = starting_asset_data_bid_15,
    regression_train_prop = 0.5,
    dependant_period = 10,
    assets_to_filter = c(c("AUD_USD", "NZD_USD"),
                          c("EUR_USD", "GBP_USD"),
                          c("EUR_JPY", "EUR_USD"),
                          c("GBP_USD", "EUR_GBP"),
                         c("AU200_AUD", "SPX500_USD"),
                         c("US2000_USD", "SPX500_USD")
                         )
) {

  AUD_NZD_USD <-
    estimating_dual_copula(
      asset_data_to_use = asset_data_to_use,
      asset_to_use = c("AUD_USD", "NZD_USD"),
      price_col = "Open",
      rolling_period = 100,
      samples_for_MLE = 0.5,
      test_samples = 0.4
    )

  AU_200_SPX <-
    estimating_dual_copula(
      asset_data_to_use = asset_data_to_use,
      asset_to_use = c("AU200_AUD", "SPX500_USD"),
      price_col = "Open",
      rolling_period = 100,
      samples_for_MLE = 0.5,
      test_samples = 0.4
    )

  SPX_US200 <-
    estimating_dual_copula(
      asset_data_to_use = asset_data_to_use,
      asset_to_use = c("US2000_USD", "SPX500_USD"),
      price_col = "Open",
      rolling_period = 100,
      samples_for_MLE = 0.5,
      test_samples = 0.4
    ) %>%
    dplyr::select(Date, contains("US2000"), US2000_USD_SPX500_USD_cor)

  EUR_USD_GBP <-
    estimating_dual_copula(
      asset_data_to_use = asset_data_to_use,
      asset_to_use = c("EUR_USD", "GBP_USD"),
      price_col = "Open",
      rolling_period = 100,
      samples_for_MLE = 0.5,
      test_samples = 0.4
    )

  EUR_USD_JPY <-
    estimating_dual_copula(
      asset_data_to_use = asset_data_to_use,
      asset_to_use = c("EUR_JPY", "EUR_USD"),
      price_col = "Open",
      rolling_period = 100,
      samples_for_MLE = 0.5,
      test_samples = 0.4
    ) %>%
    dplyr::select(Date, contains("EUR_JPY"), EUR_JPY_EUR_USD_cor)

  EUR_GBP <-
    estimating_dual_copula(
      asset_data_to_use = asset_data_to_use,
      asset_to_use = c("GBP_USD", "EUR_GBP"),
      price_col = "Open",
      rolling_period = 100,
      samples_for_MLE = 0.5,
      test_samples = 0.4
    ) %>%
    dplyr::select(Date, contains("EUR_GBP"), GBP_USD_EUR_GBP_cor)

  EUR_JPY_GBP <-
    estimating_dual_copula(
      asset_data_to_use = asset_data_to_use,
      asset_to_use = c("EUR_JPY", "GBP_JPY"),
      price_col = "Open",
      rolling_period = 100,
      samples_for_MLE = 0.5,
      test_samples = 0.4
    ) %>%
    dplyr::select(Date, EUR_JPY_GBP_JPY_cor)


  asset_joined_copulas <-
    asset_data_to_use %>%
    left_join(AUD_NZD_USD) %>%
    left_join(EUR_USD_GBP) %>%
    left_join(EUR_USD_JPY) %>%
    left_join(EUR_GBP) %>%
    left_join(SPX_US200) %>%
    left_join(AU_200_SPX) %>%
    left_join(EUR_JPY_GBP) %>%
    filter(if_all(contains("_cor"), ~ !is.na(.))) %>%
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

cor_data_list <-
  get_correlation_data_set(
    asset_data_to_use = starting_asset_data_bid_15,
    regression_train_prop = 0.5,
    dependant_period = 10,
    assets_to_filter = c(c("AUD_USD", "NZD_USD"),
                         c("EUR_USD", "GBP_USD"),
                         c("EUR_JPY", "EUR_USD"),
                         c("GBP_USD", "EUR_GBP"),
                         c("AU200_AUD", "SPX500_USD"),
                         c("US2000_USD", "SPX500_USD")
    )
  )

testing_data <- cor_data_list[[1]]

get_cor_trade_results <-
  function(
    testing_data = testing_data,
    raw_asset_data = starting_asset_data_ask_15,
    sd_fac1 = 1,
    stop_factor = 17,
    profit_factor = 27,
    trade_direction = "Long",
    mean_values_by_asset_for_loop = mean_values_by_asset_for_loop_15_ask,
    currency_conversion = currency_conversion,
    asset_infor = asset_infor,
    risk_dollar_value = 10
    ) {

    tagged_trades <-
      testing_data %>%
      mutate(
        trade_col =
          case_when(
            pred >= mean_pred + sd_pred*sd_fac1 ~ trade_direction
          )
      ) %>%
      filter(!is.na(trade_col))

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
        sd_fac1 = sd_fac1
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
        sd_fac1 = sd_fac1
      ) %>%
      bind_cols(trade_timings_by_asset_neg)

  }

