helperfunctions35South::load_custom_functions()
one_drive_path <- helperfunctions35South::create_one_drive_path(
  path_extension = "raw data")
library(neuralnet)
all_aud_symbols <- get_oanda_symbols() %>%
  keep(~ str_detect(.x, "AUD")|str_detect(.x, "USD_SEK|USD_NOK|USD_HUF|USD_ZAR|USD_CNY|USD_MXN|USD_CNH"))
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


db_location = "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data For EDA.db"
start_date = "2011-01-01"
end_date = today() %>% as.character()
time_frame = "H1"

major_indices <-
  get_all_major_indices(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame
  )

major_indices$Asset %>% unique()
gc()

major_indices_log_cumulative <-
  c("SPX500_USD", "US2000_USD", "NAS100_USD", "SG30_SGD", "AU200_AUD", "EU50_EUR", "DE30_EUR") %>%
  map_dfr(
    ~
      create_log_cumulative_returns(
        asset_data_to_use = major_indices,
        asset_to_use = c(.x[1]),
        price_col = "Open",
        return_long_format = TRUE
      )
  ) %>%
  left_join(
    major_indices %>% distinct(Date, Asset, Price, Open, High, Low)
  )

create_Index_PCA_copula <-
  function(
    major_indices_log_cumulative = major_indices_log_cumulative,
    assets_to_use = c("SPX500_USD", "US2000_USD", "NAS100_USD", "SG30_SGD", "AU200_AUD", "EU50_EUR", "DE30_EUR"),
    samples_for_MLE = 0.75,
    test_samples = 0.25,
    rolling_period = 100
    ) {

    major_indices_log_cumulative <-
      major_indices_log_cumulative %>%
      group_by(Asset) %>%
      mutate(
        Return_Index_Diff = ((Price - Open)/Open)*100
      ) %>%
      ungroup() %>%
      filter(!is.na(Return_Index_Diff))

    major_indices_PCA_Index <-
      create_PCA_Asset_Index(
        asset_data_to_use = major_indices_log_cumulative,
        asset_to_use = assets_to_use,
        price_col = "Return_Index"
      ) %>%
      rename(
        Average_PCA_Index = Average_PCA
      )

    major_indices_cumulative_pca <-
      major_indices_log_cumulative %>%
      left_join(major_indices_PCA_Index) %>%
      filter(!is.na(Average_PCA_Index)) %>%
      group_by(Asset) %>%
      mutate(
        Average_PCA_Returns =
          ((Average_PCA_Index - lag(Average_PCA_Index))/lag(Average_PCA_Index))*100
      ) %>%
      ungroup() %>%
      filter(!is.na(Average_PCA_Returns))

    SPX500_USD_Index_copula_retun <-
      cauchy_dual_copula_generic(
        asset_data_to_use = major_indices_cumulative_pca %>% filter(Asset == "SPX500_USD"),
        cols_to_use = c("Return_Index_Diff", "Average_PCA_Returns"),
        rolling_period = 100,
        samples_for_MLE = samples_for_MLE,
        test_samples = test_samples
      ) %>%
      dplyr::select(Date, SPX_FX1_DIFF_Return = FX1,
                    SPX_FX2_INDEX_DIFF = FX2,
                    SPX_joint_density_copula_INDEX_PCA_DIFF = joint_density_copula,
                    SPX_joint_density_INDEX_PCA_DIFF = joint_density)

    SPX500_USD_Index_copula_Index <-
      cauchy_dual_copula_generic(
        asset_data_to_use = major_indices_cumulative_pca %>% filter(Asset == "SPX500_USD"),
        cols_to_use = c("Return_Index", "Average_PCA_Index"),
        rolling_period = 100,
        samples_for_MLE = samples_for_MLE,
        test_samples = test_samples
      ) %>%
      dplyr::select(Date,
                    SPX_FX1_Index = FX1,
                    SPX_FX2_PCA_Index = FX2,
                    SPX_joint_density_copula_INDEX_PCA = joint_density_copula,
                    SPX_joint_density_INDEX_PCA = joint_density)

    AU200_AUD_Index_copula_returns <-
      cauchy_dual_copula_generic(
        asset_data_to_use = major_indices_cumulative_pca %>% filter(Asset == "AU200_AUD"),
        cols_to_use = c("Return_Index_Diff", "Average_PCA_Returns"),
        rolling_period = 100,
        samples_for_MLE = samples_for_MLE,
        test_samples = test_samples
      ) %>%
      dplyr::select(Date,
                    AU200_FX1_DIFF_Return = FX1,
                    AU200_FX2_INDEX_DIFF = FX2,
                    AU200_joint_density_copula_INDEX_PCA_DIFF = joint_density_copula,
                    AU200_joint_density_INDEX_PCA_DIFF = joint_density)

    AU200_AUD_Index_copula_Index <-
      cauchy_dual_copula_generic(
        asset_data_to_use = major_indices_cumulative_pca %>% filter(Asset == "AU200_AUD"),
        cols_to_use = c("Return_Index", "Average_PCA_Index"),
        rolling_period = 100,
        samples_for_MLE = samples_for_MLE,
        test_samples = test_samples
      ) %>%
      dplyr::select(Date,
                    AU200_FX1_Index = FX1,
                    AU200_FX2_PCA_Index = FX2,
                    AU200_joint_density_copula_INDEX_PCA = joint_density_copula,
                    AU200_joint_density_INDEX_PCA = joint_density)


    US2000_USD_Index_copula_returns <-
      cauchy_dual_copula_generic(
        asset_data_to_use = major_indices_cumulative_pca %>% filter(Asset == "US2000_USD"),
        cols_to_use = c("Return_Index_Diff", "Average_PCA_Returns"),
        rolling_period = 100,
        samples_for_MLE = samples_for_MLE,
        test_samples = test_samples
      ) %>%
      dplyr::select(Date,
                    US2000_FX1_DIFF_Return = FX1,
                    US2000_FX2_INDEX_DIFF = FX2,
                    US2000_joint_density_copula_INDEX_PCA_DIFF = joint_density_copula,
                    US2000_joint_density_INDEX_PCA_DIFF = joint_density)

    US2000_USD_Index_copula_Index <-
      cauchy_dual_copula_generic(
        asset_data_to_use = major_indices_cumulative_pca %>% filter(Asset == "US2000_USD"),
        cols_to_use = c("Return_Index", "Average_PCA_Index"),
        rolling_period = 100,
        samples_for_MLE = samples_for_MLE,
        test_samples = test_samples
      ) %>%
      dplyr::select(Date,
                    US2000_FX1_Index = FX1,
                    US2000_FX2_PCA_Index = FX2,
                    US2000_joint_density_copula_INDEX_PCA = joint_density_copula,
                    US2000_joint_density_INDEX_PCA = joint_density)

    full_PCA_Copula_Data <-
      major_indices_cumulative_pca %>%
      left_join(SPX500_USD_Index_copula_retun)%>%
      left_join(SPX500_USD_Index_copula_Index)%>%
      left_join(AU200_AUD_Index_copula_returns)%>%
      left_join(AU200_AUD_Index_copula_Index) %>%
      left_join(US2000_USD_Index_copula_returns)%>%
      left_join(US2000_USD_Index_copula_Index)

    full_PCA_Copula_Data2 <-
      full_PCA_Copula_Data %>%
      filter(!is.na(US2000_joint_density_INDEX_PCA)) %>%
      mutate(
        across(-c(Date, Price, Asset, Open, High, Low), .fns = ~ lag(.))
      ) %>%
      filter(!is.na(US2000_joint_density_INDEX_PCA)) %>%
      mutate(
        US2000_joint_density_INDEX_PCA_mean =
          slider::slide_dbl(.x = US2000_joint_density_INDEX_PCA, .f = ~ mean(.x, na.rm = T), .before = rolling_period),
        US2000_joint_density_INDEX_PCA_DIFF_mean =
          slider::slide_dbl(.x = US2000_joint_density_INDEX_PCA_DIFF, .f = ~ mean(.x, na.rm = T), .before = rolling_period),
        AU200_joint_density_INDEX_PCA_mean  =
          slider::slide_dbl(.x = AU200_joint_density_INDEX_PCA, .f = ~ mean(.x, na.rm = T), .before = rolling_period),
        AU200_joint_density_INDEX_PCA_DIFF_mean =
          slider::slide_dbl(.x = AU200_joint_density_INDEX_PCA_DIFF, .f = ~ mean(.x, na.rm = T), .before = rolling_period),
        SPX_joint_density_INDEX_PCA_mean =
          slider::slide_dbl(.x = SPX_joint_density_INDEX_PCA, .f = ~ mean(.x, na.rm = T), .before = rolling_period),
        SPX_joint_density_INDEX_PCA_DIFF_mean =
          slider::slide_dbl(.x = SPX_joint_density_INDEX_PCA_DIFF, .f = ~ mean(.x, na.rm = T), .before = rolling_period),

        US2000_joint_density_INDEX_PCA_sd =
          slider::slide_dbl(.x = US2000_joint_density_INDEX_PCA, .f = ~ sd(.x, na.rm = T), .before = rolling_period),
        US2000_joint_density_INDEX_PCA_DIFF_sd =
          slider::slide_dbl(.x = US2000_joint_density_INDEX_PCA_DIFF, .f = ~ sd(.x, na.rm = T), .before = rolling_period),
        AU200_joint_density_INDEX_PCA_sd  =
          slider::slide_dbl(.x = AU200_joint_density_INDEX_PCA, .f = ~ sd(.x, na.rm = T), .before = rolling_period),
        AU200_joint_density_INDEX_PCA_DIFF_sd =
          slider::slide_dbl(.x = AU200_joint_density_INDEX_PCA_DIFF, .f = ~ sd(.x, na.rm = T), .before = rolling_period),
        SPX_joint_density_INDEX_PCA_sd =
          slider::slide_dbl(.x = SPX_joint_density_INDEX_PCA, .f = ~ sd(.x, na.rm = T), .before = rolling_period),
        SPX_joint_density_INDEX_PCA_DIFF_sd =
          slider::slide_dbl(.x = SPX_joint_density_INDEX_PCA_DIFF, .f = ~ sd(.x, na.rm = T), .before = rolling_period)
      ) %>%
      mutate(
        Price_Index_minus_PCA_Index = Average_PCA_Index - Return_Index
      )

    return(major_indices_cumulative_pca)

  }




#' get_all_AUD_USD_specific_data
#'
#' @param db_location
#' @param start_date
#' @param end_date
#'
#' @return
#' @export
#'
#' @examples
get_all_AUD_USD_specific_data <-
  function(
    db_location = "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data For EDA.db",
    start_date = "2016-01-01",
    end_date = today() %>% as.character()
  ) {

    AUD_USD <- create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "ask",
      time_frame = "M15",
      asset = "AUD_USD",
      keep_bid_to_ask = TRUE
    )

    NZD_USD <- create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "ask",
      time_frame = "M15",
      asset = "NZD_USD",
      keep_bid_to_ask = TRUE
    )

    XAG_USD <- create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "ask",
      time_frame = "M15",
      asset = "XAG_USD",
      keep_bid_to_ask = TRUE
    )

    XAU_USD <- create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "ask",
      time_frame = "M15",
      asset = "XAU_USD",
      keep_bid_to_ask = TRUE
    )

    AUD_USD_NZD_USD <- AUD_USD %>% bind_rows(NZD_USD) %>% bind_rows(XAG_USD)
    rm(AUD_USD, NZD_USD, XAG_USD)
    gc()
    mean_values_by_asset_for_loop_15_ask <- wrangle_asset_data(AUD_USD_NZD_USD, summarise_means = TRUE)

    AUD_USD <- create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "bid",
      time_frame = "M15",
      asset = "AUD_USD",
      keep_bid_to_ask = TRUE
    )

    NZD_USD <- create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "bid",
      time_frame = "M15",
      asset = "NZD_USD",
      keep_bid_to_ask = TRUE
    )

    XAG_USD <- create_asset_high_freq_data(
      db_location = db_location,
      start_date = start_date,
      end_date = end_date,
      bid_or_ask = "bid",
      time_frame = "M15",
      asset = "XAG_USD",
      keep_bid_to_ask = TRUE
    )


    AUD_USD_NZD_USD_short <- AUD_USD %>% bind_rows(NZD_USD) %>% bind_rows(XAG_USD)
    rm(AUD_USD, NZD_USD)
    gc()

    return(list(AUD_USD_NZD_USD, AUD_USD_NZD_USD_short))
  }


#' get_AUD_USD_NZD_Specific_Trades
#'
#' @param AUD_USD
#' @param db_location
#' @param start_date
#' @param raw_macro_data
#' @param lag_days
#' @param lm_period
#' @param lm_train_prop
#' @param lm_test_prop
#' @param sd_fac_lm_trade
#' @param trade_direction
#'
#' @return
#' @export
#'
#' @examples
get_AUD_USD_NZD_Specific_Trades <-
  function(
    AUD_USD_NZD_USD = AUD_USD_NZD_USD,
    start_date = "2016-01-01",
    raw_macro_data = raw_macro_data,
    lag_days = 4,
    lm_period = 80,
    lm_train_prop = 0.25,
    lm_test_prop = 0.75,
    sd_fac_lm_trade = 1,
    sd_fac_lm_trade2 = 4,
    sd_fac_lm_trade3 = 1,
    trade_direction = "Long",
    stop_factor = 5,
    profit_factor = 10,
    assets_to_return = c("AUD_USD", "NZD_USD")
  ) {

    aus_macro_data <-
      get_AUS_Indicators(raw_macro_data,
                         lag_days = lag_days)
    nzd_macro_data <-
      get_NZD_Indicators(raw_macro_data,
                         lag_days = lag_days)
    usd_macro_data <-
      get_USD_Indicators(raw_macro_data,
                         lag_days = lag_days)

    cny_macro_data <-
      get_CNY_Indicators(raw_macro_data,
                         lag_days = lag_days)

    aud_macro_vars <- names(aus_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    nzd_macro_vars <- names(nzd_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    usd_macro_vars <- names(usd_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    cny_macro_vars <- names(cny_macro_data) %>% keep(~ .x != "date") %>% unlist() %>% as.character()
    all_macro_vars <- c(aud_macro_vars, nzd_macro_vars, usd_macro_vars, cny_macro_vars)

    copula_data <-
      estimating_dual_copula(
        asset_data_to_use = AUD_USD_NZD_USD,
        asset_to_use = c("AUD_USD", "NZD_USD"),
        price_col = "Open",
        rolling_period = 100,
        samples_for_MLE = 0.15,
        test_samples = 0.85
      )

    copula_data_macro <-
      copula_data %>%
      mutate(Date_for_join = as_date(Date)) %>%
      left_join(
        aus_macro_data %>%
          rename(Date_for_join = date)
      ) %>%
      left_join(
        nzd_macro_data %>%
          rename(Date_for_join = date)
      ) %>%
      left_join(
        usd_macro_data %>%
          rename(Date_for_join = date)
      ) %>%
      left_join(
        cny_macro_data %>%
          rename(Date_for_join = date)
      ) %>%
      fill(!contains("AUD_USD|Date"), .direction = "down") %>%
      filter(if_all(everything() ,.fns = ~ !is.na(.))) %>%
      mutate(
        dependant_var_aud_usd = log(lead(AUD_USD, lm_period)/AUD_USD),
        dependant_var_nzd_usd = log(lead(NZD_USD, lm_period)/NZD_USD)
      )

    max_data_in_copula <- copula_data_macro %>%
      pull(Date) %>%
      max(na.rm = T) %>%
      as.character()

    message(glue::glue("Max Date in Copula Data AUD NZD: {max_data_in_copula}"))

    lm_quant_vars <- names(copula_data_macro) %>% keep(~ str_detect(.x,"quantiles|tangent|cor"))
    lm_vars1 <- c(all_macro_vars, lm_quant_vars)

    training_data <- copula_data_macro %>%
      slice_head(prop = lm_train_prop)
    testing_data <- copula_data_macro %>%
      slice_tail(prop = lm_test_prop)

    max_data_in_testing_data <- testing_data %>%
      pull(Date) %>%
      max(na.rm = T) %>%
      as.character()

    message(glue::glue("Max Date in Testing Data AUD NZD: {max_data_in_testing_data}"))

    lm_formula_AUD_USD <- create_lm_formula(dependant = "dependant_var_aud_usd", independant = lm_vars1)
    lm_formula_AUD_USD_quant <- create_lm_formula(dependant = "dependant_var_aud_usd", independant = lm_quant_vars)
    lm_model_AUD_USD <- lm(formula = lm_formula_AUD_USD, data = training_data)
    lm_model_AUD_USD_quant <- lm(formula = lm_formula_AUD_USD_quant, data = training_data)

    predicted_train_AUD_USD <- predict.lm(lm_model_AUD_USD, newdata = training_data)
    mean_pred_AUD_USD <- mean(predicted_train_AUD_USD, na.rm = T)
    sd_pred_AUD_USD <- sd(predicted_train_AUD_USD, na.rm = T)
    predicted_test_AUD_USD <- predict.lm(lm_model_AUD_USD, newdata = testing_data) %>% as.numeric()
    mean_pred_test_AUD_USD <- mean(predicted_test_AUD_USD, na.rm = T)
    sd_pred_test_AUD_USD <- sd(predicted_test_AUD_USD, na.rm = T)

    predicted_train_AUD_USD_quant <- predict.lm(lm_model_AUD_USD_quant, newdata = training_data)
    mean_pred_AUD_USD_quant <- mean(predicted_train_AUD_USD_quant, na.rm = T)
    sd_pred_AUD_USD_quant <- sd(predicted_train_AUD_USD_quant, na.rm = T)
    predicted_test_AUD_USD_quant <- predict.lm(lm_model_AUD_USD_quant, newdata = testing_data) %>% as.numeric()
    mean_pred_test_AUD_USD_quant <- mean(predicted_test_AUD_USD_quant, na.rm = T)
    sd_pred_test_AUD_USD_quant <- sd(predicted_test_AUD_USD_quant, na.rm = T)

    tagged_trades_AUD_USD <-
      testing_data %>%
      mutate(
        lm_pred_AUD_USD = predicted_test_AUD_USD,
        lm_pred_AUD_USD_quant = predicted_test_AUD_USD_quant
      ) %>%
      mutate(
        trade_col =
          case_when(

            # lm_pred_AUD_USD >= mean_pred_AUD_USD + sd_fac_lm_trade*sd_pred_AUD_USD &
            #   trade_direction == "Long" ~trade_direction,
            # lm_pred_AUD_USD <= mean_pred_AUD_USD - sd_fac_lm_trade*sd_pred_AUD_USD &
            #   trade_direction == "Short" ~ trade_direction,

            # lm_pred_AUD_USD_quant >= mean_pred_AUD_USD_quant + sd_fac_lm_trade*sd_pred_AUD_USD_quant &
            #   trade_direction == "Long" ~trade_direction,
            # lm_pred_AUD_USD_quant <= mean_pred_AUD_USD_quant - sd_fac_lm_trade*sd_pred_AUD_USD_quant &
            #   trade_direction == "Short" ~ trade_direction,


            AUD_USD_NZD_USD_quant_lm <= AUD_USD_NZD_USD_quant_lm_mean - AUD_USD_NZD_USD_quant_lm_sd*sd_fac_lm_trade2 &
              trade_direction == "Short" &  AUD_USD_tangent_angle1 < 0 & NZD_USD_tangent_angle2 > 0~trade_direction,

            AUD_USD_NZD_USD_quant_lm <= AUD_USD_NZD_USD_quant_lm_mean - AUD_USD_NZD_USD_quant_lm_sd*sd_fac_lm_trade2 &
              trade_direction == "Long" &  AUD_USD_tangent_angle1 > 0 & NZD_USD_tangent_angle2 < 0~trade_direction,

            AUD_USD_NZD_USD_quant_lm >= AUD_USD_NZD_USD_quant_lm_mean + AUD_USD_NZD_USD_quant_lm_sd*sd_fac_lm_trade2 &
              trade_direction == "Short" &  AUD_USD_tangent_angle1 < 0 & NZD_USD_tangent_angle2 > 0~trade_direction,

            AUD_USD_NZD_USD_quant_lm >= AUD_USD_NZD_USD_quant_lm_mean + AUD_USD_NZD_USD_quant_lm_sd*sd_fac_lm_trade2 &
              trade_direction == "Long" &  AUD_USD_tangent_angle1 > 0 & NZD_USD_tangent_angle2 < 0~trade_direction



            # AUD_USD_NZD_USD_cor <= AUD_USD_NZD_USD_cor_mean - AUD_USD_NZD_USD_cor_sd*sd_fac_lm_trade3 &
            #   trade_direction == "Long" &  AUD_USD_tangent_angle1 < 0 & NZD_USD_tangent_angle2 > 0~trade_direction,
            #
            # AUD_USD_NZD_USD_cor <= AUD_USD_NZD_USD_cor_mean - AUD_USD_NZD_USD_cor_sd*sd_fac_lm_trade3 &
            #   trade_direction == "Short" &  AUD_USD_tangent_angle1 > 0 & NZD_USD_tangent_angle2 < 0~trade_direction,
            #
            # AUD_USD_NZD_USD_cor >= AUD_USD_NZD_USD_cor_mean + AUD_USD_NZD_USD_cor_sd*sd_fac_lm_trade3 &
            #   trade_direction == "Long" &  AUD_USD_tangent_angle1 < 0 & NZD_USD_tangent_angle2 > 0~trade_direction,
            #
            # AUD_USD_NZD_USD_cor >= AUD_USD_NZD_USD_cor_mean + AUD_USD_NZD_USD_cor_sd*sd_fac_lm_trade3 &
            #   trade_direction == "Short" &  AUD_USD_tangent_angle1 > 0 & NZD_USD_tangent_angle2 < 0~trade_direction


          )
      ) %>%
      filter(!is.na(trade_col)) %>%
      dplyr::select(Date, trade_col) %>%
      mutate(
        Asset = "AUD_USD"
      ) %>%
      mutate(
        stop_factor = stop_factor,
        profit_factor = profit_factor
      )

    max_trades_AUD_USD <-
      tagged_trades_AUD_USD %>%
      pull(Date) %>%
      max(na.rm = T) %>%
      as.character()

    message(glue::glue("Max Date in Tagged Data AUD_USD: {max_trades_AUD_USD}"))

    lm_formula_NZD_USD <- create_lm_formula(dependant = "dependant_var_nzd_usd", independant = lm_vars1)
    lm_model_NZD_USD <- lm(formula = lm_formula_NZD_USD, data = training_data)
    lm_formula_NZD_USD_quant <- create_lm_formula(dependant = "dependant_var_nzd_usd", independant = lm_quant_vars)
    lm_model_NZD_USD_quant <- lm(formula = lm_formula_NZD_USD_quant, data = training_data)

    predicted_train_NZD_USD <- predict.lm(lm_model_NZD_USD, newdata = training_data)
    mean_pred_NZD_USD <- mean(predicted_train_NZD_USD, na.rm = T)
    sd_pred_NZD_USD <- sd(predicted_train_NZD_USD, na.rm = T)
    predicted_test_NZD_USD <- predict.lm(lm_model_NZD_USD, newdata = testing_data) %>% as.numeric()
    mean_pred_test_NZD_USD <- mean(predicted_test_NZD_USD, na.rm = T)
    sd_pred_test_NZD_USD <- sd(predicted_test_NZD_USD, na.rm = T)

    predicted_train_NZD_USD_quant <- predict.lm(lm_model_NZD_USD_quant, newdata = training_data)
    mean_pred_NZD_USD_quant <- mean(predicted_train_NZD_USD_quant, na.rm = T)
    sd_pred_NZD_USD_quant <- sd(predicted_train_NZD_USD_quant, na.rm = T)
    predicted_test_NZD_USD_quant <- predict.lm(lm_model_NZD_USD_quant, newdata = testing_data) %>% as.numeric()
    mean_pred_test_NZD_USD_quant <- mean(predicted_test_NZD_USD_quant, na.rm = T)
    sd_pred_test_NZD_USD_quant <- sd(predicted_test_NZD_USD_quant, na.rm = T)

    tagged_trades_NZD_USD <-
      testing_data %>%
      mutate(
        lm_pred_NZD_USD = predicted_test_NZD_USD,
        lm_pred_NZD_USD_quant = predicted_test_NZD_USD_quant
      ) %>%
      mutate(
        trade_col =
          case_when(
            # lm_pred_NZD_USD >= mean_pred_NZD_USD + sd_fac_lm_trade*sd_pred_NZD_USD &
            #   trade_direction == "Long" ~ trade_direction,
            #
            # lm_pred_NZD_USD <= mean_pred_NZD_USD - sd_fac_lm_trade*sd_pred_NZD_USD &
            #   trade_direction == "Short" ~ trade_direction,

            # lm_pred_NZD_USD_quant <= mean_pred_NZD_USD_quant - sd_fac_lm_trade*sd_pred_NZD_USD_quant &
            #   trade_direction == "Long" ~ trade_direction,

            AUD_USD_NZD_USD_quant_lm <= AUD_USD_NZD_USD_quant_lm_mean - AUD_USD_NZD_USD_quant_lm_sd*sd_fac_lm_trade2 &
              trade_direction == "Long" &  AUD_USD_tangent_angle1 > 0 & NZD_USD_tangent_angle2 < 0~trade_direction,

            AUD_USD_NZD_USD_quant_lm <= AUD_USD_NZD_USD_quant_lm_mean - AUD_USD_NZD_USD_quant_lm_sd*sd_fac_lm_trade2 &
              trade_direction == "Short" &  AUD_USD_tangent_angle1 < 0 & NZD_USD_tangent_angle2 > 0~trade_direction,

            AUD_USD_NZD_USD_quant_lm >= AUD_USD_NZD_USD_quant_lm_mean + AUD_USD_NZD_USD_quant_lm_sd*sd_fac_lm_trade2 &
              trade_direction == "Long" &  AUD_USD_tangent_angle1 > 0 & NZD_USD_tangent_angle2 < 0~trade_direction,

            AUD_USD_NZD_USD_quant_lm >= AUD_USD_NZD_USD_quant_lm_mean + AUD_USD_NZD_USD_quant_lm_sd*sd_fac_lm_trade2 &
              trade_direction == "Short" &  AUD_USD_tangent_angle1 < 0 & NZD_USD_tangent_angle2 > 0~trade_direction


            # AUD_USD_NZD_USD_cor <= AUD_USD_NZD_USD_cor_mean - AUD_USD_NZD_USD_cor_sd*sd_fac_lm_trade3 &
            #   trade_direction == "Short" &  AUD_USD_tangent_angle1 > 0 & NZD_USD_tangent_angle2 < 0~trade_direction,
            #
            # AUD_USD_NZD_USD_cor <= AUD_USD_NZD_USD_cor_mean - AUD_USD_NZD_USD_cor_sd*sd_fac_lm_trade3 &
            #   trade_direction == "Long" &  AUD_USD_tangent_angle1 < 0 & NZD_USD_tangent_angle2 > 0~trade_direction,
            #
            # AUD_USD_NZD_USD_cor >= AUD_USD_NZD_USD_cor_mean + AUD_USD_NZD_USD_cor_sd*sd_fac_lm_trade3 &
            #   trade_direction == "Short" &  AUD_USD_tangent_angle1 > 0 & NZD_USD_tangent_angle2 < 0~trade_direction,
            #
            # AUD_USD_NZD_USD_cor >= AUD_USD_NZD_USD_cor_mean + AUD_USD_NZD_USD_cor_sd*sd_fac_lm_trade3 &
            #   trade_direction == "Long" &  AUD_USD_tangent_angle1 < 0 & NZD_USD_tangent_angle2 > 0~trade_direction

          )
      ) %>%
      filter(!is.na(trade_col)) %>%
      dplyr::select(Date, trade_col) %>%
      mutate(
        Asset = "NZD_USD"
      ) %>%
      mutate(
        stop_factor = stop_factor,
        profit_factor = profit_factor
      )

    max_trades_AUD_USD <-
      tagged_trades_AUD_USD %>%
      pull(Date) %>%
      max(na.rm = T) %>%
      as.character()

    message(glue::glue("Max Date in Tagged Data NZD_USD: {max_trades_AUD_USD}"))

    return(list(tagged_trades_NZD_USD %>% filter(Asset %in% assets_to_return),
                tagged_trades_AUD_USD %>% filter(Asset %in% assets_to_return) ))

  }
