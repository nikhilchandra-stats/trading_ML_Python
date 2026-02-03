helpeR::load_custom_functions()

all_aud_symbols <- get_oanda_symbols() %>%
  keep(~ str_detect(.x, "AUD")|str_detect(.x, "USD_SEK|USD_NOK|USD_HUF|USD_ZAR|USD_CNY|USD_MXN|USD_CZK"))
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

asset_list_oanda =
  c("HK33_HKD", "USD_JPY",
    "BTC_USD",
    "AUD_NZD", "GBP_CHF",
    "EUR_HUF", "EUR_ZAR", "NZD_JPY", "EUR_NZD",
    "USB02Y_USD",
    "XAU_CAD", "GBP_JPY", "EUR_NOK", "USD_SGD", "EUR_SEK",
    "DE30_EUR",
    "AUD_CAD",
    "UK10YB_GBP",
    "XPD_USD",
    "UK100_GBP",
    "USD_CHF", "GBP_NZD",
    "GBP_SGD", "USD_SEK", "EUR_SGD", "XCU_USD", "SUGAR_USD", "CHF_ZAR",
    "AUD_CHF", "EUR_CHF", "USD_MXN", "GBP_USD", "WTICO_USD", "EUR_JPY", "USD_NOK",
    "XAU_USD",
    "DE10YB_EUR",
    "USD_CZK", "AUD_SGD", "USD_HUF", "WHEAT_USD",
    "EUR_USD", "SG30_SGD", "GBP_AUD", "NZD_CAD", "AU200_AUD", "XAG_USD",
    "XAU_EUR", "EUR_GBP", "USD_CNH", "USD_CAD", "NAS100_USD",
    "USB10Y_USD",
    "EU50_EUR", "NATGAS_USD", "CAD_JPY", "FR40_EUR", "USD_ZAR", "XAU_GBP",
    "CH20_CHF", "ESPIX_EUR",
    "XPT_USD",
    "EUR_AUD", "SOYBN_USD",
    "US2000_USD",
    "XAG_USD", "XAG_EUR", "XAG_CAD", "XAG_AUD", "XAG_GBP", "XAG_JPY", "XAG_SGD", "XAG_CHF",
    "XAG_NZD",
    "XAU_USD", "XAU_EUR", "XAU_CAD", "XAU_AUD", "XAU_GBP", "XAU_JPY", "XAU_SGD", "XAU_CHF",
    "XAU_NZD",
    "BTC_USD", "LTC_USD", "BCH_USD",
    "US30_USD", "FR40_EUR", "US2000_USD", "CH20_CHF", "SPX500_USD", "AU200_AUD",
    "JP225_USD", "JP225Y_JPY", "SG30_SGD", "EU50_EUR", "HK33_HKD",
    "USB02Y_USD", "USB05Y_USD", "USB30Y_USD", "USB10Y_USD", "UK100_GBP",
    "EUR_CHF", #1 EUR_CHF
    "EUR_SEK" , #2 EUR_SEK
    "GBP_CHF", #3 GBP_CHF
    "GBP_JPY", #4 GBP_JPY
    "USD_CZK", #5 USD_CZK
    "USD_NOK" , #6 USD_NOK
    "XAG_CAD", #7 XAG_CAD
    "XAG_CHF", #8 XAG_CHF
    "XAG_JPY" , #9 XAG_JPY
    "GBP_NZD" , #10 GBP_NZD
    "NZD_CHF" , #11 NZD_CHF
    "USD_MXN" , #12 USD_MXN
    "XPD_USD" , #13 XPD_USD
    "XPT_USD" , #14 XPT_USD
    "NATGAS_USD" , #15 NATGAS_USD
    "SG30_SGD" , #16 SG30_SGD
    "SOYBN_USD" , #17 SOYBN_USD
    "WHEAT_USD" , #18 WHEAT_USD
    "SUGAR_USD" , #19 SUGAR_USD
    "DE30_EUR" , #20 DE30_EUR
    "UK10YB_GBP" , #21 UK10YB_GBP
    "JP225_USD" , #22 JP225_USD
    "CH20_CHF" , #23 CH20_CHF
    "NL25_EUR" , #24 NL25_EUR
    "XAG_SGD" , #25 XAG_SGD,
    "BCH_USD" , #26 BCH_USD
    "LTC_USD" #27 LTC_USD
  ) %>%
  unique()

asset_infor <- get_instrument_info()
#---------------------Data
load_custom_functions()
db_location = "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data_Most_Assets_2025-09-13.db"
start_date = "2019-06-01"
end_date = today() %>% as.character()

# bin_factor = NULL
# stop_value_var = 2
# profit_value_var = 15
# period_var = 48

All_Daily_Data <-
  get_DAILY_ALGO_DATA_API_REQUEST()

Indices_Metals_Bonds <- get_Port_Buy_Data_2_complete(
  db_location = db_location,
  start_date = start_date,
  end_date = today() %>% as.character(),
  time_frame = "H1"
)

actual_wins_losses <-
  get_actual_wins_losses(
    assets_to_analyse =
      c("EUR_CHF", #1 EUR_CHF
        "EUR_SEK" , #2 EUR_SEK
        "GBP_CHF", #3 GBP_CHF
        "GBP_JPY", #4 GBP_JPY
        "USD_CZK", #5 USD_CZK
        "USD_NOK" , #6 USD_NOK
        "XAG_CAD", #7 XAG_CAD
        "XAG_CHF", #8 XAG_CHF
        "XAG_JPY" , #9 XAG_JPY
        "GBP_NZD" , #10 GBP_NZD
        "NZD_CHF" , #11 NZD_CHF
        "USD_MXN" , #12 USD_MXN
        "XPD_USD" , #13 XPD_USD
        "XPT_USD" , #14 XPT_USD
        "NATGAS_USD" , #15 NATGAS_USD
        "SG30_SGD" , #16 SG30_SGD
        "SOYBN_USD" , #17 SOYBN_USD
        "WHEAT_USD" , #18 WHEAT_USD
        "SUGAR_USD" , #19 SUGAR_USD
        "DE30_EUR" , #20 DE30_EUR
        "UK10YB_GBP" , #21 UK10YB_GBP
        "JP225_USD" , #22 JP225_USD
        "CH20_CHF" , #23 CH20_CHF
        "NL25_EUR" , #24 NL25_EUR
        "XAG_SGD", #25 XAG_SGD
        "BCH_USD" , #26 BCH_USD
        "LTC_USD" #27 LTC_USD
      ),
    asset_data = Indices_Metals_Bonds,
    stop_factor = 5,
    profit_factor = 30,
    risk_dollar_value = 10,
    trade_direction = "Long",
    currency_conversion = currency_conversion,
    asset_infor = asset_infor,
    periods_ahead = 24
  )


#' Single_Asset_V3_Gen_Model
#'
#' @param Indices_Metals_Bonds
#' @param actual_wins_losses
#' @param asset_of_interest
#' @param actuals_periods_needed
#' @param training_end_date
#'
#' @return
#' @export
#'
#' @examples
Single_Asset_V3_Gen_Model <-
  function(Indices_Metals_Bonds,
           actual_wins_losses,
           asset_of_interest = "GBP_JPY",
           actuals_periods_needed = c("period_return_24_Price", "period_return_35_Price", "period_return_46_Price"),
           training_end_date = "2025-05-01",
           bin_threshold = 5,
           rolling_mean_pred_period = 500,
           correlation_rolling_periods = c(100,200, 300),
           copula_assets = c("GBP_USD", "EUR_JPY", "USD_JPY", "XAU_JPY", "GBP_CHF", "XAG_GBP", "GBP_NZD", "UK100_GBP", "EUR_USD", "GBP_AUD") ) {

    asset_data = Indices_Metals_Bonds[[1]] %>% filter(Asset == asset_of_interest)
    actual_wins_losses_asset <- actual_wins_losses %>% filter(Asset == asset_of_interest)

    AR_model_data <-
      Single_Asset_V3_AR_Model_data(
        asset_data = asset_data,
        asset_of_interest = asset_of_interest,
        lag_value_1 = 10,
        lag_value_2 = 20,
        lag_value_3 = 30,
        lag_value_4 = 40,
        lag_value_5 = 50,
        lag_value_6 = 60,
        lag_value_7 = 70,
        MA_period_1 = 10,
        MA_period_2 = 20,
        MA_period_3 = 30,
        MA_period_4 = 40,
        MA_period_5 = 20,
        MA_period_6 = 20
      )

    for (i in 1:length(actuals_periods_needed)) {
      Single_Asset_V3_AR_Gen_Model(
        AR_model_data = AR_model_data,
        asset_of_interest = asset_of_interest,
        actual_wins_losses_asset = actual_wins_losses_asset,
        period_of_analysis = actuals_periods_needed[i],
        training_end_date = training_end_date,
        bin_threshold = bin_threshold,
        sig_thresh = 0.15
      )
    }

    AR_preds_list <- list()

    for (i in 1:length(actuals_periods_needed)) {
      AR_preds_list[[i]] <-
        Single_Asset_V3_AR_read_model(
        AR_model_data = AR_model_data,
        asset_of_interest = asset_of_interest,
        period_of_analysis = actuals_periods_needed[i],
        training_end_date = training_end_date,
        roll_mean_period = rolling_mean_pred_period
      )
    }

    AR_Train_Preds_mean <-
      AR_preds_list %>%
      map(~.x %>% pluck("training_data")) %>%
      reduce(left_join)

    AR_Test_Preds <-
      AR_preds_list %>%
      map(~.x %>% pluck("testing_data")) %>%
      reduce(left_join)

    rm(AR_preds_list)

    copula_list <- list()

    for (i in 1:length(correlation_rolling_periods)) {

      copula_list[[i]] <-
        Single_Asset_V3_Cop_data(
          All_Asset_Data =
            Indices_Metals_Bonds[[1]] %>%
            filter(Asset == asset_of_interest| Asset %in% copula_assets),
          asset_of_interest = asset_of_interest,
          copula_assets = copula_assets,
          rolling_period_cor = correlation_rolling_periods[i]
        )

    }

    copula_data <- copula_list %>% reduce(left_join)

    for (i in 1:length(actuals_periods_needed)) {
      Single_Asset_V3_Copula_Gen_Model(
        copula_data = copula_data,
        asset_of_interest = asset_of_interest,
        actual_wins_losses_asset = actual_wins_losses_asset,
        period_of_analysis = actuals_periods_needed[i],
        training_end_date = training_end_date,
        bin_threshold = bin_threshold,
        sig_thresh = 0.01
      )
    }

    copula_preds_list <- list()

    for (i in 1:length(actuals_periods_needed)) {
      copula_preds_list[[i]] <-
        Single_Asset_V3_Copula_read_Model(
          copula_data = copula_data,
          asset_of_interest = asset_of_interest,
          period_of_analysis = actuals_periods_needed[i],
          training_end_date = training_end_date,
          roll_mean_period = rolling_mean_pred_period
        )
    }

    Copula_Train_Preds_mean <-
      copula_preds_list %>%
      map(~.x %>% pluck("training_data")) %>%
      reduce(left_join)

    Copula_Test_Preds <-
      copula_preds_list %>%
      map(~.x %>% pluck("testing_data")) %>%
      reduce(left_join)

    rm(copula_preds_list)

    state_space_list <- list()
    loop_list_cols <- c("Price", "Low", "High")
    state_space_periods = c(20, 40, 60, 100, 200)
    state_space_rolling = c(100, 200)
    c = 0

    for (j in 1:length(loop_list_cols) ) {
      for (i in 1:length(state_space_periods)) {
        for (k in 1:length(state_space_rolling)) {
          c = c + 1
          state_space_list[[c]] <-
            Single_Asset_V3_state_space(
              asset_data = asset_data,
              asset_of_interest = asset_of_interest,
              Price_diff_lag = state_space_periods[i],
              roll_period_state_space = state_space_rolling[k],
              price_col = loop_list_cols[j]
            )
        }
      }
    }

    state_space_data <-
      state_space_list %>%
      reduce(left_join)

    for (i in 1:length(actuals_periods_needed)) {
      Single_Asset_V3_state_space_Gen_Model(
        state_space_data = state_space_data,
        asset_of_interest = asset_of_interest,
        actual_wins_losses_asset = actual_wins_losses_asset,
        period_of_analysis = actuals_periods_needed[i],
        training_end_date = training_end_date,
        bin_threshold = bin_threshold,
        sig_thresh = 0.01
      )
    }

    state_space_preds_list <- list()

    for (i in 1:length(actuals_periods_needed)) {
      state_space_preds_list[[i]] <-
        Single_Asset_V3_state_space_read_Model(
          state_space_data = state_space_data,
          asset_of_interest = asset_of_interest,
          period_of_analysis = actuals_periods_needed[i],
          training_end_date = training_end_date,
          roll_mean_period = rolling_mean_pred_period
        )
    }

    state_space_Train_Preds_mean <-
      state_space_preds_list %>%
      map(~.x %>% pluck("training_data")) %>%
      reduce(left_join)

    state_space_Test_Preds <-
      state_space_preds_list %>%
      map(~.x %>% pluck("testing_data")) %>%
      reduce(left_join)


    complete_preds_train <-
      AR_Train_Preds_mean %>%
      left_join(
        Copula_Train_Preds_mean
      ) %>%
      left_join(
        state_space_Train_Preds_mean
      )

    first_non_NA_date <-
      complete_preds_train %>%
      filter(if_all(everything(), ~!is.na(.))) %>%
      pull(Date) %>%
      min(na.rm = T)


    complete_preds_train <-
      complete_preds_train %>%
      filter(Date >= first_non_NA_date)

    complete_preds_test <-
      AR_Test_Preds %>%
      left_join(
        Copula_Test_Preds
      ) %>%
      left_join(
        state_space_Test_Preds
      )

    return(
      list(
        "complete_preds_test" = complete_preds_test,
        "complete_preds_train" = complete_preds_train
      )
    )

  }

#' Single_Asset_V3_state_space
#'
#' @return
#' @export
#'
#' @examples
Single_Asset_V3_state_space <-
  function(
    asset_data = asset_data,
    asset_of_interest = asset_of_interest,
    Price_diff_lag = 20,
    roll_period_state_space = 100,
    price_col = "Price"
    ) {

    state_space_dat <-
      asset_data %>%
      group_by(Asset) %>%
      arrange(Date) %>%
      mutate(
        Price_diff = lag(!!as.name(price_col)) - lag(!!as.name(price_col), Price_diff_lag),
        state_space_mean =
          slider::slide_dbl(.x = Price_diff, .f = ~ mean(.x, na.rm = T), .before = roll_period_state_space),
        state_space_sd =
          slider::slide_dbl(.x = Price_diff, .f = ~ mean(.x, na.rm = T), .before = roll_period_state_space)
        # state_space_sd =
        #   slider::slide_dbl(.x = Price_diff, .f = ~ sd(.x, na.rm = T), .before = roll_period_state_space)

      ) %>%
      mutate(

        state_space_min =
          case_when(
            Price_diff <= state_space_mean - state_space_sd*2.5 ~ 1,
            TRUE ~ 0
          ),

        state_space_lowest =
          case_when(
            Price_diff > state_space_mean - state_space_sd*2.5 & Price_diff <= state_space_mean - state_space_sd*1.5 ~ 1,
            TRUE ~ 0
          ),
        state_space_second_lowest =
          case_when(
            Price_diff > state_space_mean - state_space_sd*1.5 & Price_diff <= state_space_mean - state_space_sd*1 ~ 1,
            TRUE ~ 0
          ),
        state_space_third_lowest =
          case_when(
            Price_diff > state_space_mean - state_space_sd*1 & Price_diff <= state_space_mean - state_space_sd*0 ~ 1,
            TRUE ~ 0
          ),
        state_space_third_highest =
          case_when(
            Price_diff > state_space_mean + state_space_sd*0 & Price_diff <= state_space_mean + state_space_sd*1 ~ 1,
            TRUE ~ 0
          ),
        state_space_second_highest =
          case_when(
            Price_diff > state_space_mean + state_space_sd*1 & Price_diff <= state_space_mean + state_space_sd*1.5 ~ 1,
            TRUE ~ 0
          ),
        state_space_highest =
          case_when(
            Price_diff > state_space_mean + state_space_sd*1.5 & Price_diff <= state_space_mean + state_space_sd*2.5 ~ 1,
            TRUE ~ 0
          ),
        state_space_max =
          case_when(
            Price_diff > state_space_mean + state_space_sd*2.5 ~ 1,
            TRUE ~ 0
          )

      ) %>%
      mutate(
        across(
          .cols = c(state_space_max, state_space_highest, state_space_second_highest, state_space_third_highest,
                    state_space_third_lowest, state_space_second_lowest, state_space_lowest, state_space_min),
          .fns = ~
            slider::slide_dbl(.x = ., .f = ~ sum(.x, na.rm = T), .before = roll_period_state_space)
        )
      ) %>%
      filter(!is.na(Price_diff)) %>%
      mutate(
        total_state_space =
          state_space_max + state_space_highest + state_space_second_highest + state_space_third_highest +
          state_space_third_lowest + state_space_second_lowest + state_space_lowest + state_space_min,

        !!as.name( glue::glue("perc_space_max_{Price_diff_lag}_{roll_period_state_space}_{price_col}") ) :=
          state_space_max/total_state_space,
        !!as.name( glue::glue("perc_space_highest_{Price_diff_lag}_{roll_period_state_space}_{price_col}") ) :=
          state_space_highest/total_state_space,
        !!as.name( glue::glue("perc_space_second_highest_{Price_diff_lag}_{roll_period_state_space}_{price_col}") ) :=
          state_space_second_highest/total_state_space,
        !!as.name( glue::glue("perc_space_third_highest_{Price_diff_lag}_{roll_period_state_space}_{price_col}") ) :=
          state_space_third_highest/total_state_space,
        !!as.name( glue::glue("perc_space_third_lowest_{Price_diff_lag}_{roll_period_state_space}_{price_col}") ) :=
          state_space_third_lowest/total_state_space,
        !!as.name( glue::glue("perc_space_second_lowest_{Price_diff_lag}_{roll_period_state_space}_{price_col}") ) :=
          state_space_second_lowest/total_state_space,
        !!as.name( glue::glue("perc_space_space_lowest_{Price_diff_lag}_{roll_period_state_space}_{price_col}") ) :=
          state_space_lowest/total_state_space,
        !!as.name( glue::glue("perc_space_space_min_{Price_diff_lag}_{roll_period_state_space}_{price_col}") ) :=
          state_space_min/total_state_space
      ) %>%
      dplyr::select(Date, Asset, contains("perc_space_")) %>%
      arrange(Date) %>%
      fill(contains("perc_space_"), .direction = "down")

  }

#' Single_Asset_V3_state_space_Gen_Model
#'
#' @param state_space_data
#' @param asset_of_interest
#' @param actual_wins_losses_asset
#' @param period_of_analysis
#' @param training_end_date
#' @param bin_threshold
#' @param sig_thresh
#'
#' @return
#' @export
#'
#' @examples
Single_Asset_V3_state_space_Gen_Model <-
  function(
    state_space_data = state_space_data,
    asset_of_interest = asset_of_interest,
    actual_wins_losses_asset = actual_wins_losses_asset,
    period_of_analysis = actuals_periods_needed[1],
    training_end_date = training_end_date,
    bin_threshold = bin_threshold,
    sig_thresh = 0.01
    ) {

    joined_data <-
      state_space_data %>%
      group_by(Asset) %>%
      arrange(Date, .by_group = TRUE) %>%
      left_join(
        actual_wins_losses_asset %>%
          filter(Asset == asset_of_interest) %>%
          dplyr::select(Date, Asset, !!as.name(period_of_analysis))
      ) %>%
      filter(
        Date <= training_end_date
      ) %>%
      mutate(
        bin_var = ifelse( !!as.name(period_of_analysis) >= bin_threshold, 1, 0)
      )

    dependants <-
      names(joined_data) %>%
      keep(~ str_detect(.x, "perc_space_"))

    lm_form <-
      create_lm_formula(dependant = period_of_analysis, independant = dependants)

    LM_model <- lm(formula = lm_form, data = joined_data)

    sig_coefs <- get_sig_coefs(LM_model, p_value_thresh_for_inputs = sig_thresh)

    lm_form <-
      create_lm_formula(dependant = period_of_analysis, independant = sig_coefs)

    LM_model <- lm(formula = lm_form, data = joined_data)

    saveRDS(LM_model,
            glue::glue("C:/Users/Nikhil Chandra/Documents/trade_data/single_asset_models_v1/LM_state_space_{period_of_analysis}_{asset_of_interest}.RDS")
    )


    dependants <-
      names(joined_data) %>%
      keep(~ str_detect(.x, "perc_space_"))

    Glm_form <-
      create_lm_formula(dependant = "bin_var", independant = dependants)

    GLM_model <- glm(formula = Glm_form, data = joined_data, family = binomial("logit"))

    sig_coefs <- get_sig_coefs(GLM_model, p_value_thresh_for_inputs = sig_thresh)

    Glm_form <-
      create_lm_formula(dependant = "bin_var", independant = sig_coefs)

    GLM_model <- glm(formula = Glm_form, data = joined_data, family = binomial("logit"))

    saveRDS(GLM_model,
            glue::glue("C:/Users/Nikhil Chandra/Documents/trade_data/single_asset_models_v1/GLM_state_space_{period_of_analysis}_{asset_of_interest}.RDS")
    )

  }

#' Single_Asset_V3_state_space_read_Model
#'
#' @param state_space_data
#' @param asset_of_interest
#' @param period_of_analysis
#' @param training_end_date
#' @param roll_mean_period
#'
#' @return
#' @export
#'
#' @examples
Single_Asset_V3_state_space_read_Model <-
  function(
    state_space_data = state_space_data,
    asset_of_interest = asset_of_interest,
    period_of_analysis = actuals_periods_needed[1],
    training_end_date = training_end_date,
    roll_mean_period = 100
  ) {

    LM_model <-
      readRDS(
        glue::glue("C:/Users/Nikhil Chandra/Documents/trade_data/single_asset_models_v1/LM_state_space_{period_of_analysis}_{asset_of_interest}.RDS")
      )

    preds_all <- predict.lm(object = LM_model, newdata = state_space_data)

    GLM_model <-
      readRDS(
        glue::glue("C:/Users/Nikhil Chandra/Documents/trade_data/single_asset_models_v1/GLM_state_space_{period_of_analysis}_{asset_of_interest}.RDS")
      )

    preds_all_GLM <- predict(object = GLM_model, newdata = state_space_data, type = "response")

    complete_state_space_data <-
      state_space_data %>%
      filter(Asset == asset_of_interest) %>%
      distinct(Date, Asset) %>%
      mutate(
        !!as.name(glue::glue("state_space_LM_Pred_{period_of_analysis}")) := preds_all,
        !!as.name(glue::glue("state_space_GLM_Pred_{period_of_analysis}")) := preds_all_GLM
      ) %>%
      mutate(
        !!as.name(glue::glue("state_space_LM_Pred_{period_of_analysis}_mean")) :=
          slider::slide_dbl(.x =
                              !!as.name(glue::glue("state_space_LM_Pred_{period_of_analysis}")),
                            .f = ~ mean(.x, na.rm = T),
                            .before = roll_mean_period),
        !!as.name(glue::glue("state_space_LM_Pred_{period_of_analysis}_sd")) :=
          slider::slide_dbl(.x =
                              !!as.name(glue::glue("state_space_LM_Pred_{period_of_analysis}")),
                            .f = ~ sd(.x, na.rm = T),
                            .before = roll_mean_period),

        !!as.name(glue::glue("state_space_GLM_Pred_{period_of_analysis}_mean")) :=
          slider::slide_dbl(.x =
                              !!as.name(glue::glue("state_space_GLM_Pred_{period_of_analysis}")),
                            .f = ~ mean(.x, na.rm = T),
                            .before = roll_mean_period),
        !!as.name(glue::glue("state_space_GLM_Pred_{period_of_analysis}_sd")) :=
          slider::slide_dbl(.x =
                              !!as.name(glue::glue("state_space_GLM_Pred_{period_of_analysis}")),
                            .f = ~ sd(.x, na.rm = T),
                            .before = roll_mean_period)
      )

    testing_data <-
      complete_state_space_data %>%
      filter(Date > training_end_date)

    training_data <-
      complete_state_space_data %>%
      filter(Date <= training_end_date)

    return(list("testing_data" = testing_data, "training_data" = training_data) )

  }

#' Single_Asset_V3_Cop_data
#'
#' @param All_Asset_Data
#' @param asset_of_interest
#' @param copula_assets
#' @param rolling_period_cor
#'
#' @return
#' @export
#'
#' @examples
Single_Asset_V3_Cop_data <-
  function(All_Asset_Data = Indices_Metals_Bonds[[1]] %>% filter(Asset == asset_of_interest| Asset %in% copula_assets),
           asset_of_interest = asset_of_interest,
           copula_assets = copula_assets,
           rolling_period_cor = 100) {

    asset_data_cop <-
      All_Asset_Data %>%
      filter(Asset == asset_of_interest)

    cop_accumulator <- list()

    for (i in 1:length(copula_assets) ) {

      col_prefix <- paste0(asset_of_interest, "_", copula_assets[i])

      cop_comparison_data <-
        All_Asset_Data %>%
        filter(Asset == copula_assets[i]) %>%
        dplyr::select(
          Date,
          Price_2 = Price,
          High_2 = High,
          Low_2 = Low
        )

      cop_accumulator[[i]] <-
        asset_data_cop %>%
        left_join(cop_comparison_data) %>%
        arrange(Date) %>%
        fill(c(Price_2, High_2, Low_2), .direction = "down") %>%
        mutate(

          !!as.name(paste0(col_prefix,"_" ,"cor_price", "_", rolling_period_cor)) :=
            slider::slide2_dbl(.x = (Price), .y = (Price_2), .f = ~ cor(.x, .y), .before = rolling_period_cor),
           !!as.name(paste0(col_prefix,"_" ,"cor_Low", "_", rolling_period_cor)) :=
            slider::slide2_dbl(.x = (Low), .y = (Low_2), .f = ~ cor(.x, .y), .before = rolling_period_cor),
          !!as.name(paste0(col_prefix,"_" ,"cor_High", "_", rolling_period_cor)) :=
            slider::slide2_dbl(.x = (High), .y = (High_2), .f = ~ cor(.x, .y), .before = rolling_period_cor),

          !!as.name(paste0(col_prefix,"_" ,"cor_price_mean", "_", rolling_period_cor)) :=
            slider::slide_dbl(.x = !!as.name(paste0(col_prefix,"_" ,"cor_price", "_", rolling_period_cor)),  .f = ~ mean(.x, na.rm = T), .before = rolling_period_cor),
          !!as.name(paste0(col_prefix,"_" ,"cor_Low_mean", "_", rolling_period_cor)) :=
            slider::slide_dbl(.x = !!as.name(paste0(col_prefix,"_" ,"cor_Low", "_", rolling_period_cor)),  .f = ~ mean(.x, na.rm = T), .before = rolling_period_cor),
          !!as.name(paste0(col_prefix,"_" ,"cor_High_mean", "_", rolling_period_cor)) :=
            slider::slide_dbl(.x = !!as.name(paste0(col_prefix,"_" ,"cor_High", "_", rolling_period_cor)), .f = ~ mean(.x, na.rm = T), .before = rolling_period_cor),

          !!as.name(paste0(col_prefix,"_" ,"cor_price_sd", "_", rolling_period_cor)) :=
            slider::slide_dbl(.x = !!as.name(paste0(col_prefix,"_" ,"cor_price", "_", rolling_period_cor)),  .f = ~ sd(.x, na.rm = T), .before = rolling_period_cor),
          !!as.name(paste0(col_prefix,"_" ,"cor_Low_sd", "_", rolling_period_cor)) :=
            slider::slide_dbl(.x = !!as.name(paste0(col_prefix,"_" ,"cor_Low", "_", rolling_period_cor)),  .f = ~ sd(.x, na.rm = T), .before = rolling_period_cor),
          !!as.name(paste0(col_prefix,"_" ,"cor_High_sd", "_", rolling_period_cor)) :=
            slider::slide_dbl(.x = !!as.name(paste0(col_prefix,"_" ,"cor_High", "_", rolling_period_cor)), .f = ~ sd(.x, na.rm = T), .before = rolling_period_cor)

        ) %>%
        dplyr::select(-Price, -Price_2, -High, -High_2, -Low, -Low_2, -Vol., -Open) %>%
        group_by(Asset) %>%
        arrange(Date, .by_group = TRUE) %>%
        group_by(Asset) %>%
        mutate(across(
          .cols = contains("cor_"),
          .fns = ~ lag(.)
        ))

    }

    returned_data <-
      cop_accumulator %>%
      reduce(left_join)


  }

#' Single_Asset_V3_Copula_Gen_Model
#'
#' @param copula_data
#' @param asset_of_interest
#' @param actual_wins_losses_asset
#' @param period_of_analysis
#' @param training_end_date
#' @param bin_threshold
#' @param sig_thresh
#'
#' @return
#' @export
#'
#' @examples
Single_Asset_V3_Copula_Gen_Model <-
  function(
    copula_data = copula_data,
    asset_of_interest = asset_of_interest,
    actual_wins_losses_asset = actual_wins_losses_asset,
    period_of_analysis = actuals_periods_needed[1],
    training_end_date = training_end_date,
    bin_threshold = bin_threshold,
    sig_thresh = 0.15
  ) {

    joined_data <-
      copula_data %>%
      group_by(Asset) %>%
      arrange(Date, .by_group = TRUE) %>%
      fill(contains("cor"), .direction = "down") %>%
      left_join(
        actual_wins_losses_asset %>%
          filter(Asset == asset_of_interest) %>%
          dplyr::select(Date, Asset, !!as.name(period_of_analysis))
      ) %>%
      filter(
        Date <= training_end_date
      ) %>%
      mutate(
        bin_var = ifelse( !!as.name(period_of_analysis) >= bin_threshold, 1, 0)
      )

    dependants <-
      names(joined_data) %>%
      keep(~ str_detect(.x, "cor_"))

    lm_form <-
      create_lm_formula(dependant = period_of_analysis, independant = dependants)

    LM_model <- lm(formula = lm_form, data = joined_data)

    sig_coefs <- get_sig_coefs(LM_model, p_value_thresh_for_inputs = sig_thresh)

    lm_form <-
      create_lm_formula(dependant = period_of_analysis, independant = sig_coefs)

    LM_model <- lm(formula = lm_form, data = joined_data)

    saveRDS(LM_model,
            glue::glue("C:/Users/Nikhil Chandra/Documents/trade_data/single_asset_models_v1/LM_Copula_{period_of_analysis}_{asset_of_interest}.RDS")
    )

    dependants <-
      names(joined_data) %>%
      keep(~ str_detect(.x, "cor_"))

    Glm_form <-
      create_lm_formula(dependant = "bin_var", independant = dependants)

    GLM_model <- glm(formula = Glm_form, data = joined_data, family = binomial("logit"))

    sig_coefs <- get_sig_coefs(GLM_model, p_value_thresh_for_inputs = sig_thresh)

    Glm_form <-
      create_lm_formula(dependant = "bin_var", independant = sig_coefs)

    GLM_model <- glm(formula = Glm_form, data = joined_data, family = binomial("logit"))

    saveRDS(GLM_model,
            glue::glue("C:/Users/Nikhil Chandra/Documents/trade_data/single_asset_models_v1/GLM_Copula_{period_of_analysis}_{asset_of_interest}.RDS")
    )

    rm(GLM_model)

  }

#' Single_Asset_V3_Copula_read_Model
#'
#' @param copula_data
#' @param asset_of_interest
#' @param period_of_analysis
#' @param training_end_date
#' @param roll_mean_period
#'
#' @return
#' @export
#'
#' @examples
Single_Asset_V3_Copula_read_Model <-
  function(
    copula_data = copula_data,
    asset_of_interest = asset_of_interest,
    period_of_analysis = actuals_periods_needed[1],
    training_end_date = training_end_date,
    roll_mean_period = 100
  ) {

    LM_model <-
      readRDS(
        glue::glue("C:/Users/Nikhil Chandra/Documents/trade_data/single_asset_models_v1/LM_Copula_{period_of_analysis}_{asset_of_interest}.RDS")
      )

    preds_all <- predict.lm(object = LM_model, newdata = copula_data)

    GLM_model <-
      readRDS(
        glue::glue("C:/Users/Nikhil Chandra/Documents/trade_data/single_asset_models_v1/GLM_Copula_{period_of_analysis}_{asset_of_interest}.RDS")
      )

    preds_all_GLM <- predict(object = GLM_model, newdata = copula_data, type = "response")

    complete_copula_data <-
      copula_data %>%
      filter(Asset == asset_of_interest) %>%
      distinct(Date, Asset) %>%
      mutate(
        !!as.name(glue::glue("Copula_LM_Pred_{period_of_analysis}")) := preds_all,
        !!as.name(glue::glue("Copula_GLM_Pred_{period_of_analysis}")) := preds_all_GLM
      ) %>%
      mutate(
        !!as.name(glue::glue("Copula_LM_Pred_{period_of_analysis}_mean")) :=
          slider::slide_dbl(.x =
                              !!as.name(glue::glue("Copula_LM_Pred_{period_of_analysis}")),
                            .f = ~ mean(.x, na.rm = T),
                            .before = roll_mean_period),
        !!as.name(glue::glue("Copula_LM_Pred_{period_of_analysis}_sd")) :=
          slider::slide_dbl(.x =
                              !!as.name(glue::glue("Copula_LM_Pred_{period_of_analysis}")),
                            .f = ~ sd(.x, na.rm = T),
                            .before = roll_mean_period),

        !!as.name(glue::glue("Copula_GLM_Pred_{period_of_analysis}_mean")) :=
          slider::slide_dbl(.x =
                              !!as.name(glue::glue("Copula_GLM_Pred_{period_of_analysis}")),
                            .f = ~ mean(.x, na.rm = T),
                            .before = roll_mean_period),
        !!as.name(glue::glue("Copula_GLM_Pred_{period_of_analysis}_sd")) :=
          slider::slide_dbl(.x =
                              !!as.name(glue::glue("Copula_GLM_Pred_{period_of_analysis}")),
                            .f = ~ sd(.x, na.rm = T),
                            .before = roll_mean_period)
      )

    testing_data <-
      complete_copula_data %>%
      filter(Date > training_end_date)

    training_data <-
      complete_copula_data %>%
      filter(Date <= training_end_date)

    return(list("testing_data" = testing_data, "training_data" = training_data) )


  }

#' Single_Asset_V3_AR_read_model
#'
#' @param AR_model_data
#' @param asset_of_interest
#' @param period_of_analysis
#' @param training_end_date
#' @param bin_threshold
#' @param sig_thresh
#'
#' @return
#' @export
#'
#' @examples
Single_Asset_V3_AR_read_model <-
  function(
    AR_model_data = AR_model_data,
    asset_of_interest = asset_of_interest,
    period_of_analysis = actuals_periods_needed[1],
    training_end_date = training_end_date,
    roll_mean_period = 100
    ) {

    LM_model <-
      readRDS(
        glue::glue("C:/Users/Nikhil Chandra/Documents/trade_data/single_asset_models_v1/LM_AR_{period_of_analysis}_{asset_of_interest}.RDS")
        )

    preds_all <- predict.lm(object = LM_model, newdata = AR_model_data)

    GLM_model <-
      readRDS(
        glue::glue("C:/Users/Nikhil Chandra/Documents/trade_data/single_asset_models_v1/GLM_AR_{period_of_analysis}_{asset_of_interest}.RDS")
      )

    preds_all_GLM <- predict(object = GLM_model, newdata = AR_model_data, type = "response")

    complete_AR_data <-
      AR_model_data %>%
      filter(Asset == asset_of_interest) %>%
      distinct(Date, Asset) %>%
      mutate(
        !!as.name(glue::glue("AR_LM_Pred_{period_of_analysis}")) := preds_all,
        !!as.name(glue::glue("AR_GLM_Pred_{period_of_analysis}")) := preds_all_GLM
      ) %>%
      mutate(
        !!as.name(glue::glue("AR_LM_Pred_{period_of_analysis}_mean")) :=
          slider::slide_dbl(.x =
                              !!as.name(glue::glue("AR_LM_Pred_{period_of_analysis}")),
                            .f = ~ mean(.x, na.rm = T),
                            .before = roll_mean_period),
        !!as.name(glue::glue("AR_LM_Pred_{period_of_analysis}_sd")) :=
          slider::slide_dbl(.x =
                              !!as.name(glue::glue("AR_LM_Pred_{period_of_analysis}")),
                            .f = ~ sd(.x, na.rm = T),
                            .before = roll_mean_period),

        !!as.name(glue::glue("AR_GLM_Pred_{period_of_analysis}_mean")) :=
          slider::slide_dbl(.x =
                              !!as.name(glue::glue("AR_GLM_Pred_{period_of_analysis}")),
                            .f = ~ mean(.x, na.rm = T),
                            .before = roll_mean_period),
        !!as.name(glue::glue("AR_GLM_Pred_{period_of_analysis}_sd")) :=
          slider::slide_dbl(.x =
                              !!as.name(glue::glue("AR_GLM_Pred_{period_of_analysis}")),
                            .f = ~ sd(.x, na.rm = T),
                            .before = roll_mean_period)
      )

    testing_data <-
      complete_AR_data %>%
      filter(Date > training_end_date)

    training_data <-
      complete_AR_data %>%
      filter(Date <= training_end_date)

    return(list("testing_data" = testing_data, "training_data" = training_data) )

  }

#' Single_Asset_V3_AR_Gen_Model
#'
#' @param AR_model_data
#' @param asset_of_interest
#' @param actual_wins_losses_asset
#' @param period_of_analysis
#' @param training_end_date
#' @param bin_threshold
#' @param sig_thresh
#'
#' @return
#' @export
#'
#' @examples
Single_Asset_V3_AR_Gen_Model <-
  function(
    AR_model_data = AR_model_data,
    asset_of_interest = asset_of_interest,
    actual_wins_losses_asset = actual_wins_losses_asset,
    period_of_analysis = actuals_periods_needed[1],
    training_end_date = training_end_date,
    bin_threshold = bin_threshold,
    sig_thresh = 0.15
    ) {

    joined_data <-
      AR_model_data %>%
      left_join(
        actual_wins_losses_asset %>%
          filter(Asset == asset_of_interest) %>%
          dplyr::select(Date, Asset, !!as.name(period_of_analysis))
      ) %>%
      filter(
        Date <= training_end_date
      ) %>%
      mutate(
        bin_var = ifelse( !!as.name(period_of_analysis) >= bin_threshold, 1, 0)
      )

    dependants <-
      names(joined_data) %>%
      keep(~ str_detect(.x, "MA_|lagged_|MSD_"))

    lm_form <-
      create_lm_formula(dependant = period_of_analysis, independant = dependants)

    LM_model <- lm(formula = lm_form, data = joined_data)

    sig_coefs <- get_sig_coefs(LM_model, p_value_thresh_for_inputs = sig_thresh)

    lm_form <-
      create_lm_formula(dependant = period_of_analysis, independant = sig_coefs)

    LM_model <- lm(formula = lm_form, data = joined_data)

    saveRDS(LM_model,
            glue::glue("C:/Users/Nikhil Chandra/Documents/trade_data/single_asset_models_v1/LM_AR_{period_of_analysis}_{asset_of_interest}.RDS")
            )

    rm(LM_model)

    dependants <-
      names(joined_data) %>%
      keep(~ str_detect(.x, "MA_|lagged_|MSD_"))

    Glm_form <-
      create_lm_formula(dependant = "bin_var", independant = dependants)

    GLM_model <- glm(formula = Glm_form, data = joined_data, family = binomial("logit"))

    sig_coefs <- get_sig_coefs(GLM_model, p_value_thresh_for_inputs = sig_thresh)

    Glm_form <-
      create_lm_formula(dependant = "bin_var", independant = sig_coefs)

    GLM_model <- glm(formula = Glm_form, data = joined_data, family = binomial("logit"))

    saveRDS(GLM_model,
            glue::glue("C:/Users/Nikhil Chandra/Documents/trade_data/single_asset_models_v1/GLM_AR_{period_of_analysis}_{asset_of_interest}.RDS")
    )

  }

#' Single_Asset_V3_AR_Model_data
#'
#' @param asset_data
#' @param asset_of_interest
#' @param lag_value_1
#' @param lag_value_2
#' @param lag_value_3
#' @param lag_value_4
#' @param lag_value_5
#' @param lag_value_6
#' @param MA_period_1
#' @param MA_period_2
#' @param MA_period_3
#' @param MA_period_4
#'
#' @return
#' @export
#'
#' @examples
Single_Asset_V3_AR_Model_data <-
  function(
    asset_data = Indices_Metals_Bonds,
    asset_of_interest = asset_of_interest,
    lag_value_1 = 2,
    lag_value_2 = 4,
    lag_value_3 = 6,
    lag_value_4 = 8,
    lag_value_5 = 10,
    lag_value_6 = 12,
    lag_value_7 = 20,

    MA_period_1 = 5,
    MA_period_2 = 10,
    MA_period_3 = 15,
    MA_period_4 = 20,
    MA_period_5 = 30,
    MA_period_6 = 40
    ) {

    returned_data <-
      asset_data %>%
      ungroup() %>%
      filter(Asset == asset_of_interest) %>%
      arrange(Date) %>%
      mutate(
             lagged_Price = lag(Price) - lag(Price, lag_value_1 + 1),
             lagged_High = lag(High) - lag(Price, lag_value_1 + 1),
             lagged_Low = lag(Low) - lag(Price, lag_value_1 + 1),

             lagged_Price2 = lag(Price) - lag(Price, lag_value_2 + 1),
             lagged_High2 = lag(High) - lag(Price, lag_value_2 + 1),
             lagged_Low2 = lag(Low) - lag(Price, lag_value_2 + 1),

             lagged_Price3 = lag(Price) - lag(Price, lag_value_3 + 1),
             lagged_High3 = lag(High) - lag(Price, lag_value_3 + 1),
             lagged_Low3 = lag(Low) - lag(Price, lag_value_3 + 1),

             lagged_Price4 = lag(Price) - lag(Price, lag_value_4 + 1),
             lagged_High4 = lag(High) - lag(Price, lag_value_4 + 1),
             lagged_Low4 = lag(Low) - lag(Price, lag_value_4 + 1),

             lagged_Price5 = lag(Price) - lag(Price, lag_value_5 + 1),
             lagged_High5 = lag(High) - lag(Price, lag_value_5 + 1),
             lagged_Low5 = lag(Low) - lag(Price, lag_value_5 + 1),

             lagged_Price6 = lag(Price) - lag(Price, lag_value_6 + 1),
             lagged_High6 = lag(High) - lag(Price, lag_value_6 + 1),
             lagged_Low6 = lag(Low) - lag(Price, lag_value_6 + 1),

             lagged_Price7 = lag(Price) - lag(Price, lag_value_7 + 1),
             lagged_High7 = lag(High) - lag(Price, lag_value_7 + 1),
             lagged_Low7 = lag(Low) - lag(Price, lag_value_7 + 1)

             ) %>%
      mutate(
        MA_Price_1 = slider::slide_dbl(.x = lagged_Price, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_1),
        MA_High_1 = slider::slide_dbl(.x = lagged_High, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_1),
        MA_Low_1 = slider::slide_dbl(.x = lagged_Low, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_1),

        MA_Price_2 = slider::slide_dbl(.x = lagged_Price2, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_2),
        MA_High_2 = slider::slide_dbl(.x = lagged_High2, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_2),
        MA_Low_2 = slider::slide_dbl(.x = lagged_Low2, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_2),

        MA_Price_3 = slider::slide_dbl(.x = lagged_Price3, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_3),
        MA_High_3 = slider::slide_dbl(.x = lagged_High3, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_3),
        MA_Low_3 = slider::slide_dbl(.x = lagged_Low3, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_3),

        MA_Price_4 = slider::slide_dbl(.x = lagged_Price4, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_4),
        MA_High_4 = slider::slide_dbl(.x = lagged_High4, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_4),
        MA_Low_4 = slider::slide_dbl(.x = lagged_Low4, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_4),

        MA_Price_5 = slider::slide_dbl(.x = lagged_Price5, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_5),
        MA_High_5 = slider::slide_dbl(.x = lagged_High5, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_5),
        MA_Low_5 = slider::slide_dbl(.x = lagged_Low5, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_5),

        MA_Price_6 = slider::slide_dbl(.x = lagged_Price6, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_6),
        MA_High_6 = slider::slide_dbl(.x = lagged_High6, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_6),
        MA_Low_6 = slider::slide_dbl(.x = lagged_Low6, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_6),


        MSD_Price_1 = slider::slide_dbl(.x = lagged_Price, .f = ~ sd(.x, na.rm = T) ,.before = MA_period_1),
        MSD_High_1 = slider::slide_dbl(.x = lagged_High, .f = ~ sd(.x, na.rm = T) ,.before = MA_period_1),
        MSD_Low_1 = slider::slide_dbl(.x = lagged_Low, .f = ~ sd(.x, na.rm = T) ,.before = MA_period_1),

        MSD_Price_2 = slider::slide_dbl(.x = lagged_Price2, .f = ~ sd(.x, na.rm = T) ,.before = MA_period_2),
        MSD_High_2 = slider::slide_dbl(.x = lagged_High2, .f = ~ sd(.x, na.rm = T) ,.before = MA_period_2),
        MSD_Low_2 = slider::slide_dbl(.x = lagged_Low2, .f = ~ sd(.x, na.rm = T) ,.before = MA_period_2),

        MSD_Price_3 = slider::slide_dbl(.x = lagged_Price3, .f = ~ sd(.x, na.rm = T) ,.before = MA_period_3),
        MSD_High_3 = slider::slide_dbl(.x = lagged_High3, .f = ~ sd(.x, na.rm = T) ,.before = MA_period_3),
        MSD_Low_3 = slider::slide_dbl(.x = lagged_Low3, .f = ~ sd(.x, na.rm = T) ,.before = MA_period_3),

        MSD_Price_4 = slider::slide_dbl(.x = lagged_Price4, .f = ~ sd(.x, na.rm = T) ,.before = MA_period_4),
        MSD_High_4 = slider::slide_dbl(.x = lagged_High4, .f = ~ sd(.x, na.rm = T) ,.before = MA_period_4),
        MSD_Low_4 = slider::slide_dbl(.x = lagged_Low4, .f = ~ sd(.x, na.rm = T) ,.before = MA_period_4),

        MSD_Price_5 = slider::slide_dbl(.x = lagged_Price5, .f = ~ sd(.x, na.rm = T) ,.before = MA_period_5),
        MSD_High_5 = slider::slide_dbl(.x = lagged_High5, .f = ~ sd(.x, na.rm = T) ,.before = MA_period_5),
        MSD_Low_5 = slider::slide_dbl(.x = lagged_Low5, .f = ~ sd(.x, na.rm = T) ,.before = MA_period_5),

        MSD_Price_6 = slider::slide_dbl(.x = lagged_Price6, .f = ~ sd(.x, na.rm = T) ,.before = MA_period_6),
        MSD_High_6 = slider::slide_dbl(.x = lagged_High6, .f = ~ sd(.x, na.rm = T) ,.before = MA_period_6),
        MSD_Low_6 = slider::slide_dbl(.x = lagged_Low6, .f = ~ sd(.x, na.rm = T) ,.before = MA_period_6)
      )

    return(returned_data)

  }


