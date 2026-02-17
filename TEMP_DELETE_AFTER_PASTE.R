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
    roll_mean_period = 100,
    base_path = "C:/Users/Nikhil Chandra/Documents/trade_data/single_asset_models_v1/"
  ) {

    LM_model <-
      readRDS(
        glue::glue("{base_path}/LM_AR_{period_of_analysis}_{asset_of_interest}.RDS")
      )

    preds_all <- predict.lm(object = LM_model, newdata = AR_model_data)

    GLM_model <-
      readRDS(
        glue::glue("{base_path}/GLM_AR_{period_of_analysis}_{asset_of_interest}.RDS")
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
    sig_thresh = 0.15,
    base_path = "C:/Users/Nikhil Chandra/Documents/trade_data/single_asset_models_v1/"
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
            glue::glue("{base_path}/LM_AR_{period_of_analysis}_{asset_of_interest}.RDS")
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
            glue::glue("{base_path}/GLM_AR_{period_of_analysis}_{asset_of_interest}.RDS")
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
Single_Asset_V3_FIB_Model_data <-
  function(
    asset_data = Indices_Metals_Bonds[[1]] %>% filter(Asset == asset_of_interest),
    asset_of_interest = asset_of_interest,
    fib_lengths = c(144, 233, 377),
    rolling_period = c(144, 233, 377)
  ) {

    returned_data <-
      asset_data %>%
      ungroup() %>%
      filter(Asset == asset_of_interest) %>%
      arrange(Date) %>%
      ungroup() %>%
      mutate(
        Fib_1_Price_max = slider::slide_dbl(.x = High, .f = ~ max(.x, na.rm = T), .before =  fib_lengths[1]),
        Fib_1_Price_min = slider::slide_dbl(.x = Low, .f = ~ min(.x, na.rm = T), .before =  fib_lengths[1]),
        Fib_1_Price_diff = Fib_1_Price_max - Fib_1_Price_min,
        Fib_1_Price_perc = (Price - lag(Price, fib_lengths[1]))/Fib_1_Price_diff,

        Fib_2_Price_max = slider::slide_dbl(.x = High, .f = ~ max(.x, na.rm = T), .before =  fib_lengths[2]),
        Fib_2_Price_min = slider::slide_dbl(.x = Low, .f = ~ min(.x, na.rm = T), .before =  fib_lengths[2]),
        Fib_2_Price_diff = Fib_2_Price_max - Fib_2_Price_min,
        Fib_2_Price_perc = (Price - lag(Price, fib_lengths[2]))/Fib_2_Price_diff,

        Fib_3_Price_max = slider::slide_dbl(.x = High, .f = ~ max(.x, na.rm = T), .before =  fib_lengths[2]),
        Fib_3_Price_min = slider::slide_dbl(.x = Low, .f = ~ min(.x, na.rm = T), .before =  fib_lengths[2]),
        Fib_3_Price_diff = Fib_3_Price_max - Fib_3_Price_min,
        Fib_3_Price_perc = (Price - lag(Price, fib_lengths[2]))/Fib_3_Price_diff
      ) %>%
      mutate(
        Fib_1_state_space_neg_168 = ifelse(Fib_1_Price_perc <= -1.618, 1, 0),
        Fib_1_state_space_neg_786 = ifelse(Fib_1_Price_perc > -1.618 & Fib_1_Price_perc <= -0.786, 1, 0),
        Fib_1_state_space_neg_618 = ifelse(Fib_1_Price_perc > -0.786 & Fib_1_Price_perc <= -0.618, 1, 0),
        Fib_1_state_space_neg_386 = ifelse(Fib_1_Price_perc > -0.618 & Fib_1_Price_perc <= -0.386, 1, 0),
        Fib_1_state_space_neg_236 = ifelse(Fib_1_Price_perc > -0.386 & Fib_1_Price_perc <= -0.236, 1, 0),
        Fib_1_state_space_neg_0 = ifelse(Fib_1_Price_perc > -0.236 & Fib_1_Price_perc <= 0, 1, 0),

        Fib_1_state_space_pos_168 = ifelse(Fib_1_Price_perc >= 1.618, 1, 0),
        Fib_1_state_space_pos_786 = ifelse(Fib_1_Price_perc < 1.618 & Fib_1_Price_perc >= 0.786, 1, 0),
        Fib_1_state_space_pos_618 = ifelse(Fib_1_Price_perc < 0.786 & Fib_1_Price_perc >= 0.618, 1, 0),
        Fib_1_state_space_pos_386 = ifelse(Fib_1_Price_perc < 0.618 & Fib_1_Price_perc >= 0.386, 1, 0),
        Fib_1_state_space_pos_236 = ifelse(Fib_1_Price_perc < 0.386 & Fib_1_Price_perc >= 0.236, 1, 0),
        Fib_1_state_space_pos_0 = ifelse(Fib_1_Price_perc < 0.236 & Fib_1_Price_perc >= 0, 1, 0),

        Fib_2_state_space_neg_168 = ifelse(Fib_2_Price_perc <= -1.618, 1, 0),
        Fib_2_state_space_neg_786 = ifelse(Fib_2_Price_perc > -1.618 & Fib_2_Price_perc <= -0.786, 1, 0),
        Fib_2_state_space_neg_618 = ifelse(Fib_2_Price_perc > -0.786 & Fib_2_Price_perc <= -0.618, 1, 0),
        Fib_2_state_space_neg_386 = ifelse(Fib_2_Price_perc > -0.618 & Fib_2_Price_perc <= -0.386, 1, 0),
        Fib_2_state_space_neg_236 = ifelse(Fib_2_Price_perc > -0.386 & Fib_2_Price_perc <= -0.236, 1, 0),
        Fib_2_state_space_neg_0 = ifelse(Fib_2_Price_perc > -0.236 & Fib_2_Price_perc <= 0, 1, 0),

        Fib_2_state_space_pos_168 = ifelse(Fib_2_Price_perc >= 1.618, 1, 0),
        Fib_2_state_space_pos_786 = ifelse(Fib_2_Price_perc < 1.618 & Fib_2_Price_perc >= 0.786, 1, 0),
        Fib_2_state_space_pos_618 = ifelse(Fib_2_Price_perc < 0.786 & Fib_2_Price_perc >= 0.618, 1, 0),
        Fib_2_state_space_pos_386 = ifelse(Fib_2_Price_perc < 0.618 & Fib_2_Price_perc >= 0.386, 1, 0),
        Fib_2_state_space_pos_236 = ifelse(Fib_2_Price_perc < 0.386 & Fib_2_Price_perc >= 0.236, 1, 0),
        Fib_2_state_space_pos_0 = ifelse(Fib_2_Price_perc < 0.236 & Fib_2_Price_perc >= 0, 1, 0),


        Fib_3_state_space_neg_168 = ifelse(Fib_3_Price_perc <= -1.618, 1, 0),
        Fib_3_state_space_neg_786 = ifelse(Fib_3_Price_perc > -1.618 & Fib_3_Price_perc <= -0.786, 1, 0),
        Fib_3_state_space_neg_618 = ifelse(Fib_3_Price_perc > -0.786 & Fib_3_Price_perc <= -0.618, 1, 0),
        Fib_3_state_space_neg_386 = ifelse(Fib_3_Price_perc > -0.618 & Fib_3_Price_perc <= -0.386, 1, 0),
        Fib_3_state_space_neg_236 = ifelse(Fib_3_Price_perc > -0.386 & Fib_3_Price_perc <= -0.236, 1, 0),
        Fib_3_state_space_neg_0 = ifelse(Fib_3_Price_perc > -0.236 & Fib_3_Price_perc <= 0, 1, 0),

        Fib_3_state_space_pos_168 = ifelse(Fib_3_Price_perc >= 1.618, 1, 0),
        Fib_3_state_space_pos_786 = ifelse(Fib_3_Price_perc < 1.618 & Fib_3_Price_perc >= 0.786, 1, 0),
        Fib_3_state_space_pos_618 = ifelse(Fib_3_Price_perc < 0.786 & Fib_3_Price_perc >= 0.618, 1, 0),
        Fib_3_state_space_pos_386 = ifelse(Fib_3_Price_perc < 0.618 & Fib_3_Price_perc >= 0.386, 1, 0),
        Fib_3_state_space_pos_236 = ifelse(Fib_3_Price_perc < 0.386 & Fib_3_Price_perc >= 0.236, 1, 0),
        Fib_3_state_space_pos_0 = ifelse(Fib_3_Price_perc < 0.236 & Fib_3_Price_perc >= 0, 1, 0)

      ) %>%
      mutate(
        across(
          .cols = contains("Fib_1_state_space")|contains("Fib_2_state_space")|contains("Fib_3_state_space"),
          .fns = ~ ifelse(is.na(.), 0, .)
        )
      ) %>%
      mutate(
        Fib_1_state_space_neg_168_1 =
          slider::slide_dbl(.x = Fib_1_state_space_neg_168,.f = ~ sum(.x, na.rm = T) ,.before = fib_lengths[1])/fib_lengths[1],
        Fib_1_state_space_neg_786_1 =
          slider::slide_dbl(.x = Fib_1_state_space_neg_786,.f = ~ sum(.x, na.rm = T) ,.before = fib_lengths[1])/fib_lengths[1],
        Fib_1_state_space_neg_618_1 =
          slider::slide_dbl(.x = Fib_1_state_space_neg_618,.f = ~ sum(.x, na.rm = T) ,.before = fib_lengths[1])/fib_lengths[1],
        Fib_1_state_space_neg_386_1 =
          slider::slide_dbl(.x = Fib_1_state_space_neg_386,.f = ~ sum(.x, na.rm = T) ,.before = fib_lengths[1])/fib_lengths[1],
        Fib_1_state_space_neg_236_1 =
          slider::slide_dbl(.x = Fib_1_state_space_neg_236,.f = ~ sum(.x, na.rm = T) ,.before = fib_lengths[1])/fib_lengths[1],
        Fib_1_state_space_neg_0_1 =
          slider::slide_dbl(.x = Fib_1_state_space_neg_0,.f = ~ sum(.x, na.rm = T) ,.before = fib_lengths[1])/fib_lengths[1],

        Fib_1_state_space_neg_168_2 =
          slider::slide_dbl(.x = Fib_1_state_space_neg_168,.f = ~ sum(.x, na.rm = T) ,.before = fib_lengths[2])/fib_lengths[2],
        Fib_1_state_space_neg_786_2 =
          slider::slide_dbl(.x = Fib_1_state_space_neg_786,.f = ~ sum(.x, na.rm = T) ,.before = fib_lengths[2])/fib_lengths[2],
        Fib_1_state_space_neg_618_2 =
          slider::slide_dbl(.x = Fib_1_state_space_neg_618,.f = ~ sum(.x, na.rm = T) ,.before = fib_lengths[2])/fib_lengths[2],
        Fib_1_state_space_neg_386_2 =
          slider::slide_dbl(.x = Fib_1_state_space_neg_386,.f = ~ sum(.x, na.rm = T) ,.before = fib_lengths[2])/fib_lengths[2],
        Fib_1_state_space_neg_236_2 =
          slider::slide_dbl(.x = Fib_1_state_space_neg_236,.f = ~ sum(.x, na.rm = T) ,.before = fib_lengths[2])/fib_lengths[2],
        Fib_1_state_space_neg_0_2 =
          slider::slide_dbl(.x = Fib_1_state_space_neg_0,.f = ~ sum(.x, na.rm = T) ,.before = fib_lengths[2])/fib_lengths[2],


        Fib_1_state_space_neg_168_3 =
          slider::slide_dbl(.x = Fib_1_state_space_neg_168,.f = ~ sum(.x, na.rm = T) ,.before = fib_lengths[3])/fib_lengths[3],
        Fib_1_state_space_neg_786_3 =
          slider::slide_dbl(.x = Fib_1_state_space_neg_786,.f = ~ sum(.x, na.rm = T) ,.before = fib_lengths[3])/fib_lengths[3],
        Fib_1_state_space_neg_618_3 =
          slider::slide_dbl(.x = Fib_1_state_space_neg_618,.f = ~ sum(.x, na.rm = T) ,.before = fib_lengths[3])/fib_lengths[3],
        Fib_1_state_space_neg_386_3 =
          slider::slide_dbl(.x = Fib_1_state_space_neg_386,.f = ~ sum(.x, na.rm = T) ,.before = fib_lengths[3])/fib_lengths[3],
        Fib_1_state_space_neg_236_3 =
          slider::slide_dbl(.x = Fib_1_state_space_neg_236,.f = ~ sum(.x, na.rm = T) ,.before = fib_lengths[3])/fib_lengths[3],
        Fib_1_state_space_neg_0_3 =
          slider::slide_dbl(.x = Fib_1_state_space_neg_0,.f = ~ sum(.x, na.rm = T) ,.before = fib_lengths[3])/fib_lengths[3],


        Fib_1_state_space_pos_168_1 =
          slider::slide_dbl(.x = Fib_1_state_space_pos_168,.f = ~ sum(.x, na.rm = T) ,.before = fib_lengths[1]),
        Fib_1_state_space_pos_786_1 =
          slider::slide_dbl(.x = Fib_1_state_space_pos_786,.f = ~ sum(.x, na.rm = T) ,.before = fib_lengths[1]),
        Fib_1_state_space_pos_618_1 =
          slider::slide_dbl(.x = Fib_1_state_space_pos_618,.f = ~ sum(.x, na.rm = T) ,.before = fib_lengths[1]),
        Fib_1_state_space_pos_386_1 =
          slider::slide_dbl(.x = Fib_1_state_space_pos_386,.f = ~ sum(.x, na.rm = T) ,.before = fib_lengths[1]),
        Fib_1_state_space_pos_236_1 =
          slider::slide_dbl(.x = Fib_1_state_space_pos_236,.f = ~ sum(.x, na.rm = T) ,.before = fib_lengths[1]),
        Fib_1_state_space_pos_0_1 =
          slider::slide_dbl(.x = Fib_1_state_space_pos_0,.f = ~ sum(.x, na.rm = T) ,.before = fib_lengths[1]),

        Fib_1_state_space_pos_168_2 =
          slider::slide_dbl(.x = Fib_1_state_space_pos_168,.f = ~ sum(.x, na.rm = T) ,.before = fib_lengths[2]),
        Fib_1_state_space_pos_786_2 =
          slider::slide_dbl(.x = Fib_1_state_space_pos_786,.f = ~ sum(.x, na.rm = T) ,.before = fib_lengths[2]),
        Fib_1_state_space_pos_618_2 =
          slider::slide_dbl(.x = Fib_1_state_space_pos_618,.f = ~ sum(.x, na.rm = T) ,.before = fib_lengths[2]),
        Fib_1_state_space_pos_386_2 =
          slider::slide_dbl(.x = Fib_1_state_space_pos_386,.f = ~ sum(.x, na.rm = T) ,.before = fib_lengths[2]),
        Fib_1_state_space_pos_236_2 =
          slider::slide_dbl(.x = Fib_1_state_space_pos_236,.f = ~ sum(.x, na.rm = T) ,.before = fib_lengths[2]),
        Fib_1_state_space_pos_0_2 =
          slider::slide_dbl(.x = Fib_1_state_space_pos_0,.f = ~ sum(.x, na.rm = T) ,.before = fib_lengths[2]),


        Fib_1_state_space_pos_168_3 =
          slider::slide_dbl(.x = Fib_1_state_space_pos_168,.f = ~ sum(.x, na.rm = T) ,.before = fib_lengths[3]),
        Fib_1_state_space_pos_786_3 =
          slider::slide_dbl(.x = Fib_1_state_space_pos_786,.f = ~ sum(.x, na.rm = T) ,.before = fib_lengths[3]),
        Fib_1_state_space_pos_618_3 =
          slider::slide_dbl(.x = Fib_1_state_space_pos_618,.f = ~ sum(.x, na.rm = T) ,.before = fib_lengths[3]),
        Fib_1_state_space_pos_386_3 =
          slider::slide_dbl(.x = Fib_1_state_space_pos_386,.f = ~ sum(.x, na.rm = T) ,.before = fib_lengths[3]),
        Fib_1_state_space_pos_236_3 =
          slider::slide_dbl(.x = Fib_1_state_space_pos_236,.f = ~ sum(.x, na.rm = T) ,.before = fib_lengths[3]),
        Fib_1_state_space_pos_0_3 =
          slider::slide_dbl(.x = Fib_1_state_space_pos_0,.f = ~ sum(.x, na.rm = T) ,.before = fib_lengths[3])
      )

    return(returned_data)

  }


