combined_model_data <-
  accumulating_probs %>%
  arrange(Date) %>%
  fill(!contains("Date"), .direction = "down") %>%
  filter(if_all(.cols = everything(), ~!is.na(.)))

model_string = "_combined_"

models_in_path <-
  fs::dir_info(save_path) %>%
  filter(str_detect(path, Asset_of_interest),
         str_detect(path, trade_direction),
         str_detect(path, model_string)) %>%
  pull(path)

comb_model_test <- readRDS(models_in_path[6])
summary(comb_model_test)

combined_preds <-
  single_asset_read_models_and_get_pred(
    pred_data = combined_model_data,
    trade_direction = trade_direction,
    Asset_of_interest = Asset_of_interest,
    save_path = save_path,
    model_string = "_combined_"
  ) %>%
  mutate(
    Asset = Asset_of_interest
  )

thresh_test <- 0.6

train_means <-
  combined_preds %>%
  filter(Date < date_test_start) %>%
  mutate(Asset = Asset_of_interest) %>%
  dplyr::select(Date, Asset, contains("pred_combined")) %>%
  mutate(
    Averaged_Bin_Pred = (pred_combined_2 + pred_combined_4 + pred_combined_6)/3
  ) %>%
  summarise(
    mid_pred = round( quantile(Averaged_Bin_Pred, 0.5, na.rm = T), 4 ),
    low_pred = round( quantile(Averaged_Bin_Pred, 0.25, na.rm = T), 4),
    high_pred = round( quantile(Averaged_Bin_Pred, 0.75, na.rm = T), 4 )
  )

out_of_sample_means_3_months <-
  combined_preds %>%
  filter(Date >= date_test_start ) %>%
  # filter(Date <= date_test_start + months(3) ) %>%
  mutate(Asset = Asset_of_interest) %>%
  dplyr::select(Date, Asset, contains("pred_combined")) %>%
  mutate(
    Averaged_Bin_Pred = (pred_combined_2 + pred_combined_4 + pred_combined_6)/3
  ) %>%
  summarise(
    mid_pred = round( quantile(Averaged_Bin_Pred, 0.5, na.rm = T), 10 ),
    low_pred = round( quantile(Averaged_Bin_Pred, 0.25, na.rm = T), 10),
    high_pred = round( quantile(Averaged_Bin_Pred, 0.75, na.rm = T), 10 )
  )

test_combined <-
  combined_preds %>%
  filter(Date >= date_test_start) %>%
  mutate(Asset = Asset_of_interest) %>%
  left_join(
    combined_model_data %>% dplyr::select(Date, Asset, contains("pred_"))
  ) %>%
  # dplyr::select(Date, Asset, contains("pred_combined")) %>%
  dplyr::select(Date, Asset, contains("pred_")) %>%
  left_join(actual_wins_losses %>%
              filter(trade_col == "Long",
                     stop_factor == stop_value_var,
                     profit_factor == profit_value_var) %>%
              dplyr::select(Date, Asset, trade_col,
                            period_return_35_Price, period_return_25_Price, period_return_15_Price)
  ) %>%
  mutate(
    Averaged_Bin_Pred = (pred_macro_4 + pred_index_4 + pred_combined_4 +
                           pred_daily_4 + pred_technical_4 + pred_copula_4)/6,

    Averaged_Bin_Pred_2 = (pred_macro_6 + pred_index_6 + pred_combined_6 +
                           pred_daily_6 + pred_technical_6 + pred_copula_6)/6,


    Averaged_Bin_Pred_3 = (pred_macro_2 + pred_index_2 + pred_combined_2 +
                             pred_daily_2 + pred_technical_2 + pred_copula_2)/6,

    Comb_Averaged_Bin_Pred_3 = (Averaged_Bin_Pred + Averaged_Bin_Pred_2 + Averaged_Bin_Pred_3)/3,

    high_return_date =
      case_when(
        period_return_35_Price > 0 & period_return_25_Price > 0 & period_return_15_Price > 0 ~ "Detected",
        TRUE ~ "Dont Trade"
      )
  )

test_combined %>%
  filter(pred_combined_4 >= 0.999999999999999) %>%
  pull(period_return_35_Price) %>%
  sum(na.rm = TRUE)

# test_preds_binary <-
test_combined %>%
  filter(Date > date_test_start) %>%
  # filter(Date <= date_test_start + months(1)) %>%
  group_by(high_return_date) %>%
  summarise(
    # mid_pred = round( quantile(Averaged_Bin_Pred, 0.5, na.rm = T), 4 ),
    # low_pred = round( quantile(Averaged_Bin_Pred, 0.25, na.rm = T), 4),
    # high_pred = round( quantile(Averaged_Bin_Pred, 0.75, na.rm = T), 4 )

    very_low_pred = round( quantile(Comb_Averaged_Bin_Pred_3, 0.1, na.rm = T), 4),
    low_pred = round( quantile(Comb_Averaged_Bin_Pred_3, 0.25, na.rm = T), 4),
    mid_pred = round( quantile(Comb_Averaged_Bin_Pred_3, 0.5, na.rm = T), 4 ),
    high_pred = round( quantile(Comb_Averaged_Bin_Pred_3, 0.75, na.rm = T), 4 ),
    very_high_pred = round( quantile(Comb_Averaged_Bin_Pred_3, 0.9, na.rm = T), 4 ),

    counts = n()
  )

lm_vars <- names(test_combined) %>%
  keep(~ str_detect(.x, "pred")) %>%
  unlist()

lm_form <-
  create_lm_formula(dependant = "period_return_35_Price", independant = lm_vars)

lm_model <-
  lm(data = test_combined, formula = lm_form)

summary(lm_model)

sig_coefs <-
  get_sig_coefs(model_object_of_interest = lm_model,
                p_value_thresh_for_inputs = 0.9)

lm_form <-
  create_lm_formula(dependant = "period_return_35_Price", independant = sig_coefs)

lm_model <-
  lm(data =
        test_combined %>%
        filter(Date <= date_test_start + months(18)),
      formula = lm_form)

summary(lm_model)

glm_form <-
  create_lm_formula(dependant = "high_return_date == 'Detected' ", independant = lm_vars)

glm_model <-
  glm(data =
        test_combined %>%
        filter(Date <= date_test_start + months(18)),
      formula = glm_form, family = binomial("logit"))

summary(glm_model)

sig_coefs <-
  get_sig_coefs(model_object_of_interest = glm_model,
                p_value_thresh_for_inputs = 0.99)

glm_form <-
  create_lm_formula(dependant = "high_return_date == 'Detected' ", independant = sig_coefs)

glm_model <-
  glm(data =
        test_combined %>%
        filter(Date <= date_test_start + months(18)),
      formula = glm_form, family = binomial("logit"))

summary(glm_model)

test_data_post_pred <-
  test_combined %>%
  filter(Date > date_test_start + months(18))

test_data_post_pred <-
  test_data_post_pred %>%
  mutate(
    Post_Pred = predict.glm(object = glm_model,
                            newdata = test_data_post_pred,
                            type = "response"),
    Post_Pred_lin = predict.lm(object = lm_model,
                            newdata = test_data_post_pred)
  ) %>%
  group_by(Asset) %>%
  arrange(Date, .by_group = TRUE) %>%
  ungroup() %>%
  group_by(Asset) %>%
  mutate(
    rolling_pred_mean =
      slider::slide_dbl(
      .x = Post_Pred,
      .f = ~mean(.x, na.rm = T),
      .before = 50),

    rolling_pred_sd =
      slider::slide_dbl(
        .x = Post_Pred,
        .f = ~sd(.x, na.rm = T),
        .before = 50)
  )

pre_test_preds_mean <-
  glm_model$fitted.values %>% mean()
pre_test_preds_sd <-
  glm_model$fitted.values %>% sd()

test_data_post_pred %>%
  filter(Date > date_test_start + months(18)) %>%
  mutate(
    high_return_date = ifelse(period_return_35_Price > 0, "Detected", "No Trade" )
  ) %>%
  group_by(high_return_date) %>%
  summarise(
    # mid_pred = round( quantile(Averaged_Bin_Pred, 0.5, na.rm = T), 4 ),
    # low_pred = round( quantile(Averaged_Bin_Pred, 0.25, na.rm = T), 4),
    # high_pred = round( quantile(Averaged_Bin_Pred, 0.75, na.rm = T), 4 )

    very_low_pred = round( quantile(Post_Pred, 0.1, na.rm = T), 4),
    low_pred = round( quantile(Post_Pred, 0.25, na.rm = T), 4),
    mid_pred = round( quantile(Post_Pred, 0.5, na.rm = T), 4 ),
    high_pred = round( quantile(Post_Pred, 0.75, na.rm = T), 4 ),
    very_high_pred = round( quantile(Post_Pred, 0.9, na.rm = T), 4 ),

    # very_low_pred = round( quantile(Post_Pred_lin, 0.1, na.rm = T), 4),
    # low_pred = round( quantile(Post_Pred_lin, 0.25, na.rm = T), 4),
    # mid_pred = round( quantile(Post_Pred_lin, 0.5, na.rm = T), 4 ),
    # high_pred = round( quantile(Post_Pred_lin, 0.75, na.rm = T), 4 ),
    # very_high_pred = round( quantile(Post_Pred_lin, 0.9, na.rm = T), 4 ),

    counts = n()
  )


test_data_post_pred %>%
  mutate(wins =
           ifelse(period_return_35_Price > 0, 1, 0 )
  )  %>%
  summarise(
    total_trades = n_distinct(Date),
    wins = sum(wins, na.rm = T),
    perc = wins/total_trades
  )

final_results_test <-
  test_data_post_pred %>%
  filter(Post_Pred >= rolling_pred_mean + rolling_pred_sd*10) %>%
  # filter(Post_Pred >= 0.5) %>%
  # filter(Post_Pred_lin >= 1) %>%
  mutate(wins =
           ifelse(period_return_35_Price > 0, 1, 0 )
         ) %>%
  summarise(
    total_trades = n_distinct(Date),
    wins = sum(wins, na.rm = T),
    perc = wins/total_trades,
    total_return = sum(period_return_35_Price, na.rm = T),

    very_low_return_35 = round( quantile(period_return_35_Price, 0.1, na.rm = T), 4),
    low_return_35 = round( quantile(period_return_35_Price, 0.25, na.rm = T), 4),
    mid_return_35 = round( quantile(period_return_35_Price, 0.5, na.rm = T), 4 ),
    high_return_35 = round( quantile(period_return_35_Price, 0.75, na.rm = T), 4 ),
    very_high_return_35 = round( quantile(period_return_35_Price, 0.9, na.rm = T), 4 ),
    mean_return_35 = mean(period_return_35_Price, na.rm = T),

    very_low_return_25 = round( quantile(period_return_25_Price, 0.1, na.rm = T), 4),
    low_return_25 = round( quantile(period_return_25_Price, 0.25, na.rm = T), 4),
    mid_return_25 = round( quantile(period_return_25_Price, 0.5, na.rm = T), 4 ),
    high_return_25 = round( quantile(period_return_25_Price, 0.75, na.rm = T), 4 ),
    very_high_return_25 = round( quantile(period_return_25_Price, 0.9, na.rm = T), 4 ),
    mean_return_25 = mean(period_return_25_Price, na.rm = T),

  )
