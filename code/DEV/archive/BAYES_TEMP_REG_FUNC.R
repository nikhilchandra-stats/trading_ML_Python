test <- readRDS(glue::glue("D:/trade_data/Day_Trader_Cor_Continuous_Models//Bayes_Equity.RDS"))
summary(test)
rm(test)
gc()

sig_coefs <-
  names(reg_data_bayes_train[[1]]) %>%
  keep(~ (.x == "Asset"|
            (str_detect(.x, "state_space")&str_detect(.x, "rolling"))|
            (str_detect(.x, "Price_diff_Low"))|
            (str_detect(.x, "Period_Return_Lag_"))|
            (str_detect(.x, "Lagged_Final_Return_"))|

            .x %in%
            c(
              "Cor_Model_1_pred",
              "Cor_Model_1_Low_Sig_pred",
              "Cor_Model_2_pred",
              "diff_dat_model_1_pred",
              "diff_dat_model_1_Low_Sig_pred",
              "diff_dat_model_2_pred",
              "return_based_model_1_pred",
              "return_based_model_2_pred"
            )
  )
  # !(str_detect(.x, "cor_"))
  ) %>%
  unlist()

interact_vars <-
  names(training_data) %>%
  keep(~str_detect(.x, paste(c("Cor_Model_1_pred",
                               "return_based_model_1_pred",
                               "return_based_model_2_pred",
                               "diff_dat_model_1_pred",
                               "diff_dat_model_2_pred",
                               "diff_dat_model_1_Low_Sig_pred",
                               "Period_Return_Lag_24"), collapse = "|") )) %>%
  unlist()

interact_list <-
  interact_vars %>%
  map(
    ~ c("Asset", .x)
  )

lm_form <- create_lm_formula_interact(
  dependant = dependant_var,
  independant = sig_coefs,
  interacts = interact_list
)

portfolio_atomic_pre_train_lm <-
  function(training_data = training_data,
           pre_train_length = 3000,
           date_filter_train = date_filter_train,
           dependant_var = "Final_Return",
           reg_vars = sig_coefs ) {

    pre_train_input_model_data <-
      training_data %>%
      filter(Date <= date_filter_train) %>%
      group_by(Asset) %>%
      slice_sample(n = pre_train_length) %>%
      ungroup()

    lm_form <- create_lm_formula_interact(
      dependant = dependant_var,
      independant = reg_vars,
      interacts = NULL
    )

    pre_train_model <- lm(formula = lm_form, data = pre_train_input_model_data)
    pre_train_prediction <-
      predict(object = pre_train_model, newdata = training_data, type = "response") %>%
      as.numeric()

    return(pre_train_prediction)

  }

pre_train_vectors <-
  seq(800,830,1) %>%
  map(
    ~ glue::glue("pretrain_data_{.x}")
  ) %>%
  unlist()

execute_pre_train_code <-
  seq(800,830,1) %>%
  map(
    ~ glue::glue("pretrain_data_{.x} = portfolio_atomic_pre_train_lm(training_data = training_data, pre_train_length = {.x}, date_filter_train = date_filter_train, dependant_var = 'Final_Return', reg_vars = sig_coefs) ")
  ) %>%
  unlist() %>%
  paste(collapse = "\n")

eval(parse(text = execute_pre_train_code))

add_to_df_code <-
  pre_train_vectors %>%
  map(
    ~ glue::glue("{.x} = {.x}")
  ) %>%
  unlist() %>%
  paste(collapse = ",")

add_to_df_code <- glue::glue("training_data = training_data %>% mutate({add_to_df_code})")

eval(parse(text = add_to_df_code))

rm_pre_train_code <-
  pre_train_vectors %>%
  map(~glue::glue("rm({.x})")) %>%
  unlist() %>%
  paste(collapse = "\n")

eval(parse(text = rm_pre_train_code))
gc()

rolling_pre_train_code <-
  seq(800,830,1) %>%
  map(
    ~ glue::glue("pretrain_data_{.x} = slider::slide(.x = pretrain_data_{.x}, .f = ~ mean(.x, na.rm = T), .before = 200)")
  ) %>%
  unlist() %>%
  paste(collapse = ",")

rolling_pre_train_code <-
  glue::glue("training_data = training_data %>% mutate({rolling_pre_train_code})")

eval(parse(text = rolling_pre_train_code))

lm_form <- create_lm_formula_interact(
  dependant = dependant_var,
  independant = c(sig_coefs),
  interacts = interact_list
)

temp <- lm(formula = lm_form,
           data = training_data)

summary(temp)
