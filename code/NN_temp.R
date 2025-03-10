
set.seed(333)

testing_data <- testing_data %>%
  mutate(
    bin_dat = ifelse(change_var < 0, 0, 1)
  )

testing_data_train <- testing_data %>%
  slice_head(n = 1000)
testing_data_test <- testing_data %>%
  slice_tail(n = 1250)
testing_data_ML <- testing_data %>%
  slice_tail(n = 750)

remove_spaces_in_names <- names(testing_data_train) %>%
  map(~ str_replace_all(.x," ", "_") %>% str_trim()) %>% unlist() %>% as.character()

names(testing_data_train) <- remove_spaces_in_names
names(testing_data_test) <- remove_spaces_in_names
names(testing_data_ML) <- testing_data_ML

reg_vars <- names(testing_data_train) %>%
  keep(~ .x != "date" & .x != "Price"& .x != "Open" &
         .x != "High" & .x != "Low" & .x != "Change_%" &
         .x != "daily_change" & .x!= "change_var" & .x!= "bin_dat")
macro_vars <- reg_vars %>%
  keep(~ !str_detect(.x, "lagged"))%>%
  keep(~ .x != "ma" & .x != "ma2" & .x != "ma3" &
         .x != "sdma" & .x != "sdma2" & .x != "sdma3")

dependant_var <- "change_var"

testing_data_train <- testing_data_train %>%
  mutate(
    across(matches(macro_vars), .fns = ~ scale(.))
  )

testing_data_test <- testing_data_test %>%
  mutate(
    across(matches(macro_vars), .fns = ~ scale(.))
  )


reg_formula <- create_lm_formula_no_space(dependant = dependant_var, independant = reg_vars)

n <- neuralnet::neuralnet(reg_formula ,
               data = testing_data_train,
               hidden = 20,
               err.fct = "sse",
               linear.output = TRUE,
               lifesign = 'full',
               rep = 2,
               algorithm = "rprop+",
               stepmax = 1900000)

prediction_nn <- compute(n, rep = 2, testing_data_test %>%
                           dplyr::select(matches(reg_vars, ignore.case = FALSE)))
prediction_nn$net.result

lm_reg <- lm(data = testing_data_train, formula = reg_formula)

summary(lm_reg)

trade_results_NN <- testing_data_test %>%
  mutate(
    pred = round(prediction_nn$net.result %>% as.numeric(), 5),
    pred_lm = round(predict.lm(lm_reg, testing_data_test), 4),
    trade = case_when(
      pred < 0 & change_var<0 ~ abs(change_var),
      pred < 0 & change_var>0 ~ -abs(change_var),

      pred > 0 & change_var>0 ~ abs(change_var),
      pred > 0 & change_var >0 ~ -abs(change_var),
    )
  ) %>%
  mutate(
    month_date = lubridate::floor_date(date, "month")
  ) %>%
  group_by(month_date) %>%
  mutate(
    monthly_return = sum(trade, na.rm = T)
  ) %>%
  mutate(
    Asset = asset_name,
    change_var = round(change_var, 5)
  )
