helperfunctions35South::load_custom_functions()
one_drive_path <- helperfunctions35South::create_one_drive_path(
  path_extension = "raw data")
library(neuralnet)



raw_macro_data <- get_macro_event_data()

eur_data <- get_EUR_exports()

AUD_exports_total <- get_AUS_exports()  %>%
  pivot_longer(-TIME_PERIOD, names_to = "category", values_to = "Aus_Export") %>%
  rename(date = TIME_PERIOD) %>%
  group_by(date) %>%
  summarise(Aus_Export = sum(Aus_Export, na.rm = T))

USD_exports_total <- get_US_exports()  %>%
  pivot_longer(-date, names_to = "category", values_to = "US_Export") %>%
  group_by(date) %>%
  summarise(US_Export = sum(US_Export, na.rm = T)) %>%
  left_join(AUD_exports_total) %>%
  ungroup()

USD_exports_total <- USD_exports_total %>%
  mutate(
    month_date = lubridate::floor_date(date, "month")
  )

AUD_exports_total <- AUD_exports_total %>%
  mutate(
    month_date = lubridate::floor_date(date, "month")
  )

asset_data_combined <- fs::dir_info("C:/Users/Nikhil Chandra/Documents/Asset Data/Futures/") %>%
  mutate(asset_name =
           str_remove(path, "C\\:\\/Users/Nikhil Chandra\\/Documents\\/Asset Data\\/Futures\\/") %>%
           str_remove("\\.csv") %>%
           str_remove("Historical Data")%>%
           str_remove("Stock Price") %>%
           str_remove("History")
  ) %>%
  dplyr::select(path, asset_name) %>%
  split(.$asset_name, drop = FALSE) %>%
  map_dfr( ~ read_csv(.x[1,1] %>% as.character()) %>%
             transform_asset_to_weekly()  %>%
             mutate(Asset = .x[1,2] %>% as.character())
  )

trading_dat <- asset_data_combined

macro_us <- transform_macro_to_monthly(
  macro_dat_for_transform = get_USD_Indicators(raw_macro_data = raw_macro_data),
  transform_to_week = TRUE)
macro_eur <- transform_macro_to_monthly(
  macro_dat_for_transform = get_EUR_Indicators(raw_macro_data = raw_macro_data),
  transform_to_week = TRUE)
macro_jpy <- transform_macro_to_monthly(
  macro_dat_for_transform = get_JPY_Indicators(raw_macro_data = raw_macro_data),
  transform_to_week = TRUE)
macro_aud <- transform_macro_to_monthly(
  macro_dat_for_transform = get_AUS_Indicators(raw_macro_data = raw_macro_data),
  transform_to_week = TRUE)
Macro_CNY <- transform_macro_to_monthly(
  get_CNY_Indicators(raw_macro_data = raw_macro_data),
  transform_to_week = TRUE)
Macro_GBP <- transform_macro_to_monthly(
  macro_dat_for_transform = get_GBP_Indicators(raw_macro_data = raw_macro_data),
  transform_to_week = TRUE)
macro_cad <- transform_macro_to_monthly(
  get_CAD_Indicators(raw_macro_data = raw_macro_data),
  transform_to_week = TRUE)

EUR_trade2 <- get_EUR_exports() %>% dplyr::select(month_date = date, `EUR Export Total`) %>%
  mutate(
    `EUR Export Total` = `EUR Export Total` - lag(`EUR Export Total`)
  ) %>% filter(!is.na(`EUR Export Total`)) %>%
  mutate(month_date = month_date + months(1)) %>%
  mutate(week_date = lubridate::floor_date(month_date, "week")) %>%
  dplyr::select(-month_date)

USD_exports_total2 <- USD_exports_total %>%
  mutate(
    US_Export = US_Export - lag(US_Export),
    Aus_Export  = Aus_Export  - lag(Aus_Export )
  ) %>%
  filter(!is.na(US_Export), !is.na(Aus_Export)) %>%
  mutate(month_date = month_date + months(1)) %>%
  dplyr::select(-date)  %>%
  mutate(week_date = lubridate::floor_date(month_date, "week")) %>%
  dplyr::select(-month_date)

testing_data <- trading_dat %>%
  left_join(macro_us) %>%
  left_join(macro_eur)%>%
  left_join(macro_jpy)%>%
  left_join(macro_aud) %>%
  left_join(Macro_CNY) %>%
  left_join(Macro_GBP) %>%
  left_join(macro_cad) %>%
  left_join(EUR_trade2) %>%
  left_join(USD_exports_total2) %>%
  # left_join(AUD_exports_total2) %>%
  # filter(!is.na(`USD Monthly Budget Statement`)) %>%
  # filter(!is.na(`EUR Export Total`)) %>%
  group_by(Asset) %>%
  arrange(week_date, .by_group = TRUE) %>%
  group_by(Asset) %>%
  mutate(
    lagged_var = Week_Change_lag,
    lagged_var2 = lag(Week_Change_lag),
    lagged_var3 = lag(Week_Change_lag, 2),
    ma3 = slider::slide_dbl(.x = lagged_var, .f = ~ mean(.x, na.rm = T), .before = 3),
    sd3 = slider::slide_dbl(.x = lagged_var, .f = ~ sd(.x, na.rm = T), .before = 3)
  ) %>%
  filter(!is.na(lagged_var),
         !is.na(ma3),
         !is.na(lagged_var3)
  ) %>%
  group_by(Asset) %>%
  fill(where(is.numeric), .direction = "down") %>%
  group_by(Asset) %>%
  fill(where(is.numeric), .direction = "up") %>%
  ungroup() %>%
  # mutate(
  #   EUR_check = ifelse(str_detect(Asset, "EUR"), 1, 0),
  #   AUD_check = ifelse(str_detect(Asset, "AUD"), 1, 0),
  #   USD_check = ifelse(str_detect(Asset, "USD"), 1, 0),
  #   GBP_check = ifelse(str_detect(Asset, "GBP"), 1, 0),
  #   JPY_check = ifelse(str_detect(Asset, "JPY"), 1, 0),
  #   CNY_check = ifelse(str_detect(Asset, "CNY"), 1, 0),
  #   CAD_check = ifelse(str_detect(Asset, "CAD"), 1, 0)
  # ) %>%
  mutate(
    bin_dat = case_when(
      Week_Change >= 0 ~ 1,
      Week_Change < 0 ~ 0
    )
  ) %>%
  filter(if_all(everything(), ~ !is.na(.)))

testing_data_train <- testing_data %>%
  group_by(Asset) %>%
  slice_head(n = 300) %>%
  ungroup()
testing_data_test <- testing_data %>%
  group_by(Asset) %>%
  slice_tail(n = 250)%>%
  ungroup()

remove_spaces_in_names <- names(testing_data_train) %>%
  map(~ str_replace_all(.x," ", "_") %>% str_trim()) %>% unlist() %>% as.character()

names(testing_data_train) <- remove_spaces_in_names
names(testing_data_test) <- remove_spaces_in_names

reg_vars <- names(testing_data_train) %>%
  keep(~ .x != "date" & .x != "Price"& .x != "Open" &
         .x != "High" & .x != "Low" & .x != "Change_%" &
         .x != "daily_change" & .x!= "change_var" & .x!= "bin_dat" &
         .x != "week_date" & .x != "week_start_price" &
         .x != "weekly_forward_return" & .x != "Week_Change" & .x != "Asset" &
         .x != "Week_Change_lag" & .x != "sd3")
macro_vars <- reg_vars %>%
  keep(~ !str_detect(.x, "lagged"))%>%
  keep(~ .x != "ma" & .x != "ma2" & .x != "ma3" & .x != "week_date" & .x != "week_start_price" &
         .x != "sdma" & .x != "sdma2" & .x != "sdma3" &
         .x != "weekly_forward_return" & .x != "Week_Change" & .x != "Asset" &
         .x != "Week_Change_lag" & .x != "sd3" & .x != "bin_dat")

dependant_var <- "Week_Change"

reg_formula <- create_lm_formula_no_space(dependant = dependant_var, independant = reg_vars)

reg_formula_lm <-  create_lm_formula_no_space(dependant = dependant_var,
                                              independant = reg_vars
                                              # keep(~ !str_detect(.x, "CAD_") &
                                              #        !str_detect(.x, "_check")
                                              #      ) %>%
                                              # unlist() %>% as.character()
)
lm_reg <- lm(data = testing_data_train, formula = reg_formula_lm)
predict.lm(lm_reg, testing_data_test)
summary(lm_reg)

safely_n_net <- safely(neuralnet::neuralnet, otherwise = NULL)

#----------------Notes
# Best model so far has been hidden = c(100) and all data logged and then
# first differenced.

n <- safely_n_net(reg_formula ,
                  data = testing_data_train,
                  hidden = c(110),
                  err.fct = "sse",
                  linear.output = TRUE,
                  lifesign = 'full',
                  rep = 1,
                  algorithm = "rprop+",
                  stepmax = 15*(10^6),
                  threshold = 0.1) %>%
  pluck('result')

prediction_nn <- compute(n, rep = 1, testing_data_test %>%
                           dplyr::select(matches(reg_vars, ignore.case = FALSE)))
prediction_nn$net.result

asset_data_daily <- fs::dir_info("C:/Users/Nikhil Chandra/Documents/Asset Data/Futures/") %>%
  mutate(asset_name =
           str_remove(path, "C\\:\\/Users/Nikhil Chandra\\/Documents\\/Asset Data\\/Futures\\/") %>%
           str_remove("\\.csv") %>%
           str_remove("Historical Data")%>%
           str_remove("Stock Price") %>%
           str_remove("History")
  ) %>%
  dplyr::select(path, asset_name) %>%
  split(.$asset_name, drop = FALSE) %>%
  map_dfr( ~ read_csv(.x[1,1] %>% as.character()) %>%
             mutate(Asset = .x[1,2] %>% as.character())
  )

asset_data_daily <- asset_data_daily_raw %>%
  mutate(Date = as.Date(Date, format =  "%m/%d/%Y"))

raw_LM_trade_df <- testing_data_test %>%
  dplyr::select(Asset, week_date) %>%
  mutate(
    LM_pred = predict.lm(lm_reg, testing_data_test) %>% as.numeric()
  )

mean_LM_value <-
  testing_data_train %>%
  dplyr::select(Asset, week_date) %>%
  mutate(
    LM_pred = predict.lm(lm_reg, testing_data_train) %>% as.numeric()
  ) %>%
  group_by(Asset) %>%
  summarise(
    mean_value = mean(LM_pred, na.rm = T),
    sd_value = sd(LM_pred, na.rm = T)
  )


trade_with_daily_data <- asset_data_daily %>%
  mutate(
    week_date = lubridate::floor_date(Date, "week")
  ) %>%
  left_join(raw_LM_trade_df, by = c("week_date", "Asset")) %>%
  group_by(Asset) %>%
  arrange(Date, .by_group = TRUE) %>%
  group_by(Asset) %>%
  mutate(
    Pred_Filled = LM_pred
  ) %>%
  group_by(Asset) %>%
  fill(Pred_Filled, .direction = "down") %>%
  ungroup() %>%
  mutate(
    new_week_date = lubridate::floor_date(Date, "week",week_start = 1)
  ) %>%
  mutate(
    Pred_trade =
      case_when(
        new_week_date == Date ~ LM_pred
      )
  ) %>%
  filter(!is.na(Pred_Filled)) %>%
  left_join(mean_LM_value) %>%
  mutate(
    trade_col =
      case_when(

        between(Pred_trade,mean_value  + sd_value*2,  mean_value  + sd_value*3) ~ "Long",
        between(Pred_trade,mean_value  + sd_value*0,  mean_value  + sd_value*1) ~ "Long",
        # between(Pred_trade,mean_value  - sd_value*1,  mean_value  - sd_value*0) ~ "Long",

        between(Pred_trade,mean_value  - sd_value*200,  mean_value  - sd_value*20) ~ "Short",
        between(Pred_trade,mean_value  + sd_value*15,  mean_value  + sd_value*100) ~ "Long",

        # between(Pred_trade,mean_value  - sd_value*50,  mean_value  - sd_value*10) ~ "Long"
      )
  )

NN_mean <-
  testing_data_train %>% dplyr::select(Asset) %>%
  mutate(
    NN_dat =   compute(n, rep = 1, testing_data_train %>%
                         dplyr::select(matches(reg_vars, ignore.case = FALSE))) %>%
      pluck("net.result") %>% as.numeric(),
    LM_dat = predict.lm(lm_reg, testing_data_train) %>% as.numeric()
  ) %>%
  group_by(Asset) %>%
  summarise(NN_mean = mean(NN_dat, na.rm =T),
            NN_sd = sd(NN_dat, na.rm =T),
            LM_mean = mean(LM_dat, na.rm = T),
            LM_sd = sd(LM_dat, na.rm = T))



summary(lm_reg)

analyse_trade_condition <- function(testing_data = testing_data_test,
                                    nn_model = prediction_nn,
                                    lm_model = lm_reg,
                                    NN_mean = NN_mean,
                                    sd_factor_LM_short = 60,
                                    sd_factor_LM_long = 75,
                                    sd_factor_NN_short = 100,
                                    sd_factor_NN_long = 50,
                                    date_col = "week_date"
) {

  trade_results_NN <- testing_data %>%
    mutate(
      pred = round(nn_model$net.result %>% as.numeric(), 5),
      pred_lm = round(predict.lm(lm_model, testing_data_test), 4)
    ) %>%
    left_join(NN_mean) %>%
    mutate(
      short_long_NN = case_when(
        pred > 0 ~  "long",
        pred <0 ~ "short"
      ),

      short_long_NN_strong  = case_when(
        pred > NN_mean + NN_sd*sd_factor_NN_long  ~  "long",
        pred < NN_mean - NN_sd*sd_factor_NN_short ~ "short"
      ),

      short_long_LM = case_when(
        pred_lm > 0 ~  "long",
        pred_lm <0 ~ "short"
      ),

      short_long_LM_strong = case_when(
        pred_lm > LM_mean + LM_sd*sd_factor_LM_long ~  "long",
        pred_lm < LM_mean - LM_sd*sd_factor_LM_short ~ "short"
      ),

      short_long_LM_NN = case_when(
        pred_lm > 0 & pred > 0 ~  "long",
        pred_lm <0 & pred <0 ~ "short"
      ),

      short_long_LM_NN_Anti = case_when(
        pred_lm > 0 & pred < 0 ~  "long",
        pred_lm <0 & pred >0 ~ "short"
      ),

      short_long_LM_NN_AVG = case_when(
        (pred_lm + pred)/2 > 0  ~  "long",
        (pred_lm + pred)/2 <0  ~ "short"
      ),

      strong_synergy  =
        case_when(
          short_long_LM_strong == "long" & short_long_NN_strong == "long" ~ "long",
          short_long_LM_strong == "short" & short_long_NN_strong == "short" ~ "short",
        ),

      strong_antogonist1  =
        case_when(
          short_long_LM_strong == "short" & short_long_NN_strong == "long" ~ "long",
          short_long_LM_strong == "long" & short_long_NN_strong == "short" ~ "short",
        )

    ) %>%
    mutate(
      wins_LM =
        case_when(
          short_long_LM == "short" & !!as.name(dependant_var) < 0 ~ 1,
          short_long_LM == "long" & !!as.name(dependant_var) > 0 ~ 1,

          short_long_LM == "short" & !!as.name(dependant_var) > 0 ~ 0,
          short_long_LM == "long" & !!as.name(dependant_var) < 0 ~ 0
        ),

      wins_NN_strong =
        case_when(
          short_long_NN_strong == "short" & !!as.name(dependant_var) < 0 ~ 1,
          short_long_NN_strong == "long" & !!as.name(dependant_var) > 0 ~ 1,

          short_long_NN_strong == "short" & !!as.name(dependant_var) > 0 ~ 0,
          short_long_NN_strong == "long" & !!as.name(dependant_var) < 0 ~ 0
        ),

      wins_NN =
        case_when(
          short_long_NN == "short" & !!as.name(dependant_var) < 0 ~ 1,
          short_long_NN == "long" & !!as.name(dependant_var) > 0 ~ 1,

          short_long_NN == "short" & !!as.name(dependant_var) > 0 ~ 0,
          short_long_NN == "long" & !!as.name(dependant_var) < 0 ~ 0
        ),

      wins_LM_strong =
        case_when(
          short_long_LM_strong == "short" & !!as.name(dependant_var) < 0 ~ 1,
          short_long_LM_strong == "long" & !!as.name(dependant_var) > 0 ~ 1,

          short_long_LM_strong == "short" & !!as.name(dependant_var) > 0 ~ 0,
          short_long_LM_strong == "long" & !!as.name(dependant_var) < 0 ~ 0
        ),

      wins_LM_LM_NN_Anti =
        case_when(
          short_long_LM_NN_Anti == "short" & !!as.name(dependant_var) < 0 ~ 1,
          short_long_LM_NN_Anti == "long" & !!as.name(dependant_var) > 0 ~ 1,

          short_long_LM_NN_Anti == "short" & !!as.name(dependant_var) > 0 ~ 0,
          short_long_LM_NN_Anti == "long" & !!as.name(dependant_var) < 0 ~ 0
        ),

      wins_LM_NN =
        case_when(
          short_long_LM_NN == "short" & !!as.name(dependant_var) < 0 ~ 1,
          short_long_LM_NN == "long" & !!as.name(dependant_var) > 0 ~ 1,

          short_long_LM_NN == "short" & !!as.name(dependant_var) > 0 ~ 0,
          short_long_LM_NN == "long" & !!as.name(dependant_var) < 0 ~ 0
        ),
      wins_strong_synergy =
        case_when(
          strong_synergy == "short" & !!as.name(dependant_var) < 0 ~ 1,
          strong_synergy == "long" & !!as.name(dependant_var) > 0 ~ 1,

          strong_synergy == "short" & !!as.name(dependant_var) > 0 ~ 0,
          strong_synergy == "long" & !!as.name(dependant_var) < 0 ~ 0
        ),

      wins_strong_antogonist1 =
        case_when(
          strong_antogonist1 == "short" & !!as.name(dependant_var) < 0 ~ 1,
          strong_antogonist1 == "long" & !!as.name(dependant_var) > 0 ~ 1,

          strong_antogonist1 == "short" & !!as.name(dependant_var) > 0 ~ 0,
          strong_antogonist1 == "long" & !!as.name(dependant_var) < 0 ~ 0
        )

    )

  control <- trade_results_NN %>%
    select(!!as.name(dependant_var), !!as.name(date_col) ) %>%
    mutate(
      short_long =
        case_when(
          !!as.name(dependant_var) < 0 ~ "short",
          !!as.name(dependant_var) > 0 ~ "long"
        )
    ) %>%
    group_by(short_long) %>%
    summarise(
      Trades_control = n()
    ) %>%
    mutate(
      Perc_control = Trades_control/sum(Trades_control)
    )

  returned <-
    list(

      trade_results_NN %>%
        filter(!is.na(short_long_LM_strong)) %>%
        group_by(short_long_LM_strong) %>%
        summarise(
          total_trades = n(),
          wins = sum(wins_LM_strong, na.rm = T)
        ) %>%
        mutate(
          perc = wins/total_trades
        )%>%
        left_join(control, by = c("short_long_LM_strong" = "short_long")) %>%
        mutate(
          sd_factor_LM_short = sd_factor_LM_short,
          sd_factor_LM_long = sd_factor_LM_long,
          sd_factor_NN_short = sd_factor_NN_short,
          sd_factor_NN_long = sd_factor_NN_long
        ) %>%
        rename(
          long_short = short_long_LM_strong
        ) %>%
        mutate(
          trade_type = "short_long_LM_strong"
        ),

      trade_results_NN %>%
        filter(!is.na(short_long_NN_strong)) %>%
        group_by(short_long_NN_strong) %>%
        summarise(
          total_trades = n(),
          wins = sum(wins_NN_strong, na.rm = T)
        ) %>%
        mutate(
          perc = wins/total_trades
        )%>%
        left_join(control, by = c("short_long_NN_strong" = "short_long")) %>%
        mutate(
          sd_factor_LM_short = sd_factor_LM_short,
          sd_factor_LM_long = sd_factor_LM_long,
          sd_factor_NN_short = sd_factor_NN_short,
          sd_factor_NN_long = sd_factor_NN_long
        ) %>%
        rename(
          long_short = short_long_NN_strong
        ) %>%
        mutate(
          trade_type = "short_long_NN_strong"
        ),

      trade_results_NN %>%
        filter(!is.na(strong_synergy)) %>%
        group_by(strong_synergy) %>%
        summarise(
          total_trades = n(),
          wins = sum(wins_strong_synergy, na.rm = T)
        ) %>%
        mutate(
          perc = wins/total_trades
        )%>%
        left_join(control, by = c("strong_synergy" = "short_long")) %>%
        mutate(
          sd_factor_LM_short = sd_factor_LM_short,
          sd_factor_LM_long = sd_factor_LM_long,
          sd_factor_NN_short = sd_factor_NN_short,
          sd_factor_NN_long = sd_factor_NN_long
        ) %>%
        rename(
          long_short = strong_synergy
        ) %>%
        mutate(
          trade_type = "strong_synergy"
        ),

      trade_results_NN %>%
        filter(!is.na(strong_antogonist1)) %>%
        group_by(strong_antogonist1) %>%
        summarise(
          total_trades = n(),
          wins = sum(wins_strong_antogonist1, na.rm = T)
        ) %>%
        mutate(
          perc = wins/total_trades
        )%>%
        left_join(control, by = c("strong_antogonist1" = "short_long")) %>%
        mutate(
          sd_factor_LM_short = sd_factor_LM_short,
          sd_factor_LM_long = sd_factor_LM_long,
          sd_factor_NN_short = sd_factor_NN_short,
          sd_factor_NN_long = sd_factor_NN_long
        ) %>%
        rename(
          long_short = strong_antogonist1
        ) %>%
        mutate(
          trade_type = "strong_antogonist1"
        )

    )

  return(returned)

}


test_analysis_control <- analyse_trade_condition(
  testing_data = testing_data_test,
  nn_model = prediction_nn,
  lm_model = lm_reg,
  NN_mean = NN_mean,
  sd_factor_LM_short = 0,
  sd_factor_LM_long = 20,
  sd_factor_NN_short = 0,
  sd_factor_NN_long = 2,
  date_col = "week_date"
) %>% map_dfr(bind_rows)

sd_factor_LM_short <- seq(5,160,20)
sd_factor_LM_long <- seq(5,160,20)
sd_factor_NN_short <- seq(5,160,20)
sd_factor_NN_long <- seq(5,160,20)

analysis_data <- list()
c = 0

for (i in 1:length(sd_factor_LM_short)) {

  for (j in 1:length(sd_factor_LM_long)) {
    for (k in 1:length(sd_factor_NN_short)) {

      for (o in 1:length(sd_factor_NN_long)) {

        c = c + 1
        analysis_data[[c]] <-
          analyse_trade_condition(
            testing_data = testing_data_test,
            nn_model = prediction_nn,
            lm_model = lm_reg,
            NN_mean = NN_mean,
            sd_factor_LM_short = sd_factor_LM_short[i],
            sd_factor_LM_long = sd_factor_LM_long[j],
            sd_factor_NN_short = sd_factor_NN_short[k],
            sd_factor_NN_long = sd_factor_NN_long[o]
          ) %>% map_dfr(bind_rows)

      }
    }
  }
}

plot_data_raw <- analysis_data %>%
  map_dfr(bind_rows) %>%
  group_by(trade_type,long_short, sd_factor_LM_short ,
           sd_factor_LM_long, sd_factor_NN_short,sd_factor_NN_long ) %>%
  summarise(  perc  = mean(perc, na.rm = T),
              total_trades = mean(total_trades, na.rm = T)) %>%
  ungroup()

plot_data <- plot_data_raw %>%
  filter(total_trades > 50)

plot_data %>% ungroup() %>%  distinct(trade_type)

plot_data %>%
  filter(long_short == "long") %>%
  filter(trade_type == "short_long_NN_strong"
         # trade_type == "strong_antogonist1"|
         # trade_type == "strong_synergy"
  ) %>%
  ggplot(aes(x = sd_factor_NN_long, y = perc, color = long_short)) +
  geom_line() +
  # facet_wrap(.~trade_type, scales = "free") +
  theme_minimal() +
  theme(legend.position = "bottom")

plot_data %>%
  filter(long_short == "short") %>%
  filter(trade_type == "short_long_NN_strong"
         # trade_type == "strong_antogonist1"|
         # trade_type == "strong_synergy"
  ) %>%
  ggplot(aes(x = sd_factor_NN_short, y = perc, color = long_short)) +
  geom_line() +
  # facet_wrap(.~trade_type, scales = "free") +
  theme_minimal() +
  theme(legend.position = "bottom")

plot_data %>%
  # filter(long_short == "long") %>%
  filter(trade_type == "strong_antogonist1"
         # trade_type == "strong_antogonist1"|
         # trade_type == "strong_synergy"
  ) %>%
  ggplot(aes(x =sd_factor_LM_long  , y = sd_factor_LM_long, color = perc)) +
  geom_point() +
  facet_wrap(.~long_short, scales = "free") +
  theme_minimal() +
  scale_color_continuous(type = "viridis") +
  theme(legend.position = "bottom")

plot_data %>%
  # filter(long_short == "long") %>%
  filter(trade_type == "strong_antogonist1"
         # trade_type == "strong_antogonist1"|
         # trade_type == "strong_synergy"
  ) %>%
  ggplot(aes(x =sd_factor_LM_short  , y = sd_factor_LM_short, color = perc)) +
  geom_point() +
  facet_wrap(.~long_short, scales = "free") +
  theme_minimal() +
  scale_color_continuous(type = "viridis") +
  theme(legend.position = "bottom")

plot_data %>%
  filter(trade_type == "strong_synergy"
         # trade_type == "strong_antogonist1"|
         # trade_type == "strong_synergy"
  ) %>%
  mutate(
    sd_factor_ratio = sd_factor_LM_long/sd_factor_LM_short
  ) %>%
  ggplot(aes(x =sd_factor_LM_short  , y = sd_factor_LM_short, color = perc)) +
  geom_point() +
  facet_wrap(.~long_short, scales = "free") +
  theme_minimal() +
  scale_color_continuous(type = "viridis") +
  theme(legend.position = "bottom")


max_values <- plot_data %>%
  group_by(trade_type, long_short) %>%
  slice_max(perc) %>%
  group_by(trade_type, long_short) %>%
  slice_max(total_trades) %>%
  group_by(trade_type, long_short) %>%
  slice_head(n = 1) %>%
  left_join(test_analysis_control %>%
              dplyr::distinct(long_short,
                              Trades_control,Perc_control)
  )

max_values <- plot_data_raw %>%
  filter(perc > 0.55)  %>%
  group_by(trade_type, long_short) %>%
  slice_max(total_trades)  %>%
  group_by(trade_type, long_short) %>%
  slice_head(n = 1) %>%
  left_join(test_analysis_control %>%
              dplyr::distinct(long_short,
                              Trades_control,Perc_control)
  )
#---------------------------------------------------
control <- trade_results_NN %>%
  # filter(str_detect(Asset, "\\_")) %>%
  filter(str_detect(Asset, "GBP|AUD|USD|EUR|CAD|CNY")) %>%
  select(!!as.name(dependant_var), month_date) %>%
  mutate(
    short_long =
      case_when(
        !!as.name(dependant_var) < 0 ~ "short",
        !!as.name(dependant_var) > 0 ~ "long"
      )
  ) %>%
  group_by(short_long) %>%
  summarise(
    Trades_control = n()
  ) %>%
  mutate(
    Perc_control = Trades_control/sum(Trades_control)
  )

all_results <-
  list(

    trade_results_NN %>%
      filter(str_detect(Asset, "GBP|AUD|USD|EUR|CAD|CNY")) %>%
      filter(!is.na(short_long_NN_strong)) %>%
      group_by(short_long_NN_strong) %>%
      summarise(
        total_trades = n(),
        wins = sum(wins_NN_strong, na.rm = T)
      ) %>%
      mutate(
        perc = wins/total_trades
      )%>%
      left_join(control, by = c("short_long_NN_strong" = "short_long")),

    trade_results_NN %>%
      filter(str_detect(Asset, "GBP|AUD|USD|EUR|CAD|CNY")) %>%
      filter(!is.na(short_long_LM_strong)) %>%
      group_by(short_long_LM_strong) %>%
      summarise(
        total_trades = n(),
        wins = sum(wins_LM_strong, na.rm = T)
      ) %>%
      mutate(
        perc = wins/total_trades
      )%>%
      left_join(control, by = c("short_long_LM_strong" = "short_long"))

  )

all_results
