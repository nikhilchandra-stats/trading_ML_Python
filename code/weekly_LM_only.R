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
)
lm_reg <- lm(data = testing_data_train, formula = reg_formula_lm)
predict.lm(lm_reg, testing_data_test)
summary(lm_reg)


asset_data_daily_raw <- fs::dir_info("C:/Users/Nikhil Chandra/Documents/Asset Data/Futures/") %>%
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
  left_join(mean_LM_value)


sd_factor_low <- seq(0, 40, 1)
sd_factor_high <- seq(0, 40, 1)
prof_factor <- seq(1,15,1)
loss_factor <- seq(1,15,1)

all_sd_factors <- sd_factor_low %>%
  map_dfr(
    ~  tibble(sd_factor_high = sd_factor_high) %>%
          mutate(
            sd_factor_low = .x
          )
  ) %>%
  filter(sd_factor_high != sd_factor_low) %>%
  filter(sd_factor_high > sd_factor_low)

profit_loss_factors <- prof_factor %>%
  map_dfr(
    ~  tibble(loss_factor = loss_factor) %>%
      mutate(
        prof_factor = .x
      )
  ) %>%
  filter(prof_factor == loss_factor)

mean_values_by_asset_for_loop =
  wrangle_asset_data(
    asset_data_daily_raw = asset_data_daily_raw,
    summarise_means = TRUE
  )
c = 0

get_trade_tags_weekly_LM <- function(trade_data_for_tagging = trade_with_daily_data,
                                     sd_factor_low = sd_factor_low ,
                                     sd_factor_high = sd_factor_high) {


  trade_data_for_tagging %>%
               mutate(
                 trade_col =
                   case_when(

                     between(Pred_trade,mean_value  + sd_value*low_fac,  mean_value  + sd_value*high_fac) ~ "Long",
                     between(Pred_trade,mean_value  - sd_value*high_fac,  mean_value  - sd_value*low_fac) ~ "Short"

                   )
               )

}

accumualtor <- list()

for (j in 1:dim(profit_loss_factors)) {

  stop_fac <- profit_loss_factors[j,1] %>% pull(loss_factor ) %>% as.numeric()
  prof_fac <- profit_loss_factors[j,2] %>% pull(prof_factor ) %>% as.numeric()

  for (i in 1:dim(all_sd_factors)[1] ) {

    c = c + 1
    low_fac <- all_sd_factors[i,2] %>% pull(sd_factor_low) %>% as.numeric()
    high_fac <- all_sd_factors[i,1] %>% pull(sd_factor_high) %>% as.numeric()

    temp_for_trade <- trade_with_daily_data %>%
      mutate(
        trade_col =
          case_when(

            between(Pred_trade,mean_value  + sd_value*low_fac,  mean_value  + sd_value*high_fac) ~ "Long",
            between(Pred_trade,mean_value  - sd_value*high_fac,  mean_value  - sd_value*low_fac) ~ "Short"

          )
      )

    results_temp <- generic_trade_finder(
      tagged_trades = temp_for_trade,
      asset_data_daily_raw = asset_data_daily_raw,
      stop_factor = stop_fac,
      profit_factor = prof_fac,
      trade_col = "trade_col",
      date_col = "Date",
      max_hold_period = 100,
      start_price_col = "Price",
      mean_values_by_asset = mean_values_by_asset_for_loop,
      return_summary = TRUE
    ) %>%
      map_dfr(
        ~ .x %>% mutate(
          sd_factor_high = high_fac,
          sd_factor_low = low_fac
        )
      )

    accumualtor[[c]] <- results_temp

  }

}

store_already_looped <- accumualtor
#stopped point
i = 622
j = 10


#--------------------------
db_con <- connect_db("C:/Users/Nikhil Chandra/Documents/trade_data/weekly_LM_trade_sim.db")

trade_frame <- accumualtor %>% map_dfr(bind_rows)

write_table_sql_lite(.data = trade_frame,
                     table_name = "weekly_LM_trade_sim",
                     conn = db_con,
                     overwrite_true = TRUE)

trade_frame_2 <- trade_frame %>%
  group_by(sd_factor_high, sd_factor_low,
           trade_direction,
           profit_factor,
           stop_factor, trade_category) %>%
  summarise(
    Trades = sum(Trades, na.rm = T)
  )%>%
  group_by(sd_factor_high, sd_factor_low,
           trade_direction,
           profit_factor,
           stop_factor) %>%
  mutate(
    Total_Trades = sum(Trades, na.rm = T)
  ) %>%
  mutate(
    Perc = round(Trades/Total_Trades, 5)
  ) %>%
  filter(Total_Trades >= 400)

trade_frame_3 <- trade_frame_2 %>%
  dplyr::select(
    sd_factor_high, sd_factor_low,
    trade_direction,
    profit_factor,
    stop_factor,
    trade_category,
    Total_Trades,
    Perc
  ) %>%
  pivot_wider(names_from = trade_category, values_from = Perc)

trade_frame_4 <- trade_frame_2 %>%
  dplyr::select(
    sd_factor_high, sd_factor_low,
    trade_direction,
    profit_factor,
    stop_factor,
    trade_category,
    Total_Trades,
    Perc
  ) %>%
  pivot_wider(names_from = trade_category, values_from = Perc) %>%
  group_by(
    trade_direction
  ) %>%
  filter(`TRUE WIN` >= 0.55) %>%
  group_by(
    trade_direction
  ) %>%
  slice_max(Total_Trades)
