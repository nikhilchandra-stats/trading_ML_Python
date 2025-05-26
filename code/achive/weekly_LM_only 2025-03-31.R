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
             transform_asset_to_weekly(filt_NA_lead_values = FALSE)  %>%
             mutate(Asset = .x[1,2] %>% as.character())
  )

reg_data_list <- run_reg_weekly_variant(
  raw_macro_data = raw_macro_data,
  eur_data = eur_data,
  AUD_exports_total = AUD_exports_total,
  USD_exports_total = USD_exports_total,
  asset_data_combined = asset_data_combined
)

regression_prediction <- reg_data_list[[2]]

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
raw_LM_trade_df <- reg_data_list[[2]]

LM_preped <- prep_LM_wkly_trade_data(
  asset_data_daily_raw = asset_data_daily_raw,
  raw_LM_trade_df = reg_data_list[[2]],
  raw_LM_trade_df_training = reg_data_list[[3]]
)

trade_with_daily_data <- LM_preped %>% pluck("LM Merged to Daily")

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
c = 5987 - 1
trade_with_daily_data <- LM_preped %>% pluck("LM Merged to Daily")

for (j in 1:dim(profit_loss_factors)[1] ) {

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
i = 695
j = 242


#--------------------------
db_con <- connect_db("C:/Users/Nikhil Chandra/Documents/trade_data/weekly_LM_trade_sim.db")

trade_frame <- accumualtor %>% map_dfr(bind_rows)

write_table_sql_lite(.data = trade_frame,
                     table_name = "weekly_LM_trade_sim_with_binary",
                     conn = db_con,
                     overwrite_true = TRUE)

db_con_LM_weekly <- connect_db("C:/Users/Nikhil Chandra/Documents/trade_data/weekly_LM_trade_sim.db")
trade_frame <- DBI::dbGetQuery(conn = db_con_LM_weekly,
                               statement = "SELECT * FROM weekly_LM_trade_sim")
DBI::dbDisconnect(db_con_LM_weekly)
rm(db_con_LM_weekly)
gc()

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
  filter(Total_Trades >= 200)

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
  filter(`TRUE WIN` >= 0.6) %>%
  group_by(
    trade_direction
  ) %>%
  slice_max(Total_Trades) %>%
  group_by(
    trade_direction
  )  %>%
  slice_max(`TRUE WIN`) %>%
  group_by(
    trade_direction
  )  %>%
  slice_max(profit_factor)%>%
  group_by(
    trade_direction
  )  %>%
  slice_min(sd_factor_high)

#-------------------------------Desired Params Long trades = 297, Win = 0.62
# sd_factor_high  = 12
# sd_factor_low  = 6


trade_params <-
  tibble(
    sd_factor_low = c(3,4,5,6,7,8,9,10, 12)
  ) %>%
  mutate(
    sd_factor_high = 200
  )
profit_factor  = 4
stop_factor  = 3
trade_with_daily_data <- LM_preped %>% pluck("LM Merged to Daily")

new_trades_this_week <- list()

for (j in 1:dim(trade_params)[1]) {

  sd_factor_low <- trade_params$sd_factor_low[j] %>% as.numeric()
  sd_factor_high <- trade_params$sd_factor_high[j] %>% as.numeric()

  temp_for_trade <- trade_with_daily_data %>%
    mutate(
      trade_col =
        case_when(

          between(Pred_trade,mean_value  + sd_value*sd_factor_low,  mean_value  + sd_value*sd_factor_high) ~ "Long",
          between(Pred_trade,mean_value  - sd_value*sd_factor_high,  mean_value  - sd_value*sd_factor_low) ~ "Short"

        )
    )

  retest_long <-
    generic_trade_finder_conservative(
      tagged_trades = temp_for_trade,
      asset_data_daily_raw = asset_data_daily_raw,
      stop_factor = 4,
      profit_factor = 4,
      trade_col = "trade_col",
      date_col = "Date",
      max_hold_period = 100,
      start_price_col = "Price",
      mean_values_by_asset = mean_values_by_asset_for_loop,
      return_summary = TRUE
    ) %>%
    map_dfr(
      ~ .x %>% mutate(
        sd_factor_high = 12,
        sd_factor_low = 6
      )
    )

  retest_long_sum <- retest_long %>%
    group_by(trade_category, trade_direction) %>%
    summarise(
      Trades = sum(Trades, na.rm = T)
    ) %>%
    pivot_wider(names_from = trade_category, values_from = Trades) %>%
    mutate(
      Perc = `TRUE WIN`/ (`TRUE LOSS` + `TRUE WIN`)
    )

  trade_with_daily_data <- LM_preped %>% pluck("LM Merged to Daily")

  chance_of_win <- retest_long_sum %>% filter(trade_direction == "Long") %>%
    pull(Perc) %>% as.numeric()

  if( length(chance_of_win) == 0 ) { chance_of_win = 0 }

  new_trades_data_long <- trade_with_daily_data %>%
    mutate(
      trade_col =
        case_when(
          between(Pred_trade,mean_value  + sd_value*sd_factor_low,  mean_value  + sd_value*sd_factor_high) ~ "Long",
          between(Pred_trade,mean_value  - sd_value*sd_factor_high,  mean_value  - sd_value*sd_factor_low) ~ "Short"
        )
    ) %>%
    dplyr::slice_max(Date, n = 1) %>%
    mutate(
      sd_factor_low = sd_factor_low,
      sd_factor_high = sd_factor_high
    ) %>%
    mutate(
      chance_of_win = chance_of_win
    )

  new_trades_this_week[[j]] <- new_trades_data_long

}

new_trades_this_week <- new_trades_this_week %>% map_dfr(bind_rows)

new_trades_this_week_filt <- new_trades_this_week %>%
  filter(trade_col == "Long") %>%
  group_by(Asset) %>%
  slice_max(chance_of_win) %>%
  left_join(mean_values_by_asset_for_loop)  %>%
  mutate(
    across(
      .cols = c(Price, mean_daily, sd_daily),
      .fns = ~ ifelse(
        str_detect(Asset, "Copper"),
        .*1000,
        .
      )
    ),

    across(
      .cols = c(Price, mean_daily, sd_daily),
      .fns = ~ ifelse(
        str_detect(Asset, "Brent|Silver|WTI"),
        .*100,
        .
      )
    )
  ) %>%
  mutate(
    profit_point = Price + mean_daily + sd_daily*profit_factor,
    stop_point = Price - mean_daily + sd_daily*stop_factor,

    profit_points =  mean_daily + sd_daily*profit_factor,
    stop_points = mean_daily + sd_daily*stop_factor
  )


#-------------------------------Desired Params Short trades = 0.64
sd_factor_high  = 6
sd_factor_low  = 5
profit_factor  = 1
stop_factor  = 1

trade_params <-
  tibble(
    sd_factor_low = c(3,4,5,6,7,8,9,10, 12)
  ) %>%
  mutate(
    sd_factor_high = sd_factor_low*2
  )
profit_factor  = 5
stop_factor  = 5
trade_with_daily_data <- LM_preped %>% pluck("LM Merged to Daily")

new_trades_this_week_short <- list()

for (j in 1:dim(trade_params)[1]) {

  sd_factor_low <- trade_params$sd_factor_low[j] %>% as.numeric()
  sd_factor_high <- trade_params$sd_factor_high[j] %>% as.numeric()

  temp_for_trade <- trade_with_daily_data %>%
    mutate(
      trade_col =
        case_when(

          between(Pred_trade,mean_value  + sd_value*sd_factor_low,  mean_value  + sd_value*sd_factor_high) ~ "Long",
          between(Pred_trade,mean_value  - sd_value*sd_factor_high,  mean_value  - sd_value*sd_factor_low) ~ "Short"

        )
    )

  retest_short <-
    generic_trade_finder_conservative(
      tagged_trades = temp_for_trade,
      asset_data_daily_raw = asset_data_daily_raw,
      stop_factor = stop_factor ,
      profit_factor = profit_factor,
      trade_col = "trade_col",
      date_col = "Date",
      max_hold_period = 100,
      start_price_col = "Price",
      mean_values_by_asset = mean_values_by_asset_for_loop,
      return_summary = TRUE
    ) %>%
    map_dfr(
      ~ .x %>% mutate(
        sd_factor_high = 12,
        sd_factor_low = 6
      )
    )

  retest_short_sum <- retest_short %>%
    group_by(trade_category, trade_direction) %>%
    summarise(
      Trades = sum(Trades, na.rm = T)
    ) %>%
    pivot_wider(names_from = trade_category, values_from = Trades) %>%
    mutate(
      Perc = `TRUE WIN`/ (`TRUE LOSS` + `TRUE WIN`)
    )

  trade_with_daily_data <- LM_preped %>% pluck("LM Merged to Daily")

  chance_of_win <- retest_short_sum %>% filter(trade_direction == "Short") %>%
    pull(Perc) %>% as.numeric()

  if( length(chance_of_win) == 0 ) { chance_of_win = 0 }

  new_trades_data_short <- trade_with_daily_data %>%
    mutate(
      trade_col =
        case_when(
          between(Pred_trade,mean_value  + sd_value*sd_factor_low,  mean_value  + sd_value*sd_factor_high) ~ "Long",
          between(Pred_trade,mean_value  - sd_value*sd_factor_high,  mean_value  - sd_value*sd_factor_low) ~ "Short"
        )
    ) %>%
    dplyr::slice_max(Date, n = 1) %>%
    mutate(
      sd_factor_low = sd_factor_low,
      sd_factor_high = sd_factor_high
    ) %>%
    mutate(
      chance_of_win = chance_of_win
    )

  new_trades_this_week_short[[j]] <- new_trades_data_short

}

new_trades_this_week_short <- new_trades_this_week_short %>% map_dfr(bind_rows)

new_trades_this_week_short_filt <- new_trades_this_week_short %>%
  filter(trade_col == "Short") %>%
  group_by(Asset) %>%
  slice_max(chance_of_win) %>%
  left_join(mean_values_by_asset_for_loop)  %>%
  mutate(
    across(
      .cols = c(Price, mean_daily, sd_daily),
      .fns = ~ ifelse(
        str_detect(Asset, "Copper"),
        .*1000,
        .
      )
    ),

    across(
      .cols = c(Price, mean_daily, sd_daily),
      .fns = ~ ifelse(
        str_detect(Asset, "Brent|Silver|WTI"),
        .*100,
        .
      )
    )
  ) %>%
  mutate(
    profit_point = Price + mean_daily + sd_daily*profit_factor,
    stop_point = Price - mean_daily + sd_daily*stop_factor,

    profit_points =  mean_daily + sd_daily*profit_factor,
    stop_points = mean_daily + sd_daily*stop_factor
  )
