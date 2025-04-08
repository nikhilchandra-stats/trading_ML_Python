get_pois_calc <- function(asset_data = read_csv("C:/Users/Nikhil Chandra/Documents/Asset Data/Futures/EUR_USD Historical Data.csv") %>%
                            mutate(Asset = "AUD_USD"),
                          # starting_scale = 0.5,
                          # starting_shape = 10,
                          # prior_weight = 10,
                          rolling_period = 50,
                          lm_dependant_var = "running_bin_sum",
                          independant_var = c("Pois_Change"),
                          modelling_sample = 0.5,
                          prior_weight = 1,
                          prior_period = 2) {

  prepped_data <-
    asset_data %>%
    group_by(Asset) %>%
    mutate(Date = as.Date(Date, format =  "%m/%d/%Y"))  %>%
    rename(date = Date) %>%
    arrange(date, .by_group = TRUE) %>%
    mutate(
      week_date = lubridate::floor_date(date, "week")
    ) %>%
    group_by(Asset) %>%
    group_by(Asset, week_date) %>%
    mutate(
      tau = n()
    ) %>%
    ungroup() %>%
    mutate(
      group_block = ceiling(row_number()/tau)
    ) %>%
    group_by(Asset, group_block) %>%
    mutate(
      change_bin =
        case_when(
          Open - Price > 0 ~ 1,
          Open - Price <= 0 ~ 0
          )
    ) %>%
    group_by(Asset, group_block) %>%
    mutate(
      running_bin = cumsum(change_bin)
    )

  pois_estimates <-
    prepped_data %>%
    group_by(Asset, week_date) %>%
    summarise(running_bin = sum(change_bin, na.rm = T),
              tau = median(tau, na.rm = T)) %>%
    group_by(Asset) %>%
    arrange(week_date, .by_group = TRUE) %>%
    ungroup() %>%
    group_by(Asset) %>%
    mutate(
      running_mean = slider::slide_dbl(.x = running_bin,
                                       .before = rolling_period,
                                       .f = ~ mean(.x, na.rm = T),
                                       .complete = FALSE),

      running_sum = slider::slide_dbl(.x = running_bin,
                                      .before = prior_period,
                                      .f = ~ sum(.x, na.rm = T),
                                      .complete = FALSE)
    ) %>%
    mutate(
      prior_rate = round(tau),
      prior_shape = round(lag(running_sum, 2)*tau)
    ) %>%
    group_by(Asset, week_date) %>%
    mutate(
      prior_mean =  (mean(rgamma(n = 100,
                                      shape = prior_shape,
                                      rate =  prior_rate)
      ))
    ) %>%
    ungroup() %>%
    mutate(
      post_shape_comp1 = lag(running_mean)*tau,
      post_shape = (prior_shape*prior_weight) + (post_shape_comp1*(1 - prior_weight)),
      post_rate = prior_rate + tau
    ) %>%
    group_by(Asset, week_date) %>%
    mutate(
      post_mean =  (mean(rgamma(n = 100,
                                     shape = post_shape,
                                     rate = post_rate)
      ))
    ) %>%
    ungroup() %>%
    filter(!is.na(prior_mean)) %>%
    group_by(Asset) %>%
    mutate(
      next_blocks_bins = lead(running_bin)
    ) %>%
    ungroup()

  dat_with_pois <- prepped_data %>%
    left_join(pois_estimates %>%
                ungroup() %>%
                dplyr::select(Asset, week_date,post_shape,
                              post_rate, post_mean,
                              running_bin_sum = running_bin,
                              next_blocks_bins)) %>%
    filter(!is.na(post_rate))

  dat_with_pois_weekly2 <- dat_with_pois %>%
    group_by(week_date, Asset) %>%
    mutate(
      Weekly_start = case_when(
        date == min(date, na.rm = T) ~ Open
      ),
      Weekly_end = case_when(
        date == max(date, na.rm = T) ~ Price
      )
    ) %>%
    group_by(week_date, Asset) %>%
    fill(c(Weekly_start, Weekly_end), .direction = "updown") %>%
    group_by(week_date, Asset) %>%
    mutate(Weekly_Close_to_High = (max(High, na.rm = T) - Weekly_start)/Weekly_start)%>%
    mutate(Weekly_Low_to_Close = (min(Low, na.rm = T) - Weekly_start)/Weekly_start) %>%
    mutate(
      Weekly_Close_to_Close = (Weekly_start - Weekly_end)/Weekly_start
    )

  dat_with_pois_week_sum <- dat_with_pois_weekly2 %>%
    group_by(week_date, Asset) %>%
    summarise(
      post_rate = median(post_rate, na.rm= T),
      post_mean = median(post_mean, na.rm = T),
      running_bin_sum = median(running_bin_sum, na.rm = T),
      next_blocks_bins = median(next_blocks_bins, na.rm = T),
      Weekly_Close_to_High = median(Weekly_Close_to_High, na.rm = T),
      Weekly_Low_to_Close = median(Weekly_Low_to_Close, na.rm = T),
      Weekly_Close_to_Close = median(Weekly_Close_to_Close, na.rm = T)
    ) %>%
    ungroup() %>%
    group_by(Asset) %>%
    arrange(week_date, .by_group = TRUE) %>%
    ungroup() %>%
    mutate(Pois_Change = post_mean - lag(post_mean, 1)) %>%
    mutate(
      rolling_period = rolling_period,
      prior_weight = prior_weight,
      prior_period = prior_period
    )

  # lm_form <- create_lm_formula(dependant = lm_dependant_var, independant = independant_var)
  # test_lm <- lm(formula = lm_form, data = dat_with_pois_week_sum)
  # LM_results <- summary(test_lm)
  # LM_results$r.squared

  # return(list("Poisson Transformed Weekly Data" = dat_with_pois_week_sum,  "R Squared" =LM_results$r.squared))

  return(dat_with_pois_week_sum)

}

test_func_temp <- function() {


  asset_data = read_csv("C:/Users/Nikhil Chandra/Documents/Asset Data/Futures/EUR_USD Historical Data.csv") %>%
    mutate(Asset = "EUR_USD") %>%
    bind_rows(
      read_csv("C:/Users/Nikhil Chandra/Documents/Asset Data/Futures/AUD_USD Historical Data.csv") %>%
        mutate(Asset = "AUD_USD")
    )%>%
    bind_rows(
      read_csv("C:/Users/Nikhil Chandra/Documents/Asset Data/Futures/XAU_USD Historical Data.csv") %>%
        mutate(Asset = "XAU_USD")
    )%>%
    bind_rows(
      read_csv("C:/Users/Nikhil Chandra/Documents/Asset Data/Futures/Wesfarmers Stock Price History.csv") %>%
        mutate(Asset = "Wesfarmers")
    )%>%
    bind_rows(
      read_csv("C:/Users/Nikhil Chandra/Documents/Asset Data/Futures/USD_JPY Historical Data.csv") %>%
        mutate(Asset = "USD_JPY")
    )%>%
    bind_rows(
      read_csv("C:/Users/Nikhil Chandra/Documents/Asset Data/Futures/USD_CHF Historical Data.csv") %>%
        mutate(Asset = "USD_CHF")
    )%>%
    bind_rows(
      read_csv("C:/Users/Nikhil Chandra/Documents/Asset Data/Futures/Nikkei 225 Historical Data.csv") %>%
        mutate(Asset = "Nikkei")
    )%>%
    bind_rows(
      read_csv("C:/Users/Nikhil Chandra/Documents/Asset Data/Futures/GBP_USD Historical Data.csv") %>%
        mutate(Asset = "GBP_USD")
    )%>%
    bind_rows(
      read_csv("C:/Users/Nikhil Chandra/Documents/Asset Data/Futures/US 500 Cash Historical Data.csv") %>%
        mutate(Asset = "US 500 Cash")
    )%>%
    bind_rows(
      read_csv("C:/Users/Nikhil Chandra/Documents/Asset Data/Futures/Silver Futures Historical Data.csv") %>%
        mutate(Asset = "Silver")
    )%>%
    bind_rows(
      read_csv("C:/Users/Nikhil Chandra/Documents/Asset Data/Futures/NZD_USD Historical Data.csv") %>%
        mutate(Asset = "NZD_USD")
    )%>%
    bind_rows(
      read_csv("C:/Users/Nikhil Chandra/Documents/Asset Data/Futures/USD_CAD Historical Data.csv") %>%
        mutate(Asset = "USD_CAD")
    )%>%
    bind_rows(
      read_csv("C:/Users/Nikhil Chandra/Documents/Asset Data/Futures/EUR_JPY Historical Data.csv") %>%
        mutate(Asset = "EUR_JPY")
    )

  asset_data_combined <- fs::dir_info("C:/Users/Nikhil Chandra/Documents/Asset Data/Futures/") %>%
    mutate(Asset =
             str_remove(path, "C\\:\\/Users/Nikhil Chandra\\/Documents\\/Asset Data\\/Futures\\/") %>%
             str_remove("\\.csv") %>%
             str_remove("Historical Data")%>%
             str_remove("Stock Price") %>%
             str_remove("History")
    ) %>%
    dplyr::select(path, Asset) %>%
    split(.$Asset, drop = FALSE) %>%
    map_dfr( ~ read_csv(.x[1,1] %>% as.character()) %>%
               mutate(Asset = .x[1,2] %>% as.character())
    )


  rolling_periods <- seq(5, 100, 20)
  prior_periods <- seq(5, 100, 20)
  prior_weights <- seq(2, 0.5, -0.2)
  accumulator <- list()
  c <- 0

  db_con <- connect_db("C:/Users/Nikhil Chandra/Documents/trade_data/Pois_Tagged_TimeSeries.db")

  DBI::dbListTables(db_con)

  for (k in prior_weights) {
    for (i in rolling_periods) {
      for (j in prior_periods) {

        c <- c + 1
        temp <- get_pois_calc(
          asset_data = asset_data_combined,
          rolling_period = i,
          lm_dependant_var = "Weekly_Close_to_Close",
          independant_var = c("Pois_Change"),
          prior_weight = k,
          prior_period = j
        )

        if(c == 1) {

          write_table_sql_lite(.data = temp,
                               table_name = "Pois_Tagged_TimeSeries_2025_03_31_High_Weight",
                               conn = db_con,
                               overwrite_true = TRUE)

          db_con <- connect_db("C:/Users/Nikhil Chandra/Documents/trade_data/Pois_Tagged_TimeSeries.db")

        }

        if(c > 1) {

          append_table_sql_lite(.data = temp,
                                table_name = "Pois_Tagged_TimeSeries_2025_03_31_High_Weight",
                                conn = db_con)

        }

      }
    }
  }

  db_con <- connect_db("C:/Users/Nikhil Chandra/Documents/trade_data/Pois_Tagged_TimeSeries.db")

  raw_pois_data <- DBI::dbGetQuery(conn = db_con,
                                   statement =
                                     "SELECT Asset,week_date,prior_period,rolling_period,prior_weight,Pois_Change,post_rate
                                      FROM Pois_Tagged_TimeSeries")

  DBI::dbDisconnect(db_con)

  rm(db_con)

  raw_pois_data <- raw_pois_data %>%
    distinct() %>%
    mutate(week_date = lubridate::as_date(week_date))

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

  asset_data_daily_raw <- asset_data_daily_raw %>%
    mutate(Date = as.Date(Date, format =  "%m/%d/%Y"))

  mean_values_by_asset_for_loop =
    wrangle_asset_data(
      asset_data_daily_raw = asset_data_daily_raw,
      summarise_means = TRUE
    )

  gc()

bind_pois_to_daily_price <- function(raw_pois_data = raw_pois_data,
                                     asset_data_daily_raw = asset_data_daily_raw,
                                     rolling_period_var = 125,
                                     prior_period_var = 125,
                                     prior_weight_var = 0.3,
                                     sd_fac_low = 1,
                                     sd_fac_high = 2) {

  tagged_trades <-
    raw_pois_data %>%
    filter(rolling_period %in% c(rolling_period_var),
           prior_period %in% c(prior_period_var),
           prior_weight %in% c(prior_weight_var) ) %>%
    mutate(
      trade_col =
        case_when(
          Pois_Change > median(Pois_Change, na.rm = T) +  sd_fac_low*sd(Pois_Change, na.rm = T) &
            Pois_Change <= median(Pois_Change, na.rm = T) + sd_fac_high*sd(Pois_Change, na.rm = T) ~ "Long",

          Pois_Change < median(Pois_Change, na.rm = T) -  sd_fac_low*sd(Pois_Change, na.rm = T) &
            Pois_Change >= median(Pois_Change, na.rm = T) -  sd_fac_high*sd(Pois_Change, na.rm = T) ~ "Short"
        )
    )
    # mutate(
    #   # week_date =
    #   #   case_when(
    #   #     week_date == max(week_date, na.rm = T) ~ week_date + lubridate::days(1),
    #   #     TRUE ~ week_date
    #   #   )
    #   week_date = week_date + lubridate::days(1)
    # )

  median_value <- median(tagged_trades$Pois_Change, na.rm = T)
  sd_value <- sd(tagged_trades$Pois_Change, na.rm = T)

  tagged_trades_with_Price <- asset_data_daily_raw %>%
    left_join(
      tagged_trades,
      by = c("Asset", "Date" = "week_date")
    ) %>%
    mutate(
      sd_fac_low = sd_fac_low,
      sd_fac_high = sd_fac_high
    )

  return(tagged_trades_with_Price)

}

raw_pois_data %>%
  distinct(prior_weight)

rolling_periods <- seq(5, 150, 20)
prior_periods <- seq(5, 150, 20)
prior_weights <- c(0.4,0.3)

sd_fac_low <- 0
sd_fac_high <- 1
stop_fac <- 4
prof_fac <- 4

gc()

variation_params1 <- rolling_periods %>%
  map_dfr(
    ~ tibble(
      prior_periods = prior_periods
    )  %>%
      mutate(
        rolling_periods = .x
      )
  )

variation_params2 <- prior_weights %>%
  map_dfr(
    ~ variation_params1 %>%
      mutate(
        prior_weights = .x
      )
  )

variation_params <- variation_params2 %>%
  mutate(
      split_index = row_number()
  ) %>%
  split(.$split_index) %>%
  map_dfr(
    ~
      bind_pois_to_daily_price(
        raw_pois_data = raw_pois_data,
        asset_data_daily_raw = asset_data_daily_raw,
        rolling_period_var = .x$rolling_periods[1] %>% as.numeric(),
        prior_period_var = .x$prior_periods[1] %>% as.numeric(),
        prior_weight_var = .x$prior_weights[1] %>% as.numeric(),
        sd_fac_low = 1,
        sd_fac_high = 2
      )
  )

gc()

testing <- variation_params %>%
  filter(!is.na(trade_col))

trade_results <-
  generic_trade_finder_conservative(
      tagged_trades = variation_params,
      asset_data_daily_raw = asset_data_daily_raw,
      stop_factor = stop_fac,
      profit_factor = prof_fac,
      trade_col = "trade_col",
      date_col = "Date",
      max_hold_period = 100,
      start_price_col = "Price",
      mean_values_by_asset = mean_values_by_asset_for_loop,
      return_summary = TRUE,
      additional_grouping_vars = c("Asset", "rolling_period", "prior_period", "prior_weight")
    ) %>%
    map_dfr(bind_rows)

trade_results_sum <- trade_results %>%
    group_by(stop_factor, profit_factor,
             rolling_period, prior_period, prior_weight, trade_direction, trade_category) %>%
    summarise(
      wins = sum(Trades, na.rm = T)
    ) %>%
  pivot_wider(names_from = trade_category, values_from = wins)%>%
  mutate(across(.cols = c(`TRUE WIN`, `TRUE LOSS`, `NA`),
                .fns = ~ ifelse(is.na(.), 0, .))) %>%
  mutate(
    Total_Trades = `TRUE WIN` + `TRUE LOSS` + `NA`,
    Perc =  `TRUE WIN`/Total_Trades
  )

trade_results_sum_filt <- trade_results_sum %>%
  filter(Total_Trades > 100) %>%
  group_by(trade_direction) %>%
  slice_max(Perc)

trade_results %>%
  ungroup() %>%
  filter(trade_direction == "Long") %>%
  filter(trade_category == "TRUE WIN") %>%
  ggplot(aes(x =  rolling_period, y = Perc, color = prior_period)) +
  geom_point() +
  theme_minimal()

lm_model <- lm(formula = Perc ~ rolling_period + prior_period + prior_weight,
               data = trade_results %>%
                 filter(trade_direction == "Long") %>%
                 filter(trade_category == "TRUE WIN")
               )
summary(lm_model)

#Model Params
# stop 4, profit 4, rolling period 125, prior_period 125, trade_direction Long, SD low = 1, SD High = 2

#-----------------------------------------------------------------------Legacy Code
# Change Model
  sd_point = 0.75
  i = 1
  j = 1
  stop_factor <- c(0.2, 0.5, 0.7, 1)
  profit_factor <- c(0.2, 0.5, 0.7, 1)

  raw_pois_data_plot2 <- raw_pois_data %>%
    ungroup() %>%
    mutate(
      break_point =
        case_when(
          Pois_Change > median(Pois_Change, na.rm = T) +  sd_point*sd(Pois_Change, na.rm = T) ~ "Upper",
          TRUE ~ "Lower"
        ),
      break_horizontal  = median(Pois_Change, na.rm = T) +  sd_point*sd(Pois_Change, na.rm = T)
    ) %>%
    mutate(
      close_bin = ifelse(Weekly_Close_to_Close > 0, 1, 0),
      low_to_high_bin = ifelse(abs(Weekly_Low_to_Close) < abs(Weekly_Close_to_High ) , 1, 0),
      low_to_high_ratio = (abs(Weekly_Close_to_High/Weekly_Low_to_Close))
    ) %>%
    group_by(break_point, rolling_period, prior_period, prior_weight) %>%
    summarise(
      close_bin = sum(close_bin, na.rm = T),
      low_to_high_bin = sum(low_to_high_bin, na.rm = T),
      low_to_high_ratio = median(low_to_high_ratio, na.rm = T),
      total_trades = n(),
      mean_low = mean(Weekly_Low_to_Close, na.rm = T),
      mean_high = mean(Weekly_Close_to_High, na.rm = T),
      running_bin_sum  = mean(running_bin_sum , na.rm = T),
      total_bear = sum(Weekly_Low_to_Close, na.rm = T),
      total_bull = sum(Weekly_Close_to_High, na.rm = T)
    ) %>%
    mutate(
      close_bin_perc = close_bin/total_trades,
      low_to_high_bin_perc = low_to_high_bin/total_trades,
      ratio_mean_high_to_low = abs(mean_high/mean_low)
    ) %>%
    filter(total_trades > 100)


  sd_point = 0.5
  i = 4
  j = 4
  stop_factor <- c(0.2, 0.5, 0.7, 1)
  profit_factor <- c(0.2, 0.5, 0.7, 1)

  raw_pois_data_plot2 <- raw_pois_data %>%
    ungroup() %>%
    mutate(
      break_point =
        case_when(
          Pois_Change > median(Pois_Change, na.rm = T) +  0*sd(Pois_Change, na.rm = T) &
            Pois_Change <= median(Pois_Change, na.rm = T) +  0.2*sd(Pois_Change, na.rm = T) ~ "Upper 0-0.15",
          Pois_Change > median(Pois_Change, na.rm = T) +  0.2*sd(Pois_Change, na.rm = T) &
            Pois_Change <= median(Pois_Change, na.rm = T) +  0.4*sd(Pois_Change, na.rm = T) ~ "Upper 0.15-0.3",
          Pois_Change > median(Pois_Change, na.rm = T) +  0.4*sd(Pois_Change, na.rm = T) &
            Pois_Change <= median(Pois_Change, na.rm = T) +  0.6*sd(Pois_Change, na.rm = T) ~ "Upper 0.3-0.45",
          Pois_Change > median(Pois_Change, na.rm = T) +  0.6*sd(Pois_Change, na.rm = T)  &
            Pois_Change <= median(Pois_Change, na.rm = T) +  0.8*sd(Pois_Change, na.rm = T)  ~ "Upper 0.45-0.6",
          Pois_Change > median(Pois_Change, na.rm = T) +  0.8*sd(Pois_Change, na.rm = T)  ~ "Upper >0.6",

          Pois_Change < median(Pois_Change, na.rm = T) -  0*sd(Pois_Change, na.rm = T) &
            Pois_Change >= median(Pois_Change, na.rm = T) -  0.2*sd(Pois_Change, na.rm = T) ~ "Lower 0-0.15",
          Pois_Change < median(Pois_Change, na.rm = T) -  0.2*sd(Pois_Change, na.rm = T) &
            Pois_Change >= median(Pois_Change, na.rm = T) -  0.4*sd(Pois_Change, na.rm = T) ~ "Lower 0.15-0.3",
          Pois_Change < median(Pois_Change, na.rm = T) -  0.4*sd(Pois_Change, na.rm = T) &
            Pois_Change >= median(Pois_Change, na.rm = T) -  0.6*sd(Pois_Change, na.rm = T) ~ "Lower 0.3-0.45",
          Pois_Change < median(Pois_Change, na.rm = T) -  0.6*sd(Pois_Change, na.rm = T) &
            Pois_Change >= median(Pois_Change, na.rm = T) -  0.8*sd(Pois_Change, na.rm = T) ~ "Lower 0.45-0.6",
          Pois_Change < median(Pois_Change, na.rm = T) -  0.8*sd(Pois_Change, na.rm = T)  ~ "Lower >0.6"
        ),
      break_horizontal  = median(Pois_Change, na.rm = T) +  sd_point*sd(Pois_Change, na.rm = T)
    )  %>%
    left_join(mean_returns) %>%
    filter(!is.na(avg_low_open )) %>%
    mutate(
      loss_value_short = abs(avg_high_open)*stop_factor[i],
      loss_value_long = abs(avg_low_open)*stop_factor[i],

      win_value_short = abs(avg_low_open)*profit_factor[j],
      win_value_long = abs(avg_high_open)*profit_factor[j]
    ) %>%
    mutate(
      short_win =
        ifelse(abs(Weekly_Low_to_Close) > abs(avg_low_open)*profit_factor[j]  &
                 abs(Weekly_Close_to_High ) < abs(avg_high_open)*stop_factor[i] , 1, 0),

      short_loss =
        ifelse(abs(Weekly_Low_to_Close) >= abs(avg_low_open)*stop_factor[i] &
                           abs(Weekly_Close_to_High ) <= abs(avg_high_open)*profit_factor[j] , 1, 0),

      long_win =
        ifelse(abs(Weekly_Low_to_Close) < abs(avg_low_open)*stop_factor[i] &
                 abs(Weekly_Close_to_High ) > abs(avg_high_open)*profit_factor[j] , 1, 0),

      long_loss =
        ifelse(abs(Weekly_Low_to_Close) >= abs(avg_low_open)*stop_factor[i] &
                           abs(Weekly_Close_to_High ) <= abs(avg_high_open)*profit_factor[j] , 1, 0),

      short_win_value =
        case_when(
          abs(Weekly_Low_to_Close) > abs(avg_low_open)*profit_factor[j]  &
            abs(Weekly_Close_to_High ) < abs(avg_high_open)*stop_factor[i] ~ profit_factor[j]*abs(avg_low_open),

          abs(Weekly_Low_to_Close) <= abs(avg_low_open)*profit_factor[j]  &
            abs(Weekly_Close_to_High ) >= abs(avg_high_open)*stop_factor[i] ~ -1*stop_factor[i]*abs(avg_high_open),

          # TRUE ~ -1*Weekly_Close_to_Close
          TRUE ~ 0

        ),

      long_win_value =
        case_when(
          abs(Weekly_Low_to_Close) < abs(avg_low_open)*stop_factor[i] &
            abs(Weekly_Close_to_High ) > abs(avg_high_open)*profit_factor[j] ~ profit_factor[j]*abs(avg_high_open),
          abs(Weekly_Low_to_Close) >= abs(avg_low_open)*stop_factor[i] &
            abs(Weekly_Close_to_High ) <= abs(avg_high_open)*profit_factor[j] ~ -1*stop_factor[i]*abs(avg_low_open),
          # TRUE ~Weekly_Close_to_Close
          TRUE ~0
        )

    ) %>%
    group_by(rolling_period, prior_period, prior_weight, break_point) %>%
      summarise(
        total_trades = n(),

        short_loss = sum(short_loss, na.rm = T),
        long_loss = sum(long_loss, na.rm = T),

        short_win = sum(short_win, na.rm = T),
        long_win = sum(long_win, na.rm = T),
        short_win_value = sum(short_win_value, na.rm = T),
        long_win = sum(long_win, na.rm = T),
        short_win_value = sum(short_win_value, na.rm = T),
        long_win_value = sum(long_win_value, na.rm = T)
      ) %>%
      ungroup() %>%
      mutate(
        perc_short = short_win/total_trades,
        perc_long = long_win/total_trades
      ) %>%
      mutate(
        total_known_short = short_win +short_loss,
        total_known_long = long_win +long_loss,

        total_unknown_short = total_trades - total_known_short,
        total_unknown_long = total_trades - total_known_long,

        adj_short_win = (total_unknown_short)*0.3 + short_win,
        adj_long_win = (total_unknown_long)*0.3 + long_win,
        adj_perc_short = adj_short_win/total_trades,
        adj_perc_long = adj_long_win/total_trades,
        profit_factor = profit_factor[j],
        stop_factor = stop_factor[i],
      ) %>%
    filter(total_trades > 100)


  #--------------------------------Probability Model
  test <- raw_pois_data %>%
    mutate(
      sucess_1_prob = dpois(1, lambda = post_mean ),
      sucess_2_prob = dpois(2, lambda = post_mean ),
      sucess_3_prob = dpois(3, lambda = post_mean ),
      sucess_4_prob = dpois(4, lambda = post_mean ),
      sucess_5_prob = dpois(5, lambda = post_mean ),
      bull_prob = sucess_3_prob + sucess_4_prob + sucess_5_prob
    )

  prob_test <- test %>%
    group_by(rolling_period, prior_period, prior_weight) %>%
    mutate(
      bull_prob_mean = mean(bull_prob, na.rm = T),
      bull_prob_sd = sd(bull_prob, na.rm = T)
    ) %>%
    ungroup() %>%
    mutate(
      split_point =
        case_when(
          bull_prob < bull_prob_mean - bull_prob_sd*0 &
            bull_prob >= bull_prob_mean - bull_prob_sd*0.5 ~ "0-0.5 SD",

          bull_prob < bull_prob_mean - bull_prob_sd*0.5 &
            bull_prob >= bull_prob_mean - bull_prob_sd*1 ~ "0.5-1 SD",

          bull_prob < bull_prob_mean - bull_prob_sd*1 &
            bull_prob >= bull_prob_mean - bull_prob_sd*1.5 ~ "1-1.5 SD",

          bull_prob < bull_prob_mean - bull_prob_sd*1.5  ~ ">1.5 SD",

          bull_prob >= bull_prob_mean + bull_prob_sd*0 &
            bull_prob < bull_prob_mean + bull_prob_sd*0.5 ~ "0-0.5 SD pos",

          bull_prob >= bull_prob_mean + bull_prob_sd*0.5 &
            bull_prob < bull_prob_mean + bull_prob_sd*1 ~ "0.5-1 SD pos",

          bull_prob >= bull_prob_mean + bull_prob_sd*1 &
            bull_prob < bull_prob_mean + bull_prob_sd*1.5 ~ "1-1.5 SD pos",

          bull_prob >= bull_prob_mean + bull_prob_sd*1.5  ~ ">1.5 SD pos"
        )

    ) %>%
    mutate(
      close_bin = ifelse(Weekly_Close_to_Close > 0, 1, 0),
      low_to_high_bin = ifelse(abs(Weekly_Low_to_Close) < abs(Weekly_Close_to_High ) , 1, 0),
      low_to_high_ratio = (abs(Weekly_Close_to_High/Weekly_Low_to_Close))
    ) %>%
    group_by(rolling_period, prior_period, prior_weight, split_point) %>%
    summarise(
      close_bin = sum(close_bin, na.rm = T),
      low_to_high_bin = sum(low_to_high_bin, na.rm = T),
      low_to_high_ratio = median(low_to_high_ratio, na.rm = T),
      total_trades = n(),
      mean_low = mean(Weekly_Low_to_Close, na.rm = T),
      mean_high = mean(Weekly_Close_to_High, na.rm = T),
      running_bin_sum  = mean(running_bin_sum , na.rm = T),
      total_bear = sum(Weekly_Low_to_Close, na.rm = T),
      total_bull = sum(Weekly_Close_to_High, na.rm = T)
    ) %>%
    mutate(
      close_bin_perc = close_bin/total_trades,
      low_to_high_bin_perc = low_to_high_bin/total_trades,
      ratio_mean_high_to_low = abs(mean_high/mean_low)
    ) %>%
    ungroup() %>%
    filter(!is.na(split_point)) %>%
    filter(total_trades >= 100) %>%
    filter(ratio_mean_high_to_low < 1 & close_bin_perc < 0.45)

  stop_factor <- c(0.2, 0.5, 0.7, 1)
  profit_factor <- c(0.2, 0.5, 0.7, 1)

  mean_returns <- test %>%
    group_by(Asset) %>%
    summarise(
      avg_low_open = mean(Weekly_Low_to_Close, na.rm = T),
      avg_high_open = mean(Weekly_Close_to_High, na.rm = T),
      sd_low_open = sd(Weekly_Low_to_Close, na.rm = T),
      sd_high_open = sd(Weekly_Close_to_High, na.rm = T)
    )

  prob_test_accumulator <- list()
  c = 0

  for (i in 1:length(stop_factor) ) {

    for (j in 1:length(profit_factor) ) {

      c = c + 1

      prob_test_accumulator[[c]] <-  test %>%
        group_by(rolling_period, prior_period, prior_weight) %>%
        mutate(
          bull_prob_mean = mean(bull_prob, na.rm = T),
          bull_prob_sd = sd(bull_prob, na.rm = T)
        ) %>%
        ungroup() %>%
        mutate(
          split_point =
            case_when(
              bull_prob < bull_prob_mean - bull_prob_sd*0 &
                bull_prob >= bull_prob_mean - bull_prob_sd*0.5 ~ "0-0.5 SD",

              bull_prob < bull_prob_mean - bull_prob_sd*0.5 &
                bull_prob >= bull_prob_mean - bull_prob_sd*1 ~ "0.5-1 SD",

              bull_prob < bull_prob_mean - bull_prob_sd*1 &
                bull_prob >= bull_prob_mean - bull_prob_sd*1.5 ~ "1-1.5 SD",

              bull_prob < bull_prob_mean - bull_prob_sd*1.5  ~ ">1.5 SD",

              bull_prob >= bull_prob_mean + bull_prob_sd*0 &
                bull_prob < bull_prob_mean + bull_prob_sd*0.5 ~ "0-0.5 SD pos",

              bull_prob >= bull_prob_mean + bull_prob_sd*0.5 &
                bull_prob < bull_prob_mean + bull_prob_sd*1 ~ "0.5-1 SD pos",

              bull_prob >= bull_prob_mean + bull_prob_sd*1 &
                bull_prob < bull_prob_mean + bull_prob_sd*1.5 ~ "1-1.5 SD pos",

              bull_prob >= bull_prob_mean + bull_prob_sd*1.5  ~ ">1.5 SD pos"
            )
        ) %>%
        left_join(mean_returns) %>%
        filter(!is.na(avg_low_open )) %>%
        mutate(
          loss_value_short = abs(avg_high_open)*stop_factor[i],
          loss_value_long = abs(avg_low_open)*stop_factor[i],

          win_value_short = abs(avg_low_open)*profit_factor[j],
          win_value_long = abs(avg_high_open)*profit_factor[j]
        ) %>%
        mutate(
          short_win =
            ifelse(abs(Weekly_Low_to_Close) > abs(avg_low_open)*profit_factor[j]  &
                     abs(Weekly_Close_to_High ) < abs(avg_high_open)*stop_factor[i] , 1, 0),
          long_win =
            ifelse(abs(Weekly_Low_to_Close) < abs(avg_low_open)*stop_factor[i] &
                     abs(Weekly_Close_to_High ) > abs(avg_high_open)*profit_factor[j] , 1, 0),

          short_win_value =
            case_when(
              abs(Weekly_Low_to_Close) > abs(avg_low_open)*profit_factor[j]  &
                abs(Weekly_Close_to_High ) < abs(avg_high_open)*stop_factor[i] ~ profit_factor[j]*abs(avg_low_open),

              abs(Weekly_Low_to_Close) <= abs(avg_low_open)*profit_factor[j]  &
                abs(Weekly_Close_to_High ) >= abs(avg_high_open)*stop_factor[i] ~ -1*stop_factor[i]*abs(avg_high_open),

              TRUE ~ -1*Weekly_Close_to_Close

            ),

          long_win_value =
            case_when(
              abs(Weekly_Low_to_Close) < abs(avg_low_open)*stop_factor[i] &
                abs(Weekly_Close_to_High ) > abs(avg_high_open)*profit_factor[j] ~ profit_factor[j]*abs(avg_high_open),
              abs(Weekly_Low_to_Close) >= abs(avg_low_open)*stop_factor[i] &
                abs(Weekly_Close_to_High ) <= abs(avg_high_open)*profit_factor[j] ~ -1*stop_factor[i]*abs(avg_low_open),
              TRUE ~Weekly_Close_to_Close
            )

        ) %>%
        group_by(rolling_period, prior_period, prior_weight, split_point) %>%
        summarise(
          total_trades = n(),
          short_win = sum(short_win, na.rm = T),
          long_win = sum(long_win, na.rm = T),
          short_win_value = sum(short_win_value, na.rm = T),
          long_win_value = sum(long_win_value, na.rm = T)
        ) %>%
        ungroup() %>%
        mutate(
          perc_short = short_win/total_trades,
          perc_long = long_win/total_trades
        ) %>%
        mutate(
          profit_factor = profit_factor[j],
          stop_factor = stop_factor[i]
        )

    }

  }

  tagged_trades <-  raw_pois_data %>%
    ungroup() %>%
    mutate(
      break_point =
        case_when(
          Pois_Change > median(Pois_Change, na.rm = T) +  0*sd(Pois_Change, na.rm = T) &
            Pois_Change <= median(Pois_Change, na.rm = T) +  0.2*sd(Pois_Change, na.rm = T) ~ "Upper 0-0.15",
          Pois_Change > median(Pois_Change, na.rm = T) +  0.2*sd(Pois_Change, na.rm = T) &
            Pois_Change <= median(Pois_Change, na.rm = T) +  0.4*sd(Pois_Change, na.rm = T) ~ "Upper 0.15-0.3",
          Pois_Change > median(Pois_Change, na.rm = T) +  0.4*sd(Pois_Change, na.rm = T) &
            Pois_Change <= median(Pois_Change, na.rm = T) +  0.6*sd(Pois_Change, na.rm = T) ~ "Upper 0.3-0.45",
          Pois_Change > median(Pois_Change, na.rm = T) +  0.6*sd(Pois_Change, na.rm = T)  &
            Pois_Change <= median(Pois_Change, na.rm = T) +  0.8*sd(Pois_Change, na.rm = T)  ~ "Upper 0.45-0.6",
          Pois_Change > median(Pois_Change, na.rm = T) +  0.8*sd(Pois_Change, na.rm = T)  ~ "Upper >0.6",

          Pois_Change < median(Pois_Change, na.rm = T) -  0*sd(Pois_Change, na.rm = T) &
            Pois_Change >= median(Pois_Change, na.rm = T) -  0.2*sd(Pois_Change, na.rm = T) ~ "Lower 0-0.15",
          Pois_Change < median(Pois_Change, na.rm = T) -  0.2*sd(Pois_Change, na.rm = T) &
            Pois_Change >= median(Pois_Change, na.rm = T) -  0.4*sd(Pois_Change, na.rm = T) ~ "Lower 0.15-0.3",
          Pois_Change < median(Pois_Change, na.rm = T) -  0.4*sd(Pois_Change, na.rm = T) &
            Pois_Change >= median(Pois_Change, na.rm = T) -  0.6*sd(Pois_Change, na.rm = T) ~ "Lower 0.3-0.45",
          Pois_Change < median(Pois_Change, na.rm = T) -  0.6*sd(Pois_Change, na.rm = T) &
            Pois_Change >= median(Pois_Change, na.rm = T) -  0.8*sd(Pois_Change, na.rm = T) ~ "Lower 0.45-0.6",
          Pois_Change < median(Pois_Change, na.rm = T) -  0.8*sd(Pois_Change, na.rm = T)  ~ "Lower >0.6"
        ),
      break_horizontal  = median(Pois_Change, na.rm = T) +  sd_point*sd(Pois_Change, na.rm = T)
    ) %>%
    mutate(
      break_point_trade =
        case_when(
          break_point == "Upper 0-0.15" ~ "long",
          break_point == "Upper 0.15-0.3" ~ "long",
          break_point == "Upper 0.3-0.45" ~ "long",
          break_point == "Upper 0.45-0.6" ~ "long",
          break_point == "Upper >0.6" ~ "long",

          break_point == "Lower 0-0.15" ~ "short",
          break_point == "Lower 0.15-0.3" ~ "short",
          break_point == "Lower 0.3-0.45" ~ "short",
          break_point == "Lower 0.45-0.6" ~ "short",
          break_point == "Lower >0.6" ~ "short"
        )
    )

}
