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
      prior_rate = round(tau*prior_weight*prior_period),
      prior_shape = round(lag(running_sum, 2)*tau*prior_weight*prior_period)
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
      post_shape = prior_shape + post_shape_comp1,
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


  rolling_periods <- seq(2, 200, 10)
  prior_periods <- seq(2, 200, 10)
  prior_weights <- c(0.5,2,10)
  accumulator <- list()
  c <- 0

  for (k in prior_weights) {
    for (i in rolling_periods) {
      for (j in prior_periods) {

        c <- c + 1
        temp <- get_pois_calc(
          asset_data = asset_data,
          rolling_period = i,
          lm_dependant_var = "Weekly_Close_to_Close",
          independant_var = c("Pois_Change"),
          prior_weight = k,
          prior_period = j
        )
        accumulator[[c]] <- temp
      }
    }
  }


  raw_pois_data  <-accumulator %>%
    keep(~ !is.null(.x)) %>%
    map_dfr(bind_rows) %>%
    group_by(Asset, rolling_period, prior_period, prior_weight) %>%
    mutate(Pois_Change = post_mean - lag(post_mean, 1))

  sd_point = 0.75
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

  test_lm <- lm(data = raw_pois_data,
                formula = Weekly_Low_to_Close  ~ post_mean + rolling_period + prior_weight +prior_period)

  summary(test_lm)

  test_lm <- lm(data = raw_pois_data_plot2,
                formula = ratio_mean_high_to_low  ~  rolling_period + prior_weight +prior_period)

  summary(test_lm)

}
