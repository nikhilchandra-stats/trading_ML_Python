transform_asset_to_weekly <- function(asset_data= aud,
                                      filt_NA_lead_values = TRUE,
                                      drop_high_low = TRUE) {

  transformed_asset_data <- asset_data %>%
    mutate(Date = as.Date(Date, format =  "%m/%d/%Y"))  %>%
    rename(date = Date) %>%
    arrange(date) %>%
    mutate(
      week_date = lubridate::floor_date(date, "week")
    ) %>%
    group_by(week_date, Asset) %>%
    mutate(
      week_start_price = case_when(
        date == min(date, na.rm = T) ~ Price
      ),
      weekly_high = max(High, na.rm = T),
      weekly_low = min(Low, na.rm = T)
    ) %>%
    ungroup() %>%
    filter(!is.na(week_start_price)) %>%
    group_by(Asset) %>%
    arrange(week_date, .by_group = TRUE) %>%
    ungroup() %>%
    dplyr::select(-date) %>%
    # left_join(USD_exports_total) %>%
    # fill(c(US_Export, Aus_Export), .direction = "down") %>%
    mutate(
      week_start_price = log(week_start_price)
    ) %>%
    # filter(!is.na(US_Export))%>%
    # filter(!is.na(Aus_Export)) %>%
    arrange(week_date) %>%
    mutate(
      Week_Change = lead(week_start_price) - week_start_price,
      Week_Change_lag = week_start_price - lag(week_start_price) ,
      # Month_Change_US_EXPORT = US_Export  - lag(US_Export ),
      # Month_Change_Aus_Export  = Aus_Export   - lag(Aus_Export  )
    )

  if(drop_high_low == TRUE) {
    transformed_asset_data <- transformed_asset_data %>%
      dplyr::select(-weekly_high, -weekly_low)
  }

  if(filt_NA_lead_values) {
    transformed_asset_data <-
      transformed_asset_data %>%
      filter(!is.na(Week_Change), !is.na(Week_Change_lag)) %>%
      dplyr::select(-Vol., -`Change %`)
  } else {
    transformed_asset_data <-
      transformed_asset_data %>%
      filter(!is.na(Week_Change_lag)) %>%
      dplyr::select(-Vol., -`Change %`)
  }

  return(transformed_asset_data)

}

transform_macro_to_monthly <- function(
    macro_dat_for_transform = get_USD_Indicators(raw_macro_data = raw_macro_data),
    transform_to_week = FALSE) {

  if(transform_to_week == FALSE) {

    transformed_dat <- macro_dat_for_transform %>%
      mutate(
        month_date = lubridate::floor_date(date, "month") + months(1)
      ) %>%
      group_by(month_date) %>%
      summarise(across(.cols = where(is.numeric), .fns = ~ median(.))) %>%
      ungroup() %>%
      mutate(
        across(.cols = where(is.numeric), .fns = ~ ifelse(. == 0, NA, .))
      ) %>%
      ungroup() %>%
      fill(where(is.numeric), .direction = "down") %>%
      # fill(where(is.numeric), .direction = "up") %>%
      mutate(
        across(
          .cols = where(is.numeric), .fns = ~. - lag(.)
        )
      ) %>%
      filter(if_all(where(is.numeric), ~ !is.na(.)))

  }

  if(transform_to_week) {

    transformed_dat <- macro_dat_for_transform %>%
      mutate(
        month_date = lubridate::floor_date(date, "month") + months(1)
      ) %>%
      group_by(month_date) %>%
      summarise(across(.cols = where(is.numeric), .fns = ~ median(.))) %>%
      ungroup()  %>%
      mutate(
        across(
          .cols = where(is.numeric), .fns = ~. - lag(.)
        )
      ) %>%
      mutate(
        across(.cols = where(is.numeric), .fns = ~ ifelse(. == 0, NA, .))
      ) %>%
      ungroup() %>%
      fill(where(is.numeric), .direction = "down") %>%
      mutate(
        across(
          contains(c("Interest", "Index")),
          ~ ifelse(is.na(.), 0, .)
        )
      ) %>%
      # fill(where(is.numeric), .direction = "up") %>%
      mutate(
        across(
          .cols = where(is.numeric), .fns = ~. - lag(.)
        )
      ) %>%
      filter(if_all(where(is.numeric), ~ !is.na(.)))

    start_date <- transformed_dat$month_date %>% min(na.rm = T)
    end_date <- transformed_dat$month_date %>% max(na.rm = T)

    redo_tibble <-
      tibble(
        week_date = seq(start_date, end_date, "week")
      ) %>%
      mutate(
        month_date = lubridate::floor_date(week_date, "month")
      ) %>%
      left_join(transformed_dat) %>%
      mutate(
        across(.cols = where(is.numeric), .fns = ~ ifelse(. == 0, NA, .))
      ) %>%
      fill(where(is.numeric), .direction = "down")%>%
      fill(where(is.numeric), .direction = "up") %>%
      dplyr::select(-month_date) %>%
      mutate(
        week_date  = lubridate::floor_date(week_date, "week")
      )

    transformed_dat <-redo_tibble

  }

  return(transformed_dat)

}
