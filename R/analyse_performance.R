analyse_vol_trades <- function(position_date_min = "2025-04-15") {

  pos_1_book <- get_closed_positions(account_var = 1) %>%
    mutate(openTime = as_datetime(openTime) %>% floor_date(unit = "hours"))
  pos_2_book <- get_closed_positions(account_var = 3) %>%
    mutate(openTime = as_datetime(openTime) %>% floor_date(unit = "hours"))

  pos1_vol_trades <- pos_1_book %>%
    filter(openTime >= position_date_min) %>%
    distinct(instrument, realizedPL, openTime) %>%
    rename(realizedPL1 = realizedPL)

  pos2_vol_trades <- pos_2_book %>%
    filter(openTime >= position_date_min) %>%
    distinct(instrument, realizedPL, openTime)  %>%
    rename(realizedPL2 = realizedPL)

  performance <- pos2_vol_trades %>%
    left_join(pos1_vol_trades) %>%
    mutate(across(.cols = c(realizedPL1, realizedPL2),
                  .fns = ~ as.numeric(.)
                  )
           ) %>%
    mutate(across(.cols = c(realizedPL1, realizedPL2),
                  .fns = ~ ifelse(is.na(.), 0, .) )
           ) %>%
    distinct() %>%
    mutate(
      return = realizedPL1 + realizedPL2
    ) %>%
    mutate(
      total_return = sum(return)
    )

}



