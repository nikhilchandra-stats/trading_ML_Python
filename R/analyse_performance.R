trade_results_upload <- function(position_date_min = "2025-05-01",
                           assets_to_analyse =
                             c("HK33_HKD", "USD_JPY",
                               "BTC_USD",
                               "AUD_NZD", "GBP_CHF",
                               "EUR_HUF", "EUR_ZAR", "NZD_JPY", "EUR_NZD",
                               "USB02Y_USD",
                               "XAU_CAD", "GBP_JPY", "EUR_NOK", "USD_SGD", "EUR_SEK",
                               "DE30_EUR",
                               "AUD_CAD",
                               "UK10YB_GBP",
                               "XPD_USD",
                               "UK100_GBP",
                               "USD_CHF", "GBP_NZD",
                               "GBP_SGD", "USD_SEK", "EUR_SGD", "XCU_USD", "SUGAR_USD", "CHF_ZAR",
                               "AUD_CHF", "EUR_CHF", "USD_MXN", "GBP_USD", "WTICO_USD", "EUR_JPY", "USD_NOK",
                               "XAU_USD",
                               "DE10YB_EUR",
                               "USD_CZK", "AUD_SGD", "USD_HUF", "WHEAT_USD",
                               "EUR_USD", "SG30_SGD", "GBP_AUD", "NZD_CAD", "AU200_AUD", "XAG_USD",
                               "XAU_EUR", "EUR_GBP", "USD_CNH", "USD_CAD", "NAS100_USD",
                               "USB10Y_USD",
                               "EU50_EUR", "NATGAS_USD", "CAD_JPY", "FR40_EUR", "USD_ZAR", "XAU_GBP",
                               "CH20_CHF", "ESPIX_EUR",
                               "XPT_USD",
                               "EUR_AUD", "SOYBN_USD",
                               "US2000_USD",
                               "XAG_USD", "XAG_EUR", "XAG_CAD", "XAG_AUD", "XAG_GBP", "XAG_JPY", "XAG_SGD", "XAG_CHF",
                               "XAG_NZD",
                               "XAU_USD", "XAU_EUR", "XAU_CAD", "XAU_AUD", "XAU_GBP", "XAU_JPY", "XAU_SGD", "XAU_CHF",
                               "XAU_NZD",
                               "BTC_USD", "LTC_USD", "BCH_USD",
                               "US30_USD", "FR40_EUR", "US2000_USD", "CH20_CHF", "SPX500_USD", "AU200_AUD",
                               "JP225_USD", "JP225Y_JPY", "SG30_SGD", "EU50_EUR", "HK33_HKD",
                               "USB02Y_USD", "USB05Y_USD", "USB30Y_USD", "USB10Y_USD", "UK100_GBP"),
                           db_path = "C:/Users/Nikhil Chandra/Documents/trade_data/trade_results.db"
                           ) {



  pos_1_book <- list()
  pos_2_book <- list()
  pos_3_book <- list()
  pos_4_book <- list()
  pos_5_book <- list()
  safely_get_positions <- safely(get_closed_positions, otherwise = NULL)
  for (i in 1:length(assets_to_analyse)) {

    pos_1_book[[i]] <- safely_get_positions(account_var = 1, asset = assets_to_analyse[i])
    Sys.sleep(1)
    pos_2_book[[i]] <- safely_get_positions(account_var = 2, asset = assets_to_analyse[i])
    Sys.sleep(1)
    pos_3_book[[i]] <- safely_get_positions(account_var = 3, asset = assets_to_analyse[i])
    Sys.sleep(1)
    pos_4_book[[i]] <- safely_get_positions(account_var = 4, asset = assets_to_analyse[i])
    Sys.sleep(1)
    pos_5_book[[i]] <- safely_get_positions(account_var = 5, asset = assets_to_analyse[i])
  }

  all_perf_results_combined <-
    list(
    list(pos_1_book, "account1"),
    list(pos_2_book, "account2"),
    list(pos_3_book, "account3"),
    list(pos_4_book, "account4"),
    list(pos_5_book, "account5")
  ) %>%
    map_dfr(
      ~
        .x[[1]] %>%
        map(~ .x %>% pluck('result')) %>%
        # keep(!is.null) %>%
        map_dfr(bind_rows) %>%
        mutate(
          account_var = .x[[2]]
        )
    )

  db_con <- connect_db(db_path)
  # write_table_sql_lite(.data = all_perf_results_combined,
  #                       table_name = "trade_results",
  #                       conn = db_con,
  #                       overwrite_true = TRUE)
  current_data_in_db <-
    DBI::dbGetQuery(conn = db_con, statement = "SELECT * FROM trade_results")

  current_ids <- current_data_in_db %>% distinct(id, account_var, Asset, realizedPL)

  total_trade_results_new <-
    all_perf_results_combined %>%
    anti_join(current_ids)

  if(dim(total_trade_results_new)[1] > 0) {
    append_table_sql_lite(.data = total_trade_results_new,
                          table_name = "trade_results",
                          conn = db_con)
  }

  DBI::dbDisconnect(db_con)

}


analyse_trade_results <- function(
    position_date_min = "2025-04-01",
    db_path = "C:/Users/Nikhil Chandra/Documents/trade_data/trade_results.db" ,
    accounts = c("account1", "account2", "account4"),
    direction = "Long"
) {

  current_balances <-
    c(1,2,3,4,5) %>%
    map_dfr(
      ~
        tibble(
          account_var = glue::glue("account{.x}"),
          Balance = get_account_summary(account_var = .x) %>% distinct(NAV) %>% pull(NAV) %>% as.numeric()
          )
    )

  current_balances_relavent <-
    current_balances %>%
    filter(account_var %in% accounts)

  total_balance <- current_balances_relavent$Balance %>% sum()

  db_con <- connect_db(db_path)
  current_data_in_db <-
    DBI::dbGetQuery(conn = db_con, statement = "SELECT * FROM trade_results")
  DBI::dbDisconnect(db_con)


  analysis_data <- current_data_in_db %>%
    mutate(date_open = as_datetime(date_open) %>% floor_date(unit = "hours")) %>%
    filter(account_var %in% accounts) %>%
    filter(date_open > position_date_min)

  total_by_date <-
    analysis_data %>%
    mutate(date_closed = as_datetime(date_closed) %>% floor_date(unit = "hours")) %>%
    mutate(realizedPL = as.numeric(realizedPL),
           initialUnits = as.numeric(initialUnits)) %>%
    filter( (initialUnits >0 & direction == "Long")|(initialUnits <0 & direction == "Short") ) %>%
    group_by(date_closed) %>%
    summarise(
      realizedPL = sum(realizedPL, na.rm = T)
    ) %>%
    arrange(date_closed) %>%
    mutate(
      cumulative_returns = cumsum(realizedPL)
    )

  total_return <- total_by_date$realizedPL %>% sum(na.rm = T)
  total_deposits <- total_balance - total_return
  percent_return <- total_return/total_deposits
  total_return_string <- scales::label_dollar()(total_return)
  total_deposits_string <- scales::label_dollar()(total_deposits)
  percent_return_string <- scales::label_percent()(percent_return)

  days_since_start <-
    floor(as.numeric(as_date(max(total_by_date$date_closed, na.rm = T)) - as_date(position_date_min)))

  daily_income <- total_return/days_since_start
  daily_income_string <- scales::label_dollar()(daily_income)

  title_var <-
    glue::glue("Trade Performance Over Time: Total % Return: {percent_return_string}")
  subtitle_var <-
    glue::glue("Total Deposits: {total_deposits_string},        Returns: {total_return_string},          Daily Income:{daily_income_string}")

  p1 <- total_by_date %>%
    ggplot(aes(x = date_closed, y = cumulative_returns)) +
    geom_line() +
    theme_minimal() +
    ylab("Winnings ($)") +
    labs(title = title_var, subtitle = subtitle_var) +
    theme(axis.title.x = element_blank())

  return(p1)

}


