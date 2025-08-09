trade_results_upload <- function(position_date_min = "2025-07-31",
                           assets_to_analyse =
                             c("USD_JPY", "GBP_JPY", "USD_SGD", "EUR_SEK",
                               "DE30_EUR",
                               "USD_CHF", "USD_SEK", "XCU_USD", "SUGAR_USD",
                               "USD_MXN", "GBP_USD", "WTICO_USD", "EUR_JPY", "USD_NOK",
                               "XAU_USD",
                               "USD_CZK",  "WHEAT_USD",
                               "EUR_USD", "SG30_SGD", "AU200_AUD", "XAG_USD",
                               "EUR_GBP", "USD_CNH", "USD_CAD", "NAS100_USD",
                               "EU50_EUR", "NATGAS_USD", "SOYBN_USD",
                               "US2000_USD",
                               "BCO_USD", "AUD_USD", "NZD_USD", "NZD_CHF", "WHEAT_USD",
                               "JP225_USD", "SPX500_USD"),
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
        mutate(openTime = as_datetime(openTime) %>% floor_date(unit = "hours")) %>%
        mutate(
          account_var = .x[[2]]
        )
    )

  # account1 <- pos_1_book %>%
  #   map(~ .x %>% pluck('result')) %>%
  #   keep(~ !is.null(.x)) %>%
  #   map_dfr(
  #     ~ .x %>%
  #       mutate(openTime = as_datetime(openTime) %>% floor_date(unit = "hours"))
  #   ) %>%
  #   mutate(
  #     account_var = "account1"
  #   )
  #
  # account2 <- pos_2_book %>%
  #   map(~ .x %>% pluck('result')) %>%
  #   keep(~ !is.null(.x)) %>%
  #   map_dfr(
  #     ~ .x %>%
  #       mutate(openTime = as_datetime(openTime) %>% floor_date(unit = "hours"))
  #   )%>%
  #   mutate(
  #     account_var = "account2"
  #   )
  #
  # account3 <- pos_3_book %>%
  #   map(~ .x %>% pluck('result')) %>%
  #   keep(~ !is.null(.x)) %>%
  #   map_dfr(
  #     ~ .x %>%
  #       mutate(openTime = as_datetime(openTime) %>% floor_date(unit = "hours"))
  #   ) %>%
  #   mutate(
  #     account_var = "account3"
  #   )
  #
  # total_trade_results <-
  #   account1 %>%
  #   bind_rows(account2) %>%
  #   bind_rows(account3)

  db_con <- connect_db(db_path)
  current_data_in_db <-
    DBI::dbGetQuery(conn = db_con, statement = "SELECT * FROM trade_results")

  current_ids <- current_data_in_db %>% distinct(id, account_var, price, realizedPL)

  total_trade_results_new <-
    # total_trade_results %>%
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
    accounts = c("account1", "account2", "account4", "account5"),
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
    mutate(openTime = as_datetime(openTime) %>% floor_date(unit = "hours")) %>%
    filter(account_var %in% accounts) %>%
    filter(openTime > position_date_min)

  total_by_date <-
    analysis_data %>%
    mutate(closeTime = as_datetime(closeTime) %>% floor_date(unit = "hours")) %>%
    mutate(realizedPL = as.numeric(realizedPL),
           initialUnits = as.numeric(initialUnits)) %>%
    filter( (initialUnits >0 & direction == "Long")|(initialUnits <0 & direction == "Short") ) %>%
    group_by(closeTime) %>%
    summarise(
      realizedPL = sum(realizedPL, na.rm = T)
    ) %>%
    arrange(closeTime) %>%
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
    floor(as.numeric(as_date(max(total_by_date$closeTime, na.rm = T)) - as_date(position_date_min)))

  daily_income <- total_return/days_since_start
  daily_income_string <- scales::label_dollar()(daily_income)

  title_var <-
    glue::glue("Trade Performance Over Time: Total % Return: {percent_return_string}")
  subtitle_var <-
    glue::glue("Total Deposits: {total_deposits_string}, Returns - {total_return_string}, Daily Income:{daily_income_string}")

  p1 <- total_by_date %>%
    ggplot(aes(x = closeTime, y = cumulative_returns)) +
    geom_line() +
    theme_minimal() +
    ylab("Winnings ($)") +
    labs(title = title_var, subtitle = subtitle_var) +
    theme(axis.title.x = element_blank())

  return(p1)

}


