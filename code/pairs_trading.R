helpeR::load_custom_functions()

all_aud_symbols <- get_oanda_symbols() %>%
  keep(~ str_detect(.x, "AUD")|str_detect(.x, "USD_SEK|USD_NOK|USD_HUF|USD_ZAR|USD_CNY|USD_MXN"))
asset_infor <- get_instrument_info()
aud_assets <- read_all_asset_data_intra_day(
  asset_list_oanda = all_aud_symbols,
  save_path_oanda_assets = "C:/Users/Nikhil Chandra/Documents/Asset Data/oanda_data/",
  read_csv_or_API = "API",
  time_frame = "D",
  bid_or_ask = "bid",
  how_far_back = 10,
  start_date = (today() - days(2)) %>% as.character()
)
aud_assets <- aud_assets %>% map_dfr(bind_rows)
aud_usd_today <- get_aud_conversion(asset_data_daily_raw = aud_assets)

currency_conversion <-
  aud_usd_today %>%
  mutate(
    not_aud_asset = ending_value
  ) %>%
  dplyr::select(not_aud_asset, adjusted_conversion) %>%
  bind_rows(
    tibble(not_aud_asset = "AUD", adjusted_conversion = 1)
  )

asset_list_oanda <- get_oanda_symbols() %>%
  keep( ~ .x %in% c("HK33_HKD", "USD_JPY","SPX500_USD",
                    "AUD_NZD", "GBP_CHF",
                    "EUR_HUF", "EUR_ZAR", "NZD_JPY", "EUR_NZD",
                    "XAU_CAD", "GBP_JPY", "EUR_NOK", "USD_SGD", "EUR_SEK",
                    "DE30_EUR",
                    "AUD_CAD",
                    "XPD_USD",
                    "UK100_GBP",
                    "USD_CHF", "GBP_NZD",
                    "GBP_SGD", "USD_SEK", "EUR_SGD", "XCU_USD", "SUGAR_USD", "CHF_ZAR",
                    "AUD_CHF", "EUR_CHF", "USD_MXN", "GBP_USD", "WTICO_USD", "EUR_JPY", "USD_NOK",
                    "XAU_USD",
                    "USD_CZK", "AUD_SGD", "USD_HUF", "WHEAT_USD",
                    "EUR_USD", "SG30_SGD", "GBP_AUD", "NZD_CAD", "AU200_AUD", "XAG_USD",
                    "XAU_EUR", "EUR_GBP", "USD_CNH", "USD_CAD", "NAS100_USD",
                    "EU50_EUR", "NATGAS_USD", "CAD_JPY", "FR40_EUR", "USD_ZAR", "XAU_GBP",
                    "EUR_AUD", "SOYBN_USD",
                    "US2000_USD",
                    "BCO_USD", "AUD_USD", "NZD_USD", "NZD_CHF", "WHEAT_USD", "AUD_JPY", "AUD_SEK")
  )

asset_infor <- get_instrument_info()
#---------------------Data
db_location <- "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data.db"

starting_asset_data_ask_15 <-
  get_db_price(
    db_location = db_location,
    start_date = "2019-01-01",
    end_date = today() %>% as.character(),
    bid_or_ask = "ask",
    time_frame = "M15"
  )
starting_asset_data_bid_15 <-
  get_db_price(
    db_location = db_location,
    start_date = "2019-01-01",
    end_date = today() %>% as.character(),
    bid_or_ask = "bid",
    time_frame = "M15"
  )

gc()

#------------------------------------------
dates_to_choose_from <- seq(as_datetime("2016-01-01"), as_datetime("2022-07-01"), "day")
pairs_db <-
  glue::glue("C:/Users/Nikhil Chandra/Documents/trade_data/pairs_2025-07-03.db")
db_con <- connect_db(pairs_db)
create_new_table = FALSE
loop_dates <- dates_to_choose_from %>% sample(600)
safely_get_trades <-
  safely(get_cor_trade_results, otherwise = NULL)
c = 0

trade_params <-
  tibble(
    stop_factor = c(10,17),
    profit_factor = c(15,25)
  )

trade_params2 <-
  tibble(
    sd_fac1 = c(40,25,20),
    sd_fac2 = c(55,35,25)
  )  %>%
  split(.$sd_fac1, drop = FALSE) %>%
  map_dfr(
    ~ trade_params %>%
      mutate(
        sd_fac1 = .x$sd_fac1[1] %>% as.numeric(),
        sd_fac2 = .x$sd_fac2[1] %>% as.numeric()
      )
  )

for (i in 6:dim(trade_params2)[1] ) {

  stop_factor <- trade_params2$stop_factor[i] %>% as.numeric()
  profit_factor <- trade_params2$profit_factor[i] %>% as.numeric()
  sd_fac1 <- trade_params2$sd_fac1[i] %>% as.numeric()
  sd_fac2 <- trade_params2$sd_fac2[i] %>% as.numeric()

  for (j in 1:length(loop_dates)) {

    start_date <- (loop_dates[j])
    end_date <- lubridate::add_with_rollback(start_date, years(2))

    sim_data_bid <- starting_asset_data_bid_15 %>%
      filter(Date >= start_date & Date <= end_date )

    sim_data_ask <- starting_asset_data_ask_15 %>%
      filter(Date >= start_date & Date <= end_date )

    cor_data_list <-
      get_correlation_reg_dat(
        asset_data_to_use = sim_data_bid,
        samples_for_MLE = 0.5,
        test_samples = 0.4,
        regression_train_prop = 0.5,
        dependant_period = 10,
        assets_to_filter = c(
          c("AUD_USD", "NZD_USD"),
          c("EUR_USD", "GBP_USD"),
          c("EUR_JPY", "EUR_USD"),
          c("GBP_USD", "EUR_GBP"),
          c("AU200_AUD", "SPX500_USD"),
          c("US2000_USD", "SPX500_USD"),
          c("WTICO_USD", "BCO_USD"),
          c("XAG_USD", "XAU_USD"),
          c("USD_CAD", "USD_JPY")
        )
      )

    testing_data <- cor_data_list[[1]]
    testing_ramapped <-
      testing_data %>% dplyr::select(-c(Price, Open, High, Low))

    mean_values_by_asset_for_loop_15_bid =
      wrangle_asset_data(
        asset_data_daily_raw = sim_data_bid,
        summarise_means = TRUE
      )
    mean_values_by_asset_for_loop_15_ask =
      wrangle_asset_data(
        asset_data_daily_raw = sim_data_ask,
        summarise_means = TRUE
      )

    # get_cor_trade_results
    #safely_get_trades
    trade_results <-
      safely_get_trades(
        testing_data = testing_ramapped,
        raw_asset_data = sim_data_ask,
        sd_fac1 = sd_fac1,
        sd_fac2 = sd_fac2,
        stop_factor = stop_factor,
        profit_factor = profit_factor,
        trade_direction = "Long",
        mean_values_by_asset_for_loop = mean_values_by_asset_for_loop_15_ask,
        currency_conversion = currency_conversion,
        asset_infor = asset_infor,
        risk_dollar_value = 10,
        return_analysis = TRUE,
        pos_or_neg = "neg"
      ) %>%
      pluck('result')

    if(!is.null(trade_results)) {

      c = c + 1

      total_analysis <- trade_results[[1]] %>%
        mutate(
          start_date =  as_date(start_date),
          end_date =  as_date(end_date)
        )

      asset_analysis <- trade_results[[2]] %>%
        mutate(
          start_date =  as_date(start_date),
          end_date =  as_date(end_date)
        )

      if(create_new_table == TRUE & c == 1) {
        write_table_sql_lite(conn = db_con, .data = total_analysis, table_name = "pairs_lm")
        write_table_sql_lite(conn = db_con, .data = asset_analysis, table_name = "pairs_lm_asset")
        create_new_table <- FALSE
      }

      if(create_new_table == FALSE) {
        append_table_sql_lite(conn = db_con, .data = total_analysis, table_name = "pairs_lm")
        append_table_sql_lite(conn = db_con, .data = asset_analysis, table_name = "pairs_lm_asset")
      }

    }

    rm(asset_analysis)
    rm(total_analysis)
    rm(trade_results)
    rm(testing_data)
    rm(testing_ramapped)
    rm(cor_data_list)
    rm(sim_data_bid)
    rm(sim_data_ask)

  }


}


raw_results <- DBI::dbGetQuery(db_con, statement = "SELECT * FROM pairs_lm") %>%
  filter(pos_or_neg == "neg")
test <- raw_results %>%
  filter(pos_or_neg == "neg") %>%
  filter(Trades > 500) %>%
  mutate(
    risk_weighted_return = round(risk_weighted_return, 5)
  ) %>%
  group_by(sd_fac1, profit_factor, stop_factor, sd_fac2) %>%
  summarise(

    min_return = quantile(risk_weighted_return, 0.01),
    low_return = quantile(risk_weighted_return, 0.25),
    mean_return = mean(risk_weighted_return, na.rm = T),
    median_return = median(risk_weighted_return, na.rm = T),
    high_return = quantile(risk_weighted_return, 0.75),
    max_return = quantile(risk_weighted_return, 0.99),

  ) %>%
  mutate(
    low_to_high_ratio = abs(high_return)/abs(low_return)
  )

raw_results %>%
  ggplot(aes(x = risk_weighted_return)) +
  geom_density(alpha = 0.25, fill = "darkorange") +
  facet_wrap(.~profit_factor, scales = "free")

testing <- test %>%
  summarise(
    Trades = sum(Trades),
    wins = sum(wins),
    mid_win = mean(maximum_win),
    mid_loss = mean(minimal_loss)
  ) %>%
  mutate(
    risk_weighted_return =
      (mid_win/mid_loss)*(wins/Trades) - ((1 - (wins/Trades)))
  )

DBI::dbDisconnect(db_con)

#------------------------------------------Investigate Different Methods
starting_asset_data_ask_15 %>% mutate(Date = as_date(Date)) %>%  distinct(Date) %>% pull(Date) %>% min()
dates_to_choose_from <- seq(as_datetime("2019-01-02"), as_datetime("2024-07-01"), "day")
pairs_db <-
  glue::glue("C:/Users/Nikhil Chandra/Documents/trade_data/pairs_2025-07-03.db")
db_con <- connect_db(pairs_db)
create_new_table = TRUE
loop_dates <- dates_to_choose_from %>% sample(1200, replace = FALSE)
safely_get_trades <- safely(get_cor_trade_results_v2, otherwise = NULL)
safely_get_correlation_reg_dat_v2 <- safely(get_rolling_correlation_estimates, otherwise = NULL)
c = 0

trade_params <-
  tibble(
    stop_factor = c(12,10,10,15,17),
    profit_factor = c(12,15,20,22,25)
  )


for (i in 4:dim(trade_params)[1] ) {

  stop_factor <- trade_params$stop_factor[i] %>% as.numeric()
  profit_factor <- trade_params$profit_factor[i] %>% as.numeric()

  for (j in 1:length(loop_dates)) {

    start_date <- (loop_dates[j])
    end_date <- lubridate::add_with_rollback(start_date, years(1))

    sim_data_bid <- starting_asset_data_bid_15 %>%
      filter(Date >= start_date & Date <= end_date )

    sim_data_ask <- starting_asset_data_ask_15 %>%
      filter(Date >= start_date & Date <= end_date )

    cor_data_list <-
      safely_get_correlation_reg_dat_v2(
        # asset_data_to_use = sim_data_bid,
        asset_data_to_use = starting_asset_data_bid_15,
        samples_for_MLE = 0.5,
        test_samples = 0.85,
        assets_to_filter = c(c("AUD_USD", "NZD_USD"),
                             c("EUR_USD", "GBP_USD"),
                             c("EUR_JPY", "EUR_USD"),
                             c("GBP_USD", "EUR_GBP"),
                             c("AU200_AUD", "SPX500_USD"),
                             c("US2000_USD", "SPX500_USD"),
                             c("WTICO_USD", "BCO_USD"),
                             c("XAG_USD", "XAU_USD"),
                             c("USD_CAD", "USD_JPY"),
                             c("SG30_SGD", "SPX500_USD"),
                             c("NZD_CHF", "NZD_USD"),
                             c("SPX500_USD", "XAU_USD"),
                             c("NZD_CHF", "USD_CHF"),
                             c("EU50_EUR", "DE30_EUR"),
                             c("EU50_EUR", "SPX500_USD"),
                             c("DE30_EUR", "SPX500_USD"),
                             c("USD_SEK", "EUR_SEK"),
                             c("EUR_JPY", "GBP_JPY")),
        rolling_period = 50
      ) %>%
      pluck('result')

    message("Did the trade bit")

    if(!is.null(cor_data_list)) {

      message("Made it inside if statement, cor result found")

      testing_data <- cor_data_list
      testing_ramapped <-
        testing_data %>% dplyr::select(-c(Price, Open, High, Low))

      rm(cor_data_list)
      rm(testing_data)

      gc()

      mean_values_by_asset_for_loop_15_bid =
        wrangle_asset_data(
          asset_data_daily_raw = sim_data_bid,
          # asset_data_daily_raw = starting_asset_data_bid_15,
          summarise_means = TRUE
        )
      mean_values_by_asset_for_loop_15_ask =
        wrangle_asset_data(
          asset_data_daily_raw = sim_data_ask,
          # asset_data_daily_raw = starting_asset_data_ask_15,
          summarise_means = TRUE
        )

      # get_cor_trade_results
      #safely_get_trades
      message("Trying to get trades")
      safely_get_trades <- safely(get_cor_trade_results_v2, otherwise = NULL)
      trade_results <-
        safely_get_trades(
          testing_ramapped = testing_ramapped,
          # raw_asset_data_ask = sim_data_ask,
          # raw_asset_data_bid = sim_data_bid,

          raw_asset_data_ask = starting_asset_data_ask_15,
          raw_asset_data_bid = starting_asset_data_bid_15,

          AUD_USD_sd = 0.25,
          NZD_USD_sd = 0.25,
          EUR_JPY_SD = 2.5,
          GBP_JPY_SD = 2,
          SPX_US2000_SD = 1,
          EUR_DE30_SD = 3,
          GBP_EUR_USD_SD = 1.25,
          EUR_GBP_USD_USD_SD = 1.25,
          NZD_CHF_USD_SD = 2.5,
          WTI_BCO_SD = 2.5,
          stop_factor = stop_factor,
          profit_factor = profit_factor,
          mean_values_by_asset_for_loop = mean_values_by_asset_for_loop_15_ask,
          currency_conversion = currency_conversion,
          asset_infor = asset_infor,
          risk_dollar_value = 10,
          return_analysis = TRUE
        ) %>%
        pluck('result')

      if(!is.null(trade_results)) {

        message("Found Trades")

        c = c + 1

        total_analysis <- trade_results[[1]] %>%
          mutate(
            start_date =  as_date(start_date),
            end_date =  as_date(end_date)
          )

        asset_analysis <- trade_results[[2]] %>%
          mutate(
            start_date =  as_date(start_date),
            end_date =  as_date(end_date)
          )

        rm(trade_results)

        gc()

        if(create_new_table == TRUE & c == 1) {
          write_table_sql_lite(conn = db_con, .data = total_analysis, table_name = "pairs_asset_specific")
          write_table_sql_lite(conn = db_con, .data = asset_analysis, table_name = "pairs_asset_specific_asset")
          create_new_table <- FALSE
        }

        if(create_new_table == FALSE) {
          append_table_sql_lite(conn = db_con, .data = total_analysis, table_name = "pairs_asset_specific")
          append_table_sql_lite(conn = db_con, .data = asset_analysis, table_name = "pairs_asset_specific_asset")
        }

      }

      rm(asset_analysis)
      rm(total_analysis)
      rm(trade_results)
      rm(testing_data)
      rm(testing_ramapped)
      rm(cor_data_list)
      rm(sim_data_bid)
      rm(sim_data_ask)

    }

    gc()

  }


}


raw_results <- DBI::dbGetQuery(db_con, statement = "SELECT * FROM pairs_asset_specific")
test <- raw_results %>%
  # filter(Trades > 500) %>%
  mutate(
    risk_weighted_return = round(risk_weighted_return, 5)
  ) %>%
  group_by(profit_factor, stop_factor, trade_direction) %>%
  summarise(

    min_return = quantile(risk_weighted_return, 0.01),
    low_return = quantile(risk_weighted_return, 0.25),
    mean_return = mean(risk_weighted_return, na.rm = T),
    median_return = median(risk_weighted_return, na.rm = T),
    high_return = quantile(risk_weighted_return, 0.75),
    max_return = quantile(risk_weighted_return, 0.99),
    Trades = mean(Trades, na.rm = T),
    Avg_Final_Dollars = mean(Final_Dollars),
    Low_Final_Dollars = quantile(Final_Dollars, 0.25)

  ) %>%
  mutate(
    low_to_high_ratio = abs(high_return)/abs(low_return)
  )

raw_results_asset <- DBI::dbGetQuery(db_con, statement = "SELECT * FROM pairs_asset_specific_asset")
test_asset <- raw_results_asset %>%
  # filter(Trades > 500) %>%
  mutate(
    risk_weighted_return = round(risk_weighted_return, 5)
  ) %>%
  group_by(profit_factor, stop_factor, Asset, trade_direction) %>%
  summarise(

    min_return = quantile(risk_weighted_return, 0.01),
    low_return = quantile(risk_weighted_return, 0.25),
    mean_return = mean(risk_weighted_return, na.rm = T),
    median_return = median(risk_weighted_return, na.rm = T),
    high_return = quantile(risk_weighted_return, 0.75),
    max_return = quantile(risk_weighted_return, 0.99),
    Trades = mean(Trades, na.rm = T),
    Avg_Final_Dollars = mean(Final_Dollars),
    Low_Final_Dollars = quantile(Final_Dollars, 0.25)

  ) %>%
  mutate(
    low_to_high_ratio = abs(high_return)/abs(low_return)
  )

raw_results %>%
  ggplot(aes(x = risk_weighted_return)) +
  geom_density(alpha = 0.25, fill = "darkorange") +
  facet_wrap(.~profit_factor, scales = "free")

testing <- test %>%
  summarise(
    Trades = sum(Trades),
    wins = sum(wins),
    mid_win = mean(maximum_win),
    mid_loss = mean(minimal_loss)
  ) %>%
  mutate(
    risk_weighted_return =
      (mid_win/mid_loss)*(wins/Trades) - ((1 - (wins/Trades)))
  )

DBI::dbDisconnect(db_con)
