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
    start_date = "2021-01-01",
    end_date = today() %>% as.character(),
    bid_or_ask = "ask",
    time_frame = "M15"
  )
starting_asset_data_bid_15 <-
  get_db_price(
    db_location = db_location,
    start_date = "2021-01-01",
    end_date = today() %>% as.character(),
    bid_or_ask = "bid",
    time_frame = "M15"
  )

gc()

#------------------------------------------Investigate Different Methods
load_custom_functions()
starting_asset_data_ask_15 %>% mutate(Date = as_date(Date)) %>%  distinct(Date) %>% pull(Date) %>% min()
dates_to_choose_from <- seq(as_datetime("2019-01-02"), as_datetime("2024-07-01"), "day")
pairs_db <-
  glue::glue("C:/Users/Nikhil Chandra/Documents/trade_data/pairs_2025-07-03.db")
db_con <- connect_db(pairs_db)
create_new_table = FALSE
loop_dates <- dates_to_choose_from %>% sample(1200, replace = FALSE)
source("code/PAIRS_TEMP_DELETE_AFTER_TEMP_FUNC.R")
safely_get_trades <- safely(get_cor_trade_results_v2, otherwise = NULL)
safely_get_correlation_reg_dat_v2 <- safely(get_rolling_correlation_estimates, otherwise = NULL)
c = 0

trade_params1 <-
  tibble(
    stop_factor = c(12,10,10,15,17),
    profit_factor = c(12,15,20,22,25)
  )

trade_params2 <-
  c(0.5, 0.75, 1, 1.25, 1.5, 1.75, 2, 2.25,2.5,2.75, 3,3.25 ) %>%
  map_dfr(
    ~ trade_params1 %>%
      mutate(
        sd_fac_all = .x
      )
  ) %>%
  filter(profit_factor == 22)

trade_params <-
  c(15, 20, 30, 50,70, 100, 125, 150, 175, 200, 225, 250, 275, 300, 400, 500) %>%
  map_dfr(
    ~ trade_params2 %>%
      mutate(
        rolling_period = .x
      )
  )

params_done <-
  DBI::dbGetQuery(db_con, statement = "SELECT * FROM pairs_asset_specific") %>%
  distinct(stop_factor, profit_factor, across(contains("_SD")), rolling_period) %>%
  mutate(sd_fac_all  = GBP_EUR_USD_SD) %>%
  dplyr::distinct(stop_factor, profit_factor, rolling_period, sd_fac_all)
params_left <- trade_params %>% anti_join(params_done)

create_new_table <- FALSE

for (i in 1:dim(params_left)[1] ) {

  stop_factor <- trade_params$stop_factor[i] %>% as.numeric()
  profit_factor <- trade_params$profit_factor[i] %>% as.numeric()
  sd_fac_all <- trade_params$sd_fac_all[i] %>% as.numeric()
  rolling_period <- trade_params$rolling_period[i] %>% as.numeric()

    cor_data_list <-
      get_rolling_correlation_estimates(
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
        rolling_period = rolling_period
      )
      # pluck('result')

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
          asset_data_daily_raw = starting_asset_data_bid_15,
          summarise_means = TRUE
        )
      mean_values_by_asset_for_loop_15_ask =
        wrangle_asset_data(
          asset_data_daily_raw = starting_asset_data_ask_15,
          summarise_means = TRUE
        )

      # get_cor_trade_results
      #safely_get_trades
      message("Trying to get trades")
      safely_get_trades <- safely(get_cor_trade_results_v2, otherwise = NULL)
      trade_results <-
        safely_get_trades(
          testing_ramapped = testing_ramapped,
          raw_asset_data_ask = starting_asset_data_ask_15,
          raw_asset_data_bid = starting_asset_data_bid_15,
          AUD_USD_sd = sd_fac_all,
          NZD_USD_sd = sd_fac_all,
          EUR_JPY_SD = sd_fac_all,
          GBP_JPY_SD = sd_fac_all,
          SPX_US2000_SD = sd_fac_all,
          EUR_DE30_SD = sd_fac_all,
          GBP_EUR_USD_SD = sd_fac_all,
          EUR_GBP_USD_USD_SD = sd_fac_all,
          NZD_CHF_USD_SD = sd_fac_all,
          WTI_BCO_SD = sd_fac_all,
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
          mutate(rolling_period = rolling_period)

        asset_analysis <- trade_results[[2]] %>%
          mutate(rolling_period = rolling_period)

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

raw_results <- DBI::dbGetQuery(db_con, statement = "SELECT * FROM pairs_asset_specific")
test <- raw_results %>%
  # filter(Trades > 500) %>%
  mutate(
    risk_weighted_return = round(risk_weighted_return, 5)
  ) %>%
  group_by(profit_factor, stop_factor, across(contains("SD")), rolling_period ) %>%
  summarise(

    min_return = quantile(risk_weighted_return, 0.01),
    low_return = quantile(risk_weighted_return, 0.25),
    mean_return = mean(risk_weighted_return, na.rm = T),
    median_return = median(risk_weighted_return, na.rm = T),
    high_return = quantile(risk_weighted_return, 0.75),
    max_return = quantile(risk_weighted_return, 0.99),
    Trades = sum(Trades, na.rm = T),
    wins = sum(wins, na.rm = T),
    Avg_Final_Dollars = mean(Final_Dollars),
    Low_Final_Dollars = quantile(Final_Dollars, 0.25)

  ) %>%
  mutate(
    low_to_high_ratio = abs(high_return)/abs(low_return)
  )

best_params_based_on_trade_num <-
  DBI::dbGetQuery(db_con, statement = "SELECT * FROM pairs_asset_specific_asset") %>%
  group_by(Asset, trade_direction) %>%
  arrange(desc(risk_weighted_return), .by_group = TRUE) %>%
  ungroup() %>%
  filter(risk_weighted_return > 0.1) %>%
  group_by(Asset, trade_direction) %>%
  slice_max(Trades)

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
