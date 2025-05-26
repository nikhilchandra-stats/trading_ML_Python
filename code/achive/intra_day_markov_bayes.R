
helperfunctions35South::load_custom_functions()
one_drive_path <- helperfunctions35South::create_one_drive_path(
  path_extension = "raw data")
library(neuralnet)

all_aud_symbols <- get_oanda_symbols() %>%
  keep(~ str_detect(.x, "AUD"))
all_aud_symbols <- get_oanda_symbols() %>%
  keep( ~ .x %in% c(all_aud_symbols, "USD_SEK", "USD_ZAR", "USD_NOK", "USD_MXN", "USD_HUF"))

asset_infor <- get_instrument_info()

aud_conversion_data <-
  read_all_asset_data_intra_day(
    asset_list_oanda = all_aud_symbols,
    save_path_oanda_assets = "C:/Users/Nikhil Chandra/Documents/Asset Data/oanda_data/",
    read_csv_or_API = "API",
    time_frame = "D",
    bid_or_ask = "ask",
    how_far_back = 5000,
    start_date = "2011-01-01"
  )
aud_conversion_data <- aud_conversion_data %>% map_dfr(bind_rows)
aud_usd_today <- get_aud_conversion(asset_data_daily_raw = aud_conversion_data)

currency_conversion <-
  aud_usd_today %>%
  mutate(
    not_aud_asset = ending_value
  ) %>%
  dplyr::select(not_aud_asset, adjusted_conversion) %>%
  bind_rows(
    tibble(not_aud_asset = "AUD", adjusted_conversion = 1)
  )


#---------------------------------------------------

asset_list_oanda <- get_oanda_symbols() %>%
  keep( ~ .x %in% c("USD_JPY", "GBP_JPY", "USD_SGD", "EUR_SEK",
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
                    "JP225_USD", "SPX500_USD")
  )

asset_infor <- get_instrument_info()

data_list_dfr <- get_intra_day_asset_data(bid_or_ask = "ask")

save_path <- "C:/Users/Nikhil Chandra/Documents/trade_data/quick_load/"

write.csv(data_list_dfr,
          paste0(save_path, "/extracted_asset_data_h1_ts_ask.csv"),
          row.names = FALSE)

data_list_dfr <- read_csv(paste0(save_path, "/extracted_asset_data_h1_ts_ask.csv"))

data_list_dfr <- data_list_dfr %>%
  mutate(
    Date = as_datetime(Date)
  )

#-------------------------------------------------------------------------

#----Short Term - Short = 0.39, risk = 0.18, 15000 Trades
# trade_sd_fact_post = 1.5
# trade_sd_fact_sigma = 0.25
# bayes_prior = 200
# bayes_prior_trade = 50
# rolling_period = 100
# stop = 2, prof = 4

#----Short Term - Short = 0.43, risk = 0.0766, 10000 Trades
# trade_sd_fact_post = 1.5
# trade_sd_fact_sigma = 0.5
# bayes_prior = 350
# bayes_prior_trade =  100
# rolling_period = 100
# stop = 2, prof = 3

#----Short Term - Short = 0.40, risk = 0.2024993, 7442 Trades
#----Restricted Assets
# trade_sd_fact_post = 1.5
# trade_sd_fact_sigma = 0.25
# bayes_prior = 200
# bayes_prior_trade =  50
# rolling_period = 100
# profit_factor = 2, profit_factor = 4

#----Short Term - Long = 0.37, risk = 0.1154352, 7442 Trades
#----Restricted Assets
# trade_sd_fact_post = 1.5
# trade_sd_fact_sigma = 0.25
# bayes_prior = 200
# bayes_prior_trade =  50
# rolling_period = 100
# stop_factor = 2, profit_factor = 4

generic_anlyser <- function(trade_data = long_bayes_loop_analysis,
                            profit_factor = profit_factor,
                            stop_factor = stop_factor) {
  trade_data %>%
    rename(Asset = asset) %>%
    convert_stop_profit_AUD(
      asset_infor = asset_infor,
      asset_col = "Asset",
      stop_col = "starting_stop_value",
      profit_col = "starting_profit_value",
      price_col = "trade_start_prices",
      risk_dollar_value = risk_dollar_value,
      returns_present = TRUE,
      trade_return_col = "trade_returns",
      currency_conversion = currency_conversion
    ) %>%
    filter(volume_required > 0) %>%
    mutate(wins = ifelse(trade_return_dollars_AUD > 0, 1, 0)) %>%
    rename(trade_direction = trade_col) %>%
    group_by(trade_direction, dates) %>%
    summarise(
      Trades = n(),
      wins = sum(wins),
      Total_Dollars = sum(trade_return_dollars_AUD)
    ) %>%
    mutate(
      Perc = wins/(Trades)
    ) %>%
    group_by(trade_direction) %>%
    arrange(dates, .by_group = TRUE) %>%
    group_by(trade_direction) %>%
    mutate(
      cumulative_dollars = cumsum(Total_Dollars)
    ) %>%
    group_by(trade_direction) %>%
    summarise(
      Trades = sum(Trades),
      wins = sum(wins),
      Final_Dollars = sum(Total_Dollars),
      Lowest_Dollars = min(cumulative_dollars),
      Dollars_quantile_25 = quantile(cumulative_dollars, 0.25),
      Dollars_quantile_75 = quantile(cumulative_dollars, 0.75),
      max_Dollars = max(cumulative_dollars)
    ) %>%
    mutate(
      Perc = wins/Trades,
      stop_factor = stop_factor,
      profit_factor =profit_factor,
      risk_weighted_return =
        Perc*(profit_factor/stop_factor) - (1- Perc)*(1)
    )
}

long_bayes_loop_analysis2 <- list()
summary_of_trades <- list()
long_bayes_loop_ts <- list()
summary_dplyr_method <- list()
stop_factor =
profit_factor = 3
risk_dollar_value <- 20
asset_data_daily_raw <- data_list_dfr
asset_data_combined <- data_list_dfr

mean_values_by_asset_for_loop =
  wrangle_asset_data(
    asset_data_daily_raw = data_list_dfr,
    summarise_means = TRUE
  )

target_assets <-
  c("USD_JPY", "GBP_JPY", "USD_SGD", "EUR_SEK",
    "USD_CHF", "USD_SEK", "XCU_USD",
    "USD_MXN", "GBP_USD", "WTICO_USD", "EUR_JPY", "USD_NOK",
    "EUR_USD",
    "SG30_SGD",
    "AU200_AUD",
    "XAG_USD",
    "EUR_GBP",
    "USD_CNH",
    "USD_CAD",
    "EU50_EUR",
    # "NATGAS_USD", "SOYBN_USD",
    "US2000_USD",
    # "BCO_USD",
    "AUD_USD",
    "NZD_USD",
    "NZD_CHF",
    # "WHEAT_USD",
    # "JP225_USD",
    "SPX500_USD"
    )

trade_params <-
  c(1.25, 1.5, 1.75, 2, 2.25, 2.5) %>%
  map_dfr(
    ~
      tibble(
        trade_sd_fact_sigma = c(0.15, 0.3, 0.6, 0.8, 1)
      ) %>%
      mutate(
        trade_sd_fact_post = .x
      )
  )

trade_params2 <- c(100, 200, 300) %>%
  map_dfr(
    ~ trade_params %>%
      mutate(
        bayes_prior = .x
      )
  )

trade_params3 <- c(50, 100) %>%
  map_dfr(
    ~ trade_params2 %>%
      mutate(
        bayes_prior_trade = .x
      )
  )

trade_params4 <- c(1,3, 5, 7) %>%
  map_dfr(
    ~ trade_params3 %>%
      mutate(
        stop_factor = .x
      ) %>%
      mutate(
        profit_factor = 1.35*.x
      ) %>%
      bind_rows(
        trade_params3 %>%
          mutate(
            stop_factor = .x
          ) %>%
          mutate(
            profit_factor = 2.5*.x
          )
      ) %>%
      bind_rows(
        trade_params3 %>%
          mutate(
            stop_factor = .x
          ) %>%
          mutate(
            profit_factor = 3*.x
          )
      )
  )

c <- 0
performance_data <- list()
db_con <- connect_db("C:/Users/Nikhil Chandra/Documents/trade_data/markov_bayes_results.db")

for (j in 299:dim(trade_params4)[1] ) {

  trade_sd_fact_sigma <- trade_params4$trade_sd_fact_sigma[j] %>% as.numeric()
  trade_sd_fact_post <- trade_params4$trade_sd_fact_post[j] %>% as.numeric()
  bayes_prior <- trade_params4$bayes_prior[j] %>% as.numeric()
  bayes_prior_trade <- trade_params4$bayes_prior_trade[j] %>% as.numeric()
  profit_factor <- trade_params4$profit_factor[j] %>% as.numeric()
  stop_factor <- trade_params4$stop_factor[j] %>% as.numeric()

  markov_bayes_analysis <-
    get_markov_tag_bayes(
      asset_data_combined = asset_data_combined %>%
        filter(
          Asset %in% target_assets
        ),
      training_perc = 1,
      sd_divides = seq(0.5,2,0.5),
      quantile_divides = seq(0.1,0.9, 0.1),
      rolling_period = 100,
      markov_col_on_interest_pos = "Markov_Point_Pos_roll_sum_1.5",
      markov_col_on_interest_neg = "Markov_Point_Neg_roll_sum_-1.5",
      sum_sd_cut_off = "",
      profit_factor  = profit_factor,
      stop_factor  = stop_factor,
      asset_data_daily_raw = asset_data_combined,
      mean_values_by_asset_for_loop = mean_values_by_asset_for_loop,
      trade_sd_fact_post = trade_sd_fact_post,
      trade_sd_fact_post_high = 1,
      trade_sd_fact_sigma = trade_sd_fact_sigma,
      bayes_prior = bayes_prior,
      bayes_prior_trade = bayes_prior_trade
    )

  long_bayes_loop_analysis <-
    generic_trade_finder_loop(
      tagged_trades = markov_bayes_analysis[[1]],
      asset_data_daily_raw = asset_data_daily_raw,
      stop_factor = stop_factor,
      profit_factor =profit_factor,
      trade_col = "trade_col",
      date_col = "Date",
      start_price_col = "Price",
      mean_values_by_asset =
        wrangle_asset_data(
          asset_data_daily_raw = asset_data_daily_raw,
          summarise_means = TRUE
        )
    )

  if(j ==1 ) {
    write_table_sql_lite(conn = db_con,
                         .data = long_bayes_loop_analysis  %>%
                           mutate(
                             trade_sd_fact_sigma  = trade_sd_fact_sigma,
                             trade_sd_fact_post = trade_sd_fact_post,
                             bayes_prior =bayes_prior,
                             bayes_prior_trade =bayes_prior_trade,
                             profit_factor = profit_factor,
                             stop_factor = stop_factor,
                             index_j = j
                           ),
                         table_name = "markov_bayes_results", overwrite_true = TRUE)
    db_con <- connect_db("C:/Users/Nikhil Chandra/Documents/trade_data/markov_bayes_results.db")
  } else {
    append_table_sql_lite(
      conn = db_con,
      .data = long_bayes_loop_analysis  %>%
        mutate(
          trade_sd_fact_sigma  = trade_sd_fact_sigma,
          trade_sd_fact_post = trade_sd_fact_post,
          bayes_prior =bayes_prior,
          bayes_prior_trade =bayes_prior_trade,
          profit_factor = profit_factor,
          stop_factor = stop_factor,
          index_j = j
        ),
      table_name = "markov_bayes_results"
    )
  }

  results_data<- long_bayes_loop_analysis %>%
    split(.$trade_col) %>%
    map(
      ~ analyse_trailing_trades(
        trade_data = .x,
        asset_data_daily_raw = asset_data_daily_raw,
        asset_infor,
        risk_dollar_value = 20,
        currency_conversion = currency_conversion
      )
    )

  # temp <-
  #   long_bayes_loop_analysis  %>%
  #   rename(Asset = asset) %>%
  #   convert_stop_profit_AUD(
  #     asset_infor = asset_infor,
  #     asset_col = "Asset",
  #     stop_col = "starting_stop_value",
  #     profit_col = "starting_profit_value",
  #     price_col = "trade_start_prices",
  #     risk_dollar_value = risk_dollar_value,
  #     returns_present = TRUE,
  #     trade_return_col = "trade_returns",
  #     currency_conversion = currency_conversion
  #   )
  #
  # testing <- temp %>%
  #   filter(trade_col == "Long") %>%
  #   mutate(wins = ifelse(trade_return_dollars_AUD >0, 1, 0)) %>%
  #   group_by(wins, Asset) %>%
  #   summarise(
  #     Aud_Value = mean(trade_return_dollars_AUD),
  #     stop_value = mean(stop_value, na.rm = T),
  #     profit_value = mean(profit_value, na.rm = T),
  #     stop_value_AUD = mean(stop_value_AUD, na.rm = T),
  #     profit_value_AUD = mean(profit_value_AUD, na.rm = T),
  #
  #   )

  performance_data[[j]] <-
    generic_anlyser(trade_data = long_bayes_loop_analysis,
                    profit_factor = profit_factor,
                    stop_factor = stop_factor) %>%
    mutate(
      Ideal_Est_Return =
        Trades*((risk_dollar_value*(profit_factor/stop_factor))*Perc - (1 - Perc)*(risk_dollar_value))
    ) %>%
    mutate(
      trade_sd_fact_sigma  = trade_sd_fact_sigma,
      trade_sd_fact_post = trade_sd_fact_post,
      bayes_prior =bayes_prior,
      bayes_prior_trade =bayes_prior_trade,
      profit_factor = profit_factor,
      stop_factor = stop_factor
    )

  # long_ts <- results_data[[1]][[2]]
  #
  # long_ts %>%
  #   ggplot(aes(x = dates, y = cumulative_return_aud)) +
  #   geom_line() +
  #   theme_minimal()
  #
  # short_ts <- results_data[[2]][[2]]
  #
  # short_ts %>%
  #   ggplot(aes(x = dates, y = cumulative_return_aud)) +
  #   geom_line() +
  #   theme_minimal()

}

summary_of_trades_dfr <-
  performance_data %>%
  map_dfr(bind_rows) %>%
  filter(trade_direction == "Long")
  # filter(stop_factor <= profit_factor)

write.csv(summary_of_trades_dfr,
          file = "C:/Users/Nikhil Chandra/Documents/trade_data/trades_summary_markov_bayes_long_2.csv",
          row.names = FALSE)

long_bayes_loop_ts %>%
  map_dfr(bind_rows) %>%
  filter(trade_col == "Long") %>%
  ggplot(aes(x = dates, y = cumulative_aud)) +
  geom_line(aes(color = trade_col) ) +
  facet_wrap(.~ simulation_j, scales = "free") +
  theme_minimal()

#-------------------------------------------------------------------------


helperfunctions35South::load_custom_functions()
one_drive_path <- helperfunctions35South::create_one_drive_path(
  path_extension = "raw data")
library(neuralnet)

asset_list_oanda <- get_oanda_symbols() %>%
  keep( ~ .x %in% c("USD_JPY", "GBP_JPY", "USD_SGD", "EUR_SEK",
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
                    "JP225_USD", "SPX500_USD")
  )

asset_infor <- get_instrument_info()

extracted_asset_data1 <-
  read_all_asset_data_intra_day(
    asset_list_oanda = asset_list_oanda,
    save_path_oanda_assets = "C:/Users/Nikhil Chandra/Documents/Asset Data/oanda_data/",
    read_csv_or_API = "API",
    time_frame = "H1",
    bid_or_ask = "bid",
    how_far_back = 5000,
    start_date = "2017-01-01"
  )

extracted_asset_data1 <- extracted_asset_data1 %>% map_dfr(bind_rows)
max_date_in_1 <- extracted_asset_data1$Date %>% max(na.rm = T) %>% as_date() %>% as.character()

extracted_asset_data2 <-
  read_all_asset_data_intra_day(
    asset_list_oanda = asset_list_oanda,
    save_path_oanda_assets = "C:/Users/Nikhil Chandra/Documents/Asset Data/oanda_data/",
    read_csv_or_API = "API",
    time_frame = "H1",
    bid_or_ask = "bid",
    how_far_back = 5000,
    start_date = max_date_in_1
  )

extracted_asset_data2 <- extracted_asset_data2 %>% map_dfr(bind_rows)
max_date_in_2 <- extracted_asset_data2$Date %>% max(na.rm = T) %>% as_date() %>% as.character()

extracted_asset_data3 <-
  read_all_asset_data_intra_day(
    asset_list_oanda = asset_list_oanda,
    save_path_oanda_assets = "C:/Users/Nikhil Chandra/Documents/Asset Data/oanda_data/",
    read_csv_or_API = "API",
    time_frame = "H1",
    bid_or_ask = "bid",
    how_far_back = 5000,
    start_date = max_date_in_2
  )

extracted_asset_data3 <- extracted_asset_data3 %>% map_dfr(bind_rows)
max_date_in_3 <- extracted_asset_data3$Date %>% max(na.rm = T) %>% as_date() %>% as.character()

extracted_asset_data4 <-
  read_all_asset_data_intra_day(
    asset_list_oanda = asset_list_oanda,
    save_path_oanda_assets = "C:/Users/Nikhil Chandra/Documents/Asset Data/oanda_data/",
    read_csv_or_API = "API",
    time_frame = "H1",
    bid_or_ask = "bid",
    how_far_back = 5000,
    start_date = max_date_in_3
  )

extracted_asset_data4 <- extracted_asset_data4 %>% map_dfr(bind_rows)
max_date_in_4 <- extracted_asset_data4$Date %>% max(na.rm = T) %>% as_date() %>% as.character()

asset_data_combined <-
  extracted_asset_data1 %>%
  bind_rows(extracted_asset_data2) %>%
  bind_rows(extracted_asset_data3) %>%
  bind_rows(extracted_asset_data4) %>%
  distinct() %>%
  group_by(Asset, Date) %>%
  mutate(row_n = n()) %>%
  group_by(Asset, Date) %>%
  slice_max(row_n) %>%
  ungroup() %>%
  dplyr::select(-row_n)

asset_data_daily_raw <-asset_data_combined
asset_data_combined$Date %>% min()
asset_data_combined$Date %>% max()

mean_values_by_asset_for_loop =
  wrangle_asset_data(
    asset_data_daily_raw = asset_data_daily_raw,
    summarise_means = TRUE
  )

aud_usd_today <- asset_data_daily_raw %>% filter(str_detect(Asset, "AUD")) %>%
  slice_max(Date)  %>%
  dplyr::select(Price, Asset) %>%
  mutate(ending_value = str_extract(Asset, "_[A-Z][A-Z][A-Z]") %>% str_remove_all("_")) %>%
  mutate(
    adjusted_conversion =
      case_when(
        ending_value != "AUD" ~ 1/Price,
        TRUE ~ Price
      )
  )

currency_conversion <-
  aud_usd_today %>%
  mutate(
    not_aud_asset = str_remove_all(Asset, "AUD") %>% str_remove_all("_")
  ) %>%
  dplyr::select(not_aud_asset, adjusted_conversion) %>%
  bind_rows(
    tibble(not_aud_asset = "AUD", adjusted_conversion = 1)
  ) %>%
  mutate(
    adjusted_conversion =
      case_when(
        not_aud_asset == "JPY" ~ adjusted_conversion*100,
        TRUE ~ adjusted_conversion
      )
  )

rm(extracted_asset_data1)
rm(extracted_asset_data2)
rm(extracted_asset_data3)
rm(extracted_asset_data4)

#----Short Term - Short = 0.39, risk = 0.18, 15000 Trades
# trade_sd_fact_post = 1.5
# trade_sd_fact_sigma = 0.25
# bayes_prior = 200
# bayes_prior_trade = 50
# rolling_period = 100
# stop = 2, prof = 4

#----Short Term - Short = 0.43, risk = 0.0766, 10000 Trades
# trade_sd_fact_post = 1.5
# trade_sd_fact_sigma = 0.5
# bayes_prior = 350
# bayes_prior_trade =  100
# rolling_period = 100
# stop = 2, prof = 3

#----Short Term - Short = 0.40, risk = 0.2024993, 7442 Trades
#----Restricted Assets
# trade_sd_fact_post = 1.5
# trade_sd_fact_sigma = 0.25
# bayes_prior = 200
# bayes_prior_trade =  50
# rolling_period = 100
# profit_factor = 2, profit_factor = 4

#----Short Term - Long = 0.37, risk = 0.1154352, 7442 Trades
#----Restricted Assets
# trade_sd_fact_post = 1.5
# trade_sd_fact_sigma = 0.25
# bayes_prior = 200
# bayes_prior_trade =  50
# rolling_period = 100
# stop_factor = 2, profit_factor = 4

markov_bayes_analysis <-
  get_markov_tag_bayes(
    asset_data_combined = asset_data_combined %>%
      filter(
        Asset %in%
          c(
            "AUD_USD", "EUR_USD", "USD_JPY", "GBP_USD",
            "NZD_USD", "USD_SEK", "USD_NOK", "USD_CHF", "USD_SGD",
            "EUR_GBP"
            # "XCU_USD",
            # "NAS100_USD",
            # "NATGAS_USD",
            # "EU50_EUR",  "AU200_AUD","SPX500_USD",
            # "BCO_USD", "WHEAT_USD",
            # "XAG_USD", "WTICO_USD",
            # "USD_MXN"
            )
      )
    ,
    training_perc = 1,
    sd_divides = seq(0.5,2,0.5),
    quantile_divides = seq(0.1,0.9, 0.1),
    rolling_period = 100,
    markov_col_on_interest_pos = "Markov_Point_Pos_roll_sum_1.5",
    markov_col_on_interest_neg = "Markov_Point_Neg_roll_sum_-1.5",
    sum_sd_cut_off = "",
    profit_factor  = 4,
    stop_factor  = 2,
    asset_data_daily_raw = asset_data_combined,
    mean_values_by_asset_for_loop = mean_values_by_asset_for_loop,
    trade_sd_fact_post = 1.5,
    trade_sd_fact_post_high = 1,
    trade_sd_fact_sigma = 0.25,
    bayes_prior = 200,
    bayes_prior_trade = 50
  )

markov_bayes_analysis$Markov_Trades_Bayes_Summary
markov_bayes_long <- markov_bayes_analysis$tagged_trades
stop_factor = 2
profit_factor =4

long_bayes_loop_analysis <-
  generic_trade_finder_loop(
    tagged_trades = markov_bayes_long,
    asset_data_daily_raw = asset_data_daily_raw,
    stop_factor = stop_factor,
    profit_factor =profit_factor,
    trade_col = "trade_col",
    date_col = "Date",
    start_price_col = "Price",
    mean_values_by_asset =
      wrangle_asset_data(
        asset_data_daily_raw = asset_data_daily_raw,
        summarise_means = TRUE
      )
  )

risk_dollar_value <- 20

long_bayes_loop_analysis2 <-
  long_bayes_loop_analysis %>%
  rename(Asset = asset) %>%
  # filter(Asset %in% target_assets) %>%
  left_join(asset_infor %>% rename(Asset = name)) %>%
  mutate(ending_value = str_extract(Asset, "_[A-Z][A-Z][A-Z]") %>% str_remove_all("_")) %>%
  left_join(currency_conversion, by =c("ending_value" = "not_aud_asset")) %>%
  mutate(
    minimumTradeSize = abs(log10(as.numeric(minimumTradeSize))),
    marginRate = as.numeric(marginRate),
    pipLocation = as.numeric(pipLocation),
    displayPrecision = as.numeric(displayPrecision)
  ) %>%
  mutate(
    trade_returns_pip = trade_returns/(10^pipLocation)
  ) %>%
  ungroup() %>%
  mutate(
    stop_value = round(starting_stop_value, abs(pipLocation) ),
    trade_returns = round(trade_returns, abs(pipLocation) )
  ) %>%
  mutate(
    asset_size = floor(log10(trade_start_prices)),
    volume_adjustment =
      case_when(
        str_detect(Asset, "ZAR") & type == "CURRENCY" ~ 10,
        str_detect(Asset, "JPY") & type == "CURRENCY" ~ 100,
        str_detect(Asset, "NOK") & type == "CURRENCY" ~ 10,
        str_detect(Asset, "SEK") & type == "CURRENCY" ~ 10,
        str_detect(Asset, "CZK") & type == "CURRENCY" ~ 10,
        TRUE ~ 1
      )
  ) %>%
  mutate(
    AUD_Price =
      case_when(
        !is.na(adjusted_conversion) ~ (trade_start_prices*adjusted_conversion)/volume_adjustment,
        TRUE ~ trade_start_prices/volume_adjustment
      ),
    stop_value_AUD =
      case_when(
        !is.na(adjusted_conversion) ~ (stop_value*adjusted_conversion)/volume_adjustment,
        TRUE ~ stop_value/volume_adjustment
      ),
    return_value_AUD =
      case_when(
        !is.na(adjusted_conversion) ~ (trade_returns*adjusted_conversion)/volume_adjustment,
        TRUE ~ trade_returns/volume_adjustment
      ),

    volume_unadj =  risk_dollar_value/stop_value_AUD,
    volume_required = volume_unadj,
    volume_adj = round(volume_unadj, minimumTradeSize),
    minimal_loss =  volume_adj*stop_value_AUD,
    trade_return_dollars_AUD =  volume_adj*return_value_AUD,
    trade_value = AUD_Price*volume_adj*marginRate,
    estimated_margin = trade_value,
    volume_required = volume_adj
  )%>%
  dplyr::select(-c(displayPrecision,
                   tradeUnitsPrecision,
                   maximumOrderUnits,
                   maximumPositionSize,
                   maximumTrailingStopDistance,
                   longRate,
                   shortRate,
                   minimumTrailingStopDistance,
                   minimumGuaranteedStopLossDistance,
                   guaranteedStopLossOrderMode,
                   guaranteedStopLossOrderExecutionPremium)
  )

summary_of_trades <-
  long_bayes_loop_analysis2 %>%
  mutate(
    wins = ifelse(trade_returns<0, 0, 1)
  ) %>%
  group_by(trade_col) %>%
  summarise(
    trade_return_dollars_AUD = sum(trade_return_dollars_AUD, na.rm = T),
    wins = sum(wins, na.rm = T),
    total_trades = n()
  ) %>%
  mutate(
    Perc = wins/(total_trades)
  ) %>%
  mutate(
    risk_weighted_return =
      Perc*(profit_factor/stop_factor) - (1- Perc)*(1),
    hypothetical_binomial_20_dollars =
      wins*(20*(profit_factor/stop_factor)) - (total_trades - wins)*(20)
  ) %>%
  ungroup()

long_bayes_loop_ts <-
  long_bayes_loop_analysis2 %>%
  mutate(
    wins = ifelse(trade_return_dollars_AUD<0, 0, 1)
  ) %>%
  group_by(dates, trade_col) %>%
  summarise(
    trade_return_dollars_AUD = sum(trade_return_dollars_AUD, na.rm = T),
    wins = sum(wins, na.rm = T),
    total_trades = n()
  ) %>%
  group_by(trade_col) %>%
  arrange(dates, .by_group = TRUE) %>%
  group_by(trade_col) %>%
  mutate(
    cumulative_aud = cumsum(trade_return_dollars_AUD)
  ) %>%
  group_by(trade_col) %>%
  mutate(
    total_trades_total = sum(total_trades),
    total_wins = sum(wins)
  ) %>%
  group_by(trade_col) %>%
  mutate(
    Perc = total_wins/ (total_trades_total)
  ) %>%
  mutate(
    risk_weighted_return =
      Perc*(profit_factor/stop_factor) - (1- Perc)*(1),
    hypothetical_binomial_20_dollars =
      total_wins*(20*(profit_factor/stop_factor)) - (total_trades_total - total_wins)*(20)
  ) %>%
  ungroup()


long_bayes_loop_ts %>%
  filter(trade_col == "Short") %>%
  ggplot(aes(x = dates, y = cumulative_aud)) +
  geom_line(aes(color = trade_col) ) + theme_minimal()

