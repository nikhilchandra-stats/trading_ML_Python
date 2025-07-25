helperfunctions35South::load_custom_functions()
one_drive_path <- helperfunctions35South::create_one_drive_path(
  path_extension = "raw data")
library(neuralnet)
all_aud_symbols <- get_oanda_symbols() %>%
  keep(~ str_detect(.x, "AUD")|str_detect(.x, "USD_SEK|USD_NOK|USD_HUF|USD_ZAR|USD_CNY|USD_MXN|USD_CNH"))
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
raw_macro_data <- niksmacrohelpers::get_macro_event_data()

currency_conversion <-
  aud_usd_today %>%
  mutate(
    not_aud_asset = ending_value
  ) %>%
  dplyr::select(not_aud_asset, adjusted_conversion) %>%
  bind_rows(
    tibble(not_aud_asset = "AUD", adjusted_conversion = 1)
  )


db_location = "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data For EDA.db"
start_date = "2011-01-01"
end_date = today() %>% as.character()
time_frame = "H1"

major_indices <-
  get_all_major_indices(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame
  )

major_indices_bid <-
  get_all_major_indices(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame
  )

mean_values_by_asset_for_loop <-
  wrangle_asset_data(
    asset_data_daily_raw = major_indices,
    summarise_means = TRUE
  )

major_indices$Asset %>% unique()
gc()

control_random_samples <-
  get_random_samples_MLE_beta(
    random_results_db_location = "C:/Users/Nikhil Chandra/Documents/trade_data/random_results.db",
    stop_factor = 5,
    profit_factor = 10,
    analysis_syms = c("AU200_AUD", "SPX500_USD", "EU50_EUR", "US2000_USD"),
    time_frame = "H1",
    return_summary = TRUE
  )

major_indices_log_cumulative <-
  c("SPX500_USD", "US2000_USD", "NAS100_USD", "SG30_SGD", "AU200_AUD", "EU50_EUR", "DE30_EUR") %>%
  map_dfr(
    ~
      create_log_cumulative_returns(
        asset_data_to_use = major_indices,
        asset_to_use = c(.x[1]),
        price_col = "Open",
        return_long_format = TRUE
      )
  ) %>%
  left_join(
    major_indices %>% distinct(Date, Asset, Price, Open, High, Low)
  )

major_indices_log_cumulative_bid <-
  c("SPX500_USD", "US2000_USD", "NAS100_USD", "SG30_SGD", "AU200_AUD", "EU50_EUR", "DE30_EUR") %>%
  map_dfr(
    ~
      create_log_cumulative_returns(
        asset_data_to_use = major_indices_bid,
        asset_to_use = c(.x[1]),
        price_col = "Open",
        return_long_format = TRUE
      )
  ) %>%
  left_join(
    major_indices_bid %>% distinct(Date, Asset, Price, Open, High, Low)
  )

SPX_bench_mark <-
  get_bench_mark_results(
    asset_data = major_indices_log_cumulative,
    Asset_Var = "SPX500_USD",
    min_date = "2018-01-01",
    trade_direction = "Long",
    risk_dollar_value = 5,
    stop_factor = 4,
    profit_factor = 8,
    mean_values_by_asset_for_loop = mean_values_by_asset_for_loop,
    currency_conversion = currency_conversion
  )

US2000_bench_mark <-
  get_bench_mark_results(
    asset_data = major_indices_log_cumulative_bid,
    Asset_Var = "US2000_USD",
    min_date = "2018-01-01",
    trade_direction = "Short",
    risk_dollar_value = 5,
    stop_factor = 4,
    profit_factor = 8,
    mean_values_by_asset_for_loop = mean_values_by_asset_for_loop,
    currency_conversion = currency_conversion
  )

#Rolling Period 100
first_date <- major_indices_log_cumulative %>% filter(Date >= "2018-01-01") %>%
  pull(Date) %>% min(na.rm = T)
last_date <- major_indices_log_cumulative %>% filter(Date >= "2018-01-01") %>%
  pull(Date) %>% max(na.rm = T)
simulation_date_vec <- seq(first_date,last_date, "hour" )
all_analysis_stored <- list()
equity_sim_db_location = "C:/Users/Nikhil Chandra/Documents/trade_data/equity_sim_results.db"
db_con_quity_sim <- connect_db(equity_sim_db_location)
new_table <- TRUE
c = 0

for (i in 813:length(simulation_date_vec)) {

  all_tagged_trades_equity <-
    equity_index_asset_model_trades(
      major_indices_log_cumulative = major_indices_log_cumulative %>% filter(Date <= simulation_date_vec[i]) ,
      assets_to_use = c("SPX500_USD", "US2000_USD", "NAS100_USD", "SG30_SGD", "AU200_AUD", "EU50_EUR", "DE30_EUR"),
      samples_for_MLE = 0.5,
      test_samples = 0.45,
      rolling_period = 100,
      date_filter_min = "2011-01-01",
      stop_factor = 4,
      profit_factor = 8,
      stop_factor_long = 10,
      profit_factor_long = 15
    )

  all_tagged_trades_equity2 <-
    all_tagged_trades_equity %>%
    map(
      ~ .x %>%
        group_by(Asset, trade_col) %>%
        slice_max(Date)
    )

  check_if_trades_present <-
    all_tagged_trades_equity2 %>%
    map_dfr(bind_rows) %>%
    filter(Date == simulation_date_vec[i])

  if(dim(check_if_trades_present)[1] > 0) {

    c = c + 1
    analysis_data <-
      equity_index_asset_model(
        asset_data_ask = major_indices_log_cumulative,
        asset_data_bid = major_indices_log_cumulative_bid,
        asset_1_tag = all_tagged_trades_equity2[[1]],
        asset_2_tag = all_tagged_trades_equity2[[2]],
        asset_3_tag = all_tagged_trades_equity2[[3]],
        asset_4_tag = all_tagged_trades_equity2[[4]],
        asset_5_tag = all_tagged_trades_equity2[[5]],
        risk_dollar_value = 5,
        stop_factor = 4,
        profit_factor = 8,
        stop_factor_long = 10,
        profit_factor_long = 15,
        control_random_samples
      )

    analysis_data_with_margin <-
      analysis_data %>%
      ungroup() %>%
      mutate(kk = row_number()) %>%
      mutate(trade_col = trade_direction) %>%
      dplyr::select(Date, Asset, stop_factor, profit_factor, trade_col, kk, Price) %>%
      split(.$kk) %>%
      map_dfr(
        ~ get_stops_profs_volume_trades(
          tagged_trades = .x,
          mean_values_by_asset = mean_values_by_asset_for_loop,
          trade_col = "trade_col",
          currency_conversion = currency_conversion,
          risk_dollar_value = risk_dollar_value,
          stop_factor = .x$stop_factor[1],
          profit_factor =.x$profit_factor[1],
          asset_col = "Asset",
          stop_col = "stop_value",
          profit_col = "profit_value",
          price_col = "Price",
          trade_return_col = "trade_returns"
        )
      ) %>%
      dplyr::select(Date, Asset, trade_col, estimated_margin, volume_adj)

    full_sim_data_at_time_i <-
      analysis_data %>%
      mutate(Date, Asset, Final_Dollars, Price, Open, High, Low, trade_col = trade_direction ) %>%
      left_join(analysis_data_with_margin) %>%
      filter(Date == simulation_date_vec[i])

    all_analysis_stored[[c]] <- full_sim_data_at_time_i

    if(new_table == TRUE & c == 1 ) {
      write_table_sql_lite(conn = db_con_quity_sim,
                           .data = full_sim_data_at_time_i,
                           table_name = "equity_sim_results",
                           overwrite_true = TRUE)
    } else {
      append_table_sql_lite(conn = db_con_quity_sim,
                           .data = full_sim_data_at_time_i,
                           table_name = "equity_sim_results")
    }

  }

}

test <- all_analysis_stored %>%
  map_dfr(bind_rows) %>%
  filter(Date >= "2018-01-01") %>%
  group_by(Asset, trade_col) %>%
  summarise(Returns = sum(Final_Dollars))
test$Returns %>% sum()


#' equity_index_asset_model
#'
#' @param major_indices_log_cumulative
#' @param assets_to_use
#' @param samples_for_MLE
#' @param test_samples
#' @param rolling_period
#' @param asset1
#' @param asset2
#' @param asset_1_fac
#' @param asset_2_fac
#'
#' @return
#' @export
#'
#' @examples
equity_index_asset_model_trades <-
  function(
    major_indices_log_cumulative = major_indices_log_cumulative_raw ,
    assets_to_use = c("SPX500_USD", "US2000_USD", "NAS100_USD", "SG30_SGD", "AU200_AUD", "EU50_EUR", "DE30_EUR"),
    samples_for_MLE = 0.5,
    test_samples = 0.45,
    rolling_period = 100,
    date_filter_min = "2018-01-01",
    stop_factor = 4,
    profit_factor = 8,
    stop_factor_long = 10,
    profit_factor_long = 15
  ) {

    major_indices_log_cumulative <-
      major_indices_log_cumulative %>%
      group_by(Asset) %>%
      mutate(
        Return_Index_Diff = ((Price - Open)/Open)*100
      ) %>%
      ungroup() %>%
      filter(!is.na(Return_Index_Diff))
    # filter(Date >= date_filter_min)

    major_indices_PCA_Index <-
      create_PCA_Asset_Index(
        asset_data_to_use = major_indices_log_cumulative,
        asset_to_use = assets_to_use,
        price_col = "Return_Index"
      ) %>%
      rename(
        Average_PCA_Index = Average_PCA
      )

    major_indices_cumulative_pca <-
      major_indices_log_cumulative %>%
      left_join(major_indices_PCA_Index)

    asset_1 <-
      get_PCA_Index_rolling_cor_sd_mean(
        raw_asset_data_for_PCA_cor = major_indices_log_cumulative %>% filter(Asset == "SPX500_USD"),
        PCA_data = major_indices_cumulative_pca,
        rolling_period = rolling_period
      )

    asset_2 <-
      get_PCA_Index_rolling_cor_sd_mean(
        raw_asset_data_for_PCA_cor = major_indices_log_cumulative %>% filter(Asset == "US2000_USD"),
        PCA_data = major_indices_cumulative_pca,
        rolling_period = rolling_period
      )

    asset_3 <-
      get_PCA_Index_rolling_cor_sd_mean(
        raw_asset_data_for_PCA_cor = major_indices_log_cumulative %>% filter(Asset == "EU50_EUR"),
        PCA_data = major_indices_cumulative_pca,
        rolling_period = rolling_period
      )

    asset_4 <-
      get_PCA_Index_rolling_cor_sd_mean(
        raw_asset_data_for_PCA_cor = major_indices_log_cumulative %>% filter(Asset == "AU200_AUD"),
        PCA_data = major_indices_cumulative_pca,
        rolling_period = rolling_period
      )

    asset_5 <-
      get_PCA_Index_rolling_cor_sd_mean(
        raw_asset_data_for_PCA_cor = major_indices_log_cumulative %>% filter(Asset == "SG30_SGD"),
        PCA_data = major_indices_cumulative_pca,
        rolling_period = rolling_period
      )

    asset_1_tag <-
      asset_1 %>%
      mutate(trade_col =
               case_when(
                 rolling_cor_PC2 <= rolling_cor_PC2_mean - 2*rolling_cor_PC2_sd & tan_angle < 0 ~ "Long",
                 rolling_cor_PC2 >= rolling_cor_PC2_mean + 1.5*rolling_cor_PC2_sd & tan_angle < 0 ~ "Short",
                 rolling_cor_PC1 >= rolling_cor_PC1_mean + 1*rolling_cor_PC1_sd & tan_angle < 0 ~ "Short",
                 rolling_cor_PC3 <= rolling_cor_PC3_mean - 1.5*rolling_cor_PC3_sd & tan_angle > 0 ~ "Long",
                 rolling_cor_PC3 >= rolling_cor_PC3_mean + 3.5*rolling_cor_PC3_sd & tan_angle < 0 ~ "Short",
                 rolling_cor_PC4 >= rolling_cor_PC4_mean + 2.5*rolling_cor_PC4_sd & tan_angle > 0 ~ "Long"
               )
      ) %>%
      filter(!is.na(trade_col))

    asset_2_tag <-
      asset_2 %>%
      mutate(trade_col =
               case_when(
                 rolling_cor_PC2 <= rolling_cor_PC2_mean - 4*rolling_cor_PC2_sd & tan_angle < 0  ~ "Long",
                 rolling_cor_PC2 >= rolling_cor_PC2_mean + 1*rolling_cor_PC2_sd & tan_angle < 0 ~ "Short",
                 rolling_cor_PC2 <= rolling_cor_PC2_mean - 4*rolling_cor_PC2_sd & tan_angle > 0 ~ "Long",
                 rolling_cor_PC1 >= rolling_cor_PC1_mean + 1*rolling_cor_PC1_sd & tan_angle < 0 ~ "Short",
                 rolling_cor_PC1 >= rolling_cor_PC1_mean + 2*rolling_cor_PC1_sd & tan_angle > 0  ~ "Long"
               )
      ) %>%
      filter(!is.na(trade_col))

    asset_3_tag <-
      asset_3 %>%
      mutate(trade_col =
               case_when(
                 rolling_cor_PC2 <= rolling_cor_PC2_mean - 2*rolling_cor_PC2_sd & tan_angle < 0 ~ "Long",
                 rolling_cor_PC2 >= rolling_cor_PC2_mean + 1.5*rolling_cor_PC2_sd & tan_angle > 0 ~ "Short",
                 rolling_cor_PC1 >= rolling_cor_PC1_mean + 2*rolling_cor_PC1_sd & tan_angle > 0 ~ "Short",
                 rolling_cor_PC1 <= rolling_cor_PC1_mean - 1.5*rolling_cor_PC1_sd & tan_angle > 0 ~ "Long",
                 rolling_cor_PC3 <= rolling_cor_PC3_mean - 2.5*rolling_cor_PC3_sd & tan_angle < 0 ~ "Long",
                 rolling_cor_PC3 >= rolling_cor_PC3_mean + 2*rolling_cor_PC3_sd & tan_angle > 0 ~ "Long",
                 rolling_cor_PC3 <= rolling_cor_PC3_mean - 2.5*rolling_cor_PC3_sd & tan_angle > 0 ~ "Short"


               )
      ) %>%
      filter(!is.na(trade_col))

    asset_4_tag <-
      asset_4 %>%
      mutate(trade_col =
               case_when(
                 rolling_cor_PC2 >= rolling_cor_PC2_mean + 2*rolling_cor_PC2_sd & tan_angle > 0 ~ "Long",
                 rolling_cor_PC1 <= rolling_cor_PC1_mean - 2*rolling_cor_PC1_sd & tan_angle > 0 ~ "Long",
                 rolling_cor_PC3 <= rolling_cor_PC3_mean - 2*rolling_cor_PC3_sd & tan_angle > 0 ~ "Long",
                 rolling_cor_PC3 >= rolling_cor_PC3_mean + 2*rolling_cor_PC3_sd & tan_angle > 0 ~ "Long",
                 rolling_cor_PC3 >= rolling_cor_PC3_mean + 3.5*rolling_cor_PC3_sd & tan_angle < 0 ~ "Short",
                 rolling_cor_PC4 <= rolling_cor_PC4_mean - 2*rolling_cor_PC4_sd & tan_angle < 0 ~ "Long",
                 rolling_cor_PC4 <= rolling_cor_PC4_mean - 1.75*rolling_cor_PC4_sd & tan_angle > 0 ~ "Short"

               )
      ) %>%
      filter(!is.na(trade_col))

    asset_5_tag <-
      asset_5 %>%
      mutate(trade_col =
               case_when(
                 rolling_cor_PC1 >= rolling_cor_PC1_mean + 2*rolling_cor_PC1_sd & tan_angle < 0 ~ "Short",
                 rolling_cor_PC1 <= rolling_cor_PC1_mean - 2*rolling_cor_PC1_sd & tan_angle > 0 ~ "Long",
                 rolling_cor_PC2 >= rolling_cor_PC2_mean + 2*rolling_cor_PC2_sd & tan_angle < 0 ~ "Short",
                 rolling_cor_PC2 <= rolling_cor_PC2_mean - 2*rolling_cor_PC2_sd & tan_angle > 0 ~ "Long",
                 rolling_cor_PC3 <= rolling_cor_PC3_mean - 2*rolling_cor_PC3_sd & tan_angle > 0 ~ "Long",
                 rolling_cor_PC4 <= rolling_cor_PC4_mean - 2.5*rolling_cor_PC4_sd & tan_angle > 0 ~ "Long",
                 rolling_cor_PC4 >= rolling_cor_PC4_mean + 2*rolling_cor_PC4_sd & tan_angle > 0 ~ "Long"

               )
      ) %>%
      filter(!is.na(trade_col))

    return(list(
      asset_1_tag,
      asset_2_tag,
      asset_3_tag,
      asset_4_tag,
      asset_5_tag
    ))

  }

#' mean_values_by_asset_for_loop
#'
#' @param tagged_trades
#' @param stop_factor
#' @param profit_factor
#' @param raw_asset_data
#'
#' @return
#' @export
#'
#' @examples
run_pairs_analysis <- function(
    tagged_trades = tagged_trades_AUD_USD %>% bind_rows(tagged_trades_NZD_USD),
    stop_factor = 5,
    profit_factor = 10,
    raw_asset_data = AUD_USD_NZD_USD,
    risk_dollar_value = 10,
    return_trade_ts = FALSE
) {

  mean_values_by_asset_for_loop <-
    wrangle_asset_data(
      asset_data_daily_raw = raw_asset_data,
      summarise_means = TRUE
    )

  long_bayes_loop_analysis<-
    generic_trade_finder_loop(
      tagged_trades = tagged_trades ,
      asset_data_daily_raw = raw_asset_data,
      stop_factor = stop_factor,
      profit_factor =profit_factor,
      trade_col = "trade_col",
      date_col = "Date",
      start_price_col = "Price",
      mean_values_by_asset = mean_values_by_asset_for_loop
    )

  trade_timings <-
    long_bayes_loop_analysis %>%
    mutate(
      ending_date_trade = as_datetime(ending_date_trade),
      dates = as_datetime(dates)
    ) %>%
    mutate(Time_Required = (ending_date_trade - dates)/dhours(1) )

  trade_timings_by_asset <- trade_timings %>%
    mutate(win_loss = ifelse(trade_returns < 0, "loss", "wins") ) %>%
    group_by(win_loss) %>%
    summarise(
      Time_Required = median(Time_Required, na.rm = T)
    )

  analysis_data <-
    generic_anlyser(
      trade_data = long_bayes_loop_analysis %>% rename(Asset = asset),
      profit_factor = profit_factor,
      stop_factor = stop_factor,
      asset_infor = asset_infor,
      currency_conversion = currency_conversion,
      asset_col = "Asset",
      stop_col = "starting_stop_value",
      profit_col = "starting_profit_value",
      price_col = "trade_start_prices",
      trade_return_col = "trade_returns",
      risk_dollar_value = risk_dollar_value,
      grouping_vars = "trade_col"
    ) %>%
    bind_cols(trade_timings_by_asset)

  analysis_data_asset <-
    generic_anlyser(
      trade_data = long_bayes_loop_analysis %>% rename(Asset = asset),
      profit_factor = profit_factor,
      stop_factor = stop_factor,
      asset_infor = asset_infor,
      currency_conversion = currency_conversion,
      asset_col = "Asset",
      stop_col = "starting_stop_value",
      profit_col = "starting_profit_value",
      price_col = "trade_start_prices",
      trade_return_col = "trade_returns",
      risk_dollar_value = risk_dollar_value,
      grouping_vars = "Asset"
    ) %>%
    bind_cols(trade_timings_by_asset)

  if(return_trade_ts == TRUE) {
    return(long_bayes_loop_analysis)
  } else {
    return(list(analysis_data, analysis_data_asset))
  }

}

#' equity_index_asset_model
#'
#' @param major_indices_log_cumulative
#' @param assets_to_use
#' @param samples_for_MLE
#' @param test_samples
#' @param rolling_period
#' @param asset1
#' @param asset2
#' @param asset_1_fac
#' @param asset_2_fac
#'
#' @return
#' @export
#'
#' @examples
equity_index_asset_model <-
  function(
    asset_data_ask = major_indices_log_cumulative,
    asset_data_bid = major_indices_log_cumulative_bid,
    asset_1_tag,
    asset_2_tag,
    asset_3_tag,
    asset_4_tag,
    asset_5_tag,
    risk_dollar_value = 5,
    stop_factor = 4,
    profit_factor = 8,
    stop_factor_long = 10,
    profit_factor_long = 15,
    control_random_samples
  ) {

    combined_results <-
      get_5_index_equity_combined_results(
        asset_1_tag_trades = asset_1_tag,
        asset_2_tag_trades = asset_2_tag,
        asset_3_tag_trades = asset_3_tag,
        asset_4_tag_trades = asset_4_tag,
        asset_5_tag_trades = asset_5_tag,
        asset_data_ask = asset_data_ask,
        asset_data_bid = asset_data_bid,
        risk_dollar_value = risk_dollar_value,
        profit_factor = profit_factor,
        stop_factor = stop_factor,
        profit_factor_long = profit_factor_long,
        stop_factor_long = stop_factor_long,
        control_random_samples = control_random_samples
      )

    all_trade_dates <-
      list(asset_1_tag,
           asset_2_tag,
           asset_3_tag,
           asset_4_tag,
           asset_5_tag) %>%
      map_dfr(
        ~ .x %>% dplyr::select(Date, Asset, trade_direction = trade_col, Price, Open, High, Low)
      )

    combined_results_benched <-
      combined_results %>%
      left_join(all_trade_dates)


    return(combined_results_benched)

  }

