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
  start_date = (today() - days(5)) %>% as.character()
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
save_db_path_PCA = "C:/Users/Nikhil Chandra/Documents/trade_data/PCA_Rolling_equity_temp.db"
db_con <- connect_db(save_db_path_PCA)
PCA_data <- DBI::dbGetQuery(conn = db_con, statement = "SELECT * FROM PCA_Rolling_equity_temp") %>%
  mutate(Date = as_datetime(Date))
DBI::dbDisconnect(db_con)

all_tagged_trades_equity <-
  equity_index_asset_model_trades(
    major_indices_log_cumulative = major_indices_log_cumulative ,
    PCA_Data = PCA_data,
    assets_to_use = c("SPX500_USD", "US2000_USD", "NAS100_USD", "SG30_SGD", "AU200_AUD", "EU50_EUR", "DE30_EUR"),
    samples_for_MLE = 0.5,
    test_samples = 0.45,
    rolling_period = 100,
    date_filter_min = "2018-01-01",
    stop_factor = 4,
    profit_factor = 8,
    stop_factor_long = 10,
    profit_factor_long = 15
  )

analysis_data <-
  equity_index_asset_model(
    # asset_data_ask = major_indices_log_cumulative,
    # asset_data_bid = major_indices_log_cumulative_bid,

    asset_data_ask = major_indices,
    asset_data_bid = major_indices_bid,
    asset_1_tag = all_tagged_trades_equity[[1]],
    asset_2_tag = all_tagged_trades_equity[[2]],
    asset_3_tag = all_tagged_trades_equity[[3]],
    asset_4_tag = all_tagged_trades_equity[[4]],
    asset_5_tag = all_tagged_trades_equity[[5]],
    risk_dollar_value = 5,
    stop_factor = 4,
    profit_factor = 8,
    stop_factor_long = 10,
    profit_factor_long = 15,
    control_random_samples,
    US2000_bench_mark = US2000_bench_mark,
    SPX_bench_mark = SPX_bench_mark
  )


analysis_comparison <- analysis_data[[1]] %>% distinct()
analysis_data[[2]]

#----------------------------------------------------
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
    major_indices_log_cumulative = major_indices_log_cumulative ,
    PCA_Data = NULL,
    assets_to_use = c("SPX500_USD", "US2000_USD", "NAS100_USD", "SG30_SGD", "AU200_AUD", "EU50_EUR", "DE30_EUR"),
    samples_for_MLE = 0.5,
    test_samples = 0.45,
    rolling_period = 100,
    date_filter_min = "2018-01-01",
    # stop_factor = 4,
    # profit_factor = 8,
    # stop_factor_long = 10,
    # profit_factor_long = 15
    stop_factor = 4,
    profit_factor = 8,
    stop_factor_long = 4,
    profit_factor_long = 8,
    price_col = "Return_Index_Diff"
  ) {

    major_indices_log_cumulative <-
      major_indices_log_cumulative %>%
      group_by(Asset) %>%
      mutate(
        Return_Index_Diff = ((Price - Open)/Open)*100
      ) %>%
      ungroup() %>%
      filter(!is.na(Return_Index_Diff)) %>%
      filter(Date >= date_filter_min)

    if(is.null(PCA_Data)) {
      major_indices_PCA_Index <-
        create_PCA_Asset_Index(
          asset_data_to_use = major_indices_log_cumulative,
          asset_to_use = assets_to_use,
          price_col = price_col
        ) %>%
        rename(
          Average_PCA_Index = Average_PCA
        )
    }

    if(!is.null(PCA_Data)) {
      major_indices_PCA_Index <-
        PCA_Data  %>%
        rename(
          Average_PCA_Index = Average_PCA
        )
    }

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
                 rolling_cor_PC1 <= rolling_cor_PC1_mean - 2.0*rolling_cor_PC1_sd & tan_angle > 0 ~ "Long", #19%
                 rolling_cor_PC1 >= rolling_cor_PC1_mean + 2.75*rolling_cor_PC1_sd & tan_angle < 0 ~ "Long", #16%
                 rolling_cor_PC2 >= rolling_cor_PC2_mean + 2*rolling_cor_PC2_sd & tan_angle > 0 ~ "Long",  #20%
                 rolling_cor_PC3 <= rolling_cor_PC3_mean - 2.25*rolling_cor_PC3_sd & tan_angle > 0 ~ "Long", #4%
                 rolling_cor_PC3 <= rolling_cor_PC3_mean - 2.25*rolling_cor_PC3_sd & tan_angle < 0 ~ "Short", #13%
                 rolling_cor_PC3 >= rolling_cor_PC3_mean + 2.5*rolling_cor_PC3_sd & tan_angle > 0 ~ "Long", #4%
                 rolling_cor_PC3 >= rolling_cor_PC3_mean + 2.5*rolling_cor_PC3_sd & tan_angle < 0 ~ "Long", #13%
                 rolling_cor_PC4 <= rolling_cor_PC4_mean - 2.5*rolling_cor_PC4_sd & tan_angle > 0 ~ "Long", #15%
                 rolling_cor_PC4 <= rolling_cor_PC4_mean - 2.5*rolling_cor_PC4_sd & tan_angle < 0 ~ "Long",  #15%
                 rolling_cor_PC4 >= rolling_cor_PC4_mean + 2.25*rolling_cor_PC4_sd & tan_angle < 0 ~ "Short" #18%
               )
      ) %>%
      filter(!is.na(trade_col))

    asset_2_tag <-
      asset_2 %>%
      mutate(trade_col =
               case_when(
                 rolling_cor_PC1 <= rolling_cor_PC1_mean - 2.75*rolling_cor_PC1_sd & tan_angle > 0 ~ "Short", #7%
                 rolling_cor_PC1 >= rolling_cor_PC1_mean + 2*rolling_cor_PC1_sd & tan_angle > 0 ~ "Long", #13%
                 rolling_cor_PC2 <= rolling_cor_PC2_mean - 2.25*rolling_cor_PC2_sd & tan_angle > 0 ~ "Long", #7%
                 rolling_cor_PC2 >= rolling_cor_PC2_mean + 2.5*rolling_cor_PC2_sd & tan_angle < 0 ~ "Short",
                 rolling_cor_PC2 >= rolling_cor_PC2_mean + 2.25*rolling_cor_PC2_sd & tan_angle > 0 ~ "Long",
                 rolling_cor_PC4 <= rolling_cor_PC4_mean - 2.5*rolling_cor_PC4_sd & tan_angle > 0 ~ "Long"
               )
      ) %>%
      filter(!is.na(trade_col))

    # EU50_EUR
    asset_3_tag <-
      asset_3 %>%
      mutate(trade_col =
               case_when(
                 rolling_cor_PC1 <= rolling_cor_PC1_mean - 2.5*rolling_cor_PC1_sd & tan_angle < 0 ~ "Short",
                 rolling_cor_PC1 <= rolling_cor_PC1_mean - 2.5*rolling_cor_PC1_sd & tan_angle > 0 ~ "Short",

                 rolling_cor_PC1 >= rolling_cor_PC1_mean + 2.5*rolling_cor_PC1_sd & tan_angle < 0 ~ "Long"
                 # rolling_cor_PC1 >= rolling_cor_PC1_mean + 2.5*rolling_cor_PC1_sd & tan_angle > 0 ~ "Short"
               )
      ) %>%
      filter(!is.na(trade_col))

    # AU200_AUD
    asset_4_tag <-
      asset_4 %>%
      mutate(trade_col =
               case_when(
                 rolling_cor_PC1 <= rolling_cor_PC1_mean - 2.5*rolling_cor_PC1_sd & tan_angle < 0 ~ "Long",
                 rolling_cor_PC1 <= rolling_cor_PC1_mean - 2.5*rolling_cor_PC1_sd & tan_angle > 0 ~ "Short"
               )
      ) %>%
      filter(!is.na(trade_col))

    asset_5_tag <-
      asset_5 %>%
      mutate(trade_col =
               case_when(
                 # rolling_cor_PC1 <= rolling_cor_PC1_mean - 2.5*rolling_cor_PC1_sd & tan_angle < 0 ~ "Short",
                 # rolling_cor_PC1 <= rolling_cor_PC1_mean - 2.5*rolling_cor_PC1_sd & tan_angle > 0 ~ "Long"

                 rolling_cor_PC1 <= rolling_cor_PC1_mean - 2.5*rolling_cor_PC1_sd ~ "Short",
                 rolling_cor_PC1 >= rolling_cor_PC1_mean + 2.5*rolling_cor_PC1_sd ~ "Long"

               )
      ) %>%
      filter(!is.na(trade_col))

    all_distinct_trade_dates <-
      list(
        asset_1_tag,
        asset_2_tag,
        asset_3_tag,
        asset_4_tag,
        asset_5_tag
      ) %>%
      map_dfr(
        ~ .x %>% ungroup() %>% dplyr::select(Date)
      ) %>%
      distinct()

    dates_missing_trades <-
      asset_1 %>%
      anti_join(all_distinct_trade_dates) %>%
      mutate(
        trade_col = "Long"
      )

    all_tagged_trades_equity <-
      list(
        asset_1_tag,
        asset_2_tag,
        asset_3_tag,
        asset_4_tag,
        asset_5_tag,
        dates_missing_trades
      )

    analysis_data <-
      equity_index_asset_model(
        # asset_data_ask = major_indices_log_cumulative,
        # asset_data_bid = major_indices_log_cumulative_bid,

        asset_data_ask = major_indices,
        asset_data_bid = major_indices_bid,
        asset_1_tag = all_tagged_trades_equity[[1]],
        asset_2_tag = all_tagged_trades_equity[[2]],
        asset_3_tag = all_tagged_trades_equity[[3]],
        asset_4_tag = all_tagged_trades_equity[[4]],
        asset_5_tag = all_tagged_trades_equity[[5]],
        risk_dollar_value = 5,
        stop_factor = stop_factor,
        profit_factor = profit_factor,
        stop_factor_long = stop_factor_long,
        profit_factor_long = profit_factor_long,
        control_random_samples,
        US2000_bench_mark = US2000_bench_mark,
        SPX_bench_mark = SPX_bench_mark
      )


    analysis_comparison <- analysis_data[[1]] %>% distinct()
    analysis_data[[2]]
    analysis_comparison$Final_Dollars %>% sum()
    benchmark_performance <- SPX_bench_mark[[3]] %>% distinct() %>%
      dplyr::select(
        risk_weighted_return_bench = risk_weighted_return,
        Total_Trades_SPY_Bench = Trades,
        Total_wins_SPY_Bench = wins
      )

    total_performance <-
      analysis_comparison %>%
      ungroup() %>%
      summarise(Trades = sum(Trades),
                wins = sum(wins),
                profit_factor = mean(profit_factor),
                stop_factor = mean(stop_factor)) %>%
      mutate(
        risk_weighted_return_strat =
          (profit_factor/stop_factor)*(wins/Trades) - (Trades - wins)/Trades
      ) %>%
      bind_cols(benchmark_performance)

    return(list(
      asset_1_tag,
      asset_2_tag,
      asset_3_tag,
      asset_4_tag,
      asset_5_tag
    ))

  }

