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
all_tagged_trades_equity <-
  equity_index_asset_model_trades(
    major_indices_log_cumulative = major_indices_log_cumulative ,
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

all_tagged_trades_equity_dfr <-
  all_tagged_trades_equity %>%
  map_dfr(bind_rows) %>%
  slice_max(Date)

analysis_data <-
  equity_index_asset_model(
    asset_data_ask = major_indices_log_cumulative,
    asset_data_bid = major_indices_log_cumulative_bid,
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

analysis_comparison <- analysis_data[[1]]
analysis_data[[2]]

#Rolling Period 20

SPX_bench_mark <-
  get_bench_mark_results(
    asset_data = major_indices_log_cumulative,
    Asset_Var = "SPX500_USD",
    min_date = "2018-01-01",
    trade_direction = "Long",
    risk_dollar_value = 5,
    stop_factor = 3,
    profit_factor = 6,
    mean_values_by_asset_for_loop = mean_values_by_asset_for_loop,
    currency_conversion = currency_conversion
  )

US2000_bench_mark_short <-
  get_bench_mark_results(
    asset_data = major_indices_log_cumulative_bid,
    Asset_Var = "US2000_USD",
    min_date = "2018-01-01",
    trade_direction = "Long",
    risk_dollar_value = 5,
    stop_factor = 3,
    profit_factor = 6,
    mean_values_by_asset_for_loop = mean_values_by_asset_for_loop,
    currency_conversion = currency_conversion
  )

US2000_bench_mark_short <-
  get_bench_mark_results(
    asset_data = major_indices_log_cumulative_bid,
    Asset_Var = "US2000_USD",
    min_date = "2018-01-01",
    trade_direction = "Short",
    risk_dollar_value = 5,
    stop_factor = 3,
    profit_factor = 6,
    mean_values_by_asset_for_loop = mean_values_by_asset_for_loop,
    currency_conversion = currency_conversion
  )

all_tagged_trades_equity <-
  equity_index_asset_model_trades(
  major_indices_log_cumulative = major_indices_log_cumulative ,
  assets_to_use = c("SPX500_USD", "US2000_USD", "NAS100_USD", "SG30_SGD", "AU200_AUD", "EU50_EUR", "DE30_EUR"),
  samples_for_MLE = 0.5,
  test_samples = 0.45,
  rolling_period = 20,
  date_filter_min = "2018-01-01",
  stop_factor = 3,
  profit_factor = 6,
  stop_factor_long = 3,
  profit_factor_long = 6
)

all_tagged_trades_equity_dfr <-
  all_tagged_trades_equity %>%
  map_dfr(bind_rows) %>%
  slice_max(Date)

analysis_data <-
  equity_index_asset_model(
    asset_data_ask = major_indices_log_cumulative,
    asset_data_bid = major_indices_log_cumulative_bid,
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

analysis_comparison <- analysis_data[[1]]
analysis_data[[2]]


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
      filter(!is.na(Return_Index_Diff)) %>%
      filter(Date >= date_filter_min)

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

#-----------------------------------------------------------------------------------------
mean(rbeta(n = 900000, shape1 = 5000, shape2 = 5000))
samples <- 1000
random_results_db_location <- "C:/Users/Nikhil Chandra/Documents/trade_data/random_results.db"
db_con <- connect_db(random_results_db_location)
stop_factor = 5
profit_factor = 10
analysis_syms = c("AU200_AUD", "SPX500_USD", "EU50_EUR", "US2000_USD")
trade_samples = 10000
new_table = FALSE
time_frame = "H1"

for (i in 1:samples) {

  temp_results <-
    get_random_results_trades(
      raw_asset_data_ask = major_indices,
      raw_asset_data_bid = major_indices_bid,
      stop_factor = stop_factor,
      profit_factor = profit_factor,
      risk_dollar_value = 10,
      analysis_syms = analysis_syms,
      trade_samples = trade_samples
    )

  complete_results <-
    temp_results[[1]] %>%
    bind_rows(temp_results[[2]]) %>%
    mutate(trade_samples = trade_samples,
           time_frame = time_frame)

  if(new_table == TRUE) {
    write_table_sql_lite(.data = complete_results,
                         table_name = "random_results",
                         conn = db_con,
                         overwrite_true = TRUE)
  }

  if(new_table == FALSE) {
    append_table_sql_lite(
      .data = complete_results,
      table_name = "random_results",
      conn = db_con
    )
  }

}

DBI::dbDisconnect(db_con)


create_Index_PCA_copula <-
  function(
    major_indices_log_cumulative = major_indices_log_cumulative,
    assets_to_use = c("SPX500_USD", "US2000_USD", "NAS100_USD", "SG30_SGD", "AU200_AUD", "EU50_EUR", "DE30_EUR"),
    samples_for_MLE = 0.5,
    test_samples = 0.45,
    rolling_period = 100
    ) {

    major_indices_log_cumulative <-
      major_indices_log_cumulative %>%
      group_by(Asset) %>%
      mutate(
        Return_Index_Diff = ((Price - Open)/Open)*100
      ) %>%
      ungroup() %>%
      filter(!is.na(Return_Index_Diff))

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
      left_join(major_indices_PCA_Index) %>%
      filter(!is.na(Average_PCA_Index)) %>%
      group_by(Asset) %>%
      mutate(
        Average_PCA_Returns =
          ((Average_PCA_Index - lag(Average_PCA_Index))/lag(Average_PCA_Index))*100
      ) %>%
      ungroup() %>%
      filter(!is.na(Average_PCA_Returns))

    SPX500_USD_Index_copula_retun <-
      cauchy_dual_copula_generic(
        asset_data_to_use = major_indices_cumulative_pca %>% filter(Asset == "SPX500_USD"),
        cols_to_use = c("Return_Index_Diff", "Average_PCA_Returns"),
        rolling_period = 100,
        samples_for_MLE = samples_for_MLE,
        test_samples = test_samples
      ) %>%
      dplyr::select(Date, SPX_FX1_DIFF_Return = FX1,
                    SPX_FX2_INDEX_DIFF = FX2,
                    SPX_joint_density_copula_INDEX_PCA_DIFF = joint_density_copula,
                    SPX_joint_density_INDEX_PCA_DIFF = joint_density)

    SPX500_USD_Index_copula_Index <-
      cauchy_dual_copula_generic(
        asset_data_to_use = major_indices_cumulative_pca %>% filter(Asset == "SPX500_USD"),
        cols_to_use = c("Return_Index", "Average_PCA_Index"),
        rolling_period = 100,
        samples_for_MLE = samples_for_MLE,
        test_samples = test_samples
      ) %>%
      dplyr::select(Date,
                    SPX_FX1_Index = FX1,
                    SPX_FX2_PCA_Index = FX2,
                    SPX_joint_density_copula_INDEX_PCA = joint_density_copula,
                    SPX_joint_density_INDEX_PCA = joint_density)

    AU200_AUD_Index_copula_returns <-
      cauchy_dual_copula_generic(
        asset_data_to_use = major_indices_cumulative_pca %>% filter(Asset == "AU200_AUD"),
        cols_to_use = c("Return_Index_Diff", "Average_PCA_Returns"),
        rolling_period = 100,
        samples_for_MLE = samples_for_MLE,
        test_samples = test_samples
      ) %>%
      dplyr::select(Date,
                    AU200_FX1_DIFF_Return = FX1,
                    AU200_FX2_INDEX_DIFF = FX2,
                    AU200_joint_density_copula_INDEX_PCA_DIFF = joint_density_copula,
                    AU200_joint_density_INDEX_PCA_DIFF = joint_density)

    AU200_AUD_Index_copula_Index <-
      cauchy_dual_copula_generic(
        asset_data_to_use = major_indices_cumulative_pca %>% filter(Asset == "AU200_AUD"),
        cols_to_use = c("Return_Index", "Average_PCA_Index"),
        rolling_period = 100,
        samples_for_MLE = samples_for_MLE,
        test_samples = test_samples
      ) %>%
      dplyr::select(Date,
                    AU200_FX1_Index = FX1,
                    AU200_FX2_PCA_Index = FX2,
                    AU200_joint_density_copula_INDEX_PCA = joint_density_copula,
                    AU200_joint_density_INDEX_PCA = joint_density)


    US2000_USD_Index_copula_returns <-
      cauchy_dual_copula_generic(
        asset_data_to_use = major_indices_cumulative_pca %>% filter(Asset == "US2000_USD"),
        cols_to_use = c("Return_Index_Diff", "Average_PCA_Returns"),
        rolling_period = 100,
        samples_for_MLE = samples_for_MLE,
        test_samples = test_samples
      ) %>%
      dplyr::select(Date,
                    US2000_FX1_DIFF_Return = FX1,
                    US2000_FX2_INDEX_DIFF = FX2,
                    US2000_joint_density_copula_INDEX_PCA_DIFF = joint_density_copula,
                    US2000_joint_density_INDEX_PCA_DIFF = joint_density)

    US2000_USD_Index_copula_Index <-
      cauchy_dual_copula_generic(
        asset_data_to_use = major_indices_cumulative_pca %>% filter(Asset == "US2000_USD"),
        cols_to_use = c("Return_Index", "Average_PCA_Index"),
        rolling_period = 100,
        samples_for_MLE = samples_for_MLE,
        test_samples = test_samples
      ) %>%
      dplyr::select(Date,
                    US2000_FX1_Index = FX1,
                    US2000_FX2_PCA_Index = FX2,
                    US2000_joint_density_copula_INDEX_PCA = joint_density_copula,
                    US2000_joint_density_INDEX_PCA = joint_density)


    EU50_EUR_Index_copula_returns <-
      cauchy_dual_copula_generic(
        asset_data_to_use = major_indices_cumulative_pca %>% filter(Asset == "EU50_EUR"),
        cols_to_use = c("Return_Index_Diff", "Average_PCA_Returns"),
        rolling_period = 100,
        samples_for_MLE = samples_for_MLE,
        test_samples = test_samples
      ) %>%
      dplyr::select(Date,
                    EU50_EUR_FX1_DIFF_Return = FX1,
                    EU50_EUR_FX2_INDEX_DIFF = FX2,
                    EU50_EUR_joint_density_copula_INDEX_PCA_DIFF = joint_density_copula,
                    EU50_EUR_joint_density_INDEX_PCA_DIFF = joint_density)

    EU50_EUR_Index_copula_Index <-
      cauchy_dual_copula_generic(
        asset_data_to_use = major_indices_cumulative_pca %>% filter(Asset == "EU50_EUR"),
        cols_to_use = c("Return_Index", "Average_PCA_Index"),
        rolling_period = 100,
        samples_for_MLE = samples_for_MLE,
        test_samples = test_samples
      ) %>%
      dplyr::select(Date,
                    EU50_EUR_FX1_Index = FX1,
                    EU50_EUR_FX2_PCA_Index = FX2,
                    EU50_EUR_joint_density_copula_INDEX_PCA = joint_density_copula,
                    EU50_EUR_joint_density_INDEX_PCA = joint_density)

    full_PCA_Copula_Data <-
      major_indices_cumulative_pca %>%
      left_join(SPX500_USD_Index_copula_retun)%>%
      left_join(SPX500_USD_Index_copula_Index)%>%
      left_join(AU200_AUD_Index_copula_returns)%>%
      left_join(AU200_AUD_Index_copula_Index) %>%
      left_join(US2000_USD_Index_copula_returns)%>%
      left_join(US2000_USD_Index_copula_Index)%>%
      left_join(EU50_EUR_Index_copula_returns)%>%
      left_join(EU50_EUR_Index_copula_Index)

    full_PCA_Copula_Data2 <-
      full_PCA_Copula_Data %>%
      filter(!is.na(US2000_joint_density_INDEX_PCA)) %>%
      mutate(
        across(-c(Date, Price, Asset, Open, High, Low), .fns = ~ lag(.))
      ) %>%
      filter(!is.na(US2000_joint_density_INDEX_PCA), !is.na(AU200_joint_density_INDEX_PCA_DIFF)) %>%
      group_by(Asset) %>%
      mutate(
        US2000_joint_density_INDEX_PCA_mean =
          slider::slide_dbl(.x = US2000_joint_density_INDEX_PCA, .f = ~ mean(.x, na.rm = T), .before = rolling_period),
        US2000_joint_density_INDEX_PCA_DIFF_mean =
          slider::slide_dbl(.x = US2000_joint_density_INDEX_PCA_DIFF, .f = ~ mean(.x, na.rm = T), .before = rolling_period),
        AU200_joint_density_INDEX_PCA_mean  =
          slider::slide_dbl(.x = AU200_joint_density_INDEX_PCA, .f = ~ mean(.x, na.rm = T), .before = rolling_period),
        AU200_joint_density_INDEX_PCA_DIFF_mean =
          slider::slide_dbl(.x = AU200_joint_density_INDEX_PCA_DIFF, .f = ~ mean(.x, na.rm = T), .before = rolling_period),
        SPX_joint_density_INDEX_PCA_mean =
          slider::slide_dbl(.x = SPX_joint_density_INDEX_PCA, .f = ~ mean(.x, na.rm = T), .before = rolling_period),
        SPX_joint_density_INDEX_PCA_DIFF_mean =
          slider::slide_dbl(.x = SPX_joint_density_INDEX_PCA_DIFF, .f = ~ mean(.x, na.rm = T), .before = rolling_period),
        EU50_EUR_density_INDEX_PCA_mean =
          slider::slide_dbl(.x = EU50_EUR_joint_density_INDEX_PCA, .f = ~ mean(.x, na.rm = T), .before = rolling_period),
        EU50_EUR_density_INDEX_PCA_DIFF_mean =
          slider::slide_dbl(.x = EU50_EUR_joint_density_INDEX_PCA_DIFF, .f = ~ mean(.x, na.rm = T), .before = rolling_period),



        US2000_joint_density_INDEX_PCA_sd =
          slider::slide_dbl(.x = US2000_joint_density_INDEX_PCA, .f = ~ sd(.x, na.rm = T), .before = rolling_period),
        US2000_joint_density_INDEX_PCA_DIFF_sd =
          slider::slide_dbl(.x = US2000_joint_density_INDEX_PCA_DIFF, .f = ~ sd(.x, na.rm = T), .before = rolling_period),
        AU200_joint_density_INDEX_PCA_sd  =
          slider::slide_dbl(.x = AU200_joint_density_INDEX_PCA, .f = ~ sd(.x, na.rm = T), .before = rolling_period),
        AU200_joint_density_INDEX_PCA_DIFF_sd =
          slider::slide_dbl(.x = AU200_joint_density_INDEX_PCA_DIFF, .f = ~ sd(.x, na.rm = T), .before = rolling_period),
        SPX_joint_density_INDEX_PCA_sd =
          slider::slide_dbl(.x = SPX_joint_density_INDEX_PCA, .f = ~ sd(.x, na.rm = T), .before = rolling_period),
        SPX_joint_density_INDEX_PCA_DIFF_sd =
          slider::slide_dbl(.x = SPX_joint_density_INDEX_PCA_DIFF, .f = ~ sd(.x, na.rm = T), .before = rolling_period),
        EU50_EUR_joint_density_INDEX_PCA_sd =
          slider::slide_dbl(.x = EU50_EUR_joint_density_INDEX_PCA, .f = ~ sd(.x, na.rm = T), .before = rolling_period),
        EU50_EUR_joint_density_INDEX_PCA_DIFF_sd =
          slider::slide_dbl(.x = EU50_EUR_joint_density_INDEX_PCA_DIFF, .f = ~ sd(.x, na.rm = T), .before = rolling_period)


      )%>%
      group_by(Asset) %>%
      mutate(
        Price_Index_minus_PCA_Index = Average_PCA_Index - Return_Index,
        Price_Index_minus_PCA_prob_roll =
          slider::slide_dbl(.x = Price_Index_minus_PCA_Index,
                            .f = ~rolling_cauchy(.x, summarise_func= "max"),
                            .before = rolling_period, .complete = TRUE),
        SPX_joint_density_INDEX_PCA_prob_roll =
          slider::slide_dbl(.x = SPX_joint_density_INDEX_PCA,
                            .f = ~rolling_cauchy(.x, summarise_func= "max"),
                            .before = rolling_period, .complete = TRUE),
        US2000_joint_density_INDEX_PCA_prob_roll =
          slider::slide_dbl(.x = US2000_joint_density_INDEX_PCA,
                            .f = ~rolling_cauchy(.x, summarise_func= "max"),
                            .before = rolling_period, .complete = TRUE),
        AU200_joint_density_INDEX_PCA_prob_roll =
          slider::slide_dbl(.x = AU200_joint_density_INDEX_PCA,
                            .f = ~rolling_cauchy(.x, summarise_func= "max"),
                            .before = rolling_period, .complete = TRUE),

        EU50_EUR_joint_density_INDEX_PCA_prob_roll =
          slider::slide_dbl(.x = EU50_EUR_joint_density_INDEX_PCA,
                            .f = ~rolling_cauchy(.x, summarise_func= "max"),
                            .before = rolling_period, .complete = TRUE)
      ) %>%
      filter(!is.na(SPX_joint_density_INDEX_PCA_prob_roll))

    # tagged_trades %>%
    #   group_by(trade_col, Asset) %>%
    #   summarise(
    #     # return_mean = mean(return_10_High, na.rm= T),
    #     return_median = median(return_10_High, na.rm= T),
    #     return25_high = quantile(return_10_High,0.25 ,na.rm= T),
    #     return_75_high = quantile(return_10_High,0.75 ,na.rm= T),
    #
    #     return_25_low = quantile(return_10_Low,0.25 ,na.rm= T),
    #     return_75_low = quantile(return_10_Low,0.75 ,na.rm= T),
    #
    #     wins = sum(long_win, na.rm = T),
    #     lose = sum(long_lose, na.rm = T),
    #     percent_win = wins/(wins + lose)
    #   ) %>%
    #   arrange(Asset)

    return(full_PCA_Copula_Data2)

  }

full_PCA_Copula_Data2 <-
  create_Index_PCA_copula(
    major_indices_log_cumulative = major_indices_log_cumulative,
    assets_to_use = c("SPX500_USD", "US2000_USD", "NAS100_USD", "SG30_SGD", "AU200_AUD", "EU50_EUR", "DE30_EUR"),
    samples_for_MLE = 0.5,
    test_samples = 0.45,
    rolling_period = 100
  )

test <-
  analysis_data %>% filter(Asset == "SPX500_USD") %>%
  dplyr::select(Date, Asset,US2000_joint_density_INDEX_PCA_prob_roll, Price_Index_minus_PCA_prob_roll  )

get_tagged_PCA_Index_Equities_Analysis <- function(
  analysis_data = full_PCA_Copula_Data2,
  raw_asset_data_ask = major_indices_log_cumulative,
  raw_asset_data_bid = major_indices_log_cumulative_bid,
  stop_factor = 5,
  profit_factor = 10,
  risk_dollar_value = 10,
  analysis_syms = c("AU200_AUD", "SPX500_USD", "EU50_EUR", "US2000_USD"),
  control_random_samples = control_random_samples
  ) {

  #--------------------------------------Random Results

  total_time_periods <- dim(analysis_data)[1]

  analysis_data %>% distinct(Asset)

  tagged_trades <-
    analysis_data %>%
    arrange(Date) %>%
    left_join(asset_infor %>% distinct(Asset = name, pipLocation)) %>%
    mutate(pipLocation = as.numeric(pipLocation)) %>%
    mutate(
      trade_col =
        case_when(
          SPX_joint_density_INDEX_PCA_prob_roll   > 0.5 & Asset == "SPX500_USD"  ~ "Long",
          SPX_joint_density_INDEX_PCA_prob_roll    < 0.5 & Asset == "SPX500_USD"  ~ "Short"
        ),
      return_10_High = ((lead(High, 14) - Open))/(10^pipLocation),
      return_10_Low = ((lead(Low, 14) - Open))/(10^pipLocation),
      long_win =
        case_when(
          ((lead(Price, 14) - Price))/(10^pipLocation)  > 0 ~ 1,
          TRUE ~ 0
        ),

      long_lose =
        case_when(
          ((lead(Price, 14) - Price))/(10^pipLocation)  <= 0 ~ 1,
          TRUE ~ 0
        )
    ) %>%
    filter(!is.na(trade_col))

  tagged_trades_long <- analysis_data %>%
    mutate(
      trade_col =
        case_when(
          # SPX_joint_density_INDEX_PCA_prob_roll   > 0.65 & Asset == "SPX500_USD"  ~ "Long"
          Price_Index_minus_PCA_prob_roll > 0.92~ "Long"
          # SPX_joint_density_copula_INDEX_PCA  < 0.5 & Asset == "SPX500_USD"  ~ "Short"
          )
      ) %>%
    filter(!is.na(trade_col))

  tagged_trades_short <- analysis_data %>%
    mutate(
      trade_col =
        case_when(
          # SPX_joint_density_copula_INDEX_PCA > 0.5 & Asset == "SPX500_USD"  ~ "Long",
          # SPX_joint_density_INDEX_PCA_prob_roll    < 0.35 & Asset == "SPX500_USD"  ~ "Short"
          Price_Index_minus_PCA_prob_roll < 0.08~ "Short"
        )
    ) %>%
    filter(!is.na(trade_col))

  percent_trades_taken <- dim(tagged_trades)[1]/total_time_periods
  stop_factor = 5
  profit_factor = 10
  #---------------------------------------------------------------------------
    long_analysis <- run_pairs_analysis(
      tagged_trades = tagged_trades_long %>% filter(trade_col == "Long"),
      stop_factor = stop_factor,
      profit_factor = profit_factor,
      raw_asset_data = raw_asset_data_ask,
      risk_dollar_value = risk_dollar_value
    )

    long_analysis_total <- long_analysis[[1]]
    long_analysis_asset <- long_analysis[[2]]

    long_comparison <- long_analysis_asset %>%
      dplyr::select(trade_direction , Asset, Trades, Final_Dollars,
                    risk_weighted_return_strat = risk_weighted_return) %>%
    left_join(control_random_samples %>%
                ungroup() %>%
                dplyr::select(-stop_factor, -profit_factor)) %>%
      mutate(
        p_value_risk =
          round(pnorm(risk_weighted_return_strat, mean = mean_risk, sd = sd_risk), 4)
      )
  #--------------------------------------------------------------------------------
  short_analysis <- run_pairs_analysis(
      tagged_trades = tagged_trades_short %>% filter(trade_col == "Short"),
      stop_factor = stop_factor,
      profit_factor = profit_factor,
      raw_asset_data = raw_asset_data_bid,
      risk_dollar_value = risk_dollar_value
    )

    short_analysis_total <- short_analysis[[1]]
    short_analysis_asset <- short_analysis[[2]]
    short_comparison <- short_analysis_asset %>%
      dplyr::select(trade_direction, Asset, Trades, Final_Dollars,
                    risk_weighted_return_strat = risk_weighted_return)  %>%
      left_join(control_random_samples %>%
                  ungroup() %>%
                  dplyr::select(-stop_factor, -profit_factor)) %>%
      mutate(
        p_value_risk =
          pnorm(risk_weighted_return_strat, mean = mean_risk, sd = sd_risk)
      )

}


