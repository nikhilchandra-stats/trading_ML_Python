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

commod_USD <-
  get_all_commod_USD(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "ask",
    time_frame = time_frame
  )

commod_USD_bid <-
  get_all_commod_USD(
    db_location = db_location,
    start_date = start_date,
    end_date = end_date,
    bid_or_ask = "bid",
    time_frame = time_frame
  )

mean_values_by_asset_for_loop <-
  wrangle_asset_data(
    asset_data_daily_raw = commod_USD,
    summarise_means = TRUE
  )

major_indices$Asset %>% unique()
gc()

samples <- 1000
random_results_db_location <- "C:/Users/Nikhil Chandra/Documents/trade_data/random_results.db"
db_con <- connect_db(random_results_db_location)
stop_factor = 5
profit_factor = 10
analysis_syms = c("BCO_USD", "WTICO_USD" ,"NATGAS_USD", "SOYBN_USD", "SUGAR_USD", "WHEAT_USD", "XAG_USD", "XAU_USD", "XCU_USD")
trade_samples = 5000
new_table = FALSE
time_frame = "H1"

for (i in 1:samples) {

  temp_results <-
    get_random_results_trades(
      raw_asset_data_ask = commod_USD,
      raw_asset_data_bid = commod_USD_bid,
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

control_random_samples <-
  get_random_samples_MLE_beta(
    random_results_db_location = "C:/Users/Nikhil Chandra/Documents/trade_data/random_results.db",
    stop_factor = 5,
    profit_factor = 10,
    analysis_syms =  c("BCO_USD", "WTICO_USD" ,"NATGAS_USD", "SOYBN_USD", "SUGAR_USD","WTI_" ,"WHEAT_USD", "XAG_USD", "XCU_USD"),
    time_frame = "H1",
    return_summary = TRUE
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

#----------------------------------------------------------------------------------
commod_log_cumulative <-
  analysis_syms %>%
  map_dfr(
    ~
      create_log_cumulative_returns(
        asset_data_to_use = commod_USD,
        asset_to_use = c(.x[1]),
        price_col = "Open",
        return_long_format = TRUE
      )
  ) %>%
  left_join(
    commod_USD %>% distinct(Date, Asset, Price, Open, High, Low)
  )

commod_log_cumulative_bid <-
  analysis_syms %>%
  map_dfr(
    ~
      create_log_cumulative_returns(
        asset_data_to_use = commod_USD_bid,
        asset_to_use = c(.x[1]),
        price_col = "Open",
        return_long_format = TRUE
      )
  ) %>%
  left_join(
    commod_USD_bid %>% distinct(Date, Asset, Price, Open, High, Low)
  )


#' commod_model_trades_diff_vers
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
commod_model_trades_diff_vers <-
  function(
    commod_data = commod_log_cumulative ,
    PCA_Data = NULL,
    assets_to_use = analysis_syms,
    samples_for_MLE = 0.5,
    test_samples = 0.45,
    rolling_period = 100,
    date_filter_min = "2011-01-01",
    stop_factor = 12,
    profit_factor = 15
  ) {

    commod_data <-
      commod_data %>%
      group_by(Asset) %>%
      mutate(
        Return_Index_Diff = ((Price - Open)/Open)*100
      ) %>%
      ungroup() %>%
      filter(!is.na(Return_Index_Diff)) %>%
      filter(Date >= date_filter_min)

    if(is.null(PCA_Data)) {
      commod_data_Index <-
        create_PCA_Asset_Index(
          asset_data_to_use = commod_data,
          asset_to_use = assets_to_use,
          price_col = "Return_Index_Diff"
        ) %>%
        rename(
          Average_PCA_Index = Average_PCA
        )
    }

    if(!is.null(PCA_Data)) {
      commod_data_Index <-
        PCA_Data  %>%
        rename(
          Average_PCA_Index = Average_PCA
        )
    }

    commod_data_pca <-
      commod_data %>%
      left_join(commod_data_Index)

    asset_1 <-
      get_PCA_Index_rolling_cor_sd_mean(
        raw_asset_data_for_PCA_cor = commod_data %>% filter(Asset == "BCO_USD"),
        PCA_data = commod_data_pca,
        rolling_period = rolling_period
      )

    asset_2 <-
      get_PCA_Index_rolling_cor_sd_mean(
        raw_asset_data_for_PCA_cor = commod_data %>% filter(Asset == "NATGAS_USD"),
        PCA_data = commod_data_pca,
        rolling_period = rolling_period
      )

    asset_3 <-
      get_PCA_Index_rolling_cor_sd_mean(
        raw_asset_data_for_PCA_cor = commod_data %>% filter(Asset == "SOYBN_USD"),
        PCA_data = commod_data_pca,
        rolling_period = rolling_period
      )

    asset_4 <-
      get_PCA_Index_rolling_cor_sd_mean(
        raw_asset_data_for_PCA_cor = commod_data %>% filter(Asset == "SUGAR_USD"),
        PCA_data = commod_data_pca,
        rolling_period = rolling_period
      )

    asset_5 <-
      get_PCA_Index_rolling_cor_sd_mean(
        raw_asset_data_for_PCA_cor = commod_data %>% filter(Asset == "WHEAT_USD"),
        PCA_data = commod_data_pca,
        rolling_period = rolling_period
      )

    asset_6 <-
      get_PCA_Index_rolling_cor_sd_mean(
        raw_asset_data_for_PCA_cor = commod_data %>% filter(Asset == "WTICO_USD"),
        PCA_data = commod_data_pca,
        rolling_period = rolling_period
      )

    asset_7 <-
      get_PCA_Index_rolling_cor_sd_mean(
        raw_asset_data_for_PCA_cor = commod_data %>% filter(Asset == "XCU_USD"),
        PCA_data = commod_data_pca,
        rolling_period = rolling_period
      )


    # BCO_USD
    asset_1_tag <-
      asset_1 %>%
      mutate(trade_col =
               case_when(
                 # rolling_cor_PC1 <= rolling_cor_PC1_mean - 2*rolling_cor_PC1_sd & tan_angle > 0 ~ "Long",
                 # rolling_cor_PC1 <= rolling_cor_PC1_mean - 2.5*rolling_cor_PC1_sd & tan_angle < 0 ~ "Short",
                 # rolling_cor_PC1 >= rolling_cor_PC1_mean + 2.5*rolling_cor_PC1_sd & tan_angle < 0 ~ "Short",
                 # rolling_cor_PC2 <= rolling_cor_PC2_mean - 2*rolling_cor_PC2_sd & tan_angle > 0 ~ "Long",
                 # rolling_cor_PC2 <= rolling_cor_PC2_mean - 2*rolling_cor_PC2_sd & tan_angle < 0 ~ "Short",

                 # rolling_cor_PC3 <= rolling_cor_PC3_mean - 2*rolling_cor_PC3_sd & tan_angle < 0 ~ "Short", #2%
                 # rolling_cor_PC3 <= rolling_cor_PC3_mean - 2*rolling_cor_PC3_sd & tan_angle > 0 ~ "Long" #5%

                 rolling_cor_PC3 >= rolling_cor_PC3_mean + 2*rolling_cor_PC3_sd & tan_angle < 0 ~ "Short", #9%
                 rolling_cor_PC3 >= rolling_cor_PC3_mean + 2*rolling_cor_PC3_sd & tan_angle > 0 ~ "Long" #23%
               )
      ) %>%
      filter(!is.na(trade_col))


    # NATGAS_USD
    asset_2_tag <-
      asset_2 %>%
      mutate(trade_col =
               case_when(
                 # rolling_cor_PC1 <= rolling_cor_PC1_mean - 2.5*rolling_cor_PC1_sd & tan_angle > 0 ~ "Long", #15%
                 # rolling_cor_PC1 <= rolling_cor_PC1_mean - 2.75*rolling_cor_PC1_sd & tan_angle < 0 ~ "Short", #8%
                 # rolling_cor_PC2 <= rolling_cor_PC2_mean - 2*rolling_cor_PC2_sd & tan_angle > 0 ~ "Long",
                 # rolling_cor_PC2 >= rolling_cor_PC2_mean + 2*rolling_cor_PC2_sd & tan_angle > 0 ~ "Long"

                 # rolling_cor_PC3 <= rolling_cor_PC3_mean - 2*rolling_cor_PC3_sd & tan_angle < 0 ~ "Short", #-9%
                 # rolling_cor_PC3 <= rolling_cor_PC3_mean - 2*rolling_cor_PC3_sd & tan_angle > 0 ~ "Long" #13%

                 rolling_cor_PC3 >= rolling_cor_PC3_mean + 2*rolling_cor_PC3_sd & tan_angle < 0 ~ "Short", #0%
                 rolling_cor_PC3 >= rolling_cor_PC3_mean + 2*rolling_cor_PC3_sd & tan_angle > 0 ~ "Long" #11%
               )
      ) %>%
      filter(!is.na(trade_col))

    # SOYBN_USD
    asset_3_tag <-
      asset_3 %>%
      mutate(trade_col =
               case_when(
                 # rolling_cor_PC1 <= rolling_cor_PC1_mean - 2.75*rolling_cor_PC1_sd & tan_angle < 0 ~ "Short", #7%
                 # rolling_cor_PC1 >= rolling_cor_PC1_mean + 2.75*rolling_cor_PC1_sd & tan_angle > 0 ~ "Long",
                 # rolling_cor_PC2 >= rolling_cor_PC2_mean + 2.5*rolling_cor_PC2_sd & tan_angle > 0 ~ "Long",
                 # rolling_cor_PC2 >= rolling_cor_PC2_mean + 2.5*rolling_cor_PC2_sd & tan_angle < 0 ~ "Short"

                 # rolling_cor_PC3 <= rolling_cor_PC3_mean - 2*rolling_cor_PC3_sd & tan_angle < 0 ~ "Short", #1%
                 # rolling_cor_PC3 <= rolling_cor_PC3_mean - 2*rolling_cor_PC3_sd & tan_angle > 0 ~ "Long" #-4%

                 rolling_cor_PC3 >= rolling_cor_PC3_mean + 2*rolling_cor_PC3_sd & tan_angle < 0 ~ "Short",
                 rolling_cor_PC3 >= rolling_cor_PC3_mean + 2*rolling_cor_PC3_sd & tan_angle > 0 ~ "Long"
               )
      ) %>%
      filter(!is.na(trade_col))

    # SUGAR_USD
    asset_4_tag <-
      asset_4 %>%
      mutate(trade_col =
               case_when(
                 # rolling_cor_PC1 <= rolling_cor_PC1_mean - 2.5*rolling_cor_PC1_sd & tan_angle < 0 ~ "Short", #7%
                 # rolling_cor_PC1 >= rolling_cor_PC1_mean + 2*rolling_cor_PC1_sd & tan_angle < 0 ~ "Short", #16%
                 # rolling_cor_PC2 <= rolling_cor_PC2_mean - 2*rolling_cor_PC2_sd & tan_angle > 0 ~ "Short",
                 # rolling_cor_PC2 <= rolling_cor_PC2_mean - 2*rolling_cor_PC2_sd & tan_angle < 0 ~ "Long"

                 # rolling_cor_PC3 <= rolling_cor_PC3_mean - 2*rolling_cor_PC3_sd & tan_angle < 0 ~ "Short", #8%
                 # rolling_cor_PC3 <= rolling_cor_PC3_mean - 2*rolling_cor_PC3_sd & tan_angle > 0 ~ "Long" #-13%

                 rolling_cor_PC3 >= rolling_cor_PC3_mean + 2*rolling_cor_PC3_sd & tan_angle < 0 ~ "Short",
                 rolling_cor_PC3 >= rolling_cor_PC3_mean + 2*rolling_cor_PC3_sd & tan_angle > 0 ~ "Long"
               )
      ) %>%
      filter(!is.na(trade_col))

    # WHEAT_USD
    asset_5_tag <-
      asset_5 %>%
      mutate(trade_col =
               case_when(
                 # rolling_cor_PC1 >= rolling_cor_PC1_mean + 3*rolling_cor_PC1_sd & tan_angle > 0 ~ "Short",#%-2%
                 # rolling_cor_PC1 >= rolling_cor_PC1_mean + 3*rolling_cor_PC1_sd & tan_angle < 0 ~ "Long", #6%
                 # rolling_cor_PC2 <= rolling_cor_PC2_mean - 2.5*rolling_cor_PC2_sd & tan_angle > 0 ~ "Short"

                 # rolling_cor_PC3 <= rolling_cor_PC3_mean - 2*rolling_cor_PC3_sd & tan_angle < 0 ~ "Short", #-2%
                 # rolling_cor_PC3 <= rolling_cor_PC3_mean - 2*rolling_cor_PC3_sd & tan_angle > 0 ~ "Long" #1%

                 rolling_cor_PC3 >= rolling_cor_PC3_mean + 2*rolling_cor_PC3_sd & tan_angle < 0 ~ "Short",
                 rolling_cor_PC3 >= rolling_cor_PC3_mean + 2*rolling_cor_PC3_sd & tan_angle > 0 ~ "Long"

               )
      ) %>%
      filter(!is.na(trade_col))

    # WTICO_USD
    asset_6_tag <-
      asset_6 %>%
      mutate(trade_col =
               case_when(
                 # rolling_cor_PC1 <= rolling_cor_PC1_mean - 2.75*rolling_cor_PC1_sd & tan_angle > 0 ~ "Long",
                 # rolling_cor_PC1 <= rolling_cor_PC1_mean - 2.75*rolling_cor_PC1_sd & tan_angle < 0 ~ "Short",
                 # rolling_cor_PC2 <= rolling_cor_PC2_mean - 2*rolling_cor_PC2_sd & tan_angle > 0 ~ "Long",
                 # rolling_cor_PC2 <= rolling_cor_PC2_mean - 2*rolling_cor_PC2_sd & tan_angle < 0 ~ "Short"

                 # rolling_cor_PC3 <= rolling_cor_PC3_mean - 2*rolling_cor_PC3_sd & tan_angle < 0 ~ "Long", #4%
                 # rolling_cor_PC3 <= rolling_cor_PC3_mean - 2*rolling_cor_PC3_sd & tan_angle > 0 ~ "Short" #-5%

                 rolling_cor_PC3 >= rolling_cor_PC3_mean + 2*rolling_cor_PC3_sd & tan_angle < 0 ~ "Short",
                 rolling_cor_PC3 >= rolling_cor_PC3_mean + 2*rolling_cor_PC3_sd & tan_angle > 0 ~ "Long"
               )
      ) %>%
      filter(!is.na(trade_col))

    risk_dollar_value <- 10

    combined_results <-
      get_5_index_equity_combined_results(
        asset_1_tag_trades = asset_1_tag,
        asset_2_tag_trades = asset_2_tag,
        asset_3_tag_trades = asset_3_tag,
        asset_4_tag_trades = asset_4_tag,
        asset_5_tag_trades = asset_5_tag,
        asset_6_tag_trades = asset_6_tag,
        asset_data_ask = commod_USD,
        asset_data_bid = commod_USD_bid,
        risk_dollar_value = risk_dollar_value,
        profit_factor = profit_factor,
        stop_factor = stop_factor,
        profit_factor_long = profit_factor,
        stop_factor_long = stop_factor,
        control_random_samples = control_random_samples
      )

    combined_results$Final_Dollars %>% sum()

    return(list(
      asset_1_tag,
      asset_2_tag,
      asset_3_tag,
      asset_4_tag,
      asset_5_tag
    ))

  }
