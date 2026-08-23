helpeR::load_custom_functions()

all_aud_symbols <- get_oanda_symbols() %>%
  keep(~ str_detect(.x, "AUD")|str_detect(.x, "USD_SEK|USD_NOK|USD_HUF|USD_ZAR|USD_CNY|USD_MXN"))
asset_infor <- get_instrument_info()
aud_assets <- read_all_asset_data_intra_day(
  asset_list_oanda = all_aud_symbols,
  save_path_oanda_assets = "C:/Users/nikhi/Documents/trade_data//oanda_data/",
  read_csv_or_API = "API",
  time_frame = "D",
  bid_or_ask = "bid",
  how_far_back = 10,
  start_date = (today() - days(7)) %>% as.character()
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

asset_list_oanda =
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
    "UK100_GBP", "NZD_USD",
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
    "USB02Y_USD", "USB05Y_USD", "USB30Y_USD", "USB10Y_USD", "UK100_GBP") %>%
  unique()

asset_infor <- get_instrument_info()
#---------------------Data
load_custom_functions()
db_location = "C:/Users/nikhi/Documents/Asset Data/Oanda_Asset_Data_Most_Assets_2025-09-13.db"
start_date = "2012-01-01"
training_date = "2026-07-01"
end_date = today() %>% as.character()

assets_to_port =
  c(
    "SPX500_USD",
    "XAU_USD",
    "BTC_USD",
    "EU50_EUR"
  ) %>% unique()

Indices_Metals_Bonds <- list()
Indices_Metals_Bonds[[1]] <-
  get_db_data_quickly_algo(
    db_location = db_location,
    start_date = start_date,
    end_date = as.character(today() + days(30)),
    time_frame = "H1",
    bid_or_ask = "ask",
    assets =   assets_to_port
  ) %>%
  distinct()
Indices_Metals_Bonds[[2]] <-
  get_db_data_quickly_algo(
    db_location = db_location,
    start_date = start_date,
    end_date = as.character(today() + days(30)),
    time_frame = "H1",
    bid_or_ask = "bid",
    assets =   assets_to_port
  ) %>%
  distinct()

test_parameters <-
  tibble(
    stop_factor_var = seq(4,20, 1)
  ) %>%
  split(.$stop_factor_var, drop = FALSE) %>%
  map_dfr(
    ~
      tibble(
        profit_factor_var = seq(5,100,5)
      ) %>%
      bind_cols(.x)
  ) %>%
  mutate(
    end_point_loss = -5,
    end_point_profit = abs(end_point_loss)*ceiling(profit_factor_var/stop_factor_var)
  )

db_con_returns <- connect_db("C:/Users/nikhi/Documents/Asset Data/Return_Structure_Stored_MIXED.db")
rerun_DB <- TRUE
c = 0

for (i in 1:dim(test_parameters)[1] ) {

  stop_factor_var = test_parameters$stop_factor_var[i]
  profit_factor_var = test_parameters$profit_factor_var[i]
  risk_dollar_value_var = 5
  end_period = 132
  trade_direction = "Long"
  end_point_loss = test_parameters$end_point_loss[i]
  end_point_profit = test_parameters$end_point_profit[i]
  asset_accumulator <- list()

  for (j in 1:length(assets_to_port)) {
    c = c + 1
    temp <-
      get_portfolio_model_fast_summed(
        asset_data = Indices_Metals_Bonds %>% map(~ .x %>% filter(Date < training_date)),
        asset_of_interest = assets_to_port[j],
        stop_factor_var = stop_factor_var,
        profit_factor_var = profit_factor_var,
        risk_dollar_value_var = risk_dollar_value_var,
        end_period = end_period,
        time_frame = "H1",
        trade_direction = trade_direction,
        currency_conversion = currency_conversion,
        asset_infor = asset_infor,
        end_point_loss = end_point_loss,
        end_point_profit = end_point_profit,
        sum_as_portfolio = TRUE,

        overwrite_volume = NULL,
        min_volume_only = FALSE,
        return_only_interested_col = FALSE
        # return_only_Final = TRUE
      ) %>%
      dplyr::select(Date, Asset, end_point_loss, end_point_profit, risk_dollar_value, stop_factor,
                    profit_factor, end_point_point_win, end_point_point_loss,
                    Final_Return, period_return_10_Price, period_return_20_Price,
                    period_return_30_Price, period_return_40_Price, period_return_50_Price,
                    period_return_60_Price, period_return_70_Price, period_return_80_Price,
                    period_return_90_Price, period_return_100_Price, period_return_110_Price,
                    period_return_120_Price, period_return_130_Price)

    gc()

    asset_accumulator[[j]] <- temp

    # if(c == 1 & rerun_DB == TRUE){
    #   write_table_sql_lite(.data = temp,
    #                        table_name = "Return_Structure_Stored",
    #                        conn = db_con_returns,
    #                        overwrite_true = TRUE)
    # }
    #
    # if(c != 1 | rerun_DB == FALSE){
    #   append_table_sql_lite(.data = temp,
    #                         table_name = "Return_Structure_Stored",
    #                         conn = db_con_returns)
    # }
    #
    rm(temp)
    gc()

  }

  cumulative_structure <-
    asset_accumulator %>%
    map_dfr(bind_rows) %>%
    group_by(Asset) %>%
    arrange(Date, .by_group = TRUE) %>%
    mutate(
      across(.cols = c(period_return_10_Price, period_return_20_Price,
                       period_return_30_Price, period_return_40_Price, period_return_50_Price,
                       period_return_60_Price, period_return_70_Price, period_return_80_Price,
                       period_return_90_Price, period_return_100_Price, period_return_110_Price,
                       period_return_120_Price, period_return_130_Price),
             .fns = ~ ifelse(is.na(.), 0, .) ),
      across(.cols = c(period_return_10_Price, period_return_20_Price,
                       period_return_30_Price, period_return_40_Price, period_return_50_Price,
                       period_return_60_Price, period_return_70_Price, period_return_80_Price,
                       period_return_90_Price, period_return_100_Price, period_return_110_Price,
                       period_return_120_Price, period_return_130_Price),
             .fns = ~ cumsum(.) )
    )

  xx <- c(10,20,30,40,50,60,70,80,90,100,110,120,130)
  statements_min <- list()

  for (k in 1:length(xx) ) {
    statements_min[[k]] <-
      seq(100,2000,100) %>%
      map(
        ~
          glue::glue("min_{.x} = period_return_{xx[k]}_Price - lag(period_return_{xx[k]}_Price, {.x})")
      ) %>%
      unlist()
  }

  statements_min_all <-
    statements_min %>%
    unlist() %>%
    as.character() %>%
    paste(collapse = ",")

  statements_min_all_mutate <-
    glue::glue("cumulative_structure %>% group_by(Asset) %>% arrange(Date, .by_group = TRUE) %>% mutate({statements_min_all})")

  cumulative_structure <-
    asset_accumulator %>%
    map_dfr(bind_rows) %>%
    group_by(Asset) %>%
    arrange(Date, .by_group = TRUE) %>%
    mutate(
      across(.cols = c(period_return_10_Price, period_return_20_Price,
                       period_return_30_Price, period_return_40_Price, period_return_50_Price,
                       period_return_60_Price, period_return_70_Price, period_return_80_Price,
                       period_return_90_Price, period_return_100_Price, period_return_110_Price,
                       period_return_120_Price, period_return_130_Price),
             .fns = ~ ifelse(is.na(.), 0, .) ),
      across(.cols = c(period_return_10_Price, period_return_20_Price,
                       period_return_30_Price, period_return_40_Price, period_return_50_Price,
                       period_return_60_Price, period_return_70_Price, period_return_80_Price,
                       period_return_90_Price, period_return_100_Price, period_return_110_Price,
                       period_return_120_Price, period_return_130_Price),
             .fns = ~ cumsum(.) )
    )

  cumulative_structure_mins <-
    eval(parse(text = statements_min_all_mutate))

}

