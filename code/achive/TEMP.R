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

asset_data_raw_ask <- get_db_price(
  db_location = db_location,
  start_date = start_date,
  end_date = end_date,
  bid_or_ask = "ask",
  time_frame = "H1"
)

asset_data_raw_bid <- get_db_price(
  db_location = db_location,
  start_date = start_date,
  end_date = end_date,
  bid_or_ask = "bid",
  time_frame = "H1"
)

asset_data_raw_ask$Asset %>% unique()

samples <- 1200
random_results_db_location <- "C:/Users/Nikhil Chandra/Documents/trade_data/random_results.db"
db_con <- connect_db(random_results_db_location)
stop_factor = 12
profit_factor = 40
analysis_syms = c("XAG_USD", "AUD_USD", "EUR_USD", "AU200_AUD", "WHEAT_USD",
                             "USD_CNH", "WTICO_USD", "SOYBN_USD", "SUGAR_USD", "NATGAS_USD",
                             "EUR_SEK", "USD_SGD", "USD_MXN")
trade_samples = 1000
new_table = FALSE
time_frame = "H1"

for (i in 1:samples) {

  temp_results <-
    get_random_results_trades(
      raw_asset_data_ask = asset_data_raw_ask,
      raw_asset_data_bid = asset_data_raw_bid,
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
    stop_factor = 12,
    profit_factor = 40,
    analysis_syms = analysis_syms,
    time_frame = "H1",
    return_summary = TRUE
  )

SPX_bench_mark <-
  get_bench_mark_results(
    asset_data = asset_data_raw_ask %>% filter(Date >= "2018-01-01"),
    Asset_Var = "SPX500_USD",
    min_date = "2018-01-01",
    trade_direction = "Long",
    risk_dollar_value = 5,
    stop_factor = 4,
    profit_factor = 8,
    mean_values_by_asset_for_loop = wrangle_asset_data(asset_data_raw_ask, summarise_means = TRUE),
    currency_conversion = currency_conversion
  )



trade_data <-
  data_with_rolling_binom2 %>%
  ungroup() %>%
  mutate(
    trade_col =
      case_when(
        # Prob_Slow_minus_High >= Prob_Slow_minus_High_mean + 2.5*Prob_Slow_minus_High_sd ~ "Short",
        # Prob_Slow_minus_High <= Prob_Slow_minus_High_mean - 2.5*Prob_Slow_minus_High_sd ~ "Long"

        Rolling_Bayes_P_Highs >= Rolling_Bayes_P_Highs_mean + 2.25*Rolling_Bayes_P_Highs_sd ~ "Long",
        Rolling_Bayes_P_Lows <= Rolling_Bayes_P_Lows_mean - 2.25*Rolling_Bayes_P_Lows_sd ~ "Short"
      )
  ) %>%
  filter(!is.na(trade_col))

trade_data <- AUD_NZD_Trades_short %>% bind_rows(AUD_NZD_Trades_long)

mean_values_asset <- wrangle_asset_data(asset_data_daily_raw = AUD_USD_NZD_USD_list[[1]], summarise_means = TRUE)

timing_closure_analysis <-
  function(trade_data = trade_data,
           asset_data_raw_ask = AUD_USD_NZD_USD_list[[1]],
           asset_data_raw_bid = AUD_USD_NZD_USD_list[[2]],
           ahead_period = 24,
           asset_infor = asset_infor,
           currency_conversion = currency_conversion,
           asset_col = "Asset",
           stop_col = "starting_stop_value",
           profit_col = "starting_profit_value",
           price_col = "trade_start_prices",
           risk_dollar_value = 5,
           mean_values_asset = mean_values_asset,
           stop_factor = 12,
           profit_factor = 12) {

    raw_asset_data_Long <- asset_data_raw_ask %>%
      filter(Asset %in% (trade_data %>% pull(Asset) %>% unique()) ) %>%
      dplyr::select(Date, Asset, Price, High, Low, Open) %>%
      group_by(Asset) %>%
      mutate(
        # High_lead = lead(slider::slide_dbl(.x = High, .f = ~max(High, na.rm = T), .before = ahead_period )),
        # Low_lead = lead(slider::slide_dbl(.x = Low, .f = ~max(High, na.rm = T), .before = ahead_period )),
        start_point_long = lead(Price)
      ) %>%
      ungroup() %>%
      left_join(asset_data_raw_bid %>% dplyr::select(Date, Asset, Price_End = Price)) %>%
      group_by(Asset) %>%
      mutate(
        end_point_long = lead(Price_End, ahead_period + 1)
      ) %>%
      ungroup()

    stops_and_profs_for_volume <-
      mean_values_asset %>%
      mutate(
        stop_value_start = mean_daily + sd_daily*stop_factor,
        profit_value_start = mean_daily + sd_daily*profit_factor
      )

    asset_infor_internal <- asset_infor %>%
      rename(!!as.name(asset_col) := name)

    trade_volumes <-
      stops_and_profs_for_volume %>%
      mutate(ending_value = str_extract(!!as.name(asset_col), "_[A-Z][A-Z][A-Z]"),
             ending_value = str_remove_all(ending_value, "_")
      ) %>%
      left_join(currency_conversion, by =c("ending_value" = "not_aud_asset")) %>%
      left_join(asset_infor_internal %>%
                  dplyr::select(Asset, minimumTradeSize, marginRate, pipLocation, displayPrecision)
      ) %>%
      mutate(
        minimumTradeSize = abs(log10(as.numeric(minimumTradeSize))),
        marginRate = as.numeric(marginRate),
        pipLocation = as.numeric(pipLocation),
        displayPrecision = as.numeric(displayPrecision)
      ) %>%
      ungroup() %>%
      mutate(
        stop_value = round(stop_value_start , abs(pipLocation) ),
        profit_value = round(profit_value_start, abs(pipLocation) ),
        trade_start_prices = 0
      ) %>%
      mutate(
        # asset_size = floor(log10(!!as.name(price_col))),
        volume_adjustment = 1,
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
        profit_value_AUD =
          case_when(
            !is.na(adjusted_conversion) ~ (profit_value*adjusted_conversion)/volume_adjustment,
            TRUE ~ profit_value/volume_adjustment
          )
      ) %>%
      mutate(
        # volume_unadj =  risk_dollar_value/stop_value_AUD,
        volume_unadj =
          case_when(
            str_detect(Asset,"SEK|NOK|ZAR|MXN|CNH") ~ (risk_dollar_value/stop_value)*adjusted_conversion,
            TRUE ~ (risk_dollar_value/stop_value)/adjusted_conversion
          ),
        volume_required = volume_unadj,
        volume_adj = round(volume_unadj, minimumTradeSize),
        minimal_loss =
          case_when(
            str_detect(Asset,"SEK|NOK|ZAR|MXN|CNH") ~ ((risk_dollar_value/stop_value)/adjusted_conversion)*stop_value_AUD,
            TRUE ~ volume_adj*stop_value_AUD
          ),
        maximum_win =
          case_when(
            str_detect(Asset,"SEK|NOK|ZAR|MXN|CNH") ~ ((risk_dollar_value/stop_value)/adjusted_conversion)*profit_value_AUD,
            TRUE ~ volume_adj*profit_value_AUD
          ),
        trade_value = AUD_Price*volume_adj*marginRate,
        estimated_margin = trade_value,
        volume_required = volume_adj
      )


    trade_analysis_long <-
      trade_data %>%
      dplyr::select(Date, Asset, trade_col) %>%
      filter(trade_col == "Long") %>%
      left_join(raw_asset_data_Long) %>%
      mutate(
        ending_value_start =
          case_when(
            trade_col == "Long" & end_point_long>start_point_long ~ end_point_long - start_point_long,
            trade_col == "Long" & end_point_long<=start_point_long ~ end_point_long - start_point_long,
            trade_col == "Short" & end_point_long<start_point_long ~ start_point_long - end_point_long,
            trade_col == "Short" & end_point_long>=start_point_long ~ start_point_long - end_point_long
          )
      )


    trade_analysis_long2 <- trade_analysis_long %>%
      left_join(
        trade_volumes %>%
          dplyr::select(Asset, volume_required)
      ) %>%
      mutate(ending_value = str_extract(!!as.name(asset_col), "_[A-Z][A-Z][A-Z]"),
             ending_value = str_remove_all(ending_value, "_")
      ) %>%
      left_join(currency_conversion, by =c("ending_value" = "not_aud_asset")) %>%
      left_join(asset_infor_internal %>%
                  dplyr::select(Asset, minimumTradeSize, marginRate, pipLocation, displayPrecision)
      ) %>%
      mutate(
        minimumTradeSize = abs(log10(as.numeric(minimumTradeSize))),
        marginRate = as.numeric(marginRate),
        pipLocation = as.numeric(pipLocation),
        displayPrecision = as.numeric(displayPrecision)
      ) %>%
      ungroup() %>%
      mutate(
        AUD_End = (volume_required*ending_value_start)*adjusted_conversion
      )

    trade_analysis_long2 %>%
      ggplot(aes(x = AUD_End, fill = trade_col)) +
      geom_density(alpha = 0.5) +
      theme_minimal() +
      theme(legend.position = "bottom")

    trade_analysis_long2 %>%
      filter(trade_col == "Long") %>%
      group_by(Asset, trade_col) %>%
      summarise(
        AUD_End_median = median(AUD_End, na.rm = T),
        AUD_End_low = quantile(AUD_End, 0.25, na.rm = T),
        AUD_End_high = quantile(AUD_End, 0.75, na.rm = T)
      ) %>%
      mutate(
        Percent_diff =
          100*((AUD_End_high - abs(AUD_End_low))/abs(AUD_End_low))
      )


    returns_ts <-
      trade_analysis_long2 %>%
      filter(trade_col == "Long") %>%
      group_by(Date, Asset) %>%
      summarise(
        AUD_return = sum(AUD_End, na.rm = T)
      ) %>%
      group_by(Asset) %>%
      arrange(Date, .by_group = TRUE) %>%
      group_by(Asset) %>%
      mutate(
        cumulative_returns = cumsum(AUD_return)
      ) %>%
      ungroup()

    returns_ts %>%
      ggplot(aes(x = Date, y = cumulative_returns)) +
      geom_line() +
      facet_wrap(.~ Asset, scales = "free") +
      theme_minimal()


    raw_asset_data_Short <- asset_data_raw_bid %>%
      filter(Asset %in% (trade_data %>% pull(Asset) %>% unique()) ) %>%
      dplyr::select(Date, Asset, Price, High, Low, Open) %>%
      group_by(Asset) %>%
      mutate(
        # High_lead = lead(slider::slide_dbl(.x = High, .f = ~max(High, na.rm = T), .before = ahead_period )),
        # Low_lead = lead(slider::slide_dbl(.x = Low, .f = ~max(High, na.rm = T), .before = ahead_period )),
        start_point_short = lead(Price)
      ) %>%
      ungroup() %>%
      left_join(asset_data_raw_ask %>% dplyr::select(Date, Asset, Price_End = Price)) %>%
      group_by(Asset) %>%
      mutate(
        end_point_short = lead(Price_End, ahead_period + 1)
      ) %>%
      ungroup()


    trade_analysis_Short <-
      trade_data %>%
      dplyr::select(Date, Asset, trade_col) %>%
      filter(trade_col == "Short") %>%
      left_join(raw_asset_data_Short) %>%
      mutate(
        ending_value_start =
          case_when(
            trade_col == "Long" & end_point_short>start_point_short ~ end_point_short - start_point_short,
            trade_col == "Long" & end_point_short<=start_point_short ~ end_point_short - start_point_short,
            trade_col == "Short" & end_point_short<start_point_short ~ start_point_short - end_point_short,
            trade_col == "Short" & end_point_short>=start_point_short ~ start_point_short - end_point_short
          )
      )


    trade_analysis_short2 <- trade_analysis_Short %>%
      left_join(
        trade_volumes %>%
          dplyr::select(Asset, volume_required)
      ) %>%
      mutate(ending_value = str_extract(!!as.name(asset_col), "_[A-Z][A-Z][A-Z]"),
             ending_value = str_remove_all(ending_value, "_")
      ) %>%
      left_join(currency_conversion, by =c("ending_value" = "not_aud_asset")) %>%
      left_join(asset_infor_internal %>%
                  dplyr::select(Asset, minimumTradeSize, marginRate, pipLocation, displayPrecision)
      ) %>%
      mutate(
        minimumTradeSize = abs(log10(as.numeric(minimumTradeSize))),
        marginRate = as.numeric(marginRate),
        pipLocation = as.numeric(pipLocation),
        displayPrecision = as.numeric(displayPrecision)
      ) %>%
      ungroup() %>%
      mutate(
        AUD_End = (volume_required*ending_value_start)*adjusted_conversion
      )

    trade_analysis_short2 %>%
      ggplot(aes(x = AUD_End, fill = trade_col)) +
      geom_density(alpha = 0.5) +
      theme_minimal() +
      theme(legend.position = "bottom")

    trade_analysis_short2 %>%
      filter(trade_col == "Short") %>%
      group_by(Asset, trade_col) %>%
      summarise(
        AUD_End_median = median(AUD_End, na.rm = T),
        AUD_End_low = quantile(AUD_End, 0.25, na.rm = T),
        AUD_End_high = quantile(AUD_End, 0.75, na.rm = T)
      ) %>%
      mutate(
        Percent_diff =
          100*((AUD_End_high - abs(AUD_End_low))/abs(AUD_End_low))
      )


    returns_ts <-
      trade_analysis_short2 %>%
      filter(trade_col == "Short")  %>%
      group_by(Date, Asset) %>%
      summarise(
        AUD_return = sum(AUD_End, na.rm = T)
      ) %>%
      group_by(Asset) %>%
      arrange(Date, .by_group = TRUE) %>%
      group_by(Asset) %>%
      mutate(
        cumulative_returns = cumsum(AUD_return)
      ) %>%
      ungroup()

    returns_ts %>%
      ggplot(aes(x = Date, y = cumulative_returns)) +
      geom_line() +
      facet_wrap(.~ Asset, scales = "free") +
      theme_minimal()

  }
