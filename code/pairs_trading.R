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
    start_date = "2016-01-01",
    end_date = "2024-01-01",
    bid_or_ask = "ask",
    time_frame = "M15"
  )
starting_asset_data_bid_15 <-
  get_db_price(
    db_location = db_location,
    start_date = "2016-01-01",
    end_date = "2024-01-01",
    bid_or_ask = "bid",
    time_frame = "M15"
  )

#------------------------------------------
dates_to_choose_from <- seq(as_datetime("2016-01-01"), as_datetime("2022-01-01"), "day")
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


#' get_correlation_reg_dat_v2
#'
#' @param asset_data_to_use
#' @param samples_for_MLE
#' @param test_samples
#' @param regression_train_prop
#' @param dependant_period
#' @param assets_to_filter
#'
#' @return
#' @export
#'
#' @examples
get_correlation_reg_dat_v2 <- function(
    asset_data_to_use = starting_asset_data_bid_15,
    samples_for_MLE = 0.5,
    test_samples = 0.4,
    regression_train_prop = 0.5,
    dependant_period = 10,
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
                         c("USD_SEK", "EUR_SEK"))
) {

  asset_joined_copulas <-
    get_correlation_data_set_v2(
      asset_data_to_use = asset_data_to_use,
      samples_for_MLE = samples_for_MLE,
      test_samples = test_samples,
      assets_to_filter = assets_to_filter
    ) %>%
    group_by(Asset) %>%
    mutate(
      dependant_var = log(lead(Price, dependant_period)/Price)
    ) %>%
    ungroup()

  regressors <- names(asset_joined_copulas) %>%
    keep(~ str_detect(.x, "quantiles|_cor|tangent|log"))
  lm_formula <- create_lm_formula(dependant = "dependant_var",
                                  independant = regressors)

  train_data <- asset_joined_copulas %>%
    filter(!is.na(dependant_var)) %>%
    group_by(Asset) %>%
    slice_head(prop = regression_train_prop) %>%
    ungroup() %>%
    filter(Asset %in% assets_to_filter)

  lm_model <- lm(formula = lm_formula, data = train_data)
  summary(lm_model)

  training_predictions <-
    predict.lm(object = lm_model, newdata = train_data) %>% as.numeric()

  mean_sd_predictons <-train_data%>%
    mutate(
      pred = training_predictions
    ) %>%
    group_by(Asset) %>%
    summarise(
      mean_pred = mean(pred, na.rm = T),
      sd_pred = sd(pred, na.rm = T)
    )

  testing_data <- asset_joined_copulas %>%
    group_by(Asset) %>%
    slice_tail(prop = (1 - regression_train_prop) ) %>%
    ungroup() %>%
    filter(Asset %in% assets_to_filter)

  predictions <- predict.lm(object = lm_model, newdata = testing_data) %>% as.numeric()

  testing_data <- testing_data %>%
    mutate(
      pred =predictions
    ) %>%
    left_join(mean_sd_predictons)

  cor_summs_mean <-
    train_data %>%
    dplyr::select(contains("_cor"))

  new_names_cor_summs <-
    names(cor_summs_mean) %>%
    map(
      ~
        paste0(.x, "_mean")
    ) %>%
    unlist()

  names(cor_summs_mean) <- new_names_cor_summs

  cor_summs_mean <- cor_summs_mean %>%
    summarise(
      across(
        .cols = everything(),
        .fns = ~ mean(., na.rm = T)
      )
    )

  cor_summs_sd <-
    train_data %>%
    dplyr::select(contains("_cor"))

  new_names_cor_summs <-
    names(cor_summs_sd) %>%
    map(
      ~
        paste0(.x, "_sd")
    ) %>%
    unlist()

  names(cor_summs_sd) <- new_names_cor_summs

  cor_summs_sd <- cor_summs_sd %>%
    summarise(
      across(
        .cols = everything(),
        .fns = ~ sd(., na.rm = T)
      )
    )

  testing_data2 <-
    testing_data %>%
    bind_cols(cor_summs_mean)%>%
    bind_cols(cor_summs_sd)

  return(list(testing_data2, lm_model))

}

cor_data_list <-
  get_correlation_reg_dat_v2(
    asset_data_to_use = starting_asset_data_bid_15,
    samples_for_MLE = 0.5,
    test_samples = 0.4,
    regression_train_prop = 0.5,
    dependant_period = 10,
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
                         c("USD_SEK", "EUR_SEK"))
  )
testing_data <- cor_data_list[[1]]
testing_ramapped <-
  testing_data2 %>% dplyr::select(-c(Price, Open, High, Low))
mean_values_by_asset_for_loop_15_ask =
  wrangle_asset_data(
    asset_data_daily_raw = starting_asset_data_ask_15,
    summarise_means = TRUE
  )

#' get_cor_trade_results
#'
#' @param testing_data
#' @param raw_asset_data
#' @param sd_fac1
#' @param sd_fac2
#' @param stop_factor
#' @param profit_factor
#' @param trade_direction
#' @param mean_values_by_asset_for_loop
#' @param currency_conversion
#' @param asset_infor
#' @param risk_dollar_value
#' @param return_analysis
#'
#' @return
#' @export
#'
#' @examples
get_cor_trade_results_v2 <-
  function(
    testing_data = testing_ramapped,
    raw_asset_data = starting_asset_data_ask_15,
    sd_fac1 = 2,
    sd_fac2 = 2,
    AUD_USD_sd = 0,
    NZD_USD_sd = 0,
    EUR_JPY_SD = 0,
    GBP_JPY_SD = 0,
    stop_factor = 17,
    profit_factor = 25,
    trade_direction = "Long",
    mean_values_by_asset_for_loop = mean_values_by_asset_for_loop_15_ask,
    currency_conversion = currency_conversion,
    asset_infor = asset_infor,
    risk_dollar_value = 10,
    return_analysis = TRUE,
    pos_or_neg = "neg"
  ) {

    if(pos_or_neg == "pos") {
      tagged_trades <-
        testing_data %>%
        mutate(
          trade_col =
            case_when(
              pred >= mean_pred + sd_pred*sd_fac1 ~ trade_direction
              # pred <= mean_pred - sd_pred*sd_fac2 ~ trade_direction

              # pred >= mean_pred + sd_pred*sd_fac1 &
              #   pred <= mean_pred - sd_pred*sd_fac2 ~ trade_direction

            )
        ) %>%
        filter(!is.na(trade_col))
    }

    # param_tracking
    # NZD_USD sd: 0, NZD_USD_tangent_angle2 < 0, (AUD_USD_NZD_USD_cor <= AUD_USD_NZD_USD_cor_mean - AUD_USD_NZD_USD_cor_sd*0)
    # AUD_USD sd: 0, AUD_USD_tangent_angle1 < 0, (AUD_USD_NZD_USD_cor >= AUD_USD_NZD_USD_cor_mean + AUD_USD_NZD_USD_cor_sd*0)
    # pred <= mean_pred - sd_pred*sd_fac1 ~ trade_direction
    # EUR_JPY: EUR_JPY_SD= 0, (EUR_JPY_GBP_JPY_cor >= EUR_JPY_GBP_JPY_cor_mean + EUR_JPY_GBP_JPY_cor_sd*EUR_JPY_SD) &
    #   Asset == "EUR_JPY" & EUR_JPY_tangent_angle1 < 0

    names(testing_data) %>%
      keep(~ str_detect(.x, "tangent"))

    if(pos_or_neg == "neg") {
      tagged_trades <-
        testing_data %>%
        mutate(
          trade_col =
            case_when(
              # pred <= mean_pred - sd_pred*sd_fac1 ~ trade_direction,
              # (AUD_USD_NZD_USD_cor <= AUD_USD_NZD_USD_cor_mean - AUD_USD_NZD_USD_cor_sd*NZD_USD_sd) &
              #   Asset == "NZD_USD" & NZD_USD_tangent_angle2 < 0~ trade_direction,
              # (AUD_USD_NZD_USD_cor >= AUD_USD_NZD_USD_cor_mean + AUD_USD_NZD_USD_cor_sd*AUD_USD_sd) &
              #   Asset == "AUD_USD" & AUD_USD_tangent_angle1 < 0~ trade_direction,
              # (EUR_JPY_GBP_JPY_cor >= EUR_JPY_GBP_JPY_cor_mean + EUR_JPY_GBP_JPY_cor_sd*EUR_JPY_SD) &
              #   Asset == "EUR_JPY" & EUR_JPY_tangent_angle1 < 0~ trade_direction,

              (EUR_JPY_GBP_JPY_cor <= EUR_JPY_GBP_JPY_cor_mean - EUR_JPY_GBP_JPY_cor_sd*EUR_JPY_SD) &
                Asset == "GBP_JPY" & EUR_JPY_tangent_angle1 > 0~ trade_direction

            )
        ) %>%
        filter(!is.na(trade_col))
    }


    if(return_analysis == TRUE) {

      asset_in_trades <- tagged_trades %>%
        ungroup() %>%
        distinct(Asset) %>%
        pull(Asset) %>%
        unique()

      raw_asset_data_trade <-
        raw_asset_data %>%
        ungroup() %>%
        filter(Asset %in% asset_in_trades)

      long_bayes_loop_analysis_neg <-
        generic_trade_finder_loop(
          tagged_trades = tagged_trades ,
          asset_data_daily_raw = raw_asset_data_trade,
          stop_factor = stop_factor,
          profit_factor =profit_factor,
          trade_col = "trade_col",
          date_col = "Date",
          start_price_col = "Price",
          mean_values_by_asset = mean_values_by_asset_for_loop
        )

      trade_timings_neg <-
        long_bayes_loop_analysis_neg %>%
        mutate(
          ending_date_trade = as_datetime(ending_date_trade),
          dates = as_datetime(dates)
        ) %>%
        mutate(Time_Required = (ending_date_trade - dates)/dhours(1) )

      trade_timings_by_asset_neg <- trade_timings_neg %>%
        mutate(win_loss = ifelse(trade_returns < 0, "loss", "wins") ) %>%
        group_by(win_loss) %>%
        summarise(
          Time_Required = median(Time_Required, na.rm = T)
        ) %>%
        pivot_wider(names_from = win_loss, values_from = Time_Required) %>%
        rename(loss_time_hours = loss,
               win_time_hours = wins)

      analysis_data_neg <-
        generic_anlyser(
          trade_data = long_bayes_loop_analysis_neg %>% rename(Asset = asset),
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
        mutate(
          sd_fac1 = sd_fac1,
          pos_or_neg = pos_or_neg,
          sd_fac2 = sd_fac2
        ) %>%
        bind_cols(trade_timings_by_asset_neg)

      analysis_data_asset_neg <-
        generic_anlyser(
          trade_data = long_bayes_loop_analysis_neg %>% rename(Asset = asset),
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
        mutate(
          sd_fac1 = sd_fac1,
          pos_or_neg = pos_or_neg,
          sd_fac2= sd_fac2
        ) %>%
        bind_cols(trade_timings_by_asset_neg)

      # return(
      #   list(
      #     analysis_data_neg,
      #     analysis_data_asset_neg,
      #     tagged_trades
      #   )
      # )

    } else {

      return(
        list(
          tagged_trades
        )
      )

    }

  }
