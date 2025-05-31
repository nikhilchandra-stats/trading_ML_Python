helperfunctions35South::load_custom_functions()
one_drive_path <- helperfunctions35South::create_one_drive_path(
  path_extension = "raw data")
library(neuralnet)
raw_macro_data <- get_macro_event_data()
eur_data <- get_EUR_exports()
AUD_exports_total <- get_AUS_exports()  %>%
  pivot_longer(-TIME_PERIOD, names_to = "category", values_to = "Aus_Export") %>%
  rename(date = TIME_PERIOD) %>%
  group_by(date) %>%
  summarise(Aus_Export = sum(Aus_Export, na.rm = T))
USD_exports_total <- get_US_exports()  %>%
  pivot_longer(-date, names_to = "category", values_to = "US_Export") %>%
  group_by(date) %>%
  summarise(US_Export = sum(US_Export, na.rm = T)) %>%
  left_join(AUD_exports_total) %>%
  ungroup()
USD_exports_total <- USD_exports_total %>%
  mutate(
    month_date = lubridate::floor_date(date, "month")
  )
AUD_exports_total <- AUD_exports_total %>%
  mutate(
    month_date = lubridate::floor_date(date, "month")
  )
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

#' generic_trade_finder_trail_asset
#'
#' @param tagged_trades
#' @param asset_data_daily_raw
#' @param stop_factor
#' @param profit_factor
#' @param trade_col
#' @param date_col
#' @param start_price_col
#' @param mean_values_by_asset
#' @param asset_under_analysis
#' @param trailing_amount
#' @param currency_conversion
#' @param asset_infor
#' @param risk_dollar_value
#'
#' @return
#' @export
#'
#' @examples
generic_trade_finder_trail_asset <-
  function(
    tagged_trades = tagged_trades,
    asset_data_daily_raw = new_H1_data_ask,
    stop_factor = 3,
    profit_factor =3,
    trade_col = "trade_col",
    date_col = "Date",
    start_price_col = "Price",
    mean_values_by_asset =
      wrangle_asset_data(
        asset_data_daily_raw = new_H1_data_ask,
        summarise_means = TRUE),
    asset_under_analysis = "AU200_AUD",
    trailing_amount = 0.25,
    currency_conversion = currency_conversion,
    asset_infor = asset_infor,
    risk_dollar_value = 10,
    analysis_time_unit = 15
  ) {

    asset_data_for_analysis <-
      asset_data_daily_raw %>%
      filter(Asset == asset_under_analysis)

    trades_under_analysis <-
      tagged_trades %>%
      filter(Asset == asset_under_analysis) %>%
      filter(!is.na(!!as.name(trade_col)))

    distinct_trade_dates <-
      trades_under_analysis %>%
      pull(!!as.name(date_col))

    number_of_trades <- dim(trades_under_analysis)[1]

    stops_profs <-
      mean_values_by_asset %>%
      filter(Asset == asset_under_analysis) %>%
      mutate(
        stop_price = mean_daily + stop_factor*sd_daily,
        profit_price = mean_daily + profit_factor*sd_daily,
        stop_price_trail = stop_price*trailing_amount,
        profit_price_trail = profit_price*trailing_amount
      )


    stop_distance <- stops_profs$stop_price[1] %>% as.numeric()
    profit_distance <- stops_profs$profit_price[1] %>% as.numeric()

    stop_distance_trail <- stops_profs$stop_price_trail[1] %>% as.numeric()
    profit_distance_trail <- stops_profs$profit_price_trail[1] %>% as.numeric()

    end_price_speed <- numeric(number_of_trades)
    start_price_speed <- numeric(number_of_trades)
    profit_price_speed <- numeric(number_of_trades)
    trade_directions_speed <- trades_under_analysis %>% pull(!!as.name(trade_col)) %>% as.character()
    time_index_to_finish_speed <- numeric(number_of_trades)

    plot_list<- list()

    for (i in 1:number_of_trades) {

      trade_date <- distinct_trade_dates[i]

      pricing_data_internal_loop <-
        asset_data_for_analysis %>%
        filter(!!as.name(date_col) >= trade_date)

      starting_price <-
        pricing_data_internal_loop %>%
        pull(!!as.name(start_price_col)) %>%
        pluck(1) %>%
        as.numeric()

      pricing_data_internal_loop <-
        pricing_data_internal_loop %>%
        arrange(!!as.name(date_col))

      Lows <- pricing_data_internal_loop$Low %>% as.numeric()
      Highs <- pricing_data_internal_loop$High %>% as.numeric()
      prices <- pricing_data_internal_loop$Price %>% as.numeric()

      if(trade_directions_speed[i] == "Long") {
        stop_price <- starting_price - stop_distance
        profit_price <- starting_price + profit_distance

        trade_finished <- FALSE
        k = 0
        current_stop_distance <- stop_distance

        # track_stops <- numeric(length(Lows))
        # track_profits <- numeric(length(Lows))
        # track_lows <- numeric(length(Lows))

        while(trade_finished == FALSE) {
          k = k + 1

          # track_stops[k] <- stop_price
          # track_profits[k] <- profit_price
          # track_lows[k] <- Lows[k]

          if(Lows[k] <= stop_price) {
            end_price <- stop_price
            trade_finished <- TRUE
          }

          if(Highs[k] >= profit_price) {
            end_price <- profit_price
            trade_finished <- TRUE
          }

          if(Lows[k] >= stop_price + trailing_amount*profit_distance) {
            stop_price <- stop_price + trailing_amount*profit_distance
            profit_price <- profit_price + trailing_amount*profit_distance
          }

          if(k == length(Lows)) {
            trade_finished <- TRUE
            end_price <- Lows[k]
          }

        }

        # track_stops <- track_stops[1:k]
        # track_profits <- track_profits[1:k]
        # track_lows <- track_lows[1:k]
        #
        # test_plot <-
        #   tibble(
        #     index = seq(1,k, 1),
        #     track_stops = track_stops,
        #     track_profits = track_profits,
        #     track_lows = track_lows
        #   )
        #
        # plot_list[[i]] <- test_plot %>%
        #   pivot_longer(-index, names_to = "XX", values_to = "Price") %>%
        #   ggplot(aes(x = index, color = XX, y = Price)) +
        #   geom_line() +
        #   geom_point() +
        #   theme_minimal()

      }

      if(trade_directions_speed[i] == "Short") {
        stop_price <- starting_price + stop_distance
        profit_price <- starting_price - profit_distance

        trade_finished <- FALSE
        k = 0
        current_stop_distance <- stop_distance

        while(trade_finished == FALSE) {
          k = k + 1

          if(Highs[k] >= stop_price) {
            end_price <- stop_price
            trade_finished <- TRUE
          }

          if(Lows[k] <= profit_price) {
            end_price <- profit_price
            trade_finished <- TRUE
          }

          if(Highs[k] <= stop_price - trailing_amount*profit_distance) {
            stop_price <- stop_price - trailing_amount*profit_distance
            profit_price <- profit_price - trailing_amount*profit_distance
          }

          if(k == length(Lows)) {
            trade_finished <- TRUE
            end_price <- Highs[k]
          }
        }

      }

      start_price_speed[i] <- starting_price
      end_price_speed[i] <- end_price
      profit_price_speed[i] <- abs(profit_price - starting_price)
      time_index_to_finish_speed[i] <- k

    }

    returned_data <-
      tibble(
        Date = distinct_trade_dates,
        Asset = asset_under_analysis,
        start_price = start_price_speed,
        end_price = end_price_speed,
        trade_col = trade_directions_speed,
        stop_distance = stop_distance,
        profit_distance = profit_price_speed,
        stop_factor = stop_factor,
        profit_factor = profit_factor,
        loop_index_when_trade_finished = time_index_to_finish_speed
      ) %>%
      left_join(asset_infor %>%
                  dplyr::select(Asset = name,
                                displayPrecision,
                                pipLocation,
                                tradeUnitsPrecision)
                ) %>%
      mutate(
        displayPrecision= as.numeric(displayPrecision),
        tradeUnitsPrecision= as.numeric(tradeUnitsPrecision),
        pipLocation= as.numeric(pipLocation)
      ) %>%
      mutate(
        trade_return =
          case_when(
            trade_col == "Long" ~
            # & end_price >= start_price ~
              end_price - start_price,
            trade_col == "Short" ~
            # & end_price < start_price ~
              start_price - end_price
            ),
        trade_return = round(trade_return, displayPrecision)
      ) %>%
      dplyr::select(-displayPrecision, -tradeUnitsPrecision, -pipLocation) %>%
      left_join(
        asset_data_for_analysis
      )

    trades_with_volume <-
      get_stops_profs_volume_trades(
        tagged_trades = returned_data,
        mean_values_by_asset = mean_values_by_asset,
        trade_col = "trade_col",
        currency_conversion = currency_conversion,
        risk_dollar_value = risk_dollar_value,
        stop_factor = stop_factor,
        profit_factor =profit_factor,
        asset_col = "Asset",
        stop_col = "stop_distance",
        profit_col = "profit_distance",
        price_col = "Price",
        trade_return_col = "trade_return"
      ) %>%
      mutate(
        trade_return_dollars_AUD =
          volume_adj*trade_return
      )

    return(trades_with_volume)

  }

generic_trade_finder_trail_all <-
  function(
    tagged_trades = tagged_trades,
    asset_data_daily_raw = new_H1_data_ask,
    stop_factor = 1,
    profit_factor =1,
    trade_col = "trade_col",
    date_col = "Date",
    start_price_col = "Price",
    mean_values_by_asset =
      wrangle_asset_data(
        asset_data_daily_raw = new_H1_data_bid,
        summarise_means = TRUE),
    trailing_amount = 0.25,
    currency_conversion = currency_conversion,
    asset_infor = asset_infor,
    risk_dollar_value = 10
  ) {

    distinct_assets <- tagged_trades %>% distinct(Asset) %>% pull(Asset)

    all_results <-
      distinct_assets %>%
      map(
        ~
          generic_trade_finder_trail_asset(
            tagged_trades = tagged_trades %>% filter(Asset == .x),
            asset_data_daily_raw = asset_data_daily_raw %>% filter(Asset == .x),
            stop_factor = stop_factor,
            profit_factor = profit_factor,
            trade_col = trade_col,
            date_col = date_col,
            start_price_col = start_price_col,
            mean_values_by_asset = mean_values_by_asset,
            asset_under_analysis = .x,
            trailing_amount = trailing_amount,
            currency_conversion = currency_conversion,
            asset_infor = asset_infor,
            risk_dollar_value = risk_dollar_value
          )
      )

    all_results_dfr <- all_results %>%
      map_dfr(
        ~ .x  %>%
          mutate(
            across(.cols = c(trade_return_dollars_AUD, trade_return),
                   .fns = ~
                        case_when(
                          !!as.name(trade_col) == "Long" & end_price > start_price ~ abs(.),
                          !!as.name(trade_col) == "Long" & end_price <= start_price ~ -1*abs(.),
                          !!as.name(trade_col) == "Short" & end_price < start_price ~ abs(.),
                          !!as.name(trade_col) == "Short" & end_price >= start_price ~ -1*abs(.)
                        )
                     )
          )
      )

    return(all_results_dfr)

  }

#----------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------
#--------------------------------------------Short Neural Network
db_location = "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data.db"
start_date_day = "2011-01-01"
end_date_day = today() %>% as.character()
current_date <- now() %>% as_date(tz = "Australia/Canberra")

starting_asset_data_ask_daily <-
  get_db_price(
    db_location = db_location,
    start_date = start_date_day,
    end_date = end_date_day,
    bid_or_ask = "ask",
    time_frame = "D"
  )

starting_asset_data_ask_H1 <-
  get_db_price(
    db_location = db_location,
    start_date = start_date_day,
    end_date = end_date_day,
    bid_or_ask = "ask",
    time_frame = "H1"
  )

mean_values_by_asset_for_loop_D_ask =
  wrangle_asset_data(
    asset_data_daily_raw = starting_asset_data_ask_daily,
    summarise_means = TRUE
  )

mean_values_by_asset_for_loop_H1_ask =
  wrangle_asset_data(
    asset_data_daily_raw = starting_asset_data_ask_H1,
    summarise_means = TRUE
  )

starting_asset_data_bid_daily <-
  get_db_price(
    db_location = db_location,
    start_date = start_date_day,
    end_date = end_date_day,
    bid_or_ask = "bid",
    time_frame = "D"
  )

starting_asset_data_bid_H1 <-
  get_db_price(
    db_location = db_location,
    start_date = start_date_day,
    end_date = end_date_day,
    bid_or_ask = "bid",
    time_frame = "H1"
  )

mean_values_by_asset_for_loop_D_bid =
  wrangle_asset_data(
    asset_data_daily_raw = starting_asset_data_bid_daily,
    summarise_means = TRUE
  )

mean_values_by_asset_for_loop_H1_bid =
  wrangle_asset_data(
    asset_data_daily_raw = starting_asset_data_bid_H1,
    summarise_means = TRUE
  )

new_daily_data_ask <-
  updated_data_internal(starting_asset_data = starting_asset_data_ask_daily,
                        end_date_day = current_date,
                        time_frame = "D", bid_or_ask = "ask") %>%
  distinct()
new_H1_data_ask <-
  updated_data_internal(starting_asset_data = starting_asset_data_ask_H1,
                        end_date_day = current_date,
                        time_frame = "H1", bid_or_ask = "ask")%>%
  distinct()

new_daily_data_bid <-
  updated_data_internal(starting_asset_data = starting_asset_data_bid_daily,
                        end_date_day = current_date,
                        time_frame = "D", bid_or_ask = "bid") %>%
  distinct()
new_H1_data_bid <-
  updated_data_internal(starting_asset_data = starting_asset_data_bid_H1,
                        end_date_day = current_date,
                        time_frame = "H1", bid_or_ask = "bid")%>%
  distinct()

Hour_data_with_LM_ask <-
  run_LM_join_to_H1(
    daily_data_internal = new_daily_data_ask,
    H1_data_internal = new_H1_data_ask,
    raw_macro_data = raw_macro_data,
    AUD_exports_total = AUD_exports_total,
    USD_exports_total = USD_exports_total,
    eur_data = eur_data
  )

Hour_data_with_LM_markov_ask <-
  extract_required_markov_data(
    Hour_data_with_LM = Hour_data_with_LM_ask,
    new_daily_data_ask = new_daily_data_ask,
    currency_conversion = currency_conversion,
    mean_values_by_asset_for_loop = mean_values_by_asset_for_loop_D_ask,
    profit_factor  = 5,
    stop_factor  = 3,
    risk_dollar_value = 5,
    trade_sd_fact = 2
  )

Hour_data_with_LM_bid <-
  run_LM_join_to_H1(
    daily_data_internal = new_daily_data_bid,
    H1_data_internal = new_H1_data_bid,
    raw_macro_data = raw_macro_data,
    AUD_exports_total = AUD_exports_total,
    USD_exports_total = USD_exports_total,
    eur_data = eur_data
  )

Hour_data_with_LM_markov_bid <-
  extract_required_markov_data(
    Hour_data_with_LM = Hour_data_with_LM_bid,
    new_daily_data_ask = new_daily_data_bid,
    currency_conversion = currency_conversion,
    mean_values_by_asset_for_loop = mean_values_by_asset_for_loop_D_bid,
    profit_factor  = 5,
    stop_factor  = 3,
    risk_dollar_value = 5,
    trade_sd_fact = 2
  )

H1_Model_data_train_bid <-
  Hour_data_with_LM_markov_bid %>%
  group_by(Asset) %>%
  slice_head(prop = 0.4)

H1_Model_data_test_bid <-
  Hour_data_with_LM_markov_bid %>%
  group_by(Asset) %>%
  slice_tail(prop = 0.55)

H1_Model_data_train_ask <-
  Hour_data_with_LM_markov_ask %>%
  group_by(Asset) %>%
  slice_head(prop = 0.4)

H1_Model_data_test_ask <-
  Hour_data_with_LM_markov_ask %>%
  group_by(Asset) %>%
  slice_tail(prop = 0.55)

#---------------------------------------------Daily Regression Join
db_location = "C:/Users/Nikhil Chandra/Documents/Asset Data/Oanda_Asset_Data.db"
start_date_day = "2011-01-01"
end_date_day = today() %>% as.character()

H1_model_High_SD_25_71_neg <- readRDS(
  glue::glue("C:/Users/Nikhil Chandra/Documents/trade_data/H1_LM_Markov_NN_25_SD_71Perc_2025-05-13.rds")
)

H1_LM_Markov_NN_Long_56_prof_10_4sd2025_05_17 <- readRDS(
  glue::glue("C:/Users/Nikhil Chandra/Documents/trade_data/H1_LM_Markov_NN_Long_56_prof_10_4sd2025-05-17.rds")
)

LM_ML_HighLead_14_14_layer <-
  readRDS(glue::glue("C:/Users/Nikhil Chandra/Documents/trade_data/LM_ML_HighLead_14_14_layer_2025-05-19.rds"))


trades_NN_1 <- get_NN_best_trades_from_mult_anaysis(
  db_path = "C:/Users/Nikhil Chandra/Documents/trade_data/NN_simulation_results.db",
  network_name = "H1_LM_Markov_NN_Long_56_prof_10_4sd2025_05_17",
  NN_model = H1_LM_Markov_NN_Long_56_prof_10_4sd2025_05_17,
  Hour_data_with_LM_markov = Hour_data_with_LM_markov_ask,
  mean_values_by_asset_for_loop_H1 = mean_values_by_asset_for_loop_H1_ask,
  currency_conversion = currency_conversion,
  asset_infor = asset_infor,
  risk_dollar_value = 10,
  win_threshold = 0.6,
  slice_max = FALSE
)

trades_NN_1_stripped <- trades_NN_1 %>%
  dplyr::select(Date, Asset, Price, Low, High, Open, trade_col)

test_assets<- c("AUD_USD", "SPX_USD", "EUR_USD", "EU50_EUR", "EUR_GBP")

long_trades <-
  generic_trade_finder_trail_all(
    tagged_trades = trades_NN_1 ,
    asset_data_daily_raw = new_H1_data_ask,
    stop_factor = 3,
    profit_factor =15,
    trade_col = "trade_col",
    date_col = "Date",
    start_price_col = "Price",
    mean_values_by_asset =
      wrangle_asset_data(
        asset_data_daily_raw = new_H1_data_ask,
        summarise_means = TRUE),
    trailing_amount = 0.05,
    currency_conversion = currency_conversion,
    asset_infor = asset_infor,
    risk_dollar_value = 10
  )

long_trades_NN1_sum <- long_trades %>%
  group_by(Asset) %>%
  summarise(trade_return_dollars_AUD = sum(trade_return_dollars_AUD, na.rm = T))
