generic_trade_finder <- function(
                                 tagged_trades = trade_with_daily_data,
                                 asset_data_daily_raw = asset_data_daily_raw,
                                 stop_factor = 3,
                                 profit_factor =3,
                                 trade_col = "trade_col",
                                 date_col = "Date",
                                 max_hold_period = 100,
                                 start_price_col = "Price",
                                 mean_values_by_asset =
                                   wrangle_asset_data(
                                     asset_data_daily_raw = asset_data_daily_raw,
                                     summarise_means = TRUE
                                   ),
                                 return_summary = TRUE,
                                 additional_grouping_vars = c("Asset")
                                 ) {


  trades_stripped_down <-
    tagged_trades %>%
    distinct(!!as.name(date_col), !!as.name(trade_col),
             across(matches(additional_grouping_vars)),
             Price, Low, High, Open)

 long_analysis <- trades_stripped_down %>%
   group_by(across(matches(additional_grouping_vars))) %>%
   arrange(!!as.name(date_col), .by_group = TRUE) %>%
   group_by(across(matches(additional_grouping_vars))) %>%
   mutate(
     start_price = ifelse(!!as.name(trade_col) == "Long", !!as.name(start_price_col), NA),
     max_in_30_days= slider::slide_dbl(.x = High, .f = ~ max(.x, na.rm = T), .after = max_hold_period ),
     min_in_30_days= slider::slide_dbl(.x = Low, .f = ~ min(.x, na.rm = T), .after = max_hold_period ),
     close_in_30_days = lead(Price, max_hold_period),
     start_to_High = max_in_30_days - start_price,
     start_to_Low = start_price - min_in_30_days,
     start_to_Close = close_in_30_days - start_price,
     row_num_x_1 = floor(row_number()/max_hold_period),
     max_point = slider::slide_dbl(.x = High,
                                   .f = ~ which(.x == max(.x, na.rm = T))[1] ,
                                   .after = max_hold_period ),
     min_point = slider::slide_dbl(.x = Low,
                                   .f = ~ which(.x == min(.x, na.rm = T))[1] ,
                                   .after = max_hold_period )
   ) %>%
   ungroup() %>%
   left_join(mean_values_by_asset) %>%
   mutate(
     stop_value = mean_daily + sd_daily*stop_factor,
     profit_value = mean_daily + sd_daily*profit_factor
   ) %>%
   mutate(
     low_crosses_stop = stop_value <= start_to_Low,
     high_crosses_profit =  profit_value <= start_to_High,
     Low_Point_Earlier = min_point <= max_point,
     High_Point_Earlier = max_point < min_point & Low_Point_Earlier == FALSE
   ) %>%
   mutate(
      win_loss =
        case_when(
          low_crosses_stop == TRUE & high_crosses_profit == FALSE  ~0,
          high_crosses_profit == TRUE & low_crosses_stop == FALSE ~ 1,

          high_crosses_profit == TRUE & low_crosses_stop == TRUE & Low_Point_Earlier == TRUE ~ 0,
          high_crosses_profit == TRUE & low_crosses_stop == TRUE & High_Point_Earlier == TRUE ~ 1,

          high_crosses_profit == FALSE & low_crosses_stop == FALSE & start_to_Close < 0 ~ 0,
          high_crosses_profit == FALSE & low_crosses_stop == FALSE &start_to_Close > 0 ~ 1
        )
   ) %>%
   mutate(
     trade_category =
       case_when(
         low_crosses_stop == TRUE & high_crosses_profit == FALSE  ~ "TRUE LOSS",
         high_crosses_profit == TRUE & low_crosses_stop == FALSE ~ "TRUE WIN",

         high_crosses_profit == TRUE & low_crosses_stop == TRUE & Low_Point_Earlier == TRUE ~ "TRUE LOSS",
         high_crosses_profit == TRUE & low_crosses_stop == TRUE & High_Point_Earlier == TRUE ~ "TRUE WIN",

         high_crosses_profit == FALSE & low_crosses_stop == FALSE & start_to_Close > 0 ~ "WEAK WIN",
         high_crosses_profit == FALSE & low_crosses_stop == FALSE &start_to_Close < 0 ~ "WEAK LOSS"
       )
   ) %>%
   mutate(
     profit_loss_value =
       case_when(
         low_crosses_stop == TRUE & high_crosses_profit == FALSE ~ -1*stop_value,
         high_crosses_profit == TRUE & low_crosses_stop == FALSE ~ profit_value,

         high_crosses_profit == TRUE & low_crosses_stop == TRUE & Low_Point_Earlier == TRUE ~ -1*stop_value,
         high_crosses_profit == TRUE & low_crosses_stop == TRUE & High_Point_Earlier == TRUE ~ profit_value,

         high_crosses_profit == FALSE & low_crosses_stop == FALSE & start_to_Close < 0 ~ start_to_Close,
         high_crosses_profit == FALSE & low_crosses_stop == FALSE &start_to_Close > 0 ~ start_to_Close
   )
   ) %>%
   filter(!is.na(!!as.name(trade_col)),
          !!as.name(trade_col) == "Long")

 # long_analysis_summary <-
 #   long_analysis %>%
 #   group_by(Asset) %>%
 #   filter(!is.na(!!as.name(trade_col)) , !!as.name(trade_col) == "Long") %>%
 #   summarise(
 #     wins = sum(win_loss, na.rm = T),
 #     profit_loss = round(sum(profit_loss_value, na.rm = T)),
 #     total_trades = n()
 #   ) %>%
 #   mutate(
 #     Perc = wins/total_trades
 #   )

 long_analysis_summary <-
   long_analysis %>%
   filter(!is.na(!!as.name(trade_col)), !!as.name(trade_col) == "Long" ) %>%
   group_by(trade_category, across(matches(additional_grouping_vars)) ) %>%
   summarise(
     Trades = n(),
     profit_loss = round(sum(profit_loss_value, na.rm = T)),
     average_stop = median(stop_value, na.rm = T),
     averaeg_profit = median(profit_value, na.rm = T)
   ) %>%
   ungroup() %>%
   group_by(across(matches(additional_grouping_vars))) %>%
   mutate(
     total_trades = sum(Trades, na.rm = T),
     Perc = Trades/total_trades,
     profit_factor = profit_factor,
     stop_factor = stop_factor,
     trade_direction = "Long"
   )


 short_analysis <- trades_stripped_down %>%
   group_by(across(matches(additional_grouping_vars))) %>%
   arrange(!!as.name(date_col), .by_group = TRUE) %>%
   group_by(across(matches(additional_grouping_vars))) %>%
   mutate(
     start_price = ifelse(!!as.name(trade_col) == "Short", !!as.name(start_price_col), NA),
     max_in_30_days= slider::slide_dbl(.x = High, .f = ~ max(.x, na.rm = T), .after = max_hold_period ),
     min_in_30_days= slider::slide_dbl(.x = Low, .f = ~ min(.x, na.rm = T), .after = max_hold_period ),
     close_in_30_days = lead(Price, max_hold_period),
     start_to_High = max_in_30_days - start_price,
     start_to_Low = start_price - min_in_30_days,
     start_to_Close = close_in_30_days - start_price,
     row_num_x_1 = floor(row_number()/max_hold_period),
     max_point = slider::slide_dbl(.x = High,
                                   .f = ~ which(.x == max(.x, na.rm = T))[1] ,
                                   .after = max_hold_period ),
     min_point = slider::slide_dbl(.x = Low,
                                   .f = ~ which(.x == min(.x, na.rm = T))[1] ,
                                   .after = max_hold_period )
   ) %>%
   ungroup() %>%
   left_join(mean_values_by_asset) %>%
   mutate(
     stop_value = mean_daily + sd_daily*stop_factor,
     profit_value = mean_daily + sd_daily*profit_factor
   ) %>%
   mutate(
     low_crosses_profit =  profit_value<= start_to_Low,
     high_crosses_stop =   stop_value <= start_to_High,
     Low_Point_Earlier = min_point < max_point,
     High_Point_Earlier = max_point <= min_point
   ) %>%
   mutate(
     win_loss =
       case_when(

         high_crosses_stop == TRUE & low_crosses_profit == FALSE  ~0,
         low_crosses_profit == TRUE & high_crosses_stop == FALSE ~ 1,

         low_crosses_profit == TRUE & high_crosses_stop == TRUE &  High_Point_Earlier== TRUE ~ 0,
         low_crosses_profit == TRUE & high_crosses_stop == TRUE &  Low_Point_Earlier== TRUE ~ 1,

         low_crosses_profit == FALSE & high_crosses_stop == FALSE & start_to_Close < 0 ~ 1,
         low_crosses_profit == FALSE & high_crosses_stop == FALSE & start_to_Close > 0 ~ 0
       )
   ) %>%
   mutate(
     trade_category =
       case_when(
         high_crosses_stop == TRUE & low_crosses_profit == FALSE  ~ "TRUE LOSS",
         low_crosses_profit == TRUE & high_crosses_stop == FALSE ~ "TRUE WIN",

         low_crosses_profit == TRUE & high_crosses_stop == TRUE &  High_Point_Earlier== TRUE ~ "TRUE LOSS",
         low_crosses_profit == TRUE & high_crosses_stop == TRUE &  Low_Point_Earlier== TRUE ~ "TRUE WIN",

         low_crosses_profit == FALSE & high_crosses_stop == FALSE & start_to_Close < 0 ~ "WEAK WIN",
         low_crosses_profit == FALSE & high_crosses_stop == FALSE &start_to_Close > 0 ~ "WEAK LOSS"
       )
   ) %>%
   mutate(
     profit_loss_value =
       case_when(
         high_crosses_stop == TRUE & low_crosses_profit == FALSE ~ -1*stop_value,
         low_crosses_profit == TRUE & high_crosses_stop ~ profit_value,

         low_crosses_profit == TRUE & high_crosses_stop == TRUE &  High_Point_Earlier== TRUE ~ -1*stop_value,
         low_crosses_profit == TRUE & high_crosses_stop == TRUE &  Low_Point_Earlier== TRUE ~ profit_value,

         low_crosses_profit == FALSE & high_crosses_stop == FALSE & start_to_Close < 0 ~ start_to_Close,
         low_crosses_profit == FALSE & high_crosses_stop == FALSE &start_to_Close > 0 ~ start_to_Close
       )
   ) %>%
   filter(!is.na(!!as.name(trade_col)),
          !!as.name(trade_col) == "Short")

 short_analysis_summary <-
   short_analysis %>%
   filter(!is.na(!!as.name(trade_col)), !!as.name(trade_col) == "Short" ) %>%
   group_by(trade_category, across(matches(additional_grouping_vars)) ) %>%
   summarise(
     Trades = n(),
     profit_loss = round(sum(profit_loss_value, na.rm = T)),
     average_stop = median(stop_value, na.rm = T),
     averaeg_profit = median(profit_value, na.rm = T)
   ) %>%
   ungroup() %>%
   group_by(Asset) %>%
   mutate(
     total_trades = sum(Trades, na.rm = T),
     Perc = Trades/total_trades,
     profit_factor = profit_factor,
     stop_factor = stop_factor,
     trade_direction = "Short"
   )

 if(return_summary) {
   return(list(long_analysis_summary, short_analysis_summary))
 }

 if(return_summary == FALSE) {
   return(list(long_analysis, short_analysis))
 }

}

get_asset_daily_changes <- function(path_var = "C:/Users/Nikhil Chandra/Documents/Asset Data/Futures/") {

  asset_data_daily_raw <- fs::dir_info(path_var) %>%
    mutate(asset_name =
             str_remove(path, "C\\:\\/Users/Nikhil Chandra\\/Documents\\/Asset Data\\/Futures\\/") %>%
             str_remove("\\.csv") %>%
             str_remove("Historical Data")%>%
             str_remove("Stock Price") %>%
             str_remove("History")
    ) %>%
    dplyr::select(path, asset_name) %>%
    split(.$asset_name, drop = FALSE) %>%
    map_dfr( ~ read_csv(.x[1,1] %>% as.character()) %>%
               mutate(Asset = .x[1,2] %>% as.character())
    )

  return(asset_data_daily_raw)

}

wrangle_asset_data <- function(asset_data_daily_raw = asset_data_daily_raw,
                               summarise_means = TRUE) {

  transformed_data <- asset_data_daily_raw %>%
    mutate(Date = as.Date(Date, format =  "%m/%d/%Y"))  %>%
    mutate(
      week_date = lubridate::floor_date(Date, "week", 1)
    ) %>%
    group_by(Asset) %>%
    arrange(Date, .by_group = TRUE) %>%
    group_by(Asset) %>%
    mutate(
      daily_change = Price - Open
    ) %>%
    group_by(Asset, week_date) %>%
    mutate(weekly_close = ifelse(Date == max(Date, na.rm = T), Price, NA),
           weekly_open = ifelse(Date == min(Date, na.rm = T), Open, NA)
           )%>%
    group_by(Asset) %>%
    fill(c(weekly_open), .direction = "down")%>%
    group_by(Asset) %>%
    fill(c(weekly_close), .direction = "up") %>%
    mutate(
      weekly_change = weekly_close - weekly_open
    )

  if(summarise_means) {
    transformed_data <- transformed_data %>%
      group_by(Asset) %>%
      summarise(
        mean_daily = mean(abs(daily_change), na.rm = T),
        sd_daily = sd(abs(daily_change), na.rm = T),

        mean_weekly = mean(abs(weekly_change), na.rm = T),
        sd_weekly = sd(abs(weekly_change), na.rm = T)
      )
  }

  return(transformed_data)

}

generic_trade_finder_conservative <- function(
    tagged_trades = trade_with_daily_data,
    asset_data_daily_raw = asset_data_daily_raw,
    stop_factor = 3,
    profit_factor =3,
    trade_col = "trade_col",
    date_col = "Date",
    max_hold_period = 100,
    start_price_col = "Price",
    mean_values_by_asset =
      wrangle_asset_data(
        asset_data_daily_raw = asset_data_daily_raw,
        summarise_means = TRUE
      ),
    return_summary = TRUE,
    additional_grouping_vars = c("Asset")
) {


  trades_stripped_down <-
    tagged_trades %>%
    distinct(!!as.name(date_col), !!as.name(trade_col),
             across(matches(additional_grouping_vars)),
             Price, Low, High, Open)

  long_analysis <- trades_stripped_down %>%
    group_by(across(matches(additional_grouping_vars))) %>%
    arrange(!!as.name(date_col), .by_group = TRUE) %>%
    group_by(across(matches(additional_grouping_vars))) %>%
    mutate(
      start_price = ifelse(!!as.name(trade_col) == "Long", !!as.name(start_price_col), NA),
      max_in_30_days= lead(slider::slide_dbl(.x = High, .f = ~ max(.x, na.rm = T), .after = max_hold_period ), 1),
      min_in_30_days= lead(slider::slide_dbl(.x = Low, .f = ~ min(.x, na.rm = T), .after = max_hold_period ), 1),
      close_in_30_days = lead(Price, max_hold_period),
      start_to_High = max_in_30_days - start_price,
      start_to_Low = start_price - min_in_30_days,
      start_to_Close = close_in_30_days - start_price,
      row_num_x_1 = floor(row_number()/max_hold_period),
      max_point = lead(slider::slide_dbl(.x = High,
                                    .f = ~ which(.x == max(.x, na.rm = T))[1] ,
                                    .after = max_hold_period ),1),
      min_point = lead(slider::slide_dbl(.x = Low,
                                    .f = ~ which(.x == min(.x, na.rm = T))[1] ,
                                    .after = max_hold_period ),1)
    ) %>%
    ungroup() %>%
    left_join(mean_values_by_asset) %>%
    mutate(
      stop_value = mean_daily + sd_daily*stop_factor,
      profit_value = mean_daily + sd_daily*profit_factor
    ) %>%
    mutate(
      low_crosses_stop = stop_value <= start_to_Low,
      high_crosses_profit =  profit_value <= start_to_High,
      Low_Point_Earlier = min_point <= max_point,
      High_Point_Earlier = max_point < min_point & Low_Point_Earlier == FALSE
    ) %>%
    mutate(
      win_loss =
        case_when(
          low_crosses_stop == TRUE & high_crosses_profit == FALSE  ~0,
          high_crosses_profit == TRUE & low_crosses_stop == FALSE ~ 1,

          high_crosses_profit == TRUE & low_crosses_stop == TRUE & Low_Point_Earlier == TRUE ~ 0,
          high_crosses_profit == TRUE & low_crosses_stop == TRUE & High_Point_Earlier == TRUE ~ 1,

          high_crosses_profit == FALSE & low_crosses_stop == FALSE & start_to_Close < 0 ~ 0,
          high_crosses_profit == FALSE & low_crosses_stop == FALSE &start_to_Close > 0 ~ 1
        )
    ) %>%
    mutate(
      trade_category =
        case_when(
          low_crosses_stop == TRUE & high_crosses_profit == FALSE  ~ "TRUE LOSS",
          high_crosses_profit == TRUE & low_crosses_stop == FALSE ~ "TRUE WIN",

          high_crosses_profit == TRUE & low_crosses_stop == TRUE & Low_Point_Earlier == TRUE ~ "TRUE LOSS",
          high_crosses_profit == TRUE & low_crosses_stop == TRUE & High_Point_Earlier == TRUE ~ "TRUE WIN",

          high_crosses_profit == FALSE & low_crosses_stop == FALSE & start_to_Close > 0 ~ "WEAK WIN",
          high_crosses_profit == FALSE & low_crosses_stop == FALSE &start_to_Close < 0 ~ "WEAK LOSS"
        )
    ) %>%
    mutate(
      profit_loss_value =
        case_when(
          low_crosses_stop == TRUE & high_crosses_profit == FALSE ~ -1*stop_value,
          high_crosses_profit == TRUE & low_crosses_stop == FALSE ~ profit_value,

          high_crosses_profit == TRUE & low_crosses_stop == TRUE & Low_Point_Earlier == TRUE ~ -1*stop_value,
          high_crosses_profit == TRUE & low_crosses_stop == TRUE & High_Point_Earlier == TRUE ~ profit_value,

          high_crosses_profit == FALSE & low_crosses_stop == FALSE & start_to_Close < 0 ~ start_to_Close,
          high_crosses_profit == FALSE & low_crosses_stop == FALSE &start_to_Close > 0 ~ start_to_Close
        )
    ) %>%
    filter(!is.na(!!as.name(trade_col)),
           !!as.name(trade_col) == "Long")

  # long_analysis_summary <-
  #   long_analysis %>%
  #   group_by(Asset) %>%
  #   filter(!is.na(!!as.name(trade_col)) , !!as.name(trade_col) == "Long") %>%
  #   summarise(
  #     wins = sum(win_loss, na.rm = T),
  #     profit_loss = round(sum(profit_loss_value, na.rm = T)),
  #     total_trades = n()
  #   ) %>%
  #   mutate(
  #     Perc = wins/total_trades
  #   )

  long_analysis_summary <-
    long_analysis %>%
    filter(!is.na(!!as.name(trade_col)), !!as.name(trade_col) == "Long" ) %>%
    group_by(trade_category, across(matches(additional_grouping_vars)) ) %>%
    summarise(
      Trades = n(),
      profit_loss = round(sum(profit_loss_value, na.rm = T)),
      average_stop = median(stop_value, na.rm = T),
      averaeg_profit = median(profit_value, na.rm = T)
    ) %>%
    ungroup() %>%
    group_by(across(matches(additional_grouping_vars))) %>%
    mutate(
      total_trades = sum(Trades, na.rm = T),
      Perc = Trades/total_trades,
      profit_factor = profit_factor,
      stop_factor = stop_factor,
      trade_direction = "Long"
    )


  short_analysis <- trades_stripped_down %>%
    group_by(across(matches(additional_grouping_vars))) %>%
    arrange(!!as.name(date_col), .by_group = TRUE) %>%
    group_by(across(matches(additional_grouping_vars))) %>%
    mutate(
      start_price = ifelse(!!as.name(trade_col) == "Short", !!as.name(start_price_col), NA),
      max_in_30_days= lead(slider::slide_dbl(.x = High, .f = ~ max(.x, na.rm = T), .after = max_hold_period )),
      min_in_30_days= lead(slider::slide_dbl(.x = Low, .f = ~ min(.x, na.rm = T), .after = max_hold_period )),
      close_in_30_days = lead(Price, max_hold_period),
      start_to_High = max_in_30_days - start_price,
      start_to_Low = start_price - min_in_30_days,
      start_to_Close = close_in_30_days - start_price,
      row_num_x_1 = floor(row_number()/max_hold_period),
      max_point = lead(slider::slide_dbl(.x = High,
                                    .f = ~ which(.x == max(.x, na.rm = T))[1] ,
                                    .after = max_hold_period ), 1),
      min_point = lead(slider::slide_dbl(.x = Low,
                                    .f = ~ which(.x == min(.x, na.rm = T))[1] ,
                                    .after = max_hold_period ), 1)
    ) %>%
    ungroup() %>%
    left_join(mean_values_by_asset) %>%
    mutate(
      stop_value = mean_daily + sd_daily*stop_factor,
      profit_value = mean_daily + sd_daily*profit_factor
    ) %>%
    mutate(
      low_crosses_profit =  profit_value<= start_to_Low,
      high_crosses_stop =   stop_value <= start_to_High,
      Low_Point_Earlier = min_point < max_point,
      High_Point_Earlier = max_point <= min_point
    ) %>%
    mutate(
      win_loss =
        case_when(

          high_crosses_stop == TRUE & low_crosses_profit == FALSE  ~0,
          low_crosses_profit == TRUE & high_crosses_stop == FALSE ~ 1,

          low_crosses_profit == TRUE & high_crosses_stop == TRUE &  High_Point_Earlier== TRUE ~ 0,
          low_crosses_profit == TRUE & high_crosses_stop == TRUE &  Low_Point_Earlier== TRUE ~ 1,

          low_crosses_profit == FALSE & high_crosses_stop == FALSE & start_to_Close < 0 ~ 1,
          low_crosses_profit == FALSE & high_crosses_stop == FALSE & start_to_Close > 0 ~ 0
        )
    ) %>%
    mutate(
      trade_category =
        case_when(
          high_crosses_stop == TRUE & low_crosses_profit == FALSE  ~ "TRUE LOSS",
          low_crosses_profit == TRUE & high_crosses_stop == FALSE ~ "TRUE WIN",

          low_crosses_profit == TRUE & high_crosses_stop == TRUE &  High_Point_Earlier== TRUE ~ "TRUE LOSS",
          low_crosses_profit == TRUE & high_crosses_stop == TRUE &  Low_Point_Earlier== TRUE ~ "TRUE WIN",

          low_crosses_profit == FALSE & high_crosses_stop == FALSE & start_to_Close < 0 ~ "WEAK WIN",
          low_crosses_profit == FALSE & high_crosses_stop == FALSE &start_to_Close > 0 ~ "WEAK LOSS"
        )
    ) %>%
    mutate(
      profit_loss_value =
        case_when(
          high_crosses_stop == TRUE & low_crosses_profit == FALSE ~ -1*stop_value,
          low_crosses_profit == TRUE & high_crosses_stop ~ profit_value,

          low_crosses_profit == TRUE & high_crosses_stop == TRUE &  High_Point_Earlier== TRUE ~ -1*stop_value,
          low_crosses_profit == TRUE & high_crosses_stop == TRUE &  Low_Point_Earlier== TRUE ~ profit_value,

          low_crosses_profit == FALSE & high_crosses_stop == FALSE & start_to_Close < 0 ~ start_to_Close,
          low_crosses_profit == FALSE & high_crosses_stop == FALSE &start_to_Close > 0 ~ start_to_Close
        )
    ) %>%
    filter(!is.na(!!as.name(trade_col)),
           !!as.name(trade_col) == "Short")

  short_analysis_summary <-
    short_analysis %>%
    filter(!is.na(!!as.name(trade_col)), !!as.name(trade_col) == "Short" ) %>%
    group_by(trade_category, across(matches(additional_grouping_vars)) ) %>%
    summarise(
      Trades = n(),
      profit_loss = round(sum(profit_loss_value, na.rm = T)),
      average_stop = median(stop_value, na.rm = T),
      averaeg_profit = median(profit_value, na.rm = T)
    ) %>%
    ungroup() %>%
    group_by(Asset) %>%
    mutate(
      total_trades = sum(Trades, na.rm = T),
      Perc = Trades/total_trades,
      profit_factor = profit_factor,
      stop_factor = stop_factor,
      trade_direction = "Short"
    )

  if(return_summary) {
    return(list(long_analysis_summary, short_analysis_summary))
  }

  if(return_summary == FALSE) {
    return(list(long_analysis, short_analysis))
  }

}

generic_trade_finder_trailing <- function(
    tagged_trades = trade_with_daily_data,
    asset_data_daily_raw = asset_data_daily_raw,
    stop_factor = 3,
    profit_factor =3,
    trade_col = "trade_col",
    date_col = "Date",
    start_price_col = "Price",
    mean_values_by_asset =
      wrangle_asset_data(
        asset_data_daily_raw = asset_data_daily_raw,
        summarise_means = TRUE
      ),
    trail_points = c(0.5, 0.75, 1.25, 1.5, 1.75),
    trail_stops = c(0.05, 0.4, 0.75, 1.25, 1.5)
  ) {

  long_data_with_price<-
    asset_data_daily_raw %>%
    left_join(
      tagged_trades %>%
      dplyr::distinct(Asset, !!as.name(date_col), !!as.name(trade_col))
    ) %>%
    dplyr::group_by(Asset) %>%
    arrange(
      !!as.name(date_col)
    )

  distinct_assets_trades <-
    long_data_with_price %>%
    filter(!!as.name(trade_col) == "Long") %>%
    distinct(Asset, Date, trade_col)

  trade_directions_speed <- distinct_assets_trades$trade_col
  trade_asset_speed <- distinct_assets_trades$Asset
  trade_date_speed <- distinct_assets_trades$Date

  distinct_assets <-
    long_data_with_price %>%
    filter(!!as.name(trade_col) == "Long") %>%
    distinct(Asset) %>%
    pull(Asset)

  stops_profs_asset <- mean_values_by_asset %>%
    mutate(
      stop_value = mean_daily + stop_factor*sd_daily,
      profit_value = mean_daily + profit_factor*sd_daily
    )

  accumulating_list <- list()
  c <- 0

  for (i in 1:length(distinct_assets) ) {

    asset_data_loop <- long_data_with_price %>%
      filter(Asset == distinct_assets[i]) %>%
      arrange(!!as.name(date_col))

    distinct_trade_dates <- asset_data_loop %>%
      filter(!!as.name(trade_col) == "Long") %>%
      filter(!is.na(!!as.name(trade_col))) %>%
      pull(!!as.name(date_col))

    price_data_loop <- asset_data_loop %>% pull(Price)
    date_data_loop <- asset_data_loop %>% pull(!!as.name(date_col))
    high_loop <- asset_data_loop %>% pull(High)
    Low_loop <- asset_data_loop %>% pull(Low)

    stop_value <- stops_profs_asset  %>%
      filter(Asset == distinct_assets[i]) %>%
      pull(stop_value) %>% as.numeric()
    profit_value <- stops_profs_asset  %>%
      filter(Asset == distinct_assets[i]) %>%
      pull(profit_value) %>% as.numeric()

    if(length(distinct_trade_dates) > 0) {

      trade_returns <- numeric(length(distinct_trade_dates))
      trade_start_prices <- numeric(length(distinct_trade_dates))
      trade_end_prices <- numeric(length(distinct_trade_dates))
      starting_stop_price <- numeric(length(distinct_trade_dates))
      trail_price_1 <- numeric(length(distinct_trade_dates))
      trail_price_2 <- numeric(length(distinct_trade_dates))
      trail_price_3 <- numeric(length(distinct_trade_dates))
      trail_price_4 <- numeric(length(distinct_trade_dates))
      trail_price_5 <- numeric(length(distinct_trade_dates))
      ending_date_trade <- numeric(length(distinct_trade_dates))

      for (j in 1:length(distinct_trade_dates)) {

        starting_index <- which(date_data_loop == distinct_trade_dates[j])
        dates_for_tracking <- distinct_trade_dates[starting_index:length(distinct_trade_dates)]
        starting_price <- price_data_loop[starting_index]
        stop_price <- starting_price - stop_value
        profit_price <- starting_price + profit_value

        highs_after <- high_loop[(starting_index + 1):length(high_loop)]
        lows_after <- Low_loop[(starting_index+ 1):length(Low_loop)]
        price_after <- price_data_loop[(starting_index + 1):length(price_data_loop)]

        stop_point <- which(lows_after <= stop_price )[1]
        stop_point <- ifelse(is.na(stop_point), 10000,stop_point)
        final_profit_point <- which(highs_after >= profit_price )[1]
        final_profit_point <- ifelse(is.na(final_profit_point), 10000,final_profit_point)

        if(all(trail_stops == 0)) {

          return_value_trade <-
            ifelse(stop_point <= final_profit_point, -1*stop_value, profit_value)
          where_were_you_stopped <-
            ifelse(stop_point <= final_profit_point,
                   starting_price - stop_value,
                   profit_value + starting_price)
          trade_end_date <-
            ifelse(stop_point <= final_profit_point,
                   dates_for_tracking[stop_point],
                   dates_for_tracking[final_profit_point])

          trade_returns[j] <- return_value_trade
          trade_start_prices[j] <- starting_price
          trade_end_prices[j] <- where_were_you_stopped
          starting_stop_price[j] <- stop_price
          trail_price_1[j] <- stop_price
          trail_price_2[j] <- stop_price
          trail_price_3[j] <- stop_price
          trail_price_4[j] <- stop_price
          trail_price_5[j] <- stop_price
          ending_date_trade[j] <- trade_end_date


        } else {

          trail_point1 <- which(highs_after >= (starting_price + trail_points[1]*profit_value) )[1]
          trail_point2 <- which(highs_after >= (starting_price + trail_points[2]*profit_value) )[1]
          trail_point3 <- which(highs_after >= (starting_price + trail_points[3]*profit_value) )[1]
          trail_point4 <- which(highs_after >= (starting_price + trail_points[4]*profit_value) )[1]
          trail_point5 <- which(highs_after >= (starting_price + trail_points[5]*profit_value) )[1]

          if(!is.na(trail_point1)) {
            trail_stop1 <- which(lows_after <= (starting_price - (stop_value - trail_stops[1]*profit_value)) ) %>%
              keep(~ .x > trail_point1) %>% as.vector() %>% pluck(1)
            trail_stop1 <- ifelse(is.null(trail_stop1), 10000, trail_stop1)
          } else {trail_stop1 = NA}

          if(!is.na(trail_point2)) {
            trail_stop2 <- which(lows_after <= (starting_price - (stop_value - trail_stops[2]*profit_value)) ) %>%
              keep(~ .x > trail_point2) %>% as.vector() %>% pluck(1)
            trail_stop2 <- ifelse(is.null(trail_stop2), 10000, trail_stop2)
          } else {
            trail_stop2 <- NA
          }

          if(!is.na(trail_point3)) {
            trail_stop3 <- which(lows_after <= (starting_price - (stop_value - trail_stops[3]*profit_value)) ) %>%
              keep(~ .x > trail_point3) %>% as.vector() %>% pluck(1)
            trail_stop3 <- ifelse(is.null(trail_stop3), 10000, trail_stop3)
          } else {
            trail_stop3 <- NA
          }

          if(!is.na(trail_point4)) {
            trail_stop4 <- which(lows_after <= (starting_price - (stop_value - trail_stops[4]*profit_value)) ) %>%
              keep(~ .x > trail_point4) %>% as.vector() %>% pluck(1)
            trail_stop4 <- ifelse(is.null(trail_stop4), 10000, trail_stop4)
          } else {
            trail_stop4 = NA
          }

          if(!is.na(trail_point5)) {
            trail_point5 <- which(lows_after <= (starting_price - (stop_value - trail_stops[5]*profit_value)) ) %>%
              keep(~ .x > trail_point5) %>% as.vector() %>% pluck(1)
            trail_point5 <- ifelse(is.null(trail_point5), 10000, trail_point5)
          } else {
            trail_stop5 = NA
          }

          where_were_you_stopped <-
            case_when(
              stop_point<= trail_stop1 ~ stop_price,
              stop_point> trail_stop1 &
                (trail_stop1 <= trail_point2|is.na(trail_point2)) ~ (starting_price - (stop_value - trail_stops[1]*profit_value)),
              stop_point> trail_stop1 &
                trail_stop1 > trail_point2 &
                (trail_stop2 <= trail_point3|is.na(trail_point3)) ~ (starting_price - (stop_value - trail_stops[2]*profit_value)),
              stop_point> trail_stop1 &
                trail_stop1 > trail_point2 &
                trail_stop2 > trail_point3 &
                (trail_stop3 <= trail_point4|is.na(trail_point4)) ~ (starting_price - (stop_value - trail_stops[3]*profit_value)),
              stop_point> trail_stop1 &
                trail_stop1 > trail_point2 &
                trail_stop2 > trail_point3 &
                trail_stop3 > trail_point4 &
                (trail_stop4 <= trail_point5|is.na(trail_point5))  ~ (starting_price - (stop_value - trail_stops[4]*profit_value)),
              stop_point> trail_stop1 &
                trail_stop1 > trail_point2 &
                trail_stop2 > trail_point3 &
                trail_stop3 > trail_point4 &
                trail_stop4 > trail_point5 &
                (trail_stop4 <= final_profit_point|is.na(final_profit_point)) ~ (starting_price - (stop_value - trail_stops[5]*profit_value)),
              stop_point> trail_stop1 &
                trail_stop1 > trail_point2 &
                trail_stop2 > trail_point3 &
                trail_stop3 > trail_point4 &
                trail_stop4 > trail_point5 &
                trail_stop4 > final_profit_point ~ starting_price + (trail_points[5] + 0.25)*profit_value
            )

          trade_returns[j] <- where_were_you_stopped - starting_price
          trade_start_prices[j] <- starting_price
          trade_end_prices[j] <- where_were_you_stopped
          starting_stop_price[j] <- stop_price
          trail_price_1[j] <- (starting_price - (stop_value - trail_stops[1]*profit_value))
          trail_price_2[j] <- (starting_price - (stop_value - trail_stops[2]*profit_value))
          trail_price_3[j] <- (starting_price - (stop_value - trail_stops[3]*profit_value))
          trail_price_4[j] <- (starting_price - (stop_value - trail_stops[4]*profit_value))
          trail_price_5[j] <- (starting_price - (stop_value - trail_stops[5]*profit_value))
        }


      }

      c <- c + 1
      accumulating_list[[c]] <-
        tibble(
          trade_returns = trade_returns,
          trade_start_prices = trade_start_prices,
          trade_end_prices = trade_end_prices,
          asset = distinct_assets[i],
          dates = distinct_trade_dates,
          ending_date_trade = ending_date_trade,
          starting_stop_value = stop_value,
          starting_profit_value = profit_value,
          trail_price_1 = trail_price_1,
          trail_price_2 = trail_price_2,
          trail_price_3 = trail_price_3,
          trail_price_4 = trail_price_4,
          trail_price_5 = trail_price_5
        )

    }

  }

  all_trade_data <- accumulating_list %>%
    map_dfr(
      ~ .x %>%
        mutate(
          trade_col = "Long"
        )
    )

  return(all_trade_data)

}


generic_trade_finder_loop <- function(
    tagged_trades = trading_data_vol %>% filter(trade_col == "Short"),
    asset_data_daily_raw = asset_data_daily_raw,
    stop_factor = 1,
    profit_factor =1,
    trade_col = "trade_col",
    date_col = "Date",
    start_price_col = "Price",
    mean_values_by_asset =
      wrangle_asset_data(
        asset_data_daily_raw = asset_data_daily_raw,
        summarise_means = TRUE
      )
) {

  long_data_with_price<-
    asset_data_daily_raw %>%
    left_join(
      tagged_trades %>%
        dplyr::distinct(Asset, !!as.name(date_col), !!as.name(trade_col))
    ) %>%
    dplyr::group_by(Asset) %>%
    arrange(
      !!as.name(date_col)
    )


  distinct_assets_trades <-
    long_data_with_price %>%
    filter(!is.na(!!as.name(trade_col))) %>%
    distinct(Asset, Date, trade_col)

  trade_directions_speed <- distinct_assets_trades$trade_col
  trade_asset_speed <- distinct_assets_trades$Asset
  trade_date_speed <- distinct_assets_trades$Date

  distinct_assets <-
    long_data_with_price %>%
    filter(!is.na(!!as.name(trade_col))) %>%
    distinct(Asset) %>%
    pull(Asset)

  stops_profs_asset <- mean_values_by_asset %>%
    mutate(
      stop_value = mean_daily + stop_factor*sd_daily,
      profit_value = mean_daily + profit_factor*sd_daily
    )

  accumulating_list <- list()
  c <- 0

  for (i in 1:length(distinct_assets) ) {

    asset_data_loop <- long_data_with_price %>%
      filter(Asset == distinct_assets[i]) %>%
      arrange(!!as.name(date_col))

    distinct_trade_dates <- asset_data_loop %>%
      filter(!is.na(!!as.name(trade_col))) %>%
      pull(!!as.name(date_col))

    distinct_trade_direction <- asset_data_loop %>%
      filter(!is.na(!!as.name(trade_col))) %>%
      pull(!!as.name(trade_col))

    price_data_loop <- asset_data_loop %>% pull(Price)
    date_data_loop <- asset_data_loop %>% pull(!!as.name(date_col))
    high_loop <- asset_data_loop %>% pull(High)
    Low_loop <- asset_data_loop %>% pull(Low)

    stop_value <- stops_profs_asset  %>%
      filter(Asset == distinct_assets[i]) %>%
      pull(stop_value) %>% as.numeric()
    profit_value <- stops_profs_asset  %>%
      filter(Asset == distinct_assets[i]) %>%
      pull(profit_value) %>% as.numeric()

    if(length(distinct_trade_dates) > 0) {

      trade_returns <- numeric(length(distinct_trade_dates))
      trade_start_prices <- numeric(length(distinct_trade_dates))
      trade_end_prices <- numeric(length(distinct_trade_dates))
      starting_stop_price <- numeric(length(distinct_trade_dates))
      ending_date_trade <- numeric(length(distinct_trade_dates))

      for (j in 1:length(distinct_trade_dates)) {

        trade_direction <-distinct_trade_direction[j]
        starting_index <- which(date_data_loop == distinct_trade_dates[j])[1]
        dates_for_tracking <- date_data_loop[seq(starting_index,length(date_data_loop))]
        starting_price <- price_data_loop[starting_index]

        highs_after <- high_loop[(starting_index + 1):length(high_loop)]
        lows_after <- Low_loop[(starting_index+ 1):length(Low_loop)]
        price_after <- price_data_loop[(starting_index + 1):length(price_data_loop)]

        if(trade_direction == "Long") {
          stop_price <- starting_price - stop_value
          profit_price <- starting_price + profit_value

          stop_point <- which(lows_after <= stop_price )[1]
          stop_point <- ifelse(is.na(stop_point)|length(stop_point) < 1, 10000,stop_point)
          final_profit_point <- which(highs_after >= profit_price )[1]
          final_profit_point <- ifelse(is.na(final_profit_point)|length(final_profit_point) < 1, 10000,final_profit_point)
        }

        if(trade_direction == "Short") {
          stop_price <- starting_price + stop_value
          profit_price <- starting_price - profit_value

          stop_point <- which(highs_after >= stop_price )[1]
          stop_point <- ifelse(is.na(stop_point)|length(stop_point) < 1, 10000,stop_point)
          final_profit_point <- which(lows_after <= profit_price )[1]
          final_profit_point <- ifelse(is.na(final_profit_point)|length(final_profit_point) < 1, 10000,final_profit_point)
        }

        return_value_trade <-
          ifelse(stop_point <= final_profit_point, -1*stop_value, profit_value)

        if(trade_direction == "Long") {
          where_were_you_stopped <-
            ifelse(stop_point <= final_profit_point,
                   starting_price - stop_value,
                   profit_value + starting_price)
        }

        if(trade_direction == "Short") {
          where_were_you_stopped <-
            ifelse(stop_point <= final_profit_point,
                   starting_price + stop_value,
                   profit_value - starting_price)
        }

        trade_end_date <-
          ifelse(stop_point <= final_profit_point,
                 dates_for_tracking[stop_point],
                 dates_for_tracking[final_profit_point])

        trade_returns[j] <- return_value_trade
        trade_start_prices[j] <- starting_price
        trade_end_prices[j] <- where_were_you_stopped
        starting_stop_price[j] <- stop_price
        ending_date_trade[j] <- trade_end_date

      }

      c <- c + 1
      accumulating_list[[c]] <-
        tibble(
          trade_returns = trade_returns,
          trade_start_prices = trade_start_prices,
          trade_end_prices = trade_end_prices,
          asset = distinct_assets[i] %>% as.character(),
          dates = distinct_trade_dates,
          ending_date_trade = ending_date_trade,
          starting_stop_value = stop_value,
          starting_profit_value = profit_value,
          trade_col = distinct_trade_direction
        )

    }

  }

  all_trade_data <- accumulating_list %>%
    map_dfr(bind_rows)

  return(all_trade_data)

}

analyse_trailing_trades <- function(
  trade_data = tagged_trades_markov_1,
  asset_data_daily_raw = asset_data_daily_raw,
  asset_infor,
  risk_dollar_value = 20,
  currency_conversion = currency_conversion
  ) {
  # trade_data <- trade_data %>%
  #   rename(Asset = asset)

  analysis <- trade_data %>%
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
    mutate(
      trade_returns_pip = trade_returns/(10^as.numeric(pipLocation))
    )


  return_time_series <-
    analysis %>%
    dplyr::select(dates, Asset, trade_returns_pip, estimated_margin, trade_return_dollars_AUD) %>%
    group_by(dates) %>%
    summarise(
      Total_Margain = sum(estimated_margin, na.rm = T),
      Total_Return_Pip = sum(trade_returns_pip, na.rm = T),
      trade_return_dollars_AUD = sum(trade_return_dollars_AUD, na.rm = T)
    ) %>%
    mutate(
      Total_Return_Pip = ifelse(is.infinite(Total_Return_Pip), 0, Total_Return_Pip),
      Total_Margain = ifelse(is.infinite(Total_Margain), 0, Total_Margain),
      trade_return_dollars_AUD = ifelse(is.infinite(trade_return_dollars_AUD), 0, trade_return_dollars_AUD)
    ) %>%
    arrange(dates) %>%
    mutate(
      cumulative_return_pip = cumsum(Total_Return_Pip),
      cumulative_return_aud = cumsum(trade_return_dollars_AUD)
    ) %>%
    ungroup()

  return(
    list(
      "analysis" = analysis,
      "return_time_series" = return_time_series
    )
  )

}


get_stops_profs_volume_trades <- function(
                                       tagged_trades = tagged_trades,
                                       mean_values_by_asset = mean_values_by_asset,
                                       trade_col = "trade_col",
                                       currency_conversion = currency_conversion,
                                       risk_dollar_value = risk_dollar_value,
                                       stop_factor = stop_factor,
                                       profit_factor =profit_factor,
                                       asset_col = "Asset",
                                       stop_col = "stop_value",
                                       profit_col = "profit_value",
                                       price_col = "Price",
                                       trade_return_col = "trade_returns") {

  stops_profs_asset <- mean_values_by_asset %>%
    mutate(
      stop_value = mean_daily + stop_factor*sd_daily,
      profit_value = mean_daily + profit_factor*sd_daily
    )

  returned <-
    tagged_trades %>%
    left_join(stops_profs_asset) %>%
    mutate(trade_returns = 0) %>%
    convert_stop_profit_AUD(
      asset_infor = asset_infor,
      asset_col = asset_col,
      stop_col = stop_col,
      profit_col = profit_col,
      price_col = price_col,
      risk_dollar_value = risk_dollar_value,
      returns_present = TRUE,
      trade_return_col = trade_return_col,
      currency_conversion = currency_conversion
    ) %>%
    mutate(
      volume_required =
        ifelse(!!as.name(trade_col) == "Short", -1*volume_required, volume_required)
    )

  return(returned)

}

#' get_random_results_trades
#'
#' @param raw_asset_data_ask
#' @param raw_asset_data_bid
#' @param stop_factor
#' @param profit_factor
#' @param risk_dollar_value
#' @param analysis_syms
#' @param trade_samples
#'
#' @return
#' @export
#'
#' @examples
get_random_results_trades <-
  function(
    raw_asset_data_ask = major_indices_log_cumulative,
    raw_asset_data_bid = major_indices_log_cumulative_bid,
    stop_factor = 3,
    profit_factor = 5,
    risk_dollar_value = 10,
    analysis_syms = c("AUD_USD", "SPX500_USD", "EU50_EUR", "US2000_USD"),
    trade_samples = 10000
  ) {

    #--------------------------------------Random Results
    random_trades_long <-
      raw_asset_data_ask %>%
      filter(Asset %in% analysis_syms) %>%
      group_by(Asset) %>%
      slice_sample(n = trade_samples) %>%
      ungroup() %>%
      mutate(
        trade_col = "Long"
      )

    random_analysis_long <-
      run_pairs_analysis(
        tagged_trades = random_trades_long %>% filter(trade_col == "Long"),
        stop_factor = stop_factor,
        profit_factor = profit_factor,
        raw_asset_data = raw_asset_data_ask,
        risk_dollar_value = risk_dollar_value
      )

    random_results_long_asset <-
      random_analysis_long[[2]]

    random_trades_short <-
      raw_asset_data_bid %>%
      filter(Asset %in% analysis_syms) %>%
      group_by(Asset) %>%
      slice_sample(n = trade_samples) %>%
      ungroup() %>%
      mutate(
        trade_col = "Short"
      )

    random_analysis_short <-
      run_pairs_analysis(
        tagged_trades = random_trades_short %>% filter(trade_col == "Short"),
        stop_factor = stop_factor,
        profit_factor = profit_factor,
        raw_asset_data = raw_asset_data_bid,
        risk_dollar_value = risk_dollar_value
      )

    random_results_short_asset <-
      random_analysis_short[[2]]

    return(list(random_results_long_asset, random_results_short_asset))

  }

#' get_random_samples_MLE_beta
#'
#' @param random_results_db_location
#' @param stop_factor
#' @param profit_factor
#' @param analysis_syms
#' @param time_frame
#' @param return_summary
#'
#' @return
#' @export
#'
#' @examples
get_random_samples_MLE_beta <-
  function(
    random_results_db_location = "C:/Users/Nikhil Chandra/Documents/trade_data/random_results.db",
    stop_factor = 8,
    profit_factor = 16,
    analysis_syms = c("AUD_USD", "NZD_USD", "XCU_USD"),
    time_frame = "H1",
    return_summary = FALSE
  ) {

    db_con <- connect_db(random_results_db_location)
    random_samples_data <-
      DBI::dbGetQuery(conn = db_con,
                      statement = "SELECT * FROM random_results")

    DBI::dbDisconnect(db_con)

    perc_random_samples <-
      random_samples_data %>%
      filter(Asset %in% analysis_syms) %>%
      filter(stop_factor == stop_factor, profit_factor == profit_factor,
             time_frame == time_frame)

    perc_random_samples_short <-
      perc_random_samples %>% filter(trade_direction == "Short")

    perc_random_samples_long <-
      perc_random_samples %>% filter(trade_direction == "Long")

    assets_all <- analysis_syms
    accumulator <- list()
    accumulator2 <- list()

    for (i in 1:length(assets_all)) {

      temp_long <- perc_random_samples_long %>% filter(Asset == assets_all[i])
      temp_short <- perc_random_samples_short %>% filter(Asset == assets_all[i])

      beta_short_dist <- fitdistrplus::fitdist(data = temp_short$Perc, distr = "beta")
      beta_samples_short <-
        rbeta(n = 500000,
              shape1 = beta_short_dist$estimate[1] %>% as.numeric(),
              shape2 = beta_short_dist$estimate[2] %>% as.numeric())

      beta_long_dist <- fitdistrplus::fitdist(data = temp_long$Perc, distr = "beta")
      beta_samples_long <-
        rbeta(n = 500000,
              shape1 = beta_long_dist$estimate[1] %>% as.numeric(),
              shape2 = beta_long_dist$estimate[2] %>% as.numeric())

      accumulator[[i]] <-
        tibble(
          Perc = beta_samples_long,
          Asset = assets_all[i],
          time_frame =time_frame,
          profit_factor = profit_factor,
          stop_factor = stop_factor,
          trade_direction = "Long"
        )  %>%
        bind_rows(
          tibble(
            Perc = beta_samples_short,
            Asset = assets_all[i],
            time_frame =time_frame,
            profit_factor = profit_factor,
            stop_factor = stop_factor,
            trade_direction = "Short"
          )
        ) %>%
        mutate(
          risk_weighted_return = (profit_factor/stop_factor)*Perc - (1 - Perc)
        )

      accumulator2[[i]] <-
        tibble(
          shape1 = beta_long_dist$estimate[1] %>% as.numeric(),
          shape2 = beta_long_dist$estimate[2] %>% as.numeric(),
          Asset = assets_all[i],
          time_frame =time_frame,
          profit_factor = profit_factor,
          stop_factor = stop_factor,
          trade_direction = "Long"
        )  %>%
        bind_rows(
          tibble(
            shape1 = beta_long_dist$estimate[1] %>% as.numeric(),
            shape2 = beta_long_dist$estimate[2] %>% as.numeric(),
            Asset = assets_all[i],
            time_frame =time_frame,
            profit_factor = profit_factor,
            stop_factor = stop_factor,
            trade_direction = "Short"
          )
        )

    }

    all_results <-
      accumulator %>%
      map_dfr(bind_rows)

    if(return_summary == TRUE) {
      all_results <-
        all_results %>%
        group_by(trade_direction, profit_factor, stop_factor, time_frame, Asset) %>%
        summarise(
          mean_risk = mean(risk_weighted_return, na.rm = T),
          sd_risk = sd(risk_weighted_return, na.rm = T),
          mean_perc = mean(Perc, na.rm = T),
          sd_perc = sd(Perc, na.rm = T)
        )
    }

    return(all_results)

  }

#' upload_trade_actuals_to_db
#'
#' @param asset_data_raw_list
#' @param date_filter
#' @param stop_factor
#' @param profit_factor
#' @param risk_dollar_value
#' @param append_or_write
#' @param full_ts_trade_db_location
#'
#' @return
#' @export
#'
#' @examples
upload_trade_actuals_to_db <-
  function(
    asset_data_raw_list,
    date_filter = "2008-01-01",
    stop_factor = 3,
    profit_factor = 6,
    risk_dollar_value = 10,
    append_or_write = "append",
    full_ts_trade_db_location = "C:/Users/Nikhil Chandra/Documents/trade_data/full_ts_trades_mapped_AUD_USD.db"
  ) {

    Long_Data_tagged <-
      asset_data_raw_list[[1]] %>%
      filter(Date >= date_filter) %>%
      mutate(
        trade_col = "Long"
      )

    Long_trade_data <-
      run_pairs_analysis(
        tagged_trades = Long_Data_tagged,
        stop_factor = stop_factor,
        profit_factor = profit_factor,
        raw_asset_data = asset_data_raw_list[[1]],
        risk_dollar_value = risk_dollar_value,
        return_trade_ts = TRUE
      )

    Short_Data_tagged <-
      asset_data_raw_list[[2]] %>%
      filter(Date >= date_filter) %>%
      mutate(
        trade_col = "Short"
      )

    Short_trade_data <-
      run_pairs_analysis(
        tagged_trades = Short_Data_tagged,
        stop_factor = stop_factor,
        profit_factor = profit_factor,
        raw_asset_data = asset_data_raw_list[[2]],
        risk_dollar_value = risk_dollar_value,
        return_trade_ts = TRUE
      )

    full_data_for_upload <-
      Long_trade_data %>%
      mutate(
        stop_factor = stop_factor,
        profit_factor = profit_factor
      ) %>%
      bind_rows(
        Short_trade_data %>%
          mutate(
            stop_factor = stop_factor,
            profit_factor = profit_factor
          )
      )

    if(append_or_write == "append") {
      full_ts_trade_db_con <- connect_db(path = full_ts_trade_db_location)
      append_table_sql_lite(.data = full_data_for_upload,
                            table_name = "full_ts_trades_mapped",
                            conn = full_ts_trade_db_con)
      DBI::dbDisconnect(full_ts_trade_db_con)
      rm(full_ts_trade_db_con)
      gc()

      rm(full_data_for_upload)
      gc()
    }

    if(append_or_write == "write") {
      full_ts_trade_db_con <- connect_db(path = full_ts_trade_db_location)
      write_table_sql_lite(.data = full_data_for_upload,
                           table_name = "full_ts_trades_mapped",
                           conn = full_ts_trade_db_con,
                           overwrite_true = TRUE)
      DBI::dbDisconnect(full_ts_trade_db_con)
      rm(full_ts_trade_db_con)
      gc()

      rm(full_data_for_upload)
      gc()
    }

  }


#' get_X_period_ahead_return
#'
#' @param tagged_trades
#' @param ask_price
#' @param bid_price
#' @param stop_factor
#' @param profit_factor
#' @param trade_col
#' @param date_col
#' @param start_price_col
#' @param mean_values_by_asset
#' @param periods_ahead
#' @param trade_direction
#'
#' @return
#' @export
#'
#' @examples
get_X_period_ahead_return <-
  function(
    tagged_trades = SPX_US2000_XAG_trades ,
    ask_price = SPX_US2000_XAG_ask,
    bid_price = SPX_US2000_XAG_bid,
    stop_factor = 0.75,
    profit_factor =3,
    trade_col = "trade_col",
    date_col = "Date",
    start_price_col = "Price",
    mean_values_by_asset = mean_values_by_asset_for_loop,
    periods_ahead = 24,
    trade_direction = "Long"
  ) {


    trade_dates_by_asset <-
      tagged_trades %>%
      filter(!is.na(trade_col)) %>%
      distinct(Date, Asset, trade_col) %>%
      filter(trade_col == trade_direction)

    loop_determined_trades <-
      generic_trade_finder_loop(
        tagged_trades = tagged_trades %>% filter(trade_col == trade_direction),
        asset_data_daily_raw = ask_price,
        stop_factor = stop_factor,
        profit_factor =profit_factor,
        trade_col = trade_col,
        date_col = date_col,
        start_price_col = start_price_col,
        mean_values_by_asset =mean_values_by_asset
      )

    trades_where_did_we_stop <-
      loop_determined_trades %>%
      dplyr::select(Asset = asset,
                    Date = dates,
                    End_Date = ending_date_trade,
                    trade_returns_stop_prof = trade_returns,
                    trade_col,
                    trade_start_prices,
                    starting_profit_value,
                    starting_stop_value,
                    trade_end_prices_stop_prof = trade_end_prices) %>%
      mutate(
        End_Date = as_datetime(End_Date, tz = "Australia/Sydney"),
        Time_Periods = (as.numeric(End_Date - Date)/60)/60
      )

    analysis_data <-
      ask_price %>%
      rename(Ask_Price = Price) %>%
      left_join(bid_price %>% dplyr::select(Date, Asset, Bid_Price = Price)) %>%
      left_join(trade_dates_by_asset) %>%
      dplyr::select(-`Vol.`) %>%
      group_by(Asset) %>%
      arrange(Date, .by_group = TRUE) %>%
      group_by(Asset) %>%
      mutate(
        Price_X_periods_Bid = lead(Bid_Price, periods_ahead),
        Price_X_periods_Ask = lead(Ask_Price, periods_ahead),
        End_Date_X_Periods = lead(Date, periods_ahead)
      ) %>%
      left_join(trades_where_did_we_stop) %>%
      mutate(
        trade_return =
          case_when(
            Time_Periods <= periods_ahead ~ trade_returns_stop_prof,
            Time_Periods > periods_ahead & trade_direction == "Long" ~ Price_X_periods_Bid - Ask_Price,
            Time_Periods > periods_ahead & trade_direction == "Short" ~ Bid_Price - Price_X_periods_Ask
          ),

        trade_end_prices =
          case_when(
            Time_Periods <= periods_ahead ~ abs(trade_end_prices_stop_prof),
            Time_Periods > periods_ahead & trade_direction == "Long" ~ abs(Price_X_periods_Bid),
            Time_Periods > periods_ahead & trade_direction == "Short" ~ abs(Price_X_periods_Ask)
          )
      ) %>%
      dplyr::select(-trade_end_prices_stop_prof)

    return(analysis_data)

  }


#' upload_trade_actuals_to_db
#'
#' @param asset_data_raw_list
#' @param date_filter
#' @param stop_factor
#' @param profit_factor
#' @param risk_dollar_value
#' @param append_or_write
#' @param full_ts_trade_db_location
#'
#' @return
#' @export
#'
#' @examples
upload_trade_actuals_period_version_to_db <-
  function(
    asset_data_raw_list,
    date_filter = "2008-01-01",
    stop_factor = 3,
    profit_factor = 6,
    risk_dollar_value = 5,
    periods_ahead = 24,
    append_or_write = "append",
    full_ts_trade_db_location = "C:/Users/Nikhil Chandra/Documents/trade_data/full_ts_trades_mapped_AUD_USD.db",
    currency_conversion = currency_conversion,
    asset_infor = asset_infor
  ) {

    Long_Data_tagged <-
      asset_data_raw_list[[1]] %>%
      filter(Date >= date_filter) %>%
      mutate(
        trade_col = "Long"
      )

    mean_values_by_asset_for_loop <-
      wrangle_asset_data(asset_data_raw_list[[1]], summarise_means = TRUE)

    Long_trade_data <-
      get_X_period_ahead_return(
        tagged_trades = Long_Data_tagged ,
        ask_price = asset_data_raw_list[[1]],
        bid_price = asset_data_raw_list[[2]],
        stop_factor = stop_factor,
        profit_factor =profit_factor,
        trade_col = "trade_col",
        date_col = "Date",
        start_price_col = "Price",
        mean_values_by_asset = mean_values_by_asset_for_loop,
        periods_ahead = periods_ahead,
        trade_direction = "Long"
      )

    data_for_dollar_values_long <-
      convert_stop_profit_AUD(trade_data = Long_trade_data,
                              asset_infor = asset_infor,
                              currency_conversion = currency_conversion,
                              asset_col = "Asset",
                              stop_col = "starting_stop_value",
                              profit_col = "starting_profit_value",
                              price_col = "trade_start_prices",
                              risk_dollar_value = risk_dollar_value,
                                        returns_present = FALSE,
                                        trade_return_col = "trade_return") %>%
      mutate(
        trade_return_dollar_aud =
          case_when(
            str_detect(Asset,"SEK|NOK|ZAR|MXN|CNH") ~ ((risk_dollar_value/trade_return)/adjusted_conversion)*trade_return,
            TRUE ~ volume_adj*trade_return
          )
      )

    Short_Data_tagged <-
      asset_data_raw_list[[2]] %>%
      filter(Date >= date_filter) %>%
      mutate(
        trade_col = "Short"
      )

    Short_trade_data <-
      get_X_period_ahead_return(
        tagged_trades = Short_Data_tagged ,
        ask_price = asset_data_raw_list[[1]],
        bid_price = asset_data_raw_list[[2]],
        stop_factor = stop_factor,
        profit_factor =profit_factor,
        trade_col = "trade_col",
        date_col = "Date",
        start_price_col = "Price",
        mean_values_by_asset = mean_values_by_asset_for_loop,
        periods_ahead = periods_ahead,
        trade_direction = "Short"
      )


    data_for_dollar_values_short <-
      convert_stop_profit_AUD(trade_data = Short_trade_data,
                              asset_infor = asset_infor,
                              currency_conversion = currency_conversion,
                              asset_col = "Asset",
                              stop_col = "starting_stop_value",
                              profit_col = "starting_profit_value",
                              price_col = "trade_start_prices",
                              risk_dollar_value = risk_dollar_value,
                              returns_present = FALSE,
                              trade_return_col = "trade_return") %>%
      mutate(
        trade_return_dollar_aud =
          case_when(
            str_detect(Asset,"SEK|NOK|ZAR|MXN|CNH") ~ ((risk_dollar_value/trade_return)/adjusted_conversion)*trade_return,
            TRUE ~ volume_adj*trade_return
          )
      )

    full_data_for_upload <-
      data_for_dollar_values_long %>%
      mutate(
        stop_factor = stop_factor,
        profit_factor = profit_factor,
        periods_ahead = periods_ahead
      ) %>%
      bind_rows(
        data_for_dollar_values_short %>%
          mutate(
            stop_factor = stop_factor,
            profit_factor = profit_factor,
            periods_ahead = periods_ahead
          )
      )

    if(append_or_write == "append") {
      full_ts_trade_db_con <- connect_db(path = full_ts_trade_db_location)
      append_table_sql_lite(.data = full_data_for_upload,
                            table_name = "full_ts_trades_mapped",
                            conn = full_ts_trade_db_con)
      DBI::dbDisconnect(full_ts_trade_db_con)
      rm(full_ts_trade_db_con)
      gc()

      rm(full_data_for_upload)
      gc()
    }

    if(append_or_write == "write") {
      full_ts_trade_db_con <- connect_db(path = full_ts_trade_db_location)
      write_table_sql_lite(.data = full_data_for_upload,
                           table_name = "full_ts_trades_mapped",
                           conn = full_ts_trade_db_con,
                           overwrite_true = TRUE)
      DBI::dbDisconnect(full_ts_trade_db_con)
      rm(full_ts_trade_db_con)
      gc()

      rm(full_data_for_upload)
      gc()
    }

  }
