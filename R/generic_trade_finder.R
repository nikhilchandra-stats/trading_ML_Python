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

analyse_trailing_trades <- function(
  trade_data,
  asset_data_daily_raw = asset_data_daily_raw,
  asset_infor,
  risk_dollar_value = 20
  ) {

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
    )

  analysis <- trade_data %>%
    rename(Asset = asset) %>%
    left_join(asset_infor %>% rename(Asset = name)) %>%
    mutate(ending_value = str_extract(Asset, "_[A-Z][A-Z][A-Z]") %>% str_remove_all("_")) %>%
    left_join(currency_conversion, by =c("ending_value" = "not_aud_asset")) %>%
    mutate(
      trade_returns_pip =   trade_returns/(10^pipLocation),
      stop_points_pip = starting_stop_value/(10^pipLocation)
    ) %>%
    mutate(
      minimumTradeSize = abs(log10(as.numeric(minimumTradeSize))),
      marginRate = as.numeric(marginRate),
      pipLocation = as.numeric(pipLocation),
      displayPrecision = as.numeric(displayPrecision)
    ) %>%
    ungroup() %>%
    mutate(
      stop_value = round(stop_value, abs(pipLocation) ),
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


  return_time_series <-
    analysis %>%
    dplyr::select(dates, Asset, trade_returns_pip, estimated_margin) %>%
    group_by(dates) %>%
    summarise(
      Total_Margain = sum(estimated_margin, na.rm = T),
      Total_Return = sum(trade_returns_pip, na.rm = T)
    ) %>%
    mutate(
      Total_Return = ifelse(is.infinite(Total_Return), 0, Total_Return),
      Total_Margain = ifelse(is.infinite(Total_Margain), 0, Total_Margain)
    ) %>%
    arrange(dates) %>%
    mutate(
      cumulative_return = cumsum(Total_Return)
    )

  return(
    list(
      "analysis" = analysis,
      "return_time_series" = return_time_series
    )
  )

}
