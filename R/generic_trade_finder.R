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

