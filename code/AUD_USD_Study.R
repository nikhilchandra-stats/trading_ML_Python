helperfunctions35South::load_custom_functions()
one_drive_path <- helperfunctions35South::create_one_drive_path(
  path_extension = "raw data")

library(neuralnet)
raw_macro_data <- get_macro_event_data()
#----------------------------------------------------
asset_trades_data <- fs::dir_info("C:/Users/Nikhil Chandra/Documents/Asset Data/ASX 200/") %>%
  mutate(asset_name =
           str_remove(path, "C\\:\\/Users/Nikhil Chandra\\/Documents\\/Asset Data\\/ASX 200\\/") %>%
           str_remove("\\.csv") %>%
           str_remove("Historical Data")%>%
           str_remove("Stock Price") %>%
           str_remove("History")
         ) %>%
  dplyr::select(path, asset_name) %>%
  split(.$asset_name, drop = FALSE) %>%
  map(
    ~
      run_reg_for_trades(
        raw_macro_data = raw_macro_data,
        asset_data = read_csv(.x[1,1] %>% as.character()),
        asset_name = .x[1,2]%>% as.character()
      )
  )

names(asset_trades_data[[1]][[1]])

asset_trades_data_df <- asset_trades_data %>%
  map_dfr(~ .x[[1]]) %>%
  mutate(weekly_date = lubridate::floor_date(date, "week")) %>%
  group_by(weekly_date, Asset) %>%
  mutate(
    weekly_return = sum(trade, na.rm = T)
  ) %>%
  group_by(Asset) %>%
  mutate(
    mean_daily_return = mean(change_var, na.rm = T),
    sd_daily_return = sd(change_var, na.rm = T)
  ) %>%
  mutate(
    short_long =
      case_when(
        pred < 0 & pred <= (mean_daily_return - 1.5*sd_daily_return) ~ "Very Very Short",
        pred < 0 & pred <= (mean_daily_return - 0.75*sd_daily_return) ~ "Very Short",
        pred < 0  ~ "Short",
        pred > 0 & pred >= (mean_daily_return + 1.5*sd_daily_return) ~ "Very Very Long",
        pred > 0 & pred >= (mean_daily_return + 0.75*sd_daily_return) ~ "Very Long",
        pred > 0 ~ "Long"
      )
  )

asset_trades_data_df_sum <- asset_trades_data_df %>%
  group_by(Asset, short_long) %>%
  summarise(monthly_return_mean = mean(monthly_return, na.rm = T),
            monthly_return_mean = quantile(monthly_return,0.1, na.rm = T),

            monthly_return_total = sum(monthly_return, na.rm = T),
            weekly_return_average = round(mean(weekly_return, na.rm = T),4),
            weekly_return_low = round(quantile(weekly_return,0.1 ,na.rm = T),4)
            )

asset_trades_data_df_week_sum <- asset_trades_data_df %>%
  group_by(Asset, weekly_date) %>%
  slice_min(date) %>%
  group_by(Asset, short_long) %>%
  summarise(monthly_return_mean = mean(trade, na.rm = T),
            monthly_return_mean = quantile(trade,0.1, na.rm = T),

            monthly_return_total = sum(trade, na.rm = T),
            weekly_return_average = round(mean(trade, na.rm = T),4),
            weekly_return_low = round(quantile(trade,0.1 ,na.rm = T),4)
  ) %>%
  filter(!is.na(monthly_return_mean))


asset_trades_data_df <- asset_trades_data %>%
  map_dfr(~ .x[[1]]) %>%
  mutate(
    short_long =
      case_when(
        pred < 0 ~ "Short",
        pred>0 ~ "Long"
      )
  )

test_sumtheory <- aud_usd_dat %>%
  mutate(
    change_7_day = Price - lag(Price, 7),
    change_1_day = Price - lag(Price)
  ) %>%
  mutate(
    sum_7_day = slider::slide_dbl(.x = change_1_day, .f = ~ sum(.x, na.rm = T), .before = 6)
  )

#----------------------------------------------------
#----------------------------------------------------Futures
AUD_exports = get_AUS_exports()
all_vars_in_reg <- 140
futures_trades_data <- fs::dir_info("C:/Users/Nikhil Chandra/Documents/Asset Data/Futures/") %>%
  mutate(asset_name =
           str_remove(path, "C\\:\\/Users/Nikhil Chandra\\/Documents\\/Asset Data\\/Futures\\/") %>%
           str_remove("\\.csv") %>%
           str_remove("Historical Data")%>%
           str_remove("Stock Price") %>%
           str_remove("History")
  ) %>%
  dplyr::select(path, asset_name) %>%
  split(.$asset_name, drop = FALSE) %>%
  map(
    ~
      run_reg_for_trades_with_NN(
        raw_macro_data = raw_macro_data,
        asset_data = read_csv(.x[1,1] %>% as.character()),
        asset_name = .x[1,2]%>% as.character(),
        hidden_layers = c(round(all_vars_in_reg/2)
                          # round(all_vars_in_reg/2),
                          # round(all_vars_in_reg/2),
                          # round(all_vars_in_reg/2),
                          # round(all_vars_in_reg/2)
                          ),
        iterations = 400000,
        AUD_exports = AUD_exports
      )
  )

names(futures_trades_data[[1]][[1]])


futures_trades_data_df <- futures_trades_data %>%
  map_dfr(~ .x[[1]]) %>%
  dplyr::select(date, Price, Open, High, Low, change_var,
                pred, pred_lm, trade_NN, trade_LM, month_date, Asset) %>%
  mutate(weekly_date = lubridate::floor_date(date, "week")) %>%
  mutate(
    short_long =
      case_when(
        # pred < 0 & pred <= (mean_daily_return - 1.5*sd_daily_return) ~ "Very Very Short",
        # pred < 0 & pred <= (mean_daily_return - 0.75*sd_daily_return) ~ "Very Short",
        pred < 0  ~ "Short",
        # pred > 0 & pred >= (mean_daily_return + 1.5*sd_daily_return) ~ "Very Very Long",
        # pred > 0 & pred >= (mean_daily_return + 0.75*sd_daily_return) ~ "Very Long",
        pred > 0 ~ "Long"
      )
  ) %>%
  mutate(
    win_loss_NN = ifelse(trade_NN < 0, 0, 1),
    win_loss_LM = ifelse(trade_LM < 0, 0, 1)
  ) %>%
  filter(!is.na(trade_LM))

futures_trades_data_df_sum <- futures_trades_data_df %>%
  filter(str_detect(Asset, "AUD|USD|EUR|JPY")) %>%
  group_by(Asset, short_long) %>%
  summarise(
    wins_NN = sum(win_loss_NN, na.rm = T),
    wins_LM = sum(win_loss_LM, na.rm = T),
    total_trades = n()
  ) %>%
  mutate(
    Perc_NN = wins_NN/total_trades,
    Perc_LM = wins_LM/total_trades
  ) %>%
  filter(!is.na(short_long)) %>%
  ungroup() %>%
  group_by(short_long) %>%
  mutate(
    Total_Trades= sum(total_trades),
    Total_wins_NN = sum(wins_NN),
    Perc_Total_NN = Total_wins_NN/Total_Trades,

    Total_wins_LM = sum(wins_LM),
    Perc_Total_LM = Total_wins_LM/Total_Trades
  )

futures_trades_data_df_week_sum <- futures_trades_data_df %>%
  group_by(Asset, weekly_date) %>%
  slice_min(date) %>%
  group_by(Asset, short_long) %>%
  summarise(monthly_return_mean = mean(trade, na.rm = T),
            monthly_return_mean = quantile(trade,0.1, na.rm = T),

            monthly_return_total = sum(trade, na.rm = T),
            weekly_return_average = round(mean(trade, na.rm = T),4),
            weekly_return_low = round(quantile(trade,0.1 ,na.rm = T),4)
  ) %>%
  filter(!is.na(monthly_return_mean))

condition_based_trade_simulation <- function(modelled_data = futures_trades_data_df,
                                             condition_quantile_LM = 2,
                                             condition_quantile_NN = 2,
                                             model_col_LM = "pred_lm",
                                             model_col_NN = "pred",
                                             condition_type = "NN_only",
                                             trade_length = 6) {

  if(condition_type == "combined_synergistic") {
    tagged_trades <- modelled_data %>%
      mutate(Asset = str_trim(Asset)) %>%
      split(.$Asset, drop = FALSE) %>%
      map_dfr(
        ~ .x  %>%
          group_by(Asset) %>%
          mutate(
            take_trade :=
              case_when(
                !!as.name(model_col_LM) >
                  ( mean(change_var, na.rm = TRUE) + condition_quantile_LM*sd(change_var, na.rm = T) ) &
                  !!as.name(model_col_NN) >
                  ( mean(change_var, na.rm = TRUE) + condition_quantile_NN*sd(change_var, na.rm = T) ) ~ "short",

                !!as.name(model_col_LM) <
                  ( mean(change_var, na.rm = TRUE) - condition_quantile_LM*sd(change_var, na.rm = T) ) &
                  !!as.name(model_col_NN) <
                  ( mean(change_var, na.rm = TRUE) - condition_quantile_NN*sd(change_var, na.rm = T) ) ~ "long"
              )
          ) %>%
          group_by(Asset) %>%
          mutate(
            Open_1_day_lead = lead(Open, 1),
            Price_2_day_lead = lead(Price, 2),
            Price_7_day_lead = lead(Price, 7),

            High_7_day_lead = lead(High, 7),
            Low_7_day_lead = lead(Low, 7)
          ) %>%
          ungroup()
      )
  }

  if(condition_type == "NN_only") {
    tagged_trades <- modelled_data %>%
      mutate(Asset = str_trim(Asset)) %>%
      split(.$Asset, drop = FALSE) %>%
      map_dfr(
        ~ .x  %>%
          group_by(Asset) %>%
          mutate(
            take_trade :=
              case_when(
                !!as.name(model_col_NN) >
                  ( mean(change_var, na.rm = TRUE) + condition_quantile_NN*sd(change_var, na.rm = T) ) ~ "short",
                !!as.name(model_col_NN) <
                  ( mean(change_var, na.rm = TRUE) - condition_quantile_NN*sd(change_var, na.rm = T) ) ~ "long"

                  # !!as.name(model_col_NN) >
                  #   ( mean( !!as.name(model_col_NN), na.rm = TRUE) + condition_quantile_NN*sd(!!as.name(model_col_NN), na.rm = T) ) ~ "short",
                  # !!as.name(model_col_NN) > 0 ~ "long",
                  # !!as.name(model_col_NN) < 0 ~ "short"
              )
            # case_when(
            #   !!as.name(model_col_NN) > 0 ~ "long",
            #   !!as.name(model_col_NN) < 0 ~ "short"
            # )
          ) %>%
          group_by(Asset) %>%
          mutate(
            Open_1_day_lead = lead(Open, 1),
            Price_2_day_lead = lead(Price, 2),
            Price_7_day_lead = lead(Price, 7),

            High_7_day_lead = lead(High, 7),
            Low_7_day_lead = lead(Low, 7)
          ) %>%
          ungroup()
      )
  }

  if(condition_type == "LM_only") {
    tagged_trades <- modelled_data %>%
      mutate(Asset = str_trim(Asset)) %>%
      split(.$Asset, drop = FALSE) %>%
      map_dfr(
        ~ .x  %>%
          group_by(Asset) %>%
          mutate(
            take_trade :=
              case_when(
                !!as.name(model_col_LM) >
                  ( mean(change_var, na.rm = TRUE) + condition_quantile_LM*sd(change_var, na.rm = T) ) ~ "short",
                !!as.name(model_col_LM) <
                  ( mean(change_var, na.rm = TRUE) - condition_quantile_LM*sd(change_var, na.rm = T) ) ~ "long"
              )
            # case_when(
            #   !!as.name(model_col_LM) > 0 ~ "long",
            #   !!as.name(model_col_LM) < 0 ~ "short"
            # )
          ) %>%
          group_by(Asset) %>%
          mutate(
            Open_1_day_lead = lead(Open, 1),
            Price_2_day_lead = lead(Price, 2),
            Price_7_day_lead = lead(Price, 7),

            High_7_day_lead = lead(High, 7),
            Low_7_day_lead = lead(Low, 7)
          ) %>%
          ungroup()
      )
  }

  if(condition_type == "antogonistic") {

    tagged_trades <- modelled_data %>%
      mutate(Asset = str_trim(Asset)) %>%
      split(.$Asset, drop = FALSE) %>%
      map_dfr(
        ~ .x  %>%
          group_by(Asset) %>%
          mutate(
            take_trade :=
              case_when(
                !!as.name(model_col_LM) <
                  ( mean(change_var, na.rm = TRUE) + condition_quantile_LM*sd(change_var, na.rm = T) ) &
                  !!as.name(model_col_NN) >
                  ( mean(change_var, na.rm = TRUE) + condition_quantile_NN*sd(change_var, na.rm = T) ) ~ "long",

                !!as.name(model_col_LM) >
                  ( mean(change_var, na.rm = TRUE) - condition_quantile_LM*sd(change_var, na.rm = T) ) &
                  !!as.name(model_col_NN) <
                  ( mean(change_var, na.rm = TRUE) - condition_quantile_NN*sd(change_var, na.rm = T) ) ~ "short"
              )
          ) %>%
          group_by(Asset) %>%
          mutate(
            Open_1_day_lead = Open,
            Price_2_day_lead = lead(Price, 2),
            Price_7_day_lead = lead(Price, 7),

            High_7_day_lead = lead(High, 7),
            Low_7_day_lead = lead(Low, 7)
          ) %>%
          ungroup()
      )

  }

  tagged_trades <- tagged_trades %>%
    group_by(Asset) %>%
    arrange(date, .by_group = TRUE) %>%
    group_by(Asset) %>%
    mutate(
      Open_1_day_lead = Open,
      Price_2_day_lead = lead(Price, 1),
      Price_7_day_lead = lead(Price, 7),

      High_7_day_lead = lead(High, 7),
      Low_7_day_lead = lead(Low, 7)
    ) %>%
    ungroup()

  trade_return_col <- paste0("Price_",(trade_length + 1),"_day_lead")

  tagged_trades2 <- tagged_trades %>%
    filter(!is.na(take_trade)) %>%
    dplyr::select(Asset, take_trade, pred, pred_lm,
                  Open_1_day_lead, !!as.name(trade_return_col), Price, Low, High, Open ) %>%
    group_by(Asset) %>%
    mutate(
      trade_return =
        case_when(
          take_trade == "long" & (!!as.name(trade_return_col) - Open_1_day_lead) > 0 ~ abs((!!as.name(trade_return_col) - Open_1_day_lead)),
          take_trade == "long" & (!!as.name(trade_return_col) - Open_1_day_lead) < 0 ~ -1*abs((!!as.name(trade_return_col) - Open_1_day_lead)),

          take_trade == "short" & (!!as.name(trade_return_col) - Open_1_day_lead) > 0 ~ -1*abs((!!as.name(trade_return_col) - Open_1_day_lead)),
          take_trade == "short" & (!!as.name(trade_return_col) - Open_1_day_lead) < 0 ~ abs((!!as.name(trade_return_col) - Open_1_day_lead))
        ),
      win_loss =
        case_when(
          trade_return < 0 ~ 0,
          trade_return > 0 ~ 1
          )
    ) %>%
    ungroup()

  control_data <- modelled_data %>%
    group_by(Asset) %>%
    mutate(
      Open_1_day_lead = lead(Open, 1),
      Price_2_day_lead = lead(Price, 2),
      Price_7_day_lead = lead(Price, 7),

      High_7_day_lead = lead(High, 7),
      Low_7_day_lead = lead(Low, 7)
    ) %>%
    ungroup() %>%
    mutate(
      price_change = (!!as.name(trade_return_col) - Open_1_day_lead)
    ) %>%
    mutate(
      take_trade =
        case_when(
          price_change <0 ~ "short",
          price_change >0 ~ "long"
        )
    ) %>%
    group_by(take_trade, Asset) %>%
    summarise(
      Actual_Candle = n()
    ) %>%
    ungroup() %>%
    group_by(Asset) %>%
    mutate(
      Total_Trades = sum(Actual_Candle, na.rm = T),
      Actual_Perc = Actual_Candle/Total_Trades
    ) %>%
    filter(!is.na(take_trade)) %>%
    dplyr::select(Asset, take_trade, Actual_Perc,
                   Total_Candles = Total_Trades) %>%
    mutate(
      Asset = str_trim(Asset)
    )

  tagged_trades_sum <- tagged_trades2 %>%
    ungroup() %>%
    mutate(
      trade_return =
        case_when(
          str_detect(Asset ,"[A-Z][A-Z][A-Z]_[A-Z][A-Z][A-Z]") &
            Asset != "XAU_USD" & Asset != "XAU_USD"~ 1000*trade_return,
          TRUE ~ trade_return
        )
    ) %>%
    group_by(Asset, take_trade) %>%
    summarise(
      trade_return_sum = sum(trade_return, na.rm = T),

      win_loss = sum(win_loss, na.rm = T),
      avg_return_per_trade = mean(trade_return, na.rm = T),
      total_trades = n()
    ) %>%
    ungroup() %>%
    filter(total_trades >= 100) %>%
    mutate(
      Perc = win_loss/total_trades
    ) %>%
    group_by(take_trade) %>%
    mutate(
      total_return = sum(trade_return_sum),
      total_return_mean = mean(avg_return_per_trade)
    ) %>%
    mutate(
      Perc_Total = sum(win_loss)/sum(total_trades)
    ) %>%
    left_join(control_data) %>%
    mutate(
      Edge_Beyond_Actual =  round(Perc - Actual_Perc, 5)
    ) %>%
    mutate(Average_Edge = mean(Edge_Beyond_Actual))

  return(tagged_trades_sum)

}
