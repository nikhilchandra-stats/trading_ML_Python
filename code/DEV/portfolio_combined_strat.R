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
raw_macro_data <- get_macro_event_data()
#---------------------Data
load_custom_functions()
db_location = "C:/Users/nikhi/Documents/Asset Data/Oanda_Asset_Data_Most_Assets_2025-09-13.db"
start_date = "2019-01-01"
end_date = today() %>% as.character()

bin_factor = NULL
stop_value_var = 10
profit_value_var = 50
period_var = 50

Indices_Metals_Bonds <- list()

Indices_Metals_Bonds[[1]] <-
  get_db_data_quickly_algo(
    db_location = db_location,
    start_date = start_date,
    end_date = as.character(today() + days(30)),
    time_frame = "H1",
    bid_or_ask = "ask",
    assets =   c("SPX500_USD","US2000_USD","EU50_EUR","SG30_SGD" ,
                 "AU200_AUD" ,"XAG_USD","XAU_USD","USD_JPY" ,
                 "AUD_USD" ,"UK100_GBP" ,"JP225Y_JPY","FR40_EUR" ,
                 "CH20_CHF","USB10Y_USD","USB02Y_USD" ,"UK10YB_GBP" ,
                 "HK33_HKD" ,"EUR_USD" ,"GBP_USD" ,"XAG_EUR" ,
                 "XAU_EUR" ,"XAU_GBP" ,"XAG_GBP" ,"EUR_GBP" ,
                 "WTICO_USD" ,"BCO_USD" ,"XCU_USD" ,"XAU_JPY",
                 "XAG_JPY" ,"XAU_AUD" ,"XAG_AUD" ,"USD_CAD" ,
                 "EUR_AUD" ,"NZD_USD" ,"EUR_NZD" ,"AUD_NZD" ,
                 "GBP_AUD" ,"GBP_NZD" ,"GBP_CAD" ,"GBP_JPY" ,
                 "USD_SGD" ,"EUR_JPY" , "BTC_USD" ,"ETH_USD" ,"NATGAS_USD" ,
                 "EUR_SEK" ,"USD_SEK" ,"LTC_USD" , "XAG_NZD")
  ) %>%
  distinct()
Indices_Metals_Bonds[[2]] <-
  get_db_data_quickly_algo(
    db_location = db_location,
    start_date = start_date,
    end_date = as.character(today() + days(30)),
    time_frame = "H1",
    bid_or_ask = "bid",
    assets =   c("SPX500_USD","US2000_USD","EU50_EUR","SG30_SGD" ,
                 "AU200_AUD" ,"XAG_USD","XAU_USD","USD_JPY" ,
                 "AUD_USD" ,"UK100_GBP" ,"JP225Y_JPY","FR40_EUR" ,
                 "CH20_CHF","USB10Y_USD","USB02Y_USD" ,"UK10YB_GBP" ,
                 "HK33_HKD" ,"EUR_USD" ,"GBP_USD" ,"XAG_EUR" ,
                 "XAU_EUR" ,"XAU_GBP" ,"XAG_GBP" ,"EUR_GBP" ,
                 "WTICO_USD" ,"BCO_USD" ,"XCU_USD" ,"XAU_JPY",
                 "XAG_JPY" ,"XAU_AUD" ,"XAG_AUD" ,"USD_CAD" ,
                 "EUR_AUD" ,"NZD_USD" ,"EUR_NZD" ,"AUD_NZD" ,
                 "GBP_AUD" ,"GBP_NZD" ,"GBP_CAD" ,"GBP_JPY" ,
                 "USD_SGD" ,"EUR_JPY" , "BTC_USD" ,"ETH_USD" ,"NATGAS_USD" ,
                 "EUR_SEK" ,"USD_SEK" ,"LTC_USD" , "XAG_NZD")
  ) %>%
  distinct()

Indices_Metals_Bonds[[1]] <- Indices_Metals_Bonds[[1]] %>% filter(Date >= "2022-01-01")
Indices_Metals_Bonds[[2]] <- Indices_Metals_Bonds[[2]] %>% filter(Date >= "2022-01-01")

tictoc::tic()
all_preds <-
  Single_Asset_V3_get_all_preds(
    Indices_Metals_Bonds = Indices_Metals_Bonds,
    raw_macro_data = raw_macro_data,
    base_path = "C:/Users/nikhi/Documents/trade_data/Day_Trader_Single_Asset_V3_Expanded_Models/",
    actuals_periods_needed = c("period_return_50_Price"),
    correlation_rolling_periods = c(100,200, 300,400, 500),
    state_space_periods = c(20, 40, 60, 100, 200,300, 400,  500),
    state_space_rolling = c(100, 200, 300, 400),
    date_for_true_simualtion = "2019-01-01",
    training_end_date = "2021-01-01",
    asset_index_start = 1,
    asset_index_end = 35
  )
tictoc::toc()


all_preds_dfr <- all_preds

trade_direction <- "Long"

generated_preds <-
  all_preds_dfr %>%
  mutate(
    across(.cols = c(Date, training_end_date, date_for_true_simualtion),
           .fns = ~ as_datetime(., tz = "Australia/Canberra"))
  ) %>%
  filter(Date > training_end_date) %>%
  filter(Date > date_for_true_simualtion)


create_combined_port_dat_for_reg <-
  function(
    pred_data = generated_preds,
    asssets_to_combine = c("SPX500_USD", "XAU_USD", "EU50_EUR", "USD_JPY", "AUD_USD", "EUR_USD",
                           "XAG_USD"),
    actual_wins_losses,
    return_col = "period_return_8_Price",
    stop_factor_var = 3,
    profit_factor_var = 6,
    cumulative_lag = 24,
    state_space_periods = c(20, 40, 60, 100, 200,300, 400,  500),
    state_space_rolling = c(100, 200, 300, 400),
    cor_period = 100
    ) {

    returned_list <- list()
    for (i in 1:length(asssets_to_combine)) {
      temp <-
        generated_preds %>%
        filter(Asset == asssets_to_combine[i])
      temp2 <- temp %>%
        dplyr::select(-Date, -training_end_date, -date_for_true_simualtion,
                      -Asset, -contains("_sd"), -contains("_mean"))
      names_temp2 <-
        names(temp2) %>%
        map(~ paste0(asssets_to_combine[i], "_", .x) ) %>%
        unlist()

      names(temp2) <- names_temp2

      returned_list[[i]] <-
        temp %>%
        dplyr::select(Date,
                      training_end_date,
                      date_for_true_simualtion) %>%
        bind_cols(temp2)
    }

    returned_list_dfr <-
      returned_list %>%
      reduce(left_join)

    returnd_data_combined <-
      actual_wins_losses %>%
      filter(Asset %in% asssets_to_combine) %>%
      ungroup() %>%
      filter(stop_factor == stop_factor_var, profit_factor == profit_factor_var) %>%
      dplyr::select(Date, !!as.name(return_col)) %>%
      group_by(Date) %>%
      summarise(
        !!as.name(return_col) := sum(!!as.name(return_col), na.rm = T)
      )

    final_data <-
      returned_list_dfr %>%
      left_join(returnd_data_combined) %>%
      filter(if_all(everything(), ~!is.na(.)))  %>%
      ungroup() %>%
      mutate(
        Asset = "Portfolio",
        return_values = period_return_24_Price
      ) %>%
      arrange(Date) %>%
      mutate(
        cumulative_return = cumsum(period_return_24_Price),
        cumulative_return = lag(cumulative_return, cumulative_lag)
      )

    state_space_LM_version_list <- list()
    c = 0

    for (i in 1:length(state_space_periods)) {
      for (j in 1:length(state_space_rolling)) {

        c = c + 1
        state_space_LM_version_list[[c]] <-
          Single_Asset_V3_state_space(asset_data = final_data,
                                      asset_of_interest = "Portfolio",
                                      Price_diff_lag = state_space_periods[i],
                                      roll_period_state_space = state_space_rolling[j],
                                      price_col = "cumulative_return")

      }
    }

    state_space_LM_version <-
      state_space_LM_version_list %>%
      reduce(left_join)

    final_data_2 <-
      final_data %>%
      left_join(state_space_LM_version) %>%
      filter(if_all(everything(), ~ !is.na(.)))

    correlation_variables <-
      names(final_data_2) %>%
      keep(~ str_detect(.x, "_LM"))

    for (i in 1:length(correlation_variables)) {
      for (j in 1:length(correlation_variables)) {
       var_1 <- correlation_variables[i]
       var_2 <- correlation_variables[j]
       new_var_name <-
         paste0("Cor_Var_", i, "_", j, "_", cor_period)

        if(var_1 != var_2) {
          final_data_2 <-
            final_data_2 %>%
            mutate(
              !!as.name(new_var_name) :=
                slider::slide2_dbl(.x = !!as.name(var_1),
                                   .y = !!as.name(var_2),
                                   .f = ~cor(.x, .y),
                                   .before = cor_period)
            )
        }
      }
    }

    return(final_data_2)

  }

create_combined_port_LM <-
  function(
    lm_dat = lm_dat,
    return_col = "period_return_8_Price",
    sig_thresh = 0.01,
    bin_value = 10,
    training_date_start = "2019-01-01",
    training_date_end = "2023-01-01"
    ) {

    training_data <-
      lm_dat %>%
      filter(Date >= training_date_start,
             Date <= training_date_end) %>%
      mutate(
        bin_var =
          ifelse( !!as.name(return_col) >= bin_value,
                  1, 0)
      )

    testing_data <-
      lm_dat %>%
      filter(Date > training_date_end)

    lm_vars <-
      names(lm_dat) %>%
      keep(~ !(.x %in% c("Date", return_col,
                         "training_end_date",
                         "date_for_true_simualtion",
                         "return_values", "Asset", "cumulative_return")))
    lm_form <-
      create_lm_formula(dependant = return_col, independant = lm_vars)

    lm_model <-
      lm(data = training_data, formula = lm_form)

    glm_form <-
      create_lm_formula(dependant = "bin_var", independant = lm_vars)

    glm_model <-
      glm(data = training_data, formula = glm_form, family = binomial("logit"))

    pred_out_of_sample_raw <-
      testing_data %>%
      mutate(
        LM_Pred = predict(object = lm_model, testing_data),
        GLM_Pred = predict(object = glm_model, testing_data, type = "response")
      )

    sig_coefs <- get_sig_coefs(lm_model,
                               p_value_thresh_for_inputs = sig_thresh)

    lm_vars <- sig_coefs

    lm_form <-
      create_lm_formula(dependant = return_col, independant = lm_vars)

    lm_model <-
      lm(data = training_data, formula = lm_form)

    glm_form <-
      create_lm_formula(dependant = "bin_var", independant = lm_vars)

    glm_model <-
      glm(data = training_data, formula = glm_form, family = binomial("logit"))

    pred_out_of_sample_sig <-
      testing_data %>%
      mutate(
        LM_Pred_sig = predict(object = lm_model, testing_data),
        GLM_Pred_sig = predict.glm(object = glm_model, testing_data, type = "response")
      )

    pred_out_of_sample <-
      pred_out_of_sample_sig %>%
      left_join(pred_out_of_sample_raw) %>%
      filter(Date > training_date_end)

    return(pred_out_of_sample)

  }

assets_to_port <-
  c("SPX500_USD", "XAU_USD", "EU50_EUR", "USD_JPY", "AUD_USD", "EUR_USD",
            "XAG_USD", "HK33_HKD", "USD_CAD", "USB10Y_USD", "NATGAS_USD", "AU200_AUD",
            "WTICO_USD", "XCU_USD")

actual_wins_losses <-
  get_actual_wins_losses(
    assets_to_analyse =
      c(
        # "EUR_USD", #1
        # "EU50_EUR", #2
        # "SPX500_USD", #3
        # "US2000_USD", #4
        # "USB10Y_USD", #5
        # "USD_JPY", #6
        # "AUD_USD", #7
        # "EUR_GBP", #8
        # "AU200_AUD" ,#9
        # "EUR_AUD", #10
        # "WTICO_USD", #11
        # "UK100_GBP", #12
        # "USD_CAD", #13
        # "GBP_USD", #14
        # "GBP_CAD", #15
        # "EUR_JPY", #16
        # "EUR_NZD", #17
        # "XAG_USD", #18
        # "XAG_EUR", #19
        # "XAG_AUD", #20
        # "XAG_NZD", #21
        # "HK33_HKD", #22
        # "FR40_EUR", #23
        # "BTC_USD", #24
        # "XAG_GBP", #25
        # "GBP_AUD", #26
        # "USD_SEK", #27
        # "USD_SGD", #28
        # "NZD_USD", #29
        # "GBP_NZD", #30
        # "XCU_USD", #31
        # "NATGAS_USD", #32
        # "GBP_JPY", #33
        # "SG30_SGD", #34
        # "XAU_USD", #35
        # "EUR_SEK", #36
        # "XAU_AUD", #37
        # "UK10YB_GBP", #38
        # "JP225Y_JPY", #39
        # "ETH_USD" #40
        assets_to_port
      ),
    asset_data = Indices_Metals_Bonds,
    # stop_factor = stop_value_var,
    # profit_factor = profit_value_var,
    stop_factor = 5,
    profit_factor = 10,
    risk_dollar_value = 10,
    trade_direction = "Long",
    currency_conversion = currency_conversion,
    asset_infor = asset_infor,
    periods_ahead = period_var
  )

lm_dat <-
  create_combined_port_dat_for_reg(
    pred_data = generated_preds,
    asssets_to_combine = assets_to_port,
    actual_wins_losses = actual_wins_losses,
    return_col = "period_return_24_Price",
    stop_factor_var = 5,
    profit_factor_var = 10
  )

actual_wins_losses_port <-
  actual_wins_losses %>%
  filter(Asset %in% asssets_to_combine) %>%
  ungroup() %>%
  filter(stop_factor == stop_factor_var, profit_factor == profit_factor_var) %>%
  dplyr::select(Date, !!as.name(return_col),
                stop_factor, profit_factor, volume_required) %>%
  group_by(Date, stop_factor, profit_factor) %>%
  summarise(
    !!as.name(return_col) := sum(!!as.name(return_col), na.rm = T),
    volume_required = sum(volume_required, na.rm = T)
  ) %>%
  mutate(
    Asset = "Portfolio"
  )

out_of_sample_dat <-
  create_combined_port_LM(
  lm_dat = lm_dat,
  return_col = "period_return_24_Price",
  sig_thresh = 0.0001,
  training_date_start = "2018-01-01",
  training_date_end = "2023-01-01"
) %>%
  dplyr::select(-period_return_24_Price,) %>%
  mutate(Asset = "Portfolio")

trade_statement <- "LM_Pred < 300 & LM_Pred > 0"

actual_wins_losses_port <-
  actual_wins_losses %>%
  filter(Asset %in% assets_to_port) %>%
  ungroup() %>%
  group_by(Date, trade_col) %>%
  summarise(
    across(contains("period_return_"),
           .fns = ~ sum(., na.rm = T)),
    volume_required = sum(volume_required, na.rm = T)
  ) %>%
  ungroup() %>%
  mutate(
    Asset = "Portfolio"
  )

cumulative_returns_sim_data <-
  get_total_portfolio_summary(
    generated_preds = out_of_sample_dat %>%
      filter(Asset == "Portfolio")
    # filter( str_detect(Asset, "[A-Z]"))
    ,
    trade_statement = trade_statement,
    actual_wins_losses =actual_wins_losses_port %>%
      filter(Asset == "Portfolio")
    # filter( str_detect(Asset, "[A-Z]"))
    ,
    trade_direction = "Long",
    return_col = "period_return_24_Price"
  )

cumulative_returns_sim_data %>%
  ggplot(aes(x = Date, y = Cumulative_Return)) +
  geom_line() +
  facet_wrap(.~trade_col, scales = "free") +
  scale_y_continuous(n.breaks = 20) +
  theme_minimal()

asset_summaries_control <-
  get_asset_random_sim_returns(
    generated_preds = out_of_sample_dat %>%
      filter(Asset == "Portfolio")
    # filter( str_detect(Asset, "[A-Z]"))
    ,
    trade_statement = "str_detect(Asset, '[A-Z]')",
    actual_wins_losses = actual_wins_losses_port %>%
      filter(Asset == "Portfolio")
    # filter( str_detect(Asset, "[A-Z]"))
    ,
    trade_direction = "Long",
    return_col = "period_return_24_Price",
    simulations = 5000,
    samples = 50
  )

asset_summaries <-
  get_asset_random_sim_returns(
    generated_preds = out_of_sample_dat %>%
      filter(Asset == "Portfolio")
    # filter( str_detect(Asset, "[A-Z]"))
    ,
    trade_statement = trade_statement,
    actual_wins_losses = actual_wins_losses_port %>%
      filter(Asset == "Portfolio")
    # filter( str_detect(Asset, "[A-Z]"))
    ,
    trade_direction = "Long",
    return_col = "period_return_24_Price",
    simulations = 7000,
    samples = 50
  )
