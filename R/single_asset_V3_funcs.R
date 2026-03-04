#' #' single_asset_Logit_indicator_adv_gen_models
#' #'
#' #' @param asset_data
#' #' @param All_Daily_Data
#' #' @param Asset_of_interest
#' #' @param actual_wins_losses
#' #' @param interest_rates
#' #' @param cpi_data
#' #' @param sentiment_index
#' #' @param equity_index
#' #' @param gold_index
#' #' @param silver_index
#' #' @param bonds_index
#' #' @param USD_index
#' #' @param EUR_index
#' #' @param GBP_index
#' #' @param AUD_index
#' #' @param countries_for_int_strength
#' #' @param date_train_end
#' #' @param date_train_phase_2_end
#' #' @param date_test_start
#' #' @param couplua_assets
#' #' @param stop_value_var
#' #' @param profit_value_var
#' #' @param period_var
#' #' @param bin_var_col
#' #' @param trade_direction
#' #' @param save_path
#' #'
#' #' @returns
#' #' @export
#' #'
#' #' @examples
#' single_asset_Logit_indicator_V3_V2_Gen_Model <-
#'   function(
#'     asset_data = Indices_Metals_Bonds[[1]],
#'     All_Daily_Data = All_Daily_Data,
#'     Asset_of_interest = "EUR_USD",
#'     actual_wins_losses = actual_wins_losses,
#'     interest_rates = interest_rates,
#'     cpi_data = cpi_data,
#'     sentiment_index = sentiment_index,
#'     gdp_data = gdp_data,
#'     unemp_data = unemp_data,
#'     manufac_pmi = manufac_pmi,
#'     USD_Macro = USD_Macro,
#'     EUR_Macro = EUR_Macro,
#'
#'     equity_index = equity_index,
#'     gold_index = gold_index,
#'     silver_index = silver_index,
#'     bonds_index = bonds_index,
#'     USD_index = USD_index,
#'     EUR_index = EUR_index,
#'     GBP_index = GBP_index,
#'     AUD_index = AUD_index,
#'     COMMOD_index = COMMOD_index,
#'     USD_STOCKS_index = USD_STOCKS_index,
#'     NZD_index = NZD_index,
#'
#'     countries_for_int_strength = countries_for_int_strength,
#'     date_train_end = post_train_date_start,
#'     date_train_phase_2_end = post_train_date_start + months(6),
#'     date_test_start = post_train_date_start + months(7),
#'
#'     couplua_assets = couplua_assets,
#'
#'     stop_value_var = stop_value_var,
#'     profit_value_var = profit_value_var,
#'     period_var = period_var,
#'
#'     bin_var_col = c("period_return_20_Price", "period_return_35_Price"),
#'     trade_direction = "Long",
#'     save_path = "C:/Users/nikhi/Documents/trade_data/Day_Trader_Single_Asset_V3_trade_store_stop_2"
#'
#'   ) {
#'
#'     asset_data_internal <-
#'       asset_data %>%
#'       filter(Asset == Asset_of_interest)
#'
#'     AR_model_data <-
#'       Single_Asset_V3_AR_Model_data(
#'         asset_data = asset_data_internal,
#'         asset_of_interest = Asset_of_interest,
#'         lag_value_1 = 10,
#'         lag_value_2 = 20,
#'         lag_value_3 = 30,
#'         lag_value_4 = 40,
#'         lag_value_5 = 50,
#'         lag_value_6 = 60,
#'         lag_value_7 = 70,
#'         MA_period_1 = 10,
#'         MA_period_2 = 20,
#'         MA_period_3 = 30,
#'         MA_period_4 = 40,
#'         MA_period_5 = 20,
#'         MA_period_6 = 20
#'       )
#'
#'     for (i in 1:length(bin_var_col)) {
#'       Single_Asset_V3_AR_Gen_Model(
#'         AR_model_data = AR_model_data %>%
#'           filter(Date <= date_train_end),
#'         asset_of_interest = Asset_of_interest,
#'         actual_wins_losses_asset =
#'           actual_wins_losses %>%
#'           filter(Asset == Asset_of_interest),
#'         period_of_analysis = bin_var_col[i],
#'         training_end_date = date_train_end,
#'         bin_threshold = 5,
#'         sig_thresh = 0.15,
#'         base_path = save_path
#'       )
#'     }
#'
#'     AR_preds_list <- list()
#'
#'     for (i in 1:length(bin_var_col)) {
#'       AR_preds_list[[i]] <-
#'         Single_Asset_V3_AR_read_model(
#'           AR_model_data = AR_model_data,
#'           asset_of_interest = Asset_of_interest,
#'           period_of_analysis = bin_var_col[i],
#'           training_end_date = date_train_end,
#'           roll_mean_period = 500,
#'           base_path = save_path
#'         )
#'     }
#'
#'     AR_Train_Preds_mean <-
#'       AR_preds_list %>%
#'       map(~.x %>% pluck("training_data")) %>%
#'       reduce(left_join)
#'
#'     AR_Test_Preds <-
#'       AR_preds_list %>%
#'       map(~.x %>% pluck("testing_data")) %>%
#'       reduce(left_join)
#'
#'     AR_Train_Preds <-
#'       AR_preds_list %>%
#'       map(~.x %>% pluck("training_data")) %>%
#'       reduce(left_join)
#'
#'     rm(AR_preds_list)
#'
#'     AR_preds <-
#'       AR_Test_Preds %>%
#'       bind_rows(AR_Train_Preds) %>%
#'       dplyr::select(-contains("_mean"))%>%
#'       dplyr::select(-contains("_sd"))
#'
#'     names(AR_preds) <-
#'       names(AR_preds) %>%
#'       map(
#'         ~
#'           str_remove_all(.x, "period_return_")
#'       ) %>%
#'       unlist()
#'
#'     AR_preds_averages <-
#'       AR_Train_Preds %>%
#'       filter(if_all( everything(), ~ !is.nan(.) & !is.infinite(.) ) ) %>%
#'       summarise(
#'         across(.cols = contains("pred"), .fns = ~ mean(., na.rm = T))
#'       )
#'
#'     names(AR_preds_averages) <-
#'       names(AR_preds_averages) %>%
#'       map(~ paste0(.x, "_mean")) %>%
#'       unlist() %>%
#'       as.character()
#'
#'     AR_preds_sd <-
#'       AR_Train_Preds %>%
#'       filter(if_all( everything(), ~ !is.nan(.) & !is.infinite(.) ) ) %>%
#'       summarise(
#'         across(.cols = contains("pred"), .fns = ~ sd(., na.rm = T))
#'       )
#'
#'     names(AR_preds_sd) <-
#'       names(AR_preds_sd) %>%
#'       map(~ paste0(.x, "_sd")) %>%
#'       unlist() %>%
#'       as.character()
#'
#'     rm(AR_Test_Preds, AR_Train_Preds)
#'
#'     copula_v3_list <- list()
#'     correlation_rolling_periods <- c(200, 300)
#'
#'     for (i in 1:length(correlation_rolling_periods)) {
#'
#'       copula_v3_list[[i]] <-
#'         Single_Asset_V3_Cop_data(
#'           All_Asset_Data =
#'             Indices_Metals_Bonds[[1]] %>%
#'             filter(Asset == Asset_of_interest| Asset %in% couplua_assets),
#'           asset_of_interest = Asset_of_interest,
#'           copula_assets = couplua_assets,
#'           rolling_period_cor = correlation_rolling_periods[i]
#'         )
#'
#'     }
#'
#'     copula_v3_data <- copula_v3_list %>% reduce(left_join)
#'     rm(copula_v3_list)
#'
#'     for (i in 1:length(bin_var_col)) {
#'       Single_Asset_V3_Copula_Gen_Model(
#'         copula_data = copula_v3_data,
#'         asset_of_interest = Asset_of_interest,
#'         actual_wins_losses_asset =
#'           actual_wins_losses %>%
#'           filter(Asset == Asset_of_interest),
#'         period_of_analysis = bin_var_col[i],
#'         training_end_date = date_train_end,
#'         bin_threshold = 5,
#'         sig_thresh = 0.01,
#'         base_path = save_path
#'       )
#'     }
#'
#'     copula_preds_list <- list()
#'
#'     for (i in 1:length(bin_var_col)) {
#'       copula_preds_list[[i]] <-
#'         Single_Asset_V3_Copula_read_Model(
#'           copula_data = copula_v3_data,
#'           asset_of_interest = Asset_of_interest,
#'           period_of_analysis = bin_var_col[i],
#'           training_end_date = date_train_end,
#'           roll_mean_period = 500,
#'           base_path = save_path
#'         )
#'     }
#'
#'     Copula_v3_Test_Preds <-
#'       copula_preds_list %>%
#'       map(~.x %>% pluck("testing_data")) %>%
#'       reduce(left_join)
#'
#'     Copula_v3_Train_Preds <-
#'       copula_preds_list %>%
#'       map(~.x %>% pluck("training_data")) %>%
#'       reduce(left_join)
#'
#'     rm(copula_preds_list)
#'
#'     Copula_v3_preds <-
#'       Copula_v3_Test_Preds %>%
#'       bind_rows(Copula_v3_Train_Preds) %>%
#'       dplyr::select(-contains("_mean"))%>%
#'       dplyr::select(-contains("_sd"))
#'
#'     names(Copula_v3_preds) <-
#'       names(Copula_v3_preds) %>%
#'       map(
#'         ~
#'           str_remove_all(.x, "period_return_") %>%
#'           str_replace_all(pattern = "Copula_", replacement = "Copulav3_")
#'       ) %>%
#'       unlist()
#'
#'     Copula_v3_preds_averages <-
#'       Copula_v3_Train_Preds %>%
#'       filter(if_all( everything(), ~ !is.nan(.) & !is.infinite(.) ) ) %>%
#'       summarise(
#'         across(.cols = contains("pred"), .fns = ~ mean(., na.rm = T))
#'       )
#'
#'     names(Copula_v3_preds_averages) <-
#'       names(Copula_v3_preds_averages) %>%
#'       map(~ paste0(.x, "_mean")) %>%
#'       unlist() %>%
#'       as.character()
#'
#'     Copula_v3_preds_sd <-
#'       Copula_v3_Train_Preds %>%
#'       filter(if_all( everything(), ~ !is.nan(.) & !is.infinite(.) ) ) %>%
#'       summarise(
#'         across(.cols = contains("pred"), .fns = ~ sd(., na.rm = T))
#'       )
#'
#'     names(Copula_v3_preds_sd) <-
#'       names(Copula_v3_preds_sd) %>%
#'       map(~ paste0(.x, "_sd")) %>%
#'       unlist() %>%
#'       as.character()
#'
#'     rm(Copula_v3_Test_Preds, Copula_v3_Train_Preds)
#'
#'     macro_data <-
#'       prepare_macro_indicator_model_data(
#'         asset_data = asset_data_internal,
#'         Asset_of_interest = Asset_of_interest,
#'         interest_rates = interest_rates,
#'         cpi_data = cpi_data,
#'         sentiment_index = sentiment_index,
#'         gdp_data = gdp_data,
#'         unemp_data = unemp_data,
#'         manufac_pmi = manufac_pmi,
#'         USD_Macro = USD_Macro,
#'         EUR_Macro = EUR_Macro,
#'         countries_for_int_strength = countries_for_int_strength,
#'         date_limit = now(tzone = "Australia/Canberra")
#'       )
#'
#'     macro_train_data <-
#'       macro_data %>%
#'       filter(Date <= date_train_end)
#'
#'     message("Saving macro Models")
#'     prepare_macro_indicator_model(
#'       macro_for_join = macro_train_data,
#'       actual_wins_losses = actual_wins_losses,
#'       Asset_of_interest = Asset_of_interest,
#'       date_limit = date_train_end,
#'       stop_value_var = stop_value_var,
#'       profit_value_var = profit_value_var,
#'       period_var = period_var,
#'       bin_var_col = bin_var_col,
#'       trade_direction = trade_direction,
#'       save_path = save_path
#'     )
#'
#'     macro_phase_2_data <-
#'       macro_data %>%
#'       filter(Date < date_train_phase_2_end)
#'
#'     macro_preds <-
#'       single_asset_read_models_and_get_pred(
#'         pred_data = macro_phase_2_data,
#'         trade_direction = trade_direction,
#'         Asset_of_interest = Asset_of_interest,
#'         save_path = save_path,
#'         model_string = "_macro_"
#'       )
#'
#'     macro_preds_averages <-
#'       macro_preds %>%
#'       filter(if_all( everything(), ~ !is.nan(.) & !is.infinite(.) ) ) %>%
#'       summarise(
#'         across(.cols = contains("pred"), .fns = ~ mean(., na.rm = T))
#'       )
#'
#'     names(macro_preds_averages) <-
#'       names(macro_preds_averages) %>%
#'       map(~ paste0(.x, "_mean")) %>%
#'       unlist() %>%
#'       as.character()
#'
#'     macro_preds_sd <-
#'       macro_preds %>%
#'       filter(if_all( everything(), ~ !is.nan(.) & !is.infinite(.) ) ) %>%
#'       summarise(
#'         across(.cols = contains("pred"), .fns = ~ sd(., na.rm = T))
#'       )
#'
#'     names(macro_preds_sd) <-
#'       names(macro_preds_sd) %>%
#'       map(~ paste0(.x, "_sd")) %>%
#'       unlist() %>%
#'       as.character()
#'
#'     daily_data_for_modelling <-
#'       prepare_daily_indicator_data(
#'         asset_data = asset_data_internal,
#'         All_Daily_Data = All_Daily_Data,
#'         Asset_of_interest = Asset_of_interest,
#'         date_limit = now(tzone = "Australia/Canberra")
#'       )
#'
#'     daily_data_for_modelling_train <-
#'       daily_data_for_modelling %>%
#'       filter(Date <= date_train_end)
#'
#'     prepare_daily_indicator_model(
#'       daily_indicator = daily_data_for_modelling_train,
#'       actual_wins_losses = actual_wins_losses,
#'       Asset_of_interest = Asset_of_interest,
#'       date_limit = date_train_end,
#'       stop_value_var = stop_value_var,
#'       profit_value_var = profit_value_var,
#'       period_var = period_var,
#'       bin_var_col = bin_var_col,
#'       trade_direction = trade_direction,
#'       save_path = save_path
#'     )
#'
#'     daily_phase_2_data <-
#'       daily_data_for_modelling %>%
#'       filter(Date < date_train_phase_2_end)
#'
#'     daily_preds <-
#'       single_asset_read_models_and_get_pred(
#'         pred_data = daily_phase_2_data,
#'         trade_direction = trade_direction,
#'         Asset_of_interest = Asset_of_interest,
#'         save_path = save_path,
#'         model_string = "_daily_"
#'       )
#'
#'     daily_preds_averages <-
#'       daily_preds %>%
#'       filter(if_all( everything(), ~ !is.nan(.) & !is.infinite(.) ) ) %>%
#'       summarise(
#'         across(.cols = contains("pred"), .fns = ~ mean(., na.rm = T))
#'       )
#'
#'     names(daily_preds_averages) <-
#'       names(daily_preds_averages) %>%
#'       map(~ paste0(.x, "_mean")) %>%
#'       unlist() %>%
#'       as.character()
#'
#'     daily_preds_sd <-
#'       daily_preds%>%
#'       filter(if_all( everything(), ~ !is.nan(.) & !is.infinite(.) ) ) %>%
#'       summarise(
#'         across(.cols = contains("pred"), .fns = ~ sd(., na.rm = T))
#'       )
#'
#'     names(daily_preds_sd) <-
#'       names(daily_preds_sd) %>%
#'       map(~ paste0(.x, "_sd")) %>%
#'       unlist() %>%
#'       as.character()
#'
#'     copula_data <-
#'       prepare_copula_data(
#'         asset_data = asset_data,
#'         couplua_assets = couplua_assets,
#'         Asset_of_interest = Asset_of_interest,
#'         date_limit = now(tzone = "Australia/Canberra")
#'       )
#'
#'     copula_data_train <-
#'       copula_data %>%
#'       filter(Date <= date_train_end)
#'
#'     prepare_copula_model(
#'       copula_data = copula_data_train,
#'       actual_wins_losses = actual_wins_losses,
#'       Asset_of_interest = Asset_of_interest,
#'       date_limit = date_train_end,
#'       stop_value_var = stop_value_var,
#'       profit_value_var = profit_value_var,
#'       period_var = period_var,
#'       bin_var_col = bin_var_col,
#'       trade_direction = trade_direction,
#'       save_path = save_path
#'     )
#'
#'     copula_phase_2_data <-
#'       copula_data %>%
#'       filter(Date < date_train_phase_2_end)
#'
#'     copula_preds <-
#'       single_asset_read_models_and_get_pred(
#'         pred_data = copula_phase_2_data,
#'         trade_direction = trade_direction,
#'         Asset_of_interest = Asset_of_interest,
#'         save_path = save_path,
#'         model_string = "_copula_"
#'       )
#'
#'     copula_preds_averages <-
#'       copula_preds %>%
#'       filter(if_all( everything(), ~ !is.nan(.) & !is.infinite(.) ) ) %>%
#'       summarise(
#'         across(.cols = contains("pred"), .fns = ~ mean(., na.rm = T))
#'       )
#'
#'     names(copula_preds_averages) <-
#'       names(copula_preds_averages) %>%
#'       map(~ paste0(.x, "_mean")) %>%
#'       unlist() %>%
#'       as.character()
#'
#'     copula_preds_sd <-
#'       copula_preds%>%
#'       filter(if_all( everything(), ~ !is.nan(.) & !is.infinite(.) ) ) %>%
#'       summarise(
#'         across(.cols = contains("pred"), .fns = ~ sd(., na.rm = T))
#'       )
#'
#'     names(copula_preds_sd) <-
#'       names(copula_preds_sd) %>%
#'       map(~ paste0(.x, "_sd")) %>%
#'       unlist() %>%
#'       as.character()
#'
#'     technical_data <-
#'       create_technical_indicators(asset_data = asset_data_internal) %>%
#'       dplyr::select(-Price, -Low, -High, -Open)
#'
#'     technical_data <-
#'       technical_data %>%
#'       filter(Asset == Asset_of_interest) %>%
#'       arrange(Date) %>%
#'       mutate(
#'         across(.cols = !contains("Date"),
#'                .fns = ~ lag(.) )
#'       )
#'
#'     technical_data_train <-
#'       technical_data %>%
#'       filter(Date <= date_train_end)
#'
#'     prepare_technical_model(
#'       technical_data = technical_data_train,
#'       actual_wins_losses = actual_wins_losses,
#'       Asset_of_interest = Asset_of_interest,
#'       date_limit = date_train_end,
#'       stop_value_var = stop_value_var,
#'       profit_value_var = profit_value_var,
#'       period_var = period_var,
#'       bin_var_col = bin_var_col,
#'       trade_direction = trade_direction,
#'       save_path = save_path
#'     )
#'
#'     technical_phase_2_data <-
#'       technical_data %>%
#'       filter(Date < date_train_phase_2_end)
#'
#'     technical_preds <-
#'       single_asset_read_models_and_get_pred(
#'         pred_data = technical_phase_2_data,
#'         trade_direction = trade_direction,
#'         Asset_of_interest = Asset_of_interest,
#'         save_path = save_path,
#'         model_string = "_technical_"
#'       )
#'
#'     technical_preds_averages <-
#'       technical_preds %>%
#'       filter(if_all( everything(), ~ !is.nan(.) & !is.infinite(.) ) ) %>%
#'       summarise(
#'         across(.cols = contains("pred"), .fns = ~ mean(., na.rm = T))
#'       )
#'
#'     names(technical_preds_averages) <-
#'       names(technical_preds_averages) %>%
#'       map(~ paste0(.x, "_mean")) %>%
#'       unlist() %>%
#'       as.character()
#'
#'     technical_preds_sd <-
#'       technical_preds%>%
#'       filter(if_all( everything(), ~ !is.nan(.) & !is.infinite(.) ) ) %>%
#'       summarise(
#'         across(.cols = contains("pred"), .fns = ~ sd(., na.rm = T))
#'       )
#'
#'     names(technical_preds_sd) <-
#'       names(technical_preds_sd) %>%
#'       map(~ paste0(.x, "_sd")) %>%
#'       unlist() %>%
#'       as.character()
#'
#'     accumulating_probs <-
#'       asset_data_internal %>%
#'       distinct(Date, Asset) %>%
#'       left_join(macro_preds) %>%
#'       left_join(daily_preds) %>%
#'       left_join(copula_preds) %>%
#'       left_join(Copula_v3_preds) %>%
#'       left_join(technical_preds) %>%
#'       left_join(AR_preds) %>%
#'       left_join(macro_data) %>%
#'       left_join(copula_data)%>%
#'       left_join(copula_v3_data)%>%
#'       left_join(
#'         daily_data_for_modelling %>%
#'           dplyr::select(Date, Asset,
#'                         (contains("perc_line_")&contains("_20")),
#'                         contains("Support"),
#'                         contains("Resistance"),
#'                         contains("Bear"),
#'                         contains("Bull"),
#'                         (contains("perc_line_")&contains("_50")),
#'                         contains("moving_average_markov_")
#'           )
#'       ) %>%
#'       left_join(
#'         technical_data %>%
#'           dplyr::select(Date, Asset,
#'                         (contains("perc_line_")&contains("_500")),
#'                         contains("Support"),
#'                         contains("Resistance"),
#'                         contains("Bear"),
#'                         contains("Bull"),
#'                         (contains("perc_line_")&contains("_500")),
#'                         contains("moving_average_markov_")
#'           )
#'       ) %>%
#'       distinct()
#'
#'     rm(macro_data, macro_phase_2_data, macro_train_data)
#'     rm(index_pca_data, index_pca_phase_2_data, index_pca_train_data)
#'     rm(daily_phase_2_data, daily_data_for_modelling, daily_data_for_modelling_train)
#'     rm(copula_data, copula_data_train, copula_phase_2_data)
#'     rm(technical_data, technical_data_train, technical_phase_2_data)
#'     gc()
#'
#'     rm(macro_preds ,
#'        index_preds ,
#'        daily_preds ,
#'        copula_preds,
#'        technical_preds)
#'
#'     gc()
#'
#'     combined_model_data <-
#'       accumulating_probs %>%
#'       filter(Date < date_train_phase_2_end) %>%
#'       arrange(Date) %>%
#'       fill(!contains("Date"), .direction = "down") %>%
#'       filter(if_all(.cols = everything(), ~!is.na(.)))
#'
#'     prepare_combined_model(
#'       combined_model_data = combined_model_data,
#'       actual_wins_losses = actual_wins_losses,
#'       Asset_of_interest = Asset_of_interest,
#'       date_limit = date_train_end,
#'       stop_value_var = stop_value_var,
#'       profit_value_var = profit_value_var,
#'       period_var = period_var,
#'       bin_var_col = bin_var_col,
#'       trade_direction = trade_direction,
#'       save_path = save_path
#'     )
#'
#'     combined_preds <-
#'       single_asset_read_models_and_get_pred(
#'         pred_data = combined_model_data,
#'         trade_direction = trade_direction,
#'         Asset_of_interest = Asset_of_interest,
#'         save_path = save_path,
#'         model_string = "_combined_"
#'       )
#'
#'     combined_preds_averages <-
#'       combined_preds %>%
#'       summarise(
#'         across(.cols = contains("pred"), .fns = ~ mean(., na.rm = T))
#'       )
#'
#'     names(combined_preds_averages) <-
#'       names(combined_preds_averages) %>%
#'       map(~ paste0(.x, "_mean")) %>%
#'       unlist() %>%
#'       as.character()
#'
#'     combined_preds_sd <-
#'       combined_preds%>%
#'       summarise(
#'         across(.cols = contains("pred"), .fns = ~ sd(., na.rm = T))
#'       )
#'
#'     names(combined_preds_sd) <-
#'       names(combined_preds_sd) %>%
#'       map(~ paste0(.x, "_sd")) %>%
#'       unlist() %>%
#'       as.character()
#'
#'     mean_sd_values <-
#'       asset_data_internal %>%
#'       distinct(Date, Asset) %>%
#'       bind_cols(macro_preds_averages) %>%
#'       bind_cols(macro_preds_sd) %>%
#'       bind_cols(index_preds_averages) %>%
#'       bind_cols(index_preds_sd) %>%
#'       bind_cols(daily_preds_averages) %>%
#'       bind_cols(daily_preds_sd) %>%
#'       bind_cols(copula_preds_averages) %>%
#'       bind_cols(copula_preds_sd) %>%
#'       bind_cols(combined_preds_averages) %>%
#'       bind_cols(combined_preds_sd) %>%
#'       bind_cols(technical_preds_averages) %>%
#'       bind_cols(technical_preds_sd)
#'
#'     saveRDS(mean_sd_values,
#'             file =
#'               glue::glue("{save_path}/{Asset_of_interest}_{trade_direction}_mean_sd_values.RDS")
#'     )
#'
#'   }
#'
#' #' single_asset_Logit_indicator_adv_get_preds
#' #'
#' #' @param asset_data
#' #' @param All_Daily_Data
#' #' @param Asset_of_interest
#' #' @param actual_wins_losses
#' #' @param interest_rates
#' #' @param cpi_data
#' #' @param sentiment_index
#' #' @param equity_index
#' #' @param gold_index
#' #' @param silver_index
#' #' @param bonds_index
#' #' @param USD_index
#' #' @param EUR_index
#' #' @param GBP_index
#' #' @param AUD_index
#' #' @param countries_for_int_strength
#' #' @param date_train_end
#' #' @param date_train_phase_2_end
#' #' @param date_test_start
#' #' @param couplua_assets
#' #' @param stop_value_var
#' #' @param profit_value_var
#' #' @param period_var
#' #' @param bin_var_col
#' #' @param trade_direction
#' #' @param save_path
#' #'
#' #' @returns
#' #' @export
#' #'
#' #' @examples
#' single_asset_Logit_indicator_V3_V2_get_pred <-
#'   function(
#'     asset_data = Indices_Metals_Bonds[[1]],
#'     All_Daily_Data = All_Daily_Data,
#'     Asset_of_interest = "EUR_USD",
#'     actual_wins_losses = actual_wins_losses,
#'     interest_rates = interest_rates,
#'     cpi_data = cpi_data,
#'     sentiment_index = sentiment_index,
#'     gdp_data = gdp_data,
#'     unemp_data = unemp_data,
#'     manufac_pmi = manufac_pmi,
#'     USD_Macro = USD_Macro,
#'     EUR_Macro = EUR_Macro,
#'
#'     equity_index = equity_index,
#'     gold_index = gold_index,
#'     silver_index = silver_index,
#'     bonds_index = bonds_index,
#'     USD_index = USD_index,
#'     EUR_index = EUR_index,
#'     GBP_index = GBP_index,
#'     AUD_index = AUD_index,
#'     COMMOD_index = COMMOD_index,
#'     USD_STOCKS_index = USD_STOCKS_index,
#'     NZD_index = NZD_index,
#'
#'     countries_for_int_strength = countries_for_int_strength,
#'     date_train_end = post_train_date_start,
#'     date_train_phase_2_end = post_train_date_start + months(6),
#'     date_test_start = post_train_date_start + months(7),
#'     couplua_assets = couplua_assets,
#'     stop_value_var = stop_value_var,
#'     profit_value_var = profit_value_var,
#'     period_var = period_var,
#'     bin_var_col = c("period_return_20_Price", "period_return_35_Price"),
#'     trade_direction = "Long",
#'     save_path = "C:/Users/nikhi/Documents/trade_data/Day_Trader_Single_Asset_V3_trade_store_stop_2"
#'   ) {
#'
#'
#'     asset_data_internal <-
#'       asset_data %>%
#'       filter(Asset == Asset_of_interest)
#'
#'     AR_model_data <-
#'       Single_Asset_V3_AR_Model_data(
#'         asset_data = asset_data_internal,
#'         asset_of_interest = Asset_of_interest,
#'         lag_value_1 = 10,
#'         lag_value_2 = 20,
#'         lag_value_3 = 30,
#'         lag_value_4 = 40,
#'         lag_value_5 = 50,
#'         lag_value_6 = 60,
#'         lag_value_7 = 70,
#'         MA_period_1 = 10,
#'         MA_period_2 = 20,
#'         MA_period_3 = 30,
#'         MA_period_4 = 40,
#'         MA_period_5 = 20,
#'         MA_period_6 = 20
#'       )
#'
#'     AR_preds_list <- list()
#'
#'     for (i in 1:length(bin_var_col)) {
#'       AR_preds_list[[i]] <-
#'         Single_Asset_V3_AR_read_model(
#'           AR_model_data = AR_model_data,
#'           asset_of_interest = Asset_of_interest,
#'           period_of_analysis = bin_var_col[i],
#'           training_end_date = date_train_end,
#'           roll_mean_period = 500,
#'           base_path = save_path
#'         )
#'     }
#'
#'     AR_Test_Preds <-
#'       AR_preds_list %>%
#'       map(~.x %>% pluck("testing_data")) %>%
#'       reduce(left_join)
#'
#'     AR_Train_Preds <-
#'       AR_preds_list %>%
#'       map(~.x %>% pluck("training_data")) %>%
#'       reduce(left_join)
#'
#'     rm(AR_preds_list)
#'
#'     AR_preds <-
#'       AR_Test_Preds %>%
#'       bind_rows(AR_Train_Preds) %>%
#'       dplyr::select(-contains("_mean"))%>%
#'       dplyr::select(-contains("_sd"))
#'
#'     names(AR_preds) <-
#'       names(AR_preds) %>%
#'       map(
#'         ~
#'           str_remove_all(.x, "period_return_")
#'       ) %>%
#'       unlist()
#'
#'     rm(AR_Test_Preds, AR_Train_Preds)
#'
#'     copula_v3_list <- list()
#'     correlation_rolling_periods <- c(200, 300)
#'
#'     for (i in 1:length(correlation_rolling_periods)) {
#'
#'       copula_v3_list[[i]] <-
#'         Single_Asset_V3_Cop_data(
#'           All_Asset_Data =
#'             Indices_Metals_Bonds[[1]] %>%
#'             filter(Asset == Asset_of_interest| Asset %in% couplua_assets),
#'           asset_of_interest = Asset_of_interest,
#'           copula_assets = couplua_assets,
#'           rolling_period_cor = correlation_rolling_periods[i]
#'         )
#'
#'     }
#'
#'     copula_v3_data <- copula_v3_list %>% reduce(left_join)
#'     rm(copula_v3_list)
#'
#'     for (i in 1:length(bin_var_col)) {
#'       Single_Asset_V3_Copula_Gen_Model(
#'         copula_data = copula_v3_data,
#'         asset_of_interest = Asset_of_interest,
#'         actual_wins_losses_asset =
#'           actual_wins_losses %>%
#'           filter(Asset == Asset_of_interest),
#'         period_of_analysis = bin_var_col[i],
#'         training_end_date = date_train_end,
#'         bin_threshold = 5,
#'         sig_thresh = 0.01,
#'         base_path = save_path
#'       )
#'     }
#'
#'     copula_preds_list <- list()
#'
#'     for (i in 1:length(bin_var_col)) {
#'       copula_preds_list[[i]] <-
#'         Single_Asset_V3_Copula_read_Model(
#'           copula_data = copula_v3_data,
#'           asset_of_interest = Asset_of_interest,
#'           period_of_analysis = bin_var_col[i],
#'           training_end_date = date_train_end,
#'           roll_mean_period = 500,
#'           base_path = save_path
#'         )
#'     }
#'
#'     Copula_v3_Test_Preds <-
#'       copula_preds_list %>%
#'       map(~.x %>% pluck("testing_data")) %>%
#'       reduce(left_join)
#'
#'     Copula_v3_Train_Preds <-
#'       copula_preds_list %>%
#'       map(~.x %>% pluck("training_data")) %>%
#'       reduce(left_join)
#'
#'     rm(copula_preds_list)
#'
#'     Copula_v3_preds <-
#'       Copula_v3_Test_Preds %>%
#'       bind_rows(Copula_v3_Train_Preds) %>%
#'       dplyr::select(-contains("_mean"))%>%
#'       dplyr::select(-contains("_sd"))
#'
#'     names(Copula_v3_preds) <-
#'       names(Copula_v3_preds) %>%
#'       map(
#'         ~
#'           str_remove_all(.x, "period_return_") %>%
#'           str_replace_all(pattern = "Copula_", replacement = "Copulav3_")
#'       ) %>%
#'       unlist()
#'
#'     rm(Copula_v3_Test_Preds, Copula_v3_Train_Preds)
#'
#'     macro_data <-
#'       prepare_macro_indicator_model_data(
#'         asset_data = asset_data_internal,
#'         Asset_of_interest = Asset_of_interest,
#'         interest_rates = interest_rates,
#'         cpi_data = cpi_data,
#'         sentiment_index = sentiment_index,
#'         gdp_data = gdp_data,
#'         unemp_data = unemp_data,
#'         manufac_pmi = manufac_pmi,
#'         USD_Macro = USD_Macro,
#'         EUR_Macro = EUR_Macro,
#'         countries_for_int_strength = countries_for_int_strength,
#'         date_limit = now(tzone = "Australia/Canberra")
#'       )
#'
#'     macro_preds <-
#'       single_asset_read_models_and_get_pred(
#'         pred_data = macro_data,
#'         trade_direction = trade_direction,
#'         Asset_of_interest = Asset_of_interest,
#'         save_path = save_path,
#'         model_string = "_macro_"
#'       )
#'
#'     index_pca_data <-
#'       get_pca_index_indicator_data(
#'         asset_data = asset_data_internal,
#'         Asset_of_interest = Asset_of_interest,
#'         equity_index = equity_index,
#'         gold_index = gold_index,
#'         silver_index = silver_index,
#'         bonds_index = bonds_index,
#'         USD_index = USD_index,
#'         EUR_index = EUR_index,
#'         GBP_index = GBP_index,
#'         AUD_index = AUD_index,
#'         COMMOD_index = COMMOD_index,
#'         USD_STOCKS_index = USD_STOCKS_index,
#'         NZD_index = NZD_index,
#'         date_limit = now(tzone = "Australia/Canberra")
#'       )
#'
#'     index_preds <-
#'       single_asset_read_models_and_get_pred(
#'         pred_data = index_pca_data,
#'         trade_direction = trade_direction,
#'         Asset_of_interest = Asset_of_interest,
#'         save_path = save_path,
#'         model_string = "_index_"
#'       )
#'
#'     daily_data_for_modelling <-
#'       prepare_daily_indicator_data(
#'         asset_data = asset_data_internal,
#'         All_Daily_Data = All_Daily_Data,
#'         Asset_of_interest = Asset_of_interest,
#'         date_limit = now(tzone = "Australia/Canberra")
#'       ) %>%
#'       filter(Asset == Asset_of_interest) %>%
#'       dplyr::select(-Asset)
#'
#'     daily_preds <-
#'       single_asset_read_models_and_get_pred(
#'         pred_data = daily_data_for_modelling,
#'         trade_direction = trade_direction,
#'         Asset_of_interest = Asset_of_interest,
#'         save_path = save_path,
#'         model_string = "_daily_"
#'       )
#'
#'     copula_data <-
#'       prepare_copula_data(
#'         asset_data = asset_data,
#'         couplua_assets = couplua_assets,
#'         Asset_of_interest = Asset_of_interest
#'       )
#'
#'     copula_preds <-
#'       single_asset_read_models_and_get_pred(
#'         pred_data = copula_data,
#'         trade_direction = trade_direction,
#'         Asset_of_interest = Asset_of_interest,
#'         save_path = save_path,
#'         model_string = "_copula_"
#'       )
#'
#'     technical_data <-
#'       create_technical_indicators(asset_data = asset_data_internal) %>%
#'       dplyr::select(-Price, -Low, -High, -Open)
#'
#'     technical_data <-
#'       technical_data %>%
#'       filter(Asset == Asset_of_interest) %>%
#'       arrange(Date) %>%
#'       mutate(
#'         across(.cols = !contains("Date"),
#'                .fns = ~ lag(.) )
#'       ) %>%
#'       ungroup() %>%
#'       dplyr::select(-Asset)
#'
#'     technical_preds <-
#'       single_asset_read_models_and_get_pred(
#'         pred_data = technical_data,
#'         trade_direction = trade_direction,
#'         Asset_of_interest = Asset_of_interest,
#'         save_path = save_path,
#'         model_string = "_technical_"
#'       )
#'
#'     accumulating_probs <-
#'       asset_data_internal %>%
#'       distinct(Date, Asset) %>%
#'       left_join(macro_preds) %>%
#'       left_join(index_preds) %>%
#'       left_join(daily_preds) %>%
#'       left_join(copula_preds)%>%
#'       left_join(Copula_v3_preds) %>%
#'       left_join(technical_preds) %>%
#'       left_join(AR_preds) %>%
#'       left_join(macro_data) %>%
#'       left_join(copula_data)%>%
#'       left_join(copula_v3_data)%>%
#'       left_join(
#'         daily_data_for_modelling %>%
#'           dplyr::select(Date,
#'                         (contains("perc_line_")&contains("_20")),
#'                         contains("Support"),
#'                         contains("Resistance"),
#'                         contains("Bear"),
#'                         contains("Bull"),
#'                         (contains("perc_line_")&contains("_50")),
#'                         contains("moving_average_markov_")
#'           )
#'       ) %>%
#'       left_join(index_pca_data) %>%
#'       left_join(
#'         technical_data %>%
#'           dplyr::select(Date,
#'                         (contains("perc_line_")&contains("_500")),
#'                         contains("Support"),
#'                         contains("Resistance"),
#'                         contains("Bear"),
#'                         contains("Bull"),
#'                         (contains("perc_line_")&contains("_500")),
#'                         contains("moving_average_markov_")
#'           )
#'       ) %>%
#'       distinct()
#'
#'     combined_model_data <-
#'       accumulating_probs %>%
#'       ungroup() %>%
#'       arrange(Date) %>%
#'       fill(!contains("Date"), .direction = "down") %>%
#'       filter(if_all(.cols = everything(), ~!is.na(.)))
#'
#'     mean_sd_values <-
#'       readRDS(
#'         glue::glue("{save_path}/{Asset_of_interest}_{trade_direction}_mean_sd_values.RDS")
#'       ) %>%
#'       dplyr::select(-Asset_sd, -Asset_mean) %>%
#'       dplyr::select(-Date) %>%
#'       distinct() %>%
#'       group_by(Asset) %>%
#'       summarise(across(.cols = where(is.numeric),
#'                        .fns = ~ mean(., na.rm = T))) %>%
#'       ungroup()
#'
#'
#'     combined_preds <-
#'       single_asset_read_models_and_get_pred(
#'         pred_data = combined_model_data,
#'         trade_direction = trade_direction,
#'         Asset_of_interest = Asset_of_interest,
#'         save_path = save_path,
#'         model_string = "_combined_"
#'       ) %>%
#'       mutate(
#'         Asset = Asset_of_interest
#'       ) %>%
#'       left_join(
#'         accumulating_probs %>%
#'           dplyr::select(Date, Asset, contains("pred"))
#'       ) %>%
#'       left_join(mean_sd_values)
#'
#'     rm(macro_data, macro_phase_2_data, macro_train_data)
#'     rm(index_pca_data, index_pca_phase_2_data, index_pca_train_data)
#'     rm(daily_phase_2_data, daily_data_for_modelling, daily_data_for_modelling_train)
#'     rm(copula_data, copula_data_train, copula_phase_2_data)
#'     rm(technical_data, technical_data_train, technical_phase_2_data)
#'     gc()
#'
#'     return(combined_preds)
#'
#'   }
#'
#' #' Single_Asset_V3_get_all_preds
#' #'
#' #' @param Indices_Metals_Bonds
#' #' @param asset_of_interest
#' #' @param actuals_periods_needed
#' #' @param training_end_date
#' #' @param bin_threshold
#' #' @param rolling_mean_pred_period
#' #' @param correlation_rolling_periods
#' #' @param copula_assets
#' #' @param training_end_date
#' #' @param rolling_mean_pred_period
#' #' @param bin_threshold
#' #' @param start_index
#' #' @param end_index
#' #'
#' #' @return
#' #' @export
#' #'
#' #' @examples
#' Single_Asset_V3_get_all_preds <-
#'   function(
#'     Indices_Metals_Bonds = Indices_Metals_Bonds,
#'     actuals_periods_needed = c("period_return_35_Price", "period_return_46_Price"),
#'     correlation_rolling_periods = c(100,200, 300),
#'     training_end_date = "2025-05-01",
#'     rolling_mean_pred_period = 500,
#'     bin_threshold = 5,
#'     start_index = 1,
#'     end_index = 27,
#'     base_path = "C:/Users/Nikhil Chandra/Documents/trade_data/single_asset_models_v1/"
#'   ) {
#'
#'     indicator_mapping <- list(
#'       Asset = c(
#'         # "EUR_CHF", #1 EUR_CHF
#'         "EUR_SEK" , #2 EUR_SEK
#'         "GBP_CHF", #3 GBP_CHF
#'         "GBP_JPY", #4 GBP_JPY
#'         "USD_CZK", #5 USD_CZK
#'         "USD_NOK" , #6 USD_NOK
#'         "XAG_CAD", #7 XAG_CAD
#'         "XAG_CHF", #8 XAG_CHF
#'         "XAG_JPY" , #9 XAG_JPY
#'         "GBP_NZD" , #10 GBP_NZD
#'         "NZD_CHF" , #11 NZD_CHF
#'         "USD_MXN" , #12 USD_MXN
#'         # "XPD_USD" , #13 XPD_USD
#'         # "XPT_USD" , #14 XPT_USD
#'         "NATGAS_USD" , #15 NATGAS_USD
#'         "SG30_SGD" , #16 SG30_SGD
#'         "SOYBN_USD" , #17 SOYBN_USD
#'         # "WHEAT_USD" , #18 WHEAT_USD
#'         # "SUGAR_USD" , #19 SUGAR_USD
#'         "DE30_EUR" , #20 DE30_EUR
#'         "UK10YB_GBP" , #21 UK10YB_GBP
#'         "JP225_USD" , #22 JP225_USD
#'         # "CH20_CHF" , #23 CH20_CHF
#'         "NL25_EUR" , #24 NL25_EUR
#'         "XAG_SGD" , #25 XAG_SGD
#'         "BCH_USD" , #26 BCH_USD
#'         "LTC_USD" ), #27 LTC_USD
#'       couplua_assets =
#'         list(
#'           # c(
#'           #   "EUR_SEK", "DE30_EUR", "XAG_CHF", "EUR_USD", "EU50_EUR", "XAG_EUR", "XAU_EUR",
#'           #   "EUR_AUD", "EUR_JPY", "FR40_EUR", "GBP_CHF", "NZD_CHF", "CH20_CHF", "XAU_USD"
#'           # ) %>% unique() , #1 EUR_CHF
#'
#'           c("EUR_CHF", "DE30_EUR", "NL25_EUR", "EUR_USD", "EU50_EUR", "XAG_EUR", "XAU_EUR",
#'             "EUR_AUD", "EUR_JPY", "FR40_EUR", "XAU_USD") %>% unique(), #2 EUR_SEK
#'
#'           c("GBP_JPY", "GBP_NZD", "UK10YB_GBP", "GBP_USD", "GBP_AUD", "XAG_GBP", "XAU_GBP",
#'             "UK100_GBP", "EUR_JPY", "FR40_EUR", "EUR_USD",  "EUR_CHF", "NZD_CHF", "CH20_CHF",
#'             "XAU_USD") %>% unique(), #3 GBP_CHF
#'
#'           c("GBP_CHF", "GBP_NZD", "UK10YB_GBP", "GBP_USD", "GBP_AUD", "XAG_GBP", "XAU_GBP",
#'             "UK100_GBP", "XAG_JPY", "USD_JPY", "EUR_JPY", "XAU_JPY", "XAU_USD") %>% unique(), #4 GBP_JPY
#'
#'           c("USD_NOK", "EUR_USD", "USD_JPY", "AUD_USD", "XAG_USD", "XAU_USD", "GBP_USD",
#'             "USD_CAD", "USD_SEK", "NZD_USD", "EUR_SEK") %>% unique(), #5 USD_CZK
#'
#'           c("USD_NOK", "EUR_USD", "USD_JPY", "AUD_USD", "XAG_USD", "XAU_USD", "GBP_USD",
#'             "USD_CAD", "USD_SEK", "NZD_USD", "EUR_SEK") %>% unique(), #6 USD_NOK
#'
#'           c("XAG_CHF", "XAG_JPY", "XAG_SGD", "XAG_USD", "XAG_EUR", "XAG_GBP", "XAG_NZD",
#'             "XAG_AUD", "XAU_USD", "XAU_EUR", "XAU_GBP",
#'             "XAU_SGD", "XAU_CAD", "XAU_NZD", "XAU_AUD") %>% unique(), #7 XAG_CAD
#'
#'           c("XAG_CAD", "XAG_JPY", "XAG_SGD", "XAG_USD", "XAG_EUR", "XAG_GBP", "XAG_NZD",
#'             "XAG_AUD", "XAU_USD", "XAU_EUR", "XAU_GBP",
#'             "XAU_SGD", "XAU_CAD", "XAU_NZD", "XAU_AUD") %>% unique(), #8 XAG_CHF
#'
#'           c("XAG_CAD", "XAG_CHF", "XAG_SGD", "XAG_USD", "XAG_EUR", "XAG_GBP", "XAG_NZD",
#'             "XAG_AUD", "XAU_USD", "XAU_EUR", "XAU_GBP",
#'             "XAU_SGD", "XAU_CAD", "XAU_NZD", "XAU_AUD") %>% unique(), #9 XAG_JPY
#'
#'           c("GBP_CHF", "GBP_JPY", "UK10YB_GBP", "GBP_USD", "GBP_AUD", "XAG_GBP", "XAU_GBP",
#'             "UK100_GBP", "NZD_CHF", "NZD_USD", "XAU_NZD", "XAG_NZD") %>% unique(), #10 GBP_NZD
#'
#'           c( "GBP_NZD", "NZD_USD", "XAU_NZD", "XAG_NZD", "EUR_CHF", "GBP_CHF", "XAG_CHF",
#'              "CH20_CHF") %>% unique(), #11 NZD_CHF
#'
#'           c( "USD_NOK", "EUR_USD", "USD_JPY", "AUD_USD", "XAG_USD", "XAU_USD", "GBP_USD",
#'              "USD_CAD", "USD_SEK", "NZD_USD") %>% unique(), #12 USD_MXN
#'
#'           # c( "USD_NOK", "EUR_USD", "USD_JPY", "AUD_USD", "XAG_USD", "XAU_USD", "GBP_USD",
#'           #    "USD_CAD", "USD_SEK", "NZD_USD",
#'           #    "NATGAS_USD", "XPT_USD", "USB10Y_USD") %>% unique(), #13 XPD_USD
#'
#'           # c("USD_NOK", "EUR_USD", "USD_JPY", "AUD_USD", "XAG_USD", "XAU_USD", "GBP_USD",
#'           #   "USD_CAD", "USD_SEK", "NZD_USD",
#'           #   "NATGAS_USD", "XPD_USD", "USB10Y_USD") %>% unique(), #14 XPT_USD
#'
#'           c(
#'             "USD_NOK", "EUR_USD", "USD_JPY", "AUD_USD", "XAG_USD", "XAU_USD", "GBP_USD",
#'             "USD_CAD", "USD_SEK", "NZD_USD",
#'             "XPT_USD", "XPD_USD", "USB10Y_USD"
#'           ) %>% unique(), #15 NATGAS_USD
#'
#'           c("USB10Y_USD", "USD_SGD", "XAU_SGD", "XAG_SGD", "AU200_AUD", "US2000_USD", "SPX500_USD",
#'             "NL25_EUR", "NL25_EUR", "FR40_EUR", "EU50_EUR", "JP225_USD", "XPT_USD", "XAU_USD", "DE30_EUR",
#'             "CH20_CHF") %>% unique(), #16 SG30_SGD
#'
#'           c(
#'             "USD_NOK", "EUR_USD", "USD_JPY", "AUD_USD", "XAG_USD", "XAU_USD", "GBP_USD",
#'             "USD_CAD", "USD_SEK", "NZD_USD", "NATGAS_USD", "XPT_USD", "USB10Y_USD", "WHEAT_USD",
#'             "SUGAR_USD","SPX500_USD", "US2000_USD"
#'           ) %>% unique(), #17 SOYBN_USD
#'
#'           # c(
#'           #   "USD_NOK", "EUR_USD", "USD_JPY", "AUD_USD", "XAG_USD", "XAU_USD", "GBP_USD",
#'           #   "USD_CAD", "USD_SEK", "NZD_USD", "NATGAS_USD", "XPT_USD", "USB10Y_USD", "SOYBN_USD",
#'           #   "SUGAR_USD","SPX500_USD", "US2000_USD"
#'           # ) %>% unique(), #18 WHEAT_USD #####HERE
#'
#'           # c(
#'           #   "USD_NOK", "EUR_USD", "USD_JPY", "AUD_USD", "XAG_USD", "XAU_USD", "GBP_USD",
#'           #   "USD_CAD", "NZD_USD", "NATGAS_USD", "XPT_USD", "USB10Y_USD", "SOYBN_USD",
#'           #   "WHEAT_USD","SPX500_USD", "US2000_USD"
#'           # ) %>% unique(), #19 SUGAR_USD
#'
#'           c(
#'             "USB10Y_USD", "USD_SGD", "XAU_USD", "XAG_EUR", "AU200_AUD", "US2000_USD", "SPX500_USD",
#'             "NL25_EUR", "NL25_EUR", "FR40_EUR", "EU50_EUR", "JP225_USD", "XAG_USD",
#'             "CH20_CHF", "XAU_EUR", "EUR_USD"
#'           ) %>% unique(), #20 DE30_EUR
#'
#'           c(
#'             "XAG_GBP", "AU200_AUD", "US2000_USD", "SPX500_USD",
#'             "NL25_EUR", "NL25_EUR", "FR40_EUR", "EU50_EUR", "JP225_USD", "XAG_USD",
#'             "CH20_CHF", "XAU_GBP", "GBP_USD", "UK100_GBP"
#'           ) %>% unique(), #21 UK10YB_GBP
#'
#'           c(
#'             "USB10Y_USD", "USD_SGD", "XAU_USD", "XAG_EUR", "AU200_AUD", "US2000_USD", "SPX500_USD",
#'             "NL25_EUR", "NL25_EUR", "FR40_EUR", "EU50_EUR", "DE30_EUR", "XAG_USD",
#'             "CH20_CHF", "XAU_EUR", "EUR_USD"
#'           ) %>% unique(), #22 JP225_USD
#'
#'           # c(
#'           #   "USB10Y_USD", "USD_SGD", "XAU_USD", "XAG_CHF", "AU200_AUD", "US2000_USD", "SPX500_USD",
#'           #   "NL25_EUR", "NL25_EUR", "FR40_EUR", "EU50_EUR", "DE30_EUR", "XAG_USD",
#'           #   "JP225_USD", "XAU_CHF", "EUR_CHF"
#'           # ) %>% unique(), #23 CH20_CHF
#'
#'           c(
#'             "USB10Y_USD", "USD_SGD", "XAU_USD", "XAG_CHF", "AU200_AUD", "US2000_USD", "SPX500_USD",
#'             "CH20_CHF", "FR40_EUR", "EU50_EUR", "DE30_EUR", "XAG_USD",
#'             "JP225_USD", "XAU_CHF", "EUR_CHF"
#'           ) %>% unique(), #24 NL25_EUR
#'
#'           c("XAG_CAD", "XAG_JPY", "XAG_CHF", "XAG_USD", "XAG_EUR", "XAG_GBP", "XAG_NZD",
#'             "XAG_AUD", "XAU_USD", "XAU_EUR", "XAU_GBP",
#'             "XAU_SGD", "XAU_CAD", "XAU_NZD", "XAU_AUD") %>% unique(), #25 XAG_SGD
#'
#'           c(
#'             "USB10Y_USD", "USD_SGD", "XAU_USD", "BTC_USD", "LTC_USD", "US2000_USD", "SPX500_USD",
#'             "NL25_EUR", "NL25_EUR", "FR40_EUR", "EU50_EUR", "JP225_USD", "XAG_USD",
#'             "CH20_CHF", "XAU_GBP", "GBP_USD", "UK100_GBP"
#'           ) %>% unique(), #26 BCH_USD
#'
#'           c(
#'             "USB10Y_USD", "USD_SGD", "XAU_USD", "BTC_USD", "BCH_USD", "US2000_USD", "SPX500_USD",
#'             "NL25_EUR", "NL25_EUR", "FR40_EUR", "EU50_EUR", "JP225_USD", "XAG_USD",
#'             "CH20_CHF", "XAU_GBP", "GBP_USD", "UK100_GBP"
#'           ) %>% unique() #27 LTC_USD
#'
#'         ),
#'       countries_for_int_strength =
#'         list(
#'
#'           # c("GBP", "USD", "EUR", "AUD", "JPY"),  #1
#'           c("GBP", "USD", "EUR", "AUD", "JPY"),  #2
#'           c("GBP", "USD", "EUR", "AUD", "JPY"),  #3
#'           c("GBP", "USD", "EUR", "AUD", "JPY"),  #4
#'           c("GBP", "USD", "EUR", "AUD", "JPY"),  #5
#'           c("GBP", "USD", "EUR", "AUD", "JPY"),  #6
#'           c("GBP", "USD", "EUR", "AUD", "JPY"),  #7
#'           c("GBP", "USD", "EUR", "AUD", "JPY"),  #8
#'           c("GBP", "USD", "EUR", "AUD", "JPY"),  #9
#'           c("GBP", "USD", "EUR", "AUD", "JPY"),  #10
#'           c("GBP", "USD", "EUR", "AUD", "JPY"),  #11
#'           c("GBP", "USD", "EUR", "AUD", "JPY"),  #12
#'           # c("GBP", "USD", "EUR", "AUD", "JPY"),  #13
#'           # c("GBP", "USD", "EUR", "AUD", "JPY"),  #14
#'           c("GBP", "USD", "EUR", "AUD", "JPY"),  #15
#'           c("GBP", "USD", "EUR", "AUD", "JPY"),  #16
#'           c("GBP", "USD", "EUR", "AUD", "JPY"),  #17
#'           c("GBP", "USD", "EUR", "AUD", "JPY"),  #18
#'           # c("GBP", "USD", "EUR", "AUD", "JPY"),  #19
#'           c("GBP", "USD", "EUR", "AUD", "JPY"),  #20
#'           c("GBP", "USD", "EUR", "AUD", "JPY"),  #21
#'           c("GBP", "USD", "EUR", "AUD", "JPY"),  #22
#'           # c("GBP", "USD", "EUR", "AUD", "JPY"),  #23
#'           c("GBP", "USD", "EUR", "AUD", "JPY"),  #24
#'           c("GBP", "USD", "EUR", "AUD", "JPY"),  #25
#'           c("GBP", "USD", "EUR", "AUD", "JPY"),  #26
#'           c("GBP", "USD", "EUR", "AUD", "JPY")  #27
#'
#'         )
#'     )
#'
#'     all_assets <-
#'       indicator_mapping$Asset
#'
#'     raw_base_preds <-
#'       list()
#'
#'     for (i in start_index:end_index ) {
#'       tictoc::tic()
#'       asset_loop <- indicator_mapping$Asset[i]
#'       copula_assets <- indicator_mapping$couplua_assets[[i]]
#'
#'
#'       pred_generated <-
#'         Single_Asset_V3_Read_in_Probs(
#'           Indices_Metals_Bonds = Indices_Metals_Bonds,
#'           asset_of_interest = asset_loop,
#'           actuals_periods_needed = actuals_periods_needed,
#'           training_end_date = training_end_date,
#'           bin_threshold = bin_threshold,
#'           rolling_mean_pred_period = rolling_mean_pred_period,
#'           correlation_rolling_periods = correlation_rolling_periods,
#'           copula_assets = copula_assets,
#'           base_path = base_path
#'         )
#'
#'       raw_base_preds[[i]] <-
#'         pred_generated %>%
#'         pluck("complete_preds_test") %>%
#'         mutate(
#'           training_end_date = training_end_date,
#'           rolling_mean_pred_period = rolling_mean_pred_period,
#'           bin_threshold = bin_threshold
#'         )
#'       tictoc::toc()
#'     }
#'
#'
#'     returned <-
#'       raw_base_preds %>%
#'       map_dfr(bind_rows) %>%
#'       ungroup() %>%
#'       mutate(
#'         averaged_35_LM_pred =
#'           (state_space_LM_Pred_period_return_35_Price +
#'              AR_LM_Pred_period_return_35_Price +
#'              Copula_LM_Pred_period_return_35_Price)/3,
#'
#'         averaged_35_GLM_pred =
#'           (state_space_GLM_Pred_period_return_35_Price +
#'              AR_GLM_Pred_period_return_35_Price +
#'              Copula_GLM_Pred_period_return_35_Price)/3,
#'
#'         averaged_35_46_GLM_pred =
#'           (state_space_GLM_Pred_period_return_35_Price +
#'              AR_GLM_Pred_period_return_35_Price +
#'              Copula_GLM_Pred_period_return_35_Price +
#'              state_space_GLM_Pred_period_return_46_Price +
#'              AR_GLM_Pred_period_return_46_Price +
#'              Copula_GLM_Pred_period_return_46_Price)/6,
#'
#'         averaged_35_46_LM_pred =
#'           (state_space_LM_Pred_period_return_35_Price +
#'              AR_LM_Pred_period_return_35_Price +
#'              Copula_LM_Pred_period_return_35_Price +
#'              state_space_LM_Pred_period_return_46_Price +
#'              AR_LM_Pred_period_return_46_Price +
#'              Copula_LM_Pred_period_return_46_Price)/6
#'       )
#'
#'     return(returned)
#'
#'   }
#'
#' #' Single_Asset_V3_Gen_Model
#' #'
#' #' @param Indices_Metals_Bonds
#' #' @param actual_wins_losses
#' #' @param asset_of_interest
#' #' @param actuals_periods_needed
#' #' @param training_end_date
#' #'
#' #' @return
#' #' @export
#' #'
#' #' @examples
#' Single_Asset_V3_Gen_Model <-
#'   function(Indices_Metals_Bonds,
#'            actual_wins_losses,
#'            asset_of_interest = "GBP_JPY",
#'            actuals_periods_needed = c("period_return_24_Price", "period_return_35_Price", "period_return_46_Price"),
#'            training_end_date = "2025-05-01",
#'            bin_threshold = 5,
#'            rolling_mean_pred_period = 500,
#'            correlation_rolling_periods = c(100,200, 300),
#'            copula_assets = c("GBP_USD", "EUR_JPY", "USD_JPY", "XAU_JPY", "GBP_CHF", "XAG_GBP", "GBP_NZD", "UK100_GBP", "EUR_USD", "GBP_AUD"),
#'            base_path = "C:/Users/Nikhil Chandra/Documents/trade_data/single_asset_models_v1/") {
#'
#'     asset_data = Indices_Metals_Bonds[[1]] %>% filter(Asset == asset_of_interest)
#'     actual_wins_losses_asset <- actual_wins_losses %>% filter(Asset == asset_of_interest)
#'
#'     AR_model_data <-
#'       Single_Asset_V3_AR_Model_data(
#'         asset_data = asset_data,
#'         asset_of_interest = asset_of_interest,
#'         lag_value_1 = 10,
#'         lag_value_2 = 20,
#'         lag_value_3 = 30,
#'         lag_value_4 = 40,
#'         lag_value_5 = 50,
#'         lag_value_6 = 60,
#'         lag_value_7 = 70,
#'         MA_period_1 = 10,
#'         MA_period_2 = 20,
#'         MA_period_3 = 30,
#'         MA_period_4 = 40,
#'         MA_period_5 = 20,
#'         MA_period_6 = 20
#'       )
#'
#'     for (i in 1:length(actuals_periods_needed)) {
#'       Single_Asset_V3_AR_Gen_Model(
#'         AR_model_data = AR_model_data,
#'         asset_of_interest = asset_of_interest,
#'         actual_wins_losses_asset = actual_wins_losses_asset,
#'         period_of_analysis = actuals_periods_needed[i],
#'         training_end_date = training_end_date,
#'         bin_threshold = bin_threshold,
#'         sig_thresh = 0.15,
#'         base_path = base_path
#'       )
#'     }
#'
#'     AR_preds_list <- list()
#'
#'     for (i in 1:length(actuals_periods_needed)) {
#'       AR_preds_list[[i]] <-
#'         Single_Asset_V3_AR_read_model(
#'           AR_model_data = AR_model_data,
#'           asset_of_interest = asset_of_interest,
#'           period_of_analysis = actuals_periods_needed[i],
#'           training_end_date = training_end_date,
#'           roll_mean_period = rolling_mean_pred_period,
#'           base_path = base_path
#'         )
#'     }
#'
#'     AR_Train_Preds_mean <-
#'       AR_preds_list %>%
#'       map(~.x %>% pluck("training_data")) %>%
#'       reduce(left_join)
#'
#'     AR_Test_Preds <-
#'       AR_preds_list %>%
#'       map(~.x %>% pluck("testing_data")) %>%
#'       reduce(left_join)
#'
#'     rm(AR_preds_list)
#'
#'     copula_list <- list()
#'
#'     for (i in 1:length(correlation_rolling_periods)) {
#'
#'       copula_list[[i]] <-
#'         Single_Asset_V3_Cop_data(
#'           All_Asset_Data =
#'             Indices_Metals_Bonds[[1]] %>%
#'             filter(Asset == asset_of_interest| Asset %in% copula_assets),
#'           asset_of_interest = asset_of_interest,
#'           copula_assets = copula_assets,
#'           rolling_period_cor = correlation_rolling_periods[i]
#'         )
#'
#'     }
#'
#'     copula_data <- copula_list %>% reduce(left_join)
#'
#'     for (i in 1:length(actuals_periods_needed)) {
#'       Single_Asset_V3_Copula_Gen_Model(
#'         copula_data = copula_data,
#'         asset_of_interest = asset_of_interest,
#'         actual_wins_losses_asset = actual_wins_losses_asset,
#'         period_of_analysis = actuals_periods_needed[i],
#'         training_end_date = training_end_date,
#'         bin_threshold = bin_threshold,
#'         sig_thresh = 0.01,
#'         base_path = base_path
#'       )
#'     }
#'
#'     copula_preds_list <- list()
#'
#'     for (i in 1:length(actuals_periods_needed)) {
#'       copula_preds_list[[i]] <-
#'         Single_Asset_V3_Copula_read_Model(
#'           copula_data = copula_data,
#'           asset_of_interest = asset_of_interest,
#'           period_of_analysis = actuals_periods_needed[i],
#'           training_end_date = training_end_date,
#'           roll_mean_period = rolling_mean_pred_period,
#'           base_path = base_path
#'         )
#'     }
#'
#'     Copula_Train_Preds_mean <-
#'       copula_preds_list %>%
#'       map(~.x %>% pluck("training_data")) %>%
#'       reduce(left_join)
#'
#'     Copula_Test_Preds <-
#'       copula_preds_list %>%
#'       map(~.x %>% pluck("testing_data")) %>%
#'       reduce(left_join)
#'
#'     rm(copula_preds_list)
#'
#'     state_space_list <- list()
#'     loop_list_cols <- c("Price", "Low", "High")
#'     state_space_periods = c(20, 40, 60, 100, 200)
#'     state_space_rolling = c(100, 200)
#'     c = 0
#'
#'     for (j in 1:length(loop_list_cols) ) {
#'       for (i in 1:length(state_space_periods)) {
#'         for (k in 1:length(state_space_rolling)) {
#'           c = c + 1
#'           state_space_list[[c]] <-
#'             Single_Asset_V3_state_space(
#'               asset_data = asset_data,
#'               asset_of_interest = asset_of_interest,
#'               Price_diff_lag = state_space_periods[i],
#'               roll_period_state_space = state_space_rolling[k],
#'               price_col = loop_list_cols[j]
#'             )
#'         }
#'       }
#'     }
#'
#'     state_space_data <-
#'       state_space_list %>%
#'       reduce(left_join)
#'
#'     for (i in 1:length(actuals_periods_needed)) {
#'       Single_Asset_V3_state_space_Gen_Model(
#'         state_space_data = state_space_data,
#'         asset_of_interest = asset_of_interest,
#'         actual_wins_losses_asset = actual_wins_losses_asset,
#'         period_of_analysis = actuals_periods_needed[i],
#'         training_end_date = training_end_date,
#'         bin_threshold = bin_threshold,
#'         sig_thresh = 0.01,
#'         base_path = base_path
#'       )
#'     }
#'
#'     state_space_preds_list <- list()
#'
#'     for (i in 1:length(actuals_periods_needed)) {
#'       state_space_preds_list[[i]] <-
#'         Single_Asset_V3_state_space_read_Model(
#'           state_space_data = state_space_data,
#'           asset_of_interest = asset_of_interest,
#'           period_of_analysis = actuals_periods_needed[i],
#'           training_end_date = training_end_date,
#'           roll_mean_period = rolling_mean_pred_period,
#'           base_path = base_path
#'         )
#'     }
#'
#'     state_space_Train_Preds_mean <-
#'       state_space_preds_list %>%
#'       map(~.x %>% pluck("training_data")) %>%
#'       reduce(left_join)
#'
#'     state_space_Test_Preds <-
#'       state_space_preds_list %>%
#'       map(~.x %>% pluck("testing_data")) %>%
#'       reduce(left_join)
#'
#'
#'     complete_preds_train <-
#'       AR_Train_Preds_mean %>%
#'       left_join(
#'         Copula_Train_Preds_mean
#'       ) %>%
#'       left_join(
#'         state_space_Train_Preds_mean
#'       )
#'
#'     first_non_NA_date <-
#'       complete_preds_train %>%
#'       filter(if_all(everything(), ~!is.na(.))) %>%
#'       pull(Date) %>%
#'       min(na.rm = T)
#'
#'
#'     complete_preds_train <-
#'       complete_preds_train %>%
#'       filter(Date >= first_non_NA_date)
#'
#'     complete_preds_test <-
#'       AR_Test_Preds %>%
#'       left_join(
#'         Copula_Test_Preds
#'       ) %>%
#'       left_join(
#'         state_space_Test_Preds
#'       )
#'
#'     return(
#'       list(
#'         "complete_preds_test" = complete_preds_test,
#'         "complete_preds_train" = complete_preds_train
#'       )
#'     )
#'
#'   }
#'
#' #' Single_Asset_V3_Read_in_Probs
#' #'
#' #' @param Indices_Metals_Bonds
#' #' @param actual_wins_losses
#' #' @param asset_of_interest
#' #' @param actuals_periods_needed
#' #' @param training_end_date
#' #'
#' #' @return
#' #' @export
#' #'
#' #' @examples
#' Single_Asset_V3_Read_in_Probs <-
#'   function(Indices_Metals_Bonds,
#'            asset_of_interest = "GBP_JPY",
#'            actuals_periods_needed = c("period_return_24_Price", "period_return_35_Price", "period_return_46_Price"),
#'            training_end_date = "2025-05-01",
#'            bin_threshold = 5,
#'            rolling_mean_pred_period = 500,
#'            correlation_rolling_periods = c(100,200, 300),
#'            copula_assets = c("GBP_USD", "EUR_JPY", "USD_JPY", "XAU_JPY", "GBP_CHF", "XAG_GBP", "GBP_NZD", "UK100_GBP", "EUR_USD", "GBP_AUD"),
#'            base_path = "C:/Users/Nikhil Chandra/Documents/trade_data/single_asset_models_v1/" ) {
#'
#'     asset_data = Indices_Metals_Bonds[[1]] %>% filter(Asset == asset_of_interest)
#'
#'     AR_model_data <-
#'       Single_Asset_V3_AR_Model_data(
#'         asset_data = asset_data,
#'         asset_of_interest = asset_of_interest,
#'         lag_value_1 = 10,
#'         lag_value_2 = 20,
#'         lag_value_3 = 30,
#'         lag_value_4 = 40,
#'         lag_value_5 = 50,
#'         lag_value_6 = 60,
#'         lag_value_7 = 70,
#'         MA_period_1 = 10,
#'         MA_period_2 = 20,
#'         MA_period_3 = 30,
#'         MA_period_4 = 40,
#'         MA_period_5 = 20,
#'         MA_period_6 = 20
#'       )
#'
#'     AR_preds_list <- list()
#'
#'     for (i in 1:length(actuals_periods_needed)) {
#'       AR_preds_list[[i]] <-
#'         Single_Asset_V3_AR_read_model(
#'           AR_model_data = AR_model_data,
#'           asset_of_interest = asset_of_interest,
#'           period_of_analysis = actuals_periods_needed[i],
#'           training_end_date = training_end_date,
#'           roll_mean_period = rolling_mean_pred_period,
#'           base_path = base_path
#'         )
#'     }
#'
#'     AR_Train_Preds_mean <-
#'       AR_preds_list %>%
#'       map(~.x %>% pluck("training_data")) %>%
#'       reduce(left_join)
#'
#'     AR_Test_Preds <-
#'       AR_preds_list %>%
#'       map(~.x %>% pluck("testing_data")) %>%
#'       reduce(left_join)
#'
#'     rm(AR_preds_list)
#'
#'     copula_list <- list()
#'
#'     for (i in 1:length(correlation_rolling_periods)) {
#'
#'       copula_list[[i]] <-
#'         Single_Asset_V3_Cop_data(
#'           All_Asset_Data =
#'             Indices_Metals_Bonds[[1]] %>%
#'             filter(Asset == asset_of_interest| Asset %in% copula_assets),
#'           asset_of_interest = asset_of_interest,
#'           copula_assets = copula_assets,
#'           rolling_period_cor = correlation_rolling_periods[i]
#'         )
#'
#'     }
#'
#'     copula_data <- copula_list %>% reduce(left_join)
#'
#'     copula_preds_list <- list()
#'
#'     for (i in 1:length(actuals_periods_needed)) {
#'       copula_preds_list[[i]] <-
#'         Single_Asset_V3_Copula_read_Model(
#'           copula_data = copula_data,
#'           asset_of_interest = asset_of_interest,
#'           period_of_analysis = actuals_periods_needed[i],
#'           training_end_date = training_end_date,
#'           roll_mean_period = rolling_mean_pred_period,
#'           base_path = base_path
#'         )
#'     }
#'
#'     Copula_Train_Preds_mean <-
#'       copula_preds_list %>%
#'       map(~.x %>% pluck("training_data")) %>%
#'       reduce(left_join)
#'
#'     Copula_Test_Preds <-
#'       copula_preds_list %>%
#'       map(~.x %>% pluck("testing_data")) %>%
#'       reduce(left_join)
#'
#'     rm(copula_preds_list)
#'
#'     state_space_list <- list()
#'     loop_list_cols <- c("Price", "Low", "High")
#'     state_space_periods = c(20, 40, 60, 100, 200)
#'     state_space_rolling = c(100, 200)
#'     c = 0
#'
#'     for (j in 1:length(loop_list_cols) ) {
#'       for (i in 1:length(state_space_periods)) {
#'         for (k in 1:length(state_space_rolling)) {
#'           c = c + 1
#'           state_space_list[[c]] <-
#'             Single_Asset_V3_state_space(
#'               asset_data = asset_data,
#'               asset_of_interest = asset_of_interest,
#'               Price_diff_lag = state_space_periods[i],
#'               roll_period_state_space = state_space_rolling[k],
#'               price_col = loop_list_cols[j]
#'             )
#'         }
#'       }
#'     }
#'
#'     state_space_data <-
#'       state_space_list %>%
#'       reduce(left_join)
#'
#'     state_space_preds_list <- list()
#'
#'     for (i in 1:length(actuals_periods_needed)) {
#'       state_space_preds_list[[i]] <-
#'         Single_Asset_V3_state_space_read_Model(
#'           state_space_data = state_space_data,
#'           asset_of_interest = asset_of_interest,
#'           period_of_analysis = actuals_periods_needed[i],
#'           training_end_date = training_end_date,
#'           roll_mean_period = rolling_mean_pred_period,
#'           base_path = base_path
#'         )
#'     }
#'
#'     state_space_Train_Preds_mean <-
#'       state_space_preds_list %>%
#'       map(~.x %>% pluck("training_data")) %>%
#'       reduce(left_join)
#'
#'     state_space_Test_Preds <-
#'       state_space_preds_list %>%
#'       map(~.x %>% pluck("testing_data")) %>%
#'       reduce(left_join)
#'
#'
#'     complete_preds_train <-
#'       AR_Train_Preds_mean %>%
#'       left_join(
#'         Copula_Train_Preds_mean
#'       ) %>%
#'       left_join(
#'         state_space_Train_Preds_mean
#'       )
#'
#'     first_non_NA_date <-
#'       complete_preds_train %>%
#'       filter(if_all(everything(), ~!is.na(.))) %>%
#'       pull(Date) %>%
#'       min(na.rm = T)
#'
#'
#'     complete_preds_train <-
#'       complete_preds_train %>%
#'       filter(Date >= first_non_NA_date)
#'
#'     complete_preds_test <-
#'       AR_Test_Preds %>%
#'       left_join(
#'         Copula_Test_Preds
#'       ) %>%
#'       left_join(
#'         state_space_Test_Preds
#'       )
#'
#'     return(
#'       list(
#'         "complete_preds_test" = complete_preds_test,
#'         "complete_preds_train" = complete_preds_train
#'       )
#'     )
#'
#'   }
#'
#' #' Single_Asset_V3_state_space
#' #'
#' #' @return
#' #' @export
#' #'
#' #' @examples
#' Single_Asset_V3_state_space <-
#'   function(
#'     asset_data = asset_data,
#'     asset_of_interest = asset_of_interest,
#'     Price_diff_lag = 20,
#'     roll_period_state_space = 100,
#'     price_col = "Price"
#'   ) {
#'
#'     state_space_dat <-
#'       asset_data %>%
#'       group_by(Asset) %>%
#'       arrange(Date) %>%
#'       mutate(
#'         Price_diff = lag(!!as.name(price_col)) - lag(!!as.name(price_col), Price_diff_lag),
#'         state_space_mean =
#'           slider::slide_dbl(.x = Price_diff, .f = ~ mean(.x, na.rm = T), .before = roll_period_state_space),
#'         state_space_sd =
#'           slider::slide_dbl(.x = Price_diff, .f = ~ sd(.x, na.rm = T), .before = roll_period_state_space)
#'         # state_space_sd =
#'         #   slider::slide_dbl(.x = Price_diff, .f = ~ sd(.x, na.rm = T), .before = roll_period_state_space)
#'
#'       ) %>%
#'       mutate(
#'
#'         state_space_min =
#'           case_when(
#'             Price_diff <= state_space_mean - state_space_sd*2.5 ~ 1,
#'             TRUE ~ 0
#'           ),
#'
#'         state_space_lowest =
#'           case_when(
#'             Price_diff > state_space_mean - state_space_sd*2.5 & Price_diff <= state_space_mean - state_space_sd*1.5 ~ 1,
#'             TRUE ~ 0
#'           ),
#'         state_space_second_lowest =
#'           case_when(
#'             Price_diff > state_space_mean - state_space_sd*1.5 & Price_diff <= state_space_mean - state_space_sd*1 ~ 1,
#'             TRUE ~ 0
#'           ),
#'         state_space_third_lowest =
#'           case_when(
#'             Price_diff > state_space_mean - state_space_sd*1 & Price_diff <= state_space_mean - state_space_sd*0 ~ 1,
#'             TRUE ~ 0
#'           ),
#'         state_space_third_highest =
#'           case_when(
#'             Price_diff > state_space_mean + state_space_sd*0 & Price_diff <= state_space_mean + state_space_sd*1 ~ 1,
#'             TRUE ~ 0
#'           ),
#'         state_space_second_highest =
#'           case_when(
#'             Price_diff > state_space_mean + state_space_sd*1 & Price_diff <= state_space_mean + state_space_sd*1.5 ~ 1,
#'             TRUE ~ 0
#'           ),
#'         state_space_highest =
#'           case_when(
#'             Price_diff > state_space_mean + state_space_sd*1.5 & Price_diff <= state_space_mean + state_space_sd*2.5 ~ 1,
#'             TRUE ~ 0
#'           ),
#'         state_space_max =
#'           case_when(
#'             Price_diff > state_space_mean + state_space_sd*2.5 ~ 1,
#'             TRUE ~ 0
#'           )
#'
#'       ) %>%
#'       mutate(
#'         across(
#'           .cols = c(state_space_max, state_space_highest, state_space_second_highest, state_space_third_highest,
#'                     state_space_third_lowest, state_space_second_lowest, state_space_lowest, state_space_min),
#'           .fns = ~
#'             slider::slide_dbl(.x = ., .f = ~ sum(.x, na.rm = T), .before = roll_period_state_space)
#'         )
#'       ) %>%
#'       filter(!is.na(Price_diff)) %>%
#'       mutate(
#'         total_state_space =
#'           state_space_max + state_space_highest + state_space_second_highest + state_space_third_highest +
#'           state_space_third_lowest + state_space_second_lowest + state_space_lowest + state_space_min,
#'
#'         !!as.name( glue::glue("perc_space_max_{Price_diff_lag}_{roll_period_state_space}_{price_col}") ) :=
#'           state_space_max/total_state_space,
#'         !!as.name( glue::glue("perc_space_highest_{Price_diff_lag}_{roll_period_state_space}_{price_col}") ) :=
#'           state_space_highest/total_state_space,
#'         !!as.name( glue::glue("perc_space_second_highest_{Price_diff_lag}_{roll_period_state_space}_{price_col}") ) :=
#'           state_space_second_highest/total_state_space,
#'         !!as.name( glue::glue("perc_space_third_highest_{Price_diff_lag}_{roll_period_state_space}_{price_col}") ) :=
#'           state_space_third_highest/total_state_space,
#'         !!as.name( glue::glue("perc_space_third_lowest_{Price_diff_lag}_{roll_period_state_space}_{price_col}") ) :=
#'           state_space_third_lowest/total_state_space,
#'         !!as.name( glue::glue("perc_space_second_lowest_{Price_diff_lag}_{roll_period_state_space}_{price_col}") ) :=
#'           state_space_second_lowest/total_state_space,
#'         !!as.name( glue::glue("perc_space_space_lowest_{Price_diff_lag}_{roll_period_state_space}_{price_col}") ) :=
#'           state_space_lowest/total_state_space,
#'         !!as.name( glue::glue("perc_space_space_min_{Price_diff_lag}_{roll_period_state_space}_{price_col}") ) :=
#'           state_space_min/total_state_space
#'       ) %>%
#'       dplyr::select(Date, Asset, contains("perc_space_")) %>%
#'       arrange(Date) %>%
#'       fill(contains("perc_space_"), .direction = "down")
#'
#'   }
#'
#' #' Single_Asset_V3_state_space_Gen_Model
#' #'
#' #' @param state_space_data
#' #' @param asset_of_interest
#' #' @param actual_wins_losses_asset
#' #' @param period_of_analysis
#' #' @param training_end_date
#' #' @param bin_threshold
#' #' @param sig_thresh
#' #'
#' #' @return
#' #' @export
#' #'
#' #' @examples
#' Single_Asset_V3_state_space_Gen_Model <-
#'   function(
#'     state_space_data = state_space_data,
#'     asset_of_interest = asset_of_interest,
#'     actual_wins_losses_asset = actual_wins_losses_asset,
#'     period_of_analysis = actuals_periods_needed[1],
#'     training_end_date = training_end_date,
#'     bin_threshold = bin_threshold,
#'     sig_thresh = 0.01,
#'     base_path = "C:/Users/Nikhil Chandra/Documents/trade_data/single_asset_models_v1/"
#'   ) {
#'
#'     joined_data <-
#'       state_space_data %>%
#'       group_by(Asset) %>%
#'       arrange(Date, .by_group = TRUE) %>%
#'       left_join(
#'         actual_wins_losses_asset %>%
#'           filter(Asset == asset_of_interest) %>%
#'           dplyr::select(Date, Asset, !!as.name(period_of_analysis))
#'       ) %>%
#'       filter(
#'         Date <= training_end_date
#'       ) %>%
#'       mutate(
#'         bin_var = ifelse( !!as.name(period_of_analysis) >= bin_threshold, 1, 0)
#'       )
#'
#'     dependants <-
#'       names(joined_data) %>%
#'       keep(~ str_detect(.x, "perc_space_"))
#'
#'     lm_form <-
#'       create_lm_formula(dependant = period_of_analysis, independant = dependants)
#'
#'     LM_model <- lm(formula = lm_form, data = joined_data)
#'
#'     sig_coefs <- get_sig_coefs(LM_model, p_value_thresh_for_inputs = sig_thresh)
#'
#'     lm_form <-
#'       create_lm_formula(dependant = period_of_analysis, independant = sig_coefs)
#'
#'     LM_model <- lm(formula = lm_form, data = joined_data)
#'
#'     saveRDS(LM_model,
#'             glue::glue("{base_path}/LM_state_space_{period_of_analysis}_{asset_of_interest}.RDS")
#'     )
#'
#'
#'     dependants <-
#'       names(joined_data) %>%
#'       keep(~ str_detect(.x, "perc_space_"))
#'
#'     Glm_form <-
#'       create_lm_formula(dependant = "bin_var", independant = dependants)
#'
#'     GLM_model <- glm(formula = Glm_form, data = joined_data, family = binomial("logit"))
#'
#'     sig_coefs <- get_sig_coefs(GLM_model, p_value_thresh_for_inputs = sig_thresh)
#'
#'     Glm_form <-
#'       create_lm_formula(dependant = "bin_var", independant = sig_coefs)
#'
#'     GLM_model <- glm(formula = Glm_form, data = joined_data, family = binomial("logit"))
#'
#'     saveRDS(GLM_model,
#'             glue::glue("{base_path}/GLM_state_space_{period_of_analysis}_{asset_of_interest}.RDS")
#'     )
#'
#'   }
#'
#' #' Single_Asset_V3_state_space_read_Model
#' #'
#' #' @param state_space_data
#' #' @param asset_of_interest
#' #' @param period_of_analysis
#' #' @param training_end_date
#' #' @param roll_mean_period
#' #'
#' #' @return
#' #' @export
#' #'
#' #' @examples
#' Single_Asset_V3_state_space_read_Model <-
#'   function(
#'     state_space_data = state_space_data,
#'     asset_of_interest = asset_of_interest,
#'     period_of_analysis = actuals_periods_needed[1],
#'     training_end_date = training_end_date,
#'     roll_mean_period = 100,
#'     base_path = "C:/Users/Nikhil Chandra/Documents/trade_data/single_asset_models_v1/"
#'   ) {
#'
#'     LM_model <-
#'       readRDS(
#'         glue::glue("{base_path}/LM_state_space_{period_of_analysis}_{asset_of_interest}.RDS")
#'       )
#'
#'     preds_all <- predict.lm(object = LM_model, newdata = state_space_data)
#'
#'     GLM_model <-
#'       readRDS(
#'         glue::glue("{base_path}/GLM_state_space_{period_of_analysis}_{asset_of_interest}.RDS")
#'       )
#'
#'     preds_all_GLM <- predict(object = GLM_model, newdata = state_space_data, type = "response")
#'
#'     complete_state_space_data <-
#'       state_space_data %>%
#'       filter(Asset == asset_of_interest) %>%
#'       distinct(Date, Asset) %>%
#'       mutate(
#'         !!as.name(glue::glue("state_space_LM_Pred_{period_of_analysis}")) := preds_all,
#'         !!as.name(glue::glue("state_space_GLM_Pred_{period_of_analysis}")) := preds_all_GLM
#'       ) %>%
#'       mutate(
#'         !!as.name(glue::glue("state_space_LM_Pred_{period_of_analysis}_mean")) :=
#'           slider::slide_dbl(.x =
#'                               !!as.name(glue::glue("state_space_LM_Pred_{period_of_analysis}")),
#'                             .f = ~ mean(.x, na.rm = T),
#'                             .before = roll_mean_period),
#'         !!as.name(glue::glue("state_space_LM_Pred_{period_of_analysis}_sd")) :=
#'           slider::slide_dbl(.x =
#'                               !!as.name(glue::glue("state_space_LM_Pred_{period_of_analysis}")),
#'                             .f = ~ sd(.x, na.rm = T),
#'                             .before = roll_mean_period),
#'
#'         !!as.name(glue::glue("state_space_GLM_Pred_{period_of_analysis}_mean")) :=
#'           slider::slide_dbl(.x =
#'                               !!as.name(glue::glue("state_space_GLM_Pred_{period_of_analysis}")),
#'                             .f = ~ mean(.x, na.rm = T),
#'                             .before = roll_mean_period),
#'         !!as.name(glue::glue("state_space_GLM_Pred_{period_of_analysis}_sd")) :=
#'           slider::slide_dbl(.x =
#'                               !!as.name(glue::glue("state_space_GLM_Pred_{period_of_analysis}")),
#'                             .f = ~ sd(.x, na.rm = T),
#'                             .before = roll_mean_period)
#'       )
#'
#'     testing_data <-
#'       complete_state_space_data %>%
#'       filter(Date > training_end_date)
#'
#'     training_data <-
#'       complete_state_space_data %>%
#'       filter(Date <= training_end_date)
#'
#'     return(list("testing_data" = testing_data, "training_data" = training_data) )
#'
#'   }
#'
#' #' Single_Asset_V3_Cop_data
#' #'
#' #' @param All_Asset_Data
#' #' @param asset_of_interest
#' #' @param copula_assets
#' #' @param rolling_period_cor
#' #'
#' #' @return
#' #' @export
#' #'
#' #' @examples
#' Single_Asset_V3_Cop_data <-
#'   function(All_Asset_Data = Indices_Metals_Bonds[[1]] %>% filter(Asset == asset_of_interest| Asset %in% copula_assets),
#'            asset_of_interest = asset_of_interest,
#'            copula_assets = copula_assets,
#'            rolling_period_cor = 100) {
#'
#'     asset_data_cop <-
#'       All_Asset_Data %>%
#'       filter(Asset == asset_of_interest)
#'
#'     cop_accumulator <- list()
#'
#'     for (i in 1:length(copula_assets) ) {
#'
#'       col_prefix <- paste0(asset_of_interest, "_", copula_assets[i])
#'
#'       cop_comparison_data <-
#'         All_Asset_Data %>%
#'         filter(Asset == copula_assets[i]) %>%
#'         dplyr::select(
#'           Date,
#'           Price_2 = Price,
#'           High_2 = High,
#'           Low_2 = Low
#'         )
#'
#'       cop_accumulator[[i]] <-
#'         asset_data_cop %>%
#'         left_join(cop_comparison_data) %>%
#'         arrange(Date) %>%
#'         fill(c(Price_2, High_2, Low_2), .direction = "down") %>%
#'         mutate(
#'
#'           !!as.name(paste0(col_prefix,"_" ,"cor_price", "_", rolling_period_cor)) :=
#'             slider::slide2_dbl(.x = (Price), .y = (Price_2), .f = ~ cor(.x, .y), .before = rolling_period_cor),
#'           !!as.name(paste0(col_prefix,"_" ,"cor_Low", "_", rolling_period_cor)) :=
#'             slider::slide2_dbl(.x = (Low), .y = (Low_2), .f = ~ cor(.x, .y), .before = rolling_period_cor),
#'           !!as.name(paste0(col_prefix,"_" ,"cor_High", "_", rolling_period_cor)) :=
#'             slider::slide2_dbl(.x = (High), .y = (High_2), .f = ~ cor(.x, .y), .before = rolling_period_cor),
#'
#'           !!as.name(paste0(col_prefix,"_" ,"cor_price_mean", "_", rolling_period_cor)) :=
#'             slider::slide_dbl(.x = !!as.name(paste0(col_prefix,"_" ,"cor_price", "_", rolling_period_cor)),  .f = ~ mean(.x, na.rm = T), .before = rolling_period_cor),
#'           !!as.name(paste0(col_prefix,"_" ,"cor_Low_mean", "_", rolling_period_cor)) :=
#'             slider::slide_dbl(.x = !!as.name(paste0(col_prefix,"_" ,"cor_Low", "_", rolling_period_cor)),  .f = ~ mean(.x, na.rm = T), .before = rolling_period_cor),
#'           !!as.name(paste0(col_prefix,"_" ,"cor_High_mean", "_", rolling_period_cor)) :=
#'             slider::slide_dbl(.x = !!as.name(paste0(col_prefix,"_" ,"cor_High", "_", rolling_period_cor)), .f = ~ mean(.x, na.rm = T), .before = rolling_period_cor),
#'
#'           !!as.name(paste0(col_prefix,"_" ,"cor_price_sd", "_", rolling_period_cor)) :=
#'             slider::slide_dbl(.x = !!as.name(paste0(col_prefix,"_" ,"cor_price", "_", rolling_period_cor)),  .f = ~ sd(.x, na.rm = T), .before = rolling_period_cor),
#'           !!as.name(paste0(col_prefix,"_" ,"cor_Low_sd", "_", rolling_period_cor)) :=
#'             slider::slide_dbl(.x = !!as.name(paste0(col_prefix,"_" ,"cor_Low", "_", rolling_period_cor)),  .f = ~ sd(.x, na.rm = T), .before = rolling_period_cor),
#'           !!as.name(paste0(col_prefix,"_" ,"cor_High_sd", "_", rolling_period_cor)) :=
#'             slider::slide_dbl(.x = !!as.name(paste0(col_prefix,"_" ,"cor_High", "_", rolling_period_cor)), .f = ~ sd(.x, na.rm = T), .before = rolling_period_cor)
#'
#'         ) %>%
#'         dplyr::select(-Price, -Price_2, -High, -High_2, -Low, -Low_2, -Vol., -Open) %>%
#'         group_by(Asset) %>%
#'         arrange(Date, .by_group = TRUE) %>%
#'         group_by(Asset) %>%
#'         mutate(across(
#'           .cols = contains("cor_"),
#'           .fns = ~ lag(.)
#'         ))
#'
#'     }
#'
#'     returned_data <-
#'       cop_accumulator %>%
#'       reduce(left_join)
#'
#'
#'   }
#'
#' #' Single_Asset_V3_Copula_Gen_Model
#' #'
#' #' @param copula_data
#' #' @param asset_of_interest
#' #' @param actual_wins_losses_asset
#' #' @param period_of_analysis
#' #' @param training_end_date
#' #' @param bin_threshold
#' #' @param sig_thresh
#' #'
#' #' @return
#' #' @export
#' #'
#' #' @examples
#' Single_Asset_V3_Copula_Gen_Model <-
#'   function(
#'     copula_data = copula_data,
#'     asset_of_interest = asset_of_interest,
#'     actual_wins_losses_asset = actual_wins_losses_asset,
#'     period_of_analysis = actuals_periods_needed[1],
#'     training_end_date = training_end_date,
#'     bin_threshold = bin_threshold,
#'     sig_thresh = 0.15,
#'     base_path = "C:/Users/Nikhil Chandra/Documents/trade_data/single_asset_models_v1/"
#'   ) {
#'
#'     joined_data <-
#'       copula_data %>%
#'       group_by(Asset) %>%
#'       arrange(Date, .by_group = TRUE) %>%
#'       fill(contains("cor"), .direction = "down") %>%
#'       left_join(
#'         actual_wins_losses_asset %>%
#'           filter(Asset == asset_of_interest) %>%
#'           dplyr::select(Date, Asset, !!as.name(period_of_analysis))
#'       ) %>%
#'       filter(
#'         Date <= training_end_date
#'       ) %>%
#'       mutate(
#'         bin_var = ifelse( !!as.name(period_of_analysis) >= bin_threshold, 1, 0)
#'       )
#'
#'     dependants <-
#'       names(joined_data) %>%
#'       keep(~ str_detect(.x, "cor_"))
#'
#'     lm_form <-
#'       create_lm_formula(dependant = period_of_analysis, independant = dependants)
#'
#'     LM_model <- lm(formula = lm_form, data = joined_data)
#'
#'     sig_coefs <- get_sig_coefs(LM_model, p_value_thresh_for_inputs = sig_thresh)
#'
#'     lm_form <-
#'       create_lm_formula(dependant = period_of_analysis, independant = sig_coefs)
#'
#'     LM_model <- lm(formula = lm_form, data = joined_data)
#'
#'     saveRDS(LM_model,
#'             glue::glue("{base_path}/LM_Copula_{period_of_analysis}_{asset_of_interest}.RDS")
#'     )
#'
#'     dependants <-
#'       names(joined_data) %>%
#'       keep(~ str_detect(.x, "cor_"))
#'
#'     Glm_form <-
#'       create_lm_formula(dependant = "bin_var", independant = dependants)
#'
#'     GLM_model <- glm(formula = Glm_form, data = joined_data, family = binomial("logit"))
#'
#'     sig_coefs <- get_sig_coefs(GLM_model, p_value_thresh_for_inputs = sig_thresh)
#'
#'     Glm_form <-
#'       create_lm_formula(dependant = "bin_var", independant = sig_coefs)
#'
#'     GLM_model <- glm(formula = Glm_form, data = joined_data, family = binomial("logit"))
#'
#'     saveRDS(GLM_model,
#'             glue::glue("{base_path}/GLM_Copula_{period_of_analysis}_{asset_of_interest}.RDS")
#'     )
#'
#'     rm(GLM_model)
#'
#'   }
#'
#' #' Single_Asset_V3_Copula_read_Model
#' #'
#' #' @param copula_data
#' #' @param asset_of_interest
#' #' @param period_of_analysis
#' #' @param training_end_date
#' #' @param roll_mean_period
#' #'
#' #' @return
#' #' @export
#' #'
#' #' @examples
#' Single_Asset_V3_Copula_read_Model <-
#'   function(
#'     copula_data = copula_data,
#'     asset_of_interest = asset_of_interest,
#'     period_of_analysis = actuals_periods_needed[1],
#'     training_end_date = training_end_date,
#'     roll_mean_period = 100,
#'     base_path = "C:/Users/Nikhil Chandra/Documents/trade_data/single_asset_models_v1/"
#'   ) {
#'
#'     LM_model <-
#'       readRDS(
#'         glue::glue("{base_path}/LM_Copula_{period_of_analysis}_{asset_of_interest}.RDS")
#'       )
#'
#'     preds_all <- predict.lm(object = LM_model, newdata = copula_data)
#'
#'     GLM_model <-
#'       readRDS(
#'         glue::glue("{base_path}/GLM_Copula_{period_of_analysis}_{asset_of_interest}.RDS")
#'       )
#'
#'     preds_all_GLM <- predict(object = GLM_model, newdata = copula_data, type = "response")
#'
#'     complete_copula_data <-
#'       copula_data %>%
#'       filter(Asset == asset_of_interest) %>%
#'       distinct(Date, Asset) %>%
#'       mutate(
#'         !!as.name(glue::glue("Copula_LM_Pred_{period_of_analysis}")) := preds_all,
#'         !!as.name(glue::glue("Copula_GLM_Pred_{period_of_analysis}")) := preds_all_GLM
#'       ) %>%
#'       mutate(
#'         !!as.name(glue::glue("Copula_LM_Pred_{period_of_analysis}_mean")) :=
#'           slider::slide_dbl(.x =
#'                               !!as.name(glue::glue("Copula_LM_Pred_{period_of_analysis}")),
#'                             .f = ~ mean(.x, na.rm = T),
#'                             .before = roll_mean_period),
#'         !!as.name(glue::glue("Copula_LM_Pred_{period_of_analysis}_sd")) :=
#'           slider::slide_dbl(.x =
#'                               !!as.name(glue::glue("Copula_LM_Pred_{period_of_analysis}")),
#'                             .f = ~ sd(.x, na.rm = T),
#'                             .before = roll_mean_period),
#'
#'         !!as.name(glue::glue("Copula_GLM_Pred_{period_of_analysis}_mean")) :=
#'           slider::slide_dbl(.x =
#'                               !!as.name(glue::glue("Copula_GLM_Pred_{period_of_analysis}")),
#'                             .f = ~ mean(.x, na.rm = T),
#'                             .before = roll_mean_period),
#'         !!as.name(glue::glue("Copula_GLM_Pred_{period_of_analysis}_sd")) :=
#'           slider::slide_dbl(.x =
#'                               !!as.name(glue::glue("Copula_GLM_Pred_{period_of_analysis}")),
#'                             .f = ~ sd(.x, na.rm = T),
#'                             .before = roll_mean_period)
#'       )
#'
#'     testing_data <-
#'       complete_copula_data %>%
#'       filter(Date > training_end_date)
#'
#'     training_data <-
#'       complete_copula_data %>%
#'       filter(Date <= training_end_date)
#'
#'     return(list("testing_data" = testing_data, "training_data" = training_data) )
#'
#'
#'   }
#'
#' #' Single_Asset_V3_AR_read_model
#' #'
#' #' @param AR_model_data
#' #' @param asset_of_interest
#' #' @param period_of_analysis
#' #' @param training_end_date
#' #' @param bin_threshold
#' #' @param sig_thresh
#' #'
#' #' @return
#' #' @export
#' #'
#' #' @examples
#' Single_Asset_V3_AR_read_model <-
#'   function(
#'     AR_model_data = AR_model_data,
#'     asset_of_interest = asset_of_interest,
#'     period_of_analysis = actuals_periods_needed[1],
#'     training_end_date = training_end_date,
#'     roll_mean_period = 100,
#'     base_path = "C:/Users/Nikhil Chandra/Documents/trade_data/single_asset_models_v1/"
#'   ) {
#'
#'     LM_model <-
#'       readRDS(
#'         glue::glue("{base_path}/LM_AR_{period_of_analysis}_{asset_of_interest}.RDS")
#'       )
#'
#'     preds_all <- predict.lm(object = LM_model, newdata = AR_model_data)
#'
#'     GLM_model <-
#'       readRDS(
#'         glue::glue("{base_path}/GLM_AR_{period_of_analysis}_{asset_of_interest}.RDS")
#'       )
#'
#'     preds_all_GLM <- predict(object = GLM_model, newdata = AR_model_data, type = "response")
#'
#'     complete_AR_data <-
#'       AR_model_data %>%
#'       filter(Asset == asset_of_interest) %>%
#'       distinct(Date, Asset) %>%
#'       mutate(
#'         !!as.name(glue::glue("AR_LM_Pred_{period_of_analysis}")) := preds_all,
#'         !!as.name(glue::glue("AR_GLM_Pred_{period_of_analysis}")) := preds_all_GLM
#'       ) %>%
#'       mutate(
#'         !!as.name(glue::glue("AR_LM_Pred_{period_of_analysis}_mean")) :=
#'           slider::slide_dbl(.x =
#'                               !!as.name(glue::glue("AR_LM_Pred_{period_of_analysis}")),
#'                             .f = ~ mean(.x, na.rm = T),
#'                             .before = roll_mean_period),
#'         !!as.name(glue::glue("AR_LM_Pred_{period_of_analysis}_sd")) :=
#'           slider::slide_dbl(.x =
#'                               !!as.name(glue::glue("AR_LM_Pred_{period_of_analysis}")),
#'                             .f = ~ sd(.x, na.rm = T),
#'                             .before = roll_mean_period),
#'
#'         !!as.name(glue::glue("AR_GLM_Pred_{period_of_analysis}_mean")) :=
#'           slider::slide_dbl(.x =
#'                               !!as.name(glue::glue("AR_GLM_Pred_{period_of_analysis}")),
#'                             .f = ~ mean(.x, na.rm = T),
#'                             .before = roll_mean_period),
#'         !!as.name(glue::glue("AR_GLM_Pred_{period_of_analysis}_sd")) :=
#'           slider::slide_dbl(.x =
#'                               !!as.name(glue::glue("AR_GLM_Pred_{period_of_analysis}")),
#'                             .f = ~ sd(.x, na.rm = T),
#'                             .before = roll_mean_period)
#'       )
#'
#'     testing_data <-
#'       complete_AR_data %>%
#'       filter(Date > training_end_date)
#'
#'     training_data <-
#'       complete_AR_data %>%
#'       filter(Date <= training_end_date)
#'
#'     return(list("testing_data" = testing_data, "training_data" = training_data) )
#'
#'   }
#'
#' #' Single_Asset_V3_AR_Gen_Model
#' #'
#' #' @param AR_model_data
#' #' @param asset_of_interest
#' #' @param actual_wins_losses_asset
#' #' @param period_of_analysis
#' #' @param training_end_date
#' #' @param bin_threshold
#' #' @param sig_thresh
#' #'
#' #' @return
#' #' @export
#' #'
#' #' @examples
#' Single_Asset_V3_AR_Gen_Model <-
#'   function(
#'     AR_model_data = AR_model_data,
#'     asset_of_interest = asset_of_interest,
#'     actual_wins_losses_asset = actual_wins_losses_asset,
#'     period_of_analysis = actuals_periods_needed[1],
#'     training_end_date = training_end_date,
#'     bin_threshold = bin_threshold,
#'     sig_thresh = 0.15,
#'     base_path = "C:/Users/Nikhil Chandra/Documents/trade_data/single_asset_models_v1/"
#'   ) {
#'
#'     joined_data <-
#'       AR_model_data %>%
#'       left_join(
#'         actual_wins_losses_asset %>%
#'           filter(Asset == asset_of_interest) %>%
#'           dplyr::select(Date, Asset, !!as.name(period_of_analysis))
#'       ) %>%
#'       filter(
#'         Date <= training_end_date
#'       ) %>%
#'       mutate(
#'         bin_var = ifelse( !!as.name(period_of_analysis) >= bin_threshold, 1, 0)
#'       )
#'
#'     dependants <-
#'       names(joined_data) %>%
#'       keep(~ str_detect(.x, "MA_|lagged_|MSD_"))
#'
#'     lm_form <-
#'       create_lm_formula(dependant = period_of_analysis, independant = dependants)
#'
#'     LM_model <- lm(formula = lm_form, data = joined_data)
#'
#'     sig_coefs <- get_sig_coefs(LM_model, p_value_thresh_for_inputs = sig_thresh)
#'
#'     lm_form <-
#'       create_lm_formula(dependant = period_of_analysis, independant = sig_coefs)
#'
#'     LM_model <- lm(formula = lm_form, data = joined_data)
#'
#'     saveRDS(LM_model,
#'             glue::glue("{base_path}/LM_AR_{period_of_analysis}_{asset_of_interest}.RDS")
#'     )
#'
#'     rm(LM_model)
#'
#'     dependants <-
#'       names(joined_data) %>%
#'       keep(~ str_detect(.x, "MA_|lagged_|MSD_"))
#'
#'     Glm_form <-
#'       create_lm_formula(dependant = "bin_var", independant = dependants)
#'
#'     GLM_model <- glm(formula = Glm_form, data = joined_data, family = binomial("logit"))
#'
#'     sig_coefs <- get_sig_coefs(GLM_model, p_value_thresh_for_inputs = sig_thresh)
#'
#'     Glm_form <-
#'       create_lm_formula(dependant = "bin_var", independant = sig_coefs)
#'
#'     GLM_model <- glm(formula = Glm_form, data = joined_data, family = binomial("logit"))
#'
#'     saveRDS(GLM_model,
#'             glue::glue("{base_path}/GLM_AR_{period_of_analysis}_{asset_of_interest}.RDS")
#'     )
#'
#'   }
#'
#' #' Single_Asset_V3_AR_Model_data
#' #'
#' #' @param asset_data
#' #' @param asset_of_interest
#' #' @param lag_value_1
#' #' @param lag_value_2
#' #' @param lag_value_3
#' #' @param lag_value_4
#' #' @param lag_value_5
#' #' @param lag_value_6
#' #' @param MA_period_1
#' #' @param MA_period_2
#' #' @param MA_period_3
#' #' @param MA_period_4
#' #'
#' #' @return
#' #' @export
#' #'
#' #' @examples
#' Single_Asset_V3_AR_Model_data <-
#'   function(
#'     asset_data = Indices_Metals_Bonds,
#'     asset_of_interest = asset_of_interest,
#'     lag_value_1 = 2,
#'     lag_value_2 = 4,
#'     lag_value_3 = 6,
#'     lag_value_4 = 8,
#'     lag_value_5 = 10,
#'     lag_value_6 = 12,
#'     lag_value_7 = 20,
#'
#'     MA_period_1 = 5,
#'     MA_period_2 = 10,
#'     MA_period_3 = 15,
#'     MA_period_4 = 20,
#'     MA_period_5 = 30,
#'     MA_period_6 = 40
#'   ) {
#'
#'     returned_data <-
#'       asset_data %>%
#'       ungroup() %>%
#'       filter(Asset == asset_of_interest) %>%
#'       arrange(Date) %>%
#'       mutate(
#'         lagged_Price = lag(Price) - lag(Price, lag_value_1 + 1),
#'         lagged_High = lag(High) - lag(Price, lag_value_1 + 1),
#'         lagged_Low = lag(Low) - lag(Price, lag_value_1 + 1),
#'
#'         lagged_Price2 = lag(Price) - lag(Price, lag_value_2 + 1),
#'         lagged_High2 = lag(High) - lag(Price, lag_value_2 + 1),
#'         lagged_Low2 = lag(Low) - lag(Price, lag_value_2 + 1),
#'
#'         lagged_Price3 = lag(Price) - lag(Price, lag_value_3 + 1),
#'         lagged_High3 = lag(High) - lag(Price, lag_value_3 + 1),
#'         lagged_Low3 = lag(Low) - lag(Price, lag_value_3 + 1),
#'
#'         lagged_Price4 = lag(Price) - lag(Price, lag_value_4 + 1),
#'         lagged_High4 = lag(High) - lag(Price, lag_value_4 + 1),
#'         lagged_Low4 = lag(Low) - lag(Price, lag_value_4 + 1),
#'
#'         lagged_Price5 = lag(Price) - lag(Price, lag_value_5 + 1),
#'         lagged_High5 = lag(High) - lag(Price, lag_value_5 + 1),
#'         lagged_Low5 = lag(Low) - lag(Price, lag_value_5 + 1),
#'
#'         lagged_Price6 = lag(Price) - lag(Price, lag_value_6 + 1),
#'         lagged_High6 = lag(High) - lag(Price, lag_value_6 + 1),
#'         lagged_Low6 = lag(Low) - lag(Price, lag_value_6 + 1),
#'
#'         lagged_Price7 = lag(Price) - lag(Price, lag_value_7 + 1),
#'         lagged_High7 = lag(High) - lag(Price, lag_value_7 + 1),
#'         lagged_Low7 = lag(Low) - lag(Price, lag_value_7 + 1)
#'
#'       ) %>%
#'       mutate(
#'         MA_Price_1 = slider::slide_dbl(.x = lagged_Price, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_1),
#'         MA_High_1 = slider::slide_dbl(.x = lagged_High, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_1),
#'         MA_Low_1 = slider::slide_dbl(.x = lagged_Low, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_1),
#'
#'         MA_Price_2 = slider::slide_dbl(.x = lagged_Price2, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_2),
#'         MA_High_2 = slider::slide_dbl(.x = lagged_High2, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_2),
#'         MA_Low_2 = slider::slide_dbl(.x = lagged_Low2, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_2),
#'
#'         MA_Price_3 = slider::slide_dbl(.x = lagged_Price3, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_3),
#'         MA_High_3 = slider::slide_dbl(.x = lagged_High3, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_3),
#'         MA_Low_3 = slider::slide_dbl(.x = lagged_Low3, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_3),
#'
#'         MA_Price_4 = slider::slide_dbl(.x = lagged_Price4, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_4),
#'         MA_High_4 = slider::slide_dbl(.x = lagged_High4, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_4),
#'         MA_Low_4 = slider::slide_dbl(.x = lagged_Low4, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_4),
#'
#'         MA_Price_5 = slider::slide_dbl(.x = lagged_Price5, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_5),
#'         MA_High_5 = slider::slide_dbl(.x = lagged_High5, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_5),
#'         MA_Low_5 = slider::slide_dbl(.x = lagged_Low5, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_5),
#'
#'         MA_Price_6 = slider::slide_dbl(.x = lagged_Price6, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_6),
#'         MA_High_6 = slider::slide_dbl(.x = lagged_High6, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_6),
#'         MA_Low_6 = slider::slide_dbl(.x = lagged_Low6, .f = ~ mean(.x, na.rm = T) ,.before = MA_period_6),
#'
#'
#'         MSD_Price_1 = slider::slide_dbl(.x = lagged_Price, .f = ~ sd(.x, na.rm = T) ,.before = MA_period_1),
#'         MSD_High_1 = slider::slide_dbl(.x = lagged_High, .f = ~ sd(.x, na.rm = T) ,.before = MA_period_1),
#'         MSD_Low_1 = slider::slide_dbl(.x = lagged_Low, .f = ~ sd(.x, na.rm = T) ,.before = MA_period_1),
#'
#'         MSD_Price_2 = slider::slide_dbl(.x = lagged_Price2, .f = ~ sd(.x, na.rm = T) ,.before = MA_period_2),
#'         MSD_High_2 = slider::slide_dbl(.x = lagged_High2, .f = ~ sd(.x, na.rm = T) ,.before = MA_period_2),
#'         MSD_Low_2 = slider::slide_dbl(.x = lagged_Low2, .f = ~ sd(.x, na.rm = T) ,.before = MA_period_2),
#'
#'         MSD_Price_3 = slider::slide_dbl(.x = lagged_Price3, .f = ~ sd(.x, na.rm = T) ,.before = MA_period_3),
#'         MSD_High_3 = slider::slide_dbl(.x = lagged_High3, .f = ~ sd(.x, na.rm = T) ,.before = MA_period_3),
#'         MSD_Low_3 = slider::slide_dbl(.x = lagged_Low3, .f = ~ sd(.x, na.rm = T) ,.before = MA_period_3),
#'
#'         MSD_Price_4 = slider::slide_dbl(.x = lagged_Price4, .f = ~ sd(.x, na.rm = T) ,.before = MA_period_4),
#'         MSD_High_4 = slider::slide_dbl(.x = lagged_High4, .f = ~ sd(.x, na.rm = T) ,.before = MA_period_4),
#'         MSD_Low_4 = slider::slide_dbl(.x = lagged_Low4, .f = ~ sd(.x, na.rm = T) ,.before = MA_period_4),
#'
#'         MSD_Price_5 = slider::slide_dbl(.x = lagged_Price5, .f = ~ sd(.x, na.rm = T) ,.before = MA_period_5),
#'         MSD_High_5 = slider::slide_dbl(.x = lagged_High5, .f = ~ sd(.x, na.rm = T) ,.before = MA_period_5),
#'         MSD_Low_5 = slider::slide_dbl(.x = lagged_Low5, .f = ~ sd(.x, na.rm = T) ,.before = MA_period_5),
#'
#'         MSD_Price_6 = slider::slide_dbl(.x = lagged_Price6, .f = ~ sd(.x, na.rm = T) ,.before = MA_period_6),
#'         MSD_High_6 = slider::slide_dbl(.x = lagged_High6, .f = ~ sd(.x, na.rm = T) ,.before = MA_period_6),
#'         MSD_Low_6 = slider::slide_dbl(.x = lagged_Low6, .f = ~ sd(.x, na.rm = T) ,.before = MA_period_6)
#'       )
#'
#'     return(returned_data)
#'
#'   }
#'
#' #' single_asset_algo_generate_models
#' #'
#' #' @param All_Daily_Data
#' #' @param Indices_Metals_Bonds
#' #' @param raw_macro_data
#' #' @param currency_conversion
#' #' @param asset_infor
#' #' @param start_index
#' #' @param end_index
#' #' @param risk_dollar_value
#' #' @param trade_direction
#' #' @param stop_value_var
#' #' @param profit_value_var
#' #' @param period_var
#' #' @param bin_var_col
#' #' @param date_train_end_pre
#' #' @param date_train_phase_2_end_pre
#' #' @param training_date_start_post
#' #' @param training_date_end_post
#' #' @param model_data_store_path
#' #' @param save_path
#' #'
#' #' @returns
#' #' @export
#' #'
#' #' @examples
#' single_asset_algo_generate_models_V2_V3 <-
#'   function(
#'     All_Daily_Data = All_Daily_Data,
#'     Indices_Metals_Bonds = Indices_Metals_Bonds,
#'     raw_macro_data = raw_macro_data,
#'     currency_conversion = currency_conversion,
#'     asset_infor = asset_infor,
#'     start_index = 1,
#'     end_index = 40,
#'     risk_dollar_value = 15,
#'     trade_direction = "Long",
#'     stop_value_var = 5,
#'     profit_value_var = 30,
#'     period_var = 24,
#'     bin_var_col = c("period_return_20_Price", "period_return_24_Price", "period_return_28_Price"),
#'     date_train_end_pre = "2023-06-01",
#'     date_train_phase_2_end_pre = "2024-06-01",
#'     training_date_start_post = "2024-07-04",
#'     training_date_end_post = "2025-09-01",
#'     test_end_date = as.character(today()),
#'     post_bins_cols =
#'       c("period_return_24_Price",
#'         "period_return_30_Price",
#'         "period_return_44_Price"),
#'     post_dependant_threshold = 5,
#'     post_dependant_var = "period_return_24_Price",
#'     model_data_store_path = "C:/Users/nikhi/Documents/trade_data/Day_Trader_Single_Asset_V2_trade_store_stop_2.db",
#'     save_path = "C:/Users/nikhi/Documents/trade_data/Day_Trader_Single_Asset_V2_trade_store_stop_2"
#'   ) {
#'
#'     equity_index <-
#'       get_equity_index(index_data = Indices_Metals_Bonds[[1]])
#'
#'     gold_index <-
#'       get_Gold_index(index_data = Indices_Metals_Bonds[[1]])
#'
#'     silver_index <-
#'       get_silver_index(index_data = Indices_Metals_Bonds[[1]])
#'
#'     bonds_index <-
#'       get_bonds_index(index_data = Indices_Metals_Bonds[[1]])
#'
#'     USD_index <-
#'       get_USD_index_for_models(index_data = Indices_Metals_Bonds[[1]])
#'
#'     EUR_index <-
#'       get_EUR_index_for_models(index_data = Indices_Metals_Bonds[[1]])
#'
#'     GBP_index <-
#'       get_GBP_index_for_models(index_data = Indices_Metals_Bonds[[1]])
#'
#'     AUD_index <-
#'       get_AUD_index_for_models(index_data = Indices_Metals_Bonds[[1]])
#'
#'     COMMOD_index <-
#'       get_COMMOD_index_for_models(index_data = Indices_Metals_Bonds[[1]])
#'
#'     USD_STOCKS_index <-
#'       get_USD_AND_STOCKS_index_for_models(index_data = Indices_Metals_Bonds[[1]])
#'
#'     NZD_index <-
#'       get_NZD_index_for_models(index_data = Indices_Metals_Bonds[[1]])
#'
#'     interest_rates <-
#'       get_interest_rates(
#'         raw_macro_data = raw_macro_data,
#'         lag_days = 1
#'       )
#'
#'     cpi_data <-
#'       get_cpi(
#'         raw_macro_data = raw_macro_data,
#'         lag_days = 1
#'       )
#'
#'     sentiment_index <-
#'       create_sentiment_index(
#'         raw_macro_data = raw_macro_data,
#'         lag_days = 1,
#'         date_start = "2011-01-01",
#'         end_date = today() %>% as.character(),
#'         first_difference = TRUE,
#'         scale_values = FALSE
#'       )
#'
#'     gdp_data <-
#'       get_GDP_countries(
#'         raw_macro_data = raw_macro_data,
#'         lag_days = 1
#'       )
#'
#'     unemp_data <-
#'       get_unemp_countries(
#'         raw_macro_data = raw_macro_data,
#'         lag_days = 1
#'       )
#'
#'     manufac_pmi <-
#'       get_manufac_countries(
#'         raw_macro_data = raw_macro_data,
#'         lag_days = 1
#'       )
#'
#'     USD_Macro <-
#'       get_additional_USD_Macro(
#'         raw_macro_data = raw_macro_data,
#'         lag_days = 1
#'       )
#'
#'     EUR_Macro <-
#'       get_additional_EUR_Macro(
#'         raw_macro_data = raw_macro_data,
#'         lag_days = 1
#'       )
#'
#'     indicator_mapping <- list(
#'       Asset = c("EUR_USD", #1
#'                 "EU50_EUR", #2
#'                 "SPX500_USD", #3
#'                 "US2000_USD", #4
#'                 "USB10Y_USD", #5
#'                 "USD_JPY", #6
#'                 "AUD_USD", #7
#'                 "EUR_GBP", #8
#'                 "AU200_AUD" ,#9
#'                 "EUR_AUD", #10
#'                 "WTICO_USD", #11
#'                 "UK100_GBP", #12
#'                 "USD_CAD", #13
#'                 "GBP_USD", #14
#'                 "GBP_CAD", #15
#'                 "EUR_JPY", #16
#'                 "EUR_NZD", #17
#'                 "XAG_USD", #18
#'                 "XAG_EUR", #19
#'                 "XAG_AUD", #20
#'                 "XAG_NZD", #21
#'                 "HK33_HKD", #22
#'                 "FR40_EUR", #23
#'                 "BTC_USD", #24
#'                 "XAG_GBP", #25
#'                 "GBP_AUD", #26
#'                 "USD_SEK", #27
#'                 "USD_SGD", #28
#'                 "NZD_USD", #29
#'                 "GBP_NZD", #30
#'                 "XCU_USD", #31
#'                 "NATGAS_USD", #32
#'                 "GBP_JPY", #33
#'                 "SG30_SGD", #34
#'                 "XAU_USD", #35
#'                 "EUR_SEK", #36
#'                 "XAU_AUD", #37
#'                 "UK10YB_GBP", #38
#'                 "JP225Y_JPY", #39
#'                 "ETH_USD" #40
#'       ),
#'       couplua_assets =
#'         list(
#'           # EUR_USD
#'           c("XAU_EUR", "XAG_EUR", "EUR_JPY", "EU50_EUR", "EUR_AUD", "EUR_GBP",
#'             "SPX500_USD", "XAU_USD", "USD_JPY", "GBP_USD", "EUR_NZD", "XAG_GBP", "XAU_GBP",
#'             "EUR_SEK", "USD_CAD") %>% unique(), #1
#'
#'           # EU50_EUR
#'           c("XAU_EUR", "XAG_EUR", "XAU_USD", "UK100_GBP", "SG30_SGD", "EUR_GBP", "SPX500_USD",
#'             "SPX500_USD", "XAU_USD", "AU200_AUD", "CH20_CHF", "US2000_USD",
#'             "XAG_GBP", "XAU_GBP", "WTICO_USD", "FR40_EUR", "HK33_HKD") %>% unique(), #2
#'
#'           # SPX500_USD
#'           c("US2000_USD", "AU200_AUD", "USB10Y_USD", "UK100_GBP", "XAU_USD", "EU50_EUR",
#'             "HK33_HKD", "FR40_EUR", "WTICO_USD", "USD_JPY", "EUR_USD", "GBP_USD", "AU200_AUD",
#'             "SG30_SGD", "XAU_EUR", "XAG_EUR", "XAG_GBP", "XAU_GBP", "XAG_USD" ) %>% unique(), #3
#'
#'           # US2000_USD
#'           c("SPX500_USD",  "AU200_AUD", "USB10Y_USD", "UK100_GBP", "XAU_USD", "EU50_EUR",
#'             "HK33_HKD", "FR40_EUR", "WTICO_USD", "USD_JPY", "EUR_USD", "GBP_USD", "AU200_AUD",
#'             "SG30_SGD", "XAU_EUR", "XAG_EUR", "XAG_GBP", "XAU_GBP","XAG_USD" ) %>% unique(), #4
#'
#'           # USB10Y_USD
#'           c("SPX500_USD",  "AU200_AUD", "UK100_GBP", "XAU_USD", "EU50_EUR",
#'             "HK33_HKD", "FR40_EUR", "WTICO_USD", "USD_JPY", "EUR_USD",
#'             "XAU_EUR", "AU200_AUD", "XAG_USD",
#'             "SG30_SGD", "XAU_EUR", "XAG_EUR", "XAG_GBP", "XAU_GBP") %>% unique(), #5
#'
#'           # USD_JPY
#'           c("EUR_JPY", "XAU_JPY", "XAG_JPY", "GBP_JPY", "XAU_USD", "SPX500_USD",
#'             "XAG_USD","NZD_USD", "AUD_USD", "EUR_USD", "GBP_USD", "USD_CAD",
#'             "USD_SEK", "USD_SGD", "USB10Y_USD") %>% unique(), #6
#'
#'           # AUD_USD
#'           c("XCU_USD", "AU200_AUD", "XAU_AUD", "GBP_AUD", "XAU_USD", "EUR_AUD",
#'             "XAG_USD","NZD_USD", "USD_JPY", "EUR_USD", "GBP_USD", "USD_CAD",
#'             "USD_SEK", "USD_SGD", "USB10Y_USD", "NZD_USD") %>% unique(), #7
#'
#'           # EUR_GBP
#'           c("GBP_USD", "EUR_USD", "XAU_EUR", "XAU_GBP", "GBP_JPY", "EUR_JPY",
#'             "XAG_EUR", "XAG_GBP", "USD_JPY", "UK100_GBP", "FR40_EUR", "EU50_EUR",
#'             "EUR_SEK", "USD_SEK", "EUR_AUD", "EUR_NZD", "EUR_SEK") %>% unique(), #8
#'
#'           # AU200_AUD
#'           c("XCU_USD", "US2000_USD", "UK100_GBP", "XAU_USD", "EU50_EUR",
#'             "HK33_HKD", "FR40_EUR", "WTICO_USD", "GBP_AUD", "EUR_AUD",
#'             "SG30_SGD", "XAU_EUR", "XAG_EUR", "XAG_GBP", "XAU_GBP", "XAG_USD" ) %>% unique(), #9
#'
#'           # EUR_AUD
#'           c("XCU_USD", "AU200_AUD", "XAU_AUD", "GBP_AUD", "XAU_USD", "AUD_USD",
#'             "XAG_USD","NZD_USD", "USD_JPY", "EUR_USD",
#'             "USB10Y_USD", "NZD_USD", "FR40_EUR", "EU50_EUR",
#'             "EUR_SEK", "EUR_NZD", "EUR_SEK") %>% unique(), #10
#'
#'           # WTICO_USD
#'           c("NATGAS_USD", "XAG_USD", "BCO_USD", "SPX500_USD", "UK10YB_GBP", "XAU_USD",
#'             "US2000_USD", "AU200_AUD", "USB10Y_USD", "UK100_GBP", "XAU_USD", "EU50_EUR",
#'             "HK33_HKD", "FR40_EUR", "USD_JPY", "EUR_USD", "GBP_USD",
#'             "XAU_EUR", "XAG_EUR", "XAG_GBP", "XAU_GBP") %>% unique(), #11
#'
#'           # "UK100_GBP", #12
#'           c("XAU_EUR", "XAG_EUR", "XAU_USD", "SG30_SGD", "EUR_GBP", "US2000_USD",
#'             "SPX500_USD", "XAU_USD", "AU200_AUD", "CH20_CHF", "UK10YB_GBP", "USB10Y_USD",
#'             "XAG_GBP", "XAU_GBP", "WTICO_USD", "FR40_EUR", "HK33_HKD") %>% unique(), #12
#'
#'           # "USD_CAD", #13
#'           c("XAU_JPY", "XAU_GBP", "XAU_EUR", "XAU_USD", "EUR_JPY", "GBP_JPY",
#'             "XAG_USD","NZD_USD", "USD_JPY", "EUR_USD", "GBP_USD", "GBP_CAD",
#'             "USD_SEK", "USD_SGD", "USB10Y_USD") %>% unique(), #13
#'
#'           # "GBP_USD", #14
#'           c("GBP_JPY", "GBP_CAD", "GBP_AUD", "GBP_NZD", "XAU_GBP", "XAG_GBP", "UK100_GBP",
#'             "XAU_USD", "XAG_USD", "EUR_GBP", "EUR_USD", "XAG_EUR", "XAU_EUR", "USD_JPY",
#'             "EUR_JPY", "UK10YB_GBP", "AUD_USD", "USD_SEK", "USD_CAD") %>% unique(), #14
#'
#'           # "GBP_CAD", #15
#'           c("GBP_JPY", "GBP_USD", "GBP_AUD", "GBP_NZD", "XAU_GBP", "XAG_GBP", "UK100_GBP",
#'             "XAU_USD", "XAG_USD", "EUR_GBP", "EUR_USD", "XAG_EUR", "XAU_EUR", "USD_JPY",
#'             "EUR_JPY", "UK10YB_GBP", "AUD_USD", "USD_SEK", "USD_CAD") %>% unique(), #15
#'
#'           # "EUR_JPY", #16
#'           c("GBP_USD", "EUR_USD", "XAU_EUR", "XAU_JPY", "USD_JPY", "EUR_AUD",
#'             "EUR_GBP", "EUR_NZD", "EUR_SEK", "XAG_EUR", "XAU_USD", "XAG_USD", "USD_JPY",
#'             "GBP_JPY", "FR40_EUR", "EU50_EUR") %>% unique(), #16
#'
#'           # "EUR_NZD", #17
#'           c("EUR_AUD", "EUR_USD", "XAU_EUR", "XAU_AUD", "NZD_USD", "EUR_JPY", "EUR_GBP",
#'             "GBP_NZD", "XAG_NZD", "XAG_EUR", "XAU_USD", "XAG_USD", "EUR_SEK",
#'             "FR40_EUR", "EU50_EUR", "AU200_AUD") %>% unique(), #17
#'
#'           # "XAG_USD", #18
#'           c("XAG_JPY", "XAG_GBP", "XAG_EUR", "XAG_AUD", "XAU_USD", "EU50_EUR", "SPX500_USD",
#'             "XAG_NZD", "XAU_AUD", "XAU_GBP", "XAU_JPY", "XAU_EUR", "EUR_USD", "USD_JPY",
#'             "GBP_USD", "AUD_USD", "USD_CAD", "USD_SEK") %>% unique(), #18
#'
#'           # "XAG_EUR", #19
#'           c("XAG_JPY", "XAG_GBP", "XAG_USD", "XAG_AUD", "XAU_USD", "EU50_EUR", "SPX500_USD",
#'             "XAG_NZD", "XAU_AUD", "XAU_GBP", "XAU_JPY", "XAU_EUR", "EUR_USD", "EUR_JPY",
#'             "EUR_GBP", "EUR_AUD", "EUR_SEK", "EUR_NZD") %>% unique(), #19
#'
#'           # "XAG_AUD", #20
#'           c("XAG_JPY", "XAG_GBP", "XAG_USD", "XAG_EUR", "XAU_USD", "EU50_EUR", "SPX500_USD",
#'             "XAG_NZD", "XAU_AUD", "XAU_GBP", "XAU_JPY", "XAU_EUR", "AU200_AUD",
#'             "AUD_USD", "EUR_AUD", "GBP_AUD") %>% unique(), #20
#'
#'           # "XAG_NZD", #21
#'           c("XAG_JPY", "XAG_GBP", "XAG_USD", "XAG_EUR", "XAU_USD", "EU50_EUR", "SPX500_USD",
#'             "XAG_AUD", "XAU_AUD", "XAU_GBP", "XAU_JPY", "XAU_EUR", "AU200_AUD",
#'             "NZD_USD", "GBP_NZD", "EUR_NZD") %>% unique(), #21
#'
#'           # "HK33_HKD", #22
#'           c("US2000_USD", "AU200_AUD", "USB10Y_USD", "UK100_GBP", "XAU_USD", "EU50_EUR",
#'             "SPX500_USD", "FR40_EUR", "WTICO_USD", "USD_JPY", "EUR_USD", "GBP_USD", "AU200_AUD",
#'             "SG30_SGD", "XAU_EUR", "XAG_EUR", "XAG_GBP", "XAU_GBP", "XAG_USD") %>% unique(), #22
#'
#'           # "FR40_EUR" #23
#'           c("UK100_GBP", "EU50_EUR", "XAG_USD", "AU200_AUD",
#'             "XAU_USD", "USB10Y_USD", "SPX500_USD", "EUR_USD", "EUR_AUD",
#'             "XAU_EUR", "XAG_EUR", "EUR_NZD", "EUR_JPY") %>% unique(), #23
#'
#'           # "BTC_USD", #24
#'           c("US2000_USD", "AU200_AUD", "USB10Y_USD", "UK100_GBP", "XAU_USD", "EU50_EUR",
#'             "HK33_HKD", "FR40_EUR", "WTICO_USD", "USD_JPY", "EUR_USD", "GBP_USD", "AU200_AUD",
#'             "SG30_SGD", "XAU_EUR", "XAG_EUR", "XAG_GBP", "XAU_GBP", "XAG_USD" ) %>% unique(), #24
#'
#'           # "XAG_GBP", #25
#'           c("XAG_JPY", "XAG_NZD", "XAG_USD", "XAG_EUR", "XAU_USD", "EU50_EUR", "SPX500_USD",
#'             "XAG_AUD", "XAU_AUD", "XAU_GBP", "XAU_JPY", "XAU_EUR", "UK100_GBP",
#'             "GBP_USD", "GBP_NZD", "GBP_AUD") %>% unique(), #25
#'
#'           # "GBP_AUD" #26
#'           c("GBP_JPY", "GBP_CAD", "GBP_USD", "GBP_NZD", "XAU_GBP", "XAG_GBP", "UK100_GBP",
#'             "XAU_AUD", "XAG_USD", "EUR_GBP", "EUR_USD", "XAG_AUD", "XAU_EUR", "AU200_AUD",
#'             "EUR_JPY", "UK10YB_GBP", "AUD_USD", "USD_SEK", "EUR_AUD") %>% unique(), #26
#'
#'           # "USD_SEK" #27
#'           c("AUD_USD", "EUR_USD", "GBP_USD", "USD_JPY",
#'             "XAU_USD", "USD_CAD", "NZD_USD", "XAG_USD", "XAG_USD") %>% unique(), #27
#'
#'           # "USD_SGD" #28
#'           c("AUD_USD", "EUR_USD", "GBP_USD", "USD_JPY",
#'             "XAU_USD", "USD_CAD", "NZD_USD", "XAG_USD", "WTICO_USD", "BCO_USD",
#'             "XCU_USD", "USD_SEK", "SPX500_USD", "EU50_EUR", "UK100_GBP",
#'             "NATGAS_USD") %>% unique(), #28,
#'
#'           # "NZD_USD", #29
#'           c("XAG_JPY", "XAG_GBP", "XAG_USD", "XAG_EUR", "GBP_USD", "EUR_USD", "AUD_USD",
#'             "XAG_AUD", "XAU_AUD", "USD_CAD", "USD_JPY", "XAU_EUR", "AU200_AUD",
#'             "GBP_NZD", "EUR_NZD") %>% unique(), #29
#'
#'           # "GBP_NZD", #30
#'           c("GBP_JPY", "GBP_CAD", "GBP_USD", "XAU_GBP", "XAG_GBP", "UK100_GBP",
#'             "GBP_JPY", "XAG_USD", "EUR_GBP", "NZD_USD", "EUR_NZD", "AUD_USD", "XAG_NZD",
#'             "AUD_USD", "UK10YB_GBP") %>% unique(), #30
#'
#'           # "XCU_USD", #31
#'           c("XAG_JPY", "XAG_GBP", "XAG_EUR", "XAG_AUD", "XAU_USD", "EU50_EUR", "SPX500_USD",
#'             "XAG_NZD", "XAU_AUD", "XAU_GBP", "XAU_JPY", "XAU_EUR", "EUR_USD", "USD_JPY",
#'             "GBP_USD", "AUD_USD", "USD_CAD", "USD_SEK", "XAG_USD") %>% unique(), #31
#'
#'           # "NATGAS_USD" #32
#'           c("XAG_JPY", "XAG_GBP", "XAG_EUR", "XAG_AUD", "XAU_USD", "EU50_EUR", "SPX500_USD",
#'             "XAG_NZD", "XAU_AUD", "XAU_GBP", "XAU_JPY", "XAU_EUR", "EUR_USD", "USD_JPY",
#'             "GBP_USD", "AUD_USD", "USD_CAD", "WTICO_USD", "XAG_USD") %>% unique(), #32
#'
#'           # "GBP_JPY" #33
#'           c("GBP_CAD", "GBP_USD", "XAU_GBP", "XAG_GBP", "UK100_GBP",
#'             "GBP_NZD", "XAG_USD", "EUR_GBP", "EUR_JPY", "XAU_JPY", "USD_JPY", "XAG_JPY",
#'             "AUD_USD", "UK10YB_GBP") %>% unique(), #33
#'
#'           # "SG30_SGD" #34
#'           c("AUD_USD", "EUR_USD", "GBP_USD", "USD_JPY",
#'             "XAU_USD", "US2000_USD", "NZD_USD", "XAG_USD", "WTICO_USD", "BCO_USD",
#'             "XCU_USD", "HK33_HKD", "SPX500_USD", "EU50_EUR", "UK100_GBP",
#'             "NATGAS_USD"), #34
#'
#'           # "XAU_USD", #35
#'           c("XAG_JPY", "XAG_GBP", "XAG_EUR", "XAG_AUD", "XAG_USD", "EU50_EUR", "SPX500_USD",
#'             "XAG_NZD", "XAU_AUD", "XAU_GBP", "XAU_JPY", "XAU_EUR", "EUR_USD", "USD_JPY",
#'             "GBP_USD", "AUD_USD", "USD_CAD", "USD_SEK") %>% unique(), #35
#'
#'           # "EUR_SEK", #36
#'           c("GBP_USD", "EUR_USD", "XAU_EUR", "USD_SEK", "EUR_AUD",
#'             "EUR_GBP", "EUR_NZD", "EUR_JPY", "XAG_EUR", "XAU_USD", "XAG_USD",
#'             "GBP_JPY", "FR40_EUR", "EU50_EUR") %>% unique(), #36
#'
#'           # "XAU_AUD", #37
#'           c("XAG_JPY", "XAG_GBP", "XAG_EUR", "XAG_AUD", "XAG_USD", "EU50_EUR", "SPX500_USD",
#'             "XAG_NZD", "XAU_USD", "XAU_GBP", "XAU_JPY", "XAU_EUR", "AU200_AUD", "USD_JPY",
#'             "GBP_AUD", "AUD_USD", "EUR_AUD", "AUD_USD") %>% unique(), #37
#'
#'           # "UK10YB_GBP", #38
#'           c("XAU_GBP", "XAG_GBP", "XAU_USD", "EUR_GBP", "XAU_EUR", "GBP_AUD", "GBP_NZD",
#'             "SPX500_USD", "BCO_USD", "UK100_GBP", "USB10Y_USD", "GBP_CAD", "GBP_JPY",
#'             "XAG_GBP", "WTICO_USD", "GBP_USD") %>% unique(), #38
#'
#'           # "JP225Y_JPY" #39
#'           c("US2000_USD", "AU200_AUD", "USB10Y_USD", "UK100_GBP", "XAU_USD", "EU50_EUR",
#'             "SPX500_USD", "FR40_EUR", "WTICO_USD", "USD_JPY", "EUR_USD", "AU200_AUD",
#'             "SG30_SGD", "XAU_EUR", "XAG_JPY", "XAG_GBP", "XAU_JPY", "XAG_USD") %>% unique(), #39
#'
#'           # "ETH_USD" #40
#'           c("US2000_USD", "AU200_AUD", "USB10Y_USD", "UK100_GBP", "XAU_USD", "EU50_EUR",
#'             "HK33_HKD", "FR40_EUR", "WTICO_USD", "USD_JPY", "EUR_USD", "GBP_USD", "AU200_AUD",
#'             "BTC_USD", "XAU_EUR", "XAG_EUR", "XAG_GBP", "XAU_GBP", "XAG_USD" ) %>% unique()
#'         ),
#'       countries_for_int_strength =
#'         list(
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #1
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #2
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #3
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #4
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #5
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #6
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #7
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #8
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #9
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #10
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #11
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #12
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #13
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #14
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #15
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #16
#'
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #17
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #18
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #19
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #20
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #21
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #22
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #23
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #24
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #25
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #26
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #27
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #28
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #29
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #30
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #31
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #32
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #33
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #34
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #35
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #36
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #37
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #38
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #39
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD") #40
#'         )
#'     )
#'
#'     assets_to_analyse <-
#'       indicator_mapping$Asset
#'
#'     temp_actual_wins_losses <- list()
#'
#'     for (i in 1:length(assets_to_analyse)) {
#'
#'       temp_actual_wins_losses[[i]] <-
#'         create_running_profits(
#'           asset_of_interest = assets_to_analyse[i],
#'           asset_data = Indices_Metals_Bonds,
#'           stop_factor = stop_value_var,
#'           profit_factor = profit_value_var,
#'           risk_dollar_value = risk_dollar_value,
#'           trade_direction = trade_direction,
#'           currency_conversion = currency_conversion,
#'           asset_infor = asset_infor
#'         )
#'
#'     }
#'
#'     actual_wins_losses <-
#'       temp_actual_wins_losses %>%
#'       map_dfr(bind_rows) %>%
#'       dplyr::select(-volume_unadj, -minimumTradeSize_OG, -marginRate,
#'                     -adjusted_conversion, -pipLocation, -minimumTradeSize_OG) %>%
#'       dplyr::rename(
#'         High = Bid_High,
#'         Low =  Bid_Low
#'       ) %>%
#'       mutate(
#'         trade_return_dollar_aud = !!as.name(glue::glue("period_return_{period_var}_Price") ),
#'
#'         trade_start_prices =
#'           case_when(
#'             trade_col == "Long" ~ Ask_Price,
#'             trade_col == "Short" ~ Bid_Price
#'           ),
#'         trade_end_prices =
#'           case_when(
#'             trade_col == "Long" ~ Bid_Price,
#'             trade_col == "Short" ~ Ask_Price
#'           ),
#'         stop_factor = stop_value_var,
#'         profit_factor = profit_value_var,
#'         periods_ahead = period_var
#'       )
#'
#'     model_data_store_db <-
#'       connect_db(model_data_store_path)
#'
#'     date_test_start = as.character(as_date(date_train_phase_2_end_pre) + days(3))
#'     c = 0
#'     redo_db = TRUE
#'
#'     for (j in 1:length(indicator_mapping$Asset) ) {
#'
#'       countries_for_int_strength <-
#'         unlist(indicator_mapping$countries_for_int_strength[j])
#'       couplua_assets = unlist(indicator_mapping$couplua_assets[j])
#'       Asset_of_interest = unlist(indicator_mapping$Asset[j])
#'
#'
#'       single_asset_algo_generate_models_V2_V3(
#'         asset_data = Indices_Metals_Bonds[[1]],
#'         All_Daily_Data = All_Daily_Data,
#'         Asset_of_interest = Asset_of_interest,
#'         actual_wins_losses = actual_wins_losses,
#'         interest_rates = interest_rates,
#'         cpi_data = cpi_data,
#'         sentiment_index = sentiment_index,
#'         gdp_data = gdp_data,
#'         unemp_data = unemp_data,
#'         manufac_pmi = manufac_pmi,
#'         USD_Macro = USD_Macro,
#'         EUR_Macro = EUR_Macro,
#'         equity_index = equity_index,
#'         gold_index = gold_index,
#'         silver_index = silver_index,
#'         bonds_index = bonds_index,
#'         USD_index = USD_index,
#'         EUR_index = EUR_index,
#'         GBP_index = GBP_index,
#'         AUD_index = AUD_index,
#'         COMMOD_index = COMMOD_index,
#'         USD_STOCKS_index = USD_STOCKS_index,
#'         NZD_index = NZD_index,
#'         countries_for_int_strength = countries_for_int_strength,
#'
#'         date_train_end = date_train_end_pre,
#'         date_train_phase_2_end = date_train_phase_2_end_pre,
#'         date_test_start = as.character(date_test_start),
#'
#'         couplua_assets = couplua_assets,
#'         stop_value_var = stop_value_var,
#'         profit_value_var = profit_value_var,
#'         period_var = period_var,
#'         bin_var_col = bin_var_col,
#'         trade_direction = trade_direction,
#'         save_path = save_path
#'       )
#'
#'
#'       long_sim <-
#'         single_asset_Logit_indicator_V3_V2_get_pred(
#'           asset_data = Indices_Metals_Bonds[[1]],
#'           All_Daily_Data = All_Daily_Data,
#'           Asset_of_interest = Asset_of_interest,
#'           actual_wins_losses = actual_wins_losses,
#'
#'           interest_rates = interest_rates,
#'           cpi_data = cpi_data,
#'           sentiment_index = sentiment_index,
#'           gdp_data = gdp_data,
#'           unemp_data = unemp_data,
#'           manufac_pmi = manufac_pmi,
#'           USD_Macro = USD_Macro,
#'           EUR_Macro = EUR_Macro,
#'
#'           equity_index = equity_index,
#'           gold_index = gold_index,
#'           silver_index = silver_index,
#'           bonds_index = bonds_index,
#'           USD_index = USD_index,
#'           EUR_index = EUR_index,
#'           GBP_index = GBP_index,
#'           AUD_index = AUD_index,
#'           COMMOD_index = COMMOD_index,
#'           USD_STOCKS_index = USD_STOCKS_index,
#'           NZD_index = NZD_index,
#'
#'           countries_for_int_strength = countries_for_int_strength,
#'
#'           date_train_end = date_train_end_pre,
#'           date_train_phase_2_end = date_train_phase_2_end_pre,
#'           date_test_start = as.character(date_test_start),
#'
#'           couplua_assets = couplua_assets,
#'
#'           stop_value_var = stop_value_var,
#'           profit_value_var = profit_value_var,
#'           period_var = period_var,
#'
#'           bin_var_col = bin_var_col,
#'           trade_direction = trade_direction,
#'           save_path = save_path
#'         )
#'
#'       long_sim_transformed <-
#'         long_sim %>%
#'         filter(Date >= date_test_start) %>%
#'         left_join(actual_wins_losses %>%
#'                     filter(trade_col == trade_direction,
#'                            stop_factor == stop_value_var,
#'                            profit_factor == profit_value_var)
#'         ) %>%
#'         mutate(
#'           trade_col = trade_direction,
#'           test_end_date = test_end_date,
#'           date_train_end = date_train_end_pre,
#'           date_train_phase_2_end = date_train_phase_2_end_pre,
#'           date_test_start = date_test_start,
#'           sim_index = 1,
#'           bin_var_col = paste(bin_var_col, collapse = ", ")
#'         )
#'
#'       complete_sim <-
#'         list(long_sim_transformed) %>%
#'         map_dfr(bind_rows)
#'
#'       if(dim(complete_sim)[1] > 0) {
#'         c = c + 1
#'         if(redo_db == TRUE & c == 1) {
#'           write_table_sql_lite(.data = complete_sim,
#'                                table_name = "single_asset_improved",
#'                                conn = model_data_store_db,
#'                                overwrite_true = TRUE)
#'           redo_db = FALSE
#'         }
#'
#'         if(redo_db == FALSE) {
#'           append_table_sql_lite(.data = complete_sim,
#'                                 table_name = "single_asset_improved",
#'                                 conn = model_data_store_db)
#'         }
#'       }
#'
#'     }
#'
#'     DBI::dbDisconnect(model_data_store_db)
#'     gc()
#'     rm(model_data_store_db)
#'
#'     indicator_data <-
#'       get_indicator_pred_from_db(
#'         model_data_store_path = model_data_store_path,
#'         table_name = "single_asset_improved"
#'       )
#'
#'     post_bins_cols %>%
#'       map(
#'         ~
#'           prepare_post_ss_gen2_model(
#'             indicator_data = indicator_data,
#'             actual_wins_losses = actual_wins_losses,
#'             new_post_DB = TRUE,
#'             new_sym = TRUE,
#'             post_model_data_save_path = save_path,
#'             dependant_var = .x,
#'             dependant_threshold = post_dependant_threshold,
#'             training_date_start = training_date_start_post,
#'             training_date_end = training_date_end_post
#'           )
#'       )
#'
#'   }
#'
#'
#' #' single_asset_algo_generate_preds
#' #'
#' #' @param All_Daily_Data
#' #' @param Indices_Metals_Bonds
#' #' @param raw_macro_data
#' #' @param currency_conversion
#' #' @param asset_infor
#' #' @param start_index
#' #' @param end_index
#' #' @param risk_dollar_value
#' #' @param trade_direction
#' #' @param stop_value_var
#' #' @param profit_value_var
#' #' @param period_var
#' #' @param bin_var_col
#' #' @param date_train_end_pre
#' #' @param date_train_phase_2_end_pre
#' #' @param training_date_start_post
#' #' @param training_date_end_post
#' #' @param model_data_store_path
#' #' @param save_path
#' #'
#' #' @returns
#' #' @export
#' #'
#' #' @examples
#' single_asset_algo_generate_preds_V2_V3 <-
#'   function(
#'     All_Daily_Data = All_Daily_Data,
#'     Indices_Metals_Bonds = Indices_Metals_Bonds,
#'     raw_macro_data = raw_macro_data,
#'     currency_conversion = currency_conversion,
#'     asset_infor = asset_infor,
#'     start_index = 1,
#'     end_index = 40,
#'     risk_dollar_value = 15,
#'     trade_direction = "Long",
#'     stop_value_var = 5,
#'     profit_value_var = 30,
#'     period_var = 24,
#'     bin_var_col = c("period_return_20_Price", "period_return_24_Price", "period_return_28_Price"),
#'     date_train_end_pre = "2023-06-01",
#'     date_train_phase_2_end_pre = "2024-06-01",
#'     training_date_start_post = "2024-07-04",
#'     training_date_end_post = "2025-09-01",
#'     test_end_date = as.character(today()),
#'     post_bins_cols =
#'       c("period_return_24_Price",
#'         "period_return_30_Price",
#'         "period_return_44_Price"),
#'     post_dependant_threshold = 5,
#'     post_dependant_var = "period_return_24_Price",
#'     model_data_store_path = "C:/Users/nikhi/Documents/trade_data/Day_Trader_Single_Asset_V2_trade_store_stop_2.db",
#'     save_path = "C:/Users/nikhi/Documents/trade_data/Day_Trader_Single_Asset_V2_trade_store_stop_2/"
#'   ) {
#'
#'     equity_index <-
#'       get_equity_index(index_data = Indices_Metals_Bonds[[1]])
#'
#'     gold_index <-
#'       get_Gold_index(index_data = Indices_Metals_Bonds[[1]])
#'
#'     silver_index <-
#'       get_silver_index(index_data = Indices_Metals_Bonds[[1]])
#'
#'     bonds_index <-
#'       get_bonds_index(index_data = Indices_Metals_Bonds[[1]])
#'
#'     USD_index <-
#'       get_USD_index_for_models(index_data = Indices_Metals_Bonds[[1]])
#'
#'     EUR_index <-
#'       get_EUR_index_for_models(index_data = Indices_Metals_Bonds[[1]])
#'
#'     GBP_index <-
#'       get_GBP_index_for_models(index_data = Indices_Metals_Bonds[[1]])
#'
#'     AUD_index <-
#'       get_AUD_index_for_models(index_data = Indices_Metals_Bonds[[1]])
#'
#'     COMMOD_index <-
#'       get_COMMOD_index_for_models(index_data = Indices_Metals_Bonds[[1]])
#'
#'     USD_STOCKS_index <-
#'       get_USD_AND_STOCKS_index_for_models(index_data = Indices_Metals_Bonds[[1]])
#'
#'     NZD_index <-
#'       get_NZD_index_for_models(index_data = Indices_Metals_Bonds[[1]])
#'
#'     interest_rates <-
#'       get_interest_rates(
#'         raw_macro_data = raw_macro_data,
#'         lag_days = 1
#'       )
#'
#'     cpi_data <-
#'       get_cpi(
#'         raw_macro_data = raw_macro_data,
#'         lag_days = 1
#'       )
#'
#'     sentiment_index <-
#'       create_sentiment_index(
#'         raw_macro_data = raw_macro_data,
#'         lag_days = 1,
#'         date_start = "2011-01-01",
#'         end_date = today(tz = "Australia/Canberra") %>% as.character(),
#'         first_difference = TRUE,
#'         scale_values = FALSE
#'       )
#'
#'     gdp_data <-
#'       get_GDP_countries(
#'         raw_macro_data = raw_macro_data,
#'         lag_days = 1
#'       )
#'
#'     unemp_data <-
#'       get_unemp_countries(
#'         raw_macro_data = raw_macro_data,
#'         lag_days = 1
#'       )
#'
#'     manufac_pmi <-
#'       get_manufac_countries(
#'         raw_macro_data = raw_macro_data,
#'         lag_days = 1
#'       )
#'
#'     USD_Macro <-
#'       get_additional_USD_Macro(
#'         raw_macro_data = raw_macro_data,
#'         lag_days = 1
#'       )
#'
#'     EUR_Macro <-
#'       get_additional_EUR_Macro(
#'         raw_macro_data = raw_macro_data,
#'         lag_days = 1
#'       )
#'
#'     indicator_mapping <- list(
#'       Asset = c("EUR_USD", #1
#'                 "EU50_EUR", #2
#'                 "SPX500_USD", #3
#'                 "US2000_USD", #4
#'                 "USB10Y_USD", #5
#'                 "USD_JPY", #6
#'                 "AUD_USD", #7
#'                 "EUR_GBP", #8
#'                 "AU200_AUD" ,#9
#'                 "EUR_AUD", #10
#'                 "WTICO_USD", #11
#'                 "UK100_GBP", #12
#'                 "USD_CAD", #13
#'                 "GBP_USD", #14
#'                 "GBP_CAD", #15
#'                 "EUR_JPY", #16
#'                 "EUR_NZD", #17
#'                 "XAG_USD", #18
#'                 "XAG_EUR", #19
#'                 "XAG_AUD", #20
#'                 "XAG_NZD", #21
#'                 "HK33_HKD", #22
#'                 # "FR40_EUR", #23
#'                 # "BTC_USD", #24
#'                 "XAG_GBP", #25
#'                 "GBP_AUD", #26
#'                 "USD_SEK", #27
#'                 "USD_SGD", #28
#'                 "NZD_USD", #29
#'                 "GBP_NZD", #30
#'                 "XCU_USD", #31
#'                 "NATGAS_USD", #32
#'                 "GBP_JPY", #33
#'                 "SG30_SGD", #34
#'                 "XAU_USD", #35
#'                 "EUR_SEK", #36
#'                 "XAU_AUD", #37
#'                 "UK10YB_GBP", #38
#'                 "JP225Y_JPY", #39
#'                 "ETH_USD" #40
#'       ),
#'       couplua_assets =
#'         list(
#'           # EUR_USD
#'           c("XAU_EUR", "XAG_EUR", "EUR_JPY", "EU50_EUR", "EUR_AUD", "EUR_GBP",
#'             "SPX500_USD", "XAU_USD", "USD_JPY", "GBP_USD", "EUR_NZD", "XAG_GBP", "XAU_GBP",
#'             "EUR_SEK", "USD_CAD") %>% unique(), #1
#'
#'           # EU50_EUR
#'           c("XAU_EUR", "XAG_EUR", "XAU_USD", "UK100_GBP", "SG30_SGD", "EUR_GBP", "SPX500_USD",
#'             "SPX500_USD", "XAU_USD", "AU200_AUD", "CH20_CHF", "US2000_USD",
#'             "XAG_GBP", "XAU_GBP", "WTICO_USD", "FR40_EUR", "HK33_HKD") %>% unique(), #2
#'
#'           # SPX500_USD
#'           c("US2000_USD", "AU200_AUD", "USB10Y_USD", "UK100_GBP", "XAU_USD", "EU50_EUR",
#'             "HK33_HKD", "FR40_EUR", "WTICO_USD", "USD_JPY", "EUR_USD", "GBP_USD", "AU200_AUD",
#'             "SG30_SGD", "XAU_EUR", "XAG_EUR", "XAG_GBP", "XAU_GBP", "XAG_USD" ) %>% unique(), #3
#'
#'           # US2000_USD
#'           c("SPX500_USD",  "AU200_AUD", "USB10Y_USD", "UK100_GBP", "XAU_USD", "EU50_EUR",
#'             "HK33_HKD", "FR40_EUR", "WTICO_USD", "USD_JPY", "EUR_USD", "GBP_USD", "AU200_AUD",
#'             "SG30_SGD", "XAU_EUR", "XAG_EUR", "XAG_GBP", "XAU_GBP","XAG_USD" ) %>% unique(), #4
#'
#'           # USB10Y_USD
#'           c("SPX500_USD",  "AU200_AUD", "UK100_GBP", "XAU_USD", "EU50_EUR",
#'             "HK33_HKD", "FR40_EUR", "WTICO_USD", "USD_JPY", "EUR_USD",
#'             "XAU_EUR", "AU200_AUD", "XAG_USD",
#'             "SG30_SGD", "XAU_EUR", "XAG_EUR", "XAG_GBP", "XAU_GBP") %>% unique(), #5
#'
#'           # USD_JPY
#'           c("EUR_JPY", "XAU_JPY", "XAG_JPY", "GBP_JPY", "XAU_USD", "SPX500_USD",
#'             "XAG_USD","NZD_USD", "AUD_USD", "EUR_USD", "GBP_USD", "USD_CAD",
#'             "USD_SEK", "USD_SGD", "USB10Y_USD") %>% unique(), #6
#'
#'           # AUD_USD
#'           c("XCU_USD", "AU200_AUD", "XAU_AUD", "GBP_AUD", "XAU_USD", "EUR_AUD",
#'             "XAG_USD","NZD_USD", "USD_JPY", "EUR_USD", "GBP_USD", "USD_CAD",
#'             "USD_SEK", "USD_SGD", "USB10Y_USD", "NZD_USD") %>% unique(), #7
#'
#'           # EUR_GBP
#'           c("GBP_USD", "EUR_USD", "XAU_EUR", "XAU_GBP", "GBP_JPY", "EUR_JPY",
#'             "XAG_EUR", "XAG_GBP", "USD_JPY", "UK100_GBP", "FR40_EUR", "EU50_EUR",
#'             "EUR_SEK", "USD_SEK", "EUR_AUD", "EUR_NZD", "EUR_SEK") %>% unique(), #8
#'
#'           # AU200_AUD
#'           c("XCU_USD", "US2000_USD", "UK100_GBP", "XAU_USD", "EU50_EUR",
#'             "HK33_HKD", "FR40_EUR", "WTICO_USD", "GBP_AUD", "EUR_AUD",
#'             "SG30_SGD", "XAU_EUR", "XAG_EUR", "XAG_GBP", "XAU_GBP", "XAG_USD" ) %>% unique(), #9
#'
#'           # EUR_AUD
#'           c("XCU_USD", "AU200_AUD", "XAU_AUD", "GBP_AUD", "XAU_USD", "AUD_USD",
#'             "XAG_USD","NZD_USD", "USD_JPY", "EUR_USD",
#'             "USB10Y_USD", "NZD_USD", "FR40_EUR", "EU50_EUR",
#'             "EUR_SEK", "EUR_NZD", "EUR_SEK") %>% unique(), #10
#'
#'           # WTICO_USD
#'           c("NATGAS_USD", "XAG_USD", "BCO_USD", "SPX500_USD", "UK10YB_GBP", "XAU_USD",
#'             "US2000_USD", "AU200_AUD", "USB10Y_USD", "UK100_GBP", "XAU_USD", "EU50_EUR",
#'             "HK33_HKD", "FR40_EUR", "USD_JPY", "EUR_USD", "GBP_USD",
#'             "XAU_EUR", "XAG_EUR", "XAG_GBP", "XAU_GBP") %>% unique(), #11
#'
#'           # "UK100_GBP", #12
#'           c("XAU_EUR", "XAG_EUR", "XAU_USD", "SG30_SGD", "EUR_GBP", "US2000_USD",
#'             "SPX500_USD", "XAU_USD", "AU200_AUD", "CH20_CHF", "UK10YB_GBP", "USB10Y_USD",
#'             "XAG_GBP", "XAU_GBP", "WTICO_USD", "FR40_EUR", "HK33_HKD") %>% unique(), #12
#'
#'           # "USD_CAD", #13
#'           c("XAU_JPY", "XAU_GBP", "XAU_EUR", "XAU_USD", "EUR_JPY", "GBP_JPY",
#'             "XAG_USD","NZD_USD", "USD_JPY", "EUR_USD", "GBP_USD", "GBP_CAD",
#'             "USD_SEK", "USD_SGD", "USB10Y_USD") %>% unique(), #13
#'
#'           # "GBP_USD", #14
#'           c("GBP_JPY", "GBP_CAD", "GBP_AUD", "GBP_NZD", "XAU_GBP", "XAG_GBP", "UK100_GBP",
#'             "XAU_USD", "XAG_USD", "EUR_GBP", "EUR_USD", "XAG_EUR", "XAU_EUR", "USD_JPY",
#'             "EUR_JPY", "UK10YB_GBP", "AUD_USD", "USD_SEK", "USD_CAD") %>% unique(), #14
#'
#'           # "GBP_CAD", #15
#'           c("GBP_JPY", "GBP_USD", "GBP_AUD", "GBP_NZD", "XAU_GBP", "XAG_GBP", "UK100_GBP",
#'             "XAU_USD", "XAG_USD", "EUR_GBP", "EUR_USD", "XAG_EUR", "XAU_EUR", "USD_JPY",
#'             "EUR_JPY", "UK10YB_GBP", "AUD_USD", "USD_SEK", "USD_CAD") %>% unique(), #15
#'
#'           # "EUR_JPY", #16
#'           c("GBP_USD", "EUR_USD", "XAU_EUR", "XAU_JPY", "USD_JPY", "EUR_AUD",
#'             "EUR_GBP", "EUR_NZD", "EUR_SEK", "XAG_EUR", "XAU_USD", "XAG_USD", "USD_JPY",
#'             "GBP_JPY", "FR40_EUR", "EU50_EUR") %>% unique(), #16
#'
#'           # "EUR_NZD", #17
#'           c("EUR_AUD", "EUR_USD", "XAU_EUR", "XAU_AUD", "NZD_USD", "EUR_JPY", "EUR_GBP",
#'             "GBP_NZD", "XAG_NZD", "XAG_EUR", "XAU_USD", "XAG_USD", "EUR_SEK",
#'             "FR40_EUR", "EU50_EUR", "AU200_AUD") %>% unique(), #17
#'
#'           # "XAG_USD", #18
#'           c("XAG_JPY", "XAG_GBP", "XAG_EUR", "XAG_AUD", "XAU_USD", "EU50_EUR", "SPX500_USD",
#'             "XAG_NZD", "XAU_AUD", "XAU_GBP", "XAU_JPY", "XAU_EUR", "EUR_USD", "USD_JPY",
#'             "GBP_USD", "AUD_USD", "USD_CAD", "USD_SEK") %>% unique(), #18
#'
#'           # "XAG_EUR", #19
#'           c("XAG_JPY", "XAG_GBP", "XAG_USD", "XAG_AUD", "XAU_USD", "EU50_EUR", "SPX500_USD",
#'             "XAG_NZD", "XAU_AUD", "XAU_GBP", "XAU_JPY", "XAU_EUR", "EUR_USD", "EUR_JPY",
#'             "EUR_GBP", "EUR_AUD", "EUR_SEK", "EUR_NZD") %>% unique(), #19
#'
#'           # "XAG_AUD", #20
#'           c("XAG_JPY", "XAG_GBP", "XAG_USD", "XAG_EUR", "XAU_USD", "EU50_EUR", "SPX500_USD",
#'             "XAG_NZD", "XAU_AUD", "XAU_GBP", "XAU_JPY", "XAU_EUR", "AU200_AUD",
#'             "AUD_USD", "EUR_AUD", "GBP_AUD") %>% unique(), #20
#'
#'           # "XAG_NZD", #21
#'           c("XAG_JPY", "XAG_GBP", "XAG_USD", "XAG_EUR", "XAU_USD", "EU50_EUR", "SPX500_USD",
#'             "XAG_AUD", "XAU_AUD", "XAU_GBP", "XAU_JPY", "XAU_EUR", "AU200_AUD",
#'             "NZD_USD", "GBP_NZD", "EUR_NZD") %>% unique(), #21
#'
#'           # "HK33_HKD", #22
#'           c("US2000_USD", "AU200_AUD", "USB10Y_USD", "UK100_GBP", "XAU_USD", "EU50_EUR",
#'             "SPX500_USD", "FR40_EUR", "WTICO_USD", "USD_JPY", "EUR_USD", "GBP_USD", "AU200_AUD",
#'             "SG30_SGD", "XAU_EUR", "XAG_EUR", "XAG_GBP", "XAU_GBP", "XAG_USD") %>% unique(), #22
#'
#'           # # "FR40_EUR" #23
#'           # c("UK100_GBP", "EU50_EUR", "XAG_USD", "AU200_AUD",
#'           #   "XAU_USD", "USB10Y_USD", "SPX500_USD", "EUR_USD", "EUR_AUD",
#'           #   "XAU_EUR", "XAG_EUR", "EUR_NZD", "EUR_JPY") %>% unique(), #23
#'
#'           # # "BTC_USD", #24
#'           # c("US2000_USD", "AU200_AUD", "USB10Y_USD", "UK100_GBP", "XAU_USD", "EU50_EUR",
#'           #   "HK33_HKD", "FR40_EUR", "WTICO_USD", "USD_JPY", "EUR_USD", "GBP_USD", "AU200_AUD",
#'           #   "SG30_SGD", "XAU_EUR", "XAG_EUR", "XAG_GBP", "XAU_GBP", "XAG_USD" ) %>% unique(), #24
#'
#'           # "XAG_GBP", #25
#'           c("XAG_JPY", "XAG_NZD", "XAG_USD", "XAG_EUR", "XAU_USD", "EU50_EUR", "SPX500_USD",
#'             "XAG_AUD", "XAU_AUD", "XAU_GBP", "XAU_JPY", "XAU_EUR", "UK100_GBP",
#'             "GBP_USD", "GBP_NZD", "GBP_AUD") %>% unique(), #25
#'
#'           # "GBP_AUD" #26
#'           c("GBP_JPY", "GBP_CAD", "GBP_USD", "GBP_NZD", "XAU_GBP", "XAG_GBP", "UK100_GBP",
#'             "XAU_AUD", "XAG_USD", "EUR_GBP", "EUR_USD", "XAG_AUD", "XAU_EUR", "AU200_AUD",
#'             "EUR_JPY", "UK10YB_GBP", "AUD_USD", "USD_SEK", "EUR_AUD") %>% unique(), #26
#'
#'           # "USD_SEK" #27
#'           c("AUD_USD", "EUR_USD", "GBP_USD", "USD_JPY",
#'             "XAU_USD", "USD_CAD", "NZD_USD", "XAG_USD", "XAG_USD") %>% unique(), #27
#'
#'           # "USD_SGD" #28
#'           c("AUD_USD", "EUR_USD", "GBP_USD", "USD_JPY",
#'             "XAU_USD", "USD_CAD", "NZD_USD", "XAG_USD", "WTICO_USD", "BCO_USD",
#'             "XCU_USD", "USD_SEK", "SPX500_USD", "EU50_EUR", "UK100_GBP",
#'             "NATGAS_USD") %>% unique(), #28,
#'
#'           # "NZD_USD", #29
#'           c("XAG_JPY", "XAG_GBP", "XAG_USD", "XAG_EUR", "GBP_USD", "EUR_USD", "AUD_USD",
#'             "XAG_AUD", "XAU_AUD", "USD_CAD", "USD_JPY", "XAU_EUR", "AU200_AUD",
#'             "GBP_NZD", "EUR_NZD") %>% unique(), #29
#'
#'           # "GBP_NZD", #30
#'           c("GBP_JPY", "GBP_CAD", "GBP_USD", "XAU_GBP", "XAG_GBP", "UK100_GBP",
#'             "GBP_JPY", "XAG_USD", "EUR_GBP", "NZD_USD", "EUR_NZD", "AUD_USD", "XAG_NZD",
#'             "AUD_USD", "UK10YB_GBP") %>% unique(), #30
#'
#'           # "XCU_USD", #31
#'           c("XAG_JPY", "XAG_GBP", "XAG_EUR", "XAG_AUD", "XAU_USD", "EU50_EUR", "SPX500_USD",
#'             "XAG_NZD", "XAU_AUD", "XAU_GBP", "XAU_JPY", "XAU_EUR", "EUR_USD", "USD_JPY",
#'             "GBP_USD", "AUD_USD", "USD_CAD", "USD_SEK", "XAG_USD") %>% unique(), #31
#'
#'           # "NATGAS_USD" #32
#'           c("XAG_JPY", "XAG_GBP", "XAG_EUR", "XAG_AUD", "XAU_USD", "EU50_EUR", "SPX500_USD",
#'             "XAG_NZD", "XAU_AUD", "XAU_GBP", "XAU_JPY", "XAU_EUR", "EUR_USD", "USD_JPY",
#'             "GBP_USD", "AUD_USD", "USD_CAD", "WTICO_USD", "XAG_USD") %>% unique(), #32
#'
#'           # "GBP_JPY" #33
#'           c("GBP_CAD", "GBP_USD", "XAU_GBP", "XAG_GBP", "UK100_GBP",
#'             "GBP_NZD", "XAG_USD", "EUR_GBP", "EUR_JPY", "XAU_JPY", "USD_JPY", "XAG_JPY",
#'             "AUD_USD", "UK10YB_GBP") %>% unique(), #33
#'
#'           # "SG30_SGD" #34
#'           c("AUD_USD", "EUR_USD", "GBP_USD", "USD_JPY",
#'             "XAU_USD", "US2000_USD", "NZD_USD", "XAG_USD", "WTICO_USD", "BCO_USD",
#'             "XCU_USD", "HK33_HKD", "SPX500_USD", "EU50_EUR", "UK100_GBP",
#'             "NATGAS_USD"), #34
#'
#'           # "XAU_USD", #35
#'           c("XAG_JPY", "XAG_GBP", "XAG_EUR", "XAG_AUD", "XAG_USD", "EU50_EUR", "SPX500_USD",
#'             "XAG_NZD", "XAU_AUD", "XAU_GBP", "XAU_JPY", "XAU_EUR", "EUR_USD", "USD_JPY",
#'             "GBP_USD", "AUD_USD", "USD_CAD", "USD_SEK") %>% unique(), #35
#'
#'           # "EUR_SEK", #36
#'           c("GBP_USD", "EUR_USD", "XAU_EUR", "USD_SEK", "EUR_AUD",
#'             "EUR_GBP", "EUR_NZD", "EUR_JPY", "XAG_EUR", "XAU_USD", "XAG_USD",
#'             "GBP_JPY", "FR40_EUR", "EU50_EUR") %>% unique(), #36
#'
#'           # "XAU_AUD", #37
#'           c("XAG_JPY", "XAG_GBP", "XAG_EUR", "XAG_AUD", "XAG_USD", "EU50_EUR", "SPX500_USD",
#'             "XAG_NZD", "XAU_USD", "XAU_GBP", "XAU_JPY", "XAU_EUR", "AU200_AUD", "USD_JPY",
#'             "GBP_AUD", "AUD_USD", "EUR_AUD", "AUD_USD") %>% unique(), #37
#'
#'           # "UK10YB_GBP", #38
#'           c("XAU_GBP", "XAG_GBP", "XAU_USD", "EUR_GBP", "XAU_EUR", "GBP_AUD", "GBP_NZD",
#'             "SPX500_USD", "BCO_USD", "UK100_GBP", "USB10Y_USD", "GBP_CAD", "GBP_JPY",
#'             "XAG_GBP", "WTICO_USD", "GBP_USD") %>% unique(), #38
#'
#'           # "JP225Y_JPY" #39
#'           c("US2000_USD", "AU200_AUD", "USB10Y_USD", "UK100_GBP", "XAU_USD", "EU50_EUR",
#'             "SPX500_USD", "FR40_EUR", "WTICO_USD", "USD_JPY", "EUR_USD", "AU200_AUD",
#'             "SG30_SGD", "XAU_EUR", "XAG_JPY", "XAG_GBP", "XAU_JPY", "XAG_USD") %>% unique(), #39
#'
#'           # "ETH_USD" #40
#'           c("US2000_USD", "AU200_AUD", "USB10Y_USD", "UK100_GBP", "XAU_USD", "EU50_EUR",
#'             "HK33_HKD", "FR40_EUR", "WTICO_USD", "USD_JPY", "EUR_USD", "GBP_USD", "AU200_AUD",
#'             "BTC_USD", "XAU_EUR", "XAG_EUR", "XAG_GBP", "XAU_GBP", "XAG_USD" ) %>% unique()
#'         ),
#'       countries_for_int_strength =
#'         list(
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #1
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #2
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #3
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #4
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #5
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #6
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #7
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #8
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #9
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #10
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #11
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #12
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #13
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #14
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #15
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #16
#'
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #17
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #18
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #19
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #20
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #21
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #22
#'           # c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #23
#'           # c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #24
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #25
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #26
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #27
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #28
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #29
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #30
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #31
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #32
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #33
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #34
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #35
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #36
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #37
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #38
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD"), #39
#'           c("GBP", "USD", "EUR", "AUD", "JPY", "NZD", "CAD") #40
#'         )
#'     )
#'
#'     assets_to_analyse <-
#'       indicator_mapping$Asset
#'
#'     all_pred_data <- list()
#'
#'     date_test_start = as.character(as_date(date_train_phase_2_end_pre) + days(3))
#'
#'     for (j in start_index:end_index ) {
#'
#'       tictoc::tic()
#'
#'       countries_for_int_strength <-
#'         unlist(indicator_mapping$countries_for_int_strength[j])
#'       couplua_assets = unlist(indicator_mapping$couplua_assets[j])
#'       Asset_of_interest = unlist(indicator_mapping$Asset[j])
#'
#'
#'       long_sim <-
#'         single_asset_Logit_indicator_V3_V2_get_pred(
#'           asset_data = Indices_Metals_Bonds[[1]],
#'           All_Daily_Data = All_Daily_Data,
#'           Asset_of_interest = Asset_of_interest,
#'           actual_wins_losses = NULL,
#'
#'           interest_rates = interest_rates,
#'           cpi_data = cpi_data,
#'           sentiment_index = sentiment_index,
#'           gdp_data = gdp_data,
#'           unemp_data = unemp_data,
#'           manufac_pmi = manufac_pmi,
#'           USD_Macro = USD_Macro,
#'           EUR_Macro = EUR_Macro,
#'
#'           equity_index = equity_index,
#'           gold_index = gold_index,
#'           silver_index = silver_index,
#'           bonds_index = bonds_index,
#'           USD_index = USD_index,
#'           EUR_index = EUR_index,
#'           GBP_index = GBP_index,
#'           AUD_index = AUD_index,
#'           COMMOD_index = COMMOD_index,
#'           USD_STOCKS_index = USD_STOCKS_index,
#'           NZD_index = NZD_index,
#'
#'           countries_for_int_strength = countries_for_int_strength,
#'
#'           date_train_end = date_train_end_pre,
#'           date_train_phase_2_end = date_train_phase_2_end_pre,
#'           date_test_start = as.character(date_test_start),
#'
#'           couplua_assets = couplua_assets,
#'
#'           stop_value_var = stop_value_var,
#'           profit_value_var = profit_value_var,
#'           period_var = period_var,
#'
#'           bin_var_col = bin_var_col,
#'           trade_direction = trade_direction,
#'           save_path = save_path
#'         )
#'
#'       long_sim_transformed <-
#'         long_sim %>%
#'         filter(Date >= date_test_start) %>%
#'         mutate(
#'           trade_col = trade_direction,
#'           test_end_date = test_end_date,
#'           date_train_end = date_train_end_pre,
#'           date_train_phase_2_end = date_train_phase_2_end_pre,
#'           date_test_start = date_test_start,
#'           sim_index = 1,
#'           bin_var_col = paste(bin_var_col, collapse = ", ")
#'         )
#'
#'       complete_sim <-
#'         list(long_sim_transformed) %>%
#'         map_dfr(bind_rows)
#'
#'       all_pred_data[[j]] <- complete_sim
#'
#'       rm(complete_sim, long_sim_transformed, long_sim)
#'       gc()
#'
#'       tictoc::toc()
#'
#'     }
#'
#'     all_pred_data <-
#'       all_pred_data %>%
#'       map_dfr(bind_rows)
#'
#'
#'     post_preds_all <-
#'       read_post_models_and_get_preds(
#'         indicator_data = all_pred_data,
#'         post_model_data_save_path =save_path,
#'         test_date_start = training_date_start_post,
#'         test_date_end = as.character(today() + days(100)) ,
#'         dependant_var = post_dependant_var,
#'         dependant_threshold = post_dependant_threshold,
#'         ignore_dependant_var = TRUE
#'       )
#'
#'
#'     post_preds_all_rolling <-
#'       get_rolling_post_preds(
#'         post_pred_data = post_preds_all,
#'         rolling_periods = c(3,50,100,200,400,500,2000),
#'         test_date_start = "2025-10-01",
#'         test_date_end = as.character(today() + days(100)),
#'         pred_price_cols = post_bins_cols
#'       )
#'
#'     post_preds_all_rolling_and_originals <-
#'       post_preds_all_rolling %>%
#'       left_join(
#'         all_pred_data %>%
#'           dplyr::select(Date, Asset, contains("pred_combined"),
#'                         contains("pred_macro"), contains("pred_index"),
#'                         contains("pred_daily"), contains("pred_copula"),
#'                         contains("pred_technical")) %>%
#'           distinct()
#'       )
#'
#'     rm(all_pred_data, post_preds_all_rolling, post_preds_all)
#'     gc()
#'
#'     return(post_preds_all_rolling_and_originals)
#'
#'   }
#'
