
create_cor_trig_indicators <-
  function(data_for_indicators = Indices_Metals_Bonds[[1]],
           tangent_period = 50,
           correlation_period = 20,
           Asset1 = "SPX500_USD",
           Asset2 = "EU50_EUR",
           asset_of_interest = "SPX500_USD") {

    a1 <-
      data_for_indicators %>%
      filter(Asset == Asset1) %>%
      arrange(Date) %>%
      mutate(

        !!as.name(paste0("rolling_Bull_Count_", Asset1))  :=
          ifelse(Price > lag(Price), 1, -1 ),

        !!as.name(paste0("rolling_Bear_Count_", Asset1)) :=
          ifelse(Price <= lag(Price), 1, -1),

        !!as.name(paste0("rolling_Bull_Count_", Asset1))  :=
          ifelse( is.na( !!as.name(paste0("rolling_Bull_Count_", Asset1)) )  , 0, !!as.name(paste0("rolling_Bull_Count_", Asset1))   ),

        !!as.name(paste0("rolling_Bear_Count_", Asset1))  :=
          ifelse( is.na(  !!as.name(paste0("rolling_Bear_Count_", Asset1)) )  , 0, !!as.name(paste0("rolling_Bear_Count_", Asset1))   ),

        # !!as.name(paste("rolling_Bull_Count_", Asset2)) := cumsum(!!as.name(paste("rolling_Bull_Count_", Asset2)) ),
        !!as.name(paste0("rolling_Bull_Count_", Asset1)) :=
          slider::slide_dbl(.x = !!as.name(paste0("rolling_Bull_Count_", Asset1)),
                            .f = sum,
                            .before = tangent_period) ,
        # !!as.name(paste("rolling_Bear_Count_", Asset2)) := cumsum( !!as.name(paste("rolling_Bear_Count_", Asset2)) ),
        !!as.name(paste0("rolling_Bear_Count_", Asset1)) :=
          slider::slide_dbl(.x = !!as.name(paste0("rolling_Bear_Count_", Asset1)),
                            .f = sum,
                            .before = tangent_period),

            !!as.name(paste0(Asset1,"_Tangent_", tangent_period,"_Price") ) :=
               (Price - lag(Price, tangent_period))/tangent_period,
             !!as.name(paste0(Asset1,"_Tangent_", tangent_period,"_High") ) :=
               (High - lag(High, tangent_period))/tangent_period,
             !!as.name(paste0(Asset1,"_Tangent_", tangent_period,"_Low") ) :=
               (Low - lag(Low, tangent_period))/tangent_period
      ) %>%
      ungroup() %>%
      dplyr::select(Date,
                    !!as.name(paste0(Asset1,"_Price") ) := Price,
                    !!as.name(paste0(Asset1,"_High") ) := High,
                    !!as.name(paste0(Asset1,"_Low") ) := Low,
                    !!as.name(paste0(Asset1,"_Tangent_", tangent_period,"_Price") ),
                    !!as.name(paste0(Asset1,"_Tangent_", tangent_period,"_High") ),
                    !!as.name(paste0(Asset1,"_Tangent_", tangent_period,"_Low") ),

                    !!as.name(paste0("rolling_Bull_Count_", Asset1, "_", tangent_period)) := !!as.name(paste0("rolling_Bull_Count_", Asset1)),
                    !!as.name(paste0("rolling_Bear_Count_", Asset1, "_", tangent_period)) := !!as.name(paste0("rolling_Bear_Count_", Asset1))
                    )  %>%
      filter(!is.na(!!as.name(paste0(Asset1,"_Tangent_", tangent_period,"_Price") ))) %>%
      mutate(
        across(
          .cols = contains( c("_Tangent_", "rolling_Bull_Count_", "rolling_Bear_Count_") ) ,
          .fns = ~ lag(.)
        )
      )

    a2 <-
      data_for_indicators %>%
      filter(Asset == Asset2) %>%
      arrange(Date) %>%
      distinct() %>%
      mutate(

        !!as.name(paste0("rolling_Bull_Count_", Asset2))  :=
          ifelse(Price > lag(Price), 1, -1 ),

        !!as.name(paste0("rolling_Bear_Count_", Asset2)) :=
          ifelse(Price <= lag(Price), 1, -1),

        !!as.name(paste0("rolling_Bull_Count_", Asset2))  :=
          ifelse( is.na( !!as.name(paste0("rolling_Bull_Count_", Asset2)) )  , 0, !!as.name(paste0("rolling_Bull_Count_", Asset2))   ),

        !!as.name(paste0("rolling_Bear_Count_", Asset2))  :=
          ifelse( is.na(  !!as.name(paste0("rolling_Bear_Count_", Asset2)) )  , 0, !!as.name(paste0("rolling_Bear_Count_", Asset2))   ),

        # !!as.name(paste("rolling_Bull_Count_", Asset2)) := cumsum(!!as.name(paste("rolling_Bull_Count_", Asset2)) ),
        !!as.name(paste0("rolling_Bull_Count_", Asset2)) :=
          slider::slide_dbl(.x = !!as.name(paste0("rolling_Bull_Count_", Asset2)),
                            .f = sum,
                            .before = tangent_period) ,
        # !!as.name(paste("rolling_Bear_Count_", Asset2)) := cumsum( !!as.name(paste("rolling_Bear_Count_", Asset2)) ),
        !!as.name(paste0("rolling_Bear_Count_", Asset2)) :=
          slider::slide_dbl(.x = !!as.name(paste0("rolling_Bear_Count_", Asset2)),
                            .f = sum,
                            .before = tangent_period),

        !!as.name(paste0(Asset2,"_Tangent_", tangent_period,"_Price") ) :=
          (Price - lag(Price, tangent_period))/tangent_period,
        !!as.name(paste0(Asset2,"_Tangent_", tangent_period,"_High") ) :=
          (High - lag(High, tangent_period))/tangent_period,
        !!as.name(paste0(Asset2,"_Tangent_", tangent_period,"_Low") ) :=
          (Low - lag(Low, tangent_period))/tangent_period
      ) %>%
      ungroup() %>%
      dplyr::select(Date,
                    !!as.name(paste0(Asset2,"_Price") ) := Price,
                    !!as.name(paste0(Asset2,"_High") ) := High,
                    !!as.name(paste0(Asset2,"_Low") ) := Low,
                    !!as.name(paste0(Asset2,"_Tangent_", tangent_period,"_Price") ),
                    !!as.name(paste0(Asset2,"_Tangent_", tangent_period,"_High") ),
                    !!as.name(paste0(Asset2,"_Tangent_", tangent_period,"_Low") ),

                    !!as.name(paste0("rolling_Bull_Count_", Asset2, "_", tangent_period)) := !!as.name(paste0("rolling_Bull_Count_", Asset2)),
                    !!as.name(paste0("rolling_Bear_Count_", Asset2, "_", tangent_period)) := !!as.name(paste0("rolling_Bear_Count_", Asset2))
      )  %>%
      filter(!is.na(!!as.name(paste0(Asset2,"_Tangent_", tangent_period,"_Price") ))) %>%
      mutate(
        across(
          .cols = contains( c("_Tangent_", "rolling_Bull_Count_", "rolling_Bear_Count_") ) ,
          .fns = ~ lag(.)
        )
      )

    tagged_data <-
      a1 %>%
      left_join(a2) %>%
      arrange(Date) %>%
      fill(
        everything(), .direction = "down"
      ) %>%
      mutate(
        !!as.name(paste0(Asset1,"_",Asset2, "_Cor_Price_",tangent_period,"_", correlation_period,"_Tan")) :=
          slider::slide2_dbl(.x = !!as.name(paste0(Asset1,"_Tangent_", tangent_period,"_Price") ),
                             .y = !!as.name(paste0(Asset2,"_Tangent_", tangent_period,"_Price") ),
                             .f = ~ cor(.x, .y),
                             .before = correlation_period),

        !!as.name(paste0(Asset1,"_",Asset2, "_Cor_High_",tangent_period,"_", correlation_period,"_Tan")) :=
          slider::slide2_dbl(.x = !!as.name(paste0(Asset1,"_Tangent_", tangent_period,"_High") ),
                             .y = !!as.name(paste0(Asset2,"_Tangent_", tangent_period,"_High") ),
                             .f = ~ cor(.x, .y),
                             .before = correlation_period),

        !!as.name(paste0(Asset1,"_",Asset2, "_Cor_Low_",tangent_period,"_", correlation_period,"_Tan")) :=
          slider::slide2_dbl(.x = !!as.name(paste0(Asset1,"_Tangent_", tangent_period,"_Low") ),
                             .y = !!as.name(paste0(Asset2,"_Tangent_", tangent_period,"_Low") ),
                             .f = ~ cor(.x, .y),
                             .before = correlation_period),

        !!as.name(paste0(Asset1,"_",Asset2, "Cor_Bull_Run",tangent_period,"_", correlation_period,"_Tan")) :=
          slider::slide2_dbl(.x = !!as.name(paste0("rolling_Bull_Count_", Asset1, "_", tangent_period)),
                             .y = !!as.name(paste0("rolling_Bull_Count_", Asset2, "_", tangent_period)),
                             .f = ~ cor(.x, .y),
                             .before = correlation_period),

        !!as.name(paste0(Asset1,"_",Asset2, "Cor_Bear_Run",tangent_period,"_", correlation_period,"_Tan")) :=
          slider::slide2_dbl(.x = !!as.name(paste0("rolling_Bear_Count_", Asset1, "_", tangent_period)),
                             .y = !!as.name(paste0("rolling_Bear_Count_", Asset2, "_", tangent_period)),
                             .f = ~ cor(.x, .y),
                             .before = correlation_period)
      ) %>%
      mutate(
        Asset = asset_of_interest
      )

    return(tagged_data)

  }

stop_factor_long = 6
profit_factor_long = 15

stop_factor_short = 6
profit_factor_short = 4

period_var_long = 24
period_var_short = 4
asset_data = Indices_Metals_Bonds
asset_of_interest = "AUD_USD"
risk_dollar_value_long = 10
risk_dollar_value_short = 10

Longs <- create_running_profits(
  asset_of_interest = asset_of_interest,
  asset_data = asset_data,
  stop_factor = stop_factor_long,
  profit_factor = profit_factor_long,
  risk_dollar_value = risk_dollar_value_long,
  trade_direction = "Long",
  currency_conversion = currency_conversion,
  asset_infor = asset_infor
)

Shorts <- create_running_profits(
  asset_of_interest = asset_of_interest,
  asset_data = asset_data,
  stop_factor = stop_factor_short,
  profit_factor = profit_factor_short,
  risk_dollar_value = risk_dollar_value_short,
  trade_direction = "Short",
  currency_conversion = currency_conversion,
  asset_infor = asset_infor
)

asset_data = Indices_Metals_Bonds[[1]]
asset_of_interest = "AUD_USD"
tangent_period = c(200,50,25)
correlation_period = c(20,40,80, 160)
correlation_assets = c(
                      # "XAU_USD",
                      # "AU200_AUD",
                      # "XAG_USD",
                      "NZD_USD",
                       "EUR_AUD",
                      "XAG_AUD",
                      "GBP_AUD",
                      # "EUR_NZD",
                      "XAU_AUD") %>% unique()
return_data = Longs
trade_direction = "Long"
reg_var_strings = c("Cor_", "_Tan", "_Run", "Bear", "Bull")
return_thresh = 0
return_period_min = 18
return_period_max = 24
return_period_final = 24
training_date_phase1 = "2023-06-01"
training_date_phase2 = "2025-01-01"
trade_thresh = 0



cor_accumulator <- list()
c = 0

for (k in 1:length(tangent_period)) {

  for (j in 1:length(correlation_period)) {

    for (i in 1:length(correlation_assets)) {

      c = c + 1

      if(c == 1 & k == 1) {
        cor_trig_data <-
          create_cor_trig_indicators(
            data_for_indicators = asset_data,
            tangent_period = tangent_period[k],
            correlation_period = correlation_period[j],
            Asset1 = asset_of_interest,
            Asset2 = correlation_assets[i],
            asset_of_interest = asset_of_interest
          )
      }

      if(c != 1 & k == 1) {
        cor_trig_data <-
          create_cor_trig_indicators(
            data_for_indicators = asset_data,
            tangent_period = tangent_period[k],
            correlation_period = correlation_period[j],
            Asset1 = asset_of_interest,
            Asset2 = correlation_assets[i],
            asset_of_interest = asset_of_interest
          ) %>%
          dplyr::select(
            -c(
              !!as.name(paste0(asset_of_interest,"_Price") ),
              !!as.name(paste0(asset_of_interest,"_High") ) ,
              !!as.name(paste0(asset_of_interest,"_Low") ),
              !!as.name(paste0(asset_of_interest,"_Tangent_", tangent_period[k],"_Price") ),
              !!as.name(paste0(asset_of_interest,"_Tangent_", tangent_period[k],"_High") ),
              !!as.name(paste0(asset_of_interest,"_Tangent_", tangent_period[k],"_Low") ),
              !!as.name(paste0("rolling_Bull_Count_", asset_of_interest, "_", tangent_period[k])),
              !!as.name(paste0("rolling_Bear_Count_", asset_of_interest, "_", tangent_period[k]))
            )
          )
      }

      if(c != 1) {
        cor_trig_data <-
          create_cor_trig_indicators(
            data_for_indicators = asset_data,
            tangent_period = tangent_period[k],
            correlation_period = correlation_period[j],
            Asset1 = asset_of_interest,
            Asset2 = correlation_assets[i],
            asset_of_interest = asset_of_interest
          ) %>%
          dplyr::select(
            -c(
                !!as.name(paste0(asset_of_interest,"_Price") ),
                !!as.name(paste0(asset_of_interest,"_High") ) ,
                !!as.name(paste0(asset_of_interest,"_Low") ),
                !!as.name(paste0(asset_of_interest,"_Tangent_", tangent_period[k],"_Price") ),
                !!as.name(paste0(asset_of_interest,"_Tangent_", tangent_period[k],"_High") ),
                !!as.name(paste0(asset_of_interest,"_Tangent_", tangent_period[k],"_Low") ),
                !!as.name(paste0("rolling_Bull_Count_", asset_of_interest, "_", tangent_period[k])),
                !!as.name(paste0("rolling_Bear_Count_", asset_of_interest, "_", tangent_period[k])),

                !!as.name(paste0(correlation_assets[i],"_Price") ),
                !!as.name(paste0(correlation_assets[i],"_High") ) ,
                !!as.name(paste0(correlation_assets[i],"_Low") ),
                !!as.name(paste0(correlation_assets[i],"_Tangent_", tangent_period[k],"_Price") ),
                !!as.name(paste0(correlation_assets[i],"_Tangent_", tangent_period[k],"_High") ),
                !!as.name(paste0(correlation_assets[i],"_Tangent_", tangent_period[k],"_Low") ),
                !!as.name(paste0("rolling_Bull_Count_", correlation_assets[i], "_", tangent_period[k])),
                !!as.name(paste0("rolling_Bear_Count_", correlation_assets[i], "_", tangent_period[k]))

                )
          )
      }

      cor_accumulator[[c]] <- cor_trig_data

      rm(cor_trig_data)
      gc()

    }

  }

}

cor_trig_data <- cor_accumulator %>% reduce(left_join)

cor_trig_data_joined_returns <-
  cor_trig_data %>%
  left_join(
    return_data %>%
      filter(trade_col == trade_direction) %>%
      dplyr::select(Date, Asset, contains("period_return_"), trade_col)
  )

names(cor_trig_data_joined_returns) <-
  names(cor_trig_data_joined_returns) %>%
  map(
    ~ case_when(
      str_detect(.x, "period_return_") ~ paste0(trade_direction,"_",.x),
      TRUE ~ .x
    )
  ) %>%
  unlist() %>%
  as.character()

period_return_vars <-
  names(cor_trig_data_joined_returns) %>%
  keep(~ str_detect(.x, "period_return_")) %>%
  unlist()%>%
  as.character()

logit_model_vars <-
  names(cor_trig_data_joined_returns) %>%
  keep(~ str_detect(.x, paste(reg_var_strings,collapse = "|") )) %>%
  unlist()%>%
  as.character()

model_accumulator_lm <- list()
model_accumulator_glm <- list()
predictions_accumulator <- list()
c = 0
# rm(cor_trig_data_joined_returns, asset_data)
gc()

for (i in return_period_min:return_period_max ) {

  c = c + 1

  model_data <-
    cor_trig_data_joined_returns %>%
    dplyr::select(Date, matches(logit_model_vars), matches(period_return_vars[i]), trade_col, Asset ) %>%
    mutate(
      bin_var =
        ifelse(
          !!as.name(period_return_vars[i]) >return_thresh , "win", "loss"
        )
    )

  traning_data_p1 <-
    model_data %>%
    filter(Date <= training_date_phase1)

  traning_data_p2 <-
    model_data %>%
    filter(Date > training_date_phase1)

  lm_formula <- create_lm_formula(period_return_vars[i], independant = logit_model_vars)

  lm_model <-
    lm(data = traning_data_p1, formula = lm_formula)

  glm_formula <- create_lm_formula("bin_var == 'win'", independant = logit_model_vars)

  glm_model <-
    glm(data = traning_data_p1, formula = glm_formula,  family = binomial("logit"))

  gc()

  preds_lm_1 <- predict(object = lm_model, newdata = traning_data_p2)
  preds_glm_1 <- predict(object = glm_model, newdata = traning_data_p2, type = "response")

  predictions_accumulator[[c]] <-
    traning_data_p2 %>%
    dplyr::select(Date, Asset, trade_col) %>%
    mutate(
      !!as.name(paste0("pred_", i, "_LM")) := preds_lm_1,
      !!as.name(paste0("pred_", i, "_GLM")) := preds_glm_1
    )

  rm(lm_model, glm_model, traning_data_p2, traning_data_p1, model_data)
  gc()

}


model_data_p2 <-
  predictions_accumulator %>%
  reduce(left_join) %>%
  left_join(cor_trig_data_joined_returns %>%
                  dplyr::select(Date, matches(logit_model_vars), trade_col, Asset )
                ) %>%
  dplyr::select(Date,
                matches(logit_model_vars),
                trade_col,
                Asset, contains("pred_") ) %>%
  left_join(return_data) %>%
  mutate(
    bin_var_loss =
      ifelse(
        !!as.name(paste0("period_return_", return_period_final ,"_Price")) <= (-1*risk_dollar_value_long)*0.85 ,
        "win", "loss"
      ),

    bin_var =
      ifelse(
        !!as.name(paste0("period_return_", return_period_final ,"_Price")) > return_thresh , "win", "loss"
      )
  )

model_data_p2_train <-
  model_data_p2 %>%
  filter(Date <= training_date_phase2 & Date >= training_date_phase1)

pred_vars <-
  names(model_data_p2) %>%
  keep( ~ str_detect(.x, "pred_")) %>%
  unlist() %>%
  as.character()

lm_formula <- create_lm_formula(paste0("period_return_", return_period_final ,"_Price"),
                                independant = c(pred_vars ,logit_model_vars) )

lm_model <-
  lm(data = model_data_p2_train, formula = lm_formula)

summary(lm_model)

glm_formula <- create_lm_formula("bin_var == 'win'", independant = c(pred_vars, logit_model_vars))

glm_model <-
  glm(data = model_data_p2_train, formula = glm_formula, family = binomial("logit"))

summary(glm_model)

gc()

glm_formula <- create_lm_formula("bin_var_loss == 'win'", independant = c(pred_vars, logit_model_vars))

glm_model_loss <-
  glm(data = model_data_p2_train, formula = glm_formula, family = binomial("logit"))

summary(glm_model_loss)

gc()

preds_multi_model <-
  model_data_p2 %>%
  mutate(
    pred_multi_GLM_cor_trig = predict(object = glm_model, newdata = model_data_p2, type = "response"),
    pred_multi_GLM_cor_trig_loss = predict(object = glm_model_loss, newdata = model_data_p2, type = "response"),
    pred_multi_GLM_cor_trig_LM = predict(object = lm_model, newdata = model_data_p2)
  ) %>%
  mutate(
    pred_multi_GLM_cor_trig_mean =
      mean(ifelse(Date <= training_date_phase2, pred_multi_GLM_cor_trig, NA), na.rm = T ),
    pred_multi_GLM_cor_trig_sd =
      sd(ifelse(Date <= training_date_phase2, pred_multi_GLM_cor_trig, NA), na.rm = T ),

    pred_multi_GLM_cor_trig_mean_loss =
      mean(ifelse(Date <= training_date_phase2, pred_multi_GLM_cor_trig_loss, NA), na.rm = T ),
    pred_multi_GLM_cor_trig_sd_loss =
      sd(ifelse(Date <= training_date_phase2, pred_multi_GLM_cor_trig_loss, NA), na.rm = T ),

    pred_multi_GLM_cor_trig_LM_mean =
      mean(ifelse(Date <= training_date_phase2, pred_multi_GLM_cor_trig_LM, NA), na.rm = T ),
    pred_multi_GLM_cor_trig_LM_sd =
      sd(ifelse(Date <= training_date_phase2, pred_multi_GLM_cor_trig_LM, NA), na.rm = T )
  ) %>%
  filter(Date > training_date_phase2)

testing_predictions <-
  return_data %>%
  dplyr::select(Date, Asset, trade_col,
                !!as.name(paste0("period_return_", return_period_final ,"_Price"))
                ) %>%
  left_join(
    preds_multi_model %>%
      dplyr::select(Date, Asset,
                    pred_multi_GLM_cor_trig,
                    pred_multi_GLM_cor_trig_mean,
                    pred_multi_GLM_cor_trig_sd,

                    pred_multi_GLM_cor_trig_loss,
                    pred_multi_GLM_cor_trig_mean_loss,
                    pred_multi_GLM_cor_trig_sd_loss,

                    pred_multi_GLM_cor_trig_LM,
                    pred_multi_GLM_cor_trig_LM_mean,
                    pred_multi_GLM_cor_trig_LM_sd
                    # contains("pred_")
                    )
  ) %>%
  filter(Date > training_date_phase2) %>%
  filter(!is.na(pred_multi_GLM_cor_trig)) %>%
  arrange(Date) %>%
  mutate(
    mean_pred_multi_GLM_cor_trig =
      slider::slide_dbl(.x = pred_multi_GLM_cor_trig,
                        .f = ~ mean(.x, na.rm = T),
                        .before = 10),

    sd_pred_multi_GLM_cor_trig =
      slider::slide_dbl(.x = pred_multi_GLM_cor_trig,
                        .f = ~ sd(.x, na.rm = T),
                        .before = 10),

    mean_pred_multi_GLM_cor_trig_LM =
      slider::slide_dbl(.x = pred_multi_GLM_cor_trig_LM,
                        .f = ~ mean(.x, na.rm = T),
                        .before = 10),

    sd_pred_multi_GLM_cor_trig_LM =
      slider::slide_dbl(.x = pred_multi_GLM_cor_trig_LM,
                        .f = ~ sd(.x, na.rm = T),
                        .before = 10)
  ) %>%
  mutate(
    trade_signal =
      case_when(
        pred_multi_GLM_cor_trig >=
          mean_pred_multi_GLM_cor_trig + sd_pred_multi_GLM_cor_trig*trade_thresh &

        # pred_multi_GLM_cor_trig >
        #   pred_multi_GLM_cor_trig_mean + pred_multi_GLM_cor_trig_sd*trade_thresh &

        pred_multi_GLM_cor_trig_loss <
          pred_multi_GLM_cor_trig_mean_loss - pred_multi_GLM_cor_trig_sd_loss*0 &

        # pred_multi_GLM_cor_trig_LM >
        #   pred_multi_GLM_cor_trig_LM_mean + pred_multi_GLM_cor_trig_LM_sd*trade_thresh

          pred_multi_GLM_cor_trig_LM >=
          mean_pred_multi_GLM_cor_trig_LM + sd_pred_multi_GLM_cor_trig_LM*trade_thresh

        # pred_multi_GLM_cor_trig > 0.5 &
        # pred_multi_GLM_cor_trig_loss < 0.5 &
        # pred_multi_GLM_cor_trig_LM > 1

        ~ trade_direction
      )
  )

summary_of_returns <-
  testing_predictions %>%
  ungroup() %>%
  filter(trade_signal == trade_direction) %>%
  arrange(Date) %>%
  mutate(
    returns = !!as.name(paste0("period_return_", return_period_final ,"_Price")),
    return_sum = cumsum(returns)
  ) %>%
  mutate(
    win_loss =
      ifelse(
        returns > 0, 1, 0
      )
  )

summary_of_returns %>%
  summarise(
    Total_Returns = sum(returns, na.rm = T),
    wins = sum(win_loss, na.rm = T),
    total_trades = n()
  ) %>%
  mutate(
    perc = wins/total_trades
  )

