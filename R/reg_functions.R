run_reg_for_trades <- function(
  raw_macro_data = get_macro_event_data(),
  asset_data = read_csv("C:/Users/Nikhil Chandra/Documents/Asset Data/ASX 200/AUD_USD Historical Data.csv"),
  asset_name = "S&P_ASX 200"
) {

  asset_data <-asset_data%>%
    mutate(Date = as.Date(Date, format =  "%m/%d/%Y"))

  Macro_Indicators <- get_AUS_Indicators(raw_macro_data = raw_macro_data)

  Macro_Indicators_USD <- get_USD_Indicators(raw_macro_data = raw_macro_data)

  Macro_EUR <- get_EUR_Indicators(raw_macro_data = raw_macro_data)

  Macro_JPY <- get_JPY_Indicators(raw_macro_data = raw_macro_data)

  testing_data <- asset_data %>%
    rename(date = Date) %>%
    mutate(
      daily_change = Price - lag(Price)
    ) %>%
    dplyr::select(-Vol.) %>%
    left_join(Macro_Indicators)%>%
    left_join(Macro_Indicators_USD) %>%
    left_join(Macro_EUR) %>%
    arrange(date) %>%
    fill(everything(), .direction = "down") %>%
    filter(if_all(everything(), ~ !is.na(.) )) %>%
    arrange(date) %>%
    mutate(
      change_var = (Price - lag(Price))/lag(Price),
      lagged = lag(change_var),
      lagged2 = lag(change_var,2),
      lagged3 = lag(change_var,3),
      lagged4 = lag(change_var,4),
      lagged5 = lag(change_var,5),
      lagged6 = lag(change_var,6),
      lagged7 = lag(change_var,7),
      lagged8 = lag(change_var,8),
      lagged9 = lag(change_var,9),
      lagged10 = lag(change_var,10),
      lagged11 = lag(change_var,11),
      lagged12 = lag(change_var,12)
    ) %>%
    mutate(
      ma = slider::slide_dbl(.x = change_var, .f = ~ mean(.x, na.rm = T), .before = 15),
      ma = lag(ma),

      ma2 = slider::slide_dbl(.x = change_var, .f = ~ mean(.x, na.rm = T), .before = 30),
      ma2 = lag(ma),

      ma3 = slider::slide_dbl(.x = change_var, .f = ~ mean(.x, na.rm = T), .before = 50),
      ma3 = lag(ma),

      sdma = slider::slide_dbl(.x = change_var, .f = ~ sd(.x, na.rm = T), .before = 15),
      sdma = lag(sdma),

      sdma2 = slider::slide_dbl(.x = change_var, .f = ~ sd(.x, na.rm = T), .before = 30),
      sdma2 = lag(sdma),

      sdma3 = slider::slide_dbl(.x = change_var, .f = ~ sd(.x, na.rm = T), .before = 50),
      sdma2 = lag(sdma)
    ) %>%
    filter(!is.na(change_var), !is.na(lagged12))

  reg_vars <- names(testing_data) %>%
    keep(~ .x != "date" & .x != "Price"& .x != "Open" &
           .x != "High" & .x != "Low" & .x != "Change %" &
           .x != "daily_change" & .x != "change_var")

  dependant_var <- "change_var"

  reg_formula <- create_lm_formula(dependant = dependant_var, independant = reg_vars)

  testing_data_train <- testing_data %>%
    slice_head(n = 1500)
  testing_data_test <- testing_data %>%
    slice_tail(n = 1000)
  testing_data_ML <- testing_data %>%
    slice_tail(n = 750)

  lm_reg <- lm(data = testing_data_train, formula = reg_formula)


  summary(lm_reg)

  trade_results <- testing_data_test %>%
    mutate(
      pred = round(predict.lm(lm_reg, testing_data_test), 4),
      trade = case_when(
        pred < 0 & change_var<0 ~ abs(change_var),
        pred < 0 & change_var>0 ~ -abs(change_var),

        pred > 0 & change_var>0 ~ abs(change_var),
        pred > 0 & change_var < 0 ~ -abs(change_var),
      )
    ) %>%
    mutate(
      month_date = lubridate::floor_date(date, "month")
    ) %>%
    group_by(month_date) %>%
    mutate(
      monthly_return = sum(trade, na.rm = T)
    ) %>%
    mutate(
      Asset = asset_name
    )

  return(list(trade_results,lm_reg, testing_data_ML))

}

get_AUS_Indicators <-
  function(
    raw_macro_data = get_macro_event_data(),
    lag_days = 3
    ) {

    AIG_INDEX <- raw_macro_data %>%
      filter(symbol == "AUD") %>%
      mutate(
        Index_Type =
          case_when(
            # str_detect(event, "AiG Industry") ~ "AIG Industry Index",
            str_detect(event, "AiG Manufacturing|Mfg") ~ "AUD AIG Manufacturing Index",
            # str_detect(event, "AiG Construction") ~ "AUD AIG Construction Index",
            str_detect(event, "AiG Performance of Services Index") ~ "AUD AIG Services Index",
            str_detect(event, "Westpac Consumer Confidence") ~ "AUD Westpac Consumer Confidence Index",
            str_detect(event, "National Australia Bank's") & !str_detect(event, "QoQ") ~ "AUD NAB Business Confidence Index",
            str_detect(event, "Private Sector Credit \\(MoM\\)") ~ "AUD Private Sector Credit",
            str_detect(event, "Producer Price Index") ~ "AUD Producer Price Index",
            str_detect(event, "RBA Commodity Index SDR") ~ "AUD RBA Commodity Index SDR",
            # str_detect(event, "Judo Bank Composite PMI") ~ "AUD Judo Bank Composite PMI",
            # str_detect(event, "Judo Bank Services PMI") ~ "AUD Judo Bank Services PMI",
            str_detect(event, "Building Permits \\(MoM\\)") ~ "AUD Building Permits",
            str_detect(event, "Retail Sales s\\.a\\. \\(MoM\\)") ~ "AUD Retail Sales",
            str_detect(event, "Unemployment Rate s\\.a\\.") ~ "AUD Unemployment Rate",
            str_detect(event, "Trade Balance \\(MoM\\)") ~ "AUD Trade Balance",
            str_detect(event, "Exports \\(MoM\\)") ~ "AUD Exports",
            str_detect(event, "Home Loans") ~ "AUD Home Loans",
            str_detect(event, "Investment Lending for Homes") ~ "AUD Investment Lending for Homes",
            str_detect(event, "Consumer Price Index \\(QoQ\\)") ~ "AUD Consumer Price Index",
            str_detect(event, "ANZ Job Advertisements") ~ "AUD ANZ Job Advertisements",
            str_detect(event, "TD Securities Inflation \\(MoM\\)") ~ "AUD TD Securities Inflation",
            str_detect(event, "Wage Price Index \\(QoQ\\)") ~ "AUD Wage Price Index",
            str_detect(event, "Construction Work Done") ~ "AUD Wage Price Index",
            str_detect(event, "RBA Interest Rate Decision") ~ "AUD Interest Rate"
            )
      ) %>%
      filter(!is.na(Index_Type)) %>%
      dplyr::select(Index_Type, actual,date ) %>%
      dplyr::group_by(Index_Type,date ) %>%
      summarise(
        actual = median(actual, na.rm = T)
      ) %>%
      ungroup() %>%
      mutate(date = date + lubridate::days(lag_days) ) %>%
      mutate(
        date =
          case_when(
            lubridate::wday(date) == 7 ~ date + lubridate::days(2),
            lubridate::wday(date) == 1 ~ date + lubridate::days(1),
            TRUE ~ date
          )
      ) %>%
      pivot_wider(names_from = Index_Type, values_from = actual, values_fn = median) %>%
      arrange(date) %>%
      fill(everything(), .direction = "down") %>%
      filter(if_all(everything(), ~ !is.na(.) ))%>%
      mutate(
        across(
          .cols = where(is.numeric),
          .fns = ~ case_when(
            . > 0 ~ log(.),
            TRUE ~ .
          )
        )
      )

    return(AIG_INDEX)

}

get_USD_Indicators <- function(
    raw_macro_data = get_macro_event_data(),
    lag_days = 3
    ) {

  USD_INDEX <- raw_macro_data %>%
    filter(symbol == "USD") %>%
    mutate(
      Index_Type =
        case_when(
          str_detect(event, "ISM Manufacturing PMI") ~ "USD ISM Manufacturing PMI",
          str_detect(event, "ISM Services PMI") ~ "USD ISM Services PMI",
          str_detect(event, "NFIB Business Optimism Index") ~ "USD NFIB Business Optimism Index",
          str_detect(event, "Average Hourly Earnings \\(MoM\\)") ~ "USD Average Hourly Earnings",
          str_detect(event, "Monthly Budget Statement") ~ "USD Monthly Budget Statement",
          str_detect(event, "Redbook Index \\(MoM\\)") ~ "USD Redbook Index",
          str_detect(event, "MBA Mortgage Applications") ~ "USD MBA Mortgage Applications",
          str_detect(event, "Consumer Price Index \\(MoM\\)") ~ "USD Consumer Price Index",
          str_detect(event, "Consumer Price Index ex Food & Energy \\(MoM\\)") ~ "USD Consumer Price Index No Food",
          str_detect(event, "Nonfarm Payrolls") ~ "USD Nonfarm Payrolls",
          str_detect(event, "Michigan Consumer Sentiment Index") &
            !str_detect(event, "\\) Prel") & !str_detect(event, "\\)Prel") ~ "USD Michigan Consumer Sentiment Index",
          str_detect(event, "Consumer Credit Change") ~ "USD Consumer Credit Change",
          str_detect(event, "Fed Interest Rate Decision") ~ "USD Interest Rate",
          str_detect(event, "EIA Crude Oil Stocks Change") ~ "USD Oil Stock Change",
          str_detect(event, "Construction Spending") ~ "USD Construction Spending",
          str_detect(event, "Unemployment Rate") ~ "USD Unemployment Rate",
          str_detect(event, "Factory Orders") ~ "USD Factory Orders",
          str_detect(event, "Wholesale Inventories") ~ "USD Wholesale Inventories",
          str_detect(event, "Export Price Index \\(MoM\\)") ~ "USD Export Price Index",
          str_detect(event, "Import Price Index \\(MoM\\)") ~ "USD Import Price Index",
          str_detect(event, "TIPP Economic Optimism \\(MoM\\)") ~ "USD Economic Optimism Index",
          str_detect(event, "Retail Sales \\(MoM\\)") ~ "USD Retail Sales",
          str_detect(event, "Capacity Utilization") ~ "USD Capacity Utilization",
          str_detect(event, "Net Long\\-Term TIC Flows") ~ "USD Net Long Term TIC Flows",
          str_detect(event, "New Home Sales Change \\(MoM\\)") ~ "USD New Home Sales",
          str_detect(event, "Philadelphia Fed Manufacturing Survey") ~ "USD Fed Manuf Survey",
          str_detect(event, "Michigan Consumer Sentiment Index") & !str_detect(event, "Prel") ~ "USD M Consumer Sentiment",
          str_detect(event, "Gross Domestic Product Annualized") & !str_detect(event, "Prel") ~ "USD GDP",
          str_detect(event, "ISM Manufacturing Prices Paid") & !str_detect(event, "Prel") ~ "USD ISM Manufacturing Prices"

        )
    ) %>%
    filter(!is.na(Index_Type)) %>%
    dplyr::select(Index_Type, actual,date ) %>%
    dplyr::group_by(Index_Type,date ) %>%
    summarise(
      actual = median(actual, na.rm = T)
    ) %>%
    ungroup() %>%
    mutate(date = date + lubridate::days(lag_days) ) %>%
    mutate(
      date =
        case_when(
          lubridate::wday(date) == 7 ~ date + lubridate::days(2),
          lubridate::wday(date) == 1 ~ date + lubridate::days(1),
          TRUE ~ date
        )
    ) %>%
    pivot_wider(names_from = Index_Type, values_from = actual, values_fn = median) %>%
    arrange(date) %>%
    fill(everything(), .direction = "down") %>%
    filter(if_all(everything(), ~ !is.na(.) ))%>%
    mutate(
      across(
        .cols = where(is.numeric),
        .fns = ~ case_when(
          . > 0 ~ log(.),
          TRUE ~ .
        )
      )
    )

  return(USD_INDEX)

}

get_EUR_Indicators <- function(
    raw_macro_data = get_macro_event_data(),
    lag_days = 3
) {

  EUR_INDEX <- raw_macro_data %>%
    filter(symbol == "EUR") %>%
    mutate(
      Index_Type =
        case_when(
          str_detect(event, "Unemployment Rate") & !str_detect(event, "s\\.a") ~ "EUR Unemployment Rate",
          str_detect(event, "Public Deficit\\/GDP\\(Q")  ~ "EUR Public Deficit",
          str_detect(event, "Sentix Investor Confidence") & !str_detect(event, "Prel|prel")  ~ "EUR Sentix Investor Confidence",
          str_detect(event, "Economic Sentiment Indicator") & !str_detect(event, "Prel|prel")  ~ "EUR Economic Sentiment Indicator",
          str_detect(event, "Manufacturing PMI") & !str_detect(event, "Prel|prel")  ~ "EUR Manufacturing PMI",
          str_detect(event, "Consumer Confidence") & !str_detect(event, "Prel|prel")  ~ "EUR Consumer Confidence",
          str_detect(event, "Industrial Confidence") & !str_detect(event, "Prel|prel")  ~ "EUR Industrial Confidence",
          str_detect(event, "Retail Sales") & str_detect(event, "MoM")  ~ "EUR Retail Sales",
          str_detect(event, "Consumer Price Index \\(EU norm\\)")|
            str_detect(event, "Consumer Price Index\\(EU norm\\)") ~ "EUR Consumer Price Index",
          str_detect(event, "ECB Interest Rate Decision") ~ "EUR Interest Rate",
          str_detect(event, "Global Services PMI") & !str_detect(event, "Prel|prel") ~ "EUR Services PMI",
          str_detect(event, "Industrial New Orders \\(YoY\\)") ~ "EUR Industrial New Orders",
          str_detect(event, "Producer Price Index \\(MoM\\)") ~ "EUR Producer Price Index",
          str_detect(event, "Factory Orders s\\.a\\. \\(MoM\\)") ~ "EUR Factory Orders",
          str_detect(event, "Current Account s\\.a") ~ "EUR Current Account",
          str_detect(event, "Trade Balance EUR") ~ "EUR Current Account",
          str_detect(event, "Gross Domestic Product s\\.a\\. \\(YoY\\)") ~ "EUR GDP",
          str_detect(event, "Industrial Production s\\.a\\. \\(MoM\\)") ~ "EUR Industrial Production",
          str_detect(event, "ZEW Survey") & str_detect(event, "Economic Sentiment") ~ "EUR ZEW Survey Economic Sentiment",
          str_detect(event, "ZEW Survey") & str_detect(event, "Current Situation") ~ "EUR ZEW Survey Current Situation",
          str_detect(event, "Business Climate in Manufacturing") ~ "EUR Climate in Manufacturing",
          str_detect(event, "IFO") & str_detect(event, "Business Climate") ~ "EUR IFO Business Climate",
          str_detect(event, "IFO") & str_detect(event, "Current Assessment") ~ "EUR IFO Current Assessment",
          str_detect(event, "M3 Money Supply \\(YoY\\)") ~ "EUR M3 Money Supply"
        )
    ) %>%
    filter(!is.na(Index_Type)) %>%
    dplyr::select(Index_Type, actual,date ) %>%
    dplyr::group_by(Index_Type,date ) %>%
    summarise(
      actual = median(actual, na.rm = T)
    ) %>%
    ungroup() %>%
    mutate(date = date + lubridate::days(lag_days) ) %>%
    mutate(
      date =
        case_when(
          lubridate::wday(date) == 7 ~ date + lubridate::days(2),
          lubridate::wday(date) == 1 ~ date + lubridate::days(1),
          TRUE ~ date
        )
    ) %>%
    pivot_wider(names_from = Index_Type, values_from = actual, values_fn = median) %>%
    arrange(date) %>%
    fill(everything(), .direction = "down") %>%
    filter(if_all(everything(), ~ !is.na(.) ))%>%
    mutate(
      across(
        .cols = where(is.numeric),
        .fns = ~ case_when(
          . > 0 ~ log(.),
          TRUE ~ .
        )
      )
    )

  return(EUR_INDEX)

}

get_JPY_Indicators <- function(
    raw_macro_data = get_macro_event_data(),
    lag_days = 3
) {

  JPY_INDEX <- raw_macro_data %>%
    filter(symbol == "JPY") %>%
    mutate(
      Index_Type =
        case_when(
          str_detect(event, "Monetary Base")  ~ "JPY Monetary Base",
          str_detect(event, "Coincident Index") & !str_detect(event, "Prel|prel") ~ "JPY Coincident Index",
          str_detect(event, "Leading Economic Index") & !str_detect(event, "Prel|prel") ~ "JPY Leading Economic Index",
          str_detect(event, "Producer Price Index") & str_detect(event, "MoM") ~ "JPY Producer Price Index",
          str_detect(event, "Producer Price Index") & str_detect(event, "MoM") ~ "JPY Producer Price Index",
          str_detect(event, "National Consumer Price Index") ~ "JPY National Consumer Price Index",
          str_detect(event, "BoJ Interest Rate Decision") ~ "JPY BoJ Interest Rate Decision",
          str_detect(event, "Current Account") ~ "JPY Current Account",
          str_detect(event, "Trade Balance \\- BOP Basis") ~ "JPY Trade Balance",
          str_detect(event, "Eco Watchers Survey\\: Outlook") ~ "JPY Eco Watchers Survey Outlook",
          str_detect(event, "Machinery Orders") & str_detect(event, "MoM") ~ "JPY Machinery Orders",
          str_detect(event, "Machine Tool Orders") & str_detect(event, "MoM") ~ "JPY Machinery Orders",
          str_detect(event, "Capacity Utilization") ~ "JPY Capacity Utilization",
          str_detect(event, "Industrial Production") & !str_detect(event, "Prel|prel") ~ "JPY Industrial Production",
          str_detect(event, "All Industry Activity Index") & str_detect(event, "MoM") ~ "JPY All Industry Activity Index",
          str_detect(event, "Tokyo Consumer Price Index") ~ "JPY Tokyo Consumer Price Index"
        )
    ) %>%
    filter(!is.na(Index_Type)) %>%
    dplyr::select(Index_Type, actual,date ) %>%
    dplyr::group_by(Index_Type,date ) %>%
    summarise(
      actual = median(actual, na.rm = T)
    ) %>%
    ungroup() %>%
    mutate(date = date + lubridate::days(lag_days) ) %>%
    mutate(
      date =
        case_when(
          lubridate::wday(date) == 7 ~ date + lubridate::days(2),
          lubridate::wday(date) == 1 ~ date + lubridate::days(1),
          TRUE ~ date
        )
    ) %>%
    pivot_wider(names_from = Index_Type, values_from = actual, values_fn = median) %>%
    arrange(date) %>%
    fill(everything(), .direction = "down") %>%
    filter(if_all(everything(), ~ !is.na(.) ))%>%
    mutate(
      across(
        .cols = where(is.numeric),
        .fns = ~ case_when(
          . > 0 ~ log(.),
          TRUE ~ .
        )
      )
    )

  return(JPY_INDEX)

}

get_CNY_Indicators <- function(
    raw_macro_data = get_macro_event_data(),
    lag_days = 3
) {

  CNY_INDEX <- raw_macro_data %>%
    filter(symbol == "CNY") %>%
    mutate(
      Index_Type =
        case_when(
          str_detect(event, "PBoC Interest Rate Decision")  ~ "CNY Interest Rate",
          str_detect(event, "NBS Manufacturing PMI")  ~ "CNY NBS Manufacturing PMI",
          str_detect(event, "Consumer Price Index \\(YoY\\)")  ~ "CNY Consumer Price Index",
          str_detect(event, "FDI \\- Foreign Direct Investment")  ~ "CNY FDI",
          str_detect(event, "Fixed Asset Investment")  ~ "CNY Fixed Asset Investment",
          str_detect(event, "Gross Domestic Product \\(YoY\\)")  ~ "CNY GDP",
          str_detect(event, "Industrial Production")  ~ "CNY Industrial Production",
          str_detect(event, "Retail Sales \\(YoY\\)")  ~ "CNY Retail Sales",
          str_detect(event, "Trade Balance")  ~ "CNY Trade Balance",
          str_detect(event, "Producer Price Index")  ~ "CNY Producer Price Index"
        )
    ) %>%
    filter(!is.na(Index_Type)) %>%
    dplyr::select(Index_Type, actual,date ) %>%
    dplyr::group_by(Index_Type,date ) %>%
    summarise(
      actual = median(actual, na.rm = T)
    ) %>%
    ungroup() %>%
    mutate(date = date + lubridate::days(lag_days) ) %>%
    mutate(
      date =
        case_when(
          lubridate::wday(date) == 7 ~ date + lubridate::days(2),
          lubridate::wday(date) == 1 ~ date + lubridate::days(1),
          TRUE ~ date
        )
    ) %>%
    pivot_wider(names_from = Index_Type, values_from = actual, values_fn = median) %>%
    arrange(date) %>%
    fill(everything(), .direction = "down") %>%
    filter(if_all(everything(), ~ !is.na(.) )) %>%
    mutate(
      across(
        .cols = where(is.numeric),
        .fns = ~ case_when(
          . > 0 ~ log(.),
          TRUE ~ .
        )
      )
    )

  return(CNY_INDEX)

}

get_GBP_Indicators <- function(
    raw_macro_data = get_macro_event_data(),
    lag_days = 3
) {

  GBP_INDEX <- raw_macro_data %>%
    filter(symbol == "GBP") %>%
    mutate(
      Index_Type =
        case_when(
          str_detect(event, "M4 Money Supply \\(MoM\\)")  ~ "GBP M4 Money Supply",
          str_detect(event, "Mortgage Approvals") & !str_detect(event, "BPA")  ~ "GBP Mortgage Approvals",
          str_detect(event, "Manufacturing PMI")   ~ "GBP Manufacturing PMI",
          str_detect(event, "Construction PMI")   ~ "GBP Construction PMI",
          str_detect(event, "Halifax House Prices \\(MoM\\)")   ~ "GBP Halifax House Prices",
          str_detect(event, "BRC Shop Price Index")   ~ "GBP BRC Shop Price Index",
          str_detect(event, "Goods Trade Balance")   ~ "GBP Goods Trade Balance",
          str_detect(event, "Industrial Production \\(MoM\\)")   ~ "GBP Industrial Production",
          str_detect(event, "BoE Interest Rate Decision")   ~ "GBP Interest Rate",
          str_detect(event, "Manufacturing Production \\(MoM\\)")   ~ "GBP Manufacturing Production",
          str_detect(event, "NIESR GDP Estimate")   ~ "GBP GDP Estimate",
          str_detect(event, "Producer Price Index \\- Input \\(MoM\\)")   ~ "GBP Producer Price Index Input",
          str_detect(event, "Producer Price Index \\- Output \\(MoM\\)")   ~ "GBP Producer Price Index Output",
          str_detect(event, "GfK Consumer Confidence")   ~ "GBP Consumer Confidence",
          str_detect(event, "Consumer Price Index \\(MoM\\)")   ~ "Consumer Price Index"
        )
    ) %>%
    filter(!is.na(Index_Type)) %>%
    dplyr::select(Index_Type, actual,date ) %>%
    dplyr::group_by(Index_Type,date ) %>%
    summarise(
      actual = median(actual, na.rm = T)
    ) %>%
    ungroup() %>%
    mutate(date = date + lubridate::days(lag_days) ) %>%
    mutate(
      date =
        case_when(
          lubridate::wday(date) == 7 ~ date + lubridate::days(2),
          lubridate::wday(date) == 1 ~ date + lubridate::days(1),
          TRUE ~ date
        )
    ) %>%
    pivot_wider(names_from = Index_Type, values_from = actual, values_fn = median) %>%
    arrange(date) %>%
    fill(everything(), .direction = "down") %>%
    filter(if_all(everything(), ~ !is.na(.) )) %>%
    mutate(
      across(
        .cols = where(is.numeric),
        .fns = ~ case_when(
          . > 0 ~ log(.),
          TRUE ~ .
        )
      )
    )

  return(GBP_INDEX)

}

get_CAD_Indicators <- function(
    raw_macro_data = get_macro_event_data(),
    lag_days = 3
) {

  CAD_INDEX <- raw_macro_data %>%
    filter(symbol == "CAD") %>%
    mutate(
      Index_Type =
        case_when(
          str_detect(event, "Industrial Product Price \\(MoM\\)")  ~ "CAD Industrial Product Price",
          str_detect(event, "Raw Material Price Index")  ~ "CAD Raw Material Price Index",
          str_detect(event, "Ivey Purchasing Managers Index") &
            !str_detect(event, "s\\.a")~ "CAD Purchasing Managers Index",
          str_detect(event, "Net Change in Employment")  ~ "CAD Change in Employment",
          str_detect(event, "Unemployment Rate")  ~ "CAD Unemployment Rate",
          str_detect(event, "Building Permits \\(MoM\\)")  ~ "CAD Building Permits",
          str_detect(event, "Housing Starts s\\.a \\(YoY\\)")  ~ "CAD Housing Starts",
          str_detect(event, "New Housing Price Index \\(MoM\\)")  ~ "CAD New Housing Price Index",
          str_detect(event, "International Merchandise Trade")  ~ "CAD International Merchandise Trade",
          str_detect(event, "Canadian Portfolio Investment in Foreign Securities")  ~ "CAD Canadian Securities Investment",
          str_detect(event, "Foreign Portfolio Investment in Canadian Securities")  ~ "CAD Foreign Securities Investment",
          str_detect(event, "BoC Interest Rate Decision")  ~ "CAD Interest Rate",
          str_detect(event, "Manufacturing Sales")  ~ "CAD Manufacturing Sales",
          str_detect(event, "Wholesale Sales")  ~ "CAD Wholesale Sales",
          str_detect(event, "Retail Sales \\(MoM\\)")  ~ "CAD Retail Sales",
          str_detect(event, "Consumer Price Index Core \\(YoY\\)")  ~ "CAD CPI Core",
          str_detect(event, "Consumer Price Index \\(YoY\\)")  ~ "CAD CPI",
          str_detect(event, "Gross Domestic Product \\(MoM\\)")  ~ "CAD GDP",
          str_detect(event, "Current Account")  ~ "CAD Current Account",
          str_detect(event, "Labor Productivity")  ~ "CAD Labor Productivity",
          str_detect(event, "Capacity Utilization")  ~ "CAD Capacity Utilization",
          str_detect(event, "Capacity Utilization")  ~ "CAD Capacity Utilization"
        )
    ) %>%
    filter(!is.na(Index_Type)) %>%
    dplyr::select(Index_Type, actual,date ) %>%
    dplyr::group_by(Index_Type,date ) %>%
    summarise(
      actual = median(actual, na.rm = T)
    ) %>%
    ungroup() %>%
    mutate(date = date + lubridate::days(lag_days) ) %>%
    mutate(
      date =
        case_when(
          lubridate::wday(date) == 7 ~ date + lubridate::days(2),
          lubridate::wday(date) == 1 ~ date + lubridate::days(1),
          TRUE ~ date
        )
    ) %>%
    pivot_wider(names_from = Index_Type, values_from = actual, values_fn = median) %>%
    arrange(date) %>%
    fill(everything(), .direction = "down") %>%
    filter(if_all(everything(), ~ !is.na(.) )) %>%
    mutate(
      across(
        .cols = where(is.numeric),
        .fns = ~ case_when(
          . > 0 ~ log(.),
          TRUE ~ .
        )
      )
    )

  return(CAD_INDEX)

}


run_reg_for_trades_with_NN <- function(
    raw_macro_data = get_macro_event_data(),
    asset_data = read_csv("C:/Users/Nikhil Chandra/Documents/Asset Data/Futures/EUR_USD Historical Data.csv"),
    asset_name = "S&P_ASX 200",
    hidden_layers = c(20,20),
    iterations = 100000,
    AUD_exports = get_AUS_exports()
) {

  asset_data <-asset_data%>%
    mutate(Date = as.Date(Date, format =  "%m/%d/%Y"))

  Macro_Indicators <- get_AUS_Indicators(raw_macro_data = raw_macro_data)

  Macro_Indicators_USD <- get_USD_Indicators(raw_macro_data = raw_macro_data)

  Macro_EUR <- get_EUR_Indicators(raw_macro_data = raw_macro_data)

  Macro_JPY <- get_JPY_Indicators(raw_macro_data = raw_macro_data)

  Macro_CNY <- get_CNY_Indicators(raw_macro_data = raw_macro_data)

  Macro_GBP <- get_GBP_Indicators(raw_macro_data = raw_macro_data)

  EUR_trade <- get_EUR_exports()

  US_exports <- get_US_exports()

  testing_data <- asset_data %>%
    rename(date = Date) %>%
    mutate(
      daily_change = Price - lag(Price)
    ) %>%
    dplyr::select(-Vol.) %>%
    left_join(Macro_Indicators)%>%
    left_join(Macro_Indicators_USD) %>%
    left_join(Macro_EUR) %>%
    left_join(Macro_JPY) %>%
    left_join(US_exports) %>%
    left_join(Macro_CNY) %>%
    left_join(Macro_GBP) %>%
    left_join(EUR_trade) %>%
    left_join(AUD_exports, c("date" = "TIME_PERIOD")) %>%
    arrange(date) %>%
    fill(everything(), .direction = "down") %>%
    filter(if_all(everything(), ~ !is.na(.) )) %>%
    arrange(date) %>%
    mutate(
      change_var = (Price - lag(Price))/lag(Price),
      lagged = lag(change_var),
      lagged2 = lag(change_var,2),
      lagged3 = lag(change_var,3),
      lagged4 = lag(change_var,4),
      lagged5 = lag(change_var,5),
      lagged6 = lag(change_var,6),
      lagged7 = lag(change_var,7),
      lagged8 = lag(change_var,8),
      lagged9 = lag(change_var,9),
      lagged10 = lag(change_var,10),
      lagged11 = lag(change_var,11),
      lagged12 = lag(change_var,12),
      weekly_forward_return = (Price - lead(Price, 7))/Price
    ) %>%
    mutate(
      ma = slider::slide_dbl(.x = change_var, .f = ~ mean(.x, na.rm = T), .before = 15),
      ma = lag(ma),

      ma2 = slider::slide_dbl(.x = change_var, .f = ~ mean(.x, na.rm = T), .before = 30),
      ma2 = lag(ma),

      ma3 = slider::slide_dbl(.x = change_var, .f = ~ mean(.x, na.rm = T), .before = 50),
      ma3 = lag(ma),

      sdma = slider::slide_dbl(.x = change_var, .f = ~ sd(.x, na.rm = T), .before = 15),
      sdma = lag(sdma),

      sdma2 = slider::slide_dbl(.x = change_var, .f = ~ sd(.x, na.rm = T), .before = 30),
      sdma2 = lag(sdma),

      sdma3 = slider::slide_dbl(.x = change_var, .f = ~ sd(.x, na.rm = T), .before = 50),
      sdma2 = lag(sdma)
    ) %>%
    filter(!is.na(change_var), !is.na(lagged12), !is.na(weekly_forward_return)) %>%
    arrange(date) %>%
    mutate(
      bin_dat = ifelse(change_var < 0, 0, 1)
    )

  testing_data_train <- testing_data %>%
    slice_head(n = 2000)
  testing_data_test <- testing_data %>%
    slice_tail(n = 1000)
  testing_data_ML <- testing_data %>%
    slice_tail(n = 750)

  remove_spaces_in_names <- names(testing_data_train) %>%
    map(~ str_replace_all(.x," ", "_") %>% str_trim()) %>% unlist() %>% as.character()

  names(testing_data_train) <- remove_spaces_in_names
  names(testing_data_test) <- remove_spaces_in_names
  names(testing_data_ML) <- remove_spaces_in_names

  reg_vars <- names(testing_data_train) %>%
    keep(~ .x != "date" & .x != "Price"& .x != "Open" &
           .x != "High" & .x != "Low" & .x != "Change_%" &
           .x != "daily_change" & .x!= "change_var" & .x!= "bin_dat" &
           .x != "weekly_forward_return")
  macro_vars <- reg_vars %>%
    keep(~ !str_detect(.x, "lagged"))%>%
    keep(~ .x != "ma" & .x != "ma2" & .x != "ma3" &
           .x != "sdma" & .x != "sdma2" & .x != "sdma3" & .x != "weekly_forward_return")

  dependant_var <- "weekly_forward_return"


  # for (j in 1:length(macro_vars)) {
  #
  #   stored_mean <- mean(testing_data_train %>% pull(!!as.name(macro_vars[j])), na.rm = T)
  #
  #   scaled_values <-
  #     ( testing_data_train %>% pull(!!as.name(macro_vars[j])) )/mean(testing_data_train %>% pull(!!as.name(macro_vars[j])), na.rm = T)
  #   testing_data_train <- testing_data_train %>%
  #     mutate(
  #       !!as.name(macro_vars[j]) := scaled_values
  #     )
  #
  #   scaled_values <-
  #     ( testing_data_test %>% pull(!!as.name(macro_vars[j])) )/stored_mean
  #   testing_data_test <- testing_data_test %>%
  #     mutate(
  #       !!as.name(macro_vars[j]) := scaled_values
  #     )
  #
  # }

  # testing_data_train <- testing_data_train %>%
  #   mutate(
  #     across(matches(macro_vars), .fns = ~ scale(.) %>% as.numeric())
  #   )
  #
  # testing_data_test <- testing_data_test %>%
  #   mutate(
  #     across(matches(macro_vars), .fns = ~ scale(.) %>% as.numeric())
  #   )


  reg_formula <- create_lm_formula_no_space(dependant = dependant_var, independant = reg_vars)

  safely_n_net <- safely(neuralnet::neuralnet, otherwise = NULL)

  n <- safely_n_net(reg_formula ,
                            data = testing_data_train,
                            hidden = c(50),
                            err.fct = "sse",
                            linear.output = TRUE,
                            lifesign = 'full',
                            rep = 1,
                            algorithm = "rprop+",
                            stepmax = iterations) %>%
    pluck('result')


  if(is.null(n)) {
    n <- safely_n_net(reg_formula ,
                      data = testing_data_train,
                      hidden = hidden_layers,
                      err.fct = "sse",
                      linear.output = TRUE,
                      lifesign = 'full',
                      rep = 1,
                      algorithm = "rprop+",
                      stepmax = iterations) %>%
      pluck('result')
  }

  if(is.null(n)) {
    n <- safely_n_net(reg_formula ,
                      data = testing_data_train,
                      hidden = hidden_layers,
                      err.fct = "sse",
                      linear.output = TRUE,
                      lifesign = 'full',
                      rep = 1,
                      algorithm = "rprop+",
                      stepmax = iterations) %>%
      pluck('result')
  }

  prediction_nn <- compute(n, rep = 1, testing_data_test %>%
                             dplyr::select(matches(reg_vars, ignore.case = FALSE)))
  prediction_nn$net.result

  lm_reg <- lm(data = testing_data_train, formula = reg_formula)
  predict.lm(lm_reg, testing_data_test)

  summary(lm_reg)

  trade_results_NN <- testing_data_test %>%
    mutate(
      pred = round(prediction_nn$net.result %>% as.numeric(), 5),
      pred_lm = round(predict.lm(lm_reg, testing_data_test), 4),

      trade_NN = case_when(
        pred < 0 & !!as.name(dependant_var)<0 ~ abs(!!as.name(dependant_var)),
        pred < 0 & !!as.name(dependant_var)>0 ~ -abs(!!as.name(dependant_var)),

        pred > 0 & !!as.name(dependant_var)>0 ~ abs(!!as.name(dependant_var)),
        pred > 0 & !!as.name(dependant_var) <0 ~ -abs(!!as.name(dependant_var)),
      ),

      # trade_NN = case_when(
      #   pred < 0.5 & change_var<0 ~ abs(change_var),
      #   pred < 0.5 & change_var>0 ~ -abs(change_var),
      #
      #   pred > 0.5 & change_var>0 ~ abs(change_var),
      #   pred > 0.5 & change_var <0 ~ -abs(change_var),
      # ),

      trade_LM = case_when(
        pred_lm < 0 & !!as.name(dependant_var)<0 ~ abs(!!as.name(dependant_var)),
        pred_lm < 0 & !!as.name(dependant_var)>0 ~ -abs(!!as.name(dependant_var)),

        pred_lm > 0 & !!as.name(dependant_var)>0 ~ abs(!!as.name(dependant_var)),
        pred_lm > 0 & !!as.name(dependant_var) <0 ~ -abs(!!as.name(dependant_var)),
      ),

      # trade_LM = case_when(
      #   pred_lm < 0.5 & change_var<0 ~ abs(change_var),
      #   pred_lm < 0.5 & change_var>0 ~ -abs(change_var),
      #
      #   pred_lm > 0.5 & change_var>0 ~ abs(change_var),
      #   pred_lm > 0.5 & change_var <0 ~ -abs(change_var),
      # )
    ) %>%
    mutate(
      month_date = lubridate::floor_date(date, "month")
    ) %>%
    group_by(month_date) %>%
    mutate(
      monthly_return_NN = sum(trade_NN, na.rm = T),
      monthly_return_LM = sum(trade_LM, na.rm = T)
    ) %>%
    mutate(
      Asset = asset_name,
      change_var := round(!!as.name(dependant_var), 5)
    )

  test <- trade_results_NN %>%
    dplyr::select(pred, pred_lm, trade_NN, trade_LM,change_var ,!!as.name(dependant_var))

  return(list(trade_results_NN))

}


run_reg_for_trades_with_NN_grouped <- function(
    raw_macro_data = get_macro_event_data(),
    asset_data = read_csv("C:/Users/Nikhil Chandra/Documents/Asset Data/ASX 200/XAU_USD Historical Data.csv"),
    hidden_layers = c(20,20),
    iterations = 100000,
    AUD_exports = get_AUS_exports()
) {

  asset_data <-asset_data%>%
    mutate(Date = as.Date(Date, format =  "%m/%d/%Y"))

  Macro_Indicators <- get_AUS_Indicators(raw_macro_data = raw_macro_data)

  Macro_Indicators_USD <- get_USD_Indicators(raw_macro_data = raw_macro_data)

  Macro_EUR <- get_EUR_Indicators(raw_macro_data = raw_macro_data)

  Macro_JPY <- get_JPY_Indicators(raw_macro_data = raw_macro_data)

  Macro_CNY <- get_CNY_Indicators(raw_macro_data = raw_macro_data)

  Macro_GBP <- get_GBP_Indicators(raw_macro_data = raw_macro_data)

  US_exports <- get_US_exports()

  EUR_trade <- get_EUR_exports()

  testing_data <- asset_data %>%
    rename(date = Date) %>%
    group_by(Asset) %>%
    mutate(
      daily_change = Price - lag(Price)
    ) %>%
    ungroup() %>%
    dplyr::select(-Vol.) %>%
    left_join(Macro_Indicators)%>%
    left_join(Macro_Indicators_USD) %>%
    left_join(Macro_EUR) %>%
    left_join(Macro_JPY) %>%
    left_join(US_exports) %>%
    left_join(Macro_CNY) %>%
    left_join(Macro_GBP) %>%
    left_join(EUR_trade) %>%
    left_join(AUD_exports, c("date" = "TIME_PERIOD")) %>%
    group_by(Asset) %>%
    arrange(date, .by_group = TRUE) %>%
    group_by(Asset) %>%
    fill(everything(), .direction = "down") %>%
    group_by(Asset) %>%
    filter(if_all(everything(), ~ !is.na(.) )) %>%
    group_by(Asset) %>%
    arrange(date, .by_group = TRUE) %>%
    group_by(Asset) %>%
    mutate(
      change_var = (Price - lag(Price))/lag(Price),
      lagged = lag(change_var),
      # lagged2 = lag(change_var,2),
      # lagged3 = lag(change_var,3),
      # lagged4 = lag(change_var,4),
      # lagged5 = lag(change_var,5),
      # lagged6 = lag(change_var,6),
      # lagged7 = lag(change_var,7),
      # lagged8 = lag(change_var,8),
      # lagged9 = lag(change_var,9),
      # lagged10 = lag(change_var,10),
      # lagged11 = lag(change_var,11),
      # lagged12 = lag(change_var,12),
      weekly_forward_return = (Price - lead(Price, 7))/Price
    ) %>%
    group_by(Asset) %>%
    mutate(
      ma = slider::slide_dbl(.x = change_var, .f = ~ mean(.x, na.rm = T), .before = 15),
      ma = lag(ma),

      # ma2 = slider::slide_dbl(.x = change_var, .f = ~ mean(.x, na.rm = T), .before = 30),
      # ma2 = lag(ma),
      #
      # ma3 = slider::slide_dbl(.x = change_var, .f = ~ mean(.x, na.rm = T), .before = 50),
      # ma3 = lag(ma),
      #
      # sdma = slider::slide_dbl(.x = change_var, .f = ~ sd(.x, na.rm = T), .before = 15),
      # sdma = lag(sdma),
      #
      # sdma2 = slider::slide_dbl(.x = change_var, .f = ~ sd(.x, na.rm = T), .before = 30),
      # sdma2 = lag(sdma),
      #
      # sdma3 = slider::slide_dbl(.x = change_var, .f = ~ sd(.x, na.rm = T), .before = 50),
      # sdma2 = lag(sdma)
    ) %>%
    filter(
      !is.na(change_var),
      !is.na(lagged),
      !is.na(weekly_forward_return)) %>%
    group_by(Asset) %>%
    arrange(date, .by_group = TRUE) %>%
    mutate(
      bin_dat = ifelse(change_var < 0, 0, 1)
    )

  testing_data_train <- testing_data %>%
    group_by(Asset) %>%
    slice_head(n = 2000) %>%
    ungroup()
  testing_data_test <- testing_data %>%
    group_by(Asset) %>%
    slice_tail(n = 1000) %>%
    ungroup()
  testing_data_ML <- testing_data %>%
    slice_tail(n = 750)

  remove_spaces_in_names <- names(testing_data_train) %>%
    map(~ str_replace_all(.x," ", "_") %>% str_trim()) %>% unlist() %>% as.character()

  names(testing_data_train) <- remove_spaces_in_names
  names(testing_data_test) <- remove_spaces_in_names
  names(testing_data_ML) <- remove_spaces_in_names

  reg_vars <- names(testing_data_train) %>%
    keep(~ .x != "date" & .x != "Price"& .x != "Open" &
           .x != "High" & .x != "Low" & .x != "Change_%" &
           .x != "daily_change" & .x!= "change_var" & .x!= "bin_dat" &
           .x != "weekly_forward_return" &.x != "Asset")
  macro_vars <- reg_vars %>%
    keep(~ !str_detect(.x, "lagged"))%>%
    keep(~ .x != "ma" & .x != "ma2" & .x != "ma3" & .x != "bin_dat" &
           .x != "sdma" & .x != "sdma2" & .x != "sdma3" &
           .x != "weekly_forward_return" & .x != "Asset")

  dependant_var <- "weekly_forward_return"


  # for (j in 1:length(macro_vars)) {
  #
  #   stored_mean <- mean(testing_data_train %>% pull(!!as.name(macro_vars[j])), na.rm = T)
  #
  #   scaled_values <-
  #     ( testing_data_train %>% pull(!!as.name(macro_vars[j])) )/mean(testing_data_train %>% pull(!!as.name(macro_vars[j])), na.rm = T)
  #   testing_data_train <- testing_data_train %>%
  #     mutate(
  #       !!as.name(macro_vars[j]) := scaled_values
  #     )
  #
  #   scaled_values <-
  #     ( testing_data_test %>% pull(!!as.name(macro_vars[j])) )/stored_mean
  #   testing_data_test <- testing_data_test %>%
  #     mutate(
  #       !!as.name(macro_vars[j]) := scaled_values
  #     )
  #
  # }

  # testing_data_train <- testing_data_train %>%
  #   mutate(
  #     across(matches(macro_vars), .fns = ~ scale(.) %>% as.numeric())
  #   )
  #
  # testing_data_test <- testing_data_test %>%
  #   mutate(
  #     across(matches(macro_vars), .fns = ~ scale(.) %>% as.numeric())
  #   )


  reg_formula <- create_lm_formula_no_space(dependant = dependant_var, independant = reg_vars)

  safely_n_net <- safely(neuralnet::neuralnet, otherwise = NULL)


  test <-
    neuralnet(reg_formula ,
                 data = testing_data_train,
                 hidden = c(70),
                 err.fct = "ce",
                 linear.output = FALSE,
                 lifesign = 'full',
                 rep = 1,
                 algorithm = "rprop+",
                 stepmax = iterations)

  n <- safely_n_net(reg_formula ,
                    data = testing_data_train,
                    hidden = c(50),
                    err.fct = "sse",
                    linear.output = TRUE,
                    lifesign = 'full',
                    rep = 1,
                    algorithm = "rprop+",
                    stepmax = iterations) %>%
    pluck('result')


  if(is.null(n)) {
    n <- safely_n_net(reg_formula ,
                      data = testing_data_train,
                      hidden = hidden_layers,
                      err.fct = "sse",
                      linear.output = TRUE,
                      lifesign = 'full',
                      rep = 1,
                      algorithm = "rprop+",
                      stepmax = iterations) %>%
      pluck('result')
  }

  if(is.null(n)) {
    n <- safely_n_net(reg_formula ,
                      data = testing_data_train,
                      hidden = hidden_layers,
                      err.fct = "sse",
                      linear.output = TRUE,
                      lifesign = 'full',
                      rep = 1,
                      algorithm = "rprop+",
                      stepmax = iterations) %>%
      pluck('result')
  }

  prediction_nn <- compute(n, rep = 1, testing_data_test %>%
                             dplyr::select(matches(reg_vars, ignore.case = FALSE)))
  prediction_nn$net.result

  lm_reg <- lm(data = testing_data_train, formula = reg_formula)
  predict.lm(lm_reg, testing_data_test)

  summary(lm_reg)

  trade_results_NN <- testing_data_test %>%
    mutate(
      # pred = round(prediction_nn$net.result %>% as.numeric(), 5),
      pred_lm = round(predict.lm(lm_reg, testing_data_test), 4),

      # trade_NN = case_when(
      #   pred < 0 & !!as.name(dependant_var)<0 ~ abs(!!as.name(dependant_var)),
      #   pred < 0 & !!as.name(dependant_var)>0 ~ -abs(!!as.name(dependant_var)),
      #
      #   pred > 0 & !!as.name(dependant_var)>0 ~ abs(!!as.name(dependant_var)),
      #   pred > 0 & !!as.name(dependant_var) <0 ~ -abs(!!as.name(dependant_var)),
      # ),

      # trade_NN = case_when(
      #   pred < 0.5 & change_var<0 ~ abs(change_var),
      #   pred < 0.5 & change_var>0 ~ -abs(change_var),
      #
      #   pred > 0.5 & change_var>0 ~ abs(change_var),
      #   pred > 0.5 & change_var <0 ~ -abs(change_var),
      # ),

      trade_LM = case_when(
        pred_lm < 0 & !!as.name(dependant_var)<0 ~ abs(!!as.name(dependant_var)),
        pred_lm < 0 & !!as.name(dependant_var)>0 ~ -abs(!!as.name(dependant_var)),

        pred_lm > 0 & !!as.name(dependant_var)>0 ~ abs(!!as.name(dependant_var)),
        pred_lm > 0 & !!as.name(dependant_var) <0 ~ -abs(!!as.name(dependant_var)),
      ),

      # trade_LM = case_when(
      #   pred_lm < 0.5 & change_var<0 ~ abs(change_var),
      #   pred_lm < 0.5 & change_var>0 ~ -abs(change_var),
      #
      #   pred_lm > 0.5 & change_var>0 ~ abs(change_var),
      #   pred_lm > 0.5 & change_var <0 ~ -abs(change_var),
      # )

      short_long_LM = case_when(
        pred_lm > 0 ~  "long",
        pred_lm <0 ~ "short"
      ),
      win_LM = case_when(
        pred_lm < 0 & !!as.name(dependant_var)<0 ~ 1,
        pred_lm < 0 & !!as.name(dependant_var)>0 ~ 0,

        pred_lm > 0 & !!as.name(dependant_var)>0 ~ 1,
        pred_lm > 0 & !!as.name(dependant_var) <0 ~ 0,
      )
    ) %>%
    mutate(
      month_date = lubridate::floor_date(date, "month")
    ) %>%
    group_by(month_date) %>%
    mutate(
      # monthly_return_NN = sum(trade_NN, na.rm = T),
      monthly_return_LM = sum(trade_LM, na.rm = T)
    ) %>%
    mutate(
      change_var := round(!!as.name(dependant_var), 5)
    ) %>%
    ungroup() %>%
    mutate(
      # wins = sum(win_NN, na.rm = T),
      total_trades = n(),
      # perc = wins/total_trades,

      winsLM = sum(win_LM, na.rm = T),
      perc_LM = winsLM/total_trades
    )

  trade_results_NN %>%
    select(!!as.name(dependant_var), month_date) %>%
    mutate(
      short_long =
        case_when(
          !!as.name(dependant_var) < 0 ~ "short",
          !!as.name(dependant_var) > 0 ~ "long"
        )
    ) %>%
    group_by(short_long) %>%
    summarise(
      Trades = n()
    ) %>%
    mutate(
      Perc = Trades/sum(Trades)
    )

  trade_results_NN %>%
    filter(!is.na(short_long_LM)) %>%
    group_by(short_long_LM) %>%
    summarise(
      total_trades = n(),
      wins = sum(win_LM, na.rm = T)
    ) %>%
    mutate(
      perc = wins/total_trades
    )

  test <- trade_results_NN %>%
    dplyr::select(pred, pred_lm, trade_NN, trade_LM,change_var ,!!as.name(dependant_var))

  return(list(trade_results_NN))

}



get_AUS_exports <- function() {

  one_drive_path <- helperfunctions35South::create_one_drive_path(
    path_extension = "raw data")
  export_sitc <- read_csv(glue::glue("{one_drive_path}/abs_merchandise_export/ABS_MERCH_EXP_1.0.0.csv"))


  export_sitc2 <- export_sitc %>%
    filter(COMMODITY_SITC != "TOT") %>%
    filter(COUNTRY_DEST == "TOT") %>%
    filter(STATE_ORIGIN == "TOT") %>%
    filter(nchar(COMMODITY_SITC) == 2) %>%
    dplyr::select(`Commodity by SITC` ,TIME_PERIOD, OBS_VALUE ) %>%
    mutate(
      TIME_PERIOD = glue::glue("{TIME_PERIOD}-01") %>% str_trim() %>% lubridate::as_date()
    ) %>%
    distinct() %>%
    mutate(
      `Commodity by SITC` =
        case_when(
          str_detect(`Commodity by SITC`, "Live animals") ~ "AUD Live Exports",
          str_detect(`Commodity by SITC`, "Meat and meat") ~ "AUD Meat",
          str_detect(`Commodity by SITC`,
                     "crustaceans, molluscs and aquatic invertebrates, and preparations") ~ "AUD Seafood",
          str_detect(`Commodity by SITC`,
                     "Dairy products and birds eggs") ~ "AUD Diary",
          str_detect(`Commodity by SITC`,
                     "Coal, coke and briquettes") ~ "AUD Coal",
          str_detect(`Commodity by SITC`,
                     "Petroleum, petroleum products and related materials") ~ "AUD Petroleum",
          str_detect(`Commodity by SITC`,
                     "Gas, natural and manufactured") ~ "AUD Natural Gas",
          str_detect(`Commodity by SITC`,
                     "Metalliferous ores and metal scrap") ~ "AUD Ore and Metal",
          str_detect(`Commodity by SITC`,
                     "Iron and steel") ~ "AUD Iron and Steel",
          str_detect(`Commodity by SITC`,
                     "Beverages") ~ "AUD Beverages"
        )
    ) %>%
    filter(!is.na(`Commodity by SITC`)) %>%
    pivot_wider(names_from = `Commodity by SITC`, values_from = OBS_VALUE, values_fn  = median ) %>%
    mutate(
      across(
        .cols = where(is.numeric),
        .fns = ~ case_when(
          . > 0 ~ log(.),
          TRUE ~ .
        )
      )
    )

  gc()

  return(export_sitc2)

}

get_US_exports <- function() {

  path_exports <- "C:/Users/Nikhil Chandra/Documents/Repos/trading_ML_Python/data/DataWeb-Query-Export.xlsx"

  USD_data <- readxl::read_excel(path_exports, sheet = 2) %>%
    mutate(
      date = glue::glue("{Year}-{Month}-01") %>% lubridate::as_date()
    ) %>%
    dplyr::select(date, Description, `FAS Value`) %>%
    mutate(
      Description = str_remove_all(Description, "\\,|\\.|"),
      Description = paste0("USD ", Description)
    ) %>%
    pivot_wider(values_from = `FAS Value`, names_from = Description) %>%
    mutate(
      across(
        .cols = where(is.numeric),
        .fns = ~ case_when(
          . > 0 ~ log(.),
          TRUE ~ .
        )
      )
    )

  return(USD_data)

}

#' How to get data:
#'
#' https://www-genesis.destatis.de/datenbank/online/statistic/51000/table/51000-0006/
#'
#' Customise the view and then click on the year values and tick all the way back to 2011
#'
#' @param path_var
#'
#' @return
#' @export
#'
#' @examples
get_EUR_exports <- function(
    # path_var = "C:/Users/Nikhil Chandra/Documents/51000-0006_en_flat.csv"
  path_var = "C:/Users/Nikhil Chandra/Documents/51000-0006_en_flat_2025-04-08.csv"
    ) {

  test <- read_delim(path_var, ";")

  returned_EUR_trade <- test %>%
    filter(value_unit == "EUR 1000") %>%
    dplyr::select(`3_variable_attribute_label`, value,
                  value_variable_label, time, `1_variable_attribute_label`) %>%
    mutate(
      export_import =
        case_when(
          str_detect(value_variable_label, "Exports") ~ "Exports",
          str_detect(value_variable_label, "Imports") ~ "Imports"
        ),
      pivot_wide_var =
        glue::glue("EUR {`3_variable_attribute_label`} {export_import}") %>%
        as.character() %>%
        str_trim() %>%
        str_remove_all(pattern = "\\,|\\;|\\:|\\-|\\-|\\'|\\'")%>%
        str_remove_all(pattern = "etc\\.")
    ) %>%
    mutate(
      date = glue::glue("{time}-{`1_variable_attribute_label`}-01") %>%
        as.character() %>%
        as.Date(format = "%Y-%B-%d")
    ) %>%
    dplyr::select(date, pivot_wide_var, value) %>%
    mutate(value = as.numeric(value))

  eur_trade_totals <-
    returned_EUR_trade %>%
    group_by(date) %>%
    summarise(value = sum(value, na.rm = T),
              pivot_wide_var = "EUR Export Total")

  returned_EUR_trade2 <-
    returned_EUR_trade %>%
    bind_rows(eur_trade_totals) %>%
    pivot_wider(names_from = pivot_wide_var, values_from = value, values_fn = sum) %>%
    mutate(
      date =
        case_when(
          lubridate::wday(date) == 7 ~ date + lubridate::days(2),
          lubridate::wday(date) == 1 ~ date + lubridate::days(1),
          TRUE ~ date
        )
    ) %>%
    mutate(
      across(
        .cols = where(is.numeric),
        .fns = ~ case_when(
          . > 0 ~ log(.),
          TRUE ~ .
        )
      )
    )

  return(returned_EUR_trade2)

}

run_reg_weekly_variant <- function(

    raw_macro_data = get_macro_event_data(),

    eur_data = get_EUR_exports(),

    AUD_exports_total = get_AUS_exports()  %>%
      pivot_longer(-TIME_PERIOD, names_to = "category", values_to = "Aus_Export") %>%
      rename(date = TIME_PERIOD) %>%
      group_by(date) %>%
      summarise(Aus_Export = sum(Aus_Export, na.rm = T)),

    USD_exports_total = get_US_exports()  %>%
      pivot_longer(-date, names_to = "category", values_to = "US_Export") %>%
      group_by(date) %>%
      summarise(US_Export = sum(US_Export, na.rm = T)) %>%
      left_join(AUD_exports_total) %>%
      ungroup(),

    asset_data_combined = fs::dir_info("C:/Users/Nikhil Chandra/Documents/Asset Data/Futures/") %>%
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
                 transform_asset_to_weekly()  %>%
                 mutate(Asset = .x[1,2] %>% as.character())
      ),

    train_percent = 0.6
  ) {

  trading_dat <- asset_data_combined

  USD_exports_total = USD_exports_total %>%
    mutate(
      month_date = lubridate::floor_date(date, "month")
    )

  AUD_exports_total = AUD_exports_total %>%
    mutate(
      month_date = lubridate::floor_date(date, "month")
    )

  macro_us <- transform_macro_to_monthly(
    macro_dat_for_transform = get_USD_Indicators(raw_macro_data = raw_macro_data),
    transform_to_week = TRUE)
  macro_eur <- transform_macro_to_monthly(
    macro_dat_for_transform = get_EUR_Indicators(raw_macro_data = raw_macro_data),
    transform_to_week = TRUE)
  macro_jpy <- transform_macro_to_monthly(
    macro_dat_for_transform = get_JPY_Indicators(raw_macro_data = raw_macro_data),
    transform_to_week = TRUE)
  macro_aud <- transform_macro_to_monthly(
    macro_dat_for_transform = get_AUS_Indicators(raw_macro_data = raw_macro_data),
    transform_to_week = TRUE)
  Macro_CNY <- transform_macro_to_monthly(
    get_CNY_Indicators(raw_macro_data = raw_macro_data),
    transform_to_week = TRUE)
  Macro_GBP <- transform_macro_to_monthly(
    macro_dat_for_transform = get_GBP_Indicators(raw_macro_data = raw_macro_data),
    transform_to_week = TRUE)
  macro_cad <- transform_macro_to_monthly(
    get_CAD_Indicators(raw_macro_data = raw_macro_data),
    transform_to_week = TRUE)

  EUR_trade2 <- get_EUR_exports() %>% dplyr::select(month_date = date, `EUR Export Total`) %>%
    mutate(
      `EUR Export Total` = `EUR Export Total` - lag(`EUR Export Total`)
    ) %>% filter(!is.na(`EUR Export Total`)) %>%
    mutate(month_date = month_date + months(1)) %>%
    mutate(week_date = lubridate::floor_date(month_date, "week")) %>%
    dplyr::select(-month_date)

  USD_exports_total2 <- USD_exports_total %>%
    mutate(
      US_Export = US_Export - lag(US_Export),
      Aus_Export  = Aus_Export  - lag(Aus_Export )
    ) %>%
    filter(!is.na(US_Export), !is.na(Aus_Export)) %>%
    mutate(month_date = month_date + months(1)) %>%
    dplyr::select(-date)  %>%
    mutate(week_date = lubridate::floor_date(month_date, "week")) %>%
    dplyr::select(-month_date)

  testing_data <- trading_dat %>%
    left_join(macro_us) %>%
    left_join(macro_eur)%>%
    left_join(macro_jpy)%>%
    left_join(macro_aud) %>%
    left_join(Macro_CNY) %>%
    left_join(Macro_GBP) %>%
    left_join(macro_cad) %>%
    left_join(EUR_trade2) %>%
    left_join(USD_exports_total2) %>%
    # left_join(AUD_exports_total2) %>%
    # filter(!is.na(`USD Monthly Budget Statement`)) %>%
    # filter(!is.na(`EUR Export Total`)) %>%
    group_by(Asset) %>%
    arrange(week_date, .by_group = TRUE) %>%
    group_by(Asset) %>%
    mutate(
      lagged_var = Week_Change_lag,
      lagged_var2 = lag(Week_Change_lag),
      lagged_var3 = lag(Week_Change_lag, 2),
      ma3 = slider::slide_dbl(.x = lagged_var, .f = ~ mean(.x, na.rm = T), .before = 3),
      sd3 = slider::slide_dbl(.x = lagged_var, .f = ~ sd(.x, na.rm = T), .before = 3)
    ) %>%
    filter(!is.na(lagged_var),
           !is.na(ma3),
           !is.na(lagged_var3)
    ) %>%
    group_by(Asset) %>%
    fill(where(is.numeric), .direction = "down") %>%
    group_by(Asset) %>%
    fill(where(is.numeric), .direction = "up") %>%
    ungroup() %>%
    mutate(
      EUR_check = ifelse(str_detect(Asset, "EUR"), 1, 0),
      AUD_check = ifelse(str_detect(Asset, "AUD"), 1, 0),
      USD_check = ifelse(str_detect(Asset, "USD"), 1, 0),
      GBP_check = ifelse(str_detect(Asset, "GBP"), 1, 0),
      JPY_check = ifelse(str_detect(Asset, "JPY"), 1, 0),
      CNY_check = ifelse(str_detect(Asset, "CNY"), 1, 0),
      CAD_check = ifelse(str_detect(Asset, "CAD"), 1, 0)
    ) %>%
    mutate(
      bin_dat = case_when(
        Week_Change >= 0 ~ 1,
        Week_Change < 0 ~ 0
      )
    ) %>%
    filter(if_all(everything(), ~ !is.na(.)))

  testing_data_train <- testing_data %>%
    group_by(Asset) %>%
    slice_head(prop = train_percent) %>%
    ungroup()
  testing_data_test <- testing_data %>%
    group_by(Asset) %>%
    slice_tail(prop = (1 - train_percent) - 0.03 )%>%
    ungroup()

  remove_spaces_in_names <- names(testing_data_train) %>%
    map(~ str_replace_all(.x," ", "_") %>% str_trim()) %>% unlist() %>% as.character()

  names(testing_data_train) <- remove_spaces_in_names
  names(testing_data_test) <- remove_spaces_in_names

  reg_vars <- names(testing_data_train) %>%
    keep(~ .x != "date" & .x != "Price"& .x != "Open" &
           .x != "High" & .x != "Low" & .x != "Change_%" &
           .x != "daily_change" & .x!= "change_var" & .x!= "bin_dat" &
           .x != "week_date" & .x != "week_start_price" &
           .x != "weekly_forward_return" & .x != "Week_Change" & .x != "Asset" &
           .x != "Week_Change_lag" & .x != "sd3")
  macro_vars <- reg_vars %>%
    keep(~ !str_detect(.x, "lagged"))%>%
    keep(~ .x != "ma" & .x != "ma2" & .x != "ma3" & .x != "week_date" & .x != "week_start_price" &
           .x != "sdma" & .x != "sdma2" & .x != "sdma3" &
           .x != "weekly_forward_return" & .x != "Week_Change" & .x != "Asset" &
           .x != "Week_Change_lag" & .x != "sd3" & .x != "bin_dat")

  dependant_var <- "Week_Change"

  reg_formula <- create_lm_formula_no_space(dependant = dependant_var, independant = reg_vars)

  reg_formula_lm <-  create_lm_formula_no_space(dependant = dependant_var,
                                                independant = reg_vars
  )

  lm_reg <- lm(data = testing_data_train, formula = reg_formula_lm)
  prediction <- predict.lm(lm_reg, testing_data_test)
  prediction_training <- predict.lm(lm_reg, testing_data_train)

  raw_LM_trade_df <- testing_data_test %>%
    dplyr::select(Asset, week_date) %>%
    mutate(
      LM_pred = predict.lm(lm_reg, testing_data_test) %>% as.numeric()
    )

  raw_LM_trade_df_training <- testing_data_train %>%
    dplyr::select(Asset, week_date) %>%
    mutate(
      LM_pred = predict.lm(lm_reg, testing_data_train) %>% as.numeric()
    )

  return(

    list(
       "LM Model"= lm_reg,
       "Testing Data"= raw_LM_trade_df,
       "Training Data" = raw_LM_trade_df_training
    )

  )

}

prep_LM_wkly_trade_data <- function(
    asset_data_daily_raw = fs::dir_info("C:/Users/Nikhil Chandra/Documents/Asset Data/Futures/") %>%
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
      ),
    raw_LM_trade_df,
    raw_LM_trade_df_training,
    new_week_date_start_day = 1

  ) {

  asset_data_daily <- asset_data_daily_raw %>%
    mutate(Date = as.Date(Date, format =  "%m/%d/%Y"))

  mean_LM_value <-
    raw_LM_trade_df_training %>%
    group_by(Asset) %>%
    summarise(
      mean_value = mean(LM_pred, na.rm = T),
      sd_value = sd(LM_pred, na.rm = T)
    )

  trade_with_daily_data <- asset_data_daily %>%
    mutate(
      week_date = lubridate::floor_date(Date, "week")
    ) %>%
    left_join(raw_LM_trade_df, by = c("week_date", "Asset")) %>%
    group_by(Asset) %>%
    arrange(Date, .by_group = TRUE) %>%
    group_by(Asset) %>%
    mutate(
      Pred_Filled = LM_pred
    ) %>%
    group_by(Asset) %>%
    fill(Pred_Filled, .direction = "down") %>%
    ungroup() %>%
    mutate(
      new_week_date = lubridate::floor_date(Date, "week",week_start = new_week_date_start_day)
    ) %>%
    mutate(
      Pred_trade =
        case_when(
          new_week_date == Date ~ LM_pred
        )
    ) %>%
    filter(!is.na(Pred_Filled)) %>%
    left_join(mean_LM_value)

  return(
    list(
      "LM Merged to Daily" = trade_with_daily_data,
      "Mean LM Values" = mean_LM_value
    )
  )

}

run_reg_daily_variant <- function(

  raw_macro_data = get_macro_event_data(),

  eur_data = get_EUR_exports(),

  AUD_exports_total = get_AUS_exports()  %>%
    pivot_longer(-TIME_PERIOD, names_to = "category", values_to = "Aus_Export") %>%
    rename(date = TIME_PERIOD) %>%
    group_by(date) %>%
    summarise(Aus_Export = sum(Aus_Export, na.rm = T)),

  USD_exports_total = get_US_exports()  %>%
    pivot_longer(-date, names_to = "category", values_to = "US_Export") %>%
    group_by(date) %>%
    summarise(US_Export = sum(US_Export, na.rm = T)) %>%
    left_join(AUD_exports_total) %>%
    ungroup(),

  asset_data_daily_raw = asset_data_daily_raw,

  train_percent = 0.6
) {

  trading_dat <- asset_data_daily_raw %>%
    mutate(
      week_start_price = log(Price),
      Week_Change = lead(week_start_price) - week_start_price,
      Week_Change_lag = week_start_price - lag(week_start_price)
    )

  USD_exports_total = USD_exports_total %>%
    mutate(
      month_date = lubridate::floor_date(date, "month")
    )

  AUD_exports_total = AUD_exports_total %>%
    mutate(
      month_date = lubridate::floor_date(date, "month")
    )

  macro_us <- transform_macro_to_monthly(
    macro_dat_for_transform = get_USD_Indicators(raw_macro_data = raw_macro_data),
    transform_to_week = TRUE)
  macro_eur <- transform_macro_to_monthly(
    macro_dat_for_transform = get_EUR_Indicators(raw_macro_data = raw_macro_data),
    transform_to_week = TRUE)
  macro_jpy <- transform_macro_to_monthly(
    macro_dat_for_transform = get_JPY_Indicators(raw_macro_data = raw_macro_data),
    transform_to_week = TRUE)
  macro_aud <- transform_macro_to_monthly(
    macro_dat_for_transform = get_AUS_Indicators(raw_macro_data = raw_macro_data),
    transform_to_week = TRUE)
  Macro_CNY <- transform_macro_to_monthly(
    get_CNY_Indicators(raw_macro_data = raw_macro_data),
    transform_to_week = TRUE)
  Macro_GBP <- transform_macro_to_monthly(
    macro_dat_for_transform = get_GBP_Indicators(raw_macro_data = raw_macro_data),
    transform_to_week = TRUE)
  macro_cad <- transform_macro_to_monthly(
    get_CAD_Indicators(raw_macro_data = raw_macro_data),
    transform_to_week = TRUE)

  EUR_trade2 <- get_EUR_exports() %>% dplyr::select(month_date = date, `EUR Export Total`) %>%
    mutate(
      `EUR Export Total` = `EUR Export Total` - lag(`EUR Export Total`)
    ) %>% filter(!is.na(`EUR Export Total`)) %>%
    mutate(month_date = month_date + months(1)) %>%
    mutate(week_date = lubridate::floor_date(month_date, "week")) %>%
    dplyr::select(-month_date)

  USD_exports_total2 <- USD_exports_total %>%
    mutate(
      US_Export = US_Export - lag(US_Export),
      Aus_Export  = Aus_Export  - lag(Aus_Export )
    ) %>%
    filter(!is.na(US_Export), !is.na(Aus_Export)) %>%
    mutate(month_date = month_date + months(1)) %>%
    dplyr::select(-date)  %>%
    mutate(week_date = lubridate::floor_date(month_date, "week")) %>%
    dplyr::select(-month_date)

  testing_data <- trading_dat %>%
    left_join(macro_us, by = c("Date" = "week_date") ) %>%
    left_join(macro_eur, by = c("Date" = "week_date"))%>%
    left_join(macro_jpy, by = c("Date" = "week_date"))%>%
    left_join(macro_aud, by = c("Date" = "week_date")) %>%
    left_join(Macro_CNY, by = c("Date" = "week_date")) %>%
    left_join(Macro_GBP, by = c("Date" = "week_date")) %>%
    left_join(macro_cad, by = c("Date" = "week_date")) %>%
    left_join(EUR_trade2, by = c("Date" = "week_date")) %>%
    left_join(USD_exports_total2, by = c("Date" = "week_date")) %>%
    # left_join(AUD_exports_total2) %>%
    # filter(!is.na(`USD Monthly Budget Statement`)) %>%
    # filter(!is.na(`EUR Export Total`)) %>%
    group_by(Asset) %>%
    arrange(Date, .by_group = TRUE) %>%
    group_by(Asset) %>%
    mutate(
      lagged_var = Week_Change_lag,
      lagged_var2 = lag(Week_Change_lag),
      lagged_var3 = lag(Week_Change_lag, 2),
      ma3 = slider::slide_dbl(.x = lagged_var, .f = ~ mean(.x, na.rm = T), .before = 3),
      sd3 = slider::slide_dbl(.x = lagged_var, .f = ~ sd(.x, na.rm = T), .before = 3)
    ) %>%
    filter(!is.na(lagged_var),
           !is.na(ma3),
           !is.na(lagged_var3)
    ) %>%
    group_by(Asset) %>%
    arrange(Date, .by_group = TRUE) %>%
    group_by(Asset) %>%
    fill(where(is.numeric), .direction = "down") %>%
    group_by(Asset) %>%
    arrange(Date, .by_group = TRUE) %>%
    group_by(Asset) %>%
    fill(where(is.numeric), .direction = "up") %>%
    ungroup() %>%
    mutate(
      EUR_check = ifelse(str_detect(Asset, "EUR"), 1, 0),
      AUD_check = ifelse(str_detect(Asset, "AUD"), 1, 0),
      USD_check = ifelse(str_detect(Asset, "USD"), 1, 0),
      GBP_check = ifelse(str_detect(Asset, "GBP"), 1, 0),
      JPY_check = ifelse(str_detect(Asset, "JPY"), 1, 0),
      CNY_check = ifelse(str_detect(Asset, "CNY"), 1, 0),
      CAD_check = ifelse(str_detect(Asset, "CAD"), 1, 0)
    ) %>%
    mutate(
      bin_dat = case_when(
        Week_Change >= 0 ~ 1,
        Week_Change < 0 ~ 0
      )
    ) %>%
    filter(if_all(everything(), ~ !is.na(.)))

  testing_data_train <- testing_data %>%
    group_by(Asset) %>%
    slice_head(prop = train_percent) %>%
    ungroup()
  testing_data_test <- testing_data %>%
    group_by(Asset) %>%
    slice_tail(prop = (1 - train_percent) - 0.03 )%>%
    ungroup()

  remove_spaces_in_names <- names(testing_data_train) %>%
    map(~ str_replace_all(.x," ", "_") %>% str_trim()) %>% unlist() %>% as.character()

  names(testing_data_train) <- remove_spaces_in_names
  names(testing_data_test) <- remove_spaces_in_names

  reg_vars <- names(testing_data_train) %>%
    keep(~ .x != "date" & .x != "Price"& .x != "Open" &
           .x != "High" & .x != "Low" & .x != "Change_%" &
           .x != "daily_change" & .x!= "change_var" & .x!= "bin_dat" &
           .x != "week_date" & .x != "week_start_price" &
           .x != "weekly_forward_return" & .x != "Week_Change" & .x != "Asset" &
           .x != "Week_Change_lag" & .x != "sd3")
  macro_vars <- reg_vars %>%
    keep(~ !str_detect(.x, "lagged"))%>%
    keep(~ .x != "ma" & .x != "ma2" & .x != "ma3" & .x != "week_date" & .x != "week_start_price" &
           .x != "sdma" & .x != "sdma2" & .x != "sdma3" &
           .x != "weekly_forward_return" & .x != "Week_Change" & .x != "Asset" &
           .x != "Week_Change_lag" & .x != "sd3" & .x != "bin_dat")

  dependant_var <- "Week_Change"

  reg_formula <- create_lm_formula_no_space(dependant = dependant_var, independant = reg_vars)

  reg_formula_lm <-  create_lm_formula_no_space(dependant = dependant_var,
                                                independant = reg_vars
  )

  lm_reg <- lm(data = testing_data_train, formula = reg_formula_lm)
  prediction <- predict.lm(lm_reg, testing_data_test)
  prediction_training <- predict.lm(lm_reg, testing_data_train)

  raw_LM_trade_df <- testing_data_test %>%
    dplyr::select(Asset, Date) %>%
    mutate(
      LM_pred = predict.lm(lm_reg, testing_data_test) %>% as.numeric()
    )

  raw_LM_trade_df_training <- testing_data_train %>%
    dplyr::select(Asset, Date) %>%
    mutate(
      LM_pred = predict.lm(lm_reg, testing_data_train) %>% as.numeric()
    )

  return(

    list(
      "LM Model"= lm_reg,
      "Testing Data"= raw_LM_trade_df,
      "Training Data" = raw_LM_trade_df_training
    )

  )

}

prep_LM_daily_trade_data <- function(
    asset_data_daily_raw = fs::dir_info("C:/Users/Nikhil Chandra/Documents/Asset Data/Futures/") %>%
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
      ),
    raw_LM_trade_df,
    raw_LM_trade_df_training,
    new_week_date_start_day = 1

) {

  asset_data_daily <- asset_data_daily_raw %>%
    mutate(Date = as.Date(Date, format =  "%m/%d/%Y"))

  mean_LM_value <-
    raw_LM_trade_df_training %>%
    group_by(Asset) %>%
    summarise(
      mean_value = mean(LM_pred, na.rm = T),
      sd_value = sd(LM_pred, na.rm = T)
    )

  trade_with_daily_data <- asset_data_daily %>%
    left_join(raw_LM_trade_df, by = c("Date", "Asset")) %>%
    group_by(Asset) %>%
    arrange(Date, .by_group = TRUE) %>%
    group_by(Asset) %>%
    mutate(
      Pred_Filled = LM_pred
    ) %>%
    group_by(Asset) %>%
    fill(Pred_Filled, .direction = "down") %>%
    ungroup() %>%
    mutate(
      Pred_trade =LM_pred
    ) %>%
    filter(!is.na(Pred_Filled)) %>%
    left_join(mean_LM_value)

  return(
    list(
      "LM Merged to Daily" = trade_with_daily_data,
      "Mean LM Values" = mean_LM_value
    )
  )

}

