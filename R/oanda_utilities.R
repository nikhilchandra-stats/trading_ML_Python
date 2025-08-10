get_oanda_data_candles_normalised <- function(
    assets = c("AUD_USD"),
    granularity = "D",
    date_var = "2011-01-01",
    date_var_start = NULL,
    time = "T15:00:00.000000000Z",
    how_far_back = 5000,
    bid_or_ask = "bid",
    sleep_time = 1
) {

  oanda_asset_data <-
    get_oanda_data_candles(
      assets = assets,
      granularity = granularity,
      date_var = date_var,
      date_var_start = date_var_start,
      time = time,
      how_far_back = how_far_back,
      sleep_time = sleep_time
    )

  transformed_data <-
    oanda_asset_data %>%
    filter(bid_ask == bid_or_ask) %>%
    dplyr::select(
      Date = date,
      Price = c,
      Open = o,
      High = h,
      Low = l,
      `Vol.` = volume
    ) %>%
    mutate(
      `Change %` = Price -  Open
    )

  return(transformed_data)

}

get_oanda_data_candles_normalised_intra <- function(
    assets = c("AUD_USD"),
    granularity = "D",
    date_var = "2011-01-01",
    date_var_start = NULL,
    time = "T15:00:00.000000000Z",
    how_far_back = 5000,
    bid_or_ask = "bid",
    sleep_time = 1
) {

  oanda_asset_data <-
    get_oanda_data_candles(
      assets = assets,
      granularity = granularity,
      date_var = date_var,
      date_var_start = date_var_start,
      time = time,
      how_far_back = how_far_back,
      sleep_time = sleep_time
    )

  transformed_data_1 <-
    oanda_asset_data %>%
    filter(bid_ask == bid_or_ask) %>%
    dplyr::select(
      Date = date_time,
      Price = c,
      Open = o
      # High = h,
      # Low = l,
      # `Vol.` = volume
    )
    # mutate(
    #   `Change %` = Price -  Open
    # )

  if(bid_or_ask == "bid") {
    second_bid_ask <- "ask"
  } else {
    second_bid_ask <- "bid"
  }

  transformed_data_2 <-
    oanda_asset_data %>%
    filter(bid_ask == second_bid_ask) %>%
    dplyr::select(
      Date = date_time,
      High = h,
      Low = l,
      `Vol.` = volume
    )

  transformed_data <- transformed_data_1 %>%
    left_join(
      transformed_data_2
    )


  return(transformed_data)

}

get_oanda_api_keyring <- function(){

  vars_sys <- Sys.getenv()

  index_req <- str_detect(names(vars_sys), "OANDA")

  index_req <- which(index_req, TRUE)

  oanda_var <- names(vars_sys[index_req]) %>% pluck(1)

  key2 <- str_extract_all(Sys.getenv(oanda_var), "[a-z]|[A-Z]|[0-9]|-| ", simplify = T) %>% paste0(collapse = "")

  return(key2)
}

get_oanda_from_sys <- function(){

  vars_sys <- Sys.getenv()

  index_req <- str_detect(names(vars_sys), "OANDA")

  index_req <- which(index_req, TRUE)

  oanda_var <- names(vars_sys[index_req]) %>% pluck(1)

  key2 <- str_extract_all(Sys.getenv(oanda_var), "[a-z]|[A-Z]|[0-9]|-| ", simplify = T) %>% paste0(collapse = "")

  return(key2)

}

get_list_of_accounts <- function(){

  headers = c(
    `Content-Type` = 'application/json',
    `Authorization` = get_oanda_from_sys()
  )

  res <- httr::GET(url =
                     paste0('https://api-fxtrade.oanda.com/v3/accounts'),
                   httr::add_headers(.headers=headers))
  returned_value <- jsonlite::fromJSON( jsonlite::prettify(res) )

  return(returned_value)

}


get_list_of_positions <- function(account_var = 1){

  headers = c(
    `Content-Type` = 'application/json',
    `Authorization` = get_oanda_from_sys()
  )

  res <- httr::GET(url =
                     paste0(get_oanda_url(account = account_var),"/openPositions"),
                   httr::add_headers(.headers=headers))
  returned_value <- jsonlite::fromJSON( jsonlite::prettify(res) )

  if( is_empty(returned_value$positions) == FALSE ){

    positions <-
      bind_cols(returned_value$positions$instrument,
                returned_value$positions$long) %>%
      filter(units >0) %>%
      mutate(
        direction = "long"
      ) %>%
      bind_rows(
            bind_cols(returned_value$positions$instrument,
                returned_value$positions$short) %>%
                  filter(units < 0) %>%
              mutate(
                direction = "short"
              )
                ) %>%
      rename(instrument = 1) %>%
      select(instrument, units,direction)  %>%
      mutate(units = abs(as.numeric(units)))
  }else{
    positions = c("empty")
  }


  return(positions)

}


get_oanda_url <- function( account = 1 ){

  if(account == 1){  return('https://api-fxtrade.oanda.com/v3/accounts/001-011-1615559-001')}
  if(account == 2){  return('https://api-fxtrade.oanda.com/v3/accounts/001-011-1615559-003')}
  if(account == 3){  return('https://api-fxtrade.oanda.com/v3/accounts/001-011-1615559-004')}

  # Equity Accounts
  if(account == 4){  return('https://api-fxtrade.oanda.com/v3/accounts/001-011-1615559-002')}
  if(account == 5){  return('https://api-fxtrade.oanda.com/v3/accounts/001-011-1615559-005')}

}

get_oanda_account_number <- function(account_name = "primary"){

  if(account_name == "primary"){return("001-011-1615559-001")}
  if(account_name == "mt4_hedging"){return("001-011-1615559-003")}
  if(account_name == "corr_no_macro"){return("001-011-1615559-004")}
  if(account_name == "equity_long"){return("001-011-1615559-002")}
  if(account_name == "equity_short"){return("001-011-1615559-005")}

}

get_account_summary <- function(account_var = 2){

  headers = c(
    `Content-Type` = 'application/json',
    `Authorization` = get_oanda_from_sys()
    # `Authorization` = "Bearer 0321f8a633d09bf602613fb11255fadf-b2c91d351f1e7d21f82c6caab40c4872"
  )

  res <- httr::GET(url =
                     paste0(get_oanda_url(account = account_var),"/summary"),
                   httr::add_headers(.headers=headers))

  test <- jsonlite::fromJSON( jsonlite::prettify(res) )
  account_sum <- test$account %>% as_tibble()

  return(account_sum)

}

get_oanda_symbols <- function( account_var = 1){

  require(httr)

  headers = c(
    `Content-Type` = 'application/json',
    `Authorization` = get_oanda_from_sys()
  )

  res <- httr::GET(url =
                     paste0(get_oanda_url(account = account_var),"/instruments"),
                   httr::add_headers(.headers=headers))

  test <- jsonlite::fromJSON( jsonlite::prettify(res) )
  symbols <- test$instruments %>% as_tibble()
  symbol_info <- as_tibble(
    data.frame( symbols = symbols$name, stringsAsFactors = FALSE)
  )

  return(symbol_info$symbols)

}

earliest_allowable_date <- function(){
  return("2014-01-01")
}

get_oanda_data_candles <- function(assets = assets_x,
                                   granularity = "H2",
                                   date_var = "2020-01-01",
                                   date_var_start = NULL,
                                   time = "T15:00:00.000000000Z",
                                   how_far_back = 2001,
                                   sleep_time = 1){

  asset_list = assets
  returned_data <- list()

  for(i in 1:length(asset_list) ){

    Sys.sleep(sleep_time)

    headers = c(
      `Content-Type` = 'application/json',
      `Authorization` = get_oanda_api_keyring()
    )

    if(is.null(date_var_start)){
      params = list(
        `price` = 'BAM',
        `from` = glue::glue('{date_var}{time}'),
        `granularity` = granularity,
        `count` = how_far_back,
        `alignmentTimezone` = "Australia/Sydney"
      )
    }else{
      params = list(
        `price` = 'BAM',
        `from` = glue::glue('{date_var_start}{time}'),
        `granularity` = granularity,
        `count` = 2000,
        `alignmentTimezone` = 'Australia/Sydney'
      )

    }


    res <- httr::GET(url = glue::glue('https://api-fxtrade.oanda.com/v3/instruments/{asset_list[i]}/candles'),
                     httr::add_headers(.headers=headers), query = params)

    test <- jsonlite::fromJSON( jsonlite::prettify(res) )

    data1 <- test$candles
    dates <- data1$time
    volume <- data1$volume

    returned_data[[i]]  <- data1$bid %>%
      dplyr::as_tibble() %>%
      dplyr::mutate(o = as.numeric(o),
                    h = as.numeric(h),
                    l = as.numeric(l),
                    c = as.numeric(c),
                    bid_ask = "bid") %>%
      dplyr::mutate(date_time = dates) %>%
      dplyr::mutate(volume = volume) %>%
      dplyr::bind_rows( data1$ask %>%
              dplyr::as_tibble()%>%
              dplyr::mutate(o = as.numeric(o),
              h = as.numeric(h),
              l = as.numeric(l),
              c =  as.numeric(c),
             bid_ask = "ask" ) %>%
               dplyr::mutate(date_time = dates) %>%
               dplyr::mutate(volume = volume)
             ) %>%
      dplyr::bind_rows( data1$mid %>%
                          dplyr::as_tibble()%>%
                          dplyr::mutate(o = as.numeric(o),
                                        h = as.numeric(h),
                                        l = as.numeric(l),
                                        c =  as.numeric(c),
                                        bid_ask = "mid" ) %>%
                          dplyr::mutate(date_time = dates) %>%
                          dplyr::mutate(volume = volume)
      ) %>%
      dplyr::mutate(
        symbol = asset_list[i]
      ) %>%
      dplyr::mutate(time_frame = granularity ) %>%
      dplyr::mutate(
        date_time = lubridate::as_datetime(date_time, tz = "Australia/Sydney"),
        date = lubridate::as_date(
          paste0( lubridate::year(date_time),"-",lubridate::month(date_time),"_",lubridate::day(date_time))
        ),
        time = strftime(date_time, format = "%H:%M:%S")
      )


  }

  names(returned_data) <- asset_list
  returned_data <- returned_data %>%
    purrr::map_df(bind_rows)

  return(returned_data)

}



get_oanda_data_position_book <- function(assets = assets_x){

  asset_list = assets
  returned_data <- list()
  for(i in 1:length(asset_list) ){

    headers = c(
      `Content-Type` = 'application/json',
      `Authorization` = get_oanda_api_keyring()
    )

    params = list(
      `time` = '2021-01-17T15:00:00.000000000Z'
    )

    res <- httr::GET(url = glue::glue('https://api-fxtrade.oanda.com/v3/instruments/{asset_list[i]}/positionBook'),
                     httr::add_headers(.headers=headers), query = params)

    test <- jsonlite::fromJSON( jsonlite::prettify(res) )

    data1 <- test$positionBook

  }

}


get_closed_positions <- function(save_csv = FALSE,
                                 account_var = 2,
                                 asset = "AUD_USD"){

    headers = c(
      `Content-Type` = 'application/json',
      `Authorization` = get_oanda_from_sys()
    )

    params <-
      list(
        `state` = "CLOSED",
        `count` = 500L,
        instrument = asset
      )

    res <- httr::GET(url =
                       paste0(get_oanda_url(account = account_var),"/trades"),
                     httr::add_headers(.headers=headers),
                     query = params)

    returned_value <- jsonlite::fromJSON( jsonlite::prettify(res))

    returned_value$trades <-  returned_value$trades

    complete_frame <- returned_value$trades  %>%
      select(-takeProfitOrder,-stopLossOrder) %>%
      bind_cols(returned_value$trades$takeProfitOrder %>%
                  select(cancelled_or_not = state)) %>%
      mutate(
        date_open = as_datetime(openTime),
        date_closed = as_datetime(closeTime)
      )  %>%
      distinct(id, instrument, realizedPL, date_closed, date_open)

    return(complete_frame)

}


get_instrument_info <- function( account_var = 2){

  headers = c(
    `Content-Type` = 'application/json',
    `Authorization` = get_oanda_from_sys()
  )

  instrument_details <-httr::GET(url =
                                   paste0(get_oanda_url(account = account_var),"/instruments"),
                                 httr::add_headers(.headers=headers))

  instrument_details <- jsonlite::fromJSON( jsonlite::prettify(instrument_details) )

  instrument_details <- instrument_details$instruments

  financing_info <- instrument_details$financing %>%
    dplyr::select(longRate,shortRate)


  instrument_info <- instrument_details %>%
    dplyr::select(-financing,-guaranteedStopLossOrderLevelRestriction,-tags) %>%
    dplyr::bind_cols(financing_info)

  return(instrument_info)

}



#' read_all_asset_data
#'
#' @param asset_list_oanda
#' @param save_path_oanda_assets
#' @param read_csv_or_API
#'
#' @return
#' @export
#'
#' @examples
read_all_asset_data <- function(
    asset_list_oanda = get_oanda_symbols() %>%
      keep( ~ .x %in% c("HK33_HKD", "USD_JPY", "BTC_USD", "AUD_NZD", "GBP_CHF",
                        "EUR_HUF", "EUR_ZAR", "NZD_JPY", "EUR_NZD", "USB02Y_USD",
                        "XAU_CAD", "GBP_JPY", "EUR_NOK", "USD_SGD", "EUR_SEK", "DE30_EUR",
                        "AUD_CAD", "UK10YB_GBP", "XPD_USD", "UK100_GBP", "USD_CHF", "GBP_NZD",
                        "GBP_SGD", "USD_SEK", "EUR_SGD", "XCU_USD", "SUGAR_USD", "CHF_ZAR",
                        "AUD_CHF", "EUR_CHF", "USD_MXN", "GBP_USD", "WTICO_USD", "EUR_JPY", "USD_NOK",
                        "XAU_USD", "DE10YB_EUR", "USD_CZK", "AUD_SGD", "USD_HUF", "WHEAT_USD",
                        "EUR_USD", "SG30_SGD", "GBP_AUD", "NZD_CAD", "AU200_AUD", "XAG_USD",
                        "XAU_EUR", "EUR_GBP", "USD_CNH", "USD_CAD", "NAS100_USD", "USB10Y_USD",
                        "EU50_EUR", "NATGAS_USD", "CAD_JPY", "FR40_EUR", "USD_ZAR", "XAU_GBP",
                        "CH20_CHF", "ESPIX_EUR", "XPT_USD", "EUR_AUD", "SOYBN_USD", "US2000_USD",
                        "BCO_USD")
      ),
    save_path_oanda_assets = "C:/Users/Nikhil Chandra/Documents/Asset Data/oanda_data/",
    read_csv_or_API = "csv"
) {

  if(read_csv_or_API == "csv") {
    extracted_asset_data <- fs::dir_info(save_path_oanda_assets) %>%
      pull(path) %>%
      map(read_csv)
  }

  if(read_csv_or_API == "API") {

    asset_infor <- get_instrument_info()
    extracted_asset_data <- list()
    Sys.sleep(0.5)

    for (i in 1:length(asset_list_oanda)) {

      extracted_asset_data[[i]] <-
        get_oanda_data_candles_normalised(
          assets = c(asset_list_oanda[i]),
          granularity = "D",
          date_var = "2011-01-01",
          date_var_start = NULL,
          time = "T15:00:00.000000000Z",
          how_far_back = 5000,
          bid_or_ask = "bid",
          sleep_time = 0
        ) %>%
        mutate(
          Asset = asset_list_oanda[i]
        )

      write.csv(
        x = extracted_asset_data[[i]],
        file= glue::glue("{save_path_oanda_assets}{asset_list_oanda[i]}.csv"),
        row.names = FALSE
      )

    }

  }

  return(extracted_asset_data)

}


#' read_all_asset_data
#'
#' @param asset_list_oanda
#' @param save_path_oanda_assets
#' @param read_csv_or_API
#'
#' @return
#' @export
#'
#' @examples
read_all_asset_data_intra_day <- function(
    asset_list_oanda = get_oanda_symbols() %>%
      keep( ~ .x %in% c("HK33_HKD", "USD_JPY", "BTC_USD", "AUD_NZD", "GBP_CHF",
                        "EUR_HUF", "EUR_ZAR", "NZD_JPY", "EUR_NZD", "USB02Y_USD",
                        "XAU_CAD", "GBP_JPY", "EUR_NOK", "USD_SGD", "EUR_SEK", "DE30_EUR",
                        "AUD_CAD", "UK10YB_GBP", "XPD_USD", "UK100_GBP", "USD_CHF", "GBP_NZD",
                        "GBP_SGD", "USD_SEK", "EUR_SGD", "XCU_USD", "SUGAR_USD", "CHF_ZAR",
                        "AUD_CHF", "EUR_CHF", "USD_MXN", "GBP_USD", "WTICO_USD", "EUR_JPY", "USD_NOK",
                        "XAU_USD", "DE10YB_EUR", "USD_CZK", "AUD_SGD", "USD_HUF", "WHEAT_USD",
                        "EUR_USD", "SG30_SGD", "GBP_AUD", "NZD_CAD", "AU200_AUD", "XAG_USD",
                        "XAU_EUR", "EUR_GBP", "USD_CNH", "USD_CAD", "NAS100_USD", "USB10Y_USD",
                        "EU50_EUR", "NATGAS_USD", "CAD_JPY", "FR40_EUR", "USD_ZAR", "XAU_GBP",
                        "CH20_CHF", "ESPIX_EUR", "XPT_USD", "EUR_AUD", "SOYBN_USD", "US2000_USD",
                        "BCO_USD")
      ),
    save_path_oanda_assets = "C:/Users/Nikhil Chandra/Documents/Asset Data/oanda_data/",
    read_csv_or_API = "csv",
    time_frame = "H1",
    bid_or_ask = "bid",
    how_far_back = 5000,
    start_date = "2011-01-01"
) {

  read_csv_or_API = "API"

  if(read_csv_or_API == "API") {

    extracted_asset_data <- list()
    # Sys.sleep(0.5)

    for (i in 1:length(asset_list_oanda)) {

      extracted_asset_data[[i]] <-
        get_oanda_data_candles_normalised_intra(
          assets = c(asset_list_oanda[i]),
          granularity = time_frame,
          date_var = start_date,
          date_var_start = NULL,
          time = "T15:00:00.000000000Z",
          how_far_back = how_far_back,
          bid_or_ask = bid_or_ask,
          sleep_time = 0
        ) %>%
        mutate(
          Asset = asset_list_oanda[i]
        )

    }

  }

  return(extracted_asset_data)

}

get_aud_conversion <- function(asset_data_daily_raw = asset_data_daily_raw) {

  aud_usd_today <- asset_data_daily_raw %>% filter(str_detect(Asset, "AUD")) %>%
    group_by(Asset) %>%
    slice_max(Date)  %>%
    ungroup() %>%
    dplyr::select(Price, Asset) %>%
    mutate(ending_value = str_extract(Asset, "_[A-Z][A-Z][A-Z]") %>% str_remove_all("_"),
           starting_value = str_extract(Asset, "[A-Z][A-Z][A-Z]_") %>% str_remove_all("_")) %>%
    mutate(
      adjusted_conversion =
        case_when(
          ending_value != "AUD" ~ 1/Price,
          TRUE ~ Price
        ),
      ending_value =
        case_when(
          ending_value == "AUD" ~ starting_value,
          TRUE ~ ending_value
        )
    ) %>%
    dplyr::select(-starting_value) %>%
    filter(!is.na(ending_value))

  aud_usd_value <- aud_usd_today %>%
    filter(Asset == "AUD_USD") %>%
    dplyr::select(Asset, adjusted_conversion) %>%
    pull(adjusted_conversion)

  SEK_ZAR_HUF_NOK_conversion <- asset_data_daily_raw %>%
    filter(str_detect(Asset, "USD_SEK|USD_NOK|USD_HUF|USD_ZAR|CNY|USD_MXN|USD_CNH"))  %>%
    group_by(Asset) %>%
    slice_max(Date)  %>%
    ungroup() %>%
    dplyr::select(Price, Asset) %>%
    dplyr::mutate(dummy_join = "USD") %>%
    mutate(
      adjusted_conversion = aud_usd_value,
      adjusted_conversion = Price/adjusted_conversion,
      ending_value  = str_extract(Asset, "_[A-Z][A-Z][A-Z]") %>% str_remove_all("_")
    ) %>%
    dplyr::select(-dummy_join)
    # mutate(
    #   adjusted_conversion =
    #     case_when(
    #       ending_value == "SEK" | ending_value == "NOK" ~ 1,
    #       TRUE ~ adjusted_conversion
    #     )
    # )

  returned_value <- aud_usd_today %>%
    bind_rows(SEK_ZAR_HUF_NOK_conversion) %>%
    filter(!str_detect(ending_value, "XAU|XAG"))

  # test_calculation <-
  #   mean_values_by_asset_for_loop %>%
  #   mutate(ending_value = str_extract(Asset, "_[A-Z][A-Z][A-Z]") %>% str_remove_all("_")) %>%
  #   left_join(returned_value %>%
  #               dplyr::select(-Asset, -Price) %>%
  #               bind_rows(
  #                 tibble(ending_value  = "AUD", adjusted_conversion = 1)
  #               )
  #             ) %>%
  #   mutate(
  #     raw_price_move = round(mean_daily + sd_daily*3, 5)*adjusted_conversion,
  #     raw_price_move_aud = raw_price_move*adjusted_conversion
  #   ) %>%
  #   left_join(asset_infor %>%  rename(Asset = name))

  return(returned_value)

}

convert_stop_profit_AUD <- function(trade_data = trade_data,
                                    asset_infor = asset_infor,
                                    currency_conversion = currency_conversion,
                                    asset_col = "Asset",
                                    stop_col = "starting_stop_value",
                                    profit_col = "starting_profit_value",
                                    price_col = "trade_start_prices",
                                    risk_dollar_value = 20,
                                    returns_present = FALSE,
                                    trade_return_col = "trade_return") {

  asset_infor_internal <- asset_infor %>%
    rename(!!as.name(asset_col) := name)

  analysis <-
    trade_data %>%
    mutate(ending_value = str_extract(!!as.name(asset_col), "_[A-Z][A-Z][A-Z]"),
           ending_value = str_remove_all(ending_value, "_")
           ) %>%
    left_join(currency_conversion, by =c("ending_value" = "not_aud_asset")) %>%
    left_join(asset_infor_internal) %>%
    mutate(
      minimumTradeSize = abs(log10(as.numeric(minimumTradeSize))),
      marginRate = as.numeric(marginRate),
      pipLocation = as.numeric(pipLocation),
      displayPrecision = as.numeric(displayPrecision)
    ) %>%
    ungroup() %>%
    mutate(
      stop_value = round(!!as.name(stop_col), abs(pipLocation) ),
      profit_value = round(!!as.name(profit_col), abs(pipLocation) )
    ) %>%
    mutate(
      # asset_size = floor(log10(!!as.name(price_col))),
      volume_adjustment = 1,
      AUD_Price =
        case_when(
          !is.na(adjusted_conversion) ~ (!!as.name(price_col)*adjusted_conversion)/volume_adjustment,
          TRUE ~ !!as.name(price_col)/volume_adjustment
        ),
      stop_value_AUD =
        case_when(
          !is.na(adjusted_conversion) ~ (stop_value*adjusted_conversion)/volume_adjustment,
          TRUE ~ stop_value/volume_adjustment
        ),
      profit_value_AUD =
        case_when(
          !is.na(adjusted_conversion) ~ (profit_value*adjusted_conversion)/volume_adjustment,
          TRUE ~ profit_value/volume_adjustment
        )
    ) %>%
    mutate(
      # volume_unadj =  risk_dollar_value/stop_value_AUD,
      volume_unadj =
        case_when(
          str_detect(Asset,"SEK|NOK|ZAR|MXN|CNH") ~ (risk_dollar_value/stop_value)*adjusted_conversion,
          TRUE ~ (risk_dollar_value/stop_value)/adjusted_conversion
        ),
      volume_required = volume_unadj,
      volume_adj = round(volume_unadj, minimumTradeSize),
      minimal_loss =
        case_when(
          str_detect(Asset,"SEK|NOK|ZAR|MXN|CNH") ~ ((risk_dollar_value/stop_value)/adjusted_conversion)*stop_value_AUD,
          TRUE ~ volume_adj*stop_value_AUD
        ),
      maximum_win =
        case_when(
          str_detect(Asset,"SEK|NOK|ZAR|MXN|CNH") ~ ((risk_dollar_value/stop_value)/adjusted_conversion)*profit_value_AUD,
          TRUE ~ volume_adj*profit_value_AUD
        ),
      trade_value = AUD_Price*volume_adj*marginRate,
      estimated_margin = trade_value,
      volume_required = volume_adj
    )

  if(returns_present == TRUE) {

    analysis <- analysis %>%
      mutate(
        trade_return_dollars_AUD =
          case_when(
            !!as.name(trade_return_col) < 0 ~ -minimal_loss,
            !!as.name(trade_return_col) > 0 ~ maximum_win
          )
      )

  }

  analysis <- analysis %>%
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

  return(analysis)

}

get_intra_day_asset_data <- function(
    asset_list_oanda = get_oanda_symbols() %>%
      keep( ~ .x %in% c("USD_JPY", "GBP_JPY", "USD_SGD", "EUR_SEK",
                        "DE30_EUR",
                        "USD_CHF", "USD_SEK", "XCU_USD", "SUGAR_USD",
                        "USD_MXN", "GBP_USD", "WTICO_USD", "EUR_JPY", "USD_NOK",
                        "XAU_USD",
                        "USD_CZK",  "WHEAT_USD",
                        "EUR_USD", "SG30_SGD", "AU200_AUD", "XAG_USD",
                        "EUR_GBP", "USD_CNH", "USD_CAD", "NAS100_USD",
                        "EU50_EUR", "NATGAS_USD", "SOYBN_USD",
                        "US2000_USD",
                        "BCO_USD", "AUD_USD", "NZD_USD", "NZD_CHF", "WHEAT_USD",
                        "JP225_USD", "SPX500_USD")
      ),
    bid_or_ask = "ask",
    start_date = "2016-01-01"
  ) {

  extracted_asset_data1 <-
    read_all_asset_data_intra_day(
      asset_list_oanda = asset_list_oanda,
      save_path_oanda_assets = "C:/Users/Nikhil Chandra/Documents/Asset Data/oanda_data/",
      read_csv_or_API = "API",
      time_frame = "H1",
      bid_or_ask = bid_or_ask,
      how_far_back = 5000,
      start_date = start_date
    )

  extracted_asset_data1 <- extracted_asset_data1 %>% map_dfr(bind_rows)
  max_date_in_1 <- extracted_asset_data1 %>%
    group_by(Asset) %>%
    slice_max(Date) %>%
    ungroup() %>%
    slice_min(Date) %>%
    pull(Date) %>% pluck(1) %>% as.character()
  min_date_in_1 <- extracted_asset_data1$Date %>% min(na.rm = T) %>% as_date() %>% as.character()

  extracted_asset_data2 <-
    read_all_asset_data_intra_day(
      asset_list_oanda = asset_list_oanda,
      save_path_oanda_assets = "C:/Users/Nikhil Chandra/Documents/Asset Data/oanda_data/",
      read_csv_or_API = "API",
      time_frame = "H1",
      bid_or_ask = bid_or_ask,
      how_far_back = 5000,
      start_date = max_date_in_1
    )

  extracted_asset_data2 <- extracted_asset_data2 %>% map_dfr(bind_rows)
  max_date_in_2 <- extracted_asset_data2 %>%
    group_by(Asset) %>%
    slice_max(Date) %>%
    ungroup() %>%
    slice_min(Date) %>%
    pull(Date) %>% pluck(1) %>% as.character()
  min_date_in_2 <- extracted_asset_data2$Date %>% min(na.rm = T) %>% as_date() %>% as.character()

  extracted_asset_data3 <-
    read_all_asset_data_intra_day(
      asset_list_oanda = asset_list_oanda,
      save_path_oanda_assets = "C:/Users/Nikhil Chandra/Documents/Asset Data/oanda_data/",
      read_csv_or_API = "API",
      time_frame = "H1",
      bid_or_ask = bid_or_ask,
      how_far_back = 5000,
      start_date = max_date_in_2
    )

  extracted_asset_data3 <- extracted_asset_data3 %>% map_dfr(bind_rows)
  max_date_in_3 <- extracted_asset_data3 %>%
    group_by(Asset) %>%
    slice_max(Date) %>%
    ungroup() %>%
    slice_min(Date) %>%
    pull(Date) %>% pluck(1) %>% as.character()

  extracted_asset_data4 <-
    read_all_asset_data_intra_day(
      asset_list_oanda = asset_list_oanda,
      save_path_oanda_assets = "C:/Users/Nikhil Chandra/Documents/Asset Data/oanda_data/",
      read_csv_or_API = "API",
      time_frame = "H1",
      bid_or_ask = bid_or_ask,
      how_far_back = 5000,
      start_date = max_date_in_3
    )

  extracted_asset_data4 <- extracted_asset_data4 %>% map_dfr(bind_rows)
  max_date_in_4 <- extracted_asset_data4 %>%
    group_by(Asset) %>%
    slice_max(Date) %>%
    ungroup() %>%
    slice_min(Date) %>%
    pull(Date) %>% pluck(1) %>% as.character()

  extracted_asset_data5 <-
    read_all_asset_data_intra_day(
      asset_list_oanda = asset_list_oanda,
      save_path_oanda_assets = "C:/Users/Nikhil Chandra/Documents/Asset Data/oanda_data/",
      read_csv_or_API = "API",
      time_frame = "H1",
      bid_or_ask = bid_or_ask,
      how_far_back = 5000,
      start_date = max_date_in_4
    )

  extracted_asset_data5 <- extracted_asset_data5 %>% map_dfr(bind_rows)
  max_date_in_5 <- extracted_asset_data5 %>%
    group_by(Asset) %>%
    slice_max(Date) %>%
    ungroup() %>%
    slice_min(Date) %>%
    pull(Date) %>% pluck(1) %>% as.character()

  extracted_asset_data6 <-
    read_all_asset_data_intra_day(
      asset_list_oanda = asset_list_oanda,
      save_path_oanda_assets = "C:/Users/Nikhil Chandra/Documents/Asset Data/oanda_data/",
      read_csv_or_API = "API",
      time_frame = "H1",
      bid_or_ask = bid_or_ask,
      how_far_back = 5000,
      start_date = max_date_in_5
    )

  extracted_asset_data6 <- extracted_asset_data6 %>% map_dfr(bind_rows)
  max_date_in_6 <- extracted_asset_data6 %>%
    group_by(Asset) %>%
    slice_max(Date) %>%
    ungroup() %>%
    slice_min(Date) %>%
    pull(Date) %>% pluck(1) %>% as.character()

  extracted_asset_data7 <-
    read_all_asset_data_intra_day(
      asset_list_oanda = asset_list_oanda,
      save_path_oanda_assets = "C:/Users/Nikhil Chandra/Documents/Asset Data/oanda_data/",
      read_csv_or_API = "API",
      time_frame = "H1",
      bid_or_ask = bid_or_ask,
      how_far_back = 5000,
      start_date = max_date_in_6
    )


  data_list <- list(extracted_asset_data1,
                    extracted_asset_data2,
                    extracted_asset_data3,
                    extracted_asset_data4,
                    extracted_asset_data5,
                    extracted_asset_data6,
                    extracted_asset_data7)

  data_list_dfr <- data_list %>%
    map_dfr(bind_rows) %>%
    group_by(Asset, Date) %>%
    mutate(
      row_counts = n()
    ) %>%
    ungroup() %>%
    slice_min(row_counts) %>%
    dplyr::select(-row_counts) %>%
    distinct()

  return(data_list_dfr)

}
