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

}

get_oanda_account_number <- function(account_name = "primary"){

  if(account_name == "primary"){return("001-011-1615559-001")}
  if(account_name == "mt4_hedging"){return("001-011-1615559-003")}
  if(account_name == "corr_no_macro"){return("001-011-1615559-004")}


}

get_account_summary <- function(account_var = 1){

  headers = c(
    `Content-Type` = 'application/json',
    `Authorization` = get_oanda_from_sys()
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
                                 account_var = 2){

    headers = c(
      `Content-Type` = 'application/json',
      `Authorization` = get_oanda_from_sys()
    )

    params <-
      list(
        `state` = "CLOSED",
        `count` = 500L
      )

    res <- httr::GET(url =
                       paste0(get_oanda_url(account = account_var),"/trades"),
                     httr::add_headers(.headers=headers),
                     query = params)

    returned_value <- jsonlite::fromJSON( jsonlite::prettify(res))

    if(any(str_detect(names(returned_value$trades),"trailingStopLossOrder|guaranteedStopLossOrder")) == TRUE){
      returned_value$trades <- returned_value$trades %>%
        select(-trailingStopLossOrder,-guaranteedStopLossOrder)
    }else{
      returned_value$trades <-  returned_value$trades
    }

    winning <- returned_value$trades  %>%
      select(-takeProfitOrder,-stopLossOrder) %>%
      bind_cols(returned_value$trades$takeProfitOrder %>%
                  select(cancelled_or_not = state)) %>%
      filter(cancelled_or_not != "CANCELLED") %>%
      mutate(
        date = as_date(str_extract(openTime,"[0-9]+-[0-9]+-[0-9]+"))
      ) %>%
      filter(
        date > "2021-09-02"
      ) %>%
      as_tibble() %>%
      select(date,id,instrument,price,openTime,initialUnits,initialMarginRequired,state,currentUnits,realizedPL,
             financing,dividendAdjustment,closeTime,averageClosePrice,state,cancelled_or_not)

    losing <- returned_value$trades %>%
      bind_cols(returned_value$trades$stopLossOrder %>%
                  select(cancelled_or_not = state)) %>%
      filter(cancelled_or_not != "CANCELLED") %>%
      mutate(
        date = as_date(str_extract(openTime,"[0-9]+-[0-9]+-[0-9]+"))
      ) %>%
      filter(
        date > "2021-09-02"
      ) %>%
      as_tibble() %>%
      select(date,id,instrument,price,openTime,initialUnits,initialMarginRequired,state,currentUnits,realizedPL,
             financing,dividendAdjustment,closeTime,averageClosePrice,state,cancelled_or_not)

    complete_frame <- winning %>%
      bind_rows(losing) %>%
      mutate(
        closing_date =  as_date(str_extract(closeTime,"[0-9]+-[0-9]+-[0-9]+"))
      )

    if(save_csv == TRUE){
      write.csv(x = complete_frame,file = glue::glue("data/live_trade_data/live_trades_{format(lubridate::now(), '%Y_%m_%d_%H_%M_%S')}.csv"))
    }

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

