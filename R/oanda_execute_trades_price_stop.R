oanda_place_order_price_stop <- function(
  asset = "AUD_JPY",
  volume = -1,
  stopLoss = round(trades_to_take$stop_loss[1],3),
  takeProfit = round(trades_to_take$take_profit[1],3),
  type = "MARKET",
  timeinForce = "FOK",
  acc_name = "mt4_hedging",
  position_fill = "DEFAULT",
  price

){

  headers = c(
    `Content-Type` = 'application/json',
    `Authorization` = get_oanda_from_sys()
  )

  if(type == "MARKET"){

    params = list(
      `order` = list(
        `units` = glue::glue("{volume}"),
        `instrument` = glue::glue("{asset}"),
        `timeInForce` = glue::glue("{timeinForce}"),
        `type` = glue::glue("{type}"),
        `positionFill` = position_fill,
        `stopLossOnFill` = list(
          `timeInForce` = glue::glue("GTC"),
          `price` = glue::glue("{stopLoss}")
        ),
        `takeProfitOnFill` = list(
          `timeInForce` = glue::glue("GTC"),
          `price` = glue::glue("{takeProfit}")
        )
      )
    )

  }else{

    params = list(
      `order` = list(
        `units` = glue::glue("{volume}"),
        `instrument` = glue::glue("{asset}"),
        `timeInForce` = glue::glue("{timeinForce}"),
        `price` =  glue::glue("{price]"),
        `type` = "LIMIT",
        `positionFill` = position_fill
      ),
      `stopLossOnFill` = list(
        `timeInForce` = glue::glue("GTC"),
        `price` = glue::glue("{stopLoss}")
      ),
      `takeProfitOnFill` = list(
        `timeInForce` = glue::glue("GTC"),
        `price` = glue::glue("{takeProfit}")
      )
    )

  }


  json_body <- rjson::toJSON(params)

  account_number <- get_oanda_account_number(account_name = acc_name)


  posted <-  httr::POST(url = glue::glue('https://api-fxtrade.oanda.com/v3/accounts/{account_number}/orders'),
             httr::add_headers(.headers=headers),
             body = json_body,
             encode = "raw",verbose())

  return(posted)
}


#' Title
#'
#' @param asset
#' @param volume
#' @param stopLoss
#' @param takeProfit
#' @param type
#' @param timeinForce
#' @param acc_name
#' @param position_fill
#' @param price
#'
#' @return
#' @export
#'
#' @examples
oanda_place_order_pip_stop <- function(
  asset = "AUD_JPY",
  volume = -1,
  stopLoss = round(trades_to_take$stop_loss[1],3),
  takeProfit = round(trades_to_take$take_profit[1],3),
  type = "MARKET",
  timeinForce = "FOK",
  acc_name = "mt4_hedging",
  position_fill = "DEFAULT",
  price

){

  headers = c(
    `Content-Type` = 'application/json',
    `Authorization` = get_oanda_from_sys()
  )

  if(type == "MARKET"){

    params = list(
      `order` = list(
        `units` = glue::glue("{volume}"),
        `instrument` = glue::glue("{asset}"),
        `timeInForce` = glue::glue("{timeinForce}"),
        `type` = glue::glue("{type}"),
        `positionFill` = position_fill,
        `stopLossOnFill` = list(
          `timeInForce` = glue::glue("GTC"),
          `distance` = (glue::glue("{stopLoss}"))
        ),
        `takeProfitOnFill` = list(
          `timeInForce` = glue::glue("GTC"),
          `distance` = (glue::glue("{takeProfit}"))
        )
      )
    )

  }else{

    params = list(
      `order` = list(
        `units` = glue::glue("{volume}"),
        `instrument` = glue::glue("{asset}"),
        `timeInForce` = glue::glue("{timeinForce}"),
        `price` =  glue::glue("{price]"),
        `type` = "LIMIT",
        `positionFill` = position_fill
      ),
      `stopLossOnFill` = list(
        `timeInForce` = glue::glue("GTC"),
        `distance` = (glue::glue("{stopLoss}"))
      ),
      `takeProfitOnFill` = list(
        `timeInForce` = glue::glue("GTC"),
        `distance` = (glue::glue("{takeProfit}"))
      )
    )

  }


  json_body <- rjson::toJSON(params)

  account_number <- get_oanda_account_number(account_name = acc_name)


  posted <-  httr::POST(url = glue::glue('https://api-fxtrade.oanda.com/v3/accounts/{account_number}/orders'),
                        httr::add_headers(.headers=headers),
                        body = json_body,
                        encode = "raw",verbose())

  return(posted)
}


#' oanda_store_executed_trade
#'
#' @param asset
#' @param volume
#' @param stopLoss
#' @param takeProfit
#' @param type
#' @param timeinForce
#' @param acc_name
#' @param position_fill
#' @param price
#'
#' @return
#' @export
#'
#' @examples
oanda_store_executed_trade <-
  function(
    asset = 'AUD_USD',
    volume = 1,
    stopLoss = 0.001,
    takeProfit = 0.001,
    type = "MARKET",
    timeinForce = "FOK",
    acc_name = "equity_long",
    position_fill = "OPEN_ONLY" ,
    price
  ) {

    http_return <- oanda_place_order_pip_stop(
      asset = asset,
      volume = volume,
      stopLoss = stopLoss,
      takeProfit = takeProfit,
      type = "MARKET",
      timeinForce = "FOK",
      acc_name = acc_name,
      position_fill = "OPEN_ONLY" ,
      price
    )

    returned_response <- jsonlite::fromJSON( jsonlite::prettify(http_return))
    trade_Id_Details <-
      returned_response$orderFillTransaction

    trade_Id_Details2 <- trade_Id_Details$tradeOpened
    trade_Id_Details_tibble <-
      tibble(Price = as.numeric(trade_Id_Details2$price),
             tradeID = as.character(trade_Id_Details2$tradeID),
             units = as.numeric(trade_Id_Details2$units)
      )

    return(trade_Id_Details_tibble)

  }

#' extract_put_request_return
#'
#' @param http_return_var
#'
#' @return
#' @export
#'
#' @examples
extract_put_request_return <-
  function(http_return_var = http_return) {

    returned_response <- jsonlite::fromJSON( jsonlite::prettify(http_return))
    trade_Id_Details <-
      returned_response$orderFillTransaction

    trade_Id_Details2 <- trade_Id_Details$tradeOpened
    trade_Id_Details_tibble <-
      tibble(Price = as.numeric(trade_Id_Details2$price),
             tradeID = as.character(trade_Id_Details2$tradeID),
             units = as.numeric(trade_Id_Details2$units)
      )

    return(trade_Id_Details_tibble)

  }

#' Title
#'
#' @param tradeID
#'
#' @return
#' @export
#'
#' @examples
oanda_close_trade_ID <-
  function(tradeID = "4337",
           units = 1,
           account = "equity_long",
           volume = NULL) {

    headers = c(
      `Content-Type` = 'application/json',
      `Authorization` = get_oanda_from_sys()
    )

    params = list(
      `units` = glue::glue("{units}")
    )

    json_body <- rjson::toJSON(params)

    account_number <- get_oanda_account_number(account_name = account)

    returned_response <-
      httr::PUT(url = glue::glue('https://api-fxtrade.oanda.com/v3/accounts/{account_number}/trades/{tradeID}/close'),
               httr::add_headers(.headers=headers),
               # body = json_body,
               encode = "raw")

    return(returned_response)

  }

