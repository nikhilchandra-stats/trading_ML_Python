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
