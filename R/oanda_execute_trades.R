oanda_place_order <- function(
  asset = "AUD_JPY",
  volume = 1,
  stopLoss = 0.01,
  takeProfit = 0.01,
  type = "MARKET",
  timeinForce = "FOK",
  price,
  account = get_oanda_account_number(account_name = "primary")

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
      `positionFill` = "DEFAULT"
    ),
    `stopLossOnFill` = list(
      `timeInForce` = glue::glue("GTC"),
      `distance` = glue::glue("{stopLoss}")
    ),
    `takeProfitOnFill` = list(
      `timeInForce` = glue::glue("GTC"),
      `distance` = glue::glue("{takeProfit}")
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
      `positionFill` = "DEFAULT"
    ),
    `stopLossOnFill` = list(
      `timeInForce` = glue::glue("GTC"),
      `distance` = glue::glue("{stopLoss}")
    ),
    `takeProfitOnFill` = list(
      `timeInForce` = glue::glue("GTC"),
      `distance` = glue::glue("{takeProfit}")
    )
  )

}


  json_body <- rjson::toJSON(params)


  httr::POST(url = glue::glue('https://api-fxtrade.oanda.com/v3/accounts/{account}/orders'),
                   httr::add_headers(.headers=headers),
                   body = json_body,
                  encode = "raw")
}

