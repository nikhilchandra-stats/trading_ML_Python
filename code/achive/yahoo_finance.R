library(tidyverse)
library(RSelenium)
library(rvest)

port_value = 3451L

driver <- rsDriver(browser = c("firefox"),
                   port = port_value, chromever = "108.0.5359.71")

remote_driver <- driver[["client"]]

remote_driver$open()

driver$server$output()


navigating_url <-
  glue::glue("https://au.finance.yahoo.com/quote/CBA.AX/history/?period1=1585833504&period2=1743599894") %>%
  as.character()

remote_driver$navigate(url = navigating_url)

page_sourced <- remote_driver$getPageSource()
html_read <- page_sourced[[1]] %>%
  rvest::read_html()
asset_data <- html_read %>% rvest::html_table()

remote_driver$close()
driver$server$stop()
gc()
