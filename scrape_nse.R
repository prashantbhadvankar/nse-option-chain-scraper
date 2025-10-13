library(rvest)
library(dplyr)
library(RSelenium)
library(readr)
library(tidyr)
library(readxl)
library(writexl)

# Start RSelenium
rD <- rsDriver(browser = "firefox", port = 4444L, check = FALSE)
remDr <- rD$client

# Navigate to NSE NIFTY derivatives page
remDr$navigate("https://www.nseindia.com/get-quotes/derivatives?symbol=NIFTY")
Sys.sleep(5)

# Click the "Option Chain" tab
option_chain_tab <- remDr$findElement(using = "id", 
                                      value = "equity-derivative-cntrctinfo-optionchain-main")
option_chain_tab$clickElement()
Sys.sleep(5)

# (optional) Now you can continue scraping the table here
page_source <- remDr$getPageSource()[[1]]
webpage <- read_html(page_source)

# Example: extract table
option_table <- webpage %>%
  html_element("div#equity-derivative-optionChainTable") %>%
  html_table(fill = TRUE)

# View or save
View(option_table)
write_xlsx(option_table, "NIFTY_OptionChain.xlsx")

remDr$close() 
rD$server$stop()
