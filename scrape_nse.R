# scrape_nse.R — NSE Option Chain via API (stable for GitHub Actions)

# pkgs
library(RSelenium)
library(httr)
library(jsonlite)
library(dplyr)
library(tidyr)
library(readr)
library(writexl)

# ---------------------------------------------------------------------
# setup
dir.create("output", showWarnings = FALSE)
dir.create("debug",  showWarnings = FALSE)

log_msg <- function(...) cat(format(Sys.time(), "[%Y-%m-%d %H:%M:%S]"), ..., "\n")

# Desktop UA helps with NSE/CDN
ua <- "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# Connect to Selenium running as a service in GitHub Actions (localhost:4444)
remDr <- remoteDriver(
  remoteServerAddr = "localhost",
  port = 4444L,
  browserName = "firefox",
  extraCapabilities = list(
    "moz:firefoxOptions" = list(
      prefs = list("general.useragent.override" = ua),
      args  = list("--headless")
    )
  )
)

retry <- function(expr, tries = 3, wait = 3) {
  for (i in seq_len(tries)) {
    out <- tryCatch(force(expr), error = function(e) e)
    if (!inherits(out, "error")) return(out)
    if (i < tries) Sys.sleep(wait)
  }
  stop(out)
}

save_debug <- function(name, remDr_obj = remDr) {
  # screenshot
  try({
    f <- file.path("debug", paste0(name, "_screenshot.png"))
    remDr_obj$screenshot(file = f)
    log_msg("Saved screenshot:", f)
  }, silent = TRUE)
  # page source
  try({
    ps <- remDr_obj$getPageSource()[[1]]
    f2 <- file.path("debug", paste0(name, "_page.html"))
    writeLines(ps, f2, useBytes = TRUE)
    log_msg("Saved page source:", f2, "(len:", nchar(ps), ")")
  }, silent = TRUE)
}

# ---------------------------------------------------------------------
# 1) open NSE pages to seed cookies
log_msg("Opening Selenium session")
retry(remDr$open(), tries = 5, wait = 2)
remDr$setTimeout("page load", 90000L)

log_msg("Seed cookies: open NSE home")
retry(remDr$navigate("https://www.nseindia.com"), tries = 3, wait = 3)
Sys.sleep(3)

log_msg("Visit NIFTY derivatives page (referer for API)")
retry(remDr$navigate("https://www.nseindia.com/get-quotes/derivatives?symbol=NIFTY"), tries = 3, wait = 3)
Sys.sleep(3)

# Build Cookie header from Selenium session
cks <- remDr$getAllCookies()
if (length(cks) == 0) { Sys.sleep(2); cks <- remDr$getAllCookies() }
cookie_header <- if (length(cks)) {
  paste(vapply(cks, function(x) paste0(x$name, "=", x$value), ""), collapse = "; ")
} else {
  ""  # API often still works with just UA+Referer
}
log_msg("Cookie header length:", nchar(cookie_header))

# ---------------------------------------------------------------------
# 2) call official NSE API (more reliable than clicking tabs in headless)
api_url <- "https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY"
log_msg("Calling API:", api_url)

h <- add_headers(
  "User-Agent"      = ua,
  "Accept"          = "application/json, text/plain, */*",
  "Accept-Language" = "en-US,en;q=0.9",
  "Referer"         = "https://www.nseindia.com/get-quotes/derivatives?symbol=NIFTY",
  "Connection"      = "keep-alive",
  "Cookie"          = cookie_header
)

resp <- try(GET(api_url, h, timeout(40)), silent = TRUE)
if (inherits(resp, "try-error") || http_error(resp)) {
  save_debug("api_call_failed")
  stop(paste("API call failed with:", if (inherits(resp, "try-error")) resp else status_code(resp)))
}

txt <- content(resp, as = "text", encoding = "UTF-8")
if (!nzchar(txt)) {
  save_debug("api_empty_response")
  stop("Empty API response")
}

# Save raw JSON for verification
writeLines(txt, file.path("output", "NIFTY_OptionChain_raw.json"))

j <- fromJSON(txt)

# ---------------------------------------------------------------------
#



