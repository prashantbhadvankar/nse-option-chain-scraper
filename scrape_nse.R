# scrape_nse.R — NSE Option Chain via Selenium cookies -> API -> tidy table -> CSV/XLSX

library(RSelenium)
library(httr)
library(jsonlite)
library(dplyr)
library(tidyr)
library(readr)
library(writexl)

# ------------------------ setup ------------------------
dir.create("output", showWarnings = FALSE)
dir.create("debug",  showWarnings = FALSE)
log_msg <- function(...) cat(format(Sys.time(), "[%Y-%m-%d %H:%M:%S]"), ..., "\n")

ua <- "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"

save_debug <- function(name, rem = NULL) {
  if (!is.null(rem)) try(rem$screenshot(file = file.path("debug", paste0(name, "_s.png"))), silent = TRUE)
}

retry <- function(expr, tries = 4, wait = 2) {
  last <- NULL
  for (i in seq_len(tries)) {
    last <- try(force(expr), silent = TRUE)
    if (!inherits(last, "try-error")) return(last)
    if (i < tries) Sys.sleep(wait * i)
  }
  stop(last)
}

# helper: safe numeric coalesce across possibly-missing columns (usable outside verbs)
coalesce_num <- function(df, cols) {
  sel <- dplyr::select(df, dplyr::any_of(cols))  # only existing columns
  if (ncol(sel) == 0) return(rep(NA_real_, nrow(df)))
  out <- sel[[1]]
  if (ncol(sel) >= 2) {
    for (i in 2:ncol(sel)) out <- dplyr::coalesce(out, sel[[i]])
  }
  suppressWarnings(as.numeric(out))
}

# ------------------------ 1) get cookies with Selenium ------------------------
log_msg("Opening Selenium session")
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
retry(remDr$open(), tries = 6, wait = 2)
remDr$setTimeout("page load", 90000L)

symbol <- "NIFTY"

log_msg("Navigate to NSE home (seed Akamai cookies)")
retry(remDr$navigate("https://www.nseindia.com"), tries = 4, wait = 2)
Sys.sleep(3)

log_msg("Navigate to derivatives page for referer")
retry(remDr$navigate(paste0("https://www.nseindia.com/get-quotes/derivatives?symbol=", symbol)), tries = 4, wait = 2)
Sys.sleep(3)

cks <- remDr$getAllCookies()
if (length(cks) == 0) { Sys.sleep(2); cks <- remDr$getAllCookies() }
cookie_header <- if (length(cks)) paste(vapply(cks, function(x) paste0(x$name, "=", x$value), ""), collapse = "; ") else ""
log_msg("Cookie header length:", nchar(cookie_header))

# ------------------------ 2) call official API with cookies -------------------
api_url <- "https://www.nseindia.com/api/option-chain-indices"
log_msg("Calling API:", api_url, "symbol =", symbol)

h <- add_headers(
  "User-Agent"      = ua,
  "Accept"          = "application/json, text/plain, */*",
  "Accept-Language" = "en-US,en;q=0.9",
  "Referer"         = paste0("https://www.nseindia.com/get-quotes/derivatives?symbol=", symbol),
  "Connection"      = "keep-alive",
  "Cookie"          = cookie_header
)

resp <- retry(GET(url = api_url, query = list(symbol = symbol), h, timeout(40)), tries = 4, wait = 2)
if (http_error(resp)) {
  save_debug("api_http_error", remDr)
  stop(paste("API error:", status_code(resp)))
}
txt <- content(resp, as = "text", encoding = "UTF-8")
if (!nzchar(txt) || grepl("<HTML>|Access Denied", txt, ignore.case = TRUE)) {
  save_debug("api_access_denied", remDr)
  stop("API returned Access Denied / empty body")
}

# Save raw JSON
writeLines(txt, file.path("output", paste0(symbol, "_OptionChain_raw.json")))

j <- fromJSON(txt, simplifyVector = TRUE)
if (is.null(j$records) || is.null(j$records$data)) {
  save_debug("api_missing_records", remDr)
  stop("No records$data in payload")
}
raw_df <- tibble::as_tibble(j$records$data)
if (!nrow(raw_df)) stop("records$data empty")

# ------------------------ 3) flatten CE / PE safely ---------------------------
# keys
keys <- raw_df %>%
  transmute(
    symbol     = if ("symbol" %in% names(raw_df)) symbol else !!symbol,
    expiryDate = .data$expiryDate,
    strikePrice = suppressWarnings(as.numeric(.data$strikePrice))
  )

# CE and PE wide expansions
ce_wide <- raw_df %>% select(expiryDate, strikePrice, CE) %>% unnest_wider(CE, names_sep = "_", simplify = TRUE)
pe_wide <- raw_df %>% select(expiryDate, strikePrice, PE) %>% unnest_wider(PE, names_sep = "_", simplify = TRUE)

# CE numeric columns (column-safe coalescing)
ce_tbl <- ce_wide %>%
  mutate(
    strikePrice = suppressWarnings(as.numeric(strikePrice)),
    underlying  = coalesce_num(ce_wide, c("CE_underlyingValue", "CE_underlyingvalue")),
    CE_OI       = suppressWarnings(as.numeric(CE_openInterest)),
    CE_CHGOI    = suppressWarnings(as.numeric(CE_changeinOpenInterest)),
    CE_IV       = suppressWarnings(as.numeric(CE_impliedVolatility)),
    CE_LTP      = suppressWarnings(as.numeric(CE_lastPrice)),
    CE_BIDQTY   = suppressWarnings(as.numeric(CE_bidQty)),
    CE_BID      = coalesce_num(ce_wide, c("CE_bidprice", "CE_bidPrice")),
    CE_ASK      = coalesce_num(ce_wide, c("CE_askPrice", "CE_askprice")),
    CE_ASKQTY   = suppressWarnings(as.numeric(CE_askQty))
  ) %>%
  select(expiryDate, strikePrice, underlying, CE_OI:CE_ASKQTY)

# PE numeric columns
pe_tbl <- pe_wide %>%
  mutate(
    strikePrice = suppressWarnings(as.numeric(strikePrice)),
    PE_OI       = suppressWarnings(as.numeric(PE_openInterest)),
    PE_CHGOI    = suppressWarnings(as.numeric(PE_changeinOpenInterest)),
    PE_IV       = suppressWarnings(as.numeric(PE_impliedVolatility)),
    PE_LTP      = suppressWarnings(as.numeric(PE_lastPrice)),
    PE_BIDQTY   = suppressWarnings(as.numeric(PE_bidQty)),
    PE_BID      = coalesce_num(pe_wide, c("PE_bidprice", "PE_bidPrice")),
    PE_ASK      = coalesce_num(pe_wide, c("PE_askPrice", "PE_askprice")),
    PE_ASKQTY   = suppressWarnings(as.numeric(PE_askQty))
  ) %>%
  select(expiryDate, strikePrice, PE_OI:PE_ASKQTY)

# Join, order, fill underlying down
option_chain <- keys %>%
  left_join(ce_tbl, by = c("expiryDate", "strikePrice")) %>%
  left_join(pe_tbl, by = c("expiryDate", "strikePrice")) %>%
  arrange(expiryDate, strikePrice) %>%
  tidyr::fill(underlying, .direction = "down")

# diagnostics
nz <- vapply(option_chain, function(x) sum(!is.na(x)), integer(1))
log_msg("Non-NA counts:", paste(names(nz), nz, sep = "=", collapse = ", "))
log_msg("Rows:", nrow(option_chain))

# ------------------------ 4) save ---------------------------
ts <- format(Sys.time(), "%Y-%m-%d_%H-%M-%S", tz = "UTC")
csv_path  <- file.path("output", paste0(symbol, "_OptionChain_", ts, "_UTC.csv"))
xlsx_path <- file.path("output", paste0(symbol, "_OptionChain_", ts, "_UTC.xlsx"))

write_csv(option_chain, csv_path)
writexl::write_xlsx(list(OptionChain = option_chain), xlsx_path)

log_msg("Saved:", csv_path)
log_msg("Saved:", xlsx_path)

# ------------------------ teardown -----------------------
try(remDr$close(), silent = TRUE)
