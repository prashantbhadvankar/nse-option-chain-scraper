# scrape_nse.R — NSE Option Chain (no Selenium) → tidy table → CSV/XLSX

library(httr)
library(jsonlite)
library(dplyr)
library(tidyr)
library(readr)
library(writexl)

dir.create("output", showWarnings = FALSE)
log_msg <- function(...) cat(format(Sys.time(), "[%Y-%m-%d %H:%M:%S]"), ..., "\n")

ua <- "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

retry <- function(fun, tries = 4, wait = 2) {
  last <- NULL
  for (i in seq_len(tries)) {
    last <- try(fun(), silent = TRUE)
    if (!inherits(last, "try-error")) return(last)
    if (i < tries) Sys.sleep(wait * i)
  }
  stop(last)
}

# ---------- 1) Warm cookies (httr handle) ----------
h <- handle("https://www.nseindia.com")

log_msg("Warming cookies on homepage")
retry(function() {
  GET(
    url    = "https://www.nseindia.com/",
    handle = h,
    user_agent(ua),
    add_headers(Accept = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
  )
}, tries = 5, wait = 1)

# ---------- 2) Call Option Chain API ----------
sym <- "NIFTY"
api_url <- "https://www.nseindia.com/api/option-chain-indices"

log_msg("Calling API for", sym)
resp <- retry(function() {
  GET(
    url    = api_url,
    handle = h,
    query  = list(symbol = sym),
    user_agent(ua),
    add_headers(
      Accept            = "application/json, text/plain, */*",
      `Accept-Language` = "en-US,en;q=0.9",
      Referer           = paste0("https://www.nseindia.com/get-quotes/derivatives?symbol=", sym),
      Connection        = "keep-alive"
    )
  )
}, tries = 5, wait = 2)

stop_for_status(resp)
txt <- content(resp, as = "text", encoding = "UTF-8")
if (!nzchar(txt)) stop("Empty API response")

writeLines(txt, file.path("output", paste0(sym, "_OptionChain_raw.json")))

j <- fromJSON(txt, simplifyVector = TRUE)
if (is.null(j$records) || is.null(j$records$data)) stop("No records$data in API payload")

raw_df <- as_tibble(j$records$data)
if (!nrow(raw_df)) stop("records$data was empty")

# ---------- 3) Flatten CE and PE cleanly ----------
# Keep symbol/keys
keys <- raw_df %>%
  transmute(symbol = if ("symbol" %in% names(raw_df)) symbol else sym,
            expiryDate, strikePrice = suppressWarnings(as.numeric(strikePrice)))

# CE flatten
ce <- raw_df %>%
  select(expiryDate, strikePrice, CE) %>%
  unnest_wider(CE, names_sep = "_", simplify = TRUE) %>%
  mutate(
    strikePrice = suppressWarnings(as.numeric(strikePrice)),
    underlying  = coalesce(CE_underlyingValue, CE_underlyingvalue),
    CE_OI       = suppressWarnings(as.numeric(CE_openInterest)),
    CE_CHGOI    = suppressWarnings(as.numeric(CE_changeinOpenInterest)),
    CE_IV       = suppressWarnings(as.numeric(CE_impliedVolatility)),
    CE_LTP      = suppressWarnings(as.numeric(CE_lastPrice)),
    CE_BIDQTY   = suppressWarnings(as.numeric(CE_bidQty)),
    CE_BID      = suppressWarnings(as.numeric(coalesce(CE_bidprice, CE_bidPrice))),
    CE_ASK      = suppressWarnings(as.numeric(coalesce(CE_askPrice, CE_askprice))),
    CE_ASKQTY   = suppressWarnings(as.numeric(CE_askQty))
  ) %>%
  select(expiryDate, strikePrice, underlying, CE_OI:CE_ASKQTY)

# PE flatten
pe <- raw_df %>%
  select(expiryDate, strikePrice, PE) %>%
  unnest_wider(PE, names_sep = "_", simplify = TRUE) %>%
  mutate(
    strikePrice = suppressWarnings(as.numeric(strikePrice)),
    PE_OI       = suppressWarnings(as.numeric(PE_openInterest)),
    PE_CHGOI    = suppressWarnings(as.numeric(PE_changeinOpenInterest)),
    PE_IV       = suppressWarnings(as.numeric(PE_impliedVolatility)),
    PE_LTP      = suppressWarnings(as.numeric(PE_lastPrice)),
    PE_BIDQTY   = suppressWarnings(as.numeric(PE_bidQty)),
    PE_BID      = suppressWarnings(as.numeric(coalesce(PE_bidprice, PE_bidPrice))),
    PE_ASK      = suppressWarnings(as.numeric(coalesce(PE_askPrice, PE_askprice))),
    PE_ASKQTY   = suppressWarnings(as.numeric(PE_askQty))
  ) %>%
  select(expiryDate, strikePrice, PE_OI:PE_ASKQTY)

# Join CE & PE on keys
option_chain <- keys %>%
  left_join(ce, by = c("expiryDate", "strikePrice")) %>%
  left_join(pe, by = c("expiryDate", "strikePrice")) %>%
  arrange(expiryDate, strikePrice) %>%
  # forward-fill underlying (top-down)
  tidyr::fill(underlying, .direction = "down")

# Diagnostics
nz <- vapply(option_chain, function(col) sum(!is.na(col)), integer(1))
log_msg("Non-NA counts → ", paste(names(nz), nz, sep = ":", collapse = ", "))
log_msg("Rows →", nrow(option_chain))

# ---------- 4) Save CSV + XLSX ----------
ts <- format(Sys.time(), "%Y-%m-%d_%H-%M-%S", tz = "UTC")
csv_path  <- file.path("output", paste0(sym, "_OptionChain_", ts, "_UTC.csv"))
xlsx_path <- file.path("output", paste0(sym, "_OptionChain_", ts, "_UTC.xlsx"))

write_csv(option_chain, csv_path)
writexl::write_xlsx(list(OptionChain = option_chain), xlsx_path)

log_msg("Saved:", csv_path)
log_msg("Saved:", xlsx_path)
