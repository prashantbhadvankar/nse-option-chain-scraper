# scrape_nse.R — NSE Option Chain via API (no Selenium; robust cookies + parsing)

library(httr)
library(jsonlite)
library(dplyr)
library(readr)
library(writexl)

dir.create("output", showWarnings = FALSE)
log_msg <- function(...) cat(format(Sys.time(), "[%Y-%m-%d %H:%M:%S]"), ..., "\n")

ua <- "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# ---------- helpers ----------
retry <- function(fun, tries = 4, wait = 2) {
  for (i in seq_len(tries)) {
    res <- try(fun(), silent = TRUE)
    if (!inherits(res, "try-error")) return(res)
    if (i < tries) Sys.sleep(wait * i)
  }
  stop(res)
}

get_num2 <- function(lst, fields, n) {
  out <- vapply(lst, function(x) {
    if (is.null(x) || !is.list(x) || length(x) == 0) return(NA_real_)
    for (f in fields) if (!is.null(x[[f]])) return(suppressWarnings(as.numeric(x[[f]])))
    NA_real_
  }, numeric(1))
  if (length(out) != n) out <- c(out, rep_len(NA_real_, n - length(out)))
  out
}

# ---------- 1) warm cookies with an httr handle ----------
h <- handle("https://www.nseindia.com")

log_msg("Warming cookies on homepage")
retry(function() GET(handle = h, path = "/", add_headers(
  "User-Agent" = ua,
  "Accept"     = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
))), tries = 5, wait = 1)

# ---------- 2) call the Option Chain API through the same handle ----------
api_path <- "/api/option-chain-indices"
sym <- "NIFTY"

log_msg("Calling API for", sym)
resp <- retry(function() GET(handle = h, path = api_path, query = list(symbol = sym),
                             add_headers(
                               "User-Agent"      = ua,
                               "Accept"          = "application/json, text/plain, */*",
                               "Accept-Language" = "en-US,en;q=0.9",
                               "Referer"         = paste0("https://www.nseindia.com/get-quotes/derivatives?symbol=", sym),
                               "Connection"      = "keep-alive"
                             ))), tries = 5, wait = 2)

stop_for_status(resp)
txt <- content(resp, as = "text", encoding = "UTF-8")
if (!nzchar(txt)) stop("Empty API response")

# Save raw JSON for quick inspection
writeLines(txt, file.path("output", paste0(sym, "_OptionChain_raw.json")))

j <- fromJSON(txt, simplifyVector = TRUE)
if (is.null(j$records) || is.null(j$records$data)) stop("No records$data in API payload")

raw_df <- as_tibble(j$records$data)
if (!nrow(raw_df)) stop("records$data was empty")

# ---------- 3) build tidy table (length-safe + multi-key) ----------
n <- nrow(raw_df)

# Ensure CE/PE exist and have length n
if (!"CE" %in% names(raw_df)) raw_df$CE <- replicate(n, NULL, simplify = FALSE)
if (!"PE" %in% names(raw_df)) raw_df$PE <- replicate(n, NULL, simplify = FALSE)
CE <- raw_df$CE; PE <- raw_df$PE
if (length(CE) != n) CE <- c(CE, replicate(n - length(CE), NULL, simplify = FALSE))
if (length(PE) != n) PE <- c(PE, replicate(n - length(PE), NULL, simplify = FALSE))

symbol_vec     <- if ("symbol" %in% names(raw_df)) as.character(raw_df$symbol) else rep(sym, n)
expiry_vec     <- if ("expiryDate" %in% names(raw_df)) raw_df$expiryDate else rep(NA_character_, n)
strike_vec     <- suppressWarnings(as.numeric(raw_df$strikePrice))[seq_len(n)]
underlying_vec <- get_num2(CE, c("underlyingValue", "underlyingvalue"), n)

# forward-fill underlying
if (n > 1) for (i in 2:n) if (is.na(underlying_vec[i])) underlying_vec[i] <- underlying_vec[i-1]

option_chain <- tibble(
  symbol      = symbol_vec,
  expiryDate  = expiry_vec,
  strikePrice = strike_vec,
  underlying  = underlying_vec,

  CE_OI       = get_num2(CE, c("openInterest", "openinterest"), n),
  CE_CHGOI    = get_num2(CE, c("changeinOpenInterest", "changeInOpenInterest", "chgOI"), n),
  CE_IV       = get_num2(CE, c("impliedVolatility", "iv"), n),
  CE_LTP      = get_num2(CE, c("lastPrice", "ltp"), n),
  CE_BIDQTY   = get_num2(CE, c("bidQty", "bidqty"), n),
  CE_BID      = get_num2(CE, c("bidprice", "bidPrice"), n),
  CE_ASK      = get_num2(CE, c("askPrice", "askprice"), n),
  CE_ASKQTY   = get_num2(CE, c("askQty", "askqty"), n),

  PE_OI       = get_num2(PE, c("openInterest", "openinterest"), n),
  PE_CHGOI    = get_num2(PE, c("changeinOpenInterest", "changeInOpenInterest", "chgOI"), n),
  PE_IV       = get_num2(PE, c("impliedVolatility", "iv"), n),
  PE_LTP      = get_num2(PE, c("lastPrice", "ltp"), n),
  PE_BIDQTY   = get_num2(PE, c("bidQty", "bidqty"), n),
  PE_BID      = get_num2(PE, c("bidprice", "bidPrice"), n),
  PE_ASK      = get_num2(PE, c("askPrice", "askprice"), n),
  PE_ASKQTY   = get_num2(PE, c("askQty", "askqty"), n)
) %>%
  arrange(expiryDate, strikePrice)

# diagnostics
nz <- vapply(option_chain, function(col) sum(!is.na(col)), integer(1))
log_msg("Non-NA counts -> ", paste(names(nz), nz, sep=":", collapse=", "))
log_msg("Rows in option_chain:", nrow(option_chain))

# ---------- 4) write files ----------
ts <- format(Sys.time(), "%Y-%m-%d_%H-%M-%S", tz = "UTC")
csv_path  <- file.path("output", paste0(sym, "_OptionChain_", ts, "_UTC.csv"))
xlsx_path <- file.path("output", paste0(sym, "_OptionChain_", ts, "_UTC.xlsx"))

write_csv(option_chain, csv_path)
writexl::write_xlsx(list(OptionChain = option_chain), xlsx_path)

log_msg("Saved:", csv_path)
log_msg("Saved:", xlsx_path)
