# scrape_nse.R — NSE Option Chain via API (robust: multi-key + length-safe)

library(RSelenium)
library(httr)
library(jsonlite)
library(dplyr)
library(tidyr)
library(readr)
library(writexl)

# --- setup --------------------------------------------------------------------
dir.create("output", showWarnings = FALSE)
dir.create("debug",  showWarnings = FALSE)
log_msg <- function(...) cat(format(Sys.time(), "[%Y-%m-%d %H:%M:%S]"), ..., "\n")

ua <- "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

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

# --- 1) seed cookies via Selenium --------------------------------------------
log_msg("Opening Selenium session")
retry(remDr$open(), tries = 5, wait = 2)
remDr$setTimeout("page load", 90000L)

log_msg("Seed cookies: open NSE home")
retry(remDr$navigate("https://www.nseindia.com"), tries = 3, wait = 3)
Sys.sleep(3)

log_msg("Visit NIFTY derivatives page (referer for API)")
retry(remDr$navigate("https://www.nseindia.com/get-quotes/derivatives?symbol=NIFTY"), tries = 3, wait = 3)
Sys.sleep(3)

cks <- remDr$getAllCookies()
if (length(cks) == 0) { Sys.sleep(2); cks <- remDr$getAllCookies() }
cookie_header <- if (length(cks)) {
  paste(vapply(cks, function(x) paste0(x$name, "=", x$value), ""), collapse = "; ")
} else ""
log_msg("Cookie header length:", nchar(cookie_header))

# --- 2) call official API -----------------------------------------------------
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

j <- try(jsonlite::fromJSON(txt), silent = TRUE)
if (inherits(j, "try-error")) { save_debug("json_parse_error"); stop("Failed to parse JSON from NSE.") }
if (is.null(j$records) || is.null(j$records$data)) {
  save_debug("api_missing_records_data"); stop("NSE API returned no 'records$data'.")
}

raw_df <- dplyr::as_tibble(j$records$data)
if (nrow(raw_df) == 0) { save_debug("api_empty_data"); stop("NSE API returned empty 'records$data'.") }

# --- 3) build tidy table (length-safe + multi-key) ----------------------------
n <- nrow(raw_df)

# Ensure CE/PE exist and have length n
if (!"CE" %in% names(raw_df)) raw_df$CE <- replicate(n, NULL, simplify = FALSE)
if (!"PE" %in% names(raw_df)) raw_df$PE <- replicate(n, NULL, simplify = FALSE)
CE <- raw_df$CE; PE <- raw_df$PE
if (length(CE) != n) CE <- c(CE, replicate(n - length(CE), NULL, simplify = FALSE))
if (length(PE) != n) PE <- c(PE, replicate(n - length(PE), NULL, simplify = FALSE))

# Extractor that tries multiple possible field names (handles casing variants)
get_num2 <- function(lst, fields) {
  out <- vapply(lst, function(x) {
    if (is.null(x) || !is.list(x) || length(x) == 0) return(NA_real_)
    for (f in fields) if (!is.null(x[[f]])) return(suppressWarnings(as.numeric(x[[f]])))
    NA_real_
  }, numeric(1))
  if (length(out) != n) out <- c(out, rep_len(NA_real_, n - length(out)))
  out
}

symbol_vec     <- if ("symbol" %in% names(raw_df)) as.character(raw_df$symbol) else rep("NIFTY", n)
expiry_vec     <- if ("expiryDate" %in% names(raw_df)) raw_df$expiryDate else rep(NA_character_, n)
strike_vec     <- suppressWarnings(as.numeric(raw_df$strikePrice))[seq_len(n)]
underlying_vec <- get_num2(CE, c("underlyingValue", "underlyingvalue"))

# forward-fill underlying
if (n > 1) for (i in 2:n) if (is.na(underlying_vec[i])) underlying_vec[i] <- underlying_vec[i-1]

option_chain <- tibble::tibble(
  symbol      = symbol_vec,
  expiryDate  = expiry_vec,
  strikePrice = strike_vec,
  underlying  = underlying_vec,

  CE_OI       = get_num2(CE, c("openInterest", "openinterest")),
  CE_CHGOI    = get_num2(CE, c("changeinOpenInterest", "changeInOpenInterest", "chgOI")),
  CE_IV       = get_num2(CE, c("impliedVolatility", "iv")),
  CE_LTP      = get_num2(CE, c("lastPrice", "ltp")),
  CE_BIDQTY   = get_num2(CE, c("bidQty", "bidqty")),
  CE_BID      = get_num2(CE, c("bidprice", "bidPrice")),
  CE_ASK      = get_num2(CE, c("askPrice", "askprice")),
  CE_ASKQTY   = get_num2(CE, c("askQty", "askqty")),

  PE_OI       = get_num2(PE, c("openInterest", "openinterest")),
  PE_CHGOI    = get_num2(PE, c("changeinOpenInterest", "changeInOpenInterest", "chgOI")),
  PE_IV       = get_num2(PE, c("impliedVolatility", "iv")),
  PE_LTP      = get_num2(PE, c("lastPrice", "ltp")),
  PE_BIDQTY   = get_num2(PE, c("bidQty", "bidqty")),
  PE_BID      = get_num2(PE, c("bidprice", "bidPrice")),
  PE_ASK      = get_num2(PE, c("askPrice", "askprice")),
  PE_ASKQTY   = get_num2(PE, c("askQty", "askqty"))
) %>% dplyr::arrange(expiryDate, strikePrice)

if (!nrow(option_chain)) { save_debug("option_chain_empty_after_parse"); stop("Built option_chain is empty.") }

# quick diagnostics (will show in Actions logs)
nz <- vapply(option_chain, function(col) sum(!is.na(col)), integer(1))
log_msg("Non-NA counts -> ", paste(names(nz), nz, sep=":", collapse=", "))
log_msg("Rows in option_chain:", nrow(option_chain))

# --- 4) save outputs ----------------------------------------------------------
ts <- format(Sys.time(), "%Y-%m-%d_%H-%M-%S", tz = "UTC")
csv_path  <- file.path("output", paste0("NIFTY_OptionChain_", ts, "_UTC.csv"))
xlsx_path <- file.path("output", paste0("NIFTY_OptionChain_", ts, "_UTC.xlsx"))

readr::write_csv(option_chain, csv_path)
writexl::write_xlsx(list(NIFTY_OptionChain = option_chain), xlsx_path)

log_msg("Saved:", csv_path)
log_msg("Saved:", xlsx_path)

# --- teardown -----------------------------------------------------------------
try(remDr$close(), silent = TRUE)
