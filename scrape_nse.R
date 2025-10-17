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

save_debug <- function(name, rem = NULL, content = NULL) {
  if (!is.null(rem)) try(rem$screenshot(file = file.path("debug", paste0(name, "_s.png"))), silent = TRUE)
  if (!is.null(content)) writeLines(content, file.path("debug", paste0(name, ".txt")))
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

# column-safe numeric coalesce (works even if some columns don't exist)
coalesce_num <- function(df, cols) {
  sel <- dplyr::select(df, dplyr::any_of(cols))
  if (ncol(sel) == 0) return(rep(NA_real_, nrow(df)))
  out <- sel[[1]]
  if (ncol(sel) >= 2) for (i in 2:ncol(sel)) out <- dplyr::coalesce(out, sel[[i]])
  suppressWarnings(as.numeric(out))
}

# ------------------------ helper: call API w/ fallback re-mint ----------------
fetch_option_chain <- function(remDr, symbol, ua) {
  api_url <- "https://www.nseindia.com/api/option-chain-indices"

  get_cookie_header <- function() {
    cks <- remDr$getAllCookies()
    if (length(cks) == 0) return("")
    paste(vapply(cks, function(x) paste0(x$name, "=", x$value), ""), collapse="; ")
  }

  call_api <- function() {
    h <- add_headers(
      "User-Agent"      = ua,
      "Accept"          = "application/json, text/plain, */*",
      "Accept-Language" = "en-US,en;q=0.9",
      "Referer"         = paste0("https://www.nseindia.com/get-quotes/derivatives?symbol=", symbol),
      "Connection"      = "keep-alive",
      "Cookie"          = get_cookie_header()
    )
    resp <- GET(api_url, query = list(symbol = symbol), h, timeout(40))
    list(resp = resp, body = content(resp, as = "text", encoding = "UTF-8"))
  }

  # 1st attempt
  out <- call_api()
  bad <- http_error(out$resp) || !nzchar(out$body) ||
         grepl("Access Denied|<HTML", out$body, ignore.case = TRUE)

  if (bad) {
    log_msg("API looked bad; re-minting cookies & retrying...")
    # re-warm cookies
    retry(remDr$navigate("https://www.nseindia.com"), tries = 3, wait = 2)
    Sys.sleep(2)
    retry(remDr$navigate(paste0("https://www.nseindia.com/get-quotes/derivatives?symbol=", symbol)), tries = 3, wait = 2)
    Sys.sleep(2)
    # 2nd attempt
    out <- call_api()
  }
  out
}

# ------------------------ 1) open Selenium & mint cookies ---------------------
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

symbols <- c("NIFTY")   # add "BANKNIFTY" here if you want both

for (symbol in symbols) {
  log_msg("Seeding cookies for", symbol)
  retry(remDr$navigate("https://www.nseindia.com"), tries = 4, wait = 2); Sys.sleep(2)
  retry(remDr$navigate(paste0("https://www.nseindia.com/get-quotes/derivatives?symbol=", symbol)), tries = 4, wait = 2); Sys.sleep(2)

  cks <- remDr$getAllCookies()
  cookie_header <- if (length(cks)) paste(vapply(cks, function(x) paste0(x$name, "=", x$value), ""), collapse = "; ") else ""
  log_msg("Cookie header length:", nchar(cookie_header))

  # ------------------------ 2) call API (with fallback) ----------------------
  api <- fetch_option_chain(remDr, symbol, ua)
  resp <- api$resp; txt <- api$body

  if (http_error(resp) || !nzchar(txt) || grepl("Access Denied|<HTML", txt, ignore.case = TRUE)) {
    save_debug(paste0("api_failure_", symbol), rem = remDr, content = txt)
    stop("API error/denied for ", symbol)
  }

  raw_json_path <- file.path("output", paste0(symbol, "_OptionChain_raw.json"))
  writeLines(txt, raw_json_path)

  j <- fromJSON(txt, simplifyVector = TRUE)
  if (is.null(j$records) || is.null(j$records$data)) {
    save_debug(paste0("missing_records_", symbol), rem = remDr, content = txt)
    stop("No records$data in payload for ", symbol)
  }
  raw_df <- tibble::as_tibble(j$records$data)
  if (!nrow(raw_df)) stop("records$data empty for ", symbol)

  # ------------------------ 3) flatten CE/PE safely --------------------------
  keys <- raw_df %>%
    transmute(
      symbol      = if ("symbol" %in% names(raw_df)) symbol else !!symbol,
      expiryDate  = .data$expiryDate,
      strikePrice = suppressWarnings(as.numeric(.data$strikePrice))
    )

  ce_wide <- raw_df %>% select(expiryDate, strikePrice, CE) %>% unnest_wider(CE, names_sep = "_", simplify = TRUE)
  pe_wide <- raw_df %>% select(expiryDate, strikePrice, PE) %>% unnest_wider(PE, names_sep = "_", simplify = TRUE)

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

  option_chain <- keys %>%
    left_join(ce_tbl, by = c("expiryDate", "strikePrice")) %>%
    left_join(pe_tbl, by = c("expiryDate", "strikePrice")) %>%
    arrange(expiryDate, strikePrice) %>%
    tidyr::fill(underlying, .direction = "down")

  # simple sanity check to fail fast on empty tables
  if ((sum(!is.na(option_chain$CE_LTP)) + sum(!is.na(option_chain$PE_LTP))) == 0) {
    save_debug(paste0("empty_values_", symbol), content = txt)
    stop("Sanity check failed: empty CE/PE LTP for ", symbol)
  }

  nz <- vapply(option_chain, function(x) sum(!is.na(x)), integer(1))
  log_msg("Non-NA counts(", symbol, "): ", paste(names(nz), nz, sep="=", collapse=", "))
  log_msg("Rows(", symbol, "): ", nrow(option_chain))

  # ------------------------ 4) save ---------------------------
  ts <- format(Sys.time(), "%Y-%m-%d_%H-%M-%S", tz = "UTC")
  write_csv(option_chain, file.path("output", paste0(symbol, "_OptionChain_", ts, "_UTC.csv")))
  writexl::write_xlsx(list(OptionChain = option_chain),
                      file.path("output", paste0(symbol, "_OptionChain_", ts, "_UTC.xlsx")))
}

# ------------------------ teardown -----------------------
try(remDr$close(), silent = TRUE)
