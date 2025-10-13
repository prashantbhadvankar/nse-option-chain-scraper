# scrape_nse.R (resilient)
library(RSelenium)
library(rvest)
library(dplyr)
library(tidyr)
library(readr)
library(writexl)
library(stringr)

dir.create("output",  showWarnings = FALSE)
dir.create("debug",   showWarnings = FALSE)

log_msg <- function(...) {
  cat(format(Sys.time(), "[%Y-%m-%d %H:%M:%S]"), ..., "\n")
}

# ---- Connect to Selenium (service: localhost:4444) ----
ua <- "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

remDr <- remoteDriver(
  remoteServerAddr = "localhost",
  port = 4444L,
  browserName = "firefox",
  extraCapabilities = list(
    "moz:firefoxOptions" = list(
      prefs = list("general.useragent.override" = ua),
      args = list("--headless")
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

retry(remDr$open(), tries = 5, wait = 2)

# Helpful timeouts
remDr$setTimeout(type = "page load", milliseconds = 90000)
remDr$setTimeout(type = "implicit",  milliseconds = 0L)
remDr$setTimeout(type = "script",    milliseconds = 30000)

# ---- Helpers ----
wait_for <- function(css, timeout = 60) {
  t0 <- Sys.time()
  repeat {
    el <- try(remDr$findElement("css selector", css), silent = TRUE)
    if (!inherits(el, "try-error")) return(el)
    if (as.numeric(difftime(Sys.time(), t0, units = "secs")) > timeout)
      stop(paste("Timeout waiting for:", css))
    Sys.sleep(1)
  }
}

save_debug <- function(name) {
  # screenshot
  try({
    f <- file.path("debug", paste0(name, "_screenshot.png"))
    remDr$screenshot(file = f)
    log_msg("Saved screenshot:", f)
  }, silent = TRUE)
  # page source
  try({
    ps  <- remDr$getPageSource()[[1]]
    f2  <- file.path("debug", paste0(name, "_page.html"))
    writeLines(ps, f2, useBytes = TRUE)
    log_msg("Saved page source:", f2, "(len:", nchar(ps), ")")
  }, silent = TRUE)
}

# ---- Navigate ----
log_msg("Opening NSE home (to seed cookies)")
retry(remDr$navigate("https://www.nseindia.com"), tries = 3, wait = 3)
Sys.sleep(3)

log_msg("Opening NIFTY derivatives page")
retry(remDr$navigate("https://www.nseindia.com/get-quotes/derivatives?symbol=NIFTY"), tries = 3, wait = 3)
Sys.sleep(5)
save_debug("after_navigate")

# Click the Option Chain tab (your id)
log_msg("Clicking Option Chain tab")
tab <- retry(remDr$findElement("id", "equity-derivative-cntrctinfo-optionchain-main"))
retry(tab$clickElement())
Sys.sleep(4)

# Wait for the Option Chain container
log_msg("Waiting for option chain table container")
oc_div <- try(wait_for("#equity-derivative-optionChainTable", timeout = 60), silent = TRUE)
if (inherits(oc_div, "try-error")) {
  log_msg("Option chain container not found, saving debug and failing.")
  save_debug("no_option_chain")
  stop("Option Chain container not found")
}

# Extract only the container's HTML first
oc_html <- oc_div$getElementAttribute("outerHTML")[[1]]
doc <- read_html(oc_html)

tbl_nodes <- html_elements(doc, "table")
if (length(tbl_nodes) == 0) {
  # Fallback to full page source
  log_msg("No tables in container HTML; falling back to full page source")
  save_debug("no_tables_in_container")
  page_source <- remDr$getPageSource()[[1]]
  full_doc    <- read_html(page_source)
  tbl_nodes   <- html_elements(full_doc, "div#equity-derivative-optionChainTable table")
}

if (length(tbl_nodes) == 0) {
  save_debug("tables_still_missing")
  stop("No tables found under #equity-derivative-optionChainTable")
}

tables <- lapply(tbl_nodes, function(x) html_table(x, fill = TRUE, header = TRUE))
option_chain_raw <- suppressWarnings(bind_rows(tables))

# Clean
option_chain <- option_chain_raw %>%
  select(where(~ any(nzchar(trimws(as.character(.)))))) %>%
  rename_with(~ gsub("\\s+", "_", trimws(.x))) %>%
  mutate(across(everything(), ~ ifelse(is.character(.), trimws(.), .)))

numify <- function(x) {
  if (!is.character(x)) return(x)
  y <- gsub(",", "", x, fixed = TRUE)
  y <- gsub("—|–|-", "", y)
  suppressWarnings(as.numeric(y))
}
maybe_numeric <- c("OI","CHNG_IN_OI","VOLUME","IV","LTP","CHNG","BID_QTY","BID","ASK","ASK_QTY","STRIKE")
for (nm in intersect(maybe_numeric, names(option_chain))) {
  option_chain[[nm]] <- numify(option_chain[[nm]])
}

# Save
ts <- format(Sys.time(), "%Y-%m-%d_%H-%M-%S", tz = "UTC")
csv_path  <- file.path("output", paste0("NIFTY_OptionChain_", ts, "_UTC.csv"))
xlsx_path <- file.path("output", paste0("NIFTY_OptionChain_", ts, "_UTC.xlsx"))

write_csv(option_chain, csv_path)
writexl::write_xlsx(list(NIFTY_OptionChain = option_chain), xlsx_path)

log_msg("Wrote:", csv_path)
log_msg("Wrote:", xlsx_path)

try(remDr$close(), silent = TRUE)
