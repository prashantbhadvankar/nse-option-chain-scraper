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
# --- Click the Option Chain tab (robust) ---
log_msg("Clicking Option Chain tab (robust)")

# 1) Wait for the tab anchor to exist in the DOM
tab_el <- try(wait_for("a#equity-derivative-cntrctinfo-optionchain-main", timeout = 60), silent = TRUE)

# 2) If not found, try alternative selectors
if (inherits(tab_el, "try-error")) {
  log_msg("Primary selector failed; trying fallbacks for Option Chain tab")
  alt_selectors <- c(
    "#equity-derivative-cntrctinfo-optionchain-main",
    "a[href='#equity-derivative-cntrctinfo-optionchain']",
    "a.nav-link[data-bs-toggle='tab'][href*='optionchain']",
    "//a[contains(@href, 'optionchain')]",
    "//a[normalize-space()='Option Chain']"
  )
  for (sel in alt_selectors) {
    tab_el <- try(
      if (startsWith(sel, "//")) remDr$findElement("xpath", sel)
      else remDr$findElement("css selector", sel),
      silent = TRUE
    )
    if (!inherits(tab_el, "try-error")) break
  }
}

# 3) If still not found, try loading the hash URL directly (sometimes activates the tab)
if (inherits(tab_el, "try-error")) {
  log_msg("Tab element not found; navigating directly to hash URL and retrying")
  retry(remDr$navigate("https://www.nseindia.com/get-quotes/derivatives?symbol=NIFTY#equity-derivative-cntrctinfo-optionchain"))
  Sys.sleep(4)
  tab_el <- try(wait_for("#equity-derivative-optionChainTable", timeout = 20), silent = TRUE)
  if (!inherits(tab_el, "try-error")) {
    log_msg("Option Chain content loaded via hash URL")
  } else {
    # Last attempt: dump debug and stop
    save_debug("option_tab_not_found")
    stop("Could not locate/click the Option Chain tab")
  }
} else {
  # 4) Scroll into view and click via JS (more reliable than WebDriver click here)
  try(remDr$executeScript("arguments[0].scrollIntoView({block: 'center'});", list(tab_el)), silent = TRUE)
  Sys.sleep(1)
  # JS click
  retry(remDr$executeScript("arguments[0].click();", list(tab_el)))
  Sys.sleep(3)
}

# 5) Wait for the Option Chain content to be present
log_msg("Waiting for option chain table container after click")
oc_div <- try(wait_for("#equity-derivative-optionChainTable", timeout = 60), silent = TRUE)
if (inherits(oc_div, "try-error")) {
  log_msg("Option chain container not visible after click; saving debug")
  save_debug("option_chain_missing_post_click")
  stop("Option Chain container did not appear after clicking the tab")
}


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
csv_path  <- fi_
