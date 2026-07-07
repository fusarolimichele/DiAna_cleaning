# ------------------------------------------------------------------------------
# Script: 01_download_faers.R
# Purpose:
#   Download FAERS ASCII zip files, unzip them.
#   Apply the three known historical line-break fixes to specific FAERS DRUG files.
#   Validate the FAERS download/extract stage (S1).
#
# Checks:
#   1. The FDA page still yields ASCII ZIP links.
#   2. Manifest row count matches scraped link count.
#   3. ZIP files referenced by manifest exist locally when status is OK.
#   4. Extract directories referenced by manifest exist locally when status is OK.
#   5. Extracted TXT file count is > 0.
#   6. DEMO18Q1.txt exists somewhere under 2018Q1 extraction.
#   7. DEMO18Q1_new.txt no longer exists under 2018Q1 extraction.
# ------------------------------------------------------------------------------

suppressPackageStartupMessages({
  library(xml2)
  library(rvest)
  library(data.table)
})

# configuration ----------------------------------------------------------------
options(timeout = 1200)

FAERS_QDE_URL <- "https://fis.fda.gov/extensions/FPD-QDE-FAERS/FPD-QDE-FAERS.html"

BASE_DIR <- "."
RAW_DIR <- file.path(BASE_DIR, "data_raw")
ZIP_DIR <- file.path(RAW_DIR, "faers_zip")
ASCII_DIR <- file.path(RAW_DIR, "faers_ascii")
MANIFEST_PATH <- file.path(RAW_DIR, "faers_download_manifest.csv")

overwrite_downloads <- FALSE
overwrite_extracts <- FALSE

# helpers ----------------------------------------------------------------------
dir_create_safe <- function(path) {
  dir.create(path, recursive = TRUE, showWarnings = FALSE)
  invisible(path)
}

normalize_url <- function(href, base_url) {
  if (is.na(href) || !nzchar(href)) return(NA_character_)
  xml2::url_absolute(href, base_url)
}


extract_quarter_id <- function(x) {
  x <- basename(tolower(x))
  m <- regexpr("(19|20)[0-9]{2}q[1-4]", x, perl = TRUE)
  if (m[1] == -1L) return(NA_character_)
  toupper(regmatches(x, m))
}

extract_archive_name <- function(url) {
  basename(sub("\\?.*$", "", url))
}

zip_stem <- function(zip_filename) {
  sub("\\.zip$", "", zip_filename, ignore.case = TRUE)
}

load_manifest <- function(path) {
  if (!file.exists(path)) {
    return(data.table(
      quarter_id = character(),
      zip_name = character(),
      url = character(),
      zip_path = character(),
      extract_dir = character(),
      downloaded = logical(),      # action happened this run
      extracted = logical(),       # action happened this run
      download_ok = logical(),     # end-state/status OK
      extract_ok = logical(),      # end-state/status OK
      download_time = character(), # last time action happened
      extract_time = character(),  # last time action happened
      last_checked_time = character(),
      zip_bytes = numeric(),
      unzip_ok = logical(),        # backward-compatible alias for extract_ok
      notes = character()
    ))
  }
  
  dt <- fread(path, na.strings = c("", "NA"))
  setDT(dt)
  
  needed <- c(
    "quarter_id", "zip_name", "url", "zip_path", "extract_dir",
    "downloaded", "extracted", "download_ok", "extract_ok",
    "download_time", "extract_time", "last_checked_time",
    "zip_bytes", "unzip_ok", "notes"
  )
  
  missing_cols <- setdiff(needed, names(dt))
  for (nm in missing_cols) dt[, (nm) := NA]
  
  dt[, ..needed]
}

save_manifest <- function(dt, path) {
  fwrite(dt, path)
}

manifest_key <- function(dt) {
  do.call(paste, c(dt[, .(quarter_id, zip_name, url)], sep = "\r"))
}

find_manifest_row <- function(manifest, quarter_id_value, zip_name_value, url_value) {
  if (nrow(manifest) == 0L) return(NULL)
  
  out <- manifest[
    quarter_id == quarter_id_value &
      zip_name == zip_name_value &
      url == url_value
  ]
  
  if (nrow(out) == 0L) return(NULL)
  out[1]
}

upsert_manifest_row <- function(manifest, row_dt) {
  setDT(manifest)
  setDT(row_dt)
  
  key_cols <- c("quarter_id", "zip_name", "url")
  
  if (nrow(manifest) == 0L) return(copy(row_dt))
  
  mkey <- do.call(paste, c(manifest[, ..key_cols], sep = "\r"))
  rkey <- do.call(paste, c(row_dt[, ..key_cols], sep = "\r"))
  idx <- match(rkey, mkey)
  
  if (is.na(idx)) {
    manifest <- rbindlist(list(manifest, row_dt), fill = TRUE, use.names = TRUE)
  } else {
    for (nm in names(row_dt)) {
      set(manifest, i = idx, j = nm, value = row_dt[[nm]])
    }
  }
  
  manifest[]
}

list_ascii_zip_links <- function(page_url) {
  pg <- read_html(page_url)
  hrefs <- html_attr(html_elements(pg, "a"), "href")
  hrefs <- unique(stats::na.omit(hrefs))
  hrefs <- vapply(hrefs, normalize_url, character(1), base_url = page_url)
  
  hrefs <- hrefs[
    grepl("\\.zip($|\\?)", hrefs, ignore.case = TRUE) &
      grepl("ascii", hrefs, ignore.case = TRUE)
  ]
  
  hrefs <- unique(hrefs)
  
  data.table(
    url = hrefs,
    zip_name = vapply(hrefs, extract_archive_name, character(1)),
    quarter_id = vapply(hrefs, extract_quarter_id, character(1))
  )[order(quarter_id, zip_name)]
}

download_zip <- function(url, destfile, overwrite = FALSE) {
  if (file.exists(destfile) && !overwrite) {
    return(list(
      ok = TRUE,
      downloaded = FALSE,
      notes = "zip already present"
    ))
  }
  
  tryCatch(
    {
      download.file(url, destfile, mode = "wb", quiet = FALSE)
      list(ok = TRUE, downloaded = TRUE, notes = NA_character_)
    },
    error = function(e) {
      list(ok = FALSE, downloaded = FALSE, notes = conditionMessage(e))
    }
  )
}

normalize_extracted_filenames <- function(extract_dir) {
  # Preserve the audited one-off rename exactly:
  # DEMO18Q1_new.txt -> DEMO18Q1.txt
  old_path <- list.files(
    extract_dir,
    pattern = "^DEMO18Q1_new\\.txt$",
    full.names = TRUE,
    recursive = TRUE,
    ignore.case = TRUE
  )
  
  if (!length(old_path)) return(invisible(NULL))
  
  for (src in old_path) {
    dst <- sub("_new(\\.txt)$", "\\1", src, ignore.case = TRUE)
    if (!file.exists(dst)) {
      file.rename(src, dst)
    }
  }
  
  invisible(NULL)
}

extract_zip <- function(zip_path, extract_dir, overwrite = FALSE) {
  if (dir.exists(extract_dir) && !overwrite) {
    existing_files <- list.files(extract_dir, recursive = TRUE, all.files = TRUE)
    if (length(existing_files) > 0L) {
      return(list(
        ok = TRUE,
        extracted = FALSE,
        notes = "extract dir already populated"
      ))
    }
  }
  
  dir_create_safe(extract_dir)
  
  tryCatch(
    {
      unzip(zip_path, exdir = extract_dir)
      normalize_extracted_filenames(extract_dir)
      list(ok = TRUE, extracted = TRUE, notes = NA_character_)
    },
    error = function(e) {
      list(ok = FALSE, extracted = FALSE, notes = conditionMessage(e))
    }
  )
}

pass <- function(...) {
  message(sprintf(...))
}

# main -------------------------------------------------------------------------
dir_create_safe(RAW_DIR)
dir_create_safe(ZIP_DIR)
dir_create_safe(ASCII_DIR)

message("Scraping FAERS quarterly ASCII ZIP links...")
links_dt <- list_ascii_zip_links(FAERS_QDE_URL)

if (nrow(links_dt) == 0L) {
  stop("No ASCII ZIP links found on the FAERS QDE page.")
}

message(sprintf("Found %d ASCII ZIP link(s).", nrow(links_dt)))

manifest <- load_manifest(MANIFEST_PATH)

for (i in seq_len(nrow(links_dt))) {
  row <- links_dt[i]
  
  url <- row$url
  zip_name <- row$zip_name
  quarter_id <- row$quarter_id
  
  zip_path <- file.path(ZIP_DIR, zip_name)
  extract_dir <- file.path(
    ASCII_DIR,
    ifelse(is.na(quarter_id), zip_stem(zip_name), quarter_id)
  )
  
  prev <- find_manifest_row(manifest, quarter_id, zip_name, url)
  now_chr <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  
  message("")
  message(sprintf("[%d/%d] %s", i, nrow(links_dt), zip_name))
  message(sprintf(" quarter: %s", ifelse(is.na(quarter_id), "NA", quarter_id)))
  
  dl <- download_zip(url, zip_path, overwrite = overwrite_downloads)
  
  zip_bytes <- if (file.exists(zip_path)) file.info(zip_path)$size else NA_real_
  
  ex <- if (isTRUE(dl$ok)) {
    extract_zip(zip_path, extract_dir, overwrite = overwrite_extracts)
  } else {
    list(ok = FALSE, extracted = FALSE, notes = "download failed; extraction skipped")
  }
  
  notes <- paste(na.omit(c(dl$notes, ex$notes)), collapse = " | ")
  if (!nzchar(notes)) notes <- NA_character_
  
  download_time <- if (isTRUE(dl$downloaded)) {
    now_chr
  } else if (!is.null(prev)) {
    prev$download_time
  } else {
    NA_character_
  }
  
  extract_time <- if (isTRUE(ex$extracted)) {
    now_chr
  } else if (!is.null(prev)) {
    prev$extract_time
  } else {
    NA_character_
  }
  
  manifest_row <- data.table(
    quarter_id = quarter_id,
    zip_name = zip_name,
    url = url,
    zip_path = zip_path,
    extract_dir = extract_dir,
    
    downloaded = isTRUE(dl$downloaded),
    extracted = isTRUE(ex$extracted),
    
    download_ok = isTRUE(dl$ok),
    extract_ok = isTRUE(ex$ok),
    
    download_time = download_time,
    extract_time = extract_time,
    last_checked_time = now_chr,
    
    zip_bytes = zip_bytes,
    unzip_ok = isTRUE(ex$ok),  # backward-compatible alias
    notes = notes
  )
  
  manifest <- upsert_manifest_row(manifest, manifest_row)
  save_manifest(manifest, MANIFEST_PATH)
}

message("")
message("Done.")
message(sprintf("Manifest written to: %s", MANIFEST_PATH))
message(sprintf("ZIP files stored in: %s", ZIP_DIR))
message(sprintf("Extracted files stored in: %s", ASCII_DIR))

# patch files ------------------------------------------------------------------

ASCII_ROOT <- file.path(".", "data_raw", "faers_ascii")

correct_problematic_file <- function(file_path, old_line) {
  if (!file.exists(file_path)) {
    stop(sprintf("File not found: %s", file_path), call. = FALSE)
  }
  
  pre_lines <- readLines(file(file_path, open = "r"), warn = FALSE)
  
  if (!any(grepl(old_line, pre_lines, fixed = TRUE))) {
    message(sprintf("Pattern not found, leaving file unchanged: %s | %s", file_path, old_line))
    return(invisible(FALSE))
  }
  
  post_lines <- unlist(strsplit(
    gsub(
      old_line,
      gsub("([0-9]+)$", "SePaRaToR\\1", old_line),
      pre_lines,
      fixed = TRUE
    ),
    "SePaRaToR"
  ))
  
  writeLines(post_lines, con = file_path)
  
  # validation
  final_lines <- readLines(file(file_path, open = "r"), warn = FALSE)
  
  if (any(grepl(old_line, final_lines, fixed = TRUE))) {
    stop(sprintf("Patch failed; corrupt pattern still present: %s", file_path), call. = FALSE)
  }
  
  if (length(final_lines) != length(pre_lines) + 1L) {
    warning(sprintf(
      "Unexpected line-count change in %s: before=%d after=%d (expected +1)",
      file_path, length(pre_lines), length(final_lines)
    ), call. = FALSE)
  }
  
  message(sprintf(
    "Patched: %s | lines before=%d after=%d",
    file_path, length(pre_lines), length(final_lines)
  ))
  
  invisible(TRUE)
}

correct_problematic_file(
  file.path(ASCII_ROOT, "2011Q2", "ascii", "DRUG11Q2.txt"),
  "$$$$$$7475791"
)

correct_problematic_file(
  file.path(ASCII_ROOT, "2011Q3", "ascii", "DRUG11Q3.txt"),
  "$$$$$$7652730"
)

correct_problematic_file(
  file.path(ASCII_ROOT, "2011Q4", "ascii", "DRUG11Q4.txt"),
  "021487$7941354"
)

replace_literal_delimiter <- function(file_path, bad_text, safe_text) {
  if (!file.exists(file_path)) {
    stop(sprintf("File not found: %s", file_path), call. = FALSE)
  }
  
  lines <- readLines(file_path, warn = FALSE, encoding = "UTF-8")
  n_before <- sum(grepl(bad_text, lines, fixed = TRUE))
  
  if (n_before == 0L) {
    message(sprintf("Pattern not found, leaving file unchanged: %s | %s", file_path, bad_text))
    return(invisible(FALSE))
  }
  
  lines2 <- gsub(bad_text, safe_text, lines, fixed = TRUE)
  n_after_bad <- sum(grepl(bad_text, lines2, fixed = TRUE))
  n_after_safe <- sum(grepl(safe_text, lines2, fixed = TRUE))
  
  if (n_after_bad != 0L) {
    stop(sprintf("Replacement failed for file: %s", file_path), call. = FALSE)
  }
  
  writeLines(lines2, con = file_path, useBytes = TRUE)
  
  message(sprintf(
    "Patched literal delimiter in %s | occurrences replaced: %d",
    file_path, n_after_safe
  ))
  
  invisible(TRUE)
}

replace_literal_delimiter(
  file.path(ASCII_ROOT, "2012Q1", "ascii", "DEMO12Q1.TXT"),
  "JP-CUBIST-$E2B0000000182",
  "JP-CUBIST-__LITERAL_DOLLAR__E2B0000000182"
)
# Check S1----------------------------------------------------------------------
FAERS_QDE_URL <- "https://fis.fda.gov/extensions/FPD-QDE-FAERS/FPD-QDE-FAERS.html"

BASE_DIR <- "."
RAW_DIR <- file.path(BASE_DIR, "data_raw")
ZIP_DIR <- file.path(RAW_DIR, "faers_zip")
ASCII_DIR <- file.path(RAW_DIR, "faers_ascii")
MANIFEST_PATH <- file.path(RAW_DIR, "faers_download_manifest.csv")

if (!file.exists(MANIFEST_PATH)) {
  fail("Manifest not found: %s", MANIFEST_PATH)
}

manifest <- fread(MANIFEST_PATH, na.strings = c("", "NA"))
setDT(manifest)

required_cols <- c(
  "quarter_id", "zip_name", "url", "zip_path", "extract_dir",
  "downloaded", "extracted", "download_ok", "extract_ok",
  "download_time", "extract_time", "last_checked_time",
  "zip_bytes", "unzip_ok", "notes"
)

missing_cols <- setdiff(required_cols, names(manifest))
if (length(missing_cols)) {
  fail("Manifest is missing required columns: %s", paste(missing_cols, collapse = ", "))
}

links_dt <- list_ascii_zip_links(FAERS_QDE_URL)

if (nrow(links_dt) == 0L) {
  fail("No ASCII ZIP links found on the FDA page.")
}
pass("OK: scraped %d ASCII ZIP links from FDA page.", nrow(links_dt))

if (nrow(manifest) != nrow(links_dt)) {
  fail(
    "Manifest row count (%d) does not match scraped link count (%d).",
    nrow(manifest), nrow(links_dt)
  )
}
pass("OK: manifest row count matches scraped link count (%d).", nrow(manifest))

# Check uniqueness of manifest keys
dup_keys <- manifest[, .N, by = .(quarter_id, zip_name, url)][N > 1L]
if (nrow(dup_keys) > 0L) {
  fail("Manifest contains duplicate key rows.")
}
pass("OK: manifest keys are unique.")

# Check that all expected links are represented
missing_links <- fsetdiff(
  links_dt[, .(quarter_id, zip_name, url)],
  manifest[, .(quarter_id, zip_name, url)]
)

if (nrow(missing_links) > 0L) {
  fail("Some scraped links are missing from the manifest.")
}
pass("OK: every scraped link is represented in the manifest.")

# ZIP existence where download_ok is TRUE
zip_missing <- manifest[download_ok == TRUE & !file.exists(zip_path)]
if (nrow(zip_missing) > 0L) {
  fail("Some manifest rows have download_ok=TRUE but ZIP file is missing locally.")
}
pass("OK: ZIP presence agrees with manifest status.")

# Extract directory existence where extract_ok is TRUE
extract_missing <- manifest[extract_ok == TRUE & !dir.exists(extract_dir)]
if (nrow(extract_missing) > 0L) {
  fail("Some manifest rows have extract_ok=TRUE but extract_dir is missing locally.")
}
pass("OK: extract directory presence agrees with manifest status.")

# TXT count > 0
txt_files <- list.files(
  path = ASCII_DIR,
  recursive = TRUE,
  pattern = "\\.TXT$|\\.txt$",
  full.names = TRUE,
  ignore.case = TRUE
)

if (length(txt_files) == 0L) {
  fail("No extracted TXT files found under: %s", ASCII_DIR)
}
pass("OK: found %d extracted TXT file(s).", length(txt_files))

# 2018Q1 rename checks
q2018_dir <- file.path(ASCII_DIR, "2018Q1")

demo18q1_ok <- length(list.files(
  path = q2018_dir,
  recursive = TRUE,
  pattern = "^DEMO18Q1\\.txt$",
  full.names = TRUE,
  ignore.case = TRUE
)) > 0L

demo18q1_new_still_exists <- length(list.files(
  path = q2018_dir,
  recursive = TRUE,
  pattern = "^DEMO18Q1_new\\.txt$",
  full.names = TRUE,
  ignore.case = TRUE
)) > 0L

if (!demo18q1_ok) {
  fail("DEMO18Q1.txt was not found under %s", q2018_dir)
}
pass("OK: DEMO18Q1.txt exists under 2018Q1 extraction.")

if (demo18q1_new_still_exists) {
  fail("DEMO18Q1_new.txt still exists under %s", q2018_dir)
}
pass("OK: DEMO18Q1_new.txt is absent after normalization.")

# Quarter-level table presence summary -----------------------------------------

required_tables <- c("DEMO", "INDI", "DRUG", "REAC", "THER", "OUTC", "RPSR")

# only check quarters that the manifest says extracted successfully
quarter_dirs <- unique(
  manifest[extract_ok == TRUE & !is.na(quarter_id),
           .(quarter_id, extract_dir)]
)

if (nrow(quarter_dirs) == 0L) {
  fail("No extracted quarter directories available for table presence checks.")
}

quarter_table_summary <- rbindlist(
  lapply(seq_len(nrow(quarter_dirs)), function(i) {
    qid <- quarter_dirs$quarter_id[i]
    qdir <- quarter_dirs$extract_dir[i]
    
    # default empty result
    present_required <- setNames(rep(FALSE, length(required_tables)), required_tables)
    delete_present <- FALSE
    txt_n_ascii <- NA_integer_
    
    if (dir.exists(qdir)) {
      # 1) core files: search all TXT files under the quarter, but ignore DELETED subtree
      all_txt <- list.files(
        path = qdir,
        recursive = TRUE,
        full.names = TRUE,
        ignore.case = TRUE
      )
      all_txt <- all_txt[grepl("\\.txt$", all_txt, ignore.case = TRUE)]
      
      # exclude files located under a DELETED directory for the core-table scan
      core_txt <- all_txt[!grepl("(^|[/\\\\])DELETED([/\\\\]|$)", all_txt, ignore.case = TRUE)]
      core_txt_base <- basename(core_txt)
      txt_n_ascii <- length(core_txt_base)
      
      present_required <- vapply(
        required_tables,
        function(tb) any(grepl(paste0("^", tb), core_txt_base, ignore.case = TRUE)),
        logical(1)
      )
      
      # 2) optional DELETE files: specifically search inside DELETED subtree
      deleted_txt <- all_txt[grepl("(^|[/\\\\])DELETED([/\\\\]|$)", all_txt, ignore.case = TRUE)]
      deleted_txt_base <- basename(deleted_txt)
      
      delete_present <- any(grepl("^DELETE", deleted_txt_base, ignore.case = TRUE))
    }
    
    data.table(
      quarter_id = qid,
      extract_dir = qdir,
      txt_file_count_core = txt_n_ascii,
      DEMO = unname(present_required["DEMO"]),
      INDI = unname(present_required["INDI"]),
      DRUG = unname(present_required["DRUG"]),
      REAC = unname(present_required["REAC"]),
      THER = unname(present_required["THER"]),
      OUTC = unname(present_required["OUTC"]),
      RPSR = unname(present_required["RPSR"]),
      DELETE = delete_present
    )
  }),
  fill = TRUE
)[order(quarter_id)]

message("")
message("Quarter table presence summary")
message("----------------------------")
print(quarter_table_summary)

# optional: save summary table
quarter_summary_path <- file.path(RAW_DIR, "faers_quarter_table_summary.csv")
fwrite(quarter_table_summary, quarter_summary_path)
message(sprintf("Quarter summary written to: %s", quarter_summary_path))

# fail if any required tables are missing
missing_required <- quarter_table_summary[
  !(DEMO & INDI & DRUG & REAC & THER & OUTC & RPSR)
]

if (nrow(missing_required) > 0L) {
  fail(
    "Some quarters are missing one or more required tables. See summary: %s",
    quarter_summary_path
  )
}

pass("OK: every extracted quarter contains all required tables; optional DELETE tracked separately.")

# summary counts----------------------------------------------------------------
n_downloaded_this_run <- sum(manifest$downloaded == TRUE, na.rm = TRUE)
n_extracted_this_run <- sum(manifest$extracted == TRUE, na.rm = TRUE)
n_download_ok <- sum(manifest$download_ok == TRUE, na.rm = TRUE)
n_extract_ok <- sum(manifest$extract_ok == TRUE, na.rm = TRUE)

message("")
message("Summary")
message("-------")
message(sprintf("Manifest rows        : %d", nrow(manifest)))
message(sprintf("ZIP files on disk    : %d", length(list.files(ZIP_DIR, pattern = "\\.zip$", ignore.case = TRUE))))
message(sprintf("TXT files extracted  : %d", length(txt_files)))
message(sprintf("downloaded == TRUE   : %d", n_downloaded_this_run))
message(sprintf("extracted == TRUE    : %d", n_extracted_this_run))
message(sprintf("download_ok == TRUE  : %d", n_download_ok))
message(sprintf("extract_ok == TRUE   : %d", n_extract_ok))

message("")
message("All S1 download checks passed.")
