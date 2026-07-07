# ------------------------------------------------------------------------------
# Script: 07_final_check_against_old.R
# Purpose:
#   Low-memory comparison of a new FAERS database versus an older RDS version.
#
# Inputs:
#   - NEW_RDS_DIR/<TABLE>.rds  or  NEW_RDS_DIR/<TABLE>.parquet  (see new_data_format)
#   - OLD_RDS_DIR/<TABLE>.rds  (old database is always RDS)
#
# Outputs:
#   - comparison_old_vs_new/findings.csv
#   - comparison_old_vs_new/table_summary.csv
#   - comparison_old_vs_new/schema_differences.csv
#   - comparison_old_vs_new/report.txt
#   - comparison_old_vs_new/sample_*.csv
#
# Design:
#   - Loads one table at a time
#   - Keeps both DEMO tables in memory together for detailed row-level comparison
#   - Avoids loading all tables as one giant database object
#   - Avoids expensive full signature joins for REAC/DRUG by default
#   - new_data_format controls how the new database is read:
#       "auto"    — try parquet first, fall back to RDS (default)
#       "parquet" — parquet only
#       "rds"     — RDS only (original behavior)
# ------------------------------------------------------------------------------

suppressPackageStartupMessages({
  library(data.table)
  library(arrow)
})

options(timeout = 1200)

# ------------------------------- configuration ------------------------------- #

BASE_DIR <- "."
source(file.path(BASE_DIR, "R", "utils.R"))

NEW_RDS_DIR <- file.path(BASE_DIR, "data_legacy")
OLD_RDS_DIR <- file.path(BASE_DIR, "old_data_rds")

# Format of the NEW database files. The old database is always RDS.
#   "rds"     — RDS only (data_legacy/ contains only RDS files)
#   "auto"    — try parquet first, fall back to RDS
#   "parquet" — parquet only
new_data_format <- "rds"

OUT_DIR <- file.path(BASE_DIR, "comparison_old_vs_new")

max_example_rows <- 2000L
save_all_samples <- TRUE

TABLE_PRIORITY <- c(
  "DEMO", "DEMO_SUPP",
  "DRUG", "DRUG_NAME", "DOSES", "DRUG_SUPP",
  "REAC", "INDI", "OUTC", "THER"
)

DEMO_COMPARE_COLS <- c(
  "caseid",
  "quarter",
  "fda_dt",
  "rept_dt",
  "event_dt",
  "caseversion",
  "sex",
  "age_in_days",
  "age_grp",
  "wt_in_kgs",
  "reporter_country",
  "premarketing",
  "literature",
  "RB_duplicates",
  "RB_duplicates_only_susp"
)

# These are the heavier content-comparison sections.
# Keep FALSE for safer memory usage.
COMPARE_REAC_ROWCOUNTS_PER_PID <- TRUE
COMPARE_DRUG_ROWCOUNTS_PER_PID <- TRUE
COMPARE_REAC_TERM_FREQUENCIES  <- TRUE
COMPARE_DRUG_SUBSTANCE_FREQ    <- TRUE
COMPARE_THER_SUMMARY           <- TRUE

# Keep FALSE by default: these can be expensive on very large databases.
COMPARE_REAC_SIGNATURES <- FALSE
COMPARE_DRUG_SIGNATURES <- FALSE
COMPARE_DRUG_SUSPECTED_SIGNATURES <- FALSE

# Derivative drug table comparisons (DRUG_NAME, DOSES, DRUG_SUPP)
COMPARE_DRUG_NAME_COVERAGE <- TRUE
COMPARE_DOSES_COVERAGE     <- TRUE
COMPARE_DRUG_SUPP_COVERAGE <- TRUE

TOP_TERM_N <- 200L

# ------------------------------- helpers ------------------------------------- #

as_cmp_char <- function(x) {
  if (is.factor(x)) x <- as.character(x)
  if (inherits(x, "POSIXt")) x <- format(x, tz = "UTC", usetz = TRUE)
  if (inherits(x, "Date")) x <- as.character(x)
  normalize_text(x)
}

class_signature <- function(x) {
  paste(class(x), collapse = "|")
}

ordered_signature <- function(x) {
  if (!is.factor(x)) return(NA)
  is.ordered(x)
}

level_signature <- function(x, max_levels = 50L) {
  if (!is.factor(x)) return(NA_character_)
  lv <- levels(x)
  if (length(lv) > max_levels) {
    paste(c(lv[seq_len(max_levels)], "..."), collapse = " | ")
  } else {
    paste(lv, collapse = " | ")
  }
}

list_tables_in_dir <- function(dir_path) {
  if (!dir.exists(dir_path)) {
    stop("Directory does not exist: ", dir_path, call. = FALSE)
  }

  rds_files <- list.files(dir_path, pattern = "\\.rds$", full.names = FALSE)
  rds_files <- rds_files[
    !rds_files %in% c("faers_master_db.rds", "faers_master_db_index.rds")
  ]

  parquet_files <- list.files(dir_path, pattern = "\\.parquet$", full.names = FALSE)

  rds_tabs     <- toupper(tools::file_path_sans_ext(rds_files))
  parquet_tabs <- toupper(tools::file_path_sans_ext(parquet_files))

  tabs <- unique(c(rds_tabs, parquet_tabs))
  tabs[nzchar(tabs)]
}

load_table <- function(dir_path, tbl_name, col_select = NULL) {
  tbl_upper <- toupper(tbl_name)

  parquet_path <- file.path(dir_path, sprintf("%s.parquet", tbl_upper))
  rds_path     <- file.path(dir_path, sprintf("%s.rds",     tbl_upper))

  # The old database is always RDS. Apply new_data_format only for the new dir.
  is_old <- identical(
    normalizePath(dir_path, mustWork = FALSE),
    normalizePath(OLD_RDS_DIR, mustWork = FALSE)
  )
  fmt <- if (is_old) "rds" else new_data_format

  if (fmt == "parquet" || (fmt == "auto" && file.exists(parquet_path))) {
    if (!file.exists(parquet_path)) return(NULL)
    x <- if (is.null(col_select)) {
      as.data.table(read_parquet(parquet_path))
    } else {
      as.data.table(read_parquet(parquet_path, col_select = col_select))
    }
  } else {
    if (!file.exists(rds_path)) return(NULL)
    x <- readRDS(rds_path)
    if (!is.data.table(x)) setDT(x)
    if (!is.null(col_select)) {
      cols <- intersect(col_select, names(x))
      x <- x[, ..cols]
    }
  }

  # Old RDS files store primaryid as numeric; normalise to character so joins
  # between old and new tables never produce an incompatible-type bmerge error.
  if ("primaryid" %in% names(x) && !is.character(x$primaryid)) {
    x[, primaryid := as.character(primaryid)]
  }

  x
}

write_sample <- function(dt, filename) {
  if (!save_all_samples || is.null(dt) || nrow(dt) == 0L) return(invisible(NULL))
  fwrite(head(dt, max_example_rows), file.path(OUT_DIR, filename))
  invisible(NULL)
}

# ------------------------------- output stores ------------------------------- #

findings <- data.table(
  expectation   = character(),   # info / expected / review
  severity      = character(),   # info / warn / high
  table_name    = character(),
  metric        = character(),
  old_value     = character(),
  new_value     = character(),
  n_affected    = integer(),
  likely_reason = character(),
  review_hint   = character()
)

table_summary <- data.table(
  table_name            = character(),
  old_rows              = integer(),
  new_rows              = integer(),
  old_cols              = integer(),
  new_cols              = integer(),
  old_unique_primaryids = integer(),
  new_unique_primaryids = integer()
)

schema_diffs <- data.table(
  table_name     = character(),
  column_name    = character(),
  issue          = character(),
  old_value      = character(),
  new_value      = character(),
  likely_reason  = character()
)

report_lines <- character()

add_report <- function(...) {
  line <- paste0(...)
  report_lines <<- c(report_lines, line)
  message(line)
}

add_finding <- function(expectation = c("info", "expected", "review"),
                        severity = c("info", "warn", "high"),
                        table_name,
                        metric,
                        old_value = NA,
                        new_value = NA,
                        n_affected = NA_integer_,
                        likely_reason = "",
                        review_hint = "") {
  expectation <- match.arg(expectation)
  severity <- match.arg(severity)
  
  row <- data.table(
    expectation   = expectation,
    severity      = severity,
    table_name    = table_name,
    metric        = metric,
    old_value     = as.character(old_value),
    new_value     = as.character(new_value),
    n_affected    = as.integer(n_affected),
    likely_reason = likely_reason,
    review_hint   = review_hint
  )
  
  findings <<- rbindlist(list(findings, row), fill = TRUE)
  
  prefix <- switch(
    expectation,
    info     = "[INFO]",
    expected = "[EXPECTED]",
    review   = "[REVIEW]"
  )
  
  msg <- paste0(
    prefix, " ", table_name, " / ", metric,
    if (!is.na(n_affected)) paste0(" / n=", n_affected) else "",
    if (!is.na(old_value) || !is.na(new_value)) paste0(" / old=", old_value, " / new=", new_value) else "",
    if (nzchar(likely_reason)) paste0(" / likely reason: ", likely_reason) else "",
    if (nzchar(review_hint)) paste0(" / check: ", review_hint) else ""
  )
  
  add_report(msg)
}

# ------------------------------- metadata ------------------------------------ #

summarize_table <- function(dt, tbl_name) {
  out <- list(
    nrow = nrow(dt),
    ncol = ncol(dt),
    cols = names(dt),
    unique_primaryids = NA_integer_,
    schema = NULL
  )
  
  if ("primaryid" %in% names(dt)) {
    out$unique_primaryids <- uniqueN(as.character(dt$primaryid))
  }
  
  schema <- data.table(
    column_name = names(dt),
    class_sig   = vapply(dt, class_signature, character(1)),
    is_factor   = vapply(dt, is.factor, logical(1)),
    is_ordered  = vapply(dt, ordered_signature, logical(1)),
    level_sig   = vapply(dt, level_signature, character(1))
  )
  
  out$schema <- schema
  out
}

compare_schema <- function(old_meta, new_meta, tbl_name) {
  old_schema <- copy(old_meta$schema)
  new_schema <- copy(new_meta$schema)
  
  old_only_cols <- setdiff(old_schema$column_name, new_schema$column_name)
  new_only_cols <- setdiff(new_schema$column_name, old_schema$column_name)
  
  if (length(old_only_cols)) {
    for (col in old_only_cols) {
      schema_diffs <<- rbindlist(list(
        schema_diffs,
        data.table(
          table_name = tbl_name,
          column_name = col,
          issue = "column_missing_in_new",
          old_value = "present",
          new_value = "missing",
          likely_reason = "Could be intentional schema redesign or an unintended dropped column."
        )
      ))
    }
    
    add_finding(
      expectation = "review",
      severity = "warn",
      table_name = tbl_name,
      metric = "columns_missing_in_new",
      n_affected = length(old_only_cols),
      likely_reason = "Column-name differences should be explicit.",
      review_hint = paste(head(old_only_cols, 10), collapse = ", ")
    )
  }
  
  if (length(new_only_cols)) {
    for (col in new_only_cols) {
      schema_diffs <<- rbindlist(list(
        schema_diffs,
        data.table(
          table_name = tbl_name,
          column_name = col,
          issue = "column_new_in_new",
          old_value = "missing",
          new_value = "present",
          likely_reason = "Could be intentional enrichment or a schema drift/change."
        )
      ))
    }
    
    add_finding(
      expectation = "review",
      severity = "warn",
      table_name = tbl_name,
      metric = "columns_new_in_new",
      n_affected = length(new_only_cols),
      likely_reason = "New columns may be fine, but should be explicit.",
      review_hint = paste(head(new_only_cols, 10), collapse = ", ")
    )
  }
  
  common_cols <- intersect(old_schema$column_name, new_schema$column_name)
  if (!length(common_cols)) return(invisible(NULL))
  
  o <- old_schema[column_name %in% common_cols]
  n <- new_schema[column_name %in% common_cols]
  setkey(o, column_name)
  setkey(n, column_name)
  
  for (col in common_cols) {
    oc <- o[list(col)]
    nc <- n[list(col)]
    
    if (!identical(oc$class_sig, nc$class_sig)) {
      schema_diffs <<- rbindlist(list(
        schema_diffs,
        data.table(
          table_name = tbl_name,
          column_name = col,
          issue = "class_difference",
          old_value = oc$class_sig,
          new_value = nc$class_sig,
          likely_reason = "Could be a typing change, factor restoration difference, or an unintended class drift."
        )
      ))
      
      add_finding(
        expectation = "review",
        severity = "warn",
        table_name = tbl_name,
        metric = paste0("class_diff:", col),
        old_value = oc$class_sig,
        new_value = nc$class_sig,
        likely_reason = "Class changed between old and new versions.",
        review_hint = col
      )
    }
    
    if (!identical(oc$is_ordered, nc$is_ordered) && (!is.na(oc$is_ordered) || !is.na(nc$is_ordered))) {
      schema_diffs <<- rbindlist(list(
        schema_diffs,
        data.table(
          table_name = tbl_name,
          column_name = col,
          issue = "ordered_status_difference",
          old_value = as.character(oc$is_ordered),
          new_value = as.character(nc$is_ordered),
          likely_reason = "Ordered-factor status changed; this is often a typing-restoration or parquet/RDS round-trip difference."
        )
      ))
      
      add_finding(
        expectation = "review",
        severity = "warn",
        table_name = tbl_name,
        metric = paste0("ordered_diff:", col),
        old_value = oc$is_ordered,
        new_value = nc$is_ordered,
        likely_reason = "Ordered-factor status changed.",
        review_hint = col
      )
    }
    
    if (isTRUE(oc$is_factor) && isTRUE(nc$is_factor) && !identical(oc$level_sig, nc$level_sig)) {
      schema_diffs <<- rbindlist(list(
        schema_diffs,
        data.table(
          table_name = tbl_name,
          column_name = col,
          issue = "factor_levels_difference",
          old_value = oc$level_sig,
          new_value = nc$level_sig,
          likely_reason = "Factor levels changed; could be expected after dictionary updates or type restoration changes."
        )
      ))
      
      add_finding(
        expectation = "review",
        severity = "warn",
        table_name = tbl_name,
        metric = paste0("levels_diff:", col),
        likely_reason = "Factor levels changed.",
        review_hint = col
      )
    }
  }
  
  invisible(NULL)
}

# ------------------------------- DEMO compare -------------------------------- #

compare_demo <- function(old_demo, new_demo) {
  add_report("")
  add_report("### DEMO comparison")
  
  keep_old <- intersect(names(old_demo), c("primaryid", DEMO_COMPARE_COLS, "mfr_num", "mfr_sndr"))
  keep_new <- intersect(names(new_demo), c("primaryid", DEMO_COMPARE_COLS, "mfr_num", "mfr_sndr"))
  
  old_demo <- copy(old_demo[, ..keep_old])
  new_demo <- copy(new_demo[, ..keep_new])
  
  assert_has_cols(old_demo, "primaryid", "old DEMO")
  assert_has_cols(new_demo, "primaryid", "new DEMO")
  
  old_demo[, primaryid := normalize_text(primaryid)]
  new_demo[, primaryid := normalize_text(primaryid)]
  
  old_demo <- old_demo[!is.na(primaryid)]
  new_demo <- new_demo[!is.na(primaryid)]
  
  old_pids <- safe_unique_char(old_demo$primaryid)
  new_pids <- safe_unique_char(new_demo$primaryid)
  
  common_pids   <- intersect(old_pids, new_pids)
  old_only_pids <- setdiff(old_pids, new_pids)
  new_only_pids <- setdiff(new_pids, old_pids)
  
  add_finding(
    expectation = "info",
    severity = "info",
    table_name = "DEMO",
    metric = "common_primaryids",
    n_affected = length(common_pids),
    likely_reason = "These are directly comparable across old and new databases."
  )
  
  latest_qtr <- NA_character_
  latest_qtr_new_only <- character()
  
  if ("quarter" %in% names(new_demo) && length(new_only_pids)) {
    qrank <- quarter_rank(new_demo$quarter)
    if (any(qrank >= 0L)) {
      latest_qtr <- unique(new_demo[qrank == max(qrank, na.rm = TRUE), quarter])[1]
      latest_qtr_pids <- safe_unique_char(new_demo[quarter == latest_qtr, primaryid])
      latest_qtr_new_only <- intersect(new_only_pids, latest_qtr_pids)
      
      add_finding(
        expectation = "expected",
        severity = "info",
        table_name = "DEMO",
        metric = "new_only_primaryids_from_latest_quarter",
        new_value = latest_qtr,
        n_affected = length(latest_qtr_new_only),
        likely_reason = "These are most likely the expected gain from the added quarter."
      )
    }
  }
  
  remaining_new_only <- setdiff(new_only_pids, latest_qtr_new_only)
  if (length(remaining_new_only)) {
    add_finding(
      expectation = "review",
      severity = "warn",
      table_name = "DEMO",
      metric = "new_only_primaryids_not_explained_by_latest_quarter",
      n_affected = length(remaining_new_only),
      likely_reason = "Could reflect changed completeness filters, structural dedup changes, or better dictionary/standardization rescuing previously dropped reports.",
      review_hint = "sample_new_only_primaryids_not_explained.csv"
    )
    write_sample(
      data.table(primaryid = remaining_new_only),
      "sample_new_only_primaryids_not_explained.csv"
    )
  }
  
  if (length(old_only_pids)) {
    add_finding(
      expectation = "review",
      severity = "warn",
      table_name = "DEMO",
      metric = "old_only_primaryids",
      n_affected = length(old_only_pids),
      likely_reason = "Could reflect superseded case versions, changed manufacturer dedup, deletion/nullification handling, or changed completeness filtering.",
      review_hint = "sample_old_only_primaryids.csv"
    )
    write_sample(
      data.table(primaryid = old_only_pids),
      "sample_old_only_primaryids.csv"
    )
    
    if ("caseid" %in% names(old_demo) && "caseid" %in% names(new_demo)) {
      old_case_map <- unique(old_demo[primaryid %in% old_only_pids, .(old_primaryid = primaryid, caseid)])
      new_case_map <- unique(new_demo[, .(new_primaryid = primaryid, caseid)])
      case_replaced <- merge(old_case_map, new_case_map, by = "caseid", allow.cartesian = TRUE)
      case_replaced <- case_replaced[old_primaryid != new_primaryid]
      
      if (nrow(case_replaced)) {
        add_finding(
          expectation = "expected",
          severity = "info",
          table_name = "DEMO",
          metric = "old_only_primaryids_with_caseid_still_present_in_new",
          n_affected = uniqueN(case_replaced$old_primaryid),
          likely_reason = "These look like old primaryids superseded by a later surviving version of the same case."
        )
        write_sample(case_replaced, "sample_old_primaryids_replaced_by_new_case_versions.csv")
      }
    }
    
    if (all(c("mfr_num", "mfr_sndr") %in% names(old_demo)) &&
        all(c("mfr_num", "mfr_sndr") %in% names(new_demo))) {
      old_mfr <- unique(old_demo[
        primaryid %in% old_only_pids & !is.na(mfr_num) & !is.na(mfr_sndr),
        .(old_primaryid = primaryid, mfr_num, mfr_sndr)
      ])
      
      new_mfr <- unique(new_demo[
        !is.na(mfr_num) & !is.na(mfr_sndr),
        .(new_primaryid = primaryid, mfr_num, mfr_sndr)
      ])
      
      mfr_replaced <- merge(old_mfr, new_mfr, by = c("mfr_num", "mfr_sndr"), allow.cartesian = TRUE)
      mfr_replaced <- mfr_replaced[old_primaryid != new_primaryid]
      
      if (nrow(mfr_replaced)) {
        add_finding(
          expectation = "expected",
          severity = "info",
          table_name = "DEMO",
          metric = "old_only_primaryids_with_same_mfr_key_still_present_in_new",
          n_affected = uniqueN(mfr_replaced$old_primaryid),
          likely_reason = "These may have been replaced because manufacturer-ID dedup kept a different surviving report."
        )
        write_sample(mfr_replaced, "sample_old_primaryids_replaced_by_mfr_dedup.csv")
      }
    }
  }
  
  # Compare fields on common primaryids
  setkey(old_demo, primaryid)
  setkey(new_demo, primaryid)
  
  cols_to_check <- intersect(intersect(DEMO_COMPARE_COLS, names(old_demo)), names(new_demo))
  
  field_reasons <- list(
    caseid = "A common report changing caseid is unusual and may mean a version-selection mismatch.",
    quarter = "Quarter shifts can be expected if the newer pipeline keeps a later surviving record.",
    fda_dt = "Can change if a later case version survives or if date ordering/cleaning differs.",
    rept_dt = "Can change if a later case version survives or if date cleaning differs.",
    event_dt = "Can change if the surviving version changed; widespread shifts deserve review.",
    caseversion = "Likely due to later surviving case versions in the new pipeline.",
    sex = "Could reflect differences in sex cleaning behavior; large changes deserve review.",
    age_in_days = "Could reflect revised year-to-day conversion, DEC handling, or other age-cleaning changes.",
    age_grp = "If age_grp changes, the age difference is more substantial than a tiny conversion-constant refinement.",
    wt_in_kgs = "Could reflect weight cleaning/unit conversion differences.",
    reporter_country = "Could reflect country-standardization differences.",
    premarketing = "Could reflect changed drug standardization, better dictionary coverage, or trial parsing differences.",
    literature = "Could reflect changed lit_ref cleaning or a different surviving version.",
    RB_duplicates = "Could reflect the extra quarter introducing new duplicate matches, or changed PT/drug standardization / NA grouping / fda_dt ordering in RB dedup.",
    RB_duplicates_only_susp = "Could reflect the extra quarter introducing new duplicate matches, or changed suspect-drug/PT signatures."
  )
  
  for (col in cols_to_check) {
    old_col <- old_demo[J(common_pids), .(primaryid, old_val = as_cmp_char(get(col)))]
    new_col <- new_demo[J(common_pids), .(primaryid, new_val = as_cmp_char(get(col)))]
    
    cmp <- merge(old_col, new_col, by = "primaryid")
    diffs <- cmp[
      (is.na(old_val) & !is.na(new_val)) |
        (!is.na(old_val) & is.na(new_val)) |
        (!is.na(old_val) & !is.na(new_val) & old_val != new_val)
    ]
    
    if (nrow(diffs)) {
      expectation <- if (col %in% c(
        "quarter", "caseversion", "fda_dt", "rept_dt",
        "RB_duplicates", "RB_duplicates_only_susp",
        "age_in_days", "premarketing", "literature"
      )) "expected" else "review"
      
      sev <- if (col %in% c("caseid", "sex", "wt_in_kgs")) "warn" else "info"
      
      add_finding(
        expectation = expectation,
        severity = sev,
        table_name = "DEMO",
        metric = paste0("common_pid_field_diff:", col),
        n_affected = nrow(diffs),
        likely_reason = field_reasons[[col]],
        review_hint = paste0("sample_demo_diff_", col, ".csv")
      )
      
      write_sample(diffs, paste0("sample_demo_diff_", col, ".csv"))
    }
    
    rm(old_col, new_col, cmp, diffs)
    gc()
  }
  
  # Age refinement clue
  if (all(c("age_in_days", "age_grp") %in% names(old_demo)) &&
      all(c("age_in_days", "age_grp") %in% names(new_demo))) {
    old_age <- old_demo[J(common_pids), .(
      primaryid,
      old_age = as_cmp_char(age_in_days),
      old_age_grp = as_cmp_char(age_grp)
    )]
    new_age <- new_demo[J(common_pids), .(
      primaryid,
      new_age = as_cmp_char(age_in_days),
      new_age_grp = as_cmp_char(age_grp)
    )]
    
    age_cmp <- merge(old_age, new_age, by = "primaryid")
    age_diff <- age_cmp[
      (is.na(old_age) & !is.na(new_age)) |
        (!is.na(old_age) & is.na(new_age)) |
        (!is.na(old_age) & !is.na(new_age) & old_age != new_age)
    ]
    
    if (nrow(age_diff)) {
      same_grp_n <- age_diff[
        (is.na(old_age_grp) & is.na(new_age_grp)) |
          (!is.na(old_age_grp) & !is.na(new_age_grp) & old_age_grp == new_age_grp),
        .N
      ]
      
      add_finding(
        expectation = "info",
        severity = "info",
        table_name = "DEMO",
        metric = "age_in_days_diff_with_same_age_grp",
        n_affected = same_grp_n,
        likely_reason = "If most age_in_days changes leave age_grp unchanged, that points toward conversion-factor refinement rather than substantive age-classification drift."
      )
    }
    
    rm(old_age, new_age, age_cmp, age_diff)
    gc()
  }
  
  invisible(list(
    common_pids = common_pids,
    old_only_pids = old_only_pids,
    new_only_pids = new_only_pids
  ))
}

# -------------------------- table-by-table summaries ------------------------- #

compare_table_basic <- function(tbl_name, old_dir, new_dir) {
  add_report("")
  add_report("### Basic comparison for ", tbl_name)
  
  old_dt <- load_table(old_dir, tbl_name)
  new_dt <- load_table(new_dir, tbl_name)
  
  if (is.null(old_dt) && is.null(new_dt)) return(invisible(NULL))
  
  if (is.null(old_dt)) {
    add_finding(
      expectation = "review",
      severity = "warn",
      table_name = tbl_name,
      metric = "table_missing_in_old",
      likely_reason = "Table exists only in new database.",
      review_hint = tbl_name
    )
    return(invisible(NULL))
  }
  
  if (is.null(new_dt)) {
    add_finding(
      expectation = "review",
      severity = "warn",
      table_name = tbl_name,
      metric = "table_missing_in_new",
      likely_reason = "Table exists only in old database.",
      review_hint = tbl_name
    )
    return(invisible(NULL))
  }
  
  old_meta <- summarize_table(old_dt, tbl_name)
  new_meta <- summarize_table(new_dt, tbl_name)
  
  table_summary <<- rbindlist(list(
    table_summary,
    data.table(
      table_name = tbl_name,
      old_rows = old_meta$nrow,
      new_rows = new_meta$nrow,
      old_cols = old_meta$ncol,
      new_cols = new_meta$ncol,
      old_unique_primaryids = old_meta$unique_primaryids,
      new_unique_primaryids = new_meta$unique_primaryids
    )
  ))
  
  if (old_meta$nrow != new_meta$nrow) {
    add_finding(
      expectation = "expected",
      severity = "info",
      table_name = tbl_name,
      metric = "row_count_difference",
      old_value = old_meta$nrow,
      new_value = new_meta$nrow,
      n_affected = abs(new_meta$nrow - old_meta$nrow),
      likely_reason = if (tbl_name == "DEMO") {
        "Some increase is expected because the new database includes one extra quarter. Large decreases or odd shifts may reflect changed dedup/filter logic."
      } else {
        "Some increase is expected from the extra quarter. For content tables, bigger shifts may also reflect dictionary or row-expansion differences."
      }
    )
  }
  
  compare_schema(old_meta, new_meta, tbl_name)
  
  # lightweight within-database integrity checks for the table itself
  if (tbl_name == "DEMO") {
    for (which in list(
      list(meta = old_meta, label = "OLD_DEMO", hint = "Check old finalize output."),
      list(meta = new_meta, label = "NEW_DEMO", hint = "Check new finalize output.")
    )) {
      if (is.na(which$meta$unique_primaryids)) {
        add_finding(
          expectation = "review",
          severity = "high",
          table_name = which$label,
          metric = "primaryid_column_missing",
          likely_reason = paste(which$label, "does not have a primaryid column."),
          review_hint = which$hint
        )
      } else if (which$meta$unique_primaryids != which$meta$nrow) {
        add_finding(
          expectation = "review",
          severity = "high",
          table_name = which$label,
          metric = "primaryid_unique",
          old_value = which$meta$unique_primaryids,
          new_value = which$meta$nrow,
          likely_reason = paste(which$label, "is not unique by primaryid."),
          review_hint = which$hint
        )
      }
    }
  }
  
  rm(old_dt, new_dt, old_meta, new_meta)
  gc()
  
  invisible(NULL)
}

# ------------------------------- relation checks ----------------------------- #

check_relations_one_db <- function(db_dir, db_label) {
  demo <- load_table(db_dir, "DEMO")
  if (is.null(demo) || !("primaryid" %in% names(demo))) return(invisible(NULL))
  
  demo_pids <- safe_unique_char(demo$primaryid)
  rm(demo)
  gc()
  
  child_tables <- c("DRUG", "DRUG_INFO", "REAC", "INDI", "OUTC", "THER", "RPSR")

  for (tbl in child_tables) {
    # Load only primaryid — avoids materialising full wide tables (e.g. DRUG_INFO)
    dt <- load_table(db_dir, tbl, col_select = "primaryid")
    if (is.null(dt) || !("primaryid" %in% names(dt))) {
      rm(dt)
      gc()
      next
    }

    child_pids <- safe_unique_char(dt$primaryid)
    missing_pids <- setdiff(child_pids, demo_pids)

    if (length(missing_pids)) {
      add_finding(
        expectation = "review",
        severity = "high",
        table_name = paste0(db_label, "_", tbl),
        metric = "primaryids_not_in_demo",
        n_affected = length(missing_pids),
        likely_reason = "Child table contains primaryids absent from DEMO.",
        review_hint = paste0(db_label, "_", tbl)
      )
    } else {
      add_finding(
        expectation = "info",
        severity = "info",
        table_name = paste0(db_label, "_", tbl),
        metric = "primaryids_not_in_demo",
        n_affected = 0L,
        likely_reason = "Child-table primaryids are a subset of DEMO."
      )
    }

    rm(dt, child_pids, missing_pids)
    gc()
  }

  invisible(NULL)
}

# -------------------------- REAC / DRUG / THER compare ----------------------- #

compare_reac_counts <- function(old_dir, new_dir, common_pids) {
  old_reac <- load_table(old_dir, "REAC")
  new_reac <- load_table(new_dir, "REAC")
  if (is.null(old_reac) || is.null(new_reac)) return(invisible(NULL))
  
  add_report("")
  add_report("### REAC comparison")
  
  if (!all(c("primaryid", "pt") %in% names(old_reac)) ||
      !all(c("primaryid", "pt") %in% names(new_reac))) {
    rm(old_reac, new_reac)
    gc()
    return(invisible(NULL))
  }
  
  old_reac <- old_reac[as.character(primaryid) %chin% common_pids]
  new_reac <- new_reac[as.character(primaryid) %chin% common_pids]
  
  if (COMPARE_REAC_ROWCOUNTS_PER_PID) {
    old_n <- old_reac[, .N, by = "primaryid"]
    new_n <- new_reac[, .N, by = "primaryid"]
    nn <- merge(old_n, new_n, by = "primaryid", suffixes = c("_old", "_new"))
    nn_diff <- nn[N_old != N_new]
    
    if (nrow(nn_diff)) {
      add_finding(
        expectation = "expected",
        severity = "info",
        table_name = "REAC",
        metric = "row_count_per_primaryid_diff",
        n_affected = nrow(nn_diff),
        likely_reason = "Can reflect PT cleaning/standardization changes or a different surviving report version.",
        review_hint = "sample_reac_rowcount_diff.csv"
      )
      write_sample(nn_diff, "sample_reac_rowcount_diff.csv")
    }
    
    rm(old_n, new_n, nn, nn_diff)
    gc()
  }
  
  if (COMPARE_REAC_TERM_FREQUENCIES) {
    old_freq <- old_reac[, .N, by = .(pt = as.character(pt))]
    new_freq <- new_reac[, .N, by = .(pt = as.character(pt))]
    
    setnames(old_freq, "N", "N_old")
    setnames(new_freq, "N", "N_new")
    
    freq <- merge(old_freq, new_freq, by = "pt", all = TRUE)
    freq[is.na(N_old), N_old := 0L]
    freq[is.na(N_new), N_new := 0L]
    freq[, abs_diff := abs(N_new - N_old)]
    
    top_diff <- freq[order(-abs_diff)][seq_len(min(.N, TOP_TERM_N))]
    
    add_finding(
      expectation = "expected",
      severity = "info",
      table_name = "REAC",
      metric = "pt_frequency_shift_summary",
      n_affected = nrow(top_diff),
      likely_reason = "Likely driven by PT standardization/manual-fix differences, newer quarter content, or different surviving report versions.",
      review_hint = "sample_reac_pt_frequency_shifts.csv"
    )
    write_sample(top_diff, "sample_reac_pt_frequency_shifts.csv")
    
    rm(old_freq, new_freq, freq, top_diff)
    gc()
  }
  
  if (COMPARE_REAC_SIGNATURES) {
    old_sig <- unique(old_reac[order(as.character(pt)), .(
      pt_signature = paste0(as.character(pt), collapse = "; ")
    ), by = "primaryid"])
    
    new_sig <- unique(new_reac[order(as.character(pt)), .(
      pt_signature = paste0(as.character(pt), collapse = "; ")
    ), by = "primaryid"])
    
    sig_cmp <- merge(old_sig, new_sig, by = "primaryid", suffixes = c("_old", "_new"))
    sig_diff <- sig_cmp[pt_signature_old != pt_signature_new]
    
    if (nrow(sig_diff)) {
      add_finding(
        expectation = "expected",
        severity = "info",
        table_name = "REAC",
        metric = "pt_signature_diff_on_common_primaryids",
        n_affected = nrow(sig_diff),
        likely_reason = "Likely due to PT standardization changes or different surviving report versions.",
        review_hint = "sample_reac_pt_signature_diff.csv"
      )
      write_sample(sig_diff, "sample_reac_pt_signature_diff.csv")
    }
    
    rm(old_sig, new_sig, sig_cmp, sig_diff)
    gc()
  }
  
  rm(old_reac, new_reac)
  gc()
  
  invisible(NULL)
}

compare_drug_counts <- function(old_dir, new_dir, common_pids) {
  old_drug <- load_table(old_dir, "DRUG")
  new_drug <- load_table(new_dir, "DRUG")
  if (is.null(old_drug) || is.null(new_drug)) return(invisible(NULL))
  
  add_report("")
  add_report("### DRUG comparison")
  
  if (!all(c("primaryid", "substance") %in% names(old_drug)) ||
      !all(c("primaryid", "substance") %in% names(new_drug))) {
    rm(old_drug, new_drug)
    gc()
    return(invisible(NULL))
  }
  
  old_drug <- old_drug[as.character(primaryid) %chin% common_pids]
  new_drug <- new_drug[as.character(primaryid) %chin% common_pids]
  
  if (COMPARE_DRUG_ROWCOUNTS_PER_PID) {
    old_n <- old_drug[, .N, by = "primaryid"]
    new_n <- new_drug[, .N, by = "primaryid"]
    nn <- merge(old_n, new_n, by = "primaryid", suffixes = c("_old", "_new"))
    nn_diff <- nn[N_old != N_new]
    
    if (nrow(nn_diff)) {
      add_finding(
        expectation = "expected",
        severity = "info",
        table_name = "DRUG",
        metric = "row_count_per_primaryid_diff",
        n_affected = nrow(nn_diff),
        likely_reason = "Can reflect improved substance dictionary coverage, different multi-substance expansion, or different surviving report versions.",
        review_hint = "sample_drug_rowcount_diff.csv"
      )
      write_sample(nn_diff, "sample_drug_rowcount_diff.csv")
    }
    
    rm(old_n, new_n, nn, nn_diff)
    gc()
  }
  
  if (COMPARE_DRUG_SUBSTANCE_FREQ) {
    old_freq <- old_drug[, .N, by = .(substance = as.character(substance))]
    new_freq <- new_drug[, .N, by = .(substance = as.character(substance))]
    
    setnames(old_freq, "N", "N_old")
    setnames(new_freq, "N", "N_new")
    
    freq <- merge(old_freq, new_freq, by = "substance", all = TRUE)
    freq[is.na(N_old), N_old := 0L]
    freq[is.na(N_new), N_new := 0L]
    freq[, abs_diff := abs(N_new - N_old)]
    
    top_diff <- freq[order(-abs_diff)][seq_len(min(.N, TOP_TERM_N))]
    
    add_finding(
      expectation = "expected",
      severity = "info",
      table_name = "DRUG",
      metric = "substance_frequency_shift_summary",
      n_affected = nrow(top_diff),
      likely_reason = "Likely driven by dictionary improvements, punctuation standardization, the added quarter, or different surviving report versions.",
      review_hint = "sample_drug_substance_frequency_shifts.csv"
    )
    write_sample(top_diff, "sample_drug_substance_frequency_shifts.csv")
    
    rm(old_freq, new_freq, freq, top_diff)
    gc()
  }
  
  if (COMPARE_DRUG_SIGNATURES) {
    old_sig <- unique(old_drug[order(as.character(substance)), .(
      substance_signature = paste0(as.character(substance), collapse = "; ")
    ), by = "primaryid"])
    
    new_sig <- unique(new_drug[order(as.character(substance)), .(
      substance_signature = paste0(as.character(substance), collapse = "; ")
    ), by = "primaryid"])
    
    sig_cmp <- merge(old_sig, new_sig, by = "primaryid", suffixes = c("_old", "_new"))
    sig_diff <- sig_cmp[substance_signature_old != substance_signature_new]
    
    if (nrow(sig_diff)) {
      add_finding(
        expectation = "expected",
        severity = "info",
        table_name = "DRUG",
        metric = "substance_signature_diff_on_common_primaryids",
        n_affected = nrow(sig_diff),
        likely_reason = "Likely due to improved drug dictionary coverage, punctuation standardization, or different surviving report versions.",
        review_hint = "sample_drug_substance_signature_diff.csv"
      )
      write_sample(sig_diff, "sample_drug_substance_signature_diff.csv")
    }
    
    rm(old_sig, new_sig, sig_cmp, sig_diff)
    gc()
  }
  
  if (COMPARE_DRUG_SUSPECTED_SIGNATURES &&
      "role_cod" %in% names(old_drug) && "role_cod" %in% names(new_drug)) {
    old_susp <- old_drug[as.character(role_cod) %chin% c("PS", "SS")]
    new_susp <- new_drug[as.character(role_cod) %chin% c("PS", "SS")]
    
    old_sig <- unique(old_susp[order(as.character(substance)), .(
      suspected_signature = paste0(as.character(substance), collapse = "; ")
    ), by = "primaryid"])
    
    new_sig <- unique(new_susp[order(as.character(substance)), .(
      suspected_signature = paste0(as.character(substance), collapse = "; ")
    ), by = "primaryid"])
    
    sig_cmp <- merge(old_sig, new_sig, by = "primaryid", suffixes = c("_old", "_new"))
    sig_diff <- sig_cmp[suspected_signature_old != suspected_signature_new]
    
    if (nrow(sig_diff)) {
      add_finding(
        expectation = "expected",
        severity = "info",
        table_name = "DRUG",
        metric = "suspected_substance_signature_diff_on_common_primaryids",
        n_affected = nrow(sig_diff),
        likely_reason = "These changes can directly affect RB duplicate matching because suspect-drug signatures are part of the key.",
        review_hint = "sample_drug_suspected_signature_diff.csv"
      )
      write_sample(sig_diff, "sample_drug_suspected_signature_diff.csv")
    }
    
    rm(old_susp, new_susp, old_sig, new_sig, sig_cmp, sig_diff)
    gc()
  }
  
  rm(old_drug, new_drug)
  gc()
  
  invisible(NULL)
}

compare_ther_summary <- function(old_dir, new_dir, common_pids) {
  old_ther <- load_table(old_dir, "THER")
  new_ther <- load_table(new_dir, "THER")
  if (is.null(old_ther) || is.null(new_ther)) return(invisible(NULL))
  
  add_report("")
  add_report("### THER comparison")
  
  if (!("primaryid" %in% names(old_ther)) || !("primaryid" %in% names(new_ther))) {
    rm(old_ther, new_ther)
    gc()
    return(invisible(NULL))
  }
  
  old_ther <- old_ther[as.character(primaryid) %chin% common_pids]
  new_ther <- new_ther[as.character(primaryid) %chin% common_pids]
  
  for (col in intersect(c("dur_in_days", "time_to_onset"), intersect(names(old_ther), names(new_ther)))) {
    old_summary <- old_ther[, .(
      n = .N,
      non_na = sum(!is.na(get(col))),
      mean = suppressWarnings(mean(as.numeric(get(col)), na.rm = TRUE)),
      median = suppressWarnings(median(as.numeric(get(col)), na.rm = TRUE))
    )]
    
    new_summary <- new_ther[, .(
      n = .N,
      non_na = sum(!is.na(get(col))),
      mean = suppressWarnings(mean(as.numeric(get(col)), na.rm = TRUE)),
      median = suppressWarnings(median(as.numeric(get(col)), na.rm = TRUE))
    )]
    
    add_finding(
      expectation = "info",
      severity = "info",
      table_name = "THER",
      metric = paste0("summary_compare:", col),
      old_value = paste(names(old_summary), old_summary, sep = "=", collapse = "; "),
      new_value = paste(names(new_summary), new_summary, sep = "=", collapse = "; "),
      likely_reason = "Differences can reflect date parsing/validation differences or a different surviving report version."
    )
  }
  
  rm(old_ther, new_ther)
  gc()

  invisible(NULL)
}

# -------------------- DRUG_NAME / DOSES / DRUG_SUPP compare ----------------- #

compare_drug_name_coverage <- function(old_dir, new_dir, common_pids) {
  old_dn <- load_table(old_dir, "DRUG_NAME")
  new_dn <- load_table(new_dir, "DRUG_NAME")
  if (is.null(old_dn) || is.null(new_dn)) return(invisible(NULL))

  add_report("")
  add_report("### DRUG_NAME comparison")

  old_dn <- old_dn[as.character(primaryid) %chin% common_pids]
  new_dn <- new_dn[as.character(primaryid) %chin% common_pids]

  coverage_cols <- intersect(
    c("drugname", "prod_ai", "val_vbm", "nda_num"),
    intersect(names(old_dn), names(new_dn))
  )

  for (col in coverage_cols) {
    old_pct <- round(100 * sum(!is.na(old_dn[[col]])) / max(nrow(old_dn), 1L), 1)
    new_pct <- round(100 * sum(!is.na(new_dn[[col]])) / max(nrow(new_dn), 1L), 1)

    add_finding(
      expectation = "info",
      severity    = "info",
      table_name  = "DRUG_NAME",
      metric      = paste0("coverage_pct:", col),
      old_value   = old_pct,
      new_value   = new_pct,
      likely_reason = "Coverage differences reflect dictionary improvements or different surviving report versions."
    )
  }

  rm(old_dn, new_dn)
  gc()
  invisible(NULL)
}

compare_doses_summary <- function(old_dir, new_dir, common_pids) {
  old_d <- load_table(old_dir, "DOSES")
  new_d <- load_table(new_dir, "DOSES")
  if (is.null(old_d) || is.null(new_d)) return(invisible(NULL))

  add_report("")
  add_report("### DOSES comparison")

  old_d <- old_d[as.character(primaryid) %chin% common_pids]
  new_d <- new_d[as.character(primaryid) %chin% common_pids]

  coverage_cols <- intersect(
    c("dose_vbm", "cum_dose_unit", "cum_dose_chr", "dose_amt", "dose_unit", "dose_freq"),
    intersect(names(old_d), names(new_d))
  )

  for (col in coverage_cols) {
    old_pct <- round(100 * sum(!is.na(old_d[[col]])) / max(nrow(old_d), 1L), 1)
    new_pct <- round(100 * sum(!is.na(new_d[[col]])) / max(nrow(new_d), 1L), 1)

    add_finding(
      expectation = "info",
      severity    = "info",
      table_name  = "DOSES",
      metric      = paste0("coverage_pct:", col),
      old_value   = old_pct,
      new_value   = new_pct,
      likely_reason = "Coverage differences expected from improved dose standardization."
    )
  }

  for (col in intersect(c("dose_unit", "dose_freq"), intersect(names(old_d), names(new_d)))) {
    old_freq <- old_d[, .N, by = col][order(-N)][seq_len(min(.N, TOP_TERM_N))]
    new_freq <- new_d[, .N, by = col][order(-N)][seq_len(min(.N, TOP_TERM_N))]
    setnames(old_freq, "N", "N_old")
    setnames(new_freq, "N", "N_new")
    freq <- merge(old_freq, new_freq, by = col, all = TRUE)
    freq[is.na(N_old), N_old := 0L]
    freq[is.na(N_new), N_new := 0L]
    freq[, abs_diff := abs(N_new - N_old)]
    setorderv(freq, "abs_diff", order = -1L)

    add_finding(
      expectation   = "expected",
      severity      = "info",
      table_name    = "DOSES",
      metric        = paste0(col, "_frequency_shift_summary"),
      n_affected    = freq[abs_diff > 0L, .N],
      likely_reason = "Differences reflect dose standardization improvements.",
      review_hint   = paste0("sample_doses_", col, "_freq.csv")
    )
    write_sample(freq, paste0("sample_doses_", col, "_freq.csv"))
    rm(old_freq, new_freq, freq)
  }

  rm(old_d, new_d)
  gc()
  invisible(NULL)
}

compare_drug_supp_summary <- function(old_dir, new_dir, common_pids) {
  old_ds <- load_table(old_dir, "DRUG_SUPP")
  new_ds <- load_table(new_dir, "DRUG_SUPP")
  if (is.null(old_ds) || is.null(new_ds)) return(invisible(NULL))

  add_report("")
  add_report("### DRUG_SUPP comparison")

  old_ds <- old_ds[as.character(primaryid) %chin% common_pids]
  new_ds <- new_ds[as.character(primaryid) %chin% common_pids]

  coverage_cols <- intersect(
    c("route", "dose_form", "dechal", "rechal", "lot_num", "exp_dt"),
    intersect(names(old_ds), names(new_ds))
  )

  for (col in coverage_cols) {
    old_pct <- round(100 * sum(!is.na(old_ds[[col]])) / max(nrow(old_ds), 1L), 1)
    new_pct <- round(100 * sum(!is.na(new_ds[[col]])) / max(nrow(new_ds), 1L), 1)

    add_finding(
      expectation = "info",
      severity    = "info",
      table_name  = "DRUG_SUPP",
      metric      = paste0("coverage_pct:", col),
      old_value   = old_pct,
      new_value   = new_pct,
      likely_reason = "Coverage differences expected from improved route/form standardization."
    )
  }

  # Frequency distributions for route and dose_form
  for (col in intersect(c("route", "dose_form"), intersect(names(old_ds), names(new_ds)))) {
    old_freq <- old_ds[, .N, by = col][order(-N)][seq_len(min(.N, TOP_TERM_N))]
    new_freq <- new_ds[, .N, by = col][order(-N)][seq_len(min(.N, TOP_TERM_N))]
    setnames(old_freq, "N", "N_old")
    setnames(new_freq, "N", "N_new")
    freq <- merge(old_freq, new_freq, by = col, all = TRUE)
    freq[is.na(N_old), N_old := 0L]
    freq[is.na(N_new), N_new := 0L]
    freq[, abs_diff := abs(N_new - N_old)]
    setorderv(freq, "abs_diff", order = -1L)

    add_finding(
      expectation   = "expected",
      severity      = "info",
      table_name    = "DRUG_SUPP",
      metric        = paste0(col, "_frequency_shift_summary"),
      n_affected    = freq[abs_diff > 0L, .N],
      likely_reason = paste0("Differences reflect ", col, " standardization changes."),
      review_hint   = paste0("sample_drug_supp_", col, "_freq.csv")
    )
    write_sample(freq, paste0("sample_drug_supp_", col, "_freq.csv"))
    rm(old_freq, new_freq, freq)
  }

  # dechal / rechal value distributions
  for (col in intersect(c("dechal", "rechal"), intersect(names(old_ds), names(new_ds)))) {
    old_vals <- old_ds[, .N, by = col][order(col)]
    new_vals <- new_ds[, .N, by = col][order(col)]

    fmt_dist <- function(dt, n_col) {
      paste(
        apply(dt, 1, function(r) paste0(ifelse(is.na(r[[1]]), "NA", r[[1]]), "=", r[[2]])),
        collapse = "; "
      )
    }

    add_finding(
      expectation   = "expected",
      severity      = "info",
      table_name    = "DRUG_SUPP",
      metric        = paste0(col, "_value_distribution"),
      old_value     = fmt_dist(old_vals),
      new_value     = fmt_dist(new_vals),
      likely_reason = paste0(col, " standardization restricts values to Y/N/D; other values become NA.")
    )
    rm(old_vals, new_vals)
  }

  rm(old_ds, new_ds)
  gc()
  invisible(NULL)
}

# ------------------------------- main ---------------------------------------- #

dir_create_safe(OUT_DIR)

old_tables <- list_tables_in_dir(OLD_RDS_DIR)
new_tables <- list_tables_in_dir(NEW_RDS_DIR)

all_tables <- union(TABLE_PRIORITY, union(old_tables, new_tables))
all_tables <- unique(c(intersect(TABLE_PRIORITY, all_tables), setdiff(all_tables, TABLE_PRIORITY)))

add_report("Old dir (RDS):  ", OLD_RDS_DIR)
add_report("New dir (", new_data_format, "): ", NEW_RDS_DIR)
add_report("")
add_report("Old tables: ", paste(sort(old_tables), collapse = ", "))
add_report("New tables: ", paste(sort(new_tables), collapse = ", "))

# Cross-table availability
old_only_tables <- setdiff(old_tables, new_tables)
new_only_tables <- setdiff(new_tables, old_tables)

if (length(old_only_tables)) {
  add_finding(
    expectation = "review",
    severity = "warn",
    table_name = "DATABASE",
    metric = "tables_missing_in_new",
    n_affected = length(old_only_tables),
    likely_reason = "Could be intentional redesign of outputs, but should be explicit.",
    review_hint = paste(old_only_tables, collapse = ", ")
  )
}

if (length(new_only_tables)) {
  add_finding(
    expectation = "review",
    severity = "warn",
    table_name = "DATABASE",
    metric = "tables_new_in_new",
    n_affected = length(new_only_tables),
    likely_reason = "Could be intentional enrichment, but should be explicit.",
    review_hint = paste(new_only_tables, collapse = ", ")
  )
}

# Lightweight per-table summary/schema comparison
for (tbl in intersect(old_tables, new_tables)) {
  compare_table_basic(tbl, OLD_RDS_DIR, NEW_RDS_DIR)
}

# Within-database relation checks, one DB at a time
add_report("")
add_report("### Within-database relation checks")
check_relations_one_db(OLD_RDS_DIR, "OLD")
check_relations_one_db(NEW_RDS_DIR, "NEW")

# Detailed DEMO comparison
old_demo <- load_table(OLD_RDS_DIR, "DEMO")
new_demo <- load_table(NEW_RDS_DIR, "DEMO")

if (is.null(old_demo) || is.null(new_demo)) {
  stop("Both old and new directories must contain a DEMO table for detailed comparison.", call. = FALSE)
}

demo_cmp <- compare_demo(old_demo, new_demo)
common_pids <- demo_cmp$common_pids

rm(old_demo, new_demo)
gc()

# Optional child-table comparisons on common primaryids
if (COMPARE_REAC_ROWCOUNTS_PER_PID || COMPARE_REAC_TERM_FREQUENCIES || COMPARE_REAC_SIGNATURES) {
  compare_reac_counts(OLD_RDS_DIR, NEW_RDS_DIR, common_pids)
}

if (COMPARE_DRUG_ROWCOUNTS_PER_PID || COMPARE_DRUG_SUBSTANCE_FREQ ||
    COMPARE_DRUG_SIGNATURES || COMPARE_DRUG_SUSPECTED_SIGNATURES) {
  compare_drug_counts(OLD_RDS_DIR, NEW_RDS_DIR, common_pids)
}

if (COMPARE_THER_SUMMARY) {
  compare_ther_summary(OLD_RDS_DIR, NEW_RDS_DIR, common_pids)
}

if (COMPARE_DRUG_NAME_COVERAGE) {
  compare_drug_name_coverage(OLD_RDS_DIR, NEW_RDS_DIR, common_pids)
}

if (COMPARE_DOSES_COVERAGE) {
  compare_doses_summary(OLD_RDS_DIR, NEW_RDS_DIR, common_pids)
}

if (COMPARE_DRUG_SUPP_COVERAGE) {
  compare_drug_supp_summary(OLD_RDS_DIR, NEW_RDS_DIR, common_pids)
}

# ------------------------------- save outputs -------------------------------- #

setorder(findings, expectation, -n_affected, table_name, metric, na.last = TRUE)

fwrite(findings, file.path(OUT_DIR, "findings.csv"))
fwrite(table_summary, file.path(OUT_DIR, "table_summary.csv"))
fwrite(schema_diffs, file.path(OUT_DIR, "schema_differences.csv"))

summary_lines <- c(
  "FAERS database comparison report (low-memory version)",
  paste0("Old dir (RDS):  ", OLD_RDS_DIR),
  paste0("New dir (", new_data_format, "): ", NEW_RDS_DIR),
  "",
  "Interpretation guide:",
  "  [EXPECTED] = difference is plausibly explained by the extra quarter or known intentional pipeline improvements.",
  "  [REVIEW]   = difference might be justified, but it deserves human inspection.",
  "  [INFO]     = descriptive checkpoint.",
  "",
  "This version is designed to avoid memory blowups:",
  "  - one table loaded at a time",
  "  - only DEMO old/new are held together for detailed comparison",
  "  - expensive REAC/DRUG full-signature joins are OFF by default",
  "",
  paste0("Total findings: ", nrow(findings)),
  paste0("Expected findings: ", findings[expectation == 'expected', .N]),
  paste0("Review findings: ", findings[expectation == 'review', .N]),
  paste0("Info findings: ", findings[expectation == 'info', .N]),
  "",
  "Detailed log:",
  report_lines
)

writeLines(summary_lines, con = file.path(OUT_DIR, "report.txt"))

add_report("")
add_report("Done.")
add_report("Outputs written to: ", OUT_DIR)
add_report("  - findings.csv")
add_report("  - table_summary.csv")
add_report("  - schema_differences.csv")
add_report("  - report.txt")
