# ------------------------------------------------------------------------------
# Script: 02_ingest_faers.R
# Purpose:
#   Read extracted FAERS ASCII TXT files, harmonize column names, keep only
#   required columns, and write quarter-level staged parquet files.
#
# Inputs:
#   - data_raw/faers_ascii/**/**/*.txt
#
# Outputs:
#   - data_stage/<TABLE>/<TABLE>_<YYQ#>.parquet
#   - data_stage/faers_file_inventory.csv
#   - data_stage/faers_ingest_manifest.csv
#
# Notes:
#   - This is the staging layer only.
#   - Text patching is NOT done here; run the dedicated patch script first.
#   - It does not standardize PTs, drug names, dates, age, or deduplicate cases.
#   - All staged columns are written as character to keep schema stable.
#   - DRUG is staged wide once; downstream scripts can derive DRUG and DRUG_INFO.
# ------------------------------------------------------------------------------

suppressPackageStartupMessages({
  library(data.table)
  library(arrow)
})

options(timeout = 1200)

# ------------------------------- configuration ------------------------------- #

BASE_DIR <- "."
source(file.path(BASE_DIR, "R", "utils.R"))

ASCII_ROOT    <- file.path(BASE_DIR, "data_raw", "faers_ascii")
STAGE_ROOT    <- file.path(BASE_DIR, "data_stage")
INVENTORY_CSV <- file.path(STAGE_ROOT, "faers_file_inventory.csv")
MANIFEST_CSV  <- file.path(STAGE_ROOT, "faers_ingest_manifest.csv")
SCHEMA_WARNINGS_CSV <- file.path(STAGE_ROOT, "faers_schema_warnings.csv")
COLUMN_PROFILES_CSV <- file.path(STAGE_ROOT, "faers_unexpected_column_profiles.csv")
max_profile_examples <- 5L
max_profile_chars <- 80L

overwrite_stage <- FALSE
parquet_compression <- "zstd"
fail_on_discarded_columns <- TRUE

# ------------------------------- helpers ------------------------------------- #

truncate_text <- function(x, max_chars = 80L) {
  x <- as.character(x)
  x[is.na(x)] <- NA_character_
  
  too_long <- !is.na(x) & nchar(x) > max_chars
  x[too_long] <- paste0(substr(x[too_long], 1L, max_chars - 3L), "...")
  x
}

profile_columns <- function(dt, file_path, table_name, quarter, column_names,
                            stage,
                            max_examples = 5L,
                            max_chars = 80L) {
  if (!length(column_names)) {
    return(data.table(
      timestamp = character(),
      file_path = character(),
      table_name = character(),
      quarter = character(),
      stage = character(),
      column_name = character(),
      n_non_missing = integer(),
      n_unique_non_missing = integer(),
      example_values = character()
    ))
  }
  
  out <- rbindlist(
    lapply(column_names, function(col) {
      vals <- dt[[col]]
      vals_chr <- trimws(as.character(vals))
      vals_chr[vals_chr == ""] <- NA_character_
      
      non_missing <- vals_chr[!is.na(vals_chr)]
      uniq_vals <- unique(non_missing)
      uniq_vals <- truncate_text(uniq_vals, max_chars = max_chars)
      
      examples <- head(uniq_vals, max_examples)
      example_text <- if (length(examples)) {
        paste(examples, collapse = " | ")
      } else {
        NA_character_
      }
      
      data.table(
        timestamp = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
        file_path = file_path,
        table_name = table_name,
        quarter = quarter,
        stage = stage,
        column_name = col,
        n_non_missing = length(non_missing),
        n_unique_non_missing = length(uniq_vals),
        example_values = example_text
      )
    }),
    fill = TRUE,
    use.names = TRUE
  )
  
  out
}

load_column_profiles <- function(path) {
  needed <- c(
    "timestamp", "file_path", "table_name", "quarter", "stage",
    "column_name", "n_non_missing", "n_unique_non_missing", "example_values"
  )
  
  if (!file.exists(path)) {
    return(data.table(
      timestamp = character(),
      file_path = character(),
      table_name = character(),
      quarter = character(),
      stage = character(),
      column_name = character(),
      n_non_missing = integer(),
      n_unique_non_missing = integer(),
      example_values = character()
    ))
  }
  
  dt <- fread(path, na.strings = c("", "NA"))
  setDT(dt)
  
  missing_cols <- setdiff(needed, names(dt))
  for (nm in missing_cols) dt[, (nm) := NA]
  
  dt[, timestamp := as.character(timestamp)]
  dt[, file_path := as.character(file_path)]
  dt[, table_name := as.character(table_name)]
  dt[, quarter := as.character(quarter)]
  dt[, stage := as.character(stage)]
  dt[, column_name := as.character(column_name)]
  dt[, n_non_missing := as.integer(n_non_missing)]
  dt[, n_unique_non_missing := as.integer(n_unique_non_missing)]
  dt[, example_values := as.character(example_values)]
  
  dt[, ..needed]
}

append_column_profiles <- function(profiles_dt, new_rows) {
  if (nrow(new_rows) == 0L) return(profiles_dt)
  if (nrow(profiles_dt) == 0L) return(copy(new_rows))
  rbindlist(list(profiles_dt, new_rows), fill = TRUE, use.names = TRUE)
}

message_column_profiles <- function(profile_dt) {
  if (nrow(profile_dt) == 0L) return(invisible(NULL))
  
  for (i in seq_len(nrow(profile_dt))) {
    message(sprintf(
      paste0(
        "Unexpected column profile | table=%s | quarter=%s | stage=%s | ",
        "column=%s | non_missing=%s | unique_non_missing=%s | examples=%s"
      ),
      profile_dt$table_name[i],
      profile_dt$quarter[i],
      profile_dt$stage[i],
      profile_dt$column_name[i],
      ifelse(is.na(profile_dt$n_non_missing[i]), "NA", profile_dt$n_non_missing[i]),
      ifelse(is.na(profile_dt$n_unique_non_missing[i]), "NA", profile_dt$n_unique_non_missing[i]),
      ifelse(is.na(profile_dt$example_values[i]), "<all missing>", profile_dt$example_values[i])
    ))
  }
  
  invisible(NULL)
}

is_blank_or_na <- function(x) {
  x_chr <- trimws(as.character(x))
  is.na(x_chr) | x_chr == ""
}

find_all_missing_v_columns <- function(dt) {
  nms <- names(dt)
  v_cols <- nms[grepl("^v[0-9]+$", nms, perl = TRUE)]
  
  if (!length(v_cols)) return(character())
  
  keep_artifacts <- vapply(
    v_cols,
    function(nm) all(is_blank_or_na(dt[[nm]])),
    logical(1)
  )
  
  v_cols[keep_artifacts]
}

drop_columns_if_present <- function(dt, cols) {
  cols <- intersect(cols, names(dt))
  if (!length(cols)) return(dt)
  dt[, (cols) := NULL]
  dt
}


message_ignored_artifact_columns <- function(path, table_name, quarter, column_names,
                                             stage,
                                             notes = "All-missing placeholder columns from fread fill behavior.") {
  if (!length(column_names)) return(invisible(NULL))
  
  message(sprintf(
    paste0(
      "Ignoring artifact column(s) | table=%s | quarter=%s | stage=%s | ",
      "file=%s | columns=%s"
    ),
    table_name,
    quarter,
    stage,
    path,
    paste(column_names, collapse = ", ")
  ))
  
  if (!is.na(notes) && nzchar(notes)) {
    message("  reason: ", notes)
  }
  
  invisible(NULL)
}

extract_quarter_yyq <- function(x) {
  s <- toupper(paste(x, collapse = " "))
  
  m1 <- regexpr("((19|20)[0-9]{2})Q[1-4]", s, perl = TRUE)
  if (m1[1] != -1L) {
    hit <- regmatches(s, m1)
    yr4 <- sub("Q[1-4]$", "", hit)
    qtr <- sub("^.*(Q[1-4])$", "\\1", hit)
    return(paste0(substr(yr4, 3, 4), qtr))
  }
  
  m2 <- regexpr("[0-9]{2}Q[1-4]", s, perl = TRUE)
  if (m2[1] != -1L) {
    return(regmatches(s, m2))
  }
  
  NA_character_
}

table_from_path <- function(path) {
  b <- basename(path)
  
  if (grepl("(?i)^demo", b, perl = TRUE))    return("DEMO")
  if (grepl("(?i)^drug", b, perl = TRUE))    return("DRUG")
  if (grepl("(?i)^indi", b, perl = TRUE))    return("INDI")
  if (grepl("(?i)^outc", b, perl = TRUE))    return("OUTC")
  if (grepl("(?i)^reac", b, perl = TRUE))    return("REAC")
  if (grepl("(?i)^rpsr", b, perl = TRUE))    return("RPSR")
  if (grepl("(?i)^ther", b, perl = TRUE))    return("THER")
  if (grepl("(?i)^delete", b, perl = TRUE))  return("DELETED")
  
  NA_character_
}

normalize_empty_strings <- function(dt) {
  char_cols <- names(dt)[vapply(dt, is.character, logical(1))]
  for (j in char_cols) {
    idx <- which(dt[[j]] == "")
    if (length(idx)) {
      set(dt, i = idx, j = j, value = NA_character_)
    }
  }
  dt
}

convert_all_to_character <- function(dt) {
  for (j in names(dt)) {
    set(dt, j = j, value = as.character(dt[[j]]))
  }
  dt
}

coalesce_duplicate_names <- function(dt) {
  nms <- names(dt)
  dup_targets <- unique(nms[duplicated(nms)])
  
  if (!length(dup_targets)) return(dt)
  
  for (nm in dup_targets) {
    idx <- which(names(dt) == nm)
    target_idx <- idx[1L]
    
    if (length(idx) > 1L) {
      for (j in idx[-1L]) {
        target_vals <- dt[[target_idx]]
        repl_vals <- dt[[j]]
        
        miss <- is.na(target_vals) | target_vals == ""
        if (any(miss)) {
          set(dt, i = which(miss), j = target_idx, value = repl_vals[miss])
        }
      }
      
      keep_idx <- setdiff(seq_along(dt), idx[-1L])
      dt <- dt[, ..keep_idx]
    }
  }
  
  dt
}

load_manifest <- function(path) {
  needed <- c(
    "table_name", "quarter", "out_file", "n_source", "n_rows",
    "status", "timestamp", "notes"
  )
  
  if (!file.exists(path)) {
    return(data.table(
      table_name = character(),
      quarter    = character(),
      out_file   = character(),
      n_source   = integer(),
      n_rows     = integer(),
      status     = character(),
      timestamp  = character(),
      notes      = character()
    ))
  }
  
  dt <- fread(path, na.strings = c("", "NA"))
  setDT(dt)
  
  missing_cols <- setdiff(needed, names(dt))
  for (nm in missing_cols) dt[, (nm) := NA]
  
  dt <- dt[, ..needed]
  
  # Force stable column types
  dt[, table_name := as.character(table_name)]
  dt[, quarter    := as.character(quarter)]
  dt[, out_file   := as.character(out_file)]
  dt[, n_source   := as.integer(n_source)]
  dt[, n_rows     := as.integer(n_rows)]
  dt[, status     := as.character(status)]
  dt[, timestamp  := as.character(timestamp)]
  dt[, notes      := as.character(notes)]
  
  dt
}
save_manifest <- function(dt, path) {
  fwrite(dt, path)
}
validate_table_specs <- function(specs) {
  for (tbl in names(specs)) {
    spec <- specs[[tbl]]
    
    if (is.null(spec$exclude)) {
      spec$exclude <- character()
      specs[[tbl]] <- spec
    }
    
    overlap <- intersect(spec$keep, spec$exclude)
    if (length(overlap)) {
      stop(
        sprintf(
          "TABLE_SPECS[%s] has columns present in both keep and exclude: %s",
          tbl, paste(overlap, collapse = ", ")
        ),
        call. = FALSE
      )
    }
  }
  
  invisible(TRUE)
}
upsert_manifest_row <- function(manifest, row_dt) {
  setDT(manifest)
  setDT(row_dt)
  
  key_cols <- c("table_name", "quarter", "out_file")
  
  # enforce row_dt types too
  row_dt[, table_name := as.character(table_name)]
  row_dt[, quarter    := as.character(quarter)]
  row_dt[, out_file   := as.character(out_file)]
  row_dt[, n_source   := as.integer(n_source)]
  row_dt[, n_rows     := as.integer(n_rows)]
  row_dt[, status     := as.character(status)]
  row_dt[, timestamp  := as.character(timestamp)]
  row_dt[, notes      := as.character(notes)]
  
  if (nrow(manifest) == 0L) return(copy(row_dt))
  
  manifest_key <- do.call(paste, c(manifest[, ..key_cols], sep = "\r"))
  row_key <- do.call(paste, c(row_dt[, ..key_cols], sep = "\r"))
  idx <- match(row_key, manifest_key)
  
  if (is.na(idx)) {
    manifest <- rbindlist(list(manifest, row_dt), fill = TRUE, use.names = TRUE)
  } else {
    for (nm in names(row_dt)) {
      set(manifest, i = idx, j = nm, value = row_dt[[nm]])
    }
  }
  
  manifest[]
}
read_deleted_ascii <- function(path) {
  first_line <- readLines(path, n = 1L, warn = FALSE, encoding = "UTF-8")
  first_line_norm <- trimws(tolower(first_line))
  
  has_header <- identical(first_line_norm, "caseid")
  
  dt <- tryCatch(
    {
      data.table::fread(
        file = path,
        sep = "$",
        header = has_header,
        quote = "",
        comment.char = "",
        na.strings = "",
        fill = Inf,
        showProgress = TRUE,
        data.table = TRUE
      )
    },
    finally = {
      try(closeAllConnections(), silent = TRUE)
    }
  )
  
  setDT(dt)
  
  if (has_header) {
    setnames(dt, normalize_names(names(dt)))
  } else {
    if (ncol(dt) != 1L) {
      stop(
        sprintf(
          "DELETE file without header did not read as a single column: %s (ncol=%d)",
          path, ncol(dt)
        ),
        call. = FALSE
      )
    }
    setnames(dt, "caseid")
  }
  
  dt
}

load_schema_warnings <- function(path) {
  if (!file.exists(path)) {
    return(data.table(
      timestamp = character(),
      file_path = character(),
      table_name = character(),
      quarter = character(),
      warning_type = character(),
      column_name = character(),
      notes = character()
    ))
  }
  
  dt <- fread(path, na.strings = c("", "NA"))
  setDT(dt)
  
  needed <- c(
    "timestamp", "file_path", "table_name", "quarter",
    "warning_type", "column_name", "notes"
  )
  
  missing_cols <- setdiff(needed, names(dt))
  for (nm in missing_cols) dt[, (nm) := NA_character_]
  
  dt[, ..needed]
}

append_schema_warnings <- function(warnings_dt, new_rows) {
  if (nrow(warnings_dt) == 0L) return(copy(new_rows))
  rbindlist(list(warnings_dt, new_rows), fill = TRUE, use.names = TRUE)
}

log_schema_warning <- function(warnings_dt, file_path, table_name, quarter,
                               warning_type, column_names, notes = NA_character_) {
  if (!length(column_names)) return(warnings_dt)
  
  new_rows <- data.table(
    timestamp = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
    file_path = file_path,
    table_name = table_name,
    quarter = quarter,
    warning_type = warning_type,
    column_name = column_names,
    notes = notes
  )
  
  append_schema_warnings(warnings_dt, new_rows)
}
report_discarded_columns <- function(path, table_name, quarter, column_names,
                                     discard_stage,
                                     notes = NA_character_) {
  if (!length(column_names)) return(invisible(NULL))
  
  msg <- sprintf(
    paste0(
      "Discarded column(s) detected | ",
      "table=%s | quarter=%s | stage=%s | file=%s | columns=%s"
    ),
    table_name,
    quarter,
    discard_stage,
    path,
    paste(column_names, collapse = ", ")
  )
  
  message(msg)
  
  if (!is.na(notes) && nzchar(notes)) {
    message("  reason: ", notes)
  }
  
  warning(msg, call. = FALSE)
  invisible(msg)
}
abort_on_discarded_columns <- function(path, table_name, quarter, column_names,
                                       discard_stage,
                                       fail_on_discarded = FALSE) {
  if (!isTRUE(fail_on_discarded) || !length(column_names)) {
    return(invisible(NULL))
  }
  
  stop(
    sprintf(
      paste0(
        "Stopping because discarded column(s) were detected | ",
        "table=%s | quarter=%s | stage=%s | file=%s | columns=%s"
      ),
      table_name,
      quarter,
      discard_stage,
      path,
      paste(column_names, collapse = ", ")
    ),
    call. = FALSE
  )
}
# ------------------------------- schema specs -------------------------------- #

TABLE_SPECS <- list(
  
  DEMO = list(
    rename = c(
      isr              = "primaryid",
      case             = "caseid",
      foll_seq         = "caseversion",
      i_f_cod          = "i_f_cod",
      i_f_code         = "i_f_cod",
      event_dt         = "event_dt",
      mfr_dt           = "mfr_dt",
      fda_dt           = "fda_dt",
      init_fda_dt      = "init_fda_dt",
      rept_cod         = "rept_cod",
      mfr_num          = "mfr_num",
      mfr_sndr         = "mfr_sndr",
      age              = "age",
      age_cod          = "age_cod",
      age_grp          = "age_grp",
      gndr_cod         = "sex",
      sex              = "sex",
      e_sub            = "e_sub",
      wt               = "wt",
      wt_cod           = "wt_cod",
      rept_dt          = "rept_dt",
      occp_cod         = "occp_cod",
      to_mfr           = "to_mfr",
      reporter_country = "reporter_country",
      occr_country     = "occr_country",
      auth_num         = "auth_num",
      lit_ref          = "lit_ref"
    ),
    keep = c(
      "primaryid","caseid","caseversion","i_f_cod","sex","age","age_cod","age_grp",
      "wt","wt_cod","reporter_country","occr_country","event_dt","rept_dt",
      "mfr_dt","init_fda_dt","fda_dt","rept_cod","occp_cod","mfr_num",
      "mfr_sndr","to_mfr","e_sub","auth_num","lit_ref"
    ),
    exclude = c("image","death_dt","confid")
  ),
  
  DRUG = list(
    rename = c(
      isr            = "primaryid",
      drug_seq       = "drug_seq",
      role_cod       = "role_cod",
      drugname       = "drugname",
      prod_ai        = "prod_ai",
      val_vbm        = "val_vbm",
      route          = "route",
      dose_vbm       = "dose_vbm",
      dechal         = "dechal",
      rechal         = "rechal",
      lot_num        = "lot_num",
      lot_nbr        = "lot_num",
      nda_num        = "nda_num",
      exp_dt         = "exp_dt",
      dose_form      = "dose_form",
      dose_freq      = "dose_freq",
      cum_dose_unit  = "cum_dose_unit",
      cum_dose_chr   = "cum_dose_chr",
      dose_amt       = "dose_amt",
      dose_unit      = "dose_unit"
    ),
    keep = c(
      "primaryid","drug_seq","role_cod","drugname","prod_ai","val_vbm","route",
      "dose_vbm","dechal","rechal","lot_num","nda_num","exp_dt","dose_form",
      "dose_freq","cum_dose_unit","cum_dose_chr","dose_amt","dose_unit"
    ),
    exclude = c("caseid")
  ),
  
  INDI = list(
    rename = c(
      isr           = "primaryid",
      drug_seq      = "drug_seq",
      indi_drug_seq = "drug_seq",
      indi_pt       = "indi_pt"
    ),
    keep = c("primaryid","drug_seq","indi_pt"),
    exclude = c("caseid")
  ),
  
  OUTC = list(
    rename = c(
      isr       = "primaryid",
      outc_cod  = "outc_cod",
      outc_code = "outc_cod"
    ),
    keep = c("primaryid","outc_cod"),
    exclude = c("caseid")
  ),
  
  REAC = list(
    rename = c(
      isr          = "primaryid",
      pt           = "pt",
      drug_rec_act = "drug_rec_act"
    ),
    keep = c("primaryid","pt","drug_rec_act"),
    exclude = c("caseid")
  ),
  
  RPSR = list(
    rename = c(
      isr      = "primaryid",
      rpsr_cod = "rpsr_cod"
    ),
    keep = c("primaryid","rpsr_cod"),
    exclude = c("caseid")
  ),
  
  THER = list(
    rename = c(
      isr          = "primaryid",
      drug_seq     = "drug_seq",
      dsg_drug_seq = "drug_seq",
      start_dt     = "start_dt",
      end_dt       = "end_dt",
      dur          = "dur",
      dur_cod      = "dur_cod"
    ),
    keep = c("primaryid","drug_seq","start_dt","end_dt","dur","dur_cod"),
    exclude = c("caseid")
  ),
  
  DELETED = list(
    rename = c(
      caseid = "caseid"
    ),
    keep = c("caseid"),
    exclude = character()
  )
)
validate_table_specs(TABLE_SPECS)

# ------------------------------- core reader --------------------------------- #

read_faers_ascii <- function(path, table_name, spec, schema_warnings_dt, column_profiles_dt) {
  if (identical(table_name, "DELETED")) {
    dt <- read_deleted_ascii(path)
  } else {
    dt <- tryCatch(
      {
        data.table::fread(
          file = path,
          sep = "$",
          header = TRUE,
          quote = "",
          comment.char = "",
          na.strings = "",
          fill = Inf,
          showProgress = TRUE,
          data.table = TRUE
        )
      },
      finally = {
        try(closeAllConnections(), silent = TRUE)
      }
    )
    
    setDT(dt)
    setnames(dt, normalize_names(names(dt)))
  }
  
  qtr <- extract_quarter_yyq(path)
  if (is.na(qtr)) {
    stop("Could not extract quarter from path: ", path, call. = FALSE)
  }
  
  # Drop all-missing fread placeholder columns like v1, v2, ...
  artifact_v_cols_raw <- find_all_missing_v_columns(dt)
  
  if (length(artifact_v_cols_raw)) {
    message_ignored_artifact_columns(
      path = path,
      table_name = table_name,
      quarter = qtr,
      column_names = artifact_v_cols_raw,
      stage = "raw_read"
    )
    
    dt <- drop_columns_if_present(dt, artifact_v_cols_raw)
  }
  
  # 1) raw columns that are completely unknown before harmonization
  allowed_raw_names <- unique(c(names(spec$rename), spec$keep, spec$exclude))
  unexpected_raw <- setdiff(names(dt), allowed_raw_names)
  
  if (length(unexpected_raw)) {
    report_discarded_columns(
      path = path,
      table_name = table_name,
      quarter = qtr,
      column_names = unexpected_raw,
      discard_stage = "unexpected_raw_column",
      notes = "Column present in source TXT but not in rename map, keep list, or explicit exclude list."
    )
    
    schema_warnings_dt <- log_schema_warning(
      warnings_dt = schema_warnings_dt,
      file_path = path,
      table_name = table_name,
      quarter = qtr,
      warning_type = "unexpected_raw_column",
      column_names = unexpected_raw,
      notes = "Column present in source TXT but not in rename map, keep list, or explicit exclude list."
    )
    
    new_profiles <- profile_columns(
      dt = dt,
      file_path = path,
      table_name = table_name,
      quarter = qtr,
      column_names = unexpected_raw,
      stage = "unexpected_raw_column",
      max_examples = max_profile_examples,
      max_chars = max_profile_chars
    )
    
    message_column_profiles(new_profiles)
    column_profiles_dt <- append_column_profiles(column_profiles_dt, new_profiles)
    
    abort_on_discarded_columns(
      path = path,
      table_name = table_name,
      quarter = qtr,
      column_names = unexpected_raw,
      discard_stage = "unexpected_raw_column",
      fail_on_discarded = fail_on_discarded_columns
    )
  }
  
  # 2) harmonize names
  src <- intersect(names(spec$rename), names(dt))
  if (length(src)) {
    setnames(dt, old = src, new = unname(spec$rename[src]))
  }
  
  dt <- convert_all_to_character(dt)
  dt <- coalesce_duplicate_names(dt)
  
  # Drop all-missing placeholder columns again in case any remain after harmonization
  artifact_v_cols_harmonized <- find_all_missing_v_columns(dt)
  
  if (length(artifact_v_cols_harmonized)) {
    message_ignored_artifact_columns(
      path = path,
      table_name = table_name,
      quarter = qtr,
      column_names = artifact_v_cols_harmonized,
      stage = "post_harmonization"
    )
    
    dt <- drop_columns_if_present(dt, artifact_v_cols_harmonized)
  }
  
  # 3) after harmonization, every column must be either kept or explicitly excluded
  allowed_after_harmonization <- unique(c(spec$keep, spec$exclude))
  unauthorized_after_harmonization <- setdiff(names(dt), allowed_after_harmonization)
  
  if (length(unauthorized_after_harmonization)) {
    report_discarded_columns(
      path = path,
      table_name = table_name,
      quarter = qtr,
      column_names = unauthorized_after_harmonization,
      discard_stage = "unauthorized_after_harmonization",
      notes = "Column present after rename/coalesce but not included in keep or explicit exclude list."
    )
    
    schema_warnings_dt <- log_schema_warning(
      warnings_dt = schema_warnings_dt,
      file_path = path,
      table_name = table_name,
      quarter = qtr,
      warning_type = "unauthorized_after_harmonization",
      column_names = unauthorized_after_harmonization,
      notes = "Column present after rename/coalesce but not included in keep or explicit exclude list."
    )
    
    new_profiles <- profile_columns(
      dt = dt,
      file_path = path,
      table_name = table_name,
      quarter = qtr,
      column_names = unauthorized_after_harmonization,
      stage = "unauthorized_after_harmonization",
      max_examples = max_profile_examples,
      max_chars = max_profile_chars
    )
    
    message_column_profiles(new_profiles)
    column_profiles_dt <- append_column_profiles(column_profiles_dt, new_profiles)
    
    abort_on_discarded_columns(
      path = path,
      table_name = table_name,
      quarter = qtr,
      column_names = unauthorized_after_harmonization,
      discard_stage = "unauthorized_after_harmonization",
      fail_on_discarded = fail_on_discarded_columns
    )
  }
  
  # 4) report columns intentionally excluded
  intentionally_excluded <- intersect(names(dt), spec$exclude)
  
  if (length(intentionally_excluded)) {
    message(
      sprintf(
        "Intentionally excluded column(s) | table=%s | quarter=%s | file=%s | columns=%s",
        table_name,
        qtr,
        path,
        paste(intentionally_excluded, collapse = ", ")
      )
    )
  }
  
  # 5) keep only staged columns
  keep <- intersect(spec$keep, names(dt))
  dt <- dt[, ..keep]
  
  # 6) add quarter and finalize
  dt[, quarter := qtr]
  dt <- normalize_empty_strings(dt)
  dt <- unique(dt)
  
  attr(dt, "schema_warnings_dt") <- schema_warnings_dt
  attr(dt, "column_profiles_dt") <- column_profiles_dt
  dt
}


write_stage_table <- function(table_name, quarter, files, spec, schema_warnings_dt, column_profiles_dt) {
  out_dir <- file.path(STAGE_ROOT, table_name)
  dir_create_safe(out_dir)
  
  out_file <- file.path(out_dir, sprintf("%s_%s.parquet", table_name, quarter))
  
  if (file.exists(out_file) && !overwrite_stage) {
    message(sprintf("Skipping existing %s %s", table_name, quarter))
    return(list(
      out_file = out_file,
      n_rows = NA_integer_,
      status = "skipped_existing",
      notes = NA_character_,
      schema_warnings_dt = schema_warnings_dt,
      column_profiles_dt = column_profiles_dt
    ))
  }
  
  message(sprintf("Building %s %s from %d file(s)", table_name, quarter, length(files)))
  
  parts <- vector("list", length(files))
  
  for (k in seq_along(files)) {
    p <- files[k]
    message("  reading: ", p)
    
    part <- read_faers_ascii(
      path = p,
      table_name = table_name,
      spec = spec,
      schema_warnings_dt = schema_warnings_dt,
      column_profiles_dt = column_profiles_dt
    )
    
    schema_warnings_dt <- attr(part, "schema_warnings_dt")
    column_profiles_dt <- attr(part, "column_profiles_dt")
    
    attr(part, "schema_warnings_dt") <- NULL
    attr(part, "column_profiles_dt") <- NULL
    
    parts[[k]] <- part
  }
  
  dt <- rbindlist(parts, use.names = TRUE, fill = TRUE)
  dt <- unique(dt)
  
  write_parquet(
    x = dt,
    sink = out_file,
    compression = parquet_compression
  )
  
  n_rows <- nrow(dt)
  
  rm(parts, dt)
  gc()
  
  list(
    out_file = out_file,
    n_rows = n_rows,
    status = "written",
    notes = NA_character_,
    schema_warnings_dt = schema_warnings_dt,
    column_profiles_dt = column_profiles_dt
  )
}

# ------------------------------- main ---------------------------------------- #

dir_create_safe(STAGE_ROOT)
schema_warnings <- load_schema_warnings(SCHEMA_WARNINGS_CSV)
column_profiles <- load_column_profiles(COLUMN_PROFILES_CSV)
if (!dir.exists(ASCII_ROOT)) {
  stop("ASCII_ROOT does not exist: ", ASCII_ROOT,
       "\nRun download/extract first.")
}

txt_files <- list.files(
  path = ASCII_ROOT,
  pattern = "\\.[Tt][Xx][Tt]$",
  recursive = TRUE,
  full.names = TRUE
)

if (!length(txt_files)) {
  stop("No TXT files found under: ", ASCII_ROOT)
}

# Drop helper/stat/size files
txt_files <- txt_files[!grepl("(?i)(STAT|SIZE)", basename(txt_files), perl = TRUE)]

inventory <- data.table(path = txt_files)
inventory[, file_name := basename(path)]
inventory[, quarter := vapply(path, extract_quarter_yyq, character(1))]
inventory[, table_name := vapply(path, table_from_path, character(1))]
inventory[, include_for_stage := !is.na(table_name)]
inventory[, notes := NA_character_]

# Keep a full inventory for auditing, including ignored files.
fwrite(inventory[order(table_name, quarter, file_name, na.last = TRUE)], INVENTORY_CSV)

recognized <- inventory[include_for_stage == TRUE]

if (nrow(recognized) == 0L) {
  stop("No recognized FAERS table TXT files found under: ", ASCII_ROOT)
}

bad_quarters <- recognized[is.na(quarter)]
if (nrow(bad_quarters) > 0L) {
  stop(
    "Quarter parsing failed for recognized FAERS files:\n",
    paste(bad_quarters$path, collapse = "\n"),
    call. = FALSE
  )
}

manifest <- load_manifest(MANIFEST_CSV)

tables_to_build <- intersect(names(TABLE_SPECS), unique(recognized$table_name))
tables_to_build <- sort(tables_to_build)

if (!length(tables_to_build)) {
  stop("No recognized FAERS tables matched TABLE_SPECS.")
}

for (tbl in tables_to_build) {
  spec <- TABLE_SPECS[[tbl]]
  inv_t <- recognized[table_name == tbl]
  if (nrow(inv_t) == 0L) next
  quarters <- sort(unique(inv_t$quarter))
  
  message("")
  message("================================================================")
  message(sprintf("TABLE: %s | %d source file(s) | %d quarter(s)",
                  tbl, nrow(inv_t), length(quarters)))
  message("================================================================")
for (qtr in quarters) {
  inv_q <- inv_t[quarter == qtr]
  if (nrow(inv_q) == 0L) next
  
  message("")
  message(sprintf("Processing %s %s from %d file(s)", tbl, qtr, nrow(inv_q)))
  
  res <- tryCatch(
    {
      write_stage_table(
        table_name = tbl,
        quarter = qtr,
        files = inv_q$path,
        spec = spec,
        schema_warnings_dt = schema_warnings,
        column_profiles_dt = column_profiles
      )
    },
    error = function(e) {
      list(
        out_file = file.path(STAGE_ROOT, tbl, sprintf("%s_%s.parquet", tbl, qtr)),
        n_rows = NA_integer_,
        status = "error",
        notes = conditionMessage(e),
        schema_warnings_dt = schema_warnings,
        column_profiles_dt = column_profiles
      )
    }
  )
  
  # pull updated schema warning log out of the result
  schema_warnings <- res$schema_warnings_dt
  column_profiles <- res$column_profiles_dt
  manifest_row <- data.table(
    table_name = tbl,
    quarter = qtr,
    out_file = res$out_file,
    n_source = nrow(inv_q),
    n_rows = res$n_rows,
    status = res$status,
    timestamp = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
    notes = res$notes
  )
  
  manifest <- upsert_manifest_row(manifest, manifest_row)
  
  # persist after every table-quarter so the run is restartable
  save_manifest(manifest, MANIFEST_CSV)
  if (is.null(schema_warnings)) {
    schema_warnings <- load_schema_warnings(SCHEMA_WARNINGS_CSV)
  }
  
  if (is.null(column_profiles)) {
    column_profiles <- load_column_profiles(COLUMN_PROFILES_CSV)
  }
  fwrite(schema_warnings, SCHEMA_WARNINGS_CSV)
  fwrite(column_profiles, COLUMN_PROFILES_CSV)
  message(sprintf(
    "Completed %s %s | status=%s | rows=%s",
    tbl, qtr, res$status,
    ifelse(is.na(res$n_rows), "NA", format(res$n_rows, big.mark = ","))
  ))
  
  if (!is.na(res$notes) && nzchar(res$notes)) {
    message("  notes: ", res$notes)
  }
}
}

message("")
message("Done.")
message("Inventory: ", INVENTORY_CSV)
message("Manifest:  ", MANIFEST_CSV)
message("Stage dir: ", STAGE_ROOT)
if (file.exists(SCHEMA_WARNINGS_CSV)) {
  sw <- fread(SCHEMA_WARNINGS_CSV, na.strings = c("", "NA"))
  setDT(sw)
  
  if (nrow(sw) > 0L) {
    warning(
      sprintf(
        "Schema warnings were recorded (%d row(s)). Review %s before treating the run as clean.",
        nrow(sw), SCHEMA_WARNINGS_CSV
      ),
      call. = FALSE
    )
  } else {
    message("No schema warnings recorded.")
  }
}
## check ingest faers-----------------------------------------------------------
suppressPackageStartupMessages({
  library(data.table)
  library(arrow)
})

options(timeout = 1200)

# ------------------------------- configuration ------------------------------- #

BASE_DIR <- "."

ASCII_ROOT    <- file.path(BASE_DIR, "data_raw", "faers_ascii")
STAGE_ROOT    <- file.path(BASE_DIR, "data_stage")
INVENTORY_CSV <- file.path(STAGE_ROOT, "faers_file_inventory.csv")
MANIFEST_CSV  <- file.path(STAGE_ROOT, "faers_ingest_manifest.csv")

allowed_manifest_status <- c("written", "skipped_existing", "error")

# ------------------------------- helpers ------------------------------------- #

fail <- function(...) {
  stop(sprintf(...), call. = FALSE)
}

pass <- function(...) {
  message(sprintf(...))
}

dir_exists_or_fail <- function(path, label) {
  if (!dir.exists(path)) fail("%s directory not found: %s", label, path)
}

file_exists_or_fail <- function(path, label) {
  if (!file.exists(path)) fail("%s file not found: %s", label, path)
}
strict_fread <- function(file, ..., fail_on_warning = TRUE) {
  warnings_seen <- character()
  
  out <- withCallingHandlers(
    {
      message("Reading: ", file)
      fread(file = file, ...)
    },
    warning = function(w) {
      msg <- conditionMessage(w)
      warnings_seen <<- c(warnings_seen, msg)
      message("\nWARNING while reading: ", file)
      message(msg)
      invokeRestart("muffleWarning")
    }
  )
  
  if (fail_on_warning && length(warnings_seen) > 0L) {
    stop(
      sprintf(
        "Parsing warning detected for file:\n%s\n\nFirst warning:\n%s",
        file, warnings_seen[1]
      ),
      call. = FALSE
    )
  }
  
  out
}

count_file_lines <- function(path) {
  length(readLines(path, warn = FALSE, encoding = "UTF-8"))
}

choose_representative_files <- function(inv) {
  inv2 <- copy(inv[include_for_stage == TRUE])
  setorder(inv2, quarter, table_name, file_name)
  
  picks <- character()
  
  add_pick <- function(x) {
    for (p in x) {
      if (!is.na(p) && nzchar(p) && !(p %in% picks)) {
        picks <<- c(picks, p)
      }
    }
  }
  
  # earliest and latest recognized files
  if (nrow(inv2) > 0L) {
    add_pick(inv2$path[1L])
    add_pick(inv2$path[nrow(inv2)])
  }
  
  # target some load-bearing tables if present
  for (tbl in c("DEMO", "DRUG", "REAC", "THER", "INDI")) {
    cand <- inv2[table_name == tbl, path]
    if (length(cand)) add_pick(cand[1L])
  }
  
  # fill up to 5 if needed
  if (length(picks) < 5L) {
    for (p in inv2$path) {
      if (!(p %in% picks)) picks <- c(picks, p)
      if (length(picks) >= 5L) break
    }
  }
  
  unique(picks)[seq_len(min(5L, length(unique(picks))))]
}

read_header_only_expected_cols <- function(path, table_name, spec) {
  # DELETED is special: schema is always caseid + quarter
  if (identical(table_name, "DELETED")) {
    return(c("caseid", "quarter"))
  }
  
  hdr <- strict_fread(
    file = path,
    sep = "$",
    header = TRUE,
    quote = "",
    comment.char = "",
    na.strings = "",
    fill = TRUE,
    nrows = 0L,
    showProgress = FALSE,
    data.table = TRUE
  )
  
  setDT(hdr)
  setnames(hdr, normalize_names(names(hdr)))
  
  src <- intersect(names(spec$rename), names(hdr))
  if (length(src)) {
    setnames(hdr, old = src, new = unname(spec$rename[src]))
  }
  
  hdr <- convert_all_to_character(hdr)
  hdr <- coalesce_duplicate_names(hdr)
  
  keep <- intersect(spec$keep, names(hdr))
  c(keep, "quarter")
}

read_faers_ascii_check <- function(path, table_name, spec) {
  if (identical(table_name, "DELETED")) {
    first_line <- readLines(path, n = 1L, warn = FALSE, encoding = "UTF-8")
    first_line_norm <- trimws(tolower(first_line))
    has_header <- identical(first_line_norm, "caseid")
    
    dt <- strict_fread(
      file = path,
      sep = "$",
      header = has_header,
      quote = "",
      comment.char = "",
      na.strings = "",
      fill = TRUE,
      showProgress = FALSE,
      data.table = TRUE
    )
    
    setDT(dt)
    
    if (has_header) {
      setnames(dt, normalize_names(names(dt)))
    } else {
      if (ncol(dt) != 1L) {
        fail(
          "DELETED file without header did not read as a single column: %s (ncol=%d)",
          path, ncol(dt)
        )
      }
      setnames(dt, "caseid")
    }
    
  } else {
    dt <- strict_fread(
      file = path,
      sep = "$",
      header = TRUE,
      quote = "",
      comment.char = "",
      na.strings = "",
      fill = TRUE,
      showProgress = FALSE,
      data.table = TRUE
    )
    
    setDT(dt)
    setnames(dt, normalize_names(names(dt)))
  }
  
  src <- intersect(names(spec$rename), names(dt))
  if (length(src)) {
    setnames(dt, old = src, new = unname(spec$rename[src]))
  }
  
  dt <- convert_all_to_character(dt)
  dt <- coalesce_duplicate_names(dt)
  
  keep <- intersect(spec$keep, names(dt))
  
  if (identical(table_name, "DELETED") && !identical(keep, "caseid")) {
    fail(
      "DELETED ingest did not resolve to caseid column for file: %s | names seen: %s",
      path, paste(names(dt), collapse = ", ")
    )
  }
  
  dt <- dt[, ..keep]
  
  qtr <- extract_quarter_yyq(path)
  if (is.na(qtr)) {
    fail("Could not extract quarter from path during validation: %s", path)
  }
  
  dt[, quarter := qtr]
  dt <- normalize_empty_strings(dt)
  dt <- unique(dt)
}

load_parquet_dt <- function(path) {
  as.data.table(read_parquet(path))
}

assert_identical_dt <- function(x, y, msg_prefix) {
  # compare as character-friendly ordered tables with identical names
  if (!identical(names(x), names(y))) {
    fail("%s: column names differ.", msg_prefix)
  }
  
  x2 <- copy(x)
  y2 <- copy(y)
  
  setcolorder(x2, names(x))
  setcolorder(y2, names(y))
  
  setkeyv(x2, names(x2))
  setkeyv(y2, names(y2))
  
  if (!identical(x2, y2)) {
    fail("%s: data content differs.", msg_prefix)
  }
}

# ------------------------------- schema specs -------------------------------- #

TABLE_SPECS <- list(
  
  DEMO = list(
    rename = c(
      isr              = "primaryid",
      case             = "caseid",
      foll_seq         = "caseversion",
      i_f_cod          = "i_f_cod",
      i_f_code         = "i_f_cod",
      event_dt         = "event_dt",
      mfr_dt           = "mfr_dt",
      fda_dt           = "fda_dt",
      init_fda_dt      = "init_fda_dt",
      rept_cod         = "rept_cod",
      mfr_num          = "mfr_num",
      mfr_sndr         = "mfr_sndr",
      age              = "age",
      age_cod          = "age_cod",
      age_grp          = "age_grp",
      gndr_cod         = "sex",
      sex              = "sex",
      e_sub            = "e_sub",
      wt               = "wt",
      wt_cod           = "wt_cod",
      rept_dt          = "rept_dt",
      occp_cod         = "occp_cod",
      to_mfr           = "to_mfr",
      reporter_country = "reporter_country",
      occr_country     = "occr_country",
      auth_num         = "auth_num",
      lit_ref          = "lit_ref"
    ),
    keep = c(
      "primaryid","caseid","caseversion","i_f_cod","sex","age","age_cod","age_grp",
      "wt","wt_cod","reporter_country","occr_country","event_dt","rept_dt",
      "mfr_dt","init_fda_dt","fda_dt","rept_cod","occp_cod","mfr_num",
      "mfr_sndr","to_mfr","e_sub","auth_num","lit_ref"
    )
  ),
  
  DRUG = list(
    rename = c(
      isr            = "primaryid",
      drug_seq       = "drug_seq",
      role_cod       = "role_cod",
      drugname       = "drugname",
      prod_ai        = "prod_ai",
      val_vbm        = "val_vbm",
      route          = "route",
      dose_vbm       = "dose_vbm",
      dechal         = "dechal",
      rechal         = "rechal",
      lot_num        = "lot_num",
      lot_nbr        = "lot_num",
      nda_num        = "nda_num",
      exp_dt         = "exp_dt",
      dose_form      = "dose_form",
      dose_freq      = "dose_freq",
      cum_dose_unit  = "cum_dose_unit",
      cum_dose_chr   = "cum_dose_chr",
      dose_amt       = "dose_amt",
      dose_unit      = "dose_unit"
    ),
    keep = c(
      "primaryid","drug_seq","role_cod","drugname","prod_ai","val_vbm","route",
      "dose_vbm","dechal","rechal","lot_num","nda_num","exp_dt","dose_form",
      "dose_freq","cum_dose_unit","cum_dose_chr","dose_amt","dose_unit"
    )
  ),
  
  INDI = list(
    rename = c(
      isr           = "primaryid",
      drug_seq      = "drug_seq",
      indi_drug_seq = "drug_seq",
      indi_pt       = "indi_pt"
    ),
    keep = c("primaryid","drug_seq","indi_pt")
  ),
  
  OUTC = list(
    rename = c(
      isr       = "primaryid",
      outc_cod  = "outc_cod",
      outc_code = "outc_cod"
    ),
    keep = c("primaryid","outc_cod")
  ),
  
  REAC = list(
    rename = c(
      isr          = "primaryid",
      pt           = "pt",
      drug_rec_act = "drug_rec_act"
    ),
    keep = c("primaryid","pt","drug_rec_act")
  ),
  
  RPSR = list(
    rename = c(
      isr      = "primaryid",
      rpsr_cod = "rpsr_cod"
    ),
    keep = c("primaryid","rpsr_cod")
  ),
  
  THER = list(
    rename = c(
      isr          = "primaryid",
      drug_seq     = "drug_seq",
      dsg_drug_seq = "drug_seq",
      start_dt     = "start_dt",
      end_dt       = "end_dt",
      dur          = "dur",
      dur_cod      = "dur_cod"
    ),
    keep = c("primaryid","drug_seq","start_dt","end_dt","dur","dur_cod")
  ),
  
  DELETED = list(
    rename = c(
      caseid = "caseid"
    ),
    keep = c("caseid")
  )
)

# ------------------------------- checks -------------------------------------- #

dir_exists_or_fail(ASCII_ROOT, "ASCII_ROOT")
dir_exists_or_fail(STAGE_ROOT, "STAGE_ROOT")
file_exists_or_fail(INVENTORY_CSV, "Inventory")
file_exists_or_fail(MANIFEST_CSV, "Manifest")

txt_files <- list.files(
  path = ASCII_ROOT,
  pattern = "\\.[Tt][Xx][Tt]$",
  recursive = TRUE,
  full.names = TRUE
)

if (!length(txt_files)) {
  fail("No TXT files found under %s", ASCII_ROOT)
}

txt_files <- txt_files[!grepl("(?i)(STAT|SIZE)", basename(txt_files), perl = TRUE)]

current_inventory <- data.table(path = txt_files)
current_inventory[, file_name := basename(path)]
current_inventory[, quarter := vapply(path, extract_quarter_yyq, character(1))]
current_inventory[, table_name := vapply(path, table_from_path, character(1))]
current_inventory[, include_for_stage := !is.na(table_name)]
current_inventory[, notes := NA_character_]

saved_inventory <- strict_fread(INVENTORY_CSV, na.strings = c("", "NA"))
setDT(saved_inventory)

# check saved inventory content against current scan
inv_cols <- c("path", "file_name", "quarter", "table_name", "include_for_stage")
missing_inv_cols <- setdiff(inv_cols, names(saved_inventory))
if (length(missing_inv_cols)) {
  fail("Inventory CSV missing required columns: %s", paste(missing_inv_cols, collapse = ", "))
}

ci <- copy(current_inventory[, ..inv_cols])
si <- copy(saved_inventory[, ..inv_cols])

setorderv(ci, inv_cols)
setorderv(si, inv_cols)

if (!identical(ci, si)) {
  fail("Saved inventory does not match current TXT scan.")
}
pass("OK: saved inventory matches current TXT scan.")

recognized <- current_inventory[include_for_stage == TRUE]
if (nrow(recognized) == 0L) {
  fail("No recognized FAERS tables found in TXT inventory.")
}
pass("OK: found %d recognized FAERS TXT file(s).", nrow(recognized))

bad_quarters <- recognized[is.na(quarter)]
if (nrow(bad_quarters) > 0L) {
  fail("Recognized TXT files with unparseable quarter:\n%s", paste(bad_quarters$path, collapse = "\n"))
}
pass("OK: all recognized FAERS TXT files have parseable quarters.")

manifest <- strict_fread(MANIFEST_CSV, na.strings = c("", "NA"))
setDT(manifest)

man_cols <- c("table_name", "quarter", "out_file", "n_source", "n_rows", "status", "timestamp", "notes")
missing_man_cols <- setdiff(man_cols, names(manifest))
if (length(missing_man_cols)) {
  fail("Manifest CSV missing required columns: %s", paste(missing_man_cols, collapse = ", "))
}

bad_status <- manifest[!(status %in% allowed_manifest_status)]
if (nrow(bad_status) > 0L) {
  fail("Manifest contains unexpected status values.")
}
pass("OK: manifest statuses are valid.")

dup_man <- manifest[, .N, by = .(table_name, quarter, out_file)][N > 1L]
if (nrow(dup_man) > 0L) {
  fail("Manifest contains duplicate (table_name, quarter, out_file) rows.")
}
pass("OK: manifest keys are unique.")

manifest_errors <- manifest[status == "error"]
if (nrow(manifest_errors) > 0L) {
  fail("Manifest contains %d error row(s). Fix ingestion first.", nrow(manifest_errors))
}
pass("OK: manifest contains no error rows.")

expected_groups <- unique(recognized[, .(
  table_name,
  quarter,
  out_file = file.path(STAGE_ROOT, table_name, sprintf("%s_%s.parquet", table_name, quarter))
)])

man_groups <- unique(manifest[, .(table_name, quarter, out_file)])

setkey(expected_groups, table_name, quarter, out_file)
setkey(man_groups, table_name, quarter, out_file)

if (!identical(expected_groups, man_groups)) {
  fail("Manifest table-quarter coverage does not match recognized source-file groups.")
}
pass("OK: manifest table-quarter coverage matches recognized source-file groups.")

# Per staged parquet checks
for (i in seq_len(nrow(expected_groups))) {
  tbl <- expected_groups$table_name[i]
  qtr <- expected_groups$quarter[i]
  out_file_i <- expected_groups$out_file[i]
  spec <- TABLE_SPECS[[tbl]]
  
  row_man <- manifest[
    table_name == tbl &
      quarter == qtr &
      out_file == out_file_i
  ]
  
  if (nrow(row_man) != 1L) {
    fail("Expected exactly one manifest row for %s %s", tbl, qtr)
  }
  row_man <- row_man[1]
  
  if (!file.exists(out_file_i)) {
    fail("Missing staged parquet file for %s %s: %s", tbl, qtr, out_file_i)
  }
  
  dt_stage <- load_parquet_dt(out_file_i)
  
  # expected standardized columns from source-file headers
  source_files <- recognized[table_name == tbl & quarter == qtr, path]
  expected_cols_list <- lapply(
    source_files,
    function(p) read_header_only_expected_cols(
      path = p,
      table_name = tbl,
      spec = spec
    )
  )
  expected_cols <- unique(unlist(expected_cols_list, use.names = FALSE))
  
  if (!("quarter" %in% expected_cols)) {
    expected_cols <- c(expected_cols, "quarter")
  }
  
  if (!setequal(names(dt_stage), expected_cols)) {
    fail(
      "Column set mismatch for %s %s.\nExpected: %s\nActual:   %s",
      tbl, qtr,
      paste(sort(expected_cols), collapse = ", "),
      paste(sort(names(dt_stage)), collapse = ", ")
    )
  }
  
  if (nrow(dt_stage) != nrow(unique(dt_stage))) {
    fail("Exact duplicate rows found in staged parquet for %s %s.", tbl, qtr)
  }
  
  if (!("quarter" %in% names(dt_stage))) {
    fail("quarter column missing in staged parquet for %s %s.", tbl, qtr)
  }
  
  bad_qvals <- unique(dt_stage$quarter)
  bad_qvals <- bad_qvals[!is.na(bad_qvals)]
  if (!all(bad_qvals == qtr)) {
    fail(
      "quarter column values do not match expected quarter for %s %s: %s",
      tbl, qtr, paste(bad_qvals, collapse = ", ")
    )
  }
  
  if (identical(row_man$status, "written")) {
    if (is.na(row_man$n_rows)) {
      fail("Manifest n_rows is NA for written output %s %s.", tbl, qtr)
    }
    if (nrow(dt_stage) != row_man$n_rows) {
      fail(
        "Manifest n_rows mismatch for %s %s: manifest=%d actual=%d",
        tbl, qtr, row_man$n_rows, nrow(dt_stage)
      )
    }
  }
  
  n_source_expected <- length(source_files)
  if (!identical(as.integer(row_man$n_source), as.integer(n_source_expected))) {
    fail(
      "Manifest n_source mismatch for %s %s: manifest=%d expected=%d",
      tbl, qtr, row_man$n_source, n_source_expected
    )
  }
}
pass("OK: all staged parquet files passed manifest, column-set, quarter, and duplicate checks.")

# -------------------------- raw-reader spot checks --------------------------- #

sample_files <- choose_representative_files(current_inventory)

if (!length(sample_files)) {
  fail("Could not select representative source files for spot checks.")
}

pass("Running raw-reader spot checks on %d representative TXT file(s).", length(sample_files))

for (p in sample_files) {
  tbl <- table_from_path(p)
  spec <- TABLE_SPECS[[tbl]]
  
  # check raw line count vs fread row count
  raw_line_count <- count_file_lines(p)
  
  dt_fread <- tryCatch(
    {
      strict_fread(
        file = p,
        sep = "$",
        header = TRUE,
        quote = "",
        comment.char = "",
        na.strings = "",
        fill = TRUE,
        showProgress = FALSE,
        data.table = TRUE
      )
    },
    finally = {
      try(closeAllConnections(), silent = TRUE)
    }
  )
  
  expected_data_rows <- raw_line_count - 1L
  if (nrow(dt_fread) != expected_data_rows) {
    fail(
      "Raw-reader spot check failed for %s: fread rows=%d, line_count-1=%d",
      p, nrow(dt_fread), expected_data_rows
    )
  }
  
  # check normalized/renamed/kept header
  expected_cols <- read_header_only_expected_cols(
    path = p,
    table_name = tbl,
    spec = spec
  )
  
  dt_check <- read_faers_ascii_check(
    path = p,
    table_name = tbl,
    spec = spec
  )
  
  if (!identical(names(dt_check), expected_cols)) {
    fail(
      "Normalized header mismatch for %s.\nExpected: %s\nActual:   %s",
      p,
      paste(expected_cols, collapse = ", "),
      paste(names(dt_check), collapse = ", ")
    )
  }
  
  # check quarter value
  uq <- unique(dt_check$quarter)
  uq <- uq[!is.na(uq)]
  if (length(uq) > 1L) {
    fail("More than one quarter value found after ingest transform for %s", p)
  }
}
pass("OK: raw-reader spot checks passed.")

# -------------------------- optional full rebuild check ---------------------- #
# Rebuild every staged table-quarter from source TXT and compare to parquet.
# This is heavier, but it is the strongest validation of the revised script.

pass("Running full rebuild comparison for all staged table-quarter outputs...")

for (i in seq_len(nrow(expected_groups))) {
  tbl <- expected_groups$table_name[i]
  qtr <- expected_groups$quarter[i]
  out_file <- expected_groups$out_file[i]
  spec <- TABLE_SPECS[[tbl]]
  print(paste0(tbl,qtr))
  
  source_files <- recognized[table_name == tbl & quarter == qtr, path]
  
  rebuilt_parts <- lapply(source_files, function(p) read_faers_ascii_check(p,tbl, spec=spec))
  rebuilt <- rbindlist(rebuilt_parts, use.names = TRUE, fill = TRUE)
  rebuilt <- unique(rebuilt)
  
  staged <- load_parquet_dt(out_file)
  
  # harmonize column order before comparing
  common_order <- names(rebuilt)
  if (!setequal(names(rebuilt), names(staged))) {
    fail("Full rebuild column-set mismatch for %s %s.", tbl, qtr)
  }
  
  setcolorder(staged, common_order)
  setcolorder(rebuilt, common_order)
  
  # compare sorted content
  setkeyv(rebuilt, common_order)
  setkeyv(staged, common_order)
  
  if (!identical(rebuilt, staged)) {
    fail("Full rebuild content mismatch for %s %s.", tbl, qtr)
  }
}
pass("OK: full rebuild comparison passed for all staged outputs.")

message("")
message("All ingestion/staging checks passed.")

# inspecting bad lines ---------------------------------------------------------
# bad_file <- "./data_raw/faers_ascii/2012Q1/ascii/DEMO12Q1.TXT"
# lines <- readLines(bad_file, warn = FALSE, encoding = "UTF-8")
# 
# cat(lines[1], "\n\n")                # header
# cat(lines[105917], "\n\n")           # offending line
# cat(lines[105916], "\n\n")           # previous line
# cat(lines[105918], "\n\n")           # next line
# 
# count_fields <- function(x) lengths(strsplit(x, "\\$", perl = TRUE))
# 
# header_n <- count_fields(lines[1])
# bad_n    <- count_fields(lines[105917])
# 
# header_n
# bad_n
# 
# 
# endsWith(lines[105917], "$")
# endsWith(lines[1], "$")
# 
# nearby_counts <- data.table(
#   line_no = 105910:105925,
#   n_fields = vapply(lines[105910:105925], count_fields, integer(1))
# )
# 
# nearby_counts
# 
# r1 <- purrr::flatten(strsplit(as.character(lines[1], "\n\n"),split="\\$"))
# r_bad <- purrr::flatten(strsplit(as.character(lines[105917], "\n\n"),split="\\$"))             # header
# r_good <- purrr::flatten(strsplit(as.character(lines[105916], "\n\n"),split="\\$"))             # header
# 
# View(rbindlist(list(r1,r_bad,r_good),fill = TRUE,use.names=FALSE))
# 
