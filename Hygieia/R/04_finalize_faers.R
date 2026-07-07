# ------------------------------------------------------------------------------
# Script: 04_finalize_faers.R
# Purpose:
#   Build a canonical cross-quarter FAERS master layer from cleaned quarter-level
#   parquet files.
#
# Inputs:
#   - data_clean/<TABLE>/<TABLE>_<YYQ#>.parquet
#
# Outputs:
#   - data_master/<TABLE>/<TABLE>_<YYQ#>.parquet
#   - data_master/finalize_manifest.csv
#   - data_master/finalize_counts.csv
#
# What this script does:
#   1. Loads all cleaned DEMO rows across quarters
#   2. Removes nullified cases using cleaned DELETED
#   3. Resolves repeated reports across quarters:
#        - latest row per primaryid
#        - latest caseversion per caseid
#        - latest row per (mfr_num, mfr_sndr) when both are present
#   4. Identifies canonical surviving report keys: (primaryid, quarter)
#   5. Filters out reports with no valid drug or no valid reaction
#   6. Adds report-level flags:
#        - premarketing
#        - literature
#   7. Streams all other cleaned tables and writes master parquet outputs
#
# Notes:
#   - This is the canonical structural finalize layer, not the rule-based
#     duplicate flagging stage (RB_duplicates / RB_duplicates_only_susp).
#   - Non-DEMO tables are filtered by canonical (primaryid, quarter), not
#     by primaryid alone, to avoid reintroducing cross-quarter rows.
#   - The script is restart-friendlier than the original monolith:
#       * manifest rows are upserted by (table, quarter)
#       * key assertions fail fast on schema problems
# ------------------------------------------------------------------------------

suppressPackageStartupMessages({
  library(data.table)
  library(arrow)
})

options(timeout = 1200)

# ------------------------------- configuration ------------------------------- #

BASE_DIR <- "."
source(file.path(BASE_DIR, "R", "utils.R"))

CLEAN_ROOT  <- file.path(BASE_DIR, "data_clean")
MASTER_ROOT <- file.path(BASE_DIR, "data_master")

MANIFEST_CSV <- file.path(MASTER_ROOT, "finalize_manifest.csv")
COUNTS_CSV   <- file.path(MASTER_ROOT, "finalize_counts.csv")

overwrite_master <- FALSE
parquet_compression <- "zstd"

apply_mfr_dedup <- TRUE

# canonical filters carried over from earlier logic
INVALID_DRUG_SUBSTANCES <- c("no medication", "unspecified")
INVALID_REACTIONS       <- c("no adverse event")

PRIMARYID_TABLES <- c("DRUG", "DRUG_INFO", "REAC", "INDI", "OUTC", "THER", "RPSR")

# ------------------------------- helpers ------------------------------------- #

empty_key_dt <- function() {
  data.table(
    primaryid = character(),
    quarter   = character()
  )
}

coerce_logical_flag <- function(x) {
  if (is.logical(x)) return(x)
  
  if (is.numeric(x)) {
    out <- rep(NA, length(x))
    out[!is.na(x)] <- x[!is.na(x)] != 0
    return(out)
  }
  
  x <- toupper(normalize_text(x))
  out <- rep(NA, length(x))
  
  out[x %chin% c("TRUE", "T", "1", "Y", "YES")]  <- TRUE
  out[x %chin% c("FALSE", "F", "0", "N", "NO")] <- FALSE
  
  out
}

list_parquet_files <- function(root_dir) {
  files <- list.files(
    path = root_dir,
    pattern = "\\.parquet$",
    recursive = TRUE,
    full.names = TRUE
  )
  
  if (!length(files)) {
    stop("No parquet files found under: ", root_dir, call. = FALSE)
  }
  
  parsed <- lapply(basename(files), parse_file_name)
  ok <- vapply(parsed, `[[`, logical(1), "ok")
  
  if (!all(ok)) {
    bad <- basename(files)[!ok]
    stop(
      "Found parquet files with unexpected names. Expected <TABLE>_<YYQ#>.parquet. Bad files: ",
      paste(bad, collapse = ", "),
      call. = FALSE
    )
  }
  
  dt <- data.table(path = files)
  dt[, file_name  := basename(path)]
  dt[, table_name := vapply(parsed, `[[`, character(1), "table_name")]
  dt[, quarter    := vapply(parsed, `[[`, character(1), "quarter")]
  dt[]
}

get_table_files <- function(file_index, tbl_name) {
  dt <- copy(file_index[table_name == toupper(tbl_name)])
  if (nrow(dt) == 0L) return(dt)
  
  dt[, qrank := quarter_rank(quarter)]
  setorder(dt, qrank, file_name)
  dt[, qrank := NULL]
  dt[]
}

read_parquet_dt <- function(path) {
  dt <- as.data.table(read_parquet(path))
  setDT(dt)
  dt
}

faers_date_rank <- function(x) {
  x <- normalize_text(x)
  out <- rep(-1L, length(x))
  
  ok4 <- !is.na(x) & nchar(x) == 4L & grepl("^[0-9]{4}$", x)
  ok6 <- !is.na(x) & nchar(x) == 6L & grepl("^[0-9]{6}$", x)
  ok8 <- !is.na(x) & nchar(x) == 8L & grepl("^[0-9]{8}$", x)
  
  out[ok4] <- as.integer(paste0(x[ok4], "0000"))
  out[ok6] <- as.integer(paste0(x[ok6], "00"))
  out[ok8] <- as.integer(x[ok8])
  
  out
}

num_rank <- function(x) {
  x <- suppressWarnings(as.numeric(normalize_text(x)))
  x[is.na(x)] <- -Inf
  x
}

load_manifest <- function(path) {
  if (!file.exists(path)) {
    return(data.table(
      output_table = character(),
      quarter      = character(),
      out_file     = character(),
      source_rows  = integer(),
      written_rows = integer(),
      status       = character(),
      timestamp    = character(),
      notes        = character()
    ))
  }
  
  dt <- fread(path, na.strings = c("", "NA"))
  setDT(dt)
  
  needed <- c(
    "output_table", "quarter", "out_file", "source_rows",
    "written_rows", "status", "timestamp", "notes"
  )
  
  missing_cols <- setdiff(needed, names(dt))
  for (nm in missing_cols) dt[, (nm) := NA]
  
  dt[, ..needed]
}

save_manifest <- function(dt, path) fwrite(dt, path)

upsert_manifest <- function(manifest, row_dt) {
  if (nrow(row_dt) == 0L) return(manifest)
  if (nrow(manifest) == 0L) return(copy(row_dt))
  
  keys <- unique(row_dt[, .(output_table, quarter)])
  manifest <- manifest[!keys, on = .(output_table, quarter)]
  
  rbindlist(list(manifest, row_dt), fill = TRUE, use.names = TRUE)
}

write_counts <- function(count_dt, path) {
  fwrite(count_dt, path)
}

prepare_table_dir <- function(tbl_name, overwrite = FALSE) {
  out_dir <- file.path(MASTER_ROOT, toupper(tbl_name))
  if (overwrite && dir.exists(out_dir)) {
    unlink(out_dir, recursive = TRUE, force = TRUE)
  }
  dir_create_safe(out_dir)
  out_dir
}

write_master_quarter <- function(dt, tbl_name, qtr_id, source_rows = NA_integer_) {
  out_dir <- prepare_table_dir(tbl_name, overwrite = FALSE)
  out_file <- file.path(out_dir, sprintf("%s_%s.parquet", toupper(tbl_name), toupper(qtr_id)))
  
  if (file.exists(out_file)) {
    if (overwrite_master) {
      unlink(out_file, force = TRUE)
    } else {
      return(list(
        out_file = out_file,
        source_rows = source_rows,
        written_rows = NA_integer_,
        status = "skipped_existing",
        notes = "existing output preserved"
      ))
    }
  }
  
  write_parquet(dt, out_file, compression = parquet_compression)
  
  list(
    out_file = out_file,
    source_rows = source_rows,
    written_rows = nrow(dt),
    status = "written",
    notes = NA_character_
  )
}

write_master_partitioned <- function(dt, tbl_name) {
  out_dir <- prepare_table_dir(tbl_name, overwrite = overwrite_master)
  
  dt <- unique(copy(dt))
  assert_has_cols(dt, "quarter", paste0(tbl_name, " master partition input"))
  
  dt[, quarter := normalize_text(quarter)]
  
  qtrs <- sort(unique(dt$quarter))
  qtrs <- qtrs[!is.na(qtrs)]
  
  if (!length(qtrs)) {
    stop("No valid quarter values found while writing master ", tbl_name, call. = FALSE)
  }
  
  results <- vector("list", length(qtrs))
  
  for (i in seq_along(qtrs)) {
    qtr_id <- qtrs[i]
    chunk <- dt[quarter == qtr_id]
    out_file <- file.path(out_dir, sprintf("%s_%s.parquet", toupper(tbl_name), toupper(qtr_id)))
    
    if (file.exists(out_file) && !overwrite_master) {
      results[[i]] <- list(
        out_file = out_file,
        source_rows = nrow(chunk),
        written_rows = NA_integer_,
        status = "skipped_existing",
        notes = "existing output preserved",
        quarter = qtr_id
      )
      next
    }
    
    if (file.exists(out_file) && overwrite_master) {
      unlink(out_file, force = TRUE)
    }
    
    write_parquet(chunk, out_file, compression = parquet_compression)
    
    results[[i]] <- list(
      out_file = out_file,
      source_rows = nrow(chunk),
      written_rows = nrow(chunk),
      status = "written",
      notes = NA_character_,
      quarter = qtr_id
    )
  }
  
  rbindlist(lapply(results, as.data.table), fill = TRUE)
}

make_keep_map <- function(keys_dt) {
  if (nrow(keys_dt) == 0L) return(list())
  
  keys_dt <- unique(copy(keys_dt[, .(primaryid, quarter)]))
  keys_dt[, primaryid := normalize_text(primaryid)]
  keys_dt[, quarter := normalize_text(quarter)]
  
  split(keys_dt$primaryid, keys_dt$quarter)
}

key_unique <- function(dt) {
  if (nrow(dt) == 0L) return(empty_key_dt())
  unique(copy(dt[, .(primaryid, quarter)]))
}

key_intersect <- function(...) {
  xs <- list(...)
  if (!length(xs)) return(empty_key_dt())
  
  xs <- lapply(xs, key_unique)
  if (any(vapply(xs, nrow, integer(1)) == 0L)) {
    return(empty_key_dt())
  }
  
  out <- xs[[1]]
  setkey(out, primaryid, quarter)
  
  if (length(xs) > 1L) {
    for (i in 2:length(xs)) {
      y <- xs[[i]]
      setkey(y, primaryid, quarter)
      out <- merge(out, y, by = c("primaryid", "quarter"))
    }
  }
  
  unique(out[])
}

validate_demo_final <- function(demo_dt, deleted_caseids = character()) {
  assert_has_cols(demo_dt, c("primaryid", "caseid", "quarter"), "final DEMO")
  
  if (any(is.na(demo_dt$primaryid))) {
    stop("final DEMO contains missing primaryid values", call. = FALSE)
  }
  
  if (any(is.na(normalize_text(demo_dt$quarter)))) {
    stop("final DEMO contains missing/blank quarter values", call. = FALSE)
  }
  
  if (uniqueN(demo_dt$primaryid) != nrow(demo_dt)) {
    stop("final DEMO is not unique by primaryid", call. = FALSE)
  }
  
  if (length(deleted_caseids) && demo_dt[, any(caseid %chin% deleted_caseids)]) {
    stop("final DEMO still contains deleted caseids", call. = FALSE)
  }
  
  invisible(TRUE)
}

# ------------------------------- DEMO finalization --------------------------- #

load_all_demo <- function(clean_index) {
  files <- get_table_files(clean_index, "DEMO")
  if (nrow(files) == 0L) stop("No cleaned DEMO parquet files found.", call. = FALSE)
  
  parts <- lapply(files$path, read_parquet_dt)
  dt <- rbindlist(parts, use.names = TRUE, fill = TRUE)
  dt <- unique(dt)
  
  assert_has_cols(dt, c("primaryid", "caseid", "quarter"), "cleaned DEMO")
  
  # Optional columns used in ranking/dedup; create if absent so logic is stable.
  for (nm in c("fda_dt", "rept_dt", "caseversion", "mfr_num", "mfr_sndr", "lit_ref")) {
    if (!(nm %in% names(dt))) dt[, (nm) := NA_character_]
  }
  
  for (j in names(dt)) {
    if (is.character(dt[[j]]) || is.factor(dt[[j]])) {
      set(dt, j = j, value = normalize_text(dt[[j]]))
    }
  }
  
  dt <- dt[!is.na(primaryid)]
  dt[]
}

load_all_deleted_caseids <- function(clean_index) {
  files <- get_table_files(clean_index, "DELETED")
  if (nrow(files) == 0L) return(character())
  
  parts <- lapply(files$path, function(p) {
    dt <- read_parquet_dt(p)
    if (!("caseid" %in% names(dt))) return(character())
    safe_unique_char(dt$caseid)
  })
  
  unique(unlist(parts, use.names = FALSE))
}

finalize_demo_global <- function(demo_dt, deleted_caseids, apply_mfr_dedup = TRUE) {
  dt <- copy(demo_dt)
  initial_n <- nrow(dt)
  
  # Remove nullified cases
  if (length(deleted_caseids)) {
    dt <- dt[!caseid %chin% deleted_caseids]
  }
  after_deleted_n <- nrow(dt)
  
  # Ranking helpers: missing values are ranked low so they never win as "latest".
  dt[, qrank        := quarter_rank(quarter)]
  dt[, fda_rank     := faers_date_rank(fda_dt)]
  dt[, rept_rank    := faers_date_rank(rept_dt)]
  dt[, version_rank := num_rank(caseversion)]
  
  # Keep latest row per primaryid
  setorderv(
    dt,
    cols = c("primaryid", "qrank", "fda_rank", "rept_rank", "version_rank", "caseid"),
    order = c(1, 1, 1, 1, 1, 1),
    na.last = FALSE
  )
  dt <- dt[dt[, .I[.N], by = primaryid]$V1]
  after_primaryid_n <- nrow(dt)
  
  # Keep latest caseversion per caseid
  setorderv(
    dt,
    cols = c("caseid", "version_rank", "qrank", "fda_rank", "rept_rank", "primaryid"),
    order = c(1, 1, 1, 1, 1, 1),
    na.last = FALSE
  )
  dt <- dt[dt[, .I[.N], by = caseid]$V1]
  after_caseid_n <- nrow(dt)
  
  # Remove duplicated manufacturer IDs
  if (apply_mfr_dedup) {
    keep_missing <- dt[is.na(mfr_num) | is.na(mfr_sndr), .I]
    
    keep_present <- integer()
    if (dt[!is.na(mfr_num) & !is.na(mfr_sndr), .N] > 0L) {
      setorderv(
        dt,
        cols = c("mfr_num", "mfr_sndr", "fda_rank", "qrank", "version_rank", "primaryid"),
        order = c(1, 1, 1, 1, 1, 1),
        na.last = FALSE
      )
      
      keep_present <- dt[
        !is.na(mfr_num) & !is.na(mfr_sndr),
        .I[.N],
        by = .(mfr_num, mfr_sndr)
      ]$V1
    }
    
    keep_idx <- sort(unique(c(keep_missing, keep_present)))
    dt <- dt[keep_idx]
  }
  after_mfr_n <- nrow(dt)
  
  dt[, c("qrank", "fda_rank", "rept_rank", "version_rank") := NULL]
  
  # Final uniqueness should be by primaryid.
  dt <- unique(dt)
  if (uniqueN(dt$primaryid) != nrow(dt)) {
    stop("DEMO finalization did not produce unique primaryid rows", call. = FALSE)
  }
  
  list(
    demo = dt[],
    counts = data.table(
      step = c(
        "demo_loaded",
        "after_deleted_caseids",
        "after_latest_primaryid",
        "after_latest_caseid",
        "after_mfr_dedup"
      ),
      n_rows = c(
        initial_n,
        after_deleted_n,
        after_primaryid_n,
        after_caseid_n,
        after_mfr_n
      )
    )
  )
}

# ------------------------------- membership scans ---------------------------- #

collect_valid_drug_membership <- function(clean_index, keep_keys) {
  files <- get_table_files(clean_index, "DRUG")
  if (nrow(files) == 0L) {
    return(list(valid = empty_key_dt(), trial = empty_key_dt()))
  }
  
  keep_map <- make_keep_map(keep_keys)
  valid_keys <- vector("list", nrow(files))
  trial_keys <- vector("list", nrow(files))
  
  for (i in seq_len(nrow(files))) {
    p <- files$path[i]
    qtr_id <- files$quarter[i]
    
    keep_pids <- keep_map[[qtr_id]]
    if (is.null(keep_pids) || !length(keep_pids)) {
      valid_keys[[i]] <- empty_key_dt()
      trial_keys[[i]] <- empty_key_dt()
      next
    }
    
    dt <- read_parquet_dt(p)
    assert_has_cols(dt, "primaryid", paste0("DRUG file ", basename(p)))
    
    needed <- intersect(c("primaryid", "substance", "trial"), names(dt))
    dt <- dt[, ..needed]
    
    dt[, primaryid := normalize_text(primaryid)]
    if ("substance" %in% names(dt)) {
      dt[, substance := normalize_term(substance)]
    } else {
      dt[, substance := NA_character_]
    }
    
    if ("trial" %in% names(dt)) {
      dt[, trial := coerce_logical_flag(trial)]
    } else {
      dt[, trial := FALSE]
    }
    
    dt <- dt[primaryid %chin% keep_pids]
    
    valid_keys[[i]] <- unique(
      dt[
        !is.na(substance) & !substance %chin% INVALID_DRUG_SUBSTANCES,
        .(primaryid, quarter = qtr_id)
      ]
    )
    
    trial_keys[[i]] <- unique(
      dt[
        !is.na(trial) & trial,
        .(primaryid, quarter = qtr_id)
      ]
    )
    
    rm(dt)
    gc()
  }
  
  list(
    valid = unique(rbindlist(valid_keys, fill = TRUE)),
    trial = unique(rbindlist(trial_keys, fill = TRUE))
  )
}

collect_valid_reac_membership <- function(clean_index, keep_keys) {
  files <- get_table_files(clean_index, "REAC")
  if (nrow(files) == 0L) {
    return(empty_key_dt())
  }
  
  keep_map <- make_keep_map(keep_keys)
  valid_keys <- vector("list", nrow(files))
  
  for (i in seq_len(nrow(files))) {
    p <- files$path[i]
    qtr_id <- files$quarter[i]
    
    keep_pids <- keep_map[[qtr_id]]
    if (is.null(keep_pids) || !length(keep_pids)) {
      valid_keys[[i]] <- empty_key_dt()
      next
    }
    
    dt <- read_parquet_dt(p)
    assert_has_cols(dt, c("primaryid", "pt"), paste0("REAC file ", basename(p)))
    
    dt <- dt[, .(primaryid, pt)]
    dt[, primaryid := normalize_text(primaryid)]
    dt[, pt := normalize_term(pt)]
    dt <- dt[primaryid %chin% keep_pids]
    
    valid_keys[[i]] <- unique(
      dt[
        !is.na(pt) & !pt %chin% INVALID_REACTIONS,
        .(primaryid, quarter = qtr_id)
      ]
    )
    
    rm(dt)
    gc()
  }
  
  unique(rbindlist(valid_keys, fill = TRUE))
}

# ------------------------------- table streaming ----------------------------- #

stream_filter_write_primaryid_table <- function(clean_index, tbl_name, keep_keys) {
  files <- get_table_files(clean_index, tbl_name)
  if (nrow(files) == 0L) return(data.table())
  
  prepare_table_dir(tbl_name, overwrite = overwrite_master)
  
  keep_map <- make_keep_map(keep_keys)
  rows_out <- vector("list", nrow(files))
  
  for (i in seq_len(nrow(files))) {
    p <- files$path[i]
    qtr_id <- files$quarter[i]
    keep_pids <- keep_map[[qtr_id]]
    
    dt <- read_parquet_dt(p)
    src_n <- nrow(dt)
    
    assert_has_cols(dt, "primaryid", paste0(tbl_name, " file ", basename(p)))
    dt[, primaryid := normalize_text(primaryid)]
    
    if (is.null(keep_pids) || !length(keep_pids)) {
      dt <- dt[0]
    } else {
      dt <- dt[primaryid %chin% keep_pids]
    }
    
    if (tbl_name == "DRUG" && "substance" %in% names(dt)) {
      dt[, substance := normalize_term(substance)]
      dt <- dt[!is.na(substance) & !substance %chin% INVALID_DRUG_SUBSTANCES]
    }
    
    if (tbl_name == "REAC" && "pt" %in% names(dt)) {
      dt[, pt := normalize_term(pt)]
      dt <- dt[!is.na(pt) & !pt %chin% INVALID_REACTIONS]
    }
    
    dt <- unique(dt)
    
    res <- write_master_quarter(dt, tbl_name, qtr_id, source_rows = src_n)
    
    rows_out[[i]] <- data.table(
      output_table = tbl_name,
      quarter = qtr_id,
      out_file = res$out_file,
      source_rows = res$source_rows,
      written_rows = res$written_rows,
      status = res$status,
      timestamp = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
      notes = res$notes
    )
    
    rm(dt)
    gc()
  }
  
  rbindlist(rows_out, fill = TRUE)
}

stream_filter_write_deleted <- function(clean_index) {
  files <- get_table_files(clean_index, "DELETED")
  if (nrow(files) == 0L) return(data.table())
  
  prepare_table_dir("DELETED", overwrite = overwrite_master)
  rows_out <- vector("list", nrow(files))
  
  for (i in seq_len(nrow(files))) {
    p <- files$path[i]
    qtr_id <- files$quarter[i]
    
    dt <- read_parquet_dt(p)
    src_n <- nrow(dt)
    dt <- unique(dt)
    
    res <- write_master_quarter(dt, "DELETED", qtr_id, source_rows = src_n)
    
    rows_out[[i]] <- data.table(
      output_table = "DELETED",
      quarter = qtr_id,
      out_file = res$out_file,
      source_rows = res$source_rows,
      written_rows = res$written_rows,
      status = res$status,
      timestamp = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
      notes = res$notes
    )
    
    rm(dt)
    gc()
  }
  
  rbindlist(rows_out, fill = TRUE)
}

# ------------------------------- main ---------------------------------------- #

dir_create_safe(MASTER_ROOT)
manifest <- load_manifest(MANIFEST_CSV)
counts_list <- list()

clean_index <- list_parquet_files(CLEAN_ROOT)

if (overwrite_master) {
  for (tbl in c("DEMO", "DELETED", PRIMARYID_TABLES)) {
    out_dir <- file.path(MASTER_ROOT, tbl)
    if (dir.exists(out_dir)) unlink(out_dir, recursive = TRUE, force = TRUE)
  }
}

# 1) Load and finalize DEMO globally
message("Loading cleaned DEMO across quarters...")
demo_all <- load_all_demo(clean_index)

message("Loading cleaned DELETED caseids...")
deleted_caseids <- load_all_deleted_caseids(clean_index)

message("Finalizing DEMO globally...")
demo_final_obj <- finalize_demo_global(
  demo_dt = demo_all,
  deleted_caseids = deleted_caseids,
  apply_mfr_dedup = apply_mfr_dedup
)

demo_final <- demo_final_obj$demo
counts_list[[length(counts_list) + 1L]] <- copy(demo_final_obj$counts)

rm(demo_all, demo_final_obj)
gc()

# 2) Determine valid report membership from DRUG and REAC using canonical keys
demo_keys <- key_unique(demo_final[, .(primaryid, quarter)])

message("Collecting valid DRUG membership...")
drug_membership <- collect_valid_drug_membership(clean_index, keep_keys = demo_keys)

message("Collecting valid REAC membership...")
reac_valid_keys <- collect_valid_reac_membership(clean_index, keep_keys = demo_keys)

valid_complete_keys <- key_intersect(
  demo_keys,
  drug_membership$valid,
  reac_valid_keys
)

counts_list[[length(counts_list) + 1L]] <- data.table(
  step = c(
    "demo_before_complete_case_filter",
    "valid_drug_report_keys",
    "valid_reac_report_keys",
    "final_complete_report_keys"
  ),
  n_rows = c(
    nrow(demo_keys),
    nrow(unique(drug_membership$valid)),
    nrow(unique(reac_valid_keys)),
    nrow(unique(valid_complete_keys))
  )
)

# 3) Apply complete-case filter and final DEMO flags
message("Applying complete-case filter and final DEMO flags...")

setkey(valid_complete_keys, primaryid, quarter)
setkey(demo_final, primaryid, quarter)
demo_final <- demo_final[valid_complete_keys, nomatch = 0L]

trial_keys <- unique(copy(drug_membership$trial))
if (nrow(trial_keys)) {
  trial_keys[, premarketing := TRUE]
  setkey(trial_keys, primaryid, quarter)
  demo_final <- trial_keys[demo_final]
  demo_final[is.na(premarketing), premarketing := FALSE]
} else {
  demo_final[, premarketing := FALSE]
}

if ("lit_ref" %in% names(demo_final)) {
  demo_final[, literature := !is.na(lit_ref)]
} else {
  demo_final[, literature := FALSE]
}

demo_final <- unique(demo_final)

validate_demo_final(demo_final, deleted_caseids = deleted_caseids)

counts_list[[length(counts_list) + 1L]] <- data.table(
  step = c("final_demo_rows", "final_demo_primaryids", "final_demo_caseids"),
  n_rows = c(nrow(demo_final), uniqueN(demo_final$primaryid), uniqueN(demo_final$caseid))
)

# 4) Write canonical DEMO
message("Writing master DEMO...")
demo_manifest <- write_master_partitioned(demo_final, "DEMO")
demo_manifest[, output_table := "DEMO"]
demo_manifest[, timestamp := format(Sys.time(), "%Y-%m-%d %H:%M:%S")]
setcolorder(
  demo_manifest,
  c("output_table", "quarter", "out_file", "source_rows", "written_rows", "status", "timestamp", "notes")
)

manifest <- upsert_manifest(manifest, demo_manifest)
save_manifest(manifest, MANIFEST_CSV)

surviving_keys <- unique(demo_final[, .(primaryid, quarter)])

# 5) Write canonical DELETED for auditability
message("Writing master DELETED...")
deleted_manifest <- stream_filter_write_deleted(clean_index)
if (nrow(deleted_manifest)) {
  manifest <- upsert_manifest(manifest, deleted_manifest)
  save_manifest(manifest, MANIFEST_CSV)
}

# 6) Stream all primaryid-based tables using canonical (primaryid, quarter) membership
for (tbl_name in PRIMARYID_TABLES) {
  message("Writing master ", tbl_name, "...")
  tbl_manifest <- stream_filter_write_primaryid_table(clean_index, tbl_name, surviving_keys)
  
  if (nrow(tbl_manifest)) {
    manifest <- upsert_manifest(manifest, tbl_manifest)
    save_manifest(manifest, MANIFEST_CSV)
    
    counts_list[[length(counts_list) + 1L]] <- tbl_manifest[, .(
      step = paste0("master_", tbl_name, "_written"),
      n_rows = sum(fifelse(is.na(written_rows), 0L, written_rows))
    )]
  }
}

# 7) Write finalize counts
counts_dt <- rbindlist(counts_list, fill = TRUE, use.names = TRUE)
write_counts(counts_dt, COUNTS_CSV)

message("")
message("Done.")
message("Master root: ", MASTER_ROOT)
message("Manifest:    ", MANIFEST_CSV)
message("Counts:      ", COUNTS_CSV)