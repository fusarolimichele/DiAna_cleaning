# ------------------------------------------------------------------------------
# Script: 06_merge_quarters.R
# Purpose:
#   Consolidate quarter-partitioned master parquet tables into one file per table.
#   Supports RDS, parquet, or both output formats, controlled by output_format.
#
# Inputs:
#   - data_master/<TABLE>/<TABLE>_<YYQ#>.parquet
#
# Outputs (depending on output_format):
#   - data_rds/<TABLE>.rds              (output_format "rds" or "both")
#   - data_rds/<TABLE>.parquet          (output_format "parquet" or "both")
#   - data_rds/faers_master_db_index.rds
#   - data_rds/consolidate_manifest.csv
#   - data_rds/consolidate_counts.csv
#
# Notes:
#   - This script avoids building one giant in-memory database object.
#   - When output_format includes "rds", tables are materialised into R for type
#     restoration before saving; parquet output uses the Arrow path (no R heap).
#   - Tables in parquet_tables are never materialised into R; they are always
#     written as parquet regardless of output_format, and never as RDS.
#   - saveRDS(..., compress = FALSE) is the default to reduce memory pressure.
# ------------------------------------------------------------------------------

suppressPackageStartupMessages({
  library(data.table)
  library(arrow)
})

options(timeout = 1200)

# ------------------------------- configuration ------------------------------- #

BASE_DIR <- "."
source(file.path(BASE_DIR, "R", "utils.R"))

MASTER_ROOT <- file.path(BASE_DIR, "data_master")
RDS_ROOT    <- file.path(BASE_DIR, "data_rds")

MANIFEST_CSV <- file.path(RDS_ROOT, "consolidate_manifest.csv")
COUNTS_CSV   <- file.path(RDS_ROOT, "consolidate_counts.csv")
DB_INDEX_RDS <- file.path(RDS_ROOT, "faers_master_db_index.rds")

# Output format: one of "rds", "parquet", "both"
output_format <- "both"

# Applies to both RDS and parquet outputs
overwrite_output <- FALSE

# RDS-specific: strongly recommended FALSE for very large tables
compress_rds <- FALSE

# Parquet-specific compression codec
parquet_compression <- "zstd"

# Optional lightweight DB index (always written as RDS; metadata only)
save_database_index <- TRUE

# Expected tables from the canonical master layer
TABLES <- c(
  "DEMO",
  "DELETED",
  "DRUG",
  "DRUG_INFO",
  "REAC",
  "INDI",
  "OUTC",
  "THER",
  "RPSR"
)

include_all_detected_tables <- TRUE

# Type restoration applied before RDS save; parquet preserves types natively
# so restore_ordered_factors has no effect when output_format is "parquet".
restore_ordered_factors <- TRUE

MEDDRA_PRIMARY_CSV <- file.path(
  BASE_DIR,
  "External Sources", "Dictionaries", "MedDRA", "meddra.csv"
)

ROLE_COD_LEVELS <- c("C", "I", "SS", "PS")

OUTC_LEVELS <- c("OT", "CA", "HO", "RI", "DS", "LT", "DE")

# Sorting can be expensive on very large tables. Keep FALSE unless needed.
sort_tables_before_save <- FALSE

# More expensive relational check; off by default for low-memory runs.
check_drug_druginfo_key_set <- FALSE

# Tables too large to materialise in R's heap. These are always written as
# parquet regardless of output_format, and are never written as RDS.
parquet_tables <- c("DRUG_INFO")

# ------------------------------- validate config ------------------------------ #

if (!output_format %in% c("rds", "parquet", "both")) {
  stop("output_format must be one of: \"rds\", \"parquet\", \"both\"", call. = FALSE)
}

wants_rds     <- output_format %in% c("rds",  "both")
wants_parquet <- output_format %in% c("parquet", "both")

# ------------------------------- helpers ------------------------------------- #

list_detected_table_dirs <- function(root_dir) {
  if (!dir.exists(root_dir)) return(character())
  dirs <- list.dirs(root_dir, recursive = FALSE, full.names = FALSE)
  dirs <- toupper(dirs)
  dirs[nzchar(dirs)]
}

resolve_tables <- function() {
  tables <- toupper(TABLES)

  if (include_all_detected_tables && dir.exists(MASTER_ROOT)) {
    detected <- list_detected_table_dirs(MASTER_ROOT)
    tables <- union(tables, detected)
  }

  tables
}

list_table_files <- function(tbl_name) {
  tbl_dir <- file.path(MASTER_ROOT, toupper(tbl_name))
  if (!dir.exists(tbl_dir)) return(data.table())

  files <- list.files(
    path = tbl_dir,
    pattern = "\\.parquet$",
    full.names = TRUE
  )

  if (!length(files)) return(data.table())

  parsed <- lapply(basename(files), parse_file_name)
  ok <- vapply(parsed, `[[`, logical(1), "ok")

  if (!all(ok)) {
    bad <- basename(files)[!ok]
    stop(
      "Found parquet files with unexpected names in ",
      tbl_dir,
      ". Expected <TABLE>_<YYQ#>.parquet. Bad files: ",
      paste(bad, collapse = ", "),
      call. = FALSE
    )
  }

  dt <- data.table(
    path       = files,
    file_name  = basename(files),
    table_name = vapply(parsed, `[[`, character(1), "table_name"),
    quarter    = vapply(parsed, `[[`, character(1), "quarter")
  )

  dt <- dt[table_name == toupper(tbl_name)]
  setorder(dt, quarter, file_name)
  dt[]
}

load_manifest <- function(path) {
  if (!file.exists(path)) {
    return(data.table(
      output_table = character(),
      format       = character(),
      out_file     = character(),
      source_files = integer(),
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
    "output_table", "format", "out_file", "source_files", "source_rows",
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

  manifest <- manifest[!row_dt[, .(out_file)], on = "out_file"]
  rbindlist(list(manifest, row_dt), fill = TRUE, use.names = TRUE)
}

write_counts <- function(count_dt, path) fwrite(count_dt, path)

load_meddra_primary_levels <- function(path) {
  if (!file.exists(path)) return(NULL)

  meddra <- fread(path, sep = ";", na.strings = c("", "NA"), header = TRUE)
  assert_has_cols(meddra, "pt", "MedDRA dictionary")

  lvls <- unique(as.character(meddra$pt))
  lvls[!is.na(lvls)]
}

restore_table_types <- function(dt, tbl_name, meddra_levels = NULL) {
  tbl_name <- toupper(tbl_name)

  to_factor <- function(col) {
    if (col %in% names(dt)) dt[, (col) := factor(as.character(dt[[col]]))]
  }
  to_int <- function(col) {
    if (col %in% names(dt))
      dt[, (col) := suppressWarnings(as.integer(as.character(dt[[col]])))]
  }
  to_num <- function(col) {
    if (col %in% names(dt))
      dt[, (col) := suppressWarnings(as.numeric(as.character(dt[[col]])))]
  }

  # ---- ordered factors (gated by restore_ordered_factors) ----
  if (restore_ordered_factors) {
    if (tbl_name == "REAC" && !is.null(meddra_levels)) {
      if ("pt"           %in% names(dt)) dt[, pt           := factor(as.character(pt),           meddra_levels, ordered = TRUE)]
      if ("drug_rec_act" %in% names(dt)) dt[, drug_rec_act := factor(as.character(drug_rec_act), meddra_levels, ordered = TRUE)]
    }
    if (tbl_name == "INDI" && !is.null(meddra_levels)) {
      if ("indi_pt" %in% names(dt)) dt[, indi_pt := factor(as.character(indi_pt), meddra_levels, ordered = TRUE)]
    }
    if (tbl_name == "DRUG") {
      if ("role_cod" %in% names(dt)) dt[, role_cod := factor(as.character(role_cod), ROLE_COD_LEVELS, ordered = TRUE)]
    }
    if (tbl_name == "OUTC" && !is.null(OUTC_LEVELS)) {
      if ("outc_cod" %in% names(dt)) dt[, outc_cod := factor(as.character(outc_cod), OUTC_LEVELS, ordered = TRUE)]
    }
  }

  # ---- unordered factors ----
  if (tbl_name == "DEMO") {
    for (col in c("sex", "age_grp", "occp_cod", "rept_cod", "occr_country",
                  "reporter_country", "i_f_cod", "e_sub", "quarter", "caseversion"))
      to_factor(col)
  }
  if (tbl_name == "DRUG") {
    for (col in c("substance", "drugname", "prod_ai")) to_factor(col)
  }
  if (tbl_name == "DRUG_INFO") {
    for (col in c("dechal", "rechal", "route", "dose_form", "dose_freq")) to_factor(col)
  }

  # ---- integer / numeric conversions ----
  if (tbl_name == "DEMO") {
    for (col in c("event_dt", "fda_dt", "init_fda_dt", "mfr_dt", "rept_dt")) to_int(col)
  }
  if (tbl_name == "DRUG_INFO") {
    for (col in c("drug_seq", "val_vbm", "exp_dt")) to_int(col)
  }
  if (tbl_name %in% c("DRUG", "INDI", "THER")) {
    to_int("drug_seq")
  }
  if (tbl_name == "THER") {
    to_int("event_dt")
    for (col in c("start_dt", "end_dt")) to_num(col)
  }

  dt
}

sort_table_for_save <- function(dt, tbl_name) {
  if (!sort_tables_before_save || nrow(dt) == 0L) return(dt)

  preferred <- c(
    "primaryid", "caseid", "drug_seq", "indi_drug_seq",
    "start_dt", "end_dt", "quarter"
  )
  cols <- intersect(preferred, names(dt))

  if (length(cols)) {
    setorderv(dt, cols = cols, order = rep(1L, length(cols)), na.last = TRUE)
  }

  dt
}

# Reads all quarter parquets into a single data.table (materialises into R heap).
# Applies type restoration and optional sorting. Used for RDS output.
read_partitioned_table <- function(tbl_name, meddra_levels = NULL) {
  files <- list_table_files(tbl_name)
  if (nrow(files) == 0L) {
    return(list(data = NULL, source_files = 0L, source_rows = 0L))
  }

  arrow_tables <- vector("list", nrow(files))
  src_rows     <- integer(nrow(files))

  for (i in seq_len(nrow(files))) {
    tbl             <- read_parquet(files$path[i], as_data_frame = FALSE)
    src_rows[i]     <- tbl$num_rows
    arrow_tables[[i]] <- tbl
  }

  dt <- setDT(as.data.frame(do.call(concat_tables, arrow_tables)))
  rm(arrow_tables)
  gc()

  dt <- restore_table_types(dt, tbl_name, meddra_levels = meddra_levels)
  dt <- sort_table_for_save(dt, tbl_name)

  list(
    data         = dt,
    source_files = nrow(files),
    source_rows  = sum(src_rows)
  )
}

# Reads all quarter parquets into a single Arrow Table (stays off R heap).
# Used for parquet output and for tables in parquet_tables.
read_partitioned_arrow <- function(tbl_name) {
  files <- list_table_files(tbl_name)
  if (nrow(files) == 0L) {
    return(list(data = NULL, source_files = 0L, source_rows = 0L))
  }

  arrow_tbls <- vector("list", nrow(files))
  src_rows   <- integer(nrow(files))

  for (i in seq_len(nrow(files))) {
    tbl             <- read_parquet(files$path[i], as_data_frame = FALSE)
    src_rows[i]     <- tbl$num_rows
    arrow_tbls[[i]] <- tbl
  }

  combined <- do.call(concat_tables, arrow_tbls)
  rm(arrow_tbls)
  gc()

  list(data = combined, source_files = nrow(files), source_rows = sum(src_rows))
}

# Saves a data.table to RDS.
save_table_rds <- function(dt, tbl_name) {
  out_file <- file.path(RDS_ROOT, sprintf("%s.rds", toupper(tbl_name)))

  if (file.exists(out_file) && !overwrite_output) {
    return(list(
      out_file     = out_file,
      written_rows = NA_integer_,
      status       = "skipped_existing",
      format       = "rds",
      notes        = "existing RDS preserved"
    ))
  }

  if (file.exists(out_file) && overwrite_output) unlink(out_file, force = TRUE)

  gc()
  saveRDS(dt, out_file, compress = compress_rds)

  list(
    out_file     = out_file,
    written_rows = nrow(dt),
    status       = "written",
    format       = "rds",
    notes        = if (isTRUE(compress_rds)) "compressed" else "uncompressed"
  )
}

# Saves an Arrow Table or data.table to a single consolidated parquet file.
save_table_parquet <- function(x, tbl_name) {
  out_file <- file.path(RDS_ROOT, sprintf("%s.parquet", toupper(tbl_name)))

  if (file.exists(out_file) && !overwrite_output) {
    return(list(
      out_file     = out_file,
      written_rows = NA_integer_,
      status       = "skipped_existing",
      format       = "parquet",
      notes        = "existing parquet preserved"
    ))
  }

  if (file.exists(out_file) && overwrite_output) unlink(out_file, force = TRUE)

  write_parquet(x, out_file, compression = parquet_compression)

  n_rows <- if (inherits(x, "ArrowTabular")) as.integer(x$num_rows) else nrow(x)

  list(
    out_file     = out_file,
    written_rows = n_rows,
    status       = "written",
    format       = "parquet",
    notes        = parquet_compression
  )
}

# ------------------------------- main ---------------------------------------- #

dir_create_safe(RDS_ROOT)

manifest    <- load_manifest(MANIFEST_CSV)
counts_list <- list()

tables <- resolve_tables()
if (!length(tables)) {
  stop("No table directories found under: ", MASTER_ROOT, call. = FALSE)
}

meddra_levels <- load_meddra_primary_levels(MEDDRA_PRIMARY_CSV)

demo_primaryids  <- NULL
table_primaryids <- list()
drug_keys        <- NULL
drug_info_keys   <- NULL

for (tbl_name in tables) {
  message("Consolidating ", tbl_name, "...")

  # Use Arrow-only path when RDS output is not needed, or when the table is
  # too large to materialise. Arrow-only means parquet is always written.
  use_arrow_only <- !wants_rds || tbl_name %in% parquet_tables

  if (use_arrow_only && wants_rds && tbl_name %in% parquet_tables) {
    message(sprintf(
      "Note: %s is in parquet_tables and cannot be written as RDS; writing parquet only.",
      tbl_name
    ))
  }

  if (use_arrow_only) {
    obj <- read_partitioned_arrow(tbl_name)
    if (is.null(obj$data)) {
      message(sprintf("  No quarter files found for %s, skipping.", tbl_name))
      next
    }

    arrow_tbl <- obj$data
    col_names <- arrow_tbl$schema$names

    counts_list[[length(counts_list) + 1L]] <- data.table(
      step   = c(
        paste0(tbl_name, "_source_files"), paste0(tbl_name, "_source_rows"),
        paste0(tbl_name, "_final_rows"),   paste0(tbl_name, "_final_cols")
      ),
      n_rows = c(
        obj$source_files, obj$source_rows,
        as.integer(arrow_tbl$num_rows), as.integer(arrow_tbl$num_columns)
      )
    )

    if ("primaryid" %in% col_names) {
      pids <- unique(as.character(as.vector(arrow_tbl[["primaryid"]])))
      pids <- pids[!is.na(pids)]
      table_primaryids[[tbl_name]] <- pids
      counts_list[[length(counts_list) + 1L]] <- data.table(
        step = paste0(tbl_name, "_unique_primaryids"), n_rows = length(pids)
      )
    }

    if (check_drug_druginfo_key_set &&
        all(c("primaryid", "drug_seq") %in% col_names)) {
      drug_info_keys <- unique(data.table(
        primaryid = as.character(as.vector(arrow_tbl[["primaryid"]])),
        drug_seq  = as.character(as.vector(arrow_tbl[["drug_seq"]]))
      ))
    }

    res_list <- list(save_table_parquet(arrow_tbl, tbl_name))
    rm(arrow_tbl)

  } else {
    # R path: materialise into data.table for type restoration, then write
    # RDS and/or parquet from the in-memory table.
    obj <- read_partitioned_table(tbl_name, meddra_levels = meddra_levels)
    if (is.null(obj$data)) {
      message(sprintf("  No quarter files found for %s, skipping.", tbl_name))
      next
    }

    dt <- obj$data

    counts_list[[length(counts_list) + 1L]] <- data.table(
      step   = c(
        paste0(tbl_name, "_source_files"), paste0(tbl_name, "_source_rows"),
        paste0(tbl_name, "_final_rows"),   paste0(tbl_name, "_final_cols")
      ),
      n_rows = c(obj$source_files, obj$source_rows, nrow(dt), ncol(dt))
    )

    if ("primaryid" %in% names(dt)) {
      pids <- safe_unique_char(dt$primaryid)
      table_primaryids[[tbl_name]] <- pids
      counts_list[[length(counts_list) + 1L]] <- data.table(
        step = paste0(tbl_name, "_unique_primaryids"), n_rows = length(pids)
      )

      if (tbl_name == "DEMO") {
        demo_primaryids <- pids
        if (uniqueN(dt$primaryid) != nrow(dt)) {
          stop("DEMO is not unique by primaryid", call. = FALSE)
        }
        counts_list[[length(counts_list) + 1L]] <- data.table(
          step = "DEMO_primaryid_unique_check", n_rows = 1L
        )
      }
    }

    if (check_drug_druginfo_key_set && tbl_name %in% c("DRUG", "DRUG_INFO")) {
      if (all(c("primaryid", "drug_seq") %in% names(dt))) {
        key_dt <- unique(dt[, .(
          primaryid = as.character(primaryid),
          drug_seq  = as.character(drug_seq)
        )])
        if (tbl_name == "DRUG") drug_keys <- key_dt else drug_info_keys <- key_dt
      }
    }

    res_list <- list()
    if (wants_rds)     res_list <- c(res_list, list(save_table_rds(dt, tbl_name)))
    if (wants_parquet) res_list <- c(res_list, list(save_table_parquet(dt, tbl_name)))

    rm(dt)
  }

  for (res in res_list) {
    manifest_row <- data.table(
      output_table = tbl_name,
      format       = res$format,
      out_file     = res$out_file,
      source_files = obj$source_files,
      source_rows  = obj$source_rows,
      written_rows = res$written_rows,
      status       = res$status,
      timestamp    = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
      notes        = res$notes
    )
    manifest <- upsert_manifest(manifest, manifest_row)
  }

  save_manifest(manifest, MANIFEST_CSV)

  rm(obj, res_list)
  gc()
}

# --------------------------- post-hoc relational checks ---------------------- #

message("Running lightweight relational checks...")

if (!is.null(demo_primaryids)) {
  child_tables <- intersect(
    c("DRUG", "DRUG_INFO", "REAC", "INDI", "OUTC", "THER", "RPSR"),
    names(table_primaryids)
  )

  for (tbl in child_tables) {
    missing_pids <- setdiff(table_primaryids[[tbl]], demo_primaryids)

    counts_list[[length(counts_list) + 1L]] <- data.table(
      step   = paste0(tbl, "_primaryids_not_in_demo"),
      n_rows = length(missing_pids)
    )

    if (length(missing_pids)) {
      stop(
        "Table ", tbl, " contains primaryids not present in DEMO: ",
        paste(head(missing_pids, 10), collapse = ", "),
        if (length(missing_pids) > 10) " ..." else "",
        call. = FALSE
      )
    }
  }
}

if (check_drug_druginfo_key_set && !is.null(drug_keys) && !is.null(drug_info_keys)) {
  same_keys <- setequal(drug_keys, drug_info_keys)

  counts_list[[length(counts_list) + 1L]] <- data.table(
    step   = "DRUG_DRUG_INFO_key_set_equal",
    n_rows = as.integer(same_keys)
  )

  if (!same_keys) {
    stop("DRUG and DRUG_INFO do not share the same primaryid+drug_seq set.", call. = FALSE)
  }
}

# --------------------------- save lightweight DB index ----------------------- #

if (save_database_index) {
  db_index <- list(
    root                    = RDS_ROOT,
    tables                  = tables,
    output_format           = output_format,
    created_at              = Sys.time(),
    compress_rds            = compress_rds,
    parquet_compression     = parquet_compression,
    restore_ordered_factors = restore_ordered_factors,
    sort_tables_before_save = sort_tables_before_save
  )

  if (file.exists(DB_INDEX_RDS) && overwrite_output) unlink(DB_INDEX_RDS, force = TRUE)

  if (!file.exists(DB_INDEX_RDS) || overwrite_output) {
    saveRDS(db_index, DB_INDEX_RDS, compress = compress_rds)
  }
}

counts_dt <- rbindlist(counts_list, fill = TRUE, use.names = TRUE)
write_counts(counts_dt, COUNTS_CSV)

message("")
message("Done.")
message("Output root: ", RDS_ROOT)
message("Format:      ", output_format)
message("Manifest:    ", MANIFEST_CSV)
message("Counts:      ", COUNTS_CSV)
if (save_database_index) {
  message("DB index:    ", DB_INDEX_RDS)
}
