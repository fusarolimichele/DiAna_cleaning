# ------------------------------------------------------------------------------
# Script: 05_rule_based_dedup.R
# Purpose:
#   Add rule-based duplicate flags to the canonical FAERS master DEMO layer.
#
# Inputs:
#   - data_master/DEMO/DEMO_<YYQ#>.parquet
#   - data_master/DRUG/DRUG_<YYQ#>.parquet
#   - data_master/REAC/REAC_<YYQ#>.parquet
#   - optionally: External Sources/Dictionaries/MedDRA/meddra.csv
#
# Outputs:
#   - data_master/DEMO/DEMO_<YYQ#>.parquet   (updated in place with flags)
#   - data_master/rb_dedup_manifest.csv
#   - data_master/rb_dedup_counts.csv
#
# Flags added to DEMO:
#   - RB_duplicates
#   - RB_duplicates_only_susp
#
# Notes:
#   - This is a flagging step, not a filtering step.
#   - The audit asked to preserve the old duplicate-key construction:
#       * paste0(..., collapse = "; ")
#       * order(pt) / order(substance) before collapsing
#       * temp <- temp[order(fda_dt)] before grouping
#       * .GRP grouping semantics, including NA grouping
#   - This stage is global and memory-heavy by design.
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

DEMO_DIR <- file.path(MASTER_ROOT, "DEMO")
DRUG_DIR <- file.path(MASTER_ROOT, "DRUG")
REAC_DIR <- file.path(MASTER_ROOT, "REAC")

MANIFEST_CSV <- file.path(MASTER_ROOT, "rb_dedup_manifest.csv")
COUNTS_CSV   <- file.path(MASTER_ROOT, "rb_dedup_counts.csv")

MEDDRA_PRIMARY_CSV <- file.path(BASE_DIR, "external", "MedDRA", "meddra.csv")

overwrite_demo <- TRUE
parquet_compression <- "zstd"

# If REAC$pt is not already an ordered factor from parquet, restore the old
# ordering before `order(pt)` using the MedDRA primary PT list.
restore_pt_order_from_meddra <- TRUE

# ------------------------------- helpers ------------------------------------- #

list_table_parquet_files <- function(tbl_dir, tbl_name) {
  files <- list.files(
    path = tbl_dir,
    pattern = "\\.parquet$",
    full.names = TRUE
  )
  
  if (!length(files)) {
    stop("No parquet files found under: ", tbl_dir, call. = FALSE)
  }
  
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
    path = files,
    file_name = basename(files),
    table_name = vapply(parsed, `[[`, character(1), "table_name"),
    quarter = vapply(parsed, `[[`, character(1), "quarter")
  )
  
  dt <- dt[table_name == toupper(tbl_name)]
  if (nrow(dt) == 0L) {
    stop("No files found for table ", tbl_name, " under ", tbl_dir, call. = FALSE)
  }
  
  setorder(dt, quarter, file_name)
  dt[]
}

read_parquet_dt <- function(path, cols = NULL) {
  if (is.null(cols)) {
    dt <- as.data.table(read_parquet(path))
  } else {
    dt <- as.data.table(read_parquet(path, col_select = cols))
  }
  setDT(dt)
  dt
}

load_master_table <- function(tbl_name, cols = NULL, ensure_quarter = FALSE) {
  tbl_dir <- file.path(MASTER_ROOT, toupper(tbl_name))
  files <- list_table_parquet_files(tbl_dir, tbl_name)
  
  parts <- vector("list", nrow(files))
  
  for (i in seq_len(nrow(files))) {
    dt <- read_parquet_dt(files$path[i], cols = cols)
    
    if (ensure_quarter && !("quarter" %in% names(dt))) {
      dt[, quarter := files$quarter[i]]
    }
    
    parts[[i]] <- dt
  }
  
  out <- rbindlist(parts, use.names = TRUE, fill = TRUE)
  setDT(out)
  out
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

write_counts <- function(count_dt, path) fwrite(count_dt, path)

restore_pt_order <- function(reac_dt) {
  assert_has_cols(reac_dt, c("primaryid", "pt"), "REAC")
  
  if (is.ordered(reac_dt$pt)) {
    return(reac_dt)
  }
  
  if (!restore_pt_order_from_meddra) {
    reac_dt[, pt := as.character(pt)]
    return(reac_dt)
  }
  
  if (!file.exists(MEDDRA_PRIMARY_CSV)) {
    stop(
      "REAC$pt is not an ordered factor and MedDRA dictionary was not found at: ",
      MEDDRA_PRIMARY_CSV,
      "\nEither provide the dictionary or set restore_pt_order_from_meddra <- FALSE.",
      call. = FALSE
    )
  }
  
  meddra <- fread(MEDDRA_PRIMARY_CSV, sep = ";", na.strings = c("", "NA"),header = TRUE)
  assert_has_cols(meddra, "pt", "MedDRA dictionary")
  
  pt_levels <- unique(as.character(meddra$pt))
  pt_levels <- pt_levels[!is.na(pt_levels)]
  
  reac_dt[, pt := factor(as.character(pt), levels = pt_levels, ordered = TRUE)]
  reac_dt
}

build_temp_table <- function(demo_view, reac_dt, drug_dt) {
  # Preserve the old ordering-and-collapse logic.
  temp_reac <- unique(
    reac_dt[order(pt)][
      ,
      .(pt = paste0(as.character(pt), collapse = "; ")),
      by = "primaryid"
    ]
  )
  
  temp_drug_PS <- unique(
    drug_dt[order(substance)][
      role_cod == "PS",
      .(PS = paste0(as.character(substance), collapse = "; ")),
      by = "primaryid"
    ]
  )
  
  temp_drug_SS <- unique(
    drug_dt[order(substance)][
      role_cod == "SS",
      .(SS = paste0(as.character(substance), collapse = "; ")),
      by = "primaryid"
    ]
  )
  
  temp_drug_IC <- unique(
    drug_dt[order(substance)][
      role_cod %in% c("I", "C"),
      .(IC = paste0(as.character(substance), collapse = "; ")),
      by = "primaryid"
    ]
  )
  
  temp_drug_suspected <- unique(
    drug_dt[order(substance)][
      role_cod %in% c("PS", "SS"),
      .(suspected = paste0(as.character(substance), collapse = "; ")),
      by = "primaryid"
    ]
  )
  
  setkey(demo_view, primaryid)
  setkey(temp_reac, primaryid)
  setkey(temp_drug_PS, primaryid)
  setkey(temp_drug_SS, primaryid)
  setkey(temp_drug_IC, primaryid)
  setkey(temp_drug_suspected, primaryid)
  
  temp <- temp_reac[
    temp_drug_suspected[
      temp_drug_IC[
        temp_drug_SS[
          temp_drug_PS[demo_view, on = "primaryid"],
          on = "primaryid"
        ],
        on = "primaryid"
      ],
      on = "primaryid"
    ],
    on = "primaryid"
  ]
  
  list(
    temp = temp[],
    counts = data.table(
      step = c(
        "temp_reac_primaryids",
        "temp_drug_PS_primaryids",
        "temp_drug_SS_primaryids",
        "temp_drug_IC_primaryids",
        "temp_drug_suspected_primaryids",
        "temp_rows"
      ),
      n_rows = c(
        nrow(temp_reac),
        nrow(temp_drug_PS),
        nrow(temp_drug_SS),
        nrow(temp_drug_IC),
        nrow(temp_drug_suspected),
        nrow(temp)
      )
    )
  )
}

flag_duplicates <- function(temp, key_cols, flag_name) {
  assert_has_cols(temp, c("primaryid", "fda_dt", key_cols), paste0("temp for ", flag_name))
  
  # Preserve old behavior exactly:
  #   temp <- temp[order(fda_dt)]
  # This keeps base/data.table NA-last ordering semantics.
  temp <- copy(temp)
  temp <- temp[order(fda_dt)]
  
  # Preserve .GRP semantics, including grouping on NA values.
  temp_grouped <- temp[, DUP_ID := .GRP, by = key_cols]
  
  dup_sizes <- temp_grouped[, .N, by = "DUP_ID"]
  singlet_dup_ids <- dup_sizes[N == 1L, DUP_ID]
  
  singlets_pids <- temp_grouped[DUP_ID %in% singlet_dup_ids, primaryid]
  
  duplicates <- temp_grouped[!primaryid %chin% singlets_pids]
  
  duplicates_pids <- duplicates[
    duplicates[, .I[primaryid == last(primaryid)], by = "DUP_ID"]$V1,
    primaryid
  ]
  
  kept_pids <- c(singlets_pids, duplicates_pids)
  
  flags <- temp[, .(primaryid)]
  flags[, (flag_name) := !primaryid %chin% kept_pids]
  
  list(
    flags = flags[],
    counts = data.table(
      step = c(
        paste0(flag_name, "_groups"),
        paste0(flag_name, "_singlet_groups"),
        paste0(flag_name, "_duplicate_groups"),
        paste0(flag_name, "_singlet_primaryids"),
        paste0(flag_name, "_kept_duplicate_primaryids"),
        paste0(flag_name, "_flagged_true"),
        paste0(flag_name, "_flagged_false")
      ),
      n_rows = c(
        uniqueN(temp_grouped$DUP_ID),
        dup_sizes[N == 1L, .N],
        dup_sizes[N > 1L, .N],
        uniqueN(singlets_pids),
        uniqueN(duplicates_pids),
        sum(flags[[flag_name]]),
        sum(!flags[[flag_name]])
      )
    )
  )
}

write_demo_partitioned <- function(demo_dt) {
  assert_has_cols(demo_dt, c("primaryid", "quarter"), "updated DEMO")
  
  qtrs <- sort(unique(normalize_text(demo_dt$quarter)))
  qtrs <- qtrs[!is.na(qtrs)]
  
  if (!length(qtrs)) {
    stop("No valid quarter values found in DEMO", call. = FALSE)
  }
  
  rows_out <- vector("list", length(qtrs))
  
  for (i in seq_along(qtrs)) {
    qtr_id <- qtrs[i]
    chunk <- demo_dt[quarter == qtr_id]
    out_file <- file.path(DEMO_DIR, sprintf("DEMO_%s.parquet", qtr_id))
    
    if (file.exists(out_file) && !overwrite_demo) {
      rows_out[[i]] <- data.table(
        output_table = "DEMO",
        quarter = qtr_id,
        out_file = out_file,
        source_rows = nrow(chunk),
        written_rows = NA_integer_,
        status = "skipped_existing",
        timestamp = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
        notes = "existing DEMO output preserved"
      )
      next
    }
    
    if (file.exists(out_file) && overwrite_demo) {
      unlink(out_file, force = TRUE)
    }
    
    write_parquet(chunk, out_file, compression = parquet_compression)
    
    rows_out[[i]] <- data.table(
      output_table = "DEMO",
      quarter = qtr_id,
      out_file = out_file,
      source_rows = nrow(chunk),
      written_rows = nrow(chunk),
      status = "written",
      timestamp = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
      notes = NA_character_
    )
  }
  
  rbindlist(rows_out, fill = TRUE)
}

# ------------------------------- main ---------------------------------------- #

dir_create_safe(MASTER_ROOT)
manifest <- load_manifest(MANIFEST_CSV)
counts_list <- list()

message("Loading master DEMO...")
Demo <- load_master_table("DEMO", cols = NULL, ensure_quarter = TRUE)
assert_has_cols(
  Demo,
  c("primaryid", "quarter", "fda_dt", "event_dt", "sex", "reporter_country", "age_in_days", "wt_in_kgs"),
  "master DEMO"
)

Demo[, primaryid := normalize_text(primaryid)]
Demo[, quarter := normalize_text(quarter)]

if (uniqueN(Demo$primaryid) != nrow(Demo)) {
  stop("Master DEMO is not unique by primaryid; run finalize before RB dedup.", call. = FALSE)
}

message("Loading master REAC...")
Reac <- load_master_table("REAC", cols = c("primaryid", "pt"))
assert_has_cols(Reac, c("primaryid", "pt"), "master REAC")
Reac[, primaryid := normalize_text(primaryid)]
Reac <- Reac[!is.na(primaryid)]
Reac <- restore_pt_order(Reac)

message("Loading master DRUG...")
Drug <- load_master_table("DRUG", cols = c("primaryid", "role_cod", "substance"))
assert_has_cols(Drug, c("primaryid", "role_cod", "substance"), "master DRUG")
Drug[, primaryid := normalize_text(primaryid)]
Drug[, role_cod := as.character(role_cod)]
Drug[, substance := as.character(substance)]
Drug <- Drug[!is.na(primaryid)]

counts_list[[length(counts_list) + 1L]] <- data.table(
  step = c("demo_loaded", "reac_loaded", "drug_loaded"),
  n_rows = c(nrow(Demo), nrow(Reac), nrow(Drug))
)

# Smaller DEMO view for join construction; keep full DEMO for final writeback.
demo_view <- Demo[, .(
  primaryid,
  fda_dt,
  event_dt,
  sex,
  reporter_country,
  age_in_days,
  wt_in_kgs
)]

message("Building duplicate-signature table...")
temp_obj <- build_temp_table(
  demo_view = demo_view,
  reac_dt   = Reac,
  drug_dt   = Drug
)
temp <- temp_obj$temp
counts_list[[length(counts_list) + 1L]] <- temp_obj$counts

rm(temp_obj, demo_view, Reac, Drug)
gc()

# 1) Full conservative rule-based duplicates
message("Flagging RB_duplicates...")
complete_duplicates <- c(
  "event_dt", "sex", "reporter_country", "age_in_days", "wt_in_kgs",
  "pt", "PS", "SS", "IC"
)

rb1 <- flag_duplicates(
  temp = temp,
  key_cols = complete_duplicates,
  flag_name = "RB_duplicates"
)
counts_list[[length(counts_list) + 1L]] <- rb1$counts

setkey(Demo, primaryid)
setkey(rb1$flags, primaryid)
Demo[rb1$flags, RB_duplicates := i.RB_duplicates]
Demo[is.na(RB_duplicates), RB_duplicates := FALSE]

# 2) Suspect-drugs-only duplicate screen
message("Flagging RB_duplicates_only_susp...")
complete_duplicates <- c(
  "event_dt", "sex", "reporter_country", "age_in_days", "wt_in_kgs",
  "pt", "suspected"
)

rb2 <- flag_duplicates(
  temp = temp,
  key_cols = complete_duplicates,
  flag_name = "RB_duplicates_only_susp"
)
counts_list[[length(counts_list) + 1L]] <- rb2$counts

setkey(rb2$flags, primaryid)
Demo[rb2$flags, RB_duplicates_only_susp := i.RB_duplicates_only_susp]
Demo[is.na(RB_duplicates_only_susp), RB_duplicates_only_susp := FALSE]

rm(temp, rb1, rb2)
gc()

# Final checks
if (Demo[, any(is.na(RB_duplicates))]) {
  stop("RB_duplicates contains NA values after assignment", call. = FALSE)
}

if (Demo[, any(is.na(RB_duplicates_only_susp))]) {
  stop("RB_duplicates_only_susp contains NA values after assignment", call. = FALSE)
}

counts_list[[length(counts_list) + 1L]] <- data.table(
  step = c(
    "final_demo_rows",
    "RB_duplicates_true",
    "RB_duplicates_false",
    "RB_duplicates_only_susp_true",
    "RB_duplicates_only_susp_false"
  ),
  n_rows = c(
    nrow(Demo),
    Demo[RB_duplicates == TRUE, .N],
    Demo[RB_duplicates == FALSE, .N],
    Demo[RB_duplicates_only_susp == TRUE, .N],
    Demo[RB_duplicates_only_susp == FALSE, .N]
  )
)

# Write updated DEMO parquet files back by quarter
message("Writing updated DEMO parquet partitions...")
demo_manifest <- write_demo_partitioned(Demo)
manifest <- upsert_manifest(manifest, demo_manifest)
save_manifest(manifest, MANIFEST_CSV)

# Write counts
counts_dt <- rbindlist(counts_list, fill = TRUE, use.names = TRUE)
write_counts(counts_dt, COUNTS_CSV)

message("")
message("Done.")
message("Updated DEMO dir: ", DEMO_DIR)
message("Manifest:        ", MANIFEST_CSV)
message("Counts:          ", COUNTS_CSV)
