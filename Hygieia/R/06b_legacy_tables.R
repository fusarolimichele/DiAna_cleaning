# ------------------------------------------------------------------------------
# Script: 06b_legacy_tables.R
# Purpose:
#   Reshape the consolidated master tables (data_rds/) into the legacy table
#   structure matching old_cleaning_scriptv2.R's final output.
#
# Inputs:
#   - data_rds/DEMO.rds/.parquet
#   - data_rds/DRUG.rds/.parquet
#   - data_rds/DRUG_INFO.rds/.parquet
#   - data_rds/REAC.rds/.parquet
#   - data_rds/INDI.rds/.parquet
#   - data_rds/OUTC.rds/.parquet
#   - data_rds/THER.rds/.parquet
#   - data_rds/RPSR.rds/.parquet
#
# Outputs (in data_legacy/):
#   - DEMO.rds         — clinical subset (sex, age, wt, country, dates, flags)
#   - DEMO_SUPP.rds    — admin subset (ids, dates, reporter info) + rpsr_cod
#   - DRUG.rds         — substance + role_cod (ordered factor)
#   - DRUG_NAME.rds    — drugname, prod_ai, val_vbm, nda_num
#   - DOSES.rds        — dose amounts, units, frequency
#   - DRUG_SUPP.rds    — route, dose_form, dechal/rechal, lot/exp
#   - REAC.rds         — pt, drug_rec_act (ordered factors)
#   - INDI.rds         — indi_pt (ordered factor)
#   - OUTC.rds         — outc_cod (ordered factor)
#   - THER.rds         — dates, dur_in_days, time_to_onset, event_dt
#
# Notes:
#   - No new cleaning is applied; this is a pure reshaping step.
#   - RPSR is absorbed into DEMO_SUPP (no standalone RPSR in legacy format).
#   - DELETED is not written in legacy format.
#   - DRUG_INFO is read with col_select to avoid full materialisation.
# ------------------------------------------------------------------------------

suppressPackageStartupMessages({
  library(data.table)
  library(arrow)
})

options(timeout = 1200)

# ------------------------------- configuration ------------------------------- #

BASE_DIR <- "."
source(file.path(BASE_DIR, "R", "utils.R"))

RDS_ROOT    <- file.path(BASE_DIR, "data_rds")
LEGACY_ROOT <- file.path(BASE_DIR, "data_legacy")

# Format of consolidated master files — must match 06_merge_quarters.R output.
#   "auto"    — try parquet first, fall back to RDS
#   "parquet" — parquet only
#   "rds"     — RDS only
input_format <- "auto"

overwrite_legacy  <- FALSE

# TRUE uses gzip compression (default R level 6) — recommended.
# FALSE writes uncompressed RDS, which is much larger but faster to read back.
# The source parquet files use zstd columnar compression, so FALSE produces
# files that are many times larger than their parquet equivalents.
compress_legacy <- TRUE

MEDDRA_PRIMARY_CSV <- file.path(
  BASE_DIR, "external", "MedDRA", "meddra.csv"
)

ROLE_COD_LEVELS <- c("C", "I", "SS", "PS")
OUTC_LEVELS     <- c("OT", "CA", "HO", "RI", "DS", "LT", "DE")

# ------------------------------- helpers ------------------------------------- #

load_master <- function(tbl_name, col_select = NULL) {
  tbl_upper    <- toupper(tbl_name)
  parquet_path <- file.path(RDS_ROOT, sprintf("%s.parquet", tbl_upper))
  rds_path     <- file.path(RDS_ROOT, sprintf("%s.rds",     tbl_upper))

  if (input_format == "parquet" ||
      (input_format == "auto" && file.exists(parquet_path))) {
    if (!file.exists(parquet_path)) return(NULL)
    if (is.null(col_select)) {
      return(as.data.table(read_parquet(parquet_path)))
    }
    return(as.data.table(read_parquet(parquet_path, col_select = col_select)))
  }

  if (!file.exists(rds_path)) return(NULL)
  x <- readRDS(rds_path)
  if (!is.data.table(x)) setDT(x)
  if (!is.null(col_select)) {
    cols <- intersect(col_select, names(x))
    x <- x[, ..cols]
  }
  x
}

save_legacy <- function(dt, tbl_name) {
  out_file <- file.path(LEGACY_ROOT, sprintf("%s.rds", toupper(tbl_name)))

  if (file.exists(out_file) && !overwrite_legacy) {
    message(sprintf("Skipping existing %s", basename(out_file)))
    return(invisible(out_file))
  }

  saveRDS(dt, out_file, compress = compress_legacy)
  message(sprintf(
    "Written: %-14s  %s rows",
    basename(out_file),
    format(nrow(dt), big.mark = ",")
  ))
  invisible(out_file)
}

keep_cols <- function(dt, cols) {
  present <- intersect(cols, names(dt))
  dt[, ..present]
}

to_factor <- function(dt, col) {
  if (col %in% names(dt)) dt[, (col) := factor(as.character(dt[[col]]))]
}
to_int <- function(dt, col) {
  if (col %in% names(dt))
    dt[, (col) := suppressWarnings(as.integer(as.character(dt[[col]])))]
}
to_num <- function(dt, col) {
  if (col %in% names(dt))
    dt[, (col) := suppressWarnings(as.numeric(as.character(dt[[col]])))]
}

load_meddra_pt_levels <- function(path) {
  if (!file.exists(path)) return(NULL)
  m <- fread(path, sep = ";", na.strings = c("", "NA"),header = TRUE)
  setnames(m, tolower(trimws(names(m))))
  if (!"pt" %in% names(m)) return(NULL)
  unique(as.character(m$pt[!is.na(m$pt)]))
}

# ------------------------------- main ---------------------------------------- #

dir_create_safe(LEGACY_ROOT)

# ---- DEMO / DEMO_SUPP -------------------------------------------------------

message("Processing DEMO / DEMO_SUPP...")

demo <- load_master("DEMO")
if (is.null(demo)) stop("DEMO not found in ", RDS_ROOT, call. = FALSE)

# Subset for legacy DEMO (clinical fields only)
DEMO_COLS <- c(
  "primaryid", "sex", "age_in_days", "wt_in_kgs", "occr_country",
  "event_dt", "occp_cod", "reporter_country", "rept_cod",
  "init_fda_dt", "fda_dt", "premarketing", "literature",
  "RB_duplicates", "RB_duplicates_only_susp"
)

# Subset for legacy DEMO_SUPP (administrative fields)
DEMO_SUPP_COLS <- c(
  "primaryid", "caseid", "caseversion", "i_f_cod", "auth_num",
  "e_sub", "lit_ref", "rept_dt", "to_mfr", "mfr_sndr",
  "mfr_num", "mfr_dt", "quarter"
)

demo_supp <- keep_cols(demo, DEMO_SUPP_COLS)

# Join rpsr_cod into DEMO_SUPP (matches old script: right join from RPSR)
rpsr <- load_master("RPSR", col_select = c("primaryid", "rpsr_cod"))
if (!is.null(rpsr)) {
  rpsr[, rpsr_cod := factor(as.character(rpsr_cod))]
  demo_supp <- rpsr[demo_supp, on = "primaryid"]
  rm(rpsr)
}

# caseid is integer in old format; master DEMO stores it as character
to_int(demo_supp, "caseid")
save_legacy(demo_supp, "DEMO_SUPP")
rm(demo_supp)
gc()

# Keep event_dt map for THER join before narrowing DEMO
event_dt_map <- unique(demo[!is.na(event_dt), .(primaryid, event_dt)])

demo_pids <- safe_unique_char(demo$primaryid)
save_legacy(unique(keep_cols(demo, DEMO_COLS)), "DEMO")
rm(demo)
gc()

# ---- DRUG / DRUG_NAME -------------------------------------------------------

message("Processing DRUG / DRUG_NAME...")

drug <- load_master("DRUG")
if (is.null(drug)) stop("DRUG not found in ", RDS_ROOT, call. = FALSE)

drug <- drug[as.character(primaryid) %chin% demo_pids]

# DRUG_NAME: drugname + prod_ai (val_vbm + nda_num added from DRUG_INFO below)
drug_name <- unique(keep_cols(drug, c("primaryid", "drug_seq", "drugname", "prod_ai")))

# DRUG: substance + role_cod only
drug_out <- unique(keep_cols(drug, c("primaryid", "drug_seq", "substance", "role_cod")))
if ("role_cod" %in% names(drug_out)) {
  drug_out[, role_cod := factor(as.character(role_cod),
                                levels = ROLE_COD_LEVELS, ordered = TRUE)]
}
save_legacy(drug_out, "DRUG")
rm(drug, drug_out)
gc()

# ---- DRUG_INFO-derived tables (three separate passes to limit memory) -------

# Pass 1: DRUG_NAME (val_vbm + nda_num joined onto drugname + prod_ai from DRUG)
message("Processing DRUG_INFO → DRUG_NAME...")

drug_info_p1 <- load_master("DRUG_INFO", col_select = c("primaryid", "drug_seq", "val_vbm", "nda_num"))
if (is.null(drug_info_p1)) stop("DRUG_INFO not found in ", RDS_ROOT, call. = FALSE)
drug_info_p1 <- drug_info_p1[as.character(primaryid) %chin% demo_pids]
to_int(drug_info_p1, "drug_seq")   # align type with drug_name before join
drug_name_full <- drug_info_p1[drug_name, on = c("primaryid", "drug_seq")]
to_int(drug_name_full, "val_vbm")
save_legacy(unique(drug_name_full), "DRUG_NAME")
rm(drug_info_p1, drug_name, drug_name_full)
gc()

# Pass 2: DOSES
message("Processing DRUG_INFO → DOSES...")

drug_info_p2 <- load_master("DRUG_INFO", col_select = c(
  "primaryid", "drug_seq",
  "dose_vbm", "cum_dose_unit", "cum_dose_chr", "dose_amt", "dose_unit", "dose_freq"
))
to_int(drug_info_p2, "drug_seq")
drug_info_p2 <- drug_info_p2[as.character(primaryid) %chin% demo_pids]
to_factor(drug_info_p2, "dose_freq")
to_num(drug_info_p2, "cum_dose_chr")
save_legacy(unique(drug_info_p2), "DOSES")
rm(drug_info_p2)
gc()

# Pass 3: DRUG_SUPP
message("Processing DRUG_INFO → DRUG_SUPP...")

drug_info_p3 <- load_master("DRUG_INFO", col_select = c(
  "primaryid", "drug_seq",
  "route", "dose_form", "dechal", "rechal", "lot_num", "exp_dt"
))
to_int(drug_info_p3, "drug_seq")
drug_info_p3 <- drug_info_p3[as.character(primaryid) %chin% demo_pids]
to_int(drug_info_p3, "exp_dt")
for (col in c("route", "dose_form", "dechal", "rechal")) to_factor(drug_info_p3, col)
save_legacy(unique(drug_info_p3), "DRUG_SUPP")
rm(drug_info_p3)
gc()

# ---- REAC -------------------------------------------------------------------

message("Processing REAC...")

reac <- load_master("REAC")
if (!is.null(reac)) {
  reac <- reac[as.character(primaryid) %chin% demo_pids]

  pt_levels <- load_meddra_pt_levels(MEDDRA_PRIMARY_CSV)
  for (col in intersect(c("pt", "drug_rec_act"), names(reac))) {
    vals <- as.character(reac[[col]])
    lvls <- if (!is.null(pt_levels)) pt_levels else sort(unique(vals[!is.na(vals)]))
    reac[, (col) := factor(vals, levels = lvls, ordered = TRUE)]
  }

  save_legacy(unique(keep_cols(reac, c("primaryid", "pt", "drug_rec_act"))), "REAC")
  rm(reac)
  gc()
}

# ---- INDI -------------------------------------------------------------------

message("Processing INDI...")

indi <- load_master("INDI")
if (!is.null(indi)) {
  indi <- indi[as.character(primaryid) %chin% demo_pids]

  if ("indi_pt" %in% names(indi)) {
    pt_levels <- load_meddra_pt_levels(MEDDRA_PRIMARY_CSV)
    vals <- as.character(indi$indi_pt)
    lvls <- if (!is.null(pt_levels)) pt_levels else sort(unique(vals[!is.na(vals)]))
    indi[, indi_pt := factor(vals, levels = lvls, ordered = TRUE)]
  }

  save_legacy(unique(keep_cols(indi, c("primaryid", "drug_seq", "indi_pt"))), "INDI")
  rm(indi)
  gc()
}

# ---- OUTC -------------------------------------------------------------------

message("Processing OUTC...")

outc <- load_master("OUTC")
if (!is.null(outc)) {
  outc <- outc[as.character(primaryid) %chin% demo_pids]

  if ("outc_cod" %in% names(outc) && !is.ordered(outc$outc_cod)) {
    outc[, outc_cod := factor(as.character(outc_cod), OUTC_LEVELS, ordered = TRUE)]
  }

  save_legacy(unique(keep_cols(outc, c("primaryid", "outc_cod"))), "OUTC")
  rm(outc)
  gc()
}

# ---- THER -------------------------------------------------------------------

message("Processing THER...")

ther <- load_master("THER")
if (!is.null(ther)) {
  ther <- ther[as.character(primaryid) %chin% demo_pids]

  # event_dt was dropped from THER after time_to_onset computation; re-join from DEMO
  ther <- event_dt_map[ther, on = "primaryid"]

  THER_COLS <- c(
    "primaryid", "drug_seq",
    "start_dt", "dur_in_days", "end_dt", "time_to_onset", "event_dt"
  )
  save_legacy(unique(keep_cols(ther, THER_COLS)), "THER")
  rm(ther)
  gc()
}

rm(demo_pids, event_dt_map)

message("")
message("Done. Legacy tables written to: ", LEGACY_ROOT)
