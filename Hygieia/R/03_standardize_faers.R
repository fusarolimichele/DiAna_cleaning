# ------------------------------------------------------------------------------
# Script: 03_standardize_faers.R
# Purpose:
#   Standardize quarter-level staged FAERS parquet files and write quarter-level
#   cleaned parquet files.
#
# Inputs:
#   - data_stage/<TABLE>/<TABLE>_<YYQ#>.parquet
#   - external/MedDRA/meddra.csv
#   - external/manual_fix/pt_fixed.csv
#   - external/DiAna_dictionary/drugnames_standardized.csv
#   - external/manual_fix/countries.csv
#   - external/manual_fix/route_st.csv
#   - external/manual_fix/dose_form_st.csv
#   - external/manual_fix/dose_freq_st.csv
#   - external/manual_fix/route_form_st.csv
#
# Outputs:
#   - data_clean/<TABLE>/<TABLE>_<YYQ#>.parquet
#   - data_clean/reports/*.csv
#   - data_clean/faers_standardize_manifest.csv
#
# Notes:
#   - Standardization happens quarter-by-quarter for memory stability.
#   - DRUG stage is read once per quarter and split into cleaned DRUG/DRUG_INFO.
#   - This script does not perform final exclusions, deduplication, or release
#     packaging yet.
# ------------------------------------------------------------------------------

suppressPackageStartupMessages({
  library(data.table)
  library(arrow)
  library(lubridate)
})
options(timeout = 1200)

# ------------------------------- configuration ------------------------------- #

BASE_DIR <- "."
source(file.path(BASE_DIR, "R", "utils.R"))
STAGE_ROOT  <- file.path(BASE_DIR, "data_stage")
CLEAN_ROOT  <- file.path(BASE_DIR, "data_clean")
REPORT_ROOT <- file.path(CLEAN_ROOT, "reports")

MANIFEST_CSV <- file.path(CLEAN_ROOT, "faers_standardize_manifest.csv")
UNRESOLVED_SUMMARY_CSV <- file.path(REPORT_ROOT, "unresolved_summary.csv")

# If TRUE, stop the script at the end when unresolved items remain.
fail_on_unresolved <- FALSE
overwrite_clean <- TRUE
parquet_compression <- "zstd"

MEDDRA_PATH       <- file.path(BASE_DIR, "external","MedDRA","meddra.csv")
PT_FIX_PATH       <- file.path(BASE_DIR, "external","manual_fix","pt_fixed.csv")
DIANA_PATH        <- file.path(BASE_DIR, "external", "DiAna_dictionary", "drugnames_standardized.csv")
COUNTRIES_PATH    <- file.path(BASE_DIR, "external", "manual_fix", "countries.csv")
ROUTE_ST_PATH     <- file.path(BASE_DIR, "external","manual_fix", "route_st.csv")
DOSE_FORM_ST_PATH <- file.path(BASE_DIR, "external","manual_fix", "dose_form_st.csv")
DOSE_FREQ_ST_PATH <- file.path(BASE_DIR, "external","manual_fix", "dose_freq_st.csv")
ROUTE_FORM_PATH   <- file.path(BASE_DIR, "external","manual_fix", "route_form_st.csv")

# ------------------------------- helpers ------------------------------------- #
ensure_columns <- function(dt, cols, fill = NA_character_) {
  missing_cols <- setdiff(cols, names(dt))
  if (length(missing_cols)) {
    for (j in missing_cols) {
      dt[, (j) := fill]
    }
  }
  invisible(dt)
}
normalize_drugname <- function(x) {
  x <- normalize_term(x)
  x <- gsub("\\.$", "", x)
  x <- gsub("\\(\\s+", "(", x)
  x <- gsub("\\s+\\)", ")", x)
  x <- gsub("\\s+", " ", x)
  trimws(x)
}

read_delim_auto <- function(path) {
  if (!file.exists(path)) stop("Missing file: ", path)
  
  dt <- tryCatch(
    fread(path, sep = ";", encoding = "UTF-8", na.strings = "", header = TRUE),
    error = function(e) NULL
  )
  
  if (is.null(dt) || ncol(dt) == 1L) {
    dt <- fread(path, sep = ",", encoding = "UTF-8", na.strings = "", header = TRUE)
  }
  
  setDT(dt)
  setnames(dt, normalize_names(names(dt)))
  dt
}

list_parquet_files <- function(root_dir) {
  files <- list.files(
    path = root_dir,
    pattern = "\\.parquet$",
    recursive = TRUE,
    full.names = TRUE
  )
  
  if (!length(files)) {
    stop("No parquet files found under: ", root_dir)
  }
  
  dt <- data.table(path = files)
  dt[, file_name := basename(path)]
  dt[, table_name := toupper(sub("_.*$", "", file_name))]
  dt[, quarter := toupper(sub("^.*_([0-9]{2}Q[1-4])\\.parquet$", "\\1", file_name, perl = TRUE))]
  dt[]
}

get_quarters <- function(file_index, tbl_name) {
  sort(unique(file_index[table_name == toupper(tbl_name)]$quarter))
}

get_parquet_file <- function(file_index, tbl_name, qtr_id) {
  x <- file_index[table_name == toupper(tbl_name) & quarter == toupper(qtr_id)]$path
  if (!length(x)) return(NA_character_)
  x[1L]
}

read_parquet_table <- function(file_index, tbl_name, qtr_id) {
  path <- get_parquet_file(file_index, tbl_name, qtr_id)
  if (is.na(path) || !file.exists(path)) return(NULL)
  dt <- as.data.table(read_parquet(path))
  setDT(dt)
  dt
}

write_clean_table <- function(dt, tbl_name, qtr_id) {
  out_dir <- file.path(CLEAN_ROOT, toupper(tbl_name))
  dir_create_safe(out_dir)
  
  out_file <- file.path(out_dir, sprintf("%s_%s.parquet", toupper(tbl_name), toupper(qtr_id)))
  
  if (file.exists(out_file) && !overwrite_clean) {
    return(list(out_file = out_file, status = "skipped_existing", n_rows = NA_integer_))
  }
  
  write_parquet(dt, out_file, compression = parquet_compression)
  list(out_file = out_file, status = "written", n_rows = nrow(dt))
}

manifest_spec <- manifest_spec_standardize()

load_manifest <- function(path) {
  load_manifest_generic(path, manifest_spec)
}

save_manifest <- function(dt, path) {
  save_manifest_generic(dt, path)
}

upsert_manifest_row <- function(manifest, row_dt) {
  upsert_manifest_row_generic(manifest, row_dt, manifest_spec)
}

yyq_to_quarter_end <- function(yyq) {
  yy <- substr(yyq, 1, 2)
  q  <- substr(yyq, 3, 4)
  yyyy <- paste0("20", yy)
  mmdd <- switch(q, Q1 = "0331", Q2 = "0630", Q3 = "0930", Q4 = "1231", "1231")
  as.integer(paste0(yyyy, mmdd))
}

infer_global_max_date <- function(file_index) {
  qtrs <- unique(file_index$quarter)
  qtrs <- qtrs[grepl("^[0-9]{2}Q[1-4]$", qtrs)]
  if (!length(qtrs)) return(20501231L)
  yyq_to_quarter_end(sort(qtrs)[length(qtrs)])
}

validate_faers_date <- function(x, max_date) {
  x <- normalize_text(x)
  x[!grepl("^[0-9]{4}([0-9]{2}([0-9]{2})?)?$", x) & !is.na(x)] <- NA_character_
  
  n <- nchar(x)
  y4 <- as.integer(substr(x, 1, 4))
  
  bad <- rep(FALSE, length(x))
  bad <- bad | (n == 4 & (y4 < 1985L | y4 > as.integer(substr(as.character(max_date), 1, 4))))
  bad <- bad | (n == 6 & (as.integer(x) < 198500L | as.integer(x) > as.integer(substr(as.character(max_date), 1, 6))))
  bad <- bad | (n == 8 & (as.integer(x) < 19850000L | as.integer(x) > max_date))
  bad <- bad | (!n %in% c(4L, 6L, 8L) & !is.na(x))
  
  x[bad] <- NA_character_
  x
}

faers8_to_date <- function(x) {
  x <- normalize_text(x)
  out <- as.Date(rep(NA_character_, length(x)))
  idx <- !is.na(x) & nchar(x) == 8L
  if (any(idx)) {
    out[idx] <- suppressWarnings(ymd(x[idx]))
  }
  out
}

safe_rbind_reports <- function(lst) {
  lst <- Filter(function(x) !is.null(x) && nrow(x) > 0L, lst)
  if (!length(lst)) return(data.table())
  rbindlist(lst, fill = TRUE, use.names = TRUE)
}
summarize_unresolved_report <- function(dt, report_name, value_col = NULL) {
  if (is.null(dt) || nrow(dt) == 0L) {
    return(data.table(
      report_name = report_name,
      n_rows = 0L,
      n_distinct_values = 0L,
      total_count = 0L
    ))
  }
  
  total_count <- if ("N" %in% names(dt)) sum(dt$N, na.rm = TRUE) else as.integer(nrow(dt))
  
  n_distinct_values <- 0L
  if (!is.null(value_col) && value_col %in% names(dt)) {
    n_distinct_values <- uniqueN(dt[[value_col]][!is.na(dt[[value_col]])])
  }
  
  data.table(
    report_name = report_name,
    n_rows = as.integer(nrow(dt)),
    n_distinct_values = as.integer(n_distinct_values),
    total_count = as.integer(total_count)
  )
}

print_unresolved_summary <- function(summary_dt) {
  message("")
  message("Unresolved summary")
  message("------------------")
  
  for (i in seq_len(nrow(summary_dt))) {
    message(sprintf(
      "%-22s rows=%d | distinct=%d | total_count=%d",
      summary_dt$report_name[i],
      summary_dt$n_rows[i],
      summary_dt$n_distinct_values[i],
      summary_dt$total_count[i]
    ))
  }
}

# ------------------------------- dictionaries -------------------------------- #

build_pt_dictionary <- function(meddra_path, pt_fix_path) {
  meddra <- read_delim_auto(meddra_path)
  if (!("pt" %in% names(meddra)))  stop("meddra.csv must contain a 'pt' column.")
  if (!("llt" %in% names(meddra))) stop("meddra.csv must contain a 'llt' column.")
  
  for (j in names(meddra)) {
    if (is.character(meddra[[j]]) || is.factor(meddra[[j]])) {
      set(meddra, j = j, value = normalize_term(meddra[[j]]))
    }
  }
  
  pt_exact <- unique(meddra[!is.na(pt), .(raw_term = pt, standard_pt = pt)])
  llt_map  <- unique(meddra[!is.na(llt) & !is.na(pt), .(raw_term = llt, standard_pt = pt)])
  
  pt_fix <- read_delim_auto(pt_fix_path)
  if (!all(c("pt", "standard_pt") %in% names(pt_fix))) {
    stop("pt_fixed.csv must contain columns 'pt' and 'standard_pt'.")
  }
  
  pt_fix <- unique(pt_fix[, .(
    raw_term = normalize_term(pt),
    standard_pt = normalize_term(standard_pt)
  )][!is.na(raw_term)])
  
  dict <- rbindlist(list(pt_fix, pt_exact, llt_map), use.names = TRUE, fill = TRUE)
  dict <- dict[!is.na(raw_term)]
  dict <- dict[!duplicated(raw_term)]
  setkey(dict, raw_term)
  
  list(
    dict = dict,
    pt_levels = sort(unique(dict$standard_pt[!is.na(dict$standard_pt)]))
  )
}

build_drug_dictionary <- function(diana_path) {
  diana <- read_delim_auto(diana_path)
  
  if (!("drugname" %in% names(diana))) {
    stop("Drug dictionary must contain a 'drugname' column.")
  }
  if (!("substance" %in% names(diana))) {
    stop("Drug dictionary must contain a 'substance' column.")
  }
  
  dict <- unique(diana[, .(
    drugname = normalize_drugname(drugname),
    substance = normalize_term(substance)
  )])
  
  dict <- dict[!is.na(drugname)]
  setkey(dict, drugname)
  dict
}

load_country_map <- function(path) {
  dt <- read_delim_auto(path)
  
  if (!("country" %in% names(dt))) {
    stop("countries.csv must contain column 'country'.")
  }
  
  target_col <- if ("country_name" %in% names(dt)) {
    "country_name"
  } else if ("countryname" %in% names(dt)) {
    "countryname"
  } else {
    stop("countries.csv must contain 'country_name' or 'countryname'.")
  }
  
  out <- unique(dt[, .(
    country = normalize_term(country),
    country_std = normalize_term(get(target_col))
  )])
  
  setkey(out, country)
  out
}

load_simple_map <- function(path, source_col, target_col) {
  dt <- read_delim_auto(path)
  if (!all(c(source_col, target_col) %in% names(dt))) {
    stop("Missing expected columns in ", path, ": ", source_col, ", ", target_col)
  }
  
  out <- unique(dt[, .(
    source = normalize_term(get(source_col)),
    target = normalize_term(get(target_col))
  )])
  
  setkey(out, source)
  out
}

load_route_form_map <- function(path) {
  dt <- read_delim_auto(path)
  if (!all(c("dose_form_st", "route_plus") %in% names(dt))) {
    stop("route_form_st.csv must contain 'dose_form_st' and 'route_plus'.")
  }
  
  out <- unique(dt[, .(
    dose_form_st = normalize_term(dose_form_st),
    route_plus = normalize_term(route_plus)
  )])
  
  setkey(out, dose_form_st)
  out
}

# ------------------------------- standardizers ------------------------------- #

standardize_pt_column <- function(dt, col_name, pt_dict, qtr_id, tbl_name) {
  raw <- normalize_term(dt[[col_name]])
  idx <- match(raw, pt_dict$raw_term)
  mapped <- pt_dict$standard_pt[idx]
  
  unresolved <- data.table(
    quarter = qtr_id,
    table_name = tbl_name,
    variable = col_name,
    raw_term = raw,
    mapped = mapped
  )[!is.na(raw_term) & is.na(mapped),
    .N, by = .(quarter, table_name, variable, raw_term)][order(-N)]
  
  set(dt, j = col_name, value = fifelse(!is.na(mapped), mapped, raw))
  list(data = dt, unresolved = unresolved)
}

standardize_demo <- function(dt, country_map, max_date, qtr_id) {
  setDT(dt)
  dt <- unique(dt)
  
  for (j in names(dt)) {
    set(dt, j = j, value = normalize_text(dt[[j]]))
  }
  
  if ("sex" %in% names(dt)) {
    dt[, sex := toupper(sex)]
    dt[!sex %chin% c("F", "M"), sex := NA_character_]
  }
  
  age_factor_days <- c(
    DEC = 3650,
    YR  = 365,
    MON = 30.4375,
    WK  = 7,
    DY  = 1,
    HR  = 1/24,
    MIN = 1/(24 * 60),
    SEC = 1/(24 * 60 * 60)
  )
  
  dt[, age_num := suppressWarnings(as.numeric(age))]
  dt[, age_unit := toupper(fifelse(is.na(age_cod), "YR", age_cod))]
  dt[, age_in_days := round(abs(age_num) * unname(age_factor_days[age_unit]))]
  dt[is.na(age_unit) | !(age_unit %chin% names(age_factor_days)), age_in_days := NA_real_]
  dt[age_in_days > 122 * 365, age_in_days := NA_real_]
  dt[, age_in_years := round(age_in_days / 365)]
  
  dt[, age_grp_std := NA_character_]
  dt[!is.na(age_in_years), age_grp_std := "E"]
  dt[!is.na(age_in_years) & age_in_years < 65, age_grp_std := "A"]
  dt[!is.na(age_in_years) & age_in_years < 18, age_grp_std := "T"]
  dt[!is.na(age_in_years) & age_in_years < 12, age_grp_std := "C"]
  dt[!is.na(age_in_years) & age_in_years < 2,  age_grp_std := "I"]
  dt[!is.na(age_in_days)  & age_in_days  < 28, age_grp_std := "N"]
  
  dt[, wt_num := suppressWarnings(as.numeric(wt))]
  dt[, wt_unit := toupper(wt_cod)]
  dt[, wt_corrector := NA_real_]
  dt[wt_unit %chin% c("LBS", "IB"), wt_corrector := 0.453592]
  dt[wt_unit %chin% c("KG", "KGS"), wt_corrector := 1]
  dt[wt_unit == "GMS", wt_corrector := 0.001]
  dt[wt_unit == "MG", wt_corrector := 1e-06]
  dt[is.na(wt_unit) & !is.na(wt_num), wt_corrector := 1]
  dt[, wt_in_kgs := round(abs(wt_num) * wt_corrector)]
  dt[wt_in_kgs > 635, wt_in_kgs := NA_real_]
  
  country_reports <- list()
  
  for (cc in c("occr_country", "reporter_country")) {
    if (cc %in% names(dt)) {
      raw_vals <- normalize_term(dt[[cc]])
      idx <- match(raw_vals, country_map$country)
      mapped_vals <- country_map$country_std[idx]
      
      rep_dt <- data.table(
        quarter = qtr_id,
        table_name = "DEMO",
        variable = cc,
        raw_value = raw_vals,
        mapped = mapped_vals
      )[!is.na(raw_value) & is.na(mapped),
        .N, by = .(quarter, table_name, variable, raw_value)][order(-N)]
      
      country_reports[[length(country_reports) + 1L]] <- rep_dt
      set(dt, j = cc, value = fifelse(!is.na(mapped_vals), mapped_vals, raw_vals))
    }
  }
  
  if ("occp_cod" %in% names(dt)) {
    dt[, occp_cod := toupper(occp_cod)]
    dt[!occp_cod %chin% c("MD","CN","OT","PH","HP","LW","RN"), occp_cod := NA_character_]
  }
  
  if ("rept_cod" %in% names(dt)) {
    dt[, rept_cod := toupper(rept_cod)]
    dt[rept_cod %chin% c("30DAY", "5DAY"), rept_cod := "EXP"]
  }
  
  for (col_name in intersect(c("fda_dt", "rept_dt", "mfr_dt", "init_fda_dt", "event_dt"), names(dt))) {
    set(dt, j = col_name, value = validate_faers_date(dt[[col_name]], max_date))
  }
  
  drop_cols <- intersect(
    c("age", "age_cod", "age_grp", "age_num", "age_unit",
      "wt", "wt_cod", "wt_num", "wt_unit", "wt_corrector"),
    names(dt)
  )
  if (length(drop_cols)) dt[, (drop_cols) := NULL]
  
  if ("age_grp_std" %in% names(dt)) setnames(dt, "age_grp_std", "age_grp")
  
  list(
    data = unique(dt),
    country_unresolved = safe_rbind_reports(country_reports)
  )
}

standardize_reac <- function(dt, pt_dict, qtr_id) {
  setDT(dt)
  dt <- unique(dt)
  
  out1 <- standardize_pt_column(dt, "pt", pt_dict, qtr_id, "REAC")
  dt <- out1$data
  
  pt_reports <- list(out1$unresolved)
  
  if ("drug_rec_act" %in% names(dt)) {
    out2 <- standardize_pt_column(dt, "drug_rec_act", pt_dict, qtr_id, "REAC")
    dt <- out2$data
    pt_reports[[length(pt_reports) + 1L]] <- out2$unresolved
  }
  
  dt <- dt[!is.na(pt)]
  list(data = unique(dt), pt_unresolved = safe_rbind_reports(pt_reports))
}

standardize_indi <- function(dt, pt_dict, qtr_id) {
  setDT(dt)
  dt <- unique(dt)
  
  out <- standardize_pt_column(dt, "indi_pt", pt_dict, qtr_id, "INDI")
  dt <- out$data[!is.na(indi_pt)]
  
  list(data = unique(dt), pt_unresolved = out$unresolved)
}

standardize_outc <- function(dt) {
  setDT(dt)
  dt <- unique(dt)
  
  if ("outc_cod" %in% names(dt)) {
    dt[, outc_cod := toupper(normalize_text(outc_cod))]
  }
  
  unique(dt[!is.na(outc_cod)])
}

standardize_rpsr <- function(dt) {
  setDT(dt)
  dt <- unique(dt)
  
  if ("rpsr_cod" %in% names(dt)) {
    dt[, rpsr_cod := toupper(normalize_text(rpsr_cod))]
  }
  
  unique(dt)
}

standardize_deleted <- function(dt) {
  setDT(dt)
  dt <- unique(dt)
  if ("caseid" %in% names(dt)) dt[, caseid := normalize_text(caseid)]
  unique(dt[!is.na(caseid)])
}

standardize_drug_and_info <- function(dt, drug_dict, route_map, dose_form_map, dose_freq_map,
                                      route_form_map, max_exp_date, qtr_id) {
  setDT(dt)
  dt <- unique(dt)
  
  expected_cols <- c(
    "primaryid","drug_seq","role_cod","drugname","prod_ai","val_vbm","route",
    "dose_vbm","dechal","rechal","lot_num","nda_num","exp_dt","dose_form",
    "dose_freq","cum_dose_unit","cum_dose_chr","dose_amt","dose_unit","quarter"
  )
  missing_cols <- setdiff(expected_cols, names(dt))
  if (length(missing_cols)) {
    message(sprintf(
      "DRUG %s missing columns: %s",
      qtr_id,
      paste(missing_cols, collapse = ", ")
    ))
    for (j in missing_cols) dt[, (j) := NA_character_]
  }
  ensure_columns(dt, expected_cols, fill = NA_character_)
  
  dt[, quarter := fifelse(is.na(quarter) | quarter == "", qtr_id, quarter)]
  
  for (j in intersect(
    c("primaryid","drug_seq","role_cod","drugname","prod_ai","val_vbm","route",
      "dose_vbm","dechal","rechal","lot_num","nda_num","exp_dt","dose_form",
      "dose_freq","cum_dose_unit","cum_dose_chr","dose_amt","dose_unit","quarter"),
    names(dt)
  )) {
    set(dt, j = j, value = normalize_text(dt[[j]]))
  }
  
  dt[, drugname := normalize_drugname(drugname)]
  idx_drug <- match(dt$drugname, drug_dict$drugname)
  dt[, substance_raw := drug_dict$substance[idx_drug]]
  
  drug_unresolved <- data.table(
    quarter = qtr_id,
    table_name = "DRUG",
    raw_drugname = dt$drugname,
    substance_raw = dt$substance_raw
  )[
    !is.na(raw_drugname) & is.na(substance_raw),
    .N, by = .(quarter, table_name, raw_drugname)
  ][order(-N)]
  
  dt[, trial := grepl(", trial$", substance_raw)]
  dt[, substance_raw := sub(", trial$", "", substance_raw)]
  dt[, substance_raw := normalize_term(substance_raw)]
  
  if ("role_cod" %in% names(dt)) {
    dt[, role_cod := toupper(role_cod)]
    dt[!role_cod %chin% c("PS", "SS", "C", "I"), role_cod := NA_character_]
  }
  
  route_unresolved <- data.table()
  if ("route" %in% names(dt)) {
    raw_route <- normalize_term(dt$route)
    idx_route <- match(raw_route, route_map$source)
    dt[, route_st := route_map$target[idx_route]]
    
    route_unresolved <- data.table(
      quarter = qtr_id,
      table_name = "DRUG_INFO",
      variable = "route",
      raw_value = raw_route,
      mapped = dt$route_st
    )[
      !is.na(raw_value) & is.na(mapped),
      .N, by = .(quarter, table_name, variable, raw_value)
    ][order(-N)]
  } else {
    dt[, route_st := NA_character_]
  }
  
  dose_form_unresolved <- data.table()
  if ("dose_form" %in% names(dt)) {
    raw_df <- normalize_term(dt$dose_form)
    idx_df <- match(raw_df, dose_form_map$source)
    dt[, dose_form_st := dose_form_map$target[idx_df]]
    
    dose_form_unresolved <- data.table(
      quarter = qtr_id,
      table_name = "DRUG_INFO",
      variable = "dose_form",
      raw_value = raw_df,
      mapped = dt$dose_form_st
    )[
      !is.na(raw_value) & is.na(mapped),
      .N, by = .(quarter, table_name, variable, raw_value)
    ][order(-N)]
  } else {
    dt[, dose_form_st := NA_character_]
  }
  
  dose_freq_unresolved <- data.table()
  if ("dose_freq" %in% names(dt)) {
    raw_dq <- normalize_term(dt$dose_freq)
    idx_dq <- match(raw_dq, dose_freq_map$source)
    dt[, dose_freq_st := dose_freq_map$target[idx_dq]]
    
    dose_freq_unresolved <- data.table(
      quarter = qtr_id,
      table_name = "DRUG_INFO",
      variable = "dose_freq",
      raw_value = raw_dq,
      mapped = dt$dose_freq_st
    )[
      !is.na(raw_value) & is.na(mapped),
      .N, by = .(quarter, table_name, variable, raw_value)
    ][order(-N)]
  } else {
    dt[, dose_freq_st := NA_character_]
  }
  
  if (nrow(route_form_map) > 0L) {
    idx_rf <- match(dt$dose_form_st, route_form_map$dose_form_st)
    dt[, route_plus := route_form_map$route_plus[idx_rf]]
  } else {
    dt[, route_plus := NA_character_]
  }
  
  dt[, route_clean := fifelse(is.na(route_st) | route_st == "unknown", route_plus, route_st)]
  
  for (cc in intersect(c("dechal", "rechal"), names(dt))) {
    dt[, (cc) := toupper(get(cc))]
    dt[!get(cc) %chin% c("Y", "N", "D"), (cc) := NA_character_]
  }
  
  if ("exp_dt" %in% names(dt)) {
    dt[, exp_dt := validate_faers_date(exp_dt, max_exp_date)]
  }
  
  drug_info <- dt[, .(
    primaryid, drug_seq, val_vbm, route = route_clean,
    dose_vbm, cum_dose_unit, cum_dose_chr, dose_amt, dose_unit,
    dose_form = dose_form_st, dose_freq = dose_freq_st,
    dechal, rechal, lot_num, nda_num, exp_dt, quarter
  )]
  drug_info <- unique(drug_info)
  
  drug_core <- dt[, .(
    primaryid, drug_seq, role_cod, drugname, prod_ai,
    substance_raw, trial, quarter
  )]
  
  multi <- drug_core[!is.na(substance_raw) & grepl(";", substance_raw, fixed = TRUE)]
  one   <- drug_core[is.na(substance_raw) | !grepl(";", substance_raw, fixed = TRUE)]
  
  if (nrow(multi) > 0L) {
    multi <- multi[, .(
      substance = trimws(unlist(strsplit(substance_raw, ";", fixed = TRUE)))
    ), by = .(primaryid, drug_seq, role_cod, drugname, prod_ai, trial, quarter)]
  } else {
    multi <- data.table(
      primaryid = character(), drug_seq = character(), role_cod = character(),
      drugname = character(), prod_ai = character(), trial = logical(),
      quarter = character(), substance = character()
    )
  }
  
  if (nrow(one) > 0L) {
    one <- one[, .(
      primaryid, drug_seq, role_cod, drugname, prod_ai, trial, quarter,
      substance = substance_raw
    )]
  } else {
    one <- data.table(
      primaryid = character(), drug_seq = character(), role_cod = character(),
      drugname = character(), prod_ai = character(), trial = logical(),
      quarter = character(), substance = character()
    )
  }
  
  drug_clean <- rbindlist(list(multi, one), fill = TRUE, use.names = TRUE)
  drug_clean[, substance := normalize_term(substance)]
  drug_clean <- unique(drug_clean)
  
  list(
    drug = drug_clean,
    drug_info = drug_info,
    drug_unresolved = drug_unresolved,
    route_unresolved = route_unresolved,
    dose_form_unresolved = dose_form_unresolved,
    dose_freq_unresolved = dose_freq_unresolved
  )
}

standardize_ther <- function(dt, demo_dt, max_date) {
  setDT(dt)
  dt <- unique(dt)
  
  for (j in intersect(c("primaryid","drug_seq","start_dt","end_dt","dur","dur_cod","quarter"), names(dt))) {
    set(dt, j = j, value = normalize_text(dt[[j]]))
  }
  
  dt[, start_dt := validate_faers_date(start_dt, max_date)]
  dt[, end_dt   := validate_faers_date(end_dt,   max_date)]
  
  dt[, dur_num := suppressWarnings(as.numeric(dur))]
  dt[, dur_code := toupper(dur_cod)]
  
  dur_factor <- c(
    YR = 365,
    MON = 30.41667,
    WK = 7,
    DAY = 1,
    HR = 1/24,
    MIN = 1/(24 * 60),
    SEC = 1/(24 * 60 * 60)
  )
  
  dt[, dur_in_days_from_unit := abs(dur_num) * unname(dur_factor[dur_code])]
  dt[!(dur_code %chin% names(dur_factor)), dur_in_days_from_unit := NA_real_]
  dt[dur_in_days_from_unit > 50 * 365, dur_in_days_from_unit := NA_real_]
  
  start_d <- faers8_to_date(dt$start_dt)
  end_d   <- faers8_to_date(dt$end_dt)
  
  dur_from_dates <- as.numeric(end_d - start_d) + 1
  dur_from_dates[dur_from_dates < 0] <- NA_real_
  
  dt[, dur_in_days := fifelse(!is.na(dur_from_dates), dur_from_dates, dur_in_days_from_unit)]
  
  fill_start_idx <- is.na(dt$start_dt) & !is.na(dt$end_dt) & nchar(dt$end_dt) == 8L & !is.na(dt$dur_in_days)
  if (any(fill_start_idx)) {
    dur_fill <- as.integer(round(dt$dur_in_days[fill_start_idx]))
    new_start <- as.character(format(end_d[fill_start_idx] - (dur_fill - 1L), "%Y%m%d"))
    dt[fill_start_idx, start_dt := new_start]
  }
  
  fill_end_idx <- is.na(dt$end_dt) & !is.na(dt$start_dt) & nchar(dt$start_dt) == 8L & !is.na(dt$dur_in_days)
  if (any(fill_end_idx)) {
    dur_fill <- as.integer(round(dt$dur_in_days[fill_end_idx]))
    start_d2 <- faers8_to_date(dt$start_dt[fill_end_idx])
    new_end <- as.character(format(start_d2 + (dur_fill - 1L), "%Y%m%d"))
    dt[fill_end_idx, end_dt := new_end]
  }
  
  event_map <- unique(demo_dt[, .(primaryid, event_dt)])
  setkey(event_map, primaryid)
  setkey(dt, primaryid)
  dt <- event_map[dt]
  
  event_d <- faers8_to_date(dt$event_dt)
  start_d3 <- faers8_to_date(dt$start_dt)
  
  dt[, time_to_onset := as.numeric(event_d - start_d3) + 1]
  dt[is.na(time_to_onset), time_to_onset := NA_real_]
  dt[time_to_onset <= 0 & !is.na(event_dt) & as.integer(event_dt) <= 20121231L, time_to_onset := NA_real_]
  
  drop_cols <- intersect(c("dur", "dur_cod", "dur_num", "dur_code", "dur_in_days_from_unit"), names(dt))
  if (length(drop_cols)) dt[, (drop_cols) := NULL]
  
  unique(dt)
}

# ------------------------------- main ---------------------------------------- #

dir_create_safe(CLEAN_ROOT)
dir_create_safe(REPORT_ROOT)

stage_index <- list_parquet_files(STAGE_ROOT)
manifest <- load_manifest(MANIFEST_CSV)

max_faers_date <- infer_global_max_date(stage_index)
max_exp_date   <- 20500101L

pt_obj <- build_pt_dictionary(MEDDRA_PATH, PT_FIX_PATH)
pt_dict <- pt_obj$dict

drug_dict      <- build_drug_dictionary(DIANA_PATH)
country_map    <- load_country_map(COUNTRIES_PATH)
route_map      <- load_simple_map(ROUTE_ST_PATH, "route", "route_st")
dose_form_map  <- load_simple_map(DOSE_FORM_ST_PATH, "dose_form", "dose_form_st")
dose_freq_map  <- load_simple_map(DOSE_FREQ_ST_PATH, "dose_freq", "dose_freq_st")
route_form_map <- load_route_form_map(ROUTE_FORM_PATH)

reports_pt         <- list()
reports_drug       <- list()
reports_route      <- list()
reports_dose_form  <- list()
reports_dose_freq  <- list()
reports_country    <- list()

# ------------------------------- DEMO ---------------------------------------- #

demo_quarters <- get_quarters(stage_index, "DEMO")

for (qtr in demo_quarters) {
  dt <- read_parquet_table(stage_index, "DEMO", qtr)
  if (is.null(dt)) next
  
  res <- tryCatch(
    standardize_demo(dt, country_map, max_faers_date, qtr),
    error = function(e) e
  )
  
  if (inherits(res, "error")) {
    manifest <- upsert_manifest_row(manifest, data.table(
      output_table = "DEMO",
      quarter = qtr,
      out_file = file.path(CLEAN_ROOT, "DEMO", sprintf("DEMO_%s.parquet", qtr)),
      n_rows = NA_integer_,
      status = "error",
      timestamp = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
      notes = conditionMessage(res)
    ))
    save_manifest(manifest, MANIFEST_CSV)
    next
  }
  
  wr <- write_clean_table(res$data, "DEMO", qtr)
  reports_country[[length(reports_country) + 1L]] <- res$country_unresolved
  
  manifest <- upsert_manifest_row(manifest, data.table(
    output_table = "DEMO",
    quarter = qtr,
    out_file = wr$out_file,
    n_rows = wr$n_rows,
    status = wr$status,
    timestamp = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
    notes = NA_character_
  ))
  save_manifest(manifest, MANIFEST_CSV)
  
  rm(dt, res, wr)
  gc()
}

# ------------------------------- REAC ---------------------------------------- #

reac_quarters <- get_quarters(stage_index, "REAC")

for (qtr in reac_quarters) {
  dt <- read_parquet_table(stage_index, "REAC", qtr)
  if (is.null(dt)) next
  
  res <- tryCatch(
    standardize_reac(dt, pt_dict, qtr),
    error = function(e) e
  )
  
  if (inherits(res, "error")) {
    manifest <- upsert_manifest_row(manifest, data.table(
      output_table = "REAC",
      quarter = qtr,
      out_file = file.path(CLEAN_ROOT, "REAC", sprintf("REAC_%s.parquet", qtr)),
      n_rows = NA_integer_,
      status = "error",
      timestamp = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
      notes = conditionMessage(res)
    ))
    save_manifest(manifest, MANIFEST_CSV)
    next
  }
  
  wr <- write_clean_table(res$data, "REAC", qtr)
  reports_pt[[length(reports_pt) + 1L]] <- res$pt_unresolved
  
  manifest <- upsert_manifest_row(manifest, data.table(
    output_table = "REAC",
    quarter = qtr,
    out_file = wr$out_file,
    n_rows = wr$n_rows,
    status = wr$status,
    timestamp = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
    notes = NA_character_
  ))
  save_manifest(manifest, MANIFEST_CSV)
  
  rm(dt, res, wr)
  gc()
}

# ------------------------------- INDI ---------------------------------------- #

indi_quarters <- get_quarters(stage_index, "INDI")

for (qtr in indi_quarters) {
  dt <- read_parquet_table(stage_index, "INDI", qtr)
  if (is.null(dt)) next
  
  res <- tryCatch(
    standardize_indi(dt, pt_dict, qtr),
    error = function(e) e
  )
  
  if (inherits(res, "error")) {
    manifest <- upsert_manifest_row(manifest, data.table(
      output_table = "INDI",
      quarter = qtr,
      out_file = file.path(CLEAN_ROOT, "INDI", sprintf("INDI_%s.parquet", qtr)),
      n_rows = NA_integer_,
      status = "error",
      timestamp = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
      notes = conditionMessage(res)
    ))
    save_manifest(manifest, MANIFEST_CSV)
    next
  }
  
  wr <- write_clean_table(res$data, "INDI", qtr)
  reports_pt[[length(reports_pt) + 1L]] <- res$pt_unresolved
  
  manifest <- upsert_manifest_row(manifest, data.table(
    output_table = "INDI",
    quarter = qtr,
    out_file = wr$out_file,
    n_rows = wr$n_rows,
    status = wr$status,
    timestamp = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
    notes = NA_character_
  ))
  save_manifest(manifest, MANIFEST_CSV)
  
  rm(dt, res, wr)
  gc()
}

# ------------------------------- OUTC ---------------------------------------- #

outc_quarters <- get_quarters(stage_index, "OUTC")

for (qtr in outc_quarters) {
  dt <- read_parquet_table(stage_index, "OUTC", qtr)
  if (is.null(dt)) next
  
  res <- tryCatch(
    standardize_outc(dt),
    error = function(e) e
  )
  
  if (inherits(res, "error")) {
    manifest <- upsert_manifest_row(manifest, data.table(
      output_table = "OUTC",
      quarter = qtr,
      out_file = file.path(CLEAN_ROOT, "OUTC", sprintf("OUTC_%s.parquet", qtr)),
      n_rows = NA_integer_,
      status = "error",
      timestamp = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
      notes = conditionMessage(res)
    ))
    save_manifest(manifest, MANIFEST_CSV)
    next
  }
  
  wr <- write_clean_table(res, "OUTC", qtr)
  
  manifest <- upsert_manifest_row(manifest, data.table(
    output_table = "OUTC",
    quarter = qtr,
    out_file = wr$out_file,
    n_rows = wr$n_rows,
    status = wr$status,
    timestamp = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
    notes = NA_character_
  ))
  save_manifest(manifest, MANIFEST_CSV)
  
  rm(dt, res, wr)
  gc()
}

# ------------------------------- RPSR ---------------------------------------- #

rpsr_quarters <- get_quarters(stage_index, "RPSR")

for (qtr in rpsr_quarters) {
  dt <- read_parquet_table(stage_index, "RPSR", qtr)
  if (is.null(dt)) next
  
  res <- tryCatch(
    standardize_rpsr(dt),
    error = function(e) e
  )
  
  if (inherits(res, "error")) {
    manifest <- upsert_manifest_row(manifest, data.table(
      output_table = "RPSR",
      quarter = qtr,
      out_file = file.path(CLEAN_ROOT, "RPSR", sprintf("RPSR_%s.parquet", qtr)),
      n_rows = NA_integer_,
      status = "error",
      timestamp = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
      notes = conditionMessage(res)
    ))
    save_manifest(manifest, MANIFEST_CSV)
    next
  }
  
  wr <- write_clean_table(res, "RPSR", qtr)
  
  manifest <- upsert_manifest_row(manifest, data.table(
    output_table = "RPSR",
    quarter = qtr,
    out_file = wr$out_file,
    n_rows = wr$n_rows,
    status = wr$status,
    timestamp = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
    notes = NA_character_
  ))
  save_manifest(manifest, MANIFEST_CSV)
  
  rm(dt, res, wr)
  gc()
}

# ------------------------------- DELETED ------------------------------------- #

deleted_quarters <- get_quarters(stage_index, "DELETED")

for (qtr in deleted_quarters) {
  dt <- read_parquet_table(stage_index, "DELETED", qtr)
  if (is.null(dt)) next
  
  res <- tryCatch(
    standardize_deleted(dt),
    error = function(e) e
  )
  
  if (inherits(res, "error")) {
    manifest <- upsert_manifest_row(manifest, data.table(
      output_table = "DELETED",
      quarter = qtr,
      out_file = file.path(CLEAN_ROOT, "DELETED", sprintf("DELETED_%s.parquet", qtr)),
      n_rows = NA_integer_,
      status = "error",
      timestamp = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
      notes = conditionMessage(res)
    ))
    save_manifest(manifest, MANIFEST_CSV)
    next
  }
  
  wr <- write_clean_table(res, "DELETED", qtr)
  
  manifest <- upsert_manifest_row(manifest, data.table(
    output_table = "DELETED",
    quarter = qtr,
    out_file = wr$out_file,
    n_rows = wr$n_rows,
    status = wr$status,
    timestamp = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
    notes = NA_character_
  ))
  save_manifest(manifest, MANIFEST_CSV)
  
  rm(dt, res, wr)
  gc()
}

# ------------------------------- DRUG / DRUG_INFO ---------------------------- #

drug_quarters <- get_quarters(stage_index, "DRUG")

for (qtr in drug_quarters) {
  dt <- read_parquet_table(stage_index, "DRUG", qtr)
  if (is.null(dt)) next
  
  res <- tryCatch(
    standardize_drug_and_info(
      dt = dt,
      drug_dict = drug_dict,
      route_map = route_map,
      dose_form_map = dose_form_map,
      dose_freq_map = dose_freq_map,
      route_form_map = route_form_map,
      max_exp_date = max_exp_date,
      qtr_id = qtr
    ),
    error = function(e) e
  )
  
  if (inherits(res, "error")) {
    for (tbl in c("DRUG", "DRUG_INFO")) {
      manifest <- upsert_manifest_row(manifest, data.table(
        output_table = tbl,
        quarter = qtr,
        out_file = file.path(CLEAN_ROOT, tbl, sprintf("%s_%s.parquet", tbl, qtr)),
        n_rows = NA_integer_,
        status = "error",
        timestamp = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
        notes = conditionMessage(res)
      ))
    }
    save_manifest(manifest, MANIFEST_CSV)
    next
  }
  
  wr1 <- write_clean_table(res$drug, "DRUG", qtr)
  wr2 <- write_clean_table(res$drug_info, "DRUG_INFO", qtr)
  
  reports_drug[[length(reports_drug) + 1L]]          <- res$drug_unresolved
  reports_route[[length(reports_route) + 1L]]        <- res$route_unresolved
  reports_dose_form[[length(reports_dose_form) + 1L]] <- res$dose_form_unresolved
  reports_dose_freq[[length(reports_dose_freq) + 1L]] <- res$dose_freq_unresolved
  
  manifest <- upsert_manifest_row(manifest, data.table(
    output_table = "DRUG",
    quarter = qtr,
    out_file = wr1$out_file,
    n_rows = wr1$n_rows,
    status = wr1$status,
    timestamp = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
    notes = NA_character_
  ))
  
  manifest <- upsert_manifest_row(manifest, data.table(
    output_table = "DRUG_INFO",
    quarter = qtr,
    out_file = wr2$out_file,
    n_rows = wr2$n_rows,
    status = wr2$status,
    timestamp = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
    notes = NA_character_
  ))
  
  save_manifest(manifest, MANIFEST_CSV)
  
  rm(dt, res, wr1, wr2)
  gc()
}

# ------------------------------- THER ---------------------------------------- #

ther_quarters <- get_quarters(stage_index, "THER")

for (qtr in ther_quarters) {
  dt <- read_parquet_table(stage_index, "THER", qtr)
  
  demo_path <- file.path(CLEAN_ROOT, "DEMO", sprintf("DEMO_%s.parquet", qtr))
  demo_dt <- if (file.exists(demo_path)) as.data.table(read_parquet(demo_path)) else NULL
  
  if (is.null(dt) || is.null(demo_dt)) next
  
  res <- tryCatch(
    standardize_ther(dt, demo_dt, max_faers_date),
    error = function(e) e
  )
  
  if (inherits(res, "error")) {
    manifest <- upsert_manifest_row(manifest, data.table(
      output_table = "THER",
      quarter = qtr,
      out_file = file.path(CLEAN_ROOT, "THER", sprintf("THER_%s.parquet", qtr)),
      n_rows = NA_integer_,
      status = "error",
      timestamp = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
      notes = conditionMessage(res)
    ))
    save_manifest(manifest, MANIFEST_CSV)
    next
  }
  
  wr <- write_clean_table(res, "THER", qtr)
  
  manifest <- upsert_manifest_row(manifest, data.table(
    output_table = "THER",
    quarter = qtr,
    out_file = wr$out_file,
    n_rows = wr$n_rows,
    status = wr$status,
    timestamp = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
    notes = NA_character_
  ))
  save_manifest(manifest, MANIFEST_CSV)
  
  rm(dt, demo_dt, res, wr)
  gc()
}

# ------------------------------- reports ------------------------------------- #

write_report <- function(dt, path) {
  if (is.null(dt) || nrow(dt) == 0L) {
    fwrite(data.table(), path)
  } else {
    fwrite(dt[order(-N)], path)
  }
}

write_report(safe_rbind_reports(reports_pt),         file.path(REPORT_ROOT, "pt_unresolved.csv"))
write_report(safe_rbind_reports(reports_drug),       file.path(REPORT_ROOT, "drug_unresolved.csv"))
write_report(safe_rbind_reports(reports_route),      file.path(REPORT_ROOT, "route_unresolved.csv"))
write_report(safe_rbind_reports(reports_dose_form),  file.path(REPORT_ROOT, "dose_form_unresolved.csv"))
write_report(safe_rbind_reports(reports_dose_freq),  file.path(REPORT_ROOT, "dose_freq_unresolved.csv"))
write_report(safe_rbind_reports(reports_country),    file.path(REPORT_ROOT, "country_unresolved.csv"))

message("")
message("Done.")
message("Clean root:   ", CLEAN_ROOT)
message("Reports root: ", REPORT_ROOT)
message("Manifest:     ", MANIFEST_CSV)

# ------------------------------- unresolved summary -------------------------- #

pt_unresolved_dt        <- safe_rbind_reports(reports_pt)
drug_unresolved_dt      <- safe_rbind_reports(reports_drug)
route_unresolved_dt     <- safe_rbind_reports(reports_route)
dose_form_unresolved_dt <- safe_rbind_reports(reports_dose_form)
dose_freq_unresolved_dt <- safe_rbind_reports(reports_dose_freq)
country_unresolved_dt   <- safe_rbind_reports(reports_country)

unresolved_summary <- rbindlist(list(
  summarize_unresolved_report(pt_unresolved_dt,        "pt_unresolved",        "raw_term"),
  summarize_unresolved_report(drug_unresolved_dt,      "drug_unresolved",      "raw_drugname"),
  summarize_unresolved_report(route_unresolved_dt,     "route_unresolved",     "raw_value"),
  summarize_unresolved_report(dose_form_unresolved_dt, "dose_form_unresolved", "raw_value"),
  summarize_unresolved_report(dose_freq_unresolved_dt, "dose_freq_unresolved", "raw_value"),
  summarize_unresolved_report(country_unresolved_dt,   "country_unresolved",   "raw_value")
), fill = TRUE, use.names = TRUE)

fwrite(unresolved_summary, UNRESOLVED_SUMMARY_CSV)
print_unresolved_summary(unresolved_summary)

total_unresolved_rows <- sum(unresolved_summary$n_rows, na.rm = TRUE)
total_unresolved_count <- sum(unresolved_summary$total_count, na.rm = TRUE)

message("")
message(sprintf("Unresolved summary CSV: %s", UNRESOLVED_SUMMARY_CSV))
message(sprintf("Total unresolved report rows: %d", total_unresolved_rows))
message(sprintf("Total unresolved item count: %d", total_unresolved_count))

if (fail_on_unresolved && total_unresolved_rows > 0L) {
  stop(
    sprintf(
      "Unresolved items remain (%d report rows; %d total counts). Review %s before treating the run as clean.",
      total_unresolved_rows, total_unresolved_count, UNRESOLVED_SUMMARY_CSV
    ),
    call. = FALSE
  )
}
