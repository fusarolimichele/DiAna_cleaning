suppressPackageStartupMessages({
  library(data.table)
})

# ------------------------------- shared utilities ----------------------------- #

dir_create_safe <- function(path) {
  dir.create(path, recursive = TRUE, showWarnings = FALSE)
  invisible(path)
}

normalize_names <- function(x) {
  x <- trimws(x)
  tolower(x)
}

normalize_text <- function(x) {
  x <- as.character(x)
  x <- trimws(x)
  x[x == ""] <- NA_character_
  x
}

normalize_term <- function(x) {
  x <- normalize_text(x)
  x <- tolower(x)
  x <- gsub("\\s+", " ", x)
  trimws(x)
}

assert_has_cols <- function(dt, cols, context = "object") {
  missing_cols <- setdiff(cols, names(dt))
  if (length(missing_cols)) {
    stop(
      sprintf(
        "%s is missing required columns: %s",
        context,
        paste(missing_cols, collapse = ", ")
      ),
      call. = FALSE
    )
  }
  invisible(TRUE)
}

parse_file_name <- function(file_name) {
  m <- regexec("^(.+)_([0-9]{2}Q[1-4])\\.parquet$", file_name, perl = TRUE)
  parts <- regmatches(file_name, m)[[1]]

  if (length(parts) != 3L) {
    return(list(ok = FALSE, table_name = NA_character_, quarter = NA_character_))
  }

  list(
    ok = TRUE,
    table_name = toupper(parts[2]),
    quarter = toupper(parts[3])
  )
}

safe_unique_char <- function(x) {
  unique(normalize_text(x[!is.na(x)]))
}

quarter_rank <- function(qtr) {
  qtr <- toupper(normalize_text(qtr))
  out <- rep(-1L, length(qtr))

  ok <- !is.na(qtr) & grepl("^[0-9]{2}Q[1-4]$", qtr)
  if (any(ok)) {
    yy <- as.integer(substr(qtr[ok], 1, 2))
    qq <- as.integer(substr(qtr[ok], 4, 4))

    # Handles historic 2-digit years more safely than lexicographic ordering.
    yyyy <- ifelse(yy >= 60L, 1900L + yy, 2000L + yy)
    out[ok] <- yyyy * 10L + qq
  }

  out
}

manifest_spec_standardize <- function() {
  list(
    cols = c("output_table", "quarter", "out_file", "n_rows", "status", "timestamp", "notes"),
    types = c(
      output_table = "character",
      quarter      = "character",
      out_file     = "character",
      n_rows       = "integer",
      status       = "character",
      timestamp    = "character",
      notes        = "character"
    ),
    key = c("output_table", "quarter", "out_file")
  )
}

coerce_manifest_types <- function(dt, spec) {
  dt <- as.data.table(copy(dt))
  
  # add missing columns
  for (nm in spec$cols) {
    if (!nm %in% names(dt)) {
      dt[, (nm) := NA]
    }
  }
  
  # keep only expected columns, in expected order
  dt <- dt[, spec$cols, with = FALSE]
  
  # force stable types with set()
  for (nm in names(spec$types)) {
    target_type <- spec$types[[nm]]
    val <- dt[[nm]]
    
    if (target_type == "character") {
      val <- as.character(val)
    } else if (target_type == "integer") {
      val <- as.integer(val)
    } else if (target_type == "logical") {
      val <- as.logical(val)
    } else if (target_type == "numeric") {
      val <- as.numeric(val)
    } else {
      stop("Unsupported target type: ", target_type, call. = FALSE)
    }
    
    set(dt, j = nm, value = val)
  }
  
  dt[]
}

load_manifest_generic <- function(path, spec) {
  if (!file.exists(path)) {
    return(coerce_manifest_types(data.table(), spec))
  }
  
  dt <- fread(path, na.strings = c("", "NA"))
  coerce_manifest_types(dt, spec)
}

save_manifest_generic <- function(dt, path) {
  fwrite(dt, path)
}

upsert_manifest_row_generic <- function(manifest, row_dt, spec) {
  manifest <- coerce_manifest_types(manifest, spec)
  row_dt   <- coerce_manifest_types(row_dt, spec)
  
  key_cols <- spec$key
  
  if (nrow(manifest) == 0L) {
    return(copy(row_dt))
  }
  
  manifest_key <- do.call(paste, c(manifest[, ..key_cols], sep = "\r"))
  row_key      <- do.call(paste, c(row_dt[, ..key_cols], sep = "\r"))
  idx <- match(row_key, manifest_key)
  
  if (is.na(idx)) {
    manifest <- rbindlist(list(manifest, row_dt), fill = TRUE, use.names = TRUE)
  } else {
    for (nm in spec$cols) {
      set(manifest, i = idx, j = nm, value = row_dt[[nm]])
    }
  }
  
  manifest[]
}