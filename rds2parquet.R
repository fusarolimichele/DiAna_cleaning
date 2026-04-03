#!/usr/bin/env Rscript
# This script converts RDS files in the input directory
# to parquet files, writing them to the output directory.
# Usage: rds2parquet.R input_dir output_dir

suppressPackageStartupMessages(library(arrow))

args <- commandArgs(trailingOnly = TRUE)

if (length(args) < 2) {
  stop("Usage: rds2parquet.R input_dir output_dir", call. = FALSE)
}

input_dir <- args[1]
output_dir <- args[2]

if (!dir.exists(output_dir)) {
  stop("Output directory does not exist: ", output_dir, call. = FALSE)
}

if (!dir.exists(input_dir)) {
  stop("Input directory does not exist: ", input_dir, call. = FALSE)
}

rds_files <- list.files(input_dir, pattern = "\\.rds$", full.names = TRUE)

if (length(rds_files) == 0) {
  message("No RDS files found in ", input_dir)
  quit(status = 0)
}

message("Found ", length(rds_files), " RDS file(s):")
for (i in seq_along(rds_files)) {
  message(i, ": ", basename(rds_files[i]))
}

message("\nStarting conversion:")
for (i in seq_along(rds_files)) {
  rds_file <- rds_files[i]
  obj <- readRDS(rds_file)
  parquet_file <- file.path(
    output_dir,
    paste0(tools::file_path_sans_ext(basename(rds_file)), ".parquet")
  )
  write_parquet(as.data.frame(obj), parquet_file, compression = "zstd")
  message(i, ": Converted ", basename(rds_file), " -> ", basename(parquet_file))
}
