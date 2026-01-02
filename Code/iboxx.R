#############################################
# iboxx.R
# Nicholas von Turkovich
# Date: 2023-10-30
# Note: Main file that cleans and uploads iBoxx data
#############################################

# Clear workspace
rm(list = setdiff(ls(), c("years", "test")))

# Load environment variables from .env file if present
if (file.exists(".env")) {
  readRenviron(".env")
}

# Import libraries

# Helper functions
source("utilities/utilities.R")
source("data/iboxx/compile_iboxx_shared.R")

# Compile data and upload to server -----------------------------------------

years <- 2012:2025
test <- FALSE

process_iboxx_data(get_iboxx_config("USD", "underlyings"), years, test)
process_iboxx_data(get_iboxx_config("GLOBALHY", "underlyings"), years, test)
process_iboxx_data(get_iboxx_config("EUR", "underlyings"), years, test)
process_iboxx_data(get_iboxx_config("USD", "components"), years, test)
process_iboxx_data(get_iboxx_config("EUR", "components"), years, test)

# Compute QuantLib values -------------------------------------------------

# Specify the path to your Python environment
reticulate::use_condaenv("research_data", required = TRUE)

# Run the Python script
reticulate::py_run_file("./data/iboxx/compute_ql.py")
reticulate::py_run_file("./data/iboxx/compare_ql.py")


