#' Create Database Connections
#' 
#' Establishes connections to SQL Server and optionally to WRDS and SFTP servers.
#'
#' @param include_wrds Logical. If TRUE, also creates WRDS database connection (default FALSE).
#' @param include_sftp Logical. If TRUE, also creates SFTP connection (default FALSE).
#' @param sftp_folder Character. Folder path for SFTP connection. Must be provided if include_sftp is TRUE.
#' 
#' @return List containing database connection objects.
#' @export
create_connections <- function(include_wrds = FALSE, include_sftp = FALSE, sftp_folder = NULL) {

  dotenv::load_dot_env()
  
  if (is.null(Sys.getenv("mssql_uid")) | is.null(Sys.getenv("mssql_pw"))){
    stop("UID/PW not set in environment variables")
  }
  
  # SQL Server connection
  sql_conn <- DBI::dbConnect(
    odbc::odbc(),
    Driver = "{ODBC Driver 17 for SQL Server}",
    Server = "sts-liramota",
    Database = "master",
    UID = Sys.getenv("mssql_uid"),
    PWD = Sys.getenv("mssql_pw")
  )

  # WRDS connection (if requested)
  if (include_wrds) {
    wrds <- DBI::dbConnect(
      RPostgres::Postgres(),
      host = "wrds-pgdata.wharton.upenn.edu",
      port = 9737,
      dbname = "wrds",
      user = Sys.getenv("WRDS_USER"),
      password = Sys.getenv("WRDS_PASSWORD"),
      sslmode = "require"
    )
  }

  # SFTP connection (if requested)
  if (include_sftp) {
    if (is.null(sftp_folder)) {
      stop("sftp_folder argument must be provided if include_sftp is TRUE.")
    }
    sftp_con <- sftp::sftp_connect(
      server = "sftp2.ihsmarkit.com", 
      folder = sftp_folder, 
      username = Sys.getenv("sftp_username"), 
      password = Sys.getenv("sftp_pw"), 
      port = 22,
      timeout = 60*3
    )
  }

  connections <- list(sql_conn = sql_conn)
  if (include_wrds) connections$wrds <- wrds
  if (include_sftp) connections$sftp_con <- sftp_con

  return(connections)
}

#' Get configuration for different iBoxx data types
#' @param currency "USD" or "EUR"
#' @param data_type "components" or "underlyings"
#' @return Configuration list
get_iboxx_config <- function(currency, data_type) {
  
  configs <- list(
    "USD_components" = list(
      currency = "USD",
      data_type = "components",
      identifier = "cusip",
      sftp_folder = "IBOXX_USD/USD_EOM_COMPONENTS",
      zip_pattern = "iboxx_usd_eom_components.*.zip",
      csv_pattern = "iboxx_usd_eom_components.*.csv",
      no_zip_pattern = "iboxx_usd_eom_components_",
      table_prefix = "components_usd"
    ),
    "USD_underlyings" = list(
      currency = "USD",
      data_type = "underlyings",
      identifier = "cusip",
      sftp_folder = "IBOXX_USD/IBOXX_USD_his/USD_EOD_UNDERLYINGS",
      zip_pattern = "iboxx_usd_eod_underlyings.*.zip",
      csv_pattern = "iboxx_usd_eod_underlyings.*.csv",
      no_zip_pattern = "iboxx_usd_eod_underlyings_",
      table_prefix = "underlyings_usd"
    ),
    "GLOBALHY_underlyings" = list(
      currency = "GLOBALHY",
      data_type = "underlyings",
      identifier = "cusip",
      sftp_folder = "IBOXX_GLOBALHY/IBOXX_GLOBALHY_his/UNDERLYINGS",
      zip_pattern = "iboxx_GblDevHy_eod_underlying.*.zip",
      csv_pattern = "iboxx_GblDevHy_eod_underlying.*.csv",
      no_zip_pattern = "iboxx_GblDevHy_eod_underlying_",
      table_prefix = "underlyings_gbldevhy"
    ),
    "EUR_components" = list(
      currency = "EUR",
      data_type = "components",
      identifier = "cusip",
      sftp_folder = "IBOXX_EUR/eur_eom_components",
      zip_pattern = "iboxx_eur_eom_components.*.zip",
      csv_pattern = "iboxx_eur_eom_components.*.csv",
      no_zip_pattern = "iboxx_eur_eom_components_",
      table_prefix = "components_eur"
    ),
    "EUR_underlyings" = list(
      currency = "EUR",
      data_type = "underlyings",
      identifier = "isin",
      sftp_folder = "IBOXX_EUR/IBOXX_EUR_his/EUR_EOD_UNDERLYINGS",
      zip_pattern = "_eod_underlyings.*.zip",
      csv_pattern = "_eod_underlyings.*.csv",
      no_zip_pattern = "_eod_underlyings_",
      table_prefix = "underlyings_eur"
    )
  )
  
  config_key <- paste(currency, data_type, sep = "_")
  if (!config_key %in% names(configs)) {
    stop(paste("Invalid configuration:", currency, data_type))
  }
  
  return(configs[[config_key]])
} 

#' Clean iBoxx file with configurable parameters
#' @param file_name Path to the CSV file to clean
#' @param config Configuration list with data type specific parameters
#' @return Cleaned data.frame
clean_iboxx_file <- function(file_name, config) {

  # Read in file as data.frame
  df <- read.csv(file_name, check.names = FALSE, stringsAsFactors = FALSE)

  # Clean column names
  colnames(df) <- tolower(colnames(df))
  colnames(df) <- gsub("\\s+", "_", colnames(df))
  colnames(df) <- gsub("-", "_", colnames(df))
  colnames(df) <- gsub("\\+", "plus", colnames(df))

  # Additional column cleaning for EUR underlyings
  if ((config$currency == "EUR" || config$currency == "GLOBALHY") && config$data_type == "underlyings") {
    colnames(df) <- gsub("\\(", "", colnames(df))
    colnames(df) <- gsub("\\)", "", colnames(df))
  }

  # Make sure date/identifier is not empty
  df <- dplyr::filter(df, .data$date != "")
  df <- dplyr::filter(df, .data[[config$identifier]] != "")

  # Ensure file only has a single date's data
  df_date <- unique(df$date)
  if (length(df_date) != 1) {
    stop("Daily file contains multiple dates")
  }

  # Format date
  if (grepl("^\\d{4}-\\d{2}-\\d{2}$", df$date[1])) {
    df$date <- as.Date(df$date)
  } else if (grepl("^\\d{1,2}/\\d{1,2}/\\d{4}$", df$date[1])) {
    df$date <- as.Date(df$date, format = "%m/%d/%Y")
  } else {
    stop("Unknown date format")
  }
  df_date <- df$date[1]

  # Remove blanks
  df[df == ""] <- NA

  # Put date first column
  df <- dplyr::select(df, date, dplyr::any_of(config$identifier), dplyr::everything())

  # Scale notional amount by 1e6 to represent as millions of dollars
  if ("notional" %in% colnames(df)) {
    df <- dplyr::mutate(df, notional = .data$notional / 1e6)
  }

  if ("notional_amount" %in% colnames(df)) {
    df <- dplyr::mutate(df, notional_amount = .data$notional_amount / 1e6)
  }

  if ("notional_amount_constrained_local_currency" %in% colnames(df)) {
    df <- dplyr::mutate(df, notional_amount_constrained_local_currency = .data$notional_amount_constrained_local_currency / 1e6)
  }

  # Set encoding for column of issuer name
  if ("issuer" %in% colnames(df)) {
    df$issuer <- iconv(df$issuer, from = "ISO-8859-1", to = "UTF-8")
  }

  if ("issuer_name" %in% colnames(df)) {
    df$issuer_name <- iconv(df$issuer_name, from = "ISO-8859-1", to = "UTF-8")
  }

  # Additional encoding for EUR underlyings
  if ((config$currency == "EUR" || config$currency == "GLOBALHY") && config$data_type == "underlyings") {
    if ("index_name" %in% colnames(df)) {
      df$index_name <- iconv(df$index_name, from = "ISO-8859-1", to = "UTF-8")
    }

    # Make local_1 and local_2 characters
    if ("local_1" %in% colnames(df)) {
      df$local_1 <- as.character(df$local_1)
    }
    if ("local_2" %in% colnames(df)) {
      df$local_2 <- as.character(df$local_2)
    }
  }

  # Check to make sure that the dataframe is not empty
  if (nrow(df) == 0) {
    stop(paste0("Empty dataframe for file ", file_name))
  }

  # Wherever first character is a number, prefix with an x
  num_cols <- colnames(df)[grepl("^[0-9]", colnames(df))]
  if (length(num_cols) > 0) {
    new_names <- paste0("x", num_cols)
    colnames(df)[match(num_cols, colnames(df))] <- new_names
  }

  # If fx_version in columns when GLOBALHY, make sure only Index Currency is kept
  if ("fx_version" %in% colnames(df) && config$currency == "GLOBALHY") {
    df <- dplyr::filter(df, .data$fx_version == "Index Currency")
  }

  # Check that identifier/date together are unique identifiers
  if (any(duplicated(df[, c("date", config$identifier)]))) {
    stop(paste("Date and", config$identifier, "don't form unique index"))
  }

  return(df)
}

#' Process iBoxx data for a specific configuration
#' @param config Configuration list with all necessary parameters
#' @param years Optional parameter to limit processing to recent years
#' @param test Optional parameter to run a test run
process_iboxx_data <- function(config, years = NULL, test = FALSE) {

  connections <- create_connections(include_sftp = TRUE, sftp_folder = config$sftp_folder)

  # Download names of zip files
  filenames <- sftp::sftp_listfiles(connections$sftp_con)
  filenames_zip <- sort(filenames$name[grepl(config$zip_pattern, filenames$name)])
  filenames_csv <- sort(filenames$name[grepl(config$csv_pattern, filenames$name)])

  years_in_zip <- unique(as.numeric(stringr::str_extract(filenames_zip, "\\d{4}")))
  years_in_csv <- unique(as.numeric(stringr::str_extract(filenames_csv, "\\d{4}")))

  # Drop NA values from years_in_zip and years_in_csv
  years_in_zip <- years_in_zip[!is.na(years_in_zip)]
  years_in_csv <- years_in_csv[!is.na(years_in_csv)]

  years_zip_only <- setdiff(years_in_zip, years_in_csv) 
  years_csv_only <- setdiff(years_in_csv, years_in_zip) 
  years_both <- intersect(years_in_zip, years_in_csv)

  years_both_zip <- years_both[years_both < max(years_zip_only)]
  years_both_csv <- years_both[years_both >= max(years_zip_only)]

  years_zip <- c(years_zip_only, years_both_zip) |> sort()
  years_no_zip <- c(years_both_csv, years_csv_only) |> sort()

  if (!is.null(years)){
    years_zip <- intersect(years_zip, years)
    years_no_zip <- intersect(years_no_zip, years)
  }

  filenames_no_zip <- filenames$name[grepl(paste0(config$no_zip_pattern, "(", paste(years_no_zip, collapse = "|"), ").*.csv"), filenames$name)]

  # Process zip files
  if (length(years_zip) > 0){
    purrr::map(years_zip,
               .f = function(y) {
                 
                 # Delete all files in temp folder
                 unlink("Data/Temp/*.csv", recursive = TRUE)
                 unlink("Data/Temp/*.zip", recursive = TRUE)

                 # Files of a particular year
                 fname <- filenames_zip[grepl(y, filenames_zip)]
                 
                 # Download zip file from sftp server
                 sftp::sftp_download(file = fname, tofolder = "Data/Temp", sftp_connection = connections$sftp_con)
                 
                 # Full path to file
                 fname_full <- paste0("Data/Temp/", fname)
                 
                 # Unzip file
                 unzip(fname_full, exdir = "Data/Temp")
                 
                 # Subset files for that year
                 year_files <- list.files("Data/Temp/", pattern = ".*.csv")
                 
                 # Year of files
                 y <- as.numeric(stringr::str_extract(fname, "\\d{4}"))
                 
                 # Make sure year_files include only the year in question
                 year_files <- year_files[grepl(y, year_files)]
                 
                 if (length(year_files) > 0){
                   # Load and clean file names
                   df_year <- purrr::map(paste0("Data/Temp/", year_files),
                                         ~clean_iboxx_file(.x, config)) |>
                     purrr::reduce(dplyr::bind_rows)
                   
                   table_name <- paste0(config$table_prefix, "_", y, ifelse(test, "_test", ""))
                   DBI::dbWriteTable(
                     conn = connections$sql_conn,
                     name = DBI::Id(catalog = "iboxx", schema = "dbo", table = table_name),
                     value = df_year
                   )
                 }
                 
                 rm(df_year)
                 
                 # Delete all files in temp folder
                 unlink("Data/Temp/*.csv", recursive = TRUE)
                 unlink("Data/Temp/*.zip", recursive = TRUE)
                 
                 gc()
                 print(paste0("Year ", y, " completed"))
                 
               })
  }

  # Process non-zip files
  if (length(years_no_zip) > 0){
    purrr::map(years_no_zip,
               .f = function(y) {
                 
                 # Delete all files in temp folder
                 unlink("Data/Temp/*.csv", recursive = TRUE)
                 unlink("Data/Temp/*.zip", recursive = TRUE)
                 
                 # Files of a particular year
                 fnames <- filenames_no_zip[grepl(paste0("_", y), filenames_no_zip)]
                 
                 chunk_size <- 100
                 fn_chunks <- split(fnames, ceiling(seq_along(fnames)/chunk_size))

                 for (chunk in fn_chunks) {
                   purrr::walk(chunk, ~ sftp::sftp_download(file = .x, tofolder = "Data/Temp", sftp_connection = connections$sftp_con))
                 }

                 # Subset files for that year
                 year_files <- list.files("Data/Temp/", pattern = ".*.csv")
                 
                 if (length(year_files) > 0){
                   # Load and clean file names
                   df_year <- purrr::map(paste0("Data/Temp/", year_files),
                                         ~clean_iboxx_file(.x, config)) |>
                     purrr::reduce(dplyr::bind_rows)
                   
                   table_name <- paste0(config$table_prefix, "_", y, ifelse(test, "_test", ""))
                   
                   DBI::dbWriteTable(
                     conn = connections$sql_conn,
                     name = DBI::Id(catalog = "iboxx", schema = "dbo", table = table_name),
                     value = df_year
                   )
                 }
                 
                 rm(df_year)
                 
                 # Delete all files in temp folder
                 unlink("Data/Temp/*.csv", recursive = TRUE)
                 unlink("Data/Temp/*.zip", recursive = TRUE)
                 
                 gc()
                 print(paste0("Year ", y, " completed"))
                 
               })
  }

  purrr::walk(connections, function(con) {
    tryCatch({
      DBI::dbDisconnect(con)
    }, error = function(e) {
      # Ignore errors on disconnect
      invisible(NULL)
    })
  })
}