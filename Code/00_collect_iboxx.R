source("Code/utils.r")

config <- get_iboxx_config("USD", "underlyings")

process_iboxx_data(config, years = 2002, test = TRUE)
