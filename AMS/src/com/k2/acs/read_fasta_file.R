library(Biostrings)

filter_fasta_by_pattern <- function(fasta_data, pattern) {
  filtered_data <- fasta_data[grep(pattern, names(fasta_data))]
  return(filtered_data)
}

find_first_description_by_pattern <- function(fasta_data, pattern) {
  match_index <- grep(pattern, names(fasta_data))
  if (length(match_index) > 0) {
    return(names(fasta_data)[match_index[1]])
  } else {
    return(NULL)
  }
}

fasta_data <- readDNAStringSet("path/to/the/file.fasta")

filtered_fasta <- filter_fasta_by_pattern(fasta_data, "my_pattern")
