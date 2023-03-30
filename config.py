CONFIG = (
    # "twitter-data-small.json",
    "./data/smallTwitter.json", # file_path
    "./data/sal.json", # sal.json
    6000, # interval_len - use 25000 in Spartan
    8, # node_number
    7, # counts_array_size - according to the format in notebook, it's the number of fields necessary for recording total counts and capital counts.
    
)

# jq 1.5
jq = "utility/jq-osx-amd64"

# jq 1.6 on local; 1.5 on spartan
# jq = "jq"