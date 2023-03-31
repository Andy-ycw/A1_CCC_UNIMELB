from functions import *
from mpi4py import MPI
from config import CONFIG
from collections import defaultdict
import numpy as np
import json
import ijson
import pandas as pd

tweets_path, sal_path, counts_array_size = CONFIG

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

sal_json = json.load(open(sal_path, "r"))
sal_json_info = process_sal(sal_json)

# Initialise global variables necessary for data extraction and counting.
epoch = 0 
recvbuff = np.empty(size, dtype='object') if rank == 0 else None
            
send_counts = defaultdict(init_count_array)
with open(tweets_path, "rb") as f:
    tweet_counter = 0
    read_counter = 0
    for tweet in ijson.items(f, "item"):
        if tweet_counter == rank + size * read_counter:
            author_id = tweet["data"]["author_id"]
            loc_info = tweet["includes"]["places"][0]["full_name"]
            count_array = compute_counts(loc_info, sal_json_info)
            send_counts[author_id] += count_array
            read_counter += 1
        tweet_counter += 1

recvbuff = comm.gather(send_counts, 0)

if rank == 0:
    total_counts = defaultdict(init_count_array)
    for counts in recvbuff:
        for k, v in counts.items():
            total_counts[k] += v

    report(total_counts)


    
    
    
            


