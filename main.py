from functions import *
from mpi4py import MPI
from collections import defaultdict
import numpy as np
import json
import ijson
import pandas as pd
import os

# Process information in sal
sal_path = "data/sal.json"
sal_json = json.load(open(sal_path, "r"))
sal_json_info = process_sal(sal_json)

# Get MPI information
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

# Settings for processing the taget file
tweets_path = "data/bigTwitter.json"
counts_array_size = 9
file_size = os.path.getsize(tweets_path)
id_buff = None
author_id = None
loc_info = None
id_flag = '    "_id"'
loc_flag = '          "full_name"'
author_flag = '      "author_id"'    
stop_flag = None

if rank != 0:
    with open(tweets_path, 'r') as f:
        f.seek(rank * (file_size/size))
        f.readline()
        while True:
            line = f.readline()
            if line[:9] == id_flag:
                id_buff = line.split(":")[1][2:-3]
                break   

if rank != 0:
    comm.send(id_buff, dest=rank-1)
    # print(f"Rank {rank} - stop flag {id_buff} sent to rank {rank-1}")
if rank != size - 1:
    stop_flag = comm.recv(source=rank+1)
    # print(f"Rank {rank} - stop flag {stop_flag} received from rank {rank+1}")

finding_author = True
start_filtering = False
recvbuff = np.empty(size, dtype='object') if rank == 0 else None            
send_counts = defaultdict(init_count_array)
with open(tweets_path, "r") as f:
    f.seek(rank * (file_size/size))
    f.readline()
    while True:
        line = f.readline()
        if not line:
            break
        if line[:9] == id_flag:
            start_filtering = True
        if start_filtering:
            if line[:17] == author_flag and finding_author:
                finding_author = not finding_author
                author_id = line.split(":")[1][2:-3]
                
            if line[:21] == loc_flag and not finding_author:
                finding_author = not finding_author
                loc_info = line.split(":")[1][2:-3]
                count_array = compute_counts(loc_info, sal_json_info)
                send_counts[author_id] += count_array

            if line[:9] == id_flag:
                id_buff = line.split(":")[1][2:-3]
                if id_buff == stop_flag:
                    break
        
            
if size != 1:
    recvbuff = comm.gather(send_counts, 0)
else:
    if rank == 0:
        recvbuff = [send_counts]

if rank == 0:
    total_counts = defaultdict(init_count_array)
    for counts in recvbuff:
        for k, v in counts.items():
            total_counts[k] += v

    report(total_counts)


    
    
    
            


