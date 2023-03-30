from functions import *
from mpi4py import MPI
from config import CONFIG
from collections import defaultdict
import numpy as np
import logging
import json
import subprocess

# logging.basicConfig(level=logging.INFO)
# logging.debug('This will get logged')


file_path, sal_path,interval_len, node_number, counts_array_size = CONFIG

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

sal_json = json.load(open(sal_path, "r"))
sal_json_info = process_sal(sal_json)

# Calculate how many json objects each process will read
command = ['jq', f'. | length', file_path]
output = subprocess.check_output(command)
output_str = output.decode('utf-8')
jArraySize = int(output_str)
load = jArraySize//8
start = rank*load
end = (rank+1)*load if rank != 7 else jArraySize - 1 



# Initialise global variables necessary for data extraction and counting.
epoch = 0 
recvbuff = np.empty(8, dtype='object') if rank == 0 else None
            
counts = defaultdict(init_count_array)
while True:
    epoch_start = start+epoch*interval_len
    
    epoch_end = start+(epoch+1)*interval_len 
    epoch_end = end if epoch_end > end else epoch_end
    
    
    if epoch_start >= end:
        sendbuf = counts
        print("Rank {} finishes counting. Sending results.".format(rank))
        recvbuff = comm.gather(sendbuf, 0)
        break
    
    logging.debug("Rank {} starting epoch {}; progess: {}%".format(rank, epoch, round((epoch_start-start)/load*100)))
    logging.error("Utility flag")
    json_segments = get_json_segments(file_path, (epoch_start, epoch_end))
    
    for tweet in json_segments:
        author_id = tweet["author_id"]
        loc_info = tweet["loc_info"]
        count_array = compute_counts(loc_info, sal_json_info)
        counts[author_id] += count_array
    
    epoch += 1

if rank != 0:
    print("Rank {} finishes sending. Terminating.".format(rank))
else:
    print("Rank {} reveives all results. Start summarizing information.".format(rank))
    total_counts = defaultdict(init_count_array)
    for counts in recvbuff:
        for k, v in counts.items():
            total_counts[k] += v

    sort_count = sorted(total_counts.items(), key = lambda x: x[1][0], reverse=True)
    for count in sort_count[:10]:
        print('{} {}'.format(count[0], count[1][0]))
    
    
    
            


