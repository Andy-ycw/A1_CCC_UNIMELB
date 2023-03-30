import functions
from mpi4py import MPI
from config import CONFIG
from collections import defaultdict
import numpy as np
import logging
import json

logging.basicConfig(level=logging.ERROR)
# logging.debug('This will get logged')


file_path, sal_path,interval_len, node_number, default_array_size = CONFIG

comm = MPI.COMM_WORLD
rank = comm.Get_rank()

sal_json = json.load(open(sal_path, "r"))
sal_json_info = functions.process_sal(sal_json)

epoch = 0 

default_array = np.zeros(default_array_size)
total_counts = defaultdict(functions.init_count_array)
end = False

while not end:
    epoch_len = epoch * interval_len * node_number
    # For each node
    interval = (epoch_len+rank*interval_len, epoch_len+(rank+1)*interval_len)
    json_segments = functions.get_json_segments(file_path, interval)

    if len(json_segments) == 0:
        logging.info("Rank {} breaks".format(rank))
        if rank != 0:
            comm.send(None, dest=0)
        break
    
    send_buffer = defaultdict(functions.init_count_array)
    for tweet in json_segments:
        author_id = tweet["author_id"]
        loc_info = tweet["loc_info"]
        count_array = functions.compute_counts(loc_info, sal_json_info)
        send_buffer[author_id] += count_array

        #TODO: Task 1 - Extract data fpr greater capital city
        # - [Done] Use notebook to explore what kind of location, and formats are in the file. Document assumptions.
        #TODO: Task 2 - Extract data for author numbers

        #TODO: Task 3 - Extract data for autor/tweet geo info
        

    if rank != 0:
        # TODO: The data send need to be re-format. Also the receving side.
        logging.info("Rank {} epoch {} sending data in json array [{}].".format(rank, epoch, interval))
        comm.send(send_buffer, dest=0)
        logging.info("Rank {} epoch {} finishes processing data in json array [{}].".format(rank, epoch, interval))
        comm.recv(source=0) # blocking slaves
    else:
        counts_gathered = []
        counts_gathered.append(send_buffer)
        # Could make faster if necessary
        for i in range(1, 8):
            data = comm.recv(source=i)
            # data = comm.recv()
            if data:
                counts_gathered.append(data)

    # Process gathered data in 0
    if rank == 0:
        for counts in counts_gathered:
            for k, v in counts.items():
                total_counts[k] += v

        # notify slave to go to the next epoch
        for r in range(1,8):
            comm.send("Let's go", dest=r) 
    
    epoch += 1
        

if rank == 0:
    sort_count = sorted(total_counts.items(), key = lambda x: x[1][0], reverse=True)
    for count in sort_count[:10]:
        print('{} {}'.format(count[0], count[1][0]))
    
    
    
            


