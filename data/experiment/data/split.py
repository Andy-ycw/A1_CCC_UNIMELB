import os 
import subprocess
from mpi4py import MPI

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()
file = "twitter-data-small.json"

# ß
    # subprocess.run(["split", "-d", "-n", str(size), "-a", "1", "twitter-data-small.json", "chunk"])
file_size = os.path.getsize(file)
    
comm.bcast("", root=0)

with open(file, 'r') as f:
    f.seek(rank * (file_size/size))
    f.readline()
    id_flag = '    "_id"'
    loc_flag = '          "full_name"'
    
    id_buff = ""
    loc_buff = ""
    
    finding_id = True


    inf_counter = 0
    while True:
        line = f.readline()
        if line[:9] == id_flag and finding_id:
            finding_id = not finding_id
            id_buff = line.split(":")[1][2:-3]
            print(id_buff)
        if line[:21] == loc_flag and not finding_id:
            finding_id = not finding_id
            loc_buff = line.split(":")[1][2:-3]
            print(loc_buff)
            inf_counter += 1
            
        
        if inf_counter == 5:
            print("Too much")
            break
    # nake_line = line.strip()
    # if nake_line[:5] == '"_id"' or line[0] == "[":
    #     break
    #     # f.seek(ptr)
        # f.write("\n")
        
    
# with open(f"chunk{rank}", 'r+') as f:
#     for i in range(counter-1):
#         f.write("")

    

    

    