# A1_CCC_UNIMELB

Before running the program: 
1. Download json data from canvas/spartan
2. In config.py, change CONFIG[0] to the path to the target twitter json; change CONFIG[1] to sal.json.

Run the below command to try the program.
`mpirun -n 8 python main.py`

Let me know if you run into any problems!

## Note
1. jq 1.5 and 1.6 behave very different. 
    - Changing the interval_len in config.py will change the counting in 1.5; while 1.6 is not affected by this.

2. At worst the root needs to sort 10^7 rows (estimated number of 20GB of twitter counts). However, need to be aware that numpy crashes at 10**10.