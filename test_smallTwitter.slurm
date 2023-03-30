#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=8

##SBATCH --time=01:00:00 

module purge
module load foss/2019b
module load python/3.7.4
module load numpy/1.18.0-python-3.7.4
module load mpi4py/3.0.2-timed-pingpong
module load jq/1.5

mpirun -n 8 python main.py

##DO NOT ADD/EDIT BEYOND THIS LINE##
##Job monitor command to list the resource usage
my-job-stats -a -n -s

