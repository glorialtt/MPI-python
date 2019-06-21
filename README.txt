To run the program, one should submit the .slurm file (written in bash) to SPARTAN.

The 'mpiexec' command line is to tell the SPARTAN to execute the program. 
Note that, one can pass different files to be processed by the program in the last two arguments of the 'mpiexec' command,
if no files specified, default files ('melbGrid.json' and 'bigTwitter.json' in this case) will be processed. 

One can also change the 'nodes' parameter to specify the number of nodes used to run the program,
One can also change the 'ntasks' parameter to specify the number of processors used to run the program.


Files:
mpiprogram.py contains all Python codes
1node1core.slurm is the slurm file for running with 1 node and 1 core;
1node8cores.slurm is the slurm file for running with 1 node and 8 cores;
2nodes8cores.slurm is the slurm file for running with 2 nodes and 8 cores;