#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks=8
#SBATCH --time=00:05:00
#SBATCH --cpus-per-task=1
module load Python/3.4.3-goolf-2015a
mpiexec -n 8 python mpiprogram.py melbGrid.json bigTwitter.json