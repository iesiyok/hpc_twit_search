#!/usr/bin/env bash
#	iesiyok, 
#	9 Apr 2015, 14:21
#	This is the general script for running twit_search.py program in parallel
#	Usage : cmd> ' ./gen_script.sh [node] [core] search_str '
#	[node] and [core] arguments are compulsory
#	For running this program on the edward or any HPC system:
#	>> qsub -q fast -l nodes=1:ppn=8,walltime=12:00:00,pmem=2000mb -F "1 8 abbott" gen_script.sh

clear

wordList=('abbott' 'cancer' 'michael' 'australia' 'melbourne' 'facebook' 
'football' 'soccer' 'council' 'victoria' 'hospital' 'hospitality' 
'laptop' 'suburbs' 'flinders' 'tennis' 'youtube' 'sydney' 
'canberra' 'street' 'research' 'science' 'computing' 'engineering' 
'scientist' 'society' 'software' 'movie' 'museum' 'avenue' 
'program' 'university' 'college' 'london' 'berlin' 'warming' 
'obama' 'merkel' 'putin' 'european' 'union' 'ticket'
'weather' 'club' 'global' 'online' 'internet' 'heavy'
'traffic' 'phone')
wordList_count=50

# random_num : creates random number between 0 - wordList_count
function random_num() {
	value=`od -An -N2 -tu2 /dev/urandom | head -c 4`
	r_num=`expr $value % $wordList_count`
	return $r_num
}
# error_msg : generates error message if node and core arguments are not given
function error_msg() {
	echo "node and core numbers are compulsory..search_str isn't compulsory.." 
	echo "\"usage twit_search.sh 'node' 'core' 'search_str'\""
	echo "exiting.."
	exit
}

random_num
r_num=$?
r_str=${wordList[$r_num]} #getting the word from list according to the index

if [ "$1" == "" ]; then #$1 is node
	error_msg
fi

if [ "$2" == "" ]; then #$2 is core
	error_msg
fi

if [ "$3" == "" ]; then #$3 is search string which can be created randomly
	search_str=$r_str
	echo "random search string = '$search_str'" 
else
	search_str=$3
fi

if [ "$1" -eq 1 -a "$2" -eq 1 ]; then
	queue='serial'
else
	queue='parallel'
fi
echo $search_str
echo $queue
core=$(($1 * $2)) #total number of cores is = node * core

#PBS -M iesiyok@

#load modules : mpi and python
module load openmpi-gcc
module load python/2.7.3-gcc

cd $PBS_O_WORKDIR
dir="/home/iesiyok/data/Twitter.csv"
size=`ls -lh $dir`
echo $size
cd $PBS_O_WORKDIR
#call python code to run search program
mpiexec -np $core python twit_search.py $search_str "${dir}" ${PBS_JOBID} 






