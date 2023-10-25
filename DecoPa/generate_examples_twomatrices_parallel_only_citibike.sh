#!/bin/sh
OUTPUTDIR=$1
myscaling=$2 
mkdir -p plans/parallel_example
mkdir -p plans/parallel_example/pyinput
cd code/
python generateEvaluationPlan_parallelized_os_json.py parallel_example
alphabet=$(cat "../plans/parallel_example/alphabet.txt")
cd ../poisson-event-gen 
python3.10 generate_trace_citibike.py ../plans/parallel_example/matrix.txt ../plans/parallel_example/matrix.txt 1 -f citibike_1d1min_2023_10_10.csv -M $myscaling -a $alphabet
cd traces/
cp 0/*.csv ../../plans/parallel_example
rm -r *
cd ../../code/
cp network current_wl selectivities projrates singleSelectivities ../plans/parallel_example/pyinput

cd ../plans/
mkdir -p "$OUTPUTDIR"

mv *_example "$OUTPUTDIR"

