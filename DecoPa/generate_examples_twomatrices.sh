#!/bin/sh

OUTPUTDIR=$1
tw=${2:-1} 
length=$((4 * tw))
mkdir -p plans/parallel_example
mkdir -p plans/stateparallel_example
mkdir -p plans/parallel_example/pyinput
mkdir -p plans/stateparallel_example/pyinput
cd code/
python generateEvaluationPlan_parallelized_os_json.py parallel_example
python generateEvaluationPlan_state_parallel_json.py stateparallel_example
cd ../poisson-event-gen 
python3.10 generate_trace.py ../plans/parallel_example/matrix.txt ../plans/stateparallel_example/matrix.txt $length
cd traces/
cp 0/*.csv ../../plans/parallel_example
cp 1/*.csv ../../plans/stateparallel_example
rm -r *
cd ../../code/
cp network current_wl selectivities projrates singleSelectivities ../plans/parallel_example/pyinput
cp network current_wl selectivities projrates singleSelectivities ../plans/stateparallel_example/pyinput

cd ../plans/
mkdir -p "$OUTPUTDIR"

mv *_example "$OUTPUTDIR"

