#!/bin/bash

set -x

function run() {
	path="$1"
	duration="$2"
	cluster_id=$(($3))
	n=$((20))

	./run "example_inputs/cb/$path" $cluster_id $n &
	run_pid=$! # Capture the PID of the last backgrounded command (./run command)
	trap "pkill -P $run_pid" EXIT # Set a trap to kill the process group associated with the ./run command
        
	trap "echo skipping sleep" SIGINT
	sleep "$duration"
	trap - SIGINT

	./kill $cluster_id $n
    	
    	pkill -P $run_pid 	# Kill the process group associated with the ./run command
	trap - EXIT		# Reset the trap

    	mkdir -p "results/$path"
    	./collect $cluster_id $n "results/$path"
}

#run "googleKLDIB2/stateparallel_4.97753906250000000000/stateparallel_example" 10m 1
run "googleDCIE3/stateparallel_.15527343750000000000/stateparallel_example" 4h 1

#run "googleCEAG1/parallel_.84191894531250000000/parallel_example" 10m 0
#run "googleCEAG1/stateparallel_.02050781250000000000/parallel_example" 1h 0

#run "googleCEAG/stateparallel_.03613281250000000000/stateparallel_example" 20m 1
#run "googleKLDIB/stateparallel_.41625976562500000000/stateparallel_example" 20m  1
#run "googleKLDIB/parallel_.67187690734863281250/parallel_example" 12m  1

#run "googleBEHA/stateparallel_.08401489257812500000/parallel_example" 1h 1

#run "new_longsequence/stateparallel_.33300781250000000000/parallel_example" 100m 1
#run "new_longsequence/parallel_1.39697265625000000000/parallel_example" 10m 1

