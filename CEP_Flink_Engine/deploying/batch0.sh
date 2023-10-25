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


run "googleDCIE3/stateparallel_.15527343750000000000/parallel_example" 4h 0
#run "googleKLDIB2/stateparallel_4.97753906250000000000/parallel_example" 10m 0

#run "googleBEHA/parallel_.55957031250000000000_BHEA/parallel_example" 15m 0
#run "googleCEAG/parallel_.07910156250000000000/parallel_example" 12m 0

#run "googleCEAG/stateparallel_.03613281250000000000/parallel_example" 20m 0
#run "googleKLDIB/stateparallel_.41625976562500000000/parallel_example" 20m  0
#run "googleBEHA/stateparallel_.08401489257812500000/parallel_example" 1h 0
