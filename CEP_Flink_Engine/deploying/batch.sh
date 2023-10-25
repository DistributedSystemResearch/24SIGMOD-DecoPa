#!/bin/bash


function run() {
	set -x
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
	set +x
}
