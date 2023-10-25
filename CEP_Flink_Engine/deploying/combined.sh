#!/bin/bash

# Calling bash script a
./scaling_0_5.sh &

# Storing the process ID of bash script a
pid_a=$!

# Sleeping for 7 hours
sleep 5h


# Storing the process ID of bash script a
pid_a=$!

# Sleeping for 7 hours
sleep 5h

./scaling_0_15.sh 
