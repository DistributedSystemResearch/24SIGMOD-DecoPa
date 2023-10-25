#!/bin/bash

cd code/
# autoscaling folder
# 3 loops, nw, sel, dop
# nw_sel_dop_folder
# for 10 networks nw x
# for 6 selectivities per network sel x
# for dop 5 10 15 20 dop x

LC_NUMERIC=C
for nw in {1..1}; do
	scaling=1
	python generate_selectivity.py
	python generate_network.py 1 "$scaling" #new network generated and must be saved to pickle 

		for par in  10 15 20;do			
		#for tw in 1 10 20 30; do
			dir_name="${nw}_${par}"

			
			noScaling=0
			scaling=1.0
		        last=$scaling
			old=100.0
			dif=1000
			mkdir -p ../plans/autoscaling/"$dir_name"
			#get initial value
			python generate_network.py 0 "$scaling" #$qlen
			python generate_selectivity.py
			python generate_qwls.py ${qlen}	
			python generate_projections.py			
			python combigen_latency_optimized_shorter_playground_qwl.py ${par} 1 #${tw}
			
			while (( $(echo "$dif > 0.001" | bc -l) )) || [ "$(cat parallel.txt)" -gt 0 ]; do
		            tail -n 1 parallel.txt >> parallel_all.txt
			    old=$scaling
			    if [ "$(cat parallel.txt)" -eq 0 ]; then
				scaling=$(echo "$scaling + $last" | bc -l) #always doubling if there is some space is stupid!
				echo "MODIFIED SCALING $scaling and old $old"
				python generate_network.py 0 "$scaling"
				python combigen_latency_optimized_shorter_playground_qwl.py "${par}" 1 #${tw}
			    else
				thisScaling=$scaling
				scaling=$(echo "$thisScaling - $last / 2" | bc -l)
			        echo "MODIFIED SCALING $scaling and old $old"
				if (( $(echo "$scaling < 0" | bc -l) )); then
				    break
				fi
				python generate_network.py 0 "$scaling"
				python combigen_latency_optimized_shorter_playground_qwl.py "${par}" 1 #${tw}
			    fi

			    last=$(echo "$scaling - $old" | bc -l)
			    if (( $(echo "$last < 0" | bc -l) )); then
				last=$(echo "$last * -1" | bc -l)
			    fi

			    dif=$(echo "$old - $scaling" | bc -l)
			    if (( $(echo "$dif < 0" | bc -l) )); then
				dif=$(echo "$dif * -1" | bc -l)
			    fi
			done

			# generate parallel_scaling_folder and inputfolders, only if noScaling == 0			
			cd ..
			./generate_examples_twomatrices_parallel_only.sh "parallel_${scaling}" ${scaling}
			# move folder to directory	
			cd plans
			mv "parallel_${scaling}" autoscaling/"$dir_name"			
			cd ../code

		        
			# generate example for stateparallel
			noScaling=0
			scaling=1.0
		        last=$scaling
			old=100.0
			dif=1000
			python generate_network.py 0 "$scaling"
			python generate_projections.py
			python combigen_latency_optimized_shorter_playground_qwl.py ${par} 0 #${tw}

			while (( $(echo "$dif > 0.001" | bc -l) )) || [ "$(cat stateparallel.txt)" -gt 0 ]; do
			    tail -n 1 stateparallel.txt >> stateparallel_all.txt	
			    old=$scaling
			    if [ "$(cat stateparallel.txt)" -eq 0 ]; then
				scaling=$(echo "$scaling + $last" | bc -l) 
				echo "MODIFIED SCALING $scaling and old $old"
				python generate_network.py 0 "$scaling"
				python combigen_latency_optimized_shorter_playground_qwl.py "${par}" 0 #${tw}
			    else
				thisScaling=$scaling
				scaling=$(echo "$thisScaling - $last / 2" | bc -l)
				if (( $(echo "$scaling < 0" | bc -l) )); then
				    break
				fi
				python generate_network.py 0 "$scaling"
				python combigen_latency_optimized_shorter_playground_qwl.py "${par}" 0 #${tw}
			    fi

			    last=$(echo "$scaling - $old" | bc -l)
			    if (( $(echo "$last < 0" | bc -l) )); then
				last=$(echo "$last * -1" | bc -l)
			    fi

			    dif=$(echo "$old - $scaling" | bc -l)
			    if (( $(echo "$dif < 0" | bc -l) )); then
				dif=$(echo "$dif * -1" | bc -l)
			    fi
			done

			python generate_network.py 0 "$scaling"
			python combigen_latency_optimized_shorter_playground_qwl.py "${par}"  1 #${tw}
			# generate state_parallel_scaling_folder and inputfolders, only if noScaling == 0

			cd ..
			./generate_examples_twomatrices.sh  "stateparallel_${scaling}" ${scaling}
			# move folder to directory	
			cd plans
			mv "stateparallel_${scaling}" autoscaling/"$dir_name"			
			cd ../code
			stateparallelScaling=$scaling

		
done
done
