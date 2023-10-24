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

		for par in 5 10 15;do			

			dir_name="${nw}_${par}"		
			noScaling=0
			scaling=1.0
		        last=$scaling
			old=100.0
			dif=1000
			mkdir -p ../plans/autoscaling/"$dir_name"
			#get initial value
			stateparallelScaling=1
			# generate example for single/centralized
			noScaling=0
			scaling=$stateparallelScaling
		        last=$scaling
			old=100.0
			dif=1000
			python generate_network.py 0 "$scaling"
			python combigen_latency_optimized_shorter_playground_qwl.py ${par} 0 #${tw}

			while (( $(echo "$dif > 0.001" | bc -l) )) || [ "$(cat singleOk.txt)" -eq 0 ]; do
			    old=$scaling
			    if [ "$(cat singleOk.txt)" -eq 1 ]; then
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

			# generate state_parallel_scaling_folder and inputfolders, only if noScaling == 0
			output_file1="single_${scaling}.txt"
			echo "nothing" > "$output_file1"
			mv $output_file1 ../plans/autoscaling/"$dir_name"	


			# generate example for llsf
			noScaling=0
			scaling=$stateparallelScaling
		        last=$scaling
			old=100.0
			dif=1000
			python generate_network.py 0 "$scaling"
			python combigen_latency_optimized_shorter_playground_qwl.py ${par} 0 #${tw}

			while (( $(echo "$dif > 0.001" | bc -l) )) || [ "$(cat llsfOk.txt)" -eq 0 ]; do
			    old=$scaling
			    if [ "$(cat llsfOk.txt)" -eq 1 ]; then
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

			# generate state_parallel_scaling_folder and inputfolders, only if noScaling == 0
			output_file1="llsf_${scaling}.txt"
			echo "nothing" > "$output_file1"
			mv $output_file1 ../plans/autoscaling/"$dir_name"

		   	# generate example for rip
			noScaling=0
			scaling=$stateparallelScaling
		        last=$scaling
			old=100.0
			dif=1000
			python generate_network.py 0 "$scaling"
			python combigen_latency_optimized_shorter_playground_qwl.py ${par} 0 #${tw}

			while (( $(echo "$dif > 0.001" | bc -l) )) || [ "$(cat ripOk.txt)" -eq 0 ]; do
			    old=$scaling
			    if [ "$(cat ripOk.txt)" -eq 1 ]; then
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

			# generate state_parallel_scaling_folder and inputfolders, only if noScaling == 0
			output_file1="rip_${scaling}.txt"
			echo "nothing" > "$output_file1"
			mv $output_file1 ../plans/autoscaling/"$dir_name"	

			# generate example for simplestate
			noScaling=0
			scaling=$stateparallelScaling
		        last=$scaling
			old=100.0
			dif=1000
			python generate_network.py 0 "$scaling"
			python combigen_latency_optimized_shorter_playground_qwl.py ${par} 0 #${tw}

			while (( $(echo "$dif > 0.001" | bc -l) )) || [ "$(cat simple_stateparallel.txt)" -eq 0 ]; do
			    old=$scaling
			    if [ "$(cat simple_stateparallel.txt)" -eq 1 ]; then
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

			# generate state_parallel_scaling_folder and inputfolders, only if noScaling == 0
			output_file1="simplestateparallel_${scaling}.txt"
			echo "nothing" > "$output_file1"
			mv $output_file1 ../plans/autoscaling/"$dir_name"	

done
done
