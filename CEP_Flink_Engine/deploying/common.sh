#!/bin/bash

BASE_DIR=$(realpath "$(dirname "$0")"/..) #directory above the one where this script is
JAVA_DIR="$BASE_DIR/java-cep"
PY_DIR="$BASE_DIR/python"
DEPLOYING_DIR="$BASE_DIR/deploying"
JAR="$JAVA_DIR/target/beispiel-1.0-SNAPSHOT.jar"
CLUSTER_SIZE=20

compile() {
        cd "$JAVA_DIR" || exit;
        mvn clean package || exit;
}

remote_compile() {
        ZIP="$DEPLOYING_DIR/$(uuidgen).zip"
        cd "$JAVA_DIR" || exit;
        zip --exclude="target" "$ZIP" ./*
        curl -X POST -F "file=@$ZIP" https://ob.ddnss.org/f679665b-232c-4d2d-91a7-05c02eb109ec/compileJava -o "$JAR"
        rm -f "$ZIP"
}

node() {
    CLUSTER_ID=$(($1))
    NODE_ID=$(($2))
	echo "paramuno$((CLUSTER_SIZE * CLUSTER_ID + NODE_ID))"
}

install_ssh_key() {
  cp "$DEPLOYING_DIR/ssh/sm24user" "$HOME/.ssh/sm24user" 
}

SSH() {
	ssh -F "$DEPLOYING_DIR/ssh/config" "$@"
}

SCP() {
  scp -F "$DEPLOYING_DIR/ssh/config" -p "$@"
}

install () {
  if (( $# < 2 )); then
    echo "Usage: install CLUSTER_ID N";
    echo "Install code to nodes 0..N-1 on cluster CLUSTER_ID";
    echo "N: number of nodes, max $CLUSTER_SIZE";
    echo "CLUSTER_ID: logical cluster ID (0, 1, 2 ...)";
    exit 1
  else
    CLUSTER_ID=$(($1))
	N_NODES=$(($2))
  fi;

  #if hostname | grep gruenau; then
  #         remote_compile
  #else
  #         compile
  #fi;


  for ((i=0; i<N_NODES; i++)); do
	  #delete everything on the remote
	  SSH "$(node $CLUSTER_ID $i)" "rm -rf sm24"
	
	  #create folder $HOME/sm24 and $HOME/sm24/conf on the remote
	  SSH "$(node $CLUSTER_ID $i)" "mkdir -p sm24/conf"
	  
	  #copy code
	  SCP "$JAR" "$(node $CLUSTER_ID $i):sm24/cep-node.jar"
	  SCP "$PY_DIR/send_eventstream.py" "$(node $CLUSTER_ID $i):sm24/"	  
  done;
}

run() {
  if (( $# < 3 )); then
    echo "Usage: run INPUT_DIR CLUSTER_ID N";
    echo "Deploys configs and runs all nodes."
    echo "INPUT_DIR: path to a folder with config_i.json and trace_i.csv for each node i";
    echo "CLUSTER_ID: cluster id"
    echo "N: number of nodes, 0..$((CLUSTER_SIZE-1))";
    exit 1  
  else
    INPUT_DIR="$(realpath "$1")"
    CLUSTER_ID=$(($2))
    N_NODES=$(($3))
  fi;

  echo "Ensuring no python sender or java is running..."
  kill_processes "$CLUSTER_ID" "$N_NODES"
  sleep 5

  #Copy configuration to nodes
  for ((i=0; i<N_NODES; i++)); do
	  #delete everything except the code on the remote
	  SSH "$(node "$CLUSTER_ID" "$i" )" "rm -rf sm24/conf/* sm24/*.csv sm24/trace* sm24/*.log sm24/*.err"

	  #delete temporary flink files
	  SSH "$(node "$CLUSTER_ID" "$i" )" "rm -rf /tmp/flink*.jar"
	
	  #copy stuff (add stuff here if you need more copied!)
	  SSH "$(node "$CLUSTER_ID" "$i" )" "mkdir -p sm24/conf"
	  SCP "$JAVA_DIR/conf/flink-conf.yaml" "$(node "$CLUSTER_ID" "$i" ):sm24/conf/"
    	  SCP "$DEPLOYING_DIR/address_book_$CLUSTER_ID.json" "$(node "$CLUSTER_ID" "$i" ):sm24/conf/address_book.json" 	#note the cluster ID-specific address book, which is required
	  SCP "$INPUT_DIR/config_$i.json" "$(node "$CLUSTER_ID" "$i" ):sm24/conf/config.json"
	  SCP "$INPUT_DIR/trace_$i.csv" "$(node "$CLUSTER_ID" "$i" ):sm24/"
  done;


	#start everywhere
  for ((i=0; i<N_NODES; i++)); do
		echo "starting $i"
	  SSH "$(node "$CLUSTER_ID" "$i" )" "cd sm24 && nohup java -Xmx256M -jar cep-node.jar >run.log 2>run.err && echo $? >run.exitcode &" &
  done;

	sleep 60;

	#check that all processes are running
  for ((i=0; i<N_NODES; i++)); do
		if ! SSH "$(node "$CLUSTER_ID" "$i" )" "pgrep java >/dev/null"; then
			echo "cep not running on node $i"
			exit 1
		fi
	done

	#start input srcs
  for ((i=0; i<N_NODES; i++)); do
		echo starting inputs on "$i"
		SSH "$(node "$CLUSTER_ID" "$i" )" "cd sm24 && nohup python3 send_eventstream.py $i >py.log 2>py.err &" &
	done

	sleep 10

	#wait for process termination
  finished=false
	while [[ "$finished" == false ]]; do
		finished=true;
		for ((i=0; i<N_NODES; i++)); do
			if SSH "$(node "$CLUSTER_ID" "$i" )" "pgrep java >/dev/null"; then
				echo "cep not finished on node $i"
				finished=false
				break
			fi
		done
		sleep 15
	done;

  runid=$(date '+%F-%H_%M_%S')
  OUTPUT_DIR="$INPUT_DIR/results/$runid"
  #collect_results "$CLUSTER_ID" "$N_NODES" "$OUTPUT_DIR"
}

collect_results() {
    CLUSTER_ID=$(($1))
	N_NODES=$(($2))
	OUTPUT_DIR=$(realpath "$3")
  	mkdir -p "$OUTPUT_DIR/inputs"	
	mkdir -p "$OUTPUT_DIR/mem_usage"	
	mkdir -p "$OUTPUT_DIR/throughput"	
	for ((i=0; i<N_NODES; i++)); do
	mkdir -p "$OUTPUT_DIR/unexpected/$i"
        #copy results		
    SCP "$(node "$CLUSTER_ID" "$i" ):sm24/run.log" "$OUTPUT_DIR/$i.log"
		SCP "$(node "$CLUSTER_ID" "$i" ):sm24/run.err" "$OUTPUT_DIR/$i.err"
		SCP "$(node "$CLUSTER_ID" "$i" ):sm24/py.err" "$OUTPUT_DIR/py$i.err"
		SCP "$(node "$CLUSTER_ID" "$i" ):sm24/py.log" "$OUTPUT_DIR/py$i.log"
		
	  SCP "$(node "$CLUSTER_ID" "$i" ):sm24/throughput_node_$i.csv" "$OUTPUT_DIR/throughput/"
	  SCP "$(node "$CLUSTER_ID" "$i" ):sm24/memory_usage_node_$i.csv" "$OUTPUT_DIR/mem_usage/"
        #copy inputs        
    SCP "$(node "$CLUSTER_ID" "$i" ):sm24/conf/config.json" "$OUTPUT_DIR/inputs/config_$i.json"
    SCP "$(node "$CLUSTER_ID" "$i" ):sm24/trace_$i.csv" "$OUTPUT_DIR/inputs/"
    SCP "$(node "$CLUSTER_ID" "$i" ):sm24/conf/address_book.json" "$OUTPUT_DIR/inputs/"
	  SCP "$(node "$CLUSTER_ID" "$i" ):sm24/conf/flink-conf.yaml" "$OUTPUT_DIR/inputs/"

	#copy unexpected files
	SCP "$(node "$CLUSTER_ID" "$i" ):sm24/hs-err*log" "$OUTPUT_DIR/unexpected/$i/"

    
  done

	for file in "$OUTPUT_DIR/"*.err; do
    if [[ ! -s "$file" ]]; then rm -f "$file"; fi; 
	done;
}

kill_processes() {
  if (( $# < 2 )); then
    echo "Usage: kill CLUSTER_ID N"
	  echo "Stop nodes 0..N-!";
    echo "N: number of nodes";
    exit 1  
  else
    CLUSTER_ID=$(($1))
	N_NODES=$(($2))
  fi;

	#kill all jvms
  for ((i=0; i<N_NODES; i++)); do
		echo "TERM $i"
	  SSH "$(node "$CLUSTER_ID" "$i" )" "killall -q -w java"
      SSH "$(node "$CLUSTER_ID" "$i" )" "killall -q -w python3"
  done;
}

install_ssh_key
