#!/bin/bash
#run on pis to locally distribute jar
min=$(($1))
max=$(($2))
if [[ $max == 0 ]]; then
	max=20;
fi

for ((i=max; i>=min; i--))
do
	echo paramuno$i; 
	if [[ paramuno$i != $(hostname) ]];
		then scp ~/sm24/cep-node.jar paramuno$i:sm24/cep-node.jar;
	fi
done
