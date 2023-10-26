# DecoPa: Query Decomposition for Parallel Complex Event Processing

## Overview

This repository contains the implementation of algorithms for the construction of DecoPa plans, tools for the generation of input streams and a Flink-based implementation for parallel CEP matching.


#### INEv

The directory `DecoPa` contains the implementation of our algorithms and some of the scripts used to conduct the experiments presented in the paper.
Moreover, the subdirectory `DecoPa/poisson-event-gen` contains the tools to generate local input streams for a set of processing units as they would be provided by a shuffler, based on a given DecoPa plan.
To this end, either a poisson process is envoked for an event rate distribution or, the path to a `.csv`-file comprising a global input stream is passed. The latter was done for the two real-world data sets citibike and google cluster, which are also contained in the directory (`task\_events\_48h\_2023\_06\_10\_.tar.xz` and `citibike\_1d1min\_2023\_10\_10.tar.xz`).

The script `./findScaling_dop_binary_single.sh` can be used, to generate DecoPa plans (and respective local input streams) for a query of length 6 and for clusters with a increasing number of processing units. After the termination of the script, the resulting plans with respective scaling factors can be found in  `DecoPa/plans/autoscaling`. 


#### CEP_Flink_Engine

The directory `CEP_Flink_Engine` contains the implementation of a Flink-based implementation for parallel CEP matching. The folder `CEP\_Flink\_Engine/deploying/example\_inputs` contains the DecoPa plans and local streams used for the real-world data experiments. 


