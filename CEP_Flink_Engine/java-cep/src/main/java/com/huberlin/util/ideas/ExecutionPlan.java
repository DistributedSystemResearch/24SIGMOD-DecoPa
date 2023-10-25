package com.huberlin.util.ideas;

import org.apache.commons.lang3.NotImplementedException;

/**
 * A plan for executing CEP queries, as per Samira's sigmod24 paper.
 * This is basically a graph of muse query projections ('combination') with at most 2 inputs per node, where each
 * node is labeled with a multiplicity (how many resources to assign to it), and where for each node we specify which
 * of the two inputs is to be partitioned, and which is to be broadcast.
 * <p>
 * This will be used to generate multiplicity(node) 'computing node process' per node. Each of these will run flink or
 * some other engine, to produce the matches of the complex event denoted by the projection
 * <p>
 * Specifies
 * - a list of processing nodes (resources)
 * - for each resource:
 *  - multiplicity
 *  - projection evaluated
 *  -
 */
public class ExecutionPlan {
    static ExecutionPlan deserialize(String serialized){
        throw new NotImplementedException("");
    }


}
