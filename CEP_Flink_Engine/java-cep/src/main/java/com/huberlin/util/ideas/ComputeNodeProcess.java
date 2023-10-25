package com.huberlin.util.ideas;

/**
 * The process that does "the work done by a node in the muse graph". We have one of these per compute node.
 * <p>
 * Generates the matches of its projection from the matches of its inputs. Possible implementations:
 *    - Using a copy of flink [1]
 *    - Using the old engine written in c#
 * <p>
 * Communicates with other instances through message-passing. (Possible implementations:
 *  TCP sockets;
 *  unix domain, or other, sockets;
 *  java producer-consumer queues (if all CNPs are threads in one big multithreaded program, where threads share nothing except the queues)
 *  Java object serialization scheme (java RMI/RPC + stubs of queues)?
 * <p>
 *
 *  ----
 *  Footnotes
 *  [1] Flink is likely to also be overkill however: The expected use of flink is to represent the entire combination (or even,
 * the entire muse graph) as one flink-CEP graph. It seems unlikely to be efficient to use flink just to do one CEP matching step
 * per flink process. But flink is likely capable of it...
 */
public class ComputeNodeProcess {
    public String nodeID;


}


