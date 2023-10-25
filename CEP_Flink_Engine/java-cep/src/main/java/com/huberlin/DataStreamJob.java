
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.huberlin;

import com.huberlin.communication.OldSourceFunction;
import com.huberlin.communication.EventSender;
import com.huberlin.communication.addresses.TCPAddressString;
import com.huberlin.config.JSONQueryParser;
import com.huberlin.config.QueryInformation;

import com.huberlin.event.ComplexEvent;
import com.huberlin.event.Event;
import com.huberlin.event.SimpleEvent;
import org.apache.commons.cli.*;

import java.lang.*;
import java.util.*;
import java.util.UUID;

import com.huberlin.communication.TCPEventSender;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.configuration.*;

import org.apache.flink.core.fs.FileSystem;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


import org.apache.flink.cep.pattern.Pattern;

import org.apache.flink.cep.*;

import org.apache.flink.streaming.api.TimeCharacteristic;

import java.time.LocalTime;

//metrics
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.metrics.Meter;//throughput
import org.apache.flink.metrics.MeterView;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Working legacy implementation of ComputeNodeProcess (CNP) [1]. This implementation has the following properties:
 * <ul>
 * <li>There is one java process, containing one flink process, per CNP</li>
 *
 * <li>This CNP communicates with other CNPs through messages passed via TCP [2]. Number of Sockets per CNP: N_input_nodes
 * + N_output_nodes + 1 (one for listening for incoming connections)</li>
 *
 * <li>They are serialized with .toString() and fromString methods in the Event class</li>
 *
 * <li>(Intended to change): The TCP connections aren't long lived; adds 1-2 RTT to message latency (might not matter)</li>
 *
 * <li>(Detail) Number  of threads per CNP: N_input_nodes + N_output_nodes + 1 (for sockets) + 1 (main)
 * + 1 (single-threaded flink) + 1 (throughput analytics); most of these are I/O blocked (except flink).</li>
 * </ul>
 * <p>
 * Footnotes<ol>
 * <li>Reminder: A CNP is the process that does the things that happen at a muse-graph-node, i.e. produce matches of a
 * 'bigger' projection from the matches of two 'smaller' projections.</li>
 * <li>Reminder: The CNPs always communicate with message-passing. However
 * that the messages are passed over TCP (and not through, say, a java object serialization mechanism) is a detail. Also,
 * the way the messages are serialized (or whether that is needed) is a detail.</li>
 * </ol>
 */


public class DataStreamJob {
    final static private Logger log = LoggerFactory.getLogger(DataStreamJob.class);
    private static final LinkedHashMap<String, Long> already_sent = createAlreadySentMap();

    private static LinkedHashMap<String, Long> createAlreadySentMap() {
        return new LinkedHashMap<String, Long>() {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Long> eldest) {
                return false;
            }
        };
    }

    private static void removeExpiredKeysFromAlreadySentMap(QueryInformation config) {
        Iterator<Map.Entry<String, Long>> iterator = already_sent.entrySet().iterator();
    
        while (iterator.hasNext()) {
            Map.Entry<String, Long> entry = iterator.next();
            if (Math.abs(entry.getValue() - OldSourceFunction.getCurrentGlobalWatermark()) > config.processing.time_window_size * 1000) {
                iterator.remove();
            } else {
                break; // LinkedHashMap maintains insertion order, so no need to check further entries
            }
        }
    }

    private static CommandLine parse_cmdline_args(String[] args) {
        final Options cmdline_opts = new Options();
        final HelpFormatter formatter = new HelpFormatter();
        cmdline_opts.addOption(new Option("localconfig", true, "Path to the local configuration file"));
        cmdline_opts.addOption(new Option("globalconfig", true, "Path to the global configuration file"));
        cmdline_opts.addOption(new Option("flinkconfig", true, "Path to the directory with the flink configuration"));
        final CommandLineParser parser = new DefaultParser();
        try {
            return parser.parse(cmdline_opts, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("java -jar cep-node.jar", cmdline_opts);
            System.exit(1);
        }
        assert (false);
        return null;
    }

    
    public static void main(String[] args) throws Exception {
        // Parse command line arguments

        CommandLine cmd = parse_cmdline_args(args);

        //Read global and local configuration files, and create config object ('QueryInformation')
        String filePath_local = cmd.getOptionValue("localconfig", "./conf/config.json"); // local config
        String filePath_global = cmd.getOptionValue("globalconfig", "./conf/address_book.json"); // global config
        QueryInformation config = JSONQueryParser.parseJsonFile(filePath_local, filePath_global);
        if (config != null) {
            System.out.println("Parsed JSON successfully");
            // You can now access the data structure's attributes, e.g., data.forwarding.send_mode or data.processing.selectivity
        } else {
            log.error("Failed to parse JSON");
            System.exit(1);
        }
        final int REST_PORT = 8081 + config.forwarding.node_id * 2;
        Configuration flinkConfig = GlobalConfiguration.loadConfiguration(cmd.getOptionValue("flinkconfig", "conf"));
        flinkConfig.set(JobManagerOptions.RPC_BIND_PORT, 6123+config.forwarding.node_id);
        flinkConfig.set(JobManagerOptions.PORT, 6123+config.forwarding.node_id);
        flinkConfig.set(RestOptions.BIND_PORT, REST_PORT + "-" + (REST_PORT + 1));
        flinkConfig.set(RestOptions.PORT, REST_PORT);
        flinkConfig.set(TaskManagerOptions.RPC_BIND_PORT, 51000+config.forwarding.node_id );
        flinkConfig.set(TaskManagerOptions.RPC_PORT, "0");
        flinkConfig.set(BlobServerOptions.PORT, "0");
                //Set up flink;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(flinkConfig);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        FileSystem.initialize(flinkConfig);

        // Only one engine-thread will work (output is displayed in the same way the packets arrive)
        env.setParallelism(1);


	// find out if outputselection is used, if yes, change the ids of the events that are selected for the match

        DataStream<Event> inputStream = env.addSource(new OldSourceFunction(
                5500+config.forwarding.node_id, //FIXME: read address book or config for (base) port
                config.forwarding.connections_to_establish.size()));


        SingleOutputStreamOperator<Event> monitored_stream = inputStream.map(new RichMapFunction<Event, Event>() {
            private transient MetricsRecorder.MetricsWriter memory_usage_writer;
            private transient MetricsRecorder.MemoryUsageRecorder memory_usage_recorder;
            private transient Thread memory_usage_recorder_thread;
        
            @Override
            public void open(Configuration parameters) {        
                // Initialize MetricsWriter
                memory_usage_writer = new MetricsRecorder.MetricsWriter("memory_usage_node_" + config.forwarding.node_id + ".csv");
        
                // Initialize and start the memory usage recorder thread
                memory_usage_recorder = new MetricsRecorder.MemoryUsageRecorder(memory_usage_writer);
        
                memory_usage_recorder_thread = new Thread(memory_usage_recorder);
        
                memory_usage_recorder_thread.start();
            }
        
            @Override
            public Event map(Event event) {

                return event;
            }
        
            @Override
            public void close() {
                // Stop and clean up the recorder threads and MetricsWriter
                memory_usage_recorder_thread.interrupt();
        
                try {
                    memory_usage_recorder_thread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).uid("metrics");


        ArrayList<Pattern<Event, ?>>  genericPatterns = PatternFactorySelector.create(config);
        DataStream<ComplexEvent> outputStream =null;


	// Case for unary Pattern
	if (config.processing.is_negated == 1 || config.processing.kleene_type == 1){

	Pattern<Event, ?> genericPattern = genericPatterns.get(0);
	PatternStream<Event> matchStream = CEP.pattern(inputStream, genericPattern);

	  // Outputselection on matchstream1 to obtain outputstream1
       	outputStream = matchStream.select(new PatternSelectFunction<Event, ComplexEvent>() {
            final boolean IS_OUTPUT_SELECTION_QUERY = config.processing.output_selection.size() < config.processing.query_length;
            @Override
            public ComplexEvent select(Map<String, List<Event>> match) throws Exception {
                Set<String> addedEvents = new HashSet<>();
                ArrayList<SimpleEvent> newEventList = new ArrayList<>();

                for (Event evt : match.get("first")) {
                	for (SimpleEvent contained: evt.getContainedSimpleEvents()) {
	                    if (config.processing.output_selection.contains(contained.getEventType())) {
                            boolean it_was_new = addedEvents.add(contained.getID()); // 'add new id to event in case of outputselection'
                            if (it_was_new)
                                newEventList.add(contained);
                    }}
                }

                if (IS_OUTPUT_SELECTION_QUERY)
                    for (SimpleEvent e : newEventList){
                        String new_event_id = UUID.randomUUID().toString().substring(0,8);
                        e.setEventID(new_event_id);
                }
                long creation_time = (LocalTime.now().toNanoOfDay() / 1000L); //FIXME: What if it is almost midnight? Then the monsters come out!
                ComplexEvent new_complex_event = new ComplexEvent(creation_time, config.processing.query_name, newEventList);
                System.out.println(new_complex_event);
                return new_complex_event;
            }

        });

	}
	else{// Case for binary Pattern

	// get both sequence patterns
	Pattern<Event, ?> genericPattern1 = genericPatterns.get(0);
	Pattern<Event, ?> genericPattern2 = genericPatterns.get(1);

        // apply patterns to Datastream
        PatternStream<Event> matchStream1 = CEP.pattern(inputStream, genericPattern1);
	PatternStream<Event> matchStream2 = CEP.pattern(inputStream, genericPattern2);



        // Outputselection on matchstream1 to obtain outputstream1
        DataStream<ComplexEvent> outputStream1 = matchStream1.select(new PatternSelectFunction<Event, ComplexEvent>() {
            final boolean IS_OUTPUT_SELECTION_QUERY = config.processing.output_selection.size() < config.processing.query_length;
            @Override
            public ComplexEvent select(Map<String, List<Event>> match) throws Exception {
                Set<String> addedEvents = new HashSet<>();
                ArrayList<SimpleEvent> newEventList = new ArrayList<>();

                for (Event evt : match.get("first_1")) {
                	for (SimpleEvent contained: evt.getContainedSimpleEvents()) {
	                    if (config.processing.output_selection.contains(contained.getEventType())) {
                            boolean it_was_new = addedEvents.add(contained.getID()); // 'add new id to event in case of outputselection'
                            if (it_was_new)
                                newEventList.add(contained);
                    }}
                }

		for (Event evt : match.get("second_1")) {
                	for (SimpleEvent contained: evt.getContainedSimpleEvents()) {
	                    if (config.processing.output_selection.contains(contained.getEventType())) {
                            boolean it_was_new = addedEvents.add(contained.getID()); // 'add new id to event in case of outputselection'
                            if (it_was_new)
                                newEventList.add(contained);
                    }}
                }
                if (IS_OUTPUT_SELECTION_QUERY)
                    for (SimpleEvent e : newEventList){
                        String new_event_id = UUID.randomUUID().toString().substring(0,8);
                        e.setEventID(new_event_id);
                }
                long creation_time = (LocalTime.now().toNanoOfDay() / 1000L); //FIXME: What if it is almost midnight? Then the monsters come out!
                ComplexEvent new_complex_event = new ComplexEvent(creation_time, config.processing.query_name, newEventList);
                System.out.println(new_complex_event);
                return new_complex_event;
            }

        });


	// Outputselection on matchstream2 to obtain outputstream2
        DataStream<ComplexEvent> outputStream2 = matchStream2.select(new PatternSelectFunction<Event, ComplexEvent>() {
            final boolean IS_OUTPUT_SELECTION_QUERY = config.processing.output_selection.size() < config.processing.query_length;
            @Override
            public ComplexEvent select(Map<String, List<Event>> match) throws Exception {
                Set<String> addedEvents = new HashSet<>();
                ArrayList<SimpleEvent> newEventList = new ArrayList<>();

                for (Event evt : match.get("first_2")) {
                	for (SimpleEvent contained: evt.getContainedSimpleEvents()) {
	                    if (config.processing.output_selection.contains(contained.getEventType())) {
                            boolean it_was_new = addedEvents.add(contained.getID()); // 'add new id to event in case of outputselection'
                            if (it_was_new)
                                newEventList.add(contained);
                    }}
                }
		for (Event evt : match.get("second_2")) {
                	for (SimpleEvent contained: evt.getContainedSimpleEvents()) {
	                    if (config.processing.output_selection.contains(contained.getEventType())) {
                            boolean it_was_new = addedEvents.add(contained.getID()); // 'add new id to event in case of outputselection'
                            if (it_was_new)
                                newEventList.add(contained);
                    }}
                }
                if (IS_OUTPUT_SELECTION_QUERY)
                    for (SimpleEvent e : newEventList){
                        String new_event_id = UUID.randomUUID().toString().substring(0,8);
                        e.setEventID(new_event_id);
                }
                long creation_time = (LocalTime.now().toNanoOfDay() / 1000L); //FIXME: What if it is almost midnight? Then the monsters come out!
                ComplexEvent new_complex_event = new ComplexEvent(creation_time, config.processing.query_name, newEventList);
                System.out.println(new_complex_event);
                return new_complex_event;
            }

        });

	// merge outputstreams/matchstreams 
	outputStream = outputStream1.union(outputStream2);}






        DataStream<ComplexEvent> filteredOutputStream = outputStream.filter(new FilterFunction<ComplexEvent>() {
            private final int match_counter = 0;
            private final int remove_expired_keys_check_threshold = 100;
            //private transient MetricsRecorder.MetricsWriter detection_latency_writer = new MetricsRecorder.MetricsWriter("detection_latency_node_" + config.forwarding.node_id + ".csv");
            @Override
            public boolean filter(ComplexEvent complex_event) throws Exception {
                System.out.println("LATENCYYYYYYYYYYYYYYYYYYYY " + (long)complex_event.getLatencyMs());
		        return true;

            }
        });

        //filteredOutputStream.addSink(new LatencyPrintSink());
        
        //add Sink method decides what to do with the resulting event stream

        filteredOutputStream.addSink((SinkFunction<ComplexEvent>)(Object) new TCPEventSender(config.forwarding.send_mode.equals("broadcast") ? EventSender.Mode.BROADCAST : EventSender.Mode.ROUND_ROBIN,
                config.forwarding.recipient, config.forwarding.address_book)); //The cast expresses the fact that a TCPEventSender is a SinkFunction<? extends Event>, not just a SInkFunction<Event>. I can't specify it in java though.

        // Start cluster/CEP-engine
        env.execute("Flink Java API Skeleton");
    }
}
