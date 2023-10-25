package com.huberlin.config;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.huberlin.event.Event;
import org.json.JSONArray;
import org.json.JSONObject;

public  class JSONQueryParser {
    public static QueryInformation parseJsonFile(String local_config, String global_config) throws IOException {
        try {
            String jsonString = new String(Files.readAllBytes(Paths.get(local_config))); //  local config (address book)
            JSONObject jsonObject = new JSONObject(jsonString);
            
            String jsonString2 = new String(Files.readAllBytes(Paths.get(global_config))); // global config (address book)
            JSONObject jsonObject2 = new JSONObject(jsonString2);


            QueryInformation query_information = new QueryInformation();

            // Parse forwarding object
            JSONObject forwardingObject = jsonObject.getJSONObject("forwarding");
            query_information.forwarding = new QueryInformation.Forwarding();
            
            
            query_information.forwarding.send_mode = forwardingObject.getString("send_mode");
            query_information.forwarding.node_id = forwardingObject.getInt("node_id");
            JSONArray recipient_json = ((JSONArray) forwardingObject.get("recipient"));
            query_information.forwarding.recipient = new ArrayList<>();
            for (Object l : recipient_json)
            	query_information.forwarding.recipient.add((int)l);


            JSONArray connections_json = ((JSONArray) forwardingObject.get("connections_to_establish"));
            query_information.forwarding.connections_to_establish = new ArrayList<>();
            for (Object l : connections_json)
            	query_information.forwarding.connections_to_establish.add((Integer)l);
            
            
            JSONObject address_book = jsonObject2;
            for (String nodeId_str : address_book.keySet()) {
                int nodeId = Integer.parseInt((nodeId_str).trim());
                String addr = (String) address_book.get(nodeId_str);
                query_information.forwarding.address_book.put(nodeId, addr);
            }

            
            System.out.println("Forwarding:");
            System.out.println("  Send mode: " + query_information.forwarding.send_mode);
            System.out.println("  Recipient: " + query_information.forwarding.recipient);
    

            // Parse processing object
            JSONObject processingObject = jsonObject.getJSONObject("processing");
            query_information.processing = new QueryInformation.Processing();
            query_information.processing.query_name = processingObject.getString("query_name");
            query_information.processing.query_length = processingObject.getInt("query_length");
            query_information.processing.output_selection = jsonArrayToList(processingObject.getJSONArray("output_selection"));
            query_information.processing.context = jsonArrayToList(processingObject.getJSONArray("context"));
            query_information.processing.is_negated = processingObject.getInt("is_negated");
	    query_information.processing.kleene_type = processingObject.getInt("kleene_type");
            query_information.processing.input_1 = processingObject.getString("input_1");
            query_information.processing.input_2 = processingObject.getString("input_2");
            query_information.processing.selectivity = processingObject.getDouble("selectivity");

            JSONArray sequenceConstraintsArray = processingObject.getJSONArray("sequence_constraints");
            query_information.processing.sequence_constraints = new ArrayList<>();
            for (int i = 0; i < sequenceConstraintsArray.length(); i++) {
                    query_information.processing.sequence_constraints.add(jsonArrayToList(sequenceConstraintsArray.getJSONArray(i)));
            }

	    JSONArray predicateConstraintsArray = processingObject.getJSONArray("predicate_constraints");
            query_information.processing.predicate_constraints = new ArrayList<>();
            for (int i = 0; i < predicateConstraintsArray.length(); i++) {
                    query_information.processing.predicate_constraints.add(jsonArrayToList(predicateConstraintsArray.getJSONArray(i)));
            }
            
            query_information.processing.id_constraints = jsonArrayToList(processingObject.getJSONArray("id_constraints"));
            query_information.processing.time_window_size = processingObject.getInt("time_window_size");
            query_information.processing.predicate_checks = processingObject.getInt("predicate_checks");
            System.out.println("Processing:");
            System.out.println("  Query name: " + query_information.processing.query_name);
            System.out.println("  Output selection: " + query_information.processing.output_selection);
            System.out.println("  Input 1: " + query_information.processing.input_1);
            System.out.println("  Input 2: " + query_information.processing.input_2);
            System.out.println("  Selectivity: " + query_information.processing.selectivity);
            System.out.println("  Sequence constraints: " + query_information.processing.sequence_constraints);
	    System.out.println("  Sequence constraints: " + query_information.processing.predicate_constraints);
            System.out.println("  ID constraints: " + query_information.processing.id_constraints);
            System.out.println("  Time window size: " + query_information.processing.time_window_size);
            System.out.println("  Predicate checks: " + query_information.processing.predicate_checks);
	    System.out.println("  Context: " + query_information.processing.context);
	    System.out.println("  Negated: " + query_information.processing.is_negated);
	    System.out.println("  Kleene Type: " + query_information.processing.kleene_type);
            /*if (Event.getPrimitiveTypes(query_information.processing.query_name).size() != query_information.processing.query_length)
                throw new IllegalArgumentException("Query given as " + query_information.processing.query_name
                        + " does not match query_length, given as " + query_information.processing.query_length);*/

            return query_information;
        } catch (IOException e) {
            System.err.println("Error reading JSON file: " + e.getMessage());
            throw e;
        }
    }

    private static List<String> jsonArrayToList(JSONArray jsonArray) {
        return jsonArray.toList().stream().map(Object::toString).collect(Collectors.toList());
    }
}

