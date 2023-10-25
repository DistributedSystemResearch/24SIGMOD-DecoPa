package com.huberlin.config;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class QueryInformation implements Serializable {
   public Forwarding forwarding;
   public Processing processing;

   public static class Forwarding implements Serializable {
       public String send_mode;
       public ArrayList<Integer> recipient;
       public int node_id;
       public ArrayList<Integer> connections_to_establish;
       public final HashMap<Integer, String> address_book = new HashMap<>();

   }

   public static class Processing implements Serializable{
       public String query_name;
       public long query_length;
       public List<String> output_selection;
       public String input_1;
       public String input_2;
       public double selectivity;
       public List<List<String>> sequence_constraints;
       public List<List<String>> predicate_constraints;
       public List<String> id_constraints;
       public long time_window_size;
       public long predicate_checks;
       public int is_negated;
       public List<String> context; 
       public int kleene_type;	
	 
   }
}
