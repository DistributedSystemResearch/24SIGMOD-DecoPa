package com.huberlin;

import com.huberlin.config.QueryInformation;
import com.huberlin.event.Event;
import com.huberlin.event.SimpleEvent;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.windowing.time.Time;


import java.util.*;
import java.util.function.BiPredicate;


public class PatternFactory_kleene_binary extends AbstractPatternFactory{

    static public ArrayList<Pattern<Event, ?>> create(QueryInformation query_information, BiPredicate<SimpleEvent, SimpleEvent> predicate) {
        ArrayList<Pattern<Event, ?>> pattern = new ArrayList<>(2);
        final long TIME_WINDOW_SIZE_US = query_information.processing.time_window_size * 1_000_000;

        Pattern<Event, ?> kleene_pattern = Pattern.<Event>begin("first_1").where(new SimpleCondition<Event>() {
            @Override
            public boolean filter(Event first) {
                //kleene type
                return first.getEventType().equals(query_information.processing.input_1);
            }
        }).where(new IterativeCondition<Event>() {
            @Override
            public boolean filter(Event new_event, Context<Event> ctx) throws Exception {
                for (Event old_event : ctx.getEventsForPattern("first_1")) { // predicate over kleene
                    //ck: both old_event and new_event are of type input_1

                    for (List<String> predicate_constraint : query_information.processing.predicate_constraints) {

                        String first_eventtype = predicate_constraint.get(0);
                        String second_eventtype = predicate_constraint.get(0);

                        SimpleEvent e1 = old_event.getEventOfType(first_eventtype);
                        SimpleEvent e2 = new_event.getEventOfType(second_eventtype);


                        SimpleEvent earlier_primitive_event = null;
                        SimpleEvent later_primitive_event = null;

                        if (e1.timestamp <= e2.timestamp) {
                            earlier_primitive_event = e1;
                            later_primitive_event = e2;
                        }
                        else {
                            earlier_primitive_event = e2;
                            later_primitive_event = e1;
                        }

                        // Check attributes based on attribute indices (0 for mem_request, 1 for priority, 2 for cpu_request)
                        if (!predicate.test(earlier_primitive_event, later_primitive_event)) return false;

                    }
                    ////////////////////////////////////////////////


                    for (String id_constraint : query_information.processing.id_constraints) {
                        if (!old_event.getEventIdOf(id_constraint).equals(new_event.getEventIdOf(id_constraint)))
                            return false;
                    }
                    //return true //removed (ck) (fixme?)
                }
                return true;
            }
        }).oneOrMore();

        //ck: I think kleene1 detects patterns of the form OP(KL(input_1) input_2)
        Pattern<Event, ?> kleene1 = kleene_pattern.allowCombinations().followedByAny("second_1").where(new SimpleCondition<Event>() {
            @Override
            public boolean filter(Event first) throws Exception {
                //non-kleene input
                return first.getEventType().equals(query_information.processing.input_2);
            }
        }).where(new IterativeCondition<Event>() {
            @Override
            public boolean filter(Event new_event, Context<Event> ctx) throws Exception {
                Iterable<Event> events = ctx.getEventsForPattern("first_1");
                Event old_event = null;
                for (Event e : events) {
                    old_event = e;          //old_event is of type "first_1" i.e. one or more elements of type input_1
                }
                System.out.println("Comparing " + new_event + " and " + old_event);
                if (Math.abs(old_event.getHighestTimestamp() - new_event.getLowestTimestamp()) > TIME_WINDOW_SIZE_US || Math.abs(new_event.getHighestTimestamp() - old_event.getLowestTimestamp()) > TIME_WINDOW_SIZE_US)
                    return false;

                for (List<String> predicate_constraint : query_information.processing.predicate_constraints) {

                    String first_eventtype = predicate_constraint.get(0); // from input1
                    String second_eventtype = predicate_constraint.get(1); // from input2

                    SimpleEvent e1 = old_event.getEventOfType(first_eventtype);
                    SimpleEvent e2 = new_event.getEventOfType(second_eventtype);


                    SimpleEvent earlier_primitive_event = null;
                    SimpleEvent later_primitive_event = null;

                    if (e1.timestamp <= e2.timestamp) {
                        earlier_primitive_event = e1;
                        later_primitive_event = e2;
                    }
                    else {
                        earlier_primitive_event = e2;
                        later_primitive_event = e1;
                    }

                    if (!predicate.test(earlier_primitive_event, later_primitive_event)) return false;

                }


                for (List<String> sequence_constraint : query_information.processing.sequence_constraints) {
                    String first_eventtype = sequence_constraint.get(0);
                    String second_eventtype = sequence_constraint.get(1);

                    // Sequence constraint check (for both directions)
                    if (old_event.getTimestampOf(first_eventtype) != null && new_event.getTimestampOf(second_eventtype) != null && old_event.getTimestampOf(first_eventtype) >= new_event.getTimestampOf(second_eventtype)) {
                        return false;
                    }

                    if (new_event.getTimestampOf(first_eventtype) != null && old_event.getTimestampOf(second_eventtype) != null && new_event.getTimestampOf(first_eventtype) >= old_event.getTimestampOf(second_eventtype)) {
                        return false;
                    }
                }

                return true;
            }
        }).within(Time.milliseconds(60000000));

        //ck: Pattern OP(input_2,KL(input_1)) //ck why is the logic for this one different?
        Pattern<Event, ?> kleene2 = Pattern.<Event>begin("first_2").where(new SimpleCondition<Event>() {
            @Override
            public boolean filter(Event first) {
                //non-kleene input
                return first.getEventType().equals(query_information.processing.input_2);
            }
        }).followedByAny("second_2").where(new SimpleCondition<Event>() {
            @Override
            public boolean filter(Event first) {
                //kleene type
                return first.getEventType().equals(query_information.processing.input_1); //new_event is of type input_1
            }
        }).where(new IterativeCondition<Event>() {
            @Override
            public boolean filter(Event new_event, Context<Event> ctx) throws Exception {
                for (Event old_event : ctx.getEventsForPattern("first_2")) { //old_event is of type input_2
                    for (String id_constraint : query_information.processing.id_constraints) {
                        if (!old_event.getEventIdOf(id_constraint).equals(new_event.getEventIdOf(id_constraint)))
                            return false;
                    }

                    if (! fulfils_predicate_constraints(query_information.processing.predicate_constraints, new_event, old_event, predicate, false))
                        return false;

                }//TODO add predicates over first_1 (non-kleene input)
                return true;

            }
        }).oneOrMore().allowCombinations().within(Time.milliseconds(60000000));


        pattern.add(kleene1);
        pattern.add(kleene2);

        return pattern;
    }
}
