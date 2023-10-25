package com.huberlin;

import com.huberlin.config.QueryInformation;
import com.huberlin.event.Event;
import com.huberlin.event.SimpleEvent;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;
import java.util.function.Predicate;


public class PatternFactory extends AbstractPatternFactory {
    static private final Logger log = LoggerFactory.getLogger(PatternFactory.class);
    static private AtomicInteger timestamp_counter = new AtomicInteger(0);

    static public ArrayList<Pattern<Event, ?>> create(QueryInformation query_information, BiPredicate<SimpleEvent, SimpleEvent> predicate) {

        ThroughputLogger counter_logger = new ThroughputLogger(timestamp_counter, "throughput_node_" + query_information.forwarding.node_id + ".csv");
        counter_logger.setDaemon(true); // Make sure the thread doesn't prevent the JVM from exiting
        counter_logger.start();


        ArrayList<Pattern<Event, ?>> pattern = new ArrayList<>(2);
        final long TIME_WINDOW_SIZE_US = query_information.processing.time_window_size * 1_000_000;

        Pattern<Event, ?> seq1 = Pattern.<Event>begin("first_1")
                .where(new SimpleCondition<Event>() {
                    String latest_eventID = "";

                    @Override
                    public boolean filter(Event first) throws Exception {

                        String simp_eventID = first.getID();
                        if (!simp_eventID.equals(latest_eventID)) {
                            latest_eventID = simp_eventID;
                            timestamp_counter.incrementAndGet();
                        }
                        return first.getEventType().equals(query_information.processing.input_1);
                    }
                }).followedByAny("second_1").where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event first) {
                        return first.getEventType().equals(query_information.processing.input_2);
                    }
                }).where(new IterativeCondition<Event>() {
                    @Override
                    public boolean filter(Event new_event, Context<Event> ctx) throws Exception {


                        if (new_event.getEventType().equals(query_information.processing.input_2)) {  //new_event is of type input_2

                            // new_event is second
                            Iterable<Event> events = ctx.getEventsForPattern("first_1");
                            Event old_event = null;
                            for (Event e : events) {
                                old_event = e; //old_event (i.e. partial match) is of type input_1
                            }

                            LocalDateTime now = LocalDateTime.now();
                            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
                            System.out.println("Comparing " + old_event.getID() + " " + new_event.getID() + " " + now.format(formatter));


                            // TIMEWINDOW
                            if (Math.abs(old_event.getHighestTimestamp() - new_event.getLowestTimestamp()) > TIME_WINDOW_SIZE_US ||
                                    Math.abs(new_event.getHighestTimestamp() - old_event.getLowestTimestamp()) > TIME_WINDOW_SIZE_US)
                                return false;


                            // SELECTIVTIES// ADD REAL PREDICATE CHECKS HERE:
                            if (! fulfils_predicate_constraints(query_information.processing.predicate_constraints, old_event, new_event, predicate, true))
                                return false;

                            // REAL WORLD

                            for (String id_constraint : query_information.processing.id_constraints) {
                                if (!old_event.getEventIdOf(id_constraint).equals(new_event.getEventIdOf(id_constraint)))
                                    return false;
                            }

                            // SEQUENCE CONSTRAINTS
                            for (List<String> sequence_constraint : query_information.processing.sequence_constraints) {
                                String first_eventtype = sequence_constraint.get(0);
                                String second_eventtype = sequence_constraint.get(1);

                                // Sequence constraint check (for both directions)
                                if (old_event.getTimestampOf(first_eventtype) != null &&
                                        new_event.getTimestampOf(second_eventtype) != null &&
                                        old_event.getTimestampOf(first_eventtype) >= new_event.getTimestampOf(second_eventtype)) {
                                    return false;
                                }

                                if (new_event.getTimestampOf(first_eventtype) != null &&
                                        old_event.getTimestampOf(second_eventtype) != null &&
                                        new_event.getTimestampOf(first_eventtype) >= old_event.getTimestampOf(second_eventtype)) {
                                    return false;
                                }
                            }

                            return true;
                        } else
                            //return false;}}).within(Time.milliseconds(1000000*query_information.processing.time_window_size));
                            return false;
                    }
                }).within(Time.milliseconds(1000000 * query_information.processing.time_window_size));


        Pattern<Event, ?> seq2 = Pattern.<Event>begin("first_2")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event first) throws Exception {
                        //B
                        return first.getEventType().equals(query_information.processing.input_2);
                    }
                }).followedByAny("second_2").where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event first) throws Exception {
                        //A
                        return first.getEventType().equals(query_information.processing.input_1);
                    }
                }).where(new IterativeCondition<Event>() {


                    @Override
                    public boolean filter(Event new_event, Context<Event> ctx) throws Exception {

                        if (new_event.getEventType().equals(query_information.processing.input_1)) {
                            // new_event is of type input_1
                            Iterable<Event> events = ctx.getEventsForPattern("first_2");
                            Event old_event = null;
                            for (Event e : events) {
                                old_event = e;  //type input_2

                            }
                            LocalDateTime now = LocalDateTime.now();
                            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
                            System.out.println("Comparing " + old_event.getID() + " " + new_event.getID() + " " + now.format(formatter));

                            if (Math.abs(old_event.getHighestTimestamp() - new_event.getLowestTimestamp()) > TIME_WINDOW_SIZE_US ||
                                    Math.abs(new_event.getHighestTimestamp() - old_event.getLowestTimestamp()) > TIME_WINDOW_SIZE_US)
                                return false;

                            // SELECTIVTIES// ADD REAL WORLD PREDICATE CHECKS HERE
                            if (! fulfils_predicate_constraints(query_information.processing.predicate_constraints, new_event, old_event, predicate, false)) //new_event of type input_1, old_event of type input_2
                                return false; //why are predicate checks not last, e.g. after sequence constraint checks?
                            ////////////////////////////////////////////////

                            for (String id_constraint : query_information.processing.id_constraints) {
                                if (!old_event.getEventIdOf(id_constraint).equals(new_event.getEventIdOf(id_constraint)))
                                    return false;
                            }

                            for (List<String> sequence_constraint : query_information.processing.sequence_constraints) {
                                String first_eventtype = sequence_constraint.get(0); //primitive type from event of type input_1
                                String second_eventtype = sequence_constraint.get(1);//primitive type from event of type input_2

                                // Sequence constraint check (for both directions)
                                if (old_event.getTimestampOf(first_eventtype) != null &&
                                        new_event.getTimestampOf(second_eventtype) != null &&
                                        old_event.getTimestampOf(first_eventtype) >= new_event.getTimestampOf(second_eventtype)) {
                                    return false;
                                }

                                if (new_event.getTimestampOf(first_eventtype) != null &&
                                        old_event.getTimestampOf(second_eventtype) != null &&
                                        new_event.getTimestampOf(first_eventtype) >= old_event.getTimestampOf(second_eventtype)) {
                                    return false;
                                }
                            }

                            return true;
                        } else
                            //return false;}}).within(Time.milliseconds(1000000*query_information.processing.time_window_size));
                            return false;
                    }
                }).within(Time.milliseconds(1000000 * query_information.processing.time_window_size));


        pattern.add(seq1);
        pattern.add(seq2);

        return pattern;
    }
}
