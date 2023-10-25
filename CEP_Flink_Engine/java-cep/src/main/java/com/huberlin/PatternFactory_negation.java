package com.huberlin;

import com.huberlin.config.QueryInformation;
import com.huberlin.event.Event;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Predicate;


public class PatternFactory_negation {

    static public ArrayList<Pattern<Event, ?>> create(QueryInformation query_information) {

        ArrayList<Pattern<Event, ?>> pattern = new ArrayList<>(2);
        final long TIME_WINDOW_SIZE_US = query_information.processing.time_window_size * 1_000_000;


        Pattern<Event, ?> negated_pattern = Pattern.<Event>begin("negated")
                .where(new IterativeCondition<Event>() {
                    @Override
                    public boolean filter(Event negated, Context<Event> ctx) throws Exception {
                        //negated Type
                        return negated.getEventType().equals(query_information.processing.input_1);
                    }
                }).oneOrMore().optional().greedy()
                .followedByAny("first").where(new IterativeCondition<Event>() {
                    @Override
                    public boolean filter(Event new_event, Context<Event> ctx) throws Exception {

                        if (new_event.getEventType().equals(query_information.processing.input_2)) { //complex event with context	  //use rich pattern function to get nfa states?

                            for (Event e : ctx.getEventsForPattern("negated")) {            // getPartialMatches()
                                if (e.getTimestamp() > new_event.getTimestampOf(query_information.processing.context.get(0)) && e.getTimestamp() < new_event.getTimestampOf(query_information.processing.context.get(1))) {
                                    return false;
                                }
                            }
                            return true;
                        } else
                            return false;
                    }
                }).within(Time.milliseconds(60000000));

        pattern.add(negated_pattern);
        pattern.add(negated_pattern);

        return pattern;
    }

    ;
}
