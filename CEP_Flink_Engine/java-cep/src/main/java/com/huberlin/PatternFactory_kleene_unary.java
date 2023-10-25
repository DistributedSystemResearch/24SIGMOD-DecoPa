package com.huberlin;

import com.huberlin.config.QueryInformation;
import com.huberlin.event.Event;
import com.huberlin.event.SimpleEvent;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.windowing.time.Time;


import java.util.*;
import java.util.function.BiPredicate;


public class PatternFactory_kleene_unary extends AbstractPatternFactory {

    static public ArrayList<Pattern<Event, ?>> create(QueryInformation query_information, BiPredicate<SimpleEvent, SimpleEvent> predicate) {

        ArrayList<Pattern<Event, ?>> pattern = new ArrayList<>(2);
        Pattern<Event, ?> kleene_pattern = Pattern.<Event>begin("first")
                .where(new IterativeCondition<Event>() {
                    @Override
                    public boolean filter(Event new_event, Context<Event> ctx) throws Exception {
                        new Random();
                        if (new_event.getEventType().equals(query_information.processing.input_1)) { //kleene type
                            for (Event old_event : ctx.getEventsForPattern("first")) { // add selectivity between kleene type

                                for (String id_constraint : query_information.processing.id_constraints) {
                                    if (!old_event.getEventIdOf(id_constraint).equals(new_event.getEventIdOf(id_constraint)))
                                        return false;
                                }

                                for (List<String> predicate_constraint : query_information.processing.predicate_constraints) {

                                    String first_eventtype = predicate_constraint.get(0); // from input1
                                    String second_eventtype = predicate_constraint.get(1); // from input2 //ck: I am pretty sure this is constrained to always be input_1
                                    //assert(first_eventtype.equals(second_eventtype)); //ck

                                    SimpleEvent e1 = old_event.getEventOfType(first_eventtype); // Use first_eventtype
                                    SimpleEvent e2 = new_event.getEventOfType(second_eventtype); // Use second_eventtype

                                    SimpleEvent earlier_event_at = null;
                                    SimpleEvent later_event_at = null;

                                    if (e1.timestamp <= e2.timestamp) {
                                        earlier_event_at = e1;
                                        later_event_at = e2;
                                    }
                                    else  {
                                        earlier_event_at = e2;
                                        later_event_at = e1;
                                    }

                                    if (!predicate.test(earlier_event_at, later_event_at))
                                        return false;
                                }
                            }
                            return true;
                        } else
                            return false;
                    }
                }).oneOrMore().optional().allowCombinations().within(Time.milliseconds(60000000));

        pattern.add(kleene_pattern); //ck: we're adding it twice? why?
        pattern.add(kleene_pattern);

        return pattern;
    }
}
