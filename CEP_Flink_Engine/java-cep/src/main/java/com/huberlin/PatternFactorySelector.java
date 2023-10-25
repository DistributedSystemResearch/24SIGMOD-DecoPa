package com.huberlin;

import java.util.*;
import java.util.function.BiPredicate;

import com.huberlin.event.SimpleEvent;
import org.apache.flink.cep.pattern.Pattern;
import com.huberlin.config.QueryInformation;
import com.huberlin.event.Event;


public abstract class PatternFactorySelector {
    public static ArrayList<Pattern<Event, ?>> create(QueryInformation query_information) {
        final BiPredicate<SimpleEvent, SimpleEvent> PREDICATE = Predicates.OLD_PREDICATE; //configure it here, or read from query_information
        boolean is_negated = (query_information.processing.is_negated != 0);
        if (query_information.processing.kleene_type == 0) {
            if (is_negated) {
                return PatternFactory_negation.create(query_information);
            } else {
                return PatternFactory.create(query_information, PREDICATE);
            }
        } else if (query_information.processing.kleene_type == 1) { // kleene_unary
            return PatternFactory_kleene_unary.create(query_information, PREDICATE);
        } else
            return PatternFactory_kleene_binary.create(query_information, PREDICATE); // kleene binary
    }

}
