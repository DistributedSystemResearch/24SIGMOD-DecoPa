package com.huberlin;

import com.huberlin.config.QueryInformation;
import com.huberlin.event.Event;
import com.huberlin.event.SimpleEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.function.BiPredicate;

public abstract class AbstractPatternFactory {
    static boolean fulfils_predicate_constraints(List<List<String>> predicate_constraints,
                                                 Event input1_event,
                                                 Event input2_event,
                                                 BiPredicate<SimpleEvent, SimpleEvent> predicate,
                                                 boolean input1_event_is_old_event) throws Exception {
        // (same as sequence consraint checks) iterate over tuples in predicate constraints
        // for type in tuple, get respective event in primitive events of [old_event, new_event]
        // determine order of tuple of primitive events, set to old_event_at, new_event_at, then check predicate (e.g. <,>) at positions
        // unpack simple events in complex events, order by timestamp

        for (List<String> predicate_constraint : predicate_constraints){

            String input1_primitive_type = predicate_constraint.get(0);  // PE to select from event [of type? belonging to?] input1 //todo: If input1!=input2 always holds then you can say "of type"
            String input2_primitive_type = predicate_constraint.get(1);  // PE to select from event [of type? belonging to?] input2

            SimpleEvent input1_primitive_event = input1_event.getEventOfType(input1_primitive_type);
            SimpleEvent input2_primitive_event = input2_event.getEventOfType(input2_primitive_type);

            SimpleEvent earlier_primitive_event = null;
            SimpleEvent later_primitive_event = null;
            try {
                if (input1_primitive_event.timestamp < input2_primitive_event.timestamp) {
                    earlier_primitive_event = input1_primitive_event;
                    later_primitive_event = input2_primitive_event;
                }
                else if (input2_primitive_event.timestamp < input1_primitive_event.timestamp) {
                    earlier_primitive_event = input2_primitive_event;
                    later_primitive_event = input1_primitive_event;
                }
                else {
                    earlier_primitive_event = input1_event_is_old_event ? input1_primitive_event : input2_primitive_event;
                    later_primitive_event = input1_event_is_old_event ? input2_primitive_event : input1_primitive_event;
                }
            } catch (NullPointerException e) {
                if (input1_primitive_event == null)
                    throw new Exception("Method getEventOfType(" + input1_primitive_type + ") returned null for the input1_event " + input1_event + "(other event: " + input2_event + ")");
                else if (input2_primitive_event == null)
                    throw new Exception("Method getEventOfType(" + input2_primitive_type + ") returned null for the input2_event " + input2_event + "(other event: " + input1_event + ")");
                else
                    throw e;
            }

            if (!predicate.test(earlier_primitive_event, later_primitive_event))
                return false;
        }
        return true;
    }
}
