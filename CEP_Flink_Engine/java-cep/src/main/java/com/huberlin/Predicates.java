package com.huberlin;

import com.huberlin.event.SimpleEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static com.huberlin.util.Functions.great_circle_distance;


public class Predicates {
    static private final Logger log = LoggerFactory.getLogger(Predicates.class);
    static final SerializableBiPredicate<SimpleEvent, SimpleEvent> OLD_PREDICATE = (earlier_event_at, later_event_at) -> {
        // Check attributes based on attribute indices (0 for mem_request, 1 for priority, 2 for cpu_request)
        if (Float.parseFloat(earlier_event_at.attributeList.get(1)) <= Float.parseFloat(later_event_at.attributeList.get(1))) {
            return false;
        }
        if (Float.parseFloat(earlier_event_at.attributeList.get(7)) <= Float.parseFloat(later_event_at.attributeList.get(7))) {
            return false;
        }
        if (Float.parseFloat(earlier_event_at.attributeList.get(6)) <= Float.parseFloat(later_event_at.attributeList.get(6))) {
            return false;
        }
        return true;
    };

    static final SerializableBiPredicate<SimpleEvent, SimpleEvent> LONG_DISTANCE = (old_event_at, new_event_at) -> {
        final int THRESHOLD_KM = 6;
        double lat2 = Double.parseDouble(new_event_at.attributeList.get(4)); //start_station_latitude
        double lon2 = Double.parseDouble(new_event_at.attributeList.get(5)); //start_station_longitude
        double lat1 = Double.parseDouble(old_event_at.attributeList.get(7)); //end_station_latitude
        double lon1 = Double.parseDouble(old_event_at.attributeList.get(8)); //end_station_longitude
        return great_circle_distance(lat2, lon2, lat1, lon1) > THRESHOLD_KM;
    };

    static final SerializableBiPredicate<SimpleEvent, SimpleEvent> SYMMETRICAL_LONG_DISTANCE = (old_event_at, new_event_at) -> {
        final int THRESHOLD_KM = 6;
        double lat2 = Double.parseDouble(new_event_at.attributeList.get(4)); //start_station_latitude
        double lon2 = Double.parseDouble(new_event_at.attributeList.get(5)); //start_station_longitude
        double lat1 = Double.parseDouble(old_event_at.attributeList.get(4)); //start_station_latitude
        double lon1 = Double.parseDouble(old_event_at.attributeList.get(5)); //start_station_longitude
        log.info("distance: " + great_circle_distance(lat2, lon2, lat1, lon1));
        log.info("lat2 " + lat2);
        log.info("lon2 " + lon2);
        log.info("lat1 " + lat1);
        log.info("lon1 " + lon1);
        log.info("new_event attributes:" + new_event_at.attributeList.toString());
        log.info("new_event:" + new_event_at);

        log.info("old_event attributes:" + old_event_at.attributeList.toString());
        log.info("old_event:" + old_event_at);

        return great_circle_distance(lat2, lon2, lat1, lon1) > THRESHOLD_KM;
    };
}
