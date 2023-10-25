package com.huberlin.communication;

import com.huberlin.event.Event;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public interface EventSender extends SinkFunction<Event> {
    Mode getSenderMode();

    enum Mode {
        BROADCAST,
        ROUND_ROBIN
    }
}
