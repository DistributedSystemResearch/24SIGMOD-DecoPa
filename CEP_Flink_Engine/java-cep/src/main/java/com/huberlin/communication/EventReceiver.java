package com.huberlin.communication;

import com.huberlin.event.Event;

public interface EventReceiver {
    class EventWithWatermark {
        public final Event payload;
        public final long watermarkTimestamp;

        public EventWithWatermark(Event event, long timestamp) {
            this.payload = event;
            this.watermarkTimestamp = timestamp;
        }
    }
}
