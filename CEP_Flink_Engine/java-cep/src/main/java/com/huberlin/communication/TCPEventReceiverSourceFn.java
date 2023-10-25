package com.huberlin.communication;

import com.huberlin.event.Event;
import com.huberlin.config.QueryInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.io.IOException;

public class TCPEventReceiverSourceFn extends RichSourceFunction<Event> {
    private volatile boolean isCancelled;
    private TCPEventReceiver receiver;
    private Thread receiver_thread; //TODO?: Remove thread by refactoring receiver into a SinkFunction

    private final QueryInformation config;

    TCPEventReceiverSourceFn(QueryInformation config){
        this.config = config;
    }

    @Override
    public void run(SourceContext<Event> sourceContext) {
        receiver_thread.start();
        while (!isCancelled) {
            //retrieve and remove the head of the queue (event stream)
            EventReceiver.EventWithWatermark tuple = receiver.dequeue();
            //process it with Flink
            sourceContext.collectWithTimestamp(tuple.payload, tuple.payload.getTimestamp());
            sourceContext.emitWatermark(new Watermark(tuple.watermarkTimestamp));
        }
    }

    @Override
    public void cancel() {
        receiver.cancel();
        isCancelled = true;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        assert config != null;
        receiver = new TCPEventReceiver(
                config.forwarding.connections_to_establish, config.forwarding.address_book,
                config.forwarding.node_id, Event::parse);
        receiver_thread = new Thread(() -> {
            try {
                receiver.run();
            } catch (IOException e) { //FIXME: What happens if this is thrown?
                throw new RuntimeException(e);
            }
        });
    }
}
