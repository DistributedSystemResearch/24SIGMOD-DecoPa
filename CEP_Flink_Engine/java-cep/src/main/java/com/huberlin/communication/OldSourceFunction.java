package com.huberlin.communication;

import com.huberlin.event.Event;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
public class OldSourceFunction extends RichSourceFunction<Event> {
    private static final Logger log = LoggerFactory.getLogger(OldSourceFunction.class);
    private volatile boolean isCancelled = false;
    private BlockingQueue<Event> merged_event_stream;
    private final int connections_to_establish;
    private final int port;
    private static volatile long current_global_watermark = 0;
    private static volatile long watermarkCounter = 0;	

    public OldSourceFunction(int listen_port, int n_connections_to_establish ){
        super();
        this.port = listen_port;
        this.connections_to_establish = n_connections_to_establish;
    }

    private static final Map<String, Long> ip_to_newest_watermarking_timestamp = Collections.synchronizedMap(new HashMap<>());

    @Override
    public void run(SourceFunction.SourceContext<Event> sourceContext) throws Exception {
        while (!isCancelled) {
            //retrieve and remove the head of the queue (event stream)
            Event event = merged_event_stream.take();

            //process it with Flink
            sourceContext.collectWithTimestamp(event, current_global_watermark+1);
            updateWatermark(event, sourceContext);

        }
    }
    @Override
    public void cancel() {
        isCancelled = true;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        merged_event_stream = new LinkedBlockingQueue<>();

        Thread new_connection_accepting_thread = new Thread(() -> {
            try (ServerSocket accepting_socket = new ServerSocket()) {
                accepting_socket.bind(new InetSocketAddress(port));
                while (!isCancelled) {
                    try {
                        Socket new_client = accepting_socket.accept();
                        new_client.shutdownOutput(); //one-way data connection (only read)
                        new Thread(() -> handleClient(new_client)).start();
                        log.info("New client connected: " + new_client);
                    }
                    catch (IOException e) {
                        log.warn("Exception in connection-accepting thread: " +  e);
                    }
                }
            } catch (IOException e) {
                    log.error("Failed to open server socket: " + e);
                    e.printStackTrace(System.err);
                    System.exit(1);
            }
        });
        new_connection_accepting_thread.start();
    }
    public void handleClient(Socket socket) {
        try {
            BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String client_address = socket.getRemoteSocketAddress().toString();
            while (!isCancelled) {
                String message = input.readLine();

                //check, if a client has sent his entire event stream..
                if (message == null || message.contains("end-of-the-stream")) {
                    //set the watermark to the highest possible value to be not considered anymore
                    log.info("Reached the end of the stream for " + client_address + "->" + ip_to_newest_watermarking_timestamp.get(client_address));
                    ip_to_newest_watermarking_timestamp.put(client_address, Long.MAX_VALUE);
                    if (message == null)
                        log.warn("Stream terminated without end-of-the-stream marker.");
                    input.close();
                    socket.close();
                    return;
                }
                //.. else, simply process the incoming event accordingly
                else {
                    if (!message.contains("|"))
                        continue;

                    Event event = Event.parse(message);

                    //update watermark for a sending node
                    ip_to_newest_watermarking_timestamp.put(client_address, event.getTimestamp());
                    log.debug("Updated watermark for source " + client_address + " to " + event.getTimestamp());
                    merged_event_stream.put(event);
                    LocalTime currTime = LocalTime.now();
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss:SSSSSS");
                    System.out.println(event + " was received at: " + currTime.format(formatter));
                }
            }
        } catch (IOException | InterruptedException e) {
            log.info("Client disconnected");
            try {
                socket.close();
            } catch (IOException ignored) {}
        }
    }

    public void updateWatermark(Event event, SourceContext<Event> sourceContext) {
        //current_global_watermark =  event.getTimestamp();
	watermarkCounter += 1; 
	current_global_watermark =  (LocalTime.now().toNanoOfDay() / 1000L) +  watermarkCounter; 	     
	sourceContext.emitWatermark(new Watermark(current_global_watermark));
       /*long oldestTimestamp = Long.MAX_VALUE;
        String client_address = "";

        //if not every node has sent an event yet, wait for it to update the watermark
        if (connections_to_establish != ip_to_newest_watermarking_timestamp.size())
            return;

        
        // why is the event not used to upate the watermark?
        //Compute the minimum of the individual predecessor node's newest watermarks
        for (String client_address_watermark_key : ip_to_newest_watermarking_timestamp.keySet()) {
            if (ip_to_newest_watermarking_timestamp.get(client_address_watermark_key) < oldestTimestamp) {
                oldestTimestamp = ip_to_newest_watermarking_timestamp.get(client_address_watermark_key);
                client_address = client_address_watermark_key;
            }
        }


        if (oldestTimestamp < Long.MAX_VALUE) {
            //update watermark
            //sourceContext.emitWatermark(new Watermark(oldestTimestamp - 1));
	    sourceContext.emitWatermark(new Watermark(oldestTimestamp));
            current_global_watermark = oldestTimestamp;
            System.out.println("Global Watermark: "+ (oldestTimestamp - 1) + " after event " + event);
        }*/
    }

    public static long getCurrentGlobalWatermark() {
        return current_global_watermark;
    }

}
