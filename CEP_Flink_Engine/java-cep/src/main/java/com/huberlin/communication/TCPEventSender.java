package com.huberlin.communication;

import com.huberlin.event.Event;
import com.huberlin.communication.addresses.TCPAddressString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.net.Socket;
import java.util.*;

public class TCPEventSender implements EventSender {
    static private final Logger log = LoggerFactory.getLogger(TCPEventSender.class);
    private final Mode mode;
    private final List<TCPAddressString> destinations; //ip:port of each node that outputs are to be forwarded to
    private int next_dest_index = 0; //only valid if there are any destinations
    private final Map<TCPAddressString, PrintWriter> connections = new HashMap<>();
    final private HashSet<Event> event_ids_sent = new HashSet<>();

    public TCPEventSender(Mode mode, List<Integer> destinations, Map<Integer, String> address_book) {
        this.mode = mode;
        this.destinations = new ArrayList<>();
        for (Integer destination : destinations){
            String myAddr = address_book.get(destination);
            if (myAddr == null)
                throw new IllegalArgumentException("The address book does not have an entry for the destination node ID " + destination);
            this.destinations.add(new TCPAddressString(myAddr));
        }
    }
    public TCPEventSender(Mode mode, List<TCPAddressString> destinations) {
        this.mode = mode;
        this.destinations = destinations;
    }

    @Override
    public Mode getSenderMode() {
        return mode;
    }

    /**
     * Called by flink to send events
     */
    @Override
    public void invoke(Event event){
        if (mode == Mode.ROUND_ROBIN) {
            if (destinations.size() != 0) {
                TCPAddressString destination = destinations.get(next_dest_index);
                next_dest_index = (next_dest_index + 1) % destinations.size();
                send_to(event, destination);
            }
        }
        else if (mode == Mode.BROADCAST){
            for (TCPAddressString destination : destinations){
                send_to(event, destination);
            }
        }
    }

    @Override
    public void invoke(Event event, Context ignored){
        invoke(event);
    }

    private void send_to(Event event, TCPAddressString target_ip_port) {
        try {
            //if the connection to a forwarding target was not established yet then establish it
            if (!connections.containsKey(target_ip_port)) {
                try {
                    String host = target_ip_port.getHost();
                    int port = target_ip_port.getPort();

                    Socket client_socket = new Socket(host, port);
                    client_socket.setTcpNoDelay(true);
                    client_socket.setKeepAlive(true);
                    PrintWriter writer = new PrintWriter(client_socket.getOutputStream(), true);
                    connections.put(target_ip_port, writer);
                    log.info("Connection for forwarding events to " + target_ip_port + " established");
                }
                catch (Exception e){
                    log.error("Failure to establish connection to " + target_ip_port + " for forwarding events. Error: " + e);
                    e.printStackTrace(System.err);
                    System.exit(1);
                }
            }
            connections.get(target_ip_port).println(event.toString());
        } catch (Exception e) {
            log.warn("Forwarding Error: " + e + " - Event:" + event + " to " + target_ip_port);
            e.printStackTrace(System.err);
        }
    }

    @Override
    public void finish() {
        for (PrintWriter conn : connections.values()) {
            conn.flush();
        }
    }
    /*
     * Used by flink to propagate watermarks, but only if flink considers it an 'advanced sink'... //TODO: Do we use this?
     */
    //@Override
    //public void writeWatermark(Watermark watermark){}
}
