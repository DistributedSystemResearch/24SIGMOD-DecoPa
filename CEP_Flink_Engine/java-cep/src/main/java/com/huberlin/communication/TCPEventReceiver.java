package com.huberlin.communication;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;

import com.huberlin.event.Event;

import com.huberlin.communication.addresses.TCPAddressString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TCPEventReceiver implements EventReceiver{

    private final Logger log = LoggerFactory.getLogger(TCPEventReceiver.class);
    private final Function<String, Event> deserializer;
    private final int connections_to_establish;
    private final int port;
    private long current_global_watermark = 0;
    private boolean isCancelled = false;
    private Selector connections;
    private final Map<SelectionKey, Long> neweset_watermarking_timestamp = new HashMap<>();
    private final Map<SelectionKey, LineReader> message_readers = new HashMap<>();
    private final BlockingQueue<EventWithWatermark> output = new LinkedBlockingQueue<>(); //TODO: Instead of writing to output, make this a sink function, and call the provided flink event-submission function?
    private Thread new_connection_accepting_thread;

    public TCPEventReceiver(List<Integer> connectionsToEstablish,
                            Map<Integer, String> node_id_to_tcp_address,
                            int my_nodeId,
                            Function<String, Event> deserializer) {

        this.connections_to_establish = connectionsToEstablish.size(); //we only use the number
        this.deserializer = deserializer;
        TCPAddressString my_external_tcp_addr = new TCPAddressString(node_id_to_tcp_address.get(my_nodeId));
        this.port = my_external_tcp_addr.getPort();

        try {
            this.connections = Selector.open();
        }
        catch (IOException e) {
            System.err.println("Failed to open selector");
            e.printStackTrace(System.err);
        }
    }

    TCPEventReceiver(int connectionsToEstablish,
                     int port,
                     Function<String, Event> deserializer) {

        this.connections_to_establish = connectionsToEstablish;
        this.deserializer = deserializer;
        this.port = port;
        try {
            this.connections = Selector.open();
        }
        catch (IOException e) {
            System.err.println("Failed to open selector");
            e.printStackTrace(System.err);
        }
    }


    /**
     * Accept connections
     * @param num_to_accept Accept connections until this number of live connecions is reached
     */
    private void accept_new_connections(int num_to_accept) {
        ServerSocketChannel socket;
        try {
            socket = ServerSocketChannel.open();
            socket.bind(new InetSocketAddress(port));
        }
        catch (IOException e) {
            log.error("Failed to open server socket");
            e.printStackTrace(System.err);
            return;
        }
        while (!isCancelled && connections.keys().size() < num_to_accept) {
                //blockingly accept another (non-blocking) connection //todo?: we could do without this thread if we wanted, i.e. also accept connections non-blockingly like we accept data
            try {
                SocketChannel client_socket = socket.accept();
                log.info("Accepted connection: " + client_socket);
                client_socket.configureBlocking(false);
                socket.configureBlocking(true);

                SelectionKey sk = client_socket.register(connections, SelectionKey.OP_READ);
                message_readers.put(sk, new LineReader(client_socket));
                neweset_watermarking_timestamp.put(sk, Long.MIN_VALUE); //FIXME: seems like this can allow the global watermark to move backwards if a connection is added later? Maybe first acept all connections, then start running (do not use seperate thread)
            }
            catch (IOException e) {
                e.printStackTrace(System.err);
                System.err.println("Warning: Failed to accept a connection");
            }
        }
        try {
            socket.close();
        } catch (IOException e) {log.warn("Failed to close listening port: " + e);}
    }

    public void run() throws IOException {
        //initially, wait for connections from all predecessor nodes
        this.accept_new_connections(this.connections_to_establish);

        //start listening for more connectios later
        //this.new_connection_accepting_thread = new Thread((this)-> {this.accept_new_connections(Integer.MAX_VALUE});
        //this.new_connection_accepting_thread.start();

        //then listen for events and process them
        event_loop();
    }

    void event_loop() throws IOException {
        //handle clients
        while (! this.isCancelled){
            //Select a socket on which there is data (blocking read from first available socket)
            int n_connections_with_data_to_read = connections.select(10);

            if (n_connections_with_data_to_read == 0) {
                log.debug("select returned, but no new sockets selected");
                continue;
            }

            Set<SelectionKey> connections_with_data_to_read = connections.selectedKeys();
            Iterator<SelectionKey> it = connections_with_data_to_read.iterator();
            while (it.hasNext()){
                SelectionKey sk = it.next();
                if (sk.isReadable() && sk.isValid()) {
                    LineReader reader = this.message_readers.get(sk);
                    String[] messages = reader.readLines();
                    if (messages == null) {
                        try {
                            log.info("Got -1 from readLines / socketchanne.read - assuming, no more data is coming - closing " + sk.channel());
                            sk.channel().close();
                            sk.cancel();
                        } catch (Exception e) {
                            log.info("Excption "+ e + " while closing channel" + sk.channel());
                        }
                    }
                    else
                        for (String message : messages)
                            process_message(message, sk);
                }
                it.remove();
            }
        }
    }

    /**
     * Process a message received from a connection<br>
     *
     * Currently, either "end-of-the-stream" or a serialized event
     * @param line
     * @param sk
     * @throws InterruptedException
     */
    private void process_message(String line, SelectionKey sk) {
        //check, if a client has sent his entire event stream..
        if (line.contains("end-of-the-stream")) {
            neweset_watermarking_timestamp.put(sk, Long.MAX_VALUE);
            log.info("Reached the end of the stream for " + sk.channel() + "->" + neweset_watermarking_timestamp.get(sk));
            close_connection(sk);
        }
        else {
            if (!line.contains("|")) { //FIXME?: replace with !IsEventMessage(line), or just, catch deserializer InvalidArgumentExceptions and ignore them
                log.warn("Invalid message ignored: " + line);
                return;//This is a dependency on the concrete serialization format of events; makes no sense to have that encapsulated in the deserializer if I also depend on it here
            }
            Event event = deserializer.apply(line);
            log.trace("Got event: " + event);

            //watermarking stuff
            neweset_watermarking_timestamp.put(sk, event.getTimestamp()); //FIXME: assumes events arrive in order
            try {
                output.put(new EventWithWatermark(event, event.getTimestamp())); //FIXME: Need to insert in the right position + compute the right watermark; again this is the SHKs' job
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public EventWithWatermark dequeue() {
        try {
            return this.output.take();
        } catch (InterruptedException e) { //FIXME: I don't understand when .take() throws this exception... is it safe to igore it like this? Currently we assume it desnt happen...
            throw new RuntimeException(e);
        }
    }

    public int size() {
        return this.output.size();
    }

    /**
     * Attempt graceful shutdown
     */
    public void cancel() {
        this.isCancelled = true;
        for (SelectionKey sk : this.connections.keys())  {
            try {
                SocketChannel c = (SocketChannel) sk.channel();
                c.close(); //close all inbound TCP connections
                sk.cancel();
                log.info("Closed connection: " + c);
            }
            catch (IOException e){
                log.warn("Unable to close connection (" + e + ")" + sk.channel());
            }
        }
    }

    private void cleanup_dead_connections() {
        for (SelectionKey sk : this.connections.keys()) {
            if (!((SocketChannel) sk.channel()).socket().isConnected())
                close_connection(sk);
        }
    }

    private void close_connection(SelectionKey sk){
        try {
            SocketChannel channel = (SocketChannel) sk.channel();

            sk.channel().close();
            sk.cancel();
            log.info("Disconnected:" + channel);
        } catch (Exception e) {
            log.info("Excption " + e + " while closing channel" + sk.channel());
        }
    }

    public void updateGlobalWatermark() { //FIXME: unused; also the global watermark itself is unused (except here); Check how SHKs intend to provide watermarks
        //if not every node has sent an event yet, wait for it to update the watermark
        if (connections_to_establish > neweset_watermarking_timestamp.size()) {
            log.info("Not everyone has connected yet!" + connections_to_establish + " vs. " + neweset_watermarking_timestamp.size());
        }
        else {
            long oldestTimestamp = Collections.min(neweset_watermarking_timestamp.values());
            if (oldestTimestamp < Long.MAX_VALUE && current_global_watermark < (oldestTimestamp - 1)) {
                //update watermark
                current_global_watermark = oldestTimestamp - 1;
            }
        }
    }
}