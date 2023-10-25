/*
package com.huberlin.communication;

import com.huberlin.util.testing.LoopbackTCPConnection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static org.apache.commons.lang3.ArrayUtils.addAll;
import static org.junit.Assert.*;

public class LineReaderTest {

    private static final Charset ENCODING = StandardCharsets.UTF_8;
    private static final char LINE_DELIMITER = '\n';

    private SelectionKey selectionKey;
    private SocketChannel socketChannel;
    private LineReader lineReader;
    private LoopbackTCPConnection loopback_tcp_connection;

    private Selector selector;

    @Before
    public void setup() throws IOException, InterruptedException {
        loopback_tcp_connection = new LoopbackTCPConnection();
        socketChannel = loopback_tcp_connection.getRxSocketChannel();
        selector = Selector.open();
        selectionKey = socketChannel.register(this.selector, SelectionKey.OP_READ);
        lineReader = new LineReader(socketChannel);
    }

    @After
    public void teardown() {
        try {
            socketChannel.close();
        } catch (IOException ignored) {}
        selectionKey.cancel();
        lineReader = null;
        socketChannel = null;
        selector = null;
        selectionKey = null;
        loopback_tcp_connection = null;
    }

    @Test(timeout = 2000)
    public void multiple_lines_at_once() throws IOException {
        String line1 = "Hello, world!";
        String line2 = "How are you?";
        String line3 = "I'm fine.";
        String lines = line1 + LINE_DELIMITER + line2 + LINE_DELIMITER + line3;
        byte[] data_to_send = lines.getBytes(ENCODING);

        loopback_tcp_connection.transmit_data(data_to_send,0, true);

        //collect everything sent
        String[] result = new String[]{};
        String[] partial_result;
        while ((partial_result = lineReader.readLines()) != null )
            result = addAll(result, partial_result);

        assertEquals(3, result.length);
        assertEquals(line1, result[0]);
        assertEquals(line2, result[1]);
        assertEquals(line3, result[2]);
        assertNull(lineReader.readLines());
    }

    @Test(timeout = 4000)
    public void multiple_pieces_per_line_and_multiple_lines_per_piece() throws IOException, InterruptedException {
        String piece1 = "Hel";
        String piece2 = "lo, ";
        String piece3 = "world!" + LINE_DELIMITER + "How are";
        String piece4 = " you?" + LINE_DELIMITER;

        Thread t = loopback_tcp_connection.transmit_data(piece1.getBytes(ENCODING));
        t.join(1000);
        assertArrayEquals("No output yet...", new String[]{},lineReader.readLines());

        t = loopback_tcp_connection.transmit_data(piece2.getBytes(ENCODING));
        t.join(1000);
        assertArrayEquals("No output yet...", new String[]{},lineReader.readLines());
        t = loopback_tcp_connection.transmit_data(piece3.getBytes(ENCODING));

        t.join(1000);
        assertArrayEquals("First line is output one complete", new String[] {"Hello, world!"}, lineReader.readLines());
        assertArrayEquals("Start of second line isn't output", new String[]{}, lineReader.readLines());
        t = loopback_tcp_connection.transmit_data(piece4.getBytes(ENCODING));
        t.join(1000);
        assertArrayEquals("Rest of second line is output later", new String[]{"How are you?"}, lineReader.readLines());
        t = loopback_tcp_connection.transmit_data(new byte[]{}, 0, true);
        t.join();
        assertNull(lineReader.readLines());
    }

    @Test(timeout = 1200)
    public void no_data() throws IOException, InterruptedException {
        Thread t = loopback_tcp_connection.transmit_data(new byte[]{}, 0, true);
        t.join(1000);
        assertNull("Null output if connection opens then closes with no data", lineReader.readLines());
    }

    @Test(timeout = 1200)
    public void empty_string() throws IOException, InterruptedException {
        Thread t = loopback_tcp_connection.transmit_data("".getBytes(ENCODING), 0, true);
        t.join(1000);
        assertNull("Null output if connection opens then closes with no data", lineReader.readLines());
    }


    @Test(timeout = 1200)
    public void blank_lines_give_empty_strings() throws IOException, InterruptedException {
        Thread t = loopback_tcp_connection
                .transmit_data((""+LINE_DELIMITER+LINE_DELIMITER+LINE_DELIMITER)
                .getBytes(ENCODING), 0, true);
        t.join(1000);
        assertArrayEquals("One empty string per blank line", new String[]{"","",""}, lineReader.readLines());
        assertNull(lineReader.readLines());
    }
    @Test()
    public void strsplit() {
        assertArrayEquals("c",new String[]{"","","c"}, "\n\nc\n".split("\n"));
        assertArrayEquals("a",new String[]{"a"}, "a\n\n\n".split("\n"));
        assertArrayEquals("all newlines",new String[]{"","",""}, "\n\n".split("\n", -1));
    }
}
*/
