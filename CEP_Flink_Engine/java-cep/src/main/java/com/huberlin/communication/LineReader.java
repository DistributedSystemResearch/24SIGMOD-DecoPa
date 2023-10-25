package com.huberlin.communication;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

/**
 * Reads lines from a (non-blocking, buffer-based) java.nio.SocketChannel
 */
public class LineReader {
    final public char LINE_DELIMITER = '\n'; //currently this has to be a single ASCII character (unicode character number 0 to 127)
    final public int BUFSIZE = 4096;
    final public Charset TEXT_ENCODING = StandardCharsets.UTF_8;
    final private CharsetDecoder decoder = TEXT_ENCODING.newDecoder();
    private final SocketChannel socketChannel;
    private final ByteBuffer buffer = ByteBuffer.allocate(BUFSIZE);
    public LineReader(SocketChannel c) {
        socketChannel = c;
    }

    /**
     * Read zero or more lines.
     *
     * @return Array of lines read, or null if the socket has been closed. The lines don't contain the trailing line-separator character. Empty lines will become empty strings.
     * @throws IOException
     * @implNote Data preceding EOF without a terminating newline may never be returned from LineReader. Avoid this by terminating all messages with \n
     */
    public String[] readLines() throws IOException {
        int bytes_read = socketChannel.read(buffer);
        if (bytes_read == 0) {
            //System.out.println("Info: LineReader.readLines() called but no data is available. This can be avoided by using select()"); //todo: use logger on low severity loglevel instead
            return new String[]{};
        }
        else if (bytes_read > 0) {
            //Remove any completed lines from the buffer. Do not include the line-separator character in these lines.
            ArrayList<String> lines = new ArrayList<>();
            buffer.flip(); //put buffer into read-mode. See https://jenkov.com/tutorials/java-nio/buffers.html#flip
            final int LENGTH = buffer.limit();
            for (int i = LENGTH - bytes_read; i < LENGTH; i++) { //We know there are no line-ends at earlier indexes (readLines() preserves this invariant)
                if (buffer.get(i) == LINE_DELIMITER) {
                    //ByteBuffer line = buffer.slice(buffer.position(), i - buffer.position()).asReadOnlyBuffer(); //Does not work in java 11
                    ByteBuffer line = ByteBuffer.allocate(i - buffer.position());
                    //TODO: copy stuff in from buffer //FIXME!!
                    lines.add(decoder.decode(line).toString());
                    buffer.position(i+1);//mark data as read
                }
            }
            buffer.compact(); //Put buffer back into write mode. Moves any unread data to the start of the buffer (hopefully not physically, but via index adjustments + circular buffer)

            return lines.toArray(new String[0]);
            //TODO?: Return a collection, not an array, to avoid copying here. Or: take an 'output' collection as 1st argument
            //TODO?: Return CharBuffers, not strings (or collections thereof), to avoid copying caused by .toString()
        }
        else if (bytes_read == -1) {
            //source is exhausted, and will give no data in the future
            //assert(! this.socketChannel.isConnected());// FIXME!: Wrong! May cause trouble!!
            if (buffer.position() <= 0 /*No data left over since last explicit line break*/) {
                return null;
            }
            else {
                //return remaining data before EOF as the 'last line' //TODO: Simplify by ignoring this data, require sender to terminate all its lines explicitly (i.e change 'end-of-stream' to 'end-of-stream\n')
                buffer.flip();
                String [] result = new String[]{decoder.decode(buffer).toString()};
                buffer.clear();
                return result;
            }
        }
        else {
            throw new AssertionError("java.io.Reader.read() returned value below -1");
        }
    }
}