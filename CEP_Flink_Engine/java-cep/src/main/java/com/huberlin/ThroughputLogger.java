package com.huberlin;

import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class ThroughputLogger extends Thread {
    private final AtomicInteger counter;
    private final String file_path;

    public ThroughputLogger(AtomicInteger counter, String file_path) {
        this.counter = counter;
        this.file_path = file_path;
    }

    @Override
    public void run() {
        try (FileWriter writer = new FileWriter(file_path, true)) {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(10000);
                    int count = counter.getAndSet(0);
                    String log_line = count + "\n";
                    writer.write(log_line);
                    writer.flush();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (IOException e) {
                    e.printStackTrace();
                    break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
