package com.huberlin.communication;

/*
public class TCPEventReceiverTest {

    private TCPEventReceiver<Event> rcv;
    @Before
    public void setUp() {
    }



    @After
    public void tearDown() {
    }

    @Test(timeout = 10000)
    public void does_not_crash() throws IOException {
        TCPEventReceiver<Event> rcv = new TCPEventReceiver<>(1,5000, Event::parse);

        new Thread(() -> {
            try {
                rcv.run();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).start();

        //start python sender
        Process sensor = Runtime.getRuntime().exec(
                new String[]{"venv/bin/python", "send_eventstream.py"},
                null,
                new File("../python"));

        for(int i=0; i<100; i++){
            EventReceiver.EventWithWatermark<Event> ew = rcv.dequeue();
            Event e = ew.payload;
            long wm = ew.watermarkTimestamp;
            System.out.println("Got event " + e + " with warermark ts\t\t " + wm);
        }
        sensor.destroy();
        return;
    }
}*/
