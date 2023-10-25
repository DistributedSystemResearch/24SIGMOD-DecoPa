package com.huberlin.communication;

/*
public class TCPEventSenderTest {

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    @Test()
    public void receiver_receives_events_sent_by_sender() throws Exception {
        TCPEventReceiver<Event> rcv = new TCPEventReceiver<>(1, 50001, Event::parse);
        TCPEventSender<Event> snd = new TCPEventSender<>(SenderMode.BROADCAST, List.of(new TCPAddressString("localhost:50001")));

        Event[] to_exchange = new ArrayList<>(List.of(
                new Event("simple", "one", "13:01:01:983", "A"),
                new Event("simple", "two", "00:06:00:329", "B"))).toArray(new Event[0]);

        Thread rcvt = new Thread(() -> {
            try {
                rcv.run();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        rcvt.start();
        snd.invoke(to_exchange[0]);
        snd.invoke(to_exchange[1]);
        snd.finish();
        //rcv.cancel();
        rcvt.join(100);
        assertEquals(to_exchange[0], rcv.dequeue().payload);
        assertEquals(to_exchange[1], rcv.dequeue().payload);
        assertEquals(0, rcv.size());
    }
*/

    /*@Test(timeout = 8000)
    public void receiver_receives_events_sent_by_sender_no_race_condition() throws Exception {
        TCPEventReceiver<Event> rcv = new TCPEventReceiver<>(1, 50001, Event::parse);
        TCPEventSender<Event> snd = new TCPEventSender<>(SenderMode.BROADCAST, List.of(new TCPAddressString("localhost:50001")));

        Event[] to_exchange = new ArrayList<>(List.of(
                new Event("simple", "one", "13:01:01:983", "A"),
                new Event("simple", "two", "00:06:00:329", "B"))).toArray(new Event[0]);

        Thread rcvt = new Thread(() -> {
            try {
                rcv.run();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        rcvt.start(); //FIXME: Race condition
        Thread.sleep(1000);
        snd.invoke(to_exchange[0]);
        Thread.sleep(1000);
        snd.invoke(to_exchange[1]);
        Thread.sleep(1000);
        snd.finish();
        Thread.sleep(1000);
        rcv.cancel();
        Thread.sleep(1000);
        rcvt.join(100);
        Thread.sleep(1000);
        assertEquals(to_exchange[0], rcv.dequeue().payload);
        assertEquals(to_exchange[1], rcv.dequeue().payload);
        assertEquals(0, rcv.size());
    }
*/
//}