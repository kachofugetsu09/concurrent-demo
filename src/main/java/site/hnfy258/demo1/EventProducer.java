package site.hnfy258.demo1;

import java.util.concurrent.atomic.AtomicInteger;

public class EventProducer implements  Runnable{
    private int success =0;
    private int fail = 0;
    private EventBufferQueue queue;
    private final int number;

    public EventProducer(EventBufferQueue queue,int number){
        this.queue = queue;
        this.number = number;
    }
    @Override
    public void run() {
        for(int i=0;i<number;i++){
            try{
                Event event = new Event(System.currentTimeMillis(), "type",
                        "payload-" + i, "traceId-" + System.nanoTime());
                boolean res = queue.offerEvent(event);
                if(res){
                    success++;
                }
                else{
                    fail++;
                }
                Thread.sleep(10);
            }
            catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    public int getSuccess() {
        return success;
    }

    public int getFail() {
        return fail;
    }
}
