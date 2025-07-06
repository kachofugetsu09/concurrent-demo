package site.hnfy258.demo1;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class EventBufferQueue {
    private final BlockingQueue<Event> queue;
    private volatile boolean productionComplete = false;

    public static final int DEFAULT_CAPACITY = 1000;

    public EventBufferQueue(){
        this.queue = new ArrayBlockingQueue<>(DEFAULT_CAPACITY);
    }

    public boolean offerEvent(Event event){
        boolean success = queue.offer(event);
        if(!success){
            System.out.println("Queue is full, failed to add event: " + event);
        }
        return success;
    }

    public void putEvent(Event event) throws InterruptedException {
        queue.put(event);
    }

    public Event pollEvent(long timeout, TimeUnit unit) throws InterruptedException {
        return queue.poll(timeout, unit);
    }

    public Event takeEvent() throws InterruptedException {
        return queue.take();
    }

    public boolean isFull(){
        return queue.remainingCapacity() == 0;
    }

    public boolean isEmpty(){
        return queue.isEmpty();
    }

    public int size() {
        return queue.size();
    }

    public void markProductionComplete() {
        this.productionComplete = true;
    }

    public boolean isProductionComplete() {
        return productionComplete;
    }
}
