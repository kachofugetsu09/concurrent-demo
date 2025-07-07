package site.hnfy258.demo1;

import site.hnfy258.demo2.DynamicProcessingConfig;
import site.hnfy258.demo2.EventMetrics;

import java.util.concurrent.TimeUnit;

public class EventConsumer implements Runnable{
    private long successCount = 0;
    private final EventBufferQueue queue;
    private final EventMetrics metrics;
    private final DynamicProcessingConfig config;

    public EventConsumer(EventBufferQueue queue, EventMetrics eventMetrics, DynamicProcessingConfig config){
        this.queue =  queue;
        this.metrics = eventMetrics;
        this.config = config;
    }


    @Override
    public void run() {
        String threadName = Thread.currentThread().getName();
        System.out.println(threadName + " started consuming.");

        while(true){
            Event event = null;
            try {
                if(!config.isProcessingEnabled()){
                    Thread.sleep(10);
                    continue;
                }
                event = queue.pollEvent(500, TimeUnit.MILLISECONDS);
                if (event != null) {
                    successCount++;
                    metrics.incrementProcessedCount();
                    metrics.incrementEventCategoryCount(event.type());
                    Thread.sleep(50);
                } else {
                    // 检查生产是否已完成且队列为空
                    if (queue.isProductionComplete() && queue.isEmpty()) {
                        System.out.println(threadName + " detected production complete and empty queue, exiting gracefully.");
                        break; // 退出循环
                    }
                }
            }catch(InterruptedException e){

                Thread.currentThread().interrupt();
                System.err.println(threadName + " was interrupted while waiting for events. Processed: " + successCount);
                break;
            }catch (Exception e){
                System.err.println(threadName + " encountered an unexpected error: " + e.getMessage());
                metrics.incrementErrorCount();
                e.printStackTrace();
            }
        }
        System.out.println(threadName + " finished consuming. Total processed: " + successCount);
    }

    public long getSuccessCount() {
        return successCount;
    }
}