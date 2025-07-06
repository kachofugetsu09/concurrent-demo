package site.hnfy258.demo1;

import java.util.concurrent.TimeUnit;

public class EventConsumer implements Runnable{
    private long successCount = 0; // 建议改为 long 类型

    public EventConsumer(EventBufferQueue queue){
        this.queue =  queue;
    }
    private EventBufferQueue queue;
    @Override
    public void run() {
        String threadName = Thread.currentThread().getName();
        System.out.println(threadName + " started consuming.");

        while(true){
            Event event = null;
            try {
                // 使用带超时的 pollEvent 方法。
                // 如果队列为空，会等待直到超时或者有新事件。
                event = queue.pollEvent(500, TimeUnit.MILLISECONDS); // 等待 500ms

                if (event != null) {
                    successCount++;
                    Thread.sleep(50); // 模拟处理延迟
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
                e.printStackTrace();
            }
        }
        System.out.println(threadName + " finished consuming. Total processed: " + successCount);
    }

    public long getSuccessCount() {
        return successCount;
    }
}