package site.hnfy258.demo1;

import site.hnfy258.demo2.DynamicProcessingConfig;
import site.hnfy258.demo2.EventMetrics;
import site.hnfy258.demo3.EventProcessorService;
import site.hnfy258.demo3.ProcessingResult;
import site.hnfy258.demo4.VirtualThreadFileLogger;
import site.hnfy258.demo4.ConcurrentBlocklist;

import java.util.Collections;
import java.util.concurrent.CompletionException; // 引入 CompletionException，用于处理 CompletableFuture 抛出的异常
import java.util.concurrent.TimeUnit;

public class EventConsumer implements Runnable {
    // 移除本地的 successCount 变量。
    // 现在，所有的成功/失败计数都将通过 EventMetrics 进行全局且线程安全的统计。
    // private long successCount = 0;

    private final EventBufferQueue queue;
    private final EventMetrics metrics; // 注入 EventMetrics
    private final DynamicProcessingConfig config; // 注入 DynamicProcessingConfig
    private final EventProcessorService eventProcessorService; // 注入 EventProcessorService
    private final VirtualThreadFileLogger fileLogger;
    private final ConcurrentBlocklist blacklist; // 注入黑名单

    public EventConsumer(EventBufferQueue queue,
                         EventMetrics eventMetrics,
                         DynamicProcessingConfig config,
                         EventProcessorService eventProcessorService,
                         VirtualThreadFileLogger fileLogger,
                         ConcurrentBlocklist blacklist) {
        this.queue = queue;
        this.metrics = eventMetrics;
        this.config = config;
        this.eventProcessorService = eventProcessorService;
        this.fileLogger = fileLogger;
        this.blacklist = blacklist;
    }

    @Override
    public void run() {
        String threadName = Thread.currentThread().getName();
        System.out.println(threadName + " 开始异步消费事件。"); // 更新日志，表明是异步消费

        while (true) {
            Event event = null;
            try {
                if (!config.isProcessingEnabled()) {
                    Thread.sleep(10);
                    continue;
                }

                event = queue.pollEvent(500, TimeUnit.MILLISECONDS);

                if (event != null) {
                    // 检查事件是否在黑名单中
                    if (isEventBlacklisted(event)) {
                        System.out.println(threadName + " 事件被黑名单过滤: " + event.traceId() +
                                         " (类型: " + event.type() + ", 载荷: " + event.payload() + ")");
                        metrics.incrementErrorCount(); // 将被过滤的事件计为错误
                        fileLogger.log("事件" + event.traceId() + "被黑名单过滤，线程: " + threadName);
                        continue; // 跳过此事件，继续处理下一个
                    }

                    final Event currentEvent = event;
                    eventProcessorService.processEvent(currentEvent)
                            .thenAccept(result -> {
                                if (result.success()) {
                                    String callbackThreadName = Thread.currentThread().getName();
                                    metrics.incrementProcessedCount();
                                    metrics.incrementEventCategoryCount(currentEvent.type());
                                    System.out.println(Thread.currentThread().getName() + " [异步回调] 成功处理事件: " + currentEvent.traceId());
                                    fileLogger.log("事件"+currentEvent.traceId()+"处理成功，线程: " + callbackThreadName);
                                } else {
                                    // 4.2 异步回调：处理失败结果
                                    metrics.incrementErrorCount(); // 线程安全更新全局错误计数
                                    System.err.println(Thread.currentThread().getName() + " [异步回调] 处理事件失败: " + currentEvent.traceId() + ". 错误: " + result.errorMessage());
                                    fileLogger.log("事件"+currentEvent.traceId()+"处理失败，错误: " + result.errorMessage() + "，线程: " + Thread.currentThread().getName());
                                }
                            })
                            .exceptionally(ex -> {

                                metrics.incrementErrorCount();
                                Throwable actualCause = (ex instanceof CompletionException) ? ex.getCause() : ex; // 获取原始异常
                                System.err.println(Thread.currentThread().getName() + " [异步回调] 处理事件时发生意外错误: " + currentEvent.traceId() + ". 错误类型: " + actualCause.getClass().getSimpleName() + ". 详细: " + actualCause.getMessage());
                                actualCause.printStackTrace();
                                fileLogger.log("事件"+currentEvent.traceId()+"处理异常，错误: " + actualCause.getMessage() + "，线程: " + Thread.currentThread().getName());
                                return null;
                            });

                } else {
                    // 5. 队列为空时的退出条件：当生产者完成且队列已空时，消费者优雅退出
                    if (queue.isProductionComplete() && queue.isEmpty()) {
                        System.out.println(threadName + " 检测到生产完成且队列已空，优雅退出。");
                        break; // 退出循环
                    }
                }
            } catch (InterruptedException e) {
                // 6. 消费者线程自身被中断 (通常是 ExecutorService 关闭时的信号)
                Thread.currentThread().interrupt(); // 恢复中断状态
                System.err.println(threadName + " 在等待事件时被中断。正在退出。");
                break; // 退出循环
            } catch (Exception e) {

                System.err.println(threadName + " 消费者循环中发生意外错误: " + e.getMessage());
                metrics.incrementErrorCount(); // 记录为错误
                e.printStackTrace();
            }
        }
        System.out.println(threadName + " 消费任务完成（异步提交任务）。"); // 更新日志，反映异步性质
    }

    /**
     * 检查事件是否在黑名单中
     * 可以根据事件的不同属性进行检查：类型、载荷、或追踪ID
     */
    private boolean isEventBlacklisted(Event event) {
        // 检查事件类型是否在黑名单中
        if (blacklist.contains(event.type())) {
            return true;
        }

        // 检查载荷是否在黑名单中
        if (blacklist.contains(event.payload())) {
            return true;
        }

        // 检查追踪ID是否在黑名单中
        if (blacklist.contains(event.traceId())) {
            return true;
        }

        return false;
    }
}

