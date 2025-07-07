package site.hnfy258.demo1;

import site.hnfy258.demo2.ConfigUpdater;
import site.hnfy258.demo2.DynamicProcessingConfig;
import site.hnfy258.demo2.EventMetrics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.*;

public class PipelineLauncher {
    public static void main(String[] args) {
        EventBufferQueue eventBufferQueue = new EventBufferQueue();
        final int EVENT_PER_CONSUMER = 100;
        final int PRODUCER_COUNT = 5;
        final int CONSUMER_COUNT = 10;

        ExecutorService producerExecutor = Executors.newFixedThreadPool(PRODUCER_COUNT);
        List<Future<?>> producersFutures = new ArrayList<>();
        List<EventProducer> producers = new ArrayList<>();

        EventMetrics eventMetrics = new EventMetrics(new ConcurrentHashMap<>());

        DynamicProcessingConfig config = new DynamicProcessingConfig();
        ExecutorService configUpdaterExecutor = Executors.newSingleThreadExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setName("ConfigUpdaterThread");

                return t;
            }
        });
        ConfigUpdater configUpdater = new ConfigUpdater(config);
        configUpdaterExecutor.submit(configUpdater);

        System.out.println("Starting producers...");
        for(int i=0;i<PRODUCER_COUNT;i++){
            EventProducer  producer = new EventProducer(eventBufferQueue,EVENT_PER_CONSUMER);
            producers.add(producer);
            producersFutures.add(producerExecutor.submit(producer));
        }

        ExecutorService consumersExecutor = Executors.newFixedThreadPool(CONSUMER_COUNT);
        List<Future<?>> consumerFutures = new ArrayList<>();
        List<EventConsumer> consumers = new ArrayList<>();


        for (int i = 0; i < CONSUMER_COUNT; i++) {
            EventConsumer consumer = new EventConsumer(eventBufferQueue,eventMetrics,config);
            consumers.add(consumer);
            consumerFutures.add(consumersExecutor.submit(consumer));
        }

        producerExecutor.shutdown();
        System.out.println("Producers started shutdown");

        try{
            if(!producerExecutor.awaitTermination(1, TimeUnit.MINUTES)){
                producerExecutor.shutdownNow();
            }
            else{
                System.out.println("Producers finished");
            }
        }catch(Exception e){
            producerExecutor.shutdownNow();
            Thread.currentThread().interrupt();
            System.out.println("Producers interrupted");
        }

        // 标记生产完成，让消费者知道不会再有新事件
        eventBufferQueue.markProductionComplete();
        System.out.println("Production marked as complete");

        consumersExecutor.shutdown();
        System.out.println("Consumers started shutdown. Waiting for consumers to finish processing remaining events...");
        try{

            if(!consumersExecutor.awaitTermination(5,TimeUnit.MINUTES)){
                consumersExecutor.shutdownNow();
                System.err.println("Consumers did not terminate in time (5min), forced shutdown. Some events might not be processed.");
            }else{
                System.out.println("Consumers finished");
            }
        }catch(InterruptedException e){
            consumersExecutor.shutdownNow();
            Thread.currentThread().interrupt();
            System.err.println("Consumers await termination interrupted: " + e.getMessage());
        }

        configUpdaterExecutor.shutdown();
        System.out.println("ConfigUpdater started shutdown. Waiting for config updates to finish...");

        try {
            if (!configUpdaterExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                // 如果优雅关闭超时，强制中断
                configUpdaterExecutor.shutdownNow();
                System.out.println("ConfigUpdater did not terminate gracefully, forcing shutdown.");

                // 再等待一点时间确保中断处理完成
                if (!configUpdaterExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    System.err.println("ConfigUpdater did not terminate even after forced shutdown.");
                }
            } else {
                System.out.println("ConfigUpdater finished gracefully");
            }
        } catch (InterruptedException e) {
            configUpdaterExecutor.shutdownNow();
            Thread.currentThread().interrupt();
            System.err.println("ConfigUpdater await termination interrupted: " + e.getMessage());
        }


        System.out.println("FINISH");
        long totalProducedSuccessFully = 0;
        long totalProducedFailed = 0;

        for(EventProducer producer: producers){
            totalProducedFailed+= producer.getFail();
            totalProducedSuccessFully += producer.getSuccess();
        }

        System.out.println("Total produced successfully: " + totalProducedSuccessFully);
        System.out.println("Total produced failed: " + totalProducedFailed);

        long totalConsumedLocal =0; // 本地统计
        for(EventConsumer consumer: consumers){
            totalConsumedLocal += consumer.getSuccessCount();
        }
        System.out.println("Total consumed (local): " + totalConsumedLocal);

        eventMetrics.printMetrics(); // 打印 EventMetrics 中的统计数据
        System.out.println("Events remaining in queue: " + eventBufferQueue.size());

        config.printConfig(); // 打印最终配置状态
    }
}
