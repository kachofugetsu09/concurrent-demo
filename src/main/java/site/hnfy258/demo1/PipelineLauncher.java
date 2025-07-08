package site.hnfy258.demo1;

import site.hnfy258.demo2.ConfigUpdater;
import site.hnfy258.demo2.DynamicProcessingConfig;
import site.hnfy258.demo2.EventMetrics;
import site.hnfy258.demo3.EventProcessorService;
import site.hnfy258.demo3.MockExternalService;
import site.hnfy258.demo4.ConcurrentBlocklist;
import site.hnfy258.demo4.VirtualThreadFileLogger;

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

        // 初始化黑名单，添加一些会被生产者实际生产的黑名单项
        // 不阻止所有事件，而是阻止特定的载荷，这样可以看到过滤效果
        ConcurrentBlocklist blackList = new ConcurrentBlocklist(
            "payload-5",         // 这会阻止第6个事件（i=5时）
            "payload-15",        // 这会阻止第16个事件（i=15时）
            "payload-25",        // 这会阻止第26个事件（i=25时）
            "payload-50",        // 这会阻止第51个事件（i=50时）
            "payload-75",        // 这会阻止第76个事件（i=75时）
            "payload-99"         // 这会阻止第100个事件（i=99时）
        );

        System.out.println("黑名单初始化完成，包含 " + blackList.size() + " 个项目");
        System.out.println("黑名单将过滤以下载荷：payload-5, payload-15, payload-25, payload-50, payload-75, payload-99");

        ExecutorService configUpdaterExecutor = Executors.newSingleThreadExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setName("ConfigUpdaterThread");
                t.setDaemon(true); // 设置为守护线程
                return t;
            }
        });
        ConfigUpdater configUpdater = new ConfigUpdater(config);
        configUpdaterExecutor.submit(configUpdater);

        MockExternalService service = new MockExternalService();
        EventProcessorService processorService = new EventProcessorService(service,blackList);

        // 初始化文件日志器
        VirtualThreadFileLogger fileLogger = new VirtualThreadFileLogger();

        System.out.println("Starting producers...");
        for(int i=0;i<PRODUCER_COUNT;i++){
            EventProducer producer = new EventProducer(eventBufferQueue, EVENT_PER_CONSUMER);
            producers.add(producer);
            producersFutures.add(producerExecutor.submit(producer));
        }

        ExecutorService consumersExecutor = Executors.newFixedThreadPool(CONSUMER_COUNT);
        List<Future<?>> consumerFutures = new ArrayList<>();
        List<EventConsumer> consumers = new ArrayList<>();

        // 创建消费者时传入黑名单
        for (int i = 0; i < CONSUMER_COUNT; i++) {
            EventConsumer consumer = new EventConsumer(
                eventBufferQueue,
                eventMetrics,
                config,
                processorService,
                fileLogger,
                blackList  // 传入黑名单
            );
            consumers.add(consumer);
            consumerFutures.add(consumersExecutor.submit(consumer));
        }

        // 关闭生产者执行器
        producerExecutor.shutdown();
        System.out.println("Producers started shutdown");

        try{
            if(!producerExecutor.awaitTermination(1, TimeUnit.MINUTES)){
                producerExecutor.shutdownNow();
            }
            else{
                System.out.println("Producers finished");
            }
        }catch(InterruptedException e){
            producerExecutor.shutdownNow();
            Thread.currentThread().interrupt();
            System.out.println("Producers interrupted");
        }

        // 标记生产完成，让消费者知道不会再有新事件
        eventBufferQueue.markProductionComplete();
        System.out.println("Production marked as complete");

        // 关闭消费者执行器
        consumersExecutor.shutdown();
        System.out.println("Consumers started shutdown. Waiting for consumers to finish processing remaining events...");
        try{
            if(!consumersExecutor.awaitTermination(5, TimeUnit.MINUTES)){
                consumersExecutor.shutdownNow();
                System.err.println("Consumers did not terminate in time (5min), forced shutdown. Some events might not be processed.");
            }else{
                System.out.println("All consumers finished successfully");
            }
        }catch(InterruptedException e){
            consumersExecutor.shutdownNow();
            Thread.currentThread().interrupt();
            System.err.println("Consumers interrupted during shutdown");
        }

        // 关闭配置更新器
        configUpdaterExecutor.shutdown();
        try{
            if(!configUpdaterExecutor.awaitTermination(10, TimeUnit.SECONDS)){
                configUpdaterExecutor.shutdownNow();
            }
            System.out.println("Config updater shutdown complete");
        }catch(InterruptedException e){
            configUpdaterExecutor.shutdownNow();
            Thread.currentThread().interrupt();
            System.err.println("Config updater interrupted during shutdown");
        }

        eventMetrics.printMetrics();
    }
}
