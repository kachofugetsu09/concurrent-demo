package site.hnfy258.demo1;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class PipelineLauncher {
    public static void main(String[] args) {
        EventBufferQueue eventBufferQueue = new EventBufferQueue();
        final int EVENT_PER_CONSUMER = 100;
        final int PRODUCER_COUNT = 5;
        final int CONSUMER_COUNT = 10;

        ExecutorService producerExecutor = Executors.newFixedThreadPool(PRODUCER_COUNT);
        List<Future<?>> producersFutures = new ArrayList<>();
        List<EventProducer> producers = new ArrayList<>();

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
            EventConsumer consumer = new EventConsumer(eventBufferQueue);
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


        System.out.println("FINISH");
        long totalProducedSuccessFully = 0;
        long totalProducedFailed = 0;

        for(EventProducer producer: producers){
            totalProducedFailed+= producer.getFail();
            totalProducedSuccessFully += producer.getSuccess();
        }

        System.out.println("Total produced successfully: " + totalProducedSuccessFully);
        System.out.println("Total produced failed: " + totalProducedFailed);

        long totalConsumed =0;
        for(EventConsumer consumer: consumers){
            totalConsumed += consumer.getSuccessCount();
        }

        System.out.println("Total consumed: " + totalConsumed);





    }
}
