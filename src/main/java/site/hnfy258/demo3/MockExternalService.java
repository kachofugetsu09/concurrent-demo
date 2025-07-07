package site.hnfy258.demo3;

import site.hnfy258.demo1.Event; // 确保 Event 类的包名正确
import site.hnfy258.demo3.exception.*;

import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


public class MockExternalService {
    private final ExecutorService cpuTaskExecutor;
    private final ExecutorService IOTaskExecutor;
    private final ConcurrentHashMap<String, Event> transformedDataCache = new ConcurrentHashMap<>();
    private final Random random = new Random();

    public MockExternalService() {
        this.cpuTaskExecutor = Executors.newFixedThreadPool(
                Runtime.getRuntime().availableProcessors() + 1,
                r -> new Thread(r, "Mock-CPU-Task-Thread-" + r.hashCode()) // 命名线程更清晰
        );
        this.IOTaskExecutor = Executors.newCachedThreadPool(
                r -> new Thread(r, "Mock-IO-Task-Thread-" + r.hashCode()) // 命名线程更清晰
        );
    }

    // --- 模拟事件校验服务 (I/O 密集型) ---
    public CompletableFuture<Boolean> validateEvent(Event event) {
        return CompletableFuture.supplyAsync(() -> {
            String threadName = Thread.currentThread().getName();
            System.out.println(threadName + " is validating event: " + event.traceId() + "..."); // 使用 traceId 更简洁

            try {
                Thread.sleep(50); // 模拟延迟

                int outcome = random.nextInt(100); // 0-99
                if (outcome < 2) { // 2% 概率抛出 NetworkException
                    System.err.println(threadName + " Validation FAILED (NetworkError) for event: " + event.traceId());
                    throw new NetworkException("Simulated network issue for event: " + event.traceId());
                } else if (outcome < 4) { // 另外 2% 概率抛出 TimeoutException
                    System.err.println(threadName + " Validation FAILED (TimeoutError) for event: " + event.traceId());
                    throw new TimeoutException("Simulated timeout for event: " + event.traceId());
                } else if (outcome < 10) { // 另外 6% 概率校验失败 (返回 false)
                    System.out.println(threadName + " Validation result: **FALSE** for event: " + event.traceId());
                    return false;
                } else { // 90% 概率校验成功 (返回 true)
                    System.out.println(threadName + " Validation result: **TRUE** for event: " + event.traceId());
                    return true;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); // 重新设置中断标志
                System.err.println(threadName + " Validation INTERRUPTED for event: " + event.traceId());
                throw new RuntimeException("Validation interrupted", e); // 转换为运行时异常抛出
            }
        }, IOTaskExecutor); // 校验是 I/O 密集型
    }

    // --- 模拟数据转换服务 (CPU 密集型) ---
    public CompletableFuture<String> transformData(Event event) { // 🚨 修正：返回 CompletableFuture<String>
        return CompletableFuture.supplyAsync(() -> {
            String threadName = Thread.currentThread().getName();
            System.out.println(threadName + " is transforming data for event: " + event.traceId() + "...");

            try {
                Thread.sleep(30); // 模拟处理延迟

                if (random.nextInt(100) < 5) { // 5% 概率抛出 DataFormatException
                    System.err.println(threadName + " Transformation FAILED (FormatError) for event: " + event.traceId());
                    throw new DataFormatException("Simulated data format error for event: " + event.traceId());
                }

                String transformedPayload = event.payload() + "-TRANSFORMED-TS(" + System.nanoTime() + ")";
                System.out.println(threadName + " Transformation **SUCCESS** for event " + event.traceId() + " -> " + transformedPayload.substring(0, Math.min(transformedPayload.length(), 40)) + "...");
                return transformedPayload; // 🚨 返回 String
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); // Reset interrupt flag
                System.err.println(threadName + " Transformation INTERRUPTED for event: " + event.traceId());
                throw new RuntimeException("Transformation interrupted", e);
            }
        }, cpuTaskExecutor); // 转换是 CPU 密集型
    }

    // --- 模拟数据存储服务 (I/O 密集型) ---
    // 🚨 修正：接收转换后的数据 (String) 和原始 Event
    public CompletableFuture<Void> storeData(String transformedData, Event originalEvent) {
        return CompletableFuture.runAsync(() -> { // 无返回值，用 runAsync
            String threadName = Thread.currentThread().getName();
            System.out.println(threadName + " is storing data for event: " + originalEvent.traceId() + " (payload: " + transformedData.substring(0, Math.min(transformedData.length(), 20)) + "...)");
            try {
                Thread.sleep(100); // 模拟 I/O 延迟

                if (random.nextInt(100) < 5) { // 5% 概率抛出 StorageException
                    System.err.println(threadName + " Storage FAILED (StorageError) for event: " + originalEvent.traceId());
                    throw new StorageException("Simulated storage error for event: " + originalEvent.traceId());
                }

                // 🚨 关键：将原始事件和转换后的数据“存储”到缓存中
                // 存储一个包含转换后数据的新 Event 对象
                Event eventToStore = new Event(originalEvent.timestamp(), originalEvent.type(), transformedData, originalEvent.traceId());
                transformedDataCache.put(eventToStore.traceId(), eventToStore); // 存储到 ConcurrentHashMap

                System.out.println(threadName + " Storage **SUCCESS** for event: " + originalEvent.traceId() + ". Data cached.");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.err.println(threadName + " Storage INTERRUPTED for event: " + originalEvent.traceId());
                throw new RuntimeException("Storage interrupted", e);
            }
        }, IOTaskExecutor); // 存储是 I/O 密集型
    }

    // --- 模拟通知发送服务 (I/O 密集型) ---
    public CompletableFuture<Void> sendNotification(String eventId) {
        return CompletableFuture.runAsync(() -> {
            String threadName = Thread.currentThread().getName();
            System.out.println(threadName + " is sending notification for event ID: " + eventId + "...");
            try {
                Thread.sleep(20); // 模拟延迟

                if (random.nextInt(100) < 2) { // 2% 概率抛出 NotificationException
                    System.err.println(threadName + " Notification FAILED (NotificationError) for event ID: " + eventId);
                    throw new NotificationException("Simulated notification error for event ID: " + eventId);
                }
                System.out.println(threadName + " Notification **SENT** for event ID: " + eventId);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.err.println(threadName + " Notification INTERRUPTED for event ID: " + eventId);
                throw new RuntimeException("Notification interrupted", e);
            }
        }, IOTaskExecutor); // 通知是 I/O 密集型
    }

    // --- 可选：模拟 IP 地址到地理位置的解析 (新的异步 I/O 模拟服务，用于 thenCombine 演示) ---
    public CompletableFuture<String> geoLocateIp(String ipAddress) {
        return CompletableFuture.supplyAsync(() -> {
            String threadName = Thread.currentThread().getName();
            System.out.println(threadName + " is geo-locating IP: " + ipAddress + "...");
            try {
                Thread.sleep(80); // 模拟 I/O 延迟
                if (random.nextInt(100) < 10) { // 10% 概率失败
                    System.err.println(threadName + " Geo-location FAILED for IP: " + ipAddress);
                    throw new RuntimeException("Simulated geo-location failure for IP: " + ipAddress);
                }
                String location = "Location(" + ipAddress + ")_Country(" + (random.nextBoolean() ? "USA" : "JPN") + ")";
                System.out.println(threadName + " Geo-location **SUCCESS** for IP: " + ipAddress + " -> " + location);
                return location;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.err.println(threadName + " Geo-location INTERRUPTED for IP: " + ipAddress);
                throw new RuntimeException("Geo-location interrupted", e);
            }
        }, IOTaskExecutor);
    }

    // 🚨 修正：更名为 shutdownExecutors，并添加 awaitTermination 确保优雅关闭
    public void shutdownExecutors() {
        System.out.println("MockExternalService: Shutting down CPU and I/O executors...");
        cpuTaskExecutor.shutdown();
        IOTaskExecutor.shutdown();
        try {
            // 等待任务完成合理的时间
            if (!cpuTaskExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                cpuTaskExecutor.shutdownNow(); // 如果超时未终止，强制关闭
                System.err.println("MockExternalService: CPU executor did not terminate gracefully, forced shutdown.");
            }
            if (!IOTaskExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                IOTaskExecutor.shutdownNow(); // 如果超时未终止，强制关闭
                System.err.println("MockExternalService: I/O executor did not terminate gracefully, forced shutdown.");
            }
            System.out.println("MockExternalService: All executors shut down.");
        } catch (InterruptedException e) {
            cpuTaskExecutor.shutdownNow();
            IOTaskExecutor.shutdownNow();
            Thread.currentThread().interrupt(); // 恢复中断状态
            System.err.println("MockExternalService: Executor shutdown interrupted.");
        }
    }

    // 获取缓存数据以便在 PipelineLauncher 中验证
    public ConcurrentHashMap<String, Event> getTransformedDataCache() {
        return transformedDataCache;
    }
}