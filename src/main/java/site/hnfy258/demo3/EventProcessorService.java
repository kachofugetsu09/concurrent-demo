package site.hnfy258.demo3;

import site.hnfy258.demo1.Event;
import site.hnfy258.demo3.exception.*; // 导入你的自定义异常包
import site.hnfy258.demo4.ConcurrentBlocklist;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException; // 用于处理 CompletableFuture 包装的异常

public class EventProcessorService {
    private final MockExternalService mockExternalService;
    private final ConcurrentBlocklist blackList;

    public EventProcessorService(MockExternalService mockExternalService,
                                 ConcurrentBlocklist blackList) {
        this.mockExternalService = mockExternalService;
        this.blackList = blackList;

    }

    public CompletableFuture<ProcessingResult> processEvent(Event event) {
        System.out.println("Processor: Starting processing for event: " + event.traceId());
        if(blackList.contains(event.traceId())){
            System.out.println("Processor: Event " + event.traceId() + " is in the blacklist. Skipping processing.");
            return CompletableFuture.completedFuture(
                    new ProcessingResult(event, false, "Event is blacklisted", Collections.emptyMap()));
        }

        return mockExternalService.validateEvent(event)
                .thenCompose(isValid -> {
                    if (isValid) {
                        System.out.println("Processor: Validation SUCCESS for event: " + event.traceId() + ". Proceeding to transform.");
                        return mockExternalService.transformData(event)
                                .thenCompose(transformedData -> {
                                    System.out.println("Processor: Transformation SUCCESS for event: " + event.traceId() + ". Proceeding to store.");

                                    return mockExternalService.storeData(transformedData, event)
                                            .thenCompose(v -> {
                                                System.out.println("Processor: Storage SUCCESS for event: " + event.traceId() + ". Proceeding to notify.");

                                                return mockExternalService.sendNotification(event.traceId())
                                                        .thenApply(nv -> {
                                                            System.out.println("Processor: Notification SUCCESS for event: " + event.traceId() + ". ALL STEPS COMPLETE.");
                                                            Map<String, Object> data = new HashMap<>();
                                                            data.put("transformedPayload", transformedData);
                                                            return new ProcessingResult(event, true, null, data);
                                                        })
                                                        .exceptionally(ex -> {
                                                            Throwable actualEx = (ex instanceof CompletionException) ? ex.getCause() : ex;
                                                            String errorMsg = "Notification failed: " + actualEx.getMessage();
                                                            System.err.println("Processor: " + errorMsg + " for event: " + event.traceId());
                                                            return new ProcessingResult(event, false, errorMsg, Collections.emptyMap());
                                                        });
                                            })
                                            .exceptionally(ex -> {
                                                Throwable actualEx = (ex instanceof CompletionException) ? ex.getCause() : ex;
                                                String errorMsg = "Storage failed: " + actualEx.getMessage();
                                                System.err.println("Processor: " + errorMsg + " for event: " + event.traceId());
                                                return new ProcessingResult(event, false, errorMsg, Collections.emptyMap());
                                            });
                                })
                                .exceptionally(ex -> {
                                    Throwable actualEx = (ex instanceof CompletionException) ? ex.getCause() : ex;
                                    String errorMsg = "Transformation failed: " + actualEx.getMessage();
                                    System.err.println("Processor: " + errorMsg + " for event: " + event.traceId());
                                    return new ProcessingResult(event, false, errorMsg, Collections.emptyMap());
                                });
                    } else {
                        System.out.println("Processor: Validation FAILED (result: FALSE) for event: " + event.traceId() + ". Stopping chain.");
                        return CompletableFuture.completedFuture(
                                new ProcessingResult(event, false, "Validation failed: Event is invalid", Collections.emptyMap()));
                    }
                })
                .exceptionally(ex -> {
                    Throwable actualEx = (ex instanceof CompletionException) ? ex.getCause() : ex;
                    String errorMsg = "Validation failed: " + actualEx.getMessage();
                    System.err.println("Processor: " + errorMsg + " for event: " + event.traceId());
                    return new ProcessingResult(event, false, errorMsg, Collections.emptyMap());
                })

                .exceptionally(ex -> {
                    Throwable actualEx = (ex instanceof CompletionException) ? ex.getCause() : ex;
                    String errorMsg = "Event processing failed due to an unexpected error in pipeline: " + actualEx.getMessage();
                    System.err.println("Processor: " + errorMsg + " for event: " + event.traceId());
                    return new ProcessingResult(event, false, errorMsg, Collections.emptyMap());
                });
    }
}