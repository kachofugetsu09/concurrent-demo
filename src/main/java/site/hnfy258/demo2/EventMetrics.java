package site.hnfy258.demo2;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class EventMetrics {
    private final AtomicLong totalEventsProcessed = new AtomicLong(0);
    private final AtomicLong eventErrorCount = new AtomicLong(0);

    private final Map<String,AtomicLong> eventCategoryCounts;

    public EventMetrics(Map<String,AtomicLong> initMap){
        this.eventCategoryCounts = initMap;
    }

    public long incrementProcessedCount(){
        return totalEventsProcessed.incrementAndGet();
    }

    public long incrementErrorCount(){
        return eventErrorCount.incrementAndGet();
    }

    public Map<String,AtomicLong> getEventCategoryCounts() {
        return eventCategoryCounts;
    }

    public void incrementEventCategoryCount(String category) {
        AtomicLong count =eventCategoryCounts.computeIfAbsent(category,k -> new AtomicLong(0L));
        count.incrementAndGet();
    }
    public long getTotalEventsProcessed() {
        return totalEventsProcessed.get();
    }

    public long getEventErrorCount() {
        return eventErrorCount.get();
    }

    // 打印当前统计数据
    public void printMetrics() {
        System.out.println("\n--- Current Event Metrics ---");
        System.out.println("Total Events Processed: " + totalEventsProcessed.get());
        System.out.println("Event Error Count: " + eventErrorCount.get());
        System.out.println("Event Category Counts: " + eventCategoryCounts);
        System.out.println("----------------------------");
    }

}
