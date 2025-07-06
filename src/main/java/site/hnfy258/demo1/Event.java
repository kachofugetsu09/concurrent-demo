package site.hnfy258.demo1;

public record Event(long timestamp,
                    String type,
                    String payload,
                    String traceId) {

}
