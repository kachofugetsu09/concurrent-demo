package site.hnfy258.demo3;

import site.hnfy258.demo1.Event;

import java.util.Map;

public record ProcessingResult(Event originalEvent,
                               boolean success,
                               String errorMessage,
                               Map<String,Object> processedDate) {
}
