package site.hnfy258.demo3.exception;

public class NetworkException extends RuntimeException {
    public NetworkException(String message) {
        super(message);
    }
}