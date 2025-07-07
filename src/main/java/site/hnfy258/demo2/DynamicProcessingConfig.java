package site.hnfy258.demo2;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DynamicProcessingConfig {
    private volatile boolean processingEnabled;
    private volatile int batchSize;
    private Map<String,Boolean> featureFlags;
    private ReadWriteLock lock = new ReentrantReadWriteLock();
    private Lock readLock= lock.readLock();
    private Lock writeLock = lock.writeLock();


    public DynamicProcessingConfig(){
        this.processingEnabled = true; // 默认启用处理
        this.batchSize = 100; // 默认批处理大小
        this.featureFlags = new HashMap<>();
        featureFlags.put("featureA_enabled",true);
        featureFlags.put("featureB_enabled",false);
    }

    public boolean isProcessingEnabled(){
        readLock.lock();
        try{
            return processingEnabled;
        }finally {
            readLock.unlock();
        }
    }

    public int getBatchSize(){
        readLock.lock();
        try{
            return batchSize;
        }finally {
            readLock.unlock();
        }
    }
    public Boolean isFeatureEnabled(String featureName) {
        readLock.lock();
        try {
            return featureFlags.getOrDefault(featureName, false);
        } finally {
            readLock.unlock();
        }
    }

    public Map<String, Boolean> getAllFeatureFlags() {
        readLock.lock();
        try {
            return Collections.unmodifiableMap(new HashMap<>(featureFlags));
        } finally {
            readLock.unlock();
        }
    }

    public void updateAllConfig(boolean enabled, int newBatchSize, Map<String, Boolean> newFlags) {
        writeLock.lock();
        try {
            this.processingEnabled = enabled;
            if (newBatchSize > 0) {
                this.batchSize = newBatchSize;
            } else {
                throw new IllegalArgumentException("New batch size must be greater than 0");
            }
            this.featureFlags = new HashMap<>(newFlags != null ? newFlags : Map.of());
        } finally {
            writeLock.unlock();
        }
    }

    public void updateProcessingEnabled(boolean enabled){
        writeLock.lock();
        try{
            this.processingEnabled = enabled;
        }finally {
            writeLock.unlock();
        }
    }

    public void updateBatchSize(int newSize){
        writeLock.lock();
        try{
            if(newSize > 0) {
                this.batchSize = newSize;
            } else {
                throw new IllegalArgumentException("Batch size must be greater than 0");
            }
        }finally {
            writeLock.unlock();
        }
    }

    public void setFeatureFlag(String featureName, boolean enabled) {
        writeLock.lock();
        try {
            this.featureFlags.put(featureName, enabled);
        } finally {
            writeLock.unlock();
        }
    }

    public void printConfig() {
        readLock.lock();
        try {
            System.out.println("\n--- Current Processing Config ---");
            System.out.println("Processing Enabled: " + processingEnabled);
            System.out.println("Batch Size: " + batchSize);
            System.out.println("Feature Flags: " + featureFlags);
            System.out.println("---------------------------------");
        } finally {
            readLock.unlock();
        }
    }
}
