package site.hnfy258.demo2;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class ConfigUpdater implements Runnable{
    private final DynamicProcessingConfig config;
    private Random random;
    public ConfigUpdater(DynamicProcessingConfig config) {
        this.config = config;
        this.random = new Random();
    }
    @Override
    public void run() {
        while(!Thread.currentThread().isInterrupted()){
            try{
                Thread.sleep(5000);

                boolean newEnabled = random.nextBoolean();
                int newBatchSize = random.nextInt(50) + 50; // 50到100之间的随机数
                String newFeature = "feature" + (random.nextInt(5) + 1) + "_enabled";

                Map<String,Boolean> updateFlags =config.getAllFeatureFlags();
                Map<String,Boolean> newFlags = new HashMap<>(updateFlags);
                newFlags.put(newFeature,newEnabled);
                config.updateAllConfig(newEnabled, newBatchSize, newFlags);
                config.printConfig();
            }catch(InterruptedException e){
                Thread.currentThread().interrupt();
                System.out.println("ConfigUpdater interrupted, stopping updates.");
                break;
            } catch (Exception e) {
                System.err.println("Error updating config: " + e.getMessage());
            }
        }
        System.out.println("ConfigUpdater has stopped.");
    }
}
