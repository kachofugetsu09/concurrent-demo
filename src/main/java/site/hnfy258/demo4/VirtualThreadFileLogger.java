package site.hnfy258.demo4;

import java.io.File;
import java.io.FileOutputStream;

public class VirtualThreadFileLogger {
    private static final String FILENAME = "virtual_thread_log.txt";

   public VirtualThreadFileLogger(){
       File logFile = new File(FILENAME);
       if(logFile.exists()){
           System.out.println("Log file already exists, no need to create a new one.");
       }
       else{
           try{
               logFile.createNewFile();
               System.out.println("Log file created successfully: " + FILENAME);
           }catch (Exception e){
               System.out.println("Failed to create log file: " + e.getMessage());
           }
       }
   }

   public void log(String logMessage){
       Thread.ofVirtual().start(()->{
           String threadName = Thread.currentThread().getName();
           //启用追加模式
           try(FileOutputStream fos = new FileOutputStream(FILENAME,true)){
               fos.write((logMessage+"\n").getBytes());
               fos.flush();
           }catch (Exception e){
                System.err.println(threadName + " - Failed to write log: " + e.getMessage());
                e.printStackTrace();
           }
       });
   }
}

