package site.hnfy258.demo2;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class DeadLockDemo {
    private ReentrantLock lockA = new ReentrantLock();
    private ReentrantLock lockB = new ReentrantLock();
    private Random random =new Random();

    private void safeUnlock(ReentrantLock lock){
        //虽然可重入锁已经记录了exclusiveThread但是还是需要这个方法
        //这个方法避免了抛出异常，提前做了检验
        if(lock.isHeldByCurrentThread()){
            lock.unlock();
        }
    }

    public void methodA() {
        String threadName = Thread.currentThread().getName();
        System.out.println(threadName + " methodA: 开始尝试获取 lockA 和 lockB");
        while (true) { // 循环重试，直到成功获取所有锁或被中断
            lockA.lock(); // 总是先获取 lockA
            try {
                System.out.println(threadName + " methodA: 成功获取 lockA，尝试获取 lockB...");
                // 尝试获取 lockB，带超时
                if (lockB.tryLock(random.nextInt(100) + 1, TimeUnit.MILLISECONDS)) { // 随机短时间等待
                    try {
                        System.out.println(threadName + " methodA: 成功获取 lockB，执行业务逻辑...");
                        // 业务逻辑（模拟耗时）
                        Thread.sleep(random.nextInt(50) + 10);
                        System.out.println(threadName + " methodA: 业务逻辑完成。");
                        return; // 成功获取所有锁并完成业务，退出循环
                    } finally {
                        safeUnlock(lockB); // 释放 lockB
                    }
                } else {
                    // 未能获取 lockB，执行回退逻辑：释放 lockA 并重试
                    System.out.println(threadName + " methodA: 未能获取 lockB，释放 lockA，稍后重试。");
                    // 模拟等待一段时间再重试，防止CPU空转
                    Thread.sleep(random.nextInt(10) + 1); // 随机等待1-10ms
                }
            } catch (InterruptedException e) {
                System.out.println(threadName + " methodA: 被中断，放弃获取锁并退出。");
                Thread.currentThread().interrupt(); // 重新设置中断标志
                return; // 退出方法
            } finally {
                safeUnlock(lockA); // 确保 lockA 总是被释放
            }
        }
    }

    public void methodB() {
        String threadName = Thread.currentThread().getName();
        System.out.println(threadName + " methodB: 开始尝试获取 lockB 和 lockA");
        while (true) {
            lockB.lock();
            try {
                System.out.println(threadName + " methodB: 成功获取 lockB，尝试获取 lockA...");
                if (lockA.tryLock(random.nextInt(100) + 1, TimeUnit.MILLISECONDS)) {
                    try {
                        System.out.println(threadName + " methodB: 成功获取 lockA，执行业务逻辑...");
                        Thread.sleep(random.nextInt(50) + 10);
                        System.out.println(threadName + " methodB: 业务逻辑完成。");
                        return;
                    } finally {
                        safeUnlock(lockA);
                    }
                } else {

                    System.out.println(threadName + " methodB: 未能获取 lockA，释放 lockB，稍后重试。");
                    // 模拟等待一段时间再重试
                    Thread.sleep(random.nextInt(10) + 1);
                }
            } catch (InterruptedException e) {
                System.out.println(threadName + " methodB: 被中断，放弃获取锁并退出。");
                Thread.currentThread().interrupt();
                return;
            } finally {
                safeUnlock(lockB);
            }
        }
    }

    public static void main(String[] args) {
        DeadLockDemo deadLockDemo = new DeadLockDemo();
        Thread threadA = new Thread(deadLockDemo::methodA, "Thread-A");
        Thread threadB = new Thread(deadLockDemo::methodB, "Thread-B");

        threadA.start();
        threadB.start();

        try {
            Thread.sleep(1000);

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
