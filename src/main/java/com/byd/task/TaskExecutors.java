package com.byd.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author wang.yonggui1
 * @date 2021-10-30 14:40
 */
public class TaskExecutors {

    private static Logger log= LoggerFactory.getLogger(TaskExecutors.class);

    public static final ExecutorService CORE_THREAD_POOL=new ThreadPoolExecutor(10,
            50,
            60L,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(1000),
            new NamedThreadFactory("Bigdata-CorePool",false),
            new ThreadPoolExecutor.AbortPolicy()
            );



    public static void main(String[] args) throws InterruptedException {


    }

    public static void shutdown(){
        CORE_THREAD_POOL.shutdown();
        try {
            CORE_THREAD_POOL.awaitTermination(60,TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.error("shutdown error:",e);
        }
    }

    public static Runnable task2Runable(Task task){
        Runnable runnable = () -> task.execute();
        return runnable;
    }

    public static void addTask(Task task){
        CORE_THREAD_POOL.execute(task2Runable(task));
    }

    public static Future<?> submitTask(Task task){
        return CORE_THREAD_POOL.submit(task2Runable(task));
    }


    public static class NamedThreadFactory implements ThreadFactory {

        private String poolName;
        private boolean isDaemon;
        private AtomicInteger incre=new AtomicInteger();

        public NamedThreadFactory(String poolName,boolean isDaemon){
            this.poolName=poolName;
            this.isDaemon=isDaemon;
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r);
            thread.setName(poolName+"-"+incre.getAndIncrement());
            thread.setDaemon(isDaemon);
            return thread;
        }

        public String getPoolName() {
            return poolName;
        }

        public void setPoolName(String poolName) {
            this.poolName = poolName;
        }

        public boolean isDaemon() {
            return isDaemon;
        }

        public void setDaemon(boolean daemon) {
            isDaemon = daemon;
        }
    }

}
