package com.xjd.bd.myCallable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Project : big-data-example
 * PackageName : com.cnit.dlp.myCallable
 * Author : Xu Jiandong
 * CreateTime : 2017-07-06 13:44:00
 * ModificationHistory :
 */
public class MyCallable {

    public static void main(String[] args){
        MyCallable myCallable = new MyCallable();
        //myCallable.runMythread1();

        myCallable.runThreadPool();
    }


    public void runMythread1()   {
        MyThread1 myThread1 = new MyThread1("1");
        FutureTask<Integer> futureTask = new FutureTask<Integer>(myThread1);
        new Thread(futureTask, "线程名：有返回值的线程").start();
        try{
            System.out.println("子线程的返回值：" + futureTask.get(1, TimeUnit.SECONDS));
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            e.printStackTrace();
        }

    }

    public void runThreadPool() {
        ExecutorService executorService = Executors.newFixedThreadPool(5);

        List<FutureTask<Integer>> futureTasks = new ArrayList<FutureTask<Integer>>();

        for(int i=0; i<10; i++){
            //创建一个异步任务
            FutureTask<Integer> futureTask = new FutureTask<Integer>(new MyThread1("123"));
            futureTasks.add(futureTask);
            //提交异步任务到线程池，让线程池管理任务 特爽把。
            //由于是异步并行任务，所以这里并不会阻塞
            executorService.submit(futureTask);
        }



            for (FutureTask<Integer> futureTask : futureTasks) {
                //futureTask.get() 得到我们想要的结果
                //该方法有一个重载get(long timeout, TimeUnit unit) 第一个参数为最大等待时间，第二个为时间的单位
                try{

                    System.out.println(futureTask.get(10000, TimeUnit.MILLISECONDS));
                }catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                } catch (TimeoutException e) {
                    //定义超时后的状态修改
                    System.out.println("thread time out");
                    e.printStackTrace();
                    futureTask.cancel(true);
                }

            }

        executorService.shutdown();

    }
}



class BaseAccount{

}


class MyThread1 extends BaseAccount implements Callable<Integer>{
    private String res;
    public MyThread1(String res){
        this.res = res;
    }
    @Override
    public Integer call() throws Exception {
        System.out.println("Thread Name = " + Thread.currentThread().getName());
        int i = 0;
        for(; i < 5; i++){
            System.out.println("*********** i = " + i + res);
        }
        try {

            Thread.sleep(500);
        }catch (InterruptedException e){
            e.printStackTrace();
        }
        return 100;
    }
}
