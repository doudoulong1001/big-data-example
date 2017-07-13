package com.xjd.bd.myRunnable;

/**
 * Created by root on 5/11/17.
 */
public class MyRunnable implements Runnable {
    private int ticket = 5;

    /**
     * 需要接线程锁，不然会出现小于0的问题
     */

    @Override
    public synchronized void run(){
        for (int i = 0; i < 10; i++){
            if (ticket > 0){
                System.out.println("ticket = " + ticket--);
            }
        }
    }
}
