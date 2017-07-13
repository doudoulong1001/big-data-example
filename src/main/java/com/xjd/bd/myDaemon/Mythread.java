package com.xjd.bd.myDaemon;

/**
 * Created by root on 5/11/17.
 */
public class Mythread extends Thread{
    private int ticket = 5;

    /**
     * @param
     */
    @Override
    public void run(){
        for (int i = 0; i < 10; i++){
            if (ticket > 0){
                System.out.println("ticket = " + ticket--);
                try {

                    Thread.sleep(10*1000);
                }catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
