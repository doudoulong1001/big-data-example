package com.xjd.bd.multiThreads;

import com.xjd.bd.myRunnable.DisplayMessage;

/**
 * Project : big-data-example
 * PackageName : com.cnit.dlp.multiThreads
 * Author : Xu Jiandong
 * CreateTime : 2017-06-29 09:46:00
 * ModificationHistory :
 */
public class ThreadClassDemo {

    /**
     * <p><b>description:</b><br>
     * <p><b>creatTime:</b> 3:26 PM 6/29/17<br>
     * @author: Xu Jiandong
     * @param args main args
     */
    public static void main(String[] args) {
        Runnable hello = new DisplayMessage("hello");
        Thread thread1 = new Thread(hello);
        thread1.setDaemon(true);
        thread1.setName("hello");
        System.out.println("starting hello thread ...");
        thread1.start();

        Runnable bye = new DisplayMessage("Goodbye");
        Thread thread2 = new Thread(bye);
        thread2.setPriority(Thread.MIN_PRIORITY);
        thread2.setDaemon(true);
        System.out.println("starting goodbye thread ...");
        thread2.start();

        System.out.println("Starting thread3...");
        Thread thread3 = new GuessANumber(27);
        thread3.start();

        try {
            thread3.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Starting thread4 ... ");
        Thread thread4 = new GuessANumber(75);
        thread4.start();
        System.out.println("main() is running");

    }


    /**
     * <p><b>description:</b><br>
     *     DOC 说明
     * <p><b>creatTime:</b> 3:21 PM 6/29/17<br>
     * @author: Xu Jiandong
     * @param param1 不知
     * @param param2 测试
     * @return "1"
     */
    public String testTemplate(String param1, int param2){
        System.out.println(param1 + param2);
        return "1";
    }
}
