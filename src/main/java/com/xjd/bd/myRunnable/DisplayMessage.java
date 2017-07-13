package com.xjd.bd.myRunnable;


/**
 * Project : big-data-example
 * PackageName : com.cnit.dlp.myRunnable
 * Author : Xu Jiandong
 * CreateTime : 2017-06-29 09:35:00
 * ModificationHistory :
 */
public class DisplayMessage implements Runnable{
    private String message;
    public DisplayMessage(String message){
        this.message = message;
    }
    @Override
    public void run() {
        while (true){
            System.out.println(message);
        }
    }
}
