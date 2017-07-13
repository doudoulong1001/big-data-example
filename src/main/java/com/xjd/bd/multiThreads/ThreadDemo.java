package com.xjd.bd.multiThreads;

/**
 * Created by root on 5/11/17.
 */
public class ThreadDemo {
    public static void main(String [] args){
        new Mythread().start();
        new Mythread().start();
        new Mythread().start();
    }

}
