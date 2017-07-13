package com.xjd.bd.myRunnable;

/**
 * Created by root on 5/11/17.
 */
public class RunnableDemo {
    public static void main(String [] args){
        MyRunnable myRunnable = new MyRunnable();
        new Thread(myRunnable).start();
        new Thread(myRunnable).start();
        new Thread(myRunnable).start();
        String str = "{\"namespace\":\"kakfa.avro.test\",\"type\":\"record\",\"name\":\"myrecord\",\"fields\":[{\"name\":\"content\",\"type\":\"string\"}]}";
    }
}
