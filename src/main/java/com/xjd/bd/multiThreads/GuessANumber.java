package com.xjd.bd.multiThreads;

/**
 * Project : big-data-example
 * PackageName : com.cnit.dlp.multiThreads
 * Author : Xu Jiandong
 * CreateTime : 2017-06-29 09:38:00
 * ModificationHistory :
 */
public class GuessANumber extends Thread{
    private int number;
    public GuessANumber(int number){
        this.number = number;
    }

    public void run(){
        int counter = 0;
        int guess = 0;
        do{
            guess = (int) (Math.random() * 100 + 1);
            System.out.println(this.getName() + " guess" + guess);
            counter++;
        }while (guess != number);
        System.out.println("** correct!" + this.getName() + "in" + counter + "guesses.**");
    }
}
