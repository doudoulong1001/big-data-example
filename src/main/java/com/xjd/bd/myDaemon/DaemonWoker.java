package com.xjd.bd.myDaemon;

/**
 * Project : data-monitor
 * PackageName : com.zcah.sh.quartzJobs
 * Author : Xu Jiandong
 * CreateTime : 2017-07-11 09:41:00
 * ModificationHistory :
 */
public class DaemonWoker extends Thread{

    public volatile Boolean isStopped;
    @Override
    public void run(){
        isStopped = true;
        while (isStopped){
            try{
                Thread.sleep(1000);
            }catch (InterruptedException e){
                e.printStackTrace();
            }
            if (Thread.interrupted()) {
                return;
            }
        }
    }
    public void stopRunning() {
        isStopped = false;
    }
}
