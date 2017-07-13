package com.xjd.bd.myDaemon;

import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.daemon.DaemonInitException;

import java.util.Timer;
/**
 * Project : big-data-example
 * PackageName : com.cnit.dlp.myDaemon
 * Author : Xu Jiandong
 * CreateTime : 2017-07-11 13:42:00
 * ModificationHistory :
 */
public class Main implements Daemon {

    private static Timer timer = null;
    private Mythread mythread = new Mythread();
    public static void main(String[] args) throws Exception {
        timer = new Timer();
        timer.schedule(new EchoTask(), 0, 1000);
        /*Main main = new Main();
        main.start();*/
    }

    @Override
    public void init(DaemonContext dc) throws DaemonInitException, Exception {
        System.out.println("initializing ...");

    }

    @Override
    public void start() throws Exception {
        System.out.println("starting ...");
        //main(null);
        mythread.start();

    }

    @Override
    public void stop() throws Exception {
        System.out.println("stopping ...");
        /*if (timer != null) {
            timer.cancel();
        }*/
        mythread.interrupt();
    }

    @Override
    public void destroy() {
        System.out.println("done.");
    }

}
