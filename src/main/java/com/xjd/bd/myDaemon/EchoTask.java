package com.xjd.bd.myDaemon;

import java.util.Date;
import java.util.TimerTask;

/**
 * Project : big-data-example
 * PackageName : com.cnit.dlp.myDaemon
 * Author : Xu Jiandong
 * CreateTime : 2017-07-11 13:41:00
 * ModificationHistory :
 */
class EchoTask extends TimerTask {
    @Override
    public void run() {
        System.out.println(new Date() + " running ...");
    }
}
