package com.xjd.bd.myHadoop;

/**
 * Project : big-data-example
 * PackageName : com.cnit.dlp.myHadoop
 * Author : Xu Jiandong
 * CreateTime : 2017-06-20 18:52:00
 * ModificationHistory :
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.lang.reflect.Method;
import java.net.URI;

public class MyHadoop {


    public static void main(String[] args) throws Exception{
        //downloadLog();
        //downloadNetflow();
        String uri="hdfs://10.20.10.198:8020/";   //hdfs 地址
        String local="/data/";  //本地路径
        String remote="hdfs://10.20.10.198:8020/cnit/dlp/file-parse-txt/2017-06-20/198.168.101.127-.1497960561207.seq";
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        fs.setReplication(new Path(remote), (short)2);
        System.out.println(fs.getDefaultReplication(new Path(remote)));
        MyHadoop myHadoop = new MyHadoop();
        Method refGetDefaultReplication =  myHadoop.reflectGetDefaultReplication(fs);
        Path destPath = new Path(remote);
        short replication = (Short) refGetDefaultReplication.invoke(fs, destPath);
        System.out.println(replication);
        ContentSummary cSummary = fs.getContentSummary(destPath);
        long length = cSummary.getLength();
        System.out.println(cSummary);
        System.out.println(length/(1024));
        /*Path dataset = new Path(fs.getHomeDirectory(), destPath);
        FileStatus datasetFile = fs.getFileStatus(dataset);


        BlockLocation myBlocks [] = fs.getFileBlockLocations(datasetFile,0,datasetFile.getLen());
        for(BlockLocation b : myBlocks){
            System.out.println("Length "+b.getLength());
            for(String host : b.getHosts()){
                System.out.println("host "+host);
            }
        }*/


    }

    private Method reflectGetDefaultReplication(FileSystem fileSystem) {
        Method m = null;
        if (fileSystem != null) {
            Class<?> fsClass = fileSystem.getClass();
            try {
                m = fsClass.getMethod("getDefaultReplication",
                        new Class<?>[] { Path.class });
            } catch (NoSuchMethodException e) {
                System.out.println("FileSystem implementation doesn't support"
                        + " getDefaultReplication(Path); -- HADOOP-8014 not available; " +
                        "className = " + fsClass.getName() + "; err = " + e);
            } catch (SecurityException e) {
                System.out.println("No access to getDefaultReplication(Path) on "
                        + "FileSystem implementation -- HADOOP-8014 not available; " +
                        "className = " + fsClass.getName() + "; err = " + e);
            }
        }
        if (m != null) {
            System.out.println("Using FileSystem.getDefaultReplication(Path) from " +
                    "HADOOP-8014");
        }
        return m;
    }
}
