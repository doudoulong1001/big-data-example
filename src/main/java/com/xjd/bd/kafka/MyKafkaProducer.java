package com.xjd.bd.kafka;

/**
 * Created by root on 5/16/17.
 */

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;


import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;


public class MyKafkaProducer {
    private static final String USER_SCHEMA = "{"
            + "\"type\":\"record\","
            + "\"name\":\"myrecord\","
            + "\"fields\":["
            + "{ \"name\":\"content\", \"type\":[\"null\", \"string\"]},"
            + "{\"name\":\"facility\",\"type\":[\"null\", \"string\"]},"
            + "{\"name\":\"fileSize\",\"type\":[\"null\", \"long\"]}"
            + "]}";
    public static void main(String [] args) throws InterruptedException, IOException, ParseException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "x-master:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(USER_SCHEMA);

        /*File file = new File("/data/project/study/big-data-example/src/main/resources/accident.avsc");
        Schema schema = new Schema.Parser().parse(file);*/
        Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);
        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props);

        for (int i = 0; i < 1; i++) {
            GenericData.Record avroRecord = new GenericData.Record(schema);
            avroRecord.put("content", "get");
            String ctm = "2017-02-18 20:00:00";
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            long millionSeconds = sdf.parse(ctm).getTime()/1000;
            System.out.println(millionSeconds);
            //millionSeconds = 1495074938;

            avroRecord.put("fileSize", millionSeconds);

            byte[] bytes = recordInjection.apply(avroRecord);

            ProducerRecord<String, byte[]> record = new ProducerRecord<>("avro-test-20170519", bytes);
            producer.send(record/*,
                    (metadata, e) -> {
                        if(e != null)
                            e.printStackTrace();
                        System.out.println("The offset of the record we just sent is: " + metadata.offset());
                    }*/);
            Thread.sleep(250);
        }
        System.out.println("thank you very much");

        producer.close();
    }
}
