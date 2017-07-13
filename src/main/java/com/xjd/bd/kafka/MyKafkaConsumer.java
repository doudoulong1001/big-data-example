package com.xjd.bd.kafka;

/**
 * Created by root on 5/5/17.
 */

import com.xjd.bd.commons.JsonToAvro;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.io.File;
import java.util.Collections;
import java.util.Properties;

public class MyKafkaConsumer {
    private static final String USER_SCHEMA = "{"
            + "\"type\":\"record\","
            + "\"name\":\"myrecord\","
            + "\"fields\":["
            + "{ \"name\":\"content\", \"type\":\"string\" }"
            + "]}";
    public static void main(String[] args) throws Exception {
        JsonToAvro jsonToAvro = new JsonToAvro();
        //Kafka consumer configuration settings
        String topicName = "test-elasticsearch-sink";
        Properties props = new Properties();
        props.put("auto.offset.reset", "latest");
        props.put("bootstrap.servers", "kafka01:9092");
        props.put("group.id", "test-elasticsearch-sink");
        //自动提交 offset
        props.put("enable.auto.commit", "true");
        //自动提交间隔
        props.put("auto.commit.interval.ms", "1000");
        //心跳定时的时长
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        //props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        Schema.Parser parser = new Schema.Parser();
        File file = new File("/data/project/study/big-data-example/src/main/resources/accident.avsc");
        Schema schema = parser.parse(USER_SCHEMA);
        schema = new Schema.Parser().parse(file);
        Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);
        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props);

        //Kafka Consumer subscribes list of topics here.
        consumer.subscribe(Collections.singletonList(topicName));

        //print the topic name
        System.out.println("Subscribed to topic " + topicName);
        try{
            while (true) {
                ConsumerRecords<String, byte[]> records = consumer.poll(100);
                for (ConsumerRecord<String, byte[]> record : records) {

                    System.out.printf("offset = %d, key = %s, value = %s \n", record.offset(), record.key(), (record.value()));
                    //GenericRecord res = recordInjection.invert(record.value()).get();

                    //System.out.println(res.get("fileSize"));
                    /*byte[] bytes = record.value();
                    String json = jsonToAvro.avroToJson(bytes, schema);
                    System.out.println(json);*/

                }
            }
        }catch (WakeupException ignored){
        }finally {
            consumer.close();
        }
    }
}
