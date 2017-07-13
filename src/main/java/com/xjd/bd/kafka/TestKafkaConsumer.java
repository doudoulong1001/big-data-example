package com.xjd.bd.kafka;

import com.xjd.bd.commons.JsonToAvro;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.util.*;

/**
 * Created by root on 5/18/17.
 */
public class TestKafkaConsumer {
    private static final String USER_SCHEMA = "{"
            + "\"type\":\"record\","
            + "\"name\":\"myrecord\","
            + "\"fields\":["
            + "{ \"name\":\"content\", \"type\":[\"null\", \"string\"]},"
            + "{\"name\":\"facility\",\"type\":[\"null\", \"string\"]},"
            + "{\"name\":\"fileSize\",\"type\":[\"null\", \"long\"]}"
            + "]}";
    public static void main(String[] args) throws Exception {
        JsonToAvro jsonToAvro = new JsonToAvro();
        //Kafka consumer configuration settings
        String topicName = "avro-test-20170519";
        Properties props = new Properties();
        props.put("auto.offset.reset", "earliest");
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test-consumer-group-2");
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
        //File file = new File("/data/project/study/big-data-example/src/main/resources/accident.avsc");
        Schema schema = parser.parse(USER_SCHEMA);
        //schema = new Schema.Parser().parse(file);
        Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);
        KafkaConsumer<String,byte[]> consumer = new KafkaConsumer<>(props);

        //Kafka Consumer subscribes list of topics here.
        consumer.subscribe(Collections.singletonList(topicName));

        //print the topic name
        System.out.println("Subscribed to topic " + topicName);

        List<String> keyList = new ArrayList<>();
        for (int i = 0; i < schema.getFields().size(); i++) {
            String str = schema.getFields().get(i).toString().split(" ")[0];
            keyList.add(str);
        }

        try{
            while (true) {
                ConsumerRecords<String, byte[]> records = consumer.poll(100);
                for (ConsumerRecord<String, byte[]> record : records) {

                    System.out.printf("offset = %d, key = %s, value = %s \n", record.offset(), record.key(), (record.value()));
                    GenericRecord res = recordInjection.invert(record.value()).get();


                    Map<String, Object> map = new HashMap<>();
                    for (String key: keyList){
                        map.put(key, res.get(key));
                    }

                    /*System.out.println(res.get("fileSize"));*/



                    /*byte[] bytes = record.value()*//*.getBytes()*//*;
                    System.out.println(Arrays.toString(bytes));
                    String json = jsonToAvro.avroToJson(bytes, schema);
                    System.out.println(json);
//                    jsonToAvro.avroToJsonByClass(bytes, schema);
                    jsonToAvro.myDeserialize(bytes, schema);*/
                }
            }
        }catch (WakeupException ignored){
        }finally {
            consumer.close();
        }
    }
}
