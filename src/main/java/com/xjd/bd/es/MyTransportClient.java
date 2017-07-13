package com.xjd.bd.es;

import org.apache.kafka.connect.errors.ConnectException;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;

/**
 * Created by root on 5/13/17.
 */
public class MyTransportClient {
    public static void main(String[] args){
        TransportClient client = null;
        try {
            Settings settings = Settings.builder()
                    .put("cluster.name", "dlp_es").build();
            client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("es-master-136"), 9300));
            String json = "{" +
                    "\"content\": \"测试测试啊\"" +
                    "}";
            IndexResponse response = client.prepareIndex("test-ik", "fulltext")
                    .setSource(json)
                    .get();
        } catch (Exception e) {
            throw new ConnectException("Impossible to connect to hosts");
        }finally {
            if (client != null)
                client.close();
        }
    }
}

