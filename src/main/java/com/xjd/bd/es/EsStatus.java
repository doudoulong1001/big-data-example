package com.xjd.bd.es;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.cnit.dlp.entity.StorageConfigureEntity;
import org.apache.commons.lang3.ArrayUtils;

import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.apache.commons.lang3.StringUtils;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.util.*;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

/**
 * Project : big-data-example
 * PackageName : com.cnit.dlp.es
 * Author : Xu Jiandong
 * CreateTime : 2017-07-13 09:20:00
 * ModificationHistory :
 */
public class EsStatus {

    private static final Logger logger = Logger.getLogger(EsStatus.class);
    private String configName;
    private Properties config;
    private String clusterIps;
    private int clusterHttpPort;
    private HttpHost[] hosts;
    private RestClient restClient;
    private TransportClient transportClient;
    private IndicesAdminClient indicesAdminClient;
    public  EsStatus() throws Exception {
        this.setUp();
    }
    /**
     * <p><b>description:</b><br>
     * <p><b>creatTime:</b> 11:51 AM 7/3/17<br>
     * @author: Xu Jiandong
     * @exception Exception
     */
    public void setUp() throws Exception {
        configName = "config.properties";
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        InputStream stream = loader.getResourceAsStream(configName);
        config = new Properties();
        try {
            config.load(stream);
        } catch (IOException e) {
            logger.error("加载config.properties配置文件错误");
            logger.error(e.getMessage());
        }
        clusterIps = config.getProperty("cluster.ips");
        if (StringUtils.isBlank(clusterIps)) {
            logger.error("cluster.ips 不能为空，请检查config.properties文件！");
            return;
        }
        String tmp = config.getProperty("cluster.http.port");
        if (StringUtils.isBlank(tmp)) {
            logger.warn("cluster.http.port 不能为空，请检查config.properties文件！");
            return;
        } else {
            try {
                clusterHttpPort = Integer.parseInt(tmp);
            } catch (Exception e) {
                logger.error("cluster.http.port  必须为数字！", e);
            }
        }
        String[] ips = StringUtils.split(clusterIps, ",");
        if (ArrayUtils.isEmpty(ips)) {
            logger.warn("cluster.ips 不能为空！");
            return;
        }
        hosts = new HttpHost[ips.length];
        for (int i = 0; i < ips.length; i++) {
            String ip = ips[i];
            HttpHost host = new HttpHost(ip, clusterHttpPort, "http");
            hosts[i] = host;
        }
        restClient = RestClient.builder(hosts).build();
        //transport client
        String clusterName = config.getProperty("cluster.name");
        try {
            Settings settings = Settings.builder().put("cluster.name", clusterName).build();
            transportClient = new PreBuiltTransportClient(settings);
            for (String host : ips) {
                String address;
                Integer port;
                String[] hostArray = host.split(":");
                address = hostArray[0];
                try {
                    port = Integer.parseInt(hostArray[1]);
                } catch (Exception e) {
                    port = 9300;
                }
                logger.info("address " + address + "port " + port);
                transportClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(address), port));
            }
        } catch (Exception e) {
            logger.log(Level.ERROR, e.getMessage(), e);
        }
        indicesAdminClient = transportClient.admin().indices();
    }
    /**
     * <p><b>description:</b><br>
     *     获取ES索引的存储节点分布
     * <p><b>creatTime:</b> 11:52 AM 7/3/17<br>
     * @author: Xu Jiandong
     * @param indexName 索引名称
     */
    public Set<String> getIndexNodes(String indexName){
        try {
            Map<String, String> nameIPMap = new HashMap<String, String>();
            Response nodeName = restClient.performRequest("GET", "_nodes/_all/name",
                    Collections.singletonMap("pretty", "true"));
            String nodeNameStr = EntityUtils.toString(nodeName.getEntity());
            JSONObject nodeNameJson = JSON.parseObject(nodeNameStr).getJSONObject("nodes");
            for (String key : nodeNameJson.keySet()) {
                JSONObject node = nodeNameJson.getJSONObject(key);
                nameIPMap.put(key, node.getString("ip"));
                //System.out.println(key + " : " + node.getString("ip"));
            }
            Response response = restClient.performRequest("GET", "/"+ indexName + "/_shard_stores?status=green",
                    Collections.singletonMap("pretty", "true"));
            String resJsonStr = EntityUtils.toString(response.getEntity());
            //logger.info("/"+ indexName + "/_shard_stores?status=green response = "   + resJsonStr);
            JSONObject resJson = JSON.parseObject(resJsonStr);
            JSONObject indices = resJson.getJSONObject("indices");
            //存放节点
            Set<String> ipSet = new HashSet<String>();
            for (String key : indices.keySet()){
                JSONObject shards = indices.getJSONObject(key).getJSONObject("shards");
                for (String key1 : shards.keySet()) {
                    JSONArray stores = shards.getJSONObject(key1).getJSONArray("stores");
                    int storesSize = stores.size();
                    for (int i = 0; i < storesSize; i++){
                        //System.out.println(ip);
                        stores.getJSONObject(i).keySet().stream().filter(key_1 -> !(key_1.equals("allocation") || key_1.equals("allocation_id"))).forEach(key_1 -> {
                            String ip = nameIPMap.get(key_1);
                            ipSet.add(ip);
                            //System.out.println(ip);
                        });
                    }
                }
            }
            logger.info("nodes = " + ipSet);
            return ipSet;
            //System.out.println(indices);
        }catch (IOException e){
            logger.error(e.getMessage());
        }
        return null;
    }
    /**
     * <p><b>description:</b><br>
     *     获取ES索引的存储大小
     * <p><b>creatTime:</b> 11:53 AM 7/3/17<br>
     * @author: Xu Jiandong
     * @param indexName 索引名称
     */
    public Float getIndexSize(String indexName) {
        Long totalSizeInBytes = 0L;
        Float unit = 1024*1024.0f;
        try{
            Response response = restClient.performRequest("GET", "/" + indexName + "/_stats/store",
                    Collections.singletonMap("pretty", "true"));
            String resStr = EntityUtils.toString(response.getEntity());
            /*logger.info("/" + indexName + "/_stats/store" + " response = "   + resStr);*/
            JSONObject resJson = JSON.parseObject(resStr);
            totalSizeInBytes = resJson.getJSONObject("_all")
                    .getJSONObject("total")
                    .getJSONObject("store")
                    .getLong("size_in_bytes");
            logger.info("totalSizeInBytes = " + totalSizeInBytes);
        }catch (IOException e){
            logger.error(e.getMessage());
        }
        return totalSizeInBytes/unit;
    }
    /**
     * <p><b>description:</b><br>
     *     close ES rest client
     * <p><b>creatTime:</b> 4:30 PM 7/3/17<br>
     * @author: Xu Jiandong
     *
     */
    public void closeClient(){
        if (this.restClient != null){
            try {
                this.restClient.close();
                this.transportClient.close();
            }catch (IOException e){
                logger.error(e.getMessage());
            }
        }
    }
    public boolean isExistIndex(String index){
        IndicesExistsRequest request = new IndicesExistsRequest(index);
        IndicesExistsResponse response = indicesAdminClient.exists(request).actionGet();
        return response.isExists();
    }

    /**
     * <p><b>description:</b><br>
     *     测试
     * <p><b>creatTime:</b> 11:55 AM 7/3/17<br>
     * @author: Xu Jiandong
     * @param args main 参数
     */
    public static void main(String[] args) throws Exception {
        EsStatus esStatus = new EsStatus();
        esStatus.getIndexNodes("twitter");
        esStatus.getIndexSize("twitter");
        boolean isExistIndex =  esStatus.isExistIndex("twitter");
        System.out.println(isExistIndex);
        esStatus.closeClient();
        /*StorageConfigureEntity cfg = new StorageConfigureEntity();
        Set<String> ipSet = new HashSet<String>();
        ipSet.add("10.20.10.123");
        cfg.setStorage_nodes(ipSet.toArray(new String[ipSet.size()]));*/
    }
}
