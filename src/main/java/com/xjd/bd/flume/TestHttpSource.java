package com.xjd.bd.flume;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.flume.event.JSONEvent;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

/**
 * Project : big-data-example
 * PackageName : com.cnit.dlp.flume
 * Author : Xu Jiandong
 * CreateTime : 2017-07-12 16:28:00
 * ModificationHistory :
 */
public class TestHttpSource {
    private PoolingHttpClientConnectionManager connManager;
    private CloseableHttpClient httpClient;
    private HttpPost httpPost;
    private String encoding;

    public static void main(String[] args) {
        TestHttpSource testHttpSource = new TestHttpSource();
        String msg = "{\"ctm\":\"2017-01-20 09:27:03\",\"sid\":\"192-168-101-20036-110-147-36-50876-80\",\"snp\":[{\"tgs\":[{\"tgt\":3,\"dat\":{\"fid\":0,\"cue\":\"1111.txt\",\"scs\":[{\"cut\":\"enen\",\"wln\":12,\"tos\":0,\"wos\":0}],\"wds\":1,\"pid\":1,\"tms\":1},\"mts\":1}],\"cty\":1,\"cid\":467}],\"pvr\":1,\"rls\":[192],\"prt\":7,\"flt\":[{\"uid\":\"cbe79440-c702-46bd-8ded-045a45496b9d\",\"fid\":1,\"zip\":1,\"suc\":1,\"lev\":0,\"nty\":1,\"pid\":1,\"cue\":\"user_added.zip\",\"fnm\":\"user_added.zip\",\"aut\":\"\",\"pth\":\"/opt/TM_DLP/tempfile/session_stream/cbe79440-c702-46bd-8ded-045a45496b9d\",\"fsz\":184460,\"ecr\":0,\"typ\":\"zip\"},{\"uid\":\"36784678-d95d-4e2d-8d2e-a102d6270553\",\"fid\":2,\"zip\":0,\"suc\":1,\"lev\":1,\"nty\":3,\"pid\":1,\"cue\":\"user_added.zip/uud_added.txt\",\"txt\":\"/opt/TM_DLP/tempfile/session_stream/36784678-d95d-4e2d-8d2e-a102d6270553.txt\",\"fnm\":\"uud_added.txt\",\"aut\":\"text\",\"pth\":\"/opt/TM_DLP/tempfile/session_stream/36784678-d95d-4e2d-8d2e-a102d6270553\",\"fsz\":805018,\"ecr\":0,\"typ\":\"txt\"}],\"pid\":8,\"dpt\":80,\"spt\":50876,\"ept\":0,\"lvl\":1,\"dip\":\"36.110.147.36\",\"sip\":\"192.168.101.200\",\"pln\":\"zhidaole\",\"atr\":{\"mto\":[],\"mtm\":\"\",\"url\":\"pc.profile.pinyin.sogou.com/upload.php?hid=53CC1F491B94C34D3503CD8A7FB7DEA9&v=8.0.0.8268&brand=1&platform=6&ifbak=0&ifmobile=0&ifauto=1&filename=user_added.zip&m=895AB0ECF10C137E5817AF9B7017CD6C\"},\"md5\":\"7b634eae8e97ae87c889b3cb51f6f539_3e4b141f-bb56-46d1-84a2-ef679ec367c0\",\"gid\":112,\"tid\":\"\",\"mid\":34,\"typ\":1,\"o_ret\":{\"ret_sbj\":0,\"ret_pln\":0,\"ret_f_array\":[0,1]},\"zipPassword\":\"123456dlp\",\"fp\":\"/incidents/2016-11-24\"}";
        testHttpSource.putWithEncoding(msg);
        testHttpSource.close();
    }

    public TestHttpSource() {
        init();
    }
    private void init(){
        connManager = new PoolingHttpClientConnectionManager();
        //httpClient = HttpClients.createDefault();
        httpClient = HttpClients.custom().setConnectionManager(connManager).build();
        httpPost = new HttpPost("http://10.20.10.195:10011");
        encoding = "UTF-8";
    }

    public void close(){
        if(httpClient != null){
            try {
                httpClient.close();
            }catch (IOException e2){
                e2.printStackTrace();
            }
        }
        connManager.close();
    }
    private void putWithEncoding(String msg) {
        CloseableHttpResponse response;
        try {
            Type listType = new TypeToken<List<JSONEvent>>() {
            }.getType();
            List<JSONEvent> events = Lists.newArrayList();
            Map<String, String> input = Maps.newHashMap();
            input.put("accidentType", "V2-monitor");
            input.put("protocol", "");

            JSONEvent e = new JSONEvent();
            e.setHeaders(input);
            e.setBody(msg.getBytes(encoding));
            events.add(e);

            Gson gson = new Gson();
            String json = gson.toJson(events, listType);
            StringEntity input1 = new StringEntity(json);
            input1.setContentType("application/json; charset=" + encoding);
            httpPost.setEntity(input1);
            response = httpClient.execute(httpPost);
            try {
                System.out.println(response.getStatusLine());
                HttpEntity entity2 = response.getEntity();
                // do something useful with the response body
                // and ensure it is fully consumed
                EntityUtils.consume(entity2);
            } finally {
                if (response != null){
                    try{
                        response.close();
                    }catch (IOException e0){
                        e0.printStackTrace();
                    }
                }
            }

        } catch (IOException e1) {
            e1.printStackTrace();
        }/*finally {
            if(httpClient != null){
                try {
                    httpClient.close();
                }catch (IOException e2){
                    e2.printStackTrace();
                }
            }
        }*/
    }

}
