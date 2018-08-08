package com.gxf.es.client;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.ResourceBundle;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) {
        /**
         * 读取配置文件
         */
        ResourceBundle rb = ResourceBundle.getBundle("conf");
        String clustername = rb.getString("elasticsearch.clustername");
        String addr1 = rb.getString("elasticsearch.addr1");

        String[] temparray = addr1.split("\\.");
        byte[] localAddr = new byte[temparray.length];
        for (int i = 0; i < temparray.length; i++) {
            localAddr[i] = Byte.parseByte(temparray[i]);
        }

        TransportClient client = null;
        try {
            Settings settings = Settings.builder().put("cluster.name", clustername).build();
            client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new TransportAddress(InetAddress.getByAddress(localAddr), 9300));
            //插入你的处理逻辑

            try {
                XContentBuilder builder = jsonBuilder().startObject().field("user", "user002").field("postDate", new Date()).field("message", "trying out elasticsearch").endObject();
                IndexResponse response = client.prepareIndex("testindex1","testtype","2").setSource(builder).execute().actionGet();

                System.out.println("============================");
                System.out.println(response.getIndex());
                System.out.println(response.getType());
                System.out.println(response.getId());
                System.out.println("============================");
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
            }


        } catch (UnknownHostException e) {
            e.printStackTrace();
        } finally {
            client.close();
        }


    }
}