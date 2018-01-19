package io.github.zuston.webService.Test;

import io.github.zuston.webService.Listener.HBaseListener;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;

/**
 * Created by zuston on 2018/1/18.
 */
public class HBaseToolTest {
    public static void main(String[] args) throws IOException {
        Configuration configuration;
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.master", "master:60000");
        configuration.set("hbase.zookeeper.quorum","slave4,slave2,slave3");
        HTable table = null;
        try {
            table = new HTable(HBaseListener.configuration, "Validate");
            ResultScanner rs = table.getScanner(new Scan());
            for (Result r : rs) {
                System.out.println(new String(r.getRow()));
                for (KeyValue keyValue : r.raw()) {
                    System.out.println(new String(keyValue.getKey())+"=="+new String(keyValue.getValueArray()));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            table.close();
        }
    }
}
