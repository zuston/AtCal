package io.github.zuston.webService.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by zuston on 2018/1/23.
 */
public class HBaseTest {
    public static void main(String[] args) throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.master", "master:60000");
        configuration.set("hbase.zookeeper.quorum","slave4,slave2,slave3");
        HTable table = new HTable(configuration, "siteIndex_Out");
        Get get = new Get(Bytes.toBytes("15304"));
        Result res = table.get(get);
        for (Cell kv : res.rawCells()){
            if (new String(kv.getQualifier()).equals("index")){
                System.out.println(new String(kv.getValue()));
            }
        }
    }
}
