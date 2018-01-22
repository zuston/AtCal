package io.github.zuston.webService.Test;

import io.github.zuston.webService.Tool.HBaseTool;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by zuston on 2018/1/18.
 */
public class HBaseToolTest {
    public static void main(String[] args) throws IOException {
//        Configuration configuration;
//        configuration = HBaseConfiguration.create();
//        configuration.set("hbase.master", "master:60000");
//        configuration.set("hbase.zookeeper.quorum","slave4,slave2,slave3");
//        HTable table = null;
//        try {
//            table = new HTable(HBaseListener.configuration, "Validate");
//            ResultScanner rs = table.getScanner(new Scan());
//            for (Result r : rs) {
//                System.out.println(new String(r.getRow()));
//                for (KeyValue keyValue : r.raw()) {
//                    System.out.println(new String(keyValue.getKey())+"=="+new String(keyValue.getValueArray()));
//                }
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        } finally {
//            table.close();
//        }

        HashMap<String, String> res = HBaseTool.ScanByPrefix("ActiveRecord_Out","15305",10);
        String [] batchList = new String[res.size()];
        int count = 0;
        for (Map.Entry<String, String> entry : res.entrySet()){
            batchList[count] = entry.getValue();
            count++;
        }
//        List<List<TraceInfoPojo>> list = HBaseTool.BatchGet("trace",batchList);
//        for (List<TraceInfoPojo> pojos : list){
//            System.out.println(pojos.get(0).getEWB_NO());
//        }
    }
}
