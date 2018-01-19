package io.github.zuston.webService.Tool;

import io.github.zuston.webService.Listener.HBaseListener;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by zuston on 2018/1/18.
 */
public class HBaseTool {

    public static final Logger LOGGER = LoggerFactory.getLogger(HBaseTool.class);

    public static void Get(String tableName, String rowKey){
        HTable table = null;
        try {
            table = new HTable(HBaseListener.configuration, tableName);
            Get scan = new Get(rowKey.getBytes());// 根据rowkey查询
            Result r = table.get(scan);
            System.out.println("获得到rowkey:" + new String(r.getRow()));
            for (KeyValue keyValue : r.raw()) {
                System.out.println("列：" + new String(keyValue.getFamily())
                        + "====值:" + new String(keyValue.getValue()));
            }
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static List<HashMap<String, String>> BatchGet(String tableName, String [] batchList) throws IOException {
        List<HashMap<String,String>> reslist = new ArrayList<HashMap<String, String>>();
        HTable table = HBaseListener.Container.get(tableName);
        List<Get> conditionRowList = new ArrayList<Get>();
        for (String batch : batchList){
            Get get = new Get(Bytes.toBytes(batch));
            conditionRowList.add(get);
        }

        Result[] results = table.get(conditionRowList);//重点在这，直接查getList<Get>
        for (Result result : results){
            HashMap<String, String> hm = new HashMap<String, String>();
            hm.put("rowkey",new String(result.getRow()));
            for (Cell kv : result.rawCells()) {
                hm.put(new String(kv.getQualifier()),new String(kv.getValue()));
            }
            reslist.add(hm);
        }
        return reslist;
    }


    // 列表页加载
    public static List<HashMap<String, String>> ScanByPrefix(String tableName, String rowPrefix, int size) {
        List<HashMap<String,String>> reslist = new ArrayList<HashMap<String, String>>();
        HTable table = HBaseListener.Container.get(tableName);
        try {
            Scan scanner = new Scan();
            scanner.setFilter(new PrefixFilter(rowPrefix.getBytes()));
            scanner.setBatch(2);
            scanner.setMaxResultSize(10);

            ResultScanner rs = table.getScanner(scanner);

            for (Result r : rs) {
                KeyValue[] kv = r.raw();
                HashMap<String,String> hm = new HashMap<String, String>();
                hm.put("rowkey",new String(r.getRow()));
                for (KeyValue keyValue : kv){
                    hm.put(new String(keyValue.getQualifier()), new String(keyValue.getValue()));
                }
                reslist.add(hm);
                if (reslist.size() >= size) break;
            }
            rs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return reslist;
    }

    public static List<HashMap<String, String>> QueryAll(String tableName) throws IOException {
        List<HashMap<String,String>> reslist = new ArrayList<HashMap<String, String>>();
        HTable table = HBaseListener.Container.get(tableName);
        try {
            Scan scanner = new Scan();
            scanner.setMaxResultSize(400);
            ResultScanner rs = table.getScanner(scanner);
            for (Result r : rs) {
                HashMap<String,String> hm = new HashMap<String, String>();
                hm.put("rowkey",new String(r.getRow()));
                for (KeyValue keyValue : r.raw()) {
                    hm.put(Bytes.toString(keyValue.getQualifierArray(),keyValue.getQualifierOffset(),keyValue.getQualifierLength()),new String(keyValue.getValue()));
                }
                reslist.add(hm);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            return reslist;
        }
    }

}


//scan "ActiveRecord_Out",{ROWPREFIXFILTER=>'15148',FILTER=>"(QualifierFilter(>=,'info:ptime'))"}

//scan "ActiveRecord_Out", {FILTER => org.apache.hadoop.hbase.filter.PrefixFilter.new(org.apache.hadoop.hbase.util.Bytes.toBytes("15148")),LIMIT=>10}

// {STARTROW=>"1000009#15265",STOPROW=>"536724#15437"}

