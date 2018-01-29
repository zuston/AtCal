package io.github.zuston.webService.Tool;

import io.github.zuston.webService.Listener.HBaseListener;
import io.github.zuston.webService.Pojo.TraceInfoPojo;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Created by zuston on 2018/1/18.
 */
public class HBaseTool {

    public static final Logger LOGGER = LoggerFactory.getLogger(HBaseTool.class);

    public static List<HashMap<String,String>> GetIndex(String tableName, String [] rowKeys) throws IOException {

        HTable table = HBaseListener.Container.get(tableName);
        List<Get> conditionRowList = new ArrayList<Get>();
        for (String batch : rowKeys){
            Get get = new Get(Bytes.toBytes(batch));
            conditionRowList.add(get);
        }
        Result[] results = table.get(conditionRowList);

        List<HashMap<String,String>> siteIDs = new ArrayList<HashMap<String,String>>();
        for (Result result : results) {
            HashMap<String,String> hm = new HashMap<String, String>();
            for (Cell kv : result.rawCells()) {
                if (new String(kv.getQualifier()).equals("index")){
                    hm.put(new String(result.getRow()),new String(kv.getValue()));
                }
            }
            siteIDs.add(hm);
        }
        return siteIDs;
    }

    public static List<String> GetIndex(String tableName, String rowkey) throws IOException {
        HTable table = HBaseListener.Container.get(tableName);
        Get get = new Get(Bytes.toBytes(rowkey));
        String ewbNoList = null;
        Result res = table.get(get);
        for (Cell kv : res.rawCells()){
            if (new String(kv.getQualifier()).equals("index")){
                ewbNoList = new String(kv.getValue());
            }
        }
        List<String> reslist = new ArrayList<String>();
        for (String ewbNo : ewbNoList.split("#")){
            reslist.add(ewbNo);
        }
        return reslist;
    }

    public static List<String> GetBySiteId(String tabelName, String siteId, int size, int page) throws IOException {
        HTable table = HBaseListener.Container.get(tabelName);
        Get get = new Get(Bytes.toBytes(siteId));
        String ewbNoList = null;
        Result res = table.get(get);
        for (Cell kv : res.rawCells()){
            if (new String(kv.getQualifier()).equals("index")){
                ewbNoList = new String(kv.getValue());
            }
        }
        int count = 0;
        List<String> reslist = new ArrayList<String>();
        String [] ewbArr = ewbNoList.split("#");
        for (int i=(page-1)*size;i<size;i++){
            reslist.add(ewbArr[i]);
        }
//        for (String ewbNo : ewbNoList.split("#")){
//            if (count>=size)    break;
//            count ++;
//            reslist.add(ewbNo);
//        }
        return reslist;
    }

    // 获取某个站点订单总量
    public static List<Long> GetEwbCount(String siteId) throws IOException {
        List<Long> reslist = new ArrayList<Long>();
        Get get = new Get(Bytes.toBytes(siteId));
        HTable outTable = HBaseListener.Container.get("siteIndex_Out");
        HTable inTable = HBaseListener.Container.get("siteIndex_In");

        String outLine = null;
        Result outRes = outTable.get(get);
        for (Cell kv : outRes.rawCells()){
            if (new String(kv.getQualifier()).equals("index")){
                outLine = new String(kv.getValue());
            }
        }

        String inLine = null;
        Result inRes = inTable.get(get);
        for (Cell kv : inRes.rawCells()){
            if (new String(kv.getQualifier()).equals("index")){
                inLine = new String(kv.getValue());
            }
        }

        if (inLine!=null && outLine!=null){
            reslist.add(Long.valueOf(inLine.split("#").length));
            reslist.add(Long.valueOf(outLine.split("#").length));
            return reslist;
        }
        return null;
//        if (inLine!=null) return inLine.split("#").length;
//        if (outLine != null)    return outLine.split("#").length;
//        return 0;
    }


    public static List<TraceInfoPojo> BatchGet(String tableName, String[] rowkeys) throws IOException {
        HTable table = HBaseListener.Container.get(tableName);
        List<Get> conditionRowList = new ArrayList<Get>();
        for (String batch : rowkeys){
            Get get = new Get(Bytes.toBytes(batch));
            conditionRowList.add(get);
        }
        Result[] results = table.get(conditionRowList);
        List<TraceInfoPojo> list = new ArrayList<TraceInfoPojo>();
        for (Result result : results){
            TraceInfoPojo pojo = new TraceInfoPojo();
            for (Cell kv : result.rawCells()) {
                String key = new String(kv.getQualifier());
                String value = new String(kv.getValue());
                if (key.equals("TRACE_ID")){
                    pojo.setTRACE_ID(value);
                }
                if (key.equals("EWB_NO")){
                    pojo.setEWB_NO(value);
                }
                if (key.equals("SITE_ID")){
                    pojo.setSITE_ID(value);
                }
                if (key.equals("SITE_NAME")){
                    pojo.setSITE_NAME(value);
                }
                if (key.equals("SCAN_TIME")){
                    pojo.setSCAN_TIME(value);
                }
                if (key.equals("DEST_SITE_ID")){
                    pojo.setDEST_SITE_ID(value);
                }
                if (key.equals("DEST_SITE_NAME")){
                    pojo.setDEST_SITE_NAME(value);
                }
                if (key.equals("PREDICT_TIME")){
                    pojo.setPREDICT_TIME(value);
                }
                if (key.equals("DESCPT")){
                    pojo.setDESCPT(value);
                }
            }
            if (pojo.getDEST_SITE_ID()==null)   pojo.setDEST_SITE_ID("");
            if (pojo.getDEST_SITE_NAME()==null) pojo.setDEST_SITE_NAME("");
            list.add(pojo);
        }
        return list;
    }

    public static List<List<TraceInfoPojo>> BatchScan(String tableName, String [] batchList) throws IOException {

//        private String TRACE_ID        ;
//        private String EWB_NO 			;
//        private String SITE_ID			;
//        private String SITE_NAME		;
//        private String SCAN_TIME		;
//        private String DEST_SITE_ID	;
//        private String DEST_SITE_NAME 	;
//        private String PREDICT_TIME;

        List<List<TraceInfoPojo>> list = new ArrayList<List<TraceInfoPojo>>();


        HTable table = HBaseListener.Container.get(tableName);
        try {
            Scan scanner = new Scan();

            scanner.setBatch(2);
            scanner.setMaxResultSize(10);

            for (String prefix : batchList){
                scanner.setFilter(new PrefixFilter(prefix.getBytes()));
                ResultScanner rs = table.getScanner(scanner);
                HashMap<String, TraceInfoPojo> hm = new HashMap<String, TraceInfoPojo>();

                for (Result result : rs){
                    String rowKey = new String(result.getRow());
                    TraceInfoPojo pojo = null;
                    if (hm.containsKey(rowKey)){
                        pojo = hm.get(rowKey);
                    }else{
                        pojo = new TraceInfoPojo();
                    }
                    for (KeyValue keyValue : result.raw()){
                        String key = new String(keyValue.getQualifier());
                        String value = new String(keyValue.getValue());
                        if (key.equals("TRACE_ID")){
                            pojo.setTRACE_ID(value);
                        }
                        if (key.equals("EWB_NO")){
                            pojo.setEWB_NO(value);
                        }
                        if (key.equals("SITE_ID")){
                            pojo.setSITE_ID(value);
                        }
                        if (key.equals("SITE_NAME")){
                            pojo.setSITE_NAME(value);
                        }
                        if (key.equals("SCAN_TIME")){
                            pojo.setSCAN_TIME(value);
                        }
                        if (key.equals("DEST_SITE_ID")){

                            pojo.setDEST_SITE_ID(value);
                        }
                        if (key.equals("DEST_SITE_NAME")){

                            pojo.setDEST_SITE_NAME(value);
                        }
                        hm.put(rowKey,pojo);
                    }
                }
                List<TraceInfoPojo> groupTrace = new ArrayList<TraceInfoPojo>();
                for (Map.Entry<String, TraceInfoPojo> entry : hm.entrySet()){
                    groupTrace.add(entry.getValue());
                }
                list.add(groupTrace);
            }

        }catch (Exception e){

        }
        return list;
    }


    // 列表页加载
    public static HashMap<String, String> ScanByPrefix(String tableName, String rowPrefix, int size) {
//        List<HashMap<String,String>> reslist = new ArrayList<HashMap<String, String>>();
        HTable table = HBaseListener.Container.get(tableName);
        HashMap<String, String> resHM = new HashMap<String, String>();

        try {
            Scan scanner = new Scan();
            scanner.setFilter(new PrefixFilter(rowPrefix.getBytes()));
            scanner.setBatch(2);
            scanner.setMaxResultSize(10);

            ResultScanner rs = table.getScanner(scanner);


            for (Result r : rs) {
                KeyValue[] kv = r.raw();

//                HashMap<String,String> hm = new HashMap<String, String>();
//                hm.put("rowkey",new String(r.getRow()));

                String rowkey = new String(r.getRow());

                for (KeyValue keyValue : kv){
//                    hm.put(new String(keyValue.getQualifier()), new String(keyValue.getValue()));
                    if (new String(keyValue.getQualifier()).equals("EWB_NO")){
                        resHM.put(rowkey,new String(keyValue.getValue()));
                    }
                }
//                reslist.add(hm);
//                if (reslist.size() >= size) break;
                if (resHM.size()==size) break;
            }
            rs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return resHM;
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

    public static void get() throws IOException {
//        List<Get> conditionRowList = new ArrayList<Get>();
//        conditionRowList.add(new Get(Bytes.toBytes("90000350700606#19199")));
//
//
//        Result [] results = HBaseListener.ActiveTraceOutTable.get(conditionRowList);//重点在这，直接查getList<Get>
//        System.out.println(results);
    }

}


//scan "ActiveRecord_Out",{ROWPREFIXFILTER=>'15148',FILTER=>"(QualifierFilter(>=,'info:ptime'))"}

//scan "ActiveRecord_Out", {FILTER => org.apache.hadoop.hbase.filter.PrefixFilter.new(org.apache.hadoop.hbase.util.Bytes.toBytes("15148")),LIMIT=>10}

// {STARTROW=>"1000009#15265",STOPROW=>"536724#15437"}

