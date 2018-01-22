package io.github.zuston.webService.Test;

import io.github.zuston.Util.ListTool;
import io.github.zuston.webService.Pojo.TraceInfoPojo;
import io.github.zuston.webService.Tool.HBaseTool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by zuston on 2018/1/18.
 */
public class HBaseToolTest {
    public static void main(String[] args) throws IOException {
        String tableName = "ActiveRecord_Out";
        int size = 10;
        String siteId = String.valueOf(15305);
        HashMap<String, String> res = HBaseTool.ScanByPrefix(tableName, String.valueOf(siteId), size);

        List<String> ewbList = new ArrayList<String>();

        // 获取全部 ewb_no
        for (Map.Entry<String, String> entry : res.entrySet()){
            ewbList.add(entry.getValue());
        }

        List<List<TraceInfoPojo>> reslist = new ArrayList<List<TraceInfoPojo>>();

        // 获取 ids 的字符串
        List<HashMap<String,String>> idList = HBaseTool.GetIndex("index", ListTool.list2arr(ewbList));

        for (HashMap<String,String> hashMap : idList){
            List<String> rowKeyList = new ArrayList<String>();
            for (Map.Entry<String, String> entry : hashMap.entrySet()){
                String ewbNo = entry.getKey();
                String [] siteArr = entry.getValue().split("#");
                for (String site : siteArr){
                    rowKeyList.add(site + "#" +ewbNo);
                }
            }
            List<TraceInfoPojo> pojos = HBaseTool.BatchGet(tableName, ListTool.list2arr(rowKeyList));
            reslist.add(pojos);
        }

    }
}
