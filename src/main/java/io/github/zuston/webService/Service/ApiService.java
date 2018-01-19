package io.github.zuston.webService.Service;

import com.google.gson.Gson;
import io.github.zuston.webService.Pojo.Site2SitePojo;
import io.github.zuston.webService.Tool.HBaseTool;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by zuston on 2018/1/18.
 */
@Service
public class ApiService {

    @Autowired
    private Gson gson;

    public static final String VALIDATE_TABLE_NAME = "Validate";

    public String siteTraceInfo(){
        List<Site2SitePojo> reslist = new ArrayList<Site2SitePojo>();
        String resJson = "";
        try {
            List<HashMap<String,String>> site2siteList = HBaseTool.QueryAll(VALIDATE_TABLE_NAME);
            for (HashMap<String, String> hm : site2siteList){
                String site2siteLine = hm.get("rowkey");
                String startName = site2siteLine.split("#")[0];
                String endName = site2siteLine.split("#")[1];
                int total = Integer.parseInt(hm.get("total"));
                int abnormal = Integer.parseInt(hm.get("abnormal"));
                Site2SitePojo pojo = new Site2SitePojo(startName, endName, total, abnormal);
                reslist.add(pojo);
            }
            resJson = gson.toJson(reslist);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return resJson;
    }

    // 查找出当前站点的在途订单，再查询订单详情。
    public String siteInfo(long siteId, int size, int tag) {
        String tableName = "ActiveRecord_Out";
        if (tag==2){
            tableName = "ActiveRecord_In";
        }
        List<HashMap<String,String>> siteTraceList = HBaseTool.ScanByPrefix(tableName, String.valueOf(siteId), size);
        List<String> rowKeyList = new ArrayList<String>();
        for (HashMap<String, String> hm : siteTraceList){
            String joinKey = hm.get("rowkeys");
//            rowKeyList.add();
        }
        return "";
    }
}
