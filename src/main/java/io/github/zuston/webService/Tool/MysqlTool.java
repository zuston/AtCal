package io.github.zuston.webService.Tool;

import io.github.zuston.webService.Pojo.Site2SitePojo;
import io.github.zuston.webService.Pojo.TraceInfoPojo;
import io.github.zuston.webService.Util.MysqlUtil;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by zuston on 2018/1/20.
 */
public class MysqlTool {

    public static List<Site2SitePojo> QueryAll(String tableName) throws SQLException {
        Connection connection = MysqlUtil.getInstance();
        Statement statement = connection.createStatement();
        String sql = "select * from " + tableName + " where total-abnormal > 20 and total > 100 and start_id != end_id order by total DESC limit 800;";    //要执行的SQL
        ResultSet rs = statement.executeQuery(sql);//创建数据对象
        List<Site2SitePojo> reslist = new ArrayList<Site2SitePojo>();
        while (rs.next()){
            Site2SitePojo pojo = new Site2SitePojo();
            pojo.startId = rs.getString(1);
            pojo.endId = rs.getString(2);
            pojo.total = rs.getInt(3);
            pojo.abnormal = rs.getInt(4);
            reslist.add(pojo);
        }
        rs.close();
        statement.close();
        return reslist;
    }

    /**
     * select q.* from (select ewb_no from activeTrace where site_id = 1000009 group by ewb_no limit 10 ) p left join activeTrace q on q.ewb_no = p.ewb_no;
     * @param tableName
     * @param siteId
     * @param size
     * @param tag
     * @return
     */
    public static List<List<TraceInfoPojo>> Query(String tableName, long siteId, int size, int tag) throws SQLException {
        List<List<TraceInfoPojo>> reslist = new ArrayList<List<TraceInfoPojo>>();
        String selectOutputParams = "q.TRACE_ID,  q.EWB_NO, q.SITE_ID,  q.SITE_NAME,  q.SCAN_TIME,  q.DEST_SITE_ID,  q.DEST_SITE_NAME,  q.PREDICT_TIME";
        String sql = "select %s from (select ewb_no from %s where %s group by ewb_no limit %s ) p left join %s q on q.ewb_no = p.ewb_no";
        String condition = "";
        if (tag==1) condition += "SITE_ID=" + siteId;
        if (tag==2) condition += "DEST_SITE_ID="+siteId;
        String generSql = String.format(sql,
                selectOutputParams,
                tableName,
                condition,
                size,
                tableName
                );
        System.out.println("输出 sql : "+generSql);

        Connection connection = MysqlUtil.getInstance();
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(generSql);//创建数据对象
        HashMap<String,List<TraceInfoPojo>> tempHm = new HashMap<String, List<TraceInfoPojo>>();
        while (rs.next()){
            String TRACE_ID = rs.getString(1);
            String EWB_NO = rs.getString(2);
            String SITE_ID = rs.getString(3);
            String SITE_NAME = rs.getString(4);
            String SCAN_TIME = rs.getString(5);
            String DEST_SITE_ID = rs.getString(6);
            String DEST_SITE_NAME = rs.getString(7);
            String PREDICT_TIME = rs.getString(8);

            TraceInfoPojo pojo = new TraceInfoPojo(TRACE_ID,EWB_NO,SITE_ID,SITE_NAME,SCAN_TIME,DEST_SITE_ID,DEST_SITE_NAME,PREDICT_TIME);
            if (tempHm.containsKey(EWB_NO)){
                tempHm.get(EWB_NO).add(pojo);
            }else{
                List<TraceInfoPojo> list = new ArrayList<TraceInfoPojo>();
                list.add(pojo);
                tempHm.put(EWB_NO,list);
            }
        }
        for (Map.Entry<String, List<TraceInfoPojo>> entry : tempHm.entrySet()){
            reslist.add(entry.getValue());
        }
        rs.close();
        statement.close();
        return reslist;
    }

    public static List<String> GetTargetInfo(String targetId) throws SQLException {
        List<String> reslist = new ArrayList<String>();
        Connection connection = MysqlUtil.getInstance();
        Statement statement = connection.createStatement();
//        String outSql = "select sum(*) from validate where start_id = "+targetId;
//        String inSql = "select sum(*) from validate where end_id = "+ targetId;
        String abnormalSql = "select sum(abnormal) from validate where start_id = "+ targetId + " or end_id = " + targetId;

//        ResultSet outRes = statement.executeQuery(outSql);
//        while (outRes.next()){
//            reslist.add(outRes.getString(1));
//        }
//        outRes.close();

//        ResultSet inRes = statement.executeQuery(inSql);
//        while (inRes.next()){
//            reslist.add(inRes.getString(1));
//        }
//        inRes.close();
        reslist.add("0");
        reslist.add("1");

        ResultSet abnormalRes = statement.executeQuery(abnormalSql);
        while (abnormalRes.next()){
            reslist.add(abnormalRes.getString(1));
        }
        abnormalRes.close();

        statement.close();
        return reslist;
    }

    public static String GetIntraveCount() throws SQLException {
        Connection connection = MysqlUtil.getInstance();
        Statement statement = connection.createStatement();
        String sql = "select sum(total) from validate";
        ResultSet res = statement.executeQuery(sql);
        while (res.next()){
            return res.getString(1);
        }
        res.close();
        statement.close();
        return null;
    }

    public static List<List<String>> GetLinkSites(String siteId) throws SQLException {
        Connection connection = MysqlUtil.getInstance();
        Statement statement = connection.createStatement();
        String sqlOut = "select end_id from validate where start_id = " + siteId;
        String sqlIn = "select start_id from validate where end_id = " + siteId;

        List<List<String>> allList = new ArrayList<List<String>>();
        List<String> outList = new ArrayList<String>();
        List<String> inList = new ArrayList<String>();

        ResultSet resOut = statement.executeQuery(sqlOut);
        while (resOut.next()){
            outList.add(resOut.getString(1));
        }
        resOut.close();
        ResultSet resIn = statement.executeQuery(sqlIn);
        while (resIn.next()){
            inList.add(resIn.getString(1));
        }
        resIn.close();
        statement.close();
        allList.add(outList);
        allList.add(inList);
        return allList;
    }
}
