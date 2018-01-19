package io.github.zuston.webService.Tool;

import io.github.zuston.webService.Pojo.Site2SitePojo;
import io.github.zuston.webService.Util.MysqlUtil;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zuston on 2018/1/20.
 */
public class MysqlTool {

    public static List<Site2SitePojo> QueryAll(String tableName) throws SQLException {
        Connection connection = MysqlUtil.getInstance();
        Statement statement = connection.createStatement();
        String sql = "select * from " + tableName;    //要执行的SQL
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

    public static List<TraceInfoPojo> Query(String tableName, String condition, int size){
        return null;
    }
}
