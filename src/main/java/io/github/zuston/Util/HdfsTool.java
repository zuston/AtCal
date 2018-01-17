package io.github.zuston.Util;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zuston on 2018/1/15.
 */
public class HdfsTool {

    public static Logger logger = LoggerFactory.getLogger(HdfsTool.class);

    public static final String HDFS_URL = "hdfs://10.10.0.91:8020";

    public static List<String> readFromHdfs(Configuration configuration, String path) throws IOException {
        String dst = HDFS_URL + path;
        FileSystem fs = FileSystem.get(URI.create(dst), configuration);

        FSDataInputStream hdfsInStream = fs.open(new Path(dst));
        // 防止中文乱码
        // 简直傻逼，ubuntu下默认编码不是 utf-8，但是 mac 下就是 默认编码。我擦擦擦啊
        BufferedReader bf=new BufferedReader(new InputStreamReader(hdfsInStream, "UTF-8"));


        String record = null;
        List<String> lineList = new ArrayList<String>();
        while ((record=bf.readLine())!=null){
            lineList.add(record);
        }
        bf.close();
        hdfsInStream.close();
        // 关闭 fs 导致 context.write 的时候 nio 已经响应关闭
//        fs.close();
        return lineList;
    }


    public static boolean deleteFile(String path){
        Configuration configuration = new Configuration();
        String dst = HDFS_URL + path;
        FileSystem fs = null;
        try {
            fs = FileSystem.get(configuration);
            fs.delete(new Path(dst));
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public static boolean deleteDir(String dir) throws IOException {
        if (StringUtils.isBlank(dir)) {
            return false;
        }
        dir = HDFS_URL + dir;
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(dir), conf);
        fs.delete(new Path(dir), true);
        fs.close();
        return true;
    }
}
